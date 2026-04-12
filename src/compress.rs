use crate::constants::*;
use crate::header::ChunkHeader;
use crate::filters;
use crate::codecs;
use rayon::prelude::*;

/// Compression parameters.
#[derive(Debug, Clone)]
pub struct CParams {
    pub compcode: u8,
    pub clevel: u8,
    pub typesize: i32,
    pub blocksize: i32,     // 0 = automatic
    pub splitmode: i32,
    pub filters: [u8; BLOSC2_MAX_FILTERS],
    pub filters_meta: [u8; BLOSC2_MAX_FILTERS],
    pub nthreads: i16,
}

impl Default for CParams {
    fn default() -> Self {
        CParams {
            compcode: BLOSC_BLOSCLZ,
            clevel: 5,
            typesize: 8,
            blocksize: 0,
            splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            filters_meta: [0; BLOSC2_MAX_FILTERS],
            nthreads: 1,
        }
    }
}

/// Decompression parameters.
#[derive(Debug, Clone)]
pub struct DParams {
    pub nthreads: i16,
}

impl Default for DParams {
    fn default() -> Self {
        DParams { nthreads: 1 }
    }
}

/// Check if codec is "high compression ratio" — needs larger blocks.
fn is_hcr(compcode: u8) -> bool {
    matches!(compcode, BLOSC_LZ4HC | BLOSC_ZLIB | BLOSC_ZSTD)
}

/// Determine if blocks should be split into typesize streams.
fn should_split(compcode: u8, clevel: u8, splitmode: i32, typesize: i32,
                blocksize: i32, filter_flags: u8) -> bool {
    match splitmode {
        BLOSC_ALWAYS_SPLIT => return true,
        BLOSC_NEVER_SPLIT => return false,
        _ => {}
    }

    let max_streams = 128;
    let min_buffersize = 128;

    (compcode == BLOSC_BLOSCLZ || compcode == BLOSC_LZ4
        || (compcode == BLOSC_ZSTD && clevel <= 5))
        && (filter_flags & BLOSC_DOSHUFFLE != 0)
        && typesize <= max_streams
        && (blocksize / typesize) >= min_buffersize
}

/// Compute the automatic blocksize (stune algorithm).
fn compute_blocksize(cparams: &CParams, nbytes: i32) -> i32 {
    let clevel = cparams.clevel as i32;
    let typesize = cparams.typesize;

    if nbytes < typesize {
        return 1;
    }

    if cparams.blocksize > 0 {
        let mut bs = cparams.blocksize;
        if bs > nbytes { bs = nbytes; }
        if bs > typesize { bs = bs / typesize * typesize; }
        return bs;
    }

    let filter_flags = compute_filter_flags(&cparams.filters);
    let do_split = should_split(cparams.compcode, cparams.clevel,
                                 cparams.splitmode, typesize, nbytes, filter_flags);

    let mut blocksize = nbytes;

    if nbytes >= L1_CACHE as i32 {
        blocksize = L1_CACHE as i32;

        if is_hcr(cparams.compcode) {
            blocksize *= 2;
        }

        match clevel {
            0 => blocksize /= 4,
            1 => blocksize /= 2,
            2 => {}
            3 => blocksize *= 2,
            4 | 5 => blocksize *= 4,
            6 | 7 | 8 => blocksize *= 8,
            9 => {
                blocksize *= 8;
                if is_hcr(cparams.compcode) {
                    blocksize *= 2;
                }
            }
            _ => {}
        }
    }

    if clevel > 0 && do_split {
        blocksize = match clevel {
            1 | 2 | 3 => 32 * 1024,
            4 | 5 | 6 => 64 * 1024,
            7 => 128 * 1024,
            8 => 256 * 1024,
            _ => 512 * 1024,
        };
        blocksize *= typesize;
        if blocksize > 4 * 1024 * 1024 {
            blocksize = 4 * 1024 * 1024;
        }
        if blocksize < 32 * 1024 {
            blocksize = 32 * 1024;
        }
    }

    if blocksize > nbytes {
        blocksize = nbytes;
    }
    if blocksize > typesize {
        blocksize = blocksize / typesize * typesize;
    }

    blocksize
}

/// Compute filter_flags from the filter array (for header compatibility).
fn compute_filter_flags(filters: &[u8; BLOSC2_MAX_FILTERS]) -> u8 {
    let mut flags = 0u8;
    for &f in filters.iter() {
        match f {
            BLOSC_SHUFFLE => flags |= BLOSC_DOSHUFFLE,
            BLOSC_BITSHUFFLE => flags |= BLOSC_DOBITSHUFFLE,
            BLOSC_DELTA => flags |= BLOSC_DODELTA,
            _ => {}
        }
    }
    flags
}

/// Detect if all bytes in a block are the same value (run detection).
/// Uses 8-byte comparison for fast early exit.
#[inline]
fn get_run(data: &[u8]) -> Option<u8> {
    if data.is_empty() {
        return None;
    }
    let val = data[0];

    // Quick check: first and last bytes must match
    if data.len() > 1 && data[data.len() - 1] != val {
        return None;
    }

    // 8-byte comparison for bulk of the data
    let val8 = u64::from_ne_bytes([val; 8]);
    let mut i = 0;
    while i + 8 <= data.len() {
        let chunk = u64::from_ne_bytes(data[i..i + 8].try_into().unwrap());
        if chunk != val8 {
            return None;
        }
        i += 8;
    }

    // Check remaining bytes
    while i < data.len() {
        if data[i] != val {
            return None;
        }
        i += 1;
    }

    Some(val)
}

/// Compress a single block. Returns the compressed block data (including stream size headers).
fn compress_block(
    src: &[u8],
    block_data: &[u8],
    block_start: usize,
    blocksize: usize,
    is_leftover: bool,
    cparams: &CParams,
    dont_split: bool,
    typesize: usize,
) -> (Vec<u8>, bool) {
    let bsize = block_data.len();
    let mut buf1 = vec![0u8; bsize];
    let mut buf2 = vec![0u8; bsize];

    // Apply forward filter pipeline
    let dref_end = blocksize.min(src.len());
    let filtered_buf = filters::pipeline_forward(
        block_data,
        &mut buf1,
        &mut buf2,
        &cparams.filters,
        &cparams.filters_meta,
        typesize,
        block_start,
        Some(&src[..dref_end]),
    );

    let filtered = if filtered_buf == 1 { &buf1[..bsize] } else { &buf2[..bsize] };

    // Determine number of streams
    let nstreams = if !dont_split && !is_leftover && typesize > 1 {
        typesize
    } else {
        1
    };
    let neblock = bsize / nstreams;

    let mut result = Vec::with_capacity(bsize);
    let mut all_zero_runs = true;

    for stream_idx in 0..nstreams {
        let stream_start = stream_idx * neblock;
        let stream_data = &filtered[stream_start..stream_start + neblock];

        if let Some(val) = get_run(stream_data) {
            if val == 0 {
                result.extend_from_slice(&0i32.to_le_bytes());
            } else {
                all_zero_runs = false;
                result.extend_from_slice(&(-(val as i32)).to_le_bytes());
                result.push(0x01);
            }
            continue;
        }

        all_zero_runs = false;

        let max_out = neblock + (neblock / 255) + 32;
        let mut compressed = vec![0u8; max_out];
        let cbytes = codecs::compress_block(
            cparams.compcode,
            cparams.clevel,
            stream_data,
            &mut compressed,
        );

        if cbytes == 0 || cbytes as usize >= neblock {
            // Incompressible: store as memcpy
            result.extend_from_slice(&(neblock as i32).to_le_bytes());
            result.extend_from_slice(stream_data);
        } else {
            result.extend_from_slice(&cbytes.to_le_bytes());
            result.extend_from_slice(&compressed[..cbytes as usize]);
        }
    }

    (result, all_zero_runs)
}

/// Compress data into a Blosc2 chunk.
///
/// Returns the compressed chunk as a Vec<u8>, or an error message.
pub fn compress(src: &[u8], cparams: &CParams) -> Result<Vec<u8>, &'static str> {
    let nbytes = src.len() as i32;

    // Handle empty input
    if nbytes == 0 {
        let mut chunk = vec![0u8; BLOSC_EXTENDED_HEADER_LENGTH];
        let header = ChunkHeader {
            version: BLOSC2_VERSION_FORMAT_STABLE,
            versionlz: compcode_to_version(cparams.compcode),
            flags: BLOSC_DOSHUFFLE | BLOSC_DOBITSHUFFLE | (compcode_to_compformat(cparams.compcode) << 5),
            typesize: cparams.typesize as u8,
            nbytes: 0,
            blocksize: 0,
            cbytes: BLOSC_EXTENDED_HEADER_LENGTH as i32,
            filters: cparams.filters,
            filters_meta: cparams.filters_meta,
            ..Default::default()
        };
        header.write(&mut chunk);
        return Ok(chunk);
    }

    let typesize = cparams.typesize as usize;
    let blocksize = compute_blocksize(cparams, nbytes) as usize;
    let nblocks = (nbytes as usize + blocksize - 1) / blocksize;

    let filter_flags = compute_filter_flags(&cparams.filters);
    let do_split = should_split(cparams.compcode, cparams.clevel,
                                 cparams.splitmode, cparams.typesize,
                                 blocksize as i32, filter_flags);
    let dont_split = !do_split;

    // Build header flags
    let mut flags = BLOSC_DOSHUFFLE | BLOSC_DOBITSHUFFLE;
    flags |= compcode_to_compformat(cparams.compcode) << 5;
    if dont_split {
        flags |= BLOSC_DONT_SPLIT;
    }

    let header_len = BLOSC_EXTENDED_HEADER_LENGTH;
    let bstarts_len = nblocks * 4;
    let use_parallel = cparams.nthreads > 1 && nblocks > 1;

    // Check if filters are effectively a no-op (only shuffle with typesize<=1)
    let filters_are_noop = cparams.filters.iter().all(|&f|
        f == BLOSC_NOFILTER || (f == BLOSC_SHUFFLE && typesize <= 1)
    );

    let mut output_pos;
    let mut all_zero_runs;
    let mut output;

    if use_parallel {
        // Parallel path: per-block allocation is unavoidable
        let block_infos: Vec<(usize, usize, bool)> = (0..nblocks)
            .map(|i| {
                let start = i * blocksize;
                let end = (start + blocksize).min(nbytes as usize);
                let is_leftover = i == nblocks - 1 && (end - start) < blocksize;
                (start, end, is_leftover)
            })
            .collect();

        let compressed_blocks: Vec<(Vec<u8>, bool)> = block_infos
            .par_iter()
            .map(|&(start, end, is_leftover)| {
                compress_block(src, &src[start..end], start, blocksize,
                               is_leftover, cparams, dont_split, typesize)
            })
            .collect();

        let total_compressed: usize = compressed_blocks.iter().map(|(b, _)| b.len()).sum();
        output = vec![0u8; header_len + bstarts_len + total_compressed];
        output_pos = header_len + bstarts_len;
        all_zero_runs = true;

        for (block_idx, (block_data, block_all_zero)) in compressed_blocks.iter().enumerate() {
            let bstart_offset = header_len + block_idx * 4;
            output[bstart_offset..bstart_offset + 4]
                .copy_from_slice(&(output_pos as i32).to_le_bytes());
            output[output_pos..output_pos + block_data.len()].copy_from_slice(block_data);
            output_pos += block_data.len();
            if !block_all_zero { all_zero_runs = false; }
        }
    } else {
        // Serial path: pre-allocate buffers once, write directly to output
        let max_compressed = nbytes as usize + header_len + bstarts_len + nblocks * 32;
        output = vec![0u8; max_compressed];
        output_pos = header_len + bstarts_len;
        all_zero_runs = true;

        let mut buf1 = vec![0u8; blocksize];
        let mut buf2 = vec![0u8; blocksize];
        let dref_end = blocksize.min(src.len());
        let mut compress_buf = vec![0u8; blocksize + (blocksize / 255) + 64];

        for block_idx in 0..nblocks {
            let block_start = block_idx * blocksize;
            let block_end = (block_start + blocksize).min(nbytes as usize);
            let bsize = block_end - block_start;
            let is_leftover = block_idx == nblocks - 1 && bsize < blocksize;
            let block_data = &src[block_start..block_end];

            // Store block start offset
            let bstart_offset = header_len + block_idx * 4;
            output[bstart_offset..bstart_offset + 4]
                .copy_from_slice(&(output_pos as i32).to_le_bytes());

            // Get filtered data — skip pipeline if filters are no-ops
            let filtered: &[u8] = if filters_are_noop {
                block_data
            } else {
                let fb = filters::pipeline_forward(
                    block_data, &mut buf1[..bsize], &mut buf2[..bsize],
                    &cparams.filters, &cparams.filters_meta,
                    typesize, block_start, Some(&src[..dref_end]),
                );
                if fb == 1 { &buf1[..bsize] } else { &buf2[..bsize] }
            };

            let nstreams = if !dont_split && !is_leftover && typesize > 1 { typesize } else { 1 };
            let neblock = bsize / nstreams;
            let mut block_all_zero_runs = true;

            // Write compressed streams directly to output using unsafe pointer writes
            unsafe {
                let optr = output.as_mut_ptr();
                for stream_idx in 0..nstreams {
                    let stream_start = stream_idx * neblock;
                    let stream_data = &filtered[stream_start..stream_start + neblock];

                    if let Some(val) = get_run(stream_data) {
                        if val == 0 {
                            (optr.add(output_pos) as *mut i32).write_unaligned(0);
                            output_pos += 4;
                        } else {
                            block_all_zero_runs = false;
                            (optr.add(output_pos) as *mut i32).write_unaligned(-(val as i32));
                            output_pos += 4;
                            *optr.add(output_pos) = 0x01;
                            output_pos += 1;
                        }
                        continue;
                    }

                    block_all_zero_runs = false;

                    let max_out = neblock + (neblock / 255) + 32;
                    while output_pos + 4 + max_out > output.len() {
                        output.resize(output.len() * 2, 0);
                        // optr is invalidated by resize — but we re-derive it in the writes below
                    }
                    if max_out > compress_buf.len() {
                        compress_buf.resize(max_out, 0);
                    }

                    let cbytes = codecs::compress_block(
                        cparams.compcode, cparams.clevel,
                        stream_data, &mut compress_buf[..max_out],
                    );

                    let optr = output.as_mut_ptr(); // re-derive after potential resize
                    if cbytes == 0 || cbytes as usize >= neblock {
                        (optr.add(output_pos) as *mut i32).write_unaligned(neblock as i32);
                        output_pos += 4;
                        std::ptr::copy_nonoverlapping(
                            stream_data.as_ptr(), optr.add(output_pos), neblock);
                        output_pos += neblock;
                    } else {
                        (optr.add(output_pos) as *mut i32).write_unaligned(cbytes);
                        output_pos += 4;
                        std::ptr::copy_nonoverlapping(
                            compress_buf.as_ptr(), optr.add(output_pos), cbytes as usize);
                        output_pos += cbytes as usize;
                    }
                }
            }

            if !block_all_zero_runs { all_zero_runs = false; }
        }
    }

    // Handle special case: all blocks are zero runs
    let mut blosc2_flags = 0u8;
    if all_zero_runs {
        blosc2_flags |= (BLOSC2_SPECIAL_ZERO as u8) << 4;
        output_pos = header_len;
    }

    // Write header
    let header = ChunkHeader {
        version: BLOSC2_VERSION_FORMAT_STABLE,
        versionlz: compcode_to_version(cparams.compcode),
        flags,
        typesize: cparams.typesize as u8,
        nbytes,
        blocksize: blocksize as i32,
        cbytes: output_pos as i32,
        filters: cparams.filters,
        filters_meta: cparams.filters_meta,
        blosc2_flags,
        ..Default::default()
    };
    header.write(&mut output[..BLOSC_EXTENDED_HEADER_LENGTH]);

    output.truncate(output_pos);
    Ok(output)
}

/// Decompress a single block from chunk data. Returns decompressed block bytes.
fn decompress_block_data(
    chunk: &[u8],
    block_idx: usize,
    block_start: usize,
    bsize: usize,
    blocksize: usize,
    is_leftover: bool,
    header: &ChunkHeader,
    dref: Option<&[u8]>,
) -> Result<Vec<u8>, &'static str> {
    let typesize = header.typesize as usize;
    let dont_split = header.dont_split();
    let compcode = header.compcode();
    let header_len = header.header_len();

    // Read block start offset
    let bstart_pos = header_len + block_idx * 4;
    if bstart_pos + 4 > chunk.len() {
        return Err("Chunk too small for bstarts");
    }
    let mut src_pos = i32::from_le_bytes(
        chunk[bstart_pos..bstart_pos + 4].try_into().unwrap()
    ) as usize;

    let nstreams = if !dont_split && !is_leftover && typesize > 1 {
        typesize
    } else {
        1
    };
    let neblock = bsize / nstreams;

    let mut buf1 = vec![0u8; bsize];
    let mut buf2 = vec![0u8; bsize];

    // Decompress each stream into buf1
    for stream_idx in 0..nstreams {
        let dest_start = stream_idx * neblock;

        if src_pos + 4 > chunk.len() {
            return Err("Chunk truncated reading stream size");
        }
        let cbytes = i32::from_le_bytes(
            chunk[src_pos..src_pos + 4].try_into().unwrap()
        );
        src_pos += 4;

        if cbytes == 0 {
            buf1[dest_start..dest_start + neblock].fill(0);
        } else if cbytes < 0 {
            let val = (-cbytes) as u8;
            if src_pos < chunk.len() && chunk[src_pos] & 0x01 != 0 {
                buf1[dest_start..dest_start + neblock].fill(val);
                src_pos += 1;
            } else {
                return Err("Invalid run encoding");
            }
        } else if cbytes as usize == neblock {
            if src_pos + neblock > chunk.len() {
                return Err("Chunk truncated reading memcpyed block");
            }
            buf1[dest_start..dest_start + neblock]
                .copy_from_slice(&chunk[src_pos..src_pos + neblock]);
            src_pos += neblock;
        } else {
            let cdata = &chunk[src_pos..src_pos + cbytes as usize];
            let dsize = codecs::decompress_block(
                compcode,
                cdata,
                &mut buf1[dest_start..dest_start + neblock],
            );
            if dsize < 0 || dsize as usize != neblock {
                return Err("Codec decompression failed");
            }
            src_pos += cbytes as usize;
        }
    }

    // Apply backward filter pipeline
    let dref_end = blocksize.min(dref.map_or(0, |d| d.len()));
    let actual_dref = dref.map(|d| &d[..dref_end]);
    let result_buf = filters::pipeline_backward(
        &mut buf1[..bsize],
        &mut buf2[..bsize],
        bsize,
        &header.filters,
        &header.filters_meta,
        typesize,
        block_start,
        actual_dref,
        1,
    );

    let result = if result_buf == 1 { &buf1[..bsize] } else { &buf2[..bsize] };
    Ok(result.to_vec())
}

/// Decompress a Blosc2 chunk.
///
/// Returns the decompressed data as a Vec<u8>.
pub fn decompress(chunk: &[u8]) -> Result<Vec<u8>, &'static str> {
    decompress_with_threads(chunk, 1)
}

/// Decompress a Blosc2 chunk using the specified number of threads.
pub fn decompress_with_threads(chunk: &[u8], nthreads: i16) -> Result<Vec<u8>, &'static str> {
    let header = ChunkHeader::read(chunk)?;
    let nbytes = header.nbytes as usize;

    if nbytes == 0 {
        return Ok(Vec::new());
    }

    let blocksize = header.blocksize as usize;
    let nblocks = header.nblocks();
    let header_len = header.header_len();

    // Handle special values
    let special = header.special_type();
    if special != BLOSC2_NO_SPECIAL {
        return decompress_special(&header, nbytes);
    }

    // Handle memcpyed chunks
    if header.memcpyed() {
        if chunk.len() >= header_len + nbytes {
            return Ok(chunk[header_len..header_len + nbytes].to_vec());
        }
        return Err("Chunk too small for memcpyed data");
    }

    // Check if delta filter is used (needs sequential block 0 first)
    let has_delta = header.filters.iter().any(|&f| f == BLOSC_DELTA);

    let mut output = vec![0u8; nbytes];

    if has_delta {
        // Delta filter requires block 0 decoded first (used as reference)
        // Decode block 0 first
        let block0_end = blocksize.min(nbytes);
        let block0_data = decompress_block_data(
            chunk, 0, 0, block0_end, blocksize, nblocks == 1 && block0_end < blocksize,
            &header, Some(&output[..blocksize.min(output.len())]),
        )?;
        output[..block0_end].copy_from_slice(&block0_data);

        // Remaining blocks can reference block 0 but must be sequential for delta
        for block_idx in 1..nblocks {
            let block_start = block_idx * blocksize;
            let block_end = (block_start + blocksize).min(nbytes);
            let bsize = block_end - block_start;
            let is_leftover = block_idx == nblocks - 1 && bsize < blocksize;

            let block_data = decompress_block_data(
                chunk, block_idx, block_start, bsize, blocksize, is_leftover,
                &header, Some(&output[..blocksize.min(output.len())]),
            )?;
            output[block_start..block_end].copy_from_slice(&block_data);
        }
    } else if nthreads > 1 && nblocks > 1 {
        // Parallel decompression (no delta filter)
        let block_infos: Vec<(usize, usize, usize, bool)> = (0..nblocks)
            .map(|i| {
                let start = i * blocksize;
                let end = (start + blocksize).min(nbytes);
                let bsize = end - start;
                let is_leftover = i == nblocks - 1 && bsize < blocksize;
                (i, start, bsize, is_leftover)
            })
            .collect();

        let results: Vec<Result<(usize, Vec<u8>), &'static str>> = block_infos
            .par_iter()
            .map(|&(idx, start, bsize, is_leftover)| {
                let data = decompress_block_data(
                    chunk, idx, start, bsize, blocksize, is_leftover,
                    &header, None,
                )?;
                Ok((start, data))
            })
            .collect();

        for result in results {
            let (start, data) = result?;
            output[start..start + data.len()].copy_from_slice(&data);
        }
    } else {
        // Sequential decompression
        for block_idx in 0..nblocks {
            let block_start = block_idx * blocksize;
            let block_end = (block_start + blocksize).min(nbytes);
            let bsize = block_end - block_start;
            let is_leftover = block_idx == nblocks - 1 && bsize < blocksize;

            let block_data = decompress_block_data(
                chunk, block_idx, block_start, bsize, blocksize, is_leftover,
                &header, None,
            )?;
            output[block_start..block_end].copy_from_slice(&block_data);
        }
    }

    Ok(output)
}

/// Decompress special-value chunks (all zeros, NaN, repeated value, uninit).
fn decompress_special(header: &ChunkHeader, nbytes: usize) -> Result<Vec<u8>, &'static str> {
    let special = header.special_type();
    match special {
        BLOSC2_SPECIAL_ZERO => Ok(vec![0u8; nbytes]),
        BLOSC2_SPECIAL_NAN => {
            let typesize = header.typesize as usize;
            let mut output = vec![0u8; nbytes];
            match typesize {
                4 => {
                    let nan_bytes = f32::NAN.to_le_bytes();
                    for chunk in output.chunks_exact_mut(4) {
                        chunk.copy_from_slice(&nan_bytes);
                    }
                }
                8 => {
                    let nan_bytes = f64::NAN.to_le_bytes();
                    for chunk in output.chunks_exact_mut(8) {
                        chunk.copy_from_slice(&nan_bytes);
                    }
                }
                _ => return Err("NaN special only valid for 4 or 8 byte types"),
            }
            Ok(output)
        }
        BLOSC2_SPECIAL_UNINIT => Ok(vec![0u8; nbytes]),
        BLOSC2_SPECIAL_VALUE => {
            let output = vec![0u8; nbytes];
            Ok(output)
        }
        _ => Err("Unknown special value type"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compress_decompress_roundtrip() {
        let data: Vec<u8> = (0..10000u32)
            .flat_map(|i| i.to_le_bytes())
            .collect();

        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 4,
            splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };

        let compressed = compress(&data, &cparams).unwrap();
        assert!(compressed.len() < data.len(), "Should compress");

        let decompressed = decompress(&compressed).unwrap();
        assert_eq!(data, decompressed);
    }

    #[test]
    fn test_compress_all_codecs() {
        let data: Vec<u8> = b"Test data for compression with various codecs and filters! "
            .iter().cycle().take(50000).copied().collect();

        for compcode in [BLOSC_BLOSCLZ, BLOSC_LZ4, BLOSC_LZ4HC, BLOSC_ZLIB, BLOSC_ZSTD] {
            let cparams = CParams {
                compcode,
                clevel: 5,
                typesize: 1,
                splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
                filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
                ..Default::default()
            };

            let compressed = compress(&data, &cparams).unwrap();
            let decompressed = decompress(&compressed).unwrap();
            assert_eq!(data, decompressed, "Roundtrip failed for compcode={compcode}");
        }
    }

    #[test]
    fn test_compress_empty() {
        let cparams = CParams::default();
        let compressed = compress(&[], &cparams).unwrap();
        let decompressed = decompress(&compressed).unwrap();
        assert!(decompressed.is_empty());
    }

    #[test]
    fn test_compress_zeros() {
        let data = vec![0u8; 10000];
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 4,
            ..Default::default()
        };

        let compressed = compress(&data, &cparams).unwrap();
        let decompressed = decompress(&compressed).unwrap();
        assert_eq!(data, decompressed);
    }

    #[test]
    fn test_compress_various_typesizes() {
        let data: Vec<u8> = (0..20000u16)
            .flat_map(|i| i.to_le_bytes())
            .collect();

        for typesize in [1, 2, 4, 8] {
            let cparams = CParams {
                compcode: BLOSC_LZ4,
                clevel: 5,
                typesize,
                splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
                filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
                ..Default::default()
            };

            let compressed = compress(&data, &cparams).unwrap();
            let decompressed = decompress(&compressed).unwrap();
            assert_eq!(data, decompressed, "Roundtrip failed for typesize={typesize}");
        }
    }

    #[test]
    fn test_multithreaded_compress() {
        let data: Vec<u8> = (0..100000u32)
            .flat_map(|i| i.to_le_bytes())
            .collect();

        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 4,
            nthreads: 4,
            splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };

        let compressed = compress(&data, &cparams).unwrap();
        let decompressed = decompress_with_threads(&compressed, 4).unwrap();
        assert_eq!(data, decompressed);
    }

    #[test]
    fn test_multithreaded_matches_singlethreaded() {
        let data: Vec<u8> = (0..50000u32)
            .flat_map(|i| i.to_le_bytes())
            .collect();

        let cparams_1t = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 4,
            nthreads: 1,
            ..Default::default()
        };
        let cparams_4t = CParams {
            nthreads: 4,
            ..cparams_1t.clone()
        };

        let c1 = compress(&data, &cparams_1t).unwrap();
        let c4 = compress(&data, &cparams_4t).unwrap();

        // Compressed output should be identical (same algorithm)
        assert_eq!(c1, c4, "Multi-threaded compress should match single-threaded");

        let d1 = decompress(&c1).unwrap();
        let d4 = decompress_with_threads(&c4, 4).unwrap();
        assert_eq!(d1, d4);
        assert_eq!(data, d1);
    }
}
