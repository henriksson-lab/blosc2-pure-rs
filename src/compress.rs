use crate::codecs;
use crate::constants::*;
use crate::filters;
use crate::header::ChunkHeader;
use rayon::prelude::*;

/// Compression parameters.
#[derive(Debug, Clone)]
pub struct CParams {
    /// Codec identifier, such as `BLOSC_LZ4` or `BLOSC_ZSTD`.
    pub compcode: u8,
    /// Compression level from 0 to 9.
    pub clevel: u8,
    /// Logical element size in bytes. Filters such as shuffle operate over this size.
    pub typesize: i32,
    /// Block size in bytes. Use 0 for automatic sizing.
    pub blocksize: i32, // 0 = automatic
    /// Stream split mode, such as `BLOSC_FORWARD_COMPAT_SPLIT`.
    pub splitmode: i32,
    /// Filter pipeline codes. The last position is commonly used for the primary filter.
    pub filters: [u8; BLOSC2_MAX_FILTERS],
    /// Per-filter metadata bytes.
    pub filters_meta: [u8; BLOSC2_MAX_FILTERS],
    /// Number of worker threads for block-parallel compression.
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
    /// Number of worker threads for block-parallel decompression.
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
fn should_split(
    compcode: u8,
    clevel: u8,
    splitmode: i32,
    typesize: i32,
    blocksize: i32,
    filter_flags: u8,
) -> bool {
    match splitmode {
        BLOSC_ALWAYS_SPLIT => return true,
        BLOSC_NEVER_SPLIT => return false,
        _ => {}
    }

    let max_streams = 128;
    let min_buffersize = 128;

    (compcode == BLOSC_BLOSCLZ || compcode == BLOSC_LZ4 || (compcode == BLOSC_ZSTD && clevel <= 5))
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
        if bs > nbytes {
            bs = nbytes;
        }
        if bs > typesize {
            bs = bs / typesize * typesize;
        }
        return bs;
    }

    let filter_flags = compute_filter_flags(&cparams.filters);
    let do_split = should_split(
        cparams.compcode,
        cparams.clevel,
        cparams.splitmode,
        typesize,
        nbytes,
        filter_flags,
    );

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
            6..=8 => blocksize *= 8,
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
            1..=3 => 32 * 1024,
            4..=6 => 64 * 1024,
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

fn validate_cparams(cparams: &CParams, nbytes: usize) -> Result<(), &'static str> {
    if nbytes > BLOSC2_MAX_BUFFERSIZE as usize {
        return Err("Input too large");
    }
    if !(1..=BLOSC_MAX_TYPESIZE as i32).contains(&cparams.typesize) {
        return Err("Invalid typesize");
    }
    if cparams.clevel > 9 {
        return Err("Invalid compression level");
    }
    if cparams.blocksize < 0 {
        return Err("Invalid blocksize");
    }
    if cparams.nthreads < 1 {
        return Err("Invalid thread count");
    }
    if !matches!(
        cparams.splitmode,
        BLOSC_ALWAYS_SPLIT | BLOSC_NEVER_SPLIT | BLOSC_AUTO_SPLIT | BLOSC_FORWARD_COMPAT_SPLIT
    ) {
        return Err("Invalid split mode");
    }
    if !matches!(
        cparams.compcode,
        BLOSC_BLOSCLZ | BLOSC_LZ4 | BLOSC_LZ4HC | BLOSC_ZLIB | BLOSC_ZSTD
    ) {
        return Err("Unsupported codec");
    }
    for &filter in &cparams.filters {
        if !matches!(
            filter,
            BLOSC_NOFILTER | BLOSC_SHUFFLE | BLOSC_BITSHUFFLE | BLOSC_DELTA | BLOSC_TRUNC_PREC
        ) {
            return Err("Unsupported filter");
        }
    }

    Ok(())
}

fn validate_header(header: &ChunkHeader, chunk_len: usize) -> Result<(), &'static str> {
    let header_len = header.header_len();
    if chunk_len < header_len {
        return Err("Chunk too small for header");
    }
    if header.nbytes < 0 {
        return Err("Invalid negative nbytes");
    }
    if header.cbytes < 0 {
        return Err("Invalid negative cbytes");
    }

    let nbytes = header.nbytes as usize;
    let cbytes = header.cbytes as usize;
    if cbytes > chunk_len {
        return Err("Chunk truncated");
    }
    if cbytes < header_len {
        return Err("Invalid compressed size");
    }
    if nbytes > BLOSC2_MAX_BUFFERSIZE as usize {
        return Err("Invalid nbytes");
    }
    if header.special_type() == BLOSC2_NO_SPECIAL {
        if header.use_dict() {
            return Err("Dictionary chunks are not supported");
        }
        if header.blosc2_flags & (BLOSC2_INSTR_CODEC | BLOSC2_LAZY_CHUNK) != 0 {
            return Err("Unsupported chunk flags");
        }
        if header.vl_blocks() {
            return Err("Variable-length blocks are not supported");
        }
    }
    if header.memcpyed() {
        let min_memcpy_len = header_len
            .checked_add(nbytes)
            .ok_or("Invalid memcpyed chunk size")?;
        if cbytes < min_memcpy_len {
            return Err("Invalid memcpyed chunk size");
        }
    }
    if nbytes == 0 {
        return Ok(());
    }
    if header.typesize == 0 || header.typesize as usize > BLOSC_MAX_TYPESIZE {
        return Err("Invalid typesize");
    }
    if header.blocksize <= 0 {
        return Err("Invalid blocksize");
    }
    if !matches!(
        header.compcode(),
        BLOSC_BLOSCLZ | BLOSC_LZ4 | BLOSC_LZ4HC | BLOSC_ZLIB | BLOSC_ZSTD
    ) {
        return Err("Unsupported codec");
    }
    match header.special_type() {
        BLOSC2_SPECIAL_VALUE => {
            let min_special_len = header_len
                .checked_add(header.typesize as usize)
                .ok_or("Invalid special value size")?;
            if cbytes < min_special_len {
                return Err("Invalid special value size");
            }
            if !nbytes.is_multiple_of(header.typesize as usize) {
                return Err("Invalid special value nbytes");
            }
        }
        BLOSC2_SPECIAL_NAN => {
            if !matches!(header.typesize, 4 | 8) || !nbytes.is_multiple_of(header.typesize as usize)
            {
                return Err("Invalid NaN special value size");
            }
        }
        BLOSC2_SPECIAL_ZERO | BLOSC2_SPECIAL_UNINIT | BLOSC2_NO_SPECIAL => {}
        _ => return Err("Unknown special value type"),
    }
    for &filter in &header.filters {
        if !matches!(
            filter,
            BLOSC_NOFILTER | BLOSC_SHUFFLE | BLOSC_BITSHUFFLE | BLOSC_DELTA | BLOSC_TRUNC_PREC
        ) {
            return Err("Unsupported filter");
        }
    }

    let nblocks = nbytes.div_ceil(header.blocksize as usize);
    let min_block_table_len = nblocks
        .checked_mul(4)
        .and_then(|len| header_len.checked_add(len))
        .ok_or("Invalid block table size")?;
    if !header.memcpyed()
        && header.special_type() == BLOSC2_NO_SPECIAL
        && cbytes < min_block_table_len
    {
        return Err("Chunk too small for block table");
    }

    Ok(())
}

fn stream_count(dont_split: bool, is_leftover: bool, typesize: usize, bsize: usize) -> usize {
    if !dont_split
        && !is_leftover
        && typesize > 1
        && bsize >= typesize
        && bsize.is_multiple_of(typesize)
    {
        typesize
    } else {
        1
    }
}

fn ensure_len(buf: &mut Vec<u8>, len: usize) {
    if len > buf.len() {
        buf.resize(len, 0);
    }
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
#[allow(clippy::too_many_arguments)]
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

    let filtered = if filtered_buf == 1 {
        &buf1[..bsize]
    } else {
        &buf2[..bsize]
    };

    // Determine number of streams
    let nstreams = stream_count(dont_split, is_leftover, typesize, bsize);
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
    validate_cparams(cparams, src.len())?;
    let nbytes = src.len() as i32;

    // Handle empty input
    if nbytes == 0 {
        let mut chunk = vec![0u8; BLOSC_EXTENDED_HEADER_LENGTH];
        let header = ChunkHeader {
            version: BLOSC2_VERSION_FORMAT_STABLE,
            versionlz: compcode_to_version(cparams.compcode),
            flags: BLOSC_DOSHUFFLE
                | BLOSC_DOBITSHUFFLE
                | (compcode_to_compformat(cparams.compcode) << 5),
            typesize: cparams.typesize as u8,
            nbytes: 0,
            blocksize: 0,
            cbytes: BLOSC_EXTENDED_HEADER_LENGTH as i32,
            filters: cparams.filters,
            filters_meta: cparams.filters_meta,
            ..Default::default()
        };
        header.try_write(&mut chunk)?;
        return Ok(chunk);
    }

    let typesize = cparams.typesize as usize;
    let blocksize = compute_blocksize(cparams, nbytes) as usize;
    let nblocks = (nbytes as usize).div_ceil(blocksize);

    let filter_flags = compute_filter_flags(&cparams.filters);
    let do_split = should_split(
        cparams.compcode,
        cparams.clevel,
        cparams.splitmode,
        cparams.typesize,
        blocksize as i32,
        filter_flags,
    );
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
    let filters_are_noop = cparams
        .filters
        .iter()
        .all(|&f| f == BLOSC_NOFILTER || (f == BLOSC_SHUFFLE && typesize <= 1));

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
                compress_block(
                    src,
                    &src[start..end],
                    start,
                    blocksize,
                    is_leftover,
                    cparams,
                    dont_split,
                    typesize,
                )
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
            if !block_all_zero {
                all_zero_runs = false;
            }
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
                    block_data,
                    &mut buf1[..bsize],
                    &mut buf2[..bsize],
                    &cparams.filters,
                    &cparams.filters_meta,
                    typesize,
                    block_start,
                    Some(&src[..dref_end]),
                );
                if fb == 1 {
                    &buf1[..bsize]
                } else {
                    &buf2[..bsize]
                }
            };

            let nstreams = stream_count(dont_split, is_leftover, typesize, bsize);
            let neblock = bsize / nstreams;
            let mut block_all_zero_runs = true;

            for stream_idx in 0..nstreams {
                let stream_start = stream_idx * neblock;
                let stream_data = &filtered[stream_start..stream_start + neblock];

                if let Some(val) = get_run(stream_data) {
                    if val == 0 {
                        ensure_len(&mut output, output_pos + 4);
                        output[output_pos..output_pos + 4].copy_from_slice(&0i32.to_le_bytes());
                        output_pos += 4;
                    } else {
                        block_all_zero_runs = false;
                        ensure_len(&mut output, output_pos + 5);
                        output[output_pos..output_pos + 4]
                            .copy_from_slice(&(-(val as i32)).to_le_bytes());
                        output_pos += 4;
                        output[output_pos] = 0x01;
                        output_pos += 1;
                    }
                    continue;
                }

                block_all_zero_runs = false;

                let max_out = neblock + (neblock / 255) + 32;
                ensure_len(&mut output, output_pos + 4 + max_out);
                if max_out > compress_buf.len() {
                    compress_buf.resize(max_out, 0);
                }

                let cbytes = codecs::compress_block(
                    cparams.compcode,
                    cparams.clevel,
                    stream_data,
                    &mut compress_buf[..max_out],
                );

                if cbytes == 0 || cbytes as usize >= neblock {
                    ensure_len(&mut output, output_pos + 4 + neblock);
                    output[output_pos..output_pos + 4]
                        .copy_from_slice(&(neblock as i32).to_le_bytes());
                    output_pos += 4;
                    output[output_pos..output_pos + neblock].copy_from_slice(stream_data);
                    output_pos += neblock;
                } else {
                    let cbytes = cbytes as usize;
                    ensure_len(&mut output, output_pos + 4 + cbytes);
                    output[output_pos..output_pos + 4]
                        .copy_from_slice(&(cbytes as i32).to_le_bytes());
                    output_pos += 4;
                    output[output_pos..output_pos + cbytes]
                        .copy_from_slice(&compress_buf[..cbytes]);
                    output_pos += cbytes;
                }
            }

            if !block_all_zero_runs {
                all_zero_runs = false;
            }
        }
    }

    // Handle special case: all blocks are zero runs
    let mut blosc2_flags = 0u8;
    if all_zero_runs {
        blosc2_flags |= BLOSC2_SPECIAL_ZERO << 4;
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
    header.try_write(&mut output[..BLOSC_EXTENDED_HEADER_LENGTH])?;

    output.truncate(output_pos);
    Ok(output)
}

/// Decompress a single block from chunk data. Returns decompressed block bytes.
#[allow(clippy::too_many_arguments)]
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
    let chunk_limit = header.cbytes as usize;
    let nblocks = header.nblocks();

    // Read block start offset
    let bstart_pos = header_len + block_idx * 4;
    let bstart_end = bstart_pos
        .checked_add(4)
        .ok_or("Invalid block table offset")?;
    if bstart_end > chunk_limit {
        return Err("Chunk too small for bstarts");
    }
    let src_pos_i32 = i32::from_le_bytes(chunk[bstart_pos..bstart_end].try_into().unwrap());
    if src_pos_i32 < 0 {
        return Err("Invalid negative block offset");
    }
    let mut src_pos = src_pos_i32 as usize;
    if src_pos > chunk_limit {
        return Err("Invalid block offset");
    }

    let block_limit = if block_idx + 1 < nblocks {
        let next_bstart_pos = header_len + (block_idx + 1) * 4;
        let next_bstart_end = next_bstart_pos
            .checked_add(4)
            .ok_or("Invalid block table offset")?;
        if next_bstart_end > chunk_limit {
            return Err("Chunk too small for bstarts");
        }
        let next_src_pos_i32 =
            i32::from_le_bytes(chunk[next_bstart_pos..next_bstart_end].try_into().unwrap());
        if next_src_pos_i32 < 0 {
            return Err("Invalid negative block offset");
        }
        let next_src_pos = next_src_pos_i32 as usize;
        if next_src_pos < src_pos || next_src_pos > chunk_limit {
            return Err("Invalid block offset order");
        }
        next_src_pos
    } else {
        chunk_limit
    };

    let nstreams = stream_count(dont_split, is_leftover, typesize, bsize);
    let neblock = bsize / nstreams;

    let mut buf1 = vec![0u8; bsize];
    let mut buf2 = vec![0u8; bsize];

    // Decompress each stream into buf1
    for stream_idx in 0..nstreams {
        let dest_start = stream_idx * neblock;

        let stream_size_end = src_pos.checked_add(4).ok_or("Invalid stream size offset")?;
        if stream_size_end > block_limit {
            return Err("Chunk truncated reading stream size");
        }
        let cbytes = i32::from_le_bytes(chunk[src_pos..stream_size_end].try_into().unwrap());
        src_pos = stream_size_end;

        if cbytes == 0 {
            buf1[dest_start..dest_start + neblock].fill(0);
        } else if cbytes < 0 {
            let val = (-cbytes) as u8;
            if src_pos < block_limit && chunk[src_pos] & 0x01 != 0 {
                buf1[dest_start..dest_start + neblock].fill(val);
                src_pos += 1;
            } else {
                return Err("Invalid run encoding");
            }
        } else if cbytes as usize == neblock {
            let block_end = src_pos
                .checked_add(neblock)
                .ok_or("Invalid memcpyed block size")?;
            if block_end > block_limit {
                return Err("Chunk truncated reading memcpyed block");
            }
            buf1[dest_start..dest_start + neblock].copy_from_slice(&chunk[src_pos..block_end]);
            src_pos = block_end;
        } else {
            let block_end = src_pos
                .checked_add(cbytes as usize)
                .ok_or("Invalid compressed block size")?;
            if block_end > block_limit {
                return Err("Chunk truncated reading compressed block");
            }
            let cdata = &chunk[src_pos..block_end];
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
    if src_pos != block_limit {
        return Err("Invalid block stream length");
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

    let result = if result_buf == 1 {
        &buf1[..bsize]
    } else {
        &buf2[..bsize]
    };
    Ok(result.to_vec())
}

/// Decompress a Blosc2 chunk.
///
/// Returns the decompressed data as a Vec<u8>.
pub fn decompress(chunk: &[u8]) -> Result<Vec<u8>, &'static str> {
    decompress_with_threads(chunk, 1)
}

/// Return `(nbytes, cbytes, blocksize)` from a compressed chunk header.
pub fn cbuffer_sizes(chunk: &[u8]) -> Result<(usize, usize, usize), &'static str> {
    let header = ChunkHeader::read(chunk)?;
    validate_header(&header, chunk.len())?;

    Ok((
        header.nbytes as usize,
        header.cbytes as usize,
        header.blocksize.max(0) as usize,
    ))
}

/// Return `(typesize, compcode, filters)` from a compressed chunk header.
pub fn cbuffer_metainfo(
    chunk: &[u8],
) -> Result<(usize, u8, [u8; BLOSC2_MAX_FILTERS]), &'static str> {
    let header = ChunkHeader::read(chunk)?;
    validate_header(&header, chunk.len())?;

    Ok((header.typesize as usize, header.compcode(), header.filters))
}

/// Validate that a buffer contains a supported compressed chunk.
pub fn cbuffer_validate(chunk: &[u8]) -> Result<(), &'static str> {
    let header = ChunkHeader::read(chunk)?;
    validate_header(&header, chunk.len())
}

/// Extract `nitems` logical items starting at `start` from a compressed chunk.
///
/// `start` and `nitems` are item counts, not byte offsets. This currently
/// decompresses the chunk and slices the result; it is correct but not yet a
/// block-local partial decompressor.
pub fn getitem(chunk: &[u8], start: usize, nitems: usize) -> Result<Vec<u8>, &'static str> {
    let header = ChunkHeader::read(chunk)?;
    validate_header(&header, chunk.len())?;
    let typesize = header.typesize as usize;
    if typesize == 0 {
        return Err("Invalid typesize");
    }

    let byte_start = start.checked_mul(typesize).ok_or("Item range overflow")?;
    let byte_len = nitems.checked_mul(typesize).ok_or("Item range overflow")?;
    let byte_end = byte_start
        .checked_add(byte_len)
        .ok_or("Item range overflow")?;
    if byte_end > header.nbytes as usize {
        return Err("Item range out of bounds");
    }

    let data = decompress(chunk)?;
    Ok(data[byte_start..byte_end].to_vec())
}

/// Blosc1-style compression wrapper using the default BloscLZ codec.
///
/// `doshuffle` accepts `BLOSC_NOFILTER`, `BLOSC_SHUFFLE`, or
/// `BLOSC_BITSHUFFLE`. The compressed chunk is written into `dest`, and the
/// number of bytes written is returned.
pub fn blosc1_compress(
    clevel: u8,
    doshuffle: u8,
    typesize: i32,
    src: &[u8],
    dest: &mut [u8],
) -> Result<usize, &'static str> {
    if !matches!(doshuffle, BLOSC_NOFILTER | BLOSC_SHUFFLE | BLOSC_BITSHUFFLE) {
        return Err("Unsupported Blosc1 shuffle mode");
    }

    let cparams = CParams {
        compcode: BLOSC_BLOSCLZ,
        clevel,
        typesize,
        splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
        filters: [0, 0, 0, 0, 0, doshuffle],
        ..Default::default()
    };
    let compressed = compress(src, &cparams)?;
    if dest.len() < compressed.len() {
        return Err("Destination too small");
    }
    dest[..compressed.len()].copy_from_slice(&compressed);
    Ok(compressed.len())
}

/// Blosc1-style decompression wrapper.
pub fn blosc1_decompress(src: &[u8], dest: &mut [u8]) -> Result<usize, &'static str> {
    let decompressed = decompress(src)?;
    if dest.len() < decompressed.len() {
        return Err("Destination too small");
    }
    dest[..decompressed.len()].copy_from_slice(&decompressed);
    Ok(decompressed.len())
}

/// Decompress a Blosc2 chunk using the specified number of threads.
pub fn decompress_with_threads(chunk: &[u8], nthreads: i16) -> Result<Vec<u8>, &'static str> {
    if nthreads < 1 {
        return Err("Invalid thread count");
    }

    let header = ChunkHeader::read(chunk)?;
    validate_header(&header, chunk.len())?;
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
        return decompress_special(chunk, &header, nbytes);
    }

    // Handle memcpyed chunks
    if header.memcpyed() {
        if chunk.len() >= header_len + nbytes {
            return Ok(chunk[header_len..header_len + nbytes].to_vec());
        }
        return Err("Chunk too small for memcpyed data");
    }

    // Check if delta filter is used (needs sequential block 0 first)
    let has_delta = header.filters.contains(&BLOSC_DELTA);

    let mut output = vec![0u8; nbytes];

    if has_delta {
        // Delta filter requires block 0 decoded first (used as reference)
        // Decode block 0 first
        let block0_end = blocksize.min(nbytes);
        let block0_data = decompress_block_data(
            chunk,
            0,
            0,
            block0_end,
            blocksize,
            nblocks == 1 && block0_end < blocksize,
            &header,
            Some(&output[..blocksize.min(output.len())]),
        )?;
        output[..block0_end].copy_from_slice(&block0_data);

        // Remaining blocks can reference block 0 but must be sequential for delta
        for block_idx in 1..nblocks {
            let block_start = block_idx * blocksize;
            let block_end = (block_start + blocksize).min(nbytes);
            let bsize = block_end - block_start;
            let is_leftover = block_idx == nblocks - 1 && bsize < blocksize;

            let block_data = decompress_block_data(
                chunk,
                block_idx,
                block_start,
                bsize,
                blocksize,
                is_leftover,
                &header,
                Some(&output[..blocksize.min(output.len())]),
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
                    chunk,
                    idx,
                    start,
                    bsize,
                    blocksize,
                    is_leftover,
                    &header,
                    None,
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
                chunk,
                block_idx,
                block_start,
                bsize,
                blocksize,
                is_leftover,
                &header,
                None,
            )?;
            output[block_start..block_end].copy_from_slice(&block_data);
        }
    }

    Ok(output)
}

/// Decompress special-value chunks (all zeros, NaN, repeated value, uninit).
fn decompress_special(
    chunk: &[u8],
    header: &ChunkHeader,
    nbytes: usize,
) -> Result<Vec<u8>, &'static str> {
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
            let typesize = header.typesize as usize;
            let value_start = header.header_len();
            let value_end = value_start
                .checked_add(typesize)
                .ok_or("Invalid special value size")?;
            if value_end > header.cbytes as usize || value_end > chunk.len() {
                return Err("Invalid special value size");
            }
            if !nbytes.is_multiple_of(typesize) {
                return Err("Invalid special value nbytes");
            }

            let repeated = &chunk[value_start..value_end];
            let mut output = vec![0u8; nbytes];
            for item in output.chunks_exact_mut(typesize) {
                item.copy_from_slice(repeated);
            }
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
        let data: Vec<u8> = (0..10000u32).flat_map(|i| i.to_le_bytes()).collect();

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
    fn test_cbuffer_metadata_and_getitem() {
        let data: Vec<u8> = (0..256u32).flat_map(|i| i.to_le_bytes()).collect();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 4,
            splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let compressed = compress(&data, &cparams).unwrap();

        let (nbytes, cbytes, blocksize) = cbuffer_sizes(&compressed).unwrap();
        assert_eq!(nbytes, data.len());
        assert_eq!(cbytes, compressed.len());
        assert!(blocksize > 0);

        let (typesize, compcode, filters) = cbuffer_metainfo(&compressed).unwrap();
        assert_eq!(typesize, 4);
        assert_eq!(compcode, BLOSC_LZ4);
        assert_eq!(filters, cparams.filters);
        assert!(cbuffer_validate(&compressed).is_ok());

        let items = getitem(&compressed, 10, 20).unwrap();
        assert_eq!(items, data[10 * 4..30 * 4]);
        assert!(getitem(&compressed, 250, 10).is_err());

        let mut truncated = compressed.clone();
        truncated.truncate(truncated.len() - 1);
        assert!(cbuffer_validate(&truncated).is_err());
    }

    #[test]
    fn test_blosc1_wrappers_roundtrip_and_validate_buffers() {
        let data: Vec<u8> = (0..1024u32).flat_map(|i| i.to_le_bytes()).collect();
        let mut compressed = vec![0u8; data.len() + BLOSC2_MAX_OVERHEAD + 1024];
        let csize = blosc1_compress(5, BLOSC_SHUFFLE, 4, &data, &mut compressed).unwrap();
        assert!(csize > 0);

        let (nbytes, _, _) = cbuffer_sizes(&compressed[..csize]).unwrap();
        assert_eq!(nbytes, data.len());

        let mut restored = vec![0u8; data.len()];
        let dsize = blosc1_decompress(&compressed[..csize], &mut restored).unwrap();
        assert_eq!(dsize, data.len());
        assert_eq!(restored, data);

        let mut short_compressed = vec![0u8; 8];
        assert!(blosc1_compress(5, BLOSC_SHUFFLE, 4, &data, &mut short_compressed).is_err());

        let mut short_restored = vec![0u8; data.len() - 1];
        assert!(blosc1_decompress(&compressed[..csize], &mut short_restored).is_err());
        assert!(blosc1_compress(5, BLOSC_DELTA, 4, &data, &mut compressed).is_err());
    }

    #[test]
    fn test_compress_all_codecs() {
        let data: Vec<u8> = b"Test data for compression with various codecs and filters! "
            .iter()
            .cycle()
            .take(50000)
            .copied()
            .collect();

        for compcode in [
            BLOSC_BLOSCLZ,
            BLOSC_LZ4,
            BLOSC_LZ4HC,
            BLOSC_ZLIB,
            BLOSC_ZSTD,
        ] {
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
            assert_eq!(
                data, decompressed,
                "Roundtrip failed for compcode={compcode}"
            );
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
    fn test_invalid_compression_params_return_errors() {
        let data = [1u8, 2, 3, 4];

        for typesize in [0, -1, BLOSC_MAX_TYPESIZE as i32 + 1] {
            let cparams = CParams {
                typesize,
                ..Default::default()
            };
            assert!(compress(&data, &cparams).is_err());
        }

        let bad_cases = [
            CParams {
                clevel: 10,
                ..Default::default()
            },
            CParams {
                blocksize: -1,
                ..Default::default()
            },
            CParams {
                splitmode: 99,
                ..Default::default()
            },
            CParams {
                compcode: 99,
                ..Default::default()
            },
            CParams {
                filters: [0, 0, 0, 0, 0, 99],
                ..Default::default()
            },
            CParams {
                nthreads: 0,
                ..Default::default()
            },
        ];

        for cparams in bad_cases {
            assert!(compress(&data, &cparams).is_err());
        }
    }

    #[test]
    fn test_malformed_headers_return_errors() {
        let data: Vec<u8> = (0..4096u32).flat_map(|i| i.to_le_bytes()).collect();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 4,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let chunk = compress(&data, &cparams).unwrap();

        let mut negative_nbytes = chunk.clone();
        negative_nbytes[4..8].copy_from_slice(&(-1i32).to_le_bytes());
        assert!(decompress(&negative_nbytes).is_err());

        let mut zero_blocksize = chunk.clone();
        zero_blocksize[8..12].copy_from_slice(&0i32.to_le_bytes());
        assert!(decompress(&zero_blocksize).is_err());

        let mut unsupported_filter = chunk.clone();
        unsupported_filter[BLOSC2_CHUNK_FILTER_CODES + 5] = 99;
        assert!(decompress(&unsupported_filter).is_err());

        let mut bad_nan_special = chunk.clone();
        bad_nan_special[BLOSC2_CHUNK_TYPESIZE] = 2;
        bad_nan_special[BLOSC2_CHUNK_BLOSC2_FLAGS] = BLOSC2_SPECIAL_NAN << 4;
        assert!(decompress(&bad_nan_special).is_err());

        for (flag_offset, flag) in [
            (BLOSC2_CHUNK_BLOSC2_FLAGS, BLOSC2_USEDICT),
            (BLOSC2_CHUNK_BLOSC2_FLAGS, BLOSC2_INSTR_CODEC),
            (BLOSC2_CHUNK_BLOSC2_FLAGS, BLOSC2_LAZY_CHUNK),
            (BLOSC2_CHUNK_BLOSC2_FLAGS2, BLOSC2_VL_BLOCKS),
        ] {
            let mut unsupported = chunk.clone();
            unsupported[flag_offset] |= flag;
            assert!(decompress(&unsupported).is_err());
        }

        let mut oversized_cbytes = chunk.clone();
        oversized_cbytes[12..16].copy_from_slice(&((chunk.len() + 1) as i32).to_le_bytes());
        assert!(decompress(&oversized_cbytes).is_err());

        let mut understated_cbytes = chunk.clone();
        understated_cbytes[12..16].copy_from_slice(&((chunk.len() - 1) as i32).to_le_bytes());
        assert!(decompress(&understated_cbytes).is_err());

        let mut negative_bstart = chunk.clone();
        negative_bstart[BLOSC_EXTENDED_HEADER_LENGTH..BLOSC_EXTENDED_HEADER_LENGTH + 4]
            .copy_from_slice(&(-1i32).to_le_bytes());
        assert!(decompress(&negative_bstart).is_err());

        let data_two_blocks: Vec<u8> = (0..4096u32).flat_map(|i| i.to_le_bytes()).collect();
        let cparams_two_blocks = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 4,
            blocksize: 4096,
            filters: [0, 0, 0, 0, 0, BLOSC_NOFILTER],
            ..Default::default()
        };
        let mut bad_block_boundary = compress(&data_two_blocks, &cparams_two_blocks).unwrap();
        let second_bstart_pos = BLOSC_EXTENDED_HEADER_LENGTH + 4;
        let second_bstart = i32::from_le_bytes(
            bad_block_boundary[second_bstart_pos..second_bstart_pos + 4]
                .try_into()
                .unwrap(),
        );
        bad_block_boundary[second_bstart_pos..second_bstart_pos + 4]
            .copy_from_slice(&(second_bstart + 1).to_le_bytes());
        assert!(decompress(&bad_block_boundary).is_err());

        let payload = [1u8, 2, 3, 4];
        let mut bad_memcpyed = vec![0u8; BLOSC_EXTENDED_HEADER_LENGTH + payload.len()];
        let header = ChunkHeader {
            version: BLOSC2_VERSION_FORMAT_STABLE,
            versionlz: 1,
            flags: BLOSC_DOSHUFFLE | BLOSC_MEMCPYED | BLOSC_DOBITSHUFFLE,
            typesize: 1,
            nbytes: payload.len() as i32,
            blocksize: payload.len() as i32,
            cbytes: (BLOSC_EXTENDED_HEADER_LENGTH + payload.len() - 1) as i32,
            filters: [0, 0, 0, 0, 0, BLOSC_NOFILTER],
            ..Default::default()
        };
        header.write(&mut bad_memcpyed[..BLOSC_EXTENDED_HEADER_LENGTH]);
        bad_memcpyed[BLOSC_EXTENDED_HEADER_LENGTH..].copy_from_slice(&payload);
        assert!(decompress(&bad_memcpyed).is_err());

        assert!(decompress_with_threads(&chunk, 0).is_err());
    }

    #[test]
    fn test_special_value_repeats_payload() {
        let repeated = 0xA1B2C3D4u32.to_le_bytes();
        let typesize = repeated.len();
        let nitems = 10usize;
        let mut chunk = vec![0u8; BLOSC_EXTENDED_HEADER_LENGTH + typesize];
        let header = ChunkHeader {
            version: BLOSC2_VERSION_FORMAT_STABLE,
            versionlz: 1,
            flags: BLOSC_DOSHUFFLE | BLOSC_DOBITSHUFFLE,
            typesize: typesize as u8,
            nbytes: (nitems * typesize) as i32,
            blocksize: (nitems * typesize) as i32,
            cbytes: chunk.len() as i32,
            filters: [0, 0, 0, 0, 0, BLOSC_NOFILTER],
            blosc2_flags: BLOSC2_SPECIAL_VALUE << 4,
            ..Default::default()
        };
        header.write(&mut chunk[..BLOSC_EXTENDED_HEADER_LENGTH]);
        chunk[BLOSC_EXTENDED_HEADER_LENGTH..].copy_from_slice(&repeated);

        let decompressed = decompress(&chunk).unwrap();
        for item in decompressed.chunks_exact(typesize) {
            assert_eq!(item, repeated);
        }

        let mut truncated = chunk.clone();
        truncated[12..16].copy_from_slice(&(BLOSC_EXTENDED_HEADER_LENGTH as i32).to_le_bytes());
        assert!(decompress(&truncated).is_err());
    }

    #[test]
    fn test_truncated_compressed_stream_returns_error() {
        let data: Vec<u8> = (0..10000u32).flat_map(|i| i.to_le_bytes()).collect();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 4,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut chunk = compress(&data, &cparams).unwrap();
        chunk.pop();
        let truncated_len = chunk.len() as i32;
        chunk[12..16].copy_from_slice(&truncated_len.to_le_bytes());

        assert!(decompress(&chunk).is_err());
    }

    #[test]
    fn test_always_split_small_block_roundtrip() {
        let data = [1u8, 2, 3];
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 8,
            splitmode: BLOSC_ALWAYS_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_NOFILTER],
            ..Default::default()
        };

        let compressed = compress(&data, &cparams).unwrap();
        let decompressed = decompress(&compressed).unwrap();
        assert_eq!(data, decompressed.as_slice());
    }

    #[test]
    fn test_always_split_run_streams_can_grow_output() {
        let data = vec![7u8; 255];
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 255,
            blocksize: 255,
            splitmode: BLOSC_ALWAYS_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_NOFILTER],
            ..Default::default()
        };

        let compressed = compress(&data, &cparams).unwrap();
        let decompressed = decompress(&compressed).unwrap();
        assert_eq!(data, decompressed);
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
        let data: Vec<u8> = (0..20000u16).flat_map(|i| i.to_le_bytes()).collect();

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
            assert_eq!(
                data, decompressed,
                "Roundtrip failed for typesize={typesize}"
            );
        }
    }

    #[test]
    fn test_multithreaded_compress() {
        let data: Vec<u8> = (0..100000u32).flat_map(|i| i.to_le_bytes()).collect();

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
        let data: Vec<u8> = (0..50000u32).flat_map(|i| i.to_le_bytes()).collect();

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
        assert_eq!(
            c1, c4,
            "Multi-threaded compress should match single-threaded"
        );

        let d1 = decompress(&c1).unwrap();
        let d4 = decompress_with_threads(&c4, 4).unwrap();
        assert_eq!(d1, d4);
        assert_eq!(data, d1);
    }

    #[test]
    fn test_repeated_compress_decompress_cycles() {
        for iteration in 0..200u32 {
            let data: Vec<u8> = (0..4096u32)
                .flat_map(|i| i.wrapping_mul(31).wrapping_add(iteration).to_le_bytes())
                .collect();
            let cparams = CParams {
                compcode: match iteration % 4 {
                    0 => BLOSC_BLOSCLZ,
                    1 => BLOSC_LZ4,
                    2 => BLOSC_ZLIB,
                    _ => BLOSC_ZSTD,
                },
                clevel: (iteration % 10) as u8,
                typesize: 4,
                splitmode: match iteration % 3 {
                    0 => BLOSC_ALWAYS_SPLIT,
                    1 => BLOSC_NEVER_SPLIT,
                    _ => BLOSC_FORWARD_COMPAT_SPLIT,
                },
                filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
                ..Default::default()
            };

            let compressed = compress(&data, &cparams).unwrap();
            let restored = decompress(&compressed).unwrap();
            assert_eq!(restored, data, "cycle {iteration} failed");
        }
    }

    #[test]
    fn test_parallel_thread_safety_roundtrips() {
        let handles: Vec<_> = (0..8u32)
            .map(|thread_id| {
                std::thread::spawn(move || {
                    for iteration in 0..50u32 {
                        let data: Vec<u8> = (0..2048u32)
                            .flat_map(|i| {
                                i.wrapping_mul(17)
                                    .wrapping_add(thread_id * 1000 + iteration)
                                    .to_le_bytes()
                            })
                            .collect();
                        let cparams = CParams {
                            compcode: if iteration % 2 == 0 {
                                BLOSC_LZ4
                            } else {
                                BLOSC_ZSTD
                            },
                            clevel: 5,
                            typesize: 4,
                            nthreads: 4,
                            splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
                            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
                            ..Default::default()
                        };

                        let compressed = compress(&data, &cparams).unwrap();
                        let restored = decompress_with_threads(&compressed, 4).unwrap();
                        assert_eq!(restored, data);
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().expect("worker thread panicked");
        }
    }
}
