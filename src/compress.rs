use crate::codecs;
use crate::constants::*;
use crate::filters;
use crate::header::ChunkHeader;
use rayon::prelude::*;

pub(crate) fn with_thread_pool<T: Send>(nthreads: i16, op: impl FnOnce() -> T + Send) -> T {
    if nthreads <= 1 {
        return op();
    }
    match rayon::ThreadPoolBuilder::new()
        .num_threads(nthreads as usize)
        .build()
    {
        Ok(pool) => pool.install(op),
        Err(_) => op(),
    }
}

/// Compression parameters.
#[derive(Debug, Clone)]
pub struct CParams {
    /// Codec identifier, such as `BLOSC_LZ4` or `BLOSC_ZSTD`.
    pub compcode: u8,
    /// Per-codec metadata byte stored in the extended chunk header.
    pub compcode_meta: u8,
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
    /// Train and embed a per-chunk codec dictionary when supported.
    pub use_dict: bool,
    /// Number of worker threads for block-parallel compression.
    pub nthreads: i16,
}

impl Default for CParams {
    fn default() -> Self {
        CParams {
            compcode: BLOSC_BLOSCLZ,
            compcode_meta: 0,
            clevel: 5,
            typesize: 8,
            blocksize: 0,
            splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            filters_meta: [0; BLOSC2_MAX_FILTERS],
            use_dict: false,
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
    ) && !codecs::is_registered_codec(cparams.compcode)
    {
        return Err("Unsupported codec");
    }
    #[cfg(not(feature = "lz4hc-sys"))]
    if cparams.compcode == BLOSC_LZ4HC {
        return Err("LZ4HC compression requires the lz4hc-sys feature");
    }
    if cparams.use_dict && cparams.compcode != BLOSC_ZSTD {
        return Err("Dictionary compression is only supported for Zstd");
    }
    for &filter in &cparams.filters {
        if !matches!(
            filter,
            BLOSC_NOFILTER | BLOSC_SHUFFLE | BLOSC_BITSHUFFLE | BLOSC_DELTA | BLOSC_TRUNC_PREC
        ) && !filters::is_registered_filter(filter)
        {
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
        if header.use_dict() && header.compcode() != BLOSC_ZSTD {
            return Err("Dictionary chunks are only supported for Zstd");
        }
        if header.blosc2_flags & (BLOSC2_INSTR_CODEC | BLOSC2_LAZY_CHUNK) != 0 {
            return Err("Unsupported chunk flags");
        }
        if header.vl_blocks() {
            if header.version != BLOSC2_VERSION_FORMAT_VL_BLOCKS {
                return Err("Invalid VL-block chunk version");
            }
            if header.blocksize <= 0 {
                return Err("Invalid VL-block count");
            }
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
    ) && !codecs::is_registered_codec(header.compcode())
    {
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
        ) && !filters::is_registered_filter(filter)
        {
            return Err("Unsupported filter");
        }
    }

    if header.vl_blocks() {
        let nblocks = header.blocksize as usize;
        let min_block_table_len = nblocks
            .checked_mul(4)
            .and_then(|len| header_len.checked_add(len))
            .ok_or("Invalid VL-block table size")?;
        if cbytes < min_block_table_len {
            return Err("Chunk too small for VL-block table");
        }
        if header.use_dict()
            && cbytes
                < min_block_table_len
                    .checked_add(4)
                    .ok_or("Invalid dictionary size")?
        {
            return Err("Chunk too small for dictionary size");
        }
        return Ok(());
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
    if header.use_dict()
        && header.special_type() == BLOSC2_NO_SPECIAL
        && !header.memcpyed()
        && cbytes
            < min_block_table_len
                .checked_add(4)
                .ok_or("Invalid dictionary size")?
    {
        return Err("Chunk too small for dictionary size");
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

fn udcompcode_for_header(compcode: u8) -> u8 {
    if compcode_to_compformat(compcode) == BLOSC_UDCODEC_FORMAT {
        compcode
    } else {
        0
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
    let filters_are_noop = cparams
        .filters
        .iter()
        .all(|&f| f == BLOSC_NOFILTER || (f == BLOSC_SHUFFLE && typesize <= 1));
    if filters_are_noop {
        return compress_pre_filtered_block(
            block_data,
            cparams,
            dont_split,
            typesize,
            is_leftover,
            None,
        );
    }

    if let Some(shuffle_typesize) =
        single_shuffle_filter(&cparams.filters, &cparams.filters_meta, typesize)
    {
        let mut filtered = vec![0u8; bsize];
        filters::shuffle(shuffle_typesize, block_data, &mut filtered);
        return compress_pre_filtered_block(
            &filtered,
            cparams,
            dont_split,
            typesize,
            is_leftover,
            None,
        );
    }

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

    compress_pre_filtered_block(filtered, cparams, dont_split, typesize, is_leftover, None)
}

fn single_shuffle_filter(
    filters: &[u8; BLOSC2_MAX_FILTERS],
    filters_meta: &[u8; BLOSC2_MAX_FILTERS],
    typesize: usize,
) -> Option<usize> {
    let mut shuffle_typesize = None;
    for (idx, &filter) in filters.iter().enumerate() {
        if filter == BLOSC_NOFILTER {
            continue;
        }
        if filter != BLOSC_SHUFFLE || shuffle_typesize.is_some() {
            return None;
        }
        let ts = if filters_meta[idx] == 0 {
            typesize
        } else {
            filters_meta[idx] as usize
        };
        if ts <= 1 {
            return None;
        }
        shuffle_typesize = Some(ts);
    }
    shuffle_typesize
}

fn compress_pre_filtered_block(
    filtered: &[u8],
    cparams: &CParams,
    dont_split: bool,
    typesize: usize,
    is_leftover: bool,
    dict: Option<&[u8]>,
) -> (Vec<u8>, bool) {
    let bsize = filtered.len();
    // Determine number of streams
    let nstreams = stream_count(dont_split, is_leftover, typesize, bsize);
    let neblock = bsize / nstreams;

    let mut result = Vec::with_capacity(bsize);
    let mut all_zero_runs = true;
    let max_out = neblock + (neblock / 255) + 32;
    let mut compressed = vec![0u8; max_out];

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

        let cbytes = match dict {
            Some(dict) => codecs::compress_block_with_dict(
                cparams.compcode,
                cparams.clevel,
                stream_data,
                &mut compressed,
                dict,
            ),
            None => codecs::compress_block_with_meta(
                cparams.compcode,
                cparams.clevel,
                cparams.compcode_meta,
                stream_data,
                &mut compressed,
            ),
        };

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

fn filtered_blocks_for_dict(
    src: &[u8],
    cparams: &CParams,
    blocksize: usize,
    nblocks: usize,
    typesize: usize,
    filters_are_noop: bool,
) -> Vec<Vec<u8>> {
    let dref_end = blocksize.min(src.len());
    (0..nblocks)
        .map(|block_idx| {
            let block_start = block_idx * blocksize;
            let block_end = (block_start + blocksize).min(src.len());
            let bsize = block_end - block_start;
            let block_data = &src[block_start..block_end];
            if filters_are_noop {
                return block_data.to_vec();
            }

            if let Some(shuffle_typesize) =
                single_shuffle_filter(&cparams.filters, &cparams.filters_meta, typesize)
            {
                let mut filtered = vec![0u8; bsize];
                filters::shuffle(shuffle_typesize, block_data, &mut filtered);
                return filtered;
            }

            let mut buf1 = vec![0u8; bsize];
            let mut buf2 = vec![0u8; bsize];
            let fb = filters::pipeline_forward(
                block_data,
                &mut buf1,
                &mut buf2,
                &cparams.filters,
                &cparams.filters_meta,
                typesize,
                block_start,
                Some(&src[..dref_end]),
            );
            if fb == 1 {
                buf1
            } else {
                buf2
            }
        })
        .collect()
}

fn train_zstd_dict(samples: &[Vec<u8>], nbytes: usize) -> Option<Vec<u8>> {
    let dict_maxsize = BLOSC2_MAXDICTSIZE.min(nbytes / 20);
    if dict_maxsize < BLOSC2_MINUSEFULDICT || samples.is_empty() {
        return None;
    }

    let mut sample_data = Vec::with_capacity(samples.iter().map(Vec::len).sum());
    let mut sample_sizes = Vec::with_capacity(samples.len());
    for sample in samples {
        if sample.is_empty() {
            return None;
        }
        sample_sizes.push(sample.len());
        sample_data.extend_from_slice(sample);
    }

    zstd::dict::from_continuous(&sample_data, &sample_sizes, dict_maxsize).ok()
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
            udcompcode: udcompcode_for_header(cparams.compcode),
            compcode_meta: cparams.compcode_meta,
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

    if cparams.use_dict && cparams.compcode == BLOSC_ZSTD && cparams.clevel > 0 {
        let filtered_blocks =
            filtered_blocks_for_dict(src, cparams, blocksize, nblocks, typesize, filters_are_noop);
        if let Some(dict) = train_zstd_dict(&filtered_blocks, nbytes as usize) {
            let dict_section_len = 4 + dict.len();
            let max_compressed =
                nbytes as usize + header_len + bstarts_len + dict_section_len + nblocks * 32;
            let mut output = vec![0u8; max_compressed];
            let mut output_pos = header_len + bstarts_len;

            output[output_pos..output_pos + 4].copy_from_slice(&(dict.len() as i32).to_le_bytes());
            output_pos += 4;
            output[output_pos..output_pos + dict.len()].copy_from_slice(&dict);
            output_pos += dict.len();

            let mut all_zero_runs = true;
            for (block_idx, filtered) in filtered_blocks.iter().enumerate() {
                let block_start = block_idx * blocksize;
                let block_end = (block_start + blocksize).min(nbytes as usize);
                let bsize = block_end - block_start;
                let is_leftover = block_idx == nblocks - 1 && bsize < blocksize;

                let bstart_offset = header_len + block_idx * 4;
                output[bstart_offset..bstart_offset + 4]
                    .copy_from_slice(&(output_pos as i32).to_le_bytes());

                let (block_data, block_all_zero) = compress_pre_filtered_block(
                    filtered,
                    cparams,
                    dont_split,
                    typesize,
                    is_leftover,
                    Some(&dict),
                );
                ensure_len(&mut output, output_pos + block_data.len());
                output[output_pos..output_pos + block_data.len()].copy_from_slice(&block_data);
                output_pos += block_data.len();
                if !block_all_zero {
                    all_zero_runs = false;
                }
            }

            let mut blosc2_flags = BLOSC2_USEDICT;
            if all_zero_runs {
                blosc2_flags = BLOSC2_SPECIAL_ZERO << 4;
                output_pos = header_len;
            }

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
                udcompcode: udcompcode_for_header(cparams.compcode),
                compcode_meta: cparams.compcode_meta,
                blosc2_flags,
                ..Default::default()
            };
            header.try_write(&mut output[..BLOSC_EXTENDED_HEADER_LENGTH])?;

            output.truncate(output_pos);
            return Ok(output);
        }
    }

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

        let compressed_blocks: Vec<(Vec<u8>, bool)> = with_thread_pool(cparams.nthreads, || {
            block_infos
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
                .collect()
        });

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
        output = Vec::with_capacity(max_compressed);
        output.resize(header_len + bstarts_len, 0);
        output_pos = header_len + bstarts_len;
        all_zero_runs = true;

        let mut buf1 = vec![0u8; blocksize];
        let single_shuffle =
            single_shuffle_filter(&cparams.filters, &cparams.filters_meta, typesize);
        let mut buf2 = if single_shuffle.is_some() {
            Vec::new()
        } else {
            vec![0u8; blocksize]
        };
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
            } else if let Some(shuffle_typesize) = single_shuffle {
                filters::shuffle(shuffle_typesize, block_data, &mut buf1[..bsize]);
                &buf1[..bsize]
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

                let cbytes = codecs::compress_block_with_meta(
                    cparams.compcode,
                    cparams.clevel,
                    cparams.compcode_meta,
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
        udcompcode: udcompcode_for_header(cparams.compcode),
        compcode_meta: cparams.compcode_meta,
        blosc2_flags,
        ..Default::default()
    };
    header.try_write(&mut output[..BLOSC_EXTENDED_HEADER_LENGTH])?;

    output.truncate(output_pos);
    Ok(output)
}

pub fn compress_many(buffers: &[&[u8]], cparams: &CParams) -> Result<Vec<Vec<u8>>, &'static str> {
    if buffers.len() > 1 && cparams.nthreads > 1 {
        let per_chunk_params = CParams {
            nthreads: 1,
            ..cparams.clone()
        };
        with_thread_pool(cparams.nthreads, || {
            buffers
                .par_iter()
                .map(|buffer| compress(buffer, &per_chunk_params))
                .collect()
        })
    } else {
        buffers
            .iter()
            .map(|buffer| compress(buffer, cparams))
            .collect()
    }
}

fn validate_vl_inputs(blocks: &[&[u8]], cparams: &CParams) -> Result<usize, &'static str> {
    if blocks.is_empty() {
        return Err("VL-block input cannot be empty");
    }
    if blocks.len() > i32::MAX as usize {
        return Err("Too many VL-blocks");
    }
    if cparams.use_dict {
        if cparams.compcode != BLOSC_ZSTD {
            return Err("Dictionary VL-block chunks are only supported for Zstd");
        }
        if cparams.clevel == 0 {
            return Err("Dictionary VL-block chunks require compression");
        }
    }
    if cparams.filters.contains(&BLOSC_DELTA) {
        return Err("VL-block compression does not support delta filters");
    }

    let mut total = 0usize;
    for block in blocks {
        if block.is_empty() {
            return Err("VL-blocks cannot be empty");
        }
        total = total
            .checked_add(block.len())
            .ok_or("VL-block input too large")?;
    }
    validate_cparams(cparams, total)?;
    Ok(total)
}

fn filtered_vl_blocks(blocks: &[&[u8]], cparams: &CParams) -> Vec<Vec<u8>> {
    let typesize = cparams.typesize as usize;
    let filters_are_noop = cparams
        .filters
        .iter()
        .all(|&f| f == BLOSC_NOFILTER || (f == BLOSC_SHUFFLE && typesize <= 1));
    blocks
        .iter()
        .map(|block| {
            if filters_are_noop {
                return block.to_vec();
            }

            if let Some(shuffle_typesize) =
                single_shuffle_filter(&cparams.filters, &cparams.filters_meta, typesize)
            {
                let mut filtered = vec![0u8; block.len()];
                filters::shuffle(shuffle_typesize, block, &mut filtered);
                return filtered;
            }

            let mut buf1 = vec![0u8; block.len()];
            let mut buf2 = vec![0u8; block.len()];
            let selected = filters::pipeline_forward(
                block,
                &mut buf1,
                &mut buf2,
                &cparams.filters,
                &cparams.filters_meta,
                typesize,
                0,
                None,
            );
            if selected == 1 {
                buf1
            } else {
                buf2
            }
        })
        .collect()
}

fn compress_filtered_vl_block(filtered: &[u8], cparams: &CParams, dict: Option<&[u8]>) -> Vec<u8> {
    let max_out = filtered.len() + (filtered.len() / 255) + 32;
    let mut compressed = vec![0u8; max_out];
    let cbytes = match dict {
        Some(dict) => codecs::compress_block_with_dict(
            cparams.compcode,
            cparams.clevel,
            filtered,
            &mut compressed[..max_out],
            dict,
        ),
        None => codecs::compress_block_with_meta(
            cparams.compcode,
            cparams.clevel,
            cparams.compcode_meta,
            filtered,
            &mut compressed[..max_out],
        ),
    };

    let mut out = Vec::with_capacity(4 + filtered.len());
    out.extend_from_slice(&(filtered.len() as i32).to_le_bytes());
    if cparams.clevel == 0 || cbytes <= 0 || cbytes as usize >= filtered.len() {
        out.extend_from_slice(filtered);
    } else {
        out.extend_from_slice(&compressed[..cbytes as usize]);
    }
    out
}

fn compress_vl_block(block: &[u8], cparams: &CParams) -> Vec<u8> {
    let filtered = filtered_vl_blocks(&[block], cparams);
    compress_filtered_vl_block(&filtered[0], cparams, None)
}

/// Compress independent variable-length blocks into one Blosc2 VL-block chunk.
///
/// Each VL block is filtered and compressed independently with block offset 0.
/// Dictionary mode is Zstd-only.
pub fn vlcompress(blocks: &[&[u8]], cparams: &CParams) -> Result<Vec<u8>, &'static str> {
    let total_nbytes = validate_vl_inputs(blocks, cparams)?;
    let header_len = BLOSC_EXTENDED_HEADER_LENGTH;
    let bstarts_len = blocks.len() * 4;

    let mut flags = BLOSC_DOSHUFFLE | BLOSC_DOBITSHUFFLE;
    flags |= compcode_to_compformat(cparams.compcode) << 5;
    flags |= BLOSC_DONT_SPLIT;

    let filtered_blocks = if cparams.use_dict {
        Some(filtered_vl_blocks(blocks, cparams))
    } else {
        None
    };
    let dict = filtered_blocks
        .as_ref()
        .and_then(|filtered| train_zstd_dict(filtered, total_nbytes));
    let dict = dict.as_deref();

    let compressed_blocks: Vec<Vec<u8>> = match (filtered_blocks.as_ref(), dict) {
        (Some(filtered_blocks), Some(dict)) if cparams.nthreads > 1 && blocks.len() > 1 => {
            with_thread_pool(cparams.nthreads, || {
                filtered_blocks
                    .par_iter()
                    .map(|block| compress_filtered_vl_block(block, cparams, Some(dict)))
                    .collect()
            })
        }
        (Some(filtered_blocks), Some(dict)) => filtered_blocks
            .iter()
            .map(|block| compress_filtered_vl_block(block, cparams, Some(dict)))
            .collect(),
        _ if cparams.nthreads > 1 && blocks.len() > 1 => with_thread_pool(cparams.nthreads, || {
            blocks
                .par_iter()
                .map(|block| compress_vl_block(block, cparams))
                .collect()
        }),
        _ => blocks
            .iter()
            .map(|block| compress_vl_block(block, cparams))
            .collect(),
    };

    let dict_section_len = dict.map_or(0, |dict| 4 + dict.len());

    let total_cbytes = compressed_blocks.iter().try_fold(
        header_len + bstarts_len + dict_section_len,
        |acc, block| {
            acc.checked_add(block.len())
                .ok_or("VL-block chunk too large")
        },
    )?;
    if total_cbytes > i32::MAX as usize {
        return Err("VL-block chunk too large");
    }

    let mut output = vec![0u8; total_cbytes];
    let mut output_pos = header_len + bstarts_len;
    if let Some(dict) = dict {
        output[output_pos..output_pos + 4].copy_from_slice(&(dict.len() as i32).to_le_bytes());
        output_pos += 4;
        output[output_pos..output_pos + dict.len()].copy_from_slice(dict);
        output_pos += dict.len();
    }
    for (idx, block) in compressed_blocks.iter().enumerate() {
        let bstart_offset = header_len + idx * 4;
        output[bstart_offset..bstart_offset + 4]
            .copy_from_slice(&(output_pos as i32).to_le_bytes());
        output[output_pos..output_pos + block.len()].copy_from_slice(block);
        output_pos += block.len();
    }

    let header = ChunkHeader {
        version: BLOSC2_VERSION_FORMAT_VL_BLOCKS,
        versionlz: compcode_to_version(cparams.compcode),
        flags,
        typesize: cparams.typesize as u8,
        nbytes: total_nbytes as i32,
        blocksize: blocks.len() as i32,
        cbytes: total_cbytes as i32,
        filters: cparams.filters,
        filters_meta: cparams.filters_meta,
        udcompcode: udcompcode_for_header(cparams.compcode),
        compcode_meta: cparams.compcode_meta,
        blosc2_flags: if dict.is_some() { BLOSC2_USEDICT } else { 0 },
        blosc2_flags2: BLOSC2_VL_BLOCKS,
    };
    header.try_write(&mut output[..BLOSC_EXTENDED_HEADER_LENGTH])?;
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
    dict: Option<&[u8]>,
) -> Result<Vec<u8>, &'static str> {
    let typesize = header.typesize as usize;
    let dont_split = header.dont_split();
    let compcode = header.compcode();
    let header_len = header.header_len();
    let chunk_limit = header.cbytes as usize;
    let nblocks = if header.vl_blocks() {
        header.blocksize as usize
    } else {
        header.nblocks()
    };

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
    if let Some(dict) = dict {
        let min_block_start = header_len
            .checked_add(nblocks.checked_mul(4).ok_or("Invalid block table size")?)
            .and_then(|pos| pos.checked_add(4))
            .and_then(|pos| pos.checked_add(dict.len()))
            .ok_or("Invalid dictionary size")?;
        if src_pos < min_block_start {
            return Err("Invalid dictionary block offset");
        }
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
            let dsize = match dict {
                Some(dict) => codecs::decompress_block_with_dict(
                    compcode,
                    cdata,
                    &mut buf1[dest_start..dest_start + neblock],
                    dict,
                ),
                None => codecs::decompress_block_with_meta(
                    compcode,
                    header.compcode_meta,
                    cdata,
                    &mut buf1[dest_start..dest_start + neblock],
                ),
            };
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

fn embedded_dictionary<'a>(
    chunk: &'a [u8],
    header: &ChunkHeader,
) -> Result<Option<&'a [u8]>, &'static str> {
    if !header.use_dict() {
        return Ok(None);
    }

    let nblocks = if header.vl_blocks() {
        header.blocksize as usize
    } else {
        header.nblocks()
    };
    let dict_size_pos = header
        .header_len()
        .checked_add(nblocks.checked_mul(4).ok_or("Invalid block table size")?)
        .ok_or("Invalid dictionary offset")?;
    let dict_size_end = dict_size_pos
        .checked_add(4)
        .ok_or("Invalid dictionary offset")?;
    if dict_size_end > header.cbytes as usize || dict_size_end > chunk.len() {
        return Err("Chunk too small for dictionary size");
    }

    let dict_size = i32::from_le_bytes(chunk[dict_size_pos..dict_size_end].try_into().unwrap());
    if dict_size <= 0 || dict_size as usize > BLOSC2_MAXDICTSIZE {
        return Err("Invalid dictionary size");
    }
    let dict_end = dict_size_end
        .checked_add(dict_size as usize)
        .ok_or("Invalid dictionary size")?;
    if dict_end > header.cbytes as usize || dict_end > chunk.len() {
        return Err("Chunk too small for dictionary");
    }

    Ok(Some(&chunk[dict_size_end..dict_end]))
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

fn vl_block_span<'a>(
    chunk: &'a [u8],
    header: &ChunkHeader,
    nblock: usize,
) -> Result<&'a [u8], &'static str> {
    let nblocks = header.blocksize as usize;
    if nblock >= nblocks {
        return Err("VL-block index out of range");
    }
    let header_len = header.header_len();
    let bstart_pos = header_len
        .checked_add(nblock.checked_mul(4).ok_or("Invalid VL-block table size")?)
        .ok_or("Invalid VL-block table offset")?;
    let bstart_end = bstart_pos
        .checked_add(4)
        .ok_or("Invalid VL-block table offset")?;
    if bstart_end > header.cbytes as usize || bstart_end > chunk.len() {
        return Err("Chunk too small for VL-block table");
    }
    let start_i32 = i32::from_le_bytes(chunk[bstart_pos..bstart_end].try_into().unwrap());
    if start_i32 < 0 {
        return Err("Invalid negative VL-block offset");
    }
    let start = start_i32 as usize;
    let mut min_start = header_len
        .checked_add(
            nblocks
                .checked_mul(4)
                .ok_or("Invalid VL-block table size")?,
        )
        .ok_or("Invalid VL-block table size")?;
    if header.use_dict() {
        let dict_size_end = min_start
            .checked_add(4)
            .ok_or("Invalid dictionary offset")?;
        if dict_size_end > header.cbytes as usize || dict_size_end > chunk.len() {
            return Err("Chunk too small for dictionary size");
        }
        let dict_size = i32::from_le_bytes(chunk[min_start..dict_size_end].try_into().unwrap());
        if dict_size <= 0 || dict_size as usize > BLOSC2_MAXDICTSIZE {
            return Err("Invalid dictionary size");
        }
        min_start = dict_size_end
            .checked_add(dict_size as usize)
            .ok_or("Invalid dictionary size")?;
    }
    if start < min_start || start > header.cbytes as usize || start > chunk.len() {
        return Err("Invalid VL-block offset");
    }

    let end = if nblock + 1 < nblocks {
        let next_pos = header_len + (nblock + 1) * 4;
        let next_end = next_pos + 4;
        if next_end > header.cbytes as usize || next_end > chunk.len() {
            return Err("Chunk too small for VL-block table");
        }
        let next_i32 = i32::from_le_bytes(chunk[next_pos..next_end].try_into().unwrap());
        if next_i32 < 0 {
            return Err("Invalid negative VL-block offset");
        }
        next_i32 as usize
    } else {
        header.cbytes as usize
    };
    if end <= start || end > header.cbytes as usize || end > chunk.len() {
        return Err("Invalid VL-block offset order");
    }
    Ok(&chunk[start..end])
}

/// Return the number of variable-length blocks in a VL-block chunk.
pub fn vlchunk_get_nblocks(chunk: &[u8]) -> Result<usize, &'static str> {
    let header = ChunkHeader::read(chunk)?;
    validate_header(&header, chunk.len())?;
    if !header.vl_blocks() {
        return Err("Chunk does not use VL-blocks");
    }
    Ok(header.blocksize as usize)
}

/// Decompress one block from a VL-block chunk.
pub fn vldecompress_block(chunk: &[u8], nblock: usize) -> Result<Vec<u8>, &'static str> {
    let header = ChunkHeader::read(chunk)?;
    validate_header(&header, chunk.len())?;
    if !header.vl_blocks() {
        return Err("Chunk does not use VL-blocks");
    }

    let span = vl_block_span(chunk, &header, nblock)?;
    if span.len() < 4 {
        return Err("VL-block span too small");
    }
    let bsize_i32 = i32::from_le_bytes(span[..4].try_into().unwrap());
    if bsize_i32 <= 0 {
        return Err("Invalid VL-block uncompressed size");
    }
    let bsize = bsize_i32 as usize;
    let payload = &span[4..];
    let typesize = header.typesize as usize;
    let dict = embedded_dictionary(chunk, &header)?;

    let mut filtered = vec![0u8; bsize];
    if payload.len() == bsize {
        filtered.copy_from_slice(payload);
    } else {
        let dsize = match dict {
            Some(dict) => {
                codecs::decompress_block_with_dict(header.compcode(), payload, &mut filtered, dict)
            }
            None => codecs::decompress_block_with_meta(
                header.compcode(),
                header.compcode_meta,
                payload,
                &mut filtered,
            ),
        };
        if dsize < 0 || dsize as usize != bsize {
            return Err("Codec decompression failed");
        }
    }

    let mut scratch = vec![0u8; bsize];
    let result_buf = filters::pipeline_backward(
        &mut filtered,
        &mut scratch,
        bsize,
        &header.filters,
        &header.filters_meta,
        typesize,
        0,
        None,
        1,
    );
    if result_buf == 1 {
        Ok(filtered)
    } else {
        Ok(scratch)
    }
}

/// Decompress a VL-block chunk into individual block buffers.
pub fn vldecompress(chunk: &[u8]) -> Result<Vec<Vec<u8>>, &'static str> {
    let nblocks = vlchunk_get_nblocks(chunk)?;
    (0..nblocks)
        .map(|nblock| vldecompress_block(chunk, nblock))
        .collect()
}

/// Extract `nitems` logical items starting at `start` from a compressed chunk.
///
/// `start` and `nitems` are item counts, not byte offsets. Only the compressed
/// blocks intersecting the requested byte range are decompressed.
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
    if byte_len == 0 {
        return Ok(Vec::new());
    }
    if header.vl_blocks() {
        return getitem_vlblocks(chunk, &header, byte_start, byte_end, byte_len);
    }

    let nbytes = header.nbytes as usize;
    let header_len = header.header_len();
    let special = header.special_type();
    if special != BLOSC2_NO_SPECIAL {
        let data = decompress_special(chunk, &header, nbytes)?;
        return Ok(data[byte_start..byte_end].to_vec());
    }

    if header.memcpyed() {
        let payload_start = header_len
            .checked_add(byte_start)
            .ok_or("Item range overflow")?;
        let payload_end = header_len
            .checked_add(byte_end)
            .ok_or("Item range overflow")?;
        if payload_end > header.cbytes as usize || payload_end > chunk.len() {
            return Err("Chunk too small for memcpyed data");
        }
        return Ok(chunk[payload_start..payload_end].to_vec());
    }

    let blocksize = header.blocksize as usize;
    if blocksize == 0 {
        return Err("Invalid blocksize");
    }
    let first_block = byte_start / blocksize;
    let last_block = (byte_end - 1) / blocksize;
    let nblocks = header.nblocks();
    if last_block >= nblocks {
        return Err("Item range out of bounds");
    }

    let dict = embedded_dictionary(chunk, &header)?;
    let has_delta = header.filters.contains(&BLOSC_DELTA);
    let block0_ref = if has_delta {
        let block0_end = blocksize.min(nbytes);
        Some(decompress_block_data(
            chunk,
            0,
            0,
            block0_end,
            blocksize,
            nblocks == 1 && block0_end < blocksize,
            &header,
            Some(&vec![0u8; blocksize.min(nbytes)]),
            dict,
        )?)
    } else {
        None
    };

    let mut out = Vec::with_capacity(byte_len);
    for block_idx in first_block..=last_block {
        let block_start = block_idx * blocksize;
        let block_end = (block_start + blocksize).min(nbytes);
        let bsize = block_end - block_start;
        let is_leftover = block_idx == nblocks - 1 && bsize < blocksize;

        let block_data = if has_delta && block_idx == 0 {
            block0_ref
                .as_ref()
                .ok_or("Missing delta reference block")?
                .clone()
        } else {
            decompress_block_data(
                chunk,
                block_idx,
                block_start,
                bsize,
                blocksize,
                is_leftover,
                &header,
                block0_ref.as_deref(),
                dict,
            )?
        };

        let local_start = byte_start.saturating_sub(block_start);
        let local_end = byte_end.min(block_end) - block_start;
        out.extend_from_slice(&block_data[local_start..local_end]);
    }

    Ok(out)
}

fn getitem_vlblocks(
    chunk: &[u8],
    header: &ChunkHeader,
    byte_start: usize,
    byte_end: usize,
    byte_len: usize,
) -> Result<Vec<u8>, &'static str> {
    let nblocks = header.blocksize as usize;
    let mut out = Vec::with_capacity(byte_len);
    let mut block_start = 0usize;

    for block_idx in 0..nblocks {
        let span = vl_block_span(chunk, header, block_idx)?;
        if span.len() < 4 {
            return Err("VL-block span too small");
        }
        let bsize_i32 = i32::from_le_bytes(span[..4].try_into().unwrap());
        if bsize_i32 <= 0 {
            return Err("Invalid VL-block uncompressed size");
        }
        let bsize = bsize_i32 as usize;
        let block_end = block_start
            .checked_add(bsize)
            .ok_or("VL-block sizes overflow")?;

        if block_end > byte_start && block_start < byte_end {
            let block = vldecompress_block(chunk, block_idx)?;
            if block.len() != bsize {
                return Err("Invalid VL-block uncompressed size");
            }
            let local_start = byte_start.saturating_sub(block_start);
            let local_end = byte_end.min(block_end) - block_start;
            out.extend_from_slice(&block[local_start..local_end]);
        }
        if block_end >= byte_end {
            break;
        }
        block_start = block_end;
    }

    if out.len() != byte_len {
        return Err("VL-block sizes do not add up to chunk nbytes");
    }
    Ok(out)
}

fn read_block_payload_spans(
    chunk: &[u8],
    header: &ChunkHeader,
    min_payload_start: usize,
) -> Result<Vec<std::ops::Range<usize>>, &'static str> {
    let nblocks = header.nblocks();
    let header_len = header.header_len();
    let chunk_limit = header.cbytes as usize;
    let mut spans = Vec::with_capacity(nblocks);

    for block_idx in 0..nblocks {
        let bstart_pos = header_len
            .checked_add(block_idx.checked_mul(4).ok_or("Invalid block table size")?)
            .ok_or("Invalid block table offset")?;
        let bstart_end = bstart_pos
            .checked_add(4)
            .ok_or("Invalid block table offset")?;
        if bstart_end > chunk_limit || bstart_end > chunk.len() {
            return Err("Chunk too small for bstarts");
        }

        let start_i32 = i32::from_le_bytes(chunk[bstart_pos..bstart_end].try_into().unwrap());
        if start_i32 < 0 {
            return Err("Invalid negative block offset");
        }
        let start = start_i32 as usize;
        if start < min_payload_start || start > chunk_limit || start > chunk.len() {
            return Err("Invalid block offset");
        }

        let end = if block_idx + 1 < nblocks {
            let next_pos = header_len
                .checked_add(
                    (block_idx + 1)
                        .checked_mul(4)
                        .ok_or("Invalid block table size")?,
                )
                .ok_or("Invalid block table offset")?;
            let next_end = next_pos
                .checked_add(4)
                .ok_or("Invalid block table offset")?;
            if next_end > chunk_limit || next_end > chunk.len() {
                return Err("Chunk too small for bstarts");
            }
            let next_i32 = i32::from_le_bytes(chunk[next_pos..next_end].try_into().unwrap());
            if next_i32 < 0 {
                return Err("Invalid negative block offset");
            }
            next_i32 as usize
        } else {
            chunk_limit
        };

        if end < start || end > chunk_limit || end > chunk.len() {
            return Err("Invalid block offset order");
        }
        spans.push(start..end);
    }

    Ok(spans)
}

/// Replace a byte range by recompressing only the compressed blocks it touches.
///
/// Returns `Ok(None)` when callers should fall back to a full chunk rewrite.
pub fn replace_aligned_blocks(
    chunk: &[u8],
    byte_start: usize,
    data: &[u8],
    cparams: &CParams,
) -> Result<Option<Vec<u8>>, &'static str> {
    let header = ChunkHeader::read(chunk)?;
    validate_header(&header, chunk.len())?;
    if data.is_empty() {
        return Ok(Some(chunk.to_vec()));
    }

    let nbytes = header.nbytes as usize;
    let byte_end = byte_start
        .checked_add(data.len())
        .ok_or("Item range overflow")?;
    if byte_end > nbytes {
        return Err("Item range out of bounds");
    }
    if header.vl_blocks() || header.special_type() != BLOSC2_NO_SPECIAL {
        return Ok(None);
    }

    let header_len = header.header_len();
    if header.memcpyed() {
        let payload_start = header_len
            .checked_add(byte_start)
            .ok_or("Item range overflow")?;
        let payload_end = header_len
            .checked_add(byte_end)
            .ok_or("Item range overflow")?;
        if payload_end > header.cbytes as usize || payload_end > chunk.len() {
            return Err("Chunk too small for memcpyed data");
        }
        let mut updated = chunk.to_vec();
        updated[payload_start..payload_end].copy_from_slice(data);
        return Ok(Some(updated));
    }

    let blocksize = header.blocksize as usize;
    if blocksize == 0 {
        return Ok(None);
    }
    let first_block = byte_start / blocksize;
    let last_block = (byte_end - 1) / blocksize;

    let nblocks = header.nblocks();
    if last_block >= nblocks {
        return Err("Item range out of bounds");
    }
    if cparams.compcode != header.compcode()
        || cparams.typesize as u8 != header.typesize
        || cparams.filters != header.filters
        || cparams.filters_meta != header.filters_meta
    {
        return Ok(None);
    }

    let has_delta = header.filters.contains(&BLOSC_DELTA);
    if has_delta && first_block == 0 {
        let block0_end = blocksize.min(nbytes);
        if byte_start != 0 || byte_end < block0_end || last_block + 1 < nblocks {
            return Ok(None);
        }
    }

    let dict = embedded_dictionary(chunk, &header)?;
    let table_end = header_len
        .checked_add(nblocks.checked_mul(4).ok_or("Invalid block table size")?)
        .ok_or("Invalid block table size")?;
    let min_payload_start = table_end
        .checked_add(dict.map_or(0, |dict| 4 + dict.len()))
        .ok_or("Invalid dictionary size")?;
    let old_spans = read_block_payload_spans(chunk, &header, min_payload_start)?;

    let delta_ref = if has_delta && first_block == 0 {
        Some(data[..blocksize.min(data.len())].to_vec())
    } else if has_delta {
        let block0_end = blocksize.min(nbytes);
        Some(decompress_block_data(
            chunk,
            0,
            0,
            block0_end,
            blocksize,
            nblocks == 1 && block0_end < blocksize,
            &header,
            Some(&vec![0u8; blocksize.min(nbytes)]),
            dict,
        )?)
    } else {
        None
    };

    let mut block_payloads: Vec<Vec<u8>> = Vec::with_capacity(nblocks);
    for (block_idx, old_span) in old_spans.iter().enumerate() {
        if block_idx < first_block || block_idx > last_block {
            block_payloads.push(chunk[old_span.clone()].to_vec());
            continue;
        }

        let block_start = block_idx * blocksize;
        let block_end = (block_start + blocksize).min(nbytes);
        let bsize = block_end - block_start;
        let is_leftover = block_idx == nblocks - 1 && bsize < blocksize;
        let mut block_data;
        let replacement_start = byte_start.max(block_start);
        let replacement_end = byte_end.min(block_end);
        let local_start = replacement_start - block_start;
        let local_end = replacement_end - block_start;

        if local_start == 0 && local_end == bsize {
            let data_start = replacement_start - byte_start;
            block_data = data[data_start..data_start + bsize].to_vec();
        } else {
            let old_block = decompress_block_data(
                chunk,
                block_idx,
                block_start,
                bsize,
                blocksize,
                is_leftover,
                &header,
                delta_ref.as_deref(),
                dict,
            )?;
            block_data = old_block;
            let data_start = replacement_start - byte_start;
            block_data[local_start..local_end]
                .copy_from_slice(&data[data_start..data_start + (local_end - local_start)]);
        }

        let mut buf1 = vec![0u8; bsize];
        let single_shuffle = single_shuffle_filter(
            &header.filters,
            &header.filters_meta,
            header.typesize as usize,
        );
        let mut buf2 = Vec::new();
        let filtered = if let Some(shuffle_typesize) = single_shuffle {
            filters::shuffle(shuffle_typesize, &block_data, &mut buf1);
            &buf1[..bsize]
        } else {
            buf2.resize(bsize, 0);
            let filtered_buf = filters::pipeline_forward(
                &block_data,
                &mut buf1,
                &mut buf2,
                &header.filters,
                &header.filters_meta,
                header.typesize as usize,
                block_start,
                delta_ref.as_deref(),
            );
            if filtered_buf == 1 {
                &buf1[..bsize]
            } else {
                &buf2[..bsize]
            }
        };
        let (block_payload, _) = compress_pre_filtered_block(
            filtered,
            cparams,
            header.dont_split(),
            header.typesize as usize,
            is_leftover,
            dict,
        );
        block_payloads.push(block_payload);
    }

    let total_len = block_payloads
        .iter()
        .try_fold(min_payload_start, |acc, payload| {
            acc.checked_add(payload.len()).ok_or("Chunk too large")
        })?;
    if total_len > i32::MAX as usize {
        return Err("Chunk too large");
    }

    let mut output = vec![0u8; table_end];
    output[..header_len].copy_from_slice(&chunk[..header_len]);
    output.extend_from_slice(&chunk[table_end..min_payload_start]);

    for (block_idx, payload) in block_payloads.iter().enumerate() {
        let bstart_offset = header_len + block_idx * 4;
        let payload_offset = output.len() as i32;
        output[bstart_offset..bstart_offset + 4].copy_from_slice(&payload_offset.to_le_bytes());
        output.extend_from_slice(payload);
    }

    let mut updated_header = header;
    updated_header.cbytes = output.len() as i32;
    updated_header.try_write(&mut output[..header_len])?;
    Ok(Some(output))
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

    if header.vl_blocks() {
        let blocks = vldecompress(chunk)?;
        let mut output = Vec::with_capacity(nbytes);
        for block in blocks {
            output.extend(block);
        }
        if output.len() != nbytes {
            return Err("VL-block sizes do not add up to chunk nbytes");
        }
        return Ok(output);
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

    let dict = embedded_dictionary(chunk, &header)?;

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
            dict,
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
                dict,
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

        let results: Vec<Result<(usize, Vec<u8>), &'static str>> =
            with_thread_pool(nthreads, || {
                block_infos
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
                            dict,
                        )?;
                        Ok((start, data))
                    })
                    .collect()
            });

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
                dict,
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
    fn test_getitem_decompresses_only_touched_blocks() {
        let data: Vec<u8> = (0..512u32).flat_map(|i| i.to_le_bytes()).collect();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 4,
            blocksize: 128,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut compressed = compress(&data, &cparams).unwrap();
        let header = ChunkHeader::read(&compressed).unwrap();
        assert!(header.nblocks() > 2);

        let block2_bstart = BLOSC_EXTENDED_HEADER_LENGTH + 2 * 4;
        compressed[block2_bstart..block2_bstart + 4].copy_from_slice(&(-1i32).to_le_bytes());
        assert!(decompress(&compressed).is_err());

        let items = getitem(&compressed, 4, 8).unwrap();
        assert_eq!(items, data[4 * 4..12 * 4]);
    }

    #[test]
    fn test_getitem_block_local_with_delta_and_dictionary() {
        let data: Vec<u8> = (0..8192u32).flat_map(|i| (i % 257).to_le_bytes()).collect();
        for (filters, use_dict) in [
            ([0, 0, 0, 0, BLOSC_DELTA, BLOSC_SHUFFLE], false),
            ([0, 0, 0, 0, 0, BLOSC_SHUFFLE], true),
        ] {
            let cparams = CParams {
                compcode: BLOSC_ZSTD,
                clevel: 5,
                typesize: 4,
                blocksize: 1024,
                splitmode: BLOSC_NEVER_SPLIT,
                filters,
                use_dict,
                ..Default::default()
            };
            let compressed = compress(&data, &cparams).unwrap();
            let items = getitem(&compressed, 300, 600).unwrap();
            assert_eq!(items, data[300 * 4..900 * 4]);
        }
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

        let codecs = {
            let codecs = vec![BLOSC_BLOSCLZ, BLOSC_LZ4, BLOSC_ZLIB, BLOSC_ZSTD];
            #[cfg(feature = "lz4hc-sys")]
            {
                let mut codecs = codecs;
                codecs.insert(2, BLOSC_LZ4HC);
                codecs
            }
            #[cfg(not(feature = "lz4hc-sys"))]
            {
                codecs
            }
        };

        for compcode in codecs {
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
    #[cfg(not(feature = "lz4hc-sys"))]
    fn test_lz4hc_compression_requires_sys_feature() {
        let data = b"high-level LZ4HC compression should fail without lz4hc-sys";
        let cparams = CParams {
            compcode: BLOSC_LZ4HC,
            clevel: 9,
            ..Default::default()
        };

        assert_eq!(
            compress(data, &cparams).unwrap_err(),
            "LZ4HC compression requires the lz4hc-sys feature"
        );
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
    fn test_zstd_dictionary_chunk_roundtrip() {
        let data: Vec<u8> = (0..200_000u32)
            .flat_map(|i| {
                let value = i % 4096;
                value.to_le_bytes()
            })
            .collect();
        let cparams = CParams {
            compcode: BLOSC_ZSTD,
            clevel: 5,
            typesize: 4,
            blocksize: 4096,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            use_dict: true,
            ..Default::default()
        };

        let compressed = compress(&data, &cparams).unwrap();
        let header = ChunkHeader::read(&compressed).unwrap();
        assert!(header.use_dict());
        assert!(embedded_dictionary(&compressed, &header).unwrap().is_some());

        let decompressed = decompress_with_threads(&compressed, 4).unwrap();
        assert_eq!(data, decompressed);
    }

    fn sequence_codec_compress(_clevel: u8, meta: u8, src: &[u8], dest: &mut [u8]) -> i32 {
        if src.len() < 2 || dest.len() < 3 {
            return 0;
        }
        dest[0] = src[0];
        dest[1] = src[1].wrapping_sub(src[0]);
        dest[2] = meta;
        3
    }

    fn sequence_codec_decompress(meta: u8, src: &[u8], dest: &mut [u8]) -> i32 {
        if src.len() != 3 || src[2] != meta {
            return -1;
        }
        for (idx, byte) in dest.iter_mut().enumerate() {
            *byte = src[0].wrapping_add(src[1].wrapping_mul(idx as u8));
        }
        dest.len() as i32
    }

    #[test]
    fn test_user_defined_codec_roundtrip_and_metadata() {
        const CODEC_ID: u8 = 200;
        codecs::register_codec(CODEC_ID, sequence_codec_compress, sequence_codec_decompress)
            .unwrap();

        let data: Vec<u8> = (0..200u8).collect();
        let cparams = CParams {
            compcode: CODEC_ID,
            compcode_meta: 17,
            clevel: 5,
            typesize: 1,
            blocksize: 200,
            filters: [0; BLOSC2_MAX_FILTERS],
            ..Default::default()
        };

        let compressed = compress(&data, &cparams).unwrap();
        let header = ChunkHeader::read(&compressed).unwrap();
        assert_eq!(header.compcode(), CODEC_ID);
        assert_eq!(header.compcode_meta, 17);
        assert_eq!(decompress(&compressed).unwrap(), data);
    }

    #[test]
    fn test_dictionary_falls_back_for_small_payload() {
        let data = b"small payload";
        let cparams = CParams {
            compcode: BLOSC_ZSTD,
            clevel: 5,
            typesize: 1,
            use_dict: true,
            ..Default::default()
        };

        let compressed = compress(data, &cparams).unwrap();
        let header = ChunkHeader::read(&compressed).unwrap();
        assert!(!header.use_dict());
        assert_eq!(decompress(&compressed).unwrap(), data);
    }

    #[test]
    fn test_vlblocks_roundtrip() {
        let blocks: [&[u8]; 3] = [b"red\0", b"green-green\0", b"blue-blue-blue-blue\0"];
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            nthreads: 4,
            ..Default::default()
        };

        let compressed = vlcompress(&blocks, &cparams).unwrap();
        let header = ChunkHeader::read(&compressed).unwrap();
        assert!(header.vl_blocks());
        assert_eq!(header.version, BLOSC2_VERSION_FORMAT_VL_BLOCKS);
        assert_eq!(vlchunk_get_nblocks(&compressed).unwrap(), 3);

        let split = vldecompress(&compressed).unwrap();
        assert_eq!(
            split,
            blocks
                .iter()
                .map(|block| block.to_vec())
                .collect::<Vec<_>>()
        );
        assert_eq!(
            vldecompress_block(&compressed, 1).unwrap(),
            b"green-green\0"
        );
        assert_eq!(
            decompress(&compressed).unwrap(),
            b"red\0green-green\0blue-blue-blue-blue\0"
        );
        assert_eq!(getitem(&compressed, 2, 16).unwrap(), b"d\0green-green\0bl");
    }

    #[test]
    fn test_vlblocks_typesize4_shuffle_and_bitshuffle_roundtrip() {
        let blocks: Vec<Vec<u8>> = [
            (0..64u32).collect::<Vec<_>>(),
            (1000..1137u32).collect::<Vec<_>>(),
            (9000..9131u32).map(|value| value ^ 0x55aa_3300).collect(),
        ]
        .into_iter()
        .map(|values| values.into_iter().flat_map(u32::to_le_bytes).collect())
        .collect();
        let block_refs: Vec<&[u8]> = blocks.iter().map(Vec::as_slice).collect();
        let expected_concat: Vec<u8> = blocks
            .iter()
            .flat_map(|block| block.iter())
            .copied()
            .collect();

        for filter in [BLOSC_SHUFFLE, BLOSC_BITSHUFFLE] {
            let cparams = CParams {
                compcode: BLOSC_LZ4,
                clevel: 5,
                typesize: 4,
                nthreads: 4,
                filters: [0, 0, 0, 0, 0, filter],
                ..Default::default()
            };

            let compressed = vlcompress(&block_refs, &cparams).unwrap();
            let header = ChunkHeader::read(&compressed).unwrap();
            assert!(header.vl_blocks());
            assert_eq!(header.typesize, 4);
            assert_eq!(vldecompress(&compressed).unwrap(), blocks);
            assert_eq!(decompress(&compressed).unwrap(), expected_concat);
            assert_eq!(
                getitem(&compressed, 60, 24).unwrap(),
                expected_concat[60 * 4..84 * 4]
            );
        }
    }

    #[test]
    fn test_vlblocks_allow_non_typesize_multiple_block_sizes() {
        let blocks: [&[u8]; 3] = [b"abcde", b"123456789", b"tail-bytes-not-aligned"];
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 4,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };

        let compressed = vlcompress(&blocks, &cparams).unwrap();
        let expected_concat: Vec<u8> = blocks
            .iter()
            .flat_map(|block| block.iter())
            .copied()
            .collect();
        assert_eq!(vldecompress(&compressed).unwrap(), blocks);
        assert_eq!(decompress(&compressed).unwrap(), expected_concat);
    }

    #[test]
    fn test_zstd_dictionary_vlblocks_roundtrip() {
        let blocks: Vec<Vec<u8>> = (0..64)
            .map(|i| {
                format!(
                    "{{\"id\":\"ingredient-{i:03}\",\"vegan\":\"{}\",\"percent\":{},\"text\":\"INGREDIENT NUMBER {i:03}\"}}",
                    if i % 3 == 0 { "maybe" } else { "yes" },
                    i % 17
                )
                .into_bytes()
            })
            .collect();
        let block_refs: Vec<&[u8]> = blocks.iter().map(Vec::as_slice).collect();
        let cparams = CParams {
            compcode: BLOSC_ZSTD,
            clevel: 5,
            typesize: 1,
            nthreads: 4,
            use_dict: true,
            ..Default::default()
        };

        let compressed = vlcompress(&block_refs, &cparams).unwrap();
        let header = ChunkHeader::read(&compressed).unwrap();
        assert!(header.vl_blocks());
        assert!(header.use_dict());
        assert!(embedded_dictionary(&compressed, &header).unwrap().is_some());

        assert_eq!(vldecompress(&compressed).unwrap(), blocks);
        assert_eq!(vldecompress_block(&compressed, 17).unwrap(), blocks[17]);
        let expected_concat: Vec<u8> = blocks.iter().flatten().copied().collect();
        assert_eq!(decompress(&compressed).unwrap(), expected_concat);
        assert_eq!(
            getitem(&compressed, 10, 128).unwrap(),
            expected_concat[10..138]
        );
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
