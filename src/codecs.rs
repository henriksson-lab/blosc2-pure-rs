pub mod blosclz;

use crate::constants::*;
use std::collections::HashMap;
use std::sync::{OnceLock, RwLock};

pub type CodecCompressFn = fn(clevel: u8, meta: u8, src: &[u8], dest: &mut [u8]) -> i32;
pub type CodecDecompressFn = fn(meta: u8, src: &[u8], dest: &mut [u8]) -> i32;

#[derive(Clone, Copy)]
struct UserCodec {
    compress: CodecCompressFn,
    decompress: CodecDecompressFn,
}

static USER_CODECS: OnceLock<RwLock<HashMap<u8, UserCodec>>> = OnceLock::new();

fn user_codecs() -> &'static RwLock<HashMap<u8, UserCodec>> {
    USER_CODECS.get_or_init(|| RwLock::new(HashMap::new()))
}

pub fn register_codec(
    compcode: u8,
    compress: CodecCompressFn,
    decompress: CodecDecompressFn,
) -> Result<(), &'static str> {
    if compcode < BLOSC2_USER_DEFINED_CODECS_START {
        return Err("User-defined codec IDs must be >= 32");
    }
    user_codecs()
        .write()
        .map_err(|_| "Codec registry poisoned")?
        .insert(
            compcode,
            UserCodec {
                compress,
                decompress,
            },
        );
    Ok(())
}

pub fn is_registered_codec(compcode: u8) -> bool {
    user_codecs()
        .read()
        .is_ok_and(|codecs| codecs.contains_key(&compcode))
}

/// Compress a block using the specified codec.
/// Returns the number of compressed bytes, or 0 if incompressible.
pub fn compress_block(compcode: u8, clevel: u8, src: &[u8], dest: &mut [u8]) -> i32 {
    compress_block_with_meta(compcode, clevel, 0, src, dest)
}

pub fn compress_block_with_meta(
    compcode: u8,
    clevel: u8,
    meta: u8,
    src: &[u8],
    dest: &mut [u8],
) -> i32 {
    match compcode {
        BLOSC_BLOSCLZ => blosclz::compress(clevel as i32, src, dest),
        BLOSC_LZ4 => lz4_compress(clevel, src, dest),
        BLOSC_LZ4HC => lz4hc_compress(clevel, src, dest),
        BLOSC_ZLIB => zlib_compress(src, dest, clevel),
        BLOSC_ZSTD => zstd_compress(src, dest, clevel),
        _ => user_codecs()
            .read()
            .ok()
            .and_then(|codecs| codecs.get(&compcode).copied())
            .map_or(0, |codec| (codec.compress)(clevel, meta, src, dest)),
    }
}

pub fn compress_block_with_dict(
    compcode: u8,
    clevel: u8,
    src: &[u8],
    dest: &mut [u8],
    dict: &[u8],
) -> i32 {
    match compcode {
        BLOSC_ZSTD => zstd_compress_with_dict(src, dest, clevel, dict),
        _ => compress_block(compcode, clevel, src, dest),
    }
}

/// Decompress a block using the specified codec.
/// Returns the number of decompressed bytes, or negative on error.
pub fn decompress_block(compcode: u8, src: &[u8], dest: &mut [u8]) -> i32 {
    decompress_block_with_meta(compcode, 0, src, dest)
}

pub fn decompress_block_with_meta(compcode: u8, meta: u8, src: &[u8], dest: &mut [u8]) -> i32 {
    match compcode {
        BLOSC_BLOSCLZ => blosclz::decompress(src, dest),
        BLOSC_LZ4 | BLOSC_LZ4HC => lz4_decompress(src, dest),
        BLOSC_ZLIB => zlib_decompress(src, dest),
        BLOSC_ZSTD => zstd_decompress(src, dest),
        _ => user_codecs()
            .read()
            .ok()
            .and_then(|codecs| codecs.get(&compcode).copied())
            .map_or(-1, |codec| (codec.decompress)(meta, src, dest)),
    }
}

pub fn decompress_block_with_dict(compcode: u8, src: &[u8], dest: &mut [u8], dict: &[u8]) -> i32 {
    match compcode {
        BLOSC_ZSTD => zstd_decompress_with_dict(src, dest, dict),
        _ => decompress_block(compcode, src, dest),
    }
}

fn lz4_compress(clevel: u8, src: &[u8], dest: &mut [u8]) -> i32 {
    use lz4_pure::block::CompressionMode;

    let accel = (10 - i32::from(clevel.clamp(0, 9))).max(1);
    match lz4_pure::block::compress_to_buffer(src, Some(CompressionMode::FAST(accel)), false, dest)
    {
        Ok(n) => n as i32,
        Err(_) => 0,
    }
}

fn lz4hc_compress(clevel: u8, src: &[u8], dest: &mut [u8]) -> i32 {
    use lz4_pure::block::CompressionMode;
    match lz4_pure::block::compress_to_buffer(
        src,
        Some(CompressionMode::HIGHCOMPRESSION(i32::from(clevel))),
        false,
        dest,
    ) {
        Ok(n) => n as i32,
        Err(_) => 0,
    }
}

fn lz4_decompress(src: &[u8], dest: &mut [u8]) -> i32 {
    match lz4_pure::block::decompress_to_buffer(src, Some(dest.len() as i32), dest) {
        Ok(n) => n as i32,
        Err(_) => -1,
    }
}

fn zlib_compress(src: &[u8], dest: &mut [u8], clevel: u8) -> i32 {
    use flate2::Compression;

    // Use compress directly into dest buffer via flate2's low-level API
    let level = Compression::new(clevel as u32);
    let mut compress = flate2::Compress::new(level, true);

    let status = compress.compress(src, dest, flate2::FlushCompress::Finish);

    match status {
        Ok(flate2::Status::StreamEnd) => compress.total_out() as i32,
        Ok(flate2::Status::Ok | flate2::Status::BufError) => {
            // Output buffer too small or incomplete
            0
        }
        Err(_) => 0,
    }
}

fn zlib_decompress(src: &[u8], dest: &mut [u8]) -> i32 {
    use flate2::Decompress;
    use flate2::FlushDecompress;

    let mut decompress = Decompress::new(true);
    match decompress.decompress(src, dest, FlushDecompress::Finish) {
        Ok(flate2::Status::StreamEnd) => decompress.total_out() as i32,
        Ok(_) => -1,
        Err(_) => -1,
    }
}

/// Map blosc clevel (0..=9) to the underlying zstd compression level,
/// matching `zstd_wrap_compress` in c-blosc2/blosc/blosc2.c:543.
///
/// C formula: `clevel = (clevel < 9) ? clevel * 2 - 1 : ZSTD_maxCLevel();`
/// which gives: 0→-1, 1→1, 2→3, 3→5, 4→7, 5→9, 6→11, 7→13, 8→15, 9→22.
fn blosc_clevel_to_zstd(clevel: u8) -> i32 {
    if clevel < 9 {
        // Signed to accommodate blosc 0 → zstd -1 (fastest / negative-level).
        (clevel as i32) * 2 - 1
    } else {
        // ZSTD_maxCLevel() is 22 in upstream zstd (has been stable since 1.0).
        22
    }
}

fn zstd_compress(src: &[u8], dest: &mut [u8], clevel: u8) -> i32 {
    // Use compress_to_buffer to write directly into dest
    match zstd::bulk::compress_to_buffer(src, dest, blosc_clevel_to_zstd(clevel)) {
        Ok(n) => n as i32,
        Err(_) => 0,
    }
}

fn zstd_compress_with_dict(src: &[u8], dest: &mut [u8], clevel: u8, dict: &[u8]) -> i32 {
    match zstd::bulk::Compressor::with_dictionary(blosc_clevel_to_zstd(clevel), dict)
        .and_then(|mut compressor| compressor.compress_to_buffer(src, dest))
    {
        Ok(n) => n as i32,
        Err(_) => 0,
    }
}

fn zstd_decompress(src: &[u8], dest: &mut [u8]) -> i32 {
    // Use decompress_to_buffer to write directly into dest
    match zstd::bulk::decompress_to_buffer(src, dest) {
        Ok(n) => n as i32,
        Err(_) => -1,
    }
}

fn zstd_decompress_with_dict(src: &[u8], dest: &mut [u8], dict: &[u8]) -> i32 {
    match zstd::bulk::Decompressor::with_dictionary(dict)
        .and_then(|mut decompressor| decompressor.decompress_to_buffer(src, dest))
    {
        Ok(n) => n as i32,
        Err(_) => -1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blosc_clevel_to_zstd_matches_c_library_mapping() {
        // Table from c-blosc2/blosc/blosc2.c zstd_wrap_compress.
        // Blosc level → zstd level.
        let expected = [
            (0, -1),
            (1, 1),
            (2, 3),
            (3, 5),
            (4, 7),
            (5, 9),
            (6, 11),
            (7, 13),
            (8, 15),
            (9, 22),
        ];
        for (blosc, zstd) in expected {
            assert_eq!(
                blosc_clevel_to_zstd(blosc),
                zstd,
                "blosc {blosc} must map to zstd {zstd}"
            );
        }
    }

    #[test]
    fn zstd_at_higher_blosc_level_compresses_better() {
        // A quick sanity check: after the mapping fix, blosc level 9 should
        // produce a significantly smaller or equal output than level 1 on
        // repetitive data. With the old identity mapping, level 9 used zstd
        // level 9; with the fix, level 9 uses zstd level 22 (maxCLevel).
        let data: Vec<u8> = (0..16384u32).flat_map(|i| (i % 17).to_le_bytes()).collect();
        let mut buf1 = vec![0u8; data.len() + 256];
        let mut buf9 = vec![0u8; data.len() + 256];

        let csize1 = zstd_compress(&data, &mut buf1, 1);
        let csize9 = zstd_compress(&data, &mut buf9, 9);

        assert!(csize1 > 0 && csize9 > 0, "compression must not fail");
        assert!(
            csize9 <= csize1,
            "level 9 must compress at least as well as level 1 (got {csize9} vs {csize1})"
        );
    }

    #[test]
    fn lz4hc_roundtrips_via_lz4_decoder() {
        let data: Vec<u8> = (0..8192u32).flat_map(|i| (i % 64).to_le_bytes()).collect();
        let mut compressed = vec![0; data.len() + 1024];

        let csize = compress_block(BLOSC_LZ4HC, 9, &data, &mut compressed);
        assert!(csize > 0);

        let mut decompressed = vec![0; data.len()];
        let dsize = decompress_block(
            BLOSC_LZ4HC,
            &compressed[..csize as usize],
            &mut decompressed,
        );

        assert_eq!(dsize as usize, data.len());
        assert_eq!(decompressed, data);
    }
}
