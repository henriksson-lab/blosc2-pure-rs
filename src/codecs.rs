pub mod blosclz;

use crate::constants::*;

/// Compress a block using the specified codec.
/// Returns the number of compressed bytes, or 0 if incompressible.
pub fn compress_block(
    compcode: u8,
    clevel: u8,
    src: &[u8],
    dest: &mut [u8],
) -> i32 {
    match compcode {
        BLOSC_BLOSCLZ => blosclz::compress(clevel as i32, src, dest),
        BLOSC_LZ4 | BLOSC_LZ4HC => lz4_compress(src, dest),
        BLOSC_ZLIB => zlib_compress(src, dest, clevel),
        BLOSC_ZSTD => zstd_compress(src, dest, clevel),
        _ => 0,
    }
}

/// Decompress a block using the specified codec.
/// Returns the number of decompressed bytes, or negative on error.
pub fn decompress_block(
    compcode: u8,
    src: &[u8],
    dest: &mut [u8],
) -> i32 {
    match compcode {
        BLOSC_BLOSCLZ => blosclz::decompress(src, dest),
        BLOSC_LZ4 | BLOSC_LZ4HC => lz4_decompress(src, dest),
        BLOSC_ZLIB => zlib_decompress(src, dest),
        BLOSC_ZSTD => zstd_decompress(src, dest),
        _ => -1,
    }
}

fn lz4_compress(src: &[u8], dest: &mut [u8]) -> i32 {
    // compress_into requires dest.len() >= get_maximum_output_size(src.len())
    // If dest is big enough, compress directly. Otherwise fall back to allocating.
    let max_out = lz4_flex::block::get_maximum_output_size(src.len());
    if dest.len() >= max_out {
        match lz4_flex::block::compress_into(src, dest) {
            Ok(n) => n as i32,
            Err(_) => 0,
        }
    } else {
        let compressed = lz4_flex::block::compress(src);
        if compressed.len() > dest.len() {
            return 0;
        }
        dest[..compressed.len()].copy_from_slice(&compressed);
        compressed.len() as i32
    }
}

fn lz4_decompress(src: &[u8], dest: &mut [u8]) -> i32 {
    match lz4_flex::block::decompress_into(src, dest) {
        Ok(n) => n as i32,
        Err(_) => -1,
    }
}

fn zlib_compress(src: &[u8], dest: &mut [u8], clevel: u8) -> i32 {
    use flate2::Compression;

    // Use compress directly into dest buffer via flate2's low-level API
    let level = Compression::new(clevel as u32);
    let mut compress = flate2::Compress::new(level, true);

    let status = compress.compress(
        src,
        dest,
        flate2::FlushCompress::Finish,
    );

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
        Ok(_) => decompress.total_out() as i32,
        Err(_) => -1,
    }
}

fn zstd_compress(src: &[u8], dest: &mut [u8], clevel: u8) -> i32 {
    // Use compress_to_buffer to write directly into dest
    match zstd::bulk::compress_to_buffer(src, dest, clevel as i32) {
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
