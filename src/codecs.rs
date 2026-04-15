pub mod blosclz;

use crate::constants::*;

/// Compress a block using the specified codec.
/// Returns the number of compressed bytes, or 0 if incompressible.
pub fn compress_block(compcode: u8, clevel: u8, src: &[u8], dest: &mut [u8]) -> i32 {
    match compcode {
        BLOSC_BLOSCLZ => blosclz::compress(clevel as i32, src, dest),
        BLOSC_LZ4 => lz4_compress(src, dest),
        BLOSC_LZ4HC => lz4hc_compress(clevel, src, dest),
        BLOSC_ZLIB => zlib_compress(src, dest, clevel),
        BLOSC_ZSTD => zstd_compress(src, dest, clevel),
        _ => 0,
    }
}

/// Decompress a block using the specified codec.
/// Returns the number of decompressed bytes, or negative on error.
pub fn decompress_block(compcode: u8, src: &[u8], dest: &mut [u8]) -> i32 {
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

fn lz4hc_compress(clevel: u8, src: &[u8], dest: &mut [u8]) -> i32 {
    let Ok(src_len) = i32::try_from(src.len()) else {
        return 0;
    };
    let Ok(dst_cap) = i32::try_from(dest.len()) else {
        return 0;
    };

    // SAFETY: lz4-sys only reads `src_len` bytes from `src` and writes at most
    // `dst_cap` bytes to `dest`. Both lengths were checked to fit C `int`.
    let written = unsafe {
        lz4_sys::LZ4_compress_HC(
            src.as_ptr().cast(),
            dest.as_mut_ptr().cast(),
            src_len,
            dst_cap,
            i32::from(clevel),
        )
    };

    written.max(0)
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

#[cfg(test)]
mod tests {
    use super::*;

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
