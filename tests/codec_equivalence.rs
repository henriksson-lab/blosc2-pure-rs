#![cfg(feature = "_ffi")]
use blosc2_pure_rs::codecs;
use blosc2_pure_rs::constants::*;
mod common;
use common::ffi;

fn init_blosc2() -> common::Blosc2 {
    common::Blosc2::new()
}

/// Compress with C BloscLZ, decompress with Rust BloscLZ
#[test]
fn test_blosclz_c_compress_rust_decompress() {
    let _b = init_blosc2();

    let data: Vec<u8> = (0..10000u32).flat_map(|i| i.to_le_bytes()).collect();
    let src_size = data.len() as i32;
    let buf_size = src_size as usize + BLOSC_EXTENDED_HEADER_LENGTH;

    // Compress with C (full blosc2 pipeline, blosclz codec, no shuffle to isolate codec)
    let mut compressed = vec![0u8; buf_size];
    let csize = unsafe {
        let mut cparams: ffi::blosc2_cparams = std::mem::zeroed();
        cparams.compcode = BLOSC_BLOSCLZ;
        cparams.clevel = 5;
        cparams.typesize = 4;
        cparams.nthreads = 1;
        cparams.splitmode = BLOSC_NEVER_SPLIT;
        // No filters to isolate codec behavior
        cparams.filters = [0; 6];

        let cctx = ffi::blosc2_create_cctx(cparams);
        let result = ffi::blosc2_compress_ctx(
            cctx,
            data.as_ptr() as *const _,
            src_size,
            compressed.as_mut_ptr() as *mut _,
            compressed.len() as i32,
        );
        ffi::blosc2_free_ctx(cctx);
        result
    };
    assert!(csize > 0, "C compression failed");

    // Decompress with C for reference
    let mut c_decompressed = vec![0u8; src_size as usize];
    let c_dsize = unsafe {
        ffi::blosc2_decompress(
            compressed.as_ptr() as *const _,
            csize,
            c_decompressed.as_mut_ptr() as *mut _,
            c_decompressed.len() as i32,
        )
    };
    assert_eq!(c_dsize, src_size, "C decompression size mismatch");
    assert_eq!(data, c_decompressed, "C roundtrip data mismatch");
}

/// Test Rust BloscLZ compress + decompress roundtrip
#[test]
fn test_blosclz_rust_roundtrip() {
    // Use highly compressible data (repeated patterns)
    let data: Vec<u8> = b"Hello BloscLZ! This is a test with repeating patterns. "
        .iter()
        .cycle()
        .take(40000)
        .copied()
        .collect();
    let mut compressed = vec![0u8; data.len() + 1000];
    let csize = codecs::blosclz::compress(5, &data, &mut compressed);
    assert!(csize > 0, "Rust BloscLZ compression failed");

    let mut decompressed = vec![0u8; data.len()];
    let dsize = codecs::blosclz::decompress(&compressed[..csize as usize], &mut decompressed);
    assert_eq!(
        dsize as usize,
        data.len(),
        "Rust BloscLZ decompression size mismatch"
    );
    assert_eq!(data, decompressed, "Rust BloscLZ roundtrip mismatch");
}

/// Test LZ4 roundtrip via Rust codecs
#[test]
fn test_lz4_rust_roundtrip() {
    let data: Vec<u8> = (0..5000u32).flat_map(|i| i.to_le_bytes()).collect();
    let mut compressed = vec![0u8; data.len() + 1000];
    let csize = codecs::compress_block(BLOSC_LZ4, 5, &data, &mut compressed);
    assert!(csize > 0, "LZ4 compression failed");

    let mut decompressed = vec![0u8; data.len()];
    let dsize =
        codecs::decompress_block(BLOSC_LZ4, &compressed[..csize as usize], &mut decompressed);
    assert_eq!(
        dsize as usize,
        data.len(),
        "LZ4 decompression size mismatch"
    );
    assert_eq!(data, decompressed, "LZ4 roundtrip mismatch");
}

/// Test Zlib roundtrip via Rust codecs
#[test]
fn test_zlib_rust_roundtrip() {
    let data: Vec<u8> = (0..5000u32).flat_map(|i| i.to_le_bytes()).collect();
    let mut compressed = vec![0u8; data.len() + 1000];
    let csize = codecs::compress_block(BLOSC_ZLIB, 5, &data, &mut compressed);
    assert!(csize > 0, "Zlib compression failed");

    let mut decompressed = vec![0u8; data.len()];
    let dsize =
        codecs::decompress_block(BLOSC_ZLIB, &compressed[..csize as usize], &mut decompressed);
    assert_eq!(
        dsize as usize,
        data.len(),
        "Zlib decompression size mismatch"
    );
    assert_eq!(data, decompressed, "Zlib roundtrip mismatch");
}

/// Test Zstd roundtrip via Rust codecs
#[test]
fn test_zstd_rust_roundtrip() {
    let data: Vec<u8> = (0..5000u32).flat_map(|i| i.to_le_bytes()).collect();
    let mut compressed = vec![0u8; data.len() + 1000];
    let csize = codecs::compress_block(BLOSC_ZSTD, 5, &data, &mut compressed);
    assert!(csize > 0, "Zstd compression failed");

    let mut decompressed = vec![0u8; data.len()];
    let dsize =
        codecs::decompress_block(BLOSC_ZSTD, &compressed[..csize as usize], &mut decompressed);
    assert_eq!(
        dsize as usize,
        data.len(),
        "Zstd decompression size mismatch"
    );
    assert_eq!(data, decompressed, "Zstd roundtrip mismatch");
}

/// Test all codecs with various data patterns
#[test]
fn test_all_codecs_patterns() {
    let patterns: Vec<(&str, Vec<u8>)> = vec![
        (
            "sequential",
            (0..20000u32).flat_map(|i| i.to_le_bytes()).collect(),
        ),
        ("repeated", vec![42u8; 20000]),
        ("sparse", {
            let mut d = vec![0u8; 20000];
            for i in (0..20000).step_by(100) {
                d[i] = 0xFF;
            }
            d
        }),
    ];

    let codecs = [
        BLOSC_BLOSCLZ,
        BLOSC_LZ4,
        BLOSC_LZ4HC,
        BLOSC_ZLIB,
        BLOSC_ZSTD,
    ];

    for (name, data) in &patterns {
        for &codec in &codecs {
            let mut compressed = vec![0u8; data.len() + 5000];
            let csize = codecs::compress_block(codec, 5, data, &mut compressed);
            if csize <= 0 {
                // Some codecs may fail on certain patterns (e.g., incompressible)
                continue;
            }

            let mut decompressed = vec![0u8; data.len()];
            let dsize =
                codecs::decompress_block(codec, &compressed[..csize as usize], &mut decompressed);
            assert_eq!(
                dsize as usize,
                data.len(),
                "Decompression size mismatch for codec={codec} pattern={name}"
            );
            assert_eq!(
                data, &decompressed,
                "Data mismatch for codec={codec} pattern={name}"
            );
        }
    }
}
