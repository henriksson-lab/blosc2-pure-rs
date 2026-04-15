#![cfg(feature = "_ffi")]
use blosc2_pure_rs::compress::{compress, decompress, CParams};
use blosc2_pure_rs::constants::*;
mod common;
use blosc2_pure_rs::schunk::Schunk;
use common::ffi;

fn init_blosc2() -> common::Blosc2 {
    common::Blosc2::new()
}

/// Compress with C FFI, decompress with pure Rust engine
#[test]
fn test_c_compress_rust_decompress() {
    let _b = init_blosc2();

    let data: Vec<u8> = (0..10000u32).flat_map(|i| i.to_le_bytes()).collect();
    let src_size = data.len() as i32;
    let buf_size = src_size as usize + BLOSC2_MAX_OVERHEAD;

    for &compcode in &[
        BLOSC_BLOSCLZ,
        BLOSC_LZ4,
        BLOSC_LZ4HC,
        BLOSC_ZLIB,
        BLOSC_ZSTD,
    ] {
        let mut compressed = vec![0u8; buf_size];
        let csize = unsafe {
            let mut cparams: ffi::blosc2_cparams = std::mem::zeroed();
            cparams.compcode = compcode;
            cparams.clevel = 5;
            cparams.typesize = 4;
            cparams.nthreads = 1;
            cparams.splitmode = BLOSC_FORWARD_COMPAT_SPLIT;
            cparams.filters[BLOSC2_MAX_FILTERS - 1] = BLOSC_SHUFFLE;

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
        assert!(csize > 0, "C compression failed for codec={compcode}");

        // Decompress with Rust
        let decompressed = decompress(&compressed[..csize as usize])
            .unwrap_or_else(|e| panic!("Rust decompress failed for codec={compcode}: {e}"));
        assert_eq!(data, decompressed, "C→Rust mismatch for codec={compcode}");
    }
}

/// Compress with pure Rust engine, decompress with C FFI
#[test]
fn test_rust_compress_c_decompress() {
    let _b = init_blosc2();

    let data: Vec<u8> = (0..10000u32).flat_map(|i| i.to_le_bytes()).collect();

    for &compcode in &[
        BLOSC_BLOSCLZ,
        BLOSC_LZ4,
        BLOSC_LZ4HC,
        BLOSC_ZLIB,
        BLOSC_ZSTD,
    ] {
        let cparams = CParams {
            compcode,
            clevel: 5,
            typesize: 4,
            splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };

        let compressed = compress(&data, &cparams)
            .unwrap_or_else(|e| panic!("Rust compress failed for codec={compcode}: {e}"));

        // Decompress with C
        let mut c_decompressed = vec![0u8; data.len()];
        let dsize = unsafe {
            ffi::blosc2_decompress(
                compressed.as_ptr() as *const _,
                compressed.len() as i32,
                c_decompressed.as_mut_ptr() as *mut _,
                c_decompressed.len() as i32,
            )
        };
        assert_eq!(
            dsize,
            data.len() as i32,
            "C decompress size mismatch for codec={compcode}: got {dsize}"
        );
        assert_eq!(data, c_decompressed, "Rust→C mismatch for codec={compcode}");
    }
}

/// Test with various filter combinations
#[test]
fn test_cross_compat_filters() {
    let _b = init_blosc2();

    let data: Vec<u8> = (0..5000u32).flat_map(|i| i.to_le_bytes()).collect();

    for &filter in &[BLOSC_NOFILTER, BLOSC_SHUFFLE, BLOSC_BITSHUFFLE] {
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 4,
            splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
            filters: [0, 0, 0, 0, 0, filter],
            ..Default::default()
        };

        let compressed = compress(&data, &cparams).unwrap();
        let decompressed = decompress(&compressed).unwrap();
        assert_eq!(data, decompressed, "Roundtrip failed for filter={filter}");

        // Also verify C can decompress our output
        let mut c_decompressed = vec![0u8; data.len()];
        let dsize = unsafe {
            ffi::blosc2_decompress(
                compressed.as_ptr() as *const _,
                compressed.len() as i32,
                c_decompressed.as_mut_ptr() as *mut _,
                c_decompressed.len() as i32,
            )
        };
        assert_eq!(
            dsize,
            data.len() as i32,
            "C decompress failed for filter={filter}"
        );
        assert_eq!(
            data, c_decompressed,
            "C decompress mismatch for filter={filter}"
        );
    }
}

#[test]
fn test_rust_compress_c_decompress_splitmode_matrix() {
    let _b = init_blosc2();

    let data: Vec<u8> = (0..12000u32).flat_map(|i| i.to_le_bytes()).collect();

    for &splitmode in &[
        BLOSC_ALWAYS_SPLIT,
        BLOSC_NEVER_SPLIT,
        BLOSC_FORWARD_COMPAT_SPLIT,
    ] {
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 4,
            splitmode,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };

        let compressed = compress(&data, &cparams).unwrap();
        let mut c_decompressed = vec![0u8; data.len()];
        let dsize = unsafe {
            ffi::blosc2_decompress(
                compressed.as_ptr() as *const _,
                compressed.len() as i32,
                c_decompressed.as_mut_ptr() as *mut _,
                c_decompressed.len() as i32,
            )
        };
        assert_eq!(
            dsize,
            data.len() as i32,
            "C decompress failed for splitmode={splitmode}"
        );
        assert_eq!(
            data, c_decompressed,
            "Rust→C splitmode={splitmode} mismatch"
        );
    }
}

#[test]
fn test_rust_frame_c_reads() {
    let _b = init_blosc2();

    let cparams = CParams {
        compcode: BLOSC_LZ4,
        clevel: 5,
        typesize: 4,
        splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
        filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
        ..Default::default()
    };
    let mut schunk = Schunk::new(cparams, Default::default());
    let chunks: Vec<Vec<u8>> = (0..3)
        .map(|chunk| {
            (0..2048u32)
                .flat_map(|i| (i + chunk * 2048).to_le_bytes())
                .collect()
        })
        .collect();
    for chunk in &chunks {
        schunk.append_buffer(chunk).unwrap();
    }

    let mut frame = schunk.to_frame();
    let c_schunk =
        unsafe { ffi::blosc2_schunk_from_buffer(frame.as_mut_ptr(), frame.len() as i64, true) };
    assert!(!c_schunk.is_null(), "C failed to open Rust-produced frame");

    unsafe {
        assert_eq!((*c_schunk).nchunks, chunks.len() as i64);
    }

    for (idx, expected) in chunks.iter().enumerate() {
        let mut restored = vec![0u8; expected.len()];
        let dsize = unsafe {
            ffi::blosc2_schunk_decompress_chunk(
                c_schunk,
                idx as i64,
                restored.as_mut_ptr() as *mut _,
                restored.len() as i32,
            )
        };
        assert_eq!(
            dsize,
            expected.len() as i32,
            "C failed to decompress Rust frame chunk {idx}"
        );
        assert_eq!(&restored, expected, "Rust frame chunk {idx} mismatch");
    }

    let rc = unsafe { ffi::blosc2_schunk_free(c_schunk) };
    assert_eq!(rc, 0);
}
