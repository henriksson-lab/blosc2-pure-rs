#![cfg(feature = "_ffi")]
use blosc2_pure_rs::ffi;
use blosc2_pure_rs::compress::{compress, decompress, CParams};
use blosc2_pure_rs::constants::*;

fn init_blosc2() -> blosc2_pure_rs::Blosc2 {
    blosc2_pure_rs::Blosc2::new()
}

/// Compress with C FFI, decompress with pure Rust engine
#[test]
fn test_c_compress_rust_decompress() {
    let _b = init_blosc2();

    let data: Vec<u8> = (0..10000u32)
        .flat_map(|i| i.to_le_bytes())
        .collect();
    let src_size = data.len() as i32;
    let buf_size = src_size as usize + BLOSC2_MAX_OVERHEAD;

    for &compcode in &[BLOSC_BLOSCLZ, BLOSC_LZ4, BLOSC_LZ4HC, BLOSC_ZLIB, BLOSC_ZSTD] {
        let mut compressed = vec![0u8; buf_size];
        let csize = unsafe {
            let mut cparams: ffi::blosc2_cparams = std::mem::zeroed();
            cparams.compcode = compcode;
            cparams.clevel = 5;
            cparams.typesize = 4;
            cparams.nthreads = 1;
            cparams.splitmode = BLOSC_FORWARD_COMPAT_SPLIT as i32;
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

    let data: Vec<u8> = (0..10000u32)
        .flat_map(|i| i.to_le_bytes())
        .collect();

    for &compcode in &[BLOSC_LZ4, BLOSC_ZLIB, BLOSC_ZSTD] {
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
            dsize, data.len() as i32,
            "C decompress size mismatch for codec={compcode}: got {dsize}"
        );
        assert_eq!(data, c_decompressed, "Rust→C mismatch for codec={compcode}");
    }
}

/// Test with various filter combinations
#[test]
fn test_cross_compat_filters() {
    let _b = init_blosc2();

    let data: Vec<u8> = (0..5000u32)
        .flat_map(|i| i.to_le_bytes())
        .collect();

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
        assert_eq!(dsize, data.len() as i32, "C decompress failed for filter={filter}");
        assert_eq!(data, c_decompressed, "C decompress mismatch for filter={filter}");
    }
}
