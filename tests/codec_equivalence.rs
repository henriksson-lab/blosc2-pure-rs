#![cfg(feature = "_ffi")]
use blosc2_pure_rs::compress::CParams;
use blosc2_pure_rs::constants::*;
use blosc2_pure_rs::{codecs, compress};
mod common;
use common::ffi;

fn init_blosc2() -> common::Blosc2 {
    common::Blosc2::new()
}

#[test]
fn test_ndlz_c_plugin_decode_fixtures_for_supported_meta_values() {
    let _b = init_blosc2();

    for (meta, rows, cols, payload) in [
        (4, 3i32, 4i32, (1..=12).collect::<Vec<_>>()),
        (8, 2i32, 3i32, vec![20, 21, 22, 23, 24, 25]),
    ] {
        let mut encoded = vec![2];
        encoded.extend_from_slice(&rows.to_le_bytes());
        encoded.extend_from_slice(&cols.to_le_bytes());
        encoded.push(0);
        encoded.extend_from_slice(&payload);

        let mut decoded = vec![0; payload.len()];
        assert_eq!(
            codecs::decompress_block_with_meta(BLOSC_CODEC_NDLZ, meta, &encoded, &mut decoded),
            payload.len() as i32,
            "NDLZ meta={meta} literal fixture must decode"
        );
        assert_eq!(
            decoded, payload,
            "NDLZ meta={meta} literal payload mismatch"
        );
    }
}

#[test]
fn test_ndlz_c_plugin_repeat_and_reject_status_fixtures() {
    let _b = init_blosc2();

    let mut repeat_cell = vec![2];
    repeat_cell.extend_from_slice(&4i32.to_le_bytes());
    repeat_cell.extend_from_slice(&4i32.to_le_bytes());
    repeat_cell.push(0x40);
    repeat_cell.push(7);

    let mut decoded = vec![0; 16];
    assert_eq!(
        codecs::decompress_block_with_meta(BLOSC_CODEC_NDLZ, 4, &repeat_cell, &mut decoded),
        decoded.len() as i32
    );
    assert_eq!(decoded, vec![7; 16]);

    assert_eq!(
        codecs::decompress_block_with_meta(BLOSC_CODEC_NDLZ, 5, &repeat_cell, &mut decoded),
        -1,
        "NDLZ only supports C plugin cell metadata 4 and 8"
    );

    let cparams = CParams {
        compcode: BLOSC_CODEC_NDLZ,
        compcode_meta: 4,
        typesize: 4,
        filters: [0; BLOSC2_MAX_FILTERS],
        ..Default::default()
    };
    assert!(compress::blosc2_create_cctx(cparams.clone()).is_ok());
    assert_eq!(
        compress::compress(&(0..128u8).collect::<Vec<_>>(), &cparams).unwrap_err(),
        "Codec compression failed"
    );
}

#[test]
fn test_ndlz_rust_encoder_emits_c_wire_full_cell_match_fixtures() {
    let _b = init_blosc2();

    for (meta, rows, cols, cell_shape, expected_cbytes) in [
        (4u8, 4i32, 8i32, 4usize, 29i32),
        (8u8, 8i32, 16i32, 8usize, 77i32),
    ] {
        let first_cell: Vec<u8> = (0..cell_shape * cell_shape)
            .map(|i| ((i * 5 + 3) % 251) as u8)
            .collect();
        let mut input = Vec::with_capacity((rows * cols) as usize);
        for row in 0..cell_shape {
            input.extend_from_slice(&first_cell[row * cell_shape..row * cell_shape + cell_shape]);
            input.extend_from_slice(&first_cell[row * cell_shape..row * cell_shape + cell_shape]);
        }

        let mut encoded = vec![0; input.len() + 64];
        let cbytes = codecs::ndlz_compress_block_2d(meta, [rows, cols], &input, &mut encoded);
        assert_eq!(cbytes, expected_cbytes);
        let match_token_pos = 9 + 1 + cell_shape * cell_shape;
        assert_eq!(encoded[match_token_pos], 0xc0);
        assert_eq!(
            u16::from_le_bytes([encoded[match_token_pos + 1], encoded[match_token_pos + 2]]),
            (cell_shape * cell_shape) as u16
        );

        let mut decoded = vec![0; input.len()];
        assert_eq!(
            codecs::decompress_block_with_meta(
                BLOSC_CODEC_NDLZ,
                meta,
                &encoded[..cbytes as usize],
                &mut decoded,
            ),
            input.len() as i32,
            "Rust-emitted NDLZ full-cell match fixture must decode for meta={meta}"
        );
        assert_eq!(decoded, input);
    }
}

#[test]
fn test_zfp_c_plugin_modes_are_explicitly_unsupported_until_codec_abi_exists() {
    let _b = init_blosc2();

    for (compcode, compcode_meta) in [
        (BLOSC_CODEC_ZFP_FIXED_ACCURACY, (-1i8) as u8),
        (BLOSC_CODEC_ZFP_FIXED_PRECISION, 25),
        (BLOSC_CODEC_ZFP_FIXED_RATE, 45),
    ] {
        let cparams = CParams {
            compcode,
            compcode_meta,
            typesize: 4,
            filters: [0; BLOSC2_MAX_FILTERS],
            splitmode: BLOSC_NEVER_SPLIT,
            ..Default::default()
        };

        assert!(compress::blosc2_create_cctx(cparams.clone()).is_ok());
        assert_eq!(
            compress::compress(&[0u8; 128], &cparams).unwrap_err(),
            "ZFP plugin codecs are not supported",
            "ZFP compcode={compcode} compression must fail before writing data"
        );
    }
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

    // Decompress with Rust for the cross-compatibility assertion.
    let rust_decompressed =
        compress::decompress(&compressed[..csize as usize]).expect("Rust decompression failed");
    assert_eq!(rust_decompressed, data, "Rust decompression mismatch");

    // Decompress with C too, so failures are easier to diagnose.
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
