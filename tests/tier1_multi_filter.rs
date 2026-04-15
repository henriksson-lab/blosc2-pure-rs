#![cfg(feature = "_ffi")]
//! Tier 1: Multi-filter pipeline tests
//! Tests combined filters: DELTA+SHUFFLE, BITSHUFFLE+DELTA, etc.

use blosc2_pure_rs::compress::{compress, decompress, CParams};
use blosc2_pure_rs::constants::*;
mod common;
use blosc2_pure_rs::filters;
use common::ffi;

fn init() -> common::Blosc2 {
    common::Blosc2::new()
}

fn sequential_f32(n: usize) -> Vec<u8> {
    (0..n as u32)
        .flat_map(|i| (i as f32).to_le_bytes())
        .collect()
}

fn sequential_u64(n: usize) -> Vec<u8> {
    (0..n as u64).flat_map(|i| i.to_le_bytes()).collect()
}

// ─── Pipeline forward/backward roundtrip ─────────────────────────

#[test]
fn test_pipeline_delta_shuffle_roundtrip() {
    let data: Vec<u8> = sequential_f32(2048);
    let bsize = data.len();
    let mut buf1 = vec![0u8; bsize];
    let mut buf2 = vec![0u8; bsize];

    let filter_array = [0, 0, 0, 0, BLOSC_DELTA, BLOSC_SHUFFLE];
    let meta = [0u8; BLOSC2_MAX_FILTERS];

    let result_buf = filters::pipeline_forward(
        &data,
        &mut buf1,
        &mut buf2,
        &filter_array,
        &meta,
        4,
        0,
        None,
    );

    // Data should be transformed
    let filtered = if result_buf == 1 {
        &buf1[..bsize]
    } else {
        &buf2[..bsize]
    };
    assert_ne!(&data[..], filtered, "Filters should transform data");

    // Reverse
    let mut rbuf1 = filtered.to_vec();
    let mut rbuf2 = vec![0u8; bsize];

    let restored_buf = filters::pipeline_backward(
        &mut rbuf1,
        &mut rbuf2,
        bsize,
        &filter_array,
        &meta,
        4,
        0,
        None,
        1,
    );

    let restored = if restored_buf == 1 {
        &rbuf1[..bsize]
    } else {
        &rbuf2[..bsize]
    };
    assert_eq!(
        &data[..],
        restored,
        "DELTA+SHUFFLE pipeline roundtrip failed"
    );
}

#[test]
fn test_pipeline_shuffle_only_roundtrip() {
    let data: Vec<u8> = sequential_f32(1024);
    let bsize = data.len();
    let mut buf1 = vec![0u8; bsize];
    let mut buf2 = vec![0u8; bsize];

    let filter_array = [0, 0, 0, 0, 0, BLOSC_SHUFFLE];
    let meta = [0u8; BLOSC2_MAX_FILTERS];

    let result_buf = filters::pipeline_forward(
        &data,
        &mut buf1,
        &mut buf2,
        &filter_array,
        &meta,
        4,
        0,
        None,
    );
    let filtered = if result_buf == 1 {
        &buf1[..bsize]
    } else {
        &buf2[..bsize]
    };

    let mut rbuf1 = filtered.to_vec();
    let mut rbuf2 = vec![0u8; bsize];
    let restored_buf = filters::pipeline_backward(
        &mut rbuf1,
        &mut rbuf2,
        bsize,
        &filter_array,
        &meta,
        4,
        0,
        None,
        1,
    );
    let restored = if restored_buf == 1 {
        &rbuf1[..bsize]
    } else {
        &rbuf2[..bsize]
    };
    assert_eq!(
        &data[..],
        restored,
        "SHUFFLE-only pipeline roundtrip failed"
    );
}

#[test]
fn test_pipeline_bitshuffle_only_roundtrip() {
    // Bitshuffle needs size to be multiple of 8*typesize
    let data: Vec<u8> = sequential_f32(1024); // 4096 bytes, 1024 elements
    let bsize = data.len();
    let mut buf1 = vec![0u8; bsize];
    let mut buf2 = vec![0u8; bsize];

    let filter_array = [0, 0, 0, 0, 0, BLOSC_BITSHUFFLE];
    let meta = [0u8; BLOSC2_MAX_FILTERS];

    let result_buf = filters::pipeline_forward(
        &data,
        &mut buf1,
        &mut buf2,
        &filter_array,
        &meta,
        4,
        0,
        None,
    );
    let filtered = if result_buf == 1 {
        &buf1[..bsize]
    } else {
        &buf2[..bsize]
    };

    let mut rbuf1 = filtered.to_vec();
    let mut rbuf2 = vec![0u8; bsize];
    let restored_buf = filters::pipeline_backward(
        &mut rbuf1,
        &mut rbuf2,
        bsize,
        &filter_array,
        &meta,
        4,
        0,
        None,
        1,
    );
    let restored = if restored_buf == 1 {
        &rbuf1[..bsize]
    } else {
        &rbuf2[..bsize]
    };
    assert_eq!(
        &data[..],
        restored,
        "BITSHUFFLE-only pipeline roundtrip failed"
    );
}

// ─── Full compress/decompress with multi-filter ──────────────────

#[test]
fn test_compress_delta_shuffle() {
    let data = sequential_f32(5000);
    let cparams = CParams {
        compcode: BLOSC_LZ4,
        clevel: 5,
        typesize: 4,
        filters: [0, 0, 0, 0, BLOSC_DELTA, BLOSC_SHUFFLE],
        ..Default::default()
    };
    let chunk = compress(&data, &cparams).unwrap();
    let restored = decompress(&chunk).unwrap();
    assert_eq!(data, restored, "DELTA+SHUFFLE compress roundtrip failed");
}

#[test]
fn test_compress_delta_bitshuffle() {
    let data = sequential_u64(1024);
    let cparams = CParams {
        compcode: BLOSC_ZSTD,
        clevel: 5,
        typesize: 8,
        filters: [0, 0, 0, 0, BLOSC_DELTA, BLOSC_BITSHUFFLE],
        ..Default::default()
    };
    let chunk = compress(&data, &cparams).unwrap();
    let restored = decompress(&chunk).unwrap();
    assert_eq!(data, restored, "DELTA+BITSHUFFLE compress roundtrip failed");
}

#[test]
fn test_compress_trunc_prec_shuffle() {
    let data = sequential_f32(5000);
    let cparams = CParams {
        compcode: BLOSC_LZ4,
        clevel: 5,
        typesize: 4,
        // TRUNC_PREC with 16 significant bits + SHUFFLE
        filters: [0, 0, 0, 0, BLOSC_TRUNC_PREC, BLOSC_SHUFFLE],
        filters_meta: [0, 0, 0, 0, 16, 0],
        ..Default::default()
    };
    let chunk = compress(&data, &cparams).unwrap();
    let restored = decompress(&chunk).unwrap();

    // TRUNC_PREC is lossy, so data won't match exactly
    // But the roundtrip should be stable (compress again should give same result)
    assert_eq!(data.len(), restored.len());
    let chunk2 = compress(&restored, &cparams).unwrap();
    let restored2 = decompress(&chunk2).unwrap();
    assert_eq!(
        restored, restored2,
        "TRUNC_PREC should be stable after first application"
    );
}

// ─── Cross-check multi-filter with C FFI ─────────────────────────

#[test]
fn test_c_delta_shuffle_rust_decompress() {
    let _b = init();
    let data = sequential_f32(5000);
    let src_size = data.len() as i32;

    let mut c_chunk = vec![0u8; data.len() + BLOSC2_MAX_OVERHEAD];
    let csize = unsafe {
        let mut cp: ffi::blosc2_cparams = std::mem::zeroed();
        cp.compcode = BLOSC_LZ4;
        cp.clevel = 5;
        cp.typesize = 4;
        cp.nthreads = 1;
        cp.splitmode = BLOSC_FORWARD_COMPAT_SPLIT;
        cp.filters[4] = BLOSC_DELTA;
        cp.filters[5] = BLOSC_SHUFFLE;
        let cctx = ffi::blosc2_create_cctx(cp);
        let r = ffi::blosc2_compress_ctx(
            cctx,
            data.as_ptr() as *const _,
            src_size,
            c_chunk.as_mut_ptr() as *mut _,
            c_chunk.len() as i32,
        );
        ffi::blosc2_free_ctx(cctx);
        r
    };
    assert!(csize > 0, "C DELTA+SHUFFLE compression failed");

    let restored = decompress(&c_chunk[..csize as usize]).unwrap();
    assert_eq!(data, restored, "C DELTA+SHUFFLE → Rust decompress mismatch");
}

#[test]
fn test_rust_delta_shuffle_c_decompress() {
    let _b = init();
    let data = sequential_f32(5000);

    let cparams = CParams {
        compcode: BLOSC_LZ4,
        clevel: 5,
        typesize: 4,
        filters: [0, 0, 0, 0, BLOSC_DELTA, BLOSC_SHUFFLE],
        ..Default::default()
    };
    let chunk = compress(&data, &cparams).unwrap();

    let mut c_restored = vec![0u8; data.len()];
    let dsize = unsafe {
        ffi::blosc2_decompress(
            chunk.as_ptr() as *const _,
            chunk.len() as i32,
            c_restored.as_mut_ptr() as *mut _,
            c_restored.len() as i32,
        )
    };
    assert_eq!(
        dsize,
        data.len() as i32,
        "C decompress of Rust DELTA+SHUFFLE failed"
    );
    assert_eq!(
        data, c_restored,
        "Rust DELTA+SHUFFLE → C decompress mismatch"
    );
}

// ─── All codec × multi-filter combinations ───────────────────────

#[test]
fn test_all_codecs_delta_shuffle() {
    let data = sequential_f32(5000);
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
            typesize: 4,
            filters: [0, 0, 0, 0, BLOSC_DELTA, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let chunk = compress(&data, &cparams).unwrap();
        let restored = decompress(&chunk).unwrap();
        assert_eq!(data, restored, "DELTA+SHUFFLE failed for codec={compcode}");
    }
}

#[test]
fn test_three_filters_stacked() {
    // DELTA at position 3, TRUNC_PREC at 4, SHUFFLE at 5
    let data = sequential_f32(2048);
    let cparams = CParams {
        compcode: BLOSC_LZ4,
        clevel: 5,
        typesize: 4,
        filters: [0, 0, 0, BLOSC_DELTA, BLOSC_TRUNC_PREC, BLOSC_SHUFFLE],
        filters_meta: [0, 0, 0, 0, 20, 0], // 20 bits precision
        ..Default::default()
    };
    let chunk = compress(&data, &cparams).unwrap();
    let restored = decompress(&chunk).unwrap();

    // TRUNC_PREC is lossy, so verify stability
    assert_eq!(data.len(), restored.len());
    let chunk2 = compress(&restored, &cparams).unwrap();
    let restored2 = decompress(&chunk2).unwrap();
    assert_eq!(restored, restored2, "Three-filter stack should be stable");
}
