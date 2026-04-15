#![cfg(feature = "_ffi")]
//! Tier 2: Multi-chunk stress tests and expanded cross-compatibility

use blosc2_pure_rs::compress::{CParams, DParams};
use blosc2_pure_rs::constants::*;
mod common;
use blosc2_pure_rs::schunk::Schunk;
use common::ffi;

fn init() -> common::Blosc2 {
    common::Blosc2::new()
}

// ─── Multi-chunk stress ──────────────────────────────────────────

#[test]
fn test_100_chunks() {
    let cparams = CParams {
        compcode: BLOSC_LZ4,
        clevel: 5,
        typesize: 4,
        filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
        ..Default::default()
    };
    let mut schunk = Schunk::new(cparams, DParams::default());

    let chunk_data: Vec<Vec<u8>> = (0..100)
        .map(|c| {
            (0..1000u32)
                .flat_map(|i| (i + c * 1000).to_le_bytes())
                .collect()
        })
        .collect();

    for data in &chunk_data {
        schunk.append_buffer(data).unwrap();
    }

    assert_eq!(schunk.nchunks(), 100);

    // Verify every chunk
    for (idx, expected) in chunk_data.iter().enumerate() {
        let restored = schunk.decompress_chunk(idx as i64).unwrap();
        assert_eq!(expected, &restored, "Chunk {idx} mismatch");
    }
}

#[test]
fn test_100_chunks_frame_roundtrip() {
    let cparams = CParams {
        compcode: BLOSC_ZSTD,
        clevel: 3,
        typesize: 4,
        filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
        ..Default::default()
    };
    let mut schunk = Schunk::new(cparams, DParams::default());

    let chunk_size = 4000;
    let nchunks = 100;

    // Append chunks with different data
    for c in 0..nchunks {
        let data: Vec<u8> = (0..chunk_size as u32)
            .flat_map(|i| (i.wrapping_mul(c as u32 + 1)).to_le_bytes())
            .collect();
        schunk.append_buffer(&data).unwrap();
    }

    // Serialize to frame and back
    let frame = schunk.to_frame();
    let schunk2 = Schunk::from_frame(&frame).unwrap();

    assert_eq!(schunk2.nchunks(), nchunks);

    // Verify all chunks
    for c in 0..nchunks {
        let orig = schunk.decompress_chunk(c).unwrap();
        let restored = schunk2.decompress_chunk(c).unwrap();
        assert_eq!(orig, restored, "Frame roundtrip chunk {c} mismatch");
    }
}

#[test]
fn test_100_chunks_file_roundtrip() {
    let cparams = CParams {
        compcode: BLOSC_LZ4,
        clevel: 5,
        typesize: 8,
        filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
        ..Default::default()
    };
    let mut schunk = Schunk::new(cparams, DParams::default());

    let nchunks = 50;
    let all_data: Vec<Vec<u8>> = (0..nchunks)
        .map(|c| {
            (0..500u64)
                .flat_map(|i| (i + c as u64 * 500).to_le_bytes())
                .collect()
        })
        .collect();

    for data in &all_data {
        schunk.append_buffer(data).unwrap();
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.b2frame");
    schunk.to_file(path.to_str().unwrap()).unwrap();

    let schunk2 = Schunk::open(path.to_str().unwrap()).unwrap();
    assert_eq!(schunk2.nchunks(), nchunks as i64);

    for (idx, expected) in all_data.iter().enumerate() {
        let restored = schunk2.decompress_chunk(idx as i64).unwrap();
        assert_eq!(expected, &restored, "File roundtrip chunk {idx} mismatch");
    }
}

#[test]
fn test_variable_last_chunk() {
    // Last chunk smaller than chunksize
    let cparams = CParams {
        compcode: BLOSC_LZ4,
        clevel: 5,
        typesize: 4,
        ..Default::default()
    };
    let mut schunk = Schunk::new(cparams, DParams::default());

    let full_chunk = vec![42u8; 10000];
    let partial_chunk = vec![99u8; 3000]; // Smaller

    schunk.append_buffer(&full_chunk).unwrap();
    schunk.append_buffer(&full_chunk).unwrap();
    schunk.append_buffer(&partial_chunk).unwrap();

    assert_eq!(schunk.nchunks(), 3);
    assert_eq!(schunk.decompress_chunk(0).unwrap(), full_chunk);
    assert_eq!(schunk.decompress_chunk(1).unwrap(), full_chunk);
    assert_eq!(schunk.decompress_chunk(2).unwrap(), partial_chunk);

    // Frame roundtrip with variable last chunk
    let frame = schunk.to_frame();
    let schunk2 = Schunk::from_frame(&frame).unwrap();
    assert_eq!(schunk2.decompress_chunk(2).unwrap(), partial_chunk);
}

// ─── Cross-compat matrix: C compress → Rust decompress ──────────

#[test]
fn test_c_compress_all_codecs_rust_decompress() {
    let _b = init();
    let data: Vec<u8> = (0..10000u32).flat_map(|i| i.to_le_bytes()).collect();

    for &compcode in &[
        BLOSC_BLOSCLZ,
        BLOSC_LZ4,
        BLOSC_LZ4HC,
        BLOSC_ZLIB,
        BLOSC_ZSTD,
    ] {
        let mut c_chunk = vec![0u8; data.len() + BLOSC2_MAX_OVERHEAD];
        let csize = unsafe {
            let mut cp: ffi::blosc2_cparams = std::mem::zeroed();
            cp.compcode = compcode;
            cp.clevel = 5;
            cp.typesize = 4;
            cp.nthreads = 1;
            cp.splitmode = BLOSC_FORWARD_COMPAT_SPLIT;
            cp.filters[BLOSC2_MAX_FILTERS - 1] = BLOSC_SHUFFLE;
            let cctx = ffi::blosc2_create_cctx(cp);
            let r = ffi::blosc2_compress_ctx(
                cctx,
                data.as_ptr() as *const _,
                data.len() as i32,
                c_chunk.as_mut_ptr() as *mut _,
                c_chunk.len() as i32,
            );
            ffi::blosc2_free_ctx(cctx);
            r
        };
        assert!(csize > 0, "C compress failed for codec={compcode}");

        let restored = blosc2_pure_rs::compress::decompress(&c_chunk[..csize as usize]).unwrap();
        assert_eq!(data, restored, "C→Rust codec={compcode} mismatch");
    }
}

#[test]
fn test_c_compress_all_filters_rust_decompress() {
    let _b = init();
    let data: Vec<u8> = (0..5000u32).flat_map(|i| i.to_le_bytes()).collect();

    for &filter in &[BLOSC_NOFILTER, BLOSC_SHUFFLE, BLOSC_BITSHUFFLE] {
        let mut c_chunk = vec![0u8; data.len() + BLOSC2_MAX_OVERHEAD];
        let csize = unsafe {
            let mut cp: ffi::blosc2_cparams = std::mem::zeroed();
            cp.compcode = BLOSC_LZ4;
            cp.clevel = 5;
            cp.typesize = 4;
            cp.nthreads = 1;
            cp.splitmode = BLOSC_FORWARD_COMPAT_SPLIT;
            cp.filters[BLOSC2_MAX_FILTERS - 1] = filter;
            let cctx = ffi::blosc2_create_cctx(cp);
            let r = ffi::blosc2_compress_ctx(
                cctx,
                data.as_ptr() as *const _,
                data.len() as i32,
                c_chunk.as_mut_ptr() as *mut _,
                c_chunk.len() as i32,
            );
            ffi::blosc2_free_ctx(cctx);
            r
        };
        assert!(csize > 0, "C compress failed for filter={filter}");

        let restored = blosc2_pure_rs::compress::decompress(&c_chunk[..csize as usize]).unwrap();
        assert_eq!(data, restored, "C→Rust filter={filter} mismatch");
    }
}

#[test]
fn test_c_compress_all_splitmodes_rust_decompress() {
    let _b = init();
    let data: Vec<u8> = (0..10000u32).flat_map(|i| i.to_le_bytes()).collect();

    for &splitmode in &[1i32, 2, 4] {
        // ALWAYS, NEVER, FORWARD_COMPAT
        let mut c_chunk = vec![0u8; data.len() + BLOSC2_MAX_OVERHEAD];
        let csize = unsafe {
            let mut cp: ffi::blosc2_cparams = std::mem::zeroed();
            cp.compcode = BLOSC_LZ4;
            cp.clevel = 5;
            cp.typesize = 4;
            cp.nthreads = 1;
            cp.splitmode = splitmode;
            cp.filters[BLOSC2_MAX_FILTERS - 1] = BLOSC_SHUFFLE;
            let cctx = ffi::blosc2_create_cctx(cp);
            let r = ffi::blosc2_compress_ctx(
                cctx,
                data.as_ptr() as *const _,
                data.len() as i32,
                c_chunk.as_mut_ptr() as *mut _,
                c_chunk.len() as i32,
            );
            ffi::blosc2_free_ctx(cctx);
            r
        };
        assert!(csize > 0, "C compress failed for splitmode={splitmode}");

        let restored = blosc2_pure_rs::compress::decompress(&c_chunk[..csize as usize]).unwrap();
        assert_eq!(data, restored, "C→Rust splitmode={splitmode} mismatch");
    }
}

// ─── Large data test ─────────────────────────────────────────────

#[test]
fn test_large_data_10mb() {
    let data: Vec<u8> = (0..2500000u32).flat_map(|i| i.to_le_bytes()).collect();
    assert_eq!(data.len(), 10_000_000);

    let cparams = CParams {
        compcode: BLOSC_LZ4,
        clevel: 5,
        typesize: 4,
        filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
        ..Default::default()
    };
    let mut schunk = Schunk::new(cparams, DParams::default());

    // Append in 1MB chunks
    for chunk_start in (0..data.len()).step_by(1_000_000) {
        let chunk_end = (chunk_start + 1_000_000).min(data.len());
        schunk.append_buffer(&data[chunk_start..chunk_end]).unwrap();
    }

    // Verify all chunks
    let mut restored = Vec::new();
    for i in 0..schunk.nchunks() {
        restored.extend(schunk.decompress_chunk(i).unwrap());
    }
    assert_eq!(data, restored);
}
