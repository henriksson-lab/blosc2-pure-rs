#![cfg(feature = "_ffi")]
//! Tier 1: Special value chunk tests
//! Tests SPECIAL_ZERO, SPECIAL_NAN, SPECIAL_UNINIT handling in compress/decompress.

use blosc2_pure_rs::compress::{compress, decompress, CParams};
use blosc2_pure_rs::constants::*;
mod common;
use blosc2_pure_rs::header::ChunkHeader;
use common::ffi;

fn init() -> common::Blosc2 {
    common::Blosc2::new()
}

// ─── All-zero data ───────────────────────────────────────────────

#[test]
fn test_compress_all_zeros_detected() {
    let data = vec![0u8; 40000];
    let cparams = CParams {
        compcode: BLOSC_LZ4,
        clevel: 5,
        typesize: 4,
        ..Default::default()
    };
    let chunk = compress(&data, &cparams).unwrap();
    let header = ChunkHeader::read(&chunk).unwrap();

    assert_eq!(
        header.special_type(),
        BLOSC2_SPECIAL_ZERO,
        "All-zero data should produce SPECIAL_ZERO chunk"
    );
    // Special zero chunks should be very small (just the header)
    assert!(
        chunk.len() <= BLOSC_EXTENDED_HEADER_LENGTH + 8,
        "SPECIAL_ZERO chunk should be tiny, got {} bytes",
        chunk.len()
    );

    let restored = decompress(&chunk).unwrap();
    assert_eq!(data, restored);
}

#[test]
fn test_compress_all_zeros_various_typesizes() {
    for typesize in [1, 2, 4, 8, 16] {
        let data = vec![0u8; 10000];
        let cparams = CParams {
            compcode: BLOSC_BLOSCLZ,
            clevel: 5,
            typesize,
            ..Default::default()
        };
        let chunk = compress(&data, &cparams).unwrap();
        let restored = decompress(&chunk).unwrap();
        assert_eq!(
            data, restored,
            "Zero roundtrip failed for typesize={typesize}"
        );
    }
}

#[test]
fn test_c_compressed_zeros_rust_decompress() {
    let _b = init();
    let data = vec![0u8; 20000];

    // Compress with C
    let mut c_chunk = vec![0u8; data.len() + BLOSC2_MAX_OVERHEAD];
    let csize = unsafe {
        let mut cp: ffi::blosc2_cparams = std::mem::zeroed();
        cp.compcode = BLOSC_LZ4;
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
    assert!(csize > 0, "C compression of zeros failed");

    // Decompress with Rust
    let restored = decompress(&c_chunk[..csize as usize]).unwrap();
    assert_eq!(
        data, restored,
        "C-compressed zeros → Rust decompress mismatch"
    );
}

// ─── All-NaN data ────────────────────────────────────────────────

#[test]
fn test_compress_all_nan_f32() {
    let nan_val = f32::NAN;
    let data: Vec<u8> = std::iter::repeat_n(nan_val.to_le_bytes(), 5000)
        .flatten()
        .collect();

    let cparams = CParams {
        compcode: BLOSC_LZ4,
        clevel: 5,
        typesize: 4,
        ..Default::default()
    };
    let chunk = compress(&data, &cparams).unwrap();
    let restored = decompress(&chunk).unwrap();

    // NaN != NaN, so compare bytes
    // The restored data should have NaN pattern in every 4 bytes
    assert_eq!(data.len(), restored.len());
    for i in (0..restored.len()).step_by(4) {
        let val = f32::from_le_bytes(restored[i..i + 4].try_into().unwrap());
        assert!(val.is_nan(), "Expected NaN at offset {i}, got {val}");
    }
}

#[test]
fn test_compress_all_nan_f64() {
    let nan_val = f64::NAN;
    let data: Vec<u8> = std::iter::repeat_n(nan_val.to_le_bytes(), 2500)
        .flatten()
        .collect();

    let cparams = CParams {
        compcode: BLOSC_ZSTD,
        clevel: 5,
        typesize: 8,
        ..Default::default()
    };
    let chunk = compress(&data, &cparams).unwrap();
    let restored = decompress(&chunk).unwrap();

    assert_eq!(data.len(), restored.len());
    for i in (0..restored.len()).step_by(8) {
        let val = f64::from_le_bytes(restored[i..i + 8].try_into().unwrap());
        assert!(val.is_nan(), "Expected NaN at offset {i}");
    }
}

// ─── Repeated non-zero value ─────────────────────────────────────

#[test]
fn test_compress_repeated_byte() {
    let data = vec![0xABu8; 20000];
    let cparams = CParams {
        compcode: BLOSC_LZ4,
        clevel: 5,
        typesize: 1,
        ..Default::default()
    };
    let chunk = compress(&data, &cparams).unwrap();
    let restored = decompress(&chunk).unwrap();
    assert_eq!(data, restored);
}

#[test]
fn test_compress_repeated_u32() {
    let val: u32 = 0xDEADBEEF;
    let data: Vec<u8> = std::iter::repeat_n(val.to_le_bytes(), 5000)
        .flatten()
        .collect();

    let cparams = CParams {
        compcode: BLOSC_BLOSCLZ,
        clevel: 9,
        typesize: 4,
        filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
        ..Default::default()
    };
    let chunk = compress(&data, &cparams).unwrap();
    let restored = decompress(&chunk).unwrap();
    assert_eq!(data, restored);
}

// ─── Mixed data patterns ────────────────────────────────────────

#[test]
fn test_mostly_zeros_some_nonzero() {
    let mut data = vec![0u8; 20000];
    data[100] = 1;
    data[5000] = 0xFF;
    data[19999] = 42;

    let cparams = CParams {
        compcode: BLOSC_LZ4,
        clevel: 5,
        typesize: 4,
        ..Default::default()
    };
    let chunk = compress(&data, &cparams).unwrap();
    let header = ChunkHeader::read(&chunk).unwrap();

    // Should NOT be SPECIAL_ZERO since there are non-zero bytes
    assert_ne!(
        header.special_type(),
        BLOSC2_SPECIAL_ZERO,
        "Mixed data should not be SPECIAL_ZERO"
    );

    let restored = decompress(&chunk).unwrap();
    assert_eq!(data, restored);
}

// ─── Schunk with special value chunks ────────────────────────────

#[test]
fn test_schunk_with_zero_chunks() {
    use blosc2_pure_rs::compress::DParams;
    use blosc2_pure_rs::schunk::Schunk;

    let cparams = CParams {
        compcode: BLOSC_LZ4,
        clevel: 5,
        typesize: 4,
        ..Default::default()
    };
    let dparams = DParams::default();
    let mut schunk = Schunk::new(cparams, dparams);

    let zeros = vec![0u8; 10000];
    let nonzero: Vec<u8> = (0..10000u32)
        .flat_map(|i| (i % 256).to_le_bytes())
        .collect();

    schunk.append_buffer(&zeros).unwrap();
    schunk.append_buffer(&nonzero[..10000]).unwrap();
    schunk.append_buffer(&zeros).unwrap();

    assert_eq!(schunk.nchunks(), 3);

    let d0 = schunk.decompress_chunk(0).unwrap();
    let d1 = schunk.decompress_chunk(1).unwrap();
    let d2 = schunk.decompress_chunk(2).unwrap();

    assert_eq!(d0, zeros);
    assert_eq!(d1, &nonzero[..10000]);
    assert_eq!(d2, zeros);
}

#[test]
fn test_schunk_frame_roundtrip_with_zeros() {
    use blosc2_pure_rs::compress::DParams;
    use blosc2_pure_rs::schunk::Schunk;

    let cparams = CParams {
        compcode: BLOSC_LZ4,
        clevel: 5,
        typesize: 4,
        ..Default::default()
    };
    let mut schunk = Schunk::new(cparams, DParams::default());

    let zeros = vec![0u8; 8000];
    schunk.append_buffer(&zeros).unwrap();
    schunk.append_buffer(&zeros).unwrap();

    let frame = schunk.to_frame();
    let schunk2 = Schunk::from_frame(&frame).unwrap();

    assert_eq!(schunk2.nchunks(), 2);
    assert_eq!(schunk2.decompress_chunk(0).unwrap(), zeros);
    assert_eq!(schunk2.decompress_chunk(1).unwrap(), zeros);
}
