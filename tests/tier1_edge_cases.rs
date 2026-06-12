#![cfg(feature = "_ffi")]
//! Tier 1: Split modes, large typesizes, non-aligned sizes, incompressible data

use blosc2_pure_rs::b2nd::{
    b2nd_get_slice_cbuffer, b2nd_get_slice_cbuffer_c, b2nd_get_slice_nchunks,
    b2nd_set_slice_cbuffer, b2nd_set_slice_cbuffer_c, B2ndArray, B2ndMeta,
};
use blosc2_pure_rs::compress::{blosc2_decompress, compress, decompress, CParams, DParams};
use blosc2_pure_rs::constants::*;
use blosc2_pure_rs::schunk::{
    blosc2_schunk_delete_chunk, blosc2_schunk_insert_chunk, blosc2_schunk_update_chunk, Schunk,
};
mod common;
use blosc2_pure_rs::filters;
use common::ffi;
use std::ffi::c_void;

unsafe extern "C" {
    fn free(ptr: *mut c_void);
}

fn init() -> common::Blosc2 {
    common::Blosc2::new()
}

fn c_compress(data: &[u8], compcode: u8, typesize: i32, splitmode: i32, filter: u8) -> Vec<u8> {
    let mut c_chunk = vec![0u8; data.len() + BLOSC2_MAX_OVERHEAD];
    let csize = unsafe {
        let mut cp: ffi::blosc2_cparams = std::mem::zeroed();
        cp.compcode = compcode;
        cp.clevel = 5;
        cp.typesize = typesize;
        cp.nthreads = 1;
        cp.splitmode = splitmode;
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
    assert!(
        csize > 0,
        "C compress failed for codec={compcode} typesize={typesize} splitmode={splitmode}"
    );
    c_chunk.truncate(csize as usize);
    c_chunk
}

fn assert_c_decompresses_to(chunk: &[u8], expected: &[u8], context: &str) {
    let mut c_restored = vec![0u8; expected.len()];
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
        expected.len() as i32,
        "C decompress failed for {context}"
    );
    assert_eq!(expected, c_restored, "C decompress mismatch for {context}");
}

fn assert_decompress_error_matches_c(
    chunk: &[u8],
    dest_len: usize,
    expected_code: i32,
    context: &str,
) {
    assert_decompress_error_with_sizes_matches_c(
        chunk,
        chunk.len() as i32,
        dest_len,
        dest_len as i32,
        expected_code,
        context,
    );
}

fn assert_decompress_error_with_sizes_matches_c(
    chunk: &[u8],
    srcsize: i32,
    dest_len: usize,
    destsize: i32,
    expected_code: i32,
    context: &str,
) {
    let mut rust_dest = vec![0u8; dest_len];
    let rust_code = blosc2_decompress(chunk, srcsize, &mut rust_dest, destsize);

    let mut c_dest = vec![0u8; dest_len];
    let c_code = unsafe {
        ffi::blosc2_decompress(
            chunk.as_ptr() as *const _,
            srcsize,
            c_dest.as_mut_ptr() as *mut _,
            destsize,
        )
    };

    assert_eq!(
        rust_code, c_code,
        "Rust/C return-code mismatch for {context}"
    );
    assert_eq!(
        rust_code, expected_code,
        "Unexpected return code for {context}"
    );
}

fn schunk_cparams() -> CParams {
    CParams {
        compcode: BLOSC_BLOSCLZ,
        clevel: 5,
        typesize: 1,
        splitmode: BLOSC_NEVER_SPLIT,
        filters: [0, 0, 0, 0, 0, BLOSC_NOFILTER],
        ..Default::default()
    }
}

fn compressed_chunk(data: &[u8]) -> Vec<u8> {
    let mut source = Schunk::new(schunk_cparams(), DParams::default());
    source.append_buffer(data).unwrap();
    source.compressed_chunk(0).unwrap().to_vec()
}

fn frame_chunksize(frame: &[u8]) -> i32 {
    i32::from_be_bytes(frame[58..62].try_into().unwrap())
}

fn c_schunk_frame_after_buffers_and_mutation(
    initial: &[&[u8]],
    mutate: impl FnOnce(*mut ffi::blosc2_schunk),
) -> (i32, i64, Vec<u8>) {
    unsafe {
        let mut cparams: ffi::blosc2_cparams = std::mem::zeroed();
        cparams.compcode = BLOSC_BLOSCLZ;
        cparams.clevel = 5;
        cparams.typesize = 1;
        cparams.nthreads = 1;
        cparams.splitmode = BLOSC_NEVER_SPLIT;
        cparams.filters[BLOSC2_MAX_FILTERS - 1] = BLOSC_NOFILTER;

        let mut dparams: ffi::blosc2_dparams = std::mem::zeroed();
        dparams.nthreads = 1;
        dparams.typesize = 1;

        let mut storage = ffi::blosc2_storage {
            contiguous: true,
            urlpath: std::ptr::null_mut(),
            cparams: &mut cparams,
            dparams: &mut dparams,
            io: std::ptr::null_mut(),
        };
        let schunk = ffi::blosc2_schunk_new(&mut storage);
        assert!(!schunk.is_null(), "C failed to create schunk");

        for data in initial {
            let rc =
                ffi::blosc2_schunk_append_buffer(schunk, data.as_ptr().cast(), data.len() as i32);
            assert!(rc >= 0, "C append_buffer failed: {rc}");
        }

        mutate(schunk);

        let chunksize = (*schunk).chunksize;
        let nbytes = (*schunk).nbytes;
        let mut frame_ptr: *mut u8 = std::ptr::null_mut();
        let mut needs_free = false;
        let frame_len = ffi::blosc2_schunk_to_buffer(schunk, &mut frame_ptr, &mut needs_free);
        assert!(frame_len > 0, "C to_buffer failed: {frame_len}");
        assert!(!frame_ptr.is_null());
        let frame = std::slice::from_raw_parts(frame_ptr, frame_len as usize).to_vec();
        if needs_free {
            free(frame_ptr.cast());
        }
        assert_eq!(ffi::blosc2_schunk_free(schunk), 0);

        (chunksize, nbytes, frame)
    }
}

// ─── Block splitting modes ───────────────────────────────────────

#[test]
fn test_always_split() {
    let data: Vec<u8> = (0..20000u32).flat_map(|i| i.to_le_bytes()).collect();
    for compcode in [BLOSC_BLOSCLZ, BLOSC_LZ4, BLOSC_ZSTD] {
        let cparams = CParams {
            compcode,
            clevel: 5,
            typesize: 4,
            splitmode: BLOSC_ALWAYS_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let chunk = compress(&data, &cparams).unwrap();
        let restored = decompress(&chunk).unwrap();
        assert_eq!(data, restored, "ALWAYS_SPLIT failed for codec={compcode}");
    }
}

#[test]
fn test_never_split() {
    let data: Vec<u8> = (0..20000u32).flat_map(|i| i.to_le_bytes()).collect();
    for compcode in [BLOSC_BLOSCLZ, BLOSC_LZ4, BLOSC_ZSTD] {
        let cparams = CParams {
            compcode,
            clevel: 5,
            typesize: 4,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let chunk = compress(&data, &cparams).unwrap();
        let restored = decompress(&chunk).unwrap();
        assert_eq!(data, restored, "NEVER_SPLIT failed for codec={compcode}");
    }
}

#[test]
fn test_forward_compat_split() {
    let data: Vec<u8> = (0..20000u32).flat_map(|i| i.to_le_bytes()).collect();
    let cparams = CParams {
        compcode: BLOSC_LZ4,
        clevel: 5,
        typesize: 4,
        splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
        filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
        ..Default::default()
    };
    let chunk = compress(&data, &cparams).unwrap();
    let restored = decompress(&chunk).unwrap();
    assert_eq!(data, restored);
}

#[test]
fn test_split_mode_cross_compat_with_c() {
    let _b = init();
    let data: Vec<u8> = (0..10000u32).flat_map(|i| i.to_le_bytes()).collect();

    for splitmode in [
        BLOSC_ALWAYS_SPLIT,
        BLOSC_NEVER_SPLIT,
        BLOSC_AUTO_SPLIT,
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
        let chunk = compress(&data, &cparams).unwrap();
        assert_c_decompresses_to(&chunk, &data, &format!("splitmode={splitmode}"));

        let c_chunk = c_compress(&data, BLOSC_LZ4, 4, splitmode, BLOSC_SHUFFLE);
        let rust_restored = decompress(&c_chunk)
            .unwrap_or_else(|e| panic!("Rust decompress failed for splitmode={splitmode}: {e}"));
        assert_eq!(
            data, rust_restored,
            "Rust decompress mismatch for C splitmode={splitmode}"
        );
    }
}

// ─── Large typesizes ─────────────────────────────────────────────

#[test]
fn test_typesize_16() {
    let _b = init();
    let data: Vec<u8> = (0..5000u16)
        .map(|i| (i.wrapping_mul(7) & 0xFF) as u8)
        .collect();
    let cparams = CParams {
        compcode: BLOSC_LZ4,
        clevel: 5,
        typesize: 16,
        ..Default::default()
    };
    let chunk = compress(&data, &cparams).unwrap();
    let restored = decompress(&chunk).unwrap();
    assert_eq!(data, restored);
    assert_c_decompresses_to(&chunk, &data, "typesize=16");
}

#[test]
fn test_typesize_32() {
    let _b = init();
    let data: Vec<u8> = (0..10000u16)
        .map(|i| (i.wrapping_mul(13) & 0xFF) as u8)
        .collect();
    let cparams = CParams {
        compcode: BLOSC_ZSTD,
        clevel: 5,
        typesize: 32,
        ..Default::default()
    };
    let chunk = compress(&data, &cparams).unwrap();
    let restored = decompress(&chunk).unwrap();
    assert_eq!(data, restored);
    assert_c_decompresses_to(&chunk, &data, "typesize=32");
}

#[test]
fn test_typesize_64() {
    let _b = init();
    let data: Vec<u8> = (0..20000u16)
        .map(|i| (i.wrapping_mul(11) & 0xFF) as u8)
        .collect();
    let cparams = CParams {
        compcode: BLOSC_LZ4,
        clevel: 5,
        typesize: 64,
        ..Default::default()
    };
    let chunk = compress(&data, &cparams).unwrap();
    let restored = decompress(&chunk).unwrap();
    assert_eq!(data, restored);
    assert_c_decompresses_to(&chunk, &data, "typesize=64");
}

#[test]
fn test_typesize_128() {
    let _b = init();
    let data: Vec<u8> = (0..20000u16)
        .map(|i| (i.wrapping_add(42) & 0xFF) as u8)
        .collect();
    let cparams = CParams {
        compcode: BLOSC_BLOSCLZ,
        clevel: 5,
        typesize: 128,
        ..Default::default()
    };
    let chunk = compress(&data, &cparams).unwrap();
    let restored = decompress(&chunk).unwrap();
    assert_eq!(data, restored);
    assert_c_decompresses_to(&chunk, &data, "typesize=128");
}

#[test]
fn test_typesize_255() {
    let _b = init();
    // 255 is BLOSC_MAX_TYPESIZE
    let data = vec![0xABu8; 255 * 100]; // 25500 bytes, exactly 100 elements
    let cparams = CParams {
        compcode: BLOSC_LZ4,
        clevel: 5,
        typesize: 255,
        ..Default::default()
    };
    let chunk = compress(&data, &cparams).unwrap();
    let restored = decompress(&chunk).unwrap();
    assert_eq!(data, restored);
    assert_c_decompresses_to(&chunk, &data, "typesize=255");
}

#[test]
fn test_large_typesize_shuffle_roundtrip() {
    for typesize in [16, 32, 64, 128, 255, 256] {
        let n = 1024;
        let data: Vec<u8> = (0..n).map(|i| (i % 256) as u8).collect();
        let mut shuffled = vec![0u8; n];
        let mut restored = vec![0u8; n];

        filters::shuffle(typesize, &data, &mut shuffled);
        filters::unshuffle(typesize, &shuffled, &mut restored);
        assert_eq!(
            data, restored,
            "Shuffle roundtrip failed for typesize={typesize}"
        );
    }
}

#[test]
fn test_raw_shuffle_large_typesize_boundary_matches_c() {
    let _b = init();

    for typesize in [255, 256] {
        let data: Vec<u8> = (0..(typesize * 2 + 1))
            .map(|i: i32| (i.wrapping_mul(17).wrapping_add(23) % 251) as u8)
            .collect();
        let mut rust_shuffled = vec![0u8; data.len()];
        let mut rust_restored = vec![0u8; data.len()];
        let mut c_shuffled = vec![0u8; data.len()];
        let mut c_restored = vec![0u8; data.len()];

        let rust_code = filters::blosc2_shuffle(
            typesize as i32,
            data.len() as i32,
            &data,
            &mut rust_shuffled,
        );
        let c_code = unsafe {
            ffi::blosc2_shuffle(
                typesize as i32,
                data.len() as i32,
                data.as_ptr().cast(),
                c_shuffled.as_mut_ptr().cast(),
            )
        };
        assert_eq!(
            rust_code, c_code,
            "shuffle return code for typesize={typesize}"
        );
        assert_eq!(
            rust_shuffled, c_shuffled,
            "shuffle bytes for typesize={typesize}"
        );

        let rust_code = filters::blosc2_unshuffle(
            typesize as i32,
            data.len() as i32,
            &rust_shuffled,
            &mut rust_restored,
        );
        let c_code = unsafe {
            ffi::blosc2_unshuffle(
                typesize as i32,
                data.len() as i32,
                c_shuffled.as_ptr().cast(),
                c_restored.as_mut_ptr().cast(),
            )
        };
        assert_eq!(
            rust_code, c_code,
            "unshuffle return code for typesize={typesize}"
        );
        assert_eq!(
            rust_restored, c_restored,
            "unshuffle bytes for typesize={typesize}"
        );
        assert_eq!(
            rust_restored, data,
            "unshuffle roundtrip for typesize={typesize}"
        );
    }

    let data = vec![0u8; 257];
    let mut rust_dest = vec![0u8; data.len()];
    let mut c_dest = vec![0u8; data.len()];
    let rust_code = filters::blosc2_shuffle(257, data.len() as i32, &data, &mut rust_dest);
    let c_code = unsafe {
        ffi::blosc2_shuffle(
            257,
            data.len() as i32,
            data.as_ptr().cast(),
            c_dest.as_mut_ptr().cast(),
        )
    };
    assert_eq!(rust_code, c_code, "typesize=257 must be rejected like C");
    assert_eq!(rust_code, BLOSC2_ERROR_INVALID_PARAM);

    let rust_code = filters::blosc2_unshuffle(257, data.len() as i32, &data, &mut rust_dest);
    let c_code = unsafe {
        ffi::blosc2_unshuffle(
            257,
            data.len() as i32,
            data.as_ptr().cast(),
            c_dest.as_mut_ptr().cast(),
        )
    };
    assert_eq!(
        rust_code, c_code,
        "unshuffle typesize=257 must be rejected like C"
    );
    assert_eq!(rust_code, BLOSC2_ERROR_INVALID_PARAM);
}

#[test]
fn test_c_compressed_large_typesizes_rust_decompress() {
    let _b = init();

    for typesize in [16, 32, 64, 128, BLOSC_MAX_TYPESIZE as i32] {
        let len = typesize as usize * 128;
        let data: Vec<u8> = (0..len)
            .map(|i| i.wrapping_mul(37).wrapping_add(11) as u8)
            .collect();
        let c_chunk = c_compress(
            &data,
            BLOSC_LZ4,
            typesize,
            BLOSC_FORWARD_COMPAT_SPLIT,
            BLOSC_SHUFFLE,
        );
        let restored = decompress(&c_chunk)
            .unwrap_or_else(|e| panic!("Rust decompress failed for C typesize={typesize}: {e}"));
        assert_eq!(data, restored, "C→Rust typesize={typesize} mismatch");
    }
}

#[test]
fn test_rust_compressed_large_typesizes_c_decompress() {
    let _b = init();

    for typesize in [16, 32, 64, 128, BLOSC_MAX_TYPESIZE as i32] {
        let len = typesize as usize * 128;
        let data: Vec<u8> = (0..len)
            .map(|i| i.wrapping_mul(41).wrapping_add(17) as u8)
            .collect();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize,
            splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let chunk = compress(&data, &cparams).unwrap();
        assert_c_decompresses_to(&chunk, &data, &format!("typesize={typesize}"));
    }
}

// ─── Non-aligned data sizes ─────────────────────────────────────

#[test]
fn test_data_size_not_multiple_of_typesize() {
    // 1003 bytes with typesize=4 (not a multiple)
    let data: Vec<u8> = (0..1003u16).map(|i| (i % 256) as u8).collect();
    let cparams = CParams {
        compcode: BLOSC_LZ4,
        clevel: 5,
        typesize: 4,
        ..Default::default()
    };
    let chunk = compress(&data, &cparams).unwrap();
    let restored = decompress(&chunk).unwrap();
    assert_eq!(data, restored);
}

#[test]
fn test_c_compressed_data_size_not_multiple_of_typesize_rust_decompress() {
    let _b = init();
    let data: Vec<u8> = (0..1003u16).map(|i| (i % 256) as u8).collect();

    let c_chunk = c_compress(
        &data,
        BLOSC_LZ4,
        4,
        BLOSC_FORWARD_COMPAT_SPLIT,
        BLOSC_SHUFFLE,
    );
    let restored = decompress(&c_chunk).unwrap();
    assert_eq!(data, restored);
}

#[test]
fn test_rust_compressed_data_size_not_multiple_of_typesize_c_decompress() {
    let _b = init();
    let data: Vec<u8> = (0..1003u16).map(|i| (i % 256) as u8).collect();

    let cparams = CParams {
        compcode: BLOSC_LZ4,
        clevel: 5,
        typesize: 4,
        splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
        filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
        ..Default::default()
    };
    let chunk = compress(&data, &cparams).unwrap();
    assert_c_decompresses_to(&chunk, &data, "non-multiple nbytes/typesize");
}

#[test]
fn test_various_small_sizes() {
    for size in [1, 2, 3, 7, 15, 31, 33, 100, 255, 256, 1000] {
        let data: Vec<u8> = (0..size).map(|i| (i * 7 + 3) as u8).collect();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            ..Default::default()
        };
        let chunk = compress(&data, &cparams).unwrap();
        let restored = decompress(&chunk).unwrap();
        assert_eq!(data, restored, "Failed for size={size}");
    }
}

#[test]
fn test_size_smaller_than_blocksize() {
    let data = vec![42u8; 500]; // Much smaller than default blocksize
    let cparams = CParams {
        compcode: BLOSC_ZSTD,
        clevel: 5,
        typesize: 4,
        ..Default::default()
    };
    let chunk = compress(&data, &cparams).unwrap();
    let restored = decompress(&chunk).unwrap();
    assert_eq!(data, restored);
}

#[test]
fn test_odd_typesize_7() {
    let data: Vec<u8> = (0..7000u16).map(|i| (i % 256) as u8).collect();
    let cparams = CParams {
        compcode: BLOSC_LZ4,
        clevel: 5,
        typesize: 7,
        ..Default::default()
    };
    let chunk = compress(&data, &cparams).unwrap();
    let restored = decompress(&chunk).unwrap();
    assert_eq!(data, restored);
}

// ─── Incompressible data ─────────────────────────────────────────

#[test]
fn test_incompressible_random_data() {
    // Pseudo-random data that won't compress
    let data: Vec<u8> = (0..50000u32)
        .map(|i| {
            let x = i.wrapping_mul(2654435761);
            (x >> 16) as u8
        })
        .collect();

    for compcode in [BLOSC_BLOSCLZ, BLOSC_LZ4, BLOSC_ZLIB, BLOSC_ZSTD] {
        let cparams = CParams {
            compcode,
            clevel: 5,
            typesize: 1,
            ..Default::default()
        };
        let chunk = compress(&data, &cparams).unwrap();
        let restored = decompress(&chunk).unwrap();
        assert_eq!(
            data, restored,
            "Incompressible roundtrip failed for codec={compcode}"
        );
    }
}

#[test]
fn test_incompressible_with_shuffle() {
    let data: Vec<u8> = (0..40000u32)
        .map(|i| ((i.wrapping_mul(7919) >> 8) & 0xFF) as u8)
        .collect();

    let cparams = CParams {
        compcode: BLOSC_LZ4,
        clevel: 5,
        typesize: 4,
        filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
        ..Default::default()
    };
    let chunk = compress(&data, &cparams).unwrap();
    let restored = decompress(&chunk).unwrap();
    assert_eq!(data, restored);
}

#[test]
fn test_incompressible_cross_compat_c() {
    let _b = init();
    let data: Vec<u8> = (0..20000u32)
        .map(|i| ((i.wrapping_mul(48271) >> 12) & 0xFF) as u8)
        .collect();

    let cparams = CParams {
        compcode: BLOSC_LZ4,
        clevel: 5,
        typesize: 1,
        splitmode: BLOSC_NEVER_SPLIT,
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
        "C decompress of incompressible data failed"
    );
    assert_eq!(data, c_restored);
}

// ─── Malformed/output-size failures ──────────────────────────────

#[test]
fn test_too_small_destination_rejected_like_c() {
    let _b = init();
    let data: Vec<u8> = (0..4096u32).flat_map(|i| i.to_le_bytes()).collect();
    let cparams = CParams {
        compcode: BLOSC_LZ4,
        clevel: 5,
        typesize: 4,
        splitmode: BLOSC_NEVER_SPLIT,
        filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
        ..Default::default()
    };
    let chunk = compress(&data, &cparams).unwrap();

    let mut rust_dest = vec![0u8; data.len() - 1];
    let rust_dest_len = rust_dest.len() as i32;
    let rust_dsize = blosc2_decompress(&chunk, chunk.len() as i32, &mut rust_dest, rust_dest_len);
    assert_eq!(
        rust_dsize, BLOSC2_ERROR_WRITE_BUFFER,
        "Rust C-style decompress should reject undersized output like C"
    );

    let mut c_dest = vec![0u8; data.len() - 1];
    let c_dsize = unsafe {
        ffi::blosc2_decompress(
            chunk.as_ptr() as *const _,
            chunk.len() as i32,
            c_dest.as_mut_ptr() as *mut _,
            c_dest.len() as i32,
        )
    };
    assert_eq!(
        c_dsize, BLOSC2_ERROR_WRITE_BUFFER,
        "C decompress should reject undersized output"
    );
}

#[test]
fn test_invalid_destination_sizes_rejected_like_c() {
    let _b = init();
    let data: Vec<u8> = (0..2048u32).flat_map(|i| i.to_le_bytes()).collect();
    let cparams = CParams {
        compcode: BLOSC_LZ4,
        clevel: 5,
        typesize: 4,
        splitmode: BLOSC_NEVER_SPLIT,
        filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
        ..Default::default()
    };
    let chunk = compress(&data, &cparams).unwrap();

    for (dest_len, destsize, expected_code, context) in [
        (
            data.len(),
            -1,
            BLOSC2_ERROR_WRITE_BUFFER,
            "negative destination size",
        ),
        (
            data.len() - 1,
            (data.len() - 1) as i32,
            BLOSC2_ERROR_WRITE_BUFFER,
            "declared destination one byte too small",
        ),
        (0, 0, BLOSC2_ERROR_WRITE_BUFFER, "empty destination"),
    ] {
        assert_decompress_error_with_sizes_matches_c(
            &chunk,
            chunk.len() as i32,
            dest_len,
            destsize,
            expected_code,
            context,
        );
    }
}

#[test]
fn test_truncated_source_rejected_with_c_error_code() {
    let _b = init();
    let data: Vec<u8> = (0..4096u32).flat_map(|i| i.to_le_bytes()).collect();
    let cparams = CParams {
        compcode: BLOSC_LZ4,
        clevel: 5,
        typesize: 4,
        splitmode: BLOSC_NEVER_SPLIT,
        filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
        ..Default::default()
    };
    let chunk = compress(&data, &cparams).unwrap();
    let truncated = &chunk[..chunk.len() - 1];

    let mut rust_dest = vec![0u8; data.len()];
    let rust_dest_len = rust_dest.len() as i32;
    let rust_dsize = blosc2_decompress(
        truncated,
        truncated.len() as i32,
        &mut rust_dest,
        rust_dest_len,
    );

    let mut c_dest = vec![0u8; data.len()];
    let c_dsize = unsafe {
        ffi::blosc2_decompress(
            truncated.as_ptr() as *const _,
            truncated.len() as i32,
            c_dest.as_mut_ptr() as *mut _,
            c_dest.len() as i32,
        )
    };

    assert_eq!(
        rust_dsize, c_dsize,
        "Rust and C should return the same code for truncated source"
    );
    assert_eq!(
        rust_dsize, BLOSC2_ERROR_INVALID_HEADER,
        "Truncated source should fail before block decoding like C"
    );
}

#[test]
fn test_declared_truncated_source_sizes_match_c_error_codes() {
    let _b = init();
    let data: Vec<u8> = (0..4096u32).flat_map(|i| i.to_le_bytes()).collect();
    let cparams = CParams {
        compcode: BLOSC_LZ4,
        clevel: 5,
        typesize: 4,
        splitmode: BLOSC_NEVER_SPLIT,
        filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
        ..Default::default()
    };
    let chunk = compress(&data, &cparams).unwrap();

    assert_decompress_error_with_sizes_matches_c(
        &chunk,
        -1,
        data.len(),
        data.len() as i32,
        BLOSC2_ERROR_READ_BUFFER,
        "negative source size",
    );

    for srcsize in [0, 1, BLOSC_MIN_HEADER_LENGTH as i32 - 1] {
        assert_decompress_error_with_sizes_matches_c(
            &chunk,
            srcsize,
            data.len(),
            data.len() as i32,
            BLOSC2_ERROR_READ_BUFFER,
            &format!("declared source size {srcsize} shorter than header"),
        );
    }

    assert_decompress_error_with_sizes_matches_c(
        &chunk,
        chunk.len() as i32 - 1,
        data.len(),
        data.len() as i32,
        BLOSC2_ERROR_INVALID_HEADER,
        "declared source size one byte shorter than cbytes",
    );
}

#[test]
fn test_short_header_rejected_with_c_error_code() {
    let _b = init();

    for len in 0..BLOSC_MIN_HEADER_LENGTH {
        let malformed = vec![0u8; len];
        assert_decompress_error_with_sizes_matches_c(
            &malformed,
            malformed.len() as i32,
            1,
            1,
            BLOSC2_ERROR_READ_BUFFER,
            &format!("short legacy header length {len}"),
        );
    }
}

#[test]
fn test_short_extended_header_rejected_with_c_error_code() {
    let _b = init();
    let data: Vec<u8> = (0..1024u32).flat_map(|i| i.to_le_bytes()).collect();
    let cparams = CParams {
        compcode: BLOSC_LZ4,
        clevel: 5,
        typesize: 4,
        splitmode: BLOSC_NEVER_SPLIT,
        filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
        ..Default::default()
    };
    let chunk = compress(&data, &cparams).unwrap();

    assert!(
        chunk[BLOSC2_CHUNK_FLAGS] & BLOSC_DOSHUFFLE != 0
            && chunk[BLOSC2_CHUNK_FLAGS] & BLOSC_DOBITSHUFFLE != 0,
        "test requires an extended-header chunk"
    );

    for len in BLOSC_MIN_HEADER_LENGTH..BLOSC_EXTENDED_HEADER_LENGTH {
        let malformed = &chunk[..len];
        assert_decompress_error_with_sizes_matches_c(
            malformed,
            malformed.len() as i32,
            data.len(),
            data.len() as i32,
            BLOSC2_ERROR_READ_BUFFER,
            &format!("short extended header length {len}"),
        );
    }
}

#[test]
fn test_malformed_chunk_headers_rejected_with_c_error_codes() {
    let _b = init();
    let data: Vec<u8> = (0..4096u32).flat_map(|i| i.to_le_bytes()).collect();
    let cparams = CParams {
        compcode: BLOSC_LZ4,
        clevel: 5,
        typesize: 4,
        splitmode: BLOSC_NEVER_SPLIT,
        filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
        ..Default::default()
    };
    let chunk = compress(&data, &cparams).unwrap();

    let mut too_small_cbytes = chunk.clone();
    too_small_cbytes[BLOSC2_CHUNK_CBYTES..BLOSC2_CHUNK_CBYTES + 4]
        .copy_from_slice(&((BLOSC_MIN_HEADER_LENGTH - 1) as i32).to_le_bytes());
    assert_decompress_error_matches_c(
        &too_small_cbytes,
        data.len(),
        BLOSC2_ERROR_INVALID_HEADER,
        "cbytes smaller than minimum header",
    );

    let mut short_extended_cbytes = chunk.clone();
    short_extended_cbytes[BLOSC2_CHUNK_CBYTES..BLOSC2_CHUNK_CBYTES + 4]
        .copy_from_slice(&((BLOSC_EXTENDED_HEADER_LENGTH - 1) as i32).to_le_bytes());
    assert_decompress_error_matches_c(
        &short_extended_cbytes,
        data.len(),
        BLOSC2_ERROR_INVALID_HEADER,
        "extended cbytes smaller than extended header",
    );

    let mut oversized_cbytes = chunk.clone();
    oversized_cbytes[BLOSC2_CHUNK_CBYTES..BLOSC2_CHUNK_CBYTES + 4]
        .copy_from_slice(&((chunk.len() + 1) as i32).to_le_bytes());
    assert_decompress_error_matches_c(
        &oversized_cbytes,
        data.len(),
        BLOSC2_ERROR_INVALID_HEADER,
        "cbytes larger than source",
    );

    let mut zero_typesize = chunk.clone();
    zero_typesize[BLOSC2_CHUNK_TYPESIZE] = 0;
    assert_decompress_error_matches_c(
        &zero_typesize,
        data.len(),
        BLOSC2_ERROR_INVALID_HEADER,
        "zero typesize",
    );

    let mut zero_blocksize = chunk.clone();
    zero_blocksize[BLOSC2_CHUNK_BLOCKSIZE..BLOSC2_CHUNK_BLOCKSIZE + 4]
        .copy_from_slice(&0i32.to_le_bytes());
    assert_decompress_error_matches_c(
        &zero_blocksize,
        data.len(),
        BLOSC2_ERROR_INVALID_HEADER,
        "zero blocksize",
    );
}

// ─── Super-chunk mutation/frame chunksize parity ────────────────

#[test]
fn test_schunk_update_chunksize_transitions_match_c() {
    let _b = init();
    let replacement = compressed_chunk(b"zz");
    let oversized = compressed_chunk(b"zzzzz");

    let mut middle_update = Schunk::new(schunk_cparams(), DParams::default());
    middle_update.append_buffer(b"aaaa").unwrap();
    middle_update.append_buffer(b"bbbb").unwrap();
    middle_update.append_buffer(b"cccc").unwrap();
    assert_eq!(middle_update.chunksize, 4);
    assert_eq!(
        blosc2_schunk_update_chunk(&mut middle_update, 1, &replacement, true),
        3
    );

    let (c_middle_chunksize, c_middle_nbytes, c_middle_frame) =
        c_schunk_frame_after_buffers_and_mutation(&[b"aaaa", b"bbbb", b"cccc"], |schunk| unsafe {
            assert_eq!(
                ffi::blosc2_schunk_update_chunk(schunk, 1, replacement.as_ptr().cast_mut(), true),
                3
            );
        });
    assert_eq!(middle_update.chunksize, c_middle_chunksize as usize);
    assert_eq!(middle_update.nbytes, c_middle_nbytes);
    assert_eq!(
        frame_chunksize(&middle_update.to_frame()),
        frame_chunksize(&c_middle_frame)
    );
    assert_eq!(middle_update.decompress_all().unwrap(), b"aaaazzcccc");

    let mut last_oversized_update = Schunk::new(schunk_cparams(), DParams::default());
    last_oversized_update.append_buffer(b"aaaa").unwrap();
    last_oversized_update.append_buffer(b"bbbb").unwrap();
    assert_eq!(last_oversized_update.chunksize, 4);
    assert_eq!(
        blosc2_schunk_update_chunk(&mut last_oversized_update, 1, &oversized, true),
        2
    );

    let (c_last_chunksize, c_last_nbytes, c_last_frame) =
        c_schunk_frame_after_buffers_and_mutation(&[b"aaaa", b"bbbb"], |schunk| unsafe {
            assert_eq!(
                ffi::blosc2_schunk_update_chunk(schunk, 1, oversized.as_ptr().cast_mut(), true),
                2
            );
        });
    assert_eq!(last_oversized_update.chunksize, c_last_chunksize as usize);
    assert_eq!(last_oversized_update.nbytes, c_last_nbytes);
    assert_eq!(
        frame_chunksize(&last_oversized_update.to_frame()),
        frame_chunksize(&c_last_frame)
    );
    assert_eq!(
        last_oversized_update.decompress_all().unwrap(),
        b"aaaazzzzz"
    );
}

#[test]
fn test_schunk_insert_chunksize_transitions_match_c() {
    let _b = init();
    let short_chunk = compressed_chunk(b"zz");

    let mut middle_insert = Schunk::new(schunk_cparams(), DParams::default());
    middle_insert.append_buffer(b"aaaa").unwrap();
    middle_insert.append_buffer(b"bbbb").unwrap();
    assert_eq!(middle_insert.chunksize, 4);
    assert_eq!(
        blosc2_schunk_insert_chunk(&mut middle_insert, 1, &short_chunk, true),
        3
    );

    let (c_middle_chunksize, c_middle_nbytes, c_middle_frame) =
        c_schunk_frame_after_buffers_and_mutation(&[b"aaaa", b"bbbb"], |schunk| unsafe {
            assert_eq!(
                ffi::blosc2_schunk_insert_chunk(schunk, 1, short_chunk.as_ptr().cast_mut(), true),
                3
            );
        });
    assert_eq!(middle_insert.chunksize, c_middle_chunksize as usize);
    assert_eq!(middle_insert.nbytes, c_middle_nbytes);
    assert_eq!(
        frame_chunksize(&middle_insert.to_frame()),
        frame_chunksize(&c_middle_frame)
    );
    assert_eq!(middle_insert.decompress_all().unwrap(), b"aaaazzbbbb");

    let mut tail_insert = Schunk::new(schunk_cparams(), DParams::default());
    tail_insert.append_buffer(b"aaaa").unwrap();
    tail_insert.append_buffer(b"bbbb").unwrap();
    assert_eq!(
        blosc2_schunk_insert_chunk(&mut tail_insert, 2, &short_chunk, true),
        3
    );

    let (c_tail_chunksize, c_tail_nbytes, c_tail_frame) =
        c_schunk_frame_after_buffers_and_mutation(&[b"aaaa", b"bbbb"], |schunk| unsafe {
            assert_eq!(
                ffi::blosc2_schunk_insert_chunk(schunk, 2, short_chunk.as_ptr().cast_mut(), true),
                3
            );
        });
    assert_eq!(tail_insert.chunksize, c_tail_chunksize as usize);
    assert_eq!(tail_insert.nbytes, c_tail_nbytes);
    assert_eq!(
        frame_chunksize(&tail_insert.to_frame()),
        frame_chunksize(&c_tail_frame)
    );
    assert_eq!(tail_insert.decompress_all().unwrap(), b"aaaabbbbzz");
}

#[test]
fn test_schunk_delete_preserves_chunksize_and_frame_header_like_c() {
    let _b = init();
    let mut schunk = Schunk::new(schunk_cparams(), DParams::default());
    schunk.append_buffer(b"aaaa").unwrap();
    schunk.append_buffer(b"bb").unwrap();
    assert_eq!(schunk.chunksize, 4);
    assert_eq!(blosc2_schunk_delete_chunk(&mut schunk, 0), 1);

    let (c_chunksize, c_nbytes, c_frame) =
        c_schunk_frame_after_buffers_and_mutation(&[b"aaaa", b"bb"], |schunk| unsafe {
            assert_eq!(ffi::blosc2_schunk_delete_chunk(schunk, 0), 1);
        });
    assert_eq!(schunk.chunksize, c_chunksize as usize);
    assert_eq!(schunk.nbytes, c_nbytes);
    assert_eq!(
        frame_chunksize(&schunk.to_frame()),
        frame_chunksize(&c_frame)
    );
    assert_eq!(schunk.decompress_all().unwrap(), b"bb");

    let restored = Schunk::from_frame(&schunk.to_frame()).unwrap();
    assert_eq!(restored.chunksize, c_chunksize as usize);
    assert_eq!(restored.decompress_all().unwrap(), b"bb");
}

#[test]
fn test_b2nd_zero_extent_slice_and_nchunk_edges_match_c() {
    let cparams = CParams {
        compcode: BLOSC_LZ4,
        clevel: 5,
        typesize: 1,
        splitmode: BLOSC_NEVER_SPLIT,
        filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
        ..Default::default()
    };
    let meta = B2ndMeta::new(vec![4, 6], vec![2, 3], vec![1, 3], "|u1", 0).unwrap();
    let data: Vec<u8> = (0..24).collect();
    let mut array =
        B2ndArray::from_cbuffer(meta, &data, cparams.clone(), DParams::default()).unwrap();
    let before = array.to_cbuffer().unwrap();

    let start = [0, 100];
    let stop = [0, 101];
    let buffershape = [0, 1];
    let mut dest = vec![0xff; 4];
    assert_eq!(
        b2nd_get_slice_cbuffer_c(&array, &start, &stop, &mut dest, &buffershape, 4),
        BLOSC2_ERROR_SUCCESS
    );
    assert_eq!(dest, vec![0; 4]);
    dest.fill(0xff);
    assert_eq!(
        b2nd_get_slice_cbuffer(&array, &start, &stop, &mut dest, &buffershape),
        BLOSC2_ERROR_SUCCESS
    );
    assert_eq!(dest, vec![0; 4]);

    assert_eq!(
        b2nd_set_slice_cbuffer(&[], &buffershape, &start, &stop, &mut array),
        BLOSC2_ERROR_SUCCESS
    );
    assert_eq!(
        b2nd_set_slice_cbuffer_c(&[], 0, &buffershape, &start, &stop, &mut array),
        BLOSC2_ERROR_SUCCESS
    );
    assert_eq!(array.to_cbuffer().unwrap(), before);

    assert_eq!(
        b2nd_get_slice_nchunks(&array, &[1, 2], &[1, 2]),
        (1, Some(vec![0]))
    );
    assert_eq!(
        b2nd_get_slice_nchunks(&array, &[3, 4], &[3, 4]),
        (1, Some(vec![3]))
    );
    assert_eq!(b2nd_get_slice_nchunks(&array, &[2, 3], &[2, 3]), (0, None));
    assert_eq!(b2nd_get_slice_nchunks(&array, &[4, 6], &[4, 6]), (0, None));

    let empty_meta = B2ndMeta::new(vec![0, 5], vec![0, 5], vec![0, 1], "|u1", 0).unwrap();
    let mut empty = B2ndArray::from_cbuffer(empty_meta, &[], cparams, DParams::default()).unwrap();
    let mut empty_dest = vec![0xff; 4];
    assert_eq!(
        b2nd_get_slice_cbuffer_c(&empty, &start, &stop, &mut empty_dest, &buffershape, 4),
        BLOSC2_ERROR_SUCCESS
    );
    assert_eq!(empty_dest, vec![0; 4]);
    assert_eq!(
        b2nd_set_slice_cbuffer_c(&[], 0, &buffershape, &start, &stop, &mut empty),
        BLOSC2_ERROR_SUCCESS
    );
    assert_eq!(
        b2nd_set_slice_cbuffer_c(&[], -1, &buffershape, &start, &stop, &mut empty),
        BLOSC2_ERROR_INVALID_PARAM
    );
    assert_eq!(b2nd_get_slice_nchunks(&empty, &start, &stop), (0, None));
}

// ─── All clevels 0-9 ────────────────────────────────────────────

#[test]
fn test_all_clevels() {
    let data: Vec<u8> = b"Repeated pattern for clevel testing! "
        .iter()
        .cycle()
        .take(50000)
        .copied()
        .collect();

    for clevel in 0..=9u8 {
        for compcode in [BLOSC_BLOSCLZ, BLOSC_LZ4, BLOSC_ZSTD] {
            let cparams = CParams {
                compcode,
                clevel,
                typesize: 1,
                ..Default::default()
            };
            let chunk = compress(&data, &cparams).unwrap();
            let restored = decompress(&chunk).unwrap();
            assert_eq!(
                data, restored,
                "clevel={clevel} codec={compcode} roundtrip failed"
            );
        }
    }
}

// ─── Explicit blocksize ──────────────────────────────────────────

#[test]
fn test_explicit_blocksize() {
    let data: Vec<u8> = (0..40000u32).flat_map(|i| i.to_le_bytes()).collect();

    for blocksize in [1024, 4096, 8192, 32768] {
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 4,
            blocksize,
            ..Default::default()
        };
        let chunk = compress(&data, &cparams).unwrap();
        let restored = decompress(&chunk).unwrap();
        assert_eq!(data, restored, "blocksize={blocksize} roundtrip failed");
    }
}
