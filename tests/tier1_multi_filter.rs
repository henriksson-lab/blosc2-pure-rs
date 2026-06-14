#![cfg(feature = "_ffi")]
//! Tier 1: Multi-filter pipeline tests
//! Tests combined filters: DELTA+SHUFFLE, BITSHUFFLE+DELTA, etc.

use blosc2_pure_rs::compress::{
    blosc2_compress_ctx, blosc2_create_cctx, blosc2_decompress, compress, decompress, CParams,
};
use blosc2_pure_rs::constants::*;
use blosc2_pure_rs::header::ChunkHeader;
mod common;
use blosc2_pure_rs::filters;
use common::ffi;

unsafe extern "C" {
    fn srand(seed: u32);
    fn rand() -> i32;
}

fn init() -> common::Blosc2 {
    common::Blosc2::new()
}

fn sequential_bytes(n: usize) -> Vec<u8> {
    (0..n).map(|i| i as u8).collect()
}

fn sequential_u32(n: usize) -> Vec<u8> {
    (0..n as u32).flat_map(|i| i.to_le_bytes()).collect()
}

fn sequential_u64(n: usize) -> Vec<u8> {
    (0..n as u64).flat_map(|i| i.to_le_bytes()).collect()
}

fn patterned_u16(n: usize) -> Vec<u8> {
    unsafe {
        srand(0);
        (0..n)
            .flat_map(|_| ((rand() % 118) as u16).to_le_bytes())
            .collect()
    }
}

fn mixed_f32(n: usize) -> Vec<u8> {
    (0..n as u32)
        .flat_map(|i| {
            let value = ((i.wrapping_mul(37) % 1009) as f32 * 0.125) - 31.75;
            value.to_le_bytes()
        })
        .collect()
}

fn truncprec_f32(n: usize) -> Vec<u8> {
    let mut data = Vec::with_capacity(n * 4);
    for i in 0..n as u32 {
        let sign = (i & 1) << 31;
        let exponent = (120 + (i % 32)) << 23;
        let mantissa = i.wrapping_mul(1_103_515_245).wrapping_add(12_345) & 0x007f_ffff | 0x55;
        data.extend_from_slice(&f32::from_bits(sign | exponent | mantissa).to_le_bytes());
    }
    data
}

fn c_test_delta_data(typesize: usize) -> Vec<u8> {
    let size = 7 * 12 * 13 * 16 * 24 * 10;
    let nitems = size / typesize;
    let mut data = vec![0u8; size];

    for i in 0..nitems {
        match typesize {
            1 => data[i] = i as u8,
            2 => data[i * 2..i * 2 + 2].copy_from_slice(&(i as u16).to_ne_bytes()),
            4 => data[i * 4..i * 4 + 4].copy_from_slice(&(i as u32).to_ne_bytes()),
            7 => {
                let base = i * 4;
                data[base..base + 4].copy_from_slice(&(i as u32).to_ne_bytes());
                data[base + 2..base + 4].copy_from_slice(&(i as u16).to_ne_bytes());
                data[base + 3] = i as u8;
            }
            8 => data[i * 8..i * 8 + 8].copy_from_slice(&(i as u64).to_ne_bytes()),
            12 => {
                let base = i * 8;
                data[base..base + 8].copy_from_slice(&(i as u64).to_ne_bytes());
                data[base + 4..base + 8].copy_from_slice(&1u32.to_ne_bytes());
            }
            13 => {
                let base = i * 8;
                data[base..base + 8].copy_from_slice(&(i as u64).to_ne_bytes());
                data[base + 4..base + 8].copy_from_slice(&1u32.to_ne_bytes());
                data[base + 5] = 1;
            }
            16 => {
                if i % 2 == 0 {
                    data[i * 8..i * 8 + 8].copy_from_slice(&(i as u64).to_ne_bytes());
                    data[(i + 1) * 8..(i + 2) * 8].copy_from_slice(&((i as u64) + 1).to_ne_bytes());
                }
            }
            24 => {
                let base = i * 8;
                data[base..base + 8].copy_from_slice(&(i as u64).to_ne_bytes());
                data[base + 4..base + 8].copy_from_slice(&1u32.to_ne_bytes());
                data[base + 12..base + 20].copy_from_slice(&(i as u64).to_ne_bytes());
                data[base + 16..base + 20].copy_from_slice(&2u32.to_ne_bytes());
            }
            _ => data[i] = i as u8,
        }
    }

    data
}

fn c_compress_with_filters(
    data: &[u8],
    compcode: u8,
    clevel: u8,
    typesize: i32,
    blocksize: i32,
    filters: [u8; BLOSC2_MAX_FILTERS],
    filters_meta: [u8; BLOSC2_MAX_FILTERS],
) -> Vec<u8> {
    let mut c_chunk = vec![0u8; data.len() + BLOSC2_MAX_OVERHEAD];
    let csize = unsafe {
        let mut cp: ffi::blosc2_cparams = std::mem::zeroed();
        cp.compcode = compcode;
        cp.clevel = clevel;
        cp.typesize = typesize;
        cp.blocksize = blocksize;
        cp.nthreads = 1;
        cp.splitmode = BLOSC_NEVER_SPLIT;
        cp.filters = filters;
        cp.filters_meta = filters_meta;
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
    assert!(csize > 0, "C compression failed: {csize}");
    c_chunk.truncate(csize as usize);
    c_chunk
}

fn c_try_compress_with_filters(
    data: &[u8],
    compcode: u8,
    clevel: u8,
    typesize: i32,
    blocksize: i32,
    filters: [u8; BLOSC2_MAX_FILTERS],
    filters_meta: [u8; BLOSC2_MAX_FILTERS],
) -> i32 {
    let mut c_chunk = vec![0u8; data.len() + BLOSC2_MAX_OVERHEAD];
    unsafe {
        let mut cp: ffi::blosc2_cparams = std::mem::zeroed();
        cp.compcode = compcode;
        cp.clevel = clevel;
        cp.typesize = typesize;
        cp.blocksize = blocksize;
        cp.nthreads = 1;
        cp.splitmode = BLOSC_NEVER_SPLIT;
        cp.filters = filters;
        cp.filters_meta = filters_meta;
        let cctx = ffi::blosc2_create_cctx(cp);
        assert!(!cctx.is_null(), "C context creation failed");
        let r = ffi::blosc2_compress_ctx(
            cctx,
            data.as_ptr() as *const _,
            data.len() as i32,
            c_chunk.as_mut_ptr() as *mut _,
            c_chunk.len() as i32,
        );
        ffi::blosc2_free_ctx(cctx);
        r
    }
}

fn rust_try_compress_with_filters(
    data: &[u8],
    compcode: u8,
    clevel: u8,
    typesize: i32,
    blocksize: i32,
    filters: [u8; BLOSC2_MAX_FILTERS],
    filters_meta: [u8; BLOSC2_MAX_FILTERS],
) -> i32 {
    let cparams = CParams {
        compcode,
        clevel,
        typesize,
        blocksize,
        splitmode: BLOSC_NEVER_SPLIT,
        filters,
        filters_meta,
        ..Default::default()
    };
    let cctx = blosc2_create_cctx(cparams).expect("Rust context creation failed");
    let mut chunk = vec![0u8; data.len() + BLOSC2_MAX_OVERHEAD];
    let destsize = chunk.len() as i32;
    blosc2_compress_ctx(&cctx, data, data.len() as i32, &mut chunk, destsize)
}

fn c_decompress_to_vec(chunk: &[u8], nbytes: usize) -> Vec<u8> {
    let mut restored = vec![0u8; nbytes];
    let dsize = unsafe {
        ffi::blosc2_decompress(
            chunk.as_ptr() as *const _,
            chunk.len() as i32,
            restored.as_mut_ptr() as *mut _,
            restored.len() as i32,
        )
    };
    assert_eq!(dsize, nbytes as i32, "C decompression failed: {dsize}");
    restored
}

fn assert_chunk_header_pipeline(
    chunk: &[u8],
    filters: [u8; BLOSC2_MAX_FILTERS],
    filters_meta: [u8; BLOSC2_MAX_FILTERS],
) {
    let header = ChunkHeader::read(chunk).unwrap();
    assert_eq!(header.filters, filters, "filter slots differ");
    assert_eq!(
        header.filters_meta, filters_meta,
        "filter meta slots differ"
    );
    assert_eq!(
        &chunk[BLOSC2_CHUNK_FILTER_CODES..BLOSC2_CHUNK_FILTER_CODES + BLOSC2_MAX_FILTERS],
        &filters,
        "raw filter slots differ"
    );
    assert_eq!(
        &chunk[BLOSC2_CHUNK_FILTER_META..BLOSC2_CHUNK_FILTER_META + BLOSC2_MAX_FILTERS],
        &filters_meta,
        "raw filter meta slots differ"
    );

    let mut c_nbytes = 0;
    let mut c_cbytes = 0;
    let mut c_blocksize = 0;
    let sizes_result = unsafe {
        ffi::blosc2_cbuffer_sizes(
            chunk.as_ptr() as *const _,
            &mut c_nbytes,
            &mut c_cbytes,
            &mut c_blocksize,
        )
    };
    assert_eq!(sizes_result, 0, "C cbuffer size inspection failed");
    assert_eq!(c_nbytes, header.nbytes, "C/Rust header nbytes differ");
    assert_eq!(c_cbytes, header.cbytes, "C/Rust header cbytes differ");
    assert_eq!(
        c_blocksize, header.blocksize,
        "C/Rust header blocksize differ"
    );

    let mut c_typesize = 0;
    let mut c_flags = 0;
    unsafe {
        ffi::blosc1_cbuffer_metainfo(chunk.as_ptr() as *const _, &mut c_typesize, &mut c_flags);
    }
    assert_eq!(
        c_typesize, header.typesize as usize,
        "C/Rust typesize differ"
    );
    assert_eq!(
        c_flags as u8, header.flags,
        "C/Rust header filter flags differ"
    );
}

fn c_decompress_result(chunk: &[u8], nbytes: usize) -> i32 {
    let mut restored = vec![0u8; nbytes];
    unsafe {
        ffi::blosc2_decompress(
            chunk.as_ptr() as *const _,
            chunk.len() as i32,
            restored.as_mut_ptr() as *mut _,
            restored.len() as i32,
        )
    }
}

fn rust_decompress_result(chunk: &[u8], nbytes: usize) -> i32 {
    let mut restored = vec![0u8; nbytes];
    let destsize = restored.len() as i32;
    blosc2_decompress(chunk, chunk.len() as i32, &mut restored, destsize)
}

fn assert_raw_filter_csv_roundtrip(typesize: usize, nelems: usize, bitshuffle: bool) {
    let nbytes = typesize * nelems;
    let data = sequential_bytes(nbytes);
    let mut filtered = vec![0u8; nbytes];
    let mut restored = vec![0u8; nbytes];

    let forward = if bitshuffle {
        filters::blosc2_bitshuffle(typesize as i32, nbytes as i32, &data, &mut filtered)
    } else {
        filters::blosc2_shuffle(typesize as i32, nbytes as i32, &data, &mut filtered)
    };
    assert_eq!(
        forward, nbytes as i32,
        "C CSV roundtrip forward failed for bitshuffle={bitshuffle} typesize={typesize} nelems={nelems}"
    );

    let backward = if bitshuffle {
        filters::blosc2_bitunshuffle(typesize as i32, nbytes as i32, &filtered, &mut restored)
    } else {
        filters::blosc2_unshuffle(typesize as i32, nbytes as i32, &filtered, &mut restored)
    };
    assert_eq!(
        backward, nbytes as i32,
        "C CSV roundtrip backward failed for bitshuffle={bitshuffle} typesize={typesize} nelems={nelems}"
    );
    assert_eq!(
        restored, data,
        "C CSV roundtrip data mismatch for bitshuffle={bitshuffle} typesize={typesize} nelems={nelems}"
    );
}

// ─── Pipeline forward/backward roundtrip ─────────────────────────

#[test]
fn test_pipeline_delta_shuffle_roundtrip() {
    let data: Vec<u8> = sequential_u32(2048);
    let bsize = data.len();
    let mut buf1 = vec![0u8; bsize];
    let mut buf2 = vec![0u8; bsize];

    let filter_array = [0, 0, 0, 0, BLOSC_DELTA, BLOSC_SHUFFLE];
    let meta = [0u8; BLOSC2_MAX_FILTERS];

    let result_buf = filters::apply_filter_pipeline_for_compression(
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

    let restored_buf = filters::apply_filter_pipeline_for_decompression(
        &mut rbuf1,
        &mut rbuf2,
        bsize,
        &filter_array,
        &meta,
        BLOSC2_VERSION_FORMAT,
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
    let data: Vec<u8> = sequential_bytes(4 * 1024);
    let bsize = data.len();
    let mut buf1 = vec![0u8; bsize];
    let mut buf2 = vec![0u8; bsize];

    let filter_array = [0, 0, 0, 0, 0, BLOSC_SHUFFLE];
    let meta = [0u8; BLOSC2_MAX_FILTERS];

    let result_buf = filters::apply_filter_pipeline_for_compression(
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
    let restored_buf = filters::apply_filter_pipeline_for_decompression(
        &mut rbuf1,
        &mut rbuf2,
        bsize,
        &filter_array,
        &meta,
        BLOSC2_VERSION_FORMAT,
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
    let data: Vec<u8> = sequential_bytes(4 * 1024);
    let bsize = data.len();
    let mut buf1 = vec![0u8; bsize];
    let mut buf2 = vec![0u8; bsize];

    let filter_array = [0, 0, 0, 0, 0, BLOSC_BITSHUFFLE];
    let meta = [0u8; BLOSC2_MAX_FILTERS];

    let result_buf = filters::apply_filter_pipeline_for_compression(
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
    let restored_buf = filters::apply_filter_pipeline_for_decompression(
        &mut rbuf1,
        &mut rbuf2,
        bsize,
        &filter_array,
        &meta,
        BLOSC2_VERSION_FORMAT,
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

#[test]
fn test_raw_shuffle_roundtrip_csv_cases() {
    for typesize in [
        1, 2, 3, 4, 5, 6, 7, 8, 11, 16, 22, 30, 32, 42, 48, 52, 53, 64, 80,
    ] {
        for nelems in [7, 192, 1792] {
            assert_raw_filter_csv_roundtrip(typesize, nelems, false);
        }
    }
}

#[test]
fn test_raw_bitshuffle_roundtrip_csv_cases() {
    for typesize in [
        1, 2, 3, 4, 5, 6, 7, 8, 11, 16, 22, 30, 32, 42, 48, 52, 53, 64, 80,
    ] {
        for nelems in [7, 192, 1792] {
            assert_raw_filter_csv_roundtrip(typesize, nelems, true);
        }
    }
}

#[test]
fn test_pipeline_trunc_prec_stack_matches_c_decode() {
    let _b = init();
    let data = mixed_f32(2048);
    let bsize = data.len();
    let mut buf1 = vec![0u8; bsize];
    let mut buf2 = vec![0u8; bsize];

    let filters = [0, 0, 0, BLOSC_TRUNC_PREC, BLOSC_SHUFFLE, BLOSC_BITSHUFFLE];
    let filters_meta = [0, 0, 0, 20, 0, 0];

    let current = filters::apply_filter_pipeline_for_compression(
        &data,
        &mut buf1,
        &mut buf2,
        &filters,
        &filters_meta,
        4,
        0,
        None,
    );
    assert_ne!(current, 0, "filter pipeline forward failed");

    let mut rbuf1 = if current == 1 {
        buf1.clone()
    } else {
        buf2.clone()
    };
    let mut rbuf2 = vec![0u8; bsize];
    let restored_buf = filters::apply_filter_pipeline_for_decompression(
        &mut rbuf1,
        &mut rbuf2,
        bsize,
        &filters,
        &filters_meta,
        BLOSC2_VERSION_FORMAT,
        4,
        0,
        None,
        1,
    );
    assert_ne!(restored_buf, 0, "filter pipeline backward failed");
    let restored = if restored_buf == 1 {
        &rbuf1[..]
    } else {
        &rbuf2[..]
    };

    let c_chunk =
        c_compress_with_filters(&data, BLOSC_LZ4, 5, 4, bsize as i32, filters, filters_meta);
    assert_chunk_header_pipeline(&c_chunk, filters, filters_meta);
    assert_eq!(
        restored,
        c_decompress_to_vec(&c_chunk, data.len()),
        "Rust direct TRUNC_PREC+SHUFFLE+BITSHUFFLE pipeline differs from C decode"
    );
}

// ─── Full compress/decompress with multi-filter ──────────────────

#[test]
fn test_compress_delta_shuffle() {
    let data = sequential_u32(5000);
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
    let filters = [BLOSC_DELTA, 0, 0, 0, 0, BLOSC_BITSHUFFLE];
    let cparams = CParams {
        compcode: BLOSC_ZSTD,
        clevel: 5,
        typesize: 8,
        filters,
        ..Default::default()
    };
    let chunk = compress(&data, &cparams).unwrap();
    assert_chunk_header_pipeline(&chunk, filters, [0u8; BLOSC2_MAX_FILTERS]);
    let restored = decompress(&chunk).unwrap();
    assert_eq!(data, restored, "DELTA+BITSHUFFLE compress roundtrip failed");
}

#[test]
fn test_compress_trunc_prec_shuffle() {
    let _b = init();
    let data = truncprec_f32(5000);
    let filters = [0, 0, 0, 0, BLOSC_TRUNC_PREC, BLOSC_SHUFFLE];
    let filters_meta = [0, 0, 0, 0, 16, 0];
    let cparams = CParams {
        compcode: BLOSC_LZ4,
        clevel: 5,
        typesize: 4,
        filters,
        filters_meta,
        ..Default::default()
    };
    let chunk = compress(&data, &cparams).unwrap();
    assert_chunk_header_pipeline(&chunk, filters, filters_meta);
    let restored = decompress(&chunk).unwrap();
    let c_chunk = c_compress_with_filters(&data, BLOSC_LZ4, 5, 4, 0, filters, filters_meta);
    assert_chunk_header_pipeline(&c_chunk, filters, filters_meta);
    let c_restored = c_decompress_to_vec(&c_chunk, data.len());

    assert_eq!(data.len(), restored.len());
    assert_ne!(
        data, restored,
        "TRUNC_PREC fixture must exercise lossy C behavior"
    );
    assert_eq!(
        restored, c_restored,
        "TRUNC_PREC+SHUFFLE compress output should match C decode"
    );
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
    let data = sequential_u32(5000);
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
    assert_chunk_header_pipeline(
        &c_chunk[..csize as usize],
        [0, 0, 0, 0, BLOSC_DELTA, BLOSC_SHUFFLE],
        [0u8; BLOSC2_MAX_FILTERS],
    );

    let restored = decompress(&c_chunk[..csize as usize]).unwrap();
    assert_eq!(data, restored, "C DELTA+SHUFFLE → Rust decompress mismatch");
}

#[test]
fn test_rust_delta_shuffle_c_decompress() {
    let _b = init();
    let data = sequential_u32(5000);

    let cparams = CParams {
        compcode: BLOSC_LZ4,
        clevel: 5,
        typesize: 4,
        filters: [0, 0, 0, 0, BLOSC_DELTA, BLOSC_SHUFFLE],
        ..Default::default()
    };
    let chunk = compress(&data, &cparams).unwrap();
    assert_chunk_header_pipeline(
        &chunk,
        [0, 0, 0, 0, BLOSC_DELTA, BLOSC_SHUFFLE],
        [0u8; BLOSC2_MAX_FILTERS],
    );

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

#[test]
fn test_delta_shuffle_c_test_delta_typesize_matrix() {
    let _b = init();
    let filters = [0, 0, 0, 0, BLOSC_DELTA, BLOSC_SHUFFLE];
    let filters_meta = [0u8; BLOSC2_MAX_FILTERS];

    for typesize in [1, 2, 4, 7, 8, 12, 13, 15, 16] {
        let data = c_test_delta_data(typesize);
        let cparams = CParams {
            compcode: BLOSC_BLOSCLZ,
            clevel: 1,
            typesize: typesize as i32,
            splitmode: BLOSC_NEVER_SPLIT,
            filters,
            filters_meta,
            ..Default::default()
        };

        let rust_chunk = compress(&data, &cparams).unwrap();
        assert_chunk_header_pipeline(&rust_chunk, filters, filters_meta);
        assert_eq!(
            decompress(&rust_chunk).unwrap(),
            data,
            "Rust DELTA+SHUFFLE roundtrip failed for C test_delta typesize={typesize}"
        );
        assert_eq!(
            c_decompress_to_vec(&rust_chunk, data.len()),
            data,
            "C decode of Rust DELTA+SHUFFLE failed for C test_delta typesize={typesize}"
        );

        let c_chunk = c_compress_with_filters(
            &data,
            BLOSC_BLOSCLZ,
            1,
            typesize as i32,
            0,
            filters,
            filters_meta,
        );
        assert_chunk_header_pipeline(&c_chunk, filters, filters_meta);
        assert_eq!(
            decompress(&c_chunk).unwrap(),
            data,
            "Rust decode of C DELTA+SHUFFLE failed for C test_delta typesize={typesize}"
        );
    }
}

#[test]
fn test_c_delta_bitshuffle_rust_decompress() {
    let _b = init();
    let data = sequential_u64(4096);
    let filters = [BLOSC_DELTA, 0, 0, 0, 0, BLOSC_BITSHUFFLE];
    let meta = [0u8; BLOSC2_MAX_FILTERS];

    let c_chunk = c_compress_with_filters(&data, BLOSC_ZSTD, 5, 8, 512, filters, meta);
    assert_chunk_header_pipeline(&c_chunk, filters, meta);
    let restored = decompress(&c_chunk).unwrap();
    assert_eq!(
        data, restored,
        "C DELTA+BITSHUFFLE -> Rust decompress mismatch"
    );
}

#[test]
fn test_rust_delta_bitshuffle_c_decompress() {
    let _b = init();
    let data = sequential_u64(4096);
    let filters = [BLOSC_DELTA, 0, 0, 0, 0, BLOSC_BITSHUFFLE];
    let filters_meta = [0u8; BLOSC2_MAX_FILTERS];

    let cparams = CParams {
        compcode: BLOSC_ZSTD,
        clevel: 5,
        typesize: 8,
        blocksize: 512,
        splitmode: BLOSC_NEVER_SPLIT,
        filters,
        filters_meta,
        ..Default::default()
    };
    let chunk = compress(&data, &cparams).unwrap();
    assert_chunk_header_pipeline(&chunk, filters, filters_meta);

    let c_restored = c_decompress_to_vec(&chunk, data.len());
    assert_eq!(
        data, c_restored,
        "Rust DELTA+BITSHUFFLE -> C decompress mismatch"
    );
}

#[test]
fn test_c_bitshuffle_shuffle_rust_decompress() {
    let _b = init();
    let data = patterned_u16(39);
    let filters = [0, 0, 0, 0, BLOSC_BITSHUFFLE, BLOSC_SHUFFLE];
    let meta = [0u8; BLOSC2_MAX_FILTERS];

    let c_chunk = c_compress_with_filters(&data, BLOSC_ZSTD, 5, 2, 0, filters, meta);
    assert_chunk_header_pipeline(&c_chunk, filters, meta);
    let restored = decompress(&c_chunk).unwrap();
    assert_eq!(
        data, restored,
        "C BITSHUFFLE+SHUFFLE -> Rust decompress mismatch"
    );
}

#[test]
fn test_rust_bitshuffle_shuffle_c_decompress() {
    let _b = init();
    let data = patterned_u16(39);
    let filters = [0, 0, 0, 0, BLOSC_BITSHUFFLE, BLOSC_SHUFFLE];
    let filters_meta = [0u8; BLOSC2_MAX_FILTERS];

    let cparams = CParams {
        compcode: BLOSC_ZSTD,
        clevel: 5,
        typesize: 2,
        splitmode: BLOSC_NEVER_SPLIT,
        filters,
        filters_meta,
        ..Default::default()
    };
    let chunk = compress(&data, &cparams).unwrap();
    assert_chunk_header_pipeline(&chunk, filters, filters_meta);

    let c_restored = c_decompress_to_vec(&chunk, data.len());
    assert_eq!(
        data, c_restored,
        "Rust BITSHUFFLE+SHUFFLE -> C decompress mismatch"
    );
}

#[test]
fn test_shuffle_bitshuffle_c_rust_cross_decode() {
    let _b = init();
    let data = patterned_u16(64);
    let filters = [0, 0, 0, 0, BLOSC_SHUFFLE, BLOSC_BITSHUFFLE];
    let filters_meta = [0u8; BLOSC2_MAX_FILTERS];

    let c_chunk = c_compress_with_filters(&data, BLOSC_ZSTD, 5, 2, 0, filters, filters_meta);
    assert_chunk_header_pipeline(&c_chunk, filters, filters_meta);
    assert_eq!(
        decompress(&c_chunk).unwrap(),
        data,
        "Rust decode of C SHUFFLE+BITSHUFFLE differs"
    );

    let cparams = CParams {
        compcode: BLOSC_ZSTD,
        clevel: 5,
        typesize: 2,
        splitmode: BLOSC_NEVER_SPLIT,
        filters,
        filters_meta,
        ..Default::default()
    };
    let rust_chunk = compress(&data, &cparams).unwrap();
    assert_chunk_header_pipeline(&rust_chunk, filters, filters_meta);
    assert_eq!(
        c_decompress_to_vec(&rust_chunk, data.len()),
        data,
        "C decode of Rust SHUFFLE+BITSHUFFLE differs"
    );
}

#[test]
fn test_c_shuffle_delta_rust_decompress() {
    let _b = init();
    let data: Vec<u8> = (0..4096u32)
        .flat_map(|i| i.wrapping_mul(31).rotate_left(7).to_le_bytes())
        .collect();
    let src_size = data.len() as i32;

    let mut c_chunk = vec![0u8; data.len() + BLOSC2_MAX_OVERHEAD];
    let csize = unsafe {
        let mut cp: ffi::blosc2_cparams = std::mem::zeroed();
        cp.compcode = BLOSC_LZ4;
        cp.clevel = 5;
        cp.typesize = 4;
        cp.blocksize = 256;
        cp.nthreads = 1;
        cp.splitmode = BLOSC_NEVER_SPLIT;
        cp.filters[4] = BLOSC_SHUFFLE;
        cp.filters[5] = BLOSC_DELTA;
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
    assert!(csize > 0, "C SHUFFLE+DELTA compression failed");
    assert_chunk_header_pipeline(
        &c_chunk[..csize as usize],
        [0, 0, 0, 0, BLOSC_SHUFFLE, BLOSC_DELTA],
        [0u8; BLOSC2_MAX_FILTERS],
    );

    let restored = decompress(&c_chunk[..csize as usize]).unwrap();
    assert_eq!(
        data, restored,
        "C SHUFFLE+DELTA -> Rust decompress mismatch"
    );
}

#[test]
fn test_trunc_prec_shuffle_c_rust_output_parity() {
    let _b = init();
    let data = mixed_f32(4096);
    let filters = [0, 0, 0, 0, BLOSC_TRUNC_PREC, BLOSC_SHUFFLE];
    let filters_meta = [0, 0, 0, 0, 16, 0];
    let cparams = CParams {
        compcode: BLOSC_LZ4,
        clevel: 5,
        typesize: 4,
        blocksize: 1024,
        splitmode: BLOSC_NEVER_SPLIT,
        filters,
        filters_meta,
        ..Default::default()
    };

    let rust_chunk = compress(&data, &cparams).unwrap();
    assert_chunk_header_pipeline(&rust_chunk, filters, filters_meta);
    let rust_output = decompress(&rust_chunk).unwrap();
    let c_chunk = c_compress_with_filters(&data, BLOSC_LZ4, 5, 4, 1024, filters, filters_meta);
    assert_chunk_header_pipeline(&c_chunk, filters, filters_meta);
    let c_output = c_decompress_to_vec(&c_chunk, data.len());

    assert_eq!(
        rust_output, c_output,
        "Rust and C TRUNC_PREC+SHUFFLE outputs differ"
    );
    assert_eq!(
        decompress(&c_chunk).unwrap(),
        c_output,
        "Rust decode of C TRUNC_PREC+SHUFFLE differs from C output"
    );
    assert_eq!(
        c_decompress_to_vec(&rust_chunk, data.len()),
        c_output,
        "C decode of Rust TRUNC_PREC+SHUFFLE differs from C output"
    );
}

#[test]
fn test_negative_trunc_prec_meta_c_rust_output_parity() {
    let _b = init();
    let data = mixed_f32(4096);
    let filters = [0, 0, 0, BLOSC_TRUNC_PREC, BLOSC_SHUFFLE, BLOSC_BITSHUFFLE];
    let filters_meta = [0, 0, 0, (-8i8) as u8, 0, 0];
    let cparams = CParams {
        compcode: BLOSC_LZ4,
        clevel: 5,
        typesize: 4,
        blocksize: 1024,
        splitmode: BLOSC_NEVER_SPLIT,
        filters,
        filters_meta,
        ..Default::default()
    };

    let rust_chunk = compress(&data, &cparams).unwrap();
    assert_chunk_header_pipeline(&rust_chunk, filters, filters_meta);
    let rust_output = decompress(&rust_chunk).unwrap();
    let c_chunk = c_compress_with_filters(&data, BLOSC_LZ4, 5, 4, 1024, filters, filters_meta);
    assert_chunk_header_pipeline(&c_chunk, filters, filters_meta);
    let c_output = c_decompress_to_vec(&c_chunk, data.len());

    assert_eq!(
        rust_output, c_output,
        "Rust and C negative TRUNC_PREC meta outputs differ"
    );
    assert_eq!(
        decompress(&c_chunk).unwrap(),
        c_output,
        "Rust decode of C negative TRUNC_PREC meta differs from C output"
    );
    assert_eq!(
        c_decompress_to_vec(&rust_chunk, data.len()),
        c_output,
        "C decode of Rust negative TRUNC_PREC meta differs from C output"
    );
}

#[test]
fn test_delta_trunc_prec_shuffle_c_rust_output_parity() {
    let _b = init();
    let data = mixed_f32(4096);
    let filters = [0, 0, 0, BLOSC_DELTA, BLOSC_TRUNC_PREC, BLOSC_SHUFFLE];
    let filters_meta = [0, 0, 0, 0, 16, 0];
    let cparams = CParams {
        compcode: BLOSC_LZ4,
        clevel: 5,
        typesize: 4,
        blocksize: 1024,
        splitmode: BLOSC_NEVER_SPLIT,
        filters,
        filters_meta,
        ..Default::default()
    };

    let rust_chunk = compress(&data, &cparams).unwrap();
    assert_chunk_header_pipeline(&rust_chunk, filters, filters_meta);
    let rust_output = decompress(&rust_chunk).unwrap();
    let c_chunk = c_compress_with_filters(&data, BLOSC_LZ4, 5, 4, 1024, filters, filters_meta);
    assert_chunk_header_pipeline(&c_chunk, filters, filters_meta);
    let c_output = c_decompress_to_vec(&c_chunk, data.len());

    assert_eq!(
        rust_output, c_output,
        "Rust and C DELTA+TRUNC_PREC+SHUFFLE outputs differ"
    );
    assert_eq!(
        decompress(&c_chunk).unwrap(),
        c_output,
        "Rust decode of C DELTA+TRUNC_PREC+SHUFFLE differs from C output"
    );
    assert_eq!(
        c_decompress_to_vec(&rust_chunk, data.len()),
        c_output,
        "C decode of Rust DELTA+TRUNC_PREC+SHUFFLE differs from C output"
    );
}

#[test]
fn test_shuffle_meta_slot_c_rust_parity() {
    let _b = init();
    let data: Vec<u8> = (0..4096u32)
        .flat_map(|i| i.wrapping_mul(17).wrapping_add(3).to_le_bytes())
        .collect();
    let filters = [0, 0, 0, 0, BLOSC_SHUFFLE, BLOSC_DELTA];
    let filters_meta = [0, 0, 0, 0, 2, 0];
    let cparams = CParams {
        compcode: BLOSC_LZ4,
        clevel: 5,
        typesize: 4,
        blocksize: 1024,
        splitmode: BLOSC_NEVER_SPLIT,
        filters,
        filters_meta,
        ..Default::default()
    };

    let c_chunk = c_compress_with_filters(&data, BLOSC_LZ4, 5, 4, 1024, filters, filters_meta);
    assert_chunk_header_pipeline(&c_chunk, filters, filters_meta);
    let rust_from_c = decompress(&c_chunk).unwrap();
    assert_eq!(
        data, rust_from_c,
        "Rust decode of C SHUFFLE(meta slot 4)+DELTA differs"
    );

    let rust_chunk = compress(&data, &cparams).unwrap();
    assert_chunk_header_pipeline(&rust_chunk, filters, filters_meta);
    let c_from_rust = c_decompress_to_vec(&rust_chunk, data.len());
    assert_eq!(
        data, c_from_rust,
        "C decode of Rust SHUFFLE(meta slot 4)+DELTA differs"
    );
}

#[test]
fn test_rust_shuffle_delta_c_decompress() {
    let _b = init();
    let data: Vec<u8> = (0..4096u32)
        .flat_map(|i| i.wrapping_mul(31).rotate_left(7).to_le_bytes())
        .collect();
    let filters = [0, 0, 0, 0, BLOSC_SHUFFLE, BLOSC_DELTA];
    let filters_meta = [0u8; BLOSC2_MAX_FILTERS];

    let cparams = CParams {
        compcode: BLOSC_LZ4,
        clevel: 5,
        typesize: 4,
        blocksize: 256,
        splitmode: BLOSC_NEVER_SPLIT,
        filters,
        filters_meta,
        ..Default::default()
    };
    let chunk = compress(&data, &cparams).unwrap();
    assert_chunk_header_pipeline(&chunk, filters, filters_meta);

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
        "C decompress of Rust SHUFFLE+DELTA failed"
    );
    assert_eq!(
        data, c_restored,
        "Rust SHUFFLE+DELTA -> C decompress mismatch"
    );
}

// ─── All codec × multi-filter combinations ───────────────────────

#[test]
fn test_all_codecs_delta_shuffle() {
    let _b = init();
    let data = sequential_u32(5000);
    let filters = [0, 0, 0, 0, BLOSC_DELTA, BLOSC_SHUFFLE];
    let filters_meta = [0u8; BLOSC2_MAX_FILTERS];
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
            splitmode: BLOSC_NEVER_SPLIT,
            filters,
            filters_meta,
            ..Default::default()
        };
        let chunk = compress(&data, &cparams).unwrap();
        let restored = decompress(&chunk).unwrap();
        assert_eq!(data, restored, "DELTA+SHUFFLE failed for codec={compcode}");
        assert_chunk_header_pipeline(&chunk, filters, filters_meta);
        assert_eq!(
            c_decompress_to_vec(&chunk, data.len()),
            data,
            "C decode of Rust DELTA+SHUFFLE failed for codec={compcode}"
        );

        let c_chunk = c_compress_with_filters(&data, compcode, 5, 4, 0, filters, filters_meta);
        assert_chunk_header_pipeline(&c_chunk, filters, filters_meta);
        assert_eq!(
            decompress(&c_chunk).unwrap(),
            data,
            "Rust decode of C DELTA+SHUFFLE failed for codec={compcode}"
        );
    }
}

#[test]
fn test_three_filters_stacked() {
    let _b = init();
    // C applies filters from low to high slots. Truncate first, then apply
    // reversible filters so the decoded output is the truncated value stream.
    let data = mixed_f32(2048);
    let filters = [0, 0, 0, BLOSC_TRUNC_PREC, BLOSC_SHUFFLE, BLOSC_BITSHUFFLE];
    let filters_meta = [0, 0, 0, 20, 0, 0]; // 20 bits precision
    let cparams = CParams {
        compcode: BLOSC_LZ4,
        clevel: 5,
        typesize: 4,
        blocksize: 1024,
        splitmode: BLOSC_NEVER_SPLIT,
        filters,
        filters_meta,
        ..Default::default()
    };
    let chunk = compress(&data, &cparams).unwrap();
    assert_chunk_header_pipeline(&chunk, filters, filters_meta);
    let restored = decompress(&chunk).unwrap();
    let c_chunk = c_compress_with_filters(&data, BLOSC_LZ4, 5, 4, 1024, filters, filters_meta);
    assert_chunk_header_pipeline(&c_chunk, filters, filters_meta);
    let c_restored = c_decompress_to_vec(&c_chunk, data.len());

    // TRUNC_PREC is lossy, so verify stability
    assert_eq!(data.len(), restored.len());
    assert_eq!(
        restored, c_restored,
        "Rust and C TRUNC_PREC+SHUFFLE+BITSHUFFLE outputs differ"
    );
    assert_eq!(
        decompress(&c_chunk).unwrap(),
        c_restored,
        "Rust decode of C TRUNC_PREC+SHUFFLE+BITSHUFFLE differs from C output"
    );
    assert_eq!(
        c_decompress_to_vec(&chunk, data.len()),
        c_restored,
        "C decode of Rust TRUNC_PREC+SHUFFLE+BITSHUFFLE differs from C output"
    );
    let chunk2 = compress(&restored, &cparams).unwrap();
    let restored2 = decompress(&chunk2).unwrap();
    assert_eq!(restored, restored2, "Three-filter stack should be stable");
}

#[test]
fn test_invalid_trunc_prec_error_parity() {
    let _b = init();
    let data = mixed_f32(1024);
    let filters = [0, 0, 0, 0, BLOSC_TRUNC_PREC, BLOSC_SHUFFLE];
    let filters_meta = [0, 0, 0, 0, 24, 0];
    let cparams = CParams {
        compcode: BLOSC_LZ4,
        clevel: 5,
        typesize: 4,
        blocksize: 1024,
        splitmode: BLOSC_NEVER_SPLIT,
        filters,
        filters_meta,
        ..Default::default()
    };

    let csize = c_try_compress_with_filters(&data, BLOSC_LZ4, 5, 4, 1024, filters, filters_meta);
    let rust_csize =
        rust_try_compress_with_filters(&data, BLOSC_LZ4, 5, 4, 1024, filters, filters_meta);
    assert!(
        csize < 0,
        "C should reject TRUNC_PREC meta that keeps too many f32 mantissa bits"
    );
    assert_eq!(
        rust_csize, csize,
        "Rust C-style compression should return C's TRUNC_PREC error code"
    );
    assert!(
        compress(&data, &cparams).is_err(),
        "Rust should reject TRUNC_PREC meta that C rejects"
    );
}

#[test]
fn test_truncated_multi_filter_error_parity() {
    let _b = init();
    let data = sequential_u64(4096);
    let filters = [BLOSC_DELTA, 0, 0, 0, 0, BLOSC_BITSHUFFLE];
    let filters_meta = [0u8; BLOSC2_MAX_FILTERS];
    let cparams = CParams {
        compcode: BLOSC_LZ4,
        clevel: 5,
        typesize: 8,
        blocksize: 1024,
        splitmode: BLOSC_NEVER_SPLIT,
        filters,
        filters_meta,
        ..Default::default()
    };

    let rust_chunk = compress(&data, &cparams).unwrap();
    let c_chunk = c_compress_with_filters(&data, BLOSC_LZ4, 5, 8, 1024, filters, filters_meta);
    for (label, chunk) in [("Rust", rust_chunk.as_slice()), ("C", c_chunk.as_slice())] {
        assert_chunk_header_pipeline(chunk, filters, filters_meta);
        for cut in [
            BLOSC_MIN_HEADER_LENGTH - 1,
            BLOSC_EXTENDED_HEADER_LENGTH - 1,
            chunk.len() - 1,
        ] {
            let truncated = &chunk[..cut];
            let c_result = c_decompress_result(truncated, data.len());
            let rust_result = rust_decompress_result(truncated, data.len());
            assert!(
                c_result < 0,
                "{label} chunk truncated to {cut} bytes should fail in C"
            );
            assert_eq!(
                rust_result, c_result,
                "{label} chunk truncated to {cut} bytes has different C/Rust error code"
            );
        }
    }
}
