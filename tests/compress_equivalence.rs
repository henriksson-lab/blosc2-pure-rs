#![cfg(feature = "_ffi")]
use blosc2_pure_rs::b2nd::{B2ndArray, B2ndMeta};
use blosc2_pure_rs::compress::{
    compress, decompress, vlchunk_get_nblocks, vlcompress, vldecompress, vldecompress_block,
    CParams,
};
use blosc2_pure_rs::constants::*;
mod common;
use blosc2_pure_rs::schunk::Schunk;
use common::ffi;
use std::ffi::CString;
use std::os::raw::c_void;

unsafe extern "C" {
    fn free(ptr: *mut c_void);
}

fn init_blosc2() -> common::Blosc2 {
    common::Blosc2::new()
}

const BLOSCLZ_MAX_COPY: usize = 32;
const BLOSCLZ_MAX_DISTANCE: usize = 8191;
const BLOSCLZ_MAX_FARDISTANCE: usize = 65535 + BLOSCLZ_MAX_DISTANCE - 1;

fn blosclz_deterministic_data(len: usize) -> Vec<u8> {
    (0..len as u32)
        .map(|i| ((i.wrapping_mul(37).wrapping_add(11)) & 0xff) as u8)
        .collect()
}

fn blosclz_distance_fixture(distance: usize, match_len: usize) -> Vec<u8> {
    assert!(match_len >= 16);
    let mut data = blosclz_deterministic_data(distance + match_len + 128);
    let pattern: Vec<u8> = (0..match_len).map(|i| b'A' + (i % 26) as u8).collect();
    data[0..match_len].copy_from_slice(&pattern);
    data[distance..distance + match_len].copy_from_slice(&pattern);
    data
}

fn blosclz_optimization_fixtures() -> Vec<(&'static str, Vec<u8>)> {
    let mut overlapping_run = vec![0u8; 20_000];
    for (i, byte) in overlapping_run.iter_mut().enumerate().take(128) {
        *byte = (i & 0xff) as u8;
    }
    overlapping_run[128..].fill(b'Z');

    let literal_prefix = (BLOSCLZ_MAX_COPY * 4) + 17;
    let mut literal_run = blosclz_deterministic_data(literal_prefix);
    literal_run.extend(
        b"literal-run-boundary-tail"
            .iter()
            .cycle()
            .take(4096)
            .copied(),
    );

    vec![
        (
            "exact_max_short_distance",
            blosclz_distance_fixture(BLOSCLZ_MAX_DISTANCE, 16),
        ),
        (
            "first_far_distance",
            blosclz_distance_fixture(BLOSCLZ_MAX_DISTANCE + 1, 32),
        ),
        (
            "near_max_far_distance",
            blosclz_distance_fixture(BLOSCLZ_MAX_FARDISTANCE - 1, 32),
        ),
        (
            "long_match_extension",
            blosclz_distance_fixture(BLOSCLZ_MAX_DISTANCE + 1, 2048),
        ),
        ("overlapping_run", overlapping_run),
        ("literal_run_encoding", literal_run),
    ]
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

#[test]
fn test_blosclz_optimization_fixtures_c_decompress() {
    let _b = init_blosc2();

    for (name, data) in blosclz_optimization_fixtures() {
        let cparams = CParams {
            compcode: BLOSC_BLOSCLZ,
            clevel: 9,
            typesize: 1,
            blocksize: data.len() as i32,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0; BLOSC2_MAX_FILTERS],
            ..Default::default()
        };
        let compressed = compress(&data, &cparams)
            .unwrap_or_else(|e| panic!("Rust BloscLZ compress failed for {name}: {e}"));

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
            "C decompress size mismatch for {name}: got {dsize}"
        );
        assert_eq!(
            data, c_decompressed,
            "Rust BloscLZ -> C mismatch for {name}"
        );
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
fn test_zstd_dictionary_cross_compat() {
    let _b = init_blosc2();

    let data: Vec<u8> = (0..200_000u32)
        .flat_map(|i| (i % 4096).to_le_bytes())
        .collect();

    let rust_params = CParams {
        compcode: BLOSC_ZSTD,
        clevel: 5,
        typesize: 4,
        blocksize: 4096,
        splitmode: BLOSC_NEVER_SPLIT,
        filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
        use_dict: true,
        ..Default::default()
    };
    let rust_chunk = compress(&data, &rust_params).unwrap();
    let mut c_decompressed = vec![0u8; data.len()];
    let c_dsize = unsafe {
        ffi::blosc2_decompress(
            rust_chunk.as_ptr() as *const _,
            rust_chunk.len() as i32,
            c_decompressed.as_mut_ptr() as *mut _,
            c_decompressed.len() as i32,
        )
    };
    assert_eq!(c_dsize, data.len() as i32);
    assert_eq!(c_decompressed, data);

    let mut c_chunk = vec![0u8; data.len() + BLOSC2_MAX_OVERHEAD + 4096];
    let c_csize = unsafe {
        let mut cparams: ffi::blosc2_cparams = std::mem::zeroed();
        cparams.compcode = BLOSC_ZSTD;
        cparams.clevel = 5;
        cparams.typesize = 4;
        cparams.nthreads = 1;
        cparams.blocksize = 4096;
        cparams.splitmode = BLOSC_NEVER_SPLIT;
        cparams.use_dict = 1;
        cparams.filters[BLOSC2_MAX_FILTERS - 1] = BLOSC_SHUFFLE;

        let cctx = ffi::blosc2_create_cctx(cparams);
        let result = ffi::blosc2_compress_ctx(
            cctx,
            data.as_ptr() as *const _,
            data.len() as i32,
            c_chunk.as_mut_ptr() as *mut _,
            c_chunk.len() as i32,
        );
        ffi::blosc2_free_ctx(cctx);
        result
    };
    assert!(c_csize > 0);

    let rust_decompressed = decompress(&c_chunk[..c_csize as usize]).unwrap();
    assert_eq!(rust_decompressed, data);
}

fn vl_test_blocks() -> Vec<Vec<u8>> {
    vec![
        b"red".to_vec(),
        b"green-green".to_vec(),
        b"blue-blue-blue-blue".to_vec(),
        b"yellow-yellow-yellow-yellow-yellow".to_vec(),
    ]
}

fn vl_dict_blocks() -> Vec<Vec<u8>> {
    (0..64)
        .map(|i| {
            format!(
                "{{\"id\":\"ingredient-{i:03}\",\"vegan\":\"{}\",\"vegetarian\":\"{}\",\"percent\":{},\"text\":\"INGREDIENT NUMBER {i:03}\"}}",
                if i % 3 == 0 { "maybe" } else { "yes" },
                if i % 5 == 0 { "no" } else { "yes" },
                i % 19
            )
            .into_bytes()
        })
        .collect()
}

unsafe fn c_vl_compress(
    blocks: &[Vec<u8>],
    compcode: u8,
    typesize: i32,
    nthreads: i16,
    use_dict: bool,
) -> Vec<u8> {
    let srcs: Vec<*const c_void> = blocks
        .iter()
        .map(|block| block.as_ptr() as *const c_void)
        .collect();
    let sizes: Vec<i32> = blocks.iter().map(|block| block.len() as i32).collect();
    let total: usize = blocks.iter().map(Vec::len).sum();
    let mut compressed =
        vec![0u8; total + BLOSC2_MAX_OVERHEAD + blocks.len() * 64 + BLOSC2_MAXDICTSIZE];

    let mut cparams: ffi::blosc2_cparams = std::mem::zeroed();
    cparams.compcode = compcode;
    cparams.clevel = 5;
    cparams.typesize = typesize;
    cparams.nthreads = nthreads;
    cparams.splitmode = BLOSC_FORWARD_COMPAT_SPLIT;
    cparams.use_dict = i32::from(use_dict);
    cparams.filters[BLOSC2_MAX_FILTERS - 1] = BLOSC_SHUFFLE;

    let cctx = ffi::blosc2_create_cctx(cparams);
    assert!(!cctx.is_null());
    let csize = ffi::blosc2_vlcompress_ctx(
        cctx,
        srcs.as_ptr(),
        sizes.as_ptr(),
        blocks.len() as i32,
        compressed.as_mut_ptr() as *mut c_void,
        compressed.len() as i32,
    );
    ffi::blosc2_free_ctx(cctx);
    assert!(
        csize > 0,
        "C VL-block compression failed for codec={compcode}"
    );
    compressed.truncate(csize as usize);
    compressed
}

unsafe fn c_vl_decompress(chunk: &[u8], maxblocks: usize) -> Vec<Vec<u8>> {
    let mut dparams: ffi::blosc2_dparams = std::mem::zeroed();
    dparams.nthreads = 1;
    let dctx = ffi::blosc2_create_dctx(dparams);
    assert!(!dctx.is_null());

    let mut dests = vec![std::ptr::null_mut::<c_void>(); maxblocks];
    let mut sizes = vec![0i32; maxblocks];
    let nblocks = ffi::blosc2_vldecompress_ctx(
        dctx,
        chunk.as_ptr() as *const c_void,
        chunk.len() as i32,
        dests.as_mut_ptr(),
        sizes.as_mut_ptr(),
        maxblocks as i32,
    );
    ffi::blosc2_free_ctx(dctx);
    assert_eq!(nblocks, maxblocks as i32);

    let mut blocks = Vec::with_capacity(maxblocks);
    for (ptr, size) in dests.into_iter().zip(sizes) {
        assert!(!ptr.is_null());
        assert!(size > 0);
        let block = std::slice::from_raw_parts(ptr as *const u8, size as usize).to_vec();
        free(ptr);
        blocks.push(block);
    }
    blocks
}

#[test]
fn test_vlblocks_c_compress_rust_decompress() {
    let _b = init_blosc2();
    let blocks = vl_test_blocks();
    let expected_concat: Vec<u8> = blocks.iter().flatten().copied().collect();

    for &compcode in &[
        BLOSC_BLOSCLZ,
        BLOSC_LZ4,
        BLOSC_LZ4HC,
        BLOSC_ZLIB,
        BLOSC_ZSTD,
    ] {
        let c_chunk = unsafe { c_vl_compress(&blocks, compcode, 1, 4, false) };
        assert_eq!(vlchunk_get_nblocks(&c_chunk).unwrap(), blocks.len());
        assert_eq!(decompress(&c_chunk).unwrap(), expected_concat);
        assert_eq!(vldecompress(&c_chunk).unwrap(), blocks);
        assert_eq!(vldecompress_block(&c_chunk, 2).unwrap(), blocks[2]);
    }
}

#[test]
fn test_vlblocks_rust_compress_c_decompress() {
    let _b = init_blosc2();
    let blocks = vl_test_blocks();
    let block_refs: Vec<&[u8]> = blocks.iter().map(Vec::as_slice).collect();
    let expected_concat: Vec<u8> = blocks.iter().flatten().copied().collect();

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
            typesize: 1,
            nthreads: 4,
            splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let rust_chunk = vlcompress(&block_refs, &cparams).unwrap();

        let mut c_nblocks = 0i32;
        let rc = unsafe {
            ffi::blosc2_vlchunk_get_nblocks(
                rust_chunk.as_ptr() as *const c_void,
                rust_chunk.len() as i32,
                &mut c_nblocks,
            )
        };
        assert_eq!(rc, 0);
        assert_eq!(c_nblocks, blocks.len() as i32);

        let mut c_decompressed = vec![0u8; expected_concat.len()];
        let dsize = unsafe {
            ffi::blosc2_decompress(
                rust_chunk.as_ptr() as *const c_void,
                rust_chunk.len() as i32,
                c_decompressed.as_mut_ptr() as *mut c_void,
                c_decompressed.len() as i32,
            )
        };
        assert_eq!(dsize, expected_concat.len() as i32);
        assert_eq!(c_decompressed, expected_concat);

        let c_blocks = unsafe { c_vl_decompress(&rust_chunk, blocks.len()) };
        assert_eq!(c_blocks, blocks);

        let mut dparams: ffi::blosc2_dparams = unsafe { std::mem::zeroed() };
        dparams.nthreads = 1;
        let dctx = unsafe { ffi::blosc2_create_dctx(dparams) };
        assert!(!dctx.is_null());
        let mut block_ptr = std::ptr::null_mut::<u8>();
        let mut block_size = 0i32;
        let block_rc = unsafe {
            ffi::blosc2_vldecompress_block_ctx(
                dctx,
                rust_chunk.as_ptr() as *const c_void,
                rust_chunk.len() as i32,
                1,
                &mut block_ptr,
                &mut block_size,
            )
        };
        unsafe {
            ffi::blosc2_free_ctx(dctx);
        }
        assert_eq!(block_rc, blocks[1].len() as i32);
        assert_eq!(block_size, blocks[1].len() as i32);
        assert!(!block_ptr.is_null());
        let c_block =
            unsafe { std::slice::from_raw_parts(block_ptr, block_size as usize).to_vec() };
        unsafe {
            free(block_ptr as *mut c_void);
        }
        assert_eq!(c_block, blocks[1]);
    }
}

#[test]
fn test_vlblocks_typesize4_cross_compat() {
    let _b = init_blosc2();
    let blocks: Vec<Vec<u8>> = vec![
        (0..33u32).flat_map(u32::to_le_bytes).collect(),
        (1000..1097u32).flat_map(u32::to_le_bytes).collect(),
        b"not-a-multiple-of-typesize".to_vec(),
    ];
    let block_refs: Vec<&[u8]> = blocks.iter().map(Vec::as_slice).collect();
    let expected_concat: Vec<u8> = blocks.iter().flatten().copied().collect();

    let c_chunk = unsafe { c_vl_compress(&blocks, BLOSC_LZ4, 4, 4, false) };
    assert_eq!(vlchunk_get_nblocks(&c_chunk).unwrap(), blocks.len());
    assert_eq!(vldecompress(&c_chunk).unwrap(), blocks);
    assert_eq!(decompress(&c_chunk).unwrap(), expected_concat);

    let cparams = CParams {
        compcode: BLOSC_LZ4,
        clevel: 5,
        typesize: 4,
        nthreads: 4,
        splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
        filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
        ..Default::default()
    };
    let rust_chunk = vlcompress(&block_refs, &cparams).unwrap();
    let c_blocks = unsafe { c_vl_decompress(&rust_chunk, blocks.len()) };
    assert_eq!(c_blocks, blocks);

    let mut c_decompressed = vec![0u8; expected_concat.len()];
    let dsize = unsafe {
        ffi::blosc2_decompress(
            rust_chunk.as_ptr() as *const c_void,
            rust_chunk.len() as i32,
            c_decompressed.as_mut_ptr() as *mut c_void,
            c_decompressed.len() as i32,
        )
    };
    assert_eq!(dsize, expected_concat.len() as i32);
    assert_eq!(c_decompressed, expected_concat);
}

#[test]
fn test_zstd_dictionary_vlblocks_cross_compat() {
    let _b = init_blosc2();
    let blocks = vl_dict_blocks();
    let block_refs: Vec<&[u8]> = blocks.iter().map(Vec::as_slice).collect();
    let expected_concat: Vec<u8> = blocks.iter().flatten().copied().collect();

    let c_chunk = unsafe { c_vl_compress(&blocks, BLOSC_ZSTD, 1, 4, true) };
    assert_eq!(vlchunk_get_nblocks(&c_chunk).unwrap(), blocks.len());
    assert_eq!(vldecompress(&c_chunk).unwrap(), blocks);
    assert_eq!(decompress(&c_chunk).unwrap(), expected_concat);

    let cparams = CParams {
        compcode: BLOSC_ZSTD,
        clevel: 5,
        typesize: 1,
        nthreads: 4,
        splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
        filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
        use_dict: true,
        ..Default::default()
    };
    let rust_chunk = vlcompress(&block_refs, &cparams).unwrap();
    let c_blocks = unsafe { c_vl_decompress(&rust_chunk, blocks.len()) };
    assert_eq!(c_blocks, blocks);

    let mut c_decompressed = vec![0u8; expected_concat.len()];
    let dsize = unsafe {
        ffi::blosc2_decompress(
            rust_chunk.as_ptr() as *const c_void,
            rust_chunk.len() as i32,
            c_decompressed.as_mut_ptr() as *mut c_void,
            c_decompressed.len() as i32,
        )
    };
    assert_eq!(dsize, expected_concat.len() as i32);
    assert_eq!(c_decompressed, expected_concat);
}

#[test]
fn test_b2nd_rust_frame_c_reads() {
    let _b = init_blosc2();

    let meta = B2ndMeta::new(vec![5, 7], vec![3, 4], vec![3, 2], "<u2", 0).unwrap();
    let data: Vec<u8> = (0..35u16).flat_map(u16::to_le_bytes).collect();
    let cparams = CParams {
        compcode: BLOSC_LZ4,
        clevel: 5,
        typesize: 2,
        splitmode: BLOSC_NEVER_SPLIT,
        filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
        ..Default::default()
    };
    let array = B2ndArray::from_cbuffer(meta, &data, cparams, Default::default()).unwrap();
    let mut frame = array.to_frame();

    unsafe {
        let mut c_array: *mut ffi::b2nd_array_t = std::ptr::null_mut();
        let rc = ffi::b2nd_from_cframe(frame.as_mut_ptr(), frame.len() as i64, true, &mut c_array);
        assert_eq!(rc, 0);
        assert!(!c_array.is_null());

        let mut c_buffer = vec![0u8; data.len()];
        let rc = ffi::b2nd_to_cbuffer(c_array, c_buffer.as_mut_ptr().cast(), c_buffer.len() as i64);
        assert_eq!(rc, 0);
        assert_eq!(c_buffer, data);
        assert_eq!(ffi::b2nd_free(c_array), 0);
    }
}

#[test]
fn test_b2nd_c_frame_rust_reads() {
    let _b = init_blosc2();

    let shape = [5i64, 7];
    let chunkshape = [3i32, 4];
    let blockshape = [3i32, 2];
    let data: Vec<u8> = (0..35u16).flat_map(u16::to_le_bytes).collect();
    let dtype = CString::new("<u2").unwrap();

    unsafe {
        let mut cparams: ffi::blosc2_cparams = std::mem::zeroed();
        cparams.compcode = BLOSC_LZ4;
        cparams.clevel = 5;
        cparams.typesize = 2;
        cparams.nthreads = 1;
        cparams.splitmode = BLOSC_NEVER_SPLIT;
        cparams.filters[BLOSC2_MAX_FILTERS - 1] = BLOSC_SHUFFLE;
        let mut dparams: ffi::blosc2_dparams = std::mem::zeroed();
        dparams.nthreads = 1;
        dparams.typesize = 2;
        let storage = ffi::blosc2_storage {
            contiguous: true,
            urlpath: std::ptr::null_mut(),
            cparams: &mut cparams,
            dparams: &mut dparams,
            io: std::ptr::null_mut(),
        };

        let ctx = ffi::b2nd_create_ctx(
            &storage,
            2,
            shape.as_ptr(),
            chunkshape.as_ptr(),
            blockshape.as_ptr(),
            dtype.as_ptr(),
            0,
            std::ptr::null(),
            0,
        );
        assert!(!ctx.is_null());

        let mut c_array: *mut ffi::b2nd_array_t = std::ptr::null_mut();
        let rc = ffi::b2nd_from_cbuffer(ctx, &mut c_array, data.as_ptr().cast(), data.len() as i64);
        assert_eq!(rc, 0);
        assert!(!c_array.is_null());

        let mut cframe: *mut u8 = std::ptr::null_mut();
        let mut cframe_len = 0i64;
        let mut needs_free = false;
        let rc = ffi::b2nd_to_cframe(c_array, &mut cframe, &mut cframe_len, &mut needs_free);
        assert_eq!(rc, 0);
        assert!(!cframe.is_null());
        assert!(cframe_len > 0);
        let frame = std::slice::from_raw_parts(cframe, cframe_len as usize);
        let rust_array = B2ndArray::from_frame(frame).unwrap();
        assert_eq!(rust_array.meta.shape, shape);
        assert_eq!(rust_array.meta.chunkshape, chunkshape);
        assert_eq!(rust_array.meta.blockshape, blockshape);
        assert_eq!(rust_array.meta.dtype, "<u2");
        assert_eq!(rust_array.to_cbuffer().unwrap(), data);

        if needs_free {
            free(cframe.cast());
        }
        assert_eq!(ffi::b2nd_free(c_array), 0);
        assert_eq!(ffi::b2nd_free_ctx(ctx), 0);
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

#[test]
fn test_rust_sframe_c_reads() {
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

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rust-sframe.b2frame");
    schunk.to_sframe_dir(&path).unwrap();
    let c_path = CString::new(path.to_str().unwrap()).unwrap();
    let c_schunk = unsafe { ffi::blosc2_schunk_open(c_path.as_ptr()) };
    assert!(!c_schunk.is_null(), "C failed to open Rust-produced sframe");

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
            "C failed to decompress Rust sframe chunk {idx}"
        );
        assert_eq!(&restored, expected, "Rust sframe chunk {idx} mismatch");
    }

    let rc = unsafe { ffi::blosc2_schunk_free(c_schunk) };
    assert_eq!(rc, 0);
}

#[test]
fn test_c_sframe_rust_reads() {
    let _b = init_blosc2();

    let chunks: Vec<Vec<u8>> = (0..3)
        .map(|chunk| {
            (0..2048u32)
                .flat_map(|i| (i + chunk * 2048).to_le_bytes())
                .collect()
        })
        .collect();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("c-sframe.b2frame");
    let c_path = CString::new(path.to_str().unwrap()).unwrap();

    unsafe {
        let mut cparams: ffi::blosc2_cparams = std::mem::zeroed();
        cparams.compcode = BLOSC_LZ4;
        cparams.clevel = 5;
        cparams.typesize = 4;
        cparams.nthreads = 1;
        cparams.splitmode = BLOSC_FORWARD_COMPAT_SPLIT;
        cparams.filters[BLOSC2_MAX_FILTERS - 1] = BLOSC_SHUFFLE;
        let mut dparams: ffi::blosc2_dparams = std::mem::zeroed();
        dparams.nthreads = 1;
        dparams.typesize = 4;
        let mut storage: ffi::blosc2_storage = std::mem::zeroed();
        storage.contiguous = false;
        storage.urlpath = c_path.as_ptr() as *mut _;
        storage.cparams = &mut cparams;
        storage.dparams = &mut dparams;

        let c_schunk = ffi::blosc2_schunk_new(&mut storage);
        assert!(!c_schunk.is_null(), "C failed to create sframe");
        for chunk in &chunks {
            let rc = ffi::blosc2_schunk_append_buffer(
                c_schunk,
                chunk.as_ptr().cast(),
                chunk.len() as i32,
            );
            assert!(rc >= 0, "C failed to append sparse frame chunk: {rc}");
        }
        assert_eq!(ffi::blosc2_schunk_free(c_schunk), 0);
    }

    let rust = Schunk::open_sframe(&path).unwrap();
    assert_eq!(rust.nchunks(), chunks.len() as i64);
    for (idx, expected) in chunks.iter().enumerate() {
        assert_eq!(
            rust.decompress_chunk(idx as i64).unwrap(),
            expected.as_slice()
        );
    }
    let lazy = Schunk::open_lazy_sframe(&path).unwrap();
    assert_eq!(lazy.decompress_chunk(1).unwrap(), chunks[1]);
}
