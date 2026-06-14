#![cfg(feature = "_ffi")]
use blosc2_pure_rs::b2nd::{B2ndArray, B2ndMeta};
use blosc2_pure_rs::compress::{
    blosc1_compress, blosc2_chunk_nans_c as rust_chunk_nans_c, blosc2_chunk_nans_with_cparams,
    blosc2_chunk_repeatval_c as rust_chunk_repeatval_c, blosc2_chunk_repeatval_with_cparams,
    blosc2_chunk_uninit_c as rust_chunk_uninit_c, blosc2_chunk_uninit_with_cparams,
    blosc2_chunk_zeros_c as rust_chunk_zeros_c, blosc2_chunk_zeros_with_cparams,
    blosc2_create_cctx, blosc2_create_dctx,
    blosc2_vlchunk_get_nblocks_c as rust_vlchunk_get_nblocks_c,
    blosc2_vlcompress_ctx as rust_vlcompress_ctx,
    blosc2_vldecompress_block_ctx as rust_vldecompress_block_ctx,
    blosc2_vldecompress_block_ctx_c as rust_vldecompress_block_ctx_c,
    blosc2_vldecompress_ctx_c as rust_vldecompress_ctx_c, cbuffer_metainfo, cbuffer_sizes,
    cbuffer_validate, compress, decompress, decompress_into_with_dparams, vlchunk_get_nblocks,
    vlcompress, vldecompress, vldecompress_block, CParams, DParams, PostfilterParams,
};
use blosc2_pure_rs::constants::*;
use blosc2_pure_rs::header::ChunkHeader;
mod common;
use blosc2_pure_rs::schunk::Schunk;
use common::ffi;
use std::ffi::CString;
use std::fs;
use std::os::raw::c_void;
use std::path::PathBuf;
use zstd_pure_rs::decompress::zstd_ddict::{ZSTD_DDict_dictContent, ZSTD_createDDict};

const ZSTD_DICT_MAGIC_LE: [u8; 4] = 0xEC30_A437u32.to_le_bytes();

unsafe extern "C" {
    fn free(ptr: *mut c_void);
    fn rand() -> i32;
    fn srand(seed: u32);
}

fn init_blosc2() -> common::Blosc2 {
    common::Blosc2::new()
}

fn c_blosc2_source_dir() -> PathBuf {
    option_env!("BLOSC2_C_SOURCE_DIR_RESOLVED")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("BLOSC2_C_SOURCE_DIR").map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("c-blosc2"))
}

struct CArray {
    ctx: *mut ffi::b2nd_context_t,
    array: *mut ffi::b2nd_array_t,
}

impl CArray {
    fn from_u8_cbuffer(shape: &[i64], chunkshape: &[i32], blockshape: &[i32], data: &[u8]) -> Self {
        let dtype = CString::new("|u1").unwrap();
        unsafe {
            let cparams: &mut ffi::blosc2_cparams = Box::leak(Box::new(std::mem::zeroed()));
            cparams.compcode = BLOSC_LZ4;
            cparams.clevel = 5;
            cparams.typesize = 1;
            cparams.nthreads = 1;
            cparams.splitmode = BLOSC_NEVER_SPLIT;
            cparams.filters[BLOSC2_MAX_FILTERS - 1] = BLOSC_SHUFFLE;
            let dparams: &mut ffi::blosc2_dparams = Box::leak(Box::new(std::mem::zeroed()));
            dparams.nthreads = 1;
            dparams.typesize = 1;
            let storage: &mut ffi::blosc2_storage = Box::leak(Box::new(ffi::blosc2_storage {
                contiguous: true,
                urlpath: std::ptr::null_mut(),
                cparams,
                dparams,
                io: std::ptr::null_mut(),
            }));

            let ctx = ffi::b2nd_create_ctx(
                storage,
                shape.len() as i8,
                shape.as_ptr(),
                chunkshape.as_ptr(),
                blockshape.as_ptr(),
                dtype.as_ptr(),
                0,
                std::ptr::null(),
                0,
            );
            assert!(!ctx.is_null());

            let mut array: *mut ffi::b2nd_array_t = std::ptr::null_mut();
            let rc =
                ffi::b2nd_from_cbuffer(ctx, &mut array, data.as_ptr().cast(), data.len() as i64);
            assert_eq!(rc, 0);
            assert!(!array.is_null());

            Self { ctx, array }
        }
    }

    fn to_cbuffer(&self, len: usize) -> Vec<u8> {
        let mut out = vec![0; len];
        unsafe {
            let rc = ffi::b2nd_to_cbuffer(self.array, out.as_mut_ptr().cast(), out.len() as i64);
            assert_eq!(rc, 0);
        }
        out
    }
}

impl Drop for CArray {
    fn drop(&mut self) {
        unsafe {
            if !self.array.is_null() {
                assert_eq!(ffi::b2nd_free(self.array), 0);
            }
            if !self.ctx.is_null() {
                assert_eq!(ffi::b2nd_free_ctx(self.ctx), 0);
            }
        }
    }
}

fn b2nd_u8_meta(shape: &[i64], chunkshape: &[i32], blockshape: &[i32]) -> B2ndMeta {
    B2ndMeta::new(
        shape.to_vec(),
        chunkshape.to_vec(),
        blockshape.to_vec(),
        "|u1",
        0,
    )
    .unwrap()
}

fn b2nd_u8_cparams() -> CParams {
    CParams {
        compcode: BLOSC_LZ4,
        clevel: 5,
        typesize: 1,
        splitmode: BLOSC_NEVER_SPLIT,
        filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
        ..Default::default()
    }
}

fn b2nd_u8_array(shape: &[i64], chunkshape: &[i32], blockshape: &[i32], data: &[u8]) -> B2ndArray {
    B2ndArray::from_cbuffer(
        b2nd_u8_meta(shape, chunkshape, blockshape),
        data,
        b2nd_u8_cparams(),
        DParams::default(),
    )
    .unwrap()
}

fn ndlz_f32_fixture_data(shape: [i64; 2]) -> Vec<u8> {
    let nelem = (shape[0] * shape[1]) as usize;
    let mut data = Vec::with_capacity(nelem * 4);
    unsafe {
        // C's unseeded rand() sequence is equivalent to srand(1) on libc.
        srand(1);
        for _ in 0..nelem {
            let value = (rand() % 220) as f32;
            data.extend_from_slice(&value.to_le_bytes());
        }
    }
    data
}

fn c_compress_roundtrip_data(type_size: usize, num_elements: usize) -> Vec<u8> {
    (0..type_size * num_elements).map(|k| k as u8).collect()
}

fn ndlz_f64_some_matches_fixture_data(shape: [i64; 2]) -> Vec<u8> {
    let nelem = (shape[0] * shape[1]) as usize;
    let mut data = Vec::with_capacity(nelem * 8);
    for i in 0..nelem {
        let value = if i < nelem / 2 { i as f64 } else { 1.0 };
        data.extend_from_slice(&value.to_le_bytes());
    }
    data
}

fn ndlz_f64_same_cells_fixture_data(shape: [i64; 2]) -> Vec<u8> {
    let nelem = (shape[0] * shape[1]) as usize;
    let mut values = vec![0.0f64; nelem];
    for i in 0..nelem / 4 {
        values[i * 4] = 11111111.0;
        values[i * 4 + 1] = 99999999.0;
    }
    let mut data = Vec::with_capacity(nelem * 8);
    for value in values {
        data.extend_from_slice(&value.to_le_bytes());
    }
    data
}

fn row_major_strides(shape: &[usize]) -> Vec<usize> {
    let mut stride = 1usize;
    let mut strides = vec![0; shape.len()];
    for axis in (0..shape.len()).rev() {
        strides[axis] = stride;
        stride *= shape[axis];
    }
    strides
}

fn unravel_row_major(mut linear: usize, shape: &[usize]) -> Vec<usize> {
    let strides = row_major_strides(shape);
    let mut idx = vec![0; shape.len()];
    for axis in 0..shape.len() {
        idx[axis] = linear / strides[axis];
        linear %= strides[axis];
    }
    idx
}

fn ravel_row_major(idx: &[usize], shape: &[usize]) -> usize {
    let strides = row_major_strides(shape);
    idx.iter()
        .zip(strides)
        .map(|(&idx, stride)| idx * stride)
        .sum()
}

fn b2nd_insert_expected(
    original: &[u8],
    shape: &[usize],
    axis: usize,
    start: usize,
    inserted_extent: usize,
    inserted: &[u8],
) -> Vec<u8> {
    let mut new_shape = shape.to_vec();
    new_shape[axis] += inserted_extent;
    let mut inserted_shape = shape.to_vec();
    inserted_shape[axis] = inserted_extent;
    let mut out = vec![0; new_shape.iter().product()];
    for (linear, byte) in out.iter_mut().enumerate() {
        let idx = unravel_row_major(linear, &new_shape);
        if (start..start + inserted_extent).contains(&idx[axis]) {
            let mut insert_idx = idx.clone();
            insert_idx[axis] -= start;
            *byte = inserted[ravel_row_major(&insert_idx, &inserted_shape)];
        } else {
            let mut source_idx = idx;
            if source_idx[axis] >= start + inserted_extent {
                source_idx[axis] -= inserted_extent;
            }
            *byte = original[ravel_row_major(&source_idx, shape)];
        }
    }
    out
}

fn b2nd_delete_expected(
    original: &[u8],
    shape: &[usize],
    axis: usize,
    start: usize,
    len: usize,
) -> Vec<u8> {
    let mut new_shape = shape.to_vec();
    new_shape[axis] -= len;
    let mut out = vec![0; new_shape.iter().product()];
    for (linear, byte) in out.iter_mut().enumerate() {
        let mut source_idx = unravel_row_major(linear, &new_shape);
        if source_idx[axis] >= start {
            source_idx[axis] += len;
        }
        *byte = original[ravel_row_major(&source_idx, shape)];
    }
    out
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

fn special_cparams(typesize: i32, blocksize: i32) -> CParams {
    special_cparams_for(BLOSC_BLOSCLZ, typesize, blocksize)
}

fn special_cparams_for(compcode: u8, typesize: i32, blocksize: i32) -> CParams {
    CParams {
        compcode,
        clevel: 5,
        typesize,
        blocksize,
        nthreads: 1,
        splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
        ..Default::default()
    }
}

fn special_cparams_with_dict_for(compcode: u8, typesize: i32, blocksize: i32) -> CParams {
    CParams {
        use_dict: true,
        ..special_cparams_for(compcode, typesize, blocksize)
    }
}

unsafe fn ffi_special_cparams(typesize: i32, blocksize: i32) -> ffi::blosc2_cparams {
    ffi_special_cparams_for(BLOSC_BLOSCLZ, typesize, blocksize)
}

unsafe fn ffi_special_cparams_for(
    compcode: u8,
    typesize: i32,
    blocksize: i32,
) -> ffi::blosc2_cparams {
    let mut cparams: ffi::blosc2_cparams = std::mem::zeroed();
    cparams.compcode = compcode;
    cparams.clevel = 5;
    cparams.typesize = typesize;
    cparams.blocksize = blocksize;
    cparams.nthreads = 1;
    cparams.splitmode = BLOSC_FORWARD_COMPAT_SPLIT;
    cparams
}

unsafe fn c_special_chunk(
    compcode: u8,
    special: u8,
    nbytes: i32,
    typesize: i32,
    blocksize: i32,
    use_dict: bool,
    value: Option<&[u8]>,
) -> Vec<u8> {
    let destsize = BLOSC_EXTENDED_HEADER_LENGTH as i32 + value.map_or(0, |v| v.len() as i32);
    let mut dest = vec![0u8; destsize as usize];
    let mut cparams = ffi_special_cparams_for(compcode, typesize, blocksize);
    cparams.use_dict = i32::from(use_dict);
    let csize = match special {
        BLOSC2_SPECIAL_ZERO => {
            ffi::blosc2_chunk_zeros(cparams, nbytes, dest.as_mut_ptr() as *mut c_void, destsize)
        }
        BLOSC2_SPECIAL_NAN => {
            ffi::blosc2_chunk_nans(cparams, nbytes, dest.as_mut_ptr() as *mut c_void, destsize)
        }
        BLOSC2_SPECIAL_UNINIT => {
            ffi::blosc2_chunk_uninit(cparams, nbytes, dest.as_mut_ptr() as *mut c_void, destsize)
        }
        BLOSC2_SPECIAL_VALUE => ffi::blosc2_chunk_repeatval(
            cparams,
            nbytes,
            dest.as_mut_ptr() as *mut c_void,
            destsize,
            value.unwrap().as_ptr() as *const c_void,
        ),
        _ => unreachable!("unsupported special chunk type"),
    };
    assert!(csize > 0, "C special chunk constructor failed: {csize}");
    dest.truncate(csize as usize);
    dest
}

fn c_decompress_chunk(chunk: &[u8], nbytes: usize) -> (i32, Vec<u8>) {
    let mut dest = vec![0xa5; nbytes];
    let rc = unsafe {
        ffi::blosc2_decompress(
            chunk.as_ptr() as *const c_void,
            chunk.len() as i32,
            dest.as_mut_ptr() as *mut c_void,
            dest.len() as i32,
        )
    };
    (rc, dest)
}

unsafe fn c_compress_with_params(data: &[u8], cparams: ffi::blosc2_cparams) -> Vec<u8> {
    let mut compressed = vec![0u8; data.len() + BLOSC2_MAX_OVERHEAD + BLOSC2_MAXDICTSIZE];
    let cctx = ffi::blosc2_create_cctx(cparams);
    assert!(!cctx.is_null());
    let csize = ffi::blosc2_compress_ctx(
        cctx,
        data.as_ptr() as *const c_void,
        data.len() as i32,
        compressed.as_mut_ptr() as *mut c_void,
        compressed.len() as i32,
    );
    ffi::blosc2_free_ctx(cctx);
    assert!(csize > 0, "C compression failed: {csize}");
    compressed.truncate(csize as usize);
    compressed
}

fn regular_embedded_dict_len(chunk: &[u8]) -> Option<usize> {
    let header = ChunkHeader::read(chunk).unwrap();
    if !header.use_dict() {
        return None;
    }
    let dict_size_pos = header.header_len() + header.nblocks() * 4;
    Some(i32::from_le_bytes(chunk[dict_size_pos..dict_size_pos + 4].try_into().unwrap()) as usize)
}

fn regular_embedded_dict(chunk: &[u8]) -> Option<&[u8]> {
    let header = ChunkHeader::read(chunk).unwrap();
    if !header.use_dict() {
        return None;
    }
    let dict_size_pos = header.header_len() + header.nblocks() * 4;
    let dict_start = dict_size_pos + 4;
    let dict_len =
        i32::from_le_bytes(chunk[dict_size_pos..dict_start].try_into().unwrap()) as usize;
    Some(&chunk[dict_start..dict_start + dict_len])
}

fn zstd_dict_content(dict: &[u8]) -> Vec<u8> {
    let ddict = ZSTD_createDDict(dict).unwrap();
    ZSTD_DDict_dictContent(&ddict).to_vec()
}

#[test]
fn test_compress_roundtrip_csv_fixtures_match_c_behavior() {
    let _b = init_blosc2();

    const TYPE_SIZES: &[usize] = &[
        1, 2, 3, 4, 5, 6, 7, 8, 11, 16, 22, 30, 32, 42, 48, 52, 53, 64, 80,
    ];
    const NUM_ELEMENTS: &[usize] = &[7, 192, 1792, 500, 8000, 100_000, 702_713];

    for &shuffle_enabled in &[false, true] {
        for &type_size in TYPE_SIZES {
            for &num_elements in NUM_ELEMENTS {
                let data = c_compress_roundtrip_data(type_size, num_elements);
                let mut c_compressed = vec![0u8; data.len() + BLOSC2_MAX_OVERHEAD];
                let c_csize = unsafe {
                    ffi::blosc1_compress(
                        5,
                        i32::from(shuffle_enabled),
                        type_size,
                        data.len(),
                        data.as_ptr().cast(),
                        c_compressed.as_mut_ptr().cast(),
                        c_compressed.len(),
                    )
                };
                assert!(
                    c_csize > 0,
                    "C blosc1_compress failed for typesize={type_size}, num_elements={num_elements}, shuffle={shuffle_enabled}"
                );

                let mut c_result = vec![0u8; data.len()];
                let c_dsize = unsafe {
                    ffi::blosc1_decompress(
                        c_compressed.as_ptr().cast(),
                        c_result.as_mut_ptr().cast(),
                        c_result.len(),
                    )
                };
                assert_eq!(
                    c_dsize,
                    data.len() as i32,
                    "C blosc1_decompress size mismatch for typesize={type_size}, num_elements={num_elements}, shuffle={shuffle_enabled}"
                );
                assert_eq!(c_result, data);

                let rust_filter = if shuffle_enabled {
                    BLOSC_SHUFFLE
                } else {
                    BLOSC_NOFILTER
                };
                let mut rust_compressed = vec![0u8; data.len() + BLOSC2_MAX_OVERHEAD];
                let rust_csize =
                    blosc1_compress(5, rust_filter, type_size as i32, &data, &mut rust_compressed)
                        .unwrap_or_else(|e| {
                            panic!(
                                "Rust blosc1_compress failed for typesize={type_size}, num_elements={num_elements}, shuffle={shuffle_enabled}: {e}"
                            )
                        });
                rust_compressed.truncate(rust_csize);
                let mut rust_to_c_result = vec![0u8; data.len()];
                let rust_to_c_dsize = unsafe {
                    ffi::blosc1_decompress(
                        rust_compressed.as_ptr().cast(),
                        rust_to_c_result.as_mut_ptr().cast(),
                        rust_to_c_result.len(),
                    )
                };
                assert_eq!(
                    rust_to_c_dsize,
                    data.len() as i32,
                    "C failed to decompress Rust Blosc1 fixture for typesize={type_size}, num_elements={num_elements}, shuffle={shuffle_enabled}"
                );
                assert_eq!(rust_to_c_result, data);
            }
        }
    }
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

#[test]
fn test_blosclz_cdata_fixtures_match_c_decompress() {
    let _b = init_blosc2();
    let fixture_names = ["blosc-blosclz-3.0.0.cdata"];
    let compat_dir = c_blosc2_source_dir().join("compat");

    for fixture_name in fixture_names {
        let fixture = compat_dir.join(fixture_name);
        if !fixture.exists() {
            eprintln!("skipping missing BloscLZ C fixture: {}", fixture.display());
            continue;
        }

        let chunk = fs::read(&fixture).unwrap_or_else(|e| {
            panic!(
                "failed to read BloscLZ C fixture {}: {e}",
                fixture.display()
            )
        });
        let rust_decompressed = decompress(&chunk)
            .unwrap_or_else(|e| panic!("Rust failed to decompress {}: {e}", fixture.display()));

        let mut c_decompressed = vec![0u8; rust_decompressed.len()];
        let c_dsize = unsafe {
            ffi::blosc2_decompress(
                chunk.as_ptr() as *const _,
                chunk.len() as i32,
                c_decompressed.as_mut_ptr() as *mut _,
                c_decompressed.len() as i32,
            )
        };
        assert_eq!(
            c_dsize,
            rust_decompressed.len() as i32,
            "C decompression size mismatch for {}",
            fixture.display()
        );
        assert_eq!(
            rust_decompressed,
            c_decompressed,
            "Rust and C fixture decompression differ for {}",
            fixture.display()
        );
    }
}

#[test]
fn test_special_chunk_constructors_match_c() {
    let _b = init_blosc2();

    let cases: &[(u8, u8, i32, i32, i32, bool, Option<&[u8]>)] = &[
        (BLOSC_BLOSCLZ, BLOSC2_SPECIAL_ZERO, 96, 4, 0, false, None),
        (BLOSC_BLOSCLZ, BLOSC2_SPECIAL_NAN, 64, 8, 0, false, None),
        (
            BLOSC_BLOSCLZ,
            BLOSC2_SPECIAL_VALUE,
            80,
            4,
            0,
            false,
            Some(&[0x11, 0x22, 0x33, 0x44]),
        ),
        (BLOSC_BLOSCLZ, BLOSC2_SPECIAL_UNINIT, 48, 4, 0, false, None),
        (BLOSC_BLOSCLZ, BLOSC2_SPECIAL_ZERO, 96, 4, 32, false, None),
        (BLOSC_BLOSCLZ, BLOSC2_SPECIAL_ZERO, 4000, 4, 0, true, None),
        (BLOSC_BLOSCLZ, BLOSC2_SPECIAL_NAN, 4000, 4, 0, true, None),
        (
            BLOSC_BLOSCLZ,
            BLOSC2_SPECIAL_VALUE,
            4000,
            4,
            0,
            true,
            Some(&[0x11, 0x22, 0x33, 0x44]),
        ),
        (BLOSC_BLOSCLZ, BLOSC2_SPECIAL_UNINIT, 4000, 4, 0, true, None),
        (BLOSC_LZ4, BLOSC2_SPECIAL_ZERO, 4000, 4, 0, true, None),
        (BLOSC_LZ4, BLOSC2_SPECIAL_NAN, 4000, 4, 0, true, None),
        (
            BLOSC_LZ4,
            BLOSC2_SPECIAL_VALUE,
            4000,
            4,
            0,
            true,
            Some(&[0x11, 0x22, 0x33, 0x44]),
        ),
        (BLOSC_LZ4, BLOSC2_SPECIAL_UNINIT, 4000, 4, 0, true, None),
        (BLOSC_ZLIB, BLOSC2_SPECIAL_ZERO, 4000, 4, 0, true, None),
        (BLOSC_ZLIB, BLOSC2_SPECIAL_NAN, 4000, 4, 0, true, None),
        (
            BLOSC_ZLIB,
            BLOSC2_SPECIAL_VALUE,
            4000,
            4,
            0,
            true,
            Some(&[0x11, 0x22, 0x33, 0x44]),
        ),
        (BLOSC_ZLIB, BLOSC2_SPECIAL_UNINIT, 4000, 4, 0, true, None),
    ];

    for &(compcode, special, nbytes, typesize, blocksize, use_dict, value) in cases {
        let cparams = if use_dict {
            special_cparams_with_dict_for(compcode, typesize, blocksize)
        } else {
            special_cparams_for(compcode, typesize, blocksize)
        };
        let c_chunk = unsafe {
            c_special_chunk(
                compcode, special, nbytes, typesize, blocksize, use_dict, value,
            )
        };
        let rust_chunk = match special {
            BLOSC2_SPECIAL_ZERO => blosc2_chunk_zeros_with_cparams(nbytes as usize, &cparams),
            BLOSC2_SPECIAL_NAN => blosc2_chunk_nans_with_cparams(nbytes as usize, &cparams),
            BLOSC2_SPECIAL_VALUE => {
                blosc2_chunk_repeatval_with_cparams(nbytes as usize, value.unwrap(), &cparams)
            }
            BLOSC2_SPECIAL_UNINIT => blosc2_chunk_uninit_with_cparams(nbytes as usize, &cparams),
            _ => unreachable!(),
        }
        .unwrap();

        assert_eq!(
            rust_chunk, c_chunk,
            "Rust special constructor output must match C for special={special}"
        );

        let header = ChunkHeader::read(&rust_chunk).unwrap();
        assert_eq!(header.special_type(), special);
        assert_eq!(header.nbytes, nbytes);
        assert_eq!(header.typesize, typesize as u8);
        let c_header = ChunkHeader::read(&c_chunk).unwrap();
        assert_eq!(
            header.blocksize, c_header.blocksize,
            "Rust special constructor blocksize must match C for special={special}"
        );
        assert_eq!(header.cbytes as usize, rust_chunk.len());

        let mut rust_adapter_dest = vec![0u8; rust_chunk.len()];
        let rust_adapter_rc = match special {
            BLOSC2_SPECIAL_ZERO => rust_chunk_zeros_c(
                cparams.clone(),
                nbytes,
                &mut rust_adapter_dest,
                rust_chunk.len() as i32,
            ),
            BLOSC2_SPECIAL_NAN => rust_chunk_nans_c(
                cparams.clone(),
                nbytes,
                &mut rust_adapter_dest,
                rust_chunk.len() as i32,
            ),
            BLOSC2_SPECIAL_VALUE => rust_chunk_repeatval_c(
                cparams.clone(),
                nbytes,
                &mut rust_adapter_dest,
                rust_chunk.len() as i32,
                value.unwrap(),
            ),
            BLOSC2_SPECIAL_UNINIT => rust_chunk_uninit_c(
                cparams.clone(),
                nbytes,
                &mut rust_adapter_dest,
                rust_chunk.len() as i32,
            ),
            _ => unreachable!(),
        };
        assert_eq!(rust_adapter_rc, c_chunk.len() as i32);
        assert_eq!(rust_adapter_dest, c_chunk);

        let rust_decompressed = decompress(&c_chunk).unwrap();
        assert_eq!(rust_decompressed.len(), nbytes as usize);
        let (c_dsize, c_decompressed) = c_decompress_chunk(&rust_chunk, nbytes as usize);
        assert_eq!(
            c_dsize, nbytes,
            "C decompression size mismatch for special={special}"
        );

        if special != BLOSC2_SPECIAL_UNINIT {
            assert_eq!(
                rust_decompressed, c_decompressed,
                "C and Rust deterministic special decompression differ for special={special}"
            );
        }
    }
}

#[test]
fn test_special_chunk_constructor_error_codes_match_c() {
    let _b = init_blosc2();

    unsafe {
        let cparams = ffi_special_cparams(4, 0);
        let mut dest = vec![0u8; BLOSC_EXTENDED_HEADER_LENGTH];
        let dest_len = dest.len() as i32;

        let short_dest_len = (BLOSC_EXTENDED_HEADER_LENGTH - 1) as i32;
        assert_eq!(
            ffi::blosc2_chunk_zeros(cparams, 8, dest.as_mut_ptr() as *mut c_void, short_dest_len,),
            BLOSC2_ERROR_DATA
        );
        assert_eq!(
            rust_chunk_zeros_c(special_cparams(4, 0), 8, &mut dest, short_dest_len),
            BLOSC2_ERROR_DATA
        );

        assert_eq!(
            ffi::blosc2_chunk_nans(cparams, 8, dest.as_mut_ptr() as *mut c_void, short_dest_len,),
            BLOSC2_ERROR_DATA
        );
        assert_eq!(
            rust_chunk_nans_c(special_cparams(4, 0), 8, &mut dest, short_dest_len),
            BLOSC2_ERROR_DATA
        );

        assert_eq!(
            ffi::blosc2_chunk_uninit(cparams, 8, dest.as_mut_ptr() as *mut c_void, short_dest_len,),
            BLOSC2_ERROR_DATA
        );
        assert_eq!(
            rust_chunk_uninit_c(special_cparams(4, 0), 8, &mut dest, short_dest_len),
            BLOSC2_ERROR_DATA
        );

        assert_eq!(
            ffi::blosc2_chunk_zeros(cparams, 6, dest.as_mut_ptr() as *mut c_void, dest_len,),
            BLOSC2_ERROR_DATA
        );
        assert_eq!(
            rust_chunk_zeros_c(special_cparams(4, 0), 6, &mut dest, dest_len),
            BLOSC2_ERROR_DATA
        );
        assert!(blosc2_chunk_zeros_with_cparams(6, &special_cparams(4, 0)).is_err());

        assert_eq!(
            ffi::blosc2_chunk_nans(cparams, 6, dest.as_mut_ptr() as *mut c_void, dest_len,),
            BLOSC2_ERROR_DATA
        );
        assert_eq!(
            rust_chunk_nans_c(special_cparams(4, 0), 6, &mut dest, dest_len),
            BLOSC2_ERROR_DATA
        );
        assert!(blosc2_chunk_nans_with_cparams(6, &special_cparams(4, 0)).is_err());

        assert_eq!(
            ffi::blosc2_chunk_uninit(cparams, 6, dest.as_mut_ptr() as *mut c_void, dest_len,),
            BLOSC2_ERROR_DATA
        );
        assert_eq!(
            rust_chunk_uninit_c(special_cparams(4, 0), 6, &mut dest, dest_len),
            BLOSC2_ERROR_DATA
        );
        assert!(blosc2_chunk_uninit_with_cparams(6, &special_cparams(4, 0)).is_err());

        let mut repeat_dest = vec![0u8; BLOSC_EXTENDED_HEADER_LENGTH + 4];
        assert_eq!(
            ffi::blosc2_chunk_repeatval(
                cparams,
                8,
                repeat_dest.as_mut_ptr() as *mut c_void,
                BLOSC_EXTENDED_HEADER_LENGTH as i32,
                [1u8, 2, 3, 4].as_ptr() as *const c_void,
            ),
            BLOSC2_ERROR_DATA
        );
        assert_eq!(
            rust_chunk_repeatval_c(
                special_cparams(4, 0),
                8,
                &mut repeat_dest,
                BLOSC_EXTENDED_HEADER_LENGTH as i32,
                &[1, 2, 3, 4],
            ),
            BLOSC2_ERROR_DATA
        );

        assert_eq!(
            ffi::blosc2_chunk_repeatval(
                cparams,
                6,
                repeat_dest.as_mut_ptr() as *mut c_void,
                repeat_dest.len() as i32,
                [1u8, 2, 3, 4].as_ptr() as *const c_void,
            ),
            BLOSC2_ERROR_DATA
        );
        let repeat_dest_len = repeat_dest.len() as i32;
        assert_eq!(
            rust_chunk_repeatval_c(
                special_cparams(4, 0),
                6,
                &mut repeat_dest,
                repeat_dest_len,
                &[1, 2, 3, 4],
            ),
            BLOSC2_ERROR_DATA
        );
        assert!(
            blosc2_chunk_repeatval_with_cparams(6, &[1, 2, 3, 4], &special_cparams(4, 0)).is_err()
        );

        for &(special, nbytes, destsize, value) in &[
            (
                BLOSC2_SPECIAL_ZERO,
                -1,
                BLOSC_EXTENDED_HEADER_LENGTH as i32,
                None,
            ),
            (
                BLOSC2_SPECIAL_UNINIT,
                -1,
                BLOSC_EXTENDED_HEADER_LENGTH as i32,
                None,
            ),
            (
                BLOSC2_SPECIAL_NAN,
                -1,
                BLOSC_EXTENDED_HEADER_LENGTH as i32,
                None,
            ),
            (
                BLOSC2_SPECIAL_VALUE,
                -1,
                (BLOSC_EXTENDED_HEADER_LENGTH + 4) as i32,
                Some([1u8, 2, 3, 4].as_slice()),
            ),
            (
                BLOSC2_SPECIAL_UNINIT,
                -4,
                BLOSC_EXTENDED_HEADER_LENGTH as i32,
                None,
            ),
            (
                BLOSC2_SPECIAL_NAN,
                -4,
                BLOSC_EXTENDED_HEADER_LENGTH as i32,
                None,
            ),
            (
                BLOSC2_SPECIAL_VALUE,
                -4,
                (BLOSC_EXTENDED_HEADER_LENGTH + 4) as i32,
                Some([1u8, 2, 3, 4].as_slice()),
            ),
        ] {
            let mut c_dest = vec![0u8; destsize as usize];
            let mut rust_dest = vec![0u8; destsize as usize];
            let c_rc = match special {
                BLOSC2_SPECIAL_ZERO => ffi::blosc2_chunk_zeros(
                    cparams,
                    nbytes,
                    c_dest.as_mut_ptr() as *mut c_void,
                    destsize,
                ),
                BLOSC2_SPECIAL_UNINIT => ffi::blosc2_chunk_uninit(
                    cparams,
                    nbytes,
                    c_dest.as_mut_ptr() as *mut c_void,
                    destsize,
                ),
                BLOSC2_SPECIAL_NAN => ffi::blosc2_chunk_nans(
                    cparams,
                    nbytes,
                    c_dest.as_mut_ptr() as *mut c_void,
                    destsize,
                ),
                BLOSC2_SPECIAL_VALUE => ffi::blosc2_chunk_repeatval(
                    cparams,
                    nbytes,
                    c_dest.as_mut_ptr() as *mut c_void,
                    destsize,
                    value.unwrap().as_ptr() as *const c_void,
                ),
                _ => unreachable!(),
            };
            let rust_rc = match special {
                BLOSC2_SPECIAL_ZERO => {
                    rust_chunk_zeros_c(special_cparams(4, 0), nbytes, &mut rust_dest, destsize)
                }
                BLOSC2_SPECIAL_UNINIT => {
                    rust_chunk_uninit_c(special_cparams(4, 0), nbytes, &mut rust_dest, destsize)
                }
                BLOSC2_SPECIAL_NAN => {
                    rust_chunk_nans_c(special_cparams(4, 0), nbytes, &mut rust_dest, destsize)
                }
                BLOSC2_SPECIAL_VALUE => rust_chunk_repeatval_c(
                    special_cparams(4, 0),
                    nbytes,
                    &mut rust_dest,
                    destsize,
                    value.unwrap(),
                ),
                _ => unreachable!(),
            };
            assert_eq!(
                rust_rc, c_rc,
                "negative nbytes return code drift for special={special}, nbytes={nbytes}"
            );
            if c_rc > 0 {
                assert_eq!(
                    &rust_dest[..c_rc as usize],
                    &c_dest[..c_rc as usize],
                    "negative nbytes chunk bytes drift for special={special}, nbytes={nbytes}"
                );
            }
        }
    }
}

#[test]
fn test_legacy_blosc1_bitshuffle_fixture_matches_c_decompress() {
    let _b = init_blosc2();
    let fixture = c_blosc2_source_dir()
        .join("compat")
        .join("blosc-1.17.1-lz4-bitshuffle8-nomemcpy.cdata");
    if !fixture.exists() {
        eprintln!(
            "skipping missing legacy Blosc1 fixture: {}",
            fixture.display()
        );
        return;
    }

    let chunk = fs::read(&fixture).unwrap_or_else(|e| {
        panic!(
            "failed to read legacy Blosc1 fixture {}: {e}",
            fixture.display()
        )
    });
    assert_eq!(chunk[BLOSC2_CHUNK_VERSION], BLOSC1_VERSION_FORMAT);
    assert_eq!(
        chunk[BLOSC2_CHUNK_FLAGS],
        BLOSC_DOBITSHUFFLE | (BLOSC_LZ4_FORMAT << 5)
    );

    let header = ChunkHeader::read(&chunk).unwrap();
    assert!(!header.is_extended());
    assert_eq!(header.header_len(), BLOSC_MIN_HEADER_LENGTH);
    assert_eq!(header.typesize, 8);
    assert_eq!(header.compcode(), BLOSC_LZ4);
    assert_eq!(header.flags & BLOSC_DOBITSHUFFLE, BLOSC_DOBITSHUFFLE);
    assert_eq!(header.filters[BLOSC2_MAX_FILTERS - 1], BLOSC_BITSHUFFLE);
    assert_eq!(cbuffer_sizes(&chunk).unwrap(), (641_092, 22_760, 524_288));

    let (typesize, compcode, filters) =
        cbuffer_metainfo(&chunk[..BLOSC_MIN_HEADER_LENGTH]).unwrap();
    assert_eq!(typesize, 8);
    assert_eq!(compcode, BLOSC_LZ4);
    assert_eq!(filters[BLOSC2_MAX_FILTERS - 1], BLOSC_BITSHUFFLE);
    assert!(cbuffer_validate(&chunk).is_ok());

    let rust_decompressed = decompress(&chunk).unwrap_or_else(|e| {
        panic!(
            "Rust failed to decompress legacy fixture {}: {e}",
            fixture.display()
        )
    });
    assert_eq!(rust_decompressed.len(), 641_092);

    let mut c_decompressed = vec![0u8; rust_decompressed.len()];
    let c_dsize = unsafe {
        ffi::blosc2_decompress(
            chunk.as_ptr() as *const _,
            chunk.len() as i32,
            c_decompressed.as_mut_ptr() as *mut _,
            c_decompressed.len() as i32,
        )
    };
    assert_eq!(c_dsize, rust_decompressed.len() as i32);
    assert_eq!(rust_decompressed, c_decompressed);
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
fn test_zstd_fastcover_dictionary_uses_contiguous_c_training_budget() {
    let _b = init_blosc2();

    let data: Vec<u8> = (0..240_000u32)
        .flat_map(|i| i.wrapping_mul(37).rotate_left(11).to_le_bytes())
        .collect();

    let rust_chunk = compress(
        &data,
        &CParams {
            compcode: BLOSC_ZSTD,
            clevel: 5,
            typesize: 1,
            blocksize: 8192,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0; BLOSC2_MAX_FILTERS],
            use_dict: true,
            ..Default::default()
        },
    )
    .unwrap();
    let rust_header = ChunkHeader::read(&rust_chunk).unwrap();
    assert!(rust_header.use_dict());
    let rust_dict = regular_embedded_dict(&rust_chunk).unwrap();
    assert_eq!(&rust_dict[..4], &ZSTD_DICT_MAGIC_LE);
    let sample_nblocks = rust_header.nblocks().max(8);
    let sample_size = data.len() / sample_nblocks / 16;
    let expected_len = BLOSC2_MAXDICTSIZE
        .min(data.len() / 20)
        .min(sample_nblocks * sample_size);
    let content = zstd_dict_content(rust_dict);
    assert!(content.len() <= expected_len);
    assert_ne!(
        content,
        &data[..content.len()],
        "high-diversity fixture should exercise fastCover content selection, not prefix-only content"
    );

    let (c_dsize, c_decompressed) = c_decompress_chunk(&rust_chunk, data.len());
    assert_eq!(c_dsize, data.len() as i32);
    assert_eq!(c_decompressed, data);
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
    let rust_header = ChunkHeader::read(&rust_chunk).unwrap();
    assert!(rust_header.use_dict());
    assert!((BLOSC2_MINUSEFULDICT..=BLOSC2_MAXDICTSIZE)
        .contains(&regular_embedded_dict_len(&rust_chunk).unwrap()));
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
    c_chunk.truncate(c_csize as usize);
    let c_header = ChunkHeader::read(&c_chunk).unwrap();
    assert!(c_header.use_dict());
    assert!((BLOSC2_MINUSEFULDICT..=BLOSC2_MAXDICTSIZE)
        .contains(&regular_embedded_dict_len(&c_chunk).unwrap()));

    let rust_decompressed = decompress(&c_chunk).unwrap();
    assert_eq!(rust_decompressed, data);
}

fn c_trained_zstd_dict_regression_data() -> Vec<u8> {
    include_bytes!("../src/compress.rs").to_vec()
}

#[test]
fn test_c_trained_zstd_dictionary_chunk_decodes_in_pure_rust() {
    let _b = init_blosc2();

    let data = c_trained_zstd_dict_regression_data();
    let mut cparams: ffi::blosc2_cparams = unsafe { std::mem::zeroed() };
    cparams.compcode = BLOSC_ZSTD;
    cparams.clevel = 5;
    cparams.typesize = 1;
    cparams.nthreads = 1;
    cparams.blocksize = 0;
    cparams.splitmode = BLOSC_FORWARD_COMPAT_SPLIT;
    cparams.use_dict = 1;
    cparams.filters[BLOSC2_MAX_FILTERS - 1] = BLOSC_SHUFFLE;

    let c_chunk = unsafe { c_compress_with_params(&data, cparams) };
    let c_header = ChunkHeader::read(&c_chunk).unwrap();
    assert!(
        c_header.use_dict(),
        "C fixture must exercise dictionary path"
    );

    let (c_dsize, c_decompressed) = c_decompress_chunk(&c_chunk, data.len());
    assert_eq!(c_dsize, data.len() as i32, "C fixture must self-decode");
    assert_eq!(c_decompressed, data);

    let rust_decompressed = decompress(&c_chunk).unwrap();
    assert_eq!(rust_decompressed, data);
}

#[test]
fn test_c_trained_zstd_dictionary_lazy_frame_chunk_decodes_in_pure_rust() {
    let _b = init_blosc2();

    let data = c_trained_zstd_dict_regression_data();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("c-zstd-dict.b2frame");
    let c_path = CString::new(path.to_str().unwrap()).unwrap();

    unsafe {
        let mut cparams: ffi::blosc2_cparams = std::mem::zeroed();
        cparams.compcode = BLOSC_ZSTD;
        cparams.clevel = 5;
        cparams.typesize = 1;
        cparams.nthreads = 1;
        cparams.blocksize = 0;
        cparams.splitmode = BLOSC_FORWARD_COMPAT_SPLIT;
        cparams.use_dict = 1;
        cparams.filters[BLOSC2_MAX_FILTERS - 1] = BLOSC_SHUFFLE;

        let mut dparams: ffi::blosc2_dparams = std::mem::zeroed();
        dparams.nthreads = 1;
        dparams.typesize = 1;

        let mut storage: ffi::blosc2_storage = std::mem::zeroed();
        storage.contiguous = true;
        storage.urlpath = c_path.as_ptr() as *mut _;
        storage.cparams = &mut cparams;
        storage.dparams = &mut dparams;

        let c_schunk = ffi::blosc2_schunk_new(&mut storage);
        assert!(!c_schunk.is_null(), "C failed to create dictionary frame");
        let rc =
            ffi::blosc2_schunk_append_buffer(c_schunk, data.as_ptr().cast(), data.len() as i32);
        assert_eq!(rc, 1, "C failed to append dictionary frame chunk: {rc}");
        assert_eq!(ffi::blosc2_schunk_free(c_schunk), 0);
    }

    let lazy = Schunk::open_lazy(&path).unwrap();
    assert_eq!(lazy.nchunks(), 1);
    let chunk = lazy.compressed_chunk(0).unwrap();
    let header = ChunkHeader::read(&chunk).unwrap();
    assert!(
        header.use_dict(),
        "C frame fixture must exercise dictionary path"
    );
    let nblocks = header.nblocks();
    let dict_size_pos = header.header_len() + nblocks * 4;
    let dict_size =
        i32::from_le_bytes(chunk[dict_size_pos..dict_size_pos + 4].try_into().unwrap()) as usize;

    let lazy_chunk = lazy.lazy_chunk(0).unwrap();
    let lazy_header = ChunkHeader::read(&lazy_chunk).unwrap();
    assert!(lazy_header.use_dict());
    let lazy_dict_size_pos = lazy_header.header_len() + nblocks * 4;
    assert_eq!(
        i32::from_le_bytes(
            lazy_chunk[lazy_dict_size_pos..lazy_dict_size_pos + 4]
                .try_into()
                .unwrap()
        ) as usize,
        dict_size
    );
    assert_eq!(
        &lazy_chunk[lazy_dict_size_pos + 4..lazy_dict_size_pos + 4 + dict_size],
        &chunk[dict_size_pos + 4..dict_size_pos + 4 + dict_size]
    );

    let restored = lazy.decompress_chunk(0).unwrap();
    assert_eq!(restored, data);

    let mut oversized_dest = vec![0u8; lazy.chunksize];
    let written = decompress_into_with_dparams(&chunk, &mut oversized_dest, &lazy.dparams).unwrap();
    assert_eq!(written, data.len());
    assert_eq!(&oversized_dest[..written], data.as_slice());
}

fn identity_postfilter(params: &mut PostfilterParams<'_>) -> i32 {
    params.output.copy_from_slice(params.input);
    0
}

#[test]
fn test_c_trained_zstd_dictionary_chunk_decodes_without_ffi_fallback() {
    let _b = init_blosc2();
    let data = c_trained_zstd_dict_regression_data();
    let mut cparams: ffi::blosc2_cparams = unsafe { std::mem::zeroed() };
    cparams.compcode = BLOSC_ZSTD;
    cparams.clevel = 5;
    cparams.typesize = 1;
    cparams.nthreads = 1;
    cparams.blocksize = 0;
    cparams.splitmode = BLOSC_FORWARD_COMPAT_SPLIT;
    cparams.use_dict = 1;
    cparams.filters[BLOSC2_MAX_FILTERS - 1] = BLOSC_SHUFFLE;

    let c_chunk = unsafe { c_compress_with_params(&data, cparams) };
    let mut out = vec![0u8; data.len()];
    let dparams = DParams {
        postfilter: Some(identity_postfilter),
        ..Default::default()
    };
    let written = decompress_into_with_dparams(&c_chunk, &mut out, &dparams).unwrap();
    assert_eq!(written, data.len());
    assert_eq!(out, data);
}

#[test]
fn test_zstd_dictionary_regular_fallback_boundary_matches_c() {
    let _b = init_blosc2();

    let data: Vec<u8> = (0..6000u32).map(|i| i as u8).collect();
    let mut cparams: ffi::blosc2_cparams = unsafe { std::mem::zeroed() };
    cparams.compcode = BLOSC_ZSTD;
    cparams.clevel = 5;
    cparams.typesize = 1;
    cparams.nthreads = 1;
    cparams.blocksize = 1;
    cparams.splitmode = BLOSC_NEVER_SPLIT;
    cparams.use_dict = 1;

    let c_chunk = unsafe { c_compress_with_params(&data, cparams) };
    let c_header = ChunkHeader::read(&c_chunk).unwrap();
    assert!(
        !c_header.use_dict(),
        "C bypasses zstd dictionary training when computed sample_size is zero"
    );
    assert_eq!(decompress(&c_chunk).unwrap(), data);

    let rust_params = CParams {
        compcode: BLOSC_ZSTD,
        clevel: 5,
        typesize: 1,
        blocksize: 1,
        splitmode: BLOSC_NEVER_SPLIT,
        filters: [0; BLOSC2_MAX_FILTERS],
        use_dict: true,
        ..Default::default()
    };
    let rust_chunk = compress(&data, &rust_params).unwrap();
    let rust_header = ChunkHeader::read(&rust_chunk).unwrap();
    assert!(
        !rust_header.use_dict(),
        "Rust must mirror C's sample_size zero dictionary fallback"
    );
    let (c_dsize, c_decompressed) = c_decompress_chunk(&rust_chunk, data.len());
    assert_eq!(c_dsize, data.len() as i32);
    assert_eq!(c_decompressed, data);
}

#[test]
fn test_zstd_low_diversity_dict_size_tracks_c_training_shape() {
    let _b = init_blosc2();

    let data = vec![0u8; 200_000];
    let mut cparams: ffi::blosc2_cparams = unsafe { std::mem::zeroed() };
    cparams.compcode = BLOSC_ZSTD;
    cparams.clevel = 5;
    cparams.typesize = 1;
    cparams.nthreads = 1;
    cparams.blocksize = 4096;
    cparams.splitmode = BLOSC_NEVER_SPLIT;
    cparams.use_dict = 1;

    let c_chunk = unsafe { c_compress_with_params(&data, cparams) };
    let c_header = ChunkHeader::read(&c_chunk).unwrap();
    assert!(c_header.use_dict());
    assert!(
        regular_embedded_dict_len(&c_chunk).unwrap() < BLOSC2_MINUSEFULDICT,
        "C's trained zstd dictionary can be smaller than the pre-training useful-size guard"
    );

    let rust_chunk = compress(
        &data,
        &CParams {
            compcode: BLOSC_ZSTD,
            clevel: 5,
            typesize: 1,
            blocksize: 4096,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0; BLOSC2_MAX_FILTERS],
            use_dict: true,
            ..Default::default()
        },
    )
    .unwrap();
    let rust_header = ChunkHeader::read(&rust_chunk).unwrap();
    assert!(rust_header.use_dict());
    let rust_dict = regular_embedded_dict(&rust_chunk).unwrap();
    assert_eq!(&rust_dict[..4], &ZSTD_DICT_MAGIC_LE);
    assert!(zstd_dict_content(rust_dict).len() <= 512);

    let (c_dsize, c_decompressed) = c_decompress_chunk(&rust_chunk, data.len());
    assert_eq!(c_dsize, data.len() as i32);
    assert_eq!(c_decompressed, data);
}

fn vl_test_blocks() -> Vec<Vec<u8>> {
    vec![
        b"red\0".to_vec(),
        b"green-green\0".to_vec(),
        b"blue-blue-blue-blue\0".to_vec(),
    ]
}

fn vl_dict_blocks() -> Vec<Vec<u8>> {
    (0..64)
        .map(|i| {
            format!(
                "{{\"id\":\"en:ingredient-{i:03}\",\"vegan\":\"{}\",\
                 \"vegetarian\":\"{}\",\"percent_estimate\":{:.2},\
                 \"is_in_taxonomy\":{},\"text\":\"INGREDIENT NUMBER {i:03}\",\
                 \"from_palm_oil\":null,\"processing\":null,\"labels\":null,\
                 \"origins\":null,\"quantity\":null,\"quantity_g\":null,\
                 \"ciqual_food_code\":null,\"additives_n\":null,\
                 \"nova_group\":null,\"pnns_groups_1\":null,\
                 \"pnns_groups_2\":null,\
                 \"carbon_footprint_percent_of_known_ingredients\":null}}",
                if i % 3 == 0 { "maybe" } else { "yes" },
                if i % 5 == 0 { "no" } else { "yes" },
                (i % 20) as f64 * 2.5 + 0.1,
                i % 2
            )
            .into_bytes()
        })
        .collect()
}

fn assert_vl_chunk_header(chunk: &[u8], expected_nblocks: usize) {
    let header = ChunkHeader::read(chunk).unwrap();
    assert_eq!(header.version, BLOSC2_VERSION_FORMAT_VL_BLOCKS);
    assert_ne!(header.blosc2_flags2 & BLOSC2_VL_BLOCKS, 0);
    assert_eq!(header.blocksize, expected_nblocks as i32);
    assert_eq!(
        cbuffer_sizes(chunk).unwrap().2,
        expected_nblocks,
        "VL-block chunks store nblocks in the blocksize header field"
    );
}

fn vl_block_starts(chunk: &[u8]) -> Vec<usize> {
    let header = ChunkHeader::read(chunk).unwrap();
    assert!(header.vl_blocks());
    let header_len = header.header_len();
    (0..header.nblocks())
        .map(|idx| {
            i32::from_le_bytes(
                chunk[header_len + idx * 4..header_len + (idx + 1) * 4]
                    .try_into()
                    .unwrap(),
            ) as usize
        })
        .collect()
}

fn vl_embedded_dict_len(chunk: &[u8]) -> Option<usize> {
    let header = ChunkHeader::read(chunk).unwrap();
    if !header.use_dict() {
        return None;
    }
    let dict_size_pos = header.header_len() + header.nblocks() * 4;
    Some(i32::from_le_bytes(chunk[dict_size_pos..dict_size_pos + 4].try_into().unwrap()) as usize)
}

fn vl_embedded_dict(chunk: &[u8]) -> Option<&[u8]> {
    let header = ChunkHeader::read(chunk).unwrap();
    if !header.use_dict() {
        return None;
    }
    let dict_size_pos = header.header_len() + header.nblocks() * 4;
    let dict_start = dict_size_pos + 4;
    let dict_len =
        i32::from_le_bytes(chunk[dict_size_pos..dict_start].try_into().unwrap()) as usize;
    Some(&chunk[dict_start..dict_start + dict_len])
}

fn assert_vl_block_layout_matches_c(chunk: &[u8], blocks: &[Vec<u8>]) {
    let header = ChunkHeader::read(chunk).unwrap();
    let starts = vl_block_starts(chunk);
    assert_eq!(starts.len(), blocks.len());
    assert!(
        starts.windows(2).all(|pair| pair[0] < pair[1]),
        "VL bstarts must be stored in logical block order like C"
    );

    let table_end = header.header_len() + blocks.len() * 4;
    let min_block_start = table_end + vl_embedded_dict_len(chunk).map_or(0, |len| 4 + len);
    assert!(
        starts.first().copied().unwrap_or(0) >= min_block_start,
        "VL blocks must start after bstarts and optional dictionary"
    );

    for (idx, (start, expected)) in starts.iter().copied().zip(blocks).enumerate() {
        let stored_len = i32::from_le_bytes(chunk[start..start + 4].try_into().unwrap());
        assert_eq!(
            stored_len,
            expected.len() as i32,
            "VL block {idx} uncompressed length prefix must match C layout"
        );
        assert_eq!(
            vldecompress_block(chunk, idx).unwrap(),
            *expected,
            "VL block {idx} selective decompression must preserve exact bytes"
        );
    }
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

unsafe fn c_vl_compress_rc(blocks: &[Vec<u8>]) -> i32 {
    let srcs: Vec<*const c_void> = blocks
        .iter()
        .map(|block| block.as_ptr() as *const c_void)
        .collect();
    let sizes: Vec<i32> = blocks.iter().map(|block| block.len() as i32).collect();
    let total: usize = blocks.iter().map(Vec::len).sum();
    let mut compressed =
        vec![0u8; total + BLOSC2_MAX_OVERHEAD + blocks.len() * 64 + BLOSC2_MAXDICTSIZE];

    let mut cparams: ffi::blosc2_cparams = std::mem::zeroed();
    cparams.compcode = BLOSC_LZ4;
    cparams.clevel = 5;
    cparams.typesize = 1;
    cparams.nthreads = 1;
    cparams.splitmode = BLOSC_FORWARD_COMPAT_SPLIT;
    cparams.filters[BLOSC2_MAX_FILTERS - 1] = BLOSC_SHUFFLE;

    let cctx = ffi::blosc2_create_cctx(cparams);
    assert!(!cctx.is_null());
    let rc = ffi::blosc2_vlcompress_ctx(
        cctx,
        srcs.as_ptr(),
        sizes.as_ptr(),
        blocks.len() as i32,
        compressed.as_mut_ptr() as *mut c_void,
        compressed.len() as i32,
    );
    ffi::blosc2_free_ctx(cctx);
    rc
}

fn rust_vl_compress_rc(blocks: &[&[u8]], cparams: &CParams) -> i32 {
    let cctx = blosc2_create_cctx(cparams.clone()).unwrap();
    let total: usize = blocks.iter().map(|block| block.len()).sum();
    let mut compressed =
        vec![0u8; total + BLOSC2_MAX_OVERHEAD + blocks.len() * 64 + BLOSC2_MAXDICTSIZE];
    let destsize = compressed.len() as i32;
    rust_vlcompress_ctx(&cctx, blocks, &mut compressed, destsize)
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
    assert!(nblocks >= 0);
    assert!(nblocks as usize <= maxblocks);

    let mut blocks = Vec::with_capacity(nblocks as usize);
    for (ptr, size) in dests.into_iter().zip(sizes).take(nblocks as usize) {
        assert!(!ptr.is_null());
        assert!(size > 0);
        let block = std::slice::from_raw_parts(ptr as *const u8, size as usize).to_vec();
        free(ptr);
        blocks.push(block);
    }
    blocks
}

unsafe fn c_vl_decompress_rc(chunk: &[u8], maxblocks: usize) -> i32 {
    let mut dparams: ffi::blosc2_dparams = std::mem::zeroed();
    dparams.nthreads = 1;
    let dctx = ffi::blosc2_create_dctx(dparams);
    assert!(!dctx.is_null());

    let mut dests = vec![std::ptr::null_mut::<c_void>(); maxblocks];
    let mut sizes = vec![0i32; maxblocks];
    let rc = ffi::blosc2_vldecompress_ctx(
        dctx,
        chunk.as_ptr() as *const c_void,
        chunk.len() as i32,
        dests.as_mut_ptr(),
        sizes.as_mut_ptr(),
        maxblocks as i32,
    );
    ffi::blosc2_free_ctx(dctx);
    for ptr in dests {
        if !ptr.is_null() {
            free(ptr);
        }
    }
    rc
}

unsafe fn c_vl_decompress_block(chunk: &[u8], block: i32) -> Vec<u8> {
    let mut dparams: ffi::blosc2_dparams = std::mem::zeroed();
    dparams.nthreads = 1;
    let dctx = ffi::blosc2_create_dctx(dparams);
    assert!(!dctx.is_null());

    let mut block_ptr = std::ptr::null_mut::<u8>();
    let mut block_size = 0i32;
    let rc = ffi::blosc2_vldecompress_block_ctx(
        dctx,
        chunk.as_ptr() as *const c_void,
        chunk.len() as i32,
        block,
        &mut block_ptr,
        &mut block_size,
    );
    ffi::blosc2_free_ctx(dctx);
    assert!(rc >= 0, "C VL-block selective decompression failed");
    assert_eq!(rc, block_size);
    assert!(!block_ptr.is_null());
    let block = std::slice::from_raw_parts(block_ptr, block_size as usize).to_vec();
    free(block_ptr as *mut c_void);
    block
}

unsafe fn c_vl_decompress_block_rc(chunk: &[u8], block: i32) -> i32 {
    let mut dparams: ffi::blosc2_dparams = std::mem::zeroed();
    dparams.nthreads = 1;
    let dctx = ffi::blosc2_create_dctx(dparams);
    assert!(!dctx.is_null());

    let mut block_ptr = std::ptr::null_mut::<u8>();
    let mut block_size = 0i32;
    let rc = ffi::blosc2_vldecompress_block_ctx(
        dctx,
        chunk.as_ptr() as *const c_void,
        chunk.len() as i32,
        block,
        &mut block_ptr,
        &mut block_size,
    );
    ffi::blosc2_free_ctx(dctx);
    if !block_ptr.is_null() {
        free(block_ptr as *mut c_void);
    }
    rc
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
        assert_vl_chunk_header(&c_chunk, blocks.len());
        assert_vl_block_layout_matches_c(&c_chunk, &blocks);
        assert_eq!(vlchunk_get_nblocks(&c_chunk).unwrap(), blocks.len());
        assert_eq!(decompress(&c_chunk).unwrap(), expected_concat);
        assert_eq!(vldecompress(&c_chunk).unwrap(), blocks);
        assert_eq!(vldecompress_block(&c_chunk, 2).unwrap(), blocks[2]);
        assert_eq!(vldecompress_block(&c_chunk, 0).unwrap().last(), Some(&0));
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
        assert_vl_chunk_header(&rust_chunk, blocks.len());
        assert_vl_block_layout_matches_c(&rust_chunk, &blocks);

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
    assert_vl_chunk_header(&c_chunk, blocks.len());
    assert_vl_block_layout_matches_c(&c_chunk, &blocks);
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
    assert_vl_chunk_header(&rust_chunk, blocks.len());
    assert_vl_block_layout_matches_c(&rust_chunk, &blocks);
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
fn test_vlblocks_empty_inputs_are_rejected_like_c() {
    let _b = init_blosc2();

    let cparams = CParams {
        compcode: BLOSC_LZ4,
        clevel: 5,
        typesize: 1,
        nthreads: 1,
        splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
        filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
        ..Default::default()
    };

    let no_blocks: Vec<Vec<u8>> = Vec::new();
    let no_block_refs: Vec<&[u8]> = Vec::new();
    let c_no_blocks_rc = unsafe { c_vl_compress_rc(&no_blocks) };
    let rust_no_blocks_rc = rust_vl_compress_rc(&no_block_refs, &cparams);
    assert_eq!(c_no_blocks_rc, BLOSC2_ERROR_INVALID_PARAM);
    assert_eq!(rust_no_blocks_rc, c_no_blocks_rc);
    assert!(vlcompress(&no_block_refs, &cparams).is_err());

    let blocks = vec![b"first".to_vec(), Vec::new(), b"last".to_vec()];
    let block_refs: Vec<&[u8]> = blocks.iter().map(Vec::as_slice).collect();
    let c_empty_block_rc = unsafe { c_vl_compress_rc(&blocks) };
    let rust_empty_block_rc = rust_vl_compress_rc(&block_refs, &cparams);
    assert_eq!(c_empty_block_rc, BLOSC2_ERROR_INVALID_PARAM);
    assert_eq!(rust_empty_block_rc, c_empty_block_rc);
    assert!(vlcompress(&block_refs, &cparams).is_err());
}

#[test]
fn test_zstd_dictionary_vlblocks_cross_compat() {
    let _b = init_blosc2();
    let blocks = vl_dict_blocks();
    let block_refs: Vec<&[u8]> = blocks.iter().map(Vec::as_slice).collect();
    let expected_concat: Vec<u8> = blocks.iter().flatten().copied().collect();

    let c_chunk = unsafe { c_vl_compress(&blocks, BLOSC_ZSTD, 1, 4, true) };
    assert_vl_chunk_header(&c_chunk, blocks.len());
    assert_vl_block_layout_matches_c(&c_chunk, &blocks);
    assert_ne!(
        c_chunk[BLOSC2_CHUNK_BLOSC2_FLAGS] & BLOSC2_USEDICT,
        0,
        "C VL dictionary chunk should set BLOSC2_USEDICT"
    );
    assert_eq!(vlchunk_get_nblocks(&c_chunk).unwrap(), blocks.len());
    assert_eq!(vldecompress(&c_chunk).unwrap(), blocks);
    assert_eq!(decompress(&c_chunk).unwrap(), expected_concat);
    for &idx in &[0usize, blocks.len() / 2, blocks.len() - 1] {
        assert_eq!(vldecompress_block(&c_chunk, idx).unwrap(), blocks[idx]);
    }

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
    assert_vl_chunk_header(&rust_chunk, blocks.len());
    assert_vl_block_layout_matches_c(&rust_chunk, &blocks);
    assert_ne!(
        rust_chunk[BLOSC2_CHUNK_BLOSC2_FLAGS] & BLOSC2_USEDICT,
        0,
        "Rust VL dictionary chunk should set BLOSC2_USEDICT"
    );
    let c_blocks = unsafe { c_vl_decompress(&rust_chunk, blocks.len()) };
    assert_eq!(c_blocks, blocks);
    for &idx in &[0usize, blocks.len() / 2, blocks.len() - 1] {
        let c_block = unsafe { c_vl_decompress_block(&rust_chunk, idx as i32) };
        assert_eq!(c_block, blocks[idx]);
    }

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
fn test_zstd_low_diversity_vl_dict_size_tracks_c_training_shape() {
    let _b = init_blosc2();
    let blocks: Vec<Vec<u8>> = (0..64).map(|_| vec![0u8; 512]).collect();
    let block_refs: Vec<&[u8]> = blocks.iter().map(Vec::as_slice).collect();
    let expected_concat: Vec<u8> = blocks.iter().flatten().copied().collect();

    let c_chunk = unsafe { c_vl_compress(&blocks, BLOSC_ZSTD, 1, 1, true) };
    assert_vl_chunk_header(&c_chunk, blocks.len());
    assert_ne!(c_chunk[BLOSC2_CHUNK_BLOSC2_FLAGS] & BLOSC2_USEDICT, 0);
    assert!(
        vl_embedded_dict_len(&c_chunk).unwrap() < BLOSC2_MINUSEFULDICT,
        "C's trained zstd VL dictionary can be smaller than the pre-training useful-size guard"
    );

    let rust_chunk = vlcompress(
        &block_refs,
        &CParams {
            compcode: BLOSC_ZSTD,
            clevel: 5,
            typesize: 1,
            nthreads: 1,
            splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            use_dict: true,
            ..Default::default()
        },
    )
    .unwrap();
    assert_vl_chunk_header(&rust_chunk, blocks.len());
    assert_ne!(rust_chunk[BLOSC2_CHUNK_BLOSC2_FLAGS] & BLOSC2_USEDICT, 0);
    let rust_dict = vl_embedded_dict(&rust_chunk).unwrap();
    assert_eq!(&rust_dict[..4], &ZSTD_DICT_MAGIC_LE);
    assert!(zstd_dict_content(rust_dict).len() <= 512);
    assert_eq!(
        unsafe { c_vl_decompress(&rust_chunk, blocks.len()) },
        blocks
    );

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
fn test_lz4_dictionary_vlblocks_cross_compat() {
    let _b = init_blosc2();
    let blocks = vl_dict_blocks();
    let block_refs: Vec<&[u8]> = blocks.iter().map(Vec::as_slice).collect();
    let expected_concat: Vec<u8> = blocks.iter().flatten().copied().collect();

    let c_chunk = unsafe { c_vl_compress(&blocks, BLOSC_LZ4, 1, 4, true) };
    assert_vl_chunk_header(&c_chunk, blocks.len());
    assert_vl_block_layout_matches_c(&c_chunk, &blocks);
    assert_ne!(
        c_chunk[BLOSC2_CHUNK_BLOSC2_FLAGS] & BLOSC2_USEDICT,
        0,
        "C LZ4 VL dictionary chunk should set BLOSC2_USEDICT"
    );
    assert!(vl_embedded_dict_len(&c_chunk).unwrap() >= BLOSC2_MINUSEFULDICT);
    assert_eq!(vldecompress(&c_chunk).unwrap(), blocks);
    assert_eq!(decompress(&c_chunk).unwrap(), expected_concat);

    let cparams = CParams {
        compcode: BLOSC_LZ4,
        clevel: 5,
        typesize: 1,
        nthreads: 4,
        splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
        filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
        use_dict: true,
        ..Default::default()
    };
    let rust_chunk = vlcompress(&block_refs, &cparams).unwrap();
    assert_vl_chunk_header(&rust_chunk, blocks.len());
    assert_vl_block_layout_matches_c(&rust_chunk, &blocks);
    assert_ne!(
        rust_chunk[BLOSC2_CHUNK_BLOSC2_FLAGS] & BLOSC2_USEDICT,
        0,
        "Rust LZ4 VL dictionary chunk should set BLOSC2_USEDICT"
    );
    assert!(vl_embedded_dict_len(&rust_chunk).unwrap() >= BLOSC2_MINUSEFULDICT);
    assert_eq!(
        unsafe { c_vl_decompress(&rust_chunk, blocks.len()) },
        blocks
    );
    assert_eq!(
        unsafe { c_vl_decompress_block(&rust_chunk, blocks.len() as i32 - 1) },
        blocks[blocks.len() - 1]
    );
}

#[test]
fn test_vl_dictionary_fallback_small_payload_matches_c() {
    let _b = init_blosc2();
    let blocks = vec![b"tiny-vl-payload\0".to_vec()];
    let block_refs: Vec<&[u8]> = blocks.iter().map(Vec::as_slice).collect();

    let c_chunk = unsafe { c_vl_compress(&blocks, BLOSC_ZSTD, 1, 1, true) };
    assert_vl_chunk_header(&c_chunk, blocks.len());
    assert_eq!(
        c_chunk[BLOSC2_CHUNK_BLOSC2_FLAGS] & BLOSC2_USEDICT,
        0,
        "C falls back without BLOSC2_USEDICT when VL dictionary training is not useful"
    );
    assert_eq!(vldecompress(&c_chunk).unwrap(), blocks);

    let cparams = CParams {
        compcode: BLOSC_ZSTD,
        clevel: 5,
        typesize: 1,
        nthreads: 1,
        use_dict: true,
        ..Default::default()
    };
    let rust_chunk = vlcompress(&block_refs, &cparams).unwrap();
    assert_vl_chunk_header(&rust_chunk, blocks.len());
    assert_eq!(
        rust_chunk[BLOSC2_CHUNK_BLOSC2_FLAGS] & BLOSC2_USEDICT,
        0,
        "Rust should mirror C's VL dictionary fallback flag"
    );
    assert_eq!(
        unsafe { c_vl_decompress(&rust_chunk, blocks.len()) },
        blocks
    );
}

#[test]
fn test_vl_error_codes_match_c_for_non_vl_and_bad_indices() {
    let _b = init_blosc2();
    let data = b"regular chunk, not vl";
    let regular = compress(data, &CParams::default()).unwrap();
    let blocks = vl_test_blocks();
    let block_refs: Vec<&[u8]> = blocks.iter().map(Vec::as_slice).collect();
    let vl_chunk = vlcompress(&block_refs, &CParams::default()).unwrap();
    let dctx = blosc2_create_dctx(DParams::default()).unwrap();

    let mut c_nblocks = -1i32;
    let c_count_rc = unsafe {
        ffi::blosc2_vlchunk_get_nblocks(
            regular.as_ptr() as *const c_void,
            regular.len() as i32,
            &mut c_nblocks,
        )
    };
    assert_eq!(c_count_rc, BLOSC2_ERROR_INVALID_PARAM);
    assert_eq!(
        rust_vlchunk_get_nblocks_c(&regular, regular.len() as i32),
        (c_count_rc, 0)
    );

    let c_non_vl_rc = unsafe { c_vl_decompress_rc(&regular, blocks.len()) };
    let mut rust_blocks = vec![Vec::new(); blocks.len()];
    let mut rust_sizes = vec![0; blocks.len()];
    let rust_non_vl_rc = rust_vldecompress_ctx_c(
        &dctx,
        &regular,
        regular.len() as i32,
        &mut rust_blocks,
        &mut rust_sizes,
        blocks.len() as i32,
    );
    assert_eq!(rust_non_vl_rc, c_non_vl_rc);
    assert_eq!(c_non_vl_rc, BLOSC2_ERROR_INVALID_PARAM);

    let c_too_few_rc = unsafe { c_vl_decompress_rc(&vl_chunk, blocks.len() - 1) };
    let rust_too_few_rc = rust_vldecompress_ctx_c(
        &dctx,
        &vl_chunk,
        vl_chunk.len() as i32,
        &mut rust_blocks[..blocks.len() - 1],
        &mut rust_sizes[..blocks.len() - 1],
        blocks.len() as i32 - 1,
    );
    assert_eq!(rust_too_few_rc, c_too_few_rc);
    assert_eq!(c_too_few_rc, BLOSC2_ERROR_INVALID_PARAM);

    let mut rust_block = vec![0u8; blocks[0].len()];
    let rust_block_len = rust_block.len() as i32;
    let c_negative_block_rc = unsafe { c_vl_decompress_block_rc(&vl_chunk, -1) };
    let rust_negative_block_rc = rust_vldecompress_block_ctx_c(
        &dctx,
        &vl_chunk,
        vl_chunk.len() as i32,
        -1,
        &mut rust_block,
        rust_block_len,
    );
    assert_eq!(rust_negative_block_rc, c_negative_block_rc);
    assert_eq!(c_negative_block_rc, BLOSC2_ERROR_INVALID_PARAM);

    let c_oob_block_rc = unsafe { c_vl_decompress_block_rc(&vl_chunk, blocks.len() as i32) };
    let rust_oob_block_rc = rust_vldecompress_block_ctx_c(
        &dctx,
        &vl_chunk,
        vl_chunk.len() as i32,
        blocks.len() as i32,
        &mut rust_block,
        rust_block_len,
    );
    assert_eq!(rust_oob_block_rc, c_oob_block_rc);
    assert_eq!(c_oob_block_rc, BLOSC2_ERROR_INVALID_PARAM);
}

#[test]
fn test_vl_decompress_block_ctx_matches_c() {
    let _b = init_blosc2();
    let blocks = vl_test_blocks();
    let block_refs: Vec<&[u8]> = blocks.iter().map(Vec::as_slice).collect();
    let vl_chunk = vlcompress(&block_refs, &CParams::default()).unwrap();
    let dctx = blosc2_create_dctx(DParams::default()).unwrap();

    let c_full = unsafe { c_vl_decompress_block(&vl_chunk, 1) };
    let (rust_rc, rust_full) =
        rust_vldecompress_block_ctx(&dctx, &vl_chunk, vl_chunk.len() as i32, 1usize);
    assert_eq!(rust_rc, c_full.len() as i32);
    assert_eq!(rust_full.unwrap(), c_full);
    assert_eq!(c_full, blocks[1]);
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
fn test_b2nd_rust_special_constructors_c_reads() {
    let _b = init_blosc2();

    let meta = B2ndMeta::new(vec![3, 5], vec![2, 3], vec![2, 2], "|u1", 0).unwrap();
    let cparams = CParams {
        compcode: BLOSC_LZ4,
        clevel: 5,
        typesize: 1,
        splitmode: BLOSC_NEVER_SPLIT,
        filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
        ..Default::default()
    };

    let empty = B2ndArray::empty(meta.clone(), cparams.clone(), Default::default()).unwrap();
    assert_eq!(
        ChunkHeader::read(empty.schunk.compressed_chunk(0).unwrap())
            .unwrap()
            .special_type(),
        BLOSC2_SPECIAL_ZERO
    );
    let full = B2ndArray::full(meta, &[9], cparams, Default::default()).unwrap();
    assert_eq!(
        ChunkHeader::read(full.schunk.compressed_chunk(0).unwrap())
            .unwrap()
            .special_type(),
        BLOSC2_SPECIAL_VALUE
    );

    for (array, expected) in [(empty, vec![0u8; 15]), (full, vec![9u8; 15])] {
        let mut frame = array.to_frame();
        unsafe {
            let mut c_array: *mut ffi::b2nd_array_t = std::ptr::null_mut();
            let rc =
                ffi::b2nd_from_cframe(frame.as_mut_ptr(), frame.len() as i64, true, &mut c_array);
            assert_eq!(rc, 0);
            assert!(!c_array.is_null());

            let mut c_buffer = vec![0u8; expected.len()];
            let rc =
                ffi::b2nd_to_cbuffer(c_array, c_buffer.as_mut_ptr().cast(), c_buffer.len() as i64);
            assert_eq!(rc, 0);
            assert_eq!(c_buffer, expected);
            assert_eq!(ffi::b2nd_free(c_array), 0);
        }
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
fn test_ndlz_b2nd_rust_frame_c_reads() {
    let _b = init_blosc2();

    for (shape, chunkshape, blockshape, dtype, typesize, compcode_meta, data) in [
        (
            [32i64, 18],
            [17i32, 16],
            [8i32, 9],
            "<f4",
            4,
            4,
            ndlz_f32_fixture_data([32, 18]),
        ),
        (
            [128i64, 111],
            [32i32, 11],
            [16i32, 7],
            "<f8",
            8,
            4,
            ndlz_f64_same_cells_fixture_data([128, 111]),
        ),
        (
            [128i64, 111],
            [48i32, 32],
            [14i32, 18],
            "<f8",
            8,
            8,
            ndlz_f64_some_matches_fixture_data([128, 111]),
        ),
    ] {
        let meta = B2ndMeta::new(
            shape.to_vec(),
            chunkshape.to_vec(),
            blockshape.to_vec(),
            dtype,
            0,
        )
        .unwrap();
        let cparams = CParams {
            compcode: BLOSC_CODEC_NDLZ,
            compcode_meta,
            clevel: 5,
            typesize,
            splitmode: BLOSC_ALWAYS_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            nthreads: 1,
            ..Default::default()
        };
        let array = B2ndArray::from_cbuffer(meta, &data, cparams, Default::default()).unwrap();
        assert_eq!(array.to_cbuffer().unwrap(), data);
        let mut frame = array.to_frame();

        unsafe {
            let mut c_array: *mut ffi::b2nd_array_t = std::ptr::null_mut();
            let rc =
                ffi::b2nd_from_cframe(frame.as_mut_ptr(), frame.len() as i64, true, &mut c_array);
            assert_eq!(rc, 0);
            assert!(!c_array.is_null());

            let mut c_buffer = vec![0u8; data.len()];
            let rc =
                ffi::b2nd_to_cbuffer(c_array, c_buffer.as_mut_ptr().cast(), c_buffer.len() as i64);
            assert_eq!(rc, 0);
            assert_eq!(c_buffer, data);
            assert_eq!(ffi::b2nd_free(c_array), 0);
        }
    }
}

#[test]
fn test_ndlz_b2nd_c_frame_rust_reads() {
    let _b = init_blosc2();

    for (shape, chunkshape, blockshape, dtype, typesize, compcode_meta, data) in [
        (
            [32i64, 18],
            [17i32, 16],
            [8i32, 9],
            "<f4",
            4,
            4,
            ndlz_f32_fixture_data([32, 18]),
        ),
        (
            [128i64, 111],
            [32i32, 11],
            [16i32, 7],
            "<f8",
            8,
            4,
            ndlz_f64_same_cells_fixture_data([128, 111]),
        ),
        (
            [128i64, 111],
            [48i32, 32],
            [14i32, 18],
            "<f8",
            8,
            8,
            ndlz_f64_some_matches_fixture_data([128, 111]),
        ),
    ] {
        let dtype_c = CString::new(dtype).unwrap();

        unsafe {
            let mut cparams: ffi::blosc2_cparams = std::mem::zeroed();
            cparams.compcode = BLOSC_CODEC_NDLZ;
            cparams.compcode_meta = compcode_meta;
            cparams.clevel = 5;
            cparams.typesize = typesize;
            cparams.nthreads = 1;
            cparams.splitmode = BLOSC_ALWAYS_SPLIT;
            cparams.filters[BLOSC2_MAX_FILTERS - 1] = BLOSC_SHUFFLE;
            let mut dparams: ffi::blosc2_dparams = std::mem::zeroed();
            dparams.nthreads = 1;
            dparams.typesize = typesize;
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
                dtype_c.as_ptr(),
                0,
                std::ptr::null(),
                0,
            );
            assert!(!ctx.is_null());

            let mut c_array: *mut ffi::b2nd_array_t = std::ptr::null_mut();
            let rc =
                ffi::b2nd_from_cbuffer(ctx, &mut c_array, data.as_ptr().cast(), data.len() as i64);
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
            assert_eq!(rust_array.meta.dtype, dtype);
            assert_eq!(rust_array.to_cbuffer().unwrap(), data);

            if needs_free {
                free(cframe.cast());
            }
            assert_eq!(ffi::b2nd_free(c_array), 0);
            assert_eq!(ffi::b2nd_free_ctx(ctx), 0);
        }
    }
}

#[test]
fn test_b2nd_c_rust_mutation_and_selection_parity() {
    let _b = init_blosc2();

    let chunkshape = [3, 4];
    let blockshape = [2, 2];
    let mut shape = vec![4usize, 5];
    let data: Vec<u8> = (0..20).collect();
    let c_array = CArray::from_u8_cbuffer(&[4, 5], &chunkshape, &blockshape, &data);
    let mut rust_array = b2nd_u8_array(&[4, 5], &chunkshape, &blockshape, &data);
    assert_eq!(
        c_array.to_cbuffer(data.len()),
        rust_array.to_cbuffer().unwrap()
    );

    let appended: Vec<u8> = (100..110).collect();
    unsafe {
        assert_eq!(
            ffi::b2nd_append(
                c_array.array,
                appended.as_ptr().cast(),
                appended.len() as i64,
                0,
            ),
            0
        );
    }
    rust_array.append(0, &[2, 5], &appended).unwrap();
    let mut expected = [data.as_slice(), appended.as_slice()].concat();
    shape[0] += 2;
    assert_eq!(c_array.to_cbuffer(expected.len()), expected);
    assert_eq!(rust_array.to_cbuffer().unwrap(), expected);

    let insert_shape = [18i64, 6];
    let insert_chunkshape = [6i32, 6];
    let insert_blockshape = [3i32, 3];
    let insert_data = vec![1; 18 * 6];
    let c_insert = CArray::from_u8_cbuffer(
        &insert_shape,
        &insert_chunkshape,
        &insert_blockshape,
        &insert_data,
    );
    let mut rust_insert = b2nd_u8_array(
        &insert_shape,
        &insert_chunkshape,
        &insert_blockshape,
        &insert_data,
    );
    let inserted: Vec<u8> = (0..216).map(|i| (150u16 + i as u16) as u8).collect();
    unsafe {
        assert_eq!(
            ffi::b2nd_insert(
                c_insert.array,
                inserted.as_ptr().cast(),
                inserted.len() as i64,
                1,
                0,
            ),
            0
        );
    }
    rust_insert.insert(1, 0, &[18, 12], &inserted).unwrap();
    let expected_insert = b2nd_insert_expected(&insert_data, &[18, 6], 1, 0, 12, &inserted);
    assert_eq!(c_insert.to_cbuffer(expected_insert.len()), expected_insert);
    assert_eq!(rust_insert.to_cbuffer().unwrap(), expected_insert);

    let delete_shape = [18i64, 12];
    let delete_chunkshape = [6i32, 6];
    let delete_blockshape = [3i32, 3];
    let delete_data: Vec<u8> = (0..216).map(|i| i as u8).collect();
    let c_delete = CArray::from_u8_cbuffer(
        &delete_shape,
        &delete_chunkshape,
        &delete_blockshape,
        &delete_data,
    );
    let mut rust_delete = b2nd_u8_array(
        &delete_shape,
        &delete_chunkshape,
        &delete_blockshape,
        &delete_data,
    );
    unsafe {
        assert_eq!(ffi::b2nd_delete(c_delete.array, 1, 0, 6), 0);
    }
    rust_delete.delete(1, 0, 6).unwrap();
    let expected_delete = b2nd_delete_expected(&delete_data, &[18, 12], 1, 0, 6);
    assert_eq!(c_delete.to_cbuffer(expected_delete.len()), expected_delete);
    assert_eq!(rust_delete.to_cbuffer().unwrap(), expected_delete);

    let resize_shape = [5i64];
    let resize_data: Vec<u8> = (0..5).collect();
    let c_resize = CArray::from_u8_cbuffer(&resize_shape, &[3], &[2], &resize_data);
    let mut rust_resize = b2nd_u8_array(&resize_shape, &[3], &[2], &resize_data);
    let new_shape = [10i64];
    unsafe {
        assert_eq!(
            ffi::b2nd_resize(c_resize.array, new_shape.as_ptr(), std::ptr::null()),
            0
        );
    }
    rust_resize.resize_at(new_shape.to_vec(), None).unwrap();
    expected = resize_data;
    expected.resize(10, 0);
    assert_eq!(c_resize.to_cbuffer(expected.len()), expected);
    assert_eq!(rust_resize.to_cbuffer().unwrap(), expected);

    shape = vec![5, 6];
    expected = (0..30).collect();
    let c_select = CArray::from_u8_cbuffer(&[5, 6], &[3, 4], &[2, 2], &expected);
    let rust_select = b2nd_u8_array(&[5, 6], &[3, 4], &[2, 2], &expected);
    assert_eq!(c_select.to_cbuffer(expected.len()), expected);
    assert_eq!(rust_select.to_cbuffer().unwrap(), expected);

    let slice_start = [1i64, 1];
    let slice_stop = [4i64, 5];
    let mut c_slice_buffer = vec![0; 12];
    unsafe {
        assert_eq!(
            ffi::b2nd_get_slice_cbuffer(
                c_select.array,
                slice_start.as_ptr(),
                slice_stop.as_ptr(),
                c_slice_buffer.as_mut_ptr().cast(),
                [3i64, 4].as_ptr(),
                c_slice_buffer.len() as i64,
            ),
            0
        );
    }
    let rust_slice_buffer = rust_select
        .get_slice_cbuffer(&slice_start, &slice_stop, &[3, 4])
        .unwrap();
    let mut expected_slice = Vec::new();
    for row in 1..4 {
        for col in 1..5 {
            expected_slice.push(expected[row * shape[1] + col]);
        }
    }
    assert_eq!(c_slice_buffer, expected_slice);
    assert_eq!(rust_slice_buffer, expected_slice);

    let mut selection_axis0 = [0i64, 2, 4];
    let mut selection_axis1 = [1i64, 3];
    let mut selection = [selection_axis0.as_mut_ptr(), selection_axis1.as_mut_ptr()];
    let mut selection_size = [3i64, 2];
    let mut selection_buffershape = [3i64, 2];
    let mut c_selected = vec![0; 6];
    unsafe {
        assert_eq!(
            ffi::b2nd_get_orthogonal_selection(
                c_select.array,
                selection.as_mut_ptr(),
                selection_size.as_mut_ptr(),
                c_selected.as_mut_ptr().cast(),
                selection_buffershape.as_mut_ptr(),
                c_selected.len() as i64,
            ),
            0
        );
    }
    let rust_selected = rust_select
        .get_orthogonal_selection(&[selection_axis0.to_vec(), selection_axis1.to_vec()])
        .unwrap();
    let mut expected_selected = Vec::new();
    for row in selection_axis0 {
        for col in selection_axis1 {
            expected_selected.push(expected[row as usize * shape[1] + col as usize]);
        }
    }
    assert_eq!(c_selected, expected_selected);
    assert_eq!(rust_selected, expected_selected);

    let singleton_shape = [1i64, 6];
    let singleton_data: Vec<u8> = (200..206).collect();
    let c_singleton = CArray::from_u8_cbuffer(&singleton_shape, &[1, 3], &[1, 1], &singleton_data);
    let rust_singleton = b2nd_u8_array(&singleton_shape, &[1, 3], &[1, 1], &singleton_data);
    let mut c_squeezed: *mut ffi::b2nd_array_t = std::ptr::null_mut();
    unsafe {
        assert_eq!(ffi::b2nd_squeeze(c_singleton.array, &mut c_squeezed), 0);
        assert!(!c_squeezed.is_null());
        let rc = ffi::b2nd_to_cbuffer(
            c_squeezed,
            c_slice_buffer.as_mut_ptr().cast(),
            singleton_data.len() as i64,
        );
        assert_eq!(rc, 0);
        assert_eq!(
            &c_slice_buffer[..singleton_data.len()],
            singleton_data.as_slice()
        );
        assert_eq!(ffi::b2nd_free(c_squeezed), 0);
    }
    assert_eq!(
        rust_singleton.squeeze_view().unwrap().to_cbuffer().unwrap(),
        singleton_data
    );

    let left_data: Vec<u8> = (30..40).collect();
    let c_left = CArray::from_u8_cbuffer(&[2, 5], &chunkshape, &blockshape, &left_data);
    let rust_left = b2nd_u8_array(&[2, 5], &chunkshape, &blockshape, &left_data);
    let right_data: Vec<u8> = (50..60).collect();
    let c_right = CArray::from_u8_cbuffer(&[2, 5], &chunkshape, &blockshape, &right_data);
    let rust_right = b2nd_u8_array(&[2, 5], &chunkshape, &blockshape, &right_data);
    let concat_shape = [4i64, 5];
    let concat_ctx_data = vec![0; 20];
    let c_concat_ctx =
        CArray::from_u8_cbuffer(&concat_shape, &chunkshape, &blockshape, &concat_ctx_data);
    let mut c_concat: *mut ffi::b2nd_array_t = std::ptr::null_mut();
    unsafe {
        assert_eq!(
            ffi::b2nd_concatenate(
                c_concat_ctx.ctx,
                c_left.array,
                c_right.array,
                0,
                true,
                &mut c_concat,
            ),
            0
        );
        assert!(!c_concat.is_null());
    }
    let rust_concat = rust_left
        .concatenate_with_meta(
            &rust_right,
            0,
            b2nd_u8_meta(&concat_shape, &chunkshape, &blockshape),
            b2nd_u8_cparams(),
            DParams::default(),
        )
        .unwrap();
    let c_concat_buffer = {
        let c_concat_array = CArray {
            ctx: std::ptr::null_mut(),
            array: c_concat,
        };
        c_concat_array.to_cbuffer(20)
    };
    assert_eq!(
        c_concat_buffer,
        [left_data.as_slice(), right_data.as_slice()].concat()
    );
    assert_eq!(rust_concat.to_cbuffer().unwrap(), c_concat_buffer);
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
fn test_rust_vlblocks_frame_c_reads() {
    let _b = init_blosc2();

    let cparams = CParams {
        compcode: BLOSC_LZ4,
        clevel: 5,
        typesize: 1,
        nthreads: 4,
        splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
        filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
        ..Default::default()
    };
    let mut schunk = Schunk::new(cparams, Default::default());
    let blocks = vl_test_blocks();
    let block_refs: Vec<&[u8]> = blocks.iter().map(Vec::as_slice).collect();
    let expected_concat: Vec<u8> = blocks.iter().flatten().copied().collect();
    schunk.append_vlblocks(&block_refs).unwrap();
    let mut frame = schunk.to_frame();
    assert_ne!(
        frame[25] & FRAME_VL_BLOCKS,
        0,
        "Rust VL-block schunk frame should set FRAME_VL_BLOCKS"
    );

    let c_schunk =
        unsafe { ffi::blosc2_schunk_from_buffer(frame.as_mut_ptr(), frame.len() as i64, true) };
    assert!(
        !c_schunk.is_null(),
        "C failed to open Rust-produced VL-block frame"
    );
    unsafe {
        assert_eq!((*c_schunk).nchunks, 1);
        assert_eq!((*c_schunk).chunksize, expected_concat.len() as i32);
        assert_ne!((*c_schunk).flags2 & BLOSC2_VL_BLOCKS, 0);
    }

    let mut c_block_ptr = std::ptr::null_mut::<u8>();
    let mut c_block_size = 0i32;
    let c_block_rc = unsafe {
        ffi::blosc2_schunk_get_vlblock(c_schunk, 0, 1, &mut c_block_ptr, &mut c_block_size)
    };
    assert_eq!(c_block_rc, blocks[1].len() as i32);
    assert_eq!(c_block_size, blocks[1].len() as i32);
    assert!(!c_block_ptr.is_null());
    let c_block = unsafe { std::slice::from_raw_parts(c_block_ptr, c_block_size as usize) };
    assert_eq!(c_block, blocks[1]);
    unsafe {
        free(c_block_ptr as *mut c_void);
    }

    let mut c_decompressed = vec![0u8; expected_concat.len()];
    let c_dsize = unsafe {
        ffi::blosc2_schunk_decompress_chunk(
            c_schunk,
            0,
            c_decompressed.as_mut_ptr() as *mut c_void,
            c_decompressed.len() as i32,
        )
    };
    assert_eq!(c_dsize, c_decompressed.len() as i32);
    assert_eq!(c_decompressed, expected_concat);

    let rc = unsafe { ffi::blosc2_schunk_free(c_schunk) };
    assert_eq!(rc, 0);
}

#[test]
fn test_c_vlblocks_frame_rust_reads() {
    let _b = init_blosc2();

    let blocks = vl_test_blocks();
    let c_chunk = unsafe { c_vl_compress(&blocks, BLOSC_LZ4, 1, 1, false) };

    unsafe {
        let mut cparams: ffi::blosc2_cparams = std::mem::zeroed();
        cparams.compcode = BLOSC_LZ4;
        cparams.clevel = 5;
        cparams.typesize = 1;
        cparams.nthreads = 1;
        cparams.splitmode = BLOSC_FORWARD_COMPAT_SPLIT;
        cparams.filters[BLOSC2_MAX_FILTERS - 1] = BLOSC_SHUFFLE;
        let mut dparams: ffi::blosc2_dparams = std::mem::zeroed();
        dparams.nthreads = 1;
        dparams.typesize = 1;
        let mut storage: ffi::blosc2_storage = std::mem::zeroed();
        storage.contiguous = true;
        storage.cparams = &mut cparams;
        storage.dparams = &mut dparams;

        let c_schunk = ffi::blosc2_schunk_new(&mut storage);
        assert!(
            !c_schunk.is_null(),
            "C failed to create VL-block frame schunk"
        );
        assert_eq!(
            ffi::blosc2_schunk_append_chunk(c_schunk, c_chunk.as_ptr() as *mut u8, true),
            1
        );
        assert_ne!((*c_schunk).flags2 & BLOSC2_VL_BLOCKS, 0);

        let mut cframe: *mut u8 = std::ptr::null_mut();
        let mut needs_free = false;
        let cframe_len = ffi::blosc2_schunk_to_buffer(c_schunk, &mut cframe, &mut needs_free);
        assert!(
            cframe_len > 0,
            "C failed to serialize VL-block frame: {cframe_len}"
        );
        assert!(!cframe.is_null());
        let frame = std::slice::from_raw_parts(cframe, cframe_len as usize).to_vec();
        if needs_free {
            free(cframe.cast());
        }
        assert_eq!(ffi::blosc2_schunk_free(c_schunk), 0);

        assert_ne!(
            frame[25] & FRAME_VL_BLOCKS,
            0,
            "C VL-block schunk frame should set FRAME_VL_BLOCKS"
        );
        let rust_schunk = Schunk::from_frame(&frame).unwrap();
        assert_eq!(rust_schunk.nchunks(), 1);
        assert_eq!(rust_schunk.decompress_vlblock(0, 1).unwrap(), blocks[1]);
        assert_eq!(
            rust_schunk.decompress_chunk(0).unwrap(),
            blocks.iter().flatten().copied().collect::<Vec<_>>()
        );
        assert!(ChunkHeader::read(rust_schunk.compressed_chunk(0).unwrap())
            .unwrap()
            .vl_blocks());
    }
}

#[test]
fn test_rust_vlmetalayer_frame_c_reads() {
    let _b = init_blosc2();

    let mut schunk = Schunk::new(CParams::default(), Default::default());
    schunk.append_buffer(b"payload").unwrap();
    let content = b"variable-length metalayer content that C must decompress";
    schunk.add_vlmetalayer("vlmeta", content).unwrap();

    let mut frame = schunk.to_frame();
    let c_schunk =
        unsafe { ffi::blosc2_schunk_from_buffer(frame.as_mut_ptr(), frame.len() as i64, true) };
    assert!(
        !c_schunk.is_null(),
        "C failed to open Rust-produced VL-metalayer frame"
    );

    let name = CString::new("vlmeta").unwrap();
    let mut content_ptr = std::ptr::null_mut::<u8>();
    let mut content_len = 0i32;
    let rc = unsafe {
        ffi::blosc2_vlmeta_get(c_schunk, name.as_ptr(), &mut content_ptr, &mut content_len)
    };
    assert!(rc >= 0, "C failed to read Rust VL-metalayer");
    assert_eq!(content_len as usize, content.len());
    let restored = unsafe { std::slice::from_raw_parts(content_ptr, content.len()) };
    assert_eq!(restored, content);
    unsafe {
        free(content_ptr as *mut c_void);
    }

    let rc = unsafe { ffi::blosc2_schunk_free(c_schunk) };
    assert_eq!(rc, 0);
}

#[test]
fn test_c_frame_metalayers_rust_reads() {
    let _b = init_blosc2();

    let chunks = [
        b"C frame chunk zero".as_slice(),
        b"C frame chunk one with a longer payload".as_slice(),
    ];
    let fixed_name = CString::new("fixed").unwrap();
    let fixed_content = b"C fixed metalayer bytes";
    let vl_name = CString::new("vlmeta").unwrap();
    let vl_content = b"C variable metalayer payload that Rust must decompress from the frame";

    unsafe {
        let mut cparams: ffi::blosc2_cparams = std::mem::zeroed();
        cparams.compcode = BLOSC_LZ4;
        cparams.clevel = 5;
        cparams.typesize = 1;
        cparams.nthreads = 1;
        cparams.splitmode = BLOSC_FORWARD_COMPAT_SPLIT;
        cparams.filters[BLOSC2_MAX_FILTERS - 1] = BLOSC_SHUFFLE;
        let mut dparams: ffi::blosc2_dparams = std::mem::zeroed();
        dparams.nthreads = 1;
        dparams.typesize = 1;
        let mut storage: ffi::blosc2_storage = std::mem::zeroed();
        storage.contiguous = true;
        storage.cparams = &mut cparams;
        storage.dparams = &mut dparams;

        let c_schunk = ffi::blosc2_schunk_new(&mut storage);
        assert!(!c_schunk.is_null(), "C failed to create frame schunk");
        assert_eq!(
            ffi::blosc2_meta_add(
                c_schunk,
                fixed_name.as_ptr(),
                fixed_content.as_ptr() as *mut u8,
                fixed_content.len() as i32,
            ),
            0
        );
        assert_eq!(
            ffi::blosc2_vlmeta_add(
                c_schunk,
                vl_name.as_ptr(),
                vl_content.as_ptr() as *mut u8,
                vl_content.len() as i32,
                std::ptr::null_mut(),
            ),
            0
        );
        for chunk in chunks {
            let rc = ffi::blosc2_schunk_append_buffer(
                c_schunk,
                chunk.as_ptr().cast(),
                chunk.len() as i32,
            );
            assert!(rc >= 0, "C failed to append frame chunk: {rc}");
        }

        let mut cframe: *mut u8 = std::ptr::null_mut();
        let mut needs_free = false;
        let cframe_len = ffi::blosc2_schunk_to_buffer(c_schunk, &mut cframe, &mut needs_free);
        assert!(cframe_len > 0, "C failed to serialize frame: {cframe_len}");
        assert!(!cframe.is_null());
        let frame = std::slice::from_raw_parts(cframe, cframe_len as usize).to_vec();
        if needs_free {
            free(cframe.cast());
        }
        assert_eq!(ffi::blosc2_schunk_free(c_schunk), 0);

        let rust = Schunk::from_frame(&frame).unwrap();
        assert_eq!(rust.nchunks(), chunks.len() as i64);
        for (idx, expected) in chunks.iter().enumerate() {
            assert_eq!(rust.decompress_chunk(idx as i64).unwrap(), *expected);
        }
        assert_eq!(rust.metalayer("fixed"), Some(fixed_content.as_slice()));
        assert_eq!(rust.vlmetalayer("vlmeta"), Some(vl_content.as_slice()));
        assert_eq!(rust.metalayer_names(), vec!["fixed"]);
        assert_eq!(rust.vlmetalayer_names(), vec!["vlmeta"]);
    }
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
