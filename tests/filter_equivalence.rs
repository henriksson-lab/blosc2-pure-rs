#![cfg(feature = "_ffi")]
mod common;
use blosc2_pure_rs::b2nd::{B2ndArray, B2ndMeta};
use blosc2_pure_rs::compress::{blosc2_create_cctx, compress, CParams, DParams};
use blosc2_pure_rs::constants::{
    BLOSC2_CHUNK_FILTER_CODES, BLOSC2_ERROR_FAILURE, BLOSC2_ERROR_INVALID_PARAM,
    BLOSC2_ERROR_SUCCESS, BLOSC2_GLOBAL_REGISTERED_FILTERS_START,
    BLOSC2_GLOBAL_REGISTERED_FILTERS_STOP, BLOSC2_MAX_FILTERS, BLOSC2_USER_DEFINED_FILTERS_START,
    BLOSC_ALWAYS_SPLIT, BLOSC_BITSHUFFLE, BLOSC_BLOSCLZ, BLOSC_DELTA, BLOSC_FILTER_BYTEDELTA,
    BLOSC_FILTER_BYTEDELTA_BUGGY, BLOSC_FILTER_INT_TRUNC, BLOSC_FILTER_NDCELL, BLOSC_FILTER_NDMEAN,
    BLOSC_LZ4, BLOSC_SHUFFLE, BLOSC_TRUNC_PREC,
};
use blosc2_pure_rs::filters;
use blosc2_pure_rs::schunk::Schunk;
use blosc2_pure_rs::{
    blosc2_bitshuffle, blosc2_bitunshuffle, blosc2_register_filter, blosc2_shuffle,
    blosc2_unshuffle, Blosc2FilterAbi, Blosc2FilterCParams, Blosc2FilterDParams,
};
use common::ffi;
use std::ffi::CString;
use std::os::raw::c_void;

fn init_blosc2() -> common::Blosc2 {
    common::Blosc2::new()
}

struct B2ndFilterFixture {
    name: &'static str,
    filter: u8,
    typesize: i32,
    dtype: &'static str,
    shape: &'static [i64],
    chunkshape: &'static [i32],
    blockshape: &'static [i32],
    compcode: u8,
}

impl B2ndFilterFixture {
    fn blocksize(&self) -> i32 {
        self.blockshape
            .iter()
            .fold(self.typesize, |acc, dim| acc * *dim)
    }

    fn nbytes(&self) -> usize {
        self.shape
            .iter()
            .fold(self.typesize as usize, |acc, dim| acc * *dim as usize)
    }
}

fn ndcell_ndmean_c_fixtures() -> Vec<B2ndFilterFixture> {
    vec![
        B2ndFilterFixture {
            name: "ndcell_rand",
            filter: BLOSC_FILTER_NDCELL,
            typesize: 4,
            dtype: "<f4",
            shape: &[32, 18, 32],
            chunkshape: &[17, 16, 24],
            blockshape: &[8, 9, 8],
            compcode: BLOSC_LZ4,
        },
        B2ndFilterFixture {
            name: "ndcell_same_cells",
            filter: BLOSC_FILTER_NDCELL,
            typesize: 8,
            dtype: "<f8",
            shape: &[128, 111],
            chunkshape: &[32, 11],
            blockshape: &[16, 7],
            compcode: BLOSC_LZ4,
        },
        B2ndFilterFixture {
            name: "ndcell_some_matches",
            filter: BLOSC_FILTER_NDCELL,
            typesize: 8,
            dtype: "<f8",
            shape: &[128, 111],
            chunkshape: &[48, 32],
            blockshape: &[14, 18],
            compcode: BLOSC_LZ4,
        },
        B2ndFilterFixture {
            name: "ndmean_mean_rows",
            filter: BLOSC_FILTER_NDMEAN,
            typesize: 8,
            dtype: "<f8",
            shape: &[512],
            chunkshape: &[32],
            blockshape: &[16],
            compcode: BLOSC_BLOSCLZ,
        },
        B2ndFilterFixture {
            name: "ndmean_mean_same_cells",
            filter: BLOSC_FILTER_NDMEAN,
            typesize: 8,
            dtype: "<f8",
            shape: &[512],
            chunkshape: &[32],
            blockshape: &[16],
            compcode: BLOSC_BLOSCLZ,
        },
        B2ndFilterFixture {
            name: "ndmean_mean_some_matches",
            filter: BLOSC_FILTER_NDMEAN,
            typesize: 8,
            dtype: "<f8",
            shape: &[512],
            chunkshape: &[48],
            blockshape: &[14],
            compcode: BLOSC_BLOSCLZ,
        },
        B2ndFilterFixture {
            name: "ndmean_repart_same_cells",
            filter: BLOSC_FILTER_NDMEAN,
            typesize: 8,
            dtype: "<f8",
            shape: &[128, 64, 32],
            chunkshape: &[32, 32, 16],
            blockshape: &[16, 8, 8],
            compcode: BLOSC_BLOSCLZ,
        },
        B2ndFilterFixture {
            name: "ndmean_repart_some_matches",
            filter: BLOSC_FILTER_NDMEAN,
            typesize: 8,
            dtype: "<f8",
            shape: &[128, 128],
            chunkshape: &[48, 32],
            blockshape: &[16, 16],
            compcode: BLOSC_BLOSCLZ,
        },
    ]
}

#[derive(Clone)]
struct BytedeltaFixture {
    name: &'static str,
    typesize: i32,
    shape: &'static [i64],
    chunkshape: &'static [i32],
    blockshape: &'static [i32],
    data: Vec<u8>,
}

impl BytedeltaFixture {
    fn nbytes(&self) -> usize {
        self.shape
            .iter()
            .fold(self.typesize as usize, |acc, dim| acc * *dim as usize)
    }

    fn chunksize(&self) -> usize {
        self.chunkshape
            .iter()
            .fold(self.typesize as usize, |acc, dim| acc * *dim as usize)
    }

    fn blocksize(&self) -> i32 {
        self.blockshape
            .iter()
            .fold(self.typesize, |acc, dim| acc * *dim)
    }
}

fn bytedelta_c_fixtures() -> Vec<BytedeltaFixture> {
    let rand_data = (0..(32 * 18 * 32))
        .flat_map(|i| {
            // C's test_bytedelta.c uses unseeded libc rand() % 220 into a
            // float buffer. Keep the same type and value range without making
            // this fixture depend on platform-specific rand() output.
            let value = ((i * 37 + (i / 7) * 11) % 220) as f32;
            value.to_ne_bytes()
        })
        .collect();

    let mut mixed_values = Vec::with_capacity(128 * 111 * 4);
    for i in 0..(128 * 111) {
        let value = match i % 4 {
            0 => 11_111_111i32,
            1 => 99_999_999i32,
            _ => 0,
        };
        mixed_values.extend_from_slice(&value.to_ne_bytes());
    }

    let arange_like = (0..(128 * 111))
        .flat_map(|i| (i as f64).to_ne_bytes())
        .collect();

    vec![
        BytedeltaFixture {
            name: "bytedelta_rand",
            typesize: 4,
            shape: &[32, 18, 32],
            chunkshape: &[17, 16, 24],
            blockshape: &[8, 9, 8],
            data: rand_data,
        },
        BytedeltaFixture {
            name: "bytedelta_mixed_values",
            typesize: 4,
            shape: &[128, 111],
            chunkshape: &[32, 11],
            blockshape: &[16, 7],
            data: mixed_values,
        },
        BytedeltaFixture {
            name: "bytedelta_arange_like",
            typesize: 8,
            shape: &[128, 111],
            chunkshape: &[48, 32],
            blockshape: &[14, 18],
            data: arange_like,
        },
    ]
}

fn assert_c_accepts_b2nd_plugin_filter_fixture(fixture: &B2ndFilterFixture) {
    let dtype = CString::new(fixture.dtype).unwrap();
    let data = vec![0u8; fixture.nbytes()];

    unsafe {
        let mut base_cparams: ffi::blosc2_cparams = std::mem::zeroed();
        base_cparams.compcode = fixture.compcode;
        base_cparams.clevel = 5;
        base_cparams.typesize = fixture.typesize;
        base_cparams.nthreads = 1;
        base_cparams.splitmode = BLOSC_ALWAYS_SPLIT;
        base_cparams.blocksize = fixture.blocksize();

        let mut base_dparams: ffi::blosc2_dparams = std::mem::zeroed();
        base_dparams.nthreads = 1;
        base_dparams.typesize = fixture.typesize;

        let storage = ffi::blosc2_storage {
            contiguous: true,
            urlpath: std::ptr::null_mut(),
            cparams: &mut base_cparams,
            dparams: &mut base_dparams,
            io: std::ptr::null_mut(),
        };
        let ctx = ffi::b2nd_create_ctx(
            &storage,
            fixture.shape.len() as i8,
            fixture.shape.as_ptr(),
            fixture.chunkshape.as_ptr(),
            fixture.blockshape.as_ptr(),
            dtype.as_ptr(),
            0,
            std::ptr::null(),
            0,
        );
        assert!(!ctx.is_null(), "{} C b2nd_create_ctx failed", fixture.name);

        let mut array: *mut ffi::b2nd_array_t = std::ptr::null_mut();
        let rc = ffi::b2nd_from_cbuffer(ctx, &mut array, data.as_ptr().cast(), data.len() as i64);
        assert_eq!(rc, 0, "{} C b2nd_from_cbuffer failed", fixture.name);
        assert!(!array.is_null(), "{} C array is null", fixture.name);

        let schunk = (*array).sc.cast::<c_void>();
        let mut cparams: ffi::blosc2_cparams = std::mem::zeroed();
        cparams.compcode = fixture.compcode;
        cparams.clevel = 9;
        cparams.typesize = fixture.typesize;
        cparams.nthreads = 1;
        cparams.splitmode = BLOSC_ALWAYS_SPLIT;
        cparams.blocksize = fixture.blocksize();
        cparams.schunk = schunk;
        cparams.filters[BLOSC2_MAX_FILTERS - 2] = fixture.filter;
        cparams.filters_meta[BLOSC2_MAX_FILTERS - 2] = 4;
        cparams.filters[BLOSC2_MAX_FILTERS - 1] = BLOSC_SHUFFLE;

        let cctx = ffi::blosc2_create_cctx(cparams);
        assert!(
            !cctx.is_null(),
            "{} C plugin cctx should be accepted with B2ND schunk metadata",
            fixture.name
        );

        let mut dparams: ffi::blosc2_dparams = std::mem::zeroed();
        dparams.nthreads = 1;
        dparams.typesize = fixture.typesize;
        dparams.schunk = schunk;
        let dctx = ffi::blosc2_create_dctx(dparams);
        assert!(
            !dctx.is_null(),
            "{} C plugin dctx should be accepted with B2ND schunk metadata",
            fixture.name
        );

        ffi::blosc2_free_ctx(cctx);
        ffi::blosc2_free_ctx(dctx);
        assert_eq!(ffi::b2nd_free(array), 0);
        assert_eq!(ffi::b2nd_free_ctx(ctx), 0);
    }
}

fn assert_c_b2nd_bytedelta_meta_zero_roundtrips(data: &[u8]) {
    let dtype = CString::new("<u4").unwrap();
    let shape = [4i64, 4];
    let chunkshape = [4i32, 4];
    let blockshape = [4i32, 4];

    unsafe {
        let mut cparams: ffi::blosc2_cparams = std::mem::zeroed();
        cparams.compcode = BLOSC_LZ4;
        cparams.clevel = 5;
        cparams.typesize = 4;
        cparams.nthreads = 1;
        cparams.blocksize = data.len() as i32;
        cparams.filters[BLOSC2_MAX_FILTERS - 2] = BLOSC_SHUFFLE;
        cparams.filters[BLOSC2_MAX_FILTERS - 1] = BLOSC_FILTER_BYTEDELTA;
        cparams.filters_meta[BLOSC2_MAX_FILTERS - 1] = 0;

        let mut dparams: ffi::blosc2_dparams = std::mem::zeroed();
        dparams.nthreads = 1;
        dparams.typesize = 4;

        let storage = ffi::blosc2_storage {
            contiguous: true,
            urlpath: std::ptr::null_mut(),
            cparams: &mut cparams,
            dparams: &mut dparams,
            io: std::ptr::null_mut(),
        };
        let ctx = ffi::b2nd_create_ctx(
            &storage,
            shape.len() as i8,
            shape.as_ptr(),
            chunkshape.as_ptr(),
            blockshape.as_ptr(),
            dtype.as_ptr(),
            0,
            std::ptr::null(),
            0,
        );
        assert!(!ctx.is_null(), "C b2nd bytedelta ctx failed");

        let mut array: *mut ffi::b2nd_array_t = std::ptr::null_mut();
        assert_eq!(
            ffi::b2nd_from_cbuffer(ctx, &mut array, data.as_ptr().cast(), data.len() as i64),
            0
        );
        assert!(!array.is_null(), "C b2nd bytedelta array is null");

        let mut out = vec![0u8; data.len()];
        assert_eq!(
            ffi::b2nd_to_cbuffer(array, out.as_mut_ptr().cast(), out.len() as i64),
            0
        );
        assert_eq!(out, data, "C bytedelta meta=0 B2ND roundtrip failed");

        assert_eq!(ffi::b2nd_free(array), 0);
        assert_eq!(ffi::b2nd_free_ctx(ctx), 0);
    }
}

#[allow(clippy::too_many_arguments)]
fn c_b2nd_filter_to_cbuffer(
    filter: u8,
    filter_meta: u8,
    typesize: i32,
    dtype: &str,
    shape: &[i64],
    chunkshape: &[i32],
    blockshape: &[i32],
    data: &[u8],
) -> Vec<u8> {
    let dtype = CString::new(dtype).unwrap();

    unsafe {
        let mut cparams: ffi::blosc2_cparams = std::mem::zeroed();
        cparams.compcode = BLOSC_LZ4;
        cparams.clevel = 5;
        cparams.typesize = typesize;
        cparams.nthreads = 1;
        cparams.blocksize = blockshape.iter().fold(typesize, |acc, dim| acc * *dim);
        cparams.filters[BLOSC2_MAX_FILTERS - 1] = filter;
        cparams.filters_meta[BLOSC2_MAX_FILTERS - 1] = filter_meta;

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
            shape.len() as i8,
            shape.as_ptr(),
            chunkshape.as_ptr(),
            blockshape.as_ptr(),
            dtype.as_ptr(),
            0,
            std::ptr::null(),
            0,
        );
        assert!(!ctx.is_null(), "C b2nd filter ctx failed");

        let mut array: *mut ffi::b2nd_array_t = std::ptr::null_mut();
        assert_eq!(
            ffi::b2nd_from_cbuffer(ctx, &mut array, data.as_ptr().cast(), data.len() as i64),
            0,
            "C b2nd filter from_dense_buffer failed"
        );
        assert!(!array.is_null(), "C b2nd filter array is null");

        let mut out = vec![0u8; data.len()];
        assert_eq!(
            ffi::b2nd_to_cbuffer(array, out.as_mut_ptr().cast(), out.len() as i64),
            0,
            "C b2nd filter to_dense_buffer failed"
        );

        assert_eq!(ffi::b2nd_free(array), 0);
        assert_eq!(ffi::b2nd_free_ctx(ctx), 0);
        out
    }
}

fn c_schunk_append_decompress_with_filter(
    data: &[u8],
    mut cparams: ffi::blosc2_cparams,
) -> Vec<u8> {
    unsafe {
        cparams.nthreads = 1;
        let mut dparams: ffi::blosc2_dparams = std::mem::zeroed();
        dparams.nthreads = 1;
        dparams.typesize = cparams.typesize;
        let mut storage = ffi::blosc2_storage {
            contiguous: true,
            urlpath: std::ptr::null_mut(),
            cparams: &mut cparams,
            dparams: &mut dparams,
            io: std::ptr::null_mut(),
        };
        let schunk = ffi::blosc2_schunk_new(&mut storage);
        assert!(!schunk.is_null(), "C schunk creation failed");
        assert_eq!(
            ffi::blosc2_schunk_append_buffer(schunk, data.as_ptr().cast(), data.len() as i32,),
            1
        );

        let mut out = vec![0u8; data.len()];
        assert_eq!(
            ffi::blosc2_schunk_decompress_chunk(
                schunk,
                0,
                out.as_mut_ptr().cast(),
                out.len() as i32,
            ),
            data.len() as i32
        );
        assert_eq!(ffi::blosc2_schunk_free(schunk), 0);
        out
    }
}

fn rust_schunk_append_decompress_with_filter(
    data: &[u8],
    cparams: CParams,
) -> Result<Vec<u8>, &'static str> {
    let mut schunk = Schunk::new(cparams, DParams::default());
    schunk.append_buffer(data)?;
    schunk.decompress_chunk(0)
}

fn expected_int_trunc_bytes(typesize: usize, meta: u8, data: &[u8]) -> Vec<u8> {
    let prec_bits = meta as i8 as i16;
    let max_prec_bits = (typesize * 8) as i16;
    let zeroed_bits = if prec_bits >= 0 {
        max_prec_bits - prec_bits
    } else {
        -prec_bits
    };
    assert!(
        zeroed_bits >= 0 && zeroed_bits < max_prec_bits,
        "test fixture must use a C-supported int_trunc precision"
    );
    let mask = !((1u64 << zeroed_bits) - 1);
    let mut expected = Vec::with_capacity(data.len());
    for item in data.chunks_exact(typesize) {
        let value = match typesize {
            1 => u64::from(item[0]),
            2 => u64::from(u16::from_ne_bytes([item[0], item[1]])),
            4 => u64::from(u32::from_ne_bytes([item[0], item[1], item[2], item[3]])),
            8 => u64::from_ne_bytes([
                item[0], item[1], item[2], item[3], item[4], item[5], item[6], item[7],
            ]),
            _ => unreachable!("test fixture uses C-supported integer widths"),
        } & mask;

        match typesize {
            1 => expected.push(value as u8),
            2 => expected.extend_from_slice(&(value as u16).to_ne_bytes()),
            4 => expected.extend_from_slice(&(value as u32).to_ne_bytes()),
            8 => expected.extend_from_slice(&value.to_ne_bytes()),
            _ => unreachable!("test fixture uses C-supported integer widths"),
        }
    }
    expected
}

fn bytedelta_b2nd_meta(fixture: &BytedeltaFixture) -> B2ndMeta {
    B2ndMeta::with_default_dtype(
        fixture.shape.to_vec(),
        fixture.chunkshape.to_vec(),
        fixture.blockshape.to_vec(),
        fixture.typesize as usize,
    )
    .unwrap()
}

fn rust_bytedelta_b2nd_compress_chunks(fixture: &BytedeltaFixture, filter: u8) -> Vec<Vec<u8>> {
    let mut filters = [0; BLOSC2_MAX_FILTERS];
    let mut filters_meta = [0; BLOSC2_MAX_FILTERS];
    filters[BLOSC2_MAX_FILTERS - 2] = BLOSC_SHUFFLE;
    filters[BLOSC2_MAX_FILTERS - 1] = filter;
    filters_meta[BLOSC2_MAX_FILTERS - 1] = 0;

    let array = B2ndArray::from_dense_buffer(
        bytedelta_b2nd_meta(fixture),
        &fixture.data,
        CParams {
            compcode: BLOSC_LZ4,
            clevel: 9,
            typesize: fixture.typesize,
            filters,
            filters_meta,
            ..Default::default()
        },
        DParams::default(),
    )
    .unwrap_or_else(|err| {
        panic!(
            "{} Rust B2ND bytedelta fixture creation failed with filter {filter}: {err}",
            fixture.name
        )
    });

    (0..array.schunk.nchunks())
        .map(|nchunk| array.schunk.compressed_chunk_bytes_owned(nchunk).unwrap())
        .collect()
}

fn rust_bytedelta_schunk_decompress_chunk(
    fixture: &BytedeltaFixture,
    chunk: &[u8],
    expected_len: usize,
) -> Vec<u8> {
    let mut schunk = Schunk::new(
        CParams {
            typesize: fixture.typesize,
            ..Default::default()
        },
        DParams {
            typesize: fixture.typesize,
            ..Default::default()
        },
    );
    assert_eq!(
        schunk.append_chunk(chunk),
        Ok(1),
        "{} Rust bytedelta schunk append_chunk failed",
        fixture.name
    );
    let decoded = schunk.decompress_chunk(0).unwrap_or_else(|err| {
        panic!(
            "{} Rust bytedelta schunk decompress failed: {err}",
            fixture.name
        )
    });
    assert_eq!(
        decoded.len(),
        expected_len,
        "{} Rust bytedelta decompressed chunk length mismatch",
        fixture.name
    );
    decoded
}

fn c_bytedelta_b2nd_compress_chunks(fixture: &BytedeltaFixture, write_filter: u8) -> Vec<Vec<u8>> {
    unsafe {
        let dtype = CString::new(format!("|S{}", fixture.typesize)).unwrap();

        let mut cparams: ffi::blosc2_cparams = std::mem::zeroed();
        cparams.compcode = BLOSC_LZ4;
        cparams.clevel = 9;
        cparams.typesize = fixture.typesize;
        cparams.nthreads = 1;
        cparams.blocksize = fixture.blocksize();
        cparams.filters[BLOSC2_MAX_FILTERS - 2] = BLOSC_SHUFFLE;
        cparams.filters[BLOSC2_MAX_FILTERS - 1] = write_filter;
        cparams.filters_meta[BLOSC2_MAX_FILTERS - 1] = 0;

        let mut dparams: ffi::blosc2_dparams = std::mem::zeroed();
        dparams.nthreads = 1;
        dparams.typesize = fixture.typesize;
        let storage = ffi::blosc2_storage {
            contiguous: true,
            urlpath: std::ptr::null_mut(),
            cparams: &mut cparams,
            dparams: &mut dparams,
            io: std::ptr::null_mut(),
        };
        let ctx = ffi::b2nd_create_ctx(
            &storage,
            fixture.shape.len() as i8,
            fixture.shape.as_ptr(),
            fixture.chunkshape.as_ptr(),
            fixture.blockshape.as_ptr(),
            dtype.as_ptr(),
            0,
            std::ptr::null(),
            0,
        );
        assert!(
            !ctx.is_null(),
            "{} C bytedelta B2ND ctx failed",
            fixture.name
        );

        let mut array: *mut ffi::b2nd_array_t = std::ptr::null_mut();
        assert_eq!(
            ffi::b2nd_from_cbuffer(
                ctx,
                &mut array,
                fixture.data.as_ptr().cast(),
                fixture.data.len() as i64,
            ),
            0,
            "{} C bytedelta B2ND from_dense_buffer failed with filter {write_filter}",
            fixture.name
        );
        assert!(
            !array.is_null(),
            "{} C bytedelta B2ND array is null",
            fixture.name
        );
        let schunk = (*array).sc;
        assert!(
            !schunk.is_null(),
            "{} C bytedelta B2ND schunk is null",
            fixture.name
        );

        let mut chunks = Vec::with_capacity((*schunk).nchunks as usize);
        for nchunk in 0..(*schunk).nchunks {
            let mut chunk_ptr: *mut u8 = std::ptr::null_mut();
            let mut needs_free = false;
            let chunk_len =
                ffi::blosc2_schunk_get_chunk(schunk, nchunk, &mut chunk_ptr, &mut needs_free);
            assert!(
                chunk_len > 0 && !chunk_ptr.is_null(),
                "{} C bytedelta get_chunk failed for chunk {nchunk}",
                fixture.name
            );
            assert!(
                !needs_free,
                "{} in-memory C schunk unexpectedly returned owned chunk memory",
                fixture.name
            );
            chunks.push(std::slice::from_raw_parts(chunk_ptr, chunk_len as usize).to_vec());
        }

        assert_eq!(ffi::b2nd_free(array), 0);
        assert_eq!(ffi::b2nd_free_ctx(ctx), 0);
        chunks
    }
}

fn c_bytedelta_b2nd_decompressed_chunks(fixture: &BytedeltaFixture) -> Vec<Vec<u8>> {
    unsafe {
        let dtype = CString::new(format!("|S{}", fixture.typesize)).unwrap();

        let mut cparams: ffi::blosc2_cparams = std::mem::zeroed();
        cparams.typesize = fixture.typesize;
        cparams.nthreads = 1;

        let mut dparams: ffi::blosc2_dparams = std::mem::zeroed();
        dparams.nthreads = 1;
        dparams.typesize = fixture.typesize;
        let storage = ffi::blosc2_storage {
            contiguous: true,
            urlpath: std::ptr::null_mut(),
            cparams: &mut cparams,
            dparams: &mut dparams,
            io: std::ptr::null_mut(),
        };
        let ctx = ffi::b2nd_create_ctx(
            &storage,
            fixture.shape.len() as i8,
            fixture.shape.as_ptr(),
            fixture.chunkshape.as_ptr(),
            fixture.blockshape.as_ptr(),
            dtype.as_ptr(),
            0,
            std::ptr::null(),
            0,
        );
        assert!(
            !ctx.is_null(),
            "{} C bytedelta source B2ND ctx failed",
            fixture.name
        );

        let mut array: *mut ffi::b2nd_array_t = std::ptr::null_mut();
        assert_eq!(
            ffi::b2nd_from_cbuffer(
                ctx,
                &mut array,
                fixture.data.as_ptr().cast(),
                fixture.data.len() as i64,
            ),
            0,
            "{} C bytedelta source B2ND from_dense_buffer failed",
            fixture.name
        );
        assert!(
            !array.is_null(),
            "{} C bytedelta source B2ND array is null",
            fixture.name
        );
        let schunk = (*array).sc;
        assert!(
            !schunk.is_null(),
            "{} C bytedelta source B2ND schunk is null",
            fixture.name
        );

        let mut chunks = Vec::with_capacity((*schunk).nchunks as usize);
        for nchunk in 0..(*schunk).nchunks {
            let mut decoded = vec![0u8; (*schunk).chunksize as usize];
            assert_eq!(
                ffi::blosc2_schunk_decompress_chunk(
                    schunk,
                    nchunk,
                    decoded.as_mut_ptr().cast(),
                    decoded.len() as i32,
                ),
                decoded.len() as i32,
                "{} C bytedelta source B2ND decompression failed for chunk {nchunk}",
                fixture.name
            );
            chunks.push(decoded);
        }

        assert_eq!(ffi::b2nd_free(array), 0);
        assert_eq!(ffi::b2nd_free_ctx(ctx), 0);
        chunks
    }
}

fn c_bytedelta_schunk_decompress_chunk(
    fixture: &BytedeltaFixture,
    chunk: &[u8],
    expected_len: usize,
) -> Vec<u8> {
    unsafe {
        let mut cparams: ffi::blosc2_cparams = std::mem::zeroed();
        cparams.typesize = fixture.typesize;
        cparams.nthreads = 1;

        let mut dparams: ffi::blosc2_dparams = std::mem::zeroed();
        dparams.nthreads = 1;
        dparams.typesize = fixture.typesize;
        let mut storage = ffi::blosc2_storage {
            contiguous: true,
            urlpath: std::ptr::null_mut(),
            cparams: &mut cparams,
            dparams: &mut dparams,
            io: std::ptr::null_mut(),
        };
        let schunk = ffi::blosc2_schunk_new(&mut storage);
        assert!(
            !schunk.is_null(),
            "{} C bytedelta decode schunk failed",
            fixture.name
        );
        assert_eq!(
            ffi::blosc2_schunk_append_chunk(schunk, chunk.as_ptr().cast_mut(), true),
            1,
            "{} C bytedelta decode append_chunk failed",
            fixture.name
        );

        let mut decoded = vec![0u8; expected_len];
        assert_eq!(
            ffi::blosc2_schunk_decompress_chunk(
                schunk,
                0,
                decoded.as_mut_ptr().cast(),
                decoded.len() as i32,
            ),
            expected_len as i32,
            "{} C bytedelta schunk decompression failed",
            fixture.name
        );

        assert_eq!(ffi::blosc2_schunk_free(schunk), 0);
        decoded
    }
}

fn force_bytedelta_reader_filter(mut chunk: Vec<u8>, read_filter: u8) -> Vec<u8> {
    chunk[BLOSC2_CHUNK_FILTER_CODES + BLOSC2_MAX_FILTERS - 1] = read_filter;
    chunk
}

unsafe extern "C" fn c_copy_filter_forward(
    input: *const u8,
    output: *mut u8,
    length: i32,
    _meta: u8,
    _cparams: *mut ffi::blosc2_cparams,
    _id: u8,
) -> i32 {
    unsafe {
        std::ptr::copy_nonoverlapping(input, output, length as usize);
    }
    BLOSC2_ERROR_SUCCESS
}

unsafe extern "C" fn c_copy_filter_backward(
    input: *const u8,
    output: *mut u8,
    length: i32,
    _meta: u8,
    _dparams: *mut ffi::blosc2_dparams,
    _id: u8,
) -> i32 {
    unsafe {
        std::ptr::copy_nonoverlapping(input, output, length as usize);
    }
    BLOSC2_ERROR_SUCCESS
}

unsafe extern "C" fn rust_c_abi_copy_filter_forward(
    input: *const u8,
    output: *mut u8,
    length: i32,
    _meta: u8,
    _cparams: *mut Blosc2FilterCParams,
    _id: u8,
) -> i32 {
    unsafe {
        std::ptr::copy_nonoverlapping(input, output, length as usize);
    }
    BLOSC2_ERROR_SUCCESS
}

unsafe extern "C" fn rust_c_abi_copy_filter_backward(
    input: *const u8,
    output: *mut u8,
    length: i32,
    _meta: u8,
    _dparams: *mut Blosc2FilterDParams,
    _id: u8,
) -> i32 {
    unsafe {
        std::ptr::copy_nonoverlapping(input, output, length as usize);
    }
    BLOSC2_ERROR_SUCCESS
}

#[test]
fn test_ndcell_ndmean_register_and_fail_without_b2nd_metadata() {
    let _b = init_blosc2();

    for (filter, name) in [
        (BLOSC_FILTER_NDCELL, "ndcell"),
        (BLOSC_FILTER_NDMEAN, "ndmean"),
    ] {
        assert_eq!(filters::known_global_filter_info(filter), Some((name, 1)));
        assert_eq!(filters::registered_filter_info(filter), Some((name, 1)));

        let mut cparams = CParams {
            typesize: 4,
            filters: [0; BLOSC2_MAX_FILTERS],
            filters_meta: [0; BLOSC2_MAX_FILTERS],
            ..Default::default()
        };
        cparams.filters[BLOSC2_MAX_FILTERS - 2] = filter;
        cparams.filters_meta[BLOSC2_MAX_FILTERS - 2] = 4;

        assert!(blosc2_create_cctx(cparams.clone()).is_ok());
        assert_eq!(
            compress(&[0u8; 128], &cparams).unwrap_err(),
            "Filter pipeline failed"
        );
    }
}

#[test]
fn test_public_c_style_filter_registration_rejects_global_ids_like_c() {
    let _b = init_blosc2();

    for id in [
        BLOSC2_GLOBAL_REGISTERED_FILTERS_START,
        BLOSC_FILTER_NDCELL,
        BLOSC2_GLOBAL_REGISTERED_FILTERS_STOP,
    ] {
        let c_name = CString::new(format!("c-global-filter-{id}")).unwrap();
        let mut c_filter = ffi::blosc2_filter {
            id,
            name: c_name.as_ptr().cast_mut(),
            version: 1,
            forward: Some(c_copy_filter_forward),
            backward: Some(c_copy_filter_backward),
        };
        let rust_name = CString::new(format!("rust-global-filter-{id}")).unwrap();
        let rust_filter = Blosc2FilterAbi {
            id,
            name: rust_name.as_ptr(),
            version: 1,
            forward: Some(rust_c_abi_copy_filter_forward),
            backward: Some(rust_c_abi_copy_filter_backward),
        };

        let c_rc = unsafe { ffi::blosc2_register_filter(&mut c_filter) };
        assert_eq!(
            c_rc, BLOSC2_ERROR_FAILURE,
            "C accepted global filter id {id}"
        );
        assert_eq!(
            blosc2_register_filter(&rust_filter),
            c_rc,
            "Rust C-style registration must match C for global filter id {id}"
        );
    }

    let c_name = CString::new("c-user-filter-250").unwrap();
    let mut c_filter = ffi::blosc2_filter {
        id: BLOSC2_USER_DEFINED_FILTERS_START + 90,
        name: c_name.as_ptr().cast_mut(),
        version: 1,
        forward: Some(c_copy_filter_forward),
        backward: Some(c_copy_filter_backward),
    };
    let rust_name = CString::new("rust-user-filter-251").unwrap();
    let rust_filter = Blosc2FilterAbi {
        id: BLOSC2_USER_DEFINED_FILTERS_START + 91,
        name: rust_name.as_ptr(),
        version: 1,
        forward: Some(rust_c_abi_copy_filter_forward),
        backward: Some(rust_c_abi_copy_filter_backward),
    };

    assert_eq!(
        unsafe { ffi::blosc2_register_filter(&mut c_filter) },
        BLOSC2_ERROR_SUCCESS
    );
    assert_eq!(blosc2_register_filter(&rust_filter), BLOSC2_ERROR_SUCCESS);
}

#[test]
fn test_raw_filter_wrappers_validate_typesize_and_buffers_like_c() {
    let _b = init_blosc2();

    let data: Vec<u8> = (0..64).map(|i| (i * 5 + 7) as u8).collect();
    let mut c_out = vec![0u8; data.len()];
    let mut rust_out = vec![0u8; data.len()];

    for typesize in [1, 2, 4, 8, 16, 255, 256] {
        c_out.fill(0);
        rust_out.fill(0);
        let c_rc = unsafe {
            ffi::blosc2_shuffle(
                typesize,
                data.len() as i32,
                data.as_ptr().cast(),
                c_out.as_mut_ptr().cast(),
            )
        };
        assert_eq!(
            blosc2_shuffle(typesize, data.len() as i32, &data, &mut rust_out),
            c_rc
        );
        assert_eq!(
            rust_out, c_out,
            "shuffle output mismatch for typesize={typesize}"
        );

        c_out.fill(0);
        rust_out.fill(0);
        let c_rc = unsafe {
            ffi::blosc2_unshuffle(
                typesize,
                data.len() as i32,
                data.as_ptr().cast(),
                c_out.as_mut_ptr().cast(),
            )
        };
        assert_eq!(
            blosc2_unshuffle(typesize, data.len() as i32, &data, &mut rust_out),
            c_rc
        );
        assert_eq!(
            rust_out, c_out,
            "unshuffle output mismatch for typesize={typesize}"
        );

        c_out.fill(0);
        rust_out.fill(0);
        let c_rc = unsafe {
            ffi::blosc2_bitshuffle(
                typesize,
                data.len() as i32,
                data.as_ptr().cast(),
                c_out.as_mut_ptr().cast(),
            )
        };
        assert_eq!(
            blosc2_bitshuffle(typesize, data.len() as i32, &data, &mut rust_out),
            c_rc
        );
        assert_eq!(
            rust_out, c_out,
            "bitshuffle output mismatch for typesize={typesize}"
        );

        c_out.fill(0);
        rust_out.fill(0);
        let c_rc = unsafe {
            ffi::blosc2_bitunshuffle(
                typesize,
                data.len() as i32,
                data.as_ptr().cast(),
                c_out.as_mut_ptr().cast(),
            )
        };
        assert_eq!(
            blosc2_bitunshuffle(typesize, data.len() as i32, &data, &mut rust_out),
            c_rc
        );
        assert_eq!(
            rust_out, c_out,
            "bitunshuffle output mismatch for typesize={typesize}"
        );
    }

    for typesize in [0] {
        assert_eq!(
            blosc2_shuffle(typesize, data.len() as i32, &data, &mut rust_out),
            BLOSC2_ERROR_INVALID_PARAM
        );
        assert_eq!(
            blosc2_unshuffle(typesize, data.len() as i32, &data, &mut rust_out),
            BLOSC2_ERROR_INVALID_PARAM
        );
        assert_eq!(
            blosc2_bitshuffle(typesize, data.len() as i32, &data, &mut rust_out),
            BLOSC2_ERROR_INVALID_PARAM
        );
        assert_eq!(
            blosc2_bitunshuffle(typesize, data.len() as i32, &data, &mut rust_out),
            BLOSC2_ERROR_INVALID_PARAM
        );
    }

    for typesize in [0, 257] {
        let c_rc = unsafe {
            ffi::blosc2_shuffle(
                typesize,
                data.len() as i32,
                data.as_ptr().cast(),
                c_out.as_mut_ptr().cast(),
            )
        };
        assert_eq!(
            blosc2_shuffle(typesize, data.len() as i32, &data, &mut rust_out),
            c_rc
        );

        let c_rc = unsafe {
            ffi::blosc2_unshuffle(
                typesize,
                data.len() as i32,
                data.as_ptr().cast(),
                c_out.as_mut_ptr().cast(),
            )
        };
        assert_eq!(
            blosc2_unshuffle(typesize, data.len() as i32, &data, &mut rust_out),
            c_rc
        );
    }

    for typesize in [257] {
        let c_rc = unsafe {
            ffi::blosc2_bitshuffle(
                typesize,
                data.len() as i32,
                data.as_ptr().cast(),
                c_out.as_mut_ptr().cast(),
            )
        };
        assert_eq!(c_rc, BLOSC2_ERROR_INVALID_PARAM);
        rust_out.fill(0);
        assert_eq!(
            blosc2_bitshuffle(typesize, data.len() as i32, &data, &mut rust_out),
            c_rc
        );

        let c_rc = unsafe {
            ffi::blosc2_bitunshuffle(
                typesize,
                data.len() as i32,
                data.as_ptr().cast(),
                c_out.as_mut_ptr().cast(),
            )
        };
        assert_eq!(
            c_rc,
            data.len() as i32,
            "C bitunshuffle accepts typesize={typesize}"
        );
        rust_out.fill(0);
        assert_eq!(
            blosc2_bitunshuffle(typesize, data.len() as i32, &data, &mut rust_out),
            c_rc
        );
    }

    assert_eq!(
        blosc2_bitshuffle(4, -1, &data, &mut rust_out),
        BLOSC2_ERROR_INVALID_PARAM
    );
    assert_eq!(
        blosc2_bitunshuffle(4, data.len() as i32, &data, &mut rust_out[..4]),
        BLOSC2_ERROR_INVALID_PARAM
    );
}

#[test]
fn test_ndcell_ndmean_c_fixtures_require_live_b2nd_schunk_metadata() {
    let _b = init_blosc2();

    for fixture in ndcell_ndmean_c_fixtures() {
        assert_c_accepts_b2nd_plugin_filter_fixture(&fixture);
        assert!(filters::global_filter_requires_b2nd_metadata(
            fixture.filter
        ));

        let mut cparams = CParams {
            typesize: fixture.typesize,
            compcode: fixture.compcode,
            blocksize: fixture.blocksize(),
            splitmode: BLOSC_ALWAYS_SPLIT,
            filters: [0; BLOSC2_MAX_FILTERS],
            filters_meta: [0; BLOSC2_MAX_FILTERS],
            ..Default::default()
        };
        cparams.filters[BLOSC2_MAX_FILTERS - 2] = fixture.filter;
        cparams.filters_meta[BLOSC2_MAX_FILTERS - 2] = 4;
        cparams.filters[BLOSC2_MAX_FILTERS - 1] = BLOSC_SHUFFLE;

        assert!(blosc2_create_cctx(cparams.clone()).is_ok());
        assert_eq!(
            compress(&vec![0; fixture.blocksize() as usize], &cparams).unwrap_err(),
            "Filter pipeline failed",
            "{} standalone compression must not silently use metadata-free NDCELL/NDMEAN",
            fixture.name
        );
    }
}

#[test]
fn test_ported_global_plugin_filters_are_accepted_by_context_creation() {
    let _b = init_blosc2();

    for filter in [
        BLOSC_FILTER_BYTEDELTA_BUGGY,
        BLOSC_FILTER_BYTEDELTA,
        BLOSC_FILTER_INT_TRUNC,
    ] {
        let mut cparams = CParams {
            typesize: 4,
            filters: [0; BLOSC2_MAX_FILTERS],
            filters_meta: [0; BLOSC2_MAX_FILTERS],
            ..Default::default()
        };
        cparams.filters[BLOSC2_MAX_FILTERS - 1] = filter;
        cparams.filters_meta[BLOSC2_MAX_FILTERS - 1] = 4;

        assert!(blosc2_create_cctx(cparams).is_ok());
    }

    let mut cparams = CParams {
        filters: [0; BLOSC2_MAX_FILTERS],
        ..Default::default()
    };
    cparams.filters[BLOSC2_MAX_FILTERS - 1] = BLOSC_FILTER_NDCELL;
    assert_eq!(
        blosc2_pure_rs::compress::blosc2_create_cctx_c(cparams).0,
        blosc2_pure_rs::constants::BLOSC2_ERROR_SUCCESS
    );
}

#[test]
fn test_bytedelta_meta_zero_uses_b2nd_schunk_typesize_like_c_fixture() {
    let _b = init_blosc2();

    let meta = B2ndMeta::new(vec![4, 4], vec![4, 4], vec![4, 4], "<u4", 0).unwrap();
    let values: Vec<u32> = (0..16).map(|i| (i * 257 + 13) as u32).collect();
    let data: Vec<u8> = values.into_iter().flat_map(u32::to_ne_bytes).collect();
    assert_c_b2nd_bytedelta_meta_zero_roundtrips(&data);

    let mut filters = [0; BLOSC2_MAX_FILTERS];
    let mut filters_meta = [0; BLOSC2_MAX_FILTERS];
    filters[BLOSC2_MAX_FILTERS - 2] = BLOSC_SHUFFLE;
    filters[BLOSC2_MAX_FILTERS - 1] = BLOSC_FILTER_BYTEDELTA;
    filters_meta[BLOSC2_MAX_FILTERS - 1] = 0;

    let array = B2ndArray::from_dense_buffer(
        meta,
        &data,
        CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 4,
            blocksize: data.len() as i32,
            filters,
            filters_meta,
            ..Default::default()
        },
        DParams::default(),
    )
    .unwrap();

    assert_eq!(array.to_dense_buffer().unwrap(), data);
}

#[test]
fn test_bytedelta_c_fixtures_cross_decode_fixed_and_compat_filters() {
    let _b = init_blosc2();

    for fixture in bytedelta_c_fixtures() {
        assert_eq!(fixture.data.len(), fixture.nbytes(), "{}", fixture.name);
        assert!(fixture.chunksize() > 0, "{}", fixture.name);
        let expected_chunks = c_bytedelta_b2nd_decompressed_chunks(&fixture);
        let crosses_legacy_simd_tail = expected_chunks.iter().any(|chunk| {
            chunk.chunks(fixture.blocksize() as usize).any(|block| {
                let stream_len = block.len() / fixture.typesize as usize;
                stream_len >= 16 && stream_len % 16 != 0
            })
        });
        for write_filter in [BLOSC_FILTER_BYTEDELTA, BLOSC_FILTER_BYTEDELTA_BUGGY] {
            let c_chunks = c_bytedelta_b2nd_compress_chunks(&fixture, write_filter);
            let rust_chunks = rust_bytedelta_b2nd_compress_chunks(&fixture, write_filter);
            assert_eq!(
                c_chunks.len(),
                expected_chunks.len(),
                "{} C bytedelta chunk count mismatch",
                fixture.name
            );
            assert_eq!(
                rust_chunks.len(),
                expected_chunks.len(),
                "{} Rust bytedelta chunk count mismatch",
                fixture.name
            );

            for (nchunk, ((c_chunk, rust_chunk), expected_chunk)) in c_chunks
                .iter()
                .zip(rust_chunks.iter())
                .zip(expected_chunks.iter())
                .enumerate()
            {
                assert_eq!(
                    rust_bytedelta_schunk_decompress_chunk(&fixture, c_chunk, expected_chunk.len()),
                    *expected_chunk,
                    "{} Rust must decode C bytedelta writer filter {write_filter} chunk {nchunk}",
                    fixture.name
                );
                assert_eq!(
                    c_bytedelta_schunk_decompress_chunk(&fixture, rust_chunk, expected_chunk.len()),
                    *expected_chunk,
                    "{} C must decode Rust bytedelta writer filter {write_filter} chunk {nchunk}",
                    fixture.name
                );

                for read_filter in [BLOSC_FILTER_BYTEDELTA, BLOSC_FILTER_BYTEDELTA_BUGGY] {
                    let forced_c_chunk =
                        force_bytedelta_reader_filter(c_chunk.clone(), read_filter);
                    let forced_rust_chunk =
                        force_bytedelta_reader_filter(rust_chunk.clone(), read_filter);
                    let c_forced_self = c_bytedelta_schunk_decompress_chunk(
                        &fixture,
                        &forced_c_chunk,
                        expected_chunk.len(),
                    );
                    let rust_forced_self = rust_bytedelta_schunk_decompress_chunk(
                        &fixture,
                        &forced_rust_chunk,
                        expected_chunk.len(),
                    );
                    let rust_reads_c = rust_bytedelta_schunk_decompress_chunk(
                        &fixture,
                        &forced_c_chunk,
                        expected_chunk.len(),
                    );
                    let c_reads_rust = c_bytedelta_schunk_decompress_chunk(
                        &fixture,
                        &forced_rust_chunk,
                        expected_chunk.len(),
                    );
                    let c_roundtrips = c_forced_self == *expected_chunk;
                    if write_filter == read_filter || !crosses_legacy_simd_tail {
                        assert!(
                            c_roundtrips,
                            "{} C bytedelta must roundtrip for write_filter={write_filter} read_filter={read_filter} chunk {nchunk}",
                            fixture.name
                        );
                    }

                    assert_eq!(
                        rust_forced_self == *expected_chunk,
                        c_roundtrips,
                        "{} Rust forced bytedelta roundtrip status must match C for write_filter={write_filter} read_filter={read_filter} chunk {nchunk}",
                        fixture.name
                    );
                    assert_eq!(
                        rust_forced_self, c_forced_self,
                        "{} forced bytedelta decode mismatch for write_filter={write_filter} read_filter={read_filter} chunk {nchunk}",
                        fixture.name
                    );
                    assert_eq!(
                        rust_reads_c, c_forced_self,
                        "{} Rust forced reader must match C on C chunk for write_filter={write_filter} read_filter={read_filter} chunk {nchunk}",
                        fixture.name
                    );
                    assert_eq!(
                        c_reads_rust, c_forced_self,
                        "{} C forced reader must match C on Rust chunk for write_filter={write_filter} read_filter={read_filter} chunk {nchunk}",
                        fixture.name
                    );
                }
            }
        }
    }
}

#[test]
fn test_int_trunc_matches_c_fixture_outputs() {
    let _b = init_blosc2();

    const C_INT_TRUNC_CHUNKSIZE: usize = 500 * 1000;
    let c_fixture_nchunk = 7;

    let cases: [(usize, u8, Vec<u8>); 4] = [
        (
            8,
            (-50i8) as u8,
            (0..C_INT_TRUNC_CHUNKSIZE)
                .flat_map(|i| {
                    let value = ((i as i64 * c_fixture_nchunk) + i as i64) << (50 - 20);
                    value.to_ne_bytes()
                })
                .collect(),
        ),
        (
            4,
            (-20i8) as u8,
            (0..C_INT_TRUNC_CHUNKSIZE)
                .flat_map(|i| {
                    let value = (i as i32 * c_fixture_nchunk as i32) + i as i32;
                    value.to_ne_bytes()
                })
                .collect(),
        ),
        (
            2,
            (-10i8) as u8,
            (0..C_INT_TRUNC_CHUNKSIZE)
                .flat_map(|i| (i as i16).to_ne_bytes())
                .collect(),
        ),
        (
            1,
            (-5i8) as u8,
            (0..C_INT_TRUNC_CHUNKSIZE)
                .map(|i| (i as i8).to_ne_bytes()[0])
                .collect(),
        ),
    ];

    for (typesize, meta, data) in cases {
        let mut rust_filters = [0; BLOSC2_MAX_FILTERS];
        let mut rust_filters_meta = [0; BLOSC2_MAX_FILTERS];
        rust_filters[BLOSC2_MAX_FILTERS - 1] = BLOSC_FILTER_INT_TRUNC;
        rust_filters_meta[BLOSC2_MAX_FILTERS - 1] = meta;
        let rust_out = rust_schunk_append_decompress_with_filter(
            &data,
            CParams {
                compcode: BLOSC_BLOSCLZ,
                clevel: 9,
                typesize: typesize as i32,
                blocksize: data.len() as i32,
                filters: rust_filters,
                filters_meta: rust_filters_meta,
                ..Default::default()
            },
        )
        .unwrap();

        let mut cparams: ffi::blosc2_cparams = unsafe { std::mem::zeroed() };
        cparams.compcode = BLOSC_BLOSCLZ;
        cparams.clevel = 9;
        cparams.typesize = typesize as i32;
        cparams.blocksize = data.len() as i32;
        cparams.filters[BLOSC2_MAX_FILTERS - 1] = BLOSC_FILTER_INT_TRUNC;
        cparams.filters_meta[BLOSC2_MAX_FILTERS - 1] = meta;
        let c_out = c_schunk_append_decompress_with_filter(&data, cparams);

        let expected = expected_int_trunc_bytes(typesize, meta, &data);
        assert_eq!(c_out, expected, "C int_trunc fixture output mismatch");
        assert_eq!(
            rust_out, c_out,
            "Rust int_trunc output mismatch for typesize={typesize} meta={meta}"
        );
    }
}

#[test]
fn test_standard_filter_pipelines_match_c_decompressed_outputs() {
    let _b = init_blosc2();

    let int_data: Vec<u8> = (0..2048u32)
        .flat_map(|i| {
            let value = i.wrapping_mul(1_664_525).wrapping_add(1_013_904_223) ^ ((i & 7) << 21);
            value.to_ne_bytes()
        })
        .collect();
    let f32_data: Vec<u8> = (0..2048u32)
        .flat_map(|i| {
            let sign = (i & 1) << 31;
            let mantissa = i.wrapping_mul(7_919).wrapping_add(0x12345) & 0x7f_ffff;
            let value = f32::from_bits(sign | 0x3f80_0000 | mantissa);
            value.to_ne_bytes()
        })
        .collect();
    let f64_data: Vec<u8> = (0..1024u64)
        .flat_map(|i| {
            let sign = (i & 1) << 63;
            let mantissa =
                i.wrapping_mul(1_000_003).wrapping_add(0x12345_6789) & 0x000f_ffff_ffff_ffff;
            let value = f64::from_bits(sign | 0x3ff0_0000_0000_0000 | mantissa);
            value.to_ne_bytes()
        })
        .collect();

    let mut cases: Vec<(
        &str,
        i32,
        i32,
        [u8; BLOSC2_MAX_FILTERS],
        [u8; BLOSC2_MAX_FILTERS],
        Vec<u8>,
    )> = Vec::new();

    let mut filters = [0; BLOSC2_MAX_FILTERS];
    filters[BLOSC2_MAX_FILTERS - 1] = BLOSC_DELTA;
    cases.push((
        "delta",
        4,
        320,
        filters,
        [0; BLOSC2_MAX_FILTERS],
        int_data.clone(),
    ));

    let mut filters = [0; BLOSC2_MAX_FILTERS];
    filters[BLOSC2_MAX_FILTERS - 2] = BLOSC_DELTA;
    filters[BLOSC2_MAX_FILTERS - 1] = BLOSC_SHUFFLE;
    cases.push((
        "delta_then_shuffle",
        4,
        512,
        filters,
        [0; BLOSC2_MAX_FILTERS],
        int_data.clone(),
    ));

    let mut filters = [0; BLOSC2_MAX_FILTERS];
    filters[BLOSC2_MAX_FILTERS - 2] = BLOSC_DELTA;
    filters[BLOSC2_MAX_FILTERS - 1] = BLOSC_BITSHUFFLE;
    cases.push((
        "delta_then_bitshuffle",
        4,
        768,
        filters,
        [0; BLOSC2_MAX_FILTERS],
        int_data,
    ));

    let mut filters = [0; BLOSC2_MAX_FILTERS];
    let mut filters_meta = [0; BLOSC2_MAX_FILTERS];
    filters[BLOSC2_MAX_FILTERS - 2] = BLOSC_TRUNC_PREC;
    filters_meta[BLOSC2_MAX_FILTERS - 2] = 16;
    filters[BLOSC2_MAX_FILTERS - 1] = BLOSC_SHUFFLE;
    cases.push((
        "truncprec_f32_then_shuffle",
        4,
        512,
        filters,
        filters_meta,
        f32_data,
    ));

    let mut filters = [0; BLOSC2_MAX_FILTERS];
    let mut filters_meta = [0; BLOSC2_MAX_FILTERS];
    filters[BLOSC2_MAX_FILTERS - 1] = BLOSC_TRUNC_PREC;
    filters_meta[BLOSC2_MAX_FILTERS - 1] = 24;
    cases.push(("truncprec_f64", 8, 1024, filters, filters_meta, f64_data));

    for (name, typesize, blocksize, filters, filters_meta, data) in cases {
        let rust_out = rust_schunk_append_decompress_with_filter(
            &data,
            CParams {
                compcode: BLOSC_LZ4,
                clevel: 5,
                typesize,
                blocksize,
                filters,
                filters_meta,
                ..Default::default()
            },
        )
        .unwrap_or_else(|err| panic!("Rust standard filter pipeline {name} failed: {err}"));

        let mut cparams: ffi::blosc2_cparams = unsafe { std::mem::zeroed() };
        cparams.compcode = BLOSC_LZ4;
        cparams.clevel = 5;
        cparams.typesize = typesize;
        cparams.blocksize = blocksize;
        cparams.filters = filters;
        cparams.filters_meta = filters_meta;
        let c_out = c_schunk_append_decompress_with_filter(&data, cparams);

        assert_eq!(
            rust_out, c_out,
            "Rust standard filter pipeline must match C decompressed output for {name}"
        );
        if filters.contains(&BLOSC_TRUNC_PREC) {
            assert_ne!(
                c_out, data,
                "truncprec fixture {name} should exercise C's lossy filter output"
            );
        } else {
            assert_eq!(
                c_out, data,
                "lossless filter fixture {name} should roundtrip through C"
            );
        }
    }
}

#[test]
fn test_ndcell_b2nd_roundtrips_with_registered_global_filter() {
    let _b = init_blosc2();

    let meta = B2ndMeta::new(vec![4, 4], vec![4, 4], vec![4, 4], "<u4", 0).unwrap();
    let data: Vec<u8> = (0..16u32).flat_map(u32::to_ne_bytes).collect();
    let c_out = c_b2nd_filter_to_cbuffer(
        BLOSC_FILTER_NDCELL,
        2,
        4,
        "<u4",
        &[4, 4],
        &[4, 4],
        &[4, 4],
        &data,
    );
    assert_eq!(c_out, data, "C NDCELL B2ND fixture must be lossless");

    let mut filters = [0; BLOSC2_MAX_FILTERS];
    let mut filters_meta = [0; BLOSC2_MAX_FILTERS];
    filters[BLOSC2_MAX_FILTERS - 1] = BLOSC_FILTER_NDCELL;
    filters_meta[BLOSC2_MAX_FILTERS - 1] = 2;

    let array = B2ndArray::from_dense_buffer(
        meta,
        &data,
        CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 4,
            blocksize: 64,
            filters,
            filters_meta,
            ..Default::default()
        },
        DParams::default(),
    )
    .unwrap();

    assert_eq!(array.to_dense_buffer().unwrap(), c_out);
}

#[test]
fn test_ndmean_b2nd_outputs_cell_means() {
    let _b = init_blosc2();

    let meta = B2ndMeta::new(vec![8], vec![8], vec![8], "<f8", 0).unwrap();
    let values = [1.0f64, 3.0, 5.0, 7.0, 10.0, 14.0, 18.0, 22.0];
    let data: Vec<u8> = values.into_iter().flat_map(f64::to_ne_bytes).collect();
    let expected: Vec<u8> = [4.0f64; 4]
        .into_iter()
        .chain([16.0f64; 4])
        .flat_map(f64::to_ne_bytes)
        .collect();
    let c_out = c_b2nd_filter_to_cbuffer(BLOSC_FILTER_NDMEAN, 4, 8, "<f8", &[8], &[8], &[8], &data);
    assert_eq!(c_out, expected, "C NDMEAN B2ND fixture output changed");

    let mut filters = [0; BLOSC2_MAX_FILTERS];
    let mut filters_meta = [0; BLOSC2_MAX_FILTERS];
    filters[BLOSC2_MAX_FILTERS - 1] = BLOSC_FILTER_NDMEAN;
    filters_meta[BLOSC2_MAX_FILTERS - 1] = 4;

    let array = B2ndArray::from_dense_buffer(
        meta,
        &data,
        CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 8,
            blocksize: 64,
            filters,
            filters_meta,
            ..Default::default()
        },
        DParams::default(),
    )
    .unwrap();

    assert_eq!(array.to_dense_buffer().unwrap(), c_out);
}

#[test]
fn test_shuffle_matches_c() {
    let _b = init_blosc2();

    for typesize in [1, 2, 4, 8, 16] {
        let blocksize = 1024;
        let data: Vec<u8> = (0..blocksize).map(|i| (i * 7 + 13) as u8).collect();

        // Rust shuffle
        let mut rust_out = vec![0u8; blocksize];
        filters::shuffle(typesize, &data, &mut rust_out);

        // C shuffle
        let mut c_out = vec![0u8; blocksize];
        unsafe {
            ffi::blosc2_shuffle(
                typesize as i32,
                blocksize as i32,
                data.as_ptr() as *const _,
                c_out.as_mut_ptr() as *mut _,
            );
        }

        assert_eq!(rust_out, c_out, "Shuffle mismatch for typesize={typesize}");

        // Test unshuffle
        let mut rust_restored = vec![0u8; blocksize];
        filters::unshuffle(typesize, &rust_out, &mut rust_restored);

        let mut c_restored = vec![0u8; blocksize];
        unsafe {
            ffi::blosc2_unshuffle(
                typesize as i32,
                blocksize as i32,
                c_out.as_ptr() as *const _,
                c_restored.as_mut_ptr() as *mut _,
            );
        }

        assert_eq!(
            rust_restored, c_restored,
            "Unshuffle mismatch for typesize={typesize}"
        );
        assert_eq!(
            data, rust_restored,
            "Shuffle roundtrip failed for typesize={typesize}"
        );
    }
}

#[test]
fn test_shuffle_various_sizes() {
    let _b = init_blosc2();

    // Test with sizes that aren't perfect multiples of typesize
    for typesize in [2, 4, 8] {
        for blocksize in [33, 100, 255, 513, 1000, 4096] {
            let data: Vec<u8> = (0..blocksize).map(|i| (i * 3 + 5) as u8).collect();

            let mut rust_out = vec![0u8; blocksize];
            filters::shuffle(typesize, &data, &mut rust_out);

            let mut c_out = vec![0u8; blocksize];
            unsafe {
                ffi::blosc2_shuffle(
                    typesize as i32,
                    blocksize as i32,
                    data.as_ptr() as *const _,
                    c_out.as_mut_ptr() as *mut _,
                );
            }

            assert_eq!(
                rust_out, c_out,
                "Shuffle mismatch for typesize={typesize} blocksize={blocksize}"
            );
        }
    }
}

#[test]
fn test_bitshuffle_matches_c() {
    let _b = init_blosc2();

    for typesize in [1, 2, 4, 8] {
        for blocksize in [128 * typesize, 128 * typesize + 3 * typesize] {
            let data: Vec<u8> = (0..blocksize).map(|i| (i * 11 + 3) as u8).collect();

            // Rust bitshuffle
            let mut rust_out = vec![0u8; blocksize];
            assert_eq!(
                filters::bitshuffle(typesize, &data, &mut rust_out),
                blocksize as i64
            );

            // C bitshuffle
            let mut c_out = vec![0u8; blocksize];
            let c_shuffled = unsafe {
                ffi::blosc2_bitshuffle(
                    typesize as i32,
                    blocksize as i32,
                    data.as_ptr() as *const _,
                    c_out.as_mut_ptr() as *mut _,
                )
            };
            assert_eq!(c_shuffled, blocksize as i32);

            assert_eq!(
                rust_out, c_out,
                "Bitshuffle mismatch for typesize={typesize} blocksize={blocksize}"
            );

            // Test roundtrip and compare C bitunshuffle.
            let mut rust_restored = vec![0u8; blocksize];
            assert_eq!(
                filters::bitunshuffle(typesize, &rust_out, &mut rust_restored),
                blocksize as i64
            );
            assert_eq!(
                data, rust_restored,
                "Bitshuffle roundtrip failed for typesize={typesize} blocksize={blocksize}"
            );

            let mut c_restored = vec![0u8; blocksize];
            let c_unshuffled = unsafe {
                ffi::blosc2_bitunshuffle(
                    typesize as i32,
                    blocksize as i32,
                    c_out.as_ptr() as *const _,
                    c_restored.as_mut_ptr() as *mut _,
                )
            };
            assert_eq!(c_unshuffled, blocksize as i32);
            assert_eq!(
                data, c_restored,
                "C bitunshuffle roundtrip failed for typesize={typesize} blocksize={blocksize}"
            );
        }
    }
}
