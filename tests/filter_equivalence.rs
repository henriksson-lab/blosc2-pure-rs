#![cfg(feature = "_ffi")]
mod common;
use blosc2_pure_rs::b2nd::{B2ndArray, B2ndMeta};
use blosc2_pure_rs::compress::{blosc2_create_cctx, compress, CParams, DParams};
use blosc2_pure_rs::constants::{
    BLOSC2_MAX_FILTERS, BLOSC_ALWAYS_SPLIT, BLOSC_BLOSCLZ, BLOSC_FILTER_BYTEDELTA,
    BLOSC_FILTER_BYTEDELTA_BUGGY, BLOSC_FILTER_INT_TRUNC, BLOSC_FILTER_NDCELL, BLOSC_FILTER_NDMEAN,
    BLOSC_LZ4, BLOSC_SHUFFLE,
};
use blosc2_pure_rs::filters;
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
            name: "ndmean_repart_same_cells",
            filter: BLOSC_FILTER_NDMEAN,
            typesize: 8,
            dtype: "<f8",
            shape: &[128, 64, 32],
            chunkshape: &[32, 32, 16],
            blockshape: &[16, 8, 8],
            compcode: BLOSC_BLOSCLZ,
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
fn test_ndcell_b2nd_roundtrips_with_registered_global_filter() {
    let _b = init_blosc2();

    let meta = B2ndMeta::new(vec![4, 4], vec![4, 4], vec![4, 4], "<u4", 0).unwrap();
    let data: Vec<u8> = (0..16u32).flat_map(u32::to_ne_bytes).collect();
    let mut filters = [0; BLOSC2_MAX_FILTERS];
    let mut filters_meta = [0; BLOSC2_MAX_FILTERS];
    filters[BLOSC2_MAX_FILTERS - 1] = BLOSC_FILTER_NDCELL;
    filters_meta[BLOSC2_MAX_FILTERS - 1] = 2;

    let array = B2ndArray::from_cbuffer(
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

    assert_eq!(array.to_cbuffer().unwrap(), data);
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
    let mut filters = [0; BLOSC2_MAX_FILTERS];
    let mut filters_meta = [0; BLOSC2_MAX_FILTERS];
    filters[BLOSC2_MAX_FILTERS - 1] = BLOSC_FILTER_NDMEAN;
    filters_meta[BLOSC2_MAX_FILTERS - 1] = 4;

    let array = B2ndArray::from_cbuffer(
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

    assert_eq!(array.to_cbuffer().unwrap(), expected);
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
