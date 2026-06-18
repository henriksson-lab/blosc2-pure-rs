#![cfg(feature = "_ffi")]
//! Regression guard: Rust (lz4-pure-rs) and C LZ4/LZ4HC must produce
//! byte-for-byte identical *compressed* chunks, across inputs and clevels.
use blosc2_pure_rs::compress::{compress, CParams};
use blosc2_pure_rs::constants::*;
mod common;
use common::ffi;

fn c_compress(data: &[u8], compcode: u8, clevel: u8) -> Vec<u8> {
    let mut out = vec![0u8; data.len() + BLOSC2_MAX_OVERHEAD + 4096];
    let csize = unsafe {
        let mut cparams: ffi::blosc2_cparams = std::mem::zeroed();
        cparams.compcode = compcode;
        cparams.clevel = clevel;
        cparams.typesize = 4;
        cparams.nthreads = 1;
        cparams.splitmode = BLOSC_FORWARD_COMPAT_SPLIT;
        cparams.filters[BLOSC2_MAX_FILTERS - 1] = BLOSC_SHUFFLE;
        let cctx = ffi::blosc2_create_cctx(cparams);
        let r = ffi::blosc2_compress_ctx(
            cctx,
            data.as_ptr() as *const _,
            data.len() as i32,
            out.as_mut_ptr() as *mut _,
            out.len() as i32,
        );
        ffi::blosc2_free_ctx(cctx);
        r
    };
    assert!(
        csize > 0,
        "C compress failed codec={compcode} clevel={clevel}"
    );
    out.truncate(csize as usize);
    out
}

fn rust_compress(data: &[u8], compcode: u8, clevel: u8) -> Vec<u8> {
    let cparams = CParams {
        compcode,
        clevel,
        typesize: 4,
        splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
        filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
        ..Default::default()
    };
    compress(data, &cparams).expect("rust compress failed")
}

fn datasets() -> Vec<(&'static str, Vec<u8>)> {
    vec![
        (
            "seq_u32_10k",
            (0..10_000u32).flat_map(|i| i.to_le_bytes()).collect(),
        ),
        (
            "lowdiv",
            (0..200_000usize)
                .map(|i| [0x11u8, 0x22, 0x33, 0x44][i % 4])
                .collect(),
        ),
        (
            "noisy",
            (0..120_000u32)
                .flat_map(|i| ((i.wrapping_mul(2654435761)) ^ (i >> 3)).to_le_bytes())
                .collect(),
        ),
        ("zeros", vec![0u8; 64_000]),
    ]
}

/// Rust and C LZ4/LZ4HC compressed chunks must be byte-identical.
#[test]
fn lz4_compressed_bytes_match_c() {
    let _b = common::Blosc2::new();
    for compcode in [BLOSC_LZ4, BLOSC_LZ4HC] {
        for clevel in [1u8, 5, 9] {
            for (name, data) in datasets() {
                let c = c_compress(&data, compcode, clevel);
                let r = rust_compress(&data, compcode, clevel);
                let first_diff = c
                    .iter()
                    .zip(r.iter())
                    .position(|(a, b)| a != b)
                    .map(|p| p as i64)
                    .unwrap_or(-1);
                assert_eq!(
                    r,
                    c,
                    "codec={compcode} clevel={clevel} data={name}: \
                     rust={} bytes, c={} bytes, first diff at offset {first_diff}",
                    r.len(),
                    c.len()
                );
            }
        }
    }
}
