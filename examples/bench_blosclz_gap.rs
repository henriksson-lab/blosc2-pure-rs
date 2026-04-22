#![cfg(feature = "_ffi")]

#[path = "../tests/common/mod.rs"]
mod common;

use blosc2_pure_rs::compress::{self, CParams};
use blosc2_pure_rs::constants::*;
use blosc2_pure_rs::filters;
use common::ffi;
use std::time::Instant;

const DATA_SIZE: usize = 10 * 1024 * 1024;

fn signal_f32_bytes(len: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(len);
    let mut state = 0x1234_5678_u32;
    for i in 0..(len / 4) {
        state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
        let noise = ((state >> 8) as f32 / 16_777_216.0 - 0.5) * 0.01;
        let x = (i as f32 * 0.01).sin() + (i as f32 * 0.001).sin() * 0.25 + noise;
        out.extend_from_slice(&x.to_le_bytes());
    }
    out.resize(len, 0);
    out
}

fn median(mut xs: Vec<f64>) -> f64 {
    xs.sort_by(|a, b| a.partial_cmp(b).unwrap());
    xs[xs.len() / 2]
}

fn mib_per_s(bytes: usize, secs: f64) -> f64 {
    (bytes as f64 / (1024.0 * 1024.0)) / secs
}

fn rust_cparams(filter: u8) -> CParams {
    CParams {
        compcode: BLOSC_BLOSCLZ,
        compcode_meta: 0,
        clevel: 5,
        typesize: 4,
        blocksize: 0,
        splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
        filters: [0, 0, 0, 0, 0, filter],
        filters_meta: [0; BLOSC2_MAX_FILTERS],
        use_dict: false,
        nthreads: 1,
        ..Default::default()
    }
}

unsafe fn c_cparams(filter: u8) -> ffi::blosc2_cparams {
    let mut cparams: ffi::blosc2_cparams = std::mem::zeroed();
    cparams.compcode = ffi::BLOSC_BLOSCLZ as u8;
    cparams.clevel = 5;
    cparams.typesize = 4;
    cparams.nthreads = 1;
    cparams.blocksize = 0;
    cparams.splitmode = ffi::BLOSC_FORWARD_COMPAT_SPLIT as i32;
    cparams.filters[ffi::BLOSC2_MAX_FILTERS as usize - 1] = filter;
    cparams
}

fn bench_rust(data: &[u8], filter: u8, iters: usize) -> (usize, f64, f64) {
    let cparams = rust_cparams(filter);
    let compressed = compress::compress(data, &cparams).unwrap();
    let restored = compress::decompress(&compressed).unwrap();
    assert_eq!(restored, data);

    let mut c_times = Vec::with_capacity(iters);
    for _ in 0..iters {
        let start = Instant::now();
        let out = compress::compress(data, &cparams).unwrap();
        c_times.push(start.elapsed().as_secs_f64());
        std::hint::black_box(out);
    }

    let mut d_times = Vec::with_capacity(iters);
    for _ in 0..iters {
        let start = Instant::now();
        let out = compress::decompress(&compressed).unwrap();
        d_times.push(start.elapsed().as_secs_f64());
        std::hint::black_box(out);
    }

    (
        compressed.len(),
        mib_per_s(data.len(), median(c_times)),
        mib_per_s(data.len(), median(d_times)),
    )
}

fn bench_c(data: &[u8], filter: u8, iters: usize) -> (usize, f64, f64) {
    let _b = common::Blosc2::new();

    let src_size = data.len() as i32;
    let mut compressed = vec![0u8; data.len() + 4096];
    let csize = unsafe {
        let cctx = ffi::blosc2_create_cctx(c_cparams(filter));
        assert!(!cctx.is_null());
        let n = ffi::blosc2_compress_ctx(
            cctx,
            data.as_ptr() as *const _,
            src_size,
            compressed.as_mut_ptr() as *mut _,
            compressed.len() as i32,
        );
        ffi::blosc2_free_ctx(cctx);
        n
    };
    assert!(csize > 0, "c compression failed: {csize}");
    compressed.truncate(csize as usize);

    let mut restored = vec![0u8; data.len()];
    let dsize = unsafe {
        let mut dparams: ffi::blosc2_dparams = std::mem::zeroed();
        dparams.nthreads = 1;
        let dctx = ffi::blosc2_create_dctx(dparams);
        assert!(!dctx.is_null());
        let n = ffi::blosc2_decompress_ctx(
            dctx,
            compressed.as_ptr() as *const _,
            compressed.len() as i32,
            restored.as_mut_ptr() as *mut _,
            restored.len() as i32,
        );
        ffi::blosc2_free_ctx(dctx);
        n
    };
    assert_eq!(dsize, src_size);
    assert_eq!(restored, data);

    let mut c_times = Vec::with_capacity(iters);
    for _ in 0..iters {
        let mut out = vec![0u8; data.len() + 4096];
        let start = Instant::now();
        let n = unsafe {
            let cctx = ffi::blosc2_create_cctx(c_cparams(filter));
            assert!(!cctx.is_null());
            let n = ffi::blosc2_compress_ctx(
                cctx,
                data.as_ptr() as *const _,
                src_size,
                out.as_mut_ptr() as *mut _,
                out.len() as i32,
            );
            ffi::blosc2_free_ctx(cctx);
            n
        };
        c_times.push(start.elapsed().as_secs_f64());
        assert!(n > 0);
        std::hint::black_box(out);
    }

    let mut d_times = Vec::with_capacity(iters);
    for _ in 0..iters {
        let mut out = vec![0u8; data.len()];
        let start = Instant::now();
        let n = unsafe {
            let mut dparams: ffi::blosc2_dparams = std::mem::zeroed();
            dparams.nthreads = 1;
            let dctx = ffi::blosc2_create_dctx(dparams);
            assert!(!dctx.is_null());
            let n = ffi::blosc2_decompress_ctx(
                dctx,
                compressed.as_ptr() as *const _,
                compressed.len() as i32,
                out.as_mut_ptr() as *mut _,
                out.len() as i32,
            );
            ffi::blosc2_free_ctx(dctx);
            n
        };
        d_times.push(start.elapsed().as_secs_f64());
        assert_eq!(n, src_size);
        std::hint::black_box(out);
    }

    (
        compressed.len(),
        mib_per_s(data.len(), median(c_times)),
        mib_per_s(data.len(), median(d_times)),
    )
}

fn scalar_unshuffle4(src: &[u8], dest: &mut [u8]) {
    let nelements = src.len() / 4;
    let main_len = nelements * 4;
    let (s0, rest) = src[..main_len].split_at(nelements);
    let (s1, rest) = rest.split_at(nelements);
    let (s2, s3) = rest.split_at(nelements);
    unsafe {
        let out = dest.as_mut_ptr();
        for i in 0..nelements {
            let value = u32::from_ne_bytes([s0[i], s1[i], s2[i], s3[i]]);
            std::ptr::write_unaligned(out.add(i * 4).cast::<u32>(), value);
        }
    }
    dest[main_len..src.len()].copy_from_slice(&src[main_len..]);
}

fn bench_unshuffle(data: &[u8], iters: usize) -> (f64, f64) {
    let mut shuffled = vec![0u8; data.len()];
    filters::shuffle(4, data, &mut shuffled);

    let mut scalar_times = Vec::with_capacity(iters);
    let mut scalar_dest = vec![0u8; data.len()];
    for _ in 0..iters {
        let start = Instant::now();
        scalar_unshuffle4(&shuffled, &mut scalar_dest);
        scalar_times.push(start.elapsed().as_secs_f64());
    }
    assert_eq!(scalar_dest, data);

    let mut dispatch_times = Vec::with_capacity(iters);
    let mut dispatch_dest = vec![0u8; data.len()];
    for _ in 0..iters {
        let start = Instant::now();
        filters::unshuffle(4, &shuffled, &mut dispatch_dest);
        dispatch_times.push(start.elapsed().as_secs_f64());
    }
    assert_eq!(dispatch_dest, data);

    (
        mib_per_s(data.len(), median(scalar_times)),
        mib_per_s(data.len(), median(dispatch_times)),
    )
}

fn main() {
    let data = signal_f32_bytes(DATA_SIZE);
    for (label, filter) in [("nofilter", BLOSC_NOFILTER), ("shuffle", BLOSC_SHUFFLE)] {
        let (rust_size, rust_c, rust_d) = bench_rust(&data, filter, 20);
        let (c_size, c_c, c_d) = bench_c(&data, filter, 20);
        println!(
            "blosclz/{label}: csize rust={rust_size} c={c_size}; compress MB/s rust={rust_c:.1} c={c_c:.1}; decompress MB/s rust={rust_d:.1} c={c_d:.1}"
        );
    }

    let (scalar, dispatch) = bench_unshuffle(&data, 200);
    println!("unshuffle4: scalar={scalar:.1} MB/s dispatch={dispatch:.1} MB/s");
}
