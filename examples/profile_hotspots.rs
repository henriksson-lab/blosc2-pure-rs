use blosc2_pure_rs::compress::{self, CParams};
use blosc2_pure_rs::constants::*;
use std::hint::black_box;

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

fn random_bytes(len: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(len);
    let mut state = 0x1234_5678_9abc_def0_u64;
    for _ in 0..len {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        out.push((state >> 32) as u8);
    }
    out
}

fn cparams(compcode: u8, typesize: i32) -> CParams {
    CParams {
        compcode,
        compcode_meta: 0,
        clevel: 9,
        typesize,
        blocksize: 0,
        splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
        filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
        filters_meta: [0; BLOSC2_MAX_FILTERS],
        use_dict: false,
        nthreads: 4,
        ..Default::default()
    }
}

fn cparams_nofilter(compcode: u8, typesize: i32) -> CParams {
    CParams {
        compcode,
        compcode_meta: 0,
        clevel: 9,
        typesize,
        blocksize: 0,
        splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
        filters: [0; BLOSC2_MAX_FILTERS],
        filters_meta: [0; BLOSC2_MAX_FILTERS],
        use_dict: false,
        nthreads: 4,
        ..Default::default()
    }
}

fn run_compress(label: &str, data: &[u8], params: &CParams, iterations: usize) {
    let mut total = 0usize;
    for _ in 0..iterations {
        let compressed = compress::compress(black_box(data), black_box(params)).unwrap();
        total = total.wrapping_add(compressed.len());
        black_box(&compressed);
    }
    eprintln!("{label}: {iterations} compress iterations, checksum={total}");
}

fn run_decompress(label: &str, data: &[u8], params: &CParams, iterations: usize) {
    let compressed = compress::compress(data, params).unwrap();
    let mut total = 0usize;
    for _ in 0..iterations {
        let decompressed =
            compress::decompress_with_threads(black_box(&compressed), params.nthreads).unwrap();
        total = total.wrapping_add(decompressed.len());
        black_box(&decompressed);
    }
    eprintln!("{label}: {iterations} decompress iterations, checksum={total}");
}

fn print_usage(program: &str) {
    eprintln!("Usage: {program} <case> [iterations]");
    eprintln!("Cases:");
    eprintln!("  blosclz-t4-signal-compress");
    eprintln!("  lz4-t4-signal-compress");
    eprintln!("  zlib-t4-signal-compress");
    eprintln!("  random-blosclz-t1-compress");
    eprintln!("  random-lz4-t4-compress");
    eprintln!("  blosclz-t4-signal-decompress");
    eprintln!("  blosclz-t4-signal-nofilter-decompress");
    eprintln!("  lz4-t4-signal-decompress");
}

fn main() {
    let mut args = std::env::args();
    let program = args.next().unwrap_or_else(|| "profile_hotspots".into());
    let Some(case) = args.next() else {
        print_usage(&program);
        std::process::exit(2);
    };
    let iterations = args
        .next()
        .map(|s| s.parse::<usize>())
        .transpose()
        .expect("iterations must be an integer")
        .unwrap_or(100);

    match case.as_str() {
        "blosclz-t4-signal-compress" => run_compress(
            &case,
            &signal_f32_bytes(DATA_SIZE),
            &cparams(BLOSC_BLOSCLZ, 4),
            iterations,
        ),
        "lz4-t4-signal-compress" => run_compress(
            &case,
            &signal_f32_bytes(DATA_SIZE),
            &cparams(BLOSC_LZ4, 4),
            iterations,
        ),
        "zlib-t4-signal-compress" => run_compress(
            &case,
            &signal_f32_bytes(DATA_SIZE),
            &cparams(BLOSC_ZLIB, 4),
            iterations,
        ),
        "random-blosclz-t1-compress" => run_compress(
            &case,
            &random_bytes(DATA_SIZE),
            &cparams(BLOSC_BLOSCLZ, 1),
            iterations,
        ),
        "random-lz4-t4-compress" => run_compress(
            &case,
            &random_bytes(DATA_SIZE),
            &cparams(BLOSC_LZ4, 4),
            iterations,
        ),
        "blosclz-t4-signal-decompress" => run_decompress(
            &case,
            &signal_f32_bytes(DATA_SIZE),
            &cparams(BLOSC_BLOSCLZ, 4),
            iterations,
        ),
        "blosclz-t4-signal-nofilter-decompress" => run_decompress(
            &case,
            &signal_f32_bytes(DATA_SIZE),
            &cparams_nofilter(BLOSC_BLOSCLZ, 4),
            iterations,
        ),
        "lz4-t4-signal-decompress" => run_decompress(
            &case,
            &signal_f32_bytes(DATA_SIZE),
            &cparams(BLOSC_LZ4, 4),
            iterations,
        ),
        _ => {
            eprintln!("Unknown case: {case}");
            print_usage(&program);
            std::process::exit(2);
        }
    }
}
