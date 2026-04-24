#[cfg(feature = "compare-blosc2-rs")]
mod enabled {
    use blosc2::{self, CLevel, Codec, Filter};
    use blosc2_pure_rs::compress::{self, CParams as RustCParams};
    use blosc2_pure_rs::constants::{
        BLOSC2_MAX_FILTERS, BLOSC_BLOSCLZ, BLOSC_FORWARD_COMPAT_SPLIT, BLOSC_LZ4, BLOSC_NOFILTER,
        BLOSC_SHUFFLE, BLOSC_ZSTD,
    };
    use std::env;
    use std::time::Instant;

    const DATA_SIZE: usize = 10 * 1024 * 1024;
    const ITERS: usize = 20;

    fn iterations() -> usize {
        env::var("BLOSC2_COMPARE_ITERS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|&n| n > 0)
            .unwrap_or(ITERS)
    }

    fn show_extra_nofilter_decompress_metrics() -> bool {
        env::var("BLOSC2_COMPARE_EXTRA_NOFILTER")
            .ok()
            .map(|v| !matches!(v.as_str(), "0" | "false" | "False" | "FALSE"))
            .unwrap_or(true)
    }

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

    fn rust_cparams(codec: u8, filter: u8, nthreads: usize) -> RustCParams {
        RustCParams {
            compcode: codec,
            compcode_meta: 0,
            clevel: 5,
            typesize: 4,
            blocksize: 0,
            splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
            filters: [0, 0, 0, 0, 0, filter],
            filters_meta: [0; BLOSC2_MAX_FILTERS],
            use_dict: false,
            nthreads: nthreads.try_into().unwrap(),
            ..Default::default()
        }
    }

    fn c_codec(codec: u8) -> Codec {
        match codec {
            BLOSC_BLOSCLZ => Codec::BloscLz,
            BLOSC_LZ4 => Codec::LZ4,
            BLOSC_ZSTD => Codec::ZSTD,
            other => panic!("unsupported codec code for comparison: {other}"),
        }
    }

    fn c_filter(filter: u8) -> Filter {
        match filter {
            BLOSC_NOFILTER => Filter::NoFilter,
            BLOSC_SHUFFLE => Filter::Shuffle,
            other => panic!("unsupported filter code for comparison: {other}"),
        }
    }

    fn bench_rust(data: &[u8], codec: u8, filter: u8, nthreads: usize) -> (usize, f64, f64) {
        let cparams = rust_cparams(codec, filter, nthreads);
        let compressed = compress::compress(data, &cparams).unwrap();
        if !compress_only() {
            let restored =
                compress::decompress_with_threads(&compressed, nthreads.try_into().unwrap())
                    .unwrap();
            assert_eq!(restored, data);
        }

        let iters = iterations();
        let mut c_times = Vec::with_capacity(iters);
        if !decompress_only() {
            for _ in 0..iters {
                let start = Instant::now();
                let out = compress::compress(data, &cparams).unwrap();
                c_times.push(start.elapsed().as_secs_f64());
                std::hint::black_box(out);
            }
        }

        let mut d_times = Vec::with_capacity(iters);
        if !compress_only() {
            for _ in 0..iters {
                let start = Instant::now();
                let out =
                    compress::decompress_with_threads(&compressed, nthreads.try_into().unwrap())
                        .unwrap();
                d_times.push(start.elapsed().as_secs_f64());
                std::hint::black_box(out);
            }
        }

        (
            compressed.len(),
            if c_times.is_empty() {
                0.0
            } else {
                mib_per_s(data.len(), median(c_times))
            },
            if d_times.is_empty() {
                0.0
            } else {
                mib_per_s(data.len(), median(d_times))
            },
        )
    }

    fn bench_rust_into(data: &[u8], codec: u8, filter: u8, nthreads: usize) -> (usize, f64) {
        let cparams = rust_cparams(codec, filter, nthreads);
        let compressed = compress::compress(data, &cparams).unwrap();
        let mut restored = vec![0u8; data.len()];
        let written = compress::decompress_into_with_threads(
            &compressed,
            &mut restored,
            nthreads.try_into().unwrap(),
        )
        .unwrap();
        assert_eq!(written, data.len());
        assert_eq!(restored, data);

        let iters = iterations();
        let mut d_times = Vec::with_capacity(iters);
        let mut out = vec![0u8; data.len()];
        for _ in 0..iters {
            let start = Instant::now();
            let written = compress::decompress_into_with_threads(
                &compressed,
                &mut out,
                nthreads.try_into().unwrap(),
            )
            .unwrap();
            d_times.push(start.elapsed().as_secs_f64());
            std::hint::black_box(written);
        }

        (compressed.len(), mib_per_s(data.len(), median(d_times)))
    }

    fn bench_blosc1_into(data: &[u8], codec: u8, filter: u8, nthreads: usize) -> (usize, f64) {
        let cparams = rust_cparams(codec, filter, nthreads);
        let compressed = compress::compress(data, &cparams).unwrap();
        let mut restored = vec![0u8; data.len()];
        let written = compress::blosc1_decompress(&compressed, &mut restored).unwrap();
        assert_eq!(written, data.len());
        assert_eq!(restored, data);

        let prev = compress::blosc2_set_nthreads(nthreads.try_into().unwrap());
        let iters = iterations();
        let mut d_times = Vec::with_capacity(iters);
        let mut out = vec![0u8; data.len()];
        for _ in 0..iters {
            let start = Instant::now();
            let written = compress::blosc1_decompress(&compressed, &mut out).unwrap();
            d_times.push(start.elapsed().as_secs_f64());
            std::hint::black_box(written);
        }
        let _ = compress::blosc2_set_nthreads(prev);

        (compressed.len(), mib_per_s(data.len(), median(d_times)))
    }

    fn bench_c(data: &[u8], codec: u8, filter: u8, nthreads: usize) -> (usize, f64, f64) {
        blosc2::set_nthreads(nthreads);
        let compressed = blosc2::compress(
            data,
            Some(4),
            Some(CLevel::Five),
            Some(c_filter(filter)),
            Some(c_codec(codec)),
        )
        .unwrap();
        if !compress_only() {
            let restored = blosc2::decompress::<u8>(&compressed).unwrap();
            assert_eq!(restored, data);
        }

        let iters = iterations();
        let mut c_times = Vec::with_capacity(iters);
        if !decompress_only() {
            for _ in 0..iters {
                let start = Instant::now();
                let out = blosc2::compress(
                    data,
                    Some(4),
                    Some(CLevel::Five),
                    Some(c_filter(filter)),
                    Some(c_codec(codec)),
                )
                .unwrap();
                c_times.push(start.elapsed().as_secs_f64());
                std::hint::black_box(out);
            }
        }

        let mut d_times = Vec::with_capacity(iters);
        if !compress_only() {
            for _ in 0..iters {
                let start = Instant::now();
                let out = blosc2::decompress::<u8>(&compressed).unwrap();
                d_times.push(start.elapsed().as_secs_f64());
                std::hint::black_box(out);
            }
        }

        (
            compressed.len(),
            if c_times.is_empty() {
                0.0
            } else {
                mib_per_s(data.len(), median(c_times))
            },
            if d_times.is_empty() {
                0.0
            } else {
                mib_per_s(data.len(), median(d_times))
            },
        )
    }

    fn run_case(label: &str, data: &[u8], codec: u8, filter: u8, nthreads: usize) {
        if run_rust_only() {
            let (rust_size, rust_c, rust_d) = bench_rust(data, codec, filter, nthreads);
            if compress_only() {
                println!(
                    "{label} @ {nthreads} thread(s): csize pure={rust_size}; compress MB/s pure={rust_c:.1}"
                );
            } else if decompress_only() {
                println!(
                    "{label} @ {nthreads} thread(s): csize pure={rust_size}; decompress MB/s pure={rust_d:.1}"
                );
            } else {
                println!(
                    "{label} @ {nthreads} thread(s): csize pure={rust_size}; compress MB/s pure={rust_c:.1}; decompress MB/s pure={rust_d:.1}"
                );
            }
            if !compress_only()
                && codec == BLOSC_BLOSCLZ
                && filter == BLOSC_NOFILTER
                && show_extra_nofilter_decompress_metrics()
            {
                let (_, rust_into_d) = bench_rust_into(data, codec, filter, nthreads);
                let (_, blosc1_into_d) = bench_blosc1_into(data, codec, filter, nthreads);
                println!("{label} @ {nthreads} thread(s): pure decompress_into MB/s={rust_into_d:.1}");
                println!(
                    "{label} @ {nthreads} thread(s): pure blosc1_decompress MB/s={blosc1_into_d:.1}"
                );
            }
            return;
        }

        if run_c_only() {
            let (c_size, c_c, c_d) = bench_c(data, codec, filter, nthreads);
            if compress_only() {
                println!(
                    "{label} @ {nthreads} thread(s): csize blosc2-rs={c_size}; compress MB/s blosc2-rs={c_c:.1}"
                );
            } else if decompress_only() {
                println!(
                    "{label} @ {nthreads} thread(s): csize blosc2-rs={c_size}; decompress MB/s blosc2-rs={c_d:.1}"
                );
            } else {
                println!(
                    "{label} @ {nthreads} thread(s): csize blosc2-rs={c_size}; compress MB/s blosc2-rs={c_c:.1}; decompress MB/s blosc2-rs={c_d:.1}"
                );
            }
            return;
        }

        let (rust_size, rust_c, rust_d) = bench_rust(data, codec, filter, nthreads);
        let (c_size, c_c, c_d) = bench_c(data, codec, filter, nthreads);
        if compress_only() {
            println!(
                "{label} @ {nthreads} thread(s): csize pure={rust_size} blosc2-rs={c_size}; compress MB/s pure={rust_c:.1} blosc2-rs={c_c:.1}"
            );
        } else if decompress_only() {
            println!(
                "{label} @ {nthreads} thread(s): csize pure={rust_size} blosc2-rs={c_size}; decompress MB/s pure={rust_d:.1} blosc2-rs={c_d:.1}"
            );
        } else {
            println!(
                "{label} @ {nthreads} thread(s): csize pure={rust_size} blosc2-rs={c_size}; compress MB/s pure={rust_c:.1} blosc2-rs={c_c:.1}; decompress MB/s pure={rust_d:.1} blosc2-rs={c_d:.1}"
            );
        }
        if !compress_only()
            && codec == BLOSC_BLOSCLZ
            && filter == BLOSC_NOFILTER
            && show_extra_nofilter_decompress_metrics()
        {
            let (_, rust_into_d) = bench_rust_into(data, codec, filter, nthreads);
            let (_, blosc1_into_d) = bench_blosc1_into(data, codec, filter, nthreads);
            println!("{label} @ {nthreads} thread(s): pure decompress_into MB/s={rust_into_d:.1}");
            println!("{label} @ {nthreads} thread(s): pure blosc1_decompress MB/s={blosc1_into_d:.1}");
        }
    }

    fn selected_case() -> Option<String> {
        env::var("BLOSC2_COMPARE_CASE")
            .ok()
            .map(|s| s.trim().to_ascii_lowercase())
            .filter(|s| !s.is_empty())
    }

    fn selected_threads() -> Option<usize> {
        env::var("BLOSC2_COMPARE_THREADS")
            .ok()
            .and_then(|s| s.trim().parse::<usize>().ok())
            .filter(|&n| n > 0)
    }

    fn selected_impl() -> Option<String> {
        env::var("BLOSC2_COMPARE_IMPL")
            .ok()
            .map(|s| s.trim().to_ascii_lowercase())
            .filter(|s| !s.is_empty())
    }

    fn selected_mode() -> Option<String> {
        env::var("BLOSC2_COMPARE_MODE")
            .ok()
            .map(|s| s.trim().to_ascii_lowercase())
            .filter(|s| !s.is_empty())
    }

    fn case_enabled(label: &str, nthreads: usize) -> bool {
        if let Some(selected) = selected_case() {
            if label.to_ascii_lowercase() != selected {
                return false;
            }
        }
        if let Some(selected) = selected_threads() {
            if nthreads != selected {
                return false;
            }
        }
        true
    }

    fn run_rust_only() -> bool {
        matches!(selected_impl().as_deref(), Some("pure") | Some("rust"))
    }

    fn run_c_only() -> bool {
        matches!(selected_impl().as_deref(), Some("c") | Some("blosc2-rs"))
    }

    fn compress_only() -> bool {
        matches!(selected_mode().as_deref(), Some("compress"))
    }

    fn decompress_only() -> bool {
        matches!(selected_mode().as_deref(), Some("decompress"))
    }

    pub fn main() {
        let data = signal_f32_bytes(DATA_SIZE);
        for nthreads in [1, 4] {
            if case_enabled("blosclz/nofilter", nthreads) {
                run_case(
                    "blosclz/nofilter",
                    &data,
                    BLOSC_BLOSCLZ,
                    BLOSC_NOFILTER,
                    nthreads,
                );
            }
            if case_enabled("blosclz/shuffle", nthreads) {
                run_case(
                    "blosclz/shuffle",
                    &data,
                    BLOSC_BLOSCLZ,
                    BLOSC_SHUFFLE,
                    nthreads,
                );
            }
            if case_enabled("lz4/shuffle", nthreads) {
                run_case("lz4/shuffle", &data, BLOSC_LZ4, BLOSC_SHUFFLE, nthreads);
            }
            if case_enabled("zstd/shuffle", nthreads) {
                run_case("zstd/shuffle", &data, BLOSC_ZSTD, BLOSC_SHUFFLE, nthreads);
            }
        }
    }
}

#[cfg(feature = "compare-blosc2-rs")]
fn main() {
    enabled::main();
}

#[cfg(not(feature = "compare-blosc2-rs"))]
fn main() {}
