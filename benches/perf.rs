use blosc2_pure_rs::codecs;
use blosc2_pure_rs::compress::{self, CParams, DParams};
use blosc2_pure_rs::constants::*;
use blosc2_pure_rs::filters;
use blosc2_pure_rs::schunk::Schunk;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::path::Path;
use std::process::Command;

const DATA_SIZE: usize = 10 * 1024 * 1024;
const CHUNK_SIZE: usize = 1024 * 1024;

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

fn cparams(compcode: u8, typesize: i32, filter: u8) -> CParams {
    CParams {
        compcode,
        compcode_meta: 0,
        clevel: 9,
        typesize,
        blocksize: 0,
        splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
        filters: [0, 0, 0, 0, 0, filter],
        filters_meta: [0; BLOSC2_MAX_FILTERS],
        use_dict: false,
        nthreads: 1,
    }
}

fn bench_filters(c: &mut Criterion) {
    let data = signal_f32_bytes(CHUNK_SIZE);
    let mut group = c.benchmark_group("filters");
    group.throughput(Throughput::Bytes(data.len() as u64));

    for typesize in [1usize, 2, 4, 8] {
        let mut shuffled = vec![0; data.len()];
        filters::shuffle(typesize, &data, &mut shuffled);

        group.bench_with_input(
            BenchmarkId::new("shuffle", typesize),
            &typesize,
            |b, &ts| {
                b.iter_batched(
                    || vec![0; data.len()],
                    |mut dest| filters::shuffle(ts, black_box(&data), black_box(&mut dest)),
                    criterion::BatchSize::SmallInput,
                );
            },
        );

        group.bench_with_input(
            BenchmarkId::new("unshuffle", typesize),
            &typesize,
            |b, &ts| {
                b.iter_batched(
                    || vec![0; shuffled.len()],
                    |mut dest| filters::unshuffle(ts, black_box(&shuffled), black_box(&mut dest)),
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }

    for typesize in [1usize, 2, 4, 8] {
        let mut bitshuffled = vec![0; data.len()];
        filters::bitshuffle(typesize, &data, &mut bitshuffled);

        group.bench_with_input(
            BenchmarkId::new("bitshuffle", typesize),
            &typesize,
            |b, &ts| {
                b.iter_batched(
                    || vec![0; data.len()],
                    |mut dest| {
                        black_box(filters::bitshuffle(
                            ts,
                            black_box(&data),
                            black_box(&mut dest),
                        ))
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );

        group.bench_with_input(
            BenchmarkId::new("bitunshuffle", typesize),
            &typesize,
            |b, &ts| {
                b.iter_batched(
                    || vec![0; bitshuffled.len()],
                    |mut dest| {
                        black_box(filters::bitunshuffle(
                            ts,
                            black_box(&bitshuffled),
                            black_box(&mut dest),
                        ))
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

fn bench_codec_blocks(c: &mut Criterion) {
    let signal = signal_f32_bytes(CHUNK_SIZE);
    let random = random_bytes(CHUNK_SIZE);
    let mut group = c.benchmark_group("codec_blocks");
    group.throughput(Throughput::Bytes(signal.len() as u64));

    for (dataset, data) in [("signal", &signal), ("random", &random)] {
        for (name, compcode) in [
            ("blosclz", BLOSC_BLOSCLZ),
            ("lz4", BLOSC_LZ4),
            ("zlib", BLOSC_ZLIB),
            ("zstd", BLOSC_ZSTD),
        ] {
            let maxout = data.len() + BLOSC2_MAX_OVERHEAD;
            let mut compressed = vec![0; maxout];
            let csize = codecs::compress_block(compcode, 9, data, &mut compressed);
            assert!(csize > 0, "{name} failed to prepare compressed block");
            compressed.truncate(csize as usize);

            group.bench_function(format!("{dataset}/{name}/compress"), |b| {
                b.iter_batched(
                    || vec![0; maxout],
                    |mut dest| {
                        black_box(codecs::compress_block(
                            compcode,
                            9,
                            black_box(data),
                            black_box(&mut dest),
                        ))
                    },
                    criterion::BatchSize::SmallInput,
                );
            });

            group.bench_function(format!("{dataset}/{name}/decompress"), |b| {
                b.iter_batched(
                    || vec![0; data.len()],
                    |mut dest| {
                        black_box(codecs::decompress_block(
                            compcode,
                            black_box(&compressed),
                            black_box(&mut dest),
                        ))
                    },
                    criterion::BatchSize::SmallInput,
                );
            });
        }
    }

    group.finish();
}

fn bench_chunks(c: &mut Criterion) {
    let signal = signal_f32_bytes(DATA_SIZE);
    let mut group = c.benchmark_group("chunks");
    group.throughput(Throughput::Bytes(signal.len() as u64));

    for (name, compcode) in [
        ("blosclz_t4", BLOSC_BLOSCLZ),
        ("lz4_t4", BLOSC_LZ4),
        ("zlib_t4", BLOSC_ZLIB),
        ("zstd_t4", BLOSC_ZSTD),
    ] {
        let params = cparams(compcode, 4, BLOSC_SHUFFLE);
        let compressed = compress::compress(&signal, &params).unwrap();

        group.bench_function(format!("{name}/compress"), |b| {
            b.iter(|| {
                black_box(compress::compress(black_box(&signal), black_box(&params)).unwrap())
            });
        });

        group.bench_function(format!("{name}/decompress"), |b| {
            b.iter(|| black_box(compress::decompress(black_box(&compressed)).unwrap()));
        });
    }

    group.finish();
}

fn bench_schunk_frame(c: &mut Criterion) {
    let data = signal_f32_bytes(DATA_SIZE);
    let chunks: Vec<&[u8]> = data.chunks(CHUNK_SIZE).collect();
    let params = cparams(BLOSC_BLOSCLZ, 4, BLOSC_SHUFFLE);
    let dparams = DParams { nthreads: 1 };
    let mut schunk = Schunk::new(params.clone(), dparams.clone());
    for chunk in &chunks {
        schunk.append_buffer(chunk).unwrap();
    }
    let frame = schunk.to_frame();

    let mut group = c.benchmark_group("schunk_frame");
    group.throughput(Throughput::Bytes(data.len() as u64));

    group.bench_function("append_buffers", |b| {
        b.iter(|| {
            let mut s = Schunk::new(params.clone(), dparams.clone());
            for chunk in &chunks {
                s.append_buffer(black_box(chunk)).unwrap();
            }
            black_box(s)
        });
    });

    group.bench_function("to_frame", |b| {
        b.iter(|| black_box(schunk.to_frame()));
    });

    group.bench_function("from_frame", |b| {
        b.iter(|| black_box(Schunk::from_frame(black_box(&frame)).unwrap()));
    });

    group.bench_function("decompress_all", |b| {
        b.iter(|| black_box(schunk.decompress_all().unwrap()));
    });

    group.finish();
}

fn bench_c_helpers(c: &mut Criterion) {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let c_compress = root.join("c_compress_codec");
    let c_decompress = root.join("c_decompress_1t");
    if !c_compress.exists() || !c_decompress.exists() {
        return;
    }

    let temp = tempfile::tempdir().expect("tempdir for C helper benchmark");
    let input = temp.path().join("signal_f32.bin");
    std::fs::write(&input, signal_f32_bytes(DATA_SIZE)).expect("write C helper input");

    let compressed = temp.path().join("c_lz4.b2frame");
    let status = Command::new(&c_compress)
        .args([&input, &compressed])
        .args(["lz4", "4"])
        .status()
        .expect("prepare C helper compressed file");
    assert!(status.success(), "C helper compression setup failed");

    let mut group = c.benchmark_group("c_helpers");
    group.sample_size(10);
    group.throughput(Throughput::Bytes(DATA_SIZE as u64));

    group.bench_function("lz4_t4/compress_process", |b| {
        b.iter(|| {
            let output = temp.path().join("c_lz4_iter.b2frame");
            let status = Command::new(&c_compress)
                .args([black_box(&input), black_box(&output)])
                .args(["lz4", "4"])
                .status()
                .expect("run C helper compression");
            assert!(status.success());
        });
    });

    group.bench_function("lz4_t4/decompress_process", |b| {
        b.iter(|| {
            let output = temp.path().join("c_lz4_iter.out");
            let status = Command::new(&c_decompress)
                .args([black_box(&compressed), black_box(&output)])
                .status()
                .expect("run C helper decompression");
            assert!(status.success());
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_filters,
    bench_codec_blocks,
    bench_chunks,
    bench_schunk_frame,
    bench_c_helpers
);
criterion_main!(benches);
