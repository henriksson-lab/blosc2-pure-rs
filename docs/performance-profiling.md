# Performance Profiling Notes

Captured on April 16, 2026 with:

```bash
RUSTFLAGS="-C target-cpu=native" cargo build --release --example profile_hotspots
perf record -F 99 -g -o /tmp/blosc2-rs-perf-blosclz.data -- \
  ./target/release/examples/profile_hotspots blosclz-t4-signal-compress 30
perf record -F 99 -g -o /tmp/blosc2-rs-perf-lz4.data -- \
  ./target/release/examples/profile_hotspots lz4-t4-signal-compress 100
perf record -F 99 -g -o /tmp/blosc2-rs-perf-zlib.data -- \
  ./target/release/examples/profile_hotspots zlib-t4-signal-compress 8
perf record -F 99 -g -o /tmp/blosc2-rs-perf-random-lz4.data -- \
  ./target/release/examples/profile_hotspots random-lz4-t4-compress 100
perf report --stdio -n --no-children -i /tmp/blosc2-rs-perf-lz4.data
```

Kernel symbols were restricted by the local system settings, but user-space symbols were resolved.

## Benchmark Discipline

Use two separate benchmark classes:

| Class | Purpose | Command |
|------|---------|---------|
| Hot-path/library benchmarks | Drive codec, filter, chunk, and frame optimization decisions | `RUSTFLAGS="-C target-cpu=native" cargo bench --bench perf --features lz4hc-sys` |
| CLI/process-level benchmarks | Estimate end-user command-line behavior, including process startup, file I/O, frame writing, allocation, and argument parsing | release `target/release/blosc2` compared with local C helper binaries |

Do not use process-level CLI timings to justify low-level codec or filter changes. They are useful for checking the whole command-line path, but they include work outside the compression hot path and vary more across runs.

Every benchmark result should record:

- command line and feature flags
- build flags, especially `RUSTFLAGS`
- input shape and data generator
- codec, level, filter, typesize, chunk size, block size, and thread count
- whether timings are library-level Criterion measurements or process-level CLI/helper measurements
- whether decompressed bytes were verified against the original input

## Top Hotspots

| Case | Top user-space samples |
|------|------------------------|
| BloscLZ typesize=4 signal compression | `filters::pipeline_forward` ~50%, `codecs::compress_block_with_meta` ~15%, `codecs::blosclz::get_match_generic` ~13% |
| LZ4 typesize=4 signal compression | `filters::pipeline_forward` ~57%, `lz4_flex::block::compress::compress_internal` ~24%, libc copy/fill ~15% |
| zlib typesize=4 signal compression | `codecs::compress_block_with_meta` ~88%, `miniz_oxide::deflate::core::flush_block` ~7%, `filters::pipeline_forward` ~2% |
| LZ4 typesize=4 random compression | `filters::pipeline_forward` ~61%, libc copy/fill ~32%, `lz4_flex::block::compress::compress_internal` ~2% |

## Implications

- The next high-impact target is the `typesize=4` filter pipeline. LZ4 and incompressible-data profiles are dominated by shuffle/copy work before codec compression.
- BloscLZ still has a codec-side match-finding cost, but the filter pipeline is the first bottleneck for the measured `typesize=4` path.
- zlib compression is dominated by the pure-Rust deflate backend, so C/zlib-ng parity is unlikely without changing backend strategy.
- Allocation and memory clearing/copying are visible in the LZ4 paths, so scratch-buffer reuse should be profiled after filter-path changes.

## Follow-up Check

After adding a fast path for the common single-`SHUFFLE` compression pipeline, the LZ4
`typesize=4` signal profile moved from the generic `filters::pipeline_forward` symbol to
`filters::shuffle` directly. That confirms the generic pipeline dispatch was removed from
the hot path; the remaining primary target is the shuffle implementation itself.

After replacing the common-width shuffle/unshuffle path for 2, 4, and 8 byte elements with
specialized safe loops, the LZ4 `typesize=4` signal compression driver improved from about
2.00s to 1.27s for 100 iterations on this machine. The follow-up profile shows
`lz4_flex::block::compress::compress_internal` as the dominant user-space symbol at ~73%,
while `filters::shuffle` drops to ~4%.

BloscLZ match scanning was then updated to use XOR prefix-length detection on 8-byte
matches and audited unaligned word loads for the hot fixed-width reads. The
`blosclz-t4-signal-compress` driver improved from about 0.90s to 0.66s for 100 iterations.
The remaining profile no longer shows `get_match_generic` as a top symbol; memory
clear/copy and the inlined block compression path dominate.

The serial compression path now preallocates output capacity without zero-filling the full
worst-case compressed size. It keeps enough initialized bytes for the header and block
offset table, then grows only as bytes are emitted. This avoids a large upfront memset while
preserving capacity so compressible LZ4 does not regress through repeated reallocations.
The shared block helper also reuses one codec scratch buffer across streams and skips filter
scratch allocation entirely for no-op filter pipelines.

For zlib, `flate2`'s optional `zlib-rs` backend was tested as an opt-in pure-Rust alternative.
On `zlib-t4-signal-compress` with 8 iterations, the default miniz backend took about 3.03s
and `zlib-rs` took about 3.27s on this machine, so the default remains unchanged. Keep the
published default pure Rust unless the benchmark harness shows a faster pure-Rust backend.
Treat native zlib/zlib-ng-style backends as future opt-in work, not default behavior. For
performance-focused users that do not need zlib/deflate compatibility, recommend LZ4 for
speed or Zstd for stronger compression.
