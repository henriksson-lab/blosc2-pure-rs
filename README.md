# blosc2-pure-rs

A pure Rust implementation of the [Blosc2](https://www.blosc.org/) high-performance compression library, providing both a CLI tool and a library API.

Blosc2 is a block-oriented compressor optimized for binary data such as numerical arrays, tensors, and structured formats. It applies a filter pipeline (shuffle, bitshuffle, delta) before compression to exploit data patterns, then compresses with one of several codecs.

The library is feature complete except for one edge case (get in touch if this is a problem). The speed is more or less comparable to the C implementation (benchmarks below). 


* 2026-04-22: Ready for testing, passing current battery of tests. But be vigilant that errors may still remain; report if possible


## This is an LLM-mediated faithful (hopefully) translation, not the original code! 

Most users should probably first see if the existing original code works for them, unless they have reason otherwise. The original source
may have newer features and it has had more love in terms of fixing bugs. In fact, we aim to replicate bugs if they are present, for the
sake of reproducibility! (but then we might have added a few more in the process)

There are however cases when you might prefer this Rust version. We generally agree with [this manifesto](https://rewrites.bio/) but more specifically:
* We have had many issues with ensuring that our software works using existing containers (Docker, PodMan, Singularity). One size does not fit all and it eats our resources trying to keep up with every way of delivering software
* Common package managers do not work well. It was great when we had a few Linux distributions with stable procedures, but now there are just too many ecosystems (Homebrew, Conda). Conda has an NP-complete resolver which does not scale. Homebrew is only so-stable. And our dependencies in Python still break. These can no longer be considered professional serious options. Meanwhile, Cargo enables multiple versions of packages to be available, even within the same program(!)
* The future is the web. We deploy software in the web browser, and until now that has meant Javascript. This is a language where even the == operator is broken. Typescript is one step up, but a game changer is the ability to compile Rust code into webassembly, enabling performance and sharing of code with the backend. Translating code to Rust enables new ways of deployment and running code in the browser has especial benefits for science - researchers do not have deep pockets to run servers, so pushing compute to the user enables deployment that otherwise would be impossible
* Old CLI-based utilities are bad for the environment(!). A large amount of compute resources are spent creating and communicating via small files, which we can bypass by using code as libraries. Even better, we can avoid frequent reloading of databases by hoisting this stage, with up to 100x speedups in some cases. Less compute means faster compute and less electricity wasted
* LLM-mediated translations may actually be safer to use than the original code. This article shows that [running the same code on different operating systems can give somewhat different answers](https://doi.org/10.1038/nbt.3820). This is a gap that Rust+Cargo can reduce. Typesafe interfaces also reduce coding mistakes and error handling, as opposed to typical command-line scripting

But:

* **This approach should still be considered experimental**. The LLM technology is immature and has sharp corners. But there are opportunities to reap, and the genie is not going back into the bottle. This translation is as much aimed to learn how to improve the technology and get feedback on the results.
* Translations are not endorsed by the original authors unless otherwise noted. **Do not send bug reports to the original developers**. Use our Github issues page instead.
* **Treat the benchmarks on this page as local measurements, not universal truths**. They are used to evaluate the translation on one machine and compiler setup. If performance matters for your workload, benchmark your own data and call patterns.
* **Check the original Github pages for information about the package**. This README is kept sparse on purpose. It is not meant to be the primary source of information
* **If you are the author of the original code and wish to move to Rust, you can obtain ownership of this repository and crate**. Until then, our commitment is to offer an as-faithful-as-possible translation of a snapshot of your code. If we find serious bugs, we will report them to you. Otherwise we will just replicate them, to ensure comparability across studies that claim to use package XYZ v.666. Think of this like a fancy Ubuntu .deb-package of your software - that is how we treat it

This blurb might be out of date. Go to [this page](https://github.com/henriksson-lab/rustification) for the latest information and further information about how we approach translation


## Features

- **5 codecs**: BloscLZ (ported from C), LZ4, LZ4HC, Zlib, Zstd — all pure Rust
- **4 filters**: Shuffle, Bitshuffle, Delta, Truncated Precision
- **Frame format**: Compatible with C-Blosc2 `.b2frame` files (read and write)
- **Lazy frame reads**: File-backed `LazySchunk` loads compressed chunks on demand
- **VL-block chunks**: Pure-Rust variable-length block chunks with split/block decompression
- **Multi-threaded**: Bounded per-call Rayon scheduling for block-level and super-chunk chunk-level work
- **Zstd dictionaries**: Per-chunk dictionary training with C/Rust-compatible dictionary chunks
- **CLI**: Compress and decompress files (optional `cli` feature)
- **Library API**: In-memory compression with `Schunk` container

## Current Limitations

- B2ND metadata serialization supports up to 15 dimensions. 16-D arrays are extremely uncommon and are out of scope
  for now.

## Installation

Package name on crates.io: `blosc2-pure-rs`

Library crate name in Rust code: `blosc2_pure_rs`

CLI binary name: `blosc2` (enable the `cli` feature)

```bash
# Library dependency
cargo add blosc2-pure-rs

# CLI tool
cargo install blosc2-pure-rs --features cli
```

## CLI Usage

### Compress

```bash
blosc2 compress input.bin output.b2frame
blosc2 compress input.bin output.b2frame --codec zstd --clevel 9
blosc2 compress input.bin output.b2frame -c lz4 -l 5 -t 4 -f shuffle
blosc2 compress floats.bin floats.b2frame -c zstd -l 7 -t 4 -b 262144 --chunksize 4194304 --splitmode forward
blosc2 compress floats.bin floats-trunc.b2frame -f truncprec --filter-meta 16 -t 4
```

Options:
- `-c, --codec`: Compression codec (`blosclz`, `lz4`, `lz4hc`, `zlib`, `zstd`). Default: `blosclz`
- `-l, --clevel`: Compression level (0-9). Default: `9`
- `-t, --typesize`: Element type size in bytes. Default: `1`
- `-b, --blocksize`: Explicit block size in bytes (`0` = automatic). Default: `0`
- `--chunksize`: Input bytes per frame chunk. Default: `4194304` (4 MiB).
- `-s, --splitmode`: Split mode (`always`, `never`, `auto`, `forward`). Default: `forward`
- `-n, --nthreads`: Number of threads. Default: `4`
- `-f, --filter`: Filter (`nofilter`, `shuffle`, `bitshuffle`, `delta`, `truncprec`). Default: `shuffle`
- `--filter-meta`: Filter metadata byte. For `truncprec`, this is the retained precision in bits. Default: `0`

Chunk-size guidance: keep the default for general file compression unless you have workload-specific measurements showing a better setting.

### Decompress

```bash
blosc2 decompress output.b2frame restored.bin
```

### Verify roundtrip

```bash
blosc2 compress myfile.dat myfile.b2frame -c zstd -l 9
blosc2 decompress myfile.b2frame myfile.restored
diff myfile.dat myfile.restored
```

## Library Usage

### Compress and decompress a buffer

```rust
use blosc2_pure_rs::compress::{compress, decompress, CParams};
use blosc2_pure_rs::constants::*;

let data: Vec<u8> = (0..10000u32)
    .flat_map(|i| i.to_le_bytes())
    .collect();

let cparams = CParams {
    compcode: BLOSC_LZ4,
    clevel: 5,
    typesize: 4,
    filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
    ..Default::default()
};

let chunk = compress(&data, &cparams).unwrap();
let restored = decompress(&chunk).unwrap();
assert_eq!(data, restored);
```

### Reuse an output buffer for fast decompression

For hot decompression paths, especially when chunks are effectively stored rather than compressed,
prefer the destination-buffer API so the caller owns the output allocation:

```rust
use blosc2_pure_rs::compress::{compress, decompress_into, decompress_into_with_threads, CParams};
use blosc2_pure_rs::constants::*;

let data: Vec<u8> = (0..10000u32)
    .flat_map(|i| i.to_le_bytes())
    .collect();

let cparams = CParams {
    compcode: BLOSC_BLOSCLZ,
    clevel: 5,
    typesize: 4,
    filters: [0, 0, 0, 0, 0, BLOSC_NOFILTER],
    nthreads: 4,
    ..Default::default()
};

let chunk = compress(&data, &cparams).unwrap();
let mut restored = vec![0u8; data.len()];
let written = decompress_into(&chunk, &mut restored).unwrap();
assert_eq!(written, data.len());
assert_eq!(restored, data);

let written = decompress_into_with_threads(&chunk, &mut restored, 4).unwrap();
assert_eq!(written, data.len());
assert_eq!(restored, data);
```

### Chunk metadata and item slicing

```rust
use blosc2_pure_rs::compress::{cbuffer_sizes, compress, getitem, CParams};
use blosc2_pure_rs::constants::*;

let data: Vec<u8> = (0..100u32)
    .flat_map(|i| i.to_le_bytes())
    .collect();
let chunk = compress(
    &data,
    &CParams {
        compcode: BLOSC_LZ4,
        clevel: 5,
        typesize: 4,
        filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
        ..Default::default()
    },
)
.unwrap();

let (nbytes, cbytes, blocksize) = cbuffer_sizes(&chunk).unwrap();
assert_eq!(nbytes, data.len());
assert_eq!(cbytes, chunk.len());
assert!(blocksize > 0);

let items_10_to_19 = getitem(&chunk, 10, 10).unwrap();
assert_eq!(items_10_to_19, data[10 * 4..20 * 4]);
```

### Multi-chunk container (Schunk)

```rust
use blosc2_pure_rs::compress::{CParams, DParams};
use blosc2_pure_rs::constants::*;
use blosc2_pure_rs::schunk::Schunk;

let cparams = CParams {
    compcode: BLOSC_ZSTD,
    clevel: 5,
    typesize: 8,
    nthreads: 4,  // multi-threaded compression
    filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
    ..Default::default()
};

let mut schunk = Schunk::new(
    cparams,
    DParams {
        nthreads: 4,
        ..Default::default()
    },
);

// Append data in chunks
let data: Vec<u8> = (0..100000u64)
    .flat_map(|i| i.to_le_bytes())
    .collect();

for chunk_start in (0..data.len()).step_by(100_000) {
    let chunk_end = (chunk_start + 100_000).min(data.len());
    schunk.append_buffer(&data[chunk_start..chunk_end]).unwrap();
}

// Save to file
schunk.to_file("data.b2frame").unwrap();

// Read back
let schunk2 = Schunk::open("data.b2frame").unwrap();
let restored = schunk2.decompress_chunk(0).unwrap();

let mut restored_into = vec![0u8; 100_000];
let written = schunk2.decompress_chunk_into(0, &mut restored_into).unwrap();
assert_eq!(written, 100_000);

let compressed = schunk2.compressed_chunk(0).unwrap();
let view = schunk2.compressed_chunk_view(0).unwrap();
assert_eq!(compressed, view.as_slice());

// Or keep chunks on disk and read only what is needed
let lazy = Schunk::open_lazy("data.b2frame").unwrap();
let tail = lazy.get_slice(1024, 256).unwrap();
```

### In-memory frames and slices

```rust
use blosc2_pure_rs::compress::{CParams, DParams};
use blosc2_pure_rs::constants::*;
use blosc2_pure_rs::schunk::Schunk;

let mut schunk = Schunk::new(
    CParams {
        compcode: BLOSC_LZ4,
        clevel: 5,
        typesize: 1,
        ..Default::default()
    },
    DParams::default(),
);
schunk.append_buffer(b"example payload").unwrap();

let frame = schunk.to_frame();
let mut from_memory = Schunk::from_frame(&frame).unwrap();

let first_bytes = from_memory.get_slice(0, 7).unwrap();
from_memory.set_slice(0, &first_bytes).unwrap();
let all_data = from_memory.decompress_all().unwrap();
```

### Blosc1-style wrappers

```rust
use blosc2_pure_rs::compress::{blosc1_compress, blosc1_decompress};
use blosc2_pure_rs::constants::*;

let data: Vec<u8> = (0..100u32)
    .flat_map(|i| i.to_le_bytes())
    .collect();
let mut compressed = vec![0u8; data.len() + blosc2_pure_rs::constants::BLOSC2_MAX_OVERHEAD];
let csize = blosc1_compress(5, BLOSC_SHUFFLE, 4, &data, &mut compressed).unwrap();

let mut restored = vec![0u8; data.len()];
let dsize = blosc1_decompress(&compressed[..csize], &mut restored).unwrap();
assert_eq!(dsize, data.len());
assert_eq!(restored, data);
```

## Benchmarks

On the benchmark host below, library-API compression ranges from roughly parity to a clear win versus the reference C implementation depending on codec when both libraries are compiled with native CPU flags. After the latest BloscLZ and unshuffle tuning, no-filter BloscLZ decompression is at parity or slightly faster than C on this host, while the shuffled BloscLZ path is now only modestly behind.

Unless otherwise noted, numbers below use a 10 MiB float32 signal-with-noise workload,
single-chunk library API calls, `typesize=4`, `clevel=5`, and one thread. Both C and Rust
were built with native CPU tuning on the benchmark host (Xeon Gold 6138 @ 2.00GHz):

- C: `-O3 -march=native -DNDEBUG`
- Rust: `-C target-cpu=native`

These are local measurements on one machine. They are useful for tracking translation
progress, not for predicting every workload.

| Codec | C Compress (MB/s) | Rust Compress (MB/s) | C ÷ Rust | C Decompress (MB/s) | Rust Decompress (MB/s) | C ÷ Rust | Ratio |
|-------|------------------:|---------------------:|---------:|--------------------:|-----------------------:|---------:|------:|
| BloscLZ | **961.8** | 702.9 | 1.37× | **5465.9** | 3341.2 | 1.64× | 1.55x |
| LZ4     | 591.6 | **920.5** | **0.64×** (Rust faster) | **1536.4** | 1385.1 | 1.11× | 0.84x |
| Zstd    | **107.0** | 98.0 | 1.09× | **1772.8** | 1498.4 | 1.18× | 1.11x |

That table is still useful as a broader codec snapshot, but the BloscLZ row is stale because
it predates the latest tuning work. The current BloscLZ-focused results are below.

### Latest BloscLZ Tuning Snapshot

These measurements were rechecked on April 22, 2026 with the current codebase and the same
native build flags. The no-filter and shuffle cases are split explicitly so codec cost and
filter cost are not conflated.

| Case | Rust Compress (MB/s) | C Compress (MB/s) | Rust Decompress (MB/s) | C Decompress (MB/s) |
|------|---------------------:|------------------:|-----------------------:|--------------------:|
| BloscLZ, no filter | 1413.3 | 2690.6 | **11378.5** | 11256.6 |
| BloscLZ, shuffle | 655.3 | 1024.7 | 5704.0 | **5967.7** |

Current interpretation:

- The earlier no-filter decompression gap is gone on this host; Rust is now at parity or slightly ahead.
- The remaining BloscLZ gap is concentrated in the shuffled path, and is now small enough that run-to-run noise matters.
- The `typesize=4` unshuffle fast path is now slightly faster than the scalar fallback on this CPU.

The latest `typesize=4` unshuffle microbenchmark on this host:

| Kernel | Throughput (MB/s) |
|--------|-------------------:|
| Scalar unshuffle4 | 11725.5 |
| Dispatched SIMD unshuffle4 | **11893.3** |

Recent BloscLZ and filter-side improvements include:

- C-style exact literal copies in the BloscLZ decoder
- overlap-aware match-copy helpers, including the `copy_match_16`-style SSSE3 path
- removal of a redundant full-block postfilter copy in the no-postfilter decompression path
- enabling the AVX2 `typesize=4` unshuffle path in normal dispatch
- aligned AVX2 stores and light source-plane prefetching in `unshuffle4_avx2`

The main remaining gap versus C is in the shuffled decode path, split across the BloscLZ
decode loop and the unshuffle loop.

### Compared with `blosc2-rs`

If you are deciding between this crate and
[`blosc2-rs`](https://crates.io/crates/blosc2-rs), the practical tradeoff is:

- [`blosc2-rs`](https://crates.io/crates/blosc2-rs) is a Rust binding layer over the C-Blosc2 library.
- `blosc2-pure-rs` is a Rust implementation of the runtime itself.

This repo now includes a direct comparison example:

```bash
RUSTFLAGS="-C target-cpu=native" cargo run --release --example compare_blosc2_rs --features compare-blosc2-rs
```

The numbers below were produced on April 22, 2026 using that example, on the same Xeon Gold 6138
host and 10 MiB `float32` signal-with-noise workload used elsewhere in this README.

For BloscLZ with no filter on this workload, `blosc2-pure-rs` now emits a chunk-level
`memcpyed` representation when the data is effectively stored rather than compressed. That is
why the no-filter rows below are best understood as the stored-chunk fast path, not as a hot
BloscLZ decode loop benchmark.

Direct single-thread comparison:

| Case | Pure size | `blosc2-rs` size | Pure compress (MB/s) | `blosc2-rs` compress (MB/s) | Pure decompress (MB/s) | `blosc2-rs` decompress (MB/s) |
|------|----------:|-----------------:|---------------------:|----------------------------:|-----------------------:|------------------------------:|
| BloscLZ, no filter | 10485792 | 10486432 | **4342.1** | 872.1 | 10238.8 | **11023.8** |
| BloscLZ, shuffle | 8037478 | 8033115 | 547.8 | **588.9** | 4154.8 | **4335.3** |
| LZ4, shuffle | 7941596 | 7823630 | **778.0** | 435.1 | **1914.6** | 1517.3 |
| Zstd, shuffle | 7259575 | 7259575 | 82.6 | **86.5** | **1669.6** | 1614.6 |

Direct four-thread comparison:

| Case | Pure size | `blosc2-rs` size | Pure compress (MB/s) | `blosc2-rs` compress (MB/s) | Pure decompress (MB/s) | `blosc2-rs` decompress (MB/s) |
|------|----------:|-----------------:|---------------------:|----------------------------:|-----------------------:|------------------------------:|
| BloscLZ, no filter | 10485792 | 10486432 | **4268.9** | 2847.1 | 19586.6 | **38411.3** |
| BloscLZ, shuffle | 8037478 | 8033115 | 1166.2 | **1627.0** | 10920.1 | **11463.2** |
| LZ4, shuffle | 7941596 | 7823630 | **1724.6** | 1226.9 | 5877.4 | **6040.0** |
| Zstd, shuffle | 7259575 | 7259575 | 301.9 | **318.4** | 4604.3 | **6526.8** |

Interpretation:

- The no-filter BloscLZ target changed substantially after adding an early `memcpyed` fallback and a threaded copy path. On this workload, `blosc2-pure-rs` now wins no-filter compression clearly at both one and four threads.
- The remaining no-filter gap is on four-thread decompression, where `blosc2-rs` still has a clear advantage on this host, though the Rust path has improved materially from the earlier benchmark runs.
- For Rust-side callers on stored-chunk workloads, `decompress_into_with_threads()` is now the recommended fast path because it lets the caller reuse the destination buffer.
- On shuffled data, the picture is mixed: `blosc2-pure-rs` wins several LZ4 and BloscLZ rows, while `blosc2-rs` keeps an edge on most Zstd rows and on some compression paths.
- The compressed sizes are identical for Zstd shuffle, slightly different for shuffled BloscLZ and LZ4, and differ by a small fixed header amount for BloscLZ no-filter because `blosc2-pure-rs` now chooses the chunk-level `memcpyed` representation in this case.
- This is a throughput comparison, not a bit-identical output comparison.

So the current choice is:

- If you want the more conservative choice with mature C-Blosc2 backing and the strongest four-thread decompression on the no-filter stored-chunk path, `blosc2-rs` is still the simpler answer.
- If you want a Rust implementation with no C-Blosc2 runtime dependency, `blosc2-pure-rs` is now competitive or faster in many of the rows above, and it is especially strong on the no-filter stored-chunk case and on several LZ4 paths.

## Codec Comparison

| Codec | Speed | Compression | Best for |
|-------|-------|-------------|----------|
| BloscLZ | Fast | Moderate | General purpose |
| LZ4 | Fastest | Moderate | Speed-critical |
| LZ4HC | Slow | Good | High-compression LZ4 variant (pure Rust) |
| Zlib | Slow | Good | Compatibility with zlib/deflate users |
| Zstd | Moderate | Best | Storage-critical |

## Building

```bash
cargo build --release                              # Library only
cargo build --release --features cli               # Library + CLI
cargo build --release --no-default-features --features cli,zlib-miniz
                                                   # Use the miniz_oxide-backed fallback instead
```

For benchmarks, compile with native CPU optimizations:

```bash
RUSTFLAGS="-C target-cpu=native" cargo build --release --features cli
```

To reproduce the direct crates.io comparison against `blosc2-rs`:

```bash
RUSTFLAGS="-C target-cpu=native" cargo run --release --example compare_blosc2_rs --features compare-blosc2-rs
```

The default zlib backend is `flate2` with the `zlib-rs` backend. That keeps the default build
Rust-first and avoids adding native zlib or zlib-ng requirements, while outperforming the
`miniz_oxide`-backed fallback on the local 10 MiB signal benchmark in this repo
(about 37.0 vs 27.8 MB/s compression and 611.1 vs 472.8 MB/s decompression with native Rust
flags). If you need the older fallback for comparison or troubleshooting, build with
`--no-default-features --features zlib-miniz` instead. When zlib/deflate compatibility is not
required, prefer LZ4 for speed or Zstd for stronger compression.

## Testing

The full test suite cross-checks against C-Blosc2 via FFI and requires the `c-blosc2` source directory, cmake, and libclang:

```bash
cargo test --all-features
cargo test --lib --all-features
cargo clippy --all-targets --all-features -- -D warnings
```

## License

BSD 3-Clause (same as the original C-Blosc2 license)
