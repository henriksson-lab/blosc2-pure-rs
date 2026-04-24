# blosc2-pure-rs

A pure Rust implementation of the [Blosc2](https://www.blosc.org/) high-performance compression library, providing both a CLI tool and a library API.

Blosc2 is a block-oriented compressor optimized for binary data such as numerical arrays, tensors, and structured formats. It applies a filter pipeline (shuffle, bitshuffle, delta) before compression to exploit data patterns, then compresses with one of several codecs.

The library is feature complete except for one edge case (get in touch if this is a problem). The speed is more or less comparable to the C implementation (benchmarks below). 

* 2026-04-24: Speed improved a ton
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

These are local measurements from April 24, 2026, not universal truths. They come from the
checked-in comparison example:

```bash
cargo run --release --example compare_blosc2_rs --features compare-blosc2-rs
```

The workload is the example's default 10 MiB `float32` signal-with-noise buffer at `clevel=5`
and `typesize=4`. The comparison is against
[`blosc2-rs`](https://crates.io/crates/blosc2-rs), which wraps the original C-Blosc2 library.
The example now respects `compress` vs `decompress` mode correctly; older README numbers were
removed because they came from an earlier harness revision.

Single-thread results:

| Case | Pure size | `blosc2-rs` size | Pure compress (MB/s) | `blosc2-rs` compress (MB/s) | Compress ratio | Pure decompress (MB/s) | `blosc2-rs` decompress (MB/s) | Decompress ratio |
|------|----------:|-----------------:|---------------------:|----------------------------:|---------------:|-----------------------:|------------------------------:|-----------------:|
| BloscLZ, no filter | 10485792 | 10486432 | **4261.7** | 874.0 | **4.88x** | 10110.9 | **10751.9** | 0.94x |
| BloscLZ, shuffle | 8037478 | 8033115 | **900.4** | 598.9 | **1.50x** | **5080.7** | 4746.1 | **1.07x** |
| LZ4, shuffle | 7941596 | 7823630 | **986.7** | 461.8 | **2.14x** | **2249.9** | 1669.2 | **1.35x** |
| Zstd, shuffle | 7259575 | 7259575 | **91.4** | 88.9 | **1.03x** | **1715.9** | 1652.8 | **1.04x** |

Four-thread results:

| Case | Pure size | `blosc2-rs` size | Pure compress (MB/s) | `blosc2-rs` compress (MB/s) | Compress ratio | Pure decompress (MB/s) | `blosc2-rs` decompress (MB/s) | Decompress ratio |
|------|----------:|-----------------:|---------------------:|----------------------------:|---------------:|-----------------------:|------------------------------:|-----------------:|
| BloscLZ, no filter | 10485792 | 10486432 | **4245.5** | 2278.5 | **1.86x** | **20553.5** | 16314.4 | **1.26x** |
| BloscLZ, shuffle | 8037478 | 8033115 | **1909.5** | 1448.7 | **1.32x** | 8016.3 | **11655.3** | 0.69x |
| LZ4, shuffle | 7941596 | 7823630 | **2233.8** | 1255.4 | **1.78x** | **8111.0** | 5381.3 | **1.51x** |
| Zstd, shuffle | 7259575 | 7259575 | **313.1** | 311.3 | **1.01x** | 4714.5 | **6108.6** | 0.77x |

Current reading:

- `blosc2-pure-rs` is faster on all compression rows in this run, with Zstd essentially tied.
- Single-thread decompression is competitive across the board; only BloscLZ no-filter is modestly behind in this mixed run.
- Multithreaded decompression is mixed: pure Rust is ahead on BloscLZ no-filter and LZ4, behind on BloscLZ shuffle and Zstd in this particular run.
- The no-filter BloscLZ rows should be read as a stored-chunk fast-path benchmark, because the pure Rust implementation chooses a chunk-level `memcpyed` representation there.
- Some decompression rows are still measurement-sensitive; for serious tuning, rerun individual cases with `BLOSC2_COMPARE_ITERS=...`.

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
cargo run --release --example compare_blosc2_rs --features compare-blosc2-rs
```

The default zlib backend is `flate2` with the `zlib-rs` backend. That keeps the default build
Rust-first and avoids adding native zlib or zlib-ng requirements. If you need the older
fallback for comparison or troubleshooting, build with
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
