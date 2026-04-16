# blosc2-pure-rs

A pure Rust implementation of the [Blosc2](https://www.blosc.org/) high-performance compression library, providing both a CLI tool and a library API.

Blosc2 is a block-oriented compressor optimized for binary data such as numerical arrays, tensors, and structured formats. It applies a filter pipeline (shuffle, bitshuffle, delta) before compression to exploit data patterns, then compresses with one of several codecs.

The aim is a pure Rust runtime implementation. 
* LZ4HC compression is currently out of scope for the pure-Rust target. The default build rejects LZ4HC compression.
* Enable the `lz4hc-sys` feature to use `lz4-sys` as a temporary compatibility shim while a pure-Rust replacement is unavailable.
* Anyone interested in pure-Rust LZ4HC support can contact us about having it added.

Compressing is currently slower than C. This will be fixed in the future

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
* **Do not trust the benchmarks on this page**. They are used to help evaluate the translation. If you want improved performance, you generally have to use this code as a library, and use the additional tricks it offers. We generally accept performance losses in order to reduce our dependency issues
* **Check the original Github pages for information about the package**. This README is kept sparse on purpose. It is not meant to be the primary source of information
* **If you are the author of the original code and wish to move to Rust, you can obtain ownership of this repository and crate**. Until then, our commitment is to offer an as-faithful-as-possible translation of a snapshot of your code. If we find serious bugs, we will report them to you. Otherwise we will just replicate them, to ensure comparability across studies that claim to use package XYZ v.666. Think of this like a fancy Ubuntu .deb-package of your software - that is how we treat it

This blurb might be out of date. Go to [this page](https://github.com/henriksson-lab/rustification) for the latest information and further information about how we approach translation


## Features

- **5 codecs**: BloscLZ (ported from C), LZ4, Zlib, Zstd, plus feature-gated LZ4HC compression
- **4 filters**: Shuffle, Bitshuffle, Delta, Truncated Precision
- **Frame format**: Compatible with C-Blosc2 `.b2frame` files (read and write)
- **Lazy frame reads**: File-backed `LazySchunk` loads compressed chunks on demand
- **VL-block chunks**: Pure-Rust variable-length block chunks with split/block decompression
- **Multi-threaded**: Bounded per-call Rayon scheduling for block-level and super-chunk chunk-level work
- **Zstd dictionaries**: Per-chunk dictionary training with C/Rust-compatible dictionary chunks
- **CLI**: Compress and decompress files (optional `cli` feature)
- **Library API**: In-memory compression with `Schunk` container
- **Pure-Rust default build**: LZ4HC compression is feature-gated behind the temporary `lz4hc-sys` shim

## Current Limitations

- `lz4hc` compression requires the `lz4hc-sys` feature, which uses `lz4-sys` for true LZ4HC compression. Pure-Rust
  LZ4HC is out of scope for now; anyone interested in having it added can contact us. LZ4HC decompression remains
  available in default builds through the pure-Rust LZ4 decoder.
- B2ND metadata serialization supports up to 15 dimensions. 16-D arrays are extremely uncommon and are out of scope
  for now.

## Installation

```bash
# Library dependency
cargo add blosc2-pure-rs

# CLI tool
cargo install blosc2-pure-rs --features cli
```

## CLI Usage

The CLI binary is named `blosc2` and requires the `cli` feature.

### Compress

```bash
blosc2 compress input.bin output.b2frame
blosc2 compress input.bin output.b2frame --codec zstd --clevel 9
blosc2 compress input.bin output.b2frame -c lz4 -l 5 -t 4 -f shuffle
blosc2 compress floats.bin floats.b2frame -c zstd -l 7 -t 4 -b 262144 --chunksize 1000000 --splitmode forward
blosc2 compress floats.bin floats-trunc.b2frame -f truncprec --filter-meta 16 -t 4
```

Options:
- `-c, --codec`: Compression codec (`blosclz`, `lz4`, `lz4hc`, `zlib`, `zstd`). `lz4hc` requires `lz4hc-sys`. Default: `blosclz`
- `-l, --clevel`: Compression level (0-9). Default: `9`
- `-t, --typesize`: Element type size in bytes. Default: `1`
- `-b, --blocksize`: Explicit block size in bytes (`0` = automatic). Default: `0`
- `--chunksize`: Input bytes per frame chunk. Default: `1000000`
- `-s, --splitmode`: Split mode (`always`, `never`, `auto`, `forward`). Default: `forward`
- `-n, --nthreads`: Number of threads. Default: `4`
- `-f, --filter`: Filter (`nofilter`, `shuffle`, `bitshuffle`, `delta`, `truncprec`). Default: `shuffle`
- `--filter-meta`: Filter metadata byte. For `truncprec`, this is the retained precision in bits. Default: `0`

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

### Chunk metadata and item slicing

```rust
use blosc2_pure_rs::compress::{cbuffer_sizes, getitem};

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

let mut schunk = Schunk::new(cparams, DParams { nthreads: 4 });

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

// Or keep chunks on disk and read only what is needed
let lazy = Schunk::open_lazy("data.b2frame").unwrap();
let tail = lazy.get_slice(1024, 256).unwrap();
```

### In-memory frames and slices

```rust
let frame = schunk.to_frame();
let mut from_memory = Schunk::from_frame(&frame).unwrap();

let first_bytes = from_memory.get_slice(0, 128).unwrap();
from_memory.set_slice(0, &first_bytes).unwrap();
let all_data = from_memory.decompress_all().unwrap();
```

### Blosc1-style wrappers

```rust
use blosc2_pure_rs::compress::{blosc1_compress, blosc1_decompress};

let mut compressed = vec![0u8; data.len() + blosc2_pure_rs::constants::BLOSC2_MAX_OVERHEAD];
let csize = blosc1_compress(5, BLOSC_SHUFFLE, 4, &data, &mut compressed).unwrap();

let mut restored = vec![0u8; data.len()];
let dsize = blosc1_decompress(&compressed[..csize], &mut restored).unwrap();
assert_eq!(dsize, data.len());
assert_eq!(restored, data);
```

## Benchmarks

10 MiB inputs, single-threaded, compiled with `-C target-cpu=native`. LZ4HC rows use the optional
`lz4hc-sys` feature. All comparisons are
1 thread vs 1 thread against the local C-Blosc2 3.0.0 test helper binaries, measured on
April 16, 2026. Values are median MB/s across 5 runs, and decompressed output was verified
against the original input.

### Realistic data (10 MiB float32 signal data with noise)

| Codec | Typesize | C Compress (MB/s) | Rust Compress (MB/s) | C Decompress (MB/s) | Rust Decompress (MB/s) | Ratio |
|-------|---------:|------------------:|---------------------:|--------------------:|-----------------------:|------:|
| BloscLZ | 1 | 282.7 | 263.6 | 526.7 | 852.8 | 1.0x |
| BloscLZ | 4 | 333.9 | 204.6 | 501.7 | 569.7 | 1.3x |
| LZ4 | 4 | 274.1 | 206.9 | 455.0 | 583.5 | 1.3x |
| LZ4HC | 4 | 322.1 | 27.9 | 465.6 | 596.6 | C 1.3x / Rust 1.4x |
| Zlib | 4 | 319.3 | 22.1 | 496.0 | 328.2 | C 1.3x / Rust 1.5x |
| Zstd | 4 | 2.3 | 68.8 | 336.6 | 532.5 | C 1.5x / Rust 1.4x |

### Random data (10 MiB, incompressible)

| Codec | Typesize | C Compress (MB/s) | Rust Compress (MB/s) | C Decompress (MB/s) | Rust Decompress (MB/s) | Ratio |
|-------|---------:|------------------:|---------------------:|--------------------:|-----------------------:|------:|
| BloscLZ | 1 | 302.3 | 258.6 | 547.9 | 849.3 | 1.0x |
| LZ4 | 4 | 319.4 | 179.8 | 1038.1 | 690.2 | 1.0x |

Rust BloscLZ `typesize=1` compression is close to C and Rust decompression is faster in these
local runs. `typesize=4` BloscLZ/LZ4 compression remains slower than C, but `typesize=4`
decompression is faster for the signal-data cases in this run. zlib compression and optional
`lz4hc-sys` LZ4HC compression remain slower than C. SIMD acceleration uses audited SSE2
bitshuffle/bitunshuffle wrappers and specialized safe shuffle/unshuffle paths with scalar fallback.

## Codec Comparison

| Codec | Speed | Compression | Best for |
|-------|-------|-------------|----------|
| BloscLZ | Fast | Moderate | General purpose |
| LZ4 | Fastest | Moderate | Speed-critical |
| LZ4HC | Slow | Good | Optional `lz4hc-sys` shim; pure-Rust LZ4HC is out of scope |
| Zlib | Slow | Good | Balanced |
| Zstd | Moderate | Best | Storage-critical |

## Building

```bash
cargo build --release                              # Library only
cargo build --release --features cli               # Library + CLI
cargo build --release --features zlib-rs           # Use flate2's zlib-rs backend
cargo build --release --features lz4hc-sys         # Enable temporary lz4-sys LZ4HC compression
cargo build --release --features "cli lz4hc-sys"   # CLI with LZ4HC compression
```

For benchmarks, compile with native CPU optimizations:

```bash
RUSTFLAGS="-C target-cpu=native" cargo build --release --features cli
```

The default zlib backend is `flate2`'s pure-Rust miniz backend. The `zlib-rs` feature is also
pure Rust and can be tested locally; in the current benchmark workload it was slightly slower
than the default, so it is not enabled by default.

## Testing

The full test suite cross-checks against C-Blosc2 via FFI and requires the `c-blosc2` source directory, cmake, and libclang:

```bash
cargo test --all-features
cargo test --lib --all-features
cargo clippy --all-targets --all-features -- -D warnings
```

## Frame Format Compatibility

Files written by this Rust implementation can be read by C-Blosc2, and vice versa. The frame format (`.b2frame`) uses msgpack headers with compressed chunk data and an offset index.

## License

BSD 3-Clause, matching the original C-Blosc2 license.
