# blosc2-pure-rs

A pure Rust implementation of the [Blosc2](https://www.blosc.org/) high-performance compression library, providing both a CLI tool and a library API.

Blosc2 is a block-oriented compressor optimized for binary data such as numerical arrays, tensors, and structured formats. It applies a filter pipeline (shuffle, bitshuffle, delta) before compression to exploit data patterns, then compresses with one of several codecs.

This is a translation of the original code and not the authoritative implementation. This code should generate bitwise
equal output to the original. Please report any deviations.

The aim of this project is to increase performance, especially by providing this code through a type-safe library interface.
The code can also be compiled to be used for webassembly.
The aim is a pure Rust runtime implementation. LZ4HC compression is currently out of scope for the pure-Rust target;
the crate uses `lz4-sys` as a temporary compatibility shim. Anyone interested in pure-Rust LZ4HC support can contact us
about having it added.

Compressing is currently slower than C. This will be fixed in the future

## Features

- **5 codecs**: BloscLZ (ported from C), LZ4, LZ4HC, Zlib, Zstd
- **4 filters**: Shuffle, Bitshuffle, Delta, Truncated Precision
- **Frame format**: Compatible with C-Blosc2 `.b2frame` files (read and write)
- **Lazy frame reads**: File-backed `LazySchunk` loads compressed chunks on demand
- **VL-block chunks**: Pure-Rust variable-length block chunks with split/block decompression
- **Multi-threaded**: Bounded per-call Rayon scheduling for block-level and super-chunk chunk-level work
- **Zstd dictionaries**: Per-chunk dictionary training with C/Rust-compatible dictionary chunks
- **CLI**: Compress and decompress files (optional `cli` feature)
- **Library API**: In-memory compression with `Schunk` container
- **Mostly Rust runtime**: LZ4HC temporarily uses `lz4-sys`; pure-Rust LZ4HC is out of scope for now

## Current Limitations

- `lz4hc` currently uses `lz4-sys` for true LZ4HC compression. Pure-Rust LZ4HC is out of scope for now; anyone
  interested in having it added can contact us.

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
- `-c, --codec`: Compression codec (`blosclz`, `lz4`, `lz4hc`, `zlib`, `zstd`). Default: `blosclz`
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

10 MiB deterministic float32 signal data with noise, single-threaded, compiled with
`-C target-cpu=native`. All comparisons are 1 thread vs 1 thread against the local C-Blosc2 3.0.0
test helper binaries, measured on April 15, 2026.

### Realistic data (10 MiB float32 signal data with noise)

| Codec | Compress (MB/s) | Decompress (MB/s) | Ratio |
|-------|----------------:|-------------------:|------:|
| C-Blosc2 BloscLZ (typesize=1) | 441 | 449 | 1.0x |
| Rust BloscLZ (typesize=1) | 193 | 669 | 1.0x |
| C-Blosc2 BloscLZ (typesize=4) | 155 | 358 | 1.5x |
| Rust BloscLZ (typesize=4) | 139 | 242 | 1.5x |
| C-Blosc2 LZ4 (typesize=4) | 340 | 911 | 1.6x |
| Rust LZ4 (typesize=4) | 212 | 257 | 1.6x |
| C-Blosc2 Zstd (typesize=4) | 2 | 662 | 1.8x |
| Rust Zstd (typesize=4) | 135 | 267 | 1.7x |

### Random data (10 MiB, incompressible)

| Codec | Compress (MB/s) | Decompress (MB/s) |
|-------|----------------:|-------------------:|
| C-Blosc2 BloscLZ | 350 | 519 |
| Rust BloscLZ | 221 | 619 |

Rust BloscLZ decompression is faster than C on the incompressible random case in this local run, but
compression is currently slower. SIMD acceleration uses audited SSE2 bitshuffle/bitunshuffle wrappers
and SSE2/AVX2 shuffle/unshuffle wrappers with scalar fallback.

## Codec Comparison

| Codec | Speed | Compression | Best for |
|-------|-------|-------------|----------|
| BloscLZ | Fast | Moderate | General purpose |
| LZ4 | Fastest | Moderate | Speed-critical |
| LZ4HC | Slow | Good | Via `lz4-sys`; pure-Rust LZ4HC is out of scope |
| Zlib | Slow | Good | Balanced |
| Zstd | Moderate | Best | Storage-critical |

## Building

```bash
cargo build --release                              # Library only
cargo build --release --features cli               # Library + CLI
```

For benchmarks, compile with native CPU optimizations:

```bash
RUSTFLAGS="-C target-cpu=native" cargo build --release --features cli
```

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
