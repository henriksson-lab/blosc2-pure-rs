# blosc2-pure-rs

A pure Rust implementation of the [Blosc2](https://www.blosc.org/) high-performance compression library, providing both a CLI tool and a library API.

Blosc2 is a block-oriented compressor optimized for binary data such as numerical arrays, tensors, and structured formats. It applies a filter pipeline (shuffle, bitshuffle, delta) before compression to exploit data patterns, then compresses with one of several codecs.

This is a translation of the original code and not the authoritative implementation. This code should generate bitwise
equal output to the original. Please report any deviations.

The aim of this project is to increase performance, especially by providing this code through a type-safe library interface.
The code can also be compiled to be used for webassembly.

Compressing is currently slower than C. This will be fixed in the future

## Features

- **5 codecs**: BloscLZ (ported from C), LZ4, LZ4HC, Zlib, Zstd
- **4 filters**: Shuffle, Bitshuffle, Delta, Truncated Precision
- **Frame format**: Compatible with C-Blosc2 `.b2frame` files (read and write)
- **Multi-threaded**: Block-parallel compression and decompression via rayon
- **CLI**: Compress and decompress files (optional `cli` feature)
- **Library API**: In-memory compression with `Schunk` container
- **Pure Rust**: No C dependencies at runtime

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
```

Options:
- `-c, --codec`: Compression codec (`blosclz`, `lz4`, `lz4hc`, `zlib`, `zstd`). Default: `blosclz`
- `-l, --clevel`: Compression level (0-9). Default: `9`
- `-t, --typesize`: Element type size in bytes. Default: `1`
- `-n, --nthreads`: Number of threads. Default: `4`
- `-f, --filter`: Filter (`nofilter`, `shuffle`, `bitshuffle`, `delta`, `truncprec`). Default: `shuffle`

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
```

## Benchmarks

10 MB float32 sensor data with noise, single-threaded, compiled with `-C target-cpu=native`.
All comparisons are 1 thread vs 1 thread against C-Blosc2 3.0.0.

### Realistic data (10 MB float32 sensor data with noise)

| Codec | Compress (MB/s) | Decompress (MB/s) | Ratio |
|-------|----------------:|-------------------:|------:|
| C-Blosc2 BloscLZ (typesize=1) | 311 | 327 | 1.0x |
| **Rust BloscLZ (typesize=1)** | **232** | **742** | **1.0x** |
| **Rust BloscLZ (typesize=4)** | **317** | **502** | **1.9x** |
| **Rust LZ4 (typesize=4)** | **251** | **395** | **1.9x** |
| Rust Zstd (typesize=4) | 145 | 350 | 2.0x |

### Random data (10 MB, incompressible)

| Codec | Compress (MB/s) | Decompress (MB/s) |
|-------|----------------:|-------------------:|
| C-Blosc2 BloscLZ | 443 | 411 |
| **Rust BloscLZ** | **289** | **802** |

Rust decompression is **2.3x faster** than C. With SSE2 SIMD shuffle, Rust compress with typesize=4 matches C compress speed while achieving 1.9x compression ratio. Overall throughput (compress + decompress combined) is **1.5x faster** than C.

## Codec Comparison

| Codec | Speed | Compression | Best for |
|-------|-------|-------------|----------|
| BloscLZ | Fast | Moderate | General purpose |
| LZ4 | Fastest | Moderate | Speed-critical |
| LZ4HC | Slow | Good | Not yet differentiated from LZ4 |
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
cargo test --features _ffi,cli    # Full test suite (113 tests)
cargo test --lib                  # Unit tests only (16 tests, no C dependency)
```

## Frame Format Compatibility

Files written by this Rust implementation can be read by C-Blosc2, and vice versa. The frame format (`.b2frame`) uses msgpack headers with compressed chunk data and an offset index.

## License

BSD 3-Clause, matching the original C-Blosc2 license.
