# blosc2-pure-rs

A pure Rust implementation of the [Blosc2](https://www.blosc.org/) high-performance compression library, providing both a CLI tool and a library API.

Blosc2 is a block-oriented compressor optimized for binary data such as numerical arrays, tensors, and structured formats. It applies a filter pipeline (shuffle, bitshuffle, delta) before compression to exploit data patterns, then compresses with one of several codecs.

**Beware that translation is immature technology. Check that this crate works on your data to avoid data loss**

* 2026-06-14: More audit. Possibly converged. Some differences in speed stem from dependencies that need to be fixed
* 2026-06-02: Further audit
* 2026-05-20: New audit approach - many smaller(?) issues fixed, but further auditing needed 
* 2026-05-19: Another audit pass with fixes
* 2026-04-27: Synthetic in-memory benchmarks were broadly comparable to, or faster than, C-Blosc2 on the default benchmark workload. Later real-data CLI checks show similar RSS but load-sensitive wall-clock timings; benchmark your own workload.
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

- **5 high-level codecs**: BloscLZ (ported from C), LZ4, LZ4HC, Zlib, Zstd — all pure Rust
- **4 high-level filters**: Shuffle, Bitshuffle, Delta, Truncated Precision
- **Plugin codec/filter IDs**: Lower-level constants and registration APIs support NDLZ, BYTEDELTA, INT_TRUNC, NDCELL, and NDMEAN paths.
- **Frame format**: Compatible with C-Blosc2 `.b2frame` files (read and write)
- **Lazy frame reads**: File-backed `LazySchunk` loads compressed chunks on demand
- **VL-block chunks**: Pure-Rust variable-length block chunks with split/block decompression
- **Multi-threaded**: Bounded per-call Rayon scheduling for block-level and super-chunk chunk-level work
- **Zstd dictionaries**: Per-chunk dictionary training with C/Rust-compatible dictionary chunks
- **CLI**: Compress and decompress files (optional `cli` feature)
- **Library API**: In-memory compression with `Schunk` container

## Current Limitations

- B2ND metadata supports the C-Blosc2 16-D limit, but some B2ND C-parity gaps remain: file-backed storage choices, zero-copy/view semantics, append fast paths, string-shuffle coverage, and broader append/insert/delete/resize/selection parity tests.
- Attached frame and sparse-frame mutation semantics are incomplete for opened file-backed `Schunk`s; some operations currently update in-memory state without C-equivalent persistence guarantees.
- Dynamic plugin loading is not a current goal. In-process Rust codec/filter registration is supported, and the built-in global plugin paths for NDLZ, ZFP, BYTEDELTA, INT_TRUNC, NDCELL, and NDMEAN are implemented via lower-level APIs/constants rather than the high-level `Codec`/`Filter` CLI enum. Other C global plugins are not implemented.

Default Cargo features statically enable the ported C global plugin algorithms:
`plugin-ndlz`, `plugin-bytedelta`, `plugin-int-trunc`, `plugin-ndcell`, and
`plugin-ndmean`. Disabling default features keeps the IDs/API constants visible
but leaves those built-in algorithms unavailable. There is no dynamic plugin
loader.
- The `*_c` helpers are Rust-shaped C-name compatibility adapters, not `extern "C"` ABI exports. Full C ABI compatibility would require a separate pointer-ownership and struct-layout layer.
- B2ND does not currently model C's internal `chunk_cache_s`; this is treated as a performance gap rather than a correctness requirement until profiling shows otherwise.
- Rust-generated frames target format compatibility with C-Blosc2, not byte-for-byte identical output for every offsets chunk or special-value encoding strategy.

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

The CLI is intentionally a raw file-to-frame tool: `compress` reads an input
file as bytes and writes a Blosc2 frame, while `decompress` reads a Blosc2 frame
and writes raw bytes. B2ND arrays, sparse frames, metadata editing, dictionary
management, VL-block editing, and plugin registration are library API workflows
rather than CLI subcommands.

The CLI's `--codec` and `--filter` flags use the high-level `Codec` and
`Filter` parser enums, so they accept only the built-in names listed below.
NDLZ and plugin filter IDs require lower-level library APIs such as explicit
`CParams` constants or registration.

Compression and decompression accept ordinary platform paths for input and
output, including non-UTF-8 paths on Unix. Both write destinations through a
sibling temporary file before replacing it.

### Compress

```bash
blosc2 compress input.bin output.b2frame
blosc2 compress input.bin output.b2frame --codec zstd --clevel 9
blosc2 compress input.bin output.b2frame -c lz4 -l 5 -t 4 -f shuffle
blosc2 compress floats.bin floats.b2frame -c zstd -l 7 -t 4 -b 262144 --chunksize 4194304 --splitmode forward
blosc2 compress floats.bin floats-trunc.b2frame -f truncprec --filter-meta 16 -t 4
blosc2 compress input.bin dict.b2frame -c zstd --use-dict
```

Options:
- `-c, --codec`: Compression codec (`blosclz`, `lz4`, `lz4hc`, `zlib`, `zstd`). Default: `blosclz`
- `-l, --clevel`: Compression level (0-9). Default: `9`
- `-t, --typesize`: Element type size in bytes. Default: `1`
- `-b, --blocksize`: Explicit block size in bytes (`0` = automatic). Default: `0`
- `--chunksize`: Input bytes per frame chunk. Default: `1000000`.
- `-s, --splitmode`: Split mode (`always`, `never`, `auto`, `forward`). Default: `forward`
- `-n, --nthreads`: Number of threads. Default: `4`
- `-f, --filter`: Filter (`nofilter`, `shuffle`, `bitshuffle`, `delta`, `truncprec`). Default: `shuffle`
- `--filter-meta`: Filter metadata byte. For `truncprec`, this is the retained precision in bits. Default: `0`
- `--use-dict`: Enable codec dictionary training for supported codecs (`lz4`, `lz4hc`, `zstd`). Default: disabled.

Chunk-size guidance: keep the default for general file compression unless you have workload-specific measurements showing a better setting.

## Real-data Rust vs C-Blosc2 benchmarks

Use `tools/bench_against_c_blosc2.py` to compare this Rust CLI against original
C-Blosc2 on real files. The harness measures process wall time and max RSS with
`/usr/bin/time`, verifies decompressed bytes with SHA-256, and writes
`results.csv` plus `summary.md`.

```bash
tools/bench_against_c_blosc2.py \
  --manifest tools/bench_real_manifest.example.toml \
  --profile quick
```

The script builds `target/release/blosc2` with the `cli` feature and builds a
small C helper from `tools/c_blosc2_file_bench.c`. It uses
`c-blosc2/build-ref/blosc/libblosc2.a` when present, otherwise it attempts a
local CMake build of the vendored `c-blosc2/` tree. Pass `--rust-bin` or
`--c-helper` to benchmark prebuilt binaries.

Profiles:
- `quick`: small smoke matrix over `blosclz`, `lz4`, `zstd`, shuffle/no-filter, dictionary-enabled LZ4/Zstd shuffle cases, and 1/4 threads.
- `publish`: broader real-data matrix over all built-in codecs, core filters, levels, split modes, chunk sizes, and dictionary-enabled LZ4/LZ4HC/Zstd cases.
- `full`: larger matrix intended for overnight/local investigation.

Manifest format:

```toml
[[dataset]]
name = "float32_signal"
path = "fixtures/real/float32_signal.bin"
typesizes = [4]

[[dataset]]
name = "fastq"
path = "fixtures/real/sample.fastq"
typesizes = [1]
```

CSV rows include dataset, implementation (`rust` or `c`), mode (`compress` or
`decompress`), frame producer (`rust`, `c`, or `self` for compression), codec,
filter, dictionary mode, type size, thread count, chunk size, compressed size,
ratio, wall time, max RSS, throughput, verification status, process status, and
failure classification with captured stdout/stderr. Decompression rows cover
both self-roundtrips and cross-decompression of Rust-produced frames with C and
C-produced frames with Rust. The harness only attempts Rust-on-C-frame
cross-decompression after C's own self-roundtrip for that frame succeeds, so
C reference frame failures are not double-counted as Rust compatibility
failures.

### Decompress

```bash
blosc2 decompress output.b2frame restored.bin
blosc2 decompress output.b2frame restored.bin --nthreads 2
```

Options:
- `-n, --nthreads`: Number of threads. Default: `4`

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

These are local measurements, not universal truths. They are included to show the
current translation status on one machine and should be rerun for your own data,
compiler flags, CPU, storage, and call pattern.

### Real-data CLI Check

Latest direct CLI checks from June 12, 2026 used a 60.3 MB real-data byte corpus
at `--nthreads 1`, default CLI compression settings, and compared:

```bash
target/release/blosc2 compress .tmp/real_bench/corpus.input out.rust.b2frame --nthreads 1
./c_compress_1t .tmp/real_bench/corpus.input out.c.b2frame
```

Both implementations produced decompressible frames with the same restored
SHA-256 for the original corpus. Rust-generated frame bytes are format-compatible
but not expected to match C frame bytes exactly.

The current practical result is:

| Implementation | Compressed size | Max RSS range | Wall-clock range |
|----------------|----------------:|--------------:|-----------------:|
| C reference helper | 39.4 MB | 5.2-5.5 MiB | 2.48-4.33 s |
| Pure Rust CLI | 39.4 MB | 5.1-5.7 MiB | 2.15-4.74 s |

The machine was under variable load during this run, and C itself varied by more
than 70% between adjacent samples. Treat the table as an RSS/parity smoke check,
not as a stable throughput claim. Earlier unloaded samples on the same corpus
were much faster for both programs and showed Rust RSS near C while compression
remained the main area for tuning.

### Synthetic In-memory Comparison

These measurements were refreshed on June 12, 2026 from the checked-in comparison
examples and compare against
[`blosc2-rs`](https://crates.io/crates/blosc2-rs), which wraps the original C-Blosc2 library.

```bash
cargo run --release --example compare_blosc2_rs --features compare-blosc2-rs
cargo run --release --example bench_blosclz_gap --features _ffi
```

The workload is the examples' default 10 MiB `float32` signal-with-noise buffer at `clevel=5`
and `typesize=4`. Ratios are pure Rust speed divided by C-Blosc2 wrapper speed.

#### Full Comparison

| Case | Threads | Size pure/C | Compress pure/C (MB/s) | Compress ratio | Decompress pure/C (MB/s) | Decompress ratio |
|------|--------:|------------:|-----------------------:|---------------:|-------------------------:|-----------------:|
| BloscLZ, no filter | 1 | 10486432 / 10486432 | 1810.2 / 997.8 | **1.81x** | 10830.5 / 11200.4 | 0.97x |
| BloscLZ, shuffle | 1 | 8024160 / 8033115 | 907.5 / 665.5 | **1.36x** | 3505.9 / 5008.7 | 0.70x |
| LZ4, shuffle | 1 | 7823630 / 7823630 | 715.0 / 567.2 | **1.26x** | 2704.4 / 2680.0 | **1.01x** |
| Zstd, shuffle | 1 | 7259575 / 7259575 | 88.2 / 93.4 | 0.94x | 1687.2 / 1773.0 | 0.95x |
| BloscLZ, no filter | 4 | 10486432 / 10486432 | 1945.6 / 2066.7 | 0.94x | 15681.1 / 12500.8 | **1.25x** |
| BloscLZ, shuffle | 4 | 8024160 / 8033115 | 2138.8 / 2110.7 | **1.01x** | 7734.5 / 5812.7 | **1.33x** |
| LZ4, shuffle | 4 | 7823630 / 7823630 | 1688.9 / 1901.3 | 0.89x | 5455.1 / 3025.2 | **1.80x** |
| Zstd, shuffle | 4 | 7259575 / 7259575 | 329.7 / 347.8 | 0.95x | 5533.9 / 2265.8 | **2.44x** |

#### Focused BloscLZ Comparison

| Case | Size pure/C | Compress pure/C (MB/s) | Compress ratio | Decompress pure/C (MB/s) | Decompress ratio |
|------|------------:|-----------------------:|---------------:|-------------------------:|-----------------:|
| BloscLZ, no filter | 10486432 / 10485792 | 2019.9 / 2157.2 | 0.94x | 11091.0 / 10649.2 | **1.04x** |
| BloscLZ, shuffle | 8024160 / 8024160 | 828.5 / 848.0 | 0.98x | 3351.0 / 4850.5 | 0.69x |
| Unshuffle4 dispatch/scalar | n/a | n/a | n/a | 11652.8 / 5397.8 | **2.16x** |

Current reading:

- Pure Rust is faster than C-Blosc2 on the single-thread BloscLZ and LZ4 compression rows in this run, but four-thread compression is mixed.
- Four-thread decompression is currently strong for pure Rust in the synthetic comparison, especially LZ4 and Zstd shuffle. Single-thread BloscLZ shuffle decompression remains slower than C.
- Zstd compression remains the main weakness; profiling shows most time inside `zstd-pure-rs`'s core lazy compressor rather than this crate's block orchestration.
- Some rows are format-compatible but not byte-identical to C-Blosc2 because block scheduling and fallback choices can differ while preserving decoded bytes.
- All rows decode to identical bytes.
- For serious tuning, rerun individual cases with `BLOSC2_COMPARE_ITERS=...`, `BLOSC2_COMPARE_CASE=...`, and `BLOSC2_COMPARE_THREADS=...`.

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

The full test suite cross-checks against C-Blosc2 via FFI and requires cmake,
libclang, and C-Blosc2 sources. The `_ffi` feature uses `BLOSC2_C_SOURCE_DIR`
when set, or the repo-local `c-blosc2/` directory otherwise. Packaged crates do
not vendor that C source tree.

```bash
BLOSC2_C_SOURCE_DIR=/path/to/c-blosc2 cargo test --all-features
cargo test --all-features
cargo test --lib --all-features
cargo clippy --all-targets --all-features -- -D warnings
```

## License

BSD 3-Clause (same as the original C-Blosc2 license)
