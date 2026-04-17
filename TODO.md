# TODO

## Test Porting Plan

Port the C test suite to Rust. Tests are grouped by priority. Each test should verify
that Rust output matches C output (via FFI comparison) where applicable, and should also
work as standalone pure-Rust tests.

### Tier 1 — Critical correctness (existing code, zero test coverage)

These test functionality we already implement but don't test:

- [x] **Special value chunks** (port `test_fill_special.c`, `test_zero_runlen.c`)
  - Compress all-zero data → verify SPECIAL_ZERO flag in chunk header
  - Compress all-NaN float32/float64 → verify SPECIAL_NAN
  - Decompress special chunks back to original data
  - Cross-check: C-compressed special chunks readable by Rust decompress
  - Test `decompress_special()` in compress.rs for all special types

- [x] **Multi-filter pipelines** (port `test_filters.c`)
  - DELTA + SHUFFLE combined (filters[4]=DELTA, filters[5]=SHUFFLE)
  - BITSHUFFLE + DELTA combined
  - TRUNC_PREC + SHUFFLE combined
  - Verify `pipeline_forward` → `pipeline_backward` roundtrip with multiple active filters
  - Cross-check against FFI: same filters produce same decompressed output

- [x] **Block splitting modes** (port from `test_compress_roundtrip.c`)
  - BLOSC_ALWAYS_SPLIT: verify nstreams == typesize
  - BLOSC_NEVER_SPLIT: verify nstreams == 1
  - BLOSC_FORWARD_COMPAT_SPLIT: verify auto-selection logic
  - Test each mode × each codec × typesize 1,4,8
  - Verify `should_split()` function directly

- [x] **Large typesizes** (port from `test_shuffle_roundtrip.c`, `test_delta.c`)
  - typesize = 16, 32, 64, 128, 255
  - Shuffle/unshuffle roundtrip for each
  - Bitshuffle/bitunshuffle roundtrip for each
  - Delta encode/decode for each
  - Full compress/decompress pipeline for each

- [x] **Non-aligned data sizes**
  - Data size not a multiple of typesize (e.g. 1003 bytes with typesize=4)
  - Data size not a multiple of blocksize
  - Data size = 1, 2, 3, 7, 15, 31, 33, 100, 255, 256, 1000
  - Verify shuffle leftover bytes are handled correctly

- [x] **Incompressible data through full pipeline** (port `test_maxout.c`)
  - Random data that won't compress — verify memcpy fallback
  - Verify cbytes >= nbytes case is handled
  - Verify output buffer overflow protection

### Tier 2 — Important edge cases and cross-compat

- [x] **All compression levels 0-9** (port `test_compress_roundtrip.c`)
  - clevel=0: plain copy, no compression
  - clevel=1-9: each level with each codec
  - Verify blocksize auto-tuning changes per clevel (test `compute_blocksize`)
  - Verify entropy probing thresholds per level

- [x] **Multi-chunk stress test** (port `test_schunk.c`, `test_schunk_frame.c`)
  - File with 100+ chunks
  - Variable-size last chunk (not full chunksize)
  - Verify frame offset table correctness for many chunks
  - Verify frame header nbytes/cbytes totals match sum of chunks
  - Read back each chunk and verify

- [x] **Schunk frame roundtrip** (port `test_frame.c`)
  - Write frame to file, read back, decompress all chunks, verify
  - Test with each codec
  - Test in-memory frame (to_frame / from_frame)
  - Test file-backed frame (to_file / open)

- [x] **Cross-compat: C writes, Rust reads** (expand existing tests)
  - C compress with ALWAYS_SPLIT → Rust decompress
  - C compress with NEVER_SPLIT → Rust decompress
  - C compress with delta filter → Rust decompress
  - C compress with bitshuffle → Rust decompress
  - C compress with each codec at each clevel → Rust decompress

- [x] **Cross-compat: Rust writes, C reads** (expand existing tests)
  - Same matrix as above but Rust compress → C decompress
  - Verify frame format compatibility for each combination

- [x] **Getitem / partial decompression** (port `test_getitem.c`)
  - Extract items [100..200] from a compressed chunk
  - Test with different typesizes
  - Test at block boundaries
  - Implemented `getitem` as block-local partial decompression for regular chunks

- [x] **Empty and minimal data** (port `test_empty_buffer.c`, `test_small_chunks.c`)
  - 0 bytes → compress → decompress → 0 bytes
  - 1 byte with each codec
  - Data smaller than minimum header size
  - Data smaller than blocksize (single block)

### Tier 3 — Schunk operations (requires implementing new methods)

- [x] **Insert chunk** (port `test_insert_chunk.c`)
  - Insert at beginning, middle, end
  - Verify chunk indices shift correctly
  - Verify decompression of all chunks after insert

- [x] **Delete chunk** (port `test_delete_chunk.c`)
  - Delete from beginning, middle, end
  - Verify remaining chunks decompress correctly
  - Delete all chunks → empty schunk

- [x] **Update chunk** (port `test_update_chunk.c`)
  - Update first, middle, last chunk
  - Update with different-sized data
  - Verify non-updated chunks unchanged

- [x] **Copy schunk** (port `test_copy.c`, `test_special_chunk_copy.c`)
  - Deep copy of schunk
  - Copy with special value chunks
  - Modify copy, verify original unchanged

- [x] **Slice operations** (port `test_get_slice_buffer.c`, `test_set_slice_buffer.c`)
  - Get byte slice spanning multiple chunks
  - Set byte slice spanning multiple chunks
  - Edge cases: slice at chunk boundary

- [x] **Reorder offsets** (port `test_reorder_offsets.c`)
  - Reorder chunks in a schunk
  - Verify all chunks still decompress correctly

### Tier 4 — Advanced features (requires new implementations)

- [x] **Variable-length chunks** (port `test_variable_chunks.c`)
  - [x] Chunks with different uncompressed sizes
  - [x] Frame format with variable chunk flag

- [x] **Dictionary compression** (port `test_dict_schunk.c`)
  - [x] Zstd with dictionary training
  - [x] Verify dictionary flag in chunk header
  - [x] Verify C/Rust cross-compatibility for dictionary chunks
  - [x] Preserve dictionary flag in frame headers

- [x] **Metadata / metalayers** (port `test_schunk.c` metalayer parts)
  - [x] Add metalayer to schunk
  - [x] Write frame with metalayers
  - [x] Read frame with metalayers
  - [x] Verify metalayer data preserved

- [x] **VL-metalayers** (port `test_schunk_header.c`)
  - [x] Add variable-length metalayer
  - [x] Verify in frame trailer

- [x] **Blosc1 API compatibility** (port `test_blosc1_compat.c`, `test_api.c`)
  - [x] blosc1_compress / blosc1_decompress roundtrip
  - [x] cbuffer_sizes, cbuffer_metainfo, cbuffer_validate
  - [x] blosc1_compress / blosc1_decompress wrappers

- [x] **Lazy chunks** (port `test_lazychunk.c`, `test_lazychunk_memcpyed.c`)
  - File-backed `LazySchunk` stores chunk offsets/sizes and loads compressed chunks on demand
  - Lazy byte slices read only touched chunks from `.b2frame` files

- [x] **VL-blocks** (port `test_vlblocks.c`)
  - [x] Core pure-Rust VL-block chunk compression/decompression
  - [x] Support C-compatible VL-block compression for `typesize > 1` and non-typesize-multiple block sizes
  - [x] Split VL-block decompression and single-block extraction
  - [x] VL-block `Schunk` frame roundtrip and lazy file-backed read
  - [x] C/Rust cross-compatibility matrix for VL-block chunks
  - [x] Dictionary-compressed VL-block chunks

### Tier 5 — Platform and stress tests

- [x] **Bitshuffle leftovers** (port `test_bitshuffle_leftovers.c`)
  - Non-multiple-of-8 element counts
  - Verify leftover bytes handled correctly

- [x] **Thread safety** (port `test_nthreads.c`, `test_change_nthreads_append.c`)
  - Concurrent compression from multiple threads
  - Change thread count mid-operation

- [x] **GCC segfault regression** (port `gcc-segfault-issue.c`)
  - Repeated compress/decompress cycles
  - Stress test for memory safety

- [x] **Frame offset queries** (port `test_frame_get_offsets.c`, `test_get_slice_nchunks.c`)
  - Get chunk offsets from frame
  - Get chunk range for byte slice

## Phase 6: Safety & Polish
- [x] Validate compression parameters (`typesize`, `blocksize`, `clevel`, `nthreads`) before chunk encoding
- [x] Validate chunk headers before decompression to reject negative sizes, invalid block sizes, bad cbytes, and invalid typesizes
- [x] Return `Err` instead of panicking for truncated compressed stream payloads
- [x] Wire CLI decompression `--nthreads` into the decompression path
- [x] Make `cargo clippy --all-targets -- -D warnings` pass for default features
- [x] Put temporary `lz4-sys` LZ4HC compression shim behind the optional `lz4hc-sys` feature
- Deferred: replace temporary feature-gated `lz4-sys` LZ4HC compression shim with a drop-in pure Rust port. This is out of scope for now.
- [x] Remove remaining warnings in test code
- [x] Replace unsafe filter pipeline aliasing with safe slice borrows
- [x] Guard public filter helpers against undersized destination, scratch, source, and reference buffers
- [x] Guard SSE2 shuffle dispatchers against invalid typesizes and undersized destination buffers before removing the module
- [x] Add fallible `ChunkHeader::try_write` and use it from compression paths
- [x] Remove generic shuffle/filter unchecked indexing
- [x] Replace BloscLZ entropy-probe unchecked indexing with safe slice operations
- [x] Replace BloscLZ compression hot-path raw pointer writes with safe slice indexing
- [x] Remove BloscLZ SSE2 unsafe match helper; use safe generic matching
- [x] Remove unsafe SSE2 shuffle module; use safe generic shuffle/unshuffle
- [x] Convert remaining non-FFI unsafe blocks to safe Rust where performance allows
- [x] Remove unused FFI code from library (keep for tests only)

## Phase 7: Performance Optimization
- [x] Reintroduce SIMD only behind audited safe wrappers (SSE2 shuffle/unshuffle wrappers with scalar fallback; future bitshuffle/AVX2 work should follow the same wrapper pattern)
- [x] Add multi-threading using rayon for block-parallel compression/decompression
- [x] Re-benchmark after optimizations

### Highest-impact follow-up tuning
- [x] Add a repeatable benchmark harness before changing hot paths
  - [x] Criterion benchmarks for filter-only shuffle/unshuffle, bitshuffle/bitunshuffle, BloscLZ block compression/decompression, full chunk compression/decompression, and schunk/frame compression/decompression
  - [x] C helper comparison for the same deterministic inputs and settings when local helper binaries are present
  - [x] Capture `perf` profiles for `typesize=4` BloscLZ, `typesize=4` LZ4, zlib, and incompressible random data
- [x] Optimize the `typesize=4` filter hot path
  - [x] Replace pseudo-SIMD shuffle/unshuffle dispatch for common typesizes 2, 4, and 8 with faster specialized safe loops
  - [x] Reduce generic filter-pipeline dispatch/allocation overhead for the common single-`SHUFFLE` compression path
  - [x] Keep scalar fallback and C-equivalence tests for leftover element counts
- [x] Optimize BloscLZ match finding and encode loops
  - [x] Profile `src/codecs/blosclz.rs` compression on realistic `typesize=4` data
  - [x] Reduce repeated bounds checks in match/run scanning with word-level prefix detection
  - [x] Add small audited unaligned word-load helpers where profiling proved a measurable win
- [x] Reduce block pipeline allocation and copy overhead
  - [x] Reuse codec scratch buffers across streams in the shared block compression helper
  - [x] Avoid per-block `Vec` allocation in the single-thread compression path
  - [x] Skip filter scratch allocation for no-op filter pipelines in the shared block compression helper
  - [x] Avoid zero-filling the serial output buffer's full worst-case compressed capacity
  - [x] Avoid copying filtered data when source/destination buffers can be reused safely
  - Deferred: direct compression into uninitialized frame output would require unsafe initialized-slice handling; keep current safe scratch-buffer boundary
- [x] Revisit pure-Rust zlib backend performance
  - [x] Benchmark current `flate2`/miniz path against available pure-Rust alternatives
  - [x] Add opt-in `zlib-rs` feature for local comparison
  - [x] Keep C/zlib-ng parity as a documented non-goal unless a faster pure-Rust path is available
- [x] Separate CLI/process-level benchmarks from hot-path optimization benchmarks
  - [x] Keep process-level C-vs-Rust tables for end-user CLI behavior only
  - [x] Use Criterion/library-level benchmarks for codec, filter, chunk, and frame hot-path decisions
  - [x] Record benchmark command lines, feature flags, input shape, chunk size, thread count, and build flags with every result
- [x] Optimize sequential full chunk decompression throughput
  - [x] Add a decompression workspace that reuses filter and codec scratch buffers across blocks
  - [x] Add an internal path that writes decompressed blocks directly into the final output buffer
  - [x] Avoid per-block `Vec` allocation for regular sequential full-chunk decompression
  - [x] Fast-path no-op filters by decoding directly into the final output block
  - [x] Fast-path single `SHUFFLE` by unshuffling directly into the final output block
  - [x] Preserve block-local partial decompression behavior for `getitem` and slice helpers
  - [x] Extend scratch-buffer reuse to parallel decompression workers without regressing scheduling
  - [x] Avoid per-block result vectors and result-copy pass in non-delta parallel full-chunk decompression
- [ ] Reuse compression scratch buffers more broadly
  - [x] Reuse worker-local filter buffers and codec output buffers in parallel block compression
  - [ ] Share per-call filter buffers, codec output buffers, and block assembly buffers across blocks/chunks where lifetimes allow
  - [ ] Audit remaining filtered-data copies and remove them only when tests cover aliasing and leftover cases
  - [ ] Keep safe initialized-buffer boundaries unless unsafe handling has a narrow wrapper and dedicated tests
  - Deferred: parallel compressed block assembly still needs owned per-block buffers because block sizes are variable before frame assembly
- [ ] Improve unshuffle performance for decompression-heavy workloads
  - [x] Profile `typesize=4` LZ4 and BloscLZ decompression with `perf`
  - [x] Tune common-width unshuffle loops for `typesize` 2, 4, and 8
  - [x] Benchmark shuffle and unshuffle separately so compression and decompression regressions are visible
  - [x] Keep scalar-first shuffle dispatch for now; existing AVX2/SSE2 shuffle/unshuffle path benchmarked slower than the common-width scalar path on `typesize=4`
  - [x] `perf` showed `filters::unshuffle` as the top resolved user-space symbol for 64 MiB LZ4 and BloscLZ CLI decompression after file-write kernel time
  - [x] Use narrow unaligned integer stores for common-width unshuffle; `filters/unshuffle/4` improved to about 697 us for 1 MiB on the local Criterion run
- [ ] Add further audited SIMD only behind safe wrappers
  - [ ] Add runtime-dispatched SSE2/AVX2 implementations for the dominant `typesize=4` shuffle/unshuffle paths if scalar profiling shows a clear ceiling
  - [ ] Keep scalar fallback and dispatch equivalence tests for supported widths and leftovers
  - [ ] Avoid compile-time-only CPU assumptions in published builds
- [ ] Continue BloscLZ tuning conservatively
  - [x] Add boundary tests for distances 8191, 8192, far-distance limits, long match extension, overlapping copies, and run encoding
  - [x] Compare Rust-compressed chunks against C decompression for each new BloscLZ optimization fixture
  - [ ] Avoid changing C distance-bias and match-length ordering without trace-backed evidence
  - [ ] Use `/home/mahogny/github/claude/newhmmer/tracehash` for hard-to-track BloscLZ divergence cases
- [x] Keep zlib backend strategy explicit
  - [x] Keep pure-Rust default unless a faster pure-Rust backend is demonstrated on the benchmark harness
  - [x] Treat optional native zlib backends as future opt-in work, not default publish behavior
  - [x] Recommend LZ4/Zstd for performance-focused users when zlib compatibility is not required
- [ ] Improve CLI-specific throughput separately from library hot paths
  - [x] Profile file I/O, frame buffering, allocation, and process-level overhead separately from compression work
  - [x] Add buffered file output for frame writing and CLI decompression output after `perf` showed substantial write-side system time
  - [x] Evaluate larger default or documented `--chunksize` guidance for large files
  - [x] Raise CLI/library default chunk size to 4 MiB after local 64 MiB CLI benchmarks showed better compression throughput than 1,000,000-byte chunks without the decompression penalty of one huge chunk
  - [x] Avoid unnecessary intermediate frame buffers when streaming chunks to disk

## Phase 8: Documentation
- [x] Create README.md with benchmark results, CLI usage, library API examples
- [x] Add library API documentation (doc comments)

## Phase 9: Cleanup
- [x] Ensure all CLI parameters are wired to the algorithm
- [x] Ensure library API supports in-memory I/O
- [x] Upgrade all dependencies, no yanked packages
- [x] Keep c-blosc2 dependency for tests only; remove it from the public library API

## Phase 10: Remaining Limitation Fixes
- [x] Add full C-Blosc2-style super-chunk scheduling parity beyond block-parallel compression/decompression
- [x] Extend audited SIMD wrappers beyond SSE2 shuffle/unshuffle
  - [x] Add audited SIMD bitshuffle/bitunshuffle wrappers with scalar fallback
  - [x] Add audited AVX2 shuffle/unshuffle wrappers with scalar fallback
  - [x] Add tests that compare AVX2/SSE2 dispatch and scalar outputs for supported shuffle typesizes and leftover sizes
- [x] Implement B2ND (N-dimensional array) support
  - [x] Define Rust B2ND metadata/layout API
  - [x] Support dense row-major buffer conversion and B2ND frame serialization/deserialization
  - [x] Store chunk payloads in C-compatible B2ND block-major chunk layout
  - [x] Add higher-level B2ND shape, slicing, slice update, and resize helpers
  - [x] Add C/Rust cross-compatibility tests for B2ND arrays
- [x] Implement sparse frame (sframe) support
  - [x] Read sparse frames
  - [x] Write sparse frames
  - [x] Add lazy chunk and VL-block coverage for sparse frames
  - [x] Add C/Rust cross-compatibility tests for sparse frames
- [x] Implement user-defined codec/filter plugin support
  - [x] Define safe Rust registration APIs
  - [x] Preserve plugin metadata in chunks and frames
  - [x] Add validation and roundtrip tests for plugin chunks and frames
- [x] Replace correctness-first `getitem` with block-local partial decompression
  - [x] Avoid decompressing untouched blocks
  - [x] Support VL-block chunks by only decoding intersecting VL blocks
  - [x] Preserve current validation and out-of-bounds behavior
  - [x] Add tests for block boundaries, filters, split modes, and dictionary chunks
- [x] Improve Schunk slice updates to avoid recompressing untouched block data where possible
  - [x] Add block-local update path for aligned and unaligned writes
  - [x] Preserve chunk boundaries and existing frame compatibility
  - [x] Add tests for cross-chunk and partial-block updates

## Scope Notes and Future Work
- LZ4HC compression is unavailable by default; enable `lz4hc-sys` to use temporary `lz4-sys`. A pure Rust LZ4HC port is out of scope for now
- User-defined codec/filter plugins are supported through in-process Rust registration APIs; external dynamic plugin loading is not implemented
- Fixed-size frame metalayers and VL-metalayers are supported
