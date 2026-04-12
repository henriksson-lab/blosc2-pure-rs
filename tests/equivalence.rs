#![cfg(all(feature = "_ffi", feature = "cli"))]
use std::fs;
use std::path::Path;
use std::process::Command;

const RUST_BIN: &str = env!("CARGO_BIN_EXE_blosc2");

fn c_ref_compress() -> &'static str {
    // Built from c-blosc2/examples/compress_file.c
    "compress_file_ref"
}

fn c_ref_decompress() -> &'static str {
    "decompress_file_ref"
}

fn project_root() -> &'static str {
    env!("CARGO_MANIFEST_DIR")
}

fn c_compress_path() -> String {
    format!("{}/{}", project_root(), c_ref_compress())
}

fn c_decompress_path() -> String {
    format!("{}/{}", project_root(), c_ref_decompress())
}

/// Create test data with repeating patterns (compressible).
fn create_compressible_data(size: usize) -> Vec<u8> {
    let pattern = b"Blosc2 test data with repeating patterns for compression testing! ";
    let mut data = Vec::with_capacity(size);
    while data.len() < size {
        let remaining = size - data.len();
        let chunk = &pattern[..remaining.min(pattern.len())];
        data.extend_from_slice(chunk);
    }
    data
}

/// Create test data that's mostly zeros with some variation.
fn create_sparse_data(size: usize) -> Vec<u8> {
    let mut data = vec![0u8; size];
    for i in (0..size).step_by(1024) {
        data[i] = (i % 256) as u8;
    }
    data
}

/// Create random-ish data (less compressible).
fn create_pseudorandom_data(size: usize) -> Vec<u8> {
    let mut data = Vec::with_capacity(size);
    let mut state: u64 = 0xDEADBEEF;
    for _ in 0..size {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        data.push((state >> 33) as u8);
    }
    data
}

/// Test: Rust compress → Rust decompress roundtrip
fn rust_roundtrip(data: &[u8], codec: &str, clevel: u8, filter: &str, typesize: i32) {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("input.bin");
    let compressed = dir.path().join("output.b2frame");
    let restored = dir.path().join("restored.bin");

    fs::write(&input, data).unwrap();

    let status = Command::new(RUST_BIN)
        .args([
            "compress",
            input.to_str().unwrap(),
            compressed.to_str().unwrap(),
            "-c", codec,
            "-l", &clevel.to_string(),
            "-f", filter,
            "-t", &typesize.to_string(),
            "-n", "1",
        ])
        .status()
        .expect("Failed to run Rust compress");
    assert!(status.success(), "Rust compress failed for codec={codec} clevel={clevel} filter={filter}");

    let status = Command::new(RUST_BIN)
        .args([
            "decompress",
            compressed.to_str().unwrap(),
            restored.to_str().unwrap(),
            "-n", "1",
        ])
        .status()
        .expect("Failed to run Rust decompress");
    assert!(status.success(), "Rust decompress failed for codec={codec}");

    let original = data;
    let restored_data = fs::read(&restored).unwrap();
    assert_eq!(
        original, &restored_data[..],
        "Roundtrip mismatch for codec={codec} clevel={clevel} filter={filter} typesize={typesize}"
    );
}

/// Test: C compress → Rust decompress (cross-compatibility)
fn c_compress_rust_decompress(data: &[u8]) {
    let c_compress = c_compress_path();
    if !Path::new(&c_compress).exists() {
        eprintln!("Skipping C→Rust test: {} not found", c_compress);
        return;
    }
    let c_decompress = c_decompress_path();

    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("input.bin");
    let c_compressed = dir.path().join("c_output.b2frame");
    let rust_restored = dir.path().join("rust_restored.bin");
    let c_restored = dir.path().join("c_restored.bin");

    fs::write(&input, data).unwrap();

    // C compress
    let status = Command::new(&c_compress)
        .args([input.to_str().unwrap(), c_compressed.to_str().unwrap()])
        .status()
        .expect("Failed to run C compress");
    assert!(status.success(), "C compress failed");

    // Rust decompress what C compressed
    let status = Command::new(RUST_BIN)
        .args([
            "decompress",
            c_compressed.to_str().unwrap(),
            rust_restored.to_str().unwrap(),
        ])
        .status()
        .expect("Failed to run Rust decompress");
    assert!(status.success(), "Rust decompress of C-compressed data failed");

    // C decompress for reference
    let status = Command::new(&c_decompress)
        .args([c_compressed.to_str().unwrap(), c_restored.to_str().unwrap()])
        .status()
        .expect("Failed to run C decompress");
    assert!(status.success(), "C decompress failed");

    let original = data;
    let rust_data = fs::read(&rust_restored).unwrap();
    let c_data = fs::read(&c_restored).unwrap();

    assert_eq!(original, &rust_data[..], "C compress → Rust decompress mismatch");
    assert_eq!(original, &c_data[..], "C roundtrip mismatch");
}

/// Test: Rust compress → C decompress (cross-compatibility)
fn rust_compress_c_decompress(data: &[u8]) {
    let c_decompress = c_decompress_path();
    if !Path::new(&c_decompress).exists() {
        eprintln!("Skipping Rust→C test: {} not found", c_decompress);
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("input.bin");
    let rust_compressed = dir.path().join("rust_output.b2frame");
    let c_restored = dir.path().join("c_restored.bin");

    fs::write(&input, data).unwrap();

    // Rust compress
    let status = Command::new(RUST_BIN)
        .args([
            "compress",
            input.to_str().unwrap(),
            rust_compressed.to_str().unwrap(),
            "-c", "blosclz",
            "-l", "9",
            "-n", "1",
        ])
        .status()
        .expect("Failed to run Rust compress");
    assert!(status.success(), "Rust compress failed");

    // C decompress what Rust compressed
    let status = Command::new(&c_decompress)
        .args([rust_compressed.to_str().unwrap(), c_restored.to_str().unwrap()])
        .status()
        .expect("Failed to run C decompress");
    assert!(status.success(), "C decompress of Rust-compressed data failed");

    let original = data;
    let c_data = fs::read(&c_restored).unwrap();
    assert_eq!(original, &c_data[..], "Rust compress → C decompress mismatch");
}

// === Roundtrip tests for all codecs ===

#[test]
fn test_roundtrip_blosclz() {
    let data = create_compressible_data(2_000_000);
    rust_roundtrip(&data, "blosclz", 9, "shuffle", 1);
}

#[test]
fn test_roundtrip_lz4() {
    let data = create_compressible_data(2_000_000);
    rust_roundtrip(&data, "lz4", 9, "shuffle", 1);
}

#[test]
fn test_roundtrip_lz4hc() {
    let data = create_compressible_data(2_000_000);
    rust_roundtrip(&data, "lz4hc", 9, "shuffle", 1);
}

#[test]
fn test_roundtrip_zlib() {
    let data = create_compressible_data(2_000_000);
    rust_roundtrip(&data, "zlib", 9, "shuffle", 1);
}

#[test]
fn test_roundtrip_zstd() {
    let data = create_compressible_data(2_000_000);
    rust_roundtrip(&data, "zstd", 9, "shuffle", 1);
}

// === Different compression levels ===

#[test]
fn test_clevel_0() {
    let data = create_compressible_data(500_000);
    rust_roundtrip(&data, "blosclz", 0, "shuffle", 1);
}

#[test]
fn test_clevel_5() {
    let data = create_compressible_data(500_000);
    rust_roundtrip(&data, "blosclz", 5, "shuffle", 1);
}

#[test]
fn test_clevel_9() {
    let data = create_compressible_data(500_000);
    rust_roundtrip(&data, "blosclz", 9, "shuffle", 1);
}

// === Different filters ===

#[test]
fn test_filter_nofilter() {
    let data = create_compressible_data(500_000);
    rust_roundtrip(&data, "lz4", 5, "nofilter", 4);
}

#[test]
fn test_filter_shuffle() {
    let data = create_compressible_data(500_000);
    rust_roundtrip(&data, "lz4", 5, "shuffle", 4);
}

#[test]
fn test_filter_bitshuffle() {
    let data = create_compressible_data(500_000);
    rust_roundtrip(&data, "lz4", 5, "bitshuffle", 4);
}

#[test]
fn test_filter_delta() {
    let data = create_compressible_data(500_000);
    rust_roundtrip(&data, "lz4", 5, "delta", 4);
}

// === Different typesizes ===

#[test]
fn test_typesize_1() {
    let data = create_compressible_data(500_000);
    rust_roundtrip(&data, "zstd", 5, "shuffle", 1);
}

#[test]
fn test_typesize_2() {
    let data = create_compressible_data(500_000);
    rust_roundtrip(&data, "zstd", 5, "shuffle", 2);
}

#[test]
fn test_typesize_4() {
    let data = create_compressible_data(500_000);
    rust_roundtrip(&data, "zstd", 5, "shuffle", 4);
}

#[test]
fn test_typesize_8() {
    let data = create_compressible_data(500_000);
    rust_roundtrip(&data, "zstd", 5, "shuffle", 8);
}

// === Different data patterns ===

#[test]
fn test_sparse_data() {
    let data = create_sparse_data(1_000_000);
    rust_roundtrip(&data, "zstd", 5, "shuffle", 4);
}

#[test]
fn test_pseudorandom_data() {
    let data = create_pseudorandom_data(1_000_000);
    rust_roundtrip(&data, "lz4", 5, "shuffle", 1);
}

// === Edge cases ===

#[test]
fn test_empty_file() {
    let data = vec![];
    rust_roundtrip(&data, "blosclz", 5, "shuffle", 1);
}

#[test]
fn test_small_data() {
    let data = vec![42u8; 100];
    rust_roundtrip(&data, "lz4", 5, "shuffle", 1);
}

#[test]
fn test_one_byte() {
    let data = vec![0xFFu8];
    rust_roundtrip(&data, "zstd", 5, "nofilter", 1);
}

#[test]
fn test_exactly_chunksize() {
    let data = create_compressible_data(1_000_000);
    rust_roundtrip(&data, "blosclz", 5, "shuffle", 1);
}

#[test]
fn test_larger_than_chunksize() {
    let data = create_compressible_data(3_500_000);
    rust_roundtrip(&data, "lz4", 5, "shuffle", 1);
}

// === Cross-compatibility with C reference ===

#[test]
fn test_c_compress_rust_decompress_compressible() {
    let data = create_compressible_data(2_000_000);
    c_compress_rust_decompress(&data);
}

#[test]
fn test_c_compress_rust_decompress_random() {
    let data = create_pseudorandom_data(1_000_000);
    c_compress_rust_decompress(&data);
}

#[test]
fn test_rust_compress_c_decompress_compressible() {
    let data = create_compressible_data(2_000_000);
    rust_compress_c_decompress(&data);
}

#[test]
fn test_rust_compress_c_decompress_random() {
    let data = create_pseudorandom_data(1_000_000);
    rust_compress_c_decompress(&data);
}

// === Multi-threaded tests ===

#[test]
fn test_multithreaded_compress() {
    let dir = tempfile::tempdir().unwrap();
    let data = create_compressible_data(5_000_000);
    let input = dir.path().join("input.bin");
    let compressed = dir.path().join("output.b2frame");
    let restored = dir.path().join("restored.bin");

    fs::write(&input, &data).unwrap();

    let status = Command::new(RUST_BIN)
        .args([
            "compress",
            input.to_str().unwrap(),
            compressed.to_str().unwrap(),
            "-c", "lz4",
            "-n", "4",
        ])
        .status()
        .unwrap();
    assert!(status.success());

    let status = Command::new(RUST_BIN)
        .args([
            "decompress",
            compressed.to_str().unwrap(),
            restored.to_str().unwrap(),
            "-n", "4",
        ])
        .status()
        .unwrap();
    assert!(status.success());

    let restored_data = fs::read(&restored).unwrap();
    assert_eq!(data, restored_data);
}

// === All codecs × filters matrix ===

#[test]
fn test_codec_filter_matrix() {
    let data = create_compressible_data(500_000);
    let codecs = ["blosclz", "lz4", "lz4hc", "zlib", "zstd"];
    let filters = ["nofilter", "shuffle", "bitshuffle"];

    for codec in &codecs {
        for filter in &filters {
            rust_roundtrip(&data, codec, 5, filter, 4);
        }
    }
}
