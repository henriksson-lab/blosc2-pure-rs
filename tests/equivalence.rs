#![cfg(all(feature = "_ffi", feature = "cli"))]
use std::fs;
use std::path::Path;
use std::process::Command;
use std::sync::{Mutex, OnceLock};

use blosc2_pure_rs::{
    blosc1_compress, blosc1_decompress, blosc2_get_delta, blosc2_set_delta_enabled, CParams,
    DParams, Schunk, BLOSC2_MAX_FILTERS, BLOSC_BITSHUFFLE, BLOSC_FORWARD_COMPAT_SPLIT, BLOSC_LZ4,
    BLOSC_SHUFFLE, BLOSC_ZSTD,
};

const RUST_BIN: &str = env!("CARGO_BIN_EXE_blosc2");

fn c_ref_compress_candidates() -> [&'static str; 2] {
    // Built from c-blosc2/examples/compress_file.c or its local 1-thread helper variant.
    ["compress_file_ref", "c_compress_1t"]
}

fn c_ref_decompress_candidates() -> [&'static str; 2] {
    ["decompress_file_ref", "c_decompress_1t"]
}

fn project_root() -> &'static str {
    env!("CARGO_MANIFEST_DIR")
}

fn c_helper_path(candidates: &[&str], role: &str) -> String {
    candidates
        .into_iter()
        .map(|candidate| format!("{}/{}", project_root(), candidate))
        .find(|candidate| Path::new(candidate).exists())
        .unwrap_or_else(|| {
            panic!(
                "C {role} helper not found; expected one of: {}",
                candidates.join(", ")
            )
        })
}

fn c_compress_path() -> String {
    c_helper_path(&c_ref_compress_candidates(), "compress")
}

fn c_decompress_path() -> String {
    c_helper_path(&c_ref_decompress_candidates(), "decompress")
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
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        data.push((state >> 33) as u8);
    }
    data
}

/// Create little-endian f32 data with non-trivial mantissas for truncprec.
fn create_float_data(elements: usize) -> Vec<u8> {
    let mut data = Vec::with_capacity(elements * 4);
    for i in 0..elements {
        let sign = ((i as u32) & 1) << 31;
        let exponent = (120 + ((i as u32) % 32)) << 23;
        let mantissa =
            (i as u32).wrapping_mul(1_103_515_245).wrapping_add(12_345) & 0x007f_ffff | 0x55;
        let value = f32::from_bits(sign | exponent | mantissa);
        data.extend_from_slice(&value.to_le_bytes());
    }
    data
}

fn expected_truncprec_f32_meta16(data: &[u8]) -> Vec<u8> {
    let mut truncated = data.to_vec();
    for bytes in truncated.chunks_exact_mut(4) {
        let value = u32::from_le_bytes(bytes.try_into().unwrap());
        bytes.copy_from_slice(&(value & !0x7f).to_le_bytes());
    }
    truncated
}

fn assert_byte_stream_eq(expected: &[u8], actual: &[u8], context: &str) {
    assert_eq!(
        expected.len(),
        actual.len(),
        "{context}: byte length mismatch"
    );
    if let Some(index) = expected
        .iter()
        .zip(actual)
        .position(|(lhs, rhs)| lhs != rhs)
    {
        panic!(
            "{context}: first byte mismatch at offset {index}: expected 0x{:02x}, got 0x{:02x}",
            expected[index], actual[index]
        );
    }
}

/// Update data in the same shape as c-blosc2/tests/test_delta.c for a selected typesize.
fn write_delta_data(typesize: usize, data: &mut [u8]) {
    let size = data.len();
    let elements = size / typesize;

    match typesize {
        1 => {
            for (i, byte) in data.iter_mut().enumerate().take(elements) {
                *byte = i as u8;
            }
        }
        2 => {
            for i in 0..elements {
                data[i * 2..i * 2 + 2].copy_from_slice(&(i as u16).to_ne_bytes());
            }
        }
        4 => {
            for i in 0..elements {
                data[i * 4..i * 4 + 4].copy_from_slice(&(i as u32).to_ne_bytes());
            }
        }
        7 => {
            for i in 0..elements {
                let offset = i * 4;
                data[offset..offset + 4].copy_from_slice(&(i as u32).to_ne_bytes());
                data[offset + 2..offset + 4].copy_from_slice(&(i as u16).to_ne_bytes());
                data[offset + 3] = i as u8;
            }
        }
        8 => {
            for i in 0..elements {
                data[i * 8..i * 8 + 8].copy_from_slice(&(i as u64).to_ne_bytes());
            }
        }
        12 => {
            for i in 0..elements {
                let offset = i * 8;
                data[offset..offset + 8].copy_from_slice(&(i as u64).to_ne_bytes());
                data[offset + 4..offset + 8].copy_from_slice(&1u32.to_ne_bytes());
            }
        }
        13 => {
            for i in 0..elements {
                let offset = i * 8;
                data[offset..offset + 8].copy_from_slice(&(i as u64).to_ne_bytes());
                data[offset + 4..offset + 8].copy_from_slice(&1u32.to_ne_bytes());
                data[offset + 5] = 1;
            }
        }
        16 => {
            for i in (0..elements).step_by(2) {
                data[i * 8..i * 8 + 8].copy_from_slice(&(i as u64).to_ne_bytes());
                data[i * 8 + 8..i * 8 + 16].copy_from_slice(&((i as u64) + 1).to_ne_bytes());
            }
        }
        24 => {
            for i in 0..elements {
                let offset = i * 8;
                data[offset..offset + 8].copy_from_slice(&(i as u64).to_ne_bytes());
                data[offset + 4..offset + 8].copy_from_slice(&1u32.to_ne_bytes());
                data[offset + 12..offset + 20].copy_from_slice(&(i as u64).to_ne_bytes());
                data[offset + 16..offset + 20].copy_from_slice(&2u32.to_ne_bytes());
            }
        }
        _ => {
            for (i, byte) in data.iter_mut().enumerate().take(elements) {
                *byte = i as u8;
            }
        }
    }
}

/// Test: Rust compress → Rust decompress roundtrip
fn rust_roundtrip(data: &[u8], codec: &str, clevel: u8, filter: &str, typesize: i32) {
    rust_roundtrip_with_splitmode(data, codec, clevel, filter, typesize, None);
}

fn rust_roundtrip_with_splitmode(
    data: &[u8],
    codec: &str,
    clevel: u8,
    filter: &str,
    typesize: i32,
    splitmode: Option<&str>,
) {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("input.bin");
    let compressed = dir.path().join("output.b2frame");
    let restored = dir.path().join("restored.bin");

    fs::write(&input, data).unwrap();

    let mut args = vec![
        "compress".to_string(),
        input.to_str().unwrap().to_string(),
        compressed.to_str().unwrap().to_string(),
        "-c".to_string(),
        codec.to_string(),
        "-l".to_string(),
        clevel.to_string(),
        "-f".to_string(),
        filter.to_string(),
        "-t".to_string(),
        typesize.to_string(),
        "-n".to_string(),
        "1".to_string(),
    ];
    if let Some(splitmode) = splitmode {
        args.push("-s".to_string());
        args.push(splitmode.to_string());
    }

    let status = Command::new(RUST_BIN)
        .args(args)
        .status()
        .expect("Failed to run Rust compress");
    assert!(
        status.success(),
        "Rust compress failed for codec={codec} clevel={clevel} filter={filter} splitmode={splitmode:?}"
    );

    let status = Command::new(RUST_BIN)
        .args([
            "decompress",
            compressed.to_str().unwrap(),
            restored.to_str().unwrap(),
            "-n",
            "1",
        ])
        .status()
        .expect("Failed to run Rust decompress");
    assert!(status.success(), "Rust decompress failed for codec={codec}");

    let original = data;
    let restored_data = fs::read(&restored).unwrap();
    assert_eq!(
        original,
        &restored_data[..],
        "Roundtrip mismatch for codec={codec} clevel={clevel} filter={filter} typesize={typesize} splitmode={splitmode:?}"
    );
}

fn rust_compress(
    data: &[u8],
    output: &Path,
    codec: &str,
    clevel: u8,
    filter: &str,
    typesize: i32,
    filter_meta: Option<u8>,
    splitmode: Option<&str>,
) {
    let input = output.with_file_name("input.bin");
    fs::write(&input, data).unwrap();

    let mut args = vec![
        "compress".to_string(),
        input.to_str().unwrap().to_string(),
        output.to_str().unwrap().to_string(),
        "-c".to_string(),
        codec.to_string(),
        "-l".to_string(),
        clevel.to_string(),
        "-f".to_string(),
        filter.to_string(),
        "-t".to_string(),
        typesize.to_string(),
        "-n".to_string(),
        "1".to_string(),
    ];
    if let Some(meta) = filter_meta {
        args.push("--filter-meta".to_string());
        args.push(meta.to_string());
    }
    if let Some(splitmode) = splitmode {
        args.push("-s".to_string());
        args.push(splitmode.to_string());
    }

    let status = Command::new(RUST_BIN)
        .args(args)
        .status()
        .expect("Failed to run Rust compress");
    assert!(
        status.success(),
        "Rust compress failed for codec={codec} clevel={clevel} filter={filter} splitmode={splitmode:?}"
    );
}

fn rust_decompress(input: &Path, output: &Path) {
    let status = Command::new(RUST_BIN)
        .args([
            "decompress",
            input.to_str().unwrap(),
            output.to_str().unwrap(),
            "-n",
            "1",
        ])
        .status()
        .expect("Failed to run Rust decompress");
    assert!(status.success(), "Rust decompress failed");
}

fn blosc1_delta_fixture_cbytes(data: &[u8], typesize: i32) -> (usize, usize) {
    static DELTA_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    let _guard = DELTA_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
    let previous_delta = blosc2_get_delta();

    let mut no_delta_dest = vec![0u8; data.len() + 256];
    blosc2_set_delta_enabled(false);
    let no_delta_cbytes = blosc1_compress(1, BLOSC_SHUFFLE, typesize, data, &mut no_delta_dest)
        .expect("Blosc1 no-delta fixture compression failed");

    let mut delta_dest = vec![0u8; data.len() + 256];
    blosc2_set_delta_enabled(true);
    let delta_cbytes = blosc1_compress(1, BLOSC_SHUFFLE, typesize, data, &mut delta_dest)
        .expect("Blosc1 delta fixture compression failed");

    blosc2_set_delta_enabled(previous_delta);

    let mut restored = vec![0u8; data.len()];
    let nbytes = blosc1_decompress(&delta_dest[..delta_cbytes], &mut restored)
        .expect("Blosc1 delta fixture decompression failed");
    assert_eq!(nbytes, data.len(), "Blosc1 delta nbytes mismatch");
    assert_byte_stream_eq(data, &restored, "Blosc1 delta roundtrip");

    (no_delta_cbytes, delta_cbytes)
}

fn u16_bytes(values: &[u16]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(std::mem::size_of_val(values));
    for value in values {
        bytes.extend_from_slice(&value.to_ne_bytes());
    }
    bytes
}

fn i32_bytes(values: &[i32]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(std::mem::size_of_val(values));
    for value in values {
        bytes.extend_from_slice(&value.to_ne_bytes());
    }
    bytes
}

/// Test: C compress → Rust decompress (cross-compatibility)
fn c_compress_rust_decompress(data: &[u8]) {
    let c_compress = c_compress_path();
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
    assert!(
        status.success(),
        "Rust decompress of C-compressed data failed"
    );

    // C decompress for reference
    let status = Command::new(&c_decompress)
        .args([c_compressed.to_str().unwrap(), c_restored.to_str().unwrap()])
        .status()
        .expect("Failed to run C decompress");
    assert!(status.success(), "C decompress failed");

    let original = data;
    let rust_data = fs::read(&rust_restored).unwrap();
    let c_data = fs::read(&c_restored).unwrap();

    assert_eq!(
        original,
        &rust_data[..],
        "C compress → Rust decompress mismatch"
    );
    assert_eq!(
        c_data, rust_data,
        "C and Rust decompression differ for C-compressed data"
    );
    assert_eq!(original, &c_data[..], "C roundtrip mismatch");
}

/// Test: Rust compress → C decompress (cross-compatibility)
fn rust_compress_c_decompress(data: &[u8]) {
    rust_compress_c_decompress_with_params(data, "blosclz", 9, "shuffle", 1, None, None);
}

fn rust_compress_c_decompress_with_params(
    data: &[u8],
    codec: &str,
    clevel: u8,
    filter: &str,
    typesize: i32,
    filter_meta: Option<u8>,
    splitmode: Option<&str>,
) {
    let c_decompress = c_decompress_path();

    let dir = tempfile::tempdir().unwrap();
    let rust_compressed = dir.path().join("rust_output.b2frame");
    let rust_restored = dir.path().join("rust_restored.bin");
    let c_restored = dir.path().join("c_restored.bin");

    rust_compress(
        data,
        &rust_compressed,
        codec,
        clevel,
        filter,
        typesize,
        filter_meta,
        splitmode,
    );
    rust_decompress(&rust_compressed, &rust_restored);

    // C decompress what Rust compressed
    let status = Command::new(&c_decompress)
        .args([
            rust_compressed.to_str().unwrap(),
            c_restored.to_str().unwrap(),
        ])
        .status()
        .expect("Failed to run C decompress");
    assert!(
        status.success(),
        "C decompress of Rust-compressed data failed"
    );

    let rust_data = fs::read(&rust_restored).unwrap();
    let c_data = fs::read(&c_restored).unwrap();
    assert_eq!(
        rust_data, c_data,
        "Rust and C decompression differ for codec={codec} clevel={clevel} filter={filter} typesize={typesize} splitmode={splitmode:?}"
    );
    if filter == "truncprec" && typesize == 4 && filter_meta == Some(16) {
        let expected = expected_truncprec_f32_meta16(data);
        assert_byte_stream_eq(
            &expected,
            &c_data,
            &format!(
                "Rust compress → C decompress did not produce C-style truncprec output for codec={codec} clevel={clevel} typesize={typesize} splitmode={splitmode:?}"
            ),
        );
    } else {
        assert_eq!(
            data,
            &c_data[..],
            "Rust compress → C decompress mismatch for codec={codec} clevel={clevel} filter={filter} typesize={typesize} splitmode={splitmode:?}"
        );
    }
}

fn rust_default_cli_c_decompress(data: &[u8]) {
    let c_decompress = c_decompress_path();

    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("input.bin");
    let rust_compressed = dir.path().join("rust_default_output.b2frame");
    let rust_restored = dir.path().join("rust_restored.bin");
    let c_restored = dir.path().join("c_restored.bin");

    fs::write(&input, data).unwrap();

    let status = Command::new(RUST_BIN)
        .args([
            "compress",
            input.to_str().unwrap(),
            rust_compressed.to_str().unwrap(),
        ])
        .status()
        .expect("Failed to run Rust default CLI compress");
    assert!(status.success(), "Rust default CLI compress failed");

    rust_decompress(&rust_compressed, &rust_restored);

    let status = Command::new(&c_decompress)
        .args([
            rust_compressed.to_str().unwrap(),
            c_restored.to_str().unwrap(),
        ])
        .status()
        .expect("Failed to run C decompress");
    assert!(
        status.success(),
        "C decompress of Rust default CLI-compressed data failed"
    );

    let rust_data = fs::read(&rust_restored).unwrap();
    let c_data = fs::read(&c_restored).unwrap();
    assert_eq!(data, &rust_data[..], "Rust default CLI roundtrip mismatch");
    assert_eq!(
        data,
        &c_data[..],
        "C decode of Rust default CLI frame mismatch"
    );
    assert_eq!(
        rust_data, c_data,
        "Rust and C decompression differ for Rust default CLI frame"
    );
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

#[test]
fn test_rust_default_cli_matches_c_decompress() {
    let data = create_compressible_data(1_000_001);
    rust_default_cli_c_decompress(&data);
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
            "-c",
            "lz4",
            "-n",
            "4",
        ])
        .status()
        .unwrap();
    assert!(status.success());

    let status = Command::new(RUST_BIN)
        .args([
            "decompress",
            compressed.to_str().unwrap(),
            restored.to_str().unwrap(),
            "-n",
            "4",
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
    let filters = ["nofilter", "shuffle", "bitshuffle", "delta"];

    for codec in &codecs {
        for filter in &filters {
            rust_roundtrip(&data, codec, 5, filter, 4);
            rust_compress_c_decompress_with_params(&data, codec, 5, filter, 4, None, None);
        }
    }
}

#[test]
fn test_codec_truncprec_matrix_matches_c_decompress() {
    let data = create_float_data(4096);
    let codecs = ["blosclz", "lz4", "lz4hc", "zlib", "zstd"];

    for codec in &codecs {
        rust_compress_c_decompress_with_params(&data, codec, 5, "truncprec", 4, Some(16), None);
    }
}

#[test]
fn test_truncprec_fixture_exercises_lossy_behavior() {
    let data = create_float_data(4096);
    let restored = expected_truncprec_f32_meta16(&data);

    assert_eq!(
        data.len(),
        restored.len(),
        "truncprec fixture changed byte length"
    );
    assert_ne!(
        data, restored,
        "truncprec fixture decoded losslessly; fixture must exercise lossy behavior"
    );
    let restored2 = expected_truncprec_f32_meta16(&restored);
    assert_eq!(
        restored, restored2,
        "truncprec output should be stable after the first lossy application"
    );
}

#[test]
fn test_splitmode_matrix_matches_c_decompress() {
    let data = create_compressible_data(500_000);
    let splitmodes = ["always", "never", "auto", "forward"];

    for splitmode in &splitmodes {
        rust_roundtrip_with_splitmode(&data, "lz4", 5, "shuffle", 4, Some(splitmode));
        rust_compress_c_decompress_with_params(
            &data,
            "lz4",
            5,
            "shuffle",
            4,
            None,
            Some(splitmode),
        );
    }
}

#[test]
fn test_delta_typesize_matrix_matches_c_test_delta() {
    let size = 7 * 12 * 13 * 16 * 24 * 10;
    let typesizes = [1, 2, 4, 7, 8, 12, 13, 15, 16];
    let mut data = vec![0x99u8; size];

    for typesize in typesizes {
        write_delta_data(typesize, &mut data);
        let (no_delta_cbytes, delta_cbytes) = blosc1_delta_fixture_cbytes(&data, typesize as i32);

        if matches!(typesize, 12 | 15 | 24) {
            assert!(
                2 * delta_cbytes < 4 * no_delta_cbytes,
                "delta compressed size regressed for typesize={typesize}: no_delta={no_delta_cbytes}, delta={delta_cbytes}"
            );
        } else {
            assert!(
                delta_cbytes < no_delta_cbytes,
                "delta should improve compression for typesize={typesize}: no_delta={no_delta_cbytes}, delta={delta_cbytes}"
            );
        }
    }
}

#[test]
fn test_filter_pipeline_matches_c_test_filters() {
    const LEN: usize = 39;
    const TYPESIZE: i32 = 2;

    let ref_values: Vec<u16> = (0..LEN)
        .scan(1u32, |state, _| {
            *state = state.wrapping_mul(1_103_515_245).wrapping_add(12_345);
            Some((*state % 118) as u16)
        })
        .collect();
    let ref_data = u16_bytes(&ref_values);

    let mut filters = [0u8; BLOSC2_MAX_FILTERS];
    filters[BLOSC2_MAX_FILTERS - 2] = BLOSC_BITSHUFFLE;
    filters[BLOSC2_MAX_FILTERS - 1] = BLOSC_SHUFFLE;

    let cparams = CParams {
        compcode: BLOSC_ZSTD,
        typesize: TYPESIZE,
        filters,
        nthreads: 1,
        ..Default::default()
    };
    let dparams = DParams::default();
    let mut schunk = Schunk::new(cparams, dparams);

    assert_eq!(
        schunk.append_buffer(&ref_data).unwrap(),
        1,
        "test_filters.c appends exactly one chunk"
    );
    let restored = schunk.decompress_chunk(0).unwrap();
    assert_byte_stream_eq(&ref_data, &restored, "test_filters.c pipeline roundtrip");
}

#[test]
fn test_frame_simple_memory_and_file_roundtrip() {
    const CHUNKSIZE: usize = 200 * 1000;
    const NCHUNKS: usize = 4;
    const NTHREADS: i16 = 4;

    let cparams = CParams {
        compcode: BLOSC_LZ4,
        clevel: 9,
        typesize: std::mem::size_of::<i32>() as i32,
        splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
        nthreads: NTHREADS,
        ..Default::default()
    };
    let dparams = DParams {
        nthreads: NTHREADS,
        ..Default::default()
    };
    let mut schunk = Schunk::new(cparams, dparams);

    for nchunk in 0..NCHUNKS {
        let values: Vec<i32> = (0..CHUNKSIZE)
            .map(|i| (i as i32) * (nchunk as i32))
            .collect();
        assert_eq!(
            schunk.append_buffer(&i32_bytes(&values)).unwrap(),
            (nchunk + 1) as i64,
            "frame_simple.c append count mismatch"
        );
    }

    let frame = schunk.to_frame();
    let schunk_from_memory = Schunk::from_frame(&frame).unwrap();

    let dir = tempfile::tempdir().unwrap();
    let frame_path = dir.path().join("frame_simple.b2frame");
    schunk.to_file(frame_path.to_str().unwrap()).unwrap();
    let schunk_from_file =
        Schunk::open(&format!("file:///{}", frame_path.to_str().unwrap())).unwrap();

    for nchunk in 0..NCHUNKS {
        let expected = i32_bytes(
            &(0..CHUNKSIZE)
                .map(|i| (i as i32) * (nchunk as i32))
                .collect::<Vec<_>>(),
        );
        assert_byte_stream_eq(
            &expected,
            &schunk_from_memory.decompress_chunk(nchunk as i64).unwrap(),
            "frame_simple.c memory frame chunk",
        );
        assert_byte_stream_eq(
            &expected,
            &schunk_from_file.decompress_chunk(nchunk as i64).unwrap(),
            "frame_simple.c file frame chunk",
        );
    }
}
