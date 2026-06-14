#![cfg(feature = "cli")]

use std::ffi::OsStr;
use std::fs;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::process::{Command, Output};

use blosc2_pure_rs::compress::{CParams, DParams};
use blosc2_pure_rs::constants::{
    BLOSC2_MAX_FILTERS, BLOSC_BLOSCLZ, BLOSC_FORWARD_COMPAT_SPLIT, BLOSC_SHUFFLE, BLOSC_ZSTD,
};
use blosc2_pure_rs::schunk::{frame, Schunk};

const RUST_BIN: &str = env!("CARGO_BIN_EXE_blosc2");

fn compressible_data(size: usize) -> Vec<u8> {
    let pattern = b"blosc2 cli integration data ";
    let mut data = Vec::with_capacity(size);
    while data.len() < size {
        let remaining = size - data.len();
        data.extend_from_slice(&pattern[..remaining.min(pattern.len())]);
    }
    data
}

fn run<I, S>(args: I) -> Output
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    Command::new(RUST_BIN)
        .args(args)
        .output()
        .expect("failed to run blosc2 CLI")
}

#[cfg(unix)]
fn run_with_stale_first_temp<I, S>(args: I, output_path: &Path) -> Output
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    Command::new("bash")
        .arg("-c")
        .arg(
            r#"
set -eu
out=$1
shift
dir=$(dirname -- "$out")
base=$(basename -- "$out")
printf stale > "$dir/.$base.tmp.$$.0"
exec "$@"
"#,
        )
        .arg("blosc2-stale-temp")
        .arg(output_path)
        .arg(RUST_BIN)
        .args(args)
        .output()
        .expect("failed to run blosc2 CLI through stale-temp harness")
}

fn assert_success(output: Output) {
    assert!(
        output.status.success(),
        "command failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn assert_success_c_stats_stdout(output: Output, ratio_prefix: &str, time_prefix: &str) {
    assert!(
        output.status.success(),
        "command failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.stderr.is_empty(),
        "successful C example commands should not write stderr\nstderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.starts_with("Blosc version info: "),
        "stdout should start with C version banner\nstdout:\n{stdout}"
    );
    assert!(
        stdout.contains(ratio_prefix),
        "stdout did not contain {ratio_prefix:?}\nstdout:\n{stdout}"
    );
    assert!(
        stdout.contains(time_prefix),
        "stdout did not contain {time_prefix:?}\nstdout:\n{stdout}"
    );
}

fn assert_failure_contains(output: Output, expected: &str) {
    assert!(
        !output.status.success(),
        "command unexpectedly succeeded\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains(expected),
        "stderr did not contain {expected:?}\nstderr:\n{stderr}"
    );
}

fn assert_failure_code_stderr_contains(output: Output, expected_code: i32, expected: &str) {
    assert_failure_code(output, expected_code, |output| {
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            stdout.starts_with("Blosc version info: "),
            "stdout should keep the C version banner before stderr failures\nstdout:\n{stdout}"
        );
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains(expected),
            "stderr did not contain {expected:?}\nstderr:\n{stderr}"
        );
    });
}

fn assert_c_arg_count_usage(output: Output, expected_usage: &str) {
    assert_failure_code(output, -1, |output| {
        assert!(
            output.stdout.is_empty(),
            "C argument-count usage exits before the version banner\nstdout:\n{}",
            String::from_utf8_lossy(&output.stdout)
        );
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert_eq!(
            stderr,
            format!("{expected_usage}\n"),
            "argument-count usage should match the C example exactly"
        );
    });
}

fn assert_failure_code_stdout_ends_with(output: Output, expected_code: i32, expected: &str) {
    assert_failure_code(output, expected_code, |output| {
        assert!(
            output.stderr.is_empty(),
            "stderr should be empty for C stdout error\nstderr:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            stdout.starts_with("Blosc version info: "),
            "stdout should keep the C version banner before stdout open errors\nstdout:\n{stdout}"
        );
        assert!(
            stdout.ends_with(expected),
            "stdout did not end with {expected:?}\nstdout:\n{stdout}"
        );
        assert!(
            !stdout.ends_with('\n'),
            "C stdout open-error messages do not include a trailing newline\nstdout:\n{stdout:?}"
        );
    });
}

fn assert_failure_code<F>(output: Output, expected_code: i32, inspect: F)
where
    F: FnOnce(&Output),
{
    assert!(
        !output.status.success(),
        "command unexpectedly succeeded\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let expected = process_exit_code(expected_code);
    assert!(
        output.status.code() == Some(expected),
        "exit code mismatch: expected {:?}, got {:?}\nstdout:\n{}\nstderr:\n{}",
        Some(expected),
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    inspect(&output);
}

#[cfg(unix)]
fn process_exit_code(code: i32) -> i32 {
    code.rem_euclid(256)
}

#[cfg(not(unix))]
fn process_exit_code(code: i32) -> i32 {
    code
}

fn assert_stdout_contains(output: Output, expected: &str) {
    assert!(
        output.status.success(),
        "command failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains(expected),
        "stdout did not contain {expected:?}\nstdout:\n{stdout}"
    );
}

fn repo_relative_file_url(path: &Path) -> String {
    let cwd = fs::canonicalize(".").unwrap();
    let path = fs::canonicalize(path)
        .or_else(|_| {
            let parent = path.parent().unwrap_or_else(|| Path::new("."));
            let file_name = path
                .file_name()
                .expect("file URL test path must include a file name");
            Ok::<_, std::io::Error>(fs::canonicalize(parent)?.join(file_name))
        })
        .unwrap();
    let path = path
        .strip_prefix(&cwd)
        .expect("file URL test path must be inside the repository");
    assert!(
        !path.is_absolute(),
        "file URL test path must stay repo-relative: {}",
        path.display()
    );
    format!("file:///{}", path.display())
}

fn roundtrip(args: &[&str], data: &[u8]) {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("input.bin");
    let compressed = dir.path().join("output.b2frame");
    let restored = dir.path().join("restored.bin");
    fs::write(&input, data).unwrap();

    let mut compress_args = vec![
        OsStr::new("compress"),
        input.as_os_str(),
        compressed.as_os_str(),
    ];
    compress_args.extend(args.iter().map(OsStr::new));
    assert_success(run(compress_args));

    assert_success(run([
        OsStr::new("decompress"),
        compressed.as_os_str(),
        restored.as_os_str(),
        OsStr::new("--nthreads"),
        OsStr::new("2"),
    ]));
    assert_eq!(fs::read(restored).unwrap(), data);
}

#[test]
fn wrong_positional_arg_counts_use_c_example_usage() {
    assert_c_arg_count_usage(
        run(["compress"]),
        "Usage: compress_file input_file output_file.b2frame",
    );
    assert_c_arg_count_usage(
        run(["compress", "input.bin"]),
        "Usage: compress_file input_file output_file.b2frame",
    );
    assert_c_arg_count_usage(
        run(["compress", "input.bin", "output.b2frame", "extra.bin"]),
        "Usage: compress_file input_file output_file.b2frame",
    );
    assert_c_arg_count_usage(
        run(["decompress"]),
        "Usage: decompress_file input_file.b2frame output_file",
    );
    assert_c_arg_count_usage(
        run(["decompress", "input.b2frame"]),
        "Usage: decompress_file input_file.b2frame output_file",
    );
    assert_c_arg_count_usage(
        run(["decompress", "input.b2frame", "output.bin", "extra.bin"]),
        "Usage: decompress_file input_file.b2frame output_file",
    );
}

#[test]
fn decompress_help_documents_nthreads_default() {
    // Rust-specific CLI surface: the C example has no flags, while the Rust CLI exposes
    // an optional decompression thread override.
    let help = run(["decompress", "--help"]);
    assert_stdout_contains(help, "-n, --nthreads <NTHREADS>");
    let help = run(["decompress", "--help"]);
    assert_stdout_contains(help, "defaults to the value stored in the frame");
}

#[test]
fn documented_compress_flags_roundtrip() {
    // Rust-specific CLI surface: the C example hard-codes these parameters, while the Rust CLI
    // documents flags for overriding them.
    let data = compressible_data(96_000);
    let cases: &[&[&str]] = &[
        &[],
        &["--codec", "zstd", "--clevel", "7"],
        &["-c", "lz4", "-l", "5", "-t", "4", "-f", "shuffle"],
        &[
            "-c",
            "zlib",
            "-l",
            "6",
            "-t",
            "4",
            "-b",
            "8192",
            "--chunksize",
            "32768",
            "--splitmode",
            "forward",
            "--nthreads",
            "2",
        ],
        &["-c", "lz4hc", "-s", "always", "-f", "nofilter"],
        &[
            "-c",
            "blosclz",
            "-s",
            "never",
            "-f",
            "bitshuffle",
            "-t",
            "8",
        ],
        &["-c", "zstd", "-s", "auto", "-f", "delta", "-t", "4"],
    ];

    for args in cases {
        roundtrip(args, &data);
    }

    let zero_data = vec![0; 96_000];
    roundtrip(
        &[
            "-c",
            "zstd",
            "-f",
            "truncprec",
            "--filter-meta",
            "16",
            "-t",
            "4",
        ],
        &zero_data,
    );
}

#[test]
fn default_compress_parameters_match_c_example_frame() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("input.bin");
    let compressed = dir.path().join("output.b2frame");
    fs::write(&input, b"payload").unwrap();

    assert_success_c_stats_stdout(
        run([
            OsStr::new("compress"),
            input.as_os_str(),
            compressed.as_os_str(),
        ]),
        "Compression ratio: ",
        "Compression time: ",
    );

    let schunk = Schunk::open_offset(&compressed, 0).unwrap();
    assert_eq!(schunk.cparams.compcode, BLOSC_BLOSCLZ);
    assert_eq!(schunk.cparams.clevel, 9);
    assert_eq!(schunk.cparams.typesize, 1);
    assert_eq!(schunk.cparams.blocksize, 0);
    assert_eq!(schunk.cparams.splitmode, BLOSC_FORWARD_COMPAT_SPLIT);
    assert_eq!(schunk.cparams.nthreads, 4);
    assert_eq!(schunk.dparams.nthreads, 4);
    assert_eq!(
        schunk.cparams.filters[BLOSC2_MAX_FILTERS - 1],
        BLOSC_SHUFFLE
    );
    assert_eq!(schunk.cparams.filters_meta, [0; BLOSC2_MAX_FILTERS]);
    assert!(!schunk.cparams.use_dict);
}

#[test]
fn compress_use_dict_flag_is_stored_in_frame_params() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("input.bin");
    let compressed = dir.path().join("output.b2frame");
    fs::write(&input, compressible_data(2_000_000)).unwrap();

    assert_success(run([
        OsStr::new("compress"),
        input.as_os_str(),
        compressed.as_os_str(),
        OsStr::new("--codec"),
        OsStr::new("zstd"),
        OsStr::new("--use-dict"),
    ]));

    let schunk = Schunk::open_offset(&compressed, 0).unwrap();
    assert_eq!(schunk.cparams.compcode, BLOSC_ZSTD);
    assert!(schunk.cparams.use_dict);
}

#[test]
fn invalid_codec_filter_and_splitmode_fail_cleanly() {
    // Rust-specific CLI validation for flags that the C example does not parse.
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("input.bin");
    let output = dir.path().join("output.b2frame");
    fs::write(&input, b"payload").unwrap();

    assert_failure_code_stderr_contains(
        run([
            OsStr::new("compress"),
            input.as_os_str(),
            output.as_os_str(),
            OsStr::new("--codec"),
            OsStr::new("snappy"),
        ]),
        1,
        "Unknown codec",
    );
    assert!(!output.exists());

    assert_failure_code_stderr_contains(
        run([
            OsStr::new("compress"),
            input.as_os_str(),
            output.as_os_str(),
            OsStr::new("--filter"),
            OsStr::new("checksum"),
        ]),
        1,
        "Unknown filter",
    );
    assert!(!output.exists());

    assert_failure_code_stderr_contains(
        run([
            OsStr::new("compress"),
            input.as_os_str(),
            output.as_os_str(),
            OsStr::new("--splitmode"),
            OsStr::new("sideways"),
        ]),
        1,
        "Unknown split mode",
    );
    assert!(!output.exists());
}

#[test]
fn invalid_thread_options_are_rejected() {
    // Rust-specific CLI validation for thread flags that the C examples do not parse.
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("input.bin");
    let compressed = dir.path().join("output.b2frame");
    let restored = dir.path().join("restored.bin");
    fs::write(&input, b"payload").unwrap();

    assert_failure_contains(
        run([
            OsStr::new("compress"),
            input.as_os_str(),
            compressed.as_os_str(),
            OsStr::new("--nthreads"),
            OsStr::new("0"),
        ]),
        "invalid value",
    );

    assert_success(run([
        OsStr::new("compress"),
        input.as_os_str(),
        compressed.as_os_str(),
    ]));

    assert_failure_contains(
        run([
            OsStr::new("decompress"),
            compressed.as_os_str(),
            restored.as_os_str(),
            OsStr::new("--nthreads"),
            OsStr::new("0"),
        ]),
        "invalid value",
    );
}

#[test]
fn corrupt_input_fails_without_replacing_existing_output() {
    // Rust-specific safety check: the C example dereferences the failed open result before
    // opening the output, while the Rust CLI reports the open failure explicitly.
    let dir = tempfile::tempdir().unwrap();
    let corrupt = dir.path().join("corrupt.b2frame");
    let output = dir.path().join("restored.bin");
    fs::write(&corrupt, b"not a blosc frame").unwrap();
    fs::write(&output, b"keep me").unwrap();

    assert_failure_code_stderr_contains(
        run([
            OsStr::new("decompress"),
            corrupt.as_os_str(),
            output.as_os_str(),
        ]),
        1,
        "Failed to open frame",
    );
    assert_eq!(fs::read(output).unwrap(), b"keep me");
}

#[test]
fn missing_decompress_input_fails_without_replacing_existing_output() {
    // Rust-specific safety check: the C example dereferences the failed open result before
    // opening the output, while the Rust CLI reports the open failure explicitly.
    let dir = tempfile::tempdir().unwrap();
    let missing_input = dir.path().join("missing.b2frame");
    let output = dir.path().join("restored.bin");
    fs::write(&output, b"keep me").unwrap();

    assert_failure_code_stderr_contains(
        run([
            OsStr::new("decompress"),
            missing_input.as_os_str(),
            output.as_os_str(),
        ]),
        1,
        "Failed to open frame",
    );
    assert_eq!(fs::read(output).unwrap(), b"keep me");
}

#[test]
fn successful_commands_overwrite_existing_outputs() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("input.bin");
    let compressed = dir.path().join("output.b2frame");
    let restored = dir.path().join("restored.bin");
    let data = compressible_data(4096);
    fs::write(&input, &data).unwrap();
    fs::write(&compressed, b"old compressed contents").unwrap();
    fs::write(&restored, b"old restored contents").unwrap();

    assert_success_c_stats_stdout(
        run([
            OsStr::new("compress"),
            input.as_os_str(),
            compressed.as_os_str(),
            OsStr::new("--codec"),
            OsStr::new("lz4"),
        ]),
        "Compression ratio: ",
        "Compression time: ",
    );
    assert_ne!(fs::read(&compressed).unwrap(), b"old compressed contents");

    assert_success_c_stats_stdout(
        run([
            OsStr::new("decompress"),
            compressed.as_os_str(),
            restored.as_os_str(),
        ]),
        "Decompression ratio: ",
        "Decompression time: ",
    );
    assert_eq!(fs::read(restored).unwrap(), data);
}

#[test]
fn decompress_accepts_file_url_input_like_c_schunk_open() {
    let dir = tempfile::Builder::new()
        .prefix("cli-file-url-input-")
        .tempdir_in(".")
        .unwrap();
    let input = dir.path().join("input.bin");
    let compressed = dir.path().join("output.b2frame");
    let restored = dir.path().join("restored.bin");
    let data = compressible_data(4096);
    fs::write(&input, &data).unwrap();

    assert_success(run([
        OsStr::new("compress"),
        input.as_os_str(),
        compressed.as_os_str(),
    ]));

    let file_url = repo_relative_file_url(&compressed);
    assert_success_c_stats_stdout(
        run([
            OsStr::new("decompress"),
            OsStr::new(&file_url),
            restored.as_os_str(),
        ]),
        "Decompression ratio: ",
        "Decompression time: ",
    );
    assert_eq!(fs::read(restored).unwrap(), data);
}

#[test]
fn compress_rejects_file_url_output_stale_target_like_c_frame_storage() {
    let dir = tempfile::Builder::new()
        .prefix("cli-file-url-output-")
        .tempdir_in(".")
        .unwrap();
    let input = dir.path().join("input.bin");
    let compressed = dir.path().join("url-output.b2frame");
    let data = compressible_data(4096);
    fs::write(&input, &data).unwrap();
    fs::write(&compressed, b"stale frame").unwrap();

    let file_url = repo_relative_file_url(&compressed);
    assert_failure_code_stderr_contains(
        run([
            OsStr::new("compress"),
            input.as_os_str(),
            OsStr::new(&file_url),
        ]),
        -1,
        "Error in appending data to destination file",
    );
    assert_eq!(fs::read(compressed).unwrap(), b"stale frame");
}

#[test]
fn decompress_rejects_file_url_output_like_c_fopen() {
    let dir = tempfile::Builder::new()
        .prefix("cli-file-url-decompress-output-")
        .tempdir_in(".")
        .unwrap();
    let input = dir.path().join("input.bin");
    let compressed = dir.path().join("output.b2frame");
    let restored = dir.path().join("restored.bin");
    fs::write(&input, b"payload").unwrap();

    assert_success(run([
        OsStr::new("compress"),
        input.as_os_str(),
        compressed.as_os_str(),
    ]));

    let file_url = repo_relative_file_url(&restored);
    assert_failure_code_stdout_ends_with(
        run([
            OsStr::new("decompress"),
            compressed.as_os_str(),
            OsStr::new(&file_url),
        ]),
        1,
        "Output file cannot be open.",
    );
    assert!(!restored.exists());
}

#[test]
fn compress_missing_output_parent_fails_before_opening_input() {
    // Rust-specific safety check: the C example does not check the schunk allocation result,
    // while the Rust CLI reports the destination initialization failure explicitly.
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("input.bin");
    let missing_output = dir.path().join("missing-parent").join("output.b2frame");
    fs::write(&input, b"payload").unwrap();

    assert_failure_code_stderr_contains(
        run([
            OsStr::new("compress"),
            input.as_os_str(),
            missing_output.as_os_str(),
        ]),
        1,
        "Error:",
    );
    assert!(!missing_output.exists());
}

#[test]
fn empty_input_creates_empty_frame_like_terminal_c_fread() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("empty.bin");
    let compressed = dir.path().join("empty.b2frame");
    let restored = dir.path().join("restored.bin");
    fs::write(&input, b"").unwrap();

    assert_success_c_stats_stdout(
        run([
            OsStr::new("compress"),
            input.as_os_str(),
            compressed.as_os_str(),
        ]),
        "Compression ratio: 0.0 MB -> 0.0 MB (",
        "Compression time: ",
    );

    let schunk = Schunk::open_offset(&compressed, 0).unwrap();
    assert_eq!(schunk.nchunks(), 1);
    assert_eq!(schunk.nbytes, 0);

    assert_success_c_stats_stdout(
        run([
            OsStr::new("decompress"),
            compressed.as_os_str(),
            restored.as_os_str(),
        ]),
        "Decompression ratio: 0.0 MB -> 0.0 MB (",
        "Decompression time: ",
    );
    assert_eq!(fs::read(restored).unwrap(), b"");
}

#[test]
fn default_chunk_buffer_handles_exact_and_partial_terminal_chunks() {
    let dir = tempfile::tempdir().unwrap();
    for (name, size, expected_chunks) in [("exact", 1_000_000, 2), ("partial", 1_000_001, 2)] {
        let input = dir.path().join(format!("{name}.bin"));
        let compressed = dir.path().join(format!("{name}.b2frame"));
        let restored = dir.path().join(format!("{name}.restored"));
        let data = compressible_data(size);
        fs::write(&input, &data).unwrap();

        assert_success(run([
            OsStr::new("compress"),
            input.as_os_str(),
            compressed.as_os_str(),
        ]));

        let schunk = Schunk::open_offset(&compressed, 0).unwrap();
        assert_eq!(schunk.chunksize, 1_000_000);
        assert_eq!(schunk.nchunks(), expected_chunks);

        assert_success(run([
            OsStr::new("decompress"),
            compressed.as_os_str(),
            restored.as_os_str(),
        ]));
        assert_eq!(fs::read(restored).unwrap(), data);
    }
}

#[test]
fn decompression_uses_fixed_chunk_buffer_for_variable_chunk_frames() {
    let dir = tempfile::tempdir().unwrap();
    let compressed = dir.path().join("variable.b2frame");
    let restored = dir.path().join("restored.bin");
    let mut schunk = Schunk::new(CParams::default(), DParams::default());
    schunk.append_buffer(b"first long chunk").unwrap();
    schunk.append_buffer(b"short").unwrap();
    schunk.append_buffer(b"another long chunk").unwrap();
    assert_eq!(schunk.chunksize, 0);
    {
        let mut writer = BufWriter::new(File::create(&compressed).unwrap());
        frame::write_frame_to_writer(&schunk, &mut writer).unwrap();
        writer.flush().unwrap();
    }

    assert_failure_code_stderr_contains(
        run([
            OsStr::new("decompress"),
            compressed.as_os_str(),
            restored.as_os_str(),
        ]),
        -6,
        "Decompression error.  Error code: -6",
    );
    assert_eq!(fs::read(restored).unwrap(), b"");
}

#[test]
fn compress_missing_input_replaces_existing_output_with_empty_frame() {
    let dir = tempfile::tempdir().unwrap();
    let missing_input = dir.path().join("missing.bin");
    let output = dir.path().join("output.b2frame");
    fs::write(&output, b"old compressed contents").unwrap();

    assert_failure_code_stdout_ends_with(
        run([
            OsStr::new("compress"),
            missing_input.as_os_str(),
            output.as_os_str(),
        ]),
        1,
        "Input file cannot be open.",
    );

    let schunk = Schunk::open_offset(&output, 0).unwrap();
    assert_eq!(schunk.nchunks(), 0);
    assert_eq!(schunk.nbytes, 0);
    assert_eq!(schunk.cbytes, 0);
}

#[test]
fn compress_removes_empty_output_directory_before_creating_frame() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("input.bin");
    let output = dir.path().join("output.b2frame");
    fs::write(&input, b"payload").unwrap();
    fs::create_dir(&output).unwrap();

    assert_success_c_stats_stdout(
        run([
            OsStr::new("compress"),
            input.as_os_str(),
            output.as_os_str(),
        ]),
        "Compression ratio: ",
        "Compression time: ",
    );

    assert!(output.is_file());
    assert_eq!(Schunk::open_offset(&output, 0).unwrap().nbytes, 7);
}

#[test]
fn compress_rejects_nonempty_output_directory_without_removing_contents() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("input.bin");
    let output = dir.path().join("output.b2frame");
    let sentinel = output.join("sentinel.txt");
    fs::write(&input, b"payload").unwrap();
    fs::create_dir(&output).unwrap();
    fs::write(&sentinel, b"keep me").unwrap();

    assert_failure_code_stderr_contains(
        run([
            OsStr::new("compress"),
            input.as_os_str(),
            output.as_os_str(),
        ]),
        -1,
        "Error in appending data to destination file",
    );

    assert!(output.is_dir());
    assert_eq!(fs::read(sentinel).unwrap(), b"keep me");
}

#[cfg(unix)]
#[test]
fn compress_removes_output_symlink_before_creating_frame() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("input.bin");
    let symlink_target = dir.path().join("target.bin");
    let output = dir.path().join("output.b2frame");
    fs::write(&input, b"payload").unwrap();
    fs::write(&symlink_target, b"keep target").unwrap();
    std::os::unix::fs::symlink(&symlink_target, &output).unwrap();

    assert_success_c_stats_stdout(
        run([
            OsStr::new("compress"),
            input.as_os_str(),
            output.as_os_str(),
        ]),
        "Compression ratio: ",
        "Compression time: ",
    );

    assert_eq!(fs::read(&symlink_target).unwrap(), b"keep target");
    assert!(!fs::symlink_metadata(&output)
        .unwrap()
        .file_type()
        .is_symlink());
    assert_eq!(Schunk::open_offset(&output, 0).unwrap().nbytes, 7);
}

#[test]
fn decompress_missing_output_parent_fails_after_opening_input() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("input.bin");
    let compressed = dir.path().join("output.b2frame");
    let missing_output = dir.path().join("missing-parent").join("restored.bin");
    fs::write(&input, b"payload").unwrap();

    assert_success(run([
        OsStr::new("compress"),
        input.as_os_str(),
        compressed.as_os_str(),
    ]));

    assert_failure_code_stdout_ends_with(
        run([
            OsStr::new("decompress"),
            compressed.as_os_str(),
            missing_output.as_os_str(),
        ]),
        1,
        "Output file cannot be open.",
    );
    assert!(!missing_output.exists());
}

#[cfg(unix)]
#[test]
fn decompress_follows_existing_output_symlink_like_c_fopen_wb() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("input.bin");
    let compressed = dir.path().join("output.b2frame");
    let symlink_target = dir.path().join("target.bin");
    let restored = dir.path().join("restored.bin");
    fs::write(&input, b"payload").unwrap();
    fs::write(&symlink_target, b"old target").unwrap();
    std::os::unix::fs::symlink(&symlink_target, &restored).unwrap();

    assert_success(run([
        OsStr::new("compress"),
        input.as_os_str(),
        compressed.as_os_str(),
    ]));
    assert_success_c_stats_stdout(
        run([
            OsStr::new("decompress"),
            compressed.as_os_str(),
            restored.as_os_str(),
        ]),
        "Decompression ratio: ",
        "Decompression time: ",
    );

    assert!(fs::symlink_metadata(&restored)
        .unwrap()
        .file_type()
        .is_symlink());
    assert_eq!(fs::read(symlink_target).unwrap(), b"payload");
}

#[cfg(unix)]
#[test]
fn decompression_ignores_output_write_errors_like_c_fwrite() {
    let dev_full = Path::new("/dev/full");
    if !dev_full.exists() {
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("input.bin");
    let compressed = dir.path().join("output.b2frame");
    fs::write(&input, b"payload").unwrap();

    assert_success(run([
        OsStr::new("compress"),
        input.as_os_str(),
        compressed.as_os_str(),
    ]));

    assert_success_c_stats_stdout(
        run([
            OsStr::new("decompress"),
            compressed.as_os_str(),
            dev_full.as_os_str(),
        ]),
        "Decompression ratio: ",
        "Decompression time: ",
    );
}

#[test]
fn decompression_failure_keeps_completed_chunks_in_output_stream() {
    let dir = tempfile::tempdir().unwrap();
    let compressed = dir.path().join("corrupt-second-chunk.b2frame");
    let restored = dir.path().join("restored.bin");
    let mut cparams = CParams::default();
    cparams.compcode = BLOSC_ZSTD;
    let mut schunk = Schunk::new(cparams, DParams::default());
    let first_chunk = b"first chunk".repeat(512);
    let second_chunk = b"second data".repeat(512);
    assert_eq!(first_chunk.len(), second_chunk.len());
    schunk.append_buffer(&first_chunk).unwrap();
    schunk.append_buffer(&second_chunk).unwrap();
    assert_eq!(schunk.chunksize, first_chunk.len());
    let header_size = i32::from_be_bytes(schunk.to_frame()[11..15].try_into().unwrap()) as usize;
    let second_chunk_start = header_size + schunk.chunks[0].len();
    let second_chunk_end = second_chunk_start + schunk.chunks[1].len();
    let mut frame = schunk.to_frame();
    frame[second_chunk_end - 1] ^= 0xff;
    fs::write(&compressed, frame).unwrap();
    fs::write(&restored, b"old destination").unwrap();

    let output = run([
        OsStr::new("decompress"),
        compressed.as_os_str(),
        restored.as_os_str(),
    ]);

    assert!(
        !output.status.success(),
        "command unexpectedly succeeded\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.starts_with("Blosc version info: "),
        "stdout should keep the C version banner before decompression failures\nstdout:\n{stdout}"
    );
    assert!(
        !stdout.contains("Decompression ratio: "),
        "C prints decompression stats only after all chunks succeed\nstdout:\n{stdout}"
    );
    assert_eq!(fs::read(restored).unwrap(), first_chunk);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("Decompression error.  Error code: "),
        "stderr did not contain decompression error\nstderr:\n{stderr}"
    );
}

#[cfg(unix)]
#[test]
fn commands_skip_stale_first_temp_sibling() {
    // Rust-specific regression coverage: stale temp-name siblings are not part of the C examples,
    // and should not affect the translated CLI behavior.
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("input.bin");
    let compressed = dir.path().join("output.b2frame");
    let restored = dir.path().join("restored.bin");
    let data = compressible_data(4096);
    fs::write(&input, &data).unwrap();

    assert_success(run_with_stale_first_temp(
        [
            OsStr::new("compress"),
            input.as_os_str(),
            compressed.as_os_str(),
        ],
        &compressed,
    ));

    assert_success(run_with_stale_first_temp(
        [
            OsStr::new("decompress"),
            compressed.as_os_str(),
            restored.as_os_str(),
        ],
        &restored,
    ));
    assert_eq!(fs::read(restored).unwrap(), data);
}

#[cfg(unix)]
#[test]
fn compression_accepts_non_utf8_output_paths() {
    use std::os::unix::ffi::OsStringExt;

    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("input.bin");
    fs::write(&input, b"payload").unwrap();

    let output_name = std::ffi::OsString::from_vec(b"out-\xff.b2frame".to_vec());
    let output = dir.path().join(Path::new(&output_name));

    assert_success(run([
        OsStr::new("compress"),
        input.as_os_str(),
        output.as_os_str(),
    ]));
    assert!(output.exists());
}

#[cfg(unix)]
#[test]
fn decompression_accepts_non_utf8_input_paths() {
    use std::os::unix::ffi::OsStringExt;

    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("input.bin");
    fs::write(&input, b"payload").unwrap();

    let compressed_name = std::ffi::OsString::from_vec(b"in-\xff.b2frame".to_vec());
    let compressed = dir.path().join(Path::new(&compressed_name));
    let restored = dir.path().join("restored.bin");

    assert_success(run([
        OsStr::new("compress"),
        input.as_os_str(),
        compressed.as_os_str(),
    ]));
    assert_success(run([
        OsStr::new("decompress"),
        compressed.as_os_str(),
        restored.as_os_str(),
    ]));
    assert_eq!(fs::read(restored).unwrap(), b"payload");
}
