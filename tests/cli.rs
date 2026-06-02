#![cfg(feature = "cli")]

use std::ffi::OsStr;
use std::fs;
use std::path::Path;
use std::process::{Command, Output};

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
fn decompress_help_documents_nthreads_default() {
    let help = run(["decompress", "--help"]);
    assert_stdout_contains(help, "-n, --nthreads <NTHREADS>");
    let help = run(["decompress", "--help"]);
    assert_stdout_contains(help, "[default: 4]");
}

#[test]
fn documented_compress_flags_roundtrip() {
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
fn invalid_codec_filter_and_splitmode_fail_cleanly() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("input.bin");
    let output = dir.path().join("output.b2frame");
    fs::write(&input, b"payload").unwrap();

    assert_failure_contains(
        run([
            OsStr::new("compress"),
            input.as_os_str(),
            output.as_os_str(),
            OsStr::new("--codec"),
            OsStr::new("snappy"),
        ]),
        "Unknown codec",
    );
    assert!(!output.exists());

    assert_failure_contains(
        run([
            OsStr::new("compress"),
            input.as_os_str(),
            output.as_os_str(),
            OsStr::new("--filter"),
            OsStr::new("checksum"),
        ]),
        "Unknown filter",
    );
    assert!(!output.exists());

    assert_failure_contains(
        run([
            OsStr::new("compress"),
            input.as_os_str(),
            output.as_os_str(),
            OsStr::new("--splitmode"),
            OsStr::new("sideways"),
        ]),
        "Unknown split mode",
    );
    assert!(!output.exists());
}

#[test]
fn invalid_thread_options_are_rejected() {
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
    let dir = tempfile::tempdir().unwrap();
    let corrupt = dir.path().join("corrupt.b2frame");
    let output = dir.path().join("restored.bin");
    fs::write(&corrupt, b"not a blosc frame").unwrap();
    fs::write(&output, b"keep me").unwrap();

    assert_failure_contains(
        run([
            OsStr::new("decompress"),
            corrupt.as_os_str(),
            output.as_os_str(),
        ]),
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

    assert_success(run([
        OsStr::new("compress"),
        input.as_os_str(),
        compressed.as_os_str(),
        OsStr::new("--codec"),
        OsStr::new("lz4"),
    ]));
    assert_ne!(fs::read(&compressed).unwrap(), b"old compressed contents");

    assert_success(run([
        OsStr::new("decompress"),
        compressed.as_os_str(),
        restored.as_os_str(),
    ]));
    assert_eq!(fs::read(restored).unwrap(), data);
}

#[cfg(unix)]
#[test]
fn commands_skip_stale_first_temp_sibling() {
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
