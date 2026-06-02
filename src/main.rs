//! Command-line interface for the Blosc2 compressor.
//!
//! Provides `compress` and `decompress` subcommands that read and write Blosc2 frame files
//! (`.b2frame`). Compression parameters (codec, level, type size, block size, split mode,
//! filter, thread count) are exposed as flags; decompression only needs a thread count.

use blosc2_pure_rs::compress::{CParams, DParams};
use blosc2_pure_rs::constants::*;
use blosc2_pure_rs::schunk::{frame, Schunk};
use blosc2_pure_rs::{Codec, Filter};
use clap::{Parser, Subcommand};
use std::ffi::OsString;
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

static TEMP_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);
const MAX_TEMP_CREATE_ATTEMPTS: usize = 128;
const CLI_DEFAULT_CHUNKSIZE: usize = 1_000_000;

#[derive(Parser)]
#[command(name = "blosc2", about = "Blosc2 compression tool")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Compress a file to Blosc2 frame format
    Compress {
        /// Input file path
        input: PathBuf,
        /// Output file path (.b2frame)
        output: PathBuf,
        /// Compression codec
        #[arg(short, long, default_value = "blosclz")]
        codec: String,
        /// Compression level (0-9)
        #[arg(short = 'l', long, default_value_t = 9, value_parser = clap::value_parser!(u8).range(0..=9))]
        clevel: u8,
        /// Type size in bytes
        #[arg(short, long, default_value_t = 1, value_parser = clap::value_parser!(i32).range(1..=BLOSC2_MAXTYPESIZE as i64))]
        typesize: i32,
        /// Explicit block size in bytes (0 = automatic)
        #[arg(short = 'b', long, default_value_t = 0, value_parser = clap::value_parser!(i32).range(0..))]
        blocksize: i32,
        /// Input bytes per frame chunk
        #[arg(long, default_value_t = CLI_DEFAULT_CHUNKSIZE, value_parser = parse_chunksize)]
        chunksize: usize,
        /// Split mode (always, never, auto, forward)
        #[arg(short = 's', long, default_value = "forward")]
        splitmode: String,
        /// Number of threads
        #[arg(short, long, default_value_t = 4, value_parser = clap::value_parser!(i16).range(1..))]
        nthreads: i16,
        /// Filter to apply (nofilter, shuffle, bitshuffle, delta, truncprec)
        #[arg(short, long, default_value = "shuffle")]
        filter: String,
        /// Filter metadata byte; for truncprec this is the precision in bits
        #[arg(long, default_value_t = 0)]
        filter_meta: u8,
    },
    /// Decompress a Blosc2 frame file
    Decompress {
        /// Input file path (.b2frame)
        input: PathBuf,
        /// Output file path
        output: PathBuf,
        /// Number of threads
        #[arg(short, long, default_value_t = 4, value_parser = clap::value_parser!(i16).range(1..))]
        nthreads: i16,
    },
}

/// Collected compression parameters passed to [`compress_file`].
struct CompressOptions {
    codec: Codec,
    clevel: u8,
    typesize: i32,
    blocksize: i32,
    chunksize: usize,
    splitmode: i32,
    nthreads: i16,
    filter: Filter,
    filter_meta: u8,
}

fn parse_chunksize(value: &str) -> Result<usize, String> {
    let chunksize = value
        .parse::<usize>()
        .map_err(|err| format!("invalid chunksize: {err}"))?;
    if chunksize == 0 || chunksize > BLOSC2_MAX_BUFFERSIZE as usize {
        return Err(format!(
            "chunksize must be in 1..={}",
            BLOSC2_MAX_BUFFERSIZE
        ));
    }
    Ok(chunksize)
}

/// Compresses `input` into a Blosc2 frame written to `output`.
///
/// Reads the input file in `chunksize` segments, appends each as a chunk to a [`Schunk`]
/// configured with `options`, streams the super-chunk frame to disk, and prints ratio and
/// throughput statistics. Output is first written to a sibling temporary file before replacing
/// `output`, so failed compression or serialization does not leave a partial destination file.
fn compress_file(input: &Path, output: &Path, options: CompressOptions) -> io::Result<()> {
    if options.chunksize == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "chunksize must be greater than zero",
        ));
    }

    let mut filters_meta = [0; BLOSC2_MAX_FILTERS];
    filters_meta[BLOSC2_MAX_FILTERS - 1] = options.filter_meta;
    let cparams = CParams {
        compcode: options.codec as u8,
        compcode_meta: 0,
        clevel: options.clevel,
        typesize: options.typesize,
        blocksize: options.blocksize,
        splitmode: options.splitmode,
        filters: [0, 0, 0, 0, 0, options.filter as u8],
        filters_meta,
        use_dict: false,
        nthreads: options.nthreads,
        ..Default::default()
    };
    let dparams = DParams {
        nthreads: options.nthreads,
        ..Default::default()
    };

    let mut schunk = Schunk::new(cparams, dparams);

    let start = Instant::now();

    let mut finput = File::open(input)?;
    let mut buf = vec![0u8; options.chunksize];

    loop {
        let bytes_read = read_next_chunk(&mut finput, &mut buf)?;
        let chunk = &buf[..bytes_read];
        schunk
            .append_buffer(chunk)
            .map_err(|e| io::Error::other(format!("Error compressing: {e}")))?;
        if bytes_read < options.chunksize {
            break;
        }
    }

    let nbytes = schunk.nbytes;
    let cbytes = schunk.cbytes;
    atomic_write_with(output, |writer| {
        frame::write_frame_to_writer(&schunk, writer)
    })?;
    let elapsed = start.elapsed().as_secs_f64();

    print_compression_stats(nbytes, cbytes, elapsed);

    Ok(())
}

fn read_next_chunk<R: Read>(reader: &mut R, buf: &mut [u8]) -> io::Result<usize> {
    let mut total_read = 0;
    while total_read < buf.len() {
        let bytes_read = reader.read(&mut buf[total_read..])?;
        if bytes_read == 0 {
            break;
        }
        total_read += bytes_read;
    }
    Ok(total_read)
}

/// Decompresses a Blosc2 frame at `input` into the raw file at `output`.
///
/// Opens the frame as a [`Schunk`], iterates over its chunks decoding each in turn, and writes
/// the concatenated result via a buffered temporary file before replacing `output`. Empty frames
/// produce an empty output file.
/// Prints ratio and throughput statistics on completion.
fn decompress_file(input: &Path, output: &Path, nthreads: i16) -> io::Result<()> {
    let mut schunk = Schunk::open_offset(input, 0)
        .map_err(|e| io::Error::other(format!("Failed to open frame: {e}")))?;
    schunk.dparams.nthreads = nthreads;

    let start = Instant::now();
    atomic_write_with(output, |foutput| {
        for i in 0..schunk.nchunks() {
            let data = schunk
                .decompress_chunk(i)
                .map_err(|e| io::Error::other(format!("Decompression error: {e}")))?;
            foutput.write_all(&data)?;
        }
        Ok(())
    })?;

    let nbytes = schunk.nbytes;
    let cbytes = schunk.cbytes;
    let elapsed = start.elapsed().as_secs_f64();

    print_decompression_stats(nbytes, cbytes, elapsed);

    Ok(())
}

fn atomic_write_with<F>(output: &Path, write: F) -> io::Result<()>
where
    F: FnOnce(&mut BufWriter<File>) -> io::Result<()>,
{
    let (temp_path, temp_file) = create_temp_output_file(output)?;
    let write_result = (|| {
        let mut writer = BufWriter::new(temp_file);
        write(&mut writer)?;
        writer.flush()
    })();

    if let Err(err) = write_result {
        let _ = std::fs::remove_file(&temp_path);
        return Err(err);
    }
    replace_output(&temp_path, output)
}

fn create_temp_output_file(output: &Path) -> io::Result<(PathBuf, File)> {
    let mut last_exists = None;
    for _ in 0..MAX_TEMP_CREATE_ATTEMPTS {
        let temp_path = temp_output_path(output);
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp_path)
        {
            Ok(file) => return Ok((temp_path, file)),
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {
                last_exists = Some(err);
            }
            Err(err) => return Err(err),
        }
    }

    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        format!(
            "failed to create a unique temporary output after {MAX_TEMP_CREATE_ATTEMPTS} attempts: {}",
            last_exists
                .map(|err| err.to_string())
                .unwrap_or_else(|| "temporary output already exists".to_string())
        ),
    ))
}

fn temp_output_path(output: &Path) -> PathBuf {
    let counter = TEMP_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
    temp_output_path_for(output, counter)
}

fn temp_output_path_for(output: &Path, counter: u64) -> PathBuf {
    let file_name = output
        .file_name()
        .unwrap_or_else(|| ".blosc2-output".as_ref());
    let mut temp_name = OsString::from(".");
    temp_name.push(file_name);
    temp_name.push(format!(".tmp.{}.{}", std::process::id(), counter));
    output.with_file_name(temp_name)
}

fn replace_output(temp_path: &Path, output: &Path) -> io::Result<()> {
    replace_output_impl(temp_path, output).inspect_err(|_err| {
        if temp_path.exists() {
            let _ = std::fs::remove_file(temp_path);
        }
    })
}

#[cfg(windows)]
fn replace_output_impl(temp_path: &Path, output: &Path) -> io::Result<()> {
    match std::fs::remove_file(output) {
        Ok(()) => {}
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Err(err) => return Err(err),
    }
    std::fs::rename(temp_path, output)
}

#[cfg(not(windows))]
fn replace_output_impl(temp_path: &Path, output: &Path) -> io::Result<()> {
    std::fs::rename(temp_path, output)
}

/// Returns `numerator / denominator` as `f64`, or `0.0` when `denominator` is not positive.
fn ratio(numerator: i64, denominator: i64) -> f64 {
    if denominator > 0 {
        numerator as f64 / denominator as f64
    } else {
        0.0
    }
}

/// Computes throughput in MiB/s given a byte count and an elapsed time in seconds.
///
/// Returns `0.0` for non-positive elapsed times to avoid division by zero.
fn throughput_mib(nbytes: i64, elapsed_secs: f64) -> f64 {
    if elapsed_secs > 0.0 {
        nbytes as f64 / (elapsed_secs * 1024.0 * 1024.0)
    } else {
        0.0
    }
}

fn c_general_3(value: f64) -> String {
    if value == 0.0 {
        return "0".to_string();
    }
    if !value.is_finite() {
        return value.to_string();
    }

    let sign = if value.is_sign_negative() { "-" } else { "" };
    let abs = value.abs();
    let exponent = abs.log10().floor() as i32;

    let mut formatted = if !(-4..3).contains(&exponent) {
        trim_general_number(format!("{abs:.2e}"))
    } else {
        let decimals = (2 - exponent).max(0) as usize;
        trim_general_number(format!("{abs:.decimals$}"))
    };

    if !sign.is_empty() {
        formatted.insert_str(0, sign);
    }
    formatted
}

fn trim_general_number(mut value: String) -> String {
    let exponent_index = value.find(['e', 'E']);
    let exponent = exponent_index.map(|idx| value.split_off(idx));

    if value.contains('.') {
        while value.ends_with('0') {
            value.pop();
        }
        if value.ends_with('.') {
            value.pop();
        }
    }

    if let Some(exponent) = exponent {
        value.push_str(&normalize_c_exponent(&exponent));
    }
    value
}

fn normalize_c_exponent(exponent: &str) -> String {
    let (marker, digits) = exponent.split_at(1);
    let parsed = digits.parse::<i32>().unwrap_or(0);
    format!("{marker}{parsed:+03}")
}

fn compression_stats_lines(nbytes: i64, cbytes: i64, elapsed_secs: f64) -> (String, String) {
    let mb = 1024.0 * 1024.0;
    (
        format!(
            "Compression ratio: {:.1} MB -> {:.1} MB ({:.1}x)",
            nbytes as f64 / mb,
            cbytes as f64 / mb,
            ratio(nbytes, cbytes)
        ),
        format!(
            "Compression time: {} s, {:.1} MB/s",
            c_general_3(elapsed_secs),
            throughput_mib(nbytes, elapsed_secs)
        ),
    )
}

fn print_compression_stats(nbytes: i64, cbytes: i64, elapsed_secs: f64) {
    let (ratio_line, time_line) = compression_stats_lines(nbytes, cbytes, elapsed_secs);
    println!("{ratio_line}");
    println!("{time_line}");
}

fn decompression_stats_lines(nbytes: i64, cbytes: i64, elapsed_secs: f64) -> (String, String) {
    let mb = 1024.0 * 1024.0;
    (
        format!(
            "Decompression ratio: {:.1} MB -> {:.1} MB ({:.1}x)",
            cbytes as f64 / mb,
            nbytes as f64 / mb,
            ratio(cbytes, nbytes)
        ),
        format!(
            "Decompression time: {} s, {:.1} MB/s",
            c_general_3(elapsed_secs),
            throughput_mib(nbytes, elapsed_secs)
        ),
    )
}

fn print_decompression_stats(nbytes: i64, cbytes: i64, elapsed_secs: f64) {
    let (ratio_line, time_line) = decompression_stats_lines(nbytes, cbytes, elapsed_secs);
    println!("{ratio_line}");
    println!("{time_line}");
}

fn version_info_line() -> String {
    format!(
        "Blosc version info: {} ({})",
        BLOSC2_VERSION_STRING, BLOSC2_VERSION_DATE
    )
}

fn print_version_info() {
    println!("{}", version_info_line());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn temp_output_creation_skips_stale_sibling_name() {
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("out.b2frame");
        let stale = temp_output_path_for(&output, 0);
        std::fs::write(&stale, b"stale").unwrap();

        TEMP_FILE_COUNTER.store(0, Ordering::Relaxed);
        let (created, file) = create_temp_output_file(&output).unwrap();
        drop(file);

        assert_ne!(created, stale);
        assert_eq!(std::fs::read(&stale).unwrap(), b"stale");
        assert!(created.exists());
    }

    #[test]
    fn replace_output_overwrites_existing_destination() {
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("out.b2frame");
        let temp = dir.path().join(".out.b2frame.tmp");
        std::fs::write(&output, b"old").unwrap();
        std::fs::write(&temp, b"new").unwrap();

        replace_output(&temp, &output).unwrap();

        assert_eq!(std::fs::read(&output).unwrap(), b"new");
        assert!(!temp.exists());
    }

    #[test]
    fn cli_default_chunksize_matches_c_example() {
        assert_eq!(CLI_DEFAULT_CHUNKSIZE, 1_000_000);
        assert_eq!(
            parse_chunksize(&CLI_DEFAULT_CHUNKSIZE.to_string()).unwrap(),
            CLI_DEFAULT_CHUNKSIZE
        );
    }

    #[test]
    fn version_info_line_matches_c_example_prefix() {
        assert_eq!(
            version_info_line(),
            format!(
                "Blosc version info: {} ({})",
                BLOSC2_VERSION_STRING, BLOSC2_VERSION_DATE
            )
        );
    }

    #[test]
    fn compression_stats_use_schunk_cbytes_like_c_example() {
        let (ratio_line, time_line) =
            compression_stats_lines(4 * 1024 * 1024, 2 * 1024 * 1024, 0.5);

        assert_eq!(ratio_line, "Compression ratio: 4.0 MB -> 2.0 MB (2.0x)");
        assert_eq!(time_line, "Compression time: 0.5 s, 8.0 MB/s");
    }

    #[test]
    fn decompression_stats_match_c_example_order() {
        let (ratio_line, time_line) =
            decompression_stats_lines(4 * 1024 * 1024, 2 * 1024 * 1024, 0.25);

        assert_eq!(ratio_line, "Decompression ratio: 2.0 MB -> 4.0 MB (0.5x)");
        assert_eq!(time_line, "Decompression time: 0.25 s, 16.0 MB/s");
    }

    #[test]
    fn stats_elapsed_seconds_use_c_general_precision() {
        assert_eq!(c_general_3(0.0), "0");
        assert_eq!(c_general_3(0.0185), "0.0185");
        assert_eq!(c_general_3(12.345), "12.3");
        assert_eq!(c_general_3(1234.0), "1.23e+03");
    }

    #[test]
    fn read_next_chunk_fills_buffer_across_short_reads_like_fread() {
        struct ShortReader {
            data: Vec<u8>,
            pos: usize,
            max_read: usize,
        }

        impl Read for ShortReader {
            fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
                if self.pos == self.data.len() {
                    return Ok(0);
                }
                let len = self.max_read.min(buf.len()).min(self.data.len() - self.pos);
                buf[..len].copy_from_slice(&self.data[self.pos..self.pos + len]);
                self.pos += len;
                Ok(len)
            }
        }

        let mut reader = ShortReader {
            data: b"abcdefghijkl".to_vec(),
            pos: 0,
            max_read: 3,
        };
        let mut buf = [0u8; 8];

        assert_eq!(read_next_chunk(&mut reader, &mut buf).unwrap(), 8);
        assert_eq!(&buf, b"abcdefgh");
        assert_eq!(read_next_chunk(&mut reader, &mut buf).unwrap(), 4);
        assert_eq!(&buf[..4], b"ijkl");
    }

    #[test]
    fn compress_empty_input_creates_decompressible_empty_chunk_frame() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("empty.bin");
        let output = dir.path().join("empty.b2frame");
        let restored = dir.path().join("restored.bin");
        std::fs::write(&input, []).unwrap();

        compress_file(
            &input,
            &output,
            CompressOptions {
                codec: Codec::BloscLz,
                clevel: 9,
                typesize: 1,
                blocksize: 0,
                chunksize: CLI_DEFAULT_CHUNKSIZE,
                splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
                nthreads: 1,
                filter: Filter::Shuffle,
                filter_meta: 0,
            },
        )
        .unwrap();

        let schunk = Schunk::open_offset(&output, 0).unwrap();
        assert_eq!(schunk.nchunks(), 1);
        assert_eq!(schunk.nbytes, 0);
        assert!(schunk.cbytes > 0);

        decompress_file(&output, &restored, 1).unwrap();
        assert_eq!(std::fs::read(restored).unwrap(), b"");
    }

    #[test]
    fn compress_exact_chunk_multiple_roundtrips_after_c_example_final_append() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("exact.bin");
        let output = dir.path().join("exact.b2frame");
        let restored = dir.path().join("restored.bin");
        let data = b"abcdefgh".repeat(2);
        std::fs::write(&input, &data).unwrap();

        compress_file(
            &input,
            &output,
            CompressOptions {
                codec: Codec::BloscLz,
                clevel: 9,
                typesize: 1,
                blocksize: 0,
                chunksize: 8,
                splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
                nthreads: 1,
                filter: Filter::Shuffle,
                filter_meta: 0,
            },
        )
        .unwrap();

        let schunk = Schunk::open_offset(&output, 0).unwrap();
        assert_eq!(schunk.nbytes, data.len() as i64);

        decompress_file(&output, &restored, 1).unwrap();
        assert_eq!(std::fs::read(restored).unwrap(), data);
    }
}

/// Parses a split-mode name (case-insensitive) into its Blosc2 constant.
///
/// Recognizes `always`, `never`, `auto`, and `forward` (with optional `_split` suffix); returns
/// `None` for unknown values.
fn parse_splitmode(s: &str) -> Option<i32> {
    match s.to_lowercase().as_str() {
        "always" | "always_split" => Some(BLOSC_ALWAYS_SPLIT),
        "never" | "never_split" => Some(BLOSC_NEVER_SPLIT),
        "auto" | "auto_split" => Some(BLOSC_AUTO_SPLIT),
        "forward" | "forward_compat" | "forward_compat_split" => Some(BLOSC_FORWARD_COMPAT_SPLIT),
        _ => None,
    }
}

/// CLI entry point: parses arguments, configures the rayon thread pool, and dispatches to the
/// `compress` or `decompress` handler. Exits with status 1 on error.
fn main() {
    let cli = Cli::parse();
    print_version_info();

    // Set rayon global thread pool based on nthreads from first subcommand
    let nthreads = match &cli.command {
        Commands::Compress { nthreads, .. } | Commands::Decompress { nthreads, .. } => *nthreads,
    };
    if nthreads > 1 {
        rayon::ThreadPoolBuilder::new()
            .num_threads(nthreads as usize)
            .build_global()
            .ok(); // ignore if already set
    }

    let result = match &cli.command {
        Commands::Compress {
            input,
            output,
            codec,
            clevel,
            typesize,
            blocksize,
            chunksize,
            splitmode,
            nthreads,
            filter,
            filter_meta,
        } => {
            let codec = codec.parse::<Codec>().unwrap_or_else(|_| {
                eprintln!(
                    "Unknown codec '{}'. Available: blosclz, lz4, lz4hc, zlib, zstd",
                    codec
                );
                std::process::exit(1);
            });
            let filter = filter.parse::<Filter>().unwrap_or_else(|_| {
                eprintln!(
                    "Unknown filter '{}'. Available: nofilter, shuffle, bitshuffle, delta, truncprec",
                    filter
                );
                std::process::exit(1);
            });
            let splitmode = parse_splitmode(splitmode).unwrap_or_else(|| {
                eprintln!(
                    "Unknown split mode '{}'. Available: always, never, auto, forward",
                    splitmode
                );
                std::process::exit(1);
            });
            compress_file(
                input,
                output,
                CompressOptions {
                    codec,
                    clevel: *clevel,
                    typesize: *typesize,
                    blocksize: *blocksize,
                    chunksize: *chunksize,
                    splitmode,
                    nthreads: *nthreads,
                    filter,
                    filter_meta: *filter_meta,
                },
            )
        }
        Commands::Decompress {
            input,
            output,
            nthreads,
        } => decompress_file(input, output, *nthreads),
    };

    if let Err(e) = result {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}
