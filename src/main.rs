//! Command-line interface for the Blosc2 compressor.
//!
//! Provides `compress` and `decompress` subcommands that read and write Blosc2 frame files
//! (`.b2frame`). Compression parameters (codec, level, type size, block size, split mode,
//! filter, thread count) are exposed as flags; decompression can optionally override the frame
//! thread count.

use blosc2_pure_rs::compress::{CParams, DParams};
use blosc2_pure_rs::constants::*;
use blosc2_pure_rs::schunk::{frame, Schunk};
use clap::{Parser, Subcommand};
use std::fs::File;
use std::io::{self, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

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
        /// Number of threads; defaults to the value stored in the frame
        #[arg(short, long, value_parser = clap::value_parser!(i16).range(1..))]
        nthreads: Option<i16>,
    },
}

/// Collected compression parameters passed to [`compress_file`].
struct CompressOptions {
    codec: u8,
    clevel: u8,
    typesize: i32,
    blocksize: i32,
    chunksize: usize,
    splitmode: i32,
    nthreads: i16,
    filter: u8,
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
/// throughput statistics. Like the C example, the destination is created/truncated before the
/// input is opened and before compression starts.
fn compress_file(input: &Path, output: &Path, options: CompressOptions) -> io::Result<()> {
    if options.chunksize == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "chunksize must be greater than zero",
        ));
    }

    let foutput = File::create(output)?;

    let mut filters_meta = [0; BLOSC2_MAX_FILTERS];
    filters_meta[BLOSC2_MAX_FILTERS - 1] = options.filter_meta;
    let cparams = CParams {
        compcode: options.codec,
        compcode_meta: 0,
        clevel: options.clevel,
        typesize: options.typesize,
        blocksize: options.blocksize,
        splitmode: options.splitmode,
        filters: [0, 0, 0, 0, 0, options.filter],
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
    {
        let mut writer = BufWriter::new(foutput);
        frame::write_frame_to_writer(&schunk, &mut writer)?;
        writer.flush()?;
    }
    let elapsed = start.elapsed().as_secs_f64();
    let cbytes = frame_equivalent_cbytes(output)?;

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
/// the concatenated result directly to `output`. Empty frames produce an empty output file.
/// Like the C example, mid-stream decompression failures can leave the chunks already written.
/// Prints ratio and throughput statistics on completion.
fn decompress_file(input: &Path, output: &Path, nthreads: Option<i16>) -> io::Result<()> {
    let mut schunk = Schunk::open_offset(input, 0)
        .map_err(|e| io::Error::other(format!("Failed to open frame: {e}")))?;
    apply_decompression_nthreads_override(&mut schunk, nthreads);

    let start = Instant::now();
    write_decompressed_chunks_direct(&schunk, output)?;

    let nbytes = schunk.nbytes;
    let cbytes = schunk.cbytes;
    let elapsed = start.elapsed().as_secs_f64();

    print_decompression_stats(nbytes, cbytes, elapsed);

    Ok(())
}

fn apply_decompression_nthreads_override(schunk: &mut Schunk, nthreads: Option<i16>) {
    if let Some(nthreads) = nthreads {
        schunk.dparams.nthreads = nthreads;
    }
}

fn write_decompressed_chunks_direct(schunk: &Schunk, output: &Path) -> io::Result<()> {
    let mut foutput = File::create(output)?;
    for i in 0..schunk.nchunks() {
        let data = schunk
            .decompress_chunk(i)
            .map_err(|e| io::Error::other(format!("Decompression error: {e}")))?;
        // Intentional Rust divergence from the C example: C ignores fwrite's result, while the
        // CLI reports checked write failures instead of silently producing truncated output.
        foutput.write_all(&data)?;
    }
    foutput.flush()?;
    Ok(())
}

fn frame_equivalent_cbytes(path: &Path) -> io::Result<i64> {
    Schunk::open_offset(path, 0)
        .map(|schunk| schunk.cbytes)
        .map_err(|e| io::Error::other(format!("Failed to reopen frame for stats: {e}")))
}

/// Returns `numerator / denominator` using the same floating-point behavior as the C examples.
fn ratio(numerator: i64, denominator: i64) -> f64 {
    c_float(numerator) / c_float(denominator)
}

/// Computes throughput in MiB/s given a byte count and an elapsed time in seconds.
///
fn throughput_mib(nbytes: i64, elapsed_secs: f64) -> f64 {
    c_float(nbytes) / (elapsed_secs * 1024.0 * 1024.0)
}

fn c_float(value: i64) -> f64 {
    value as f32 as f64
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
    let exponent = rounded_scientific_exponent(abs);

    let mut formatted = if exponent < -4 || exponent >= 3 {
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

fn rounded_scientific_exponent(abs: f64) -> i32 {
    let scientific = format!("{abs:.2e}");
    let exponent = scientific
        .find(['e', 'E'])
        .map(|idx| &scientific[idx + 1..])
        .unwrap_or("0");
    exponent.parse::<i32>().unwrap_or(0)
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
            c_float(nbytes) / mb,
            c_float(cbytes) / mb,
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
            c_float(cbytes) / mb,
            c_float(nbytes) / mb,
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
    fn compression_stats_use_frame_equivalent_cbytes_like_c_example() {
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
    fn decompression_stats_empty_payload_ratio_matches_c_division() {
        let (ratio_line, _) = decompression_stats_lines(0, 128, 0.25);

        assert_eq!(ratio_line, "Decompression ratio: 0.0 MB -> 0.0 MB (infx)");
    }

    #[test]
    fn decompression_nthreads_override_is_optional() {
        let mut schunk = Schunk::new(CParams::default(), DParams::default());
        schunk.dparams.nthreads = 7;

        apply_decompression_nthreads_override(&mut schunk, None);
        assert_eq!(schunk.dparams.nthreads, 7);

        apply_decompression_nthreads_override(&mut schunk, Some(3));
        assert_eq!(schunk.dparams.nthreads, 3);
    }

    #[test]
    fn decompress_cli_leaves_nthreads_unset_by_default() {
        let cli = Cli::try_parse_from(["blosc2", "decompress", "in.b2frame", "out.bin"]).unwrap();

        match cli.command {
            Commands::Decompress { nthreads, .. } => assert_eq!(nthreads, None),
            _ => panic!("expected decompress command"),
        }
    }

    #[test]
    fn decompress_cli_accepts_explicit_nthreads_override() {
        let cli = Cli::try_parse_from([
            "blosc2",
            "decompress",
            "in.b2frame",
            "out.bin",
            "--nthreads",
            "3",
        ])
        .unwrap();

        match cli.command {
            Commands::Decompress { nthreads, .. } => assert_eq!(nthreads, Some(3)),
            _ => panic!("expected decompress command"),
        }
    }

    #[test]
    fn stats_byte_counts_use_c_float_precision() {
        let (ratio_line, time_line) = compression_stats_lines(16_777_217, 1, 1.0);

        assert_eq!(
            ratio_line,
            "Compression ratio: 16.0 MB -> 0.0 MB (16777216.0x)"
        );
        assert_eq!(time_line, "Compression time: 1 s, 16.0 MB/s");
    }

    #[test]
    fn stats_elapsed_seconds_use_c_general_precision() {
        assert_eq!(c_general_3(0.0), "0");
        assert_eq!(c_general_3(0.0185), "0.0185");
        assert_eq!(c_general_3(12.345), "12.3");
        assert_eq!(c_general_3(999.5), "1e+03");
        assert_eq!(c_general_3(1234.0), "1.23e+03");
        assert_eq!(c_general_3(0.00009995), "0.0001");
        assert_eq!(c_general_3(0.00009994), "9.99e-05");
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

    fn default_test_options(chunksize: usize) -> CompressOptions {
        CompressOptions {
            codec: BLOSC_BLOSCLZ as u8,
            clevel: 9,
            typesize: 1,
            blocksize: 0,
            chunksize,
            splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
            nthreads: 1,
            filter: BLOSC_SHUFFLE as u8,
            filter_meta: 0,
        }
    }

    #[test]
    fn compress_truncates_destination_before_input_open_failure_like_c_example() {
        let dir = tempfile::tempdir().unwrap();
        let missing_input = dir.path().join("missing.bin");
        let output = dir.path().join("out.b2frame");
        std::fs::write(&output, b"old destination").unwrap();

        let err = compress_file(
            &missing_input,
            &output,
            default_test_options(CLI_DEFAULT_CHUNKSIZE),
        )
        .unwrap_err();

        assert_eq!(err.kind(), io::ErrorKind::NotFound);
        assert_eq!(std::fs::read(&output).unwrap(), b"");
    }

    #[test]
    fn decompression_writes_directly_and_keeps_completed_chunks_on_later_failure() {
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("restored.bin");
        let mut schunk = Schunk::new(CParams::default(), DParams::default());
        schunk.append_buffer(b"first chunk").unwrap();
        schunk.append_buffer(b"second chunk").unwrap();
        schunk.chunks[1] = b"not a valid blosc chunk".to_vec();
        std::fs::write(&output, b"old destination").unwrap();

        let err = write_decompressed_chunks_direct(&schunk, &output).unwrap_err();

        assert_eq!(err.kind(), io::ErrorKind::Other);
        assert_eq!(std::fs::read(&output).unwrap(), b"first chunk");
    }

    #[test]
    fn frame_equivalent_cbytes_uses_serialized_special_offsets() {
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("special-offsets.b2frame");
        let mut schunk = Schunk::new(CParams::default(), DParams::default());
        schunk.append_buffer(&vec![0u8; 128]).unwrap();
        assert!(schunk.cbytes > 0);

        {
            let mut writer = BufWriter::new(File::create(&output).unwrap());
            frame::write_frame_to_writer(&schunk, &mut writer).unwrap();
            writer.flush().unwrap();
        }

        assert_eq!(frame_equivalent_cbytes(&output).unwrap(), 0);
    }

    #[test]
    fn compress_empty_input_creates_decompressible_empty_chunk_frame() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("empty.bin");
        let output = dir.path().join("empty.b2frame");
        let restored = dir.path().join("restored.bin");
        std::fs::write(&input, []).unwrap();

        compress_file(&input, &output, default_test_options(CLI_DEFAULT_CHUNKSIZE)).unwrap();

        let schunk = Schunk::open_offset(&output, 0).unwrap();
        assert_eq!(schunk.nchunks(), 1);
        assert_eq!(schunk.nbytes, 0);
        assert!(schunk.cbytes > 0);

        decompress_file(&output, &restored, Some(1)).unwrap();
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

        compress_file(&input, &output, default_test_options(8)).unwrap();

        let schunk = Schunk::open_offset(&output, 0).unwrap();
        assert_eq!(schunk.nbytes, data.len() as i64);

        decompress_file(&output, &restored, Some(1)).unwrap();
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

fn parse_codec(s: &str) -> Option<u8> {
    match s.to_lowercase().as_str() {
        "blosclz" => Some(BLOSC_BLOSCLZ as u8),
        "lz4" => Some(BLOSC_LZ4 as u8),
        "lz4hc" => Some(BLOSC_LZ4HC as u8),
        "zlib" => Some(BLOSC_ZLIB as u8),
        "zstd" => Some(BLOSC_ZSTD as u8),
        _ => None,
    }
}

fn parse_filter(s: &str) -> Option<u8> {
    match s.to_lowercase().as_str() {
        "nofilter" | "none" => Some(BLOSC_NOFILTER as u8),
        "shuffle" => Some(BLOSC_SHUFFLE as u8),
        "bitshuffle" | "bit_shuffle" => Some(BLOSC_BITSHUFFLE as u8),
        "delta" => Some(BLOSC_DELTA as u8),
        "truncprec" | "trunc_prec" => Some(BLOSC_TRUNC_PREC as u8),
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
        Commands::Compress { nthreads, .. } => Some(*nthreads),
        Commands::Decompress { nthreads, .. } => *nthreads,
    };
    if let Some(nthreads) = nthreads.filter(|nthreads| *nthreads > 1) {
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
            let codec = parse_codec(codec).unwrap_or_else(|| {
                eprintln!(
                    "Unknown codec '{}'. Available: blosclz, lz4, lz4hc, zlib, zstd",
                    codec
                );
                std::process::exit(1);
            });
            let filter = parse_filter(filter).unwrap_or_else(|| {
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
