//! Command-line interface for the Blosc2 compressor.
//!
//! Provides `compress` and `decompress` subcommands that read and write Blosc2 frame files
//! (`.b2frame`). Compression parameters (codec, level, type size, block size, split mode,
//! filter, thread count) are exposed as flags; decompression only needs a thread count.

use blosc2_pure_rs::compress::{CParams, DParams};
use blosc2_pure_rs::constants::*;
use blosc2_pure_rs::schunk::Schunk;
use blosc2_pure_rs::{Codec, Filter};
use clap::{Parser, Subcommand};
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

static TEMP_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

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
        #[arg(long, default_value_t = DEFAULT_CHUNKSIZE)]
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

/// Compresses `input` into a Blosc2 frame written to `output`.
///
/// Reads the input file in `chunksize` segments, appends each as a chunk to a [`Schunk`]
/// configured with `options`, serializes the super-chunk to disk, and prints ratio and
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
        let bytes_read = finput.read(&mut buf)?;
        if bytes_read == 0 {
            break;
        }
        schunk
            .append_buffer(&buf[..bytes_read])
            .map_err(|e| io::Error::other(format!("Error compressing: {e}")))?;
    }

    let nbytes = schunk.nbytes;
    atomic_write_all(output, &schunk.to_frame())?;
    let cbytes = std::fs::metadata(output)?.len() as i64;
    let elapsed = start.elapsed().as_secs_f64();

    let mb = 1024.0 * 1024.0;
    println!(
        "Compression ratio: {:.1} MB -> {:.1} MB ({:.1}x)",
        nbytes as f64 / mb,
        cbytes as f64 / mb,
        ratio(nbytes, cbytes)
    );
    println!(
        "Compression time: {:.3} s, {:.1} MB/s",
        elapsed,
        throughput_mib(nbytes, elapsed)
    );

    Ok(())
}

/// Decompresses a Blosc2 frame at `input` into the raw file at `output`.
///
/// Opens the frame as a [`Schunk`], iterates over its chunks decoding each in turn, and writes
/// the concatenated result via a buffered temporary file before replacing `output`. Empty frames
/// produce an empty output file.
/// Prints ratio and throughput statistics on completion.
fn decompress_file(input: &Path, output: &Path, nthreads: i16) -> io::Result<()> {
    let input_str = input
        .to_str()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "Invalid input path"))?;

    let mut schunk = Schunk::open(input_str)
        .map_err(|e| io::Error::other(format!("Failed to open frame: {e}")))?;
    schunk.dparams.nthreads = nthreads;

    if schunk.nchunks() == 0 {
        atomic_write_all(output, &[])?;
        println!("Decompression ratio: 0.0 MB -> 0.0 MB (0.0x)");
        println!("Decompression time: 0.000 s, 0.0 MB/s");
        return Ok(());
    }

    let start = Instant::now();
    let temp_path = temp_output_path(output);
    let temp_file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp_path)?;
    let write_result = (|| {
        let mut foutput = BufWriter::new(temp_file);

        for i in 0..schunk.nchunks() {
            let data = schunk
                .decompress_chunk(i)
                .map_err(|e| io::Error::other(format!("Decompression error: {e}")))?;
            foutput.write_all(&data)?;
        }
        foutput.flush()
    })();

    if let Err(err) = write_result {
        let _ = std::fs::remove_file(&temp_path);
        return Err(err);
    }
    replace_output(&temp_path, output)?;

    let nbytes = schunk.nbytes;
    let cbytes = schunk.cbytes;
    let elapsed = start.elapsed().as_secs_f64();

    let mb = 1024.0 * 1024.0;
    println!(
        "Decompression ratio: {:.1} MB -> {:.1} MB ({:.1}x)",
        cbytes as f64 / mb,
        nbytes as f64 / mb,
        ratio(cbytes, nbytes)
    );
    println!(
        "Decompression time: {:.3} s, {:.1} MB/s",
        elapsed,
        throughput_mib(nbytes, elapsed)
    );

    Ok(())
}

/// Writes bytes to `output` through a sibling temporary file, then replaces the destination.
fn atomic_write_all(output: &Path, bytes: &[u8]) -> io::Result<()> {
    let temp_path = temp_output_path(output);
    let write_result = (|| {
        let temp_file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp_path)?;
        let mut writer = BufWriter::new(temp_file);
        writer.write_all(bytes)?;
        writer.flush()
    })();

    if let Err(err) = write_result {
        let _ = std::fs::remove_file(&temp_path);
        return Err(err);
    }
    replace_output(&temp_path, output)
}

fn temp_output_path(output: &Path) -> PathBuf {
    let counter = TEMP_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let file_name = output
        .file_name()
        .map(|name| name.to_string_lossy())
        .unwrap_or_else(|| ".blosc2-output".into());
    let temp_name = format!(".{file_name}.tmp.{}.{}", std::process::id(), counter);
    output.with_file_name(temp_name)
}

fn replace_output(temp_path: &Path, output: &Path) -> io::Result<()> {
    match std::fs::rename(temp_path, output) {
        Ok(()) => Ok(()),
        Err(first_err) if output.exists() => {
            std::fs::remove_file(output).map_err(|remove_err| {
                let _ = std::fs::remove_file(temp_path);
                io::Error::new(
                    remove_err.kind(),
                    format!(
                        "failed to remove existing output after rename failed ({first_err}); {remove_err}"
                    ),
                )
            })?;
            std::fs::rename(temp_path, output).map_err(|rename_err| {
                let _ = std::fs::remove_file(temp_path);
                rename_err
            })
        }
        Err(err) => {
            let _ = std::fs::remove_file(temp_path);
            Err(err)
        }
    }
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
