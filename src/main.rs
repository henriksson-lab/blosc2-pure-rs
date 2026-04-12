use blosc2_pure_rs::compress::{CParams, DParams};
use blosc2_pure_rs::constants::*;
use blosc2_pure_rs::schunk::Schunk;
use blosc2_pure_rs::{Codec, Filter};
use clap::{Parser, Subcommand};
use std::fs::File;
use std::io::{self, Read, Write};
use std::path::PathBuf;
use std::time::Instant;

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
        #[arg(short = 'l', long, default_value_t = 9)]
        clevel: u8,
        /// Type size in bytes
        #[arg(short, long, default_value_t = 1)]
        typesize: i32,
        /// Number of threads
        #[arg(short, long, default_value_t = 4)]
        nthreads: i16,
        /// Filter to apply (nofilter, shuffle, bitshuffle, delta, truncprec)
        #[arg(short, long, default_value = "shuffle")]
        filter: String,
    },
    /// Decompress a Blosc2 frame file
    Decompress {
        /// Input file path (.b2frame)
        input: PathBuf,
        /// Output file path
        output: PathBuf,
        /// Number of threads
        #[arg(short, long, default_value_t = 4)]
        nthreads: i16,
    },
}

fn compress_file(
    input: &PathBuf,
    output: &PathBuf,
    codec: Codec,
    clevel: u8,
    typesize: i32,
    nthreads: i16,
    filter: Filter,
) -> io::Result<()> {
    let cparams = CParams {
        compcode: codec as u8,
        clevel,
        typesize,
        splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
        filters: [0, 0, 0, 0, 0, filter as u8],
        nthreads,
        ..Default::default()
    };
    let dparams = DParams { nthreads };

    let mut schunk = Schunk::new(cparams, dparams);

    let start = Instant::now();

    let mut finput = File::open(input)?;
    let mut buf = vec![0u8; DEFAULT_CHUNKSIZE];

    loop {
        let bytes_read = finput.read(&mut buf)?;
        if bytes_read == 0 {
            break;
        }
        schunk.append_buffer(&buf[..bytes_read]).map_err(|e| {
            io::Error::new(io::ErrorKind::Other, format!("Error compressing: {e}"))
        })?;
    }

    let _ = std::fs::remove_file(output);
    schunk.to_file(
        output.to_str().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "Invalid output path")
        })?,
    )?;

    let nbytes = schunk.nbytes;
    let cbytes = std::fs::metadata(output)?.len() as i64;
    let elapsed = start.elapsed().as_secs_f64();

    let mb = 1024.0 * 1024.0;
    println!(
        "Compression ratio: {:.1} MB -> {:.1} MB ({:.1}x)",
        nbytes as f64 / mb,
        cbytes as f64 / mb,
        nbytes as f64 / cbytes as f64
    );
    println!(
        "Compression time: {:.3} s, {:.1} MB/s",
        elapsed,
        nbytes as f64 / (elapsed * mb)
    );

    Ok(())
}

fn decompress_file(input: &PathBuf, output: &PathBuf, _nthreads: i16) -> io::Result<()> {
    let input_str = input.to_str().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "Invalid input path")
    })?;

    let schunk = Schunk::open(input_str).map_err(|e| {
        io::Error::new(io::ErrorKind::Other, format!("Failed to open frame: {e}"))
    })?;

    if schunk.nchunks() == 0 {
        let _ = File::create(output)?;
        println!("Decompression ratio: 0.0 MB -> 0.0 MB (0.0x)");
        println!("Decompression time: 0.000 s, 0.0 MB/s");
        return Ok(());
    }

    let start = Instant::now();
    let mut foutput = File::create(output)?;

    for i in 0..schunk.nchunks() {
        let data = schunk.decompress_chunk(i).map_err(|e| {
            io::Error::new(io::ErrorKind::Other, format!("Decompression error: {e}"))
        })?;
        foutput.write_all(&data)?;
    }

    let nbytes = schunk.nbytes;
    let cbytes = schunk.cbytes;
    let elapsed = start.elapsed().as_secs_f64();

    let mb = 1024.0 * 1024.0;
    println!(
        "Decompression ratio: {:.1} MB -> {:.1} MB ({:.1}x)",
        cbytes as f64 / mb,
        nbytes as f64 / mb,
        cbytes as f64 / nbytes as f64
    );
    println!(
        "Decompression time: {:.3} s, {:.1} MB/s",
        elapsed,
        nbytes as f64 / (elapsed * mb)
    );

    Ok(())
}

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
            nthreads,
            filter,
        } => {
            let codec = Codec::from_str(codec).unwrap_or_else(|| {
                eprintln!(
                    "Unknown codec '{}'. Available: blosclz, lz4, lz4hc, zlib, zstd",
                    codec
                );
                std::process::exit(1);
            });
            let filter = Filter::from_str(filter).unwrap_or_else(|| {
                eprintln!(
                    "Unknown filter '{}'. Available: nofilter, shuffle, bitshuffle, delta, truncprec",
                    filter
                );
                std::process::exit(1);
            });
            compress_file(input, output, codec, *clevel, *typesize, *nthreads, filter)
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
