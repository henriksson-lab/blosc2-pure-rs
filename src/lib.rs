//! High-performance compressor for binary data (numerical arrays, tensors, structured formats).
//!
//! A pure-Rust implementation of the Blosc2 format. Supports the BloscLZ, LZ4, LZ4HC, Zlib,
//! and Zstd codecs, combined with shuffle, bitshuffle, delta, and truncate-precision filters
//! for improved compression of typed data.
//!
//! The top-level modules expose the building blocks: chunk [`header`] parsing, [`filters`],
//! [`codecs`], the core [`compress`] engine, [`schunk`] super-chunks, and the [`b2nd`]
//! N-dimensional array layer. The [`Codec`] and [`Filter`] enums and [`DEFAULT_CHUNKSIZE`]
//! constant are re-exported here for convenience.

pub mod b2nd;
pub mod codecs;
pub mod compress;
pub mod constants;
pub mod filters;
pub mod header;
pub mod schunk;

/// Codec identifiers matching the C library constants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Codec {
    BloscLz = 0,
    Lz4 = 1,
    Lz4hc = 2,
    Zlib = 4,
    Zstd = 5,
}

impl Codec {
    /// Parses a codec name (case-insensitive) into a [`Codec`] variant.
    ///
    /// Accepts `blosclz`, `lz4`, `lz4hc`, `zlib`, or `zstd`; returns `None` otherwise.
    pub fn parse_name(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "blosclz" => Some(Codec::BloscLz),
            "lz4" => Some(Codec::Lz4),
            "lz4hc" => Some(Codec::Lz4hc),
            "zlib" => Some(Codec::Zlib),
            "zstd" => Some(Codec::Zstd),
            _ => None,
        }
    }
}

impl std::str::FromStr for Codec {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse_name(s).ok_or(())
    }
}

/// Filter identifiers matching the C library constants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Filter {
    NoFilter = 0,
    Shuffle = 1,
    BitShuffle = 2,
    Delta = 3,
    TruncPrec = 4,
}

impl Filter {
    /// Parses a filter name (case-insensitive) into a [`Filter`] variant.
    ///
    /// Accepts `nofilter`/`none`, `shuffle`, `bitshuffle`, `delta`, or `truncprec`/`trunc_prec`;
    /// returns `None` otherwise.
    pub fn parse_name(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "nofilter" | "none" => Some(Filter::NoFilter),
            "shuffle" => Some(Filter::Shuffle),
            "bitshuffle" => Some(Filter::BitShuffle),
            "delta" => Some(Filter::Delta),
            "truncprec" | "trunc_prec" => Some(Filter::TruncPrec),
            _ => None,
        }
    }
}

impl std::str::FromStr for Filter {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse_name(s).ok_or(())
    }
}

/// Default chunk size used for file compression (4 MiB).
pub const DEFAULT_CHUNKSIZE: usize = 4 * 1024 * 1024;
