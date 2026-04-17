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
