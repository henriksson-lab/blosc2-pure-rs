#[cfg(feature = "_ffi")]
pub mod ffi;
pub mod constants;
pub mod header;
pub mod filters;
pub mod codecs;
pub mod shuffle_sse2;
pub mod compress;
pub mod schunk;

/// RAII guard that initializes the C-Blosc2 library via FFI.
/// Only available with the `ffi` feature.
#[cfg(feature = "_ffi")]
pub struct Blosc2 {
    _private: (),
}

#[cfg(feature = "_ffi")]
impl Blosc2 {
    pub fn new() -> Self {
        unsafe {
            ffi::blosc2_init();
        }
        Blosc2 { _private: () }
    }
}

#[cfg(feature = "_ffi")]
impl Drop for Blosc2 {
    fn drop(&mut self) {
        unsafe {
            ffi::blosc2_destroy();
        }
    }
}

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
    pub fn from_str(s: &str) -> Option<Self> {
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
    pub fn from_str(s: &str) -> Option<Self> {
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

/// Default chunk size used for file compression (1 MB).
pub const DEFAULT_CHUNKSIZE: usize = 1_000_000;

