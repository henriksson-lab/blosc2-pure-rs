// Blosc2 format versions
pub const BLOSC1_VERSION_FORMAT: u8 = 2;
pub const BLOSC2_VERSION_FORMAT_STABLE: u8 = 5;
pub const BLOSC2_VERSION_FORMAT_VL_BLOCKS: u8 = 6;
pub const BLOSC2_VERSION_FORMAT: u8 = BLOSC2_VERSION_FORMAT_VL_BLOCKS;

// Frame format versions
pub const BLOSC2_VERSION_FRAME_FORMAT_RC1: u8 = 2;
pub const BLOSC2_VERSION_FRAME_FORMAT: u8 = 3;
pub const FRAME_VARIABLE_CHUNKS: u8 = 1 << 6;
pub const FRAME_VL_BLOCKS: u8 = 1 << 7;

// Header sizes
pub const BLOSC_MIN_HEADER_LENGTH: usize = 16;
pub const BLOSC_EXTENDED_HEADER_LENGTH: usize = 32;
pub const BLOSC2_MAX_OVERHEAD: usize = BLOSC_EXTENDED_HEADER_LENGTH;

// Buffer limits
pub const BLOSC2_MAX_BUFFERSIZE: i32 = i32::MAX - BLOSC2_MAX_OVERHEAD as i32;
pub const BLOSC_MIN_BUFFERSIZE: usize = 32;
pub const BLOSC_MAX_TYPESIZE: usize = 255;
pub const BLOSC2_MAXDICTSIZE: usize = 32 * 1024;
pub const BLOSC2_MINUSEFULDICT: usize = 256;

// Filters
pub const BLOSC2_MAX_FILTERS: usize = 6;
pub const BLOSC_NOFILTER: u8 = 0;
pub const BLOSC_SHUFFLE: u8 = 1;
pub const BLOSC_BITSHUFFLE: u8 = 2;
pub const BLOSC_DELTA: u8 = 3;
pub const BLOSC_TRUNC_PREC: u8 = 4;
pub const BLOSC2_USER_DEFINED_FILTERS_START: u8 = 32;

// Codecs
pub const BLOSC_BLOSCLZ: u8 = 0;
pub const BLOSC_LZ4: u8 = 1;
pub const BLOSC_LZ4HC: u8 = 2;
pub const BLOSC_ZLIB: u8 = 4;
pub const BLOSC_ZSTD: u8 = 5;
pub const BLOSC2_USER_DEFINED_CODECS_START: u8 = 32;

// Codec format codes (for header flags bits 5-7)
pub const BLOSC_BLOSCLZ_FORMAT: u8 = 0;
pub const BLOSC_LZ4_FORMAT: u8 = 1;
pub const BLOSC_LZ4HC_FORMAT: u8 = 1; // shares with LZ4
pub const BLOSC_ZLIB_FORMAT: u8 = 3;
pub const BLOSC_ZSTD_FORMAT: u8 = 4;
pub const BLOSC_UDCODEC_FORMAT: u8 = 7;

// Codec version formats
pub const BLOSC_BLOSCLZ_VERSION_FORMAT: u8 = 1;
pub const BLOSC_LZ4_VERSION_FORMAT: u8 = 1;
pub const BLOSC_ZLIB_VERSION_FORMAT: u8 = 1;
pub const BLOSC_ZSTD_VERSION_FORMAT: u8 = 1;

// Split modes
pub const BLOSC_ALWAYS_SPLIT: i32 = 1;
pub const BLOSC_NEVER_SPLIT: i32 = 2;
pub const BLOSC_AUTO_SPLIT: i32 = 3;
pub const BLOSC_FORWARD_COMPAT_SPLIT: i32 = 4;

// Header flags (byte 2) bit positions
pub const BLOSC_DOSHUFFLE: u8 = 0x01;
pub const BLOSC_MEMCPYED: u8 = 0x02;
pub const BLOSC_DOBITSHUFFLE: u8 = 0x04;
pub const BLOSC_DODELTA: u8 = 0x08;
pub const BLOSC_DONT_SPLIT: u8 = 0x10;

// blosc2_flags (byte 31) bit positions
pub const BLOSC2_USEDICT: u8 = 0x01;
pub const BLOSC2_BIGENDIAN: u8 = 0x02;
pub const BLOSC2_INSTR_CODEC: u8 = 0x04;
pub const BLOSC2_LAZY_CHUNK: u8 = 0x08;

// Special value types (bits 4-6 of blosc2_flags)
pub const BLOSC2_NO_SPECIAL: u8 = 0x0;
pub const BLOSC2_SPECIAL_ZERO: u8 = 0x1;
pub const BLOSC2_SPECIAL_NAN: u8 = 0x2;
pub const BLOSC2_SPECIAL_VALUE: u8 = 0x3;
pub const BLOSC2_SPECIAL_UNINIT: u8 = 0x4;
pub const BLOSC2_SPECIAL_MASK: u8 = 0x7;

// blosc2_flags2 (byte 30)
pub const BLOSC2_VL_BLOCKS: u8 = 0x01;

// Chunk header field offsets
pub const BLOSC2_CHUNK_VERSION: usize = 0x0;
pub const BLOSC2_CHUNK_VERSIONLZ: usize = 0x1;
pub const BLOSC2_CHUNK_FLAGS: usize = 0x2;
pub const BLOSC2_CHUNK_TYPESIZE: usize = 0x3;
pub const BLOSC2_CHUNK_NBYTES: usize = 0x4;
pub const BLOSC2_CHUNK_BLOCKSIZE: usize = 0x8;
pub const BLOSC2_CHUNK_CBYTES: usize = 0xC;
pub const BLOSC2_CHUNK_FILTER_CODES: usize = 0x10;
pub const BLOSC2_CHUNK_UDCOMPCODE: usize = 0x16;
pub const BLOSC2_CHUNK_COMPCODE_META: usize = 0x17;
pub const BLOSC2_CHUNK_FILTER_META: usize = 0x18;
pub const BLOSC2_CHUNK_BLOSC2_FLAGS2: usize = 0x1E;
pub const BLOSC2_CHUNK_BLOSC2_FLAGS: usize = 0x1F;

// N-dimensional array limits
pub const BLOSC2_MAX_DIM: usize = 8;

// Cache sizes for tuning
pub const L1_CACHE: usize = 32 * 1024;
pub const L2_CACHE: usize = 256 * 1024;

// Default chunk size for file I/O. A 4 MiB default keeps CLI frame
// overhead modest without turning large files into single huge chunks.
pub const DEFAULT_CHUNKSIZE: usize = 4 * 1024 * 1024;

/// Map compcode to the format code stored in header flags bits 5-7.
pub fn compcode_to_compformat(compcode: u8) -> u8 {
    match compcode {
        BLOSC_BLOSCLZ => BLOSC_BLOSCLZ_FORMAT,
        BLOSC_LZ4 => BLOSC_LZ4_FORMAT,
        BLOSC_LZ4HC => BLOSC_LZ4HC_FORMAT,
        BLOSC_ZLIB => BLOSC_ZLIB_FORMAT,
        BLOSC_ZSTD => BLOSC_ZSTD_FORMAT,
        _ => BLOSC_UDCODEC_FORMAT,
    }
}

/// Map format code from header back to compcode.
pub fn compformat_to_compcode(compformat: u8) -> u8 {
    match compformat {
        BLOSC_BLOSCLZ_FORMAT => BLOSC_BLOSCLZ,
        BLOSC_LZ4_FORMAT => BLOSC_LZ4, // Could also be LZ4HC; need context
        BLOSC_ZLIB_FORMAT => BLOSC_ZLIB,
        BLOSC_ZSTD_FORMAT => BLOSC_ZSTD,
        _ => compformat,
    }
}

/// Map compcode to version format number.
pub fn compcode_to_version(compcode: u8) -> u8 {
    match compcode {
        BLOSC_BLOSCLZ => BLOSC_BLOSCLZ_VERSION_FORMAT,
        BLOSC_LZ4 | BLOSC_LZ4HC => BLOSC_LZ4_VERSION_FORMAT,
        BLOSC_ZLIB => BLOSC_ZLIB_VERSION_FORMAT,
        BLOSC_ZSTD => BLOSC_ZSTD_VERSION_FORMAT,
        _ => 1,
    }
}
