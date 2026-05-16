//! Blosc2 protocol constants: format versions, codec/filter IDs, header flags,
//! chunk header field offsets, and buffer-size limits.
//!
//! These values define the on-disk and in-memory layout of Blosc2 chunks and
//! frames and must remain byte-compatible with the C reference implementation.

/// Blosc1 pre-2.x chunk format version (legacy 16-byte header).
pub const BLOSC1_VERSION_FORMAT: u8 = 2;
/// First stable Blosc2 chunk format version.
pub const BLOSC2_VERSION_FORMAT_STABLE: u8 = 5;
/// Chunk format version that adds variable-length blocks support.
pub const BLOSC2_VERSION_FORMAT_VL_BLOCKS: u8 = 6;
/// Highest chunk format version supported by this library.
pub const BLOSC2_VERSION_FORMAT: u8 = BLOSC2_VERSION_FORMAT_VL_BLOCKS;

/// Frame format version introduced for the 2.0.0-rc1 series.
pub const BLOSC2_VERSION_FRAME_FORMAT_RC1: u8 = 2;
/// Highest contiguous-frame format version supported by this library.
pub const BLOSC2_VERSION_FRAME_FORMAT: u8 = 3;
/// Frame flag (byte 25, bit 6): frame contains variable-size chunks.
pub const FRAME_VARIABLE_CHUNKS: u8 = 1 << 6;
/// Frame flag (byte 25, bit 7): frame uses variable-length blocks.
pub const FRAME_VL_BLOCKS: u8 = 1 << 7;

/// Minimum header length (the legacy Blosc1 16-byte header).
pub const BLOSC_MIN_HEADER_LENGTH: usize = 16;
/// Extended Blosc2 header length (the full 32-byte header).
pub const BLOSC_EXTENDED_HEADER_LENGTH: usize = 32;
/// Maximum compression overhead in bytes (currently equals the extended header length).
pub const BLOSC2_MAX_OVERHEAD: usize = BLOSC_EXTENDED_HEADER_LENGTH;

/// Maximum source buffer size that can be compressed.
pub const BLOSC2_MAX_BUFFERSIZE: i32 = i32::MAX - BLOSC2_MAX_OVERHEAD as i32;
/// Minimum buffer size that can be compressed.
pub const BLOSC_MIN_BUFFERSIZE: usize = 32;
/// Maximum atomic type size; beyond this the source is treated as a stream of bytes.
pub const BLOSC_MAX_TYPESIZE: usize = 255;
/// Maximum size in bytes for a Zstd compression dictionary.
pub const BLOSC2_MAXDICTSIZE: usize = 32 * 1024;
/// Minimum dictionary size considered useful; below this, dictionary compression is bypassed.
pub const BLOSC2_MINUSEFULDICT: usize = 256;

/// Maximum number of filters in the filter pipeline.
pub const BLOSC2_MAX_FILTERS: usize = 6;
/// Filter ID: no filter (and Blosc1 no-shuffle).
pub const BLOSC_NOFILTER: u8 = 0;
/// Filter ID: byte-wise shuffle. `filters_meta` is the number of bytestreams (0 = typesize).
pub const BLOSC_SHUFFLE: u8 = 1;
/// Filter ID: bit-wise shuffle. `filters_meta` is unused.
pub const BLOSC_BITSHUFFLE: u8 = 2;
/// Filter ID: delta filter (bitwise XOR relative to a reference). `filters_meta` is unused.
pub const BLOSC_DELTA: u8 = 3;
/// Filter ID: truncate mantissa precision. Positive `filters_meta` keeps bits; negative zeros bits.
pub const BLOSC_TRUNC_PREC: u8 = 4;
/// First filter ID reserved for user-defined filters.
pub const BLOSC2_USER_DEFINED_FILTERS_START: u8 = 32;

/// Codec ID: BloscLZ.
pub const BLOSC_BLOSCLZ: u8 = 0;
/// Codec ID: LZ4.
pub const BLOSC_LZ4: u8 = 1;
/// Codec ID: LZ4HC (high-compression LZ4).
pub const BLOSC_LZ4HC: u8 = 2;
/// Codec ID: Zlib.
pub const BLOSC_ZLIB: u8 = 4;
/// Codec ID: Zstd.
pub const BLOSC_ZSTD: u8 = 5;
/// First codec ID reserved for user-defined codecs.
pub const BLOSC2_USER_DEFINED_CODECS_START: u8 = 32;

/// Codec format code (bits 5-7 of the header flags byte) for BloscLZ.
pub const BLOSC_BLOSCLZ_FORMAT: u8 = 0;
/// Codec format code for LZ4.
pub const BLOSC_LZ4_FORMAT: u8 = 1;
/// Codec format code for LZ4HC (shares format with LZ4).
pub const BLOSC_LZ4HC_FORMAT: u8 = 1;
/// Codec format code for Zlib.
pub const BLOSC_ZLIB_FORMAT: u8 = 3;
/// Codec format code for Zstd.
pub const BLOSC_ZSTD_FORMAT: u8 = 4;
/// Codec format code signalling that the actual codec is stored in the user-defined codec slot.
pub const BLOSC_UDCODEC_FORMAT: u8 = 7;

/// On-disk format version for BloscLZ-compressed streams.
pub const BLOSC_BLOSCLZ_VERSION_FORMAT: u8 = 1;
/// On-disk format version for LZ4/LZ4HC-compressed streams.
pub const BLOSC_LZ4_VERSION_FORMAT: u8 = 1;
/// On-disk format version for Zlib-compressed streams.
pub const BLOSC_ZLIB_VERSION_FORMAT: u8 = 1;
/// On-disk format version for Zstd-compressed streams.
pub const BLOSC_ZSTD_VERSION_FORMAT: u8 = 1;

/// Split mode: always split blocks into per-typesize streams (experimental).
pub const BLOSC_ALWAYS_SPLIT: i32 = 1;
/// Split mode: never split blocks (experimental).
pub const BLOSC_NEVER_SPLIT: i32 = 2;
/// Split mode: split heuristically based on codec, typesize, and blocksize.
pub const BLOSC_AUTO_SPLIT: i32 = 3;
/// Split mode: choose the variant that gives the best forward compatibility (default).
pub const BLOSC_FORWARD_COMPAT_SPLIT: i32 = 4;

/// Header flag (byte 2, bit 0): byte-wise shuffle was applied.
pub const BLOSC_DOSHUFFLE: u8 = 0x01;
/// Header flag (byte 2, bit 1): data was memcpy'd (not compressed).
pub const BLOSC_MEMCPYED: u8 = 0x02;
/// Header flag (byte 2, bit 2): bit-wise shuffle was applied.
/// When bits 0 and 2 are both set the chunk uses the extended 32-byte header.
pub const BLOSC_DOBITSHUFFLE: u8 = 0x04;
/// Header flag (byte 2, bit 3): delta coding was applied.
pub const BLOSC_DODELTA: u8 = 0x08;
/// Header flag (byte 2, bit 4): blocks are not split into multiple compressed streams.
pub const BLOSC_DONT_SPLIT: u8 = 0x10;

/// blosc2_flags (byte 31, bit 0): codec uses a dictionary.
pub const BLOSC2_USEDICT: u8 = 0x01;
/// blosc2_flags (byte 31, bit 1): data was produced on a big-endian host.
pub const BLOSC2_BIGENDIAN: u8 = 0x02;
/// blosc2_flags (byte 31, bit 2): codec was instrumented (development use).
pub const BLOSC2_INSTR_CODEC: u8 = 0x04;
/// blosc2_flags (byte 31, bit 3): this chunk is a lazy chunk (carries only metadata).
pub const BLOSC2_LAZY_CHUNK: u8 = 0x08;

/// Special-value type (blosc2_flags bits 4-6): no special value, this is a normal chunk.
pub const BLOSC2_NO_SPECIAL: u8 = 0x0;
/// Special-value type: chunk represents a run of zeros.
pub const BLOSC2_SPECIAL_ZERO: u8 = 0x1;
/// Special-value type: chunk represents a run of NaN floats (f32 or f64 per typesize).
pub const BLOSC2_SPECIAL_NAN: u8 = 0x2;
/// Special-value type: chunk represents a run of an arbitrary value stored after the header.
pub const BLOSC2_SPECIAL_VALUE: u8 = 0x3;
/// Special-value type: chunk represents uninitialized values.
pub const BLOSC2_SPECIAL_UNINIT: u8 = 0x4;
/// Mask used to extract the 3-bit special-value type from blosc2_flags.
pub const BLOSC2_SPECIAL_MASK: u8 = 0x7;

/// blosc2_flags2 (byte 30, bit 0): the chunk uses variable-length blocks.
pub const BLOSC2_VL_BLOCKS: u8 = 0x01;

/// Chunk header offset: format version of this chunk (uint8).
pub const BLOSC2_CHUNK_VERSION: usize = 0x0;
/// Chunk header offset: format version of the internal codec (uint8, normally 1).
pub const BLOSC2_CHUNK_VERSIONLZ: usize = 0x1;
/// Chunk header offset: flags and embedded codec format code (uint8).
pub const BLOSC2_CHUNK_FLAGS: usize = 0x2;
/// Chunk header offset: size in bytes of the atomic type (uint8).
pub const BLOSC2_CHUNK_TYPESIZE: usize = 0x3;
/// Chunk header offset: uncompressed buffer size, not counting the header (int32).
pub const BLOSC2_CHUNK_NBYTES: usize = 0x4;
/// Chunk header offset: size of internal blocks (int32).
pub const BLOSC2_CHUNK_BLOCKSIZE: usize = 0x8;
/// Chunk header offset: compressed buffer size including the header (int32).
pub const BLOSC2_CHUNK_CBYTES: usize = 0xC;
/// Chunk header offset: 6-byte filter pipeline IDs (one byte per filter).
pub const BLOSC2_CHUNK_FILTER_CODES: usize = 0x10;
/// Chunk header offset: user-defined codec identifier (uint8).
pub const BLOSC2_CHUNK_UDCOMPCODE: usize = 0x16;
/// Chunk header offset: compression-codec metadata byte (uint8).
pub const BLOSC2_CHUNK_COMPCODE_META: usize = 0x17;
/// Chunk header offset: 6-byte per-filter metadata (one byte per filter).
pub const BLOSC2_CHUNK_FILTER_META: usize = 0x18;
/// Chunk header offset: secondary Blosc2 flags byte.
pub const BLOSC2_CHUNK_BLOSC2_FLAGS2: usize = 0x1E;
/// Chunk header offset: primary Blosc2 flags byte.
pub const BLOSC2_CHUNK_BLOSC2_FLAGS: usize = 0x1F;

/// Maximum number of dimensions supported by the B2ND N-dimensional array layer.
pub const BLOSC2_MAX_DIM: usize = 8;

/// Typical L1 data cache size, used as a hint when tuning block sizes.
pub const L1_CACHE: usize = 32 * 1024;
/// Typical L2 cache size, used as a hint when tuning block sizes.
pub const L2_CACHE: usize = 256 * 1024;

/// Default chunk size for file I/O. A 4 MiB default keeps CLI frame
/// overhead modest without turning large files into single huge chunks.
pub const DEFAULT_CHUNKSIZE: usize = 4 * 1024 * 1024;

/// Map a codec ID to the 3-bit format code stored in header flags bits 5-7.
///
/// Unknown codecs map to [`BLOSC_UDCODEC_FORMAT`], which signals that the real
/// codec identifier lives in the [`BLOSC2_CHUNK_UDCOMPCODE`] slot.
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

/// Map a header format code back to a codec ID.
///
/// Note that [`BLOSC_LZ4_FORMAT`] is ambiguous between LZ4 and LZ4HC; callers
/// that need to distinguish them must use additional context.
pub fn compformat_to_compcode(compformat: u8) -> u8 {
    match compformat {
        BLOSC_BLOSCLZ_FORMAT => BLOSC_BLOSCLZ,
        BLOSC_LZ4_FORMAT => BLOSC_LZ4, // Could also be LZ4HC; need context
        BLOSC_ZLIB_FORMAT => BLOSC_ZLIB,
        BLOSC_ZSTD_FORMAT => BLOSC_ZSTD,
        _ => compformat,
    }
}

/// Return the on-disk format version number for a given codec ID.
pub fn compcode_to_version(compcode: u8) -> u8 {
    match compcode {
        BLOSC_BLOSCLZ => BLOSC_BLOSCLZ_VERSION_FORMAT,
        BLOSC_LZ4 | BLOSC_LZ4HC => BLOSC_LZ4_VERSION_FORMAT,
        BLOSC_ZLIB => BLOSC_ZLIB_VERSION_FORMAT,
        BLOSC_ZSTD => BLOSC_ZSTD_VERSION_FORMAT,
        _ => 1,
    }
}
