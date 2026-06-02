//! Blosc2 protocol constants: format versions, codec/filter IDs, header flags,
//! chunk header field offsets, and buffer-size limits.
//!
//! These values define the on-disk and in-memory layout of Blosc2 chunks and
//! frames and must remain byte-compatible with the C reference implementation.

/// Major version of the vendored C-Blosc2 public API snapshot.
pub const BLOSC2_VERSION_MAJOR: u8 = 3;
/// Minor version of the vendored C-Blosc2 public API snapshot.
pub const BLOSC2_VERSION_MINOR: u8 = 0;
/// Patch/release version of the vendored C-Blosc2 public API snapshot.
pub const BLOSC2_VERSION_RELEASE: u8 = 0;
/// String version of the vendored C-Blosc2 public API snapshot.
pub const BLOSC2_VERSION_STRING: &str = "3.0.0.dev";
/// Date marker from the vendored C-Blosc2 public API snapshot.
pub const BLOSC2_VERSION_DATE: &str = "$Date:: 2026-03-27 #$";
/// Blosc1-compatible alias for the major public API version.
pub const BLOSC_VERSION_MAJOR: u8 = BLOSC2_VERSION_MAJOR;
/// Blosc1-compatible alias for the minor public API version.
pub const BLOSC_VERSION_MINOR: u8 = BLOSC2_VERSION_MINOR;
/// Blosc1-compatible alias for the patch/release public API version.
pub const BLOSC_VERSION_RELEASE: u8 = BLOSC2_VERSION_RELEASE;
/// Blosc1-compatible alias for the string public API version.
pub const BLOSC_VERSION_STRING: &str = BLOSC2_VERSION_STRING;
/// Blosc1-compatible alias for the public API date marker.
pub const BLOSC_VERSION_DATE: &str = BLOSC2_VERSION_DATE;

/// Blosc1 pre-2.x chunk format version (legacy 16-byte header).
pub const BLOSC1_VERSION_FORMAT_PRE1: u8 = 1;
/// Blosc1 stable chunk format version (legacy 16-byte header).
pub const BLOSC1_VERSION_FORMAT: u8 = 2;
/// Early Blosc2 alpha chunk format version with partially undefined filter slots.
pub const BLOSC2_VERSION_FORMAT_ALPHA: u8 = 3;
/// Blosc2 beta.1 chunk format version.
pub const BLOSC2_VERSION_FORMAT_BETA1: u8 = 4;
/// First stable Blosc2 chunk format version.
pub const BLOSC2_VERSION_FORMAT_STABLE: u8 = 5;
/// Chunk format version that adds variable-length blocks support.
pub const BLOSC2_VERSION_FORMAT_VL_BLOCKS: u8 = 6;
/// Highest chunk format version supported by this library.
pub const BLOSC2_VERSION_FORMAT: u8 = BLOSC2_VERSION_FORMAT_VL_BLOCKS;

/// Frame format version introduced for the 2.0.0-beta2 series.
pub const BLOSC2_VERSION_FRAME_FORMAT_BETA2: u8 = 1;
/// Frame format version introduced for the 2.0.0-rc1 series.
pub const BLOSC2_VERSION_FRAME_FORMAT_RC1: u8 = 2;
/// Frame format version that adds variable-length blocks support.
pub const BLOSC2_VERSION_FRAME_FORMAT_VL_BLOCKS: u8 = 3;
/// Highest contiguous-frame format version supported by this library.
pub const BLOSC2_VERSION_FRAME_FORMAT: u8 = BLOSC2_VERSION_FRAME_FORMAT_VL_BLOCKS;
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
/// Blosc1-compatible alias for the maximum compression overhead.
pub const BLOSC_MAX_OVERHEAD: usize = BLOSC2_MAX_OVERHEAD;

/// Maximum source buffer size that can be compressed.
pub const BLOSC2_MAX_BUFFERSIZE: i32 = i32::MAX - BLOSC2_MAX_OVERHEAD as i32;
/// Blosc1-compatible alias for the maximum source buffer size.
pub const BLOSC_MAX_BUFFERSIZE: i32 = BLOSC2_MAX_BUFFERSIZE;
/// Maximum block size accepted by C-Blosc2.
pub const BLOSC2_MAXBLOCKSIZE: usize = 536_866_816;
/// Minimum buffer size that can be compressed.
pub const BLOSC_MIN_BUFFERSIZE: usize = 32;
/// Maximum atomic type size; beyond this the source is treated as a stream of bytes.
pub const BLOSC_MAX_TYPESIZE: usize = 255;
/// Maximum caller-supplied type size accepted by C-Blosc2.
pub const BLOSC2_MAXTYPESIZE: usize = BLOSC2_MAXBLOCKSIZE;
/// Maximum size in bytes for a Zstd compression dictionary.
pub const BLOSC2_MAXDICTSIZE: usize = 32 * 1024;
/// Minimum dictionary size considered useful; below this, dictionary compression is bypassed.
pub const BLOSC2_MINUSEFULDICT: usize = 256;
/// First codec code outside the Blosc1 built-in compressor set.
pub const BLOSC_LAST_CODEC: u8 = 6;

/// First tuner ID reserved for Blosc-defined tuners.
pub const BLOSC2_DEFINED_TUNER_START: u8 = 0;
/// Last tuner ID reserved for Blosc-defined tuners.
pub const BLOSC2_DEFINED_TUNER_STOP: u8 = 31;
/// First tuner ID reserved for globally registered Blosc2 tuners.
pub const BLOSC2_GLOBAL_REGISTERED_TUNER_START: u8 = 32;
/// Last tuner ID reserved for globally registered Blosc2 tuners.
pub const BLOSC2_GLOBAL_REGISTERED_TUNER_STOP: u8 = 159;
/// Number of globally registered Blosc2 tuners in the vendored C header.
pub const BLOSC2_GLOBAL_REGISTERED_TUNERS: u8 = 0;
/// First tuner ID reserved for user-registered tuners.
pub const BLOSC2_USER_REGISTERED_TUNER_START: u8 = 160;
/// Last tuner ID reserved for user-registered tuners.
pub const BLOSC2_USER_REGISTERED_TUNER_STOP: u8 = u8::MAX;
/// Built-in tuner ID: standard tuner.
pub const BLOSC_STUNE: u8 = 0;
/// Global plugin tuner ID: BTune.
pub const BLOSC_BTUNE: u8 = 32;
/// First tuner code outside the Blosc built-in tuner set.
pub const BLOSC_LAST_TUNER: u8 = 1;
/// Last globally registered tuner ID in the vendored C header.
pub const BLOSC_LAST_REGISTERED_TUNE: u8 = 31;

/// C API return code: success.
pub const BLOSC2_ERROR_SUCCESS: i32 = 0;
/// C API return code: generic failure.
pub const BLOSC2_ERROR_FAILURE: i32 = -1;
/// C API return code: bad stream.
pub const BLOSC2_ERROR_STREAM: i32 = -2;
/// C API return code: invalid data.
pub const BLOSC2_ERROR_DATA: i32 = -3;
/// C API return code: allocation failure.
pub const BLOSC2_ERROR_MEMORY_ALLOC: i32 = -4;
/// C API return code: not enough input bytes.
pub const BLOSC2_ERROR_READ_BUFFER: i32 = -5;
/// C API return code: not enough output space.
pub const BLOSC2_ERROR_WRITE_BUFFER: i32 = -6;
/// C API return code: codec not supported.
pub const BLOSC2_ERROR_CODEC_SUPPORT: i32 = -7;
/// C API return code: invalid codec parameter.
pub const BLOSC2_ERROR_CODEC_PARAM: i32 = -8;
/// C API return code: codec dictionary error.
pub const BLOSC2_ERROR_CODEC_DICT: i32 = -9;
/// C API return code: format version not supported.
pub const BLOSC2_ERROR_VERSION_SUPPORT: i32 = -10;
/// C API return code: invalid chunk/header metadata.
pub const BLOSC2_ERROR_INVALID_HEADER: i32 = -11;
/// C API return code: invalid function parameter.
pub const BLOSC2_ERROR_INVALID_PARAM: i32 = -12;
/// C API return code: file read failure.
pub const BLOSC2_ERROR_FILE_READ: i32 = -13;
/// C API return code: file write failure.
pub const BLOSC2_ERROR_FILE_WRITE: i32 = -14;
/// C API return code: file open failure.
pub const BLOSC2_ERROR_FILE_OPEN: i32 = -15;
/// C API return code: item not found.
pub const BLOSC2_ERROR_NOT_FOUND: i32 = -16;
/// C API return code: bad run-length encoding.
pub const BLOSC2_ERROR_RUN_LENGTH: i32 = -17;
/// C API return code: filter pipeline failure.
pub const BLOSC2_ERROR_FILTER_PIPELINE: i32 = -18;
/// C API return code: chunk insert failure.
pub const BLOSC2_ERROR_CHUNK_INSERT: i32 = -19;
/// C API return code: chunk append failure.
pub const BLOSC2_ERROR_CHUNK_APPEND: i32 = -20;
/// C API return code: chunk update failure.
pub const BLOSC2_ERROR_CHUNK_UPDATE: i32 = -21;
/// C API return code: 2 GiB limit exceeded.
pub const BLOSC2_ERROR_2GB_LIMIT: i32 = -22;
/// C API return code: super-chunk copy failure.
pub const BLOSC2_ERROR_SCHUNK_COPY: i32 = -23;
/// C API return code: wrong frame type.
pub const BLOSC2_ERROR_FRAME_TYPE: i32 = -24;
/// C API return code: file truncate failure.
pub const BLOSC2_ERROR_FILE_TRUNCATE: i32 = -25;
/// C API return code: thread creation failure.
pub const BLOSC2_ERROR_THREAD_CREATE: i32 = -26;
/// C API return code: postfilter failure.
pub const BLOSC2_ERROR_POSTFILTER: i32 = -27;
/// C API return code: special-frame failure.
pub const BLOSC2_ERROR_FRAME_SPECIAL: i32 = -28;
/// C API return code: special super-chunk failure.
pub const BLOSC2_ERROR_SCHUNK_SPECIAL: i32 = -29;
/// C API return code: I/O plugin failure.
pub const BLOSC2_ERROR_PLUGIN_IO: i32 = -30;
/// C API return code: file remove failure.
pub const BLOSC2_ERROR_FILE_REMOVE: i32 = -31;
/// C API return code: null pointer.
pub const BLOSC2_ERROR_NULL_POINTER: i32 = -32;
/// C API return code: invalid index.
pub const BLOSC2_ERROR_INVALID_INDEX: i32 = -33;
/// C API return code: metalayer not found.
pub const BLOSC2_ERROR_METALAYER_NOT_FOUND: i32 = -34;
/// C API return code: maximum buffer size exceeded.
pub const BLOSC2_ERROR_MAX_BUFSIZE_EXCEEDED: i32 = -35;
/// C API return code: tuner failure.
pub const BLOSC2_ERROR_TUNER: i32 = -36;

/// Built-in I/O backend ID: filesystem.
pub const BLOSC2_IO_FILESYSTEM: i32 = 0;
/// Built-in I/O backend ID: memory-mapped filesystem.
pub const BLOSC2_IO_FILESYSTEM_MMAP: i32 = 1;
/// First I/O backend ID outside the Blosc-defined set.
pub const BLOSC_IO_LAST_BLOSC_DEFINED: i32 = 2;
/// First I/O backend ID outside the registered set.
pub const BLOSC_IO_LAST_REGISTERED: i32 = 32;
/// First I/O backend ID reserved for Blosc-defined callbacks.
pub const BLOSC2_IO_BLOSC_DEFINED: i32 = 32;
/// First I/O backend ID reserved for registered callbacks.
pub const BLOSC2_IO_REGISTERED: i32 = 160;
/// First I/O backend ID reserved for user-defined callbacks.
pub const BLOSC2_IO_USER_DEFINED: i32 = 256;

/// Maximum number of filters in the filter pipeline.
pub const BLOSC2_MAX_FILTERS: usize = 6;
/// First filter ID reserved for Blosc-defined filters.
pub const BLOSC2_DEFINED_FILTERS_START: u8 = 0;
/// Last filter ID reserved for Blosc-defined filters.
pub const BLOSC2_DEFINED_FILTERS_STOP: u8 = 31;
/// Filter ID: no filter (and Blosc1 no-shuffle).
pub const BLOSC_NOFILTER: u8 = 0;
/// Blosc1-compatible filter ID: no shuffle.
pub const BLOSC_NOSHUFFLE: u8 = BLOSC_NOFILTER;
/// Filter ID: byte-wise shuffle. `filters_meta` is the number of bytestreams (0 = typesize).
pub const BLOSC_SHUFFLE: u8 = 1;
/// Filter ID: bit-wise shuffle. `filters_meta` is unused.
pub const BLOSC_BITSHUFFLE: u8 = 2;
/// Filter ID: delta filter (bitwise XOR relative to a reference). `filters_meta` is unused.
pub const BLOSC_DELTA: u8 = 3;
/// Filter ID: truncate mantissa precision. Positive `filters_meta` keeps bits; negative zeros bits.
pub const BLOSC_TRUNC_PREC: u8 = 4;
/// First filter ID reserved for globally registered Blosc2 filters.
pub const BLOSC2_GLOBAL_REGISTERED_FILTERS_START: u8 = 32;
/// Last filter ID reserved for globally registered Blosc2 filters.
pub const BLOSC2_GLOBAL_REGISTERED_FILTERS_STOP: u8 = 159;
/// Number of globally registered Blosc2 filters in the vendored C header.
pub const BLOSC2_GLOBAL_REGISTERED_FILTERS: u8 = 5;
/// Global plugin filter ID: NDim cell grouping.
pub const BLOSC_FILTER_NDCELL: u8 = 32;
/// Global plugin filter ID: NDim mean replacement.
pub const BLOSC_FILTER_NDMEAN: u8 = 33;
/// Global plugin filter ID: legacy buggy byte-delta filter.
pub const BLOSC_FILTER_BYTEDELTA_BUGGY: u8 = 34;
/// Global plugin filter ID: byte-wise delta.
pub const BLOSC_FILTER_BYTEDELTA: u8 = 35;
/// Global plugin filter ID: integer precision truncation.
pub const BLOSC_FILTER_INT_TRUNC: u8 = 36;
/// First filter ID reserved for user-registered filters.
///
/// IDs 32..=159 are reserved for globally registered Blosc2 filters.
pub const BLOSC2_USER_DEFINED_FILTERS_START: u8 = 160;
/// C-name alias for the first user-registered filter ID.
pub const BLOSC2_USER_REGISTERED_FILTERS_START: u8 = BLOSC2_USER_DEFINED_FILTERS_START;
/// Last user-registered filter ID.
pub const BLOSC2_USER_REGISTERED_FILTERS_STOP: u8 = u8::MAX;
/// Maximum number of user filters that can be registered through the C API.
pub const BLOSC2_MAX_UDFILTERS: usize = 16;
/// First filter code outside the Blosc built-in filter set.
pub const BLOSC_LAST_FILTER: u8 = 5;
/// Last globally registered filter ID in the vendored C header.
pub const BLOSC_LAST_REGISTERED_FILTER: u8 =
    BLOSC2_GLOBAL_REGISTERED_FILTERS_START + BLOSC2_GLOBAL_REGISTERED_FILTERS - 1;

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
/// C public compressor name for BloscLZ.
pub const BLOSC_BLOSCLZ_COMPNAME: &str = "blosclz";
/// C public compressor name for LZ4.
pub const BLOSC_LZ4_COMPNAME: &str = "lz4";
/// C public compressor name for LZ4HC.
pub const BLOSC_LZ4HC_COMPNAME: &str = "lz4hc";
/// C public compressor name for Zlib.
pub const BLOSC_ZLIB_COMPNAME: &str = "zlib";
/// C public compressor name for Zstd.
pub const BLOSC_ZSTD_COMPNAME: &str = "zstd";
/// First codec ID reserved for Blosc-defined codecs.
pub const BLOSC2_DEFINED_CODECS_START: u8 = 0;
/// Last codec ID reserved for Blosc-defined codecs.
pub const BLOSC2_DEFINED_CODECS_STOP: u8 = 31;
/// First codec ID reserved for globally registered Blosc2 codecs.
pub const BLOSC2_GLOBAL_REGISTERED_CODECS_START: u8 = 32;
/// Last codec ID reserved for globally registered Blosc2 codecs.
pub const BLOSC2_GLOBAL_REGISTERED_CODECS_STOP: u8 = 159;
/// Number of globally registered Blosc2 codecs in the vendored C header.
pub const BLOSC2_GLOBAL_REGISTERED_CODECS: u8 = 5;
/// Global plugin codec ID: NDLZ.
pub const BLOSC_CODEC_NDLZ: u8 = 32;
/// Global plugin codec ID: ZFP fixed-accuracy mode.
pub const BLOSC_CODEC_ZFP_FIXED_ACCURACY: u8 = 33;
/// Global plugin codec ID: ZFP fixed-precision mode.
pub const BLOSC_CODEC_ZFP_FIXED_PRECISION: u8 = 34;
/// Global plugin codec ID: ZFP fixed-rate mode.
pub const BLOSC_CODEC_ZFP_FIXED_RATE: u8 = 35;
/// Global plugin codec ID: OpenHTJ2K.
pub const BLOSC_CODEC_OPENHTJ2K: u8 = 36;
/// Global plugin codec ID: Grok.
pub const BLOSC_CODEC_GROK: u8 = 37;
/// Global plugin codec ID: OpenZL.
pub const BLOSC_CODEC_OPENZL: u8 = 38;
/// First codec ID reserved for user-registered codecs.
///
/// IDs 32..=159 are reserved for globally registered Blosc2 codecs.
pub const BLOSC2_USER_DEFINED_CODECS_START: u8 = 160;
/// Last user-defined codec ID.
pub const BLOSC2_USER_DEFINED_CODECS_STOP: u8 = u8::MAX;
/// C-name alias for the first user-registered codec ID.
pub const BLOSC2_USER_REGISTERED_CODECS_START: u8 = BLOSC2_USER_DEFINED_CODECS_START;
/// Last user-registered codec ID.
pub const BLOSC2_USER_REGISTERED_CODECS_STOP: u8 = BLOSC2_USER_DEFINED_CODECS_STOP;
/// Last globally registered codec ID in the vendored C header.
pub const BLOSC_LAST_REGISTERED_CODEC: u8 =
    BLOSC2_GLOBAL_REGISTERED_CODECS_START + BLOSC2_GLOBAL_REGISTERED_CODECS - 1;

/// Compression library format code for BloscLZ.
pub const BLOSC_BLOSCLZ_LIB: u8 = 0;
/// Compression library format code for LZ4/LZ4HC.
pub const BLOSC_LZ4_LIB: u8 = 1;
/// Compression library format code for Zlib.
pub const BLOSC_ZLIB_LIB: u8 = 3;
/// Compression library format code for Zstd.
pub const BLOSC_ZSTD_LIB: u8 = 4;
/// Compression library format code for user-defined codecs.
pub const BLOSC_UDCODEC_LIB: u8 = 6;
/// Compression library format code for codecs defined by the enclosing super-chunk.
pub const BLOSC_SCHUNK_LIB: u8 = 7;
/// C public library name for BloscLZ.
pub const BLOSC_BLOSCLZ_LIBNAME: &str = "BloscLZ";
/// C public library name for LZ4/LZ4HC.
pub const BLOSC_LZ4_LIBNAME: &str = "LZ4";
/// C public library name for Zlib.
pub const BLOSC_ZLIB_LIBNAME: &str = "Zlib";
/// C public library name for Zstd.
pub const BLOSC_ZSTD_LIBNAME: &str = "Zstd";

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
pub const BLOSC_UDCODEC_FORMAT: u8 = 6;
/// Codec format code signalling that the codec is defined by the enclosing super-chunk.
pub const BLOSC_SCHUNK_FORMAT: u8 = BLOSC_SCHUNK_LIB;

/// On-disk format version for BloscLZ-compressed streams.
pub const BLOSC_BLOSCLZ_VERSION_FORMAT: u8 = 1;
/// On-disk format version for LZ4/LZ4HC-compressed streams.
pub const BLOSC_LZ4_VERSION_FORMAT: u8 = 1;
/// On-disk format version for LZ4HC-compressed streams.
pub const BLOSC_LZ4HC_VERSION_FORMAT: u8 = BLOSC_LZ4_VERSION_FORMAT;
/// On-disk format version for Zlib-compressed streams.
pub const BLOSC_ZLIB_VERSION_FORMAT: u8 = 1;
/// On-disk format version for Zstd-compressed streams.
pub const BLOSC_ZSTD_VERSION_FORMAT: u8 = 1;
/// On-disk format version for user-defined codec streams.
pub const BLOSC_UDCODEC_VERSION_FORMAT: u8 = 1;

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
/// blosc2_flags (byte 31, bit 7): codec was instrumented (development use).
pub const BLOSC2_INSTR_CODEC: u8 = 0x80;
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
/// Last valid special-value type ID.
pub const BLOSC2_SPECIAL_LASTID: u8 = BLOSC2_SPECIAL_UNINIT;
/// Mask used to extract the 3-bit special-value type from blosc2_flags.
pub const BLOSC2_SPECIAL_MASK: u8 = 0x7;

/// Maximum fixed or variable-length metalayers supported by C-Blosc2 frame writers.
pub const BLOSC2_MAX_METALAYERS: usize = 16;
/// Maximum fixed-size metalayer name length, excluding the trailing NUL byte.
pub const BLOSC2_METALAYER_NAME_MAXLEN: usize = 31;
/// Maximum variable-length metalayers supported by C-Blosc2 frame writers.
pub const BLOSC2_MAX_VLMETALAYERS: usize = 8 * 1024;
/// Maximum variable-length metalayer name length, excluding the trailing NUL byte.
pub const BLOSC2_VLMETALAYERS_NAME_MAXLEN: usize = BLOSC2_METALAYER_NAME_MAXLEN;

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

/// Maximum number of dimensions supported by Blosc2 core helpers.
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
