//! Core compression and decompression engine for Blosc2 chunks.
//!
//! This module implements the chunk-level compress/decompress pipeline:
//! block splitting and sizing, the filter pipeline (shuffle, bitshuffle,
//! delta, trunc_prec), codec dispatch (BloscLZ, LZ4, LZ4HC, Zlib, Zstd),
//! special-value chunks, dictionary training, variable-length blocks and
//! the Blosc1 compatibility wrappers.
//!
//! The public API exposes the high-level [`compress`]/[`decompress`] pair
//! together with their `*_with_dparams` / `*_with_threads` variants,
//! [`CParams`]/[`DParams`] parameter structs, and the Blosc1-style helpers
//! [`blosc1_compress`]/[`blosc1_decompress`]. Process-wide settings used by
//! the Blosc1 API are exposed via the `blosc1_set_*` / `blosc2_set_*`
//! family of functions.

#![allow(clippy::too_many_arguments, clippy::type_complexity)]

use crate::codecs;
use crate::constants::*;
use crate::filters;
use crate::header::ChunkHeader;
use rayon::prelude::*;
use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::HashMap;
use std::ffi::{c_uint, c_void};
use std::sync::atomic::{AtomicBool, AtomicI16, AtomicI32, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use zstd_pure_rs::common::bits::ZSTD_highbit32;
use zstd_pure_rs::common::error::ERR_isError;
use zstd_pure_rs::common::xxhash::XXH64;
use zstd_pure_rs::compress::fse_compress::FSE_normalizeCount;
use zstd_pure_rs::compress::fse_compress::FSE_writeNCount;
use zstd_pure_rs::compress::huf_compress::{
    HUF_buildCTable_wksp, HUF_optimalTableLog, HUF_readCTableHeader, HUF_writeCTable,
    HUF_CTABLE_WORKSPACE_SIZE_U32,
};
use zstd_pure_rs::compress::zstd_compress::{
    ZSTD_compressBegin_usingCDict_deprecated, ZSTD_compressBlock_deprecated, ZSTD_compressBound,
    ZSTD_compress_usingCDict, ZSTD_createCCtx, ZSTD_createCDict, ZSTD_createCDict_byReference,
    ZSTD_getSeqStore,
};
use zstd_pure_rs::compress::zstd_hashes::ZSTD_hashPtr;
use zstd_pure_rs::decompress::zstd_decompress::ZSTD_MAGIC_DICTIONARY;
use zstd_pure_rs::decompress::zstd_decompress_block::{
    LLFSELog, MLFSELog, MaxLL, MaxML, MaxOff, OffFSELog,
};

#[cfg(feature = "_ffi")]
#[repr(C)]
struct Blosc2DParamsFfi {
    nthreads: i16,
    schunk: *mut c_void,
    postfilter: *mut c_void,
    postparams: *mut c_void,
    typesize: i32,
}

unsafe extern "C" {
    fn ZDICT_trainFromBuffer(
        dict_buffer: *mut c_void,
        dict_buffer_capacity: usize,
        samples_buffer: *const c_void,
        samples_sizes: *const usize,
        nb_samples: c_uint,
    ) -> usize;
    fn ZDICT_isError(error_code: usize) -> c_uint;
}

#[cfg(feature = "_ffi")]
unsafe extern "C" {
    #[link_name = "blosc2_decompress"]
    fn c_blosc2_decompress(
        src: *const c_void,
        srcsize: i32,
        dest: *mut c_void,
        destsize: i32,
    ) -> i32;
    #[link_name = "blosc2_create_dctx"]
    fn c_blosc2_create_dctx(dparams: Blosc2DParamsFfi) -> *mut c_void;
    #[link_name = "blosc2_free_ctx"]
    fn c_blosc2_free_ctx(context: *mut c_void);
    #[link_name = "blosc2_vldecompress_ctx"]
    fn c_blosc2_vldecompress_ctx(
        context: *mut c_void,
        src: *const c_void,
        srcsize: i32,
        dests: *mut *mut c_void,
        destsizes: *mut i32,
        maxblocks: i32,
    ) -> i32;
    #[link_name = "blosc2_vldecompress_block_ctx"]
    fn c_blosc2_vldecompress_block_ctx(
        context: *mut c_void,
        src: *const c_void,
        srcsize: i32,
        nblock: i32,
        dest: *mut *mut u8,
        destsize: *mut i32,
    ) -> i32;
    fn free(ptr: *mut c_void);
}

/// Process-wide default codec used by the Blosc1 API (`blosc1_compress`).
/// Mirrors C-Blosc2's signed `g_compressor` state, including the historical
/// `-1` value left by `blosc1_set_compressor` for an unknown name.
static GLOBAL_COMPRESSOR: AtomicI32 = AtomicI32::new(BLOSC_BLOSCLZ as i32);

/// Process-wide override blocksize. 0 means "automatic". Mirrors `g_force_blocksize`.
static GLOBAL_BLOCKSIZE: AtomicI32 = AtomicI32::new(0);

/// Process-wide splitmode. Mirrors `g_splitmode`.
static GLOBAL_SPLITMODE: AtomicI32 = AtomicI32::new(BLOSC_FORWARD_COMPAT_SPLIT);

/// Process-wide thread count used by the Blosc1 API. Mirrors `g_nthreads`.
static GLOBAL_NTHREADS: AtomicI16 = AtomicI16::new(1);

/// Whether to prepend a delta filter in the Blosc1 API pipeline. Mirrors `g_delta`.
static GLOBAL_DELTA: AtomicBool = AtomicBool::new(false);

static THREAD_POOLS: OnceLock<Mutex<HashMap<i16, Arc<rayon::ThreadPool>>>> = OnceLock::new();
const MEMCPY_PARALLEL_MIN_BYTES: usize = 8 * 1024 * 1024;
const MEMCPY_PARALLEL_MIN_BYTES_PER_THREAD: usize = 2 * 1024 * 1024;

type CompressScratch = (Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>);

thread_local! {
    static DECOMPRESS_SCRATCH: RefCell<(Vec<u8>, Vec<u8>)> =
        const { RefCell::new((Vec::new(), Vec::new())) };
    static COMPRESS_SCRATCH: RefCell<CompressScratch> =
        const { RefCell::new((Vec::new(), Vec::new(), Vec::new(), Vec::new())) };
}

/// Access the lazily-initialized map of cached rayon thread pools keyed by thread count.
fn thread_pools() -> &'static Mutex<HashMap<i16, Arc<rayon::ThreadPool>>> {
    THREAD_POOLS.get_or_init(|| Mutex::new(HashMap::new()))
}

pub(crate) fn free_cached_resources() {
    if let Some(pools) = THREAD_POOLS.get() {
        pools
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clear();
    }
}

/// Return a cached rayon thread pool sized for `nthreads`, or `None` when serial execution suffices.
fn thread_pool_for(nthreads: i16) -> Option<Arc<rayon::ThreadPool>> {
    if nthreads <= 1 {
        return None;
    }

    {
        let pools = thread_pools()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(pool) = pools.get(&nthreads) {
            return Some(Arc::clone(pool));
        }
    }

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(nthreads as usize)
        .build()
        .ok()
        .map(Arc::new)?;

    let mut pools = thread_pools()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let entry = pools.entry(nthreads).or_insert_with(|| Arc::clone(&pool));
    Some(Arc::clone(entry))
}

fn effective_nthreads(requested: i16, jobs: usize) -> i16 {
    if requested <= 1 || jobs <= 1 {
        return 1;
    }
    let available = std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(jobs);
    requested
        .min(jobs.min(available).min(i16::MAX as usize) as i16)
        .max(1)
}

fn memcpy_parallel_threads(nbytes: usize, requested: i16) -> i16 {
    if nbytes < MEMCPY_PARALLEL_MIN_BYTES {
        return 1;
    }
    let useful_jobs = nbytes / MEMCPY_PARALLEL_MIN_BYTES_PER_THREAD;
    effective_nthreads(requested, useful_jobs)
}

/// Heuristic: whether copying a memcpyed chunk of `nbytes` is worth parallelizing across `nthreads`.
fn should_parallelize_memcpyed(nbytes: usize, nthreads: i16) -> bool {
    let threads = memcpy_parallel_threads(nbytes, nthreads);
    if threads <= 1 {
        return false;
    }
    nbytes.div_ceil(threads as usize) >= MEMCPY_PARALLEL_MIN_BYTES_PER_THREAD
}

/// Map a codec numeric code to its canonical lowercase name.
fn compressor_code_to_name(code: u8) -> Option<&'static str> {
    match code {
        BLOSC_BLOSCLZ => Some("blosclz"),
        BLOSC_LZ4 => Some("lz4"),
        BLOSC_LZ4HC => Some("lz4hc"),
        BLOSC_ZLIB => Some("zlib"),
        BLOSC_ZSTD => Some("zstd"),
        _ => None,
    }
}

/// Map a built-in compressor code to its canonical C-Blosc2 name.
pub fn blosc2_compcode_to_compname(code: u8) -> Option<&'static str> {
    compressor_code_to_name(code).or_else(|| codecs::registered_codec_name(code))
}

/// C-return variant for [`blosc2_compcode_to_compname`].
pub fn blosc2_compcode_to_compname_c(code: u8) -> (i32, Option<&'static str>) {
    match blosc2_compcode_to_compname(code) {
        Some(name) => (i32::from(code), Some(name)),
        None if code >= BLOSC_LAST_CODEC => (i32::from(code), None),
        None => (-1, None),
    }
}

/// C-return variant for signed C `int` compressor codes.
pub fn blosc2_compcode_to_compname_int_c(code: i32) -> (i32, Option<&'static str>) {
    let code_u8 = match u8::try_from(code) {
        Ok(code) => code,
        Err(_) => return (-1, None),
    };
    blosc2_compcode_to_compname_c(code_u8)
}

/// Map a canonical C-Blosc2 compressor name to its code.
pub fn blosc2_compname_to_compcode(name: &str) -> Option<u8> {
    match name {
        "blosclz" => Some(BLOSC_BLOSCLZ),
        "lz4" => Some(BLOSC_LZ4),
        "lz4hc" => Some(BLOSC_LZ4HC),
        "zlib" => Some(BLOSC_ZLIB),
        "zstd" => Some(BLOSC_ZSTD),
        _ => codecs::registered_codec_code(name),
    }
}

/// C-return variant for [`blosc2_compname_to_compcode`].
pub fn blosc2_compname_to_compcode_c(name: &str) -> i32 {
    blosc2_compname_to_compcode(name)
        .map(i32::from)
        .unwrap_or(-1)
}

/// Return the built-in compressor names in C-Blosc2 comma-separated list order.
pub fn blosc2_list_compressors() -> &'static str {
    "blosclz,lz4,lz4hc,zlib,zstd"
}

fn compformat_to_complib_name(compformat: u8) -> Option<&'static str> {
    match compformat {
        BLOSC_BLOSCLZ_FORMAT => Some("BloscLZ"),
        BLOSC_LZ4_FORMAT => Some("LZ4"),
        BLOSC_ZLIB_FORMAT => Some("Zlib"),
        BLOSC_ZSTD_FORMAT => Some("Zstd"),
        _ => codecs::registered_codec_name_by_complib(compformat),
    }
}

fn codec_version_for_header(compcode: u8) -> u8 {
    codecs::registered_codec_version(compcode).unwrap_or_else(|| compcode_to_version(compcode))
}

fn unsupported_global_codec_error(compcode: u8) -> Option<&'static str> {
    if codecs::is_known_zfp_codec(compcode) && !codecs::is_static_global_codec_enabled(compcode) {
        Some("ZFP plugin codecs are not supported")
    } else if codecs::is_known_global_codec(compcode)
        && !codecs::is_static_global_codec_enabled(compcode)
    {
        Some("Global plugin codec is not supported")
    } else {
        None
    }
}

fn unsupported_global_filter_error(filter: u8) -> Option<&'static str> {
    if filters::is_known_global_filter(filter) && !filters::is_static_global_filter_enabled(filter)
    {
        Some("Global plugin filter is not supported")
    } else {
        None
    }
}

fn supported_core_or_static_codec(compcode: u8) -> bool {
    matches!(
        compcode,
        BLOSC_BLOSCLZ | BLOSC_LZ4 | BLOSC_LZ4HC | BLOSC_ZLIB | BLOSC_ZSTD
    ) || codecs::is_static_global_codec_enabled(compcode)
}

fn unsupported_global_filter_for_cparams(filter: u8, cparams: &CParams) -> Option<&'static str> {
    if filters::global_filter_requires_b2nd_metadata(filter) && cparams.b2nd_metalayer.is_none() {
        Some("Filter pipeline failed")
    } else {
        unsupported_global_filter_error(filter)
    }
}

/// Return the bundled C-Blosc2-compatible version string.
pub fn blosc2_get_version_string() -> &'static str {
    "3.0.0.dev"
}

/// Return the compression library code, library name, and library version for a compressor.
pub fn blosc2_get_complib_info(compname: &str) -> Option<(u8, &'static str, &'static str)> {
    let (code, name, version) = match compname {
        "blosclz" => (BLOSC_BLOSCLZ_FORMAT, "BloscLZ", "2.5.3"),
        "lz4" | "lz4hc" => (BLOSC_LZ4_FORMAT, "LZ4", "1.10.0"),
        "zlib" => (BLOSC_ZLIB_FORMAT, "Zlib", "2.0.7"),
        "zstd" => (BLOSC_ZSTD_FORMAT, "Zstd", "1.5.7"),
        _ => return codecs::registered_codec_complib_info(compname),
    };
    Some((code, name, version))
}

/// Set the process-wide default codec used by `blosc1_compress` by name.
/// Returns the selected codec code, or an error if the name is unknown.
///
/// Recognized names: `blosclz`, `lz4`, `lz4hc`, `zlib`, `zstd`.
pub fn blosc1_set_compressor(name: &str) -> Result<u8, &'static str> {
    let Some(code) = blosc2_compname_to_compcode(name) else {
        GLOBAL_COMPRESSOR.store(-1, Ordering::Relaxed);
        return Err("Unrecognized compressor name");
    };
    if code >= BLOSC_LAST_CODEC {
        return Err("Unsupported Blosc1 compressor code");
    }
    GLOBAL_COMPRESSOR.store(i32::from(code), Ordering::Relaxed);
    Ok(code)
}

/// C-return variant for [`blosc1_set_compressor`].
pub fn blosc1_set_compressor_c(name: &str) -> i32 {
    match blosc2_compname_to_compcode(name) {
        Some(code) if code >= BLOSC_LAST_CODEC => BLOSC2_ERROR_CODEC_SUPPORT,
        Some(code) => {
            GLOBAL_COMPRESSOR.store(i32::from(code), Ordering::Relaxed);
            i32::from(code)
        }
        None => {
            GLOBAL_COMPRESSOR.store(-1, Ordering::Relaxed);
            -1
        }
    }
}

/// Blosc1 compatibility C-return alias for [`blosc1_set_compressor_c`].
pub use self::blosc1_set_compressor_c as blosc_set_compressor_c;

/// Set the process-wide default codec by numeric code. Returns the previous code.
pub fn blosc1_set_compressor_code(code: u8) -> u8 {
    GLOBAL_COMPRESSOR.swap(i32::from(code), Ordering::Relaxed) as u8
}

/// Get the process-wide default codec currently used by `blosc1_compress`.
/// Mirrors C `blosc1_get_compressor`: invalid global compressor state is NULL.
pub fn blosc1_get_compressor() -> Option<&'static str> {
    let code = GLOBAL_COMPRESSOR.load(Ordering::Relaxed);
    blosc2_compcode_to_compname_int_c(code).1
}

/// Ergonomic string-returning variant of [`blosc1_get_compressor`].
pub fn blosc1_get_compressor_or_unknown() -> &'static str {
    blosc1_get_compressor().unwrap_or("unknown")
}

/// Get the current process-wide compressor code.
pub fn blosc1_get_compressor_code() -> u8 {
    GLOBAL_COMPRESSOR.load(Ordering::Relaxed) as u8
}

fn blosc1_get_compressor_code_i32() -> i32 {
    GLOBAL_COMPRESSOR.load(Ordering::Relaxed)
}

/// Blosc1-style getitem adapter. Mirrors C `blosc1_getitem`, returning bytes
/// written or a negative `BLOSC2_ERROR_*` code.
pub fn blosc1_getitem(chunk: &[u8], start: i32, nitems: i32, dest: &mut [u8]) -> i32 {
    let chunk = match checked_c_declared_chunk(chunk, i32::MAX) {
        Ok(chunk) => chunk,
        Err(code) => return code,
    };
    getitem_c(chunk, start, nitems, dest)
}

// Blosc1 compatibility aliases matching C-Blosc2's `BLOSC1_COMPAT` macro names.
pub use self::blosc1_cbuffer_metainfo as blosc_cbuffer_metainfo;
pub use self::blosc1_cbuffer_sizes as blosc_cbuffer_sizes;
pub use self::blosc1_cbuffer_validate as blosc_cbuffer_validate;
pub use self::blosc1_compress as blosc_compress;
pub use self::blosc1_decompress as blosc_decompress;
pub use self::blosc1_get_blocksize as blosc_get_blocksize;
pub use self::blosc1_get_compressor as blosc_get_compressor;
pub use self::blosc1_getitem as blosc_getitem;
pub use self::blosc1_set_blocksize as blosc_set_blocksize;
pub use self::blosc1_set_compressor_c as blosc_set_compressor;
pub use self::blosc1_set_splitmode as blosc_set_splitmode;
pub use self::blosc2_cbuffer_complib as blosc_cbuffer_complib;
pub use self::blosc2_compcode_to_compname as blosc_compcode_to_compname;
pub use self::blosc2_compname_to_compcode as blosc_compname_to_compcode;
pub use self::blosc2_get_complib_info as blosc_get_complib_info;
pub use self::blosc2_get_nthreads as blosc_get_nthreads;
pub use self::blosc2_get_version_string as blosc_get_version_string;
pub use self::blosc2_list_compressors as blosc_list_compressors;
pub use self::blosc2_set_nthreads as blosc_set_nthreads;
pub use self::cbuffer_versions_c as blosc_cbuffer_versions;

/// Force a specific blocksize for `blosc1_compress`. Pass 0 to restore automatic sizing.
pub fn blosc1_set_blocksize(blocksize: i32) {
    GLOBAL_BLOCKSIZE.store(blocksize, Ordering::Relaxed);
}

/// Get the forced blocksize; 0 means automatic. Mirrors `blosc1_get_blocksize`.
pub fn blosc1_get_blocksize() -> i32 {
    GLOBAL_BLOCKSIZE.load(Ordering::Relaxed)
}

/// Set the splitmode used by `blosc1_compress`.
/// Valid values: `BLOSC_ALWAYS_SPLIT`, `BLOSC_NEVER_SPLIT`, `BLOSC_AUTO_SPLIT`,
/// `BLOSC_FORWARD_COMPAT_SPLIT`. Mirrors `blosc1_set_splitmode`.
pub fn blosc1_set_splitmode(splitmode: i32) {
    GLOBAL_SPLITMODE.store(splitmode, Ordering::Relaxed);
}

/// Get the current splitmode. Mirrors `blosc1_get_splitmode`.
pub fn blosc1_get_splitmode() -> i32 {
    GLOBAL_SPLITMODE.load(Ordering::Relaxed)
}

/// Set the number of threads used by `blosc1_compress`.
///
/// Mirrors `blosc2_set_nthreads`, including its nonpositive-count transition
/// behavior through C's `check_nthreads`.
pub fn blosc2_set_nthreads(nthreads: i16) -> i16 {
    let previous = GLOBAL_NTHREADS.load(Ordering::Relaxed);
    if nthreads != previous {
        GLOBAL_NTHREADS.store(nthreads, Ordering::Relaxed);
        if previous <= 0 || nthreads < 0 {
            return BLOSC2_ERROR_INVALID_PARAM as i16;
        }
    }
    previous
}

/// Get the current thread count used by `blosc1_compress`. Mirrors `blosc2_get_nthreads`.
pub fn blosc2_get_nthreads() -> i16 {
    GLOBAL_NTHREADS.load(Ordering::Relaxed)
}

/// Enable or disable the delta filter for `blosc1_compress`.
pub fn blosc2_set_delta_enabled(enabled: bool) {
    GLOBAL_DELTA.store(enabled, Ordering::Relaxed);
}

/// C-style delta setter. Any nonzero value enables delta.
pub fn blosc2_set_delta(dodelta: i32) {
    blosc2_set_delta_enabled(dodelta != 0);
}

/// Whether the delta filter is currently enabled.
pub fn blosc2_get_delta() -> bool {
    GLOBAL_DELTA.load(Ordering::Relaxed)
}

fn parse_c_strtol_prefix(value: &str) -> i64 {
    let bytes = value.as_bytes();
    let mut idx = 0usize;
    while idx < bytes.len() && bytes[idx].is_ascii_whitespace() {
        idx += 1;
    }

    let mut sign = 1i64;
    if idx < bytes.len() {
        match bytes[idx] {
            b'-' => {
                sign = -1;
                idx += 1;
            }
            b'+' => idx += 1,
            _ => {}
        }
    }

    let mut parsed = 0i64;
    let mut saw_digit = false;
    while idx < bytes.len() && bytes[idx].is_ascii_digit() {
        saw_digit = true;
        let digit = (bytes[idx] - b'0') as i64;
        parsed = parsed.saturating_mul(10).saturating_add(digit);
        idx += 1;
    }

    if saw_digit {
        parsed.saturating_mul(sign)
    } else {
        0
    }
}

/// Apply the `BLOSC_*` environment-variable overrides documented by C-Blosc2.
/// Values are only overwritten when the corresponding env var is present and
/// parses successfully. Range validation is left to the compression setup, as
/// in C-Blosc2.
///
/// Some env vars mutate process-wide state via the public setter functions
/// (matching C's `blosc2_compress`), so calling this has durable side effects.
fn blosc_env_compressor_code(name: &str) -> Result<u8, &'static str> {
    match blosc2_compname_to_compcode(name) {
        Some(code) if code < BLOSC_LAST_CODEC => Ok(code),
        Some(_) => Err("Unsupported Blosc1 compressor code"),
        None => Err("Unsupported Blosc1 compressor code"),
    }
}

fn blosc_context_env_compressor_code(name: &str) -> Result<u8, &'static str> {
    match blosc2_compname_to_compcode(name) {
        Some(code) if code < BLOSC_LAST_CODEC => Ok(code),
        Some(_) => Err("Unsupported Blosc1 compressor code"),
        // C `blosc2_create_cctx` assigns the `-1` result from
        // `blosc2_compname_to_compcode` into the context. `blosc2_ctx_get_cparams`
        // exposes that through the uint8_t cparams field as 255.
        None => Ok(u8::MAX),
    }
}

fn apply_blosc_env_overrides(
    clevel: &mut i32,
    doshuffle: &mut u8,
    typesize: &mut i32,
    compcode: &mut i32,
) -> Result<(), &'static str> {
    if let Ok(v) = std::env::var("BLOSC_CLEVEL") {
        let parsed = parse_c_strtol_prefix(&v);
        if parsed >= 0 {
            *clevel = parsed.min(i32::MAX as i64) as i32;
        }
    }
    if let Ok(v) = std::env::var("BLOSC_SHUFFLE") {
        match v.as_str() {
            "NOSHUFFLE" => *doshuffle = BLOSC_NOFILTER,
            "SHUFFLE" => *doshuffle = BLOSC_SHUFFLE,
            "BITSHUFFLE" => *doshuffle = BLOSC_BITSHUFFLE,
            _ => {}
        }
    }
    if let Ok(v) = std::env::var("BLOSC_DELTA") {
        match v.as_str() {
            "1" => blosc2_set_delta(1),
            "0" => blosc2_set_delta(0),
            _ => {}
        }
    }
    if let Ok(v) = std::env::var("BLOSC_TYPESIZE") {
        let parsed = parse_c_strtol_prefix(&v);
        if parsed > 0 {
            *typesize = parsed as i32;
        }
    }
    if let Ok(v) = std::env::var("BLOSC_COMPRESSOR") {
        // Match C semantics: BLOSC_COMPRESSOR mutates the process-wide compressor
        // (via blosc1_set_compressor) and the new value is what gets used.
        let _ = blosc_env_compressor_code(&v);
        let _ = blosc1_set_compressor_c(&v);
        *compcode = blosc1_get_compressor_code_i32();
    }
    if let Ok(v) = std::env::var("BLOSC_BLOCKSIZE") {
        let parsed = parse_c_strtol_prefix(&v);
        if parsed > 0 {
            blosc1_set_blocksize(parsed as i32);
        }
    }
    if let Ok(v) = std::env::var("BLOSC_NTHREADS") {
        let parsed = parse_c_strtol_prefix(&v);
        if parsed > 0 {
            let _ = blosc2_set_nthreads(parsed as i16);
        }
    }
    if let Ok(v) = std::env::var("BLOSC_SPLITMODE") {
        let splitmode = match v.as_str() {
            "ALWAYS" => Some(BLOSC_ALWAYS_SPLIT),
            "NEVER" => Some(BLOSC_NEVER_SPLIT),
            "AUTO" => Some(BLOSC_AUTO_SPLIT),
            "FORWARD_COMPAT" => Some(BLOSC_FORWARD_COMPAT_SPLIT),
            _ => None,
        };
        if let Some(sm) = splitmode {
            blosc1_set_splitmode(sm);
        }
    }
    // Rust compression is already lock-free from the Blosc1 wrapper's perspective,
    // so `BLOSC_NOLOCK` is accepted as a no-op compatibility knob.
    let _ = std::env::var("BLOSC_NOLOCK");
    Ok(())
}

/// Apply the `BLOSC_*` environment-variable overrides honored by `blosc1_decompress`.
/// Returns the resulting thread count (defaults to the process-wide value).
fn apply_blosc_decompress_env_overrides() -> Result<i16, &'static str> {
    if let Ok(v) = std::env::var("BLOSC_NTHREADS") {
        let parsed = parse_c_strtol_prefix(&v);
        if parsed <= 0 || parsed > i16::MAX as i64 {
            return Err("Invalid thread count");
        }
        if blosc2_set_nthreads(parsed as i16) < 0 {
            return Err("Invalid thread count");
        }
    }
    // Rust decompression does not route through a process-global lock, so
    // `BLOSC_NOLOCK` is a no-op compatibility knob here too.
    let _ = std::env::var("BLOSC_NOLOCK");
    Ok(blosc2_get_nthreads())
}

/// Parameters passed to a user-supplied prefilter callback.
///
/// A prefilter runs once per block, ahead of the standard filter pipeline,
/// and writes its transformed output into `output`. Returning a non-zero
/// status aborts compression unless `output_is_disposable` is set.
#[derive(Debug)]
pub struct PrefilterParams<'a> {
    /// Opaque user-supplied data passed back into the callback.
    pub user_data: usize,
    /// Input block bytes.
    pub input: &'a [u8],
    /// Destination buffer for the transformed block.
    pub output: &'a mut [u8],
    /// Size in bytes of `output`.
    pub output_size: i32,
    /// Typesize the prefilter reports for the produced data.
    pub output_typesize: i32,
    /// Byte offset of the current block within the uncompressed chunk.
    pub output_offset: i32,
    /// Chunk index when invoked from a super-chunk, or `-1` for stand-alone chunks.
    pub nchunk: i64,
    /// Index of the current block within the chunk.
    pub nblock: i32,
    /// Worker thread id, or `0` in serial execution.
    pub tid: i32,
    /// When true, the engine may discard `output` if the callback fails.
    pub output_is_disposable: bool,
}

/// Parameters passed to a user-supplied postfilter callback.
///
/// A postfilter runs once per block after the backward filter pipeline,
/// before the data is delivered to the caller.
#[derive(Debug)]
pub struct PostfilterParams<'a> {
    /// Opaque user-supplied data passed back into the callback.
    pub user_data: usize,
    /// Input bytes produced by the filter pipeline.
    pub input: &'a [u8],
    /// Destination buffer for the postfilter output.
    pub output: &'a mut [u8],
    /// Size in bytes of `input`/`output`.
    pub size: i32,
    /// Logical typesize of the data.
    pub typesize: i32,
    /// Byte offset of the current block within the uncompressed chunk.
    pub offset: i32,
    /// Chunk index when invoked from a super-chunk, or `-1` for stand-alone chunks.
    pub nchunk: i64,
    /// Index of the current block within the chunk.
    pub nblock: i32,
    /// Worker thread id, or `0` in serial execution.
    pub tid: i32,
}

/// Function pointer type for a prefilter callback. A return of `0` indicates success.
pub type PrefilterFn = for<'a> fn(&mut PrefilterParams<'a>) -> i32;
/// Function pointer type for a postfilter callback. A return of `0` indicates success.
pub type PostfilterFn = for<'a> fn(&mut PostfilterParams<'a>) -> i32;

/// Run `op` on the cached rayon thread pool for `nthreads`; falls back to serial execution when `nthreads <= 1`.
pub(crate) fn with_thread_pool<T: Send>(nthreads: i16, op: impl FnOnce() -> T + Send) -> T {
    if nthreads <= 1 {
        return op();
    }
    match thread_pool_for(nthreads) {
        Some(pool) => pool.install(op),
        None => op(),
    }
}

/// Grow `buf` to exactly `len`, zero-filling any new bytes.
#[inline]
fn ensure_scratch_len(buf: &mut Vec<u8>, len: usize) {
    buf.resize(len, 0);
}

/// Grow `buf` to at least `len` without initializing the new bytes; callers must overwrite before reading.
#[inline]
fn ensure_scratch_len_uninit(buf: &mut Vec<u8>, len: usize) {
    if len > buf.len() {
        if len > buf.capacity() {
            buf.reserve(len - buf.len());
        }
        unsafe {
            buf.set_len(len);
        }
    }
}

/// Allocate a `Vec<u8>` of length `len` with uninitialized contents.
#[allow(clippy::uninit_vec)]
fn uninit_vec(len: usize) -> Vec<u8> {
    let mut buf = Vec::with_capacity(len);
    unsafe {
        // SAFETY: callers pass this buffer to decompression routines that write
        // every byte before any successful return exposes it.
        buf.set_len(len);
    }
    buf
}

fn decompression_output_buffer(len: usize, zeroed: bool) -> Result<Vec<u8>, &'static str> {
    let mut buf = Vec::new();
    buf.try_reserve_exact(len)
        .map_err(|_| "Output allocation failed")?;
    if zeroed {
        buf.resize(len, 0);
    } else {
        unsafe {
            // SAFETY: the decompression path writes every byte before exposing
            // the buffer on success. Callers request zeroed buffers for paths
            // that may intentionally leave bytes untouched.
            buf.set_len(len);
        }
    }
    Ok(buf)
}

/// Borrow two thread-local scratch buffers of `blocksize` bytes for the duration of `f`.
fn with_decompress_scratch<T>(blocksize: usize, f: impl FnOnce(&mut [u8], &mut [u8]) -> T) -> T {
    DECOMPRESS_SCRATCH.with(|scratch| {
        let mut scratch = scratch.borrow_mut();
        ensure_scratch_len(&mut scratch.0, blocksize);
        ensure_scratch_len(&mut scratch.1, blocksize);
        let (scratch1, scratch2) = &mut *scratch;
        f(&mut scratch1[..blocksize], &mut scratch2[..blocksize])
    })
}

/// Borrow four thread-local scratch buffers (sized for `blocksize`) for the duration of `f`.
fn with_compress_scratch<T>(
    blocksize: usize,
    f: impl FnOnce(&mut Vec<u8>, &mut Vec<u8>, &mut Vec<u8>, &mut Vec<u8>) -> T,
) -> T {
    COMPRESS_SCRATCH.with(|scratch| {
        let mut scratch = scratch.borrow_mut();
        let (buf1, buf2, compress_buf, prefilter_buf) = &mut *scratch;
        if buf1.len() < blocksize {
            buf1.resize(blocksize, 0);
        }
        if buf2.len() < blocksize {
            buf2.resize(blocksize, 0);
        }
        let min_compress_buf = blocksize + (blocksize / 255) + 64;
        ensure_scratch_len_uninit(compress_buf, min_compress_buf);
        f(buf1, buf2, compress_buf, prefilter_buf)
    })
}

/// Compression parameters.
#[derive(Debug, Clone)]
pub struct CParams {
    /// Codec identifier, such as `BLOSC_LZ4` or `BLOSC_ZSTD`.
    pub compcode: u8,
    /// Per-codec metadata byte stored in the extended chunk header.
    pub compcode_meta: u8,
    /// Compression level from 0 to 9.
    pub clevel: u8,
    /// Logical element size in bytes. Filters such as shuffle operate over this size.
    pub typesize: i32,
    /// Block size in bytes. Use 0 for automatic sizing.
    pub blocksize: i32, // 0 = automatic
    /// Stream split mode, such as `BLOSC_FORWARD_COMPAT_SPLIT`.
    pub splitmode: i32,
    /// Filter pipeline codes. The last position is commonly used for the primary filter.
    pub filters: [u8; BLOSC2_MAX_FILTERS],
    /// Per-filter metadata bytes.
    pub filters_meta: [u8; BLOSC2_MAX_FILTERS],
    /// Train and embed a per-chunk codec dictionary when supported.
    pub use_dict: bool,
    /// Number of worker threads for block-parallel compression.
    pub nthreads: i16,
    /// Logical super-chunk index exposed to prefilter callbacks. `-1` means standalone chunk.
    pub nchunk: i64,
    /// Opaque pointer-sized schunk handle exposed to C-compatible plugin callbacks.
    pub schunk: usize,
    /// Opaque codec-specific parameters exposed as `blosc2_cparams.codec_params`.
    pub codec_params: usize,
    /// Whether codec instrumentation is requested. Exposed as `blosc2_cparams.instr_codec`.
    pub instr_codec: bool,
    /// Raw `b2nd` metalayer payload exposed to B2ND-aware plugin callbacks.
    pub b2nd_metalayer: Option<Vec<u8>>,
    /// Optional prefilter hook applied before the standard filter pipeline.
    pub prefilter: Option<PrefilterFn>,
    /// User data pointer exposed to `prefilter`.
    pub prefilter_user_data: usize,
    /// Output typesize reported to `prefilter`. `0` means use the input typesize.
    pub prefilter_output_typesize: i32,
    /// Whether the prefilter output may be discarded when the callback returns non-zero.
    pub prefilter_output_is_disposable: bool,
}

impl Default for CParams {
    fn default() -> Self {
        CParams {
            compcode: BLOSC_BLOSCLZ,
            compcode_meta: 0,
            clevel: 5,
            typesize: 8,
            blocksize: 0,
            splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            filters_meta: [0; BLOSC2_MAX_FILTERS],
            use_dict: false,
            nthreads: 1,
            nchunk: -1,
            schunk: 0,
            codec_params: 0,
            instr_codec: false,
            b2nd_metalayer: None,
            prefilter: None,
            prefilter_user_data: 0,
            prefilter_output_typesize: 0,
            prefilter_output_is_disposable: false,
        }
    }
}

/// C-name alias returning default compression parameters.
pub fn blosc2_get_blosc2_cparams_defaults() -> CParams {
    CParams::default()
}

/// Decompression parameters.
#[derive(Debug, Clone)]
pub struct DParams {
    /// Number of worker threads for block-parallel decompression.
    pub nthreads: i16,
    /// Optional postfilter hook applied after the backward filter pipeline.
    pub postfilter: Option<PostfilterFn>,
    /// User data pointer exposed to `postfilter`.
    pub postfilter_user_data: usize,
    /// Logical typesize reported to `postfilter`.
    pub typesize: i32,
    /// Logical super-chunk index exposed to postfilter callbacks. `-1` means standalone chunk.
    pub nchunk: i64,
    /// Opaque pointer-sized schunk handle exposed to C-compatible plugin callbacks.
    pub schunk: usize,
    /// Raw `b2nd` metalayer payload exposed to B2ND-aware plugin callbacks.
    pub b2nd_metalayer: Option<Vec<u8>>,
    /// Optional per-block mask. `true` means skip decompression for that block.
    pub block_maskout: Option<Vec<bool>>,
}

impl Default for DParams {
    fn default() -> Self {
        DParams {
            nthreads: 1,
            postfilter: None,
            postfilter_user_data: 0,
            typesize: 8,
            nchunk: -1,
            schunk: 0,
            b2nd_metalayer: None,
            block_maskout: None,
        }
    }
}

/// C-name alias returning default decompression parameters.
pub fn blosc2_get_blosc2_dparams_defaults() -> DParams {
    DParams::default()
}

fn filter_cparams_context(cparams: &CParams, blocksize: i32) -> filters::FilterCParamsContext {
    filters::FilterCParamsContext {
        compcode: cparams.compcode,
        compcode_meta: cparams.compcode_meta,
        clevel: cparams.clevel,
        use_dict: cparams.use_dict,
        typesize: cparams.typesize,
        blocksize,
        splitmode: cparams.splitmode,
        filters: cparams.filters,
        filters_meta: cparams.filters_meta,
        nthreads: cparams.nthreads,
        nchunk: cparams.nchunk,
        user_data: cparams.prefilter_user_data,
        preparams: cparams.prefilter_user_data,
        tuner_id: 0,
        instr_codec: cparams.instr_codec,
        codec_params: cparams.codec_params,
    }
}

fn filter_dparams_context(dparams: &DParams) -> filters::FilterDParamsContext {
    filters::FilterDParamsContext {
        nthreads: dparams.nthreads,
        typesize: dparams.typesize,
        nchunk: dparams.nchunk,
        user_data: dparams.postfilter_user_data,
        postparams: dparams.postfilter_user_data,
    }
}

fn codec_cparams_context(cparams: &CParams, blocksize: i32) -> codecs::CodecCParamsContext {
    codecs::CodecCParamsContext {
        compcode: cparams.compcode,
        compcode_meta: cparams.compcode_meta,
        clevel: cparams.clevel,
        use_dict: if cparams.use_dict { 1 } else { 0 },
        typesize: cparams.typesize,
        blocksize,
        splitmode: cparams.splitmode,
        filters: cparams.filters,
        filters_meta: cparams.filters_meta,
        nthreads: cparams.nthreads,
        nchunk: cparams.nchunk,
        user_data: 0,
        instr_codec: cparams.instr_codec,
        codec_params: cparams.codec_params,
    }
}

fn codec_dparams_context(dparams: &DParams) -> codecs::CodecDParamsContext {
    codecs::CodecDParamsContext {
        nthreads: dparams.nthreads,
        typesize: dparams.typesize,
        nchunk: dparams.nchunk,
        user_data: 0,
    }
}

fn apply_context_env_to_cparams(mut cparams: CParams) -> Result<CParams, &'static str> {
    if let Ok(v) = std::env::var("BLOSC_CLEVEL") {
        let parsed = parse_c_strtol_prefix(&v);
        if parsed >= 0 {
            cparams.clevel = parsed.min(u8::MAX as i64) as u8;
        }
    }
    if let Ok(v) = std::env::var("BLOSC_TYPESIZE") {
        let parsed = parse_c_strtol_prefix(&v);
        if parsed > 0 && parsed <= i32::MAX as i64 {
            cparams.typesize = parsed as i32;
        }
    }
    if let Ok(v) = std::env::var("BLOSC_COMPRESSOR") {
        cparams.compcode = blosc_context_env_compressor_code(&v)?;
    }
    if let Ok(v) = std::env::var("BLOSC_SHUFFLE") {
        match v.as_str() {
            "NOSHUFFLE" => cparams.filters[BLOSC2_MAX_FILTERS - 1] = BLOSC_NOFILTER,
            "SHUFFLE" if cparams.typesize > 1 => {
                cparams.filters[BLOSC2_MAX_FILTERS - 1] = BLOSC_SHUFFLE;
            }
            "BITSHUFFLE" => cparams.filters[BLOSC2_MAX_FILTERS - 1] = BLOSC_BITSHUFFLE,
            _ => {}
        }
    }
    if let Ok(v) = std::env::var("BLOSC_DELTA") {
        match v.as_str() {
            "1" => cparams.filters[BLOSC2_MAX_FILTERS - 2] = BLOSC_DELTA,
            "0" => {}
            _ => {}
        }
    }
    if let Ok(v) = std::env::var("BLOSC_BLOCKSIZE") {
        let parsed = parse_c_strtol_prefix(&v);
        if parsed > 0 && parsed <= i32::MAX as i64 {
            cparams.blocksize = parsed as i32;
        }
    }
    if let Ok(v) = std::env::var("BLOSC_NTHREADS") {
        let parsed = parse_c_strtol_prefix(&v);
        let cast = parsed as i16;
        if cast > 0 {
            cparams.nthreads = cast;
        }
    }
    if let Ok(v) = std::env::var("BLOSC_SPLITMODE") {
        match v.as_str() {
            "ALWAYS" => cparams.splitmode = BLOSC_ALWAYS_SPLIT,
            "NEVER" => cparams.splitmode = BLOSC_NEVER_SPLIT,
            "AUTO" => cparams.splitmode = BLOSC_AUTO_SPLIT,
            "FORWARD_COMPAT" => cparams.splitmode = BLOSC_FORWARD_COMPAT_SPLIT,
            _ => {}
        }
    }
    let _ = std::env::var("BLOSC_NOLOCK");
    Ok(cparams)
}

fn apply_context_env_to_dparams(mut dparams: DParams) -> DParams {
    if let Ok(v) = std::env::var("BLOSC_NTHREADS") {
        let parsed = parse_c_strtol_prefix(&v);
        let cast = parsed as i16;
        if cast > 0 {
            dparams.nthreads = cast;
        }
    }
    let _ = std::env::var("BLOSC_NOLOCK");
    dparams
}

/// Lightweight context wrapper for C-Blosc2-style compression APIs.
#[derive(Debug, Clone)]
pub struct CContext {
    cparams: CParams,
}

impl CContext {
    /// Create a compression context holding a copy of `cparams`.
    pub fn new(cparams: CParams) -> Self {
        Self {
            cparams: apply_context_env_to_cparams(cparams).unwrap_or_else(|_| CParams {
                compcode: BLOSC_LAST_CODEC,
                ..CParams::default()
            }),
        }
    }

    /// Return the compression parameters associated with this context.
    pub fn cparams(&self) -> CParams {
        self.cparams.clone()
    }

    /// Compress `src` using this context's parameters.
    pub fn compress_chunk(&self, src: &[u8]) -> Result<Vec<u8>, &'static str> {
        compress_chunk(src, &self.cparams)
    }

    /// Backwards-compatible alias for [`CContext::compress_chunk`].
    pub fn compress(&self, src: &[u8]) -> Result<Vec<u8>, &'static str> {
        self.compress_chunk(src)
    }

    /// Compress `src` into caller-provided storage using this context's parameters.
    pub fn compress_chunk_into(&self, src: &[u8], dest: &mut [u8]) -> Result<usize, &'static str> {
        let compressed = compress_chunk_with_output_limit(src, &self.cparams, Some(dest.len()))?;
        if compressed.len() > dest.len() {
            return Err("Destination too small");
        }
        dest[..compressed.len()].copy_from_slice(&compressed);
        Ok(compressed.len())
    }

    /// Backwards-compatible alias for [`CContext::compress_chunk_into`].
    pub fn compress_into(&self, src: &[u8], dest: &mut [u8]) -> Result<usize, &'static str> {
        self.compress_chunk_into(src, dest)
    }

    /// Compress independent variable-length blocks using this context's parameters.
    pub fn compress_vl_blocks(&self, blocks: &[&[u8]]) -> Result<Vec<u8>, &'static str> {
        compress_vl_blocks(blocks, &self.cparams)
    }

    /// Backwards-compatible alias for [`CContext::compress_vl_blocks`].
    pub fn vlcompress(&self, blocks: &[&[u8]]) -> Result<Vec<u8>, &'static str> {
        self.compress_vl_blocks(blocks)
    }
}

impl From<CParams> for CContext {
    fn from(cparams: CParams) -> Self {
        Self::new(cparams)
    }
}

/// C-name alias for creating a compression context.
pub fn blosc2_create_cctx(cparams: CParams) -> Result<CContext, &'static str> {
    validate_cctx_create_filters(&cparams)?;
    let cparams = apply_context_env_to_cparams(cparams)?;
    let ctx = CContext { cparams };
    validate_cctx_create_filters(&ctx.cparams)?;
    Ok(ctx)
}

/// C-style status adapter for [`blosc2_create_cctx`].
pub fn blosc2_create_cctx_c(cparams: CParams) -> (i32, Option<CContext>) {
    match blosc2_create_cctx(cparams) {
        Ok(ctx) => (BLOSC2_ERROR_SUCCESS, Some(ctx)),
        Err(err) => (blosc2_error_code(err), None),
    }
}

fn validate_cctx_create_filters(cparams: &CParams) -> Result<(), &'static str> {
    for &filter in &cparams.filters {
        if let Some(err) = unsupported_global_filter_error(filter) {
            return Err(err);
        }
        if !is_cctx_create_filter_allowed(filter) {
            return Err("Unsupported filter");
        }
    }
    Ok(())
}

fn is_cctx_create_filter_allowed(filter: u8) -> bool {
    (filter <= BLOSC_FILTER_INT_TRUNC && is_structurally_known_filter(filter))
        || (BLOSC2_USER_REGISTERED_FILTERS_START..=BLOSC2_USER_REGISTERED_FILTERS_STOP)
            .contains(&filter)
}

/// C-style context compression adapter: returns bytes written, zero when the
/// destination is too small, or a negative `BLOSC2_ERROR_*` code on failure.
pub fn blosc2_compress_ctx(
    ctx: &CContext,
    src: &[u8],
    srcsize: i32,
    dest: &mut [u8],
    destsize: i32,
) -> i32 {
    if srcsize < 0 {
        return BLOSC2_ERROR_INVALID_PARAM;
    }
    if destsize < 0 {
        return BLOSC2_ERROR_MAX_BUFSIZE_EXCEEDED;
    }
    let srcsize = srcsize as usize;
    let destsize = destsize as usize;
    if srcsize > src.len() || destsize > dest.len() {
        return BLOSC2_ERROR_INVALID_PARAM;
    }
    if destsize < BLOSC2_MAX_OVERHEAD {
        return BLOSC2_ERROR_MAX_BUFSIZE_EXCEEDED;
    }
    match ctx.compress_chunk_into(&src[..srcsize], &mut dest[..destsize]) {
        Ok(value) => usize_to_c_return(value),
        Err("Destination too small") => 0,
        Err(err) => blosc2_error_code(err),
    }
}

/// C-style context VL-block compression adapter.
pub fn blosc2_vlcompress_ctx(
    ctx: &CContext,
    blocks: &[&[u8]],
    dest: &mut [u8],
    destsize: i32,
) -> i32 {
    if destsize < 0 {
        return BLOSC2_ERROR_MAX_BUFSIZE_EXCEEDED;
    }
    let destsize = destsize as usize;
    if destsize > dest.len() {
        return BLOSC2_ERROR_INVALID_PARAM;
    }
    if destsize < BLOSC2_MAX_OVERHEAD {
        return BLOSC2_ERROR_MAX_BUFSIZE_EXCEEDED;
    }
    match compress_vl_blocks_with_output_limit(blocks, &ctx.cparams, Some(destsize)) {
        Ok(compressed) => {
            if compressed.len() > destsize {
                return 0;
            }
            dest[..compressed.len()].copy_from_slice(&compressed);
            usize_to_c_return(compressed.len())
        }
        Err("Destination too small") => 0,
        Err(err) => blosc2_error_code(err),
    }
}

/// C-style VL-block compression adapter with explicit block count and sizes.
pub fn blosc2_vlcompress_ctx_c(
    ctx: &CContext,
    blocks: &[&[u8]],
    block_sizes: &[i32],
    nblocks: i32,
    dest: &mut [u8],
    destsize: i32,
) -> i32 {
    let nblocks = match checked_c_buffer_len(nblocks, blocks.len()) {
        Ok(nblocks) => nblocks,
        Err(code) => return code,
    };
    if nblocks == 0 {
        return BLOSC2_ERROR_INVALID_PARAM;
    }
    if block_sizes.len() < nblocks {
        return BLOSC2_ERROR_INVALID_PARAM;
    }
    let mut sized_blocks: Vec<Cow<'_, [u8]>> = Vec::with_capacity(nblocks);
    for (block, &declared_size) in blocks.iter().zip(block_sizes.iter()).take(nblocks) {
        let declared_size = match usize::try_from(declared_size) {
            Ok(size) => size,
            Err(_) => return BLOSC2_ERROR_INVALID_PARAM,
        };
        if declared_size == 0 {
            return BLOSC2_ERROR_INVALID_PARAM;
        }
        if declared_size > block.len() {
            return BLOSC2_ERROR_INVALID_PARAM;
        }
        sized_blocks.push(Cow::Borrowed(&block[..declared_size]));
    }
    let refs: Vec<&[u8]> = sized_blocks.iter().map(|block| block.as_ref()).collect();
    blosc2_vlcompress_ctx(ctx, &refs, dest, destsize)
}

/// Lightweight context wrapper for C-Blosc2-style decompression APIs.
#[derive(Debug, Clone)]
pub struct DContext {
    dparams: DParams,
    block_maskout: Arc<Mutex<Option<Vec<bool>>>>,
}

impl DContext {
    /// Create a decompression context holding a copy of `dparams`.
    pub fn new(dparams: DParams) -> Self {
        let mut dparams = apply_context_env_to_dparams(dparams);
        let block_maskout = dparams.block_maskout.take();
        Self {
            dparams,
            block_maskout: Arc::new(Mutex::new(block_maskout)),
        }
    }

    /// Return the decompression parameters associated with this context.
    pub fn dparams(&self) -> DParams {
        let mut dparams = self.dparams.clone();
        dparams.block_maskout = self
            .block_maskout
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        dparams
    }

    /// Set a one-shot block maskout for the next chunk decompression.
    ///
    /// A `true` entry skips the corresponding block and leaves that output
    /// range untouched. Decompression still returns the full chunk byte count,
    /// matching C-Blosc2.
    /// The mask is consumed by the next [`decompress`](Self::decompress) or
    /// [`decompress_into`](Self::decompress_into) call, matching C-Blosc2
    /// context semantics.
    pub fn set_maskout(&self, maskout: &[bool]) -> Result<(), &'static str> {
        let mut slot = self
            .block_maskout
            .lock()
            .map_err(|_| "Maskout mutex poisoned")?;
        *slot = Some(maskout.to_vec());
        Ok(())
    }

    fn one_shot_dparams(&self) -> DParams {
        let mut dparams = self.dparams.clone();
        dparams.block_maskout = self
            .block_maskout
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        dparams
    }

    /// Decompress `chunk` into a newly allocated buffer using this context's parameters.
    pub fn decompress_chunk(&self, chunk: &[u8]) -> Result<Vec<u8>, &'static str> {
        let dparams = self.one_shot_dparams();
        decompress_chunk_with_dparams(chunk, &dparams)
    }

    /// Backwards-compatible alias for [`DContext::decompress_chunk`].
    pub fn decompress(&self, chunk: &[u8]) -> Result<Vec<u8>, &'static str> {
        self.decompress_chunk(chunk)
    }

    /// Decompress `chunk` into `dest` using this context's parameters.
    pub fn decompress_chunk_into(
        &self,
        chunk: &[u8],
        dest: &mut [u8],
    ) -> Result<usize, &'static str> {
        let dparams = self.one_shot_dparams();
        decompress_chunk_into_with_dparams(chunk, dest, &dparams)
    }

    /// Backwards-compatible alias for [`DContext::decompress_chunk_into`].
    pub fn decompress_into(&self, chunk: &[u8], dest: &mut [u8]) -> Result<usize, &'static str> {
        self.decompress_chunk_into(chunk, dest)
    }

    /// Decompress every block in a variable-length-block chunk.
    pub fn decompress_vl_blocks(&self, chunk: &[u8]) -> Result<Vec<Vec<u8>>, &'static str> {
        let dparams = self.dparams();
        decompress_vl_blocks_with_dparams(chunk, &dparams)
    }

    /// Backwards-compatible alias for [`DContext::decompress_vl_blocks`].
    pub fn vldecompress(&self, chunk: &[u8]) -> Result<Vec<Vec<u8>>, &'static str> {
        self.decompress_vl_blocks(chunk)
    }

    /// Decompress one block from a variable-length-block chunk.
    pub fn decompress_vl_block(
        &self,
        chunk: &[u8],
        nblock: usize,
    ) -> Result<Vec<u8>, &'static str> {
        let dparams = self.dparams();
        decompress_vl_block_with_dparams(chunk, nblock, &dparams)
    }

    /// Backwards-compatible alias for [`DContext::decompress_vl_block`].
    pub fn vldecompress_block(&self, chunk: &[u8], nblock: usize) -> Result<Vec<u8>, &'static str> {
        self.decompress_vl_block(chunk, nblock)
    }

    /// Extract logical items from a chunk.
    pub fn get_items(
        &self,
        chunk: &[u8],
        start: usize,
        nitems: usize,
    ) -> Result<Vec<u8>, &'static str> {
        get_items_with_dparams(chunk, start, nitems, &self.dparams)
    }

    /// Backwards-compatible alias for [`DContext::get_items`].
    pub fn getitem(
        &self,
        chunk: &[u8],
        start: usize,
        nitems: usize,
    ) -> Result<Vec<u8>, &'static str> {
        self.get_items(chunk, start, nitems)
    }

    /// Extract logical items from a chunk into caller-provided storage.
    pub fn get_items_into(
        &self,
        chunk: &[u8],
        start: usize,
        nitems: usize,
        dest: &mut [u8],
    ) -> Result<usize, &'static str> {
        let byte_len = item_range_byte_len(chunk, start, nitems)?;
        if byte_len > dest.len() {
            return Err("Destination too small");
        }
        let items = self.get_items(chunk, start, nitems)?;
        dest[..items.len()].copy_from_slice(&items);
        Ok(items.len())
    }

    /// Backwards-compatible alias for [`DContext::get_items_into`].
    pub fn getitem_into(
        &self,
        chunk: &[u8],
        start: usize,
        nitems: usize,
        dest: &mut [u8],
    ) -> Result<usize, &'static str> {
        self.get_items_into(chunk, start, nitems, dest)
    }
}

impl From<DParams> for DContext {
    fn from(dparams: DParams) -> Self {
        Self::new(dparams)
    }
}

/// C-name alias for creating a decompression context.
pub fn blosc2_create_dctx(dparams: DParams) -> Result<DContext, &'static str> {
    let ctx = DContext::new(dparams);
    Ok(ctx)
}

/// C-style status adapter for [`blosc2_create_dctx`].
pub fn blosc2_create_dctx_c(dparams: DParams) -> (i32, Option<DContext>) {
    match blosc2_create_dctx(dparams) {
        Ok(ctx) => (BLOSC2_ERROR_SUCCESS, Some(ctx)),
        Err(err) => (blosc2_error_code(err), None),
    }
}

/// Rust contexts are owned values; this no-op consumes the context for
/// C-Blosc2 source-name parity with `blosc2_free_ctx`.
pub fn blosc2_free_ctx<T>(_ctx: T) {}

/// Nullable status-shaped context free adapter for C API parity.
pub fn blosc2_free_ctx_c<T>(_ctx: Option<T>) -> i32 {
    BLOSC2_ERROR_SUCCESS
}

/// C-name adapter for setting a one-shot decompression block maskout.
pub fn blosc2_set_maskout(ctx: &DContext, maskout: &[bool], nblocks: i32) -> i32 {
    let nblocks = match checked_c_buffer_len(nblocks, maskout.len()) {
        Ok(nblocks) => nblocks,
        Err(code) => return code,
    };
    match ctx.set_maskout(&maskout[..nblocks]) {
        Ok(()) => BLOSC2_ERROR_SUCCESS,
        Err(err) => blosc2_error_code(err),
    }
}

/// C-style context decompression adapter: returns bytes written or a negative
/// `BLOSC2_ERROR_*` code on failure.
pub fn blosc2_decompress_ctx(
    ctx: &DContext,
    src: &[u8],
    srcsize: i32,
    dest: &mut [u8],
    destsize: i32,
) -> i32 {
    let dparams = ctx.one_shot_dparams();
    let chunk = match checked_c_declared_chunk(src, srcsize) {
        Ok(chunk) => chunk,
        Err(code) => return code,
    };
    if destsize < 0 {
        return BLOSC2_ERROR_WRITE_BUFFER;
    }
    let destsize = destsize as usize;
    if destsize > dest.len() {
        return BLOSC2_ERROR_INVALID_PARAM;
    }
    match decompress_into_with_dparams(chunk, &mut dest[..destsize], &dparams) {
        Ok(written) => result_len_to_c(Ok(written)),
        Err(err) => blosc2_error_code(err),
    }
}

/// C-style context VL-block decompression adapter.
pub fn blosc2_vldecompress_ctx(
    ctx: &DContext,
    src: &[u8],
    srcsize: i32,
) -> (i32, Option<Vec<Vec<u8>>>) {
    let chunk = match checked_c_declared_chunk(src, srcsize) {
        Ok(chunk) => chunk,
        Err(code) => return (code, None),
    };
    match ctx.decompress_vl_blocks(chunk) {
        Ok(blocks) => (
            i32::try_from(blocks.len()).unwrap_or(BLOSC2_ERROR_INVALID_PARAM),
            Some(blocks),
        ),
        Err(err) => (blosc2_error_code(err), None),
    }
}

/// C-style VL-block decompression adapter with caller-owned output vectors.
pub fn blosc2_vldecompress_ctx_c(
    ctx: &DContext,
    src: &[u8],
    srcsize: i32,
    blocks: &mut [Vec<u8>],
    block_sizes: &mut [i32],
    nblocks: i32,
) -> i32 {
    if nblocks <= 0 {
        return BLOSC2_ERROR_INVALID_PARAM;
    }
    let nblocks = match checked_c_buffer_len(nblocks, blocks.len()) {
        Ok(nblocks) => nblocks,
        Err(code) => return code,
    };
    if block_sizes.len() < nblocks {
        return BLOSC2_ERROR_INVALID_PARAM;
    }
    let chunk = match checked_c_declared_chunk(src, srcsize) {
        Ok(chunk) => chunk,
        Err(code) => return code,
    };
    let header = match ChunkHeader::read(chunk) {
        Ok(header) => normalize_regular_header_blocksize(header),
        Err(err) => return cbuffer_header_error_code(err),
    };
    if let Err(err) = validate_header(&header, chunk.len()) {
        return blosc2_error_code(err);
    }
    if !header.vl_blocks() {
        return blosc2_error_code("Chunk does not use VL-blocks");
    }
    if let Err(err) = validate_vl_layout(chunk, &header) {
        return blosc2_error_code(err);
    }
    let count = header.blocksize as usize;
    if count > nblocks {
        return BLOSC2_ERROR_INVALID_PARAM;
    }
    let dparams = ctx.dparams();
    let maskout = match validated_block_maskout(&dparams, count) {
        Ok(maskout) => maskout,
        Err(err) => return blosc2_error_code(err),
    };
    for idx in 0..count {
        let block_size = match vl_block_uncompressed_size(chunk, idx) {
            Ok(size) => size,
            Err(err) => return blosc2_error_code(err),
        };
        block_sizes[idx] = i32::try_from(block_size).unwrap_or(BLOSC2_ERROR_2GB_LIMIT);
        if block_sizes[idx] < 0 {
            return block_sizes[idx];
        }
        blocks[idx].clear();
        blocks[idx].resize(block_size, 0);
        if block_is_masked(maskout, idx) {
            // C leaves malloc-backed masked block contents indeterminate.
            // Vec cannot safely expose uninitialized bytes, so this adapter
            // claims only size parity and returns deterministic zeroes.
            continue;
        }
        match decompress_vl_block_with_dparams(chunk, idx, &dparams) {
            Ok(block) => {
                blocks[idx] = block;
            }
            Err(err) => return blosc2_error_code(err),
        }
    }
    count as i32
}

/// C-name context VL-block single-block decompression adapter.
///
/// C-Blosc2 allocates storage for the decompressed block and returns it to the
/// caller via an out pointer. The Rust C-name adapter mirrors that ownership
/// shape with `Option<Vec<u8>>`; the status is the block size on success.
pub fn blosc2_vldecompress_block_ctx(
    ctx: &DContext,
    src: &[u8],
    srcsize: i32,
    nblock: usize,
) -> (i32, Option<Vec<u8>>) {
    let chunk = match checked_c_declared_chunk(src, srcsize) {
        Ok(chunk) => chunk,
        Err(code) => return (code, None),
    };
    let bsize = match vl_block_uncompressed_size(chunk, nblock) {
        Ok(size) => size,
        Err(err) => return (blosc2_error_code(err), None),
    };
    let dparams = ctx.dparams();
    let header = match ChunkHeader::read(chunk) {
        Ok(header) => normalize_regular_header_blocksize(header),
        Err(err) => return (cbuffer_header_error_code(err), None),
    };
    let maskout = match validated_block_maskout(&dparams, header.blocksize as usize) {
        Ok(maskout) => maskout,
        Err(err) => return (blosc2_error_code(err), None),
    };
    if block_is_masked(maskout, nblock) {
        return (usize_to_c_return(bsize), Some(vec![0; bsize]));
    }
    match decompress_vl_block_with_dparams(chunk, nblock, &dparams) {
        Ok(block) => (usize_to_c_return(block.len()), Some(block)),
        Err(err) => (blosc2_error_code(err), None),
    }
}

/// Caller-buffer VL-block single-block decompression adapter.
pub fn blosc2_vldecompress_block_ctx_into(
    ctx: &DContext,
    src: &[u8],
    srcsize: i32,
    nblock: usize,
    dest: &mut [u8],
) -> i32 {
    let chunk = match checked_c_declared_chunk(src, srcsize) {
        Ok(chunk) => chunk,
        Err(code) => return code,
    };
    let bsize = match vl_block_uncompressed_size(chunk, nblock) {
        Ok(size) => size,
        Err(err) => return blosc2_error_code(err),
    };
    let dparams = ctx.dparams();
    let header = match ChunkHeader::read(chunk) {
        Ok(header) => normalize_regular_header_blocksize(header),
        Err(err) => return cbuffer_header_error_code(err),
    };
    let maskout = match validated_block_maskout(&dparams, header.blocksize as usize) {
        Ok(maskout) => maskout,
        Err(err) => return blosc2_error_code(err),
    };
    if dest.len() < bsize {
        return BLOSC2_ERROR_WRITE_BUFFER;
    }
    if block_is_masked(maskout, nblock) {
        return usize_to_c_return(bsize);
    }
    match decompress_vl_block_with_dparams(chunk, nblock, &dparams) {
        Ok(block) => {
            dest[..block.len()].copy_from_slice(&block);
            usize_to_c_return(block.len())
        }
        Err(err) => blosc2_error_code(err),
    }
}

/// C-style signed VL-block single-block decompression adapter.
pub fn blosc2_vldecompress_block_ctx_c(
    ctx: &DContext,
    src: &[u8],
    srcsize: i32,
    nblock: i32,
    dest: &mut [u8],
    destsize: i32,
) -> i32 {
    if nblock < 0 {
        return BLOSC2_ERROR_INVALID_PARAM;
    }
    let destsize = match checked_c_buffer_len(destsize, dest.len()) {
        Ok(size) => size,
        Err(code) => return code,
    };
    blosc2_vldecompress_block_ctx_into(ctx, src, srcsize, nblock as usize, &mut dest[..destsize])
}

/// C-style compression-context parameter getter.
pub fn blosc2_ctx_get_cparams(ctx: &CContext) -> (i32, CParams) {
    (BLOSC2_ERROR_SUCCESS, ctx.cparams())
}

/// C-style decompression-context parameter getter.
pub fn blosc2_ctx_get_dparams(ctx: &DContext) -> (i32, DParams) {
    (BLOSC2_ERROR_SUCCESS, ctx.dparams())
}

/// Check if codec is "high compression ratio" — needs larger blocks.
fn is_hcr(compcode: u8) -> bool {
    matches!(compcode, BLOSC_LZ4HC | BLOSC_ZLIB | BLOSC_ZSTD)
}

/// Determine if blocks should be split into typesize streams.
fn should_split(
    compcode: u8,
    clevel: u8,
    splitmode: i32,
    typesize: i32,
    blocksize: i32,
    filter_flags: u8,
) -> bool {
    match splitmode {
        BLOSC_ALWAYS_SPLIT => return true,
        BLOSC_NEVER_SPLIT => return false,
        _ => {}
    }

    // MAX_STREAMS in c-blosc2/blosc/stune.h:24
    let max_streams = 16;
    let min_buffersize = BLOSC_MIN_BUFFERSIZE as i32;

    (compcode == BLOSC_BLOSCLZ || compcode == BLOSC_LZ4 || (compcode == BLOSC_ZSTD && clevel <= 5))
        && (filter_flags & BLOSC_DOSHUFFLE != 0)
        && typesize <= max_streams
        && (blocksize / typesize) >= min_buffersize
}

/// Compute the automatic blocksize (stune algorithm).
pub(crate) fn compute_blocksize(cparams: &CParams, nbytes: i32) -> i32 {
    let clevel = cparams.clevel as i32;
    let typesize = cparams.typesize;

    if nbytes < typesize {
        return 1;
    }

    if cparams.blocksize > 0 {
        let mut bs = cparams.blocksize;
        if bs > nbytes {
            bs = nbytes;
        }
        if bs > typesize {
            bs = bs / typesize * typesize;
        }
        return bs;
    }

    let filter_flags = compute_filter_flags(&cparams.filters);
    let do_split = should_split(
        cparams.compcode,
        cparams.clevel,
        cparams.splitmode,
        typesize,
        nbytes,
        filter_flags,
    );

    let mut blocksize = nbytes;

    if nbytes >= L1_CACHE as i32 {
        blocksize = L1_CACHE as i32;

        if is_hcr(cparams.compcode) {
            blocksize *= 2;
        }

        match clevel {
            0 => blocksize /= 4,
            1 => blocksize /= 2,
            2 => {}
            3 => blocksize *= 2,
            4 | 5 => blocksize *= 4,
            6..=8 => blocksize *= 8,
            9 => {
                blocksize *= 8;
                if is_hcr(cparams.compcode) {
                    blocksize *= 2;
                }
            }
            _ => {}
        }
    }

    if clevel > 0 && do_split {
        blocksize = match clevel {
            1..=3 => 32 * 1024,
            4..=6 => 64 * 1024,
            7 => 128 * 1024,
            8 => 256 * 1024,
            _ => 512 * 1024,
        };
        blocksize *= typesize;
        if blocksize > 4 * 1024 * 1024 {
            blocksize = 4 * 1024 * 1024;
        }
        if blocksize < 32 * 1024 {
            blocksize = 32 * 1024;
        }
    }

    if blocksize > nbytes {
        blocksize = nbytes;
    }
    if blocksize > typesize {
        blocksize = blocksize / typesize * typesize;
    }

    blocksize
}

/// Compute filter_flags from the filter array (for header compatibility).
fn compute_filter_flags(filters: &[u8; BLOSC2_MAX_FILTERS]) -> u8 {
    let mut flags = 0u8;
    for &f in filters.iter() {
        match f {
            BLOSC_SHUFFLE => flags |= BLOSC_DOSHUFFLE,
            BLOSC_BITSHUFFLE => flags |= BLOSC_DOBITSHUFFLE,
            BLOSC_DELTA => flags |= BLOSC_DODELTA,
            _ => {}
        }
    }
    flags
}

fn normalized_typesize(typesize: i32) -> i32 {
    if typesize as usize > BLOSC_MAX_TYPESIZE {
        1
    } else {
        typesize
    }
}

pub(crate) fn normalized_cparams(cparams: &CParams) -> CParams {
    let mut normalized = cparams.clone();
    normalized.typesize = normalized_typesize(normalized.typesize);
    if matches!(normalized.typesize, 1 | 2 | 4 | 8)
        && normalized.filters[..BLOSC2_MAX_FILTERS - 1]
            .iter()
            .all(|&filter| filter == BLOSC_NOFILTER)
        && normalized.filters[BLOSC2_MAX_FILTERS - 1] == BLOSC_DELTA
    {
        normalized.filters[BLOSC2_MAX_FILTERS - 1] = if normalized.typesize > 1 {
            BLOSC_SHUFFLE
        } else {
            BLOSC_NOFILTER
        };
        normalized.filters[BLOSC2_MAX_FILTERS - 2] = BLOSC_DELTA;
    }
    normalized
}

/// Validate caller-supplied compression parameters against the chunk size and codec/filter registry.
pub(crate) fn validate_cparams(cparams: &CParams, nbytes: usize) -> Result<(), &'static str> {
    if nbytes > BLOSC2_MAX_BUFFERSIZE as usize {
        return Err("Input too large");
    }
    if !(1..=BLOSC2_MAXTYPESIZE as i32).contains(&cparams.typesize) {
        return Err("Invalid typesize");
    }
    if cparams.clevel > 9 {
        return Err("Invalid compression level");
    }
    if cparams.blocksize < 0 {
        return Err("Invalid blocksize");
    }
    if cparams.nthreads < 1 {
        return Err("Invalid thread count");
    }
    if cparams.prefilter.is_some() {
        let effective_typesize = normalized_typesize(cparams.typesize);
        if cparams.prefilter_output_typesize != 0
            && cparams.prefilter_output_typesize != effective_typesize
        {
            return Err("Unsupported prefilter output typesize");
        }
    }
    if let Some(err) = unsupported_global_codec_error(cparams.compcode) {
        return Err(err);
    }
    if !supported_core_or_static_codec(cparams.compcode)
        && !codecs::is_registered_codec(cparams.compcode)
    {
        return Err("Unsupported codec");
    }
    if cparams.use_dict && !codecs::codec_supports_dict(cparams.compcode) {
        return Err("Dictionary compression is only supported for Zstd, LZ4, and LZ4HC");
    }
    for &filter in &cparams.filters {
        if let Some(err) = unsupported_global_filter_for_cparams(filter, cparams) {
            return Err(err);
        }
        if !is_structurally_known_filter(filter) {
            return Err("Unsupported filter");
        }
    }

    Ok(())
}

fn is_structurally_known_filter(filter: u8) -> bool {
    matches!(
        filter,
        BLOSC_NOFILTER | BLOSC_SHUFFLE | BLOSC_BITSHUFFLE | BLOSC_DELTA | BLOSC_TRUNC_PREC
    ) || filters::is_registered_filter(filter)
}

/// Validate that a chunk header is self-consistent and that the encoded sizes fit `chunk_len`.
fn validate_header(header: &ChunkHeader, chunk_len: usize) -> Result<(), &'static str> {
    let header_len = header.header_len();
    if chunk_len < header_len {
        return Err("Chunk too small for header");
    }
    if header.nbytes < 0 {
        return Err("Invalid negative nbytes");
    }
    if header.cbytes < 0 {
        return Err("Invalid negative cbytes");
    }
    if header.version > BLOSC2_VERSION_FORMAT && (header.blosc2_flags2 & !BLOSC2_VL_BLOCKS) != 0 {
        return Err("Unsupported chunk version");
    }

    let nbytes = header.nbytes as usize;
    let cbytes = header.cbytes as usize;
    if cbytes > chunk_len {
        return Err("Chunk truncated");
    }
    if cbytes < header_len {
        return Err("Invalid compressed size");
    }
    if nbytes > BLOSC2_MAX_BUFFERSIZE as usize {
        return Err("Invalid nbytes");
    }
    if header.vl_blocks() && header.special_type() != BLOSC2_NO_SPECIAL {
        return Err("VL-block chunks cannot use special values");
    }
    if header.special_type() == BLOSC2_NO_SPECIAL && !header.memcpyed() {
        if !matches!(
            header.compformat(),
            BLOSC_BLOSCLZ_FORMAT
                | BLOSC_LZ4_FORMAT
                | BLOSC_ZLIB_FORMAT
                | BLOSC_ZSTD_FORMAT
                | BLOSC_UDCODEC_FORMAT
        ) {
            return Err("Unsupported codec format");
        }
        if header.compformat() == BLOSC_UDCODEC_FORMAT
            && header.udcompcode != BLOSC_CODEC_NDLZ
            && !codecs::is_registered_codec(header.udcompcode)
        {
            return Err(
                unsupported_global_codec_error(header.udcompcode).unwrap_or("Unsupported codec")
            );
        }
        if header.use_dict() && !codecs::codec_supports_dict(header.compcode()) {
            return Err("Dictionary chunks are only supported for Zstd, LZ4, and LZ4HC");
        }
        if header.blosc2_flags & (BLOSC2_INSTR_CODEC | BLOSC2_LAZY_CHUNK) != 0 {
            return Err("Unsupported chunk flags");
        }
        if header.vl_blocks() {
            if header.version < BLOSC2_VERSION_FORMAT_VL_BLOCKS {
                return Err("Invalid VL-block chunk version");
            }
            if header.blocksize <= 0 {
                return Err("Invalid VL-block count");
            }
        }
    }
    if header.typesize == 0 || header.typesize as usize > BLOSC_MAX_TYPESIZE {
        return Err("Invalid typesize");
    }
    if header.blocksize <= 0 || header.blocksize as usize > BLOSC2_MAXBLOCKSIZE {
        return Err("Invalid blocksize");
    }
    if header.memcpyed() {
        let min_memcpy_len = header_len
            .checked_add(nbytes)
            .ok_or("Invalid memcpyed chunk size")?;
        if cbytes != min_memcpy_len {
            return Err("Invalid memcpyed chunk size");
        }
    }
    if header.special_type() == BLOSC2_NO_SPECIAL
        && !header.memcpyed()
        && !matches!(
            header.compcode(),
            BLOSC_BLOSCLZ | BLOSC_LZ4 | BLOSC_LZ4HC | BLOSC_ZLIB | BLOSC_ZSTD | BLOSC_CODEC_NDLZ
        )
        && !codecs::is_registered_codec(header.compcode())
    {
        return Err("Unsupported codec");
    }
    match header.special_type() {
        BLOSC2_SPECIAL_VALUE => {
            let value_size = cbytes
                .checked_sub(header_len)
                .ok_or("Invalid special value size")?;
            if value_size == 0
                || value_size > BLOSC2_MAXTYPESIZE
                || (nbytes != 0 && value_size > nbytes)
            {
                return Err("Invalid special value size");
            }
            if !nbytes.is_multiple_of(value_size) {
                return Err("Invalid special value nbytes");
            }
        }
        BLOSC2_SPECIAL_NAN => {
            if !nbytes.is_multiple_of(header.typesize as usize) {
                return Err("Invalid NaN special value size");
            }
        }
        BLOSC2_SPECIAL_ZERO | BLOSC2_SPECIAL_UNINIT => {
            if !header.use_dict() && cbytes < header_len {
                return Err("Invalid special chunk size");
            }
        }
        BLOSC2_NO_SPECIAL => {}
        _ => return Err("Unknown special value type"),
    }
    if nbytes == 0 {
        return Ok(());
    }
    if header.special_type() == BLOSC2_NO_SPECIAL && !header.memcpyed() {
        for &filter in &header.filters {
            if let Some(err) = unsupported_global_filter_error(filter) {
                return Err(err);
            }
            if !is_structurally_known_filter(filter) {
                return Err("Unsupported filter");
            }
        }
    }

    if header.vl_blocks() {
        let nblocks = header.blocksize as usize;
        let min_block_table_len = nblocks
            .checked_mul(4)
            .and_then(|len| header_len.checked_add(len))
            .ok_or("Invalid VL-block table size")?;
        if cbytes < min_block_table_len {
            return Err("Chunk too small for VL-block table");
        }
        if header.use_dict()
            && cbytes
                < min_block_table_len
                    .checked_add(4)
                    .ok_or("Invalid dictionary size")?
        {
            return Err("Chunk too small for dictionary size");
        }
        return Ok(());
    }

    let nblocks = nbytes.div_ceil(header.blocksize as usize);
    let min_block_table_len = nblocks
        .checked_mul(4)
        .and_then(|len| header_len.checked_add(len))
        .ok_or("Invalid block table size")?;
    if !header.memcpyed()
        && header.special_type() == BLOSC2_NO_SPECIAL
        && cbytes < min_block_table_len
    {
        return Err("Chunk too small for block table");
    }
    if header.use_dict()
        && header.special_type() == BLOSC2_NO_SPECIAL
        && !header.memcpyed()
        && cbytes
            < min_block_table_len
                .checked_add(4)
                .ok_or("Invalid dictionary size")?
    {
        return Err("Chunk too small for dictionary size");
    }

    Ok(())
}

fn validate_minimal_header(header: &ChunkHeader) -> Result<(), &'static str> {
    if header.typesize == 0 {
        return Err("Invalid typesize");
    }
    if header.nbytes < 0 {
        return Err("Invalid nbytes");
    }
    if header.blocksize <= 0 || header.blocksize as usize > BLOSC2_MAXBLOCKSIZE {
        return Err("Invalid blocksize");
    }
    if header.cbytes < BLOSC_MIN_HEADER_LENGTH as i32 {
        return Err("Invalid cbytes");
    }
    Ok(())
}

fn cbuffer_header_error_code(err: &str) -> i32 {
    match err {
        "Buffer too small for header" => BLOSC2_ERROR_READ_BUFFER,
        "Invalid cbytes" | "Invalid blocksize" | "Invalid typesize" => BLOSC2_ERROR_INVALID_HEADER,
        _ => blosc2_error_code(err),
    }
}

fn read_cbuffer_query_header(chunk: &[u8]) -> Result<ChunkHeader, &'static str> {
    if chunk.len() < BLOSC_MIN_HEADER_LENGTH {
        return Err("Buffer too small for header");
    }

    let header = ChunkHeader {
        version: chunk[BLOSC2_CHUNK_VERSION],
        versionlz: chunk[BLOSC2_CHUNK_VERSIONLZ],
        flags: chunk[BLOSC2_CHUNK_FLAGS],
        typesize: chunk[BLOSC2_CHUNK_TYPESIZE],
        nbytes: i32::from_le_bytes(
            chunk[BLOSC2_CHUNK_NBYTES..BLOSC2_CHUNK_NBYTES + 4]
                .try_into()
                .unwrap(),
        ),
        blocksize: i32::from_le_bytes(
            chunk[BLOSC2_CHUNK_BLOCKSIZE..BLOSC2_CHUNK_BLOCKSIZE + 4]
                .try_into()
                .unwrap(),
        ),
        cbytes: i32::from_le_bytes(
            chunk[BLOSC2_CHUNK_CBYTES..BLOSC2_CHUNK_CBYTES + 4]
                .try_into()
                .unwrap(),
        ),
        ..Default::default()
    };

    if header.cbytes < BLOSC_MIN_HEADER_LENGTH as i32 {
        return Err("Invalid cbytes");
    }
    if header.blocksize <= 0 || header.blocksize as usize > BLOSC2_MAXBLOCKSIZE {
        return Err("Invalid blocksize");
    }
    if header.typesize == 0 {
        return Err("Invalid typesize");
    }

    Ok(header)
}

/// Validate the VL-block offset table and stored uncompressed sizes before decoding.
fn validate_vl_layout(chunk: &[u8], header: &ChunkHeader) -> Result<(), &'static str> {
    if !header.vl_blocks() {
        return Ok(());
    }

    let nblocks = header.blocksize as usize;
    let header_len = header.header_len();
    let chunk_limit = header.cbytes as usize;
    let table_end = header_len
        .checked_add(
            nblocks
                .checked_mul(4)
                .ok_or("Invalid VL-block table size")?,
        )
        .ok_or("Invalid VL-block table size")?;
    if table_end > chunk_limit {
        return Err("Chunk too small for VL-block table");
    }

    let dict = embedded_codec_dictionary(chunk, header)?;
    let min_block_start = table_end
        .checked_add(dict.map_or(0, |dict| 4 + dict.len()))
        .ok_or("Invalid dictionary size")?;
    let mut total_nbytes = 0usize;
    let mut previous = min_block_start;

    for idx in 0..nblocks {
        let bstart_pos = header_len + idx * 4;
        let src_pos_i32 = i32::from_le_bytes(chunk[bstart_pos..bstart_pos + 4].try_into().unwrap());
        if src_pos_i32 < 0 {
            return Err("Invalid negative block offset");
        }
        let src_pos = src_pos_i32 as usize;
        if src_pos < min_block_start || (idx > 0 && src_pos <= previous) || src_pos > chunk_limit {
            return Err("Invalid VL-block offset");
        }
        previous = src_pos;

        let block_limit = compressed_block_limit(chunk, header, src_pos, nblocks)?;
        let size_end = src_pos
            .checked_add(4)
            .ok_or("Invalid VL-block size offset")?;
        if size_end > block_limit {
            return Err("Chunk truncated reading VL-block size");
        }
        let bsize = i32::from_le_bytes(chunk[src_pos..size_end].try_into().unwrap());
        if bsize <= 0 {
            return Err("Invalid VL-block size");
        }
        total_nbytes = total_nbytes
            .checked_add(bsize as usize)
            .ok_or("Invalid VL-block sizes")?;
    }

    if total_nbytes != header.nbytes as usize {
        return Err("VL-block sizes do not add up to chunk nbytes");
    }
    Ok(())
}

/// Validate the regular Blosc block offset table before block decode.
fn validate_block_layout(chunk: &[u8], header: &ChunkHeader) -> Result<(), &'static str> {
    if header.vl_blocks() || header.memcpyed() || header.special_type() != BLOSC2_NO_SPECIAL {
        return Ok(());
    }

    let nblocks = header.nblocks();
    let header_len = header.header_len();
    let chunk_limit = header.cbytes as usize;
    let table_end = header_len
        .checked_add(nblocks.checked_mul(4).ok_or("Invalid block table size")?)
        .ok_or("Invalid block table size")?;
    if table_end > chunk_limit {
        return Err("Chunk too small for block table");
    }

    let dict = embedded_codec_dictionary(chunk, header)?;
    let min_block_start = table_end
        .checked_add(dict.map_or(0, |dict| 4 + dict.len()))
        .ok_or("Invalid dictionary size")?;
    let mut starts = Vec::with_capacity(nblocks);
    for idx in 0..nblocks {
        let bstart_pos = header_len + idx * 4;
        let src_pos_i32 = i32::from_le_bytes(chunk[bstart_pos..bstart_pos + 4].try_into().unwrap());
        if src_pos_i32 < 0 {
            return Err("Invalid negative block offset");
        }
        let src_pos = src_pos_i32 as usize;
        if src_pos < min_block_start || src_pos > chunk_limit {
            return Err("Invalid block offset");
        }
        starts.push(src_pos);
    }
    starts.sort_unstable();
    if starts.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err("Duplicate block offset");
    }
    Ok(())
}

/// Number of independent sub-streams a block is split into for compression (`typesize` when splitting, otherwise `1`).
fn stream_count(dont_split: bool, is_leftover: bool, typesize: usize, bsize: usize) -> usize {
    if !dont_split
        && !is_leftover
        && typesize > 1
        && bsize >= typesize
        && bsize.is_multiple_of(typesize)
    {
        typesize
    } else {
        1
    }
}

/// Grow `buf` to at least `len` bytes, zero-filling new positions.
fn ensure_len(buf: &mut Vec<u8>, len: usize) {
    if len > buf.len() {
        buf.resize(len, 0);
    }
}

fn check_output_budget(len: usize, output_limit: Option<usize>) -> Result<(), &'static str> {
    if output_limit.is_some_and(|limit| len > limit) {
        Err("Destination too small")
    } else {
        Ok(())
    }
}

fn ensure_len_with_budget(
    buf: &mut Vec<u8>,
    len: usize,
    output_limit: Option<usize>,
) -> Result<(), &'static str> {
    check_output_budget(len, output_limit)?;
    ensure_len(buf, len);
    Ok(())
}

/// Size in bytes of a fully memcpy-fallback-stored block: the block payload plus a per-stream 4-byte length prefix.
fn stored_block_len(dont_split: bool, is_leftover: bool, typesize: usize, bsize: usize) -> usize {
    let nstreams = stream_count(dont_split, is_leftover, typesize, bsize);
    bsize + nstreams * 4
}

/// Compute the upper byte bound for the compressed-block payload starting at `src_pos`
/// by finding the next non-overlapping block start in the block-offset table.
fn compressed_block_limit(
    chunk: &[u8],
    header: &ChunkHeader,
    src_pos: usize,
    nblocks: usize,
) -> Result<usize, &'static str> {
    let header_len = header.header_len();
    let chunk_limit = header.cbytes as usize;
    let mut block_limit = chunk_limit;
    for idx in 0..nblocks {
        let bstart_pos = header_len + idx * 4;
        let bstart_end = bstart_pos
            .checked_add(4)
            .ok_or("Invalid block table offset")?;
        if bstart_end > chunk_limit {
            return Err("Chunk too small for bstarts");
        }
        let pos_i32 = i32::from_le_bytes(chunk[bstart_pos..bstart_end].try_into().unwrap());
        if pos_i32 < 0 {
            continue;
        }
        let pos = pos_i32 as usize;
        if pos > chunk_limit {
            return Err("Invalid block offset");
        }
        if pos > src_pos && pos < block_limit {
            block_limit = pos;
        }
    }
    Ok(block_limit)
}

fn filter_is_noop(filter: u8, filter_meta: u8, typesize: usize) -> bool {
    if filter == BLOSC_NOFILTER {
        return true;
    }
    if filter != BLOSC_SHUFFLE {
        return false;
    }
    let shuffle_typesize = if filter_meta == 0 {
        typesize
    } else {
        filter_meta as usize
    };
    shuffle_typesize <= 1
}

fn filters_effectively_noop(
    filters: &[u8; BLOSC2_MAX_FILTERS],
    filters_meta: &[u8; BLOSC2_MAX_FILTERS],
    typesize: usize,
) -> bool {
    filters
        .iter()
        .zip(filters_meta.iter())
        .all(|(&filter, &meta)| filter_is_noop(filter, meta, typesize))
}

/// Emit a memcpy chunk (header + raw payload) when the parameters allow and the budget is sufficient.
fn maybe_memcpy_fallback_for_budget(
    src: &[u8],
    cparams: &CParams,
    flags: u8,
    blocksize: usize,
    output_limit: Option<usize>,
) -> Option<Vec<u8>> {
    if output_limit.is_some_and(|limit| limit < BLOSC_EXTENDED_HEADER_LENGTH + src.len()) {
        return None;
    }
    if cparams.clevel != 0 && cparams.use_dict {
        return None;
    }
    prefiltered_memcpy_chunk_with_flags(src, cparams, blocksize, flags | BLOSC_MEMCPYED).ok()
}

pub(crate) fn memcpy_chunk(src: &[u8], cparams: &CParams, blocksize: usize) -> Vec<u8> {
    memcpy_chunk_with_flags(
        src,
        cparams,
        blocksize,
        BLOSC_DOSHUFFLE | BLOSC_DOBITSHUFFLE | BLOSC_MEMCPYED,
    )
}

fn memcpy_chunk_with_flags(src: &[u8], cparams: &CParams, blocksize: usize, flags: u8) -> Vec<u8> {
    let memcpy_cbytes = BLOSC_EXTENDED_HEADER_LENGTH + src.len();
    let mut memcpyed = vec![0u8; memcpy_cbytes];
    memcpyed[BLOSC_EXTENDED_HEADER_LENGTH..].copy_from_slice(src);

    let header = ChunkHeader {
        version: BLOSC2_VERSION_FORMAT_STABLE,
        versionlz: codec_version_for_header(cparams.compcode),
        flags,
        typesize: cparams.typesize as u8,
        nbytes: src.len() as i32,
        blocksize: blocksize as i32,
        cbytes: memcpy_cbytes as i32,
        filters: cparams.filters,
        filters_meta: cparams.filters_meta,
        udcompcode: udcompcode_for_header(cparams.compcode),
        compcode_meta: cparams.compcode_meta,
        blosc2_flags: 0,
        ..Default::default()
    };
    header
        .try_write(&mut memcpyed[..BLOSC_EXTENDED_HEADER_LENGTH])
        .expect("memcpyed chunk header must fit");
    memcpyed
}

fn special_chunk(
    special_type: u8,
    nbytes: usize,
    typesize: usize,
    repeated_value: Option<&[u8]>,
) -> Result<Vec<u8>, &'static str> {
    let cparams = CParams {
        typesize: typesize as i32,
        ..Default::default()
    };
    special_chunk_with_cparams_raw(special_type, nbytes, &cparams, repeated_value, false)
}

fn special_chunk_with_cparams(
    special_type: u8,
    nbytes: usize,
    cparams: &CParams,
    repeated_value: Option<&[u8]>,
) -> Result<Vec<u8>, &'static str> {
    special_chunk_with_cparams_raw(special_type, nbytes, cparams, repeated_value, true)
}

pub(crate) fn special_chunk_with_cparams_no_env(
    special_type: u8,
    nbytes: usize,
    cparams: &CParams,
    repeated_value: Option<&[u8]>,
) -> Result<Vec<u8>, &'static str> {
    special_chunk_with_cparams_raw(special_type, nbytes, cparams, repeated_value, false)
}

fn special_chunk_with_cparams_raw(
    special_type: u8,
    nbytes: usize,
    cparams: &CParams,
    repeated_value: Option<&[u8]>,
    apply_env: bool,
) -> Result<Vec<u8>, &'static str> {
    let raw_cparams = cparams.clone();
    if raw_cparams.typesize <= 0 {
        return Err("Invalid typesize");
    }
    let cparams = if apply_env {
        apply_context_env_to_cparams(cparams.clone())?
    } else {
        cparams.clone()
    };
    validate_special_cparams(&cparams, nbytes)?;
    if cparams.typesize <= 0 {
        return Err("Invalid typesize");
    }
    let repeat_value_size = if special_type == BLOSC2_SPECIAL_VALUE {
        raw_cparams.typesize as usize
    } else {
        0
    };
    let raw_item_size = if special_type == BLOSC2_SPECIAL_VALUE {
        repeat_value_size
    } else {
        raw_cparams.typesize as usize
    };
    if nbytes != 0 && !nbytes.is_multiple_of(raw_item_size) {
        return Err("Invalid special value nbytes");
    }
    let cparams = normalized_cparams(&cparams);
    let typesize = cparams.typesize as usize;
    if !(1..=BLOSC_MAX_TYPESIZE).contains(&typesize) {
        return Err("Invalid typesize");
    }
    if special_type == BLOSC2_SPECIAL_VALUE {
        let value = repeated_value.ok_or("Missing special value")?;
        if value.len() != repeat_value_size || (nbytes != 0 && value.len() > nbytes) {
            return Err("Invalid special value size");
        }
    }

    let mut cbytes = BLOSC_EXTENDED_HEADER_LENGTH;
    if let Some(value) = repeated_value {
        cbytes = cbytes
            .checked_add(value.len())
            .ok_or("Invalid special value size")?;
    }
    let blocksize = compute_blocksize(&cparams, nbytes as i32);
    let mut chunk = vec![0u8; cbytes];
    let header = ChunkHeader {
        version: BLOSC2_VERSION_FORMAT_STABLE,
        versionlz: BLOSC_BLOSCLZ_VERSION_FORMAT,
        flags: BLOSC_DOSHUFFLE | BLOSC_DOBITSHUFFLE,
        typesize: typesize as u8,
        nbytes: nbytes as i32,
        blocksize,
        cbytes: cbytes as i32,
        blosc2_flags: special_type << 4,
        ..Default::default()
    };
    header.try_write(&mut chunk)?;
    if let Some(value) = repeated_value {
        chunk[BLOSC_EXTENDED_HEADER_LENGTH..].copy_from_slice(value);
    }
    Ok(chunk)
}

fn validate_special_cparams(cparams: &CParams, nbytes: usize) -> Result<(), &'static str> {
    if nbytes > BLOSC2_MAX_BUFFERSIZE as usize {
        return Err("Input too large");
    }
    if !(1..=BLOSC2_MAXTYPESIZE as i32).contains(&cparams.typesize) {
        return Err("Invalid typesize");
    }
    if cparams.clevel > 9 {
        return Err("Invalid compression level");
    }
    if cparams.blocksize < 0 {
        return Err("Invalid blocksize");
    }
    if cparams.nthreads < 1 {
        return Err("Invalid thread count");
    }
    validate_cctx_create_filters(cparams)?;
    Ok(())
}

/// Build an extended-header chunk representing `nbytes` zero bytes.
pub fn blosc2_chunk_zeros(nbytes: usize, typesize: usize) -> Result<Vec<u8>, &'static str> {
    special_chunk(BLOSC2_SPECIAL_ZERO, nbytes, typesize, None)
}

/// Build a zero special chunk using C-Blosc2-style compression parameters.
pub fn blosc2_chunk_zeros_with_cparams(
    nbytes: usize,
    cparams: &CParams,
) -> Result<Vec<u8>, &'static str> {
    special_chunk_with_cparams(BLOSC2_SPECIAL_ZERO, nbytes, cparams, None)
}

/// C-style `blosc2_chunk_zeros` adapter.
pub fn blosc2_chunk_zeros_c(cparams: CParams, nbytes: i32, dest: &mut [u8], destsize: i32) -> i32 {
    special_chunk_c(BLOSC2_SPECIAL_ZERO, cparams, nbytes, dest, destsize, None)
}

/// Build an extended-header chunk representing floating-point NaNs.
pub fn blosc2_chunk_nans(nbytes: usize, typesize: usize) -> Result<Vec<u8>, &'static str> {
    special_chunk(BLOSC2_SPECIAL_NAN, nbytes, typesize, None)
}

/// Build a NaN special chunk using C-Blosc2-style compression parameters.
pub fn blosc2_chunk_nans_with_cparams(
    nbytes: usize,
    cparams: &CParams,
) -> Result<Vec<u8>, &'static str> {
    special_chunk_with_cparams(BLOSC2_SPECIAL_NAN, nbytes, cparams, None)
}

/// C-style `blosc2_chunk_nans` adapter.
pub fn blosc2_chunk_nans_c(cparams: CParams, nbytes: i32, dest: &mut [u8], destsize: i32) -> i32 {
    special_chunk_c(BLOSC2_SPECIAL_NAN, cparams, nbytes, dest, destsize, None)
}

/// Build an extended-header chunk representing `value` repeated to `nbytes`.
pub fn blosc2_chunk_repeatval(nbytes: usize, value: &[u8]) -> Result<Vec<u8>, &'static str> {
    special_chunk(BLOSC2_SPECIAL_VALUE, nbytes, value.len(), Some(value))
}

/// Build a repeat-value special chunk using C-Blosc2-style compression parameters.
pub fn blosc2_chunk_repeatval_with_cparams(
    nbytes: usize,
    value: &[u8],
    cparams: &CParams,
) -> Result<Vec<u8>, &'static str> {
    special_chunk_with_cparams(BLOSC2_SPECIAL_VALUE, nbytes, cparams, Some(value))
}

/// C-style `blosc2_chunk_repeatval` adapter.
pub fn blosc2_chunk_repeatval_c(
    cparams: CParams,
    nbytes: i32,
    dest: &mut [u8],
    destsize: i32,
    value: &[u8],
) -> i32 {
    special_chunk_c(
        BLOSC2_SPECIAL_VALUE,
        cparams,
        nbytes,
        dest,
        destsize,
        Some(value),
    )
}

/// Build an extended-header chunk representing uninitialized bytes.
///
/// Decompression returns zeroed bytes, matching this crate's deterministic
/// handling for Blosc2 uninitialized special chunks.
pub fn blosc2_chunk_uninit(nbytes: usize, typesize: usize) -> Result<Vec<u8>, &'static str> {
    special_chunk(BLOSC2_SPECIAL_UNINIT, nbytes, typesize, None)
}

/// Build an uninitialized special chunk using C-Blosc2-style compression parameters.
pub fn blosc2_chunk_uninit_with_cparams(
    nbytes: usize,
    cparams: &CParams,
) -> Result<Vec<u8>, &'static str> {
    special_chunk_with_cparams(BLOSC2_SPECIAL_UNINIT, nbytes, cparams, None)
}

/// C-style `blosc2_chunk_uninit` adapter.
pub fn blosc2_chunk_uninit_c(cparams: CParams, nbytes: i32, dest: &mut [u8], destsize: i32) -> i32 {
    special_chunk_c(BLOSC2_SPECIAL_UNINIT, cparams, nbytes, dest, destsize, None)
}

fn special_chunk_c(
    special_type: u8,
    cparams: CParams,
    nbytes: i32,
    dest: &mut [u8],
    destsize: i32,
    repeated_value: Option<&[u8]>,
) -> i32 {
    if nbytes < 0 {
        if cparams.typesize <= 0 {
            return BLOSC2_ERROR_DATA;
        }
        let raw_typesize = cparams.typesize as usize;
        let min_dest = BLOSC_EXTENDED_HEADER_LENGTH
            + if special_type == BLOSC2_SPECIAL_VALUE {
                raw_typesize
            } else {
                0
            };
        if destsize < i32::try_from(min_dest).unwrap_or(i32::MAX) {
            return BLOSC2_ERROR_DATA;
        }
        let destsize = destsize as usize;
        if destsize > dest.len() {
            return BLOSC2_ERROR_INVALID_PARAM;
        }
        let raw_item_size = if special_type == BLOSC2_SPECIAL_VALUE {
            raw_typesize
        } else {
            cparams.typesize as usize
        };
        if special_type != BLOSC2_SPECIAL_ZERO
            && (raw_item_size == 0 || nbytes % raw_item_size as i32 != 0)
        {
            return BLOSC2_ERROR_DATA;
        }

        let repeated_prefix;
        let repeated_value = if special_type == BLOSC2_SPECIAL_VALUE {
            let value = match repeated_value {
                Some(value) => value,
                None => return BLOSC2_ERROR_DATA,
            };
            if value.len() < raw_typesize {
                return BLOSC2_ERROR_DATA;
            }
            repeated_prefix = &value[..raw_typesize];
            Some(repeated_prefix)
        } else {
            None
        };

        let cparams = match apply_context_env_to_cparams(cparams) {
            Ok(cparams) => cparams,
            Err(err) => return blosc2_error_code(err),
        };
        if validate_cctx_create_filters(&cparams).is_err() {
            return BLOSC2_ERROR_NULL_POINTER;
        }
        let typesize = normalized_typesize(cparams.typesize);
        if typesize <= 0 || typesize as usize > BLOSC_MAX_TYPESIZE {
            return BLOSC2_ERROR_DATA;
        }
        let cbytes = min_dest;
        let header = ChunkHeader {
            version: BLOSC2_VERSION_FORMAT_STABLE,
            versionlz: BLOSC_BLOSCLZ_VERSION_FORMAT,
            flags: BLOSC_DOSHUFFLE | BLOSC_DOBITSHUFFLE,
            typesize: typesize as u8,
            nbytes,
            blocksize: 1,
            cbytes: cbytes as i32,
            blosc2_flags: special_type << 4,
            ..Default::default()
        };
        if let Err(err) = header.try_write(&mut dest[..cbytes]) {
            return blosc2_error_code(err);
        }
        if let Some(value) = repeated_value {
            dest[BLOSC_EXTENDED_HEADER_LENGTH..cbytes].copy_from_slice(value);
        }
        return cbytes as i32;
    }
    let min_dest = BLOSC_EXTENDED_HEADER_LENGTH
        + if special_type == BLOSC2_SPECIAL_VALUE {
            cparams.typesize.max(0) as usize
        } else {
            0
        };
    if destsize < i32::try_from(min_dest).unwrap_or(i32::MAX) {
        return BLOSC2_ERROR_DATA;
    }
    let destsize = destsize as usize;
    if destsize > dest.len() {
        return BLOSC2_ERROR_INVALID_PARAM;
    }

    let repeated_prefix;
    let repeated_value = if special_type == BLOSC2_SPECIAL_VALUE {
        let value = match repeated_value {
            Some(value) => value,
            None => return BLOSC2_ERROR_DATA,
        };
        let raw_typesize = cparams.typesize.max(0) as usize;
        if value.len() < raw_typesize {
            return BLOSC2_ERROR_DATA;
        }
        repeated_prefix = &value[..raw_typesize];
        Some(repeated_prefix)
    } else {
        repeated_value
    };

    let chunk =
        match special_chunk_with_cparams(special_type, nbytes as usize, &cparams, repeated_value) {
            Ok(chunk) => chunk,
            Err("Invalid special value nbytes" | "Invalid special value size") => {
                return BLOSC2_ERROR_DATA;
            }
            Err("Unsupported filter") => return BLOSC2_ERROR_NULL_POINTER,
            Err(err) => return blosc2_error_code(err),
        };
    if chunk.len() > destsize {
        return BLOSC2_ERROR_DATA;
    }
    dest[..chunk.len()].copy_from_slice(&chunk);
    usize_to_c_return(chunk.len())
}

fn prefiltered_memcpy_chunk(
    src: &[u8],
    cparams: &CParams,
    blocksize: usize,
) -> Result<Vec<u8>, &'static str> {
    prefiltered_memcpy_chunk_with_flags(
        src,
        cparams,
        blocksize,
        BLOSC_DOSHUFFLE | BLOSC_DOBITSHUFFLE | BLOSC_MEMCPYED,
    )
}

fn prefiltered_memcpy_chunk_with_flags(
    src: &[u8],
    cparams: &CParams,
    blocksize: usize,
    flags: u8,
) -> Result<Vec<u8>, &'static str> {
    let Some(_) = cparams.prefilter else {
        return Ok(memcpy_chunk_with_flags(src, cparams, blocksize, flags));
    };

    let mut filtered = vec![0u8; src.len()];
    let nblocks = src.len().div_ceil(blocksize);
    let mut scratch = Vec::new();
    for block_idx in 0..nblocks {
        let block_start = block_idx * blocksize;
        let block_end = (block_start + blocksize).min(src.len());
        let block = &src[block_start..block_end];
        let prefiltered = apply_prefilter(
            cparams,
            block,
            block_start,
            blocksize,
            &mut scratch,
            0,
            false,
        )?
        .expect("prefilter was checked above");
        if prefiltered.data.len() != block.len() {
            return Err("Prefilter output size mismatch");
        }
        filtered[block_start..block_end].copy_from_slice(prefiltered.data);
    }
    Ok(memcpy_chunk_with_flags(
        &filtered, cparams, blocksize, flags,
    ))
}

fn write_legacy_header(
    buf: &mut [u8],
    version: u8,
    versionlz: u8,
    flags: u8,
    typesize: u8,
    nbytes: i32,
    blocksize: i32,
    cbytes: i32,
) -> Result<(), &'static str> {
    if buf.len() < BLOSC_MIN_HEADER_LENGTH {
        return Err("Buffer too small for header");
    }

    buf[BLOSC2_CHUNK_VERSION] = version;
    buf[BLOSC2_CHUNK_VERSIONLZ] = versionlz;
    buf[BLOSC2_CHUNK_FLAGS] = flags;
    buf[BLOSC2_CHUNK_TYPESIZE] = typesize;
    buf[4..8].copy_from_slice(&nbytes.to_le_bytes());
    buf[8..12].copy_from_slice(&blocksize.to_le_bytes());
    buf[12..16].copy_from_slice(&cbytes.to_le_bytes());
    Ok(())
}

fn blosc1_compat_enabled() -> bool {
    std::env::var_os("BLOSC_BLOSC1_COMPAT").is_some() && std::env::var_os("BLOSC_NOLOCK").is_none()
}

fn legacy_memcpy_chunk(
    src: &[u8],
    cparams: &CParams,
    blocksize: usize,
) -> Result<Vec<u8>, &'static str> {
    let cbytes = BLOSC_MIN_HEADER_LENGTH
        .checked_add(src.len())
        .ok_or("Input too large")?;
    let nbytes_i32 = i32::try_from(src.len()).map_err(|_| "Input too large")?;
    let cbytes_i32 = i32::try_from(cbytes).map_err(|_| "Input too large")?;
    let blocksize_i32 = i32::try_from(blocksize.max(1)).map_err(|_| "Invalid blocksize")?;
    let mut out = vec![0u8; cbytes];
    write_legacy_header(
        &mut out[..BLOSC_MIN_HEADER_LENGTH],
        BLOSC2_VERSION_FORMAT_STABLE,
        codec_version_for_header(cparams.compcode),
        BLOSC_MEMCPYED,
        normalized_typesize(cparams.typesize) as u8,
        nbytes_i32,
        blocksize_i32,
        cbytes_i32,
    )?;
    out[BLOSC_MIN_HEADER_LENGTH..].copy_from_slice(src);
    Ok(out)
}

fn legacy_flags_from_header(header: &ChunkHeader) -> u8 {
    if header.memcpyed() {
        return BLOSC_MEMCPYED | (header.compformat() << 5);
    }

    let mut flags = compute_filter_flags(&header.filters);
    if header.dont_split() {
        flags |= BLOSC_DONT_SPLIT;
    }
    flags | (header.compformat() << 5)
}

fn legacy_zero_run_chunk(header: &ChunkHeader) -> Result<Vec<u8>, &'static str> {
    if header.nbytes < 0 || header.blocksize <= 0 || header.typesize == 0 {
        return Err("Invalid header");
    }
    let nbytes = header.nbytes as usize;
    let blocksize = header.blocksize as usize;
    let typesize = header.typesize as usize;
    let nblocks = header.nblocks();
    let table_len = nblocks.checked_mul(4).ok_or("Invalid block table size")?;
    let mut payload_len = 0usize;
    for block_idx in 0..nblocks {
        let block_start = block_idx
            .checked_mul(blocksize)
            .ok_or("Invalid block size")?;
        let bsize = (block_start + blocksize).min(nbytes) - block_start;
        let is_leftover = block_idx == nblocks - 1 && bsize < blocksize;
        let nstreams = stream_count(header.dont_split(), is_leftover, typesize, bsize);
        payload_len = payload_len
            .checked_add(nstreams.checked_mul(4).ok_or("Invalid stream count")?)
            .ok_or("Invalid chunk size")?;
    }

    let cbytes = BLOSC_MIN_HEADER_LENGTH
        .checked_add(table_len)
        .and_then(|len| len.checked_add(payload_len))
        .ok_or("Invalid chunk size")?;
    let cbytes_i32 = i32::try_from(cbytes).map_err(|_| "Input too large")?;
    let mut out = vec![0u8; cbytes];
    write_legacy_header(
        &mut out[..BLOSC_MIN_HEADER_LENGTH],
        header.version,
        header.versionlz,
        legacy_flags_from_header(header),
        header.typesize,
        header.nbytes,
        header.blocksize,
        cbytes_i32,
    )?;

    let mut output_pos = BLOSC_MIN_HEADER_LENGTH + table_len;
    for block_idx in 0..nblocks {
        let table_pos = BLOSC_MIN_HEADER_LENGTH + block_idx * 4;
        out[table_pos..table_pos + 4].copy_from_slice(&(output_pos as i32).to_le_bytes());

        let block_start = block_idx * blocksize;
        let bsize = (block_start + blocksize).min(nbytes) - block_start;
        let is_leftover = block_idx == nblocks - 1 && bsize < blocksize;
        let nstreams = stream_count(header.dont_split(), is_leftover, typesize, bsize);
        for _ in 0..nstreams {
            out[output_pos..output_pos + 4].copy_from_slice(&0i32.to_le_bytes());
            output_pos += 4;
        }
    }

    Ok(out)
}

fn convert_blosc1_compat_chunk_with_output_limit(
    chunk: &[u8],
    src: &[u8],
    cparams: &CParams,
    output_limit: Option<usize>,
) -> Result<Vec<u8>, &'static str> {
    let header = normalize_regular_header_blocksize(ChunkHeader::read(chunk)?);
    if !header.is_extended() {
        check_output_budget(chunk.len(), output_limit)?;
        return Ok(chunk.to_vec());
    }
    if header.cbytes < BLOSC_EXTENDED_HEADER_LENGTH as i32 || header.nbytes < 0 {
        return Err("Invalid chunk size");
    }
    let old_cbytes = header.cbytes as usize;
    if old_cbytes > chunk.len() {
        return Err("Chunk truncated");
    }
    if header.nbytes == 0 {
        check_output_budget(BLOSC_MIN_HEADER_LENGTH + src.len(), output_limit)?;
        return legacy_memcpy_chunk(src, cparams, header.blocksize.max(1) as usize);
    }
    if header.vl_blocks() || header.use_dict() || header.compformat() == BLOSC_UDCODEC_FORMAT {
        return Err("Cannot encode Blosc1 compatibility header");
    }
    if header.special_type() != BLOSC2_NO_SPECIAL {
        if header.special_type() == BLOSC2_SPECIAL_ZERO {
            let chunk = legacy_zero_run_chunk(&header)?;
            check_output_budget(chunk.len(), output_limit)?;
            return Ok(chunk);
        }
        check_output_budget(BLOSC_MIN_HEADER_LENGTH + src.len(), output_limit)?;
        return legacy_memcpy_chunk(src, cparams, header.blocksize.max(1) as usize);
    }

    if header.memcpyed() {
        check_output_budget(BLOSC_MIN_HEADER_LENGTH + src.len(), output_limit)?;
        return legacy_memcpy_chunk(src, cparams, header.blocksize.max(1) as usize);
    }

    let new_cbytes = old_cbytes
        .checked_sub(BLOSC_EXTENDED_HEADER_LENGTH - BLOSC_MIN_HEADER_LENGTH)
        .ok_or("Invalid chunk size")?;
    check_output_budget(new_cbytes, output_limit)?;
    let new_cbytes_i32 = i32::try_from(new_cbytes).map_err(|_| "Input too large")?;
    let nblocks = header.nblocks();
    let table_len = nblocks.checked_mul(4).ok_or("Invalid block table size")?;
    let old_table_start = BLOSC_EXTENDED_HEADER_LENGTH;
    let old_payload_start = old_table_start
        .checked_add(table_len)
        .ok_or("Invalid block table size")?;
    let new_payload_start = BLOSC_MIN_HEADER_LENGTH
        .checked_add(table_len)
        .ok_or("Invalid block table size")?;
    if old_payload_start > old_cbytes || new_payload_start > new_cbytes {
        return Err("Chunk too small for block table");
    }

    let mut out = vec![0u8; new_cbytes];
    write_legacy_header(
        &mut out[..BLOSC_MIN_HEADER_LENGTH],
        header.version,
        header.versionlz,
        legacy_flags_from_header(&header),
        header.typesize,
        header.nbytes,
        header.blocksize,
        new_cbytes_i32,
    )?;

    for block_idx in 0..nblocks {
        let old_offset = old_table_start + block_idx * 4;
        let old_bstart = i32::from_le_bytes(chunk[old_offset..old_offset + 4].try_into().unwrap());
        let new_bstart = old_bstart
            .checked_sub((BLOSC_EXTENDED_HEADER_LENGTH - BLOSC_MIN_HEADER_LENGTH) as i32)
            .ok_or("Invalid block offset")?;
        if new_bstart < new_payload_start as i32 {
            return Err("Invalid block offset");
        }
        let new_offset = BLOSC_MIN_HEADER_LENGTH + block_idx * 4;
        out[new_offset..new_offset + 4].copy_from_slice(&new_bstart.to_le_bytes());
    }

    out[new_payload_start..].copy_from_slice(&chunk[old_payload_start..old_cbytes]);
    Ok(out)
}

fn apply_postfilter_to_blocks(
    dparams: &DParams,
    dest: &mut [u8],
    nbytes: usize,
    blocksize: usize,
    tid: i32,
) -> Result<(), &'static str> {
    if dparams.postfilter.is_none() {
        return Ok(());
    }
    if blocksize == 0 {
        return Err("Invalid blocksize");
    }
    let nblocks = nbytes.div_ceil(blocksize);
    if dparams.nthreads > 1 && nblocks > 1 {
        let threads = effective_nthreads(dparams.nthreads, nblocks);
        let next_block = AtomicUsize::new(0);
        let dest_addr = dest.as_mut_ptr() as usize;
        let first_err = Mutex::new(None::<&'static str>);
        with_thread_pool(threads, || {
            rayon::scope(|scope| {
                for _ in 0..threads as usize {
                    let next_block = &next_block;
                    let first_err = &first_err;
                    scope.spawn(move |_| loop {
                        let block_idx = next_block.fetch_add(1, Ordering::Relaxed);
                        if block_idx >= nblocks {
                            break;
                        }
                        let block_start = block_idx * blocksize;
                        let block_end = (block_start + blocksize).min(nbytes);
                        let bsize = block_end - block_start;
                        let block = unsafe {
                            std::slice::from_raw_parts_mut(
                                (dest_addr as *mut u8).add(block_start),
                                bsize,
                            )
                        };
                        let input = block.to_vec();
                        let tid = rayon::current_thread_index().unwrap_or(0) as i32;
                        if let Err(err) =
                            apply_postfilter(dparams, &input, block, block_start, block_idx, tid)
                        {
                            let mut slot = first_err.lock().unwrap();
                            if slot.is_none() {
                                *slot = Some(err);
                            }
                            break;
                        }
                    });
                }
            });
        });
        if let Some(err) = *first_err.lock().unwrap() {
            return Err(err);
        }
        return Ok(());
    }
    for block_idx in 0..nblocks {
        let block_start = block_idx * blocksize;
        let block_end = (block_start + blocksize).min(nbytes);
        let input = dest[block_start..block_end].to_vec();
        apply_postfilter(
            dparams,
            &input,
            &mut dest[block_start..block_end],
            block_start,
            block_idx,
            tid,
        )?;
    }
    Ok(())
}

/// Codec code as stored in the extended header's `udcompcode` field.
fn udcompcode_for_header(compcode: u8) -> u8 {
    compcode
}

/// Detect if all bytes in a block are the same value (run detection).
/// Uses 8-byte comparison for fast early exit.
#[inline]
fn get_run(data: &[u8]) -> Option<u8> {
    if data.is_empty() {
        return None;
    }
    let val = data[0];

    // Quick check: first and last bytes must match
    if data.len() > 1 && data[data.len() - 1] != val {
        return None;
    }

    // 8-byte comparison for bulk of the data
    let val8 = u64::from_ne_bytes([val; 8]);
    let mut i = 0;
    while i + 8 <= data.len() {
        let chunk = u64::from_ne_bytes(data[i..i + 8].try_into().unwrap());
        if chunk != val8 {
            return None;
        }
        i += 8;
    }

    // Check remaining bytes
    while i < data.len() {
        if data[i] != val {
            return None;
        }
        i += 1;
    }

    Some(val)
}

/// Apply the forward filter pipeline (with optional prefilter) and codec to a single block,
/// returning the encoded block bytes, whether every produced stream is an all-zero run,
/// and whether any stream had to be stored literally.
#[allow(clippy::too_many_arguments)]
fn compress_block_with_scratch(
    src: &[u8],
    block_data: &[u8],
    block_start: usize,
    blocksize: usize,
    is_leftover: bool,
    cparams: &CParams,
    dont_split: bool,
    typesize: usize,
    buf1: &mut Vec<u8>,
    buf2: &mut Vec<u8>,
    compress_buf: &mut Vec<u8>,
    prefilter_buf: &mut Vec<u8>,
    tid: i32,
    block_output_limit: Option<usize>,
) -> Result<(Vec<u8>, bool, bool), &'static str> {
    let mut skip_filters = false;
    let mut force_zero_run = false;
    let block_data = if let Some(filtered) = apply_prefilter(
        cparams,
        block_data,
        block_start,
        blocksize,
        prefilter_buf,
        tid,
        true,
    )? {
        skip_filters = filtered.skip_filters;
        force_zero_run = filtered.force_zero_run;
        filtered.data
    } else {
        block_data
    };
    let bsize = block_data.len();
    let filters_are_noop =
        filters_effectively_noop(&cparams.filters, &cparams.filters_meta, typesize);
    if filters_are_noop || skip_filters {
        let zero_storage;
        let block_data = if force_zero_run {
            zero_storage = vec![0u8; bsize];
            &zero_storage[..]
        } else {
            block_data
        };
        return compress_pre_filtered_block_with_scratch(
            block_data,
            src.as_ptr(),
            cparams,
            dont_split,
            typesize,
            is_leftover,
            None,
            block_start,
            blocksize,
            (block_start / blocksize) as i32,
            compress_buf,
            block_output_limit,
        );
    }

    if let Some(shuffle_typesize) =
        single_shuffle_filter(&cparams.filters, &cparams.filters_meta, typesize)
    {
        ensure_scratch_len_uninit(buf1, bsize);
        filters::shuffle(shuffle_typesize, block_data, &mut buf1[..bsize]);
        if force_zero_run {
            buf1[..bsize].fill(0);
        }
        return compress_pre_filtered_block_with_scratch(
            &buf1[..bsize],
            src.as_ptr(),
            cparams,
            dont_split,
            typesize,
            is_leftover,
            None,
            block_start,
            blocksize,
            (block_start / blocksize) as i32,
            compress_buf,
            block_output_limit,
        );
    }

    if buf1.len() < bsize {
        buf1.resize(bsize, 0);
    }
    if buf2.len() < bsize {
        buf2.resize(bsize, 0);
    }

    // Apply forward filter pipeline
    let delta_ref_storage = if block_start == 0 {
        None
    } else {
        delta_reference_block(src, cparams, blocksize, tid)?
    };
    let filter_cparams = filter_cparams_context(cparams, blocksize as i32);
    let filtered_buf = filters::apply_filter_pipeline_for_compression_with_context(
        block_data,
        &mut buf1[..bsize],
        &mut buf2[..bsize],
        &cparams.filters,
        &cparams.filters_meta,
        typesize,
        block_start,
        delta_ref_storage.as_deref(),
        Some(filters::FilterPipelineContext {
            cparams: Some(&filter_cparams),
            dparams: None,
            chunk: filters::FilterChunkContext {
                schunk: cparams.schunk,
                nchunk: cparams.nchunk,
                nblock: (block_start / blocksize) as i32,
                block_offset: block_start,
                blocksize,
                bsize,
            },
            b2nd_metalayer: cparams.b2nd_metalayer.as_deref(),
            user_data: cparams.prefilter_user_data,
        }),
    );
    if filtered_buf == 0 {
        return Err("Filter pipeline failed");
    }

    let filtered = if filtered_buf == 1 {
        &buf1[..bsize]
    } else {
        &buf2[..bsize]
    };
    let zero_storage;
    let filtered = if force_zero_run {
        zero_storage = vec![0u8; bsize];
        &zero_storage[..]
    } else {
        filtered
    };

    compress_pre_filtered_block_with_scratch(
        filtered,
        src.as_ptr(),
        cparams,
        dont_split,
        typesize,
        is_leftover,
        None,
        block_start,
        blocksize,
        (block_start / blocksize) as i32,
        compress_buf,
        block_output_limit,
    )
}

/// If the filter pipeline reduces to a single shuffle filter, return its typesize;
/// otherwise return `None`. Enables the fast shuffle-only fast path.
fn single_shuffle_filter(
    filters: &[u8; BLOSC2_MAX_FILTERS],
    filters_meta: &[u8; BLOSC2_MAX_FILTERS],
    typesize: usize,
) -> Option<usize> {
    let mut shuffle_typesize = None;
    for (idx, &filter) in filters.iter().enumerate() {
        if filter == BLOSC_NOFILTER {
            continue;
        }
        if filter != BLOSC_SHUFFLE || shuffle_typesize.is_some() {
            return None;
        }
        let ts = if filters_meta[idx] == 0 {
            typesize
        } else {
            filters_meta[idx] as usize
        };
        if ts <= 1 {
            return None;
        }
        shuffle_typesize = Some(ts);
    }
    shuffle_typesize
}

fn delta_filter_slot(filters: &[u8; BLOSC2_MAX_FILTERS]) -> Option<usize> {
    filters.iter().position(|&filter| filter == BLOSC_DELTA)
}

fn active_filter_count(filters: &[u8; BLOSC2_MAX_FILTERS]) -> usize {
    filters
        .iter()
        .filter(|&&filter| filter != BLOSC_NOFILTER)
        .count()
}

fn c_source_write_active_ordinal(cparams: &CParams) -> Option<usize> {
    if cparams.prefilter.is_some() {
        return None;
    }

    let active = active_filter_count(&cparams.filters);
    if active == 0 {
        return None;
    }

    // C's apply_filter_pipeline_for_compression rotates three destinations: dest, tmp, and the
    // original source pointer.
    let prefilter_rotation = 0usize;
    (1..=active)
        .rev()
        .find(|ordinal| (ordinal + prefilter_rotation) % 3 == 0)
}

fn c_forward_pipeline_writes_source(cparams: &CParams) -> bool {
    delta_filter_slot(&cparams.filters).is_some()
        && c_source_write_active_ordinal(cparams).is_some()
}

fn c_raw_stream_decode_needs_memcpy_fallback(cparams: &CParams) -> bool {
    let Some(delta_slot) = delta_filter_slot(&cparams.filters) else {
        return false;
    };
    let active = active_filter_count(&cparams.filters);
    active > 1
        && cparams.filters[delta_slot + 1..]
            .iter()
            .all(|&filter| filter == BLOSC_NOFILTER)
}

fn filter_prefix_through_active_ordinal(
    filters: &[u8; BLOSC2_MAX_FILTERS],
    ordinal: usize,
) -> [u8; BLOSC2_MAX_FILTERS] {
    let mut prefix = [BLOSC_NOFILTER; BLOSC2_MAX_FILTERS];
    let mut active = 0usize;
    for (idx, &filter) in filters.iter().enumerate() {
        if filter == BLOSC_NOFILTER {
            continue;
        }
        active += 1;
        if active <= ordinal {
            prefix[idx] = filter;
        } else {
            break;
        }
    }
    prefix
}

fn delta_reference_block(
    src: &[u8],
    cparams: &CParams,
    _blocksize: usize,
    _tid: i32,
) -> Result<Option<Vec<u8>>, &'static str> {
    if delta_filter_slot(&cparams.filters).is_none() {
        return Ok(None);
    }
    Ok(Some(src.to_vec()))
}

struct PrefilteredBlock<'a> {
    data: &'a [u8],
    skip_filters: bool,
    force_zero_run: bool,
}

/// Invoke the optional prefilter for one block.
/// Returns `Ok(Some(transformed))` when a prefilter ran, or `Ok(None)` when no prefilter is configured.
fn apply_prefilter<'a>(
    cparams: &CParams,
    block: &'a [u8],
    block_start: usize,
    blocksize: usize,
    scratch: &'a mut Vec<u8>,
    tid: i32,
    simulate_disposable_zero: bool,
) -> Result<Option<PrefilteredBlock<'a>>, &'static str> {
    let Some(prefilter) = cparams.prefilter else {
        return Ok(None);
    };

    let output_typesize = if cparams.prefilter_output_typesize > 0 {
        cparams.prefilter_output_typesize
    } else {
        cparams.typesize
    };
    let output_size = if output_typesize == cparams.typesize {
        block.len()
    } else {
        let nelems = block.len() / (cparams.typesize as usize);
        nelems
            .checked_mul(output_typesize as usize)
            .ok_or("Prefilter output size overflow")?
    };
    if output_size > i32::MAX as usize {
        return Err("Prefilter output size overflow");
    }
    scratch.resize(output_size, 0);
    if !cparams.prefilter_output_is_disposable {
        scratch[..output_size].fill(0);
    }

    let mut params = PrefilterParams {
        user_data: cparams.prefilter_user_data,
        input: block,
        output: &mut scratch[..output_size],
        output_size: output_size as i32,
        output_typesize,
        output_offset: block_start as i32,
        nchunk: cparams.nchunk,
        nblock: (block_start / blocksize) as i32,
        tid,
        output_is_disposable: cparams.prefilter_output_is_disposable,
    };
    let rc = prefilter(&mut params);
    if rc != 0 && !cparams.prefilter_output_is_disposable {
        return Err("Execution of prefilter function failed");
    }
    let discard_disposable = cparams.prefilter_output_is_disposable && rc != 0;
    if discard_disposable {
        scratch[..output_size].fill(0);
    }
    Ok(Some(PrefilteredBlock {
        data: &scratch[..output_size],
        skip_filters: discard_disposable,
        force_zero_run: cparams.prefilter_output_is_disposable
            && (discard_disposable || simulate_disposable_zero),
    }))
}

/// Invoke the optional postfilter for one block.
/// When no postfilter is configured, copies `input` to `output` (or is a no-op if they alias).
fn apply_postfilter(
    dparams: &DParams,
    input: &[u8],
    output: &mut [u8],
    block_start: usize,
    block_idx: usize,
    tid: i32,
) -> Result<(), &'static str> {
    let Some(postfilter) = dparams.postfilter else {
        if output.len() != input.len() {
            return Err("Postfilter input/output size mismatch");
        }
        if !std::ptr::eq(input.as_ptr(), output.as_ptr()) {
            output.copy_from_slice(input);
        }
        return Ok(());
    };

    let mut params = PostfilterParams {
        user_data: dparams.postfilter_user_data,
        input,
        output,
        size: input.len() as i32,
        typesize: dparams.typesize,
        offset: block_start as i32,
        nchunk: dparams.nchunk,
        nblock: block_idx as i32,
        tid,
    };
    if postfilter(&mut params) != 0 {
        return Err("Execution of postfilter function failed");
    }
    Ok(())
}

/// Encode an already-filtered block, splitting into per-typesize streams when requested.
///
/// Per-stream runs of constant bytes are emitted as the special `cbytes == 0` or negative
/// run-of-`val` form; otherwise the codec output (or a literal-stored stream when it does
/// not shrink) is written.
fn compress_pre_filtered_block_with_scratch(
    filtered: &[u8],
    chunk_source: *const u8,
    cparams: &CParams,
    dont_split: bool,
    typesize: usize,
    is_leftover: bool,
    dict: Option<&[u8]>,
    block_start: usize,
    blocksize: usize,
    nblock: i32,
    _compressed: &mut Vec<u8>,
    output_limit: Option<usize>,
) -> Result<(Vec<u8>, bool, bool), &'static str> {
    #[inline(always)]
    unsafe fn push_bytes(dst: &mut Vec<u8>, pos: &mut usize, src: &[u8]) {
        std::ptr::copy_nonoverlapping(src.as_ptr(), dst.as_mut_ptr().add(*pos), src.len());
        *pos += src.len();
    }

    #[inline(always)]
    unsafe fn push_i32(dst: &mut Vec<u8>, pos: &mut usize, value: i32) {
        push_bytes(dst, pos, &value.to_le_bytes());
    }

    let bsize = filtered.len();
    let nstreams = stream_count(dont_split, is_leftover, typesize, bsize);
    let neblock = bsize / nstreams;

    let mut result = Vec::with_capacity(stored_block_len(dont_split, is_leftover, typesize, bsize));
    let mut result_len = 0usize;
    let mut all_zero_runs = true;
    let mut any_literal_stream = false;
    let max_out = neblock;

    for stream_idx in 0..nstreams {
        let stream_start = stream_idx * neblock;
        let stream_data = &filtered[stream_start..stream_start + neblock];

        if let Some(val) = get_run(stream_data) {
            if val == 0 {
                check_output_budget(result_len + 4, output_limit)?;
                unsafe { push_i32(&mut result, &mut result_len, 0) };
            } else {
                all_zero_runs = false;
                check_output_budget(result_len + 5, output_limit)?;
                unsafe {
                    push_i32(&mut result, &mut result_len, -(val as i32));
                    *result.as_mut_ptr().add(result_len) = 0x01;
                }
                result_len += 1;
            }
            continue;
        }

        all_zero_runs = false;

        let header_pos = result_len;
        let payload_pos = header_pos + 4;
        ensure_scratch_len_uninit(&mut result, payload_pos + max_out);

        let cparams_context = codec_cparams_context(cparams, blocksize as i32);
        let codec_context = codecs::CodecCallbackContext {
            compcode: cparams.compcode,
            complib: None,
            meta: cparams.compcode_meta,
            clevel: cparams.clevel,
            cparams: Some(&cparams_context),
            dparams: None,
            chunk: codecs::CodecChunkContext {
                schunk: cparams.schunk,
                nchunk: cparams.nchunk,
                nblock,
                chunk_source: chunk_source as usize,
                block_offset: block_start,
                blocksize,
                bsize,
            },
            b2nd_metalayer: cparams.b2nd_metalayer.as_deref(),
            user_data: cparams.prefilter_user_data,
        };
        let cbytes = match dict {
            Some(dict) => codecs::compress_block_with_dict(
                cparams.compcode,
                cparams.clevel,
                stream_data,
                &mut result[payload_pos..payload_pos + max_out],
                dict,
            ),
            None => codecs::compress_block_with_context(
                cparams.compcode,
                cparams.clevel,
                cparams.compcode_meta,
                stream_data,
                &mut result[payload_pos..payload_pos + max_out],
                Some(codec_context),
            ),
        };

        if cbytes < 0 {
            return Err("Codec compression failed");
        }
        if cbytes == 0 || cbytes as usize >= neblock {
            any_literal_stream = true;
            check_output_budget(payload_pos + neblock, output_limit)?;
            unsafe {
                result.set_len(payload_pos + neblock);
                std::ptr::copy_nonoverlapping(
                    stream_data.as_ptr(),
                    result.as_mut_ptr().add(payload_pos),
                    neblock,
                );
                std::ptr::copy_nonoverlapping(
                    (neblock as i32).to_le_bytes().as_ptr(),
                    result.as_mut_ptr().add(header_pos),
                    4,
                );
            }
            result_len = payload_pos + neblock;
        } else {
            check_output_budget(payload_pos + cbytes as usize, output_limit)?;
            unsafe {
                result.set_len(payload_pos + cbytes as usize);
                std::ptr::copy_nonoverlapping(
                    cbytes.to_le_bytes().as_ptr(),
                    result.as_mut_ptr().add(header_pos),
                    4,
                );
            }
            result_len = payload_pos + cbytes as usize;
        }
    }

    unsafe {
        result.set_len(result_len);
    }
    Ok((result, all_zero_runs, any_literal_stream))
}

/// Run the forward filter pipeline over every block of `src`, returning the filtered
/// payloads. Used to gather training samples for dictionary construction.
fn filtered_blocks_for_dict(
    src: &[u8],
    cparams: &CParams,
    blocksize: usize,
    nblocks: usize,
    typesize: usize,
    filters_are_noop: bool,
    emulate_source_alias: bool,
) -> Result<Vec<Vec<u8>>, &'static str> {
    let single_shuffle = single_shuffle_filter(&cparams.filters, &cparams.filters_meta, typesize);
    let mut scratch: Vec<u8> = Vec::new();
    let mut out: Vec<Vec<u8>> = Vec::with_capacity(nblocks);
    let mut prefilter_scratch: Vec<u8> = Vec::new();
    let mut filter_source =
        (emulate_source_alias && c_forward_pipeline_writes_source(cparams)).then(|| src.to_vec());
    let source_write_ordinal = c_source_write_active_ordinal(cparams);
    let mut source_write_buf1 = Vec::new();
    let mut source_write_buf2 = Vec::new();
    for block_idx in 0..nblocks {
        let block_start = block_idx * blocksize;
        let block_end = (block_start + blocksize).min(src.len());
        let block_storage = filter_source
            .as_ref()
            .map(|source| source[block_start..block_end].to_vec());
        let block_data = block_storage
            .as_deref()
            .unwrap_or(&src[block_start..block_end]);
        let mut skip_filters = false;
        let block_data = if let Some(filtered) = apply_prefilter(
            cparams,
            block_data,
            block_start,
            blocksize,
            &mut prefilter_scratch,
            0,
            false,
        )? {
            skip_filters = filtered.skip_filters;
            filtered.data
        } else {
            block_data
        };
        let bsize = block_data.len();
        if filters_are_noop || skip_filters {
            out.push(block_data.to_vec());
            continue;
        }
        if let Some(shuffle_typesize) = single_shuffle {
            let mut filtered = vec![0u8; bsize];
            filters::shuffle(shuffle_typesize, block_data, &mut filtered);
            out.push(filtered);
            continue;
        }
        let mut buf1 = vec![0u8; bsize];
        scratch.resize(bsize, 0);
        let delta_ref_storage = if block_start == 0 {
            None
        } else {
            delta_reference_block(src, cparams, blocksize, 0)?
        };
        let filter_cparams = filter_cparams_context(cparams, blocksize as i32);
        let fb = filters::apply_filter_pipeline_for_compression_with_context(
            block_data,
            &mut buf1,
            &mut scratch[..bsize],
            &cparams.filters,
            &cparams.filters_meta,
            typesize,
            block_start,
            delta_ref_storage.as_deref(),
            Some(filters::FilterPipelineContext {
                cparams: Some(&filter_cparams),
                dparams: None,
                chunk: filters::FilterChunkContext {
                    schunk: cparams.schunk,
                    nchunk: cparams.nchunk,
                    nblock: block_idx as i32,
                    block_offset: block_start,
                    blocksize,
                    bsize,
                },
                b2nd_metalayer: cparams.b2nd_metalayer.as_deref(),
                user_data: cparams.prefilter_user_data,
            }),
        );
        match fb {
            1 => {
                if let Some(source) = filter_source.as_mut() {
                    if let Some(ordinal) = source_write_ordinal {
                        let source_write_data = source_alias_filtered_block(
                            cparams,
                            block_data,
                            source,
                            block_start,
                            blocksize,
                            bsize,
                            typesize,
                            ordinal,
                            &mut source_write_buf1,
                            &mut source_write_buf2,
                        )?;
                        source[block_start..block_start + bsize]
                            .copy_from_slice(&source_write_data);
                    }
                }
                out.push(buf1);
            }
            2 => {
                if let Some(source) = filter_source.as_mut() {
                    if let Some(ordinal) = source_write_ordinal {
                        let source_write_data = source_alias_filtered_block(
                            cparams,
                            block_data,
                            source,
                            block_start,
                            blocksize,
                            bsize,
                            typesize,
                            ordinal,
                            &mut source_write_buf1,
                            &mut source_write_buf2,
                        )?;
                        source[block_start..block_start + bsize]
                            .copy_from_slice(&source_write_data);
                    }
                }
                std::mem::swap(&mut buf1, &mut scratch);
                buf1.truncate(bsize);
                out.push(buf1);
            }
            _ => return Err("Filter pipeline failed"),
        }
    }
    Ok(out)
}

#[allow(clippy::too_many_arguments)]
fn source_alias_filtered_block(
    cparams: &CParams,
    block_data: &[u8],
    source: &[u8],
    block_start: usize,
    blocksize: usize,
    bsize: usize,
    typesize: usize,
    ordinal: usize,
    buf1: &mut Vec<u8>,
    buf2: &mut Vec<u8>,
) -> Result<Vec<u8>, &'static str> {
    let prefix_filters = filter_prefix_through_active_ordinal(&cparams.filters, ordinal);
    ensure_scratch_len_uninit(buf1, bsize);
    ensure_scratch_len_uninit(buf2, bsize);
    let delta_ref_storage = if block_start == 0 {
        None
    } else {
        delta_reference_block(source, cparams, blocksize, 0)?
    };
    let filter_cparams = filter_cparams_context(cparams, blocksize as i32);
    let fb = filters::apply_filter_pipeline_for_compression_with_context(
        block_data,
        &mut buf1[..bsize],
        &mut buf2[..bsize],
        &prefix_filters,
        &cparams.filters_meta,
        typesize,
        block_start,
        delta_ref_storage.as_deref(),
        Some(filters::FilterPipelineContext {
            cparams: Some(&filter_cparams),
            dparams: None,
            chunk: filters::FilterChunkContext {
                schunk: cparams.schunk,
                nchunk: cparams.nchunk,
                nblock: (block_start / blocksize) as i32,
                block_offset: block_start,
                blocksize,
                bsize,
            },
            b2nd_metalayer: cparams.b2nd_metalayer.as_deref(),
            user_data: cparams.prefilter_user_data,
        }),
    );
    match fb {
        1 => Ok(buf1[..bsize].to_vec()),
        2 => Ok(buf2[..bsize].to_vec()),
        _ => Err("Filter pipeline failed"),
    }
}

/// Train a Zstd dictionary from the same sample window C feeds into
/// `ZDICT_trainFromBuffer()`.
fn train_zstd_dictionary(
    samples_buffer: &[u8],
    nbytes: usize,
    sample_sizes: &[usize],
) -> Option<Vec<u8>> {
    let dict_maxsize = BLOSC2_MAXDICTSIZE.min(nbytes / 20);
    if dict_maxsize < BLOSC2_MINUSEFULDICT || samples_buffer.is_empty() || sample_sizes.is_empty() {
        return None;
    }

    let samples_len = sample_sizes.iter().try_fold(0usize, |acc, &size| {
        if size == 0 {
            None
        } else {
            acc.checked_add(size)
        }
    })?;
    let target = dict_maxsize.min(samples_len);
    if target < BLOSC2_MINUSEFULDICT || samples_buffer.len() < samples_len {
        return None;
    }

    if let Some(dict) = train_zstd_dictionary_with_zdict(samples_buffer, sample_sizes, dict_maxsize)
    {
        return Some(dict);
    }

    let mut content = zstd_fastcover_content(samples_buffer, sample_sizes, target)?;

    if let Some(cap) = low_diversity_zstd_content_cap(&content) {
        content.truncate(cap.min(content.len()));
    }

    let training_sample_count =
        zstd_fastcover_training_count(sample_sizes).unwrap_or(sample_sizes.len());
    let entropy_len = sample_sizes[..training_sample_count]
        .iter()
        .try_fold(0usize, |acc, &size| acc.checked_add(size))?;
    let entropy_samples = &samples_buffer[..entropy_len];
    let entropy_sample_sizes = &sample_sizes[..training_sample_count];
    finalize_zstd_fallback_dict(
        &content,
        entropy_samples,
        entropy_sample_sizes,
        dict_maxsize,
    )
}

fn train_zstd_dictionary_with_zdict(
    samples_buffer: &[u8],
    sample_sizes: &[usize],
    dict_maxsize: usize,
) -> Option<Vec<u8>> {
    let nb_samples = c_uint::try_from(sample_sizes.len()).ok()?;
    let mut dict = vec![0u8; dict_maxsize];
    let actual_size = unsafe {
        ZDICT_trainFromBuffer(
            dict.as_mut_ptr().cast::<c_void>(),
            dict.len(),
            samples_buffer.as_ptr().cast::<c_void>(),
            sample_sizes.as_ptr(),
            nb_samples,
        )
    };
    if unsafe { ZDICT_isError(actual_size) } != 0 || actual_size == 0 || actual_size > dict.len() {
        return None;
    }
    dict.truncate(actual_size);
    Some(dict)
}

#[derive(Clone, Copy)]
struct FastCoverSegment {
    begin: usize,
    end: usize,
    score: u32,
}

fn zstd_fastcover_content(
    samples_buffer: &[u8],
    sample_sizes: &[usize],
    dict_capacity: usize,
) -> Option<Vec<u8>> {
    const D: usize = 8;
    const DEFAULT_K_CANDIDATES: [usize; 5] = [50, 537, 1024, 1511, 1998];

    if dict_capacity < BLOSC2_MINUSEFULDICT || sample_sizes.len() < 5 {
        return None;
    }
    let training_sample_count = sample_sizes.len() * 3 / 4;
    if training_sample_count < 5 || training_sample_count >= sample_sizes.len() {
        return None;
    }
    let mut offsets = Vec::with_capacity(sample_sizes.len() + 1);
    offsets.push(0usize);
    for &size in sample_sizes {
        offsets.push(offsets.last()?.checked_add(size)?);
    }
    let total_size = offsets[training_sample_count];
    if *offsets.last()? > samples_buffer.len() || total_size < D {
        return None;
    }

    let nb_dmers = total_size.checked_sub(D)?.checked_add(1)?;
    let mut best = Vec::new();
    let mut best_score = usize::MAX;
    let training_offsets = &offsets[..=training_sample_count];
    let entropy_samples = &samples_buffer[..total_size];
    for k in DEFAULT_K_CANDIDATES
        .into_iter()
        .filter(|&k| k >= D && k <= dict_capacity)
    {
        let Some(candidate) = fastcover_build_dictionary(
            samples_buffer,
            training_offsets,
            nb_dmers,
            k,
            dict_capacity,
        ) else {
            continue;
        };
        let Some(score) = fastcover_candidate_score(
            samples_buffer,
            sample_sizes,
            &offsets,
            training_sample_count,
            &candidate,
            entropy_samples,
            &sample_sizes[..training_sample_count],
            dict_capacity,
        ) else {
            continue;
        };
        if score < best_score {
            best_score = score;
            best = candidate;
        }
    }
    (!best.is_empty()).then_some(best)
}

fn fastcover_candidate_score(
    samples_buffer: &[u8],
    sample_sizes: &[usize],
    offsets: &[usize],
    test_sample_start: usize,
    content: &[u8],
    entropy_samples: &[u8],
    entropy_sample_sizes: &[usize],
    dict_capacity: usize,
) -> Option<usize> {
    let effective_content;
    let content = if let Some(cap) = low_diversity_zstd_content_cap(content) {
        effective_content = content[..cap.min(content.len())].to_vec();
        &effective_content
    } else {
        content
    };
    let dict = finalize_zstd_fallback_dict(
        content,
        entropy_samples,
        entropy_sample_sizes,
        dict_capacity,
    )?;
    let cdict = ZSTD_createCDict(&dict, 3)?;
    let max_sample_size = sample_sizes[test_sample_start..].iter().copied().max()?;
    let mut dst = vec![0u8; ZSTD_compressBound(max_sample_size)];
    let mut cctx = ZSTD_createCCtx()?;
    let mut score = dict.len();
    for idx in test_sample_start..sample_sizes.len() {
        let sample = &samples_buffer[offsets[idx]..offsets[idx + 1]];
        let written = ZSTD_compress_usingCDict(&mut cctx, &mut dst, sample, &cdict);
        if ERR_isError(written) {
            return None;
        }
        score = score.checked_add(written)?;
    }
    Some(score)
}

fn zstd_fastcover_training_count(sample_sizes: &[usize]) -> Option<usize> {
    let training_sample_count = sample_sizes.len() * 3 / 4;
    if training_sample_count < 5 || training_sample_count >= sample_sizes.len() {
        return None;
    }
    Some(training_sample_count)
}

fn fastcover_build_dictionary(
    samples_buffer: &[u8],
    offsets: &[usize],
    nb_dmers: usize,
    k: usize,
    dict_capacity: usize,
) -> Option<Vec<u8>> {
    const D: usize = 8;
    const F: u32 = 20;
    const MAX_ZERO_SCORE_RUN: usize = 10;

    let mut freqs = fastcover_compute_frequencies(samples_buffer, offsets)?;
    let (num_epochs, epoch_size) = fastcover_compute_epochs(dict_capacity, nb_dmers, k);
    if num_epochs == 0 || epoch_size == 0 {
        return None;
    }

    let mut segment_freqs = vec![0u16; 1usize << F];
    let mut dict = vec![0u8; dict_capacity];
    let mut tail = dict_capacity;
    let mut zero_score_run = 0usize;
    let mut epoch = 0usize;

    while tail > 0 {
        let epoch_begin = epoch.checked_mul(epoch_size)?;
        let segment = fastcover_select_segment(
            samples_buffer,
            &mut freqs,
            epoch_begin,
            epoch_begin + epoch_size,
            k,
            &mut segment_freqs,
        )?;
        if segment.score == 0 {
            zero_score_run += 1;
            if zero_score_run >= MAX_ZERO_SCORE_RUN {
                break;
            }
            epoch = (epoch + 1) % num_epochs;
            continue;
        }
        zero_score_run = 0;

        let segment_size = (segment.end - segment.begin + D - 1).min(tail);
        if segment_size < D {
            break;
        }
        tail -= segment_size;
        dict[tail..tail + segment_size]
            .copy_from_slice(&samples_buffer[segment.begin..segment.begin + segment_size]);
        epoch = (epoch + 1) % num_epochs;
    }

    (tail < dict_capacity).then(|| dict[tail..].to_vec())
}

fn fastcover_compute_frequencies(samples_buffer: &[u8], offsets: &[usize]) -> Option<Vec<u32>> {
    const D: usize = 8;
    const F: u32 = 20;

    let mut freqs = vec![0u32; 1usize << F];
    for window in offsets.windows(2) {
        let mut pos = window[0];
        let end = window[1];
        while pos + D <= end {
            let idx = ZSTD_hashPtr(&samples_buffer[pos..], F, D as u32);
            freqs[idx] = freqs[idx].wrapping_add(1);
            pos += 1;
        }
    }
    Some(freqs)
}

fn fastcover_compute_epochs(max_dict_size: usize, nb_dmers: usize, k: usize) -> (usize, usize) {
    let min_epoch_size = k * 10;
    let mut num = (max_dict_size / k).max(1);
    let mut size = nb_dmers / num;
    if size >= min_epoch_size {
        return (num, size);
    }
    size = min_epoch_size.min(nb_dmers);
    num = nb_dmers / size;
    (num.max(1), size)
}

fn fastcover_select_segment(
    samples_buffer: &[u8],
    freqs: &mut [u32],
    begin: usize,
    end: usize,
    k: usize,
    segment_freqs: &mut [u16],
) -> Option<FastCoverSegment> {
    const D: usize = 8;
    const F: u32 = 20;

    let dmers_in_k = k - D + 1;
    let mut best = FastCoverSegment {
        begin: 0,
        end: 0,
        score: 0,
    };
    let mut active = FastCoverSegment {
        begin,
        end: begin,
        score: 0,
    };

    while active.end < end {
        let idx = ZSTD_hashPtr(&samples_buffer[active.end..], F, D as u32);
        if segment_freqs[idx] == 0 {
            active.score = active.score.wrapping_add(freqs[idx]);
        }
        active.end += 1;
        segment_freqs[idx] = segment_freqs[idx].wrapping_add(1);

        if active.end - active.begin == dmers_in_k + 1 {
            let del_idx = ZSTD_hashPtr(&samples_buffer[active.begin..], F, D as u32);
            segment_freqs[del_idx] = segment_freqs[del_idx].wrapping_sub(1);
            if segment_freqs[del_idx] == 0 {
                active.score = active.score.wrapping_sub(freqs[del_idx]);
            }
            active.begin += 1;
        }

        if active.score > best.score {
            best = active;
        }
    }

    while active.begin < end {
        let del_idx = ZSTD_hashPtr(&samples_buffer[active.begin..], F, D as u32);
        segment_freqs[del_idx] = segment_freqs[del_idx].wrapping_sub(1);
        active.begin += 1;
    }

    for pos in best.begin..best.end {
        let idx = ZSTD_hashPtr(&samples_buffer[pos..], F, D as u32);
        freqs[idx] = 0;
    }

    Some(best)
}

fn finalize_zstd_fallback_dict(
    content: &[u8],
    entropy_samples: &[u8],
    entropy_sample_sizes: &[usize],
    dict_maxsize: usize,
) -> Option<Vec<u8>> {
    if content.is_empty() || dict_maxsize < BLOSC2_MINUSEFULDICT {
        return None;
    }

    build_minimal_zstd_dict(content, entropy_samples, entropy_sample_sizes, dict_maxsize)
}

fn build_minimal_zstd_dict(
    content: &[u8],
    entropy_samples: &[u8],
    entropy_sample_sizes: &[usize],
    dict_capacity: usize,
) -> Option<Vec<u8>> {
    const MIN_CONTENT_SIZE: usize = 8;

    if content.is_empty() || dict_capacity < BLOSC2_MINUSEFULDICT || dict_capacity < content.len() {
        return None;
    }

    let mut out = Vec::with_capacity(content.len() + 256);
    out.extend_from_slice(&ZSTD_MAGIC_DICTIONARY.to_le_bytes());
    let random_id = XXH64(content, 0);
    let compliant_id = (random_id % ((1u64 << 31) - 32768)) + 32768;
    out.extend_from_slice(&(compliant_id as u32).to_le_bytes());

    let entropy_start = out.len();
    if append_zstd_entropy_tables_from_block_samples(
        &mut out,
        content,
        entropy_samples,
        entropy_sample_sizes,
    )
    .is_none()
    {
        out.truncate(entropy_start);
        let sample_len = entropy_samples.len().min(content.len());
        let huf_source = if sample_len == 0 {
            content
        } else {
            &entropy_samples[..sample_len]
        };
        if append_minimal_zstd_huf_table(&mut out, huf_source).is_none() {
            out.truncate(entropy_start);
            append_minimal_zstd_huf_table(&mut out, content)?;
        }
        append_zstd_fallback_sequence_tables(&mut out, content.len())?;
    }

    let header_len = out.len() + 12;
    if header_len > dict_capacity {
        return None;
    }
    let content_len = content.len().min(dict_capacity - header_len);
    if content_len < MIN_CONTENT_SIZE && header_len + MIN_CONTENT_SIZE > dict_capacity {
        return None;
    }
    let padding_len = MIN_CONTENT_SIZE.saturating_sub(content_len);
    for rep in [1u32, 4, 8] {
        out.extend_from_slice(&rep.to_le_bytes());
    }
    out.resize(out.len() + padding_len, 0);
    out.extend_from_slice(&content[..content_len]);
    if out.len() > dict_capacity {
        return None;
    }
    Some(out)
}

fn append_zstd_entropy_tables_from_block_samples(
    out: &mut Vec<u8>,
    content: &[u8],
    entropy_samples: &[u8],
    entropy_sample_sizes: &[usize],
) -> Option<()> {
    const ZDICT_OFFCODE_MAX: u32 = 30;
    const ZSTD_BLOCKSIZE_MAX: usize = 128 * 1024;

    let samples_len = entropy_sample_sizes
        .iter()
        .try_fold(0usize, |acc, &size| acc.checked_add(size))?;
    if samples_len == 0 || samples_len > entropy_samples.len() {
        return None;
    }

    let offcode_max = ZSTD_highbit32((content.len() + ZSTD_BLOCKSIZE_MAX) as u32);
    if offcode_max > ZDICT_OFFCODE_MAX || offcode_max > MaxOff {
        return None;
    }

    let mut literal_count = [1u32; 256];
    let mut offcode_count = zstd_seeded_offcode_counts(offcode_max);
    let mut ml_count = vec![1u32; (MaxML + 1) as usize];
    let mut ll_count = vec![1u32; (MaxLL + 1) as usize];

    let cdict = ZSTD_createCDict_byReference(content, 3)?;
    let mut cctx = ZSTD_createCCtx()?;
    let mut work_place = vec![0u8; ZSTD_BLOCKSIZE_MAX];
    let mut sample_offset = 0usize;
    for &sample_size in entropy_sample_sizes {
        let sample_end = sample_offset.checked_add(sample_size)?;
        let sample = &entropy_samples[sample_offset..sample_end];
        let sample = &sample[..sample.len().min(ZSTD_BLOCKSIZE_MAX)];
        sample_offset = sample_end;
        if sample.is_empty() {
            continue;
        }

        if ERR_isError(ZSTD_compressBegin_usingCDict_deprecated(&mut cctx, &cdict)) {
            return None;
        }
        let csize = ZSTD_compressBlock_deprecated(&mut cctx, &mut work_place, sample);
        if ERR_isError(csize) {
            return None;
        }
        if csize == 0 {
            continue;
        }

        let seq_store = ZSTD_getSeqStore(&cctx)?;
        for &literal in &seq_store.literals {
            literal_count[literal as usize] = literal_count[literal as usize].saturating_add(1);
        }
        for idx in 0..seq_store.sequences.len() {
            let offcode = *seq_store.ofCode.get(idx)? as u32;
            if offcode > offcode_max {
                return None;
            }
            offcode_count[offcode as usize] = offcode_count[offcode as usize].saturating_add(1);

            let ml_code = *seq_store.mlCode.get(idx)? as u32;
            let ll_code = *seq_store.llCode.get(idx)? as u32;
            if ml_code > MaxML || ll_code > MaxLL {
                return None;
            }
            ml_count[ml_code as usize] = ml_count[ml_code as usize].saturating_add(1);
            ll_count[ll_code as usize] = ll_count[ll_code as usize].saturating_add(1);
        }
    }

    append_zstd_huf_table_from_counts(out, &literal_count)?;
    append_normalized_zstd_count(
        out,
        &offcode_count,
        offcode_max,
        ZDICT_OFFCODE_MAX,
        OffFSELog,
    )?;
    append_normalized_zstd_count(out, &ml_count, MaxML, MaxML, MLFSELog)?;
    append_normalized_zstd_count(out, &ll_count, MaxLL, MaxLL, LLFSELog)?;
    Some(())
}

fn append_zstd_fallback_sequence_tables(out: &mut Vec<u8>, dict_content_len: usize) -> Option<()> {
    const ZDICT_OFFCODE_MAX: u32 = 30;

    let offcode_max = ZSTD_highbit32((dict_content_len + (128 * 1024)) as u32);
    if offcode_max > ZDICT_OFFCODE_MAX || offcode_max > MaxOff {
        return None;
    }
    let offcode_count = zstd_seeded_offcode_counts(offcode_max);
    let ml_count = vec![1u32; (MaxML + 1) as usize];
    let ll_count = vec![1u32; (MaxLL + 1) as usize];
    append_normalized_zstd_count(
        out,
        &offcode_count,
        offcode_max,
        ZDICT_OFFCODE_MAX,
        OffFSELog,
    )?;
    append_normalized_zstd_count(out, &ml_count, MaxML, MaxML, MLFSELog)?;
    append_normalized_zstd_count(out, &ll_count, MaxLL, MaxLL, LLFSELog)?;
    Some(())
}

fn zstd_seeded_offcode_counts(offcode_max: u32) -> Vec<u32> {
    let mut offcode_count = vec![0u32; (MaxOff + 1) as usize];
    for count in offcode_count.iter_mut().take(offcode_max as usize + 1) {
        *count = 1;
    }
    offcode_count
}

fn append_normalized_zstd_count(
    out: &mut Vec<u8>,
    count: &[u32],
    normalize_max_symbol: u32,
    write_max_symbol: u32,
    table_log: u32,
) -> Option<()> {
    let total = count
        .iter()
        .take(normalize_max_symbol as usize + 1)
        .map(|&count| count as usize)
        .sum::<usize>();
    let mut normalized = vec![0i16; count.len()];
    let normalized_log = FSE_normalizeCount(
        &mut normalized,
        table_log,
        count,
        total,
        normalize_max_symbol,
        1,
    );
    if ERR_isError(normalized_log) || normalized_log == 0 {
        return None;
    }
    let mut fse_header = vec![0u8; 256];
    let written = FSE_writeNCount(
        &mut fse_header,
        &normalized,
        write_max_symbol,
        normalized_log as u32,
    );
    if ERR_isError(written) {
        return None;
    }
    out.extend_from_slice(&fse_header[..written]);
    Some(())
}

fn append_minimal_zstd_huf_table(out: &mut Vec<u8>, huf_source: &[u8]) -> Option<()> {
    let mut count = [1u32; 256];
    for &byte in huf_source {
        count[byte as usize] += 1;
    }
    append_zstd_huf_table_from_counts(out, &count)
}

fn append_zstd_huf_table_from_counts(out: &mut Vec<u8>, count: &[u32; 256]) -> Option<()> {
    let max_symbol_value = 255;
    let total_count = count.iter().sum::<u32>() as usize;
    let table_log = HUF_optimalTableLog(11, total_count, max_symbol_value);
    let mut ctable = vec![0u64; 257];
    let mut workspace = vec![0u32; HUF_CTABLE_WORKSPACE_SIZE_U32];
    let rc = HUF_buildCTable_wksp(
        &mut ctable,
        count,
        max_symbol_value,
        table_log,
        &mut workspace,
    );
    if ERR_isError(rc) {
        return None;
    }
    if rc == 8 {
        let mut flat_count = [2u32; 256];
        flat_count[0] = 4;
        flat_count[253] = 1;
        flat_count[254] = 1;
        return append_zstd_huf_table_from_counts(out, &flat_count);
    }
    let table_log = HUF_readCTableHeader(&ctable).tableLog as u32;
    let mut huf_header = vec![0u8; 512];
    let written = HUF_writeCTable(&mut huf_header, &ctable, max_symbol_value, table_log);
    if ERR_isError(written) {
        return None;
    }
    out.extend_from_slice(&huf_header[..written]);
    Some(())
}

fn low_diversity_zstd_content_cap(dict: &[u8]) -> Option<usize> {
    if dict.len() <= 512 {
        return None;
    }

    let mut unique_windows = [0u64; 513];
    let mut unique_count = 0usize;
    for window in dict.windows(8).step_by(8) {
        let value = u64::from_le_bytes(window.try_into().ok()?);
        if unique_windows[..unique_count].contains(&value) {
            continue;
        }
        if unique_count == unique_windows.len() {
            return None;
        }
        unique_windows[unique_count] = value;
        unique_count += 1;
        if unique_count > 512 {
            return None;
        }
    }

    Some(512)
}

/// Build an LZ4-style dictionary from the leading portions of the filtered samples.
fn build_lz4_dictionary(
    samples_buffer: &[u8],
    nbytes: usize,
    dict_target: usize,
) -> Option<Vec<u8>> {
    let dict_maxsize = BLOSC2_MAXDICTSIZE.min(nbytes / 20);
    let dict_target = dict_target.min(dict_maxsize);
    if dict_target < BLOSC2_MINUSEFULDICT || samples_buffer.len() < dict_target {
        return None;
    }

    Some(samples_buffer[..dict_target].to_vec())
}

/// Dispatch to the dictionary builder appropriate for `compcode`. Returns `None` for codecs without dictionary support.
fn build_codec_dictionary(
    samples_buffer: &[u8],
    nbytes: usize,
    compcode: u8,
    lz4_dict_target: usize,
    zstd_sample_sizes: &[usize],
) -> Option<Vec<u8>> {
    match compcode {
        BLOSC_LZ4 | BLOSC_LZ4HC => build_lz4_dictionary(samples_buffer, nbytes, lz4_dict_target),
        BLOSC_ZSTD => train_zstd_dictionary(samples_buffer, nbytes, zstd_sample_sizes),
        _ => None,
    }
}

fn dictionary_training_buffer(filtered_blocks: &[Vec<u8>], limit: usize) -> Option<Vec<u8>> {
    let available = filtered_blocks
        .iter()
        .try_fold(0usize, |acc, block| acc.checked_add(block.len()))?;
    if available < limit {
        return None;
    }

    let mut buffer = Vec::with_capacity(limit);
    for block in filtered_blocks {
        let remaining = limit - buffer.len();
        if remaining == 0 {
            break;
        }
        buffer.extend_from_slice(&block[..remaining.min(block.len())]);
    }
    Some(buffer)
}

/// Compress `src` into a Blosc2 chunk using the supplied compression parameters.
///
/// Splits `src` into blocks, applies the filter pipeline (optionally preceded
/// by a prefilter), feeds each block through the configured codec and writes
/// the extended chunk header. If regular compression cannot fit in the caller's
/// output budget, an inline memcpy chunk is used as C-Blosc2's final fallback.
/// Returns the compressed chunk on success.
pub fn compress_chunk(src: &[u8], cparams: &CParams) -> Result<Vec<u8>, &'static str> {
    compress_chunk_with_output_limit(src, cparams, None)
}

/// Backwards-compatible alias for [`compress_chunk`].
pub fn compress(src: &[u8], cparams: &CParams) -> Result<Vec<u8>, &'static str> {
    compress_chunk(src, cparams)
}

fn compress_chunk_with_output_limit(
    src: &[u8],
    cparams: &CParams,
    output_limit: Option<usize>,
) -> Result<Vec<u8>, &'static str> {
    validate_cparams(cparams, src.len())?;
    let normalized_cparams = normalized_cparams(cparams);
    let cparams = &normalized_cparams;
    let nbytes = src.len() as i32;

    // Handle empty input
    if nbytes == 0 {
        check_output_budget(BLOSC_EXTENDED_HEADER_LENGTH, output_limit)?;
        let mut chunk = vec![0u8; BLOSC_EXTENDED_HEADER_LENGTH];
        let header = ChunkHeader {
            version: BLOSC2_VERSION_FORMAT_STABLE,
            versionlz: codec_version_for_header(cparams.compcode),
            flags: BLOSC_DOSHUFFLE | BLOSC_DOBITSHUFFLE | BLOSC_MEMCPYED,
            typesize: cparams.typesize as u8,
            nbytes: 0,
            blocksize: 1,
            cbytes: BLOSC_EXTENDED_HEADER_LENGTH as i32,
            filters: cparams.filters,
            filters_meta: cparams.filters_meta,
            udcompcode: udcompcode_for_header(cparams.compcode),
            compcode_meta: cparams.compcode_meta,
            ..Default::default()
        };
        header.try_write(&mut chunk)?;
        return Ok(chunk);
    }

    let typesize = cparams.typesize as usize;
    let blocksize = compute_blocksize(cparams, nbytes) as usize;
    let nblocks = (nbytes as usize).div_ceil(blocksize);

    let filter_flags = compute_filter_flags(&cparams.filters);
    let do_split = should_split(
        cparams.compcode,
        cparams.clevel,
        cparams.splitmode,
        cparams.typesize,
        blocksize as i32,
        filter_flags,
    );
    let dont_split = !do_split;

    // Build header flags
    let mut flags = BLOSC_DOSHUFFLE | BLOSC_DOBITSHUFFLE;
    flags |= compcode_to_compformat(cparams.compcode) << 5;
    flags |= filter_flags & BLOSC_DODELTA;
    if dont_split {
        flags |= BLOSC_DONT_SPLIT;
    }

    let header_len = BLOSC_EXTENDED_HEADER_LENGTH;
    let bstarts_len = nblocks * 4;
    let c_mutates_filter_source = c_forward_pipeline_writes_source(cparams);
    let use_parallel =
        output_limit.is_none() && cparams.nthreads > 1 && nblocks > 1 && !c_mutates_filter_source;
    let table_end = header_len
        .checked_add(bstarts_len)
        .ok_or("Invalid block table size")?;
    if let Err(err) = check_output_budget(table_end, output_limit) {
        let header_fallback_flags = BLOSC_DOSHUFFLE | BLOSC_DOBITSHUFFLE;
        if let Some(memcpyed) = maybe_memcpy_fallback_for_budget(
            src,
            cparams,
            header_fallback_flags,
            blocksize,
            output_limit,
        ) {
            return Ok(memcpyed);
        }
        return Err(err);
    }

    // Check if filters are effectively a no-op (only shuffle with typesize<=1)
    let filters_are_noop =
        filters_effectively_noop(&cparams.filters, &cparams.filters_meta, typesize);

    if src.len() < BLOSC_MIN_BUFFERSIZE {
        check_output_budget(BLOSC_EXTENDED_HEADER_LENGTH + src.len(), output_limit)?;
        return prefiltered_memcpy_chunk(src, cparams, blocksize);
    }

    if cparams.clevel == 0 {
        check_output_budget(BLOSC_EXTENDED_HEADER_LENGTH + src.len(), output_limit)?;
        return prefiltered_memcpy_chunk(src, cparams, blocksize);
    }

    if cparams.use_dict && codecs::codec_supports_dict(cparams.compcode) && cparams.clevel > 0 {
        let training_blocks = filtered_blocks_for_dict(
            src,
            cparams,
            blocksize,
            nblocks,
            typesize,
            filters_are_noop,
            true,
        )?;
        let sample_nblocks = if dont_split {
            nblocks
        } else {
            nblocks.saturating_mul(typesize)
        }
        .max(8);
        let sample_size = (nbytes as usize) / sample_nblocks / 16;
        let training_buffer_len = sample_nblocks.saturating_mul(sample_size);
        let training_buffer = dictionary_training_buffer(&training_blocks, training_buffer_len);
        let zstd_sample_sizes = vec![sample_size; sample_nblocks];
        if let Some(dict) = build_codec_dictionary(
            training_buffer.as_deref().unwrap_or(&[]),
            nbytes as usize,
            cparams.compcode,
            training_buffer_len,
            &zstd_sample_sizes,
        ) {
            let filtered_blocks = filtered_blocks_for_dict(
                src,
                cparams,
                blocksize,
                nblocks,
                typesize,
                filters_are_noop,
                false,
            )?;
            let dict_section_len = 4 + dict.len();
            let table_and_dict_end = header_len
                .checked_add(bstarts_len)
                .and_then(|pos| pos.checked_add(dict_section_len))
                .ok_or("Invalid dictionary size")?;
            check_output_budget(table_and_dict_end, output_limit)?;
            let mut output = vec![0u8; table_and_dict_end];
            let mut output_pos = header_len + bstarts_len;

            output[output_pos..output_pos + 4].copy_from_slice(&(dict.len() as i32).to_le_bytes());
            output_pos += 4;
            output[output_pos..output_pos + dict.len()].copy_from_slice(&dict);
            output_pos += dict.len();

            let mut compress_scratch: Vec<u8> = Vec::new();
            for (block_idx, filtered) in filtered_blocks.iter().enumerate() {
                let block_start = block_idx * blocksize;
                let block_end = (block_start + blocksize).min(nbytes as usize);
                let bsize = block_end - block_start;
                let is_leftover = block_idx == nblocks - 1 && bsize < blocksize;

                let bstart_offset = header_len + block_idx * 4;
                output[bstart_offset..bstart_offset + 4]
                    .copy_from_slice(&(output_pos as i32).to_le_bytes());

                let (block_data, _block_all_zero, _any_literal_stream) =
                    compress_pre_filtered_block_with_scratch(
                        filtered,
                        src.as_ptr(),
                        cparams,
                        dont_split,
                        typesize,
                        is_leftover,
                        Some(&dict),
                        block_start,
                        blocksize,
                        block_idx as i32,
                        &mut compress_scratch,
                        output_limit.map(|limit| limit.saturating_sub(output_pos)),
                    )?;
                ensure_len_with_budget(&mut output, output_pos + block_data.len(), output_limit)?;
                output[output_pos..output_pos + block_data.len()].copy_from_slice(&block_data);
                output_pos += block_data.len();
            }

            let blosc2_flags = BLOSC2_USEDICT;

            let header = ChunkHeader {
                version: BLOSC2_VERSION_FORMAT_STABLE,
                versionlz: codec_version_for_header(cparams.compcode),
                flags,
                typesize: cparams.typesize as u8,
                nbytes,
                blocksize: blocksize as i32,
                cbytes: output_pos as i32,
                filters: cparams.filters,
                filters_meta: cparams.filters_meta,
                udcompcode: udcompcode_for_header(cparams.compcode),
                compcode_meta: cparams.compcode_meta,
                blosc2_flags,
                ..Default::default()
            };
            header.try_write(&mut output[..BLOSC_EXTENDED_HEADER_LENGTH])?;

            output.truncate(output_pos);
            return Ok(output);
        }
    }

    let mut output_pos;
    let mut all_zero_runs;
    let mut any_literal_stream = false;
    let mut output;

    if use_parallel && nblocks == 2 {
        let threads = effective_nthreads(cparams.nthreads, nblocks);
        let (block0, block1) = with_thread_pool(threads, || {
            rayon::join(
                || {
                    with_compress_scratch(blocksize, |buf1, buf2, compress_buf, prefilter_buf| {
                        compress_block_with_scratch(
                            src,
                            &src[..blocksize.min(nbytes as usize)],
                            0,
                            blocksize,
                            false,
                            cparams,
                            dont_split,
                            typesize,
                            buf1,
                            buf2,
                            compress_buf,
                            prefilter_buf,
                            rayon::current_thread_index().unwrap_or(0) as i32,
                            None,
                        )
                    })
                },
                || {
                    with_compress_scratch(blocksize, |buf1, buf2, compress_buf, prefilter_buf| {
                        let start = blocksize;
                        let end = (start + blocksize).min(nbytes as usize);
                        compress_block_with_scratch(
                            src,
                            &src[start..end],
                            start,
                            blocksize,
                            end - start < blocksize,
                            cparams,
                            dont_split,
                            typesize,
                            buf1,
                            buf2,
                            compress_buf,
                            prefilter_buf,
                            rayon::current_thread_index().unwrap_or(1) as i32,
                            None,
                        )
                    })
                },
            )
        });
        let compressed_blocks = [block0?, block1?];
        let total_compressed: usize = compressed_blocks.iter().map(|(b, _, _)| b.len()).sum();
        let total_len = header_len + bstarts_len + total_compressed;
        check_output_budget(total_len, output_limit)?;
        output = uninit_vec(total_len);
        output_pos = header_len + bstarts_len;
        all_zero_runs = true;

        for (block_idx, (block_data, block_all_zero, block_has_literal)) in
            compressed_blocks.iter().enumerate()
        {
            let bstart_offset = header_len + block_idx * 4;
            output[bstart_offset..bstart_offset + 4]
                .copy_from_slice(&(output_pos as i32).to_le_bytes());
            output[output_pos..output_pos + block_data.len()].copy_from_slice(block_data);
            output_pos += block_data.len();
            if !block_all_zero {
                all_zero_runs = false;
            }
            if *block_has_literal {
                any_literal_stream = true;
            }
        }
    } else if use_parallel {
        let compressed_blocks: Vec<OnceLock<Result<(Vec<u8>, bool, bool), &'static str>>> =
            (0..nblocks).map(|_| OnceLock::new()).collect();
        let next_block = AtomicI32::new(0);
        let threads = effective_nthreads(cparams.nthreads, nblocks);
        with_thread_pool(threads, || {
            rayon::scope(|scope| {
                for _ in 0..threads as usize {
                    let next_block = &next_block;
                    let compressed_blocks = &compressed_blocks;
                    scope.spawn(move |_| {
                        with_compress_scratch(
                            blocksize,
                            |buf1, buf2, compress_buf, prefilter_buf| loop {
                                let block_idx = next_block.fetch_add(1, Ordering::Relaxed) as usize;
                                if block_idx >= nblocks {
                                    break;
                                }
                                let start = block_idx * blocksize;
                                let end = (start + blocksize).min(nbytes as usize);
                                let is_leftover =
                                    block_idx == nblocks - 1 && (end - start) < blocksize;
                                let compressed = compress_block_with_scratch(
                                    src,
                                    &src[start..end],
                                    start,
                                    blocksize,
                                    is_leftover,
                                    cparams,
                                    dont_split,
                                    typesize,
                                    buf1,
                                    buf2,
                                    compress_buf,
                                    prefilter_buf,
                                    rayon::current_thread_index().unwrap_or(0) as i32,
                                    None,
                                );
                                compressed_blocks[block_idx]
                                    .set(compressed)
                                    .expect("parallel block slot written more than once");
                            },
                        );
                    });
                }
            });
        });

        let compressed_blocks: Vec<(Vec<u8>, bool, bool)> = compressed_blocks
            .into_iter()
            .map(|slot| {
                slot.into_inner()
                    .expect("parallel block slot was not written")
            })
            .collect::<Result<Vec<_>, _>>()?;

        let total_compressed: usize = compressed_blocks.iter().map(|(b, _, _)| b.len()).sum();
        let total_len = header_len + bstarts_len + total_compressed;
        check_output_budget(total_len, output_limit)?;
        output = uninit_vec(total_len);
        output_pos = header_len + bstarts_len;
        all_zero_runs = true;

        for (block_idx, (block_data, block_all_zero, block_has_literal)) in
            compressed_blocks.iter().enumerate()
        {
            let bstart_offset = header_len + block_idx * 4;
            output[bstart_offset..bstart_offset + 4]
                .copy_from_slice(&(output_pos as i32).to_le_bytes());
            output[output_pos..output_pos + block_data.len()].copy_from_slice(block_data);
            output_pos += block_data.len();
            if !block_all_zero {
                all_zero_runs = false;
            }
            if *block_has_literal {
                any_literal_stream = true;
            }
        }
    } else {
        // Serial path: pre-allocate buffers once, write directly to output
        let max_compressed = nbytes as usize + header_len + bstarts_len + nblocks * 32;
        output = Vec::with_capacity(output_limit.map_or(max_compressed, |limit| {
            max_compressed.min(limit).max(table_end)
        }));
        output.resize(header_len + bstarts_len, 0);
        output_pos = header_len + bstarts_len;
        all_zero_runs = true;

        let single_shuffle =
            single_shuffle_filter(&cparams.filters, &cparams.filters_meta, typesize);
        let mut buf1 = if single_shuffle.is_some() {
            let mut buf = Vec::with_capacity(blocksize);
            ensure_scratch_len_uninit(&mut buf, blocksize);
            buf
        } else {
            vec![0u8; blocksize]
        };
        let mut buf2 = if single_shuffle.is_some() {
            Vec::new()
        } else {
            vec![0u8; blocksize]
        };
        let mut compress_buf = Vec::with_capacity(blocksize + (blocksize / 255) + 64);
        ensure_scratch_len_uninit(&mut compress_buf, blocksize + (blocksize / 255) + 64);
        let mut prefilter_buf = Vec::new();
        let mut filter_source = c_mutates_filter_source.then(|| src.to_vec());
        let source_write_ordinal = c_source_write_active_ordinal(cparams);
        let mut source_write_buf1 = Vec::new();
        let mut source_write_buf2 = Vec::new();

        for block_idx in 0..nblocks {
            let block_start = block_idx * blocksize;
            let block_end = (block_start + blocksize).min(nbytes as usize);
            let original_bsize = block_end - block_start;
            let is_leftover = block_idx == nblocks - 1 && original_bsize < blocksize;

            if !c_mutates_filter_source {
                let bstart_offset = header_len + block_idx * 4;
                output[bstart_offset..bstart_offset + 4]
                    .copy_from_slice(&(output_pos as i32).to_le_bytes());

                let (block_data, block_all_zero, block_has_literal) =
                    match compress_block_with_scratch(
                        src,
                        &src[block_start..block_end],
                        block_start,
                        blocksize,
                        is_leftover,
                        cparams,
                        dont_split,
                        typesize,
                        &mut buf1,
                        &mut buf2,
                        &mut compress_buf,
                        &mut prefilter_buf,
                        0,
                        output_limit.map(|limit| limit.saturating_sub(output_pos)),
                    ) {
                        Ok(block) => block,
                        Err("Destination too small") => {
                            if let Some(memcpyed) = maybe_memcpy_fallback_for_budget(
                                src,
                                cparams,
                                flags,
                                blocksize,
                                output_limit,
                            ) {
                                return Ok(memcpyed);
                            }
                            return Err("Destination too small");
                        }
                        Err(err) => return Err(err),
                    };
                if let Err(err) =
                    ensure_len_with_budget(&mut output, output_pos + block_data.len(), output_limit)
                {
                    if let Some(memcpyed) = maybe_memcpy_fallback_for_budget(
                        src,
                        cparams,
                        flags,
                        blocksize,
                        output_limit,
                    ) {
                        return Ok(memcpyed);
                    }
                    return Err(err);
                }
                output[output_pos..output_pos + block_data.len()].copy_from_slice(&block_data);
                output_pos += block_data.len();
                if !block_all_zero {
                    all_zero_runs = false;
                }
                if block_has_literal {
                    any_literal_stream = true;
                }
                continue;
            }

            let block_storage = filter_source
                .as_ref()
                .map(|source| source[block_start..block_end].to_vec());
            let block_data = block_storage
                .as_deref()
                .unwrap_or(&src[block_start..block_end]);
            let mut skip_filters = false;
            let mut force_zero_run = false;
            let block_data = if let Some(filtered) = apply_prefilter(
                cparams,
                block_data,
                block_start,
                blocksize,
                &mut prefilter_buf,
                0,
                true,
            )? {
                skip_filters = filtered.skip_filters;
                force_zero_run = filtered.force_zero_run;
                filtered.data
            } else {
                block_data
            };
            let bsize = block_data.len();

            // Store block start offset
            let bstart_offset = header_len + block_idx * 4;
            output[bstart_offset..bstart_offset + 4]
                .copy_from_slice(&(output_pos as i32).to_le_bytes());

            // Get filtered data — skip pipeline if filters are no-ops
            let filtered: &[u8] = if filters_are_noop || skip_filters {
                block_data
            } else if let Some(shuffle_typesize) = single_shuffle {
                ensure_scratch_len_uninit(&mut buf1, bsize);
                filters::shuffle(shuffle_typesize, block_data, &mut buf1[..bsize]);
                &buf1[..bsize]
            } else {
                ensure_scratch_len_uninit(&mut buf1, bsize);
                ensure_scratch_len_uninit(&mut buf2, bsize);
                let delta_ref_storage = if block_start == 0 {
                    None
                } else {
                    let delta_source = filter_source.as_deref().unwrap_or(src);
                    delta_reference_block(delta_source, cparams, blocksize, 0)?
                };
                let filter_cparams = filter_cparams_context(cparams, blocksize as i32);
                let fb = filters::apply_filter_pipeline_for_compression_with_context(
                    block_data,
                    &mut buf1[..bsize],
                    &mut buf2[..bsize],
                    &cparams.filters,
                    &cparams.filters_meta,
                    typesize,
                    block_start,
                    delta_ref_storage.as_deref(),
                    Some(filters::FilterPipelineContext {
                        cparams: Some(&filter_cparams),
                        dparams: None,
                        chunk: filters::FilterChunkContext {
                            schunk: cparams.schunk,
                            nchunk: cparams.nchunk,
                            nblock: block_idx as i32,
                            block_offset: block_start,
                            blocksize,
                            bsize,
                        },
                        b2nd_metalayer: cparams.b2nd_metalayer.as_deref(),
                        user_data: cparams.prefilter_user_data,
                    }),
                );
                match fb {
                    1 => &buf1[..bsize],
                    2 => &buf2[..bsize],
                    _ => return Err("Filter pipeline failed"),
                }
            };
            let zero_storage;
            let filtered = if force_zero_run {
                zero_storage = vec![0u8; bsize];
                &zero_storage[..]
            } else {
                filtered
            };
            let filtered_owned;
            let filtered = if let Some(source) = filter_source.as_mut() {
                let source_write_owned;
                let source_write_data: &[u8] = if filters_are_noop || skip_filters {
                    filtered
                } else if let Some(ordinal) = source_write_ordinal {
                    source_write_owned = source_alias_filtered_block(
                        cparams,
                        block_data,
                        source,
                        block_start,
                        blocksize,
                        bsize,
                        typesize,
                        ordinal,
                        &mut source_write_buf1,
                        &mut source_write_buf2,
                    )?;
                    &source_write_owned[..]
                } else {
                    filtered
                };
                source[block_start..block_start + bsize].copy_from_slice(source_write_data);
                filtered_owned = filtered.to_vec();
                &filtered_owned[..]
            } else {
                filtered
            };

            let (block_data, block_all_zero_runs, block_has_literal) =
                match compress_pre_filtered_block_with_scratch(
                    filtered,
                    src.as_ptr(),
                    cparams,
                    dont_split,
                    typesize,
                    is_leftover,
                    None,
                    block_start,
                    blocksize,
                    block_idx as i32,
                    &mut compress_buf,
                    output_limit.map(|limit| limit.saturating_sub(output_pos)),
                ) {
                    Ok(block) => block,
                    Err("Destination too small") => {
                        if let Some(memcpyed) = maybe_memcpy_fallback_for_budget(
                            src,
                            cparams,
                            flags,
                            blocksize,
                            output_limit,
                        ) {
                            return Ok(memcpyed);
                        }
                        return Err("Destination too small");
                    }
                    Err(err) => return Err(err),
                };
            if let Err(err) =
                ensure_len_with_budget(&mut output, output_pos + block_data.len(), output_limit)
            {
                if let Some(memcpyed) =
                    maybe_memcpy_fallback_for_budget(src, cparams, flags, blocksize, output_limit)
                {
                    return Ok(memcpyed);
                }
                return Err(err);
            }
            output[output_pos..output_pos + block_data.len()].copy_from_slice(&block_data);
            output_pos += block_data.len();
            if !block_all_zero_runs {
                all_zero_runs = false;
            }
            if block_has_literal {
                any_literal_stream = true;
            }
        }
    }

    if any_literal_stream && c_raw_stream_decode_needs_memcpy_fallback(cparams) {
        let fallback_len = BLOSC_EXTENDED_HEADER_LENGTH
            .checked_add(src.len())
            .ok_or("Input too large")?;
        check_output_budget(fallback_len, output_limit)?;
        return Ok(memcpy_chunk_with_flags(
            src,
            cparams,
            blocksize,
            flags | BLOSC_MEMCPYED,
        ));
    }

    let should_memcpy_oversized_chunk = output_limit.is_some()
        || (any_literal_stream && codecs::is_known_zfp_codec(cparams.compcode));
    if should_memcpy_oversized_chunk
        && !all_zero_runs
        && output_pos
            >= BLOSC_EXTENDED_HEADER_LENGTH
                .checked_add(src.len())
                .ok_or("Input too large")?
    {
        if let Some(memcpyed) =
            maybe_memcpy_fallback_for_budget(src, cparams, flags, blocksize, output_limit)
        {
            return Ok(memcpyed);
        }
    }

    // Handle special case: all blocks are zero runs
    let mut blosc2_flags = 0u8;
    if all_zero_runs {
        blosc2_flags |= BLOSC2_SPECIAL_ZERO << 4;
        output_pos = header_len;
    }

    let header = ChunkHeader {
        version: BLOSC2_VERSION_FORMAT_STABLE,
        versionlz: codec_version_for_header(cparams.compcode),
        flags,
        typesize: cparams.typesize as u8,
        nbytes,
        blocksize: blocksize as i32,
        cbytes: output_pos as i32,
        filters: cparams.filters,
        filters_meta: cparams.filters_meta,
        udcompcode: udcompcode_for_header(cparams.compcode),
        compcode_meta: cparams.compcode_meta,
        blosc2_flags,
        ..Default::default()
    };
    header.try_write(&mut output[..BLOSC_EXTENDED_HEADER_LENGTH])?;

    output.truncate(output_pos);
    Ok(output)
}

/// Compress a slice of input buffers in parallel when `cparams.nthreads > 1`,
/// returning one Blosc2 chunk per input. Each call shares the supplied parameters
/// but compresses each chunk single-threaded.
pub fn compress_many(buffers: &[&[u8]], cparams: &CParams) -> Result<Vec<Vec<u8>>, &'static str> {
    if buffers.len() > 1 && cparams.nthreads > 1 {
        let per_chunk_params = CParams {
            nthreads: 1,
            ..cparams.clone()
        };
        with_thread_pool(effective_nthreads(cparams.nthreads, buffers.len()), || {
            buffers
                .par_iter()
                .map(|buffer| compress(buffer, &per_chunk_params))
                .collect()
        })
    } else {
        buffers
            .iter()
            .map(|buffer| compress(buffer, cparams))
            .collect()
    }
}

/// Validate variable-length-block compression inputs and return the total payload size in bytes.
fn validate_vl_inputs(blocks: &[&[u8]], cparams: &CParams) -> Result<usize, &'static str> {
    if blocks.is_empty() {
        return Err("VL-block input cannot be empty");
    }
    if blocks.len() > BLOSC2_MAXBLOCKSIZE {
        return Err("Too many VL-blocks");
    }
    let mut total = 0usize;
    for block in blocks {
        if block.is_empty() {
            return Err("VL-blocks cannot be empty");
        }
        total = total
            .checked_add(block.len())
            .ok_or("VL-block input too large")?;
    }
    let mut validation_cparams = cparams.clone();
    validation_cparams.use_dict = false;
    validate_cparams(&validation_cparams, total)?;
    Ok(total)
}

/// Apply the prefilter and forward filter pipeline to each variable-length block independently.
fn filtered_vl_blocks(
    blocks: &[&[u8]],
    cparams: &CParams,
    max_blocksize: usize,
    tid: i32,
) -> Result<Vec<Vec<u8>>, &'static str> {
    let typesize = cparams.typesize as usize;
    let filters_are_noop =
        filters_effectively_noop(&cparams.filters, &cparams.filters_meta, typesize);
    let single_shuffle = single_shuffle_filter(&cparams.filters, &cparams.filters_meta, typesize);
    let mut scratch: Vec<u8> = Vec::new();
    let mut out: Vec<Vec<u8>> = Vec::with_capacity(blocks.len());
    let mut prefilter_scratch = Vec::new();
    for (block_idx, block) in blocks.iter().enumerate() {
        let mut skip_filters = false;
        let block = if let Some(filtered) = apply_prefilter(
            cparams,
            block,
            0,
            max_blocksize.max(1),
            &mut prefilter_scratch,
            tid,
            false,
        )? {
            skip_filters = filtered.skip_filters;
            filtered.data
        } else {
            *block
        };
        if filters_are_noop || skip_filters {
            out.push(block.to_vec());
            continue;
        }
        if let Some(shuffle_typesize) = single_shuffle {
            let mut filtered = vec![0u8; block.len()];
            filters::shuffle(shuffle_typesize, block, &mut filtered);
            out.push(filtered);
            continue;
        }
        let mut buf1 = vec![0u8; block.len()];
        scratch.resize(block.len(), 0);
        let filter_cparams = filter_cparams_context(cparams, max_blocksize as i32);
        let selected = filters::apply_filter_pipeline_for_compression_with_context(
            block,
            &mut buf1,
            &mut scratch[..block.len()],
            &cparams.filters,
            &cparams.filters_meta,
            typesize,
            0,
            None,
            Some(filters::FilterPipelineContext {
                cparams: Some(&filter_cparams),
                dparams: None,
                chunk: filters::FilterChunkContext {
                    schunk: cparams.schunk,
                    nchunk: cparams.nchunk,
                    nblock: block_idx as i32,
                    block_offset: 0,
                    blocksize: max_blocksize,
                    bsize: block.len(),
                },
                b2nd_metalayer: cparams.b2nd_metalayer.as_deref(),
                user_data: cparams.prefilter_user_data,
            }),
        );
        match selected {
            1 => out.push(buf1),
            2 => {
                std::mem::swap(&mut buf1, &mut scratch);
                buf1.truncate(block.len());
                out.push(buf1);
            }
            _ => return Err("Filter pipeline failed"),
        }
    }
    Ok(out)
}

/// Codec-encode a single filtered VL block (with optional shared dictionary), prefixed by its uncompressed size.
fn compress_filtered_vl_block(
    filtered: &[u8],
    chunk_source: *const u8,
    cparams: &CParams,
    max_blocksize: usize,
    block_idx: i32,
    dict: Option<&[u8]>,
) -> Result<Vec<u8>, &'static str> {
    let max_out = filtered.len() + (filtered.len() / 255) + 32;
    let mut compressed = vec![0u8; max_out];
    let cbytes = match dict {
        Some(dict) => codecs::compress_block_with_dict(
            cparams.compcode,
            cparams.clevel,
            filtered,
            &mut compressed[..max_out],
            dict,
        ),
        None => {
            let codec_cparams = codec_cparams_context(cparams, max_blocksize as i32);
            let chunk_source = chunk_source as usize;
            let block_offset = if chunk_source == 0 {
                // C-Blosc2 initializes VL compression contexts with
                // context->src == NULL, and passes that value as the C ABI
                // codec `chunk` argument. The codec adapter derives its
                // fallback argument as input - block_offset, so make that
                // fallback null for VL compression when no context source is
                // available.
                filtered.as_ptr() as usize
            } else {
                0
            };
            codecs::compress_block_with_context(
                cparams.compcode,
                cparams.clevel,
                cparams.compcode_meta,
                filtered,
                &mut compressed[..max_out],
                Some(codecs::CodecCallbackContext {
                    compcode: cparams.compcode,
                    complib: None,
                    meta: cparams.compcode_meta,
                    clevel: cparams.clevel,
                    cparams: Some(&codec_cparams),
                    dparams: None,
                    chunk: codecs::CodecChunkContext {
                        schunk: cparams.schunk,
                        nchunk: cparams.nchunk,
                        nblock: block_idx,
                        chunk_source,
                        block_offset,
                        blocksize: max_blocksize,
                        bsize: filtered.len(),
                    },
                    b2nd_metalayer: cparams.b2nd_metalayer.as_deref(),
                    user_data: cparams.prefilter_user_data,
                }),
            )
        }
    };

    let mut out = Vec::with_capacity(4 + filtered.len());
    out.extend_from_slice(&(filtered.len() as i32).to_le_bytes());
    if cbytes < 0 {
        return Err("Codec compression failed");
    }
    if cparams.clevel == 0 || cbytes == 0 || cbytes as usize >= filtered.len() {
        out.extend_from_slice(filtered);
    } else {
        out.extend_from_slice(&compressed[..cbytes as usize]);
    }
    Ok(out)
}

/// Filter and codec-encode a single VL block end-to-end (no dictionary path).
fn compress_vl_block(
    block: &[u8],
    cparams: &CParams,
    max_blocksize: usize,
    block_idx: i32,
    tid: i32,
) -> Result<Vec<u8>, &'static str> {
    let filtered = filtered_vl_blocks(&[block], cparams, max_blocksize, tid)?;
    compress_filtered_vl_block(
        &filtered[0],
        std::ptr::null(),
        cparams,
        max_blocksize,
        block_idx,
        None,
    )
}

/// Compress independent variable-length blocks into one Blosc2 VL-block chunk.
///
/// Each VL block is filtered and compressed independently with block offset 0.
pub fn compress_vl_blocks(blocks: &[&[u8]], cparams: &CParams) -> Result<Vec<u8>, &'static str> {
    compress_vl_blocks_with_output_limit(blocks, cparams, None)
}

/// Backwards-compatible alias for [`compress_vl_blocks`].
pub fn vlcompress(blocks: &[&[u8]], cparams: &CParams) -> Result<Vec<u8>, &'static str> {
    compress_vl_blocks(blocks, cparams)
}

fn compress_vl_blocks_with_output_limit(
    blocks: &[&[u8]],
    cparams: &CParams,
    output_limit: Option<usize>,
) -> Result<Vec<u8>, &'static str> {
    let total_nbytes = validate_vl_inputs(blocks, cparams)?;
    let normalized_cparams = normalized_cparams(cparams);
    let cparams = &normalized_cparams;
    let max_blocksize = blocks.iter().map(|block| block.len()).max().unwrap_or(1);
    let header_len = BLOSC_EXTENDED_HEADER_LENGTH;
    let bstarts_len = blocks.len() * 4;
    let table_end = header_len
        .checked_add(bstarts_len)
        .ok_or("Invalid VL-block table size")?;
    check_output_budget(table_end, output_limit)?;

    let mut flags = BLOSC_DOSHUFFLE | BLOSC_DOBITSHUFFLE;
    flags |= compcode_to_compformat(cparams.compcode) << 5;
    flags |= compute_filter_flags(&cparams.filters) & BLOSC_DODELTA;
    flags |= BLOSC_DONT_SPLIT;

    let can_use_dict = cparams.use_dict
        && cparams.clevel > 0
        && blocks.len() >= 8
        && total_nbytes / blocks.len() >= 16;
    let filtered_blocks = if can_use_dict {
        Some(filtered_vl_blocks(blocks, cparams, max_blocksize, 0)?)
    } else {
        None
    };
    let dict = filtered_blocks.as_ref().and_then(|filtered| {
        let training_buffer_len = total_nbytes;
        let training_buffer = dictionary_training_buffer(filtered, training_buffer_len)?;
        let zstd_sample_sizes: Vec<usize> = filtered.iter().map(Vec::len).collect();
        build_codec_dictionary(
            &training_buffer,
            total_nbytes,
            cparams.compcode,
            total_nbytes,
            &zstd_sample_sizes,
        )
    });
    let dict = dict.as_deref();

    let compressed_blocks: Vec<Vec<u8>> = match (filtered_blocks.as_ref(), dict) {
        (Some(filtered_blocks), Some(dict)) if cparams.nthreads > 1 && blocks.len() > 1 => {
            with_thread_pool(effective_nthreads(cparams.nthreads, blocks.len()), || {
                filtered_blocks
                    .par_iter()
                    .enumerate()
                    .map(|(idx, block)| {
                        compress_filtered_vl_block(
                            block,
                            std::ptr::null(),
                            cparams,
                            max_blocksize,
                            idx as i32,
                            Some(dict),
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()
            })?
        }
        (Some(filtered_blocks), Some(dict)) => filtered_blocks
            .iter()
            .enumerate()
            .map(|(idx, block)| {
                compress_filtered_vl_block(
                    block,
                    std::ptr::null(),
                    cparams,
                    max_blocksize,
                    idx as i32,
                    Some(dict),
                )
            })
            .collect::<Result<Vec<_>, _>>()?,
        _ if cparams.nthreads > 1 && blocks.len() > 1 => {
            with_thread_pool(effective_nthreads(cparams.nthreads, blocks.len()), || {
                blocks
                    .par_iter()
                    .enumerate()
                    .map(|(idx, block)| {
                        compress_vl_block(
                            block,
                            cparams,
                            max_blocksize,
                            idx as i32,
                            rayon::current_thread_index().unwrap_or(0) as i32,
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()
            })?
        }
        _ => blocks
            .iter()
            .enumerate()
            .map(|(idx, block)| compress_vl_block(block, cparams, max_blocksize, idx as i32, 0))
            .collect::<Result<Vec<_>, _>>()?,
    };

    let dict_section_len = dict.map_or(0, |dict| 4 + dict.len());
    check_output_budget(
        table_end
            .checked_add(dict_section_len)
            .ok_or("Invalid dictionary size")?,
        output_limit,
    )?;

    let total_cbytes = compressed_blocks.iter().try_fold(
        header_len + bstarts_len + dict_section_len,
        |acc, block| {
            acc.checked_add(block.len())
                .ok_or("VL-block chunk too large")
        },
    )?;
    if total_cbytes > i32::MAX as usize {
        return Err("VL-block chunk too large");
    }
    check_output_budget(total_cbytes, output_limit)?;

    let mut output = vec![0u8; total_cbytes];
    let mut output_pos = header_len + bstarts_len;
    if let Some(dict) = dict {
        output[output_pos..output_pos + 4].copy_from_slice(&(dict.len() as i32).to_le_bytes());
        output_pos += 4;
        output[output_pos..output_pos + dict.len()].copy_from_slice(dict);
        output_pos += dict.len();
    }
    for (idx, block) in compressed_blocks.iter().enumerate() {
        let bstart_offset = header_len + idx * 4;
        output[bstart_offset..bstart_offset + 4]
            .copy_from_slice(&(output_pos as i32).to_le_bytes());
        output[output_pos..output_pos + block.len()].copy_from_slice(block);
        output_pos += block.len();
    }

    let header = ChunkHeader {
        version: BLOSC2_VERSION_FORMAT_VL_BLOCKS,
        versionlz: codec_version_for_header(cparams.compcode),
        flags,
        typesize: cparams.typesize as u8,
        nbytes: total_nbytes as i32,
        blocksize: blocks.len() as i32,
        cbytes: total_cbytes as i32,
        filters: cparams.filters,
        filters_meta: cparams.filters_meta,
        udcompcode: udcompcode_for_header(cparams.compcode),
        compcode_meta: cparams.compcode_meta,
        blosc2_flags: if dict.is_some() { BLOSC2_USEDICT } else { 0 },
        blosc2_flags2: BLOSC2_VL_BLOCKS,
    };
    header.try_write(&mut output[..BLOSC_EXTENDED_HEADER_LENGTH])?;
    Ok(output)
}

/// Decompress a single block from chunk data. Returns decompressed block bytes.
#[allow(clippy::too_many_arguments)]
fn decompress_block_data(
    chunk: &[u8],
    block_idx: usize,
    block_start: usize,
    bsize: usize,
    blocksize: usize,
    is_leftover: bool,
    header: &ChunkHeader,
    dref: Option<&[u8]>,
    dict: Option<&[u8]>,
    dparams: &DParams,
) -> Result<Vec<u8>, &'static str> {
    let typesize = header.typesize as usize;
    let dont_split = header.dont_split();
    let compcode = header.compcode();
    let header_len = header.header_len();
    let chunk_limit = header.cbytes as usize;
    let nblocks = if header.vl_blocks() {
        header.blocksize as usize
    } else {
        header.nblocks()
    };

    // Read block start offset
    let bstart_pos = header_len + block_idx * 4;
    let bstart_end = bstart_pos
        .checked_add(4)
        .ok_or("Invalid block table offset")?;
    if bstart_end > chunk_limit {
        return Err("Chunk too small for bstarts");
    }
    let src_pos_i32 = i32::from_le_bytes(chunk[bstart_pos..bstart_end].try_into().unwrap());
    if src_pos_i32 < 0 {
        return Err("Invalid negative block offset");
    }
    let mut src_pos = src_pos_i32 as usize;
    if src_pos > chunk_limit {
        return Err("Invalid block offset");
    }
    if let Some(dict) = dict {
        let min_block_start = header_len
            .checked_add(nblocks.checked_mul(4).ok_or("Invalid block table size")?)
            .and_then(|pos| pos.checked_add(4))
            .and_then(|pos| pos.checked_add(dict.len()))
            .ok_or("Invalid dictionary size")?;
        if src_pos < min_block_start {
            return Err("Invalid dictionary block offset");
        }
    }

    let block_limit = compressed_block_limit(chunk, header, src_pos, nblocks)?;

    let nstreams = stream_count(dont_split, is_leftover, typesize, bsize);
    let neblock = bsize / nstreams;

    let mut buf1 = vec![0u8; bsize];
    let mut buf2 = vec![0u8; bsize];

    // Decompress each stream into buf1
    for stream_idx in 0..nstreams {
        let dest_start = stream_idx * neblock;

        let stream_size_end = src_pos.checked_add(4).ok_or("Invalid stream size offset")?;
        if stream_size_end > block_limit {
            return Err("Chunk truncated reading stream size");
        }
        let cbytes = i32::from_le_bytes(chunk[src_pos..stream_size_end].try_into().unwrap());
        src_pos = stream_size_end;

        if cbytes == 0 {
            buf1[dest_start..dest_start + neblock].fill(0);
        } else if cbytes < 0 {
            if cbytes < -255 {
                return Err("Invalid run encoding");
            }
            let val = (-cbytes) as u8;
            if src_pos < block_limit && chunk[src_pos] & 0x01 != 0 {
                buf1[dest_start..dest_start + neblock].fill(val);
                src_pos += 1;
            } else {
                return Err("Invalid run encoding");
            }
        } else if cbytes as usize == neblock {
            let block_end = src_pos
                .checked_add(neblock)
                .ok_or("Invalid memcpyed block size")?;
            if block_end > block_limit {
                return Err("Chunk truncated reading memcpyed block");
            }
            buf1[dest_start..dest_start + neblock].copy_from_slice(&chunk[src_pos..block_end]);
            src_pos = block_end;
        } else {
            let block_end = src_pos
                .checked_add(cbytes as usize)
                .ok_or("Invalid compressed block size")?;
            if block_end > block_limit {
                return Err("Chunk truncated reading compressed block");
            }
            let cdata = &chunk[src_pos..block_end];
            let codec_dparams = codec_dparams_context(dparams);
            let dsize = match dict {
                Some(dict) => codecs::decompress_block_with_dict(
                    compcode,
                    cdata,
                    &mut buf1[dest_start..dest_start + neblock],
                    dict,
                ),
                None => codecs::decompress_block_with_context(
                    compcode,
                    header.compcode_meta,
                    cdata,
                    &mut buf1[dest_start..dest_start + neblock],
                    Some(codecs::CodecCallbackContext {
                        compcode,
                        complib: None,
                        meta: header.compcode_meta,
                        clevel: 0,
                        cparams: None,
                        dparams: Some(&codec_dparams),
                        chunk: codecs::CodecChunkContext {
                            schunk: dparams.schunk,
                            nchunk: dparams.nchunk,
                            nblock: block_idx as i32,
                            chunk_source: chunk.as_ptr() as usize,
                            block_offset: block_start,
                            blocksize,
                            bsize,
                        },
                        b2nd_metalayer: dparams.b2nd_metalayer.as_deref(),
                        user_data: dparams.postfilter_user_data,
                    }),
                ),
            };
            if dsize < 0 || dsize as usize != neblock {
                return Err("Codec decompression failed");
            }
            src_pos += cbytes as usize;
        }
    }
    if src_pos != block_limit {
        return Err("Invalid block stream length");
    }

    // Apply backward filter pipeline
    let dref_end = blocksize.min(dref.map_or(0, |d| d.len()));
    let actual_dref = dref.map(|d| &d[..dref_end]);
    let filter_dparams = filter_dparams_context(dparams);
    let result_buf = filters::apply_filter_pipeline_for_decompression_with_context(
        &mut buf1[..bsize],
        &mut buf2[..bsize],
        bsize,
        &header.filters,
        &header.filters_meta,
        header.version,
        typesize,
        block_start,
        actual_dref,
        1,
        Some(filters::FilterPipelineContext {
            cparams: None,
            dparams: Some(&filter_dparams),
            chunk: filters::FilterChunkContext {
                schunk: dparams.schunk,
                nchunk: dparams.nchunk,
                nblock: block_idx as i32,
                block_offset: block_start,
                blocksize,
                bsize,
            },
            b2nd_metalayer: dparams.b2nd_metalayer.as_deref(),
            user_data: dparams.postfilter_user_data,
        }),
    );

    let result = if result_buf == 1 {
        &buf1[..bsize]
    } else if result_buf == 2 {
        &buf2[..bsize]
    } else {
        return Err("Filter pipeline failed");
    };
    let mut out = vec![0u8; result.len()];
    apply_postfilter(dparams, result, &mut out, block_start, block_idx, 0)?;
    Ok(out)
}

/// Decompress a single block directly into `dest`, reusing caller-provided scratch buffers.
///
/// Applies the backward filter pipeline and optional postfilter. `dref` carries the
/// decoded contents of block 0 when delta filtering is active.
#[allow(clippy::too_many_arguments)]
fn decompress_block_into(
    chunk: &[u8],
    block_idx: usize,
    block_start: usize,
    dest: &mut [u8],
    blocksize: usize,
    is_leftover: bool,
    header: &ChunkHeader,
    dref: Option<&[u8]>,
    dict: Option<&[u8]>,
    dparams: &DParams,
    scratch1: &mut [u8],
    scratch2: &mut [u8],
    tid: i32,
) -> Result<(), &'static str> {
    let bsize = dest.len();
    if scratch1.len() < bsize || scratch2.len() < bsize {
        return Err("Scratch buffer too small");
    }

    let typesize = header.typesize as usize;
    let dont_split = header.dont_split();
    let compcode = header.compcode();
    let header_len = header.header_len();
    let chunk_limit = header.cbytes as usize;
    let nblocks = if header.vl_blocks() {
        header.blocksize as usize
    } else {
        header.nblocks()
    };

    let bstart_pos = header_len + block_idx * 4;
    let bstart_end = bstart_pos
        .checked_add(4)
        .ok_or("Invalid block table offset")?;
    if bstart_end > chunk_limit {
        return Err("Chunk too small for bstarts");
    }
    let src_pos_i32 = i32::from_le_bytes(chunk[bstart_pos..bstart_end].try_into().unwrap());
    if src_pos_i32 < 0 {
        return Err("Invalid negative block offset");
    }
    let mut src_pos = src_pos_i32 as usize;
    if src_pos > chunk_limit {
        return Err("Invalid block offset");
    }
    if let Some(dict) = dict {
        let min_block_start = header_len
            .checked_add(nblocks.checked_mul(4).ok_or("Invalid block table size")?)
            .and_then(|pos| pos.checked_add(4))
            .and_then(|pos| pos.checked_add(dict.len()))
            .ok_or("Invalid dictionary size")?;
        if src_pos < min_block_start {
            return Err("Invalid dictionary block offset");
        }
    }

    let block_limit = compressed_block_limit(chunk, header, src_pos, nblocks)?;

    let nstreams = stream_count(dont_split, is_leftover, typesize, bsize);
    let neblock = bsize / nstreams;
    let filters_are_noop =
        filters_effectively_noop(&header.filters, &header.filters_meta, typesize);
    let single_shuffle = single_shuffle_filter(&header.filters, &header.filters_meta, typesize);

    let filtered = if filters_are_noop {
        &mut dest[..bsize]
    } else {
        &mut scratch1[..bsize]
    };

    for stream_idx in 0..nstreams {
        let dest_start = stream_idx * neblock;

        let stream_size_end = src_pos.checked_add(4).ok_or("Invalid stream size offset")?;
        if stream_size_end > block_limit {
            return Err("Chunk truncated reading stream size");
        }
        let cbytes = i32::from_le_bytes(chunk[src_pos..stream_size_end].try_into().unwrap());
        src_pos = stream_size_end;

        if cbytes == 0 {
            filtered[dest_start..dest_start + neblock].fill(0);
        } else if cbytes < 0 {
            if cbytes < -255 {
                return Err("Invalid run encoding");
            }
            let val = (-cbytes) as u8;
            if src_pos < block_limit && chunk[src_pos] & 0x01 != 0 {
                filtered[dest_start..dest_start + neblock].fill(val);
                src_pos += 1;
            } else {
                return Err("Invalid run encoding");
            }
        } else if cbytes as usize == neblock {
            let block_end = src_pos
                .checked_add(neblock)
                .ok_or("Invalid memcpyed block size")?;
            if block_end > block_limit {
                return Err("Chunk truncated reading memcpyed block");
            }
            filtered[dest_start..dest_start + neblock].copy_from_slice(&chunk[src_pos..block_end]);
            src_pos = block_end;
        } else {
            let block_end = src_pos
                .checked_add(cbytes as usize)
                .ok_or("Invalid compressed block size")?;
            if block_end > block_limit {
                return Err("Chunk truncated reading compressed block");
            }
            let cdata = &chunk[src_pos..block_end];
            let codec_dparams = codec_dparams_context(dparams);
            let dsize = match dict {
                Some(dict) => codecs::decompress_block_with_dict(
                    compcode,
                    cdata,
                    &mut filtered[dest_start..dest_start + neblock],
                    dict,
                ),
                None => codecs::decompress_block_with_context(
                    compcode,
                    header.compcode_meta,
                    cdata,
                    &mut filtered[dest_start..dest_start + neblock],
                    Some(codecs::CodecCallbackContext {
                        compcode,
                        complib: None,
                        meta: header.compcode_meta,
                        clevel: 0,
                        cparams: None,
                        dparams: Some(&codec_dparams),
                        chunk: codecs::CodecChunkContext {
                            schunk: dparams.schunk,
                            nchunk: dparams.nchunk,
                            nblock: block_idx as i32,
                            chunk_source: chunk.as_ptr() as usize,
                            block_offset: block_start,
                            blocksize,
                            bsize,
                        },
                        b2nd_metalayer: dparams.b2nd_metalayer.as_deref(),
                        user_data: dparams.postfilter_user_data,
                    }),
                ),
            };
            if dsize < 0 || dsize as usize != neblock {
                return Err("Codec decompression failed");
            }
            src_pos += cbytes as usize;
        }
    }
    if src_pos != block_limit {
        return Err("Invalid block stream length");
    }

    if filters_are_noop {
        if dparams.postfilter.is_some() {
            let input = dest.to_vec();
            apply_postfilter(dparams, &input, dest, block_start, block_idx, tid)?;
        }
        return Ok(());
    }

    if let Some(shuffle_typesize) = single_shuffle {
        filters::unshuffle(shuffle_typesize, &scratch1[..bsize], dest);
        if dparams.postfilter.is_some() {
            let input = dest.to_vec();
            apply_postfilter(dparams, &input, dest, block_start, block_idx, tid)?;
        }
        return Ok(());
    }

    let dref_end = blocksize.min(dref.map_or(0, |d| d.len()));
    let actual_dref = dref.map(|d| &d[..dref_end]);
    let filter_dparams = filter_dparams_context(dparams);
    let result_buf = filters::apply_filter_pipeline_for_decompression_with_context(
        &mut scratch1[..bsize],
        &mut scratch2[..bsize],
        bsize,
        &header.filters,
        &header.filters_meta,
        header.version,
        typesize,
        block_start,
        actual_dref,
        1,
        Some(filters::FilterPipelineContext {
            cparams: None,
            dparams: Some(&filter_dparams),
            chunk: filters::FilterChunkContext {
                schunk: dparams.schunk,
                nchunk: dparams.nchunk,
                nblock: block_idx as i32,
                block_offset: block_start,
                blocksize,
                bsize,
            },
            b2nd_metalayer: dparams.b2nd_metalayer.as_deref(),
            user_data: dparams.postfilter_user_data,
        }),
    );

    let input = match result_buf {
        1 => &scratch1[..bsize],
        2 => &scratch2[..bsize],
        _ => return Err("Filter pipeline failed"),
    };
    apply_postfilter(dparams, input, dest, block_start, block_idx, tid)?;
    Ok(())
}

/// Extract the embedded codec dictionary slice, or `Ok(None)` if the chunk does not carry one.
fn embedded_codec_dictionary<'a>(
    chunk: &'a [u8],
    header: &ChunkHeader,
) -> Result<Option<&'a [u8]>, &'static str> {
    if !header.use_dict() {
        return Ok(None);
    }
    let (dict_size_end, dict_end) = embedded_dictionary_span(chunk, header)?;
    Ok(Some(&chunk[dict_size_end..dict_end]))
}

fn embedded_payload_start(chunk: &[u8], header: &ChunkHeader) -> Result<usize, &'static str> {
    if !header.use_dict() {
        return Ok(header.header_len());
    }
    let (_, dict_end) = embedded_dictionary_span(chunk, header)?;
    match header.special_type() {
        BLOSC2_SPECIAL_ZERO | BLOSC2_SPECIAL_NAN | BLOSC2_SPECIAL_UNINIT => Ok(dict_end),
        _ => Ok(header.header_len()),
    }
}

fn embedded_dictionary_span(
    chunk: &[u8],
    header: &ChunkHeader,
) -> Result<(usize, usize), &'static str> {
    let nblocks = if header.special_type() != BLOSC2_NO_SPECIAL || header.memcpyed() {
        0
    } else if header.vl_blocks() {
        header.blocksize as usize
    } else {
        header.nblocks()
    };
    let dict_size_pos = header
        .header_len()
        .checked_add(nblocks.checked_mul(4).ok_or("Invalid block table size")?)
        .ok_or("Invalid dictionary offset")?;
    let dict_size_end = dict_size_pos
        .checked_add(4)
        .ok_or("Invalid dictionary offset")?;
    if dict_size_end > header.cbytes as usize || dict_size_end > chunk.len() {
        return Err("Chunk too small for dictionary size");
    }

    let dict_size = i32::from_le_bytes(chunk[dict_size_pos..dict_size_end].try_into().unwrap());
    if dict_size <= 0 || dict_size as usize > BLOSC2_MAXDICTSIZE {
        return Err("Invalid dictionary size");
    }
    let dict_end = dict_size_end
        .checked_add(dict_size as usize)
        .ok_or("Invalid dictionary size")?;
    if dict_end > header.cbytes as usize || dict_end > chunk.len() {
        return Err("Chunk too small for dictionary");
    }

    Ok((dict_size_end, dict_end))
}

/// Decompress a Blosc2 chunk into a freshly allocated buffer.
///
/// Reads the extended header, decodes every block, and applies the backward
/// filter pipeline. Returns the reconstructed bytes whose length equals the
/// `nbytes` recorded in the header. Single-threaded; for parallel decoding
/// use [`decompress_chunk_with_threads`] or [`decompress_chunk_with_dparams`].
pub fn decompress_chunk(chunk: &[u8]) -> Result<Vec<u8>, &'static str> {
    decompress_chunk_with_threads(chunk, 1)
}

/// Backwards-compatible alias for [`decompress_chunk`].
pub fn decompress(chunk: &[u8]) -> Result<Vec<u8>, &'static str> {
    decompress_chunk(chunk)
}

/// Decompress a Blosc2 chunk into a caller-supplied destination buffer.
///
/// `dest` must be at least as large as the chunk's `nbytes`. Returns the
/// number of bytes written, which equals `nbytes`.
pub fn decompress_chunk_into(chunk: &[u8], dest: &mut [u8]) -> Result<usize, &'static str> {
    decompress_chunk_into_with_threads(chunk, dest, 1)
}

/// Backwards-compatible alias for [`decompress_chunk_into`].
pub fn decompress_into(chunk: &[u8], dest: &mut [u8]) -> Result<usize, &'static str> {
    decompress_chunk_into(chunk, dest)
}

/// Return `(nbytes, cbytes, blocksize)` from a compressed chunk header.
pub fn chunk_sizes(chunk: &[u8]) -> Result<(usize, usize, usize), &'static str> {
    let header = normalize_cbuffer_header_for_query(ChunkHeader::read_minimal(chunk)?);
    validate_minimal_header(&header)?;

    Ok((
        header.nbytes as usize,
        header.cbytes as usize,
        header.blocksize as usize,
    ))
}

/// C-name alias for [`chunk_sizes`].
pub fn blosc1_cbuffer_sizes(chunk: &[u8]) -> Result<(usize, usize, usize), &'static str> {
    chunk_sizes(chunk)
}

/// Return `(typesize, compcode, filters)` from a compressed chunk header.
pub fn chunk_metainfo(chunk: &[u8]) -> Result<(usize, u8, [u8; BLOSC2_MAX_FILTERS]), &'static str> {
    let minimal = ChunkHeader::read_minimal(chunk)?;
    validate_minimal_header(&minimal)?;
    let header = if minimal.is_extended() {
        ChunkHeader::read(chunk)?
    } else {
        minimal
    };

    Ok((header.typesize as usize, header.compcode(), header.filters))
}

/// C-name alias for [`chunk_metainfo_flags`].
pub fn blosc1_cbuffer_metainfo(chunk: &[u8]) -> Result<(usize, u8), &'static str> {
    chunk_metainfo_flags(chunk)
}

/// Return Blosc1-style `(typesize, flags)` from the 16-byte chunk header prefix.
pub fn chunk_metainfo_flags(chunk: &[u8]) -> Result<(usize, u8), &'static str> {
    let header = ChunkHeader::read_minimal(chunk)?;
    validate_minimal_header(&header)?;
    Ok((header.typesize as usize, header.flags))
}

/// Return `(version, versionlz)` from the 16-byte chunk header prefix.
pub fn chunk_versions(chunk: &[u8]) -> Result<(u8, u8), &'static str> {
    let header = ChunkHeader::read_minimal(chunk)?;
    validate_minimal_header(&header)?;
    Ok((header.version, header.versionlz))
}

/// C-style `blosc2_cbuffer_sizes`: returns `(rc, nbytes, cbytes, blocksize)`.
///
/// On malformed 16-byte prefixes, C zeroes all outputs and returns the header
/// read error instead of propagating a Rust `Result`.
pub fn cbuffer_sizes_c(chunk: &[u8]) -> (i32, i32, i32, i32) {
    match read_cbuffer_query_header(chunk) {
        Ok(header) => (
            BLOSC2_ERROR_SUCCESS,
            header.nbytes,
            header.cbytes,
            header.blocksize,
        ),
        Err(err) => (cbuffer_header_error_code(err), 0, 0, 0),
    }
}

fn normalize_regular_header_blocksize(mut header: ChunkHeader) -> ChunkHeader {
    if !header.vl_blocks()
        && header.nbytes > 0
        && header.blocksize > header.nbytes
        && (header.blocksize as usize) <= BLOSC2_MAXBLOCKSIZE
    {
        header.blocksize = header.nbytes;
    }
    header
}

fn normalize_header_for_regular_decompression(mut header: ChunkHeader) -> ChunkHeader {
    header = normalize_regular_header_blocksize(header);
    if !header.is_extended() {
        if header.flags & BLOSC_DOBITSHUFFLE != 0 {
            header.filters[BLOSC2_MAX_FILTERS - 1] = BLOSC_BITSHUFFLE;
        } else if header.flags & BLOSC_DOSHUFFLE != 0 {
            header.filters[BLOSC2_MAX_FILTERS - 1] = BLOSC_SHUFFLE;
        }
        if header.flags & BLOSC_DODELTA != 0 {
            header.filters[BLOSC2_MAX_FILTERS - 2] = BLOSC_DELTA;
        }
    }
    header
}

fn normalize_cbuffer_header_for_query(header: ChunkHeader) -> ChunkHeader {
    normalize_regular_header_blocksize(header)
}

/// Exact C-name wrapper for [`cbuffer_sizes_c`].
pub fn blosc2_cbuffer_sizes(chunk: &[u8]) -> (i32, i32, i32, i32) {
    cbuffer_sizes_c(chunk)
}

/// C-style `blosc1_cbuffer_metainfo`: returns `(rc, typesize, flags)`.
pub fn cbuffer_metainfo_flags_c(chunk: &[u8]) -> (i32, usize, i32) {
    match read_cbuffer_query_header(chunk) {
        Ok(header) => (
            BLOSC2_ERROR_SUCCESS,
            header.typesize as usize,
            header.flags as i32,
        ),
        Err(err) => (cbuffer_header_error_code(err), 0, 0),
    }
}

/// Exact C-name wrapper for [`cbuffer_metainfo_flags_c`].
pub fn blosc2_cbuffer_metainfo(chunk: &[u8]) -> (i32, usize, i32) {
    cbuffer_metainfo_flags_c(chunk)
}

/// C-style Blosc2 metainfo: returns `(rc, typesize, compcode, filters)`.
pub fn blosc2_cbuffer_metainfo2_c(chunk: &[u8]) -> (i32, usize, i32, [u8; BLOSC2_MAX_FILTERS]) {
    match chunk_metainfo(chunk) {
        Ok((typesize, compcode, filters)) => {
            (BLOSC2_ERROR_SUCCESS, typesize, compcode as i32, filters)
        }
        Err(err) => (
            cbuffer_header_error_code(err),
            0,
            0,
            [0; BLOSC2_MAX_FILTERS],
        ),
    }
}

/// C-style `blosc2_cbuffer_versions`: returns `(rc, version, versionlz)`.
pub fn cbuffer_versions_c(chunk: &[u8]) -> (i32, i32, i32) {
    match read_cbuffer_query_header(chunk) {
        Ok(header) => (
            BLOSC2_ERROR_SUCCESS,
            header.version as i32,
            header.versionlz as i32,
        ),
        Err(err) => (cbuffer_header_error_code(err), 0, 0),
    }
}

/// Exact C-name wrapper for [`cbuffer_versions_c`].
pub fn blosc2_cbuffer_versions(chunk: &[u8]) -> (i32, i32, i32) {
    cbuffer_versions_c(chunk)
}

/// Return the compressor library/format name encoded in a chunk header prefix.
pub fn chunk_compressor_library(chunk: &[u8]) -> Option<&'static str> {
    let header = read_cbuffer_query_header(chunk).ok()?;
    compformat_to_complib_name((header.flags & 0xE0) >> 5)
}

/// C-name alias for [`chunk_compressor_library`].
pub fn blosc2_cbuffer_complib(chunk: &[u8]) -> Option<&'static str> {
    chunk_compressor_library(chunk)
}

/// Validate that a buffer contains a supported compressed chunk.
pub fn validate_chunk(chunk: &[u8]) -> Result<(), &'static str> {
    let header = normalize_regular_header_blocksize(ChunkHeader::read(chunk)?);
    validate_header(&header, chunk.len())?;
    if header.cbytes as usize != chunk.len() {
        return Err("Chunk size does not match header");
    }
    if header.use_dict() {
        let payload_start = embedded_payload_start(chunk, &header)?;
        match header.special_type() {
            BLOSC2_SPECIAL_VALUE => {
                let value_size = (header.cbytes as usize)
                    .checked_sub(payload_start)
                    .ok_or("Invalid special value size")?;
                let nbytes = header.nbytes as usize;
                if value_size == 0
                    || value_size > BLOSC2_MAXTYPESIZE
                    || (nbytes != 0 && value_size > nbytes)
                {
                    return Err("Invalid special value size");
                }
                if !nbytes.is_multiple_of(value_size) {
                    return Err("Invalid special value nbytes");
                }
            }
            BLOSC2_SPECIAL_NAN | BLOSC2_SPECIAL_ZERO | BLOSC2_SPECIAL_UNINIT => {
                if payload_start != header.cbytes as usize {
                    return Err("Invalid special chunk size");
                }
            }
            BLOSC2_NO_SPECIAL if header.memcpyed() => {
                let expected = payload_start
                    .checked_add(header.nbytes as usize)
                    .ok_or("Invalid memcpyed chunk size")?;
                if expected != header.cbytes as usize {
                    return Err("Invalid memcpyed chunk size");
                }
            }
            _ => {}
        }
    }
    if header.vl_blocks() {
        validate_vl_layout(chunk, &header)?;
    } else {
        validate_block_layout(chunk, &header)?;
    }
    Ok(())
}

/// Blosc1-style shallow chunk validation, returning the uncompressed byte size.
pub fn blosc1_cbuffer_validate(chunk: &[u8]) -> Result<usize, &'static str> {
    blosc1_cbuffer_validate_shallow(chunk).map_err(|err| match err {
        Blosc1ValidateError::ReadBuffer => "Chunk too small",
        Blosc1ValidateError::InvalidHeader => "Invalid chunk header",
        Blosc1ValidateError::MemoryAlloc => "Invalid chunk size",
    })?;
    let (nbytes, _, _) = chunk_sizes(chunk)?;
    Ok(nbytes)
}

enum Blosc1ValidateError {
    ReadBuffer,
    InvalidHeader,
    MemoryAlloc,
}

fn blosc1_cbuffer_validate_shallow(chunk: &[u8]) -> Result<(), Blosc1ValidateError> {
    let header = ChunkHeader::read_minimal(chunk).map_err(|err| match err {
        "Invalid cbytes" | "Invalid blocksize" | "Invalid typesize" => {
            Blosc1ValidateError::InvalidHeader
        }
        _ => Blosc1ValidateError::ReadBuffer,
    })?;
    if header.cbytes < 0 {
        return Err(Blosc1ValidateError::InvalidHeader);
    }
    if header.cbytes < BLOSC_MIN_HEADER_LENGTH as i32
        || header.blocksize <= 0
        || header.blocksize as usize > BLOSC2_MAXBLOCKSIZE
        || header.typesize == 0
    {
        return Err(Blosc1ValidateError::InvalidHeader);
    }
    if header.cbytes as usize != chunk.len() {
        return Err(Blosc1ValidateError::InvalidHeader);
    }
    if header.nbytes < 0 || header.nbytes as usize > BLOSC2_MAX_BUFFERSIZE as usize {
        return Err(Blosc1ValidateError::MemoryAlloc);
    }
    Ok(())
}

#[cfg(feature = "_ffi")]
fn ffi_zstd_dict_chunk(header: &ChunkHeader) -> bool {
    header.use_dict() && header.compcode() == BLOSC_ZSTD
}

#[cfg(feature = "_ffi")]
fn ffi_dparams(dparams: &DParams, typesize: i32) -> Blosc2DParamsFfi {
    Blosc2DParamsFfi {
        nthreads: dparams.nthreads,
        schunk: std::ptr::null_mut(),
        postfilter: std::ptr::null_mut(),
        postparams: std::ptr::null_mut(),
        typesize,
    }
}

#[cfg(feature = "_ffi")]
fn decompress_into_with_c_blosc2(
    chunk: &[u8],
    header: &ChunkHeader,
    dest: &mut [u8],
) -> Result<usize, &'static str> {
    let nbytes = header.nbytes as usize;
    if dest.len() < nbytes {
        return Err("Destination buffer too small");
    }
    let destsize = i32::try_from(dest.len()).map_err(|_| "Destination buffer too large")?;
    let written = unsafe {
        c_blosc2_decompress(
            chunk.as_ptr() as *const c_void,
            header.cbytes,
            dest.as_mut_ptr() as *mut c_void,
            destsize,
        )
    };
    if written < 0 || written as usize != nbytes {
        return Err("Codec decompression failed");
    }
    Ok(written as usize)
}

#[cfg(feature = "_ffi")]
fn vldecompress_with_c_blosc2(
    chunk: &[u8],
    header: &ChunkHeader,
    dparams: &DParams,
) -> Result<Vec<Vec<u8>>, &'static str> {
    let nblocks = header.blocksize as usize;
    let dctx = unsafe { c_blosc2_create_dctx(ffi_dparams(dparams, header.typesize as i32)) };
    if dctx.is_null() {
        return Err("Cannot create decompression context");
    }

    let mut dests = vec![std::ptr::null_mut::<c_void>(); nblocks];
    let mut sizes = vec![0i32; nblocks];
    let rc = unsafe {
        c_blosc2_vldecompress_ctx(
            dctx,
            chunk.as_ptr() as *const c_void,
            header.cbytes,
            dests.as_mut_ptr(),
            sizes.as_mut_ptr(),
            nblocks as i32,
        )
    };
    unsafe {
        c_blosc2_free_ctx(dctx);
    }
    if rc < 0 || rc as usize != nblocks {
        for ptr in dests {
            if !ptr.is_null() {
                unsafe { free(ptr) };
            }
        }
        return Err("Codec decompression failed");
    }

    let mut blocks = Vec::with_capacity(nblocks);
    if dests
        .iter()
        .zip(&sizes)
        .any(|(&ptr, &size)| ptr.is_null() || size < 0)
    {
        for ptr in dests {
            if !ptr.is_null() {
                unsafe { free(ptr) };
            }
        }
        return Err("Codec decompression failed");
    }
    for (ptr, size) in dests.into_iter().zip(sizes) {
        if ptr.is_null() || size < 0 {
            return Err("Codec decompression failed");
        }
        let block = unsafe { std::slice::from_raw_parts(ptr as *const u8, size as usize) }.to_vec();
        unsafe {
            free(ptr);
        }
        blocks.push(block);
    }
    Ok(blocks)
}

#[cfg(feature = "_ffi")]
fn vldecompress_block_with_c_blosc2(
    chunk: &[u8],
    header: &ChunkHeader,
    nblock: usize,
    dparams: &DParams,
) -> Result<Vec<u8>, &'static str> {
    let dctx = unsafe { c_blosc2_create_dctx(ffi_dparams(dparams, header.typesize as i32)) };
    if dctx.is_null() {
        return Err("Cannot create decompression context");
    }
    let mut ptr = std::ptr::null_mut::<u8>();
    let mut size = 0i32;
    let rc = unsafe {
        c_blosc2_vldecompress_block_ctx(
            dctx,
            chunk.as_ptr() as *const c_void,
            header.cbytes,
            nblock as i32,
            &mut ptr,
            &mut size,
        )
    };
    unsafe {
        c_blosc2_free_ctx(dctx);
    }
    if rc < 0 || ptr.is_null() || size < 0 {
        if !ptr.is_null() {
            unsafe {
                free(ptr as *mut c_void);
            }
        }
        return Err("Codec decompression failed");
    }
    let block = unsafe { std::slice::from_raw_parts(ptr, size as usize) }.to_vec();
    unsafe {
        free(ptr as *mut c_void);
    }
    Ok(block)
}

/// Locate the byte span (length-prefix + payload) of VL-block index `nblock` within a VL-block chunk.
fn vl_block_span<'a>(
    chunk: &'a [u8],
    header: &ChunkHeader,
    nblock: usize,
) -> Result<&'a [u8], &'static str> {
    let nblocks = header.blocksize as usize;
    if nblock >= nblocks {
        return Err("VL-block index out of range");
    }
    let header_len = header.header_len();
    let bstart_pos = header_len
        .checked_add(nblock.checked_mul(4).ok_or("Invalid VL-block table size")?)
        .ok_or("Invalid VL-block table offset")?;
    let bstart_end = bstart_pos
        .checked_add(4)
        .ok_or("Invalid VL-block table offset")?;
    if bstart_end > header.cbytes as usize || bstart_end > chunk.len() {
        return Err("Chunk too small for VL-block table");
    }
    let start_i32 = i32::from_le_bytes(chunk[bstart_pos..bstart_end].try_into().unwrap());
    if start_i32 < 0 {
        return Err("Invalid negative VL-block offset");
    }
    let start = start_i32 as usize;
    let mut min_start = header_len
        .checked_add(
            nblocks
                .checked_mul(4)
                .ok_or("Invalid VL-block table size")?,
        )
        .ok_or("Invalid VL-block table size")?;
    if header.use_dict() {
        let dict_size_end = min_start
            .checked_add(4)
            .ok_or("Invalid dictionary offset")?;
        if dict_size_end > header.cbytes as usize || dict_size_end > chunk.len() {
            return Err("Chunk too small for dictionary size");
        }
        let dict_size = i32::from_le_bytes(chunk[min_start..dict_size_end].try_into().unwrap());
        if dict_size <= 0 || dict_size as usize > BLOSC2_MAXDICTSIZE {
            return Err("Invalid dictionary size");
        }
        min_start = dict_size_end
            .checked_add(dict_size as usize)
            .ok_or("Invalid dictionary size")?;
    }
    if start < min_start || start > header.cbytes as usize || start > chunk.len() {
        return Err("Invalid VL-block offset");
    }

    let end = if nblock + 1 < nblocks {
        let next_pos = header_len + (nblock + 1) * 4;
        let next_end = next_pos + 4;
        if next_end > header.cbytes as usize || next_end > chunk.len() {
            return Err("Chunk too small for VL-block table");
        }
        let next_i32 = i32::from_le_bytes(chunk[next_pos..next_end].try_into().unwrap());
        if next_i32 < 0 {
            return Err("Invalid negative VL-block offset");
        }
        next_i32 as usize
    } else {
        header.cbytes as usize
    };
    if end <= start || end > header.cbytes as usize || end > chunk.len() {
        return Err("Invalid VL-block offset order");
    }
    Ok(&chunk[start..end])
}

fn vl_block_uncompressed_size(chunk: &[u8], nblock: usize) -> Result<usize, &'static str> {
    let header = normalize_regular_header_blocksize(ChunkHeader::read(chunk)?);
    validate_header(&header, chunk.len())?;
    if !header.vl_blocks() {
        return Err("Chunk does not use VL-blocks");
    }
    validate_vl_layout(chunk, &header)?;
    let span = vl_block_span(chunk, &header, nblock)?;
    if span.len() < 4 {
        return Err("VL-block span too small");
    }
    let bsize_i32 = i32::from_le_bytes(span[..4].try_into().unwrap());
    if bsize_i32 <= 0 {
        return Err("Invalid VL-block uncompressed size");
    }
    Ok(bsize_i32 as usize)
}

fn vl_max_block_nbytes(chunk: &[u8], header: &ChunkHeader) -> Result<usize, &'static str> {
    let nblocks = header.blocksize as usize;
    let mut max_nbytes = 0usize;
    for nblock in 0..nblocks {
        let span = vl_block_span(chunk, header, nblock)?;
        if span.len() < 4 {
            return Err("VL-block span too small");
        }
        let bsize_i32 = i32::from_le_bytes(span[..4].try_into().unwrap());
        if bsize_i32 <= 0 {
            return Err("Invalid VL-block uncompressed size");
        }
        max_nbytes = max_nbytes.max(bsize_i32 as usize);
    }
    Ok(max_nbytes)
}

/// Return the number of variable-length blocks in a VL-block chunk.
pub fn vl_chunk_block_count(chunk: &[u8]) -> Result<usize, &'static str> {
    let header = normalize_regular_header_blocksize(ChunkHeader::read(chunk)?);
    validate_header(&header, chunk.len())?;
    if !header.vl_blocks() {
        return Err("Chunk does not use VL-blocks");
    }
    validate_vl_layout(chunk, &header)?;
    Ok(header.blocksize as usize)
}

/// Backwards-compatible alias for [`vl_chunk_block_count`].
pub fn vlchunk_get_nblocks(chunk: &[u8]) -> Result<usize, &'static str> {
    vl_chunk_block_count(chunk)
}

/// C-style VL-block count query.
///
/// This mirrors C-Blosc2's header-only `blosc2_vlchunk_get_nblocks`: it does
/// not validate or walk the VL payload layout.
pub fn blosc2_vlchunk_get_nblocks_c(src: &[u8], srcsize: i32) -> (i32, i32) {
    let srcsize = match checked_c_chunk_src_len(srcsize, src.len()) {
        Ok(size) => size,
        Err(code) => return (code, 0),
    };
    let header = match ChunkHeader::read(&src[..srcsize]) {
        Ok(header) => header,
        Err(err) => return (blosc2_error_code(err), 0),
    };
    if !header.vl_blocks() {
        return (BLOSC2_ERROR_INVALID_PARAM, 0);
    }
    (BLOSC2_ERROR_SUCCESS, header.blocksize)
}

/// Decompress one block from a VL-block chunk.
pub fn decompress_vl_block(chunk: &[u8], nblock: usize) -> Result<Vec<u8>, &'static str> {
    decompress_vl_block_with_dparams(chunk, nblock, &DParams::default())
}

/// Backwards-compatible alias for [`decompress_vl_block`].
pub fn vldecompress_block(chunk: &[u8], nblock: usize) -> Result<Vec<u8>, &'static str> {
    decompress_vl_block(chunk, nblock)
}

/// Decompress a single VL block using the supplied decompression parameters.
pub(crate) fn decompress_vl_block_with_dparams(
    chunk: &[u8],
    nblock: usize,
    dparams: &DParams,
) -> Result<Vec<u8>, &'static str> {
    let header = normalize_regular_header_blocksize(ChunkHeader::read(chunk)?);
    validate_header(&header, chunk.len())?;
    let mut normalized_dparams = dparams.clone();
    normalized_dparams.typesize = header.typesize as i32;
    let dparams = &normalized_dparams;
    if !header.vl_blocks() {
        return Err("Chunk does not use VL-blocks");
    }
    validate_vl_layout(chunk, &header)?;
    let nblocks = header.blocksize as usize;
    let maskout = validated_block_maskout(dparams, nblocks)?;
    if nblock >= nblocks {
        return Err("VL-block index out of range");
    }
    let span = vl_block_span(chunk, &header, nblock)?;
    if span.len() < 4 {
        return Err("VL-block span too small");
    }
    let bsize_i32 = i32::from_le_bytes(span[..4].try_into().unwrap());
    if bsize_i32 <= 0 {
        return Err("Invalid VL-block uncompressed size");
    }
    let bsize = bsize_i32 as usize;
    if block_is_masked(maskout, nblock) {
        // C allocates the full VL block and leaves masked contents
        // indeterminate. Safe Rust returns deterministic zero bytes, while
        // preserving the C-visible block size.
        return Ok(vec![0; bsize]);
    }

    #[cfg(feature = "_ffi")]
    if ffi_zstd_dict_chunk(&header) && dparams.postfilter.is_none() {
        if let Ok(block) = vldecompress_block_with_c_blosc2(chunk, &header, nblock, dparams) {
            return Ok(block);
        }
    }

    let payload = &span[4..];
    let typesize = header.typesize as usize;
    let dict = embedded_codec_dictionary(chunk, &header)?;

    let mut filtered = vec![0u8; bsize];
    if payload.len() == bsize {
        filtered.copy_from_slice(payload);
    } else {
        let codec_dparams = codec_dparams_context(dparams);
        let dsize = match dict {
            Some(dict) => {
                codecs::decompress_block_with_dict(header.compcode(), payload, &mut filtered, dict)
            }
            None => codecs::decompress_block_with_context(
                header.compcode(),
                header.compcode_meta,
                payload,
                &mut filtered,
                Some(codecs::CodecCallbackContext {
                    compcode: header.compcode(),
                    complib: None,
                    meta: header.compcode_meta,
                    clevel: 0,
                    cparams: None,
                    dparams: Some(&codec_dparams),
                    chunk: codecs::CodecChunkContext {
                        schunk: dparams.schunk,
                        nchunk: dparams.nchunk,
                        nblock: nblock as i32,
                        chunk_source: chunk.as_ptr() as usize,
                        block_offset: 0,
                        blocksize: bsize,
                        bsize,
                    },
                    b2nd_metalayer: dparams.b2nd_metalayer.as_deref(),
                    user_data: dparams.postfilter_user_data,
                }),
            ),
        };
        if dsize < 0 || dsize as usize != bsize {
            return Err("Codec decompression failed");
        }
    }

    let mut scratch = vec![0u8; bsize];
    let filter_dparams = filter_dparams_context(dparams);
    let result_buf = filters::apply_filter_pipeline_for_decompression_with_context(
        &mut filtered,
        &mut scratch,
        bsize,
        &header.filters,
        &header.filters_meta,
        header.version,
        typesize,
        0,
        None,
        1,
        Some(filters::FilterPipelineContext {
            cparams: None,
            dparams: Some(&filter_dparams),
            chunk: filters::FilterChunkContext {
                schunk: dparams.schunk,
                nchunk: dparams.nchunk,
                nblock: nblock as i32,
                block_offset: 0,
                blocksize: bsize,
                bsize,
            },
            b2nd_metalayer: dparams.b2nd_metalayer.as_deref(),
            user_data: dparams.postfilter_user_data,
        }),
    );
    let input = if result_buf == 1 {
        &filtered[..]
    } else if result_buf == 2 {
        &scratch[..]
    } else {
        return Err("Filter pipeline failed");
    };
    let mut output = vec![0u8; bsize];
    let max_bsize = vl_max_block_nbytes(chunk, &header)?;
    let postfilter_offset = nblock
        .checked_mul(max_bsize)
        .ok_or("VL-block postfilter offset overflow")?;
    apply_postfilter(dparams, input, &mut output, postfilter_offset, nblock, 0)?;
    Ok(output)
}

/// Decompress a VL-block chunk into individual block buffers.
pub fn decompress_vl_blocks(chunk: &[u8]) -> Result<Vec<Vec<u8>>, &'static str> {
    decompress_vl_blocks_with_dparams(chunk, &DParams::default())
}

/// Backwards-compatible alias for [`decompress_vl_blocks`].
pub fn vldecompress(chunk: &[u8]) -> Result<Vec<Vec<u8>>, &'static str> {
    decompress_vl_blocks(chunk)
}

/// Decompress every VL block in `chunk` using the supplied decompression parameters.
fn decompress_vl_blocks_with_dparams(
    chunk: &[u8],
    dparams: &DParams,
) -> Result<Vec<Vec<u8>>, &'static str> {
    let header = normalize_regular_header_blocksize(ChunkHeader::read(chunk)?);
    validate_header(&header, chunk.len())?;
    if !header.vl_blocks() {
        return Err("Chunk does not use VL-blocks");
    }
    validate_vl_layout(chunk, &header)?;

    #[cfg(feature = "_ffi")]
    if ffi_zstd_dict_chunk(&header) && dparams.postfilter.is_none() {
        if let Ok(blocks) = vldecompress_with_c_blosc2(chunk, &header, dparams) {
            return Ok(blocks);
        }
    }

    let nblocks = header.blocksize as usize;
    validated_block_maskout(dparams, nblocks)?;
    (0..nblocks)
        .map(|nblock| decompress_vl_block_with_dparams(chunk, nblock, dparams))
        .collect()
}

/// Extract `nitems` logical items starting at `start` from a compressed chunk.
///
/// `start` and `nitems` are item counts, not byte offsets. Only the compressed
/// blocks intersecting the requested byte range are decompressed.
pub fn get_items(chunk: &[u8], start: usize, nitems: usize) -> Result<Vec<u8>, &'static str> {
    get_items_with_dparams(chunk, start, nitems, &DParams::default())
}

/// Backwards-compatible alias for [`get_items`].
pub fn getitem(chunk: &[u8], start: usize, nitems: usize) -> Result<Vec<u8>, &'static str> {
    get_items(chunk, start, nitems)
}

/// Extract `nitems` logical items using the supplied decompression parameters.
pub fn get_items_with_dparams(
    chunk: &[u8],
    start: usize,
    nitems: usize,
    dparams: &DParams,
) -> Result<Vec<u8>, &'static str> {
    let header = normalize_header_for_regular_decompression(ChunkHeader::read(chunk)?);
    validate_header(&header, chunk.len())?;
    let typesize = header.typesize as usize;
    if typesize == 0 {
        return Err("Invalid typesize");
    }

    let byte_len = nitems.checked_mul(typesize).ok_or("Item range overflow")?;
    if header.vl_blocks() {
        return Err("getitem is not supported for VL-block chunks");
    }
    if byte_len == 0 {
        return Ok(Vec::new());
    }
    let byte_start = start.checked_mul(typesize).ok_or("Item range overflow")?;
    let byte_end = byte_start
        .checked_add(byte_len)
        .ok_or("Item range overflow")?;
    if byte_end > header.nbytes as usize {
        return Err("Item range out of bounds");
    }

    let nbytes = header.nbytes as usize;
    let payload_start = embedded_payload_start(chunk, &header)?;
    let special = header.special_type();
    if special != BLOSC2_NO_SPECIAL {
        return getitem_special_with_dparams(
            chunk,
            &header,
            nbytes,
            payload_start,
            byte_start,
            byte_len,
            dparams,
        );
    }

    if dparams.postfilter.is_some() {
        let data = decompress_with_dparams(chunk, dparams)?;
        return Ok(data[byte_start..byte_end].to_vec());
    }

    if header.memcpyed() {
        let payload_start = payload_start
            .checked_add(byte_start)
            .ok_or("Item range overflow")?;
        let payload_end = payload_start
            .checked_add(byte_len)
            .ok_or("Item range overflow")?;
        if payload_end > header.cbytes as usize || payload_end > chunk.len() {
            return Err("Chunk too small for memcpyed data");
        }
        return Ok(chunk[payload_start..payload_end].to_vec());
    }

    let blocksize = header.blocksize as usize;
    if blocksize == 0 {
        return Err("Invalid blocksize");
    }
    let first_block = byte_start / blocksize;
    let last_block = (byte_end - 1) / blocksize;
    let nblocks = header.nblocks();
    if last_block >= nblocks {
        return Err("Item range out of bounds");
    }

    let dict = embedded_codec_dictionary(chunk, &header)?;
    let has_delta = header.filters.contains(&BLOSC_DELTA);
    let mut effective_dparams = dparams.clone();
    effective_dparams.typesize = i32::from(header.typesize);
    let block0_ref = if has_delta {
        let block0_end = blocksize.min(nbytes);
        Some(decompress_block_data(
            chunk,
            0,
            0,
            block0_end,
            blocksize,
            nblocks == 1 && block0_end < blocksize,
            &header,
            None,
            dict,
            &effective_dparams,
        )?)
    } else {
        None
    };

    let mut out = Vec::with_capacity(byte_len);
    for block_idx in first_block..=last_block {
        let block_start = block_idx * blocksize;
        let block_end = (block_start + blocksize).min(nbytes);
        let bsize = block_end - block_start;
        let is_leftover = block_idx == nblocks - 1 && bsize < blocksize;

        let block_data = if has_delta && block_idx == 0 {
            block0_ref
                .as_ref()
                .ok_or("Missing delta reference block")?
                .clone()
        } else {
            decompress_block_data(
                chunk,
                block_idx,
                block_start,
                bsize,
                blocksize,
                is_leftover,
                &header,
                block0_ref.as_deref(),
                dict,
                &effective_dparams,
            )?
        };

        let local_start = byte_start.saturating_sub(block_start);
        let local_end = byte_end.min(block_end) - block_start;
        out.extend_from_slice(&block_data[local_start..local_end]);
    }

    Ok(out)
}

fn item_range_byte_len(chunk: &[u8], start: usize, nitems: usize) -> Result<usize, &'static str> {
    let header = normalize_regular_header_blocksize(ChunkHeader::read(chunk)?);
    validate_header(&header, chunk.len())?;
    let typesize = header.typesize as usize;
    if typesize == 0 {
        return Err("Invalid typesize");
    }
    let byte_len = nitems.checked_mul(typesize).ok_or("Item range overflow")?;
    if header.vl_blocks() {
        return Err("getitem is not supported for VL-block chunks");
    }
    if byte_len == 0 {
        return Ok(0);
    }
    let byte_start = start.checked_mul(typesize).ok_or("Item range overflow")?;
    let byte_end = byte_start
        .checked_add(byte_len)
        .ok_or("Item range overflow")?;
    if byte_end > header.nbytes as usize {
        return Err("Item range out of bounds");
    }
    Ok(byte_len)
}

fn write_special_item_range(
    chunk: &[u8],
    header: &ChunkHeader,
    nbytes: usize,
    payload_start: usize,
    range_start: usize,
    dest: &mut [u8],
) -> Result<(), &'static str> {
    if header.special_type() != BLOSC2_SPECIAL_VALUE {
        return write_special_range(chunk, header, nbytes, payload_start, range_start, dest);
    }

    let stored_value_size = (header.cbytes as usize)
        .checked_sub(payload_start)
        .ok_or("Invalid special value size")?;
    let typesize = usize::from(header.typesize).min(stored_value_size);
    let value_end = payload_start
        .checked_add(typesize)
        .ok_or("Invalid special value size")?;
    if typesize == 0 || value_end > header.cbytes as usize || value_end > chunk.len() {
        return Err("Invalid special value size");
    }
    if !nbytes.is_multiple_of(typesize)
        || !range_start.is_multiple_of(typesize)
        || !dest.len().is_multiple_of(typesize)
    {
        return Err("Invalid special value nbytes");
    }

    let repeated = &chunk[payload_start..value_end];
    for item in dest.chunks_exact_mut(typesize) {
        item.copy_from_slice(repeated);
    }
    Ok(())
}

fn getitem_special_with_dparams(
    chunk: &[u8],
    header: &ChunkHeader,
    nbytes: usize,
    payload_start: usize,
    byte_start: usize,
    byte_len: usize,
    dparams: &DParams,
) -> Result<Vec<u8>, &'static str> {
    let byte_end = byte_start
        .checked_add(byte_len)
        .ok_or("Item range overflow")?;

    if dparams.postfilter.is_none() {
        let mut data = vec![0u8; byte_len];
        write_special_item_range(chunk, header, nbytes, payload_start, byte_start, &mut data)?;
        return Ok(data);
    }

    let blocksize = header.blocksize as usize;
    if blocksize == 0 {
        return Err("Invalid blocksize");
    }
    let first_block = byte_start / blocksize;
    let last_block = (byte_end - 1) / blocksize;
    let nblocks = header.nblocks();
    if last_block >= nblocks {
        return Err("Item range out of bounds");
    }

    let mut effective_dparams = dparams.clone();
    effective_dparams.typesize = i32::from(header.typesize);
    let mut out = Vec::with_capacity(byte_len);
    for block_idx in first_block..=last_block {
        let block_start = block_idx * blocksize;
        let block_end = (block_start + blocksize).min(nbytes);
        let bsize = block_end - block_start;
        let mut block = vec![0u8; bsize];
        write_special_item_range(
            chunk,
            header,
            nbytes,
            payload_start,
            block_start,
            &mut block,
        )?;
        let input = block.clone();
        apply_postfilter(
            &effective_dparams,
            &input,
            &mut block,
            block_start,
            block_idx,
            0,
        )?;

        let local_start = byte_start.saturating_sub(block_start);
        let local_end = byte_end.min(block_end) - block_start;
        out.extend_from_slice(&block[local_start..local_end]);
    }

    Ok(out)
}

/// Read every block's compressed payload byte range from the block-offset table.
///
/// `min_payload_start` is the smallest legal payload offset (header end plus block
/// table and any embedded dictionary). Used by [`replace_aligned_blocks`].
fn read_block_payload_spans(
    chunk: &[u8],
    header: &ChunkHeader,
    min_payload_start: usize,
) -> Result<Vec<std::ops::Range<usize>>, &'static str> {
    let nblocks = header.nblocks();
    let header_len = header.header_len();
    let chunk_limit = header.cbytes as usize;
    let mut spans = Vec::with_capacity(nblocks);

    for block_idx in 0..nblocks {
        let bstart_pos = header_len
            .checked_add(block_idx.checked_mul(4).ok_or("Invalid block table size")?)
            .ok_or("Invalid block table offset")?;
        let bstart_end = bstart_pos
            .checked_add(4)
            .ok_or("Invalid block table offset")?;
        if bstart_end > chunk_limit || bstart_end > chunk.len() {
            return Err("Chunk too small for bstarts");
        }

        let start_i32 = i32::from_le_bytes(chunk[bstart_pos..bstart_end].try_into().unwrap());
        if start_i32 < 0 {
            return Err("Invalid negative block offset");
        }
        let start = start_i32 as usize;
        if start < min_payload_start || start > chunk_limit || start > chunk.len() {
            return Err("Invalid block offset");
        }

        let end = compressed_block_limit(chunk, header, start, nblocks)?;

        if end <= start || end > chunk_limit || end > chunk.len() {
            return Err("Invalid block offset order");
        }
        spans.push(start..end);
    }

    Ok(spans)
}

/// Replace a byte range by recompressing only the compressed blocks it touches.
///
/// Returns `Ok(None)` when callers should fall back to a full chunk rewrite.
pub fn replace_aligned_blocks(
    chunk: &[u8],
    byte_start: usize,
    data: &[u8],
    cparams: &CParams,
) -> Result<Option<Vec<u8>>, &'static str> {
    let header = normalize_header_for_regular_decompression(ChunkHeader::read(chunk)?);
    validate_header(&header, chunk.len())?;
    if header.vl_blocks() {
        validate_vl_layout(chunk, &header)?;
    } else {
        validate_block_layout(chunk, &header)?;
    }
    if data.is_empty() {
        return Ok(Some(chunk.to_vec()));
    }

    let nbytes = header.nbytes as usize;
    let byte_end = byte_start
        .checked_add(data.len())
        .ok_or("Item range overflow")?;
    if byte_end > nbytes {
        return Err("Item range out of bounds");
    }
    if header.vl_blocks() || header.special_type() != BLOSC2_NO_SPECIAL {
        return Ok(None);
    }
    if cparams.prefilter.is_some() {
        return Ok(None);
    }

    let header_len = header.header_len();
    let payload_start = embedded_payload_start(chunk, &header)?;
    if header.memcpyed() {
        let payload_start = payload_start
            .checked_add(byte_start)
            .ok_or("Item range overflow")?;
        let payload_end = payload_start
            .checked_add(data.len())
            .ok_or("Item range overflow")?;
        if payload_end > header.cbytes as usize || payload_end > chunk.len() {
            return Err("Chunk too small for memcpyed data");
        }
        let mut updated = chunk.to_vec();
        updated[payload_start..payload_end].copy_from_slice(data);
        return Ok(Some(updated));
    }

    let blocksize = header.blocksize as usize;
    if blocksize == 0 {
        return Ok(None);
    }
    let first_block = byte_start / blocksize;
    let last_block = (byte_end - 1) / blocksize;

    let nblocks = header.nblocks();
    if last_block >= nblocks {
        return Err("Item range out of bounds");
    }
    if cparams.compcode != header.compcode()
        || normalized_typesize(cparams.typesize) as u8 != header.typesize
        || cparams.filters != header.filters
        || cparams.filters_meta != header.filters_meta
    {
        return Ok(None);
    }

    let has_delta = header.filters.contains(&BLOSC_DELTA);
    if has_delta && first_block == 0 {
        let block0_end = blocksize.min(nbytes);
        if byte_start != 0 || byte_end < block0_end || last_block + 1 < nblocks {
            return Ok(None);
        }
    }

    let dict = embedded_codec_dictionary(chunk, &header)?;
    let table_end = header_len
        .checked_add(nblocks.checked_mul(4).ok_or("Invalid block table size")?)
        .ok_or("Invalid block table size")?;
    let min_payload_start = table_end
        .checked_add(dict.map_or(0, |dict| 4 + dict.len()))
        .ok_or("Invalid dictionary size")?;
    let old_spans = read_block_payload_spans(chunk, &header, min_payload_start)?;

    let default_dparams = DParams::default();
    let delta_ref = if has_delta && first_block == 0 {
        Some(data[..blocksize.min(data.len())].to_vec())
    } else if has_delta {
        let block0_end = blocksize.min(nbytes);
        let block0 = decompress_block_data(
            chunk,
            0,
            0,
            block0_end,
            blocksize,
            nblocks == 1 && block0_end < blocksize,
            &header,
            Some(&vec![0u8; blocksize.min(nbytes)]),
            dict,
            &default_dparams,
        )?;
        Some(block0)
    } else {
        None
    };

    let mut block_payloads: Vec<Vec<u8>> = Vec::with_capacity(nblocks);
    let single_shuffle = single_shuffle_filter(
        &header.filters,
        &header.filters_meta,
        header.typesize as usize,
    );
    let mut buf1: Vec<u8> = Vec::new();
    let mut buf2: Vec<u8> = Vec::new();
    let mut compress_scratch: Vec<u8> = Vec::new();
    for (block_idx, old_span) in old_spans.iter().enumerate() {
        if block_idx < first_block || block_idx > last_block {
            block_payloads.push(chunk[old_span.clone()].to_vec());
            continue;
        }

        let block_start = block_idx * blocksize;
        let block_end = (block_start + blocksize).min(nbytes);
        let bsize = block_end - block_start;
        let is_leftover = block_idx == nblocks - 1 && bsize < blocksize;
        let mut block_data;
        let replacement_start = byte_start.max(block_start);
        let replacement_end = byte_end.min(block_end);
        let local_start = replacement_start - block_start;
        let local_end = replacement_end - block_start;

        if local_start == 0 && local_end == bsize {
            let data_start = replacement_start - byte_start;
            block_data = data[data_start..data_start + bsize].to_vec();
        } else {
            let old_block = decompress_block_data(
                chunk,
                block_idx,
                block_start,
                bsize,
                blocksize,
                is_leftover,
                &header,
                delta_ref.as_deref(),
                dict,
                &default_dparams,
            )?;
            block_data = old_block;
            let data_start = replacement_start - byte_start;
            block_data[local_start..local_end]
                .copy_from_slice(&data[data_start..data_start + (local_end - local_start)]);
        }

        let filtered = if let Some(shuffle_typesize) = single_shuffle {
            ensure_scratch_len_uninit(&mut buf1, bsize);
            filters::shuffle(shuffle_typesize, &block_data, &mut buf1[..bsize]);
            &buf1[..bsize]
        } else {
            buf1.resize(bsize, 0);
            buf2.resize(bsize, 0);
            let filter_cparams = filter_cparams_context(cparams, blocksize as i32);
            let filtered_buf = filters::apply_filter_pipeline_for_compression_with_context(
                &block_data,
                &mut buf1[..bsize],
                &mut buf2[..bsize],
                &header.filters,
                &header.filters_meta,
                header.typesize as usize,
                block_start,
                delta_ref.as_deref(),
                Some(filters::FilterPipelineContext {
                    cparams: Some(&filter_cparams),
                    dparams: None,
                    chunk: filters::FilterChunkContext {
                        schunk: cparams.schunk,
                        nchunk: cparams.nchunk,
                        nblock: block_idx as i32,
                        block_offset: block_start,
                        blocksize,
                        bsize,
                    },
                    b2nd_metalayer: cparams.b2nd_metalayer.as_deref(),
                    user_data: cparams.prefilter_user_data,
                }),
            );
            match filtered_buf {
                1 => &buf1[..bsize],
                2 => &buf2[..bsize],
                _ => return Err("Filter pipeline failed"),
            }
        };
        let (block_payload, _, _) = compress_pre_filtered_block_with_scratch(
            filtered,
            block_data.as_ptr(),
            cparams,
            header.dont_split(),
            header.typesize as usize,
            is_leftover,
            dict,
            block_start,
            blocksize,
            block_idx as i32,
            &mut compress_scratch,
            None,
        )?;
        block_payloads.push(block_payload);
    }

    let total_len = block_payloads
        .iter()
        .try_fold(min_payload_start, |acc, payload| {
            acc.checked_add(payload.len()).ok_or("Chunk too large")
        })?;
    if total_len > i32::MAX as usize {
        return Err("Chunk too large");
    }

    let mut output = vec![0u8; table_end];
    output[..header_len].copy_from_slice(&chunk[..header_len]);
    output.extend_from_slice(&chunk[table_end..min_payload_start]);

    for (block_idx, payload) in block_payloads.iter().enumerate() {
        let bstart_offset = header_len + block_idx * 4;
        let payload_offset = output.len() as i32;
        output[bstart_offset..bstart_offset + 4].copy_from_slice(&payload_offset.to_le_bytes());
        output.extend_from_slice(payload);
    }

    let mut updated_header = header;
    updated_header.cbytes = output.len() as i32;
    updated_header.try_write(&mut output[..header_len])?;
    Ok(Some(output))
}

/// Blosc1-style compression wrapper.
///
/// The codec defaults to the process-wide value set via [`blosc1_set_compressor`]
/// (initially `BLOSC_BLOSCLZ`). Caller arguments `clevel`, `doshuffle`, and
/// `typesize` can be overridden by the `BLOSC_CLEVEL`, `BLOSC_SHUFFLE`, and
/// `BLOSC_TYPESIZE` environment variables respectively; the `BLOSC_COMPRESSOR`
/// env var can override the codec using C's lowercase codec names.
///
/// `doshuffle` accepts `BLOSC_NOFILTER`, `BLOSC_SHUFFLE`, or
/// `BLOSC_BITSHUFFLE`. The compressed chunk is written into `dest`, and the
/// number of bytes written is returned.
pub fn blosc1_compress(
    clevel: u8,
    doshuffle: u8,
    typesize: i32,
    src: &[u8],
    dest: &mut [u8],
) -> Result<usize, &'static str> {
    blosc1_compress_i32(clevel as i32, doshuffle as i32, typesize, src, dest)
}

fn blosc1_compress_i32(
    clevel: i32,
    doshuffle: i32,
    typesize: i32,
    src: &[u8],
    dest: &mut [u8],
) -> Result<usize, &'static str> {
    let mut clevel = clevel;
    let mut doshuffle = doshuffle as u8;
    let mut typesize = typesize;
    let mut compcode = blosc1_get_compressor_code_i32();
    apply_blosc_env_overrides(&mut clevel, &mut doshuffle, &mut typesize, &mut compcode)?;
    blosc1_compress_i32_prepared(clevel, doshuffle, typesize, compcode, src, dest)
}

fn blosc1_compress_i32_prepared(
    clevel: i32,
    doshuffle: u8,
    typesize: i32,
    compcode: i32,
    src: &[u8],
    dest: &mut [u8],
) -> Result<usize, &'static str> {
    // Build the filter pipeline the way C's `build_filters` does: terminal
    // shuffle at slot 5 for recognized values, optional delta at slot 4 when
    // `g_delta` is set. Unknown `doshuffle` values are accepted as no-filter,
    // matching C's fall-through behavior.
    let mut filters = [0u8; BLOSC2_MAX_FILTERS];
    if doshuffle == BLOSC_BITSHUFFLE || (doshuffle == BLOSC_SHUFFLE && typesize > 1) {
        filters[BLOSC2_MAX_FILTERS - 1] = doshuffle;
    }
    if blosc2_get_delta() {
        filters[BLOSC2_MAX_FILTERS - 2] = BLOSC_DELTA;
    }

    let nolock = std::env::var_os("BLOSC_NOLOCK").is_some();
    let mut cctx_clevel = if nolock {
        (clevel as u8) as i32
    } else {
        clevel
    };
    let mut cctx_typesize = if nolock {
        (typesize as u8) as i32
    } else {
        typesize
    };
    if nolock {
        if let Ok(v) = std::env::var("BLOSC_CLEVEL") {
            let parsed = parse_c_strtol_prefix(&v);
            if parsed >= 0 {
                cctx_clevel = parsed.min(i32::MAX as i64) as i32;
            }
        }
        if let Ok(v) = std::env::var("BLOSC_TYPESIZE") {
            let parsed = parse_c_strtol_prefix(&v);
            if parsed > 0 {
                cctx_typesize = parsed.min(i32::MAX as i64) as i32;
            }
        }
        if cctx_typesize as usize > BLOSC_MAX_TYPESIZE {
            cctx_typesize = 1;
        }
    }
    if !(0..=9).contains(&cctx_clevel) {
        return Err("Invalid compression level");
    }
    if dest.len() < BLOSC2_MAX_OVERHEAD {
        return Err("Destination too small");
    }
    let cparams = CParams {
        compcode: compcode as u8,
        clevel: cctx_clevel as u8,
        typesize: cctx_typesize,
        blocksize: if nolock { 0 } else { blosc1_get_blocksize() },
        splitmode: blosc1_get_splitmode(),
        nthreads: blosc2_get_nthreads(),
        filters,
        ..Default::default()
    };
    let compat_headroom = if blosc1_compat_enabled() {
        BLOSC_EXTENDED_HEADER_LENGTH - BLOSC_MIN_HEADER_LENGTH
    } else {
        0
    };
    let compression_limit = Some(
        dest.len()
            .checked_add(compat_headroom)
            .ok_or("Destination too small")?,
    );
    let mut compressed = match compress_chunk_with_output_limit(src, &cparams, compression_limit) {
        Ok(compressed) => compressed,
        Err("Destination too small") => return Ok(0),
        Err(err) => return Err(err),
    };
    if blosc1_compat_enabled() {
        compressed = match convert_blosc1_compat_chunk_with_output_limit(
            &compressed,
            src,
            &cparams,
            Some(dest.len()),
        ) {
            Ok(compressed) => compressed,
            Err("Destination too small") => return Ok(0),
            Err(err) => return Err(err),
        };
    }
    if dest.len() < compressed.len() {
        return Ok(0);
    }
    dest[..compressed.len()].copy_from_slice(&compressed);
    Ok(compressed.len())
}

/// Blosc1-style decompression wrapper.
pub fn blosc1_decompress(src: &[u8], dest: &mut [u8]) -> Result<usize, &'static str> {
    let dparams = DParams {
        nthreads: apply_blosc_decompress_env_overrides()?,
        ..Default::default()
    };
    decompress_into_with_dparams(src, dest, &dparams)
}

/// Map this crate's string errors onto the closest C-Blosc2 error code.
///
/// The high-level Rust API intentionally keeps returning `Result`; this mapper is
/// only for C-style adapter functions whose successful return is a byte/count
/// value and whose failure return is a negative `BLOSC2_ERROR_*` code.
pub fn blosc2_error_code(err: &str) -> i32 {
    match err {
        "Destination too small" => BLOSC2_ERROR_WRITE_BUFFER,
        "Buffer too small for header"
        | "Chunk too small"
        | "Chunk too small for header"
        | "Chunk too small for block table"
        | "Chunk too small for compressed block"
        | "Chunk too small for dictionary"
        | "Chunk too small for dictionary size"
        | "Chunk too small for memcpyed data"
        | "Chunk too small for VL-block table"
        | "VL-block span too small" => BLOSC2_ERROR_READ_BUFFER,
        "Unsupported codec" | "Unsupported Blosc1 compressor code" => BLOSC2_ERROR_CODEC_SUPPORT,
        "Codec compression failed"
        | "Codec decompression failed"
        | "Dictionary compression is only supported for Zstd, LZ4, and LZ4HC" => {
            BLOSC2_ERROR_CODEC_PARAM
        }
        "Chunk truncated" => BLOSC2_ERROR_INVALID_HEADER,
        "Dictionary compression failed"
        | "Dictionary decompression failed"
        | "Invalid dictionary size"
        | "Invalid dictionary offset" => BLOSC2_ERROR_CODEC_DICT,
        "Filter pipeline failed"
        | "Execution of prefilter function failed"
        | "Unsupported filter"
        | "Global plugin filter is not supported"
        | "Invalid trunc_prec filter metadata" => BLOSC2_ERROR_FILTER_PIPELINE,
        "Execution of postfilter function failed" => BLOSC2_ERROR_POSTFILTER,
        "NaN special only valid for 4 or 8 byte types"
        | "Maskout length must match the number of blocks"
        | "Maskout is not supported for VL-block chunks" => BLOSC2_ERROR_DATA,
        "Input too large" | "VL-block input too large" | "Too many VL-blocks" => {
            BLOSC2_ERROR_MAX_BUFSIZE_EXCEEDED
        }
        "Invalid compression level" => BLOSC2_ERROR_CODEC_PARAM,
        "Unsupported Blosc1 shuffle mode"
        | "Invalid blocksize"
        | "Invalid split mode"
        | "Invalid splitmode"
        | "Invalid thread count"
        | "Invalid typesize"
        | "Item range out of bounds"
        | "Item range overflow"
        | "Chunk does not use VL-blocks"
        | "VL-block input cannot be empty"
        | "VL-blocks cannot be empty"
        | "getitem is not supported for VL-block chunks"
        | "VL-block index out of range" => BLOSC2_ERROR_INVALID_PARAM,
        err if err.contains("version") || err.contains("Version") => BLOSC2_ERROR_VERSION_SUPPORT,
        err if err.contains("out of bounds") || err.contains("overflow") => {
            BLOSC2_ERROR_INVALID_PARAM
        }
        err if err.contains("too small") => BLOSC2_ERROR_READ_BUFFER,
        err if err.contains("Invalid")
            || err.contains("Malformed")
            || err.contains("Unsupported")
            || err.contains("mismatch") =>
        {
            BLOSC2_ERROR_INVALID_HEADER
        }
        _ => BLOSC2_ERROR_FAILURE,
    }
}

fn usize_to_c_return(value: usize) -> i32 {
    i32::try_from(value).unwrap_or(BLOSC2_ERROR_2GB_LIMIT)
}

fn result_len_to_c(result: Result<usize, &'static str>) -> i32 {
    match result {
        Ok(value) => usize_to_c_return(value),
        Err(err) => blosc2_error_code(err),
    }
}

fn checked_c_buffer_len(size: i32, available: usize) -> Result<usize, i32> {
    let size = usize::try_from(size).map_err(|_| BLOSC2_ERROR_INVALID_PARAM)?;
    if size > available {
        return Err(BLOSC2_ERROR_INVALID_PARAM);
    }
    Ok(size)
}

fn checked_c_chunk_src_len(srcsize: i32, available: usize) -> Result<usize, i32> {
    if srcsize < 0 {
        return Err(BLOSC2_ERROR_READ_BUFFER);
    }
    let srcsize = srcsize as usize;
    if srcsize > available {
        return Err(BLOSC2_ERROR_INVALID_PARAM);
    }
    if srcsize < BLOSC_MIN_HEADER_LENGTH {
        return Err(BLOSC2_ERROR_READ_BUFFER);
    }
    Ok(srcsize)
}

fn checked_c_declared_chunk(src: &[u8], srcsize: i32) -> Result<&[u8], i32> {
    if srcsize < 0 {
        return Err(BLOSC2_ERROR_READ_BUFFER);
    }
    let declared = srcsize as usize;
    if declared < BLOSC_MIN_HEADER_LENGTH {
        return Err(BLOSC2_ERROR_READ_BUFFER);
    }
    let minimal = match ChunkHeader::read_minimal(src) {
        Ok(header) => header,
        Err(err) => return Err(cbuffer_header_error_code(err)),
    };
    if minimal.is_extended() && declared < BLOSC_EXTENDED_HEADER_LENGTH {
        return Err(BLOSC2_ERROR_READ_BUFFER);
    }
    let header = match ChunkHeader::read(src) {
        Ok(header) => header,
        Err(err) => return Err(cbuffer_header_error_code(err)),
    };
    if header.cbytes < BLOSC_MIN_HEADER_LENGTH as i32 {
        return Err(BLOSC2_ERROR_INVALID_HEADER);
    }
    let cbytes = header.cbytes as usize;
    if cbytes > declared {
        return Err(BLOSC2_ERROR_INVALID_HEADER);
    }
    if cbytes > src.len() {
        return Err(BLOSC2_ERROR_READ_BUFFER);
    }
    Ok(&src[..cbytes])
}

/// C-style compression adapter: returns bytes written, or a negative
/// `BLOSC2_ERROR_*` code on failure.
pub fn blosc1_compress_c(
    clevel: i32,
    doshuffle: i32,
    typesize: i32,
    src: &[u8],
    dest: &mut [u8],
) -> i32 {
    let mut clevel = clevel;
    let mut doshuffle = doshuffle as u8;
    let mut typesize = typesize;
    let mut compcode = blosc1_get_compressor_code_i32();
    if let Err(err) =
        apply_blosc_env_overrides(&mut clevel, &mut doshuffle, &mut typesize, &mut compcode)
    {
        return blosc2_error_code(err);
    }
    if dest.len() < BLOSC2_MAX_OVERHEAD {
        return BLOSC2_ERROR_MAX_BUFSIZE_EXCEEDED;
    }
    match blosc1_compress_i32_prepared(clevel, doshuffle, typesize, compcode, src, dest) {
        Ok(value) => usize_to_c_return(value),
        Err("Destination too small") => 0,
        Err(err) => blosc2_error_code(err),
    }
}

/// C-style non-context Blosc2 compression adapter with explicit buffer sizes.
pub fn blosc2_compress(
    clevel: i32,
    doshuffle: i32,
    typesize: i32,
    src: &[u8],
    srcsize: i32,
    dest: &mut [u8],
    destsize: i32,
) -> i32 {
    let mut clevel = clevel;
    let mut doshuffle = doshuffle as u8;
    let mut typesize = typesize;
    let mut compcode = blosc1_get_compressor_code_i32();
    if let Err(err) =
        apply_blosc_env_overrides(&mut clevel, &mut doshuffle, &mut typesize, &mut compcode)
    {
        return blosc2_error_code(err);
    }
    let srcsize = match checked_c_buffer_len(srcsize, src.len()) {
        Ok(size) => size,
        Err(code) => return code,
    };
    if destsize < 0 {
        return BLOSC2_ERROR_MAX_BUFSIZE_EXCEEDED;
    }
    let destsize = match checked_c_buffer_len(destsize, dest.len()) {
        Ok(size) => size,
        Err(code) => return code,
    };
    if destsize < BLOSC2_MAX_OVERHEAD {
        return BLOSC2_ERROR_MAX_BUFSIZE_EXCEEDED;
    }
    match blosc1_compress_i32_prepared(
        clevel,
        doshuffle,
        typesize,
        compcode,
        &src[..srcsize],
        &mut dest[..destsize],
    ) {
        Ok(value) => usize_to_c_return(value),
        Err("Destination too small") => 0,
        Err(err) => blosc2_error_code(err),
    }
}

/// C-style decompression adapter: returns bytes written, or a negative
/// `BLOSC2_ERROR_*` code on failure.
pub fn blosc1_decompress_c(src: &[u8], dest: &mut [u8]) -> i32 {
    result_len_to_c(blosc1_decompress(src, dest))
}

/// C-style non-context Blosc2 decompression adapter with explicit buffer sizes.
pub fn blosc2_decompress(src: &[u8], srcsize: i32, dest: &mut [u8], destsize: i32) -> i32 {
    if let Err(err) = apply_blosc_decompress_env_overrides() {
        return blosc2_error_code(err);
    }
    let chunk = match checked_c_declared_chunk(src, srcsize) {
        Ok(chunk) => chunk,
        Err(code) => return code,
    };
    if destsize < 0 {
        return BLOSC2_ERROR_WRITE_BUFFER;
    }
    let destsize = match checked_c_buffer_len(destsize, dest.len()) {
        Ok(size) => size,
        Err(code) => return code,
    };
    result_len_to_c(blosc1_decompress(chunk, &mut dest[..destsize]))
}

/// C-style chunk validation adapter: returns `BLOSC2_ERROR_SUCCESS` on success,
/// or a negative `BLOSC2_ERROR_*` code on failure.
pub fn cbuffer_validate_c(chunk: &[u8]) -> i32 {
    if chunk.len() < BLOSC_MIN_HEADER_LENGTH {
        return BLOSC2_ERROR_WRITE_BUFFER;
    }
    match blosc1_cbuffer_validate_shallow(chunk) {
        Ok(()) => BLOSC2_ERROR_SUCCESS,
        Err(Blosc1ValidateError::ReadBuffer) => BLOSC2_ERROR_READ_BUFFER,
        Err(Blosc1ValidateError::InvalidHeader) => BLOSC2_ERROR_INVALID_HEADER,
        Err(Blosc1ValidateError::MemoryAlloc) => BLOSC2_ERROR_MEMORY_ALLOC,
    }
}

/// C-style getitem adapter: writes into `dest` and returns bytes written, or a
/// negative `BLOSC2_ERROR_*` code on failure.
pub fn getitem_c(chunk: &[u8], start: i32, nitems: i32, dest: &mut [u8]) -> i32 {
    if nitems == 0 {
        return getitem_zero_items_c(chunk);
    }
    if nitems < 0 {
        let header = match ChunkHeader::read(chunk) {
            Ok(header) => header,
            Err(err) => return cbuffer_header_error_code(err),
        };
        if header.vl_blocks() {
            return blosc2_error_code("getitem is not supported for VL-block chunks");
        }
        let typesize = i32::from(header.typesize);
        let stop = start.wrapping_add(nitems);
        if start < 0 || start.saturating_mul(typesize) > header.nbytes {
            return BLOSC2_ERROR_INVALID_PARAM;
        }
        if stop < 0 || stop.saturating_mul(typesize) > header.nbytes {
            return BLOSC2_ERROR_INVALID_PARAM;
        }
        return 0;
    }
    if start < 0 {
        return BLOSC2_ERROR_INVALID_PARAM;
    }
    if let Some(required) = getitem_required_dest_len(chunk, nitems as usize) {
        if dest.len() < required {
            return BLOSC2_ERROR_WRITE_BUFFER;
        }
    }
    match getitem_special_uninit_len_c(chunk, start as usize, nitems as usize) {
        Ok(Some(len)) => return usize_to_c_return(len),
        Ok(None) => {}
        Err(err) => return blosc2_error_code(err),
    }
    match getitem(chunk, start as usize, nitems as usize) {
        Ok(items) => {
            if dest.len() < items.len() {
                return BLOSC2_ERROR_WRITE_BUFFER;
            }
            dest[..items.len()].copy_from_slice(&items);
            usize_to_c_return(items.len())
        }
        Err(err) => blosc2_error_code(err),
    }
}

fn getitem_zero_items_c(chunk: &[u8]) -> i32 {
    let header = match ChunkHeader::read(chunk) {
        Ok(header) => header,
        Err(err) => return cbuffer_header_error_code(err),
    };
    if header.vl_blocks() {
        return blosc2_error_code("getitem is not supported for VL-block chunks");
    }
    0
}

fn checked_getitem_c_header(chunk: &[u8]) -> Result<(), i32> {
    let header = ChunkHeader::read(chunk).map_err(cbuffer_header_error_code)?;
    if header.vl_blocks() {
        return Err(blosc2_error_code(
            "getitem is not supported for VL-block chunks",
        ));
    }
    Ok(())
}

fn getitem_required_dest_len(chunk: &[u8], nitems: usize) -> Option<usize> {
    let header = ChunkHeader::read_minimal(chunk).ok()?;
    validate_minimal_header(&header).ok()?;
    (header.typesize as usize).checked_mul(nitems)
}

fn getitem_special_uninit_len_c(
    chunk: &[u8],
    start: usize,
    nitems: usize,
) -> Result<Option<usize>, &'static str> {
    let header = normalize_header_for_regular_decompression(ChunkHeader::read(chunk)?);
    validate_header(&header, chunk.len())?;
    if header.vl_blocks() {
        return Err("getitem is not supported for VL-block chunks");
    }
    if header.special_type() != BLOSC2_SPECIAL_UNINIT {
        return Ok(None);
    }
    let typesize = header.typesize as usize;
    if typesize == 0 {
        return Err("Invalid typesize");
    }
    let byte_start = start.checked_mul(typesize).ok_or("Item range overflow")?;
    let byte_len = nitems.checked_mul(typesize).ok_or("Item range overflow")?;
    let byte_end = byte_start
        .checked_add(byte_len)
        .ok_or("Item range overflow")?;
    if byte_end > header.nbytes as usize {
        return Err("Item range out of bounds");
    }
    Ok(Some(byte_len))
}

/// C-style `blosc2_getitem` adapter with explicit source and destination sizes.
pub fn blosc2_getitem_c(
    chunk: &[u8],
    srcsize: i32,
    start: i32,
    nitems: i32,
    dest: &mut [u8],
    destsize: i32,
) -> i32 {
    let chunk = match checked_c_declared_chunk(chunk, srcsize) {
        Ok(chunk) => chunk,
        Err(code) => return code,
    };
    if let Err(code) = checked_getitem_c_header(chunk) {
        return code;
    }
    if nitems == 0 {
        return 0;
    }
    let header = match ChunkHeader::read(chunk) {
        Ok(header) => header,
        Err(_) => return BLOSC2_ERROR_INVALID_PARAM,
    };
    let typesize = header.typesize as i32;
    let stop = start.wrapping_add(nitems);
    if nitems.saturating_mul(typesize) > destsize {
        return BLOSC2_ERROR_WRITE_BUFFER;
    }
    if start < 0 || start.saturating_mul(typesize) > header.nbytes {
        return BLOSC2_ERROR_INVALID_PARAM;
    }
    if stop < 0 || stop.saturating_mul(typesize) > header.nbytes {
        return BLOSC2_ERROR_INVALID_PARAM;
    }
    if nitems < 0 {
        return 0;
    }
    if destsize < 0 {
        return BLOSC2_ERROR_WRITE_BUFFER;
    }
    let destsize = destsize as usize;
    if destsize > dest.len() {
        return BLOSC2_ERROR_INVALID_PARAM;
    }
    if let Some(required) = getitem_required_dest_len(chunk, nitems as usize) {
        if destsize < required {
            return BLOSC2_ERROR_WRITE_BUFFER;
        }
    }
    getitem_c(chunk, start, nitems, &mut dest[..destsize])
}

/// C-style `blosc2_getitem_ctx` adapter with explicit source and destination sizes.
pub fn blosc2_getitem_ctx_c(
    dctx: &DContext,
    chunk: &[u8],
    srcsize: i32,
    start: i32,
    nitems: i32,
    dest: &mut [u8],
    destsize: i32,
) -> i32 {
    let chunk = match checked_c_declared_chunk(chunk, srcsize) {
        Ok(chunk) => chunk,
        Err(code) => return code,
    };
    if let Err(code) = checked_getitem_c_header(chunk) {
        return code;
    }
    if nitems == 0 {
        return 0;
    }
    let header = match ChunkHeader::read(chunk) {
        Ok(header) => header,
        Err(_) => return BLOSC2_ERROR_INVALID_PARAM,
    };
    let typesize = header.typesize as i32;
    let stop = start.wrapping_add(nitems);
    if nitems.saturating_mul(typesize) > destsize {
        return BLOSC2_ERROR_WRITE_BUFFER;
    }
    if start < 0 || start.saturating_mul(typesize) > header.nbytes {
        return BLOSC2_ERROR_INVALID_PARAM;
    }
    if stop < 0 || stop.saturating_mul(typesize) > header.nbytes {
        return BLOSC2_ERROR_INVALID_PARAM;
    }
    if nitems < 0 {
        return 0;
    }
    if destsize < 0 {
        return BLOSC2_ERROR_WRITE_BUFFER;
    }
    let destsize = destsize as usize;
    if destsize > dest.len() {
        return BLOSC2_ERROR_INVALID_PARAM;
    }
    if let Some(required) = getitem_required_dest_len(chunk, nitems as usize) {
        if destsize < required {
            return BLOSC2_ERROR_WRITE_BUFFER;
        }
    }
    match getitem_special_uninit_len_c(chunk, start as usize, nitems as usize) {
        Ok(Some(len)) => return usize_to_c_return(len),
        Ok(None) => {}
        Err(err) => return blosc2_error_code(err),
    }
    result_len_to_c(dctx.get_items_into(
        chunk,
        start as usize,
        nitems as usize,
        &mut dest[..destsize],
    ))
}

/// Decompress a Blosc2 chunk using the specified number of threads.
pub fn decompress_chunk_with_threads(chunk: &[u8], nthreads: i16) -> Result<Vec<u8>, &'static str> {
    let dparams = DParams {
        nthreads,
        ..Default::default()
    };
    decompress_chunk_with_dparams(chunk, &dparams)
}

/// Backwards-compatible alias for [`decompress_chunk_with_threads`].
pub fn decompress_with_threads(chunk: &[u8], nthreads: i16) -> Result<Vec<u8>, &'static str> {
    decompress_chunk_with_threads(chunk, nthreads)
}

/// Decompress a Blosc2 chunk into a caller-provided destination buffer using the specified
/// number of threads. Returns the number of bytes written.
pub fn decompress_chunk_into_with_threads(
    chunk: &[u8],
    dest: &mut [u8],
    nthreads: i16,
) -> Result<usize, &'static str> {
    let dparams = DParams {
        nthreads,
        ..Default::default()
    };
    decompress_chunk_into_with_dparams(chunk, dest, &dparams)
}

/// Backwards-compatible alias for [`decompress_chunk_into_with_threads`].
pub fn decompress_into_with_threads(
    chunk: &[u8],
    dest: &mut [u8],
    nthreads: i16,
) -> Result<usize, &'static str> {
    decompress_chunk_into_with_threads(chunk, dest, nthreads)
}

/// Decompress a Blosc2 chunk using the supplied decompression parameters.
pub fn decompress_chunk_with_dparams(
    chunk: &[u8],
    dparams: &DParams,
) -> Result<Vec<u8>, &'static str> {
    if dparams.nthreads < 1 {
        return Err("Invalid thread count");
    }

    let header = normalize_header_for_regular_decompression(ChunkHeader::read(chunk)?);
    validate_header(&header, chunk.len())?;
    if header.vl_blocks() {
        validate_vl_layout(chunk, &header)?;
    } else {
        validate_block_layout(chunk, &header)?;
    }
    let nbytes = header.nbytes as usize;
    let mut output = decompression_output_buffer(
        nbytes,
        header.special_type() == BLOSC2_SPECIAL_UNINIT || dparams.block_maskout.is_some(),
    )?;
    let written = match decompress_chunk_into_with_header(chunk, &header, &mut output, dparams) {
        Ok(written) => written,
        #[cfg(feature = "_ffi")]
        Err(_) if ffi_zstd_dict_chunk(&header) && dparams.postfilter.is_none() => {
            decompress_into_with_c_blosc2(chunk, &header, &mut output)?
        }
        Err(err) => return Err(err),
    };
    debug_assert_eq!(written, nbytes);
    Ok(output)
}

/// Backwards-compatible alias for [`decompress_chunk_with_dparams`].
pub fn decompress_with_dparams(chunk: &[u8], dparams: &DParams) -> Result<Vec<u8>, &'static str> {
    decompress_chunk_with_dparams(chunk, dparams)
}

/// Decompress a Blosc2 chunk into a caller-provided destination buffer using the supplied
/// decompression parameters. Returns the number of bytes written.
pub fn decompress_chunk_into_with_dparams(
    chunk: &[u8],
    dest: &mut [u8],
    dparams: &DParams,
) -> Result<usize, &'static str> {
    if dparams.nthreads < 1 {
        return Err("Invalid thread count");
    }

    let header = normalize_header_for_regular_decompression(ChunkHeader::read(chunk)?);
    validate_header(&header, chunk.len())?;
    if header.vl_blocks() {
        validate_vl_layout(chunk, &header)?;
    } else {
        validate_block_layout(chunk, &header)?;
    }
    match decompress_chunk_into_with_header(chunk, &header, dest, dparams) {
        Ok(written) => Ok(written),
        #[cfg(feature = "_ffi")]
        Err(_) if ffi_zstd_dict_chunk(&header) && dparams.postfilter.is_none() => {
            decompress_into_with_c_blosc2(chunk, &header, dest)
        }
        Err(err) => Err(err),
    }
}

/// Backwards-compatible alias for [`decompress_chunk_into_with_dparams`].
pub fn decompress_into_with_dparams(
    chunk: &[u8],
    dest: &mut [u8],
    dparams: &DParams,
) -> Result<usize, &'static str> {
    decompress_chunk_into_with_dparams(chunk, dest, dparams)
}

fn validated_block_maskout(
    dparams: &DParams,
    nblocks: usize,
) -> Result<Option<&[bool]>, &'static str> {
    match dparams.block_maskout.as_deref() {
        Some(maskout) if maskout.len() != nblocks => {
            Err("Maskout length must match the number of blocks")
        }
        Some(maskout) => Ok(Some(maskout)),
        None => Ok(None),
    }
}

fn block_is_masked(maskout: Option<&[bool]>, block_idx: usize) -> bool {
    maskout.is_some_and(|maskout| maskout[block_idx])
}

fn apply_postfilter_to_unmasked_blocks(
    dparams: &DParams,
    dest: &mut [u8],
    nbytes: usize,
    blocksize: usize,
    maskout: Option<&[bool]>,
    tid: i32,
) -> Result<(), &'static str> {
    if maskout.is_none() {
        return apply_postfilter_to_blocks(dparams, dest, nbytes, blocksize, tid);
    }
    if dparams.postfilter.is_none() {
        return Ok(());
    }
    if blocksize == 0 {
        return Err("Invalid blocksize");
    }
    let maskout = maskout.unwrap();
    for (block_idx, &masked) in maskout.iter().enumerate() {
        if masked {
            continue;
        }
        let block_start = block_idx * blocksize;
        let block_end = (block_start + blocksize).min(nbytes);
        let input = dest[block_start..block_end].to_vec();
        apply_postfilter(
            dparams,
            &input,
            &mut dest[block_start..block_end],
            block_start,
            block_idx,
            tid,
        )?;
    }
    Ok(())
}

/// Decompress a chunk given its already-parsed header, writing the output into `dest`.
///
/// Selects the appropriate fast path (special chunks, memcpyed payload, VL blocks,
/// delta-sequential, or block-parallel decoding) based on the header.
fn decompress_chunk_into_with_header(
    chunk: &[u8],
    header: &ChunkHeader,
    dest: &mut [u8],
    dparams: &DParams,
) -> Result<usize, &'static str> {
    let mut normalized_dparams = dparams.clone();
    normalized_dparams.typesize = header.typesize as i32;
    let dparams = &normalized_dparams;
    let nbytes = header.nbytes as usize;
    if dest.len() < nbytes {
        return Err("Destination too small");
    }

    if nbytes == 0 {
        return Ok(0);
    }

    if header.vl_blocks() {
        let nblocks = header.blocksize as usize;
        let maskout = validated_block_maskout(dparams, nblocks)?;
        let mut output_len = 0usize;
        for nblock in 0..nblocks {
            let bsize = vl_block_uncompressed_size(chunk, nblock)?;
            let end = output_len
                .checked_add(bsize)
                .ok_or("VL-block sizes do not add up to chunk nbytes")?;
            if end > nbytes {
                return Err("VL-block sizes do not add up to chunk nbytes");
            }
            if block_is_masked(maskout, nblock) {
                output_len = end;
                continue;
            }
            let block = decompress_vl_block_with_dparams(chunk, nblock, dparams)?;
            if block.len() != bsize {
                return Err("VL-block sizes do not add up to chunk nbytes");
            }
            dest[output_len..end].copy_from_slice(&block);
            output_len = end;
        }
        if output_len != nbytes {
            return Err("VL-block sizes do not add up to chunk nbytes");
        }
        return Ok(output_len);
    }

    let blocksize = header.blocksize as usize;
    let nblocks = header.nblocks();
    let maskout = validated_block_maskout(dparams, nblocks)?;
    let payload_start = embedded_payload_start(chunk, header)?;

    // Handle special values
    let special = header.special_type();
    if special != BLOSC2_NO_SPECIAL {
        for block_idx in 0..nblocks {
            if block_is_masked(maskout, block_idx) {
                continue;
            }
            let block_start = block_idx * blocksize;
            let block_end = (block_start + blocksize).min(nbytes);
            write_special_range(
                chunk,
                header,
                nbytes,
                payload_start,
                block_start,
                &mut dest[block_start..block_end],
            )?;
        }
        apply_postfilter_to_unmasked_blocks(
            dparams,
            &mut dest[..nbytes],
            nbytes,
            blocksize,
            maskout,
            0,
        )?;
        return Ok(nbytes);
    }

    // Handle memcpyed chunks
    if header.memcpyed() {
        if chunk.len() >= payload_start + nbytes {
            let src = &chunk[payload_start..payload_start + nbytes];
            if let Some(maskout) = maskout {
                for (block_idx, &masked) in maskout.iter().enumerate() {
                    if masked {
                        continue;
                    }
                    let block_start = block_idx * blocksize;
                    let block_end = (block_start + blocksize).min(nbytes);
                    dest[block_start..block_end].copy_from_slice(&src[block_start..block_end]);
                }
                apply_postfilter_to_unmasked_blocks(
                    dparams,
                    &mut dest[..nbytes],
                    nbytes,
                    blocksize,
                    Some(maskout),
                    0,
                )?;
                return Ok(nbytes);
            }
            if should_parallelize_memcpyed(nbytes, dparams.nthreads) {
                let threads = memcpy_parallel_threads(nbytes, dparams.nthreads) as usize;
                let part_len = nbytes.div_ceil(threads);
                let src_addr = src.as_ptr() as usize;
                let dst_addr = dest.as_mut_ptr() as usize;
                with_thread_pool(threads as i16, || {
                    rayon::scope(|scope| {
                        for worker_idx in 0..threads {
                            let start = worker_idx * part_len;
                            if start >= nbytes {
                                break;
                            }
                            let end = (start + part_len).min(nbytes);
                            scope.spawn(move |_| unsafe {
                                // SAFETY: each worker copies a distinct
                                // contiguous subrange from `src` to `dest`.
                                std::ptr::copy_nonoverlapping(
                                    (src_addr as *const u8).add(start),
                                    (dst_addr as *mut u8).add(start),
                                    end - start,
                                );
                            });
                        }
                    });
                });
                apply_postfilter_to_blocks(dparams, &mut dest[..nbytes], nbytes, blocksize, 0)?;
                return Ok(nbytes);
            }
            dest[..nbytes].copy_from_slice(src);
            apply_postfilter_to_blocks(dparams, &mut dest[..nbytes], nbytes, blocksize, 0)?;
            return Ok(nbytes);
        }
        return Err("Chunk too small for memcpyed data");
    }

    let dict = embedded_codec_dictionary(chunk, header)?;

    // Check if delta filter is used (needs sequential block 0 first)
    let has_delta = header.filters.contains(&BLOSC_DELTA);

    // Allocate output without zero-filling. If a maskout is present, callers
    // own the skipped bytes; the allocating wrapper zero-initializes its buffer.
    let output = &mut dest[..nbytes];

    if has_delta {
        // Delta filter requires block 0 decoded first because later blocks
        // reference it. Reuse scratch buffers while writing finished blocks
        // directly into the final output buffer.
        with_decompress_scratch(
            blocksize,
            |scratch1, scratch2| -> Result<(), &'static str> {
                let block0_end = blocksize.min(nbytes);
                let (first_block, rest) = output.split_at_mut(block0_end);
                if !block_is_masked(maskout, 0) {
                    decompress_block_into(
                        chunk,
                        0,
                        0,
                        first_block,
                        blocksize,
                        nblocks == 1 && block0_end < blocksize,
                        header,
                        None,
                        dict,
                        dparams,
                        scratch1,
                        scratch2,
                        0,
                    )?;
                }
                let dref: &[u8] = first_block;

                for block_idx in 1..nblocks {
                    if block_is_masked(maskout, block_idx) {
                        continue;
                    }
                    let block_start = block_idx * blocksize;
                    let block_end = (block_start + blocksize).min(nbytes);
                    let bsize = block_end - block_start;
                    let is_leftover = block_idx == nblocks - 1 && bsize < blocksize;
                    let rest_start = block_start
                        .checked_sub(block0_end)
                        .ok_or("Invalid block offset")?;

                    decompress_block_into(
                        chunk,
                        block_idx,
                        block_start,
                        &mut rest[rest_start..rest_start + bsize],
                        blocksize,
                        is_leftover,
                        header,
                        Some(dref),
                        dict,
                        dparams,
                        scratch1,
                        scratch2,
                        0,
                    )?;
                }
                Ok(())
            },
        )?;
    } else if dparams.nthreads > 1 && nblocks > 1 {
        // Parallel decompression (no delta filter). Dynamically assign blocks
        // so workers stay balanced when compressed block costs vary.
        let threads = effective_nthreads(dparams.nthreads, nblocks);
        let next_block = AtomicUsize::new(0);
        let output_addr = output.as_mut_ptr() as usize;
        let first_err = std::sync::Mutex::new(None::<&'static str>);

        with_thread_pool(threads, || {
            rayon::scope(|scope| {
                for _ in 0..threads as usize {
                    let next_block = &next_block;
                    let first_err = &first_err;
                    scope.spawn(move |_| {
                        let result = with_decompress_scratch(
                            blocksize,
                            |scratch1, scratch2| -> Result<(), &'static str> {
                                loop {
                                    let block_idx = next_block.fetch_add(1, Ordering::Relaxed);
                                    if block_idx >= nblocks {
                                        break;
                                    }
                                    if block_is_masked(maskout, block_idx) {
                                        continue;
                                    }
                                    let block_start = block_idx * blocksize;
                                    let block_end = (block_start + blocksize).min(nbytes);
                                    let bsize = block_end - block_start;
                                    let is_leftover = block_idx == nblocks - 1 && bsize < blocksize;
                                    let block_out = unsafe {
                                        std::slice::from_raw_parts_mut(
                                            (output_addr as *mut u8).add(block_start),
                                            bsize,
                                        )
                                    };
                                    decompress_block_into(
                                        chunk,
                                        block_idx,
                                        block_start,
                                        block_out,
                                        blocksize,
                                        is_leftover,
                                        header,
                                        None,
                                        dict,
                                        dparams,
                                        scratch1,
                                        scratch2,
                                        rayon::current_thread_index().unwrap_or(0) as i32,
                                    )?;
                                }
                                Ok(())
                            },
                        );

                        if let Err(err) = result {
                            let mut slot = first_err.lock().unwrap();
                            if slot.is_none() {
                                *slot = Some(err);
                            }
                        }
                    });
                }
            });
        });

        let err = *first_err.lock().unwrap();
        if let Some(err) = err {
            return Err(err);
        }
    } else {
        // Sequential decompression: reuse scratch buffers and write finished
        // blocks directly into the final output buffer.
        with_decompress_scratch(
            blocksize,
            |scratch1, scratch2| -> Result<(), &'static str> {
                for block_idx in 0..nblocks {
                    if block_is_masked(maskout, block_idx) {
                        continue;
                    }
                    let block_start = block_idx * blocksize;
                    let block_end = (block_start + blocksize).min(nbytes);
                    let bsize = block_end - block_start;
                    let is_leftover = block_idx == nblocks - 1 && bsize < blocksize;

                    decompress_block_into(
                        chunk,
                        block_idx,
                        block_start,
                        &mut output[block_start..block_end],
                        blocksize,
                        is_leftover,
                        header,
                        None,
                        dict,
                        dparams,
                        scratch1,
                        scratch2,
                        0,
                    )?;
                }
                Ok(())
            },
        )?;
    }

    Ok(nbytes)
}

fn write_special_range(
    chunk: &[u8],
    header: &ChunkHeader,
    nbytes: usize,
    payload_start: usize,
    range_start: usize,
    dest: &mut [u8],
) -> Result<(), &'static str> {
    match header.special_type() {
        BLOSC2_SPECIAL_ZERO => {
            dest.fill(0);
            Ok(())
        }
        BLOSC2_SPECIAL_UNINIT => Ok(()),
        BLOSC2_SPECIAL_NAN => {
            let typesize = header.typesize as usize;
            if typesize == 0 {
                return Err("Invalid special value nbytes");
            }
            if !range_start.is_multiple_of(typesize) || !dest.len().is_multiple_of(typesize) {
                return Err("Invalid special value nbytes");
            }
            match typesize {
                4 => {
                    let nan_bytes = f32::NAN.to_le_bytes();
                    for item in dest.chunks_exact_mut(4) {
                        item.copy_from_slice(&nan_bytes);
                    }
                    Ok(())
                }
                8 => {
                    let nan_bytes = f64::NAN.to_le_bytes();
                    for item in dest.chunks_exact_mut(8) {
                        item.copy_from_slice(&nan_bytes);
                    }
                    Ok(())
                }
                _ => Err("NaN special only valid for 4 or 8 byte types"),
            }
        }
        BLOSC2_SPECIAL_VALUE => {
            let stored_value_size = (header.cbytes as usize)
                .checked_sub(payload_start)
                .ok_or("Invalid special value size")?;
            let stored_value_end = payload_start
                .checked_add(stored_value_size)
                .ok_or("Invalid special value size")?;
            if stored_value_size == 0
                || stored_value_size > BLOSC2_MAXTYPESIZE
                || (nbytes != 0 && stored_value_size > nbytes)
            {
                return Err("Invalid special value size");
            }
            if stored_value_end > header.cbytes as usize || stored_value_end > chunk.len() {
                return Err("Invalid special value size");
            }
            if !nbytes.is_multiple_of(stored_value_size) {
                return Err("Invalid special value nbytes");
            }

            let typesize = usize::from(header.typesize).min(stored_value_size);
            let value_end = payload_start
                .checked_add(typesize)
                .ok_or("Invalid special value size")?;
            if typesize == 0 || value_end > stored_value_end {
                return Err("Invalid special value size");
            }
            if !range_start.is_multiple_of(typesize)
                || !dest.len().is_multiple_of(typesize)
                || !nbytes.is_multiple_of(typesize)
            {
                return Err("Invalid special value nbytes");
            }
            let repeated = &chunk[payload_start..value_end];
            for item in dest.chunks_exact_mut(typesize) {
                item.copy_from_slice(repeated);
            }
            Ok(())
        }
        _ => Err("Unknown special value type"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{
        AtomicI32, AtomicU64, AtomicU8, AtomicUsize, Ordering as AtomicOrdering,
    };

    static VL_POSTFILTER_OFFSET_SUM: AtomicI32 = AtomicI32::new(0);
    static PREFILTER_TID_MASK: AtomicU64 = AtomicU64::new(0);
    static POSTFILTER_TID_MASK: AtomicU64 = AtomicU64::new(0);
    static POSTFILTER_CALLS: AtomicI32 = AtomicI32::new(0);
    static COMPRESSION_BUDGET_PREFILTER_CALLS: AtomicUsize = AtomicUsize::new(0);
    static CALLBACK_ABI_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    static FILTER_FORWARD_ID: AtomicU8 = AtomicU8::new(0);
    static FILTER_FORWARD_META: AtomicU8 = AtomicU8::new(0);
    static FILTER_FORWARD_CLEVEL: AtomicU8 = AtomicU8::new(0);
    static FILTER_FORWARD_NCHUNK: AtomicI32 = AtomicI32::new(0);
    static FILTER_FORWARD_NBLOCK: AtomicI32 = AtomicI32::new(0);
    static FILTER_FORWARD_USER_DATA: AtomicUsize = AtomicUsize::new(0);
    static FILTER_BACKWARD_ID: AtomicU8 = AtomicU8::new(0);
    static FILTER_BACKWARD_META: AtomicU8 = AtomicU8::new(0);
    static FILTER_BACKWARD_NCHUNK: AtomicI32 = AtomicI32::new(0);
    static FILTER_BACKWARD_NBLOCK: AtomicI32 = AtomicI32::new(0);
    static FILTER_BACKWARD_USER_DATA: AtomicUsize = AtomicUsize::new(0);
    static CODEC_COMPRESS_CODE: AtomicU8 = AtomicU8::new(0);
    static CODEC_COMPRESS_META: AtomicU8 = AtomicU8::new(0);
    static CODEC_COMPRESS_CLEVEL: AtomicU8 = AtomicU8::new(0);
    static CODEC_COMPRESS_NCHUNK: AtomicI32 = AtomicI32::new(0);
    static CODEC_COMPRESS_NBLOCK: AtomicI32 = AtomicI32::new(0);
    static CODEC_COMPRESS_USER_DATA: AtomicUsize = AtomicUsize::new(0);
    static CODEC_COMPRESS_CODEC_PARAMS: AtomicUsize = AtomicUsize::new(0);
    static CODEC_COMPRESS_BLOCKSIZE: AtomicI32 = AtomicI32::new(0);
    static CODEC_COMPRESS_CHUNK_ARG: AtomicUsize = AtomicUsize::new(0);
    static CODEC_COMPRESS_PREPARAMS: AtomicUsize = AtomicUsize::new(0);
    static CODEC_DECOMPRESS_CODE: AtomicU8 = AtomicU8::new(0);
    static CODEC_DECOMPRESS_META: AtomicU8 = AtomicU8::new(0);
    static CODEC_DECOMPRESS_NCHUNK: AtomicI32 = AtomicI32::new(0);
    static CODEC_DECOMPRESS_NBLOCK: AtomicI32 = AtomicI32::new(0);
    static CODEC_DECOMPRESS_USER_DATA: AtomicUsize = AtomicUsize::new(0);
    static CODEC_DECOMPRESS_CHUNK_ARG: AtomicUsize = AtomicUsize::new(0);
    static CODEC_DECOMPRESS_POSTPARAMS: AtomicUsize = AtomicUsize::new(0);

    fn xor_prefilter(params: &mut PrefilterParams<'_>) -> i32 {
        for (dst, src) in params.output.iter_mut().zip(params.input.iter().copied()) {
            *dst = src ^ 0x5A;
        }
        0
    }

    fn budget_probe_prefilter(params: &mut PrefilterParams<'_>) -> i32 {
        COMPRESSION_BUDGET_PREFILTER_CALLS.fetch_add(1, AtomicOrdering::SeqCst);
        params.output.copy_from_slice(params.input);
        0
    }

    fn failing_prefilter(params: &mut PrefilterParams<'_>) -> i32 {
        params.output.copy_from_slice(params.input);
        1
    }

    fn record_prefilter_tid(params: &mut PrefilterParams<'_>) -> i32 {
        if (0..64).contains(&params.tid) {
            PREFILTER_TID_MASK.fetch_or(1u64 << params.tid, AtomicOrdering::SeqCst);
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
        params.output.copy_from_slice(params.input);
        0
    }

    fn disposable_failing_prefilter(params: &mut PrefilterParams<'_>) -> i32 {
        params.output.fill(0xa5);
        1
    }

    fn disposable_success_prefilter(params: &mut PrefilterParams<'_>) -> i32 {
        params.output.fill(0xa5);
        0
    }

    fn xor_postfilter(params: &mut PostfilterParams<'_>) -> i32 {
        for (dst, src) in params.output.iter_mut().zip(params.input.iter().copied()) {
            *dst = src ^ 0x5A;
        }
        0
    }

    fn record_vl_postfilter_offset(params: &mut PostfilterParams<'_>) -> i32 {
        VL_POSTFILTER_OFFSET_SUM.fetch_add(params.offset, AtomicOrdering::SeqCst);
        params.output.copy_from_slice(params.input);
        0
    }

    fn xor_user_filter_with_offset(
        meta: u8,
        _typesize: usize,
        block_offset: usize,
        src: &[u8],
        dest: &mut [u8],
    ) {
        let key = meta ^ (block_offset as u8);
        for (dst, src) in dest.iter_mut().zip(src.iter().copied()) {
            *dst = src ^ key;
        }
    }

    fn record_postfilter_tid(params: &mut PostfilterParams<'_>) -> i32 {
        if (0..64).contains(&params.tid) {
            POSTFILTER_TID_MASK.fetch_or(1u64 << params.tid, AtomicOrdering::SeqCst);
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
        params.output.copy_from_slice(params.input);
        0
    }

    fn require_typesize_two_postfilter(params: &mut PostfilterParams<'_>) -> i32 {
        if params.typesize != 2 {
            return 1;
        }
        params.output.copy_from_slice(params.input);
        0
    }

    fn fill_uninit_postfilter(params: &mut PostfilterParams<'_>) -> i32 {
        POSTFILTER_CALLS.fetch_add(1, AtomicOrdering::SeqCst);
        params.output.fill(0x7b);
        0
    }

    #[test]
    fn test_compress_decompress_roundtrip() {
        let data: Vec<u8> = (0..10000u32).flat_map(|i| i.to_le_bytes()).collect();

        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 4,
            splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };

        let compressed = compress(&data, &cparams).unwrap();
        assert!(compressed.len() < data.len(), "Should compress");

        let decompressed = decompress(&compressed).unwrap();
        assert_eq!(data, decompressed);
    }

    #[test]
    fn test_cbuffer_metadata_and_getitem() {
        let data: Vec<u8> = (0..256u32).flat_map(|i| i.to_le_bytes()).collect();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 4,
            splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let compressed = compress(&data, &cparams).unwrap();

        let (nbytes, cbytes, blocksize) = chunk_sizes(&compressed).unwrap();
        assert_eq!(nbytes, data.len());
        assert_eq!(cbytes, compressed.len());
        assert!(blocksize > 0);
        assert_eq!(
            chunk_sizes(&compressed[..BLOSC_MIN_HEADER_LENGTH]).unwrap(),
            (data.len(), compressed.len(), blocksize)
        );
        assert_eq!(
            blosc1_cbuffer_sizes(&compressed[..BLOSC_MIN_HEADER_LENGTH]).unwrap(),
            (data.len(), compressed.len(), blocksize)
        );
        assert_eq!(
            blosc_cbuffer_sizes(&compressed[..BLOSC_MIN_HEADER_LENGTH]).unwrap(),
            (data.len(), compressed.len(), blocksize)
        );
        assert_eq!(
            cbuffer_sizes_c(&compressed[..BLOSC_MIN_HEADER_LENGTH]),
            (
                BLOSC2_ERROR_SUCCESS,
                data.len() as i32,
                compressed.len() as i32,
                blocksize as i32
            )
        );
        assert_eq!(
            blosc2_cbuffer_sizes(&compressed[..BLOSC_MIN_HEADER_LENGTH]),
            cbuffer_sizes_c(&compressed[..BLOSC_MIN_HEADER_LENGTH])
        );
        let mut oversized_blocksize = compressed[..BLOSC_MIN_HEADER_LENGTH].to_vec();
        oversized_blocksize[BLOSC2_CHUNK_BLOCKSIZE..BLOSC2_CHUNK_BLOCKSIZE + 4]
            .copy_from_slice(&((data.len() as i32) + 4).to_le_bytes());
        assert_eq!(
            cbuffer_sizes_c(&oversized_blocksize),
            (
                BLOSC2_ERROR_SUCCESS,
                data.len() as i32,
                compressed.len() as i32,
                data.len() as i32 + 4
            )
        );
        assert_eq!(
            chunk_sizes(&oversized_blocksize).unwrap(),
            (data.len(), compressed.len(), data.len())
        );

        let (typesize, compcode, filters) = chunk_metainfo(&compressed).unwrap();
        assert_eq!(typesize, 4);
        assert_eq!(compcode, BLOSC_LZ4);
        assert_eq!(filters, cparams.filters);
        assert!(chunk_metainfo(&compressed[..BLOSC_MIN_HEADER_LENGTH]).is_err());
        assert_eq!(
            chunk_metainfo_flags(&compressed[..BLOSC_MIN_HEADER_LENGTH]).unwrap(),
            (4, compressed[BLOSC2_CHUNK_FLAGS])
        );
        assert_eq!(
            blosc1_cbuffer_metainfo(&compressed[..BLOSC_MIN_HEADER_LENGTH]).unwrap(),
            (4, compressed[BLOSC2_CHUNK_FLAGS])
        );
        assert_eq!(
            blosc_cbuffer_metainfo(&compressed[..BLOSC_MIN_HEADER_LENGTH]).unwrap(),
            (4, compressed[BLOSC2_CHUNK_FLAGS])
        );
        assert_eq!(
            chunk_versions(&compressed[..BLOSC_MIN_HEADER_LENGTH]).unwrap(),
            (
                compressed[BLOSC2_CHUNK_VERSION],
                compressed[BLOSC2_CHUNK_VERSIONLZ]
            )
        );
        assert_eq!(
            cbuffer_metainfo_flags_c(&compressed[..BLOSC_MIN_HEADER_LENGTH]),
            (
                BLOSC2_ERROR_SUCCESS,
                4,
                compressed[BLOSC2_CHUNK_FLAGS] as i32
            )
        );
        assert_eq!(
            blosc2_cbuffer_metainfo2_c(&compressed),
            (BLOSC2_ERROR_SUCCESS, 4, BLOSC_LZ4 as i32, cparams.filters)
        );
        assert_eq!(
            cbuffer_versions_c(&compressed[..BLOSC_MIN_HEADER_LENGTH]),
            (
                BLOSC2_ERROR_SUCCESS,
                compressed[BLOSC2_CHUNK_VERSION] as i32,
                compressed[BLOSC2_CHUNK_VERSIONLZ] as i32
            )
        );
        assert_eq!(
            blosc2_cbuffer_versions(&compressed[..BLOSC_MIN_HEADER_LENGTH]),
            cbuffer_versions_c(&compressed[..BLOSC_MIN_HEADER_LENGTH])
        );
        assert_eq!(
            blosc_cbuffer_versions(&compressed[..BLOSC_MIN_HEADER_LENGTH]),
            (
                BLOSC2_ERROR_SUCCESS,
                compressed[BLOSC2_CHUNK_VERSION] as i32,
                compressed[BLOSC2_CHUNK_VERSIONLZ] as i32
            )
        );
        assert_eq!(chunk_compressor_library(&compressed), Some("LZ4"));
        assert_eq!(blosc2_cbuffer_complib(&compressed), Some("LZ4"));
        assert_eq!(blosc_cbuffer_complib(&compressed), Some("LZ4"));
        let mut invalid_prefix = compressed[..BLOSC_MIN_HEADER_LENGTH].to_vec();
        invalid_prefix[BLOSC2_CHUNK_TYPESIZE] = 0;
        assert!(chunk_metainfo_flags(&invalid_prefix).is_err());
        assert!(chunk_versions(&invalid_prefix).is_err());
        assert_eq!(
            cbuffer_sizes_c(&invalid_prefix),
            (BLOSC2_ERROR_INVALID_HEADER, 0, 0, 0)
        );
        assert_eq!(
            cbuffer_metainfo_flags_c(&invalid_prefix),
            (BLOSC2_ERROR_INVALID_HEADER, 0, 0)
        );
        assert_eq!(
            blosc2_cbuffer_metainfo2_c(&invalid_prefix),
            (BLOSC2_ERROR_INVALID_HEADER, 0, 0, [0; BLOSC2_MAX_FILTERS])
        );
        assert_eq!(
            cbuffer_versions_c(&invalid_prefix),
            (BLOSC2_ERROR_INVALID_HEADER, 0, 0)
        );
        assert_eq!(chunk_compressor_library(&invalid_prefix), None);

        let mut negative_nbytes_prefix = compressed[..BLOSC_MIN_HEADER_LENGTH].to_vec();
        negative_nbytes_prefix[BLOSC2_CHUNK_NBYTES..BLOSC2_CHUNK_NBYTES + 4]
            .copy_from_slice(&(-1i32).to_le_bytes());
        assert!(chunk_sizes(&negative_nbytes_prefix).is_err());
        assert_eq!(
            cbuffer_sizes_c(&negative_nbytes_prefix),
            (
                BLOSC2_ERROR_SUCCESS,
                -1,
                compressed.len() as i32,
                blocksize as i32
            )
        );
        assert_eq!(
            blosc2_cbuffer_sizes(&negative_nbytes_prefix),
            (
                BLOSC2_ERROR_SUCCESS,
                -1,
                compressed.len() as i32,
                blocksize as i32
            )
        );
        assert_eq!(
            cbuffer_metainfo_flags_c(&negative_nbytes_prefix),
            (
                BLOSC2_ERROR_SUCCESS,
                4,
                compressed[BLOSC2_CHUNK_FLAGS] as i32
            )
        );
        assert_eq!(
            cbuffer_versions_c(&negative_nbytes_prefix),
            (
                BLOSC2_ERROR_SUCCESS,
                compressed[BLOSC2_CHUNK_VERSION] as i32,
                compressed[BLOSC2_CHUNK_VERSIONLZ] as i32
            )
        );
        assert_eq!(
            chunk_compressor_library(&negative_nbytes_prefix),
            Some("LZ4")
        );
        assert!(validate_chunk(&compressed).is_ok());
        assert_eq!(blosc1_cbuffer_validate(&compressed).unwrap(), data.len());
        assert_eq!(blosc_cbuffer_validate(&compressed).unwrap(), data.len());

        let items = getitem(&compressed, 10, 20).unwrap();
        assert_eq!(items, data[10 * 4..30 * 4]);
        assert!(getitem(&compressed, 250, 10).is_err());
        let mut zero_dest = [];
        assert_eq!(getitem_c(&compressed, -1, 0, &mut zero_dest), 0);
        assert_eq!(
            blosc2_getitem_c(
                &compressed,
                compressed.len() as i32,
                -1,
                0,
                &mut zero_dest,
                0
            ),
            0
        );
        assert_eq!(
            blosc2_getitem_c(
                &compressed,
                compressed.len() as i32,
                (data.len() / 4) as i32 + 1,
                0,
                &mut zero_dest,
                0
            ),
            0
        );
        assert_eq!(
            blosc2_getitem_c(
                &compressed,
                compressed.len() as i32,
                (data.len() / 4) as i32,
                0,
                &mut zero_dest,
                0
            ),
            0
        );
        let dctx = DContext::new(DParams::default());
        assert_eq!(
            blosc2_getitem_ctx_c(
                &dctx,
                &compressed,
                compressed.len() as i32,
                -1,
                0,
                &mut zero_dest,
                0
            ),
            0
        );
        let mut invalid_chunk = compressed.clone();
        invalid_chunk[BLOSC2_CHUNK_TYPESIZE] = 0;
        assert_eq!(
            getitem_c(&invalid_chunk, 0, 0, &mut zero_dest),
            BLOSC2_ERROR_INVALID_HEADER
        );
        assert_eq!(
            blosc2_getitem_c(
                &compressed,
                (compressed.len() - 1) as i32,
                0,
                0,
                &mut zero_dest,
                0
            ),
            BLOSC2_ERROR_INVALID_HEADER
        );
        let vl_blocks: [&[u8]; 2] = [b"alpha", b"beta"];
        let vlchunk = vlcompress(&vl_blocks, &CParams::default()).unwrap();
        assert_eq!(
            getitem_c(&vlchunk, 0, 0, &mut zero_dest),
            BLOSC2_ERROR_INVALID_PARAM
        );
        assert_eq!(
            blosc2_getitem_c(&vlchunk, vlchunk.len() as i32, 0, 0, &mut zero_dest, -1),
            BLOSC2_ERROR_INVALID_PARAM
        );
        assert_eq!(
            blosc2_getitem_ctx_c(
                &dctx,
                &vlchunk,
                vlchunk.len() as i32,
                0,
                1,
                &mut zero_dest,
                -1
            ),
            BLOSC2_ERROR_INVALID_PARAM
        );

        let mut truncated = compressed.clone();
        truncated.truncate(truncated.len() - 1);
        assert!(validate_chunk(&truncated).is_err());
        assert!(blosc1_cbuffer_validate(&truncated).is_err());
    }

    #[test]
    fn test_getitem_decompresses_only_touched_blocks() {
        let data: Vec<u8> = (0..512u32).flat_map(|i| i.to_le_bytes()).collect();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 4,
            blocksize: 128,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut compressed = compress(&data, &cparams).unwrap();
        let header = ChunkHeader::read(&compressed).unwrap();
        assert!(header.nblocks() > 2);

        let block2_bstart = BLOSC_EXTENDED_HEADER_LENGTH + 2 * 4;
        compressed[block2_bstart..block2_bstart + 4].copy_from_slice(&(-1i32).to_le_bytes());
        assert!(decompress(&compressed).is_err());

        let items = getitem(&compressed, 4, 8).unwrap();
        assert_eq!(items, data[4 * 4..12 * 4]);
    }

    #[test]
    fn test_getitem_block_local_with_delta_and_dictionary() {
        let data: Vec<u8> = (0..8192u32).flat_map(|i| (i % 257).to_le_bytes()).collect();
        for (filters, use_dict) in [
            ([0, 0, 0, 0, BLOSC_DELTA, BLOSC_SHUFFLE], false),
            ([0, 0, 0, 0, 0, BLOSC_SHUFFLE], true),
        ] {
            let cparams = CParams {
                compcode: BLOSC_ZSTD,
                clevel: 5,
                typesize: 4,
                blocksize: 1024,
                splitmode: BLOSC_NEVER_SPLIT,
                filters,
                use_dict,
                ..Default::default()
            };
            let compressed = compress(&data, &cparams).unwrap();
            let items = getitem(&compressed, 300, 600).unwrap();
            assert_eq!(items, data[300 * 4..900 * 4]);
        }
    }

    #[test]
    fn test_getitem_zero_items_matches_c_ordering() {
        let data: Vec<u8> = (0..128u32).flat_map(u32::to_le_bytes).collect();
        let compressed = compress(
            &data,
            &CParams {
                compcode: BLOSC_LZ4,
                clevel: 5,
                typesize: 4,
                filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
                ..Default::default()
            },
        )
        .unwrap();

        assert_eq!(
            getitem(&compressed, data.len() / 4 + 1024, 0).unwrap(),
            Vec::<u8>::new()
        );

        let dctx = DContext::new(DParams::default());
        let mut dest = [];
        assert_eq!(
            dctx.get_items_into(&compressed, data.len() / 4 + 1024, 0, &mut dest)
                .unwrap(),
            0
        );

        let vl_blocks: [&[u8]; 2] = [b"alpha", b"beta"];
        let vlchunk = vlcompress(&vl_blocks, &CParams::default()).unwrap();
        assert!(getitem(&vlchunk, 0, 0).is_err());
        assert!(dctx.get_items_into(&vlchunk, 0, 0, &mut dest).is_err());
    }

    #[test]
    fn test_blosc1_wrappers_roundtrip_and_validate_buffers() {
        // blosc1_compress reads process-wide globals (compressor/blocksize/etc.)
        // that other tests mutate; hold BLOSC_ENV_LOCK so they don't race.
        let _lock = BLOSC_ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let data: Vec<u8> = (0..1024u32).flat_map(|i| i.to_le_bytes()).collect();
        let mut compressed = vec![0u8; data.len() + BLOSC2_MAX_OVERHEAD + 1024];
        let csize = blosc1_compress(5, BLOSC_SHUFFLE, 4, &data, &mut compressed).unwrap();
        assert!(csize > 0);

        let (nbytes, _, _) = chunk_sizes(&compressed[..csize]).unwrap();
        assert_eq!(nbytes, data.len());

        let mut restored = vec![0u8; data.len()];
        let dsize = blosc1_decompress(&compressed[..csize], &mut restored).unwrap();
        assert_eq!(dsize, data.len());
        assert_eq!(restored, data);

        let mut short_compressed = vec![0u8; 8];
        assert!(blosc1_compress(5, BLOSC_SHUFFLE, 4, &data, &mut short_compressed).is_err());

        let mut short_restored = vec![0u8; data.len() - 1];
        assert!(blosc1_decompress(&compressed[..csize], &mut short_restored).is_err());
        assert!(blosc1_compress(10, BLOSC_SHUFFLE, 4, &data, &mut compressed).is_err());
    }

    #[test]
    fn test_c_style_wrappers_return_c_compatible_codes() {
        let _lock = BLOSC_ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let data: Vec<u8> = (0..512u32).flat_map(|i| i.to_le_bytes()).collect();
        let mut compressed = vec![0u8; data.len() + BLOSC2_MAX_OVERHEAD + 1024];

        let csize = blosc1_compress_c(5, BLOSC_SHUFFLE as i32, 4, &data, &mut compressed);
        assert!(csize > 0);

        let chunk = &compressed[..csize as usize];
        assert_eq!(
            blosc2_cbuffer_metainfo(chunk),
            cbuffer_metainfo_flags_c(chunk)
        );
        let mut compressed2 = vec![0u8; compressed.len()];
        let csize2 = blosc2_compress(
            5,
            BLOSC_SHUFFLE as i32,
            4,
            &data,
            data.len() as i32,
            &mut compressed2,
            compressed.len() as i32,
        );
        assert!(csize2 > 0);
        let mut restored2 = vec![0u8; data.len()];
        assert_eq!(
            blosc2_decompress(&compressed2, csize2, &mut restored2, data.len() as i32),
            data.len() as i32
        );
        assert_eq!(restored2, data);
        assert_eq!(
            blosc2_compress(
                5,
                BLOSC_SHUFFLE as i32,
                4,
                &data,
                -1,
                &mut compressed2,
                compressed.len() as i32,
            ),
            BLOSC2_ERROR_INVALID_PARAM
        );
        assert_eq!(
            blosc2_compress(
                5,
                BLOSC_SHUFFLE as i32,
                4,
                &data,
                data.len() as i32,
                &mut compressed2,
                -1,
            ),
            BLOSC2_ERROR_MAX_BUFSIZE_EXCEEDED
        );
        assert_eq!(
            blosc2_compress(
                5,
                BLOSC_SHUFFLE as i32,
                4,
                &data,
                data.len() as i32,
                &mut compressed2,
                8,
            ),
            BLOSC2_ERROR_MAX_BUFSIZE_EXCEEDED
        );
        assert_eq!(
            blosc2_decompress(&compressed2, -1, &mut restored2, data.len() as i32),
            BLOSC2_ERROR_READ_BUFFER
        );
        assert_eq!(
            blosc2_decompress(&compressed2, csize2, &mut restored2, -1),
            BLOSC2_ERROR_WRITE_BUFFER
        );
        assert_eq!(cbuffer_validate_c(chunk), BLOSC2_ERROR_SUCCESS);
        let strict_chunk = compress(
            &data,
            &CParams {
                compcode: BLOSC_LZ4,
                clevel: 5,
                typesize: 4,
                splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
                filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
                ..Default::default()
            },
        )
        .unwrap();
        let mut unsupported_filter = strict_chunk;
        unsupported_filter[BLOSC2_CHUNK_FILTER_CODES + 5] = 99;
        assert!(validate_chunk(&unsupported_filter).is_err());
        assert_eq!(
            cbuffer_validate_c(&unsupported_filter),
            BLOSC2_ERROR_SUCCESS
        );
        assert_eq!(
            blosc2_error_code("Execution of prefilter function failed"),
            BLOSC2_ERROR_FILTER_PIPELINE
        );
        assert_eq!(
            blosc2_error_code("Execution of postfilter function failed"),
            BLOSC2_ERROR_POSTFILTER
        );
        assert_eq!(
            blosc2_error_code("NaN special only valid for 4 or 8 byte types"),
            BLOSC2_ERROR_DATA
        );

        let mut restored = vec![0u8; data.len()];
        assert_eq!(blosc1_decompress_c(chunk, &mut restored), data.len() as i32);
        assert_eq!(restored, data);

        let mut short_compressed = vec![0u8; 8];
        assert_eq!(
            blosc1_compress_c(5, BLOSC_SHUFFLE as i32, 4, &data, &mut short_compressed),
            BLOSC2_ERROR_MAX_BUFSIZE_EXCEEDED
        );

        let mut short_restored = vec![0u8; data.len() - 1];
        assert_eq!(
            blosc1_decompress_c(chunk, &mut short_restored),
            BLOSC2_ERROR_WRITE_BUFFER
        );

        let mut items = vec![0u8; 8 * 4];
        assert_eq!(getitem_c(chunk, 10, 8, &mut items), items.len() as i32);
        assert_eq!(items, data[10 * 4..18 * 4]);
        let item_destsize = items.len() as i32;
        items.fill(0);
        assert_eq!(
            blosc2_getitem_c(chunk, chunk.len() as i32, 10, 8, &mut items, item_destsize),
            item_destsize
        );
        assert_eq!(items, data[10 * 4..18 * 4]);
        items.fill(0);
        assert_eq!(blosc1_getitem(chunk, 10, 8, &mut items), item_destsize);
        assert_eq!(items, data[10 * 4..18 * 4]);
        items.fill(0);
        assert_eq!(blosc_getitem(chunk, 10, 8, &mut items), item_destsize);
        assert_eq!(items, data[10 * 4..18 * 4]);
        assert_eq!(
            blosc1_getitem(chunk, -1, 8, &mut items),
            BLOSC2_ERROR_INVALID_PARAM
        );
        assert!(
            blosc2_getitem_c(
                chunk,
                (chunk.len() - 1) as i32,
                10,
                8,
                &mut items,
                item_destsize
            ) < 0
        );
        assert_eq!(
            blosc2_getitem_c(chunk, -1, 10, 8, &mut items, item_destsize),
            BLOSC2_ERROR_READ_BUFFER
        );
        assert_eq!(
            blosc2_getitem_c(
                chunk,
                chunk.len() as i32,
                10,
                8,
                &mut items,
                item_destsize - 1
            ),
            BLOSC2_ERROR_WRITE_BUFFER
        );
        assert_eq!(
            blosc2_getitem_c(
                chunk,
                chunk.len() as i32,
                -1,
                8,
                &mut items,
                item_destsize - 1
            ),
            BLOSC2_ERROR_WRITE_BUFFER
        );

        let dctx = DContext::new(DParams::default());
        items.fill(0);
        assert_eq!(
            blosc2_getitem_ctx_c(
                &dctx,
                chunk,
                chunk.len() as i32,
                10,
                8,
                &mut items,
                item_destsize,
            ),
            item_destsize
        );
        assert_eq!(items, data[10 * 4..18 * 4]);
        assert_eq!(
            blosc2_getitem_ctx_c(
                &dctx,
                chunk,
                chunk.len() as i32,
                -1,
                8,
                &mut items,
                item_destsize - 1,
            ),
            BLOSC2_ERROR_WRITE_BUFFER
        );

        let mut short_items = vec![0u8; items.len() - 1];
        assert_eq!(
            getitem_c(chunk, 10, 8, &mut short_items),
            BLOSC2_ERROR_WRITE_BUFFER
        );
        assert_eq!(
            getitem_c(chunk, -1, 8, &mut items),
            BLOSC2_ERROR_INVALID_PARAM
        );
        assert_eq!(
            getitem_c(chunk, 0, -1, &mut items),
            BLOSC2_ERROR_INVALID_PARAM
        );
        items.fill(0x5a);
        assert_eq!(getitem_c(chunk, 10, -1, &mut items), 0);
        assert_eq!(blosc1_getitem(chunk, 10, -1, &mut items), 0);
        assert_eq!(items, vec![0x5a; 8 * 4]);
        assert_eq!(getitem_c(chunk, -1, 0, &mut items), 0);
        assert_eq!(
            getitem_c(chunk, 10_000, 1, &mut items),
            BLOSC2_ERROR_INVALID_PARAM
        );
        let mut too_short_for_oob = vec![0u8; 2];
        assert_eq!(
            getitem_c(chunk, 10_000, 1, &mut too_short_for_oob),
            BLOSC2_ERROR_WRITE_BUFFER
        );

        let uninit = blosc2_chunk_uninit(data.len(), 4).unwrap();
        let mut uninit_items = vec![0xA5; 8 * 4];
        let uninit_items_len = uninit_items.len() as i32;
        assert_eq!(
            getitem_c(&uninit, 10, 8, &mut uninit_items),
            uninit_items_len
        );
        assert_eq!(uninit_items, vec![0xA5; 8 * 4]);
        assert_eq!(
            blosc2_getitem_c(
                &uninit,
                uninit.len() as i32,
                10,
                8,
                &mut uninit_items,
                item_destsize,
            ),
            item_destsize
        );
        assert_eq!(uninit_items, vec![0xA5; 8 * 4]);
        assert_eq!(
            blosc2_getitem_ctx_c(
                &dctx,
                &uninit,
                uninit.len() as i32,
                10,
                8,
                &mut uninit_items,
                item_destsize,
            ),
            item_destsize
        );
        assert_eq!(uninit_items, vec![0xA5; 8 * 4]);

        let mut truncated = chunk.to_vec();
        truncated.truncate(BLOSC_MIN_HEADER_LENGTH - 1);
        assert_eq!(cbuffer_validate_c(&truncated), BLOSC2_ERROR_WRITE_BUFFER);

        let mut mismatched_cbytes = chunk.to_vec();
        mismatched_cbytes[12..16].copy_from_slice(&((chunk.len() + 1) as i32).to_le_bytes());
        assert_eq!(
            cbuffer_validate_c(&mismatched_cbytes),
            BLOSC2_ERROR_INVALID_HEADER
        );

        let mut oversized_nbytes = chunk.to_vec();
        oversized_nbytes[4..8].copy_from_slice(&(BLOSC2_MAX_BUFFERSIZE + 1).to_le_bytes());
        assert_eq!(
            cbuffer_validate_c(&oversized_nbytes),
            BLOSC2_ERROR_MEMORY_ALLOC
        );

        let mut mismatched_and_oversized = chunk.to_vec();
        mismatched_and_oversized[4..8].copy_from_slice(&(BLOSC2_MAX_BUFFERSIZE + 1).to_le_bytes());
        mismatched_and_oversized[12..16].copy_from_slice(&((chunk.len() + 1) as i32).to_le_bytes());
        assert_eq!(
            cbuffer_validate_c(&mismatched_and_oversized),
            BLOSC2_ERROR_INVALID_HEADER
        );

        let mut zero_typesize = chunk.to_vec();
        zero_typesize[BLOSC2_CHUNK_TYPESIZE] = 0;
        assert_eq!(
            cbuffer_validate_c(&zero_typesize),
            BLOSC2_ERROR_INVALID_HEADER
        );

        let mut zero_blocksize = chunk.to_vec();
        zero_blocksize[BLOSC2_CHUNK_BLOCKSIZE..BLOSC2_CHUNK_BLOCKSIZE + 4]
            .copy_from_slice(&0i32.to_le_bytes());
        assert_eq!(
            cbuffer_validate_c(&zero_blocksize),
            BLOSC2_ERROR_INVALID_HEADER
        );
    }

    #[test]
    fn test_c_chunk_source_size_errors_match_read_chunk_header() {
        let data: Vec<u8> = (0..128u32).flat_map(|i| i.to_le_bytes()).collect();
        let chunk = compress(
            &data,
            &CParams {
                compcode: BLOSC_LZ4,
                clevel: 5,
                typesize: 4,
                filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
                ..Default::default()
            },
        )
        .unwrap();
        let mut dest = vec![0u8; data.len()];
        let mut item_dest = vec![0u8; 16];
        let dest_len = dest.len() as i32;
        let item_dest_len = item_dest.len() as i32;
        let dctx = DContext::new(DParams::default());
        let short = vec![0u8; BLOSC_MIN_HEADER_LENGTH - 1];

        assert_eq!(
            blosc2_decompress(&chunk, i32::MAX, &mut dest, dest_len),
            dest_len
        );
        assert_eq!(dest, data);
        dest.fill(0);
        assert_eq!(
            blosc2_decompress_ctx(&dctx, &chunk, i32::MAX, &mut dest, dest_len),
            dest_len
        );
        assert_eq!(dest, data);
        item_dest.fill(0);
        assert_eq!(
            blosc2_getitem_c(&chunk, i32::MAX, 0, 4, &mut item_dest, item_dest_len),
            item_dest_len
        );
        assert_eq!(item_dest, data[..item_dest.len()]);
        item_dest.fill(0);
        assert_eq!(
            blosc2_getitem_ctx_c(&dctx, &chunk, i32::MAX, 0, 4, &mut item_dest, item_dest_len),
            item_dest_len
        );
        assert_eq!(item_dest, data[..item_dest.len()]);
        item_dest.fill(0);
        assert_eq!(blosc1_getitem(&chunk, 0, 4, &mut item_dest), item_dest_len);
        assert_eq!(item_dest, data[..item_dest.len()]);
        assert_eq!(
            blosc2_decompress(&chunk, chunk.len() as i32 - 1, &mut dest, dest_len),
            BLOSC2_ERROR_INVALID_HEADER
        );
        assert_eq!(
            blosc2_decompress_ctx(&dctx, &chunk, chunk.len() as i32 - 1, &mut dest, dest_len,),
            BLOSC2_ERROR_INVALID_HEADER
        );
        assert_eq!(
            blosc2_getitem_c(
                &chunk,
                chunk.len() as i32 - 1,
                0,
                4,
                &mut item_dest,
                item_dest_len,
            ),
            BLOSC2_ERROR_INVALID_HEADER
        );
        assert_eq!(
            blosc2_getitem_ctx_c(
                &dctx,
                &chunk,
                chunk.len() as i32 - 1,
                0,
                4,
                &mut item_dest,
                item_dest_len,
            ),
            BLOSC2_ERROR_INVALID_HEADER
        );

        assert_eq!(
            blosc2_decompress(&chunk, -1, &mut dest, dest_len),
            BLOSC2_ERROR_READ_BUFFER
        );
        assert_eq!(
            blosc2_decompress(&short, short.len() as i32, &mut dest, dest_len),
            BLOSC2_ERROR_READ_BUFFER
        );
        assert_eq!(
            blosc2_decompress(&short, BLOSC_MIN_HEADER_LENGTH as i32, &mut dest, dest_len),
            BLOSC2_ERROR_READ_BUFFER
        );
        assert_eq!(
            blosc2_decompress_ctx(&dctx, &short, short.len() as i32, &mut dest, dest_len),
            BLOSC2_ERROR_READ_BUFFER
        );
        assert_eq!(
            blosc2_decompress_ctx(
                &dctx,
                &short,
                BLOSC_MIN_HEADER_LENGTH as i32,
                &mut dest,
                dest_len,
            ),
            BLOSC2_ERROR_READ_BUFFER
        );
        assert_eq!(
            blosc2_decompress(&chunk, chunk.len() as i32, &mut dest, -1),
            BLOSC2_ERROR_WRITE_BUFFER
        );
        assert_eq!(
            blosc2_decompress_ctx(&dctx, &chunk, chunk.len() as i32, &mut dest, -1),
            BLOSC2_ERROR_WRITE_BUFFER
        );

        assert_eq!(
            blosc2_getitem_c(&chunk, -1, 0, 1, &mut item_dest, item_dest_len),
            BLOSC2_ERROR_READ_BUFFER
        );
        assert_eq!(
            blosc2_getitem_c(
                &short,
                short.len() as i32,
                0,
                1,
                &mut item_dest,
                item_dest_len,
            ),
            BLOSC2_ERROR_READ_BUFFER
        );
        assert_eq!(
            blosc2_getitem_c(
                &short,
                BLOSC_MIN_HEADER_LENGTH as i32,
                0,
                1,
                &mut item_dest,
                item_dest_len,
            ),
            BLOSC2_ERROR_READ_BUFFER
        );
        assert_eq!(
            blosc2_getitem_c(
                &chunk,
                chunk.len() as i32,
                -1,
                1,
                &mut item_dest,
                item_dest_len
            ),
            BLOSC2_ERROR_INVALID_PARAM
        );
        assert_eq!(
            blosc2_getitem_c(
                &chunk,
                chunk.len() as i32,
                0,
                -1,
                &mut item_dest,
                item_dest_len
            ),
            BLOSC2_ERROR_INVALID_PARAM
        );
        assert_eq!(
            blosc2_getitem_c(
                &short,
                short.len() as i32,
                0,
                -1,
                &mut item_dest,
                item_dest_len,
            ),
            BLOSC2_ERROR_READ_BUFFER
        );
        assert_eq!(
            blosc2_getitem_c(
                &chunk,
                chunk.len() as i32 - 1,
                0,
                -1,
                &mut item_dest,
                item_dest_len,
            ),
            BLOSC2_ERROR_INVALID_HEADER
        );
        assert_eq!(
            blosc2_getitem_c(&chunk, chunk.len() as i32, 0, 1, &mut item_dest, -1),
            BLOSC2_ERROR_WRITE_BUFFER
        );
        assert_eq!(
            blosc2_getitem_c(&chunk, chunk.len() as i32, 0, 0, &mut item_dest, -1),
            0
        );
        assert_eq!(
            blosc2_getitem_ctx_c(&dctx, &chunk, chunk.len() as i32, 0, 1, &mut item_dest, -1),
            BLOSC2_ERROR_WRITE_BUFFER
        );
        assert_eq!(
            blosc2_getitem_ctx_c(&dctx, &chunk, chunk.len() as i32, 0, 0, &mut item_dest, -1),
            0
        );

        assert_eq!(
            blosc2_getitem_ctx_c(
                &dctx,
                &short,
                short.len() as i32,
                0,
                1,
                &mut item_dest,
                item_dest_len,
            ),
            BLOSC2_ERROR_READ_BUFFER
        );
        assert_eq!(
            blosc2_getitem_ctx_c(
                &dctx,
                &short,
                BLOSC_MIN_HEADER_LENGTH as i32,
                0,
                1,
                &mut item_dest,
                item_dest_len,
            ),
            BLOSC2_ERROR_READ_BUFFER
        );
        assert_eq!(
            blosc2_getitem_ctx_c(
                &dctx,
                &short,
                short.len() as i32,
                0,
                -1,
                &mut item_dest,
                item_dest_len,
            ),
            BLOSC2_ERROR_READ_BUFFER
        );
        assert_eq!(
            blosc2_getitem_ctx_c(
                &dctx,
                &chunk,
                chunk.len() as i32 - 1,
                0,
                -1,
                &mut item_dest,
                item_dest_len,
            ),
            BLOSC2_ERROR_INVALID_HEADER
        );

        assert_eq!(
            blosc2_vlchunk_get_nblocks_c(&short, -1),
            (BLOSC2_ERROR_READ_BUFFER, 0)
        );
        assert_eq!(
            blosc2_vlchunk_get_nblocks_c(&short, short.len() as i32),
            (BLOSC2_ERROR_READ_BUFFER, 0)
        );
        assert_eq!(
            blosc2_vlchunk_get_nblocks_c(&short, BLOSC_MIN_HEADER_LENGTH as i32),
            (BLOSC2_ERROR_INVALID_PARAM, 0)
        );

        let mut blocks = vec![Vec::new()];
        let mut block_sizes = vec![0i32];
        let vl_input: [&[u8]; 2] = [b"alpha", b"bravo-bravo"];
        let vlchunk = vlcompress(
            &vl_input,
            &CParams {
                compcode: BLOSC_LZ4,
                clevel: 5,
                typesize: 1,
                ..Default::default()
            },
        )
        .unwrap();
        let mut vl_blocks = vec![Vec::new(), Vec::new()];
        let mut vl_block_sizes = vec![0i32; 2];
        assert_eq!(
            blosc2_vldecompress_ctx(&dctx, &vlchunk, i32::MAX).0,
            vl_input.len() as i32
        );
        assert_eq!(
            blosc2_vldecompress_ctx_c(
                &dctx,
                &vlchunk,
                i32::MAX,
                &mut vl_blocks,
                &mut vl_block_sizes,
                vl_input.len() as i32,
            ),
            vl_input.len() as i32
        );
        assert_eq!(vl_blocks, vec![b"alpha".to_vec(), b"bravo-bravo".to_vec()]);
        assert_eq!(vl_block_sizes, vec![5, 11]);
        assert_eq!(
            blosc2_vldecompress_block_ctx(&dctx, &vlchunk, i32::MAX, 1),
            (b"bravo-bravo".len() as i32, Some(b"bravo-bravo".to_vec()))
        );
        item_dest.fill(0);
        assert_eq!(
            blosc2_vldecompress_block_ctx_c(
                &dctx,
                &vlchunk,
                i32::MAX,
                1,
                &mut item_dest,
                item_dest_len,
            ),
            b"bravo-bravo".len() as i32
        );
        assert_eq!(&item_dest[..b"bravo-bravo".len()], b"bravo-bravo");
        assert_eq!(
            blosc2_vldecompress_ctx(&dctx, &vlchunk, vlchunk.len() as i32 - 1).0,
            BLOSC2_ERROR_INVALID_HEADER
        );
        assert_eq!(
            blosc2_vldecompress_ctx_c(
                &dctx,
                &vlchunk,
                vlchunk.len() as i32 - 1,
                &mut vl_blocks,
                &mut vl_block_sizes,
                vl_input.len() as i32,
            ),
            BLOSC2_ERROR_INVALID_HEADER
        );
        assert_eq!(
            blosc2_vldecompress_block_ctx(&dctx, &vlchunk, vlchunk.len() as i32 - 1, 1).0,
            BLOSC2_ERROR_INVALID_HEADER
        );
        assert_eq!(
            blosc2_vldecompress_block_ctx_c(
                &dctx,
                &vlchunk,
                vlchunk.len() as i32 - 1,
                1,
                &mut item_dest,
                item_dest_len,
            ),
            BLOSC2_ERROR_INVALID_HEADER
        );

        assert_eq!(
            blosc2_vldecompress_ctx_c(
                &dctx,
                &short,
                short.len() as i32,
                &mut blocks,
                &mut block_sizes,
                0,
            ),
            BLOSC2_ERROR_INVALID_PARAM
        );
        assert_eq!(
            blosc2_vldecompress_ctx(&dctx, &short, short.len() as i32).0,
            BLOSC2_ERROR_READ_BUFFER
        );
        assert_eq!(
            blosc2_vldecompress_ctx_c(
                &dctx,
                &short,
                short.len() as i32,
                &mut blocks,
                &mut block_sizes,
                1,
            ),
            BLOSC2_ERROR_READ_BUFFER
        );
        assert_eq!(
            blosc2_vldecompress_block_ctx(&dctx, &short, short.len() as i32, 0).0,
            BLOSC2_ERROR_READ_BUFFER
        );
        assert_eq!(
            blosc2_vldecompress_block_ctx_c(
                &dctx,
                &short,
                short.len() as i32,
                0,
                &mut item_dest,
                item_dest_len,
            ),
            BLOSC2_ERROR_READ_BUFFER
        );
    }

    // Env-var tests mutate the process environment, so they must run serially.
    // We also restore the prior value on exit to avoid cross-test bleed.
    static BLOSC_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    struct EnvGuard {
        key: &'static str,
        prev: Option<std::ffi::OsString>,
    }

    impl EnvGuard {
        fn set(key: &'static str, value: &str) -> Self {
            let prev = std::env::var_os(key);
            // Safety: test runs under BLOSC_ENV_LOCK; no other thread reads/writes this var concurrently.
            unsafe { std::env::set_var(key, value) };
            EnvGuard { key, prev }
        }

        fn remove(key: &'static str) -> Self {
            let prev = std::env::var_os(key);
            // Safety: test runs under BLOSC_ENV_LOCK; no other thread reads/writes this var concurrently.
            unsafe { std::env::remove_var(key) };
            EnvGuard { key, prev }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            // Safety: same as set; guarded by BLOSC_ENV_LOCK.
            unsafe {
                match &self.prev {
                    Some(v) => std::env::set_var(self.key, v),
                    None => std::env::remove_var(self.key),
                }
            }
        }
    }

    #[test]
    fn test_blosc1_compress_honors_blosc_compressor_env() {
        let _lock = BLOSC_ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let data: Vec<u8> = (0..1024u32).flat_map(|i| i.to_le_bytes()).collect();
        let mut compressed = vec![0u8; data.len() + BLOSC2_MAX_OVERHEAD + 1024];

        let _g = EnvGuard::set("BLOSC_COMPRESSOR", "lz4");
        let csize = blosc1_compress(5, BLOSC_SHUFFLE, 4, &data, &mut compressed).unwrap();

        let (_, compcode, _) = chunk_metainfo(&compressed[..csize]).unwrap();
        assert_eq!(
            compcode, BLOSC_LZ4,
            "BLOSC_COMPRESSOR=lz4 should have selected LZ4, got compcode={compcode}"
        );

        // Roundtrip still works regardless of codec choice.
        let mut restored = vec![0u8; data.len()];
        let dsize = blosc1_decompress(&compressed[..csize], &mut restored).unwrap();
        assert_eq!(dsize, data.len());
        assert_eq!(restored, data);
    }

    #[test]
    fn test_blosc1_compress_honors_blosc1_compat_env() {
        let _lock = BLOSC_ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let _compat = EnvGuard::set("BLOSC_BLOSC1_COMPAT", "0");
        let _nolock = EnvGuard::remove("BLOSC_NOLOCK");
        let data: Vec<u8> = (0..4096u32).flat_map(|i| (i % 257).to_le_bytes()).collect();
        let mut compressed = vec![0u8; data.len() + BLOSC_MIN_HEADER_LENGTH];

        let csize = blosc1_compress(5, BLOSC_SHUFFLE, 4, &data, &mut compressed).unwrap();
        let chunk = &compressed[..csize];
        let header = ChunkHeader::read(chunk).unwrap();
        assert!(!header.is_extended());
        assert_eq!(header.header_len(), BLOSC_MIN_HEADER_LENGTH);
        assert_eq!(header.version, BLOSC2_VERSION_FORMAT_STABLE);
        assert_eq!(header.cbytes, csize as i32);
        assert_eq!(header.nbytes, data.len() as i32);
        assert_eq!(header.flags & BLOSC_DOSHUFFLE, BLOSC_DOSHUFFLE);
        assert_eq!(header.flags & BLOSC_DOBITSHUFFLE, 0);

        let nblocks = header.nblocks();
        let first_bstart = i32::from_le_bytes(
            chunk[BLOSC_MIN_HEADER_LENGTH..BLOSC_MIN_HEADER_LENGTH + 4]
                .try_into()
                .unwrap(),
        );
        assert_eq!(first_bstart as usize, BLOSC_MIN_HEADER_LENGTH + nblocks * 4);

        let mut restored = vec![0u8; data.len()];
        let dsize = blosc1_decompress(chunk, &mut restored).unwrap();
        assert_eq!(dsize, data.len());
        assert_eq!(restored, data);
    }

    #[test]
    fn test_blosc1_compat_memcpy_and_zero_inputs_use_legacy_header() {
        let _lock = BLOSC_ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let _compat = EnvGuard::set("BLOSC_BLOSC1_COMPAT", "");
        let _nolock = EnvGuard::remove("BLOSC_NOLOCK");
        let _compressor = EnvGuard::set("BLOSC_COMPRESSOR", "zstd");

        let small = b"tiny payload";
        let mut compressed =
            vec![0u8; (small.len() + BLOSC_MIN_HEADER_LENGTH).max(BLOSC2_MAX_OVERHEAD)];
        let csize = blosc1_compress(5, BLOSC_SHUFFLE, 4, small, &mut compressed).unwrap();
        let chunk = &compressed[..csize];
        let header = ChunkHeader::read(chunk).unwrap();
        assert!(!header.is_extended());
        assert!(header.memcpyed());
        assert_eq!(header.compcode(), BLOSC_BLOSCLZ);
        assert_eq!(chunk_compressor_library(chunk), Some("BloscLZ"));
        assert_eq!(csize, small.len() + BLOSC_MIN_HEADER_LENGTH);
        assert_eq!(&chunk[BLOSC_MIN_HEADER_LENGTH..], small);
        assert_eq!(decompress(chunk).unwrap(), small);

        let empty: &[u8] = &[];
        let mut compressed = vec![0u8; BLOSC2_MAX_OVERHEAD];
        let csize = blosc1_compress(5, BLOSC_SHUFFLE, 4, empty, &mut compressed).unwrap();
        let chunk = &compressed[..csize];
        let header = ChunkHeader::read(chunk).unwrap();
        assert!(!header.is_extended());
        assert!(header.memcpyed());
        assert_eq!(header.compcode(), BLOSC_BLOSCLZ);
        assert_eq!(chunk_compressor_library(chunk), Some("BloscLZ"));
        assert_eq!(csize, BLOSC_MIN_HEADER_LENGTH);
        assert_eq!(decompress(chunk).unwrap(), empty);

        let zeroes = vec![0u8; 4096];
        let mut compressed = vec![0u8; zeroes.len() + BLOSC_MIN_HEADER_LENGTH];
        let csize = blosc1_compress(5, BLOSC_SHUFFLE, 4, &zeroes, &mut compressed).unwrap();
        let chunk = &compressed[..csize];
        let header = ChunkHeader::read(chunk).unwrap();
        assert!(!header.is_extended());
        assert_eq!(header.special_type(), BLOSC2_NO_SPECIAL);
        assert!(!header.memcpyed());
        assert!(csize < zeroes.len());
        assert_eq!(decompress(chunk).unwrap(), zeroes);
    }

    #[test]
    fn test_blosc1_compat_bitshuffle_and_nolock_flags() {
        let _lock = BLOSC_ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let _compat = EnvGuard::set("BLOSC_BLOSC1_COMPAT", "TRUE");
        let _nolock = EnvGuard::remove("BLOSC_NOLOCK");
        let data: Vec<u8> = (0..2048u32).flat_map(|i| i.to_le_bytes()).collect();
        let mut compressed = vec![0u8; data.len() + BLOSC_MIN_HEADER_LENGTH];

        let csize = blosc1_compress(5, BLOSC_BITSHUFFLE, 4, &data, &mut compressed).unwrap();
        let header = ChunkHeader::read(&compressed[..csize]).unwrap();
        assert!(!header.is_extended());
        assert_eq!(header.flags & BLOSC_DOSHUFFLE, 0);
        assert_eq!(header.flags & BLOSC_DOBITSHUFFLE, BLOSC_DOBITSHUFFLE);
        assert_eq!(decompress(&compressed[..csize]).unwrap(), data);

        let _nolock_set = EnvGuard::set("BLOSC_NOLOCK", "1");
        let mut extended = vec![0u8; data.len() + BLOSC2_MAX_OVERHEAD + 1024];
        let csize = blosc1_compress(5, BLOSC_BITSHUFFLE, 4, &data, &mut extended).unwrap();
        let header = ChunkHeader::read(&extended[..csize]).unwrap();
        assert!(header.is_extended());
        assert_eq!(decompress(&extended[..csize]).unwrap(), data);

        let _clevel = EnvGuard::set("BLOSC_CLEVEL", "256");
        assert!(blosc1_compress(5, BLOSC_BITSHUFFLE, 4, &data, &mut extended).is_err());
    }

    #[test]
    fn test_blosc1_compressor_names_are_case_sensitive() {
        let _lock = BLOSC_ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        blosc1_set_compressor_code(BLOSC_BLOSCLZ);
        assert_eq!(blosc2_compcode_to_compname(BLOSC_LZ4HC), Some("lz4hc"));
        assert_eq!(blosc2_compname_to_compcode("lz4hc"), Some(BLOSC_LZ4HC));
        assert_eq!(blosc2_compcode_to_compname(BLOSC_CODEC_NDLZ), Some("ndlz"));
        let zfp_acc_registered =
            blosc2_compcode_to_compname(BLOSC_CODEC_ZFP_FIXED_ACCURACY) == Some("zfp_acc");
        if zfp_acc_registered {
            assert_eq!(
                blosc2_compname_to_compcode("zfp_acc"),
                Some(BLOSC_CODEC_ZFP_FIXED_ACCURACY)
            );
            assert_eq!(
                blosc2_get_complib_info("zfp_acc"),
                Some((BLOSC_CODEC_ZFP_FIXED_ACCURACY, "zfp_acc", "unknown"))
            );
        } else {
            assert_eq!(blosc2_compname_to_compcode("zfp_acc"), None);
            assert_eq!(blosc2_get_complib_info("zfp_acc"), None);
        }
        assert_eq!(
            blosc2_compcode_to_compname(BLOSC_CODEC_ZFP_FIXED_PRECISION),
            Some("zfp_prec")
        );
        assert_eq!(
            blosc2_compcode_to_compname(BLOSC_CODEC_ZFP_FIXED_RATE),
            Some("zfp_rate")
        );
        assert_eq!(
            blosc2_compcode_to_compname(BLOSC_CODEC_OPENHTJ2K),
            Some("openhtj2k")
        );
        assert_eq!(blosc2_compcode_to_compname(BLOSC_CODEC_GROK), Some("grok"));
        assert_eq!(
            blosc2_compcode_to_compname(BLOSC_CODEC_OPENZL),
            Some("openzl")
        );
        assert_eq!(blosc2_compname_to_compcode("ndlz"), Some(BLOSC_CODEC_NDLZ));
        assert_eq!(
            blosc2_compname_to_compcode("zfp_prec"),
            Some(BLOSC_CODEC_ZFP_FIXED_PRECISION)
        );
        assert_eq!(
            blosc2_compname_to_compcode("zfp_rate"),
            Some(BLOSC_CODEC_ZFP_FIXED_RATE)
        );
        assert_eq!(
            blosc2_compname_to_compcode("openhtj2k"),
            Some(BLOSC_CODEC_OPENHTJ2K)
        );
        assert_eq!(blosc2_compname_to_compcode("grok"), Some(BLOSC_CODEC_GROK));
        assert_eq!(
            blosc2_compname_to_compcode("openzl"),
            Some(BLOSC_CODEC_OPENZL)
        );
        assert_eq!(blosc2_list_compressors(), "blosclz,lz4,lz4hc,zlib,zstd");
        assert_eq!(
            blosc2_get_complib_info("lz4hc"),
            Some((BLOSC_LZ4_FORMAT, "LZ4", "1.10.0"))
        );
        assert_eq!(
            blosc2_get_complib_info("blosclz"),
            Some((BLOSC_BLOSCLZ_FORMAT, "BloscLZ", "2.5.3"))
        );
        assert_eq!(
            blosc2_get_complib_info("zlib"),
            Some((BLOSC_ZLIB_FORMAT, "Zlib", "2.0.7"))
        );
        assert_eq!(
            blosc2_get_complib_info("zstd"),
            Some((BLOSC_ZSTD_FORMAT, "Zstd", "1.5.7"))
        );
        assert_eq!(blosc2_get_complib_info("LZ4"), None);
        assert_eq!(blosc2_compname_to_compcode_c("lz4"), BLOSC_LZ4 as i32);
        assert_eq!(blosc2_compname_to_compcode_c("LZ4"), -1);
        assert_eq!(
            blosc2_compcode_to_compname_c(BLOSC_LZ4),
            (BLOSC_LZ4 as i32, Some("lz4"))
        );
        assert_eq!(blosc2_compcode_to_compname_c(250), (250, None));
        assert_eq!(
            blosc2_compcode_to_compname_int_c(BLOSC_ZSTD as i32),
            (BLOSC_ZSTD as i32, Some("zstd"))
        );
        codecs::register_named_codec(
            210,
            "int-codec-lookup-user-codec",
            sequence_codec_compress,
            sequence_codec_decompress,
        )
        .unwrap();
        assert_eq!(
            blosc2_compcode_to_compname_int_c(210),
            (210, Some("int-codec-lookup-user-codec"))
        );
        assert_eq!(blosc2_compcode_to_compname_int_c(-1), (-1, None));
        assert_eq!(blosc2_compcode_to_compname_int_c(256), (-1, None));
        assert_eq!(blosc2_get_version_string(), "3.0.0.dev");
        assert!(blosc1_set_compressor("LZ4").is_err());
        assert_eq!(blosc1_get_compressor_code(), 255);
        blosc1_set_compressor_code(BLOSC_BLOSCLZ);
        assert_eq!(
            blosc1_set_compressor("zfp_rate"),
            Err("Unsupported Blosc1 compressor code")
        );
        assert_eq!(blosc1_get_compressor_code(), BLOSC_BLOSCLZ);
        assert_eq!(blosc1_set_compressor_c("LZ4"), -1);
        assert_eq!(blosc1_get_compressor_code(), 255);
        blosc1_set_compressor_code(BLOSC_BLOSCLZ);
        assert_eq!(
            blosc1_set_compressor_c("zfp_rate"),
            BLOSC2_ERROR_CODEC_SUPPORT
        );
        assert_eq!(blosc1_get_compressor_code(), BLOSC_BLOSCLZ);
        assert_eq!(blosc1_set_compressor_c("lz4"), BLOSC_LZ4 as i32);
        assert_eq!(blosc1_get_compressor_code(), BLOSC_LZ4);
        blosc1_set_compressor_code(BLOSC_BLOSCLZ);
        assert_eq!(blosc1_get_compressor_code(), BLOSC_BLOSCLZ);

        let _g = EnvGuard::set("BLOSC_COMPRESSOR", "LZ4");
        let mut clevel = 5;
        let mut doshuffle = BLOSC_SHUFFLE;
        let mut typesize = 4;
        let mut compcode = blosc1_get_compressor_code_i32();
        apply_blosc_env_overrides(&mut clevel, &mut doshuffle, &mut typesize, &mut compcode)
            .unwrap();
        assert_eq!(compcode, -1);
        assert_eq!(blosc1_get_compressor_code(), 255);
        blosc1_set_compressor_code(BLOSC_BLOSCLZ);
        assert_eq!(
            compress(
                b"payload",
                &CParams {
                    compcode: compcode as u8,
                    ..CParams::default()
                }
            ),
            Err("Unsupported codec")
        );
    }

    #[test]
    fn test_known_global_plugin_codecs_validate_static_support() {
        let data = b"plugin codec validation";
        for compcode in [BLOSC_CODEC_OPENHTJ2K, BLOSC_CODEC_GROK, BLOSC_CODEC_OPENZL] {
            let err = compress(
                data,
                &CParams {
                    compcode,
                    ..CParams::default()
                },
            )
            .unwrap_err();
            assert_eq!(err, "Global plugin codec is not supported");
        }
    }

    #[test]
    fn test_context_wrappers_forward_to_existing_apis() {
        let _lock = BLOSC_ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let _clevel = EnvGuard::remove("BLOSC_CLEVEL");
        let _shuffle = EnvGuard::remove("BLOSC_SHUFFLE");
        let _typesize = EnvGuard::remove("BLOSC_TYPESIZE");
        let _blocksize = EnvGuard::remove("BLOSC_BLOCKSIZE");
        let _nthreads = EnvGuard::remove("BLOSC_NTHREADS");
        let _splitmode = EnvGuard::remove("BLOSC_SPLITMODE");
        let prev_delta = blosc2_get_delta();
        blosc2_set_delta(0);

        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 4,
            blocksize: 256,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            nthreads: 2,
            ..Default::default()
        };
        let cctx = CContext::new(cparams.clone());
        assert_eq!(cctx.cparams().blocksize, 256);
        let cctx_c = blosc2_create_cctx(cparams.clone()).unwrap();
        let (cctx_rc, cctx_from_c) = blosc2_create_cctx_c(cparams.clone());
        assert_eq!(cctx_rc, BLOSC2_ERROR_SUCCESS);
        assert_eq!(cctx_from_c.as_ref().unwrap().cparams().blocksize, 256);
        assert_eq!(blosc2_free_ctx_c(cctx_from_c), BLOSC2_ERROR_SUCCESS);
        let (rc, returned_cparams) = blosc2_ctx_get_cparams(&cctx_c);
        assert_eq!(rc, BLOSC2_ERROR_SUCCESS);
        assert_eq!(returned_cparams.blocksize, 256);
        assert!(blosc2_create_cctx(CParams {
            filters: [0, 0, 0, 0, 0, BLOSC2_GLOBAL_REGISTERED_FILTERS_START - 1],
            ..cparams.clone()
        })
        .is_err());
        for filter in [BLOSC_FILTER_NDCELL, BLOSC_FILTER_NDMEAN] {
            assert!(blosc2_create_cctx(CParams {
                filters: [0, 0, 0, 0, 0, filter],
                ..cparams.clone()
            })
            .is_ok());
            assert_eq!(
                blosc2_create_cctx_c(CParams {
                    filters: [0, 0, 0, 0, 0, filter],
                    ..cparams.clone()
                })
                .0,
                BLOSC2_ERROR_SUCCESS
            );
            assert_eq!(
                compress(
                    b"hello world hello world",
                    &CParams {
                        filters: [0, 0, 0, 0, 0, filter],
                        ..cparams.clone()
                    }
                )
                .unwrap_err(),
                "Filter pipeline failed"
            );
            assert!(filters::is_registered_filter(filter));
            assert!(filters::registered_filter_info(filter).is_some());
        }
        for filter in [
            BLOSC_FILTER_BYTEDELTA_BUGGY,
            BLOSC_FILTER_BYTEDELTA,
            BLOSC_FILTER_INT_TRUNC,
        ] {
            assert!(blosc2_create_cctx(CParams {
                filters: [0, 0, 0, 0, 0, filter],
                filters_meta: [0, 0, 0, 0, 0, 4],
                ..cparams.clone()
            })
            .is_ok());
            assert!(compress(
                b"hello world hello world",
                &CParams {
                    filters: [0, 0, 0, 0, 0, filter],
                    filters_meta: [0, 0, 0, 0, 0, 4],
                    ..cparams.clone()
                }
            )
            .is_ok());
            assert!(filters::is_registered_filter(filter));
            assert!(filters::registered_filter_info(filter).is_some());
        }
        assert!(blosc2_create_cctx(CParams {
            filters: [0, 0, 0, 0, 0, BLOSC_FILTER_INT_TRUNC + 1],
            ..cparams.clone()
        })
        .is_err());
        assert!(
            blosc2_create_cctx_c(CParams {
                filters: [0, 0, 0, 0, 0, BLOSC_FILTER_INT_TRUNC + 1],
                ..cparams.clone()
            })
            .0 < 0
        );
        fn noop_filter(_meta: u8, _typesize: usize, _offset: usize, src: &[u8], dest: &mut [u8]) {
            dest.copy_from_slice(src);
        }
        filters::register_global_filter(BLOSC_FILTER_INT_TRUNC + 1, noop_filter, noop_filter)
            .unwrap();
        let (registered_global_rc, registered_global_ctx) = blosc2_create_cctx_c(CParams {
            filters: [0, 0, 0, 0, 0, BLOSC_FILTER_INT_TRUNC + 1],
            ..cparams.clone()
        });
        assert_ne!(registered_global_rc, BLOSC2_ERROR_SUCCESS);
        assert!(registered_global_ctx.is_none());
        let unregistered_user_filter = (BLOSC2_USER_REGISTERED_FILTERS_START
            ..=BLOSC2_USER_REGISTERED_FILTERS_STOP)
            .find(|&filter| !filters::is_registered_filter(filter))
            .unwrap();
        let (user_filter_rc, user_filter_ctx) = blosc2_create_cctx_c(CParams {
            filters: [0, 0, 0, 0, 0, unregistered_user_filter],
            ..cparams.clone()
        });
        assert_eq!(user_filter_rc, BLOSC2_ERROR_SUCCESS);
        let user_filter_ctx = user_filter_ctx.unwrap();
        let mut user_filter_dest = vec![0u8; 256];
        assert_eq!(
            blosc2_compress_ctx(
                &user_filter_ctx,
                b"hello world hello world",
                23,
                &mut user_filter_dest,
                256
            ),
            BLOSC2_ERROR_FILTER_PIPELINE
        );
        {
            let _shuffle_override = EnvGuard::set("BLOSC_SHUFFLE", "NOSHUFFLE");
            assert!(blosc2_create_cctx(CParams {
                filters: [0, 0, 0, 0, 0, BLOSC_FILTER_INT_TRUNC + 1],
                ..cparams.clone()
            })
            .is_err());
        }
        let data: Vec<u8> = (0..1024u32).flat_map(|i| i.to_le_bytes()).collect();
        let chunk = cctx.compress(&data).unwrap();
        assert_eq!(decompress(&chunk).unwrap(), data);
        let mut short_dest = vec![0u8; BLOSC2_MAX_OVERHEAD - 1];
        assert_eq!(
            cctx.compress_chunk_into(&data, &mut short_dest),
            Err("Destination too small")
        );
        let mut dest = vec![0u8; data.len() + BLOSC2_MAX_OVERHEAD + 1024];
        let dest_len = dest.len() as i32;
        let written = cctx.compress_chunk_into(&data, &mut dest).unwrap();
        assert_eq!(decompress(&dest[..written]).unwrap(), data);
        dest.fill(0);
        let written_c = blosc2_compress_ctx(&cctx_c, &data, data.len() as i32, &mut dest, dest_len);
        assert!(written_c > 0);
        assert_eq!(decompress(&dest[..written_c as usize]).unwrap(), data);
        let dict_unsupported_ctx = CContext::new(CParams {
            compcode: BLOSC_ZLIB,
            clevel: 5,
            typesize: 4,
            use_dict: true,
            ..cparams.clone()
        });
        assert_eq!(
            blosc2_compress_ctx(
                &dict_unsupported_ctx,
                &data,
                data.len() as i32,
                &mut dest,
                dest_len
            ),
            BLOSC2_ERROR_CODEC_PARAM
        );
        let short_dest_len = short_dest.len() as i32;
        assert_eq!(
            blosc2_compress_ctx(
                &cctx_c,
                &data,
                data.len() as i32,
                &mut short_dest,
                short_dest_len
            ),
            BLOSC2_ERROR_MAX_BUFSIZE_EXCEEDED
        );
        assert_eq!(
            blosc2_compress_ctx(&cctx_c, &data, -1, &mut dest, dest_len),
            BLOSC2_ERROR_INVALID_PARAM
        );
        assert_eq!(
            blosc2_compress_ctx(&cctx_c, &data, data.len() as i32, &mut dest, -1),
            BLOSC2_ERROR_MAX_BUFSIZE_EXCEEDED
        );

        let budgeted_cctx = CContext::new(CParams {
            clevel: 5,
            typesize: 1,
            blocksize: 16,
            filters: [0; BLOSC2_MAX_FILTERS],
            prefilter: Some(budget_probe_prefilter),
            ..Default::default()
        });
        let budgeted_data = vec![7u8; 128];
        let mut overhead_only = vec![0u8; BLOSC2_MAX_OVERHEAD];
        COMPRESSION_BUDGET_PREFILTER_CALLS.store(0, AtomicOrdering::SeqCst);
        assert_eq!(
            budgeted_cctx.compress_chunk_into(&budgeted_data, &mut overhead_only),
            Err("Destination too small")
        );
        assert_eq!(
            COMPRESSION_BUDGET_PREFILTER_CALLS.load(AtomicOrdering::SeqCst),
            0
        );
        COMPRESSION_BUDGET_PREFILTER_CALLS.store(0, AtomicOrdering::SeqCst);
        assert_eq!(
            blosc2_compress_ctx(
                &budgeted_cctx,
                &budgeted_data,
                budgeted_data.len() as i32,
                &mut overhead_only,
                BLOSC2_MAX_OVERHEAD as i32
            ),
            0
        );
        assert_eq!(
            COMPRESSION_BUDGET_PREFILTER_CALLS.load(AtomicOrdering::SeqCst),
            0
        );

        let dparams = DParams {
            nthreads: 2,
            ..Default::default()
        };
        let dctx = DContext::new(dparams.clone());
        assert_eq!(dctx.dparams().nthreads, 2);
        let dctx_c = blosc2_create_dctx(dparams).unwrap();
        let (dctx_rc, dctx_from_c) = blosc2_create_dctx_c(DParams {
            nthreads: 2,
            ..Default::default()
        });
        assert_eq!(dctx_rc, BLOSC2_ERROR_SUCCESS);
        assert_eq!(dctx_from_c.as_ref().unwrap().dparams().nthreads, 2);
        assert_eq!(blosc2_free_ctx_c(dctx_from_c), BLOSC2_ERROR_SUCCESS);
        let (rc, returned_dparams) = blosc2_ctx_get_dparams(&dctx_c);
        assert_eq!(rc, BLOSC2_ERROR_SUCCESS);
        assert_eq!(returned_dparams.nthreads, 2);
        let zero_thread_dctx = blosc2_create_dctx(DParams {
            nthreads: 0,
            ..Default::default()
        })
        .unwrap();
        assert_eq!(zero_thread_dctx.dparams().nthreads, 0);
        assert_eq!(dctx.decompress(&chunk).unwrap(), data);
        let mut restored = vec![0u8; data.len()];
        assert_eq!(
            blosc2_decompress_ctx(
                &dctx_c,
                &chunk,
                chunk.len() as i32,
                &mut restored,
                data.len() as i32
            ),
            data.len() as i32
        );
        assert_eq!(restored, data);
        let mut short_restored = vec![0u8; data.len() - 1];
        assert!(
            blosc2_decompress_ctx(
                &dctx_c,
                &chunk,
                chunk.len() as i32,
                &mut short_restored,
                (data.len() - 1) as i32
            ) < 0
        );
        assert_eq!(dctx.get_items(&chunk, 3, 5).unwrap(), data[12..32]);
        let mut item_dest = vec![0u8; 20];
        assert_eq!(
            dctx.get_items_into(&chunk, 3, 5, &mut item_dest).unwrap(),
            20
        );
        assert_eq!(item_dest, data[12..32]);
        let mut short_items = vec![0u8; 19];
        assert_eq!(
            dctx.get_items_into(&chunk, 3, 5, &mut short_items),
            Err("Destination too small")
        );

        let blocks: [&[u8]; 3] = [b"first", b"second-block", b"third"];
        let vlchunk = cctx.compress_vl_blocks(&blocks).unwrap();
        let mut vl_dest = vec![0u8; 1024];
        let vl_written = blosc2_vlcompress_ctx(&cctx_c, &blocks, &mut vl_dest, 1024);
        assert!(vl_written > 0);
        let short_vl_destsize = (vl_written - 1).max(BLOSC2_MAX_OVERHEAD as i32);
        let mut short_vl_dest = vec![0u8; short_vl_destsize as usize];
        assert_eq!(
            blosc2_vlcompress_ctx(&cctx_c, &blocks, &mut short_vl_dest, short_vl_destsize),
            0
        );
        let vl_written_c =
            blosc2_vlcompress_ctx_c(&cctx_c, &blocks, &[5, 12, 5], 3, &mut vl_dest, 1024);
        assert!(vl_written_c > 0);
        assert_eq!(
            blosc2_vlcompress_ctx_c(&cctx_c, &blocks, &[5, 12, 5], -1, &mut vl_dest, 1024),
            BLOSC2_ERROR_INVALID_PARAM
        );
        assert_eq!(
            blosc2_vlcompress_ctx_c(&cctx_c, &blocks, &[5, 12, 5], 0, &mut vl_dest, 1024),
            BLOSC2_ERROR_INVALID_PARAM
        );
        assert_eq!(
            blosc2_vlcompress_ctx_c(&cctx_c, &blocks, &[5, 0, 5], 3, &mut vl_dest, 1024),
            BLOSC2_ERROR_INVALID_PARAM
        );
        assert_eq!(
            blosc2_vlcompress_ctx_c(&cctx_c, &blocks, &[5, 99, 5], 3, &mut vl_dest, 1024),
            BLOSC2_ERROR_INVALID_PARAM
        );
        assert_eq!(
            blosc2_vlcompress_ctx(
                &cctx_c,
                &blocks,
                &mut vl_dest,
                (BLOSC2_MAX_OVERHEAD - 1) as i32
            ),
            BLOSC2_ERROR_MAX_BUFSIZE_EXCEEDED
        );
        assert_eq!(
            blosc2_vlcompress_ctx_c(&cctx_c, &blocks, &[5, 12, 5], 3, &mut vl_dest, -1),
            BLOSC2_ERROR_MAX_BUFSIZE_EXCEEDED
        );
        let budgeted_vl_cctx = CContext::new(CParams {
            clevel: 5,
            typesize: 1,
            filters: [0; BLOSC2_MAX_FILTERS],
            prefilter: Some(budget_probe_prefilter),
            ..Default::default()
        });
        let budgeted_blocks: [&[u8]; 3] = [b"alpha", b"bravo", b"charlie"];
        COMPRESSION_BUDGET_PREFILTER_CALLS.store(0, AtomicOrdering::SeqCst);
        assert_eq!(
            blosc2_vlcompress_ctx(
                &budgeted_vl_cctx,
                &budgeted_blocks,
                &mut overhead_only,
                BLOSC2_MAX_OVERHEAD as i32
            ),
            0
        );
        assert_eq!(
            COMPRESSION_BUDGET_PREFILTER_CALLS.load(AtomicOrdering::SeqCst),
            0
        );
        let zlib_dict_cctx = CContext {
            cparams: CParams {
                compcode: BLOSC_ZLIB,
                clevel: 5,
                typesize: 1,
                use_dict: true,
                ..Default::default()
            },
        };
        let zlib_vl_blocks: Vec<Vec<u8>> = (0..8)
            .map(|idx| format!("zlib-dict-fallback-block-{idx:02}-payload").into_bytes())
            .collect();
        let zlib_vl_refs: Vec<&[u8]> = zlib_vl_blocks.iter().map(Vec::as_slice).collect();
        let mut zlib_vl_dest = vec![0u8; 4096];
        let zlib_dict_written =
            blosc2_vlcompress_ctx(&zlib_dict_cctx, &zlib_vl_refs, &mut zlib_vl_dest, 4096);
        assert!(zlib_dict_written > 0, "{zlib_dict_written}");
        let zlib_dict_header =
            ChunkHeader::read(&zlib_vl_dest[..zlib_dict_written as usize]).unwrap();
        assert!(!zlib_dict_header.use_dict());
        assert_eq!(
            dctx.decompress_vl_blocks(&vl_dest[..vl_written as usize])
                .unwrap(),
            blocks
                .iter()
                .map(|block| block.to_vec())
                .collect::<Vec<_>>()
        );
        let (vl_count, vl_blocks) =
            blosc2_vldecompress_ctx(&dctx_c, &vl_dest[..vl_written as usize], vl_written);
        assert_eq!(vl_count, 3);
        assert_eq!(
            vl_blocks.unwrap(),
            blocks
                .iter()
                .map(|block| block.to_vec())
                .collect::<Vec<_>>()
        );
        let mut decoded_blocks = vec![Vec::new(), Vec::new(), Vec::new()];
        let mut decoded_sizes = vec![0; 3];
        assert_eq!(
            blosc2_vldecompress_ctx_c(
                &dctx_c,
                &vl_dest[..vl_written as usize],
                vl_written,
                &mut decoded_blocks,
                &mut decoded_sizes,
                3,
            ),
            3
        );
        assert_eq!(
            decoded_blocks,
            blocks
                .iter()
                .map(|block| block.to_vec())
                .collect::<Vec<_>>()
        );
        assert_eq!(decoded_sizes, vec![5, 12, 5]);
        assert_eq!(
            blosc2_vldecompress_ctx_c(
                &dctx_c,
                &vl_dest[..vl_written as usize],
                vl_written,
                &mut decoded_blocks[..2],
                &mut decoded_sizes[..2],
                2,
            ),
            BLOSC2_ERROR_INVALID_PARAM
        );
        assert_eq!(
            dctx.decompress_vl_blocks(&vlchunk).unwrap(),
            blocks
                .iter()
                .map(|block| block.to_vec())
                .collect::<Vec<_>>()
        );
        assert_eq!(
            dctx.decompress_vl_block(&vlchunk, 1).unwrap(),
            b"second-block"
        );
        assert_eq!(
            blosc2_vldecompress_block_ctx(&dctx_c, &vlchunk, vlchunk.len() as i32, 1),
            (b"second-block".len() as i32, Some(b"second-block".to_vec()))
        );
        let mut block_dest = vec![0u8; 16];
        block_dest.fill(0);
        let block_dest_len = block_dest.len() as i32;
        assert_eq!(
            blosc2_vldecompress_block_ctx_c(
                &dctx_c,
                &vlchunk,
                vlchunk.len() as i32,
                1,
                &mut block_dest,
                b"second-block".len() as i32
            ),
            b"second-block".len() as i32
        );
        assert_eq!(&block_dest[..b"second-block".len()], b"second-block");
        assert_eq!(
            blosc2_vldecompress_block_ctx_c(
                &dctx_c,
                &vlchunk,
                vlchunk.len() as i32,
                -1,
                &mut block_dest,
                block_dest_len
            ),
            BLOSC2_ERROR_INVALID_PARAM
        );
        assert_eq!(
            blosc2_vldecompress_block_ctx_c(
                &dctx_c,
                &vlchunk,
                vlchunk.len() as i32,
                1,
                &mut block_dest,
                -1
            ),
            BLOSC2_ERROR_INVALID_PARAM
        );
        assert_eq!(
            blosc2_vldecompress_block_ctx_c(
                &dctx_c,
                &vlchunk,
                vlchunk.len() as i32,
                1,
                &mut block_dest,
                4
            ),
            BLOSC2_ERROR_WRITE_BUFFER
        );
        assert!(blosc2_vldecompress_ctx(&dctx_c, &chunk, chunk.len() as i32).0 < 0);
        assert!(blosc2_vldecompress_block_ctx(&dctx_c, &chunk, chunk.len() as i32, 0).0 < 0);
        blosc2_free_ctx(cctx_c);
        blosc2_free_ctx(dctx_c);
        blosc2_set_delta_enabled(prev_delta);
    }

    #[test]
    fn test_context_creation_stores_c_like_env_overrides() {
        let _lock = BLOSC_ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let _shuffle = EnvGuard::remove("BLOSC_SHUFFLE");
        let _typesize = EnvGuard::remove("BLOSC_TYPESIZE");
        let _blocksize = EnvGuard::remove("BLOSC_BLOCKSIZE");
        let _splitmode = EnvGuard::remove("BLOSC_SPLITMODE");
        let _compressor = EnvGuard::remove("BLOSC_COMPRESSOR");
        let _delta = EnvGuard::remove("BLOSC_DELTA");
        let _clevel = EnvGuard::set("BLOSC_CLEVEL", "300");
        let _nthreads = EnvGuard::set("BLOSC_NTHREADS", "65537");

        let cctx = blosc2_create_cctx(CParams {
            clevel: 5,
            nthreads: 4,
            ..Default::default()
        })
        .unwrap();
        let (rc, cparams) = blosc2_ctx_get_cparams(&cctx);
        assert_eq!(rc, BLOSC2_ERROR_SUCCESS);
        assert_eq!(cparams.clevel, u8::MAX);
        assert_eq!(cparams.nthreads, 1);

        let data = b"context env payload";
        let mut dest = vec![0u8; data.len() + BLOSC2_MAX_OVERHEAD + 1024];
        let dest_len = dest.len() as i32;
        assert_eq!(
            blosc2_compress_ctx(&cctx, data, data.len() as i32, &mut dest, dest_len),
            BLOSC2_ERROR_CODEC_PARAM
        );

        let dctx = blosc2_create_dctx(DParams {
            nthreads: 4,
            ..Default::default()
        })
        .unwrap();
        let (rc, dparams) = blosc2_ctx_get_dparams(&dctx);
        assert_eq!(rc, BLOSC2_ERROR_SUCCESS);
        assert_eq!(dparams.nthreads, 1);
    }

    #[test]
    fn test_context_creation_rejects_env_selected_user_codec() {
        let _lock = BLOSC_ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let _shuffle = EnvGuard::remove("BLOSC_SHUFFLE");
        let _typesize = EnvGuard::remove("BLOSC_TYPESIZE");
        let _blocksize = EnvGuard::remove("BLOSC_BLOCKSIZE");
        let _splitmode = EnvGuard::remove("BLOSC_SPLITMODE");
        let _delta = EnvGuard::remove("BLOSC_DELTA");
        let _clevel = EnvGuard::remove("BLOSC_CLEVEL");
        let _nthreads = EnvGuard::remove("BLOSC_NTHREADS");

        let codec = codecs::Blosc2Codec {
            compcode: 209,
            compname: "env_codec_209",
            complib: BLOSC_UDCODEC_FORMAT,
            version: 1,
            encoder: sequence_codec_compress,
            decoder: sequence_codec_decompress,
        };
        assert_eq!(codecs::blosc2_register_codec(&codec), BLOSC2_ERROR_SUCCESS);

        let compressor = EnvGuard::set("BLOSC_COMPRESSOR", "env_codec_209");
        assert!(matches!(
            blosc2_create_cctx(CParams::default()),
            Err("Unsupported Blosc1 compressor code")
        ));
        drop(compressor);
        let _compressor = EnvGuard::remove("BLOSC_COMPRESSOR");
        assert!(blosc2_create_cctx(CParams {
            compcode: 209,
            typesize: 1,
            filters: [0; BLOSC2_MAX_FILTERS],
            ..Default::default()
        })
        .is_ok());
    }

    #[test]
    fn test_context_creation_preserves_unknown_env_compressor_code() {
        let _lock = BLOSC_ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let _shuffle = EnvGuard::remove("BLOSC_SHUFFLE");
        let _typesize = EnvGuard::remove("BLOSC_TYPESIZE");
        let _blocksize = EnvGuard::remove("BLOSC_BLOCKSIZE");
        let _splitmode = EnvGuard::remove("BLOSC_SPLITMODE");
        let _delta = EnvGuard::remove("BLOSC_DELTA");
        let _clevel = EnvGuard::remove("BLOSC_CLEVEL");
        let _nthreads = EnvGuard::remove("BLOSC_NTHREADS");
        let _compressor = EnvGuard::set("BLOSC_COMPRESSOR", "missing_codec_name");

        let (rc, cctx) = blosc2_create_cctx_c(CParams::default());
        assert_eq!(rc, BLOSC2_ERROR_SUCCESS);
        let cctx = cctx.expect("unknown compressor should still create a C context");
        let (_, cparams) = blosc2_ctx_get_cparams(&cctx);
        assert_eq!(cparams.compcode, u8::MAX);
        assert_eq!(
            blosc2_compress_ctx(&cctx, &[1, 2, 3, 4], 4, &mut [0; 64], 64),
            BLOSC2_ERROR_CODEC_SUPPORT
        );
    }

    #[test]
    fn test_dcontext_maskout_is_one_shot_and_leaves_blocks_untouched() {
        let data: Vec<u8> = (0..512u32).map(|i| (i & 0xff) as u8).collect();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            blocksize: 128,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0; BLOSC2_MAX_FILTERS],
            ..Default::default()
        };
        let chunk = compress(&data, &cparams).unwrap();
        let maskout = [false, true, false, true];
        let dctx = DContext::new(DParams::default());
        dctx.set_maskout(&maskout).unwrap();

        let mut dest = vec![0xA5; data.len()];
        assert_eq!(dctx.decompress_into(&chunk, &mut dest).unwrap(), data.len());
        assert_eq!(&dest[..128], &data[..128]);
        assert_eq!(&dest[128..256], &[0xA5; 128]);
        assert_eq!(&dest[256..384], &data[256..384]);
        assert_eq!(&dest[384..512], &[0xA5; 128]);

        dctx.set_maskout(&maskout).unwrap();
        dest.fill(0xA5);
        assert_eq!(
            blosc2_decompress_ctx(
                &dctx,
                &chunk,
                chunk.len() as i32,
                &mut dest,
                data.len() as i32
            ),
            data.len() as i32
        );
        assert_eq!(&dest[..128], &data[..128]);
        assert_eq!(&dest[128..256], &[0xA5; 128]);
        assert_eq!(&dest[256..384], &data[256..384]);
        assert_eq!(&dest[384..512], &[0xA5; 128]);

        assert_eq!(dctx.decompress_into(&chunk, &mut dest).unwrap(), data.len());
        assert_eq!(dest, data);
    }

    #[test]
    fn test_blosc2_decompress_ctx_consumes_maskout_on_error_returns() {
        let data: Vec<u8> = (0..512u32).map(|i| (i & 0xff) as u8).collect();
        let chunk = compress(
            &data,
            &CParams {
                compcode: BLOSC_LZ4,
                clevel: 5,
                typesize: 1,
                blocksize: 128,
                splitmode: BLOSC_NEVER_SPLIT,
                filters: [0; BLOSC2_MAX_FILTERS],
                ..Default::default()
            },
        )
        .unwrap();
        let dctx = DContext::new(DParams::default());
        let mut dest = vec![0xA5; data.len()];
        let dest_len = dest.len() as i32;

        dctx.set_maskout(&[true, false, false, false]).unwrap();
        assert_eq!(
            blosc2_decompress_ctx(&dctx, &chunk, -1, &mut dest, dest_len),
            BLOSC2_ERROR_READ_BUFFER
        );
        assert!(dctx.dparams().block_maskout.is_none());

        dctx.set_maskout(&[true, false, false, false]).unwrap();
        assert_eq!(
            blosc2_decompress_ctx(&dctx, &chunk, chunk.len() as i32, &mut dest, -1),
            BLOSC2_ERROR_WRITE_BUFFER
        );
        assert!(dctx.dparams().block_maskout.is_none());

        dctx.set_maskout(&[true, false, false, false]).unwrap();
        assert_eq!(
            blosc2_decompress_ctx(&dctx, &chunk, chunk.len() as i32, &mut dest, dest_len + 1,),
            BLOSC2_ERROR_INVALID_PARAM
        );
        assert!(dctx.dparams().block_maskout.is_none());

        assert_eq!(
            blosc2_decompress_ctx(&dctx, &chunk, chunk.len() as i32, &mut dest, dest_len),
            dest_len
        );
        assert_eq!(dest, data);
    }

    #[test]
    fn test_maskout_direct_params_serial_parallel_special_and_memcpy() {
        let data: Vec<u8> = (0..512u32).map(|i| (i & 0xff) as u8).collect();
        let maskout = vec![false, true, false, true];
        let base_cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            blocksize: 128,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0; BLOSC2_MAX_FILTERS],
            ..Default::default()
        };

        for nthreads in [1, 4] {
            let chunk = compress(&data, &base_cparams).unwrap();
            let dparams = DParams {
                nthreads,
                block_maskout: Some(maskout.clone()),
                ..Default::default()
            };
            let mut dest = vec![0xA5; data.len()];
            assert_eq!(
                decompress_into_with_dparams(&chunk, &mut dest, &dparams).unwrap(),
                data.len()
            );
            assert_eq!(&dest[..128], &data[..128]);
            assert_eq!(&dest[128..256], &[0xA5; 128]);
            assert_eq!(&dest[256..384], &data[256..384]);
            assert_eq!(&dest[384..512], &[0xA5; 128]);

            let allocated = decompress_with_dparams(&chunk, &dparams).unwrap();
            assert_eq!(&allocated[..128], &data[..128]);
            assert_eq!(&allocated[128..256], &[0; 128]);
            assert_eq!(&allocated[256..384], &data[256..384]);
            assert_eq!(&allocated[384..512], &[0; 128]);
        }

        let memcpy_chunk = compress(
            &data,
            &CParams {
                clevel: 0,
                ..base_cparams.clone()
            },
        )
        .unwrap();
        let dparams = DParams {
            block_maskout: Some(maskout.clone()),
            ..Default::default()
        };
        let mut dest = vec![0xA5; data.len()];
        assert_eq!(
            decompress_into_with_dparams(&memcpy_chunk, &mut dest, &dparams).unwrap(),
            data.len()
        );
        assert_eq!(&dest[..128], &data[..128]);
        assert_eq!(&dest[128..256], &[0xA5; 128]);

        let special_chunk = compress(&vec![0u8; data.len()], &base_cparams).unwrap();
        let mut dest = vec![0xA5; data.len()];
        assert_eq!(
            decompress_into_with_dparams(&special_chunk, &mut dest, &dparams).unwrap(),
            data.len()
        );
        assert_eq!(&dest[..128], &[0; 128]);
        assert_eq!(&dest[128..256], &[0xA5; 128]);
    }

    #[test]
    fn test_delta_maskout_masked_block_zero_matches_c_reference_state() {
        let data: Vec<u8> = (0..512u32)
            .flat_map(|i| i.wrapping_mul(17).to_le_bytes())
            .collect();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 4,
            blocksize: 512,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, BLOSC_DELTA, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let chunk = compress(&data, &cparams).unwrap();
        let dparams = DParams {
            block_maskout: Some(vec![true, false, false, false]),
            ..Default::default()
        };
        let mut dest = vec![0xA5; data.len()];

        assert_eq!(
            decompress_into_with_dparams(&chunk, &mut dest, &dparams).unwrap(),
            data.len()
        );
        let mut expected = vec![0u8; data.len() - 512];
        for block_idx in 1..4 {
            let block_start = block_idx * 512;
            for i in 0..512 {
                expected[(block_idx - 1) * 512 + i] = data[block_start + i] ^ data[i] ^ 0xA5;
            }
        }
        assert_eq!(&dest[..512], &[0xA5; 512]);
        assert_eq!(&dest[512..], &expected);

        let allocated = decompress_with_dparams(&chunk, &dparams).unwrap();
        let mut allocated_expected = vec![0u8; data.len() - 512];
        for block_idx in 1..4 {
            let block_start = block_idx * 512;
            for i in 0..512 {
                allocated_expected[(block_idx - 1) * 512 + i] = data[block_start + i] ^ data[i];
            }
        }
        assert_eq!(&allocated[..512], &[0; 512]);
        assert_eq!(&allocated[512..], &allocated_expected);
    }

    #[test]
    fn test_maskout_rejects_wrong_length() {
        let data: Vec<u8> = (0..512u32).map(|i| (i & 0xff) as u8).collect();
        let chunk = compress(
            &data,
            &CParams {
                compcode: BLOSC_LZ4,
                clevel: 5,
                typesize: 1,
                blocksize: 128,
                splitmode: BLOSC_NEVER_SPLIT,
                filters: [0; BLOSC2_MAX_FILTERS],
                ..Default::default()
            },
        )
        .unwrap();
        let dparams = DParams {
            block_maskout: Some(vec![false, true]),
            ..Default::default()
        };
        let mut dest = vec![0u8; data.len()];
        assert_eq!(
            decompress_into_with_dparams(&chunk, &mut dest, &dparams),
            Err("Maskout length must match the number of blocks")
        );
        assert_eq!(
            blosc2_error_code("Maskout length must match the number of blocks"),
            BLOSC2_ERROR_DATA
        );
    }

    #[test]
    fn test_vl_context_calls_honor_maskout_without_consuming_state() {
        let data: Vec<u8> = (0..128u32).map(|i| (i & 0xff) as u8).collect();
        let regular = compress(
            &data,
            &CParams {
                compcode: BLOSC_LZ4,
                clevel: 5,
                typesize: 1,
                blocksize: data.len() as i32,
                splitmode: BLOSC_NEVER_SPLIT,
                filters: [0; BLOSC2_MAX_FILTERS],
                ..Default::default()
            },
        )
        .unwrap();
        let blocks: [&[u8]; 2] = [b"alpha-block", b"beta-block"];
        let vlchunk = vlcompress(&blocks, &CParams::default()).unwrap();
        let vl_dparams = DParams {
            block_maskout: Some(vec![true, false]),
            ..Default::default()
        };
        let mut vl_dest = vec![0xA5; b"alpha-block".len() + b"beta-block".len()];
        let vl_dest_len = vl_dest.len();
        assert_eq!(
            decompress_into_with_dparams(&vlchunk, &mut vl_dest, &vl_dparams).unwrap(),
            vl_dest_len
        );
        assert_eq!(&vl_dest[..b"alpha-block".len()], &[0xA5; 11]);
        assert_eq!(&vl_dest[b"alpha-block".len()..], b"beta-block");
        let vl_allocated = decompress_with_dparams(&vlchunk, &vl_dparams).unwrap();
        assert_eq!(&vl_allocated[..b"alpha-block".len()], &[0; 11]);
        assert_eq!(&vl_allocated[b"alpha-block".len()..], b"beta-block");

        let dctx = DContext::new(DParams::default());
        dctx.set_maskout(&[true, false]).unwrap();
        assert_eq!(
            dctx.decompress_vl_blocks(&vlchunk).unwrap(),
            vec![vec![0; b"alpha-block".len()], b"beta-block".to_vec()]
        );
        assert_eq!(dctx.dparams().block_maskout, Some(vec![true, false]));
        dctx.set_maskout(&[false]).unwrap();
        assert_eq!(dctx.decompress(&regular).unwrap(), data);
        assert!(dctx.dparams().block_maskout.is_none());

        dctx.set_maskout(&[true, false]).unwrap();
        assert_eq!(
            dctx.decompress_vl_block(&vlchunk, 0).unwrap(),
            vec![0; b"alpha-block".len()]
        );
        assert_eq!(dctx.dparams().block_maskout, Some(vec![true, false]));

        assert_eq!(
            blosc2_vldecompress_ctx(&dctx, &vlchunk, vlchunk.len() as i32).0,
            2
        );
        assert_eq!(dctx.dparams().block_maskout, Some(vec![true, false]));

        assert_eq!(
            blosc2_vldecompress_block_ctx(&dctx, &vlchunk, vlchunk.len() as i32, 0,),
            (
                b"alpha-block".len() as i32,
                Some(vec![0; b"alpha-block".len()])
            )
        );
        assert_eq!(dctx.dparams().block_maskout, Some(vec![true, false]));

        let mut block_dest = vec![0xA5; b"alpha-block".len()];
        assert_eq!(
            blosc2_vldecompress_block_ctx_into(
                &dctx,
                &vlchunk,
                vlchunk.len() as i32,
                0,
                &mut block_dest,
            ),
            b"alpha-block".len() as i32
        );
        assert_eq!(block_dest, vec![0xA5; b"alpha-block".len()]);

        let mut blocks_out = vec![b"keep-alpha".to_vec(), b"keep-beta".to_vec()];
        let mut block_sizes = vec![111, 222];
        assert_eq!(
            blosc2_vldecompress_ctx_c(
                &dctx,
                &vlchunk,
                vlchunk.len() as i32,
                &mut blocks_out,
                &mut block_sizes,
                2,
            ),
            2
        );
        assert_eq!(blocks_out[0], vec![0; b"alpha-block".len()]);
        assert_eq!(block_sizes[0], b"alpha-block".len() as i32);
        assert_eq!(blocks_out[1], b"beta-block".to_vec());
        assert_eq!(block_sizes[1], b"beta-block".len() as i32);
        assert_eq!(dctx.dparams().block_maskout, Some(vec![true, false]));
    }

    #[test]
    fn test_masked_vl_decompress_returns_full_block_sizes() {
        let blocks: [&[u8]; 3] = [b"a", b"masked-variable-block", b"tail"];
        let vlchunk = vlcompress(
            &blocks,
            &CParams {
                compcode: BLOSC_LZ4,
                clevel: 5,
                typesize: 1,
                ..Default::default()
            },
        )
        .unwrap();
        let dparams = DParams {
            block_maskout: Some(vec![false, true, false]),
            ..Default::default()
        };

        let decoded = decompress_vl_blocks_with_dparams(&vlchunk, &dparams).unwrap();
        assert_eq!(
            decoded.iter().map(Vec::len).collect::<Vec<_>>(),
            blocks.iter().map(|block| block.len()).collect::<Vec<_>>()
        );
        assert_eq!(decoded[1], vec![0; blocks[1].len()]);
        assert_eq!(
            decompress_vl_block_with_dparams(&vlchunk, 1, &dparams).unwrap(),
            vec![0; blocks[1].len()]
        );

        let dctx = DContext::new(dparams);
        let (count, ctx_blocks) = blosc2_vldecompress_ctx(&dctx, &vlchunk, vlchunk.len() as i32);
        assert_eq!(count, blocks.len() as i32);
        let ctx_blocks = ctx_blocks.unwrap();
        assert_eq!(ctx_blocks[1].len(), blocks[1].len());
        assert_eq!(ctx_blocks[1], vec![0; blocks[1].len()]);

        let mut blocks_out = vec![Vec::new(), Vec::new(), Vec::new()];
        let mut block_sizes = vec![0; 3];
        assert_eq!(
            blosc2_vldecompress_ctx_c(
                &dctx,
                &vlchunk,
                vlchunk.len() as i32,
                &mut blocks_out,
                &mut block_sizes,
                3,
            ),
            3
        );
        assert_eq!(
            block_sizes,
            blocks
                .iter()
                .map(|block| block.len() as i32)
                .collect::<Vec<_>>()
        );
        assert_eq!(blocks_out[1], vec![0; blocks[1].len()]);
    }

    #[test]
    fn test_context_creation_honors_env_overrides() {
        let _lock = BLOSC_ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let prev_blocksize = blosc1_get_blocksize();
        let prev_splitmode = blosc1_get_splitmode();
        let prev_nthreads = blosc2_get_nthreads();
        let prev_compcode = blosc1_get_compressor_code();
        let prev_delta = blosc2_get_delta();

        let _clevel = EnvGuard::set("BLOSC_CLEVEL", "3");
        let _shuffle = EnvGuard::set("BLOSC_SHUFFLE", "NOSHUFFLE");
        let _delta = EnvGuard::set("BLOSC_DELTA", "1");
        let _typesize = EnvGuard::set("BLOSC_TYPESIZE", "2");
        let _compressor = EnvGuard::set("BLOSC_COMPRESSOR", "zstd");
        let _blocksize = EnvGuard::set("BLOSC_BLOCKSIZE", "128");
        let _nthreads = EnvGuard::set("BLOSC_NTHREADS", "2");
        let _splitmode = EnvGuard::set("BLOSC_SPLITMODE", "NEVER");

        let cctx = CContext::new(CParams::default());
        let cparams = cctx.cparams();
        assert_eq!(cparams.clevel, 3);
        assert_eq!(cparams.typesize, 2);
        assert_eq!(cparams.compcode, BLOSC_ZSTD);
        assert_eq!(cparams.blocksize, 128);
        assert_eq!(cparams.nthreads, 2);
        assert_eq!(cparams.splitmode, BLOSC_NEVER_SPLIT);
        assert_eq!(cparams.filters[BLOSC2_MAX_FILTERS - 1], BLOSC_NOFILTER);
        assert_eq!(cparams.filters[BLOSC2_MAX_FILTERS - 2], BLOSC_DELTA);

        let dctx = DContext::new(DParams::default());
        assert_eq!(dctx.dparams().nthreads, 2);

        assert_eq!(blosc1_get_blocksize(), prev_blocksize);
        assert_eq!(blosc1_get_splitmode(), prev_splitmode);
        assert_eq!(blosc2_get_nthreads(), prev_nthreads);
        assert_eq!(blosc1_get_compressor_code(), prev_compcode);
        assert_eq!(blosc2_get_delta(), prev_delta);

        blosc1_set_blocksize(prev_blocksize);
        blosc1_set_splitmode(prev_splitmode);
        let _ = blosc2_set_nthreads(prev_nthreads);
        blosc1_set_compressor_code(prev_compcode);
        blosc2_set_delta_enabled(prev_delta);
    }

    #[test]
    fn test_context_creation_checks_env_nthreads_after_i16_cast() {
        let _lock = BLOSC_ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let _compressor = EnvGuard::remove("BLOSC_COMPRESSOR");
        let _nthreads = EnvGuard::set("BLOSC_NTHREADS", "40000");

        let cctx = blosc2_create_cctx(CParams {
            nthreads: 7,
            ..Default::default()
        })
        .unwrap();
        assert_eq!(cctx.cparams().nthreads, 7);

        let dctx = blosc2_create_dctx(DParams {
            nthreads: 7,
            ..Default::default()
        })
        .unwrap();
        assert_eq!(dctx.dparams().nthreads, 7);

        drop(_nthreads);
        let _nthreads = EnvGuard::set("BLOSC_NTHREADS", "65537");
        let expected = 65537_i32 as i16;

        let cctx = blosc2_create_cctx(CParams::default()).unwrap();
        assert_eq!(cctx.cparams().nthreads, expected);

        let dctx = blosc2_create_dctx(DParams::default()).unwrap();
        assert_eq!(dctx.dparams().nthreads, expected);
    }

    #[test]
    fn test_context_creation_preserves_filters_without_env_shuffle() {
        let _lock = BLOSC_ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let _shuffle = EnvGuard::remove("BLOSC_SHUFFLE");
        let _delta = EnvGuard::remove("BLOSC_DELTA");

        let cctx = CContext::new(CParams {
            typesize: 1,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        });
        assert_eq!(
            cctx.cparams().filters[BLOSC2_MAX_FILTERS - 1],
            BLOSC_SHUFFLE
        );

        let _shuffle = EnvGuard::set("BLOSC_SHUFFLE", "SHUFFLE");
        let cctx = CContext::new(CParams {
            typesize: 1,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        });
        assert_eq!(
            cctx.cparams().filters[BLOSC2_MAX_FILTERS - 1],
            BLOSC_SHUFFLE
        );

        let _shuffle = EnvGuard::set("BLOSC_SHUFFLE", "BITSHUFFLE");
        let cctx = CContext::new(CParams {
            typesize: 1,
            filters: [0; BLOSC2_MAX_FILTERS],
            ..Default::default()
        });
        assert_eq!(
            cctx.cparams().filters[BLOSC2_MAX_FILTERS - 1],
            BLOSC_BITSHUFFLE
        );

        let _delta = EnvGuard::set("BLOSC_DELTA", "0");
        let cctx = CContext::new(CParams {
            filters: [0, 0, 0, 0, BLOSC_DELTA, BLOSC_SHUFFLE],
            ..Default::default()
        });
        assert_eq!(cctx.cparams().filters[BLOSC2_MAX_FILTERS - 2], BLOSC_DELTA);
    }

    #[test]
    fn test_dcontext_getitem_honors_postfilter() {
        let data: Vec<u8> = (0..128u8).collect();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            blocksize: 32,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0; BLOSC2_MAX_FILTERS],
            ..Default::default()
        };
        let chunk = compress(&data, &cparams).unwrap();
        let dctx = DContext::new(DParams {
            postfilter: Some(xor_postfilter),
            typesize: 1,
            ..Default::default()
        });
        let expected: Vec<u8> = data[10..30].iter().map(|byte| byte ^ 0x5a).collect();
        assert_eq!(dctx.get_items(&chunk, 10, 20).unwrap(), expected);

        let zero = blosc2_chunk_zeros(64, 1).unwrap();
        assert_eq!(dctx.get_items(&zero, 8, 4).unwrap(), vec![0x5a; 4]);
    }

    #[test]
    fn test_blosc1_compress_honors_blosc_clevel_env() {
        let _lock = BLOSC_ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        // Use somewhat compressible data so level differences are observable.
        let data: Vec<u8> = (0..8192u32).flat_map(|i| (i % 37).to_le_bytes()).collect();
        let mut a = vec![0u8; data.len() + BLOSC2_MAX_OVERHEAD + 1024];

        // clevel=0 → roundtrip must still succeed; we test that the env var is applied
        // by verifying output differs from a default caller-level run.
        let csize_default = blosc1_compress(5, BLOSC_SHUFFLE, 4, &data, &mut a).unwrap();

        let _g = EnvGuard::set("BLOSC_CLEVEL", "0");
        let mut b = vec![0u8; data.len() + BLOSC2_MAX_OVERHEAD + 1024];
        let csize_env = blosc1_compress(5, BLOSC_SHUFFLE, 4, &data, &mut b).unwrap();

        assert_ne!(
            csize_env, csize_default,
            "BLOSC_CLEVEL=0 should change output size compared to caller-requested clevel=5"
        );

        let mut restored = vec![0u8; data.len()];
        let dsize = blosc1_decompress(&b[..csize_env], &mut restored).unwrap();
        assert_eq!(dsize, data.len());
        assert_eq!(restored, data);

        drop(_g);
        let _g = EnvGuard::set("BLOSC_CLEVEL", "0junk");
        let csize_env = blosc1_compress(5, BLOSC_SHUFFLE, 4, &data, &mut b).unwrap();
        let header = ChunkHeader::read(&b[..csize_env]).unwrap();
        assert!(header.memcpyed());
    }

    #[test]
    fn test_blosc1_compress_rejects_invalid_blosc_clevel_env() {
        let _lock = BLOSC_ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let data = b"compressible payload";
        let mut compressed = vec![0u8; data.len() + BLOSC2_MAX_OVERHEAD + 1024];

        let _g = EnvGuard::set("BLOSC_CLEVEL", "10");
        assert!(blosc1_compress(5, BLOSC_SHUFFLE, 4, data, &mut compressed).is_err());
        assert_eq!(
            blosc1_compress_c(5, BLOSC_SHUFFLE as i32, 4, data, &mut compressed),
            BLOSC2_ERROR_CODEC_PARAM
        );
        drop(_g);

        let _g = EnvGuard::set("BLOSC_CLEVEL", "300");
        assert!(blosc1_compress(5, BLOSC_SHUFFLE, 4, data, &mut compressed).is_err());
        assert_eq!(
            blosc1_compress_c(5, BLOSC_SHUFFLE as i32, 4, data, &mut compressed),
            BLOSC2_ERROR_CODEC_PARAM
        );
        drop(_g);

        let _g = EnvGuard::set("BLOSC_CLEVEL", "junk");
        let csize = blosc1_compress(5, BLOSC_SHUFFLE, 4, data, &mut compressed).unwrap();
        let header = ChunkHeader::read(&compressed[..csize]).unwrap();
        assert!(header.memcpyed());
    }

    #[test]
    fn test_c_compress_negative_clevel_can_be_overridden_by_env() {
        let _lock = BLOSC_ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let data = b"abcdabcdabcdabcd";
        let mut compressed = vec![0u8; 128];

        let _g = EnvGuard::set("BLOSC_CLEVEL", "5");
        assert!(blosc1_compress_c(-1, BLOSC_SHUFFLE as i32, 1, data, &mut compressed,) > 0);
        assert!(
            blosc2_compress(
                -1,
                BLOSC_SHUFFLE as i32,
                1,
                data,
                data.len() as i32,
                &mut compressed,
                128,
            ) > 0
        );
    }

    #[test]
    fn test_blosc1_compress_honors_blosc_shuffle_env() {
        let _lock = BLOSC_ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let data: Vec<u8> = (0..1024u32).flat_map(|i| i.to_le_bytes()).collect();
        let mut compressed = vec![0u8; data.len() + BLOSC2_MAX_OVERHEAD + 1024];

        let _g = EnvGuard::set("BLOSC_SHUFFLE", "BITSHUFFLE");
        // Caller asks for BLOSC_SHUFFLE; env should override to BITSHUFFLE.
        let csize = blosc1_compress(5, BLOSC_SHUFFLE, 4, &data, &mut compressed).unwrap();

        let (_, _, filters) = chunk_metainfo(&compressed[..csize]).unwrap();
        // Last filter slot is the primary filter in blosc1 wrappers.
        assert_eq!(
            filters[BLOSC2_MAX_FILTERS - 1],
            BLOSC_BITSHUFFLE,
            "BLOSC_SHUFFLE=BITSHUFFLE env should override caller-specified SHUFFLE"
        );
    }

    #[test]
    fn test_blosc1_small_dest_returns_zero_before_special_zero() {
        let _lock = BLOSC_ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let prev_nt = blosc2_get_nthreads();
        let data = vec![0u8; 4096];
        let mut too_small = vec![0u8; BLOSC2_MAX_OVERHEAD - 1];

        assert!(blosc1_compress(5, BLOSC_SHUFFLE, 1, &data, &mut too_small).is_err());
        let _nthreads = EnvGuard::set("BLOSC_NTHREADS", "4");
        blosc2_set_nthreads(1);
        assert_eq!(
            blosc1_compress_c(5, BLOSC_SHUFFLE as i32, 1, &data, &mut too_small),
            BLOSC2_ERROR_MAX_BUFSIZE_EXCEEDED
        );
        assert_eq!(blosc2_get_nthreads(), 4);
        drop(_nthreads);

        let data: Vec<u8> = (0..512u32).flat_map(|i| i.to_le_bytes()).collect();
        let mut compressed = vec![0u8; data.len() + BLOSC2_MAX_OVERHEAD - 1];

        assert_eq!(
            blosc1_compress(0, BLOSC_NOFILTER, 1, &data, &mut compressed).unwrap(),
            0
        );
        assert_eq!(
            blosc1_compress_c(0, BLOSC_NOFILTER as i32, 1, &data, &mut compressed),
            0
        );

        let _ = blosc2_set_nthreads(prev_nt);
    }

    #[test]
    fn test_blosc1_c_adapter_accepts_c_int_arguments() {
        let _lock = BLOSC_ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let data: Vec<u8> = (0..1024u32).flat_map(|i| i.to_le_bytes()).collect();
        let mut compressed = vec![0u8; data.len() + BLOSC2_MAX_OVERHEAD + 1024];

        assert_eq!(
            blosc1_compress_c(-1, BLOSC_SHUFFLE as i32, 4, &data, &mut compressed),
            BLOSC2_ERROR_CODEC_PARAM
        );
        assert_eq!(
            blosc1_compress_c(256, BLOSC_SHUFFLE as i32, 4, &data, &mut compressed),
            BLOSC2_ERROR_CODEC_PARAM
        );
        let csize = blosc1_compress_c(5, 300, 4, &data, &mut compressed);
        assert!(csize > 0);
        let (_, _, filters) = chunk_metainfo(&compressed[..csize as usize]).unwrap();
        assert_eq!(filters[BLOSC2_MAX_FILTERS - 1], BLOSC_NOFILTER);
    }

    #[test]
    fn test_blosc1_shuffle_typesize_one_is_nofilter() {
        let _lock = BLOSC_ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let data: Vec<u8> = (0..=255u8).cycle().take(4096).collect();
        let mut compressed = vec![0u8; data.len() + BLOSC2_MAX_OVERHEAD + 1024];

        let csize = blosc1_compress(5, BLOSC_SHUFFLE, 1, &data, &mut compressed).unwrap();
        let (_, _, filters) = chunk_metainfo(&compressed[..csize]).unwrap();
        assert_eq!(filters[BLOSC2_MAX_FILTERS - 1], BLOSC_NOFILTER);
        assert_eq!(decompress(&compressed[..csize]).unwrap(), data);

        let csize = blosc1_compress(5, BLOSC_BITSHUFFLE, 1, &data, &mut compressed).unwrap();
        let (_, _, filters) = chunk_metainfo(&compressed[..csize]).unwrap();
        assert_eq!(filters[BLOSC2_MAX_FILTERS - 1], BLOSC_BITSHUFFLE);
        assert_eq!(decompress(&compressed[..csize]).unwrap(), data);
    }

    #[test]
    fn test_blosc1_unknown_doshuffle_falls_back_to_nofilter() {
        let _lock = BLOSC_ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let _shuffle = EnvGuard::remove("BLOSC_SHUFFLE");
        let data: Vec<u8> = (0..1024u32).flat_map(|i| i.to_le_bytes()).collect();
        let mut compressed = vec![0u8; data.len() + BLOSC2_MAX_OVERHEAD + 1024];

        let csize = blosc1_compress(5, 99, 4, &data, &mut compressed).unwrap();
        let (_, _, filters) = chunk_metainfo(&compressed[..csize]).unwrap();
        assert_eq!(filters[BLOSC2_MAX_FILTERS - 1], BLOSC_NOFILTER);
        assert_eq!(decompress(&compressed[..csize]).unwrap(), data);
    }

    #[test]
    fn test_blosc1_set_compressor_changes_codec() {
        let _lock = BLOSC_ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        // Ensure env override isn't also in play.
        let _unset = EnvGuard {
            key: "BLOSC_COMPRESSOR",
            prev: std::env::var_os("BLOSC_COMPRESSOR"),
        };
        unsafe { std::env::remove_var("BLOSC_COMPRESSOR") };

        let data: Vec<u8> = (0..1024u32).flat_map(|i| i.to_le_bytes()).collect();
        let mut compressed = vec![0u8; data.len() + BLOSC2_MAX_OVERHEAD + 1024];

        let prev = blosc1_get_compressor_code();
        let selected = blosc1_set_compressor("zstd").expect("zstd is a recognized codec name");
        assert_eq!(selected, BLOSC_ZSTD);
        let csize = blosc1_compress(5, BLOSC_SHUFFLE, 4, &data, &mut compressed).unwrap();
        let (_, compcode, _) = chunk_metainfo(&compressed[..csize]).unwrap();
        assert_eq!(compcode, BLOSC_ZSTD);

        // Restore.
        blosc1_set_compressor_code(prev);
    }

    #[test]
    fn test_blosc1_set_compressor_rejects_named_user_codecs() {
        let _lock = BLOSC_ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let prev = blosc1_get_compressor_code();
        let user_codec_id = 205;
        codecs::register_named_codec(
            user_codec_id,
            "blosc1-rejected-user-codec",
            sequence_codec_compress,
            sequence_codec_decompress,
        )
        .unwrap();

        assert_eq!(
            blosc2_compname_to_compcode("blosc1-rejected-user-codec"),
            Some(user_codec_id)
        );
        assert_eq!(
            blosc1_set_compressor("blosc1-rejected-user-codec"),
            Err("Unsupported Blosc1 compressor code")
        );
        assert_eq!(
            blosc1_set_compressor_c("blosc1-rejected-user-codec"),
            BLOSC2_ERROR_CODEC_SUPPORT
        );
        assert_eq!(blosc1_set_compressor_c("missing-codec"), -1);
        assert_eq!(blosc1_get_compressor_code(), 255);
        assert_eq!(blosc1_get_compressor(), None);
        assert_eq!(blosc_get_compressor(), None);
        assert_eq!(blosc1_get_compressor_or_unknown(), "unknown");
        blosc1_set_compressor_code(user_codec_id);
        assert_eq!(blosc1_get_compressor(), Some("blosc1-rejected-user-codec"));
        assert_eq!(blosc_get_compressor(), Some("blosc1-rejected-user-codec"));
        assert_eq!(
            blosc1_get_compressor_or_unknown(),
            "blosc1-rejected-user-codec"
        );
        blosc1_set_compressor_code(prev);
    }

    #[test]
    fn test_blosc1_get_compressor_returns_name() {
        let _lock = BLOSC_ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let prev = blosc1_get_compressor_code();
        let selected = blosc1_set_compressor("lz4").unwrap();
        assert_eq!(selected, BLOSC_LZ4);
        assert_eq!(blosc1_get_compressor(), Some("lz4"));
        assert_eq!(blosc1_get_compressor_or_unknown(), "lz4");
        blosc1_set_compressor_code(prev);
    }

    #[test]
    fn test_blosc1_compress_honors_blosc_delta_env() {
        let _lock = BLOSC_ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let prev_delta = blosc2_get_delta();
        let data: Vec<u8> = (0..2048u32).flat_map(|i| i.to_le_bytes()).collect();
        let mut compressed = vec![0u8; data.len() + BLOSC2_MAX_OVERHEAD + 1024];

        // Ensure the global starts off.
        blosc2_set_delta(0);

        let _g = EnvGuard::set("BLOSC_DELTA", "1");
        let csize = blosc1_compress(5, BLOSC_SHUFFLE, 4, &data, &mut compressed).unwrap();

        // Env var should have flipped the global on, and the chunk header
        // should reflect a BLOSC_DELTA filter at slot 4.
        assert!(blosc2_get_delta(), "BLOSC_DELTA=1 must set the global");
        blosc2_set_delta(2);
        assert!(blosc2_get_delta());
        blosc2_set_delta(-1);
        assert!(blosc2_get_delta());
        blosc2_set_delta(0);
        assert!(!blosc2_get_delta());
        blosc2_set_delta(1);
        let (_, _, filters) = chunk_metainfo(&compressed[..csize]).unwrap();
        assert_eq!(
            filters[BLOSC2_MAX_FILTERS - 2],
            BLOSC_DELTA,
            "delta filter must land in slot 4 of the chunk filters array"
        );

        // Roundtrip must still work.
        let mut restored = vec![0u8; data.len()];
        let dsize = blosc1_decompress(&compressed[..csize], &mut restored).unwrap();
        assert_eq!(dsize, data.len());
        assert_eq!(restored, data);

        // Restore.
        blosc2_set_delta_enabled(prev_delta);
    }

    #[test]
    fn test_blosc1_compress_honors_blosc_blocksize_env() {
        let _lock = BLOSC_ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let prev_bs = blosc1_get_blocksize();
        blosc1_set_blocksize(0); // start from automatic

        let data: Vec<u8> = (0..16384u32).flat_map(|i| i.to_le_bytes()).collect();
        let mut compressed = vec![0u8; data.len() + BLOSC2_MAX_OVERHEAD + 1024];

        let _nolock = EnvGuard::remove("BLOSC_NOLOCK");
        let _g = EnvGuard::set("BLOSC_BLOCKSIZE", "4096");
        let csize = blosc1_compress(5, BLOSC_SHUFFLE, 4, &data, &mut compressed).unwrap();

        let (_, _, blocksize) = chunk_sizes(&compressed[..csize]).unwrap();
        assert_eq!(
            blocksize, 4096,
            "BLOSC_BLOCKSIZE=4096 must be reflected in the chunk header"
        );

        let _nolock_set = EnvGuard::set("BLOSC_NOLOCK", "1");
        let csize = blosc1_compress(5, BLOSC_SHUFFLE, 4, &data, &mut compressed).unwrap();
        let header = ChunkHeader::read(&compressed[..csize]).unwrap();
        let (_, _, blocksize) = chunk_sizes(&compressed[..csize]).unwrap();
        assert!(header.is_extended());
        assert_ne!(
            blocksize, 4096,
            "BLOSC_NOLOCK takes C's context path and ignores g_force_blocksize"
        );
        drop(_nolock_set);
        drop(_g);

        let _g = EnvGuard::set("BLOSC_BLOCKSIZE", "1000000000");
        let csize = blosc1_compress(5, BLOSC_SHUFFLE, 4, &data, &mut compressed).unwrap();
        let (_, _, blocksize) = chunk_sizes(&compressed[..csize]).unwrap();
        assert_eq!(
            blocksize,
            data.len(),
            "oversized positive BLOSC_BLOCKSIZE is tuned down to nbytes"
        );

        blosc1_set_blocksize(prev_bs);
    }

    #[test]
    fn test_blosc1_nolock_casts_typesize_like_c() {
        let _lock = BLOSC_ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let _nolock = EnvGuard::set("BLOSC_NOLOCK", "1");
        let _typesize = EnvGuard::set("BLOSC_TYPESIZE", "300");
        let _compat = EnvGuard::remove("BLOSC_BLOSC1_COMPAT");

        let data = vec![7u8; 44 * 4];
        let mut compressed = vec![0u8; data.len() + BLOSC2_MAX_OVERHEAD + 1024];
        let csize = blosc1_compress(5, BLOSC_SHUFFLE, 1, &data, &mut compressed).unwrap();
        let header = ChunkHeader::read(&compressed[..csize]).unwrap();
        assert!(header.is_extended());
        assert_eq!(header.typesize, 1);
        assert_eq!(decompress(&compressed[..csize]).unwrap(), data);
    }

    #[test]
    fn test_blosc1_nolock_rechecks_env_clevel_like_cctx() {
        let _lock = BLOSC_ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let _nolock = EnvGuard::set("BLOSC_NOLOCK", "1");
        let _clevel = EnvGuard::set("BLOSC_CLEVEL", "256");
        let _compat = EnvGuard::remove("BLOSC_BLOSC1_COMPAT");

        let data = b"compressible payload";
        let mut compressed = vec![0u8; data.len() + BLOSC2_MAX_OVERHEAD + 1024];
        assert!(blosc1_compress(5, BLOSC_SHUFFLE, 1, data, &mut compressed).is_err());
        assert_eq!(
            blosc1_compress_c(5, BLOSC_SHUFFLE as i32, 1, data, &mut compressed),
            BLOSC2_ERROR_CODEC_PARAM
        );
    }

    #[test]
    fn test_bounded_compress_ctx_uses_memcpy_last_chance_like_c() {
        let data: Vec<u8> = (0..96u32)
            .map(|i| ((i.wrapping_mul(1_103_515_245).wrapping_add(12_345)) >> 16) as u8)
            .collect();
        let cctx = blosc2_create_cctx(CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            blocksize: 1,
            filters: [0; BLOSC2_MAX_FILTERS],
            ..Default::default()
        })
        .unwrap();
        let mut compressed = vec![0u8; BLOSC_EXTENDED_HEADER_LENGTH + data.len()];

        let csize = blosc2_compress_ctx(
            &cctx,
            &data,
            data.len() as i32,
            &mut compressed,
            (BLOSC_EXTENDED_HEADER_LENGTH + data.len()) as i32,
        );

        assert_eq!(csize, (BLOSC_EXTENDED_HEADER_LENGTH + data.len()) as i32);
        let header = ChunkHeader::read(&compressed[..csize as usize]).unwrap();
        assert!(header.memcpyed());
        assert_eq!(decompress(&compressed[..csize as usize]).unwrap(), data);
    }

    #[test]
    fn test_block_encoder_reports_destination_too_small_like_c_blosc_c() {
        let data: Vec<u8> = (0..128u32)
            .map(|i| ((i.wrapping_mul(1_103_515_245).wrapping_add(12_345)) >> 16) as u8)
            .collect();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            blocksize: data.len() as i32,
            filters: [0; BLOSC2_MAX_FILTERS],
            nthreads: 1,
            ..Default::default()
        };
        let mut buf1 = Vec::new();
        let mut buf2 = Vec::new();
        let mut compress_buf = Vec::new();
        let mut prefilter_buf = Vec::new();

        assert_eq!(
            compress_block_with_scratch(
                &data,
                &data,
                0,
                data.len(),
                false,
                &cparams,
                true,
                1,
                &mut buf1,
                &mut buf2,
                &mut compress_buf,
                &mut prefilter_buf,
                0,
                Some(4),
            ),
            Err("Destination too small")
        );
    }

    #[test]
    fn test_bounded_memcpy_last_chance_runs_prefilter_like_c() {
        let data: Vec<u8> = (0..96u32)
            .map(|i| ((i.wrapping_mul(1_103_515_245).wrapping_add(12_345)) >> 16) as u8)
            .collect();
        let cctx = blosc2_create_cctx(CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            blocksize: 64,
            filters: [0; BLOSC2_MAX_FILTERS],
            prefilter: Some(xor_prefilter),
            ..Default::default()
        })
        .unwrap();
        let mut compressed = vec![0u8; BLOSC_EXTENDED_HEADER_LENGTH + data.len()];

        let csize = blosc2_compress_ctx(
            &cctx,
            &data,
            data.len() as i32,
            &mut compressed,
            (BLOSC_EXTENDED_HEADER_LENGTH + data.len()) as i32,
        );

        assert_eq!(csize, (BLOSC_EXTENDED_HEADER_LENGTH + data.len()) as i32);
        let chunk = &compressed[..csize as usize];
        let header = ChunkHeader::read(chunk).unwrap();
        assert!(header.memcpyed());
        let expected: Vec<u8> = data.iter().map(|byte| byte ^ 0x5a).collect();
        assert_eq!(decompress(chunk).unwrap(), expected);
    }

    #[test]
    fn test_bounded_header_table_memcpy_fallback_flags_like_c() {
        let data: Vec<u8> = (0..96u32)
            .map(|i| ((i.wrapping_mul(1_103_515_245).wrapping_add(12_345)) >> 16) as u8)
            .collect();
        let cctx = blosc2_create_cctx(CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            blocksize: 1,
            filters: [0; BLOSC2_MAX_FILTERS],
            ..Default::default()
        })
        .unwrap();
        let mut compressed = vec![0u8; BLOSC_EXTENDED_HEADER_LENGTH + data.len()];

        let csize = blosc2_compress_ctx(
            &cctx,
            &data,
            data.len() as i32,
            &mut compressed,
            (BLOSC_EXTENDED_HEADER_LENGTH + data.len()) as i32,
        );

        assert_eq!(csize, (BLOSC_EXTENDED_HEADER_LENGTH + data.len()) as i32);
        assert_eq!(
            compressed[BLOSC2_CHUNK_FLAGS] & (BLOSC_DOSHUFFLE | BLOSC_DOBITSHUFFLE),
            BLOSC_DOSHUFFLE | BLOSC_DOBITSHUFFLE
        );
        assert_eq!(decompress(&compressed[..csize as usize]).unwrap(), data);
    }

    #[test]
    fn test_blosc1_compress_honors_blosc_splitmode_env() {
        let _lock = BLOSC_ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let prev_sm = blosc1_get_splitmode();
        // Sanity: `NEVER` is observable via the `BLOSC_DONT_SPLIT` flag in the header.
        // Use a codec/typesize combination that *would* otherwise split.
        let data: Vec<u8> = (0..16384u32).flat_map(|i| i.to_le_bytes()).collect();
        let mut compressed = vec![0u8; data.len() + BLOSC2_MAX_OVERHEAD + 1024];

        let _g = EnvGuard::set("BLOSC_SPLITMODE", "NEVER");
        let csize = blosc1_compress(5, BLOSC_SHUFFLE, 4, &data, &mut compressed).unwrap();

        let header = ChunkHeader::read(&compressed[..csize]).unwrap();
        assert!(
            header.dont_split(),
            "BLOSC_SPLITMODE=NEVER must set the DONT_SPLIT flag"
        );

        blosc1_set_splitmode(prev_sm);
    }

    #[test]
    fn test_blosc1_compress_honors_blosc_nthreads_env() {
        let _lock = BLOSC_ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let prev_nt = blosc2_get_nthreads();
        blosc2_set_nthreads(1);

        let data: Vec<u8> = (0..32768u32).flat_map(|i| i.to_le_bytes()).collect();
        let mut compressed = vec![0u8; data.len() + BLOSC2_MAX_OVERHEAD + 1024];

        let _g = EnvGuard::set("BLOSC_NTHREADS", "4");
        let csize = blosc1_compress(5, BLOSC_SHUFFLE, 4, &data, &mut compressed).unwrap();

        // Observable effect: the env var mutated the global.
        assert_eq!(
            blosc2_get_nthreads(),
            4,
            "BLOSC_NTHREADS=4 must set the global"
        );

        // And the data still roundtrips regardless of thread count.
        let mut restored = vec![0u8; data.len()];
        let dsize = blosc1_decompress(&compressed[..csize], &mut restored).unwrap();
        assert_eq!(dsize, data.len());
        assert_eq!(restored, data);

        drop(_g);
        let _g = EnvGuard::set("BLOSC_NTHREADS", "65537");
        blosc2_set_nthreads(4);
        let csize = blosc1_compress(5, BLOSC_SHUFFLE, 4, &data, &mut compressed).unwrap();
        assert_eq!(
            blosc2_get_nthreads(),
            1,
            "C parses BLOSC_NTHREADS as long, then casts to int16_t"
        );
        drop(_g);
        let dsize = blosc1_decompress(&compressed[..csize], &mut restored).unwrap();
        assert_eq!(dsize, data.len());
        assert_eq!(restored, data);

        blosc2_set_nthreads(prev_nt);
    }

    #[test]
    fn test_blosc2_compress_applies_env_before_destsize_validation() {
        let _lock = BLOSC_ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let prev_nt = blosc2_get_nthreads();
        let _g = EnvGuard::set("BLOSC_NTHREADS", "4");
        blosc2_set_nthreads(1);

        let data = [0u8; 32];
        let mut dest = [0u8; BLOSC2_MAX_OVERHEAD];
        assert_eq!(
            blosc2_compress(5, BLOSC_SHUFFLE as i32, 1, &data, 32, &mut dest, 1),
            BLOSC2_ERROR_MAX_BUFSIZE_EXCEEDED
        );
        assert_eq!(
            blosc2_get_nthreads(),
            4,
            "C processes BLOSC_NTHREADS before rejecting a too-small destsize"
        );

        let _ = blosc2_set_nthreads(prev_nt);
    }

    #[test]
    fn test_blosc1_decompress_honors_blosc_nthreads_env() {
        let _lock = BLOSC_ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let prev_nt = blosc2_get_nthreads();
        let data: Vec<u8> = (0..8192u32).flat_map(|i| i.to_le_bytes()).collect();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 4,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let compressed = compress(&data, &cparams).unwrap();
        let _g = EnvGuard::set("BLOSC_NTHREADS", "4");
        let mut restored = vec![0u8; data.len()];
        let dsize = blosc1_decompress(&compressed, &mut restored).unwrap();
        assert_eq!(dsize, data.len());
        assert_eq!(restored, data);
        assert_eq!(blosc2_get_nthreads(), 4);
        let _ = blosc2_set_nthreads(prev_nt);
    }

    #[test]
    fn test_blosc2_decompress_c_invalid_env_nthreads_is_invalid_param() {
        let _lock = BLOSC_ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let prev_nt = blosc2_get_nthreads();
        let data: Vec<u8> = (0..256u32).flat_map(|i| i.to_le_bytes()).collect();
        let chunk = compress(
            &data,
            &CParams {
                compcode: BLOSC_LZ4,
                clevel: 5,
                typesize: 4,
                splitmode: BLOSC_NEVER_SPLIT,
                filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
                ..Default::default()
            },
        )
        .unwrap();
        let _g = EnvGuard::set("BLOSC_NTHREADS", "0");
        let mut restored = vec![0u8; data.len()];

        assert_eq!(
            blosc2_decompress(&chunk, chunk.len() as i32, &mut restored, data.len() as i32,),
            BLOSC2_ERROR_INVALID_PARAM
        );
        assert_eq!(
            blosc2_decompress(
                &chunk[..BLOSC_MIN_HEADER_LENGTH - 1],
                (BLOSC_MIN_HEADER_LENGTH - 1) as i32,
                &mut restored,
                data.len() as i32,
            ),
            BLOSC2_ERROR_INVALID_PARAM
        );
        assert_eq!(
            blosc2_decompress(
                &chunk[..BLOSC_MIN_HEADER_LENGTH - 1],
                BLOSC_MIN_HEADER_LENGTH as i32,
                &mut restored,
                data.len() as i32,
            ),
            BLOSC2_ERROR_INVALID_PARAM
        );

        let _ = blosc2_set_nthreads(prev_nt);
    }

    #[test]
    fn test_prefilter_postfilter_roundtrip() {
        let data: Vec<u8> = (0..4096u32).flat_map(|i| i.to_le_bytes()).collect();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 4,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_NOFILTER],
            prefilter: Some(xor_prefilter),
            ..Default::default()
        };
        let compressed = compress(&data, &cparams).unwrap();
        let dparams = DParams {
            nthreads: 1,
            postfilter: Some(xor_postfilter),
            typesize: 4,
            ..Default::default()
        };
        let decompressed = decompress_with_dparams(&compressed, &dparams).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_prefilter_delta_reference_uses_raw_first_block() {
        let data: Vec<u8> = (0..2048u32)
            .flat_map(|i| i.wrapping_mul(17).to_le_bytes())
            .collect();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 4,
            blocksize: 256,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, BLOSC_DELTA, BLOSC_SHUFFLE],
            prefilter: Some(xor_prefilter),
            ..Default::default()
        };
        let compressed = compress(&data, &cparams).unwrap();
        let dparams = DParams {
            nthreads: 1,
            postfilter: Some(xor_postfilter),
            typesize: 4,
            ..Default::default()
        };

        assert_eq!(
            decompress_with_dparams(&compressed, &dparams).unwrap(),
            data
        );
    }

    #[test]
    fn test_delta_reference_stays_raw_with_filters_before_delta() {
        let data: Vec<u8> = (0..2048u32)
            .flat_map(|i| i.wrapping_mul(31).rotate_left(7).to_le_bytes())
            .collect();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 4,
            blocksize: 256,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, BLOSC_SHUFFLE, BLOSC_DELTA],
            ..Default::default()
        };
        let compressed = compress(&data, &cparams).unwrap();

        assert_eq!(decompress(&compressed).unwrap(), data);
    }

    #[test]
    fn test_delta_pipeline_preserves_partial_typesize_tail() {
        let data: Vec<u8> = (0..4097usize)
            .map(|i| (i.wrapping_mul(31).wrapping_add(17)) as u8)
            .collect();
        let cparams = CParams {
            compcode: BLOSC_BLOSCLZ,
            clevel: 1,
            typesize: 4,
            blocksize: 0,
            splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
            filters: [0, 0, 0, BLOSC_NOFILTER, BLOSC_DELTA, BLOSC_SHUFFLE],
            nthreads: 1,
            ..Default::default()
        };

        let compressed = compress(&data, &cparams).unwrap();

        assert_eq!(decompress(&compressed).unwrap(), data);
    }

    #[test]
    fn test_all_zero_partial_typesize_chunk_uses_special_zero() {
        let data = vec![0u8; 4097];
        let cparams = CParams {
            compcode: BLOSC_BLOSCLZ,
            clevel: 1,
            typesize: 4,
            blocksize: 0,
            splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
            filters: [0, 0, 0, BLOSC_NOFILTER, BLOSC_DELTA, BLOSC_SHUFFLE],
            nthreads: 1,
            ..Default::default()
        };

        let compressed = compress(&data, &cparams).unwrap();
        let header = ChunkHeader::read(&compressed).unwrap();

        assert_eq!(header.special_type(), BLOSC2_SPECIAL_ZERO);
        assert_eq!(decompress(&compressed).unwrap(), data);
    }

    #[test]
    fn test_c_source_write_ordinal_matches_c_buffer_rotation() {
        let mut filters = [BLOSC_NOFILTER; BLOSC2_MAX_FILTERS];
        filters[0] = BLOSC_SHUFFLE;
        filters[1] = BLOSC_BITSHUFFLE;
        filters[2] = BLOSC_TRUNC_PREC;
        filters[3] = BLOSC_DELTA;
        let cparams = CParams {
            filters,
            ..Default::default()
        };
        assert_eq!(c_source_write_active_ordinal(&cparams), Some(3));
        assert!(c_forward_pipeline_writes_source(&cparams));

        let cparams = CParams {
            prefilter: Some(xor_prefilter),
            ..cparams
        };
        assert_eq!(c_source_write_active_ordinal(&cparams), None);
    }

    #[test]
    fn test_dictionary_training_samples_emulate_c_source_alias() {
        let mut filters = [BLOSC_NOFILTER; BLOSC2_MAX_FILTERS];
        filters[0] = BLOSC_SHUFFLE;
        filters[1] = BLOSC_BITSHUFFLE;
        filters[2] = BLOSC_DELTA;
        let data: Vec<u8> = (0..4096u32)
            .flat_map(|i| i.wrapping_mul(37).rotate_left(5).to_le_bytes())
            .collect();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 4,
            blocksize: 1024,
            splitmode: BLOSC_NEVER_SPLIT,
            filters,
            use_dict: true,
            nthreads: 1,
            ..Default::default()
        };
        let blocksize = compute_blocksize(&cparams, data.len() as i32) as usize;
        let nblocks = data.len().div_ceil(blocksize);

        assert!(c_forward_pipeline_writes_source(&cparams));
        assert!(
            filtered_blocks_for_dict(&data, &cparams, blocksize, nblocks, 4, false, true)
                .unwrap()
                .len()
                > 1
        );

        let safe_cparams = CParams {
            filters: [0, 0, 0, 0, BLOSC_DELTA, BLOSC_SHUFFLE],
            ..cparams
        };
        let compressed = compress(&data, &safe_cparams).unwrap();
        let header = ChunkHeader::read(&compressed).unwrap();
        assert!(header.use_dict());
        assert_eq!(decompress(&compressed).unwrap(), data);
    }

    #[test]
    fn test_replace_aligned_blocks_uses_raw_delta_reference_with_prefix_filters() {
        let mut data: Vec<u8> = (0..2048u32)
            .flat_map(|i| i.wrapping_mul(31).rotate_left(7).to_le_bytes())
            .collect();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 4,
            blocksize: 256,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, BLOSC_SHUFFLE, BLOSC_DELTA],
            ..Default::default()
        };
        let compressed = compress(&data, &cparams).unwrap();
        let replacement: Vec<u8> = (0..64u32)
            .flat_map(|i| i.wrapping_mul(97).rotate_left(3).to_le_bytes())
            .collect();
        data[256..512].copy_from_slice(&replacement);

        let updated = replace_aligned_blocks(&compressed, 256, &replacement, &cparams)
            .unwrap()
            .unwrap();

        assert_eq!(decompress(&updated).unwrap(), data);
    }

    #[test]
    fn test_prefilter_preserves_non_typesize_aligned_tail() {
        let data: Vec<u8> = (0..1003).map(|i| (i % 251) as u8).collect();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 4,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_NOFILTER],
            prefilter: Some(xor_prefilter),
            ..Default::default()
        };
        let compressed = compress(&data, &cparams).unwrap();
        let dparams = DParams {
            nthreads: 1,
            postfilter: Some(xor_postfilter),
            typesize: 4,
            ..Default::default()
        };
        assert_eq!(
            decompress_with_dparams(&compressed, &dparams).unwrap(),
            data
        );
    }

    #[test]
    fn test_prefilter_rejects_mismatched_output_typesize() {
        let data: Vec<u8> = (0..128).map(|i| i as u8).collect();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 4,
            prefilter: Some(xor_prefilter),
            prefilter_output_typesize: 8,
            ..Default::default()
        };
        assert_eq!(
            compress(&data, &cparams),
            Err("Unsupported prefilter output typesize")
        );
    }

    #[test]
    fn test_memcpy_prefilter_materializes_callback_output() {
        for (clevel, data) in [
            (0, (0..512).map(|i| (i % 251) as u8).collect::<Vec<_>>()),
            (5, b"tiny memcpy input".to_vec()),
        ] {
            let cparams = CParams {
                compcode: BLOSC_LZ4,
                clevel,
                typesize: 1,
                filters: [0; BLOSC2_MAX_FILTERS],
                prefilter: Some(xor_prefilter),
                ..Default::default()
            };
            let compressed = compress(&data, &cparams).unwrap();
            assert!(ChunkHeader::read(&compressed).unwrap().memcpyed());
            let expected: Vec<u8> = data.iter().map(|byte| byte ^ 0x5a).collect();
            assert_eq!(decompress(&compressed).unwrap(), expected);
        }
    }

    #[test]
    fn test_shuffle_meta_makes_typesize_one_non_noop() {
        let data: Vec<u8> = (0..16).collect();
        let mut shuffled = vec![0u8; data.len()];
        filters::shuffle(4, &data, &mut shuffled);

        let header_len = BLOSC_EXTENDED_HEADER_LENGTH;
        let block_start = header_len + 4;
        let cbytes = block_start + 4 + shuffled.len();
        let mut chunk = vec![0u8; cbytes];
        let mut filters = [0u8; BLOSC2_MAX_FILTERS];
        let mut filters_meta = [0u8; BLOSC2_MAX_FILTERS];
        filters[BLOSC2_MAX_FILTERS - 1] = BLOSC_SHUFFLE;
        filters_meta[BLOSC2_MAX_FILTERS - 1] = 4;
        let header = ChunkHeader {
            version: BLOSC2_VERSION_FORMAT_STABLE,
            versionlz: compcode_to_version(BLOSC_LZ4),
            flags: BLOSC_DOSHUFFLE
                | BLOSC_DOBITSHUFFLE
                | BLOSC_DONT_SPLIT
                | (compcode_to_compformat(BLOSC_LZ4) << 5),
            typesize: 1,
            nbytes: data.len() as i32,
            blocksize: data.len() as i32,
            cbytes: cbytes as i32,
            filters,
            filters_meta,
            ..Default::default()
        };
        header.try_write(&mut chunk[..header_len]).unwrap();
        chunk[header_len..header_len + 4].copy_from_slice(&(block_start as i32).to_le_bytes());
        chunk[block_start..block_start + 4].copy_from_slice(&(shuffled.len() as i32).to_le_bytes());
        chunk[block_start + 4..].copy_from_slice(&shuffled);

        assert_eq!(decompress(&chunk).unwrap(), data);
    }

    #[test]
    fn test_postfilter_typesize_comes_from_chunk_header() {
        let data: Vec<u8> = (0..2048u16).flat_map(u16::to_le_bytes).collect();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 2,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0; BLOSC2_MAX_FILTERS],
            ..Default::default()
        };
        let dparams = DParams {
            postfilter: Some(require_typesize_two_postfilter),
            ..Default::default()
        };

        let compressed = compress(&data, &cparams).unwrap();
        assert_eq!(
            decompress_with_dparams(&compressed, &dparams).unwrap(),
            data
        );
    }

    #[test]
    fn test_decompress_normalizes_oversized_regular_blocksize_like_c() {
        let data: Vec<u8> = (0..2048u32)
            .flat_map(|idx| (idx % 257).to_le_bytes())
            .collect();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 4,
            blocksize: data.len() as i32,
            splitmode: BLOSC_ALWAYS_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut chunk = compress(&data, &cparams).unwrap();
        let header = ChunkHeader::read(&chunk).unwrap();
        assert!(!header.memcpyed());
        assert!(!header.dont_split());
        assert_eq!(header.nblocks(), 1);

        let oversized_blocksize = (data.len() as i32) + cparams.typesize;
        chunk[BLOSC2_CHUNK_BLOCKSIZE..BLOSC2_CHUNK_BLOCKSIZE + 4]
            .copy_from_slice(&oversized_blocksize.to_le_bytes());
        assert_eq!(
            chunk_sizes(&chunk).unwrap(),
            (data.len(), chunk.len(), data.len())
        );
        assert_eq!(decompress(&chunk).unwrap(), data);

        let mut dest = vec![0u8; data.len()];
        assert_eq!(decompress_into(&chunk, &mut dest).unwrap(), data.len());
        assert_eq!(dest, data);
        assert_eq!(getitem(&chunk, 7, 23).unwrap(), data[28..120].to_vec());

        chunk[BLOSC2_CHUNK_BLOCKSIZE..BLOSC2_CHUNK_BLOCKSIZE + 4]
            .copy_from_slice(&i32::MAX.to_le_bytes());
        assert!(validate_chunk(&chunk).is_err());
        assert!(decompress(&chunk).is_err());
    }

    #[test]
    fn test_special_uninit_runs_postfilter() {
        POSTFILTER_CALLS.store(0, AtomicOrdering::SeqCst);
        let data = vec![0u8; 4096];
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            blocksize: 1024,
            filters: [0; BLOSC2_MAX_FILTERS],
            ..Default::default()
        };
        let mut compressed = compress(&data, &cparams).unwrap();
        compressed[BLOSC2_CHUNK_BLOSC2_FLAGS] = (compressed[BLOSC2_CHUNK_BLOSC2_FLAGS]
            & !(BLOSC2_SPECIAL_MASK << 4))
            | (BLOSC2_SPECIAL_UNINIT << 4);
        let dparams = DParams {
            postfilter: Some(fill_uninit_postfilter),
            ..Default::default()
        };

        assert_eq!(
            decompress_with_dparams(&compressed, &dparams).unwrap(),
            vec![0x7b; data.len()]
        );
        assert_eq!(POSTFILTER_CALLS.load(AtomicOrdering::SeqCst), 4);
    }

    #[test]
    fn test_parallel_prefilter_failure_returns_error() {
        let data: Vec<u8> = (0..8192u32).flat_map(|i| i.to_le_bytes()).collect();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 4,
            blocksize: 1024,
            nthreads: 2,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0; BLOSC2_MAX_FILTERS],
            prefilter: Some(failing_prefilter),
            ..Default::default()
        };

        assert_eq!(
            compress(&data, &cparams),
            Err("Execution of prefilter function failed")
        );
    }

    #[test]
    fn test_parallel_prefilter_receives_worker_tid() {
        PREFILTER_TID_MASK.store(0, AtomicOrdering::SeqCst);
        let data: Vec<u8> = (0..32768u32).flat_map(|i| i.to_le_bytes()).collect();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 4,
            blocksize: 1024,
            nthreads: 4,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0; BLOSC2_MAX_FILTERS],
            prefilter: Some(record_prefilter_tid),
            ..Default::default()
        };

        let compressed = compress(&data, &cparams).unwrap();
        assert_eq!(decompress(&compressed).unwrap(), data);
        assert!(PREFILTER_TID_MASK.load(AtomicOrdering::SeqCst).count_ones() > 1);
    }

    #[test]
    fn test_nofilter_incompressible_chunk_stays_regular_after_successful_compression() {
        let mut data = Vec::with_capacity(256 * 1024);
        let mut state = 0x1234_5678_u32;
        for i in 0..((256 * 1024) / 4) {
            state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            let noise = ((state >> 8) as f32 / 16_777_216.0 - 0.5) * 0.01;
            let x = (i as f32 * 0.01).sin() + (i as f32 * 0.001).sin() * 0.25 + noise;
            data.extend_from_slice(&x.to_le_bytes());
        }
        let cparams = CParams {
            compcode: BLOSC_BLOSCLZ,
            clevel: 5,
            typesize: 4,
            splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
            filters: [0; BLOSC2_MAX_FILTERS],
            ..Default::default()
        };

        let compressed = compress(&data, &cparams).unwrap();
        let header = ChunkHeader::read(&compressed).unwrap();
        assert!(
            !header.memcpyed(),
            "C-Blosc2 does not convert a successfully compressed regular chunk to memcpy solely because it is larger"
        );

        let restored = decompress(&compressed).unwrap();
        assert_eq!(restored, data);
    }

    #[test]
    fn test_late_compressible_blocks_prevent_memcpy_fallback() {
        let blocksize = 4096;
        let mut data = Vec::with_capacity(blocksize * 8);
        let mut state = 0x1234_5678_u32;
        for _ in 0..blocksize {
            state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            data.push((state >> 24) as u8);
        }
        data.extend(std::iter::repeat_n(0u8, blocksize * 7));
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            blocksize: blocksize as i32,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0; BLOSC2_MAX_FILTERS],
            ..Default::default()
        };

        let compressed = compress(&data, &cparams).unwrap();
        let header = ChunkHeader::read(&compressed).unwrap();
        assert!(!header.memcpyed());
        assert_eq!(decompress(&compressed).unwrap(), data);
    }

    #[test]
    fn test_shuffle_literal_streams_stay_regular_like_c() {
        let mut data = Vec::with_capacity(64 * 1024);
        let mut state = 0x8765_4321_u32;
        for _ in 0..((64 * 1024) / 4) {
            state = state.wrapping_mul(1_103_515_245).wrapping_add(12_345);
            data.extend_from_slice(&((state >> 16) as u8 as u32).to_le_bytes());
        }
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 4,
            blocksize: 4096,
            splitmode: BLOSC_ALWAYS_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };

        let compressed = compress(&data, &cparams).unwrap();
        let header = ChunkHeader::read(&compressed).unwrap();
        assert!(
            !header.memcpyed(),
            "C-Blosc2 stores literal streams inside regular shuffled blocks instead of converting the whole chunk to memcpy"
        );
        assert!(
            compressed.len() < BLOSC_EXTENDED_HEADER_LENGTH + data.len(),
            "shuffle should still let the zero byte streams compress"
        );
        assert_eq!(
            compressed[BLOSC2_CHUNK_FLAGS] & 0xe0,
            compcode_to_compformat(BLOSC_LZ4) << 5
        );
        assert_eq!(header.compcode(), BLOSC_LZ4);
        assert_eq!(chunk_compressor_library(&compressed), Some("LZ4"));
        assert_eq!(decompress(&compressed).unwrap(), data);
    }

    #[test]
    fn test_blosc2_setters_roundtrip_previous_values() {
        let _lock = BLOSC_ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());

        // blosc2_set_nthreads mirrors C's global setter, including its
        // historical zero/negative return behavior.
        let n0 = blosc2_set_nthreads(3);
        let n1 = blosc2_set_nthreads(7);
        assert_eq!(n1, 3, "second set must see first set's value as previous");
        assert_eq!(blosc_get_nthreads(), 7);
        assert_eq!(blosc_set_nthreads(5), 7);
        assert_eq!(blosc2_get_nthreads(), 5);
        blosc2_set_nthreads(n0); // restore

        // blosc1_set_blocksize is a void setter in C.
        let b0 = blosc1_get_blocksize();
        blosc1_set_blocksize(16384);
        assert_eq!(blosc1_get_blocksize(), 16384);
        assert_eq!(blosc_get_blocksize(), 16384);
        blosc1_set_blocksize(8192);
        assert_eq!(blosc1_get_blocksize(), 8192);
        blosc_set_blocksize(4096);
        assert_eq!(blosc1_get_blocksize(), 4096);
        blosc1_set_blocksize(b0);

        // blosc1_set_splitmode is a void setter in C.
        let s0 = blosc1_get_splitmode();
        blosc1_set_splitmode(BLOSC_ALWAYS_SPLIT);
        assert_eq!(blosc1_get_splitmode(), BLOSC_ALWAYS_SPLIT);
        blosc_set_splitmode(BLOSC_AUTO_SPLIT);
        assert_eq!(blosc1_get_splitmode(), BLOSC_AUTO_SPLIT);
        blosc1_set_splitmode(BLOSC_NEVER_SPLIT);
        assert_eq!(blosc1_get_splitmode(), BLOSC_NEVER_SPLIT);
        blosc1_set_splitmode(s0);

        let compressor0 = blosc1_get_compressor_code();
        assert_eq!(blosc_set_compressor("lz4"), BLOSC_LZ4 as i32);
        assert_eq!(blosc_get_compressor(), Some("lz4"));
        assert_eq!(blosc_compname_to_compcode("zstd"), Some(BLOSC_ZSTD));
        assert_eq!(blosc_compcode_to_compname(BLOSC_ZLIB), Some("zlib"));
        assert!(blosc_list_compressors().contains("blosclz"));
        assert!(!blosc_get_version_string().is_empty());
        assert_eq!(blosc_get_complib_info("lz4").unwrap().0, BLOSC_LZ4_FORMAT);
        blosc1_set_compressor_code(compressor0);

        let sample = b"legacy alias roundtrip".repeat(16);
        let mut compressed = vec![0; sample.len() + BLOSC2_MAX_OVERHEAD + 128];
        let cbytes = blosc_compress(5, BLOSC_SHUFFLE, 1, &sample, &mut compressed).unwrap();
        let mut restored = vec![0; sample.len()];
        assert_eq!(
            blosc_decompress(&compressed[..cbytes], &mut restored).unwrap(),
            sample.len()
        );
        assert_eq!(restored, sample);

        // C stores zero from a valid state and returns the previous value.
        let current = blosc2_get_nthreads();
        assert!(current > 0);
        assert_eq!(blosc2_set_nthreads(1), current);
        assert_eq!(blosc2_set_nthreads(0), 1);
        assert_eq!(blosc2_get_nthreads(), 0);
        // Once the simulated context is already invalid, C returns
        // check_nthreads' invalid-parameter result while still storing the
        // attempted process-wide value.
        assert_eq!(blosc2_set_nthreads(-1), BLOSC2_ERROR_INVALID_PARAM as i16);
        assert_eq!(blosc2_get_nthreads(), -1);
        let _ = blosc2_set_nthreads(n0);

        let default_cparams = blosc2_get_blosc2_cparams_defaults();
        assert_eq!(default_cparams.compcode, CParams::default().compcode);
        assert_eq!(default_cparams.clevel, CParams::default().clevel);
        assert_eq!(default_cparams.typesize, CParams::default().typesize);
        let default_dparams = blosc2_get_blosc2_dparams_defaults();
        assert_eq!(default_dparams.nthreads, DParams::default().nthreads);
        assert_eq!(default_dparams.typesize, DParams::default().typesize);
    }

    #[test]
    fn test_thread_pool_cache_reuses_same_pool_for_same_thread_count() {
        let pool_a = thread_pool_for(4).expect("expected cached thread pool");
        let pool_b = thread_pool_for(4).expect("expected cached thread pool");
        assert!(Arc::ptr_eq(&pool_a, &pool_b));

        let pool_c = thread_pool_for(2).expect("expected cached thread pool");
        assert!(!Arc::ptr_eq(&pool_a, &pool_c));
    }

    #[test]
    fn test_free_cached_resources_clears_thread_pool_cache() {
        let pool_a = thread_pool_for(3).expect("expected cached thread pool");
        free_cached_resources();
        let pool_b = thread_pool_for(3).expect("expected fresh thread pool");
        assert!(!Arc::ptr_eq(&pool_a, &pool_b));
    }

    #[test]
    fn test_effective_nthreads_caps_requested_workers_to_jobs() {
        assert_eq!(effective_nthreads(0, 8), 1);
        assert_eq!(effective_nthreads(8, 0), 1);
        assert_eq!(effective_nthreads(8, 1), 1);
        assert!(effective_nthreads(64, 2) <= 2);
        assert!(effective_nthreads(64, 128) >= 1);
        assert!(memcpy_parallel_threads(4 * 1024 * 1024, 64) <= 1);
        assert!(memcpy_parallel_threads(64 * 1024 * 1024, 64) >= 1);
    }

    #[test]
    fn test_memcpy_parallel_threshold() {
        assert!(!should_parallelize_memcpyed(4 * 1024 * 1024, 4));
        assert!(!should_parallelize_memcpyed(8 * 1024 * 1024 - 1, 4));
        assert!(!should_parallelize_memcpyed(64 * 1024 * 1024, 1));

        let threads = memcpy_parallel_threads(8 * 1024 * 1024, 8);
        assert!(threads <= 4);
        assert_eq!(
            should_parallelize_memcpyed(8 * 1024 * 1024, 8),
            threads > 1
                && (8usize * 1024 * 1024).div_ceil(threads as usize)
                    >= MEMCPY_PARALLEL_MIN_BYTES_PER_THREAD
        );
    }

    // Fuzz-style: mutate every byte of the first 32 (header) and ensure public
    // decompress/validate/getitem entry points return Err instead of panicking.
    #[test]
    fn test_header_mutation_never_panics() {
        let data: Vec<u8> = (0..2048u32).flat_map(|i| i.to_le_bytes()).collect();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 4,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let good = compress(&data, &cparams).unwrap();
        let header_bytes = 32.min(good.len());

        for i in 0..header_bytes {
            for v in [0u8, 0xff, 0x7f, 0x80, 0xAA, 0x55] {
                let mut bad = good.clone();
                bad[i] = v;
                // None of these must panic — they must return a Result.
                let _ = std::panic::catch_unwind(|| decompress(&bad))
                    .unwrap_or_else(|_| panic!("decompress panicked at byte={i} val={v:#x}"));
                let _ = std::panic::catch_unwind(|| validate_chunk(&bad))
                    .unwrap_or_else(|_| panic!("validate_chunk panicked at byte={i} val={v:#x}"));
                let _ = std::panic::catch_unwind(|| chunk_sizes(&bad))
                    .unwrap_or_else(|_| panic!("chunk_sizes panicked at byte={i} val={v:#x}"));
                let _ = std::panic::catch_unwind(|| chunk_metainfo(&bad))
                    .unwrap_or_else(|_| panic!("chunk_metainfo panicked at byte={i} val={v:#x}"));
                let _ = std::panic::catch_unwind(|| getitem(&bad, 10, 5))
                    .unwrap_or_else(|_| panic!("getitem panicked at byte={i} val={v:#x}"));
            }
        }
    }

    #[test]
    fn test_body_mutation_never_panics() {
        let data: Vec<u8> = (0..2048u32).flat_map(|i| i.to_le_bytes()).collect();
        // Mix of codecs and filter combinations — exercises different
        // decompression paths (splits, shuffle, bitshuffle, memcpy fallback).
        let cparam_matrix = [
            (BLOSC_LZ4, BLOSC_SHUFFLE, BLOSC_NEVER_SPLIT),
            (BLOSC_LZ4, BLOSC_BITSHUFFLE, BLOSC_ALWAYS_SPLIT),
            (BLOSC_BLOSCLZ, BLOSC_SHUFFLE, BLOSC_FORWARD_COMPAT_SPLIT),
            (BLOSC_ZSTD, BLOSC_NOFILTER, BLOSC_NEVER_SPLIT),
            (BLOSC_ZLIB, BLOSC_BITSHUFFLE, BLOSC_NEVER_SPLIT),
        ];
        // Simple deterministic PRNG — xorshift — so the test is reproducible
        // without pulling in a dependency.
        let mut state: u64 = 0xdead_beef_cafe_babe;
        let mut rand_u32 = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as u32
        };

        for (codec, filter, split) in cparam_matrix {
            let cparams = CParams {
                compcode: codec,
                clevel: 5,
                typesize: 4,
                splitmode: split,
                filters: [0, 0, 0, 0, 0, filter],
                ..Default::default()
            };
            let good = match compress(&data, &cparams) {
                Ok(c) => c,
                Err(_) => continue, // skip if compression not available
            };

            for _ in 0..200 {
                let mut bad = good.clone();
                // Flip 1..=4 random bytes anywhere in the chunk.
                let n = (rand_u32() % 4 + 1) as usize;
                for _ in 0..n {
                    let idx = rand_u32() as usize % bad.len();
                    bad[idx] ^= (rand_u32() & 0xFF) as u8;
                }
                // None of these must panic.
                let _ = std::panic::catch_unwind(|| decompress(&bad)).unwrap_or_else(|_| {
                    panic!("decompress panicked for codec={codec} filter={filter}")
                });
                let _ = std::panic::catch_unwind(|| validate_chunk(&bad)).unwrap_or_else(|_| {
                    panic!("validate_chunk panicked for codec={codec} filter={filter}")
                });
                let _ = std::panic::catch_unwind(|| getitem(&bad, 0, 10)).unwrap_or_else(|_| {
                    panic!("getitem panicked for codec={codec} filter={filter}")
                });
            }
        }
    }

    #[test]
    fn test_truncation_never_panics() {
        let data: Vec<u8> = (0..2048u32).flat_map(|i| i.to_le_bytes()).collect();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 4,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let good = compress(&data, &cparams).unwrap();

        let mut cuts: Vec<usize> = (0..=good.len()).collect();
        cuts.extend_from_slice(&[0, 1, 3, 15, 16, 17, 31, 32, 33]);
        cuts.sort();
        cuts.dedup();

        for &take in &cuts {
            if take > good.len() {
                continue;
            }
            let bad = &good[..take];
            let _ = std::panic::catch_unwind(|| decompress(bad))
                .unwrap_or_else(|_| panic!("decompress panicked at truncation={take}"));
            let _ = std::panic::catch_unwind(|| validate_chunk(bad))
                .unwrap_or_else(|_| panic!("validate_chunk panicked at truncation={take}"));
            let _ = std::panic::catch_unwind(|| chunk_sizes(bad))
                .unwrap_or_else(|_| panic!("chunk_sizes panicked at truncation={take}"));
            let _ = std::panic::catch_unwind(|| getitem(bad, 0, 1))
                .unwrap_or_else(|_| panic!("getitem panicked at truncation={take}"));
        }
    }

    #[test]
    fn test_compress_all_codecs() {
        let data: Vec<u8> = b"Test data for compression with various codecs and filters! "
            .iter()
            .cycle()
            .take(50000)
            .copied()
            .collect();

        let codecs = vec![
            BLOSC_BLOSCLZ,
            BLOSC_LZ4,
            BLOSC_LZ4HC,
            BLOSC_ZLIB,
            BLOSC_ZSTD,
        ];

        for compcode in codecs {
            let cparams = CParams {
                compcode,
                clevel: 5,
                typesize: 1,
                splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
                filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
                ..Default::default()
            };

            let compressed = compress(&data, &cparams).unwrap();
            let decompressed = decompress(&compressed).unwrap();
            assert_eq!(
                data, decompressed,
                "Roundtrip failed for compcode={compcode}"
            );
        }
    }

    #[test]
    fn test_disposable_prefilter_failure_discards_output() {
        let data: Vec<u8> = (0..4096u32).flat_map(|i| i.to_le_bytes()).collect();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 4,
            blocksize: 1024,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            prefilter: Some(disposable_failing_prefilter),
            prefilter_output_is_disposable: true,
            ..Default::default()
        };

        let compressed = compress(&data, &cparams).unwrap();
        assert_eq!(decompress(&compressed).unwrap(), vec![0; data.len()]);
    }

    #[test]
    fn test_disposable_prefilter_success_discards_output_like_c() {
        let data: Vec<u8> = (0..4096u32).flat_map(|i| i.to_le_bytes()).collect();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 4,
            blocksize: 1024,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            prefilter: Some(disposable_success_prefilter),
            prefilter_output_is_disposable: true,
            ..Default::default()
        };

        let compressed = compress(&data, &cparams).unwrap();
        assert_eq!(decompress(&compressed).unwrap(), vec![0; data.len()]);
    }

    #[test]
    fn test_compress_empty() {
        let cparams = CParams {
            compcode: BLOSC_ZSTD,
            ..Default::default()
        };
        let compressed = compress(&[], &cparams).unwrap();
        let header = ChunkHeader::read(&compressed).unwrap();
        assert!(header.memcpyed());
        assert_eq!(compressed[BLOSC2_CHUNK_FLAGS] & 0xe0, 0);
        assert_eq!(header.compcode(), BLOSC_BLOSCLZ);
        assert_eq!(chunk_compressor_library(&compressed), Some("BloscLZ"));
        let decompressed = decompress(&compressed).unwrap();
        assert!(decompressed.is_empty());
    }

    #[test]
    fn test_zero_byte_chunks_still_validate_special_flags() {
        let empty = compress(&[], &CParams::default()).unwrap();
        assert_eq!(decompress(&empty).unwrap(), Vec::<u8>::new());

        let mut zero_special_value = empty.clone();
        zero_special_value[BLOSC2_CHUNK_BLOSC2_FLAGS] = BLOSC2_SPECIAL_VALUE << 4;
        assert_eq!(
            validate_chunk(&zero_special_value),
            Err("Invalid special value typesize")
        );
        assert_eq!(
            decompress(&zero_special_value),
            Err("Invalid special value typesize")
        );

        let mut zero_unknown_special = empty;
        zero_unknown_special[BLOSC2_CHUNK_BLOSC2_FLAGS] = 0xf0;
        assert_eq!(
            validate_chunk(&zero_unknown_special),
            Err("Unknown special value type")
        );
        assert_eq!(
            decompress(&zero_unknown_special),
            Err("Unknown special value type")
        );
    }

    #[test]
    fn test_invalid_compression_params_return_errors() {
        let data = [1u8, 2, 3, 4];

        for typesize in [0, -1, BLOSC2_MAXTYPESIZE as i32 + 1] {
            let cparams = CParams {
                typesize,
                ..Default::default()
            };
            assert!(compress(&data, &cparams).is_err());
        }

        let bad_cases = [
            CParams {
                clevel: 10,
                ..Default::default()
            },
            CParams {
                blocksize: -1,
                ..Default::default()
            },
            CParams {
                compcode: 99,
                ..Default::default()
            },
            CParams {
                filters: [0, 0, 0, 0, 0, 99],
                ..Default::default()
            },
            CParams {
                nthreads: 0,
                ..Default::default()
            },
        ];

        for cparams in bad_cases {
            assert!(compress(&data, &cparams).is_err());
        }
        let forward_compatible_split = CParams {
            splitmode: 99,
            ..Default::default()
        };
        assert!(compress(&data, &forward_compatible_split).is_ok());
    }

    #[test]
    fn test_large_typesize_normalizes_to_byte_stream() {
        let data: Vec<u8> = (0..4096).map(|idx| (idx % 251) as u8).collect();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: BLOSC_MAX_TYPESIZE as i32 + 1,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };

        let compressed = compress(&data, &cparams).unwrap();
        let header = ChunkHeader::read(&compressed).unwrap();
        assert_eq!(header.typesize, 1);
        assert_eq!(decompress(&compressed).unwrap(), data);
    }

    #[test]
    fn test_malformed_headers_return_errors() {
        let data: Vec<u8> = (0..4096u32).flat_map(|i| i.to_le_bytes()).collect();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 4,
            blocksize: 256,
            filters: [0; BLOSC2_MAX_FILTERS],
            ..Default::default()
        };
        let chunk = compress(&data, &cparams).unwrap();

        let header = ChunkHeader::read(&chunk).unwrap();
        assert!(header.nblocks() > 1);
        let header_len = header.header_len();
        let first_bstart = chunk[header_len..header_len + 4].to_vec();
        let mut duplicate_bstart = chunk.clone();
        duplicate_bstart[header_len + 4..header_len + 8].copy_from_slice(&first_bstart);
        assert!(validate_chunk(&duplicate_bstart).is_err());
        assert!(decompress(&duplicate_bstart).is_err());
        assert!(replace_aligned_blocks(&duplicate_bstart, 0, &data[..4], &cparams).is_err());

        let mut negative_nbytes = chunk.clone();
        negative_nbytes[4..8].copy_from_slice(&(-1i32).to_le_bytes());
        assert!(decompress(&negative_nbytes).is_err());

        let mut zero_blocksize = chunk.clone();
        zero_blocksize[8..12].copy_from_slice(&0i32.to_le_bytes());
        assert!(decompress(&zero_blocksize).is_err());

        let mut unsupported_filter = chunk.clone();
        unsupported_filter[BLOSC2_CHUNK_FILTER_CODES + 5] = 99;
        assert!(decompress(&unsupported_filter).is_err());

        let mut reserved_compformat = chunk.clone();
        reserved_compformat[BLOSC2_CHUNK_FLAGS] =
            (reserved_compformat[BLOSC2_CHUNK_FLAGS] & !0xe0) | (2 << 5);
        assert!(validate_chunk(&reserved_compformat).is_err());
        assert!(decompress(&reserved_compformat).is_err());

        let memcpy = compress(
            &data,
            &CParams {
                compcode: BLOSC_ZSTD,
                clevel: 0,
                typesize: 4,
                filters: [0; BLOSC2_MAX_FILTERS],
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(chunk_compressor_library(&memcpy), Some("BloscLZ"));
        let mut memcpy_bad_filter = memcpy.clone();
        memcpy_bad_filter[BLOSC2_CHUNK_FILTER_CODES + 5] = 99;
        assert!(validate_chunk(&memcpy_bad_filter).is_ok());
        assert_eq!(decompress(&memcpy_bad_filter).unwrap(), data);

        let special_zero = compress(
            &[0u8; 4096],
            &CParams {
                compcode: BLOSC_LZ4,
                clevel: 5,
                typesize: 1,
                filters: [0; BLOSC2_MAX_FILTERS],
                ..Default::default()
            },
        )
        .unwrap();
        for compformat in [5, BLOSC_UDCODEC_FORMAT] {
            let mut mutated_memcpy = memcpy.clone();
            mutated_memcpy[BLOSC2_CHUNK_FLAGS] =
                (mutated_memcpy[BLOSC2_CHUNK_FLAGS] & !0xe0) | (compformat << 5);
            assert!(validate_chunk(&mutated_memcpy).is_ok());
            assert_eq!(decompress(&mutated_memcpy).unwrap(), data);

            let mut mutated_special = special_zero.clone();
            mutated_special[BLOSC2_CHUNK_FLAGS] =
                (mutated_special[BLOSC2_CHUNK_FLAGS] & !0xe0) | (compformat << 5);
            assert!(validate_chunk(&mutated_special).is_ok());
            assert_eq!(decompress(&mutated_special).unwrap(), vec![0u8; 4096]);
        }

        let mut bad_nan_special = chunk.clone();
        bad_nan_special[BLOSC2_CHUNK_TYPESIZE] = 2;
        bad_nan_special[BLOSC2_CHUNK_BLOSC2_FLAGS] = BLOSC2_SPECIAL_NAN << 4;
        assert!(decompress(&bad_nan_special).is_err());

        let mut future_compatible = chunk.clone();
        future_compatible[BLOSC2_CHUNK_VERSION] = BLOSC2_VERSION_FORMAT + 1;
        future_compatible[BLOSC2_CHUNK_BLOSC2_FLAGS2] = 0;
        assert_eq!(decompress(&future_compatible).unwrap(), data);

        let blocks: [&[u8]; 2] = [b"future-vl-alpha", b"future-vl-beta"];
        let mut future_vl_compatible = vlcompress(&blocks, &CParams::default()).unwrap();
        future_vl_compatible[BLOSC2_CHUNK_VERSION] = BLOSC2_VERSION_FORMAT + 1;
        assert_eq!(
            vldecompress(&future_vl_compatible).unwrap(),
            blocks
                .iter()
                .map(|block| block.to_vec())
                .collect::<Vec<_>>()
        );

        let mut future_unknown_flags2 = chunk.clone();
        future_unknown_flags2[BLOSC2_CHUNK_VERSION] = BLOSC2_VERSION_FORMAT + 1;
        future_unknown_flags2[BLOSC2_CHUNK_BLOSC2_FLAGS2] = 0x80;
        assert!(decompress(&future_unknown_flags2).is_err());

        for (flag_offset, flag) in [
            (BLOSC2_CHUNK_BLOSC2_FLAGS, BLOSC2_USEDICT),
            (BLOSC2_CHUNK_BLOSC2_FLAGS, BLOSC2_INSTR_CODEC),
            (BLOSC2_CHUNK_BLOSC2_FLAGS, BLOSC2_LAZY_CHUNK),
            (BLOSC2_CHUNK_BLOSC2_FLAGS2, BLOSC2_VL_BLOCKS),
        ] {
            let mut unsupported = chunk.clone();
            unsupported[flag_offset] |= flag;
            assert!(decompress(&unsupported).is_err());
        }

        let mut oversized_cbytes = chunk.clone();
        oversized_cbytes[12..16].copy_from_slice(&((chunk.len() + 1) as i32).to_le_bytes());
        assert!(decompress(&oversized_cbytes).is_err());

        let mut understated_cbytes = chunk.clone();
        understated_cbytes[12..16].copy_from_slice(&((chunk.len() - 1) as i32).to_le_bytes());
        assert!(decompress(&understated_cbytes).is_err());

        let mut negative_bstart = chunk.clone();
        negative_bstart[BLOSC_EXTENDED_HEADER_LENGTH..BLOSC_EXTENDED_HEADER_LENGTH + 4]
            .copy_from_slice(&(-1i32).to_le_bytes());
        assert!(decompress(&negative_bstart).is_err());

        let data_two_blocks: Vec<u8> = (0..8192)
            .map(|i: usize| (i.wrapping_mul(31).wrapping_add(7) % 251) as u8)
            .collect();
        let cparams_two_blocks = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            blocksize: 4096,
            filters: [0, 0, 0, 0, 0, BLOSC_NOFILTER],
            ..Default::default()
        };
        let mut bad_block_boundary = compress(&data_two_blocks, &cparams_two_blocks).unwrap();
        assert!(!ChunkHeader::read(&bad_block_boundary).unwrap().memcpyed());
        let second_bstart_pos = BLOSC_EXTENDED_HEADER_LENGTH + 4;
        let second_bstart = i32::from_le_bytes(
            bad_block_boundary[second_bstart_pos..second_bstart_pos + 4]
                .try_into()
                .unwrap(),
        );
        bad_block_boundary[second_bstart_pos..second_bstart_pos + 4]
            .copy_from_slice(&(second_bstart + 1).to_le_bytes());
        assert!(decompress(&bad_block_boundary).is_err());

        let payload = [1u8, 2, 3, 4];
        let mut bad_memcpyed = vec![0u8; BLOSC_EXTENDED_HEADER_LENGTH + payload.len()];
        let header = ChunkHeader {
            version: BLOSC2_VERSION_FORMAT_STABLE,
            versionlz: 1,
            flags: BLOSC_DOSHUFFLE | BLOSC_MEMCPYED | BLOSC_DOBITSHUFFLE,
            typesize: 1,
            nbytes: payload.len() as i32,
            blocksize: payload.len() as i32,
            cbytes: (BLOSC_EXTENDED_HEADER_LENGTH + payload.len() - 1) as i32,
            filters: [0, 0, 0, 0, 0, BLOSC_NOFILTER],
            ..Default::default()
        };
        header.write(&mut bad_memcpyed[..BLOSC_EXTENDED_HEADER_LENGTH]);
        bad_memcpyed[BLOSC_EXTENDED_HEADER_LENGTH..].copy_from_slice(&payload);
        assert!(decompress(&bad_memcpyed).is_err());

        assert!(decompress_with_threads(&chunk, 0).is_err());
    }

    #[test]
    fn test_special_value_repeats_payload() {
        let repeated = 0xA1B2C3D4u32.to_le_bytes();
        let typesize = repeated.len();
        let nitems = 10usize;
        let mut chunk = vec![0u8; BLOSC_EXTENDED_HEADER_LENGTH + typesize];
        let header = ChunkHeader {
            version: BLOSC2_VERSION_FORMAT_STABLE,
            versionlz: 1,
            flags: BLOSC_DOSHUFFLE | BLOSC_DOBITSHUFFLE,
            typesize: typesize as u8,
            nbytes: (nitems * typesize) as i32,
            blocksize: (nitems * typesize) as i32,
            cbytes: chunk.len() as i32,
            filters: [0, 0, 0, 0, 0, BLOSC_NOFILTER],
            blosc2_flags: BLOSC2_SPECIAL_VALUE << 4,
            ..Default::default()
        };
        header.write(&mut chunk[..BLOSC_EXTENDED_HEADER_LENGTH]);
        chunk[BLOSC_EXTENDED_HEADER_LENGTH..].copy_from_slice(&repeated);

        let decompressed = decompress(&chunk).unwrap();
        for item in decompressed.chunks_exact(typesize) {
            assert_eq!(item, repeated);
        }

        let mut truncated = chunk.clone();
        truncated[12..16].copy_from_slice(&(BLOSC_EXTENDED_HEADER_LENGTH as i32).to_le_bytes());
        assert!(decompress(&truncated).is_err());
    }

    #[test]
    fn test_public_special_chunk_constructors() {
        let _lock = BLOSC_ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let _typesize = EnvGuard::remove("BLOSC_TYPESIZE");
        let _blocksize = EnvGuard::remove("BLOSC_BLOCKSIZE");

        let zero = blosc2_chunk_zeros(16, 4).unwrap();
        let zero_header = ChunkHeader::read(&zero).unwrap();
        assert_eq!(zero_header.special_type(), BLOSC2_SPECIAL_ZERO);
        assert_eq!(zero_header.cbytes as usize, BLOSC_EXTENDED_HEADER_LENGTH);
        assert_eq!(decompress(&zero).unwrap(), vec![0u8; 16]);

        let nans = blosc2_chunk_nans(16, 8).unwrap();
        let nan_header = ChunkHeader::read(&nans).unwrap();
        assert_eq!(nan_header.special_type(), BLOSC2_SPECIAL_NAN);
        for item in decompress(&nans).unwrap().chunks_exact(8) {
            assert!(f64::from_le_bytes(item.try_into().unwrap()).is_nan());
        }

        let repeated = blosc2_chunk_repeatval(12, &[1, 2, 3]).unwrap();
        let repeated_header = ChunkHeader::read(&repeated).unwrap();
        assert_eq!(repeated_header.special_type(), BLOSC2_SPECIAL_VALUE);
        assert_eq!(
            repeated_header.cbytes as usize,
            BLOSC_EXTENDED_HEADER_LENGTH + 3
        );
        assert_eq!(decompress(&repeated).unwrap(), [1, 2, 3].repeat(4));

        let uninit = blosc2_chunk_uninit(8, 2).unwrap();
        let uninit_header = ChunkHeader::read(&uninit).unwrap();
        assert_eq!(uninit_header.special_type(), BLOSC2_SPECIAL_UNINIT);
        assert_eq!(decompress(&uninit).unwrap(), vec![0u8; 8]);

        let mut dest = vec![0xAA; 8];
        assert_eq!(decompress_into(&uninit, &mut dest).unwrap(), 8);
        assert_eq!(dest, vec![0xAA; 8]);

        let mut dest = vec![0xAA; 8];
        assert_eq!(
            decompress_into_with_threads(&uninit, &mut dest, 2).unwrap(),
            8
        );
        assert_eq!(dest, vec![0xAA; 8]);

        let blocked_uninit = blosc2_chunk_uninit_with_cparams(
            16,
            &CParams {
                typesize: 1,
                blocksize: 4,
                ..Default::default()
            },
        )
        .unwrap();
        let mut dest = vec![0xAA; 16];
        let dparams = DParams {
            block_maskout: Some(vec![false, true, false, true]),
            ..Default::default()
        };
        assert_eq!(
            decompress_into_with_dparams(&blocked_uninit, &mut dest, &dparams).unwrap(),
            16
        );
        assert_eq!(&dest[..4], &[0xAA; 4]);
        assert_eq!(&dest[4..8], &[0xAA; 4]);
        assert_eq!(&dest[8..12], &[0xAA; 4]);
        assert_eq!(&dest[12..], &[0xAA; 4]);

        assert!(blosc2_chunk_zeros(10, 4).is_err());
        let nans_u16 = blosc2_chunk_nans(16, 2).unwrap();
        let nans_u16_header = ChunkHeader::read(&nans_u16).unwrap();
        assert_eq!(nans_u16_header.special_type(), BLOSC2_SPECIAL_NAN);
        assert_eq!(nans_u16_header.typesize, 2);
        assert_eq!(
            decompress(&nans_u16),
            Err("NaN special only valid for 4 or 8 byte types")
        );
        assert!(blosc2_chunk_repeatval(2, &[1, 2, 3]).is_err());
    }

    #[test]
    fn test_special_chunk_c_adapters() {
        let _lock = BLOSC_ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let _clevel = EnvGuard::remove("BLOSC_CLEVEL");
        let _shuffle = EnvGuard::remove("BLOSC_SHUFFLE");
        let _typesize = EnvGuard::remove("BLOSC_TYPESIZE");
        let _blocksize = EnvGuard::remove("BLOSC_BLOCKSIZE");
        let _nthreads = EnvGuard::remove("BLOSC_NTHREADS");
        let _splitmode = EnvGuard::remove("BLOSC_SPLITMODE");
        let _compressor = EnvGuard::remove("BLOSC_COMPRESSOR");
        let cparams = CParams {
            typesize: 4,
            ..Default::default()
        };
        let mut dest = vec![0u8; BLOSC_EXTENDED_HEADER_LENGTH + 4];

        let zero_len = blosc2_chunk_zeros_c(cparams.clone(), 16, &mut dest, 32);
        assert_eq!(zero_len, BLOSC_EXTENDED_HEADER_LENGTH as i32);
        assert_eq!(
            ChunkHeader::read(&dest[..zero_len as usize])
                .unwrap()
                .special_type(),
            BLOSC2_SPECIAL_ZERO
        );
        assert_eq!(
            decompress(&dest[..zero_len as usize]).unwrap(),
            vec![0u8; 16]
        );

        let uninit_len = blosc2_chunk_uninit_c(cparams.clone(), 16, &mut dest, 32);
        assert_eq!(uninit_len, BLOSC_EXTENDED_HEADER_LENGTH as i32);
        assert_eq!(
            ChunkHeader::read(&dest[..uninit_len as usize])
                .unwrap()
                .special_type(),
            BLOSC2_SPECIAL_UNINIT
        );

        let nan_len = blosc2_chunk_nans_c(cparams.clone(), 16, &mut dest, 32);
        assert_eq!(nan_len, BLOSC_EXTENDED_HEADER_LENGTH as i32);
        assert_eq!(
            ChunkHeader::read(&dest[..nan_len as usize])
                .unwrap()
                .special_type(),
            BLOSC2_SPECIAL_NAN
        );

        let repeated = [1, 2, 3, 4];
        let repeat_len = blosc2_chunk_repeatval_c(cparams.clone(), 16, &mut dest, 36, &repeated);
        assert_eq!(repeat_len, (BLOSC_EXTENDED_HEADER_LENGTH + 4) as i32);
        assert_eq!(
            decompress(&dest[..repeat_len as usize]).unwrap(),
            repeated.repeat(4)
        );
        let zero_repeat_len =
            blosc2_chunk_repeatval_c(cparams.clone(), 0, &mut dest, 36, &repeated);
        assert_eq!(
            zero_repeat_len,
            (BLOSC_EXTENDED_HEADER_LENGTH + repeated.len()) as i32
        );
        assert_eq!(
            &dest[BLOSC_EXTENDED_HEADER_LENGTH..zero_repeat_len as usize],
            repeated
        );
        assert!(matches!(
            ChunkHeader::read(&dest[..zero_repeat_len as usize]),
            Err("Invalid special value typesize")
        ));
        assert_eq!(
            decompress(&dest[..zero_repeat_len as usize]),
            Err("Invalid special value typesize")
        );

        let short_len = (BLOSC_EXTENDED_HEADER_LENGTH - 1) as i32;
        assert_eq!(
            blosc2_chunk_zeros_c(cparams.clone(), 16, &mut dest, short_len),
            BLOSC2_ERROR_DATA
        );
        assert_eq!(
            blosc2_chunk_zeros_c(cparams.clone(), 16, &mut dest, -1),
            BLOSC2_ERROR_DATA
        );
        assert_eq!(
            blosc2_chunk_repeatval_c(cparams.clone(), 16, &mut dest, 32, &repeated),
            BLOSC2_ERROR_DATA
        );
        assert_eq!(
            blosc2_chunk_zeros_c(cparams.clone(), 10, &mut dest, 32),
            BLOSC2_ERROR_DATA
        );
        let negative_zero_len = blosc2_chunk_zeros_c(cparams.clone(), -1, &mut dest, 32);
        assert_eq!(negative_zero_len, BLOSC_EXTENDED_HEADER_LENGTH as i32);
        let negative_zero_header = ChunkHeader::read_minimal(&dest[..negative_zero_len as usize])
            .expect("negative zero special header should be written");
        assert_eq!(negative_zero_header.nbytes, -1);
        assert_eq!(negative_zero_header.blocksize, 1);
        assert_eq!(dest[BLOSC2_CHUNK_BLOSC2_FLAGS], BLOSC2_SPECIAL_ZERO << 4);
        assert_eq!(
            blosc2_chunk_uninit_c(cparams.clone(), -1, &mut dest, 32),
            BLOSC2_ERROR_DATA
        );
        assert_eq!(
            blosc2_chunk_nans_c(cparams.clone(), -1, &mut dest, 32),
            BLOSC2_ERROR_DATA
        );
        assert_eq!(
            blosc2_chunk_repeatval_c(cparams.clone(), -1, &mut dest, 36, &repeated),
            BLOSC2_ERROR_DATA
        );
        let negative_uninit_len = blosc2_chunk_uninit_c(cparams.clone(), -4, &mut dest, 32);
        assert_eq!(negative_uninit_len, BLOSC_EXTENDED_HEADER_LENGTH as i32);
        let negative_uninit_header =
            ChunkHeader::read_minimal(&dest[..negative_uninit_len as usize])
                .expect("negative uninit special header should be written");
        assert_eq!(negative_uninit_header.nbytes, -4);
        assert_eq!(dest[BLOSC2_CHUNK_BLOSC2_FLAGS], BLOSC2_SPECIAL_UNINIT << 4);
        let negative_nan_len = blosc2_chunk_nans_c(cparams.clone(), -4, &mut dest, 32);
        assert_eq!(negative_nan_len, BLOSC_EXTENDED_HEADER_LENGTH as i32);
        let negative_nan_header = ChunkHeader::read_minimal(&dest[..negative_nan_len as usize])
            .expect("negative NaN special header should be written");
        assert_eq!(negative_nan_header.nbytes, -4);
        assert_eq!(dest[BLOSC2_CHUNK_BLOSC2_FLAGS], BLOSC2_SPECIAL_NAN << 4);
        let negative_repeat_len =
            blosc2_chunk_repeatval_c(cparams.clone(), -4, &mut dest, 36, &repeated);
        assert_eq!(
            negative_repeat_len,
            (BLOSC_EXTENDED_HEADER_LENGTH + repeated.len()) as i32
        );
        let negative_repeat_header =
            ChunkHeader::read_minimal(&dest[..negative_repeat_len as usize])
                .expect("negative repeat-value special header should be written");
        assert_eq!(negative_repeat_header.nbytes, -4);
        assert_eq!(dest[BLOSC2_CHUNK_BLOSC2_FLAGS], BLOSC2_SPECIAL_VALUE << 4);
        assert_eq!(
            &dest[BLOSC_EXTENDED_HEADER_LENGTH..negative_repeat_len as usize],
            repeated
        );
        assert_eq!(
            blosc2_chunk_repeatval_c(cparams.clone(), 16, &mut dest, 36, &[1, 2]),
            BLOSC2_ERROR_DATA
        );
        let mut invalid_filter = cparams;
        invalid_filter.filters[BLOSC2_MAX_FILTERS - 1] = BLOSC2_GLOBAL_REGISTERED_FILTERS_START - 1;
        assert_eq!(
            blosc2_chunk_zeros_c(invalid_filter, 16, &mut dest, 32),
            BLOSC2_ERROR_NULL_POINTER
        );
    }

    #[test]
    fn test_special_chunk_constructors_with_cparams_honor_context_overrides() {
        let _lock = BLOSC_ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let prev_blocksize = blosc1_get_blocksize();
        let _typesize = EnvGuard::set("BLOSC_TYPESIZE", "4");
        let _blocksize = EnvGuard::set("BLOSC_BLOCKSIZE", "64");

        let cparams = CParams {
            typesize: 2,
            blocksize: 32,
            ..Default::default()
        };
        let zero = blosc2_chunk_zeros_with_cparams(128, &cparams).unwrap();
        let zero_header = ChunkHeader::read(&zero).unwrap();
        assert_eq!(zero_header.special_type(), BLOSC2_SPECIAL_ZERO);
        assert_eq!(zero_header.typesize, 4);
        assert_eq!(zero_header.blocksize, 64);
        assert_eq!(decompress(&zero).unwrap(), vec![0u8; 128]);

        let repeated = blosc2_chunk_repeatval_with_cparams(12, &[1, 2], &cparams).unwrap();
        let repeated_header = ChunkHeader::read(&repeated).unwrap();
        assert_eq!(repeated_header.special_type(), BLOSC2_SPECIAL_VALUE);
        assert_eq!(repeated_header.typesize, 4);
        assert_eq!(repeated_header.blocksize, 12);
        assert_eq!(
            repeated_header.cbytes as usize,
            BLOSC_EXTENDED_HEADER_LENGTH + 2
        );
        assert_eq!(decompress(&repeated).unwrap(), [1, 2].repeat(6));
        let mut repeated_dest = vec![0u8; BLOSC_EXTENDED_HEADER_LENGTH + 4];
        assert_eq!(
            blosc2_chunk_repeatval_c(
                cparams.clone(),
                12,
                &mut repeated_dest,
                (BLOSC_EXTENDED_HEADER_LENGTH + 4) as i32,
                &[1, 2, 9, 9],
            ),
            (BLOSC_EXTENDED_HEADER_LENGTH + 2) as i32
        );
        assert_eq!(
            decompress(&repeated_dest[..BLOSC_EXTENDED_HEADER_LENGTH + 2]).unwrap(),
            [1, 2].repeat(6)
        );

        assert!(blosc2_chunk_nans_with_cparams(16, &cparams).is_ok());
        assert!(blosc2_chunk_uninit_with_cparams(16, &cparams).is_ok());

        blosc1_set_blocksize(prev_blocksize);
    }

    #[test]
    fn test_special_chunk_no_env_constructor_ignores_context_overrides() {
        let _lock = BLOSC_ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let prev_blocksize = blosc1_get_blocksize();
        let _typesize = EnvGuard::set("BLOSC_TYPESIZE", "4");
        let _blocksize = EnvGuard::set("BLOSC_BLOCKSIZE", "64");

        let cparams = CParams {
            typesize: 2,
            blocksize: 32,
            ..Default::default()
        };
        let zero =
            special_chunk_with_cparams_no_env(BLOSC2_SPECIAL_ZERO, 128, &cparams, None).unwrap();
        let zero_header = ChunkHeader::read(&zero).unwrap();
        assert_eq!(zero_header.special_type(), BLOSC2_SPECIAL_ZERO);
        assert_eq!(zero_header.typesize, 2);
        assert_eq!(zero_header.blocksize, 32);
        assert_eq!(decompress(&zero).unwrap(), vec![0u8; 128]);

        let repeated =
            special_chunk_with_cparams_no_env(BLOSC2_SPECIAL_VALUE, 12, &cparams, Some(&[1, 2]))
                .unwrap();
        let repeated_header = ChunkHeader::read(&repeated).unwrap();
        assert_eq!(repeated_header.special_type(), BLOSC2_SPECIAL_VALUE);
        assert_eq!(repeated_header.typesize, 2);
        assert_eq!(decompress(&repeated).unwrap(), [1, 2].repeat(6));

        blosc1_set_blocksize(prev_blocksize);
    }

    #[test]
    fn test_special_chunk_large_typesize_validates_before_normalization() {
        let _lock = BLOSC_ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let _compressor = EnvGuard::remove("BLOSC_COMPRESSOR");
        let _typesize = EnvGuard::remove("BLOSC_TYPESIZE");
        let _blocksize = EnvGuard::remove("BLOSC_BLOCKSIZE");

        let cparams = CParams {
            typesize: 256,
            ..Default::default()
        };
        assert_eq!(
            blosc2_chunk_zeros_with_cparams(257, &cparams),
            Err("Invalid special value nbytes")
        );
        assert_eq!(
            blosc2_chunk_nans_with_cparams(257, &cparams),
            Err("Invalid special value nbytes")
        );
        assert_eq!(
            blosc2_chunk_uninit_with_cparams(257, &cparams),
            Err("Invalid special value nbytes")
        );
        let zero = blosc2_chunk_zeros_with_cparams(512, &cparams).unwrap();
        let header = ChunkHeader::read(&zero).unwrap();
        assert_eq!(header.typesize, 1);
        assert_eq!(decompress(&zero).unwrap(), vec![0u8; 512]);
        let repeat_value = vec![7u8; 256];
        let repeated = blosc2_chunk_repeatval_with_cparams(512, &repeat_value, &cparams).unwrap();
        let repeated_header = ChunkHeader::read(&repeated).unwrap();
        assert_eq!(repeated_header.typesize, 1);
        assert_eq!(
            repeated_header.cbytes as usize,
            BLOSC_EXTENDED_HEADER_LENGTH + 256
        );
    }

    #[test]
    fn test_special_chunk_with_cparams_allows_dict_for_special_chunks() {
        let cparams = CParams {
            compcode: BLOSC_BLOSCLZ,
            typesize: 1,
            use_dict: true,
            ..Default::default()
        };
        let zero = blosc2_chunk_zeros_with_cparams(8, &cparams).unwrap();
        let header = ChunkHeader::read(&zero).unwrap();
        assert_eq!(header.special_type(), BLOSC2_SPECIAL_ZERO);
        assert!(!header.use_dict());
        assert_eq!(decompress(&zero).unwrap(), vec![0u8; 8]);
    }

    #[test]
    fn test_special_chunk_cparams_skip_unused_codec_and_user_filter_execution() {
        let mut dest = vec![0u8; BLOSC_EXTENDED_HEADER_LENGTH];
        let unsupported_codec = CParams {
            compcode: 250,
            typesize: 4,
            ..Default::default()
        };
        let len = blosc2_chunk_zeros_c(unsupported_codec, 16, &mut dest, 32);
        assert_eq!(len, BLOSC_EXTENDED_HEADER_LENGTH as i32);
        assert_eq!(
            ChunkHeader::read(&dest[..len as usize])
                .unwrap()
                .special_type(),
            BLOSC2_SPECIAL_ZERO
        );

        let mut user_filter = CParams {
            typesize: 4,
            ..Default::default()
        };
        user_filter.filters[BLOSC2_MAX_FILTERS - 1] = BLOSC2_USER_REGISTERED_FILTERS_START;
        let len = blosc2_chunk_uninit_c(user_filter, 16, &mut dest, 32);
        assert_eq!(len, BLOSC_EXTENDED_HEADER_LENGTH as i32);
        assert_eq!(
            ChunkHeader::read(&dest[..len as usize])
                .unwrap()
                .special_type(),
            BLOSC2_SPECIAL_UNINIT
        );
    }

    #[test]
    fn test_special_repeatval_with_large_cparams_typesize_uses_header_typesize_like_c() {
        let _lock = BLOSC_ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let _compressor = EnvGuard::remove("BLOSC_COMPRESSOR");
        let _typesize = EnvGuard::remove("BLOSC_TYPESIZE");
        let _blocksize = EnvGuard::remove("BLOSC_BLOCKSIZE");

        let value: Vec<u8> = (0..300).map(|idx| (idx % 251) as u8).collect();
        let cparams = CParams {
            typesize: value.len() as i32,
            ..Default::default()
        };

        let repeated = blosc2_chunk_repeatval_with_cparams(value.len() * 2, &value, &cparams)
            .expect("large repeat-value payload should be accepted");
        let header = ChunkHeader::read(&repeated).unwrap();
        assert_eq!(header.special_type(), BLOSC2_SPECIAL_VALUE);
        assert_eq!(header.typesize, 1);
        assert_eq!(
            header.cbytes as usize,
            BLOSC_EXTENDED_HEADER_LENGTH + value.len()
        );
        assert_eq!(
            decompress(&repeated).unwrap(),
            vec![value[0]; value.len() * 2]
        );

        assert!(
            blosc2_chunk_repeatval_with_cparams(value.len() * 2, &value[..299], &cparams).is_err()
        );
        assert!(
            blosc2_chunk_repeatval_with_cparams(value.len() * 2 - 1, &value, &cparams).is_err()
        );
    }

    #[test]
    fn test_zero_length_special_repeatval_validation_allows_c_header() {
        let mut repeated = vec![0u8; BLOSC_EXTENDED_HEADER_LENGTH + 4];
        let header = ChunkHeader {
            version: BLOSC2_VERSION_FORMAT_STABLE,
            versionlz: BLOSC_BLOSCLZ_VERSION_FORMAT,
            flags: BLOSC_DOSHUFFLE | BLOSC_DOBITSHUFFLE,
            typesize: 4,
            nbytes: 0,
            blocksize: 1,
            cbytes: repeated.len() as i32,
            blosc2_flags: BLOSC2_SPECIAL_VALUE << 4,
            ..Default::default()
        };
        header
            .try_write(&mut repeated[..BLOSC_EXTENDED_HEADER_LENGTH])
            .unwrap();
        repeated[BLOSC_EXTENDED_HEADER_LENGTH..].copy_from_slice(&[1, 2, 3, 4]);

        validate_header(&header, repeated.len()).unwrap();
        assert_eq!(header.special_type(), BLOSC2_SPECIAL_VALUE);
        assert_eq!(header.nbytes, 0);
        assert_eq!(header.cbytes as usize, BLOSC_EXTENDED_HEADER_LENGTH + 4);
        let mut dest = [];
        write_special_range(
            &repeated,
            &header,
            0,
            BLOSC_EXTENDED_HEADER_LENGTH,
            0,
            &mut dest,
        )
        .unwrap();
    }

    #[test]
    fn test_getitem_large_special_repeatval_uses_header_typesize_like_c() {
        let _lock = BLOSC_ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let _compressor = EnvGuard::remove("BLOSC_COMPRESSOR");
        let _typesize = EnvGuard::remove("BLOSC_TYPESIZE");
        let _blocksize = EnvGuard::remove("BLOSC_BLOCKSIZE");

        let value: Vec<u8> = (0..300).map(|idx| (idx % 251) as u8).collect();
        let cparams = CParams {
            typesize: value.len() as i32,
            ..Default::default()
        };
        let repeated = blosc2_chunk_repeatval_with_cparams(value.len() * 2, &value, &cparams)
            .expect("large repeat-value payload should be accepted");

        assert_eq!(
            decompress(&repeated).unwrap(),
            vec![value[0]; value.len() * 2]
        );
        assert_eq!(getitem(&repeated, 0, 6).unwrap(), vec![value[0]; 6]);
        assert_eq!(getitem(&repeated, 10, 4).unwrap(), vec![value[0]; 4]);

        let dctx = DContext::new(DParams {
            postfilter: Some(xor_postfilter),
            typesize: 1,
            ..Default::default()
        });
        assert_eq!(
            dctx.get_items(&repeated, 10, 4).unwrap(),
            vec![value[0] ^ 0x5a; 4]
        );
        let mut dest = vec![0u8; 4];
        assert_eq!(
            blosc2_getitem_ctx_c(&dctx, &repeated, repeated.len() as i32, 10, 4, &mut dest, 4,),
            4
        );
        assert_eq!(dest, vec![value[0] ^ 0x5a; 4]);
    }

    #[test]
    fn test_special_zero_ignores_trailing_bytes() {
        let nbytes = 16usize;
        let mut chunk = vec![0u8; BLOSC_EXTENDED_HEADER_LENGTH + 1];
        let header = ChunkHeader {
            version: BLOSC2_VERSION_FORMAT_STABLE,
            versionlz: 1,
            flags: BLOSC_DOSHUFFLE | BLOSC_DOBITSHUFFLE,
            typesize: 4,
            nbytes: nbytes as i32,
            blocksize: nbytes as i32,
            cbytes: chunk.len() as i32,
            filters: [0, 0, 0, 0, 0, BLOSC_NOFILTER],
            blosc2_flags: BLOSC2_SPECIAL_ZERO << 4,
            ..Default::default()
        };
        header.write(&mut chunk[..BLOSC_EXTENDED_HEADER_LENGTH]);
        chunk[BLOSC_EXTENDED_HEADER_LENGTH] = 0xA5;

        assert_eq!(decompress(&chunk).unwrap(), vec![0; nbytes]);
    }

    #[test]
    fn test_truncated_compressed_stream_returns_error() {
        let data: Vec<u8> = (0..10000u32).flat_map(|i| i.to_le_bytes()).collect();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 4,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut chunk = compress(&data, &cparams).unwrap();
        chunk.pop();
        let truncated_len = chunk.len() as i32;
        chunk[12..16].copy_from_slice(&truncated_len.to_le_bytes());

        assert!(decompress(&chunk).is_err());
    }

    #[test]
    fn test_decode_accepts_unordered_regular_block_payloads() {
        let data: Vec<u8> = (0..200_000u32)
            .flat_map(|i| i.wrapping_mul(2654435761).to_le_bytes())
            .collect();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 4,
            blocksize: 32 * 1024,
            splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };

        let chunk = compress(&data, &cparams).unwrap();
        let header = ChunkHeader::read(&chunk).unwrap();
        let nblocks = header.nblocks();
        assert!(nblocks > 3);

        let header_len = header.header_len();
        let table_len = nblocks * 4;
        let mut spans = Vec::with_capacity(nblocks);
        for block_idx in 0..nblocks {
            let bstart_pos = header_len + block_idx * 4;
            let start =
                i32::from_le_bytes(chunk[bstart_pos..bstart_pos + 4].try_into().unwrap()) as usize;
            let end = compressed_block_limit(&chunk, &header, start, nblocks).unwrap();
            spans.push((start, end));
        }

        let mut reordered = chunk[..header_len + table_len].to_vec();
        for block_idx in (0..nblocks).rev() {
            let new_start = reordered.len();
            let bstart_pos = header_len + block_idx * 4;
            reordered[bstart_pos..bstart_pos + 4]
                .copy_from_slice(&(new_start as i32).to_le_bytes());
            let (start, end) = spans[block_idx];
            reordered.extend_from_slice(&chunk[start..end]);
        }
        assert_eq!(reordered.len(), chunk.len());

        assert_ne!(
            i32::from_le_bytes(reordered[header_len..header_len + 4].try_into().unwrap()),
            i32::from_le_bytes(
                reordered[header_len + 4..header_len + 8]
                    .try_into()
                    .unwrap()
            )
        );
        assert_eq!(decompress_with_threads(&reordered, 4).unwrap(), data);

        let replacement = vec![0xA5; cparams.blocksize as usize];
        let updated = replace_aligned_blocks(
            &reordered,
            cparams.blocksize as usize,
            &replacement,
            &cparams,
        )
        .unwrap()
        .expect("regular chunk block replacement should be supported");
        let mut expected = data;
        expected[cparams.blocksize as usize..cparams.blocksize as usize + replacement.len()]
            .copy_from_slice(&replacement);
        assert_eq!(decompress(&updated).unwrap(), expected);
    }

    #[test]
    fn test_always_split_small_block_roundtrip() {
        let data = [1u8, 2, 3];
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 8,
            splitmode: BLOSC_ALWAYS_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_NOFILTER],
            ..Default::default()
        };

        let compressed = compress(&data, &cparams).unwrap();
        let decompressed = decompress(&compressed).unwrap();
        assert_eq!(data, decompressed.as_slice());
    }

    #[test]
    fn test_always_split_run_streams_can_grow_output() {
        let data = vec![7u8; 255];
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 255,
            blocksize: 255,
            splitmode: BLOSC_ALWAYS_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_NOFILTER],
            ..Default::default()
        };

        let compressed = compress(&data, &cparams).unwrap();
        let decompressed = decompress(&compressed).unwrap();
        assert_eq!(data, decompressed);
    }

    #[test]
    fn test_compress_zeros() {
        let data = vec![0u8; 10000];
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 4,
            ..Default::default()
        };

        let compressed = compress(&data, &cparams).unwrap();
        let decompressed = decompress(&compressed).unwrap();
        assert_eq!(data, decompressed);
    }

    #[test]
    fn test_compress_various_typesizes() {
        let data: Vec<u8> = (0..20000u16).flat_map(|i| i.to_le_bytes()).collect();

        for typesize in [1, 2, 4, 8] {
            let cparams = CParams {
                compcode: BLOSC_LZ4,
                clevel: 5,
                typesize,
                splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
                filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
                ..Default::default()
            };

            let compressed = compress(&data, &cparams).unwrap();
            let decompressed = decompress(&compressed).unwrap();
            assert_eq!(
                data, decompressed,
                "Roundtrip failed for typesize={typesize}"
            );
        }
    }

    #[test]
    fn test_multithreaded_compress() {
        let data: Vec<u8> = (0..100000u32).flat_map(|i| i.to_le_bytes()).collect();

        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 4,
            nthreads: 4,
            splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };

        let compressed = compress(&data, &cparams).unwrap();
        let decompressed = decompress_with_threads(&compressed, 4).unwrap();
        assert_eq!(data, decompressed);
    }

    #[test]
    fn test_zstd_dictionary_chunk_roundtrip() {
        let data: Vec<u8> = (0..200_000u32)
            .flat_map(|i| {
                let value = i % 4096;
                value.to_le_bytes()
            })
            .collect();
        let cparams = CParams {
            compcode: BLOSC_ZSTD,
            clevel: 5,
            typesize: 4,
            blocksize: 4096,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            use_dict: true,
            ..Default::default()
        };

        let compressed = compress(&data, &cparams).unwrap();
        let header = ChunkHeader::read(&compressed).unwrap();
        assert!(header.use_dict());
        assert!(embedded_codec_dictionary(&compressed, &header)
            .unwrap()
            .is_some());

        let decompressed = decompress_with_threads(&compressed, 4).unwrap();
        assert_eq!(data, decompressed);
    }

    #[test]
    fn test_lz4_dictionary_chunk_roundtrip() {
        let data: Vec<u8> = (0..200_000u32)
            .flat_map(|i| {
                let value = i % 4096;
                value.to_le_bytes()
            })
            .collect();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 4,
            blocksize: 4096,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            use_dict: true,
            ..Default::default()
        };

        let compressed = compress(&data, &cparams).unwrap();
        let header = ChunkHeader::read(&compressed).unwrap();
        assert!(header.use_dict());
        assert!(embedded_codec_dictionary(&compressed, &header)
            .unwrap()
            .is_some());

        let decompressed = decompress_with_threads(&compressed, 4).unwrap();
        assert_eq!(data, decompressed);
    }

    #[test]
    fn test_dictionary_all_zero_keeps_usedict_layout() {
        let data = vec![0u8; 256 * 1024];
        for compcode in [BLOSC_LZ4, BLOSC_LZ4HC, BLOSC_ZSTD] {
            let cparams = CParams {
                compcode,
                clevel: 5,
                typesize: 4,
                blocksize: 4096,
                splitmode: BLOSC_NEVER_SPLIT,
                filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
                use_dict: true,
                ..Default::default()
            };

            let compressed = compress(&data, &cparams).unwrap();
            let header = ChunkHeader::read(&compressed).unwrap();
            assert_eq!(header.special_type(), BLOSC2_NO_SPECIAL);
            assert!(header.use_dict());
            assert_eq!(decompress(&compressed).unwrap(), data);
        }
    }

    fn add_embedded_dict_to_special_chunk(mut chunk: Vec<u8>, dict: &[u8]) -> Vec<u8> {
        let header = ChunkHeader::read(&chunk).unwrap();
        let header_len = header.header_len();
        let mut rebuilt = chunk[..header_len].to_vec();
        rebuilt.extend_from_slice(&(dict.len() as i32).to_le_bytes());
        rebuilt.extend_from_slice(dict);
        let cbytes = rebuilt.len() as i32;
        rebuilt[12..16].copy_from_slice(&cbytes.to_le_bytes());
        rebuilt[BLOSC2_CHUNK_BLOSC2_FLAGS] |= BLOSC2_USEDICT;
        chunk = rebuilt;
        chunk
    }

    #[test]
    fn test_dictionary_flagged_special_and_memcpy_chunks_are_read_like_c() {
        let dict = vec![7u8; BLOSC2_MINUSEFULDICT];

        let zeros = vec![0u8; 4096];
        let zero_chunk = compress(
            &zeros,
            &CParams {
                compcode: BLOSC_LZ4,
                clevel: 5,
                typesize: 4,
                blocksize: 4096,
                filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
                ..Default::default()
            },
        )
        .unwrap();
        let zero_with_dict = add_embedded_dict_to_special_chunk(zero_chunk, &dict);
        assert!(ChunkHeader::read(&zero_with_dict).unwrap().use_dict());
        assert!(validate_chunk(&zero_with_dict).is_ok());
        assert_eq!(decompress(&zero_with_dict).unwrap(), zeros);

        let data: Vec<u8> = (0..4096u32).flat_map(|i| i.to_le_bytes()).collect();
        let memcpy_chunk = compress(
            &data,
            &CParams {
                compcode: BLOSC_LZ4,
                clevel: 0,
                typesize: 4,
                blocksize: 4096,
                filters: [0; BLOSC2_MAX_FILTERS],
                ..Default::default()
            },
        )
        .unwrap();
        assert!(ChunkHeader::read(&memcpy_chunk).unwrap().memcpyed());
        let mut memcpy_with_fake_bstarts = memcpy_chunk.clone();
        let header = ChunkHeader::read(&memcpy_with_fake_bstarts).unwrap();
        let header_len = header.header_len();
        let raw_payload = memcpy_with_fake_bstarts[header_len..].to_vec();
        memcpy_with_fake_bstarts.truncate(header_len);
        memcpy_with_fake_bstarts.extend(std::iter::repeat_n(0u8, header.nblocks() * 4));
        memcpy_with_fake_bstarts.extend_from_slice(&(dict.len() as i32).to_le_bytes());
        memcpy_with_fake_bstarts.extend_from_slice(&dict);
        memcpy_with_fake_bstarts.extend_from_slice(&raw_payload);
        let cbytes = memcpy_with_fake_bstarts.len() as i32;
        memcpy_with_fake_bstarts[12..16].copy_from_slice(&cbytes.to_le_bytes());
        memcpy_with_fake_bstarts[BLOSC2_CHUNK_BLOSC2_FLAGS] |= BLOSC2_USEDICT;
        assert!(validate_chunk(&memcpy_with_fake_bstarts).is_err());

        let mut memcpy_with_dict_flag = memcpy_chunk;
        memcpy_with_dict_flag[BLOSC2_CHUNK_BLOSC2_FLAGS] |= BLOSC2_USEDICT;
        assert!(validate_chunk(&memcpy_with_dict_flag).is_err());
        assert!(decompress(&memcpy_with_dict_flag).is_err());
    }

    fn sequence_codec_compress(_clevel: u8, meta: u8, src: &[u8], dest: &mut [u8]) -> i32 {
        if src.len() < 2 || dest.len() < 3 {
            return 0;
        }
        dest[0] = src[0];
        dest[1] = src[1].wrapping_sub(src[0]);
        dest[2] = meta;
        3
    }

    fn sequence_codec_decompress(meta: u8, src: &[u8], dest: &mut [u8]) -> i32 {
        if src.len() != 3 || src[2] != meta {
            return -1;
        }
        for (idx, byte) in dest.iter_mut().enumerate() {
            *byte = src[0].wrapping_add(src[1].wrapping_mul(idx as u8));
        }
        dest.len() as i32
    }

    fn failing_codec_compress(_clevel: u8, _meta: u8, _src: &[u8], _dest: &mut [u8]) -> i32 {
        -1
    }

    fn failing_codec_decompress(_meta: u8, _src: &[u8], _dest: &mut [u8]) -> i32 {
        -1
    }

    fn unwritten_short_codec_compress(
        _clevel: u8,
        _meta: u8,
        _src: &[u8],
        _dest: &mut [u8],
    ) -> i32 {
        3
    }

    fn unwritten_full_codec_decompress(_meta: u8, _src: &[u8], dest: &mut [u8]) -> i32 {
        dest.len() as i32
    }

    fn context_copy_filter(
        ctx: &mut filters::FilterCallbackContext<'_>,
        src: &[u8],
        dest: &mut [u8],
    ) -> i32 {
        if let Some(cparams) = ctx.cparams {
            for (out, byte) in dest.iter_mut().zip(src) {
                *out = byte.wrapping_add(1);
            }
            FILTER_FORWARD_ID.store(ctx.filter_id, AtomicOrdering::SeqCst);
            FILTER_FORWARD_META.store(ctx.meta, AtomicOrdering::SeqCst);
            FILTER_FORWARD_CLEVEL.store(cparams.clevel, AtomicOrdering::SeqCst);
            FILTER_FORWARD_NCHUNK.store(ctx.chunk.nchunk as i32, AtomicOrdering::SeqCst);
            FILTER_FORWARD_NBLOCK.store(ctx.chunk.nblock, AtomicOrdering::SeqCst);
            FILTER_FORWARD_USER_DATA.store(ctx.user_data, AtomicOrdering::SeqCst);
        }
        if ctx.dparams.is_some() {
            for (out, byte) in dest.iter_mut().zip(src) {
                *out = byte.wrapping_sub(1);
            }
            FILTER_BACKWARD_ID.store(ctx.filter_id, AtomicOrdering::SeqCst);
            FILTER_BACKWARD_META.store(ctx.meta, AtomicOrdering::SeqCst);
            FILTER_BACKWARD_NCHUNK.store(ctx.chunk.nchunk as i32, AtomicOrdering::SeqCst);
            FILTER_BACKWARD_NBLOCK.store(ctx.chunk.nblock, AtomicOrdering::SeqCst);
            FILTER_BACKWARD_USER_DATA.store(ctx.user_data, AtomicOrdering::SeqCst);
        }
        filters::PluginCallbackStatus::Success as i32
    }

    fn context_sequence_codec_compress(
        ctx: &mut codecs::CodecCallbackContext<'_>,
        src: &[u8],
        dest: &mut [u8],
    ) -> i32 {
        if src.len() < 2 || dest.len() < 3 {
            return 0;
        }
        CODEC_COMPRESS_CODE.store(ctx.compcode, AtomicOrdering::SeqCst);
        CODEC_COMPRESS_META.store(ctx.meta, AtomicOrdering::SeqCst);
        CODEC_COMPRESS_CLEVEL.store(ctx.clevel, AtomicOrdering::SeqCst);
        CODEC_COMPRESS_NCHUNK.store(ctx.chunk.nchunk as i32, AtomicOrdering::SeqCst);
        CODEC_COMPRESS_NBLOCK.store(ctx.chunk.nblock, AtomicOrdering::SeqCst);
        CODEC_COMPRESS_USER_DATA.store(ctx.user_data, AtomicOrdering::SeqCst);
        dest[0] = src[0];
        dest[1] = src[1].wrapping_sub(src[0]);
        dest[2] = ctx.meta;
        3
    }

    fn context_sequence_codec_decompress(
        ctx: &mut codecs::CodecCallbackContext<'_>,
        src: &[u8],
        dest: &mut [u8],
    ) -> i32 {
        if src.len() != 3 || src[2] != ctx.meta {
            return -1;
        }
        CODEC_DECOMPRESS_CODE.store(ctx.compcode, AtomicOrdering::SeqCst);
        CODEC_DECOMPRESS_META.store(ctx.meta, AtomicOrdering::SeqCst);
        CODEC_DECOMPRESS_NCHUNK.store(ctx.chunk.nchunk as i32, AtomicOrdering::SeqCst);
        CODEC_DECOMPRESS_NBLOCK.store(ctx.chunk.nblock, AtomicOrdering::SeqCst);
        CODEC_DECOMPRESS_USER_DATA.store(ctx.user_data, AtomicOrdering::SeqCst);
        for (idx, byte) in dest.iter_mut().enumerate() {
            *byte = src[0].wrapping_add(src[1].wrapping_mul(idx as u8));
        }
        dest.len() as i32
    }

    unsafe extern "C" fn c_abi_copy_codec_compress(
        input: *const u8,
        input_len: i32,
        output: *mut u8,
        output_len: i32,
        _meta: u8,
        cparams: *mut codecs::Blosc2CParams,
        _chunk: *const std::ffi::c_void,
    ) -> i32 {
        if input.is_null() || output.is_null() || cparams.is_null() || input_len < 0 {
            return -1;
        }
        if output_len < input_len {
            return 0;
        }
        let cparams = unsafe { &*cparams };
        CODEC_COMPRESS_CODEC_PARAMS.store(cparams.codec_params as usize, AtomicOrdering::SeqCst);
        CODEC_COMPRESS_BLOCKSIZE.store(cparams.blocksize, AtomicOrdering::SeqCst);
        CODEC_COMPRESS_PREPARAMS.store(cparams.preparams as usize, AtomicOrdering::SeqCst);
        unsafe {
            std::ptr::copy_nonoverlapping(input, output, input_len as usize);
        }
        input_len
    }

    unsafe extern "C" fn c_abi_sequence_codec_compress(
        input: *const u8,
        input_len: i32,
        output: *mut u8,
        output_len: i32,
        meta: u8,
        cparams: *mut codecs::Blosc2CParams,
        chunk: *const std::ffi::c_void,
    ) -> i32 {
        if input.is_null() || output.is_null() || input_len < 2 || output_len < 3 {
            return -1;
        }
        if !cparams.is_null() {
            let cparams = unsafe { &*cparams };
            CODEC_COMPRESS_PREPARAMS.store(cparams.preparams as usize, AtomicOrdering::SeqCst);
        }
        CODEC_COMPRESS_CHUNK_ARG.store(chunk as usize, AtomicOrdering::SeqCst);
        unsafe {
            let first = *input;
            let second = *input.add(1);
            *output = first;
            *output.add(1) = second.wrapping_sub(first);
            *output.add(2) = meta;
        }
        3
    }

    unsafe extern "C" fn c_abi_sequence_codec_decompress(
        input: *const u8,
        input_len: i32,
        output: *mut u8,
        output_len: i32,
        meta: u8,
        dparams: *mut codecs::Blosc2DParams,
        chunk: *const std::ffi::c_void,
    ) -> i32 {
        if input.is_null() || output.is_null() || input_len != 3 || output_len < 0 {
            return -1;
        }
        if !dparams.is_null() {
            let dparams = unsafe { &*dparams };
            CODEC_DECOMPRESS_POSTPARAMS.store(dparams.postparams as usize, AtomicOrdering::SeqCst);
        }
        CODEC_DECOMPRESS_CHUNK_ARG.store(chunk as usize, AtomicOrdering::SeqCst);
        unsafe {
            let first = *input;
            let stride = *input.add(1);
            if *input.add(2) != meta {
                return -1;
            }
            for idx in 0..output_len as usize {
                *output.add(idx) = first.wrapping_add(stride.wrapping_mul(idx as u8));
            }
        }
        output_len
    }

    fn register_c_abi_sequence_codec(name_prefix: &str) -> u8 {
        (220..=251)
            .find(|&candidate| {
                let name = format!("{name_prefix}-{candidate}\0");
                let name: &'static [u8] = Box::leak(name.into_bytes().into_boxed_slice());
                let codec = codecs::Blosc2CodecAbi {
                    compcode: candidate,
                    compname: name.as_ptr().cast(),
                    complib: candidate,
                    version: 1,
                    encoder: Some(c_abi_sequence_codec_compress),
                    decoder: Some(c_abi_sequence_codec_decompress),
                };
                codecs::blosc2_register_codec_abi(&codec as *const codecs::Blosc2CodecAbi)
                    == BLOSC2_ERROR_SUCCESS
            })
            .expect("test needs an available user codec ID")
    }

    fn register_blosc2_sequence_codec(
        name_prefix: &str,
        complib: u8,
        version: u8,
    ) -> (u8, &'static str) {
        (161..=251)
            .find_map(|candidate| {
                let name: &'static str =
                    Box::leak(format!("{name_prefix}{candidate}").into_boxed_str());
                let codec = codecs::Blosc2Codec {
                    compcode: candidate,
                    compname: name,
                    complib,
                    version,
                    encoder: sequence_codec_compress,
                    decoder: sequence_codec_decompress,
                };
                (codecs::blosc2_register_codec(&codec) == BLOSC2_ERROR_SUCCESS)
                    .then_some((candidate, name))
            })
            .expect("test needs an available user codec ID")
    }

    fn register_legacy_sequence_codec() -> u8 {
        (161..=251)
            .find(|&candidate| {
                codecs::register_codec(
                    candidate,
                    sequence_codec_compress,
                    sequence_codec_decompress,
                )
                .is_ok()
            })
            .expect("test needs an available user codec ID")
    }

    fn register_named_sequence_codec(name_prefix: &str) -> (u8, &'static str) {
        (161..=251)
            .find_map(|candidate| {
                let name: &'static str =
                    Box::leak(format!("{name_prefix}{candidate}").into_boxed_str());
                codecs::register_named_codec(
                    candidate,
                    name,
                    sequence_codec_compress,
                    sequence_codec_decompress,
                )
                .is_ok()
                .then_some((candidate, name))
            })
            .expect("test needs an available user codec ID")
    }

    fn first_unused_complib() -> u8 {
        (44..=159)
            .find(|&complib| codecs::registered_codec_name_by_complib(complib).is_none())
            .expect("test needs an available user codec library ID")
    }

    #[test]
    fn test_context_filter_callbacks_receive_cparams_dparams_and_chunk_context() {
        let _guard = CALLBACK_ABI_LOCK.lock().unwrap();
        const FILTER_ID: u8 = 253;
        filters::register_context_filter(FILTER_ID, context_copy_filter, context_copy_filter)
            .unwrap();

        FILTER_FORWARD_ID.store(0, AtomicOrdering::SeqCst);
        FILTER_BACKWARD_ID.store(0, AtomicOrdering::SeqCst);
        let data: Vec<u8> = (0..16u8).collect();
        let mut filter_ids = [BLOSC_NOFILTER; BLOSC2_MAX_FILTERS];
        let mut filter_meta = [0; BLOSC2_MAX_FILTERS];
        filter_ids[BLOSC2_MAX_FILTERS - 1] = FILTER_ID;
        filter_meta[BLOSC2_MAX_FILTERS - 1] = 77;
        let cparams_context = filters::FilterCParamsContext {
            compcode: BLOSC_BLOSCLZ,
            compcode_meta: 0,
            clevel: 5,
            use_dict: false,
            typesize: 1,
            blocksize: 16,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: filter_ids,
            filters_meta: filter_meta,
            nthreads: 1,
            nchunk: 11,
            user_data: 0x55,
            preparams: 0x55,
            tuner_id: 0,
            instr_codec: false,
            codec_params: 0,
        };
        let dparams_context = filters::FilterDParamsContext {
            nthreads: 1,
            typesize: 1,
            nchunk: 12,
            user_data: 0x66,
            postparams: 0x66,
        };
        let chunk_context = filters::FilterChunkContext {
            schunk: 0,
            nchunk: 11,
            nblock: 3,
            block_offset: 48,
            blocksize: 16,
            bsize: data.len(),
        };
        let mut forward_buf1 = vec![0; data.len()];
        let mut forward_buf2 = vec![0; data.len()];
        let forward_selected = filters::apply_filter_pipeline_for_compression_with_context(
            &data,
            &mut forward_buf1,
            &mut forward_buf2,
            &filter_ids,
            &filter_meta,
            1,
            48,
            None,
            Some(filters::FilterPipelineContext {
                cparams: Some(&cparams_context),
                dparams: None,
                chunk: chunk_context,
                b2nd_metalayer: None,
                user_data: cparams_context.user_data,
            }),
        );
        assert_eq!(forward_selected, 1);
        let mut backward_buf1 = if forward_selected == 1 {
            forward_buf1
        } else {
            forward_buf2
        };
        let mut backward_buf2 = vec![0; data.len()];
        assert_eq!(
            filters::apply_filter_pipeline_for_decompression_with_context(
                &mut backward_buf1,
                &mut backward_buf2,
                data.len(),
                &filter_ids,
                &filter_meta,
                BLOSC2_VERSION_FORMAT_STABLE,
                1,
                48,
                None,
                1,
                Some(filters::FilterPipelineContext {
                    cparams: None,
                    dparams: Some(&dparams_context),
                    chunk: filters::FilterChunkContext {
                        nchunk: dparams_context.nchunk,
                        ..chunk_context
                    },
                    b2nd_metalayer: None,
                    user_data: dparams_context.user_data,
                }),
            ),
            2
        );
        assert_eq!(backward_buf2, data);
        assert_eq!(FILTER_FORWARD_ID.load(AtomicOrdering::SeqCst), FILTER_ID);
        assert_eq!(FILTER_FORWARD_META.load(AtomicOrdering::SeqCst), 77);
        assert_eq!(FILTER_FORWARD_CLEVEL.load(AtomicOrdering::SeqCst), 5);
        assert_eq!(FILTER_FORWARD_NCHUNK.load(AtomicOrdering::SeqCst), 11);
        assert!(FILTER_FORWARD_NBLOCK.load(AtomicOrdering::SeqCst) >= 0);
        assert_eq!(FILTER_FORWARD_USER_DATA.load(AtomicOrdering::SeqCst), 0x55);
        assert_eq!(FILTER_BACKWARD_ID.load(AtomicOrdering::SeqCst), FILTER_ID);
        assert_eq!(FILTER_BACKWARD_META.load(AtomicOrdering::SeqCst), 77);
        assert_eq!(FILTER_BACKWARD_NCHUNK.load(AtomicOrdering::SeqCst), 12);
        assert!(FILTER_BACKWARD_NBLOCK.load(AtomicOrdering::SeqCst) >= 0);
        assert_eq!(FILTER_BACKWARD_USER_DATA.load(AtomicOrdering::SeqCst), 0x66);
    }

    #[test]
    fn test_context_codec_callbacks_receive_cparams_dparams_and_chunk_context() {
        let _guard = CALLBACK_ABI_LOCK.lock().unwrap();
        const CODEC_ID: u8 = 253;
        codecs::register_context_codec(
            CODEC_ID,
            context_sequence_codec_compress,
            context_sequence_codec_decompress,
        )
        .unwrap();

        CODEC_COMPRESS_CODE.store(0, AtomicOrdering::SeqCst);
        CODEC_DECOMPRESS_CODE.store(0, AtomicOrdering::SeqCst);
        let data: Vec<u8> = (0..128u8).collect();
        let cparams = CParams {
            compcode: CODEC_ID,
            compcode_meta: 91,
            clevel: 6,
            typesize: 1,
            blocksize: 128,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [BLOSC_NOFILTER; BLOSC2_MAX_FILTERS],
            nchunk: 21,
            prefilter_user_data: 0x77,
            ..Default::default()
        };
        let compressed = compress(&data, &cparams).unwrap();
        let dparams = DParams {
            typesize: 1,
            nchunk: 22,
            postfilter_user_data: 0x88,
            ..Default::default()
        };

        assert_eq!(
            decompress_with_dparams(&compressed, &dparams).unwrap(),
            data
        );
        assert_eq!(CODEC_COMPRESS_CODE.load(AtomicOrdering::SeqCst), CODEC_ID);
        assert_eq!(CODEC_COMPRESS_META.load(AtomicOrdering::SeqCst), 91);
        assert_eq!(CODEC_COMPRESS_CLEVEL.load(AtomicOrdering::SeqCst), 6);
        assert_eq!(CODEC_COMPRESS_NCHUNK.load(AtomicOrdering::SeqCst), 21);
        assert_eq!(CODEC_COMPRESS_NBLOCK.load(AtomicOrdering::SeqCst), 0);
        assert_eq!(CODEC_COMPRESS_USER_DATA.load(AtomicOrdering::SeqCst), 0x77);
        assert_eq!(CODEC_DECOMPRESS_CODE.load(AtomicOrdering::SeqCst), CODEC_ID);
        assert_eq!(CODEC_DECOMPRESS_META.load(AtomicOrdering::SeqCst), 91);
        assert_eq!(CODEC_DECOMPRESS_NCHUNK.load(AtomicOrdering::SeqCst), 22);
        assert_eq!(CODEC_DECOMPRESS_NBLOCK.load(AtomicOrdering::SeqCst), 0);
        assert_eq!(
            CODEC_DECOMPRESS_USER_DATA.load(AtomicOrdering::SeqCst),
            0x88
        );
    }

    #[test]
    fn test_c_abi_codec_prepostparams_do_not_reuse_rust_filter_user_data() {
        let _guard = CALLBACK_ABI_LOCK.lock().unwrap();
        let codec_id = register_c_abi_sequence_codec("raw-codec-null-rust-filter-params");
        let data: Vec<u8> = (0..128u8).collect();
        let cparams = CParams {
            compcode: codec_id,
            compcode_meta: 19,
            typesize: 1,
            blocksize: data.len() as i32,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [BLOSC_NOFILTER; BLOSC2_MAX_FILTERS],
            prefilter_user_data: 0x77,
            ..Default::default()
        };
        let dparams = DParams {
            typesize: 1,
            postfilter_user_data: 0x88,
            ..Default::default()
        };

        CODEC_COMPRESS_PREPARAMS.store(usize::MAX, AtomicOrdering::SeqCst);
        CODEC_DECOMPRESS_POSTPARAMS.store(usize::MAX, AtomicOrdering::SeqCst);
        let compressed = compress(&data, &cparams).unwrap();

        assert_eq!(
            decompress_with_dparams(&compressed, &dparams).unwrap(),
            data
        );
        assert_eq!(CODEC_COMPRESS_PREPARAMS.load(AtomicOrdering::SeqCst), 0);
        assert_eq!(CODEC_DECOMPRESS_POSTPARAMS.load(AtomicOrdering::SeqCst), 0);
    }

    #[test]
    fn test_cparams_codec_params_reach_c_abi_codec_callbacks() {
        let _guard = CALLBACK_ABI_LOCK.lock().unwrap();
        let codec_id = (220..=251)
            .find(|&candidate| {
                let name = format!("compress-c-abi-codec-params-{candidate}\0");
                let name: &'static [u8] = Box::leak(name.into_bytes().into_boxed_slice());
                let codec = codecs::Blosc2CodecAbi {
                    compcode: candidate,
                    compname: name.as_ptr().cast(),
                    complib: candidate,
                    version: 1,
                    encoder: Some(c_abi_copy_codec_compress),
                    decoder: None,
                };
                codecs::blosc2_register_codec_abi(&codec as *const codecs::Blosc2CodecAbi)
                    == BLOSC2_ERROR_SUCCESS
            })
            .expect("test needs an available user codec ID");

        CODEC_COMPRESS_CODEC_PARAMS.store(0, AtomicOrdering::SeqCst);
        CODEC_COMPRESS_BLOCKSIZE.store(0, AtomicOrdering::SeqCst);
        let data: Vec<u8> = (0..100_000).map(|idx| (idx % 251) as u8).collect();
        let cparams = CParams {
            compcode: codec_id,
            clevel: 5,
            typesize: 1,
            blocksize: 0,
            filters: [BLOSC_NOFILTER; BLOSC2_MAX_FILTERS],
            codec_params: 0xc0decafe,
            ..Default::default()
        };
        let expected_blocksize = compute_blocksize(&cparams, data.len() as i32);
        let _ = compress(&data, &cparams).unwrap();

        assert_eq!(
            CODEC_COMPRESS_CODEC_PARAMS.load(AtomicOrdering::SeqCst),
            0xc0decafe
        );
        assert_eq!(
            CODEC_COMPRESS_BLOCKSIZE.load(AtomicOrdering::SeqCst),
            expected_blocksize
        );
    }

    #[test]
    fn test_c_abi_decoder_receives_regular_chunk_source() {
        let _guard = CALLBACK_ABI_LOCK.lock().unwrap();
        let codec_id = register_c_abi_sequence_codec("regular-decode-chunk-source");
        let data: Vec<u8> = (0..128u8).collect();
        let cparams = CParams {
            compcode: codec_id,
            compcode_meta: 17,
            typesize: 1,
            blocksize: data.len() as i32,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [BLOSC_NOFILTER; BLOSC2_MAX_FILTERS],
            ..Default::default()
        };
        let compressed = compress(&data, &cparams).unwrap();

        CODEC_DECOMPRESS_CHUNK_ARG.store(0, AtomicOrdering::SeqCst);
        assert_eq!(decompress(&compressed).unwrap(), data);
        assert_eq!(
            CODEC_DECOMPRESS_CHUNK_ARG.load(AtomicOrdering::SeqCst),
            compressed.as_ptr() as usize
        );

        CODEC_DECOMPRESS_CHUNK_ARG.store(0, AtomicOrdering::SeqCst);
        let mut dest = vec![0; data.len()];
        assert_eq!(decompress_into(&compressed, &mut dest).unwrap(), data.len());
        assert_eq!(dest, data);
        assert_eq!(
            CODEC_DECOMPRESS_CHUNK_ARG.load(AtomicOrdering::SeqCst),
            compressed.as_ptr() as usize
        );
    }

    #[test]
    fn test_c_abi_decoder_receives_block_data_chunk_source() {
        let _guard = CALLBACK_ABI_LOCK.lock().unwrap();
        let codec_id = register_c_abi_sequence_codec("block-data-decode-chunk-source");
        let data: Vec<u8> = (0..128u8).collect();
        let cparams = CParams {
            compcode: codec_id,
            compcode_meta: 23,
            typesize: 1,
            blocksize: data.len() as i32,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [BLOSC_NOFILTER; BLOSC2_MAX_FILTERS],
            ..Default::default()
        };
        let compressed = compress(&data, &cparams).unwrap();

        CODEC_DECOMPRESS_CHUNK_ARG.store(0, AtomicOrdering::SeqCst);
        assert_eq!(getitem(&compressed, 0, data.len()).unwrap(), data);
        assert_eq!(
            CODEC_DECOMPRESS_CHUNK_ARG.load(AtomicOrdering::SeqCst),
            compressed.as_ptr() as usize
        );
    }

    #[test]
    fn test_c_abi_decoder_receives_vl_chunk_source() {
        let _guard = CALLBACK_ABI_LOCK.lock().unwrap();
        let codec_id = register_c_abi_sequence_codec("vl-decode-chunk-source");
        let blocks: [&[u8]; 2] = [b"abcdefgh", b"qrstuvwxyz"];
        let cparams = CParams {
            compcode: codec_id,
            compcode_meta: 31,
            typesize: 1,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [BLOSC_NOFILTER; BLOSC2_MAX_FILTERS],
            ..Default::default()
        };
        let compressed = vlcompress(&blocks, &cparams).unwrap();

        CODEC_DECOMPRESS_CHUNK_ARG.store(0, AtomicOrdering::SeqCst);
        assert_eq!(vldecompress_block(&compressed, 1).unwrap(), blocks[1]);
        assert_eq!(
            CODEC_DECOMPRESS_CHUNK_ARG.load(AtomicOrdering::SeqCst),
            compressed.as_ptr() as usize
        );
    }

    #[test]
    fn test_c_abi_encoder_receives_null_vl_chunk_source() {
        let _guard = CALLBACK_ABI_LOCK.lock().unwrap();
        let codec_id = register_c_abi_sequence_codec("vl-compress-null-chunk-source");
        let blocks: [&[u8]; 2] = [b"abcdefgh", b"qrstuvwxyz"];
        let cparams = CParams {
            compcode: codec_id,
            compcode_meta: 37,
            typesize: 1,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [BLOSC_NOFILTER; BLOSC2_MAX_FILTERS],
            ..Default::default()
        };

        CODEC_COMPRESS_CHUNK_ARG.store(usize::MAX, AtomicOrdering::SeqCst);
        let compressed = vlcompress(&blocks, &cparams).unwrap();

        assert_eq!(CODEC_COMPRESS_CHUNK_ARG.load(AtomicOrdering::SeqCst), 0);
        assert_eq!(vldecompress_block(&compressed, 1).unwrap(), blocks[1]);
    }

    #[test]
    fn test_codec_cparams_context_forwards_use_dict() {
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            use_dict: true,
            ..Default::default()
        };
        assert_eq!(
            codec_cparams_context(&cparams, cparams.blocksize).use_dict,
            1
        );

        let cparams = CParams {
            use_dict: false,
            ..cparams
        };
        assert_eq!(
            codec_cparams_context(&cparams, cparams.blocksize).use_dict,
            0
        );
    }

    #[test]
    fn test_cparams_instr_codec_default_and_filter_context_forwarding() {
        let default_cparams = CParams::default();
        assert!(!default_cparams.instr_codec);

        let cparams = CParams {
            instr_codec: true,
            ..Default::default()
        };
        assert!(CContext::new(cparams.clone()).cparams().instr_codec);
        assert!(filter_cparams_context(&cparams, 128).instr_codec);
        assert!(codec_cparams_context(&cparams, 128).instr_codec);
    }

    #[test]
    fn test_user_defined_codec_roundtrip_and_metadata() {
        assert_eq!(
            codecs::register_codec(32, sequence_codec_compress, sequence_codec_decompress),
            Err("User-defined codec IDs must be >= 160")
        );
        assert_eq!(
            codecs::register_codec(159, sequence_codec_compress, sequence_codec_decompress),
            Err("User-defined codec IDs must be >= 160")
        );
        let (c_codec_id, c_codec_name) =
            register_blosc2_sequence_codec("compress-sequence-c-", BLOSC_UDCODEC_FORMAT, 1);
        let c_codec = codecs::Blosc2Codec {
            compcode: c_codec_id,
            compname: c_codec_name,
            complib: BLOSC_UDCODEC_FORMAT,
            version: 1,
            encoder: sequence_codec_compress,
            decoder: sequence_codec_decompress,
        };
        assert_eq!(
            codecs::blosc2_register_codec_c(None),
            BLOSC2_ERROR_INVALID_PARAM
        );
        assert_eq!(
            codecs::blosc2_register_codec_c(Some(&c_codec)),
            BLOSC2_ERROR_SUCCESS
        );
        assert_eq!(blosc2_compname_to_compcode(c_codec_name), Some(c_codec_id));
        assert_eq!(
            blosc2_compcode_to_compname_c(c_codec_id),
            (i32::from(c_codec_id), Some(c_codec_name))
        );
        let complib_info = blosc2_get_complib_info(c_codec_name).unwrap();
        assert_eq!(complib_info.0, BLOSC_UDCODEC_FORMAT);
        assert_eq!(complib_info.2, "unknown");
        let empty_name_codec_id = (161..=251)
            .find(|&candidate| {
                let empty_name_codec = codecs::Blosc2Codec {
                    compcode: candidate,
                    compname: "",
                    complib: first_unused_complib(),
                    version: 1,
                    encoder: sequence_codec_compress,
                    decoder: sequence_codec_decompress,
                };
                codecs::blosc2_register_codec(&empty_name_codec) == BLOSC2_ERROR_SUCCESS
            })
            .expect("test needs an available user codec ID");
        let empty_name_codec = codecs::Blosc2Codec {
            compcode: empty_name_codec_id,
            compname: "",
            complib: first_unused_complib(),
            version: 1,
            encoder: sequence_codec_compress,
            decoder: sequence_codec_decompress,
        };
        assert_eq!(
            codecs::blosc2_register_codec(&empty_name_codec),
            BLOSC2_ERROR_SUCCESS
        );
        assert_eq!(
            blosc2_compcode_to_compname_c(empty_name_codec_id),
            (i32::from(empty_name_codec_id), Some(""))
        );
        assert_eq!(blosc2_compname_to_compcode(""), Some(empty_name_codec_id));
        let shared_complib = first_unused_complib();
        let (higher_code, higher_name) =
            register_blosc2_sequence_codec("compress-sequence-high-fmt-", shared_complib, 1);
        let higher_first_same_complib = codecs::Blosc2Codec {
            compcode: higher_code,
            compname: higher_name,
            complib: shared_complib,
            version: 1,
            encoder: sequence_codec_compress,
            decoder: sequence_codec_decompress,
        };
        let (lower_code, lower_name) =
            register_blosc2_sequence_codec("compress-sequence-low-fmt-", shared_complib, 1);
        assert_eq!(
            codecs::registered_codec_name_by_complib(shared_complib),
            Some(higher_name)
        );
        assert_eq!(
            blosc2_get_complib_info(lower_name),
            Some((shared_complib, higher_name, "unknown"))
        );
        assert_eq!(higher_first_same_complib.compcode, higher_code);
        assert_eq!(lower_code, blosc2_compname_to_compcode(lower_name).unwrap());
        let same_name_different_callbacks = codecs::Blosc2Codec {
            encoder: failing_codec_compress,
            decoder: failing_codec_decompress,
            ..c_codec
        };
        assert_eq!(
            codecs::blosc2_register_codec(&same_name_different_callbacks),
            BLOSC2_ERROR_SUCCESS
        );
        assert!(compress(
            b"still-uses-first-codec",
            &CParams {
                compcode: c_codec.compcode,
                clevel: 1,
                typesize: 1,
                ..Default::default()
            }
        )
        .is_ok());
        let invalid_c_codec = codecs::Blosc2Codec {
            compcode: 159,
            ..c_codec
        };
        assert_eq!(
            codecs::blosc2_register_codec(&invalid_c_codec),
            BLOSC2_ERROR_CODEC_PARAM
        );
        let duplicate_c_name_id = (161..=251)
            .find(|&candidate| {
                candidate != c_codec_id
                    && codecs::blosc2_register_codec(&codecs::Blosc2Codec {
                        compcode: candidate,
                        ..c_codec
                    }) == BLOSC2_ERROR_SUCCESS
            })
            .expect("test needs an available user codec ID");
        assert_eq!(blosc2_compname_to_compcode(c_codec_name), Some(c_codec_id));
        assert_ne!(duplicate_c_name_id, c_codec_id);
        let (versioned_codec_id, versioned_codec_name) =
            register_blosc2_sequence_codec("compress-sequence-v7-", BLOSC_UDCODEC_FORMAT, 7);
        let versioned_c_codec = codecs::Blosc2Codec {
            compcode: versioned_codec_id,
            compname: versioned_codec_name,
            version: 7,
            ..c_codec
        };
        assert_eq!(
            codecs::blosc2_register_codec(&versioned_c_codec),
            BLOSC2_ERROR_SUCCESS
        );

        let codec_id = register_legacy_sequence_codec();
        assert!(codecs::register_codec(
            codec_id,
            sequence_codec_compress,
            sequence_codec_decompress
        )
        .is_ok());
        assert_eq!(
            codecs::register_codec(codec_id, failing_codec_compress, sequence_codec_decompress),
            Err("User-defined codec ID already registered")
        );
        let (named_codec_id, named_codec_name) =
            register_named_sequence_codec("compress-sequence-named-");
        assert_eq!(
            blosc2_compcode_to_compname(named_codec_id),
            Some(named_codec_name)
        );
        assert_eq!(
            blosc2_compname_to_compcode(named_codec_name),
            Some(named_codec_id)
        );
        let duplicate_named_codec_id = (161..=251)
            .find(|&candidate| {
                candidate != named_codec_id
                    && codecs::register_named_codec(
                        candidate,
                        named_codec_name,
                        sequence_codec_compress,
                        sequence_codec_decompress,
                    )
                    .is_ok()
            })
            .expect("test needs an available user codec ID");
        assert_eq!(
            blosc2_compname_to_compcode(named_codec_name),
            Some(named_codec_id)
        );
        assert_ne!(duplicate_named_codec_id, named_codec_id);

        let data: Vec<u8> = (0..200u8).collect();
        let cparams = CParams {
            compcode: codec_id,
            compcode_meta: 17,
            clevel: 5,
            typesize: 1,
            blocksize: 200,
            filters: [0; BLOSC2_MAX_FILTERS],
            ..Default::default()
        };

        let compressed = compress(&data, &cparams).unwrap();
        let header = ChunkHeader::read(&compressed).unwrap();
        assert_eq!(header.compcode(), codec_id);
        assert_eq!(header.compcode_meta, 17);
        assert!(chunk_compressor_library(&compressed).is_some());
        assert_eq!(decompress(&compressed).unwrap(), data);

        let versioned = compress(
            &data,
            &CParams {
                compcode: versioned_codec_id,
                compcode_meta: 19,
                clevel: 5,
                typesize: 1,
                blocksize: 200,
                filters: [0; BLOSC2_MAX_FILTERS],
                ..Default::default()
            },
        )
        .unwrap();
        let versioned_header = ChunkHeader::read(&versioned).unwrap();
        assert_eq!(versioned_header.compcode(), versioned_codec_id);
        assert_eq!(versioned_header.versionlz, 7);
        assert_eq!(decompress(&versioned).unwrap(), data);
    }

    #[test]
    fn test_global_plugin_codec_roundtrip_and_metadata() {
        const CODEC_ID: u8 = BLOSC2_GLOBAL_REGISTERED_CODECS_STOP - 3;

        assert_eq!(
            codecs::register_global_codec(
                BLOSC_ZSTD,
                sequence_codec_compress,
                sequence_codec_decompress
            ),
            Err("Global plugin codec IDs must be in 32..=159")
        );
        codecs::register_global_codec(CODEC_ID, sequence_codec_compress, sequence_codec_decompress)
            .unwrap();
        assert_eq!(
            codecs::register_global_codec(
                CODEC_ID,
                sequence_codec_compress,
                sequence_codec_decompress
            ),
            Err("Global plugin codec ID already registered")
        );
        assert_eq!(
            codecs::register_global_codec(
                CODEC_ID,
                sequence_codec_compress,
                failing_codec_decompress
            ),
            Err("Global plugin codec ID already registered")
        );
        assert_eq!(
            codecs::register_global_codec(
                BLOSC2_USER_DEFINED_CODECS_START,
                sequence_codec_compress,
                sequence_codec_decompress
            ),
            Err("Global plugin codec IDs must be in 32..=159")
        );
        codecs::register_named_global_codec(
            CODEC_ID + 1,
            "shared-global-codec-name",
            sequence_codec_compress,
            sequence_codec_decompress,
        )
        .unwrap();
        assert!(codecs::register_named_global_codec(
            CODEC_ID + 1,
            "shared-global-codec-name",
            sequence_codec_compress,
            failing_codec_decompress,
        )
        .is_ok());
        assert_eq!(
            codecs::register_named_global_codec(
                CODEC_ID + 1,
                "other-shared-global-codec-name",
                sequence_codec_compress,
                failing_codec_decompress,
            ),
            Err("Global plugin codec ID already registered")
        );
        codecs::register_named_global_codec(
            CODEC_ID + 2,
            "shared-global-codec-name",
            sequence_codec_compress,
            sequence_codec_decompress,
        )
        .unwrap();
        codecs::register_global_codec_with_metadata(
            CODEC_ID + 3,
            "metadata-global-codec",
            77,
            9,
            sequence_codec_compress,
            sequence_codec_decompress,
        )
        .unwrap();
        assert_eq!(
            blosc2_compname_to_compcode("metadata-global-codec"),
            Some(CODEC_ID + 3)
        );
        assert_eq!(
            blosc2_get_complib_info("metadata-global-codec"),
            Some((77, "metadata-global-codec", "unknown"))
        );

        let data: Vec<u8> = (0..200u8).collect();
        let cparams = CParams {
            compcode: CODEC_ID,
            compcode_meta: 23,
            clevel: 5,
            typesize: 1,
            blocksize: 200,
            filters: [0; BLOSC2_MAX_FILTERS],
            ..Default::default()
        };

        let compressed = compress(&data, &cparams).unwrap();
        let header = ChunkHeader::read(&compressed).unwrap();
        assert_eq!(header.compformat(), BLOSC_UDCODEC_FORMAT);
        assert_eq!(header.compcode(), CODEC_ID);
        assert_eq!(header.compcode_meta, 23);
        assert_eq!(decompress(&compressed).unwrap(), data);

        let metadata_compressed = compress(
            &data,
            &CParams {
                compcode: CODEC_ID + 3,
                compcode_meta: 24,
                clevel: 5,
                typesize: 1,
                blocksize: 200,
                filters: [0; BLOSC2_MAX_FILTERS],
                ..Default::default()
            },
        )
        .unwrap();
        let metadata_header = ChunkHeader::read(&metadata_compressed).unwrap();
        assert_eq!(metadata_header.compcode(), CODEC_ID + 3);
        assert_eq!(metadata_header.versionlz, 9);
        assert_eq!(decompress(&metadata_compressed).unwrap(), data);
    }

    #[test]
    fn test_user_defined_codec_negative_compress_returns_error() {
        let codec_id = 201;
        codecs::register_codec(codec_id, failing_codec_compress, failing_codec_decompress).unwrap();
        let cparams = CParams {
            compcode: codec_id,
            clevel: 5,
            typesize: 1,
            blocksize: 128,
            filters: [0; BLOSC2_MAX_FILTERS],
            ..Default::default()
        };
        let data: Vec<u8> = (0..128u8).collect();

        assert_eq!(compress(&data, &cparams), Err("Codec compression failed"));
    }

    #[test]
    fn test_user_codec_short_success_accepts_unwritten_compress_output_like_c() {
        let _guard = CALLBACK_ABI_LOCK.lock().unwrap();
        const CODEC_ID: u8 = 254;
        codecs::register_codec(
            CODEC_ID,
            unwritten_short_codec_compress,
            failing_codec_decompress,
        )
        .unwrap();
        let data: Vec<u8> = (0..128u8).map(|idx| idx.wrapping_mul(3)).collect();
        let compressed = compress(
            &data,
            &CParams {
                compcode: CODEC_ID,
                clevel: 5,
                typesize: 1,
                blocksize: data.len() as i32,
                filters: [0; BLOSC2_MAX_FILTERS],
                ..Default::default()
            },
        )
        .unwrap();
        let header = ChunkHeader::read(&compressed).unwrap();
        let block_start = i32::from_le_bytes(
            compressed[header.header_len()..header.header_len() + 4]
                .try_into()
                .unwrap(),
        ) as usize;
        assert_eq!(
            i32::from_le_bytes(compressed[block_start..block_start + 4].try_into().unwrap()),
            3
        );
    }

    #[test]
    fn test_user_codec_full_success_accepts_unwritten_decompress_output_like_c() {
        let _guard = CALLBACK_ABI_LOCK.lock().unwrap();
        const CODEC_ID: u8 = 252;
        codecs::register_codec(
            CODEC_ID,
            sequence_codec_compress,
            unwritten_full_codec_decompress,
        )
        .unwrap();
        let data: Vec<u8> = (0..128u8).collect();
        let compressed = compress(
            &data,
            &CParams {
                compcode: CODEC_ID,
                compcode_meta: 5,
                clevel: 5,
                typesize: 1,
                blocksize: data.len() as i32,
                filters: [0; BLOSC2_MAX_FILTERS],
                ..Default::default()
            },
        )
        .unwrap();

        assert_eq!(decompress(&compressed).unwrap().len(), data.len());
    }

    #[test]
    fn test_dictionary_falls_back_for_small_payload() {
        let data = b"small payload";
        let cparams = CParams {
            compcode: BLOSC_ZSTD,
            clevel: 5,
            typesize: 1,
            use_dict: true,
            ..Default::default()
        };

        let compressed = compress(data, &cparams).unwrap();
        let header = ChunkHeader::read(&compressed).unwrap();
        assert!(!header.use_dict());
        assert_eq!(decompress(&compressed).unwrap(), data);
    }

    #[test]
    fn test_zstd_dictionary_falls_back_when_c_sample_size_is_zero() {
        let data: Vec<u8> = (0..6000u32).map(|i| i as u8).collect();
        let cparams = CParams {
            compcode: BLOSC_ZSTD,
            clevel: 5,
            typesize: 1,
            blocksize: 1,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0; BLOSC2_MAX_FILTERS],
            use_dict: true,
            ..Default::default()
        };

        let compressed = compress(&data, &cparams).unwrap();
        let header = ChunkHeader::read(&compressed).unwrap();
        assert!(!header.use_dict());
        assert_eq!(decompress(&compressed).unwrap(), data);
    }

    #[test]
    fn test_zstd_low_diversity_dictionary_content_is_capped() {
        let data = vec![0u8; 200_000];
        let cparams = CParams {
            compcode: BLOSC_ZSTD,
            clevel: 5,
            typesize: 1,
            blocksize: 4096,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0; BLOSC2_MAX_FILTERS],
            use_dict: true,
            ..Default::default()
        };

        let compressed = compress(&data, &cparams).unwrap();
        let header = ChunkHeader::read(&compressed).unwrap();
        assert!(header.use_dict());
        let dict = embedded_codec_dictionary(&compressed, &header)
            .unwrap()
            .unwrap();
        let ddict = zstd_pure_rs::decompress::zstd_ddict::ZSTD_createDDict(dict).unwrap();
        let content = zstd_pure_rs::decompress::zstd_ddict::ZSTD_DDict_dictContent(&ddict);
        assert!(content.len() <= 512);
        assert_eq!(&dict[..4], &ZSTD_MAGIC_DICTIONARY.to_le_bytes());
        assert_eq!(decompress(&compressed).unwrap(), data);
    }

    #[test]
    fn test_zstd_high_diversity_dictionary_content_is_not_capped() {
        let data: Vec<u8> = (0..200_000u32)
            .flat_map(|i| {
                i.wrapping_mul(1_103_515_245)
                    .rotate_left(i % 17)
                    .to_le_bytes()
            })
            .collect();
        let cparams = CParams {
            compcode: BLOSC_ZSTD,
            clevel: 5,
            typesize: 1,
            blocksize: 4096,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0; BLOSC2_MAX_FILTERS],
            use_dict: true,
            ..Default::default()
        };

        let compressed = compress(&data, &cparams).unwrap();
        let header = ChunkHeader::read(&compressed).unwrap();
        assert!(header.use_dict());
        let dict = embedded_codec_dictionary(&compressed, &header)
            .unwrap()
            .unwrap();
        assert!(dict.len() > 512);
        assert!(dict.len() <= BLOSC2_MAXDICTSIZE);
        assert_eq!(&dict[..4], &ZSTD_MAGIC_DICTIONARY.to_le_bytes());
        assert_eq!(decompress(&compressed).unwrap(), data);
    }

    #[test]
    fn test_zstd_dictionary_training_honors_declared_sample_sizes() {
        let sample_sizes = [512usize; 8];
        let mut samples = Vec::new();
        for idx in 0..sample_sizes.len() {
            samples.extend(std::iter::repeat_n(0x11 + idx as u8, sample_sizes[idx]));
        }
        let trailer = vec![0xfe; 1024];
        samples.extend_from_slice(&trailer);

        let dict = train_zstd_dictionary(&samples, 200_000, &sample_sizes).unwrap();
        assert_eq!(&dict[..4], &ZSTD_MAGIC_DICTIONARY.to_le_bytes());
        let ddict = zstd_pure_rs::decompress::zstd_ddict::ZSTD_createDDict(&dict).unwrap();
        let content = zstd_pure_rs::decompress::zstd_ddict::ZSTD_DDict_dictContent(&ddict);
        assert!(!content.contains(&0xfe));
    }

    #[test]
    fn test_zstd_dictionary_uses_default_rep_start_values_like_c() {
        let samples: Vec<u8> = (0..4096u32)
            .flat_map(|i| i.wrapping_mul(2654435761).to_le_bytes())
            .collect();
        let dict = train_zstd_dictionary(&samples, 200_000, &[512; 32]).unwrap();
        let content = zstd_pure_rs::decompress::zstd_ddict::ZSTD_createDDict(&dict).unwrap();
        let content = zstd_pure_rs::decompress::zstd_ddict::ZSTD_DDict_dictContent(&content);
        let rep_start = dict.len() - content.len() - 12;
        assert_eq!(&dict[rep_start..rep_start + 4], &1u32.to_le_bytes());
        assert_eq!(&dict[rep_start + 4..rep_start + 8], &4u32.to_le_bytes());
        assert_eq!(&dict[rep_start + 8..rep_start + 12], &8u32.to_le_bytes());
    }

    #[test]
    fn test_zstd_finalize_shrinks_content_after_full_content_dict_id_like_c() {
        let content: Vec<u8> = (0..512u32)
            .map(|i| i.wrapping_mul(1_103_515_245).rotate_left(i % 11) as u8)
            .collect();
        let entropy_samples: Vec<u8> = (0..1024u32)
            .map(|i| i.wrapping_mul(2_654_435_761).rotate_right(i % 13) as u8)
            .collect();
        let sample_sizes = [128usize; 8];

        let dict =
            build_minimal_zstd_dict(&content, &entropy_samples, &sample_sizes, content.len())
                .unwrap();
        assert!(dict.len() <= content.len());

        let random_id = XXH64(&content, 0);
        let compliant_id = (random_id % ((1u64 << 31) - 32768)) + 32768;
        assert_eq!(&dict[4..8], &(compliant_id as u32).to_le_bytes());

        let ddict = zstd_pure_rs::decompress::zstd_ddict::ZSTD_createDDict(&dict).unwrap();
        let emitted_content = zstd_pure_rs::decompress::zstd_ddict::ZSTD_DDict_dictContent(&ddict);
        assert!(emitted_content.len() < content.len());
        assert_eq!(emitted_content, &content[..emitted_content.len()]);
    }

    #[test]
    fn test_zstd_finalize_uses_original_capacity_for_short_content_like_c() {
        let content: Vec<u8> = (0..512u32)
            .map(|i| i.wrapping_mul(747_796_405).rotate_left(i % 7) as u8)
            .collect();
        let entropy_samples: Vec<u8> = (0..4096u32)
            .flat_map(|i| i.wrapping_mul(2_654_435_761).to_le_bytes())
            .collect();
        let sample_sizes = [512usize; 32];

        let dict =
            build_minimal_zstd_dict(&content, &entropy_samples, &sample_sizes, 4096).unwrap();
        let ddict = zstd_pure_rs::decompress::zstd_ddict::ZSTD_createDDict(&dict).unwrap();
        let emitted_content = zstd_pure_rs::decompress::zstd_ddict::ZSTD_DDict_dictContent(&ddict);
        assert_eq!(emitted_content, &content[..]);
    }

    #[test]
    fn test_vlblocks_roundtrip() {
        let blocks: [&[u8]; 3] = [b"red\0", b"green-green\0", b"blue-blue-blue-blue\0"];
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            nthreads: 4,
            ..Default::default()
        };

        let compressed = vlcompress(&blocks, &cparams).unwrap();
        let header = ChunkHeader::read(&compressed).unwrap();
        assert!(header.vl_blocks());
        assert_eq!(header.version, BLOSC2_VERSION_FORMAT_VL_BLOCKS);
        assert_eq!(vlchunk_get_nblocks(&compressed).unwrap(), 3);
        assert_eq!(
            blosc2_vlchunk_get_nblocks_c(
                &compressed[..BLOSC_EXTENDED_HEADER_LENGTH],
                BLOSC_EXTENDED_HEADER_LENGTH as i32,
            ),
            (BLOSC2_ERROR_SUCCESS, 3)
        );

        let split = vldecompress(&compressed).unwrap();
        assert_eq!(
            split,
            blocks
                .iter()
                .map(|block| block.to_vec())
                .collect::<Vec<_>>()
        );
        assert_eq!(
            vldecompress_block(&compressed, 1).unwrap(),
            b"green-green\0"
        );
        assert_eq!(
            decompress(&compressed).unwrap(),
            b"red\0green-green\0blue-blue-blue-blue\0"
        );
        assert!(getitem(&compressed, 2, 16).is_err());
        let mut delta_cparams = cparams.clone();
        delta_cparams.filters[BLOSC2_MAX_FILTERS - 2] = BLOSC_DELTA;
        let delta_compressed = vlcompress(&blocks, &delta_cparams).unwrap();
        assert_eq!(
            vldecompress(&delta_compressed).unwrap(),
            blocks
                .iter()
                .map(|block| block.to_vec())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_vlblocks_typesize4_shuffle_and_bitshuffle_roundtrip() {
        let blocks: Vec<Vec<u8>> = [
            (0..64u32).collect::<Vec<_>>(),
            (1000..1137u32).collect::<Vec<_>>(),
            (9000..9131u32).map(|value| value ^ 0x55aa_3300).collect(),
        ]
        .into_iter()
        .map(|values| values.into_iter().flat_map(u32::to_le_bytes).collect())
        .collect();
        let block_refs: Vec<&[u8]> = blocks.iter().map(Vec::as_slice).collect();
        let expected_concat: Vec<u8> = blocks
            .iter()
            .flat_map(|block| block.iter())
            .copied()
            .collect();

        for filter in [BLOSC_SHUFFLE, BLOSC_BITSHUFFLE] {
            let cparams = CParams {
                compcode: BLOSC_LZ4,
                clevel: 5,
                typesize: 4,
                nthreads: 4,
                filters: [0, 0, 0, 0, 0, filter],
                ..Default::default()
            };

            let compressed = vlcompress(&block_refs, &cparams).unwrap();
            let header = ChunkHeader::read(&compressed).unwrap();
            assert!(header.vl_blocks());
            assert_eq!(header.typesize, 4);
            assert_eq!(vldecompress(&compressed).unwrap(), blocks);
            assert_eq!(decompress(&compressed).unwrap(), expected_concat);
            assert!(getitem(&compressed, 60, 24).is_err());
        }
    }

    #[test]
    fn test_vlblocks_allow_non_typesize_multiple_block_sizes() {
        let blocks: [&[u8]; 3] = [b"abcde", b"123456789", b"tail-bytes-not-aligned"];
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 4,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };

        let compressed = vlcompress(&blocks, &cparams).unwrap();
        let expected_concat: Vec<u8> = blocks
            .iter()
            .flat_map(|block| block.iter())
            .copied()
            .collect();
        assert_eq!(vldecompress(&compressed).unwrap(), blocks);
        assert_eq!(decompress(&compressed).unwrap(), expected_concat);
    }

    #[test]
    fn test_vlblocks_user_filter_receives_zero_block_offset() {
        const FILTER_ID: u8 = BLOSC2_USER_DEFINED_FILTERS_START + 40;
        let _ = crate::filters::register_filter(
            FILTER_ID,
            xor_user_filter_with_offset,
            xor_user_filter_with_offset,
        );
        let blocks: [&[u8]; 3] = [b"alpha", b"bravo-bravo", b"charlie-charlie-charlie"];
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            filters: [0, 0, 0, 0, 0, FILTER_ID],
            filters_meta: [0, 0, 0, 0, 0, 0x5a],
            ..Default::default()
        };

        let compressed = vlcompress(&blocks, &cparams).unwrap();
        assert_eq!(vldecompress(&compressed).unwrap(), blocks);
        assert_eq!(vldecompress_block(&compressed, 1).unwrap(), b"bravo-bravo");
    }

    #[test]
    fn test_vlblock_postfilter_offsets_use_max_block_stride() {
        VL_POSTFILTER_OFFSET_SUM.store(0, AtomicOrdering::SeqCst);
        let blocks: [&[u8]; 3] = [b"abc", b"1234567", b"tail!"];
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            filters: [0; BLOSC2_MAX_FILTERS],
            ..Default::default()
        };
        let dparams = DParams {
            postfilter: Some(record_vl_postfilter_offset),
            typesize: 1,
            ..Default::default()
        };

        let compressed = vlcompress(&blocks, &cparams).unwrap();
        assert_eq!(
            decompress_vl_blocks_with_dparams(&compressed, &dparams).unwrap(),
            blocks
        );
        assert_eq!(VL_POSTFILTER_OFFSET_SUM.load(AtomicOrdering::SeqCst), 21);
    }

    #[test]
    fn test_vl_decompression_paths_share_filter_postfilter_framing() {
        const FILTER_ID: u8 = BLOSC2_USER_DEFINED_FILTERS_START + 42;
        let _ = crate::filters::register_filter(
            FILTER_ID,
            xor_user_filter_with_offset,
            xor_user_filter_with_offset,
        );
        let blocks: Vec<Vec<u8>> = vec![
            b"alpha-variable-block".to_vec(),
            b"b".repeat(71),
            (0..97u8).map(|value| value.wrapping_mul(13)).collect(),
        ];
        let block_refs: Vec<&[u8]> = blocks.iter().map(Vec::as_slice).collect();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            filters: [0, 0, 0, 0, BLOSC_SHUFFLE, FILTER_ID],
            filters_meta: [0, 0, 0, 0, 0, 0x33],
            ..Default::default()
        };
        let dparams = DParams {
            postfilter: Some(xor_postfilter),
            typesize: 1,
            ..Default::default()
        };
        let compressed = vlcompress(&block_refs, &cparams).unwrap();

        let expected_blocks: Vec<Vec<u8>> = blocks
            .iter()
            .map(|block| block.iter().map(|byte| byte ^ 0x5a).collect())
            .collect();
        let expected_concat: Vec<u8> = expected_blocks.iter().flatten().copied().collect();

        assert_eq!(
            decompress_vl_blocks_with_dparams(&compressed, &dparams).unwrap(),
            expected_blocks
        );
        for (idx, expected) in expected_blocks.iter().enumerate() {
            assert_eq!(
                decompress_vl_block_with_dparams(&compressed, idx, &dparams).unwrap(),
                *expected
            );
        }
        assert_eq!(
            decompress_with_dparams(&compressed, &dparams).unwrap(),
            expected_concat
        );
        let mut dest = vec![0xa5; expected_concat.len()];
        assert_eq!(
            decompress_into_with_dparams(&compressed, &mut dest, &dparams).unwrap(),
            expected_concat.len()
        );
        assert_eq!(dest, expected_concat);

        let masked_dparams = DParams {
            postfilter: Some(xor_postfilter),
            block_maskout: Some(vec![false, true, false]),
            typesize: 1,
            ..Default::default()
        };
        let mut masked_blocks = expected_blocks.clone();
        masked_blocks[1] = vec![0; blocks[1].len()];
        assert_eq!(
            decompress_vl_blocks_with_dparams(&compressed, &masked_dparams).unwrap(),
            masked_blocks
        );
        let expected_masked_concat: Vec<u8> = masked_blocks.iter().flatten().copied().collect();
        assert_eq!(
            decompress_with_dparams(&compressed, &masked_dparams).unwrap(),
            expected_masked_concat
        );
    }

    #[test]
    fn test_vl_prefilter_failure_returns_error() {
        let blocks: [&[u8]; 3] = [b"alpha", b"bravo", b"charlie"];
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            filters: [0; BLOSC2_MAX_FILTERS],
            prefilter: Some(failing_prefilter),
            ..Default::default()
        };

        assert_eq!(
            vlcompress(&blocks, &cparams),
            Err("Execution of prefilter function failed")
        );
    }

    #[test]
    fn test_parallel_special_postfilter_receives_worker_tid() {
        POSTFILTER_TID_MASK.store(0, AtomicOrdering::SeqCst);
        let data = vec![0u8; 128 * 1024];
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            blocksize: 1024,
            filters: [0; BLOSC2_MAX_FILTERS],
            ..Default::default()
        };
        let dparams = DParams {
            nthreads: 4,
            typesize: 1,
            postfilter: Some(record_postfilter_tid),
            ..Default::default()
        };

        let compressed = compress(&data, &cparams).unwrap();
        assert_eq!(
            decompress_with_dparams(&compressed, &dparams).unwrap(),
            data
        );
        assert!(
            POSTFILTER_TID_MASK
                .load(AtomicOrdering::SeqCst)
                .count_ones()
                > 1
        );
    }

    #[test]
    fn test_parallel_vl_prefilter_receives_worker_tid() {
        PREFILTER_TID_MASK.store(0, AtomicOrdering::SeqCst);
        let blocks: Vec<Vec<u8>> = (0..64)
            .map(|idx| format!("payload-block-{idx:03}-with-padding").into_bytes())
            .collect();
        let block_refs: Vec<&[u8]> = blocks.iter().map(Vec::as_slice).collect();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            nthreads: 4,
            filters: [0; BLOSC2_MAX_FILTERS],
            prefilter: Some(record_prefilter_tid),
            ..Default::default()
        };

        let compressed = vlcompress(&block_refs, &cparams).unwrap();
        assert_eq!(vldecompress(&compressed).unwrap(), blocks);
        assert!(PREFILTER_TID_MASK.load(AtomicOrdering::SeqCst).count_ones() > 1);
    }

    #[test]
    fn test_zstd_dictionary_vlblocks_roundtrip() {
        let blocks: Vec<Vec<u8>> = (0..64)
            .map(|i| {
                format!(
                    "{{\"id\":\"ingredient-{i:03}\",\"vegan\":\"{}\",\"percent\":{},\"text\":\"INGREDIENT NUMBER {i:03}\"}}",
                    if i % 3 == 0 { "maybe" } else { "yes" },
                    i % 17
                )
                .into_bytes()
            })
            .collect();
        let block_refs: Vec<&[u8]> = blocks.iter().map(Vec::as_slice).collect();
        let cparams = CParams {
            compcode: BLOSC_ZSTD,
            clevel: 5,
            typesize: 1,
            nthreads: 4,
            use_dict: true,
            ..Default::default()
        };

        let compressed = vlcompress(&block_refs, &cparams).unwrap();
        let header = ChunkHeader::read(&compressed).unwrap();
        assert!(header.vl_blocks());
        assert!(header.use_dict());
        assert!(embedded_codec_dictionary(&compressed, &header)
            .unwrap()
            .is_some());

        assert_eq!(vldecompress(&compressed).unwrap(), blocks);
        assert_eq!(vldecompress_block(&compressed, 17).unwrap(), blocks[17]);
        let expected_concat: Vec<u8> = blocks.iter().flatten().copied().collect();
        assert_eq!(decompress(&compressed).unwrap(), expected_concat);
        assert!(getitem(&compressed, 10, 128).is_err());
    }

    #[test]
    fn test_lz4_dictionary_vlblocks_roundtrip() {
        let blocks: Vec<Vec<u8>> = (0..64)
            .map(|i| {
                format!(
                    "{{\"id\":\"ingredient-{i:03}\",\"vegan\":\"{}\",\"percent\":{},\"text\":\"INGREDIENT NUMBER {i:03}\"}}",
                    if i % 3 == 0 { "maybe" } else { "yes" },
                    i % 17
                )
                .into_bytes()
            })
            .collect();
        let block_refs: Vec<&[u8]> = blocks.iter().map(Vec::as_slice).collect();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            nthreads: 4,
            use_dict: true,
            ..Default::default()
        };

        let compressed = vlcompress(&block_refs, &cparams).unwrap();
        let header = ChunkHeader::read(&compressed).unwrap();
        assert!(header.vl_blocks());
        assert!(header.use_dict());
        assert!(embedded_codec_dictionary(&compressed, &header)
            .unwrap()
            .is_some());

        assert_eq!(vldecompress(&compressed).unwrap(), blocks);
        assert_eq!(vldecompress_block(&compressed, 17).unwrap(), blocks[17]);
        let expected_concat: Vec<u8> = blocks.iter().flatten().copied().collect();
        assert_eq!(decompress(&compressed).unwrap(), expected_concat);
        assert!(getitem(&compressed, 10, 128).is_err());
    }

    #[test]
    fn test_multithreaded_matches_singlethreaded() {
        let data: Vec<u8> = (0..50000u32).flat_map(|i| i.to_le_bytes()).collect();

        let cparams_1t = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 4,
            nthreads: 1,
            ..Default::default()
        };
        let cparams_4t = CParams {
            nthreads: 4,
            ..cparams_1t.clone()
        };

        let c1 = compress(&data, &cparams_1t).unwrap();
        let c4 = compress(&data, &cparams_4t).unwrap();

        // Compressed output should be identical (same algorithm)
        assert_eq!(
            c1, c4,
            "Multi-threaded compress should match single-threaded"
        );

        let d1 = decompress(&c1).unwrap();
        let d4 = decompress_with_threads(&c4, 4).unwrap();
        assert_eq!(d1, d4);
        assert_eq!(data, d1);
    }

    #[test]
    fn test_repeated_compress_decompress_cycles() {
        for iteration in 0..200u32 {
            let data: Vec<u8> = (0..4096u32)
                .flat_map(|i| i.wrapping_mul(31).wrapping_add(iteration).to_le_bytes())
                .collect();
            let cparams = CParams {
                compcode: match iteration % 4 {
                    0 => BLOSC_BLOSCLZ,
                    1 => BLOSC_LZ4,
                    2 => BLOSC_ZLIB,
                    _ => BLOSC_ZSTD,
                },
                clevel: (iteration % 10) as u8,
                typesize: 4,
                splitmode: match iteration % 3 {
                    0 => BLOSC_ALWAYS_SPLIT,
                    1 => BLOSC_NEVER_SPLIT,
                    _ => BLOSC_FORWARD_COMPAT_SPLIT,
                },
                filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
                ..Default::default()
            };

            let compressed = compress(&data, &cparams).unwrap();
            let restored = decompress(&compressed).unwrap();
            assert_eq!(restored, data, "cycle {iteration} failed");
        }
    }

    #[test]
    fn test_parallel_thread_safety_roundtrips() {
        let handles: Vec<_> = (0..8u32)
            .map(|thread_id| {
                std::thread::spawn(move || {
                    for iteration in 0..50u32 {
                        let data: Vec<u8> = (0..2048u32)
                            .flat_map(|i| {
                                i.wrapping_mul(17)
                                    .wrapping_add(thread_id * 1000 + iteration)
                                    .to_le_bytes()
                            })
                            .collect();
                        let cparams = CParams {
                            compcode: if iteration % 2 == 0 {
                                BLOSC_LZ4
                            } else {
                                BLOSC_ZSTD
                            },
                            clevel: 5,
                            typesize: 4,
                            nthreads: 4,
                            splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
                            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
                            ..Default::default()
                        };

                        let compressed = compress(&data, &cparams).unwrap();
                        let restored = decompress_with_threads(&compressed, 4).unwrap();
                        assert_eq!(restored, data);
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().expect("worker thread panicked");
        }
    }
}
