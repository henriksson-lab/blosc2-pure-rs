//! Compression codec dispatch for Blosc2 blocks.
//!
//! Provides a uniform interface over the bundled codecs — BloscLZ, LZ4,
//! LZ4HC, Zlib and Zstd — together with optional zstd/LZ4 dictionary
//! variants and registries for plugin/user-defined codecs (IDs >= 32).
//!
//! The actual BloscLZ implementation lives in the [`blosclz`] sub-module;
//! the other codecs are delegated to their respective Rust crates.

pub mod blosclz;

use crate::b2nd::B2ndMeta;
use crate::constants::*;
use std::cell::RefCell;
use std::collections::HashMap;
use std::ffi::{c_char, c_uint, c_void, CStr};
use std::sync::{OnceLock, RwLock};
#[cfg(feature = "plugin-zfp")]
use zfp_rs::{
    types::ZFP_MAX_PREC, ZfpBitStream, ZfpConfig, ZfpDimensionality, ZfpField, ZfpFieldMut,
    ZfpScalarType, ZfpStreamAlignment,
};
use zstd_pure_rs::common::error::{ErrorCode, ERROR};
use zstd_pure_rs::common::xxhash::XXH64_state_t;
use zstd_pure_rs::decompress::zstd_ddict::{ZSTD_DDict, ZSTD_DDict_dictContent, ZSTD_createDDict};
use zstd_pure_rs::decompress::zstd_decompress::{
    ZSTD_decompressFrame_withOpStart, ZSTD_loadDEntropy,
};
use zstd_pure_rs::decompress::zstd_decompress_block::{ZSTD_DCtx, ZSTD_decoder_entropy_rep};
use zstd_pure_rs::prelude::*;

/// Opaque C-ABI prefilter callback slot in `blosc2_cparams`.
pub type Blosc2PrefilterCb = Option<unsafe extern "C" fn(params: *mut c_void) -> i32>;

/// Opaque C-ABI postfilter callback slot in `blosc2_dparams`.
pub type Blosc2PostfilterCb = Option<unsafe extern "C" fn(params: *mut c_void) -> i32>;

unsafe extern "C" {
    #[link_name = "ZSTD_createCCtx"]
    fn c_zstd_create_cctx() -> *mut c_void;
    #[link_name = "ZSTD_freeCCtx"]
    fn c_zstd_free_cctx(cctx: *mut c_void) -> usize;
    #[link_name = "ZSTD_compressCCtx"]
    fn c_zstd_compress_cctx(
        cctx: *mut c_void,
        dst: *mut c_void,
        dst_capacity: usize,
        src: *const c_void,
        src_size: usize,
        compression_level: i32,
    ) -> usize;
    #[link_name = "ZSTD_createCDict"]
    fn c_zstd_create_cdict(
        dict_buffer: *const c_void,
        dict_size: usize,
        compression_level: i32,
    ) -> *mut c_void;
    #[link_name = "ZSTD_freeCDict"]
    fn c_zstd_free_cdict(cdict: *mut c_void) -> usize;
    #[link_name = "ZSTD_compress_usingCDict"]
    fn c_zstd_compress_using_cdict(
        cctx: *mut c_void,
        dst: *mut c_void,
        dst_capacity: usize,
        src: *const c_void,
        src_size: usize,
        cdict: *const c_void,
    ) -> usize;
    #[link_name = "ZSTD_isError"]
    fn c_zstd_is_error(code: usize) -> c_uint;
}

struct CZstdCCtx {
    ptr: *mut c_void,
}

impl CZstdCCtx {
    fn new() -> Option<Self> {
        let ptr = unsafe { c_zstd_create_cctx() };
        (!ptr.is_null()).then_some(Self { ptr })
    }
}

impl Drop for CZstdCCtx {
    fn drop(&mut self) {
        unsafe {
            c_zstd_free_cctx(self.ptr);
        }
    }
}

struct CZstdCDict {
    ptr: *mut c_void,
}

impl CZstdCDict {
    fn new(dict: &[u8], clevel: i32) -> Option<Self> {
        let ptr = unsafe { c_zstd_create_cdict(dict.as_ptr().cast(), dict.len(), clevel) };
        (!ptr.is_null()).then_some(Self { ptr })
    }
}

impl Drop for CZstdCDict {
    fn drop(&mut self) {
        unsafe {
            c_zstd_free_cdict(self.ptr);
        }
    }
}

/// Signature for a user-defined compression function registered via
/// [`register_codec`].
pub type CodecCompressFn = fn(clevel: u8, meta: u8, src: &[u8], dest: &mut [u8]) -> i32;
/// Signature for a user-defined decompression function registered via
/// [`register_codec`].
pub type CodecDecompressFn = fn(meta: u8, src: &[u8], dest: &mut [u8]) -> i32;

/// C-compatible callback return codes used by richer codec callbacks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum PluginCallbackStatus {
    Success = 0,
    Failure = 1,
}

/// Compression-parameter snapshot exposed to codec plugin callbacks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CodecCParamsContext {
    pub compcode: u8,
    pub compcode_meta: u8,
    pub clevel: u8,
    pub use_dict: i32,
    pub typesize: i32,
    pub blocksize: i32,
    pub splitmode: i32,
    pub filters: [u8; BLOSC2_MAX_FILTERS],
    pub filters_meta: [u8; BLOSC2_MAX_FILTERS],
    pub nthreads: i16,
    pub nchunk: i64,
    pub user_data: usize,
    pub instr_codec: bool,
    pub codec_params: usize,
}

/// Decompression-parameter snapshot exposed to codec plugin callbacks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CodecDParamsContext {
    pub nthreads: i16,
    pub typesize: i32,
    pub nchunk: i64,
    pub user_data: usize,
}

/// Per-block context exposed to C-compatible codec plugin callbacks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CodecChunkContext {
    pub schunk: usize,
    pub nchunk: i64,
    pub nblock: i32,
    pub chunk_source: usize,
    pub block_offset: usize,
    pub blocksize: usize,
    pub bsize: usize,
}

/// Rich codec callback parameters, modeled after C-Blosc2 plugin callbacks.
#[derive(Debug, Clone, Copy)]
pub struct CodecCallbackContext<'a> {
    pub compcode: u8,
    pub complib: Option<u8>,
    pub meta: u8,
    pub clevel: u8,
    pub cparams: Option<&'a CodecCParamsContext>,
    pub dparams: Option<&'a CodecDParamsContext>,
    pub chunk: CodecChunkContext,
    pub b2nd_metalayer: Option<&'a [u8]>,
    pub user_data: usize,
}

/// C-ABI compression parameters passed to raw `blosc2_codec` callbacks.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct Blosc2CParams {
    pub compcode: u8,
    pub compcode_meta: u8,
    pub clevel: u8,
    pub use_dict: i32,
    pub typesize: i32,
    pub nthreads: i16,
    pub blocksize: i32,
    pub splitmode: i32,
    pub schunk: *mut c_void,
    pub filters: [u8; BLOSC2_MAX_FILTERS],
    pub filters_meta: [u8; BLOSC2_MAX_FILTERS],
    pub prefilter: Blosc2PrefilterCb,
    pub preparams: *mut c_void,
    pub tuner_params: *mut c_void,
    pub tuner_id: i32,
    pub instr_codec: bool,
    pub codec_params: *mut c_void,
    pub filter_params: [*mut c_void; BLOSC2_MAX_FILTERS],
}

impl Blosc2CParams {
    fn from_context(ctx: &CodecCParamsContext, schunk: usize) -> Self {
        Self {
            compcode: ctx.compcode,
            compcode_meta: ctx.compcode_meta,
            clevel: ctx.clevel,
            use_dict: ctx.use_dict,
            typesize: ctx.typesize,
            nthreads: ctx.nthreads,
            blocksize: ctx.blocksize,
            splitmode: ctx.splitmode,
            schunk: schunk as *mut c_void,
            filters: ctx.filters,
            filters_meta: ctx.filters_meta,
            prefilter: None,
            preparams: std::ptr::null_mut(),
            tuner_params: std::ptr::null_mut(),
            tuner_id: 0,
            instr_codec: ctx.instr_codec,
            codec_params: ctx.codec_params as *mut c_void,
            filter_params: [std::ptr::null_mut(); BLOSC2_MAX_FILTERS],
        }
    }

    fn from_pipeline(ctx: &CodecCallbackContext<'_>) -> Self {
        ctx.cparams.map_or_else(
            || Self {
                compcode: ctx.compcode,
                compcode_meta: ctx.meta,
                clevel: ctx.clevel,
                use_dict: 0,
                typesize: 8,
                nthreads: 1,
                blocksize: 0,
                splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
                schunk: ctx.chunk.schunk as *mut c_void,
                filters: [
                    BLOSC_NOFILTER,
                    BLOSC_NOFILTER,
                    BLOSC_NOFILTER,
                    BLOSC_NOFILTER,
                    BLOSC_NOFILTER,
                    BLOSC_SHUFFLE,
                ],
                filters_meta: [0; BLOSC2_MAX_FILTERS],
                prefilter: None,
                preparams: std::ptr::null_mut(),
                tuner_params: std::ptr::null_mut(),
                tuner_id: 0,
                instr_codec: false,
                codec_params: std::ptr::null_mut(),
                filter_params: [std::ptr::null_mut(); BLOSC2_MAX_FILTERS],
            },
            |cparams| Self::from_context(cparams, ctx.chunk.schunk),
        )
    }
}

/// C-ABI decompression parameters passed to raw `blosc2_codec` callbacks.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct Blosc2DParams {
    pub nthreads: i16,
    pub schunk: *mut c_void,
    pub postfilter: Blosc2PostfilterCb,
    pub postparams: *mut c_void,
    pub typesize: i32,
}

impl Blosc2DParams {
    fn from_context(ctx: &CodecDParamsContext, schunk: usize) -> Self {
        Self {
            nthreads: ctx.nthreads,
            schunk: schunk as *mut c_void,
            postfilter: None,
            postparams: std::ptr::null_mut(),
            typesize: ctx.typesize,
        }
    }

    fn from_pipeline(ctx: &CodecCallbackContext<'_>) -> Self {
        ctx.dparams.map_or_else(
            || Self {
                nthreads: 1,
                schunk: ctx.chunk.schunk as *mut c_void,
                postfilter: None,
                postparams: std::ptr::null_mut(),
                typesize: 8,
            },
            |dparams| Self::from_context(dparams, ctx.chunk.schunk),
        )
    }
}

/// Rich compression callback signature for C-compatible codecs.
pub type ContextCodecCompressFn =
    for<'a> fn(&mut CodecCallbackContext<'a>, src: &[u8], dest: &mut [u8]) -> i32;
/// Rich decompression callback signature for C-compatible codecs.
pub type ContextCodecDecompressFn =
    for<'a> fn(&mut CodecCallbackContext<'a>, src: &[u8], dest: &mut [u8]) -> i32;

/// Raw C-ABI encoder callback signature for a `blosc2_codec`.
pub type Blosc2CodecEncoderCb = unsafe extern "C" fn(
    input: *const u8,
    input_len: i32,
    output: *mut u8,
    output_len: i32,
    meta: u8,
    cparams: *mut Blosc2CParams,
    chunk: *const c_void,
) -> i32;

/// Raw C-ABI decoder callback signature for a `blosc2_codec`.
pub type Blosc2CodecDecoderCb = unsafe extern "C" fn(
    input: *const u8,
    input_len: i32,
    output: *mut u8,
    output_len: i32,
    meta: u8,
    dparams: *mut Blosc2DParams,
    chunk: *const c_void,
) -> i32;

/// Rust-shaped user codec descriptor for ergonomic source-level registration.
#[derive(Clone, Copy)]
pub struct Blosc2Codec {
    pub compcode: u8,
    pub compname: &'static str,
    pub complib: u8,
    pub version: u8,
    pub encoder: CodecCompressFn,
    pub decoder: CodecDecompressFn,
}

/// Raw C-shaped `blosc2_codec` descriptor.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct Blosc2CodecAbi {
    pub compcode: u8,
    pub compname: *const c_char,
    pub complib: u8,
    pub version: u8,
    pub encoder: Option<Blosc2CodecEncoderCb>,
    pub decoder: Option<Blosc2CodecDecoderCb>,
}

#[derive(Clone, Copy)]
struct UserCodec {
    name: Option<&'static str>,
    complib: Option<u8>,
    version: Option<u8>,
    compress: UserCodecCompress,
    decompress: UserCodecDecompress,
}

#[derive(Clone, Copy)]
enum UserCodecCompress {
    Legacy(CodecCompressFn),
    Context(ContextCodecCompressFn),
    CAbi(Option<Blosc2CodecEncoderCb>),
}

impl UserCodecCompress {
    fn same_callback(self, other: Self) -> bool {
        match (self, other) {
            (Self::Legacy(a), Self::Legacy(b)) => a as usize == b as usize,
            (Self::Context(a), Self::Context(b)) => a as usize == b as usize,
            (Self::CAbi(a), Self::CAbi(b)) => {
                a.map(|callback| callback as usize) == b.map(|callback| callback as usize)
            }
            _ => false,
        }
    }

    fn run(
        self,
        ctx: &mut CodecCallbackContext<'_>,
        clevel: u8,
        meta: u8,
        src: &[u8],
        dest: &mut [u8],
    ) -> i32 {
        match self {
            Self::Legacy(callback) => callback(clevel, meta, src, dest),
            Self::Context(callback) => callback(ctx, src, dest),
            Self::CAbi(Some(callback)) => {
                let Ok(input_len) = i32::try_from(src.len()) else {
                    return BLOSC2_ERROR_2GB_LIMIT;
                };
                let Ok(output_len) = i32::try_from(dest.len()) else {
                    return BLOSC2_ERROR_2GB_LIMIT;
                };
                let mut cparams = Blosc2CParams::from_pipeline(ctx);
                let chunk = codec_chunk_arg(src.as_ptr(), ctx);
                unsafe {
                    callback(
                        src.as_ptr(),
                        input_len,
                        dest.as_mut_ptr(),
                        output_len,
                        meta,
                        &mut cparams,
                        chunk,
                    )
                }
            }
            Self::CAbi(None) => missing_dynamic_codec_callback(),
        }
    }
}

#[derive(Clone, Copy)]
enum UserCodecDecompress {
    Legacy(CodecDecompressFn),
    Context(ContextCodecDecompressFn),
    CAbi(Option<Blosc2CodecDecoderCb>),
}

impl UserCodecDecompress {
    fn same_callback(self, other: Self) -> bool {
        match (self, other) {
            (Self::Legacy(a), Self::Legacy(b)) => a as usize == b as usize,
            (Self::Context(a), Self::Context(b)) => a as usize == b as usize,
            (Self::CAbi(a), Self::CAbi(b)) => {
                a.map(|callback| callback as usize) == b.map(|callback| callback as usize)
            }
            _ => false,
        }
    }

    fn run(self, ctx: &mut CodecCallbackContext<'_>, meta: u8, src: &[u8], dest: &mut [u8]) -> i32 {
        match self {
            Self::Legacy(callback) => callback(meta, src, dest),
            Self::Context(callback) => callback(ctx, src, dest),
            Self::CAbi(Some(callback)) => {
                let Ok(input_len) = i32::try_from(src.len()) else {
                    return BLOSC2_ERROR_DATA;
                };
                let Ok(output_len) = i32::try_from(dest.len()) else {
                    return BLOSC2_ERROR_DATA;
                };
                let mut dparams = Blosc2DParams::from_pipeline(ctx);
                let chunk = codec_chunk_arg(src.as_ptr(), ctx);
                unsafe {
                    callback(
                        src.as_ptr(),
                        input_len,
                        dest.as_mut_ptr(),
                        output_len,
                        meta,
                        &mut dparams,
                        chunk,
                    )
                }
            }
            Self::CAbi(None) => missing_dynamic_codec_callback(),
        }
    }
}

fn missing_dynamic_codec_callback() -> i32 {
    // C-Blosc2 may dynamically resolve null codec callbacks. This crate has no
    // codec plugin loader, so preserve null callbacks and fail at invocation.
    BLOSC2_ERROR_CODEC_SUPPORT
}

fn codec_chunk_arg(block_ptr: *const u8, ctx: &CodecCallbackContext<'_>) -> *const c_void {
    if ctx.chunk.chunk_source != 0 {
        ctx.chunk.chunk_source as *const c_void
    } else {
        (block_ptr as usize)
            .checked_sub(ctx.chunk.block_offset)
            .map_or(std::ptr::null(), |addr| addr as *const c_void)
    }
}

#[derive(Clone, Copy)]
struct KnownGlobalCodec {
    compcode: u8,
    name: &'static str,
    version: u8,
}

const KNOWN_GLOBAL_CODECS: &[KnownGlobalCodec] = &[
    KnownGlobalCodec {
        compcode: BLOSC_CODEC_NDLZ,
        name: "ndlz",
        version: 1,
    },
    KnownGlobalCodec {
        compcode: BLOSC_CODEC_ZFP_FIXED_ACCURACY,
        name: "zfp_acc",
        version: 1,
    },
    KnownGlobalCodec {
        compcode: BLOSC_CODEC_ZFP_FIXED_PRECISION,
        name: "zfp_prec",
        version: 1,
    },
    KnownGlobalCodec {
        compcode: BLOSC_CODEC_ZFP_FIXED_RATE,
        name: "zfp_rate",
        version: 1,
    },
    KnownGlobalCodec {
        compcode: BLOSC_CODEC_OPENHTJ2K,
        name: "openhtj2k",
        version: 1,
    },
    KnownGlobalCodec {
        compcode: BLOSC_CODEC_GROK,
        name: "grok",
        version: 1,
    },
    KnownGlobalCodec {
        compcode: BLOSC_CODEC_OPENZL,
        name: "openzl",
        version: 1,
    },
];

impl UserCodec {
    fn same_callbacks(self, other: Self) -> bool {
        self.name == other.name
            && self.complib == other.complib
            && self.version == other.version
            && self.compress.same_callback(other.compress)
            && self.decompress.same_callback(other.decompress)
    }
}

fn known_global_codec_by_code(compcode: u8) -> Option<KnownGlobalCodec> {
    KNOWN_GLOBAL_CODECS
        .iter()
        .copied()
        .find(|codec| codec.compcode == compcode)
}

fn known_global_codec_by_name(name: &str) -> Option<KnownGlobalCodec> {
    KNOWN_GLOBAL_CODECS
        .iter()
        .copied()
        .find(|codec| codec.name == name)
}

fn builtin_complib_info(complib: u8) -> Option<(&'static str, &'static str)> {
    match complib {
        BLOSC_BLOSCLZ_LIB => Some((BLOSC_BLOSCLZ_LIBNAME, "2.5.3")),
        BLOSC_LZ4_LIB => Some((BLOSC_LZ4_LIBNAME, "1.10.0")),
        BLOSC_ZLIB_LIB => Some((BLOSC_ZLIB_LIBNAME, "2.0.7")),
        BLOSC_ZSTD_LIB => Some((BLOSC_ZSTD_LIBNAME, "1.5.7")),
        _ => None,
    }
}

/// Returns `true` for C-Blosc2 global plugin codec IDs known by this crate.
///
/// These entries provide C-compatible IDs and metadata. They do not imply that
/// the codec implementation has been ported.
pub fn is_known_global_codec(compcode: u8) -> bool {
    known_global_codec_by_code(compcode).is_some()
}

/// Returns `true` for known C-Blosc2 ZFP plugin codec modes.
pub fn is_known_zfp_codec(compcode: u8) -> bool {
    matches!(
        compcode,
        BLOSC_CODEC_ZFP_FIXED_ACCURACY
            | BLOSC_CODEC_ZFP_FIXED_PRECISION
            | BLOSC_CODEC_ZFP_FIXED_RATE
    )
}

pub fn is_static_global_codec_enabled(compcode: u8) -> bool {
    (cfg!(feature = "plugin-ndlz") && compcode == BLOSC_CODEC_NDLZ)
        || (cfg!(feature = "plugin-zfp") && is_known_zfp_codec(compcode))
}

static USER_CODECS: OnceLock<RwLock<HashMap<u8, UserCodec>>> = OnceLock::new();
static USER_CODEC_ORDER: OnceLock<RwLock<Vec<u8>>> = OnceLock::new();

thread_local! {
    static C_ZSTD_CCTX: RefCell<Option<CZstdCCtx>> = const { RefCell::new(None) };
    static C_ZSTD_DICT_CCTX: RefCell<Option<CZstdCCtx>> = const { RefCell::new(None) };
    static ZSTD_DICT_DCTX: RefCell<Box<ZSTD_DCtx>> = RefCell::new(ZSTD_createDCtx());
    static ZSTD_DCTX: RefCell<(Box<ZSTD_DCtx>, ZSTD_decoder_entropy_rep, XXH64_state_t)> =
        RefCell::new((
            ZSTD_createDCtx(),
            ZSTD_decoder_entropy_rep::default(),
            XXH64_state_t::default(),
        ));
}

/// Lazily-initialized registry of user-defined codecs, keyed by codec ID.
fn user_codecs() -> &'static RwLock<HashMap<u8, UserCodec>> {
    USER_CODECS.get_or_init(|| RwLock::new(HashMap::new()))
}

fn user_codec_order() -> &'static RwLock<Vec<u8>> {
    USER_CODEC_ORDER.get_or_init(|| RwLock::new(Vec::new()))
}

fn remember_codec_order(compcode: u8) -> Result<(), &'static str> {
    let mut order = user_codec_order()
        .write()
        .map_err(|_| "Codec registry poisoned")?;
    if !order.contains(&compcode) {
        order.push(compcode);
    }
    Ok(())
}

fn known_global_registration_status(
    compcode: u8,
    name: Option<&str>,
    duplicate_error: &'static str,
) -> Result<bool, &'static str> {
    let Some(known) = known_global_codec_by_code(compcode) else {
        return Ok(false);
    };
    if name != Some(known.name) {
        return Err(duplicate_error);
    }
    Ok(true)
}

/// Register a user-defined codec under `compcode`.
///
/// C-Blosc2 reserves IDs 32..=159 for global plugin codecs and 160..=255 for
/// user-defined codecs. Lower IDs are reserved for built-in codecs. Duplicate
/// IDs are rejected so existing chunks do not change behavior after accidental
/// callback replacement.
pub fn register_codec(
    compcode: u8,
    compress: CodecCompressFn,
    decompress: CodecDecompressFn,
) -> Result<(), &'static str> {
    register_codec_impl(
        compcode,
        None,
        compress,
        decompress,
        BLOSC2_USER_DEFINED_CODECS_START..=u8::MAX,
    )
}

/// Register a named user-defined codec under `compcode`.
///
/// The name is used by C-Blosc2-style compressor name/code lookup helpers.
pub fn register_named_codec(
    compcode: u8,
    name: &'static str,
    compress: CodecCompressFn,
    decompress: CodecDecompressFn,
) -> Result<(), &'static str> {
    if name.is_empty() {
        return Err("User-defined codec name cannot be empty");
    }
    register_codec_impl(
        compcode,
        Some(name),
        compress,
        decompress,
        BLOSC2_USER_DEFINED_CODECS_START..=u8::MAX,
    )
}

/// C-name registration wrapper for [`Blosc2Codec`].
pub fn blosc2_register_codec(codec: &Blosc2Codec) -> i32 {
    blosc2_register_codec_c(Some(codec))
}

/// C-style nullable registration wrapper for [`Blosc2Codec`].
pub fn blosc2_register_codec_c(codec: Option<&Blosc2Codec>) -> i32 {
    let Some(codec) = codec else {
        return BLOSC2_ERROR_INVALID_PARAM;
    };
    match register_blosc2_codec_impl(codec) {
        Ok(()) => BLOSC2_ERROR_SUCCESS,
        Err("Codec IDs must be >= 32")
        | Err("User-defined codec IDs must be >= 160")
        | Err("Codec ID outside allowed range")
        | Err("User-defined codec ID already registered")
        | Err("User-defined codec name already registered") => BLOSC2_ERROR_CODEC_PARAM,
        Err("User-defined codec name cannot be empty") => BLOSC2_ERROR_INVALID_PARAM,
        Err(_) => BLOSC2_ERROR_FAILURE,
    }
}

/// C-name registration wrapper for raw `blosc2_codec` descriptors.
///
/// This accepts the C callback shape:
/// `(input, input_len, output, output_len, meta, cparams/dparams, chunk)`.
pub fn blosc2_register_codec_abi(codec: *const Blosc2CodecAbi) -> i32 {
    if codec.is_null() {
        return BLOSC2_ERROR_INVALID_PARAM;
    }
    let codec = unsafe { &*codec };
    match register_blosc2_codec_abi_impl(codec) {
        Ok(()) => BLOSC2_ERROR_SUCCESS,
        Err("Codec IDs must be >= 32")
        | Err("User-defined codec IDs must be >= 160")
        | Err("Codec ID outside allowed range")
        | Err("User-defined codec ID already registered")
        | Err("User-defined codec name already registered") => BLOSC2_ERROR_CODEC_PARAM,
        Err("User-defined codec name cannot be empty") => BLOSC2_ERROR_INVALID_PARAM,
        Err(_) => BLOSC2_ERROR_FAILURE,
    }
}

fn register_blosc2_codec_impl(codec: &Blosc2Codec) -> Result<(), &'static str> {
    if codec.compcode < BLOSC2_USER_DEFINED_CODECS_START {
        return Err("User-defined codec IDs must be >= 160");
    }
    let mut codecs = user_codecs()
        .write()
        .map_err(|_| "Codec registry poisoned")?;

    let registered = UserCodec {
        name: Some(codec.compname),
        complib: Some(codec.complib),
        version: Some(codec.version),
        compress: UserCodecCompress::Legacy(codec.encoder),
        decompress: UserCodecDecompress::Legacy(codec.decoder),
    };
    if let Some(existing) = codecs.get(&codec.compcode) {
        if existing.name == registered.name {
            return Ok(());
        }
        return Err("User-defined codec ID already registered");
    }
    codecs.insert(codec.compcode, registered);
    drop(codecs);
    remember_codec_order(codec.compcode)?;
    Ok(())
}

fn blosc2_codec_abi_name(name: *const c_char) -> Result<&'static str, &'static str> {
    if name.is_null() {
        return Err("User-defined codec name cannot be empty");
    }
    let c_name = unsafe { CStr::from_ptr(name) };
    let name = match c_name.to_str() {
        Ok(name) => name.to_owned(),
        Err(_) => c_name
            .to_bytes()
            .iter()
            .map(|&byte| char::from(byte))
            .collect(),
    };
    Ok(Box::leak(name.into_boxed_str()))
}

fn register_blosc2_codec_abi_impl(codec: &Blosc2CodecAbi) -> Result<(), &'static str> {
    if codec.compcode < BLOSC2_USER_DEFINED_CODECS_START {
        return Err("User-defined codec IDs must be >= 160");
    }
    let name = blosc2_codec_abi_name(codec.compname)?;
    let mut codecs = user_codecs()
        .write()
        .map_err(|_| "Codec registry poisoned")?;
    let registered = UserCodec {
        name: Some(name),
        complib: Some(codec.complib),
        version: Some(codec.version),
        compress: UserCodecCompress::CAbi(codec.encoder),
        decompress: UserCodecDecompress::CAbi(codec.decoder),
    };
    if let Some(existing) = codecs.get(&codec.compcode) {
        if existing.name == Some(name) {
            return Ok(());
        }
        return Err("User-defined codec ID already registered");
    }
    codecs.insert(codec.compcode, registered);
    drop(codecs);
    remember_codec_order(codec.compcode)?;
    Ok(())
}

fn register_codec_impl(
    compcode: u8,
    name: Option<&'static str>,
    compress: CodecCompressFn,
    decompress: CodecDecompressFn,
    allowed: std::ops::RangeInclusive<u8>,
) -> Result<(), &'static str> {
    if compcode < BLOSC2_USER_DEFINED_CODECS_START {
        return Err("User-defined codec IDs must be >= 160");
    }
    if !allowed.contains(&compcode) {
        return Err("Codec ID outside allowed range");
    }
    let mut codecs = user_codecs()
        .write()
        .map_err(|_| "Codec registry poisoned")?;
    let codec = UserCodec {
        name,
        complib: None,
        version: None,
        compress: UserCodecCompress::Legacy(compress),
        decompress: UserCodecDecompress::Legacy(decompress),
    };
    if let Some(existing) = codecs.get(&compcode) {
        if name.is_some() && existing.name == name {
            return Ok(());
        }
        if name.is_some() && existing.name != name {
            return Err("User-defined codec ID already registered");
        }
        return if existing.same_callbacks(codec) {
            Ok(())
        } else {
            Err("User-defined codec ID already registered")
        };
    }
    codecs.insert(compcode, codec);
    drop(codecs);
    remember_codec_order(compcode)?;
    Ok(())
}

/// Register a global plugin codec under `compcode`.
///
/// This mirrors C-Blosc2's internal plugin registration path: IDs 32..=159 are
/// accepted for globally registered plugins, while user-defined IDs still use
/// [`register_codec`]. Duplicate unnamed IDs are rejected because C-Blosc2's
/// idempotent re-registration check is based on the codec name.
pub fn register_global_codec(
    compcode: u8,
    compress: CodecCompressFn,
    decompress: CodecDecompressFn,
) -> Result<(), &'static str> {
    register_global_codec_impl(compcode, None, None, None, compress, decompress)
}

/// Register a named global plugin codec under `compcode`.
pub fn register_named_global_codec(
    compcode: u8,
    name: &'static str,
    compress: CodecCompressFn,
    decompress: CodecDecompressFn,
) -> Result<(), &'static str> {
    if name.is_empty() {
        return Err("Global plugin codec name cannot be empty");
    }
    register_global_codec_impl(compcode, Some(name), None, None, compress, decompress)
}

/// Register a named global plugin codec with C-style metadata.
///
/// `complib` and `version` are used by compressor name/library lookups and by
/// chunk headers, matching C's globally registered codec descriptors.
pub fn register_global_codec_with_metadata(
    compcode: u8,
    name: &'static str,
    complib: u8,
    version: u8,
    compress: CodecCompressFn,
    decompress: CodecDecompressFn,
) -> Result<(), &'static str> {
    if name.is_empty() {
        return Err("Global plugin codec name cannot be empty");
    }
    register_global_codec_impl(
        compcode,
        Some(name),
        Some(complib),
        Some(version),
        compress,
        decompress,
    )
}

/// Register a codec through the C-Blosc2 private registration range.
///
/// This mirrors C's private `register_codec_private` path, which accepts any
/// non-built-in codec ID (`32..=255`), including the user-defined range.
pub fn register_private_codec(
    compcode: u8,
    compress: CodecCompressFn,
    decompress: CodecDecompressFn,
) -> Result<(), &'static str> {
    register_private_codec_impl(compcode, None, None, None, compress, decompress)
}

/// Register a named codec through the C-Blosc2 private registration range.
pub fn register_named_private_codec(
    compcode: u8,
    name: &'static str,
    compress: CodecCompressFn,
    decompress: CodecDecompressFn,
) -> Result<(), &'static str> {
    if name.is_empty() {
        return Err("Private codec name cannot be empty");
    }
    register_private_codec_impl(compcode, Some(name), None, None, compress, decompress)
}

/// Register a named private codec with C-style metadata.
pub fn register_private_codec_with_metadata(
    compcode: u8,
    name: &'static str,
    complib: u8,
    version: u8,
    compress: CodecCompressFn,
    decompress: CodecDecompressFn,
) -> Result<(), &'static str> {
    if name.is_empty() {
        return Err("Private codec name cannot be empty");
    }
    register_private_codec_impl(
        compcode,
        Some(name),
        Some(complib),
        Some(version),
        compress,
        decompress,
    )
}

/// Register a user-defined codec with C-compatible contextual callbacks.
pub fn register_context_codec(
    compcode: u8,
    compress: ContextCodecCompressFn,
    decompress: ContextCodecDecompressFn,
) -> Result<(), &'static str> {
    register_context_codec_impl(
        compcode,
        None,
        None,
        None,
        compress,
        decompress,
        BLOSC2_USER_DEFINED_CODECS_START..=u8::MAX,
        "User-defined codec IDs must be >= 160",
        "User-defined codec ID already registered",
    )
}

/// Register a global plugin codec with C-compatible contextual callbacks.
pub fn register_global_context_codec(
    compcode: u8,
    compress: ContextCodecCompressFn,
    decompress: ContextCodecDecompressFn,
) -> Result<(), &'static str> {
    register_context_codec_impl(
        compcode,
        None,
        None,
        None,
        compress,
        decompress,
        BLOSC2_GLOBAL_REGISTERED_CODECS_START..=BLOSC2_GLOBAL_REGISTERED_CODECS_STOP,
        "Global plugin codec IDs must be in 32..=159",
        "Global plugin codec ID already registered",
    )
}

/// Register a named global plugin codec with C-style metadata and contextual callbacks.
pub fn register_global_context_codec_with_metadata(
    compcode: u8,
    name: &'static str,
    complib: u8,
    version: u8,
    compress: ContextCodecCompressFn,
    decompress: ContextCodecDecompressFn,
) -> Result<(), &'static str> {
    if name.is_empty() {
        return Err("Global plugin codec name cannot be empty");
    }
    register_context_codec_impl(
        compcode,
        Some(name),
        Some(complib),
        Some(version),
        compress,
        decompress,
        BLOSC2_GLOBAL_REGISTERED_CODECS_START..=BLOSC2_GLOBAL_REGISTERED_CODECS_STOP,
        "Global plugin codec IDs must be in 32..=159",
        "Global plugin codec ID already registered",
    )
}

fn register_global_codec_impl(
    compcode: u8,
    name: Option<&'static str>,
    complib: Option<u8>,
    version: Option<u8>,
    compress: CodecCompressFn,
    decompress: CodecDecompressFn,
) -> Result<(), &'static str> {
    if !(BLOSC2_GLOBAL_REGISTERED_CODECS_START..=BLOSC2_GLOBAL_REGISTERED_CODECS_STOP)
        .contains(&compcode)
    {
        return Err("Global plugin codec IDs must be in 32..=159");
    }
    if known_global_registration_status(
        compcode,
        name,
        "Global plugin codec ID already registered",
    )? {
        return Ok(());
    }
    let mut codecs = user_codecs()
        .write()
        .map_err(|_| "Codec registry poisoned")?;
    let codec = UserCodec {
        name,
        complib,
        version,
        compress: UserCodecCompress::Legacy(compress),
        decompress: UserCodecDecompress::Legacy(decompress),
    };
    if let Some(existing) = codecs.get(&compcode) {
        if name.is_some() && existing.name == name {
            return Ok(());
        }
        return Err("Global plugin codec ID already registered");
    }
    codecs.insert(compcode, codec);
    drop(codecs);
    remember_codec_order(compcode)?;
    Ok(())
}

fn register_private_codec_impl(
    compcode: u8,
    name: Option<&'static str>,
    complib: Option<u8>,
    version: Option<u8>,
    compress: CodecCompressFn,
    decompress: CodecDecompressFn,
) -> Result<(), &'static str> {
    if compcode < BLOSC2_GLOBAL_REGISTERED_CODECS_START {
        return Err("Private codec IDs must be >= 32");
    }
    if known_global_registration_status(compcode, name, "Private codec ID already registered")? {
        return Ok(());
    }
    let mut codecs = user_codecs()
        .write()
        .map_err(|_| "Codec registry poisoned")?;
    let codec = UserCodec {
        name,
        complib,
        version,
        compress: UserCodecCompress::Legacy(compress),
        decompress: UserCodecDecompress::Legacy(decompress),
    };
    if let Some(existing) = codecs.get(&compcode) {
        if name.is_some() && existing.name == name {
            return Ok(());
        }
        return Err("Private codec ID already registered");
    }
    codecs.insert(compcode, codec);
    drop(codecs);
    remember_codec_order(compcode)?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn register_context_codec_impl(
    compcode: u8,
    name: Option<&'static str>,
    complib: Option<u8>,
    version: Option<u8>,
    compress: ContextCodecCompressFn,
    decompress: ContextCodecDecompressFn,
    allowed: std::ops::RangeInclusive<u8>,
    range_error: &'static str,
    duplicate_error: &'static str,
) -> Result<(), &'static str> {
    if !allowed.contains(&compcode) {
        return Err(range_error);
    }
    if known_global_registration_status(compcode, name, duplicate_error)? {
        return Ok(());
    }
    let mut codecs = user_codecs()
        .write()
        .map_err(|_| "Codec registry poisoned")?;
    let codec = UserCodec {
        name,
        complib,
        version,
        compress: UserCodecCompress::Context(compress),
        decompress: UserCodecDecompress::Context(decompress),
    };
    if let Some(existing) = codecs.get(&compcode) {
        if name.is_some() && existing.name == name {
            return Ok(());
        }
        return Err(duplicate_error);
    }
    codecs.insert(compcode, codec);
    drop(codecs);
    remember_codec_order(compcode)?;
    Ok(())
}

/// Returns `true` if `compcode` corresponds to a registered global descriptor
/// or a user-defined codec currently present in the registry.
pub fn is_registered_codec(compcode: u8) -> bool {
    known_global_codec_by_code(compcode).is_some()
        || user_codecs()
            .read()
            .is_ok_and(|codecs| codecs.contains_key(&compcode))
}

/// Return the registered codec name for a plugin/user-defined codec.
pub fn registered_codec_name(compcode: u8) -> Option<&'static str> {
    if known_global_codec_by_code(compcode).is_some() {
        return known_global_codec_by_code(compcode).map(|codec| codec.name);
    }
    user_codecs()
        .read()
        .ok()
        .and_then(|codecs| codecs.get(&compcode).and_then(|codec| codec.name))
}

/// Return C-Blosc2-style compression library info for a registered codec name.
pub fn registered_codec_complib_info(name: &str) -> Option<(u8, &'static str, &'static str)> {
    if let Some(codec) = known_global_codec_by_name(name) {
        return Some((codec.compcode, codec.name, "unknown"));
    }
    let codecs = user_codecs().read().ok()?;
    let order = user_codec_order().read().ok()?;
    order.iter().find_map(|code| {
        let codec = codecs.get(code)?;
        let codec_name = codec.name?;
        if codec_name != name {
            return None;
        }
        let complib = codec.complib?;
        if let Some((libname, version)) = builtin_complib_info(complib) {
            return Some((complib, libname, version));
        }
        let libname = order
            .iter()
            .find_map(|code| {
                let codec = codecs.get(code)?;
                (codec.complib == Some(complib)).then_some(codec.name?)
            })
            .unwrap_or(codec_name);
        codec.version.map(|_version| (complib, libname, "unknown"))
    })
}

/// Return the registered codec name for a compressor-library code.
pub fn registered_codec_name_by_complib(complib: u8) -> Option<&'static str> {
    if let Some((libname, _version)) = builtin_complib_info(complib) {
        return Some(libname);
    }
    if let Some(codec) = known_global_codec_by_code(complib) {
        return Some(codec.name);
    }
    let codecs = user_codecs().read().ok()?;
    let order = user_codec_order().read().ok()?;
    order.iter().find_map(|code| {
        let codec = codecs.get(code)?;
        (codec.complib == Some(complib)).then_some(codec.name?)
    })
}

/// Return the codec-format version registered for a plugin/user-defined codec.
pub fn registered_codec_version(compcode: u8) -> Option<u8> {
    if let Some(codec) = known_global_codec_by_code(compcode) {
        return Some(codec.version);
    }
    user_codecs()
        .read()
        .ok()
        .and_then(|codecs| codecs.get(&compcode).and_then(|codec| codec.version))
}

/// Return the registered codec ID for a plugin/user-defined codec name.
pub fn registered_codec_code(name: &str) -> Option<u8> {
    if let Some(codec) = known_global_codec_by_name(name) {
        return Some(codec.compcode);
    }
    let codecs = user_codecs().read().ok()?;
    let order = user_codec_order().read().ok()?;
    order
        .iter()
        .find_map(|&code| (codecs.get(&code)?.name == Some(name)).then_some(code))
}

/// Returns `true` if `compcode` supports compression and decompression
/// using a preset dictionary (currently LZ4, LZ4HC and Zstd).
pub fn codec_supports_dict(compcode: u8) -> bool {
    matches!(compcode, BLOSC_LZ4 | BLOSC_LZ4HC | BLOSC_ZSTD)
}

/// Compress a single block using the codec identified by `compcode`.
///
/// `clevel` is the Blosc compression level (0–9); it is mapped to each
/// codec's native level inside the codec-specific wrappers.
/// Returns the number of compressed bytes, or 0 if the data is not
/// compressible (or compression failed and the caller should store the
/// block uncompressed).
pub fn compress_block(compcode: u8, clevel: u8, src: &[u8], dest: &mut [u8]) -> i32 {
    compress_block_with_meta(compcode, clevel, 0, src, dest)
}

/// Compress a block, passing an additional `meta` byte to plugin/user-defined codecs.
///
/// `meta` is forwarded only to user codecs; the built-in codecs ignore it.
/// Returns the number of compressed bytes, or 0 if the block does not compress.
pub fn compress_block_with_meta(
    compcode: u8,
    clevel: u8,
    meta: u8,
    src: &[u8],
    dest: &mut [u8],
) -> i32 {
    compress_block_with_context(compcode, clevel, meta, src, dest, None)
}

/// Compress a block, forwarding rich context to registered plugin codecs.
pub fn compress_block_with_context(
    compcode: u8,
    clevel: u8,
    meta: u8,
    src: &[u8],
    dest: &mut [u8],
    context: Option<CodecCallbackContext<'_>>,
) -> i32 {
    match compcode {
        BLOSC_BLOSCLZ => blosclz::compress(clevel as i32, src, dest),
        BLOSC_LZ4 => lz4_compress(clevel, src, dest),
        BLOSC_LZ4HC => lz4hc_compress(clevel, src, dest),
        BLOSC_ZLIB => zlib_compress(src, dest, clevel),
        BLOSC_ZSTD => zstd_compress(src, dest, clevel),
        #[cfg(feature = "plugin-ndlz")]
        BLOSC_CODEC_NDLZ => compress_ndlz_plugin_block(meta, src, dest, context.as_ref()),
        #[cfg(feature = "plugin-zfp")]
        BLOSC_CODEC_ZFP_FIXED_ACCURACY
        | BLOSC_CODEC_ZFP_FIXED_PRECISION
        | BLOSC_CODEC_ZFP_FIXED_RATE => {
            compress_zfp_plugin_block(compcode, meta, src, dest, context.as_ref())
        }
        _ => match user_codecs()
            .read()
            .ok()
            .and_then(|codecs| codecs.get(&compcode).copied())
        {
            Some(codec) => {
                if matches!(codec.compress, UserCodecCompress::CAbi(None)) {
                    return missing_dynamic_codec_callback();
                }
                let mut callback_context = context.unwrap_or(CodecCallbackContext {
                    compcode,
                    complib: codec.complib,
                    meta,
                    clevel,
                    cparams: None,
                    dparams: None,
                    chunk: CodecChunkContext {
                        schunk: 0,
                        nchunk: -1,
                        nblock: -1,
                        chunk_source: 0,
                        block_offset: 0,
                        blocksize: src.len(),
                        bsize: src.len(),
                    },
                    b2nd_metalayer: None,
                    user_data: 0,
                });
                callback_context.compcode = compcode;
                callback_context.complib = codec.complib;
                callback_context.meta = meta;
                callback_context.clevel = clevel;
                let result = codec
                    .compress
                    .run(&mut callback_context, clevel, meta, src, dest);
                normalize_user_compress_result(result, dest.len())
            }
            None => BLOSC2_ERROR_CODEC_SUPPORT,
        },
    }
}

/// Compress a block using a preset dictionary.
///
/// Falls back to plain [`compress_block`] for codecs that do not support
/// dictionaries (see [`codec_supports_dict`]).
pub fn compress_block_with_dict(
    compcode: u8,
    clevel: u8,
    src: &[u8],
    dest: &mut [u8],
    dict: &[u8],
) -> i32 {
    match compcode {
        BLOSC_LZ4 => lz4_compress_with_dict(clevel, src, dest, dict),
        BLOSC_LZ4HC => lz4hc_compress_with_dict(clevel, src, dest, dict),
        BLOSC_ZSTD => zstd_compress_with_dict(src, dest, clevel, dict),
        _ => compress_block(compcode, clevel, src, dest),
    }
}

/// Decompress a single block using the codec identified by `compcode`.
///
/// Returns the number of decompressed bytes written to `dest`, or a
/// negative value on error (corrupted input, undersized output, etc.).
pub fn decompress_block(compcode: u8, src: &[u8], dest: &mut [u8]) -> i32 {
    decompress_block_with_meta(compcode, 0, src, dest)
}

/// Decompress a block, passing an additional `meta` byte to user-defined codecs.
///
/// `meta` is forwarded only to user codecs; the built-in codecs ignore it.
/// Returns the number of decompressed bytes, or a negative value on error.
pub fn decompress_block_with_meta(compcode: u8, meta: u8, src: &[u8], dest: &mut [u8]) -> i32 {
    decompress_block_with_context(compcode, meta, src, dest, None)
}

/// Decompress a block, forwarding rich context to registered plugin codecs.
pub fn decompress_block_with_context(
    compcode: u8,
    meta: u8,
    src: &[u8],
    dest: &mut [u8],
    context: Option<CodecCallbackContext<'_>>,
) -> i32 {
    match compcode {
        BLOSC_BLOSCLZ => blosclz::decompress(src, dest),
        BLOSC_LZ4 | BLOSC_LZ4HC => lz4_decompress(src, dest),
        BLOSC_ZLIB => zlib_decompress(src, dest),
        BLOSC_ZSTD => zstd_decompress(src, dest),
        #[cfg(feature = "plugin-ndlz")]
        BLOSC_CODEC_NDLZ => decompress_ndlz_plugin_block(meta, src, dest),
        #[cfg(feature = "plugin-zfp")]
        BLOSC_CODEC_ZFP_FIXED_ACCURACY
        | BLOSC_CODEC_ZFP_FIXED_PRECISION
        | BLOSC_CODEC_ZFP_FIXED_RATE => {
            decompress_zfp_plugin_block(compcode, meta, src, dest, context.as_ref())
        }
        _ => match user_codecs()
            .read()
            .ok()
            .and_then(|codecs| codecs.get(&compcode).copied())
        {
            Some(codec) => {
                if matches!(codec.decompress, UserCodecDecompress::CAbi(None)) {
                    return missing_dynamic_codec_callback();
                }
                // C-Blosc2 block sizes are int32-sized; user callbacks should
                // not see a direct decompression request that cannot be
                // represented by the C callback contract.
                if i32::try_from(dest.len()).is_err() {
                    return BLOSC2_ERROR_DATA;
                }
                let mut callback_context = context.unwrap_or(CodecCallbackContext {
                    compcode,
                    complib: codec.complib,
                    meta,
                    clevel: 0,
                    cparams: None,
                    dparams: None,
                    chunk: CodecChunkContext {
                        schunk: 0,
                        nchunk: -1,
                        nblock: -1,
                        chunk_source: 0,
                        block_offset: 0,
                        blocksize: dest.len(),
                        bsize: dest.len(),
                    },
                    b2nd_metalayer: None,
                    user_data: 0,
                });
                callback_context.compcode = compcode;
                callback_context.complib = codec.complib;
                callback_context.meta = meta;
                normalize_user_decompress_result(
                    codec.decompress.run(&mut callback_context, meta, src, dest),
                    dest.len(),
                )
            }
            None => BLOSC2_ERROR_CODEC_SUPPORT,
        },
    }
}

fn normalize_user_compress_result(result: i32, dest_len: usize) -> i32 {
    if result > i32::try_from(dest_len).unwrap_or(i32::MAX) {
        BLOSC2_ERROR_WRITE_BUFFER
    } else if result < 0 {
        BLOSC2_ERROR_DATA
    } else {
        result
    }
}

fn normalize_user_decompress_result(result: i32, dest_len: usize) -> i32 {
    let Ok(dest_len) = i32::try_from(dest_len) else {
        return BLOSC2_ERROR_DATA;
    };
    (result == dest_len)
        .then_some(result)
        .unwrap_or(BLOSC2_ERROR_DATA)
}

/// Decompress a block that was compressed with a preset dictionary.
///
/// Returns 0 for codecs that do not support dictionary decompression.
pub fn decompress_block_with_dict(compcode: u8, src: &[u8], dest: &mut [u8], dict: &[u8]) -> i32 {
    match compcode {
        BLOSC_LZ4 | BLOSC_LZ4HC => lz4_decompress_with_dict(src, dest, dict),
        BLOSC_ZSTD => zstd_decompress_with_dict(src, dest, dict),
        _ => 0,
    }
}

/// LZ4 fast-mode compression of a single block.
fn lz4_compress(clevel: u8, src: &[u8], dest: &mut [u8]) -> i32 {
    use lz4_pure::block::CompressionMode;

    let accel = lz4_acceleration(clevel);
    match lz4_pure::block::compress_to_buffer(src, Some(CompressionMode::FAST(accel)), false, dest)
    {
        Ok(n) => n as i32,
        Err(_) => 0,
    }
}

fn lz4_acceleration(clevel: u8) -> i32 {
    let _ = clevel;
    // c-blosc2 computes 10 - clevel in get_accel(), but the non-IPP LZ4
    // wrapper overrides that value to 1 before calling LZ4_compress_fast.
    1
}

/// LZ4HC high-compression-mode compression of a single block, parameterized
/// by `clevel` (mapped directly to the LZ4HC compression level).
fn lz4hc_compress(clevel: u8, src: &[u8], dest: &mut [u8]) -> i32 {
    use lz4_pure::block::CompressionMode;
    if let Some(error) = lz4hc_2gb_limit_result(src.len()) {
        return error;
    }
    match lz4_pure::block::compress_to_buffer(
        src,
        Some(CompressionMode::HIGHCOMPRESSION(i32::from(clevel))),
        false,
        dest,
    ) {
        Ok(n) => n as i32,
        Err(_) => 0,
    }
}

fn lz4hc_2gb_limit_result(src_len: usize) -> Option<i32> {
    (src_len > (2usize << 30)).then_some(BLOSC2_ERROR_2GB_LIMIT)
}

/// Decompress a block produced by either the LZ4 fast or LZ4HC encoder
/// (both share the same wire format).
fn lz4_decompress(src: &[u8], dest: &mut [u8]) -> i32 {
    match lz4_pure::block::decompress_to_buffer(src, Some(dest.len() as i32), dest) {
        Ok(n) if n == dest.len() => n as i32,
        Ok(_) | Err(_) => 0,
    }
}

/// Convert a buffer length to the `c_int` type used by the LZ4 C API,
/// returning `None` if the length does not fit.
fn len_as_c_int(len: usize) -> Option<lz4_pure::sys::c_int> {
    lz4_pure::sys::c_int::try_from(len).ok()
}

/// LZ4 fast-mode compression seeded with a preset dictionary.
fn lz4_compress_with_dict(clevel: u8, src: &[u8], dest: &mut [u8], dict: &[u8]) -> i32 {
    use lz4_pure::sys::{
        c_char, LZ4_compress_fast_continue, LZ4_createStream, LZ4_freeStream, LZ4_loadDict,
    };

    let Some(src_len) = len_as_c_int(src.len()) else {
        return 0;
    };
    let Some(dest_len) = len_as_c_int(dest.len()) else {
        return 0;
    };
    let Some(dict_len) = len_as_c_int(dict.len()) else {
        return 0;
    };
    let accel = lz4_acceleration(clevel);

    unsafe {
        let stream = LZ4_createStream();
        if stream.is_null() {
            return 0;
        }
        LZ4_loadDict(stream, dict.as_ptr() as *const c_char, dict_len);
        let written = LZ4_compress_fast_continue(
            stream,
            src.as_ptr() as *const c_char,
            dest.as_mut_ptr() as *mut c_char,
            src_len,
            dest_len,
            accel,
        );
        LZ4_freeStream(stream);
        written
    }
}

/// LZ4HC high-compression-mode compression seeded with a preset dictionary.
fn lz4hc_compress_with_dict(clevel: u8, src: &[u8], dest: &mut [u8], dict: &[u8]) -> i32 {
    use lz4_pure::sys::{
        c_char, LZ4_compress_HC_continue, LZ4_createStreamHC, LZ4_freeStreamHC, LZ4_loadDictHC,
        LZ4_resetStreamHC_fast,
    };

    if let Some(error) = lz4hc_2gb_limit_result(src.len()) {
        return error;
    }
    let Some(src_len) = len_as_c_int(src.len()) else {
        return 0;
    };
    let Some(dest_len) = len_as_c_int(dest.len()) else {
        return 0;
    };
    let Some(dict_len) = len_as_c_int(dict.len()) else {
        return 0;
    };

    unsafe {
        let stream = LZ4_createStreamHC();
        if stream.is_null() {
            return 0;
        }
        LZ4_resetStreamHC_fast(stream, i32::from(clevel));
        LZ4_loadDictHC(stream, dict.as_ptr() as *const c_char, dict_len);
        let written = LZ4_compress_HC_continue(
            stream,
            src.as_ptr() as *const c_char,
            dest.as_mut_ptr() as *mut c_char,
            src_len,
            dest_len,
        );
        LZ4_freeStreamHC(stream);
        written
    }
}

/// Decompress an LZ4 block produced against a preset dictionary.
fn lz4_decompress_with_dict(src: &[u8], dest: &mut [u8], dict: &[u8]) -> i32 {
    use lz4_pure::sys::{c_char, LZ4_decompress_safe_usingDict};

    let Some(src_len) = len_as_c_int(src.len()) else {
        return 0;
    };
    let Some(dest_len) = len_as_c_int(dest.len()) else {
        return 0;
    };
    let Some(dict_len) = len_as_c_int(dict.len()) else {
        return 0;
    };

    let written = unsafe {
        LZ4_decompress_safe_usingDict(
            src.as_ptr() as *const c_char,
            dest.as_mut_ptr() as *mut c_char,
            src_len,
            dest_len,
            dict.as_ptr() as *const c_char,
            dict_len,
        )
    };
    if written == dest_len {
        written
    } else {
        0
    }
}

/// Zlib (deflate) compression of a single block.
///
/// `clevel` (0–9) is passed directly to flate2's `Compression`.
fn zlib_compress(src: &[u8], dest: &mut [u8], clevel: u8) -> i32 {
    use flate2::Compression;

    // Use compress directly into dest buffer via flate2's low-level API
    let level = Compression::new(clevel as u32);
    let mut compress = flate2::Compress::new(level, true);

    let status = compress.compress(src, dest, flate2::FlushCompress::Finish);

    match status {
        Ok(flate2::Status::StreamEnd) => compress.total_out() as i32,
        Ok(flate2::Status::Ok | flate2::Status::BufError) => {
            // Output buffer too small or incomplete
            0
        }
        Err(_) => 0,
    }
}

/// Decompress a single zlib-encoded block.
fn zlib_decompress(src: &[u8], dest: &mut [u8]) -> i32 {
    use flate2::Decompress;
    use flate2::FlushDecompress;

    let mut decompress = Decompress::new(true);
    match decompress.decompress(src, dest, FlushDecompress::Finish) {
        Ok(flate2::Status::StreamEnd) => decompress.total_out() as i32,
        Ok(_) => 0,
        Err(_) => 0,
    }
}

/// Map blosc clevel (0..=9) to the underlying zstd compression level,
/// matching `zstd_wrap_compress` in c-blosc2/blosc/blosc2.c:543.
///
/// C formula: `clevel = (clevel < 9) ? clevel * 2 - 1 : ZSTD_maxCLevel();`
/// which gives: 0→-1, 1→1, 2→3, 3→5, 4→7, 5→9, 6→11, 7→13, 8→15, 9→22.
fn blosc_clevel_to_zstd(clevel: u8) -> i32 {
    if clevel < 9 {
        // Signed to accommodate blosc 0 → zstd -1 (fastest / negative-level).
        (clevel as i32) * 2 - 1
    } else {
        // ZSTD_maxCLevel() is 22 in upstream zstd (has been stable since 1.0).
        22
    }
}

/// Zstd compression of a single block, using a thread-local context to
/// avoid repeated allocations across calls.
fn zstd_compress(src: &[u8], dest: &mut [u8], clevel: u8) -> i32 {
    let n = C_ZSTD_CCTX.with(|slot| {
        let mut slot = slot.borrow_mut();
        if slot.is_none() {
            *slot = CZstdCCtx::new();
        }
        let Some(cctx) = slot.as_ref() else {
            return 0;
        };
        unsafe {
            c_zstd_compress_cctx(
                cctx.ptr,
                dest.as_mut_ptr().cast(),
                dest.len(),
                src.as_ptr().cast(),
                src.len(),
                blosc_clevel_to_zstd(clevel),
            )
        }
    });
    if c_zstd_code_is_error(n) || n > i32::MAX as usize {
        0
    } else {
        n as i32
    }
}

/// Zstd compression of a single block, seeded with a preset dictionary.
fn zstd_compress_with_dict(src: &[u8], dest: &mut [u8], _clevel: u8, dict: &[u8]) -> i32 {
    let Some(cdict) = CZstdCDict::new(dict, 1) else {
        return 0;
    };
    let n = C_ZSTD_DICT_CCTX.with(|slot| {
        let mut slot = slot.borrow_mut();
        if slot.is_none() {
            *slot = CZstdCCtx::new();
        }
        let Some(cctx) = slot.as_ref() else {
            return 0;
        };
        unsafe {
            c_zstd_compress_using_cdict(
                cctx.ptr,
                dest.as_mut_ptr().cast(),
                dest.len(),
                src.as_ptr().cast(),
                src.len(),
                cdict.ptr,
            )
        }
    });
    if c_zstd_code_is_error(n) || n > i32::MAX as usize {
        0
    } else {
        n as i32
    }
}

fn c_zstd_code_is_error(code: usize) -> bool {
    unsafe { c_zstd_is_error(code) != 0 }
}

/// Decompress a single zstd-encoded block via a thread-local decoder context.
fn zstd_decompress(src: &[u8], dest: &mut [u8]) -> i32 {
    let n = ZSTD_DCTX.with(|slot| {
        let mut slot = slot.borrow_mut();
        let (dctx, entropy_rep, xxh) = &mut *slot;
        *entropy_rep = ZSTD_decoder_entropy_rep::default();
        ZSTD_decompressDCtx(dctx, entropy_rep, xxh, dest, src)
    });
    if ERR_isError(n) {
        0
    } else {
        n as i32
    }
}

/// Decompress a zstd block that was compressed with the given preset dictionary.
fn zstd_decompress_with_dict(src: &[u8], dest: &mut [u8], dict: &[u8]) -> i32 {
    let n = ZSTD_DICT_DCTX.with(|slot| {
        let mut dctx = slot.borrow_mut();
        let Some(ddict) = ZSTD_createDDict(dict) else {
            return ERROR(ErrorCode::DictionaryCorrupted);
        };
        let n = zstd_decompress_using_ddict_with_active_entropy(&mut dctx, dest, src, &ddict);
        if ERR_isError(n) {
            if zstd_dict_has_magic(dict) {
                *dctx = ZSTD_createDCtx();
                let load = ZSTD_DCtx_loadDictionary(&mut dctx, dict);
                if !ERR_isError(load) {
                    let mut entropy_rep = ZSTD_decoder_entropy_rep::default();
                    let mut xxh = XXH64_state_t::default();
                    let n = ZSTD_decompressDCtx(&mut dctx, &mut entropy_rep, &mut xxh, dest, src);
                    if !ERR_isError(n) {
                        return n;
                    }
                }
            }
            let n = ZSTD_decompress_usingDict(&mut dctx, dest, src, dict);
            if ERR_isError(n) {
                let content = ZSTD_DDict_dictContent(&ddict);
                ZSTD_decompress_usingDict(&mut dctx, dest, src, content)
            } else {
                n
            }
        } else {
            n
        }
    });
    if ERR_isError(n) {
        0
    } else {
        n as i32
    }
}

fn zstd_decompress_using_ddict_with_active_entropy(
    dctx: &mut ZSTD_DCtx,
    dest: &mut [u8],
    src: &[u8],
    ddict: &ZSTD_DDict,
) -> usize {
    let frame_dict_id = ZSTD_getDictID_fromFrame(src);
    if frame_dict_id != 0 && ddict.dictID != 0 && frame_dict_id != ddict.dictID {
        return ERROR(ErrorCode::DictionaryWrong);
    }

    let declared = ZSTD_getFrameContentSize(src);
    let out_size = if declared == ZSTD_CONTENTSIZE_UNKNOWN || declared == ZSTD_CONTENTSIZE_ERROR {
        dest.len()
    } else {
        declared as usize
    };
    if out_size > dest.len() {
        return ERROR(ErrorCode::DstSizeTooSmall);
    }

    let rc = ZSTD_decompressBegin(dctx);
    if ERR_isError(rc) {
        return rc;
    }
    let content = ZSTD_DDict_dictContent(ddict);
    dctx.stream_dict = content.to_vec();
    dctx.dictID = ddict.dictID;
    if ddict.entropyPresent != 0 {
        let mut rep = [0u32; 3];
        let rc = ZSTD_loadDEntropy(dctx, &mut rep, &ddict.dictBuffer);
        if ERR_isError(rc) {
            return rc;
        }
        dctx.ddict_rep = rep;
        dctx.litEntropy = 1;
        dctx.fseEntropy = 1;
        dctx.fse_ll_fresh = true;
        dctx.fse_of_fresh = true;
        dctx.fse_ml_fresh = true;
        dctx.ll_default_active = false;
        dctx.of_default_active = false;
        dctx.ml_default_active = false;
    } else {
        dctx.litEntropy = 0;
        dctx.fseEntropy = 0;
        dctx.fse_ll_fresh = false;
        dctx.fse_of_fresh = false;
        dctx.fse_ml_fresh = false;
        dctx.ll_default_active = true;
        dctx.of_default_active = true;
        dctx.ml_default_active = true;
    }

    let mut combined = vec![0u8; content.len() + out_size];
    combined[..content.len()].copy_from_slice(content);
    let mut rep = ZSTD_decoder_entropy_rep {
        rep: dctx.ddict_rep,
    };
    let mut xxh = XXH64_state_t::default();
    let mut consumed = 0usize;
    let decoded = ZSTD_decompressFrame_withOpStart(
        dctx,
        &mut rep,
        &mut xxh,
        &mut combined,
        content.len(),
        src,
        &mut consumed,
    );
    if ERR_isError(decoded) {
        return decoded;
    }
    dest[..decoded].copy_from_slice(&combined[content.len()..content.len() + decoded]);
    decoded
}

fn zstd_dict_has_magic(dict: &[u8]) -> bool {
    const ZSTD_MAGIC_DICTIONARY: u32 = 0xEC30_A437;
    dict.get(..4)
        .and_then(|bytes| bytes.try_into().ok())
        .is_some_and(|bytes| u32::from_le_bytes(bytes) == ZSTD_MAGIC_DICTIONARY)
}

fn read_u16_le(src: &[u8], pos: usize) -> Option<u16> {
    let bytes: [u8; 2] = src.get(pos..pos + 2)?.try_into().ok()?;
    Some(u16::from_le_bytes(bytes))
}

fn read_i32_le(src: &[u8], pos: usize) -> Option<i32> {
    let bytes: [u8; 4] = src.get(pos..pos + 4)?.try_into().ok()?;
    Some(i32::from_le_bytes(bytes))
}

fn xxh32_seed1(input: &[u8]) -> u32 {
    const PRIME32_1: u32 = 2_654_435_761;
    const PRIME32_2: u32 = 2_246_822_519;
    const PRIME32_3: u32 = 3_266_489_917;
    const PRIME32_4: u32 = 668_265_263;
    const PRIME32_5: u32 = 374_761_393;

    fn round(acc: u32, lane: u32) -> u32 {
        acc.wrapping_add(lane.wrapping_mul(PRIME32_2))
            .rotate_left(13)
            .wrapping_mul(PRIME32_1)
    }

    let mut pos = 0usize;
    let mut hash;
    if input.len() >= 16 {
        let mut v1 = 1u32.wrapping_add(PRIME32_1).wrapping_add(PRIME32_2);
        let mut v2 = 1u32.wrapping_add(PRIME32_2);
        let mut v3 = 1u32;
        let mut v4 = 1u32.wrapping_sub(PRIME32_1);
        while pos <= input.len() - 16 {
            v1 = round(
                v1,
                u32::from_le_bytes(input[pos..pos + 4].try_into().unwrap()),
            );
            pos += 4;
            v2 = round(
                v2,
                u32::from_le_bytes(input[pos..pos + 4].try_into().unwrap()),
            );
            pos += 4;
            v3 = round(
                v3,
                u32::from_le_bytes(input[pos..pos + 4].try_into().unwrap()),
            );
            pos += 4;
            v4 = round(
                v4,
                u32::from_le_bytes(input[pos..pos + 4].try_into().unwrap()),
            );
            pos += 4;
        }
        hash = v1
            .rotate_left(1)
            .wrapping_add(v2.rotate_left(7))
            .wrapping_add(v3.rotate_left(12))
            .wrapping_add(v4.rotate_left(18));
    } else {
        hash = 1u32.wrapping_add(PRIME32_5);
    }

    hash = hash.wrapping_add(input.len() as u32);
    while pos + 4 <= input.len() {
        hash = hash
            .wrapping_add(
                u32::from_le_bytes(input[pos..pos + 4].try_into().unwrap()).wrapping_mul(PRIME32_3),
            )
            .rotate_left(17)
            .wrapping_mul(PRIME32_4);
        pos += 4;
    }
    while pos < input.len() {
        hash = hash
            .wrapping_add((input[pos] as u32).wrapping_mul(PRIME32_5))
            .rotate_left(11)
            .wrapping_mul(PRIME32_1);
        pos += 1;
    }
    hash ^= hash >> 15;
    hash = hash.wrapping_mul(PRIME32_2);
    hash ^= hash >> 13;
    hash = hash.wrapping_mul(PRIME32_3);
    hash ^ (hash >> 16)
}

fn compress_ndlz_plugin_block(
    meta: u8,
    src: &[u8],
    dest: &mut [u8],
    context: Option<&CodecCallbackContext<'_>>,
) -> i32 {
    let Some(context) = context else {
        return 0;
    };
    let Some(b2nd_metalayer) = context.b2nd_metalayer else {
        return -1;
    };
    let Ok(b2nd_meta) = B2ndMeta::deserialize(b2nd_metalayer) else {
        return -1;
    };
    if b2nd_meta.shape.len() != 2 || b2nd_meta.blockshape.len() != 2 {
        return -1;
    }
    compress_ndlz_2d_block(
        meta,
        [b2nd_meta.blockshape[0], b2nd_meta.blockshape[1]],
        src,
        dest,
    )
}

fn decompress_ndlz_plugin_block(meta: u8, src: &[u8], dest: &mut [u8]) -> i32 {
    match meta {
        4 | 8 => decompress_ndlz_cell_stream(meta as usize, src, dest),
        _ => -1,
    }
}

#[cfg(feature = "plugin-zfp")]
fn compress_zfp_plugin_block(
    compcode: u8,
    meta: u8,
    src: &[u8],
    dest: &mut [u8],
    context: Option<&CodecCallbackContext<'_>>,
) -> i32 {
    let Some(context) = context else {
        return 0;
    };
    let Some(cparams) = context.cparams else {
        return 0;
    };
    let Ok(desc) = describe_zfp_block(context.b2nd_metalayer, cparams.typesize, src.len()) else {
        return BLOSC2_ERROR_FAILURE;
    };
    let config = zfp_config_for_mode(compcode, meta, desc.scalar_type, desc.dimensionality);
    let capacity = config
        .maximum_size(desc.scalar_type, &desc.dims[..desc.ndim])
        .max(dest.len());
    let field =
        unsafe { ZfpField::from_raw(src.as_ptr(), src.len(), desc.scalar_type, desc.dims, [0; 4]) };
    let mut stream = ZfpBitStream::new(capacity);
    let Ok(cbytes) = stream.compress(&config, &field) else {
        return 0;
    };
    if cbytes == 0 || cbytes >= src.len() {
        return 0;
    }
    if cbytes > dest.len() || i32::try_from(cbytes).is_err() {
        return 0;
    }
    dest[..cbytes].copy_from_slice(&stream.as_bytes()[..cbytes]);
    cbytes as i32
}

#[cfg(feature = "plugin-zfp")]
fn decompress_zfp_plugin_block(
    compcode: u8,
    meta: u8,
    src: &[u8],
    dest: &mut [u8],
    context: Option<&CodecCallbackContext<'_>>,
) -> i32 {
    let Some(context) = context else {
        return 0;
    };
    let Some(dparams) = context.dparams else {
        return 0;
    };
    let Ok(desc) = describe_zfp_block(context.b2nd_metalayer, dparams.typesize, dest.len()) else {
        return BLOSC2_ERROR_FAILURE;
    };
    let config = zfp_config_for_mode(compcode, meta, desc.scalar_type, desc.dimensionality);
    let mut field = unsafe {
        ZfpFieldMut::from_raw(
            dest.as_mut_ptr(),
            dest.len(),
            desc.scalar_type,
            desc.dims,
            [0; 4],
        )
    };
    let mut stream = ZfpBitStream::from_bytes(src);
    match stream.decompress(&config, &mut field) {
        Ok(0) | Err(_) => 0,
        Ok(_) => i32::try_from(dest.len()).unwrap_or(BLOSC2_ERROR_DATA),
    }
}

#[cfg(feature = "plugin-zfp")]
#[derive(Clone, Copy)]
struct ZfpBlockDescriptor {
    ndim: usize,
    dims: [usize; 4],
    scalar_type: ZfpScalarType,
    dimensionality: ZfpDimensionality,
}

#[cfg(feature = "plugin-zfp")]
fn describe_zfp_block(
    b2nd_metalayer: Option<&[u8]>,
    typesize: i32,
    data_len: usize,
) -> Result<ZfpBlockDescriptor, ()> {
    let b2nd_metalayer = b2nd_metalayer.ok_or(())?;
    let b2nd_meta = B2ndMeta::deserialize(b2nd_metalayer).map_err(|_| ())?;
    let ndim = b2nd_meta.blockshape.len();
    let dimensionality = match ndim {
        1 => ZfpDimensionality::D1,
        2 => ZfpDimensionality::D2,
        3 => ZfpDimensionality::D3,
        4 => ZfpDimensionality::D4,
        _ => return Err(()),
    };
    let scalar_type = match typesize {
        4 => ZfpScalarType::Float,
        8 => ZfpScalarType::Double,
        _ => return Err(()),
    };
    let mut dims = [0usize; 4];
    let mut values = 1usize;
    for i in 0..ndim {
        let block_dim = b2nd_meta.blockshape[i];
        if block_dim < 4 {
            return Err(());
        }
        let dim = usize::try_from(block_dim).map_err(|_| ())?;
        dims[i] = usize::try_from(b2nd_meta.blockshape[ndim - 1 - i]).map_err(|_| ())?;
        values = values.checked_mul(dim).ok_or(())?;
    }
    let expected = values
        .checked_mul(usize::try_from(typesize).map_err(|_| ())?)
        .ok_or(())?;
    if data_len < expected {
        return Err(());
    }
    Ok(ZfpBlockDescriptor {
        ndim,
        dims,
        scalar_type,
        dimensionality,
    })
}

#[cfg(feature = "plugin-zfp")]
fn zfp_config_for_mode(
    compcode: u8,
    meta: u8,
    scalar_type: ZfpScalarType,
    dimensionality: ZfpDimensionality,
) -> ZfpConfig {
    match compcode {
        BLOSC_CODEC_ZFP_FIXED_ACCURACY => ZfpConfig::fixed_accuracy(10f64.powi(meta as i8 as i32)),
        BLOSC_CODEC_ZFP_FIXED_PRECISION => {
            let offset = 2 * u32::from(dimensionality) + 3;
            ZfpConfig::fixed_precision((u32::from(meta) + offset).min(ZFP_MAX_PREC))
        }
        BLOSC_CODEC_ZFP_FIXED_RATE => {
            let ratio = f64::from(meta) / 100.0;
            let rate = ratio * scalar_type.size() as f64 * 8.0;
            ZfpConfig::fixed_rate(rate, scalar_type, dimensionality, ZfpStreamAlignment::None)
        }
        _ => ZfpConfig::new(),
    }
}

const NDLZ_HASH_TABLE_SIZE: usize = 1 << 12;

fn ndlz_rows_key(cell: &[u8], cell_shape: usize, rows: impl IntoIterator<Item = usize>) -> Vec<u8> {
    let mut key = Vec::new();
    for row in rows {
        let start = row * cell_shape;
        key.extend_from_slice(&cell[start..start + cell_shape]);
    }
    key
}

fn ndlz_hash_bucket(bytes: &[u8]) -> usize {
    (xxh32_seed1(bytes) >> 20) as usize
}

fn ndlz_table_match(
    table: &[usize; NDLZ_HASH_TABLE_SIZE],
    bucket: usize,
    bytes: &[u8],
    dest: &[u8],
) -> Option<usize> {
    let start = table[bucket];
    if start == 0 {
        return None;
    }
    let stored = dest.get(start..start + bytes.len())?;
    (stored == bytes).then_some(start)
}

/// Compress one 2D NDLZ block using C-Blosc2's block wire format.
///
/// This emits literal cells, same-value cells, full-cell back references, and
/// row pair/triple back references to previously emitted literal rows.
pub fn compress_ndlz_2d_block(meta: u8, blockshape: [i32; 2], src: &[u8], dest: &mut [u8]) -> i32 {
    let cell_shape = match meta {
        4 | 8 => meta as usize,
        _ => return -1,
    };
    if blockshape[0] < 0 || blockshape[1] < 0 {
        return -1;
    }
    let rows = blockshape[0] as usize;
    let cols = blockshape[1] as usize;
    let Some(expected_len) = rows.checked_mul(cols) else {
        return -1;
    };
    if src.len() != expected_len {
        return -1;
    }
    if dest.len() < 1 + 2 * std::mem::size_of::<i32>() {
        return -1;
    }
    if expected_len < cell_shape * cell_shape {
        return 0;
    }
    let min_output_len = 17 + expected_len / (cell_shape * cell_shape) * 2 - 2;
    if dest.len() < min_output_len {
        return 0;
    }

    let stop_rows = rows.div_ceil(cell_shape);
    let stop_cols = cols.div_ceil(cell_shape);
    let mut op = 0usize;
    dest[op] = 2;
    op += 1;
    dest[op..op + 4].copy_from_slice(&blockshape[0].to_le_bytes());
    op += 4;
    dest[op..op + 4].copy_from_slice(&blockshape[1].to_le_bytes());
    op += 4;
    let mut tab_cell = [0usize; NDLZ_HASH_TABLE_SIZE];
    let mut tab_pair = [0usize; NDLZ_HASH_TABLE_SIZE];
    let mut tab_triple = [0usize; NDLZ_HASH_TABLE_SIZE];

    for cell_row in 0..stop_rows {
        'cell_cols: for cell_col in 0..stop_cols {
            let pad_rows = if cell_row == stop_rows - 1 && !rows.is_multiple_of(cell_shape) {
                rows % cell_shape
            } else {
                cell_shape
            };
            let pad_cols = if cell_col == stop_cols - 1 && !cols.is_multiple_of(cell_shape) {
                cols % cell_shape
            } else {
                cell_shape
            };
            let orig = cell_row * cell_shape * cols + cell_col * cell_shape;
            let full_cell = pad_rows == cell_shape && pad_cols == cell_shape;
            if op + cell_shape * cell_shape + 1 > dest.len() {
                return 0;
            }
            let mut cell = Vec::new();
            if full_cell {
                cell.reserve_exact(cell_shape * cell_shape);
                for row in 0..cell_shape {
                    let start = orig + row * cols;
                    cell.extend_from_slice(&src[start..start + cell_shape]);
                }
            }

            if full_cell && cell.iter().all(|&byte| byte == cell[0]) {
                if op + 2 > dest.len() {
                    return 0;
                }
                dest[op] = 0x40;
                dest[op + 1] = cell[0];
                op += 2;
                if op > src.len() {
                    return 0;
                }
                continue;
            }

            let hash_cell = full_cell.then(|| ndlz_hash_bucket(&cell));
            let mut update_triple = [0usize; 6];
            let mut hash_triple = [0usize; 6];
            let mut update_pair = [0usize; 7];
            let mut hash_pair = [0usize; 7];

            if full_cell {
                if let Some(literal_start) =
                    ndlz_table_match(&tab_cell, hash_cell.unwrap(), &cell, dest)
                {
                    let Some(offset) = op.checked_sub(literal_start) else {
                        return -1;
                    };
                    if offset > 0 && offset < u16::MAX as usize {
                        if op + 3 > dest.len() {
                            return 0;
                        }
                        dest[op] = 0xc0;
                        dest[op + 1..op + 3].copy_from_slice(&(offset as u16).to_le_bytes());
                        op += 3;
                        if op > src.len() {
                            return 0;
                        }
                        continue;
                    }
                }
            }

            if full_cell {
                let token_pos = op;
                if cell_shape == 8 {
                    for row in 0..=cell_shape - 3 {
                        let key = ndlz_rows_key(&cell, cell_shape, row..row + 3);
                        let bucket = ndlz_hash_bucket(&key);
                        if let Some(ref_start) = ndlz_table_match(&tab_triple, bucket, &key, dest) {
                            let Some(offset) = token_pos.checked_sub(ref_start) else {
                                return -1;
                            };
                            let distance = token_pos + row * cell_shape - ref_start;
                            if distance > 0 && distance < u16::MAX as usize {
                                let literal_rows = cell_shape - 3;
                                if op + 3 + literal_rows * cell_shape > dest.len() {
                                    return 0;
                                }
                                dest[op] = (21 << 3) | row as u8;
                                dest[op + 1..op + 3]
                                    .copy_from_slice(&(offset as u16).to_le_bytes());
                                op += 3;
                                for literal_row in 0..cell_shape {
                                    if literal_row < row || literal_row >= row + 3 {
                                        let start = literal_row * cell_shape;
                                        dest[op..op + cell_shape]
                                            .copy_from_slice(&cell[start..start + cell_shape]);
                                        op += cell_shape;
                                    }
                                }
                                if op > src.len() {
                                    return 0;
                                }
                                continue 'cell_cols;
                            }
                        } else {
                            update_triple[row] = token_pos + 1 + row * cell_shape;
                            hash_triple[row] = bucket;
                        }
                    }

                    for row in 0..=cell_shape - 2 {
                        let key = ndlz_rows_key(&cell, cell_shape, row..row + 2);
                        let bucket = ndlz_hash_bucket(&key);
                        if let Some(ref_start) = ndlz_table_match(&tab_pair, bucket, &key, dest) {
                            let Some(offset) = token_pos.checked_sub(ref_start) else {
                                return -1;
                            };
                            let distance = token_pos + row * cell_shape - ref_start;
                            if distance > 0 && distance < u16::MAX as usize {
                                let literal_rows = cell_shape - 2;
                                if op + 3 + literal_rows * cell_shape > dest.len() {
                                    return 0;
                                }
                                dest[op] = (17 << 3) | row as u8;
                                dest[op + 1..op + 3]
                                    .copy_from_slice(&(offset as u16).to_le_bytes());
                                op += 3;
                                for literal_row in 0..cell_shape {
                                    if literal_row < row || literal_row >= row + 2 {
                                        let start = literal_row * cell_shape;
                                        dest[op..op + cell_shape]
                                            .copy_from_slice(&cell[start..start + cell_shape]);
                                        op += cell_shape;
                                    }
                                }
                                if op > src.len() {
                                    return 0;
                                }
                                continue 'cell_cols;
                            }
                        } else {
                            update_pair[row] = token_pos + 1 + row * cell_shape;
                            hash_pair[row] = bucket;
                        }
                    }
                } else {
                    for j in 1..cell_shape {
                        let first_key = ndlz_rows_key(&cell, cell_shape, [0, j]);
                        let first_bucket = ndlz_hash_bucket(&first_key);
                        let Some(first_ref_start) =
                            ndlz_table_match(&tab_pair, first_bucket, &first_key, dest)
                        else {
                            continue;
                        };
                        let Some(first_offset) = token_pos.checked_sub(first_ref_start) else {
                            return -1;
                        };
                        if first_offset == 0 || first_offset >= u16::MAX as usize {
                            continue;
                        }

                        let mut remaining = (1..cell_shape).filter(|&row| row != j);
                        let Some(l) = remaining.next() else {
                            return -1;
                        };
                        let Some(m) = remaining.next() else {
                            return -1;
                        };
                        let second_key = ndlz_rows_key(&cell, cell_shape, [l, m]);
                        let second_bucket = ndlz_hash_bucket(&second_key);
                        let Some(second_ref_start) =
                            ndlz_table_match(&tab_pair, second_bucket, &second_key, dest)
                        else {
                            continue;
                        };
                        let Some(second_offset) = token_pos.checked_sub(second_ref_start) else {
                            return -1;
                        };
                        let second_distance = token_pos + l * cell_shape - second_ref_start;
                        if second_distance == 0 || second_distance >= u16::MAX as usize {
                            continue;
                        }
                        if op + 5 > dest.len() {
                            return 0;
                        }
                        dest[op] = (1 << 5) | ((j as u8) << 3);
                        dest[op + 1..op + 3].copy_from_slice(&(first_offset as u16).to_le_bytes());
                        dest[op + 3..op + 5].copy_from_slice(&(second_offset as u16).to_le_bytes());
                        op += 5;
                        if op > src.len() {
                            return 0;
                        }
                        continue 'cell_cols;
                    }

                    for rows in [[0usize, 1, 2], [0, 1, 3], [0, 2, 3], [1, 2, 3]] {
                        let key = ndlz_rows_key(&cell, cell_shape, rows);
                        let bucket = ndlz_hash_bucket(&key);
                        if let Some(ref_start) = ndlz_table_match(&tab_triple, bucket, &key, dest) {
                            let Some(offset) = token_pos.checked_sub(ref_start) else {
                                return -1;
                            };
                            let distance = token_pos + rows[0] * cell_shape - ref_start;
                            if distance > 0 && distance < u16::MAX as usize {
                                if op + 3 + cell_shape > dest.len() {
                                    return 0;
                                }
                                dest[op] = match rows {
                                    [1, 2, 3] => 7 << 5,
                                    [0, 1, 2] => (7 << 5) | (1 << 3),
                                    [0, 1, 3] => (7 << 5) | (2 << 3),
                                    [0, 2, 3] => (7 << 5) | (3 << 3),
                                    _ => unreachable!(),
                                };
                                dest[op + 1..op + 3]
                                    .copy_from_slice(&(offset as u16).to_le_bytes());
                                op += 3;
                                for literal_row in 0..cell_shape {
                                    if !rows.contains(&literal_row) {
                                        let start = literal_row * cell_shape;
                                        dest[op..op + cell_shape]
                                            .copy_from_slice(&cell[start..start + cell_shape]);
                                        op += cell_shape;
                                        break;
                                    }
                                }
                                if op > src.len() {
                                    return 0;
                                }
                                continue 'cell_cols;
                            }
                        } else if rows[1] - rows[0] == 1 && rows[2] - rows[1] == 1 {
                            update_triple[rows[0]] = token_pos + 1 + rows[0] * cell_shape;
                            hash_triple[rows[0]] = bucket;
                        }
                    }

                    for rows in [[0usize, 1], [0, 2], [0, 3], [1, 2], [1, 3], [2, 3]] {
                        let key = ndlz_rows_key(&cell, cell_shape, rows);
                        let bucket = ndlz_hash_bucket(&key);
                        if let Some(ref_start) = ndlz_table_match(&tab_pair, bucket, &key, dest) {
                            let Some(offset) = token_pos.checked_sub(ref_start) else {
                                return -1;
                            };
                            let distance = token_pos + rows[0] * cell_shape - ref_start;
                            if distance > 0 && distance < u16::MAX as usize {
                                if op + 3 + 2 * cell_shape > dest.len() {
                                    return 0;
                                }
                                dest[op] = if rows == [2, 3] {
                                    1 << 7
                                } else {
                                    (1 << 7) | ((rows[0] as u8) << 5) | ((rows[1] as u8) << 3)
                                };
                                dest[op + 1..op + 3]
                                    .copy_from_slice(&(offset as u16).to_le_bytes());
                                op += 3;
                                for literal_row in 0..cell_shape {
                                    if !rows.contains(&literal_row) {
                                        let start = literal_row * cell_shape;
                                        dest[op..op + cell_shape]
                                            .copy_from_slice(&cell[start..start + cell_shape]);
                                        op += cell_shape;
                                    }
                                }
                                if op > src.len() {
                                    return 0;
                                }
                                continue 'cell_cols;
                            }
                        } else if rows[1] - rows[0] == 1 {
                            update_pair[rows[0]] = token_pos + 1 + rows[0] * cell_shape;
                            hash_pair[rows[0]] = bucket;
                        }
                    }
                }
            }

            let literal_len = pad_rows * pad_cols;
            if op + 1 + literal_len > dest.len() {
                return 0;
            }
            dest[op] = 0;
            op += 1;
            let literal_start = op;
            for row in 0..pad_rows {
                let start = orig + row * cols;
                dest[op..op + pad_cols].copy_from_slice(&src[start..start + pad_cols]);
                op += pad_cols;
            }
            if full_cell {
                tab_cell[hash_cell.unwrap()] = literal_start;
                if update_triple[0] != 0 {
                    let staged_triples = cell_shape - 2;
                    for h in 0..staged_triples {
                        tab_triple[hash_triple[h]] = update_triple[h];
                    }
                }
                if update_pair[0] != 0 {
                    let staged_pairs = cell_shape - 1;
                    for h in 0..staged_pairs {
                        tab_pair[hash_pair[h]] = update_pair[h];
                    }
                }
            }

            if op > src.len() {
                return 0;
            }
        }
    }

    op as i32
}

fn ndlz_copy_rows_from_stream(
    cell: &mut [u8],
    cell_shape: usize,
    src: &[u8],
    start: usize,
    rows: impl IntoIterator<Item = usize>,
) -> Option<()> {
    for (row_idx, row) in rows.into_iter().enumerate() {
        let src_start = start.checked_add(row_idx.checked_mul(cell_shape)?)?;
        let src_end = src_start.checked_add(cell_shape)?;
        let dst_start = row.checked_mul(cell_shape)?;
        let dst_end = dst_start.checked_add(cell_shape)?;
        cell.get_mut(dst_start..dst_end)?
            .copy_from_slice(src.get(src_start..src_end)?);
    }
    Some(())
}

fn decompress_ndlz_cell_stream(cell_shape: usize, src: &[u8], dest: &mut [u8]) -> i32 {
    if src.len() < 8 {
        return 0;
    }
    if src.len() < 9 || src[0] != 2 {
        return -1;
    }
    let Some(rows) = read_i32_le(src, 1) else {
        return -1;
    };
    let Some(cols) = read_i32_le(src, 5) else {
        return -1;
    };
    if rows < 0 || cols < 0 {
        return -1;
    }
    let rows = rows as usize;
    let cols = cols as usize;
    let Some(expected_len) = rows.checked_mul(cols) else {
        return -1;
    };
    if expected_len > dest.len() {
        return 0;
    }
    dest[..expected_len].fill(0);

    let cell_size = cell_shape * cell_shape;
    let stop_rows = rows.div_ceil(cell_shape);
    let stop_cols = cols.div_ceil(cell_shape);
    let mut ip = 9usize;
    let mut last_end = 0usize;

    for cell_row in 0..stop_rows {
        for cell_col in 0..stop_cols {
            if ip >= src.len() {
                return -1;
            }
            let pad_rows = if cell_row == stop_rows - 1 && !rows.is_multiple_of(cell_shape) {
                rows % cell_shape
            } else {
                cell_shape
            };
            let pad_cols = if cell_col == stop_cols - 1 && !cols.is_multiple_of(cell_shape) {
                cols % cell_shape
            } else {
                cell_shape
            };

            let token_pos = ip;
            let token = src[ip];
            ip += 1;
            let mut cell = vec![0u8; cell_size];

            if token == 0 {
                let literal_len = pad_rows * pad_cols;
                let Some(literal) = src.get(ip..ip + literal_len) else {
                    return -1;
                };
                for row in 0..pad_rows {
                    let lit_start = row * pad_cols;
                    let dst_start = row * cell_shape;
                    cell[dst_start..dst_start + pad_cols]
                        .copy_from_slice(&literal[lit_start..lit_start + pad_cols]);
                }
                ip += literal_len;
            } else if token == 0xc0 {
                let Some(offset) = read_u16_le(src, ip).map(usize::from) else {
                    return -1;
                };
                let Some(ref_start) = ip.checked_sub(offset + 1) else {
                    return -1;
                };
                if ndlz_copy_rows_from_stream(&mut cell, cell_shape, src, ref_start, 0..cell_shape)
                    .is_none()
                {
                    return -1;
                }
                ip += 2;
            } else if token == 0x40 {
                let Some(&value) = src.get(ip) else {
                    return -1;
                };
                cell.fill(value);
                ip += 1;
            } else if cell_shape == 4 {
                if token >= 224 {
                    let Some(offset) = read_u16_le(src, ip).map(|v| usize::from(v) + 3) else {
                        return -1;
                    };
                    ip += 2;
                    let (i, j, k) = if token >> 3 == 28 {
                        (1, 2, 3)
                    } else {
                        let i = 0;
                        if token >> 3 < 30 {
                            (i, 1, 2)
                        } else if token >> 3 == 30 {
                            (i, 1, 3)
                        } else {
                            (i, 2, 3)
                        }
                    };
                    let Some(ref_start) = ip.checked_sub(offset) else {
                        return -1;
                    };
                    if ndlz_copy_rows_from_stream(&mut cell, cell_shape, src, ref_start, [i, j, k])
                        .is_none()
                    {
                        return -1;
                    }
                    for row in 0..cell_shape {
                        if row != i && row != j && row != k {
                            let Some(literal) = src.get(ip..ip + cell_shape) else {
                                return -1;
                            };
                            cell[row * cell_shape..(row + 1) * cell_shape].copy_from_slice(literal);
                            ip += cell_shape;
                            break;
                        }
                    }
                } else if (128..=191).contains(&token) {
                    let Some(offset) = read_u16_le(src, ip).map(|v| usize::from(v) + 3) else {
                        return -1;
                    };
                    ip += 2;
                    let (i, j) = if token == 128 {
                        (2, 3)
                    } else {
                        let i = ((token - 128) >> 5) as usize;
                        let j = (((token - 128) >> 3) - ((i as u8) << 2)) as usize;
                        (i, j)
                    };
                    let Some(ref_start) = ip.checked_sub(offset) else {
                        return -1;
                    };
                    if ndlz_copy_rows_from_stream(&mut cell, cell_shape, src, ref_start, [i, j])
                        .is_none()
                    {
                        return -1;
                    }
                    for row in 0..cell_shape {
                        if row != i && row != j {
                            let Some(literal) = src.get(ip..ip + cell_shape) else {
                                return -1;
                            };
                            cell[row * cell_shape..(row + 1) * cell_shape].copy_from_slice(literal);
                            ip += cell_shape;
                        }
                    }
                } else if (40..=63).contains(&token) {
                    let Some(offset_1) = read_u16_le(src, ip).map(|v| usize::from(v) + 5) else {
                        return -1;
                    };
                    ip += 2;
                    let Some(offset_2) = read_u16_le(src, ip).map(|v| usize::from(v) + 5) else {
                        return -1;
                    };
                    ip += 2;
                    let i = 0usize;
                    let j = ((token - 32) >> 3) as usize;
                    let mut rest = (1..cell_shape).filter(|&row| row != j);
                    let Some(l) = rest.next() else {
                        return -1;
                    };
                    let Some(m) = rest.next() else {
                        return -1;
                    };
                    let Some(ref_start_1) = ip.checked_sub(offset_1) else {
                        return -1;
                    };
                    let Some(ref_start_2) = ip.checked_sub(offset_2) else {
                        return -1;
                    };
                    if ndlz_copy_rows_from_stream(&mut cell, cell_shape, src, ref_start_1, [i, j])
                        .is_none()
                    {
                        return -1;
                    }
                    if ndlz_copy_rows_from_stream(&mut cell, cell_shape, src, ref_start_2, [l, m])
                        .is_none()
                    {
                        return -1;
                    }
                } else {
                    return -1;
                }
            } else {
                let match_type = token >> 3;
                if match_type == 21 || match_type == 17 {
                    let row = (token & 7) as usize;
                    let matched = if match_type == 21 { 3 } else { 2 };
                    if row + matched > cell_shape {
                        return -1;
                    }
                    let Some(offset) = read_u16_le(src, ip).map(usize::from) else {
                        return -1;
                    };
                    ip += 2;
                    let Some(ref_start) = ip.checked_sub(3 + offset) else {
                        return -1;
                    };
                    if ndlz_copy_rows_from_stream(
                        &mut cell,
                        cell_shape,
                        src,
                        ref_start,
                        row..row + matched,
                    )
                    .is_none()
                    {
                        return -1;
                    }
                    for literal_row in 0..cell_shape {
                        if literal_row < row || literal_row >= row + matched {
                            let Some(literal) = src.get(ip..ip + cell_shape) else {
                                return -1;
                            };
                            cell[literal_row * cell_shape..(literal_row + 1) * cell_shape]
                                .copy_from_slice(literal);
                            ip += cell_shape;
                        }
                    }
                } else {
                    return -1;
                }
            }

            let orig = cell_row * cell_shape * cols + cell_col * cell_shape;
            for row in 0..pad_rows {
                let dst_start = orig + row * cols;
                let dst_end = dst_start + pad_cols;
                let src_start = row * cell_shape;
                dest[dst_start..dst_end].copy_from_slice(&cell[src_start..src_start + pad_cols]);
                last_end = dst_end;
            }
            if ip < token_pos || last_end > dest.len() {
                return -1;
            }
        }
    }

    if last_end != expected_len {
        return -1;
    }
    expected_len as i32
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        atomic::{AtomicBool, AtomicI32, AtomicU8, AtomicUsize, Ordering},
        Mutex,
    };

    static C_ABI_TEST_LOCK: Mutex<()> = Mutex::new(());
    static C_ABI_COMPRESS_INPUT_LEN: AtomicI32 = AtomicI32::new(0);
    static C_ABI_COMPRESS_OUTPUT_LEN: AtomicI32 = AtomicI32::new(0);
    static C_ABI_COMPRESS_META: AtomicU8 = AtomicU8::new(0);
    static C_ABI_COMPRESS_CLEVEL: AtomicU8 = AtomicU8::new(0);
    static C_ABI_COMPRESS_COMP_META: AtomicU8 = AtomicU8::new(0);
    static C_ABI_COMPRESS_USE_DICT: AtomicI32 = AtomicI32::new(0);
    static C_ABI_COMPRESS_TYPESIZE: AtomicI32 = AtomicI32::new(0);
    static C_ABI_COMPRESS_BLOCKSIZE: AtomicI32 = AtomicI32::new(0);
    static C_ABI_COMPRESS_SPLITMODE: AtomicI32 = AtomicI32::new(0);
    static C_ABI_COMPRESS_FILTER_LAST: AtomicU8 = AtomicU8::new(0);
    static C_ABI_COMPRESS_INPUT_PTR: AtomicUsize = AtomicUsize::new(0);
    static C_ABI_COMPRESS_SCHUNK: AtomicUsize = AtomicUsize::new(0);
    static C_ABI_COMPRESS_CHUNK: AtomicUsize = AtomicUsize::new(0);
    static C_ABI_COMPRESS_PREFILTER_SET: AtomicBool = AtomicBool::new(false);
    static C_ABI_COMPRESS_PREPARAMS: AtomicUsize = AtomicUsize::new(0);
    static C_ABI_COMPRESS_TUNER_ID: AtomicI32 = AtomicI32::new(0);
    static C_ABI_COMPRESS_INSTR_CODEC: AtomicBool = AtomicBool::new(false);
    static C_ABI_COMPRESS_CODEC_PARAMS: AtomicUsize = AtomicUsize::new(0);
    static C_ABI_DECOMPRESS_INPUT_LEN: AtomicI32 = AtomicI32::new(0);
    static C_ABI_DECOMPRESS_OUTPUT_LEN: AtomicI32 = AtomicI32::new(0);
    static C_ABI_DECOMPRESS_META: AtomicU8 = AtomicU8::new(0);
    static C_ABI_DECOMPRESS_TYPESIZE: AtomicI32 = AtomicI32::new(0);
    static C_ABI_DECOMPRESS_INPUT_PTR: AtomicUsize = AtomicUsize::new(0);
    static C_ABI_DECOMPRESS_SCHUNK: AtomicUsize = AtomicUsize::new(0);
    static C_ABI_DECOMPRESS_CHUNK: AtomicUsize = AtomicUsize::new(0);
    static C_ABI_DECOMPRESS_POSTFILTER_SET: AtomicBool = AtomicBool::new(false);
    static C_ABI_DECOMPRESS_POSTPARAMS: AtomicUsize = AtomicUsize::new(0);

    unsafe extern "C" fn c_abi_codec_encoder(
        input: *const u8,
        input_len: i32,
        output: *mut u8,
        output_len: i32,
        meta: u8,
        cparams: *mut Blosc2CParams,
        chunk: *const c_void,
    ) -> i32 {
        if input.is_null() || output.is_null() || cparams.is_null() {
            return -1;
        }
        let cparams = unsafe { &*cparams };
        C_ABI_COMPRESS_INPUT_LEN.store(input_len, Ordering::SeqCst);
        C_ABI_COMPRESS_OUTPUT_LEN.store(output_len, Ordering::SeqCst);
        C_ABI_COMPRESS_META.store(meta, Ordering::SeqCst);
        C_ABI_COMPRESS_INPUT_PTR.store(input as usize, Ordering::SeqCst);
        C_ABI_COMPRESS_CLEVEL.store(cparams.clevel, Ordering::SeqCst);
        C_ABI_COMPRESS_COMP_META.store(cparams.compcode_meta, Ordering::SeqCst);
        C_ABI_COMPRESS_USE_DICT.store(cparams.use_dict, Ordering::SeqCst);
        C_ABI_COMPRESS_TYPESIZE.store(cparams.typesize, Ordering::SeqCst);
        C_ABI_COMPRESS_BLOCKSIZE.store(cparams.blocksize, Ordering::SeqCst);
        C_ABI_COMPRESS_SPLITMODE.store(cparams.splitmode, Ordering::SeqCst);
        C_ABI_COMPRESS_FILTER_LAST.store(cparams.filters[BLOSC2_MAX_FILTERS - 1], Ordering::SeqCst);
        C_ABI_COMPRESS_SCHUNK.store(cparams.schunk as usize, Ordering::SeqCst);
        C_ABI_COMPRESS_CHUNK.store(chunk as usize, Ordering::SeqCst);
        C_ABI_COMPRESS_PREFILTER_SET.store(cparams.prefilter.is_some(), Ordering::SeqCst);
        C_ABI_COMPRESS_PREPARAMS.store(cparams.preparams as usize, Ordering::SeqCst);
        C_ABI_COMPRESS_TUNER_ID.store(cparams.tuner_id, Ordering::SeqCst);
        C_ABI_COMPRESS_INSTR_CODEC.store(cparams.instr_codec, Ordering::SeqCst);
        C_ABI_COMPRESS_CODEC_PARAMS.store(cparams.codec_params as usize, Ordering::SeqCst);
        if input_len < 0 || output_len < input_len {
            return 0;
        }
        unsafe {
            std::ptr::copy_nonoverlapping(input, output, input_len as usize);
        }
        input_len
    }

    unsafe extern "C" fn c_abi_codec_decoder(
        input: *const u8,
        input_len: i32,
        output: *mut u8,
        output_len: i32,
        meta: u8,
        dparams: *mut Blosc2DParams,
        chunk: *const c_void,
    ) -> i32 {
        if input.is_null() || output.is_null() || dparams.is_null() {
            return -1;
        }
        let dparams = unsafe { &*dparams };
        C_ABI_DECOMPRESS_INPUT_LEN.store(input_len, Ordering::SeqCst);
        C_ABI_DECOMPRESS_OUTPUT_LEN.store(output_len, Ordering::SeqCst);
        C_ABI_DECOMPRESS_META.store(meta, Ordering::SeqCst);
        C_ABI_DECOMPRESS_INPUT_PTR.store(input as usize, Ordering::SeqCst);
        C_ABI_DECOMPRESS_TYPESIZE.store(dparams.typesize, Ordering::SeqCst);
        C_ABI_DECOMPRESS_SCHUNK.store(dparams.schunk as usize, Ordering::SeqCst);
        C_ABI_DECOMPRESS_CHUNK.store(chunk as usize, Ordering::SeqCst);
        C_ABI_DECOMPRESS_POSTFILTER_SET.store(dparams.postfilter.is_some(), Ordering::SeqCst);
        C_ABI_DECOMPRESS_POSTPARAMS.store(dparams.postparams as usize, Ordering::SeqCst);
        if input_len < 0 || output_len < input_len {
            return -1;
        }
        unsafe {
            std::ptr::copy_nonoverlapping(input, output, input_len as usize);
        }
        output_len
    }

    #[test]
    fn blosc_clevel_to_zstd_matches_c_library_mapping() {
        // Table from c-blosc2/blosc/blosc2.c zstd_wrap_compress.
        // Blosc level → zstd level.
        let expected = [
            (0, -1),
            (1, 1),
            (2, 3),
            (3, 5),
            (4, 7),
            (5, 9),
            (6, 11),
            (7, 13),
            (8, 15),
            (9, 22),
        ];
        for (blosc, zstd) in expected {
            assert_eq!(
                blosc_clevel_to_zstd(blosc),
                zstd,
                "blosc {blosc} must map to zstd {zstd}"
            );
        }
    }

    #[test]
    fn lz4_acceleration_matches_current_c_blosc2_fast_mode() {
        // c-blosc2/blosc/blosc2.c lz4_wrap_compress overrides get_accel's
        // 10 - clevel value to 1 in the non-IPP implementation.
        let expected = [
            (0, 1),
            (1, 1),
            (2, 1),
            (3, 1),
            (4, 1),
            (5, 1),
            (6, 1),
            (7, 1),
            (8, 1),
            (9, 1),
        ];
        for (clevel, accel) in expected {
            assert_eq!(lz4_acceleration(clevel), accel);
        }
    }

    #[test]
    fn lz4_fast_paths_roundtrip_with_clevel_acceleration() {
        let data = b"abcdefghijklmnopabcdefghZZZZabcdefghijklmnopabcdefghijklmnop";
        let dict = b"abcdefghijklmnop0123456789abcdefghijklmnop0123456789";

        let mut low = vec![0; 256];
        let mut high = vec![0; 256];
        let low_size = lz4_compress(1, data, &mut low);
        let high_size = lz4_compress(9, data, &mut high);
        assert!(low_size > 0);
        assert!(high_size > 0);
        let mut restored = vec![0; data.len()];
        assert_eq!(
            lz4_decompress(&low[..low_size as usize], &mut restored),
            data.len() as i32
        );
        assert_eq!(restored, data);
        restored.fill(0);
        assert_eq!(
            lz4_decompress(&high[..high_size as usize], &mut restored),
            data.len() as i32
        );
        assert_eq!(restored, data);

        low.fill(0);
        high.fill(0);
        let low_size = lz4_compress_with_dict(1, data, &mut low, dict);
        let high_size = lz4_compress_with_dict(9, data, &mut high, dict);
        assert!(low_size > 0);
        assert!(high_size > 0);
        restored.fill(0);
        assert_eq!(
            lz4_decompress_with_dict(&low[..low_size as usize], &mut restored, dict),
            data.len() as i32
        );
        assert_eq!(restored, data);
        restored.fill(0);
        assert_eq!(
            lz4_decompress_with_dict(&high[..high_size as usize], &mut restored, dict),
            data.len() as i32
        );
        assert_eq!(restored, data);
    }

    #[test]
    fn lz4hc_size_guard_returns_c_2gb_limit_sentinel() {
        let c_lz4hc_limit = 2usize << 30;
        assert_eq!(lz4hc_2gb_limit_result(c_lz4hc_limit), None);
        assert_eq!(
            lz4hc_2gb_limit_result(c_lz4hc_limit + 1),
            Some(BLOSC2_ERROR_2GB_LIMIT)
        );
    }

    #[test]
    fn zlib_decompress_returns_actual_size_for_oversized_output_like_c() {
        let data = b"zlib payload";
        let mut compressed = vec![0; 128];
        let cbytes = zlib_compress(data, &mut compressed, 5);
        assert!(cbytes > 0);

        let mut decoded = vec![0xaa; data.len() + 8];
        assert_eq!(
            zlib_decompress(&compressed[..cbytes as usize], &mut decoded),
            data.len() as i32
        );
        assert_eq!(&decoded[..data.len()], data);
        assert_eq!(&decoded[data.len()..], &[0xaa; 8]);
    }

    #[test]
    fn zstd_decompress_returns_actual_size_for_oversized_output_like_c() {
        let data = b"zstd payload";
        let mut compressed = vec![0; 128];
        let cbytes = zstd_compress(data, &mut compressed, 5);
        assert!(cbytes > 0);

        let mut decoded = vec![0xaa; data.len() + 8];
        assert_eq!(
            zstd_decompress(&compressed[..cbytes as usize], &mut decoded),
            data.len() as i32
        );
        assert_eq!(&decoded[..data.len()], data);
        assert_eq!(&decoded[data.len()..], &[0xaa; 8]);
    }

    #[test]
    fn zstd_dict_decompress_returns_actual_size_for_oversized_output_like_c() {
        let dict = b"payload dictionary payload dictionary";
        let data = b"dictionary payload";
        let mut compressed = vec![0; 128];
        let cbytes = zstd_compress_with_dict(data, &mut compressed, 5, dict);
        assert!(cbytes > 0);

        let mut decoded = vec![0xaa; data.len() + 8];
        assert_eq!(
            zstd_decompress_with_dict(&compressed[..cbytes as usize], &mut decoded, dict),
            data.len() as i32
        );
        assert_eq!(&decoded[..data.len()], data);
        assert_eq!(&decoded[data.len()..], &[0xaa; 8]);
    }

    #[test]
    fn known_global_codec_metadata_matches_c_registry() {
        for (code, _name) in [
            (BLOSC_CODEC_NDLZ, "ndlz"),
            (BLOSC_CODEC_ZFP_FIXED_ACCURACY, "zfp_acc"),
            (BLOSC_CODEC_ZFP_FIXED_PRECISION, "zfp_prec"),
            (BLOSC_CODEC_ZFP_FIXED_RATE, "zfp_rate"),
            (BLOSC_CODEC_OPENHTJ2K, "openhtj2k"),
            (BLOSC_CODEC_GROK, "grok"),
            (BLOSC_CODEC_OPENZL, "openzl"),
        ] {
            assert!(is_known_global_codec(code));
        }
        for (code, name) in [
            (BLOSC_CODEC_NDLZ, "ndlz"),
            (BLOSC_CODEC_ZFP_FIXED_ACCURACY, "zfp_acc"),
            (BLOSC_CODEC_ZFP_FIXED_PRECISION, "zfp_prec"),
            (BLOSC_CODEC_ZFP_FIXED_RATE, "zfp_rate"),
            (BLOSC_CODEC_OPENHTJ2K, "openhtj2k"),
            (BLOSC_CODEC_GROK, "grok"),
            (BLOSC_CODEC_OPENZL, "openzl"),
        ] {
            assert!(is_registered_codec(code));
            assert_eq!(registered_codec_name(code), Some(name));
            assert_eq!(registered_codec_code(name), Some(code));
            assert_eq!(registered_codec_version(code), Some(1));
            assert_eq!(registered_codec_name_by_complib(code), Some(name));
            assert_eq!(
                registered_codec_complib_info(name),
                Some((code, name, "unknown"))
            );
        }
    }

    #[test]
    fn unimplemented_or_missing_plugin_codecs_return_c_support_error() {
        let mut compressed = vec![0u8; 128];
        let mut decompressed = vec![0u8; 128];

        assert_eq!(
            compress_block(BLOSC_CODEC_OPENHTJ2K, 5, b"payload", &mut compressed),
            BLOSC2_ERROR_CODEC_SUPPORT
        );
        assert_eq!(
            decompress_block(BLOSC_CODEC_OPENHTJ2K, b"payload", &mut decompressed),
            BLOSC2_ERROR_CODEC_SUPPORT
        );
        assert_eq!(
            compress_block(250, 5, b"payload", &mut compressed),
            BLOSC2_ERROR_CODEC_SUPPORT
        );
        assert_eq!(
            decompress_block(250, b"payload", &mut decompressed),
            BLOSC2_ERROR_CODEC_SUPPORT
        );
    }

    fn passthrough_codec_compress(_clevel: u8, _meta: u8, src: &[u8], dest: &mut [u8]) -> i32 {
        if dest.len() < src.len() {
            return 0;
        }
        dest[..src.len()].copy_from_slice(src);
        src.len() as i32
    }

    fn passthrough_codec_decompress(_meta: u8, src: &[u8], dest: &mut [u8]) -> i32 {
        if dest.len() < src.len() {
            return 0;
        }
        dest[..src.len()].copy_from_slice(src);
        src.len() as i32
    }

    fn short_codec_compress(_clevel: u8, _meta: u8, src: &[u8], dest: &mut [u8]) -> i32 {
        if src.is_empty() || dest.is_empty() {
            return 0;
        }
        dest[0] = src[0];
        1
    }

    fn short_codec_decompress(_meta: u8, src: &[u8], dest: &mut [u8]) -> i32 {
        if src.is_empty() || dest.is_empty() {
            return 0;
        }
        dest[0] = src[0];
        1
    }

    fn negative_codec_compress(_clevel: u8, _meta: u8, _src: &[u8], _dest: &mut [u8]) -> i32 {
        -1
    }

    fn oversized_codec_compress(_clevel: u8, _meta: u8, _src: &[u8], dest: &mut [u8]) -> i32 {
        dest.len() as i32 + 1
    }

    fn exact_codec_decompress(_meta: u8, src: &[u8], dest: &mut [u8]) -> i32 {
        for (index, byte) in dest.iter_mut().enumerate() {
            *byte = src.get(index).copied().unwrap_or(0);
        }
        dest.len() as i32
    }

    fn negative_codec_decompress(_meta: u8, _src: &[u8], _dest: &mut [u8]) -> i32 {
        -1
    }

    fn oversized_codec_decompress(_meta: u8, _src: &[u8], dest: &mut [u8]) -> i32 {
        dest.len() as i32 + 1
    }

    fn context_passthrough_codec_compress(
        _ctx: &mut CodecCallbackContext<'_>,
        src: &[u8],
        dest: &mut [u8],
    ) -> i32 {
        if dest.len() < src.len() {
            return 0;
        }
        dest[..src.len()].copy_from_slice(src);
        src.len() as i32
    }

    fn context_passthrough_codec_decompress(
        _ctx: &mut CodecCallbackContext<'_>,
        src: &[u8],
        dest: &mut [u8],
    ) -> i32 {
        if dest.len() < src.len() {
            return 0;
        }
        dest[..src.len()].copy_from_slice(src);
        src.len() as i32
    }

    fn codec_test_context<'a>(b2nd_metalayer: Option<&'a [u8]>) -> CodecCallbackContext<'a> {
        CodecCallbackContext {
            compcode: BLOSC_CODEC_NDLZ,
            complib: None,
            meta: 4,
            clevel: 5,
            cparams: None,
            dparams: None,
            chunk: CodecChunkContext {
                schunk: 0,
                nchunk: -1,
                nblock: -1,
                chunk_source: 0,
                block_offset: 0,
                blocksize: 0,
                bsize: 0,
            },
            b2nd_metalayer,
            user_data: 0,
        }
    }

    #[cfg(feature = "plugin-zfp")]
    fn zfp_test_chunk_context() -> CodecChunkContext {
        CodecChunkContext {
            schunk: 1,
            nchunk: 0,
            nblock: 0,
            chunk_source: 0,
            block_offset: 0,
            blocksize: 64,
            bsize: 64,
        }
    }

    #[test]
    fn user_codec_callbacks_receive_existing_destination_bytes_like_c() {
        const COMPRESS_CODE: u8 = 240;
        const DECOMPRESS_CODE: u8 = 241;

        register_codec(
            COMPRESS_CODE,
            short_codec_compress,
            passthrough_codec_decompress,
        )
        .unwrap();
        let mut compressed = vec![0xaa; 4];
        assert_eq!(
            compress_block(COMPRESS_CODE, 5, &[0x11, 0x22], &mut compressed),
            1
        );
        assert_eq!(compressed, vec![0x11, 0xaa, 0xaa, 0xaa]);

        register_codec(
            DECOMPRESS_CODE,
            passthrough_codec_compress,
            short_codec_decompress,
        )
        .unwrap();
        let mut decompressed = vec![0xbb; 4];
        assert_eq!(
            decompress_block(DECOMPRESS_CODE, &[0x33, 0x44], &mut decompressed),
            BLOSC2_ERROR_DATA
        );
        assert_eq!(decompressed, vec![0x33, 0xbb, 0xbb, 0xbb]);
    }

    #[test]
    fn user_codec_compress_callback_results_match_c() {
        const NEGATIVE_CODE: u8 = 242;
        const OVERSIZED_CODE: u8 = 243;

        register_codec(
            NEGATIVE_CODE,
            negative_codec_compress,
            passthrough_codec_decompress,
        )
        .unwrap();
        let mut compressed = vec![0; 4];
        assert_eq!(
            compress_block(NEGATIVE_CODE, 5, b"payload", &mut compressed),
            BLOSC2_ERROR_DATA
        );

        register_codec(
            OVERSIZED_CODE,
            oversized_codec_compress,
            passthrough_codec_decompress,
        )
        .unwrap();
        assert_eq!(
            compress_block(OVERSIZED_CODE, 5, b"payload", &mut compressed),
            BLOSC2_ERROR_WRITE_BUFFER
        );
    }

    #[test]
    fn user_codec_decompress_callback_results_match_c() {
        const EXACT_CODE: u8 = 244;
        const NEGATIVE_CODE: u8 = 245;
        const OVERSIZED_CODE: u8 = 246;

        register_codec(
            EXACT_CODE,
            passthrough_codec_compress,
            exact_codec_decompress,
        )
        .unwrap();
        let mut decompressed = vec![0; 4];
        assert_eq!(
            decompress_block(EXACT_CODE, b"ab", &mut decompressed),
            decompressed.len() as i32
        );
        assert_eq!(&decompressed, b"ab\0\0");

        register_codec(
            NEGATIVE_CODE,
            passthrough_codec_compress,
            negative_codec_decompress,
        )
        .unwrap();
        assert_eq!(
            decompress_block(NEGATIVE_CODE, b"ab", &mut decompressed),
            BLOSC2_ERROR_DATA
        );

        register_codec(
            OVERSIZED_CODE,
            passthrough_codec_compress,
            oversized_codec_decompress,
        )
        .unwrap();
        assert_eq!(
            decompress_block(OVERSIZED_CODE, b"ab", &mut decompressed),
            BLOSC2_ERROR_DATA
        );
    }

    #[test]
    fn user_codec_decompress_rejects_i32_oversized_dest_len() {
        // A public decompress_block test would require constructing a valid
        // >2 GiB mutable slice. Cover the same C-contract boundary directly;
        // real Blosc block sizes are int32-bounded before codec callbacks.
        assert_eq!(
            normalize_user_decompress_result(0, i32::MAX as usize + 1),
            BLOSC2_ERROR_DATA
        );
    }

    #[test]
    fn blosc2_codec_registration_rejects_builtin_and_global_ids() {
        let global_codec = Blosc2Codec {
            compcode: 39,
            compname: "public-global-id-rejected",
            complib: 39,
            version: 1,
            encoder: passthrough_codec_compress,
            decoder: passthrough_codec_decompress,
        };
        assert_eq!(
            blosc2_register_codec(&global_codec),
            BLOSC2_ERROR_CODEC_PARAM
        );
        assert_eq!(
            blosc2_register_codec_c(Some(&global_codec)),
            BLOSC2_ERROR_CODEC_PARAM
        );

        let user_codec = Blosc2Codec {
            compcode: 247,
            compname: "public-user-id-accepted",
            complib: 247,
            version: 1,
            encoder: passthrough_codec_compress,
            decoder: passthrough_codec_decompress,
        };
        assert_eq!(blosc2_register_codec(&user_codec), BLOSC2_ERROR_SUCCESS);

        assert_eq!(
            blosc2_register_codec_c(Some(&user_codec)),
            BLOSC2_ERROR_SUCCESS
        );

        let builtin_codec = Blosc2Codec {
            compcode: BLOSC_LZ4,
            compname: "builtin-id-rejected",
            complib: BLOSC_LZ4,
            version: 1,
            encoder: passthrough_codec_compress,
            decoder: passthrough_codec_decompress,
        };
        assert_eq!(
            blosc2_register_codec(&builtin_codec),
            BLOSC2_ERROR_CODEC_PARAM
        );
    }

    #[test]
    fn known_global_codec_registration_is_descriptor_idempotent_without_dispatch() {
        let mut compressed = vec![0; 32];
        #[cfg(feature = "plugin-zfp")]
        let unsupported_without_static_plugin = 0;
        #[cfg(not(feature = "plugin-zfp"))]
        let unsupported_without_static_plugin = BLOSC2_ERROR_CODEC_SUPPORT;

        assert_eq!(
            compress_block(
                BLOSC_CODEC_ZFP_FIXED_ACCURACY,
                5,
                b"payload",
                &mut compressed
            ),
            unsupported_without_static_plugin
        );
        assert_eq!(
            registered_codec_name(BLOSC_CODEC_ZFP_FIXED_ACCURACY),
            Some("zfp_acc")
        );

        assert_eq!(
            register_global_codec_with_metadata(
                BLOSC_CODEC_ZFP_FIXED_ACCURACY,
                "zfp_acc",
                BLOSC_CODEC_ZFP_FIXED_ACCURACY,
                1,
                passthrough_codec_compress,
                passthrough_codec_decompress,
            ),
            Ok(())
        );
        assert_eq!(
            register_private_codec_with_metadata(
                BLOSC_CODEC_ZFP_FIXED_ACCURACY,
                "zfp_acc",
                BLOSC_CODEC_ZFP_FIXED_ACCURACY,
                1,
                passthrough_codec_compress,
                passthrough_codec_decompress,
            ),
            Ok(())
        );
        assert_eq!(
            register_global_context_codec_with_metadata(
                BLOSC_CODEC_ZFP_FIXED_ACCURACY,
                "zfp_acc",
                BLOSC_CODEC_ZFP_FIXED_ACCURACY,
                1,
                context_passthrough_codec_compress,
                context_passthrough_codec_decompress,
            ),
            Ok(())
        );

        assert_eq!(
            registered_codec_name(BLOSC_CODEC_ZFP_FIXED_ACCURACY),
            Some("zfp_acc")
        );
        assert_eq!(
            registered_codec_version(BLOSC_CODEC_ZFP_FIXED_ACCURACY),
            Some(1)
        );
        assert_eq!(
            registered_codec_complib_info("zfp_acc"),
            Some((BLOSC_CODEC_ZFP_FIXED_ACCURACY, "zfp_acc", "unknown"))
        );
        assert_eq!(
            compress_block(
                BLOSC_CODEC_ZFP_FIXED_ACCURACY,
                5,
                b"payload",
                &mut compressed
            ),
            unsupported_without_static_plugin
        );
        let mut decompressed = vec![0; 7];
        #[cfg(feature = "plugin-zfp")]
        let unsupported_decompress_without_static_plugin = 0;
        #[cfg(not(feature = "plugin-zfp"))]
        let unsupported_decompress_without_static_plugin = BLOSC2_ERROR_CODEC_SUPPORT;
        assert_eq!(
            decompress_block(
                BLOSC_CODEC_ZFP_FIXED_ACCURACY,
                b"payload",
                &mut decompressed
            ),
            unsupported_decompress_without_static_plugin
        );
    }

    #[test]
    fn known_global_codec_registration_is_idempotent_for_builtin_dispatch_ids() {
        assert_eq!(
            register_global_codec_with_metadata(
                BLOSC_CODEC_NDLZ,
                "ndlz",
                BLOSC_CODEC_NDLZ,
                1,
                passthrough_codec_compress,
                passthrough_codec_decompress,
            ),
            Ok(())
        );
    }

    #[test]
    fn blosc2_register_codec_abi_uses_raw_c_callback_shape() {
        let _lock = C_ABI_TEST_LOCK.lock().unwrap();
        const CODE: u8 = 170;
        let codec = Blosc2CodecAbi {
            compcode: CODE,
            compname: b"raw-c-abi-codec\0".as_ptr().cast(),
            complib: CODE,
            version: 3,
            encoder: Some(c_abi_codec_encoder),
            decoder: Some(c_abi_codec_decoder),
        };
        assert_eq!(
            blosc2_register_codec_abi(&codec as *const Blosc2CodecAbi),
            BLOSC2_ERROR_SUCCESS
        );
        assert_eq!(
            blosc2_register_codec_abi(std::ptr::null()),
            BLOSC2_ERROR_INVALID_PARAM
        );
        assert_eq!(registered_codec_name(CODE), Some("raw-c-abi-codec"));
        assert_eq!(registered_codec_version(CODE), Some(3));

        C_ABI_COMPRESS_INPUT_LEN.store(0, Ordering::SeqCst);
        C_ABI_DECOMPRESS_INPUT_LEN.store(0, Ordering::SeqCst);
        let mut source_chunk = vec![0u8; 128];
        source_chunk.extend_from_slice(b"c-abi payload");
        let src = &source_chunk[128..];
        let filtered_block = src.to_vec();
        let mut encoded = vec![0; 64];
        let cparams = CodecCParamsContext {
            compcode: CODE,
            compcode_meta: 0x5a,
            clevel: 7,
            use_dict: 1,
            typesize: 4,
            blocksize: 0,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [BLOSC_NOFILTER; BLOSC2_MAX_FILTERS],
            filters_meta: [0; BLOSC2_MAX_FILTERS],
            nthreads: 2,
            nchunk: 33,
            user_data: 0xfeed,
            instr_codec: true,
            codec_params: 0xc0de,
        };
        let chunk = CodecChunkContext {
            schunk: 0x1234,
            nchunk: 33,
            nblock: 4,
            chunk_source: source_chunk.as_ptr() as usize,
            block_offset: 128,
            blocksize: 64,
            bsize: src.len(),
        };
        let cbytes = compress_block_with_context(
            CODE,
            7,
            0x5a,
            &filtered_block,
            &mut encoded,
            Some(CodecCallbackContext {
                compcode: CODE,
                complib: Some(CODE),
                meta: 0x5a,
                clevel: 7,
                cparams: Some(&cparams),
                dparams: None,
                chunk,
                b2nd_metalayer: None,
                user_data: 0xfeed,
            }),
        );
        assert_eq!(cbytes, src.len() as i32);
        assert_eq!(&encoded[..src.len()], src);
        assert_eq!(
            C_ABI_COMPRESS_INPUT_LEN.load(Ordering::SeqCst),
            src.len() as i32
        );
        assert_eq!(
            C_ABI_COMPRESS_INPUT_PTR.load(Ordering::SeqCst),
            filtered_block.as_ptr() as usize
        );
        assert_eq!(C_ABI_COMPRESS_OUTPUT_LEN.load(Ordering::SeqCst), 64);
        assert_eq!(C_ABI_COMPRESS_META.load(Ordering::SeqCst), 0x5a);
        assert_eq!(C_ABI_COMPRESS_CLEVEL.load(Ordering::SeqCst), 7);
        assert_eq!(C_ABI_COMPRESS_COMP_META.load(Ordering::SeqCst), 0x5a);
        assert_eq!(C_ABI_COMPRESS_USE_DICT.load(Ordering::SeqCst), 1);
        assert_eq!(C_ABI_COMPRESS_TYPESIZE.load(Ordering::SeqCst), 4);
        assert_eq!(C_ABI_COMPRESS_BLOCKSIZE.load(Ordering::SeqCst), 0);
        assert_eq!(
            C_ABI_COMPRESS_SPLITMODE.load(Ordering::SeqCst),
            BLOSC_NEVER_SPLIT
        );
        assert_eq!(
            C_ABI_COMPRESS_FILTER_LAST.load(Ordering::SeqCst),
            BLOSC_NOFILTER
        );
        assert_eq!(C_ABI_COMPRESS_SCHUNK.load(Ordering::SeqCst), 0x1234);
        assert_eq!(
            C_ABI_COMPRESS_CHUNK.load(Ordering::SeqCst),
            source_chunk.as_ptr() as usize
        );
        assert!(!C_ABI_COMPRESS_PREFILTER_SET.load(Ordering::SeqCst));
        assert_eq!(C_ABI_COMPRESS_PREPARAMS.load(Ordering::SeqCst), 0);
        assert_eq!(C_ABI_COMPRESS_TUNER_ID.load(Ordering::SeqCst), 0);
        assert!(C_ABI_COMPRESS_INSTR_CODEC.load(Ordering::SeqCst));
        assert_eq!(C_ABI_COMPRESS_CODEC_PARAMS.load(Ordering::SeqCst), 0xc0de);

        let dparams = CodecDParamsContext {
            nthreads: 3,
            typesize: 4,
            nchunk: 34,
            user_data: 0xbeef,
        };
        let mut decoded = vec![0; src.len()];
        let mut compressed_chunk_bytes = vec![0u8; 128];
        compressed_chunk_bytes.extend_from_slice(&encoded[..cbytes as usize]);
        let compressed_block = encoded[..cbytes as usize].to_vec();
        assert_eq!(
            decompress_block_with_context(
                CODE,
                0x5a,
                &compressed_block,
                &mut decoded,
                Some(CodecCallbackContext {
                    compcode: CODE,
                    complib: Some(CODE),
                    meta: 0x5a,
                    clevel: 0,
                    cparams: None,
                    dparams: Some(&dparams),
                    chunk: CodecChunkContext {
                        nchunk: 34,
                        chunk_source: compressed_chunk_bytes.as_ptr() as usize,
                        ..chunk
                    },
                    b2nd_metalayer: None,
                    user_data: 0xbeef,
                }),
            ),
            src.len() as i32
        );
        assert_eq!(decoded, src);
        assert_eq!(
            C_ABI_DECOMPRESS_INPUT_LEN.load(Ordering::SeqCst),
            src.len() as i32
        );
        assert_eq!(
            C_ABI_DECOMPRESS_INPUT_PTR.load(Ordering::SeqCst),
            compressed_block.as_ptr() as usize
        );
        assert_eq!(
            C_ABI_DECOMPRESS_OUTPUT_LEN.load(Ordering::SeqCst),
            src.len() as i32
        );
        assert_eq!(C_ABI_DECOMPRESS_META.load(Ordering::SeqCst), 0x5a);
        assert_eq!(C_ABI_DECOMPRESS_TYPESIZE.load(Ordering::SeqCst), 4);
        assert_eq!(C_ABI_DECOMPRESS_SCHUNK.load(Ordering::SeqCst), 0x1234);
        assert_eq!(
            C_ABI_DECOMPRESS_CHUNK.load(Ordering::SeqCst),
            compressed_chunk_bytes.as_ptr() as usize
        );
        assert!(!C_ABI_DECOMPRESS_POSTFILTER_SET.load(Ordering::SeqCst));
        assert_eq!(C_ABI_DECOMPRESS_POSTPARAMS.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn blosc2_register_codec_abi_fallback_params_match_c_defaults() {
        let _lock = C_ABI_TEST_LOCK.lock().unwrap();
        const CODE: u8 = 171;
        let codec = Blosc2CodecAbi {
            compcode: CODE,
            compname: b"raw-c-abi-defaults\0".as_ptr().cast(),
            complib: CODE,
            version: 1,
            encoder: Some(c_abi_codec_encoder),
            decoder: Some(c_abi_codec_decoder),
        };
        assert_eq!(
            blosc2_register_codec_abi(&codec as *const Blosc2CodecAbi),
            BLOSC2_ERROR_SUCCESS
        );

        let src = b"default params";
        let mut encoded = vec![0; 64];
        C_ABI_COMPRESS_CHUNK.store(usize::MAX, Ordering::SeqCst);
        assert_eq!(
            compress_block_with_meta(CODE, 6, 0x23, src, &mut encoded),
            src.len() as i32
        );
        assert_eq!(
            C_ABI_COMPRESS_CHUNK.load(Ordering::SeqCst),
            src.as_ptr() as usize
        );
        assert_eq!(C_ABI_COMPRESS_META.load(Ordering::SeqCst), 0x23);
        assert_eq!(C_ABI_COMPRESS_CLEVEL.load(Ordering::SeqCst), 6);
        assert_eq!(C_ABI_COMPRESS_USE_DICT.load(Ordering::SeqCst), 0);
        assert_eq!(C_ABI_COMPRESS_TYPESIZE.load(Ordering::SeqCst), 8);
        assert_eq!(C_ABI_COMPRESS_BLOCKSIZE.load(Ordering::SeqCst), 0);
        assert_eq!(
            C_ABI_COMPRESS_SPLITMODE.load(Ordering::SeqCst),
            BLOSC_FORWARD_COMPAT_SPLIT
        );
        assert_eq!(
            C_ABI_COMPRESS_FILTER_LAST.load(Ordering::SeqCst),
            BLOSC_SHUFFLE
        );
        assert!(!C_ABI_COMPRESS_PREFILTER_SET.load(Ordering::SeqCst));
        assert_eq!(C_ABI_COMPRESS_PREPARAMS.load(Ordering::SeqCst), 0);
        assert_eq!(C_ABI_COMPRESS_TUNER_ID.load(Ordering::SeqCst), 0);
        assert!(!C_ABI_COMPRESS_INSTR_CODEC.load(Ordering::SeqCst));
        assert_eq!(C_ABI_COMPRESS_CODEC_PARAMS.load(Ordering::SeqCst), 0);

        let mut decoded = vec![0; src.len()];
        C_ABI_DECOMPRESS_CHUNK.store(usize::MAX, Ordering::SeqCst);
        assert_eq!(
            decompress_block_with_meta(CODE, 0x23, &encoded[..src.len()], &mut decoded),
            src.len() as i32
        );
        assert_eq!(decoded, src);
        assert_eq!(
            C_ABI_DECOMPRESS_CHUNK.load(Ordering::SeqCst),
            encoded.as_ptr() as usize
        );
        assert_eq!(C_ABI_DECOMPRESS_TYPESIZE.load(Ordering::SeqCst), 8);
        assert!(!C_ABI_DECOMPRESS_POSTFILTER_SET.load(Ordering::SeqCst));
        assert_eq!(C_ABI_DECOMPRESS_POSTPARAMS.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn blosc2_register_codec_abi_null_chunk_source_fallback_is_null() {
        let _lock = C_ABI_TEST_LOCK.lock().unwrap();
        const CODE: u8 = 172;
        let codec = Blosc2CodecAbi {
            compcode: CODE,
            compname: b"raw-c-abi-null-chunk-fallback\0".as_ptr().cast(),
            complib: CODE,
            version: 1,
            encoder: Some(c_abi_codec_encoder),
            decoder: Some(c_abi_codec_decoder),
        };
        assert_eq!(
            blosc2_register_codec_abi(&codec as *const Blosc2CodecAbi),
            BLOSC2_ERROR_SUCCESS
        );

        let src = b"null chunk";
        let mut encoded = vec![0; 32];
        C_ABI_COMPRESS_CHUNK.store(usize::MAX, Ordering::SeqCst);
        assert_eq!(
            compress_block_with_context(
                CODE,
                5,
                0,
                src,
                &mut encoded,
                Some(CodecCallbackContext {
                    compcode: CODE,
                    complib: Some(CODE),
                    meta: 0,
                    clevel: 5,
                    cparams: None,
                    dparams: None,
                    chunk: CodecChunkContext {
                        schunk: 0,
                        nchunk: -1,
                        nblock: 0,
                        chunk_source: 0,
                        block_offset: src.as_ptr() as usize,
                        blocksize: src.len(),
                        bsize: src.len(),
                    },
                    b2nd_metalayer: None,
                    user_data: 0,
                }),
            ),
            src.len() as i32
        );
        assert_eq!(C_ABI_COMPRESS_CHUNK.load(Ordering::SeqCst), 0);

        let mut decoded = vec![0; src.len()];
        C_ABI_DECOMPRESS_CHUNK.store(usize::MAX, Ordering::SeqCst);
        assert_eq!(
            decompress_block_with_context(
                CODE,
                0,
                &encoded[..src.len()],
                &mut decoded,
                Some(CodecCallbackContext {
                    compcode: CODE,
                    complib: Some(CODE),
                    meta: 0,
                    clevel: 0,
                    cparams: None,
                    dparams: None,
                    chunk: CodecChunkContext {
                        schunk: 0,
                        nchunk: -1,
                        nblock: 0,
                        chunk_source: 0,
                        block_offset: encoded.as_ptr() as usize,
                        blocksize: src.len(),
                        bsize: src.len(),
                    },
                    b2nd_metalayer: None,
                    user_data: 0,
                }),
            ),
            src.len() as i32
        );
        assert_eq!(decoded, src);
        assert_eq!(C_ABI_DECOMPRESS_CHUNK.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn blosc2_register_codec_abi_null_callbacks_return_codec_support() {
        let _lock = C_ABI_TEST_LOCK.lock().unwrap();
        const NULL_ENCODER_CODE: u8 = 173;
        let null_encoder = Blosc2CodecAbi {
            compcode: NULL_ENCODER_CODE,
            compname: b"raw-c-abi-null-encoder\0".as_ptr().cast(),
            complib: NULL_ENCODER_CODE,
            version: 1,
            encoder: None,
            decoder: Some(c_abi_codec_decoder),
        };
        assert_eq!(
            blosc2_register_codec_abi(&null_encoder as *const Blosc2CodecAbi),
            BLOSC2_ERROR_SUCCESS
        );
        let mut encoded = vec![0; 32];
        assert_eq!(
            compress_block(NULL_ENCODER_CODE, 5, b"payload", &mut encoded),
            BLOSC2_ERROR_CODEC_SUPPORT
        );

        const NULL_DECODER_CODE: u8 = 174;
        let null_decoder = Blosc2CodecAbi {
            compcode: NULL_DECODER_CODE,
            compname: b"raw-c-abi-null-decoder\0".as_ptr().cast(),
            complib: NULL_DECODER_CODE,
            version: 1,
            encoder: Some(c_abi_codec_encoder),
            decoder: None,
        };
        assert_eq!(
            blosc2_register_codec_abi(&null_decoder as *const Blosc2CodecAbi),
            BLOSC2_ERROR_SUCCESS
        );
        let mut decoded = vec![0; 7];
        assert_eq!(
            decompress_block(NULL_DECODER_CODE, b"payload", &mut decoded),
            BLOSC2_ERROR_CODEC_SUPPORT
        );
    }

    #[test]
    fn blosc2_register_codec_abi_rejects_null_name_and_parses_empty_name() {
        let _lock = C_ABI_TEST_LOCK.lock().unwrap();
        const CODE: u8 = 175;
        let null_name = Blosc2CodecAbi {
            compcode: CODE,
            compname: std::ptr::null(),
            complib: CODE,
            version: 1,
            encoder: Some(c_abi_codec_encoder),
            decoder: Some(c_abi_codec_decoder),
        };
        assert_eq!(
            blosc2_register_codec_abi(&null_name as *const Blosc2CodecAbi),
            BLOSC2_ERROR_INVALID_PARAM
        );
        assert_eq!(blosc2_codec_abi_name(b"\0".as_ptr().cast()), Ok(""));
    }

    #[test]
    fn blosc2_register_codec_abi_duplicate_id_matches_c_name_idempotence() {
        let _lock = C_ABI_TEST_LOCK.lock().unwrap();
        const CODE: u8 = 176;
        let codec = Blosc2CodecAbi {
            compcode: CODE,
            compname: b"raw-c-abi-duplicate-id\0".as_ptr().cast(),
            complib: CODE,
            version: 1,
            encoder: Some(c_abi_codec_encoder),
            decoder: Some(c_abi_codec_decoder),
        };
        assert_eq!(
            blosc2_register_codec_abi(&codec as *const Blosc2CodecAbi),
            BLOSC2_ERROR_SUCCESS
        );
        assert_eq!(
            blosc2_register_codec_abi(&codec as *const Blosc2CodecAbi),
            BLOSC2_ERROR_SUCCESS
        );

        let same_callbacks_different_name = Blosc2CodecAbi {
            compname: b"raw-c-abi-other-name\0".as_ptr().cast(),
            ..codec
        };
        assert_eq!(
            blosc2_register_codec_abi(&same_callbacks_different_name as *const Blosc2CodecAbi),
            BLOSC2_ERROR_CODEC_PARAM
        );
        assert_eq!(registered_codec_name(CODE), Some("raw-c-abi-duplicate-id"));
    }

    #[test]
    fn known_global_codec_registration_rejects_different_or_missing_names() {
        assert_eq!(
            register_global_codec(
                BLOSC_CODEC_ZFP_FIXED_PRECISION,
                passthrough_codec_compress,
                passthrough_codec_decompress,
            ),
            Err("Global plugin codec ID already registered")
        );
        assert_eq!(
            register_global_codec_with_metadata(
                BLOSC_CODEC_ZFP_FIXED_PRECISION,
                "not-zfp-prec",
                BLOSC_CODEC_ZFP_FIXED_PRECISION,
                1,
                passthrough_codec_compress,
                passthrough_codec_decompress,
            ),
            Err("Global plugin codec ID already registered")
        );
        assert_eq!(
            register_private_codec_with_metadata(
                BLOSC_CODEC_ZFP_FIXED_PRECISION,
                "not-zfp-prec",
                BLOSC_CODEC_ZFP_FIXED_PRECISION,
                1,
                passthrough_codec_compress,
                passthrough_codec_decompress,
            ),
            Err("Private codec ID already registered")
        );
        assert_eq!(
            register_global_context_codec(
                BLOSC_CODEC_ZFP_FIXED_PRECISION,
                context_passthrough_codec_compress,
                context_passthrough_codec_decompress,
            ),
            Err("Global plugin codec ID already registered")
        );
    }

    #[test]
    fn internal_global_codec_registration_accepts_global_ids_like_c_private_path() {
        const CODE: u8 = 40;
        assert_eq!(
            register_global_codec_with_metadata(
                CODE,
                "private-global-codec",
                CODE,
                1,
                passthrough_codec_compress,
                passthrough_codec_decompress,
            ),
            Ok(())
        );
        assert_eq!(registered_codec_name(CODE), Some("private-global-codec"));
        assert_eq!(
            registered_codec_complib_info("private-global-codec"),
            Some((CODE, "private-global-codec", "unknown"))
        );
        assert_eq!(
            register_global_codec_with_metadata(
                CODE,
                "private-global-codec",
                CODE,
                1,
                passthrough_codec_compress,
                passthrough_codec_decompress,
            ),
            Ok(())
        );
    }

    #[test]
    fn unnamed_global_and_private_duplicate_registration_is_rejected() {
        const GLOBAL_CODE: u8 = 42;
        assert_eq!(
            register_global_codec(
                GLOBAL_CODE,
                passthrough_codec_compress,
                passthrough_codec_decompress,
            ),
            Ok(())
        );
        assert_eq!(
            register_global_codec(
                GLOBAL_CODE,
                passthrough_codec_compress,
                passthrough_codec_decompress,
            ),
            Err("Global plugin codec ID already registered")
        );

        const PRIVATE_CODE: u8 = 43;
        assert_eq!(
            register_private_codec(
                PRIVATE_CODE,
                passthrough_codec_compress,
                passthrough_codec_decompress,
            ),
            Ok(())
        );
        assert_eq!(
            register_private_codec(
                PRIVATE_CODE,
                passthrough_codec_compress,
                passthrough_codec_decompress,
            ),
            Err("Private codec ID already registered")
        );
    }

    #[test]
    fn private_codec_registration_accepts_user_ids_like_c_private_path() {
        const CODE: u8 = 251;
        assert_eq!(
            register_global_codec_with_metadata(
                CODE,
                "global-helper-user-id-rejected",
                CODE,
                1,
                passthrough_codec_compress,
                passthrough_codec_decompress,
            ),
            Err("Global plugin codec IDs must be in 32..=159")
        );
        assert_eq!(
            register_private_codec_with_metadata(
                CODE,
                "private-user-range-codec",
                CODE,
                2,
                passthrough_codec_compress,
                passthrough_codec_decompress,
            ),
            Ok(())
        );
        assert!(is_registered_codec(CODE));
        assert_eq!(
            registered_codec_name(CODE),
            Some("private-user-range-codec")
        );
        assert_eq!(
            registered_codec_code("private-user-range-codec"),
            Some(CODE)
        );
        assert_eq!(registered_codec_version(CODE), Some(2));
        assert_eq!(
            registered_codec_complib_info("private-user-range-codec"),
            Some((CODE, "private-user-range-codec", "unknown"))
        );
        assert_eq!(
            register_private_codec(
                BLOSC_LZ4,
                passthrough_codec_compress,
                passthrough_codec_decompress,
            ),
            Err("Private codec IDs must be >= 32")
        );
    }

    #[test]
    fn registered_codec_complib_lookup_prefers_builtin_libraries_like_c() {
        const CODE: u8 = 41;
        register_global_codec_with_metadata(
            CODE,
            "lz4-backed-plugin",
            BLOSC_LZ4_LIB,
            1,
            passthrough_codec_compress,
            passthrough_codec_decompress,
        )
        .unwrap();

        assert_eq!(
            registered_codec_name_by_complib(BLOSC_LZ4_LIB),
            Some(BLOSC_LZ4_LIBNAME)
        );
        assert_eq!(
            registered_codec_complib_info("lz4-backed-plugin"),
            Some((BLOSC_LZ4_LIB, BLOSC_LZ4_LIBNAME, "1.10.0"))
        );
    }

    #[test]
    fn ndlz_compress_null_context_returns_c_fallback_sentinel() {
        let input: Vec<u8> = (1..=16).collect();
        let mut encoded = vec![0; 64];
        assert_eq!(
            compress_block_with_meta(BLOSC_CODEC_NDLZ, 5, 4, &input, &mut encoded),
            0
        );
    }

    #[test]
    fn ndlz_compress_present_context_requires_valid_b2nd_metadata() {
        let input: Vec<u8> = (1..=16).collect();
        let mut encoded = vec![0; 64];

        assert_eq!(
            compress_block_with_context(
                BLOSC_CODEC_NDLZ,
                5,
                4,
                &input,
                &mut encoded,
                Some(codec_test_context(None)),
            ),
            -1
        );
        assert_eq!(
            compress_block_with_context(
                BLOSC_CODEC_NDLZ,
                5,
                4,
                &input,
                &mut encoded,
                Some(codec_test_context(Some(b"invalid-b2nd"))),
            ),
            -1
        );
    }

    #[cfg(feature = "plugin-zfp")]
    #[test]
    fn zfp_fixed_precision_meta_clamps_to_c_max_precision() {
        let config = zfp_config_for_mode(
            BLOSC_CODEC_ZFP_FIXED_PRECISION,
            u8::MAX,
            ZfpScalarType::Float,
            ZfpDimensionality::D4,
        );

        assert_eq!(config.max_prec(), ZFP_MAX_PREC);
    }

    #[cfg(feature = "plugin-zfp")]
    #[test]
    fn zfp_fixed_rate_roundtrip_uses_b2nd_blockshape_context() {
        let b2nd_meta = B2ndMeta::new(vec![4, 4], vec![4, 4], vec![4, 4], "<f4", 0)
            .unwrap()
            .serialize()
            .unwrap();
        let cparams = CodecCParamsContext {
            compcode: BLOSC_CODEC_ZFP_FIXED_RATE,
            compcode_meta: 25,
            clevel: 5,
            use_dict: 0,
            typesize: 4,
            blocksize: 64,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [BLOSC_NOFILTER; BLOSC2_MAX_FILTERS],
            filters_meta: [0; BLOSC2_MAX_FILTERS],
            nthreads: 1,
            nchunk: 0,
            user_data: 0,
            instr_codec: false,
            codec_params: 0,
        };
        let dparams = CodecDParamsContext {
            nthreads: 1,
            typesize: 4,
            nchunk: 0,
            user_data: 0,
        };
        let chunk = zfp_test_chunk_context();
        let input: Vec<u8> = (0..16)
            .flat_map(|i| ((i as f32) * 0.25 + 1.0).to_ne_bytes())
            .collect();
        let mut encoded = vec![0; 128];
        let cbytes = compress_block_with_context(
            BLOSC_CODEC_ZFP_FIXED_RATE,
            5,
            25,
            &input,
            &mut encoded,
            Some(CodecCallbackContext {
                compcode: BLOSC_CODEC_ZFP_FIXED_RATE,
                complib: None,
                meta: 25,
                clevel: 5,
                cparams: Some(&cparams),
                dparams: None,
                chunk,
                b2nd_metalayer: Some(&b2nd_meta),
                user_data: 0,
            }),
        );
        assert!(cbytes > 0 && cbytes < input.len() as i32);

        let mut decoded = vec![0; input.len()];
        let dbytes = decompress_block_with_context(
            BLOSC_CODEC_ZFP_FIXED_RATE,
            25,
            &encoded[..cbytes as usize],
            &mut decoded,
            Some(CodecCallbackContext {
                compcode: BLOSC_CODEC_ZFP_FIXED_RATE,
                complib: None,
                meta: 25,
                clevel: 5,
                cparams: None,
                dparams: Some(&dparams),
                chunk,
                b2nd_metalayer: Some(&b2nd_meta),
                user_data: 0,
            }),
        );
        assert_eq!(dbytes, input.len() as i32);
        assert!(decoded.iter().any(|&byte| byte != 0));
    }

    #[test]
    fn ndlz_literal_blocks_decompress_for_meta_4_and_8() {
        let mut src4 = vec![2];
        src4.extend_from_slice(&3i32.to_le_bytes());
        src4.extend_from_slice(&4i32.to_le_bytes());
        src4.push(0);
        src4.extend_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]);
        let mut out4 = vec![0; 12];
        assert_eq!(
            decompress_block_with_meta(BLOSC_CODEC_NDLZ, 4, &src4, &mut out4),
            12
        );
        assert_eq!(out4, (1..=12).collect::<Vec<_>>());

        let mut src8 = vec![2];
        src8.extend_from_slice(&2i32.to_le_bytes());
        src8.extend_from_slice(&3i32.to_le_bytes());
        src8.push(0);
        src8.extend_from_slice(&[20, 21, 22, 23, 24, 25]);
        let mut out8 = vec![0; 6];
        assert_eq!(
            decompress_block_with_meta(BLOSC_CODEC_NDLZ, 8, &src8, &mut out8),
            6
        );
        assert_eq!(out8, vec![20, 21, 22, 23, 24, 25]);
    }

    #[test]
    fn ndlz_repeat_cells_and_invalid_meta_are_handled() {
        let mut src = vec![2];
        src.extend_from_slice(&4i32.to_le_bytes());
        src.extend_from_slice(&4i32.to_le_bytes());
        src.push(0x40);
        src.push(7);
        let mut out = vec![0; 16];
        assert_eq!(
            decompress_block_with_meta(BLOSC_CODEC_NDLZ, 4, &src, &mut out),
            16
        );
        assert_eq!(out, vec![7; 16]);

        assert_eq!(
            decompress_block_with_meta(BLOSC_CODEC_NDLZ, 5, &src, &mut out),
            -1
        );
    }

    #[test]
    fn ndlz_short_input_returns_c_zero_sentinel() {
        let mut out = vec![0; 16];
        assert_eq!(
            decompress_block_with_meta(BLOSC_CODEC_NDLZ, 4, &[], &mut out),
            0
        );
        assert_eq!(
            decompress_block_with_meta(BLOSC_CODEC_NDLZ, 8, &[2, 0, 0, 0, 0, 0, 0], &mut out),
            0
        );
        assert_eq!(
            decompress_block_with_meta(BLOSC_CODEC_NDLZ, 4, &[2, 0, 0, 0, 0, 0, 0, 0], &mut out),
            -1
        );
    }

    #[test]
    fn ndlz_literal_encoder_returns_c_fallback_for_oversized_blocks() {
        for (meta, blockshape, input) in [
            (4, [4, 4], (1..=16).collect::<Vec<_>>()),
            (8, [8, 8], (20..84).collect::<Vec<_>>()),
        ] {
            let mut encoded = vec![0; input.len() + 32];
            let cbytes = compress_ndlz_2d_block(meta, blockshape, &input, &mut encoded);
            assert_eq!(cbytes, 0);
        }
    }

    #[test]
    fn ndlz_encoder_returns_zero_for_valid_fallback_cases() {
        let small_input = vec![1; 12];
        let mut encoded = vec![0; 64];
        assert_eq!(
            compress_ndlz_2d_block(4, [3, 4], &small_input, &mut encoded),
            0
        );
        let mut no_header_space = vec![0; 8];
        assert_eq!(
            compress_ndlz_2d_block(4, [3, 4], &small_input, &mut no_header_space),
            -1
        );

        let input: Vec<u8> = (1..=16).collect();
        assert_eq!(
            compress_ndlz_2d_block(4, [4, 4], &input, &mut no_header_space),
            -1
        );
        let mut too_small_output = vec![0; 16];
        assert_eq!(
            compress_ndlz_2d_block(4, [4, 4], &input, &mut too_small_output),
            0
        );
        assert_eq!(compress_ndlz_2d_block(4, [4, 4], &input, &mut encoded), 0);
        let padded_input: Vec<u8> = (1..=20).collect();
        assert_eq!(
            compress_ndlz_2d_block(4, [4, 5], &padded_input, &mut encoded),
            0
        );

        let mut encoded = vec![0; 64];
        assert_eq!(compress_ndlz_2d_block(5, [4, 4], &input, &mut encoded), -1);
        assert_eq!(
            compress_ndlz_2d_block(4, [4, 4], &input[..15], &mut encoded),
            -1
        );
        assert_eq!(compress_ndlz_2d_block(4, [-1, 4], &input, &mut encoded), -1);
    }

    #[test]
    fn ndlz_encoder_applies_c_worst_case_cell_guard_before_each_cell() {
        let mut input = vec![7; 20];
        input[16..].copy_from_slice(&[1, 2, 3, 4]);
        let mut encoded = vec![0; 27];

        assert_eq!(compress_ndlz_2d_block(4, [4, 5], &input, &mut encoded), 0);
    }

    #[test]
    fn ndlz_encoder_uses_repeat_cell_token_for_full_constant_cells() {
        let input = vec![9; 16];
        let mut encoded = vec![0; 64];
        let cbytes = compress_ndlz_2d_block(4, [4, 4], &input, &mut encoded);
        assert_eq!(cbytes, 11);
        assert_eq!(&encoded[..11], &[2, 4, 0, 0, 0, 4, 0, 0, 0, 0x40, 9]);

        let mut decoded = vec![0; input.len()];
        assert_eq!(
            decompress_block_with_meta(
                BLOSC_CODEC_NDLZ,
                4,
                &encoded[..cbytes as usize],
                &mut decoded,
            ),
            input.len() as i32
        );
        assert_eq!(decoded, input);
    }

    #[test]
    fn ndlz_encoder_uses_full_cell_match_tokens_for_repeated_cells() {
        let first_cell: Vec<u8> = (1..=16).collect();
        let mut input = Vec::with_capacity(32);
        for row in 0..4 {
            input.extend_from_slice(&first_cell[row * 4..row * 4 + 4]);
            input.extend_from_slice(&first_cell[row * 4..row * 4 + 4]);
        }

        let mut encoded = vec![0; 96];
        let cbytes = compress_ndlz_2d_block(4, [4, 8], &input, &mut encoded);
        assert_eq!(cbytes, 29);
        assert_eq!(encoded[9], 0);
        assert_eq!(encoded[26], 0xc0);
        assert_eq!(u16::from_le_bytes([encoded[27], encoded[28]]), 16);

        let mut decoded = vec![0; input.len()];
        assert_eq!(
            decompress_block_with_meta(
                BLOSC_CODEC_NDLZ,
                4,
                &encoded[..cbytes as usize],
                &mut decoded,
            ),
            input.len() as i32
        );
        assert_eq!(decoded, input);
    }

    #[test]
    fn ndlz_encoder_uses_full_cell_match_tokens_for_repeated_8x8_cells() {
        let first_cell: Vec<u8> = (0..64).map(|i| ((i * 3 + 1) % 251) as u8).collect();
        let mut input = Vec::with_capacity(128);
        for row in 0..8 {
            input.extend_from_slice(&first_cell[row * 8..row * 8 + 8]);
            input.extend_from_slice(&first_cell[row * 8..row * 8 + 8]);
        }

        let mut encoded = vec![0; 192];
        let cbytes = compress_ndlz_2d_block(8, [8, 16], &input, &mut encoded);
        assert_eq!(cbytes, 77);
        assert_eq!(encoded[9], 0);
        assert_eq!(encoded[74], 0xc0);
        assert_eq!(u16::from_le_bytes([encoded[75], encoded[76]]), 64);

        let mut decoded = vec![0; input.len()];
        assert_eq!(
            decompress_block_with_meta(
                BLOSC_CODEC_NDLZ,
                8,
                &encoded[..cbytes as usize],
                &mut decoded,
            ),
            input.len() as i32
        );
        assert_eq!(decoded, input);
    }

    #[test]
    fn ndlz_encoder_uses_row_match_tokens_for_4x4_cells() {
        let first_cell: Vec<u8> = (0..16).map(|i| (i + 1) as u8).collect();

        let mut triple_input = Vec::with_capacity(64);
        for row in 0..4 {
            triple_input.extend_from_slice(&first_cell[row * 4..row * 4 + 4]);
            let second_row = match row {
                0 => &first_cell[4..8],
                1 => &first_cell[8..12],
                2 => &[90, 91, 92, 93],
                _ => &first_cell[12..16],
            };
            triple_input.extend_from_slice(second_row);
            triple_input.extend_from_slice(&[7, 7, 7, 7]);
            triple_input.extend_from_slice(&[8, 8, 8, 8]);
        }

        let mut encoded = vec![0; 96];
        let cbytes = compress_ndlz_2d_block(4, [4, 16], &triple_input, &mut encoded);
        assert_eq!(cbytes, 37);
        assert_eq!(encoded[26], (7 << 5) | (2 << 3));
        assert_eq!(u16::from_le_bytes([encoded[27], encoded[28]]), 12);

        let mut decoded = vec![0; triple_input.len()];
        assert_eq!(
            decompress_block_with_meta(
                BLOSC_CODEC_NDLZ,
                4,
                &encoded[..cbytes as usize],
                &mut decoded,
            ),
            triple_input.len() as i32
        );
        assert_eq!(decoded, triple_input);

        let mut pair_input = Vec::with_capacity(64);
        for row in 0..4 {
            pair_input.extend_from_slice(&first_cell[row * 4..row * 4 + 4]);
            let second_row = match row {
                0 => &first_cell[0..4],
                1 => &[50, 51, 52, 53],
                2 => &first_cell[4..8],
                _ => &[60, 61, 62, 63],
            };
            pair_input.extend_from_slice(second_row);
            pair_input.extend_from_slice(&[7, 7, 7, 7]);
            pair_input.extend_from_slice(&[8, 8, 8, 8]);
        }

        let cbytes = compress_ndlz_2d_block(4, [4, 16], &pair_input, &mut encoded);
        assert_eq!(cbytes, 41);
        assert_eq!(encoded[26], (1 << 7) | (2 << 3));
        assert_eq!(u16::from_le_bytes([encoded[27], encoded[28]]), 16);

        let mut decoded = vec![0; pair_input.len()];
        assert_eq!(
            decompress_block_with_meta(
                BLOSC_CODEC_NDLZ,
                4,
                &encoded[..cbytes as usize],
                &mut decoded,
            ),
            pair_input.len() as i32
        );
        assert_eq!(decoded, pair_input);
    }

    #[test]
    fn ndlz_encoder_uses_two_row_pair_match_token_for_4x4_cells() {
        let a = [1, 2, 3, 4];
        let b = [5, 6, 7, 8];
        let c = [9, 10, 11, 12];
        let d = [13, 14, 15, 16];
        let x = [21, 22, 23, 24];
        let y = [25, 26, 27, 28];
        let u = [31, 32, 33, 34];
        let v = [35, 36, 37, 38];

        let rows = [[a, u, a], [b, c, b], [x, d, c], [y, v, d]];
        let mut input = Vec::with_capacity(48);
        for row in rows {
            for cell_row in row {
                input.extend_from_slice(&cell_row);
            }
        }

        let mut encoded = vec![0; 96];
        let cbytes = compress_ndlz_2d_block(4, [4, 12], &input, &mut encoded);
        assert_eq!(cbytes, 48);
        assert_eq!(encoded[43], 40);
        assert_eq!(u16::from_le_bytes([encoded[44], encoded[45]]), 33);
        assert_eq!(u16::from_le_bytes([encoded[46], encoded[47]]), 12);

        let mut decoded = vec![0; input.len()];
        assert_eq!(
            decompress_block_with_meta(BLOSC_CODEC_NDLZ, 4, &encoded[..48], &mut decoded),
            input.len() as i32
        );
        assert_eq!(decoded, input);
    }

    #[test]
    fn ndlz_encoder_uses_row_match_tokens_for_8x8_cells() {
        let first_cell: Vec<u8> = (0..64).map(|i| ((i * 7 + 11) % 251) as u8).collect();

        let mut triple_input = Vec::with_capacity(128);
        for row in 0..8 {
            triple_input.extend_from_slice(&first_cell[row * 8..row * 8 + 8]);
            let second_row: Vec<u8> = match row {
                2 => first_cell[0..8].to_vec(),
                3 => first_cell[8..16].to_vec(),
                4 => first_cell[16..24].to_vec(),
                _ => (0..8).map(|col| (180 + row * 8 + col) as u8).collect(),
            };
            triple_input.extend_from_slice(&second_row);
        }

        let mut encoded = vec![0; 192];
        let cbytes = compress_ndlz_2d_block(8, [8, 16], &triple_input, &mut encoded);
        assert_eq!(cbytes, 117);
        assert_eq!(encoded[74], (21 << 3) | 2);
        assert_eq!(u16::from_le_bytes([encoded[75], encoded[76]]), 64);

        let mut decoded = vec![0; triple_input.len()];
        assert_eq!(
            decompress_block_with_meta(
                BLOSC_CODEC_NDLZ,
                8,
                &encoded[..cbytes as usize],
                &mut decoded,
            ),
            triple_input.len() as i32
        );
        assert_eq!(decoded, triple_input);

        let mut pair_input = Vec::with_capacity(128);
        for row in 0..8 {
            pair_input.extend_from_slice(&first_cell[row * 8..row * 8 + 8]);
            let second_row: Vec<u8> = match row {
                3 => first_cell[0..8].to_vec(),
                4 => first_cell[8..16].to_vec(),
                _ => (0..8).map(|col| (90 + row * 8 + col) as u8).collect(),
            };
            pair_input.extend_from_slice(&second_row);
        }

        let cbytes = compress_ndlz_2d_block(8, [8, 16], &pair_input, &mut encoded);
        assert_eq!(cbytes, 125);
        assert_eq!(encoded[74], (17 << 3) | 3);
        assert_eq!(u16::from_le_bytes([encoded[75], encoded[76]]), 64);

        let mut decoded = vec![0; pair_input.len()];
        assert_eq!(
            decompress_block_with_meta(
                BLOSC_CODEC_NDLZ,
                8,
                &encoded[..cbytes as usize],
                &mut decoded,
            ),
            pair_input.len() as i32
        );
        assert_eq!(decoded, pair_input);
    }

    #[test]
    fn zstd_at_higher_blosc_level_compresses_better() {
        // A quick sanity check: after the mapping fix, blosc level 9 should
        // produce a significantly smaller or equal output than level 1 on
        // repetitive data. With the old identity mapping, level 9 used zstd
        // level 9; with the fix, level 9 uses zstd level 22 (maxCLevel).
        let data: Vec<u8> = (0..16384u32).flat_map(|i| (i % 17).to_le_bytes()).collect();
        let mut buf1 = vec![0u8; data.len() + 256];
        let mut buf9 = vec![0u8; data.len() + 256];

        let csize1 = zstd_compress(&data, &mut buf1, 1);
        let csize9 = zstd_compress(&data, &mut buf9, 9);

        assert!(csize1 > 0 && csize9 > 0, "compression must not fail");
        assert!(
            csize9 <= csize1,
            "level 9 must compress at least as well as level 1 (got {csize9} vs {csize1})"
        );
    }

    #[test]
    fn zlib_zstd_direct_decompress_failures_return_c_zero_sentinel() {
        let mut dest = vec![0u8; 128];
        assert_eq!(
            decompress_block(BLOSC_ZLIB, b"not-a-zlib-block", &mut dest),
            0
        );
        let src = b"zlib payload";
        let mut compressed = vec![0u8; 128];
        let cbytes = zlib_compress(src, &mut compressed, 5);
        assert!(cbytes > 0);
        compressed.truncate(cbytes as usize);
        let mut zlib_out = vec![0u8; src.len()];
        assert_eq!(
            zlib_decompress(&compressed, &mut zlib_out),
            src.len() as i32
        );
        compressed.extend_from_slice(b"trailing");
        assert_eq!(
            zlib_decompress(&compressed, &mut zlib_out),
            src.len() as i32
        );
        assert_eq!(
            decompress_block(BLOSC_ZSTD, b"not-a-zstd-block", &mut dest),
            0
        );
        assert_eq!(
            decompress_block_with_dict(BLOSC_ZSTD, b"not-a-zstd-block", &mut dest, b"dict"),
            0
        );
    }

    #[test]
    fn zstd_dictionary_matches_c_fixed_cdict_level() {
        let dict: Vec<u8> = (0..8192u32)
            .flat_map(|i| format!("record-{i:04}-COMMON-PREFIX-COMMON-SUFFIX;").into_bytes())
            .collect();
        let data: Vec<u8> = (0..16384u32)
            .flat_map(|i| {
                format!("record-{:04}-COMMON-PREFIX-COMMON-SUFFIX;", i % 8192).into_bytes()
            })
            .collect();
        let mut buf1 = vec![0u8; data.len() + 1024];
        let mut buf9 = vec![0u8; data.len() + 1024];

        let csize1 = zstd_compress_with_dict(&data, &mut buf1, 1, &dict);
        let csize9 = zstd_compress_with_dict(&data, &mut buf9, 9, &dict);

        assert!(
            csize1 > 0 && csize9 > 0,
            "dictionary compression must not fail"
        );
        assert_eq!(
            csize9, csize1,
            "C-Blosc2 creates Zstd CDicts at level 1 regardless of Blosc clevel"
        );
        assert_eq!(&buf9[..csize9 as usize], &buf1[..csize1 as usize]);

        let mut restored = vec![0; data.len()];
        let dsize = zstd_decompress_with_dict(&buf9[..csize9 as usize], &mut restored, &dict);
        assert_eq!(dsize as usize, data.len());
        assert_eq!(restored, data);
    }

    #[test]
    fn lz4hc_roundtrips_via_lz4_decoder() {
        let data: Vec<u8> = (0..8192u32).flat_map(|i| (i % 64).to_le_bytes()).collect();
        let mut compressed = vec![0; data.len() + 1024];

        let csize = compress_block(BLOSC_LZ4HC, 9, &data, &mut compressed);
        assert!(csize > 0);

        let mut decompressed = vec![0; data.len()];
        let dsize = decompress_block(
            BLOSC_LZ4HC,
            &compressed[..csize as usize],
            &mut decompressed,
        );

        assert_eq!(dsize as usize, data.len());
        assert_eq!(decompressed, data);

        let mut oversized = vec![0; data.len() + 1];
        assert_eq!(
            decompress_block(BLOSC_LZ4HC, &compressed[..csize as usize], &mut oversized),
            0
        );
        assert_eq!(
            decompress_block(BLOSC_LZ4HC, b"bad-lz4-block", &mut decompressed),
            0
        );
    }

    #[test]
    fn lz4_dictionary_paths_roundtrip() {
        let dict = b"abcdefghijklmnop0123456789abcdefghijklmnop0123456789";
        let data = b"abcdefghijklmnopabcdefghZZZZabcdefghijklmnop";
        let mut compressed = vec![0; 256];
        let mut decompressed = vec![0; data.len()];

        let csize = lz4_compress_with_dict(5, data, &mut compressed, dict);
        assert!(csize > 0);

        let dsize =
            lz4_decompress_with_dict(&compressed[..csize as usize], &mut decompressed, dict);
        assert_eq!(dsize as usize, data.len());
        assert_eq!(decompressed, data);

        let mut oversized = vec![0; data.len() + 1];
        assert_eq!(
            lz4_decompress_with_dict(&compressed[..csize as usize], &mut oversized, dict),
            0
        );
        assert_eq!(
            lz4_decompress_with_dict(b"bad-lz4-block", &mut decompressed, dict),
            0
        );
    }

    #[test]
    fn lz4hc_dictionary_paths_roundtrip() {
        let dict = b"abcdefghijklmnop0123456789abcdefghijklmnop0123456789";
        let data = b"abcdefghijklmnopabcdefghZZZZabcdefghijklmnop";
        let mut compressed = vec![0; 256];
        let mut decompressed = vec![0; data.len()];

        let csize = lz4hc_compress_with_dict(9, data, &mut compressed, dict);
        assert!(csize > 0);

        let dsize =
            lz4_decompress_with_dict(&compressed[..csize as usize], &mut decompressed, dict);
        assert_eq!(dsize as usize, data.len());
        assert_eq!(decompressed, data);
    }
}
