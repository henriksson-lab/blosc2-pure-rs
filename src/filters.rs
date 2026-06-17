//! Filters that pre-process a block of data before it is fed to the codec.
//!
//! Filters do not compress data on their own; they rearrange or transform it so
//! that the downstream codec can compress it more efficiently. Each filter has a
//! forward variant (applied at compression time) and a backward variant
//! (applied at decompression time). The pipeline supports up to
//! [`BLOSC2_MAX_FILTERS`] filters and is applied left-to-right when encoding,
//! right-to-left when decoding.
//!
//! Built-in filters:
//! - **shuffle**: byte-wise transpose within each element, grouping bytes of
//!   equal positional significance together.
//! - **bitshuffle**: bit-wise transpose within elements (after a byte
//!   transpose), grouping bits of equal positional significance.
//! - **delta**: XOR each element against a reference, exposing redundancy
//!   across consecutive blocks or elements.
//! - **trunc_prec**: zero out least-significant mantissa bits of IEEE-754
//!   floats, trading precision for compressibility.
//!
//! Users can register custom filters via [`register_filter`]; global plugin
//! filters can be registered separately via [`register_global_filter`].

#![allow(clippy::needless_range_loop)]

use crate::constants::*;
use std::collections::HashMap;
use std::ffi::{c_char, c_void, CStr};
use std::sync::{OnceLock, RwLock};

/// Forward (encoding-side) callback signature for a user-defined filter.
pub type FilterForwardFn =
    fn(meta: u8, typesize: usize, block_offset: usize, src: &[u8], dest: &mut [u8]);
/// Backward (decoding-side) callback signature for a user-defined filter.
pub type FilterBackwardFn =
    fn(meta: u8, typesize: usize, block_offset: usize, src: &[u8], dest: &mut [u8]);
/// Fallible forward callback signature for a user-defined filter.
///
/// Return `0` on success and a non-zero value to fail the filter pipeline,
/// matching C-Blosc2 plugin callback conventions.
pub type FallibleFilterForwardFn =
    fn(meta: u8, typesize: usize, block_offset: usize, src: &[u8], dest: &mut [u8]) -> i32;
/// Fallible backward callback signature for a user-defined filter.
///
/// Return `0` on success and a non-zero value to fail the filter pipeline,
/// matching C-Blosc2 plugin callback conventions.
pub type FallibleFilterBackwardFn =
    fn(meta: u8, typesize: usize, block_offset: usize, src: &[u8], dest: &mut [u8]) -> i32;

/// C-compatible callback return codes used by richer plugin callbacks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum PluginCallbackStatus {
    Success = 0,
    Failure = BLOSC2_ERROR_FAILURE,
}

/// Compression-parameter snapshot exposed to filter plugin callbacks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FilterCParamsContext {
    pub compcode: u8,
    pub compcode_meta: u8,
    pub clevel: u8,
    pub use_dict: bool,
    pub typesize: i32,
    pub blocksize: i32,
    pub splitmode: i32,
    pub filters: [u8; BLOSC2_MAX_FILTERS],
    pub filters_meta: [u8; BLOSC2_MAX_FILTERS],
    pub nthreads: i16,
    pub nchunk: i64,
    pub user_data: usize,
    pub preparams: usize,
    pub tuner_id: i32,
    pub instr_codec: bool,
    pub codec_params: usize,
}

impl FilterCParamsContext {
    /// Return the associated super-chunk handle from the per-block callback context.
    ///
    /// C-ABI callbacks receive the same value as `cparams.schunk`.
    pub fn schunk(self, chunk: FilterChunkContext) -> usize {
        chunk.schunk
    }
}

/// Decompression-parameter snapshot exposed to filter plugin callbacks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FilterDParamsContext {
    pub nthreads: i16,
    pub typesize: i32,
    pub nchunk: i64,
    pub user_data: usize,
    pub postparams: usize,
}

impl FilterDParamsContext {
    /// Return the associated super-chunk handle from the per-block callback context.
    ///
    /// C-ABI callbacks receive the same value as `dparams.schunk`.
    pub fn schunk(self, chunk: FilterChunkContext) -> usize {
        chunk.schunk
    }
}

/// Per-block context exposed to C-compatible filter plugin callbacks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FilterChunkContext {
    pub schunk: usize,
    pub nchunk: i64,
    pub nblock: i32,
    pub block_offset: usize,
    pub blocksize: usize,
    pub bsize: usize,
}

/// Rich filter callback parameters, modeled after C-Blosc2 plugin callbacks.
#[derive(Debug, Clone, Copy)]
pub struct FilterCallbackContext<'a> {
    pub filter_id: u8,
    pub filter_slot: usize,
    pub meta: u8,
    pub typesize: usize,
    pub cparams: Option<&'a FilterCParamsContext>,
    pub dparams: Option<&'a FilterDParamsContext>,
    pub chunk: FilterChunkContext,
    pub b2nd_metalayer: Option<&'a [u8]>,
    pub user_data: usize,
}

/// Rich forward callback signature for C-compatible filters.
pub type ContextFilterForwardFn =
    for<'a> fn(&mut FilterCallbackContext<'a>, src: &[u8], dest: &mut [u8]) -> i32;
/// Rich backward callback signature for C-compatible filters.
pub type ContextFilterBackwardFn =
    for<'a> fn(&mut FilterCallbackContext<'a>, src: &[u8], dest: &mut [u8]) -> i32;

/// C-ABI compression parameters passed to raw `blosc2_filter` callbacks.
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
    pub prefilter: *mut c_void,
    pub preparams: *mut c_void,
    pub tuner_params: *mut c_void,
    pub tuner_id: i32,
    pub instr_codec: bool,
    pub codec_params: *mut c_void,
    pub filter_params: [*mut c_void; BLOSC2_MAX_FILTERS],
}

impl Blosc2CParams {
    fn from_context(ctx: &FilterCParamsContext, schunk: usize) -> Self {
        Self {
            compcode: ctx.compcode,
            compcode_meta: ctx.compcode_meta,
            clevel: ctx.clevel,
            use_dict: i32::from(ctx.use_dict),
            typesize: ctx.typesize,
            nthreads: ctx.nthreads,
            blocksize: ctx.blocksize,
            splitmode: ctx.splitmode,
            schunk: schunk as *mut c_void,
            filters: ctx.filters,
            filters_meta: ctx.filters_meta,
            // Rust prefilter callbacks and user data are not ABI-compatible with
            // C-Blosc2 prefilter pointers, so expose no raw prefilter state.
            prefilter: std::ptr::null_mut(),
            preparams: std::ptr::null_mut(),
            tuner_params: std::ptr::null_mut(),
            tuner_id: ctx.tuner_id,
            instr_codec: ctx.instr_codec,
            codec_params: ctx.codec_params as *mut c_void,
            filter_params: [std::ptr::null_mut(); BLOSC2_MAX_FILTERS],
        }
    }

    fn from_pipeline(
        ctx: &FilterCallbackContext<'_>,
        typesize: usize,
        filters: &[u8; BLOSC2_MAX_FILTERS],
        filters_meta: &[u8; BLOSC2_MAX_FILTERS],
    ) -> Self {
        ctx.cparams.map_or_else(
            || Self {
                compcode: 0,
                compcode_meta: 0,
                clevel: 0,
                use_dict: 0,
                typesize: i32::try_from(typesize).unwrap_or(i32::MAX),
                nthreads: 1,
                blocksize: i32::try_from(ctx.chunk.blocksize).unwrap_or(i32::MAX),
                splitmode: 0,
                schunk: ctx.chunk.schunk as *mut c_void,
                filters: *filters,
                filters_meta: *filters_meta,
                prefilter: std::ptr::null_mut(),
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

/// C-ABI decompression parameters passed to raw `blosc2_filter` callbacks.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct Blosc2DParams {
    pub nthreads: i16,
    pub schunk: *mut c_void,
    pub postfilter: *mut c_void,
    pub postparams: *mut c_void,
    pub typesize: i32,
}

impl Blosc2DParams {
    fn from_context(ctx: &FilterDParamsContext, schunk: usize) -> Self {
        Self {
            nthreads: ctx.nthreads,
            schunk: schunk as *mut c_void,
            // Rust postfilter callbacks and user data are not ABI-compatible with
            // C-Blosc2 postfilter pointers, so expose no raw postfilter state.
            postfilter: std::ptr::null_mut(),
            postparams: std::ptr::null_mut(),
            typesize: ctx.typesize,
        }
    }

    fn from_pipeline(ctx: &FilterCallbackContext<'_>, typesize: usize) -> Self {
        ctx.dparams.map_or_else(
            || Self {
                nthreads: 1,
                schunk: ctx.chunk.schunk as *mut c_void,
                postfilter: std::ptr::null_mut(),
                postparams: std::ptr::null_mut(),
                typesize: i32::try_from(typesize).unwrap_or(i32::MAX),
            },
            |dparams| Self::from_context(dparams, ctx.chunk.schunk),
        )
    }
}

/// Raw C-ABI forward callback signature for a `blosc2_filter`.
pub type Blosc2FilterForwardCb = unsafe extern "C" fn(
    input: *const u8,
    output: *mut u8,
    length: i32,
    meta: u8,
    cparams: *mut Blosc2CParams,
    id: u8,
) -> i32;
/// Raw C-ABI backward callback signature for a `blosc2_filter`.
pub type Blosc2FilterBackwardCb = unsafe extern "C" fn(
    input: *const u8,
    output: *mut u8,
    length: i32,
    meta: u8,
    dparams: *mut Blosc2DParams,
    id: u8,
) -> i32;

/// C-shaped user filter descriptor for source-level `blosc2_filter` parity.
#[derive(Clone, Copy)]
pub struct Blosc2Filter {
    pub id: u8,
    pub name: &'static str,
    pub version: u8,
    pub forward: FallibleFilterForwardFn,
    pub backward: FallibleFilterBackwardFn,
}

/// Raw C-shaped `blosc2_filter` descriptor.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct Blosc2FilterAbi {
    pub id: u8,
    pub name: *const c_char,
    pub version: u8,
    pub forward: Option<Blosc2FilterForwardCb>,
    pub backward: Option<Blosc2FilterBackwardCb>,
}

#[derive(Clone, Copy)]
enum UserFilterForward {
    Infallible(FilterForwardFn),
    Fallible(FallibleFilterForwardFn),
    Context(ContextFilterForwardFn),
    CAbi(Option<Blosc2FilterForwardCb>),
}

impl UserFilterForward {
    fn same_callback(self, other: Self) -> bool {
        match (self, other) {
            (Self::Infallible(a), Self::Infallible(b)) => a as usize == b as usize,
            (Self::Fallible(a), Self::Fallible(b)) => a as usize == b as usize,
            (Self::Context(a), Self::Context(b)) => a as usize == b as usize,
            (Self::CAbi(a), Self::CAbi(b)) => {
                a.map(|callback| callback as usize) == b.map(|callback| callback as usize)
            }
            _ => false,
        }
    }

    fn run(
        self,
        ctx: &mut FilterCallbackContext<'_>,
        meta: u8,
        typesize: usize,
        block_offset: usize,
        filters: &[u8; BLOSC2_MAX_FILTERS],
        filters_meta: &[u8; BLOSC2_MAX_FILTERS],
        src: &[u8],
        dest: &mut [u8],
    ) -> i32 {
        match self {
            UserFilterForward::Infallible(callback) => {
                callback(meta, typesize, block_offset, src, dest);
                0
            }
            UserFilterForward::Fallible(callback) => {
                callback(meta, typesize, block_offset, src, dest)
            }
            UserFilterForward::Context(callback) => callback(ctx, src, dest),
            UserFilterForward::CAbi(Some(callback)) => {
                let Ok(length) = i32::try_from(src.len()) else {
                    return 1;
                };
                let mut cparams =
                    Blosc2CParams::from_pipeline(ctx, typesize, filters, filters_meta);
                unsafe {
                    callback(
                        src.as_ptr(),
                        dest.as_mut_ptr(),
                        length,
                        meta,
                        &mut cparams,
                        ctx.filter_id,
                    )
                }
            }
            UserFilterForward::CAbi(None) => missing_dynamic_filter_callback(),
        }
    }
}

#[derive(Clone, Copy)]
enum UserFilterBackward {
    Infallible(FilterBackwardFn),
    Fallible(FallibleFilterBackwardFn),
    Context(ContextFilterBackwardFn),
    CAbi(Option<Blosc2FilterBackwardCb>),
}

impl UserFilterBackward {
    fn same_callback(self, other: Self) -> bool {
        match (self, other) {
            (Self::Infallible(a), Self::Infallible(b)) => a as usize == b as usize,
            (Self::Fallible(a), Self::Fallible(b)) => a as usize == b as usize,
            (Self::Context(a), Self::Context(b)) => a as usize == b as usize,
            (Self::CAbi(a), Self::CAbi(b)) => {
                a.map(|callback| callback as usize) == b.map(|callback| callback as usize)
            }
            _ => false,
        }
    }

    fn run(
        self,
        ctx: &mut FilterCallbackContext<'_>,
        meta: u8,
        typesize: usize,
        block_offset: usize,
        src: &[u8],
        dest: &mut [u8],
    ) -> i32 {
        match self {
            UserFilterBackward::Infallible(callback) => {
                callback(meta, typesize, block_offset, src, dest);
                0
            }
            UserFilterBackward::Fallible(callback) => {
                callback(meta, typesize, block_offset, src, dest)
            }
            UserFilterBackward::Context(callback) => callback(ctx, src, dest),
            UserFilterBackward::CAbi(Some(callback)) => {
                let Ok(length) = i32::try_from(src.len()) else {
                    return 1;
                };
                let mut dparams = Blosc2DParams::from_pipeline(ctx, typesize);
                unsafe {
                    callback(
                        src.as_ptr(),
                        dest.as_mut_ptr(),
                        length,
                        meta,
                        &mut dparams,
                        ctx.filter_id,
                    )
                }
            }
            UserFilterBackward::CAbi(None) => missing_dynamic_filter_callback(),
        }
    }
}

fn missing_dynamic_filter_callback() -> i32 {
    // C-Blosc2 may resolve null callbacks through plugin dynamic loading at
    // invocation time. This crate does not currently have a filter dynamic
    // loader, so preserve the null callback and fail explicitly when invoked.
    BLOSC2_ERROR_FAILURE
}

#[derive(Clone, Copy)]
struct UserFilter {
    name: Option<&'static str>,
    version: Option<u8>,
    forward: UserFilterForward,
    backward: UserFilterBackward,
}

#[derive(Clone, Copy)]
struct KnownGlobalFilter {
    filter_id: u8,
    name: &'static str,
    version: u8,
}

const KNOWN_GLOBAL_FILTERS: &[KnownGlobalFilter] = &[
    KnownGlobalFilter {
        filter_id: BLOSC_FILTER_NDCELL,
        name: "ndcell",
        version: 1,
    },
    KnownGlobalFilter {
        filter_id: BLOSC_FILTER_NDMEAN,
        name: "ndmean",
        version: 1,
    },
    KnownGlobalFilter {
        filter_id: BLOSC_FILTER_BYTEDELTA_BUGGY,
        name: "bytedelta_buggy",
        version: 1,
    },
    KnownGlobalFilter {
        filter_id: BLOSC_FILTER_BYTEDELTA,
        name: "bytedelta",
        version: 1,
    },
    KnownGlobalFilter {
        filter_id: BLOSC_FILTER_INT_TRUNC,
        name: "int_trunc",
        version: 1,
    },
];

impl UserFilter {
    fn same_callbacks(self, other: Self) -> bool {
        self.name == other.name
            && self.version == other.version
            && self.forward.same_callback(other.forward)
            && self.backward.same_callback(other.backward)
    }
}

static USER_FILTERS: OnceLock<RwLock<HashMap<u8, UserFilter>>> = OnceLock::new();
static KNOWN_GLOBAL_FILTERS_REGISTERED: OnceLock<()> = OnceLock::new();

fn user_filters() -> &'static RwLock<HashMap<u8, UserFilter>> {
    USER_FILTERS.get_or_init(|| RwLock::new(HashMap::new()))
}

fn unsupported_known_global_filter(
    _meta: u8,
    _typesize: usize,
    _block_offset: usize,
    _src: &[u8],
    _dest: &mut [u8],
) -> i32 {
    1
}

#[derive(Debug)]
struct B2ndFilterMeta {
    ndim: usize,
    blockshape: Vec<usize>,
}

fn read_b2nd_fixint(data: &[u8], pos: &mut usize) -> Option<u8> {
    let value = *data.get(*pos)?;
    if value > 0x7f {
        return None;
    }
    *pos += 1;
    Some(value)
}

fn read_b2nd_array_header(data: &[u8], pos: &mut usize) -> Option<usize> {
    let value = *data.get(*pos)?;
    *pos += 1;
    match value {
        0x90..=0x9f => Some((value - 0x90) as usize),
        0xdc => {
            let bytes: [u8; 2] = data.get(*pos..*pos + 2)?.try_into().ok()?;
            *pos += 2;
            Some(u16::from_be_bytes(bytes) as usize)
        }
        0xdd => {
            let bytes: [u8; 4] = data.get(*pos..*pos + 4)?.try_into().ok()?;
            *pos += 4;
            usize::try_from(u32::from_be_bytes(bytes)).ok()
        }
        _ => None,
    }
}

fn skip_b2nd_i64_array(data: &[u8], pos: &mut usize, len: usize) -> Option<()> {
    if read_b2nd_array_header(data, pos)? != len {
        return None;
    }
    for _ in 0..len {
        if *data.get(*pos)? != 0xd3 {
            return None;
        }
        *pos += 1 + std::mem::size_of::<i64>();
        if *pos > data.len() {
            return None;
        }
    }
    Some(())
}

fn read_b2nd_i32_array(data: &[u8], pos: &mut usize, len: usize) -> Option<Vec<usize>> {
    if read_b2nd_array_header(data, pos)? != len {
        return None;
    }
    let mut values = Vec::with_capacity(len);
    for _ in 0..len {
        if *data.get(*pos)? != 0xd2 {
            return None;
        }
        *pos += 1;
        let bytes: [u8; 4] = data.get(*pos..*pos + 4)?.try_into().ok()?;
        *pos += 4;
        let value = i32::from_be_bytes(bytes);
        if value <= 0 {
            return None;
        }
        values.push(value as usize);
    }
    Some(values)
}

fn parse_b2nd_filter_meta(data: &[u8]) -> Option<B2ndFilterMeta> {
    let mut pos = 0usize;
    let fields = read_b2nd_array_header(data, &mut pos)?;
    if fields != 7 && fields != 5 {
        return None;
    }
    if read_b2nd_fixint(data, &mut pos)? != 0 {
        return None;
    }
    let ndim = read_b2nd_fixint(data, &mut pos)? as usize;
    if ndim == 0 || ndim > 8 {
        return None;
    }
    skip_b2nd_i64_array(data, &mut pos, ndim)?;
    let _chunkshape = read_b2nd_i32_array(data, &mut pos, ndim)?;
    let blockshape = read_b2nd_i32_array(data, &mut pos, ndim)?;
    Some(B2ndFilterMeta { ndim, blockshape })
}

fn product_checked(values: &[usize]) -> Option<usize> {
    values
        .iter()
        .try_fold(1usize, |acc, &value| acc.checked_mul(value))
}

fn unidim_to_multidim(mut index: usize, shape: &[usize], dest: &mut [usize]) {
    for dim in (0..shape.len()).rev() {
        let width = shape[dim];
        dest[dim] = index % width;
        index /= width;
    }
}

fn ndcell_layout(
    meta: u8,
    typesize: usize,
    bsize: usize,
    b2nd_metalayer: Option<&[u8]>,
    clamp_to_blockshape: bool,
) -> Option<(B2ndFilterMeta, Vec<usize>, Vec<usize>)> {
    let b2nd = parse_b2nd_filter_meta(b2nd_metalayer?)?;
    let cell_shape = meta as i8;
    if cell_shape <= 0 || typesize == 0 {
        return None;
    }
    let cell_shape = cell_shape as usize;
    let cellshape: Vec<usize> = b2nd
        .blockshape
        .iter()
        .map(|&block_dim| {
            if clamp_to_blockshape {
                cell_shape.min(block_dim)
            } else {
                cell_shape
            }
        })
        .collect();
    let block_items = product_checked(&b2nd.blockshape)?;
    let blocksize = block_items.checked_mul(typesize)?;
    let cell_items = product_checked(&cellshape)?;
    if bsize != blocksize || bsize < cell_items.checked_mul(typesize)? {
        return None;
    }
    let index_shape: Vec<usize> = b2nd
        .blockshape
        .iter()
        .zip(&cellshape)
        .map(|(&block_dim, &cell_dim)| block_dim.div_ceil(cell_dim))
        .collect();
    Some((b2nd, cellshape, index_shape))
}

fn ndcell_copy_forward(
    src: &[u8],
    dest: &mut [u8],
    typesize: usize,
    b2nd: &B2ndFilterMeta,
    cellshape: &[usize],
    index_shape: &[usize],
) -> Option<()> {
    let ncells = product_checked(index_shape)?;
    let mut op = 0usize;
    let mut ii = vec![0usize; b2nd.ndim];
    let mut kk = vec![0usize; b2nd.ndim.saturating_sub(1)];
    for cell_ind in 0..ncells {
        unidim_to_multidim(cell_ind, index_shape, &mut ii);
        let mut orig = 0usize;
        let mut nd_aux = cellshape[0];
        for dim in (0..b2nd.ndim).rev() {
            orig = orig.checked_add(ii[dim].checked_mul(nd_aux)?)?;
            nd_aux = nd_aux.checked_mul(b2nd.blockshape[dim])?;
        }

        let mut pad_shape = vec![0usize; b2nd.ndim];
        for dim in 0..b2nd.ndim {
            let remainder = b2nd.blockshape[dim] % cellshape[dim];
            pad_shape[dim] = if remainder != 0 && ii[dim] == index_shape[dim] - 1 {
                remainder
            } else {
                cellshape[dim]
            };
        }
        let ncopies = product_checked(&pad_shape[..b2nd.ndim.saturating_sub(1)])?;
        for copy_ind in 0..ncopies {
            unidim_to_multidim(copy_ind, &pad_shape[..b2nd.ndim - 1], &mut kk);
            let mut ind = orig;
            nd_aux = b2nd.blockshape[b2nd.ndim - 1];
            for dim in (0..b2nd.ndim - 1).rev() {
                ind = ind.checked_add(kk[dim].checked_mul(nd_aux)?)?;
                nd_aux = nd_aux.checked_mul(b2nd.blockshape[dim])?;
            }
            let bytes = pad_shape[b2nd.ndim - 1].checked_mul(typesize)?;
            let src_start = ind.checked_mul(typesize)?;
            let src_end = src_start.checked_add(bytes)?;
            let dest_end = op.checked_add(bytes)?;
            dest.get_mut(op..dest_end)?
                .copy_from_slice(src.get(src_start..src_end)?);
            op = dest_end;
        }
    }
    (op == src.len()).then_some(())
}

fn ndcell_copy_backward(
    src: &[u8],
    dest: &mut [u8],
    typesize: usize,
    b2nd: &B2ndFilterMeta,
    cellshape: &[usize],
    index_shape: &[usize],
) -> Option<()> {
    let ncells = product_checked(index_shape)?;
    let mut ip = 0usize;
    let mut final_ind = 0usize;
    let mut final_last_dim = 0usize;
    let mut ii = vec![0usize; b2nd.ndim];
    let mut kk = vec![0usize; b2nd.ndim.saturating_sub(1)];
    for cell_ind in 0..ncells {
        unidim_to_multidim(cell_ind, index_shape, &mut ii);
        let mut orig = 0usize;
        let mut nd_aux = cellshape[0];
        for dim in (0..b2nd.ndim).rev() {
            orig = orig.checked_add(ii[dim].checked_mul(nd_aux)?)?;
            nd_aux = nd_aux.checked_mul(b2nd.blockshape[dim])?;
        }

        let mut pad_shape = vec![0usize; b2nd.ndim];
        for dim in 0..b2nd.ndim {
            let remainder = b2nd.blockshape[dim] % cellshape[dim];
            pad_shape[dim] = if remainder != 0 && ii[dim] == index_shape[dim] - 1 {
                remainder
            } else {
                cellshape[dim]
            };
        }
        let ncopies = product_checked(&pad_shape[..b2nd.ndim.saturating_sub(1)])?;
        for copy_ind in 0..ncopies {
            unidim_to_multidim(copy_ind, &pad_shape[..b2nd.ndim - 1], &mut kk);
            let mut ind = orig;
            nd_aux = b2nd.blockshape[b2nd.ndim - 1];
            for dim in (0..b2nd.ndim - 1).rev() {
                ind = ind.checked_add(kk[dim].checked_mul(nd_aux)?)?;
                nd_aux = nd_aux.checked_mul(b2nd.blockshape[dim])?;
            }
            let bytes = pad_shape[b2nd.ndim - 1].checked_mul(typesize)?;
            let dest_start = ind.checked_mul(typesize)?;
            let dest_end = dest_start.checked_add(bytes)?;
            let src_end = ip.checked_add(bytes)?;
            dest.get_mut(dest_start..dest_end)?
                .copy_from_slice(src.get(ip..src_end)?);
            ip = src_end;
            final_ind = ind;
            final_last_dim = pad_shape[b2nd.ndim - 1];
        }
    }
    (ip == src.len() && final_ind + final_last_dim == src.len() / typesize).then_some(())
}

fn apply_ndcell_filter(ctx: &mut FilterCallbackContext<'_>, src: &[u8], dest: &mut [u8]) -> i32 {
    let Some((b2nd, cellshape, index_shape)) =
        ndcell_layout(ctx.meta, ctx.typesize, src.len(), ctx.b2nd_metalayer, false)
    else {
        return BLOSC2_ERROR_FAILURE;
    };
    ndcell_copy_forward(src, dest, ctx.typesize, &b2nd, &cellshape, &index_shape)
        .map(|()| 0)
        .unwrap_or(BLOSC2_ERROR_FAILURE)
}

fn undo_ndcell_filter(ctx: &mut FilterCallbackContext<'_>, src: &[u8], dest: &mut [u8]) -> i32 {
    let Some((b2nd, cellshape, index_shape)) =
        ndcell_layout(ctx.meta, ctx.typesize, src.len(), ctx.b2nd_metalayer, false)
    else {
        return BLOSC2_ERROR_FAILURE;
    };
    ndcell_copy_backward(src, dest, ctx.typesize, &b2nd, &cellshape, &index_shape)
        .map(|()| 0)
        .unwrap_or(BLOSC2_ERROR_FAILURE)
}

fn apply_ndmean_filter(ctx: &mut FilterCallbackContext<'_>, src: &[u8], dest: &mut [u8]) -> i32 {
    if !matches!(ctx.typesize, 4 | 8) {
        return BLOSC2_ERROR_FAILURE;
    }
    let Some((b2nd, cellshape, index_shape)) =
        ndcell_layout(ctx.meta, ctx.typesize, src.len(), ctx.b2nd_metalayer, true)
    else {
        return BLOSC2_ERROR_FAILURE;
    };
    let Some(ncells) = product_checked(&index_shape) else {
        return BLOSC2_ERROR_FAILURE;
    };

    let mut op = 0usize;
    let mut ii = vec![0usize; b2nd.ndim];
    let mut kk = vec![0usize; b2nd.ndim.saturating_sub(1)];
    for cell_ind in 0..ncells {
        unidim_to_multidim(cell_ind, &index_shape, &mut ii);
        let mut orig = 0usize;
        let mut nd_aux = cellshape[0];
        for dim in (0..b2nd.ndim).rev() {
            let Some(addend) = ii[dim].checked_mul(nd_aux) else {
                return BLOSC2_ERROR_FAILURE;
            };
            let Some(next_orig) = orig.checked_add(addend) else {
                return BLOSC2_ERROR_FAILURE;
            };
            orig = next_orig;
            let Some(next_aux) = nd_aux.checked_mul(b2nd.blockshape[dim]) else {
                return BLOSC2_ERROR_FAILURE;
            };
            nd_aux = next_aux;
        }

        let mut pad_shape = vec![0usize; b2nd.ndim];
        for dim in 0..b2nd.ndim {
            let remainder = b2nd.blockshape[dim] % cellshape[dim];
            pad_shape[dim] = if remainder != 0 && ii[dim] == index_shape[dim] - 1 {
                remainder
            } else {
                cellshape[dim]
            };
        }
        let Some(ncopies) = product_checked(&pad_shape[..b2nd.ndim.saturating_sub(1)]) else {
            return BLOSC2_ERROR_FAILURE;
        };
        let mut f32_sum = 0f32;
        let mut f64_sum = 0f64;
        for copy_ind in 0..ncopies {
            unidim_to_multidim(copy_ind, &pad_shape[..b2nd.ndim - 1], &mut kk);
            let mut ind = orig;
            nd_aux = b2nd.blockshape[b2nd.ndim - 1];
            for dim in (0..b2nd.ndim - 1).rev() {
                let Some(addend) = kk[dim].checked_mul(nd_aux) else {
                    return BLOSC2_ERROR_FAILURE;
                };
                let Some(next_ind) = ind.checked_add(addend) else {
                    return BLOSC2_ERROR_FAILURE;
                };
                ind = next_ind;
                let Some(next_aux) = nd_aux.checked_mul(b2nd.blockshape[dim]) else {
                    return BLOSC2_ERROR_FAILURE;
                };
                nd_aux = next_aux;
            }
            for i in 0..pad_shape[b2nd.ndim - 1] {
                let Some(item) = ind.checked_add(i) else {
                    return BLOSC2_ERROR_FAILURE;
                };
                let Some(byte_start) = item.checked_mul(ctx.typesize) else {
                    return BLOSC2_ERROR_FAILURE;
                };
                match ctx.typesize {
                    4 => {
                        let Some(bytes) = src.get(byte_start..byte_start + 4) else {
                            return BLOSC2_ERROR_FAILURE;
                        };
                        f32_sum += f32::from_ne_bytes(bytes.try_into().unwrap());
                    }
                    8 => {
                        let Some(bytes) = src.get(byte_start..byte_start + 8) else {
                            return BLOSC2_ERROR_FAILURE;
                        };
                        f64_sum += f64::from_ne_bytes(bytes.try_into().unwrap());
                    }
                    _ => unreachable!("typesize checked above"),
                }
            }
        }
        let Some(cell_len) = ncopies.checked_mul(pad_shape[b2nd.ndim - 1]) else {
            return BLOSC2_ERROR_FAILURE;
        };
        match ctx.typesize {
            4 => {
                let mean = (f32_sum / cell_len as f32).to_ne_bytes();
                for _ in 0..cell_len {
                    let Some(end) = op.checked_add(4) else {
                        return BLOSC2_ERROR_FAILURE;
                    };
                    let Some(slot) = dest.get_mut(op..end) else {
                        return BLOSC2_ERROR_FAILURE;
                    };
                    slot.copy_from_slice(&mean);
                    op = end;
                }
            }
            8 => {
                let mean = (f64_sum / cell_len as f64).to_ne_bytes();
                for _ in 0..cell_len {
                    let Some(end) = op.checked_add(8) else {
                        return BLOSC2_ERROR_FAILURE;
                    };
                    let Some(slot) = dest.get_mut(op..end) else {
                        return BLOSC2_ERROR_FAILURE;
                    };
                    slot.copy_from_slice(&mean);
                    op = end;
                }
            }
            _ => unreachable!("typesize checked above"),
        }
    }

    if op == src.len() {
        0
    } else {
        BLOSC2_ERROR_FAILURE
    }
}

fn undo_ndmean_filter(ctx: &mut FilterCallbackContext<'_>, src: &[u8], dest: &mut [u8]) -> i32 {
    let Some((b2nd, cellshape, index_shape)) =
        ndcell_layout(ctx.meta, ctx.typesize, src.len(), ctx.b2nd_metalayer, true)
    else {
        return BLOSC2_ERROR_FAILURE;
    };
    ndcell_copy_backward(src, dest, ctx.typesize, &b2nd, &cellshape, &index_shape)
        .map(|()| 0)
        .unwrap_or(BLOSC2_ERROR_FAILURE)
}

fn bytedelta_typesize(meta: u8) -> Option<usize> {
    let typesize = usize::from(meta);
    (1..=BLOSC2_MAXTYPESIZE)
        .contains(&typesize)
        .then_some(typesize)
}

fn bytedelta_context_typesize(ctx: &FilterCallbackContext<'_>) -> Option<usize> {
    if ctx.meta == 0 {
        if ctx.chunk.schunk == 0 {
            return None;
        }
        (1..=BLOSC2_MAXTYPESIZE)
            .contains(&ctx.typesize)
            .then_some(ctx.typesize)
    } else {
        bytedelta_typesize(ctx.meta)
    }
}

fn bytedelta_forward_core(typesize: usize, src: &[u8], dest: &mut [u8]) -> i32 {
    if dest.len() < src.len() || !(1..=BLOSC2_MAXTYPESIZE).contains(&typesize) {
        return 1;
    }

    let stream_len = src.len() / typesize;
    for channel in 0..typesize {
        let base = channel * stream_len;
        let mut previous = 0u8;
        for i in 0..stream_len {
            let value = src[base + i];
            dest[base + i] = value.wrapping_sub(previous);
            previous = value;
        }
    }
    0
}

fn bytedelta_backward_core(typesize: usize, src: &[u8], dest: &mut [u8]) -> i32 {
    if dest.len() < src.len() || !(1..=BLOSC2_MAXTYPESIZE).contains(&typesize) {
        return 1;
    }

    let stream_len = src.len() / typesize;
    for channel in 0..typesize {
        let base = channel * stream_len;
        let mut previous = 0u8;
        for i in 0..stream_len {
            let value = src[base + i].wrapping_add(previous);
            dest[base + i] = value;
            previous = value;
        }
    }
    0
}

fn apply_bytedelta_filter(ctx: &mut FilterCallbackContext<'_>, src: &[u8], dest: &mut [u8]) -> i32 {
    let Some(typesize) = bytedelta_context_typesize(ctx) else {
        return BLOSC2_ERROR_FAILURE;
    };
    bytedelta_forward_core(typesize, src, dest)
}

fn undo_bytedelta_filter(ctx: &mut FilterCallbackContext<'_>, src: &[u8], dest: &mut [u8]) -> i32 {
    let Some(typesize) = bytedelta_context_typesize(ctx) else {
        return BLOSC2_ERROR_FAILURE;
    };
    bytedelta_backward_core(typesize, src, dest)
}

fn bytedelta_buggy_forward_core(typesize: usize, src: &[u8], dest: &mut [u8]) -> i32 {
    bytedelta_buggy_forward_core_with_simd(
        typesize,
        src,
        dest,
        bytedelta_buggy_simd_path_available(),
    )
}

fn bytedelta_buggy_forward_core_with_simd(
    typesize: usize,
    src: &[u8],
    dest: &mut [u8],
    simd_available: bool,
) -> i32 {
    if dest.len() < src.len() || !(1..=BLOSC2_MAXTYPESIZE).contains(&typesize) {
        return 1;
    }
    if !simd_available {
        return bytedelta_forward_core(typesize, src, dest);
    }

    // C-SIMD parity: the legacy plugin restarts the byte-delta predictor at
    // the scalar tail after each 16-byte vectorized stream segment.
    let stream_len = src.len() / typesize;
    for channel in 0..typesize {
        let base = channel * stream_len;
        let vectorizable_len = stream_len - (stream_len % 16);
        let mut previous = 0u8;
        for i in 0..vectorizable_len {
            let value = src[base + i];
            dest[base + i] = value.wrapping_sub(previous);
            previous = value;
        }

        previous = 0;
        for i in vectorizable_len..stream_len {
            let value = src[base + i];
            dest[base + i] = value.wrapping_sub(previous);
            previous = value;
        }
    }
    0
}

fn bytedelta_buggy_backward_core(typesize: usize, src: &[u8], dest: &mut [u8]) -> i32 {
    bytedelta_buggy_backward_core_with_simd(
        typesize,
        src,
        dest,
        bytedelta_buggy_simd_path_available(),
    )
}

fn bytedelta_buggy_backward_core_with_simd(
    typesize: usize,
    src: &[u8],
    dest: &mut [u8],
    simd_available: bool,
) -> i32 {
    if dest.len() < src.len() || !(1..=BLOSC2_MAXTYPESIZE).contains(&typesize) {
        return 1;
    }
    if !simd_available {
        return bytedelta_backward_core(typesize, src, dest);
    }

    // C-SIMD parity: keep the same predictor reset at the scalar tail so
    // compressed chunks from the legacy buggy plugin round-trip.
    let stream_len = src.len() / typesize;
    for channel in 0..typesize {
        let base = channel * stream_len;
        let vectorizable_len = stream_len - (stream_len % 16);
        let mut previous = 0u8;
        for i in 0..vectorizable_len {
            let value = src[base + i].wrapping_add(previous);
            dest[base + i] = value;
            previous = value;
        }

        previous = 0;
        for i in vectorizable_len..stream_len {
            let value = src[base + i].wrapping_add(previous);
            dest[base + i] = value;
            previous = value;
        }
    }
    0
}

fn bytedelta_buggy_simd_path_available() -> bool {
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    {
        return std::arch::is_x86_feature_detected!("ssse3");
    }
    #[cfg(target_arch = "aarch64")]
    {
        return true;
    }
    #[cfg(not(any(target_arch = "x86", target_arch = "x86_64", target_arch = "aarch64")))]
    {
        false
    }
}

fn bytedelta_buggy_forward_impl(
    ctx: &mut FilterCallbackContext<'_>,
    src: &[u8],
    dest: &mut [u8],
) -> i32 {
    let Some(typesize) = bytedelta_context_typesize(ctx) else {
        return BLOSC2_ERROR_FAILURE;
    };
    bytedelta_buggy_forward_core(typesize, src, dest)
}

fn bytedelta_buggy_backward_impl(
    ctx: &mut FilterCallbackContext<'_>,
    src: &[u8],
    dest: &mut [u8],
) -> i32 {
    let Some(typesize) = bytedelta_context_typesize(ctx) else {
        return BLOSC2_ERROR_FAILURE;
    };
    bytedelta_buggy_backward_core(typesize, src, dest)
}

fn int_trunc_forward_core(meta: u8, typesize: usize, src: &[u8], dest: &mut [u8]) -> i32 {
    if dest.len() < src.len() || !matches!(typesize, 1 | 2 | 4 | 8) {
        return 1;
    }

    let max_prec_bits = (typesize * 8) as i16;
    let prec_bits = meta as i8 as i16;
    let zeroed_bits = if prec_bits >= 0 {
        max_prec_bits - prec_bits
    } else {
        -prec_bits
    };
    if zeroed_bits < 0 || zeroed_bits >= max_prec_bits {
        return 1;
    }

    let mask = !((1u64 << zeroed_bits) - 1);
    let nelems = src.len() / typesize;
    let main_len = nelems * typesize;
    for (src_elem, dest_elem) in src[..main_len]
        .chunks_exact(typesize)
        .zip(dest[..main_len].chunks_exact_mut(typesize))
    {
        let value = match typesize {
            1 => u64::from(src_elem[0]),
            2 => u64::from(u16::from_ne_bytes([src_elem[0], src_elem[1]])),
            4 => u64::from(u32::from_ne_bytes([
                src_elem[0],
                src_elem[1],
                src_elem[2],
                src_elem[3],
            ])),
            8 => u64::from_ne_bytes([
                src_elem[0],
                src_elem[1],
                src_elem[2],
                src_elem[3],
                src_elem[4],
                src_elem[5],
                src_elem[6],
                src_elem[7],
            ]),
            _ => unreachable!("typesize was checked above"),
        } & mask;

        match typesize {
            1 => dest_elem[0] = value as u8,
            2 => dest_elem.copy_from_slice(&(value as u16).to_ne_bytes()),
            4 => dest_elem.copy_from_slice(&(value as u32).to_ne_bytes()),
            8 => dest_elem.copy_from_slice(&value.to_ne_bytes()),
            _ => unreachable!("typesize was checked above"),
        }
    }
    0
}

fn apply_int_trunc_filter(ctx: &mut FilterCallbackContext<'_>, src: &[u8], dest: &mut [u8]) -> i32 {
    let typesize = ctx
        .cparams
        .and_then(|cparams| usize::try_from(cparams.typesize).ok())
        .unwrap_or(ctx.typesize);
    match int_trunc_forward_core(ctx.meta, typesize, src, dest) {
        0 => 0,
        _ => BLOSC2_ERROR_FAILURE,
    }
}

fn undo_int_trunc_filter(_ctx: &mut FilterCallbackContext<'_>, src: &[u8], dest: &mut [u8]) -> i32 {
    if dest.len() < src.len() {
        return BLOSC2_ERROR_FAILURE;
    }
    dest[..src.len()].copy_from_slice(src);
    0
}

fn known_global_filter_by_id(filter_id: u8) -> Option<KnownGlobalFilter> {
    KNOWN_GLOBAL_FILTERS
        .iter()
        .copied()
        .find(|filter| filter.filter_id == filter_id)
}

fn known_global_filter_descriptor(filter_id: u8) -> Option<UserFilter> {
    let filter = known_global_filter_by_id(filter_id)?;
    let (forward, backward) = match filter.filter_id {
        #[cfg(feature = "plugin-ndcell")]
        BLOSC_FILTER_NDCELL => (
            UserFilterForward::Context(apply_ndcell_filter),
            UserFilterBackward::Context(undo_ndcell_filter),
        ),
        #[cfg(feature = "plugin-ndmean")]
        BLOSC_FILTER_NDMEAN => (
            UserFilterForward::Context(apply_ndmean_filter),
            UserFilterBackward::Context(undo_ndmean_filter),
        ),
        #[cfg(feature = "plugin-bytedelta")]
        BLOSC_FILTER_BYTEDELTA_BUGGY => (
            UserFilterForward::Context(bytedelta_buggy_forward_impl),
            UserFilterBackward::Context(bytedelta_buggy_backward_impl),
        ),
        #[cfg(feature = "plugin-bytedelta")]
        BLOSC_FILTER_BYTEDELTA => (
            UserFilterForward::Context(apply_bytedelta_filter),
            UserFilterBackward::Context(undo_bytedelta_filter),
        ),
        #[cfg(feature = "plugin-int-trunc")]
        BLOSC_FILTER_INT_TRUNC => (
            UserFilterForward::Context(apply_int_trunc_filter),
            UserFilterBackward::Context(undo_int_trunc_filter),
        ),
        _ => (
            UserFilterForward::Fallible(unsupported_known_global_filter),
            UserFilterBackward::Fallible(unsupported_known_global_filter),
        ),
    };
    Some(UserFilter {
        name: Some(filter.name),
        version: Some(filter.version),
        forward,
        backward,
    })
}

pub fn is_static_global_filter_enabled(filter_id: u8) -> bool {
    match filter_id {
        BLOSC_FILTER_NDCELL => cfg!(feature = "plugin-ndcell"),
        BLOSC_FILTER_NDMEAN => cfg!(feature = "plugin-ndmean"),
        BLOSC_FILTER_BYTEDELTA_BUGGY | BLOSC_FILTER_BYTEDELTA => cfg!(feature = "plugin-bytedelta"),
        BLOSC_FILTER_INT_TRUNC => cfg!(feature = "plugin-int-trunc"),
        _ => false,
    }
}

fn ensure_known_global_filters_registered() {
    KNOWN_GLOBAL_FILTERS_REGISTERED.get_or_init(|| {
        for filter in KNOWN_GLOBAL_FILTERS {
            let Some(descriptor) = known_global_filter_descriptor(filter.filter_id) else {
                continue;
            };
            let _ = register_named_filter_inner(
                filter.filter_id,
                descriptor,
                BLOSC2_GLOBAL_REGISTERED_FILTERS_START..=BLOSC2_GLOBAL_REGISTERED_FILTERS_STOP,
                "Global plugin filter IDs must be in 32..=159",
                "Global plugin filter ID already registered",
            );
        }
    });
}

/// Returns `true` for C-Blosc2 global plugin filter IDs known by this crate.
///
/// These entries provide C-compatible IDs and metadata. They do not imply that
/// the filter implementation has been ported.
pub fn is_known_global_filter(filter_id: u8) -> bool {
    known_global_filter_by_id(filter_id).is_some()
}

/// Returns `true` for global plugin filters that require live B2ND metadata.
///
/// These filters read the owning schunk's `"b2nd"` metalayer in C-Blosc2 to
/// recover ndim, shape, chunkshape, and blockshape. Rust callbacks receive the
/// same raw metalayer through [`FilterCallbackContext::b2nd_metalayer`].
pub fn global_filter_requires_b2nd_metadata(filter_id: u8) -> bool {
    matches!(filter_id, BLOSC_FILTER_NDCELL | BLOSC_FILTER_NDMEAN)
}

/// Return the C-shaped name/version descriptor for a known global plugin filter.
pub fn known_global_filter_info(filter_id: u8) -> Option<(&'static str, u8)> {
    let filter = known_global_filter_by_id(filter_id)?;
    Some((filter.name, filter.version))
}

/// Register a user-defined filter under `filter_id`.
///
/// `filter_id` must be at least [`BLOSC2_USER_DEFINED_FILTERS_START`]; lower IDs
/// are reserved for built-in and globally registered C-Blosc2 filters.
/// Re-registering an existing ID is rejected so existing chunks do not change
/// behavior after accidental callback replacement.
pub fn register_filter(
    filter_id: u8,
    forward: FilterForwardFn,
    backward: FilterBackwardFn,
) -> Result<(), &'static str> {
    register_filter_inner(
        filter_id,
        UserFilter {
            name: None,
            version: None,
            forward: UserFilterForward::Infallible(forward),
            backward: UserFilterBackward::Infallible(backward),
        },
        BLOSC2_USER_DEFINED_FILTERS_START..=u8::MAX,
        "User-defined filter IDs must be >= 160",
        "User-defined filter ID already registered",
    )
}

/// Register a fallible user-defined filter under `filter_id`.
///
/// The callbacks return `0` on success and non-zero on failure, mirroring
/// C-Blosc2's plugin callback convention. Existing infallible callbacks should
/// keep using [`register_filter`].
pub fn register_fallible_filter(
    filter_id: u8,
    forward: FallibleFilterForwardFn,
    backward: FallibleFilterBackwardFn,
) -> Result<(), &'static str> {
    register_filter_inner(
        filter_id,
        UserFilter {
            name: None,
            version: None,
            forward: UserFilterForward::Fallible(forward),
            backward: UserFilterBackward::Fallible(backward),
        },
        BLOSC2_USER_DEFINED_FILTERS_START..=u8::MAX,
        "User-defined filter IDs must be >= 160",
        "User-defined filter ID already registered",
    )
}

/// Register a user-defined filter with C-compatible contextual callbacks.
pub fn register_context_filter(
    filter_id: u8,
    forward: ContextFilterForwardFn,
    backward: ContextFilterBackwardFn,
) -> Result<(), &'static str> {
    register_filter_inner(
        filter_id,
        UserFilter {
            name: None,
            version: None,
            forward: UserFilterForward::Context(forward),
            backward: UserFilterBackward::Context(backward),
        },
        BLOSC2_USER_DEFINED_FILTERS_START..=u8::MAX,
        "User-defined filter IDs must be >= 160",
        "User-defined filter ID already registered",
    )
}

/// Register a Rust-shaped C-Blosc2 filter descriptor.
pub fn register_blosc2_filter(filter: &Blosc2Filter) -> i32 {
    register_blosc2_filter_c(Some(filter))
}

/// Nullable wrapper for [`register_blosc2_filter`].
pub fn register_blosc2_filter_c(filter: Option<&Blosc2Filter>) -> i32 {
    let Some(filter) = filter else {
        return BLOSC2_ERROR_INVALID_PARAM;
    };
    match register_blosc2_filter_impl(filter) {
        Ok(()) => BLOSC2_ERROR_SUCCESS,
        Err("User-defined filter IDs must be >= 160") => BLOSC2_ERROR_FAILURE,
        Err("User-defined filter ID already registered") => BLOSC2_ERROR_FAILURE,
        Err(_) => BLOSC2_ERROR_FAILURE,
    }
}

/// C-name registration wrapper for raw `blosc2_filter` descriptors.
///
/// This accepts the C callback shape:
/// `(input, output, length, meta, cparams/dparams, id)`.
pub fn blosc2_register_filter(filter: *const Blosc2FilterAbi) -> i32 {
    if filter.is_null() {
        return BLOSC2_ERROR_INVALID_PARAM;
    }
    let filter = unsafe { &*filter };
    match register_blosc2_filter_abi_impl(filter) {
        Ok(()) => BLOSC2_ERROR_SUCCESS,
        Err("User-defined filter IDs must be >= 160") => BLOSC2_ERROR_FAILURE,
        Err("User-defined filter ID already registered") => BLOSC2_ERROR_FAILURE,
        Err(_) => BLOSC2_ERROR_FAILURE,
    }
}

/// Backward-compatible alias for the raw C-shaped registration wrapper.
pub fn blosc2_register_filter_abi(filter: *const Blosc2FilterAbi) -> i32 {
    blosc2_register_filter(filter)
}

fn register_blosc2_filter_impl(filter: &Blosc2Filter) -> Result<(), &'static str> {
    if filter.id < BLOSC2_USER_DEFINED_FILTERS_START {
        return Err("User-defined filter IDs must be >= 160");
    }
    let mut filters = user_filters()
        .write()
        .map_err(|_| "Filter registry poisoned")?;
    if let Some(existing) = filters.get(&filter.id) {
        return if existing.name == Some(filter.name) {
            Ok(())
        } else {
            Err("User-defined filter ID already registered")
        };
    }
    filters.insert(
        filter.id,
        UserFilter {
            name: Some(filter.name),
            version: Some(filter.version),
            forward: UserFilterForward::Fallible(filter.forward),
            backward: UserFilterBackward::Fallible(filter.backward),
        },
    );
    Ok(())
}

fn blosc2_filter_abi_name(name: *const c_char) -> Option<&'static str> {
    if name.is_null() {
        return None;
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
    Some(Box::leak(name.into_boxed_str()))
}

fn register_blosc2_filter_abi_impl(filter: &Blosc2FilterAbi) -> Result<(), &'static str> {
    if filter.id < BLOSC2_USER_DEFINED_FILTERS_START {
        return Err("User-defined filter IDs must be >= 160");
    }
    let name = blosc2_filter_abi_name(filter.name);
    let mut filters = user_filters()
        .write()
        .map_err(|_| "Filter registry poisoned")?;
    if let Some(existing) = filters.get(&filter.id) {
        return if name.is_some() && existing.name == name {
            Ok(())
        } else {
            Err("User-defined filter ID already registered")
        };
    }
    filters.insert(
        filter.id,
        UserFilter {
            name,
            version: Some(filter.version),
            forward: UserFilterForward::CAbi(filter.forward),
            backward: UserFilterBackward::CAbi(filter.backward),
        },
    );
    Ok(())
}

fn register_filter_inner(
    filter_id: u8,
    filter: UserFilter,
    valid_range: std::ops::RangeInclusive<u8>,
    range_error: &'static str,
    duplicate_error: &'static str,
) -> Result<(), &'static str> {
    if !valid_range.contains(&filter_id) {
        return Err(range_error);
    }
    if let Some(known_filter) = known_global_filter_descriptor(filter_id) {
        return if known_filter.same_callbacks(filter) {
            Ok(())
        } else {
            Err(duplicate_error)
        };
    }
    let mut filters = user_filters()
        .write()
        .map_err(|_| "Filter registry poisoned")?;
    if let Some(existing) = filters.get(&filter_id) {
        return if existing.same_callbacks(filter) {
            Ok(())
        } else {
            Err(duplicate_error)
        };
    }
    filters.insert(filter_id, filter);
    Ok(())
}

fn register_named_filter_inner(
    filter_id: u8,
    filter: UserFilter,
    valid_range: std::ops::RangeInclusive<u8>,
    range_error: &'static str,
    duplicate_error: &'static str,
) -> Result<(), &'static str> {
    if !valid_range.contains(&filter_id) {
        return Err(range_error);
    }
    if let Some(known_filter) = known_global_filter_descriptor(filter_id) {
        if filter.name != known_filter.name {
            return Err(duplicate_error);
        }
        let mut filters = user_filters()
            .write()
            .map_err(|_| "Filter registry poisoned")?;
        filters.entry(filter_id).or_insert(known_filter);
        return Ok(());
    }
    let mut filters = user_filters()
        .write()
        .map_err(|_| "Filter registry poisoned")?;
    if let Some(existing) = filters.get(&filter_id) {
        return if filter.name.is_some() && existing.name == filter.name {
            Ok(())
        } else if filter.name.is_some() && existing.name != filter.name {
            Err(duplicate_error)
        } else if existing.same_callbacks(filter) {
            Ok(())
        } else {
            Err(duplicate_error)
        };
    }
    filters.insert(filter_id, filter);
    Ok(())
}

/// Register a global plugin filter under `filter_id`.
///
/// This mirrors C-Blosc2's internal plugin registration path: IDs 32..=159 are
/// accepted for globally registered plugins, while user-defined IDs still use
/// [`register_filter`]. Duplicate IDs are rejected because this Rust registry
/// has no separate plugin name to distinguish an idempotent re-registration from
/// an accidental replacement.
pub fn register_global_filter(
    filter_id: u8,
    forward: FilterForwardFn,
    backward: FilterBackwardFn,
) -> Result<(), &'static str> {
    register_filter_inner(
        filter_id,
        UserFilter {
            name: None,
            version: None,
            forward: UserFilterForward::Infallible(forward),
            backward: UserFilterBackward::Infallible(backward),
        },
        BLOSC2_GLOBAL_REGISTERED_FILTERS_START..=BLOSC2_GLOBAL_REGISTERED_FILTERS_STOP,
        "Global plugin filter IDs must be in 32..=159",
        "Global plugin filter ID already registered",
    )
}

/// Register a named global plugin filter under `filter_id`.
pub fn register_named_global_filter(
    filter_id: u8,
    name: &'static str,
    forward: FilterForwardFn,
    backward: FilterBackwardFn,
) -> Result<(), &'static str> {
    if name.is_empty() {
        return Err("Global plugin filter name cannot be empty");
    }
    register_named_filter_inner(
        filter_id,
        UserFilter {
            name: Some(name),
            version: None,
            forward: UserFilterForward::Infallible(forward),
            backward: UserFilterBackward::Infallible(backward),
        },
        BLOSC2_GLOBAL_REGISTERED_FILTERS_START..=BLOSC2_GLOBAL_REGISTERED_FILTERS_STOP,
        "Global plugin filter IDs must be in 32..=159",
        "Global plugin filter ID already registered",
    )
}

/// Register a named global plugin filter with C-style metadata.
pub fn register_global_filter_with_metadata(
    filter_id: u8,
    name: &'static str,
    version: u8,
    forward: FilterForwardFn,
    backward: FilterBackwardFn,
) -> Result<(), &'static str> {
    if name.is_empty() {
        return Err("Global plugin filter name cannot be empty");
    }
    register_named_filter_inner(
        filter_id,
        UserFilter {
            name: Some(name),
            version: Some(version),
            forward: UserFilterForward::Infallible(forward),
            backward: UserFilterBackward::Infallible(backward),
        },
        BLOSC2_GLOBAL_REGISTERED_FILTERS_START..=BLOSC2_GLOBAL_REGISTERED_FILTERS_STOP,
        "Global plugin filter IDs must be in 32..=159",
        "Global plugin filter ID already registered",
    )
}

/// Register a fallible global plugin filter under `filter_id`.
///
/// The callbacks return `0` on success and non-zero on failure, mirroring
/// C-Blosc2's plugin callback convention.
pub fn register_global_fallible_filter(
    filter_id: u8,
    forward: FallibleFilterForwardFn,
    backward: FallibleFilterBackwardFn,
) -> Result<(), &'static str> {
    register_filter_inner(
        filter_id,
        UserFilter {
            name: None,
            version: None,
            forward: UserFilterForward::Fallible(forward),
            backward: UserFilterBackward::Fallible(backward),
        },
        BLOSC2_GLOBAL_REGISTERED_FILTERS_START..=BLOSC2_GLOBAL_REGISTERED_FILTERS_STOP,
        "Global plugin filter IDs must be in 32..=159",
        "Global plugin filter ID already registered",
    )
}

/// Register a global plugin filter with C-compatible contextual callbacks.
pub fn register_global_context_filter(
    filter_id: u8,
    forward: ContextFilterForwardFn,
    backward: ContextFilterBackwardFn,
) -> Result<(), &'static str> {
    register_filter_inner(
        filter_id,
        UserFilter {
            name: None,
            version: None,
            forward: UserFilterForward::Context(forward),
            backward: UserFilterBackward::Context(backward),
        },
        BLOSC2_GLOBAL_REGISTERED_FILTERS_START..=BLOSC2_GLOBAL_REGISTERED_FILTERS_STOP,
        "Global plugin filter IDs must be in 32..=159",
        "Global plugin filter ID already registered",
    )
}

/// Register a named fallible global plugin filter under `filter_id`.
pub fn register_named_global_fallible_filter(
    filter_id: u8,
    name: &'static str,
    forward: FallibleFilterForwardFn,
    backward: FallibleFilterBackwardFn,
) -> Result<(), &'static str> {
    if name.is_empty() {
        return Err("Global plugin filter name cannot be empty");
    }
    register_named_filter_inner(
        filter_id,
        UserFilter {
            name: Some(name),
            version: None,
            forward: UserFilterForward::Fallible(forward),
            backward: UserFilterBackward::Fallible(backward),
        },
        BLOSC2_GLOBAL_REGISTERED_FILTERS_START..=BLOSC2_GLOBAL_REGISTERED_FILTERS_STOP,
        "Global plugin filter IDs must be in 32..=159",
        "Global plugin filter ID already registered",
    )
}

/// Register a named global plugin filter with C-compatible contextual callbacks.
pub fn register_named_global_context_filter(
    filter_id: u8,
    name: &'static str,
    forward: ContextFilterForwardFn,
    backward: ContextFilterBackwardFn,
) -> Result<(), &'static str> {
    if name.is_empty() {
        return Err("Global plugin filter name cannot be empty");
    }
    register_named_filter_inner(
        filter_id,
        UserFilter {
            name: Some(name),
            version: None,
            forward: UserFilterForward::Context(forward),
            backward: UserFilterBackward::Context(backward),
        },
        BLOSC2_GLOBAL_REGISTERED_FILTERS_START..=BLOSC2_GLOBAL_REGISTERED_FILTERS_STOP,
        "Global plugin filter IDs must be in 32..=159",
        "Global plugin filter ID already registered",
    )
}

/// Register a named fallible global plugin filter with C-style metadata.
pub fn register_global_fallible_filter_with_metadata(
    filter_id: u8,
    name: &'static str,
    version: u8,
    forward: FallibleFilterForwardFn,
    backward: FallibleFilterBackwardFn,
) -> Result<(), &'static str> {
    if name.is_empty() {
        return Err("Global plugin filter name cannot be empty");
    }
    register_named_filter_inner(
        filter_id,
        UserFilter {
            name: Some(name),
            version: Some(version),
            forward: UserFilterForward::Fallible(forward),
            backward: UserFilterBackward::Fallible(backward),
        },
        BLOSC2_GLOBAL_REGISTERED_FILTERS_START..=BLOSC2_GLOBAL_REGISTERED_FILTERS_STOP,
        "Global plugin filter IDs must be in 32..=159",
        "Global plugin filter ID already registered",
    )
}

/// Register a named global plugin filter with C-style metadata and contextual callbacks.
pub fn register_global_context_filter_with_metadata(
    filter_id: u8,
    name: &'static str,
    version: u8,
    forward: ContextFilterForwardFn,
    backward: ContextFilterBackwardFn,
) -> Result<(), &'static str> {
    if name.is_empty() {
        return Err("Global plugin filter name cannot be empty");
    }
    register_named_filter_inner(
        filter_id,
        UserFilter {
            name: Some(name),
            version: Some(version),
            forward: UserFilterForward::Context(forward),
            backward: UserFilterBackward::Context(backward),
        },
        BLOSC2_GLOBAL_REGISTERED_FILTERS_START..=BLOSC2_GLOBAL_REGISTERED_FILTERS_STOP,
        "Global plugin filter IDs must be in 32..=159",
        "Global plugin filter ID already registered",
    )
}

/// Return `true` if a user-defined filter has been registered under `filter_id`.
pub fn is_registered_filter(filter_id: u8) -> bool {
    ensure_known_global_filters_registered();
    user_filters()
        .read()
        .is_ok_and(|filters| filters.contains_key(&filter_id))
}

/// Return the name/version descriptor for a C-shaped registered filter.
pub fn registered_filter_info(filter_id: u8) -> Option<(&'static str, u8)> {
    ensure_known_global_filters_registered();
    user_filters().read().ok().and_then(|filters| {
        let filter = filters.get(&filter_id)?;
        Some((filter.name?, filter.version?))
    })
}

/// Look up a previously [`register_filter`]-ed callback pair by ID.
fn registered_filter(filter_id: u8) -> Option<UserFilter> {
    ensure_known_global_filters_registered();
    user_filters()
        .read()
        .ok()
        .and_then(|filters| filters.get(&filter_id).copied())
}

fn is_blosc_defined_filter(filter_id: u8) -> bool {
    (BLOSC2_DEFINED_FILTERS_START..=BLOSC2_DEFINED_FILTERS_STOP).contains(&filter_id)
}

/// Apply byte-wise shuffle: transpose bytes within elements of size `typesize`.
///
/// For each byte position `j` in `0..typesize`, all `j`-th bytes of the
/// elements are written contiguously into `dest`. This groups bytes of equal
/// positional significance, which typically improves the compressibility of
/// numerical data. Trailing bytes that do not form a complete element are
/// copied through unchanged.
///
/// Dispatches to SIMD or fixed-width implementations when available.
pub fn shuffle(typesize: usize, src: &[u8], dest: &mut [u8]) {
    let blocksize = src.len();
    if dest.len() < blocksize {
        return;
    }
    if typesize <= 1 || blocksize == 0 {
        dest[..blocksize].copy_from_slice(&src[..blocksize]);
        return;
    }
    if simd::try_shuffle(typesize, src, dest) {
        return;
    }
    if shuffle_common_width(typesize, src, dest) {
        return;
    }

    let neblock_quot = blocksize / typesize;
    let neblock_rem = blocksize % typesize;

    for j in 0..typesize {
        let dest_base = j * neblock_quot;
        for i in 0..neblock_quot {
            dest[dest_base + i] = src[i * typesize + j];
        }
    }

    if neblock_rem > 0 {
        let start = blocksize - neblock_rem;
        dest[start..blocksize].copy_from_slice(&src[start..blocksize]);
    }
}

/// Reverse byte-wise shuffle: untranspose bytes back into element-order.
///
/// Inverse of [`shuffle`]. `typesize` must match the value used at encode
/// time. Trailing bytes that do not form a complete element are copied
/// through unchanged.
pub fn unshuffle(typesize: usize, src: &[u8], dest: &mut [u8]) {
    let blocksize = src.len();
    if dest.len() < blocksize {
        return;
    }
    if typesize <= 1 || blocksize == 0 {
        dest[..blocksize].copy_from_slice(&src[..blocksize]);
        return;
    }
    if simd::try_unshuffle(typesize, src, dest) {
        return;
    }
    if unshuffle_common_width(typesize, src, dest) {
        return;
    }

    let neblock_quot = blocksize / typesize;
    let neblock_rem = blocksize % typesize;

    for i in 0..neblock_quot {
        let dest_base = i * typesize;
        for j in 0..typesize {
            dest[dest_base + j] = src[j * neblock_quot + i];
        }
    }

    if neblock_rem > 0 {
        let start = blocksize - neblock_rem;
        dest[start..blocksize].copy_from_slice(&src[start..blocksize]);
    }
}

fn validate_raw_shuffle_filter_args(
    typesize: i32,
    blocksize: i32,
    src_len: usize,
    dest_len: usize,
) -> Result<usize, i32> {
    if !(1..=256).contains(&typesize) || blocksize < 0 {
        return Err(BLOSC2_ERROR_INVALID_PARAM);
    }
    let blocksize = blocksize as usize;
    if src_len < blocksize || dest_len < blocksize {
        return Err(BLOSC2_ERROR_INVALID_PARAM);
    }
    Ok(blocksize)
}

fn validate_raw_bitshuffle_filter_args(
    typesize: i32,
    blocksize: i32,
    src_len: usize,
    dest_len: usize,
) -> Result<usize, i32> {
    if !(1..=256).contains(&typesize) || blocksize < 0 {
        return Err(BLOSC2_ERROR_INVALID_PARAM);
    }
    let blocksize = blocksize as usize;
    if src_len < blocksize || dest_len < blocksize {
        return Err(BLOSC2_ERROR_INVALID_PARAM);
    }
    Ok(blocksize)
}

fn validate_raw_bitunshuffle_filter_args(
    typesize: i32,
    blocksize: i32,
    src_len: usize,
    dest_len: usize,
) -> Result<usize, i32> {
    if typesize < 1 || blocksize < 0 {
        return Err(BLOSC2_ERROR_INVALID_PARAM);
    }
    let blocksize = blocksize as usize;
    if src_len < blocksize || dest_len < blocksize {
        return Err(BLOSC2_ERROR_INVALID_PARAM);
    }
    Ok(blocksize)
}

/// C-style raw shuffle wrapper: returns `blocksize` on success or a negative
/// `BLOSC2_ERROR_*` code on invalid parameters.
pub fn blosc2_shuffle(typesize: i32, blocksize: i32, src: &[u8], dest: &mut [u8]) -> i32 {
    let blocksize =
        match validate_raw_shuffle_filter_args(typesize, blocksize, src.len(), dest.len()) {
            Ok(blocksize) => blocksize,
            Err(code) => return code,
        };
    shuffle(typesize as usize, &src[..blocksize], &mut dest[..blocksize]);
    blocksize as i32
}

/// C-style raw unshuffle wrapper: returns `blocksize` on success or a negative
/// `BLOSC2_ERROR_*` code on invalid parameters.
pub fn blosc2_unshuffle(typesize: i32, blocksize: i32, src: &[u8], dest: &mut [u8]) -> i32 {
    let blocksize =
        match validate_raw_shuffle_filter_args(typesize, blocksize, src.len(), dest.len()) {
            Ok(blocksize) => blocksize,
            Err(code) => return code,
        };
    unshuffle(typesize as usize, &src[..blocksize], &mut dest[..blocksize]);
    blocksize as i32
}

/// Fast path for the common element sizes 2, 4, and 8. Returns `false`
/// otherwise so the caller falls back to the generic implementation.
fn shuffle_common_width(typesize: usize, src: &[u8], dest: &mut [u8]) -> bool {
    match typesize {
        2 => {
            shuffle2(src, dest);
            true
        }
        4 => {
            shuffle4(src, dest);
            true
        }
        8 => {
            shuffle8(src, dest);
            true
        }
        _ => false,
    }
}

/// Fast path for the common element sizes 2, 4, and 8 on the decode side.
fn unshuffle_common_width(typesize: usize, src: &[u8], dest: &mut [u8]) -> bool {
    match typesize {
        2 => {
            unshuffle2(src, dest);
            true
        }
        4 => {
            unshuffle4(src, dest);
            true
        }
        8 => {
            unshuffle8(src, dest);
            true
        }
        _ => false,
    }
}

/// Scalar shuffle specialized for 2-byte elements.
fn shuffle2(src: &[u8], dest: &mut [u8]) {
    let nelements = src.len() / 2;
    let main_len = nelements * 2;
    let (d0, d1) = dest[..main_len].split_at_mut(nelements);
    for (i, element) in src[..main_len].chunks_exact(2).enumerate() {
        d0[i] = element[0];
        d1[i] = element[1];
    }
    dest[main_len..src.len()].copy_from_slice(&src[main_len..]);
}

/// Scalar unshuffle specialized for 2-byte elements.
fn unshuffle2(src: &[u8], dest: &mut [u8]) {
    let nelements = src.len() / 2;
    let main_len = nelements * 2;
    let (s0, s1) = src[..main_len].split_at(nelements);
    // SAFETY: main_len is derived from src.len() and dest was checked by
    // unshuffle(), so every unaligned element write lands within dest.
    unsafe {
        let out = dest.as_mut_ptr();
        for i in 0..nelements {
            let value = u16::from_ne_bytes([s0[i], s1[i]]);
            std::ptr::write_unaligned(out.add(i * 2).cast::<u16>(), value);
        }
    }
    dest[main_len..src.len()].copy_from_slice(&src[main_len..]);
}

/// Scalar shuffle specialized for 4-byte elements.
fn shuffle4(src: &[u8], dest: &mut [u8]) {
    let nelements = src.len() / 4;
    let main_len = nelements * 4;
    let (d0, rest) = dest[..main_len].split_at_mut(nelements);
    let (d1, rest) = rest.split_at_mut(nelements);
    let (d2, d3) = rest.split_at_mut(nelements);
    for (i, element) in src[..main_len].chunks_exact(4).enumerate() {
        d0[i] = element[0];
        d1[i] = element[1];
        d2[i] = element[2];
        d3[i] = element[3];
    }
    dest[main_len..src.len()].copy_from_slice(&src[main_len..]);
}

/// Scalar unshuffle specialized for 4-byte elements.
fn unshuffle4(src: &[u8], dest: &mut [u8]) {
    let nelements = src.len() / 4;
    let main_len = nelements * 4;
    let (s0, rest) = src[..main_len].split_at(nelements);
    let (s1, rest) = rest.split_at(nelements);
    let (s2, s3) = rest.split_at(nelements);
    // SAFETY: main_len is derived from src.len() and dest was checked by
    // unshuffle(), so every unaligned element write lands within dest.
    unsafe {
        let out = dest.as_mut_ptr();
        for i in 0..nelements {
            let value = u32::from_ne_bytes([s0[i], s1[i], s2[i], s3[i]]);
            std::ptr::write_unaligned(out.add(i * 4).cast::<u32>(), value);
        }
    }
    dest[main_len..src.len()].copy_from_slice(&src[main_len..]);
}

/// Scalar shuffle specialized for 8-byte elements.
fn shuffle8(src: &[u8], dest: &mut [u8]) {
    let nelements = src.len() / 8;
    let main_len = nelements * 8;
    let (d0, rest) = dest[..main_len].split_at_mut(nelements);
    let (d1, rest) = rest.split_at_mut(nelements);
    let (d2, rest) = rest.split_at_mut(nelements);
    let (d3, rest) = rest.split_at_mut(nelements);
    let (d4, rest) = rest.split_at_mut(nelements);
    let (d5, rest) = rest.split_at_mut(nelements);
    let (d6, d7) = rest.split_at_mut(nelements);
    for (i, element) in src[..main_len].chunks_exact(8).enumerate() {
        d0[i] = element[0];
        d1[i] = element[1];
        d2[i] = element[2];
        d3[i] = element[3];
        d4[i] = element[4];
        d5[i] = element[5];
        d6[i] = element[6];
        d7[i] = element[7];
    }
    dest[main_len..src.len()].copy_from_slice(&src[main_len..]);
}

/// Scalar unshuffle specialized for 8-byte elements.
fn unshuffle8(src: &[u8], dest: &mut [u8]) {
    let nelements = src.len() / 8;
    let main_len = nelements * 8;
    let (s0, rest) = src[..main_len].split_at(nelements);
    let (s1, rest) = rest.split_at(nelements);
    let (s2, rest) = rest.split_at(nelements);
    let (s3, rest) = rest.split_at(nelements);
    let (s4, rest) = rest.split_at(nelements);
    let (s5, rest) = rest.split_at(nelements);
    let (s6, s7) = rest.split_at(nelements);
    // SAFETY: main_len is derived from src.len() and dest was checked by
    // unshuffle(), so every unaligned element write lands within dest.
    unsafe {
        let out = dest.as_mut_ptr();
        for i in 0..nelements {
            let value =
                u64::from_ne_bytes([s0[i], s1[i], s2[i], s3[i], s4[i], s5[i], s6[i], s7[i]]);
            std::ptr::write_unaligned(out.add(i * 8).cast::<u64>(), value);
        }
    }
    dest[main_len..src.len()].copy_from_slice(&src[main_len..]);
}

/// x86/x86_64 SIMD accelerations for byte-wise shuffle/unshuffle.
///
/// Each `try_*` entry point checks the runtime feature flags and falls back to
/// `false` if it cannot handle the given typesize/block-size, in which case
/// the scalar implementations take over.
#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
mod simd {
    #[cfg(target_arch = "x86")]
    use std::arch::x86 as arch;
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64 as arch;

    /// Try to run a SIMD shuffle. Returns `true` if `dest` was written.
    pub fn try_shuffle(typesize: usize, src: &[u8], dest: &mut [u8]) -> bool {
        if try_shuffle_avx2(typesize, src, dest) {
            return true;
        }
        if typesize != 4 || src.len() < 64 || dest.len() < src.len() {
            return false;
        }
        if !std::arch::is_x86_feature_detected!("sse2") {
            return false;
        }

        // SAFETY: The wrapper checks that src/dest cover src.len(), that the
        // element width matches this implementation, and that SSE2 is present.
        unsafe {
            shuffle4_sse2(src, dest);
        }
        true
    }

    /// AVX2 shuffle entry point for the common C-optimized widths.
    fn try_shuffle_avx2(typesize: usize, src: &[u8], dest: &mut [u8]) -> bool {
        if dest.len() < src.len() || !matches!(typesize, 2 | 4 | 8) {
            return false;
        }
        if src.len() < typesize * std::mem::size_of::<arch::__m256i>() {
            return false;
        }
        if !std::arch::is_x86_feature_detected!("avx2") {
            return false;
        }

        unsafe {
            shuffle_avx2(typesize, src, dest);
        }
        true
    }

    /// Try to run a SIMD unshuffle. Returns `true` if `dest` was written.
    pub fn try_unshuffle(typesize: usize, src: &[u8], dest: &mut [u8]) -> bool {
        if try_unshuffle_avx2(typesize, src, dest) {
            return true;
        }
        if typesize != 4 || src.len() < 64 || dest.len() < src.len() {
            return false;
        }
        if !std::arch::is_x86_feature_detected!("sse2") {
            return false;
        }

        // SAFETY: The wrapper checks that src/dest cover src.len(), that the
        // element width matches this implementation, and that SSE2 is present.
        unsafe {
            unshuffle4_sse2(src, dest);
        }
        true
    }

    /// AVX2 unshuffle entry point for the common C-optimized widths.
    fn try_unshuffle_avx2(typesize: usize, src: &[u8], dest: &mut [u8]) -> bool {
        if dest.len() < src.len() || !matches!(typesize, 2 | 4 | 8) {
            return false;
        }
        if src.len() < typesize * std::mem::size_of::<arch::__m256i>() {
            return false;
        }
        if !std::arch::is_x86_feature_detected!("avx2") {
            return false;
        }

        // SAFETY: The wrapper checks destination length, supported element
        // widths, full-element input, and AVX2 availability.
        unsafe {
            unshuffle_avx2(typesize, src, dest);
        }
        true
    }

    /// AVX2 byte-unshuffle dispatcher for the C-optimized widths.
    #[target_feature(enable = "avx2")]
    unsafe fn unshuffle_avx2(typesize: usize, src: &[u8], dest: &mut [u8]) {
        match typesize {
            2 => unsafe { unshuffle2_avx2(src, dest) },
            4 => unsafe { unshuffle4_avx2(src, dest) },
            8 => unsafe { unshuffle8_avx2(src, dest) },
            _ => unreachable!("try_unshuffle_avx2 filters unsupported widths"),
        }
    }

    /// AVX2 byte-shuffle dispatcher for the C-optimized widths.
    #[target_feature(enable = "avx2")]
    unsafe fn shuffle_avx2(typesize: usize, src: &[u8], dest: &mut [u8]) {
        match typesize {
            2 => unsafe { shuffle2_avx2(src, dest) },
            4 => unsafe { shuffle4_avx2(src, dest) },
            8 => unsafe { shuffle8_avx2(src, dest) },
            _ => unreachable!("try_shuffle_avx2 filters unsupported widths"),
        }
    }

    /// AVX2 shuffle kernel for 2-byte elements.
    #[target_feature(enable = "avx2")]
    unsafe fn shuffle2_avx2(src: &[u8], dest: &mut [u8]) {
        const BYTESOFTYPE: usize = 2;
        let blocksize = src.len();
        let vectorized_chunk_size = BYTESOFTYPE * std::mem::size_of::<arch::__m256i>();
        let vectorizable_bytes = blocksize - (blocksize % vectorized_chunk_size);
        let vectorizable_elements = vectorizable_bytes / BYTESOFTYPE;
        let total_elements = blocksize / BYTESOFTYPE;

        let src_ptr = src.as_ptr();
        let dest_ptr = dest.as_mut_ptr();
        let shmask = arch::_mm256_set_epi8(
            0x0f, 0x0d, 0x0b, 0x09, 0x07, 0x05, 0x03, 0x01, 0x0e, 0x0c, 0x0a, 0x08, 0x06, 0x04,
            0x02, 0x00, 0x0f, 0x0d, 0x0b, 0x09, 0x07, 0x05, 0x03, 0x01, 0x0e, 0x0c, 0x0a, 0x08,
            0x06, 0x04, 0x02, 0x00,
        );

        let mut j = 0usize;
        while j < vectorizable_elements {
            let ymm0 = [
                arch::_mm256_loadu_si256(src_ptr.add(j * BYTESOFTYPE) as *const arch::__m256i),
                arch::_mm256_loadu_si256(
                    src_ptr.add(j * BYTESOFTYPE + std::mem::size_of::<arch::__m256i>())
                        as *const arch::__m256i,
                ),
            ];
            let mut ymm1 = [
                arch::_mm256_shuffle_epi8(ymm0[0], shmask),
                arch::_mm256_shuffle_epi8(ymm0[1], shmask),
            ];

            let p0 = arch::_mm256_permute4x64_epi64::<0xD8>(ymm1[0]);
            let p1 = arch::_mm256_permute4x64_epi64::<0x8D>(ymm1[1]);
            ymm1[0] = arch::_mm256_blend_epi32::<0xF0>(p0, p1);
            let mixed = arch::_mm256_blend_epi32::<0x0F>(p0, p1);
            ymm1[1] = arch::_mm256_permute4x64_epi64::<0x4E>(mixed);

            for (k, reg) in ymm1.iter().enumerate() {
                arch::_mm256_storeu_si256(
                    dest_ptr.add(j + k * total_elements) as *mut arch::__m256i,
                    *reg,
                );
            }
            j += std::mem::size_of::<arch::__m256i>();
        }

        for byte_idx in 0..BYTESOFTYPE {
            let dest_base = byte_idx * total_elements;
            for element in vectorizable_elements..total_elements {
                dest[dest_base + element] = src[element * BYTESOFTYPE + byte_idx];
            }
        }

        let tail_start = total_elements * BYTESOFTYPE;
        if tail_start < blocksize {
            dest[tail_start..blocksize].copy_from_slice(&src[tail_start..blocksize]);
        }
    }

    /// AVX2 shuffle kernel for 4-byte elements.
    ///
    /// Loads four 256-bit vectors at a time, interleaves with `shuffle_epi32`
    /// and `unpack` instructions to assemble four byte planes, and uses
    /// `permutevar8x32_epi32` to restore sequential lane order.
    #[target_feature(enable = "avx2")]
    unsafe fn shuffle4_avx2(src: &[u8], dest: &mut [u8]) {
        const BYTESOFTYPE: usize = 4;
        let blocksize = src.len();
        let vectorized_chunk_size = BYTESOFTYPE * std::mem::size_of::<arch::__m256i>();
        let vectorizable_bytes = blocksize - (blocksize % vectorized_chunk_size);
        let vectorizable_elements = vectorizable_bytes / BYTESOFTYPE;
        let total_elements = blocksize / BYTESOFTYPE;

        let src_ptr = src.as_ptr();
        let dest_ptr = dest.as_mut_ptr();
        let mask = arch::_mm256_set_epi32(0x07, 0x03, 0x06, 0x02, 0x05, 0x01, 0x04, 0x00);

        let mut i = 0usize;
        while i < vectorizable_elements {
            let mut ymm0 = [arch::_mm256_setzero_si256(); 4];
            let mut ymm1 = [arch::_mm256_setzero_si256(); 4];

            for j in 0..4 {
                ymm0[j] = arch::_mm256_loadu_si256(
                    src_ptr.add(i * BYTESOFTYPE + j * std::mem::size_of::<arch::__m256i>())
                        as *const arch::__m256i,
                );
                ymm1[j] = arch::_mm256_shuffle_epi32::<0xD8>(ymm0[j]);
                ymm0[j] = arch::_mm256_shuffle_epi32::<0x8D>(ymm0[j]);
                ymm0[j] = arch::_mm256_unpacklo_epi8(ymm1[j], ymm0[j]);
                ymm1[j] = arch::_mm256_shuffle_epi32::<0x4E>(ymm0[j]);
                ymm0[j] = arch::_mm256_unpacklo_epi16(ymm0[j], ymm1[j]);
            }

            for j in 0..2 {
                ymm1[j * 2] = arch::_mm256_unpacklo_epi32(ymm0[j * 2], ymm0[j * 2 + 1]);
                ymm1[j * 2 + 1] = arch::_mm256_unpackhi_epi32(ymm0[j * 2], ymm0[j * 2 + 1]);
            }
            for j in 0..2 {
                ymm0[j * 2] = arch::_mm256_unpacklo_epi64(ymm1[j], ymm1[j + 2]);
                ymm0[j * 2 + 1] = arch::_mm256_unpackhi_epi64(ymm1[j], ymm1[j + 2]);
            }
            for reg in &mut ymm0 {
                *reg = arch::_mm256_permutevar8x32_epi32(*reg, mask);
            }

            for (j, reg) in ymm0.iter().enumerate() {
                arch::_mm256_storeu_si256(
                    dest_ptr.add(i + j * total_elements) as *mut arch::__m256i,
                    *reg,
                );
            }
            i += std::mem::size_of::<arch::__m256i>();
        }

        for byte_idx in 0..BYTESOFTYPE {
            let dest_base = byte_idx * total_elements;
            for element in vectorizable_elements..total_elements {
                dest[dest_base + element] = src[element * BYTESOFTYPE + byte_idx];
            }
        }

        let tail_start = total_elements * BYTESOFTYPE;
        if tail_start < blocksize {
            dest[tail_start..blocksize].copy_from_slice(&src[tail_start..blocksize]);
        }
    }

    /// AVX2 shuffle kernel for 8-byte elements.
    #[target_feature(enable = "avx2")]
    unsafe fn shuffle8_avx2(src: &[u8], dest: &mut [u8]) {
        const BYTESOFTYPE: usize = 8;
        let blocksize = src.len();
        let vectorized_chunk_size = BYTESOFTYPE * std::mem::size_of::<arch::__m256i>();
        let vectorizable_bytes = blocksize - (blocksize % vectorized_chunk_size);
        let vectorizable_elements = vectorizable_bytes / BYTESOFTYPE;
        let total_elements = blocksize / BYTESOFTYPE;

        let src_ptr = src.as_ptr();
        let dest_ptr = dest.as_mut_ptr();

        let mut j = 0usize;
        while j < vectorizable_elements {
            let mut ymm0 = [arch::_mm256_setzero_si256(); 8];
            let mut ymm1 = [arch::_mm256_setzero_si256(); 8];

            for k in 0..8 {
                ymm0[k] = arch::_mm256_loadu_si256(
                    src_ptr.add(j * BYTESOFTYPE + k * std::mem::size_of::<arch::__m256i>())
                        as *const arch::__m256i,
                );
                ymm1[k] = arch::_mm256_shuffle_epi32::<0x4E>(ymm0[k]);
                ymm1[k] = arch::_mm256_unpacklo_epi8(ymm0[k], ymm1[k]);
            }
            for (k, l) in (0..4).zip((0..8).step_by(2)) {
                ymm0[k * 2] = arch::_mm256_unpacklo_epi16(ymm1[l], ymm1[l + 1]);
                ymm0[k * 2 + 1] = arch::_mm256_unpackhi_epi16(ymm1[l], ymm1[l + 1]);
            }
            for k in 0..4 {
                let l = if k < 2 { k } else { k + 2 };
                ymm1[k * 2] = arch::_mm256_unpacklo_epi32(ymm0[l], ymm0[l + 2]);
                ymm1[k * 2 + 1] = arch::_mm256_unpackhi_epi32(ymm0[l], ymm0[l + 2]);
            }
            for k in 0..4 {
                ymm0[k * 2] = arch::_mm256_unpacklo_epi64(ymm1[k], ymm1[k + 4]);
                ymm0[k * 2 + 1] = arch::_mm256_unpackhi_epi64(ymm1[k], ymm1[k + 4]);
            }
            for k in 0..8 {
                ymm1[k] = arch::_mm256_permute4x64_epi64::<0x72>(ymm0[k]);
                ymm0[k] = arch::_mm256_permute4x64_epi64::<0xD8>(ymm0[k]);
                ymm0[k] = arch::_mm256_unpacklo_epi16(ymm0[k], ymm1[k]);
            }

            for (k, reg) in ymm0.iter().enumerate() {
                arch::_mm256_storeu_si256(
                    dest_ptr.add(j + k * total_elements) as *mut arch::__m256i,
                    *reg,
                );
            }
            j += std::mem::size_of::<arch::__m256i>();
        }

        for byte_idx in 0..BYTESOFTYPE {
            let dest_base = byte_idx * total_elements;
            for element in vectorizable_elements..total_elements {
                dest[dest_base + element] = src[element * BYTESOFTYPE + byte_idx];
            }
        }

        let tail_start = total_elements * BYTESOFTYPE;
        if tail_start < blocksize {
            dest[tail_start..blocksize].copy_from_slice(&src[tail_start..blocksize]);
        }
    }

    /// AVX2 unshuffle kernel for 2-byte elements.
    #[target_feature(enable = "avx2")]
    unsafe fn unshuffle2_avx2(src: &[u8], dest: &mut [u8]) {
        const BYTESOFTYPE: usize = 2;
        let blocksize = src.len();
        let total_elements = blocksize / BYTESOFTYPE;
        let vectorizable_elements =
            total_elements - (total_elements % std::mem::size_of::<arch::__m256i>());

        let src_ptr = src.as_ptr();
        let dst_ptr = dest.as_mut_ptr();

        let mut i = 0usize;
        while i < vectorizable_elements {
            let mut ymm0 = [
                arch::_mm256_loadu_si256(src_ptr.add(i) as *const arch::__m256i),
                arch::_mm256_loadu_si256(src_ptr.add(i + total_elements) as *const arch::__m256i),
            ];
            for reg in &mut ymm0 {
                *reg = arch::_mm256_permute4x64_epi64::<0xD8>(*reg);
            }
            let out0 = arch::_mm256_unpacklo_epi8(ymm0[0], ymm0[1]);
            let out1 = arch::_mm256_unpackhi_epi8(ymm0[0], ymm0[1]);
            arch::_mm256_storeu_si256(dst_ptr.add(i * BYTESOFTYPE) as *mut arch::__m256i, out0);
            arch::_mm256_storeu_si256(
                dst_ptr.add(i * BYTESOFTYPE + std::mem::size_of::<arch::__m256i>())
                    as *mut arch::__m256i,
                out1,
            );
            i += std::mem::size_of::<arch::__m256i>();
        }

        for element in vectorizable_elements..total_elements {
            let dest_base = element * BYTESOFTYPE;
            dest[dest_base] = src[element];
            dest[dest_base + 1] = src[element + total_elements];
        }

        let tail_start = total_elements * BYTESOFTYPE;
        if tail_start < blocksize {
            dest[tail_start..blocksize].copy_from_slice(&src[tail_start..blocksize]);
        }
    }

    /// AVX2 unshuffle kernel for 4-byte elements.
    ///
    /// Loads 32 bytes from each of the four byte planes, interleaves them with
    /// `unpacklo/unpackhi_epi8` and `_epi16`, then permutes 128-bit lanes to
    /// produce 128 bytes of element-ordered output per iteration.
    #[target_feature(enable = "avx2")]
    unsafe fn unshuffle4_avx2(src: &[u8], dest: &mut [u8]) {
        const BYTESOFTYPE: usize = 4;
        let blocksize = src.len();
        let total_elements = blocksize / BYTESOFTYPE;
        // AVX2 path processes 32 elements (128 bytes output) per iteration.
        let vectorizable_elements = total_elements - (total_elements % 32);

        let src_ptr = src.as_ptr();
        let dst_ptr = dest.as_mut_ptr();
        let dst_aligned = (dst_ptr as usize & 31) == 0;

        // SAFETY: vectorizable_elements is a multiple of 32; loop reads
        // `total_elements` bytes from each of 4 planes, all within src, and
        // writes `vectorizable_elements * 4` bytes to dest from the start.
        unsafe {
            let mut i = 0usize;
            while i < vectorizable_elements {
                let pf = i + 64;
                if pf < vectorizable_elements {
                    arch::_mm_prefetch(src_ptr.add(pf) as *const i8, arch::_MM_HINT_T0);
                    arch::_mm_prefetch(
                        src_ptr.add(pf + total_elements) as *const i8,
                        arch::_MM_HINT_T0,
                    );
                    arch::_mm_prefetch(
                        src_ptr.add(pf + 2 * total_elements) as *const i8,
                        arch::_MM_HINT_T0,
                    );
                    arch::_mm_prefetch(
                        src_ptr.add(pf + 3 * total_elements) as *const i8,
                        arch::_MM_HINT_T0,
                    );
                }

                // Load 32 bytes from each of 4 byte-planes.
                let ymm0_0 = arch::_mm256_loadu_si256(src_ptr.add(i) as *const arch::__m256i);
                let ymm0_1 = arch::_mm256_loadu_si256(
                    src_ptr.add(i + total_elements) as *const arch::__m256i
                );
                let ymm0_2 = arch::_mm256_loadu_si256(
                    src_ptr.add(i + 2 * total_elements) as *const arch::__m256i
                );
                let ymm0_3 = arch::_mm256_loadu_si256(
                    src_ptr.add(i + 3 * total_elements) as *const arch::__m256i
                );

                // Interleave bytes from adjacent planes (byte-level transpose step 1).
                let ymm1_0 = arch::_mm256_unpacklo_epi8(ymm0_0, ymm0_1);
                let ymm1_1 = arch::_mm256_unpacklo_epi8(ymm0_2, ymm0_3);
                let ymm1_2 = arch::_mm256_unpackhi_epi8(ymm0_0, ymm0_1);
                let ymm1_3 = arch::_mm256_unpackhi_epi8(ymm0_2, ymm0_3);

                // Interleave 2-byte words (byte-level transpose step 2).
                let y0 = arch::_mm256_unpacklo_epi16(ymm1_0, ymm1_1);
                let y1 = arch::_mm256_unpacklo_epi16(ymm1_2, ymm1_3);
                let y2 = arch::_mm256_unpackhi_epi16(ymm1_0, ymm1_1);
                let y3 = arch::_mm256_unpackhi_epi16(ymm1_2, ymm1_3);

                // Re-order 128-bit lanes to restore sequential element order.
                let out0 = arch::_mm256_permute2x128_si256::<0x20>(y0, y2);
                let out1 = arch::_mm256_permute2x128_si256::<0x20>(y1, y3);
                let out2 = arch::_mm256_permute2x128_si256::<0x31>(y0, y2);
                let out3 = arch::_mm256_permute2x128_si256::<0x31>(y1, y3);

                let dst_base = dst_ptr.add(i * BYTESOFTYPE);
                if dst_aligned {
                    arch::_mm256_store_si256(dst_base as *mut arch::__m256i, out0);
                    arch::_mm256_store_si256(dst_base.add(32) as *mut arch::__m256i, out1);
                    arch::_mm256_store_si256(dst_base.add(64) as *mut arch::__m256i, out2);
                    arch::_mm256_store_si256(dst_base.add(96) as *mut arch::__m256i, out3);
                } else {
                    arch::_mm256_storeu_si256(dst_base as *mut arch::__m256i, out0);
                    arch::_mm256_storeu_si256(dst_base.add(32) as *mut arch::__m256i, out1);
                    arch::_mm256_storeu_si256(dst_base.add(64) as *mut arch::__m256i, out2);
                    arch::_mm256_storeu_si256(dst_base.add(96) as *mut arch::__m256i, out3);
                }

                i += 32;
            }
        }

        // Scalar tail for any leftover elements (< 32).
        for element in vectorizable_elements..total_elements {
            let dest_base = element * BYTESOFTYPE;
            dest[dest_base] = src[element];
            dest[dest_base + 1] = src[element + total_elements];
            dest[dest_base + 2] = src[element + 2 * total_elements];
            dest[dest_base + 3] = src[element + 3 * total_elements];
        }

        let tail_start = total_elements * BYTESOFTYPE;
        if tail_start < blocksize {
            dest[tail_start..blocksize].copy_from_slice(&src[tail_start..blocksize]);
        }
    }

    /// AVX2 unshuffle kernel for 8-byte elements.
    #[target_feature(enable = "avx2")]
    unsafe fn unshuffle8_avx2(src: &[u8], dest: &mut [u8]) {
        const BYTESOFTYPE: usize = 8;
        let blocksize = src.len();
        let total_elements = blocksize / BYTESOFTYPE;
        let vectorizable_elements =
            total_elements - (total_elements % std::mem::size_of::<arch::__m256i>());

        let src_ptr = src.as_ptr();
        let dst_ptr = dest.as_mut_ptr();

        let mut i = 0usize;
        while i < vectorizable_elements {
            let mut ymm0 = [arch::_mm256_setzero_si256(); 8];
            let mut ymm1 = [arch::_mm256_setzero_si256(); 8];
            for j in 0..8 {
                ymm0[j] = arch::_mm256_loadu_si256(
                    src_ptr.add(i + j * total_elements) as *const arch::__m256i
                );
            }
            for j in 0..4 {
                ymm1[j] = arch::_mm256_unpacklo_epi8(ymm0[j * 2], ymm0[j * 2 + 1]);
                ymm1[4 + j] = arch::_mm256_unpackhi_epi8(ymm0[j * 2], ymm0[j * 2 + 1]);
            }
            for j in 0..4 {
                ymm0[j] = arch::_mm256_unpacklo_epi16(ymm1[j * 2], ymm1[j * 2 + 1]);
                ymm0[4 + j] = arch::_mm256_unpackhi_epi16(ymm1[j * 2], ymm1[j * 2 + 1]);
            }
            for reg in &mut ymm0 {
                *reg = arch::_mm256_permute4x64_epi64::<0xD8>(*reg);
            }
            for j in 0..4 {
                ymm1[j] = arch::_mm256_unpacklo_epi32(ymm0[j * 2], ymm0[j * 2 + 1]);
                ymm1[4 + j] = arch::_mm256_unpackhi_epi32(ymm0[j * 2], ymm0[j * 2 + 1]);
            }

            let order = [0usize, 2, 1, 3, 4, 6, 5, 7];
            for (store_idx, &reg_idx) in order.iter().enumerate() {
                arch::_mm256_storeu_si256(
                    dst_ptr.add(i * BYTESOFTYPE + store_idx * std::mem::size_of::<arch::__m256i>())
                        as *mut arch::__m256i,
                    ymm1[reg_idx],
                );
            }
            i += std::mem::size_of::<arch::__m256i>();
        }

        for element in vectorizable_elements..total_elements {
            let dest_base = element * BYTESOFTYPE;
            for byte_idx in 0..BYTESOFTYPE {
                dest[dest_base + byte_idx] = src[byte_idx * total_elements + element];
            }
        }

        let tail_start = total_elements * BYTESOFTYPE;
        if tail_start < blocksize {
            dest[tail_start..blocksize].copy_from_slice(&src[tail_start..blocksize]);
        }
    }

    /// SSE2 shuffle kernel for 4-byte elements, processing 16 input bytes
    /// (4 elements) per iteration and scattering them across 4 byte planes.
    #[target_feature(enable = "sse2")]
    unsafe fn shuffle4_sse2(src: &[u8], dest: &mut [u8]) {
        let blocksize = src.len();
        let nelements = blocksize / 4;
        let simd_elements = nelements - (nelements % 4);

        for group in 0..(simd_elements / 4) {
            let src_base = group * 16;
            let vec = unsafe {
                arch::_mm_loadu_si128(src.as_ptr().add(src_base) as *const arch::__m128i)
            };
            let mut bytes = [0u8; 16];
            unsafe {
                arch::_mm_storeu_si128(bytes.as_mut_ptr() as *mut arch::__m128i, vec);
            }
            let elem_base = group * 4;
            for lane in 0..4 {
                dest[elem_base + lane] = bytes[lane * 4];
                dest[nelements + elem_base + lane] = bytes[lane * 4 + 1];
                dest[nelements * 2 + elem_base + lane] = bytes[lane * 4 + 2];
                dest[nelements * 3 + elem_base + lane] = bytes[lane * 4 + 3];
            }
        }

        for element in simd_elements..nelements {
            let src_base = element * 4;
            dest[element] = src[src_base];
            dest[nelements + element] = src[src_base + 1];
            dest[nelements * 2 + element] = src[src_base + 2];
            dest[nelements * 3 + element] = src[src_base + 3];
        }

        let tail_start = nelements * 4;
        if tail_start < blocksize {
            dest[tail_start..blocksize].copy_from_slice(&src[tail_start..blocksize]);
        }
    }

    /// SSE2 unshuffle kernel for 4-byte elements, gathering one byte from
    /// each of the 4 planes into 16-byte vectors and storing them back as
    /// element-ordered output.
    #[target_feature(enable = "sse2")]
    unsafe fn unshuffle4_sse2(src: &[u8], dest: &mut [u8]) {
        let blocksize = src.len();
        let nelements = blocksize / 4;
        let simd_elements = nelements - (nelements % 4);

        for group in 0..(simd_elements / 4) {
            let elem_base = group * 4;
            let mut bytes = [0u8; 16];
            for lane in 0..4 {
                bytes[lane * 4] = src[elem_base + lane];
                bytes[lane * 4 + 1] = src[nelements + elem_base + lane];
                bytes[lane * 4 + 2] = src[nelements * 2 + elem_base + lane];
                bytes[lane * 4 + 3] = src[nelements * 3 + elem_base + lane];
            }
            let vec = unsafe { arch::_mm_loadu_si128(bytes.as_ptr() as *const arch::__m128i) };
            unsafe {
                arch::_mm_storeu_si128(
                    dest.as_mut_ptr().add(group * 16) as *mut arch::__m128i,
                    vec,
                );
            }
        }

        for element in simd_elements..nelements {
            let dest_base = element * 4;
            dest[dest_base] = src[element];
            dest[dest_base + 1] = src[nelements + element];
            dest[dest_base + 2] = src[nelements * 2 + element];
            dest[dest_base + 3] = src[nelements * 3 + element];
        }

        let tail_start = nelements * 4;
        if tail_start < blocksize {
            dest[tail_start..blocksize].copy_from_slice(&src[tail_start..blocksize]);
        }
    }
}

/// SIMD shim used on non-x86 targets: all entry points decline so the scalar
/// implementations run.
#[cfg(not(any(target_arch = "x86", target_arch = "x86_64")))]
mod simd {
    pub fn try_shuffle(_typesize: usize, _src: &[u8], _dest: &mut [u8]) -> bool {
        false
    }

    pub fn try_unshuffle(_typesize: usize, _src: &[u8], _dest: &mut [u8]) -> bool {
        false
    }
}

/// Pure-scalar bitshuffle used as a reference implementation in tests so that
/// SIMD output can be cross-checked against it.
#[cfg(test)]
fn bitshuffle_scalar_with_scratch(
    typesize: usize,
    src: &[u8],
    dest: &mut [u8],
    scratch: Option<&mut [u8]>,
) -> i64 {
    let blocksize = src.len();
    if typesize == 0 || blocksize == 0 || dest.len() < blocksize {
        return 0;
    }

    let size = blocksize / typesize;
    let size8 = size - (size % 8);
    let nbyte8 = size8 * typesize;

    if size8 > 0 {
        let mut owned_tmp;
        let tmp = if let Some(s) = scratch {
            if s.len() < nbyte8 {
                return 0;
            }
            &mut s[..nbyte8]
        } else {
            owned_tmp = vec![0u8; nbyte8];
            &mut owned_tmp[..]
        };

        trans_byte_elem(&src[..nbyte8], dest, size8, typesize);
        trans_bit_byte(&dest[..nbyte8], tmp, size8, typesize);
        trans_bitrow_eight(&tmp[..nbyte8], dest, size8, typesize);
    }

    if nbyte8 < blocksize {
        dest[nbyte8..blocksize].copy_from_slice(&src[nbyte8..blocksize]);
    }

    blocksize as i64
}

/// Pure-scalar bitunshuffle used as a reference implementation in tests.
#[cfg(test)]
fn bitunshuffle_scalar_with_scratch(
    typesize: usize,
    src: &[u8],
    dest: &mut [u8],
    scratch: Option<&mut [u8]>,
) -> i64 {
    let blocksize = src.len();
    if typesize == 0 || blocksize == 0 || dest.len() < blocksize {
        return 0;
    }

    let size = blocksize / typesize;
    let size8 = size - (size % 8);
    let nbyte8 = size8 * typesize;

    if size8 > 0 {
        let mut owned_tmp;
        let tmp = if let Some(s) = scratch {
            if s.len() < nbyte8 {
                return 0;
            }
            &mut s[..nbyte8]
        } else {
            owned_tmp = vec![0u8; nbyte8];
            &mut owned_tmp[..]
        };

        trans_byte_bitrow(&src[..nbyte8], tmp, size8, typesize);
        shuffle_bit_eightelem(&tmp[..nbyte8], dest, size8, typesize);
    }

    if nbyte8 < blocksize {
        dest[nbyte8..blocksize].copy_from_slice(&src[nbyte8..blocksize]);
    }

    blocksize as i64
}

/// Transpose bytes within elements (step 1 of bitshuffle).
fn trans_byte_elem(src: &[u8], dest: &mut [u8], size: usize, elem_size: usize) {
    let mut ii = 0;
    while ii + 7 < size {
        for jj in 0..elem_size {
            let dest_base = jj * size + ii;
            let src_base = ii * elem_size + jj;
            for kk in 0..8 {
                dest[dest_base + kk] = src[src_base + kk * elem_size];
            }
        }
        ii += 8;
    }
    while ii < size {
        for jj in 0..elem_size {
            dest[jj * size + ii] = src[ii * elem_size + jj];
        }
        ii += 1;
    }
}

/// Transpose 8x8 bit matrix packed in a u64 (little-endian).
#[inline]
fn trans_bit_8x8(mut x: u64) -> u64 {
    let mut t: u64;
    t = (x ^ (x >> 7)) & 0x00AA00AA00AA00AA;
    x = x ^ t ^ (t << 7);
    t = (x ^ (x >> 14)) & 0x0000CCCC0000CCCC;
    x = x ^ t ^ (t << 14);
    t = (x ^ (x >> 28)) & 0x00000000F0F0F0F0;
    x ^ t ^ (t << 28)
}

/// Big-endian counterpart of [`trans_bit_8x8`], matching C-Blosc2's scalar path.
#[inline]
#[cfg(target_endian = "big")]
fn trans_bit_8x8_be(mut x: u64) -> u64 {
    let mut t: u64;
    t = (x ^ (x >> 9)) & 0x0055005500550055;
    x = x ^ t ^ (t << 9);
    t = (x ^ (x >> 18)) & 0x0000333300003333;
    x = x ^ t ^ (t << 18);
    t = (x ^ (x >> 36)) & 0x000000000F0F0F0F;
    x ^ t ^ (t << 36)
}

#[inline]
fn trans_bit_8x8_native(x: u64) -> u64 {
    #[cfg(target_endian = "little")]
    {
        trans_bit_8x8(x)
    }
    #[cfg(target_endian = "big")]
    {
        trans_bit_8x8_be(x)
    }
}

/// Transpose bits within bytes (step 2 of bitshuffle).
fn trans_bit_byte(src: &[u8], dest: &mut [u8], size: usize, elem_size: usize) {
    let nbyte = elem_size * size;
    let nbyte_bitrow = nbyte / 8;

    for ii in 0..nbyte_bitrow {
        let x_bytes = &src[ii * 8..(ii + 1) * 8];
        let mut x = trans_bit_8x8_native(u64::from_ne_bytes(x_bytes.try_into().unwrap()));

        for kk in 0..8usize {
            let row = if cfg!(target_endian = "little") {
                kk
            } else {
                7 - kk
            };
            dest[row * nbyte_bitrow + ii] = (x & 0xFF) as u8;
            x >>= 8;
        }
    }
}

/// Transpose rows of shuffled bits within groups of 8 (step 3 of bitshuffle).
fn trans_bitrow_eight(src: &[u8], dest: &mut [u8], size: usize, elem_size: usize) {
    let nbyte_row = size / 8;

    // General transpose: (8, elem_size) blocks of nbyte_row bytes
    for ii in 0..8usize {
        for jj in 0..elem_size {
            let src_off = (ii * elem_size + jj) * nbyte_row;
            let dst_off = (jj * 8 + ii) * nbyte_row;
            dest[dst_off..dst_off + nbyte_row].copy_from_slice(&src[src_off..src_off + nbyte_row]);
        }
    }
}

/// Apply bit-wise shuffle to a block of `typesize`-sized elements.
///
/// Bits of equal positional significance across all elements are grouped
/// together, which typically improves compression for typed binary data.
/// Operates in three steps internally: byte-transpose within elements, then
/// transpose bits within each byte, then transpose bit-rows within groups of
/// eight elements. Trailing elements that do not form a complete group of 8
/// are copied through unchanged.
///
/// Returns the number of bytes processed.
pub fn bitshuffle(typesize: usize, src: &[u8], dest: &mut [u8]) -> i64 {
    bitshuffle_with_scratch(typesize, src, dest, None)
}

/// Like [`bitshuffle`], but accepts a caller-provided scratch buffer to avoid
/// per-call allocation. The scratch slice must be at least as large as the
/// number of bytes that form complete 8-element groups (`(src.len() /
/// typesize / 8) * 8 * typesize`).
pub fn bitshuffle_with_scratch(
    typesize: usize,
    src: &[u8],
    dest: &mut [u8],
    scratch: Option<&mut [u8]>,
) -> i64 {
    let blocksize = src.len();
    if typesize == 0 || blocksize == 0 || dest.len() < blocksize {
        return 0;
    }

    let size = blocksize / typesize;
    let size8 = size - (size % 8);
    let nbyte8 = size8 * typesize;

    if size8 > 0 {
        let mut owned_tmp;
        let tmp = if let Some(s) = scratch {
            if s.len() < nbyte8 {
                return 0;
            }
            &mut s[..nbyte8]
        } else {
            owned_tmp = vec![0u8; nbyte8];
            &mut owned_tmp[..]
        };

        if !bitshuffle_simd::try_bitshuffle(typesize, &src[..nbyte8], dest, tmp, size8) {
            trans_byte_elem(&src[..nbyte8], dest, size8, typesize);
            trans_bit_byte(&dest[..nbyte8], tmp, size8, typesize);
            trans_bitrow_eight(&tmp[..nbyte8], dest, size8, typesize);
        }
    }

    if nbyte8 < blocksize {
        dest[nbyte8..blocksize].copy_from_slice(&src[nbyte8..blocksize]);
    }

    blocksize as i64
}

/// Transpose bytes for each bit row (step 1 of untranspose).
fn trans_byte_bitrow(src: &[u8], dest: &mut [u8], size: usize, elem_size: usize) {
    let nbyte_row = size / 8;

    for jj in 0..elem_size {
        for ii in 0..nbyte_row {
            for kk in 0..8usize {
                dest[ii * 8 * elem_size + jj * 8 + kk] = src[(jj * 8 + kk) * nbyte_row + ii];
            }
        }
    }
}

/// Shuffle bits within eight-element groups (step 2 of untranspose).
fn shuffle_bit_eightelem(src: &[u8], dest: &mut [u8], size: usize, elem_size: usize) {
    let nbyte = elem_size * size;

    for jj in (0..8 * elem_size).step_by(8) {
        let mut ii = 0;
        while ii + 8 * elem_size - 1 < nbyte {
            let x_bytes = &src[ii + jj..ii + jj + 8];
            let mut x = trans_bit_8x8_native(u64::from_ne_bytes(x_bytes.try_into().unwrap()));

            for kk in 0..8usize {
                let elem = if cfg!(target_endian = "little") {
                    kk
                } else {
                    7 - kk
                };
                let out_index = ii + jj / 8 + elem * elem_size;
                dest[out_index] = (x & 0xFF) as u8;
                x >>= 8;
            }
            ii += 8 * elem_size;
        }
    }
}

/// x86/x86_64 SSE2 accelerations for the bit-level transpose steps of
/// bitshuffle. The entry points decline when their preconditions are not met
/// so callers fall back to the scalar path.
#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
mod bitshuffle_simd {
    use super::{trans_bit_8x8, trans_bitrow_eight, trans_byte_bitrow, trans_byte_elem};

    #[cfg(target_arch = "x86")]
    use std::arch::x86 as arch;
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64 as arch;

    /// Try a SIMD-accelerated bitshuffle. Returns `true` if successful.
    pub fn try_bitshuffle(
        typesize: usize,
        src: &[u8],
        dest: &mut [u8],
        scratch: &mut [u8],
        size8: usize,
    ) -> bool {
        let nbyte8 = size8 * typesize;
        if nbyte8 < 128 || dest.len() < nbyte8 || scratch.len() < nbyte8 {
            return false;
        }
        if !std::arch::is_x86_feature_detected!("sse2") {
            return false;
        }

        trans_byte_elem(src, dest, size8, typesize);
        // SAFETY: The wrapper checks that SSE2 is present and that the source
        // and destination ranges cover the exact nbyte8 bytes processed.
        unsafe {
            trans_bit_byte_sse2(&dest[..nbyte8], scratch, nbyte8);
        }
        trans_bitrow_eight(&scratch[..nbyte8], dest, size8, typesize);
        true
    }

    /// Try a SIMD-accelerated bitunshuffle. Returns `true` if successful.
    pub fn try_bitunshuffle(
        typesize: usize,
        src: &[u8],
        dest: &mut [u8],
        scratch: &mut [u8],
        size8: usize,
    ) -> bool {
        let nbyte8 = size8 * typesize;
        if nbyte8 < 128 || dest.len() < nbyte8 || scratch.len() < nbyte8 {
            return false;
        }
        if !std::arch::is_x86_feature_detected!("sse2") {
            return false;
        }

        trans_byte_bitrow(src, scratch, size8, typesize);
        // SAFETY: The wrapper checks that SSE2 is present and that the source
        // and destination ranges cover the exact nbyte8 bytes processed.
        unsafe {
            shuffle_bit_eightelem_sse2(&scratch[..nbyte8], dest, size8, typesize);
        }
        true
    }

    /// SSE2-accelerated equivalent of `trans_bit_byte`: transpose bits within
    /// each byte using 64-bit loads and the `trans_bit_8x8` matrix kernel.
    #[target_feature(enable = "sse2")]
    unsafe fn trans_bit_byte_sse2(src: &[u8], dest: &mut [u8], nbyte: usize) {
        let nbyte_bitrow = nbyte / 8;
        for ii in 0..nbyte_bitrow {
            let x = unsafe { load_u64_sse2(src.as_ptr().add(ii * 8)) };
            let mut transposed = trans_bit_8x8(x);
            for kk in 0..8usize {
                dest[kk * nbyte_bitrow + ii] = (transposed & 0xFF) as u8;
                transposed >>= 8;
            }
        }
    }

    /// SSE2-accelerated equivalent of `shuffle_bit_eightelem`: undo the bit
    /// transpose within each eight-element group using 64-bit loads.
    #[target_feature(enable = "sse2")]
    unsafe fn shuffle_bit_eightelem_sse2(
        src: &[u8],
        dest: &mut [u8],
        size: usize,
        elem_size: usize,
    ) {
        let nbyte = elem_size * size;
        for jj in (0..8 * elem_size).step_by(8) {
            let mut ii = 0;
            while ii + 8 * elem_size - 1 < nbyte {
                let x = unsafe { load_u64_sse2(src.as_ptr().add(ii + jj)) };
                let mut transposed = trans_bit_8x8(x);

                for kk in 0..8usize {
                    let out_index = ii + jj / 8 + kk * elem_size;
                    dest[out_index] = (transposed & 0xFF) as u8;
                    transposed >>= 8;
                }
                ii += 8 * elem_size;
            }
        }
    }

    #[target_feature(enable = "sse2")]
    unsafe fn load_u64_sse2(ptr: *const u8) -> u64 {
        let vec = unsafe { arch::_mm_loadl_epi64(ptr as *const arch::__m128i) };
        arch::_mm_cvtsi128_si64(vec) as u64
    }
}

/// Bitshuffle SIMD shim used on non-x86 targets: every entry point declines so
/// the scalar transpose runs.
#[cfg(not(any(target_arch = "x86", target_arch = "x86_64")))]
mod bitshuffle_simd {
    pub fn try_bitshuffle(
        _typesize: usize,
        _src: &[u8],
        _dest: &mut [u8],
        _scratch: &mut [u8],
        _size8: usize,
    ) -> bool {
        false
    }

    pub fn try_bitunshuffle(
        _typesize: usize,
        _src: &[u8],
        _dest: &mut [u8],
        _scratch: &mut [u8],
        _size8: usize,
    ) -> bool {
        false
    }
}

/// Reverse bit-wise shuffle.
///
/// Inverse of [`bitshuffle`]; `typesize` must match the value used to encode.
/// Returns the number of bytes processed.
pub fn bitunshuffle(typesize: usize, src: &[u8], dest: &mut [u8]) -> i64 {
    bitunshuffle_with_format_version(typesize, src, dest, BLOSC2_VERSION_FORMAT)
}

/// C-style raw bitshuffle wrapper: returns `blocksize` on success or a
/// negative `BLOSC2_ERROR_*` code on invalid parameters.
pub fn blosc2_bitshuffle(typesize: i32, blocksize: i32, src: &[u8], dest: &mut [u8]) -> i32 {
    let blocksize =
        match validate_raw_bitshuffle_filter_args(typesize, blocksize, src.len(), dest.len()) {
            Ok(blocksize) => blocksize,
            Err(code) => return code,
        };
    if bitshuffle(typesize as usize, &src[..blocksize], &mut dest[..blocksize]) == blocksize as i64
    {
        blocksize as i32
    } else {
        BLOSC2_ERROR_INVALID_PARAM
    }
}

/// C-style raw bitunshuffle wrapper: returns `blocksize` on success or a
/// negative `BLOSC2_ERROR_*` code on invalid parameters.
pub fn blosc2_bitunshuffle(typesize: i32, blocksize: i32, src: &[u8], dest: &mut [u8]) -> i32 {
    let blocksize =
        match validate_raw_bitunshuffle_filter_args(typesize, blocksize, src.len(), dest.len()) {
            Ok(blocksize) => blocksize,
            Err(code) => return code,
        };
    if bitunshuffle(typesize as usize, &src[..blocksize], &mut dest[..blocksize])
        == blocksize as i64
    {
        blocksize as i32
    } else {
        BLOSC2_ERROR_INVALID_PARAM
    }
}

/// Like [`bitunshuffle`], but accepts a caller-provided scratch buffer to
/// avoid per-call allocation.
pub fn bitunshuffle_with_scratch(
    typesize: usize,
    src: &[u8],
    dest: &mut [u8],
    scratch: Option<&mut [u8]>,
) -> i64 {
    bitunshuffle_with_scratch_and_format_version(
        typesize,
        src,
        dest,
        scratch,
        BLOSC2_VERSION_FORMAT,
    )
}

/// Bitunshuffle that honors the legacy Blosc 1 format quirk: for
/// `format_version == BLOSC1_VERSION_FORMAT`, blocks whose element count is
/// not a multiple of 8 are passed through unshuffled.
pub fn bitunshuffle_with_format_version(
    typesize: usize,
    src: &[u8],
    dest: &mut [u8],
    format_version: u8,
) -> i64 {
    bitunshuffle_with_scratch_and_format_version(typesize, src, dest, None, format_version)
}

/// Common implementation backing [`bitunshuffle`] and its variants: optional
/// scratch buffer plus format-version-aware handling of partial trailing
/// groups.
fn bitunshuffle_with_scratch_and_format_version(
    typesize: usize,
    src: &[u8],
    dest: &mut [u8],
    scratch: Option<&mut [u8]>,
    format_version: u8,
) -> i64 {
    let blocksize = src.len();
    if typesize == 0 || blocksize == 0 || dest.len() < blocksize {
        return 0;
    }

    let size = blocksize / typesize;
    if format_version == BLOSC1_VERSION_FORMAT && !size.is_multiple_of(8) {
        dest[..blocksize].copy_from_slice(src);
        return blocksize as i64;
    }
    let size8 = size - (size % 8);
    let nbyte8 = size8 * typesize;

    if size8 > 0 {
        let mut owned_tmp;
        let tmp = if let Some(s) = scratch {
            if s.len() < nbyte8 {
                return 0;
            }
            &mut s[..nbyte8]
        } else {
            owned_tmp = vec![0u8; nbyte8];
            &mut owned_tmp[..]
        };

        if !bitshuffle_simd::try_bitunshuffle(typesize, &src[..nbyte8], dest, tmp, size8) {
            trans_byte_bitrow(&src[..nbyte8], tmp, size8, typesize);
            shuffle_bit_eightelem(&tmp[..nbyte8], dest, size8, typesize);
        }
    }

    if nbyte8 < blocksize {
        dest[nbyte8..blocksize].copy_from_slice(&src[nbyte8..blocksize]);
    }

    blocksize as i64
}

/// Apply the delta filter to a block.
///
/// Replaces each element with the XOR of itself and a reference element,
/// exposing redundancy across nearby blocks or elements. When
/// `offset == 0` the block is treated as the reference block and is encoded
/// against its own preceding element; for later blocks the reference is the
/// corresponding element in `dref`. This filter can never fail.
///
/// Only element widths 1, 2, 4, and 8 are encoded element-wise; other widths
/// fall back to 8-byte (if a multiple of 8) or single-byte XORs, mirroring
/// the original C encoder so that output stays interoperable.
pub fn delta_encode(
    dref: &[u8],
    offset: usize,
    nbytes: usize,
    typesize: usize,
    src: &[u8],
    dest: &mut [u8],
) {
    if typesize == 0 || src.len() < nbytes || dest.len() < nbytes {
        return;
    }
    // Match C delta_encoder: 1, 2, 4, 8 use that element width; everything else
    // degrades to 8 (when a multiple of 8) or 1. Using the requested typesize
    // directly would produce output incompatible with the C library.
    let effective_typesize = match typesize {
        1 | 2 | 4 | 8 => typesize,
        n if n % 8 == 0 => 8,
        _ => 1,
    };
    let main_len = if effective_typesize == 1 {
        nbytes
    } else {
        nbytes - (nbytes % effective_typesize)
    };

    if offset == 0 {
        // Reference block: delta against previous elements in dref.
        if main_len == 0 {
            return;
        }
        let head = effective_typesize;
        if dref.len() < main_len.max(head) {
            return;
        }
        dest[..head].copy_from_slice(&dref[..head]);
        for i in effective_typesize..main_len {
            dest[i] = src[i] ^ dref[i - effective_typesize];
        }
    } else {
        // Non-reference block: delta against dref.
        if dref.len() < main_len {
            return;
        }
        for i in 0..main_len {
            dest[i] = src[i] ^ dref[i];
        }
    }
}

/// Reverse the delta filter in place over `dest`.
///
/// For `offset == 0` (the reference block), each element XORs against the
/// previous element in `dref`, matching C's `delta_decoder`. Passing `None`
/// uses `dest` as that reference, which is the in-place reference-block path.
/// For later blocks, each element XORs against the corresponding entry in
/// `dref`.
pub fn delta_decode(
    dref: Option<&[u8]>,
    offset: usize,
    nbytes: usize,
    typesize: usize,
    dest: &mut [u8],
) {
    if typesize == 0 || dest.len() < nbytes {
        return;
    }
    let effective_typesize = match typesize {
        1 | 2 | 4 | 8 => typesize,
        n if n % 8 == 0 => 8,
        _ => 1,
    };
    let main_len = if effective_typesize == 1 {
        nbytes
    } else {
        nbytes - (nbytes % effective_typesize)
    };

    if offset == 0 {
        // Reference block: C uses the dref pointer. In the normal in-place
        // path dref aliases dest, so keep that behavior when no dref is passed.
        if let Some(dref) = dref {
            if dref.len() < main_len {
                return;
            }
            for i in effective_typesize..main_len {
                dest[i] ^= dref[i - effective_typesize];
            }
        } else {
            for i in effective_typesize..main_len {
                dest[i] ^= dest[i - effective_typesize];
            }
        }
    } else if let Some(dref) = dref {
        // Non-reference block: undo delta against dref
        if dref.len() < main_len {
            return;
        }
        for i in 0..main_len {
            dest[i] ^= dref[i];
        }
    }
}

/// Apply the configured filter pipeline to one block in encode order.
///
/// The caller provides two working buffers `buf1` and `buf2`; filters are
/// applied left-to-right and ping-pong between them so no extra allocation is
/// needed. `src` is read directly by the first active filter to avoid a copy.
///
/// `filters` is the array of filter IDs (e.g. `BLOSC_SHUFFLE`,
/// `BLOSC_BITSHUFFLE`, `BLOSC_DELTA`, `BLOSC_TRUNC_PREC`, or a user-defined
/// ID) and `filters_meta` carries the per-filter metadata byte. `dref` is the
/// reference block used by the delta filter (defaulting to `src`).
///
/// Returns the index (1 or 2) of the buffer holding the final output, or 1
/// after a no-op copy when no filters are active.
#[derive(Debug, Clone, Copy)]
pub struct FilterPipelineContext<'a> {
    pub cparams: Option<&'a FilterCParamsContext>,
    pub dparams: Option<&'a FilterDParamsContext>,
    pub chunk: FilterChunkContext,
    pub b2nd_metalayer: Option<&'a [u8]>,
    pub user_data: usize,
}

#[allow(clippy::too_many_arguments)]
pub fn apply_filter_pipeline_for_compression(
    src: &[u8],
    buf1: &mut [u8],
    buf2: &mut [u8],
    filters: &[u8; BLOSC2_MAX_FILTERS],
    filters_meta: &[u8; BLOSC2_MAX_FILTERS],
    typesize: usize,
    block_offset: usize,
    dref: Option<&[u8]>,
) -> usize {
    apply_filter_pipeline_for_compression_with_context(
        src,
        buf1,
        buf2,
        filters,
        filters_meta,
        typesize,
        block_offset,
        dref,
        None,
    )
}

/// Apply the configured filter pipeline in encode order with plugin context.
#[allow(clippy::too_many_arguments)]
pub fn apply_filter_pipeline_for_compression_with_context(
    src: &[u8],
    buf1: &mut [u8],
    buf2: &mut [u8],
    filters: &[u8; BLOSC2_MAX_FILTERS],
    filters_meta: &[u8; BLOSC2_MAX_FILTERS],
    typesize: usize,
    block_offset: usize,
    dref: Option<&[u8]>,
    context: Option<FilterPipelineContext<'_>>,
) -> usize {
    let bsize = src.len();
    if buf1.len() < bsize || buf2.len() < bsize {
        return 0;
    }
    let base_context = context.unwrap_or(FilterPipelineContext {
        cparams: None,
        dparams: None,
        chunk: FilterChunkContext {
            schunk: 0,
            nchunk: -1,
            nblock: -1,
            block_offset,
            blocksize: bsize,
            bsize,
        },
        b2nd_metalayer: None,
        user_data: 0,
    });

    // Track current data location: 0 = src (read-only), 1 = buf1, 2 = buf2.
    // Match C's apply_filter_pipeline_for_compression: first active filter writes to dest (buf1),
    // then the destination cycles through the temporary buffer.
    let mut current = 0u8;

    for i in 0..BLOSC2_MAX_FILTERS {
        let filter = filters[i];
        if filter == BLOSC_NOFILTER {
            continue;
        }

        // Determine input and output buffers.
        // Input: src (0), buf1 (1), or buf2 (2)
        // Output: first active filter writes buf1, then alternates.
        let out_buf = if current == 1 { 2u8 } else { 1u8 };

        let (inp, out) = match (current, out_buf) {
            (0, 1) => (&src[..bsize], &mut buf1[..bsize]),
            (0, 2) => (&src[..bsize], &mut buf2[..bsize]),
            (1, 2) => (&buf1[..bsize], &mut buf2[..bsize]),
            (2, 1) => (&buf2[..bsize], &mut buf1[..bsize]),
            _ => unreachable!("filter pipeline cannot read and write the same buffer"),
        };

        match filter {
            BLOSC_SHUFFLE => {
                let ts = if filters_meta[i] == 0 {
                    typesize
                } else {
                    filters_meta[i] as usize
                };
                let Ok(ts_i32) = i32::try_from(ts) else {
                    return 0;
                };
                let Ok(bsize_i32) = i32::try_from(bsize) else {
                    return 0;
                };
                let _ = blosc2_shuffle(ts_i32, bsize_i32, inp, out);
            }
            BLOSC_BITSHUFFLE => {
                let Ok(typesize_i32) = i32::try_from(typesize) else {
                    return 0;
                };
                let Ok(bsize_i32) = i32::try_from(bsize) else {
                    return 0;
                };
                if blosc2_bitshuffle(typesize_i32, bsize_i32, inp, out) < 0 {
                    return 0;
                }
            }
            BLOSC_DELTA => {
                let actual_dref = if block_offset == 0 {
                    src
                } else {
                    dref.unwrap_or(src)
                };
                delta_encode(actual_dref, block_offset, bsize, typesize, inp, out);
                let effective_typesize = match typesize {
                    1 | 2 | 4 | 8 => typesize,
                    n if n != 0 && n % 8 == 0 => 8,
                    _ => 1,
                };
                if effective_typesize > 1 {
                    let main_len = bsize - (bsize % effective_typesize);
                    out[main_len..bsize].copy_from_slice(&inp[main_len..bsize]);
                }
            }
            BLOSC_TRUNC_PREC => {
                // C treats filters_meta as int8_t — negative values have Python-style
                // "drop this many mantissa bits" semantics.
                let prec = filters_meta[i] as i8;
                if !trunc_prec_forward(inp, out, typesize, prec) {
                    return 0;
                }
            }
            _ if is_blosc_defined_filter(filter) => {
                return 0;
            }
            _ => {
                if let Some(user_filter) = registered_filter(filter) {
                    let mut callback_context = FilterCallbackContext {
                        filter_id: filter,
                        filter_slot: i,
                        meta: filters_meta[i],
                        typesize,
                        cparams: base_context.cparams,
                        dparams: base_context.dparams,
                        chunk: FilterChunkContext {
                            block_offset,
                            bsize,
                            ..base_context.chunk
                        },
                        b2nd_metalayer: base_context.b2nd_metalayer,
                        user_data: base_context.user_data,
                    };
                    if user_filter.forward.run(
                        &mut callback_context,
                        filters_meta[i],
                        typesize,
                        block_offset,
                        filters,
                        filters_meta,
                        inp,
                        out,
                    ) != 0
                    {
                        return 0;
                    }
                } else {
                    return 0;
                }
            }
        }

        current = out_buf;
    }

    // If no filters were active, copy src to buf1
    if current == 0 {
        buf1[..bsize].copy_from_slice(src);
        return 1;
    }

    current as usize
}

/// Apply the configured filter pipeline in decode order.
///
/// Filters are applied in reverse, ping-ponging between `buf1` and `buf2`.
/// `current_buf` (1 or 2) specifies which buffer initially holds the filtered
/// data. `format_version` is forwarded to the bitunshuffle to preserve
/// Blosc 1 compatibility, and `dref` provides the reference block for the
/// delta filter.
///
/// Returns the index (1 or 2) of the buffer holding the decoded output.
#[allow(clippy::too_many_arguments)]
pub fn apply_filter_pipeline_for_decompression(
    buf1: &mut [u8],
    buf2: &mut [u8],
    bsize: usize,
    filters: &[u8; BLOSC2_MAX_FILTERS],
    filters_meta: &[u8; BLOSC2_MAX_FILTERS],
    format_version: u8,
    typesize: usize,
    block_offset: usize,
    dref: Option<&[u8]>,
    current_buf: usize,
) -> usize {
    apply_filter_pipeline_for_decompression_with_context(
        buf1,
        buf2,
        bsize,
        filters,
        filters_meta,
        format_version,
        typesize,
        block_offset,
        dref,
        current_buf,
        None,
    )
}

/// Apply the configured filter pipeline in decode order with plugin context.
#[allow(clippy::too_many_arguments)]
pub fn apply_filter_pipeline_for_decompression_with_context(
    buf1: &mut [u8],
    buf2: &mut [u8],
    bsize: usize,
    filters: &[u8; BLOSC2_MAX_FILTERS],
    filters_meta: &[u8; BLOSC2_MAX_FILTERS],
    format_version: u8,
    typesize: usize,
    block_offset: usize,
    dref: Option<&[u8]>,
    current_buf: usize,
    context: Option<FilterPipelineContext<'_>>,
) -> usize {
    if current_buf != 1 && current_buf != 2 {
        return 0;
    }
    if buf1.len() < bsize || buf2.len() < bsize {
        return 0;
    }
    let base_context = context.unwrap_or(FilterPipelineContext {
        cparams: None,
        dparams: None,
        chunk: FilterChunkContext {
            schunk: 0,
            nchunk: -1,
            nblock: -1,
            block_offset,
            blocksize: bsize,
            bsize,
        },
        b2nd_metalayer: None,
        user_data: 0,
    });
    let mut current = current_buf as u8;
    let mut pipeline_failed = false;

    // Filters applied in reverse order
    for i in (0..BLOSC2_MAX_FILTERS).rev() {
        let filter = filters[i];
        if filter == BLOSC_NOFILTER {
            continue;
        }

        let (inp, out) = if current == 1 {
            (&buf1[..bsize], &mut buf2[..bsize])
        } else {
            (&buf2[..bsize], &mut buf1[..bsize])
        };

        match filter {
            BLOSC_SHUFFLE => {
                let ts = if filters_meta[i] == 0 {
                    typesize
                } else {
                    filters_meta[i] as usize
                };
                let Ok(ts_i32) = i32::try_from(ts) else {
                    return 0;
                };
                let Ok(bsize_i32) = i32::try_from(bsize) else {
                    return 0;
                };
                let _ = blosc2_unshuffle(ts_i32, bsize_i32, inp, out);
            }
            BLOSC_BITSHUFFLE => {
                let ok = if format_version == BLOSC2_VERSION_FORMAT {
                    let Ok(typesize_i32) = i32::try_from(typesize) else {
                        return 0;
                    };
                    let Ok(bsize_i32) = i32::try_from(bsize) else {
                        return 0;
                    };
                    blosc2_bitunshuffle(typesize_i32, bsize_i32, inp, out) >= 0
                } else {
                    bitunshuffle_with_format_version(typesize, inp, out, format_version)
                        == bsize as i64
                };
                if !ok {
                    return 0;
                }
            }
            BLOSC_DELTA => {
                // Delta decode: copy data to output, then decode in-place
                out.copy_from_slice(inp);
                let actual_dref = if block_offset == 0 { None } else { dref };
                delta_decode(actual_dref, block_offset, bsize, typesize, out);
            }
            BLOSC_TRUNC_PREC => {
                // Truncation is lossy. C leaves the current buffer untouched
                // and does not cycle buffers on the backward path.
                continue;
            }
            _ if is_blosc_defined_filter(filter) => {
                pipeline_failed = true;
            }
            _ => {
                if let Some(user_filter) = registered_filter(filter) {
                    let mut callback_context = FilterCallbackContext {
                        filter_id: filter,
                        filter_slot: i,
                        meta: filters_meta[i],
                        typesize,
                        cparams: base_context.cparams,
                        dparams: base_context.dparams,
                        chunk: FilterChunkContext {
                            block_offset,
                            bsize,
                            ..base_context.chunk
                        },
                        b2nd_metalayer: base_context.b2nd_metalayer,
                        user_data: base_context.user_data,
                    };
                    if user_filter.backward.run(
                        &mut callback_context,
                        filters_meta[i],
                        typesize,
                        block_offset,
                        inp,
                        out,
                    ) != 0
                    {
                        return 0;
                    }
                } else {
                    return 0;
                }
            }
        }

        current = if current == 1 { 2 } else { 1 };
    }

    if pipeline_failed {
        0
    } else {
        current as usize
    }
}

/// Truncate precision: zero out least-significant bits of IEEE-754 floats.
///
/// Sign and exponent are preserved; only mantissa bits are cleared, so NaN
/// and infinity stay representable. Only typesizes 4 (f32) and 8 (f64) are
/// processed; other widths pass through unchanged so output stays
/// interoperable with the C library.
///
/// `prec_bits` is signed and follows Python-slice semantics:
/// - `> 0`: absolute number of mantissa bits to keep
/// - `< 0`: number of mantissa bits to drop
/// - `= 0`: keep zero bits (drop the entire mantissa)
///
/// Returns `false` when C would reject the filter (unsupported typesize,
/// invalid precision, or clearing the whole mantissa).
const BITS_MANTISSA_F32: i32 = 23;
const BITS_MANTISSA_F64: i32 = 52;

fn trunc_prec_forward(src: &[u8], dest: &mut [u8], typesize: usize, prec_bits: i8) -> bool {
    let len = src.len();
    if dest.len() < len {
        return false;
    }
    if !matches!(typesize, 4 | 8) {
        return false;
    }

    let (mantissa_bits, n_elements) = match typesize {
        4 => (BITS_MANTISSA_F32, len / 4),
        8 => (BITS_MANTISSA_F64, len / 8),
        _ => unreachable!(),
    };

    let p = prec_bits as i32;
    if p.abs() > mantissa_bits {
        return false;
    }
    let zeroed_bits = if p >= 0 { mantissa_bits - p } else { -p };
    if zeroed_bits >= mantissa_bits {
        return false;
    }

    match typesize {
        4 => {
            let mask = !((1u32 << zeroed_bits) - 1);
            for i in 0..n_elements {
                let off = i * 4;
                let v = u32::from_ne_bytes(src[off..off + 4].try_into().unwrap());
                dest[off..off + 4].copy_from_slice(&(v & mask).to_ne_bytes());
            }
        }
        8 => {
            let mask = !((1u64 << zeroed_bits) - 1);
            for i in 0..n_elements {
                let off = i * 8;
                let v = u64::from_ne_bytes(src[off..off + 8].try_into().unwrap());
                dest[off..off + 8].copy_from_slice(&(v & mask).to_ne_bytes());
            }
        }
        _ => unreachable!(),
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CString;
    use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU8, AtomicUsize, Ordering};

    static C_ABI_FORWARD_ID: AtomicU8 = AtomicU8::new(0);
    static C_ABI_BACKWARD_ID: AtomicU8 = AtomicU8::new(0);
    static C_ABI_FORWARD_META: AtomicU8 = AtomicU8::new(0);
    static C_ABI_BACKWARD_META: AtomicU8 = AtomicU8::new(0);
    static C_ABI_FORWARD_TYPESIZE: AtomicI32 = AtomicI32::new(0);
    static C_ABI_BACKWARD_TYPESIZE: AtomicI32 = AtomicI32::new(0);
    static C_ABI_FORWARD_SCHUNK: AtomicUsize = AtomicUsize::new(0);
    static C_ABI_BACKWARD_SCHUNK: AtomicUsize = AtomicUsize::new(0);
    static C_ABI_FORWARD_USE_DICT: AtomicI32 = AtomicI32::new(0);
    static C_ABI_FORWARD_PREFILTER: AtomicUsize = AtomicUsize::new(usize::MAX);
    static C_ABI_FORWARD_PREPARAMS: AtomicUsize = AtomicUsize::new(0);
    static C_ABI_FORWARD_TUNER_ID: AtomicI32 = AtomicI32::new(0);
    static C_ABI_FORWARD_INSTR_CODEC: AtomicBool = AtomicBool::new(false);
    static C_ABI_FORWARD_CODEC_PARAMS: AtomicUsize = AtomicUsize::new(0);
    static C_ABI_BACKWARD_POSTFILTER: AtomicUsize = AtomicUsize::new(usize::MAX);
    static C_ABI_BACKWARD_POSTPARAMS: AtomicUsize = AtomicUsize::new(0);
    static PLAIN_C_ABI_FORWARD_ID: AtomicU8 = AtomicU8::new(0);
    static PLAIN_C_ABI_BACKWARD_ID: AtomicU8 = AtomicU8::new(0);
    static PLAIN_C_ABI_FORWARD_META: AtomicU8 = AtomicU8::new(0);
    static PLAIN_C_ABI_BACKWARD_META: AtomicU8 = AtomicU8::new(0);
    static PLAIN_C_ABI_FORWARD_TYPESIZE: AtomicI32 = AtomicI32::new(0);
    static PLAIN_C_ABI_BACKWARD_TYPESIZE: AtomicI32 = AtomicI32::new(0);
    static PLAIN_C_ABI_FORWARD_BLOCKSIZE: AtomicI32 = AtomicI32::new(0);
    static PLAIN_C_ABI_FORWARD_FILTER: AtomicU8 = AtomicU8::new(0);
    static PLAIN_C_ABI_FORWARD_FILTER_META: AtomicU8 = AtomicU8::new(0);
    static PLAIN_C_ABI_FORWARD_SCHUNK: AtomicUsize = AtomicUsize::new(usize::MAX);
    static PLAIN_C_ABI_BACKWARD_SCHUNK: AtomicUsize = AtomicUsize::new(usize::MAX);

    fn scalar_shuffle_for_test(typesize: usize, src: &[u8], dest: &mut [u8]) {
        let blocksize = src.len();
        if typesize <= 1 || blocksize == 0 {
            dest[..blocksize].copy_from_slice(src);
            return;
        }
        let nelements = blocksize / typesize;
        let tail_start = nelements * typesize;
        for byte_idx in 0..typesize {
            for element in 0..nelements {
                dest[byte_idx * nelements + element] = src[element * typesize + byte_idx];
            }
        }
        dest[tail_start..blocksize].copy_from_slice(&src[tail_start..blocksize]);
    }

    fn scalar_unshuffle_for_test(typesize: usize, src: &[u8], dest: &mut [u8]) {
        let blocksize = src.len();
        if typesize <= 1 || blocksize == 0 {
            dest[..blocksize].copy_from_slice(src);
            return;
        }
        let nelements = blocksize / typesize;
        let tail_start = nelements * typesize;
        for element in 0..nelements {
            for byte_idx in 0..typesize {
                dest[element * typesize + byte_idx] = src[byte_idx * nelements + element];
            }
        }
        dest[tail_start..blocksize].copy_from_slice(&src[tail_start..blocksize]);
    }

    fn b2nd_meta_1d_for_test(len: i32) -> Vec<u8> {
        let mut meta = vec![0x95, 0x00, 0x01, 0x91, 0xd3];
        meta.extend_from_slice(&(len as i64).to_be_bytes());
        for _ in 0..2 {
            meta.extend_from_slice(&[0x91, 0xd2]);
            meta.extend_from_slice(&len.to_be_bytes());
        }
        meta
    }

    fn copy_filter(_meta: u8, _typesize: usize, _block_offset: usize, src: &[u8], dest: &mut [u8]) {
        dest.copy_from_slice(src);
    }

    fn reverse_filter(
        _meta: u8,
        _typesize: usize,
        _block_offset: usize,
        src: &[u8],
        dest: &mut [u8],
    ) {
        for (dst, src) in dest.iter_mut().zip(src.iter().rev()) {
            *dst = *src;
        }
    }

    fn copy_fallible_filter(
        _meta: u8,
        _typesize: usize,
        _block_offset: usize,
        src: &[u8],
        dest: &mut [u8],
    ) -> i32 {
        dest.copy_from_slice(src);
        0
    }

    fn failing_fallible_filter(
        _meta: u8,
        _typesize: usize,
        _block_offset: usize,
        src: &[u8],
        dest: &mut [u8],
    ) -> i32 {
        dest.copy_from_slice(src);
        -1
    }

    fn zero_fallible_filter(
        _meta: u8,
        _typesize: usize,
        _block_offset: usize,
        _src: &[u8],
        dest: &mut [u8],
    ) -> i32 {
        dest.fill(0);
        0
    }

    unsafe extern "C" fn c_abi_forward_filter(
        input: *const u8,
        output: *mut u8,
        length: i32,
        meta: u8,
        cparams: *mut Blosc2CParams,
        id: u8,
    ) -> i32 {
        if input.is_null() || output.is_null() || cparams.is_null() || length < 0 {
            return 1;
        }
        C_ABI_FORWARD_ID.store(id, Ordering::SeqCst);
        C_ABI_FORWARD_META.store(meta, Ordering::SeqCst);
        C_ABI_FORWARD_TYPESIZE.store((*cparams).typesize, Ordering::SeqCst);
        C_ABI_FORWARD_SCHUNK.store((*cparams).schunk as usize, Ordering::SeqCst);
        C_ABI_FORWARD_USE_DICT.store((*cparams).use_dict, Ordering::SeqCst);
        C_ABI_FORWARD_PREFILTER.store((*cparams).prefilter as usize, Ordering::SeqCst);
        C_ABI_FORWARD_PREPARAMS.store((*cparams).preparams as usize, Ordering::SeqCst);
        C_ABI_FORWARD_TUNER_ID.store((*cparams).tuner_id, Ordering::SeqCst);
        C_ABI_FORWARD_INSTR_CODEC.store((*cparams).instr_codec, Ordering::SeqCst);
        C_ABI_FORWARD_CODEC_PARAMS.store((*cparams).codec_params as usize, Ordering::SeqCst);
        std::ptr::copy_nonoverlapping(input, output, length as usize);
        for idx in 0..length as usize {
            *output.add(idx) = (*output.add(idx)).wrapping_add(3);
        }
        0
    }

    unsafe extern "C" fn c_abi_backward_filter(
        input: *const u8,
        output: *mut u8,
        length: i32,
        meta: u8,
        dparams: *mut Blosc2DParams,
        id: u8,
    ) -> i32 {
        if input.is_null() || output.is_null() || dparams.is_null() || length < 0 {
            return 1;
        }
        C_ABI_BACKWARD_ID.store(id, Ordering::SeqCst);
        C_ABI_BACKWARD_META.store(meta, Ordering::SeqCst);
        C_ABI_BACKWARD_TYPESIZE.store((*dparams).typesize, Ordering::SeqCst);
        C_ABI_BACKWARD_SCHUNK.store((*dparams).schunk as usize, Ordering::SeqCst);
        C_ABI_BACKWARD_POSTFILTER.store((*dparams).postfilter as usize, Ordering::SeqCst);
        C_ABI_BACKWARD_POSTPARAMS.store((*dparams).postparams as usize, Ordering::SeqCst);
        std::ptr::copy_nonoverlapping(input, output, length as usize);
        for idx in 0..length as usize {
            *output.add(idx) = (*output.add(idx)).wrapping_sub(3);
        }
        0
    }

    unsafe extern "C" fn plain_c_abi_forward_filter(
        input: *const u8,
        output: *mut u8,
        length: i32,
        meta: u8,
        cparams: *mut Blosc2CParams,
        id: u8,
    ) -> i32 {
        if input.is_null() || output.is_null() || cparams.is_null() || length < 0 {
            return 1;
        }
        PLAIN_C_ABI_FORWARD_ID.store(id, Ordering::SeqCst);
        PLAIN_C_ABI_FORWARD_META.store(meta, Ordering::SeqCst);
        PLAIN_C_ABI_FORWARD_TYPESIZE.store((*cparams).typesize, Ordering::SeqCst);
        PLAIN_C_ABI_FORWARD_BLOCKSIZE.store((*cparams).blocksize, Ordering::SeqCst);
        PLAIN_C_ABI_FORWARD_FILTER
            .store((*cparams).filters[BLOSC2_MAX_FILTERS - 1], Ordering::SeqCst);
        PLAIN_C_ABI_FORWARD_FILTER_META.store(
            (*cparams).filters_meta[BLOSC2_MAX_FILTERS - 1],
            Ordering::SeqCst,
        );
        PLAIN_C_ABI_FORWARD_SCHUNK.store((*cparams).schunk as usize, Ordering::SeqCst);
        std::ptr::copy_nonoverlapping(input, output, length as usize);
        for idx in 0..length as usize {
            *output.add(idx) = (*output.add(idx)).wrapping_add(5);
        }
        0
    }

    unsafe extern "C" fn plain_c_abi_backward_filter(
        input: *const u8,
        output: *mut u8,
        length: i32,
        meta: u8,
        dparams: *mut Blosc2DParams,
        id: u8,
    ) -> i32 {
        if input.is_null() || output.is_null() || dparams.is_null() || length < 0 {
            return 1;
        }
        PLAIN_C_ABI_BACKWARD_ID.store(id, Ordering::SeqCst);
        PLAIN_C_ABI_BACKWARD_META.store(meta, Ordering::SeqCst);
        PLAIN_C_ABI_BACKWARD_TYPESIZE.store((*dparams).typesize, Ordering::SeqCst);
        PLAIN_C_ABI_BACKWARD_SCHUNK.store((*dparams).schunk as usize, Ordering::SeqCst);
        std::ptr::copy_nonoverlapping(input, output, length as usize);
        for idx in 0..length as usize {
            *output.add(idx) = (*output.add(idx)).wrapping_sub(5);
        }
        0
    }

    fn unwritten_fallible_filter(
        _meta: u8,
        _typesize: usize,
        _block_offset: usize,
        _src: &[u8],
        _dest: &mut [u8],
    ) -> i32 {
        0
    }

    #[test]
    fn test_c_style_raw_filter_wrappers() {
        let data: Vec<u8> = (0..64).collect();
        let mut tmp = vec![0u8; data.len()];
        let mut restored = vec![0u8; data.len()];

        assert_eq!(blosc2_shuffle(4, data.len() as i32, &data, &mut tmp), 64);
        assert_eq!(
            blosc2_unshuffle(4, data.len() as i32, &tmp, &mut restored),
            64
        );
        assert_eq!(restored, data);

        assert_eq!(blosc2_bitshuffle(4, data.len() as i32, &data, &mut tmp), 64);
        assert_eq!(
            blosc2_bitunshuffle(4, data.len() as i32, &tmp, &mut restored),
            64
        );
        assert_eq!(restored, data);

        assert_eq!(blosc2_shuffle(1, data.len() as i32, &data, &mut tmp), 64);
        assert_eq!(tmp, data);
        tmp.fill(0);
        assert_eq!(blosc2_unshuffle(1, data.len() as i32, &data, &mut tmp), 64);
        assert_eq!(tmp, data);
        tmp.fill(0);
        restored.fill(0);
        assert_eq!(blosc2_bitshuffle(1, data.len() as i32, &data, &mut tmp), 64);
        assert_eq!(
            blosc2_bitunshuffle(1, data.len() as i32, &tmp, &mut restored),
            64
        );
        assert_eq!(restored, data);

        let wide_data: Vec<u8> = (0..512).map(|idx| (idx % 251) as u8).collect();
        let mut wide_tmp = vec![0u8; wide_data.len()];
        let mut wide_restored = vec![0u8; wide_data.len()];
        assert_eq!(
            blosc2_shuffle(256, wide_data.len() as i32, &wide_data, &mut wide_tmp),
            512
        );
        assert_eq!(
            blosc2_unshuffle(256, wide_data.len() as i32, &wide_tmp, &mut wide_restored),
            512
        );
        assert_eq!(wide_restored, wide_data);

        let data257: Vec<u8> = (0..64).map(|idx| (idx * 3 + 1) as u8).collect();
        let mut tmp257 = vec![0u8; data257.len()];
        let mut restored257 = vec![0u8; data257.len()];
        assert_eq!(
            blosc2_shuffle(257, data257.len() as i32, &data257, &mut tmp257),
            BLOSC2_ERROR_INVALID_PARAM
        );
        assert_eq!(
            blosc2_unshuffle(257, data257.len() as i32, &data257, &mut tmp257),
            BLOSC2_ERROR_INVALID_PARAM
        );
        assert_eq!(
            blosc2_bitshuffle(257, data257.len() as i32, &data257, &mut tmp257),
            BLOSC2_ERROR_INVALID_PARAM
        );
        assert_eq!(
            blosc2_bitunshuffle(257, data257.len() as i32, &tmp257, &mut restored257),
            data257.len() as i32
        );
        assert_eq!(restored257, tmp257);

        assert_eq!(
            blosc2_bitshuffle(4, -1, &data, &mut tmp),
            BLOSC2_ERROR_INVALID_PARAM
        );
        assert_eq!(
            blosc2_bitunshuffle(4, data.len() as i32, &data, &mut restored[..4]),
            BLOSC2_ERROR_INVALID_PARAM
        );
    }

    #[test]
    fn test_register_filter_rejects_c_global_registered_ids() {
        assert_eq!(BLOSC_FILTER_NDCELL, BLOSC2_GLOBAL_REGISTERED_FILTERS_START);
        assert_eq!(BLOSC_FILTER_NDMEAN, BLOSC_FILTER_NDCELL + 1);
        assert_eq!(BLOSC_FILTER_BYTEDELTA_BUGGY, BLOSC_FILTER_NDCELL + 2);
        assert_eq!(BLOSC_FILTER_BYTEDELTA, BLOSC_FILTER_NDCELL + 3);
        assert_eq!(BLOSC_FILTER_INT_TRUNC, BLOSC_FILTER_NDCELL + 4);
        assert_eq!(
            BLOSC2_USER_REGISTERED_FILTERS_START,
            BLOSC2_USER_DEFINED_FILTERS_START
        );
        assert_eq!(BLOSC2_USER_REGISTERED_FILTERS_STOP, u8::MAX);
        let c_filter = Blosc2Filter {
            id: BLOSC2_USER_DEFINED_FILTERS_START + 70,
            name: "copy",
            version: 1,
            forward: copy_fallible_filter,
            backward: copy_fallible_filter,
        };
        assert_eq!(register_blosc2_filter(&c_filter), BLOSC2_ERROR_SUCCESS);
        assert_eq!(register_blosc2_filter_c(None), BLOSC2_ERROR_INVALID_PARAM);
        assert_eq!(
            register_blosc2_filter_c(Some(&c_filter)),
            BLOSC2_ERROR_SUCCESS
        );
        assert!(is_registered_filter(c_filter.id));
        assert_eq!(registered_filter_info(c_filter.id), Some(("copy", 1)));
        let same_name_different_callbacks = Blosc2Filter {
            forward: zero_fallible_filter,
            backward: zero_fallible_filter,
            ..c_filter
        };
        assert_eq!(
            register_blosc2_filter(&same_name_different_callbacks),
            BLOSC2_ERROR_SUCCESS
        );
        let same_id_different_name = Blosc2Filter {
            name: "other-copy",
            ..same_name_different_callbacks
        };
        assert_eq!(
            register_blosc2_filter(&same_id_different_name),
            BLOSC2_ERROR_FAILURE
        );
        let global_c_filter = Blosc2Filter {
            id: BLOSC2_GLOBAL_REGISTERED_FILTERS_START + 90,
            name: "global-c-copy",
            ..c_filter
        };
        assert_eq!(
            register_blosc2_filter(&global_c_filter),
            BLOSC2_ERROR_FAILURE
        );
        let invalid_c_filter = Blosc2Filter {
            id: BLOSC2_GLOBAL_REGISTERED_FILTERS_START,
            name: "other-ndcell",
            ..c_filter
        };
        assert_eq!(
            register_blosc2_filter(&invalid_c_filter),
            BLOSC2_ERROR_FAILURE
        );
        assert_eq!(
            register_filter(
                BLOSC2_GLOBAL_REGISTERED_FILTERS_START,
                copy_filter,
                copy_filter
            ),
            Err("User-defined filter IDs must be >= 160")
        );
        assert_eq!(
            register_filter(159, copy_filter, copy_filter),
            Err("User-defined filter IDs must be >= 160")
        );
        assert!(
            register_filter(BLOSC2_USER_DEFINED_FILTERS_START, copy_filter, copy_filter).is_ok()
        );
        assert!(is_registered_filter(BLOSC2_USER_DEFINED_FILTERS_START));
        assert!(
            register_filter(BLOSC2_USER_DEFINED_FILTERS_START, copy_filter, copy_filter).is_ok()
        );
        assert_eq!(
            register_filter(
                BLOSC2_USER_DEFINED_FILTERS_START,
                reverse_filter,
                copy_filter
            ),
            Err("User-defined filter ID already registered")
        );
        assert_eq!(
            register_filter(BLOSC_TRUNC_PREC, copy_filter, copy_filter),
            Err("User-defined filter IDs must be >= 160")
        );
    }

    #[test]
    fn test_blosc2_register_filter_abi_uses_raw_c_callback_shape() {
        const FILTER_ID: u8 = BLOSC2_USER_DEFINED_FILTERS_START + 71;
        let name = CString::new("c-abi-copy").unwrap();
        let c_filter = Blosc2FilterAbi {
            id: FILTER_ID,
            name: name.as_ptr(),
            version: 1,
            forward: Some(c_abi_forward_filter),
            backward: Some(c_abi_backward_filter),
        };
        assert_eq!(
            blosc2_register_filter(&c_filter as *const Blosc2FilterAbi),
            BLOSC2_ERROR_SUCCESS
        );
        assert_eq!(registered_filter_info(FILTER_ID), Some(("c-abi-copy", 1)));
        assert_eq!(
            blosc2_register_filter(std::ptr::null()),
            BLOSC2_ERROR_INVALID_PARAM
        );
        let global_name = CString::new("global-c-abi-copy").unwrap();
        let global = Blosc2FilterAbi {
            id: BLOSC2_GLOBAL_REGISTERED_FILTERS_START + 91,
            name: global_name.as_ptr(),
            version: 1,
            forward: Some(c_abi_forward_filter),
            backward: Some(c_abi_backward_filter),
        };
        assert_eq!(
            blosc2_register_filter(&global as *const Blosc2FilterAbi),
            BLOSC2_ERROR_FAILURE
        );
        let known_name = CString::new("ndcell").unwrap();
        let known = Blosc2FilterAbi {
            id: BLOSC_FILTER_NDCELL,
            name: known_name.as_ptr(),
            version: 9,
            forward: Some(c_abi_forward_filter),
            backward: Some(c_abi_backward_filter),
        };
        assert_eq!(
            blosc2_register_filter(&known as *const Blosc2FilterAbi),
            BLOSC2_ERROR_FAILURE
        );
        let invalid_name = CString::new("other-ndcell").unwrap();
        let invalid = Blosc2FilterAbi {
            id: BLOSC_FILTER_NDCELL,
            name: invalid_name.as_ptr(),
            version: 1,
            forward: Some(c_abi_forward_filter),
            backward: Some(c_abi_backward_filter),
        };
        assert_eq!(
            blosc2_register_filter(&invalid as *const Blosc2FilterAbi),
            BLOSC2_ERROR_FAILURE
        );

        C_ABI_FORWARD_ID.store(0, Ordering::SeqCst);
        C_ABI_BACKWARD_ID.store(0, Ordering::SeqCst);
        C_ABI_FORWARD_SCHUNK.store(0, Ordering::SeqCst);
        C_ABI_BACKWARD_SCHUNK.store(0, Ordering::SeqCst);
        C_ABI_FORWARD_USE_DICT.store(0, Ordering::SeqCst);
        C_ABI_FORWARD_PREFILTER.store(usize::MAX, Ordering::SeqCst);
        C_ABI_FORWARD_PREPARAMS.store(0, Ordering::SeqCst);
        C_ABI_FORWARD_TUNER_ID.store(0, Ordering::SeqCst);
        C_ABI_FORWARD_INSTR_CODEC.store(false, Ordering::SeqCst);
        C_ABI_FORWARD_CODEC_PARAMS.store(0, Ordering::SeqCst);
        C_ABI_BACKWARD_POSTFILTER.store(usize::MAX, Ordering::SeqCst);
        C_ABI_BACKWARD_POSTPARAMS.store(0, Ordering::SeqCst);
        let src: Vec<u8> = (0..32u8).collect();
        let mut filters = [BLOSC_NOFILTER; BLOSC2_MAX_FILTERS];
        let mut filters_meta = [0; BLOSC2_MAX_FILTERS];
        filters[BLOSC2_MAX_FILTERS - 1] = FILTER_ID;
        filters_meta[BLOSC2_MAX_FILTERS - 1] = 0x6d;
        let cparams = FilterCParamsContext {
            compcode: BLOSC_BLOSCLZ,
            compcode_meta: 0,
            clevel: 5,
            use_dict: true,
            typesize: 4,
            blocksize: src.len() as i32,
            splitmode: 0,
            filters,
            filters_meta,
            nthreads: 1,
            nchunk: 7,
            user_data: 0xaaaa,
            preparams: 0xfeed,
            tuner_id: 0x123,
            instr_codec: true,
            codec_params: 0xc0decafe,
        };
        let dparams = FilterDParamsContext {
            nthreads: 1,
            typesize: 4,
            nchunk: 7,
            user_data: 0xbbbb,
            postparams: 0xbeef,
        };
        let chunk = FilterChunkContext {
            schunk: 0x1234,
            nchunk: 7,
            nblock: 2,
            block_offset: 64,
            blocksize: src.len(),
            bsize: src.len(),
        };
        assert_eq!(cparams.schunk(chunk), 0x1234);
        assert_eq!(dparams.schunk(chunk), 0x1234);

        let mut encoded = vec![0u8; src.len()];
        let mut scratch = vec![0u8; src.len()];
        let current = apply_filter_pipeline_for_compression_with_context(
            &src,
            &mut encoded,
            &mut scratch,
            &filters,
            &filters_meta,
            1,
            chunk.block_offset,
            None,
            Some(FilterPipelineContext {
                cparams: Some(&cparams),
                dparams: None,
                chunk,
                b2nd_metalayer: None,
                user_data: 0,
            }),
        );
        assert_ne!(current, 0);
        let mut encoded = if current == 1 { encoded } else { scratch };
        let expected_encoded: Vec<u8> = src.iter().map(|byte| byte.wrapping_add(3)).collect();
        assert_eq!(encoded, expected_encoded);

        let mut decoded_scratch = vec![0u8; src.len()];
        let current = apply_filter_pipeline_for_decompression_with_context(
            &mut encoded,
            &mut decoded_scratch,
            src.len(),
            &filters,
            &filters_meta,
            BLOSC2_VERSION_FORMAT,
            1,
            chunk.block_offset,
            None,
            1,
            Some(FilterPipelineContext {
                cparams: None,
                dparams: Some(&dparams),
                chunk,
                b2nd_metalayer: None,
                user_data: 0,
            }),
        );
        assert_ne!(current, 0);
        let decoded = if current == 1 {
            encoded
        } else {
            decoded_scratch
        };
        assert_eq!(decoded, src);
        assert_eq!(C_ABI_FORWARD_ID.load(Ordering::SeqCst), FILTER_ID);
        assert_eq!(C_ABI_BACKWARD_ID.load(Ordering::SeqCst), FILTER_ID);
        assert_eq!(C_ABI_FORWARD_META.load(Ordering::SeqCst), 0x6d);
        assert_eq!(C_ABI_BACKWARD_META.load(Ordering::SeqCst), 0x6d);
        assert_eq!(C_ABI_FORWARD_TYPESIZE.load(Ordering::SeqCst), 4);
        assert_eq!(C_ABI_BACKWARD_TYPESIZE.load(Ordering::SeqCst), 4);
        assert_eq!(C_ABI_FORWARD_SCHUNK.load(Ordering::SeqCst), 0x1234);
        assert_eq!(C_ABI_BACKWARD_SCHUNK.load(Ordering::SeqCst), 0x1234);
        assert_eq!(C_ABI_FORWARD_USE_DICT.load(Ordering::SeqCst), 1);
        assert_eq!(C_ABI_FORWARD_PREFILTER.load(Ordering::SeqCst), 0);
        assert_eq!(C_ABI_FORWARD_PREPARAMS.load(Ordering::SeqCst), 0);
        assert_eq!(C_ABI_FORWARD_TUNER_ID.load(Ordering::SeqCst), 0x123);
        assert!(C_ABI_FORWARD_INSTR_CODEC.load(Ordering::SeqCst));
        assert_eq!(
            C_ABI_FORWARD_CODEC_PARAMS.load(Ordering::SeqCst),
            0xc0decafe
        );
        assert_eq!(C_ABI_BACKWARD_POSTFILTER.load(Ordering::SeqCst), 0);
        assert_eq!(C_ABI_BACKWARD_POSTPARAMS.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn test_plain_pipeline_c_abi_callbacks_receive_non_null_synthesized_params() {
        const FILTER_ID: u8 = BLOSC2_USER_DEFINED_FILTERS_START + 76;
        let c_filter = Blosc2FilterAbi {
            id: FILTER_ID,
            name: c"plain-c-abi-copy".as_ptr(),
            version: 1,
            forward: Some(plain_c_abi_forward_filter),
            backward: Some(plain_c_abi_backward_filter),
        };
        assert_eq!(
            blosc2_register_filter(&c_filter as *const Blosc2FilterAbi),
            BLOSC2_ERROR_SUCCESS
        );

        PLAIN_C_ABI_FORWARD_ID.store(0, Ordering::SeqCst);
        PLAIN_C_ABI_BACKWARD_ID.store(0, Ordering::SeqCst);
        PLAIN_C_ABI_FORWARD_META.store(0, Ordering::SeqCst);
        PLAIN_C_ABI_BACKWARD_META.store(0, Ordering::SeqCst);
        PLAIN_C_ABI_FORWARD_TYPESIZE.store(0, Ordering::SeqCst);
        PLAIN_C_ABI_BACKWARD_TYPESIZE.store(0, Ordering::SeqCst);
        PLAIN_C_ABI_FORWARD_BLOCKSIZE.store(0, Ordering::SeqCst);
        PLAIN_C_ABI_FORWARD_FILTER.store(0, Ordering::SeqCst);
        PLAIN_C_ABI_FORWARD_FILTER_META.store(0, Ordering::SeqCst);
        PLAIN_C_ABI_FORWARD_SCHUNK.store(usize::MAX, Ordering::SeqCst);
        PLAIN_C_ABI_BACKWARD_SCHUNK.store(usize::MAX, Ordering::SeqCst);

        let src: Vec<u8> = (0..32u8).collect();
        let mut filters = [BLOSC_NOFILTER; BLOSC2_MAX_FILTERS];
        let mut filters_meta = [0; BLOSC2_MAX_FILTERS];
        filters[BLOSC2_MAX_FILTERS - 1] = FILTER_ID;
        filters_meta[BLOSC2_MAX_FILTERS - 1] = 0x4a;

        let mut encoded = vec![0u8; src.len()];
        let mut scratch = vec![0u8; src.len()];
        let current = apply_filter_pipeline_for_compression(
            &src,
            &mut encoded,
            &mut scratch,
            &filters,
            &filters_meta,
            4,
            0,
            None,
        );
        assert_ne!(current, 0);
        let mut encoded = if current == 1 { encoded } else { scratch };
        let expected_encoded: Vec<u8> = src.iter().map(|byte| byte.wrapping_add(5)).collect();
        assert_eq!(encoded, expected_encoded);

        let mut decoded_scratch = vec![0u8; src.len()];
        let current = apply_filter_pipeline_for_decompression(
            &mut encoded,
            &mut decoded_scratch,
            src.len(),
            &filters,
            &filters_meta,
            BLOSC2_VERSION_FORMAT,
            4,
            0,
            None,
            1,
        );
        assert_ne!(current, 0);
        let decoded = if current == 1 {
            encoded
        } else {
            decoded_scratch
        };
        assert_eq!(decoded, src);

        assert_eq!(PLAIN_C_ABI_FORWARD_ID.load(Ordering::SeqCst), FILTER_ID);
        assert_eq!(PLAIN_C_ABI_BACKWARD_ID.load(Ordering::SeqCst), FILTER_ID);
        assert_eq!(PLAIN_C_ABI_FORWARD_META.load(Ordering::SeqCst), 0x4a);
        assert_eq!(PLAIN_C_ABI_BACKWARD_META.load(Ordering::SeqCst), 0x4a);
        assert_eq!(PLAIN_C_ABI_FORWARD_TYPESIZE.load(Ordering::SeqCst), 4);
        assert_eq!(PLAIN_C_ABI_BACKWARD_TYPESIZE.load(Ordering::SeqCst), 4);
        assert_eq!(
            PLAIN_C_ABI_FORWARD_BLOCKSIZE.load(Ordering::SeqCst),
            src.len() as i32
        );
        assert_eq!(PLAIN_C_ABI_FORWARD_FILTER.load(Ordering::SeqCst), FILTER_ID);
        assert_eq!(PLAIN_C_ABI_FORWARD_FILTER_META.load(Ordering::SeqCst), 0x4a);
        assert_eq!(PLAIN_C_ABI_FORWARD_SCHUNK.load(Ordering::SeqCst), 0);
        assert_eq!(PLAIN_C_ABI_BACKWARD_SCHUNK.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn test_blosc2_register_filter_allows_null_callbacks_and_raw_names() {
        const NULL_CALLBACK_ID: u8 = BLOSC2_USER_DEFINED_FILTERS_START + 72;
        const NULL_NAME_ID: u8 = BLOSC2_USER_DEFINED_FILTERS_START + 73;
        const EMPTY_NAME_ID: u8 = BLOSC2_USER_DEFINED_FILTERS_START + 74;
        const NON_UTF8_NAME_ID: u8 = BLOSC2_USER_DEFINED_FILTERS_START + 75;

        let null_callbacks = Blosc2FilterAbi {
            id: NULL_CALLBACK_ID,
            name: c"null-callbacks".as_ptr(),
            version: 1,
            forward: None,
            backward: None,
        };
        assert_eq!(
            blosc2_register_filter(&null_callbacks as *const Blosc2FilterAbi),
            BLOSC2_ERROR_SUCCESS
        );
        assert_eq!(
            registered_filter_info(NULL_CALLBACK_ID),
            Some(("null-callbacks", 1))
        );
        let src: Vec<u8> = (0..16u8).collect();
        let mut encoded = vec![0u8; src.len()];
        let mut scratch = vec![0u8; src.len()];
        let mut filters = [BLOSC_NOFILTER; BLOSC2_MAX_FILTERS];
        let filters_meta = [0; BLOSC2_MAX_FILTERS];
        filters[BLOSC2_MAX_FILTERS - 1] = NULL_CALLBACK_ID;
        assert_eq!(
            apply_filter_pipeline_for_compression(
                &src,
                &mut encoded,
                &mut scratch,
                &filters,
                &filters_meta,
                1,
                0,
                None
            ),
            0
        );
        encoded.copy_from_slice(&src);
        assert_eq!(
            apply_filter_pipeline_for_decompression(
                &mut encoded,
                &mut scratch,
                src.len(),
                &filters,
                &filters_meta,
                BLOSC2_VERSION_FORMAT,
                1,
                0,
                None,
                1
            ),
            0
        );

        let null_name = Blosc2FilterAbi {
            id: NULL_NAME_ID,
            name: std::ptr::null(),
            version: 2,
            forward: Some(c_abi_forward_filter),
            backward: Some(c_abi_backward_filter),
        };
        assert_eq!(
            blosc2_register_filter(&null_name as *const Blosc2FilterAbi),
            BLOSC2_ERROR_SUCCESS
        );
        assert_eq!(registered_filter_info(NULL_NAME_ID), None);

        let empty_name = Blosc2FilterAbi {
            id: EMPTY_NAME_ID,
            name: c"".as_ptr(),
            version: 3,
            forward: Some(c_abi_forward_filter),
            backward: Some(c_abi_backward_filter),
        };
        assert_eq!(
            blosc2_register_filter(&empty_name as *const Blosc2FilterAbi),
            BLOSC2_ERROR_SUCCESS
        );
        assert_eq!(registered_filter_info(EMPTY_NAME_ID), Some(("", 3)));

        let non_utf8_name = [0xffu8, 0x00];
        let non_utf8_name = Blosc2FilterAbi {
            id: NON_UTF8_NAME_ID,
            name: non_utf8_name.as_ptr().cast(),
            version: 4,
            forward: Some(c_abi_forward_filter),
            backward: Some(c_abi_backward_filter),
        };
        assert_eq!(
            blosc2_register_filter(&non_utf8_name as *const Blosc2FilterAbi),
            BLOSC2_ERROR_SUCCESS
        );
        assert_eq!(
            registered_filter_info(NON_UTF8_NAME_ID),
            Some(("\u{ff}", 4))
        );
    }

    #[test]
    fn test_known_global_plugin_filters_have_c_metadata() {
        let expected = [
            (BLOSC_FILTER_NDCELL, "ndcell"),
            (BLOSC_FILTER_NDMEAN, "ndmean"),
            (BLOSC_FILTER_BYTEDELTA_BUGGY, "bytedelta_buggy"),
            (BLOSC_FILTER_BYTEDELTA, "bytedelta"),
            (BLOSC_FILTER_INT_TRUNC, "int_trunc"),
        ];

        for (filter_id, name) in expected {
            assert!(is_known_global_filter(filter_id));
            assert_eq!(known_global_filter_info(filter_id), Some((name, 1)));
            assert!(is_registered_filter(filter_id));
            assert_eq!(registered_filter_info(filter_id), Some((name, 1)));
        }
        assert!(!is_known_global_filter(BLOSC_FILTER_INT_TRUNC + 1));
        assert_eq!(known_global_filter_info(BLOSC_FILTER_INT_TRUNC + 1), None);
    }

    #[test]
    fn test_known_global_filter_ids_cannot_be_preempted_before_lazy_registration() {
        assert_eq!(
            register_filter(BLOSC_FILTER_NDCELL, copy_filter, copy_filter),
            Err("User-defined filter IDs must be >= 160")
        );
        assert_eq!(
            register_fallible_filter(
                BLOSC_FILTER_NDCELL,
                copy_fallible_filter,
                copy_fallible_filter
            ),
            Err("User-defined filter IDs must be >= 160")
        );
        assert_eq!(
            register_context_filter(BLOSC_FILTER_NDCELL, apply_ndmean_filter, undo_ndmean_filter),
            Err("User-defined filter IDs must be >= 160")
        );

        assert_eq!(
            register_global_filter(BLOSC_FILTER_NDCELL, copy_filter, copy_filter),
            Err("Global plugin filter ID already registered")
        );
        assert_eq!(
            register_named_global_filter(
                BLOSC_FILTER_NDCELL,
                "ndcell",
                reverse_filter,
                reverse_filter
            ),
            Ok(())
        );
        assert_eq!(
            register_named_global_context_filter(
                BLOSC_FILTER_NDCELL,
                "other-ndcell",
                apply_ndcell_filter,
                undo_ndcell_filter
            ),
            Err("Global plugin filter ID already registered")
        );
        assert_eq!(
            register_global_filter_with_metadata(
                BLOSC_FILTER_NDCELL,
                "ndcell",
                9,
                copy_filter,
                copy_filter
            ),
            Ok(())
        );
        assert_eq!(
            registered_filter_info(BLOSC_FILTER_NDCELL),
            Some(("ndcell", 1))
        );
        assert_eq!(
            register_global_fallible_filter(
                BLOSC_FILTER_NDCELL,
                copy_fallible_filter,
                copy_fallible_filter
            ),
            Err("Global plugin filter ID already registered")
        );
        assert_eq!(
            register_named_global_fallible_filter(
                BLOSC_FILTER_NDCELL,
                "ndcell",
                copy_fallible_filter,
                copy_fallible_filter
            ),
            Ok(())
        );
        assert_eq!(
            register_global_fallible_filter_with_metadata(
                BLOSC_FILTER_NDCELL,
                "ndcell",
                9,
                copy_fallible_filter,
                copy_fallible_filter
            ),
            Ok(())
        );
        assert_eq!(
            registered_filter_info(BLOSC_FILTER_NDCELL),
            Some(("ndcell", 1))
        );
        assert_eq!(
            register_global_context_filter(
                BLOSC_FILTER_NDCELL,
                apply_ndmean_filter,
                undo_ndmean_filter
            ),
            Err("Global plugin filter ID already registered")
        );
        assert_eq!(
            register_named_global_context_filter(
                BLOSC_FILTER_NDCELL,
                "ndcell",
                apply_ndmean_filter,
                undo_ndmean_filter
            ),
            Ok(())
        );

        assert_eq!(
            register_global_context_filter_with_metadata(
                BLOSC_FILTER_NDCELL,
                "ndcell",
                1,
                apply_ndcell_filter,
                undo_ndcell_filter
            ),
            Ok(())
        );
        assert_eq!(
            registered_filter_info(BLOSC_FILTER_NDCELL),
            Some(("ndcell", 1))
        );
    }

    #[test]
    fn test_ndcell_ndmean_are_marked_b2nd_metadata_bound() {
        for filter in [BLOSC_FILTER_NDCELL, BLOSC_FILTER_NDMEAN] {
            assert!(is_known_global_filter(filter));
            assert!(is_registered_filter(filter));
            assert!(global_filter_requires_b2nd_metadata(filter));

            let mut filters = [0; BLOSC2_MAX_FILTERS];
            let mut filters_meta = [0; BLOSC2_MAX_FILTERS];
            filters[BLOSC2_MAX_FILTERS - 2] = filter;
            filters_meta[BLOSC2_MAX_FILTERS - 2] = 4;

            let src = vec![0u8; 64];
            let mut buf1 = vec![0u8; src.len()];
            let mut buf2 = vec![0u8; src.len()];
            assert_eq!(
                apply_filter_pipeline_for_compression(
                    &src,
                    &mut buf1,
                    &mut buf2,
                    &filters,
                    &filters_meta,
                    4,
                    0,
                    None
                ),
                0
            );
        }

        assert!(!global_filter_requires_b2nd_metadata(
            BLOSC_FILTER_BYTEDELTA
        ));
        assert!(!global_filter_requires_b2nd_metadata(
            BLOSC_FILTER_INT_TRUNC
        ));
    }

    #[test]
    fn test_ndcell_ndmean_meta_is_signed_int8_and_callbacks_fail_with_c_code() {
        let b2nd_meta = b2nd_meta_1d_for_test(4);
        let src = [1u8, 2, 3, 4];
        let mut dest = [0u8; 4];
        let mut ctx = FilterCallbackContext {
            filter_id: BLOSC_FILTER_NDCELL,
            filter_slot: BLOSC2_MAX_FILTERS - 1,
            meta: 2,
            typesize: 1,
            cparams: None,
            dparams: None,
            chunk: FilterChunkContext {
                schunk: 0x1000,
                nchunk: -1,
                nblock: 0,
                block_offset: 0,
                blocksize: src.len(),
                bsize: src.len(),
            },
            b2nd_metalayer: Some(&b2nd_meta),
            user_data: 0,
        };

        assert!(ndcell_layout(2, 1, src.len(), Some(&b2nd_meta), false).is_some());
        for meta in [0, 0x80, 0xff] {
            assert!(ndcell_layout(meta, 1, src.len(), Some(&b2nd_meta), false).is_none());
            ctx.meta = meta;
            assert_eq!(
                apply_ndcell_filter(&mut ctx, &src, &mut dest),
                BLOSC2_ERROR_FAILURE
            );
            assert_eq!(
                undo_ndcell_filter(&mut ctx, &src, &mut dest),
                BLOSC2_ERROR_FAILURE
            );
        }

        let src_f32: Vec<u8> = [1.0f32, 2.0, 3.0, 4.0]
            .into_iter()
            .flat_map(f32::to_ne_bytes)
            .collect();
        let mut dest_f32 = vec![0u8; src_f32.len()];
        ctx.filter_id = BLOSC_FILTER_NDMEAN;
        ctx.typesize = 4;
        ctx.chunk.blocksize = src_f32.len();
        ctx.chunk.bsize = src_f32.len();
        for meta in [0, 0x80, 0xff] {
            ctx.meta = meta;
            assert_eq!(
                apply_ndmean_filter(&mut ctx, &src_f32, &mut dest_f32),
                BLOSC2_ERROR_FAILURE
            );
            assert_eq!(
                undo_ndmean_filter(&mut ctx, &src_f32, &mut dest_f32),
                BLOSC2_ERROR_FAILURE
            );
        }
    }

    #[test]
    fn test_bytedelta_global_filter_roundtrips_byte_streams() {
        let src: Vec<u8> = (0..130)
            .flat_map(|idx| (idx as u32).wrapping_mul(17).to_ne_bytes())
            .collect();
        let filters = [0, 0, 0, 0, 0, BLOSC_FILTER_BYTEDELTA];
        let mut filters_meta = [0; BLOSC2_MAX_FILTERS];
        filters_meta[BLOSC2_MAX_FILTERS - 1] = 4;
        let mut encoded = vec![0u8; src.len()];
        let mut scratch = vec![0u8; src.len()];
        let current = apply_filter_pipeline_for_compression(
            &src,
            &mut encoded,
            &mut scratch,
            &filters,
            &filters_meta,
            4,
            0,
            None,
        );
        assert_ne!(current, 0);
        let encoded = if current == 1 { encoded } else { scratch };
        assert_ne!(encoded, src);

        let mut decoded = encoded.clone();
        let mut scratch = vec![0u8; src.len()];
        let current = apply_filter_pipeline_for_decompression(
            &mut decoded,
            &mut scratch,
            src.len(),
            &filters,
            &filters_meta,
            BLOSC2_VERSION_FORMAT,
            4,
            0,
            None,
            1,
        );
        assert_ne!(current, 0);
        let decoded = if current == 1 { decoded } else { scratch };
        assert_eq!(decoded, src);
    }

    #[test]
    fn test_bytedelta_meta_zero_uses_schunk_context_typesize() {
        let src: Vec<u8> = (0..33)
            .flat_map(|idx| (idx as u32).wrapping_mul(19).to_ne_bytes())
            .collect();
        let filters = [0, 0, 0, 0, 0, BLOSC_FILTER_BYTEDELTA];
        let filters_meta = [0; BLOSC2_MAX_FILTERS];
        let cparams = FilterCParamsContext {
            compcode: 0,
            compcode_meta: 0,
            clevel: 0,
            use_dict: false,
            typesize: 2,
            blocksize: src.len() as i32,
            splitmode: 0,
            filters,
            filters_meta,
            nthreads: 1,
            nchunk: -1,
            user_data: 0,
            preparams: 0,
            tuner_id: 0,
            instr_codec: false,
            codec_params: 0,
        };
        let dparams = FilterDParamsContext {
            nthreads: 1,
            typesize: 2,
            nchunk: -1,
            user_data: 0,
            postparams: 0,
        };
        let chunk = FilterChunkContext {
            schunk: 0x2000,
            nchunk: -1,
            nblock: 0,
            block_offset: 0,
            blocksize: src.len(),
            bsize: src.len(),
        };
        let context = FilterPipelineContext {
            cparams: Some(&cparams),
            dparams: None,
            chunk,
            b2nd_metalayer: None,
            user_data: 0,
        };
        let mut encoded = vec![0u8; src.len()];
        let mut scratch = vec![0u8; src.len()];
        let current = apply_filter_pipeline_for_compression_with_context(
            &src,
            &mut encoded,
            &mut scratch,
            &filters,
            &filters_meta,
            4,
            0,
            None,
            Some(context),
        );
        assert_ne!(current, 0);
        let mut encoded = if current == 1 { encoded } else { scratch };

        let mut decoded_scratch = vec![0u8; src.len()];
        let current = apply_filter_pipeline_for_decompression_with_context(
            &mut encoded,
            &mut decoded_scratch,
            src.len(),
            &filters,
            &filters_meta,
            BLOSC2_VERSION_FORMAT,
            4,
            0,
            None,
            1,
            Some(FilterPipelineContext {
                cparams: None,
                dparams: Some(&dparams),
                chunk,
                b2nd_metalayer: None,
                user_data: 0,
            }),
        );
        assert_ne!(current, 0);
        let decoded = if current == 1 {
            encoded
        } else {
            decoded_scratch
        };
        assert_eq!(decoded, src);

        let missing_context = apply_filter_pipeline_for_compression(
            &src,
            &mut vec![0u8; src.len()],
            &mut vec![0u8; src.len()],
            &filters,
            &filters_meta,
            4,
            0,
            None,
        );
        assert_eq!(missing_context, 0);

        let mut ctx = FilterCallbackContext {
            filter_id: BLOSC_FILTER_BYTEDELTA,
            filter_slot: BLOSC2_MAX_FILTERS - 1,
            meta: 0,
            typesize: 4,
            cparams: Some(&cparams),
            dparams: None,
            chunk: FilterChunkContext { schunk: 0, ..chunk },
            b2nd_metalayer: None,
            user_data: 0,
        };
        let mut dest = vec![0u8; src.len()];
        for forward in [
            apply_bytedelta_filter as ContextFilterForwardFn,
            bytedelta_buggy_forward_impl,
        ] {
            assert_eq!(forward(&mut ctx, &src, &mut dest), BLOSC2_ERROR_FAILURE);
        }

        ctx.cparams = None;
        ctx.dparams = Some(&dparams);
        for backward in [
            undo_bytedelta_filter as ContextFilterBackwardFn,
            bytedelta_buggy_backward_impl,
        ] {
            assert_eq!(backward(&mut ctx, &src, &mut dest), BLOSC2_ERROR_FAILURE);
        }

        ctx.chunk.schunk = 0x2000;
        ctx.cparams = None;
        ctx.dparams = None;
        assert_eq!(apply_bytedelta_filter(&mut ctx, &src, &mut dest), 0);
        assert_ne!(dest, src);
        let mut decoded = vec![0u8; src.len()];
        assert_eq!(undo_bytedelta_filter(&mut ctx, &dest, &mut decoded), 0);
        assert_eq!(decoded, src);
        assert_eq!(bytedelta_buggy_forward_impl(&mut ctx, &src, &mut dest), 0);
    }

    #[test]
    fn test_bytedelta_rejects_c_incompatible_metadata_and_leaves_partial_tails_unwritten() {
        let src: Vec<u8> = (0..130)
            .flat_map(|idx| (idx as u32).wrapping_mul(17).to_ne_bytes())
            .collect();
        let filters = [0, 0, 0, 0, 0, BLOSC_FILTER_BYTEDELTA];
        let mut filters_meta = [0; BLOSC2_MAX_FILTERS];
        let mut encoded = vec![0u8; src.len()];
        let mut scratch = vec![0u8; src.len()];

        let current = apply_filter_pipeline_for_compression(
            &src,
            &mut encoded,
            &mut scratch,
            &filters,
            &filters_meta,
            4,
            0,
            None,
        );
        assert_eq!(current, 0);

        let mut decoded = src.clone();
        let mut decode_scratch = vec![0u8; src.len()];
        let current = apply_filter_pipeline_for_decompression(
            &mut decoded,
            &mut decode_scratch,
            src.len(),
            &filters,
            &filters_meta,
            BLOSC2_VERSION_FORMAT,
            4,
            0,
            None,
            1,
        );
        assert_eq!(current, 0);

        assert_eq!(
            apply_filter_pipeline_for_compression(
                &src,
                &mut encoded,
                &mut scratch,
                &filters,
                &filters_meta,
                BLOSC2_MAXTYPESIZE + 1,
                0,
                None,
            ),
            0
        );

        filters_meta[BLOSC2_MAX_FILTERS - 1] = 4;
        let src_with_tail = &src[..src.len() - 1];
        let current = apply_filter_pipeline_for_compression(
            src_with_tail,
            &mut encoded[..src_with_tail.len()],
            &mut scratch[..src_with_tail.len()],
            &filters,
            &filters_meta,
            4,
            0,
            None,
        );
        assert_ne!(current, 0);
        let encoded = if current == 1 {
            &encoded[..src_with_tail.len()]
        } else {
            &scratch[..src_with_tail.len()]
        };
        assert_eq!(encoded[src_with_tail.len() - 1], 0);

        let mut decoded = encoded.to_vec();
        let mut scratch = vec![0u8; src_with_tail.len()];
        let current = apply_filter_pipeline_for_decompression(
            &mut decoded,
            &mut scratch,
            src_with_tail.len(),
            &filters,
            &filters_meta,
            BLOSC2_VERSION_FORMAT,
            4,
            0,
            None,
            1,
        );
        assert_ne!(current, 0);
        let decoded = if current == 1 { decoded } else { scratch };
        assert_eq!(decoded[src_with_tail.len() - 1], 0);
    }

    #[test]
    fn test_bytedelta_buggy_matches_legacy_simd_tail_split() {
        let src: Vec<u8> = (0..70).map(|idx| (idx * 7 + 3) as u8).collect();
        let filters = [0, 0, 0, 0, 0, BLOSC_FILTER_BYTEDELTA_BUGGY];
        let mut filters_meta = [0; BLOSC2_MAX_FILTERS];
        filters_meta[BLOSC2_MAX_FILTERS - 1] = 2;
        let mut encoded = vec![0u8; src.len()];
        let mut scratch = vec![0u8; src.len()];
        let current = apply_filter_pipeline_for_compression(
            &src,
            &mut encoded,
            &mut scratch,
            &filters,
            &filters_meta,
            2,
            0,
            None,
        );
        assert_ne!(current, 0);
        let encoded = if current == 1 { encoded } else { scratch };

        let stream_len = src.len() / 2;
        let tail_start = stream_len - (stream_len % 16);
        if bytedelta_buggy_simd_path_available() {
            assert_eq!(encoded[tail_start], src[tail_start]);
            assert_eq!(
                encoded[stream_len + tail_start],
                src[stream_len + tail_start]
            );
        } else {
            assert_eq!(
                encoded[tail_start],
                src[tail_start].wrapping_sub(src[tail_start - 1])
            );
            assert_eq!(
                encoded[stream_len + tail_start],
                src[stream_len + tail_start].wrapping_sub(src[stream_len + tail_start - 1])
            );
        }

        let mut decoded = encoded.clone();
        let mut scratch = vec![0u8; src.len()];
        let current = apply_filter_pipeline_for_decompression(
            &mut decoded,
            &mut scratch,
            src.len(),
            &filters,
            &filters_meta,
            BLOSC2_VERSION_FORMAT,
            2,
            0,
            None,
            1,
        );
        assert_ne!(current, 0);
        let decoded = if current == 1 { decoded } else { scratch };
        assert_eq!(decoded, src);
    }

    #[test]
    fn test_bytedelta_buggy_has_explicit_simd_and_non_simd_paths() {
        let src: Vec<u8> = (0..70).map(|idx| (idx * 7 + 3) as u8).collect();
        let mut fixed = vec![0u8; src.len()];
        let mut simd_buggy = vec![0u8; src.len()];
        let mut non_simd_buggy = vec![0u8; src.len()];

        assert_eq!(bytedelta_forward_core(2, &src, &mut fixed), 0);
        assert_eq!(
            bytedelta_buggy_forward_core_with_simd(2, &src, &mut simd_buggy, true),
            0
        );
        assert_eq!(
            bytedelta_buggy_forward_core_with_simd(2, &src, &mut non_simd_buggy, false),
            0
        );

        assert_eq!(non_simd_buggy, fixed);
        assert_ne!(simd_buggy, fixed);

        let mut simd_decoded = vec![0u8; src.len()];
        let mut non_simd_decoded = vec![0u8; src.len()];
        assert_eq!(
            bytedelta_buggy_backward_core_with_simd(2, &simd_buggy, &mut simd_decoded, true),
            0
        );
        assert_eq!(
            bytedelta_buggy_backward_core_with_simd(
                2,
                &non_simd_buggy,
                &mut non_simd_decoded,
                false
            ),
            0
        );
        assert_eq!(simd_decoded, src);
        assert_eq!(non_simd_decoded, src);
    }

    #[test]
    fn test_bytedelta_cores_reject_zero_typesize_and_leave_partial_tails_unwritten() {
        let src: Vec<u8> = (0..23).map(|idx| (idx * 11 + 5) as u8).collect();

        for forward in [
            bytedelta_forward_core as fn(usize, &[u8], &mut [u8]) -> i32,
            bytedelta_buggy_forward_core,
        ] {
            let mut dest = vec![0xA5; src.len()];
            assert_eq!(forward(0, &src, &mut dest), 1);
            assert_eq!(dest, vec![0xA5; src.len()]);

            assert_eq!(forward(5, &src, &mut dest), 0);
            assert_eq!(&dest[20..], &[0xA5, 0xA5, 0xA5]);
        }

        for backward in [
            bytedelta_backward_core as fn(usize, &[u8], &mut [u8]) -> i32,
            bytedelta_buggy_backward_core,
        ] {
            let mut dest = vec![0x5A; src.len()];
            assert_eq!(backward(0, &src, &mut dest), 1);
            assert_eq!(dest, vec![0x5A; src.len()]);

            assert_eq!(backward(5, &src, &mut dest), 0);
            assert_eq!(&dest[20..], &[0x5A, 0x5A, 0x5A]);
        }
    }

    #[test]
    fn test_int_trunc_global_filter_truncates_integer_precision() {
        let values = [0x1234_5678u32, 0xFFFF_FFFF, 0x0000_0001, 0x8765_4321];
        let src: Vec<u8> = values.into_iter().flat_map(u32::to_ne_bytes).collect();
        let filters = [0, 0, 0, 0, 0, BLOSC_FILTER_INT_TRUNC];
        let mut filters_meta = [0; BLOSC2_MAX_FILTERS];
        filters_meta[BLOSC2_MAX_FILTERS - 1] = 20;
        let mut encoded = vec![0u8; src.len()];
        let mut scratch = vec![0u8; src.len()];
        let current = apply_filter_pipeline_for_compression(
            &src,
            &mut encoded,
            &mut scratch,
            &filters,
            &filters_meta,
            4,
            0,
            None,
        );
        assert_ne!(current, 0);
        let encoded = if current == 1 { encoded } else { scratch };

        let expected: Vec<u8> = [0x1234_5000u32, 0xFFFF_F000, 0, 0x8765_4000]
            .into_iter()
            .flat_map(u32::to_ne_bytes)
            .collect();
        assert_eq!(encoded, expected);

        let mut decoded = encoded.clone();
        let mut scratch = vec![0u8; src.len()];
        let current = apply_filter_pipeline_for_decompression(
            &mut decoded,
            &mut scratch,
            src.len(),
            &filters,
            &filters_meta,
            BLOSC2_VERSION_FORMAT,
            4,
            0,
            None,
            1,
        );
        assert_ne!(current, 0);
        let decoded = if current == 1 { decoded } else { scratch };
        assert_eq!(decoded, expected);

        let src_with_tail = [&src[..], &[0xAA, 0xBB][..]].concat();
        let mut encoded = vec![0xA5u8; src_with_tail.len()];
        let mut scratch = vec![0x5Au8; src_with_tail.len()];
        let current = apply_filter_pipeline_for_compression(
            &src_with_tail,
            &mut encoded,
            &mut scratch,
            &filters,
            &filters_meta,
            4,
            0,
            None,
        );
        assert_ne!(current, 0);
        let encoded = if current == 1 { encoded } else { scratch };
        assert_eq!(&encoded[src.len()..], &[0xA5, 0xA5]);

        let mut decoded = encoded.clone();
        let mut scratch = vec![0x5Au8; src_with_tail.len()];
        let current = apply_filter_pipeline_for_decompression(
            &mut decoded,
            &mut scratch,
            src_with_tail.len(),
            &filters,
            &filters_meta,
            BLOSC2_VERSION_FORMAT,
            4,
            0,
            None,
            1,
        );
        assert_ne!(current, 0);
        let decoded = if current == 1 { decoded } else { scratch };
        assert_eq!(&decoded[..src.len()], &expected);
        assert_eq!(&decoded[src.len()..], &[0xA5, 0xA5]);
    }

    #[test]
    fn test_int_trunc_context_uses_cparams_typesize() {
        let values = [0x1234_5678u32, 0x8765_4321];
        let src: Vec<u8> = values.into_iter().flat_map(u32::to_ne_bytes).collect();
        let filters = [0, 0, 0, 0, 0, BLOSC_FILTER_INT_TRUNC];
        let mut filters_meta = [0; BLOSC2_MAX_FILTERS];
        filters_meta[BLOSC2_MAX_FILTERS - 1] = 20;
        let cparams = FilterCParamsContext {
            compcode: 0,
            compcode_meta: 0,
            clevel: 0,
            use_dict: false,
            typesize: 4,
            blocksize: src.len() as i32,
            splitmode: 0,
            filters,
            filters_meta,
            nthreads: 1,
            nchunk: -1,
            user_data: 0,
            preparams: 0,
            tuner_id: 0,
            instr_codec: false,
            codec_params: 0,
        };
        let context = FilterPipelineContext {
            cparams: Some(&cparams),
            dparams: None,
            chunk: FilterChunkContext {
                schunk: 0,
                nchunk: -1,
                nblock: 0,
                block_offset: 0,
                blocksize: src.len(),
                bsize: src.len(),
            },
            b2nd_metalayer: None,
            user_data: 0,
        };
        let mut encoded = vec![0u8; src.len()];
        let mut scratch = vec![0u8; src.len()];

        let current = apply_filter_pipeline_for_compression_with_context(
            &src,
            &mut encoded,
            &mut scratch,
            &filters,
            &filters_meta,
            2,
            0,
            None,
            Some(context),
        );
        assert_ne!(current, 0);
        let encoded = if current == 1 { encoded } else { scratch };
        let expected: Vec<u8> = [0x1234_5000u32, 0x8765_4000]
            .into_iter()
            .flat_map(u32::to_ne_bytes)
            .collect();
        assert_eq!(encoded, expected);

        let bad_cparams = FilterCParamsContext {
            typesize: 3,
            ..cparams
        };
        let mut bad_ctx = FilterCallbackContext {
            filter_id: BLOSC_FILTER_INT_TRUNC,
            filter_slot: BLOSC2_MAX_FILTERS - 1,
            meta: 20,
            typesize: 3,
            cparams: Some(&bad_cparams),
            dparams: None,
            chunk: FilterChunkContext {
                schunk: 0,
                nchunk: -1,
                nblock: 0,
                block_offset: 0,
                blocksize: src.len(),
                bsize: src.len(),
            },
            b2nd_metalayer: None,
            user_data: 0,
        };
        let mut bad_dest = vec![0u8; src.len()];
        assert_eq!(
            apply_int_trunc_filter(&mut bad_ctx, &src, &mut bad_dest),
            BLOSC2_ERROR_FAILURE
        );
        let mut short_dest = vec![0u8; src.len() - 1];
        assert_eq!(
            undo_int_trunc_filter(&mut bad_ctx, &src, &mut short_dest),
            BLOSC2_ERROR_FAILURE
        );
    }

    #[test]
    fn test_register_global_filter_accepts_c_global_registered_ids() {
        const FILTER_ID: u8 = BLOSC2_GLOBAL_REGISTERED_FILTERS_START + 7;

        assert_eq!(
            register_global_filter(BLOSC_TRUNC_PREC, copy_filter, copy_filter),
            Err("Global plugin filter IDs must be in 32..=159")
        );
        assert!(register_global_filter(FILTER_ID, copy_filter, copy_filter).is_ok());
        assert!(is_registered_filter(FILTER_ID));
        assert!(register_global_filter(FILTER_ID, copy_filter, copy_filter).is_ok());
        assert_eq!(
            register_global_filter(FILTER_ID, copy_filter, reverse_filter),
            Err("Global plugin filter ID already registered")
        );
        const NAMED_FILTER_ID: u8 = BLOSC2_GLOBAL_REGISTERED_FILTERS_START + 8;
        assert!(register_named_global_filter(
            NAMED_FILTER_ID,
            "named-global-copy",
            copy_filter,
            copy_filter
        )
        .is_ok());
        assert!(register_named_global_filter(
            NAMED_FILTER_ID,
            "named-global-copy",
            reverse_filter,
            reverse_filter
        )
        .is_ok());
        assert_eq!(
            register_named_global_filter(
                NAMED_FILTER_ID,
                "other-named-global-copy",
                reverse_filter,
                reverse_filter
            ),
            Err("Global plugin filter ID already registered")
        );
        assert_eq!(
            register_named_global_filter(
                NAMED_FILTER_ID,
                "third-named-global-copy",
                copy_filter,
                copy_filter
            ),
            Err("Global plugin filter ID already registered")
        );
        const METADATA_FILTER_ID: u8 = BLOSC2_GLOBAL_REGISTERED_FILTERS_START + 70;
        assert_eq!(
            register_global_filter_with_metadata(
                METADATA_FILTER_ID,
                "global-copy",
                3,
                copy_filter,
                copy_filter,
            ),
            Ok(())
        );
        assert_eq!(
            registered_filter_info(METADATA_FILTER_ID),
            Some(("global-copy", 3))
        );
        assert_eq!(
            register_global_fallible_filter_with_metadata(
                METADATA_FILTER_ID + 1,
                "global-fallible-copy",
                4,
                copy_fallible_filter,
                copy_fallible_filter,
            ),
            Ok(())
        );
        assert_eq!(
            registered_filter_info(METADATA_FILTER_ID + 1),
            Some(("global-fallible-copy", 4))
        );
        assert_eq!(
            register_global_filter(BLOSC2_USER_DEFINED_FILTERS_START, copy_filter, copy_filter),
            Err("Global plugin filter IDs must be in 32..=159")
        );
    }

    #[test]
    fn test_fallible_filter_callbacks_can_fail_pipeline() {
        const FORWARD_FAIL_ID: u8 = BLOSC2_USER_DEFINED_FILTERS_START + 30;
        const BACKWARD_FAIL_ID: u8 = BLOSC2_USER_DEFINED_FILTERS_START + 31;
        register_fallible_filter(
            FORWARD_FAIL_ID,
            failing_fallible_filter,
            copy_fallible_filter,
        )
        .unwrap();
        assert!(register_fallible_filter(
            FORWARD_FAIL_ID,
            failing_fallible_filter,
            copy_fallible_filter,
        )
        .is_ok());
        assert_eq!(
            register_fallible_filter(FORWARD_FAIL_ID, copy_fallible_filter, copy_fallible_filter),
            Err("User-defined filter ID already registered")
        );
        register_fallible_filter(
            BACKWARD_FAIL_ID,
            copy_fallible_filter,
            failing_fallible_filter,
        )
        .unwrap();

        let src = [1u8, 2, 3, 4];
        let mut buf1 = [0u8; 4];
        let mut buf2 = [0u8; 4];
        let mut filters = [BLOSC_NOFILTER; BLOSC2_MAX_FILTERS];
        let meta = [0u8; BLOSC2_MAX_FILTERS];

        filters[BLOSC2_MAX_FILTERS - 1] = FORWARD_FAIL_ID;
        assert_eq!(
            apply_filter_pipeline_for_compression(
                &src, &mut buf1, &mut buf2, &filters, &meta, 1, 0, None
            ),
            0
        );

        filters[BLOSC2_MAX_FILTERS - 1] = BACKWARD_FAIL_ID;
        buf1.copy_from_slice(&src);
        assert_eq!(
            apply_filter_pipeline_for_decompression(
                &mut buf1, &mut buf2, 4, &filters, &meta, 1, 1, 0, None, 1
            ),
            0
        );
    }

    #[test]
    fn test_reserved_defined_filter_ids_fail_forward_dispatch() {
        let src = [1u8, 2, 3, 4];
        let mut buf1 = [0u8; 4];
        let mut buf2 = [0u8; 4];
        let mut filters = [BLOSC_NOFILTER; BLOSC2_MAX_FILTERS];
        let meta = [0u8; BLOSC2_MAX_FILTERS];

        filters[BLOSC2_MAX_FILTERS - 1] = BLOSC_LAST_FILTER;
        assert_eq!(
            apply_filter_pipeline_for_compression(
                &src, &mut buf1, &mut buf2, &filters, &meta, 1, 0, None
            ),
            0
        );
    }

    #[test]
    fn test_pipeline_forward_bitshuffle_rejects_raw_c_invalid_typesize() {
        let src = vec![1u8; 1024];
        let mut buf1 = vec![0u8; src.len()];
        let mut buf2 = vec![0u8; src.len()];
        let mut filters = [BLOSC_NOFILTER; BLOSC2_MAX_FILTERS];
        let filters_meta = [0u8; BLOSC2_MAX_FILTERS];
        filters[BLOSC2_MAX_FILTERS - 1] = BLOSC_BITSHUFFLE;

        assert_eq!(
            apply_filter_pipeline_for_compression(
                &src,
                &mut buf1,
                &mut buf2,
                &filters,
                &filters_meta,
                257,
                0,
                None
            ),
            0
        );
    }

    #[test]
    fn test_pipeline_shuffle_uses_raw_c_invalid_typesize_noop() {
        let src = vec![1u8; 1024];
        let mut buf1 = vec![0xA5; src.len()];
        let mut buf2 = vec![0x5A; src.len()];
        let mut filters = [BLOSC_NOFILTER; BLOSC2_MAX_FILTERS];
        let filters_meta = [0u8; BLOSC2_MAX_FILTERS];
        filters[BLOSC2_MAX_FILTERS - 1] = BLOSC_SHUFFLE;

        assert_eq!(
            apply_filter_pipeline_for_compression(
                &src,
                &mut buf1,
                &mut buf2,
                &filters,
                &filters_meta,
                257,
                0,
                None
            ),
            1
        );
        assert_eq!(buf1, vec![0xA5; src.len()]);

        assert_eq!(
            apply_filter_pipeline_for_decompression(
                &mut buf1,
                &mut buf2,
                src.len(),
                &filters,
                &filters_meta,
                BLOSC2_VERSION_FORMAT,
                257,
                0,
                None,
                1,
            ),
            2
        );
        assert_eq!(buf2, vec![0x5A; src.len()]);
    }

    #[test]
    fn test_user_filter_success_without_writes_leaves_output_buffer_unchanged() {
        const FILTER_ID: u8 = BLOSC2_USER_DEFINED_FILTERS_START + 32;
        register_fallible_filter(
            FILTER_ID,
            unwritten_fallible_filter,
            unwritten_fallible_filter,
        )
        .unwrap();

        let src = [1u8, 2, 3, 4];
        let mut buf1 = [0xA5u8; 4];
        let mut buf2 = [0x5Au8; 4];
        let mut filters = [BLOSC_NOFILTER; BLOSC2_MAX_FILTERS];
        let meta = [0u8; BLOSC2_MAX_FILTERS];

        filters[BLOSC2_MAX_FILTERS - 1] = FILTER_ID;
        let current = apply_filter_pipeline_for_compression(
            &src, &mut buf1, &mut buf2, &filters, &meta, 1, 0, None,
        );
        assert_eq!(if current == 1 { buf1 } else { buf2 }, [0xA5u8; 4]);

        buf1 = [0xA5; 4];
        buf2 = [0x5A; 4];
        let current = apply_filter_pipeline_for_decompression(
            &mut buf1, &mut buf2, 4, &filters, &meta, 1, 1, 0, None, 1,
        );
        assert_eq!(if current == 1 { buf1 } else { buf2 }, [0x5Au8; 4]);
    }

    #[test]
    fn test_delta_backward_uses_raw_reference_before_prefix_filters() {
        const PREFIX_FAIL_ID: u8 = BLOSC2_USER_DEFINED_FILTERS_START + 33;
        register_fallible_filter(
            PREFIX_FAIL_ID,
            failing_fallible_filter,
            copy_fallible_filter,
        )
        .unwrap();

        let src = [1u8, 2, 3, 4, 5, 6, 7, 8];
        let dref = [10u8, 11, 12, 13, 14, 15, 16, 17];
        let mut buf1 = src;
        let mut buf2 = [0u8; 8];
        let mut filters = [BLOSC_NOFILTER; BLOSC2_MAX_FILTERS];
        let meta = [0u8; BLOSC2_MAX_FILTERS];

        filters[0] = PREFIX_FAIL_ID;
        filters[1] = BLOSC_DELTA;
        let current = apply_filter_pipeline_for_decompression(
            &mut buf1,
            &mut buf2,
            8,
            &filters,
            &meta,
            BLOSC2_VERSION_FORMAT,
            1,
            8,
            Some(&dref),
            1,
        );
        assert_ne!(current, 0);
        let decoded = if current == 1 { buf1 } else { buf2 };
        assert_eq!(
            decoded,
            std::array::from_fn::<_, 8, _>(|idx| src[idx] ^ dref[idx])
        );
    }

    #[test]
    fn test_register_global_fallible_filter_accepts_c_global_registered_ids() {
        const FILTER_ID: u8 = BLOSC2_GLOBAL_REGISTERED_FILTERS_START + 11;

        assert_eq!(
            register_global_fallible_filter(
                BLOSC_TRUNC_PREC,
                copy_fallible_filter,
                copy_fallible_filter
            ),
            Err("Global plugin filter IDs must be in 32..=159")
        );
        assert!(register_global_fallible_filter(
            FILTER_ID,
            copy_fallible_filter,
            copy_fallible_filter
        )
        .is_ok());
        assert!(is_registered_filter(FILTER_ID));
        assert!(register_global_fallible_filter(
            FILTER_ID,
            copy_fallible_filter,
            copy_fallible_filter
        )
        .is_ok());
        assert_eq!(
            register_global_fallible_filter(
                FILTER_ID,
                failing_fallible_filter,
                copy_fallible_filter
            ),
            Err("Global plugin filter ID already registered")
        );
        assert_eq!(
            register_global_fallible_filter(
                BLOSC2_USER_DEFINED_FILTERS_START,
                copy_fallible_filter,
                copy_fallible_filter
            ),
            Err("Global plugin filter IDs must be in 32..=159")
        );
    }

    #[test]
    fn test_shuffle_unshuffle_roundtrip() {
        let data: Vec<u8> = (0..32).collect();
        let mut shuffled = vec![0u8; 32];
        let mut restored = vec![0u8; 32];

        shuffle(4, &data, &mut shuffled);
        assert_ne!(data, shuffled);
        unshuffle(4, &shuffled, &mut restored);
        assert_eq!(data, restored);
    }

    #[test]
    fn test_shuffle_dispatch_matches_scalar_for_simd_widths_and_leftovers() {
        for typesize in [2, 4, 8] {
            let avx2_chunk = typesize * 32;
            for len in [
                avx2_chunk - 1,
                avx2_chunk,
                avx2_chunk + 1,
                avx2_chunk * 2 + typesize - 1,
                avx2_chunk * 3 + 7,
            ] {
                let data: Vec<u8> = (0..len)
                    .map(|i: usize| (i.wrapping_mul(29).wrapping_add(typesize)) as u8)
                    .collect();
                let mut expected = vec![0u8; len];
                let mut actual = vec![0u8; len];
                let mut restored = vec![0u8; len];
                let mut scalar_restored = vec![0u8; len];

                scalar_shuffle_for_test(typesize, &data, &mut expected);
                shuffle(typesize, &data, &mut actual);
                assert_eq!(
                    actual, expected,
                    "shuffle dispatch diverged from scalar for typesize={typesize} len={len}"
                );

                scalar_unshuffle_for_test(typesize, &expected, &mut scalar_restored);
                unshuffle(typesize, &actual, &mut restored);
                assert_eq!(
                    restored, scalar_restored,
                    "unshuffle dispatch diverged from scalar for typesize={typesize} len={len}"
                );
                assert_eq!(restored, data);
            }
        }
    }

    #[test]
    fn test_shuffle_typesize_1() {
        let data: Vec<u8> = (0..16).collect();
        let mut shuffled = vec![0u8; 16];
        shuffle(1, &data, &mut shuffled);
        assert_eq!(data, shuffled); // typesize 1 = no-op
    }

    #[test]
    fn test_shuffle_rejects_short_destinations() {
        let data: Vec<u8> = (0..16).collect();
        let mut dest = vec![0xA5; 15];

        shuffle(4, &data, &mut dest);
        assert_eq!(dest, vec![0xA5; 15]);

        unshuffle(4, &data, &mut dest);
        assert_eq!(dest, vec![0xA5; 15]);
    }

    #[test]
    fn test_shuffle_unshuffle_typesize4_large_roundtrip() {
        let data: Vec<u8> = (0..1027usize)
            .map(|i| (i.wrapping_mul(31).wrapping_add(7)) as u8)
            .collect();
        let mut shuffled = vec![0u8; data.len()];
        let mut restored = vec![0u8; data.len()];

        shuffle(4, &data, &mut shuffled);
        unshuffle(4, &shuffled, &mut restored);
        assert_eq!(data, restored);
    }

    #[test]
    fn test_bitshuffle_roundtrip() {
        // Size must be a multiple of 8 elements
        let data: Vec<u8> = (0..64).collect(); // 16 elements of 4 bytes
        let mut shuffled = vec![0u8; 64];
        let mut restored = vec![0u8; 64];

        bitshuffle(4, &data, &mut shuffled);
        bitunshuffle(4, &shuffled, &mut restored);
        assert_eq!(data, restored);
    }

    #[test]
    fn test_bitshuffle_preserves_leftover_elements() {
        for typesize in [1, 2, 4, 8, 16] {
            for extra_elements in [1, 3, 5, 7] {
                let len = (16 + extra_elements) * typesize;
                let data: Vec<u8> = (0..len)
                    .map(|i: usize| (i.wrapping_mul(37).wrapping_add(typesize)) as u8)
                    .collect();
                let mut shuffled = vec![0u8; len];
                let mut restored = vec![0u8; len];

                assert_eq!(bitshuffle(typesize, &data, &mut shuffled), len as i64);
                assert_eq!(bitunshuffle(typesize, &shuffled, &mut restored), len as i64);
                assert_eq!(
                    data, restored,
                    "bitshuffle leftover roundtrip failed for typesize={typesize} extra_elements={extra_elements}"
                );
            }
        }
    }

    #[test]
    fn test_bitshuffle_dispatch_matches_scalar_for_typesizes_and_leftovers() {
        for typesize in [1usize, 2, 3, 4, 8, 16] {
            for extra_elements in 0..8 {
                let len = (40 + extra_elements) * typesize + (typesize / 2);
                let data: Vec<u8> = (0..len)
                    .map(|i: usize| (i.wrapping_mul(37) ^ (i >> 3).wrapping_mul(11)) as u8)
                    .collect();

                let mut dispatched = vec![0u8; len];
                let mut scalar = vec![0u8; len];
                let mut dispatch_scratch = vec![0u8; len];
                let mut scalar_scratch = vec![0u8; len];
                assert_eq!(
                    bitshuffle_with_scratch(
                        typesize,
                        &data,
                        &mut dispatched,
                        Some(&mut dispatch_scratch)
                    ),
                    len as i64
                );
                assert_eq!(
                    bitshuffle_scalar_with_scratch(
                        typesize,
                        &data,
                        &mut scalar,
                        Some(&mut scalar_scratch)
                    ),
                    len as i64
                );
                assert_eq!(
                    dispatched, scalar,
                    "bitshuffle dispatch mismatch for typesize={typesize} extra_elements={extra_elements}"
                );

                let mut dispatched_restored = vec![0u8; len];
                let mut scalar_restored = vec![0u8; len];
                assert_eq!(
                    bitunshuffle_with_scratch(
                        typesize,
                        &dispatched,
                        &mut dispatched_restored,
                        Some(&mut dispatch_scratch)
                    ),
                    len as i64
                );
                assert_eq!(
                    bitunshuffle_scalar_with_scratch(
                        typesize,
                        &scalar,
                        &mut scalar_restored,
                        Some(&mut scalar_scratch)
                    ),
                    len as i64
                );
                assert_eq!(
                    dispatched_restored, scalar_restored,
                    "bitunshuffle dispatch mismatch for typesize={typesize} extra_elements={extra_elements}"
                );
                assert_eq!(dispatched_restored, data);
            }
        }
    }

    #[test]
    fn test_bitshuffle_rejects_short_buffers() {
        let data: Vec<u8> = (0..64).collect();
        let mut short_dest = vec![0u8; 63];
        let mut scratch = vec![0u8; 63];
        let mut dest = vec![0u8; 64];

        assert_eq!(bitshuffle(4, &data, &mut short_dest), 0);
        assert_eq!(bitunshuffle(4, &data, &mut short_dest), 0);
        assert_eq!(
            bitshuffle_with_scratch(4, &data, &mut dest, Some(&mut scratch)),
            0
        );
        assert_eq!(
            bitunshuffle_with_scratch(4, &data, &mut dest, Some(&mut scratch)),
            0
        );
    }

    #[test]
    fn test_delta_roundtrip() {
        let dref: Vec<u8> = (0..16).collect();
        let src: Vec<u8> = (10..26).collect();
        let mut encoded = vec![0u8; 16];
        let mut decoded = vec![0u8; 16];

        // Non-reference block (offset != 0)
        delta_encode(&dref, 1, 16, 1, &src, &mut encoded);
        decoded.copy_from_slice(&encoded);
        delta_decode(Some(&dref), 1, 16, 1, &mut decoded);
        assert_eq!(src, decoded);
    }

    #[test]
    fn test_delta_reference_block() {
        // For offset=0, dref should equal the source data (no prior filters).
        // The encoder uses dref for XOR reference, and C's decoder uses the
        // dref pointer it was passed.
        let src: Vec<u8> = (0..16).map(|i| i * 3 + 7).collect();
        let mut encoded = vec![0u8; 16];
        let mut decoded = vec![0u8; 16];

        // Reference block (offset == 0) — dref == src
        delta_encode(&src, 0, 16, 1, &src, &mut encoded);
        decoded.copy_from_slice(&encoded);
        delta_decode(Some(&src), 0, 16, 1, &mut decoded);
        assert_eq!(src, decoded);

        decoded.copy_from_slice(&encoded);
        delta_decode(None, 0, 16, 1, &mut decoded);
        assert_eq!(src, decoded);
    }

    #[test]
    fn test_pipeline_delta_reference_block_uses_raw_src_after_prefix_filters() {
        let src: Vec<u8> = (0..16).map(|i| i * 5 + 11).collect();
        let mut buf1 = vec![0u8; src.len()];
        let mut buf2 = vec![0u8; src.len()];
        let filters = [BLOSC_SHUFFLE, BLOSC_DELTA, 0, 0, 0, 0];
        let filters_meta = [0u8; BLOSC2_MAX_FILTERS];

        let current = apply_filter_pipeline_for_compression(
            &src,
            &mut buf1,
            &mut buf2,
            &filters,
            &filters_meta,
            4,
            0,
            None,
        );
        assert_ne!(current, 0);
        let encoded = if current == 1 { &buf1 } else { &buf2 };

        let mut shuffled = vec![0u8; src.len()];
        let mut expected = vec![0u8; src.len()];
        shuffle(4, &src, &mut shuffled);
        delta_encode(&src, 0, src.len(), 4, &shuffled, &mut expected);
        assert_eq!(encoded, &expected);
    }

    #[test]
    fn test_pipeline_delta_reference_block_decode_ignores_external_dref() {
        let src: Vec<u8> = (0..16).map(|i| i * 7 + 3).collect();
        let wrong_dref = vec![0xA5; src.len()];
        let filters = [BLOSC_DELTA, 0, 0, 0, 0, 0];
        let filters_meta = [0u8; BLOSC2_MAX_FILTERS];

        let mut encoded = vec![0u8; src.len()];
        delta_encode(&src, 0, src.len(), 4, &src, &mut encoded);

        let mut buf1 = encoded;
        let mut buf2 = vec![0u8; src.len()];
        let current = apply_filter_pipeline_for_decompression(
            &mut buf1,
            &mut buf2,
            src.len(),
            &filters,
            &filters_meta,
            BLOSC2_VERSION_FORMAT,
            4,
            0,
            Some(&wrong_dref),
            1,
        );

        assert_eq!(current, 2);
        assert_eq!(buf2, src);
    }

    // C's delta_{encoder,decoder} (c-blosc2/blosc/delta.c) falls back to typesize=1
    // for non-power-of-two typesizes that are not multiples of 8, and to typesize=8
    // for multiples of 8. Rust must match for cross-compatibility.
    #[test]
    fn test_delta_falls_back_to_byte_level_for_typesize_3() {
        // Reference block (offset=0). C's encoder at typesize=3 degrades to typesize=1
        // which means dest[0]=dref[0], and dest[i]=src[i]^dref[i-1] for i>=1.
        let src: Vec<u8> = vec![
            0xA0, 0xA1, 0xA2, 0xB0, 0xB1, 0xB2, 0xC0, 0xC1, 0xC2, 0xD0, 0xD1, 0xD2,
        ];
        let mut encoded = vec![0u8; 12];
        // dref=src for offset==0 per apply_filter_pipeline_for_compression convention.
        delta_encode(&src, 0, 12, 3, &src, &mut encoded);

        // Expected (what C does): typesize=1 fallback.
        let mut expected = vec![0u8; 12];
        expected[0] = src[0];
        for i in 1..12 {
            expected[i] = src[i] ^ src[i - 1];
        }

        assert_eq!(
            encoded, expected,
            "delta_encode with typesize=3 must degrade to byte-level (C-compatible)"
        );

        // Symmetric check for decode.
        let mut dest = encoded.clone();
        delta_decode(Some(&src), 0, 12, 3, &mut dest);
        assert_eq!(dest, src, "decode must roundtrip after C-compatible encode");
    }

    #[test]
    fn test_delta_falls_back_to_u64_for_typesize_16() {
        // typesize=16 is a multiple of 8 → C falls back to typesize=8.
        let src: Vec<u8> = (0..32).map(|i| i as u8 ^ 0xA5).collect();
        let mut encoded = vec![0u8; 32];
        delta_encode(&src, 0, 32, 16, &src, &mut encoded);

        // Expected: typesize=8 behavior. Copy first 8, then XOR 8-byte blocks.
        let mut expected = vec![0u8; 32];
        expected[..8].copy_from_slice(&src[..8]);
        for i in 8..32 {
            expected[i] = src[i] ^ src[i - 8];
        }

        assert_eq!(
            encoded, expected,
            "delta_encode with typesize=16 must degrade to 8-byte granularity (C-compatible)"
        );

        let mut dest = encoded.clone();
        delta_decode(Some(&src), 0, 32, 16, &mut dest);
        assert_eq!(dest, src);
    }

    #[test]
    fn test_delta_rejects_invalid_buffers() {
        let src: Vec<u8> = (0..16).collect();
        let dref: Vec<u8> = (16..32).collect();
        let mut dest = vec![0xA5; 16];

        delta_encode(&dref, 1, 16, 0, &src, &mut dest);
        assert_eq!(dest, vec![0xA5; 16]);

        delta_encode(&dref, 1, 16, 1, &src[..15], &mut dest);
        assert_eq!(dest, vec![0xA5; 16]);

        delta_encode(&dref, 1, 16, 1, &src, &mut dest[..15]);
        assert_eq!(dest[..15], vec![0xA5; 15]);
        assert_eq!(dest[15], 0xA5);

        delta_encode(&dref[..15], 1, 16, 1, &src, &mut dest);
        assert_eq!(dest, vec![0xA5; 16]);

        delta_encode(&src[..15], 0, 16, 1, &src, &mut dest);
        assert_eq!(dest, vec![0xA5; 16]);

        delta_decode(Some(&dref), 1, 16, 0, &mut dest);
        assert_eq!(dest, vec![0xA5; 16]);

        delta_decode(Some(&dref), 1, 16, 1, &mut dest[..15]);
        assert_eq!(dest[..15], vec![0xA5; 15]);
        assert_eq!(dest[15], 0xA5);

        delta_decode(Some(&dref[..15]), 1, 16, 1, &mut dest);
        assert_eq!(dest, vec![0xA5; 16]);
    }

    #[test]
    fn test_delta_leaves_partial_tail_unwritten_for_fixed_widths() {
        for typesize in [2usize, 4, 8] {
            let nbytes = typesize * 3 + (typesize - 1);
            let src: Vec<u8> = (0..nbytes)
                .map(|i: usize| (i.wrapping_mul(17).wrapping_add(3)) as u8)
                .collect();
            let dref: Vec<u8> = (0..nbytes)
                .map(|i: usize| (i.wrapping_mul(29).wrapping_add(11)) as u8)
                .collect();
            let main_len = nbytes - (nbytes % typesize);

            let mut encoded = vec![0xA5; nbytes];
            delta_encode(&dref, 1, nbytes, typesize, &src, &mut encoded);
            assert_eq!(
                &encoded[main_len..],
                vec![0xA5; nbytes - main_len].as_slice(),
                "delta_encode must leave partial tail bytes unwritten for typesize={typesize}"
            );

            let mut ref_encoded = vec![0xA5; nbytes];
            delta_encode(&src, 0, nbytes, typesize, &src, &mut ref_encoded);
            assert_eq!(
                &ref_encoded[main_len..],
                vec![0xA5; nbytes - main_len].as_slice(),
                "reference-block delta must leave partial tail bytes unwritten for typesize={typesize}"
            );
        }
    }

    #[test]
    fn test_delta_reference_block_smaller_than_one_fixed_width_element_is_unwritten() {
        for typesize in [2usize, 4, 8] {
            let nbytes = typesize - 1;
            let src: Vec<u8> = (0..nbytes)
                .map(|i: usize| (i.wrapping_mul(17).wrapping_add(3)) as u8)
                .collect();
            let mut encoded = vec![0xA5; nbytes];

            delta_encode(&src, 0, nbytes, typesize, &src, &mut encoded);

            assert_eq!(
                encoded,
                vec![0xA5; nbytes],
                "C delta_encoder leaves sub-element reference blocks untouched for typesize={typesize}"
            );
        }
    }

    #[test]
    fn test_bitunshuffle_v2_with_leftovers_falls_back_to_memcpy() {
        let data: Vec<u8> = (0..20u8).collect();
        let mut dest = vec![0u8; data.len()];
        let processed =
            bitunshuffle_with_format_version(4, &data, &mut dest, BLOSC1_VERSION_FORMAT);
        assert_eq!(processed, data.len() as i64);
        assert_eq!(dest, data);
    }

    #[test]
    fn test_pipeline_rejects_invalid_buffers() {
        let src: Vec<u8> = (0..16).collect();
        let mut buf1 = vec![0xA5; 16];
        let mut buf2 = vec![0x5A; 16];
        let mut short_buf = vec![0u8; 15];
        let filters = [BLOSC_SHUFFLE, 0, 0, 0, 0, 0];
        let filters_meta = [0; BLOSC2_MAX_FILTERS];

        assert_eq!(
            apply_filter_pipeline_for_compression(
                &src,
                &mut short_buf,
                &mut buf2,
                &filters,
                &filters_meta,
                4,
                0,
                None,
            ),
            0
        );
        assert_eq!(buf2, vec![0x5A; 16]);

        assert_eq!(
            apply_filter_pipeline_for_compression(
                &src,
                &mut buf1,
                &mut short_buf,
                &filters,
                &filters_meta,
                4,
                0,
                None,
            ),
            0
        );
        assert_eq!(buf1, vec![0xA5; 16]);

        assert_eq!(
            apply_filter_pipeline_for_decompression(
                &mut buf1,
                &mut buf2,
                16,
                &filters,
                &filters_meta,
                BLOSC2_VERSION_FORMAT,
                4,
                0,
                None,
                0,
            ),
            0
        );

        assert_eq!(
            apply_filter_pipeline_for_decompression(
                &mut short_buf,
                &mut buf2,
                16,
                &filters,
                &filters_meta,
                BLOSC2_VERSION_FORMAT,
                4,
                0,
                None,
                1,
            ),
            0
        );
    }

    #[test]
    fn test_pipeline_rejects_unknown_filters() {
        let src: Vec<u8> = (0..16).collect();
        let mut buf1 = vec![0xA5; 16];
        let mut buf2 = vec![0x5A; 16];
        let filters = [BLOSC2_USER_DEFINED_FILTERS_START - 1, 0, 0, 0, 0, 0];
        let filters_meta = [0; BLOSC2_MAX_FILTERS];

        assert_eq!(
            apply_filter_pipeline_for_compression(
                &src,
                &mut buf1,
                &mut buf2,
                &filters,
                &filters_meta,
                4,
                0,
                None,
            ),
            0
        );
        assert_eq!(buf1, vec![0xA5; 16]);
        assert_eq!(buf2, vec![0x5A; 16]);

        buf1.copy_from_slice(&src);
        buf2.fill(0x5A);
        assert_eq!(
            apply_filter_pipeline_for_decompression(
                &mut buf1,
                &mut buf2,
                16,
                &filters,
                &filters_meta,
                BLOSC2_VERSION_FORMAT,
                4,
                0,
                None,
                1,
            ),
            0
        );
        assert_eq!(buf1, src);
        assert_eq!(buf2, vec![0x5A; 16]);
    }

    #[test]
    fn test_pipeline_backward_unknown_defined_filter_cycles_before_failure() {
        let src: Vec<u8> = (0..16).collect();
        let mut buf1 = src.clone();
        let mut buf2 = vec![0x5A; 16];
        let filters = [BLOSC_SHUFFLE, BLOSC_LAST_FILTER, 0, 0, 0, 0];
        let filters_meta = [0; BLOSC2_MAX_FILTERS];

        assert_eq!(
            apply_filter_pipeline_for_decompression(
                &mut buf1,
                &mut buf2,
                16,
                &filters,
                &filters_meta,
                BLOSC2_VERSION_FORMAT,
                4,
                0,
                None,
                1,
            ),
            0
        );
        assert_eq!(buf1, vec![0x5A; 16]);
        assert_eq!(buf2, vec![0x5A; 16]);
    }

    #[test]
    fn test_pipeline_rejects_invalid_trunc_prec() {
        let src: Vec<u8> = (0..16).collect();
        let mut buf1 = vec![0u8; 16];
        let mut buf2 = vec![0u8; 16];

        let filters = [BLOSC_TRUNC_PREC, 0, 0, 0, 0, 0];
        let filters_meta = [16; BLOSC2_MAX_FILTERS];
        assert_eq!(
            apply_filter_pipeline_for_compression(
                &src,
                &mut buf1,
                &mut buf2,
                &filters,
                &filters_meta,
                2,
                0,
                None,
            ),
            0
        );
        assert_eq!(buf1, vec![0u8; 16]);
        assert_eq!(buf2, vec![0u8; 16]);

        let filters_meta = [0; BLOSC2_MAX_FILTERS];
        buf1.fill(0xA5);
        buf2.fill(0x5A);
        assert_eq!(
            apply_filter_pipeline_for_compression(
                &src,
                &mut buf1,
                &mut buf2,
                &filters,
                &filters_meta,
                4,
                0,
                None,
            ),
            0
        );
        assert_eq!(buf1, vec![0xA5; 16]);
        assert_eq!(buf2, vec![0x5A; 16]);
    }

    #[test]
    fn test_pipeline_backward_trunc_prec_does_not_cycle_buffers() {
        let src: Vec<u8> = [1.3333333f32, -7.25, 1024.5, 0.03125]
            .into_iter()
            .flat_map(f32::to_ne_bytes)
            .collect();
        let filters = [BLOSC_TRUNC_PREC, 0, 0, 0, 0, 0];
        let filters_meta = [10; BLOSC2_MAX_FILTERS];
        let mut buf1 = vec![0u8; src.len()];
        let mut buf2 = vec![0u8; src.len()];

        let current = apply_filter_pipeline_for_compression(
            &src,
            &mut buf1,
            &mut buf2,
            &filters,
            &filters_meta,
            4,
            0,
            None,
        );
        assert_eq!(current, 1);

        let mut expected = vec![0u8; src.len()];
        assert!(trunc_prec_forward(&src, &mut expected, 4, 10));
        assert_eq!(buf1, expected);

        let before_other_buffer = buf2.clone();
        let current = apply_filter_pipeline_for_decompression(
            &mut buf1,
            &mut buf2,
            src.len(),
            &filters,
            &filters_meta,
            BLOSC2_VERSION_FORMAT,
            4,
            0,
            None,
            current,
        );
        assert_eq!(current, 1);
        assert_eq!(buf1, expected);
        assert_eq!(buf2, before_other_buffer);
    }

    #[test]
    fn test_pipeline_rejects_invalid_bitshuffle_typesize() {
        let src: Vec<u8> = (0..16).collect();
        let mut buf1 = vec![0xA5; 16];
        let mut buf2 = vec![0x5A; 16];
        let filters = [BLOSC_BITSHUFFLE, 0, 0, 0, 0, 0];
        let filters_meta = [0; BLOSC2_MAX_FILTERS];

        assert_eq!(
            apply_filter_pipeline_for_compression(
                &src,
                &mut buf1,
                &mut buf2,
                &filters,
                &filters_meta,
                0,
                0,
                None,
            ),
            0
        );
        assert_eq!(buf1, vec![0xA5; 16]);
        assert_eq!(buf2, vec![0x5A; 16]);

        buf1.copy_from_slice(&src);
        assert_eq!(
            apply_filter_pipeline_for_decompression(
                &mut buf1,
                &mut buf2,
                16,
                &filters,
                &filters_meta,
                BLOSC2_VERSION_FORMAT,
                0,
                0,
                None,
                1,
            ),
            0
        );
    }

    #[test]
    fn test_trunc_prec_leaves_tail_bytes_unwritten() {
        let src = [0xFFu8, 0xFF, 0xFF, 0xFF, 0xAA, 0xBB];
        let mut dest = [0xA5u8; 6];

        // prec_bits=16 > BITS_MANTISSA_F32(23), valid. Zero low 7 mantissa bits.
        assert!(trunc_prec_forward(&src, &mut dest, 4, 16));

        assert_eq!(&dest[4..], &[0xA5, 0xA5]);
    }

    // C's truncate_precision32 (c-blosc2/blosc/trunc-prec.c) only zeros mantissa
    // bits (BITS_MANTISSA_FLOAT = 23) — the sign and 8-bit exponent are preserved
    // so the result is still a valid IEEE-754 approximation of the input.
    // Rust must match.
    #[test]
    fn test_trunc_prec_f32_preserves_sign_and_exponent() {
        // 1.333... = 0x3FAAAAAB in IEEE-754. prec_bits = 10 → clear low 13 mantissa bits.
        let original: f32 = 1.3333333;
        let src = original.to_le_bytes();
        let mut dest = [0u8; 4];

        assert!(trunc_prec_forward(&src, &mut dest, 4, 10));

        let out = f32::from_le_bytes(dest);
        let out_bits = out.to_bits();
        let orig_bits = original.to_bits();
        // Sign and exponent (top 9 bits) must be preserved.
        assert_eq!(
            out_bits & 0xFF800000,
            orig_bits & 0xFF800000,
            "trunc_prec must preserve sign+exponent: original={:#x} got={:#x}",
            orig_bits,
            out_bits
        );
        // The output must be a reasonable approximation of the input (within 1%).
        let rel_err = ((out - original) / original).abs();
        assert!(
            rel_err < 0.01,
            "trunc_prec should approximate the input (got {out} for input {original}, rel err {rel_err})"
        );
    }

    #[test]
    fn test_trunc_prec_f64_preserves_sign_and_exponent() {
        let original: f64 = 1.3333333333333;
        let src = original.to_le_bytes();
        let mut dest = [0u8; 8];

        assert!(trunc_prec_forward(&src, &mut dest, 8, 20));

        let out = f64::from_le_bytes(dest);
        let out_bits = out.to_bits();
        let orig_bits = original.to_bits();
        // Sign and 11-bit exponent (top 12 bits) must be preserved.
        assert_eq!(
            out_bits & 0xFFF0_0000_0000_0000u64,
            orig_bits & 0xFFF0_0000_0000_0000u64,
            "trunc_prec must preserve sign+exponent for f64"
        );
    }
}
