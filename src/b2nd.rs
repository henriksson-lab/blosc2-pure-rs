//! Blosc2 N-dimensional array (b2nd) layer.
//!
//! A [`B2ndArray`] is a multidimensional array of fixed-size items backed by a
//! super-chunk ([`Schunk`]). It is described by three shapes:
//!
//! * `shape` — the logical extent of the array in items per dimension.
//! * `chunkshape` — the per-dimension extent of one compressed chunk. The
//!   array is tiled by chunks; each chunk maps to a [`Schunk`] entry.
//! * `blockshape` — the per-dimension extent of one block inside a chunk,
//!   which is also Blosc's compression unit.
//!
//! The shape/chunkshape/blockshape triple, the dtype string and the dtype
//! format are serialized into the `b2nd` fixed-size metalayer of the
//! super-chunk so that an array can be reconstructed from a frame on disk.

#![allow(
    clippy::manual_contains,
    clippy::needless_range_loop,
    clippy::too_many_arguments,
    clippy::type_complexity
)]

use crate::compress::{self, CParams, DParams};
use crate::constants::{
    BLOSC2_ERROR_DATA, BLOSC2_ERROR_FAILURE, BLOSC2_ERROR_FILE_OPEN, BLOSC2_ERROR_FILE_WRITE,
    BLOSC2_ERROR_INVALID_INDEX, BLOSC2_ERROR_INVALID_PARAM, BLOSC2_ERROR_MAX_BUFSIZE_EXCEEDED,
    BLOSC2_ERROR_METALAYER_NOT_FOUND, BLOSC2_ERROR_NULL_POINTER, BLOSC2_ERROR_SUCCESS,
    BLOSC2_ERROR_WRITE_BUFFER, BLOSC2_MAX_BUFFERSIZE, BLOSC2_MAX_METALAYERS, BLOSC2_SPECIAL_NAN,
    BLOSC2_SPECIAL_UNINIT, BLOSC2_SPECIAL_ZERO, BLOSC_BITSHUFFLE, BLOSC_CODEC_ZFP_FIXED_ACCURACY,
    BLOSC_CODEC_ZFP_FIXED_PRECISION, BLOSC_CODEC_ZFP_FIXED_RATE, BLOSC_SHUFFLE,
};
use crate::schunk::{blosc2_schunk_to_buffer, FrameStorage, Schunk};
use crate::utils::normalized_path;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// Name of the fixed-size metalayer that carries the b2nd shape descriptor.
pub const B2ND_METALAYER_NAME: &str = "b2nd";
/// Legacy Caterva metalayer name used by older b2nd frames.
const CATERVA_METALAYER_NAME: &str = "caterva";
/// Version of the b2nd metalayer format; must not exceed 127.
pub const B2ND_METALAYER_VERSION: u8 = 0;
/// Maximum number of dimensions supported by a b2nd array.
pub const B2ND_MAX_DIM: usize = 16;
/// Maximum number of user metalayers supported by a b2nd array.
pub const B2ND_MAX_METALAYERS: usize = BLOSC2_MAX_METALAYERS - 1;
/// `dtype_format` value indicating that the dtype string follows the
/// NumPy dtype convention.
pub const DTYPE_NUMPY_FORMAT: i8 = 0;
/// Default B2ND dtype used by C-Blosc2 when no dtype string is supplied.
pub const B2ND_DEFAULT_DTYPE: &str = "|u1";
/// Default B2ND dtype format used by C-Blosc2.
pub const B2ND_DEFAULT_DTYPE_FORMAT: i8 = DTYPE_NUMPY_FORMAT;

/// Lightweight B2ND creation context mirroring the shape of C-Blosc2's
/// `b2nd_context_t`.
#[derive(Clone, Debug)]
pub struct B2ndContext {
    pub meta: B2ndMeta,
    pub cparams: CParams,
    pub dparams: DParams,
    pub metalayers: Vec<(String, Vec<u8>)>,
    pub storage: Option<B2ndStorage>,
}

/// Rust model for the storage fields of C-Blosc2's `blosc2_storage`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct B2ndStorage {
    /// `true` writes/marks contiguous frames; `false` writes/marks sparse frames.
    pub contiguous: bool,
    /// Optional file or directory target for context-created arrays.
    pub urlpath: Option<PathBuf>,
}

impl B2ndStorage {
    /// In-memory storage preference without an attached `urlpath`.
    pub fn in_memory(contiguous: bool) -> Self {
        Self {
            contiguous,
            urlpath: None,
        }
    }

    /// File-backed contiguous frame target.
    pub fn contiguous_urlpath(path: impl Into<PathBuf>) -> Self {
        Self {
            contiguous: true,
            urlpath: Some(path.into()),
        }
    }

    /// File-backed sparse frame target.
    pub fn sparse_urlpath(path: impl Into<PathBuf>) -> Self {
        Self {
            contiguous: false,
            urlpath: Some(path.into()),
        }
    }
}

impl B2ndContext {
    fn metalayer_refs(&self) -> Vec<(&str, &[u8])> {
        self.metalayers
            .iter()
            .map(|(name, content)| (name.as_str(), content.as_slice()))
            .collect()
    }
}

/// Rust-friendly B2ND context constructor.
pub fn b2nd_create_ctx(
    meta: B2ndMeta,
    cparams: CParams,
    dparams: DParams,
    metalayers: Vec<(String, Vec<u8>)>,
) -> Result<B2ndContext, &'static str> {
    b2nd_create_ctx_impl(
        meta,
        cparams,
        dparams,
        metalayers,
        Some(B2ndStorage::default()),
    )
}

/// Rust-friendly B2ND context constructor with explicit storage behavior.
pub fn b2nd_create_ctx_with_storage(
    meta: B2ndMeta,
    cparams: CParams,
    dparams: DParams,
    metalayers: Vec<(String, Vec<u8>)>,
    storage: B2ndStorage,
) -> Result<B2ndContext, &'static str> {
    b2nd_create_ctx_impl(meta, cparams, dparams, metalayers, Some(storage))
}

fn b2nd_create_ctx_impl(
    meta: B2ndMeta,
    mut cparams: CParams,
    dparams: DParams,
    metalayers: Vec<(String, Vec<u8>)>,
    storage: Option<B2ndStorage>,
) -> Result<B2ndContext, &'static str> {
    meta.validate()?;
    let typesize = b2nd_cparams_typesize(&cparams)?;
    b2nd_validate_ctx_plugin_codecs(&cparams)?;
    cparams.blocksize = b2nd_c_context_blocksize(&meta.blockshape, typesize)?;
    if metalayers.len() >= BLOSC2_MAX_METALAYERS {
        return Err("Too many B2ND context metalayers");
    }
    Ok(B2ndContext {
        meta,
        cparams,
        dparams,
        metalayers,
        storage,
    })
}

fn b2nd_create_ctx_impl_c(
    mut meta: B2ndMeta,
    mut cparams: CParams,
    dparams: DParams,
    metalayers: Vec<(String, Vec<u8>)>,
    storage: B2ndStorage,
) -> Result<B2ndContext, &'static str> {
    let typesize = b2nd_cparams_typesize(&cparams)?;
    b2nd_validate_ctx_plugin_codecs(&cparams)?;
    if meta.dtype.is_empty() {
        meta.dtype = format!("|S{typesize}");
    }
    cparams.blocksize = b2nd_c_context_blocksize(&meta.blockshape, typesize)?;
    Ok(B2ndContext {
        meta,
        cparams,
        dparams,
        metalayers,
        storage: Some(storage),
    })
}

fn b2nd_create_ctx_parts_impl_c(
    shape: Vec<i64>,
    chunkshape: Vec<i32>,
    blockshape: Vec<i32>,
    dtype: Option<&str>,
    dtype_format: i8,
    mut cparams: CParams,
    dparams: DParams,
    metalayers: Vec<(String, Vec<u8>)>,
    storage: B2ndStorage,
) -> Result<B2ndContext, &'static str> {
    let typesize = b2nd_cparams_typesize(&cparams)?;
    b2nd_validate_ctx_plugin_codecs(&cparams)?;
    let meta = B2ndMeta {
        shape,
        chunkshape,
        blockshape,
        dtype: dtype
            .map(str::to_string)
            .unwrap_or_else(|| format!("|S{typesize}")),
        dtype_format,
    };
    cparams.blocksize = b2nd_c_context_blocksize(&meta.blockshape, typesize)?;
    Ok(B2ndContext {
        meta,
        cparams,
        dparams,
        metalayers,
        storage: Some(storage),
    })
}

fn b2nd_c_context_blocksize(blockshape: &[i32], typesize: usize) -> Result<i32, &'static str> {
    let typesize = i32::try_from(typesize).map_err(|_| "B2ND block too large")?;
    let block_nitems = blockshape.iter().try_fold(1i32, |acc, &dim| {
        acc.checked_mul(dim).ok_or("B2ND block too large")
    })?;
    block_nitems
        .checked_mul(typesize)
        .ok_or("B2ND block too large")
}

fn b2nd_validate_ctx_plugin_codecs(cparams: &CParams) -> Result<(), &'static str> {
    if matches!(
        cparams.compcode,
        BLOSC_CODEC_ZFP_FIXED_ACCURACY
            | BLOSC_CODEC_ZFP_FIXED_PRECISION
            | BLOSC_CODEC_ZFP_FIXED_RATE
    ) && cparams
        .filters
        .iter()
        .any(|&filter| matches!(filter, BLOSC_SHUFFLE | BLOSC_BITSHUFFLE))
    {
        return Err("ZFP cannot be run in presence of SHUFFLE / BITSHUFFLE");
    }
    Ok(())
}

fn b2nd_validate_cparams_for_array(cparams: &CParams) -> Result<(), &'static str> {
    b2nd_validate_ctx_plugin_codecs(cparams)
}

/// C-style status adapter for [`b2nd_create_ctx`].
pub fn b2nd_create_ctx_c(
    meta: B2ndMeta,
    cparams: CParams,
    dparams: DParams,
    metalayers: Vec<(String, Vec<u8>)>,
) -> (i32, Option<B2ndContext>) {
    match b2nd_create_ctx_impl_c(meta, cparams, dparams, metalayers, B2ndStorage::default()) {
        Ok(ctx) => (BLOSC2_ERROR_SUCCESS, Some(ctx)),
        Err(err) => (b2nd_array_error_code(err), None),
    }
}

/// C-style status adapter for [`b2nd_create_ctx_with_storage`].
pub fn b2nd_create_ctx_with_storage_c(
    meta: B2ndMeta,
    cparams: CParams,
    dparams: DParams,
    metalayers: Vec<(String, Vec<u8>)>,
    storage: B2ndStorage,
) -> (i32, Option<B2ndContext>) {
    match b2nd_create_ctx_impl_c(meta, cparams, dparams, metalayers, storage) {
        Ok(ctx) => (BLOSC2_ERROR_SUCCESS, Some(ctx)),
        Err(err) => (b2nd_array_error_code(err), None),
    }
}

/// C-style context constructor from shape parts.
///
/// When `dtype` is `None`, this mirrors C-Blosc2 context creation by using
/// the byte-string default dtype `"|S{typesize}"`.
pub fn b2nd_create_ctx_parts_c(
    shape: Vec<i64>,
    chunkshape: Vec<i32>,
    blockshape: Vec<i32>,
    dtype: Option<&str>,
    dtype_format: i8,
    cparams: CParams,
    dparams: DParams,
    metalayers: Vec<(String, Vec<u8>)>,
) -> (i32, Option<B2ndContext>) {
    match b2nd_create_ctx_parts_impl_c(
        shape,
        chunkshape,
        blockshape,
        dtype,
        dtype_format,
        cparams,
        dparams,
        metalayers,
        B2ndStorage::default(),
    ) {
        Ok(ctx) => (BLOSC2_ERROR_SUCCESS, Some(ctx)),
        Err(err) => (b2nd_array_error_code(err), None),
    }
}

/// C-style context constructor from shape parts with explicit storage behavior.
pub fn b2nd_create_ctx_parts_with_storage_c(
    shape: Vec<i64>,
    chunkshape: Vec<i32>,
    blockshape: Vec<i32>,
    dtype: Option<&str>,
    dtype_format: i8,
    cparams: CParams,
    dparams: DParams,
    metalayers: Vec<(String, Vec<u8>)>,
    storage: B2ndStorage,
) -> (i32, Option<B2ndContext>) {
    match b2nd_create_ctx_parts_impl_c(
        shape,
        chunkshape,
        blockshape,
        dtype,
        dtype_format,
        cparams,
        dparams,
        metalayers,
        storage,
    ) {
        Ok(ctx) => (BLOSC2_ERROR_SUCCESS, Some(ctx)),
        Err(err) => (b2nd_array_error_code(err), None),
    }
}

/// C-style B2ND context destructor.
pub fn b2nd_free_ctx_c(_ctx: Option<B2ndContext>) -> i32 {
    BLOSC2_ERROR_SUCCESS
}

/// C-name alias for [`B2ndMeta::serialize`].
pub fn b2nd_serialize_meta(meta: &B2ndMeta) -> Result<Vec<u8>, &'static str> {
    meta.serialize()
}

/// C-style metadata serializer for [`B2ndMeta::serialize`].
pub fn b2nd_serialize_meta_c(meta: &B2ndMeta, dest: &mut [u8]) -> i32 {
    if meta.dtype_format < 0 {
        return BLOSC2_ERROR_FAILURE;
    }
    match meta.serialize() {
        Ok(encoded) => {
            if dest.len() < encoded.len() {
                return BLOSC2_ERROR_WRITE_BUFFER;
            }
            dest[..encoded.len()].copy_from_slice(&encoded);
            i32::try_from(encoded.len()).unwrap_or(BLOSC2_ERROR_INVALID_PARAM)
        }
        Err(_) => BLOSC2_ERROR_INVALID_PARAM,
    }
}

/// C-name alias for [`B2ndMeta::deserialize`].
pub fn b2nd_deserialize_meta(data: &[u8]) -> Result<B2ndMeta, &'static str> {
    B2ndMeta::deserialize(data)
}

/// C-style metadata deserializer for [`B2ndMeta::deserialize`].
pub fn b2nd_deserialize_meta_c(data: &[u8]) -> (i32, Option<B2ndMeta>) {
    match B2ndMeta::deserialize_c(data) {
        Ok(decoded) => match i32::try_from(decoded.consumed) {
            Ok(consumed) => (consumed, Some(decoded.meta)),
            Err(_) => (BLOSC2_ERROR_INVALID_PARAM, None),
        },
        Err("Invalid B2ND ndim") => (BLOSC2_ERROR_FAILURE, None),
        Err(_) => (BLOSC2_ERROR_INVALID_PARAM, None),
    }
}

/// C-name-style metadata serializer from parts.
///
/// C `b2nd_serialize_meta` uses `"|u1"` when `dtype == NULL`; this differs
/// from context creation, where a missing dtype defaults to `"|S{typesize}"`.
pub fn b2nd_serialize_meta_parts(
    shape: Vec<i64>,
    chunkshape: Vec<i32>,
    blockshape: Vec<i32>,
    dtype: Option<&str>,
    dtype_format: i8,
) -> Result<Vec<u8>, &'static str> {
    B2ndMeta::new(
        shape,
        chunkshape,
        blockshape,
        dtype.unwrap_or("|u1"),
        dtype_format,
    )?
    .serialize()
}

fn b2nd_serialize_meta_parts_raw_c(
    shape: Vec<i64>,
    chunkshape: Vec<i32>,
    blockshape: Vec<i32>,
    dtype: &str,
    dtype_format: i8,
) -> Result<Vec<u8>, &'static str> {
    let dtype = dtype.as_bytes();
    if dtype.len() > i32::MAX as usize {
        return Err("B2ND dtype too large");
    }
    let ndim = shape.len();
    if chunkshape.len() < ndim || blockshape.len() < ndim {
        return Err("B2ND shape ranks differ");
    }
    let mut out = Vec::with_capacity(3 + 3 * (3 + ndim * 9) + 6 + dtype.len());
    out.push(0x90 + 7);
    out.push(B2ND_METALAYER_VERSION);
    out.push(ndim as u8);

    write_array_header(&mut out, ndim)?;
    for dim in shape {
        out.push(0xd3);
        out.extend_from_slice(&dim.to_be_bytes());
    }

    write_array_header(&mut out, ndim)?;
    for dim in chunkshape.into_iter().take(ndim) {
        out.push(0xd2);
        out.extend_from_slice(&dim.to_be_bytes());
    }

    write_array_header(&mut out, ndim)?;
    for dim in blockshape.into_iter().take(ndim) {
        out.push(0xd2);
        out.extend_from_slice(&dim.to_be_bytes());
    }

    out.push(dtype_format as u8);
    out.push(0xdb);
    out.extend_from_slice(&(dtype.len() as i32).to_be_bytes());
    out.extend_from_slice(dtype);
    Ok(out)
}

/// C-style metadata serializer from parts.
pub fn b2nd_serialize_meta_parts_c(
    shape: Vec<i64>,
    chunkshape: Vec<i32>,
    blockshape: Vec<i32>,
    dtype: Option<&str>,
    dtype_format: i8,
    dest: &mut [u8],
) -> i32 {
    if dtype_format < 0 {
        return BLOSC2_ERROR_FAILURE;
    }
    match b2nd_serialize_meta_parts_raw_c(
        shape,
        chunkshape,
        blockshape,
        dtype.unwrap_or(B2ND_DEFAULT_DTYPE),
        dtype_format,
    ) {
        Ok(encoded) => {
            if dest.len() < encoded.len() {
                return BLOSC2_ERROR_WRITE_BUFFER;
            }
            dest[..encoded.len()].copy_from_slice(&encoded);
            i32::try_from(encoded.len()).unwrap_or(BLOSC2_ERROR_INVALID_PARAM)
        }
        Err("B2ND dtype too large") => BLOSC2_ERROR_FAILURE,
        Err(_) => BLOSC2_ERROR_INVALID_PARAM,
    }
}

/// C-name alias for [`B2ndArray::uninit`].
pub fn b2nd_uninit(
    meta: B2ndMeta,
    cparams: CParams,
    dparams: DParams,
) -> Result<B2ndArray, &'static str> {
    B2ndArray::uninit(meta, cparams, dparams)
}

/// C-style status adapter for [`B2ndArray::uninit`].
pub fn b2nd_uninit_c(
    meta: B2ndMeta,
    cparams: CParams,
    dparams: DParams,
) -> (i32, Option<B2ndArray>) {
    b2nd_array_result_to_c(B2ndArray::uninit(meta, cparams, dparams))
}

/// C-style context adapter for [`B2ndArray::uninit`].
pub fn b2nd_uninit_ctx_c(ctx: &B2ndContext) -> (i32, Option<B2ndArray>) {
    let metalayers = ctx.metalayer_refs();
    b2nd_ctx_array_result_to_c(
        ctx,
        B2ndArray::uninit_with_metalayers(
            ctx.meta.clone(),
            ctx.cparams.clone(),
            ctx.dparams.clone(),
            &metalayers,
        ),
    )
}

/// C-name alias for [`B2ndArray::empty`].
pub fn b2nd_empty(
    meta: B2ndMeta,
    cparams: CParams,
    dparams: DParams,
) -> Result<B2ndArray, &'static str> {
    B2ndArray::empty(meta, cparams, dparams)
}

/// C-style status adapter for [`B2ndArray::empty`].
pub fn b2nd_empty_c(
    meta: B2ndMeta,
    cparams: CParams,
    dparams: DParams,
) -> (i32, Option<B2ndArray>) {
    b2nd_array_result_to_c(B2ndArray::empty(meta, cparams, dparams))
}

/// C-style context adapter for [`B2ndArray::empty`].
pub fn b2nd_empty_ctx_c(ctx: &B2ndContext) -> (i32, Option<B2ndArray>) {
    let metalayers = ctx.metalayer_refs();
    b2nd_ctx_array_result_to_c(
        ctx,
        B2ndArray::empty_with_metalayers(
            ctx.meta.clone(),
            ctx.cparams.clone(),
            ctx.dparams.clone(),
            &metalayers,
        ),
    )
}

/// C-name alias for [`B2ndArray::zeros`].
pub fn b2nd_zeros(
    meta: B2ndMeta,
    cparams: CParams,
    dparams: DParams,
) -> Result<B2ndArray, &'static str> {
    B2ndArray::zeros(meta, cparams, dparams)
}

/// C-style status adapter for [`B2ndArray::zeros`].
pub fn b2nd_zeros_c(
    meta: B2ndMeta,
    cparams: CParams,
    dparams: DParams,
) -> (i32, Option<B2ndArray>) {
    b2nd_array_result_to_c(B2ndArray::zeros(meta, cparams, dparams))
}

/// C-style context adapter for [`B2ndArray::zeros`].
pub fn b2nd_zeros_ctx_c(ctx: &B2ndContext) -> (i32, Option<B2ndArray>) {
    let metalayers = ctx.metalayer_refs();
    b2nd_ctx_array_result_to_c(
        ctx,
        B2ndArray::zeros_with_metalayers(
            ctx.meta.clone(),
            ctx.cparams.clone(),
            ctx.dparams.clone(),
            &metalayers,
        ),
    )
}

/// C-name alias for [`B2ndArray::nans`].
pub fn b2nd_nans(
    meta: B2ndMeta,
    cparams: CParams,
    dparams: DParams,
) -> Result<B2ndArray, &'static str> {
    B2ndArray::nans(meta, cparams, dparams)
}

/// C-style status adapter for [`B2ndArray::nans`].
pub fn b2nd_nans_c(meta: B2ndMeta, cparams: CParams, dparams: DParams) -> (i32, Option<B2ndArray>) {
    if let Err(err) = b2nd_special_size_preflight(&meta, &cparams) {
        return (b2nd_array_error_code(err), None);
    }
    let valid_nan_typesize = matches!(cparams.typesize, 4 | 8);
    let array = match B2ndArray::nans_unchecked_with_metalayers(meta, cparams, dparams, &[]) {
        Ok(array) => array,
        Err(err) => return (b2nd_array_error_code(err), None),
    };
    if !valid_nan_typesize {
        return (BLOSC2_ERROR_DATA, Some(array));
    }
    (BLOSC2_ERROR_SUCCESS, Some(array))
}

/// C-style context adapter for [`B2ndArray::nans`].
pub fn b2nd_nans_ctx_c(ctx: &B2ndContext) -> (i32, Option<B2ndArray>) {
    if let Err(err) = b2nd_special_size_preflight(&ctx.meta, &ctx.cparams) {
        return (b2nd_array_error_code(err), None);
    }
    let valid_nan_typesize = matches!(ctx.cparams.typesize, 4 | 8);
    let metalayers = ctx.metalayer_refs();
    let array = match B2ndArray::nans_unchecked_with_metalayers(
        ctx.meta.clone(),
        ctx.cparams.clone(),
        ctx.dparams.clone(),
        &metalayers,
    ) {
        Ok(array) => array,
        Err(err) => return (b2nd_array_error_code(err), None),
    };
    let array = match finish_ctx_array_storage(ctx, array) {
        Ok(array) => array,
        Err(err) => return (b2nd_ctx_storage_error_code(&err), None),
    };
    if !valid_nan_typesize {
        return (BLOSC2_ERROR_DATA, Some(array));
    }
    (BLOSC2_ERROR_SUCCESS, Some(array))
}

/// C-name alias for [`B2ndArray::full`].
pub fn b2nd_full(
    meta: B2ndMeta,
    value: &[u8],
    cparams: CParams,
    dparams: DParams,
) -> Result<B2ndArray, &'static str> {
    B2ndArray::full(meta, value, cparams, dparams)
}

/// C-style status adapter for [`B2ndArray::full`].
pub fn b2nd_full_c(
    meta: B2ndMeta,
    value: &[u8],
    value_size: i64,
    cparams: CParams,
    dparams: DParams,
) -> (i32, Option<B2ndArray>) {
    let value = match b2nd_checked_item_prefix(value, value_size, cparams.typesize) {
        Ok(value) => value,
        Err(code) => return (code, None),
    };
    b2nd_array_result_to_c(B2ndArray::full(meta, value, cparams, dparams))
}

/// C-style context adapter for [`B2ndArray::full`].
pub fn b2nd_full_ctx_c(
    ctx: &B2ndContext,
    value: &[u8],
    value_size: i64,
) -> (i32, Option<B2ndArray>) {
    let value = match b2nd_checked_item_prefix(value, value_size, ctx.cparams.typesize) {
        Ok(value) => value,
        Err(code) => return (code, None),
    };
    let metalayers = ctx.metalayer_refs();
    b2nd_ctx_array_result_to_c(
        ctx,
        B2ndArray::full_with_metalayers(
            ctx.meta.clone(),
            value,
            ctx.cparams.clone(),
            ctx.dparams.clone(),
            &metalayers,
        ),
    )
}

/// C-name alias for [`B2ndArray::from_dense_buffer`].
pub fn b2nd_from_cbuffer(
    meta: B2ndMeta,
    data: &[u8],
    cparams: CParams,
    dparams: DParams,
) -> Result<B2ndArray, &'static str> {
    B2ndArray::from_dense_buffer(meta, data, cparams, dparams)
}

/// C-style status adapter for [`B2ndArray::from_dense_buffer`].
pub fn b2nd_from_cbuffer_c(
    meta: B2ndMeta,
    data: &[u8],
    buffersize: i64,
    cparams: CParams,
    dparams: DParams,
) -> (i32, Option<B2ndArray>) {
    match B2ndArray::empty(meta, cparams, dparams) {
        Ok(array) => b2nd_fill_created_array_from_cbuffer_c(array, data, buffersize),
        Err(err) => (b2nd_array_error_code(err), None),
    }
}

/// C-style context adapter for [`B2ndArray::from_dense_buffer`].
pub fn b2nd_from_cbuffer_ctx_c(
    ctx: &B2ndContext,
    data: &[u8],
    buffersize: i64,
) -> (i32, Option<B2ndArray>) {
    let metalayers = ctx.metalayer_refs();
    let array = match B2ndArray::empty_with_metalayers(
        ctx.meta.clone(),
        ctx.cparams.clone(),
        ctx.dparams.clone(),
        &metalayers,
    ) {
        Ok(array) => array,
        Err(err) => return (b2nd_array_error_code(err), None),
    };
    match b2nd_fill_created_array_from_cbuffer_c(array, data, buffersize) {
        (BLOSC2_ERROR_SUCCESS, Some(array)) => b2nd_ctx_array_result_to_c(ctx, Ok(array)),
        result => result,
    }
}

fn b2nd_fill_created_array_from_cbuffer_c(
    mut array: B2ndArray,
    data: &[u8],
    buffersize: i64,
) -> (i32, Option<B2ndArray>) {
    let buffersize = match usize::try_from(buffersize) {
        Ok(buffersize) => buffersize,
        Err(_) => return (BLOSC2_ERROR_INVALID_PARAM, Some(array)),
    };
    let required = match array.preflight_dense_cbuffer_len() {
        Ok(required) => required,
        Err(err) => return (b2nd_array_error_code(err), Some(array)),
    };
    if buffersize < required {
        return (BLOSC2_ERROR_INVALID_PARAM, Some(array));
    }
    let data = match data.get(..buffersize) {
        Some(data) => data,
        None => return (BLOSC2_ERROR_INVALID_PARAM, Some(array)),
    };
    if array.meta.nitems().unwrap_or(usize::MAX) == 0 {
        return (BLOSC2_ERROR_SUCCESS, Some(array));
    }
    let ndim = array.meta.ndim();
    let start = vec![0i64; ndim];
    let stop = array.meta.shape.clone();
    let buffershape = array.meta.shape.clone();
    match array.set_slice_from_dense_buffer(&start, &stop, &buffershape, data) {
        Ok(()) => (BLOSC2_ERROR_SUCCESS, Some(array)),
        Err(err) => (b2nd_selection_error_code(err), Some(array)),
    }
}

/// C-name alias for [`B2ndArray::from_schunk`].
pub fn b2nd_from_schunk(schunk: Schunk) -> Result<B2ndArray, &'static str> {
    B2ndArray::from_schunk(schunk)
}

/// C-style status adapter for [`B2ndArray::from_schunk`].
pub fn b2nd_from_schunk_c(schunk: Schunk) -> (i32, Option<B2ndArray>) {
    match B2ndArray::from_schunk(schunk) {
        Ok(array) => (BLOSC2_ERROR_SUCCESS, Some(array)),
        Err("Schunk does not contain a B2ND metalayer")
        | Err("Missing b2nd metalayer")
        | Err("Missing caterva metalayer") => (BLOSC2_ERROR_METALAYER_NOT_FOUND, None),
        Err("Invalid B2ND ndim") => (BLOSC2_ERROR_FAILURE, None),
        Err(_) => (BLOSC2_ERROR_INVALID_PARAM, None),
    }
}

/// C-name alias for [`B2ndArray::from_contiguous_frame`].
pub fn b2nd_from_cframe(frame: &[u8], copy: bool) -> Result<B2ndArray, String> {
    if !copy {
        return Err("copy=false requires owned frame buffer".into());
    }
    B2ndArray::from_contiguous_frame(frame)
}

/// C-style status adapter for [`B2ndArray::from_contiguous_frame`].
pub fn b2nd_from_cframe_c(frame: &[u8], cframe_len: i64, copy: bool) -> (i32, Option<B2ndArray>) {
    let frame = match b2nd_checked_cbuffer_prefix(frame, cframe_len) {
        Ok(frame) => frame,
        Err(_) => return (BLOSC2_ERROR_FAILURE, None),
    };
    match b2nd_from_cframe(frame, copy) {
        Ok(array) => (BLOSC2_ERROR_SUCCESS, Some(array)),
        Err(err) => (b2nd_frame_error_code(&err), None),
    }
}

/// C-name alias for [`B2ndArray::to_contiguous_frame`].
pub fn b2nd_to_cframe(array: &B2ndArray) -> Vec<u8> {
    array.to_contiguous_frame()
}

/// C-style frame writer for [`B2ndArray::to_contiguous_frame`].
///
/// The returned `needs_free` flag mirrors `blosc2_schunk_to_buffer`, including
/// `false` for a borrowed in-memory contiguous frame. This Rust adapter still
/// returns a `Vec<u8>` so callers cannot observe a dangling borrowed pointer.
pub fn b2nd_to_cframe_c(array: &B2ndArray) -> (i32, Option<Vec<u8>>, i64, bool) {
    let (len, frame, needs_free) = blosc2_schunk_to_buffer(&array.schunk);
    if len < 0 {
        return (len as i32, None, 0, false);
    }
    let Some(frame) = frame else {
        return (BLOSC2_ERROR_INVALID_PARAM, None, 0, false);
    };
    (
        BLOSC2_ERROR_SUCCESS,
        Some(frame.into_owned()),
        len,
        needs_free,
    )
}

/// Rust ownership drops arrays automatically; this consumes the value for C API parity.
pub fn b2nd_free_c(_array: B2ndArray) -> i32 {
    BLOSC2_ERROR_SUCCESS
}

/// Nullable lifecycle adapter for C API parity.
pub fn b2nd_free_option_c(array: Option<B2ndArray>) -> i32 {
    if array.is_none() {
        return BLOSC2_ERROR_NULL_POINTER;
    }
    BLOSC2_ERROR_SUCCESS
}

/// C-name alias for [`B2ndArray::open`].
pub fn b2nd_open(path: impl AsRef<Path>) -> Result<B2ndArray, String> {
    B2ndArray::open(path)
}

/// C-style status adapter for [`B2ndArray::open`].
pub fn b2nd_open_c(path: impl AsRef<Path>) -> (i32, Option<B2ndArray>) {
    match B2ndArray::open(path) {
        Ok(array) => (BLOSC2_ERROR_SUCCESS, Some(array)),
        Err(err) => (b2nd_open_error_code(&err), None),
    }
}

/// C-name alias for [`B2ndArray::open_frame_at`].
pub fn b2nd_open_offset(path: impl AsRef<Path>, offset: i64) -> Result<B2ndArray, String> {
    if offset < 0 {
        return Err("Invalid frame offset".into());
    }
    B2ndArray::open_frame_at(path, offset as u64)
}

/// C-style status adapter for [`B2ndArray::open_frame_at`].
pub fn b2nd_open_offset_c(path: impl AsRef<Path>, offset: i64) -> (i32, Option<B2ndArray>) {
    let offset = match u64::try_from(offset) {
        Ok(offset) => offset,
        Err(_) => return (BLOSC2_ERROR_NULL_POINTER, None),
    };
    match B2ndArray::open_frame_at(path, offset) {
        Ok(array) => (BLOSC2_ERROR_SUCCESS, Some(array)),
        Err(err) => (b2nd_open_error_code(&err), None),
    }
}

/// C-name alias for [`B2ndArray::save`].
pub fn b2nd_save(array: &B2ndArray, path: impl AsRef<Path>) -> i32 {
    array
        .save(path)
        .map(|()| BLOSC2_ERROR_SUCCESS)
        .unwrap_or_else(|err| b2nd_file_write_error_code(&err))
}

/// C-name alias for [`B2ndArray::save_append`].
pub fn b2nd_save_append(array: &B2ndArray, path: impl AsRef<Path>) -> i64 {
    match array.save_append(path) {
        Ok(offset) => i64::try_from(offset).unwrap_or(i64::from(BLOSC2_ERROR_FILE_WRITE)),
        Err(err) => i64::from(b2nd_file_write_error_code(&err)),
    }
}

fn b2nd_file_write_error_code(err: &std::io::Error) -> i32 {
    match err.kind() {
        std::io::ErrorKind::NotFound | std::io::ErrorKind::PermissionDenied => {
            BLOSC2_ERROR_FILE_OPEN
        }
        _ => BLOSC2_ERROR_FILE_WRITE,
    }
}

/// C-name alias for [`B2ndArray::to_dense_buffer`].
pub fn b2nd_to_cbuffer_vec(array: &B2ndArray) -> Result<Vec<u8>, &'static str> {
    array.to_dense_buffer()
}

/// C-style dense buffer writer for [`B2ndArray::to_dense_buffer`].
pub fn b2nd_to_cbuffer(array: &B2ndArray, dest: &mut [u8]) -> i32 {
    let required = match array.preflight_dense_cbuffer_len() {
        Ok(required) => required,
        Err(_) => return BLOSC2_ERROR_INVALID_PARAM,
    };
    if required == 0 {
        return BLOSC2_ERROR_SUCCESS;
    }
    if dest.len() < required {
        return BLOSC2_ERROR_INVALID_PARAM;
    }
    match array.to_dense_buffer() {
        Ok(buffer) => {
            dest[..buffer.len()].copy_from_slice(&buffer);
            BLOSC2_ERROR_SUCCESS
        }
        Err(_) => BLOSC2_ERROR_INVALID_PARAM,
    }
}

/// C-style dense buffer writer with an explicit destination size.
pub fn b2nd_to_cbuffer_c(array: &B2ndArray, dest: &mut [u8], buffersize: i64) -> i32 {
    let buffersize = match b2nd_checked_dest_len(dest.len(), buffersize) {
        Ok(buffersize) => buffersize,
        Err(code) => return code,
    };
    let required = match array.preflight_dense_cbuffer_len() {
        Ok(required) => required,
        Err(_) => return BLOSC2_ERROR_INVALID_PARAM,
    };
    if required == 0 {
        return BLOSC2_ERROR_SUCCESS;
    }
    if buffersize < required {
        return BLOSC2_ERROR_INVALID_PARAM;
    }
    match array.to_dense_buffer() {
        Ok(buffer) => {
            dest[..buffersize].fill(0);
            dest[..buffer.len()].copy_from_slice(&buffer);
            BLOSC2_ERROR_SUCCESS
        }
        Err(_) => BLOSC2_ERROR_INVALID_PARAM,
    }
}

/// C-name alias for [`B2ndArray::format_meta`].
pub fn b2nd_print_meta(array: &B2ndArray) -> String {
    array.format_meta()
}

struct B2ndPrintableMeta {
    meta: B2ndMeta,
    dtype_present: bool,
}

fn b2nd_meta_from_print_metalayer(array: &B2ndArray) -> Result<B2ndPrintableMeta, i32> {
    let typesize =
        b2nd_cparams_typesize(&array.schunk.cparams).map_err(|_| BLOSC2_ERROR_INVALID_PARAM)?;
    if let Some(content) = array.schunk.metalayer(B2ND_METALAYER_NAME) {
        return B2ndMeta::deserialize_legacy_optional_dtype_with_info(content, typesize)
            .map(|decoded| B2ndPrintableMeta {
                meta: decoded.meta,
                dtype_present: decoded.dtype_present,
            })
            .map_err(|_| BLOSC2_ERROR_INVALID_PARAM);
    }
    if let Some(content) = array.schunk.metalayer(CATERVA_METALAYER_NAME) {
        return B2ndMeta::deserialize_caterva_with_info(content, typesize)
            .map(|decoded| B2ndPrintableMeta {
                meta: decoded.meta,
                dtype_present: decoded.dtype_present,
            })
            .map_err(|_| BLOSC2_ERROR_INVALID_PARAM);
    }
    Err(BLOSC2_ERROR_METALAYER_NOT_FOUND)
}

fn b2nd_format_meta_c(printable: &B2ndPrintableMeta) -> String {
    let meta = &printable.meta;
    let shape = b2nd_format_i64_dims_c(&meta.shape);
    let chunkshape = b2nd_format_i32_dims_c(&meta.chunkshape);
    let blockshape = b2nd_format_i32_dims_c(&meta.blockshape);
    let mut out = format!(
        "b2nd metalayer parameters:\n Ndim:       {}\n shape:      {}\n chunkshape: {}\n",
        meta.ndim(),
        shape,
        chunkshape
    );
    if printable.dtype_present {
        out.push_str(&format!(" dtype: {}\n", meta.dtype));
    }
    out.push_str(&format!(" blockshape: {}\n", blockshape));
    out
}

fn b2nd_format_i64_dims_c(dims: &[i64]) -> String {
    if dims.is_empty() {
        return "1".to_string();
    }
    dims.iter()
        .map(i64::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

fn b2nd_format_i32_dims_c(dims: &[i32]) -> String {
    if dims.is_empty() {
        return "1".to_string();
    }
    dims.iter()
        .map(i32::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

/// C-style status adapter for printing B2ND metadata.
pub fn b2nd_print_meta_c(array: &B2ndArray) -> i32 {
    match b2nd_meta_from_print_metalayer(array) {
        Ok(meta) => {
            print!("{}", b2nd_format_meta_c(&meta));
            BLOSC2_ERROR_SUCCESS
        }
        Err(code) => code,
    }
}

/// C-style metadata formatter with an explicit destination size.
pub fn b2nd_print_meta_to_buffer_c(array: &B2ndArray, dest: &mut [u8], buffersize: i64) -> i32 {
    let buffersize = match b2nd_checked_dest_len(dest.len(), buffersize) {
        Ok(buffersize) => buffersize,
        Err(code) => return code,
    };
    let meta = match b2nd_meta_from_print_metalayer(array) {
        Ok(meta) => meta,
        Err(code) => return code,
    };
    let formatted = b2nd_format_meta_c(&meta);
    let bytes = formatted.as_bytes();
    if buffersize < bytes.len() {
        return BLOSC2_ERROR_WRITE_BUFFER;
    }
    dest[..bytes.len()].copy_from_slice(bytes);
    i32::try_from(bytes.len()).unwrap_or(BLOSC2_ERROR_INVALID_PARAM)
}

/// C-name alias for [`B2ndArray::expand_dims_view`].
pub fn b2nd_expand_dims(array: &B2ndArray, axes: &[bool]) -> Result<B2ndArray, &'static str> {
    b2nd_expand_dims_view_c(array, axes)
}

/// C-style status adapter for [`B2ndArray::expand_dims_view`].
pub fn b2nd_expand_dims_c(array: &B2ndArray, axes: &[bool]) -> (i32, Option<B2ndArray>) {
    b2nd_array_result_to_c(b2nd_expand_dims_view_c(array, axes))
}

/// C-style status adapter for [`B2ndArray::expand_dims_view`] with explicit final ndim.
pub fn b2nd_expand_dims_final_c(
    array: &B2ndArray,
    axes: &[bool],
    final_dims: i32,
) -> (i32, Option<B2ndArray>) {
    let final_dims = match usize::try_from(final_dims) {
        Ok(final_dims) if final_dims <= axes.len() => final_dims,
        _ => return (BLOSC2_ERROR_INVALID_PARAM, None),
    };
    if axes[..final_dims].iter().filter(|&&axis| !axis).count() > array.meta.ndim() {
        return (BLOSC2_ERROR_INVALID_PARAM, None);
    }
    b2nd_array_result_to_c(b2nd_expand_dims_view_c(array, &axes[..final_dims]))
}

/// C-name alias for [`B2ndArray::squeeze_view`].
pub fn b2nd_squeeze(array: &B2ndArray) -> Result<B2ndArray, &'static str> {
    array.squeeze_view()
}

/// C-style status adapter for [`B2ndArray::squeeze_view`].
pub fn b2nd_squeeze_c(array: &B2ndArray) -> (i32, Option<B2ndArray>) {
    b2nd_array_result_to_c(array.squeeze_view())
}

/// C-name alias for [`B2ndArray::squeeze_index_view`].
pub fn b2nd_squeeze_index(array: &B2ndArray, axes: &[bool]) -> Result<B2ndArray, &'static str> {
    b2nd_squeeze_index_view_c(array, axes)
}

/// C-style status adapter for [`B2ndArray::squeeze_index_view`].
pub fn b2nd_squeeze_index_c(array: &B2ndArray, axes: &[bool]) -> (i32, Option<B2ndArray>) {
    match b2nd_squeeze_index_view_c(array, axes) {
        Ok(array) => (BLOSC2_ERROR_SUCCESS, Some(array)),
        Err(err) => (b2nd_squeeze_error_code(err), None),
    }
}

/// C-name alias for [`B2ndArray::copy_with_meta`].
pub fn b2nd_copy(
    array: &B2ndArray,
    meta: B2ndMeta,
    cparams: CParams,
    dparams: DParams,
) -> Result<B2ndArray, &'static str> {
    array.copy_with_meta(meta, cparams, dparams)
}

/// C-style status adapter for [`B2ndArray::copy_with_meta`].
pub fn b2nd_copy_c(
    array: &B2ndArray,
    meta: B2ndMeta,
    cparams: CParams,
    dparams: DParams,
) -> (i32, Option<B2ndArray>) {
    b2nd_array_result_to_c(array.copy_with_meta(meta, cparams, dparams))
}

/// C-style context adapter for [`B2ndArray::copy_with_meta`].
pub fn b2nd_copy_ctx_c(ctx: &mut B2ndContext, array: &B2ndArray) -> (i32, Option<B2ndArray>) {
    ctx.meta.shape = array.meta.shape.clone();
    b2nd_ctx_array_result_to_c(
        ctx,
        array.copy_with_meta(ctx.meta.clone(), ctx.cparams.clone(), ctx.dparams.clone()),
    )
}

/// C-name alias for [`B2ndArray::concatenate_with_meta`] and
/// [`B2ndArray::concatenate_in_place`].
pub fn b2nd_concatenate(
    array: &mut B2ndArray,
    other: &B2ndArray,
    axis: usize,
    copy: bool,
    meta: B2ndMeta,
    cparams: CParams,
    dparams: DParams,
) -> Result<Option<B2ndArray>, &'static str> {
    if copy {
        array
            .concatenate_with_meta(other, axis, meta, cparams, dparams)
            .map(Some)
    } else {
        array
            .concatenate_in_place(other, axis)
            .map(|()| Some(array.clone()))
    }
}

/// C-style status adapter for [`b2nd_concatenate`].
pub fn b2nd_concatenate_c(
    array: &mut B2ndArray,
    other: &B2ndArray,
    axis: usize,
    copy: bool,
    meta: B2ndMeta,
    cparams: CParams,
    dparams: DParams,
) -> (i32, Option<B2ndArray>) {
    if !copy {
        return match array.concatenate_in_place(other, axis) {
            Ok(()) => (BLOSC2_ERROR_SUCCESS, Some(array.clone())),
            Err(_) => (BLOSC2_ERROR_INVALID_PARAM, None),
        };
    }
    match b2nd_concatenate(array, other, axis, copy, meta, cparams, dparams) {
        Ok(array) => (BLOSC2_ERROR_SUCCESS, array),
        Err(_) => (BLOSC2_ERROR_INVALID_PARAM, None),
    }
}

/// Signed-axis C-style status adapter for [`b2nd_concatenate`].
pub fn b2nd_concatenate_axis_c(
    array: &mut B2ndArray,
    other: &B2ndArray,
    axis: i8,
    copy: bool,
    meta: B2ndMeta,
    cparams: CParams,
    dparams: DParams,
) -> (i32, Option<B2ndArray>) {
    let axis = match usize::try_from(axis) {
        Ok(axis) => axis,
        Err(_) => return (BLOSC2_ERROR_INVALID_PARAM, None),
    };
    b2nd_concatenate_c(array, other, axis, copy, meta, cparams, dparams)
}

/// C-style context adapter for [`b2nd_concatenate`].
pub fn b2nd_concatenate_ctx_c(
    ctx: &mut B2ndContext,
    array: &mut B2ndArray,
    other: &B2ndArray,
    axis: usize,
    copy: bool,
) -> (i32, Option<B2ndArray>) {
    if !copy {
        return match array.concatenate_in_place(other, axis) {
            Ok(()) => (BLOSC2_ERROR_SUCCESS, Some(array.clone())),
            Err(_) => (BLOSC2_ERROR_INVALID_PARAM, None),
        };
    }
    let shape = match array.concatenated_shape(other, axis) {
        Ok(shape) => shape,
        Err(_) => return (BLOSC2_ERROR_INVALID_PARAM, None),
    };
    ctx.meta.shape = array.meta.shape.clone();
    let mut meta = ctx.meta.clone();
    meta.shape = shape;
    b2nd_ctx_array_result_to_c(
        ctx,
        array.concatenate_with_meta(other, axis, meta, ctx.cparams.clone(), ctx.dparams.clone()),
    )
}

/// Signed-axis C-style context adapter for [`b2nd_concatenate`].
pub fn b2nd_concatenate_ctx_axis_c(
    ctx: &mut B2ndContext,
    array: &mut B2ndArray,
    other: &B2ndArray,
    axis: i8,
    copy: bool,
) -> (i32, Option<B2ndArray>) {
    let axis = match usize::try_from(axis) {
        Ok(axis) => axis,
        Err(_) => return (BLOSC2_ERROR_INVALID_PARAM, None),
    };
    b2nd_concatenate_ctx_c(ctx, array, other, axis, copy)
}

/// C-name alias for [`B2ndArray::slice_with_meta`].
pub fn b2nd_get_slice(
    array: &B2ndArray,
    start: &[i64],
    stop: &[i64],
    meta: B2ndMeta,
    cparams: CParams,
    dparams: DParams,
) -> Result<B2ndArray, &'static str> {
    b2nd_get_slice_array_with_meta_c(array, start, stop, meta, cparams, dparams, &[])
}

/// C-style status adapter for [`B2ndArray::slice_with_meta`].
pub fn b2nd_get_slice_c(
    array: &B2ndArray,
    start: &[i64],
    stop: &[i64],
    meta: B2ndMeta,
    cparams: CParams,
    dparams: DParams,
) -> (i32, Option<B2ndArray>) {
    b2nd_array_result_to_c(b2nd_get_slice_array_with_meta_c(
        array,
        start,
        stop,
        meta,
        cparams,
        dparams,
        &[],
    ))
}

/// C-style context adapter for [`B2ndArray::slice_with_meta`].
pub fn b2nd_get_slice_ctx_c(
    ctx: &mut B2ndContext,
    array: &B2ndArray,
    start: &[i64],
    stop: &[i64],
) -> (i32, Option<B2ndArray>) {
    let extents_as_i64 = match slice_extents_without_source_bounds(&array.meta, start, stop) {
        Ok(extents_as_i64) => extents_as_i64,
        Err(_) => return (BLOSC2_ERROR_INVALID_PARAM, None),
    };
    ctx.meta.shape = extents_as_i64.clone();
    if product_i64(&extents_as_i64) == Ok(0) {
        return b2nd_empty_ctx_c(ctx);
    }
    match validate_slice_bounds(&array.meta, start, stop) {
        Ok(_) => {}
        Err(_) => return (BLOSC2_ERROR_INVALID_PARAM, None),
    }
    let metalayers = ctx.metalayer_refs();
    b2nd_ctx_array_result_to_c(
        ctx,
        array.slice_with_meta_and_metalayers(
            start,
            stop,
            ctx.meta.clone(),
            ctx.cparams.clone(),
            ctx.dparams.clone(),
            &metalayers,
        ),
    )
}

/// C-name alias for [`B2ndArray::slice_to_dense_buffer`].
pub fn b2nd_get_slice_cbuffer_vec(
    array: &B2ndArray,
    start: &[i64],
    stop: &[i64],
    buffershape: &[i64],
) -> Result<Vec<u8>, &'static str> {
    array.slice_to_dense_buffer(start, stop, buffershape)
}

/// C-style slice writer for [`B2ndArray::slice_to_dense_buffer`].
pub fn b2nd_get_slice_cbuffer(
    array: &B2ndArray,
    start: &[i64],
    stop: &[i64],
    dest: &mut [u8],
    buffershape: &[i64],
) -> i32 {
    let extents_as_i64 = match slice_extents_without_source_bounds(&array.meta, start, stop) {
        Ok(extents_as_i64) => extents_as_i64,
        Err(err) => return b2nd_selection_error_code(err),
    };
    let required = match array.preflight_slice_cbuffer_len_c(start, stop, buffershape) {
        Ok(required) => required,
        Err(err) => return b2nd_selection_error_code(err),
    };
    if dest.len() < required {
        return BLOSC2_ERROR_INVALID_PARAM;
    }
    if array.meta.nitems().unwrap_or(usize::MAX) == 0
        || extents_as_i64.iter().any(|&extent| extent == 0)
    {
        dest.fill(0);
        return BLOSC2_ERROR_SUCCESS;
    }
    match array.slice_to_dense_buffer(start, stop, buffershape) {
        Ok(buffer) => {
            dest[..buffer.len()].copy_from_slice(&buffer);
            BLOSC2_ERROR_SUCCESS
        }
        Err(err) => b2nd_selection_error_code(err),
    }
}

/// C-style slice writer with an explicit destination size.
pub fn b2nd_get_slice_cbuffer_c(
    array: &B2ndArray,
    start: &[i64],
    stop: &[i64],
    dest: &mut [u8],
    buffershape: &[i64],
    buffersize: i64,
) -> i32 {
    let buffersize = match b2nd_checked_dest_len(dest.len(), buffersize) {
        Ok(buffersize) => buffersize,
        Err(code) => return code,
    };
    let result = (|| {
        let typesize = b2nd_cparams_typesize(&array.schunk.cparams)?;
        let extents_as_i64 = slice_extents_without_source_bounds(&array.meta, start, stop)?;
        validate_slice_buffershape(&extents_as_i64, buffershape)?;
        if array.meta.nitems()? == 0 || extents_as_i64.iter().any(|&extent| extent == 0) {
            return Ok(None);
        }
        let slice = validate_slice_bounds(&array.meta, start, stop)?;
        let required_len = dense_region_required_len(buffershape, &slice.extents, typesize)?;
        if buffersize < required_len {
            return Err("Invalid B2ND destination buffer size");
        }
        Ok(Some((slice, typesize)))
    })();
    let (slice, typesize) = match result {
        Ok(Some(validated)) => validated,
        Ok(None) => {
            dest[..buffersize].fill(0);
            return BLOSC2_ERROR_SUCCESS;
        }
        Err(err) => return b2nd_selection_error_code(err),
    };
    let result = (|| {
        dest[..buffersize].fill(0);
        let coords: Vec<Vec<usize>> = slice
            .starts
            .iter()
            .zip(&slice.extents)
            .map(|(&start, &extent)| (start..start + extent).collect())
            .collect();
        array.read_orthogonal_selection_chunks(
            &coords,
            &slice.extents,
            buffershape,
            &mut dest[..buffersize],
            typesize,
        )
    })();
    match result {
        Ok(()) => BLOSC2_ERROR_SUCCESS,
        Err(err) => b2nd_selection_error_code(err),
    }
}

/// C-name alias for [`B2ndArray::get_slice_nchunks`].
pub fn b2nd_get_slice_nchunks_vec(
    array: &B2ndArray,
    start: &[i64],
    stop: &[i64],
) -> Result<Vec<i64>, &'static str> {
    array.get_slice_nchunks(start, stop)
}

/// C-style chunk-index query for [`B2ndArray::get_slice_nchunks`].
pub fn b2nd_get_slice_nchunks(
    array: &B2ndArray,
    start: &[i64],
    stop: &[i64],
) -> (i32, Option<Vec<i64>>) {
    match array.get_slice_nchunks(start, stop) {
        Ok(chunks) if chunks.is_empty() => (0, None),
        Ok(chunks) => (
            i32::try_from(chunks.len()).unwrap_or(BLOSC2_ERROR_INVALID_PARAM),
            Some(chunks),
        ),
        Err(_) => (BLOSC2_ERROR_INVALID_PARAM, None),
    }
}

/// C-name alias for [`B2ndArray::set_slice_from_dense_buffer`].
pub fn b2nd_set_slice_cbuffer(
    data: &[u8],
    buffershape: &[i64],
    start: &[i64],
    stop: &[i64],
    array: &mut B2ndArray,
) -> i32 {
    let extents_as_i64 = match slice_extents_without_source_bounds(&array.meta, start, stop) {
        Ok(extents_as_i64) => extents_as_i64,
        Err(_) => return BLOSC2_ERROR_INVALID_PARAM,
    };
    if validate_slice_buffershape(&extents_as_i64, buffershape).is_err() {
        return BLOSC2_ERROR_INVALID_PARAM;
    }
    if array.meta.nitems().unwrap_or(usize::MAX) == 0
        || extents_as_i64.iter().any(|&extent| extent == 0)
    {
        return BLOSC2_ERROR_SUCCESS;
    }
    match array.set_slice_from_dense_buffer(start, stop, buffershape, data) {
        Ok(()) => BLOSC2_ERROR_SUCCESS,
        Err(_) => BLOSC2_ERROR_INVALID_PARAM,
    }
}

/// C-style slice setter with an explicit source size.
pub fn b2nd_set_slice_cbuffer_c(
    data: &[u8],
    buffersize: i64,
    buffershape: &[i64],
    start: &[i64],
    stop: &[i64],
    array: &mut B2ndArray,
) -> i32 {
    let data = match b2nd_checked_cbuffer_prefix(data, buffersize) {
        Ok(data) => data,
        Err(code) => return code,
    };
    if array.meta.nitems().unwrap_or(usize::MAX) == 0 {
        return BLOSC2_ERROR_SUCCESS;
    }
    let extents_as_i64 = match slice_extents_without_source_bounds(&array.meta, start, stop) {
        Ok(extents_as_i64) => extents_as_i64,
        Err(_) => return BLOSC2_ERROR_INVALID_PARAM,
    };
    if validate_slice_buffershape(&extents_as_i64, buffershape).is_err() {
        return BLOSC2_ERROR_INVALID_PARAM;
    }
    if extents_as_i64.iter().any(|&extent| extent == 0) {
        return BLOSC2_ERROR_SUCCESS;
    }
    b2nd_set_slice_cbuffer(data, buffershape, start, stop, array)
}

/// C-name alias for [`B2ndArray::select_orthogonal`].
pub fn b2nd_get_orthogonal_selection(
    array: &B2ndArray,
    selection: &[Vec<i64>],
) -> Result<Vec<u8>, &'static str> {
    array.select_orthogonal(selection)
}

/// C-name alias for [`B2ndArray::orthogonal_selection_to_dense_buffer`].
pub fn b2nd_get_orthogonal_selection_cbuffer(
    array: &B2ndArray,
    selection: &[Vec<i64>],
    buffershape: &[i64],
) -> Result<Vec<u8>, &'static str> {
    array.orthogonal_selection_to_dense_buffer(selection, buffershape)
}

/// C-style orthogonal selection writer with an explicit destination size.
pub fn b2nd_get_orthogonal_selection_cbuffer_c(
    array: &B2ndArray,
    selection: &[Vec<i64>],
    dest: &mut [u8],
    buffershape: &[i64],
    buffersize: i64,
) -> i32 {
    let buffersize = match b2nd_checked_dest_len(dest.len(), buffersize) {
        Ok(buffersize) => buffersize,
        Err(code) => return code,
    };
    match array.get_orthogonal_selection_cbuffer_c_into(selection, buffershape, dest, buffersize) {
        Ok(()) => BLOSC2_ERROR_SUCCESS,
        Err(err) => b2nd_selection_error_code(err),
    }
}

/// C-shaped orthogonal selection writer with per-dimension selection sizes.
///
/// `selection` is one coordinate slice per dimension, and `selection_size`
/// says how many coordinates to consume from each dimension.
pub fn b2nd_get_orthogonal_selection_c_sizes_c(
    array: &B2ndArray,
    selection: &[&[i64]],
    selection_size: &[i64],
    dest: &mut [u8],
    buffershape: &[i64],
    buffersize: i64,
) -> i32 {
    let selection = match b2nd_c_shaped_selection(array.meta.ndim(), selection, selection_size) {
        Ok(selection) => selection,
        Err(code) => return code,
    };
    b2nd_get_orthogonal_selection_cbuffer_c(array, &selection, dest, buffershape, buffersize)
}

impl B2ndArray {
    fn get_orthogonal_selection_cbuffer_c_into(
        &self,
        selection: &[Vec<i64>],
        buffershape: &[i64],
        dest: &mut [u8],
        buffersize: usize,
    ) -> Result<(), &'static str> {
        let (coords, extents, _) = validate_orthogonal_selection_c(&self.meta, selection)?;
        validate_orthogonal_buffershape_c(buffershape, &extents)?;
        let typesize = b2nd_cparams_typesize(&self.schunk.cparams)?;
        let selection_nbytes = product_usize(&extents)?
            .checked_mul(typesize)
            .ok_or("B2ND selection too large")?;
        if buffersize > selection_nbytes {
            return Err("B2ND selection buffer size does not match selection shape and typesize");
        }
        if extents.iter().any(|&extent| extent == 0) {
            return Ok(());
        }
        let required_len = dense_region_required_len(buffershape, &extents, typesize)?;
        let dest = dest
            .get_mut(..required_len)
            .ok_or("B2ND destination too small")?;
        self.read_orthogonal_selection_chunks(&coords, &extents, buffershape, dest, typesize)
    }
}

/// C-style orthogonal selection writer with per-dimension selection sizes.
pub fn b2nd_get_orthogonal_selection_c(
    array: &B2ndArray,
    selection: &[&[i64]],
    selection_size: &[i64],
    dest: &mut [u8],
    buffershape: &[i64],
    buffersize: i64,
) -> i32 {
    b2nd_get_orthogonal_selection_c_sizes_c(
        array,
        selection,
        selection_size,
        dest,
        buffershape,
        buffersize,
    )
}

/// Rust-shaped compatibility adapter with an explicit number of dimensions to consume.
pub fn b2nd_get_orthogonal_selection_count_c(
    array: &B2ndArray,
    selection: &[Vec<i64>],
    selection_size: i32,
    dest: &mut [u8],
    buffershape: &[i64],
    buffersize: i64,
) -> i32 {
    let selection_size = match usize::try_from(selection_size) {
        Ok(selection_size) if selection_size <= selection.len() => selection_size,
        _ => return BLOSC2_ERROR_INVALID_PARAM,
    };
    b2nd_get_orthogonal_selection_cbuffer_c(
        array,
        &selection[..selection_size],
        dest,
        buffershape,
        buffersize,
    )
}

/// C-name alias for [`B2ndArray::set_orthogonal_selection`].
pub fn b2nd_set_orthogonal_selection(
    array: &mut B2ndArray,
    selection: &[Vec<i64>],
    data: &[u8],
) -> Result<(), &'static str> {
    array.set_orthogonal_selection(selection, data)
}

/// C-name alias for [`B2ndArray::set_orthogonal_selection_from_dense_buffer`].
pub fn b2nd_set_orthogonal_selection_cbuffer(
    array: &mut B2ndArray,
    selection: &[Vec<i64>],
    buffershape: &[i64],
    data: &[u8],
) -> Result<(), &'static str> {
    array.set_orthogonal_selection_from_dense_buffer(selection, buffershape, data)
}

/// C-style orthogonal selection setter with an explicit source size.
pub fn b2nd_set_orthogonal_selection_cbuffer_c(
    array: &mut B2ndArray,
    selection: &[Vec<i64>],
    buffershape: &[i64],
    data: &[u8],
    buffersize: i64,
) -> i32 {
    let buffersize = match usize::try_from(buffersize) {
        Ok(buffersize) => buffersize,
        Err(_) => return BLOSC2_ERROR_INVALID_PARAM,
    };
    match array.set_orthogonal_selection_cbuffer_c_from(selection, buffershape, data, buffersize) {
        Ok(()) => BLOSC2_ERROR_SUCCESS,
        Err(err) => b2nd_selection_error_code(err),
    }
}

/// C-shaped orthogonal selection setter with per-dimension selection sizes.
///
/// `selection` is one coordinate slice per dimension, and `selection_size`
/// says how many coordinates to consume from each dimension.
pub fn b2nd_set_orthogonal_selection_c_sizes_c(
    array: &mut B2ndArray,
    selection: &[&[i64]],
    selection_size: &[i64],
    buffershape: &[i64],
    data: &[u8],
    buffersize: i64,
) -> i32 {
    let selection = match b2nd_c_shaped_selection(array.meta.ndim(), selection, selection_size) {
        Ok(selection) => selection,
        Err(code) => return code,
    };
    b2nd_set_orthogonal_selection_cbuffer_c(array, &selection, buffershape, data, buffersize)
}

/// C-style orthogonal selection setter with per-dimension selection sizes.
pub fn b2nd_set_orthogonal_selection_c(
    array: &mut B2ndArray,
    selection: &[&[i64]],
    selection_size: &[i64],
    buffershape: &[i64],
    data: &[u8],
    buffersize: i64,
) -> i32 {
    b2nd_set_orthogonal_selection_c_sizes_c(
        array,
        selection,
        selection_size,
        buffershape,
        data,
        buffersize,
    )
}

/// Rust-shaped compatibility adapter with an explicit number of dimensions to consume.
pub fn b2nd_set_orthogonal_selection_count_c(
    array: &mut B2ndArray,
    selection: &[Vec<i64>],
    selection_size: i32,
    buffershape: &[i64],
    data: &[u8],
    buffersize: i64,
) -> i32 {
    let selection_size = match usize::try_from(selection_size) {
        Ok(selection_size) if selection_size <= selection.len() => selection_size,
        _ => return BLOSC2_ERROR_INVALID_PARAM,
    };
    b2nd_set_orthogonal_selection_cbuffer_c(
        array,
        &selection[..selection_size],
        buffershape,
        data,
        buffersize,
    )
}

/// C-name alias for [`B2ndArray::resize_with_start`].
pub fn b2nd_resize(
    array: &mut B2ndArray,
    new_shape: Vec<i64>,
    start: Option<&[i64]>,
) -> Result<(), &'static str> {
    array.resize_with_start(new_shape, start)
}

/// C-style status adapter for [`B2ndArray::resize_with_start`].
pub fn b2nd_resize_c(array: &mut B2ndArray, new_shape: Vec<i64>, start: Option<&[i64]>) -> i32 {
    match array.resize_with_start(new_shape, start) {
        Ok(()) => BLOSC2_ERROR_SUCCESS,
        Err(_) => BLOSC2_ERROR_INVALID_PARAM,
    }
}

/// C-name alias for [`B2ndArray::insert_dense_buffer`].
pub fn b2nd_insert(
    array: &mut B2ndArray,
    data: &[u8],
    axis: usize,
    start: i64,
) -> Result<(), &'static str> {
    array.insert_dense_buffer(axis, start, data)
}

/// C-style status adapter for [`B2ndArray::insert_dense_buffer`].
pub fn b2nd_insert_c(
    array: &mut B2ndArray,
    data: &[u8],
    buffersize: i64,
    axis: usize,
    start: i64,
) -> i32 {
    let data = match b2nd_checked_cbuffer_prefix(data, buffersize) {
        Ok(data) => data,
        Err(code) => return code,
    };
    match array.insert_dense_buffer(axis, start, data) {
        Ok(()) => BLOSC2_ERROR_SUCCESS,
        Err(_) => BLOSC2_ERROR_INVALID_PARAM,
    }
}

/// Signed-axis C-style status adapter for [`B2ndArray::insert_dense_buffer`].
pub fn b2nd_insert_axis_c(
    array: &mut B2ndArray,
    data: &[u8],
    buffersize: i64,
    axis: i8,
    start: i64,
) -> i32 {
    let axis = match usize::try_from(axis) {
        Ok(axis) => axis,
        Err(_) => return BLOSC2_ERROR_INVALID_PARAM,
    };
    b2nd_insert_c(array, data, buffersize, axis, start)
}

/// C-name alias for [`B2ndArray::append_dense_buffer`].
pub fn b2nd_append(array: &mut B2ndArray, data: &[u8], axis: usize) -> Result<(), &'static str> {
    array.append_dense_buffer(axis, data)
}

/// C-style status adapter for [`B2ndArray::append_dense_buffer`].
pub fn b2nd_append_c(array: &mut B2ndArray, data: &[u8], buffersize: i64, axis: usize) -> i32 {
    let data = match b2nd_checked_cbuffer_prefix(data, buffersize) {
        Ok(data) => data,
        Err(code) => return code,
    };
    match array.append_dense_buffer(axis, data) {
        Ok(()) => BLOSC2_ERROR_SUCCESS,
        Err(_) => BLOSC2_ERROR_INVALID_PARAM,
    }
}

/// Signed-axis C-style status adapter for [`B2ndArray::append_dense_buffer`].
pub fn b2nd_append_axis_c(array: &mut B2ndArray, data: &[u8], buffersize: i64, axis: i8) -> i32 {
    let axis = match usize::try_from(axis) {
        Ok(axis) => axis,
        Err(_) => return BLOSC2_ERROR_INVALID_PARAM,
    };
    b2nd_append_c(array, data, buffersize, axis)
}

/// C-name alias for [`B2ndArray::delete`].
pub fn b2nd_delete(
    array: &mut B2ndArray,
    axis: usize,
    start: i64,
    len: i64,
) -> Result<(), &'static str> {
    array.delete(axis, start, len)
}

/// C-style status adapter for [`B2ndArray::delete`].
pub fn b2nd_delete_c(array: &mut B2ndArray, axis: usize, start: i64, len: i64) -> i32 {
    let ndim = array.meta.ndim();
    if axis >= ndim {
        return BLOSC2_ERROR_INVALID_PARAM;
    }
    let mut new_shape = array.meta.shape.clone();
    new_shape[axis] = match new_shape[axis].checked_sub(len) {
        Some(dim) => dim,
        None => return BLOSC2_ERROR_INVALID_PARAM,
    };
    let mut resize_start = vec![0i64; ndim];
    resize_start[axis] = start;
    let resize = if start == new_shape[axis] {
        array.resize_with_start(new_shape, None)
    } else {
        array.resize_with_start(new_shape, Some(&resize_start))
    };
    match resize {
        Ok(()) => BLOSC2_ERROR_SUCCESS,
        Err(err) => b2nd_array_error_code(err),
    }
}

/// Signed-axis C-style status adapter for [`B2ndArray::delete`].
pub fn b2nd_delete_axis_c(array: &mut B2ndArray, axis: i8, start: i64, len: i64) -> i32 {
    let axis = match usize::try_from(axis) {
        Ok(axis) => axis,
        Err(_) => return BLOSC2_ERROR_INVALID_PARAM,
    };
    b2nd_delete_c(array, axis, start, len)
}

fn b2nd_checked_cbuffer_prefix(data: &[u8], buffersize: i64) -> Result<&[u8], i32> {
    let buffersize = usize::try_from(buffersize).map_err(|_| BLOSC2_ERROR_INVALID_PARAM)?;
    data.get(..buffersize).ok_or(BLOSC2_ERROR_INVALID_PARAM)
}

fn b2nd_special_size_preflight(meta: &B2ndMeta, cparams: &CParams) -> Result<(), &'static str> {
    meta.validate()?;
    let typesize = b2nd_cparams_typesize(cparams)?;
    let chunk_nbytes = extchunk_nitems(meta)?
        .checked_mul(typesize)
        .ok_or("B2ND chunk too large")?;
    if chunk_nbytes > BLOSC2_MAX_BUFFERSIZE as usize {
        return Err("B2ND chunk too large");
    }
    let block_nbytes = product_i32(&meta.blockshape)?
        .checked_mul(typesize)
        .ok_or("B2ND block too large")?;
    if block_nbytes > i32::MAX as usize {
        return Err("B2ND block too large");
    }
    Ok(())
}

fn b2nd_cparams_typesize(cparams: &CParams) -> Result<usize, &'static str> {
    let typesize = usize::try_from(cparams.typesize).map_err(|_| "Invalid typesize")?;
    if typesize == 0 {
        return Err("Invalid typesize");
    }
    Ok(typesize)
}

fn b2nd_checked_item_prefix(data: &[u8], buffersize: i64, typesize: i32) -> Result<&[u8], i32> {
    let buffersize = usize::try_from(buffersize).map_err(|_| BLOSC2_ERROR_INVALID_PARAM)?;
    let typesize = usize::try_from(typesize).map_err(|_| BLOSC2_ERROR_INVALID_PARAM)?;
    if buffersize < typesize {
        return Err(BLOSC2_ERROR_INVALID_PARAM);
    }
    data.get(..typesize).ok_or(BLOSC2_ERROR_INVALID_PARAM)
}

fn b2nd_checked_dest_len(available: usize, buffersize: i64) -> Result<usize, i32> {
    let buffersize = usize::try_from(buffersize).map_err(|_| BLOSC2_ERROR_INVALID_PARAM)?;
    if buffersize > available {
        return Err(BLOSC2_ERROR_INVALID_PARAM);
    }
    Ok(buffersize)
}

fn b2nd_c_shaped_selection(
    ndim: usize,
    selection: &[&[i64]],
    selection_size: &[i64],
) -> Result<Vec<Vec<i64>>, i32> {
    if selection.len() != ndim || selection_size.len() != ndim {
        return Err(BLOSC2_ERROR_INVALID_PARAM);
    }
    let mut out = Vec::with_capacity(ndim);
    for (&dim_selection, &dim_size) in selection.iter().zip(selection_size) {
        let dim_size = usize::try_from(dim_size).map_err(|_| BLOSC2_ERROR_INVALID_PARAM)?;
        let dim_selection = dim_selection
            .get(..dim_size)
            .ok_or(BLOSC2_ERROR_INVALID_PARAM)?;
        out.push(dim_selection.to_vec());
    }
    Ok(out)
}

fn b2nd_array_result_to_c(result: Result<B2ndArray, &'static str>) -> (i32, Option<B2ndArray>) {
    match result {
        Ok(array) => (BLOSC2_ERROR_SUCCESS, Some(array)),
        Err(err) => (b2nd_array_error_code(err), None),
    }
}

fn b2nd_ctx_array_result_to_c(
    ctx: &B2ndContext,
    result: Result<B2ndArray, &'static str>,
) -> (i32, Option<B2ndArray>) {
    match result {
        Ok(array) => match finish_ctx_array_storage(ctx, array) {
            Ok(array) => (BLOSC2_ERROR_SUCCESS, Some(array)),
            Err(err) => (b2nd_ctx_storage_error_code(&err), None),
        },
        Err(err) => (b2nd_array_error_code(err), None),
    }
}

fn b2nd_get_slice_array_with_meta_c(
    array: &B2ndArray,
    start: &[i64],
    stop: &[i64],
    mut meta: B2ndMeta,
    cparams: CParams,
    dparams: DParams,
    metalayers: &[(&str, &[u8])],
) -> Result<B2ndArray, &'static str> {
    let extents_as_i64 = slice_extents_without_source_bounds(&array.meta, start, stop)?;
    meta.shape = extents_as_i64.clone();
    if product_i64(&extents_as_i64)? == 0 {
        return B2ndArray::empty_with_metalayers(meta, cparams, dparams, metalayers);
    }
    match validate_slice_bounds(&array.meta, start, stop) {
        Ok(_) => {}
        Err(err) => return Err(err),
    }
    array.slice_with_meta_and_metalayers(start, stop, meta, cparams, dparams, metalayers)
}

fn finish_ctx_array_storage(ctx: &B2ndContext, mut array: B2ndArray) -> Result<B2ndArray, String> {
    let Some(storage) = &ctx.storage else {
        return Ok(array);
    };
    array.schunk.set_storage(if storage.contiguous {
        FrameStorage::Contiguous
    } else {
        FrameStorage::Sparse
    });
    let Some(urlpath) = &storage.urlpath else {
        return Ok(array);
    };

    let path = normalized_path(urlpath.as_path());
    if path.as_ref().exists() {
        return Err("B2ND destination already exists".to_string());
    }
    if storage.contiguous {
        array
            .schunk
            .write_contiguous_frame_path(path.as_ref())
            .map_err(|err| format!("Failed to write B2ND frame: {err}"))?;
    } else {
        array
            .save_sframe(path.as_ref())
            .map_err(|err| format!("Failed to write B2ND sparse frame: {err}"))?;
    }
    B2ndArray::open(path.as_ref())
}

fn b2nd_expand_dims_view_c(array: &B2ndArray, axes: &[bool]) -> Result<B2ndArray, &'static str> {
    array.ensure_viewable_metalayers()?;
    if axes.len() > B2ND_MAX_DIM {
        return Err("Invalid B2ND ndim");
    }
    let mut old_axis = 0usize;
    let mut meta = array.meta.clone();
    meta.shape.clear();
    meta.chunkshape.clear();
    meta.blockshape.clear();
    for &insert_axis in axes {
        if insert_axis {
            meta.shape.push(1);
            meta.chunkshape.push(1);
            meta.blockshape.push(1);
        } else {
            if old_axis == array.meta.ndim() {
                return Err("B2ND expand_dims axes do not match array rank");
            }
            meta.shape.push(array.meta.shape[old_axis]);
            meta.chunkshape.push(array.meta.chunkshape[old_axis]);
            meta.blockshape.push(array.meta.blockshape[old_axis]);
            old_axis += 1;
        }
    }
    meta.validate()?;
    let encoded = meta.serialize()?;
    let mut view = B2ndArray::from_parts(meta.clone(), array.schunk.clone_with_shared_chunks());
    view.allow_oversized_chunks = array.allow_oversized_chunks;
    view.schunk.remove_metalayer(B2ND_METALAYER_NAME);
    view.schunk.add_metalayer(B2ND_METALAYER_NAME, &encoded)?;
    Ok(view)
}

fn b2nd_squeeze_index_view_c(array: &B2ndArray, axes: &[bool]) -> Result<B2ndArray, &'static str> {
    let ndim = array.meta.ndim();
    if axes.len() < ndim {
        return Err("B2ND squeeze axes do not match array rank");
    }
    array.squeeze_index_view(&axes[..ndim])
}

fn b2nd_array_error_code(err: &str) -> i32 {
    match err {
        "B2ND chunk too large" => BLOSC2_ERROR_MAX_BUFSIZE_EXCEEDED,
        "B2ND dtype too large" | "Invalid B2ND dtype format" => BLOSC2_ERROR_FAILURE,
        "Schunk does not contain a B2ND metalayer"
        | "Missing b2nd metalayer"
        | "Missing caterva metalayer" => BLOSC2_ERROR_METALAYER_NOT_FOUND,
        _ => BLOSC2_ERROR_INVALID_PARAM,
    }
}

fn b2nd_ctx_storage_error_code(err: &str) -> i32 {
    if err == "B2ND destination already exists" {
        BLOSC2_ERROR_FILE_WRITE
    } else if err.contains("Failed to read file")
        || err.contains("Failed to open")
        || err.contains("No such file")
    {
        BLOSC2_ERROR_FILE_OPEN
    } else {
        BLOSC2_ERROR_FILE_WRITE
    }
}

fn b2nd_frame_error_code(err: &str) -> i32 {
    if matches!(
        err,
        "Schunk does not contain a B2ND metalayer"
            | "Missing b2nd metalayer"
            | "Missing caterva metalayer"
    ) {
        BLOSC2_ERROR_METALAYER_NOT_FOUND
    } else if err == "copy=false requires owned frame buffer" {
        BLOSC2_ERROR_INVALID_PARAM
    } else {
        BLOSC2_ERROR_FAILURE
    }
}

fn b2nd_squeeze_error_code(err: &str) -> i32 {
    match err {
        "Cannot squeeze a non-singleton B2ND dimension" => BLOSC2_ERROR_INVALID_INDEX,
        _ => BLOSC2_ERROR_INVALID_PARAM,
    }
}

fn b2nd_selection_error_code(err: &str) -> i32 {
    match err {
        "Invalid B2ND selection coordinate" => BLOSC2_ERROR_INVALID_INDEX,
        _ => BLOSC2_ERROR_INVALID_PARAM,
    }
}

fn b2nd_open_error_code(err: &str) -> i32 {
    if err.contains("Failed to read file")
        || err.contains("Failed to open")
        || err.contains("No such file")
        || err.contains("Permission denied")
    {
        BLOSC2_ERROR_NULL_POINTER
    } else if matches!(
        err,
        "Schunk does not contain a B2ND metalayer"
            | "Missing b2nd metalayer"
            | "Missing caterva metalayer"
    ) {
        BLOSC2_ERROR_METALAYER_NOT_FOUND
    } else {
        BLOSC2_ERROR_INVALID_PARAM
    }
}

/// Shape descriptor for a b2nd array.
///
/// Holds the logical shape, the chunkshape, the blockshape and the dtype
/// string. This is exactly the information serialized into the `b2nd`
/// metalayer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct B2ndMeta {
    /// Shape of the original data in items per dimension.
    pub shape: Vec<i64>,
    /// Shape of each chunk in items per dimension.
    pub chunkshape: Vec<i32>,
    /// Shape of each block in items per dimension.
    pub blockshape: Vec<i32>,
    /// Data type as a string (NumPy dtype string when `dtype_format = 0`).
    pub dtype: String,
    /// Format of the data type string. `0` means NumPy.
    pub dtype_format: i8,
}

struct B2ndMetaDecode {
    meta: B2ndMeta,
    consumed: usize,
    dtype_present: bool,
}

/// A multidimensional array of fixed-size items backed by a super-chunk.
///
/// `schunk` is intentionally public for compatibility with the crate's
/// existing Rust API. B2ND-created arrays use an internal shared chunk backing
/// for metadata-only expand/squeeze views while preserving the public owned
/// field. Direct external mutation of `schunk.chunks` is synchronized back to
/// the shared backing when that same [`Schunk`] handle next enters the Schunk
/// or B2ND API. Mutating chunks through [`Schunk`] methods remains preferred
/// because Rust cannot intercept raw `Vec` edits before any method is called.
#[derive(Clone)]
pub struct B2ndArray {
    /// Shape descriptor stored as the b2nd metalayer.
    pub meta: B2ndMeta,
    /// Underlying owned super-chunk holding the compressed chunks.
    pub schunk: Schunk,
    attached_frame: Option<B2ndAttachedFrame>,
    allow_oversized_chunks: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct B2ndAttachedFrame {
    path: PathBuf,
    storage: FrameStorage,
    offset: u64,
}

impl B2ndMeta {
    /// Build a validated [`B2ndMeta`] from the array shape, chunkshape,
    /// blockshape and dtype string.
    pub fn new(
        shape: Vec<i64>,
        chunkshape: Vec<i32>,
        blockshape: Vec<i32>,
        dtype: impl Into<String>,
        dtype_format: i8,
    ) -> Result<Self, &'static str> {
        let meta = Self {
            shape,
            chunkshape,
            blockshape,
            dtype: dtype.into(),
            dtype_format,
        };
        meta.validate()?;
        Ok(meta)
    }

    /// Build metadata using C-Blosc2's default byte-string dtype for `typesize`.
    pub fn with_default_dtype(
        shape: Vec<i64>,
        chunkshape: Vec<i32>,
        blockshape: Vec<i32>,
        typesize: usize,
    ) -> Result<Self, &'static str> {
        if typesize == 0 {
            return Err("Invalid typesize");
        }
        Self::new(
            shape,
            chunkshape,
            blockshape,
            format!("|S{typesize}"),
            DTYPE_NUMPY_FORMAT,
        )
    }

    /// Number of dimensions of the array.
    pub fn ndim(&self) -> usize {
        self.shape.len()
    }

    /// Total number of items in the original (un-padded) array.
    pub fn nitems(&self) -> Result<usize, &'static str> {
        product_i64(&self.shape)
    }

    /// Number of items in a single chunk.
    pub fn chunk_nitems(&self) -> Result<usize, &'static str> {
        product_i32(&self.chunkshape)
    }

    /// Check that ranks, sizes and dtype satisfy all b2nd invariants.
    pub fn validate(&self) -> Result<(), &'static str> {
        let ndim = self.shape.len();
        if ndim > B2ND_MAX_DIM {
            return Err("Invalid B2ND ndim");
        }
        if self.chunkshape.len() != ndim || self.blockshape.len() != ndim {
            return Err("B2ND shape ranks differ");
        }
        if self.dtype.len() > i32::MAX as usize {
            return Err("B2ND dtype too large");
        }
        if !(0..=127).contains(&self.dtype_format) {
            return Err("Invalid B2ND dtype format");
        }
        for dim in 0..ndim {
            if self.shape[dim] < 0 {
                return Err("Invalid B2ND shape");
            }
            if self.shape[dim] == 0 {
                if self.chunkshape[dim] < 0 || self.blockshape[dim] < 0 {
                    return Err("Invalid B2ND chunk or block shape");
                }
                if (self.chunkshape[dim] == 0) != (self.blockshape[dim] == 0) {
                    return Err("Invalid B2ND chunk or block shape");
                }
            } else if self.chunkshape[dim] <= 0 || self.blockshape[dim] <= 0 {
                return Err("Invalid B2ND chunk or block shape");
            }
        }
        self.nitems()?;
        self.chunk_nitems()?;
        Ok(())
    }

    /// Encode the metadata as a msgpack buffer suitable for the b2nd metalayer.
    pub fn serialize(&self) -> Result<Vec<u8>, &'static str> {
        self.validate()?;
        let ndim = self.ndim();

        let dtype = self.dtype.as_bytes();
        let mut out = Vec::with_capacity(3 + 3 * (3 + ndim * 9) + 6 + dtype.len());
        out.push(0x90 + 7);
        out.push(B2ND_METALAYER_VERSION);
        out.push(ndim as u8);

        write_array_header(&mut out, ndim)?;
        for &dim in &self.shape {
            out.push(0xd3);
            out.extend_from_slice(&dim.to_be_bytes());
        }

        write_array_header(&mut out, ndim)?;
        for &dim in &self.chunkshape {
            out.push(0xd2);
            out.extend_from_slice(&dim.to_be_bytes());
        }

        write_array_header(&mut out, ndim)?;
        for &dim in &self.blockshape {
            out.push(0xd2);
            out.extend_from_slice(&dim.to_be_bytes());
        }

        out.push(self.dtype_format as u8);
        out.push(0xdb);
        out.extend_from_slice(&(dtype.len() as i32).to_be_bytes());
        out.extend_from_slice(dtype);
        Ok(out)
    }

    /// Decode the msgpack buffer stored in the b2nd metalayer back into a
    /// validated [`B2ndMeta`].
    pub fn deserialize(data: &[u8]) -> Result<Self, &'static str> {
        Self::deserialize_inner(data, None)
    }

    fn deserialize_legacy_optional_dtype(
        data: &[u8],
        typesize: usize,
    ) -> Result<Self, &'static str> {
        Self::deserialize_legacy_optional_dtype_with_info(data, typesize)
            .map(|decoded| decoded.meta)
    }

    fn deserialize_caterva(data: &[u8], typesize: usize) -> Result<Self, &'static str> {
        Self::deserialize_caterva_with_info(data, typesize).map(|decoded| decoded.meta)
    }

    fn deserialize_legacy_optional_dtype_with_info(
        data: &[u8],
        _typesize: usize,
    ) -> Result<B2ndMetaDecode, &'static str> {
        Self::deserialize_inner_with_info(data, Some(B2ND_DEFAULT_DTYPE.to_string()))
    }

    fn deserialize_caterva_with_info(
        data: &[u8],
        typesize: usize,
    ) -> Result<B2ndMetaDecode, &'static str> {
        Self::deserialize_legacy_optional_dtype_with_info(data, typesize)
    }

    fn deserialize_c(data: &[u8]) -> Result<B2ndMetaDecode, &'static str> {
        if data.len() < 3 {
            return Err("Truncated B2ND metadata");
        }
        let ndim = data[2] as usize;
        if ndim > B2ND_MAX_DIM {
            return Err("Invalid B2ND ndim");
        }
        let mut pos = 3usize;

        skip_byte(data, &mut pos)?;
        let mut shape = Vec::with_capacity(ndim);
        for _ in 0..ndim {
            expect_byte(data, &mut pos, 0xd3)?;
            shape.push(read_i64(data, &mut pos)?);
        }

        skip_byte(data, &mut pos)?;
        let mut chunkshape = Vec::with_capacity(ndim);
        for _ in 0..ndim {
            expect_byte(data, &mut pos, 0xd2)?;
            chunkshape.push(read_i32(data, &mut pos)?);
        }

        skip_byte(data, &mut pos)?;
        let mut blockshape = Vec::with_capacity(ndim);
        for _ in 0..ndim {
            expect_byte(data, &mut pos, 0xd2)?;
            blockshape.push(read_i32(data, &mut pos)?);
        }

        let (dtype, dtype_format, dtype_present) = if pos == data.len() {
            (String::new(), DTYPE_NUMPY_FORMAT, false)
        } else {
            let dtype_format = *data.get(pos).ok_or("Truncated B2ND metadata")? as i8;
            pos += 1;
            expect_byte(data, &mut pos, 0xdb)?;
            let dtype_len = read_i32(data, &mut pos)?;
            if dtype_len < 0 {
                return Err("Invalid B2ND dtype length");
            }
            let dtype_len = dtype_len as usize;
            let end = pos
                .checked_add(dtype_len)
                .ok_or("Invalid B2ND dtype length")?;
            if end > data.len() {
                return Err("Invalid B2ND metadata length");
            }
            let dtype = String::from_utf8_lossy(&data[pos..end]).into_owned();
            pos = end;
            (dtype, dtype_format, true)
        };

        let meta = Self {
            shape,
            chunkshape,
            blockshape,
            dtype,
            dtype_format,
        };
        Ok(B2ndMetaDecode {
            meta,
            consumed: pos,
            dtype_present,
        })
    }

    fn deserialize_inner(data: &[u8], legacy_dtype: Option<String>) -> Result<Self, &'static str> {
        Self::deserialize_inner_with_info(data, legacy_dtype).map(|decoded| decoded.meta)
    }

    fn deserialize_inner_with_info(
        data: &[u8],
        legacy_dtype: Option<String>,
    ) -> Result<B2ndMetaDecode, &'static str> {
        let mut pos = 0usize;
        let fields = read_array_header(data, &mut pos)?;
        if fields != 7 && !(fields == 5 && legacy_dtype.is_some()) {
            return Err("Invalid B2ND metadata");
        }
        let version = read_fixint(data, &mut pos)?;
        if version != B2ND_METALAYER_VERSION {
            return Err("Unsupported B2ND metalayer version");
        }
        let ndim = read_fixint(data, &mut pos)? as usize;
        if ndim > B2ND_MAX_DIM {
            return Err("Invalid B2ND ndim");
        }

        expect_array_header(data, &mut pos, ndim)?;
        let mut shape = Vec::with_capacity(ndim);
        for _ in 0..ndim {
            expect_byte(data, &mut pos, 0xd3)?;
            shape.push(read_i64(data, &mut pos)?);
        }

        expect_array_header(data, &mut pos, ndim)?;
        let mut chunkshape = Vec::with_capacity(ndim);
        for _ in 0..ndim {
            expect_byte(data, &mut pos, 0xd2)?;
            chunkshape.push(read_i32(data, &mut pos)?);
        }

        expect_array_header(data, &mut pos, ndim)?;
        let mut blockshape = Vec::with_capacity(ndim);
        for _ in 0..ndim {
            expect_byte(data, &mut pos, 0xd2)?;
            blockshape.push(read_i32(data, &mut pos)?);
        }

        let (dtype, dtype_format, dtype_present) = match legacy_dtype {
            Some(dtype) if pos == data.len() || fields == 5 => {
                if pos != data.len() {
                    return Err("Invalid B2ND metadata length");
                }
                (dtype, DTYPE_NUMPY_FORMAT, false)
            }
            _ => {
                let dtype_format = read_fixint(data, &mut pos)? as i8;
                expect_byte(data, &mut pos, 0xdb)?;
                let dtype_len = read_i32(data, &mut pos)?;
                if dtype_len < 0 {
                    return Err("Invalid B2ND dtype length");
                }
                let dtype_len = dtype_len as usize;
                let end = pos
                    .checked_add(dtype_len)
                    .ok_or("Invalid B2ND dtype length")?;
                if end > data.len() {
                    return Err("Invalid B2ND metadata length");
                }
                let dtype = std::str::from_utf8(&data[pos..end])
                    .map_err(|_| "B2ND dtype is not UTF-8")?
                    .to_string();
                pos = end;
                if pos != data.len() {
                    return Err("Invalid B2ND metadata length");
                }
                (dtype, dtype_format, true)
            }
        };

        let meta = Self::new(shape, chunkshape, blockshape, dtype, dtype_format)?;
        Ok(B2ndMetaDecode {
            meta,
            consumed: pos,
            dtype_present,
        })
    }
}

impl B2ndArray {
    fn from_parts(meta: B2ndMeta, mut schunk: Schunk) -> Self {
        schunk.enable_shared_chunks();
        Self {
            meta,
            schunk,
            attached_frame: None,
            allow_oversized_chunks: false,
        }
    }

    /// Build a b2nd array filled with zeros, stored as Blosc2 special chunks.
    pub fn zeros(meta: B2ndMeta, cparams: CParams, dparams: DParams) -> Result<Self, &'static str> {
        Self::zeros_with_metalayers(meta, cparams, dparams, &[])
    }

    /// Build a zero-filled b2nd array and attach extra fixed-size metalayers.
    pub fn zeros_with_metalayers(
        meta: B2ndMeta,
        cparams: CParams,
        dparams: DParams,
        metalayers: &[(&str, &[u8])],
    ) -> Result<Self, &'static str> {
        Self::from_special(meta, cparams, dparams, BLOSC2_SPECIAL_ZERO, metalayers)
    }

    /// Build a b2nd array filled with uninitialized bytes, stored as Blosc2
    /// special chunks.
    pub fn uninit(
        meta: B2ndMeta,
        cparams: CParams,
        dparams: DParams,
    ) -> Result<Self, &'static str> {
        Self::uninit_with_metalayers(meta, cparams, dparams, &[])
    }

    /// Build an uninitialized b2nd array and attach extra fixed-size metalayers.
    pub fn uninit_with_metalayers(
        meta: B2ndMeta,
        cparams: CParams,
        dparams: DParams,
        metalayers: &[(&str, &[u8])],
    ) -> Result<Self, &'static str> {
        Self::from_special(meta, cparams, dparams, BLOSC2_SPECIAL_UNINIT, metalayers)
    }

    /// Build a b2nd array with uninitialized storage.
    pub fn empty(meta: B2ndMeta, cparams: CParams, dparams: DParams) -> Result<Self, &'static str> {
        Self::empty_with_metalayers(meta, cparams, dparams, &[])
    }

    /// Build an empty b2nd array and attach extra fixed-size metalayers.
    pub fn empty_with_metalayers(
        meta: B2ndMeta,
        cparams: CParams,
        dparams: DParams,
        metalayers: &[(&str, &[u8])],
    ) -> Result<Self, &'static str> {
        Self::zeros_with_metalayers(meta, cparams, dparams, metalayers)
    }

    /// Build a b2nd array filled with NaNs, stored as Blosc2 special chunks.
    /// Materializing the data requires a 4- or 8-byte floating-point item.
    pub fn nans(meta: B2ndMeta, cparams: CParams, dparams: DParams) -> Result<Self, &'static str> {
        Self::nans_with_metalayers(meta, cparams, dparams, &[])
    }

    /// Build a NaN-filled b2nd array and attach extra fixed-size metalayers.
    pub fn nans_with_metalayers(
        meta: B2ndMeta,
        cparams: CParams,
        dparams: DParams,
        metalayers: &[(&str, &[u8])],
    ) -> Result<Self, &'static str> {
        if cparams.typesize != 4 && cparams.typesize != 8 {
            return Err("NaN special only valid for 4 or 8 byte types");
        }
        Self::from_special(meta, cparams, dparams, BLOSC2_SPECIAL_NAN, metalayers)
    }

    fn nans_unchecked_with_metalayers(
        meta: B2ndMeta,
        cparams: CParams,
        dparams: DParams,
        metalayers: &[(&str, &[u8])],
    ) -> Result<Self, &'static str> {
        Self::from_special(meta, cparams, dparams, BLOSC2_SPECIAL_NAN, metalayers)
    }

    /// Build a b2nd array filled with one repeated item value.
    pub fn full(
        meta: B2ndMeta,
        value: &[u8],
        cparams: CParams,
        dparams: DParams,
    ) -> Result<Self, &'static str> {
        Self::full_with_metalayers(meta, value, cparams, dparams, &[])
    }

    /// Build a repeat-value b2nd array and attach extra fixed-size metalayers.
    pub fn full_with_metalayers(
        meta: B2ndMeta,
        value: &[u8],
        cparams: CParams,
        dparams: DParams,
        metalayers: &[(&str, &[u8])],
    ) -> Result<Self, &'static str> {
        let typesize = b2nd_cparams_typesize(&cparams)?;
        if value.len() != typesize {
            return Err("B2ND fill value size does not match typesize");
        }
        Self::from_repeatval(meta, value, cparams, dparams, metalayers)
    }

    fn from_special(
        meta: B2ndMeta,
        mut cparams: CParams,
        dparams: DParams,
        special: u8,
        metalayers: &[(&str, &[u8])],
    ) -> Result<Self, &'static str> {
        meta.validate()?;
        let typesize = b2nd_cparams_typesize(&cparams)?;
        b2nd_validate_cparams_for_array(&cparams)?;
        let chunk_nbytes = extchunk_nitems(&meta)?
            .checked_mul(typesize)
            .ok_or("B2ND chunk too large")?;
        if chunk_nbytes > BLOSC2_MAX_BUFFERSIZE as usize {
            return Err("B2ND chunk too large");
        }
        let block_nbytes = product_i32(&meta.blockshape)?
            .checked_mul(typesize)
            .ok_or("B2ND block too large")?;
        if block_nbytes > i32::MAX as usize {
            return Err("B2ND block too large");
        }
        cparams.blocksize = block_nbytes as i32;

        let chunk_grid = chunk_grid(&meta)?;
        let chunk_count = product_usize(&chunk_grid)?;
        let stored_nitems = chunk_count
            .checked_mul(extchunk_nitems(&meta)?)
            .ok_or("B2ND buffer too large")?;

        let mut schunk = Schunk::new(cparams, dparams);
        schunk.set_storage(FrameStorage::Sparse);
        schunk.chunksize = chunk_nbytes;
        let encoded_meta = meta.serialize()?;
        schunk.cparams.b2nd_metalayer = Some(encoded_meta.clone());
        schunk.dparams.b2nd_metalayer = Some(encoded_meta.clone());
        schunk.add_metalayer(B2ND_METALAYER_NAME, &encoded_meta)?;
        add_fixed_metalayers(&mut schunk, metalayers)?;
        schunk.fill_special(stored_nitems, special, chunk_nbytes)?;

        Ok(Self::from_parts(meta, schunk))
    }

    fn from_repeatval(
        meta: B2ndMeta,
        value: &[u8],
        mut cparams: CParams,
        dparams: DParams,
        metalayers: &[(&str, &[u8])],
    ) -> Result<Self, &'static str> {
        meta.validate()?;
        let typesize = b2nd_cparams_typesize(&cparams)?;
        b2nd_validate_cparams_for_array(&cparams)?;
        if value.len() != typesize {
            return Err("B2ND fill value size does not match typesize");
        }
        let chunk_nbytes = extchunk_nitems(&meta)?
            .checked_mul(typesize)
            .ok_or("B2ND chunk too large")?;
        if chunk_nbytes > BLOSC2_MAX_BUFFERSIZE as usize {
            return Err("B2ND chunk too large");
        }
        let block_nbytes = product_i32(&meta.blockshape)?
            .checked_mul(typesize)
            .ok_or("B2ND block too large")?;
        if block_nbytes > i32::MAX as usize {
            return Err("B2ND block too large");
        }
        cparams.blocksize = block_nbytes as i32;

        let chunk_grid = chunk_grid(&meta)?;
        let chunk_count = product_usize(&chunk_grid)?;
        let stored_nitems = chunk_count
            .checked_mul(extchunk_nitems(&meta)?)
            .ok_or("B2ND buffer too large")?;

        let mut schunk = Schunk::new(cparams, dparams);
        schunk.set_storage(FrameStorage::Sparse);
        schunk.chunksize = chunk_nbytes;
        let encoded_meta = meta.serialize()?;
        schunk.cparams.b2nd_metalayer = Some(encoded_meta.clone());
        schunk.dparams.b2nd_metalayer = Some(encoded_meta.clone());
        schunk.add_metalayer(B2ND_METALAYER_NAME, &encoded_meta)?;
        add_fixed_metalayers(&mut schunk, metalayers)?;
        schunk.fill_repeatval(stored_nitems, value, chunk_nbytes)?;

        Ok(Self::from_parts(meta, schunk))
    }

    /// Build a b2nd array from a dense row-major C buffer.
    ///
    /// The buffer must contain `meta.nitems() * cparams.typesize` bytes laid out
    /// in C order. Data is split into chunks and blocks, compressed with
    /// `cparams`, and written to a new super-chunk that carries `meta` as the
    /// `b2nd` metalayer.
    pub fn from_dense_buffer(
        meta: B2ndMeta,
        data: &[u8],
        cparams: CParams,
        dparams: DParams,
    ) -> Result<Self, &'static str> {
        Self::from_cbuffer_with_metalayers(meta, data, cparams, dparams, &[])
    }

    /// Build a b2nd array from a dense row-major C buffer and attach extra
    /// fixed-size metalayers after the b2nd descriptor.
    pub fn from_cbuffer_with_metalayers(
        meta: B2ndMeta,
        data: &[u8],
        mut cparams: CParams,
        dparams: DParams,
        metalayers: &[(&str, &[u8])],
    ) -> Result<Self, &'static str> {
        meta.validate()?;
        let typesize = b2nd_cparams_typesize(&cparams)?;
        b2nd_validate_cparams_for_array(&cparams)?;
        let expected_len = meta
            .nitems()?
            .checked_mul(typesize)
            .ok_or("B2ND buffer too large")?;
        if data.len() < expected_len {
            return Err("B2ND buffer size does not match shape and typesize");
        }

        let chunk_nbytes = extchunk_nitems(&meta)?
            .checked_mul(typesize)
            .ok_or("B2ND chunk too large")?;
        if chunk_nbytes > BLOSC2_MAX_BUFFERSIZE as usize {
            return Err("B2ND chunk too large");
        }
        let block_nbytes = product_i32(&meta.blockshape)?
            .checked_mul(typesize)
            .ok_or("B2ND block too large")?;
        if block_nbytes > i32::MAX as usize {
            return Err("B2ND block too large");
        }
        cparams.blocksize = block_nbytes as i32;

        let mut schunk = Schunk::new(cparams, dparams);
        schunk.set_storage(FrameStorage::Sparse);
        schunk.chunksize = chunk_nbytes;
        let encoded_meta = meta.serialize()?;
        schunk.cparams.b2nd_metalayer = Some(encoded_meta.clone());
        schunk.dparams.b2nd_metalayer = Some(encoded_meta.clone());
        schunk.add_metalayer(B2ND_METALAYER_NAME, &encoded_meta)?;
        add_fixed_metalayers(&mut schunk, metalayers)?;

        let chunk_grid = chunk_grid(&meta)?;
        let chunk_count = product_usize(&chunk_grid)?;
        if chunk_count > 0 {
            let layout = B2ndLayout::new(&meta, typesize)?;
            for linear_chunk in 0..chunk_count {
                let chunk_index = unravel_index(linear_chunk, &chunk_grid);
                let mut chunk = vec![0u8; chunk_nbytes];
                copy_dense_to_chunk(&meta, data, &layout, &chunk_index, &mut chunk)?;
                schunk.append_buffer(&chunk)?;
            }
        }

        if let Some(encoded_meta) = schunk.metalayer(B2ND_METALAYER_NAME).map(<[u8]>::to_vec) {
            schunk.cparams.b2nd_metalayer = Some(encoded_meta.clone());
            schunk.dparams.b2nd_metalayer = Some(encoded_meta);
        }
        Ok(Self::from_parts(meta, schunk))
    }

    /// Reinterpret a super-chunk as a b2nd array by reading its `b2nd`
    /// metalayer. Chunk count and chunk byte sizes are checked by data accessors.
    pub fn from_schunk(schunk: Schunk) -> Result<Self, &'static str> {
        let typesize = b2nd_cparams_typesize(&schunk.cparams)?;
        let meta = if let Some(content) = schunk.metalayer(B2ND_METALAYER_NAME) {
            B2ndMeta::deserialize_legacy_optional_dtype(content, typesize)?
        } else if let Some(content) = schunk.metalayer(CATERVA_METALAYER_NAME) {
            B2ndMeta::deserialize_caterva(content, typesize)?
        } else {
            return Err("Schunk does not contain a B2ND metalayer");
        };
        Ok(Self::from_parts(meta, schunk))
    }

    /// Build a b2nd array from a serialized contiguous frame.
    pub fn from_contiguous_frame(frame: &[u8]) -> Result<Self, String> {
        Self::from_schunk(Schunk::from_contiguous_frame(frame)?).map_err(str::to_string)
    }

    /// Open a b2nd array from a contiguous frame file or sparse frame
    /// directory on disk.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, String> {
        let path = normalized_path(path.as_ref());
        let storage = if path.as_ref().is_dir() {
            FrameStorage::Sparse
        } else {
            FrameStorage::Contiguous
        };
        let mut array =
            Self::from_schunk(Schunk::open_frame_at(path.as_ref(), 0)?).map_err(str::to_string)?;
        array.attached_frame = Some(B2ndAttachedFrame {
            path: path.into_owned(),
            storage,
            offset: 0,
        });
        Ok(array)
    }

    /// Open a b2nd array from a contiguous frame embedded at `offset` bytes in
    /// a file.
    pub fn open_frame_at(path: impl AsRef<Path>, offset: u64) -> Result<Self, String> {
        let path = normalized_path(path.as_ref());
        let storage = if path.as_ref().is_dir() {
            FrameStorage::Sparse
        } else {
            FrameStorage::Contiguous
        };
        let mut array = Self::from_schunk(Schunk::open_frame_at(path.as_ref(), offset)?)
            .map_err(str::to_string)?;
        array.attached_frame = Some(B2ndAttachedFrame {
            path: path.into_owned(),
            storage,
            offset,
        });
        Ok(array)
    }

    /// Serialize the array as a contiguous in-memory frame.
    pub fn to_contiguous_frame(&self) -> Vec<u8> {
        self.schunk.to_contiguous_frame()
    }

    /// Write the array at `path`, preserving the source storage kind when the
    /// array was opened from a frame.
    pub fn save(&self, path: impl AsRef<Path>) -> std::io::Result<()> {
        let path = normalized_path(path.as_ref());
        if path.as_ref().exists() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "B2ND destination already exists",
            ));
        }
        match self.schunk.storage() {
            FrameStorage::Contiguous => self
                .schunk
                .write_contiguous_frame_path(path.as_ref())
                .map(|_| ()),
            FrameStorage::Sparse => self.save_sframe(path),
        }
    }

    /// Write the array as a sparse frame directory.
    pub fn save_sframe(&self, path: impl AsRef<Path>) -> std::io::Result<()> {
        self.schunk.write_sparse_frame_dir(path)
    }

    /// Append the array as a contiguous frame to `path`, returning the frame's
    /// starting byte offset.
    pub fn save_append(&self, path: impl AsRef<Path>) -> std::io::Result<u64> {
        self.schunk.append_contiguous_frame_file(path)
    }

    /// Decompress every chunk and assemble a dense row-major C buffer covering
    /// the full array shape.
    pub fn to_dense_buffer(&self) -> Result<Vec<u8>, &'static str> {
        let typesize = b2nd_cparams_typesize(&self.schunk.cparams)?;
        let out_len = self.preflight_dense_cbuffer_len()?;
        let mut out = vec![0u8; out_len];
        let chunk_count = self.preflight_chunk_count()?;
        if chunk_count == 0 {
            return Ok(out);
        }

        let layout = B2ndLayout::new(&self.meta, typesize)?;
        let chunk_grid = chunk_grid(&self.meta)?;
        for linear_chunk in 0..chunk_count {
            let chunk = self.schunk.decompress_chunk(linear_chunk as i64)?;
            let expected_chunk_len = extchunk_nitems(&self.meta)?
                .checked_mul(typesize)
                .ok_or("B2ND chunk too large")?;
            if chunk.len() < expected_chunk_len
                || (!self.allow_oversized_chunks && chunk.len() != expected_chunk_len)
            {
                return Err("B2ND chunk size does not match metadata");
            }
            let chunk_index = unravel_index(linear_chunk, &chunk_grid);
            copy_chunk_to_dense(&self.meta, &chunk, &layout, &chunk_index, &mut out)?;
        }
        Ok(out)
    }

    /// Shape of the original data in items per dimension.
    pub fn shape(&self) -> &[i64] {
        &self.meta.shape
    }

    /// Shape of each chunk in items per dimension.
    pub fn chunkshape(&self) -> &[i32] {
        &self.meta.chunkshape
    }

    /// Shape of each block in items per dimension.
    pub fn blockshape(&self) -> &[i32] {
        &self.meta.blockshape
    }

    /// Return a human-readable summary of the b2nd metadata.
    pub fn format_meta(&self) -> String {
        format!(
            "shape: {:?}\nchunkshape: {:?}\nblockshape: {:?}\ndtype: {}\ndtype_format: {}\ntypesize: {}",
            self.meta.shape,
            self.meta.chunkshape,
            self.meta.blockshape,
            self.meta.dtype,
            self.meta.dtype_format,
            self.schunk.cparams.typesize
        )
    }

    /// Return a chunk-compatible metadata view with singleton dimensions
    /// inserted wherever `axes` is true.
    ///
    /// The number of `false` entries must match the current rank. Like
    /// C-Blosc2's b2nd view helpers, this rejects arrays carrying fixed-size
    /// metalayers other than the b2nd descriptor.
    ///
    /// The returned array has independent metadata but shares chunk backing
    /// with the source for B2ND methods.
    pub fn expand_dims_view(&self, axes: &[bool]) -> Result<Self, &'static str> {
        self.ensure_viewable_metalayers()?;
        if axes.len() > B2ND_MAX_DIM {
            return Err("Invalid B2ND ndim");
        }
        let retained = axes.iter().filter(|&&axis| !axis).count();
        if retained != self.meta.ndim() {
            return Err("B2ND expand_dims axes do not match array rank");
        }

        let mut old_axis = 0usize;
        let mut meta = self.meta.clone();
        meta.shape.clear();
        meta.chunkshape.clear();
        meta.blockshape.clear();
        for &insert_axis in axes {
            if insert_axis {
                meta.shape.push(1);
                meta.chunkshape.push(1);
                meta.blockshape.push(1);
            } else {
                meta.shape.push(self.meta.shape[old_axis]);
                meta.chunkshape.push(self.meta.chunkshape[old_axis]);
                meta.blockshape.push(self.meta.blockshape[old_axis]);
                old_axis += 1;
            }
        }
        self.view_with_meta(meta)
    }

    /// Return a chunk-compatible metadata view with every singleton dimension
    /// removed.
    ///
    /// The returned array has independent metadata but shares chunk backing
    /// with the source for B2ND methods.
    pub fn squeeze_view(&self) -> Result<Self, &'static str> {
        let axes: Vec<bool> = self.meta.shape.iter().map(|&dim| dim == 1).collect();
        self.squeeze_index_view(&axes)
    }

    /// Return a chunk-compatible metadata view with selected singleton
    /// dimensions removed.
    ///
    /// Selected dimensions must have shape equal to one. If their chunk or
    /// block shape is larger than one, the resulting view keeps the same raw
    /// chunk backing and ignores the now-unreachable padded items when
    /// materialized.
    pub fn squeeze_index_view(&self, axes: &[bool]) -> Result<Self, &'static str> {
        self.ensure_viewable_metalayers()?;
        if axes.len() != self.meta.ndim() {
            return Err("B2ND squeeze axes do not match array rank");
        }

        let mut meta = self.meta.clone();
        meta.shape.clear();
        meta.chunkshape.clear();
        meta.blockshape.clear();
        for (axis, &squeeze_axis) in axes.iter().enumerate() {
            if squeeze_axis {
                if self.meta.shape[axis] != 1 {
                    return Err("Cannot squeeze a non-singleton B2ND dimension");
                }
            } else {
                meta.shape.push(self.meta.shape[axis]);
                meta.chunkshape.push(self.meta.chunkshape[axis]);
                meta.blockshape.push(self.meta.blockshape[axis]);
            }
        }
        self.view_with_meta(meta)
    }

    /// Return a dense row-major buffer for the half-open item slice
    /// `start..stop` in each dimension.
    pub fn get_slice(&self, start: &[i64], stop: &[i64]) -> Result<Vec<u8>, &'static str> {
        let slice = validate_slice_bounds(&self.meta, start, stop)?;
        self.slice_to_dense_buffer(start, stop, &slice.extents_as_i64)
    }

    /// Return the linear chunk indexes touched by the half-open item slice
    /// `start..stop`, matching C-Blosc2 `b2nd_get_slice_nchunks`.
    pub fn get_slice_nchunks(&self, start: &[i64], stop: &[i64]) -> Result<Vec<i64>, &'static str> {
        let ndim = self.meta.ndim();
        if start.len() != ndim || stop.len() != ndim {
            return Err("B2ND slice rank does not match array rank");
        }
        if self.meta.nitems()? == 0 {
            return Ok(Vec::new());
        }
        let slice = validate_slice_bounds(&self.meta, start, stop)?;
        let chunk_grid = chunk_grid(&self.meta)?;
        let mut first = vec![0usize; ndim];
        let mut shape = vec![0usize; ndim];
        for dim in 0..self.meta.ndim() {
            let chunk = self.meta.chunkshape[dim] as usize;
            let slice_stop = slice.starts[dim]
                .checked_add(slice.extents[dim])
                .ok_or("Invalid B2ND slice bounds")?;
            let mut pos = 0usize;
            while pos <= slice.starts[dim] {
                pos = pos.checked_add(chunk).ok_or("B2ND chunk index overflow")?;
            }
            first[dim] = pos / chunk - 1;
            while pos < slice_stop {
                pos = pos.checked_add(chunk).ok_or("B2ND chunk index overflow")?;
            }
            shape[dim] = pos / chunk - first[dim];
        }
        product_usize(&shape)?;
        let mut out = Vec::new();
        collect_slice_chunks_c_filtered(
            0,
            &first,
            &shape,
            &mut vec![0usize; ndim],
            &chunk_grid,
            &self.meta,
            &slice,
            &mut out,
        )?;
        Ok(out)
    }

    /// Return the half-open item slice `start..stop` as a new b2nd array.
    ///
    /// The new array keeps this array's chunkshape, blockshape, dtype and
    /// compression/decompression parameters.
    pub fn slice(&self, start: &[i64], stop: &[i64]) -> Result<Self, &'static str> {
        let slice = validate_slice_bounds(&self.meta, start, stop)?;
        let data = self.slice_to_dense_buffer(start, stop, &slice.extents_as_i64)?;
        let meta = B2ndMeta::new(
            slice.extents_as_i64,
            self.meta.chunkshape.clone(),
            self.meta.blockshape.clone(),
            self.meta.dtype.clone(),
            self.meta.dtype_format,
        )?;
        Self::from_dense_buffer(
            meta,
            &data,
            self.schunk.cparams.clone(),
            self.schunk.dparams.clone(),
        )
    }

    /// Return the half-open item slice `start..stop` as a new b2nd array using
    /// caller-provided destination metadata and compression parameters.
    ///
    /// The returned shape is always `stop - start`; `meta` supplies chunkshape,
    /// blockshape, dtype and dtype_format, mirroring C-Blosc2's destination
    /// context behavior for `b2nd_get_slice`.
    pub fn slice_with_meta(
        &self,
        start: &[i64],
        stop: &[i64],
        meta: B2ndMeta,
        cparams: CParams,
        dparams: DParams,
    ) -> Result<Self, &'static str> {
        self.slice_with_meta_and_metalayers(start, stop, meta, cparams, dparams, &[])
    }

    /// Return the half-open item slice `start..stop` as a new b2nd array using
    /// caller-provided destination metadata, compression parameters and
    /// fixed-size user metalayers.
    ///
    /// `meta.shape` is overwritten with `stop - start`; `metalayers` are
    /// attached after the managed `b2nd` descriptor.
    pub fn slice_with_meta_and_metalayers(
        &self,
        start: &[i64],
        stop: &[i64],
        mut meta: B2ndMeta,
        cparams: CParams,
        dparams: DParams,
        metalayers: &[(&str, &[u8])],
    ) -> Result<Self, &'static str> {
        let slice = validate_slice_bounds(&self.meta, start, stop)?;
        let data = self.slice_to_dense_buffer(start, stop, &slice.extents_as_i64)?;
        meta.shape = slice.extents_as_i64;
        Self::from_cbuffer_with_metalayers(meta, &data, cparams, dparams, metalayers)
    }

    /// Deep-copy this array, preserving fixed and variable-length user
    /// metalayers.
    pub fn copy_array(&self) -> Result<Self, &'static str> {
        self.copy_with_meta(
            self.meta.clone(),
            self.schunk.cparams.clone(),
            self.schunk.dparams.clone(),
        )
    }

    /// Deep-copy this array using caller-provided destination chunk/block/dtype
    /// metadata and compression parameters.
    ///
    /// `meta.shape` is overwritten with the source shape, mirroring C-Blosc2's
    /// destination context behavior for `b2nd_copy`.
    pub fn copy_with_meta(
        &self,
        mut meta: B2ndMeta,
        cparams: CParams,
        dparams: DParams,
    ) -> Result<Self, &'static str> {
        meta.shape = self.meta.shape.clone();
        b2nd_validate_cparams_for_array(&cparams)?;
        if meta.chunkshape == self.meta.chunkshape && meta.blockshape == self.meta.blockshape {
            return Ok(Self::from_parts(
                meta,
                self.schunk.copy_schunk_with_params(cparams, dparams)?,
            ));
        }

        let fixed_metalayers = user_fixed_metalayers(&self.schunk);
        let mut copied = Self::from_cbuffer_with_metalayers(
            meta,
            &self.to_dense_buffer()?,
            cparams,
            dparams,
            &fixed_metalayers,
        )?;
        copy_vlmetalayers(&self.schunk, &mut copied.schunk)?;
        Ok(copied)
    }

    /// Concatenate two arrays along `axis`, returning a new array.
    ///
    /// The result uses this array's chunkshape, blockshape, dtype and
    /// compression parameters and preserves this array's user metalayers.
    pub fn concatenate(&self, other: &Self, axis: usize) -> Result<Self, &'static str> {
        self.concatenate_with_meta(
            other,
            axis,
            self.meta.clone(),
            self.schunk.cparams.clone(),
            self.schunk.dparams.clone(),
        )
    }

    /// Concatenate `other` onto this array along `axis`, mutating this array.
    ///
    /// This mirrors C-Blosc2's `b2nd_concatenate(..., copy=false, ...)` mode.
    /// Existing user metalayers on this array are preserved.
    pub fn concatenate_in_place(&mut self, other: &Self, axis: usize) -> Result<(), &'static str> {
        self.concatenated_shape(other, axis)?;
        let data = other.to_dense_buffer()?;
        let buffershape = other.meta.shape.clone();
        self.append(axis, &buffershape, &data)
    }

    /// Concatenate two arrays along `axis` using caller-provided destination
    /// metadata and compression parameters.
    ///
    /// `meta.shape` is overwritten with the concatenated shape. User metalayers
    /// are copied from the first array, matching C-Blosc2's documented result.
    pub fn concatenate_with_meta(
        &self,
        other: &Self,
        axis: usize,
        meta: B2ndMeta,
        cparams: CParams,
        dparams: DParams,
    ) -> Result<Self, &'static str> {
        let fixed_metalayers = user_fixed_metalayers(&self.schunk);
        self.concatenate_with_meta_and_metalayers(
            other,
            axis,
            meta,
            cparams,
            dparams,
            &fixed_metalayers,
        )
    }

    fn concatenate_with_meta_and_metalayers(
        &self,
        other: &Self,
        axis: usize,
        mut meta: B2ndMeta,
        cparams: CParams,
        dparams: DParams,
        fixed_metalayers: &[(&str, &[u8])],
    ) -> Result<Self, &'static str> {
        let ndim = self.meta.ndim();
        let new_shape = self.concatenated_shape(other, axis)?;
        meta.shape = new_shape.clone();
        b2nd_validate_cparams_for_array(&cparams)?;

        if let Some(result) = self.try_concatenate_raw_chunks_axis0(
            other,
            axis,
            &meta,
            cparams.clone(),
            dparams.clone(),
        )? {
            return Ok(result);
        }

        let mut result = Self::zeros_with_metalayers(meta, cparams, dparams, fixed_metalayers)?;
        copy_vlmetalayers(&self.schunk, &mut result.schunk)?;

        let mut start = vec![0i64; ndim];
        let mut stop = self.meta.shape.clone();
        result.set_slice(&start, &stop, &self.to_dense_buffer()?)?;
        start[axis] = self.meta.shape[axis];
        stop = new_shape;
        result.set_slice(&start, &stop, &other.to_dense_buffer()?)?;
        Ok(result)
    }

    fn try_concatenate_raw_chunks_axis0(
        &self,
        other: &Self,
        axis: usize,
        meta: &B2ndMeta,
        cparams: CParams,
        dparams: DParams,
    ) -> Result<Option<Self>, &'static str> {
        if axis != 0
            || self.meta.ndim() == 0
            || other.meta.ndim() != self.meta.ndim()
            || self.schunk.dparams.postfilter.is_some()
            || other.schunk.dparams.postfilter.is_some()
            || !cparams_raw_copy_compatible(&self.schunk.cparams, &cparams)
            || meta.chunkshape != self.meta.chunkshape
            || meta.blockshape != self.meta.blockshape
            || meta.dtype != self.meta.dtype
            || meta.dtype_format != self.meta.dtype_format
            || other.meta.chunkshape != self.meta.chunkshape
            || other.meta.blockshape != self.meta.blockshape
            || other.meta.dtype != self.meta.dtype
            || other.meta.dtype_format != self.meta.dtype_format
        {
            return Ok(None);
        }

        let chunkshape = self.meta.chunkshape[0];
        if chunkshape <= 0
            || self.meta.shape[0] % i64::from(chunkshape) != 0
            || other.meta.shape[0] % i64::from(chunkshape) != 0
        {
            return Ok(None);
        }

        let mut schunk = Schunk::new(cparams, dparams);
        schunk.add_metalayer(B2ND_METALAYER_NAME, &meta.serialize()?)?;
        add_fixed_metalayers(&mut schunk, &user_fixed_metalayers(&self.schunk))?;
        for chunk in self.schunk.compressed_chunks() {
            schunk.append_chunk(&chunk)?;
        }
        for chunk in other.schunk.compressed_chunks() {
            schunk.append_chunk(&chunk)?;
        }
        copy_vlmetalayers(&self.schunk, &mut schunk)?;
        Ok(Some(Self::from_parts(meta.clone(), schunk)))
    }

    fn concatenated_shape(&self, other: &Self, axis: usize) -> Result<Vec<i64>, &'static str> {
        let ndim = self.meta.ndim();
        if ndim == 0 || other.meta.ndim() == 0 {
            return Err("B2ND concatenation does not support scalar arrays");
        }
        if axis >= ndim || other.meta.ndim() != ndim {
            return Err("B2ND concatenate rank mismatch");
        }
        if self.schunk.cparams.typesize != other.schunk.cparams.typesize {
            return Err("B2ND concatenate typesize mismatch");
        }
        let mut new_shape = self.meta.shape.clone();
        for dim in 0..ndim {
            if dim == axis {
                new_shape[dim] = new_shape[dim]
                    .checked_add(other.meta.shape[dim])
                    .ok_or("B2ND shape too large")?;
            } else if self.meta.shape[dim] != other.meta.shape[dim] {
                return Err("B2ND concatenate shape mismatch");
            }
        }
        Ok(new_shape)
    }

    /// Return a dense row-major buffer with explicit buffer shape, filling the
    /// leading region with the half-open item slice and leaving padding zeroed.
    pub fn slice_to_dense_buffer(
        &self,
        start: &[i64],
        stop: &[i64],
        buffershape: &[i64],
    ) -> Result<Vec<u8>, &'static str> {
        let typesize = b2nd_cparams_typesize(&self.schunk.cparams)?;
        let slice = validate_slice_bounds(&self.meta, start, stop)?;
        if buffershape.len() != slice.extents.len() {
            return Err("B2ND buffer rank does not match array rank");
        }
        for (extent, &buffer_dim) in slice.extents_as_i64.iter().zip(buffershape) {
            if buffer_dim < *extent {
                return Err("B2ND buffer shape is smaller than slice shape");
            }
        }
        let out_len = product_i64(buffershape)?
            .checked_mul(typesize)
            .ok_or("B2ND slice too large")?;
        let mut out = vec![0u8; out_len];
        if slice.extents.iter().any(|&extent| extent == 0) {
            return Ok(out);
        }
        let coords: Vec<Vec<usize>> = slice
            .starts
            .iter()
            .zip(&slice.extents)
            .map(|(&start, &extent)| (start..start + extent).collect())
            .collect();
        self.read_orthogonal_selection_chunks(
            &coords,
            &slice.extents,
            buffershape,
            &mut out,
            typesize,
        )?;
        Ok(out)
    }

    fn preflight_dense_cbuffer_len(&self) -> Result<usize, &'static str> {
        self.meta.validate()?;
        let typesize = b2nd_cparams_typesize(&self.schunk.cparams)?;
        self.preflight_chunk_count()?;
        self.meta
            .nitems()?
            .checked_mul(typesize)
            .ok_or("B2ND buffer too large")
    }

    fn preflight_chunk_count(&self) -> Result<usize, &'static str> {
        let chunk_grid = chunk_grid(&self.meta)?;
        let chunk_count = product_usize(&chunk_grid)?;
        if (self.schunk.nchunks() as usize) < chunk_count {
            return Err("B2ND chunk count does not match metadata");
        }
        Ok(chunk_count)
    }

    fn preflight_slice_cbuffer_len_c(
        &self,
        start: &[i64],
        stop: &[i64],
        buffershape: &[i64],
    ) -> Result<usize, &'static str> {
        let typesize = b2nd_cparams_typesize(&self.schunk.cparams)?;
        let extents_as_i64 = slice_extents_without_source_bounds(&self.meta, start, stop)?;
        validate_slice_buffershape(&extents_as_i64, buffershape)?;
        if self.meta.nitems()? != 0 && extents_as_i64.iter().all(|&extent| extent != 0) {
            validate_slice_bounds(&self.meta, start, stop)?;
        }
        product_i64(buffershape)?
            .checked_mul(typesize)
            .ok_or("B2ND slice too large")
    }

    /// Overwrite the half-open item slice `start..stop` from a dense row-major
    /// source buffer whose shape is `stop - start`.
    pub fn set_slice(
        &mut self,
        start: &[i64],
        stop: &[i64],
        data: &[u8],
    ) -> Result<(), &'static str> {
        let slice = validate_slice_bounds(&self.meta, start, stop)?;
        self.set_slice_from_dense_buffer(start, stop, &slice.extents_as_i64, data)
    }

    /// Overwrite the half-open item slice from the leading region of a dense
    /// row-major source buffer with explicit buffer shape.
    pub fn set_slice_from_dense_buffer(
        &mut self,
        start: &[i64],
        stop: &[i64],
        buffershape: &[i64],
        data: &[u8],
    ) -> Result<(), &'static str> {
        let typesize = b2nd_cparams_typesize(&self.schunk.cparams)?;
        let slice = validate_slice_bounds(&self.meta, start, stop)?;
        if buffershape.len() != slice.extents.len() {
            return Err("B2ND buffer rank does not match array rank");
        }
        for (extent, &buffer_dim) in slice.extents_as_i64.iter().zip(buffershape) {
            if buffer_dim < *extent {
                return Err("B2ND buffer shape is smaller than slice shape");
            }
        }
        if slice.extents.iter().any(|&extent| extent == 0) {
            return Ok(());
        }
        let required_len = dense_region_required_len(buffershape, &slice.extents, typesize)?;
        if data.len() < required_len {
            return Err("B2ND slice buffer size does not match slice shape and typesize");
        }

        self.transactional_mutation(|array| {
            array.update_slice_chunks_from_dense(&slice, buffershape, data, typesize)
        })
    }

    /// Return a dense row-major buffer for an orthogonal selection.
    pub fn select_orthogonal(&self, selection: &[Vec<i64>]) -> Result<Vec<u8>, &'static str> {
        let (coords, extents, out_shape) = validate_orthogonal_selection(&self.meta, selection)?;
        self.get_orthogonal_selection_cbuffer_with_validated(&coords, &extents, &out_shape)
    }

    /// Return a dense row-major buffer for an orthogonal selection with an
    /// explicit output buffer shape.
    pub fn orthogonal_selection_to_dense_buffer(
        &self,
        selection: &[Vec<i64>],
        buffershape: &[i64],
    ) -> Result<Vec<u8>, &'static str> {
        let (coords, extents, _) = validate_orthogonal_selection(&self.meta, selection)?;
        self.get_orthogonal_selection_cbuffer_with_validated(&coords, &extents, buffershape)
    }

    fn get_orthogonal_selection_cbuffer_with_validated(
        &self,
        coords: &[Vec<usize>],
        extents: &[usize],
        buffershape: &[i64],
    ) -> Result<Vec<u8>, &'static str> {
        let typesize = b2nd_cparams_typesize(&self.schunk.cparams)?;
        validate_orthogonal_buffershape(buffershape, extents)?;
        let out_len = product_i64(buffershape)?
            .checked_mul(typesize)
            .ok_or("B2ND selection too large")?;
        let mut out = vec![0u8; out_len];
        if extents.iter().any(|&extent| extent == 0) {
            return Ok(out);
        }
        self.read_orthogonal_selection_chunks(coords, extents, buffershape, &mut out, typesize)?;
        Ok(out)
    }

    fn read_orthogonal_selection_chunks(
        &self,
        coords: &[Vec<usize>],
        extents: &[usize],
        buffershape: &[i64],
        out: &mut [u8],
        typesize: usize,
    ) -> Result<(), &'static str> {
        let chunk_grid = chunk_grid(&self.meta)?;
        let chunk_count = product_usize(&chunk_grid)?;
        if (self.schunk.nchunks() as usize) < chunk_count {
            return Err("B2ND chunk count does not match metadata");
        }
        let layout = B2ndLayout::new(&self.meta, typesize)?;
        let chunk_nbytes = extchunk_nitems(&self.meta)?
            .checked_mul(typesize)
            .ok_or("B2ND chunk too large")?;
        let nblocks = product_usize(&layout.blocks_in_chunk)?;
        let dst_strides = byte_strides_i64(buffershape, typesize)?;
        let mut touched_chunks = BTreeMap::new();
        let mut idx = vec![0usize; coords.len()];
        collect_orthogonal_chunk_reads(
            0,
            coords,
            extents,
            &mut idx,
            &self.meta,
            &layout,
            &chunk_grid,
            nblocks,
            &dst_strides,
            &mut touched_chunks,
        )?;

        for (linear_chunk, read) in touched_chunks {
            let mut dparams = self.schunk.dparams.clone();
            dparams.nchunk = linear_chunk as i64;
            dparams.block_maskout = Some(read.maskout);
            dparams.b2nd_metalayer = b2nd_metalayer_for_schunk(&self.schunk);
            let compressed_chunk_bytes = self
                .schunk
                .compressed_chunk_bytes_owned(linear_chunk as i64)?;
            let chunk = compress::decompress_chunk_with_dparams(&compressed_chunk_bytes, &dparams)?;
            if chunk.len() < chunk_nbytes
                || (!self.allow_oversized_chunks && chunk.len() != chunk_nbytes)
            {
                return Err("B2ND chunk size does not match metadata");
            }
            for item in read.items {
                let src = b2nd_chunk_offset(
                    &item.local_idx,
                    &layout.extchunkshape,
                    &self.meta.blockshape,
                    &layout.blocks_in_chunk,
                    layout.block_nitems,
                    layout.typesize,
                )?;
                let src_end = src.checked_add(typesize).ok_or("B2ND copy overflow")?;
                let dst_end = item
                    .dst_offset
                    .checked_add(typesize)
                    .ok_or("B2ND copy overflow")?;
                let src_item = chunk.get(src..src_end).ok_or("B2ND source too small")?;
                let dst_item = out
                    .get_mut(item.dst_offset..dst_end)
                    .ok_or("B2ND destination too small")?;
                dst_item.copy_from_slice(src_item);
            }
        }
        Ok(())
    }

    /// Overwrite an orthogonal selection from a dense row-major source buffer.
    pub fn set_orthogonal_selection(
        &mut self,
        selection: &[Vec<i64>],
        data: &[u8],
    ) -> Result<(), &'static str> {
        let (coords, extents, out_shape) = validate_orthogonal_selection(&self.meta, selection)?;
        self.set_orthogonal_selection_cbuffer_with_validated(&coords, &extents, &out_shape, data)
    }

    /// Overwrite an orthogonal selection from the leading region of a dense
    /// row-major source buffer with explicit buffer shape.
    pub fn set_orthogonal_selection_from_dense_buffer(
        &mut self,
        selection: &[Vec<i64>],
        buffershape: &[i64],
        data: &[u8],
    ) -> Result<(), &'static str> {
        let (coords, extents, _) = validate_orthogonal_selection(&self.meta, selection)?;
        self.set_orthogonal_selection_cbuffer_with_validated(&coords, &extents, buffershape, data)
    }

    fn set_orthogonal_selection_cbuffer_with_validated(
        &mut self,
        coords: &[Vec<usize>],
        extents: &[usize],
        buffershape: &[i64],
        data: &[u8],
    ) -> Result<(), &'static str> {
        let typesize = b2nd_cparams_typesize(&self.schunk.cparams)?;
        validate_orthogonal_buffershape(buffershape, extents)?;
        if extents.iter().any(|&extent| extent == 0) {
            if !data.is_empty() {
                return Err(
                    "B2ND selection buffer size does not match selection shape and typesize",
                );
            }
            return Ok(());
        }
        let required_len = dense_region_required_len(buffershape, extents, typesize)?;
        if data.len() != required_len {
            return Err("B2ND selection buffer size does not match selection shape and typesize");
        }
        self.transactional_mutation(|array| {
            array.update_orthogonal_chunks_from_dense(coords, extents, buffershape, data, typesize)
        })
    }

    fn set_orthogonal_selection_cbuffer_c_from(
        &mut self,
        selection: &[Vec<i64>],
        buffershape: &[i64],
        data: &[u8],
        buffersize: usize,
    ) -> Result<(), &'static str> {
        let (coords, extents, _) = validate_orthogonal_selection_c(&self.meta, selection)?;
        validate_orthogonal_buffershape_c(buffershape, &extents)?;
        let typesize = b2nd_cparams_typesize(&self.schunk.cparams)?;
        let selection_nbytes = product_usize(&extents)?
            .checked_mul(typesize)
            .ok_or("B2ND selection too large")?;
        if buffersize > selection_nbytes {
            return Err("B2ND selection buffer size does not match selection shape and typesize");
        }
        if extents.iter().any(|&extent| extent == 0) {
            return Ok(());
        }
        let required_len = dense_region_required_len(buffershape, &extents, typesize)?;
        let data = data
            .get(..required_len)
            .ok_or("B2ND selection buffer size does not match selection shape and typesize")?;
        self.transactional_mutation(|array| {
            array.update_orthogonal_chunks_from_dense(
                coords.as_slice(),
                &extents,
                buffershape,
                data,
                typesize,
            )
        })
    }

    /// Resize the array, preserving the overlapping prefix region and zero-filling
    /// new cells.
    pub fn resize(&mut self, new_shape: Vec<i64>) -> Result<(), &'static str> {
        self.resize_with_start(new_shape, None)
    }

    /// Resize the array at `start`, following C-Blosc2 `b2nd_resize`
    /// semantics. `None` resizes at the array end in each dimension.
    pub fn resize_with_start(
        &mut self,
        new_shape: Vec<i64>,
        start: Option<&[i64]>,
    ) -> Result<(), &'static str> {
        let mut new_meta = self.meta.clone();
        new_meta.shape = new_shape;
        new_meta.validate()?;
        let resize = validate_resize_at(&self.meta, &new_meta.shape, start)?;

        if chunk_grid(&self.meta)? == chunk_grid(&new_meta)? {
            self.resize_same_chunk_grid(new_meta, &resize)?;
            return Ok(());
        }

        self.resize_by_chunk_mutation(new_meta, &resize)
    }

    fn resize_same_chunk_grid(
        &mut self,
        new_meta: B2ndMeta,
        resize: &B2ndResize,
    ) -> Result<(), &'static str> {
        self.transactional_mutation(|array| array.resize_same_chunk_grid_inner(new_meta, resize))
    }

    fn resize_same_chunk_grid_inner(
        &mut self,
        new_meta: B2ndMeta,
        resize: &B2ndResize,
    ) -> Result<(), &'static str> {
        self.zero_new_logical_cells_same_grid(&new_meta, resize)?;
        self.update_meta_preserving_chunks_inner(new_meta)
    }

    fn zero_new_logical_cells_same_grid(
        &mut self,
        new_meta: &B2ndMeta,
        resize: &B2ndResize,
    ) -> Result<(), &'static str> {
        let old_meta = self.meta.clone();
        let ndim = old_meta.ndim();
        let mut growth = vec![None; ndim];
        let mut has_growth = false;
        for axis in 0..ndim {
            if new_meta.shape[axis] > old_meta.shape[axis] {
                let start = resize.starts[axis];
                let stop = start
                    .checked_add((new_meta.shape[axis] - old_meta.shape[axis]) as usize)
                    .ok_or("Invalid B2ND resize shape")?;
                growth[axis] = Some(start..stop);
                has_growth = true;
            }
        }
        if !has_growth {
            return Ok(());
        }

        let chunk_grid = chunk_grid(new_meta)?;
        let chunk_count = product_usize(&chunk_grid)?;
        if self.schunk.nchunks() as usize != chunk_count {
            return Err("B2ND chunk count does not match metadata");
        }
        if chunk_count == 0 {
            return Ok(());
        }

        let typesize = b2nd_cparams_typesize(&self.schunk.cparams)?;
        let layout = B2ndLayout::new(new_meta, typesize)?;
        let chunk_nbytes = extchunk_nitems(new_meta)?
            .checked_mul(typesize)
            .ok_or("B2ND chunk too large")?;

        for linear_chunk in 0..chunk_count {
            let chunk_index = unravel_index(linear_chunk, &chunk_grid);
            if !chunk_intersects_growth_region(new_meta, &chunk_index, &growth)? {
                continue;
            }
            let mut chunk = self.schunk.decompress_chunk(linear_chunk as i64)?;
            if chunk.len() != chunk_nbytes {
                return Err("B2ND chunk size does not match metadata");
            }
            zero_growth_cells_in_chunk(
                new_meta,
                &layout,
                &chunk_index,
                &growth,
                &mut chunk,
                typesize,
            )?;
            self.schunk.update_chunk(linear_chunk as i64, &chunk)?;
        }
        Ok(())
    }

    fn resize_by_chunk_mutation(
        &mut self,
        new_meta: B2ndMeta,
        resize: &B2ndResize,
    ) -> Result<(), &'static str> {
        self.transactional_mutation(|array| array.resize_by_chunk_mutation_inner(new_meta, resize))
    }

    fn resize_by_chunk_mutation_inner(
        &mut self,
        new_meta: B2ndMeta,
        resize: &B2ndResize,
    ) -> Result<(), &'static str> {
        let old_meta = self.meta.clone();
        if old_meta.chunkshape != new_meta.chunkshape
            || old_meta.blockshape != new_meta.blockshape
            || old_meta.dtype != new_meta.dtype
            || old_meta.dtype_format != new_meta.dtype_format
        {
            return Err("B2ND resize metadata is not chunk-compatible");
        }

        let old_grid = chunk_grid(&old_meta)?;
        let new_grid = chunk_grid(&new_meta)?;
        if self.schunk.nchunks() as usize != product_usize(&old_grid)? {
            return Err("B2ND chunk count does not match shape");
        }

        let shrunk_shape: Vec<i64> = old_meta
            .shape
            .iter()
            .zip(&new_meta.shape)
            .map(|(&old_dim, &new_dim)| old_dim.min(new_dim))
            .collect();

        let old_count = product_usize(&old_grid)?;
        for linear_chunk in (0..old_count).rev() {
            let chunk_index = unravel_index(linear_chunk, &old_grid);
            if chunk_origin_in_resize_region(
                &chunk_index,
                &old_meta.chunkshape,
                resize,
                &old_meta.shape,
                &shrunk_shape,
            )? {
                self.schunk.delete_chunk(linear_chunk as i64)?;
            }
        }

        let typesize = b2nd_cparams_typesize(&self.schunk.cparams)?;
        let zero_chunk_nbytes = extchunk_nitems(&new_meta)?
            .checked_mul(typesize)
            .ok_or("B2ND chunk too large")?;
        let new_count = product_usize(&new_grid)?;
        for linear_chunk in 0..new_count {
            let chunk_index = unravel_index(linear_chunk, &new_grid);
            if chunk_origin_in_resize_region(
                &chunk_index,
                &new_meta.chunkshape,
                resize,
                &new_meta.shape,
                &shrunk_shape,
            )? {
                self.schunk
                    .insert_special_zero_chunk(linear_chunk as i64, zero_chunk_nbytes)?;
            }
        }

        if self.schunk.nchunks() as usize != new_count {
            return Err("B2ND resized chunk count does not match shape");
        }
        self.update_meta_preserving_chunks(new_meta)
    }

    fn update_meta_preserving_chunks(&mut self, meta: B2ndMeta) -> Result<(), &'static str> {
        self.transactional_mutation(|array| array.update_meta_preserving_chunks_inner(meta))
    }

    fn update_meta_preserving_chunks_inner(&mut self, meta: B2ndMeta) -> Result<(), &'static str> {
        let encoded = meta.serialize()?;
        if self.schunk.metalayer(B2ND_METALAYER_NAME).is_some() {
            self.schunk
                .update_metalayer(B2ND_METALAYER_NAME, &encoded)?;
        } else {
            self.schunk.add_metalayer(B2ND_METALAYER_NAME, &encoded)?;
        }
        self.schunk.cparams.b2nd_metalayer = Some(encoded.clone());
        self.schunk.dparams.b2nd_metalayer = Some(encoded);
        self.meta = meta;
        Ok(())
    }

    fn transactional_mutation<F>(&mut self, mutation: F) -> Result<(), &'static str>
    where
        F: FnOnce(&mut Self) -> Result<(), &'static str>,
    {
        let mut snapshot = self.clone();
        snapshot.schunk = self.schunk.transactional_snapshot();
        if let Err(err) = mutation(self) {
            let changed = self.transactional_public_state_changed(&snapshot);
            self.restore_transactional_public_state(snapshot);
            if changed {
                let _ = self.persist_b2nd_attached_frame();
            }
            return Err(err);
        }
        Ok(())
    }

    fn transactional_public_state_changed(&self, snapshot: &Self) -> bool {
        self.meta != snapshot.meta || self.schunk.transactional_state_changed(&snapshot.schunk)
    }

    fn restore_transactional_public_state(&mut self, snapshot: Self) {
        self.meta = snapshot.meta;
        self.schunk.restore_transactional_snapshot(snapshot.schunk);
        self.attached_frame = snapshot.attached_frame;
        self.allow_oversized_chunks = snapshot.allow_oversized_chunks;
    }

    fn persist_b2nd_attached_frame(&self) -> Result<(), &'static str> {
        let Some(attached) = &self.attached_frame else {
            return Ok(());
        };
        match attached.storage {
            FrameStorage::Contiguous => self
                .schunk
                .write_attached_contiguous_frame_at(&attached.path, attached.offset, None)
                .map(|_| ())
                .map_err(|_| "Failed to write attached frame"),
            FrameStorage::Sparse => self
                .schunk
                .write_attached_sparse_frame_at(&attached.path, attached.offset, None)
                .map(|_| ())
                .map_err(|_| "Failed to write attached frame"),
        }
    }

    fn ensure_viewable_metalayers(&self) -> Result<(), &'static str> {
        if self
            .schunk
            .metalayers
            .iter()
            .any(|layer| layer.name != B2ND_METALAYER_NAME)
        {
            return Err("Cannot create a B2ND view with non-b2nd metalayers");
        }
        if self.schunk.metalayer(B2ND_METALAYER_NAME).is_none() {
            return Err("Schunk does not contain a B2ND metalayer");
        }
        Ok(())
    }

    fn view_with_meta(&self, meta: B2ndMeta) -> Result<Self, &'static str> {
        meta.validate()?;
        let new_chunk_count = product_usize(&chunk_grid(&meta)?)?;
        let old_chunk_count = product_usize(&chunk_grid(&self.meta)?)?;
        let new_extchunk_nitems = extchunk_nitems(&meta)?;
        let old_extchunk_nitems = extchunk_nitems(&self.meta)?;
        if new_chunk_count != old_chunk_count || new_extchunk_nitems > old_extchunk_nitems {
            return Err("B2ND view metadata is not chunk-compatible");
        }
        let encoded = meta.serialize()?;
        let mut view = Self::from_parts(meta.clone(), self.schunk.clone_with_shared_chunks());
        view.allow_oversized_chunks =
            self.allow_oversized_chunks || new_extchunk_nitems < old_extchunk_nitems;
        view.schunk.remove_metalayer(B2ND_METALAYER_NAME);
        view.schunk.add_metalayer(B2ND_METALAYER_NAME, &encoded)?;
        Ok(view)
    }

    /// Insert a dense row-major buffer along one axis.
    pub fn insert(
        &mut self,
        axis: usize,
        start: i64,
        buffershape: &[i64],
        data: &[u8],
    ) -> Result<(), &'static str> {
        let ndim = self.meta.ndim();
        if axis >= ndim || buffershape.len() != ndim {
            return Err("B2ND insert rank does not match array rank");
        }
        if start < 0 || start > self.meta.shape[axis] || buffershape[axis] < 0 {
            return Err("Invalid B2ND insert bounds");
        }
        for (dim, &buffer_dim) in buffershape.iter().enumerate() {
            if dim != axis && buffer_dim != self.meta.shape[dim] {
                return Err("B2ND insert buffer shape does not match array shape");
            }
        }

        let mut new_shape = self.meta.shape.clone();
        new_shape[axis] = new_shape[axis]
            .checked_add(buffershape[axis])
            .ok_or("B2ND shape too large")?;
        validate_insert_buffer_size(
            b2nd_cparams_typesize(&self.schunk.cparams)?,
            buffershape,
            data,
        )?;
        self.transactional_mutation(|array| {
            if start == array.meta.shape[axis] {
                array.resize_with_start(new_shape, None)?;
            } else {
                let mut resize_start = vec![0i64; ndim];
                resize_start[axis] = start;
                array.resize_with_start(new_shape, Some(&resize_start))?;
            }

            let mut slice_start = vec![0i64; ndim];
            let mut slice_stop = array.meta.shape.clone();
            slice_start[axis] = start;
            slice_stop[axis] = start + buffershape[axis];
            array.set_slice_from_dense_buffer(&slice_start, &slice_stop, buffershape, data)
        })
    }

    /// Insert a dense row-major buffer along one axis, deriving the inserted
    /// extent from `data.len()` like C-Blosc2 `b2nd_insert`.
    pub fn insert_dense_buffer(
        &mut self,
        axis: usize,
        start: i64,
        data: &[u8],
    ) -> Result<(), &'static str> {
        let buffershape = self.axis_buffer_shape(axis, data.len())?;
        self.insert(axis, start, &buffershape, data)
    }

    /// Append a dense row-major buffer to the end of one axis.
    pub fn append(
        &mut self,
        axis: usize,
        buffershape: &[i64],
        data: &[u8],
    ) -> Result<(), &'static str> {
        if axis >= self.meta.ndim() {
            return Err("B2ND append axis out of range");
        }
        if self.try_append_full_chunk(axis, Some(buffershape), data)? {
            return Ok(());
        }
        self.insert(axis, self.meta.shape[axis], buffershape, data)
    }

    /// Append a dense row-major buffer to the end of one axis, deriving the
    /// appended extent from `data.len()` like C-Blosc2 `b2nd_append`.
    pub fn append_dense_buffer(&mut self, axis: usize, data: &[u8]) -> Result<(), &'static str> {
        if self.try_append_full_chunk(axis, None, data)? {
            return Ok(());
        }
        let buffershape = self.axis_buffer_shape(axis, data.len())?;
        self.append(axis, &buffershape, data)
    }

    fn try_append_full_chunk(
        &mut self,
        axis: usize,
        buffershape: Option<&[i64]>,
        data: &[u8],
    ) -> Result<bool, &'static str> {
        if axis != 0 || self.meta.ndim() == 0 || axis >= self.meta.ndim() {
            return Ok(false);
        }
        if let Some(buffershape) = buffershape {
            if buffershape.len() != self.meta.ndim()
                || buffershape
                    .iter()
                    .zip(&self.meta.chunkshape)
                    .any(|(&buffer_dim, &chunk_dim)| buffer_dim != i64::from(chunk_dim))
            {
                return Ok(false);
            }
        }
        if self
            .meta
            .chunkshape
            .iter()
            .zip(&self.meta.blockshape)
            .enumerate()
            .skip(1)
            .any(|(_, (&chunk, &block))| chunk != block)
        {
            return Ok(false);
        }
        let typesize = b2nd_cparams_typesize(&self.schunk.cparams)?;
        let chunk_nbytes = extchunk_nitems(&self.meta)?
            .checked_mul(typesize)
            .ok_or("B2ND chunk too large")?;
        if data.len() != chunk_nbytes {
            return Ok(false);
        }
        let axis_growth = i64::from(self.meta.chunkshape[0]);
        if axis_growth <= 0 {
            return Ok(false);
        }
        let old_meta = self.meta.clone();
        let mut new_meta = self.meta.clone();
        new_meta.shape[0] = new_meta.shape[0]
            .checked_add(axis_growth)
            .ok_or("B2ND shape too large")?;
        new_meta.validate()?;
        self.transactional_mutation(|array| {
            array.schunk.append_buffer(data)?;
            let chunks_after_append = array.schunk.nchunks() as usize;
            let new_grid = chunk_grid(&new_meta)?;
            let new_count = product_usize(&new_grid)?;
            if chunks_after_append < new_count {
                let resize = B2ndResize {
                    starts: old_meta
                        .shape
                        .iter()
                        .map(|&dim| usize::try_from(dim).map_err(|_| "Invalid B2ND resize shape"))
                        .collect::<Result<_, _>>()?,
                };
                let zero_chunk_nbytes = extchunk_nitems(&new_meta)?
                    .checked_mul(typesize)
                    .ok_or("B2ND chunk too large")?;
                for linear_chunk in 0..new_count {
                    let chunk_index = unravel_index(linear_chunk, &new_grid);
                    if chunk_origin_in_resize_region(
                        &chunk_index,
                        &new_meta.chunkshape,
                        &resize,
                        &new_meta.shape,
                        &old_meta.shape,
                    )? {
                        array
                            .schunk
                            .insert_special_zero_chunk(linear_chunk as i64, zero_chunk_nbytes)?;
                    }
                }
            }
            array.update_meta_preserving_chunks_inner(new_meta)
        })?;
        Ok(true)
    }

    /// Delete `len` items along one axis starting at `start`.
    pub fn delete(&mut self, axis: usize, start: i64, len: i64) -> Result<(), &'static str> {
        let ndim = self.meta.ndim();
        let end = start.checked_add(len).ok_or("Invalid B2ND delete bounds")?;
        if axis >= ndim || start < 0 || len < 0 || end > self.meta.shape[axis] {
            return Err("Invalid B2ND delete bounds");
        }
        let mut new_shape = self.meta.shape.clone();
        new_shape[axis] = new_shape[axis]
            .checked_sub(len)
            .ok_or("Invalid B2ND delete bounds")?;
        if end == self.meta.shape[axis] {
            self.resize_with_start(new_shape, None)
        } else {
            let mut resize_start = vec![0i64; ndim];
            resize_start[axis] = start;
            self.resize_with_start(new_shape, Some(&resize_start))
        }
    }

    fn axis_buffer_shape(&self, axis: usize, data_len: usize) -> Result<Vec<i64>, &'static str> {
        let ndim = self.meta.ndim();
        if axis >= ndim {
            return Err("B2ND axis out of range");
        }
        let typesize = b2nd_cparams_typesize(&self.schunk.cparams)?;
        if typesize == 0 || !data_len.is_multiple_of(typesize) {
            return Err("B2ND buffer size does not match array shape and typesize");
        }
        let mut axis_items = typesize;
        let mut buffershape = self.meta.shape.clone();
        for dim in 0..ndim {
            if dim != axis {
                axis_items = axis_items
                    .checked_mul(self.meta.shape[dim] as usize)
                    .ok_or("B2ND buffer too large")?;
            }
        }
        if axis_items == 0 || !data_len.is_multiple_of(axis_items) {
            return Err("B2ND buffer size does not match array shape and typesize");
        }
        buffershape[axis] = (data_len / axis_items) as i64;
        Ok(buffershape)
    }

    fn update_slice_chunks_from_dense(
        &mut self,
        slice: &B2ndSlice,
        buffershape: &[i64],
        data: &[u8],
        typesize: usize,
    ) -> Result<(), &'static str> {
        let chunk_grid = chunk_grid(&self.meta)?;
        let chunk_count = product_usize(&chunk_grid)?;
        if (self.schunk.nchunks() as usize) < chunk_count {
            return Err("B2ND chunk count does not match metadata");
        }
        if chunk_count == 0 {
            return Ok(());
        }

        let ndim = self.meta.ndim();
        let layout = B2ndLayout::new(&self.meta, typesize)?;
        let chunk_nbytes = extchunk_nitems(&self.meta)?
            .checked_mul(typesize)
            .ok_or("B2ND chunk too large")?;
        let src_strides = byte_strides_i64(buffershape, typesize)?;

        let mut first_chunk = vec![0usize; ndim];
        let mut last_chunk = vec![0usize; ndim];
        for dim in 0..ndim {
            let chunk = self.meta.chunkshape[dim] as usize;
            first_chunk[dim] = slice.starts[dim] / chunk;
            last_chunk[dim] = (slice.starts[dim] + slice.extents[dim] - 1) / chunk;
        }

        let mut chunk_index = first_chunk.clone();
        self.update_slice_chunks_from_dense_inner(
            0,
            &first_chunk,
            &last_chunk,
            &mut chunk_index,
            slice,
            data,
            &src_strides,
            &layout,
            chunk_nbytes,
            &chunk_grid,
            typesize,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn update_slice_chunks_from_dense_inner(
        &mut self,
        dim: usize,
        first_chunk: &[usize],
        last_chunk: &[usize],
        chunk_index: &mut [usize],
        slice: &B2ndSlice,
        data: &[u8],
        src_strides: &[usize],
        layout: &B2ndLayout,
        chunk_nbytes: usize,
        chunk_grid: &[usize],
        typesize: usize,
    ) -> Result<(), &'static str> {
        if dim < chunk_index.len() {
            for value in first_chunk[dim]..=last_chunk[dim] {
                chunk_index[dim] = value;
                self.update_slice_chunks_from_dense_inner(
                    dim + 1,
                    first_chunk,
                    last_chunk,
                    chunk_index,
                    slice,
                    data,
                    src_strides,
                    layout,
                    chunk_nbytes,
                    chunk_grid,
                    typesize,
                )?;
            }
            return Ok(());
        }

        let ndim = self.meta.ndim();
        let mut intersection_start = vec![0usize; ndim];
        let mut intersection_extents = vec![0usize; ndim];
        let mut chunk_local_start = vec![0usize; ndim];
        let mut src_start = vec![0usize; ndim];
        let mut covers_full_logical_chunk = true;
        for axis in 0..ndim {
            let chunk_start = chunk_index[axis]
                .checked_mul(self.meta.chunkshape[axis] as usize)
                .ok_or("B2ND chunk index overflow")?;
            let chunk_stop = chunk_start
                .checked_add(self.meta.chunkshape[axis] as usize)
                .ok_or("B2ND chunk index overflow")?;
            let slice_start = slice.starts[axis];
            let slice_stop = slice.starts[axis]
                .checked_add(slice.extents[axis])
                .ok_or("Invalid B2ND slice bounds")?;
            let start = chunk_start.max(slice_start);
            let stop = chunk_stop.min(slice_stop);
            let logical_chunk_stop = chunk_stop.min(self.meta.shape[axis] as usize);
            if start != chunk_start || stop != logical_chunk_stop {
                covers_full_logical_chunk = false;
            }
            intersection_start[axis] = start;
            intersection_extents[axis] = stop - start;
            chunk_local_start[axis] = start - chunk_start;
            src_start[axis] = start - slice_start;
        }

        let linear_chunk = ravel_index(chunk_index, chunk_grid)?;
        let mut chunk = if covers_full_logical_chunk {
            vec![0u8; chunk_nbytes]
        } else {
            self.schunk.decompress_chunk(linear_chunk as i64)?
        };
        if chunk.len() != chunk_nbytes {
            return Err("B2ND chunk size does not match metadata");
        }
        copy_region(
            0,
            &intersection_extents,
            |idx| {
                let src = dense_offset(&src_start, idx, src_strides)?;
                let mut local_idx = vec![0usize; ndim];
                for axis in 0..ndim {
                    local_idx[axis] = chunk_local_start[axis] + idx[axis];
                }
                let dst = b2nd_chunk_offset(
                    &local_idx,
                    &layout.extchunkshape,
                    &self.meta.blockshape,
                    &layout.blocks_in_chunk,
                    layout.block_nitems,
                    layout.typesize,
                )?;
                Ok((src, dst))
            },
            data,
            &mut chunk,
            typesize,
        )?;
        self.schunk.update_chunk(linear_chunk as i64, &chunk)?;
        Ok(())
    }

    fn update_orthogonal_chunks_from_dense(
        &mut self,
        coords: &[Vec<usize>],
        extents: &[usize],
        buffershape: &[i64],
        data: &[u8],
        typesize: usize,
    ) -> Result<(), &'static str> {
        let chunk_grid = chunk_grid(&self.meta)?;
        let chunk_count = product_usize(&chunk_grid)?;
        if (self.schunk.nchunks() as usize) < chunk_count {
            return Err("B2ND chunk count does not match metadata");
        }
        if chunk_count == 0 {
            return Ok(());
        }

        let layout = B2ndLayout::new(&self.meta, typesize)?;
        let chunk_nbytes = extchunk_nitems(&self.meta)?
            .checked_mul(typesize)
            .ok_or("B2ND chunk too large")?;
        let src_strides = byte_strides_i64(buffershape, typesize)?;
        let src_zero = vec![0usize; coords.len()];
        let mut touched_chunks: BTreeMap<usize, Vec<u8>> = BTreeMap::new();
        let mut idx = vec![0usize; coords.len()];

        self.update_orthogonal_chunks_from_dense_inner(
            0,
            extents,
            &mut idx,
            coords,
            data,
            &src_strides,
            &src_zero,
            &layout,
            chunk_nbytes,
            &chunk_grid,
            &mut touched_chunks,
            typesize,
        )?;

        for (linear_chunk, chunk) in touched_chunks {
            self.schunk.update_chunk(linear_chunk as i64, &chunk)?;
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn update_orthogonal_chunks_from_dense_inner(
        &self,
        dim: usize,
        extents: &[usize],
        idx: &mut [usize],
        coords: &[Vec<usize>],
        data: &[u8],
        src_strides: &[usize],
        src_zero: &[usize],
        layout: &B2ndLayout,
        chunk_nbytes: usize,
        chunk_grid: &[usize],
        touched_chunks: &mut BTreeMap<usize, Vec<u8>>,
        typesize: usize,
    ) -> Result<(), &'static str> {
        if dim < extents.len() {
            for value in 0..extents[dim] {
                idx[dim] = value;
                self.update_orthogonal_chunks_from_dense_inner(
                    dim + 1,
                    extents,
                    idx,
                    coords,
                    data,
                    src_strides,
                    src_zero,
                    layout,
                    chunk_nbytes,
                    chunk_grid,
                    touched_chunks,
                    typesize,
                )?;
            }
            return Ok(());
        }

        let ndim = self.meta.ndim();
        let mut chunk_index = vec![0usize; ndim];
        let mut local_idx = vec![0usize; ndim];
        for axis in 0..ndim {
            let coord = coords[axis][idx[axis]];
            let chunk = self.meta.chunkshape[axis] as usize;
            // C adapters accept coord == shape. If that lands beyond the
            // chunk grid, Rust represents the one-past cell as virtual padding.
            if coord / chunk >= chunk_grid[axis] {
                return Ok(());
            }
            chunk_index[axis] = coord / chunk;
            local_idx[axis] = coord % chunk;
        }
        let linear_chunk = ravel_index(&chunk_index, chunk_grid)?;
        if let std::collections::btree_map::Entry::Vacant(entry) =
            touched_chunks.entry(linear_chunk)
        {
            let chunk = self.schunk.decompress_chunk(linear_chunk as i64)?;
            if chunk.len() != chunk_nbytes {
                return Err("B2ND chunk size does not match metadata");
            }
            entry.insert(chunk);
        }

        let src = dense_offset(src_zero, idx, src_strides)?;
        let dst = b2nd_chunk_offset(
            &local_idx,
            &layout.extchunkshape,
            &self.meta.blockshape,
            &layout.blocks_in_chunk,
            layout.block_nitems,
            layout.typesize,
        )?;
        let src_end = src.checked_add(typesize).ok_or("B2ND copy overflow")?;
        let dst_end = dst.checked_add(typesize).ok_or("B2ND copy overflow")?;
        let src_item = data.get(src..src_end).ok_or("B2ND source too small")?;
        let chunk = touched_chunks
            .get_mut(&linear_chunk)
            .ok_or("B2ND chunk index out of range")?;
        let dst_item = chunk
            .get_mut(dst..dst_end)
            .ok_or("B2ND destination too small")?;
        dst_item.copy_from_slice(src_item);
        Ok(())
    }
}

fn add_fixed_metalayers(
    schunk: &mut Schunk,
    metalayers: &[(&str, &[u8])],
) -> Result<(), &'static str> {
    for &(name, content) in metalayers {
        if name == B2ND_METALAYER_NAME {
            return Err("B2ND metalayer is managed by the array");
        }
        if schunk.metalayers.len() >= BLOSC2_MAX_METALAYERS {
            return Err("Too many metalayers");
        }
        schunk.add_metalayer(name, content)?;
    }
    Ok(())
}

fn user_fixed_metalayers(schunk: &Schunk) -> Vec<(&str, &[u8])> {
    schunk
        .metalayers
        .iter()
        .filter(|layer| layer.name != B2ND_METALAYER_NAME)
        .map(|layer| (layer.name.as_str(), layer.content.as_slice()))
        .collect()
}

fn copy_vlmetalayers(src: &Schunk, dst: &mut Schunk) -> Result<(), &'static str> {
    src.copy_vlmetalayers_to(dst)
}

fn b2nd_metalayer_for_schunk(schunk: &Schunk) -> Option<Vec<u8>> {
    schunk
        .metalayer(B2ND_METALAYER_NAME)
        .map(<[u8]>::to_vec)
        .or_else(|| schunk.cparams.b2nd_metalayer.clone())
        .or_else(|| schunk.dparams.b2nd_metalayer.clone())
}

fn cparams_raw_copy_compatible(src: &CParams, dst: &CParams) -> bool {
    src.compcode == dst.compcode
        && src.compcode_meta == dst.compcode_meta
        && src.clevel == dst.clevel
        && src.typesize == dst.typesize
        && src.blocksize == dst.blocksize
        && src.splitmode == dst.splitmode
        && src.filters == dst.filters
        && src.filters_meta == dst.filters_meta
        && src.use_dict == dst.use_dict
        && src.nthreads == dst.nthreads
        && src.nchunk == dst.nchunk
        && src.prefilter.is_none()
        && dst.prefilter.is_none()
        && src.prefilter_user_data == dst.prefilter_user_data
        && src.prefilter_output_typesize == dst.prefilter_output_typesize
        && src.prefilter_output_is_disposable == dst.prefilter_output_is_disposable
}

fn validate_insert_buffer_size(
    typesize: usize,
    buffershape: &[i64],
    data: &[u8],
) -> Result<(), &'static str> {
    let mut extents = Vec::with_capacity(buffershape.len());
    for &dim in buffershape {
        extents.push(usize::try_from(dim).map_err(|_| "Invalid B2ND insert bounds")?);
    }
    if extents.iter().any(|&extent| extent == 0) {
        if !data.is_empty() {
            return Err("B2ND insert buffer size does not match buffer shape and typesize");
        }
        return Ok(());
    }
    let required_len = dense_region_required_len(buffershape, &extents, typesize)?;
    if data.len() < required_len {
        return Err("B2ND insert buffer size does not match buffer shape and typesize");
    }
    Ok(())
}

/// Validated `start..stop` slice expressed in three forms used by callers.
struct B2ndSlice {
    starts: Vec<usize>,
    extents: Vec<usize>,
    extents_as_i64: Vec<i64>,
}

/// Validated per-dimension resize mapping.
struct B2ndResize {
    starts: Vec<usize>,
}

fn validate_resize_at(
    meta: &B2ndMeta,
    new_shape: &[i64],
    start: Option<&[i64]>,
) -> Result<B2ndResize, &'static str> {
    let ndim = meta.ndim();
    if new_shape.len() != ndim {
        return Err("B2ND resize rank does not match array rank");
    }

    let mut starts = Vec::with_capacity(ndim);
    for dim in 0..ndim {
        let old_dim = meta.shape[dim];
        let new_dim = new_shape[dim];
        let start_dim = match start {
            Some(start) => {
                if start.len() != ndim {
                    return Err("B2ND resize start rank does not match array rank");
                }
                if start[dim] < 0 || start[dim] > old_dim {
                    return Err("Invalid B2ND resize start");
                }
                start[dim]
            }
            None => {
                if new_dim > old_dim {
                    old_dim
                } else {
                    new_dim
                }
            }
        };

        if start.is_some() {
            if new_dim < old_dim && start_dim > new_dim {
                return Err("Invalid B2ND resize start");
            }
            let delta = new_dim
                .checked_sub(old_dim)
                .or_else(|| old_dim.checked_sub(new_dim).and_then(i64::checked_neg))
                .ok_or("Invalid B2ND resize shape")?;
            let touches_end = if delta > 0 {
                start_dim == old_dim
            } else if delta < 0 {
                start_dim == new_dim
            } else {
                true
            };
            if !touches_end {
                let chunk = meta.chunkshape[dim] as i64;
                if start_dim % chunk != 0 || delta % chunk != 0 {
                    return Err("B2ND resize start and delta must be chunk aligned");
                }
            }
        }

        starts.push(start_dim as usize);
    }

    Ok(B2ndResize { starts })
}

fn chunk_origin_in_resize_region(
    chunk_index: &[usize],
    chunkshape: &[i32],
    resize: &B2ndResize,
    shape: &[i64],
    shrunk_shape: &[i64],
) -> Result<bool, &'static str> {
    for dim in 0..chunk_index.len() {
        let origin = (chunk_index[dim] as i64)
            .checked_mul(chunkshape[dim] as i64)
            .ok_or("B2ND chunk index overflow")?;
        let start = resize.starts[dim] as i64;
        let delta = shape[dim]
            .checked_sub(shrunk_shape[dim])
            .ok_or("Invalid B2ND resize shape")?;
        let stop = start
            .checked_add(delta)
            .ok_or("Invalid B2ND resize shape")?;
        if start <= origin && origin < stop {
            return Ok(true);
        }
    }
    Ok(false)
}

fn chunk_intersects_growth_region(
    meta: &B2ndMeta,
    chunk_index: &[usize],
    growth: &[Option<std::ops::Range<usize>>],
) -> Result<bool, &'static str> {
    for axis in 0..chunk_index.len() {
        let Some(growth_axis) = &growth[axis] else {
            continue;
        };
        let chunk_start = chunk_index[axis]
            .checked_mul(meta.chunkshape[axis] as usize)
            .ok_or("B2ND chunk index overflow")?;
        let chunk_stop = chunk_start
            .checked_add(meta.chunkshape[axis] as usize)
            .ok_or("B2ND chunk index overflow")?
            .min(meta.shape[axis] as usize);
        if chunk_start < growth_axis.end && growth_axis.start < chunk_stop {
            return Ok(true);
        }
    }
    Ok(false)
}

fn zero_growth_cells_in_chunk(
    meta: &B2ndMeta,
    layout: &B2ndLayout,
    chunk_index: &[usize],
    growth: &[Option<std::ops::Range<usize>>],
    chunk: &mut [u8],
    typesize: usize,
) -> Result<(), &'static str> {
    let ndim = meta.ndim();
    let mut extents = vec![0usize; ndim];
    for axis in 0..ndim {
        let chunk_start = chunk_index[axis]
            .checked_mul(meta.chunkshape[axis] as usize)
            .ok_or("B2ND chunk index overflow")?;
        let chunk_stop = chunk_start
            .checked_add(meta.chunkshape[axis] as usize)
            .ok_or("B2ND chunk index overflow")?
            .min(meta.shape[axis] as usize);
        extents[axis] = chunk_stop - chunk_start;
    }

    let mut idx = vec![0usize; ndim];
    zero_growth_cells_in_chunk_inner(
        0,
        &extents,
        &mut idx,
        meta,
        layout,
        chunk_index,
        growth,
        chunk,
        typesize,
    )
}

#[allow(clippy::too_many_arguments)]
fn zero_growth_cells_in_chunk_inner(
    dim: usize,
    extents: &[usize],
    idx: &mut [usize],
    meta: &B2ndMeta,
    layout: &B2ndLayout,
    chunk_index: &[usize],
    growth: &[Option<std::ops::Range<usize>>],
    chunk: &mut [u8],
    typesize: usize,
) -> Result<(), &'static str> {
    if dim < extents.len() {
        for value in 0..extents[dim] {
            idx[dim] = value;
            zero_growth_cells_in_chunk_inner(
                dim + 1,
                extents,
                idx,
                meta,
                layout,
                chunk_index,
                growth,
                chunk,
                typesize,
            )?;
        }
        return Ok(());
    }

    let mut is_new = false;
    for axis in 0..idx.len() {
        let global_idx = chunk_index[axis]
            .checked_mul(meta.chunkshape[axis] as usize)
            .and_then(|start| start.checked_add(idx[axis]))
            .ok_or("B2ND chunk index overflow")?;
        if growth[axis]
            .as_ref()
            .is_some_and(|range| range.contains(&global_idx))
        {
            is_new = true;
        }
    }
    if !is_new {
        return Ok(());
    }

    let dst = b2nd_chunk_offset(
        idx,
        &layout.extchunkshape,
        &meta.blockshape,
        &layout.blocks_in_chunk,
        layout.block_nitems,
        layout.typesize,
    )?;
    let dst_end = dst.checked_add(typesize).ok_or("B2ND copy overflow")?;
    let dst_item = chunk
        .get_mut(dst..dst_end)
        .ok_or("B2ND destination too small")?;
    dst_item.fill(0);
    Ok(())
}

/// Validate that `start..stop` is an in-bounds slice and return it
/// in the convenience forms used by the dense copy helpers.
fn validate_slice_bounds(
    meta: &B2ndMeta,
    start: &[i64],
    stop: &[i64],
) -> Result<B2ndSlice, &'static str> {
    let ndim = meta.ndim();
    if start.len() != ndim || stop.len() != ndim {
        return Err("B2ND slice rank does not match array rank");
    }

    let mut starts = Vec::with_capacity(ndim);
    let mut extents = Vec::with_capacity(ndim);
    let mut extents_as_i64 = Vec::with_capacity(ndim);
    for dim in 0..ndim {
        if start[dim] < 0 || stop[dim] > meta.shape[dim] || start[dim] > stop[dim] {
            return Err("Invalid B2ND slice bounds");
        }
        let extent = stop[dim]
            .checked_sub(start[dim])
            .ok_or("Invalid B2ND slice bounds")?;
        starts.push(start[dim] as usize);
        extents.push(extent as usize);
        extents_as_i64.push(extent);
    }
    product_usize(&extents)?;
    Ok(B2ndSlice {
        starts,
        extents,
        extents_as_i64,
    })
}

fn slice_extents_without_source_bounds(
    meta: &B2ndMeta,
    start: &[i64],
    stop: &[i64],
) -> Result<Vec<i64>, &'static str> {
    let ndim = meta.ndim();
    if start.len() != ndim || stop.len() != ndim {
        return Err("B2ND slice rank does not match array rank");
    }
    let mut extents = Vec::with_capacity(ndim);
    for dim in 0..ndim {
        let extent = stop[dim]
            .checked_sub(start[dim])
            .ok_or("Invalid B2ND slice bounds")?;
        if extent < 0 {
            return Err("Invalid B2ND slice bounds");
        }
        extents.push(extent);
    }
    product_i64(&extents)?;
    Ok(extents)
}

fn validate_slice_buffershape(
    extents_as_i64: &[i64],
    buffershape: &[i64],
) -> Result<(), &'static str> {
    if buffershape.len() != extents_as_i64.len() {
        return Err("B2ND buffer rank does not match array rank");
    }
    for (extent, &buffer_dim) in extents_as_i64.iter().zip(buffershape) {
        if buffer_dim < *extent {
            return Err("B2ND buffer shape is smaller than slice shape");
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn collect_slice_chunks_c_filtered(
    dim: usize,
    first: &[usize],
    shape: &[usize],
    chunk_index: &mut [usize],
    chunk_grid: &[usize],
    meta: &B2ndMeta,
    slice: &B2ndSlice,
    out: &mut Vec<i64>,
) -> Result<(), &'static str> {
    if dim < chunk_index.len() {
        for offset in 0..shape[dim] {
            chunk_index[dim] = first[dim]
                .checked_add(offset)
                .ok_or("B2ND chunk index overflow")?;
            collect_slice_chunks_c_filtered(
                dim + 1,
                first,
                shape,
                chunk_index,
                chunk_grid,
                meta,
                slice,
                out,
            )?;
        }
        return Ok(());
    }

    let mut chunk_empty = false;
    for dim in 0..chunk_index.len() {
        let chunk_start = chunk_index[dim]
            .checked_mul(meta.chunkshape[dim] as usize)
            .ok_or("B2ND chunk index overflow")?;
        let chunk_stop = chunk_start
            .checked_add(meta.chunkshape[dim] as usize)
            .ok_or("B2ND chunk index overflow")?
            .min(meta.shape[dim] as usize);
        let slice_stop = slice.starts[dim]
            .checked_add(slice.extents[dim])
            .ok_or("Invalid B2ND slice bounds")?;
        chunk_empty |= chunk_stop <= slice.starts[dim] || chunk_start >= slice_stop;
    }
    if !chunk_empty {
        if chunk_index
            .iter()
            .zip(chunk_grid)
            .any(|(&chunk_index, &grid)| chunk_index >= grid)
        {
            return Err("B2ND chunk index out of bounds");
        }
        out.push(ravel_index(chunk_index, chunk_grid)? as i64);
    }
    Ok(())
}

fn validate_orthogonal_selection(
    meta: &B2ndMeta,
    selection: &[Vec<i64>],
) -> Result<(Vec<Vec<usize>>, Vec<usize>, Vec<i64>), &'static str> {
    let ndim = meta.ndim();
    if selection.len() != ndim {
        return Err("B2ND selection rank does not match array rank");
    }
    let mut coords = Vec::with_capacity(ndim);
    let mut extents = Vec::with_capacity(ndim);
    let mut shape = Vec::with_capacity(ndim);
    for (dim, dim_selection) in selection.iter().enumerate() {
        let mut dim_coords = Vec::with_capacity(dim_selection.len());
        for &coord in dim_selection {
            // C may pass coord == shape into padded-chunk reads. The Rust
            // adapter rejects it to avoid treating an out-of-bounds logical
            // coordinate as an in-bounds dense offset.
            if coord < 0 || coord >= meta.shape[dim] {
                return Err("Invalid B2ND selection coordinate");
            }
            dim_coords.push(coord as usize);
        }
        extents.push(dim_coords.len());
        shape.push(dim_coords.len() as i64);
        coords.push(dim_coords);
    }
    product_usize(&extents)?;
    Ok((coords, extents, shape))
}

fn validate_orthogonal_selection_c(
    meta: &B2ndMeta,
    selection: &[Vec<i64>],
) -> Result<(Vec<Vec<usize>>, Vec<usize>, Vec<i64>), &'static str> {
    let ndim = meta.ndim();
    if selection.len() != ndim {
        return Err("B2ND selection rank does not match array rank");
    }
    let grid = chunk_grid(meta)?;
    let mut coords = Vec::with_capacity(ndim);
    let mut extents = Vec::with_capacity(ndim);
    let mut shape = Vec::with_capacity(ndim);
    for (dim, dim_selection) in selection.iter().enumerate() {
        let padded_stop = grid[dim]
            .checked_mul(meta.chunkshape[dim] as usize)
            .ok_or("B2ND selection coordinate overflow")?;
        let mut dim_coords = Vec::with_capacity(dim_selection.len());
        for &coord in dim_selection {
            if coord < 0 || coord > meta.shape[dim] {
                return Err("Invalid B2ND selection coordinate");
            }
            if coord as usize > padded_stop {
                return Err("B2ND selection chunk index out of range");
            }
            dim_coords.push(coord as usize);
        }
        extents.push(dim_coords.len());
        shape.push(dim_coords.len() as i64);
        coords.push(dim_coords);
    }
    product_usize(&extents)?;
    Ok((coords, extents, shape))
}

fn validate_orthogonal_buffershape(
    buffershape: &[i64],
    extents: &[usize],
) -> Result<(), &'static str> {
    if buffershape.len() != extents.len() {
        return Err("B2ND buffer rank does not match selection rank");
    }
    for (&buffer_dim, &extent) in buffershape.iter().zip(extents) {
        if buffer_dim < 0 || buffer_dim < extent as i64 {
            return Err("B2ND buffer shape is smaller than selection shape");
        }
    }
    if product_i64(buffershape)? > product_usize(extents)? {
        return Err("B2ND buffer shape is larger than selection shape");
    }
    Ok(())
}

fn validate_orthogonal_buffershape_c(
    buffershape: &[i64],
    extents: &[usize],
) -> Result<(), &'static str> {
    if buffershape.len() != extents.len() {
        return Err("B2ND buffer rank does not match selection rank");
    }
    for (&buffer_dim, &extent) in buffershape.iter().zip(extents) {
        if buffer_dim < 0 || buffer_dim < extent as i64 {
            return Err("B2ND buffer shape is smaller than selection shape");
        }
    }
    product_i64(buffershape)?;
    Ok(())
}

struct OrthogonalChunkRead {
    maskout: Vec<bool>,
    items: Vec<OrthogonalReadItem>,
}

struct OrthogonalReadItem {
    local_idx: Vec<usize>,
    dst_offset: usize,
}

#[allow(clippy::too_many_arguments)]
fn collect_orthogonal_chunk_reads(
    dim: usize,
    coords: &[Vec<usize>],
    extents: &[usize],
    idx: &mut [usize],
    meta: &B2ndMeta,
    layout: &B2ndLayout,
    chunk_grid: &[usize],
    nblocks: usize,
    dst_strides: &[usize],
    touched_chunks: &mut BTreeMap<usize, OrthogonalChunkRead>,
) -> Result<(), &'static str> {
    if dim < extents.len() {
        for value in 0..extents[dim] {
            idx[dim] = value;
            collect_orthogonal_chunk_reads(
                dim + 1,
                coords,
                extents,
                idx,
                meta,
                layout,
                chunk_grid,
                nblocks,
                dst_strides,
                touched_chunks,
            )?;
        }
        return Ok(());
    }

    let ndim = meta.ndim();
    let mut chunk_index = vec![0usize; ndim];
    let mut local_idx = vec![0usize; ndim];
    for axis in 0..ndim {
        let coord = coords[axis][idx[axis]];
        let chunk = meta.chunkshape[axis] as usize;
        // C adapters accept coord == shape. If that lands beyond the chunk
        // grid, Rust represents the one-past cell as virtual padding.
        if coord / chunk >= chunk_grid[axis] {
            return Ok(());
        }
        chunk_index[axis] = coord / chunk;
        local_idx[axis] = coord % chunk;
    }
    let linear_chunk = ravel_index(&chunk_index, chunk_grid)?;
    let block_index = b2nd_block_index(&local_idx, &meta.blockshape, &layout.blocks_in_chunk)?;
    let dst_offset = dense_offset(&vec![0usize; ndim], idx, dst_strides)?;
    let read = touched_chunks
        .entry(linear_chunk)
        .or_insert_with(|| OrthogonalChunkRead {
            maskout: vec![true; nblocks],
            items: Vec::new(),
        });
    read.maskout[block_index] = false;
    read.items.push(OrthogonalReadItem {
        local_idx,
        dst_offset,
    });
    Ok(())
}

/// Consume one byte from `data` at `pos` and check it matches `expected`.
fn skip_byte(data: &[u8], pos: &mut usize) -> Result<(), &'static str> {
    data.get(*pos).ok_or("Truncated B2ND metadata")?;
    *pos += 1;
    Ok(())
}

/// Consume one byte from `data` at `pos` and check it matches `expected`.
fn expect_byte(data: &[u8], pos: &mut usize, expected: u8) -> Result<(), &'static str> {
    if data.get(*pos).copied() != Some(expected) {
        return Err("Invalid B2ND metadata");
    }
    *pos += 1;
    Ok(())
}

/// Write a msgpack array header for ranks used by b2nd metadata.
fn write_array_header(out: &mut Vec<u8>, len: usize) -> Result<(), &'static str> {
    if len <= 15 {
        out.push(0x90 + len as u8);
    } else if len == 16 {
        // C-Blosc2 historically writes 0x90 + ndim even for ndim == 16.
        out.push(0xa0);
    } else if len <= u16::MAX as usize {
        out.push(0xdc);
        out.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        return Err("B2ND array too large");
    }
    Ok(())
}

/// Consume a msgpack array header and check it has the expected length.
fn expect_array_header(
    data: &[u8],
    pos: &mut usize,
    expected_len: usize,
) -> Result<(), &'static str> {
    let len = read_array_header(data, pos)?;
    if len != expected_len {
        return Err("Invalid B2ND metadata");
    }
    Ok(())
}

/// Consume a msgpack array header and return its length.
fn read_array_header(data: &[u8], pos: &mut usize) -> Result<usize, &'static str> {
    let byte = *data.get(*pos).ok_or("Truncated B2ND metadata")?;
    *pos += 1;
    let len = if (0x90..=0x9f).contains(&byte) {
        (byte - 0x90) as usize
    } else if byte == 0xa0 {
        16
    } else if byte == 0xdc {
        let end = pos.checked_add(2).ok_or("Invalid B2ND metadata")?;
        let bytes = data.get(*pos..end).ok_or("Truncated B2ND metadata")?;
        *pos = end;
        u16::from_be_bytes(bytes.try_into().unwrap()) as usize
    } else {
        return Err("Invalid B2ND metadata");
    };
    Ok(len)
}

/// Read a msgpack positive fixint (0x00-0x7f).
fn read_fixint(data: &[u8], pos: &mut usize) -> Result<u8, &'static str> {
    let byte = *data.get(*pos).ok_or("Truncated B2ND metadata")?;
    if byte > 0x7f {
        return Err("Invalid B2ND fixint");
    }
    *pos += 1;
    Ok(byte)
}

/// Read a big-endian `i64` and advance `pos` past it.
fn read_i64(data: &[u8], pos: &mut usize) -> Result<i64, &'static str> {
    let end = pos.checked_add(8).ok_or("Invalid B2ND metadata")?;
    let bytes = data.get(*pos..end).ok_or("Truncated B2ND metadata")?;
    *pos = end;
    Ok(i64::from_be_bytes(bytes.try_into().unwrap()))
}

/// Read a big-endian `i32` and advance `pos` past it.
fn read_i32(data: &[u8], pos: &mut usize) -> Result<i32, &'static str> {
    let end = pos.checked_add(4).ok_or("Invalid B2ND metadata")?;
    let bytes = data.get(*pos..end).ok_or("Truncated B2ND metadata")?;
    *pos = end;
    Ok(i32::from_be_bytes(bytes.try_into().unwrap()))
}

/// Product of non-negative `i64` values, or an error on overflow or negative
/// entries.
fn product_i64(values: &[i64]) -> Result<usize, &'static str> {
    values.iter().try_fold(1usize, |acc, &value| {
        if value < 0 {
            return Err("Invalid B2ND shape");
        }
        acc.checked_mul(value as usize)
            .ok_or("B2ND shape too large")
    })
}

/// Product of non-negative `i32` values, or an error on overflow or negative
/// entries.
fn product_i32(values: &[i32]) -> Result<usize, &'static str> {
    values.iter().try_fold(1usize, |acc, &value| {
        if value < 0 {
            return Err("Invalid B2ND shape");
        }
        acc.checked_mul(value as usize)
            .ok_or("B2ND shape too large")
    })
}

/// Product of `usize` values guarded against overflow.
fn product_usize(values: &[usize]) -> Result<usize, &'static str> {
    values.iter().try_fold(1usize, |acc, &value| {
        acc.checked_mul(value).ok_or("B2ND shape too large")
    })
}

/// Number of chunks needed to tile the shape along each dimension
/// (`ceil(shape[d] / chunkshape[d])`).
fn chunk_grid(meta: &B2ndMeta) -> Result<Vec<usize>, &'static str> {
    meta.shape
        .iter()
        .zip(&meta.chunkshape)
        .map(|(&shape, &chunk)| {
            if shape < 0 || chunk < 0 || (shape > 0 && chunk == 0) {
                return Err("Invalid B2ND shape");
            }
            Ok(if shape == 0 {
                0
            } else {
                (shape as usize).div_ceil(chunk as usize)
            })
        })
        .collect()
}

/// Padded chunk shape: each chunk dimension rounded up to a multiple of the
/// matching block dimension so that a chunk holds a whole number of blocks.
fn extchunkshape(meta: &B2ndMeta) -> Result<Vec<i32>, &'static str> {
    meta.chunkshape
        .iter()
        .zip(&meta.blockshape)
        .map(|(&chunk, &block)| {
            if chunk == 0 && block == 0 {
                return Ok(0);
            }
            if chunk <= 0 || block <= 0 {
                return Err("Invalid B2ND chunk or block shape");
            }
            let padded = if chunk % block == 0 {
                chunk as i64
            } else {
                (chunk as i64)
                    .checked_add(block as i64)
                    .and_then(|value| value.checked_sub((chunk % block) as i64))
                    .ok_or("B2ND chunk too large")?
            };
            i32::try_from(padded).map_err(|_| "B2ND chunk too large")
        })
        .collect()
}

/// Number of items in a padded chunk.
fn extchunk_nitems(meta: &B2ndMeta) -> Result<usize, &'static str> {
    product_i32(&extchunkshape(meta)?)
}

/// Number of blocks per dimension inside one padded chunk.
fn blocks_in_chunk(extchunkshape: &[i32], blockshape: &[i32]) -> Result<Vec<usize>, &'static str> {
    extchunkshape
        .iter()
        .zip(blockshape)
        .map(|(&extchunk, &block)| {
            if extchunk == 0 && block == 0 {
                return Ok(0);
            }
            if extchunk <= 0 || block <= 0 || extchunk % block != 0 {
                return Err("Invalid B2ND block grid");
            }
            Ok((extchunk / block) as usize)
        })
        .collect()
}

/// Row-major byte strides for the given shape and item size.
fn byte_strides_i64(shape: &[i64], typesize: usize) -> Result<Vec<usize>, &'static str> {
    let mut strides = vec![0; shape.len()];
    let mut stride = typesize;
    for idx in (0..shape.len()).rev() {
        strides[idx] = stride;
        stride = stride
            .checked_mul(shape[idx] as usize)
            .ok_or("B2ND shape too large")?;
    }
    Ok(strides)
}

/// Byte offset of an item at multi-index `starts + idx` in a row-major buffer
/// with the given byte strides.
fn dense_offset(starts: &[usize], idx: &[usize], strides: &[usize]) -> Result<usize, &'static str> {
    starts
        .iter()
        .zip(idx)
        .zip(strides)
        .try_fold(0usize, |acc, ((&start, &idx), &stride)| {
            start
                .checked_add(idx)
                .and_then(|coord| coord.checked_mul(stride))
                .and_then(|offset| acc.checked_add(offset))
                .ok_or("B2ND dense offset overflow")
        })
}

fn dense_region_required_len(
    shape: &[i64],
    extents: &[usize],
    typesize: usize,
) -> Result<usize, &'static str> {
    if shape.len() != extents.len() {
        return Err("B2ND dense copy rank mismatch");
    }
    if extents.is_empty() {
        return Ok(typesize);
    }
    let strides = byte_strides_i64(shape, typesize)?;
    let last_idx: Vec<usize> = extents
        .iter()
        .map(|&extent| extent.checked_sub(1).ok_or("Invalid B2ND slice bounds"))
        .collect::<Result<_, _>>()?;
    dense_offset(&vec![0; extents.len()], &last_idx, &strides)?
        .checked_add(typesize)
        .ok_or("B2ND dense offset overflow")
}

/// Copy a rectangular item region between dense row-major buffers.
///
/// This is the Rust equivalent of C-Blosc2 `b2nd_copy_buffer2`. `src_stop`
/// is exclusive, coordinates are measured in items, and `itemsize` is measured
/// in bytes. C treats bounds as caller preconditions; this Rust implementation
/// validates slice extents and buffer lengths to avoid unchecked out-of-bounds
/// memory access.
pub fn copy_buffer2(
    src: &[u8],
    src_pad_shape: &[i64],
    src_start: &[i64],
    src_stop: &[i64],
    dst: &mut [u8],
    dst_pad_shape: &[i64],
    dst_start: &[i64],
    itemsize: usize,
) -> Result<(), &'static str> {
    if itemsize == 0 {
        return Err("Invalid B2ND itemsize");
    }
    let ndim = src_pad_shape.len();
    if ndim == 0 || ndim > B2ND_MAX_DIM {
        return Err("Invalid B2ND ndim");
    }
    if src_start.len() != ndim
        || src_stop.len() != ndim
        || dst_pad_shape.len() != ndim
        || dst_start.len() != ndim
    {
        return Err("B2ND dense copy rank mismatch");
    }
    if src_pad_shape.iter().any(|&dim| dim < 0) || dst_pad_shape.iter().any(|&dim| dim < 0) {
        return Err("Invalid B2ND shape");
    }

    let mut extents = Vec::with_capacity(ndim);
    let mut src_start_usize = Vec::with_capacity(ndim);
    let mut dst_start_usize = Vec::with_capacity(ndim);
    for dim in 0..ndim {
        let start = src_start[dim];
        let stop = src_stop[dim];
        let dst_start_dim = dst_start[dim];
        if start < 0 || stop < start || dst_start_dim < 0 {
            return Err("Invalid B2ND slice bounds");
        }
        let extent = usize::try_from(stop - start).map_err(|_| "B2ND dense copy too large")?;
        if extent == 0 {
            return Ok(());
        }
        let src_pad = src_pad_shape[dim];
        let dst_pad = dst_pad_shape[dim];
        if stop > src_pad
            || dst_start_dim
                .checked_add(stop - start)
                .is_none_or(|end| end > dst_pad)
        {
            return Err("Invalid B2ND slice bounds");
        }
        extents.push(extent);
        src_start_usize.push(usize::try_from(start).map_err(|_| "Invalid B2ND slice bounds")?);
        dst_start_usize
            .push(usize::try_from(dst_start_dim).map_err(|_| "Invalid B2ND slice bounds")?);
    }

    let src_required = dense_region_required_len(src_pad_shape, &extents, itemsize)?;
    let dst_required = dense_region_required_len(dst_pad_shape, &extents, itemsize)?;
    let src_origin = dense_offset(
        &src_start_usize,
        &vec![0; ndim],
        &byte_strides_i64(src_pad_shape, itemsize)?,
    )?;
    let dst_origin = dense_offset(
        &dst_start_usize,
        &vec![0; ndim],
        &byte_strides_i64(dst_pad_shape, itemsize)?,
    )?;
    if src_origin
        .checked_add(src_required)
        .is_none_or(|len| len > src.len())
        || dst_origin
            .checked_add(dst_required)
            .is_none_or(|len| len > dst.len())
    {
        return Err("B2ND buffer too small");
    }

    copy_dense_region(
        src,
        DenseRegion {
            shape: src_pad_shape,
            start: &src_start_usize,
        },
        dst,
        DenseRegion {
            shape: dst_pad_shape,
            start: &dst_start_usize,
        },
        &extents,
        itemsize,
    )
}

fn b2nd_copy_buffer_error_code(err: &str) -> i32 {
    match err {
        "B2ND buffer too small" => BLOSC2_ERROR_WRITE_BUFFER,
        _ => BLOSC2_ERROR_INVALID_PARAM,
    }
}

/// Rust-friendly wrapper for [`copy_buffer2`].
pub fn b2nd_copy_buffer2_result(
    ndim: i8,
    itemsize: i32,
    src: &[u8],
    src_pad_shape: &[i64],
    src_start: &[i64],
    src_stop: &[i64],
    dst: &mut [u8],
    dst_pad_shape: &[i64],
    dst_start: &[i64],
) -> Result<(), &'static str> {
    if ndim < 0 || src_pad_shape.len() != ndim as usize {
        return Err("Invalid B2ND ndim");
    }
    let itemsize = usize::try_from(itemsize).map_err(|_| "Invalid B2ND itemsize")?;
    copy_buffer2(
        src,
        src_pad_shape,
        src_start,
        src_stop,
        dst,
        dst_pad_shape,
        dst_start,
        itemsize,
    )
}

/// C-name adapter for [`copy_buffer2`].
pub fn b2nd_copy_buffer2(
    ndim: i8,
    itemsize: i32,
    src: &[u8],
    src_pad_shape: &[i64],
    src_start: &[i64],
    src_stop: &[i64],
    dst: &mut [u8],
    dst_pad_shape: &[i64],
    dst_start: &[i64],
) -> i32 {
    match b2nd_copy_buffer2_result(
        ndim,
        itemsize,
        src,
        src_pad_shape,
        src_start,
        src_stop,
        dst,
        dst_pad_shape,
        dst_start,
    ) {
        Ok(()) => BLOSC2_ERROR_SUCCESS,
        Err(err) => b2nd_copy_buffer_error_code(err),
    }
}

/// Rust-friendly deprecated wrapper for [`copy_buffer2`].
pub fn b2nd_copy_buffer_result(
    ndim: i8,
    itemsize: u8,
    src: &[u8],
    src_pad_shape: &[i64],
    src_start: &[i64],
    src_stop: &[i64],
    dst: &mut [u8],
    dst_pad_shape: &[i64],
    dst_start: &[i64],
) -> Result<(), &'static str> {
    if ndim < 0 || src_pad_shape.len() != ndim as usize {
        return Err("Invalid B2ND ndim");
    }
    copy_buffer2(
        src,
        src_pad_shape,
        src_start,
        src_stop,
        dst,
        dst_pad_shape,
        dst_start,
        itemsize as usize,
    )
}

/// Deprecated C-name adapter for [`copy_buffer2`].
pub fn b2nd_copy_buffer(
    ndim: i8,
    itemsize: u8,
    src: &[u8],
    src_pad_shape: &[i64],
    src_start: &[i64],
    src_stop: &[i64],
    dst: &mut [u8],
    dst_pad_shape: &[i64],
    dst_start: &[i64],
) -> i32 {
    match b2nd_copy_buffer_result(
        ndim,
        itemsize,
        src,
        src_pad_shape,
        src_start,
        src_stop,
        dst,
        dst_pad_shape,
        dst_start,
    ) {
        Ok(()) => BLOSC2_ERROR_SUCCESS,
        Err(err) => b2nd_copy_buffer_error_code(err),
    }
}

/// Description of a rectangular region inside a dense row-major buffer.
struct DenseRegion<'a> {
    shape: &'a [i64],
    start: &'a [usize],
}

/// Copy an `extents`-shaped block of items from one dense row-major buffer
/// into another, given the source and destination regions.
fn copy_dense_region(
    src: &[u8],
    src_region: DenseRegion<'_>,
    dst: &mut [u8],
    dst_region: DenseRegion<'_>,
    extents: &[usize],
    typesize: usize,
) -> Result<(), &'static str> {
    if src_region.shape.len() != extents.len()
        || dst_region.shape.len() != extents.len()
        || src_region.start.len() != extents.len()
        || dst_region.start.len() != extents.len()
    {
        return Err("B2ND dense copy rank mismatch");
    }
    let src_strides = byte_strides_i64(src_region.shape, typesize)?;
    let dst_strides = byte_strides_i64(dst_region.shape, typesize)?;
    copy_region(
        0,
        extents,
        |idx| {
            Ok((
                dense_offset(src_region.start, idx, &src_strides)?,
                dense_offset(dst_region.start, idx, &dst_strides)?,
            ))
        },
        src,
        dst,
        typesize,
    )
}

/// Precomputed strides and block geometry shared by the chunk/dense copy
/// helpers.
struct B2ndLayout {
    data_strides: Vec<usize>,
    extchunkshape: Vec<i32>,
    blocks_in_chunk: Vec<usize>,
    block_nitems: usize,
    typesize: usize,
}

impl B2ndLayout {
    /// Build the layout cache for a given metadata and typesize.
    fn new(meta: &B2ndMeta, typesize: usize) -> Result<Self, &'static str> {
        let extchunkshape = extchunkshape(meta)?;
        let blocks_in_chunk = blocks_in_chunk(&extchunkshape, &meta.blockshape)?;
        Ok(Self {
            data_strides: byte_strides_i64(&meta.shape, typesize)?,
            extchunkshape,
            blocks_in_chunk,
            block_nitems: product_i32(&meta.blockshape)?,
            typesize,
        })
    }
}

/// Convert a linear C-order index into a multi-dimensional index.
fn unravel_index(mut index: usize, shape: &[usize]) -> Vec<usize> {
    let mut out = vec![0; shape.len()];
    for dim in (0..shape.len()).rev() {
        out[dim] = index % shape[dim];
        index /= shape[dim];
    }
    out
}

/// Convert a multi-dimensional index into a row-major linear index.
fn ravel_index(index: &[usize], shape: &[usize]) -> Result<usize, &'static str> {
    if index.len() != shape.len() {
        return Err("B2ND index rank mismatch");
    }
    index
        .iter()
        .zip(shape)
        .try_fold(0usize, |acc, (&coord, &extent)| {
            if coord >= extent {
                return Err("B2ND chunk index out of range");
            }
            acc.checked_mul(extent)
                .and_then(|value| value.checked_add(coord))
                .ok_or("B2ND chunk index overflow")
        })
}

/// Copy the items belonging to chunk `chunk_index` out of a dense row-major
/// source buffer into the chunk's block-interleaved layout.
fn copy_dense_to_chunk(
    meta: &B2ndMeta,
    data: &[u8],
    layout: &B2ndLayout,
    chunk_index: &[usize],
    chunk: &mut [u8],
) -> Result<(), &'static str> {
    let ndim = meta.ndim();
    let mut starts = vec![0usize; ndim];
    let mut extents = vec![0usize; ndim];
    for dim in 0..ndim {
        starts[dim] = chunk_index[dim]
            .checked_mul(meta.chunkshape[dim] as usize)
            .ok_or("B2ND chunk index overflow")?;
        let stop = (starts[dim] + meta.chunkshape[dim] as usize).min(meta.shape[dim] as usize);
        extents[dim] = stop - starts[dim];
    }
    copy_region(
        0,
        &extents,
        |idx| {
            let mut src = 0usize;
            let dst = b2nd_chunk_offset(
                idx,
                &layout.extchunkshape,
                &meta.blockshape,
                &layout.blocks_in_chunk,
                layout.block_nitems,
                layout.typesize,
            )?;
            for dim in 0..ndim {
                src += (starts[dim] + idx[dim]) * layout.data_strides[dim];
            }
            Ok((src, dst))
        },
        data,
        chunk,
        layout.typesize,
    )
}

/// Copy items from a single block-interleaved chunk back into the
/// corresponding region of a dense row-major destination buffer.
fn copy_chunk_to_dense(
    meta: &B2ndMeta,
    chunk: &[u8],
    layout: &B2ndLayout,
    chunk_index: &[usize],
    data: &mut [u8],
) -> Result<(), &'static str> {
    let ndim = meta.ndim();
    let mut starts = vec![0usize; ndim];
    let mut extents = vec![0usize; ndim];
    for dim in 0..ndim {
        starts[dim] = chunk_index[dim]
            .checked_mul(meta.chunkshape[dim] as usize)
            .ok_or("B2ND chunk index overflow")?;
        let stop = (starts[dim] + meta.chunkshape[dim] as usize).min(meta.shape[dim] as usize);
        extents[dim] = stop - starts[dim];
    }
    copy_region(
        0,
        &extents,
        |idx| {
            let src = b2nd_chunk_offset(
                idx,
                &layout.extchunkshape,
                &meta.blockshape,
                &layout.blocks_in_chunk,
                layout.block_nitems,
                layout.typesize,
            )?;
            let mut dst = 0usize;
            for dim in 0..ndim {
                dst += (starts[dim] + idx[dim]) * layout.data_strides[dim];
            }
            Ok((src, dst))
        },
        chunk,
        data,
        layout.typesize,
    )
}

/// Byte offset of item `idx` inside a padded chunk laid out as a grid of
/// row-major blocks (the C-Blosc2 b2nd in-chunk layout).
fn b2nd_chunk_offset(
    idx: &[usize],
    extchunkshape: &[i32],
    blockshape: &[i32],
    blocks_in_chunk: &[usize],
    block_nitems: usize,
    typesize: usize,
) -> Result<usize, &'static str> {
    let ndim = idx.len();
    let block_index = b2nd_block_index(idx, blockshape, blocks_in_chunk)?;
    let mut inblock_index = 0usize;
    for dim in 0..ndim {
        let block = blockshape[dim] as usize;
        let extchunk = extchunkshape[dim] as usize;
        if idx[dim] >= extchunk {
            return Err("B2ND chunk index out of range");
        }
        inblock_index = inblock_index
            .checked_mul(block)
            .and_then(|value| value.checked_add(idx[dim] % block))
            .ok_or("B2ND chunk offset overflow")?;
    }
    block_index
        .checked_mul(block_nitems)
        .and_then(|value| value.checked_add(inblock_index))
        .and_then(|value| value.checked_mul(typesize))
        .ok_or("B2ND chunk offset overflow")
}

fn b2nd_block_index(
    idx: &[usize],
    blockshape: &[i32],
    blocks_in_chunk: &[usize],
) -> Result<usize, &'static str> {
    idx.iter().zip(blockshape).zip(blocks_in_chunk).try_fold(
        0usize,
        |acc, ((&coord, &block), &blocks)| {
            acc.checked_mul(blocks)
                .and_then(|value| value.checked_add(coord / block as usize))
                .ok_or("B2ND chunk offset overflow")
        },
    )
}

/// Iterate over every multi-index in an `extents`-shaped region and copy one
/// item per index from `src` to `dst`, using `offsets` to map the index to the
/// source and destination byte positions.
fn copy_region(
    dim: usize,
    extents: &[usize],
    mut offsets: impl FnMut(&[usize]) -> Result<(usize, usize), &'static str>,
    src: &[u8],
    dst: &mut [u8],
    typesize: usize,
) -> Result<(), &'static str> {
    let mut idx = vec![0usize; extents.len()];
    copy_region_inner(dim, extents, &mut idx, &mut offsets, src, dst, typesize)
}

/// Recursive worker for [`copy_region`].
fn copy_region_inner(
    dim: usize,
    extents: &[usize],
    idx: &mut [usize],
    offsets: &mut impl FnMut(&[usize]) -> Result<(usize, usize), &'static str>,
    src: &[u8],
    dst: &mut [u8],
    typesize: usize,
) -> Result<(), &'static str> {
    if dim == extents.len() {
        let (src_pos, dst_pos) = offsets(idx)?;
        let src_end = src_pos.checked_add(typesize).ok_or("B2ND copy overflow")?;
        let dst_end = dst_pos.checked_add(typesize).ok_or("B2ND copy overflow")?;
        let src_item = src.get(src_pos..src_end).ok_or("B2ND source too small")?;
        let dst_item = dst
            .get_mut(dst_pos..dst_end)
            .ok_or("B2ND destination too small")?;
        dst_item.copy_from_slice(src_item);
        return Ok(());
    }
    for value in 0..extents[dim] {
        idx[dim] = value;
        copy_region_inner(dim + 1, extents, idx, offsets, src, dst, typesize)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compress::{PostfilterParams, PrefilterParams};
    use crate::constants::{
        BLOSC2_MAX_FILTERS, BLOSC2_SPECIAL_NAN, BLOSC2_SPECIAL_UNINIT, BLOSC2_SPECIAL_VALUE,
        BLOSC2_SPECIAL_ZERO, BLOSC2_USER_DEFINED_CODECS_START, BLOSC2_USER_DEFINED_FILTERS_START,
        BLOSC2_VERSION_FORMAT_STABLE, BLOSC_BLOSCLZ, BLOSC_BLOSCLZ_VERSION_FORMAT,
        BLOSC_DOBITSHUFFLE, BLOSC_DOSHUFFLE, BLOSC_EXTENDED_HEADER_LENGTH, BLOSC_FILTER_NDCELL,
        BLOSC_LZ4, BLOSC_NEVER_SPLIT, BLOSC_NOFILTER, BLOSC_SHUFFLE, BLOSC_ZSTD,
    };
    use crate::header::ChunkHeader;
    use crate::{codecs, filters};
    use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};

    static ORTHOGONAL_POSTFILTER_CALLS: AtomicUsize = AtomicUsize::new(0);
    static CONTEXT_B2ND_FILTER_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    static CONTEXT_FORWARD_B2ND_BLOCK0: AtomicUsize = AtomicUsize::new(0);
    static CONTEXT_BACKWARD_B2ND_BLOCK0: AtomicUsize = AtomicUsize::new(0);
    static CONTEXT_B2ND_CODEC_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    static CONTEXT_COMPRESS_B2ND_BLOCK0: AtomicUsize = AtomicUsize::new(0);
    static CONTEXT_DECOMPRESS_B2ND_BLOCK0: AtomicUsize = AtomicUsize::new(0);
    static FAIL_PREFILTER_NCHUNK: AtomicI64 = AtomicI64::new(-1);

    fn count_orthogonal_postfilter(params: &mut PostfilterParams<'_>) -> i32 {
        ORTHOGONAL_POSTFILTER_CALLS.fetch_add(1, Ordering::SeqCst);
        params.output.copy_from_slice(params.input);
        0
    }

    fn fail_on_selected_chunk_prefilter(params: &mut PrefilterParams<'_>) -> i32 {
        if params.nchunk == FAIL_PREFILTER_NCHUNK.load(Ordering::SeqCst) {
            return 1;
        }
        params.output.copy_from_slice(params.input);
        0
    }

    fn always_fail_prefilter(_params: &mut PrefilterParams<'_>) -> i32 {
        1
    }

    fn add_one_postfilter(params: &mut PostfilterParams<'_>) -> i32 {
        for (out, input) in params.output.iter_mut().zip(params.input.iter()) {
            *out = input.wrapping_add(1);
        }
        0
    }

    fn xor_b2nd_plugin_filter(
        meta: u8,
        _typesize: usize,
        _block_offset: usize,
        src: &[u8],
        dest: &mut [u8],
    ) {
        for (out, byte) in dest.iter_mut().zip(src) {
            *out = *byte ^ meta;
        }
    }

    fn record_b2nd_context_filter(
        ctx: &mut filters::FilterCallbackContext<'_>,
        src: &[u8],
        dest: &mut [u8],
    ) -> i32 {
        if ctx.cparams.is_some() {
            CONTEXT_FORWARD_B2ND_BLOCK0.store(123, Ordering::SeqCst);
        }
        if ctx.dparams.is_some() {
            CONTEXT_BACKWARD_B2ND_BLOCK0.store(123, Ordering::SeqCst);
        }
        if let Some(bytes) = ctx.b2nd_metalayer {
            let block0 = B2ndMeta::deserialize(bytes)
                .ok()
                .and_then(|meta| meta.blockshape.first().copied())
                .unwrap_or(-1) as usize;
            if ctx.cparams.is_some() {
                CONTEXT_FORWARD_B2ND_BLOCK0.store(block0, Ordering::SeqCst);
            }
            if ctx.dparams.is_some() {
                CONTEXT_BACKWARD_B2ND_BLOCK0.store(block0, Ordering::SeqCst);
            }
        }
        dest.copy_from_slice(src);
        filters::PluginCallbackStatus::Success as i32
    }

    fn sequence_b2nd_plugin_codec_compress(
        _clevel: u8,
        meta: u8,
        src: &[u8],
        dest: &mut [u8],
    ) -> i32 {
        if src.len() < 2 || dest.len() < 3 {
            return -1;
        }
        dest[0] = src[0];
        dest[1] = src[1].wrapping_sub(src[0]);
        dest[2] = meta;
        3
    }

    fn sequence_b2nd_plugin_codec_decompress(meta: u8, src: &[u8], dest: &mut [u8]) -> i32 {
        if src.len() != 3 || src[2] != meta {
            return -1;
        }
        for (idx, byte) in dest.iter_mut().enumerate() {
            *byte = src[0].wrapping_add(src[1].wrapping_mul(idx as u8));
        }
        dest.len() as i32
    }

    fn record_b2nd_context_codec_compress(
        ctx: &mut codecs::CodecCallbackContext<'_>,
        src: &[u8],
        dest: &mut [u8],
    ) -> i32 {
        if let Some(bytes) = ctx.b2nd_metalayer {
            CONTEXT_COMPRESS_B2ND_BLOCK0.store(
                B2ndMeta::deserialize(bytes)
                    .ok()
                    .and_then(|meta| meta.blockshape.first().copied())
                    .unwrap_or(-1) as usize,
                Ordering::SeqCst,
            );
        }
        if src.len() < 2 || dest.len() < 3 {
            return -1;
        }
        dest[0] = src[0];
        dest[1] = src[1].wrapping_sub(src[0]);
        dest[2] = ctx.meta;
        3
    }

    fn record_b2nd_context_codec_decompress(
        ctx: &mut codecs::CodecCallbackContext<'_>,
        src: &[u8],
        dest: &mut [u8],
    ) -> i32 {
        if let Some(bytes) = ctx.b2nd_metalayer {
            CONTEXT_DECOMPRESS_B2ND_BLOCK0.store(
                B2ndMeta::deserialize(bytes)
                    .ok()
                    .and_then(|meta| meta.blockshape.first().copied())
                    .unwrap_or(-1) as usize,
                Ordering::SeqCst,
            );
        }
        if src.len() != 3 || src[2] != ctx.meta {
            return -1;
        }
        for (idx, byte) in dest.iter_mut().enumerate() {
            *byte = src[0].wrapping_add(src[1].wrapping_mul(idx as u8));
        }
        dest.len() as i32
    }

    fn assert_b2nd_frame_and_reopen_roundtrip(array: &B2ndArray, expected: &[u8]) {
        let frame = array.to_contiguous_frame();
        let restored = B2ndArray::from_contiguous_frame(&frame).unwrap();
        assert_eq!(restored.to_dense_buffer().unwrap(), expected);

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("plugin-roundtrip.b2frame");
        array.save(&path).unwrap();
        let reopened = B2ndArray::open(&path).unwrap();
        assert_eq!(reopened.to_dense_buffer().unwrap(), expected);
    }

    fn b2nd_string_shuffle_data(nitems: usize, string_len: usize) -> Vec<u8> {
        let mut data = Vec::with_capacity(nitems * string_len * std::mem::size_of::<u32>());
        for item in 0..nitems {
            for ch in 0..string_len {
                let value = (item * string_len + ch + 1) as u32;
                data.extend_from_slice(&value.to_le_bytes());
            }
        }
        data
    }

    fn b2nd_total_compressed_bytes(array: &B2ndArray) -> usize {
        (0..array.schunk.nchunks())
            .map(|nchunk| {
                ChunkHeader::read(array.schunk.compressed_chunk_bytes(nchunk).unwrap())
                    .unwrap()
                    .cbytes as usize
            })
            .sum()
    }

    #[test]
    fn test_copy_buffer2_copies_padded_item_region() {
        let itemsize = 2;
        let src_shape = [3, 4];
        let dst_shape = [4, 5];
        let mut src = vec![0u8; src_shape.iter().product::<i64>() as usize * itemsize];
        for item in 0..src.len() / itemsize {
            src[item * itemsize..item * itemsize + itemsize]
                .copy_from_slice(&(item as u16).to_le_bytes());
        }
        let mut dst = vec![0xff; dst_shape.iter().product::<i64>() as usize * itemsize];

        copy_buffer2(
            &src,
            &src_shape,
            &[1, 1],
            &[3, 4],
            &mut dst,
            &dst_shape,
            &[1, 2],
            itemsize,
        )
        .unwrap();

        for row in 0..2usize {
            for col in 0..3usize {
                let src_item = (row + 1) * 4 + (col + 1);
                let dst_item = (row + 1) * 5 + (col + 2);
                assert_eq!(
                    &dst[dst_item * itemsize..dst_item * itemsize + itemsize],
                    &src[src_item * itemsize..src_item * itemsize + itemsize]
                );
            }
        }
        assert_eq!(&dst[..7 * itemsize], vec![0xff; 14].as_slice());
        assert!(copy_buffer2(
            &src,
            &src_shape,
            &[2, 3],
            &[4, 4],
            &mut dst,
            &dst_shape,
            &[0, 0],
            itemsize,
        )
        .is_err());
    }

    #[test]
    fn test_b2nd_meta_default_dtype_matches_c_context() {
        for typesize in [1usize, 2, 255] {
            let meta = B2ndMeta::with_default_dtype(vec![2], vec![2], vec![1], typesize).unwrap();
            assert_eq!(meta.dtype, format!("|S{typesize}"));
            let restored = B2ndMeta::deserialize(&meta.serialize().unwrap()).unwrap();
            assert_eq!(restored, meta);
        }
        assert!(B2ndMeta::with_default_dtype(vec![2], vec![2], vec![1], 0).is_err());
    }

    #[test]
    fn test_b2nd_format_meta_includes_public_fields() {
        let meta = B2ndMeta::new(
            vec![2, 3],
            vec![2, 3],
            vec![1, 3],
            "<i2",
            DTYPE_NUMPY_FORMAT,
        )
        .unwrap();
        let array = B2ndArray::from_dense_buffer(
            meta,
            &[0u8; 12],
            CParams {
                typesize: 2,
                ..Default::default()
            },
            DParams::default(),
        )
        .unwrap();
        let formatted = array.format_meta();
        assert!(formatted.contains("shape: [2, 3]"));
        assert!(formatted.contains("chunkshape: [2, 3]"));
        assert!(formatted.contains("blockshape: [1, 3]"));
        assert!(formatted.contains("dtype: <i2"));
        assert!(formatted.contains("dtype_format: 0"));
        assert!(formatted.contains("typesize: 2"));
    }

    #[test]
    fn test_b2nd_plugin_registry_roundtrips_frame_and_reopen() {
        const FILTER_ID: u8 = BLOSC2_USER_DEFINED_FILTERS_START + 88;
        const CODEC_ID: u8 = BLOSC2_USER_DEFINED_CODECS_START + 88;

        filters::register_filter(FILTER_ID, xor_b2nd_plugin_filter, xor_b2nd_plugin_filter)
            .unwrap();
        codecs::register_codec(
            CODEC_ID,
            sequence_b2nd_plugin_codec_compress,
            sequence_b2nd_plugin_codec_decompress,
        )
        .unwrap();

        let filter_meta = B2ndMeta::new(
            vec![4, 3],
            vec![2, 3],
            vec![1, 3],
            "<u2",
            DTYPE_NUMPY_FORMAT,
        )
        .unwrap();
        let filter_data: Vec<u8> = (0..12u16).flat_map(u16::to_le_bytes).collect();
        let mut filter_ids = [0; BLOSC2_MAX_FILTERS];
        let mut filter_meta_bytes = [0; BLOSC2_MAX_FILTERS];
        filter_ids[BLOSC2_MAX_FILTERS - 1] = FILTER_ID;
        filter_meta_bytes[BLOSC2_MAX_FILTERS - 1] = 0xa5;
        let filter_array = B2ndArray::from_dense_buffer(
            filter_meta,
            &filter_data,
            CParams {
                compcode: BLOSC_LZ4,
                typesize: 2,
                filters: filter_ids,
                filters_meta: filter_meta_bytes,
                ..Default::default()
            },
            DParams::default(),
        )
        .unwrap();
        assert_eq!(filter_array.schunk.cparams.filters, filter_ids);
        assert_b2nd_frame_and_reopen_roundtrip(&filter_array, &filter_data);

        let codec_meta =
            B2ndMeta::new(vec![24], vec![24], vec![24], "|u1", DTYPE_NUMPY_FORMAT).unwrap();
        let codec_data: Vec<u8> = (0..24u8).collect();
        let codec_array = B2ndArray::from_dense_buffer(
            codec_meta,
            &codec_data,
            CParams {
                compcode: CODEC_ID,
                compcode_meta: 0x2a,
                typesize: 1,
                filters: [0; BLOSC2_MAX_FILTERS],
                ..Default::default()
            },
            DParams::default(),
        )
        .unwrap();
        assert_eq!(codec_array.schunk.cparams.compcode, CODEC_ID);
        assert_eq!(codec_array.schunk.cparams.compcode_meta, 0x2a);
        assert_b2nd_frame_and_reopen_roundtrip(&codec_array, &codec_data);
    }

    #[test]
    fn test_b2nd_context_filter_receives_b2nd_metalayer() {
        let _guard = CONTEXT_B2ND_FILTER_LOCK.lock().unwrap();
        const FILTER_ID: u8 = BLOSC2_USER_DEFINED_FILTERS_START + 89;
        filters::register_context_filter(
            FILTER_ID,
            record_b2nd_context_filter,
            record_b2nd_context_filter,
        )
        .unwrap();
        CONTEXT_FORWARD_B2ND_BLOCK0.store(0, Ordering::SeqCst);
        CONTEXT_BACKWARD_B2ND_BLOCK0.store(0, Ordering::SeqCst);

        let meta = B2ndMeta::new(vec![64, 64], vec![64, 64], vec![8, 8], "|u1", 0).unwrap();
        let data: Vec<u8> = (0..=255).cycle().take(4096).collect();
        let mut filters = [BLOSC_NOFILTER; BLOSC2_MAX_FILTERS];
        filters[BLOSC2_MAX_FILTERS - 1] = FILTER_ID;
        let array = B2ndArray::from_dense_buffer(
            meta,
            &data,
            CParams {
                compcode: BLOSC_LZ4,
                clevel: 5,
                typesize: 1,
                filters,
                ..Default::default()
            },
            DParams::default(),
        )
        .unwrap();

        assert_eq!(
            array.schunk.cparams.filters[BLOSC2_MAX_FILTERS - 1],
            FILTER_ID
        );
        assert_eq!(
            ChunkHeader::read(array.schunk.compressed_chunk_bytes(0).unwrap())
                .unwrap()
                .filters[BLOSC2_MAX_FILTERS - 1],
            FILTER_ID
        );
        assert_eq!(CONTEXT_FORWARD_B2ND_BLOCK0.load(Ordering::SeqCst), 8);
        assert_eq!(array.to_dense_buffer().unwrap(), data);
        assert_eq!(CONTEXT_BACKWARD_B2ND_BLOCK0.load(Ordering::SeqCst), 8);
    }

    #[test]
    fn test_b2nd_reopened_orthogonal_selection_passes_b2nd_metalayer() {
        let meta = B2ndMeta::new(vec![4, 4], vec![2, 2], vec![1, 2], "|u1", 0).unwrap();
        let data: Vec<u8> = (0..16u8).collect();
        let mut filter_pipeline = [BLOSC_NOFILTER; BLOSC2_MAX_FILTERS];
        filter_pipeline[BLOSC2_MAX_FILTERS - 1] = BLOSC_FILTER_NDCELL;
        let array = B2ndArray::from_dense_buffer(
            meta,
            &data,
            CParams {
                compcode: BLOSC_LZ4,
                clevel: 5,
                typesize: 1,
                splitmode: BLOSC_NEVER_SPLIT,
                filters: filter_pipeline,
                ..Default::default()
            },
            DParams::default(),
        )
        .unwrap();

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("orthogonal-context.b2frame");
        array.save(&path).unwrap();
        let reopened = B2ndArray::open(&path).unwrap();

        assert_eq!(
            reopened
                .select_orthogonal(&[vec![1, 3], vec![0, 2]])
                .unwrap(),
            vec![4, 6, 12, 14]
        );
    }

    #[test]
    fn test_b2nd_context_codec_receives_b2nd_metalayer() {
        let _guard = CONTEXT_B2ND_CODEC_LOCK.lock().unwrap();
        const CODEC_ID: u8 = BLOSC2_USER_DEFINED_CODECS_START + 89;
        codecs::register_context_codec(
            CODEC_ID,
            record_b2nd_context_codec_compress,
            record_b2nd_context_codec_decompress,
        )
        .unwrap();
        CONTEXT_COMPRESS_B2ND_BLOCK0.store(0, Ordering::SeqCst);
        CONTEXT_DECOMPRESS_B2ND_BLOCK0.store(0, Ordering::SeqCst);

        let meta = B2ndMeta::new(vec![4096], vec![4096], vec![64], "|u1", 0).unwrap();
        let data: Vec<u8> = (0..=255).cycle().take(4096).collect();
        let array = B2ndArray::from_dense_buffer(
            meta,
            &data,
            CParams {
                compcode: CODEC_ID,
                compcode_meta: 7,
                clevel: 5,
                typesize: 1,
                filters: [BLOSC_NOFILTER; BLOSC2_MAX_FILTERS],
                ..Default::default()
            },
            DParams::default(),
        )
        .unwrap();

        assert_eq!(CONTEXT_COMPRESS_B2ND_BLOCK0.load(Ordering::SeqCst), 64);
        assert_eq!(array.to_dense_buffer().unwrap(), data);
        assert_eq!(CONTEXT_DECOMPRESS_B2ND_BLOCK0.load(Ordering::SeqCst), 64);
    }
    use crate::schunk::Schunk;

    #[test]
    fn test_b2nd_meta_matches_c_layout() {
        let meta = B2ndMeta::new(
            vec![10, 20],
            vec![4, 5],
            vec![2, 5],
            "<i4",
            DTYPE_NUMPY_FORMAT,
        )
        .unwrap();
        let encoded = meta.serialize().unwrap();
        assert_eq!(encoded[0], 0x97);
        assert_eq!(encoded[1], B2ND_METALAYER_VERSION);
        assert_eq!(encoded[2], 2);
        assert_eq!(b2nd_serialize_meta(&meta).unwrap(), encoded);
        let mut encoded_dest = vec![0u8; encoded.len()];
        assert_eq!(
            b2nd_serialize_meta_c(&meta, &mut encoded_dest),
            encoded.len() as i32
        );
        assert_eq!(encoded_dest, encoded);
        assert_eq!(
            b2nd_serialize_meta_c(&meta, &mut encoded_dest[..encoded.len() - 1]),
            BLOSC2_ERROR_WRITE_BUFFER
        );
        let negative_dtype_format_meta = B2ndMeta {
            shape: vec![10, 20],
            chunkshape: vec![4, 5],
            blockshape: vec![2, 5],
            dtype: "<i4".to_string(),
            dtype_format: -1,
        };
        assert_eq!(
            b2nd_serialize_meta_c(&negative_dtype_format_meta, &mut encoded_dest),
            BLOSC2_ERROR_FAILURE
        );

        let decoded = B2ndMeta::deserialize(&encoded).unwrap();
        assert_eq!(decoded, meta);
        assert_eq!(b2nd_deserialize_meta(&encoded).unwrap(), meta);
        assert_eq!(
            b2nd_deserialize_meta_c(&encoded),
            (encoded.len() as i32, Some(meta.clone()))
        );
        let mut c_lenient_encoded = encoded.clone();
        c_lenient_encoded[0] = 0x91;
        c_lenient_encoded[1] = 127;
        assert_eq!(
            B2ndMeta::deserialize(&c_lenient_encoded).err(),
            Some("Invalid B2ND metadata")
        );
        assert_eq!(
            b2nd_deserialize_meta_c(&c_lenient_encoded),
            (c_lenient_encoded.len() as i32, Some(meta.clone()))
        );
        let mut invalid_ndim_encoded = encoded.clone();
        invalid_ndim_encoded[2] = (B2ND_MAX_DIM + 1) as u8;
        assert_eq!(
            b2nd_deserialize_meta_c(&invalid_ndim_encoded),
            (BLOSC2_ERROR_FAILURE, None)
        );
        let dtype_start = 3 + 1 + 2 * 9 + 1 + 2 * 5 + 1 + 2 * 5;
        let legacy_without_dtype = encoded[..dtype_start].to_vec();
        assert_eq!(
            B2ndMeta::deserialize(&legacy_without_dtype).err(),
            Some("Truncated B2ND metadata")
        );
        let c_legacy_meta = B2ndMeta {
            shape: vec![10, 20],
            chunkshape: vec![4, 5],
            blockshape: vec![2, 5],
            dtype: String::new(),
            dtype_format: DTYPE_NUMPY_FORMAT,
        };
        assert_eq!(
            b2nd_deserialize_meta_c(&legacy_without_dtype),
            (legacy_without_dtype.len() as i32, Some(c_legacy_meta))
        );
        let mut c_raw_meta_encoded = encoded.clone();
        c_raw_meta_encoded[3 + 1 + 2 * 9] = 0x92;
        let chunk_start = 3 + 1 + 2 * 9 + 1;
        c_raw_meta_encoded[chunk_start] = 0xd2;
        c_raw_meta_encoded[chunk_start + 1..chunk_start + 5]
            .copy_from_slice(&(-4i32).to_be_bytes());
        let dtype_format_pos = dtype_start;
        c_raw_meta_encoded[dtype_format_pos] = 0xff;
        let dtype_payload_pos = dtype_format_pos + 1 + 1 + 4;
        c_raw_meta_encoded[dtype_payload_pos] = 0xff;
        assert!(B2ndMeta::deserialize(&c_raw_meta_encoded).is_err());
        let (raw_rc, raw_meta) = b2nd_deserialize_meta_c(&c_raw_meta_encoded);
        assert_eq!(raw_rc, c_raw_meta_encoded.len() as i32);
        let raw_meta = raw_meta.unwrap();
        assert_eq!(raw_meta.shape, vec![10, 20]);
        assert_eq!(raw_meta.chunkshape, vec![-4, 5]);
        assert_eq!(raw_meta.dtype_format, -1);
        assert!(raw_meta.dtype.starts_with('\u{fffd}'));
        let mut invalid_encoded = encoded.clone();
        invalid_encoded.push(0xff);
        assert_eq!(
            b2nd_deserialize_meta_c(&invalid_encoded),
            (encoded.len() as i32, Some(meta.clone()))
        );

        let default_dtype_encoded = b2nd_serialize_meta_parts(
            vec![10, 20],
            vec![4, 5],
            vec![2, 5],
            None,
            DTYPE_NUMPY_FORMAT,
        )
        .unwrap();
        assert_eq!(
            B2ndMeta::deserialize(&default_dtype_encoded).unwrap().dtype,
            "|u1"
        );
        let mut default_dtype_dest = vec![0u8; default_dtype_encoded.len()];
        assert_eq!(
            b2nd_serialize_meta_parts_c(
                vec![10, 20],
                vec![4, 5],
                vec![2, 5],
                None,
                DTYPE_NUMPY_FORMAT,
                &mut default_dtype_dest,
            ),
            default_dtype_encoded.len() as i32
        );
        assert_eq!(default_dtype_dest, default_dtype_encoded);
        let mut raw_dest = vec![0u8; 64];
        let raw_len = b2nd_serialize_meta_parts_c(
            vec![10],
            vec![-4],
            vec![0],
            Some("<i4"),
            DTYPE_NUMPY_FORMAT,
            &mut raw_dest,
        );
        assert!(raw_len > 0);
        let raw_meta = b2nd_deserialize_meta_c(&raw_dest[..raw_len as usize])
            .1
            .unwrap();
        assert_eq!(raw_meta.shape, vec![10]);
        assert_eq!(raw_meta.chunkshape, vec![-4]);
        assert_eq!(raw_meta.blockshape, vec![0]);
        assert_eq!(raw_meta.dtype, "<i4");
        let mut extra_dims_dest = vec![0u8; 128];
        let extra_dims_len = b2nd_serialize_meta_parts_c(
            vec![10],
            vec![4, 99],
            vec![2, 88],
            Some("<i4"),
            DTYPE_NUMPY_FORMAT,
            &mut extra_dims_dest,
        );
        assert!(extra_dims_len > 0);
        let extra_dims_meta = b2nd_deserialize_meta_c(&extra_dims_dest[..extra_dims_len as usize])
            .1
            .unwrap();
        assert_eq!(extra_dims_meta.shape, vec![10]);
        assert_eq!(extra_dims_meta.chunkshape, vec![4]);
        assert_eq!(extra_dims_meta.blockshape, vec![2]);
        assert_eq!(
            b2nd_serialize_meta_parts_c(
                vec![10, 20],
                vec![4, 5],
                vec![2, 5],
                Some("<i4"),
                -1,
                &mut default_dtype_dest,
            ),
            BLOSC2_ERROR_FAILURE
        );
        assert_eq!(
            B2ndMeta::with_default_dtype(vec![10, 20], vec![4, 5], vec![2, 5], 2)
                .unwrap()
                .dtype,
            "|S2"
        );
    }

    #[test]
    fn test_b2nd_meta_rejects_trailing_bytes() {
        let meta = B2ndMeta::new(vec![2], vec![2], vec![1], "|u1", 0).unwrap();
        let mut encoded = meta.serialize().unwrap();
        encoded.push(0xff);
        assert_eq!(
            B2ndMeta::deserialize(&encoded).err(),
            Some("Invalid B2ND metadata length")
        );
    }

    #[test]
    fn test_b2nd_meta_allows_scalar_empty_dtype_and_16d() {
        let scalar =
            B2ndMeta::new(Vec::new(), Vec::new(), Vec::new(), "", DTYPE_NUMPY_FORMAT).unwrap();
        let encoded = scalar.serialize().unwrap();
        assert_eq!(encoded[3], 0x90);
        assert_eq!(B2ndMeta::deserialize(&encoded).unwrap(), scalar);

        let meta16 = B2ndMeta::new(vec![1; 16], vec![1; 16], vec![1; 16], "", 0).unwrap();
        let encoded = meta16.serialize().unwrap();
        assert_eq!(encoded[3], 0xa0);
        assert_eq!(B2ndMeta::deserialize(&encoded).unwrap(), meta16);
    }

    #[test]
    fn test_b2nd_array_frame_roundtrip() {
        let meta = B2ndMeta::new(vec![5, 7], vec![3, 4], vec![3, 2], "<u2", 0).unwrap();
        let mut data: Vec<u8> = (0..35u16).flat_map(u16::to_le_bytes).collect();
        let expected = data.clone();
        data.extend_from_slice(b"trailing bytes ignored");
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 2,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };

        let array =
            B2ndArray::from_dense_buffer(meta.clone(), &data, cparams, DParams::default()).unwrap();
        assert_eq!(
            array.schunk.metalayer(B2ND_METALAYER_NAME).unwrap(),
            meta.serialize().unwrap()
        );
        assert_eq!(array.to_dense_buffer().unwrap(), expected);

        let frame = array.to_contiguous_frame();
        let restored = B2ndArray::from_contiguous_frame(&frame).unwrap();
        assert_eq!(restored.meta, meta);
        assert_eq!(restored.to_dense_buffer().unwrap(), expected);
    }

    #[test]
    fn test_b2nd_string_shuffle_uses_filters_meta_character_size() {
        let string_len = 10usize;
        let charsize = std::mem::size_of::<u32>();
        let typesize = string_len * charsize;
        let cases: &[(Vec<i64>, Vec<i32>, Vec<i32>)] = &[
            (vec![40, 40], vec![20, 20], vec![10, 10]),
            (vec![40, 55, 23], vec![31, 5, 22], vec![4, 4, 4]),
            (vec![40, 0, 12], vec![31, 0, 12], vec![10, 0, 12]),
            (
                vec![50, 60, 31, 12],
                vec![25, 20, 20, 10],
                vec![5, 5, 5, 10],
            ),
            (
                vec![1, 1, 1024, 1, 1],
                vec![1, 1, 500, 1, 1],
                vec![1, 1, 200, 1, 1],
            ),
            (
                vec![5, 1, 50, 3, 1, 2],
                vec![5, 1, 50, 2, 1, 2],
                vec![2, 1, 20, 2, 1, 2],
            ),
            (
                vec![2, 3, 1, 1, 1, 1, 8, 1, 2, 2, 1, 1, 1, 1, 1, 2],
                vec![1, 2, 1, 1, 1, 2, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1],
                vec![1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
            ),
        ];

        for (shape, chunkshape, blockshape) in cases {
            let meta = B2ndMeta::new(
                shape.clone(),
                chunkshape.clone(),
                blockshape.clone(),
                format!("|S{typesize}"),
                DTYPE_NUMPY_FORMAT,
            )
            .unwrap();
            let nitems = meta.nitems().unwrap();
            let data = b2nd_string_shuffle_data(nitems, string_len);

            let mut charwise_cparams = CParams {
                compcode: BLOSC_BLOSCLZ,
                clevel: 5,
                typesize: typesize as i32,
                splitmode: BLOSC_NEVER_SPLIT,
                filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
                nthreads: 2,
                ..Default::default()
            };
            charwise_cparams.filters_meta[BLOSC2_MAX_FILTERS - 1] = charsize as u8;
            let charwise = B2ndArray::from_dense_buffer(
                meta.clone(),
                &data,
                charwise_cparams,
                DParams::default(),
            )
            .unwrap();
            assert_eq!(charwise.to_dense_buffer().unwrap(), data);
            assert_eq!(
                charwise.schunk.cparams.filters_meta[BLOSC2_MAX_FILTERS - 1],
                charsize as u8
            );
            let restored =
                B2ndArray::from_contiguous_frame(&charwise.to_contiguous_frame()).unwrap();
            assert_eq!(restored.to_dense_buffer().unwrap(), data);
            assert_eq!(
                restored.schunk.cparams.filters_meta[BLOSC2_MAX_FILTERS - 1],
                charsize as u8
            );

            let stringwise = B2ndArray::from_dense_buffer(
                meta,
                &data,
                CParams {
                    compcode: BLOSC_BLOSCLZ,
                    clevel: 5,
                    typesize: typesize as i32,
                    splitmode: BLOSC_NEVER_SPLIT,
                    filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
                    nthreads: 2,
                    ..Default::default()
                },
                DParams::default(),
            )
            .unwrap();
            assert_eq!(stringwise.to_dense_buffer().unwrap(), data);
            assert!(
                b2nd_total_compressed_bytes(&charwise) <= b2nd_total_compressed_bytes(&stringwise)
            );
        }
    }

    #[test]
    fn test_b2nd_open_offset_reads_embedded_frame() {
        let meta = B2ndMeta::new(vec![3, 4], vec![2, 3], vec![1, 3], "<u2", 0).unwrap();
        let data: Vec<u8> = (0..12u16).flat_map(u16::to_le_bytes).collect();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 2,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let array =
            B2ndArray::from_dense_buffer(meta.clone(), &data, cparams, DParams::default()).unwrap();

        let prefix = b"application-prefix";
        let mut file_data = prefix.to_vec();
        file_data.extend_from_slice(&array.to_contiguous_frame());
        file_data.extend_from_slice(b"trailer");

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("embedded-b2nd.b2frame");
        std::fs::write(&path, file_data).unwrap();

        let restored = B2ndArray::open_frame_at(&path, prefix.len() as u64).unwrap();
        let file_url = format!("file:///{}", path.display());
        let restored_from_url = B2ndArray::open_frame_at(&file_url, prefix.len() as u64).unwrap();
        assert_eq!(restored.meta, meta);
        assert_eq!(restored.to_dense_buffer().unwrap(), data);
        assert_eq!(restored_from_url.meta, meta);
        assert_eq!(restored_from_url.to_dense_buffer().unwrap(), data);
    }

    #[test]
    fn test_b2nd_open_offset_mutations_persist_and_preserve_following_frame() {
        let meta = B2ndMeta::new(vec![2, 3], vec![2, 3], vec![1, 3], "<u2", 0).unwrap();
        let first_data: Vec<u8> = (0..6u16).flat_map(u16::to_le_bytes).collect();
        let second_data: Vec<u8> = (10..16u16).flat_map(u16::to_le_bytes).collect();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 2,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let first = B2ndArray::from_dense_buffer(
            meta.clone(),
            &first_data,
            cparams.clone(),
            DParams::default(),
        )
        .unwrap();
        let second =
            B2ndArray::from_dense_buffer(meta.clone(), &second_data, cparams, DParams::default())
                .unwrap();

        let prefix = b"application-prefix";
        let first_offset = prefix.len() as u64;
        let mut file_data = prefix.to_vec();
        file_data.extend_from_slice(&first.to_contiguous_frame());
        file_data.extend_from_slice(&second.to_contiguous_frame());

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("embedded-b2nd-mutation.b2frame");
        std::fs::write(&path, file_data).unwrap();

        let mut opened = B2ndArray::open_frame_at(&path, first_offset).unwrap();
        opened
            .set_slice(&[0, 0], &[1, 1], &99u16.to_le_bytes())
            .unwrap();

        let persisted = std::fs::read(&path).unwrap();
        assert_eq!(&persisted[..prefix.len()], prefix);
        let restored = B2ndArray::open_frame_at(&path, first_offset).unwrap();
        let mut expected = first_data.clone();
        expected[..2].copy_from_slice(&99u16.to_le_bytes());
        assert_eq!(restored.to_dense_buffer().unwrap(), expected);

        let second_offset = first_offset + restored.to_contiguous_frame().len() as u64;
        assert_eq!(
            B2ndArray::open_frame_at(&path, second_offset)
                .unwrap()
                .to_dense_buffer()
                .unwrap(),
            second_data
        );
    }

    #[test]
    fn test_b2nd_sparse_nonzero_offset_mutations_persist_and_preserve_index_bytes() {
        let meta = B2ndMeta::new(vec![2, 3], vec![2, 3], vec![1, 3], "<u2", 0).unwrap();
        let data: Vec<u8> = (0..6u16).flat_map(u16::to_le_bytes).collect();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 2,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let array =
            B2ndArray::from_dense_buffer(meta.clone(), &data, cparams, DParams::default()).unwrap();

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("embedded-sparse-b2nd.b2frame");
        array.save_sframe(&path).unwrap();

        let index_path = path.join("chunks.b2frame");
        let prefix = b"b2nd-sparse-prefix";
        let suffix = b"b2nd-sparse-suffix";
        let original_index = std::fs::read(&index_path).unwrap();
        let mut embedded_index = prefix.to_vec();
        embedded_index.extend_from_slice(&original_index);
        embedded_index.extend_from_slice(suffix);
        std::fs::write(&index_path, embedded_index).unwrap();

        let offset = prefix.len() as u64;
        let mut opened = B2ndArray::open_frame_at(&path, offset).unwrap();
        opened
            .set_slice(&[0, 0], &[1, 1], &99u16.to_le_bytes())
            .unwrap();

        let persisted_index = std::fs::read(&index_path).unwrap();
        assert_eq!(&persisted_index[..prefix.len()], prefix);
        assert!(persisted_index.ends_with(suffix));
        let restored = B2ndArray::open_frame_at(&path, offset).unwrap();
        let mut expected = data.clone();
        expected[..2].copy_from_slice(&99u16.to_le_bytes());
        assert_eq!(restored.meta, meta);
        assert_eq!(restored.to_dense_buffer().unwrap(), expected);
    }

    #[test]
    fn test_b2nd_save_append_returns_openable_offsets() {
        let meta = B2ndMeta::new(vec![2, 3], vec![2, 3], vec![1, 3], "<u2", 0).unwrap();
        let first_data: Vec<u8> = (0..6u16).flat_map(u16::to_le_bytes).collect();
        let second_data: Vec<u8> = (10..16u16).flat_map(u16::to_le_bytes).collect();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 2,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let first = B2ndArray::from_dense_buffer(
            meta.clone(),
            &first_data,
            cparams.clone(),
            DParams::default(),
        )
        .unwrap();
        let second =
            B2ndArray::from_dense_buffer(meta.clone(), &second_data, cparams, DParams::default())
                .unwrap();

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("append-b2nd.b2frame");
        let first_offset = first.save_append(&path).unwrap();
        let second_offset = second.save_append(&path).unwrap();

        assert_eq!(first_offset, 0);
        assert!(second_offset > first_offset);
        assert_eq!(
            B2ndArray::open_frame_at(&path, first_offset)
                .unwrap()
                .to_dense_buffer()
                .unwrap(),
            first_data
        );
        assert_eq!(
            B2ndArray::open_frame_at(&path, second_offset)
                .unwrap()
                .to_dense_buffer()
                .unwrap(),
            second_data
        );
    }

    #[test]
    fn test_b2nd_save_sframe_writes_sparse_directory() {
        let meta = B2ndMeta::new(vec![2, 2], vec![2, 2], vec![1, 2], "<u2", 0).unwrap();
        let data: Vec<u8> = (0..4u16).flat_map(u16::to_le_bytes).collect();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 2,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let array =
            B2ndArray::from_dense_buffer(meta.clone(), &data, cparams, DParams::default()).unwrap();

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("array-sframe.b2frame");
        array.save_sframe(&path).unwrap();
        assert!(path.join("chunks.b2frame").is_file());
        let restored = B2ndArray::open(&path).unwrap();
        assert_eq!(restored.meta, meta);
        assert_eq!(restored.to_dense_buffer().unwrap(), data);
    }

    #[test]
    fn test_b2nd_save_preserves_opened_storage_kind() {
        let meta = B2ndMeta::new(vec![2, 2], vec![2, 2], vec![1, 2], "<u2", 0).unwrap();
        let data: Vec<u8> = (0..4u16).flat_map(u16::to_le_bytes).collect();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 2,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let array =
            B2ndArray::from_dense_buffer(meta.clone(), &data, cparams.clone(), DParams::default())
                .unwrap();

        let dir = tempfile::tempdir().unwrap();
        let contiguous_path = dir.path().join("array.b2frame");
        array.save(&contiguous_path).unwrap();
        assert!(contiguous_path.is_dir());
        assert_eq!(
            array.save(&contiguous_path).unwrap_err().kind(),
            std::io::ErrorKind::AlreadyExists
        );
        assert_eq!(
            B2ndArray::open(&contiguous_path)
                .unwrap()
                .to_dense_buffer()
                .unwrap(),
            data
        );
        let url_path = dir.path().join("array-url.b2frame");
        array
            .save(format!("file:///{}", url_path.display()))
            .unwrap();
        assert!(url_path.is_dir());
        assert_eq!(
            B2ndArray::open(&url_path)
                .unwrap()
                .to_dense_buffer()
                .unwrap(),
            data
        );

        let from_contiguous_frame =
            B2ndArray::from_contiguous_frame(&array.to_contiguous_frame()).unwrap();
        let preserved_contiguous_path = dir.path().join("preserved-contiguous.b2frame");
        from_contiguous_frame
            .save(&preserved_contiguous_path)
            .unwrap();
        assert!(preserved_contiguous_path.is_file());
        assert_eq!(
            B2ndArray::open(&preserved_contiguous_path)
                .unwrap()
                .to_dense_buffer()
                .unwrap(),
            data
        );

        let sparse_path = dir.path().join("array-sparse.b2frame");
        array.save_sframe(&sparse_path).unwrap();
        assert_eq!(
            B2ndArray::open_frame_at(&sparse_path, 0)
                .unwrap()
                .to_dense_buffer()
                .unwrap(),
            data
        );
        let sparse = B2ndArray::open(&sparse_path).unwrap();
        let preserved_path = dir.path().join("preserved-sparse.b2frame");
        sparse.save(&preserved_path).unwrap();
        assert!(preserved_path.join("chunks.b2frame").is_file());
        let restored = B2ndArray::open(&preserved_path).unwrap();
        assert_eq!(restored.meta, meta);
        assert_eq!(restored.to_dense_buffer().unwrap(), data);
    }

    #[cfg(unix)]
    #[test]
    fn test_b2nd_contiguous_paths_preserve_non_utf8_bytes() {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt;

        let data: Vec<u8> = (0..16).collect();
        let meta = B2ndMeta::with_default_dtype(vec![16], vec![16], vec![8], 1).unwrap();
        let cparams = CParams {
            typesize: 1,
            ..Default::default()
        };
        let mut array =
            B2ndArray::from_dense_buffer(meta, &data, cparams, DParams::default()).unwrap();
        array.schunk.set_storage(FrameStorage::Contiguous);

        let dir = tempfile::tempdir().unwrap();
        let path = dir
            .path()
            .join(OsString::from_vec(b"array-\xff.b2frame".to_vec()));
        array.save(&path).unwrap();

        let reopened = B2ndArray::open(&path).unwrap();
        assert_eq!(reopened.to_dense_buffer().unwrap(), data);
        assert!(path.is_file());
        assert!(!dir.path().join("array-\u{fffd}.b2frame").exists());
    }

    #[test]
    fn test_b2nd_rejects_chunks_larger_than_metadata() {
        let meta = B2ndMeta::with_default_dtype(vec![4], vec![4], vec![4], 1).unwrap();
        let data = [1u8, 2, 3, 4];
        let cparams = CParams {
            typesize: 1,
            ..Default::default()
        };
        let mut array =
            B2ndArray::from_dense_buffer(meta, &data, cparams, DParams::default()).unwrap();
        let smaller_meta = B2ndMeta::with_default_dtype(vec![2], vec![2], vec![2], 1).unwrap();
        let encoded = smaller_meta.serialize().unwrap();
        array
            .schunk
            .update_metalayer(B2ND_METALAYER_NAME, &encoded)
            .unwrap();

        let frame = array.schunk.to_contiguous_frame();
        let reopened = B2ndArray::from_contiguous_frame(&frame).unwrap();
        assert_eq!(
            reopened.to_dense_buffer().unwrap_err(),
            "B2ND chunk size does not match metadata"
        );
        assert_eq!(
            reopened
                .orthogonal_selection_to_dense_buffer(&[vec![0, 1]], &[2])
                .unwrap_err(),
            "B2ND chunk size does not match metadata"
        );
    }

    #[test]
    fn test_b2nd_context_storage_urlpath_creates_requested_frame_kind() {
        let meta = B2ndMeta::new(vec![2, 3], vec![2, 3], vec![1, 3], "|u1", 0).unwrap();
        let data = vec![1, 2, 3, 4, 5, 6];
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let dir = tempfile::tempdir().unwrap();

        let contiguous_path = dir.path().join("ctx-contiguous.b2frame");
        let (ctx_rc, ctx) = b2nd_create_ctx_with_storage_c(
            meta.clone(),
            cparams.clone(),
            DParams::default(),
            vec![("owner".to_string(), b"ctx-storage".to_vec())],
            B2ndStorage::contiguous_urlpath(format!("file:///{}", contiguous_path.display())),
        );
        assert_eq!(ctx_rc, BLOSC2_ERROR_SUCCESS);
        let ctx = ctx.unwrap();
        let (array_rc, array) = b2nd_from_cbuffer_ctx_c(&ctx, &data, data.len() as i64);
        assert_eq!(array_rc, BLOSC2_ERROR_SUCCESS);
        assert!(contiguous_path.is_file());
        let array = array.unwrap();
        assert_eq!(array.schunk.storage(), FrameStorage::Contiguous);
        assert_eq!(array.schunk.metalayer("owner"), Some(&b"ctx-storage"[..]));
        assert_eq!(array.to_dense_buffer().unwrap(), data);
        assert_eq!(
            B2ndArray::open(&contiguous_path)
                .unwrap()
                .to_dense_buffer()
                .unwrap(),
            data
        );
        assert_eq!(
            b2nd_from_cbuffer_ctx_c(&ctx, &data, data.len() as i64).0,
            BLOSC2_ERROR_FILE_WRITE
        );

        let sparse_path = dir.path().join("ctx-sparse.b2frame");
        let sparse_ctx = b2nd_create_ctx_with_storage(
            meta,
            cparams,
            DParams::default(),
            Vec::new(),
            B2ndStorage::sparse_urlpath(&sparse_path),
        )
        .unwrap();
        let (sparse_rc, sparse) = b2nd_from_cbuffer_ctx_c(&sparse_ctx, &data, data.len() as i64);
        assert_eq!(sparse_rc, BLOSC2_ERROR_SUCCESS);
        assert!(sparse_path.join("chunks.b2frame").is_file());
        let sparse = sparse.unwrap();
        assert_eq!(sparse.schunk.storage(), FrameStorage::Sparse);
        assert_eq!(sparse.to_dense_buffer().unwrap(), data);
        assert_eq!(
            B2ndArray::open(&sparse_path)
                .unwrap()
                .to_dense_buffer()
                .unwrap(),
            data
        );
    }

    #[test]
    fn test_b2nd_context_in_memory_storage_controls_later_save_kind() {
        let meta = B2ndMeta::new(vec![2, 2], vec![2, 2], vec![1, 2], "|u1", 0).unwrap();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let ctx = b2nd_create_ctx_with_storage(
            meta,
            cparams,
            DParams::default(),
            Vec::new(),
            B2ndStorage::in_memory(true),
        )
        .unwrap();
        let (rc, array) = b2nd_zeros_ctx_c(&ctx);
        assert_eq!(rc, BLOSC2_ERROR_SUCCESS);
        let array = array.unwrap();
        assert_eq!(array.schunk.storage(), FrameStorage::Contiguous);

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ctx-in-memory-contiguous.b2frame");
        array.save(&path).unwrap();
        assert!(path.is_file());
    }

    #[test]
    fn test_b2nd_context_default_storage_is_sparse_in_memory() {
        let meta = B2ndMeta::new(vec![2, 2], vec![2, 2], vec![1, 2], "|u1", 0).unwrap();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let ctx = b2nd_create_ctx(meta, cparams, DParams::default(), Vec::new()).unwrap();
        assert_eq!(ctx.storage, Some(B2ndStorage::default()));

        let (rc, array) = b2nd_zeros_ctx_c(&ctx);
        assert_eq!(rc, BLOSC2_ERROR_SUCCESS);
        assert_eq!(array.unwrap().schunk.storage(), FrameStorage::Sparse);
    }

    #[test]
    fn test_b2nd_context_parts_storage_creates_special_file_backed_arrays() {
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 4,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let dir = tempfile::tempdir().unwrap();

        let zeros_path = dir.path().join("parts-zeros.b2frame");
        let (ctx_rc, ctx) = b2nd_create_ctx_parts_with_storage_c(
            vec![2, 2],
            vec![2, 2],
            vec![1, 2],
            None,
            DTYPE_NUMPY_FORMAT,
            cparams.clone(),
            DParams::default(),
            vec![("source".to_string(), b"parts".to_vec())],
            B2ndStorage::contiguous_urlpath(&zeros_path),
        );
        assert_eq!(ctx_rc, BLOSC2_ERROR_SUCCESS);
        let (zeros_rc, zeros) = b2nd_zeros_ctx_c(&ctx.unwrap());
        assert_eq!(zeros_rc, BLOSC2_ERROR_SUCCESS);
        assert!(zeros_path.is_file());
        let zeros = zeros.unwrap();
        assert_eq!(zeros.schunk.storage(), FrameStorage::Contiguous);
        assert_eq!(zeros.schunk.metalayer("source"), Some(&b"parts"[..]));
        assert_eq!(
            B2ndArray::open(&zeros_path)
                .unwrap()
                .to_dense_buffer()
                .unwrap(),
            vec![0; 16]
        );

        let full_path = dir.path().join("parts-full-sparse.b2frame");
        let sparse_ctx = b2nd_create_ctx_with_storage(
            B2ndMeta::new(vec![2, 2], vec![2, 2], vec![1, 2], "<u4", 0).unwrap(),
            cparams,
            DParams::default(),
            Vec::new(),
            B2ndStorage::sparse_urlpath(&full_path),
        )
        .unwrap();
        let (full_rc, full) = b2nd_full_ctx_c(&sparse_ctx, &7u32.to_le_bytes(), 4);
        assert_eq!(full_rc, BLOSC2_ERROR_SUCCESS);
        assert!(full_path.join("chunks.b2frame").is_file());
        let expected = [7u32; 4]
            .iter()
            .flat_map(|value| value.to_le_bytes())
            .collect::<Vec<_>>();
        assert_eq!(full.unwrap().to_dense_buffer().unwrap(), expected);
        assert_eq!(
            B2ndArray::open(&full_path)
                .unwrap()
                .to_dense_buffer()
                .unwrap(),
            expected
        );
    }

    #[test]
    fn test_b2nd_scalar_empty_shape_and_caterva_fallback() {
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 2,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };

        let scalar_meta = B2ndMeta::new(Vec::new(), Vec::new(), Vec::new(), "<u2", 0).unwrap();
        let scalar = B2ndArray::from_dense_buffer(
            scalar_meta.clone(),
            &[7, 0, 9],
            cparams,
            DParams::default(),
        )
        .unwrap();
        assert_eq!(scalar.to_dense_buffer().unwrap(), vec![7, 0]);
        assert_eq!(scalar.get_slice_nchunks(&[], &[]).unwrap(), vec![0]);

        let mut legacy = scalar.schunk.clone();
        let content = legacy.remove_metalayer(B2ND_METALAYER_NAME).unwrap();
        legacy
            .add_metalayer(CATERVA_METALAYER_NAME, &content)
            .unwrap();
        let restored = B2ndArray::from_schunk(legacy).unwrap();
        assert_eq!(restored.meta, scalar_meta);

        let mut legacy_content = content.clone();
        legacy_content[0] = 0x90 + 5;
        legacy_content.truncate(6);
        assert!(B2ndMeta::deserialize(&legacy_content).is_err());

        let mut legacy_no_dtype = scalar.schunk.clone();
        legacy_no_dtype
            .remove_metalayer(B2ND_METALAYER_NAME)
            .unwrap();
        legacy_no_dtype
            .add_metalayer(CATERVA_METALAYER_NAME, &legacy_content)
            .unwrap();
        let restored_no_dtype = B2ndArray::from_schunk(legacy_no_dtype).unwrap();
        assert_eq!(
            restored_no_dtype.meta,
            B2ndMeta::new(Vec::new(), Vec::new(), Vec::new(), B2ND_DEFAULT_DTYPE, 0).unwrap()
        );

        let mut content_with_trailer = content.clone();
        content_with_trailer.extend_from_slice(b"extra");
        assert_eq!(
            B2ndMeta::deserialize(&content_with_trailer).err(),
            Some("Invalid B2ND metadata length")
        );

        let empty_meta = B2ndMeta::new(vec![0, 3], vec![2, 2], vec![1, 1], "", 0).unwrap();
        let empty = B2ndArray::from_dense_buffer(
            empty_meta.clone(),
            &[],
            CParams {
                typesize: 1,
                ..CParams::default()
            },
            DParams::default(),
        )
        .unwrap();
        assert_eq!(empty.schunk.nchunks(), 0);
        assert_eq!(empty.schunk.chunksize, 4);
        assert_eq!(empty.to_dense_buffer().unwrap(), Vec::<u8>::new());
        assert_eq!(
            B2ndArray::from_contiguous_frame(&empty.to_contiguous_frame())
                .unwrap()
                .meta,
            empty_meta
        );
        assert_eq!(
            Schunk::from_contiguous_frame(&empty.to_contiguous_frame())
                .unwrap()
                .chunksize,
            4
        );
    }

    #[test]
    fn test_b2nd_constructors_accept_fixed_metalayers() {
        let meta = B2ndMeta::new(vec![2, 3], vec![2, 3], vec![1, 3], "|u1", 0).unwrap();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let extra = [("author", b"rust".as_slice()), ("revision", &[1, 2][..])];
        let array = B2ndArray::from_cbuffer_with_metalayers(
            meta.clone(),
            &[1, 2, 3, 4, 5, 6],
            cparams.clone(),
            DParams::default(),
            &extra,
        )
        .unwrap();
        assert_eq!(array.schunk.metalayer("author"), Some(&b"rust"[..]));
        assert_eq!(array.schunk.metalayer("revision"), Some(&[1, 2][..]));
        assert_eq!(
            b2nd_from_cbuffer(
                meta.clone(),
                &[1, 2, 3, 4, 5, 6],
                cparams.clone(),
                DParams::default()
            )
            .unwrap()
            .to_dense_buffer()
            .unwrap(),
            vec![1, 2, 3, 4, 5, 6]
        );
        assert!(b2nd_from_cbuffer(
            meta.clone(),
            &[1, 2, 3],
            cparams.clone(),
            DParams::default()
        )
        .is_err());
        assert_eq!(
            b2nd_zeros(meta.clone(), cparams.clone(), DParams::default())
                .unwrap()
                .to_dense_buffer()
                .unwrap(),
            vec![0; 6]
        );
        assert_eq!(
            b2nd_empty(meta.clone(), cparams.clone(), DParams::default())
                .unwrap()
                .to_dense_buffer()
                .unwrap(),
            vec![0; 6]
        );
        assert_eq!(
            b2nd_uninit(meta.clone(), cparams.clone(), DParams::default())
                .unwrap()
                .shape(),
            &[2, 3]
        );
        assert!(B2ndArray::zeros_with_metalayers(
            meta.clone(),
            cparams.clone(),
            DParams::default(),
            &[("b2nd", b"bad")]
        )
        .is_err());

        let full = B2ndArray::full_with_metalayers(
            meta.clone(),
            &[9],
            cparams.clone(),
            DParams::default(),
            &[("kind", b"repeat")],
        )
        .unwrap();
        assert_eq!(full.schunk.metalayer("kind"), Some(&b"repeat"[..]));
        assert_eq!(full.to_dense_buffer().unwrap(), vec![9; 6]);
        assert_eq!(
            b2nd_full(meta.clone(), &[7], cparams.clone(), DParams::default())
                .unwrap()
                .to_dense_buffer()
                .unwrap(),
            vec![7; 6]
        );

        let f32_meta = B2ndMeta::new(vec![2], vec![2], vec![1], "<f4", 0).unwrap();
        let f32_cparams = CParams {
            typesize: 4,
            ..cparams
        };
        assert_eq!(
            b2nd_nans(f32_meta, f32_cparams, DParams::default())
                .unwrap()
                .to_dense_buffer()
                .unwrap()
                .len(),
            8
        );
    }

    #[test]
    fn test_b2nd_copy_preserves_data_and_user_metalayers() {
        let meta = B2ndMeta::new(vec![2, 3], vec![2, 2], vec![1, 2], "|u1", 0).unwrap();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut array = B2ndArray::from_cbuffer_with_metalayers(
            meta,
            &[1, 2, 3, 4, 5, 6],
            cparams.clone(),
            DParams::default(),
            &[("fixed", b"metadata")],
        )
        .unwrap();
        array
            .schunk
            .add_vlmetalayer("variable", b"payload")
            .unwrap();

        let same_meta_copy = array.copy_array().unwrap();
        assert_eq!(same_meta_copy.meta, array.meta);
        assert_eq!(
            same_meta_copy.to_dense_buffer().unwrap(),
            array.to_dense_buffer().unwrap()
        );
        assert_eq!(
            same_meta_copy.schunk.vlmetalayer("variable"),
            Some(&b"payload"[..])
        );

        let raw_data = array.to_dense_buffer().unwrap();
        let first_chunk_data = array.schunk.decompress_chunk(0).unwrap();
        let replacement = compress::compress_chunk(
            &first_chunk_data,
            &CParams {
                compcode: BLOSC_ZSTD,
                clevel: 7,
                typesize: 1,
                splitmode: BLOSC_NEVER_SPLIT,
                filters: [0; BLOSC2_MAX_FILTERS],
                ..Default::default()
            },
        )
        .unwrap();
        array
            .schunk
            .replace_compressed_chunk(0, &replacement)
            .unwrap();
        let raw_copy = array.copy_array().unwrap();
        assert_eq!(
            raw_copy.schunk.compressed_chunk_bytes(0).unwrap(),
            replacement.as_slice()
        );
        assert_eq!(raw_copy.to_dense_buffer().unwrap(), raw_data);
        let same_layout_threaded = array
            .copy_with_meta(
                array.meta.clone(),
                CParams {
                    nthreads: 4,
                    ..array.schunk.cparams.clone()
                },
                DParams::default(),
            )
            .unwrap();
        assert_eq!(
            same_layout_threaded
                .schunk
                .compressed_chunk_bytes(0)
                .unwrap(),
            replacement.as_slice()
        );
        assert_eq!(same_layout_threaded.schunk.cparams.nthreads, 4);
        assert_eq!(same_layout_threaded.to_dense_buffer().unwrap(), raw_data);

        let dtype_only_changed = array
            .copy_with_meta(
                B2ndMeta::new(vec![0, 0], vec![2, 2], vec![1, 2], "|S1", 0).unwrap(),
                cparams.clone(),
                DParams::default(),
            )
            .unwrap();
        assert_eq!(dtype_only_changed.meta.dtype, "|S1");
        assert_eq!(
            dtype_only_changed.schunk.compressed_chunk_bytes(0).unwrap(),
            replacement.as_slice()
        );
        assert_eq!(dtype_only_changed.to_dense_buffer().unwrap(), raw_data);

        let dst_meta = B2ndMeta::new(vec![99, 99], vec![1, 3], vec![1, 1], "|u1", 0).unwrap();
        let copied = array
            .copy_with_meta(dst_meta, cparams.clone(), DParams::default())
            .unwrap();

        assert_eq!(copied.shape(), &[2, 3]);
        assert_eq!(copied.chunkshape(), &[1, 3]);
        assert_eq!(copied.blockshape(), &[1, 1]);
        assert_eq!(copied.to_dense_buffer().unwrap(), vec![1, 2, 3, 4, 5, 6]);
        assert_eq!(copied.schunk.metalayer("fixed"), Some(&b"metadata"[..]));
        assert_eq!(copied.schunk.vlmetalayer("variable"), Some(&b"payload"[..]));

        let mut changed = copied.clone();
        changed.set_slice(&[0, 0], &[1, 1], &[99]).unwrap();
        assert_eq!(changed.to_dense_buffer().unwrap(), vec![99, 2, 3, 4, 5, 6]);
        assert_eq!(array.to_dense_buffer().unwrap(), vec![1, 2, 3, 4, 5, 6]);

        let postfilter_meta = B2ndMeta::new(vec![4], vec![4], vec![2], "|u1", 0).unwrap();
        let postfilter_source = B2ndArray::from_dense_buffer(
            postfilter_meta.clone(),
            &[1, 2, 3, 4],
            cparams.clone(),
            DParams {
                postfilter: Some(add_one_postfilter),
                ..Default::default()
            },
        )
        .unwrap();
        let materialized_copy = postfilter_source
            .copy_with_meta(postfilter_meta, cparams, DParams::default())
            .unwrap();
        assert_eq!(
            postfilter_source.to_dense_buffer().unwrap(),
            vec![2, 3, 4, 5]
        );
        assert_eq!(
            materialized_copy.to_dense_buffer().unwrap(),
            vec![2, 3, 4, 5]
        );
    }

    #[test]
    fn test_b2nd_concatenate_preserves_first_user_metalayers() {
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let meta = B2ndMeta::new(vec![2, 2], vec![2, 2], vec![1, 2], "|u1", 0).unwrap();
        let mut first = B2ndArray::from_cbuffer_with_metalayers(
            meta.clone(),
            &[1, 2, 3, 4],
            cparams.clone(),
            DParams::default(),
            &[("origin", b"first")],
        )
        .unwrap();
        first
            .schunk
            .add_vlmetalayer("vlorigin", b"first-vl")
            .unwrap();
        let second =
            B2ndArray::from_dense_buffer(meta, &[5, 6, 7, 8], cparams.clone(), DParams::default())
                .unwrap();

        let axis0 = first.concatenate(&second, 0).unwrap();
        assert_eq!(axis0.shape(), &[4, 2]);
        assert_eq!(
            axis0.to_dense_buffer().unwrap(),
            vec![1, 2, 3, 4, 5, 6, 7, 8]
        );
        assert_eq!(axis0.schunk.metalayer("origin"), Some(&b"first"[..]));
        assert_eq!(axis0.schunk.vlmetalayer("vlorigin"), Some(&b"first-vl"[..]));

        let dst_meta = B2ndMeta::new(vec![0, 0], vec![2, 2], vec![1, 1], "|u1", 0).unwrap();
        let axis1 = first
            .concatenate_with_meta(&second, 1, dst_meta, cparams.clone(), DParams::default())
            .unwrap();
        assert_eq!(axis1.shape(), &[2, 4]);
        assert_eq!(axis1.blockshape(), &[1, 1]);
        assert_eq!(
            axis1.to_dense_buffer().unwrap(),
            vec![1, 2, 5, 6, 3, 4, 7, 8]
        );

        let mut in_place = first.clone();
        in_place.concatenate_in_place(&second, 1).unwrap();
        assert_eq!(in_place.shape(), &[2, 4]);
        assert_eq!(
            in_place.to_dense_buffer().unwrap(),
            first
                .concatenate(&second, 1)
                .unwrap()
                .to_dense_buffer()
                .unwrap()
        );
        assert_eq!(in_place.schunk.metalayer("origin"), Some(&b"first"[..]));
        assert_eq!(
            in_place.schunk.vlmetalayer("vlorigin"),
            Some(&b"first-vl"[..])
        );

        let mismatched_shape = B2ndArray::from_dense_buffer(
            B2ndMeta::new(vec![3, 2], vec![2, 2], vec![1, 2], "|u1", 0).unwrap(),
            &[0; 6],
            cparams.clone(),
            DParams::default(),
        )
        .unwrap();
        assert_eq!(
            first.concatenate(&mismatched_shape, 1).err(),
            Some("B2ND concatenate shape mismatch")
        );
        let mut failed_in_place = first.clone();
        assert_eq!(
            failed_in_place
                .concatenate_in_place(&mismatched_shape, 1)
                .err(),
            Some("B2ND concatenate shape mismatch")
        );
        assert_eq!(failed_in_place.shape(), first.shape());
        assert_eq!(
            failed_in_place.to_dense_buffer().unwrap(),
            first.to_dense_buffer().unwrap()
        );
        assert_eq!(
            first.concatenate(&mismatched_shape, 2).err(),
            Some("B2ND concatenate rank mismatch")
        );

        let mismatched_rank = B2ndArray::from_dense_buffer(
            B2ndMeta::new(vec![4], vec![2], vec![1], "|u1", 0).unwrap(),
            &[0; 4],
            cparams.clone(),
            DParams::default(),
        )
        .unwrap();
        assert_eq!(
            first.concatenate(&mismatched_rank, 0).err(),
            Some("B2ND concatenate rank mismatch")
        );

        let mismatched_type = B2ndArray::from_dense_buffer(
            B2ndMeta::new(vec![2, 2], vec![2, 2], vec![1, 2], "<u2", 0).unwrap(),
            &[0; 8],
            CParams {
                typesize: 2,
                ..cparams.clone()
            },
            DParams::default(),
        )
        .unwrap();
        assert_eq!(
            first.concatenate(&mismatched_type, 0).err(),
            Some("B2ND concatenate typesize mismatch")
        );

        let scalar = B2ndArray::from_dense_buffer(
            B2ndMeta::new(Vec::new(), Vec::new(), Vec::new(), "|u1", 0).unwrap(),
            &[1],
            cparams,
            DParams::default(),
        )
        .unwrap();
        assert_eq!(
            scalar.concatenate(&scalar, 0).err(),
            Some("B2ND concatenation does not support scalar arrays")
        );
    }

    #[test]
    fn test_b2nd_concatenate_preserves_aligned_1d_raw_chunks() {
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let meta = B2ndMeta::new(vec![4], vec![2], vec![1], "|u1", 0).unwrap();
        let first = B2ndArray::from_dense_buffer(
            meta.clone(),
            &[1, 2, 3, 4],
            cparams.clone(),
            DParams::default(),
        )
        .unwrap();
        let mut second =
            B2ndArray::from_dense_buffer(meta, &[5, 6, 7, 8], cparams.clone(), DParams::default())
                .unwrap();
        let first_tail = first.schunk.compressed_chunk_bytes(1).unwrap().to_vec();
        let replacement = replace_raw_chunk(&mut second, 0, &[5, 6]);

        let combined = first.concatenate(&second, 0).unwrap();
        assert_eq!(
            combined.to_dense_buffer().unwrap(),
            vec![1, 2, 3, 4, 5, 6, 7, 8]
        );
        assert_eq!(
            combined.schunk.compressed_chunk_bytes(1).unwrap(),
            first_tail
        );
        assert_eq!(
            combined.schunk.compressed_chunk_bytes(2).unwrap(),
            replacement
        );
        assert_eq!(
            combined.schunk.compressed_chunk_bytes(3).unwrap(),
            second.schunk.compressed_chunk_bytes(1).unwrap()
        );

        let meta_2d = B2ndMeta::new(vec![2, 2], vec![2, 2], vec![2, 2], "|u1", 0).unwrap();
        let left = B2ndArray::from_dense_buffer(
            meta_2d.clone(),
            &[1, 2, 3, 4],
            cparams.clone(),
            DParams::default(),
        )
        .unwrap();
        let mut right =
            B2ndArray::from_dense_buffer(meta_2d, &[5, 6, 7, 8], cparams, DParams::default())
                .unwrap();
        let raw_right = replace_raw_chunk(&mut right, 0, &[5, 6, 7, 8]);
        let out = left.concatenate(&right, 0).unwrap();
        assert_eq!(out.shape(), &[4, 2]);
        assert_eq!(out.to_dense_buffer().unwrap(), vec![1, 2, 3, 4, 5, 6, 7, 8]);
        assert_eq!(out.schunk.compressed_chunk_bytes(1).unwrap(), raw_right);
    }

    #[test]
    fn test_b2nd_special_constructors_and_full() {
        let meta = B2ndMeta::new(vec![3, 5], vec![2, 3], vec![2, 2], "|u1", 0).unwrap();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };

        let zeros = B2ndArray::zeros(meta.clone(), cparams.clone(), DParams::default()).unwrap();
        assert_eq!(zeros.schunk.nchunks(), 4);
        assert_eq!(zeros.schunk.chunksize, 8);
        assert_eq!(zeros.to_dense_buffer().unwrap(), vec![0; 15]);
        let zero_header =
            ChunkHeader::read(zeros.schunk.compressed_chunk_bytes(0).unwrap()).unwrap();
        assert_eq!(zero_header.special_type(), BLOSC2_SPECIAL_ZERO);

        let uninit = B2ndArray::empty(meta.clone(), cparams.clone(), DParams::default()).unwrap();
        let empty_header =
            ChunkHeader::read(uninit.schunk.compressed_chunk_bytes(0).unwrap()).unwrap();
        assert_eq!(empty_header.special_type(), BLOSC2_SPECIAL_ZERO);
        assert_eq!(uninit.to_dense_buffer().unwrap(), vec![0; 15]);

        let uninit = B2ndArray::uninit(meta.clone(), cparams.clone(), DParams::default()).unwrap();
        let uninit_header =
            ChunkHeader::read(uninit.schunk.compressed_chunk_bytes(0).unwrap()).unwrap();
        assert_eq!(uninit_header.special_type(), BLOSC2_SPECIAL_UNINIT);
        assert_eq!(uninit.to_dense_buffer().unwrap().len(), 15);

        let full =
            B2ndArray::full(meta.clone(), &[7], cparams.clone(), DParams::default()).unwrap();
        assert_eq!(full.to_dense_buffer().unwrap(), vec![7; 15]);
        let full_header =
            ChunkHeader::read(full.schunk.compressed_chunk_bytes(0).unwrap()).unwrap();
        assert_eq!(full_header.special_type(), BLOSC2_SPECIAL_VALUE);
        assert!(B2ndArray::full(meta, &[7, 8], cparams, DParams::default()).is_err());
    }

    #[test]
    fn test_b2nd_nans_constructor() {
        let meta = B2ndMeta::new(vec![3], vec![2], vec![1], "<f4", 0).unwrap();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 4,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let nans = B2ndArray::nans(meta.clone(), cparams.clone(), DParams::default()).unwrap();
        let header = ChunkHeader::read(nans.schunk.compressed_chunk_bytes(0).unwrap()).unwrap();
        assert_eq!(header.special_type(), BLOSC2_SPECIAL_NAN);
        for item in nans.to_dense_buffer().unwrap().chunks_exact(4) {
            assert!(f32::from_le_bytes(item.try_into().unwrap()).is_nan());
        }

        let bad = B2ndMeta::new(vec![3], vec![2], vec![1], "|u1", 0).unwrap();
        assert_eq!(
            B2ndArray::nans(
                bad,
                CParams {
                    typesize: 1,
                    ..cparams
                },
                DParams::default(),
            )
            .err(),
            Some("NaN special only valid for 4 or 8 byte types")
        );

        let bad = B2ndMeta::new(vec![3], vec![2], vec![1], "|u2", 0).unwrap();
        assert_eq!(
            B2ndArray::nans(
                bad,
                CParams {
                    typesize: 2,
                    ..CParams::default()
                },
                DParams::default(),
            )
            .err(),
            Some("NaN special only valid for 4 or 8 byte types")
        );

        let nans64 = B2ndArray::nans(
            B2ndMeta::new(vec![2], vec![2], vec![1], "<f8", 0).unwrap(),
            CParams {
                typesize: 8,
                ..CParams::default()
            },
            DParams::default(),
        )
        .unwrap();
        for item in nans64.to_dense_buffer().unwrap().chunks_exact(8) {
            assert!(f64::from_le_bytes(item.try_into().unwrap()).is_nan());
        }

        let direct_bad_chunk = compress::blosc2_chunk_nans_with_cparams(
            6,
            &CParams {
                typesize: 2,
                ..CParams::default()
            },
        )
        .unwrap();
        assert_eq!(
            compress::decompress_chunk(&direct_bad_chunk),
            Err("NaN special only valid for 4 or 8 byte types")
        );
    }

    #[test]
    fn test_b2nd_expand_and_squeeze_views() {
        let meta = B2ndMeta::new(
            vec![1, 3, 1, 2],
            vec![1, 3, 1, 2],
            vec![1, 1, 1, 1],
            "|u1",
            0,
        )
        .unwrap();
        let data: Vec<u8> = (0..6u8).collect();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut array =
            B2ndArray::from_dense_buffer(meta, &data, cparams.clone(), DParams::default()).unwrap();
        array.schunk.add_vlmetalayer("vkeep", b"variable").unwrap();

        let squeezed = array.squeeze_view().unwrap();
        assert_eq!(squeezed.shape(), &[3, 2]);
        assert_eq!(squeezed.chunkshape(), &[3, 2]);
        assert_eq!(squeezed.blockshape(), &[1, 1]);
        assert_eq!(squeezed.to_dense_buffer().unwrap(), data);
        assert_eq!(squeezed.schunk.vlmetalayer("vkeep"), Some(&b"variable"[..]));

        let expanded = squeezed
            .expand_dims_view(&[true, false, true, false])
            .unwrap();
        assert_eq!(expanded.meta, array.meta);
        assert_eq!(expanded.to_dense_buffer().unwrap(), data);

        let squeezed_one = expanded
            .squeeze_index_view(&[true, false, false, false])
            .unwrap();
        assert_eq!(squeezed_one.shape(), &[3, 1, 2]);
        assert_eq!(squeezed_one.to_dense_buffer().unwrap(), data);

        assert!(expanded
            .expand_dims_view(&[true, false, false, false])
            .is_err());
        assert!(expanded
            .squeeze_index_view(&[false, true, false, false])
            .is_err());

        let scalar_meta = B2ndMeta::new(Vec::new(), Vec::new(), Vec::new(), "|u1", 0).unwrap();
        let scalar =
            B2ndArray::from_dense_buffer(scalar_meta, &[42], cparams, DParams::default()).unwrap();
        let scalar_expanded = scalar.expand_dims_view(&[true, true]).unwrap();
        assert_eq!(scalar_expanded.shape(), &[1, 1]);
        assert_eq!(scalar_expanded.to_dense_buffer().unwrap(), vec![42]);

        let axes16 = vec![true; B2ND_MAX_DIM];
        assert_eq!(scalar.expand_dims_view(&axes16).unwrap().shape(), &[1; 16]);
        let axes17 = vec![true; B2ND_MAX_DIM + 1];
        assert_eq!(
            scalar.expand_dims_view(&axes17).err(),
            Some("Invalid B2ND ndim")
        );
    }

    #[test]
    fn test_b2nd_expand_and_squeeze_views_share_backing() {
        let meta = B2ndMeta::new(vec![1, 4], vec![1, 2], vec![1, 1], "|u1", 0).unwrap();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut array =
            B2ndArray::from_dense_buffer(meta, &[1, 2, 3, 4], cparams, DParams::default()).unwrap();
        let mut squeezed = array.squeeze_view().unwrap();
        let mut expanded = squeezed.expand_dims_view(&[true, false]).unwrap();

        array.set_slice(&[0, 0], &[1, 1], &[99]).unwrap();
        assert_eq!(array.to_dense_buffer().unwrap(), vec![99, 2, 3, 4]);
        assert_eq!(squeezed.to_dense_buffer().unwrap(), vec![99, 2, 3, 4]);
        assert_eq!(expanded.to_dense_buffer().unwrap(), vec![99, 2, 3, 4]);

        squeezed.set_slice(&[1], &[2], &[88]).unwrap();
        assert_eq!(
            array.select_orthogonal(&[vec![0], vec![1, 2]]).unwrap(),
            vec![88, 3]
        );
        expanded.set_slice(&[0, 2], &[1, 3], &[77]).unwrap();
        assert_eq!(array.to_dense_buffer().unwrap(), vec![99, 88, 77, 4]);
        assert_eq!(squeezed.to_dense_buffer().unwrap(), vec![99, 88, 77, 4]);
        assert_eq!(expanded.to_dense_buffer().unwrap(), vec![99, 88, 77, 4]);
    }

    #[test]
    fn test_b2nd_view_owned_schunk_metadata_is_independent_and_chunks_are_shared() {
        let meta = B2ndMeta::new(vec![1, 4], vec![1, 2], vec![1, 1], "|u1", 0).unwrap();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut array =
            B2ndArray::from_dense_buffer(meta.clone(), &[1, 2, 3, 4], cparams, DParams::default())
                .unwrap();
        let mut squeezed = array.squeeze_view().unwrap();

        assert_eq!(array.shape(), &[1, 4]);
        assert_eq!(squeezed.shape(), &[4]);
        assert_eq!(
            B2ndMeta::deserialize(array.schunk.metalayer(B2ND_METALAYER_NAME).unwrap()).unwrap(),
            meta
        );
        assert_eq!(
            B2ndMeta::deserialize(squeezed.schunk.metalayer(B2ND_METALAYER_NAME).unwrap())
                .unwrap()
                .shape,
            vec![4]
        );

        array.set_slice(&[0, 0], &[1, 1], &[99]).unwrap();
        squeezed.set_slice(&[1], &[2], &[88]).unwrap();

        assert_eq!(array.to_dense_buffer().unwrap(), vec![99, 88, 3, 4]);
        assert_eq!(squeezed.to_dense_buffer().unwrap(), vec![99, 88, 3, 4]);
        assert_eq!(array.shape(), &[1, 4]);
        assert_eq!(squeezed.shape(), &[4]);
    }

    #[test]
    fn test_b2nd_public_chunk_mutation_syncs_with_shared_views_at_api_boundary() {
        let meta = B2ndMeta::new(vec![1, 4], vec![1, 2], vec![1, 1], "|u1", 0).unwrap();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut array = B2ndArray::from_dense_buffer(
            meta.clone(),
            &[1, 2, 3, 4],
            cparams.clone(),
            DParams::default(),
        )
        .unwrap();
        let mut squeezed = array.squeeze_view().unwrap();
        let replacement =
            B2ndArray::from_dense_buffer(meta, &[9, 8, 7, 6], cparams, DParams::default())
                .unwrap()
                .schunk
                .chunks[0]
                .clone();

        array.schunk.chunks[0] = replacement;
        assert_eq!(array.to_dense_buffer().unwrap(), vec![9, 8, 3, 4]);
        assert_eq!(squeezed.to_dense_buffer().unwrap(), vec![9, 8, 3, 4]);

        squeezed.set_slice(&[2], &[3], &[44]).unwrap();
        assert_eq!(array.to_dense_buffer().unwrap(), vec![9, 8, 44, 4]);
        assert_eq!(squeezed.to_dense_buffer().unwrap(), vec![9, 8, 44, 4]);
    }

    #[test]
    fn test_b2nd_expand_and_squeeze_views_should_share_backing_like_c() {
        let meta = B2ndMeta::new(vec![1, 4], vec![1, 2], vec![1, 1], "|u1", 0).unwrap();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut array =
            B2ndArray::from_dense_buffer(meta, &[1, 2, 3, 4], cparams, DParams::default()).unwrap();
        let mut squeezed = array.squeeze_view().unwrap();
        let mut expanded = squeezed.expand_dims_view(&[true, false]).unwrap();

        array.set_slice(&[0, 1], &[1, 2], &[22]).unwrap();
        assert_eq!(squeezed.to_dense_buffer().unwrap(), vec![1, 22, 3, 4]);
        assert_eq!(expanded.to_dense_buffer().unwrap(), vec![1, 22, 3, 4]);

        squeezed.set_slice(&[2], &[3], &[33]).unwrap();
        assert_eq!(array.to_dense_buffer().unwrap(), vec![1, 22, 33, 4]);
        assert_eq!(expanded.to_dense_buffer().unwrap(), vec![1, 22, 33, 4]);

        expanded.set_slice(&[0, 3], &[1, 4], &[44]).unwrap();
        assert_eq!(array.to_dense_buffer().unwrap(), vec![1, 22, 33, 44]);
        assert_eq!(squeezed.to_dense_buffer().unwrap(), vec![1, 22, 33, 44]);
    }

    #[test]
    fn test_b2nd_views_reject_extra_fixed_metalayers_and_squeeze_padded_singletons() {
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };

        let meta = B2ndMeta::new(vec![1, 2], vec![1, 2], vec![1, 1], "|u1", 0).unwrap();
        let mut array =
            B2ndArray::from_dense_buffer(meta, &[1, 2], cparams.clone(), DParams::default())
                .unwrap();
        array.schunk.add_metalayer("keep", b"fixed").unwrap();
        assert_eq!(
            array.squeeze_view().err(),
            Some("Cannot create a B2ND view with non-b2nd metalayers")
        );

        let padded_meta = B2ndMeta::new(vec![1, 2], vec![2, 2], vec![1, 1], "|u1", 0).unwrap();
        let padded =
            B2ndArray::from_dense_buffer(padded_meta, &[1, 2], cparams, DParams::default())
                .unwrap();
        let squeezed = padded.squeeze_index_view(&[true, false]).unwrap();
        assert_eq!(squeezed.shape(), &[2]);
        assert_eq!(squeezed.chunkshape(), &[2]);
        assert_eq!(squeezed.blockshape(), &[1]);
        assert_eq!(squeezed.to_dense_buffer().unwrap(), vec![1, 2]);

        let non_leading_padded =
            B2ndMeta::new(vec![2, 1], vec![2, 2], vec![1, 1], "|u1", 0).unwrap();
        let array = B2ndArray::from_dense_buffer(
            non_leading_padded,
            &[11, 22],
            CParams {
                typesize: 1,
                ..Default::default()
            },
            DParams::default(),
        )
        .unwrap();
        let squeezed = b2nd_squeeze(&array).unwrap();
        assert_eq!(squeezed.shape(), &[2]);
        assert_eq!(squeezed.chunkshape(), &[2]);
        assert_eq!(squeezed.blockshape(), &[1]);
        assert_eq!(squeezed.to_dense_buffer().unwrap(), vec![11, 0]);
        assert_eq!(
            b2nd_squeeze_index(&array, &[false, true])
                .unwrap()
                .to_dense_buffer()
                .unwrap(),
            vec![11, 0]
        );
    }

    #[test]
    fn test_b2nd_empty_dimension_allows_zero_chunk_and_block_shape() {
        let meta = B2ndMeta::new(vec![20, 0], vec![7, 0], vec![3, 0], "<u2", 0).unwrap();
        assert_eq!(meta.nitems().unwrap(), 0);
        assert_eq!(meta.chunk_nitems().unwrap(), 0);

        let oversized = B2ndMeta::new(
            vec![i64::from(i32::MAX)],
            vec![i32::MAX],
            vec![i32::MAX - 1],
            "|u1",
            0,
        )
        .unwrap();
        assert_eq!(extchunk_nitems(&oversized), Err("B2ND chunk too large"));

        let oversized_payload = B2ndMeta::new(
            vec![i64::from(BLOSC2_MAX_BUFFERSIZE) + 1],
            vec![BLOSC2_MAX_BUFFERSIZE + 1],
            vec![1],
            "|u1",
            0,
        )
        .unwrap();
        assert_eq!(
            B2ndArray::zeros(
                oversized_payload,
                CParams {
                    typesize: 1,
                    ..Default::default()
                },
                DParams::default()
            )
            .err(),
            Some("B2ND chunk too large")
        );

        assert!(B2ndMeta::new(vec![20, 1], vec![7, 0], vec![3, 0], "<u2", 0).is_err());
        assert!(B2ndMeta::new(vec![20, 0], vec![7, 0], vec![3, 1], "<u2", 0).is_err());

        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 2,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let array =
            B2ndArray::from_dense_buffer(meta.clone(), &[], cparams, DParams::default()).unwrap();
        assert_eq!(array.schunk.nchunks(), 0);
        assert_eq!(array.schunk.chunksize, 0);
        assert_eq!(array.to_dense_buffer().unwrap(), Vec::<u8>::new());

        let restored = B2ndArray::from_contiguous_frame(&array.to_contiguous_frame()).unwrap();
        assert_eq!(restored.meta, meta);
        assert_eq!(restored.to_dense_buffer().unwrap(), Vec::<u8>::new());
    }

    #[test]
    fn test_b2nd_empty_special_constructors_preserve_declared_chunksize() {
        let meta = B2ndMeta::new(vec![0, 3], vec![2, 2], vec![1, 1], "|u2", 0).unwrap();
        let cparams = CParams {
            typesize: 2,
            ..Default::default()
        };

        let zeros = B2ndArray::zeros(meta.clone(), cparams.clone(), DParams::default()).unwrap();
        assert_eq!(zeros.schunk.chunksize, 8);

        let full = B2ndArray::full(meta, &[7, 0], cparams, DParams::default()).unwrap();
        assert_eq!(full.schunk.chunksize, 8);
    }

    #[test]
    fn test_b2nd_rejects_invalid_cparams_typesize_before_casts() {
        let meta = B2ndMeta::new(vec![2, 3], vec![2, 3], vec![1, 3], "|u1", 0).unwrap();
        let empty_meta = B2ndMeta::new(vec![0, 3], vec![2, 2], vec![1, 1], "|u1", 0).unwrap();

        for typesize in [-1, 0] {
            let cparams = CParams {
                typesize,
                ..Default::default()
            };
            assert_eq!(
                B2ndArray::from_dense_buffer(
                    meta.clone(),
                    &[],
                    cparams.clone(),
                    DParams::default()
                )
                .err(),
                Some("Invalid typesize")
            );
            assert_eq!(
                B2ndArray::zeros(empty_meta.clone(), cparams.clone(), DParams::default()).err(),
                Some("Invalid typesize")
            );
            assert_eq!(
                B2ndArray::empty(empty_meta.clone(), cparams.clone(), DParams::default()).err(),
                Some("Invalid typesize")
            );
            assert_eq!(
                b2nd_create_ctx_parts_c(
                    vec![2, 3],
                    vec![2, 3],
                    vec![1, 3],
                    None,
                    DTYPE_NUMPY_FORMAT,
                    cparams,
                    DParams::default(),
                    Vec::new(),
                )
                .0,
                BLOSC2_ERROR_INVALID_PARAM
            );
        }
    }

    fn u16_bytes(values: &[u16]) -> Vec<u8> {
        values
            .iter()
            .flat_map(|value| value.to_le_bytes())
            .collect()
    }

    fn replace_raw_chunk(array: &mut B2ndArray, nchunk: usize, data: &[u8]) -> Vec<u8> {
        let alt_cparams = CParams {
            compcode: BLOSC_BLOSCLZ,
            clevel: 0,
            typesize: array.schunk.cparams.typesize,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_NOFILTER],
            ..Default::default()
        };
        let replacement = crate::compress::compress_chunk(data, &alt_cparams).unwrap();
        array
            .schunk
            .replace_compressed_chunk(nchunk as i64, &replacement)
            .unwrap();
        replacement
    }

    fn row_major_strides(shape: &[usize]) -> Vec<usize> {
        let mut stride = 1usize;
        let mut strides = vec![0; shape.len()];
        for axis in (0..shape.len()).rev() {
            strides[axis] = stride;
            stride *= shape[axis];
        }
        strides
    }

    fn unravel_row_major(mut linear: usize, shape: &[usize]) -> Vec<usize> {
        let strides = row_major_strides(shape);
        let mut idx = vec![0; shape.len()];
        for axis in 0..shape.len() {
            idx[axis] = linear / strides[axis];
            linear %= strides[axis];
        }
        idx
    }

    fn ravel_row_major(idx: &[usize], shape: &[usize]) -> usize {
        let strides = row_major_strides(shape);
        idx.iter()
            .zip(strides)
            .map(|(&idx, stride)| idx * stride)
            .sum()
    }

    fn insert_axis_expected(
        original: &[u8],
        shape: &[usize],
        axis: usize,
        start: usize,
        inserted_extent: usize,
        inserted: &[u8],
    ) -> Vec<u8> {
        let mut new_shape = shape.to_vec();
        new_shape[axis] += inserted_extent;
        let mut inserted_shape = shape.to_vec();
        inserted_shape[axis] = inserted_extent;
        let mut out = vec![0; new_shape.iter().product()];
        for (linear, byte) in out.iter_mut().enumerate() {
            let idx = unravel_row_major(linear, &new_shape);
            if (start..start + inserted_extent).contains(&idx[axis]) {
                let mut insert_idx = idx.clone();
                insert_idx[axis] -= start;
                *byte = inserted[ravel_row_major(&insert_idx, &inserted_shape)];
            } else {
                let mut source_idx = idx;
                if source_idx[axis] >= start + inserted_extent {
                    source_idx[axis] -= inserted_extent;
                }
                *byte = original[ravel_row_major(&source_idx, shape)];
            }
        }
        out
    }

    fn delete_axis_expected(
        original: &[u8],
        shape: &[usize],
        axis: usize,
        start: usize,
        len: usize,
    ) -> Vec<u8> {
        let mut new_shape = shape.to_vec();
        new_shape[axis] -= len;
        let mut out = vec![0; new_shape.iter().product()];
        for (linear, byte) in out.iter_mut().enumerate() {
            let mut source_idx = unravel_row_major(linear, &new_shape);
            if source_idx[axis] >= start {
                source_idx[axis] += len;
            }
            *byte = original[ravel_row_major(&source_idx, shape)];
        }
        out
    }

    #[test]
    fn test_b2nd_slice_set_and_resize_helpers() {
        let meta = B2ndMeta::new(vec![5, 7], vec![3, 4], vec![3, 2], "<u2", 0).unwrap();
        let values: Vec<u16> = (0..35u16).collect();
        let data = u16_bytes(&values);
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 2,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut array =
            B2ndArray::from_dense_buffer(meta.clone(), &data, cparams, DParams::default()).unwrap();
        array
            .schunk
            .add_metalayer(CATERVA_METALAYER_NAME, &meta.serialize().unwrap())
            .unwrap();
        array.schunk.add_metalayer("keep", b"fixed").unwrap();
        array.schunk.add_vlmetalayer("vkeep", b"variable").unwrap();
        assert_eq!(array.shape(), &[5, 7]);
        assert_eq!(array.chunkshape(), &[3, 4]);
        assert_eq!(array.blockshape(), &[3, 2]);

        let slice = array.get_slice(&[1, 2], &[4, 6]).unwrap();
        let mut expected_slice = Vec::new();
        for row in 1..4 {
            for col in 2..6 {
                expected_slice.push(values[row * 7 + col]);
            }
        }
        assert_eq!(slice, u16_bytes(&expected_slice));
        assert_eq!(array.get_slice_nchunks(&[0, 0], &[2, 3]).unwrap(), vec![0]);
        assert_eq!(
            array.get_slice_nchunks(&[0, 4], &[5, 7]).unwrap(),
            vec![1, 3]
        );
        assert_eq!(
            array.get_slice_nchunks(&[1, 2], &[4, 6]).unwrap(),
            vec![0, 1, 2, 3]
        );
        assert_eq!(array.get_slice_nchunks(&[1, 2], &[1, 2]).unwrap(), vec![0]);
        assert_eq!(array.get_slice_nchunks(&[4, 5], &[4, 5]).unwrap(), vec![3]);
        assert_eq!(
            array.get_slice_nchunks(&[1, 0], &[1, 7]).unwrap(),
            vec![0, 1]
        );
        assert!(array
            .get_slice_nchunks(&[3, 4], &[3, 4])
            .unwrap()
            .is_empty());
        assert!(array
            .get_slice_nchunks(&[5, 7], &[5, 7])
            .unwrap()
            .is_empty());
        assert!(array.get_slice_nchunks(&[0, 0], &[6, 1]).is_err());

        let replacement: Vec<u16> = (1000..1012).collect();
        array
            .set_slice(&[1, 2], &[4, 6], &u16_bytes(&replacement))
            .unwrap();
        assert!(array.schunk.metalayer(CATERVA_METALAYER_NAME).is_some());
        assert_eq!(array.schunk.metalayer("keep"), Some(&b"fixed"[..]));
        assert_eq!(array.schunk.vlmetalayer("vkeep"), Some(&b"variable"[..]));
        let mut expected = values.clone();
        for (idx, value) in replacement.iter().enumerate() {
            let row = 1 + idx / 4;
            let col = 2 + idx % 4;
            expected[row * 7 + col] = *value;
        }
        assert_eq!(array.to_dense_buffer().unwrap(), u16_bytes(&expected));

        array.resize(vec![6, 4]).unwrap();
        assert!(array.schunk.metalayer(CATERVA_METALAYER_NAME).is_some());
        assert_eq!(array.schunk.metalayer("keep"), Some(&b"fixed"[..]));
        assert_eq!(array.schunk.vlmetalayer("vkeep"), Some(&b"variable"[..]));
        assert_eq!(array.shape(), &[6, 4]);
        let mut resized = vec![0u16; 6 * 4];
        for row in 0..5 {
            for col in 0..4 {
                resized[row * 4 + col] = expected[row * 7 + col];
            }
        }
        assert_eq!(array.to_dense_buffer().unwrap(), u16_bytes(&resized));

        array.resize(vec![2, 3]).unwrap();
        let mut shrunk = Vec::new();
        for row in 0..2 {
            for col in 0..3 {
                shrunk.push(resized[row * 4 + col]);
            }
        }
        assert_eq!(array.to_dense_buffer().unwrap(), u16_bytes(&shrunk));
        let before_empty_insert = array.to_dense_buffer().unwrap();
        array.insert(0, 1, &[0, 3], &[]).unwrap();
        assert_eq!(array.shape(), &[2, 3]);
        assert_eq!(array.to_dense_buffer().unwrap(), before_empty_insert);
        array
            .insert_dense_buffer(1, 3, &u16_bytes(&[700, 701, 702, 703]))
            .unwrap();
        assert_eq!(array.shape(), &[2, 5]);
        assert_eq!(
            array.to_dense_buffer().unwrap(),
            u16_bytes(&[
                shrunk[0], shrunk[1], shrunk[2], 700, 701, shrunk[3], shrunk[4], shrunk[5], 702,
                703
            ])
        );
        array
            .append_dense_buffer(0, &u16_bytes(&[800, 801, 802, 803, 804]))
            .unwrap();
        assert_eq!(array.shape(), &[3, 5]);
        assert!(array.append_dense_buffer(1, &[1]).is_err());
        assert!(array.get_slice(&[0, 0], &[0, 1]).unwrap().is_empty());
        let padded = array
            .slice_to_dense_buffer(&[0, 1], &[2, 3], &[2, 3])
            .unwrap();
        assert_eq!(
            padded,
            u16_bytes(&[shrunk[1], shrunk[2], 0, shrunk[4], shrunk[5], 0])
        );

        let slice_array = array.slice(&[0, 1], &[2, 3]).unwrap();
        assert_eq!(slice_array.shape(), &[2, 2]);
        assert_eq!(slice_array.chunkshape(), &[3, 4]);
        assert_eq!(slice_array.blockshape(), &[3, 2]);
        assert_eq!(
            slice_array.to_dense_buffer().unwrap(),
            u16_bytes(&[shrunk[1], shrunk[2], shrunk[4], shrunk[5]])
        );
        assert!(array
            .slice(&[0, 0], &[0, 1])
            .unwrap()
            .to_dense_buffer()
            .unwrap()
            .is_empty());
        let alt_meta = B2ndMeta::new(vec![999, 999], vec![1, 2], vec![1, 1], "<u2", 0).unwrap();
        let alt = array
            .slice_with_meta(
                &[0, 1],
                &[2, 3],
                alt_meta,
                CParams {
                    compcode: BLOSC_BLOSCLZ,
                    clevel: 5,
                    typesize: 2,
                    splitmode: BLOSC_NEVER_SPLIT,
                    filters: [0, 0, 0, 0, 0, BLOSC_NOFILTER],
                    ..Default::default()
                },
                DParams::default(),
            )
            .unwrap();
        assert_eq!(alt.shape(), &[2, 2]);
        assert_eq!(alt.chunkshape(), &[1, 2]);
        assert_eq!(alt.blockshape(), &[1, 1]);
        assert_eq!(
            alt.to_dense_buffer().unwrap(),
            u16_bytes(&[shrunk[1], shrunk[2], shrunk[4], shrunk[5]])
        );
        let alt_meta = B2ndMeta::new(vec![999, 999], vec![1, 2], vec![1, 1], "<u2", 0).unwrap();
        let alt_with_meta = array
            .slice_with_meta_and_metalayers(
                &[0, 1],
                &[2, 3],
                alt_meta,
                CParams {
                    compcode: BLOSC_BLOSCLZ,
                    clevel: 5,
                    typesize: 2,
                    splitmode: BLOSC_NEVER_SPLIT,
                    filters: [0, 0, 0, 0, 0, BLOSC_NOFILTER],
                    ..Default::default()
                },
                DParams::default(),
                &[("slice", b"meta")],
            )
            .unwrap();
        assert_eq!(alt_with_meta.shape(), &[2, 2]);
        assert_eq!(alt_with_meta.schunk.metalayer("slice"), Some(&b"meta"[..]));
        assert_eq!(
            alt_with_meta.to_dense_buffer().unwrap(),
            u16_bytes(&[shrunk[1], shrunk[2], shrunk[4], shrunk[5]])
        );

        let before_empty_set = array.to_dense_buffer().unwrap();
        array.set_slice(&[0, 0], &[0, 1], &[]).unwrap();
        assert_eq!(array.to_dense_buffer().unwrap(), before_empty_set);
        assert!(array.set_slice(&[0, 0], &[1, 1], &[1]).is_err());
        assert!(array.delete(0, i64::MAX, 1).is_err());
    }

    #[test]
    fn test_b2nd_resize_exposes_retained_tail_padding() {
        let meta = B2ndMeta::new(vec![5], vec![3], vec![1], "|u1", 0).unwrap();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut schunk = Schunk::new(cparams, DParams::default());
        schunk.chunksize = 3;
        schunk
            .add_metalayer(B2ND_METALAYER_NAME, &meta.serialize().unwrap())
            .unwrap();
        schunk.append_buffer(&[1, 1, 1]).unwrap();
        schunk.append_buffer(&[1, 1, 1]).unwrap();
        let mut array = B2ndArray::from_schunk(schunk).unwrap();

        assert_eq!(array.to_dense_buffer().unwrap(), vec![1, 1, 1, 1, 1]);
        array.resize(vec![10]).unwrap();
        assert_eq!(
            array.to_dense_buffer().unwrap(),
            vec![1, 1, 1, 1, 1, 1, 0, 0, 0, 0]
        );
    }

    #[test]
    fn test_b2nd_c_name_slice_aliases() {
        let meta = B2ndMeta::new(vec![4, 6], vec![2, 3], vec![1, 3], "|u1", 0).unwrap();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let data: Vec<u8> = (0..24).collect();
        let array =
            b2nd_from_cbuffer(meta.clone(), &data, cparams.clone(), DParams::default()).unwrap();
        assert_eq!(
            b2nd_zeros_c(meta.clone(), cparams.clone(), DParams::default()).0,
            BLOSC2_ERROR_SUCCESS
        );
        assert_eq!(
            b2nd_empty_c(meta.clone(), cparams.clone(), DParams::default()).0,
            BLOSC2_ERROR_SUCCESS
        );
        assert_eq!(
            b2nd_uninit_c(meta.clone(), cparams.clone(), DParams::default()).0,
            BLOSC2_ERROR_SUCCESS
        );
        let (nans_u1_rc, nans_u1) = b2nd_nans_c(meta.clone(), cparams.clone(), DParams::default());
        assert_eq!(nans_u1_rc, BLOSC2_ERROR_DATA);
        let nans_u1 = nans_u1.unwrap();
        assert_eq!(nans_u1.meta, meta);
        assert_eq!(
            ChunkHeader::read(nans_u1.schunk.compressed_chunk_bytes(0).unwrap())
                .unwrap()
                .special_type(),
            BLOSC2_SPECIAL_NAN
        );
        let oversized_meta =
            B2ndMeta::new(vec![1], vec![i32::MAX], vec![1], "|u1", DTYPE_NUMPY_FORMAT).unwrap();
        let oversized_cparams = CParams {
            typesize: 1,
            ..cparams.clone()
        };
        assert_eq!(
            b2nd_nans_c(
                oversized_meta.clone(),
                oversized_cparams.clone(),
                DParams::default()
            )
            .0,
            BLOSC2_ERROR_MAX_BUFSIZE_EXCEEDED
        );
        assert_eq!(
            b2nd_from_cbuffer_c(
                oversized_meta.clone(),
                &[1],
                1,
                oversized_cparams.clone(),
                DParams::default()
            )
            .0,
            BLOSC2_ERROR_MAX_BUFSIZE_EXCEEDED
        );
        let (full_rc, full) = b2nd_full_c(
            meta.clone(),
            &[7, 99],
            2,
            cparams.clone(),
            DParams::default(),
        );
        assert_eq!(full_rc, BLOSC2_ERROR_SUCCESS);
        assert_eq!(full.unwrap().to_dense_buffer().unwrap(), vec![7; 24]);
        let (ctx_rc, ctx) = b2nd_create_ctx_c(
            meta.clone(),
            cparams.clone(),
            DParams::default(),
            vec![("owner".to_string(), b"ctx".to_vec())],
        );
        assert_eq!(ctx_rc, BLOSC2_ERROR_SUCCESS);
        let mut ctx = ctx.unwrap();
        assert_eq!(ctx.cparams.blocksize, 3);
        let parts_cparams = CParams {
            typesize: 2,
            ..cparams.clone()
        };
        let (parts_ctx_rc, parts_ctx) = b2nd_create_ctx_parts_c(
            vec![2, 3],
            vec![2, 3],
            vec![1, 3],
            None,
            DTYPE_NUMPY_FORMAT,
            parts_cparams,
            DParams::default(),
            Vec::new(),
        );
        assert_eq!(parts_ctx_rc, BLOSC2_ERROR_SUCCESS);
        assert_eq!(parts_ctx.unwrap().meta.dtype, "|S2");
        let (invalid_parts_ctx_rc, invalid_parts_ctx) = b2nd_create_ctx_parts_c(
            vec![2],
            vec![2],
            vec![-2],
            Some("|u1"),
            -1,
            cparams.clone(),
            DParams::default(),
            Vec::new(),
        );
        assert_eq!(invalid_parts_ctx_rc, BLOSC2_ERROR_SUCCESS);
        let invalid_parts_ctx = invalid_parts_ctx.unwrap();
        assert_eq!(invalid_parts_ctx.meta.blockshape, vec![-2]);
        assert_eq!(invalid_parts_ctx.meta.dtype_format, -1);
        assert_eq!(invalid_parts_ctx.cparams.blocksize, -2);
        assert_eq!(b2nd_zeros_ctx_c(&invalid_parts_ctx).0, BLOSC2_ERROR_FAILURE);
        let (invalid_storage_ctx_rc, invalid_storage_ctx) = b2nd_create_ctx_parts_with_storage_c(
            vec![2],
            vec![2],
            vec![1],
            Some("|u1"),
            -1,
            cparams.clone(),
            DParams::default(),
            Vec::new(),
            B2ndStorage::in_memory(true),
        );
        assert_eq!(invalid_storage_ctx_rc, BLOSC2_ERROR_SUCCESS);
        assert_eq!(invalid_storage_ctx.unwrap().meta.dtype_format, -1);
        let (invalid_meta_ctx_rc, invalid_meta_ctx) = b2nd_create_ctx_c(
            B2ndMeta {
                shape: vec![2],
                chunkshape: vec![2],
                blockshape: vec![-1],
                dtype: String::new(),
                dtype_format: -1,
            },
            cparams.clone(),
            DParams::default(),
            Vec::new(),
        );
        assert_eq!(invalid_meta_ctx_rc, BLOSC2_ERROR_SUCCESS);
        let invalid_meta_ctx = invalid_meta_ctx.unwrap();
        assert_eq!(invalid_meta_ctx.meta.dtype, "|S1");
        assert_eq!(invalid_meta_ctx.meta.dtype_format, -1);
        assert_eq!(invalid_meta_ctx.cparams.blocksize, -1);
        let (ctx_zeros_rc, ctx_zeros) = b2nd_zeros_ctx_c(&ctx);
        assert_eq!(ctx_zeros_rc, BLOSC2_ERROR_SUCCESS);
        assert_eq!(
            ctx_zeros.unwrap().schunk.metalayer("owner"),
            Some(&b"ctx"[..])
        );
        let (ctx_nans_rc, ctx_nans) = b2nd_nans_ctx_c(&ctx);
        assert_eq!(ctx_nans_rc, BLOSC2_ERROR_DATA);
        let ctx_nans = ctx_nans.unwrap();
        assert_eq!(ctx_nans.schunk.metalayer("owner"), Some(&b"ctx"[..]));
        assert_eq!(
            ChunkHeader::read(ctx_nans.schunk.compressed_chunk_bytes(0).unwrap())
                .unwrap()
                .special_type(),
            BLOSC2_SPECIAL_NAN
        );
        let (ctx_full_rc, ctx_full) = b2nd_full_ctx_c(&ctx, &[7, 99], 2);
        assert_eq!(ctx_full_rc, BLOSC2_ERROR_SUCCESS);
        let ctx_full = ctx_full.unwrap();
        assert_eq!(ctx_full.to_dense_buffer().unwrap(), vec![7; 24]);
        assert_eq!(ctx_full.schunk.metalayer("owner"), Some(&b"ctx"[..]));
        let (short_from_cbuffer_rc, short_from_cbuffer) = b2nd_from_cbuffer_c(
            meta.clone(),
            &data,
            (data.len() - 1) as i64,
            cparams.clone(),
            DParams::default(),
        );
        assert_eq!(short_from_cbuffer_rc, BLOSC2_ERROR_INVALID_PARAM);
        assert_eq!(
            b2nd_to_cbuffer_vec(&short_from_cbuffer.unwrap()).unwrap(),
            vec![0; data.len()]
        );
        let (short_ctx_from_cbuffer_rc, short_ctx_from_cbuffer) =
            b2nd_from_cbuffer_ctx_c(&ctx, &data, (data.len() - 1) as i64);
        assert_eq!(short_ctx_from_cbuffer_rc, BLOSC2_ERROR_INVALID_PARAM);
        let short_ctx_from_cbuffer = short_ctx_from_cbuffer.unwrap();
        assert_eq!(
            b2nd_to_cbuffer_vec(&short_ctx_from_cbuffer).unwrap(),
            vec![0; data.len()]
        );
        assert_eq!(
            short_ctx_from_cbuffer.schunk.metalayer("owner"),
            Some(&b"ctx"[..])
        );
        let (oversized_ctx_rc, oversized_ctx) = b2nd_create_ctx_c(
            oversized_meta,
            oversized_cparams,
            DParams::default(),
            Vec::new(),
        );
        assert_eq!(oversized_ctx_rc, BLOSC2_ERROR_SUCCESS);
        assert_eq!(
            b2nd_nans_ctx_c(&oversized_ctx.unwrap()).0,
            BLOSC2_ERROR_MAX_BUFSIZE_EXCEEDED
        );
        let (dup_ctx_rc, dup_ctx) = b2nd_create_ctx_c(
            meta.clone(),
            cparams.clone(),
            DParams::default(),
            vec![
                ("dup".to_string(), b"a".to_vec()),
                ("dup".to_string(), b"b".to_vec()),
            ],
        );
        assert_eq!(dup_ctx_rc, BLOSC2_ERROR_SUCCESS);
        assert_eq!(
            b2nd_empty_ctx_c(&dup_ctx.unwrap()).0,
            BLOSC2_ERROR_INVALID_PARAM
        );
        let (reserved_ctx_rc, reserved_ctx) = b2nd_create_ctx_c(
            meta.clone(),
            cparams.clone(),
            DParams::default(),
            vec![("b2nd".to_string(), b"dup".to_vec())],
        );
        assert_eq!(reserved_ctx_rc, BLOSC2_ERROR_SUCCESS);
        assert_eq!(
            b2nd_zeros_ctx_c(&reserved_ctx.unwrap()).0,
            BLOSC2_ERROR_INVALID_PARAM
        );
        let too_many_metalayers: Vec<_> = (0..BLOSC2_MAX_METALAYERS)
            .map(|idx| (format!("m{idx}"), vec![idx as u8]))
            .collect();
        assert!(b2nd_create_ctx(
            meta.clone(),
            cparams.clone(),
            DParams::default(),
            too_many_metalayers.clone()
        )
        .is_err());
        let (too_many_ctx_rc, too_many_ctx) = b2nd_create_ctx_c(
            meta.clone(),
            cparams.clone(),
            DParams::default(),
            too_many_metalayers.clone(),
        );
        assert_eq!(too_many_ctx_rc, BLOSC2_ERROR_SUCCESS);
        assert_eq!(
            b2nd_zeros_ctx_c(&too_many_ctx.unwrap()).0,
            BLOSC2_ERROR_INVALID_PARAM
        );
        let (too_many_parts_rc, too_many_parts) = b2nd_create_ctx_parts_c(
            vec![2, 3],
            vec![2, 3],
            vec![1, 3],
            Some("|u1"),
            DTYPE_NUMPY_FORMAT,
            cparams.clone(),
            DParams::default(),
            too_many_metalayers,
        );
        assert_eq!(too_many_parts_rc, BLOSC2_ERROR_SUCCESS);
        assert_eq!(
            b2nd_zeros_ctx_c(&too_many_parts.unwrap()).0,
            BLOSC2_ERROR_INVALID_PARAM
        );
        assert_eq!(b2nd_free_ctx_c(Some(ctx.clone())), BLOSC2_ERROR_SUCCESS);

        let mut zfp_cparams = CParams {
            compcode: BLOSC_CODEC_ZFP_FIXED_RATE,
            filters: [0; BLOSC2_MAX_FILTERS],
            ..cparams.clone()
        };
        assert_eq!(
            b2nd_create_ctx_c(
                meta.clone(),
                zfp_cparams.clone(),
                DParams::default(),
                Vec::new()
            )
            .0,
            BLOSC2_ERROR_SUCCESS
        );
        zfp_cparams.filters[BLOSC2_MAX_FILTERS - 1] = BLOSC_SHUFFLE;
        assert_eq!(
            b2nd_create_ctx_c(
                meta.clone(),
                zfp_cparams.clone(),
                DParams::default(),
                Vec::new()
            )
            .0,
            BLOSC2_ERROR_INVALID_PARAM
        );
        assert!(
            b2nd_create_ctx(meta.clone(), zfp_cparams, DParams::default(), Vec::new()).is_err()
        );

        assert_eq!(b2nd_to_cbuffer_vec(&array).unwrap(), data);
        let mut dense_dest = vec![0u8; data.len()];
        assert_eq!(
            b2nd_to_cbuffer(&array, &mut dense_dest),
            BLOSC2_ERROR_SUCCESS
        );
        assert_eq!(dense_dest, data);
        dense_dest.fill(0);
        assert_eq!(
            b2nd_to_cbuffer_c(&array, &mut dense_dest, data.len() as i64),
            BLOSC2_ERROR_SUCCESS
        );
        assert_eq!(dense_dest, data);
        let mut oversized_dense_dest = vec![0xff; data.len() + 2];
        assert_eq!(
            b2nd_to_cbuffer_c(&array, &mut oversized_dense_dest, (data.len() + 2) as i64),
            BLOSC2_ERROR_SUCCESS
        );
        assert_eq!(&oversized_dense_dest[..data.len()], data.as_slice());
        assert_eq!(&oversized_dense_dest[data.len()..], &[0, 0]);
        assert_eq!(
            b2nd_to_cbuffer_c(&array, &mut dense_dest, -1),
            BLOSC2_ERROR_INVALID_PARAM
        );
        assert_eq!(
            b2nd_to_cbuffer_c(&array, &mut dense_dest, (data.len() - 1) as i64),
            BLOSC2_ERROR_INVALID_PARAM
        );
        let (from_cbuffer_rc, from_cbuffer_c) = b2nd_from_cbuffer_c(
            meta.clone(),
            &[data.as_slice(), &[99]].concat(),
            data.len() as i64,
            cparams.clone(),
            DParams::default(),
        );
        assert_eq!(from_cbuffer_rc, BLOSC2_ERROR_SUCCESS);
        assert_eq!(b2nd_to_cbuffer_vec(&from_cbuffer_c.unwrap()).unwrap(), data);
        let (from_ctx_rc, from_ctx) = b2nd_from_cbuffer_ctx_c(
            &ctx,
            [data.as_slice(), &[99]].concat().as_slice(),
            data.len() as i64,
        );
        assert_eq!(from_ctx_rc, BLOSC2_ERROR_SUCCESS);
        assert_eq!(b2nd_to_cbuffer_vec(&from_ctx.unwrap()).unwrap(), data);
        assert_eq!(
            b2nd_from_cbuffer_c(meta.clone(), &data, -1, cparams.clone(), DParams::default()).0,
            BLOSC2_ERROR_INVALID_PARAM
        );
        assert_eq!(
            b2nd_from_cbuffer_c(
                meta.clone(),
                &data,
                (data.len() + 1) as i64,
                cparams.clone(),
                DParams::default()
            )
            .0,
            BLOSC2_ERROR_INVALID_PARAM
        );
        assert_eq!(
            b2nd_to_cbuffer(&array, &mut dense_dest[..data.len() - 1]),
            BLOSC2_ERROR_INVALID_PARAM
        );
        let frame = b2nd_to_cframe(&array);
        let (frame_rc, frame_c, frame_len, needs_free) = b2nd_to_cframe_c(&array);
        assert_eq!(frame_rc, BLOSC2_ERROR_SUCCESS);
        assert_eq!(frame_c.as_ref().unwrap(), &frame);
        assert_eq!(frame_len, frame.len() as i64);
        assert!(needs_free);
        let from_contiguous_frame = b2nd_from_cframe(&frame, true).unwrap();
        assert_eq!(from_contiguous_frame.meta, meta);
        assert_eq!(b2nd_to_cbuffer_vec(&from_contiguous_frame).unwrap(), data);
        match b2nd_from_cframe(&frame, false) {
            Ok(_) => panic!("borrowed copy=false frame should be rejected"),
            Err(err) => assert_eq!(err, "copy=false requires owned frame buffer"),
        }
        let (from_frame_rc, from_frame_c) =
            b2nd_from_cframe_c(&[frame.as_slice(), &[0]].concat(), frame.len() as i64, true);
        assert_eq!(from_frame_rc, BLOSC2_ERROR_SUCCESS);
        assert_eq!(b2nd_to_cbuffer_vec(&from_frame_c.unwrap()).unwrap(), data);
        assert_eq!(b2nd_from_cframe_c(&frame, -1, true).0, BLOSC2_ERROR_FAILURE);
        assert_eq!(
            b2nd_from_cframe_c(&frame, (frame.len() + 1) as i64, true).0,
            BLOSC2_ERROR_FAILURE
        );
        let alias_cparams = CParams {
            clevel: 0,
            filters: [0; BLOSC2_MAX_FILTERS],
            ..cparams.clone()
        };
        let alias_data: Vec<u8> = (10..34).collect();
        let alias_array =
            b2nd_from_cbuffer(meta.clone(), &alias_data, alias_cparams, DParams::default())
                .unwrap();
        let alias_frame = b2nd_to_cframe(&alias_array);
        let copied = b2nd_from_cframe(&alias_frame, true).unwrap();
        assert_eq!(b2nd_to_cbuffer_vec(&copied).unwrap(), alias_data);
        match b2nd_from_cframe(&alias_frame, false) {
            Ok(_) => panic!("borrowed copy=false frame should be rejected"),
            Err(err) => assert_eq!(err, "copy=false requires owned frame buffer"),
        }

        let (from_frame_view_rc, from_frame_view_c) =
            b2nd_from_cframe_c(&frame, frame.len() as i64, false);
        assert_eq!(from_frame_view_rc, BLOSC2_ERROR_INVALID_PARAM);
        assert!(from_frame_view_c.is_none());
        assert_eq!(b2nd_free_option_c(None), BLOSC2_ERROR_NULL_POINTER);
        assert_eq!(
            b2nd_free_option_c(Some(array.clone())),
            BLOSC2_ERROR_SUCCESS
        );
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("array.b2frame");
        assert_eq!(b2nd_save(&array, &path), BLOSC2_ERROR_SUCCESS);
        assert!(path.is_dir());
        assert!(b2nd_save(&array, &path) < 0);
        assert_eq!(b2nd_open(&path).unwrap().to_dense_buffer().unwrap(), data);
        let append_path = dir.path().join("array-append.b2frame");
        let offset = b2nd_save_append(&array, &append_path);
        assert!(offset >= 0);
        assert!(b2nd_open_offset(&path, -1).is_err());
        assert_eq!(
            b2nd_open_offset(&append_path, offset)
                .unwrap()
                .to_dense_buffer()
                .unwrap(),
            data
        );
        assert_eq!(b2nd_open_c(&path).0, BLOSC2_ERROR_SUCCESS);
        assert_eq!(
            b2nd_open_c(dir.path().join("missing.b2frame")).0,
            BLOSC2_ERROR_NULL_POINTER
        );
        let opened_negative_offset = b2nd_open_offset_c(&append_path, -1);
        assert_eq!(opened_negative_offset.0, BLOSC2_ERROR_NULL_POINTER);
        assert!(opened_negative_offset.1.is_none());
        assert_eq!(
            b2nd_open_offset_c(dir.path().join("missing.b2frame"), 0).0,
            BLOSC2_ERROR_NULL_POINTER
        );
        assert_eq!(
            b2nd_open_offset_c(&append_path, offset).0,
            BLOSC2_ERROR_SUCCESS
        );
        assert_eq!(
            b2nd_save(&array, dir.path().join("missing/array.b2frame")),
            BLOSC2_ERROR_FILE_OPEN
        );
        assert_eq!(
            b2nd_save_append(&array, dir.path().join("missing/array.b2frame")),
            i64::from(BLOSC2_ERROR_FILE_OPEN)
        );
        let from_schunk = b2nd_from_schunk(array.schunk.clone()).unwrap();
        assert_eq!(from_schunk.meta, meta);
        assert_eq!(
            b2nd_from_schunk_c(array.schunk.clone()).0,
            BLOSC2_ERROR_SUCCESS
        );
        let mut invalid_ndim_schunk = Schunk::new(cparams.clone(), DParams::default());
        let mut invalid_ndim_meta = meta.serialize().unwrap();
        invalid_ndim_meta[2] = (B2ND_MAX_DIM + 1) as u8;
        invalid_ndim_schunk
            .add_metalayer(B2ND_METALAYER_NAME, &invalid_ndim_meta)
            .unwrap();
        let (invalid_ndim_schunk_rc, invalid_ndim_schunk_array) =
            b2nd_from_schunk_c(invalid_ndim_schunk);
        assert_eq!(invalid_ndim_schunk_rc, BLOSC2_ERROR_FAILURE);
        assert!(invalid_ndim_schunk_array.is_none());
        assert_eq!(
            b2nd_get_slice_nchunks_vec(&array, &[1, 2], &[4, 6]).unwrap(),
            vec![0, 1, 2, 3]
        );
        assert_eq!(
            b2nd_get_slice_nchunks(&array, &[1, 2], &[4, 6]),
            (4, Some(vec![0, 1, 2, 3]))
        );
        assert_eq!(b2nd_get_slice_nchunks(&array, &[0, 0], &[0, 0]), (0, None));
        assert_eq!(
            b2nd_get_slice_nchunks(&array, &[1, 2], &[1, 2]),
            (1, Some(vec![0]))
        );
        assert_eq!(
            b2nd_get_slice_nchunks(&array, &[3, 4], &[3, 4]),
            (1, Some(vec![3]))
        );
        assert_eq!(b2nd_get_slice_nchunks(&array, &[2, 3], &[2, 3]), (0, None));
        assert_eq!(b2nd_get_slice_nchunks(&array, &[4, 6], &[4, 6]), (0, None));
        let empty_array = B2ndArray::from_dense_buffer(
            B2ndMeta::new(vec![0, 5], vec![0, 5], vec![0, 1], "|u1", 0).unwrap(),
            &[],
            cparams.clone(),
            DParams::default(),
        )
        .unwrap();
        assert_eq!(
            b2nd_get_slice_nchunks(&empty_array, &[0, 100], &[0, 101]),
            (0, None)
        );
        assert_eq!(
            b2nd_get_slice_nchunks(&empty_array, &[0, 0], &[0, 5]),
            (0, None)
        );

        let slice = b2nd_get_slice_cbuffer_vec(&array, &[1, 2], &[4, 6], &[3, 5]).unwrap();
        assert_eq!(
            slice,
            vec![8, 9, 10, 11, 0, 14, 15, 16, 17, 0, 20, 21, 22, 23, 0]
        );
        let mut slice_dest = vec![0u8; slice.len() + 2];
        assert_eq!(
            b2nd_get_slice_cbuffer(&array, &[1, 2], &[4, 6], &mut slice_dest, &[3, 5]),
            BLOSC2_ERROR_SUCCESS
        );
        assert_eq!(
            b2nd_get_slice_cbuffer_c(
                &array,
                &[1, 2],
                &[4, 6],
                &mut slice_dest,
                &[3, 5],
                slice.len() as i64
            ),
            BLOSC2_ERROR_SUCCESS
        );
        slice_dest.fill(0xff);
        let slice_dest_len = slice_dest.len();
        assert_eq!(
            b2nd_get_slice_cbuffer_c(
                &array,
                &[1, 2],
                &[4, 6],
                &mut slice_dest,
                &[3, 5],
                slice_dest_len as i64
            ),
            BLOSC2_ERROR_SUCCESS
        );
        assert_eq!(&slice_dest[..slice.len()], slice.as_slice());
        assert_eq!(&slice_dest[slice.len()..], &[0, 0]);
        let mut padded_slice_dest = vec![0xff; 5];
        assert_eq!(
            b2nd_get_slice_cbuffer_c(&array, &[0, 0], &[2, 2], &mut padded_slice_dest, &[2, 3], 5),
            BLOSC2_ERROR_SUCCESS
        );
        assert_eq!(padded_slice_dest, vec![0, 1, 0, 6, 7]);
        assert_eq!(
            b2nd_get_slice_cbuffer_c(&array, &[1, 2], &[4, 6], &mut slice_dest, &[3, 5], 13),
            BLOSC2_ERROR_INVALID_PARAM
        );
        assert_eq!(&slice_dest[..slice.len()], slice.as_slice());
        assert_eq!(
            b2nd_get_slice_cbuffer(
                &array,
                &[1, 2],
                &[4, 6],
                &mut slice_dest[..slice.len() - 1],
                &[3, 5]
            ),
            BLOSC2_ERROR_INVALID_PARAM
        );

        let dst_meta = B2ndMeta::new(vec![99, 99], vec![3, 2], vec![1, 1], "|u1", 0).unwrap();
        let slice_array = b2nd_get_slice(
            &array,
            &[1, 2],
            &[4, 6],
            dst_meta.clone(),
            cparams.clone(),
            DParams::default(),
        )
        .unwrap();
        assert_eq!(
            b2nd_get_slice_c(
                &array,
                &[1, 2],
                &[4, 6],
                dst_meta.clone(),
                cparams.clone(),
                DParams::default(),
            )
            .0,
            BLOSC2_ERROR_SUCCESS
        );
        assert_eq!(slice_array.shape(), &[3, 4]);
        assert_eq!(slice_array.chunkshape(), &[3, 2]);
        assert_eq!(
            slice_array.to_dense_buffer().unwrap(),
            vec![8, 9, 10, 11, 14, 15, 16, 17, 20, 21, 22, 23]
        );
        let (ctx_slice_rc, ctx_slice) = b2nd_get_slice_ctx_c(&mut ctx, &array, &[1, 2], &[4, 6]);
        assert_eq!(ctx_slice_rc, BLOSC2_ERROR_SUCCESS);
        let ctx_slice = ctx_slice.unwrap();
        assert_eq!(ctx.meta.shape, vec![3, 4]);
        assert_eq!(ctx_slice.shape(), &[3, 4]);
        assert_eq!(
            ctx_slice.to_dense_buffer().unwrap(),
            vec![8, 9, 10, 11, 14, 15, 16, 17, 20, 21, 22, 23]
        );
        assert_eq!(ctx_slice.schunk.metalayer("owner"), Some(&b"ctx"[..]));
        assert_eq!(
            b2nd_get_slice_ctx_c(&mut ctx, &array, &[3, 5], &[5, 7]).0,
            BLOSC2_ERROR_INVALID_PARAM
        );
        assert_eq!(ctx.meta.shape, vec![2, 2]);
        let mut source_ctx_array = array.clone();
        source_ctx_array
            .schunk
            .add_metalayer("owner", b"src")
            .expect("source fixed metalayer");
        source_ctx_array
            .schunk
            .add_vlmetalayer("source-vl", b"payload")
            .expect("source VL metalayer");
        let (ctx_copy_rc, ctx_copy) = b2nd_copy_ctx_c(&mut ctx, &source_ctx_array);
        assert_eq!(ctx_copy_rc, BLOSC2_ERROR_SUCCESS);
        let ctx_copy = ctx_copy.unwrap();
        assert_eq!(ctx.meta.shape, vec![4, 6]);
        assert_eq!(ctx_copy.shape(), &[4, 6]);
        assert_eq!(b2nd_to_cbuffer_vec(&ctx_copy).unwrap(), data);
        assert_eq!(ctx_copy.schunk.metalayer("owner"), Some(&b"src"[..]));
        assert_eq!(
            ctx_copy.schunk.vlmetalayer("source-vl"),
            Some(&b"payload"[..])
        );
        let existing_ctx_path = dir.path().join("ctx-existing.b2frame");
        std::fs::write(&existing_ctx_path, b"exists").unwrap();
        ctx.meta.shape = vec![1, 1];
        ctx.storage = Some(B2ndStorage::contiguous_urlpath(&existing_ctx_path));
        assert_eq!(
            b2nd_copy_ctx_c(&mut ctx, &source_ctx_array).0,
            BLOSC2_ERROR_FILE_WRITE
        );
        assert_eq!(ctx.meta.shape, vec![4, 6]);
        assert_eq!(
            b2nd_get_slice_ctx_c(&mut ctx, &array, &[0, 0], &[0, 5]).0,
            BLOSC2_ERROR_FILE_WRITE
        );
        assert_eq!(ctx.meta.shape, vec![0, 5]);
        let mut storage_fail_concat_left = source_ctx_array.clone();
        assert_eq!(
            b2nd_concatenate_ctx_c(&mut ctx, &mut storage_fail_concat_left, &array, 0, true).0,
            BLOSC2_ERROR_FILE_WRITE
        );
        assert_eq!(ctx.meta.shape, vec![4, 6]);
        ctx.storage = None;
        let mut concat_left = source_ctx_array.clone();
        let (ctx_concat_rc, ctx_concat) =
            b2nd_concatenate_ctx_c(&mut ctx, &mut concat_left, &array, 0, true);
        assert_eq!(ctx_concat_rc, BLOSC2_ERROR_SUCCESS);
        let ctx_concat = ctx_concat.unwrap();
        assert_eq!(ctx.meta.shape, vec![4, 6]);
        assert_eq!(ctx_concat.shape(), &[8, 6]);
        assert_eq!(
            b2nd_to_cbuffer_vec(&ctx_concat).unwrap(),
            [data.as_slice(), data.as_slice()].concat()
        );
        assert_eq!(ctx_concat.schunk.metalayer("owner"), Some(&b"src"[..]));
        assert_eq!(
            ctx_concat.schunk.vlmetalayer("source-vl"),
            Some(&b"payload"[..])
        );
        let mut ctx_concat_in_place_left = source_ctx_array.clone();
        let (ctx_concat_in_place_rc, ctx_concat_in_place) =
            b2nd_concatenate_ctx_c(&mut ctx, &mut ctx_concat_in_place_left, &array, 0, false);
        assert_eq!(ctx_concat_in_place_rc, BLOSC2_ERROR_SUCCESS);
        let ctx_concat_in_place = ctx_concat_in_place.unwrap();
        assert_eq!(ctx_concat_in_place.shape(), &[8, 6]);
        assert_eq!(
            b2nd_to_cbuffer_vec(&ctx_concat_in_place).unwrap(),
            [data.as_slice(), data.as_slice()].concat()
        );
        assert_eq!(
            b2nd_to_cbuffer_vec(&ctx_concat_in_place_left).unwrap(),
            [data.as_slice(), data.as_slice()].concat()
        );
        let mut ctx_concat_axis_left = source_ctx_array.clone();
        let (ctx_concat_axis_rc, ctx_concat_axis) =
            b2nd_concatenate_ctx_axis_c(&mut ctx, &mut ctx_concat_axis_left, &array, 0, false);
        assert_eq!(ctx_concat_axis_rc, BLOSC2_ERROR_SUCCESS);
        assert_eq!(ctx_concat_axis.unwrap().shape(), &[8, 6]);
        let previous_ctx_shape = ctx.meta.shape.clone();
        let incompatible_meta =
            B2ndMeta::new(vec![4, 5], vec![2, 3], vec![1, 3], "|u1", 0).unwrap();
        let incompatible = b2nd_from_cbuffer(
            incompatible_meta,
            &(0..20).collect::<Vec<u8>>(),
            cparams.clone(),
            DParams::default(),
        )
        .unwrap();
        assert_eq!(
            b2nd_concatenate_ctx_c(&mut ctx, &mut concat_left, &incompatible, 0, true).0,
            BLOSC2_ERROR_INVALID_PARAM
        );
        assert_eq!(ctx.meta.shape, previous_ctx_shape);
        assert_eq!(
            b2nd_concatenate_ctx_axis_c(&mut ctx, &mut concat_left, &array, -1, true).0,
            BLOSC2_ERROR_INVALID_PARAM
        );
        assert_eq!(ctx.meta.shape, previous_ctx_shape);
        assert_eq!(
            b2nd_concatenate_axis_c(
                &mut concat_left,
                &array,
                -1,
                true,
                meta.clone(),
                cparams.clone(),
                DParams::default()
            )
            .0,
            BLOSC2_ERROR_INVALID_PARAM
        );

        let mut target = b2nd_zeros(meta, cparams.clone(), DParams::default()).unwrap();
        assert_eq!(
            b2nd_set_slice_cbuffer(&slice, &[3, 5], &[1, 2], &[4, 6], &mut target),
            BLOSC2_ERROR_SUCCESS
        );
        assert_eq!(
            b2nd_set_slice_cbuffer_c(
                &slice,
                slice.len() as i64,
                &[3, 5],
                &[1, 2],
                &[4, 6],
                &mut target
            ),
            BLOSC2_ERROR_SUCCESS
        );
        assert_eq!(
            b2nd_set_slice_cbuffer_c(&slice, -1, &[3, 5], &[1, 2], &[4, 6], &mut target),
            BLOSC2_ERROR_INVALID_PARAM
        );
        assert_eq!(
            b2nd_set_slice_cbuffer(
                &slice[..slice.len() - 4],
                &[3, 4],
                &[1, 2],
                &[4, 6],
                &mut target
            ),
            BLOSC2_ERROR_INVALID_PARAM
        );
        let mut expected = vec![0; 24];
        for row in 1..4 {
            for col in 2..6 {
                expected[row * 6 + col] = data[row * 6 + col];
            }
        }
        assert_eq!(target.to_dense_buffer().unwrap(), expected);

        let selected = b2nd_get_orthogonal_selection(&array, &[vec![0, 2], vec![1, 3]]).unwrap();
        assert_eq!(selected, vec![1, 3, 13, 15]);
        let selected_padded =
            b2nd_get_orthogonal_selection_cbuffer(&array, &[vec![0, 2], vec![1, 3]], &[2, 2])
                .unwrap();
        assert_eq!(selected_padded, vec![1, 3, 13, 15]);
        let mut selected_dest = vec![0u8; 4];
        assert_eq!(
            b2nd_get_orthogonal_selection_cbuffer_c(
                &array,
                &[vec![0, 2], vec![1, 3]],
                &mut selected_dest,
                &[2, 2],
                4,
            ),
            BLOSC2_ERROR_SUCCESS
        );
        assert_eq!(selected_dest, vec![1, 3, 13, 15]);
        let mut selected_dest_wide = vec![0xff; 6];
        assert_eq!(
            b2nd_get_orthogonal_selection_cbuffer_c(
                &array,
                &[vec![0, 2], vec![1, 3]],
                &mut selected_dest_wide,
                &[2, 2],
                6,
            ),
            BLOSC2_ERROR_INVALID_PARAM
        );
        assert_eq!(selected_dest_wide, vec![0xff; 6]);
        selected_dest_wide.fill(0xff);
        assert_eq!(
            b2nd_get_orthogonal_selection_count_c(
                &array,
                &[vec![0, 2], vec![1, 3], vec![99]],
                2,
                &mut selected_dest_wide,
                &[2, 2],
                6,
            ),
            BLOSC2_ERROR_INVALID_PARAM
        );
        assert_eq!(selected_dest_wide, vec![0xff; 6]);
        assert_eq!(
            b2nd_get_orthogonal_selection_count_c(
                &array,
                &[vec![0, 2], vec![1, 3]],
                3,
                &mut selected_dest_wide,
                &[2, 2],
                6,
            ),
            BLOSC2_ERROR_INVALID_PARAM
        );
        let rows_with_extra = [0, 2, 99];
        let cols_with_extra = [1, 3, 99];
        assert_eq!(
            b2nd_get_orthogonal_selection_c_sizes_c(
                &array,
                &[&rows_with_extra, &cols_with_extra],
                &[2, 2],
                &mut selected_dest,
                &[2, 2],
                4,
            ),
            BLOSC2_ERROR_SUCCESS
        );
        assert_eq!(selected_dest, vec![1, 3, 13, 15]);
        selected_dest.fill(0);
        assert_eq!(
            b2nd_get_orthogonal_selection_c(
                &array,
                &[&rows_with_extra, &cols_with_extra],
                &[2, 2],
                &mut selected_dest,
                &[2, 2],
                4,
            ),
            BLOSC2_ERROR_SUCCESS
        );
        assert_eq!(selected_dest, vec![1, 3, 13, 15]);
        assert_eq!(
            b2nd_get_orthogonal_selection_c_sizes_c(
                &array,
                &[&rows_with_extra, &cols_with_extra],
                &[4, 2],
                &mut selected_dest,
                &[2, 2],
                4,
            ),
            BLOSC2_ERROR_INVALID_PARAM
        );
        assert_eq!(
            b2nd_get_orthogonal_selection_cbuffer_c(
                &array,
                &[vec![0, 2], vec![1, 3]],
                &mut selected_dest,
                &[2, 2],
                3,
            ),
            BLOSC2_ERROR_SUCCESS
        );
        assert_eq!(selected_dest, vec![1, 3, 13, 15]);
        let padded_array = B2ndArray::from_dense_buffer(
            B2ndMeta::new(vec![3, 4], vec![2, 2], vec![1, 2], "|u1", 0).unwrap(),
            &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
            CParams {
                typesize: 1,
                ..Default::default()
            },
            DParams::default(),
        )
        .unwrap();
        let mut padded_dest = vec![0xaa; 6];
        assert_eq!(
            b2nd_get_orthogonal_selection_cbuffer_c(
                &padded_array,
                &[vec![0, 1], vec![0, 1]],
                &mut padded_dest,
                &[2, 3],
                4,
            ),
            BLOSC2_ERROR_SUCCESS
        );
        assert_eq!(padded_dest, vec![1, 2, 0xaa, 5, 6, 0xaa]);
        padded_dest.fill(0xaa);
        assert_eq!(
            b2nd_get_orthogonal_selection_cbuffer_c(
                &padded_array,
                &[vec![0, 1], vec![0, 1]],
                &mut padded_dest,
                &[2, 2],
                4,
            ),
            BLOSC2_ERROR_SUCCESS
        );
        assert_eq!(&padded_dest[..4], &[1, 2, 5, 6]);
        assert_eq!(&padded_dest[4..], &[0xaa, 0xaa]);
        padded_dest.fill(0xaa);
        assert_eq!(
            b2nd_get_orthogonal_selection_cbuffer_c(
                &padded_array,
                &[vec![0, 1], vec![0, 1]],
                &mut padded_dest,
                &[2, 3],
                6,
            ),
            BLOSC2_ERROR_INVALID_PARAM
        );
        assert_eq!(padded_dest, vec![0xaa; 6]);
        let mut tail_dest = vec![0xff; 1];
        assert_eq!(
            b2nd_get_orthogonal_selection_cbuffer_c(
                &padded_array,
                &[vec![3], vec![0]],
                &mut tail_dest,
                &[1, 1],
                1,
            ),
            BLOSC2_ERROR_SUCCESS
        );
        assert_eq!(tail_dest, vec![0]);
        let mut small_dest = vec![0u8; 2];
        assert_eq!(
            b2nd_get_orthogonal_selection_cbuffer_c(
                &padded_array,
                &[vec![0], vec![0, 1]],
                &mut small_dest,
                &[1, 2],
                1,
            ),
            BLOSC2_ERROR_SUCCESS
        );
        assert_eq!(small_dest, vec![1, 2]);
        let mut too_wide_dest = vec![0xaa; 6];
        assert_eq!(
            b2nd_get_orthogonal_selection_cbuffer_c(
                &padded_array,
                &[vec![0, 1], vec![0, 1]],
                &mut too_wide_dest,
                &[2, 3],
                6,
            ),
            BLOSC2_ERROR_INVALID_PARAM
        );
        assert_eq!(too_wide_dest, vec![0xaa; 6]);
        b2nd_set_orthogonal_selection(&mut target, &[vec![0, 2], vec![1, 3]], &[90, 91, 92, 93])
            .unwrap();
        assert_eq!(
            b2nd_set_orthogonal_selection_cbuffer_c(
                &mut target,
                &[vec![0, 2], vec![1, 3]],
                &[2, 2],
                &[80, 81, 82, 83],
                4,
            ),
            BLOSC2_ERROR_SUCCESS
        );
        assert_eq!(
            b2nd_set_orthogonal_selection_count_c(
                &mut target,
                &[vec![0, 2], vec![1, 3], vec![99]],
                2,
                &[2, 2],
                &[70, 71, 72, 73],
                4,
            ),
            BLOSC2_ERROR_SUCCESS
        );
        assert_eq!(
            b2nd_set_orthogonal_selection_c_sizes_c(
                &mut target,
                &[&rows_with_extra, &cols_with_extra],
                &[2, 2],
                &[2, 2],
                &[60, 61, 62, 63],
                4,
            ),
            BLOSC2_ERROR_SUCCESS
        );
        assert_eq!(
            b2nd_set_orthogonal_selection_c(
                &mut target,
                &[&rows_with_extra, &cols_with_extra],
                &[2, 2],
                &[2, 2],
                &[50, 51, 52, 53],
                4,
            ),
            BLOSC2_ERROR_SUCCESS
        );
        assert_eq!(
            b2nd_set_orthogonal_selection_count_c(
                &mut target,
                &[vec![0, 2], vec![1, 3]],
                3,
                &[2, 2],
                &[1, 2, 3, 4],
                4,
            ),
            BLOSC2_ERROR_INVALID_PARAM
        );
        assert_eq!(
            b2nd_set_orthogonal_selection_cbuffer_c(
                &mut target,
                &[vec![0, 2], vec![1, 3]],
                &[2, 2],
                &[1, 2, 3],
                -1,
            ),
            BLOSC2_ERROR_INVALID_PARAM
        );
        let target_dense = b2nd_to_cbuffer_vec(&target).unwrap();
        assert_eq!(target_dense[1], 50);
        assert_eq!(target_dense[3], 51);
        assert_eq!(target_dense[13], 52);
        assert_eq!(target_dense[15], 53);

        let mut padded_target = B2ndArray::from_dense_buffer(
            B2ndMeta::new(vec![3, 4], vec![2, 2], vec![1, 2], "|u1", 0).unwrap(),
            &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
            CParams {
                typesize: 1,
                ..Default::default()
            },
            DParams::default(),
        )
        .unwrap();
        let src = vec![9, 8, 0xaa, 7, 6, 0xaa];
        assert_eq!(
            b2nd_set_orthogonal_selection_cbuffer_c(
                &mut padded_target,
                &[vec![0, 1], vec![0, 1]],
                &[2, 3],
                &src,
                4,
            ),
            BLOSC2_ERROR_SUCCESS
        );
        assert_eq!(
            b2nd_set_orthogonal_selection_cbuffer_c(
                &mut padded_target,
                &[vec![0, 1], vec![0, 1]],
                &[2, 2],
                &[9, 8, 7, 6],
                4,
            ),
            BLOSC2_ERROR_SUCCESS
        );
        assert_eq!(
            b2nd_set_orthogonal_selection_cbuffer_c(
                &mut padded_target,
                &[vec![0, 1], vec![0, 1]],
                &[2, 3],
                &src,
                6,
            ),
            BLOSC2_ERROR_INVALID_PARAM
        );
        assert_eq!(
            padded_target.to_dense_buffer().unwrap(),
            vec![9, 8, 3, 4, 7, 6, 7, 8, 9, 10, 11, 12]
        );
        let before = padded_target.to_dense_buffer().unwrap();
        assert_eq!(
            b2nd_set_orthogonal_selection_cbuffer_c(
                &mut padded_target,
                &[vec![3], vec![0]],
                &[1, 1],
                &[99],
                1,
            ),
            BLOSC2_ERROR_SUCCESS
        );
        assert_eq!(padded_target.to_dense_buffer().unwrap(), before);

        let mut edge_target = B2ndArray::from_dense_buffer(
            B2ndMeta::new(vec![4, 2], vec![2, 2], vec![1, 2], "|u1", 0).unwrap(),
            &[1, 2, 3, 4, 5, 6, 7, 8],
            CParams {
                typesize: 1,
                ..Default::default()
            },
            DParams::default(),
        )
        .unwrap();
        let mut edge_dest = vec![0xff; 1];
        assert_eq!(
            b2nd_get_orthogonal_selection_cbuffer_c(
                &edge_target,
                &[vec![4], vec![0]],
                &mut edge_dest,
                &[1, 1],
                1,
            ),
            BLOSC2_ERROR_SUCCESS
        );
        assert_eq!(edge_dest, vec![0xff]);
        let edge_before = edge_target.to_dense_buffer().unwrap();
        assert_eq!(
            b2nd_set_orthogonal_selection_cbuffer_c(
                &mut edge_target,
                &[vec![4], vec![0]],
                &[1, 1],
                &[99],
                1,
            ),
            BLOSC2_ERROR_SUCCESS
        );
        assert_eq!(edge_target.to_dense_buffer().unwrap(), edge_before);

        let expanded = b2nd_expand_dims_final_c(&array, &[true, false, false], 3)
            .1
            .unwrap();
        assert_eq!(expanded.shape(), &[1, 4, 6]);
        let partial_expanded = b2nd_expand_dims_final_c(&array, &[true, false], 2)
            .1
            .unwrap();
        assert_eq!(partial_expanded.shape(), &[1, 4]);
        let partial_expanded_alias = b2nd_expand_dims(&array, &[true, false]).unwrap();
        assert_eq!(partial_expanded_alias.shape(), &[1, 4]);
        let partial_expanded_c = b2nd_expand_dims_c(&array, &[true, false]).1.unwrap();
        assert_eq!(partial_expanded_c.shape(), &[1, 4]);
        assert_eq!(
            b2nd_expand_dims_final_c(&array, &[true, false, false, true], 3).0,
            BLOSC2_ERROR_SUCCESS
        );
        assert_eq!(
            b2nd_expand_dims_final_c(&array, &[false], 1).0,
            BLOSC2_ERROR_SUCCESS
        );
        assert_eq!(
            b2nd_expand_dims_final_c(&array, &[true, false], 3).0,
            BLOSC2_ERROR_INVALID_PARAM
        );
        assert_eq!(
            b2nd_expand_dims_final_c(&array, &[false, false, false], 3).0,
            BLOSC2_ERROR_INVALID_PARAM
        );

        let copy_meta = B2ndMeta::new(vec![0, 0], vec![2, 3], vec![1, 3], "|u1", 0).unwrap();
        let copied = b2nd_copy(&array, copy_meta, cparams.clone(), DParams::default()).unwrap();
        assert_eq!(copied.shape(), array.shape());
        assert_eq!(b2nd_to_cbuffer_vec(&copied).unwrap(), data);
        assert_eq!(
            b2nd_copy_c(
                &array,
                B2ndMeta::new(vec![0, 0], vec![2, 3], vec![1, 3], "|u1", 0).unwrap(),
                cparams.clone(),
                DParams::default()
            )
            .0,
            BLOSC2_ERROR_SUCCESS
        );

        let one_d_meta = B2ndMeta::new(vec![4], vec![2], vec![1], "|u1", 0).unwrap();
        let mut concat_left = b2nd_from_cbuffer(
            one_d_meta.clone(),
            &[1, 2, 3, 4],
            cparams.clone(),
            DParams::default(),
        )
        .unwrap();
        let concat_right = b2nd_from_cbuffer(
            one_d_meta.clone(),
            &[5, 6, 7, 8],
            cparams.clone(),
            DParams::default(),
        )
        .unwrap();
        let (concat_rc, concat_result) = b2nd_concatenate_c(
            &mut concat_left,
            &concat_right,
            0,
            false,
            one_d_meta.clone(),
            cparams.clone(),
            DParams::default(),
        );
        assert_eq!(concat_rc, BLOSC2_ERROR_SUCCESS);
        let concat_result = concat_result.unwrap();
        assert_eq!(
            b2nd_to_cbuffer_vec(&concat_result).unwrap(),
            vec![1, 2, 3, 4, 5, 6, 7, 8]
        );
        assert_eq!(
            b2nd_to_cbuffer_vec(&concat_left).unwrap(),
            vec![1, 2, 3, 4, 5, 6, 7, 8]
        );
        let mut concat_alias_left = b2nd_from_cbuffer(
            one_d_meta.clone(),
            &[1, 2, 3, 4],
            cparams.clone(),
            DParams::default(),
        )
        .unwrap();
        let concat_alias_result = b2nd_concatenate(
            &mut concat_alias_left,
            &concat_right,
            0,
            false,
            one_d_meta.clone(),
            cparams.clone(),
            DParams::default(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            b2nd_to_cbuffer_vec(&concat_alias_result).unwrap(),
            vec![1, 2, 3, 4, 5, 6, 7, 8]
        );

        let mut one_d = b2nd_from_cbuffer(
            one_d_meta.clone(),
            &[1, 2, 3, 4],
            cparams.clone(),
            DParams::default(),
        )
        .unwrap();
        b2nd_insert(&mut one_d, &[9, 8], 0, 2).unwrap();
        assert_eq!(b2nd_to_cbuffer_vec(&one_d).unwrap(), vec![1, 2, 9, 8, 3, 4]);
        assert_eq!(
            b2nd_append_c(&mut one_d, &[7, 6, 99], 2, 0),
            BLOSC2_ERROR_SUCCESS
        );
        assert_eq!(
            b2nd_to_cbuffer_vec(&one_d).unwrap(),
            vec![1, 2, 9, 8, 3, 4, 7, 6]
        );
        assert_eq!(
            b2nd_append_c(&mut one_d, &[1], -1, 0),
            BLOSC2_ERROR_INVALID_PARAM
        );
        assert_eq!(
            b2nd_append_axis_c(&mut one_d, &[1], 1, -1),
            BLOSC2_ERROR_INVALID_PARAM
        );
        assert_eq!(
            b2nd_append_c(&mut one_d, &[1], 2, 0),
            BLOSC2_ERROR_INVALID_PARAM
        );
        assert_eq!(b2nd_delete_c(&mut one_d, 0, 2, 2), BLOSC2_ERROR_SUCCESS);
        assert_eq!(b2nd_to_cbuffer_vec(&one_d).unwrap(), vec![1, 2, 3, 4, 7, 6]);
        assert_eq!(
            b2nd_insert_c(&mut one_d, &[5, 6, 99], 2, 0, 4),
            BLOSC2_ERROR_SUCCESS
        );
        assert_eq!(
            b2nd_to_cbuffer_vec(&one_d).unwrap(),
            vec![1, 2, 3, 4, 5, 6, 7, 6]
        );
        assert_eq!(
            b2nd_insert_c(&mut one_d, &[1], -1, 0, 0),
            BLOSC2_ERROR_INVALID_PARAM
        );
        assert_eq!(
            b2nd_insert_axis_c(&mut one_d, &[1], 1, -1, 0),
            BLOSC2_ERROR_INVALID_PARAM
        );
        assert_eq!(
            b2nd_insert_c(&mut one_d, &[1], 2, 0, 0),
            BLOSC2_ERROR_INVALID_PARAM
        );
        assert_eq!(b2nd_delete_c(&mut one_d, 0, 4, 2), BLOSC2_ERROR_SUCCESS);
        assert_eq!(
            b2nd_delete_axis_c(&mut one_d, -1, 0, 1),
            BLOSC2_ERROR_INVALID_PARAM
        );
        assert_eq!(
            b2nd_delete_c(&mut one_d, 0, -1, 1),
            BLOSC2_ERROR_INVALID_PARAM
        );
        let mut grow_by_delete = b2nd_from_cbuffer(
            B2ndMeta::new(vec![4], vec![2], vec![1], "|u1", 0).unwrap(),
            &[1, 2, 3, 4],
            cparams.clone(),
            DParams::default(),
        )
        .unwrap();
        assert_eq!(
            b2nd_delete_c(&mut grow_by_delete, 0, 6, -2),
            BLOSC2_ERROR_SUCCESS
        );
        assert_eq!(grow_by_delete.shape(), &[6]);
        assert_eq!(
            b2nd_to_cbuffer_vec(&grow_by_delete).unwrap(),
            vec![1, 2, 3, 4, 0, 0]
        );
        assert_eq!(
            b2nd_resize_c(&mut one_d, vec![4], None),
            BLOSC2_ERROR_SUCCESS
        );
        assert_eq!(
            b2nd_resize_c(&mut one_d, vec![-1], None),
            BLOSC2_ERROR_INVALID_PARAM
        );
        assert_eq!(b2nd_to_cbuffer_vec(&one_d).unwrap(), vec![1, 2, 3, 4]);

        let singleton_meta = B2ndMeta::new(vec![1, 4], vec![1, 2], vec![1, 1], "|u1", 0).unwrap();
        let singleton = b2nd_from_cbuffer(
            singleton_meta,
            &[1, 2, 3, 4],
            cparams.clone(),
            DParams::default(),
        )
        .unwrap();
        let squeezed = b2nd_squeeze(&singleton).unwrap();
        assert_eq!(squeezed.shape(), &[4]);
        let expanded = b2nd_expand_dims(&squeezed, &[true, false]).unwrap();
        assert_eq!(expanded.shape(), &[1, 4]);
        assert_eq!(
            b2nd_squeeze_index(&expanded, &[true, false])
                .unwrap()
                .shape(),
            &[4]
        );
        assert_eq!(
            b2nd_squeeze_index(&expanded, &[true, false, true])
                .unwrap()
                .shape(),
            &[4]
        );
        assert_eq!(
            b2nd_squeeze_index_c(&expanded, &[true, false, true]).0,
            BLOSC2_ERROR_SUCCESS
        );
        assert_eq!(
            b2nd_squeeze_index_c(&expanded, &[true]).0,
            BLOSC2_ERROR_INVALID_PARAM
        );

        let mut copied_region = vec![0u8; 9];
        assert_eq!(
            b2nd_copy_buffer2(
                2,
                1,
                &data,
                &[4, 6],
                &[1, 1],
                &[3, 4],
                &mut copied_region,
                &[3, 3],
                &[0, 0],
            ),
            BLOSC2_ERROR_SUCCESS
        );
        assert_eq!(copied_region, vec![7, 8, 9, 13, 14, 15, 0, 0, 0]);
        assert!(b2nd_copy_buffer2_result(
            2,
            1,
            &data,
            &[4, 6],
            &[1, 1],
            &[3, 4],
            &mut copied_region,
            &[3, 3],
            &[0, 0],
        )
        .is_ok());
        assert_eq!(
            b2nd_copy_buffer2(
                1,
                1,
                &data,
                &[4, 6],
                &[1, 1],
                &[3, 4],
                &mut copied_region,
                &[3, 3],
                &[0, 0],
            ),
            BLOSC2_ERROR_INVALID_PARAM
        );
        assert_eq!(
            b2nd_copy_buffer2(
                2,
                1,
                &data,
                &[4, 6],
                &[1, 1],
                &[3, 4],
                &mut copied_region[..5],
                &[3, 3],
                &[0, 0],
            ),
            BLOSC2_ERROR_WRITE_BUFFER
        );
        assert_eq!(
            b2nd_copy_buffer2(
                2,
                1,
                &data[..8],
                &[4, 6],
                &[1, 1],
                &[3, 4],
                &mut copied_region,
                &[3, 3],
                &[0, 0],
            ),
            BLOSC2_ERROR_WRITE_BUFFER
        );
        assert!(b2nd_copy_buffer2_result(
            1,
            1,
            &data,
            &[4, 6],
            &[1, 1],
            &[3, 4],
            &mut copied_region,
            &[3, 3],
            &[0, 0],
        )
        .is_err());
        let mut copied_region_deprecated = vec![0u8; 9];
        assert_eq!(
            b2nd_copy_buffer(
                2,
                1,
                &data,
                &[4, 6],
                &[1, 1],
                &[3, 4],
                &mut copied_region_deprecated,
                &[3, 3],
                &[0, 0],
            ),
            BLOSC2_ERROR_SUCCESS
        );
        assert_eq!(copied_region_deprecated, copied_region);
        assert!(b2nd_print_meta(&array).contains("shape: [4, 6]"));
        assert_eq!(b2nd_print_meta_c(&array), BLOSC2_ERROR_SUCCESS);
        let mut meta_dest = vec![0u8; 128];
        let meta_len = b2nd_print_meta_to_buffer_c(&array, &mut meta_dest, 128);
        assert!(meta_len > 0);
        let meta_text = std::str::from_utf8(&meta_dest[..meta_len as usize]).unwrap();
        assert!(meta_text.contains("b2nd metalayer parameters:"));
        assert!(meta_text.contains("Ndim:       2"));
        assert!(meta_text.contains("shape:      4, 6"));
        assert!(meta_text.contains("dtype: |u1"));
        assert!(!meta_text.contains("dtype_format"));
        let mut legacy_no_dtype = array.clone();
        let legacy_content = legacy_no_dtype
            .schunk
            .remove_metalayer(B2ND_METALAYER_NAME)
            .unwrap();
        let dtype_start = legacy_content.len() - (1 + 1 + 4 + array.meta.dtype.len());
        legacy_no_dtype
            .schunk
            .add_metalayer(CATERVA_METALAYER_NAME, &legacy_content[..dtype_start])
            .unwrap();
        let legacy_meta_len = b2nd_print_meta_to_buffer_c(&legacy_no_dtype, &mut meta_dest, 128);
        assert!(legacy_meta_len > 0);
        let legacy_meta_text = std::str::from_utf8(&meta_dest[..legacy_meta_len as usize]).unwrap();
        assert!(!legacy_meta_text.contains("dtype:"));
        assert!(legacy_meta_text.contains("blockshape: 1, 3"));
        let mut no_meta = array.clone();
        no_meta.schunk.remove_metalayer(B2ND_METALAYER_NAME);
        assert_eq!(
            b2nd_print_meta_c(&no_meta),
            crate::constants::BLOSC2_ERROR_METALAYER_NOT_FOUND
        );

        let scalar_meta = B2ndMeta::new(Vec::new(), Vec::new(), Vec::new(), "|u1", 0).unwrap();
        let scalar = b2nd_from_cbuffer(
            scalar_meta.clone(),
            &[7],
            CParams {
                typesize: 1,
                ..CParams::default()
            },
            DParams::default(),
        )
        .unwrap();
        let mut scalar_meta_dest = vec![0u8; 128];
        let scalar_meta_len = b2nd_print_meta_to_buffer_c(&scalar, &mut scalar_meta_dest, 128);
        assert!(scalar_meta_len > 0);
        let scalar_meta_text =
            std::str::from_utf8(&scalar_meta_dest[..scalar_meta_len as usize]).unwrap();
        assert!(scalar_meta_text.contains("Ndim:       0"));
        assert!(scalar_meta_text.contains("shape:      1"));
        assert!(scalar_meta_text.contains("chunkshape: 1"));
        assert!(scalar_meta_text.contains("blockshape: 1"));
        assert_eq!(
            b2nd_get_slice(
                &scalar,
                &[],
                &[],
                scalar_meta,
                CParams {
                    typesize: 1,
                    ..CParams::default()
                },
                DParams::default()
            )
            .unwrap()
            .to_dense_buffer()
            .unwrap(),
            vec![7]
        );
    }

    #[test]
    fn test_b2nd_resize_same_chunk_grid_zero_fills_new_cells() {
        let meta = B2ndMeta::new(vec![5], vec![3], vec![1], "|u1", 0).unwrap();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut array =
            B2ndArray::from_dense_buffer(meta, &[1, 2, 3, 4, 5], cparams, DParams::default())
                .unwrap();
        let saved_head = array.schunk.compressed_chunk_bytes(0).unwrap().to_vec();
        let saved = replace_raw_chunk(&mut array, 1, &[4, 5, 9]);

        array.resize(vec![6]).unwrap();
        assert_eq!(array.schunk.compressed_chunk_bytes(0).unwrap(), saved_head);
        assert_ne!(
            array.schunk.compressed_chunk_bytes(1).unwrap(),
            saved.as_slice()
        );
        assert_eq!(array.to_dense_buffer().unwrap(), vec![1, 2, 3, 4, 5, 0]);
    }

    #[test]
    fn test_b2nd_resize_same_chunk_grid_zero_fills_multi_axis_growth() {
        let meta = B2ndMeta::new(vec![2, 2], vec![4, 4], vec![2, 2], "|u1", 0).unwrap();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut schunk = Schunk::new(cparams, DParams::default());
        schunk.chunksize = 16;
        schunk
            .add_metalayer(B2ND_METALAYER_NAME, &meta.serialize().unwrap())
            .unwrap();
        schunk
            .append_buffer(&[1, 2, 3, 4, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9])
            .unwrap();
        let mut array = B2ndArray::from_schunk(schunk).unwrap();

        array.resize(vec![3, 3]).unwrap();
        assert_eq!(
            array.to_dense_buffer().unwrap(),
            vec![1, 2, 0, 3, 4, 0, 0, 0, 0]
        );
    }

    #[test]
    fn test_b2nd_resize_does_not_expose_block_only_padding() {
        let meta = B2ndMeta::new(vec![5], vec![5], vec![3], "|u1", 0).unwrap();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut schunk = Schunk::new(cparams, DParams::default());
        schunk.chunksize = 6;
        schunk
            .add_metalayer(B2ND_METALAYER_NAME, &meta.serialize().unwrap())
            .unwrap();
        schunk.append_buffer(&[1, 1, 1, 1, 1, 1]).unwrap();
        let mut array = B2ndArray::from_schunk(schunk).unwrap();

        array.resize(vec![6]).unwrap();
        assert_eq!(array.to_dense_buffer().unwrap(), vec![1, 1, 1, 1, 1, 0]);
    }

    #[test]
    fn test_b2nd_set_slice_then_resize_zero_fills_new_tail_cell() {
        let meta = B2ndMeta::new(vec![5], vec![3], vec![1], "|u1", 0).unwrap();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut schunk = Schunk::new(cparams, DParams::default());
        schunk.chunksize = 3;
        schunk
            .add_metalayer(B2ND_METALAYER_NAME, &meta.serialize().unwrap())
            .unwrap();
        schunk.append_buffer(&[1, 2, 3]).unwrap();
        schunk.append_buffer(&[4, 5, 9]).unwrap();
        let mut array = B2ndArray::from_schunk(schunk).unwrap();

        array.set_slice(&[0], &[1], &[7]).unwrap();
        array.resize(vec![6]).unwrap();
        assert_eq!(array.to_dense_buffer().unwrap(), vec![7, 2, 3, 4, 5, 0]);
    }

    #[test]
    fn test_b2nd_set_slice_full_tail_chunk_clears_old_padding() {
        let meta = B2ndMeta::new(vec![5], vec![3], vec![1], "|u1", 0).unwrap();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut schunk = Schunk::new(cparams, DParams::default());
        schunk.chunksize = 3;
        schunk
            .add_metalayer(B2ND_METALAYER_NAME, &meta.serialize().unwrap())
            .unwrap();
        schunk.append_buffer(&[1, 2, 3]).unwrap();
        schunk.append_buffer(&[4, 5, 9]).unwrap();
        let mut array = B2ndArray::from_schunk(schunk).unwrap();

        array.set_slice(&[3], &[5], &[7, 8]).unwrap();
        array.resize(vec![6]).unwrap();
        assert_eq!(array.to_dense_buffer().unwrap(), vec![1, 2, 3, 7, 8, 0]);
    }

    #[test]
    fn test_b2nd_set_slice_preserves_raw_untouched_chunks() {
        let meta = B2ndMeta::new(vec![6], vec![3], vec![1], "|u1", 0).unwrap();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut array =
            B2ndArray::from_dense_buffer(meta, &[1, 2, 3, 4, 5, 6], cparams, DParams::default())
                .unwrap();
        let saved = replace_raw_chunk(&mut array, 1, &[4, 5, 6]);

        array.set_slice(&[0], &[1], &[9]).unwrap();
        assert_eq!(
            array.schunk.compressed_chunk_bytes(1).unwrap(),
            saved.as_slice()
        );
        assert_eq!(array.to_dense_buffer().unwrap(), vec![9, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn test_b2nd_orthogonal_selection_cbuffer_and_bounds() {
        let meta = B2ndMeta::new(vec![3, 4], vec![2, 2], vec![1, 2], "<u2", 0).unwrap();
        let values: Vec<u16> = (0..12u16).collect();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 2,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut array =
            B2ndArray::from_dense_buffer(meta, &u16_bytes(&values), cparams, DParams::default())
                .unwrap();
        array.schunk.add_metalayer("keep", b"fixed").unwrap();

        let selection = vec![vec![2, 0], vec![3, 1]];
        assert_eq!(
            array.select_orthogonal(&selection).unwrap(),
            u16_bytes(&[11, 9, 3, 1])
        );
        assert_eq!(
            array
                .orthogonal_selection_to_dense_buffer(&selection, &[2, 2])
                .unwrap(),
            u16_bytes(&[11, 9, 3, 1])
        );
        assert!(array
            .orthogonal_selection_to_dense_buffer(&selection, &[2, 3])
            .is_err());

        array
            .set_orthogonal_selection_from_dense_buffer(
                &selection,
                &[2, 2],
                &u16_bytes(&[100, 101, 102, 103]),
            )
            .unwrap();
        let mut expected = values.clone();
        expected[2 * 4 + 3] = 100;
        expected[2 * 4 + 1] = 101;
        expected[3] = 102;
        expected[1] = 103;
        assert_eq!(array.to_dense_buffer().unwrap(), u16_bytes(&expected));
        assert_eq!(array.schunk.metalayer("keep"), Some(&b"fixed"[..]));

        assert!(array.select_orthogonal(&[vec![-1], vec![0]]).is_err());
        assert!(array.select_orthogonal(&[vec![3], vec![0]]).is_err());
        assert!(array
            .orthogonal_selection_to_dense_buffer(&selection, &[1, 3])
            .is_err());
        assert!(array
            .set_orthogonal_selection_from_dense_buffer(
                &selection,
                &[2, 3],
                &u16_bytes(&[100, 101, 0, 102, 103, 0]),
            )
            .is_err());
        assert!(array
            .set_orthogonal_selection_from_dense_buffer(
                &selection,
                &[2, 2],
                &u16_bytes(&[100, 101, 102, 103, 104]),
            )
            .is_err());

        let before = array.to_dense_buffer().unwrap();
        assert_eq!(
            array
                .orthogonal_selection_to_dense_buffer(&[Vec::new(), vec![0]], &[0, 2])
                .unwrap(),
            Vec::<u8>::new()
        );
        array
            .set_orthogonal_selection(&[Vec::new(), vec![0]], &[])
            .unwrap();
        assert!(array
            .set_orthogonal_selection_from_dense_buffer(&[Vec::new(), vec![0]], &[0, 1], &[1, 2])
            .is_err());
        assert_eq!(array.to_dense_buffer().unwrap(), before);
    }

    #[test]
    fn test_b2nd_orthogonal_c_adapter_buffersize_matches_c_bounds() {
        let meta = B2ndMeta::new(vec![3, 4], vec![2, 2], vec![1, 2], "<u2", 0).unwrap();
        let values: Vec<u16> = (0..12u16).collect();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 2,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut array =
            B2ndArray::from_dense_buffer(meta, &u16_bytes(&values), cparams, DParams::default())
                .unwrap();
        let selection = vec![vec![2, 0], vec![3, 1]];
        let compact_len = 4 * 2;

        let mut dest = vec![0xff; compact_len];
        assert_eq!(
            b2nd_get_orthogonal_selection_cbuffer_c(
                &array,
                &selection,
                &mut dest,
                &[2, 2],
                (compact_len - 2) as i64,
            ),
            BLOSC2_ERROR_SUCCESS
        );
        assert_eq!(dest, u16_bytes(&[11, 9, 3, 1]));
        assert_eq!(
            b2nd_get_orthogonal_selection_cbuffer_c(
                &array,
                &selection,
                &mut dest,
                &[2, 2],
                (compact_len + 1) as i64,
            ),
            BLOSC2_ERROR_INVALID_PARAM
        );

        assert_eq!(
            b2nd_set_orthogonal_selection_cbuffer_c(
                &mut array,
                &selection,
                &[2, 2],
                &u16_bytes(&[100, 101, 102, 103]),
                (compact_len - 2) as i64,
            ),
            BLOSC2_ERROR_SUCCESS
        );
        let mut expected = values;
        expected[2 * 4 + 3] = 100;
        expected[2 * 4 + 1] = 101;
        expected[3] = 102;
        expected[1] = 103;
        assert_eq!(array.to_dense_buffer().unwrap(), u16_bytes(&expected));
    }

    #[test]
    fn test_b2nd_orthogonal_selection_masks_untouched_blocks() {
        let meta = B2ndMeta::new(vec![4, 4], vec![4, 4], vec![2, 2], "|u1", 0).unwrap();
        let values: Vec<u8> = (0..16u8).collect();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_NOFILTER],
            ..Default::default()
        };
        let dparams = DParams {
            postfilter: Some(count_orthogonal_postfilter),
            ..Default::default()
        };
        let array = B2ndArray::from_dense_buffer(meta, &values, cparams, dparams).unwrap();

        ORTHOGONAL_POSTFILTER_CALLS.store(0, Ordering::SeqCst);
        assert_eq!(
            array.select_orthogonal(&[vec![1], vec![1]]).unwrap(),
            vec![5]
        );
        assert_eq!(ORTHOGONAL_POSTFILTER_CALLS.load(Ordering::SeqCst), 1);

        ORTHOGONAL_POSTFILTER_CALLS.store(0, Ordering::SeqCst);
        assert_eq!(
            array
                .slice_to_dense_buffer(&[1, 1], &[2, 2], &[1, 1])
                .unwrap(),
            vec![5]
        );
        assert_eq!(ORTHOGONAL_POSTFILTER_CALLS.load(Ordering::SeqCst), 1);

        ORTHOGONAL_POSTFILTER_CALLS.store(0, Ordering::SeqCst);
        assert_eq!(
            array.select_orthogonal(&[vec![1, 3], vec![1, 3]]).unwrap(),
            vec![5, 7, 13, 15]
        );
        assert_eq!(ORTHOGONAL_POSTFILTER_CALLS.load(Ordering::SeqCst), 4);
    }

    #[test]
    fn test_b2nd_orthogonal_set_preserves_raw_untouched_chunks() {
        let meta = B2ndMeta::new(vec![4, 4], vec![2, 2], vec![2, 2], "|u1", 0).unwrap();
        let values: Vec<u8> = (0..16u8).collect();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut array =
            B2ndArray::from_dense_buffer(meta, &values, cparams, DParams::default()).unwrap();
        let saved = replace_raw_chunk(&mut array, 3, &[10, 11, 14, 15]);

        array
            .set_orthogonal_selection(&[vec![0], vec![0]], &[99])
            .unwrap();
        assert_eq!(
            array.schunk.compressed_chunk_bytes(3).unwrap(),
            saved.as_slice()
        );

        let mut expected = values;
        expected[0] = 99;
        assert_eq!(array.to_dense_buffer().unwrap(), expected);
    }

    #[test]
    fn test_b2nd_resize_at_middle_insertion() {
        let meta = B2ndMeta::new(vec![4, 6], vec![2, 3], vec![1, 3], "<u2", 0).unwrap();
        let values: Vec<u16> = (0..24u16).collect();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 2,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut array =
            B2ndArray::from_dense_buffer(meta, &u16_bytes(&values), cparams, DParams::default())
                .unwrap();

        array.resize_with_start(vec![6, 6], Some(&[2, 0])).unwrap();

        let mut expected = vec![0u16; 36];
        for row in 0..2 {
            for col in 0..6 {
                expected[row * 6 + col] = values[row * 6 + col];
            }
        }
        for row in 2..4 {
            for col in 0..6 {
                expected[(row + 2) * 6 + col] = values[row * 6 + col];
            }
        }
        assert_eq!(array.shape(), &[6, 6]);
        assert_eq!(array.to_dense_buffer().unwrap(), u16_bytes(&expected));
    }

    #[test]
    fn test_b2nd_resize_at_middle_insertion_preserves_surviving_raw_chunks() {
        let meta = B2ndMeta::new(vec![4], vec![2], vec![1], "|u1", 0).unwrap();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut array =
            B2ndArray::from_dense_buffer(meta, &[1, 2, 3, 4], cparams, DParams::default()).unwrap();
        let saved_tail = replace_raw_chunk(&mut array, 1, &[3, 4]);

        array.resize_with_start(vec![6], Some(&[2])).unwrap();

        assert_eq!(array.shape(), &[6]);
        assert_eq!(array.to_dense_buffer().unwrap(), vec![1, 2, 0, 0, 3, 4]);
        let inserted = ChunkHeader::read(array.schunk.compressed_chunk_bytes(1).unwrap()).unwrap();
        assert_eq!(inserted.version, BLOSC2_VERSION_FORMAT_STABLE);
        assert_eq!(inserted.versionlz, BLOSC_BLOSCLZ_VERSION_FORMAT);
        assert_eq!(inserted.flags, BLOSC_DOSHUFFLE | BLOSC_DOBITSHUFFLE);
        assert_eq!(inserted.special_type(), BLOSC2_SPECIAL_ZERO);
        assert_eq!(inserted.cbytes as usize, BLOSC_EXTENDED_HEADER_LENGTH);
        assert_eq!(inserted.nbytes, 2);
        assert_eq!(
            array.schunk.compressed_chunk_bytes(2).unwrap(),
            saved_tail.as_slice()
        );
    }

    #[test]
    fn test_b2nd_resize_at_middle_deletion_preserves_surviving_raw_chunks() {
        let meta = B2ndMeta::new(vec![6], vec![2], vec![1], "|u1", 0).unwrap();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut array =
            B2ndArray::from_dense_buffer(meta, &[1, 2, 3, 4, 5, 6], cparams, DParams::default())
                .unwrap();
        let saved_tail = replace_raw_chunk(&mut array, 2, &[5, 6]);

        array.resize_with_start(vec![4], Some(&[2])).unwrap();

        assert_eq!(array.shape(), &[4]);
        assert_eq!(array.to_dense_buffer().unwrap(), vec![1, 2, 5, 6]);
        assert_eq!(
            array.schunk.compressed_chunk_bytes(1).unwrap(),
            saved_tail.as_slice()
        );
    }

    #[test]
    fn test_b2nd_insert_preserves_surviving_raw_chunks() {
        let meta = B2ndMeta::new(vec![4], vec![2], vec![1], "|u1", 0).unwrap();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut array =
            B2ndArray::from_dense_buffer(meta, &[1, 2, 3, 4], cparams, DParams::default()).unwrap();
        let saved_tail = replace_raw_chunk(&mut array, 1, &[3, 4]);

        array.insert(0, 2, &[2], &[7, 8]).unwrap();

        assert_eq!(array.shape(), &[6]);
        assert_eq!(array.to_dense_buffer().unwrap(), vec![1, 2, 7, 8, 3, 4]);
        assert_eq!(
            array.schunk.compressed_chunk_bytes(2).unwrap(),
            saved_tail.as_slice()
        );
    }

    #[test]
    fn test_b2nd_delete_preserves_surviving_raw_chunks() {
        let meta = B2ndMeta::new(vec![6], vec![2], vec![1], "|u1", 0).unwrap();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut array =
            B2ndArray::from_dense_buffer(meta, &[1, 2, 3, 4, 5, 6], cparams, DParams::default())
                .unwrap();
        let saved_tail = replace_raw_chunk(&mut array, 2, &[5, 6]);

        array.delete(0, 2, 2).unwrap();

        assert_eq!(array.shape(), &[4]);
        assert_eq!(array.to_dense_buffer().unwrap(), vec![1, 2, 5, 6]);
        assert_eq!(
            array.schunk.compressed_chunk_bytes(1).unwrap(),
            saved_tail.as_slice()
        );
    }

    #[test]
    fn test_b2nd_insert_invalid_buffer_len_is_atomic() {
        let meta = B2ndMeta::new(vec![4], vec![2], vec![1], "|u1", 0).unwrap();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut array =
            B2ndArray::from_dense_buffer(meta, &[1, 2, 3, 4], cparams, DParams::default()).unwrap();
        let before_shape = array.shape().to_vec();
        let before_data = array.to_dense_buffer().unwrap();
        let before_nchunks = array.schunk.nchunks();

        assert_eq!(
            array.insert(0, 2, &[2], &[7]).err(),
            Some("B2ND insert buffer size does not match buffer shape and typesize")
        );

        assert_eq!(array.shape(), before_shape.as_slice());
        assert_eq!(array.to_dense_buffer().unwrap(), before_data);
        assert_eq!(array.schunk.nchunks(), before_nchunks);
    }

    #[test]
    fn test_b2nd_set_slice_multi_chunk_write_failure_is_atomic() {
        let _guard = CONTEXT_B2ND_FILTER_LOCK.lock().unwrap();
        let meta = B2ndMeta::new(vec![4], vec![2], vec![1], "|u1", 0).unwrap();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut array =
            B2ndArray::from_dense_buffer(meta, &[1, 2, 3, 4], cparams, DParams::default()).unwrap();
        let before_data = array.to_dense_buffer().unwrap();
        let before_chunks = array.schunk.compressed_chunks();
        array.schunk.cparams.prefilter = Some(fail_on_selected_chunk_prefilter);
        FAIL_PREFILTER_NCHUNK.store(1, Ordering::SeqCst);

        assert_eq!(
            array.set_slice(&[0], &[4], &[9, 9, 8, 8]).err(),
            Some("Execution of prefilter function failed")
        );

        FAIL_PREFILTER_NCHUNK.store(-1, Ordering::SeqCst);
        array.schunk.cparams.prefilter = None;
        assert_eq!(array.to_dense_buffer().unwrap(), before_data);
        assert_eq!(array.schunk.compressed_chunks(), before_chunks);
    }

    #[test]
    fn test_b2nd_set_orthogonal_selection_multi_chunk_write_failure_is_atomic() {
        let _guard = CONTEXT_B2ND_FILTER_LOCK.lock().unwrap();
        let meta = B2ndMeta::new(vec![4], vec![2], vec![1], "|u1", 0).unwrap();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut array =
            B2ndArray::from_dense_buffer(meta, &[1, 2, 3, 4], cparams, DParams::default()).unwrap();
        let before_data = array.to_dense_buffer().unwrap();
        let before_chunks = array.schunk.compressed_chunks();
        array.schunk.cparams.prefilter = Some(fail_on_selected_chunk_prefilter);
        FAIL_PREFILTER_NCHUNK.store(1, Ordering::SeqCst);

        assert_eq!(
            array
                .set_orthogonal_selection(&[vec![0, 1, 2, 3]], &[9, 9, 8, 8])
                .err(),
            Some("Execution of prefilter function failed")
        );

        FAIL_PREFILTER_NCHUNK.store(-1, Ordering::SeqCst);
        array.schunk.cparams.prefilter = None;
        assert_eq!(array.to_dense_buffer().unwrap(), before_data);
        assert_eq!(array.schunk.compressed_chunks(), before_chunks);
    }

    #[test]
    fn test_b2nd_insert_write_failure_after_resize_is_atomic() {
        let meta = B2ndMeta::new(vec![4], vec![2], vec![1], "|u1", 0).unwrap();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut array =
            B2ndArray::from_dense_buffer(meta, &[1, 2, 3, 4], cparams, DParams::default()).unwrap();
        let before_shape = array.shape().to_vec();
        let before_data = array.to_dense_buffer().unwrap();
        let before_chunks = array.schunk.compressed_chunks();
        let before_nchunks = array.schunk.nchunks();
        array.schunk.cparams.prefilter = Some(always_fail_prefilter);

        assert_eq!(
            array.insert(0, 2, &[2], &[7, 8]).err(),
            Some("Execution of prefilter function failed")
        );

        array.schunk.cparams.prefilter = None;
        assert_eq!(array.shape(), before_shape.as_slice());
        assert_eq!(array.to_dense_buffer().unwrap(), before_data);
        assert_eq!(array.schunk.nchunks(), before_nchunks);
        assert_eq!(array.schunk.compressed_chunks(), before_chunks);
    }

    #[test]
    fn test_b2nd_c_zero_item_slice_returns_before_source_bounds() {
        let meta = B2ndMeta::new(vec![0, 5], vec![0, 5], vec![0, 1], "|u1", 0).unwrap();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut array =
            B2ndArray::from_dense_buffer(meta.clone(), &[], cparams.clone(), DParams::default())
                .unwrap();
        let mut dest = vec![0xff; 4];
        let mut dense_dest = vec![0xff; 4];

        assert_eq!(
            b2nd_to_cbuffer(&array, &mut dense_dest),
            BLOSC2_ERROR_SUCCESS
        );
        assert_eq!(dense_dest, vec![0xff; 4]);
        assert_eq!(
            b2nd_to_cbuffer_c(&array, &mut dense_dest, 4),
            BLOSC2_ERROR_SUCCESS
        );
        assert_eq!(dense_dest, vec![0xff; 4]);

        assert_eq!(
            b2nd_get_slice_cbuffer_c(&array, &[0, 100], &[0, 101], &mut dest, &[0, 1], 4),
            BLOSC2_ERROR_SUCCESS
        );
        assert_eq!(dest, vec![0; 4]);
        dest.fill(0xff);
        assert_eq!(
            b2nd_get_slice_cbuffer_c(&array, &[0, 0], &[0, 5], &mut dest, &[0, 5], 4),
            BLOSC2_ERROR_SUCCESS
        );
        assert_eq!(dest, vec![0; 4]);
        assert_eq!(
            b2nd_set_slice_cbuffer_c(&[], 0, &[0, 1], &[0, 100], &[0, 101], &mut array),
            BLOSC2_ERROR_SUCCESS
        );
        assert_eq!(
            b2nd_set_slice_cbuffer_c(&[], -1, &[0, 1], &[0, 100], &[0, 101], &mut array),
            BLOSC2_ERROR_INVALID_PARAM
        );
        assert_eq!(
            b2nd_set_slice_cbuffer_c(&[], 0, &[0, 5], &[0, 0], &[0, 5], &mut array),
            BLOSC2_ERROR_SUCCESS
        );

        let (ctx_rc, ctx) = b2nd_create_ctx_c(
            meta,
            cparams,
            DParams::default(),
            vec![("owner".to_string(), b"ctx".to_vec())],
        );
        assert_eq!(ctx_rc, BLOSC2_ERROR_SUCCESS);
        let mut ctx = ctx.unwrap();
        let (slice_rc, slice) = b2nd_get_slice_ctx_c(&mut ctx, &array, &[0, 100], &[0, 101]);
        assert_eq!(slice_rc, BLOSC2_ERROR_SUCCESS);
        let slice = slice.unwrap();
        assert_eq!(ctx.meta.shape, vec![0, 1]);
        assert_eq!(slice.shape(), &[0, 1]);
        assert_eq!(slice.to_dense_buffer().unwrap(), Vec::<u8>::new());
        assert_eq!(slice.schunk.metalayer("owner"), Some(&b"ctx"[..]));

        let (slice_rc, slice) = b2nd_get_slice_ctx_c(&mut ctx, &array, &[0, 0], &[0, 5]);
        assert_eq!(slice_rc, BLOSC2_ERROR_SUCCESS);
        let slice = slice.unwrap();
        assert_eq!(ctx.meta.shape, vec![0, 5]);
        assert_eq!(slice.shape(), &[0, 5]);
        assert_eq!(slice.to_dense_buffer().unwrap(), Vec::<u8>::new());
        assert_eq!(slice.schunk.metalayer("owner"), Some(&b"ctx"[..]));
    }

    #[test]
    fn test_b2nd_c_zero_item_slice_on_nonempty_array_returns_before_source_bounds() {
        let meta = B2ndMeta::new(vec![4, 6], vec![2, 3], vec![1, 3], "|u1", 0).unwrap();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let data: Vec<u8> = (0..24).collect();
        let mut array =
            B2ndArray::from_dense_buffer(meta.clone(), &data, cparams.clone(), DParams::default())
                .unwrap();
        let before = array.to_dense_buffer().unwrap();
        let start = [0, 100];
        let stop = [0, 101];
        let buffershape = [0, 1];

        let mut dest = vec![0xff; 4];
        assert_eq!(
            b2nd_get_slice_cbuffer_c(&array, &start, &stop, &mut dest, &buffershape, 4),
            BLOSC2_ERROR_SUCCESS
        );
        assert_eq!(dest, vec![0; 4]);
        dest.fill(0xff);
        assert_eq!(
            b2nd_get_slice_cbuffer(&array, &start, &stop, &mut dest, &buffershape),
            BLOSC2_ERROR_SUCCESS
        );
        assert_eq!(dest, vec![0; 4]);

        assert_eq!(
            b2nd_set_slice_cbuffer(&[], &buffershape, &start, &stop, &mut array),
            BLOSC2_ERROR_SUCCESS
        );
        assert_eq!(
            b2nd_set_slice_cbuffer_c(&[], 0, &buffershape, &start, &stop, &mut array),
            BLOSC2_ERROR_SUCCESS
        );
        assert_eq!(array.to_dense_buffer().unwrap(), before);

        let slice_meta = B2ndMeta::new(vec![99, 99], vec![2, 3], vec![1, 3], "|u1", 0).unwrap();
        let slice = b2nd_get_slice(
            &array,
            &start,
            &stop,
            slice_meta.clone(),
            cparams.clone(),
            DParams::default(),
        )
        .unwrap();
        assert_eq!(slice.shape(), &[0, 1]);
        assert_eq!(slice.to_dense_buffer().unwrap(), Vec::<u8>::new());
        let (slice_rc, slice) = b2nd_get_slice_c(
            &array,
            &start,
            &stop,
            slice_meta,
            cparams.clone(),
            DParams::default(),
        );
        assert_eq!(slice_rc, BLOSC2_ERROR_SUCCESS);
        assert_eq!(slice.unwrap().shape(), &[0, 1]);

        let (ctx_rc, ctx) = b2nd_create_ctx_c(meta, cparams, DParams::default(), Vec::new());
        assert_eq!(ctx_rc, BLOSC2_ERROR_SUCCESS);
        let mut ctx = ctx.unwrap();
        let (ctx_slice_rc, ctx_slice) = b2nd_get_slice_ctx_c(&mut ctx, &array, &start, &stop);
        assert_eq!(ctx_slice_rc, BLOSC2_ERROR_SUCCESS);
        assert_eq!(ctx.meta.shape, vec![0, 1]);
        assert_eq!(ctx_slice.unwrap().shape(), &[0, 1]);
    }

    #[test]
    fn test_b2nd_append_invalid_buffer_len_is_atomic() {
        let meta = B2ndMeta::new(vec![4], vec![2], vec![1], "|u1", 0).unwrap();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut array =
            B2ndArray::from_dense_buffer(meta, &[1, 2, 3, 4], cparams, DParams::default()).unwrap();
        let before_shape = array.shape().to_vec();
        let before_data = array.to_dense_buffer().unwrap();
        let before_nchunks = array.schunk.nchunks();

        assert_eq!(
            array.append(0, &[2], &[9]).err(),
            Some("B2ND insert buffer size does not match buffer shape and typesize")
        );

        assert_eq!(array.shape(), before_shape.as_slice());
        assert_eq!(array.to_dense_buffer().unwrap(), before_data);
        assert_eq!(array.schunk.nchunks(), before_nchunks);
    }

    #[test]
    fn test_b2nd_append_cbuffer_mirrors_c_full_chunk_path_with_extra_tail_chunk() {
        let meta = B2ndMeta::new(vec![2, 5], vec![2, 3], vec![2, 3], "|u1", 0).unwrap();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut array = B2ndArray::from_dense_buffer(
            meta,
            &(0..10u8).collect::<Vec<_>>(),
            cparams,
            DParams::default(),
        )
        .unwrap();

        assert_eq!(
            b2nd_append_c(&mut array, &[10, 11, 12, 13, 14, 15], 6, 0),
            BLOSC2_ERROR_SUCCESS
        );

        assert_eq!(array.shape(), &[4, 5]);
        assert_eq!(array.schunk.nchunks(), 5);
        assert_eq!(
            array.to_dense_buffer().unwrap(),
            vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        );
    }

    #[test]
    fn test_b2nd_attached_full_chunk_append_failure_is_atomic() {
        let meta = B2ndMeta::new(vec![2], vec![2], vec![1], "|u1", 0).unwrap();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut array =
            B2ndArray::from_dense_buffer(meta, &[1, 2], cparams, DParams::default()).unwrap();
        array.schunk.set_storage(FrameStorage::Contiguous);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("attached.b2frame");
        array.save(&path).unwrap();
        let mut opened = B2ndArray::open(&path).unwrap();
        let before_shape = opened.shape().to_vec();
        let before_data = opened.to_dense_buffer().unwrap();
        let before_nchunks = opened.schunk.nchunks();
        std::fs::remove_file(&path).unwrap();
        std::fs::create_dir(&path).unwrap();

        assert_eq!(
            opened.append(0, &[2], &[3, 4]).err(),
            Some("Failed to write attached frame")
        );
        assert_eq!(opened.shape(), before_shape.as_slice());
        assert_eq!(opened.to_dense_buffer().unwrap(), before_data);
        assert_eq!(opened.schunk.nchunks(), before_nchunks);

        std::fs::remove_dir(&path).unwrap();
        opened.schunk.update_chunk(0, &[7, 8]).unwrap();
        let reopened = B2ndArray::open(&path).unwrap();
        assert_eq!(reopened.to_dense_buffer().unwrap(), vec![7, 8]);

        std::fs::remove_file(&path).unwrap();
        opened.append(0, &[2], &[3, 4]).unwrap();
        let reopened = B2ndArray::open(&path).unwrap();
        assert_eq!(reopened.to_dense_buffer().unwrap(), vec![7, 8, 3, 4]);
    }

    #[test]
    fn test_b2nd_append_full_chunk_uses_direct_chunk_path() {
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut array = B2ndArray::from_dense_buffer(
            B2ndMeta::new(vec![2, 3], vec![2, 3], vec![2, 3], "|u1", 0).unwrap(),
            &[1, 2, 3, 4, 5, 6],
            cparams.clone(),
            DParams::default(),
        )
        .unwrap();
        let first_raw = array.schunk.compressed_chunk_bytes(0).unwrap().to_vec();
        let appended = [7, 8, 9, 10, 11, 12];

        array.append(0, &[2, 3], &appended).unwrap();

        let mut expected = Schunk::new(cparams, DParams::default());
        expected.append_chunk(&first_raw).unwrap();
        expected.append_buffer(&appended).unwrap();
        assert_eq!(array.shape(), &[4, 3]);
        assert_eq!(array.schunk.nchunks(), 2);
        assert_eq!(array.schunk.compressed_chunk_bytes(0).unwrap(), first_raw);
        assert_eq!(
            array.schunk.compressed_chunk_bytes(1).unwrap(),
            expected.compressed_chunk_bytes(1).unwrap()
        );
        assert_eq!(
            array.to_dense_buffer().unwrap(),
            vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
        );
    }

    #[test]
    fn test_b2nd_insert_c_shape_cases() {
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };

        let shape = [12usize, 10, 14];
        let data: Vec<u8> = (0..shape.iter().product::<usize>())
            .map(|idx| (idx % 251) as u8)
            .collect();
        let inserted_shape = [12usize, 10, 18];
        let inserted: Vec<u8> = (0..inserted_shape.iter().product::<usize>())
            .map(|idx| (idx % 253) as u8)
            .collect();
        let mut array = B2ndArray::from_dense_buffer(
            B2ndMeta::new(vec![12, 10, 14], vec![3, 5, 9], vec![3, 4, 4], "|u1", 0).unwrap(),
            &data,
            cparams.clone(),
            DParams::default(),
        )
        .unwrap();
        array.insert(2, 9, &[12, 10, 18], &inserted).unwrap();
        assert_eq!(array.shape(), &[12, 10, 32]);
        assert_eq!(
            array.to_dense_buffer().unwrap(),
            insert_axis_expected(&data, &shape, 2, 9, 18, &inserted)
        );
        assert_eq!(
            array
                .slice_to_dense_buffer(&[0, 0, 9], &[12, 10, 27], &[12, 10, 18])
                .unwrap(),
            inserted
        );

        let shape = [10usize, 10, 5, 5];
        let data: Vec<u8> = (0..shape.iter().product::<usize>())
            .map(|idx| (idx % 251) as u8)
            .collect();
        let inserted_shape = [10usize, 10, 5, 30];
        let inserted: Vec<u8> = (0..inserted_shape.iter().product::<usize>())
            .map(|idx| (idx % 253) as u8)
            .collect();
        let mut array = B2ndArray::from_dense_buffer(
            B2ndMeta::new(
                vec![10, 10, 5, 5],
                vec![5, 7, 3, 3],
                vec![2, 2, 1, 1],
                "|u1",
                0,
            )
            .unwrap(),
            &data,
            cparams,
            DParams::default(),
        )
        .unwrap();
        array.insert(3, 3, &[10, 10, 5, 30], &inserted).unwrap();
        assert_eq!(array.shape(), &[10, 10, 5, 35]);
        assert_eq!(
            array.to_dense_buffer().unwrap(),
            insert_axis_expected(&data, &shape, 3, 3, 30, &inserted)
        );
    }

    #[test]
    fn test_b2nd_delete_c_shape_cases() {
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };

        let mut array = B2ndArray::from_dense_buffer(
            B2ndMeta::new(vec![10], vec![3], vec![2], "|u1", 0).unwrap(),
            &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            cparams.clone(),
            DParams::default(),
        )
        .unwrap();
        array.delete(0, 5, 5).unwrap();
        assert_eq!(array.shape(), &[5]);
        assert_eq!(array.to_dense_buffer().unwrap(), vec![1, 2, 3, 4, 5]);
        assert_eq!(array.schunk.nchunks(), 2);

        let shape = [12usize, 10, 32];
        let data: Vec<u8> = (0..shape.iter().product::<usize>())
            .map(|idx| (idx % 251) as u8)
            .collect();
        let mut array = B2ndArray::from_dense_buffer(
            B2ndMeta::new(vec![12, 10, 32], vec![3, 5, 9], vec![3, 4, 4], "|u1", 0).unwrap(),
            &data,
            cparams.clone(),
            DParams::default(),
        )
        .unwrap();
        array.delete(2, 9, 18).unwrap();
        assert_eq!(array.shape(), &[12, 10, 14]);
        assert_eq!(
            array.to_dense_buffer().unwrap(),
            delete_axis_expected(&data, &shape, 2, 9, 18)
        );

        let shape = [10usize, 10, 5, 35];
        let data: Vec<u8> = (0..shape.iter().product::<usize>())
            .map(|idx| (idx % 251) as u8)
            .collect();
        let mut array = B2ndArray::from_dense_buffer(
            B2ndMeta::new(
                vec![10, 10, 5, 35],
                vec![5, 7, 3, 3],
                vec![2, 2, 1, 1],
                "|u1",
                0,
            )
            .unwrap(),
            &data,
            cparams,
            DParams::default(),
        )
        .unwrap();
        array.delete(3, 3, 30).unwrap();
        assert_eq!(array.shape(), &[10, 10, 5, 5]);
        assert_eq!(
            array.to_dense_buffer().unwrap(),
            delete_axis_expected(&data, &shape, 3, 3, 30)
        );
    }

    #[test]
    fn test_b2nd_append_empty_c_shape_cases() {
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };

        let data: Vec<u8> = (0..13u8).collect();
        let mut array = B2ndArray::from_dense_buffer(
            B2ndMeta::new(vec![0], vec![3], vec![3], "|u1", 0).unwrap(),
            &[],
            cparams.clone(),
            DParams::default(),
        )
        .unwrap();
        array.append(0, &[13], &data).unwrap();
        assert_eq!(array.shape(), &[13]);
        assert_eq!(array.to_dense_buffer().unwrap(), data);

        for (chunkshape, blockshape) in [
            (vec![6, 6], vec![6, 6]),
            (vec![6, 6], vec![3, 6]),
            (vec![6, 6], vec![4, 6]),
        ] {
            let data: Vec<u8> = (0..(13 * 6)).map(|idx| (idx % 251) as u8).collect();
            let mut array = B2ndArray::from_dense_buffer(
                B2ndMeta::new(vec![0, 6], chunkshape, blockshape, "|u1", 0).unwrap(),
                &[],
                cparams.clone(),
                DParams::default(),
            )
            .unwrap();
            array.append(0, &[13, 6], &data).unwrap();
            assert_eq!(array.shape(), &[13, 6]);
            assert_eq!(array.to_dense_buffer().unwrap(), data);
        }

        let mut array = B2ndArray::from_dense_buffer(
            B2ndMeta::new(vec![4, 2], vec![2, 3], vec![2, 3], "|u1", 0).unwrap(),
            &[1, 2, 3, 4, 5, 6, 7, 8],
            cparams.clone(),
            DParams::default(),
        )
        .unwrap();
        assert_eq!(
            b2nd_append_c(&mut array, &[9, 10, 0, 11, 12, 0], 6, 0),
            BLOSC2_ERROR_SUCCESS
        );
        assert_eq!(array.shape(), &[6, 2]);
        assert_eq!(
            array.to_dense_buffer().unwrap(),
            vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
        );

        let mut unaligned = B2ndArray::from_dense_buffer(
            B2ndMeta::new(vec![3, 2], vec![2, 3], vec![2, 3], "|u1", 0).unwrap(),
            &[1, 2, 3, 4, 5, 6],
            cparams,
            DParams::default(),
        )
        .unwrap();
        assert_eq!(
            b2nd_append_c(&mut unaligned, &[7, 8, 0, 9, 10, 0], 6, 0),
            BLOSC2_ERROR_SUCCESS
        );
        assert_eq!(unaligned.shape(), &[5, 2]);
        assert_eq!(
            unaligned.to_dense_buffer().unwrap(),
            vec![1, 2, 3, 4, 5, 6, 0, 0, 7, 8]
        );

        let mut zero_extent = B2ndArray::from_dense_buffer(
            B2ndMeta::new(vec![2, 2], vec![2, 2], vec![1, 1], "|u1", 0).unwrap(),
            &[1, 2, 3, 4],
            CParams {
                typesize: 1,
                ..Default::default()
            },
            DParams::default(),
        )
        .unwrap();
        assert!(zero_extent.append(0, &[0, 2], &[9, 9]).is_err());
        assert!(zero_extent.insert(1, 1, &[2, 0], &[9, 9]).is_err());
        zero_extent.append(0, &[0, 2], &[]).unwrap();
        assert_eq!(zero_extent.to_dense_buffer().unwrap(), vec![1, 2, 3, 4]);
    }

    #[test]
    fn test_b2nd_tail_insert_delete_matches_resize_without_start() {
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let shape = [5usize, 3];
        let data: Vec<u8> = (0..shape.iter().product::<usize>())
            .map(|idx| (idx + 1) as u8)
            .collect();
        let inserted_shape = [2usize, 3];
        let inserted: Vec<u8> = (100..106u8).collect();
        let mut array = B2ndArray::from_dense_buffer(
            B2ndMeta::new(vec![5, 3], vec![3, 2], vec![2, 1], "|u1", 0).unwrap(),
            &data,
            cparams,
            DParams::default(),
        )
        .unwrap();

        array.insert(0, 5, &[2, 3], &inserted).unwrap();
        assert_eq!(array.shape(), &[7, 3]);
        assert_eq!(
            array.to_dense_buffer().unwrap(),
            insert_axis_expected(&data, &shape, 0, 5, 2, &inserted)
        );
        assert_eq!(array.schunk.nchunks(), 6);

        array.delete(0, 5, 2).unwrap();
        assert_eq!(array.shape(), &[5, 3]);
        assert_eq!(array.to_dense_buffer().unwrap(), data);
        assert_eq!(inserted_shape.iter().product::<usize>(), inserted.len());
    }

    #[test]
    fn test_b2nd_resize_tail_append_preserves_partial_tail_chunk() {
        let meta = B2ndMeta::new(vec![5], vec![3], vec![1], "|u1", 0).unwrap();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut array =
            B2ndArray::from_dense_buffer(meta, &[1, 2, 3, 4, 5], cparams, DParams::default())
                .unwrap();
        let saved_tail = replace_raw_chunk(&mut array, 1, &[4, 5, 0]);

        array.resize(vec![7]).unwrap();

        assert_eq!(array.shape(), &[7]);
        assert_eq!(array.to_dense_buffer().unwrap(), vec![1, 2, 3, 4, 5, 0, 0]);
        assert_eq!(
            array.schunk.compressed_chunk_bytes(1).unwrap(),
            saved_tail.as_slice()
        );
        assert_eq!(array.schunk.nchunks(), 3);
    }

    #[test]
    fn test_b2nd_resize_multi_axis_chunk_predicate() {
        let meta = B2ndMeta::new(vec![4, 4], vec![2, 2], vec![1, 1], "|u1", 0).unwrap();
        let values: Vec<u8> = (0..16u8).collect();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut array =
            B2ndArray::from_dense_buffer(meta, &values, cparams, DParams::default()).unwrap();
        let saved_bottom_right = replace_raw_chunk(&mut array, 3, &[10, 11, 14, 15]);

        array.resize_with_start(vec![6, 6], Some(&[2, 2])).unwrap();

        let mut expected = vec![0u8; 36];
        for row in 0..2 {
            for col in 0..2 {
                expected[row * 6 + col] = values[row * 4 + col];
            }
            for col in 2..4 {
                expected[row * 6 + col + 2] = values[row * 4 + col];
            }
        }
        for row in 2..4 {
            for col in 0..2 {
                expected[(row + 2) * 6 + col] = values[row * 4 + col];
            }
            for col in 2..4 {
                expected[(row + 2) * 6 + col + 2] = values[row * 4 + col];
            }
        }

        assert_eq!(array.shape(), &[6, 6]);
        assert_eq!(array.to_dense_buffer().unwrap(), expected);
        assert_eq!(
            array.schunk.compressed_chunk_bytes(8).unwrap(),
            saved_bottom_right.as_slice()
        );
    }

    #[test]
    fn test_b2nd_resize_at_middle_deletion_and_validation() {
        let meta = B2ndMeta::new(vec![6, 6], vec![2, 3], vec![1, 3], "<u2", 0).unwrap();
        let values: Vec<u16> = (0..36u16).collect();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 2,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut array =
            B2ndArray::from_dense_buffer(meta, &u16_bytes(&values), cparams, DParams::default())
                .unwrap();

        assert!(array.resize_with_start(vec![8, 6], Some(&[1, 0])).is_err());
        assert!(array.resize_with_start(vec![8, 6], Some(&[-2, 0])).is_err());
        assert!(array.resize_with_start(vec![8, 6], Some(&[2])).is_err());
        assert!(array.resize_with_start(vec![8], Some(&[2, 0])).is_err());

        array.resize_with_start(vec![4, 6], Some(&[2, 0])).unwrap();

        let mut expected = Vec::new();
        for row in 0..2 {
            for col in 0..6 {
                expected.push(values[row * 6 + col]);
            }
        }
        for row in 4..6 {
            for col in 0..6 {
                expected.push(values[row * 6 + col]);
            }
        }
        assert_eq!(array.shape(), &[4, 6]);
        assert_eq!(array.to_dense_buffer().unwrap(), u16_bytes(&expected));
    }
}
