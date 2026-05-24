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
pub mod utils;

pub use b2nd::{
    b2nd_append, b2nd_append_axis_c, b2nd_append_c, b2nd_concatenate, b2nd_concatenate_axis_c,
    b2nd_concatenate_c, b2nd_concatenate_ctx_axis_c, b2nd_concatenate_ctx_c, b2nd_copy,
    b2nd_copy_buffer, b2nd_copy_buffer2, b2nd_copy_c, b2nd_copy_ctx_c, b2nd_create_ctx,
    b2nd_create_ctx_c, b2nd_create_ctx_parts_c, b2nd_create_ctx_parts_with_storage_c,
    b2nd_create_ctx_with_storage, b2nd_create_ctx_with_storage_c, b2nd_delete, b2nd_delete_axis_c,
    b2nd_delete_c, b2nd_deserialize_meta, b2nd_deserialize_meta_c, b2nd_empty, b2nd_empty_c,
    b2nd_empty_ctx_c, b2nd_expand_dims, b2nd_expand_dims_c, b2nd_expand_dims_final_c, b2nd_free_c,
    b2nd_free_ctx_c, b2nd_free_option_c, b2nd_from_cbuffer, b2nd_from_cbuffer_c,
    b2nd_from_cbuffer_ctx_c, b2nd_from_cframe, b2nd_from_cframe_c, b2nd_from_schunk,
    b2nd_from_schunk_c, b2nd_full, b2nd_full_c, b2nd_full_ctx_c, b2nd_get_orthogonal_selection,
    b2nd_get_orthogonal_selection_c, b2nd_get_orthogonal_selection_c_sizes_c,
    b2nd_get_orthogonal_selection_cbuffer, b2nd_get_orthogonal_selection_cbuffer_c, b2nd_get_slice,
    b2nd_get_slice_c, b2nd_get_slice_cbuffer, b2nd_get_slice_cbuffer_c, b2nd_get_slice_cbuffer_vec,
    b2nd_get_slice_ctx_c, b2nd_get_slice_nchunks, b2nd_get_slice_nchunks_vec, b2nd_insert,
    b2nd_insert_axis_c, b2nd_insert_c, b2nd_nans, b2nd_nans_c, b2nd_nans_ctx_c, b2nd_open,
    b2nd_open_c, b2nd_open_offset, b2nd_open_offset_c, b2nd_print_meta, b2nd_print_meta_c,
    b2nd_print_meta_to_buffer_c, b2nd_resize, b2nd_resize_c, b2nd_save, b2nd_save_append,
    b2nd_serialize_meta, b2nd_serialize_meta_c, b2nd_serialize_meta_parts,
    b2nd_serialize_meta_parts_c, b2nd_set_orthogonal_selection, b2nd_set_orthogonal_selection_c,
    b2nd_set_orthogonal_selection_c_sizes_c, b2nd_set_orthogonal_selection_cbuffer,
    b2nd_set_orthogonal_selection_cbuffer_c, b2nd_set_slice_cbuffer, b2nd_set_slice_cbuffer_c,
    b2nd_squeeze, b2nd_squeeze_c, b2nd_squeeze_index, b2nd_squeeze_index_c, b2nd_to_cbuffer,
    b2nd_to_cbuffer_c, b2nd_to_cbuffer_vec, b2nd_to_cframe, b2nd_to_cframe_c, b2nd_uninit,
    b2nd_uninit_c, b2nd_uninit_ctx_c, b2nd_zeros, b2nd_zeros_c, b2nd_zeros_ctx_c, B2ndArray,
    B2ndContext, B2ndMeta, B2ndStorage, B2ND_MAX_DIM, DTYPE_NUMPY_FORMAT,
};
pub use codecs::{
    blosc2_register_codec, blosc2_register_codec_c, register_global_codec,
    register_global_codec_with_metadata, register_named_global_codec, Blosc2Codec,
};
pub use compress::{
    blosc1_cbuffer_metainfo, blosc1_cbuffer_sizes, blosc1_cbuffer_validate, blosc1_compress_c,
    blosc1_decompress_c, blosc1_get_blocksize, blosc1_get_compressor, blosc1_get_splitmode,
    blosc1_getitem, blosc1_set_blocksize, blosc1_set_compressor, blosc1_set_compressor_c,
    blosc1_set_splitmode, blosc2_cbuffer_complib, blosc2_cbuffer_metainfo,
    blosc2_cbuffer_metainfo2_c, blosc2_cbuffer_sizes, blosc2_cbuffer_versions, blosc2_chunk_nans,
    blosc2_chunk_nans_c, blosc2_chunk_nans_with_cparams, blosc2_chunk_repeatval,
    blosc2_chunk_repeatval_c, blosc2_chunk_repeatval_with_cparams, blosc2_chunk_uninit,
    blosc2_chunk_uninit_c, blosc2_chunk_uninit_with_cparams, blosc2_chunk_zeros,
    blosc2_chunk_zeros_c, blosc2_chunk_zeros_with_cparams, blosc2_compcode_to_compname,
    blosc2_compcode_to_compname_c, blosc2_compcode_to_compname_int_c, blosc2_compname_to_compcode,
    blosc2_compname_to_compcode_c, blosc2_compress, blosc2_compress_ctx, blosc2_create_cctx,
    blosc2_create_cctx_c, blosc2_create_dctx, blosc2_create_dctx_c, blosc2_ctx_get_cparams,
    blosc2_ctx_get_dparams, blosc2_decompress, blosc2_decompress_ctx, blosc2_free_ctx,
    blosc2_free_ctx_c, blosc2_get_blosc2_cparams_defaults, blosc2_get_blosc2_dparams_defaults,
    blosc2_get_complib_info, blosc2_get_delta, blosc2_get_nthreads, blosc2_get_version_string,
    blosc2_getitem_c, blosc2_getitem_ctx_c, blosc2_list_compressors, blosc2_set_delta,
    blosc2_set_nthreads, blosc2_vlchunk_get_nblocks_c, blosc2_vlcompress_ctx,
    blosc2_vlcompress_ctx_c, blosc2_vldecompress_block_ctx, blosc2_vldecompress_block_ctx_c,
    blosc2_vldecompress_ctx, blosc2_vldecompress_ctx_c, blosc_cbuffer_complib,
    blosc_cbuffer_metainfo, blosc_cbuffer_sizes, blosc_cbuffer_validate, blosc_cbuffer_versions,
    blosc_compcode_to_compname, blosc_compname_to_compcode, blosc_compress, blosc_decompress,
    blosc_get_blocksize, blosc_get_complib_info, blosc_get_compressor, blosc_get_nthreads,
    blosc_get_version_string, blosc_getitem, blosc_list_compressors, blosc_set_blocksize,
    blosc_set_compressor, blosc_set_compressor_c, blosc_set_nthreads, blosc_set_splitmode,
    cbuffer_metainfo_flags_c, cbuffer_sizes_c, cbuffer_validate_c, cbuffer_versions_c, getitem_c,
    CContext, CParams, DContext, DParams,
};
pub use filters::{
    blosc2_bitshuffle, blosc2_bitunshuffle, blosc2_register_filter, blosc2_register_filter_c,
    blosc2_shuffle, blosc2_unshuffle, is_registered_filter, register_fallible_filter,
    register_filter, register_global_fallible_filter,
    register_global_fallible_filter_with_metadata, register_global_filter,
    register_global_filter_with_metadata, register_named_global_fallible_filter,
    register_named_global_filter, registered_filter_info, Blosc2Filter,
};
pub use schunk::{
    blosc2_frame_get_offsets, blosc2_get_slice_nchunks, blosc2_meta_add, blosc2_meta_add_c,
    blosc2_meta_delete, blosc2_meta_exists, blosc2_meta_get, blosc2_meta_get_names,
    blosc2_meta_update, blosc2_meta_update_c, blosc2_schunk_append_buffer,
    blosc2_schunk_append_buffer_c, blosc2_schunk_append_chunk, blosc2_schunk_append_chunk_c,
    blosc2_schunk_append_file, blosc2_schunk_append_vlblocks_c, blosc2_schunk_copy,
    blosc2_schunk_copy_c, blosc2_schunk_decompress_chunk, blosc2_schunk_decompress_chunk_c,
    blosc2_schunk_decompress_vlblock_c, blosc2_schunk_delete_chunk, blosc2_schunk_fill_special,
    blosc2_schunk_frame_get_offsets, blosc2_schunk_frame_len, blosc2_schunk_free_c,
    blosc2_schunk_from_buffer, blosc2_schunk_from_buffer_c, blosc2_schunk_from_buffer_vec,
    blosc2_schunk_get_chunk, blosc2_schunk_get_chunk_ref, blosc2_schunk_get_cparams,
    blosc2_schunk_get_cparams_c, blosc2_schunk_get_dparams, blosc2_schunk_get_dparams_c,
    blosc2_schunk_get_lazychunk, blosc2_schunk_get_lazychunk_c, blosc2_schunk_get_slice_buffer,
    blosc2_schunk_get_slice_buffer_c, blosc2_schunk_get_slice_buffer_size_c,
    blosc2_schunk_get_slice_nchunks, blosc2_schunk_get_slice_nchunks_c, blosc2_schunk_get_vlblock,
    blosc2_schunk_insert_buffer, blosc2_schunk_insert_buffer_c, blosc2_schunk_insert_chunk,
    blosc2_schunk_insert_chunk_c, blosc2_schunk_insert_vlblocks_c, blosc2_schunk_new_c,
    blosc2_schunk_open, blosc2_schunk_open_c, blosc2_schunk_open_lazy, blosc2_schunk_open_lazy_c,
    blosc2_schunk_open_lazy_offset, blosc2_schunk_open_lazy_offset_c, blosc2_schunk_open_offset,
    blosc2_schunk_open_offset_c, blosc2_schunk_reorder_offsets, blosc2_schunk_set_slice_buffer,
    blosc2_schunk_set_slice_buffer_c, blosc2_schunk_set_slice_buffer_size_c,
    blosc2_schunk_to_buffer, blosc2_schunk_to_buffer_vec, blosc2_schunk_to_file,
    blosc2_schunk_update_buffer, blosc2_schunk_update_buffer_c, blosc2_schunk_update_chunk,
    blosc2_schunk_update_chunk_c, blosc2_schunk_update_vlblocks_c, blosc2_vlmeta_add,
    blosc2_vlmeta_add_c, blosc2_vlmeta_delete, blosc2_vlmeta_exists, blosc2_vlmeta_get,
    blosc2_vlmeta_get_names, blosc2_vlmeta_update, blosc2_vlmeta_update_c, LazySchunk, Schunk,
    SchunkLazyChunkAccessor,
};
pub use utils::{
    blosc2_destroy, blosc2_error_string, blosc2_free_resources, blosc2_init,
    blosc2_multidim_to_unidim, blosc2_remove_dir, blosc2_remove_urlpath, blosc2_rename_urlpath,
    blosc2_unidim_to_multidim, blosc_destroy, blosc_free_resources, blosc_init,
};

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
