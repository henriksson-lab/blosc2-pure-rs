//! Public API parity checklist against the vendored C-Blosc2 headers.
//!
//! This is intentionally not an extern "C" ABI test. The crate exposes
//! Rust-shaped functions and C-name-style adapters, so this matrix tracks the
//! practical parity surface: whether each public C header function has a
//! maintained Rust equivalent, is only partially equivalent, or is deliberately
//! unsupported/out of scope for now.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io;
use std::path::PathBuf;

use blosc2_pure_rs::constants::*;
use blosc2_pure_rs::{
    b2nd_from_cbuffer, b2nd_get_slice_nchunks, b2nd_get_slice_nchunks_vec, blosc2_compress_ctx,
    blosc2_create_cctx, blosc2_create_dctx, blosc2_decompress_ctx, blosc2_set_maskout, B2ndMeta,
    Blosc2CodecDecoderCb, Blosc2CodecEncoderCb, Blosc2FilterBackwardCb, Blosc2FilterForwardCb,
    Blosc2PostfilterCb, Blosc2PrefilterCb, CParams, DParams, B2ND_DEFAULT_DTYPE,
    B2ND_DEFAULT_DTYPE_FORMAT, B2ND_MAX_DIM, B2ND_MAX_METALAYERS, B2ND_METALAYER_NAME,
    B2ND_METALAYER_VERSION, DTYPE_NUMPY_FORMAT,
};

const LIB_RS: &str = include_str!("../src/lib.rs");

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum ApiStatus {
    Covered,
    Partial,
    Unsupported,
    OutOfScope,
}

#[derive(Clone, Copy, Debug)]
struct ApiRow {
    c_name: &'static str,
    rust_symbol: Option<&'static str>,
    status: ApiStatus,
    note: &'static str,
}

macro_rules! covered {
    ($name:literal) => {
        ApiRow {
            c_name: $name,
            rust_symbol: Some($name),
            status: ApiStatus::Covered,
            note: "",
        }
    };
    ($name:literal => $rust:literal) => {
        ApiRow {
            c_name: $name,
            rust_symbol: Some($rust),
            status: ApiStatus::Covered,
            note: "",
        }
    };
}

macro_rules! partial {
    ($name:literal, $note:literal) => {
        ApiRow {
            c_name: $name,
            rust_symbol: Some($name),
            status: ApiStatus::Partial,
            note: $note,
        }
    };
    ($name:literal => $rust:literal, $note:literal) => {
        ApiRow {
            c_name: $name,
            rust_symbol: Some($rust),
            status: ApiStatus::Partial,
            note: $note,
        }
    };
}

macro_rules! unsupported {
    ($name:literal, $note:literal) => {
        ApiRow {
            c_name: $name,
            rust_symbol: None,
            status: ApiStatus::Unsupported,
            note: $note,
        }
    };
}

macro_rules! out_of_scope {
    ($name:literal, $note:literal) => {
        ApiRow {
            c_name: $name,
            rust_symbol: None,
            status: ApiStatus::OutOfScope,
            note: $note,
        }
    };
}

const API_MATRIX: &[ApiRow] = &[
    covered!("blosc2_error_string"),
    covered!("blosc2_init"),
    covered!("blosc2_destroy"),
    covered!("blosc2_free_resources"),
    covered!("blosc1_compress"),
    covered!("blosc1_decompress"),
    covered!("blosc1_getitem"),
    covered!("blosc2_getitem" => "blosc2_getitem_c"),
    covered!("blosc2_get_nthreads"),
    covered!("blosc2_set_nthreads"),
    covered!("blosc1_get_compressor"),
    covered!("blosc1_set_compressor"),
    covered!("blosc2_set_delta"),
    covered!("blosc2_compcode_to_compname"),
    covered!("blosc2_compname_to_compcode"),
    covered!("blosc2_list_compressors"),
    covered!("blosc2_get_version_string"),
    covered!("blosc2_get_complib_info"),
    covered!("blosc1_cbuffer_sizes"),
    covered!("blosc2_cbuffer_sizes"),
    covered!("blosc1_cbuffer_validate"),
    covered!("blosc1_cbuffer_metainfo"),
    covered!("blosc2_cbuffer_versions"),
    covered!("blosc2_cbuffer_complib"),
    covered!("blosc2_create_cctx"),
    covered!("blosc2_create_dctx"),
    covered!("blosc2_free_ctx"),
    covered!("blosc2_ctx_get_cparams"),
    covered!("blosc2_ctx_get_dparams"),
    covered!("blosc2_compress"),
    covered!("blosc2_decompress"),
    covered!("blosc2_compress_ctx"),
    covered!("blosc2_vlcompress_ctx"),
    covered!("blosc2_decompress_ctx"),
    covered!("blosc2_vldecompress_ctx"),
    covered!("blosc2_vlchunk_get_nblocks" => "blosc2_vlchunk_get_nblocks_c"),
    covered!("blosc2_vldecompress_block_ctx"),
    covered!("blosc2_chunk_zeros"),
    covered!("blosc2_chunk_nans"),
    covered!("blosc2_chunk_repeatval"),
    covered!("blosc2_chunk_uninit"),
    covered!("blosc2_getitem_ctx" => "blosc2_getitem_ctx_c"),
    covered!("blosc2_get_blosc2_cparams_defaults"),
    covered!("blosc2_get_blosc2_dparams_defaults"),
    covered!("blosc2_schunk_from_buffer"),
    partial!(
        "blosc2_schunk_open",
        "Rust open eagerly loads into an owned Schunk; C keeps a no-copy file-backed frame, whose closest Rust equivalent is blosc2_schunk_open_lazy(_c)."
    ),
    partial!(
        "blosc2_schunk_open_offset",
        "Rust open_frame_at eagerly loads into an owned Schunk; C keeps a no-copy file-backed frame, whose closest Rust equivalent is blosc2_schunk_open_lazy_offset(_c)."
    ),
    covered!("blosc2_schunk_to_buffer"),
    covered!("blosc2_schunk_to_file"),
    covered!("blosc2_schunk_append_file"),
    covered!("blosc2_schunk_free" => "blosc2_schunk_free_c"),
    covered!("blosc2_schunk_append_buffer"),
    covered!("blosc2_schunk_decompress_chunk"),
    covered!("blosc2_schunk_get_chunk"),
    covered!("blosc2_schunk_get_vlblock"),
    covered!("blosc2_schunk_get_slice_buffer"),
    covered!("blosc2_schunk_set_slice_buffer"),
    covered!("blosc2_schunk_get_cparams"),
    covered!("blosc2_schunk_get_dparams"),
    covered!("blosc2_schunk_frame_len"),
    covered!("blosc2_schunk_fill_special"),
    covered!("blosc2_frame_get_offsets"),
    covered!("blosc2_remove_dir"),
    covered!("blosc2_remove_urlpath"),
    covered!("blosc2_rename_urlpath"),
    covered!("blosc2_unidim_to_multidim"),
    covered!("blosc2_multidim_to_unidim"),
    covered!("blosc2_get_slice_nchunks"),
    covered!("blosc2_shuffle"),
    covered!("blosc2_unshuffle"),
    covered!("blosc2_bitshuffle"),
    covered!("blosc2_bitunshuffle"),
    covered!("b2nd_free" => "b2nd_free_c"),
    covered!("b2nd_from_schunk"),
    partial!("b2nd_to_cframe", "Always serializes to an owned Vec; borrowed-frame needs_free=false ownership is not modeled."),
    covered!("b2nd_open"),
    covered!("b2nd_open_offset"),
    covered!("b2nd_save"),
    covered!("b2nd_save_append"),
    covered!("b2nd_from_cbuffer"),
    covered!("b2nd_to_cbuffer"),
    covered!("b2nd_get_slice"),
    covered!("b2nd_get_slice_cbuffer"),
    covered!("b2nd_set_slice_cbuffer"),
    covered!("b2nd_copy"),
    partial!("b2nd_concatenate", "copy=false mutates the input and returns a clone of the updated left array, matching C's observable *array = src1; exact C pointer aliasing is not modeled."),
    covered!("b2nd_print_meta"),
    covered!("b2nd_resize"),
    covered!("b2nd_insert"),
    covered!("b2nd_append"),
    covered!("b2nd_delete"),
    covered!("b2nd_get_orthogonal_selection" => "b2nd_get_orthogonal_selection_c"),
    covered!("b2nd_set_orthogonal_selection" => "b2nd_set_orthogonal_selection_c"),
    covered!("b2nd_serialize_meta"),
    covered!("b2nd_deserialize_meta"),
    covered!("b2nd_copy_buffer"),
    covered!("b2nd_copy_buffer2"),
    covered!("blosc2_register_codec" => "blosc2_register_codec_abi"),
    covered!("blosc2_register_filter"),
    partial!("blosc2_schunk_new" => "blosc2_schunk_new_c", "Rust helper uses owned storage parameters, not blosc2_storage ABI."),
    partial!("blosc2_schunk_copy", "Rust copies supported data but not all C storage/plugin semantics."),
    partial!("blosc2_schunk_append_chunk", "Implemented, but exact C precompressed chunk validation/storage semantics remain incomplete."),
    partial!("blosc2_schunk_update_chunk", "Implemented, including attached-frame persistence; exact C precompressed chunk semantics remain incomplete."),
    partial!("blosc2_schunk_insert_chunk", "Implemented, including attached-frame persistence; exact C precompressed chunk semantics remain incomplete."),
    covered!("blosc2_schunk_delete_chunk"),
    partial!(
        "blosc2_schunk_get_lazychunk" => "blosc2_schunk_get_lazychunk_c",
        "Uses the C-style adapter that reports needs_free ownership info; the returned chunk is still modeled as an owned Vec rather than a raw C pointer."
    ),
    partial!("blosc2_schunk_reorder_offsets", "In-memory reorder works; exact C frame offset ownership/ABI semantics are not modeled."),
    partial!("blosc2_meta_exists", "In-memory/frame metadata lookup is covered; C inline pointer ABI is not modeled."),
    partial!("blosc2_meta_add", "Metadata is supported, including attached-frame persistence; C inline pointer ABI is not modeled."),
    partial!("blosc2_meta_update", "Metadata is supported, including attached-frame persistence; C inline pointer ABI is not modeled."),
    partial!("blosc2_meta_get", "In-memory/frame metadata lookup is covered; C inline pointer ABI is not modeled."),
    partial!("blosc2_vlmeta_exists", "VL metadata lookup is covered; C inline pointer ABI is not modeled."),
    partial!("blosc2_vlmeta_add", "VL metadata is supported, including attached-frame persistence; C allocation ABI is not modeled."),
    partial!("blosc2_vlmeta_update", "VL metadata is supported, including attached-frame persistence; C allocation ABI is not modeled."),
    partial!("blosc2_vlmeta_get", "VL metadata lookup is covered; C ownership ABI is not modeled."),
    partial!("blosc2_vlmeta_delete", "VL metadata is supported, including attached-frame persistence; C allocation ABI is not modeled."),
    partial!("blosc2_vlmeta_get_names", "VL metadata name listing is Rust-shaped, not C allocation ABI."),
    partial!("b2nd_create_ctx", "B2ndContext exists, but blosc2_storage/urlpath behavior is not fully modeled."),
    partial!("b2nd_free_ctx" => "b2nd_free_ctx_c", "Rust owned context teardown has no C allocation ABI."),
    partial!("b2nd_uninit", "Array creation exists, but storage/urlpath behavior is not fully modeled."),
    partial!("b2nd_empty", "Array creation exists, but storage/urlpath behavior is not fully modeled."),
    partial!("b2nd_zeros", "Array creation exists, but storage/urlpath behavior is not fully modeled."),
    partial!("b2nd_nans", "Array creation exists, but storage/urlpath behavior is not fully modeled."),
    partial!("b2nd_full", "Array creation exists, but storage/urlpath behavior is not fully modeled."),
    covered!("b2nd_from_cframe"),
    covered!("b2nd_squeeze_index"),
    covered!("b2nd_squeeze"),
    covered!("b2nd_expand_dims"),
    unsupported!("blosc2_set_threads_callback", "C thread-pool callback ABI is not implemented."),
    unsupported!("blosc2_register_io_cb", "User-defined IO callback ABI is not implemented."),
    unsupported!("blosc2_get_io_cb", "User-defined IO callback registry is not implemented."),
    unsupported!("blosc2_register_tuner", "C tuner callback ABI is not implemented."),
    covered!("blosc2_set_maskout"),
    unsupported!(
        "blosc2_get_blosc2_storage_defaults",
        "The C blosc2_storage defaults API is not exposed; Rust only has a limited B2ndStorage model, not the full C storage struct/defaults."
    ),
    unsupported!("blosc2_get_blosc2_io_defaults", "blosc2_io is not modeled yet."),
    unsupported!("blosc2_get_blosc2_stdio_mmap_defaults", "stdio mmap storage is not modeled."),
    unsupported!("blosc2_schunk_avoid_cframe_free", "C frame ownership toggle is not relevant to owned Rust buffers."),
    unsupported!("blosc2_schunk_open_udio", "User-defined IO opening is not implemented."),
    unsupported!("blosc2_schunk_open_offset_udio", "User-defined IO opening is not implemented."),
    unsupported!("blosc2_stdio_open", "Public stdio IO callback shim is not exposed as a Rust API."),
    unsupported!("blosc2_stdio_close", "Public stdio IO callback shim is not exposed as a Rust API."),
    unsupported!("blosc2_stdio_size", "Public stdio IO callback shim is not exposed as a Rust API."),
    unsupported!("blosc2_stdio_write", "Public stdio IO callback shim is not exposed as a Rust API."),
    unsupported!("blosc2_stdio_read", "Public stdio IO callback shim is not exposed as a Rust API."),
    unsupported!("blosc2_stdio_truncate", "Public stdio IO callback shim is not exposed as a Rust API."),
    unsupported!("blosc2_stdio_destroy", "Public stdio IO callback shim is not exposed as a Rust API."),
    unsupported!("blosc2_stdio_mmap_open", "Public mmap IO callback shim is not exposed as a Rust API."),
    unsupported!("blosc2_stdio_mmap_close", "Public mmap IO callback shim is not exposed as a Rust API."),
    unsupported!("blosc2_stdio_mmap_size", "Public mmap IO callback shim is not exposed as a Rust API."),
    unsupported!("blosc2_stdio_mmap_write", "Public mmap IO callback shim is not exposed as a Rust API."),
    unsupported!("blosc2_stdio_mmap_read", "Public mmap IO callback shim is not exposed as a Rust API."),
    unsupported!("blosc2_stdio_mmap_truncate", "Public mmap IO callback shim is not exposed as a Rust API."),
    unsupported!("blosc2_stdio_mmap_destroy", "Public mmap IO callback shim is not exposed as a Rust API."),
    out_of_scope!("blosc_set_timestamp", "Benchmark timing helper, not a storage/compression API target."),
    out_of_scope!("blosc_elapsed_nsecs", "Benchmark timing helper, not a storage/compression API target."),
    out_of_scope!("blosc_elapsed_secs", "Benchmark timing helper, not a storage/compression API target."),
    covered!("blosc1_get_blocksize"),
    covered!("blosc1_set_blocksize"),
    covered!("blosc1_set_splitmode"),
];

#[test]
fn public_api_matrix_covers_all_vendored_header_exports() {
    let Some(headers) = read_vendored_public_headers() else {
        return;
    };
    let header_exports = headers
        .iter()
        .flat_map(|(_, header)| exported_c_functions(header))
        .collect::<BTreeSet<_>>();
    let matrix = API_MATRIX
        .iter()
        .map(|row| row.c_name)
        .map(str::to_owned)
        .collect::<BTreeSet<_>>();

    let missing = header_exports
        .difference(&matrix)
        .cloned()
        .collect::<Vec<_>>();
    let stale = matrix
        .difference(&header_exports)
        .cloned()
        .collect::<Vec<_>>();

    assert!(
        missing.is_empty(),
        "public C header exports missing from API_MATRIX: {missing:?}"
    );
    assert!(
        stale.is_empty(),
        "API_MATRIX rows no longer present in vendored headers: {stale:?}"
    );
}

#[test]
fn registry_bootstrap_helpers_are_not_public_c_exports() {
    let Some(headers) = read_vendored_public_headers() else {
        return;
    };
    let registry_exports = headers
        .iter()
        .filter(|(path, _)| path.ends_with("-registry.h"))
        .flat_map(|(_, header)| exported_c_functions(header))
        .collect::<BTreeSet<_>>();

    assert!(
        registry_exports.is_empty(),
        "registry headers should contribute public constants, not non-BLOSC_EXPORT bootstrap helpers: {registry_exports:?}"
    );

    let matrix = API_MATRIX
        .iter()
        .map(|row| row.c_name)
        .collect::<BTreeSet<_>>();
    for helper in ["register_codecs", "register_filters", "register_tuners"] {
        assert!(
            !matrix.contains(helper),
            "non-BLOSC_EXPORT registry bootstrap helper should not be tracked as a public C API row: {helper}"
        );
    }
}

#[test]
fn mapped_public_api_rows_are_reexported_from_the_crate_root() {
    let root_exports = crate_root_exports();
    let missing = API_MATRIX
        .iter()
        .filter(|row| matches!(row.status, ApiStatus::Covered | ApiStatus::Partial))
        .filter_map(|row| {
            let symbol = row.rust_symbol?;
            (!root_exports.contains(symbol)).then_some((row.c_name, symbol, row.note))
        })
        .collect::<Vec<_>>();

    assert!(
        missing.is_empty(),
        "API_MATRIX rows mapped to Rust symbols that are not re-exported from src/lib.rs: {missing:?}"
    );
}

#[test]
fn public_api_matrix_locks_known_b2nd_parity_statuses() {
    let expected = [
        (
            "b2nd_to_cframe",
            ApiStatus::Partial,
            Some("b2nd_to_cframe"),
            "borrowed-frame needs_free=false",
        ),
        (
            "b2nd_concatenate",
            ApiStatus::Partial,
            Some("b2nd_concatenate"),
            "returns a clone of the updated left array",
        ),
        (
            "b2nd_get_orthogonal_selection",
            ApiStatus::Covered,
            Some("b2nd_get_orthogonal_selection_c"),
            "",
        ),
        (
            "b2nd_set_orthogonal_selection",
            ApiStatus::Covered,
            Some("b2nd_set_orthogonal_selection_c"),
            "",
        ),
        (
            "b2nd_open_offset",
            ApiStatus::Covered,
            Some("b2nd_open_offset"),
            "",
        ),
    ];

    for (c_name, status, rust_symbol, note_fragment) in expected {
        let row = API_MATRIX
            .iter()
            .find(|row| row.c_name == c_name)
            .unwrap_or_else(|| panic!("missing API_MATRIX row for {c_name}"));
        assert_eq!(row.status, status, "wrong status for {c_name}");
        assert_eq!(row.rust_symbol, rust_symbol, "wrong symbol for {c_name}");
        assert!(
            note_fragment.is_empty() || row.note.contains(note_fragment),
            "row {c_name} note {:?} should mention {note_fragment:?}",
            row.note
        );
    }
}

#[test]
fn public_b2nd_slice_count_aliases_match_current_c_source_expectations() {
    let root_exports = crate_root_exports();
    for symbol in ["b2nd_get_slice_nchunks", "b2nd_get_slice_nchunks_vec"] {
        assert!(
            root_exports.contains(symbol),
            "public B2ND slice-count alias not re-exported from src/lib.rs: {symbol}"
        );
    }

    let meta = B2ndMeta::new(
        vec![4, 6],
        vec![2, 3],
        vec![1, 3],
        "|u1",
        DTYPE_NUMPY_FORMAT,
    )
    .unwrap();
    let cparams = CParams {
        compcode: BLOSC_LZ4,
        clevel: 5,
        typesize: 1,
        splitmode: BLOSC_NEVER_SPLIT,
        filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
        ..Default::default()
    };
    let data = (0..24).collect::<Vec<u8>>();
    let array = b2nd_from_cbuffer(meta, &data, cparams.clone(), DParams::default()).unwrap();

    assert_eq!(
        b2nd_get_slice_nchunks_vec(&array, &[1, 2], &[4, 6]).unwrap(),
        vec![0, 1, 2, 3]
    );
    assert_eq!(
        b2nd_get_slice_nchunks(&array, &[1, 2], &[4, 6]),
        (4, Some(vec![0, 1, 2, 3]))
    );
    assert_eq!(
        b2nd_get_slice_nchunks(&array, &[1, 2], &[1, 2]),
        (1, Some(vec![0]))
    );
    assert_eq!(b2nd_get_slice_nchunks(&array, &[2, 3], &[2, 3]), (0, None));

    let empty_meta = B2ndMeta::new(
        vec![0, 6],
        vec![0, 3],
        vec![0, 3],
        "|u1",
        DTYPE_NUMPY_FORMAT,
    )
    .unwrap();
    let empty_array = b2nd_from_cbuffer(empty_meta, &[], cparams, DParams::default()).unwrap();
    assert_eq!(
        b2nd_get_slice_nchunks(&empty_array, &[0, 100], &[0, 101]),
        (0, None)
    );
}

#[test]
fn c_style_root_exports_cover_implemented_adapter_variants() {
    let root_exports = crate_root_exports();
    let missing = required_c_style_root_exports()
        .iter()
        .filter(|symbol| !root_exports.contains(**symbol))
        .copied()
        .collect::<Vec<_>>();

    assert!(
        missing.is_empty(),
        "implemented C-style adapter variants not re-exported from src/lib.rs: {missing:?}"
    );
}

#[test]
fn callback_type_aliases_are_reexported_from_the_crate_root() {
    let root_exports = crate_root_exports();
    let required = [
        "Blosc2CodecEncoderCb",
        "Blosc2CodecDecoderCb",
        "Blosc2PrefilterCb",
        "Blosc2PostfilterCb",
        "Blosc2FilterForwardCb",
        "Blosc2FilterBackwardCb",
    ];
    let missing = required
        .iter()
        .filter(|symbol| !root_exports.contains(**symbol))
        .copied()
        .collect::<Vec<_>>();

    assert!(
        missing.is_empty(),
        "public callback type aliases not re-exported from src/lib.rs: {missing:?}"
    );

    let _: Option<Blosc2CodecEncoderCb> = None;
    let _: Option<Blosc2CodecDecoderCb> = None;
    let _: Blosc2PrefilterCb = None;
    let _: Blosc2PostfilterCb = None;
    let _: Option<Blosc2FilterForwardCb> = None;
    let _: Option<Blosc2FilterBackwardCb> = None;
}

#[test]
fn context_param_structs_keep_public_cparams_and_callback_fields() {
    let cparams = CParams::default();
    assert_eq!(cparams.compcode, BLOSC_BLOSCLZ);
    assert_eq!(cparams.compcode_meta, 0);
    assert_eq!(cparams.clevel, 5);
    assert!(!cparams.use_dict);
    assert_eq!(cparams.typesize, 8);
    assert_eq!(cparams.nthreads, 1);
    assert_eq!(cparams.blocksize, 0);
    assert_eq!(cparams.splitmode, BLOSC_FORWARD_COMPAT_SPLIT);
    assert_eq!(cparams.schunk, 0);
    assert_eq!(cparams.filters, [0, 0, 0, 0, 0, BLOSC_SHUFFLE]);
    assert_eq!(cparams.filters_meta, [0; BLOSC2_MAX_FILTERS]);
    assert!(cparams.prefilter.is_none());
    assert_eq!(cparams.prefilter_user_data, 0);
    assert_eq!(cparams.prefilter_output_typesize, 0);
    assert!(!cparams.prefilter_output_is_disposable);
    assert_eq!(cparams.codec_params, 0);
    assert!(!cparams.instr_codec);
    assert_eq!(cparams.nchunk, -1);
    assert!(cparams.b2nd_metalayer.is_none());

    let dparams = DParams::default();
    assert_eq!(dparams.nthreads, 1);
    assert_eq!(dparams.schunk, 0);
    assert!(dparams.postfilter.is_none());
    assert_eq!(dparams.postfilter_user_data, 0);
    assert_eq!(dparams.typesize, 8);
    assert_eq!(dparams.nchunk, -1);
    assert!(dparams.b2nd_metalayer.is_none());
    assert!(dparams.block_maskout.is_none());
}

#[test]
fn blosc2_set_maskout_drives_one_shot_blosc2_decompress_ctx() {
    let data = (0..512u32).map(|i| (i & 0xff) as u8).collect::<Vec<_>>();
    let cctx = blosc2_create_cctx(CParams {
        compcode: BLOSC_LZ4,
        clevel: 5,
        typesize: 1,
        blocksize: 128,
        splitmode: BLOSC_NEVER_SPLIT,
        filters: [0; BLOSC2_MAX_FILTERS],
        ..Default::default()
    })
    .unwrap();
    let dctx = blosc2_create_dctx(DParams::default()).unwrap();

    let mut chunk = vec![0; data.len() + BLOSC2_MAX_OVERHEAD + 32];
    let chunk_len = blosc2_compress_ctx(
        &cctx,
        &data,
        data.len() as i32,
        &mut chunk,
        (data.len() + BLOSC2_MAX_OVERHEAD + 32) as i32,
    );
    assert!(chunk_len > 0);

    let maskout = [false, true, false, true];
    assert_eq!(
        blosc2_set_maskout(&dctx, &maskout, maskout.len() as i32),
        BLOSC2_ERROR_SUCCESS
    );

    let mut dest = vec![0xA5; data.len()];
    assert_eq!(
        blosc2_decompress_ctx(&dctx, &chunk, chunk_len, &mut dest, data.len() as i32),
        data.len() as i32
    );
    assert_eq!(&dest[..128], &data[..128]);
    assert_eq!(&dest[128..256], &[0xA5; 128]);
    assert_eq!(&dest[256..384], &data[256..384]);
    assert_eq!(&dest[384..512], &[0xA5; 128]);

    assert_eq!(
        blosc2_decompress_ctx(&dctx, &chunk, chunk_len, &mut dest, data.len() as i32),
        data.len() as i32
    );
    assert_eq!(dest, data);
}

#[test]
fn public_api_matrix_is_not_just_a_smoke_test() {
    let mut counts = BTreeMap::new();
    for row in API_MATRIX {
        *counts.entry(row.status).or_insert(0usize) += 1;
    }

    assert!(
        API_MATRIX.len() > 100,
        "matrix should track the full public header surface"
    );
    assert!(
        counts.get(&ApiStatus::Covered).copied().unwrap_or(0) >= 75,
        "matrix should record substantial implemented coverage: {counts:?}"
    );
    assert!(
        counts.get(&ApiStatus::Partial).copied().unwrap_or(0) >= 20,
        "matrix should document known partial parity areas: {counts:?}"
    );
    assert!(
        counts.get(&ApiStatus::Unsupported).copied().unwrap_or(0) >= 10,
        "matrix should document deliberate unsupported C-only surfaces: {counts:?}"
    );
}

#[test]
fn public_constants_match_vendored_c_header_values() {
    let Some(headers) = read_vendored_public_headers() else {
        return;
    };
    let c_constants = exported_c_integer_constants(&joined_header_contents(&headers));
    let rust_constant_names = public_constant_matrix()
        .iter()
        .map(|(name, _)| *name)
        .collect::<BTreeSet<_>>();
    let root_exports = crate_root_exports();
    let root_exported_c_constants_missing_from_matrix = c_constants
        .keys()
        .filter(|name| root_exports.contains(name.as_str()))
        .filter(|name| !rust_constant_names.contains(name.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    assert!(
        root_exported_c_constants_missing_from_matrix.is_empty(),
        "root-exported C integer constants missing from public Rust constant matrix: {root_exported_c_constants_missing_from_matrix:?}"
    );

    let missing_rust_constants = required_public_c_integer_constants()
        .iter()
        .filter(|name| c_constants.contains_key(**name))
        .filter(|name| !rust_constant_names.contains(**name))
        .copied()
        .collect::<Vec<_>>();
    assert!(
        missing_rust_constants.is_empty(),
        "stable public C constants missing from public Rust constant matrix: {missing_rust_constants:?}"
    );

    let missing = public_constant_matrix()
        .iter()
        .filter(|(name, _)| !c_constants.contains_key(*name))
        .map(|(name, _)| *name)
        .collect::<Vec<_>>();
    assert!(
        missing.is_empty(),
        "public Rust constants not found in vendored blosc2.h or b2nd.h: {missing:?}"
    );

    let mismatches = public_constant_matrix()
        .iter()
        .filter_map(|(name, rust_value)| {
            let c_value = c_constants.get(*name)?;
            (c_value != rust_value).then_some((*name, *rust_value, *c_value))
        })
        .collect::<Vec<_>>();
    assert!(
        mismatches.is_empty(),
        "public Rust constants with values differing from vendored blosc2.h or b2nd.h: {mismatches:?}"
    );
}

#[test]
fn public_b2nd_string_constants_match_vendored_header_values() {
    let Some(headers) = read_vendored_public_headers() else {
        return;
    };
    let c_strings = exported_c_string_constants(&joined_header_contents(&headers));

    assert_eq!(
        c_strings.get("B2ND_DEFAULT_DTYPE").map(String::as_str),
        Some(B2ND_DEFAULT_DTYPE)
    );
    assert_eq!(
        c_strings.get("BLOSC2_VERSION_STRING").map(String::as_str),
        Some(BLOSC2_VERSION_STRING)
    );
    assert_eq!(
        c_strings.get("BLOSC2_VERSION_DATE").map(String::as_str),
        Some(BLOSC2_VERSION_DATE)
    );
    assert_eq!(
        c_strings.get("BLOSC_VERSION_STRING").map(String::as_str),
        Some(BLOSC_VERSION_STRING)
    );
    assert_eq!(
        c_strings.get("BLOSC_VERSION_DATE").map(String::as_str),
        Some(BLOSC_VERSION_DATE)
    );
    assert_eq!(
        c_strings.get("BLOSC_BLOSCLZ_COMPNAME").map(String::as_str),
        Some(BLOSC_BLOSCLZ_COMPNAME)
    );
    assert_eq!(
        c_strings.get("BLOSC_LZ4_COMPNAME").map(String::as_str),
        Some(BLOSC_LZ4_COMPNAME)
    );
    assert_eq!(
        c_strings.get("BLOSC_LZ4HC_COMPNAME").map(String::as_str),
        Some(BLOSC_LZ4HC_COMPNAME)
    );
    assert_eq!(
        c_strings.get("BLOSC_ZLIB_COMPNAME").map(String::as_str),
        Some(BLOSC_ZLIB_COMPNAME)
    );
    assert_eq!(
        c_strings.get("BLOSC_ZSTD_COMPNAME").map(String::as_str),
        Some(BLOSC_ZSTD_COMPNAME)
    );
    assert_eq!(
        c_strings.get("BLOSC_BLOSCLZ_LIBNAME").map(String::as_str),
        Some(BLOSC_BLOSCLZ_LIBNAME)
    );
    assert_eq!(
        c_strings.get("BLOSC_LZ4_LIBNAME").map(String::as_str),
        Some(BLOSC_LZ4_LIBNAME)
    );
    assert_eq!(
        c_strings.get("BLOSC_ZLIB_LIBNAME").map(String::as_str),
        Some(BLOSC_ZLIB_LIBNAME)
    );
    assert_eq!(
        c_strings.get("BLOSC_ZSTD_LIBNAME").map(String::as_str),
        Some(BLOSC_ZSTD_LIBNAME)
    );
    assert_eq!(B2ND_DEFAULT_DTYPE_FORMAT, DTYPE_NUMPY_FORMAT);
    assert_eq!(B2ND_METALAYER_NAME, "b2nd");
}

fn required_c_style_root_exports() -> &'static [&'static str] {
    &[
        "blosc_init",
        "blosc_destroy",
        "blosc_free_resources",
        "blosc_compress",
        "blosc_decompress",
        "blosc_getitem",
        "blosc_get_nthreads",
        "blosc_set_nthreads",
        "blosc_get_compressor",
        "blosc_set_compressor",
        "blosc_set_compressor_c",
        "blosc_get_blocksize",
        "blosc_set_blocksize",
        "blosc_set_splitmode",
        "blosc_compcode_to_compname",
        "blosc_compname_to_compcode",
        "blosc_list_compressors",
        "blosc_get_version_string",
        "blosc_get_complib_info",
        "blosc_cbuffer_sizes",
        "blosc_cbuffer_validate",
        "blosc_cbuffer_metainfo",
        "blosc_cbuffer_versions",
        "blosc_cbuffer_complib",
        "blosc1_compress_c",
        "blosc1_decompress_c",
        "blosc1_set_compressor_c",
        "blosc2_compcode_to_compname_c",
        "blosc2_compcode_to_compname_int_c",
        "blosc2_compname_to_compcode_c",
        "blosc2_create_cctx_c",
        "blosc2_create_dctx_c",
        "blosc2_free_ctx_c",
        "blosc2_getitem_c",
        "blosc2_getitem_ctx_c",
        "blosc2_vlcompress_ctx_c",
        "blosc2_vldecompress_ctx_c",
        "blosc2_vlchunk_get_nblocks_c",
        "blosc2_vldecompress_block_ctx_c",
        "blosc2_chunk_zeros_c",
        "blosc2_chunk_nans_c",
        "blosc2_chunk_repeatval_c",
        "blosc2_chunk_uninit_c",
        "blosc2_cbuffer_metainfo2_c",
        "blosc2_register_codec_c",
        "register_blosc2_filter_c",
        "blosc2_schunk_new_c",
        "blosc2_schunk_free_c",
        "blosc2_schunk_from_buffer_c",
        "blosc2_schunk_from_buffer_owned_c",
        "blosc2_schunk_open_c",
        "blosc2_schunk_open_offset_c",
        "blosc2_schunk_open_lazy_c",
        "blosc2_schunk_open_lazy_offset_c",
        "blosc2_schunk_copy_c",
        "blosc2_schunk_append_chunk_c",
        "blosc2_schunk_update_chunk_c",
        "blosc2_schunk_insert_chunk_c",
        "blosc2_schunk_append_buffer_c",
        "blosc2_schunk_insert_buffer_c",
        "blosc2_schunk_update_buffer_c",
        "blosc2_schunk_append_vlblocks_c",
        "blosc2_schunk_insert_vlblocks_c",
        "blosc2_schunk_update_vlblocks_c",
        "blosc2_schunk_get_lazychunk_c",
        "blosc2_schunk_decompress_chunk_c",
        "blosc2_schunk_decompress_vlblock_c",
        "blosc2_schunk_get_cparams_c",
        "blosc2_schunk_get_dparams_c",
        "blosc2_schunk_get_slice_nchunks_c",
        "blosc2_schunk_get_slice_buffer_c",
        "blosc2_schunk_get_slice_buffer_size_c",
        "blosc2_schunk_set_slice_buffer_c",
        "blosc2_schunk_set_slice_buffer_size_c",
        "blosc2_meta_add_c",
        "blosc2_meta_update_c",
        "blosc2_vlmeta_add_c",
        "blosc2_vlmeta_update_c",
        "b2nd_create_ctx_c",
        "b2nd_create_ctx_parts_c",
        "b2nd_create_ctx_with_storage_c",
        "b2nd_create_ctx_parts_with_storage_c",
        "b2nd_free_ctx_c",
        "b2nd_uninit_c",
        "b2nd_uninit_ctx_c",
        "b2nd_empty_c",
        "b2nd_empty_ctx_c",
        "b2nd_zeros_c",
        "b2nd_zeros_ctx_c",
        "b2nd_nans_c",
        "b2nd_nans_ctx_c",
        "b2nd_full_c",
        "b2nd_full_ctx_c",
        "b2nd_free_c",
        "b2nd_free_option_c",
        "b2nd_from_schunk_c",
        "b2nd_to_cframe_c",
        "b2nd_from_cframe_c",
        "b2nd_open_c",
        "b2nd_open_offset_c",
        "b2nd_from_cbuffer_c",
        "b2nd_from_cbuffer_ctx_c",
        "b2nd_to_cbuffer_c",
        "b2nd_get_slice_c",
        "b2nd_get_slice_ctx_c",
        "b2nd_get_slice_cbuffer_c",
        "b2nd_set_slice_cbuffer_c",
        "b2nd_copy_c",
        "b2nd_copy_ctx_c",
        "b2nd_concatenate_c",
        "b2nd_concatenate_axis_c",
        "b2nd_concatenate_ctx_c",
        "b2nd_concatenate_ctx_axis_c",
        "b2nd_print_meta_c",
        "b2nd_print_meta_to_buffer_c",
        "b2nd_resize_c",
        "b2nd_insert_c",
        "b2nd_insert_axis_c",
        "b2nd_append_c",
        "b2nd_append_axis_c",
        "b2nd_delete_c",
        "b2nd_delete_axis_c",
        "b2nd_get_orthogonal_selection_c",
        "b2nd_get_orthogonal_selection_c_sizes_c",
        "b2nd_get_orthogonal_selection_count_c",
        "b2nd_get_orthogonal_selection_cbuffer_c",
        "b2nd_set_orthogonal_selection_c",
        "b2nd_set_orthogonal_selection_c_sizes_c",
        "b2nd_set_orthogonal_selection_count_c",
        "b2nd_set_orthogonal_selection_cbuffer_c",
        "b2nd_serialize_meta_c",
        "b2nd_serialize_meta_parts_c",
        "b2nd_deserialize_meta_c",
        "b2nd_squeeze_index_c",
        "b2nd_squeeze_c",
        "b2nd_expand_dims_c",
        "b2nd_expand_dims_final_c",
    ]
}

fn read_vendored_public_headers() -> Option<Vec<(&'static str, String)>> {
    let mut headers = Vec::new();
    for relative_path in [
        "include/blosc2.h",
        "include/b2nd.h",
        "include/blosc2/blosc2-common.h",
        "include/blosc2/blosc2-export.h",
        "include/blosc2/blosc2-stdio.h",
        "include/blosc2/codecs-registry.h",
        "include/blosc2/filters-registry.h",
        "include/blosc2/tuners-registry.h",
    ] {
        headers.push((relative_path, read_optional_vendored_header(relative_path)?));
    }
    Some(headers)
}

fn joined_header_contents(headers: &[(&'static str, String)]) -> String {
    headers
        .iter()
        .map(|(_, header)| header.as_str())
        .collect::<Vec<_>>()
        .join("\n")
}

fn c_blosc2_source_dir() -> PathBuf {
    std::env::var_os("BLOSC2_C_SOURCE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("c-blosc2"))
}

fn read_optional_vendored_header(relative_path: &str) -> Option<String> {
    let path = c_blosc2_source_dir().join(relative_path);
    match fs::read_to_string(&path) {
        Ok(header) => Some(header),
        Err(err) if err.kind() == io::ErrorKind::NotFound => {
            eprintln!(
                "skipping C header parity check because {} is absent",
                path.display()
            );
            None
        }
        Err(err) => panic!("failed to read C header {}: {err}", path.display()),
    }
}

fn public_constant_matrix() -> &'static [(&'static str, i64)] {
    &[
        ("BLOSC2_VERSION_MAJOR", BLOSC2_VERSION_MAJOR as i64),
        ("BLOSC2_VERSION_MINOR", BLOSC2_VERSION_MINOR as i64),
        ("BLOSC2_VERSION_RELEASE", BLOSC2_VERSION_RELEASE as i64),
        ("BLOSC_VERSION_MAJOR", BLOSC_VERSION_MAJOR as i64),
        ("BLOSC_VERSION_MINOR", BLOSC_VERSION_MINOR as i64),
        ("BLOSC_VERSION_RELEASE", BLOSC_VERSION_RELEASE as i64),
        (
            "BLOSC1_VERSION_FORMAT_PRE1",
            BLOSC1_VERSION_FORMAT_PRE1 as i64,
        ),
        ("BLOSC1_VERSION_FORMAT", BLOSC1_VERSION_FORMAT as i64),
        (
            "BLOSC2_VERSION_FORMAT_ALPHA",
            BLOSC2_VERSION_FORMAT_ALPHA as i64,
        ),
        (
            "BLOSC2_VERSION_FORMAT_BETA1",
            BLOSC2_VERSION_FORMAT_BETA1 as i64,
        ),
        (
            "BLOSC2_VERSION_FORMAT_STABLE",
            BLOSC2_VERSION_FORMAT_STABLE as i64,
        ),
        (
            "BLOSC2_VERSION_FORMAT_VL_BLOCKS",
            BLOSC2_VERSION_FORMAT_VL_BLOCKS as i64,
        ),
        ("BLOSC2_VERSION_FORMAT", BLOSC2_VERSION_FORMAT as i64),
        (
            "BLOSC2_VERSION_FRAME_FORMAT_BETA2",
            BLOSC2_VERSION_FRAME_FORMAT_BETA2 as i64,
        ),
        (
            "BLOSC2_VERSION_FRAME_FORMAT_RC1",
            BLOSC2_VERSION_FRAME_FORMAT_RC1 as i64,
        ),
        (
            "BLOSC2_VERSION_FRAME_FORMAT_VL_BLOCKS",
            BLOSC2_VERSION_FRAME_FORMAT_VL_BLOCKS as i64,
        ),
        (
            "BLOSC2_VERSION_FRAME_FORMAT",
            BLOSC2_VERSION_FRAME_FORMAT as i64,
        ),
        ("BLOSC_MIN_HEADER_LENGTH", BLOSC_MIN_HEADER_LENGTH as i64),
        (
            "BLOSC_EXTENDED_HEADER_LENGTH",
            BLOSC_EXTENDED_HEADER_LENGTH as i64,
        ),
        ("BLOSC2_MAX_OVERHEAD", BLOSC2_MAX_OVERHEAD as i64),
        ("BLOSC_MAX_OVERHEAD", BLOSC_MAX_OVERHEAD as i64),
        ("BLOSC2_MAX_BUFFERSIZE", BLOSC2_MAX_BUFFERSIZE as i64),
        ("BLOSC_MAX_BUFFERSIZE", BLOSC_MAX_BUFFERSIZE as i64),
        ("BLOSC_MAX_TYPESIZE", BLOSC_MAX_TYPESIZE as i64),
        ("BLOSC_MIN_BUFFERSIZE", BLOSC_MIN_BUFFERSIZE as i64),
        (
            "BLOSC2_DEFINED_TUNER_START",
            BLOSC2_DEFINED_TUNER_START as i64,
        ),
        (
            "BLOSC2_DEFINED_TUNER_STOP",
            BLOSC2_DEFINED_TUNER_STOP as i64,
        ),
        (
            "BLOSC2_GLOBAL_REGISTERED_TUNER_START",
            BLOSC2_GLOBAL_REGISTERED_TUNER_START as i64,
        ),
        (
            "BLOSC2_GLOBAL_REGISTERED_TUNER_STOP",
            BLOSC2_GLOBAL_REGISTERED_TUNER_STOP as i64,
        ),
        (
            "BLOSC2_GLOBAL_REGISTERED_TUNERS",
            BLOSC2_GLOBAL_REGISTERED_TUNERS as i64,
        ),
        (
            "BLOSC2_USER_REGISTERED_TUNER_START",
            BLOSC2_USER_REGISTERED_TUNER_START as i64,
        ),
        (
            "BLOSC2_USER_REGISTERED_TUNER_STOP",
            BLOSC2_USER_REGISTERED_TUNER_STOP as i64,
        ),
        ("BLOSC_STUNE", BLOSC_STUNE as i64),
        ("BLOSC_BTUNE", BLOSC_BTUNE as i64),
        ("BLOSC_LAST_TUNER", BLOSC_LAST_TUNER as i64),
        (
            "BLOSC_LAST_REGISTERED_TUNE",
            BLOSC_LAST_REGISTERED_TUNE as i64,
        ),
        ("BLOSC2_ERROR_SUCCESS", BLOSC2_ERROR_SUCCESS as i64),
        ("BLOSC2_ERROR_FAILURE", BLOSC2_ERROR_FAILURE as i64),
        ("BLOSC2_ERROR_STREAM", BLOSC2_ERROR_STREAM as i64),
        ("BLOSC2_ERROR_DATA", BLOSC2_ERROR_DATA as i64),
        (
            "BLOSC2_ERROR_MEMORY_ALLOC",
            BLOSC2_ERROR_MEMORY_ALLOC as i64,
        ),
        ("BLOSC2_ERROR_READ_BUFFER", BLOSC2_ERROR_READ_BUFFER as i64),
        (
            "BLOSC2_ERROR_WRITE_BUFFER",
            BLOSC2_ERROR_WRITE_BUFFER as i64,
        ),
        (
            "BLOSC2_ERROR_CODEC_SUPPORT",
            BLOSC2_ERROR_CODEC_SUPPORT as i64,
        ),
        ("BLOSC2_ERROR_CODEC_PARAM", BLOSC2_ERROR_CODEC_PARAM as i64),
        ("BLOSC2_ERROR_CODEC_DICT", BLOSC2_ERROR_CODEC_DICT as i64),
        (
            "BLOSC2_ERROR_VERSION_SUPPORT",
            BLOSC2_ERROR_VERSION_SUPPORT as i64,
        ),
        (
            "BLOSC2_ERROR_INVALID_HEADER",
            BLOSC2_ERROR_INVALID_HEADER as i64,
        ),
        (
            "BLOSC2_ERROR_INVALID_PARAM",
            BLOSC2_ERROR_INVALID_PARAM as i64,
        ),
        ("BLOSC2_ERROR_FILE_READ", BLOSC2_ERROR_FILE_READ as i64),
        ("BLOSC2_ERROR_FILE_WRITE", BLOSC2_ERROR_FILE_WRITE as i64),
        ("BLOSC2_ERROR_FILE_OPEN", BLOSC2_ERROR_FILE_OPEN as i64),
        ("BLOSC2_ERROR_NOT_FOUND", BLOSC2_ERROR_NOT_FOUND as i64),
        ("BLOSC2_ERROR_RUN_LENGTH", BLOSC2_ERROR_RUN_LENGTH as i64),
        (
            "BLOSC2_ERROR_FILTER_PIPELINE",
            BLOSC2_ERROR_FILTER_PIPELINE as i64,
        ),
        (
            "BLOSC2_ERROR_CHUNK_INSERT",
            BLOSC2_ERROR_CHUNK_INSERT as i64,
        ),
        (
            "BLOSC2_ERROR_CHUNK_APPEND",
            BLOSC2_ERROR_CHUNK_APPEND as i64,
        ),
        (
            "BLOSC2_ERROR_CHUNK_UPDATE",
            BLOSC2_ERROR_CHUNK_UPDATE as i64,
        ),
        ("BLOSC2_ERROR_2GB_LIMIT", BLOSC2_ERROR_2GB_LIMIT as i64),
        ("BLOSC2_ERROR_SCHUNK_COPY", BLOSC2_ERROR_SCHUNK_COPY as i64),
        ("BLOSC2_ERROR_FRAME_TYPE", BLOSC2_ERROR_FRAME_TYPE as i64),
        (
            "BLOSC2_ERROR_FILE_TRUNCATE",
            BLOSC2_ERROR_FILE_TRUNCATE as i64,
        ),
        (
            "BLOSC2_ERROR_THREAD_CREATE",
            BLOSC2_ERROR_THREAD_CREATE as i64,
        ),
        ("BLOSC2_ERROR_POSTFILTER", BLOSC2_ERROR_POSTFILTER as i64),
        (
            "BLOSC2_ERROR_FRAME_SPECIAL",
            BLOSC2_ERROR_FRAME_SPECIAL as i64,
        ),
        (
            "BLOSC2_ERROR_SCHUNK_SPECIAL",
            BLOSC2_ERROR_SCHUNK_SPECIAL as i64,
        ),
        ("BLOSC2_ERROR_PLUGIN_IO", BLOSC2_ERROR_PLUGIN_IO as i64),
        ("BLOSC2_ERROR_FILE_REMOVE", BLOSC2_ERROR_FILE_REMOVE as i64),
        (
            "BLOSC2_ERROR_NULL_POINTER",
            BLOSC2_ERROR_NULL_POINTER as i64,
        ),
        (
            "BLOSC2_ERROR_INVALID_INDEX",
            BLOSC2_ERROR_INVALID_INDEX as i64,
        ),
        (
            "BLOSC2_ERROR_METALAYER_NOT_FOUND",
            BLOSC2_ERROR_METALAYER_NOT_FOUND as i64,
        ),
        (
            "BLOSC2_ERROR_MAX_BUFSIZE_EXCEEDED",
            BLOSC2_ERROR_MAX_BUFSIZE_EXCEEDED as i64,
        ),
        ("BLOSC2_ERROR_TUNER", BLOSC2_ERROR_TUNER as i64),
        ("BLOSC2_IO_FILESYSTEM", BLOSC2_IO_FILESYSTEM as i64),
        (
            "BLOSC2_IO_FILESYSTEM_MMAP",
            BLOSC2_IO_FILESYSTEM_MMAP as i64,
        ),
        (
            "BLOSC_IO_LAST_BLOSC_DEFINED",
            BLOSC_IO_LAST_BLOSC_DEFINED as i64,
        ),
        ("BLOSC_IO_LAST_REGISTERED", BLOSC_IO_LAST_REGISTERED as i64),
        ("BLOSC2_IO_BLOSC_DEFINED", BLOSC2_IO_BLOSC_DEFINED as i64),
        ("BLOSC2_IO_REGISTERED", BLOSC2_IO_REGISTERED as i64),
        ("BLOSC2_IO_USER_DEFINED", BLOSC2_IO_USER_DEFINED as i64),
        (
            "BLOSC2_DEFINED_FILTERS_START",
            BLOSC2_DEFINED_FILTERS_START as i64,
        ),
        (
            "BLOSC2_DEFINED_FILTERS_STOP",
            BLOSC2_DEFINED_FILTERS_STOP as i64,
        ),
        (
            "BLOSC2_GLOBAL_REGISTERED_FILTERS_START",
            BLOSC2_GLOBAL_REGISTERED_FILTERS_START as i64,
        ),
        (
            "BLOSC2_GLOBAL_REGISTERED_FILTERS_STOP",
            BLOSC2_GLOBAL_REGISTERED_FILTERS_STOP as i64,
        ),
        (
            "BLOSC2_GLOBAL_REGISTERED_FILTERS",
            BLOSC2_GLOBAL_REGISTERED_FILTERS as i64,
        ),
        (
            "BLOSC2_USER_REGISTERED_FILTERS_START",
            BLOSC2_USER_REGISTERED_FILTERS_START as i64,
        ),
        (
            "BLOSC2_USER_REGISTERED_FILTERS_STOP",
            BLOSC2_USER_REGISTERED_FILTERS_STOP as i64,
        ),
        ("BLOSC2_MAX_FILTERS", BLOSC2_MAX_FILTERS as i64),
        ("BLOSC2_MAX_UDFILTERS", BLOSC2_MAX_UDFILTERS as i64),
        ("BLOSC_NOSHUFFLE", BLOSC_NOSHUFFLE as i64),
        ("BLOSC_NOFILTER", BLOSC_NOFILTER as i64),
        ("BLOSC_SHUFFLE", BLOSC_SHUFFLE as i64),
        ("BLOSC_BITSHUFFLE", BLOSC_BITSHUFFLE as i64),
        ("BLOSC_DELTA", BLOSC_DELTA as i64),
        ("BLOSC_TRUNC_PREC", BLOSC_TRUNC_PREC as i64),
        ("BLOSC_LAST_FILTER", BLOSC_LAST_FILTER as i64),
        (
            "BLOSC_LAST_REGISTERED_FILTER",
            BLOSC_LAST_REGISTERED_FILTER as i64,
        ),
        ("BLOSC_FILTER_NDCELL", BLOSC_FILTER_NDCELL as i64),
        ("BLOSC_FILTER_NDMEAN", BLOSC_FILTER_NDMEAN as i64),
        (
            "BLOSC_FILTER_BYTEDELTA_BUGGY",
            BLOSC_FILTER_BYTEDELTA_BUGGY as i64,
        ),
        ("BLOSC_FILTER_BYTEDELTA", BLOSC_FILTER_BYTEDELTA as i64),
        ("BLOSC_FILTER_INT_TRUNC", BLOSC_FILTER_INT_TRUNC as i64),
        ("BLOSC_DOSHUFFLE", BLOSC_DOSHUFFLE as i64),
        ("BLOSC_MEMCPYED", BLOSC_MEMCPYED as i64),
        ("BLOSC_DOBITSHUFFLE", BLOSC_DOBITSHUFFLE as i64),
        ("BLOSC_DODELTA", BLOSC_DODELTA as i64),
        ("BLOSC2_USEDICT", BLOSC2_USEDICT as i64),
        ("BLOSC2_BIGENDIAN", BLOSC2_BIGENDIAN as i64),
        ("BLOSC2_INSTR_CODEC", BLOSC2_INSTR_CODEC as i64),
        ("BLOSC2_VL_BLOCKS", BLOSC2_VL_BLOCKS as i64),
        ("BLOSC2_MAXDICTSIZE", BLOSC2_MAXDICTSIZE as i64),
        ("BLOSC2_MINUSEFULDICT", BLOSC2_MINUSEFULDICT as i64),
        ("BLOSC2_MAXBLOCKSIZE", BLOSC2_MAXBLOCKSIZE as i64),
        ("BLOSC2_MAXTYPESIZE", BLOSC2_MAXTYPESIZE as i64),
        (
            "BLOSC2_DEFINED_CODECS_START",
            BLOSC2_DEFINED_CODECS_START as i64,
        ),
        (
            "BLOSC2_DEFINED_CODECS_STOP",
            BLOSC2_DEFINED_CODECS_STOP as i64,
        ),
        (
            "BLOSC2_GLOBAL_REGISTERED_CODECS_START",
            BLOSC2_GLOBAL_REGISTERED_CODECS_START as i64,
        ),
        (
            "BLOSC2_GLOBAL_REGISTERED_CODECS_STOP",
            BLOSC2_GLOBAL_REGISTERED_CODECS_STOP as i64,
        ),
        (
            "BLOSC2_GLOBAL_REGISTERED_CODECS",
            BLOSC2_GLOBAL_REGISTERED_CODECS as i64,
        ),
        (
            "BLOSC2_USER_REGISTERED_CODECS_START",
            BLOSC2_USER_REGISTERED_CODECS_START as i64,
        ),
        (
            "BLOSC2_USER_REGISTERED_CODECS_STOP",
            BLOSC2_USER_REGISTERED_CODECS_STOP as i64,
        ),
        ("BLOSC_BLOSCLZ", BLOSC_BLOSCLZ as i64),
        ("BLOSC_LZ4", BLOSC_LZ4 as i64),
        ("BLOSC_LZ4HC", BLOSC_LZ4HC as i64),
        ("BLOSC_ZLIB", BLOSC_ZLIB as i64),
        ("BLOSC_ZSTD", BLOSC_ZSTD as i64),
        ("BLOSC_CODEC_NDLZ", BLOSC_CODEC_NDLZ as i64),
        (
            "BLOSC_CODEC_ZFP_FIXED_ACCURACY",
            BLOSC_CODEC_ZFP_FIXED_ACCURACY as i64,
        ),
        (
            "BLOSC_CODEC_ZFP_FIXED_PRECISION",
            BLOSC_CODEC_ZFP_FIXED_PRECISION as i64,
        ),
        (
            "BLOSC_CODEC_ZFP_FIXED_RATE",
            BLOSC_CODEC_ZFP_FIXED_RATE as i64,
        ),
        ("BLOSC_CODEC_OPENHTJ2K", BLOSC_CODEC_OPENHTJ2K as i64),
        ("BLOSC_CODEC_GROK", BLOSC_CODEC_GROK as i64),
        ("BLOSC_CODEC_OPENZL", BLOSC_CODEC_OPENZL as i64),
        ("BLOSC_LAST_CODEC", BLOSC_LAST_CODEC as i64),
        (
            "BLOSC_LAST_REGISTERED_CODEC",
            BLOSC_LAST_REGISTERED_CODEC as i64,
        ),
        ("BLOSC_BLOSCLZ_LIB", BLOSC_BLOSCLZ_LIB as i64),
        ("BLOSC_LZ4_LIB", BLOSC_LZ4_LIB as i64),
        ("BLOSC_ZLIB_LIB", BLOSC_ZLIB_LIB as i64),
        ("BLOSC_ZSTD_LIB", BLOSC_ZSTD_LIB as i64),
        ("BLOSC_UDCODEC_LIB", BLOSC_UDCODEC_LIB as i64),
        ("BLOSC_SCHUNK_LIB", BLOSC_SCHUNK_LIB as i64),
        ("BLOSC_BLOSCLZ_FORMAT", BLOSC_BLOSCLZ_FORMAT as i64),
        ("BLOSC_LZ4_FORMAT", BLOSC_LZ4_FORMAT as i64),
        ("BLOSC_LZ4HC_FORMAT", BLOSC_LZ4HC_FORMAT as i64),
        ("BLOSC_ZLIB_FORMAT", BLOSC_ZLIB_FORMAT as i64),
        ("BLOSC_ZSTD_FORMAT", BLOSC_ZSTD_FORMAT as i64),
        ("BLOSC_UDCODEC_FORMAT", BLOSC_UDCODEC_FORMAT as i64),
        (
            "BLOSC_BLOSCLZ_VERSION_FORMAT",
            BLOSC_BLOSCLZ_VERSION_FORMAT as i64,
        ),
        ("BLOSC_LZ4_VERSION_FORMAT", BLOSC_LZ4_VERSION_FORMAT as i64),
        (
            "BLOSC_LZ4HC_VERSION_FORMAT",
            BLOSC_LZ4HC_VERSION_FORMAT as i64,
        ),
        (
            "BLOSC_ZLIB_VERSION_FORMAT",
            BLOSC_ZLIB_VERSION_FORMAT as i64,
        ),
        (
            "BLOSC_ZSTD_VERSION_FORMAT",
            BLOSC_ZSTD_VERSION_FORMAT as i64,
        ),
        (
            "BLOSC_UDCODEC_VERSION_FORMAT",
            BLOSC_UDCODEC_VERSION_FORMAT as i64,
        ),
        ("BLOSC_ALWAYS_SPLIT", BLOSC_ALWAYS_SPLIT as i64),
        ("BLOSC_NEVER_SPLIT", BLOSC_NEVER_SPLIT as i64),
        ("BLOSC_AUTO_SPLIT", BLOSC_AUTO_SPLIT as i64),
        (
            "BLOSC_FORWARD_COMPAT_SPLIT",
            BLOSC_FORWARD_COMPAT_SPLIT as i64,
        ),
        ("BLOSC2_NO_SPECIAL", BLOSC2_NO_SPECIAL as i64),
        ("BLOSC2_SPECIAL_ZERO", BLOSC2_SPECIAL_ZERO as i64),
        ("BLOSC2_SPECIAL_NAN", BLOSC2_SPECIAL_NAN as i64),
        ("BLOSC2_SPECIAL_VALUE", BLOSC2_SPECIAL_VALUE as i64),
        ("BLOSC2_SPECIAL_UNINIT", BLOSC2_SPECIAL_UNINIT as i64),
        ("BLOSC2_SPECIAL_LASTID", BLOSC2_SPECIAL_LASTID as i64),
        ("BLOSC2_SPECIAL_MASK", BLOSC2_SPECIAL_MASK as i64),
        ("BLOSC2_MAX_METALAYERS", BLOSC2_MAX_METALAYERS as i64),
        (
            "BLOSC2_METALAYER_NAME_MAXLEN",
            BLOSC2_METALAYER_NAME_MAXLEN as i64,
        ),
        ("BLOSC2_MAX_VLMETALAYERS", BLOSC2_MAX_VLMETALAYERS as i64),
        (
            "BLOSC2_VLMETALAYERS_NAME_MAXLEN",
            BLOSC2_VLMETALAYERS_NAME_MAXLEN as i64,
        ),
        ("BLOSC2_CHUNK_VERSION", BLOSC2_CHUNK_VERSION as i64),
        ("BLOSC2_CHUNK_VERSIONLZ", BLOSC2_CHUNK_VERSIONLZ as i64),
        ("BLOSC2_CHUNK_FLAGS", BLOSC2_CHUNK_FLAGS as i64),
        ("BLOSC2_CHUNK_TYPESIZE", BLOSC2_CHUNK_TYPESIZE as i64),
        ("BLOSC2_CHUNK_NBYTES", BLOSC2_CHUNK_NBYTES as i64),
        ("BLOSC2_CHUNK_BLOCKSIZE", BLOSC2_CHUNK_BLOCKSIZE as i64),
        ("BLOSC2_CHUNK_CBYTES", BLOSC2_CHUNK_CBYTES as i64),
        (
            "BLOSC2_CHUNK_FILTER_CODES",
            BLOSC2_CHUNK_FILTER_CODES as i64,
        ),
        ("BLOSC2_CHUNK_FILTER_META", BLOSC2_CHUNK_FILTER_META as i64),
        (
            "BLOSC2_CHUNK_BLOSC2_FLAGS2",
            BLOSC2_CHUNK_BLOSC2_FLAGS2 as i64,
        ),
        (
            "BLOSC2_CHUNK_BLOSC2_FLAGS",
            BLOSC2_CHUNK_BLOSC2_FLAGS as i64,
        ),
        ("BLOSC2_MAX_DIM", BLOSC2_MAX_DIM as i64),
        ("B2ND_METALAYER_VERSION", B2ND_METALAYER_VERSION as i64),
        ("B2ND_MAX_DIM", B2ND_MAX_DIM as i64),
        ("B2ND_MAX_METALAYERS", B2ND_MAX_METALAYERS as i64),
        ("DTYPE_NUMPY_FORMAT", DTYPE_NUMPY_FORMAT as i64),
        (
            "B2ND_DEFAULT_DTYPE_FORMAT",
            B2ND_DEFAULT_DTYPE_FORMAT as i64,
        ),
    ]
}

fn required_public_c_integer_constants() -> &'static [&'static str] {
    &[
        "BLOSC2_VERSION_FORMAT_BETA1",
        "BLOSC2_VERSION_MAJOR",
        "BLOSC2_VERSION_MINOR",
        "BLOSC2_VERSION_RELEASE",
        "BLOSC2_VERSION_FRAME_FORMAT_BETA2",
        "BLOSC2_VERSION_FRAME_FORMAT_RC1",
        "BLOSC2_VERSION_FRAME_FORMAT_VL_BLOCKS",
        "BLOSC2_VERSION_FRAME_FORMAT",
        "BLOSC2_METALAYER_NAME_MAXLEN",
        "BLOSC2_MAX_VLMETALAYERS",
        "BLOSC2_VLMETALAYERS_NAME_MAXLEN",
        "BLOSC_SCHUNK_LIB",
        "BLOSC_BTUNE",
        "B2ND_DEFAULT_DTYPE_FORMAT",
    ]
}

fn exported_c_functions(header: &str) -> Vec<String> {
    let without_comments = strip_c_comments(header);
    let lines = without_comments.lines().collect::<Vec<_>>();
    let mut names = Vec::new();
    let mut i = 0;

    while i < lines.len() {
        let line = lines[i].trim();
        if line.starts_with('#') {
            i += 1;
            continue;
        }
        let export = line.contains("BLOSC_EXPORT");
        let inline = line.starts_with("static inline");
        if export || inline {
            let mut decl = line.to_owned();
            while !decl.contains('(') && i + 1 < lines.len() {
                i += 1;
                decl.push(' ');
                decl.push_str(lines[i].trim());
            }
            if let Some(name) = function_name_before_paren(&decl) {
                if !matches!(name, "if" | "__attribute__" | "__declspec")
                    && (export || name.starts_with("blosc") || name.starts_with("b2nd"))
                {
                    names.push(name.to_owned());
                }
            }
        }
        i += 1;
    }

    names
}

fn exported_c_integer_constants(header: &str) -> BTreeMap<String, i64> {
    let without_comments = strip_c_comments(header);
    let mut constants = BTreeMap::from([
        ("INT_MAX".to_string(), i32::MAX as i64),
        ("UINT8_MAX".to_string(), u8::MAX as i64),
    ]);
    let mut defines = Vec::new();
    let mut in_enum = false;
    let mut next_enum_value = 0i64;

    for raw_line in without_comments.lines() {
        let line = raw_line.trim();
        if let Some(rest) = line.strip_prefix("#define ") {
            if let Some((name, expr)) = rest.split_once(char::is_whitespace) {
                if is_constant_name(name) {
                    defines.push((name.to_string(), expr.trim().to_string()));
                    if let Some(value) = eval_c_int_expr(expr.trim(), &constants) {
                        constants.insert(name.to_string(), value);
                    }
                }
            }
        }

        if line.starts_with("enum") {
            in_enum = true;
            next_enum_value = 0;
            continue;
        }
        if !in_enum {
            continue;
        }
        if line.starts_with("};") {
            in_enum = false;
            continue;
        }

        let item = line.trim_end_matches(',').trim();
        let Some(name) = item
            .split(|ch: char| !(ch == '_' || ch.is_ascii_alphanumeric()))
            .next()
            .filter(|name| is_constant_name(name))
        else {
            continue;
        };
        let value = if let Some((_, expr)) = item.split_once('=') {
            eval_c_int_expr(expr, &constants)
                .unwrap_or_else(|| panic!("could not evaluate C constant expression: {expr}"))
        } else {
            next_enum_value
        };
        constants.insert(name.to_string(), value);
        next_enum_value = value + 1;
    }

    resolve_integer_defines(&defines, &mut constants);
    constants
}

fn exported_c_string_constants(header: &str) -> BTreeMap<String, String> {
    let without_comments = strip_c_comments(header);
    let mut constants = BTreeMap::new();
    let mut defines = Vec::new();
    for raw_line in without_comments.lines() {
        let line = raw_line.trim();
        let Some(rest) = line.strip_prefix("#define ") else {
            continue;
        };
        let Some((name, expr)) = rest.split_once(char::is_whitespace) else {
            continue;
        };
        if !is_constant_name(name) {
            continue;
        }
        let expr = expr.trim();
        defines.push((name.to_string(), expr.to_string()));
        if let Some(value) = expr
            .strip_prefix('"')
            .and_then(|rest| rest.split_once('"').map(|(value, _)| value))
        {
            constants.insert(name.to_string(), value.to_string());
        }
    }
    resolve_string_defines(&defines, &mut constants);
    constants
}

fn resolve_integer_defines(defines: &[(String, String)], constants: &mut BTreeMap<String, i64>) {
    let mut changed = true;
    while changed {
        changed = false;
        for (name, expr) in defines {
            if constants.contains_key(name) {
                continue;
            }
            if let Some(value) = eval_c_int_expr(expr, constants) {
                constants.insert(name.clone(), value);
                changed = true;
            }
        }
    }
}

fn resolve_string_defines(defines: &[(String, String)], constants: &mut BTreeMap<String, String>) {
    let mut changed = true;
    while changed {
        changed = false;
        for (name, expr) in defines {
            if constants.contains_key(name) {
                continue;
            }
            if let Some(value) = constants.get(expr.trim()).cloned() {
                constants.insert(name.clone(), value);
                changed = true;
            }
        }
    }
}

fn is_constant_name(name: &str) -> bool {
    (name.starts_with("BLOSC") || name.starts_with("B2ND") || name.starts_with("DTYPE"))
        && name
            .chars()
            .all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
}

fn eval_c_int_expr(expr: &str, constants: &BTreeMap<String, i64>) -> Option<i64> {
    let tokens = expr
        .split(|ch: char| {
            ch.is_whitespace() || matches!(ch, '(' | ')' | '+' | '-' | '*' | ',' | ';')
        })
        .filter(|token| !token.is_empty())
        .collect::<Vec<_>>();
    let operators = expr
        .chars()
        .filter(|ch| matches!(ch, '+' | '-' | '*'))
        .collect::<Vec<_>>();
    let mut values = tokens
        .into_iter()
        .map(|token| {
            if let Some(hex) = token.strip_prefix("0x") {
                i64::from_str_radix(hex, 16).ok()
            } else if token.chars().all(|ch| ch.is_ascii_digit()) {
                token.parse::<i64>().ok()
            } else {
                constants.get(token).copied()
            }
        })
        .collect::<Option<Vec<_>>>()?;
    if values.is_empty() {
        return None;
    }
    if operators.len() == 1
        && operators[0] == '-'
        && expr.trim_start().starts_with('-')
        && values.len() == 1
    {
        return Some(-values[0]);
    }
    if operators.is_empty() {
        return Some(values[0]);
    }

    let mut ops = operators;
    let mut idx = 0;
    while idx < ops.len() {
        if ops[idx] == '*' {
            values[idx] *= values.remove(idx + 1);
            ops.remove(idx);
        } else {
            idx += 1;
        }
    }
    let mut value = values[0];
    for (op, rhs) in ops.into_iter().zip(values.into_iter().skip(1)) {
        match op {
            '+' => value += rhs,
            '-' => value -= rhs,
            _ => unreachable!(),
        }
    }
    Some(value)
}

fn function_name_before_paren(decl: &str) -> Option<&str> {
    let before_paren = decl.split_once('(')?.0.trim_end();
    let end = before_paren.len();
    let start = before_paren[..end]
        .rfind(|ch: char| !(ch == '_' || ch.is_ascii_alphanumeric()))
        .map_or(0, |idx| idx + 1);
    (start < end).then_some(&before_paren[start..end])
}

fn strip_c_comments(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars().peekable();
    while let Some(ch) = chars.next() {
        match (ch, chars.peek().copied()) {
            ('/', Some('*')) => {
                chars.next();
                while let Some(inner) = chars.next() {
                    if inner == '*' && chars.peek() == Some(&'/') {
                        chars.next();
                        break;
                    }
                }
                out.push(' ');
            }
            ('/', Some('/')) => {
                chars.next();
                for inner in chars.by_ref() {
                    if inner == '\n' {
                        out.push('\n');
                        break;
                    }
                }
            }
            _ => out.push(ch),
        }
    }
    out
}

fn crate_root_exports() -> BTreeSet<String> {
    let mut exports = BTreeSet::new();
    let mut in_pub_use = false;
    for line in strip_c_comments(LIB_RS).lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("pub use ") {
            in_pub_use = true;
        }
        if !in_pub_use {
            continue;
        }
        for token in trimmed
            .split(|ch: char| !(ch == '_' || ch.is_ascii_alphanumeric()))
            .filter(|token| !token.is_empty())
        {
            if !matches!(token, "pub" | "use" | "self" | "crate" | "super") {
                exports.insert(token.to_string());
            }
        }
        if trimmed.ends_with(';') {
            in_pub_use = false;
        }
    }
    exports
}
