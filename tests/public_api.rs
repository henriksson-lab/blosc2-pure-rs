//! Public API parity checklist against the vendored C-Blosc2 headers.
//!
//! This is intentionally not an extern "C" ABI test. The crate exposes
//! Rust-shaped functions and C-name-style adapters, so this matrix tracks the
//! practical parity surface: whether each public C header function has a
//! maintained Rust equivalent, is only partially equivalent, or is deliberately
//! unsupported/out of scope for now.

use std::collections::{BTreeMap, BTreeSet};

const BLOSC2_H: &str = include_str!("../c-blosc2/include/blosc2.h");
const B2ND_H: &str = include_str!("../c-blosc2/include/b2nd.h");
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
    covered!("blosc2_schunk_open"),
    covered!("blosc2_schunk_open_offset"),
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
    covered!("b2nd_to_cframe"),
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
    covered!("b2nd_concatenate"),
    covered!("b2nd_print_meta"),
    covered!("b2nd_resize"),
    covered!("b2nd_insert"),
    covered!("b2nd_append"),
    covered!("b2nd_delete"),
    covered!("b2nd_get_orthogonal_selection"),
    covered!("b2nd_set_orthogonal_selection"),
    covered!("b2nd_serialize_meta"),
    covered!("b2nd_deserialize_meta"),
    covered!("b2nd_copy_buffer"),
    covered!("b2nd_copy_buffer2"),
    partial!("blosc2_register_codec", "Rust registry exists, but not the C callback ABI."),
    partial!("blosc2_register_filter", "Rust registry exists, but callbacks are Rust-shaped and not ABI-compatible."),
    partial!("blosc2_schunk_new" => "blosc2_schunk_new_c", "Rust helper uses owned storage parameters, not blosc2_storage ABI."),
    partial!("blosc2_schunk_copy", "Rust copies supported data but not all C storage/plugin semantics."),
    partial!("blosc2_schunk_append_chunk", "Implemented, but exact C precompressed chunk validation/storage semantics remain incomplete."),
    partial!("blosc2_schunk_update_chunk", "Implemented, but attached-frame persistence/precompressed semantics remain incomplete."),
    partial!("blosc2_schunk_insert_chunk", "Implemented, but attached-frame persistence/precompressed semantics remain incomplete."),
    partial!("blosc2_schunk_delete_chunk", "Implemented, but attached-frame persistence remains incomplete."),
    covered!("blosc2_schunk_get_lazychunk"),
    partial!("blosc2_schunk_reorder_offsets", "In-memory reorder works; attached-frame persistence remains incomplete."),
    partial!("blosc2_meta_exists", "In-memory/frame metadata lookup is covered; C inline pointer ABI is not modeled."),
    partial!("blosc2_meta_add", "Metadata is supported; attached-frame flush semantics are incomplete."),
    partial!("blosc2_meta_update", "Metadata is supported; attached-frame flush semantics are incomplete."),
    partial!("blosc2_meta_get", "In-memory/frame metadata lookup is covered; C inline pointer ABI is not modeled."),
    partial!("blosc2_vlmeta_exists", "VL metadata is supported; attached-frame flush semantics are incomplete."),
    partial!("blosc2_vlmeta_add", "VL metadata is supported; attached-frame flush semantics are incomplete."),
    partial!("blosc2_vlmeta_update", "VL metadata is supported; attached-frame flush semantics are incomplete."),
    partial!("blosc2_vlmeta_get", "VL metadata lookup is covered; C ownership ABI is not modeled."),
    partial!("blosc2_vlmeta_delete", "VL metadata is supported; attached-frame flush semantics are incomplete."),
    partial!("blosc2_vlmeta_get_names", "VL metadata name listing is Rust-shaped, not C allocation ABI."),
    partial!("b2nd_create_ctx", "B2ndContext exists, but blosc2_storage/urlpath behavior is not fully modeled."),
    partial!("b2nd_free_ctx" => "b2nd_free_ctx_c", "Rust owned context teardown has no C allocation ABI."),
    partial!("b2nd_uninit", "Array creation exists, but storage/urlpath behavior is not fully modeled."),
    partial!("b2nd_empty", "Array creation exists, but storage/urlpath behavior is not fully modeled."),
    partial!("b2nd_zeros", "Array creation exists, but storage/urlpath behavior is not fully modeled."),
    partial!("b2nd_nans", "Array creation exists, but storage/urlpath behavior is not fully modeled."),
    partial!("b2nd_full", "Array creation exists, but storage/urlpath behavior is not fully modeled."),
    partial!("b2nd_from_cframe", "copy=false zero-copy/view semantics are explicitly rejected."),
    covered!("b2nd_squeeze_index"),
    covered!("b2nd_squeeze"),
    covered!("b2nd_expand_dims"),
    unsupported!("blosc2_set_threads_callback", "C thread-pool callback ABI is not implemented."),
    unsupported!("blosc2_register_io_cb", "User-defined IO callback ABI is not implemented."),
    unsupported!("blosc2_get_io_cb", "User-defined IO callback registry is not implemented."),
    unsupported!("blosc2_register_tuner", "C tuner callback ABI is not implemented."),
    unsupported!("blosc2_set_maskout", "Context maskout support is not implemented."),
    unsupported!("blosc2_get_blosc2_storage_defaults", "blosc2_storage is not modeled yet."),
    unsupported!("blosc2_get_blosc2_io_defaults", "blosc2_io is not modeled yet."),
    unsupported!("blosc2_get_blosc2_stdio_mmap_defaults", "stdio mmap storage is not modeled."),
    unsupported!("blosc2_schunk_avoid_cframe_free", "C frame ownership toggle is not relevant to owned Rust buffers."),
    unsupported!("blosc2_schunk_open_udio", "User-defined IO opening is not implemented."),
    unsupported!("blosc2_schunk_open_offset_udio", "User-defined IO opening is not implemented."),
    out_of_scope!("blosc_set_timestamp", "Benchmark timing helper, not a storage/compression API target."),
    out_of_scope!("blosc_elapsed_nsecs", "Benchmark timing helper, not a storage/compression API target."),
    out_of_scope!("blosc_elapsed_secs", "Benchmark timing helper, not a storage/compression API target."),
    covered!("blosc1_get_blocksize"),
    covered!("blosc1_set_blocksize"),
    covered!("blosc1_set_splitmode"),
];

#[test]
fn public_api_matrix_covers_all_vendored_header_exports() {
    let header_exports = exported_c_functions(BLOSC2_H)
        .into_iter()
        .chain(exported_c_functions(B2ND_H))
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

fn exported_c_functions(header: &str) -> Vec<String> {
    let without_comments = strip_c_comments(header);
    let lines = without_comments.lines().collect::<Vec<_>>();
    let mut names = Vec::new();
    let mut i = 0;

    while i < lines.len() {
        let line = lines[i].trim();
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
                if export || name.starts_with("blosc") || name.starts_with("b2nd") {
                    names.push(name.to_owned());
                }
            }
        }
        i += 1;
    }

    names
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

fn crate_root_exports() -> BTreeSet<&'static str> {
    let mut exports = BTreeSet::new();
    for row in API_MATRIX {
        if let Some(symbol) = row.rust_symbol {
            if LIB_RS.contains(symbol) {
                exports.insert(symbol);
            }
        }
    }
    exports
}
