#![cfg(feature = "_ffi")]
use blosc2_pure_rs::compress::{CParams, DParams};
use blosc2_pure_rs::constants::*;
use blosc2_pure_rs::{codecs, compress};
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicUsize, Ordering};
use std::sync::Mutex;
mod common;
use common::ffi;

static USER_CODEC_LAST_CLEVEL: AtomicUsize = AtomicUsize::new(usize::MAX);
static USER_CODEC_LAST_META: AtomicUsize = AtomicUsize::new(usize::MAX);
static RAW_CODEC_LAST_COMP_CODE: AtomicUsize = AtomicUsize::new(usize::MAX);
static RAW_CODEC_LAST_COMP_META: AtomicUsize = AtomicUsize::new(usize::MAX);
static RAW_CODEC_LAST_CLEVEL: AtomicUsize = AtomicUsize::new(usize::MAX);
static RAW_CODEC_LAST_META: AtomicUsize = AtomicUsize::new(usize::MAX);
static RAW_CODEC_LAST_TYPESIZE: AtomicUsize = AtomicUsize::new(usize::MAX);
static RAW_CODEC_LAST_USE_DICT: AtomicI32 = AtomicI32::new(i32::MIN);
static RAW_CODEC_LAST_BLOCKSIZE: AtomicI32 = AtomicI32::new(i32::MIN);
static RAW_CODEC_LAST_SPLITMODE: AtomicI32 = AtomicI32::new(i32::MIN);
static RAW_CODEC_LAST_FILTER: AtomicUsize = AtomicUsize::new(usize::MAX);
static RAW_CODEC_LAST_PREPARAMS: AtomicUsize = AtomicUsize::new(usize::MAX);
static RAW_CODEC_LAST_POSTPARAMS: AtomicUsize = AtomicUsize::new(usize::MAX);
static RAW_CODEC_LAST_INSTR_CODEC: AtomicBool = AtomicBool::new(false);
static RAW_CODEC_LAST_CODEC_PARAMS: AtomicUsize = AtomicUsize::new(usize::MAX);
static RAW_CODEC_LAST_CPARAMS_SCHUNK: AtomicUsize = AtomicUsize::new(usize::MAX);
static RAW_CODEC_LAST_DPARAMS_SCHUNK: AtomicUsize = AtomicUsize::new(usize::MAX);
static RAW_CODEC_LAST_ENCODER_INPUT_PTR: AtomicUsize = AtomicUsize::new(usize::MAX);
static RAW_CODEC_LAST_ENCODER_CHUNK_PTR: AtomicUsize = AtomicUsize::new(usize::MAX);
static RAW_CODEC_LAST_DECODER_INPUT_PTR: AtomicUsize = AtomicUsize::new(usize::MAX);
static RAW_CODEC_LAST_DECODER_CHUNK_PTR: AtomicUsize = AtomicUsize::new(usize::MAX);
static RAW_CODEC_LAST_ENCODER_INPUT_LEN: AtomicI32 = AtomicI32::new(i32::MIN);
static RAW_CODEC_LAST_ENCODER_OUTPUT_LEN: AtomicI32 = AtomicI32::new(i32::MIN);
static RAW_CODEC_LAST_DECODER_INPUT_LEN: AtomicI32 = AtomicI32::new(i32::MIN);
static RAW_CODEC_LAST_DECODER_OUTPUT_LEN: AtomicI32 = AtomicI32::new(i32::MIN);
static CODEC_REGISTRY_TEST_LOCK: Mutex<()> = Mutex::new(());

fn user_codec_encoder(clevel: u8, meta: u8, src: &[u8], dest: &mut [u8]) -> i32 {
    if dest.len() < src.len() {
        return BLOSC2_ERROR_WRITE_BUFFER;
    }
    USER_CODEC_LAST_CLEVEL.store(clevel as usize, Ordering::SeqCst);
    USER_CODEC_LAST_META.store(meta as usize, Ordering::SeqCst);
    dest[..src.len()].copy_from_slice(src);
    src.len() as i32
}

fn user_codec_decoder(meta: u8, src: &[u8], dest: &mut [u8]) -> i32 {
    if dest.len() < src.len() {
        return BLOSC2_ERROR_DATA;
    }
    USER_CODEC_LAST_META.store(meta as usize, Ordering::SeqCst);
    dest[..src.len()].copy_from_slice(src);
    src.len() as i32
}

unsafe extern "C" fn raw_codec_encoder(
    input: *const u8,
    input_len: i32,
    output: *mut u8,
    output_len: i32,
    meta: u8,
    cparams: *mut codecs::Blosc2CParams,
    chunk: *const std::ffi::c_void,
) -> i32 {
    if input.is_null() || output.is_null() || cparams.is_null() || input_len < 0 {
        return BLOSC2_ERROR_INVALID_PARAM;
    }
    if output_len < input_len {
        return BLOSC2_ERROR_WRITE_BUFFER;
    }
    RAW_CODEC_LAST_ENCODER_INPUT_PTR.store(input as usize, Ordering::SeqCst);
    RAW_CODEC_LAST_ENCODER_CHUNK_PTR.store(chunk as usize, Ordering::SeqCst);
    RAW_CODEC_LAST_ENCODER_INPUT_LEN.store(input_len, Ordering::SeqCst);
    RAW_CODEC_LAST_ENCODER_OUTPUT_LEN.store(output_len, Ordering::SeqCst);
    RAW_CODEC_LAST_COMP_CODE.store((*cparams).compcode as usize, Ordering::SeqCst);
    RAW_CODEC_LAST_COMP_META.store((*cparams).compcode_meta as usize, Ordering::SeqCst);
    RAW_CODEC_LAST_CLEVEL.store((*cparams).clevel as usize, Ordering::SeqCst);
    RAW_CODEC_LAST_META.store(meta as usize, Ordering::SeqCst);
    RAW_CODEC_LAST_TYPESIZE.store((*cparams).typesize as usize, Ordering::SeqCst);
    RAW_CODEC_LAST_USE_DICT.store((*cparams).use_dict, Ordering::SeqCst);
    RAW_CODEC_LAST_BLOCKSIZE.store((*cparams).blocksize, Ordering::SeqCst);
    RAW_CODEC_LAST_SPLITMODE.store((*cparams).splitmode, Ordering::SeqCst);
    RAW_CODEC_LAST_FILTER.store(
        (*cparams).filters[BLOSC2_MAX_FILTERS - 1] as usize,
        Ordering::SeqCst,
    );
    RAW_CODEC_LAST_PREPARAMS.store((*cparams).preparams as usize, Ordering::SeqCst);
    RAW_CODEC_LAST_INSTR_CODEC.store((*cparams).instr_codec, Ordering::SeqCst);
    RAW_CODEC_LAST_CODEC_PARAMS.store((*cparams).codec_params as usize, Ordering::SeqCst);
    RAW_CODEC_LAST_CPARAMS_SCHUNK.store((*cparams).schunk as usize, Ordering::SeqCst);
    if input_len >= 2 && output_len >= 3 {
        let first = *input;
        let second = *input.add(1);
        *output = first;
        *output.add(1) = second.wrapping_sub(first);
        *output.add(2) = meta;
        return 3;
    }
    std::ptr::copy_nonoverlapping(input, output, input_len as usize);
    input_len
}

unsafe extern "C" fn raw_codec_decoder(
    input: *const u8,
    input_len: i32,
    output: *mut u8,
    output_len: i32,
    meta: u8,
    dparams: *mut codecs::Blosc2DParams,
    chunk: *const std::ffi::c_void,
) -> i32 {
    if input.is_null() || output.is_null() || dparams.is_null() || input_len < 0 {
        return BLOSC2_ERROR_INVALID_PARAM;
    }
    if output_len < 0 {
        return BLOSC2_ERROR_DATA;
    }
    RAW_CODEC_LAST_DECODER_INPUT_PTR.store(input as usize, Ordering::SeqCst);
    RAW_CODEC_LAST_DECODER_CHUNK_PTR.store(chunk as usize, Ordering::SeqCst);
    RAW_CODEC_LAST_DECODER_INPUT_LEN.store(input_len, Ordering::SeqCst);
    RAW_CODEC_LAST_DECODER_OUTPUT_LEN.store(output_len, Ordering::SeqCst);
    RAW_CODEC_LAST_META.store(meta as usize, Ordering::SeqCst);
    RAW_CODEC_LAST_TYPESIZE.store((*dparams).typesize as usize, Ordering::SeqCst);
    RAW_CODEC_LAST_POSTPARAMS.store((*dparams).postparams as usize, Ordering::SeqCst);
    RAW_CODEC_LAST_DPARAMS_SCHUNK.store((*dparams).schunk as usize, Ordering::SeqCst);
    if input_len == 3 && *input.add(2) == meta {
        let first = *input;
        let step = *input.add(1);
        for idx in 0..output_len as usize {
            *output.add(idx) = first.wrapping_add(step.wrapping_mul(idx as u8));
        }
        return output_len;
    }
    std::ptr::copy_nonoverlapping(input, output, input_len as usize);
    input_len
}

fn raw_codec_reset_snapshots() {
    RAW_CODEC_LAST_COMP_CODE.store(usize::MAX, Ordering::SeqCst);
    RAW_CODEC_LAST_COMP_META.store(usize::MAX, Ordering::SeqCst);
    RAW_CODEC_LAST_CLEVEL.store(usize::MAX, Ordering::SeqCst);
    RAW_CODEC_LAST_META.store(usize::MAX, Ordering::SeqCst);
    RAW_CODEC_LAST_TYPESIZE.store(usize::MAX, Ordering::SeqCst);
    RAW_CODEC_LAST_USE_DICT.store(i32::MIN, Ordering::SeqCst);
    RAW_CODEC_LAST_BLOCKSIZE.store(i32::MIN, Ordering::SeqCst);
    RAW_CODEC_LAST_SPLITMODE.store(i32::MIN, Ordering::SeqCst);
    RAW_CODEC_LAST_FILTER.store(usize::MAX, Ordering::SeqCst);
    RAW_CODEC_LAST_PREPARAMS.store(usize::MAX, Ordering::SeqCst);
    RAW_CODEC_LAST_POSTPARAMS.store(usize::MAX, Ordering::SeqCst);
    RAW_CODEC_LAST_INSTR_CODEC.store(false, Ordering::SeqCst);
    RAW_CODEC_LAST_CODEC_PARAMS.store(usize::MAX, Ordering::SeqCst);
    RAW_CODEC_LAST_CPARAMS_SCHUNK.store(usize::MAX, Ordering::SeqCst);
    RAW_CODEC_LAST_DPARAMS_SCHUNK.store(usize::MAX, Ordering::SeqCst);
    RAW_CODEC_LAST_ENCODER_INPUT_PTR.store(usize::MAX, Ordering::SeqCst);
    RAW_CODEC_LAST_ENCODER_CHUNK_PTR.store(usize::MAX, Ordering::SeqCst);
    RAW_CODEC_LAST_DECODER_INPUT_PTR.store(usize::MAX, Ordering::SeqCst);
    RAW_CODEC_LAST_DECODER_CHUNK_PTR.store(usize::MAX, Ordering::SeqCst);
    RAW_CODEC_LAST_ENCODER_INPUT_LEN.store(i32::MIN, Ordering::SeqCst);
    RAW_CODEC_LAST_ENCODER_OUTPUT_LEN.store(i32::MIN, Ordering::SeqCst);
    RAW_CODEC_LAST_DECODER_INPUT_LEN.store(i32::MIN, Ordering::SeqCst);
    RAW_CODEC_LAST_DECODER_OUTPUT_LEN.store(i32::MIN, Ordering::SeqCst);
}

fn expected_effective_blocksize(cparams: &CParams, nbytes: usize) -> i32 {
    if (nbytes as i32) < cparams.typesize {
        return 1;
    }
    if cparams.blocksize > 0 {
        let mut blocksize = cparams.blocksize.min(nbytes as i32);
        if blocksize > cparams.typesize {
            blocksize = blocksize / cparams.typesize * cparams.typesize;
        }
        blocksize
    } else {
        nbytes as i32
    }
}

fn first_unused_codec_id(start: u8) -> u8 {
    (start..=u8::MAX)
        .find(|&candidate| !codecs::is_registered_codec(candidate))
        .expect("test needs an available codec ID")
}

fn static_test_name(prefix: &str, code: u8) -> &'static str {
    Box::leak(format!("{prefix}-{code}").into_boxed_str())
}

fn static_c_test_name(prefix: &str, code: u8) -> &'static [u8] {
    Box::leak(format!("{prefix}-{code}\0").into_bytes().into_boxed_slice())
}

fn init_blosc2() -> common::Blosc2 {
    common::Blosc2::new()
}

fn dictionary_fixture_data() -> Vec<u8> {
    (0..96_000u32)
        .flat_map(|i| {
            let value = (i % 4096) ^ ((i / 17) % 251);
            value.to_le_bytes()
        })
        .collect()
}

fn c_compress_chunk(data: &[u8], compcode: u8, use_dict: bool) -> Vec<u8> {
    let mut compressed = vec![0u8; data.len() + BLOSC2_MAXDICTSIZE + BLOSC2_MAX_OVERHEAD + 512];
    let csize = c_compress_status(data, compcode, use_dict, &mut compressed);
    assert!(
        csize > 0,
        "C compression failed for codec={compcode} use_dict={use_dict}: {csize}"
    );
    compressed.truncate(csize as usize);
    compressed
}

fn c_compress_status(data: &[u8], compcode: u8, use_dict: bool, dest: &mut [u8]) -> i32 {
    unsafe {
        let mut cparams: ffi::blosc2_cparams = std::mem::zeroed();
        cparams.compcode = compcode;
        cparams.clevel = 5;
        cparams.use_dict = i32::from(use_dict);
        cparams.typesize = 4;
        cparams.nthreads = 1;
        cparams.blocksize = 4096;
        cparams.splitmode = BLOSC_NEVER_SPLIT;
        cparams.filters = [0, 0, 0, 0, 0, BLOSC_SHUFFLE];

        let cctx = ffi::blosc2_create_cctx(cparams);
        assert!(
            !cctx.is_null(),
            "C context creation failed for codec={compcode}"
        );
        let result = ffi::blosc2_compress_ctx(
            cctx,
            data.as_ptr() as *const _,
            data.len() as i32,
            dest.as_mut_ptr() as *mut _,
            dest.len() as i32,
        );
        ffi::blosc2_free_ctx(cctx);
        result
    }
}

fn c_decompress_chunk(chunk: &[u8], nbytes: usize, context: &str) -> Vec<u8> {
    let mut decoded = vec![0u8; nbytes];
    let dsize = unsafe {
        let mut dparams: ffi::blosc2_dparams = std::mem::zeroed();
        dparams.nthreads = 1;
        dparams.typesize = 4;
        let dctx = ffi::blosc2_create_dctx(dparams);
        assert!(!dctx.is_null(), "C dctx creation failed for {context}");
        let result = ffi::blosc2_decompress_ctx(
            dctx,
            chunk.as_ptr() as *const _,
            chunk.len() as i32,
            decoded.as_mut_ptr() as *mut _,
            decoded.len() as i32,
        );
        ffi::blosc2_free_ctx(dctx);
        result
    };
    assert_eq!(
        dsize, nbytes as i32,
        "C decompression size mismatch for {context}: got {dsize}"
    );
    decoded
}

fn assert_dictionary_chunk(chunk: &[u8], context: &str) {
    assert!(
        chunk.len() > BLOSC2_CHUNK_BLOSC2_FLAGS,
        "{context} chunk must include the extended header"
    );
    assert_ne!(
        chunk[BLOSC2_CHUNK_BLOSC2_FLAGS] & BLOSC2_USEDICT,
        0,
        "{context} must carry the C usedict chunk flag"
    );
}

fn assert_udcodec_header(chunk: &[u8], compcode: u8, compcode_meta: u8, context: &str) {
    assert!(
        chunk.len() > BLOSC2_CHUNK_BLOSC2_FLAGS,
        "{context} chunk must include the extended header"
    );
    assert_eq!(
        (chunk[BLOSC2_CHUNK_FLAGS] >> 5) & 0x07,
        BLOSC_UDCODEC_FORMAT,
        "{context} must use C's UDCODEC chunk format"
    );
    assert_eq!(
        chunk[BLOSC2_CHUNK_UDCOMPCODE], compcode,
        "{context} must store the real codec ID in udcompcode"
    );
    assert_eq!(
        chunk[BLOSC2_CHUNK_COMPCODE_META], compcode_meta,
        "{context} must preserve compcode_meta in the extended header"
    );
}

#[test]
fn test_ndlz_c_plugin_decode_fixtures_for_supported_meta_values() {
    let _b = init_blosc2();

    for (meta, rows, cols, payload) in [
        (4, 3i32, 4i32, (1..=12).collect::<Vec<_>>()),
        (8, 2i32, 3i32, vec![20, 21, 22, 23, 24, 25]),
    ] {
        let mut encoded = vec![2];
        encoded.extend_from_slice(&rows.to_le_bytes());
        encoded.extend_from_slice(&cols.to_le_bytes());
        encoded.push(0);
        encoded.extend_from_slice(&payload);

        let mut decoded = vec![0; payload.len()];
        assert_eq!(
            codecs::decompress_block_with_meta(BLOSC_CODEC_NDLZ, meta, &encoded, &mut decoded),
            payload.len() as i32,
            "NDLZ meta={meta} literal fixture must decode"
        );
        assert_eq!(
            decoded, payload,
            "NDLZ meta={meta} literal payload mismatch"
        );
    }
}

#[test]
fn test_ndlz_c_plugin_repeat_and_reject_status_fixtures() {
    let _b = init_blosc2();

    let mut repeat_cell = vec![2];
    repeat_cell.extend_from_slice(&4i32.to_le_bytes());
    repeat_cell.extend_from_slice(&4i32.to_le_bytes());
    repeat_cell.push(0x40);
    repeat_cell.push(7);

    let mut decoded = vec![0; 16];
    assert_eq!(
        codecs::decompress_block_with_meta(BLOSC_CODEC_NDLZ, 4, &repeat_cell, &mut decoded),
        decoded.len() as i32
    );
    assert_eq!(decoded, vec![7; 16]);

    assert_eq!(
        codecs::decompress_block_with_meta(BLOSC_CODEC_NDLZ, 5, &repeat_cell, &mut decoded),
        -1,
        "NDLZ only supports C plugin cell metadata 4 and 8"
    );

    let cparams = CParams {
        compcode: BLOSC_CODEC_NDLZ,
        compcode_meta: 4,
        typesize: 4,
        filters: [0; BLOSC2_MAX_FILTERS],
        ..Default::default()
    };
    assert!(compress::blosc2_create_cctx(cparams.clone()).is_ok());
    assert_eq!(
        compress::compress(&(0..128u8).collect::<Vec<_>>(), &cparams).unwrap_err(),
        "Codec compression failed"
    );
}

#[test]
fn test_ndlz_rust_encoder_emits_c_wire_full_cell_match_fixtures() {
    let _b = init_blosc2();

    for (meta, rows, cols, cell_shape, expected_cbytes) in [
        (4u8, 4i32, 8i32, 4usize, 29i32),
        (8u8, 8i32, 16i32, 8usize, 77i32),
    ] {
        let first_cell: Vec<u8> = (0..cell_shape * cell_shape)
            .map(|i| ((i * 5 + 3) % 251) as u8)
            .collect();
        let mut input = Vec::with_capacity((rows * cols) as usize);
        for row in 0..cell_shape {
            input.extend_from_slice(&first_cell[row * cell_shape..row * cell_shape + cell_shape]);
            input.extend_from_slice(&first_cell[row * cell_shape..row * cell_shape + cell_shape]);
        }

        let mut encoded = vec![0; input.len() + 64];
        let cbytes = codecs::ndlz_compress_block_2d(meta, [rows, cols], &input, &mut encoded);
        assert_eq!(cbytes, expected_cbytes);
        let match_token_pos = 9 + 1 + cell_shape * cell_shape;
        assert_eq!(encoded[match_token_pos], 0xc0);
        assert_eq!(
            u16::from_le_bytes([encoded[match_token_pos + 1], encoded[match_token_pos + 2]]),
            (cell_shape * cell_shape) as u16
        );

        let mut decoded = vec![0; input.len()];
        assert_eq!(
            codecs::decompress_block_with_meta(
                BLOSC_CODEC_NDLZ,
                meta,
                &encoded[..cbytes as usize],
                &mut decoded,
            ),
            input.len() as i32,
            "Rust-emitted NDLZ full-cell match fixture must decode for meta={meta}"
        );
        assert_eq!(decoded, input);
    }
}

#[test]
fn test_ndlz_c_plugin_sentinel_error_fixtures() {
    let _b = init_blosc2();
    let mut decoded = vec![0u8; 16];

    for (meta, encoded, expected, context) in [
        (4, vec![], 0, "empty input returns C fallback sentinel"),
        (
            8,
            vec![2, 0, 0, 0, 0, 0, 0],
            0,
            "short 8x8 header returns C fallback sentinel",
        ),
        (
            4,
            vec![3, 0, 0, 0, 0, 0, 0, 0, 0],
            -1,
            "4x4 header with unsupported ndim returns C reject sentinel",
        ),
        (
            5,
            vec![2, 4, 0, 0, 0, 4, 0, 0, 0, 0],
            -1,
            "unsupported metadata returns C reject sentinel",
        ),
    ] {
        assert_eq!(
            codecs::decompress_block_with_meta(BLOSC_CODEC_NDLZ, meta, &encoded, &mut decoded),
            expected,
            "{context}"
        );
    }

    let input = vec![1u8; 16];
    let mut encoded = vec![0u8; 64];
    assert_eq!(
        codecs::ndlz_compress_block_2d(5, [4, 4], &input, &mut encoded),
        -1,
        "unsupported NDLZ encoder metadata must mirror C's reject sentinel"
    );
    assert_eq!(
        codecs::ndlz_compress_block_2d(4, [3, 4], &input[..12], &mut encoded),
        0,
        "non-cell-aligned NDLZ blocks must mirror C's fallback sentinel"
    );
}

#[test]
fn test_zfp_c_plugin_modes_are_explicitly_unsupported_until_codec_abi_exists() {
    let _b = init_blosc2();

    for (compcode, compcode_meta) in [
        (BLOSC_CODEC_ZFP_FIXED_ACCURACY, (-1i8) as u8),
        (BLOSC_CODEC_ZFP_FIXED_PRECISION, 25),
        (BLOSC_CODEC_ZFP_FIXED_RATE, 45),
    ] {
        let cparams = CParams {
            compcode,
            compcode_meta,
            typesize: 4,
            filters: [0; BLOSC2_MAX_FILTERS],
            splitmode: BLOSC_NEVER_SPLIT,
            ..Default::default()
        };

        assert!(compress::blosc2_create_cctx(cparams.clone()).is_ok());
        assert_eq!(
            compress::compress(&[0u8; 128], &cparams).unwrap_err(),
            "ZFP plugin codecs are not supported",
            "ZFP compcode={compcode} compression must fail before writing data"
        );
    }
}

#[test]
fn test_dictionary_codecs_c_compress_rust_and_c_decompress_parity() {
    let _b = init_blosc2();
    let data = dictionary_fixture_data();

    for compcode in [BLOSC_LZ4, BLOSC_LZ4HC, BLOSC_ZSTD] {
        let c_chunk = c_compress_chunk(&data, compcode, true);
        assert_dictionary_chunk(&c_chunk, &format!("C codec={compcode}"));

        let rust_decoded = compress::decompress(&c_chunk)
            .unwrap_or_else(|e| panic!("Rust failed to decode C dict codec={compcode}: {e}"));
        let c_decoded =
            c_decompress_chunk(&c_chunk, data.len(), &format!("C dict codec={compcode}"));

        assert_eq!(
            rust_decoded, c_decoded,
            "Rust and C decode results differ for C dict codec={compcode}"
        );
        assert_eq!(
            rust_decoded, data,
            "C→Rust dict mismatch for codec={compcode}"
        );
    }
}

#[test]
fn test_dictionary_codecs_rust_compress_c_and_rust_decompress_parity() {
    let _b = init_blosc2();
    let data = dictionary_fixture_data();

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
        let rust_chunk = compress::compress(&data, &cparams)
            .unwrap_or_else(|e| panic!("Rust dict compress failed for codec={compcode}: {e}"));
        assert_dictionary_chunk(&rust_chunk, &format!("Rust codec={compcode}"));

        let rust_decoded = compress::decompress(&rust_chunk)
            .unwrap_or_else(|e| panic!("Rust failed to decode Rust dict codec={compcode}: {e}"));
        let c_decoded = c_decompress_chunk(
            &rust_chunk,
            data.len(),
            &format!("Rust dict codec={compcode}"),
        );

        assert_eq!(
            rust_decoded, c_decoded,
            "Rust and C decode results differ for Rust dict codec={compcode}"
        );
        assert_eq!(c_decoded, data, "Rust→C dict mismatch for codec={compcode}");
    }
}

#[test]
fn test_dictionary_request_on_unsupported_codec_matches_c_reject_status() {
    let _b = init_blosc2();
    let data = dictionary_fixture_data();

    for compcode in [BLOSC_BLOSCLZ, BLOSC_ZLIB] {
        let mut c_dest = vec![0u8; data.len() + BLOSC2_MAXDICTSIZE + BLOSC2_MAX_OVERHEAD + 512];
        assert_eq!(
            c_compress_status(&data, compcode, true, &mut c_dest),
            BLOSC2_ERROR_CODEC_PARAM,
            "C rejects dictionary compression for unsupported codec={compcode}"
        );

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
        assert_eq!(
            compress::compress(&data, &cparams).unwrap_err(),
            "Dictionary compression is only supported for Zstd, LZ4, and LZ4HC",
            "Rust must mirror C's dictionary rejection before writing unsupported codec={compcode}"
        );
    }
}

#[test]
fn test_public_user_codec_registration_matches_c_range_and_name_lookup() {
    let _b = init_blosc2();
    let _lock = CODEC_REGISTRY_TEST_LOCK.lock().unwrap();

    let plugin_range_codec = codecs::Blosc2Codec {
        compcode: BLOSC2_USER_DEFINED_CODECS_START - 1,
        compname: "public-plugin-range-rejected",
        complib: BLOSC2_USER_DEFINED_CODECS_START - 1,
        version: 1,
        encoder: user_codec_encoder,
        decoder: user_codec_decoder,
    };
    assert_eq!(
        codecs::blosc2_register_codec(&plugin_range_codec),
        BLOSC2_ERROR_CODEC_PARAM,
        "C public blosc2_register_codec rejects plugin/global IDs below the user range"
    );

    let user_code = first_unused_codec_id(BLOSC2_USER_DEFINED_CODECS_START);
    let user_name = static_test_name("public-user-codec", user_code);
    let user_codec = codecs::Blosc2Codec {
        compcode: user_code,
        compname: user_name,
        complib: user_code,
        version: 7,
        encoder: user_codec_encoder,
        decoder: user_codec_decoder,
    };
    assert_eq!(
        codecs::blosc2_register_codec(&user_codec),
        BLOSC2_ERROR_SUCCESS,
        "C public blosc2_register_codec accepts user-defined IDs"
    );
    assert_eq!(
        codecs::blosc2_register_codec(&user_codec),
        BLOSC2_ERROR_SUCCESS,
        "C treats re-registering the same ID/name as idempotent"
    );
    assert_eq!(
        compress::blosc2_compcode_to_compname_c(user_code),
        (i32::from(user_code), Some(user_name))
    );
    assert_eq!(
        compress::blosc2_compname_to_compcode_c(user_name),
        i32::from(user_code)
    );

    let renamed = codecs::Blosc2Codec {
        compname: static_test_name("public-user-codec-renamed", user_code),
        ..user_codec
    };
    assert_eq!(
        codecs::blosc2_register_codec(&renamed),
        BLOSC2_ERROR_CODEC_PARAM,
        "C rejects duplicate codec IDs registered under a different name"
    );

    assert_eq!(
        codecs::register_named_codec(
            first_unused_codec_id(BLOSC2_USER_DEFINED_CODECS_START),
            "",
            user_codec_encoder,
            user_codec_decoder,
        ),
        Err("User-defined codec name cannot be empty"),
        "Rust ergonomic user-codec registration rejects empty names"
    );

    let previous_empty_name_lookup = compress::blosc2_compname_to_compcode_c("");
    let empty_name_code = first_unused_codec_id(BLOSC2_USER_DEFINED_CODECS_START);
    let empty_name_codec = codecs::Blosc2Codec {
        compcode: empty_name_code,
        compname: "",
        complib: empty_name_code,
        version: 1,
        encoder: user_codec_encoder,
        decoder: user_codec_decoder,
    };
    assert_eq!(
        codecs::blosc2_register_codec(&empty_name_codec),
        BLOSC2_ERROR_SUCCESS,
        "C public blosc2_register_codec accepts an empty codec name"
    );
    assert_eq!(
        codecs::blosc2_register_codec(&empty_name_codec),
        BLOSC2_ERROR_SUCCESS,
        "empty-name public codec registration is idempotent by ID/name"
    );
    assert_eq!(
        compress::blosc2_compcode_to_compname_c(empty_name_code),
        (i32::from(empty_name_code), Some(""))
    );
    let expected_empty_name_lookup = if previous_empty_name_lookup >= 0 {
        previous_empty_name_lookup
    } else {
        i32::from(empty_name_code)
    };
    assert_eq!(
        compress::blosc2_compname_to_compcode_c(""),
        expected_empty_name_lookup,
        "C name lookup returns the first registered codec for duplicate empty names"
    );
}

#[test]
fn test_private_registered_codec_range_and_lookup_match_c_private_path() {
    let _b = init_blosc2();
    let _lock = CODEC_REGISTRY_TEST_LOCK.lock().unwrap();

    assert_eq!(
        codecs::register_private_codec_with_metadata(
            BLOSC2_GLOBAL_REGISTERED_CODECS_START - 1,
            "private-builtin-range-rejected",
            BLOSC2_GLOBAL_REGISTERED_CODECS_START - 1,
            1,
            user_codec_encoder,
            user_codec_decoder,
        ),
        Err("Private codec IDs must be >= 32"),
        "C private register_codec_private rejects built-in codec IDs"
    );

    let private_code = first_unused_codec_id(BLOSC2_GLOBAL_REGISTERED_CODECS_START);
    let private_name = static_test_name("private-plugin-codec", private_code);
    assert!(
        codecs::register_private_codec_with_metadata(
            private_code,
            private_name,
            private_code,
            3,
            user_codec_encoder,
            user_codec_decoder,
        )
        .is_ok(),
        "C private register_codec_private accepts plugin/global-range IDs"
    );
    assert_eq!(
        codecs::registered_codec_name(private_code),
        Some(private_name)
    );
    assert_eq!(codecs::registered_codec_version(private_code), Some(3));
    assert_eq!(
        codecs::registered_codec_complib_info(private_name),
        Some((private_code, private_name, "unknown"))
    );
    assert_eq!(compcode_to_compformat(private_code), BLOSC_UDCODEC_FORMAT);
    assert_eq!(
        compcode_to_version(private_code),
        BLOSC_UDCODEC_VERSION_FORMAT
    );
}

#[test]
fn test_global_plugin_codec_registry_descriptors_match_c_registry() {
    let _b = init_blosc2();
    let _lock = CODEC_REGISTRY_TEST_LOCK.lock().unwrap();

    assert_eq!(BLOSC2_GLOBAL_REGISTERED_CODECS_START, 32);
    assert_eq!(BLOSC2_GLOBAL_REGISTERED_CODECS_STOP, 159);
    assert_eq!(
        BLOSC_LAST_REGISTERED_CODEC,
        BLOSC_CODEC_OPENHTJ2K,
        "C's public header derives LAST_REGISTERED from the exposed count, even though codecs-registry.c lists newer plugin IDs too"
    );

    for (code, name) in [
        (BLOSC_CODEC_NDLZ, "ndlz"),
        (BLOSC_CODEC_ZFP_FIXED_ACCURACY, "zfp_acc"),
        (BLOSC_CODEC_ZFP_FIXED_PRECISION, "zfp_prec"),
        (BLOSC_CODEC_ZFP_FIXED_RATE, "zfp_rate"),
        (BLOSC_CODEC_OPENHTJ2K, "openhtj2k"),
        (BLOSC_CODEC_GROK, "grok"),
        (BLOSC_CODEC_OPENZL, "openzl"),
    ] {
        assert_eq!(
            compress::blosc2_compcode_to_compname_c(code),
            (i32::from(code), Some(name)),
            "registered global codec compcode/name lookup must mirror codecs-registry.c"
        );
        assert_eq!(
            compress::blosc2_compname_to_compcode_c(name),
            i32::from(code),
            "registered global codec name/compcode lookup must mirror codecs-registry.c"
        );
        assert_eq!(codecs::registered_codec_name(code), Some(name));
        assert_eq!(codecs::registered_codec_version(code), Some(1));
        assert_eq!(
            codecs::registered_codec_complib_info(name),
            Some((code, name, "unknown")),
            "global plugin complib/version metadata mirrors C's registered blosc2_codec descriptors"
        );
        assert_eq!(compcode_to_compformat(code), BLOSC_UDCODEC_FORMAT);
        assert_eq!(compcode_to_version(code), BLOSC_UDCODEC_VERSION_FORMAT);
    }

    let first_unregistered_global = BLOSC_CODEC_OPENZL + 1;
    assert_eq!(
        compress::blosc2_compcode_to_compname_c(first_unregistered_global),
        (i32::from(first_unregistered_global), None),
        "C returns the numeric code for unknown IDs >= BLOSC_LAST_CODEC but leaves the name NULL"
    );
    assert_eq!(
        compress::blosc2_compname_to_compcode_c("not-a-c-blosc2-codec"),
        -1
    );

    assert!(
        codecs::register_private_codec_with_metadata(
            BLOSC_CODEC_NDLZ,
            "ndlz",
            BLOSC_CODEC_NDLZ,
            1,
            user_codec_encoder,
            user_codec_decoder,
        )
        .is_ok(),
        "C private register_codec_private treats re-registering the same global plugin ID/name as idempotent"
    );
    assert_eq!(
        codecs::register_private_codec_with_metadata(
            BLOSC_CODEC_NDLZ,
            "ndlz-renamed",
            BLOSC_CODEC_NDLZ,
            1,
            user_codec_encoder,
            user_codec_decoder,
        ),
        Err("Private codec ID already registered"),
        "C private register_codec_private rejects a known global plugin ID under a different name"
    );
}

#[test]
fn test_user_codec_meta_and_udcodec_header_match_c_registered_codec_wire_rules() {
    let _b = init_blosc2();
    const META: u8 = 23;
    let _lock = CODEC_REGISTRY_TEST_LOCK.lock().unwrap();

    let code = first_unused_codec_id(BLOSC2_USER_DEFINED_CODECS_START);
    let name = static_test_name("user-codec-meta", code);
    codecs::register_named_codec(code, name, user_codec_encoder, user_codec_decoder)
        .expect("user codec registration must succeed");

    let data: Vec<u8> = (0..512u32).flat_map(|i| i.to_le_bytes()).collect();
    let mut encoded = vec![0u8; data.len()];
    USER_CODEC_LAST_CLEVEL.store(usize::MAX, Ordering::SeqCst);
    USER_CODEC_LAST_META.store(usize::MAX, Ordering::SeqCst);
    assert_eq!(
        codecs::compress_block_with_meta(code, 7, META, &data, &mut encoded),
        data.len() as i32,
        "registered user codec block compression must run with C-style meta"
    );
    assert_eq!(USER_CODEC_LAST_CLEVEL.load(Ordering::SeqCst), 7);
    assert_eq!(USER_CODEC_LAST_META.load(Ordering::SeqCst), META as usize);

    let mut decoded = vec![0u8; data.len()];
    USER_CODEC_LAST_META.store(usize::MAX, Ordering::SeqCst);
    assert_eq!(
        codecs::decompress_block_with_meta(code, META, &encoded, &mut decoded),
        data.len() as i32,
        "registered user codec block decompression must receive C-style meta"
    );
    assert_eq!(decoded, data);
    assert_eq!(USER_CODEC_LAST_META.load(Ordering::SeqCst), META as usize);

    let cparams = CParams {
        compcode: code,
        compcode_meta: META,
        clevel: 7,
        typesize: 4,
        blocksize: data.len() as i32,
        splitmode: BLOSC_NEVER_SPLIT,
        filters: [0; BLOSC2_MAX_FILTERS],
        ..Default::default()
    };
    let chunk = compress::compress(&data, &cparams)
        .expect("registered user codec chunk compression must succeed");
    assert_udcodec_header(&chunk, code, META, "registered user codec");
    assert_eq!(
        compress::decompress(&chunk).expect("registered user codec chunk must decode"),
        data
    );
}

#[test]
fn test_raw_c_abi_codec_registration_forwards_codec_params_like_c() {
    let _b = init_blosc2();
    const META: u8 = 41;
    let _lock = CODEC_REGISTRY_TEST_LOCK.lock().unwrap();

    let plugin_range_raw_codec = codecs::Blosc2CodecAbi {
        compcode: BLOSC2_USER_DEFINED_CODECS_START - 1,
        compname: b"raw-c-abi-plugin-range-rejected\0".as_ptr().cast(),
        complib: BLOSC2_USER_DEFINED_CODECS_START - 1,
        version: 1,
        encoder: Some(raw_codec_encoder),
        decoder: Some(raw_codec_decoder),
    };
    assert_eq!(
        codecs::blosc2_register_codec_abi(&plugin_range_raw_codec),
        BLOSC2_ERROR_CODEC_PARAM,
        "raw C-shaped public registration rejects plugin/global IDs below the user range"
    );

    let code = first_unused_codec_id(BLOSC2_USER_DEFINED_CODECS_START);
    let raw_name = static_c_test_name("raw-c-abi-codec", code);
    let raw_codec = codecs::Blosc2CodecAbi {
        compcode: code,
        compname: raw_name.as_ptr().cast(),
        complib: code,
        version: 5,
        encoder: Some(raw_codec_encoder),
        decoder: Some(raw_codec_decoder),
    };
    assert_eq!(
        codecs::blosc2_register_codec_abi(&raw_codec),
        BLOSC2_ERROR_SUCCESS,
        "raw C-shaped blosc2_codec descriptors use the public user-codec range"
    );
    assert_eq!(
        codecs::blosc2_register_codec_abi(&raw_codec),
        BLOSC2_ERROR_SUCCESS,
        "raw C-shaped registration is idempotent for the same ID/name"
    );
    let raw_name_str = std::str::from_utf8(&raw_name[..raw_name.len() - 1]).unwrap();
    assert_eq!(
        compress::blosc2_compcode_to_compname_c(code),
        (i32::from(code), Some(raw_name_str))
    );
    assert_eq!(
        compress::blosc2_compname_to_compcode_c(raw_name_str),
        i32::from(code)
    );

    let renamed_raw_name = static_c_test_name("raw-c-abi-codec-renamed", code);
    let renamed_raw_codec = codecs::Blosc2CodecAbi {
        compname: renamed_raw_name.as_ptr().cast(),
        ..raw_codec
    };
    assert_eq!(
        codecs::blosc2_register_codec_abi(&renamed_raw_codec),
        BLOSC2_ERROR_CODEC_PARAM,
        "raw C-shaped registration rejects duplicate IDs under a different name"
    );

    let duplicate_name_code = first_unused_codec_id(BLOSC2_USER_DEFINED_CODECS_START);
    let duplicate_name_raw_codec = codecs::Blosc2CodecAbi {
        compcode: duplicate_name_code,
        complib: duplicate_name_code,
        ..raw_codec
    };
    assert_eq!(
        codecs::blosc2_register_codec_abi(&duplicate_name_raw_codec),
        BLOSC2_ERROR_SUCCESS,
        "C raw codec registration allows the same name on a different ID"
    );
    assert_eq!(
        compress::blosc2_compname_to_compcode_c(raw_name_str),
        i32::from(code),
        "C name lookup returns the first codec registered for a duplicate name"
    );

    let previous_empty_name_lookup = compress::blosc2_compname_to_compcode_c("");
    let empty_name_code = first_unused_codec_id(BLOSC2_USER_DEFINED_CODECS_START);
    let empty_name_raw_codec = codecs::Blosc2CodecAbi {
        compcode: empty_name_code,
        compname: b"\0".as_ptr().cast(),
        complib: empty_name_code,
        version: 1,
        encoder: Some(raw_codec_encoder),
        decoder: Some(raw_codec_decoder),
    };
    assert_eq!(
        codecs::blosc2_register_codec_abi(&empty_name_raw_codec),
        BLOSC2_ERROR_SUCCESS,
        "raw C-shaped registration parses an empty C string as a valid codec name"
    );
    assert_eq!(
        codecs::blosc2_register_codec_abi(&empty_name_raw_codec),
        BLOSC2_ERROR_SUCCESS,
        "empty-name raw C-shaped registration is idempotent by ID/name"
    );
    assert_eq!(
        compress::blosc2_compcode_to_compname_c(empty_name_code),
        (i32::from(empty_name_code), Some(""))
    );
    let expected_empty_name_lookup = if previous_empty_name_lookup >= 0 {
        previous_empty_name_lookup
    } else {
        i32::from(empty_name_code)
    };
    assert_eq!(
        compress::blosc2_compname_to_compcode_c(""),
        expected_empty_name_lookup,
        "C name lookup returns the first registered raw codec for duplicate empty names"
    );

    let data: Vec<u8> = (0usize..256)
        .map(|idx| 7u8.wrapping_add(3u8.wrapping_mul(idx as u8)))
        .collect();
    let mut encoded = vec![0u8; data.len()];
    raw_codec_reset_snapshots();
    let encoded_len = codecs::compress_block_with_meta(code, 6, META, &data, &mut encoded);
    assert_eq!(encoded_len, 3);
    assert_eq!(
        RAW_CODEC_LAST_ENCODER_INPUT_PTR.load(Ordering::SeqCst),
        data.as_ptr() as usize,
        "raw C ABI block encoder must receive the block input pointer"
    );
    assert_eq!(
        RAW_CODEC_LAST_ENCODER_CHUNK_PTR.load(Ordering::SeqCst),
        data.as_ptr() as usize,
        "raw C ABI block encoder fallback chunk pointer must match C's input pointer"
    );
    assert_eq!(
        RAW_CODEC_LAST_ENCODER_INPUT_LEN.load(Ordering::SeqCst),
        data.len() as i32
    );
    assert_eq!(
        RAW_CODEC_LAST_ENCODER_OUTPUT_LEN.load(Ordering::SeqCst),
        encoded.len() as i32
    );
    assert_eq!(
        RAW_CODEC_LAST_COMP_CODE.load(Ordering::SeqCst),
        code as usize
    );
    assert_eq!(
        RAW_CODEC_LAST_COMP_META.load(Ordering::SeqCst),
        META as usize
    );
    assert_eq!(RAW_CODEC_LAST_CLEVEL.load(Ordering::SeqCst), 6);
    assert_eq!(RAW_CODEC_LAST_META.load(Ordering::SeqCst), META as usize);
    assert_eq!(
        RAW_CODEC_LAST_TYPESIZE.load(Ordering::SeqCst),
        8,
        "raw C ABI block encoder fallback cparams must use C defaults"
    );
    assert_eq!(RAW_CODEC_LAST_USE_DICT.load(Ordering::SeqCst), 0);
    assert_eq!(RAW_CODEC_LAST_BLOCKSIZE.load(Ordering::SeqCst), 0);
    assert_eq!(
        RAW_CODEC_LAST_SPLITMODE.load(Ordering::SeqCst),
        BLOSC_FORWARD_COMPAT_SPLIT
    );
    assert_eq!(
        RAW_CODEC_LAST_FILTER.load(Ordering::SeqCst),
        BLOSC_SHUFFLE as usize
    );
    assert_eq!(RAW_CODEC_LAST_PREPARAMS.load(Ordering::SeqCst), 0);
    assert!(!RAW_CODEC_LAST_INSTR_CODEC.load(Ordering::SeqCst));
    assert_eq!(RAW_CODEC_LAST_CODEC_PARAMS.load(Ordering::SeqCst), 0);
    assert_eq!(RAW_CODEC_LAST_CPARAMS_SCHUNK.load(Ordering::SeqCst), 0);

    let mut decoded = vec![0u8; data.len()];
    raw_codec_reset_snapshots();
    assert_eq!(
        codecs::decompress_block_with_meta(
            code,
            META,
            &encoded[..encoded_len as usize],
            &mut decoded,
        ),
        data.len() as i32
    );
    assert_eq!(decoded, data);
    assert_eq!(
        RAW_CODEC_LAST_DECODER_INPUT_PTR.load(Ordering::SeqCst),
        encoded.as_ptr() as usize,
        "raw C ABI block decoder must receive the compressed block pointer"
    );
    assert_eq!(
        RAW_CODEC_LAST_DECODER_CHUNK_PTR.load(Ordering::SeqCst),
        encoded.as_ptr() as usize,
        "raw C ABI block decoder fallback chunk pointer must match C's input pointer"
    );
    assert_eq!(RAW_CODEC_LAST_DECODER_INPUT_LEN.load(Ordering::SeqCst), 3);
    assert_eq!(
        RAW_CODEC_LAST_DECODER_OUTPUT_LEN.load(Ordering::SeqCst),
        data.len() as i32
    );
    assert_eq!(RAW_CODEC_LAST_META.load(Ordering::SeqCst), META as usize);
    assert_eq!(
        RAW_CODEC_LAST_TYPESIZE.load(Ordering::SeqCst),
        8,
        "raw C ABI block decoder fallback dparams must use C defaults"
    );
    assert_eq!(RAW_CODEC_LAST_POSTPARAMS.load(Ordering::SeqCst), 0);
    assert_eq!(RAW_CODEC_LAST_DPARAMS_SCHUNK.load(Ordering::SeqCst), 0);

    let dict_cparams = CParams {
        compcode: code,
        use_dict: true,
        ..Default::default()
    };
    assert_eq!(
        compress::compress(&data, &dict_cparams).unwrap_err(),
        "Dictionary compression is only supported for Zstd, LZ4, and LZ4HC",
        "C rejects use_dict before invoking user-codec callbacks for unsupported codecs"
    );

    let cparams = CParams {
        compcode: code,
        compcode_meta: META,
        clevel: 6,
        typesize: 4,
        blocksize: 130,
        splitmode: BLOSC_NEVER_SPLIT,
        filters: [0; BLOSC2_MAX_FILTERS],
        codec_params: 0xc0decafe,
        instr_codec: true,
        prefilter_user_data: 0x1234,
        schunk: 0x1357,
        ..Default::default()
    };
    raw_codec_reset_snapshots();
    let chunk =
        compress::compress(&data, &cparams).expect("raw C ABI codec chunk compression must work");
    assert_udcodec_header(&chunk, code, META, "raw C ABI registered codec");
    assert_eq!(
        RAW_CODEC_LAST_COMP_CODE.load(Ordering::SeqCst),
        code as usize
    );
    assert_eq!(
        RAW_CODEC_LAST_COMP_META.load(Ordering::SeqCst),
        META as usize
    );
    assert_eq!(RAW_CODEC_LAST_CLEVEL.load(Ordering::SeqCst), 6);
    assert_eq!(RAW_CODEC_LAST_META.load(Ordering::SeqCst), META as usize);
    assert_eq!(
        RAW_CODEC_LAST_ENCODER_CHUNK_PTR.load(Ordering::SeqCst),
        data.as_ptr() as usize,
        "raw C ABI chunk encoder must receive the original source pointer, not the filtered block pointer"
    );
    assert_eq!(
        RAW_CODEC_LAST_ENCODER_INPUT_PTR.load(Ordering::SeqCst),
        data.as_ptr() as usize + expected_effective_blocksize(&cparams, data.len()) as usize,
        "raw C ABI chunk encoder input pointer must still point at the current block"
    );
    assert_eq!(
        RAW_CODEC_LAST_ENCODER_INPUT_LEN.load(Ordering::SeqCst),
        expected_effective_blocksize(&cparams, data.len())
    );
    assert_eq!(
        RAW_CODEC_LAST_ENCODER_OUTPUT_LEN.load(Ordering::SeqCst),
        expected_effective_blocksize(&cparams, data.len())
    );
    assert_eq!(
        RAW_CODEC_LAST_TYPESIZE.load(Ordering::SeqCst),
        4,
        "raw C ABI encoder must receive the active C compression typesize"
    );
    assert_eq!(RAW_CODEC_LAST_USE_DICT.load(Ordering::SeqCst), 0);
    assert_eq!(
        RAW_CODEC_LAST_BLOCKSIZE.load(Ordering::SeqCst),
        expected_effective_blocksize(&cparams, data.len()),
        "raw C ABI encoder must receive C's effective blocksize"
    );
    assert_eq!(
        RAW_CODEC_LAST_SPLITMODE.load(Ordering::SeqCst),
        BLOSC_NEVER_SPLIT
    );
    assert_eq!(RAW_CODEC_LAST_FILTER.load(Ordering::SeqCst), 0);
    assert_eq!(
        RAW_CODEC_LAST_PREPARAMS.load(Ordering::SeqCst),
        0,
        "raw C ABI encoder preparams must not be fabricated from Rust prefilter user_data"
    );
    assert!(RAW_CODEC_LAST_INSTR_CODEC.load(Ordering::SeqCst));
    assert_eq!(
        RAW_CODEC_LAST_CODEC_PARAMS.load(Ordering::SeqCst),
        0xc0decafe
    );
    assert_eq!(RAW_CODEC_LAST_CPARAMS_SCHUNK.load(Ordering::SeqCst), 0x1357);
    let dparams = DParams {
        postfilter_user_data: 0x5678,
        schunk: 0x2468,
        ..Default::default()
    };
    raw_codec_reset_snapshots();
    assert_eq!(
        compress::decompress_with_dparams(&chunk, &dparams)
            .expect("raw C ABI codec chunk must decode"),
        data
    );
    assert_eq!(
        RAW_CODEC_LAST_DECODER_CHUNK_PTR.load(Ordering::SeqCst),
        chunk.as_ptr() as usize,
        "raw C ABI chunk decoder must receive the compressed chunk pointer"
    );
    assert_ne!(
        RAW_CODEC_LAST_DECODER_INPUT_PTR.load(Ordering::SeqCst),
        RAW_CODEC_LAST_DECODER_CHUNK_PTR.load(Ordering::SeqCst),
        "raw C ABI chunk decoder input must remain the compressed block payload"
    );
    assert_eq!(RAW_CODEC_LAST_DECODER_INPUT_LEN.load(Ordering::SeqCst), 3);
    assert_eq!(
        RAW_CODEC_LAST_DECODER_OUTPUT_LEN.load(Ordering::SeqCst),
        expected_effective_blocksize(&cparams, data.len())
    );
    assert_eq!(
        RAW_CODEC_LAST_TYPESIZE.load(Ordering::SeqCst),
        4,
        "raw C ABI decoder must receive the active C decompression typesize"
    );
    assert_eq!(
        RAW_CODEC_LAST_POSTPARAMS.load(Ordering::SeqCst),
        0,
        "raw C ABI decoder postparams must not be fabricated from Rust postfilter user_data"
    );
    assert_eq!(RAW_CODEC_LAST_DPARAMS_SCHUNK.load(Ordering::SeqCst), 0x2468);

    let vl_blocks: Vec<Vec<u8>> = [17usize, 64, 9]
        .iter()
        .enumerate()
        .map(|(block_idx, &len)| {
            (0..len)
                .map(|idx| (block_idx as u8).wrapping_mul(11).wrapping_add(idx as u8))
                .collect()
        })
        .collect();
    let vl_refs: Vec<&[u8]> = vl_blocks.iter().map(Vec::as_slice).collect();
    let vl_cparams = CParams {
        compcode: code,
        compcode_meta: META,
        clevel: 6,
        typesize: 1,
        blocksize: 999,
        splitmode: BLOSC_NEVER_SPLIT,
        filters: [0; BLOSC2_MAX_FILTERS],
        codec_params: 0xfeed_beef,
        instr_codec: true,
        prefilter_user_data: 0x2468,
        ..Default::default()
    };
    raw_codec_reset_snapshots();
    let vlchunk = compress::vlcompress(&vl_refs, &vl_cparams)
        .expect("raw C ABI codec VL compression must work");
    assert_eq!(
        RAW_CODEC_LAST_ENCODER_CHUNK_PTR.load(Ordering::SeqCst),
        0,
        "raw C ABI VL encoder must receive C's null chunk pointer"
    );
    assert_eq!(RAW_CODEC_LAST_ENCODER_INPUT_LEN.load(Ordering::SeqCst), 9);
    assert_eq!(
        RAW_CODEC_LAST_ENCODER_OUTPUT_LEN.load(Ordering::SeqCst),
        9 + 32
    );
    assert_eq!(
        RAW_CODEC_LAST_BLOCKSIZE.load(Ordering::SeqCst),
        64,
        "VL raw C ABI encoder snapshot must use the maximum VL block size"
    );
    assert_eq!(RAW_CODEC_LAST_TYPESIZE.load(Ordering::SeqCst), 1);
    assert_eq!(
        RAW_CODEC_LAST_PREPARAMS.load(Ordering::SeqCst),
        0,
        "raw C ABI VL encoder preparams must not be fabricated from Rust prefilter user_data"
    );
    assert!(RAW_CODEC_LAST_INSTR_CODEC.load(Ordering::SeqCst));
    assert_eq!(
        RAW_CODEC_LAST_CODEC_PARAMS.load(Ordering::SeqCst),
        0xfeed_beef
    );
    let vl_dctx = compress::DContext::new(DParams {
        postfilter_user_data: 0x13579,
        schunk: 0x8642,
        ..Default::default()
    });
    raw_codec_reset_snapshots();
    assert_eq!(
        vl_dctx.vldecompress_block(&vlchunk, 1).unwrap(),
        vl_blocks[1]
    );
    assert_eq!(
        RAW_CODEC_LAST_DECODER_CHUNK_PTR.load(Ordering::SeqCst),
        vlchunk.as_ptr() as usize,
        "raw C ABI VL decoder must receive the compressed VL chunk pointer"
    );
    assert_ne!(
        RAW_CODEC_LAST_DECODER_INPUT_PTR.load(Ordering::SeqCst),
        RAW_CODEC_LAST_DECODER_CHUNK_PTR.load(Ordering::SeqCst),
        "raw C ABI VL decoder input must remain the compressed VL block payload"
    );
    assert_eq!(RAW_CODEC_LAST_DECODER_INPUT_LEN.load(Ordering::SeqCst), 3);
    assert_eq!(RAW_CODEC_LAST_DECODER_OUTPUT_LEN.load(Ordering::SeqCst), 64);
    assert_eq!(RAW_CODEC_LAST_TYPESIZE.load(Ordering::SeqCst), 1);
    assert_eq!(
        RAW_CODEC_LAST_POSTPARAMS.load(Ordering::SeqCst),
        0,
        "raw C ABI VL decoder postparams must not be fabricated from Rust postfilter user_data"
    );
    assert_eq!(RAW_CODEC_LAST_DPARAMS_SCHUNK.load(Ordering::SeqCst), 0x8642);
    assert_eq!(compress::vldecompress(&vlchunk).unwrap(), vl_blocks);
}

/// Compress with C BloscLZ, decompress with Rust BloscLZ
#[test]
fn test_blosclz_c_compress_rust_decompress() {
    let _b = init_blosc2();

    let data: Vec<u8> = (0..10000u32).flat_map(|i| i.to_le_bytes()).collect();
    let src_size = data.len() as i32;
    let buf_size = src_size as usize + BLOSC_EXTENDED_HEADER_LENGTH;

    // Compress with C (full blosc2 pipeline, blosclz codec, no shuffle to isolate codec)
    let mut compressed = vec![0u8; buf_size];
    let csize = unsafe {
        let mut cparams: ffi::blosc2_cparams = std::mem::zeroed();
        cparams.compcode = BLOSC_BLOSCLZ;
        cparams.clevel = 5;
        cparams.typesize = 4;
        cparams.nthreads = 1;
        cparams.splitmode = BLOSC_NEVER_SPLIT;
        // No filters to isolate codec behavior
        cparams.filters = [0; 6];

        let cctx = ffi::blosc2_create_cctx(cparams);
        let result = ffi::blosc2_compress_ctx(
            cctx,
            data.as_ptr() as *const _,
            src_size,
            compressed.as_mut_ptr() as *mut _,
            compressed.len() as i32,
        );
        ffi::blosc2_free_ctx(cctx);
        result
    };
    assert!(csize > 0, "C compression failed");

    // Decompress with Rust for the cross-compatibility assertion.
    let rust_decompressed =
        compress::decompress(&compressed[..csize as usize]).expect("Rust decompression failed");
    assert_eq!(rust_decompressed, data, "Rust decompression mismatch");

    // Decompress with C too, so failures are easier to diagnose.
    let mut c_decompressed = vec![0u8; src_size as usize];
    let c_dsize = unsafe {
        ffi::blosc2_decompress(
            compressed.as_ptr() as *const _,
            csize,
            c_decompressed.as_mut_ptr() as *mut _,
            c_decompressed.len() as i32,
        )
    };
    assert_eq!(c_dsize, src_size, "C decompression size mismatch");
    assert_eq!(data, c_decompressed, "C roundtrip data mismatch");
}

fn assert_c_chunk_rust_and_c_decompress_parity(data: &[u8], compcode: u8, context: &str) {
    let c_chunk = c_compress_chunk(data, compcode, false);
    let rust_decoded = compress::decompress(&c_chunk)
        .unwrap_or_else(|e| panic!("Rust failed to decode C chunk for {context}: {e}"));
    let c_decoded = c_decompress_chunk(&c_chunk, data.len(), context);

    assert_eq!(
        rust_decoded, c_decoded,
        "Rust and C decode results differ for {context}"
    );
    assert_eq!(rust_decoded, data, "C->Rust payload mismatch for {context}");
}

fn assert_rust_chunk_rust_and_c_decompress_parity(data: &[u8], compcode: u8, context: &str) {
    let cparams = CParams {
        compcode,
        clevel: 5,
        typesize: 4,
        blocksize: 4096,
        splitmode: BLOSC_NEVER_SPLIT,
        filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
        ..Default::default()
    };
    let rust_chunk = compress::compress(data, &cparams)
        .unwrap_or_else(|e| panic!("Rust failed to compress chunk for {context}: {e}"));
    let rust_decoded = compress::decompress(&rust_chunk)
        .unwrap_or_else(|e| panic!("Rust failed to decode Rust chunk for {context}: {e}"));
    let c_decoded = c_decompress_chunk(&rust_chunk, data.len(), context);

    assert_eq!(
        rust_decoded, c_decoded,
        "Rust and C decode results differ for Rust chunk {context}"
    );
    assert_eq!(c_decoded, data, "Rust->C payload mismatch for {context}");
}

/// Compress with C BloscLZ, decompress with Rust and C.
#[test]
fn test_blosclz_c_compress_rust_and_c_decompress_parity() {
    let _b = init_blosc2();
    let data: Vec<u8> = b"Hello BloscLZ! This is a test with repeating patterns. "
        .iter()
        .cycle()
        .take(40000)
        .copied()
        .collect();
    assert_c_chunk_rust_and_c_decompress_parity(&data, BLOSC_BLOSCLZ, "codec=blosclz");
}

/// Compress with C LZ4, decompress with Rust and C.
#[test]
fn test_lz4_c_compress_rust_and_c_decompress_parity() {
    let _b = init_blosc2();
    let data: Vec<u8> = (0..5000u32).flat_map(|i| i.to_le_bytes()).collect();
    assert_c_chunk_rust_and_c_decompress_parity(&data, BLOSC_LZ4, "codec=lz4");
}

#[test]
fn test_lz4hc_c_2gb_limit_boundary_matches_c_codec_wrapper() {
    const PROT_READ: i32 = 0x1;
    const MAP_PRIVATE: i32 = 0x02;
    const MAP_ANONYMOUS: i32 = 0x20;
    const MAP_NORESERVE: i32 = 0x4000;

    unsafe extern "C" {
        fn mmap(
            addr: *mut std::ffi::c_void,
            length: usize,
            prot: i32,
            flags: i32,
            fd: i32,
            offset: isize,
        ) -> *mut std::ffi::c_void;
        fn munmap(addr: *mut std::ffi::c_void, length: usize) -> i32;
    }

    let c_lz4hc_limit = 2usize << 30;
    let input_len = c_lz4hc_limit + 1;
    let ptr = unsafe {
        mmap(
            std::ptr::null_mut(),
            input_len,
            PROT_READ,
            MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE,
            -1,
            0,
        )
    };
    assert_ne!(
        ptr as isize, -1,
        "test needs an anonymous mapping for the over-2GiB LZ4HC size probe"
    );

    let input = unsafe { std::slice::from_raw_parts(ptr.cast::<u8>(), input_len) };
    let mut output = [];
    assert_eq!(
        codecs::compress_block(BLOSC_LZ4HC, 9, input, &mut output),
        BLOSC2_ERROR_2GB_LIMIT,
        "Rust LZ4HC must mirror C lz4hc_wrap_compress and reject input_length > (2 << 30)"
    );
    assert_eq!(unsafe { munmap(ptr, input_len) }, 0);
}

/// Compress with C Zlib, decompress with Rust and C.
#[test]
fn test_zlib_c_compress_rust_and_c_decompress_parity() {
    let _b = init_blosc2();
    let data: Vec<u8> = (0..5000u32).flat_map(|i| i.to_le_bytes()).collect();
    assert_c_chunk_rust_and_c_decompress_parity(&data, BLOSC_ZLIB, "codec=zlib");
}

/// Compress with C Zstd, decompress with Rust and C.
#[test]
fn test_zstd_c_compress_rust_and_c_decompress_parity() {
    let _b = init_blosc2();
    let data: Vec<u8> = (0..5000u32).flat_map(|i| i.to_le_bytes()).collect();
    assert_c_chunk_rust_and_c_decompress_parity(&data, BLOSC_ZSTD, "codec=zstd");
}

/// Compress with C codecs across patterns, decompress with Rust and C.
#[test]
fn test_all_codecs_patterns_c_compress_rust_and_c_decompress_parity() {
    let _b = init_blosc2();
    let patterns: Vec<(&str, Vec<u8>)> = vec![
        (
            "sequential",
            (0..20000u32).flat_map(|i| i.to_le_bytes()).collect(),
        ),
        ("repeated", vec![42u8; 20000]),
        ("sparse", {
            let mut d = vec![0u8; 20000];
            for i in (0..20000).step_by(100) {
                d[i] = 0xFF;
            }
            d
        }),
    ];

    let codecs = [
        BLOSC_BLOSCLZ,
        BLOSC_LZ4,
        BLOSC_LZ4HC,
        BLOSC_ZLIB,
        BLOSC_ZSTD,
    ];

    for (name, data) in &patterns {
        for &codec in &codecs {
            assert_c_chunk_rust_and_c_decompress_parity(
                data,
                codec,
                &format!("codec={codec} pattern={name}"),
            );
            assert_rust_chunk_rust_and_c_decompress_parity(
                data,
                codec,
                &format!("codec={codec} pattern={name}"),
            );
        }
    }
}
