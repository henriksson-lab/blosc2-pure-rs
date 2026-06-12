#![cfg(feature = "_ffi")]
mod common;
use common::ffi;

fn init_blosc2() -> common::Blosc2 {
    common::Blosc2::new()
}

unsafe extern "C" fn noop_prefilter(_params: *mut ffi::blosc2_prefilter_params) -> i32 {
    0
}

unsafe extern "C" fn noop_postfilter(_params: *mut ffi::blosc2_postfilter_params) -> i32 {
    0
}

unsafe fn compress_i32_chunk(data: &[i32]) -> (Vec<u8>, i32) {
    let src_size = std::mem::size_of_val(data) as i32;
    let mut compressed = vec![0u8; src_size as usize + ffi::BLOSC2_MAX_OVERHEAD as usize];
    let csize = unsafe {
        ffi::blosc2_compress(
            5,
            ffi::BLOSC_SHUFFLE as i32,
            std::mem::size_of::<i32>() as i32,
            data.as_ptr() as *const _,
            src_size,
            compressed.as_mut_ptr() as *mut _,
            compressed.len() as i32,
        )
    };
    assert!(csize > 0, "Compression failed: {csize}");
    (compressed, csize)
}

#[test]
fn test_context_getters_copy_callback_slots_like_c_api() {
    let _b = init_blosc2();

    unsafe {
        let mut cparams = ffi::blosc2_get_blosc2_cparams_defaults();
        cparams.typesize = 1;
        cparams.nthreads = 1;
        cparams.prefilter = Some(noop_prefilter);
        cparams.codec_params = 0x1234usize as *mut std::ffi::c_void;
        let mut preparams = ffi::blosc2_prefilter_params {
            user_data: 0x5678usize as *mut std::ffi::c_void,
            output_typesize: 1,
            nchunk: -1,
            output_is_disposable: true,
            ..Default::default()
        };
        cparams.preparams = &mut preparams;

        let cctx = ffi::blosc2_create_cctx(cparams);
        assert!(!cctx.is_null());

        let mut actual_cparams = ffi::blosc2_cparams {
            tuner_params: 0x9usize as *mut std::ffi::c_void,
            filter_params: [0xausize as *mut std::ffi::c_void; ffi::BLOSC2_MAX_FILTERS as usize],
            ..Default::default()
        };
        let rc = ffi::blosc2_ctx_get_cparams(cctx, &mut actual_cparams);
        assert_eq!(rc, ffi::BLOSC2_ERROR_SUCCESS);
        assert!(actual_cparams.prefilter.is_some());
        assert_ne!(actual_cparams.preparams, cparams.preparams);
        assert!(!actual_cparams.preparams.is_null());
        assert_eq!((*actual_cparams.preparams).user_data, preparams.user_data);
        assert_eq!(
            (*actual_cparams.preparams).output_typesize,
            preparams.output_typesize
        );
        assert_eq!((*actual_cparams.preparams).nchunk, preparams.nchunk);
        assert_eq!(
            (*actual_cparams.preparams).output_is_disposable,
            preparams.output_is_disposable
        );
        assert_eq!(actual_cparams.codec_params, cparams.codec_params);
        assert_eq!(
            actual_cparams.tuner_params, 0x9usize as *mut std::ffi::c_void,
            "C getter does not write tuner_params"
        );
        assert_eq!(
            actual_cparams.filter_params,
            [0xausize as *mut std::ffi::c_void; ffi::BLOSC2_MAX_FILTERS as usize],
            "C getter does not write filter_params"
        );
        let mut repeated_cparams = ffi::blosc2_cparams::default();
        let rc = ffi::blosc2_ctx_get_cparams(cctx, &mut repeated_cparams);
        assert_eq!(rc, ffi::BLOSC2_ERROR_SUCCESS);
        assert_eq!(
            repeated_cparams.preparams, actual_cparams.preparams,
            "C getter exposes the context-owned preparams pointer"
        );

        let mut dparams = ffi::blosc2_get_blosc2_dparams_defaults();
        dparams.nthreads = 1;
        dparams.postfilter = Some(noop_postfilter);
        let mut postparams = ffi::blosc2_postfilter_params {
            user_data: 0xabcusize as *mut std::ffi::c_void,
            typesize: 1,
            nchunk: -1,
            ..Default::default()
        };
        dparams.postparams = &mut postparams;

        let dctx = ffi::blosc2_create_dctx(dparams);
        assert!(!dctx.is_null());

        let mut actual_dparams = ffi::blosc2_dparams::default();
        let rc = ffi::blosc2_ctx_get_dparams(dctx, &mut actual_dparams);
        assert_eq!(rc, ffi::BLOSC2_ERROR_SUCCESS);
        assert!(actual_dparams.postfilter.is_some());
        assert_ne!(actual_dparams.postparams, dparams.postparams);
        assert!(!actual_dparams.postparams.is_null());
        assert_eq!((*actual_dparams.postparams).user_data, postparams.user_data);
        assert_eq!((*actual_dparams.postparams).typesize, postparams.typesize);
        assert_eq!((*actual_dparams.postparams).nchunk, postparams.nchunk);
        let mut repeated_dparams = ffi::blosc2_dparams::default();
        let rc = ffi::blosc2_ctx_get_dparams(dctx, &mut repeated_dparams);
        assert_eq!(rc, ffi::BLOSC2_ERROR_SUCCESS);
        assert_eq!(
            repeated_dparams.postparams, actual_dparams.postparams,
            "C getter exposes the context-owned postparams pointer"
        );

        ffi::blosc2_free_ctx(cctx);
        ffi::blosc2_free_ctx(dctx);
    }
}

#[test]
fn test_default_structs_match_c_api() {
    let cparams = unsafe { ffi::blosc2_get_blosc2_cparams_defaults() };
    assert_eq!(cparams.compcode, ffi::BLOSC_BLOSCLZ as u8);
    assert_eq!(cparams.compcode_meta, 0);
    assert_eq!(cparams.clevel, 5);
    assert_eq!(cparams.use_dict, 0);
    assert_eq!(cparams.typesize, 8);
    assert_eq!(cparams.nthreads, 1);
    assert_eq!(cparams.blocksize, 0);
    assert_eq!(cparams.splitmode, ffi::BLOSC_FORWARD_COMPAT_SPLIT as i32);
    assert!(cparams.schunk.is_null());
    assert_eq!(cparams.filters, [0, 0, 0, 0, 0, ffi::BLOSC_SHUFFLE as u8]);
    assert_eq!(cparams.filters_meta, [0; ffi::BLOSC2_MAX_FILTERS as usize]);
    assert!(cparams.prefilter.is_none());
    assert!(cparams.preparams.is_null());
    assert!(cparams.tuner_params.is_null());
    assert_eq!(cparams.tuner_id, 0);
    assert!(!cparams.instr_codec);
    assert!(cparams.codec_params.is_null());
    assert!(cparams.filter_params.iter().all(|param| param.is_null()));

    let dparams = unsafe { ffi::blosc2_get_blosc2_dparams_defaults() };
    assert_eq!(dparams.nthreads, 1);
    assert!(dparams.schunk.is_null());
    assert!(dparams.postfilter.is_none());
    assert!(dparams.postparams.is_null());
    assert_eq!(dparams.typesize, 8);

    let storage = unsafe { ffi::blosc2_get_blosc2_storage_defaults() };
    assert!(!storage.contiguous);
    assert!(storage.urlpath.is_null());
    assert!(storage.cparams.is_null());
    assert!(storage.dparams.is_null());
    assert!(storage.io.is_null());
}

#[test]
fn test_context_defaults_and_wrong_direction_errors() {
    let _b = init_blosc2();
    let data: Vec<i32> = (0..1024).collect();
    let (compressed, csize) = unsafe { compress_i32_chunk(&data) };
    let src_size = std::mem::size_of_val(data.as_slice()) as i32;

    unsafe {
        let mut cparams = ffi::blosc2_get_blosc2_cparams_defaults();
        cparams.typesize = std::mem::size_of::<i32>() as i32;
        cparams.nthreads = 1;

        let cctx = ffi::blosc2_create_cctx(cparams);
        assert!(!cctx.is_null());

        let sentinel_tuner_params = 0x1usize as *mut std::ffi::c_void;
        let sentinel_filter_params =
            [0x2usize as *mut std::ffi::c_void; ffi::BLOSC2_MAX_FILTERS as usize];
        let mut actual_cparams = ffi::blosc2_cparams {
            tuner_params: sentinel_tuner_params,
            filter_params: sentinel_filter_params,
            ..Default::default()
        };
        let rc = ffi::blosc2_ctx_get_cparams(cctx, &mut actual_cparams);
        assert_eq!(rc, ffi::BLOSC2_ERROR_SUCCESS);
        assert_eq!(actual_cparams.clevel, cparams.clevel);
        assert_eq!(actual_cparams.typesize, cparams.typesize);
        assert_eq!(actual_cparams.nthreads, cparams.nthreads);
        assert_eq!(actual_cparams.compcode, cparams.compcode);
        assert_eq!(actual_cparams.compcode_meta, cparams.compcode_meta);
        assert_eq!(actual_cparams.use_dict, cparams.use_dict);
        assert_eq!(actual_cparams.blocksize, cparams.blocksize);
        assert_eq!(actual_cparams.splitmode, cparams.splitmode);
        assert!(actual_cparams.schunk.is_null());
        assert_eq!(actual_cparams.filters, cparams.filters);
        assert_eq!(actual_cparams.filters_meta, cparams.filters_meta);
        assert!(actual_cparams.prefilter.is_none());
        assert!(actual_cparams.preparams.is_null());
        assert_eq!(actual_cparams.tuner_params, sentinel_tuner_params);
        assert_eq!(actual_cparams.tuner_id, cparams.tuner_id);
        assert_eq!(actual_cparams.instr_codec, cparams.instr_codec);
        assert!(actual_cparams.codec_params.is_null());
        assert_eq!(actual_cparams.filter_params, sentinel_filter_params);

        let mut restored = vec![0u8; src_size as usize];
        let wrong_ctx_decompress = ffi::blosc2_decompress_ctx(
            cctx,
            compressed.as_ptr() as *const _,
            csize,
            restored.as_mut_ptr() as *mut _,
            restored.len() as i32,
        );
        assert_eq!(wrong_ctx_decompress, ffi::BLOSC2_ERROR_INVALID_PARAM);

        let mut dparams = ffi::blosc2_get_blosc2_dparams_defaults();
        dparams.nthreads = 1;
        let dctx = ffi::blosc2_create_dctx(dparams);
        assert!(!dctx.is_null());

        let mut actual_dparams = ffi::blosc2_dparams::default();
        let rc = ffi::blosc2_ctx_get_dparams(dctx, &mut actual_dparams);
        assert_eq!(rc, ffi::BLOSC2_ERROR_SUCCESS);
        assert_eq!(actual_dparams.nthreads, dparams.nthreads);
        assert!(actual_dparams.schunk.is_null());
        assert!(actual_dparams.postfilter.is_none());
        assert!(actual_dparams.postparams.is_null());
        assert_eq!(
            actual_dparams.typesize, 0,
            "C leaves a fresh dctx typesize unset until a chunk header is read"
        );

        let mut out = vec![0u8; src_size as usize + ffi::BLOSC2_MAX_OVERHEAD as usize];
        let wrong_ctx_compress = ffi::blosc2_compress_ctx(
            dctx,
            data.as_ptr() as *const _,
            src_size,
            out.as_mut_ptr() as *mut _,
            out.len() as i32,
        );
        assert_eq!(wrong_ctx_compress, ffi::BLOSC2_ERROR_INVALID_PARAM);

        let dsize = ffi::blosc2_decompress_ctx(
            dctx,
            compressed.as_ptr() as *const _,
            csize,
            restored.as_mut_ptr() as *mut _,
            restored.len() as i32,
        );
        assert_eq!(dsize, src_size);
        let mut used_dparams = ffi::blosc2_dparams::default();
        let rc = ffi::blosc2_ctx_get_dparams(dctx, &mut used_dparams);
        assert_eq!(rc, ffi::BLOSC2_ERROR_SUCCESS);
        assert_eq!(used_dparams.typesize, cparams.typesize);

        ffi::blosc2_free_ctx(cctx);
        ffi::blosc2_free_ctx(dctx);
    }
}

#[test]
fn test_api_error_return_counts_match_c_api() {
    let _b = init_blosc2();
    let data: Vec<i32> = (0..256).collect();
    let src_size = std::mem::size_of_val(data.as_slice()) as i32;
    let (compressed, csize) = unsafe { compress_i32_chunk(&data) };

    unsafe {
        assert_eq!(ffi::BLOSC2_ERROR_READ_BUFFER, -5);
        assert_eq!(ffi::BLOSC2_ERROR_WRITE_BUFFER, -6);
        assert_eq!(ffi::BLOSC2_ERROR_CODEC_SUPPORT, -7);
        assert_eq!(ffi::BLOSC2_ERROR_CODEC_PARAM, -8);
        assert_eq!(ffi::BLOSC2_ERROR_CODEC_DICT, -9);
        assert_eq!(ffi::BLOSC2_ERROR_VERSION_SUPPORT, -10);
        assert_eq!(ffi::BLOSC2_ERROR_INVALID_HEADER, -11);
        assert_eq!(ffi::BLOSC2_ERROR_INVALID_PARAM, -12);
        assert_eq!(ffi::BLOSC2_ERROR_FILE_READ, -13);
        assert_eq!(ffi::BLOSC2_ERROR_FILE_WRITE, -14);
        assert_eq!(ffi::BLOSC2_ERROR_FILE_OPEN, -15);
        assert_eq!(ffi::BLOSC2_ERROR_NOT_FOUND, -16);
        assert_eq!(ffi::BLOSC2_ERROR_RUN_LENGTH, -17);
        assert_eq!(ffi::BLOSC2_ERROR_FILTER_PIPELINE, -18);
        assert_eq!(ffi::BLOSC2_ERROR_CHUNK_INSERT, -19);
        assert_eq!(ffi::BLOSC2_ERROR_CHUNK_APPEND, -20);
        assert_eq!(ffi::BLOSC2_ERROR_CHUNK_UPDATE, -21);
        assert_eq!(ffi::BLOSC2_ERROR_2GB_LIMIT, -22);
        assert_eq!(ffi::BLOSC2_ERROR_SCHUNK_COPY, -23);
        assert_eq!(ffi::BLOSC2_ERROR_FRAME_TYPE, -24);
        assert_eq!(ffi::BLOSC2_ERROR_FILE_TRUNCATE, -25);
        assert_eq!(ffi::BLOSC2_ERROR_THREAD_CREATE, -26);
        assert_eq!(ffi::BLOSC2_ERROR_POSTFILTER, -27);
        assert_eq!(ffi::BLOSC2_ERROR_FRAME_SPECIAL, -28);
        assert_eq!(ffi::BLOSC2_ERROR_SCHUNK_SPECIAL, -29);
        assert_eq!(ffi::BLOSC2_ERROR_PLUGIN_IO, -30);
        assert_eq!(ffi::BLOSC2_ERROR_FILE_REMOVE, -31);
        assert_eq!(ffi::BLOSC2_ERROR_NULL_POINTER, -32);
        assert_eq!(ffi::BLOSC2_ERROR_INVALID_INDEX, -33);
        assert_eq!(ffi::BLOSC2_ERROR_METALAYER_NOT_FOUND, -34);
        assert_eq!(ffi::BLOSC2_ERROR_MAX_BUFSIZE_EXCEEDED, -35);
        assert_eq!(ffi::BLOSC2_ERROR_TUNER, -36);

        let mut too_small_for_header = vec![0u8; ffi::BLOSC2_MAX_OVERHEAD as usize - 1];
        let csize_small_dest = ffi::blosc2_compress(
            5,
            ffi::BLOSC_SHUFFLE as i32,
            std::mem::size_of::<i32>() as i32,
            data.as_ptr() as *const _,
            src_size,
            too_small_for_header.as_mut_ptr() as *mut _,
            too_small_for_header.len() as i32,
        );
        assert_eq!(csize_small_dest, ffi::BLOSC2_ERROR_MAX_BUFSIZE_EXCEEDED);

        let mut restored = vec![0u8; src_size as usize - 1];
        let dsize_small_dest = ffi::blosc2_decompress(
            compressed.as_ptr() as *const _,
            csize,
            restored.as_mut_ptr() as *mut _,
            restored.len() as i32,
        );
        assert_eq!(dsize_small_dest, ffi::BLOSC2_ERROR_WRITE_BUFFER);

        let mut restored = vec![0u8; src_size as usize];
        let dsize_negative_src = ffi::blosc2_decompress(
            compressed.as_ptr() as *const _,
            -1,
            restored.as_mut_ptr() as *mut _,
            restored.len() as i32,
        );
        assert_eq!(dsize_negative_src, ffi::BLOSC2_ERROR_READ_BUFFER);
    }
}

#[test]
fn test_special_chunk_c_adapters_match_c_api() {
    let _b = init_blosc2();

    unsafe {
        let mut cparams = ffi::blosc2_get_blosc2_cparams_defaults();
        cparams.compcode = ffi::BLOSC_BLOSCLZ as u8;
        cparams.use_dict = 1;
        cparams.typesize = 8;
        cparams.nthreads = 1;
        cparams.instr_codec = true;
        cparams.codec_params = 0x1234usize as *mut std::ffi::c_void;
        cparams.filter_params[0] = 0x5678usize as *mut std::ffi::c_void;

        let mut chunk = vec![0u8; ffi::BLOSC_EXTENDED_HEADER_LENGTH as usize + 8];

        let zero_len = ffi::blosc2_chunk_zeros(
            cparams,
            32,
            chunk.as_mut_ptr() as *mut _,
            ffi::BLOSC_EXTENDED_HEADER_LENGTH as i32,
        );
        assert_eq!(zero_len, ffi::BLOSC_EXTENDED_HEADER_LENGTH as i32);
        assert_eq!(
            chunk[ffi::BLOSC2_CHUNK_TYPESIZE as usize],
            cparams.typesize as u8
        );
        assert_eq!(
            (chunk[ffi::BLOSC2_CHUNK_BLOSC2_FLAGS as usize] >> 4) & ffi::BLOSC2_SPECIAL_MASK as u8,
            ffi::BLOSC2_SPECIAL_ZERO as u8
        );
        assert_eq!(
            chunk[ffi::BLOSC2_CHUNK_BLOSC2_FLAGS as usize] & ffi::BLOSC2_INSTR_CODEC as u8,
            0,
            "C special chunks ignore instr_codec in the serialized chunk flags"
        );
        assert_eq!(
            chunk[ffi::BLOSC2_CHUNK_FLAGS as usize] & ffi::BLOSC_MEMCPYED as u8,
            0,
            "C special chunks do not serialize cparams.use_dict as a chunk header flag"
        );
        let mut restored = [1u8; 32];
        let dsize = ffi::blosc2_decompress(
            chunk.as_ptr() as *const _,
            zero_len,
            restored.as_mut_ptr() as *mut _,
            restored.len() as i32,
        );
        assert_eq!(dsize, restored.len() as i32);
        assert_eq!(restored, [0u8; 32]);

        let repeat_value = 0x1122_3344_5566_7788u64.to_le_bytes();
        let repeat_len = ffi::blosc2_chunk_repeatval(
            cparams,
            32,
            chunk.as_mut_ptr() as *mut _,
            chunk.len() as i32,
            repeat_value.as_ptr() as *const _,
        );
        assert_eq!(
            repeat_len,
            ffi::BLOSC_EXTENDED_HEADER_LENGTH as i32 + cparams.typesize
        );
        assert_eq!(
            (chunk[ffi::BLOSC2_CHUNK_BLOSC2_FLAGS as usize] >> 4) & ffi::BLOSC2_SPECIAL_MASK as u8,
            ffi::BLOSC2_SPECIAL_VALUE as u8
        );
        assert_eq!(
            chunk[ffi::BLOSC2_CHUNK_BLOSC2_FLAGS as usize] & ffi::BLOSC2_INSTR_CODEC as u8,
            0
        );
        assert_eq!(
            &chunk[ffi::BLOSC_EXTENDED_HEADER_LENGTH as usize..repeat_len as usize],
            repeat_value.as_slice()
        );
        let mut restored = [0u8; 32];
        let dsize = ffi::blosc2_decompress(
            chunk.as_ptr() as *const _,
            repeat_len,
            restored.as_mut_ptr() as *mut _,
            restored.len() as i32,
        );
        assert_eq!(dsize, restored.len() as i32);
        for item in restored.chunks_exact(repeat_value.len()) {
            assert_eq!(item, repeat_value.as_slice());
        }

        let nan_len = ffi::blosc2_chunk_nans(
            cparams,
            16,
            chunk.as_mut_ptr() as *mut _,
            ffi::BLOSC_EXTENDED_HEADER_LENGTH as i32,
        );
        assert_eq!(nan_len, ffi::BLOSC_EXTENDED_HEADER_LENGTH as i32);
        assert_eq!(
            (chunk[ffi::BLOSC2_CHUNK_BLOSC2_FLAGS as usize] >> 4) & ffi::BLOSC2_SPECIAL_MASK as u8,
            ffi::BLOSC2_SPECIAL_NAN as u8
        );
        assert_eq!(
            chunk[ffi::BLOSC2_CHUNK_BLOSC2_FLAGS as usize] & ffi::BLOSC2_INSTR_CODEC as u8,
            0
        );
        let mut restored = [0u8; 16];
        let dsize = ffi::blosc2_decompress(
            chunk.as_ptr() as *const _,
            nan_len,
            restored.as_mut_ptr() as *mut _,
            restored.len() as i32,
        );
        assert_eq!(dsize, restored.len() as i32);
        for item in restored.chunks_exact(std::mem::size_of::<f64>()) {
            assert!(f64::from_le_bytes(item.try_into().unwrap()).is_nan());
        }

        let uninit_len = ffi::blosc2_chunk_uninit(
            cparams,
            16,
            chunk.as_mut_ptr() as *mut _,
            ffi::BLOSC_EXTENDED_HEADER_LENGTH as i32,
        );
        assert_eq!(uninit_len, ffi::BLOSC_EXTENDED_HEADER_LENGTH as i32);
        assert_eq!(
            (chunk[ffi::BLOSC2_CHUNK_BLOSC2_FLAGS as usize] >> 4) & ffi::BLOSC2_SPECIAL_MASK as u8,
            ffi::BLOSC2_SPECIAL_UNINIT as u8
        );
        assert_eq!(
            chunk[ffi::BLOSC2_CHUNK_BLOSC2_FLAGS as usize] & ffi::BLOSC2_INSTR_CODEC as u8,
            0
        );

        assert_eq!(
            ffi::blosc2_chunk_zeros(
                cparams,
                32,
                chunk.as_mut_ptr() as *mut _,
                ffi::BLOSC_EXTENDED_HEADER_LENGTH as i32 - 1,
            ),
            ffi::BLOSC2_ERROR_DATA
        );
        assert_eq!(
            ffi::blosc2_chunk_repeatval(
                cparams,
                32,
                chunk.as_mut_ptr() as *mut _,
                ffi::BLOSC_EXTENDED_HEADER_LENGTH as i32 + cparams.typesize - 1,
                repeat_value.as_ptr() as *const _,
            ),
            ffi::BLOSC2_ERROR_DATA
        );
        assert_eq!(
            ffi::blosc2_chunk_zeros(
                cparams,
                33,
                chunk.as_mut_ptr() as *mut _,
                ffi::BLOSC_EXTENDED_HEADER_LENGTH as i32,
            ),
            ffi::BLOSC2_ERROR_DATA
        );
    }
}

#[test]
fn test_compress_decompress_basic() {
    let _b = init_blosc2();

    let data: Vec<u8> = (0..10000u32).flat_map(|i| i.to_le_bytes()).collect();
    let src_size = data.len() as i32;
    let mut compressed = vec![0u8; src_size as usize + ffi::BLOSC_EXTENDED_HEADER_LENGTH as usize];

    let csize = unsafe {
        ffi::blosc2_compress(
            5,                         // clevel
            ffi::BLOSC_SHUFFLE as i32, // filter
            4,                         // typesize
            data.as_ptr() as *const _,
            src_size,
            compressed.as_mut_ptr() as *mut _,
            compressed.len() as i32,
        )
    };
    assert!(csize > 0, "Compression failed: {csize}");

    let mut decompressed = vec![0u8; src_size as usize];
    let dsize = unsafe {
        ffi::blosc2_decompress(
            compressed.as_ptr() as *const _,
            csize,
            decompressed.as_mut_ptr() as *mut _,
            decompressed.len() as i32,
        )
    };
    assert_eq!(dsize, src_size, "Decompression size mismatch");
    assert_eq!(data, decompressed, "Data mismatch after roundtrip");
}

#[test]
fn test_compress_all_codecs() {
    let _b = init_blosc2();

    let data: Vec<u8> = (0..5000u32).flat_map(|i| i.to_le_bytes()).collect();
    let src_size = data.len() as i32;
    let buf_size = src_size as usize + ffi::BLOSC_EXTENDED_HEADER_LENGTH as usize;

    let codecs = [
        ffi::BLOSC_BLOSCLZ,
        ffi::BLOSC_LZ4,
        ffi::BLOSC_LZ4HC,
        ffi::BLOSC_ZLIB,
        ffi::BLOSC_ZSTD,
    ];

    for &codec in &codecs {
        unsafe {
            let mut cparams = ffi::blosc2_get_blosc2_cparams_defaults();
            cparams.compcode = codec as u8;
            cparams.clevel = 5;
            cparams.typesize = 4;
            cparams.nthreads = 1;
            cparams.splitmode = ffi::BLOSC_FORWARD_COMPAT_SPLIT as i32;
            cparams.filters[ffi::BLOSC2_MAX_FILTERS as usize - 1] = ffi::BLOSC_SHUFFLE as u8;

            let cctx = ffi::blosc2_create_cctx(cparams);
            assert!(!cctx.is_null(), "Failed to create cctx for codec {codec}");

            let mut compressed = vec![0u8; buf_size];
            let csize = ffi::blosc2_compress_ctx(
                cctx,
                data.as_ptr() as *const _,
                src_size,
                compressed.as_mut_ptr() as *mut _,
                compressed.len() as i32,
            );
            assert!(csize > 0, "Compression failed for codec {codec}: {csize}");

            let mut dparams = ffi::blosc2_get_blosc2_dparams_defaults();
            dparams.nthreads = 1;
            let dctx = ffi::blosc2_create_dctx(dparams);
            assert!(!dctx.is_null(), "Failed to create dctx for codec {codec}");

            let mut decompressed = vec![0u8; src_size as usize];
            let dsize = ffi::blosc2_decompress_ctx(
                dctx,
                compressed.as_ptr() as *const _,
                csize,
                decompressed.as_mut_ptr() as *mut _,
                decompressed.len() as i32,
            );
            assert_eq!(
                dsize, src_size,
                "Decompress size mismatch for codec {codec}"
            );
            assert_eq!(data, decompressed, "Data mismatch for codec {codec}");

            ffi::blosc2_free_ctx(cctx);
            ffi::blosc2_free_ctx(dctx);
        }
    }
}

#[test]
fn test_compress_all_filters() {
    let _b = init_blosc2();

    let data: Vec<u8> = (0..5000u64).flat_map(|i| i.to_le_bytes()).collect();
    let src_size = data.len() as i32;
    let buf_size = src_size as usize + ffi::BLOSC_EXTENDED_HEADER_LENGTH as usize;

    // TRUNC_PREC requires filters_meta to specify mantissa bits; tested separately
    let filters = [
        ffi::BLOSC_NOFILTER,
        ffi::BLOSC_SHUFFLE,
        ffi::BLOSC_BITSHUFFLE,
        ffi::BLOSC_DELTA,
    ];

    for &filter in &filters {
        unsafe {
            let mut cparams = ffi::blosc2_get_blosc2_cparams_defaults();
            cparams.compcode = ffi::BLOSC_LZ4 as u8;
            cparams.clevel = 5;
            cparams.typesize = 8;
            cparams.nthreads = 1;
            cparams.splitmode = ffi::BLOSC_FORWARD_COMPAT_SPLIT as i32;
            cparams.filters[ffi::BLOSC2_MAX_FILTERS as usize - 1] = filter as u8;

            let cctx = ffi::blosc2_create_cctx(cparams);
            assert!(!cctx.is_null(), "Failed to create cctx for filter {filter}");
            let mut compressed = vec![0u8; buf_size];
            let csize = ffi::blosc2_compress_ctx(
                cctx,
                data.as_ptr() as *const _,
                src_size,
                compressed.as_mut_ptr() as *mut _,
                compressed.len() as i32,
            );
            assert!(csize > 0, "Compression failed for filter {filter}: {csize}");

            let mut dparams = ffi::blosc2_get_blosc2_dparams_defaults();
            dparams.nthreads = 1;
            let dctx = ffi::blosc2_create_dctx(dparams);
            assert!(!dctx.is_null(), "Failed to create dctx for filter {filter}");

            let mut decompressed = vec![0u8; src_size as usize];
            let dsize = ffi::blosc2_decompress_ctx(
                dctx,
                compressed.as_ptr() as *const _,
                csize,
                decompressed.as_mut_ptr() as *mut _,
                decompressed.len() as i32,
            );
            assert_eq!(
                dsize, src_size,
                "Decompress size mismatch for filter {filter}"
            );
            assert_eq!(data, decompressed, "Data mismatch for filter {filter}");

            ffi::blosc2_free_ctx(cctx);
            ffi::blosc2_free_ctx(dctx);
        }
    }
}

#[test]
fn test_schunk_fill_special_status_counts_match_c_api() {
    let _b = init_blosc2();

    unsafe {
        let mut cparams = ffi::blosc2_get_blosc2_cparams_defaults();
        cparams.compcode = ffi::BLOSC_BLOSCLZ as u8;
        cparams.typesize = std::mem::size_of::<i32>() as i32;
        cparams.nthreads = 1;

        let mut dparams = ffi::blosc2_get_blosc2_dparams_defaults();
        dparams.nthreads = 1;

        let mut storage = ffi::blosc2_get_blosc2_storage_defaults();
        storage.cparams = &mut cparams;
        storage.dparams = &mut dparams;

        let schunk = ffi::blosc2_schunk_new(&mut storage);
        assert!(!schunk.is_null(), "Failed to create schunk");
        assert_eq!((*schunk).nchunks, 0);
        assert_eq!((*schunk).nbytes, 0);
        assert_eq!((*schunk).cbytes, 0);

        let zero_items =
            ffi::blosc2_schunk_fill_special(schunk, 0, ffi::BLOSC2_SPECIAL_LASTID as i32 + 1, 0);
        assert_eq!(
            zero_items, 0,
            "C returns before validating special value or chunksize when nitems is zero"
        );
        assert_eq!((*schunk).nchunks, 0);
        assert_eq!((*schunk).nbytes, 0);
        assert_eq!((*schunk).cbytes, 0);

        let bad_chunksize = ffi::blosc2_schunk_fill_special(
            schunk,
            1,
            ffi::BLOSC2_SPECIAL_ZERO as i32,
            cparams.typesize - 1,
        );
        assert_eq!(bad_chunksize, ffi::BLOSC2_ERROR_INVALID_PARAM as i64);

        let unsupported_special = ffi::blosc2_schunk_fill_special(
            schunk,
            1,
            ffi::BLOSC2_SPECIAL_VALUE as i32,
            cparams.typesize,
        );
        assert_eq!(unsupported_special, ffi::BLOSC2_ERROR_SCHUNK_SPECIAL as i64);

        let nchunks = ffi::blosc2_schunk_fill_special(
            schunk,
            10,
            ffi::BLOSC2_SPECIAL_ZERO as i32,
            4 * cparams.typesize,
        );
        assert_eq!(nchunks, 3);
        assert_eq!((*schunk).nchunks, 3);
        assert_eq!((*schunk).chunksize, 4 * cparams.typesize);
        assert_eq!((*schunk).nbytes, 10 * cparams.typesize as i64);
        assert_eq!(
            (*schunk).cbytes,
            3 * ffi::BLOSC_EXTENDED_HEADER_LENGTH as i64,
            "C counts in-memory special chunk headers in schunk cbytes"
        );

        for (nchunk, expected_nbytes) in [(0, 16), (1, 16), (2, 8)] {
            let mut dest = [0xffu8; 16];
            let dsize = ffi::blosc2_schunk_decompress_chunk(
                schunk,
                nchunk,
                dest.as_mut_ptr() as *mut _,
                dest.len() as i32,
            );
            assert_eq!(dsize, expected_nbytes);
            assert!(dest[..expected_nbytes as usize]
                .iter()
                .all(|&byte| byte == 0));
        }

        let refill = ffi::blosc2_schunk_fill_special(
            schunk,
            1,
            ffi::BLOSC2_SPECIAL_ZERO as i32,
            cparams.typesize,
        );
        assert_eq!(refill, ffi::BLOSC2_ERROR_FRAME_SPECIAL as i64);

        ffi::blosc2_schunk_free(schunk);
    }
}

#[test]
fn test_schunk_roundtrip() {
    let _b = init_blosc2();

    let data: Vec<u8> = (0..100000u32).flat_map(|i| i.to_le_bytes()).collect();

    unsafe {
        let mut cparams = ffi::blosc2_get_blosc2_cparams_defaults();
        cparams.compcode = ffi::BLOSC_ZSTD as u8;
        cparams.clevel = 5;
        cparams.typesize = 4;
        cparams.nthreads = 1;

        let mut dparams = ffi::blosc2_get_blosc2_dparams_defaults();
        dparams.nthreads = 1;

        let mut storage = ffi::blosc2_get_blosc2_storage_defaults();
        storage.cparams = &mut cparams;
        storage.dparams = &mut dparams;

        let schunk = ffi::blosc2_schunk_new(&mut storage);
        assert!(!schunk.is_null(), "Failed to create schunk");

        // Append data in chunks
        let chunk_size = 100_000;
        let mut offset = 0;
        while offset < data.len() {
            let end = (offset + chunk_size).min(data.len());
            let slice = &data[offset..end];
            let rc = ffi::blosc2_schunk_append_buffer(
                schunk,
                slice.as_ptr() as *const _,
                slice.len() as i32,
            );
            assert!(rc >= 0, "Append failed at offset {offset}: {rc}");
            offset = end;
        }

        // Read back all chunks
        let nchunks = (*schunk).nchunks;
        let mut restored = Vec::new();
        let mut buf = vec![0u8; chunk_size];
        for i in 0..nchunks {
            let dsize = ffi::blosc2_schunk_decompress_chunk(
                schunk,
                i,
                buf.as_mut_ptr() as *mut _,
                buf.len() as i32,
            );
            assert!(dsize > 0, "Decompress chunk {i} failed: {dsize}");
            restored.extend_from_slice(&buf[..dsize as usize]);
        }

        assert_eq!(data, restored, "Schunk roundtrip data mismatch");

        ffi::blosc2_schunk_free(schunk);
    }
}

#[test]
fn test_different_typesizes() {
    let _b = init_blosc2();

    for typesize in [1i32, 2, 4, 8] {
        let data: Vec<u8> = (0..10000u16)
            .map(|i| (i.wrapping_mul(7) & 0xFF) as u8)
            .collect();
        let src_size = data.len() as i32;
        let buf_size = src_size as usize + ffi::BLOSC_EXTENDED_HEADER_LENGTH as usize;

        let mut compressed = vec![0u8; buf_size];
        let mut decompressed = vec![0u8; src_size as usize];

        unsafe {
            let mut cparams = ffi::blosc2_get_blosc2_cparams_defaults();
            cparams.compcode = ffi::BLOSC_LZ4 as u8;
            cparams.clevel = 5;
            cparams.typesize = typesize;
            cparams.nthreads = 1;
            cparams.splitmode = ffi::BLOSC_FORWARD_COMPAT_SPLIT as i32;
            cparams.filters[ffi::BLOSC2_MAX_FILTERS as usize - 1] = ffi::BLOSC_SHUFFLE as u8;

            let cctx = ffi::blosc2_create_cctx(cparams);
            assert!(
                !cctx.is_null(),
                "Failed to create cctx for typesize={typesize}"
            );
            let csize = ffi::blosc2_compress_ctx(
                cctx,
                data.as_ptr() as *const _,
                src_size,
                compressed.as_mut_ptr() as *mut _,
                compressed.len() as i32,
            );
            assert!(csize > 0, "Compress failed for typesize={typesize}");

            let mut dparams = ffi::blosc2_get_blosc2_dparams_defaults();
            dparams.nthreads = 1;
            let dctx = ffi::blosc2_create_dctx(dparams);
            assert!(
                !dctx.is_null(),
                "Failed to create dctx for typesize={typesize}"
            );
            let dsize = ffi::blosc2_decompress_ctx(
                dctx,
                compressed.as_ptr() as *const _,
                csize,
                decompressed.as_mut_ptr() as *mut _,
                decompressed.len() as i32,
            );
            assert_eq!(
                dsize, src_size,
                "Decompress size mismatch for typesize={typesize}"
            );
            assert_eq!(data, decompressed, "Data mismatch for typesize={typesize}");

            ffi::blosc2_free_ctx(cctx);
            ffi::blosc2_free_ctx(dctx);
        }
    }
}

#[test]
fn test_compression_levels() {
    let _b = init_blosc2();

    let data: Vec<u8> = (0..20000u32).flat_map(|i| i.to_le_bytes()).collect();
    let src_size = data.len() as i32;
    let buf_size = src_size as usize + ffi::BLOSC_EXTENDED_HEADER_LENGTH as usize;

    for clevel in 0..=9u8 {
        let mut compressed = vec![0u8; buf_size];
        let mut decompressed = vec![0u8; src_size as usize];

        unsafe {
            let csize = ffi::blosc2_compress(
                clevel as i32,
                ffi::BLOSC_SHUFFLE as i32,
                4,
                data.as_ptr() as *const _,
                src_size,
                compressed.as_mut_ptr() as *mut _,
                compressed.len() as i32,
            );
            assert!(csize > 0, "Compress failed for clevel={clevel}");

            let dsize = ffi::blosc2_decompress(
                compressed.as_ptr() as *const _,
                csize,
                decompressed.as_mut_ptr() as *mut _,
                decompressed.len() as i32,
            );
            assert_eq!(
                dsize, src_size,
                "Decompress size mismatch for clevel={clevel}"
            );
            assert_eq!(data, decompressed, "Data mismatch for clevel={clevel}");
        }
    }

    for clevel in [-1, 10] {
        let mut compressed = vec![0u8; buf_size];
        let csize = unsafe {
            ffi::blosc2_compress(
                clevel,
                ffi::BLOSC_SHUFFLE as i32,
                4,
                data.as_ptr() as *const _,
                src_size,
                compressed.as_mut_ptr() as *mut _,
                compressed.len() as i32,
            )
        };
        assert_eq!(
            csize,
            ffi::BLOSC2_ERROR_CODEC_PARAM,
            "C rejects clevel={clevel} as a codec parameter error"
        );
    }
}

#[test]
fn test_getitem() {
    let _b = init_blosc2();

    let data: Vec<i32> = (0..10000).collect();
    let (compressed, csize) = unsafe { compress_i32_chunk(&data) };

    unsafe {
        // Get items 100..200
        let mut items = vec![0i32; 100];
        let rc = ffi::blosc2_getitem(
            compressed.as_ptr() as *const _,
            csize,
            100, // start
            100, // nitems
            items.as_mut_ptr() as *mut _,
            100 * 4,
        );
        assert_eq!(rc, 100 * 4, "getitem returned unexpected byte count");

        let expected: Vec<i32> = (100..200).collect();
        assert_eq!(items, expected, "getitem data mismatch");

        let mut tail = vec![0i32; 10];
        let rc = ffi::blosc2_getitem(
            compressed.as_ptr() as *const _,
            csize,
            9990,
            10,
            tail.as_mut_ptr() as *mut _,
            (tail.len() * std::mem::size_of::<i32>()) as i32,
        );
        assert_eq!(rc, 10 * std::mem::size_of::<i32>() as i32);
        let expected_tail: Vec<i32> = (9990..10000).collect();
        assert_eq!(tail, expected_tail);

        let mut empty = [0i32; 0];
        let rc = ffi::blosc2_getitem(
            compressed.as_ptr() as *const _,
            csize,
            data.len() as i32,
            0,
            empty.as_mut_ptr() as *mut _,
            0,
        );
        assert_eq!(rc, 0, "zero-length getitem at end should copy 0 bytes");

        let rc = ffi::blosc2_getitem(
            compressed.as_ptr() as *const _,
            csize,
            data.len() as i32 + 1,
            0,
            empty.as_mut_ptr() as *mut _,
            0,
        );
        assert_eq!(
            rc, 0,
            "zero-length getitem past end should match C and copy 0 bytes"
        );

        let rc = ffi::blosc2_getitem(
            compressed.as_ptr() as *const _,
            csize,
            -1,
            0,
            empty.as_mut_ptr() as *mut _,
            0,
        );
        assert_eq!(
            rc, 0,
            "zero-length getitem with a negative start should return before bounds checks"
        );

        let rc = ffi::blosc2_getitem(
            compressed.as_ptr() as *const _,
            csize,
            0,
            -1,
            empty.as_mut_ptr() as *mut _,
            0,
        );
        assert_eq!(
            rc,
            ffi::BLOSC2_ERROR_INVALID_PARAM,
            "negative nitems that make start+nitems negative should be rejected after header validation"
        );

        let rc = ffi::blosc2_getitem(
            compressed.as_ptr() as *const _,
            csize,
            1,
            -1,
            empty.as_mut_ptr() as *mut _,
            0,
        );
        assert_eq!(
            rc, 0,
            "negative nitems that leave a non-negative stop should match C and copy 0 bytes"
        );

        let rc = ffi::blosc2_getitem(
            compressed.as_ptr() as *const _,
            csize,
            data.len() as i32,
            -1,
            empty.as_mut_ptr() as *mut _,
            -1,
        );
        assert_eq!(
            rc, 0,
            "negative nitems at the end should return before negative destsize checks"
        );

        let rc = ffi::blosc2_getitem(
            compressed.as_ptr() as *const _,
            -1,
            0,
            -1,
            empty.as_mut_ptr() as *mut _,
            0,
        );
        assert_eq!(
            rc,
            ffi::BLOSC2_ERROR_READ_BUFFER,
            "getitem should validate the header before rejecting negative nitems"
        );

        let rc = ffi::blosc2_getitem(
            compressed.as_ptr() as *const _,
            csize - 1,
            0,
            -1,
            empty.as_mut_ptr() as *mut _,
            0,
        );
        assert_eq!(
            rc,
            ffi::BLOSC2_ERROR_INVALID_HEADER,
            "getitem should reject a truncated declared chunk before negative nitems behavior"
        );

        let rc = ffi::blosc2_getitem(
            compressed.as_ptr() as *const _,
            -1,
            data.len() as i32 + 1,
            0,
            empty.as_mut_ptr() as *mut _,
            0,
        );
        assert_eq!(
            rc,
            ffi::BLOSC2_ERROR_READ_BUFFER,
            "getitem should read and validate the header before zero-length item checks"
        );

        let rc = ffi::blosc2_getitem(
            compressed.as_ptr() as *const _,
            csize,
            0,
            0,
            empty.as_mut_ptr() as *mut _,
            -1,
        );
        assert_eq!(
            rc, 0,
            "zero-length getitem should return before negative destsize checks"
        );

        let mut too_small = [0i32; 9];
        let rc = ffi::blosc2_getitem(
            compressed.as_ptr() as *const _,
            csize,
            0,
            10,
            too_small.as_mut_ptr() as *mut _,
            (too_small.len() * std::mem::size_of::<i32>()) as i32,
        );
        assert_eq!(rc, ffi::BLOSC2_ERROR_WRITE_BUFFER);

        let mut one = [0i32; 1];
        let rc = ffi::blosc2_getitem(
            compressed.as_ptr() as *const _,
            csize,
            0,
            1,
            one.as_mut_ptr() as *mut _,
            -1,
        );
        assert_eq!(rc, ffi::BLOSC2_ERROR_WRITE_BUFFER);

        let rc = ffi::blosc2_getitem(
            compressed.as_ptr() as *const _,
            csize,
            data.len() as i32,
            1,
            one.as_mut_ptr() as *mut _,
            std::mem::size_of_val(&one) as i32,
        );
        assert_eq!(rc, ffi::BLOSC2_ERROR_INVALID_PARAM);

        let mut dparams = ffi::blosc2_get_blosc2_dparams_defaults();
        dparams.nthreads = 1;
        let dctx = ffi::blosc2_create_dctx(dparams);
        assert!(!dctx.is_null());

        let mut ctx_items = vec![0i32; 5];
        let rc = ffi::blosc2_getitem_ctx(
            dctx,
            compressed.as_ptr() as *const _,
            csize,
            5,
            5,
            ctx_items.as_mut_ptr() as *mut _,
            std::mem::size_of_val(ctx_items.as_slice()) as i32,
        );
        assert_eq!(rc, 5 * std::mem::size_of::<i32>() as i32);
        assert_eq!(ctx_items, vec![5, 6, 7, 8, 9]);

        let rc = ffi::blosc2_getitem_ctx(
            dctx,
            compressed.as_ptr() as *const _,
            csize,
            data.len() as i32 + 1,
            0,
            empty.as_mut_ptr() as *mut _,
            0,
        );
        assert_eq!(
            rc, 0,
            "zero-length getitem_ctx past end should match C and copy 0 bytes"
        );

        let rc = ffi::blosc2_getitem_ctx(
            dctx,
            compressed.as_ptr() as *const _,
            csize,
            -1,
            0,
            empty.as_mut_ptr() as *mut _,
            0,
        );
        assert_eq!(
            rc, 0,
            "zero-length getitem_ctx with a negative start should return before bounds checks"
        );

        let rc = ffi::blosc2_getitem_ctx(
            dctx,
            compressed.as_ptr() as *const _,
            csize,
            0,
            -1,
            empty.as_mut_ptr() as *mut _,
            0,
        );
        assert_eq!(
            rc,
            ffi::BLOSC2_ERROR_INVALID_PARAM,
            "negative nitems that make start+nitems negative should be rejected after header validation"
        );

        let rc = ffi::blosc2_getitem_ctx(
            dctx,
            compressed.as_ptr() as *const _,
            csize,
            1,
            -1,
            empty.as_mut_ptr() as *mut _,
            0,
        );
        assert_eq!(
            rc, 0,
            "negative nitems that leave a non-negative stop should match C and copy 0 bytes"
        );

        let rc = ffi::blosc2_getitem_ctx(
            dctx,
            compressed.as_ptr() as *const _,
            csize,
            data.len() as i32,
            -1,
            empty.as_mut_ptr() as *mut _,
            -1,
        );
        assert_eq!(
            rc, 0,
            "negative nitems at the end should return before negative destsize checks"
        );

        let rc = ffi::blosc2_getitem_ctx(
            dctx,
            compressed.as_ptr() as *const _,
            -1,
            0,
            -1,
            empty.as_mut_ptr() as *mut _,
            0,
        );
        assert_eq!(
            rc,
            ffi::BLOSC2_ERROR_READ_BUFFER,
            "getitem_ctx should validate the header before rejecting negative nitems"
        );

        let rc = ffi::blosc2_getitem_ctx(
            dctx,
            compressed.as_ptr() as *const _,
            csize - 1,
            0,
            -1,
            empty.as_mut_ptr() as *mut _,
            0,
        );
        assert_eq!(
            rc,
            ffi::BLOSC2_ERROR_INVALID_HEADER,
            "getitem_ctx should reject a truncated declared chunk before negative nitems behavior"
        );

        let rc = ffi::blosc2_getitem_ctx(
            dctx,
            compressed.as_ptr() as *const _,
            -1,
            data.len() as i32 + 1,
            0,
            empty.as_mut_ptr() as *mut _,
            0,
        );
        assert_eq!(
            rc,
            ffi::BLOSC2_ERROR_READ_BUFFER,
            "getitem_ctx should read and validate the header before zero-length item checks"
        );

        let rc = ffi::blosc2_getitem_ctx(
            dctx,
            compressed.as_ptr() as *const _,
            csize,
            0,
            0,
            empty.as_mut_ptr() as *mut _,
            -1,
        );
        assert_eq!(
            rc, 0,
            "zero-length getitem_ctx should return before negative destsize checks"
        );

        let rc = ffi::blosc2_getitem_ctx(
            dctx,
            compressed.as_ptr() as *const _,
            csize,
            0,
            1,
            one.as_mut_ptr() as *mut _,
            -1,
        );
        assert_eq!(rc, ffi::BLOSC2_ERROR_WRITE_BUFFER);

        ffi::blosc2_free_ctx(dctx);
    }
}
