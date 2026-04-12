#![cfg(feature = "_ffi")]
use blosc2_pure_rs::ffi;

fn init_blosc2() -> blosc2_pure_rs::Blosc2 {
    blosc2_pure_rs::Blosc2::new()
}

#[test]
fn test_compress_decompress_basic() {
    let _b = init_blosc2();

    let data: Vec<u8> = (0..10000u32)
        .flat_map(|i| i.to_le_bytes())
        .collect();
    let src_size = data.len() as i32;
    let mut compressed = vec![0u8; src_size as usize + ffi::BLOSC_EXTENDED_HEADER_LENGTH as usize];

    let csize = unsafe {
        ffi::blosc2_compress(
            5,                          // clevel
            ffi::BLOSC_SHUFFLE as i32,  // filter
            4,                          // typesize
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

    let data: Vec<u8> = (0..5000u32)
        .flat_map(|i| i.to_le_bytes())
        .collect();
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
            let mut cparams: ffi::blosc2_cparams = std::mem::zeroed();
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

            let mut dparams: ffi::blosc2_dparams = std::mem::zeroed();
            dparams.nthreads = 1;
            let dctx = ffi::blosc2_create_dctx(dparams);

            let mut decompressed = vec![0u8; src_size as usize];
            let dsize = ffi::blosc2_decompress_ctx(
                dctx,
                compressed.as_ptr() as *const _,
                csize,
                decompressed.as_mut_ptr() as *mut _,
                decompressed.len() as i32,
            );
            assert_eq!(dsize, src_size, "Decompress size mismatch for codec {codec}");
            assert_eq!(data, decompressed, "Data mismatch for codec {codec}");

            ffi::blosc2_free_ctx(cctx);
            ffi::blosc2_free_ctx(dctx);
        }
    }
}

#[test]
fn test_compress_all_filters() {
    let _b = init_blosc2();

    let data: Vec<u8> = (0..5000u64)
        .flat_map(|i| i.to_le_bytes())
        .collect();
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
            let mut cparams: ffi::blosc2_cparams = std::mem::zeroed();
            cparams.compcode = ffi::BLOSC_LZ4 as u8;
            cparams.clevel = 5;
            cparams.typesize = 8;
            cparams.nthreads = 1;
            cparams.splitmode = ffi::BLOSC_FORWARD_COMPAT_SPLIT as i32;
            cparams.filters[ffi::BLOSC2_MAX_FILTERS as usize - 1] = filter as u8;

            let cctx = ffi::blosc2_create_cctx(cparams);
            let mut compressed = vec![0u8; buf_size];
            let csize = ffi::blosc2_compress_ctx(
                cctx,
                data.as_ptr() as *const _,
                src_size,
                compressed.as_mut_ptr() as *mut _,
                compressed.len() as i32,
            );
            assert!(csize > 0, "Compression failed for filter {filter}: {csize}");

            let mut dparams: ffi::blosc2_dparams = std::mem::zeroed();
            dparams.nthreads = 1;
            let dctx = ffi::blosc2_create_dctx(dparams);

            let mut decompressed = vec![0u8; src_size as usize];
            let dsize = ffi::blosc2_decompress_ctx(
                dctx,
                compressed.as_ptr() as *const _,
                csize,
                decompressed.as_mut_ptr() as *mut _,
                decompressed.len() as i32,
            );
            assert_eq!(dsize, src_size, "Decompress size mismatch for filter {filter}");
            assert_eq!(data, decompressed, "Data mismatch for filter {filter}");

            ffi::blosc2_free_ctx(cctx);
            ffi::blosc2_free_ctx(dctx);
        }
    }
}

#[test]
fn test_schunk_roundtrip() {
    let _b = init_blosc2();

    let data: Vec<u8> = (0..100000u32)
        .flat_map(|i| i.to_le_bytes())
        .collect();

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
        let data: Vec<u8> = (0..10000u16).map(|i| (i.wrapping_mul(7) & 0xFF) as u8).collect();
        let src_size = data.len() as i32;
        let buf_size = src_size as usize + ffi::BLOSC_EXTENDED_HEADER_LENGTH as usize;

        let mut compressed = vec![0u8; buf_size];
        let mut decompressed = vec![0u8; src_size as usize];

        unsafe {
            let mut cparams: ffi::blosc2_cparams = std::mem::zeroed();
            cparams.compcode = ffi::BLOSC_LZ4 as u8;
            cparams.clevel = 5;
            cparams.typesize = typesize;
            cparams.nthreads = 1;
            cparams.splitmode = ffi::BLOSC_FORWARD_COMPAT_SPLIT as i32;
            cparams.filters[ffi::BLOSC2_MAX_FILTERS as usize - 1] = ffi::BLOSC_SHUFFLE as u8;

            let cctx = ffi::blosc2_create_cctx(cparams);
            let csize = ffi::blosc2_compress_ctx(
                cctx,
                data.as_ptr() as *const _,
                src_size,
                compressed.as_mut_ptr() as *mut _,
                compressed.len() as i32,
            );
            assert!(csize > 0, "Compress failed for typesize={typesize}");

            let mut dparams: ffi::blosc2_dparams = std::mem::zeroed();
            dparams.nthreads = 1;
            let dctx = ffi::blosc2_create_dctx(dparams);
            let dsize = ffi::blosc2_decompress_ctx(
                dctx,
                compressed.as_ptr() as *const _,
                csize,
                decompressed.as_mut_ptr() as *mut _,
                decompressed.len() as i32,
            );
            assert_eq!(dsize, src_size, "Decompress size mismatch for typesize={typesize}");
            assert_eq!(data, decompressed, "Data mismatch for typesize={typesize}");

            ffi::blosc2_free_ctx(cctx);
            ffi::blosc2_free_ctx(dctx);
        }
    }
}

#[test]
fn test_compression_levels() {
    let _b = init_blosc2();

    let data: Vec<u8> = (0..20000u32)
        .flat_map(|i| i.to_le_bytes())
        .collect();
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
            assert_eq!(dsize, src_size, "Decompress size mismatch for clevel={clevel}");
            assert_eq!(data, decompressed, "Data mismatch for clevel={clevel}");
        }
    }
}

#[test]
fn test_getitem() {
    let _b = init_blosc2();

    let data: Vec<i32> = (0..10000).collect();
    let src_size = (data.len() * 4) as i32;
    let buf_size = src_size as usize + ffi::BLOSC_EXTENDED_HEADER_LENGTH as usize;
    let mut compressed = vec![0u8; buf_size];

    unsafe {
        let csize = ffi::blosc2_compress(
            5,
            ffi::BLOSC_SHUFFLE as i32,
            4,
            data.as_ptr() as *const _,
            src_size,
            compressed.as_mut_ptr() as *mut _,
            compressed.len() as i32,
        );
        assert!(csize > 0);

        // Get items 100..200
        let mut items = vec![0i32; 100];
        let rc = ffi::blosc2_getitem(
            compressed.as_ptr() as *const _,
            csize,
            100, // start
            100, // nitems
            items.as_mut_ptr() as *mut _,
            (100 * 4) as i32,
        );
        assert!(rc > 0, "getitem failed: {rc}");

        let expected: Vec<i32> = (100..200).collect();
        assert_eq!(items, expected, "getitem data mismatch");
    }
}
