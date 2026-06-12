#![cfg(feature = "_ffi")]
//! Tier 1: Special value chunk tests
//! Tests SPECIAL_ZERO, SPECIAL_NAN, SPECIAL_UNINIT handling in compress/decompress.

use blosc2_pure_rs::compress::{
    blosc2_chunk_nans_c, blosc2_chunk_nans_with_cparams, blosc2_chunk_repeatval_c,
    blosc2_chunk_repeatval_with_cparams, blosc2_chunk_uninit_c, blosc2_chunk_uninit_with_cparams,
    blosc2_chunk_zeros_c, blosc2_chunk_zeros_with_cparams, blosc2_decompress_ctx, blosc2_getitem_c,
    blosc2_getitem_ctx_c, compress, decompress, CParams, DContext, DParams,
};
use blosc2_pure_rs::constants::*;
mod common;
use blosc2_pure_rs::header::ChunkHeader;
use common::ffi;

fn init() -> common::Blosc2 {
    common::Blosc2::new()
}

fn assert_special_header(chunk: &[u8], special: u8, nbytes: usize, cbytes: usize) {
    let header = ChunkHeader::read(chunk).unwrap();
    assert_eq!(header.special_type(), special);
    assert_eq!(header.nbytes as usize, nbytes);
    assert_eq!(header.cbytes as usize, cbytes);
    assert!(
        !header.use_dict(),
        "C special chunks ignore cparams.use_dict in the serialized header"
    );
}

fn c_zero_chunk(nbytes: usize, typesize: i32) -> Vec<u8> {
    let mut chunk = vec![0u8; BLOSC_EXTENDED_HEADER_LENGTH];
    let csize = unsafe {
        let mut cp: ffi::blosc2_cparams = std::mem::zeroed();
        cp.compcode = BLOSC_BLOSCLZ;
        cp.clevel = 5;
        cp.typesize = typesize;
        cp.nthreads = 1;
        cp.splitmode = BLOSC_FORWARD_COMPAT_SPLIT;
        cp.filters[BLOSC2_MAX_FILTERS - 1] = BLOSC_NOFILTER;
        ffi::blosc2_chunk_zeros(
            cp,
            nbytes as i32,
            chunk.as_mut_ptr() as *mut _,
            chunk.len() as i32,
        )
    };
    assert_eq!(csize, BLOSC_EXTENDED_HEADER_LENGTH as i32);
    chunk
}

fn c_nan_chunk(nbytes: usize, typesize: i32) -> Vec<u8> {
    let mut chunk = vec![0u8; BLOSC_EXTENDED_HEADER_LENGTH];
    let csize = unsafe {
        let mut cp: ffi::blosc2_cparams = std::mem::zeroed();
        cp.compcode = BLOSC_BLOSCLZ;
        cp.clevel = 5;
        cp.typesize = typesize;
        cp.nthreads = 1;
        cp.splitmode = BLOSC_FORWARD_COMPAT_SPLIT;
        cp.filters[BLOSC2_MAX_FILTERS - 1] = BLOSC_NOFILTER;
        ffi::blosc2_chunk_nans(
            cp,
            nbytes as i32,
            chunk.as_mut_ptr() as *mut _,
            chunk.len() as i32,
        )
    };
    assert_eq!(csize, BLOSC_EXTENDED_HEADER_LENGTH as i32);
    chunk
}

fn c_uninit_chunk(nbytes: usize, typesize: i32) -> Vec<u8> {
    let mut chunk = vec![0u8; BLOSC_EXTENDED_HEADER_LENGTH];
    let csize = unsafe {
        let mut cp: ffi::blosc2_cparams = std::mem::zeroed();
        cp.compcode = BLOSC_BLOSCLZ;
        cp.clevel = 5;
        cp.typesize = typesize;
        cp.nthreads = 1;
        cp.splitmode = BLOSC_FORWARD_COMPAT_SPLIT;
        cp.filters[BLOSC2_MAX_FILTERS - 1] = BLOSC_NOFILTER;
        ffi::blosc2_chunk_uninit(
            cp,
            nbytes as i32,
            chunk.as_mut_ptr() as *mut _,
            chunk.len() as i32,
        )
    };
    assert_eq!(csize, BLOSC_EXTENDED_HEADER_LENGTH as i32);
    chunk
}

fn c_repeatval_chunk(nbytes: usize, typesize: i32, repeatval: &[u8]) -> Vec<u8> {
    let mut chunk = vec![0u8; BLOSC_EXTENDED_HEADER_LENGTH + typesize as usize];
    let csize = unsafe {
        let mut cp: ffi::blosc2_cparams = std::mem::zeroed();
        cp.compcode = BLOSC_BLOSCLZ;
        cp.clevel = 5;
        cp.typesize = typesize;
        cp.nthreads = 1;
        cp.splitmode = BLOSC_FORWARD_COMPAT_SPLIT;
        cp.filters[BLOSC2_MAX_FILTERS - 1] = BLOSC_NOFILTER;
        ffi::blosc2_chunk_repeatval(
            cp,
            nbytes as i32,
            chunk.as_mut_ptr() as *mut _,
            chunk.len() as i32,
            repeatval.as_ptr() as *const _,
        )
    };
    assert_eq!(
        csize,
        (BLOSC_EXTENDED_HEADER_LENGTH + typesize as usize) as i32
    );
    chunk
}

// ─── Explicit C-style special chunk constructors ────────────────

#[test]
fn test_explicit_special_chunks_match_c_headers_and_roundtrip() {
    let cparams = CParams {
        compcode: BLOSC_BLOSCLZ,
        clevel: 9,
        typesize: 4,
        nthreads: 4,
        ..Default::default()
    };
    let nbytes = 40;

    let zeros = blosc2_chunk_zeros_with_cparams(nbytes, &cparams).unwrap();
    assert_eq!(zeros, c_zero_chunk(nbytes, cparams.typesize));
    assert_special_header(
        &zeros,
        BLOSC2_SPECIAL_ZERO,
        nbytes,
        BLOSC_EXTENDED_HEADER_LENGTH,
    );
    assert_eq!(decompress(&zeros).unwrap(), vec![0; nbytes]);

    let nans = blosc2_chunk_nans_with_cparams(nbytes, &cparams).unwrap();
    assert_eq!(nans, c_nan_chunk(nbytes, cparams.typesize));
    assert_special_header(
        &nans,
        BLOSC2_SPECIAL_NAN,
        nbytes,
        BLOSC_EXTENDED_HEADER_LENGTH,
    );
    for item in decompress(&nans).unwrap().chunks_exact(4) {
        assert!(f32::from_le_bytes(item.try_into().unwrap()).is_nan());
    }

    let uninit = blosc2_chunk_uninit_with_cparams(nbytes, &cparams).unwrap();
    assert_eq!(uninit, c_uninit_chunk(nbytes, cparams.typesize));
    assert_special_header(
        &uninit,
        BLOSC2_SPECIAL_UNINIT,
        nbytes,
        BLOSC_EXTENDED_HEADER_LENGTH,
    );
    assert_eq!(decompress(&uninit).unwrap().len(), nbytes);

    let value = 1i32.to_le_bytes();
    let repeated = blosc2_chunk_repeatval_with_cparams(nbytes, &value, &cparams).unwrap();
    assert_eq!(
        repeated,
        c_repeatval_chunk(nbytes, cparams.typesize, &value)
    );
    assert_special_header(
        &repeated,
        BLOSC2_SPECIAL_VALUE,
        nbytes,
        BLOSC_EXTENDED_HEADER_LENGTH + cparams.typesize as usize,
    );
    assert_eq!(&repeated[BLOSC_EXTENDED_HEADER_LENGTH..], value);
    for item in decompress(&repeated).unwrap().chunks_exact(4) {
        assert_eq!(item, value);
    }
}

#[test]
fn test_special_repeatval_c_adapter_copies_cparams_typesize_prefix_like_c() {
    let _b = init();

    let cparams = CParams {
        compcode: BLOSC_BLOSCLZ,
        clevel: 5,
        typesize: 4,
        nthreads: 1,
        filters: [0, 0, 0, 0, 0, BLOSC_NOFILTER],
        ..Default::default()
    };
    let nbytes = 16usize;
    let repeatval = [0x01, 0x02, 0x03, 0x04, 0xaa, 0xbb, 0xcc, 0xdd];
    let expected_value = &repeatval[..cparams.typesize as usize];
    let c_chunk = c_repeatval_chunk(nbytes, cparams.typesize, &repeatval);

    let mut rust_chunk = vec![0u8; BLOSC_EXTENDED_HEADER_LENGTH + cparams.typesize as usize];
    let rust_csize = blosc2_chunk_repeatval_c(
        cparams.clone(),
        nbytes as i32,
        &mut rust_chunk,
        (BLOSC_EXTENDED_HEADER_LENGTH + cparams.typesize as usize) as i32,
        &repeatval,
    );

    assert_eq!(
        rust_csize,
        (BLOSC_EXTENDED_HEADER_LENGTH + cparams.typesize as usize) as i32
    );
    assert_eq!(rust_chunk, c_chunk);
    assert_special_header(
        &rust_chunk,
        BLOSC2_SPECIAL_VALUE,
        nbytes,
        BLOSC_EXTENDED_HEADER_LENGTH + cparams.typesize as usize,
    );
    assert_eq!(&rust_chunk[BLOSC_EXTENDED_HEADER_LENGTH..], expected_value);
    for item in decompress(&rust_chunk)
        .unwrap()
        .chunks_exact(expected_value.len())
    {
        assert_eq!(item, expected_value);
    }
}

#[test]
fn test_special_uninit_decompress_and_getitem_preserve_dest_like_c() {
    let _b = init();

    let nbytes = 64usize;
    let cparams = CParams {
        compcode: BLOSC_BLOSCLZ,
        clevel: 5,
        typesize: 4,
        nthreads: 1,
        filters: [0, 0, 0, 0, 0, BLOSC_NOFILTER],
        ..Default::default()
    };
    let c_chunk = c_uninit_chunk(nbytes, cparams.typesize);
    let rust_chunk = blosc2_chunk_uninit_with_cparams(nbytes, &cparams).unwrap();
    assert_eq!(rust_chunk, c_chunk);
    assert_special_header(
        &rust_chunk,
        BLOSC2_SPECIAL_UNINIT,
        nbytes,
        BLOSC_EXTENDED_HEADER_LENGTH,
    );

    let dctx = DContext::new(DParams {
        nthreads: 1,
        ..Default::default()
    });

    for chunk in [&c_chunk, &rust_chunk] {
        let mut c_restored = vec![0xa5; nbytes];
        let c_dsize = unsafe {
            ffi::blosc2_decompress(
                chunk.as_ptr() as *const _,
                chunk.len() as i32,
                c_restored.as_mut_ptr() as *mut _,
                c_restored.len() as i32,
            )
        };
        assert_eq!(c_dsize, nbytes as i32);
        assert_eq!(
            c_restored,
            vec![0xa5; nbytes],
            "C leaves SPECIAL_UNINIT decompression destination untouched"
        );

        let mut rust_restored = vec![0x5a; nbytes];
        let rust_dsize = blosc2_pure_rs::compress::blosc2_decompress(
            chunk,
            chunk.len() as i32,
            &mut rust_restored,
            nbytes as i32,
        );
        assert_eq!(rust_dsize, nbytes as i32);
        assert_eq!(
            rust_restored,
            vec![0x5a; nbytes],
            "Rust C-style decompression adapter should mirror C for SPECIAL_UNINIT"
        );

        let mut rust_ctx_restored = vec![0x3c; nbytes];
        let rust_ctx_dsize = blosc2_decompress_ctx(
            &dctx,
            chunk,
            chunk.len() as i32,
            &mut rust_ctx_restored,
            nbytes as i32,
        );
        assert_eq!(rust_ctx_dsize, nbytes as i32);
        assert_eq!(
            rust_ctx_restored,
            vec![0x3c; nbytes],
            "Rust C-style context decompression adapter should mirror C for SPECIAL_UNINIT"
        );

        let mut c_item = [0xa5; 2 * std::mem::size_of::<i32>()];
        let c_item_size = unsafe {
            ffi::blosc2_getitem(
                chunk.as_ptr() as *const _,
                chunk.len() as i32,
                3,
                2,
                c_item.as_mut_ptr() as *mut _,
                c_item.len() as i32,
            )
        };
        assert_eq!(c_item_size, c_item.len() as i32);
        assert_eq!(c_item, [0xa5; 2 * std::mem::size_of::<i32>()]);

        let mut rust_item = [0x5a; 2 * std::mem::size_of::<i32>()];
        let rust_item_len = rust_item.len() as i32;
        assert_eq!(
            blosc2_getitem_c(
                chunk,
                chunk.len() as i32,
                3,
                2,
                &mut rust_item,
                rust_item_len,
            ),
            rust_item_len
        );
        assert_eq!(rust_item, [0x5a; 2 * std::mem::size_of::<i32>()]);

        let mut rust_ctx_item = [0x3c; 2 * std::mem::size_of::<i32>()];
        let rust_ctx_item_len = rust_ctx_item.len() as i32;
        assert_eq!(
            blosc2_getitem_ctx_c(
                &dctx,
                chunk,
                chunk.len() as i32,
                3,
                2,
                &mut rust_ctx_item,
                rust_ctx_item_len,
            ),
            rust_ctx_item_len
        );
        assert_eq!(rust_ctx_item, [0x3c; 2 * std::mem::size_of::<i32>()]);

        assert_eq!(decompress(chunk).unwrap().len(), nbytes);
    }
}

#[test]
fn test_special_value_use_dict_ignored_like_c_regression() {
    let repeatval = 3.14f32.to_le_bytes();
    let nbytes = 1000 * std::mem::size_of::<f32>() as i32;

    for compcode in [BLOSC_BLOSCLZ, BLOSC_LZ4, BLOSC_ZLIB] {
        let cparams = CParams {
            compcode,
            clevel: 5,
            typesize: 4,
            use_dict: true,
            nthreads: 1,
            ..Default::default()
        };
        let mut dest = vec![0u8; BLOSC_EXTENDED_HEADER_LENGTH + repeatval.len()];

        let ret = blosc2_chunk_zeros_c(
            cparams.clone(),
            nbytes,
            &mut dest,
            BLOSC_EXTENDED_HEADER_LENGTH as i32,
        );
        assert_eq!(ret, BLOSC_EXTENDED_HEADER_LENGTH as i32);
        assert!(!ChunkHeader::read(&dest[..ret as usize]).unwrap().use_dict());

        let ret = blosc2_chunk_uninit_c(
            cparams.clone(),
            nbytes,
            &mut dest,
            BLOSC_EXTENDED_HEADER_LENGTH as i32,
        );
        assert_eq!(ret, BLOSC_EXTENDED_HEADER_LENGTH as i32);
        assert!(!ChunkHeader::read(&dest[..ret as usize]).unwrap().use_dict());

        let ret = blosc2_chunk_nans_c(
            cparams.clone(),
            nbytes,
            &mut dest,
            BLOSC_EXTENDED_HEADER_LENGTH as i32,
        );
        assert_eq!(ret, BLOSC_EXTENDED_HEADER_LENGTH as i32);
        assert!(!ChunkHeader::read(&dest[..ret as usize]).unwrap().use_dict());

        let ret = blosc2_chunk_repeatval_c(
            cparams,
            nbytes,
            &mut dest,
            (BLOSC_EXTENDED_HEADER_LENGTH + repeatval.len()) as i32,
            &repeatval,
        );
        assert_eq!(ret, (BLOSC_EXTENDED_HEADER_LENGTH + repeatval.len()) as i32);
        assert!(!ChunkHeader::read(&dest[..ret as usize]).unwrap().use_dict());
    }
}

#[test]
fn test_special_chunk_c_adapter_error_returns_match_c() {
    let cparams = CParams {
        compcode: BLOSC_BLOSCLZ,
        clevel: 5,
        typesize: 4,
        nthreads: 1,
        ..Default::default()
    };
    let repeatval = 3.14f32.to_le_bytes();
    let mut dest = vec![0u8; BLOSC_EXTENDED_HEADER_LENGTH + repeatval.len()];
    let destsize = dest.len() as i32;

    let short_special_dest = (BLOSC_EXTENDED_HEADER_LENGTH - 1) as i32;
    assert_eq!(
        blosc2_chunk_zeros_c(cparams.clone(), 16, &mut dest, short_special_dest),
        BLOSC2_ERROR_DATA
    );
    assert_eq!(
        blosc2_chunk_uninit_c(cparams.clone(), 16, &mut dest, short_special_dest),
        BLOSC2_ERROR_DATA
    );
    assert_eq!(
        blosc2_chunk_nans_c(cparams.clone(), 16, &mut dest, short_special_dest),
        BLOSC2_ERROR_DATA
    );
    assert_eq!(
        blosc2_chunk_repeatval_c(
            cparams.clone(),
            16,
            &mut dest,
            BLOSC_EXTENDED_HEADER_LENGTH as i32,
            &repeatval,
        ),
        BLOSC2_ERROR_DATA
    );

    for nbytes in [1, 10] {
        assert_eq!(
            blosc2_chunk_zeros_c(cparams.clone(), nbytes, &mut dest, destsize),
            BLOSC2_ERROR_DATA
        );
        assert_eq!(
            blosc2_chunk_uninit_c(cparams.clone(), nbytes, &mut dest, destsize),
            BLOSC2_ERROR_DATA
        );
        assert_eq!(
            blosc2_chunk_nans_c(cparams.clone(), nbytes, &mut dest, destsize),
            BLOSC2_ERROR_DATA
        );
        assert_eq!(
            blosc2_chunk_repeatval_c(cparams.clone(), nbytes, &mut dest, destsize, &repeatval,),
            BLOSC2_ERROR_DATA
        );
    }
}

#[test]
fn test_special_chunk_zero_length_constructor_returns_match_c() {
    let _b = init();

    let cparams = CParams {
        compcode: BLOSC_BLOSCLZ,
        clevel: 5,
        typesize: 4,
        nthreads: 1,
        filters: [0, 0, 0, 0, 0, BLOSC_NOFILTER],
        ..Default::default()
    };
    let mut c_dest = vec![0u8; BLOSC_EXTENDED_HEADER_LENGTH + cparams.typesize as usize];
    let mut rust_dest = c_dest.clone();
    let c_destsize = c_dest.len() as i32;
    let repeatval = 0x0102_0304i32.to_le_bytes();
    let expected_repeat_cbytes = (BLOSC_EXTENDED_HEADER_LENGTH + cparams.typesize as usize) as i32;

    let mut c_cp: ffi::blosc2_cparams = unsafe { std::mem::zeroed() };
    c_cp.compcode = cparams.compcode;
    c_cp.clevel = cparams.clevel;
    c_cp.typesize = cparams.typesize;
    c_cp.nthreads = cparams.nthreads;
    c_cp.splitmode = BLOSC_FORWARD_COMPAT_SPLIT;
    c_cp.filters[BLOSC2_MAX_FILTERS - 1] = BLOSC_NOFILTER;

    let c_zeros = unsafe {
        ffi::blosc2_chunk_zeros(
            c_cp,
            0,
            c_dest.as_mut_ptr() as *mut _,
            BLOSC_EXTENDED_HEADER_LENGTH as i32,
        )
    };
    let rust_zeros = blosc2_chunk_zeros_c(
        cparams.clone(),
        0,
        &mut rust_dest,
        BLOSC_EXTENDED_HEADER_LENGTH as i32,
    );
    assert_eq!(rust_zeros, c_zeros);
    assert_eq!(rust_zeros, BLOSC_EXTENDED_HEADER_LENGTH as i32);
    assert_special_header(
        &rust_dest[..rust_zeros as usize],
        BLOSC2_SPECIAL_ZERO,
        0,
        BLOSC_EXTENDED_HEADER_LENGTH,
    );

    let c_uninit =
        unsafe { ffi::blosc2_chunk_uninit(c_cp, 0, c_dest.as_mut_ptr() as *mut _, c_destsize) };
    assert_eq!(
        blosc2_chunk_uninit_c(cparams.clone(), 0, &mut rust_dest, c_destsize),
        c_uninit
    );
    assert_eq!(c_uninit, BLOSC_EXTENDED_HEADER_LENGTH as i32);

    let c_nans =
        unsafe { ffi::blosc2_chunk_nans(c_cp, 0, c_dest.as_mut_ptr() as *mut _, c_destsize) };
    assert_eq!(
        blosc2_chunk_nans_c(cparams.clone(), 0, &mut rust_dest, c_destsize),
        c_nans
    );
    assert_eq!(c_nans, BLOSC_EXTENDED_HEADER_LENGTH as i32);

    let c_repeatval = unsafe {
        ffi::blosc2_chunk_repeatval(
            c_cp,
            0,
            c_dest.as_mut_ptr() as *mut _,
            c_destsize,
            repeatval.as_ptr() as *const _,
        )
    };
    assert_eq!(
        blosc2_chunk_repeatval_c(cparams, 0, &mut rust_dest, c_destsize, &repeatval),
        c_repeatval
    );
    assert_eq!(c_repeatval, expected_repeat_cbytes);
}

#[test]
fn test_special_chunk_negative_nbytes_constructor_returns_match_c() {
    let _b = init();

    let cparams = CParams {
        compcode: BLOSC_BLOSCLZ,
        clevel: 5,
        typesize: 4,
        nthreads: 1,
        filters: [0, 0, 0, 0, 0, BLOSC_NOFILTER],
        ..Default::default()
    };
    let mut c_dest = vec![0u8; BLOSC_EXTENDED_HEADER_LENGTH + cparams.typesize as usize];
    let mut rust_dest = c_dest.clone();
    let c_destsize = c_dest.len() as i32;
    let repeatval = 0x0102_0304i32.to_le_bytes();

    let mut c_cp: ffi::blosc2_cparams = unsafe { std::mem::zeroed() };
    c_cp.compcode = cparams.compcode;
    c_cp.clevel = cparams.clevel;
    c_cp.typesize = cparams.typesize;
    c_cp.nthreads = cparams.nthreads;
    c_cp.splitmode = BLOSC_FORWARD_COMPAT_SPLIT;
    c_cp.filters[BLOSC2_MAX_FILTERS - 1] = BLOSC_NOFILTER;

    let c_zeros = unsafe {
        ffi::blosc2_chunk_zeros(
            c_cp,
            -1,
            c_dest.as_mut_ptr() as *mut _,
            BLOSC_EXTENDED_HEADER_LENGTH as i32,
        )
    };
    let rust_zeros = blosc2_chunk_zeros_c(
        cparams.clone(),
        -1,
        &mut rust_dest,
        BLOSC_EXTENDED_HEADER_LENGTH as i32,
    );
    assert_eq!(rust_zeros, c_zeros);
    assert_eq!(rust_zeros, BLOSC_EXTENDED_HEADER_LENGTH as i32);
    assert_eq!(
        &rust_dest[..rust_zeros as usize],
        &c_dest[..c_zeros as usize]
    );
    let header = ChunkHeader::read_minimal(&rust_dest[..rust_zeros as usize]).unwrap();
    assert_eq!(header.nbytes, -1);
    assert_eq!(
        rust_dest[BLOSC2_CHUNK_BLOSC2_FLAGS],
        BLOSC2_SPECIAL_ZERO << 4
    );

    for (nbytes, expected_special) in [
        (-1, BLOSC2_SPECIAL_UNINIT),
        (-1, BLOSC2_SPECIAL_NAN),
        (-1, BLOSC2_SPECIAL_VALUE),
        (-4, BLOSC2_SPECIAL_UNINIT),
        (-4, BLOSC2_SPECIAL_NAN),
        (-4, BLOSC2_SPECIAL_VALUE),
    ] {
        let c_ret = match expected_special {
            BLOSC2_SPECIAL_UNINIT => unsafe {
                ffi::blosc2_chunk_uninit(c_cp, nbytes, c_dest.as_mut_ptr() as *mut _, c_destsize)
            },
            BLOSC2_SPECIAL_NAN => unsafe {
                ffi::blosc2_chunk_nans(c_cp, nbytes, c_dest.as_mut_ptr() as *mut _, c_destsize)
            },
            BLOSC2_SPECIAL_VALUE => unsafe {
                ffi::blosc2_chunk_repeatval(
                    c_cp,
                    nbytes,
                    c_dest.as_mut_ptr() as *mut _,
                    c_destsize,
                    repeatval.as_ptr() as *const _,
                )
            },
            _ => unreachable!(),
        };
        let rust_ret = match expected_special {
            BLOSC2_SPECIAL_UNINIT => {
                blosc2_chunk_uninit_c(cparams.clone(), nbytes, &mut rust_dest, c_destsize)
            }
            BLOSC2_SPECIAL_NAN => {
                blosc2_chunk_nans_c(cparams.clone(), nbytes, &mut rust_dest, c_destsize)
            }
            BLOSC2_SPECIAL_VALUE => blosc2_chunk_repeatval_c(
                cparams.clone(),
                nbytes,
                &mut rust_dest,
                c_destsize,
                &repeatval,
            ),
            _ => unreachable!(),
        };
        assert_eq!(
            rust_ret, c_ret,
            "negative nbytes mismatch for special={expected_special}, nbytes={nbytes}"
        );
        if rust_ret > 0 {
            assert_eq!(
                &rust_dest[..rust_ret as usize],
                &c_dest[..c_ret as usize],
                "negative nbytes chunk mismatch for special={expected_special}, nbytes={nbytes}"
            );
            let header = ChunkHeader::read_minimal(&rust_dest[..rust_ret as usize]).unwrap();
            assert_eq!(header.nbytes, nbytes);
            assert_eq!(rust_dest[BLOSC2_CHUNK_BLOSC2_FLAGS], expected_special << 4);
        }
    }
}

#[test]
fn test_special_chunk_getitem_offsets_leftovers_and_oob_match_c() {
    let cparams = CParams {
        compcode: BLOSC_BLOSCLZ,
        clevel: 9,
        typesize: 4,
        nthreads: 4,
        ..Default::default()
    };
    let repeat_value = 0x0102_0304i32.to_le_bytes();
    let full_items = 16usize;
    let leftover_items = 5usize;
    let full_nbytes = full_items * std::mem::size_of::<i32>();
    let leftover_nbytes = leftover_items * std::mem::size_of::<i32>();
    let dctx = DContext::new(DParams {
        nthreads: 4,
        ..Default::default()
    });

    for nitems in [full_items, leftover_items] {
        let nbytes = nitems * std::mem::size_of::<i32>();
        let chunks = [
            (
                BLOSC2_SPECIAL_ZERO,
                blosc2_chunk_zeros_with_cparams(nbytes, &cparams).unwrap(),
            ),
            (
                BLOSC2_SPECIAL_NAN,
                blosc2_chunk_nans_with_cparams(nbytes, &cparams).unwrap(),
            ),
            (
                BLOSC2_SPECIAL_UNINIT,
                blosc2_chunk_uninit_with_cparams(nbytes, &cparams).unwrap(),
            ),
            (
                BLOSC2_SPECIAL_VALUE,
                blosc2_chunk_repeatval_with_cparams(nbytes, &repeat_value, &cparams).unwrap(),
            ),
        ];

        for (special, chunk) in chunks {
            let expected_cbytes = if special == BLOSC2_SPECIAL_VALUE {
                BLOSC_EXTENDED_HEADER_LENGTH + cparams.typesize as usize
            } else {
                BLOSC_EXTENDED_HEADER_LENGTH
            };
            assert_special_header(&chunk, special, nbytes, expected_cbytes);

            for start in [0usize, nitems / 2, nitems - 1] {
                let plain_sentinel = [0xa5; std::mem::size_of::<i32>()];
                let ctx_sentinel = [0x5a; std::mem::size_of::<i32>()];
                let mut plain_item = plain_sentinel;
                let plain_len = plain_item.len() as i32;
                assert_eq!(
                    blosc2_getitem_c(
                        &chunk,
                        chunk.len() as i32,
                        start as i32,
                        1,
                        &mut plain_item,
                        plain_len,
                    ),
                    plain_len,
                    "getitem failed for special={special}, nitems={nitems}, start={start}"
                );

                let mut ctx_item = ctx_sentinel;
                let ctx_len = ctx_item.len() as i32;
                assert_eq!(
                    blosc2_getitem_ctx_c(
                        &dctx,
                        &chunk,
                        chunk.len() as i32,
                        start as i32,
                        1,
                        &mut ctx_item,
                        ctx_len,
                    ),
                    ctx_len,
                    "getitem_ctx failed for special={special}, nitems={nitems}, start={start}"
                );

                match special {
                    BLOSC2_SPECIAL_ZERO => {
                        assert_eq!(plain_item, [0; std::mem::size_of::<i32>()]);
                        assert_eq!(ctx_item, [0; std::mem::size_of::<i32>()]);
                    }
                    BLOSC2_SPECIAL_NAN => {
                        assert!(f32::from_le_bytes(plain_item).is_nan());
                        assert!(f32::from_le_bytes(ctx_item).is_nan());
                    }
                    BLOSC2_SPECIAL_VALUE => {
                        assert_eq!(plain_item, repeat_value);
                        assert_eq!(ctx_item, repeat_value);
                    }
                    BLOSC2_SPECIAL_UNINIT => {
                        assert_eq!(plain_item, plain_sentinel);
                        assert_eq!(ctx_item, ctx_sentinel);
                    }
                    _ => unreachable!(),
                }
            }

            let mut overflow = [0u8; 2 * std::mem::size_of::<i32>()];
            let overflow_len = overflow.len() as i32;
            let mut zero_items_dest = [0u8; std::mem::size_of::<i32>()];
            assert_eq!(
                blosc2_getitem_c(
                    &chunk,
                    chunk.len() as i32,
                    nitems as i32 + 1,
                    0,
                    &mut zero_items_dest,
                    -1,
                ),
                0,
                "C returns before validating start/destsize for zero-item getitem"
            );
            assert_eq!(
                blosc2_getitem_ctx_c(
                    &dctx,
                    &chunk,
                    chunk.len() as i32,
                    -1,
                    0,
                    &mut zero_items_dest,
                    -1,
                ),
                0,
                "C returns before validating start/destsize for zero-item getitem_ctx"
            );

            let mut too_small = [0u8; std::mem::size_of::<i32>()];
            let too_small_len = too_small.len() as i32;
            assert_eq!(
                blosc2_getitem_c(
                    &chunk,
                    chunk.len() as i32,
                    nitems as i32 - 1,
                    2,
                    &mut too_small,
                    too_small_len,
                ),
                BLOSC2_ERROR_WRITE_BUFFER,
                "C checks destsize before start+nitems range bounds"
            );
            assert_eq!(
                blosc2_getitem_ctx_c(
                    &dctx,
                    &chunk,
                    chunk.len() as i32,
                    nitems as i32 - 1,
                    2,
                    &mut too_small,
                    too_small_len,
                ),
                BLOSC2_ERROR_WRITE_BUFFER,
                "C checks destsize before start+nitems range bounds"
            );

            for (start, requested_items) in [(nitems as i32, 1), (nitems as i32 - 1, 2)] {
                assert_eq!(
                    blosc2_getitem_c(
                        &chunk,
                        chunk.len() as i32,
                        start,
                        requested_items,
                        &mut overflow,
                        overflow_len,
                    ),
                    BLOSC2_ERROR_INVALID_PARAM
                );
                assert_eq!(
                    blosc2_getitem_ctx_c(
                        &dctx,
                        &chunk,
                        chunk.len() as i32,
                        start,
                        requested_items,
                        &mut overflow,
                        overflow_len,
                    ),
                    BLOSC2_ERROR_INVALID_PARAM
                );
            }
        }
    }

    assert_eq!(full_nbytes, 64);
    assert_eq!(leftover_nbytes, 20);
}

#[test]
fn test_special_nan_materialized_bytes_match_c() {
    let _b = init();

    for typesize in [4usize, 8] {
        let nitems = 5usize;
        let nbytes = nitems * typesize;
        let c_chunk = c_nan_chunk(nbytes, typesize as i32);
        let cparams = CParams {
            compcode: BLOSC_BLOSCLZ,
            clevel: 5,
            typesize: typesize as i32,
            nthreads: 1,
            filters: [0, 0, 0, 0, 0, BLOSC_NOFILTER],
            ..Default::default()
        };
        let rust_chunk = blosc2_chunk_nans_with_cparams(nbytes, &cparams).unwrap();
        assert_eq!(rust_chunk, c_chunk);
        assert_special_header(
            &rust_chunk,
            BLOSC2_SPECIAL_NAN,
            nbytes,
            BLOSC_EXTENDED_HEADER_LENGTH,
        );

        let mut c_restored = vec![0u8; nbytes];
        let c_dsize = unsafe {
            ffi::blosc2_decompress(
                c_chunk.as_ptr() as *const _,
                c_chunk.len() as i32,
                c_restored.as_mut_ptr() as *mut _,
                c_restored.len() as i32,
            )
        };
        assert_eq!(c_dsize, nbytes as i32);

        let mut c_restored_from_rust = vec![0u8; nbytes];
        let c_rust_dsize = unsafe {
            ffi::blosc2_decompress(
                rust_chunk.as_ptr() as *const _,
                rust_chunk.len() as i32,
                c_restored_from_rust.as_mut_ptr() as *mut _,
                c_restored_from_rust.len() as i32,
            )
        };
        assert_eq!(c_rust_dsize, nbytes as i32);
        assert_eq!(c_restored_from_rust, c_restored);
        assert_eq!(decompress(&c_chunk).unwrap(), c_restored);
        assert_eq!(decompress(&rust_chunk).unwrap(), c_restored);

        let mut c_item = vec![0u8; typesize];
        let c_item_size = unsafe {
            ffi::blosc2_getitem(
                c_chunk.as_ptr() as *const _,
                c_chunk.len() as i32,
                2,
                1,
                c_item.as_mut_ptr() as *mut _,
                c_item.len() as i32,
            )
        };
        assert_eq!(c_item_size, typesize as i32);

        let mut c_item_from_rust = vec![0u8; typesize];
        let c_rust_item_size = unsafe {
            ffi::blosc2_getitem(
                rust_chunk.as_ptr() as *const _,
                rust_chunk.len() as i32,
                2,
                1,
                c_item_from_rust.as_mut_ptr() as *mut _,
                c_item_from_rust.len() as i32,
            )
        };
        assert_eq!(c_rust_item_size, typesize as i32);
        assert_eq!(c_item_from_rust, c_item);

        let mut rust_item_from_c = vec![0u8; typesize];
        assert_eq!(
            blosc2_getitem_c(
                &c_chunk,
                c_chunk.len() as i32,
                2,
                1,
                &mut rust_item_from_c,
                typesize as i32,
            ),
            typesize as i32
        );
        assert_eq!(rust_item_from_c, c_item);

        let mut rust_item = vec![0u8; typesize];
        assert_eq!(
            blosc2_getitem_c(
                &rust_chunk,
                rust_chunk.len() as i32,
                2,
                1,
                &mut rust_item,
                typesize as i32,
            ),
            typesize as i32
        );
        assert_eq!(rust_item, c_item);

        let dctx = DContext::new(DParams {
            nthreads: 1,
            ..Default::default()
        });
        let mut rust_ctx_item_from_c = vec![0u8; typesize];
        assert_eq!(
            blosc2_getitem_ctx_c(
                &dctx,
                &c_chunk,
                c_chunk.len() as i32,
                2,
                1,
                &mut rust_ctx_item_from_c,
                typesize as i32,
            ),
            typesize as i32
        );
        assert_eq!(rust_ctx_item_from_c, c_item);

        let mut rust_ctx_item = vec![0u8; typesize];
        assert_eq!(
            blosc2_getitem_ctx_c(
                &dctx,
                &rust_chunk,
                rust_chunk.len() as i32,
                2,
                1,
                &mut rust_ctx_item,
                typesize as i32,
            ),
            typesize as i32
        );
        assert_eq!(rust_ctx_item, c_item);
    }
}

// ─── All-zero data ───────────────────────────────────────────────

#[test]
fn test_compress_all_zeros_detected() {
    let data = vec![0u8; 40000];
    let cparams = CParams {
        compcode: BLOSC_LZ4,
        clevel: 5,
        typesize: 4,
        ..Default::default()
    };
    let chunk = compress(&data, &cparams).unwrap();
    let header = ChunkHeader::read(&chunk).unwrap();

    assert_eq!(
        header.special_type(),
        BLOSC2_SPECIAL_ZERO,
        "All-zero data should produce SPECIAL_ZERO chunk"
    );
    // Special zero chunks should be very small (just the header)
    assert!(
        chunk.len() <= BLOSC_EXTENDED_HEADER_LENGTH + 8,
        "SPECIAL_ZERO chunk should be tiny, got {} bytes",
        chunk.len()
    );

    let restored = decompress(&chunk).unwrap();
    assert_eq!(data, restored);
}

#[test]
fn test_compress_all_zeros_various_typesizes() {
    for typesize in [1, 2, 4, 8, 16] {
        let data = vec![0u8; 10000];
        let cparams = CParams {
            compcode: BLOSC_BLOSCLZ,
            clevel: 5,
            typesize,
            ..Default::default()
        };
        let chunk = compress(&data, &cparams).unwrap();
        let restored = decompress(&chunk).unwrap();
        assert_eq!(
            data, restored,
            "Zero roundtrip failed for typesize={typesize}"
        );
    }
}

#[test]
fn test_c_compressed_zeros_rust_decompress() {
    let _b = init();
    let data = vec![0u8; 20000];

    // Compress with C
    let mut c_chunk = vec![0u8; data.len() + BLOSC2_MAX_OVERHEAD];
    let csize = unsafe {
        let mut cp: ffi::blosc2_cparams = std::mem::zeroed();
        cp.compcode = BLOSC_LZ4;
        cp.clevel = 5;
        cp.typesize = 4;
        cp.nthreads = 1;
        cp.splitmode = BLOSC_FORWARD_COMPAT_SPLIT;
        cp.filters[BLOSC2_MAX_FILTERS - 1] = BLOSC_SHUFFLE;
        let cctx = ffi::blosc2_create_cctx(cp);
        let r = ffi::blosc2_compress_ctx(
            cctx,
            data.as_ptr() as *const _,
            data.len() as i32,
            c_chunk.as_mut_ptr() as *mut _,
            c_chunk.len() as i32,
        );
        ffi::blosc2_free_ctx(cctx);
        r
    };
    assert!(csize > 0, "C compression of zeros failed");

    // Decompress with Rust
    let restored = decompress(&c_chunk[..csize as usize]).unwrap();
    assert_eq!(
        data, restored,
        "C-compressed zeros → Rust decompress mismatch"
    );
}

// ─── All-NaN data ────────────────────────────────────────────────

#[test]
fn test_compress_all_nan_f32() {
    let nan_val = f32::NAN;
    let data: Vec<u8> = std::iter::repeat_n(nan_val.to_le_bytes(), 5000)
        .flatten()
        .collect();

    let cparams = CParams {
        compcode: BLOSC_LZ4,
        clevel: 5,
        typesize: 4,
        ..Default::default()
    };
    let chunk = compress(&data, &cparams).unwrap();
    let restored = decompress(&chunk).unwrap();

    // NaN != NaN, so compare bytes
    // The restored data should have NaN pattern in every 4 bytes
    assert_eq!(data.len(), restored.len());
    for i in (0..restored.len()).step_by(4) {
        let val = f32::from_le_bytes(restored[i..i + 4].try_into().unwrap());
        assert!(val.is_nan(), "Expected NaN at offset {i}, got {val}");
    }
}

#[test]
fn test_compress_all_nan_f64() {
    let nan_val = f64::NAN;
    let data: Vec<u8> = std::iter::repeat_n(nan_val.to_le_bytes(), 2500)
        .flatten()
        .collect();

    let cparams = CParams {
        compcode: BLOSC_ZSTD,
        clevel: 5,
        typesize: 8,
        ..Default::default()
    };
    let chunk = compress(&data, &cparams).unwrap();
    let restored = decompress(&chunk).unwrap();

    assert_eq!(data.len(), restored.len());
    for i in (0..restored.len()).step_by(8) {
        let val = f64::from_le_bytes(restored[i..i + 8].try_into().unwrap());
        assert!(val.is_nan(), "Expected NaN at offset {i}");
    }
}

// ─── Repeated non-zero value ─────────────────────────────────────

#[test]
fn test_compress_repeated_byte() {
    let data = vec![0xABu8; 20000];
    let cparams = CParams {
        compcode: BLOSC_LZ4,
        clevel: 5,
        typesize: 1,
        ..Default::default()
    };
    let chunk = compress(&data, &cparams).unwrap();
    let restored = decompress(&chunk).unwrap();
    assert_eq!(data, restored);
}

#[test]
fn test_compress_repeated_u32() {
    let val: u32 = 0xDEADBEEF;
    let data: Vec<u8> = std::iter::repeat_n(val.to_le_bytes(), 5000)
        .flatten()
        .collect();

    let cparams = CParams {
        compcode: BLOSC_BLOSCLZ,
        clevel: 9,
        typesize: 4,
        filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
        ..Default::default()
    };
    let chunk = compress(&data, &cparams).unwrap();
    let restored = decompress(&chunk).unwrap();
    assert_eq!(data, restored);
}

// ─── Mixed data patterns ────────────────────────────────────────

#[test]
fn test_mostly_zeros_some_nonzero() {
    let mut data = vec![0u8; 20000];
    data[100] = 1;
    data[5000] = 0xFF;
    data[19999] = 42;

    let cparams = CParams {
        compcode: BLOSC_LZ4,
        clevel: 5,
        typesize: 4,
        ..Default::default()
    };
    let chunk = compress(&data, &cparams).unwrap();
    let header = ChunkHeader::read(&chunk).unwrap();

    // Should NOT be SPECIAL_ZERO since there are non-zero bytes
    assert_ne!(
        header.special_type(),
        BLOSC2_SPECIAL_ZERO,
        "Mixed data should not be SPECIAL_ZERO"
    );

    let restored = decompress(&chunk).unwrap();
    assert_eq!(data, restored);
}

// ─── Schunk with special value chunks ────────────────────────────

#[test]
fn test_schunk_with_zero_chunks() {
    use blosc2_pure_rs::compress::DParams;
    use blosc2_pure_rs::schunk::Schunk;

    let cparams = CParams {
        compcode: BLOSC_LZ4,
        clevel: 5,
        typesize: 4,
        ..Default::default()
    };
    let dparams = DParams::default();
    let mut schunk = Schunk::new(cparams, dparams);

    let zeros = vec![0u8; 10000];
    let nonzero: Vec<u8> = (0..10000u32)
        .flat_map(|i| (i % 256).to_le_bytes())
        .collect();

    schunk.append_buffer(&zeros).unwrap();
    schunk.append_buffer(&nonzero[..10000]).unwrap();
    schunk.append_buffer(&zeros).unwrap();

    assert_eq!(schunk.nchunks(), 3);

    let d0 = schunk.decompress_chunk(0).unwrap();
    let d1 = schunk.decompress_chunk(1).unwrap();
    let d2 = schunk.decompress_chunk(2).unwrap();

    assert_eq!(d0, zeros);
    assert_eq!(d1, &nonzero[..10000]);
    assert_eq!(d2, zeros);
}

#[test]
fn test_schunk_fill_special_leftover_chunks_match_c() {
    use blosc2_pure_rs::schunk::{blosc2_schunk_get_chunk, blosc2_schunk_get_lazychunk, Schunk};

    const CHUNK_ITEMS: usize = 16;
    const BASE_CHUNKS: usize = 3;
    let chunk_nbytes = CHUNK_ITEMS * std::mem::size_of::<f32>();

    for special in [
        BLOSC2_SPECIAL_ZERO,
        BLOSC2_SPECIAL_NAN,
        BLOSC2_SPECIAL_UNINIT,
    ] {
        for leftover_items in [0usize, 1, 10] {
            let cparams = CParams {
                compcode: BLOSC_BLOSCLZ,
                clevel: 9,
                typesize: 4,
                use_dict: true,
                nthreads: 4,
                ..Default::default()
            };
            let mut schunk = Schunk::new(cparams, DParams::default());
            let nitems = BASE_CHUNKS * CHUNK_ITEMS + leftover_items;
            let nchunks = schunk.fill_special(nitems, special, chunk_nbytes).unwrap();
            let expected_nchunks = BASE_CHUNKS + usize::from(leftover_items != 0);

            assert_eq!(nchunks as usize, expected_nchunks);
            assert_eq!(schunk.nchunks() as usize, expected_nchunks);

            let dctx = DContext::new(DParams {
                nthreads: 4,
                ..Default::default()
            });
            for nchunk in 0..expected_nchunks {
                let chunk = schunk.compressed_chunk(nchunk as i64).unwrap();
                let expected_nbytes = if nchunk == expected_nchunks - 1 && leftover_items != 0 {
                    leftover_items * std::mem::size_of::<f32>()
                } else {
                    chunk_nbytes
                };
                assert_special_header(
                    chunk,
                    special,
                    expected_nbytes,
                    BLOSC_EXTENDED_HEADER_LENGTH,
                );

                let restored = schunk.decompress_chunk(nchunk as i64).unwrap();
                assert_eq!(restored.len(), expected_nbytes);
                match special {
                    BLOSC2_SPECIAL_ZERO => assert!(restored.iter().all(|&byte| byte == 0)),
                    BLOSC2_SPECIAL_NAN => {
                        for item in restored.chunks_exact(4) {
                            assert!(f32::from_le_bytes(item.try_into().unwrap()).is_nan());
                        }
                    }
                    BLOSC2_SPECIAL_UNINIT => {}
                    _ => unreachable!(),
                }

                let (getitem_cbytes, getitem_chunk) = match special {
                    BLOSC2_SPECIAL_NAN => {
                        let (cbytes, lazychunk) =
                            blosc2_schunk_get_lazychunk(&schunk, nchunk as i64);
                        (cbytes, lazychunk.unwrap())
                    }
                    BLOSC2_SPECIAL_ZERO | BLOSC2_SPECIAL_UNINIT => {
                        let (cbytes, chunk, _needs_free) =
                            blosc2_schunk_get_chunk(&schunk, nchunk as i64);
                        (cbytes, chunk.unwrap().into_owned())
                    }
                    _ => unreachable!(),
                };
                assert_eq!(getitem_cbytes, BLOSC_EXTENDED_HEADER_LENGTH as i32);
                let uninit_sentinel = [0xa5; std::mem::size_of::<f32>()];
                let mut first_item = uninit_sentinel;
                assert_eq!(
                    blosc2_getitem_ctx_c(
                        &dctx,
                        &getitem_chunk,
                        getitem_cbytes,
                        0,
                        1,
                        &mut first_item,
                        std::mem::size_of::<f32>() as i32,
                    ),
                    std::mem::size_of::<f32>() as i32
                );
                match special {
                    BLOSC2_SPECIAL_ZERO => {
                        assert_eq!(f32::from_le_bytes(first_item), 0.0);
                    }
                    BLOSC2_SPECIAL_NAN => {
                        assert!(f32::from_le_bytes(first_item).is_nan());
                    }
                    BLOSC2_SPECIAL_UNINIT => {
                        assert_eq!(first_item, uninit_sentinel);
                    }
                    _ => unreachable!(),
                }
            }
        }
    }
}

#[test]
fn test_schunk_repeatval_chunks_getitem_match_c_zero_runlen() {
    use blosc2_pure_rs::schunk::{blosc2_schunk_get_chunk, Schunk};

    const CHUNK_ITEMS: usize = 16;
    const FULL_CHUNKS: usize = 3;
    const LEFTOVER_ITEMS: usize = 5;
    let chunk_nbytes = CHUNK_ITEMS * std::mem::size_of::<i32>();
    let repeat_value = 0x0102_0304i32.to_le_bytes();
    let cparams = CParams {
        compcode: BLOSC_BLOSCLZ,
        clevel: 9,
        typesize: 4,
        use_dict: true,
        nthreads: 4,
        ..Default::default()
    };
    let expected_repeat_cbytes = BLOSC_EXTENDED_HEADER_LENGTH + cparams.typesize as usize;
    let mut schunk = Schunk::new(cparams, DParams::default());

    assert_eq!(
        schunk
            .fill_repeatval(
                FULL_CHUNKS * CHUNK_ITEMS + LEFTOVER_ITEMS,
                &repeat_value,
                chunk_nbytes
            )
            .unwrap(),
        (FULL_CHUNKS + 1) as i64
    );

    let dctx = DContext::new(DParams {
        nthreads: 4,
        ..Default::default()
    });
    let _b = init();
    for nchunk in 0..=FULL_CHUNKS {
        let stored = schunk.compressed_chunk(nchunk as i64).unwrap();
        let expected_items = if nchunk == FULL_CHUNKS {
            LEFTOVER_ITEMS
        } else {
            CHUNK_ITEMS
        };
        let expected_nbytes = expected_items * std::mem::size_of::<i32>();
        assert_special_header(
            stored,
            BLOSC2_SPECIAL_VALUE,
            expected_nbytes,
            expected_repeat_cbytes,
        );
        assert_eq!(&stored[BLOSC_EXTENDED_HEADER_LENGTH..], repeat_value);

        let restored = schunk.decompress_chunk(nchunk as i64).unwrap();
        assert_eq!(restored.len(), expected_nbytes);
        for item in restored.chunks_exact(std::mem::size_of::<i32>()) {
            assert_eq!(item, repeat_value);
        }

        let mut c_restored = vec![0u8; expected_nbytes];
        let c_dsize = unsafe {
            ffi::blosc2_decompress(
                stored.as_ptr() as *const _,
                stored.len() as i32,
                c_restored.as_mut_ptr() as *mut _,
                c_restored.len() as i32,
            )
        };
        assert_eq!(c_dsize, expected_nbytes as i32);
        assert_eq!(c_restored, restored);

        let mut rust_restored = vec![0u8; expected_nbytes];
        let rust_dsize = blosc2_pure_rs::compress::blosc2_decompress(
            stored,
            stored.len() as i32,
            &mut rust_restored,
            expected_nbytes as i32,
        );
        assert_eq!(rust_dsize, expected_nbytes as i32);
        assert_eq!(rust_restored, restored);

        for item_start in [0, expected_items / 2, expected_items - 1] {
            let mut item = [0u8; std::mem::size_of::<i32>()];
            let item_len = item.len() as i32;
            assert_eq!(
                blosc2_getitem_ctx_c(
                    &dctx,
                    stored,
                    stored.len() as i32,
                    item_start as i32,
                    1,
                    &mut item,
                    item_len,
                ),
                item_len,
                "stored repeatval getitem_ctx failed at chunk {nchunk}, item {item_start}"
            );
            assert_eq!(item, repeat_value);
        }

        let (cbytes, chunk, _needs_free) = blosc2_schunk_get_chunk(&schunk, nchunk as i64);
        assert_eq!(cbytes, expected_repeat_cbytes as i32);
        let chunk = chunk.unwrap();
        let item_start = (expected_items / 2) as i32;
        let mut items = [0u8; 2 * std::mem::size_of::<i32>()];
        let items_len = items.len() as i32;
        assert_eq!(
            blosc2_getitem_ctx_c(
                &dctx,
                chunk.as_ref(),
                cbytes,
                item_start,
                2,
                &mut items,
                items_len,
            ),
            items_len
        );
        for item in items.chunks_exact(std::mem::size_of::<i32>()) {
            assert_eq!(item, repeat_value);
        }

        let mut overflow_items = [0u8; 2 * std::mem::size_of::<i32>()];
        assert_eq!(
            blosc2_getitem_ctx_c(
                &dctx,
                chunk.as_ref(),
                cbytes,
                expected_items as i32 - 1,
                2,
                &mut overflow_items,
                items_len,
            ),
            BLOSC2_ERROR_INVALID_PARAM
        );
    }
}

#[test]
fn test_schunk_fill_special_c_error_returns_match_c() {
    use blosc2_pure_rs::schunk::{blosc2_schunk_fill_special, Schunk};

    let cparams = CParams {
        compcode: BLOSC_BLOSCLZ,
        clevel: 9,
        typesize: 4,
        nthreads: 4,
        ..Default::default()
    };

    let mut empty = Schunk::new(cparams.clone(), DParams::default());
    assert_eq!(blosc2_schunk_fill_special(&mut empty, 0, 99, 0), 0);
    assert_eq!(
        blosc2_schunk_fill_special(&mut empty, 1, BLOSC2_SPECIAL_ZERO, 0),
        i64::from(BLOSC2_ERROR_INVALID_PARAM)
    );

    let mut invalid_special = Schunk::new(cparams.clone(), DParams::default());
    assert_eq!(
        blosc2_schunk_fill_special(&mut invalid_special, 1, BLOSC2_SPECIAL_VALUE, 4),
        i64::from(BLOSC2_ERROR_SCHUNK_SPECIAL)
    );

    let mut nonempty = Schunk::new(cparams, DParams::default());
    nonempty.append_buffer(&[0u8; 4]).unwrap();
    assert_eq!(
        blosc2_schunk_fill_special(&mut nonempty, 1, BLOSC2_SPECIAL_ZERO, 4),
        i64::from(BLOSC2_ERROR_FRAME_SPECIAL)
    );
}

#[test]
fn test_schunk_frame_roundtrip_with_zeros() {
    use blosc2_pure_rs::compress::DParams;
    use blosc2_pure_rs::schunk::Schunk;

    let cparams = CParams {
        compcode: BLOSC_LZ4,
        clevel: 5,
        typesize: 4,
        ..Default::default()
    };
    let mut schunk = Schunk::new(cparams, DParams::default());

    let zeros = vec![0u8; 8000];
    schunk.append_buffer(&zeros).unwrap();
    schunk.append_buffer(&zeros).unwrap();

    let frame = schunk.to_frame();
    let schunk2 = Schunk::from_frame(&frame).unwrap();

    assert_eq!(schunk2.nchunks(), 2);
    assert_eq!(schunk2.decompress_chunk(0).unwrap(), zeros);
    assert_eq!(schunk2.decompress_chunk(1).unwrap(), zeros);
}
