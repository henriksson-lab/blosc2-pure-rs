pub mod blosclz;

use crate::constants::*;
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::{OnceLock, RwLock};
use zstd_pure_rs::common::error::{ErrorCode, ERROR};
use zstd_pure_rs::common::xxhash::XXH64_state_t;
use zstd_pure_rs::decompress::zstd_decompress_block::{ZSTD_DCtx, ZSTD_decoder_entropy_rep};
use zstd_pure_rs::prelude::*;

pub type CodecCompressFn = fn(clevel: u8, meta: u8, src: &[u8], dest: &mut [u8]) -> i32;
pub type CodecDecompressFn = fn(meta: u8, src: &[u8], dest: &mut [u8]) -> i32;

#[derive(Clone, Copy)]
struct UserCodec {
    compress: CodecCompressFn,
    decompress: CodecDecompressFn,
}

static USER_CODECS: OnceLock<RwLock<HashMap<u8, UserCodec>>> = OnceLock::new();

thread_local! {
    static ZSTD_CCTX: RefCell<Option<Box<ZSTD_CCtx>>> = const { RefCell::new(None) };
    static ZSTD_DICT_CCTX: RefCell<Option<Box<ZSTD_CCtx>>> = const { RefCell::new(None) };
    static ZSTD_DICT_DCTX: RefCell<Box<ZSTD_DCtx>> = RefCell::new(ZSTD_createDCtx());
    static ZSTD_DCTX: RefCell<(Box<ZSTD_DCtx>, ZSTD_decoder_entropy_rep, XXH64_state_t)> =
        RefCell::new((
            ZSTD_createDCtx(),
            ZSTD_decoder_entropy_rep::default(),
            XXH64_state_t::default(),
        ));
}

fn user_codecs() -> &'static RwLock<HashMap<u8, UserCodec>> {
    USER_CODECS.get_or_init(|| RwLock::new(HashMap::new()))
}

pub fn register_codec(
    compcode: u8,
    compress: CodecCompressFn,
    decompress: CodecDecompressFn,
) -> Result<(), &'static str> {
    if compcode < BLOSC2_USER_DEFINED_CODECS_START {
        return Err("User-defined codec IDs must be >= 32");
    }
    user_codecs()
        .write()
        .map_err(|_| "Codec registry poisoned")?
        .insert(
            compcode,
            UserCodec {
                compress,
                decompress,
            },
        );
    Ok(())
}

pub fn is_registered_codec(compcode: u8) -> bool {
    user_codecs()
        .read()
        .is_ok_and(|codecs| codecs.contains_key(&compcode))
}

pub fn codec_supports_dict(compcode: u8) -> bool {
    matches!(compcode, BLOSC_LZ4 | BLOSC_LZ4HC | BLOSC_ZSTD)
}

/// Compress a block using the specified codec.
/// Returns the number of compressed bytes, or 0 if incompressible.
pub fn compress_block(compcode: u8, clevel: u8, src: &[u8], dest: &mut [u8]) -> i32 {
    compress_block_with_meta(compcode, clevel, 0, src, dest)
}

pub fn compress_block_with_meta(
    compcode: u8,
    clevel: u8,
    meta: u8,
    src: &[u8],
    dest: &mut [u8],
) -> i32 {
    match compcode {
        BLOSC_BLOSCLZ => blosclz::compress(clevel as i32, src, dest),
        BLOSC_LZ4 => lz4_compress(clevel, src, dest),
        BLOSC_LZ4HC => lz4hc_compress(clevel, src, dest),
        BLOSC_ZLIB => zlib_compress(src, dest, clevel),
        BLOSC_ZSTD => zstd_compress(src, dest, clevel),
        _ => user_codecs()
            .read()
            .ok()
            .and_then(|codecs| codecs.get(&compcode).copied())
            .map_or(0, |codec| (codec.compress)(clevel, meta, src, dest)),
    }
}

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

/// Decompress a block using the specified codec.
/// Returns the number of decompressed bytes, or negative on error.
pub fn decompress_block(compcode: u8, src: &[u8], dest: &mut [u8]) -> i32 {
    decompress_block_with_meta(compcode, 0, src, dest)
}

pub fn decompress_block_with_meta(compcode: u8, meta: u8, src: &[u8], dest: &mut [u8]) -> i32 {
    match compcode {
        BLOSC_BLOSCLZ => blosclz::decompress(src, dest),
        BLOSC_LZ4 | BLOSC_LZ4HC => lz4_decompress(src, dest),
        BLOSC_ZLIB => zlib_decompress(src, dest),
        BLOSC_ZSTD => zstd_decompress(src, dest),
        _ => user_codecs()
            .read()
            .ok()
            .and_then(|codecs| codecs.get(&compcode).copied())
            .map_or(-1, |codec| (codec.decompress)(meta, src, dest)),
    }
}

pub fn decompress_block_with_dict(compcode: u8, src: &[u8], dest: &mut [u8], dict: &[u8]) -> i32 {
    match compcode {
        BLOSC_LZ4 | BLOSC_LZ4HC => lz4_decompress_with_dict(src, dest, dict),
        BLOSC_ZSTD => zstd_decompress_with_dict(src, dest, dict),
        _ => decompress_block(compcode, src, dest),
    }
}

fn lz4_compress(clevel: u8, src: &[u8], dest: &mut [u8]) -> i32 {
    use lz4_pure::block::CompressionMode;

    let _ = clevel;
    let accel = 1;
    match lz4_pure::block::compress_to_buffer(src, Some(CompressionMode::FAST(accel)), false, dest)
    {
        Ok(n) => n as i32,
        Err(_) => 0,
    }
}

fn lz4hc_compress(clevel: u8, src: &[u8], dest: &mut [u8]) -> i32 {
    use lz4_pure::block::CompressionMode;
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

fn lz4_decompress(src: &[u8], dest: &mut [u8]) -> i32 {
    match lz4_pure::block::decompress_to_buffer(src, Some(dest.len() as i32), dest) {
        Ok(n) => n as i32,
        Err(_) => -1,
    }
}

fn len_as_c_int(len: usize) -> Option<lz4_pure::sys::c_int> {
    lz4_pure::sys::c_int::try_from(len).ok()
}

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
    let _ = clevel;
    let accel = 1;

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

fn lz4hc_compress_with_dict(clevel: u8, src: &[u8], dest: &mut [u8], dict: &[u8]) -> i32 {
    use lz4_pure::sys::{
        c_char, LZ4_compress_HC_continue, LZ4_createStreamHC, LZ4_freeStreamHC, LZ4_loadDictHC,
        LZ4_resetStreamHC_fast,
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

fn lz4_decompress_with_dict(src: &[u8], dest: &mut [u8], dict: &[u8]) -> i32 {
    use lz4_pure::sys::{c_char, LZ4_decompress_safe_usingDict};

    let Some(src_len) = len_as_c_int(src.len()) else {
        return -1;
    };
    let Some(dest_len) = len_as_c_int(dest.len()) else {
        return -1;
    };
    let Some(dict_len) = len_as_c_int(dict.len()) else {
        return -1;
    };

    unsafe {
        LZ4_decompress_safe_usingDict(
            src.as_ptr() as *const c_char,
            dest.as_mut_ptr() as *mut c_char,
            src_len,
            dest_len,
            dict.as_ptr() as *const c_char,
            dict_len,
        )
    }
}

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

fn zlib_decompress(src: &[u8], dest: &mut [u8]) -> i32 {
    use flate2::Decompress;
    use flate2::FlushDecompress;

    let mut decompress = Decompress::new(true);
    match decompress.decompress(src, dest, FlushDecompress::Finish) {
        Ok(flate2::Status::StreamEnd) => decompress.total_out() as i32,
        Ok(_) => -1,
        Err(_) => -1,
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

fn zstd_compress(src: &[u8], dest: &mut [u8], clevel: u8) -> i32 {
    let n = ZSTD_CCTX.with(|slot| {
        let mut slot = slot.borrow_mut();
        if slot.is_none() {
            *slot = ZSTD_createCCtx();
        }
        let Some(cctx) = slot.as_deref_mut() else {
            return ERROR(ErrorCode::MemoryAllocation);
        };
        ZSTD_compressCCtx(cctx, dest, src, blosc_clevel_to_zstd(clevel))
    });
    if ERR_isError(n) {
        0
    } else {
        n as i32
    }
}

fn zstd_compress_with_dict(src: &[u8], dest: &mut [u8], clevel: u8, dict: &[u8]) -> i32 {
    let n = ZSTD_DICT_CCTX.with(|slot| {
        let mut slot = slot.borrow_mut();
        if slot.is_none() {
            *slot = ZSTD_createCCtx();
        }
        let Some(cctx) = slot.as_deref_mut() else {
            return ERROR(ErrorCode::MemoryAllocation);
        };
        ZSTD_compress_usingDict(cctx, dest, src, dict, blosc_clevel_to_zstd(clevel))
    });
    if ERR_isError(n) {
        0
    } else {
        n as i32
    }
}

fn zstd_decompress(src: &[u8], dest: &mut [u8]) -> i32 {
    let n = ZSTD_DCTX.with(|slot| {
        let mut slot = slot.borrow_mut();
        let (dctx, entropy_rep, xxh) = &mut *slot;
        *entropy_rep = ZSTD_decoder_entropy_rep::default();
        ZSTD_decompressDCtx(dctx, entropy_rep, xxh, dest, src)
    });
    if ERR_isError(n) {
        -1
    } else {
        n as i32
    }
}

fn zstd_decompress_with_dict(src: &[u8], dest: &mut [u8], dict: &[u8]) -> i32 {
    let n = ZSTD_DICT_DCTX.with(|slot| {
        let mut dctx = slot.borrow_mut();
        ZSTD_decompress_usingDict(&mut dctx, dest, src, dict)
    });
    if ERR_isError(n) {
        -1
    } else {
        n as i32
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
