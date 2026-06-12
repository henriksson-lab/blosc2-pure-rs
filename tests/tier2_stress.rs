#![cfg(feature = "_ffi")]
//! Tier 2: Multi-chunk stress tests and expanded cross-compatibility

use blosc2_pure_rs::b2nd::{B2ndArray, B2ndMeta};
use blosc2_pure_rs::compress::{compress, CParams, DParams};
use blosc2_pure_rs::constants::*;
mod common;
use blosc2_pure_rs::schunk::Schunk;
use common::ffi;
use std::ffi::c_void;

unsafe extern "C" {
    fn free(ptr: *mut c_void);
}

const CODECS: [u8; 5] = [
    BLOSC_BLOSCLZ,
    BLOSC_LZ4,
    BLOSC_LZ4HC,
    BLOSC_ZLIB,
    BLOSC_ZSTD,
];
const FILTERS: [u8; 3] = [BLOSC_NOFILTER, BLOSC_SHUFFLE, BLOSC_BITSHUFFLE];
const SPLITMODES: [i32; 4] = [
    BLOSC_ALWAYS_SPLIT,
    BLOSC_NEVER_SPLIT,
    BLOSC_AUTO_SPLIT,
    BLOSC_FORWARD_COMPAT_SPLIT,
];
const FRAME_FLAGS_OFFSET: usize = 25;
const FRAME_VERSION_MASK: u8 = 0x0f;

fn init() -> common::Blosc2 {
    common::Blosc2::new()
}

fn random_bytes(len: usize) -> Vec<u8> {
    let mut state = 0x1234_5678_9abc_def0u64;
    (0..len)
        .map(|_| {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            (state >> 32) as u8
        })
        .collect()
}

fn dont_split_flag(chunk: &[u8]) -> bool {
    chunk[BLOSC2_CHUNK_FLAGS] & BLOSC_DONT_SPLIT != 0
}

fn frame_version(frame: &[u8]) -> u8 {
    frame[FRAME_FLAGS_OFFSET] & FRAME_VERSION_MASK
}

fn variable_chunks_flag(frame: &[u8]) -> bool {
    frame[FRAME_FLAGS_OFFSET] & FRAME_VARIABLE_CHUNKS != 0
}

fn vlblocks_frame_flag(frame: &[u8]) -> bool {
    frame[FRAME_FLAGS_OFFSET] & FRAME_VL_BLOCKS != 0
}

fn assert_schunk_values(schunk: &Schunk, expected_values: &[&[u8]]) {
    for (idx, expected) in expected_values.iter().enumerate() {
        assert_eq!(schunk.decompress_chunk(idx as i64).unwrap(), *expected);
    }
}

fn assert_dont_split_parity(
    c_chunk: &[u8],
    rust_chunk: &[u8],
    compcode: u8,
    filter: u8,
    splitmode: i32,
    context: &str,
) {
    assert_eq!(
        dont_split_flag(rust_chunk),
        dont_split_flag(c_chunk),
        "Rust/C DONT_SPLIT parity mismatch for {context} codec={compcode} filter={filter} splitmode={splitmode}"
    );
}

fn c_compress(data: &[u8], compcode: u8, typesize: i32, splitmode: i32, filter: u8) -> Vec<u8> {
    let mut c_chunk = vec![0u8; data.len() + BLOSC2_MAX_OVERHEAD];
    let csize = unsafe {
        let mut cp = ffi::blosc2_get_blosc2_cparams_defaults();
        cp.compcode = compcode;
        cp.clevel = 5;
        cp.typesize = typesize;
        cp.nthreads = 1;
        cp.splitmode = splitmode;
        cp.filters[BLOSC2_MAX_FILTERS - 1] = filter;
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
    assert!(
        csize > 0,
        "C compress failed for codec={compcode} filter={filter} splitmode={splitmode}"
    );
    c_chunk.truncate(csize as usize);
    c_chunk
}

fn c_decompress(chunk: &[u8], expected_len: usize, context: &str) -> Vec<u8> {
    let mut restored = vec![0u8; expected_len];
    let dsize = unsafe {
        ffi::blosc2_decompress(
            chunk.as_ptr() as *const _,
            chunk.len() as i32,
            restored.as_mut_ptr() as *mut _,
            restored.len() as i32,
        )
    };
    assert_eq!(
        dsize, expected_len as i32,
        "C decompress failed for {context}"
    );
    restored
}

fn assert_c_decompresses_schunk_frame(mut frame: Vec<u8>, expected_chunks: &[Vec<u8>]) {
    let c_schunk =
        unsafe { ffi::blosc2_schunk_from_buffer(frame.as_mut_ptr(), frame.len() as i64, true) };
    assert!(!c_schunk.is_null(), "C failed to open Rust-produced frame");

    unsafe {
        assert_eq!((*c_schunk).nchunks, expected_chunks.len() as i64);
        assert_eq!((*c_schunk).chunksize, c_expected_chunksize(expected_chunks));
    }

    for (idx, expected) in expected_chunks.iter().enumerate() {
        let mut restored = vec![0u8; expected.len()];
        let dsize = unsafe {
            ffi::blosc2_schunk_decompress_chunk(
                c_schunk,
                idx as i64,
                restored.as_mut_ptr().cast(),
                restored.len() as i32,
            )
        };
        assert_eq!(
            dsize,
            expected.len() as i32,
            "C failed to decompress Rust frame chunk {idx}"
        );
        assert_eq!(&restored, expected, "Rust frame chunk {idx} mismatch");
    }

    let rc = unsafe { ffi::blosc2_schunk_free(c_schunk) };
    assert_eq!(rc, 0);
}

fn c_expected_chunksize(chunks: &[Vec<u8>]) -> i32 {
    let Some(first) = chunks.first() else {
        return 0;
    };
    let mut chunksize = first.len();
    for window in chunks.windows(2) {
        let last_len = window[0].len();
        let chunk_len = window[1].len();
        if chunksize != 0 && (last_len < chunksize || chunk_len > chunksize) {
            chunksize = 0;
        }
    }
    chunksize as i32
}

fn c_schunk_frame_from_buffers(chunks: &[Vec<u8>], cparams: CParams) -> Vec<u8> {
    unsafe {
        let mut c_cparams = ffi::blosc2_get_blosc2_cparams_defaults();
        c_cparams.compcode = cparams.compcode;
        c_cparams.clevel = cparams.clevel;
        c_cparams.typesize = cparams.typesize;
        c_cparams.splitmode = cparams.splitmode;
        c_cparams.nthreads = 1;
        c_cparams.filters = cparams.filters;

        let mut dparams = ffi::blosc2_dparams {
            nthreads: 1,
            ..std::mem::zeroed()
        };
        let mut storage: ffi::blosc2_storage = std::mem::zeroed();
        storage.contiguous = true;
        storage.cparams = &mut c_cparams;
        storage.dparams = &mut dparams;

        let c_schunk = ffi::blosc2_schunk_new(&mut storage);
        assert!(!c_schunk.is_null(), "C failed to create frame schunk");

        for (idx, chunk) in chunks.iter().enumerate() {
            assert_eq!(
                ffi::blosc2_schunk_append_buffer(
                    c_schunk,
                    chunk.as_ptr().cast(),
                    chunk.len() as i32,
                ),
                idx as i64 + 1,
                "C failed to append frame chunk {idx}"
            );
        }

        let mut cframe: *mut u8 = std::ptr::null_mut();
        let mut needs_free = false;
        let cframe_len = ffi::blosc2_schunk_to_buffer(c_schunk, &mut cframe, &mut needs_free);
        assert!(cframe_len > 0, "C failed to serialize frame: {cframe_len}");
        assert!(!cframe.is_null());

        let frame = std::slice::from_raw_parts(cframe, cframe_len as usize).to_vec();
        if needs_free {
            free(cframe.cast());
        }
        assert_eq!(ffi::blosc2_schunk_free(c_schunk), 0);
        frame
    }
}

// ─── Multi-chunk stress ──────────────────────────────────────────

#[test]
fn test_100_chunks() {
    let cparams = CParams {
        compcode: BLOSC_LZ4,
        clevel: 5,
        typesize: 4,
        filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
        ..Default::default()
    };
    let mut schunk = Schunk::new(cparams, DParams::default());

    let chunk_data: Vec<Vec<u8>> = (0..100)
        .map(|c| {
            (0..1000u32)
                .flat_map(|i| (i + c * 1000).to_le_bytes())
                .collect()
        })
        .collect();

    for data in &chunk_data {
        schunk.append_buffer(data).unwrap();
    }

    assert_eq!(schunk.nchunks(), 100);

    // Verify every chunk
    for (idx, expected) in chunk_data.iter().enumerate() {
        let restored = schunk.decompress_chunk(idx as i64).unwrap();
        assert_eq!(expected, &restored, "Chunk {idx} mismatch");
    }
}

#[test]
fn test_100_chunks_frame_roundtrip() {
    let _b = init();
    let cparams = CParams {
        compcode: BLOSC_ZSTD,
        clevel: 3,
        typesize: 4,
        filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
        ..Default::default()
    };
    let mut schunk = Schunk::new(cparams.clone(), DParams::default());

    let chunk_size = 4000;
    let nchunks = 100;
    let chunk_data: Vec<Vec<u8>> = (0..nchunks)
        .map(|c| {
            (0..chunk_size as u32)
                .flat_map(|i| (i.wrapping_mul(c as u32 + 1)).to_le_bytes())
                .collect()
        })
        .collect();

    // Append chunks with different data
    for data in &chunk_data {
        schunk.append_buffer(data).unwrap();
    }

    // Serialize to frame and back
    let frame = schunk.to_frame();
    assert_eq!(frame_version(&frame), BLOSC2_VERSION_FRAME_FORMAT_RC1);
    assert!(!variable_chunks_flag(&frame));
    assert!(!vlblocks_frame_flag(&frame));
    assert_c_decompresses_schunk_frame(frame.clone(), &chunk_data);

    let schunk2 = Schunk::from_frame(&frame).unwrap();

    assert_eq!(schunk2.nchunks(), nchunks);

    // Verify all chunks
    for c in 0..nchunks {
        let orig = schunk.decompress_chunk(c).unwrap();
        let restored = schunk2.decompress_chunk(c).unwrap();
        assert_eq!(orig, restored, "Frame roundtrip chunk {c} mismatch");
    }

    let c_frame = c_schunk_frame_from_buffers(&chunk_data, cparams);
    assert_eq!(frame_version(&c_frame), BLOSC2_VERSION_FRAME_FORMAT_RC1);
    assert!(!variable_chunks_flag(&c_frame));
    assert!(!vlblocks_frame_flag(&c_frame));
    let c_frame_schunk = Schunk::from_frame(&c_frame).unwrap();
    assert_eq!(c_frame_schunk.nchunks(), nchunks);
    for (idx, expected) in chunk_data.iter().enumerate() {
        assert_eq!(
            c_frame_schunk.decompress_chunk(idx as i64).unwrap(),
            *expected,
            "Rust failed to decompress C frame chunk {idx}"
        );
    }
}

#[test]
fn test_100_chunks_file_roundtrip() {
    let cparams = CParams {
        compcode: BLOSC_LZ4,
        clevel: 5,
        typesize: 8,
        filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
        ..Default::default()
    };
    let mut schunk = Schunk::new(cparams, DParams::default());

    let nchunks = 100;
    let all_data: Vec<Vec<u8>> = (0..nchunks)
        .map(|c| {
            (0..500u64)
                .flat_map(|i| (i + c as u64 * 500).to_le_bytes())
                .collect()
        })
        .collect();

    for data in &all_data {
        schunk.append_buffer(data).unwrap();
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.b2frame");
    schunk.to_file(path.to_str().unwrap()).unwrap();

    let schunk2 = Schunk::open(path.to_str().unwrap()).unwrap();
    assert_eq!(schunk2.nchunks(), nchunks as i64);

    for (idx, expected) in all_data.iter().enumerate() {
        let restored = schunk2.decompress_chunk(idx as i64).unwrap();
        assert_eq!(expected, &restored, "File roundtrip chunk {idx} mismatch");
    }
}

#[test]
fn test_variable_last_chunk() {
    let _b = init();
    let cparams = CParams {
        compcode: BLOSC_LZ4,
        clevel: 5,
        typesize: 1,
        ..Default::default()
    };
    let mut schunk = Schunk::new(cparams.clone(), DParams::default());

    let values: [&[u8]; 3] = [b"alpha\0", b"bravo bravo\0", b"charlie-charlie-charlie\0"];
    let updated_values: [&[u8]; 3] = [b"alpha\0", b"bravo bravo bravo bravo\0", b"tiny\0"];

    for (idx, value) in values.iter().enumerate() {
        assert_eq!(schunk.append_buffer(value).unwrap(), idx as i64 + 1);
    }

    assert_eq!(schunk.chunksize, 0);
    assert_eq!(schunk.nchunks(), 3);
    let frame = schunk.to_frame();
    assert_eq!(frame_version(&frame), BLOSC2_VERSION_FRAME_FORMAT_VL_BLOCKS);
    assert!(variable_chunks_flag(&frame));
    assert!(!vlblocks_frame_flag(&frame));
    assert_c_decompresses_schunk_frame(
        frame.clone(),
        &values
            .iter()
            .map(|value| value.to_vec())
            .collect::<Vec<_>>(),
    );

    let mut future_version = frame.clone();
    future_version[FRAME_FLAGS_OFFSET] = (future_version[FRAME_FLAGS_OFFSET] & !FRAME_VERSION_MASK)
        | (BLOSC2_VERSION_FRAME_FORMAT + 1);
    assert!(Schunk::from_frame(&future_version).is_err());

    for (idx, expected) in values.iter().enumerate() {
        assert_eq!(schunk.decompress_chunk(idx as i64).unwrap(), *expected);
    }

    assert_eq!(schunk.update_chunk(1, updated_values[1]).unwrap(), 3);
    assert_eq!(schunk.update_chunk(2, updated_values[2]).unwrap(), 3);
    assert_eq!(schunk.chunksize, 0);

    let updated_frame = schunk.to_frame();
    assert_eq!(
        frame_version(&updated_frame),
        BLOSC2_VERSION_FRAME_FORMAT_VL_BLOCKS
    );
    assert!(variable_chunks_flag(&updated_frame));
    assert!(!vlblocks_frame_flag(&updated_frame));
    assert_schunk_values(&schunk, &updated_values);
    assert_c_decompresses_schunk_frame(
        updated_frame.clone(),
        &updated_values
            .iter()
            .map(|value| value.to_vec())
            .collect::<Vec<_>>(),
    );

    let c_updated_frame = c_schunk_frame_from_buffers(
        &updated_values
            .iter()
            .map(|value| value.to_vec())
            .collect::<Vec<_>>(),
        cparams.clone(),
    );
    assert_eq!(
        frame_version(&c_updated_frame),
        BLOSC2_VERSION_FRAME_FORMAT_VL_BLOCKS
    );
    assert!(variable_chunks_flag(&c_updated_frame));
    assert!(!vlblocks_frame_flag(&c_updated_frame));
    let c_updated_schunk = Schunk::from_frame(&c_updated_frame).unwrap();
    assert_eq!(c_updated_schunk.chunksize, 0);
    assert_schunk_values(&c_updated_schunk, &updated_values);

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("variable_chunks.b2frame");
    schunk.to_file(path.to_str().unwrap()).unwrap();
    let reopened = Schunk::open(path.to_str().unwrap()).unwrap();
    assert_eq!(reopened.chunksize, 0);
    assert_eq!(reopened.nchunks(), 3);
    let reopened_frame = reopened.to_frame();
    assert_eq!(
        frame_version(&reopened_frame),
        BLOSC2_VERSION_FRAME_FORMAT_VL_BLOCKS
    );
    assert!(variable_chunks_flag(&reopened_frame));
    assert!(!vlblocks_frame_flag(&reopened_frame));
    assert_schunk_values(&reopened, &updated_values);

    let sparse_path = dir.path().join("variable_chunks_s.b2frame");
    schunk.to_sframe_dir(&sparse_path).unwrap();
    let reopened_sparse = Schunk::open_sframe(&sparse_path).unwrap();
    assert_eq!(reopened_sparse.chunksize, 0);
    assert_eq!(reopened_sparse.nchunks(), 3);
    let reopened_sparse_frame = reopened_sparse.to_frame();
    assert_eq!(
        frame_version(&reopened_sparse_frame),
        BLOSC2_VERSION_FRAME_FORMAT_VL_BLOCKS
    );
    assert!(variable_chunks_flag(&reopened_sparse_frame));
    assert!(!vlblocks_frame_flag(&reopened_sparse_frame));
    assert_schunk_values(&reopened_sparse, &updated_values);

    let fixed_values: [&[u8]; 3] = [b"one\0", b"two\0", b"six\0"];
    let mut fixed = Schunk::new(
        CParams {
            typesize: 1,
            ..Default::default()
        },
        DParams::default(),
    );
    for (idx, value) in fixed_values.iter().enumerate() {
        assert_eq!(fixed.append_buffer(value).unwrap(), idx as i64 + 1);
    }
    assert_eq!(fixed.chunksize, 4);
    let fixed_frame = fixed.to_frame();
    assert_eq!(frame_version(&fixed_frame), BLOSC2_VERSION_FRAME_FORMAT_RC1);
    assert!(!variable_chunks_flag(&fixed_frame));
    assert!(!vlblocks_frame_flag(&fixed_frame));
    assert_c_decompresses_schunk_frame(
        fixed_frame.clone(),
        &fixed_values
            .iter()
            .map(|value| value.to_vec())
            .collect::<Vec<_>>(),
    );
    assert_schunk_values(&fixed, &fixed_values);

    let fixed_path = dir.path().join("fixed_chunks.b2frame");
    fixed.to_file(fixed_path.to_str().unwrap()).unwrap();
    let reopened_fixed = Schunk::open(fixed_path.to_str().unwrap()).unwrap();
    assert_eq!(reopened_fixed.chunksize, 4);
    let reopened_fixed_frame = reopened_fixed.to_frame();
    assert_eq!(
        frame_version(&reopened_fixed_frame),
        BLOSC2_VERSION_FRAME_FORMAT_RC1
    );
    assert!(!variable_chunks_flag(&reopened_fixed_frame));
    assert!(!vlblocks_frame_flag(&reopened_fixed_frame));
    assert_schunk_values(&reopened_fixed, &fixed_values);

    let fixed_sparse_path = dir.path().join("fixed_chunks_s.b2frame");
    fixed.to_sframe_dir(&fixed_sparse_path).unwrap();
    let reopened_fixed_sparse = Schunk::open_sframe(&fixed_sparse_path).unwrap();
    assert_eq!(reopened_fixed_sparse.chunksize, 4);
    let reopened_fixed_sparse_frame = reopened_fixed_sparse.to_frame();
    assert_eq!(
        frame_version(&reopened_fixed_sparse_frame),
        BLOSC2_VERSION_FRAME_FORMAT_RC1
    );
    assert!(!variable_chunks_flag(&reopened_fixed_sparse_frame));
    assert!(!vlblocks_frame_flag(&reopened_fixed_sparse_frame));
    assert_schunk_values(&reopened_fixed_sparse, &fixed_values);

    let full_chunk = vec![42u8; 10_000];
    let partial_tail = vec![99u8; 3_000];
    let mut fixed_with_short_tail = Schunk::new(
        CParams {
            typesize: 1,
            ..Default::default()
        },
        DParams::default(),
    );
    assert_eq!(fixed_with_short_tail.append_buffer(&full_chunk).unwrap(), 1);
    assert_eq!(fixed_with_short_tail.append_buffer(&full_chunk).unwrap(), 2);
    assert_eq!(
        fixed_with_short_tail.append_buffer(&partial_tail).unwrap(),
        3
    );
    assert_eq!(fixed_with_short_tail.chunksize, full_chunk.len());
    let fixed_with_short_tail_frame = fixed_with_short_tail.to_frame();
    assert_eq!(
        frame_version(&fixed_with_short_tail_frame),
        BLOSC2_VERSION_FRAME_FORMAT_RC1
    );
    assert!(!variable_chunks_flag(&fixed_with_short_tail_frame));
    assert!(!vlblocks_frame_flag(&fixed_with_short_tail_frame));
    assert_c_decompresses_schunk_frame(
        fixed_with_short_tail_frame.clone(),
        &[full_chunk.clone(), full_chunk.clone(), partial_tail.clone()],
    );
    assert_eq!(
        fixed_with_short_tail.decompress_chunk(0).unwrap(),
        full_chunk
    );
    assert_eq!(
        fixed_with_short_tail.decompress_chunk(1).unwrap(),
        full_chunk
    );
    assert_eq!(
        fixed_with_short_tail.decompress_chunk(2).unwrap(),
        partial_tail
    );

    let reopened_short_tail = Schunk::from_frame(&fixed_with_short_tail_frame).unwrap();
    assert_eq!(reopened_short_tail.chunksize, 10_000);
    assert_eq!(reopened_short_tail.nchunks(), 3);
    assert_eq!(
        reopened_short_tail.decompress_chunk(2).unwrap(),
        partial_tail
    );

    let c_fixed_with_short_tail_frame = c_schunk_frame_from_buffers(
        &[full_chunk.clone(), full_chunk.clone(), partial_tail.clone()],
        CParams {
            typesize: 1,
            ..Default::default()
        },
    );
    assert_eq!(
        frame_version(&c_fixed_with_short_tail_frame),
        BLOSC2_VERSION_FRAME_FORMAT_RC1
    );
    assert!(!variable_chunks_flag(&c_fixed_with_short_tail_frame));
    assert!(!vlblocks_frame_flag(&c_fixed_with_short_tail_frame));
    let c_fixed_with_short_tail = Schunk::from_frame(&c_fixed_with_short_tail_frame).unwrap();
    assert_eq!(c_fixed_with_short_tail.chunksize, 10_000);
    assert_eq!(c_fixed_with_short_tail.nchunks(), 3);
    assert_eq!(
        c_fixed_with_short_tail.decompress_chunk(2).unwrap(),
        partial_tail
    );
}

// ─── Cross-compat matrix: C compress → Rust decompress ──────────

#[test]
fn test_c_compress_all_codecs_filters_splitmodes_rust_decompress() {
    let _b = init();
    let data: Vec<u8> = (0..10000u32).flat_map(|i| i.to_le_bytes()).collect();

    for compcode in CODECS {
        for filter in FILTERS {
            for splitmode in SPLITMODES {
                let c_chunk = c_compress(&data, compcode, 4, splitmode, filter);
                let restored = blosc2_pure_rs::compress::decompress(&c_chunk).unwrap_or_else(|e| {
                    panic!(
                        "Rust decompress failed for codec={compcode} filter={filter} splitmode={splitmode}: {e}"
                    )
                });
                assert_eq!(
                    data, restored,
                    "C→Rust mismatch for codec={compcode} filter={filter} splitmode={splitmode}"
                );
            }
        }
    }
}

#[test]
fn test_rust_compress_all_codecs_filters_splitmodes_c_decompress() {
    let _b = init();
    let data = random_bytes(256 * 1024 + 17);

    for compcode in CODECS {
        for filter in FILTERS {
            for splitmode in SPLITMODES {
                let c_chunk = c_compress(&data, compcode, 4, splitmode, filter);
                let cparams = CParams {
                    compcode,
                    clevel: 5,
                    typesize: 4,
                    splitmode,
                    filters: [0, 0, 0, 0, 0, filter],
                    ..Default::default()
                };
                let chunk = compress(&data, &cparams).unwrap_or_else(|e| {
                    panic!(
                        "Rust compress failed for codec={compcode} filter={filter} splitmode={splitmode}: {e}"
                    )
                });
                assert_dont_split_parity(
                    &c_chunk,
                    &chunk,
                    compcode,
                    filter,
                    splitmode,
                    "all-codecs matrix",
                );
                let restored = c_decompress(
                    &chunk,
                    data.len(),
                    &format!("codec={compcode} filter={filter} splitmode={splitmode}"),
                );
                assert_eq!(
                    data, restored,
                    "Rust→C mismatch for codec={compcode} filter={filter} splitmode={splitmode}"
                );
            }
        }
    }
}

#[test]
fn test_c_compress_large_random_filters_splitmodes_rust_decompress() {
    let _b = init();
    let data = random_bytes(1024 * 1024 + 29);

    for filter in FILTERS {
        for splitmode in SPLITMODES {
            let c_chunk = c_compress(&data, BLOSC_LZ4, 4, splitmode, filter);
            let restored = blosc2_pure_rs::compress::decompress(&c_chunk).unwrap_or_else(|e| {
                panic!("Rust decompress failed for filter={filter} splitmode={splitmode}: {e}")
            });
            assert_eq!(
                data, restored,
                "C→Rust large random mismatch for filter={filter} splitmode={splitmode}"
            );
        }
    }
}

#[test]
fn test_rust_compress_large_random_filters_splitmodes_c_decompress() {
    let _b = init();
    let data = random_bytes(1024 * 1024 + 29);

    for filter in FILTERS {
        for splitmode in SPLITMODES {
            let c_chunk = c_compress(&data, BLOSC_LZ4, 4, splitmode, filter);
            let cparams = CParams {
                compcode: BLOSC_LZ4,
                clevel: 5,
                typesize: 4,
                splitmode,
                filters: [0, 0, 0, 0, 0, filter],
                ..Default::default()
            };
            let chunk = compress(&data, &cparams).unwrap_or_else(|e| {
                panic!("Rust compress failed for filter={filter} splitmode={splitmode}: {e}")
            });
            assert_dont_split_parity(
                &c_chunk,
                &chunk,
                BLOSC_LZ4,
                filter,
                splitmode,
                "large random",
            );
            let restored = c_decompress(
                &chunk,
                data.len(),
                &format!("large random filter={filter} splitmode={splitmode}"),
            );
            assert_eq!(
                data, restored,
                "Rust→C large random mismatch for filter={filter} splitmode={splitmode}"
            );
        }
    }
}

#[test]
fn test_b2nd_random_multi_chunk_frame_file_and_c_roundtrip() {
    let _b = init();
    let shape = [65i64, 37, 19];
    let chunkshape = [8i32, 9, 7];
    let blockshape = [4i32, 3, 7];
    let data = random_bytes(shape.iter().product::<i64>() as usize * 2);
    let meta = B2ndMeta::new(
        shape.to_vec(),
        chunkshape.to_vec(),
        blockshape.to_vec(),
        "<u2",
        0,
    )
    .unwrap();
    let cparams = CParams {
        compcode: BLOSC_ZSTD,
        clevel: 5,
        typesize: 2,
        splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
        filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
        ..Default::default()
    };

    let array = B2ndArray::from_cbuffer(meta, &data, cparams, DParams::default()).unwrap();
    assert_eq!(array.schunk.nchunks(), 135);
    assert_eq!(array.to_cbuffer().unwrap(), data);

    let mut frame = array.to_frame();
    let from_frame = B2ndArray::from_frame(&frame).unwrap();
    assert_eq!(from_frame.meta.shape, shape);
    assert_eq!(from_frame.meta.chunkshape, chunkshape);
    assert_eq!(from_frame.meta.blockshape, blockshape);
    assert_eq!(from_frame.to_cbuffer().unwrap(), data);

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("stress.b2nd");
    array.save(&path).unwrap();
    let from_file = B2ndArray::open(&path).unwrap();
    assert_eq!(from_file.to_cbuffer().unwrap(), data);

    unsafe {
        let mut c_array: *mut ffi::b2nd_array_t = std::ptr::null_mut();
        let rc = ffi::b2nd_from_cframe(frame.as_mut_ptr(), frame.len() as i64, true, &mut c_array);
        assert_eq!(rc, 0, "C failed to open Rust-produced B2ND frame");
        assert!(!c_array.is_null());

        let mut c_buffer = vec![0u8; data.len()];
        let rc = ffi::b2nd_to_cbuffer(c_array, c_buffer.as_mut_ptr().cast(), c_buffer.len() as i64);
        assert_eq!(rc, 0, "C failed to read Rust-produced B2ND frame");
        assert_eq!(c_buffer, data);
        assert_eq!(ffi::b2nd_free(c_array), 0);
    }
}

// ─── Large data test ─────────────────────────────────────────────

#[test]
fn test_large_data_10mb() {
    let data: Vec<u8> = (0..2500000u32).flat_map(|i| i.to_le_bytes()).collect();
    assert_eq!(data.len(), 10_000_000);

    let cparams = CParams {
        compcode: BLOSC_LZ4,
        clevel: 5,
        typesize: 4,
        filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
        ..Default::default()
    };
    let mut schunk = Schunk::new(cparams, DParams::default());

    // Append in 1MB chunks
    for chunk_start in (0..data.len()).step_by(1_000_000) {
        let chunk_end = (chunk_start + 1_000_000).min(data.len());
        schunk.append_buffer(&data[chunk_start..chunk_end]).unwrap();
    }

    // Verify all chunks
    let mut restored = Vec::new();
    for i in 0..schunk.nchunks() {
        restored.extend(schunk.decompress_chunk(i).unwrap());
    }
    assert_eq!(data, restored);
}
