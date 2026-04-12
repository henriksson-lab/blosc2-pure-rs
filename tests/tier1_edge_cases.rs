#![cfg(feature = "_ffi")]
//! Tier 1: Split modes, large typesizes, non-aligned sizes, incompressible data

use blosc2_pure_rs::compress::{compress, decompress, CParams};
use blosc2_pure_rs::constants::*;
use blosc2_pure_rs::filters;
use blosc2_pure_rs::ffi;

fn init() -> blosc2_pure_rs::Blosc2 {
    blosc2_pure_rs::Blosc2::new()
}

// ─── Block splitting modes ───────────────────────────────────────

#[test]
fn test_always_split() {
    let data: Vec<u8> = (0..20000u32).flat_map(|i| i.to_le_bytes()).collect();
    for compcode in [BLOSC_BLOSCLZ, BLOSC_LZ4, BLOSC_ZSTD] {
        let cparams = CParams {
            compcode,
            clevel: 5,
            typesize: 4,
            splitmode: BLOSC_ALWAYS_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let chunk = compress(&data, &cparams).unwrap();
        let restored = decompress(&chunk).unwrap();
        assert_eq!(data, restored, "ALWAYS_SPLIT failed for codec={compcode}");
    }
}

#[test]
fn test_never_split() {
    let data: Vec<u8> = (0..20000u32).flat_map(|i| i.to_le_bytes()).collect();
    for compcode in [BLOSC_BLOSCLZ, BLOSC_LZ4, BLOSC_ZSTD] {
        let cparams = CParams {
            compcode,
            clevel: 5,
            typesize: 4,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let chunk = compress(&data, &cparams).unwrap();
        let restored = decompress(&chunk).unwrap();
        assert_eq!(data, restored, "NEVER_SPLIT failed for codec={compcode}");
    }
}

#[test]
fn test_forward_compat_split() {
    let data: Vec<u8> = (0..20000u32).flat_map(|i| i.to_le_bytes()).collect();
    let cparams = CParams {
        compcode: BLOSC_LZ4,
        clevel: 5,
        typesize: 4,
        splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
        filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
        ..Default::default()
    };
    let chunk = compress(&data, &cparams).unwrap();
    let restored = decompress(&chunk).unwrap();
    assert_eq!(data, restored);
}

#[test]
fn test_split_mode_cross_compat_with_c() {
    let _b = init();
    let data: Vec<u8> = (0..10000u32).flat_map(|i| i.to_le_bytes()).collect();

    for splitmode in [BLOSC_ALWAYS_SPLIT, BLOSC_NEVER_SPLIT, BLOSC_FORWARD_COMPAT_SPLIT] {
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 4,
            splitmode,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let chunk = compress(&data, &cparams).unwrap();

        // C should be able to decompress
        let mut c_restored = vec![0u8; data.len()];
        let dsize = unsafe {
            ffi::blosc2_decompress(
                chunk.as_ptr() as *const _, chunk.len() as i32,
                c_restored.as_mut_ptr() as *mut _, c_restored.len() as i32,
            )
        };
        assert_eq!(dsize, data.len() as i32,
            "C decompress failed for splitmode={splitmode}");
        assert_eq!(data, c_restored,
            "C decompress mismatch for splitmode={splitmode}");
    }
}

// ─── Large typesizes ─────────────────────────────────────────────

#[test]
fn test_typesize_16() {
    let data: Vec<u8> = (0..5000u16).map(|i| (i.wrapping_mul(7) & 0xFF) as u8).collect();
    let cparams = CParams {
        compcode: BLOSC_LZ4,
        clevel: 5,
        typesize: 16,
        ..Default::default()
    };
    let chunk = compress(&data, &cparams).unwrap();
    let restored = decompress(&chunk).unwrap();
    assert_eq!(data, restored);
}

#[test]
fn test_typesize_32() {
    let data: Vec<u8> = (0..10000u16).map(|i| (i.wrapping_mul(13) & 0xFF) as u8).collect();
    let cparams = CParams {
        compcode: BLOSC_ZSTD,
        clevel: 5,
        typesize: 32,
        ..Default::default()
    };
    let chunk = compress(&data, &cparams).unwrap();
    let restored = decompress(&chunk).unwrap();
    assert_eq!(data, restored);
}

#[test]
fn test_typesize_64() {
    let data: Vec<u8> = (0..20000u16).map(|i| (i.wrapping_mul(11) & 0xFF) as u8).collect();
    let cparams = CParams {
        compcode: BLOSC_LZ4,
        clevel: 5,
        typesize: 64,
        ..Default::default()
    };
    let chunk = compress(&data, &cparams).unwrap();
    let restored = decompress(&chunk).unwrap();
    assert_eq!(data, restored);
}

#[test]
fn test_typesize_128() {
    let data: Vec<u8> = (0..20000u16).map(|i| (i.wrapping_add(42) & 0xFF) as u8).collect();
    let cparams = CParams {
        compcode: BLOSC_BLOSCLZ,
        clevel: 5,
        typesize: 128,
        ..Default::default()
    };
    let chunk = compress(&data, &cparams).unwrap();
    let restored = decompress(&chunk).unwrap();
    assert_eq!(data, restored);
}

#[test]
fn test_typesize_255() {
    // 255 is BLOSC_MAX_TYPESIZE
    let data = vec![0xABu8; 255 * 100]; // 25500 bytes, exactly 100 elements
    let cparams = CParams {
        compcode: BLOSC_LZ4,
        clevel: 5,
        typesize: 255,
        ..Default::default()
    };
    let chunk = compress(&data, &cparams).unwrap();
    let restored = decompress(&chunk).unwrap();
    assert_eq!(data, restored);
}

#[test]
fn test_large_typesize_shuffle_roundtrip() {
    for typesize in [16, 32, 64, 128] {
        let n = 1024;
        let data: Vec<u8> = (0..n).map(|i| (i % 256) as u8).collect();
        let mut shuffled = vec![0u8; n];
        let mut restored = vec![0u8; n];

        filters::shuffle(typesize, &data, &mut shuffled);
        filters::unshuffle(typesize, &shuffled, &mut restored);
        assert_eq!(data, restored, "Shuffle roundtrip failed for typesize={typesize}");
    }
}

// ─── Non-aligned data sizes ─────────────────────────────────────

#[test]
fn test_data_size_not_multiple_of_typesize() {
    // 1003 bytes with typesize=4 (not a multiple)
    let data: Vec<u8> = (0..1003u16).map(|i| (i % 256) as u8).collect();
    let cparams = CParams {
        compcode: BLOSC_LZ4,
        clevel: 5,
        typesize: 4,
        ..Default::default()
    };
    let chunk = compress(&data, &cparams).unwrap();
    let restored = decompress(&chunk).unwrap();
    assert_eq!(data, restored);
}

#[test]
fn test_various_small_sizes() {
    for size in [1, 2, 3, 7, 15, 31, 33, 100, 255, 256, 1000] {
        let data: Vec<u8> = (0..size).map(|i| (i * 7 + 3) as u8).collect();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            ..Default::default()
        };
        let chunk = compress(&data, &cparams).unwrap();
        let restored = decompress(&chunk).unwrap();
        assert_eq!(data, restored, "Failed for size={size}");
    }
}

#[test]
fn test_size_smaller_than_blocksize() {
    let data = vec![42u8; 500]; // Much smaller than default blocksize
    let cparams = CParams {
        compcode: BLOSC_ZSTD,
        clevel: 5,
        typesize: 4,
        ..Default::default()
    };
    let chunk = compress(&data, &cparams).unwrap();
    let restored = decompress(&chunk).unwrap();
    assert_eq!(data, restored);
}

#[test]
fn test_odd_typesize_7() {
    let data: Vec<u8> = (0..7000u16).map(|i| (i % 256) as u8).collect();
    let cparams = CParams {
        compcode: BLOSC_LZ4,
        clevel: 5,
        typesize: 7,
        ..Default::default()
    };
    let chunk = compress(&data, &cparams).unwrap();
    let restored = decompress(&chunk).unwrap();
    assert_eq!(data, restored);
}

// ─── Incompressible data ─────────────────────────────────────────

#[test]
fn test_incompressible_random_data() {
    // Pseudo-random data that won't compress
    let data: Vec<u8> = (0..50000u32)
        .map(|i| {
            let x = i.wrapping_mul(2654435761);
            (x >> 16) as u8
        })
        .collect();

    for compcode in [BLOSC_BLOSCLZ, BLOSC_LZ4, BLOSC_ZLIB, BLOSC_ZSTD] {
        let cparams = CParams {
            compcode,
            clevel: 5,
            typesize: 1,
            ..Default::default()
        };
        let chunk = compress(&data, &cparams).unwrap();
        let restored = decompress(&chunk).unwrap();
        assert_eq!(data, restored, "Incompressible roundtrip failed for codec={compcode}");
    }
}

#[test]
fn test_incompressible_with_shuffle() {
    let data: Vec<u8> = (0..40000u32)
        .map(|i| ((i.wrapping_mul(7919) >> 8) & 0xFF) as u8)
        .collect();

    let cparams = CParams {
        compcode: BLOSC_LZ4,
        clevel: 5,
        typesize: 4,
        filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
        ..Default::default()
    };
    let chunk = compress(&data, &cparams).unwrap();
    let restored = decompress(&chunk).unwrap();
    assert_eq!(data, restored);
}

#[test]
fn test_incompressible_cross_compat_c() {
    let _b = init();
    let data: Vec<u8> = (0..20000u32)
        .map(|i| ((i.wrapping_mul(48271) >> 12) & 0xFF) as u8)
        .collect();

    let cparams = CParams {
        compcode: BLOSC_LZ4,
        clevel: 5,
        typesize: 1,
        splitmode: BLOSC_NEVER_SPLIT,
        ..Default::default()
    };
    let chunk = compress(&data, &cparams).unwrap();

    let mut c_restored = vec![0u8; data.len()];
    let dsize = unsafe {
        ffi::blosc2_decompress(
            chunk.as_ptr() as *const _, chunk.len() as i32,
            c_restored.as_mut_ptr() as *mut _, c_restored.len() as i32,
        )
    };
    assert_eq!(dsize, data.len() as i32, "C decompress of incompressible data failed");
    assert_eq!(data, c_restored);
}

// ─── All clevels 0-9 ────────────────────────────────────────────

#[test]
fn test_all_clevels() {
    let data: Vec<u8> = b"Repeated pattern for clevel testing! "
        .iter().cycle().take(50000).copied().collect();

    for clevel in 0..=9u8 {
        for compcode in [BLOSC_BLOSCLZ, BLOSC_LZ4, BLOSC_ZSTD] {
            let cparams = CParams {
                compcode,
                clevel,
                typesize: 1,
                ..Default::default()
            };
            let chunk = compress(&data, &cparams).unwrap();
            let restored = decompress(&chunk).unwrap();
            assert_eq!(data, restored,
                "clevel={clevel} codec={compcode} roundtrip failed");
        }
    }
}

// ─── Explicit blocksize ──────────────────────────────────────────

#[test]
fn test_explicit_blocksize() {
    let data: Vec<u8> = (0..40000u32).flat_map(|i| i.to_le_bytes()).collect();

    for blocksize in [1024, 4096, 8192, 32768] {
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 4,
            blocksize,
            ..Default::default()
        };
        let chunk = compress(&data, &cparams).unwrap();
        let restored = decompress(&chunk).unwrap();
        assert_eq!(data, restored, "blocksize={blocksize} roundtrip failed");
    }
}
