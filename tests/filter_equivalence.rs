#![cfg(feature = "_ffi")]
mod common;
use blosc2_pure_rs::filters;
use common::ffi;

fn init_blosc2() -> common::Blosc2 {
    common::Blosc2::new()
}

#[test]
fn test_shuffle_matches_c() {
    let _b = init_blosc2();

    for typesize in [1, 2, 4, 8, 16] {
        let blocksize = 1024;
        let data: Vec<u8> = (0..blocksize).map(|i| (i * 7 + 13) as u8).collect();

        // Rust shuffle
        let mut rust_out = vec![0u8; blocksize];
        filters::shuffle(typesize, &data, &mut rust_out);

        // C shuffle
        let mut c_out = vec![0u8; blocksize];
        unsafe {
            ffi::blosc2_shuffle(
                typesize as i32,
                blocksize as i32,
                data.as_ptr() as *const _,
                c_out.as_mut_ptr() as *mut _,
            );
        }

        assert_eq!(rust_out, c_out, "Shuffle mismatch for typesize={typesize}");

        // Test unshuffle
        let mut rust_restored = vec![0u8; blocksize];
        filters::unshuffle(typesize, &rust_out, &mut rust_restored);

        let mut c_restored = vec![0u8; blocksize];
        unsafe {
            ffi::blosc2_unshuffle(
                typesize as i32,
                blocksize as i32,
                c_out.as_ptr() as *const _,
                c_restored.as_mut_ptr() as *mut _,
            );
        }

        assert_eq!(
            rust_restored, c_restored,
            "Unshuffle mismatch for typesize={typesize}"
        );
        assert_eq!(
            data, rust_restored,
            "Shuffle roundtrip failed for typesize={typesize}"
        );
    }
}

#[test]
fn test_shuffle_various_sizes() {
    let _b = init_blosc2();

    // Test with sizes that aren't perfect multiples of typesize
    for typesize in [2, 4, 8] {
        for blocksize in [33, 100, 255, 513, 1000, 4096] {
            let data: Vec<u8> = (0..blocksize).map(|i| (i * 3 + 5) as u8).collect();

            let mut rust_out = vec![0u8; blocksize];
            filters::shuffle(typesize, &data, &mut rust_out);

            let mut c_out = vec![0u8; blocksize];
            unsafe {
                ffi::blosc2_shuffle(
                    typesize as i32,
                    blocksize as i32,
                    data.as_ptr() as *const _,
                    c_out.as_mut_ptr() as *mut _,
                );
            }

            assert_eq!(
                rust_out, c_out,
                "Shuffle mismatch for typesize={typesize} blocksize={blocksize}"
            );
        }
    }
}

#[test]
fn test_bitshuffle_matches_c() {
    let _b = init_blosc2();

    // Bitshuffle requires size to be a multiple of 8*typesize
    for typesize in [1, 2, 4, 8] {
        let n_elements = 128; // multiple of 8
        let blocksize = n_elements * typesize;
        let data: Vec<u8> = (0..blocksize).map(|i| (i * 11 + 3) as u8).collect();

        // Rust bitshuffle
        let mut rust_out = vec![0u8; blocksize];
        filters::bitshuffle(typesize, &data, &mut rust_out);

        // C bitshuffle
        let mut c_out = vec![0u8; blocksize];
        unsafe {
            ffi::blosc2_bitshuffle(
                typesize as i32,
                blocksize as i32,
                data.as_ptr() as *const _,
                c_out.as_mut_ptr() as *mut _,
            );
        }

        assert_eq!(
            rust_out, c_out,
            "Bitshuffle mismatch for typesize={typesize}"
        );

        // Test roundtrip
        let mut rust_restored = vec![0u8; blocksize];
        filters::bitunshuffle(typesize, &rust_out, &mut rust_restored);
        assert_eq!(
            data, rust_restored,
            "Bitshuffle roundtrip failed for typesize={typesize}"
        );
    }
}
