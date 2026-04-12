//! SSE2-accelerated shuffle and unshuffle routines.
//!
//! Ported from c-blosc2/blosc/shuffle-sse2.c.
//! These process 16 elements at a time using 128-bit SIMD registers.

#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;
#[cfg(target_arch = "x86")]
use std::arch::x86::*;

/// SSE2 shuffle for typesize=4 (float32). Processes 16 elements (64 bytes) per iteration.
#[target_feature(enable = "sse2")]
unsafe fn shuffle4_sse2(dest: &mut [u8], src: &[u8], vectorizable_elements: usize, total_elements: usize) {
    let mut i = 0;
    while i < vectorizable_elements {
        let src_base = i * 4;
        // Load 4 x 16-byte vectors (64 bytes = 16 float32 elements)
        let mut xmm0 = [_mm_setzero_si128(); 4];
        let mut xmm1 = [_mm_setzero_si128(); 4];

        for j in 0..4 {
            xmm0[j] = _mm_loadu_si128(src.as_ptr().add(src_base + j * 16) as *const __m128i);
            xmm1[j] = _mm_shuffle_epi32(xmm0[j], 0xd8);
            xmm0[j] = _mm_shuffle_epi32(xmm0[j], 0x8d);
            xmm0[j] = _mm_unpacklo_epi8(xmm1[j], xmm0[j]);
            xmm1[j] = _mm_shuffle_epi32(xmm0[j], 0x4e);
            xmm0[j] = _mm_unpacklo_epi16(xmm0[j], xmm1[j]);
        }
        // Transpose double words
        for j in 0..2 {
            xmm1[j * 2] = _mm_unpacklo_epi32(xmm0[j * 2], xmm0[j * 2 + 1]);
            xmm1[j * 2 + 1] = _mm_unpackhi_epi32(xmm0[j * 2], xmm0[j * 2 + 1]);
        }
        // Transpose quad words
        for j in 0..2 {
            xmm0[j * 2] = _mm_unpacklo_epi64(xmm1[j], xmm1[j + 2]);
            xmm0[j * 2 + 1] = _mm_unpackhi_epi64(xmm1[j], xmm1[j + 2]);
        }
        // Store
        let dest_base = i;
        for j in 0..4 {
            _mm_storeu_si128(dest.as_mut_ptr().add(dest_base + j * total_elements) as *mut __m128i, xmm0[j]);
        }

        i += 16;
    }
}

/// SSE2 unshuffle for typesize=4 (float32).
#[target_feature(enable = "sse2")]
unsafe fn unshuffle4_sse2(dest: &mut [u8], src: &[u8], vectorizable_elements: usize, total_elements: usize) {
    let mut i = 0;
    while i < vectorizable_elements {
        let src_base = i;
        let mut xmm0 = [_mm_setzero_si128(); 4];
        let mut xmm1 = [_mm_setzero_si128(); 4];

        for j in 0..4 {
            xmm0[j] = _mm_loadu_si128(src.as_ptr().add(src_base + j * total_elements) as *const __m128i);
        }
        // Shuffle bytes
        for j in 0..2 {
            xmm1[j] = _mm_unpacklo_epi8(xmm0[j * 2], xmm0[j * 2 + 1]);
            xmm1[2 + j] = _mm_unpackhi_epi8(xmm0[j * 2], xmm0[j * 2 + 1]);
        }
        // Shuffle 2-byte words
        for j in 0..2 {
            xmm0[j] = _mm_unpacklo_epi16(xmm1[j * 2], xmm1[j * 2 + 1]);
            xmm0[2 + j] = _mm_unpackhi_epi16(xmm1[j * 2], xmm1[j * 2 + 1]);
        }
        // Store in proper order
        let dest_base = i * 4;
        _mm_storeu_si128(dest.as_mut_ptr().add(dest_base + 0) as *mut __m128i, xmm0[0]);
        _mm_storeu_si128(dest.as_mut_ptr().add(dest_base + 16) as *mut __m128i, xmm0[2]);
        _mm_storeu_si128(dest.as_mut_ptr().add(dest_base + 32) as *mut __m128i, xmm0[1]);
        _mm_storeu_si128(dest.as_mut_ptr().add(dest_base + 48) as *mut __m128i, xmm0[3]);

        i += 16;
    }
}

/// SSE2 shuffle for typesize=8 (float64). Processes 16 elements (128 bytes) per iteration.
#[target_feature(enable = "sse2")]
unsafe fn shuffle8_sse2(dest: &mut [u8], src: &[u8], vectorizable_elements: usize, total_elements: usize) {
    let mut i = 0;
    while i < vectorizable_elements {
        let src_base = i * 8;
        let mut xmm0 = [_mm_setzero_si128(); 8];
        let mut xmm1 = [_mm_setzero_si128(); 8];

        // Load and transpose bytes
        for k in 0..8 {
            xmm0[k] = _mm_loadu_si128(src.as_ptr().add(src_base + k * 16) as *const __m128i);
            xmm1[k] = _mm_shuffle_epi32(xmm0[k], 0x4e);
            xmm1[k] = _mm_unpacklo_epi8(xmm0[k], xmm1[k]);
        }
        // Transpose words
        for k in 0..4 {
            let l = k * 2;
            xmm0[k * 2] = _mm_unpacklo_epi16(xmm1[l], xmm1[l + 1]);
            xmm0[k * 2 + 1] = _mm_unpackhi_epi16(xmm1[l], xmm1[l + 1]);
        }
        // Transpose double words
        let mut l;
        for k in 0..4 {
            l = k;
            if k == 2 { l = k + 2; }
            if k == 3 { l = k + 2; }
            // Correct indexing matching C code
            xmm1[k * 2] = _mm_unpacklo_epi32(xmm0[l], xmm0[l + 2]);
            xmm1[k * 2 + 1] = _mm_unpackhi_epi32(xmm0[l], xmm0[l + 2]);
        }
        // Transpose quad words
        for k in 0..4 {
            xmm0[k * 2] = _mm_unpacklo_epi64(xmm1[k], xmm1[k + 4]);
            xmm0[k * 2 + 1] = _mm_unpackhi_epi64(xmm1[k], xmm1[k + 4]);
        }
        // Store
        let dest_base = i;
        for k in 0..8 {
            _mm_storeu_si128(dest.as_mut_ptr().add(dest_base + k * total_elements) as *mut __m128i, xmm0[k]);
        }

        i += 16;
    }
}

/// SSE2 unshuffle for typesize=8 (float64).
#[target_feature(enable = "sse2")]
unsafe fn unshuffle8_sse2(dest: &mut [u8], src: &[u8], vectorizable_elements: usize, total_elements: usize) {
    let mut i = 0;
    while i < vectorizable_elements {
        let src_base = i;
        let mut xmm0 = [_mm_setzero_si128(); 8];
        let mut xmm1 = [_mm_setzero_si128(); 8];

        for j in 0..8 {
            xmm0[j] = _mm_loadu_si128(src.as_ptr().add(src_base + j * total_elements) as *const __m128i);
        }
        // Shuffle bytes
        for j in 0..4 {
            xmm1[j] = _mm_unpacklo_epi8(xmm0[j * 2], xmm0[j * 2 + 1]);
            xmm1[4 + j] = _mm_unpackhi_epi8(xmm0[j * 2], xmm0[j * 2 + 1]);
        }
        // Shuffle 2-byte words
        for j in 0..4 {
            xmm0[j] = _mm_unpacklo_epi16(xmm1[j * 2], xmm1[j * 2 + 1]);
            xmm0[4 + j] = _mm_unpackhi_epi16(xmm1[j * 2], xmm1[j * 2 + 1]);
        }
        // Shuffle 4-byte dwords
        for j in 0..4 {
            xmm1[j] = _mm_unpacklo_epi32(xmm0[j * 2], xmm0[j * 2 + 1]);
            xmm1[4 + j] = _mm_unpackhi_epi32(xmm0[j * 2], xmm0[j * 2 + 1]);
        }
        // Store in proper order
        let dest_base = i * 8;
        _mm_storeu_si128(dest.as_mut_ptr().add(dest_base + 0) as *mut __m128i, xmm1[0]);
        _mm_storeu_si128(dest.as_mut_ptr().add(dest_base + 16) as *mut __m128i, xmm1[4]);
        _mm_storeu_si128(dest.as_mut_ptr().add(dest_base + 32) as *mut __m128i, xmm1[2]);
        _mm_storeu_si128(dest.as_mut_ptr().add(dest_base + 48) as *mut __m128i, xmm1[6]);
        _mm_storeu_si128(dest.as_mut_ptr().add(dest_base + 64) as *mut __m128i, xmm1[1]);
        _mm_storeu_si128(dest.as_mut_ptr().add(dest_base + 80) as *mut __m128i, xmm1[5]);
        _mm_storeu_si128(dest.as_mut_ptr().add(dest_base + 96) as *mut __m128i, xmm1[3]);
        _mm_storeu_si128(dest.as_mut_ptr().add(dest_base + 112) as *mut __m128i, xmm1[7]);

        i += 16;
    }
}

/// SSE2 shuffle for typesize=2.
#[target_feature(enable = "sse2")]
unsafe fn shuffle2_sse2(dest: &mut [u8], src: &[u8], vectorizable_elements: usize, total_elements: usize) {
    let mut i = 0;
    while i < vectorizable_elements {
        let src_base = i * 2;
        let mut xmm0 = [_mm_setzero_si128(); 2];
        let mut xmm1 = [_mm_setzero_si128(); 2];

        for k in 0..2 {
            xmm0[k] = _mm_loadu_si128(src.as_ptr().add(src_base + k * 16) as *const __m128i);
            xmm0[k] = _mm_shufflelo_epi16(xmm0[k], 0xd8);
            xmm0[k] = _mm_shufflehi_epi16(xmm0[k], 0xd8);
            xmm0[k] = _mm_shuffle_epi32(xmm0[k], 0xd8);
            xmm1[k] = _mm_shuffle_epi32(xmm0[k], 0x4e);
            xmm0[k] = _mm_unpacklo_epi8(xmm0[k], xmm1[k]);
            xmm0[k] = _mm_shuffle_epi32(xmm0[k], 0xd8);
            xmm1[k] = _mm_shuffle_epi32(xmm0[k], 0x4e);
            xmm0[k] = _mm_unpacklo_epi16(xmm0[k], xmm1[k]);
            xmm0[k] = _mm_shuffle_epi32(xmm0[k], 0xd8);
        }
        xmm1[0] = _mm_unpacklo_epi64(xmm0[0], xmm0[1]);
        xmm1[1] = _mm_unpackhi_epi64(xmm0[0], xmm0[1]);

        let dest_base = i;
        for k in 0..2 {
            _mm_storeu_si128(dest.as_mut_ptr().add(dest_base + k * total_elements) as *mut __m128i, xmm1[k]);
        }

        i += 16;
    }
}

/// SSE2 unshuffle for typesize=2.
#[target_feature(enable = "sse2")]
unsafe fn unshuffle2_sse2(dest: &mut [u8], src: &[u8], vectorizable_elements: usize, total_elements: usize) {
    let mut i = 0;
    while i < vectorizable_elements {
        let src_base = i;
        let xmm0_0 = _mm_loadu_si128(src.as_ptr().add(src_base) as *const __m128i);
        let xmm0_1 = _mm_loadu_si128(src.as_ptr().add(src_base + total_elements) as *const __m128i);

        let lo = _mm_unpacklo_epi8(xmm0_0, xmm0_1);
        let hi = _mm_unpackhi_epi8(xmm0_0, xmm0_1);

        let dest_base = i * 2;
        _mm_storeu_si128(dest.as_mut_ptr().add(dest_base) as *mut __m128i, lo);
        _mm_storeu_si128(dest.as_mut_ptr().add(dest_base + 16) as *mut __m128i, hi);

        i += 16;
    }
}

/// Dispatch shuffle to SSE2 or generic based on typesize and availability.
/// Returns true if SSE2 was used.
#[cfg(any(target_arch = "x86_64", target_arch = "x86"))]
pub fn shuffle_sse2(typesize: usize, src: &[u8], dest: &mut [u8]) -> bool {
    // SSE2 is guaranteed on x86_64; on x86 check at runtime
    #[cfg(target_arch = "x86")]
    if !is_x86_feature_detected!("sse2") {
        return false;
    }

    let blocksize = src.len();
    let vectorized_chunk_size = typesize * 16; // 16 bytes per __m128i
    if blocksize < vectorized_chunk_size {
        return false;
    }

    let vectorizable_bytes = blocksize - (blocksize % vectorized_chunk_size);
    let vectorizable_elements = vectorizable_bytes / typesize;
    let total_elements = blocksize / typesize;

    unsafe {
        match typesize {
            2 => shuffle2_sse2(dest, src, vectorizable_elements, total_elements),
            4 => shuffle4_sse2(dest, src, vectorizable_elements, total_elements),
            8 => shuffle8_sse2(dest, src, vectorizable_elements, total_elements),
            _ => return false,
        }
    }

    // Handle leftover elements with generic shuffle
    if vectorizable_bytes < blocksize {
        let neblock_quot = blocksize / typesize;
        let neblock_rem = blocksize % typesize;
        // Only process non-vectorized remainder
        unsafe {
            for j in 0..typesize {
                let dest_base = j * neblock_quot;
                for i in vectorizable_elements..neblock_quot {
                    *dest.get_unchecked_mut(dest_base + i) =
                        *src.get_unchecked(i * typesize + j);
                }
            }
        }
        if neblock_rem > 0 {
            let start = blocksize - neblock_rem;
            dest[start..blocksize].copy_from_slice(&src[start..blocksize]);
        }
    }

    true
}

/// Dispatch unshuffle to SSE2 or generic.
/// Returns true if SSE2 was used.
#[cfg(any(target_arch = "x86_64", target_arch = "x86"))]
pub fn unshuffle_sse2(typesize: usize, src: &[u8], dest: &mut [u8]) -> bool {
    // SSE2 is guaranteed on x86_64; on x86 check at runtime
    #[cfg(target_arch = "x86")]
    if !is_x86_feature_detected!("sse2") {
        return false;
    }

    let blocksize = src.len();
    let vectorized_chunk_size = typesize * 16;
    if blocksize < vectorized_chunk_size {
        return false;
    }

    let vectorizable_bytes = blocksize - (blocksize % vectorized_chunk_size);
    let vectorizable_elements = vectorizable_bytes / typesize;
    let total_elements = blocksize / typesize;

    unsafe {
        match typesize {
            2 => unshuffle2_sse2(dest, src, vectorizable_elements, total_elements),
            4 => unshuffle4_sse2(dest, src, vectorizable_elements, total_elements),
            8 => unshuffle8_sse2(dest, src, vectorizable_elements, total_elements),
            _ => return false,
        }
    }

    // Handle leftover with generic unshuffle
    if vectorizable_bytes < blocksize {
        let neblock_quot = blocksize / typesize;
        let neblock_rem = blocksize % typesize;
        unsafe {
            for i in vectorizable_elements..neblock_quot {
                let dest_base = i * typesize;
                for j in 0..typesize {
                    *dest.get_unchecked_mut(dest_base + j) =
                        *src.get_unchecked(j * neblock_quot + i);
                }
            }
        }
        if neblock_rem > 0 {
            let start = blocksize - neblock_rem;
            dest[start..blocksize].copy_from_slice(&src[start..blocksize]);
        }
    }

    true
}

// Non-x86 stub
#[cfg(not(any(target_arch = "x86_64", target_arch = "x86")))]
pub fn shuffle_sse2(_typesize: usize, _src: &[u8], _dest: &mut [u8]) -> bool { false }
#[cfg(not(any(target_arch = "x86_64", target_arch = "x86")))]
pub fn unshuffle_sse2(_typesize: usize, _src: &[u8], _dest: &mut [u8]) -> bool { false }
