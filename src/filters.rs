use crate::constants::*;
use std::collections::HashMap;
use std::sync::{OnceLock, RwLock};

pub type FilterForwardFn =
    fn(meta: u8, typesize: usize, block_offset: usize, src: &[u8], dest: &mut [u8]);
pub type FilterBackwardFn =
    fn(meta: u8, typesize: usize, block_offset: usize, src: &[u8], dest: &mut [u8]);

#[derive(Clone, Copy)]
struct UserFilter {
    forward: FilterForwardFn,
    backward: FilterBackwardFn,
}

static USER_FILTERS: OnceLock<RwLock<HashMap<u8, UserFilter>>> = OnceLock::new();

fn user_filters() -> &'static RwLock<HashMap<u8, UserFilter>> {
    USER_FILTERS.get_or_init(|| RwLock::new(HashMap::new()))
}

pub fn register_filter(
    filter_id: u8,
    forward: FilterForwardFn,
    backward: FilterBackwardFn,
) -> Result<(), &'static str> {
    if filter_id < BLOSC2_USER_DEFINED_FILTERS_START {
        return Err("User-defined filter IDs must be >= 32");
    }
    user_filters()
        .write()
        .map_err(|_| "Filter registry poisoned")?
        .insert(filter_id, UserFilter { forward, backward });
    Ok(())
}

pub fn is_registered_filter(filter_id: u8) -> bool {
    user_filters()
        .read()
        .is_ok_and(|filters| filters.contains_key(&filter_id))
}

fn registered_filter(filter_id: u8) -> Option<UserFilter> {
    user_filters()
        .read()
        .ok()
        .and_then(|filters| filters.get(&filter_id).copied())
}

/// Apply byte-wise shuffle (transpose bytes within elements).
///
/// For each byte position j within a type of size `typesize`,
/// gather all j-th bytes of each element contiguously.
pub fn shuffle(typesize: usize, src: &[u8], dest: &mut [u8]) {
    let blocksize = src.len();
    if dest.len() < blocksize {
        return;
    }
    if typesize <= 1 || blocksize == 0 {
        dest[..blocksize].copy_from_slice(&src[..blocksize]);
        return;
    }
    if shuffle_common_width(typesize, src, dest) {
        return;
    }
    if simd::try_shuffle(typesize, src, dest) {
        return;
    }

    let neblock_quot = blocksize / typesize;
    let neblock_rem = blocksize % typesize;

    for j in 0..typesize {
        let dest_base = j * neblock_quot;
        for i in 0..neblock_quot {
            dest[dest_base + i] = src[i * typesize + j];
        }
    }

    if neblock_rem > 0 {
        let start = blocksize - neblock_rem;
        dest[start..blocksize].copy_from_slice(&src[start..blocksize]);
    }
}

/// Reverse byte-wise shuffle (untranspose bytes within elements).
pub fn unshuffle(typesize: usize, src: &[u8], dest: &mut [u8]) {
    let blocksize = src.len();
    if dest.len() < blocksize {
        return;
    }
    if typesize <= 1 || blocksize == 0 {
        dest[..blocksize].copy_from_slice(&src[..blocksize]);
        return;
    }
    if unshuffle_common_width(typesize, src, dest) {
        return;
    }
    if simd::try_unshuffle(typesize, src, dest) {
        return;
    }

    let neblock_quot = blocksize / typesize;
    let neblock_rem = blocksize % typesize;

    for i in 0..neblock_quot {
        let dest_base = i * typesize;
        for j in 0..typesize {
            dest[dest_base + j] = src[j * neblock_quot + i];
        }
    }

    if neblock_rem > 0 {
        let start = blocksize - neblock_rem;
        dest[start..blocksize].copy_from_slice(&src[start..blocksize]);
    }
}

fn shuffle_common_width(typesize: usize, src: &[u8], dest: &mut [u8]) -> bool {
    match typesize {
        2 => {
            shuffle2(src, dest);
            true
        }
        4 => {
            shuffle4(src, dest);
            true
        }
        8 => {
            shuffle8(src, dest);
            true
        }
        _ => false,
    }
}

fn unshuffle_common_width(typesize: usize, src: &[u8], dest: &mut [u8]) -> bool {
    match typesize {
        2 => {
            unshuffle2(src, dest);
            true
        }
        4 => {
            unshuffle4(src, dest);
            true
        }
        8 => {
            unshuffle8(src, dest);
            true
        }
        _ => false,
    }
}

fn shuffle2(src: &[u8], dest: &mut [u8]) {
    let nelements = src.len() / 2;
    let main_len = nelements * 2;
    let (d0, d1) = dest[..main_len].split_at_mut(nelements);
    for (i, element) in src[..main_len].chunks_exact(2).enumerate() {
        d0[i] = element[0];
        d1[i] = element[1];
    }
    dest[main_len..src.len()].copy_from_slice(&src[main_len..]);
}

fn unshuffle2(src: &[u8], dest: &mut [u8]) {
    let nelements = src.len() / 2;
    let main_len = nelements * 2;
    let (s0, s1) = src[..main_len].split_at(nelements);
    // SAFETY: main_len is derived from src.len() and dest was checked by
    // unshuffle(), so every unaligned element write lands within dest.
    unsafe {
        let out = dest.as_mut_ptr();
        for i in 0..nelements {
            let value = u16::from_ne_bytes([s0[i], s1[i]]);
            std::ptr::write_unaligned(out.add(i * 2).cast::<u16>(), value);
        }
    }
    dest[main_len..src.len()].copy_from_slice(&src[main_len..]);
}

fn shuffle4(src: &[u8], dest: &mut [u8]) {
    let nelements = src.len() / 4;
    let main_len = nelements * 4;
    let (d0, rest) = dest[..main_len].split_at_mut(nelements);
    let (d1, rest) = rest.split_at_mut(nelements);
    let (d2, d3) = rest.split_at_mut(nelements);
    for (i, element) in src[..main_len].chunks_exact(4).enumerate() {
        d0[i] = element[0];
        d1[i] = element[1];
        d2[i] = element[2];
        d3[i] = element[3];
    }
    dest[main_len..src.len()].copy_from_slice(&src[main_len..]);
}

fn unshuffle4(src: &[u8], dest: &mut [u8]) {
    let nelements = src.len() / 4;
    let main_len = nelements * 4;
    let (s0, rest) = src[..main_len].split_at(nelements);
    let (s1, rest) = rest.split_at(nelements);
    let (s2, s3) = rest.split_at(nelements);
    // SAFETY: main_len is derived from src.len() and dest was checked by
    // unshuffle(), so every unaligned element write lands within dest.
    unsafe {
        let out = dest.as_mut_ptr();
        for i in 0..nelements {
            let value = u32::from_ne_bytes([s0[i], s1[i], s2[i], s3[i]]);
            std::ptr::write_unaligned(out.add(i * 4).cast::<u32>(), value);
        }
    }
    dest[main_len..src.len()].copy_from_slice(&src[main_len..]);
}

fn shuffle8(src: &[u8], dest: &mut [u8]) {
    let nelements = src.len() / 8;
    let main_len = nelements * 8;
    let (d0, rest) = dest[..main_len].split_at_mut(nelements);
    let (d1, rest) = rest.split_at_mut(nelements);
    let (d2, rest) = rest.split_at_mut(nelements);
    let (d3, rest) = rest.split_at_mut(nelements);
    let (d4, rest) = rest.split_at_mut(nelements);
    let (d5, rest) = rest.split_at_mut(nelements);
    let (d6, d7) = rest.split_at_mut(nelements);
    for (i, element) in src[..main_len].chunks_exact(8).enumerate() {
        d0[i] = element[0];
        d1[i] = element[1];
        d2[i] = element[2];
        d3[i] = element[3];
        d4[i] = element[4];
        d5[i] = element[5];
        d6[i] = element[6];
        d7[i] = element[7];
    }
    dest[main_len..src.len()].copy_from_slice(&src[main_len..]);
}

fn unshuffle8(src: &[u8], dest: &mut [u8]) {
    let nelements = src.len() / 8;
    let main_len = nelements * 8;
    let (s0, rest) = src[..main_len].split_at(nelements);
    let (s1, rest) = rest.split_at(nelements);
    let (s2, rest) = rest.split_at(nelements);
    let (s3, rest) = rest.split_at(nelements);
    let (s4, rest) = rest.split_at(nelements);
    let (s5, rest) = rest.split_at(nelements);
    let (s6, s7) = rest.split_at(nelements);
    // SAFETY: main_len is derived from src.len() and dest was checked by
    // unshuffle(), so every unaligned element write lands within dest.
    unsafe {
        let out = dest.as_mut_ptr();
        for i in 0..nelements {
            let value =
                u64::from_ne_bytes([s0[i], s1[i], s2[i], s3[i], s4[i], s5[i], s6[i], s7[i]]);
            std::ptr::write_unaligned(out.add(i * 8).cast::<u64>(), value);
        }
    }
    dest[main_len..src.len()].copy_from_slice(&src[main_len..]);
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
mod simd {
    #[cfg(target_arch = "x86")]
    use std::arch::x86 as arch;
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64 as arch;

    pub fn try_shuffle(typesize: usize, src: &[u8], dest: &mut [u8]) -> bool {
        if matches!(typesize, 4 | 8) && try_shuffle_avx2(typesize, src, dest) {
            return true;
        }
        if typesize != 4 || src.len() < 64 || dest.len() < src.len() {
            return false;
        }
        if !std::arch::is_x86_feature_detected!("sse2") {
            return false;
        }

        // SAFETY: The wrapper checks that src/dest cover src.len(), that the
        // element width matches this implementation, and that SSE2 is present.
        unsafe {
            shuffle4_sse2(src, dest);
        }
        true
    }

    pub fn try_unshuffle(typesize: usize, src: &[u8], dest: &mut [u8]) -> bool {
        if matches!(typesize, 4 | 8) && try_unshuffle_avx2(typesize, src, dest) {
            return true;
        }
        if typesize != 4 || src.len() < 64 || dest.len() < src.len() {
            return false;
        }
        if !std::arch::is_x86_feature_detected!("sse2") {
            return false;
        }

        // SAFETY: The wrapper checks that src/dest cover src.len(), that the
        // element width matches this implementation, and that SSE2 is present.
        unsafe {
            unshuffle4_sse2(src, dest);
        }
        true
    }

    fn try_shuffle_avx2(typesize: usize, src: &[u8], dest: &mut [u8]) -> bool {
        if src.len() < 128 || dest.len() < src.len() || !src.len().is_multiple_of(typesize) {
            return false;
        }
        if !std::arch::is_x86_feature_detected!("avx2") {
            return false;
        }

        // SAFETY: The wrapper checks destination length, supported element
        // widths, full-element input, and AVX2 availability.
        unsafe {
            shuffle_avx2(typesize, src, dest);
        }
        true
    }

    fn try_unshuffle_avx2(typesize: usize, src: &[u8], dest: &mut [u8]) -> bool {
        if src.len() < 128 || dest.len() < src.len() || !src.len().is_multiple_of(typesize) {
            return false;
        }
        if !std::arch::is_x86_feature_detected!("avx2") {
            return false;
        }

        // SAFETY: The wrapper checks destination length, supported element
        // widths, full-element input, and AVX2 availability.
        unsafe {
            unshuffle_avx2(typesize, src, dest);
        }
        true
    }

    #[target_feature(enable = "avx2")]
    unsafe fn shuffle_avx2(typesize: usize, src: &[u8], dest: &mut [u8]) {
        let blocksize = src.len();
        let nelements = blocksize / typesize;
        let elements_per_vec = 32 / typesize;
        let simd_elements = nelements - (nelements % elements_per_vec);

        for group in 0..(simd_elements / elements_per_vec) {
            let src_base = group * 32;
            let vec = unsafe {
                arch::_mm256_loadu_si256(src.as_ptr().add(src_base) as *const arch::__m256i)
            };
            let mut bytes = [0u8; 32];
            unsafe {
                arch::_mm256_storeu_si256(bytes.as_mut_ptr() as *mut arch::__m256i, vec);
            }
            let elem_base = group * elements_per_vec;
            for lane in 0..elements_per_vec {
                for byte_idx in 0..typesize {
                    dest[byte_idx * nelements + elem_base + lane] =
                        bytes[lane * typesize + byte_idx];
                }
            }
        }

        for element in simd_elements..nelements {
            let src_base = element * typesize;
            for byte_idx in 0..typesize {
                dest[byte_idx * nelements + element] = src[src_base + byte_idx];
            }
        }
    }

    #[target_feature(enable = "avx2")]
    unsafe fn unshuffle_avx2(typesize: usize, src: &[u8], dest: &mut [u8]) {
        let blocksize = src.len();
        let nelements = blocksize / typesize;
        let elements_per_vec = 32 / typesize;
        let simd_elements = nelements - (nelements % elements_per_vec);

        for group in 0..(simd_elements / elements_per_vec) {
            let elem_base = group * elements_per_vec;
            let mut bytes = [0u8; 32];
            for lane in 0..elements_per_vec {
                for byte_idx in 0..typesize {
                    bytes[lane * typesize + byte_idx] =
                        src[byte_idx * nelements + elem_base + lane];
                }
            }
            let vec = unsafe { arch::_mm256_loadu_si256(bytes.as_ptr() as *const arch::__m256i) };
            unsafe {
                arch::_mm256_storeu_si256(
                    dest.as_mut_ptr().add(group * 32) as *mut arch::__m256i,
                    vec,
                );
            }
        }

        for element in simd_elements..nelements {
            let dest_base = element * typesize;
            for byte_idx in 0..typesize {
                dest[dest_base + byte_idx] = src[byte_idx * nelements + element];
            }
        }
    }

    #[target_feature(enable = "sse2")]
    unsafe fn shuffle4_sse2(src: &[u8], dest: &mut [u8]) {
        let blocksize = src.len();
        let nelements = blocksize / 4;
        let simd_elements = nelements - (nelements % 4);

        for group in 0..(simd_elements / 4) {
            let src_base = group * 16;
            let vec = unsafe {
                arch::_mm_loadu_si128(src.as_ptr().add(src_base) as *const arch::__m128i)
            };
            let mut bytes = [0u8; 16];
            unsafe {
                arch::_mm_storeu_si128(bytes.as_mut_ptr() as *mut arch::__m128i, vec);
            }
            let elem_base = group * 4;
            for lane in 0..4 {
                dest[elem_base + lane] = bytes[lane * 4];
                dest[nelements + elem_base + lane] = bytes[lane * 4 + 1];
                dest[nelements * 2 + elem_base + lane] = bytes[lane * 4 + 2];
                dest[nelements * 3 + elem_base + lane] = bytes[lane * 4 + 3];
            }
        }

        for element in simd_elements..nelements {
            let src_base = element * 4;
            dest[element] = src[src_base];
            dest[nelements + element] = src[src_base + 1];
            dest[nelements * 2 + element] = src[src_base + 2];
            dest[nelements * 3 + element] = src[src_base + 3];
        }

        let tail_start = nelements * 4;
        if tail_start < blocksize {
            dest[tail_start..blocksize].copy_from_slice(&src[tail_start..blocksize]);
        }
    }

    #[target_feature(enable = "sse2")]
    unsafe fn unshuffle4_sse2(src: &[u8], dest: &mut [u8]) {
        let blocksize = src.len();
        let nelements = blocksize / 4;
        let simd_elements = nelements - (nelements % 4);

        for group in 0..(simd_elements / 4) {
            let elem_base = group * 4;
            let mut bytes = [0u8; 16];
            for lane in 0..4 {
                bytes[lane * 4] = src[elem_base + lane];
                bytes[lane * 4 + 1] = src[nelements + elem_base + lane];
                bytes[lane * 4 + 2] = src[nelements * 2 + elem_base + lane];
                bytes[lane * 4 + 3] = src[nelements * 3 + elem_base + lane];
            }
            let vec = unsafe { arch::_mm_loadu_si128(bytes.as_ptr() as *const arch::__m128i) };
            unsafe {
                arch::_mm_storeu_si128(
                    dest.as_mut_ptr().add(group * 16) as *mut arch::__m128i,
                    vec,
                );
            }
        }

        for element in simd_elements..nelements {
            let dest_base = element * 4;
            dest[dest_base] = src[element];
            dest[dest_base + 1] = src[nelements + element];
            dest[dest_base + 2] = src[nelements * 2 + element];
            dest[dest_base + 3] = src[nelements * 3 + element];
        }

        let tail_start = nelements * 4;
        if tail_start < blocksize {
            dest[tail_start..blocksize].copy_from_slice(&src[tail_start..blocksize]);
        }
    }
}

#[cfg(not(any(target_arch = "x86", target_arch = "x86_64")))]
mod simd {
    pub fn try_shuffle(_typesize: usize, _src: &[u8], _dest: &mut [u8]) -> bool {
        false
    }

    pub fn try_unshuffle(_typesize: usize, _src: &[u8], _dest: &mut [u8]) -> bool {
        false
    }
}

#[cfg(test)]
fn bitshuffle_scalar_with_scratch(
    typesize: usize,
    src: &[u8],
    dest: &mut [u8],
    scratch: Option<&mut [u8]>,
) -> i64 {
    let blocksize = src.len();
    if typesize == 0 || blocksize == 0 || dest.len() < blocksize {
        return 0;
    }

    let size = blocksize / typesize;
    let size8 = size - (size % 8);
    let nbyte8 = size8 * typesize;

    if size8 > 0 {
        let mut owned_tmp;
        let tmp = if let Some(s) = scratch {
            if s.len() < nbyte8 {
                return 0;
            }
            &mut s[..nbyte8]
        } else {
            owned_tmp = vec![0u8; nbyte8];
            &mut owned_tmp[..]
        };

        trans_byte_elem(&src[..nbyte8], dest, size8, typesize);
        trans_bit_byte(&dest[..nbyte8], tmp, size8, typesize);
        trans_bitrow_eight(&tmp[..nbyte8], dest, size8, typesize);
    }

    if nbyte8 < blocksize {
        dest[nbyte8..blocksize].copy_from_slice(&src[nbyte8..blocksize]);
    }

    blocksize as i64
}

#[cfg(test)]
fn bitunshuffle_scalar_with_scratch(
    typesize: usize,
    src: &[u8],
    dest: &mut [u8],
    scratch: Option<&mut [u8]>,
) -> i64 {
    let blocksize = src.len();
    if typesize == 0 || blocksize == 0 || dest.len() < blocksize {
        return 0;
    }

    let size = blocksize / typesize;
    let size8 = size - (size % 8);
    let nbyte8 = size8 * typesize;

    if size8 > 0 {
        let mut owned_tmp;
        let tmp = if let Some(s) = scratch {
            if s.len() < nbyte8 {
                return 0;
            }
            &mut s[..nbyte8]
        } else {
            owned_tmp = vec![0u8; nbyte8];
            &mut owned_tmp[..]
        };

        trans_byte_bitrow(&src[..nbyte8], tmp, size8, typesize);
        shuffle_bit_eightelem(&tmp[..nbyte8], dest, size8, typesize);
    }

    if nbyte8 < blocksize {
        dest[nbyte8..blocksize].copy_from_slice(&src[nbyte8..blocksize]);
    }

    blocksize as i64
}

/// Transpose bytes within elements (step 1 of bitshuffle).
fn trans_byte_elem(src: &[u8], dest: &mut [u8], size: usize, elem_size: usize) {
    let mut ii = 0;
    while ii + 7 < size {
        for jj in 0..elem_size {
            let dest_base = jj * size + ii;
            let src_base = ii * elem_size + jj;
            for kk in 0..8 {
                dest[dest_base + kk] = src[src_base + kk * elem_size];
            }
        }
        ii += 8;
    }
    while ii < size {
        for jj in 0..elem_size {
            dest[jj * size + ii] = src[ii * elem_size + jj];
        }
        ii += 1;
    }
}

/// Transpose 8x8 bit matrix packed in a u64 (little-endian).
#[inline]
fn trans_bit_8x8(mut x: u64) -> u64 {
    let mut t: u64;
    t = (x ^ (x >> 7)) & 0x00AA00AA00AA00AA;
    x = x ^ t ^ (t << 7);
    t = (x ^ (x >> 14)) & 0x0000CCCC0000CCCC;
    x = x ^ t ^ (t << 14);
    t = (x ^ (x >> 28)) & 0x00000000F0F0F0F0;
    x ^ t ^ (t << 28)
}

/// Transpose bits within bytes (step 2 of bitshuffle).
fn trans_bit_byte(src: &[u8], dest: &mut [u8], size: usize, elem_size: usize) {
    let nbyte = elem_size * size;
    let nbyte_bitrow = nbyte / 8;

    for ii in 0..nbyte_bitrow {
        let x_bytes = &src[ii * 8..(ii + 1) * 8];
        let mut x = u64::from_le_bytes(x_bytes.try_into().unwrap());
        x = trans_bit_8x8(x);

        for kk in 0..8usize {
            dest[kk * nbyte_bitrow + ii] = (x & 0xFF) as u8;
            x >>= 8;
        }
    }
}

/// Transpose rows of shuffled bits within groups of 8 (step 3 of bitshuffle).
fn trans_bitrow_eight(src: &[u8], dest: &mut [u8], size: usize, elem_size: usize) {
    let nbyte_row = size / 8;

    // General transpose: (8, elem_size) blocks of nbyte_row bytes
    for ii in 0..8usize {
        for jj in 0..elem_size {
            let src_off = (ii * elem_size + jj) * nbyte_row;
            let dst_off = (jj * 8 + ii) * nbyte_row;
            dest[dst_off..dst_off + nbyte_row].copy_from_slice(&src[src_off..src_off + nbyte_row]);
        }
    }
}

/// Apply bit-wise shuffle (bitshuffle). Returns number of bytes processed.
///
/// Transposes bits within elements for better compression of typed data.
/// `scratch` is an optional pre-allocated temporary buffer (avoids per-call allocation).
pub fn bitshuffle(typesize: usize, src: &[u8], dest: &mut [u8]) -> i64 {
    bitshuffle_with_scratch(typesize, src, dest, None)
}

/// Bitshuffle with optional scratch buffer to avoid allocation.
pub fn bitshuffle_with_scratch(
    typesize: usize,
    src: &[u8],
    dest: &mut [u8],
    scratch: Option<&mut [u8]>,
) -> i64 {
    let blocksize = src.len();
    if typesize == 0 || blocksize == 0 || dest.len() < blocksize {
        return 0;
    }

    let size = blocksize / typesize;
    let size8 = size - (size % 8);
    let nbyte8 = size8 * typesize;

    if size8 > 0 {
        let mut owned_tmp;
        let tmp = if let Some(s) = scratch {
            if s.len() < nbyte8 {
                return 0;
            }
            &mut s[..nbyte8]
        } else {
            owned_tmp = vec![0u8; nbyte8];
            &mut owned_tmp[..]
        };

        if !bitshuffle_simd::try_bitshuffle(typesize, &src[..nbyte8], dest, tmp, size8) {
            trans_byte_elem(&src[..nbyte8], dest, size8, typesize);
            trans_bit_byte(&dest[..nbyte8], tmp, size8, typesize);
            trans_bitrow_eight(&tmp[..nbyte8], dest, size8, typesize);
        }
    }

    if nbyte8 < blocksize {
        dest[nbyte8..blocksize].copy_from_slice(&src[nbyte8..blocksize]);
    }

    blocksize as i64
}

/// Transpose bytes for each bit row (step 1 of untranspose).
fn trans_byte_bitrow(src: &[u8], dest: &mut [u8], size: usize, elem_size: usize) {
    let nbyte_row = size / 8;

    for jj in 0..elem_size {
        for ii in 0..nbyte_row {
            for kk in 0..8usize {
                dest[ii * 8 * elem_size + jj * 8 + kk] = src[(jj * 8 + kk) * nbyte_row + ii];
            }
        }
    }
}

/// Shuffle bits within eight-element groups (step 2 of untranspose).
fn shuffle_bit_eightelem(src: &[u8], dest: &mut [u8], size: usize, elem_size: usize) {
    let nbyte = elem_size * size;

    for jj in (0..8 * elem_size).step_by(8) {
        let mut ii = 0;
        while ii + 8 * elem_size - 1 < nbyte {
            let x_bytes = &src[ii + jj..ii + jj + 8];
            let mut x = u64::from_le_bytes(x_bytes.try_into().unwrap());
            x = trans_bit_8x8(x);

            for kk in 0..8usize {
                let out_index = ii + jj / 8 + kk * elem_size;
                dest[out_index] = (x & 0xFF) as u8;
                x >>= 8;
            }
            ii += 8 * elem_size;
        }
    }
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
mod bitshuffle_simd {
    use super::{trans_bit_8x8, trans_bitrow_eight, trans_byte_bitrow, trans_byte_elem};

    #[cfg(target_arch = "x86")]
    use std::arch::x86 as arch;
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64 as arch;

    pub fn try_bitshuffle(
        typesize: usize,
        src: &[u8],
        dest: &mut [u8],
        scratch: &mut [u8],
        size8: usize,
    ) -> bool {
        let nbyte8 = size8 * typesize;
        if nbyte8 < 128 || dest.len() < nbyte8 || scratch.len() < nbyte8 {
            return false;
        }
        if !std::arch::is_x86_feature_detected!("sse2") {
            return false;
        }

        trans_byte_elem(src, dest, size8, typesize);
        // SAFETY: The wrapper checks that SSE2 is present and that the source
        // and destination ranges cover the exact nbyte8 bytes processed.
        unsafe {
            trans_bit_byte_sse2(&dest[..nbyte8], scratch, nbyte8);
        }
        trans_bitrow_eight(&scratch[..nbyte8], dest, size8, typesize);
        true
    }

    pub fn try_bitunshuffle(
        typesize: usize,
        src: &[u8],
        dest: &mut [u8],
        scratch: &mut [u8],
        size8: usize,
    ) -> bool {
        let nbyte8 = size8 * typesize;
        if nbyte8 < 128 || dest.len() < nbyte8 || scratch.len() < nbyte8 {
            return false;
        }
        if !std::arch::is_x86_feature_detected!("sse2") {
            return false;
        }

        trans_byte_bitrow(src, scratch, size8, typesize);
        // SAFETY: The wrapper checks that SSE2 is present and that the source
        // and destination ranges cover the exact nbyte8 bytes processed.
        unsafe {
            shuffle_bit_eightelem_sse2(&scratch[..nbyte8], dest, size8, typesize);
        }
        true
    }

    #[target_feature(enable = "sse2")]
    unsafe fn trans_bit_byte_sse2(src: &[u8], dest: &mut [u8], nbyte: usize) {
        let nbyte_bitrow = nbyte / 8;
        for ii in 0..nbyte_bitrow {
            let x = unsafe { load_u64_sse2(src.as_ptr().add(ii * 8)) };
            let mut transposed = trans_bit_8x8(x);
            for kk in 0..8usize {
                dest[kk * nbyte_bitrow + ii] = (transposed & 0xFF) as u8;
                transposed >>= 8;
            }
        }
    }

    #[target_feature(enable = "sse2")]
    unsafe fn shuffle_bit_eightelem_sse2(
        src: &[u8],
        dest: &mut [u8],
        size: usize,
        elem_size: usize,
    ) {
        let nbyte = elem_size * size;
        for jj in (0..8 * elem_size).step_by(8) {
            let mut ii = 0;
            while ii + 8 * elem_size - 1 < nbyte {
                let x = unsafe { load_u64_sse2(src.as_ptr().add(ii + jj)) };
                let mut transposed = trans_bit_8x8(x);

                for kk in 0..8usize {
                    let out_index = ii + jj / 8 + kk * elem_size;
                    dest[out_index] = (transposed & 0xFF) as u8;
                    transposed >>= 8;
                }
                ii += 8 * elem_size;
            }
        }
    }

    #[target_feature(enable = "sse2")]
    unsafe fn load_u64_sse2(ptr: *const u8) -> u64 {
        let vec = unsafe { arch::_mm_loadl_epi64(ptr as *const arch::__m128i) };
        arch::_mm_cvtsi128_si64(vec) as u64
    }
}

#[cfg(not(any(target_arch = "x86", target_arch = "x86_64")))]
mod bitshuffle_simd {
    pub fn try_bitshuffle(
        _typesize: usize,
        _src: &[u8],
        _dest: &mut [u8],
        _scratch: &mut [u8],
        _size8: usize,
    ) -> bool {
        false
    }

    pub fn try_bitunshuffle(
        _typesize: usize,
        _src: &[u8],
        _dest: &mut [u8],
        _scratch: &mut [u8],
        _size8: usize,
    ) -> bool {
        false
    }
}

/// Reverse bit-wise shuffle (bitunshuffle). Returns number of bytes processed.
pub fn bitunshuffle(typesize: usize, src: &[u8], dest: &mut [u8]) -> i64 {
    bitunshuffle_with_scratch(typesize, src, dest, None)
}

/// Bitunshuffle with optional scratch buffer to avoid allocation.
pub fn bitunshuffle_with_scratch(
    typesize: usize,
    src: &[u8],
    dest: &mut [u8],
    scratch: Option<&mut [u8]>,
) -> i64 {
    let blocksize = src.len();
    if typesize == 0 || blocksize == 0 || dest.len() < blocksize {
        return 0;
    }

    let size = blocksize / typesize;
    let size8 = size - (size % 8);
    let nbyte8 = size8 * typesize;

    if size8 > 0 {
        let mut owned_tmp;
        let tmp = if let Some(s) = scratch {
            if s.len() < nbyte8 {
                return 0;
            }
            &mut s[..nbyte8]
        } else {
            owned_tmp = vec![0u8; nbyte8];
            &mut owned_tmp[..]
        };

        if !bitshuffle_simd::try_bitunshuffle(typesize, &src[..nbyte8], dest, tmp, size8) {
            trans_byte_bitrow(&src[..nbyte8], tmp, size8, typesize);
            shuffle_bit_eightelem(&tmp[..nbyte8], dest, size8, typesize);
        }
    }

    if nbyte8 < blocksize {
        dest[nbyte8..blocksize].copy_from_slice(&src[nbyte8..blocksize]);
    }

    blocksize as i64
}

/// Apply delta encoding.
///
/// If `offset == 0` (reference block), XOR each element with the previous element
/// in the reference. Otherwise, XOR with the corresponding element in the reference.
pub fn delta_encode(
    dref: &[u8],
    offset: usize,
    nbytes: usize,
    typesize: usize,
    src: &[u8],
    dest: &mut [u8],
) {
    if typesize == 0 || src.len() < nbytes || dest.len() < nbytes {
        return;
    }
    // Match C delta_encoder: 1, 2, 4, 8 use that element width; everything else
    // degrades to 8 (when a multiple of 8) or 1. Using the requested typesize
    // directly would produce output incompatible with the C library.
    let effective_typesize = match typesize {
        1 | 2 | 4 | 8 => typesize,
        n if n % 8 == 0 => 8,
        _ => 1,
    };
    if offset == 0 {
        // Reference block: delta against previous elements in dref.
        let head = effective_typesize.min(nbytes);
        if dref.len() < nbytes.max(head) {
            return;
        }
        dest[..head].copy_from_slice(&dref[..head]);
        for i in effective_typesize..nbytes {
            dest[i] = src[i] ^ dref[i - effective_typesize];
        }
    } else {
        // Non-reference block: delta against dref.
        if dref.len() < nbytes {
            return;
        }
        for i in 0..nbytes {
            dest[i] = src[i] ^ dref[i];
        }
    }
}

/// Reverse delta encoding (in-place).
/// For offset=0 (reference block), decode is self-referential: dest[i] ^= dest[i-typesize].
/// For offset>0, decode uses dref: dest[i] ^= dref[i].
pub fn delta_decode(
    dref: Option<&[u8]>,
    offset: usize,
    nbytes: usize,
    typesize: usize,
    dest: &mut [u8],
) {
    if typesize == 0 || dest.len() < nbytes {
        return;
    }
    let effective_typesize = match typesize {
        1 | 2 | 4 | 8 => typesize,
        n if n % 8 == 0 => 8,
        _ => 1,
    };
    if offset == 0 {
        // Reference block: self-referential decode (dest[i] ^= dest[i-typesize])
        for i in effective_typesize..nbytes {
            dest[i] ^= dest[i - effective_typesize];
        }
    } else if let Some(dref) = dref {
        // Non-reference block: undo delta against dref
        if dref.len() < nbytes {
            return;
        }
        for i in 0..nbytes {
            dest[i] ^= dref[i];
        }
    }
}

/// Apply the forward filter pipeline to a block.
///
/// Returns the filtered data (may swap between `buf1` and `buf2`).
/// The caller provides two working buffers; `src` is the input.
#[allow(clippy::too_many_arguments)]
pub fn pipeline_forward(
    src: &[u8],
    buf1: &mut [u8],
    buf2: &mut [u8],
    filters: &[u8; BLOSC2_MAX_FILTERS],
    filters_meta: &[u8; BLOSC2_MAX_FILTERS],
    typesize: usize,
    block_offset: usize,
    dref: Option<&[u8]>,
) -> usize {
    let bsize = src.len();
    if buf1.len() < bsize || buf2.len() < bsize {
        return 0;
    }

    // Track current data location: 0 = src (read-only), 1 = buf1, 2 = buf2
    // Start from src without copying — first filter reads src directly.
    let mut current = 0u8;

    for i in 0..BLOSC2_MAX_FILTERS {
        let filter = filters[i];
        if filter == BLOSC_NOFILTER {
            continue;
        }

        // Determine input and output buffers.
        // Input: src (0), buf1 (1), or buf2 (2)
        // Output: alternates between buf1 and buf2
        let out_buf = if current == 2 { 1u8 } else { 2u8 };

        let (inp, out) = match (current, out_buf) {
            (0, 1) => (&src[..bsize], &mut buf1[..bsize]),
            (0, 2) => (&src[..bsize], &mut buf2[..bsize]),
            (1, 2) => (&buf1[..bsize], &mut buf2[..bsize]),
            (2, 1) => (&buf2[..bsize], &mut buf1[..bsize]),
            _ => unreachable!("filter pipeline cannot read and write the same buffer"),
        };

        match filter {
            BLOSC_SHUFFLE => {
                let ts = if filters_meta[i] == 0 {
                    typesize
                } else {
                    filters_meta[i] as usize
                };
                shuffle(ts, inp, out);
            }
            BLOSC_BITSHUFFLE => {
                bitshuffle(typesize, inp, out);
            }
            BLOSC_DELTA => {
                let actual_dref = dref.unwrap_or(src);
                delta_encode(actual_dref, block_offset, bsize, typesize, inp, out);
            }
            BLOSC_TRUNC_PREC => {
                // C treats filters_meta as int8_t — negative values have Python-style
                // "drop this many mantissa bits" semantics.
                let prec = filters_meta[i] as i8;
                trunc_prec_forward(inp, out, typesize, prec);
            }
            _ => {
                if let Some(filter) = registered_filter(filter) {
                    (filter.forward)(filters_meta[i], typesize, block_offset, inp, out);
                } else {
                    out.copy_from_slice(inp);
                }
            }
        }

        current = out_buf;
    }

    // If no filters were active, copy src to buf1
    if current == 0 {
        buf1[..bsize].copy_from_slice(src);
        return 1;
    }

    current as usize
}

/// Apply the backward filter pipeline to a block (in-place friendly).
///
/// Returns which buffer holds the result (1 or 2).
#[allow(clippy::too_many_arguments)]
pub fn pipeline_backward(
    buf1: &mut [u8],
    buf2: &mut [u8],
    bsize: usize,
    filters: &[u8; BLOSC2_MAX_FILTERS],
    filters_meta: &[u8; BLOSC2_MAX_FILTERS],
    typesize: usize,
    block_offset: usize,
    dref: Option<&[u8]>,
    current_buf: usize,
) -> usize {
    if current_buf != 1 && current_buf != 2 {
        return 0;
    }
    if buf1.len() < bsize || buf2.len() < bsize {
        return 0;
    }
    let mut current = current_buf as u8;

    // Filters applied in reverse order
    for i in (0..BLOSC2_MAX_FILTERS).rev() {
        let filter = filters[i];
        if filter == BLOSC_NOFILTER {
            continue;
        }

        let (inp, out) = if current == 1 {
            (&buf1[..bsize], &mut buf2[..bsize])
        } else {
            (&buf2[..bsize], &mut buf1[..bsize])
        };

        match filter {
            BLOSC_SHUFFLE => {
                let ts = if filters_meta[i] == 0 {
                    typesize
                } else {
                    filters_meta[i] as usize
                };
                unshuffle(ts, inp, out);
            }
            BLOSC_BITSHUFFLE => {
                bitunshuffle(typesize, inp, out);
            }
            BLOSC_DELTA => {
                // Delta decode: copy data to output, then decode in-place
                out.copy_from_slice(inp);
                delta_decode(dref, block_offset, bsize, typesize, out);
            }
            BLOSC_TRUNC_PREC => {
                // Truncation is lossy — backward is a no-op (data already truncated)
                out.copy_from_slice(inp);
            }
            _ => {
                if let Some(filter_fn) = registered_filter(filter) {
                    (filter_fn.backward)(filters_meta[i], typesize, block_offset, inp, out);
                } else {
                    out.copy_from_slice(inp);
                }
            }
        }

        current = if current == 1 { 2 } else { 1 };
    }

    current as usize
}

/// Truncate precision: zero out least-significant bits of floating-point values.
// See c-blosc2/blosc/trunc-prec.c. The filter zeros *mantissa* bits of IEEE-754
// floats — sign and exponent are preserved. Only typesize 4 (f32) and 8 (f64)
// are supported by the C library. The `prec_bits` value is signed:
//   > 0 → absolute mantissa bits to keep (python-index-from-start semantics)
//   < 0 → mantissa bits to drop (python-negative-index semantics)
//   = 0 → the C library treats this as "keep 0 bits" (i.e. drop all mantissa).
const BITS_MANTISSA_F32: i32 = 23;
const BITS_MANTISSA_F64: i32 = 52;

fn trunc_prec_forward(src: &[u8], dest: &mut [u8], typesize: usize, prec_bits: i8) {
    let len = src.len();
    if dest.len() < len {
        return;
    }
    // Rust's wider typesize support: C returns error for anything but 4 or 8.
    // We pass the data through unchanged to avoid producing cross-incompatible
    // output that C would not have been able to generate.
    if !matches!(typesize, 4 | 8) {
        dest[..len].copy_from_slice(src);
        return;
    }

    let (mantissa_bits, n_elements) = match typesize {
        4 => (BITS_MANTISSA_F32, len / 4),
        8 => (BITS_MANTISSA_F64, len / 8),
        _ => unreachable!(),
    };

    let p = prec_bits as i32;
    if p.abs() > mantissa_bits {
        // C logs an error and returns -1. We mirror by passing through.
        dest[..len].copy_from_slice(src);
        return;
    }
    let zeroed_bits = if p >= 0 { mantissa_bits - p } else { -p };
    if zeroed_bits >= mantissa_bits {
        // C returns -1 in this case too.
        dest[..len].copy_from_slice(src);
        return;
    }

    match typesize {
        4 => {
            let mask = !((1u32 << zeroed_bits) - 1);
            for i in 0..n_elements {
                let off = i * 4;
                let v = u32::from_le_bytes(src[off..off + 4].try_into().unwrap());
                dest[off..off + 4].copy_from_slice(&(v & mask).to_le_bytes());
            }
        }
        8 => {
            let mask = !((1u64 << zeroed_bits) - 1);
            for i in 0..n_elements {
                let off = i * 8;
                let v = u64::from_le_bytes(src[off..off + 8].try_into().unwrap());
                dest[off..off + 8].copy_from_slice(&(v & mask).to_le_bytes());
            }
        }
        _ => unreachable!(),
    }

    // Copy any leftover bytes that don't form a complete element.
    let tail_start = n_elements * typesize;
    if tail_start < len {
        dest[tail_start..len].copy_from_slice(&src[tail_start..]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scalar_shuffle_for_test(typesize: usize, src: &[u8], dest: &mut [u8]) {
        let blocksize = src.len();
        if typesize <= 1 || blocksize == 0 {
            dest[..blocksize].copy_from_slice(src);
            return;
        }
        let nelements = blocksize / typesize;
        let tail_start = nelements * typesize;
        for byte_idx in 0..typesize {
            for element in 0..nelements {
                dest[byte_idx * nelements + element] = src[element * typesize + byte_idx];
            }
        }
        dest[tail_start..blocksize].copy_from_slice(&src[tail_start..blocksize]);
    }

    fn scalar_unshuffle_for_test(typesize: usize, src: &[u8], dest: &mut [u8]) {
        let blocksize = src.len();
        if typesize <= 1 || blocksize == 0 {
            dest[..blocksize].copy_from_slice(src);
            return;
        }
        let nelements = blocksize / typesize;
        let tail_start = nelements * typesize;
        for element in 0..nelements {
            for byte_idx in 0..typesize {
                dest[element * typesize + byte_idx] = src[byte_idx * nelements + element];
            }
        }
        dest[tail_start..blocksize].copy_from_slice(&src[tail_start..blocksize]);
    }

    #[test]
    fn test_shuffle_unshuffle_roundtrip() {
        let data: Vec<u8> = (0..32).collect();
        let mut shuffled = vec![0u8; 32];
        let mut restored = vec![0u8; 32];

        shuffle(4, &data, &mut shuffled);
        assert_ne!(data, shuffled);
        unshuffle(4, &shuffled, &mut restored);
        assert_eq!(data, restored);
    }

    #[test]
    fn test_shuffle_dispatch_matches_scalar_for_simd_widths_and_leftovers() {
        for typesize in [4, 8] {
            for extra_bytes in [0, 1, 3, 5, 7] {
                let len = 256 + extra_bytes;
                let data: Vec<u8> = (0..len)
                    .map(|i: usize| (i.wrapping_mul(29).wrapping_add(typesize)) as u8)
                    .collect();
                let mut expected = vec![0u8; len];
                let mut actual = vec![0u8; len];
                let mut restored = vec![0u8; len];
                let mut scalar_restored = vec![0u8; len];

                scalar_shuffle_for_test(typesize, &data, &mut expected);
                shuffle(typesize, &data, &mut actual);
                assert_eq!(
                    actual, expected,
                    "shuffle dispatch diverged from scalar for typesize={typesize} extra_bytes={extra_bytes}"
                );

                scalar_unshuffle_for_test(typesize, &expected, &mut scalar_restored);
                unshuffle(typesize, &actual, &mut restored);
                assert_eq!(
                    restored, scalar_restored,
                    "unshuffle dispatch diverged from scalar for typesize={typesize} extra_bytes={extra_bytes}"
                );
                assert_eq!(restored, data);
            }
        }
    }

    #[test]
    fn test_shuffle_typesize_1() {
        let data: Vec<u8> = (0..16).collect();
        let mut shuffled = vec![0u8; 16];
        shuffle(1, &data, &mut shuffled);
        assert_eq!(data, shuffled); // typesize 1 = no-op
    }

    #[test]
    fn test_shuffle_rejects_short_destinations() {
        let data: Vec<u8> = (0..16).collect();
        let mut dest = vec![0xA5; 15];

        shuffle(4, &data, &mut dest);
        assert_eq!(dest, vec![0xA5; 15]);

        unshuffle(4, &data, &mut dest);
        assert_eq!(dest, vec![0xA5; 15]);
    }

    #[test]
    fn test_shuffle_unshuffle_typesize4_large_roundtrip() {
        let data: Vec<u8> = (0..1027usize)
            .map(|i| (i.wrapping_mul(31).wrapping_add(7)) as u8)
            .collect();
        let mut shuffled = vec![0u8; data.len()];
        let mut restored = vec![0u8; data.len()];

        shuffle(4, &data, &mut shuffled);
        unshuffle(4, &shuffled, &mut restored);
        assert_eq!(data, restored);
    }

    #[test]
    fn test_bitshuffle_roundtrip() {
        // Size must be a multiple of 8 elements
        let data: Vec<u8> = (0..64).collect(); // 16 elements of 4 bytes
        let mut shuffled = vec![0u8; 64];
        let mut restored = vec![0u8; 64];

        bitshuffle(4, &data, &mut shuffled);
        bitunshuffle(4, &shuffled, &mut restored);
        assert_eq!(data, restored);
    }

    #[test]
    fn test_bitshuffle_preserves_leftover_elements() {
        for typesize in [1, 2, 4, 8, 16] {
            for extra_elements in [1, 3, 5, 7] {
                let len = (16 + extra_elements) * typesize;
                let data: Vec<u8> = (0..len)
                    .map(|i: usize| (i.wrapping_mul(37).wrapping_add(typesize)) as u8)
                    .collect();
                let mut shuffled = vec![0u8; len];
                let mut restored = vec![0u8; len];

                assert_eq!(bitshuffle(typesize, &data, &mut shuffled), len as i64);
                assert_eq!(bitunshuffle(typesize, &shuffled, &mut restored), len as i64);
                assert_eq!(
                    data, restored,
                    "bitshuffle leftover roundtrip failed for typesize={typesize} extra_elements={extra_elements}"
                );
            }
        }
    }

    #[test]
    fn test_bitshuffle_dispatch_matches_scalar_for_typesizes_and_leftovers() {
        for typesize in [1usize, 2, 3, 4, 8, 16] {
            for extra_elements in 0..8 {
                let len = (40 + extra_elements) * typesize + (typesize / 2);
                let data: Vec<u8> = (0..len)
                    .map(|i: usize| (i.wrapping_mul(37) ^ (i >> 3).wrapping_mul(11)) as u8)
                    .collect();

                let mut dispatched = vec![0u8; len];
                let mut scalar = vec![0u8; len];
                let mut dispatch_scratch = vec![0u8; len];
                let mut scalar_scratch = vec![0u8; len];
                assert_eq!(
                    bitshuffle_with_scratch(
                        typesize,
                        &data,
                        &mut dispatched,
                        Some(&mut dispatch_scratch)
                    ),
                    len as i64
                );
                assert_eq!(
                    bitshuffle_scalar_with_scratch(
                        typesize,
                        &data,
                        &mut scalar,
                        Some(&mut scalar_scratch)
                    ),
                    len as i64
                );
                assert_eq!(
                    dispatched, scalar,
                    "bitshuffle dispatch mismatch for typesize={typesize} extra_elements={extra_elements}"
                );

                let mut dispatched_restored = vec![0u8; len];
                let mut scalar_restored = vec![0u8; len];
                assert_eq!(
                    bitunshuffle_with_scratch(
                        typesize,
                        &dispatched,
                        &mut dispatched_restored,
                        Some(&mut dispatch_scratch)
                    ),
                    len as i64
                );
                assert_eq!(
                    bitunshuffle_scalar_with_scratch(
                        typesize,
                        &scalar,
                        &mut scalar_restored,
                        Some(&mut scalar_scratch)
                    ),
                    len as i64
                );
                assert_eq!(
                    dispatched_restored, scalar_restored,
                    "bitunshuffle dispatch mismatch for typesize={typesize} extra_elements={extra_elements}"
                );
                assert_eq!(dispatched_restored, data);
            }
        }
    }

    #[test]
    fn test_bitshuffle_rejects_short_buffers() {
        let data: Vec<u8> = (0..64).collect();
        let mut short_dest = vec![0u8; 63];
        let mut scratch = vec![0u8; 63];
        let mut dest = vec![0u8; 64];

        assert_eq!(bitshuffle(4, &data, &mut short_dest), 0);
        assert_eq!(bitunshuffle(4, &data, &mut short_dest), 0);
        assert_eq!(
            bitshuffle_with_scratch(4, &data, &mut dest, Some(&mut scratch)),
            0
        );
        assert_eq!(
            bitunshuffle_with_scratch(4, &data, &mut dest, Some(&mut scratch)),
            0
        );
    }

    #[test]
    fn test_delta_roundtrip() {
        let dref: Vec<u8> = (0..16).collect();
        let src: Vec<u8> = (10..26).collect();
        let mut encoded = vec![0u8; 16];
        let mut decoded = vec![0u8; 16];

        // Non-reference block (offset != 0)
        delta_encode(&dref, 1, 16, 1, &src, &mut encoded);
        decoded.copy_from_slice(&encoded);
        delta_decode(Some(&dref), 1, 16, 1, &mut decoded);
        assert_eq!(src, decoded);
    }

    #[test]
    fn test_delta_reference_block() {
        // For offset=0, dref should equal the source data (no prior filters).
        // The encoder uses dref for XOR reference, decoder is self-referential.
        let src: Vec<u8> = (0..16).map(|i| i * 3 + 7).collect();
        let mut encoded = vec![0u8; 16];
        let mut decoded = vec![0u8; 16];

        // Reference block (offset == 0) — dref == src
        delta_encode(&src, 0, 16, 1, &src, &mut encoded);
        decoded.copy_from_slice(&encoded);
        delta_decode(None, 0, 16, 1, &mut decoded); // self-referential at offset=0
        assert_eq!(src, decoded);
    }

    // C's delta_{encoder,decoder} (c-blosc2/blosc/delta.c) falls back to typesize=1
    // for non-power-of-two typesizes that are not multiples of 8, and to typesize=8
    // for multiples of 8. Rust must match for cross-compatibility.
    #[test]
    fn test_delta_falls_back_to_byte_level_for_typesize_3() {
        // Reference block (offset=0). C's encoder at typesize=3 degrades to typesize=1
        // which means dest[0]=dref[0], and dest[i]=src[i]^dref[i-1] for i>=1.
        let src: Vec<u8> = vec![
            0xA0, 0xA1, 0xA2, 0xB0, 0xB1, 0xB2, 0xC0, 0xC1, 0xC2, 0xD0, 0xD1, 0xD2,
        ];
        let mut encoded = vec![0u8; 12];
        // dref=src for offset==0 per pipeline_forward convention.
        delta_encode(&src, 0, 12, 3, &src, &mut encoded);

        // Expected (what C does): typesize=1 fallback.
        let mut expected = vec![0u8; 12];
        expected[0] = src[0];
        for i in 1..12 {
            expected[i] = src[i] ^ src[i - 1];
        }

        assert_eq!(
            encoded, expected,
            "delta_encode with typesize=3 must degrade to byte-level (C-compatible)"
        );

        // Symmetric check for decode.
        let mut dest = encoded.clone();
        delta_decode(None, 0, 12, 3, &mut dest);
        assert_eq!(dest, src, "decode must roundtrip after C-compatible encode");
    }

    #[test]
    fn test_delta_falls_back_to_u64_for_typesize_16() {
        // typesize=16 is a multiple of 8 → C falls back to typesize=8.
        let src: Vec<u8> = (0..32).map(|i| i as u8 ^ 0xA5).collect();
        let mut encoded = vec![0u8; 32];
        delta_encode(&src, 0, 32, 16, &src, &mut encoded);

        // Expected: typesize=8 behavior. Copy first 8, then XOR 8-byte blocks.
        let mut expected = vec![0u8; 32];
        expected[..8].copy_from_slice(&src[..8]);
        for i in 8..32 {
            expected[i] = src[i] ^ src[i - 8];
        }

        assert_eq!(
            encoded, expected,
            "delta_encode with typesize=16 must degrade to 8-byte granularity (C-compatible)"
        );

        let mut dest = encoded.clone();
        delta_decode(None, 0, 32, 16, &mut dest);
        assert_eq!(dest, src);
    }

    #[test]
    fn test_delta_rejects_invalid_buffers() {
        let src: Vec<u8> = (0..16).collect();
        let dref: Vec<u8> = (16..32).collect();
        let mut dest = vec![0xA5; 16];

        delta_encode(&dref, 1, 16, 0, &src, &mut dest);
        assert_eq!(dest, vec![0xA5; 16]);

        delta_encode(&dref, 1, 16, 1, &src[..15], &mut dest);
        assert_eq!(dest, vec![0xA5; 16]);

        delta_encode(&dref, 1, 16, 1, &src, &mut dest[..15]);
        assert_eq!(dest[..15], vec![0xA5; 15]);
        assert_eq!(dest[15], 0xA5);

        delta_encode(&dref[..15], 1, 16, 1, &src, &mut dest);
        assert_eq!(dest, vec![0xA5; 16]);

        delta_encode(&src[..15], 0, 16, 1, &src, &mut dest);
        assert_eq!(dest, vec![0xA5; 16]);

        delta_decode(Some(&dref), 1, 16, 0, &mut dest);
        assert_eq!(dest, vec![0xA5; 16]);

        delta_decode(Some(&dref), 1, 16, 1, &mut dest[..15]);
        assert_eq!(dest[..15], vec![0xA5; 15]);
        assert_eq!(dest[15], 0xA5);

        delta_decode(Some(&dref[..15]), 1, 16, 1, &mut dest);
        assert_eq!(dest, vec![0xA5; 16]);
    }

    #[test]
    fn test_pipeline_rejects_invalid_buffers() {
        let src: Vec<u8> = (0..16).collect();
        let mut buf1 = vec![0xA5; 16];
        let mut buf2 = vec![0x5A; 16];
        let mut short_buf = vec![0u8; 15];
        let filters = [BLOSC_SHUFFLE, 0, 0, 0, 0, 0];
        let filters_meta = [0; BLOSC2_MAX_FILTERS];

        assert_eq!(
            pipeline_forward(
                &src,
                &mut short_buf,
                &mut buf2,
                &filters,
                &filters_meta,
                4,
                0,
                None,
            ),
            0
        );
        assert_eq!(buf2, vec![0x5A; 16]);

        assert_eq!(
            pipeline_forward(
                &src,
                &mut buf1,
                &mut short_buf,
                &filters,
                &filters_meta,
                4,
                0,
                None,
            ),
            0
        );
        assert_eq!(buf1, vec![0xA5; 16]);

        assert_eq!(
            pipeline_backward(
                &mut buf1,
                &mut buf2,
                16,
                &filters,
                &filters_meta,
                4,
                0,
                None,
                0,
            ),
            0
        );

        assert_eq!(
            pipeline_backward(
                &mut short_buf,
                &mut buf2,
                16,
                &filters,
                &filters_meta,
                4,
                0,
                None,
                1,
            ),
            0
        );
    }

    #[test]
    fn test_trunc_prec_preserves_tail_bytes() {
        let src = [0xFFu8, 0xFF, 0xFF, 0xFF, 0xAA, 0xBB];
        let mut dest = [0u8; 6];

        // prec_bits=16 > BITS_MANTISSA_F32(23), valid. Zero low 7 mantissa bits.
        trunc_prec_forward(&src, &mut dest, 4, 16);

        assert_eq!(&dest[4..], &src[4..]);
    }

    // C's truncate_precision32 (c-blosc2/blosc/trunc-prec.c) only zeros mantissa
    // bits (BITS_MANTISSA_FLOAT = 23) — the sign and 8-bit exponent are preserved
    // so the result is still a valid IEEE-754 approximation of the input.
    // Rust must match.
    #[test]
    fn test_trunc_prec_f32_preserves_sign_and_exponent() {
        // 1.333... = 0x3FAAAAAB in IEEE-754. prec_bits = 10 → clear low 13 mantissa bits.
        let original: f32 = 1.3333333;
        let src = original.to_le_bytes();
        let mut dest = [0u8; 4];

        trunc_prec_forward(&src, &mut dest, 4, 10);

        let out = f32::from_le_bytes(dest);
        let out_bits = out.to_bits();
        let orig_bits = original.to_bits();
        // Sign and exponent (top 9 bits) must be preserved.
        assert_eq!(
            out_bits & 0xFF800000,
            orig_bits & 0xFF800000,
            "trunc_prec must preserve sign+exponent: original={:#x} got={:#x}",
            orig_bits,
            out_bits
        );
        // The output must be a reasonable approximation of the input (within 1%).
        let rel_err = ((out - original) / original).abs();
        assert!(
            rel_err < 0.01,
            "trunc_prec should approximate the input (got {out} for input {original}, rel err {rel_err})"
        );
    }

    #[test]
    fn test_trunc_prec_f64_preserves_sign_and_exponent() {
        let original: f64 = 1.3333333333333;
        let src = original.to_le_bytes();
        let mut dest = [0u8; 8];

        trunc_prec_forward(&src, &mut dest, 8, 20);

        let out = f64::from_le_bytes(dest);
        let out_bits = out.to_bits();
        let orig_bits = original.to_bits();
        // Sign and 11-bit exponent (top 12 bits) must be preserved.
        assert_eq!(
            out_bits & 0xFFF0_0000_0000_0000u64,
            orig_bits & 0xFFF0_0000_0000_0000u64,
            "trunc_prec must preserve sign+exponent for f64"
        );
    }
}
