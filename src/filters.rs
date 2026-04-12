use crate::constants::*;
use crate::shuffle_sse2;

/// Apply byte-wise shuffle (transpose bytes within elements).
///
/// For each byte position j within a type of size `typesize`,
/// gather all j-th bytes of each element contiguously.
pub fn shuffle(typesize: usize, src: &[u8], dest: &mut [u8]) {
    let blocksize = src.len();
    if typesize <= 1 || blocksize == 0 {
        dest[..blocksize].copy_from_slice(&src[..blocksize]);
        return;
    }

    // Try SSE2 accelerated shuffle
    if shuffle_sse2::shuffle_sse2(typesize, src, dest) {
        return;
    }

    let neblock_quot = blocksize / typesize;
    let neblock_rem = blocksize % typesize;

    // Safety: all indices are bounded by neblock_quot * typesize <= blocksize
    // which is <= src.len() and dest.len()
    debug_assert!(neblock_quot * typesize <= blocksize);
    debug_assert!(blocksize <= src.len());
    debug_assert!(blocksize <= dest.len());

    unsafe {
        for j in 0..typesize {
            let dest_base = j * neblock_quot;
            for i in 0..neblock_quot {
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

/// Reverse byte-wise shuffle (untranspose bytes within elements).
pub fn unshuffle(typesize: usize, src: &[u8], dest: &mut [u8]) {
    let blocksize = src.len();
    if typesize <= 1 || blocksize == 0 {
        dest[..blocksize].copy_from_slice(&src[..blocksize]);
        return;
    }

    // Try SSE2 accelerated unshuffle
    if shuffle_sse2::unshuffle_sse2(typesize, src, dest) {
        return;
    }

    let neblock_quot = blocksize / typesize;
    let neblock_rem = blocksize % typesize;

    debug_assert!(neblock_quot * typesize <= blocksize);
    debug_assert!(blocksize <= src.len());
    debug_assert!(blocksize <= dest.len());

    unsafe {
        for i in 0..neblock_quot {
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

/// Transpose bytes within elements (step 1 of bitshuffle).
fn trans_byte_elem(src: &[u8], dest: &mut [u8], size: usize, elem_size: usize) {
    debug_assert!(size * elem_size <= src.len());
    debug_assert!(size * elem_size <= dest.len());

    unsafe {
        let mut ii = 0;
        while ii + 7 < size {
            for jj in 0..elem_size {
                let dest_base = jj * size + ii;
                let src_base = ii * elem_size + jj;
                for kk in 0..8 {
                    *dest.get_unchecked_mut(dest_base + kk) =
                        *src.get_unchecked(src_base + kk * elem_size);
                }
            }
            ii += 8;
        }
        while ii < size {
            for jj in 0..elem_size {
                *dest.get_unchecked_mut(jj * size + ii) =
                    *src.get_unchecked(ii * elem_size + jj);
            }
            ii += 1;
        }
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
            dest[dst_off..dst_off + nbyte_row]
                .copy_from_slice(&src[src_off..src_off + nbyte_row]);
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
pub fn bitshuffle_with_scratch(typesize: usize, src: &[u8], dest: &mut [u8], scratch: Option<&mut [u8]>) -> i64 {
    let blocksize = src.len();
    if typesize == 0 || blocksize == 0 {
        return 0;
    }

    let size = blocksize / typesize;
    let size8 = size - (size % 8);
    let nbyte8 = size8 * typesize;

    if size8 > 0 {
        // Use provided scratch or allocate
        let mut owned_tmp;
        let tmp = if let Some(s) = scratch {
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

/// Transpose bytes for each bit row (step 1 of untranspose).
fn trans_byte_bitrow(src: &[u8], dest: &mut [u8], size: usize, elem_size: usize) {
    let nbyte_row = size / 8;

    for jj in 0..elem_size {
        for ii in 0..nbyte_row {
            for kk in 0..8usize {
                dest[ii * 8 * elem_size + jj * 8 + kk] =
                    src[(jj * 8 + kk) * nbyte_row + ii];
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

/// Reverse bit-wise shuffle (bitunshuffle). Returns number of bytes processed.
pub fn bitunshuffle(typesize: usize, src: &[u8], dest: &mut [u8]) -> i64 {
    bitunshuffle_with_scratch(typesize, src, dest, None)
}

/// Bitunshuffle with optional scratch buffer to avoid allocation.
pub fn bitunshuffle_with_scratch(typesize: usize, src: &[u8], dest: &mut [u8], scratch: Option<&mut [u8]>) -> i64 {
    let blocksize = src.len();
    if typesize == 0 || blocksize == 0 {
        return 0;
    }

    let size = blocksize / typesize;
    let size8 = size - (size % 8);
    let nbyte8 = size8 * typesize;

    if size8 > 0 {
        let mut owned_tmp;
        let tmp = if let Some(s) = scratch {
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

/// Apply delta encoding.
///
/// If `offset == 0` (reference block), XOR each element with the previous element
/// in the reference. Otherwise, XOR with the corresponding element in the reference.
pub fn delta_encode(dref: &[u8], offset: usize, nbytes: usize, typesize: usize,
                    src: &[u8], dest: &mut [u8]) {
    // Use byte-level XOR for simplicity — the C code optimizes by typesize
    // but the result is identical since XOR is byte-wise
    if offset == 0 {
        // Reference block: delta against previous elements in dref
        for i in 0..typesize.min(nbytes) {
            dest[i] = dref[i];
        }
        for i in typesize..nbytes {
            dest[i] = src[i] ^ dref[i - typesize];
        }
    } else {
        // Non-reference block: delta against dref
        for i in 0..nbytes {
            dest[i] = src[i] ^ dref[i];
        }
    }
}

/// Reverse delta encoding (in-place).
/// For offset=0 (reference block), decode is self-referential: dest[i] ^= dest[i-typesize].
/// For offset>0, decode uses dref: dest[i] ^= dref[i].
pub fn delta_decode(dref: Option<&[u8]>, offset: usize, nbytes: usize, typesize: usize,
                    dest: &mut [u8]) {
    if offset == 0 {
        // Reference block: self-referential decode (dest[i] ^= dest[i-typesize])
        for i in typesize..nbytes {
            dest[i] ^= dest[i - typesize];
        }
    } else if let Some(dref) = dref {
        // Non-reference block: undo delta against dref
        for i in 0..nbytes {
            dest[i] ^= dref[i];
        }
    }
}

/// Apply the forward filter pipeline to a block.
///
/// Returns the filtered data (may swap between `buf1` and `buf2`).
/// The caller provides two working buffers; `src` is the input.
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

        let inp: &[u8] = match current {
            0 => &src[..bsize],
            1 => unsafe { &*(buf1 as *const [u8]) },
            _ => unsafe { &*(buf2 as *const [u8]) },
        };
        let out: &mut [u8] = if out_buf == 1 {
            unsafe { &mut *(buf1 as *mut [u8]) }
        } else {
            unsafe { &mut *(buf2 as *mut [u8]) }
        };

        match filter {
            BLOSC_SHUFFLE => {
                let ts = if filters_meta[i] == 0 { typesize } else { filters_meta[i] as usize };
                shuffle(ts, &inp[..bsize], &mut out[..bsize]);
            }
            BLOSC_BITSHUFFLE => {
                bitshuffle(typesize, &inp[..bsize], &mut out[..bsize]);
            }
            BLOSC_DELTA => {
                let actual_dref = dref.unwrap_or(src);
                delta_encode(actual_dref, block_offset, bsize, typesize, &inp[..bsize], &mut out[..bsize]);
            }
            BLOSC_TRUNC_PREC => {
                let prec = filters_meta[i] as usize;
                trunc_prec_forward(&inp[..bsize], &mut out[..bsize], typesize, prec);
            }
            _ => {
                out[..bsize].copy_from_slice(&inp[..bsize]);
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
    let mut current = current_buf as u8;

    // Filters applied in reverse order
    for i in (0..BLOSC2_MAX_FILTERS).rev() {
        let filter = filters[i];
        if filter == BLOSC_NOFILTER {
            continue;
        }

        let (inp, out) = if current == 1 {
            let (a, b) = (buf1 as *mut [u8], buf2 as *mut [u8]);
            unsafe { (&*a, &mut *b) }
        } else {
            let (a, b) = (buf2 as *mut [u8], buf1 as *mut [u8]);
            unsafe { (&*a, &mut *b) }
        };

        match filter {
            BLOSC_SHUFFLE => {
                let ts = if filters_meta[i] == 0 { typesize } else { filters_meta[i] as usize };
                unshuffle(ts, &inp[..bsize], &mut out[..bsize]);
            }
            BLOSC_BITSHUFFLE => {
                bitunshuffle(typesize, &inp[..bsize], &mut out[..bsize]);
            }
            BLOSC_DELTA => {
                // Delta decode: copy data to output, then decode in-place
                out[..bsize].copy_from_slice(&inp[..bsize]);
                delta_decode(dref, block_offset, bsize, typesize, &mut out[..bsize]);
            }
            BLOSC_TRUNC_PREC => {
                // Truncation is lossy — backward is a no-op (data already truncated)
                out[..bsize].copy_from_slice(&inp[..bsize]);
            }
            _ => {
                out[..bsize].copy_from_slice(&inp[..bsize]);
            }
        }

        current = if current == 1 { 2 } else { 1 };
    }

    current as usize
}

/// Truncate precision: zero out least-significant bits of floating-point values.
fn trunc_prec_forward(src: &[u8], dest: &mut [u8], typesize: usize, prec_bits: usize) {
    if prec_bits == 0 || typesize == 0 {
        dest[..src.len()].copy_from_slice(src);
        return;
    }

    let total_bits = typesize * 8;
    if prec_bits >= total_bits {
        dest[..src.len()].copy_from_slice(src);
        return;
    }

    let bits_to_clear = total_bits - prec_bits;
    let bytes_to_clear = bits_to_clear / 8;
    let remaining_bits = bits_to_clear % 8;

    let n_elements = src.len() / typesize;
    for i in 0..n_elements {
        let off = i * typesize;
        // Copy the element
        dest[off..off + typesize].copy_from_slice(&src[off..off + typesize]);
        // Zero out least-significant bytes
        for b in 0..bytes_to_clear {
            dest[off + b] = 0;
        }
        // Mask remaining bits in the partial byte
        if remaining_bits > 0 {
            let mask = !((1u8 << remaining_bits) - 1);
            dest[off + bytes_to_clear] &= mask;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn test_shuffle_typesize_1() {
        let data: Vec<u8> = (0..16).collect();
        let mut shuffled = vec![0u8; 16];
        shuffle(1, &data, &mut shuffled);
        assert_eq!(data, shuffled); // typesize 1 = no-op
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
}
