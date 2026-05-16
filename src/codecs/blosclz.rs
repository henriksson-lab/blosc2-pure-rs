//! BloscLZ — a fast LZ-family codec tuned for binary and numerical data.
//!
//! BloscLZ is derived from FastLZ and is Blosc's default in-house codec.
//! It targets very high (de)compression bandwidth on the small block sizes
//! that Blosc uses, accepting a modest compression ratio in exchange.
//!
//! The wire format encodes a stream of *literal runs* and *back-references*
//! (LZ77-style copies). A 5-bit control byte selects between the two: low
//! 5 bits are a literal run length (0–31), the high 3 bits are a match
//! length (3–9) or an escape into the long-distance / long-length encoding
//! (16-bit distance, length encoded as a chain of 255-extension bytes).
//!
//! The encoder uses a small (≤ 16 KiB) hash table to find matches, performs
//! a quick *entropy probe* (see [`get_cratio_with_htab`]) to bail out on
//! incompressible input, and uses SIMD-accelerated match scanners and
//! match-copy routines on x86 when available.

/// Maximum number of bytes that can be emitted as a single literal run
/// before the encoder must restart a new run (control byte rolls over).
const MAX_COPY: u32 = 32;
/// Maximum back-reference distance encodable in the short (2-byte) match
/// form. Distances up to `MAX_FARDISTANCE` are encoded via the long form.
const MAX_DISTANCE: u32 = 8191;
/// Maximum back-reference distance encodable at all (long form).
const MAX_FARDISTANCE: u32 = 65535 + MAX_DISTANCE - 1;
/// log2 of the hash table size used for match search at the maximum level.
const HASH_LOG: u8 = 14;

/// Per-offset shift amount used by the SSSE3 16-byte repeating-pattern
/// copy: after one 16-byte block is written, the source pointer advances
/// by this many bytes so that the next block continues the repeat.
#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
const COPY_MATCH_16_SHIFTS: [u8; 17] = [0, 1, 2, 1, 4, 1, 4, 2, 8, 7, 6, 5, 4, 3, 2, 1, 16];

/// Build the lookup table of `pshufb` shuffle masks for repeating a short
/// pattern (offset 1..=16) across a 16-byte SSSE3 register. Index 0 is
/// unused; index 16 is the identity mask.
#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
const fn copy_match_16_masks() -> [[u8; 16]; 17] {
    let mut masks = [[0u8; 16]; 17];
    let mut offset = 1usize;
    while offset <= 16 {
        let mut i = 0usize;
        while i < 16 {
            masks[offset][i] = if offset == 16 {
                i as u8
            } else {
                (i % offset) as u8
            };
            i += 1;
        }
        offset += 1;
    }
    masks
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
const COPY_MATCH_16_MASKS: [[u8; 16]; 17] = copy_match_16_masks();

/// LZ4-style Fibonacci hash: multiply by the golden-ratio constant and
/// keep the top `32 - hash_shift` bits.
#[inline(always)]
fn hash_function_shift_nonzero(seq: u32, hash_shift: u32) -> u32 {
    debug_assert!(hash_shift < 32);
    seq.wrapping_mul(2654435761) >> hash_shift
}

/// Unaligned little-endian load of a 32-bit word from `base + pos`.
#[inline(always)]
unsafe fn readu32_ptr(base: *const u8, pos: usize) -> u32 {
    std::ptr::read_unaligned(base.add(pos).cast::<u32>())
}

/// Read `htab[idx]` without bounds checking.
#[inline(always)]
unsafe fn htab_get(htab: &[u32], idx: usize) -> u32 {
    debug_assert!(idx < htab.len());
    *htab.get_unchecked(idx)
}

/// Write `htab[idx] = value` without bounds checking.
#[inline(always)]
unsafe fn htab_set(htab: &mut [u32], idx: usize, value: u32) {
    debug_assert!(idx < htab.len());
    *htab.get_unchecked_mut(idx) = value;
}

/// Return the number of equal leading bytes shared by two non-equal
/// machine words, taking endianness into account.
#[inline(always)]
fn matching_prefix_len(a: u64, b: u64) -> usize {
    let diff = a ^ b;
    debug_assert_ne!(diff, 0);
    if cfg!(target_endian = "little") {
        (diff.trailing_zeros() as usize) / 8
    } else {
        (diff.leading_zeros() as usize) / 8
    }
}

/// Extend a "run" — a sequence of bytes equal to `data[ip - 1]` — starting
/// at `ip`, by reading the source via `refp` and stopping at `ip_bound`.
/// Returns the position of the first byte that breaks the run.
#[inline]
fn get_run(data: &[u8], mut ip: usize, ip_bound: usize, mut refp: usize) -> usize {
    debug_assert!(ip > 0 && ip <= data.len());
    let x = data[ip - 1];
    let x8 = u64::from_ne_bytes([x; 8]);
    let base = data.as_ptr();

    while ip + 8 < ip_bound && refp + 8 <= data.len() {
        let ref_word = unsafe { std::ptr::read_unaligned(base.add(refp).cast::<u64>()) };
        if ref_word != x8 {
            let matched = matching_prefix_len(ref_word, x8);
            return (ip + matched).min(ip_bound);
        }
        ip += 8;
        refp += 8;
    }

    let end = ip + (ip_bound - ip).min(data.len() - refp);
    unsafe {
        while ip < end && *base.add(refp) == x {
            ip += 1;
            refp += 1;
        }
    }
    ip
}

/// Extend a match starting at `(ip, refp)` and return the first `ip`
/// position whose byte differs from the corresponding reference byte
/// (one past the mismatch, matching the upstream C convention).
#[inline(always)]
fn get_match(data: &[u8], ip: usize, ip_bound: usize, refp: usize) -> usize {
    get_match_generic(data, ip, ip_bound, refp)
}

/// SSE2-accelerated match scanner that compares 16 bytes at a time
/// (x86_64 variant).
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse2")]
unsafe fn get_match_16_x86_64(
    data: &[u8],
    mut ip: usize,
    ip_bound: usize,
    mut refp: usize,
) -> usize {
    use std::arch::x86_64::{__m128i, _mm_cmpeq_epi8, _mm_loadu_si128, _mm_movemask_epi8};
    let base = data.as_ptr();

    while ip + 16 < ip_bound && refp + 16 <= data.len() {
        let lhs = _mm_loadu_si128(base.add(ip) as *const __m128i);
        let rhs = _mm_loadu_si128(base.add(refp) as *const __m128i);
        let cmp = _mm_cmpeq_epi8(lhs, rhs);
        let mask = _mm_movemask_epi8(cmp) as u32;
        if mask != 0xFFFF {
            return ip + ((!mask).trailing_zeros() as usize) + 1;
        }
        ip += 16;
        refp += 16;
    }
    let end = ip + (ip_bound - ip).min(data.len() - refp);
    while ip < end {
        if *base.add(refp) != *base.add(ip) {
            return ip + 1;
        }
        ip += 1;
        refp += 1;
    }
    ip
}

/// SSE2-accelerated match scanner that compares 16 bytes at a time
/// (32-bit x86 variant).
#[cfg(target_arch = "x86")]
#[target_feature(enable = "sse2")]
unsafe fn get_match_16_x86(data: &[u8], mut ip: usize, ip_bound: usize, mut refp: usize) -> usize {
    use std::arch::x86::{__m128i, _mm_cmpeq_epi8, _mm_loadu_si128, _mm_movemask_epi8};
    let base = data.as_ptr();

    while ip + 16 < ip_bound && refp + 16 <= data.len() {
        let lhs = _mm_loadu_si128(base.add(ip) as *const __m128i);
        let rhs = _mm_loadu_si128(base.add(refp) as *const __m128i);
        let cmp = _mm_cmpeq_epi8(lhs, rhs);
        let mask = _mm_movemask_epi8(cmp) as u32;
        if mask != 0xFFFF {
            return ip + ((!mask).trailing_zeros() as usize) + 1;
        }
        ip += 16;
        refp += 16;
    }
    let end = ip + (ip_bound - ip).min(data.len() - refp);
    while ip < end {
        if *base.add(refp) != *base.add(ip) {
            return ip + 1;
        }
        ip += 1;
        refp += 1;
    }
    ip
}

/// Portable match scanner: compare 8 bytes at a time using unaligned
/// 64-bit loads, then a byte-by-byte tail. Returns one past the first
/// differing byte.
#[inline]
fn get_match_generic(data: &[u8], mut ip: usize, ip_bound: usize, mut refp: usize) -> usize {
    let base = data.as_ptr();
    while ip + 8 < ip_bound && refp + 8 <= data.len() {
        let ip_word = unsafe { std::ptr::read_unaligned(base.add(ip).cast::<u64>()) };
        let ref_word = unsafe { std::ptr::read_unaligned(base.add(refp).cast::<u64>()) };
        if ip_word != ref_word {
            let matched = matching_prefix_len(ip_word, ref_word);
            return (ip + matched + 1).min(ip_bound);
        }
        ip += 8;
        refp += 8;
    }
    let end = ip + (ip_bound - ip).min(data.len() - refp);
    unsafe {
        while ip < end {
            if *base.add(refp) != *base.add(ip) {
                return ip + 1;
            }
            ip += 1;
            refp += 1;
        }
    }
    ip
}

/// SSSE3 fast-path for short overlapping copies (distance ∈ [1, 16]).
/// Uses `pshufb` with a precomputed mask to replicate the pattern across
/// 16-byte writes (x86_64 variant).
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "ssse3")]
unsafe fn copy_match_repeat_16_x86_64(
    base: *mut u8,
    mut op: usize,
    mut ref_pos: usize,
    mut len: usize,
) -> usize {
    use std::arch::x86_64::{
        __m128i, _mm_load_si128, _mm_loadu_si128, _mm_shuffle_epi8, _mm_storeu_si128,
    };

    let distance = op - ref_pos;
    debug_assert!((1..=16).contains(&distance));
    let shift = usize::from(COPY_MATCH_16_SHIFTS[distance]);
    let mask = COPY_MATCH_16_MASKS[distance].as_ptr() as *const __m128i;

    while len >= 16 {
        let src = _mm_loadu_si128(base.add(ref_pos) as *const __m128i);
        let block = _mm_shuffle_epi8(src, _mm_load_si128(mask));
        _mm_storeu_si128(base.add(op) as *mut __m128i, block);
        ref_pos += shift;
        op += 16;
        len -= 16;
    }

    for i in 0..len {
        *base.add(op + i) = *base.add(ref_pos + i);
    }
    op + len
}

/// SSSE3 fast-path for short overlapping copies (32-bit x86 variant).
#[cfg(target_arch = "x86")]
#[target_feature(enable = "ssse3")]
unsafe fn copy_match_repeat_16_x86(
    base: *mut u8,
    mut op: usize,
    mut ref_pos: usize,
    mut len: usize,
) -> usize {
    use std::arch::x86::{
        __m128i, _mm_load_si128, _mm_loadu_si128, _mm_shuffle_epi8, _mm_storeu_si128,
    };

    let distance = op - ref_pos;
    debug_assert!((1..=16).contains(&distance));
    let shift = usize::from(COPY_MATCH_16_SHIFTS[distance]);
    let mask = COPY_MATCH_16_MASKS[distance].as_ptr() as *const __m128i;

    while len >= 16 {
        let src = _mm_loadu_si128(base.add(ref_pos) as *const __m128i);
        let block = _mm_shuffle_epi8(src, _mm_load_si128(mask));
        _mm_storeu_si128(base.add(op) as *mut __m128i, block);
        ref_pos += shift;
        op += 16;
        len -= 16;
    }

    for i in 0..len {
        *base.add(op + i) = *base.add(ref_pos + i);
    }
    op + len
}

/// Overlapping match copy specialised by distance, using the largest
/// power-of-two-ish word writes that fit within one period. Falls back
/// to a byte loop for the tail.
#[inline(always)]
unsafe fn copy_match_overlap_exact(
    base: *mut u8,
    mut op: usize,
    mut ref_pos: usize,
    mut len: usize,
) -> usize {
    let distance = op - ref_pos;

    /// Copy 2 bytes from `from` to `op`.
    #[inline(always)]
    unsafe fn copy2(base: *mut u8, op: usize, from: usize) -> usize {
        let v = std::ptr::read_unaligned(base.add(from) as *const u16);
        std::ptr::write_unaligned(base.add(op) as *mut u16, v);
        op + 2
    }

    /// Copy 4 bytes from `from` to `op`.
    #[inline(always)]
    unsafe fn copy4(base: *mut u8, op: usize, from: usize) -> usize {
        let v = std::ptr::read_unaligned(base.add(from) as *const u32);
        std::ptr::write_unaligned(base.add(op) as *mut u32, v);
        op + 4
    }

    /// Copy 8 bytes from `from` to `op`.
    #[inline(always)]
    unsafe fn copy8(base: *mut u8, op: usize, from: usize) -> usize {
        let v = std::ptr::read_unaligned(base.add(from) as *const u64);
        std::ptr::write_unaligned(base.add(op) as *mut u64, v);
        op + 8
    }

    /// Copy 16 bytes from `from` to `op`.
    #[inline(always)]
    unsafe fn copy16(base: *mut u8, op: usize, from: usize) -> usize {
        let v = std::ptr::read_unaligned(base.add(from) as *const u128);
        std::ptr::write_unaligned(base.add(op) as *mut u128, v);
        op + 16
    }

    match distance {
        32 => {
            while len >= 32 {
                op = copy16(base, op, ref_pos);
                op = copy16(base, op, ref_pos + 16);
                len -= 32;
            }
        }
        30 => {
            while len >= 30 {
                op = copy16(base, op, ref_pos);
                op = copy8(base, op, ref_pos + 16);
                op = copy4(base, op, ref_pos + 24);
                op = copy2(base, op, ref_pos + 28);
                len -= 30;
            }
        }
        28 => {
            while len >= 28 {
                op = copy16(base, op, ref_pos);
                op = copy8(base, op, ref_pos + 16);
                op = copy4(base, op, ref_pos + 24);
                len -= 28;
            }
        }
        26 => {
            while len >= 26 {
                op = copy16(base, op, ref_pos);
                op = copy8(base, op, ref_pos + 16);
                op = copy2(base, op, ref_pos + 24);
                len -= 26;
            }
        }
        24 => {
            while len >= 24 {
                op = copy16(base, op, ref_pos);
                op = copy8(base, op, ref_pos + 16);
                len -= 24;
            }
        }
        22 => {
            while len >= 22 {
                op = copy16(base, op, ref_pos);
                op = copy4(base, op, ref_pos + 16);
                op = copy2(base, op, ref_pos + 20);
                len -= 22;
            }
        }
        20 => {
            while len >= 20 {
                op = copy16(base, op, ref_pos);
                op = copy4(base, op, ref_pos + 16);
                len -= 20;
            }
        }
        18 => {
            while len >= 18 {
                op = copy16(base, op, ref_pos);
                op = copy2(base, op, ref_pos + 16);
                len -= 18;
            }
        }
        16 => {
            while len >= 16 {
                op = copy16(base, op, ref_pos);
                len -= 16;
            }
        }
        d if d > 16 => {
            while len >= 16 {
                op = copy16(base, op, ref_pos);
                ref_pos += 16;
                len -= 16;
            }
        }
        8 => {
            while len >= 8 {
                op = copy8(base, op, ref_pos);
                len -= 8;
            }
        }
        4 => {
            while len >= 4 {
                op = copy4(base, op, ref_pos);
                len -= 4;
            }
        }
        2 => {
            while len >= 2 {
                op = copy2(base, op, ref_pos);
                len -= 2;
            }
        }
        _ => {}
    }

    for i in 0..len {
        *base.add(op + i) = *base.add(ref_pos + i);
    }
    op + len
}

/// Dispatch for overlapping match copies with small distances.
///
/// Picks an optimised path for the common shuffle-friendly distances of 2
/// and 4, an SSSE3 `pshufb`-based path for other short distances when
/// available, and otherwise falls back to [`copy_match_overlap_exact`].
#[inline(always)]
unsafe fn copy_match_small_overlap(
    base: *mut u8,
    op: usize,
    ref_pos: usize,
    match_len: usize,
    use_ssse3_repeat: bool,
) -> usize {
    let distance = op - ref_pos;

    if distance == 4 {
        let seed = std::ptr::read_unaligned(base.add(ref_pos) as *const u32) as u64;
        let pat = seed | (seed << 32);
        let mut d = base.add(op);
        let end = base.add(op + match_len);
        while d < end {
            std::ptr::write_unaligned(d as *mut u64, pat);
            d = d.add(8);
        }
        op + match_len
    } else if distance == 2 {
        let seed = std::ptr::read_unaligned(base.add(ref_pos) as *const u16) as u64;
        let pat = seed.wrapping_mul(0x0001_0001_0001_0001);
        let mut d = base.add(op);
        let end = base.add(op + match_len);
        while d < end {
            std::ptr::write_unaligned(d as *mut u64, pat);
            d = d.add(8);
        }
        op + match_len
    } else if use_ssse3_repeat && match_len >= 16 && distance <= 16 {
        #[cfg(target_arch = "x86_64")]
        {
            copy_match_repeat_16_x86_64(base, op, ref_pos, match_len)
        }
        #[cfg(target_arch = "x86")]
        {
            copy_match_repeat_16_x86(base, op, ref_pos, match_len)
        }
        #[cfg(not(any(target_arch = "x86", target_arch = "x86_64")))]
        {
            copy_match_overlap_exact(base, op, ref_pos, match_len)
        }
    } else {
        copy_match_overlap_exact(base, op, ref_pos, match_len)
    }
}

/// LZ4-style 8-byte "wild copy": stream 8 bytes at a time from `ref_pos`
/// to `op` until `op` reaches `end`. The caller must ensure `end` leaves
/// enough trailing slack for the final overrun.
#[inline(always)]
unsafe fn wild_copy_8(base: *mut u8, mut op: usize, mut ref_pos: usize, end: usize) {
    while op < end {
        let v = std::ptr::read_unaligned(base.add(ref_pos) as *const u64);
        std::ptr::write_unaligned(base.add(op) as *mut u64, v);
        op += 8;
        ref_pos += 8;
    }
}

/// Choose between a run extender and a match extender based on `run`,
/// and select the best available implementation. A zero biased distance
/// (`run == true`) means the match overlaps with a single byte and is
/// most efficiently handled as a run.
#[inline(always)]
fn get_run_or_match(
    data: &[u8],
    ip: usize,
    ip_bound: usize,
    refp: usize,
    run: bool,
    use_sse2_match: bool,
) -> usize {
    if run {
        get_run(data, ip, ip_bound, refp)
    } else {
        #[cfg(target_arch = "x86_64")]
        if use_sse2_match {
            return unsafe { get_match_16_x86_64(data, ip, ip_bound, refp) };
        }
        #[cfg(target_arch = "x86")]
        if use_sse2_match {
            return unsafe { get_match_16_x86(data, ip, ip_bound, refp) };
        }
        get_match(data, ip, ip_bound, refp)
    }
}

/// Estimate the compression ratio of `data` cheaply, used for entropy
/// probing before launching a full encode. Mirrors a streamlined version
/// of [`compress`]: it walks the hash table and accumulates the number of
/// output bytes that the real encoder would emit (`oc`), then returns
/// `bytes_read / bytes_written`. Low ratios cause the caller to bail out
/// and store the block uncompressed.
fn get_cratio_with_htab(
    data: &[u8],
    maxlen: usize,
    minlen: usize,
    ipshift: usize,
    hash_shift: u32,
    htab: &mut [u32],
    use_sse2_match: bool,
) -> f64 {
    htab.fill(0);
    let data_ptr = data.as_ptr();
    let limit = maxlen.min(htab.len()).min(data.len());
    if limit < 13 {
        return 0.0;
    }
    let ip_bound = limit - 1;
    let ip_limit = limit - 12;
    let mut oc: i64 = 0;
    let mut copy: u8 = 4;
    oc += 5;

    let mut ip = 0usize;
    if hash_shift >= 32 {
        while ip < ip_limit {
            let anchor = ip;

            let seq = unsafe { readu32_ptr(data_ptr, ip) };
            let ref_offset = unsafe { htab_get(htab, 0) as usize };

            let distance = anchor - ref_offset;
            unsafe { htab_set(htab, 0, anchor as u32) };

            if distance == 0 || distance >= MAX_FARDISTANCE as usize {
                oc += 1;
                ip = anchor + 1;
                copy += 1;
                if copy == MAX_COPY as u8 {
                    copy = 0;
                    oc += 1;
                }
                continue;
            }

            if unsafe { readu32_ptr(data_ptr, ref_offset) } != seq {
                oc += 1;
                ip = anchor + 1;
                copy += 1;
                if copy == MAX_COPY as u8 {
                    copy = 0;
                    oc += 1;
                }
                continue;
            }

            ip = anchor + 4;
            let ref_after = ref_offset + 4;
            let distance_dec = distance - 1;
            ip = get_run_or_match(
                data,
                ip,
                ip_bound,
                ref_after,
                distance_dec == 0,
                use_sse2_match,
            );

            debug_assert!(ip >= ipshift);
            ip -= ipshift;
            let len = ip - anchor;
            if len < minlen {
                oc += 1;
                ip = anchor + 1;
                copy += 1;
                if copy == MAX_COPY as u8 {
                    copy = 0;
                    oc += 1;
                }
                continue;
            }

            if copy == 0 {
                oc -= 1;
            }
            copy = 0;

            if distance < MAX_DISTANCE as usize {
                if len >= 7 {
                    oc += ((len - 7) / 255 + 1) as i64;
                }
                oc += 2;
            } else {
                if len >= 7 {
                    oc += ((len - 7) / 255 + 1) as i64;
                }
                oc += 4;
            }

            unsafe { htab_set(htab, 0, ip as u32) };
            ip += 2;
            oc += 1;
        }
    } else {
        while ip < ip_limit {
            let anchor = ip;

            let seq = unsafe { readu32_ptr(data_ptr, ip) };
            let hval = hash_function_shift_nonzero(seq, hash_shift) as usize;
            let ref_offset = unsafe { htab_get(htab, hval) as usize };

            let distance = anchor - ref_offset;
            unsafe { htab_set(htab, hval, anchor as u32) };

            if distance == 0 || distance >= MAX_FARDISTANCE as usize {
                oc += 1;
                ip = anchor + 1;
                copy += 1;
                if copy == MAX_COPY as u8 {
                    copy = 0;
                    oc += 1;
                }
                continue;
            }

            if unsafe { readu32_ptr(data_ptr, ref_offset) } != seq {
                oc += 1;
                ip = anchor + 1;
                copy += 1;
                if copy == MAX_COPY as u8 {
                    copy = 0;
                    oc += 1;
                }
                continue;
            }

            ip = anchor + 4;
            let ref_after = ref_offset + 4;
            let distance_dec = distance - 1;
            ip = get_run_or_match(
                data,
                ip,
                ip_bound,
                ref_after,
                distance_dec == 0,
                use_sse2_match,
            );

            debug_assert!(ip >= ipshift);
            ip -= ipshift;
            let len = ip - anchor;
            if len < minlen {
                oc += 1;
                ip = anchor + 1;
                copy += 1;
                if copy == MAX_COPY as u8 {
                    copy = 0;
                    oc += 1;
                }
                continue;
            }

            if copy == 0 {
                oc -= 1;
            }
            copy = 0;

            if distance < MAX_DISTANCE as usize {
                if len >= 7 {
                    oc += ((len - 7) / 255 + 1) as i64;
                }
                oc += 2;
            } else {
                if len >= 7 {
                    oc += ((len - 7) / 255 + 1) as i64;
                }
                oc += 4;
            }

            let seq2 = unsafe { readu32_ptr(data_ptr, ip) };
            let hval2 = hash_function_shift_nonzero(seq2, hash_shift) as usize;
            unsafe { htab_set(htab, hval2, ip as u32) };
            ip += 2;
            oc += 1;
        }
    }

    let ic = ip as f64;
    if oc <= 0 {
        return f64::INFINITY;
    }
    ic / oc as f64
}

/// Compress `input` into `output` using the BloscLZ codec.
///
/// `clevel` is the Blosc compression level in `0..=9`; higher values
/// enlarge the search-hash table and disable the entropy-probe early-exit
/// more aggressively. The input must be at least 16 bytes and the output
/// buffer at least 66 bytes; the buffers must not overlap.
///
/// Returns the number of bytes written to `output`, or `0` if the input
/// is incompressible or the output buffer is too small.
pub fn compress(clevel: i32, input: &[u8], output: &mut [u8]) -> i32 {
    let length = input.len();
    let maxout = output.len();

    if length < 16 || maxout < 66 {
        return 0;
    }

    let ipshift: usize = 3;
    let minlen: usize = 3;

    let hashlog_table: [u8; 10] = [
        0,
        HASH_LOG - 2,
        HASH_LOG - 1,
        HASH_LOG,
        HASH_LOG,
        HASH_LOG,
        HASH_LOG,
        HASH_LOG,
        HASH_LOG,
        HASH_LOG,
    ];
    let clevel = clevel.clamp(0, 9) as usize;
    let hashlog = hashlog_table[clevel];

    let mut maxlen = length;
    if clevel < 2 {
        maxlen /= 8;
    } else if clevel < 4 {
        maxlen /= 4;
    } else if clevel < 7 {
        maxlen /= 2;
    }

    let hashlen = 1usize << hashlog;
    let hash_shift = 32 - hashlog as u32;
    let mut htab_storage = std::mem::MaybeUninit::<[u32; 1 << HASH_LOG]>::uninit();
    // SAFETY: We only ever read entries from `htab[..hashlen]` after explicitly
    // zeroing that prefix, and the compressor never touches the unused suffix.
    let htab = unsafe { &mut *htab_storage.as_mut_ptr() };
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    let use_sse2_match = std::arch::is_x86_feature_detected!("sse2");
    #[cfg(not(any(target_arch = "x86", target_arch = "x86_64")))]
    let use_sse2_match = false;

    let shift = length - maxlen;
    let cratio = get_cratio_with_htab(
        &input[shift..],
        maxlen,
        minlen,
        ipshift,
        hash_shift,
        &mut htab[..hashlen],
        use_sse2_match,
    );

    let cratio_table: [f64; 10] = [0.0, 2.0, 1.5, 1.2, 1.2, 1.2, 1.2, 1.15, 1.1, 1.0];
    if cratio < cratio_table[clevel] {
        return 0;
    }

    htab[..hashlen].fill(0);

    let ip_bound = length - 1;
    let ip_limit = length - 12;
    let mut op: usize = 0;
    let op_limit = maxout;
    let mut copy: u8 = 4;
    if op + 5 > op_limit {
        return 0;
    }

    let mut ip = 4usize;
    output[op] = MAX_COPY as u8 - 1;
    op += 1;
    output[op..op + 4].copy_from_slice(&input[..4]);
    op += 4;

    let input_ptr = input.as_ptr();
    let output_ptr = output.as_mut_ptr();

    macro_rules! write_u8 {
        ($v:expr) => {{
            unsafe {
                *output_ptr.add(op) = $v;
            }
            op += 1;
        }};
    }

    macro_rules! emit_literal {
        ($anchor:expr) => {{
            if op + 2 > op_limit {
                return 0;
            }
            unsafe {
                *output_ptr.add(op) = *input_ptr.add($anchor);
            }
            op += 1;
            ip = $anchor + 1;
            copy += 1;
            if copy == MAX_COPY as u8 {
                copy = 0;
                write_u8!(MAX_COPY as u8 - 1);
            }
        }};
    }

    if hash_shift >= 32 {
        while ip < ip_limit {
            let anchor = ip;

            let seq = unsafe { readu32_ptr(input_ptr, ip) };
            let ref_offset = unsafe { htab_get(htab, 0) as usize };
            let distance = ip - ref_offset;

            unsafe { htab_set(htab, 0, ip as u32) };

            if distance == 0 || distance >= MAX_FARDISTANCE as usize {
                emit_literal!(anchor);
                continue;
            }

            if unsafe { readu32_ptr(input_ptr, ref_offset) } != seq {
                emit_literal!(anchor);
                continue;
            }

            ip = anchor + 4;
            let ref_after = ref_offset + 4;
            let distance = distance - 1;
            ip = get_run_or_match(
                input,
                ip,
                ip_bound,
                ref_after,
                distance == 0,
                use_sse2_match,
            );

            debug_assert!(ip >= ipshift);
            ip -= ipshift;
            let len = ip - anchor;

            if len < minlen || (len <= 5 && distance >= MAX_DISTANCE as usize) {
                emit_literal!(anchor);
                continue;
            }

            if copy > 0 {
                unsafe {
                    *output_ptr.add(op - copy as usize - 1) = copy - 1;
                }
            } else {
                op -= 1;
            }
            copy = 0;

            if distance < MAX_DISTANCE as usize {
                if len < 7 {
                    if op + 2 > op_limit {
                        return 0;
                    }
                    write_u8!(((len << 5) + (distance >> 8)) as u8);
                    write_u8!((distance & 255) as u8);
                } else {
                    if op + 1 > op_limit {
                        return 0;
                    }
                    write_u8!(((7 << 5) + (distance >> 8)) as u8);
                    let mut remaining = len - 7;
                    while remaining >= 255 {
                        if op + 1 > op_limit {
                            return 0;
                        }
                        write_u8!(255);
                        remaining -= 255;
                    }
                    if op + 2 > op_limit {
                        return 0;
                    }
                    write_u8!(remaining as u8);
                    write_u8!((distance & 255) as u8);
                }
            } else {
                let distance = distance - MAX_DISTANCE as usize;
                if len < 7 {
                    if op + 4 > op_limit {
                        return 0;
                    }
                    write_u8!(((len << 5) + 31) as u8);
                    write_u8!(255);
                    write_u8!((distance >> 8) as u8);
                    write_u8!((distance & 255) as u8);
                } else {
                    if op + 1 > op_limit {
                        return 0;
                    }
                    write_u8!((7 << 5) + 31);
                    let mut remaining = len - 7;
                    while remaining >= 255 {
                        if op + 1 > op_limit {
                            return 0;
                        }
                        write_u8!(255);
                        remaining -= 255;
                    }
                    if op + 4 > op_limit {
                        return 0;
                    }
                    write_u8!(remaining as u8);
                    write_u8!(255);
                    write_u8!((distance >> 8) as u8);
                    write_u8!((distance & 255) as u8);
                }
            }

            unsafe { htab_set(htab, 0, ip as u32) };
            ip += 2;

            if op + 1 > op_limit {
                return 0;
            }
            write_u8!(MAX_COPY as u8 - 1);
        }
    } else if clevel == 9 {
        while ip < ip_limit {
            let anchor = ip;

            let seq = unsafe { readu32_ptr(input_ptr, ip) };
            let hval = hash_function_shift_nonzero(seq, hash_shift) as usize;
            let ref_offset = unsafe { htab_get(htab, hval) as usize };
            let distance = ip - ref_offset;

            unsafe { htab_set(htab, hval, ip as u32) };

            if distance == 0 || distance >= MAX_FARDISTANCE as usize {
                emit_literal!(anchor);
                continue;
            }

            if unsafe { readu32_ptr(input_ptr, ref_offset) } != seq {
                emit_literal!(anchor);
                continue;
            }

            ip = anchor + 4;
            let ref_after = ref_offset + 4;
            let distance = distance - 1;
            ip = get_run_or_match(
                input,
                ip,
                ip_bound,
                ref_after,
                distance == 0,
                use_sse2_match,
            );

            debug_assert!(ip >= ipshift);
            ip -= ipshift;
            let len = ip - anchor;

            if len < minlen || (len <= 5 && distance >= MAX_DISTANCE as usize) {
                emit_literal!(anchor);
                continue;
            }

            if copy > 0 {
                unsafe {
                    *output_ptr.add(op - copy as usize - 1) = copy - 1;
                }
            } else {
                op -= 1;
            }
            copy = 0;

            if distance < MAX_DISTANCE as usize {
                if len < 7 {
                    if op + 2 > op_limit {
                        return 0;
                    }
                    write_u8!(((len << 5) + (distance >> 8)) as u8);
                    write_u8!((distance & 255) as u8);
                } else {
                    if op + 1 > op_limit {
                        return 0;
                    }
                    write_u8!(((7 << 5) + (distance >> 8)) as u8);
                    let mut remaining = len - 7;
                    while remaining >= 255 {
                        if op + 1 > op_limit {
                            return 0;
                        }
                        write_u8!(255);
                        remaining -= 255;
                    }
                    if op + 2 > op_limit {
                        return 0;
                    }
                    write_u8!(remaining as u8);
                    write_u8!((distance & 255) as u8);
                }
            } else {
                let distance = distance - MAX_DISTANCE as usize;
                if len < 7 {
                    if op + 4 > op_limit {
                        return 0;
                    }
                    write_u8!(((len << 5) + 31) as u8);
                    write_u8!(255);
                    write_u8!((distance >> 8) as u8);
                    write_u8!((distance & 255) as u8);
                } else {
                    if op + 1 > op_limit {
                        return 0;
                    }
                    write_u8!((7 << 5) + 31);
                    let mut remaining = len - 7;
                    while remaining >= 255 {
                        if op + 1 > op_limit {
                            return 0;
                        }
                        write_u8!(255);
                        remaining -= 255;
                    }
                    if op + 4 > op_limit {
                        return 0;
                    }
                    write_u8!(remaining as u8);
                    write_u8!(255);
                    write_u8!((distance >> 8) as u8);
                    write_u8!((distance & 255) as u8);
                }
            }

            let mut seq2 = unsafe { readu32_ptr(input_ptr, ip) };
            let hval2 = hash_function_shift_nonzero(seq2, hash_shift) as usize;
            unsafe { htab_set(htab, hval2, ip as u32) };
            ip += 1;
            seq2 >>= 8;
            let hval3 = hash_function_shift_nonzero(seq2, hash_shift) as usize;
            unsafe { htab_set(htab, hval3, ip as u32) };
            ip += 1;

            if op + 1 > op_limit {
                return 0;
            }
            write_u8!(MAX_COPY as u8 - 1);
        }
    } else {
        while ip < ip_limit {
            let anchor = ip;

            let seq = unsafe { readu32_ptr(input_ptr, ip) };
            let hval = hash_function_shift_nonzero(seq, hash_shift) as usize;
            let ref_offset = unsafe { htab_get(htab, hval) as usize };
            let distance = ip - ref_offset;

            unsafe { htab_set(htab, hval, ip as u32) };

            if distance == 0 || distance >= MAX_FARDISTANCE as usize {
                emit_literal!(anchor);
                continue;
            }

            if unsafe { readu32_ptr(input_ptr, ref_offset) } != seq {
                emit_literal!(anchor);
                continue;
            }

            ip = anchor + 4;
            let ref_after = ref_offset + 4;
            let distance = distance - 1;
            ip = get_run_or_match(
                input,
                ip,
                ip_bound,
                ref_after,
                distance == 0,
                use_sse2_match,
            );

            debug_assert!(ip >= ipshift);
            ip -= ipshift;
            let len = ip - anchor;

            if len < minlen || (len <= 5 && distance >= MAX_DISTANCE as usize) {
                emit_literal!(anchor);
                continue;
            }

            if copy > 0 {
                unsafe {
                    *output_ptr.add(op - copy as usize - 1) = copy - 1;
                }
            } else {
                op -= 1;
            }
            copy = 0;

            if distance < MAX_DISTANCE as usize {
                if len < 7 {
                    if op + 2 > op_limit {
                        return 0;
                    }
                    write_u8!(((len << 5) + (distance >> 8)) as u8);
                    write_u8!((distance & 255) as u8);
                } else {
                    if op + 1 > op_limit {
                        return 0;
                    }
                    write_u8!(((7 << 5) + (distance >> 8)) as u8);
                    let mut remaining = len - 7;
                    while remaining >= 255 {
                        if op + 1 > op_limit {
                            return 0;
                        }
                        write_u8!(255);
                        remaining -= 255;
                    }
                    if op + 2 > op_limit {
                        return 0;
                    }
                    write_u8!(remaining as u8);
                    write_u8!((distance & 255) as u8);
                }
            } else {
                let distance = distance - MAX_DISTANCE as usize;
                if len < 7 {
                    if op + 4 > op_limit {
                        return 0;
                    }
                    write_u8!(((len << 5) + 31) as u8);
                    write_u8!(255);
                    write_u8!((distance >> 8) as u8);
                    write_u8!((distance & 255) as u8);
                } else {
                    if op + 1 > op_limit {
                        return 0;
                    }
                    write_u8!((7 << 5) + 31);
                    let mut remaining = len - 7;
                    while remaining >= 255 {
                        if op + 1 > op_limit {
                            return 0;
                        }
                        write_u8!(255);
                        remaining -= 255;
                    }
                    if op + 4 > op_limit {
                        return 0;
                    }
                    write_u8!(remaining as u8);
                    write_u8!(255);
                    write_u8!((distance >> 8) as u8);
                    write_u8!((distance & 255) as u8);
                }
            }

            let seq2 = unsafe { readu32_ptr(input_ptr, ip) };
            let hval2 = hash_function_shift_nonzero(seq2, hash_shift) as usize;
            unsafe { htab_set(htab, hval2, ip as u32) };
            ip += 2;

            if op + 1 > op_limit {
                return 0;
            }
            write_u8!(MAX_COPY as u8 - 1);
        }
    }

    while ip <= ip_bound {
        emit_literal!(ip);
    }

    if copy > 0 {
        output[op - copy as usize - 1] = copy - 1;
    } else {
        op -= 1;
    }

    output[0] |= 1 << 5;

    op as i32
}

/// Decompress a BloscLZ-encoded block.
///
/// Reads tokens from `input` and writes plain bytes into `output`. The
/// decoder is bounds-safe: it never writes past `output.len()` and bails
/// out on truncated or malformed input. The buffers must not overlap.
///
/// Returns the number of bytes written to `output`, or `0` if the input
/// is corrupted or the output buffer is too small.
pub fn decompress(input: &[u8], output: &mut [u8]) -> i32 {
    let length = input.len();
    let maxout = output.len();

    if length == 0 {
        return 0;
    }

    let mut ip: usize = 0;
    let ip_limit = length;
    let mut op: usize = 0;
    let op_limit = maxout;
    let input_ptr = input.as_ptr();
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    let use_ssse3_repeat = std::arch::is_x86_feature_detected!("ssse3");
    #[cfg(not(any(target_arch = "x86", target_arch = "x86_64")))]
    let use_ssse3_repeat = false;

    // SAFETY: all pointer reads below are guarded by explicit `ip + N >= ip_limit`
    // bounds checks before the reads, matching the original safe-slice indexing.
    // `input_ptr` points to `input.as_ptr()`, so `input_ptr.add(k)` is in-bounds
    // whenever `k < ip_limit`.
    let read = |ip: usize| -> u8 { unsafe { *input_ptr.add(ip) } };

    // `length > 0` guarantees index 0 is in bounds.
    let mut ctrl = (read(0) & 31) as u32;
    ip += 1;

    loop {
        if ctrl >= 32 {
            // Match
            let mut len = (ctrl >> 5) as i32 - 1;
            let mut ofs = ((ctrl & 31) << 8) as i32;

            if len == 6 {
                loop {
                    if ip + 1 >= ip_limit {
                        return 0;
                    }
                    let code = read(ip);
                    ip += 1;
                    len += code as i32;
                    if code != 255 {
                        break;
                    }
                }
            } else if ip + 1 >= ip_limit {
                return 0;
            }

            let code = read(ip);
            ip += 1;
            len += 3;
            let mut ref_offset = op as i32 - ofs - code as i32;

            // 16-bit distance
            if code == 255 && ofs == (31 << 8) {
                if ip + 1 >= ip_limit {
                    return 0;
                }
                ofs = (read(ip) as i32) << 8;
                ip += 1;
                ofs += read(ip) as i32;
                ip += 1;
                ref_offset = op as i32 - ofs - MAX_DISTANCE as i32;
            }

            if op + len as usize > op_limit {
                return 0;
            }
            ref_offset -= 1;
            if ref_offset < 0 {
                return 0;
            }

            if ip >= ip_limit {
                break;
            }
            ctrl = read(ip) as u32;
            ip += 1;

            let ref_pos = ref_offset as usize;
            let match_len = len as usize;

            if ref_pos == op - 1 {
                // Run: fill with repeated byte
                let val = output[ref_pos];
                output[op..op + match_len].fill(val);
                op += match_len;
            } else if op - ref_pos >= 8 && op + match_len + 8 <= op_limit {
                // C uses a single 8-byte wild-copy loop for medium/large match
                // distances. Keeping one inline path here is closer to that
                // structure and avoids Rust-only branch stratification.
                unsafe {
                    wild_copy_8(output.as_mut_ptr(), op, ref_pos, op + match_len);
                }
                op += match_len;
            } else {
                // Small-overlap case (distance ∈ [2, 7]). Distances 2 and 4 are
                // common in shuffled data and benefit from an explicit u64
                // broadcast. Other distances fall back to byte-by-byte (LLVM may
                // lower to memmove, which is still correct under LZ77 overlap).
                let distance = op - ref_pos;
                let has_8_slack = op + match_len + 8 <= op_limit;
                if has_8_slack && matches!(distance, 2 | 4) {
                    unsafe {
                        op = copy_match_small_overlap(
                            output.as_mut_ptr(),
                            op,
                            ref_pos,
                            match_len,
                            false,
                        );
                    }
                } else {
                    // General case (distance ∈ {3,5,6,7} or not enough slack).
                    // SAFETY: `ref_pos < op`, `op + match_len <= op_limit`.
                    unsafe {
                        op = copy_match_small_overlap(
                            output.as_mut_ptr(),
                            op,
                            ref_pos,
                            match_len,
                            use_ssse3_repeat && op + match_len + 16 <= op_limit,
                        );
                    }
                }
            }
        } else {
            // Literal
            ctrl += 1;
            let run_len = ctrl as usize;
            if op + run_len > op_limit {
                return 0;
            }
            if ip + run_len > ip_limit {
                return 0;
            }

            // Match C's literal path structure: copy exactly `run_len` bytes.
            // The source and destination are disjoint (`input` vs `output`).
            unsafe {
                std::ptr::copy_nonoverlapping(
                    input_ptr.add(ip),
                    output.as_mut_ptr().add(op),
                    run_len,
                );
            }
            op += ctrl as usize;
            ip += ctrl as usize;

            if ip >= ip_limit {
                break;
            }
            ctrl = read(ip) as u32;
            ip += 1;
        }
    }

    op as i32
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_roundtrip(data: &[u8], clevel: i32) {
        let mut compressed = vec![0u8; data.len() + 1000];
        let csize = compress(clevel, data, &mut compressed);
        assert!(csize > 0, "Compression failed");

        let mut decompressed = vec![0u8; data.len()];
        let dsize = decompress(&compressed[..csize as usize], &mut decompressed);
        assert_eq!(dsize as usize, data.len());
        assert_eq!(data, decompressed);
    }

    fn deterministic_data(len: usize) -> Vec<u8> {
        (0..len as u32)
            .map(|i| ((i.wrapping_mul(37).wrapping_add(11)) & 0xff) as u8)
            .collect()
    }

    fn distance_fixture(distance: usize, match_len: usize) -> Vec<u8> {
        assert!(match_len >= 16);
        let mut data = deterministic_data(distance + match_len + 128);
        let pattern: Vec<u8> = (0..match_len).map(|i| b'A' + (i % 26) as u8).collect();
        data[0..match_len].copy_from_slice(&pattern);
        data[distance..distance + match_len].copy_from_slice(&pattern);
        data
    }

    #[test]
    fn test_compress_decompress_roundtrip() {
        // Use highly compressible data (repeated pattern)
        let data: Vec<u8> = b"BloscLZ compression test with repeating data patterns! "
            .iter()
            .cycle()
            .take(40000)
            .copied()
            .collect();
        assert_roundtrip(&data, 5);
    }

    #[test]
    fn test_exact_max_short_distance_roundtrip() {
        let data = distance_fixture(MAX_DISTANCE as usize, 16);
        assert_roundtrip(&data, 9);
    }

    #[test]
    fn test_first_far_distance_roundtrip() {
        let data = distance_fixture(MAX_DISTANCE as usize + 1, 32);
        assert_roundtrip(&data, 9);
    }

    #[test]
    fn test_near_max_far_distance_roundtrip() {
        let data = distance_fixture(MAX_FARDISTANCE as usize - 1, 32);
        assert_roundtrip(&data, 9);
    }

    #[test]
    fn test_long_match_extension_roundtrip() {
        let data = distance_fixture(MAX_DISTANCE as usize + 1, 2048);
        assert_roundtrip(&data, 9);
    }

    #[test]
    fn test_overlapping_run_roundtrip() {
        let mut data = vec![0u8; 20_000];
        for (i, byte) in data.iter_mut().enumerate().take(128) {
            *byte = (i & 0xff) as u8;
        }
        data[128..].fill(b'Z');
        assert_roundtrip(&data, 9);
    }

    #[test]
    fn test_literal_run_encoding_roundtrip() {
        let literal_prefix = (MAX_COPY as usize * 4) + 17;
        let mut data = deterministic_data(literal_prefix);
        data.extend(
            b"literal-run-boundary-tail"
                .iter()
                .cycle()
                .take(4096)
                .copied(),
        );
        assert_roundtrip(&data, 9);
    }

    #[test]
    fn test_incompressible() {
        // Random-looking data
        let data: Vec<u8> = (0..1000u32)
            .map(|i| ((i.wrapping_mul(7919).wrapping_add(104729)) & 0xFF) as u8)
            .collect();
        let mut compressed = vec![0u8; data.len() + 100];
        let _csize = compress(1, &data, &mut compressed);
        // May or may not compress; that's fine
    }

    // Targeted tests that pin down `get_match_generic`'s return convention
    // against C's `get_match` (c-blosc2/blosc/blosclz.c:148). These exist to
    // close FINDINGS.md SUSPECT #10 — confirming whether Rust and C agree on
    // where `ip` lands after a byte-level mismatch inside the 8-byte word loop.
    //
    // Important: the encoder does `ip -= ipshift(=4)` after get_match and the
    // decoder does `len += 3`, so the total number of bytes actually copied on
    // decode is `(returned_ip - call_site_ip) + 3`. For correctness with a
    // real match of length L starting at `anchor`, the encoder calls
    // get_match with `ip = anchor + 4`, so we need
    // `(returned_ip - (anchor + 4)) + 3 == L`,
    // i.e. `returned_ip == anchor + 1 + L`. Since anchor + 4 was the call
    // site, that means get_match must advance ip by `L - 3` from where it was
    // called — which is `L - 3` extra bytes, i.e. advance past the mismatch by
    // one (to eat the first differing byte too). That's the C "one past
    // mismatch" convention.
    //
    // Rust must therefore return the C-compatible one-past-mismatch position.

    #[test]
    fn get_match_generic_stops_at_first_differing_byte_in_word() {
        // Lay out two regions 32 bytes apart. Bytes 0..=3 match, byte 4 differs.
        let mut data = vec![0u8; 128];
        let ref_pos = 0usize;
        let ip_pos = 32usize;
        for i in 0..4 {
            data[ref_pos + i] = 0xA0 + i as u8;
            data[ip_pos + i] = 0xA0 + i as u8;
        }
        // Byte 4 differs.
        data[ref_pos + 4] = 0xEE;
        data[ip_pos + 4] = 0xFF;
        // Bytes 5..=7 arbitrary.
        for i in 5..8 {
            data[ref_pos + i] = 0x11;
            data[ip_pos + i] = 0x22;
        }

        // ip_bound must allow the 8-byte word read at ip_pos (ip + 8 <= ip_bound).
        let returned = get_match_generic(&data, ip_pos, ip_pos + 16, ref_pos);

        assert_eq!(
            returned - ip_pos,
            5,
            "Rust get_match_generic follows C's one-past-mismatch convention"
        );
    }

    #[test]
    fn get_match_generic_matches_full_word_when_equal() {
        // Two identical 16-byte regions: should advance by 16 and then stop at
        // the remainder-loop boundary.
        let mut data = vec![0u8; 64];
        for i in 0..16 {
            data[i] = 0x20 + i as u8;
            data[32 + i] = 0x20 + i as u8;
        }
        // Byte 16 onwards differs so the remainder loop immediately stops.
        data[16] = 0x55;
        data[48] = 0xAA;

        let returned = get_match_generic(&data, 32, 48, 0);
        assert_eq!(
            returned - 32,
            16,
            "Full-word matches advance ip by 8 per word; here 16 across two words"
        );
    }

    #[test]
    fn get_match_generic_handles_mismatch_at_byte_zero_of_word() {
        // Bytes 0..=7 match. Bytes 8 differs (start of second word).
        let mut data = vec![0u8; 64];
        for i in 0..8 {
            data[i] = 0xC0 + i as u8;
            data[32 + i] = 0xC0 + i as u8;
        }
        data[8] = 0x00;
        data[32 + 8] = 0xFF;

        let returned = get_match_generic(&data, 32, 48, 0);
        assert_eq!(
            returned - 32,
            9,
            "First word all match, second word byte 0 differs, so advance one past the mismatch"
        );
    }

    // End-to-end closure of SUSPECT #10: get_match_generic must match C's
    // post-increment convention while still producing round-trippable output.
    #[test]
    fn rust_blosclz_output_roundtrips_with_nontrivial_match_lengths() {
        // Build data with a known long match at a near distance so the
        // encoder must exercise the word-boundary mismatch path.
        let mut data = vec![0u8; 0];
        // First 200 bytes: pseudo-random.
        data.extend((0..200u32).map(|i| ((i.wrapping_mul(37)) & 0xFF) as u8));
        // Second region: copy of a chunk of the first, terminated by a differing byte.
        let copy_start = 20usize;
        let copy_len = 50usize;
        data.extend_from_slice(&data.clone()[copy_start..copy_start + copy_len]);
        data.push(0xAA); // forces the match to terminate mid-word
                         // Padding so the data is large enough for blosclz to consider.
        data.extend(vec![0x77u8; 200]);

        let mut compressed = vec![0u8; data.len() + 256];
        let csize = compress(9, &data, &mut compressed);
        assert!(csize > 0, "compression must succeed");

        let mut decompressed = vec![0u8; data.len()];
        let dsize = decompress(&compressed[..csize as usize], &mut decompressed);
        assert_eq!(dsize as usize, data.len());
        assert_eq!(data, decompressed);
    }
}
