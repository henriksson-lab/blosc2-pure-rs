/// BloscLZ compression codec.
///
/// Based on FastLZ, a lightning-fast lossless compression library.
/// Ported from c-blosc2/blosc/blosclz.c.

const MAX_COPY: u32 = 32;
const MAX_DISTANCE: u32 = 8191;
const MAX_FARDISTANCE: u32 = 65535 + MAX_DISTANCE - 1;
const HASH_LOG: u8 = 14;

#[inline]
fn hash_function(seq: u32, hashlog: u8) -> u32 {
    if hashlog == 0 {
        return 0;
    }
    (seq.wrapping_mul(2654435761)) >> (32 - hashlog as u32)
}

#[inline(always)]
fn readu32(p: &[u8], pos: usize) -> u32 {
    debug_assert!(pos + 4 <= p.len());
    unsafe {
        let ptr = p.as_ptr().add(pos) as *const u32;
        ptr.read_unaligned()
    }
}

#[inline(always)]
fn readu64(p: &[u8], pos: usize) -> u64 {
    debug_assert!(pos + 8 <= p.len());
    unsafe {
        let ptr = p.as_ptr().add(pos) as *const u64;
        ptr.read_unaligned()
    }
}

/// Find a run of identical bytes starting from `ip`, comparing against `refp`.
#[inline]
fn get_run(data: &[u8], mut ip: usize, ip_bound: usize, mut refp: usize) -> usize {
    debug_assert!(ip > 0 && ip <= data.len());
    let x = data[ip - 1];
    let x8 = u64::from_ne_bytes([x; 8]);

    while ip + 8 <= ip_bound && refp + 8 <= data.len() {
        if readu64(data, refp) != x8 {
            unsafe {
                while refp < data.len() && *data.get_unchecked(refp) == x {
                    ip += 1;
                    refp += 1;
                }
            }
            return ip;
        }
        ip += 8;
        refp += 8;
    }

    unsafe {
        while ip < ip_bound && refp < data.len() && *data.get_unchecked(refp) == x {
            ip += 1;
            refp += 1;
        }
    }
    ip
}

/// Find the length of a match between `ip` and `refp`.
/// Uses SSE2 16-byte comparison on x86_64, matching C's get_match_16.
#[cfg(target_arch = "x86_64")]
#[inline(always)]
fn get_match(data: &[u8], ip: usize, ip_bound: usize, refp: usize) -> usize {
    // SSE2 is guaranteed on x86_64
    unsafe { get_match_sse2(data, ip, ip_bound, refp) }
}

#[cfg(not(target_arch = "x86_64"))]
#[inline(always)]
fn get_match(data: &[u8], ip: usize, ip_bound: usize, refp: usize) -> usize {
    get_match_generic(data, ip, ip_bound, refp)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse2")]
#[inline]
unsafe fn get_match_sse2(data: &[u8], mut ip: usize, ip_bound: usize, mut refp: usize) -> usize {
    use std::arch::x86_64::*;
    let ptr = data.as_ptr();

    while ip + 16 <= ip_bound && refp + 16 <= data.len() {
        let v1 = _mm_loadu_si128(ptr.add(ip) as *const __m128i);
        let v2 = _mm_loadu_si128(ptr.add(refp) as *const __m128i);
        let cmp = _mm_cmpeq_epi32(v1, v2);
        if _mm_movemask_epi8(cmp) != 0xFFFF {
            // Find exact mismatch byte
            while refp < data.len() && *ptr.add(refp) == *ptr.add(ip) {
                ip += 1;
                refp += 1;
            }
            return ip;
        }
        ip += 16;
        refp += 16;
    }

    // Remainder: byte-by-byte
    while ip < ip_bound && refp < data.len() && *ptr.add(refp) == *ptr.add(ip) {
        ip += 1;
        refp += 1;
    }
    ip
}

#[inline]
#[allow(dead_code)]
fn get_match_generic(data: &[u8], mut ip: usize, ip_bound: usize, mut refp: usize) -> usize {
    while ip + 8 <= ip_bound && refp + 8 <= data.len() {
        if readu64(data, ip) != readu64(data, refp) {
            unsafe {
                while refp < data.len() && *data.get_unchecked(refp) == *data.get_unchecked(ip) {
                    ip += 1;
                    refp += 1;
                }
            }
            return ip;
        }
        ip += 8;
        refp += 8;
    }
    unsafe {
        while ip < ip_bound && refp < data.len()
            && *data.get_unchecked(refp) == *data.get_unchecked(ip) {
            ip += 1;
            refp += 1;
        }
    }
    ip
}

#[inline(always)]
fn get_run_or_match(data: &[u8], ip: usize, ip_bound: usize, refp: usize, run: bool) -> usize {
    if run {
        get_run(data, ip, ip_bound, refp)
    } else {
        get_match(data, ip, ip_bound, refp)
    }
}

/// Estimate compression ratio for entropy probing.
/// `data` is a slice starting from the probe offset (like C's `ibase + shift`).
fn get_cratio_with_htab(data: &[u8], maxlen: usize, minlen: usize,
                        ipshift: usize, hashlog: u8, htab: &mut [u32]) -> f64 {
    htab.fill(0);
    let limit = maxlen.min(htab.len()).min(data.len());
    if limit < 13 {
        return 0.0;
    }
    let ip_bound = limit - 1;
    let ip_limit = limit - 12;
    let htab_mask = htab.len() - 1;

    let mut oc: i64 = 0;
    let mut copy: u8 = 4;
    oc += 5;

    let mut ip = 0usize;
    // Safety: ip < ip_limit = limit - 12, and limit <= data.len(),
    // so ip + 4 <= data.len() is guaranteed in the loop body.
    unsafe {
    while ip < ip_limit {
        let anchor = ip;

        let seq = readu32(data, ip);
        let hval = hash_function(seq, hashlog) as usize & htab_mask;
        let ref_offset = *htab.get_unchecked(hval) as usize;

        let distance = anchor.saturating_sub(ref_offset);
        *htab.get_unchecked_mut(hval) = anchor as u32;

        if distance == 0 || distance >= MAX_FARDISTANCE as usize {
            oc += 1; ip = anchor + 1; copy += 1;
            if copy == MAX_COPY as u8 { copy = 0; oc += 1; }
            continue;
        }

        if ref_offset + 4 > limit {
            oc += 1; ip = anchor + 1; copy += 1;
            if copy == MAX_COPY as u8 { copy = 0; oc += 1; }
            continue;
        }

        if readu32(data, ref_offset) != readu32(data, ip) {
            oc += 1; ip = anchor + 1; copy += 1;
            if copy == MAX_COPY as u8 { copy = 0; oc += 1; }
            continue;
        }

        ip = anchor + 4;
        let ref_after = ref_offset + 4;
        let distance_dec = distance - 1;
        ip = get_run_or_match(data, ip, ip_bound, ref_after, distance_dec == 0);

        if ip > ipshift { ip -= ipshift; } else { ip = anchor + 1; }
        let len = ip - anchor;
        if len < minlen {
            oc += 1; ip = anchor + 1; copy += 1;
            if copy == MAX_COPY as u8 { copy = 0; oc += 1; }
            continue;
        }

        if copy == 0 { oc -= 1; }
        copy = 0;

        if distance < MAX_DISTANCE as usize {
            if len >= 7 { oc += ((len - 7) / 255 + 1) as i64; }
            oc += 2;
        } else {
            if len >= 7 { oc += ((len - 7) / 255 + 1) as i64; }
            oc += 4;
        }

        if ip + 4 <= data.len() {
            let seq2 = readu32(data, ip);
            let hval2 = hash_function(seq2, hashlog) as usize & htab_mask;
            *htab.get_unchecked_mut(hval2) = ip as u32;
        }
        ip += 2;
        oc += 1;
    }
    } // end unsafe

    let ic = ip as f64;
    if oc <= 0 { return f64::INFINITY; }
    ic / oc as f64
}

/// Compress data using BloscLZ.
/// Returns the number of compressed bytes, or 0 if compression fails/is not beneficial.
pub fn compress(clevel: i32, input: &[u8], output: &mut [u8]) -> i32 {
    let length = input.len();
    let maxout = output.len();

    if length < 16 || maxout < 66 {
        return 0;
    }

    let ipshift: usize = 4;
    let minlen: usize = 4;

    let hashlog_table: [u8; 10] = [0, HASH_LOG - 2, HASH_LOG - 1, HASH_LOG, HASH_LOG,
                                    HASH_LOG, HASH_LOG, HASH_LOG, HASH_LOG, HASH_LOG];
    let clevel = clevel.clamp(0, 9) as usize;
    let hashlog = hashlog_table[clevel];

    let mut maxlen = length;
    if clevel < 2 { maxlen /= 8; }
    else if clevel < 4 { maxlen /= 4; }
    else if clevel < 7 { maxlen /= 2; }

    let hashlen = 1usize << hashlog;
    let mut htab = [0u32; 1 << HASH_LOG];

    let shift = length - maxlen;
    // get_cratio_with_htab fills htab[..hashlen] with 0 at start, so no double-init
    let cratio = get_cratio_with_htab(&input[shift..], maxlen, minlen, ipshift, hashlog, &mut htab[..hashlen]);

    let cratio_table: [f64; 10] = [0.0, 2.0, 1.5, 1.2, 1.2, 1.2, 1.2, 1.15, 1.1, 1.0];
    if cratio < cratio_table[clevel] {
        return 0;
    }

    // Only clear the portion we use (not the full 64KB array)
    htab[..hashlen].fill(0);

    let ip_bound = length - 1;
    let ip_limit = length - 12;
    let mut op: usize = 0;
    let op_limit = maxout;
    let mut copy: u8 = 4;
    let htab_mask = htab.len() - 1;

    if op + 5 > op_limit { return 0; }

    // Use raw pointers for the main loop — matches C's pointer arithmetic exactly.
    // Safety: all pointer accesses are bounded by ip_limit/op_limit checks before each write.
    unsafe {
    let ibase = input.as_ptr();
    let obase = output.as_mut_ptr();
    let htab_ptr = htab.as_mut_ptr();
    let mut ipp = ibase.add(4);
    let mut opp = obase;
    let ip_limit_ptr = ibase.add(ip_limit);
    let ip_bound_ptr = ibase.add(ip_bound);
    let op_limit_ptr = obase.add(op_limit);

    *opp = MAX_COPY as u8 - 1; opp = opp.add(1);
    std::ptr::copy_nonoverlapping(ibase, opp, 4);
    opp = opp.add(4);

    while ipp < ip_limit_ptr {
        let anchor = ipp;

        let seq = (ipp as *const u32).read_unaligned();
        let hval = hash_function(seq, hashlog) as usize & htab_mask;
        let ref_offset = *htab_ptr.add(hval) as usize;
        let distance = ipp.offset_from(ibase) as usize - ref_offset;

        *htab_ptr.add(hval) = ipp.offset_from(ibase) as u32;

        if distance == 0 || distance >= MAX_FARDISTANCE as usize {
            if opp.add(2) > op_limit_ptr { return 0; }
            *opp = *anchor; opp = opp.add(1);
            ipp = anchor.add(1);
            copy += 1;
            if copy == MAX_COPY as u8 {
                copy = 0;
                *opp = MAX_COPY as u8 - 1; opp = opp.add(1);
            }
            continue;
        }

        let refp = ibase.add(ref_offset);
        if refp.add(4) > ibase.add(length) || (refp as *const u32).read_unaligned() != (ipp as *const u32).read_unaligned() {
            if opp.add(2) > op_limit_ptr { return 0; }
            *opp = *anchor; opp = opp.add(1);
            ipp = anchor.add(1);
            copy += 1;
            if copy == MAX_COPY as u8 {
                copy = 0;
                *opp = MAX_COPY as u8 - 1; opp = opp.add(1);
            }
            continue;
        }

        ipp = anchor.add(4);
        let ip_idx = ipp.offset_from(ibase) as usize;
        let ref_after = ref_offset + 4;
        let distance_dec = distance - 1;

        let new_ip = get_run_or_match(input, ip_idx, ip_bound, ref_after, distance_dec == 0);
        ipp = ibase.add(new_ip);

        if ipp.offset_from(ibase) as usize > ipshift {
            ipp = ipp.sub(ipshift);
        } else {
            ipp = anchor.add(1);
        }
        let len = ipp.offset_from(anchor) as usize;

        if len < minlen || (len <= 5 && distance >= MAX_DISTANCE as usize) {
            if opp.add(2) > op_limit_ptr { return 0; }
            *opp = *anchor; opp = opp.add(1);
            ipp = anchor.add(1);
            copy += 1;
            if copy == MAX_COPY as u8 {
                copy = 0;
                *opp = MAX_COPY as u8 - 1; opp = opp.add(1);
            }
            continue;
        }

        if copy > 0 {
            *opp.sub(copy as usize + 1) = copy - 1;
        } else {
            opp = opp.sub(1);
        }
        copy = 0;

        let mut distance = distance;

        if distance < MAX_DISTANCE as usize {
            distance -= 1;
            if len < 7 {
                if opp.add(2) > op_limit_ptr { return 0; }
                *opp = ((len << 5) + (distance >> 8)) as u8; opp = opp.add(1);
                *opp = (distance & 255) as u8; opp = opp.add(1);
            } else {
                if opp.add(1) > op_limit_ptr { return 0; }
                *opp = ((7 << 5) + (distance >> 8)) as u8; opp = opp.add(1);
                let mut remaining = len - 7;
                while remaining >= 255 {
                    if opp.add(1) > op_limit_ptr { return 0; }
                    *opp = 255; opp = opp.add(1);
                    remaining -= 255;
                }
                if opp.add(2) > op_limit_ptr { return 0; }
                *opp = remaining as u8; opp = opp.add(1);
                *opp = (distance & 255) as u8; opp = opp.add(1);
            }
        } else {
            distance -= 1;
            distance -= MAX_DISTANCE as usize;
            if len < 7 {
                if opp.add(4) > op_limit_ptr { return 0; }
                *opp = ((len << 5) + 31) as u8; opp = opp.add(1);
                *opp = 255; opp = opp.add(1);
                *opp = (distance >> 8) as u8; opp = opp.add(1);
                *opp = (distance & 255) as u8; opp = opp.add(1);
            } else {
                if opp.add(1) > op_limit_ptr { return 0; }
                *opp = (7 << 5) + 31; opp = opp.add(1);
                let mut remaining = len - 7;
                while remaining >= 255 {
                    if opp.add(1) > op_limit_ptr { return 0; }
                    *opp = 255; opp = opp.add(1);
                    remaining -= 255;
                }
                if opp.add(4) > op_limit_ptr { return 0; }
                *opp = remaining as u8; opp = opp.add(1);
                *opp = 255; opp = opp.add(1);
                *opp = (distance >> 8) as u8; opp = opp.add(1);
                *opp = (distance & 255) as u8; opp = opp.add(1);
            }
        }

        let cur_ip = ipp.offset_from(ibase) as usize;
        if cur_ip + 4 <= length {
            let seq2 = (ipp as *const u32).read_unaligned();
            let hval2 = hash_function(seq2, hashlog) as usize & htab_mask;
            *htab_ptr.add(hval2) = cur_ip as u32;
            ipp = ipp.add(1);
            if clevel == 9 && cur_ip + 4 < length {
                let seq3 = (ipp as *const u32).read_unaligned();
                let hval3 = hash_function(seq3, hashlog) as usize & htab_mask;
                *htab_ptr.add(hval3) = ipp.offset_from(ibase) as u32;
                ipp = ipp.add(1);
            } else {
                ipp = ipp.add(1);
            }
        } else {
            ipp = ipp.add(2);
        }

        if opp.add(1) > op_limit_ptr { return 0; }
        *opp = MAX_COPY as u8 - 1;
        opp = opp.add(1);
    }

    // Left-over as literal copy
    while ipp <= ip_bound_ptr {
        if opp.add(2) > op_limit_ptr { return 0; }
        *opp = *ipp; opp = opp.add(1); ipp = ipp.add(1);
        copy += 1;
        if copy == MAX_COPY as u8 {
            copy = 0;
            *opp = MAX_COPY as u8 - 1; opp = opp.add(1);
        }
    }

    // Convert back to index-based for the remaining code
    op = opp.offset_from(obase) as usize;

    } // end unsafe

    // Adjust final copy length
    if copy > 0 {
        output[op - copy as usize - 1] = copy - 1;
    } else {
        op -= 1;
    }

    // Marker for blosclz
    output[0] |= 1 << 5;

    op as i32
}

/// Decompress BloscLZ data.
/// Returns the number of decompressed bytes, or 0 on error.
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

    let mut ctrl = (input[ip] & 31) as u32;
    ip += 1;

    loop {
        if ctrl >= 32 {
            // Match
            let mut len = (ctrl >> 5) as i32 - 1;
            let mut ofs = ((ctrl & 31) << 8) as i32;

            if len == 6 {
                loop {
                    if ip + 1 >= ip_limit { return 0; }
                    let code = input[ip]; ip += 1;
                    len += code as i32;
                    if code != 255 { break; }
                }
            } else {
                if ip + 1 >= ip_limit { return 0; }
            }

            let code = input[ip]; ip += 1;
            len += 3;
            let mut ref_offset = op as i32 - ofs - code as i32;

            // 16-bit distance
            if code == 255 && ofs == (31 << 8) {
                if ip + 1 >= ip_limit { return 0; }
                ofs = (input[ip] as i32) << 8;
                ip += 1;
                ofs += input[ip] as i32;
                ip += 1;
                ref_offset = op as i32 - ofs - MAX_DISTANCE as i32;
            }

            if op + len as usize > op_limit { return 0; }
            ref_offset -= 1;
            if ref_offset < 0 { return 0; }

            if ip >= ip_limit { break; }
            ctrl = input[ip] as u32;
            ip += 1;

            let ref_pos = ref_offset as usize;

            if ref_pos == op - 1 {
                // Run: fill with repeated byte
                let val = output[ref_pos];
                output[op..op + len as usize].fill(val);
                op += len as usize;
            } else {
                // Copy match, handling overlap correctly.
                // Each byte reads from ref_pos+i, which may overlap with output being written.
                for i in 0..len as usize {
                    output[op + i] = output[ref_pos + i];
                }
                op += len as usize;
            }
        } else {
            // Literal
            ctrl += 1;
            if op + ctrl as usize > op_limit { return 0; }
            if ip + ctrl as usize > ip_limit { return 0; }

            output[op..op + ctrl as usize].copy_from_slice(&input[ip..ip + ctrl as usize]);
            op += ctrl as usize;
            ip += ctrl as usize;

            if ip >= ip_limit { break; }
            ctrl = input[ip] as u32;
            ip += 1;
        }
    }

    op as i32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compress_decompress_roundtrip() {
        // Use highly compressible data (repeated pattern)
        let data: Vec<u8> = b"BloscLZ compression test with repeating data patterns! "
            .iter().cycle().take(40000).copied().collect();
        let mut compressed = vec![0u8; data.len() + 1000];
        let csize = compress(5, &data, &mut compressed);
        assert!(csize > 0, "Compression failed");

        let mut decompressed = vec![0u8; data.len()];
        let dsize = decompress(&compressed[..csize as usize], &mut decompressed);
        assert_eq!(dsize as usize, data.len());
        assert_eq!(data, decompressed);
    }

    #[test]
    fn test_incompressible() {
        // Random-looking data
        let data: Vec<u8> = (0..1000u32).map(|i| {
            ((i.wrapping_mul(7919).wrapping_add(104729)) & 0xFF) as u8
        }).collect();
        let mut compressed = vec![0u8; data.len() + 100];
        let _csize = compress(1, &data, &mut compressed);
        // May or may not compress; that's fine
    }
}
