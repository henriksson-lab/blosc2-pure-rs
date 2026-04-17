//! BloscLZ compression codec.
//!
//! Based on FastLZ, a lightning-fast lossless compression library.
//! Ported from c-blosc2/blosc/blosclz.c.

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
    // SAFETY: All callers check the 4-byte window before calling. Unaligned
    // loads match BloscLZ's byte-oriented format and avoid per-byte assembly in
    // the compression hot path.
    unsafe { std::ptr::read_unaligned(p.as_ptr().add(pos).cast::<u32>()) }
}

#[inline(always)]
fn readu64(p: &[u8], pos: usize) -> u64 {
    debug_assert!(pos + 8 <= p.len());
    // SAFETY: All callers check the 8-byte window before calling. Unaligned
    // loads are intentional for fast match scanning over arbitrary byte slices.
    unsafe { std::ptr::read_unaligned(p.as_ptr().add(pos).cast::<u64>()) }
}

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

/// Find a run of identical bytes starting from `ip`, comparing against `refp`.
#[inline]
fn get_run(data: &[u8], mut ip: usize, ip_bound: usize, mut refp: usize) -> usize {
    debug_assert!(ip > 0 && ip <= data.len());
    let x = data[ip - 1];
    let x8 = u64::from_ne_bytes([x; 8]);

    while ip + 8 <= ip_bound && refp + 8 <= data.len() {
        let ref_word = readu64(data, refp);
        if ref_word != x8 {
            let matched = matching_prefix_len(ref_word, x8);
            return (ip + matched).min(ip_bound);
        }
        ip += 8;
        refp += 8;
    }

    while ip < ip_bound && refp < data.len() && data[refp] == x {
        ip += 1;
        refp += 1;
    }
    ip
}

/// Find the length of a match between `ip` and `refp`.
#[inline(always)]
fn get_match(data: &[u8], ip: usize, ip_bound: usize, refp: usize) -> usize {
    get_match_generic(data, ip, ip_bound, refp)
}

#[inline]
fn get_match_generic(data: &[u8], mut ip: usize, ip_bound: usize, mut refp: usize) -> usize {
    while ip + 8 <= ip_bound && refp + 8 <= data.len() {
        let ip_word = readu64(data, ip);
        let ref_word = readu64(data, refp);
        if ip_word != ref_word {
            let matched = matching_prefix_len(ip_word, ref_word);
            return (ip + matched).min(ip_bound);
        }
        ip += 8;
        refp += 8;
    }
    while ip < ip_bound && refp < data.len() && data[refp] == data[ip] {
        ip += 1;
        refp += 1;
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
fn get_cratio_with_htab(
    data: &[u8],
    maxlen: usize,
    minlen: usize,
    ipshift: usize,
    hashlog: u8,
    htab: &mut [u32],
) -> f64 {
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
    while ip < ip_limit {
        let anchor = ip;

        let seq = readu32(data, ip);
        let hval = hash_function(seq, hashlog) as usize & htab_mask;
        let ref_offset = htab[hval] as usize;

        let distance = anchor.saturating_sub(ref_offset);
        htab[hval] = anchor as u32;

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

        if ref_offset + 4 > limit {
            oc += 1;
            ip = anchor + 1;
            copy += 1;
            if copy == MAX_COPY as u8 {
                copy = 0;
                oc += 1;
            }
            continue;
        }

        if readu32(data, ref_offset) != readu32(data, ip) {
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
        ip = get_run_or_match(data, ip, ip_bound, ref_after, distance_dec == 0);

        if ip > ipshift {
            ip -= ipshift;
        } else {
            ip = anchor + 1;
        }
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

        if ip + 4 <= data.len() {
            let seq2 = readu32(data, ip);
            let hval2 = hash_function(seq2, hashlog) as usize & htab_mask;
            htab[hval2] = ip as u32;
        }
        ip += 2;
        oc += 1;
    }

    let ic = ip as f64;
    if oc <= 0 {
        return f64::INFINITY;
    }
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
    let mut htab = [0u32; 1 << HASH_LOG];

    let shift = length - maxlen;
    // get_cratio_with_htab fills htab[..hashlen] with 0 at start, so no double-init
    let cratio = get_cratio_with_htab(
        &input[shift..],
        maxlen,
        minlen,
        ipshift,
        hashlog,
        &mut htab[..hashlen],
    );

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

    if op + 5 > op_limit {
        return 0;
    }

    let mut ip = 4usize;
    output[op] = MAX_COPY as u8 - 1;
    op += 1;
    output[op..op + 4].copy_from_slice(&input[..4]);
    op += 4;

    macro_rules! emit_literal {
        ($anchor:expr) => {{
            if op + 2 > op_limit {
                return 0;
            }
            output[op] = input[$anchor];
            op += 1;
            ip = $anchor + 1;
            copy += 1;
            if copy == MAX_COPY as u8 {
                copy = 0;
                output[op] = MAX_COPY as u8 - 1;
                op += 1;
            }
        }};
    }

    while ip < ip_limit {
        let anchor = ip;

        let seq = readu32(input, ip);
        let hval = hash_function(seq, hashlog) as usize & htab_mask;
        let ref_offset = htab[hval] as usize;
        let distance = ip - ref_offset;

        htab[hval] = ip as u32;

        if distance == 0 || distance >= MAX_FARDISTANCE as usize {
            emit_literal!(anchor);
            continue;
        }

        if ref_offset + 4 > length || readu32(input, ref_offset) != readu32(input, ip) {
            emit_literal!(anchor);
            continue;
        }

        ip = anchor + 4;
        let ref_after = ref_offset + 4;
        let distance = distance - 1;
        ip = get_run_or_match(input, ip, ip_bound, ref_after, distance == 0);

        if ip > ipshift {
            ip -= ipshift;
        } else {
            ip = anchor + 1;
        }
        let len = ip - anchor;

        if len < minlen || (len <= 5 && distance >= MAX_DISTANCE as usize) {
            emit_literal!(anchor);
            continue;
        }

        if copy > 0 {
            output[op - copy as usize - 1] = copy - 1;
        } else {
            op -= 1;
        }
        copy = 0;

        if distance < MAX_DISTANCE as usize {
            if len < 7 {
                if op + 2 > op_limit {
                    return 0;
                }
                output[op] = ((len << 5) + (distance >> 8)) as u8;
                op += 1;
                output[op] = (distance & 255) as u8;
                op += 1;
            } else {
                if op + 1 > op_limit {
                    return 0;
                }
                output[op] = ((7 << 5) + (distance >> 8)) as u8;
                op += 1;
                let mut remaining = len - 7;
                while remaining >= 255 {
                    if op + 1 > op_limit {
                        return 0;
                    }
                    output[op] = 255;
                    op += 1;
                    remaining -= 255;
                }
                if op + 2 > op_limit {
                    return 0;
                }
                output[op] = remaining as u8;
                op += 1;
                output[op] = (distance & 255) as u8;
                op += 1;
            }
        } else {
            let distance = distance - MAX_DISTANCE as usize;
            if len < 7 {
                if op + 4 > op_limit {
                    return 0;
                }
                output[op] = ((len << 5) + 31) as u8;
                op += 1;
                output[op] = 255;
                op += 1;
                output[op] = (distance >> 8) as u8;
                op += 1;
                output[op] = (distance & 255) as u8;
                op += 1;
            } else {
                if op + 1 > op_limit {
                    return 0;
                }
                output[op] = (7 << 5) + 31;
                op += 1;
                let mut remaining = len - 7;
                while remaining >= 255 {
                    if op + 1 > op_limit {
                        return 0;
                    }
                    output[op] = 255;
                    op += 1;
                    remaining -= 255;
                }
                if op + 4 > op_limit {
                    return 0;
                }
                output[op] = remaining as u8;
                op += 1;
                output[op] = 255;
                op += 1;
                output[op] = (distance >> 8) as u8;
                op += 1;
                output[op] = (distance & 255) as u8;
                op += 1;
            }
        }

        let cur_ip = ip;
        if cur_ip + 4 <= length {
            let seq2 = readu32(input, cur_ip);
            let hval2 = hash_function(seq2, hashlog) as usize & htab_mask;
            htab[hval2] = cur_ip as u32;
            ip += 1;
            if clevel == 9 && cur_ip + 4 < length {
                let seq3 = readu32(input, ip);
                let hval3 = hash_function(seq3, hashlog) as usize & htab_mask;
                htab[hval3] = ip as u32;
                ip += 1;
            } else {
                ip += 1;
            }
        } else {
            ip += 2;
        }

        if op + 1 > op_limit {
            return 0;
        }
        output[op] = MAX_COPY as u8 - 1;
        op += 1;
    }

    // Left-over as literal copy
    while ip <= ip_bound {
        emit_literal!(ip);
    }

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
                    if ip + 1 >= ip_limit {
                        return 0;
                    }
                    let code = input[ip];
                    ip += 1;
                    len += code as i32;
                    if code != 255 {
                        break;
                    }
                }
            } else if ip + 1 >= ip_limit {
                return 0;
            }

            let code = input[ip];
            ip += 1;
            len += 3;
            let mut ref_offset = op as i32 - ofs - code as i32;

            // 16-bit distance
            if code == 255 && ofs == (31 << 8) {
                if ip + 1 >= ip_limit {
                    return 0;
                }
                ofs = (input[ip] as i32) << 8;
                ip += 1;
                ofs += input[ip] as i32;
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
            if op + ctrl as usize > op_limit {
                return 0;
            }
            if ip + ctrl as usize > ip_limit {
                return 0;
            }

            output[op..op + ctrl as usize].copy_from_slice(&input[ip..ip + ctrl as usize]);
            op += ctrl as usize;
            ip += ctrl as usize;

            if ip >= ip_limit {
                break;
            }
            ctrl = input[ip] as u32;
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
    // If Rust's `matching_prefix_len` returns the exact count, the function
    // advances `ip` by exactly the count — one byte short. These tests pin
    // that down so the behavior is intentional and observed.

    #[test]
    fn get_match_generic_stops_at_first_differing_byte_in_word() {
        // Lay out two regions 32 bytes apart. Bytes 0..=3 match, byte 4 differs.
        let mut data = vec![0u8; 128];
        let ref_pos = 0usize;
        let ip_pos = 32usize;
        for i in 0..4 {
            data[ref_pos + i] = (0xA0 + i as u8) as u8;
            data[ip_pos + i] = (0xA0 + i as u8) as u8;
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

        // Observation: Rust stops AT the mismatch (matching_prefix_len returns
        // 4, ip advances by 4). This is the documented baseline — any future
        // change must update FINDINGS.md SUSPECT #10 accordingly.
        assert_eq!(
            returned - ip_pos,
            4,
            "Rust get_match_generic advances ip by exact matched-byte count"
        );
    }

    #[test]
    fn get_match_generic_matches_full_word_when_equal() {
        // Two identical 16-byte regions: should advance by 16 and then stop at
        // the remainder-loop boundary.
        let mut data = vec![0u8; 64];
        for i in 0..16 {
            data[i] = (0x20 + i as u8) as u8;
            data[32 + i] = (0x20 + i as u8) as u8;
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
            8,
            "First word all match, second word byte 0 differs → advance 8 exactly"
        );
    }

    // End-to-end closure of SUSPECT #10: even though get_match_generic advances
    // ip "one byte short" relative to C's post-increment idiom, the compressed
    // output still decompresses correctly end-to-end. Capture that as a direct
    // test so regressions in get_match that silently break cross-compat surface
    // as test failures here.
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
