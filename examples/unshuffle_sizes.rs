use blosc2_pure_rs::filters::{blosc2_shuffle, blosc2_unshuffle};
use std::time::Instant;

const TYPESIZES: [usize; 7] = [1, 2, 4, 8, 16, 255, 256];
const CHECK_SIZES: [usize; 8] = [0, 1, 3, 33, 100, 255, 1024, 4099];
const BENCH_SIZES: [usize; 6] = [
    32 * 1024 + 3,
    64 * 1024 + 3,
    128 * 1024 + 3,
    256 * 1024 + 3,
    512 * 1024 + 3,
    1024 * 1024 + 3,
];

fn make_data(size: usize) -> Vec<u8> {
    (0..size).map(|i| (i * 7) as u8).collect()
}

fn c_shuffle_layout(typesize: usize, src: &[u8]) -> Vec<u8> {
    let blocksize = src.len();
    let nelements = blocksize / typesize;
    let tail_start = nelements * typesize;
    let mut dest = vec![0u8; blocksize];

    for byte_idx in 0..typesize {
        for element in 0..nelements {
            dest[byte_idx * nelements + element] = src[element * typesize + byte_idx];
        }
    }
    dest[tail_start..].copy_from_slice(&src[tail_start..]);
    dest
}

fn roundtrip(typesize: usize, size: usize) {
    let blocksize = i32::try_from(size).expect("example blocksize fits in i32");
    let src = make_data(size);
    let mut shuffled = vec![0u8; size];
    let mut restored = vec![0u8; size];

    assert_eq!(
        blosc2_shuffle(typesize as i32, blocksize, &src, &mut shuffled),
        blocksize
    );
    assert_eq!(
        shuffled,
        c_shuffle_layout(typesize, &src),
        "shuffle layout typesize={typesize} size={size}"
    );
    assert_eq!(
        blosc2_unshuffle(typesize as i32, blocksize, &shuffled, &mut restored),
        blocksize
    );
    assert_eq!(restored, src, "typesize={typesize} size={size}");
}

fn bench(typesize: usize, size: usize) {
    roundtrip(typesize, size);

    let blocksize = i32::try_from(size).expect("example blocksize fits in i32");
    let src = make_data(size);
    let mut shuffled = vec![0u8; size];
    let mut restored = vec![0u8; size];
    blosc2_shuffle(typesize as i32, blocksize, &src, &mut shuffled);

    for _ in 0..3 {
        blosc2_unshuffle(typesize as i32, blocksize, &shuffled, &mut restored);
    }
    let iters = 10000;
    let t = Instant::now();
    for _ in 0..iters {
        blosc2_unshuffle(
            typesize as i32,
            blocksize,
            std::hint::black_box(&shuffled),
            std::hint::black_box(&mut restored),
        );
    }
    let el = t.elapsed();
    let per = el / iters;
    let gbps = size as f64 / per.as_secs_f64() / 1e9;
    assert_eq!(restored, src);
    println!(
        "typesize={typesize:>2} {size:>9} bytes: {:?}/iter {:.1} GB/s",
        per, gbps
    );
}

fn main() {
    for typesize in TYPESIZES {
        for size in CHECK_SIZES {
            roundtrip(typesize, size);
        }
    }

    for size in BENCH_SIZES {
        bench(4, size);
    }
}
