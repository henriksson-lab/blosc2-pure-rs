use blosc2_pure_rs::filters;
use std::time::Instant;

fn bench(size: usize) {
    let mut src = vec![0u8; size];
    for i in 0..size {
        src[i] = (i * 7) as u8;
    }
    let mut shuffled = vec![0u8; size];
    filters::shuffle(4, &src, &mut shuffled);
    let mut restored = vec![0u8; size];
    for _ in 0..3 {
        filters::unshuffle(4, &shuffled, &mut restored);
    }
    let iters = 10000;
    let t = Instant::now();
    for _ in 0..iters {
        filters::unshuffle(
            4,
            std::hint::black_box(&shuffled),
            std::hint::black_box(&mut restored),
        );
    }
    let el = t.elapsed();
    let per = el / iters;
    let gbps = size as f64 / per.as_secs_f64() / 1e9;
    println!("{:>7} bytes: {:?}/iter {:.1} GB/s", size, per, gbps);
}
fn main() {
    bench(32 * 1024);
    bench(64 * 1024);
    bench(128 * 1024);
    bench(256 * 1024);
    bench(512 * 1024);
    bench(1024 * 1024);
}
