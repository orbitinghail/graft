use bytes::Bytes;
use criterion::{criterion_group, criterion_main, Criterion};
use roaring::RoaringBitmap;
use splinter::{Splinter, SplinterRef};
use std::hint::black_box;

fn mksplinter(values: impl IntoIterator<Item = u32>) -> Splinter {
    let mut splinter = Splinter::default();
    for i in values {
        splinter.insert(i);
    }
    splinter
}

fn mksplinter_ref(values: impl IntoIterator<Item = u32>) -> SplinterRef<Bytes> {
    SplinterRef::from_bytes(mksplinter(values).serialize_to_bytes()).unwrap()
}

fn benchmark_contains(c: &mut Criterion) {
    let cardinalities = [4u32, 16, 64, 256, 1024, 4096, 16384];

    let mut group = c.benchmark_group("splinter");

    for &cardinality in &cardinalities {
        let splinter = mksplinter(0..cardinality);

        // we want to lookup the cardinality/3th element
        let lookup = cardinality / 3;
        assert!(splinter.contains(black_box(lookup)));
        group.bench_function(format!("contains {lookup}"), |b| {
            b.iter(|| splinter.contains(black_box(lookup)))
        });
    }
    group.finish();

    let mut group = c.benchmark_group("splinter_ref");

    for &cardinality in &cardinalities {
        let splinter = mksplinter_ref(0..cardinality);

        // we want to lookup the cardinality/3th element
        let lookup = cardinality / 3;
        assert!(splinter.contains(black_box(lookup)));
        group.bench_function(format!("contains {lookup}"), |b| {
            b.iter(|| splinter.contains(black_box(lookup)))
        });
    }
    group.finish();

    let mut group = c.benchmark_group("roaring");
    for &cardinality in &cardinalities {
        let bitmap = RoaringBitmap::from_sorted_iter(0..cardinality).unwrap();

        // we want to lookup the cardinality/3th element
        let lookup = cardinality / 3;
        assert!(bitmap.contains(black_box(lookup)));
        group.bench_function(format!("contains {lookup}"), |b| {
            b.iter(|| bitmap.contains(black_box(lookup)))
        });
    }
    group.finish();
}

fn benchmark_insert(c: &mut Criterion) {
    const MAGIC: u32 = 513;

    let mut group = c.benchmark_group("insert warm");

    let mut splinter = Splinter::default();
    group.bench_function("splinter", |b| b.iter(|| splinter.insert(black_box(MAGIC))));

    let mut roaring_bitmap = RoaringBitmap::default();
    group.bench_function("roaring", |b| {
        b.iter(|| roaring_bitmap.insert(black_box(MAGIC)))
    });

    group.finish();

    let mut group = c.benchmark_group("insert cold");
    group.bench_function("splinter", |b| {
        b.iter(|| Splinter::default().insert(black_box(MAGIC)))
    });
    group.bench_function("roaring", |b| {
        b.iter(|| RoaringBitmap::default().insert(black_box(MAGIC)))
    });
    group.finish();
}

criterion_group!(benches, benchmark_contains, benchmark_insert);
criterion_main!(benches);
