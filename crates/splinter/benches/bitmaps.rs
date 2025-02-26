use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use roaring::RoaringBitmap;
use splinter::{Splinter, SplinterRef, ops::Intersection};
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

    let mut group = c.benchmark_group("contains");

    for &cardinality in &cardinalities {
        // we want to lookup the cardinality/3th element
        let lookup = cardinality / 3;

        group.bench_function(BenchmarkId::new("splinter", cardinality), |b| {
            let splinter = mksplinter(0..cardinality);
            assert!(splinter.contains(black_box(lookup)));
            b.iter(|| splinter.contains(black_box(lookup)))
        });

        group.bench_function(BenchmarkId::new("splinter ref", cardinality), |b| {
            let splinter = mksplinter_ref(0..cardinality);
            assert!(splinter.contains(black_box(lookup)));
            b.iter(|| splinter.contains(black_box(lookup)))
        });

        group.bench_function(BenchmarkId::new("roaring", cardinality), |b| {
            let bitmap = RoaringBitmap::from_sorted_iter(0..cardinality).unwrap();
            assert!(bitmap.contains(black_box(lookup)));
            b.iter(|| bitmap.contains(black_box(lookup)))
        });
    }

    group.finish();
}

fn benchmark_insert(c: &mut Criterion) {
    const MAGIC: u32 = 513;

    let mut group = c.benchmark_group("insert");

    group.bench_function("splinter/warm", |b| {
        let mut splinter = Splinter::default();
        b.iter(|| splinter.insert(black_box(MAGIC)))
    });

    group.bench_function("roaring/warm", |b| {
        let mut roaring_bitmap = RoaringBitmap::default();
        b.iter(|| roaring_bitmap.insert(black_box(MAGIC)))
    });

    group.bench_function("splinter/cold", |b| {
        b.iter(|| Splinter::default().insert(black_box(MAGIC)))
    });

    group.bench_function("roaring/cold", |b| {
        b.iter(|| RoaringBitmap::default().insert(black_box(MAGIC)))
    });

    group.finish();
}

fn benchmark_intersection(c: &mut Criterion) {
    let cardinalities = [4u32, 16, 64, 256, 1024, 4096, 16384];

    let mut group = c.benchmark_group("intersection");

    for cardinality in &cardinalities {
        group.bench_with_input(
            BenchmarkId::new("splinter", cardinality),
            cardinality,
            |b, &cardinality| {
                let splinter1 = mksplinter(0..cardinality);
                let splinter2 = mksplinter(cardinality / 2..cardinality);

                assert_eq!(splinter1.intersection(black_box(&splinter2)), splinter2);
                b.iter(|| splinter1.intersection(black_box(&splinter2)))
            },
        );

        group.bench_with_input(
            BenchmarkId::new("splinter ref", cardinality),
            cardinality,
            |b, &cardinality| {
                let splinter1 = mksplinter(0..cardinality);
                let splinter2 = mksplinter(cardinality / 2..cardinality);
                let splinter_ref2 = mksplinter_ref(cardinality / 2..cardinality);

                assert_eq!(splinter1.intersection(black_box(&splinter_ref2)), splinter2);
                b.iter(|| splinter1.intersection(black_box(&splinter_ref2)))
            },
        );

        group.bench_with_input(
            BenchmarkId::new("roaring", cardinality),
            cardinality,
            |b, &cardinality| {
                let roaring1 = RoaringBitmap::from_sorted_iter(0..cardinality).unwrap();
                let roaring2 =
                    RoaringBitmap::from_sorted_iter(cardinality / 2..cardinality).unwrap();

                assert_eq!((&roaring1 & black_box(&roaring2)), roaring2);
                b.iter(|| (&roaring1 & black_box(&roaring2)))
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    benchmark_contains,
    benchmark_insert,
    benchmark_intersection
);
criterion_main!(benches);
