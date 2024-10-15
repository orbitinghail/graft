use bytes::BytesMut;
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use splinter::{writer::SplinterBuilder, Splinter};
use std::hint::black_box;

fn mksplinter(values: impl IntoIterator<Item = u32>) -> Splinter {
    let mut writer = SplinterBuilder::new(BytesMut::default());
    for i in values {
        writer.push(i);
    }
    Splinter::from_bytes(writer.build().freeze()).unwrap()
}

fn benchmark_contains(c: &mut Criterion) {
    let cardinalities = [4u32, 16, 64, 256, 1024, 4096, 16384];

    let mut group = c.benchmark_group("splinter");

    for &cardinality in &cardinalities {
        group.throughput(Throughput::Elements(cardinality as u64));

        let splinter = mksplinter(0..cardinality);

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
        group.throughput(Throughput::Elements(cardinality as u64));

        let bitmap = roaring::RoaringBitmap::from_sorted_iter(0..cardinality).unwrap();

        // we want to lookup the cardinality/3th element
        let lookup = cardinality / 3;
        assert!(bitmap.contains(black_box(lookup)));
        group.bench_function(format!("contains {lookup}"), |b| {
            b.iter(|| bitmap.contains(black_box(lookup)))
        });
    }
    group.finish();
}

criterion_group!(benches, benchmark_contains);
criterion_main!(benches);
