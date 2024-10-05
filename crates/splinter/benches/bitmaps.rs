use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use splinter::{writer::SplinterWriter, Splinter};
use std::{hint::black_box, io};

fn mksplinter(values: impl IntoIterator<Item = u32>) -> Vec<u8> {
    let buf = io::Cursor::new(vec![]);
    let (_, mut writer) = SplinterWriter::new(buf).unwrap();
    for i in values {
        writer.push(i).unwrap();
    }
    let (_, buf) = writer.finish().unwrap();
    buf.into_inner()
}

fn benchmark_contains(c: &mut Criterion) {
    let cardinalities = [4u32, 16, 64, 256, 1024, 4096, 16384];

    let mut group = c.benchmark_group("splinter");

    for &cardinality in &cardinalities {
        group.throughput(Throughput::Elements(cardinality as u64));

        let data = mksplinter(0..cardinality);
        let splinter = Splinter::from_bytes(&data).unwrap();

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
