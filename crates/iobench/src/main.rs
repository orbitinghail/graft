//! A little playground for rust io perf

use std::{
    fs::{OpenOptions, create_dir_all, remove_dir_all},
    io::{Seek, Write},
    os::unix::fs::FileExt,
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc::{self, Sender},
    },
    thread,
    time::{Duration, Instant},
};

use rand::Rng;

struct ReaderStats {
    worker: i32,
    total_reads: usize,
    total_bytes_read: usize,
    elapsed: Duration,
}

const PAGE_SIZE: usize = 16384;
const NUM_PAGES: usize = (1024 * 1024 * 1024 * 10) / PAGE_SIZE;
const TOTAL_BYTES: usize = NUM_PAGES * PAGE_SIZE;
const FSYNC_INTERVAL: Duration = Duration::from_millis(1);

/// the maximum offset of the file that is safe to read
/// readers should never read a byte larger than this offset
static FLUSH_OFFSET: AtomicUsize = AtomicUsize::new(0);

const READ_TARGET: usize = 1024 * 1024 * 1024; // 1 GB

fn reader(worker: i32, path: PathBuf, tx: Sender<ReaderStats>) {
    // Wait for the file to have some data
    loop {
        let offset = FLUSH_OFFSET.load(Ordering::SeqCst);
        if offset >= PAGE_SIZE * 100 {
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }

    let file = OpenOptions::new()
        .read(true)
        .open(&path)
        .expect("failed to open file for reading");

    let mut rng = rand::rng();
    let mut buf = vec![0u8; PAGE_SIZE];
    let mut total_bytes_read = 0usize;
    let mut total_reads = 0usize;
    let start = Instant::now();

    // Read random pages from the file no larger than the flush offset, until we have
    // read a total of 1 GB of pages from the file
    while total_bytes_read < READ_TARGET {
        let flush_offset = FLUSH_OFFSET.load(Ordering::SeqCst);
        let max_page = flush_offset / PAGE_SIZE;

        if max_page == 0 {
            thread::sleep(Duration::from_millis(1));
            continue;
        }

        let page_idx = rng.random_range(0..max_page);
        let offset = (page_idx * PAGE_SIZE) as u64;

        let bytes_read = file.read_at(&mut buf, offset).expect("failed to read");

        total_bytes_read += bytes_read;
        total_reads += 1;
    }

    let elapsed = start.elapsed();

    tx.send(ReaderStats {
        worker,
        total_reads,
        total_bytes_read,
        elapsed,
    })
    .expect("failed to send reader stats");
}

fn main() -> anyhow::Result<()> {
    let root = PathBuf::from("./pagestore-data");
    println!("using pagestore root: {}", root.display());
    if root.exists() {
        remove_dir_all(&root)?;
    }
    create_dir_all(&root)?;

    let mut writer = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(root.join("data"))?;

    let (tx, rx) = mpsc::channel();
    let num_readers = 4;

    let data_path = root.join("data");
    for i in 0..num_readers {
        let path = data_path.clone();
        let tx = tx.clone();
        thread::spawn(move || reader(i, path, tx));
    }
    drop(tx); // Drop original sender so rx will close when all readers finish

    println!(
        "writing {} pages ({} bytes each, {} total)",
        NUM_PAGES,
        PAGE_SIZE,
        format_bytes(TOTAL_BYTES)
    );

    let write_buf = vec![0u8; PAGE_SIZE];

    // write to the pagestore
    let mut last_flush = Instant::now();
    let mut flush_elapsed = Duration::ZERO;
    let mut write_elapsed = Duration::ZERO;
    for _ in 0..NUM_PAGES {
        let write_start = Instant::now();
        writer.write_all(&write_buf)?;
        writer.flush()?;
        write_elapsed += write_start.elapsed();

        if last_flush.elapsed() > FSYNC_INTERVAL {
            let current_offset = writer.stream_position()? as usize;
            let flush_start = Instant::now();
            writer.sync_data()?;
            flush_elapsed += flush_start.elapsed();
            FLUSH_OFFSET.store(current_offset, Ordering::SeqCst);
            last_flush = Instant::now();
        }
    }

    let flush_start = Instant::now();
    writer.sync_all()?;
    flush_elapsed += flush_start.elapsed();

    let total_elapsed = write_elapsed + flush_elapsed;

    let write_throughput = TOTAL_BYTES as f64 / write_elapsed.as_secs_f64();
    let total_throughput = TOTAL_BYTES as f64 / total_elapsed.as_secs_f64();

    println!("\n--- Write Results ---");
    println!("write time:  {:?}", write_elapsed);
    println!("flush time:  {:?}", flush_elapsed);
    println!("total time:  {:?}", total_elapsed);
    println!(
        "write throughput: {}/s",
        format_bytes(write_throughput as usize)
    );
    println!(
        "total throughput: {}/s",
        format_bytes(total_throughput as usize)
    );

    // Collect and print reader stats
    println!("\n--- Read Results ---");
    let mut stats: Vec<ReaderStats> = rx.iter().collect();
    stats.sort_by_key(|s| s.worker);

    for stat in &stats {
        let throughput = stat.total_bytes_read as f64 / stat.elapsed.as_secs_f64();
        println!(
            "reader {}: {} reads, {} in {:?} ({}/s)",
            stat.worker,
            stat.total_reads,
            format_bytes(stat.total_bytes_read),
            stat.elapsed,
            format_bytes(throughput as usize)
        );
    }

    let total_read_bytes: usize = stats.iter().map(|s| s.total_bytes_read).sum();
    let total_reads: usize = stats.iter().map(|s| s.total_reads).sum();
    let avg_elapsed = stats.iter().map(|s| s.elapsed).sum::<Duration>() / stats.len() as u32;
    let combined_throughput = total_read_bytes as f64 / avg_elapsed.as_secs_f64();

    println!(
        "total: {} reads, {} ({}/s combined)",
        total_reads,
        format_bytes(total_read_bytes),
        format_bytes(combined_throughput as usize)
    );

    Ok(())
}

fn format_bytes(bytes: usize) -> String {
    const KB: usize = 1024;
    const MB: usize = KB * 1024;
    const GB: usize = MB * 1024;

    if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}
