use std::path::PathBuf;

use anyhow::Result;
use clap::{arg, Parser, Subcommand};
use graft_core::{
    page::{Page, PAGESIZE},
    page_offset::PageOffset,
    VolumeId,
};
use graft_server::segment::closed::ClosedSegment;

#[derive(Parser)]
#[command(version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    /// The segment to operate on
    segment: PathBuf,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, PartialEq)]
enum Commands {
    /// Print out all pages in a segment
    Print,

    /// Read a single page from a segment
    Read {
        /// The volume id of the page
        vid: VolumeId,

        /// The offset of the page
        #[arg(default_value_t = PageOffset::ZERO)]
        offset: PageOffset,
    },
}

fn print_page(page: Page, max: usize) {
    for &byte in page.iter().take(max) {
        // if byte is a printable ascii character
        if byte.is_ascii_alphanumeric() || byte.is_ascii_punctuation() || byte.is_ascii_whitespace()
        {
            print!("{}", byte as char);
        }
    }
    println!();
}

fn print_segment(segment: &ClosedSegment) {
    println!("Segment ID: {}", segment.sid());
    println!("Pages: {}", segment.pages());

    // print table headers: Volume id, offset, length, page_prefix
    println!(
        "{:<10} {:<10} {:<10} Prefix",
        "Volume ID", "Offset", "Empty"
    );

    for (vid, offset, page) in segment.iter() {
        print!(
            "{:<10} {:<10} {:<10} ",
            vid.short(),
            offset,
            page.iter().all(|&b| b == 0)
        );
        print_page(page, 10);
    }
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    // open segment
    let data = std::fs::read(&cli.segment)?;
    let segment = ClosedSegment::from_bytes(&data)?;

    match cli.command {
        Commands::Print => {
            println!("Segment size: {}", data.len());
            print_segment(&segment)
        }
        Commands::Read { vid, offset } => {
            if let Some(page) = segment.find_page(vid, offset) {
                print_page(page, PAGESIZE.as_usize())
            } else {
                eprintln!("page not found")
            }
        }
    }

    Ok(())
}
