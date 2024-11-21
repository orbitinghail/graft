use std::path::PathBuf;

use anyhow::Result;
use clap::{arg, Parser, Subcommand};
use graft_core::{page::Page, VolumeId};
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
        #[arg(default_value_t = 0)]
        offset: u32,
    },
}

fn print_page(page: Page) {
    for &byte in page.iter() {
        // if byte is a printable ascii character
        if byte.is_ascii_alphanumeric() || byte.is_ascii_punctuation() || byte.is_ascii_whitespace()
        {
            print!("{}", byte as char);
        }
    }
    println!();
}

fn print_segment(segment: &ClosedSegment) {
    for (vid, offset, page) in segment.iter() {
        let page_empty = page.iter().all(|&b| b == 0);
        println!("{}: {} empty={}", vid, offset, page_empty);
        print_page(page);
    }
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    // open segment
    let data = std::fs::read(&cli.segment)?;
    let segment = ClosedSegment::from_bytes(&data)?;

    match cli.command {
        Commands::Print => print_segment(&segment),
        Commands::Read { vid, offset } => {
            if let Some(page) = segment.find_page(vid, offset) {
                print_page(page)
            } else {
                eprintln!("page not found")
            }
        }
    }

    Ok(())
}
