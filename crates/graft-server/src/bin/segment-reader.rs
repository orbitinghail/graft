use std::{fmt::Debug, path::PathBuf, process::exit};

use clap::{arg, Parser, Subcommand};
use graft_core::{
    page::{Page, PAGESIZE},
    page_offset::PageOffset,
    VolumeId,
};
use graft_server::segment::closed::{ClosedSegment, SegmentValidationErr};
use thiserror::Error;
use trackerr::{format_location_stack, CallerLocation, LocationStack};

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

#[derive(Error)]
enum SegmentReaderErr {
    #[error("IO error")]
    Io(#[from] std::io::Error, #[implicit] CallerLocation),

    #[error("Segment validation error")]
    Segment(#[from] SegmentValidationErr, #[implicit] CallerLocation),

    #[error("page not found")]
    PageNotFound(CallerLocation),
}

impl LocationStack for SegmentReaderErr {
    fn location(&self) -> &CallerLocation {
        use SegmentReaderErr::*;
        match self {
            Io(_, loc) | Segment(_, loc) | PageNotFound(loc) => loc,
        }
    }

    fn next(&self) -> Option<&dyn LocationStack> {
        use SegmentReaderErr::*;
        match self {
            Segment(err, _) => Some(err),
            _ => None,
        }
    }
}

impl Debug for SegmentReaderErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        format_location_stack(f, self)
    }
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

fn main() {
    if let Err(e) = main_inner() {
        eprintln!("Error: {:?}", e);
        exit(1)
    }
}

fn main_inner() -> Result<(), SegmentReaderErr> {
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
                return Err(SegmentReaderErr::PageNotFound(Default::default()));
            }
        }
    }

    Ok(())
}
