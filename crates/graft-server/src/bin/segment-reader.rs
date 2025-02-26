use std::{fmt::Debug, io, path::PathBuf, process::exit};

use clap::{Parser, Subcommand, arg};
use culprit::{Culprit, ResultExt};
use graft_core::{
    PageIdx, VolumeId,
    page::{PAGESIZE, Page},
};
use graft_server::segment::closed::{ClosedSegment, SegmentValidationErr};
use thiserror::Error;

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

        /// The page index
        #[arg(default_value_t = PageIdx::FIRST)]
        pageidx: PageIdx,
    },
}

#[derive(Error, Debug)]
enum SegmentReaderErr {
    #[error("IO error")]
    Io(std::io::ErrorKind),

    #[error("Segment validation error")]
    Segment(#[from] SegmentValidationErr),

    #[error("page not found")]
    PageNotFound,
}

impl From<io::Error> for SegmentReaderErr {
    fn from(e: io::Error) -> Self {
        Self::Io(e.kind())
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

    // print table headers: Volume id, page, length, page_prefix
    println!(
        "{:<10} {:<10} {:<10} Prefix",
        "Volume ID", "PageIdx", "Empty"
    );

    for (vid, pageidx, page) in segment.iter() {
        print!(
            "{:<10} {:<10} {:<10} ",
            vid.short(),
            pageidx,
            page.is_empty()
        );
        print_page(page, 10);
    }
}

fn main() {
    if let Err(e) = main_inner() {
        eprintln!("Error: {e:?}");
        exit(1)
    }
}

fn main_inner() -> Result<(), Culprit<SegmentReaderErr>> {
    let cli = Cli::parse();

    // open segment
    let data = std::fs::read(&cli.segment)?;
    let segment = ClosedSegment::from_bytes(&data).or_into_ctx()?;

    match cli.command {
        Commands::Print => {
            println!("Segment size: {}", data.len());
            print_segment(&segment)
        }
        Commands::Read { vid, pageidx } => {
            if let Some(page) = segment.find_page(vid, pageidx) {
                print_page(page, PAGESIZE.as_usize())
            } else {
                return Err(Culprit::new(SegmentReaderErr::PageNotFound));
            }
        }
    }

    Ok(())
}
