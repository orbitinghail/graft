use clap::{Parser, Subcommand};
use graft::core::{LogId, SegmentId, VolumeId};

#[derive(Subcommand)]
enum Tool {
    /// Generate a Volume ID (vid)
    Vid,
    /// Generate a Log ID (logid)
    Log,
    /// Generate a Segment ID (sid)
    Sid,
}

#[derive(Parser)]
#[command(version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    #[command(subcommand)]
    tool: Tool,
}

fn main() {
    let cli = Cli::parse();
    match cli.tool {
        Tool::Vid => println!("{}", VolumeId::random()),
        Tool::Log => println!("{}", LogId::random()),
        Tool::Sid => println!("{}", SegmentId::random()),
    }
}
