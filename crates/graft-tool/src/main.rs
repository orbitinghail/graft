use clap::{Parser, Subcommand};
use graft_core::{ClientId, VolumeId};

#[derive(Subcommand)]
enum Tool {
    /// Generate a Volume ID (vid)
    Vid,
    /// Generate a Client ID (cid)
    Cid {
        #[clap(short, long)]
        derive: Option<String>,
    },
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
        Tool::Cid { derive } => match derive {
            Some(derive) => {
                println!("{}", ClientId::derive(derive.as_bytes()))
            }
            None => println!("{}", ClientId::random()),
        },
    }
}
