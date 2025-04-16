use std::str::FromStr;

use clap::{Parser, Subcommand};
use graft_core::{ClientId, VolumeId};

#[derive(thiserror::Error, Debug)]
pub enum HexEncodedKeyErr {
    #[error("invalid hex encoding")]
    InvalidHex,
    #[error("invalid key length")]
    InvalidLength,
}

#[derive(Clone)]
pub struct HexEncodedKey([u8; 32]);

impl FromStr for HexEncodedKey {
    type Err = HexEncodedKeyErr;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let bytes = hex::decode(s).map_err(|_| HexEncodedKeyErr::InvalidHex)?;
        if bytes.len() != 32 {
            return Err(HexEncodedKeyErr::InvalidLength);
        }
        let mut key = [0; 32];
        key.copy_from_slice(&bytes);
        Ok(HexEncodedKey(key))
    }
}

impl From<HexEncodedKey> for rusty_paseto::core::Key<32> {
    fn from(hex_key: HexEncodedKey) -> Self {
        rusty_paseto::core::Key::from(hex_key.0)
    }
}

#[derive(Subcommand)]
enum Tool {
    /// Generate a Volume ID (vid)
    Vid,
    /// Generate a Client ID (cid)
    Cid {
        #[clap(short, long)]
        derive: Option<String>,
    },

    /// Generate a new 32 byte hex encoded random key to use to create and
    /// validate Graft api tokens
    SecretKey,

    /// Generate an API key to use to communicate with Graft services.
    Token {
        #[clap(long)]
        sk: HexEncodedKey,

        subject: String,
    },

    /// Validate an API key
    ValidateToken {
        #[clap(long)]
        sk: HexEncodedKey,

        token: String,
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
        Tool::SecretKey => {
            let rand = rand::random::<[u8; 32]>();
            println!("{}", hex::encode(rand));
        }
        Tool::Token { sk, subject } => {
            use rusty_paseto::prelude::*;

            let key = PasetoSymmetricKey::from(Key::from(sk));
            let token = PasetoBuilder::<V4, Local>::default()
                .set_claim(SubjectClaim::from(subject.as_str()))
                .set_no_expiration_danger_acknowledged()
                .build(&key)
                .unwrap();
            println!("{token}");
        }
        Tool::ValidateToken { sk, token } => {
            use rusty_paseto::prelude::*;

            let key = PasetoSymmetricKey::from(Key::from(sk));
            let claims = PasetoParser::<V4, Local>::default()
                .parse(&token, &key)
                .unwrap();
            println!("{claims:#?}");
        }
    }
}
