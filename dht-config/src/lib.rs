//! Configuration setup
//!
use clap::Parser;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Parser, Debug, Deserialize, Serialize)]
pub struct Cache {
    /// Database URL in sqlx format
    ///
    /// - "sqlite::memory:"
    ///
    /// - "sqlite://aaskdjkasdka.sqlite"
    ///
    /// - "postgres://postgres:mysecretpassword@localhost/postgres"
    #[arg(long, short, default_value = "sqlite::memory:")]
    url: String,

    /// Name of the table that will be used to store the dht messages
    #[arg(long, short, default_value = "domo_cache")]
    table: String,

    /// Indicates if the broker should persist the DHT messages into the DB
    /// or if the DB will only be used to populate the DHT cache when the broker starts
    #[arg(long, short, default_value_t = true)]
    persistent: bool,

    /// Path to the private key file of the broker. If the path does not exist a key file
    /// will be automatically generated
    #[arg(long, short = 'k', value_hint = clap::ValueHint::FilePath)]
    private_key: PathBuf,

    /// 32 bytes long shared key in hex format
    /// used to protect access to the DHT
    #[arg(long, short = 's')]
    shared_key: String,

    /// Use only loopback iface for libp2p
    #[arg(long)]
    loopback: bool,
}

#[derive(Parser, Debug, Deserialize, Serialize)]
pub struct Broker {
    #[clap(flatten)]
    cache: Cache,

    /// HTTP port used by the broker
    #[arg(long, short = 'H', default_value = "8080")]
    http_port: u16,
}
