//! Configuration setup
//!
use clap::{Arg, CommandFactory, FromArgMatches, Parser, ValueHint};
use serde::{Deserialize, Serialize};
use std::{marker::PhantomData, path::Path, path::PathBuf};

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
    pub url: String,

    /// Name of the table that will be used to store the dht messages
    #[arg(long, short, default_value = "domo_cache")]
    pub table: String,

    /// Indicates if the broker should persist the DHT messages into the DB
    /// or if the DB will only be used to populate the DHT cache when the broker starts
    #[arg(long, short, default_value_t = true)]
    pub persistent: bool,

    /// Path to the private key file of the broker. If the path does not exist a key file
    /// will be automatically generated
    #[arg(long, short = 'k', value_hint = clap::ValueHint::FilePath)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub private_key: Option<PathBuf>,

    /// 32 bytes long shared key in hex format
    /// used to protect access to the DHT
    #[arg(long, short = 's')]
    pub shared_key: String,

    /// Use only loopback iface for libp2p
    #[arg(long)]
    pub loopback: bool,
}

impl Default for Cache {
    fn default() -> Self {
        Cache {
            url: "sqlite::memory:".to_string(),
            table: "domo_cache".to_string(),
            persistent: true,
            private_key: None,
            shared_key: "".to_string(),
            loopback: false,
        }
    }
}

#[derive(Parser, Debug, Deserialize, Serialize)]
pub struct Broker {
    #[clap(flatten)]
    pub cache: Cache,

    /// HTTP port used by the broker
    #[arg(long, short = 'H', default_value = "8080")]
    pub http_port: u16,
}

/// Create a configuration parser and a cli for the provided
/// struct T
pub struct ConfigBuilder<T> {
    default_path: PathBuf,
    _t: PhantomData<T>,
}

impl<T> ConfigBuilder<T>
where
    T: Parser + std::fmt::Debug + Serialize,
    for<'a> T: Deserialize<'a>,
{
    pub fn with_config_path<P: AsRef<Path>>(path: P) -> Self {
        Self {
            default_path: path.as_ref().to_owned(),
            _t: PhantomData,
        }
    }
    pub fn parse(&self) -> T {
        let cmd = <T as CommandFactory>::command();
        let mut matches = cmd
            .arg(
                Arg::new("config")
                    .short('c')
                    .long("config")
                    .value_parser(clap::value_parser!(PathBuf))
                    .default_value(self.default_path.as_os_str().to_os_string())
                    .value_hint(ValueHint::FilePath)
                    .value_name("FILE"),
            )
            .get_matches();
        // SAFETY: config has a default value
        let config = matches.remove_one::<PathBuf>("config").unwrap();
        dbg!(config);
        <T as FromArgMatches>::from_arg_matches(&matches).expect("Internal error parsing matches")
    }
}
