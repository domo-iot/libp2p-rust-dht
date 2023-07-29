//! Configuration setup
//!
use clap::{Arg, CommandFactory, FromArgMatches, Parser, ValueHint};
use figment::providers::{Env, Format, Toml};
use figment::value::Num;
use figment::{value::Value, Figment};
use serde::{Deserialize, Serialize};
use std::ffi::OsString;
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
    #[arg(long, short = 'H', default_value = "3000")]
    pub http_port: u16,
}

/// Create a configuration parser and a cli for the provided
/// struct T
#[derive(Default)]
pub struct ConfigParser<T> {
    default_path: Option<PathBuf>,
    prefix: Option<String>,
    _t: PhantomData<T>,
}

impl<T> ConfigParser<T>
where
    T: Parser + std::fmt::Debug + Serialize,
    for<'a> T: Deserialize<'a>,
{
    /// Create a new default parser
    pub fn new() -> Self {
        Self {
            default_path: None,
            prefix: None,
            _t: PhantomData,
        }
    }
    /// Set a custom config file path
    ///
    /// By default uses the result of [clap::Command::get_name] with the extension `.toml`.
    pub fn with_config_path<P: AsRef<Path>>(mut self, path: P) -> Self {
        self.default_path = Some(path.as_ref().to_owned());
        self
    }

    /// Set a custom prefix for the env variable lookup
    ///
    /// By default uses the result of [clap::Command::get_name] with `_`.
    pub fn with_env_prefix<S: ToString>(mut self, prefix: S) -> Self {
        self.prefix = Some(prefix.to_string());
        self
    }

    /// Parse the configuration
    ///
    /// It looks into the environment, a configuration file and the command line arguments.
    pub fn parse(&self) -> T {
        let cmd = <T as CommandFactory>::command_for_update();
        let name = cmd.get_name();

        let default_path = self
            .default_path
            .as_ref()
            .map(|p| p.as_os_str().to_os_string())
            .unwrap_or_else(|| OsString::from(format!("{name}.toml")));

        let prefix = self.prefix.as_ref().cloned().unwrap_or_else(|| {
            let mut p = name.to_uppercase().replace('-', "_");
            p.push('_');
            p
        });
        let mut matches = cmd
            .arg(
                Arg::new("config")
                    .short('c')
                    .long("config")
                    .value_parser(clap::value_parser!(PathBuf))
                    .default_value(default_path)
                    .value_hint(ValueHint::FilePath)
                    .value_name("FILE"),
            )
            .get_matches();

        // SAFETY: config has a default value
        let config = matches.remove_one::<PathBuf>("config").unwrap();

        let values = Figment::new()
            .merge(Env::prefixed(&prefix))
            .merge(Toml::file(config))
            .extract::<Value>()
            .unwrap();

        fn find_deep<'a>(value: &'a Value, key: &str) -> Option<&'a Value> {
            match value {
                Value::Dict(_, d) => d
                    .get(key)
                    .or_else(|| d.values().find_map(|v| find_deep(v, key))),
                _ => None,
            }
        }

        let mut cmd = <T as CommandFactory>::command();

        let keys = cmd
            .get_arguments()
            .map(|arg| arg.get_id().to_owned())
            .collect::<Vec<_>>();

        for (key, value) in keys
            .into_iter()
            .filter_map(|key| find_deep(&values, key.as_str()).map(|v| (key, v)))
        {
            let val = match value {
                Value::String(_, s) => s.to_owned(),
                Value::Bool(_, b) => b.to_string(),
                Value::Char(_, c) => c.to_string(),
                Value::Num(_, n) => match n {
                    Num::F32(f) => f.to_string(),
                    Num::F64(d) => d.to_string(),
                    Num::U8(n) => n.to_string(),
                    Num::I8(n) => n.to_string(),
                    Num::U16(n) => n.to_string(),
                    Num::I16(n) => n.to_string(),
                    Num::U32(n) => n.to_string(),
                    Num::I32(n) => n.to_string(),
                    Num::U64(n) => n.to_string(),
                    Num::I64(n) => n.to_string(),
                    Num::U128(n) => n.to_string(),
                    Num::I128(n) => n.to_string(),
                    Num::USize(n) => n.to_string(),
                    Num::ISize(n) => n.to_string(),
                },
                _ => {
                    todo!("More value types")
                }
            };
            cmd = cmd.mut_arg(key, |a| a.default_value(val).required(false))
        }

        let matches = cmd
            .arg(Arg::new("config").short('c').long("config"))
            .get_matches();

        <T as FromArgMatches>::from_arg_matches(&matches).expect("Internal error parsing matches")
    }
}
