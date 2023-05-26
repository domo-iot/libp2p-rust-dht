use clap::Parser;
use serde::{Deserialize, Serialize};
use sifis_config::{Broker, ConfigParser};

#[derive(Parser, Debug, Deserialize, Serialize)]
struct Cli {
    #[clap(flatten)]
    broker: Broker,
}

fn main() {
    let cli = ConfigParser::<Cli>::new().parse();

    let s = toml::to_string_pretty(&cli).unwrap();

    println!("{}", s);
}
