use clap::Parser;
use serde::{Deserialize, Serialize};
use sifis_config::{Broker, ConfigBuilder};

#[derive(Parser, Debug, Deserialize, Serialize)]
struct Cli {
    #[clap(flatten)]
    broker: Broker,
}

fn main() {
    let cli = ConfigBuilder::<Cli>::with_config_path("/etc/config").parse();

    let s = toml::to_string_pretty(&cli).unwrap();

    println!("{}", s);
}
