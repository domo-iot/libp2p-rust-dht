//! Simple DHT/messaging system based on libp2p
//!
pub mod cache;
pub mod data;
pub mod dht;
pub mod domocache;
mod domolibp2p;
mod domopersistentstorage;
pub mod utils;

pub use libp2p::identity::Keypair;

#[doc(inline)]
pub use domocache::DomoCache as Cache;

/// Cache configuration
pub use sifis_config::Cache as Config;

/// Error type
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("I/O error")]
    Io(#[from] std::io::Error),
    #[error("JSON serialization-deserialization error")]
    Json(#[from] serde_json::Error),
    #[error("Cannot decode the identity key")]
    Identity(#[from] libp2p::identity::DecodingError),
    #[error("Cannot decode the PEM information")]
    Pem(#[from] pem_rfc7468::Error),
    #[error("Cannot parse the hex string: {0}")]
    Hex(String),
    #[error("Connection setup error")]
    Transport(#[from] libp2p::TransportError<std::io::Error>),
    #[error("Cannot parse the multiaddr")]
    MultiAddr(#[from] libp2p::multiaddr::Error),
    #[error("Internal channel dropped")]
    Channel,
    #[error("Missing configuration")]
    MissingConfig,
    #[error("Invalid JSONPath expression")]
    Jsonpath,
}
