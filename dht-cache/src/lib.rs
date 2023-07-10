//! Simple DHT/messaging system based on libp2p
//!
pub mod domocache;
mod domolibp2p;
mod domopersistentstorage;
pub mod utils;

pub use libp2p::identity::Keypair;
