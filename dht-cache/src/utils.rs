//! Miscellaneous utilities
use std::time::{SystemTime, UNIX_EPOCH};

/// Compute the dht timestamps
pub fn get_epoch_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
}
