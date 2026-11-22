//! The backend-to-machine node link, which lives in `intent-node-link` (below
//! `intent-services`, so the node-connection service can use it); re-exported
//! here so `intent_transport::node_link` paths keep working. Its tests stay
//! here: they serve the executor side on this crate's WSS server.

pub use intent_node_link::*;

#[cfg(test)]
mod dial {
    pub(super) use intent_node_link::test_support::{
        decode_rejection, dial_link_with, read_welcome,
    };
}

#[cfg(test)]
mod link {
    pub(super) use intent_node_link::test_support::Tuning;
}

#[cfg(test)]
use intent_node_link::test_support::pin;

#[cfg(test)]
mod tests;
