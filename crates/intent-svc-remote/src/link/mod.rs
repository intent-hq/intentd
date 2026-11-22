//! The link halves of the remote feature: state and code the machine's
//! connector holds, which uses no `Services`. They move into
//! `intent-node-connections` with the connector; the domain halves beside
//! them in this crate find homes and machines and call in here.

pub(crate) mod home_scripts;
pub(crate) mod home_state;
pub(crate) mod terminals;
