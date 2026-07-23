//! intentd-sitter — self-updating supervisor shim for the intentd daemon.
//!
//! The sitter is the binary users install (packaged and renamed to `intentd`
//! at release time). It downloads the real daemon from the per-channel
//! release manifests, keeps it updated, and forwards all CLI args to it
//! verbatim. This crate currently holds the skeleton: CLI parsing
//! ([`cli`]), data-dir/state layout ([`paths`]), and persisted sitter state
//! ([`state`]). Update and supervision logic land separately.

pub mod cli;
pub mod paths;
pub mod state;
