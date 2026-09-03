//! intentd-sitter — self-updating supervisor shim for the intentd daemon.
//!
//! The sitter is the binary users install (packaged and renamed to `intentd`
//! at release time). It downloads the real daemon from the per-channel
//! release manifests, keeps it updated, and forwards all CLI args to it
//! verbatim. This crate holds: CLI parsing ([`cli`]), data-dir/state layout
//! ([`paths`]), the channel config and resolution ([`config`]), persisted
//! sitter state ([`state`]), the channel-manifest schema ([`manifest`]),
//! the update engine ([`updater`]), the post-restart readiness wait
//! ([`readiness`]), and the daemon supervisor loop ([`supervisor`]).

pub mod cli;
pub mod config;
#[cfg(unix)]
pub mod exact_update;
pub mod manifest;
pub mod paths;
pub mod readiness;
pub mod state;
pub mod supervisor;
pub mod updater;
