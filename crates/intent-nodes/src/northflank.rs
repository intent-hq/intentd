//! The Northflank adapter of [`crate::cloud::CloudProvider`].
//!
//! A machine is one deployment service in the entry's project, with one
//! volume mounted at `/home/user` for its disk. Pause scales the service to
//! zero and keeps the volume, so a resume keeps the disk but not the memory.
//! Commands run over Northflank's command execution WebSocket.
//!
//! GUARDRAIL: the API token is a [`SecretString`]. It goes in the
//! `Authorization` header of REST calls and in the first frame of a command
//! WebSocket, never in a URL, a log, an error or a `Debug` output.

mod client;
mod exec;
mod provider;
mod registry;

use std::fmt::Write as _;

use sha2::{Digest as _, Sha256};

pub use crate::cloud::SecretString;
pub use client::{
    NorthflankClient, NorthflankConfig, PlanInfo, ProjectInfo, DEFAULT_API_URL, DEFAULT_EXEC_URL,
};
pub use registry::ImageRef;

/// The provider id of a Northflank entry (`CloudMachine.provider`).
pub const CLOUD_PROVIDER_NORTHFLANK: &str = "northflank";

/// The team tag every Intent machine carries.
pub const NODE_TAG: &str = "intent-node";

/// The home folder the machine's volume is mounted at.
pub const HOME: &str = "/home/user";

/// The image's unprivileged account: every command runs as it unless its
/// spec names another user.
pub const RUN_AS: &str = "user";

/// Disk of a new machine when the request does not say.
pub const DEFAULT_DISK_GIB: u32 = 20;

/// The team tag of the machines of backend `backend_id`:
/// `intent-backend-` and the first 12 lowercase hex digits of its SHA-256.
#[must_use]
pub fn backend_tag(backend_id: &str) -> String {
    let digest = Sha256::digest(backend_id.as_bytes());
    let mut out = String::from("intent-backend-");
    for byte in digest.iter().take(6) {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

#[cfg(test)]
mod fake;
#[cfg(test)]
mod live_tests;
#[cfg(test)]
mod tests;
