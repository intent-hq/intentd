//! The Vercel Sandbox adapter of [`crate::cloud::CloudProvider`].
//!
//! A machine is one persistent Vercel sandbox in the entry's project,
//! started from the Intent node image in a Vercel Container Registry
//! repository. Pause is a stop, which saves the disk as a snapshot; a
//! resume starts a new session from it, without memory or processes. A
//! session has a timeout, which the backend keeps ahead of now, and a
//! plan cap, after which the backend stops and resumes the sandbox.
//! Commands run through Vercel's command API, as the image's user.
//!
//! GUARDRAIL: the access token is a [`SecretString`]. It goes in the
//! `Authorization` header (and as the registry's basic password), never in
//! a URL, a body, a log, an error or a `Debug` output.

mod client;
mod provider;

pub use crate::cloud::SecretString;
pub use client::{VercelClient, VercelConfig, DEFAULT_API_URL, DEFAULT_REGISTRY_URL};

/// The provider id of a Vercel entry (`CloudMachine.provider`).
pub const CLOUD_PROVIDER_VERCEL: &str = "vercel";

/// The public repository Intent publishes the node image to.
pub const DEFAULT_IMAGE: &str = "sutterhill/intent-images/intent-node";

/// The tag key the backend's id is stored under.
pub const BACKEND_ID_TAG: &str = "intentBackendId";

/// Most tags Vercel keeps on a sandbox.
pub const MAX_TAGS: usize = 5;

/// The vCPU counts Vercel runs (2 GiB of memory each) that entries offer.
pub const VCPUS: [u32; 3] = [2, 4, 8];

/// Snapshots older than this are deleted by Vercel: longer than the
/// longest `pausedDeleteDays`, so the backend deletes a paused sandbox
/// first, and a sandbox whose backend went away is not kept for ever.
pub const SNAPSHOT_EXPIRATION_MS: u64 = 31 * 24 * 60 * 60 * 1000;

/// The image the entry `image` (a repository without tag) starts for
/// intentd version `version`.
#[must_use]
pub fn image_ref(image: &str, version: &str) -> String {
    format!("{image}:{version}")
}

/// A Vercel team id: `team_` and then 1-64 letters or digits.
#[must_use]
pub fn valid_team_id(id: &str) -> bool {
    prefixed_id(id, "team_")
}

/// A Vercel project id: `prj_` and then 1-64 letters or digits.
#[must_use]
pub fn valid_project_id(id: &str) -> bool {
    prefixed_id(id, "prj_")
}

fn prefixed_id(id: &str, prefix: &str) -> bool {
    id.strip_prefix(prefix).is_some_and(|rest| {
        (1..=64).contains(&rest.len()) && rest.bytes().all(|b| b.is_ascii_alphanumeric())
    })
}

/// A Vercel Container Registry repository without tag or digest:
/// `<team slug>/<project>/<repository>`, or `<repository>` in the
/// entry's own project. Each part is lowercase letters, digits, `.`, `_`
/// and `-`.
#[must_use]
pub fn valid_image(image: &str) -> bool {
    let parts: Vec<&str> = image.split('/').collect();
    image.len() <= 256
        && (parts.len() == 1 || parts.len() == 3)
        && parts.iter().all(|part| {
            !part.is_empty()
                && part.bytes().all(|b| {
                    b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'.' | b'_' | b'-')
                })
        })
}

#[cfg(test)]
mod capture;
#[cfg(test)]
mod contract_tests;
#[cfg(any(test, feature = "vercel-fake"))]
pub mod fake_api;
#[cfg(any(test, feature = "vercel-fake"))]
pub mod fake_server;
#[cfg(test)]
mod live_tests;
#[cfg(test)]
mod tests;
