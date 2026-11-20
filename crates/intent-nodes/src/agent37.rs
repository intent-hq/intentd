//! The Agent37 adapter of [`crate::cloud::CloudProvider`].
//!
//! A machine is one Agent37 instance, started from a workspace template
//! that holds the published Intent node image. Pause is an Agent37 sleep,
//! which saves memory and processes; a wake usually restores them but can
//! boot fresh from the disk. Commands run over Agent37's exec API.
//!
//! GUARDRAIL: the API key is a [`SecretString`]. It goes in the
//! `Authorization` header of REST calls and the `X-Agent37-Key` header of
//! exec calls, never in a URL, a body, a log, an error or a `Debug` output.

mod client;
mod provider;

pub use crate::cloud::SecretString;
pub use client::{Agent37Client, Agent37Config, DEFAULT_API_URL, DEFAULT_EXEC_URL};

/// The provider id of an Agent37 entry (`CloudMachine.provider`).
pub const CLOUD_PROVIDER_AGENT37: &str = "agent37";

/// The image repository every Agent37 template is imported from.
pub const IMAGE_REPOSITORY: &str = "ghcr.io/intent-hq/intent-node";

/// The tag key the backend's id is stored under in an instance's metadata.
pub const BACKEND_ID_TAG: &str = "intentBackendId";

/// Seconds of no exec or URL traffic before Agent37 sleeps an instance on
/// its own: a backstop for a backend that went away, longer than the
/// backend's renewal interval.
pub const BACKSTOP_IDLE_SECS: u32 = 30 * 60;

/// The instance types an entry can pick.
pub const INSTANCE_TYPES: [&str; 2] = ["default", "vm"];

/// The regions an entry can pick; only `us` runs the `default` type.
pub const REGIONS: [&str; 3] = ["us", "eu", "ap-south"];

/// Whether Agent37 runs instance type `instance_type` in `region`.
#[must_use]
pub fn region_allows(instance_type: &str, region: &str) -> bool {
    INSTANCE_TYPES.contains(&instance_type)
        && REGIONS.contains(&region)
        && (region == "us" || instance_type == "vm")
}

/// The image of intentd version `version`.
#[must_use]
pub fn image_ref(version: &str) -> String {
    format!("{IMAGE_REPOSITORY}:{version}")
}

/// The Agent37 template that holds `image`: `intent-node-` and the image's
/// tag, each character other than a lowercase letter, digit or hyphen
/// written as `-` (Agent37 names match `^[a-z][a-z0-9-]{1,62}$`).
#[must_use]
pub fn template_name(image: &str) -> String {
    let tag = image.rsplit_once(':').map_or(image, |(_, tag)| tag);
    let mut name = String::from("intent-node-");
    name.extend(tag.chars().map(|c| {
        let c = c.to_ascii_lowercase();
        if c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' {
            c
        } else {
            '-'
        }
    }));
    name.truncate(63);
    name
}

#[cfg(test)]
mod capture;
#[cfg(test)]
mod contract_tests;
#[cfg(any(test, feature = "agent37-fake"))]
pub mod fake_api;
#[cfg(any(test, feature = "agent37-fake"))]
pub mod fake_server;
#[cfg(test)]
pub(crate) mod fields;
#[cfg(test)]
mod live_tests;
#[cfg(test)]
mod tests;
