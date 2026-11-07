//! The intent-node template manifest (`e2b-templates.json`) each intentd
//! release publishes next to its archives: which template sizes were built
//! for the version, in which E2B project. It is public and read without a
//! key. E2B itself cannot answer this for a user's key: its template lookup
//! only covers templates of the key's own project.

use std::time::Duration;

use serde::Deserialize;

use super::E2bError;

/// The manifest's file name on the release.
pub const MANIFEST_ASSET: &str = "e2b-templates.json";

/// Where releases are published: `<base>/v<version>/<MANIFEST_ASSET>`.
pub const DEFAULT_MANIFEST_BASE_URL: &str =
    "https://github.com/intent-hq/intentd-releases/releases/download";

/// Longest manifest kept, in bytes.
const MAX_MANIFEST_BYTES: usize = 64 * 1024;

/// One release's template manifest.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct TemplateManifest {
    /// The E2B project that owns the templates (`intent`).
    pub project: String,
    /// The intentd version the templates are tagged with.
    pub version: String,
    /// The sizes built for this version (`"2x4"`, `"4x8"`, ...).
    pub sizes: Vec<String>,
}

/// The manifest URL of `version` under `base_url`.
#[must_use]
pub fn manifest_url(base_url: &str, version: &str) -> String {
    format!(
        "{}/v{version}/{MANIFEST_ASSET}",
        base_url.trim_end_matches('/')
    )
}

/// Fetch the template manifest of `version` from `base_url`, following
/// redirects, within `timeout`.
///
/// # Errors
///
/// [`E2bError::NotFound`] when the release has no manifest,
/// [`E2bError::Unreachable`] / [`E2bError::Transport`] when it cannot be
/// fetched, [`E2bError::Protocol`] for a malformed or oversized one.
pub async fn fetch_template_manifest(
    base_url: &str,
    version: &str,
    timeout: Duration,
) -> Result<TemplateManifest, E2bError> {
    let client = reqwest::Client::builder()
        .timeout(timeout)
        .build()
        .map_err(|e| E2bError::Transport(e.to_string()))?;
    let response = client
        .get(manifest_url(base_url, version))
        .send()
        .await
        .map_err(|e| {
            if e.is_connect() {
                E2bError::Unreachable(e.without_url().to_string())
            } else {
                E2bError::Transport(e.without_url().to_string())
            }
        })?;
    let status = response.status();
    if status == reqwest::StatusCode::NOT_FOUND {
        return Err(E2bError::NotFound(
            "the release has no template manifest".into(),
        ));
    }
    if !status.is_success() {
        return Err(E2bError::Api {
            status: status.as_u16(),
            error_code: None,
            message: "the template manifest could not be read".into(),
            should_retry: None,
        });
    }
    let bytes = response
        .bytes()
        .await
        .map_err(|e| E2bError::Transport(e.without_url().to_string()))?;
    if bytes.len() > MAX_MANIFEST_BYTES {
        return Err(E2bError::Protocol(
            "the template manifest is too large".into(),
        ));
    }
    serde_json::from_slice(&bytes)
        .map_err(|_| E2bError::Protocol("the template manifest is malformed".into()))
}
