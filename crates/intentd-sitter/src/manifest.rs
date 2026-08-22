//! Channel manifest (`stable.json` / `beta.json` / `alpha.json`) schema and
//! URLs.
//!
//! Manifests are produced by `scripts/make-channel-manifest.sh` (schema v1)
//! and published as assets on the fixed `channel-stable` / `channel-beta` /
//! `channel-alpha` GitHub releases. Parsing is lenient about extra fields
//! (forward compat);
//! an unknown `schema` is reported as [`ManifestError::UnsupportedSchema`] —
//! a soft check failure, never a panic.

use std::collections::BTreeMap;

use serde::Deserialize;

use crate::cli::Channel;

/// Channel manifest schema version this sitter understands.
pub const MANIFEST_SCHEMA_VERSION: u64 = 1;

/// Ordered base URLs the fixed channel-release manifests are downloaded
/// from (`<base>/channel-<channel>/<channel>.json`): the public
/// `intentd-releases` repo first, then the original `intentd` repo as a
/// fallback. The updater tries each base in order for the manifest fetch
/// (archive URLs come from inside the manifest, so downloads need no
/// fallback). Overridable via [`crate::updater::Updater::with_base_url`] —
/// exactly one base, no fallback — so tests can point at a local fixture
/// server.
pub const DEFAULT_MANIFEST_BASE_URLS: &[&str] = &[
    "https://github.com/intent-hq/intentd-releases/releases/download",
    "https://github.com/intent-hq/intentd/releases/download",
];

/// Compile-time Rust target triple of this sitter build — the key into the
/// manifest's `platforms` map (set by `build.rs`).
pub const TARGET_TRIPLE: &str = env!("SITTER_TARGET_TRIPLE");

/// URL of the manifest for `channel` under `base_url`.
#[must_use]
pub fn manifest_url(base_url: &str, channel: Channel) -> String {
    let base = base_url.trim_end_matches('/');
    format!("{base}/channel-{channel}/{channel}.json")
}

/// One platform's archive in a channel manifest.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct PlatformEntry {
    /// Archive file name, e.g. `intentd-aarch64-apple-darwin.tar.xz`.
    pub asset: String,
    /// Absolute download URL for the archive.
    pub url: String,
    /// Hex sha256 digest of the archive.
    pub sha256: String,
}

/// Parsed channel manifest (schema v1).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct ChannelManifest {
    /// Schema version (see [`MANIFEST_SCHEMA_VERSION`]).
    pub schema: u64,
    /// Channel the manifest was published for
    /// (`stable` | `beta` | `alpha`).
    #[serde(default)]
    pub channel: Option<String>,
    /// Daemon version the manifest points at (no leading `v`).
    pub version: String,
    /// Release tag the archives live under.
    #[serde(default)]
    pub tag: Option<String>,
    /// When the release was published (RFC 3339).
    #[serde(default)]
    pub published_at: Option<String>,
    /// Per-target-triple archives.
    pub platforms: BTreeMap<String, PlatformEntry>,
}

/// Errors from parsing a channel manifest. All are soft check failures.
#[derive(Debug, thiserror::Error)]
pub enum ManifestError {
    #[error("invalid manifest JSON: {0}")]
    Parse(#[from] serde_json::Error),
    #[error("unsupported manifest schema {found} (this sitter understands schema {MANIFEST_SCHEMA_VERSION})")]
    UnsupportedSchema { found: u64 },
}

/// Parse manifest bytes, checking the schema version first so a manifest
/// from a future schema is reported as [`ManifestError::UnsupportedSchema`]
/// even if the rest of the document no longer matches the v1 shape.
///
/// # Errors
///
/// Returns [`ManifestError::UnsupportedSchema`] for a future schema version and [`ManifestError::Parse`] for invalid JSON.
pub fn parse(bytes: &[u8]) -> Result<ChannelManifest, ManifestError> {
    #[derive(Deserialize)]
    struct SchemaOnly {
        schema: u64,
    }
    let schema = serde_json::from_slice::<SchemaOnly>(bytes)?.schema;
    if schema != MANIFEST_SCHEMA_VERSION {
        return Err(ManifestError::UnsupportedSchema { found: schema });
    }
    Ok(serde_json::from_slice(bytes)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_bases_prefer_intentd_releases_then_intentd() {
        assert_eq!(
            DEFAULT_MANIFEST_BASE_URLS,
            [
                "https://github.com/intent-hq/intentd-releases/releases/download",
                "https://github.com/intent-hq/intentd/releases/download",
            ]
        );
    }

    #[test]
    fn manifest_urls_for_all_channels() {
        assert_eq!(
            manifest_url(DEFAULT_MANIFEST_BASE_URLS[0], Channel::Stable),
            "https://github.com/intent-hq/intentd-releases/releases/download/channel-stable/stable.json"
        );
        assert_eq!(
            manifest_url(DEFAULT_MANIFEST_BASE_URLS[0], Channel::Beta),
            "https://github.com/intent-hq/intentd-releases/releases/download/channel-beta/beta.json"
        );
        assert_eq!(
            manifest_url(DEFAULT_MANIFEST_BASE_URLS[0], Channel::Alpha),
            "https://github.com/intent-hq/intentd-releases/releases/download/channel-alpha/alpha.json"
        );
        assert_eq!(
            manifest_url(DEFAULT_MANIFEST_BASE_URLS[1], Channel::Stable),
            "https://github.com/intent-hq/intentd/releases/download/channel-stable/stable.json"
        );
        assert_eq!(
            manifest_url(DEFAULT_MANIFEST_BASE_URLS[1], Channel::Beta),
            "https://github.com/intent-hq/intentd/releases/download/channel-beta/beta.json"
        );
        assert_eq!(
            manifest_url(DEFAULT_MANIFEST_BASE_URLS[1], Channel::Alpha),
            "https://github.com/intent-hq/intentd/releases/download/channel-alpha/alpha.json"
        );
    }

    #[test]
    fn manifest_url_trims_trailing_slash() {
        assert_eq!(
            manifest_url("http://127.0.0.1:1234/", Channel::Stable),
            "http://127.0.0.1:1234/channel-stable/stable.json"
        );
    }

    #[test]
    fn parses_schema_v1_and_ignores_unknown_fields() {
        let json = r#"{
            "schema": 1,
            "channel": "stable",
            "version": "0.1.0",
            "tag": "v0.1.0",
            "published_at": "2026-07-21T00:00:00Z",
            "future_field": true,
            "platforms": {
                "aarch64-apple-darwin": {
                    "asset": "intentd-aarch64-apple-darwin.tar.xz",
                    "url": "https://example.invalid/a.tar.xz",
                    "sha256": "ab"
                }
            }
        }"#;
        let m = parse(json.as_bytes()).unwrap();
        assert_eq!(m.version, "0.1.0");
        assert_eq!(m.tag.as_deref(), Some("v0.1.0"));
        let entry = &m.platforms["aarch64-apple-darwin"];
        assert_eq!(entry.asset, "intentd-aarch64-apple-darwin.tar.xz");
    }

    #[test]
    fn unknown_schema_is_soft_error() {
        let json = r#"{"schema": 2, "totally": "different"}"#;
        match parse(json.as_bytes()) {
            Err(ManifestError::UnsupportedSchema { found: 2 }) => {}
            other => panic!("expected UnsupportedSchema, got {other:?}"),
        }
    }

    #[test]
    fn invalid_json_is_soft_error() {
        assert!(matches!(parse(b"not json"), Err(ManifestError::Parse(_))));
        assert!(matches!(
            parse(br#"{"schema": 1, "platforms": {}}"#),
            Err(ManifestError::Parse(_))
        ));
    }

    #[test]
    fn target_triple_looks_like_a_triple() {
        assert!(TARGET_TRIPLE.contains('-'), "got {TARGET_TRIPLE:?}");
    }
}
