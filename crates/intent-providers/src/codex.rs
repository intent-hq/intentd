//! Vendored Codex ACP adapter assets and host runtime cache identity.

use std::path::{Path, PathBuf};

/// Resolve on each launch so a host installation upgraded in place is picked up.
#[must_use]
pub fn host_codex_path() -> Option<PathBuf> {
    crate::find_provider_binary("codex", "codex", None)
}

/// Only an explicit override can replace the shipped adapter.
#[must_use]
pub fn adapter_override(explicit: Option<&str>) -> Option<PathBuf> {
    explicit.and_then(|path| crate::discover::resolve_explicit_path("codex", path))
}

/// Content identity of the shipped adapter, including local patches.
pub const ADAPTER_VERSION: &str = include_str!("../vendor/codex-acp/dist/version");

/// Materialize the self-contained adapter in a caller-owned private launch directory.
///
/// # Errors
/// Returns the filesystem error if an embedded asset cannot be written.
pub fn write_adapter(directory: &Path) -> std::io::Result<PathBuf> {
    for (name, content) in [
        (
            "codex-acp.mjs",
            include_str!("../vendor/codex-acp/dist/codex-acp.mjs"),
        ),
        ("LICENSE", include_str!("../vendor/codex-acp/LICENSE")),
        (
            "THIRD_PARTY_LICENSES",
            include_str!("../vendor/codex-acp/dist/THIRD_PARTY_LICENSES"),
        ),
    ] {
        std::fs::write(directory.join(name), content)?;
    }
    Ok(directory.join("codex-acp.mjs"))
}

/// File identity avoids spawning a CLI from a cached model-catalog read.
pub fn runtime_cache_key(host: Option<&Path>) -> String {
    let Some(path) = host else {
        return "missing-host-codex".into();
    };
    let target = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
    let metadata = std::fs::metadata(&target).ok();
    format!(
        "host:{}:{:?}:{:?}",
        target.display(),
        metadata.as_ref().map(std::fs::Metadata::len),
        metadata.and_then(|m| m.modified().ok())
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_identity_changes_when_host_runtime_is_replaced_or_removed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("codex");
        std::fs::write(&path, "old").unwrap();
        let old = runtime_cache_key(Some(&path));
        std::fs::write(&path, "new runtime").unwrap();
        assert_ne!(old, runtime_cache_key(Some(&path)));
        assert_ne!(old, runtime_cache_key(None));
    }
}
