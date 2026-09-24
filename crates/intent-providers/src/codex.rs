//! Runtime selection for the managed Codex ACP adapter.

use std::path::{Path, PathBuf};

/// Resolve on each launch so a host installation upgraded in place is picked up.
#[must_use]
pub fn host_codex_path() -> Option<PathBuf> {
    crate::find_provider_binary("codex", "codex", None)
}

/// Replace inherited overrides with the selected host runtime, or the adapter's
/// bundled fallback when Codex is absent. Never trust inherited `CODEX_PATH`.
pub fn configure_runtime(command: &mut std::process::Command, host: Option<&Path>) {
    command.env_remove("CODEX_CONFIG");
    if let Some(path) = host {
        command.env("CODEX_PATH", path);
    } else {
        command.env_remove("CODEX_PATH");
    }
}

/// File identity avoids spawning a CLI from a cached model-catalog read.
pub fn runtime_cache_key(host: Option<&Path>) -> String {
    let Some(path) = host else {
        return "bundled".into();
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
    fn runtime_overrides_inherited_values_and_supports_bundled_fallback() {
        let mut command = std::process::Command::new("adapter");
        for host in [Some(Path::new("/host with spaces/codex")), None] {
            command.env("CODEX_PATH", "/stale/codex");
            command.env("CODEX_CONFIG", "untrusted");
            configure_runtime(&mut command, host);
            let env = command
                .get_envs()
                .collect::<std::collections::BTreeMap<_, _>>();
            assert_eq!(
                env[std::ffi::OsStr::new("CODEX_PATH")],
                host.map(Path::as_os_str)
            );
            assert_eq!(env[std::ffi::OsStr::new("CODEX_CONFIG")], None);
        }
    }

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
