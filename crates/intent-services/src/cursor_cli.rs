//! Shared Cursor CLI resolution + identity probe.
//!
//! One path for `host.providerDiscovery`'s cursor row and the spawn-time
//! fail-fast in `agent_manager`, so both judge the binary the spawned child
//! would actually exec. Path-level Grok agent host rejection happens in
//! discovery ([`intent_providers::cursor`]); this probe adds the
//! `--version` check for a Grok host that is not recognisable by path.

use std::path::PathBuf;

use intent_providers::cursor::{
    classify_cursor_version, grok_agent_host_reason, CursorCliIdentity,
};

/// One resolved-and-probed `cursor-agent` snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CursorCliStatus {
    /// Where `cursor-agent` resolved (override, PATH, or versioned install).
    pub resolved_path: Option<PathBuf>,
    /// The `--version` identity, when the probe produced output.
    pub identity: Option<CursorCliIdentity>,
}

impl CursorCliStatus {
    /// The version string to report: the Cursor date-hash version, or the
    /// raw line for an unrecognized or Grok host answer.
    #[must_use]
    pub fn version(&self) -> Option<&str> {
        match self.identity.as_ref()? {
            CursorCliIdentity::Cursor(v)
            | CursorCliIdentity::GrokAgentHost(v)
            | CursorCliIdentity::Unrecognized(v) => Some(v),
        }
    }

    /// The reason the resolved binary is refused: only the Grok agent host.
    /// A failed or unrecognized probe stays permissive (a changed version
    /// format must not take the provider away).
    #[must_use]
    pub fn unavailable_reason(&self) -> Option<String> {
        match self.identity.as_ref()? {
            CursorCliIdentity::GrokAgentHost(line) => Some(grok_agent_host_reason(line)),
            CursorCliIdentity::Cursor(_) | CursorCliIdentity::Unrecognized(_) => None,
        }
    }
}

/// Resolve `cursor-agent` (honouring `providers.paths.cursor`) and run
/// `--version` with a 3s budget. Blocking — call from a blocking context.
#[must_use]
pub fn probe_cursor_cli(override_path: Option<&str>) -> CursorCliStatus {
    probe_cursor_cli_at(intent_providers::find_provider_binary(
        "cursor",
        intent_providers::cursor::CURSOR_COMMAND,
        override_path,
    ))
}

/// [`probe_cursor_cli`] on an already resolved launch binary (the spawn
/// path's `provider_binary`). Blocking.
#[must_use]
pub fn probe_cursor_cli_at(resolved_path: Option<PathBuf>) -> CursorCliStatus {
    let identity = resolved_path
        .as_deref()
        .and_then(crate::pi_cli::run_version_probe)
        .map(|line| classify_cursor_version(&line));
    if let Some(CursorCliIdentity::Unrecognized(line)) = &identity {
        tracing::warn!(
            version_output = %line,
            "cursor-agent --version output is not a Cursor version; not gating the cursor provider"
        );
    }
    CursorCliStatus {
        resolved_path,
        identity,
    }
}

/// Spawn-time fail-fast: refuse to launch the Grok agent host as Cursor.
pub(crate) fn check_cursor_cli_for_spawn(status: &CursorCliStatus) -> crate::Result<()> {
    match status.unavailable_reason() {
        // InvalidInput so the message survives the JSON-RPC envelope.
        Some(reason) => Err(crate::Error::InvalidInput(format!(
            "cannot start Cursor agent: {reason}"
        ))),
        None => Ok(()),
    }
}

/// Fold a probed status into the discovery row: `cliCommand`,
/// `cliResolved`, optional `cliResolvedPath` / `cliVersion`, and — only for
/// the Grok agent host — `installed: false` with `unavailableReason`.
pub(crate) fn apply_cursor_cli_fields(
    obj: &mut serde_json::Map<String, serde_json::Value>,
    installed: &mut bool,
    status: &CursorCliStatus,
) {
    obj.insert(
        "cliCommand".into(),
        serde_json::json!(intent_providers::cursor::CURSOR_COMMAND),
    );
    obj.insert(
        "cliResolved".into(),
        serde_json::json!(status.resolved_path.is_some()),
    );
    if let Some(path) = &status.resolved_path {
        obj.insert(
            "cliResolvedPath".into(),
            serde_json::json!(path.display().to_string()),
        );
    }
    if let Some(version) = status.version() {
        obj.insert("cliVersion".into(), serde_json::json!(version));
    }
    if let Some(reason) = status.unavailable_reason() {
        *installed = false;
        obj.insert("unavailableReason".into(), serde_json::json!(reason));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn status(identity: Option<CursorCliIdentity>) -> CursorCliStatus {
        CursorCliStatus {
            resolved_path: Some(PathBuf::from("/home/u/.local/bin/cursor-agent")),
            identity,
        }
    }

    #[test]
    fn cursor_version_is_reported_and_allowed() {
        let s = status(Some(CursorCliIdentity::Cursor("2026.11.05-86047df".into())));
        let mut obj = serde_json::Map::new();
        let mut installed = true;
        apply_cursor_cli_fields(&mut obj, &mut installed, &s);
        assert!(installed);
        assert_eq!(obj["cliCommand"], "cursor-agent");
        assert_eq!(obj["cliResolved"], true);
        assert_eq!(obj["cliResolvedPath"], "/home/u/.local/bin/cursor-agent");
        assert_eq!(obj["cliVersion"], "2026.11.05-86047df");
        assert!(!obj.contains_key("unavailableReason"));
        assert!(check_cursor_cli_for_spawn(&s).is_ok());
    }

    #[test]
    fn grok_agent_host_is_unavailable_and_refused_at_spawn() {
        let s = status(Some(CursorCliIdentity::GrokAgentHost(
            "grok 0.3.1 (abc) [unified]".into(),
        )));
        let mut obj = serde_json::Map::new();
        let mut installed = true;
        apply_cursor_cli_fields(&mut obj, &mut installed, &s);
        assert!(!installed);
        let reason = obj["unavailableReason"].as_str().unwrap();
        assert!(reason.contains("Grok agent host"), "{reason}");
        let err = check_cursor_cli_for_spawn(&s).unwrap_err();
        assert!(matches!(err, crate::Error::InvalidInput(_)));
        assert!(err.to_string().contains("cannot start Cursor agent"));
    }

    #[test]
    fn failed_or_unrecognized_probe_is_permissive() {
        for identity in [None, Some(CursorCliIdentity::Unrecognized("v9".into()))] {
            let s = status(identity);
            let mut obj = serde_json::Map::new();
            let mut installed = true;
            apply_cursor_cli_fields(&mut obj, &mut installed, &s);
            assert!(installed);
            assert!(!obj.contains_key("unavailableReason"));
            assert!(check_cursor_cli_for_spawn(&s).is_ok());
        }
    }

    /// A `providers.paths.cursor` override that resolves to the Grok agent
    /// host makes the discovery row unavailable with an actionable reason,
    /// whatever auto-discovery finds on this host, and the spawn/auth
    /// resolver refuses it instead of falling back.
    #[cfg(unix)]
    #[test]
    fn grok_host_override_is_unavailable_in_discovery() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let host = dir.path().join("agent-host-0.3.1-linux-x64");
        std::fs::write(&host, "#!/bin/sh\necho 'grok 0.3.1'\n").unwrap();
        std::fs::set_permissions(&host, std::fs::Permissions::from_mode(0o755)).unwrap();
        let paths =
            std::collections::HashMap::from([("cursor".to_string(), host.display().to_string())]);
        let discovery = crate::discover_providers_with_npx_overrides(&paths);
        let row = discovery["providers"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["id"] == "cursor")
            .unwrap();
        assert_eq!(row["installed"], false, "{row}");
        let reason = row["unavailableReason"].as_str().unwrap();
        assert!(reason.contains("Grok agent host"), "{reason}");
        assert!(reason.contains("providers.paths.cursor"), "{reason}");
        assert_eq!(
            intent_providers::find_provider_binary("cursor", "cursor-agent", host.to_str()),
            None
        );
    }

    /// End to end over a fake `cursor-agent` on an explicit override path:
    /// resolve → `--version` → classify, without touching PATH or HOME.
    #[cfg(unix)]
    #[test]
    fn probe_classifies_a_fake_grok_host_binary() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let fake = dir.path().join("cursor-agent");
        std::fs::write(&fake, "#!/bin/sh\necho 'grok 0.3.1 (abc) [unified]'\n").unwrap();
        std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
        let s = probe_cursor_cli(fake.to_str());
        assert_eq!(s.resolved_path.as_deref(), Some(fake.as_path()));
        assert!(matches!(
            s.identity,
            Some(CursorCliIdentity::GrokAgentHost(_))
        ));
        std::fs::write(&fake, "#!/bin/sh\necho 2026.11.05-86047df\n").unwrap();
        let s = probe_cursor_cli(fake.to_str());
        assert_eq!(s.version(), Some("2026.11.05-86047df"));
        assert!(s.unavailable_reason().is_none());
    }
}
