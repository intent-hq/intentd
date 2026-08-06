//! `agent.completeOnce` — one-shot prompt→completion RPC (PROTOCOL §5.32).
//!
//! Stateless one-shot completion so background FE requests (slug generation,
//! note-status checks — the two remaining `background-request.service.ts`
//! callers) no longer need an ACPProvider or an ephemeral agent session. The
//! daemon owns the full lifecycle: spawn the auggie CLI, collect its cleaned
//! reply, reap the process on any failure path (timeout, cancel, drop). No
//! session/agent state is created, so there is nothing to garbage-collect on
//! error — cleanup is `kill_on_drop` + a unix process-group SIGKILL on
//! timeout, both inherited from the shared `run_auggie_print` helper.

use std::path::PathBuf;

use intent_core::{Error, Result, WorkspaceId};
use serde_json::{json, Value};

use crate::enhance_ops::{
    clean_agent_message, run_auggie_print, DEFAULT_TIMEOUT_MS, MAX_TIMEOUT_MS,
};
use crate::file_ops;
use crate::Services;

impl Services {
    /// `agent.completeOnce` (PROTOCOL §5.32): stateless one-shot completion.
    /// Composes an optional `system_prompt` with `prompt` in the same shape as
    /// `agent.enhancePrompt` (§5.31) and returns the cleaned CLI reply
    /// verbatim under `text`. The router pre-validates `prompt` is non-empty
    /// and `timeout_ms` is positive. Gated on auggie being the effective
    /// provider per spec Decision 5 — the settings-derived default (provider
    /// of `model.default`, else `providers.active`) must be auggie.
    /// Unset/undecidable settings resolve the gate CLOSED (unavailable):
    /// falling through to the first registered provider would always be
    /// auggie and functionally reinstate the removed hardcoded default
    /// (matches FE #759, where unset resolves disabled).
    pub(crate) async fn agent_complete_once_op(
        &self,
        prompt: String,
        system_prompt: Option<String>,
        model: Option<String>,
        workspace_id: Option<WorkspaceId>,
        timeout_ms: Option<u64>,
    ) -> Result<Value> {
        // Provider neutrality gate: completion is an auggie-specific capability.
        // When the effective provider is not auggie — including unset/undecidable
        // settings, which resolve the gate closed rather than falling through to
        // the first registered provider — return a typed unavailable response so
        // callers can degrade gracefully without an error crash.
        let effective_provider =
            crate::agent_session::derived_default_provider(&self.effective_settings());
        if effective_provider.as_deref() != Some("auggie") {
            return Ok(json!({
                "available": false,
                "reason": "completeOnce requires auggie as the effective default provider"
            }));
        }

        // Optional cwd pin: unknown workspace surfaces as -32602 (NotFound);
        // a workspace without a filesystem root just runs without a cwd
        // (mirrors §5.31).
        let cwd: Option<PathBuf> = match &workspace_id {
            Some(id) => {
                let ws = self.store.get_workspace(id).await?;
                let root = file_ops::workspace_root(&ws);
                (!root.is_empty()).then(|| PathBuf::from(root))
            }
            None => None,
        };
        // Binary resolution order (per spec Design): self.auggie_bin (test seam) →
        // context.auggiePath (user setting, EXCLUSIVE when set) → find_auggie().
        let auggie = match &self.auggie_bin {
            Some(bin) => bin.clone(),
            None => {
                // Check context.auggiePath first; when set and non-empty, use it
                // EXCLUSIVELY (fail hard if invalid rather than falling through).
                let settings = self.effective_settings();
                match settings
                    .context
                    .auggie_path
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                {
                    Some(trimmed) => {
                        let p = PathBuf::from(trimmed);
                        if p.is_file() {
                            p
                        } else {
                            return Err(Error::InvalidParams(format!(
                                "configured auggie path is not a valid file: {}",
                                trimmed
                            )));
                        }
                    }
                    None => {
                        // Setting unset/empty → fall through to discovery
                        intent_context::discovery::find_auggie()
                            .ok_or_else(|| Error::Internal("auggie CLI not found".to_string()))?
                    }
                }
            }
        };
        // `System: <system>\n\n<prompt>` mirrors the FE streamChat composition
        // when a system prompt is supplied; otherwise the raw prompt is piped
        // through verbatim.
        let full_prompt = match system_prompt.as_deref().map(str::trim) {
            Some(s) if !s.is_empty() => format!("System: {s}\n\n{prompt}"),
            _ => prompt.clone(),
        };
        let timeout = timeout_ms.unwrap_or(DEFAULT_TIMEOUT_MS).min(MAX_TIMEOUT_MS);
        let stdout = run_auggie_print(
            &auggie,
            model.as_deref(),
            cwd.as_deref(),
            &full_prompt,
            timeout,
        )
        .await?;
        let text = clean_agent_message(&stdout);
        Ok(json!({ "text": text }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use intent_store::Store;

    /// RAII temp SQLite store: the db (and its `-wal`/`-shm` sidecars) live in
    /// a guarded temp dir removed on drop — including on panic — unless
    /// `INTENTD_TEST_KEEP_TMP` (non-empty) is set.
    struct TempDb {
        _dir: tempfile::TempDir,
        path: PathBuf,
    }

    impl TempDb {
        fn new() -> Self {
            let dir = crate::tests::test_tempdir("intentd-completeops-");
            let path = dir.path().join("store.db");
            Self { _dir: dir, path }
        }
    }

    /// Services with a fake CLI and `providers.active = "auggie"` so the
    /// provider gate is open: unset settings resolve the gate CLOSED
    /// (see `complete_once_unavailable_when_settings_unset`), so op-level
    /// tests must opt in to an auggie-active registry to reach the CLI.
    async fn services_with_bin(bin: PathBuf) -> (TempDb, Services) {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let registry = std::sync::Arc::new(
            crate::SettingsRegistry::load(tmp._dir.path().join("config.toml"))
                .expect("load registry"),
        );
        registry
            .apply(&[("providers.active".to_string(), serde_json::json!("auggie"))])
            .expect("set providers.active");
        let services = Services::new(store)
            .with_auggie_bin(bin)
            .with_settings_registry(registry);
        (tmp, services)
    }

    /// Fake auggie CLI inside an RAII temp dir; keep the returned guard alive
    /// for the duration of the test (dropping it removes the dir).
    #[cfg(unix)]
    fn fake_auggie(tag: &str, body: &str) -> (tempfile::TempDir, PathBuf) {
        use std::os::unix::fs::PermissionsExt;
        let dir = crate::tests::test_tempdir(&format!("intentd-complete-{tag}-"));
        let bin = dir.path().join("auggie");
        std::fs::write(&bin, format!("#!/bin/sh\ncat > /dev/null\n{body}\n")).unwrap();
        std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();
        (dir, bin)
    }

    #[tokio::test]
    async fn complete_once_errors_when_cli_missing() {
        let (_tmp, services) =
            services_with_bin(PathBuf::from("/nonexistent/intentd-test/auggie")).await;
        let err = services
            .agent_complete_once_op("hi".into(), None, None, None, None)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Internal(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn complete_once_unavailable_when_settings_unset() {
        // Unset/undecidable provider settings resolve the gate CLOSED: falling
        // through to the first registered provider would always be auggie and
        // functionally reinstate the removed hardcoded default (coordinator
        // ruling; matches FE #759 where unset resolves disabled). No registry
        // wired → schema defaults → both `model.default` and
        // `providers.active` unset.
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let services =
            Services::new(store).with_auggie_bin(PathBuf::from("/nonexistent/intentd-test/auggie"));
        let v = services
            .agent_complete_once_op("hi".into(), None, None, None, None)
            .await
            .unwrap();
        assert_eq!(
            v,
            serde_json::json!({
                "available": false,
                "reason": "completeOnce requires auggie as the effective default provider"
            }),
            "unset provider settings must close the gate, not fall back to the first registered provider"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn complete_once_returns_cleaned_reply() {
        let (_bin_dir, bin) = fake_auggie("ok", "printf '🤖\\nslug-goes-here\\n'");
        let (_tmp, services) = services_with_bin(bin).await;
        let v = services
            .agent_complete_once_op("make a slug".into(), None, None, None, None)
            .await
            .unwrap();
        assert_eq!(v["text"], "slug-goes-here");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn complete_once_times_out_and_reaps() {
        let (_bin_dir, bin) = fake_auggie("slow", "sleep 30");
        let (_tmp, services) = services_with_bin(bin).await;
        let err = services
            .agent_complete_once_op("hi".into(), None, None, None, Some(200))
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("timed out after 200ms"),
            "got {err:?}"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn complete_once_errors_on_nonzero_exit() {
        let (_bin_dir, bin) = fake_auggie("fail", "exit 3");
        let (_tmp, services) = services_with_bin(bin).await;
        let err = services
            .agent_complete_once_op("hi".into(), None, None, None, None)
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("exited with code 3"),
            "got {err:?}"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn binary_resolution_order() {
        use serde_json::json;
        use std::sync::Arc;

        let (_fake_dir, fake) = fake_auggie("setting", "printf 'from-setting\\n'");
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let config_dir = tempfile::tempdir().expect("temp config dir");
        let registry = Arc::new(
            crate::SettingsRegistry::load(config_dir.path().join("config.toml"))
                .expect("load registry"),
        );
        // Keep the provider gate open (unset settings resolve it closed).
        registry
            .apply(&[("providers.active".to_string(), json!("auggie"))])
            .expect("set providers.active");

        // Case 1: context.auggiePath set and valid → use it exclusively
        registry
            .apply(&[(
                "context.auggiePath".to_string(),
                json!(fake.to_str().unwrap()),
            )])
            .expect("set setting");
        let services = Services::new(store.clone()).with_settings_registry(registry.clone());
        let result = services
            .agent_complete_once_op("test".into(), None, None, None, None)
            .await
            .unwrap();
        assert_eq!(result["text"], "from-setting");

        // Case 2: context.auggiePath set but invalid → fail hard
        registry
            .apply(&[(
                "context.auggiePath".to_string(),
                json!("/nonexistent/auggie"),
            )])
            .expect("set setting");
        let services = Services::new(store.clone()).with_settings_registry(registry.clone());
        let err = services
            .agent_complete_once_op("test".into(), None, None, None, None)
            .await
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("configured auggie path is not a valid file"),
            "got {err:?}"
        );

        // Case 3: context.auggiePath empty → fall through to discovery
        // (skip this case since a real auggie on the system would make it nondeterministic)
    }

    #[tokio::test]
    async fn complete_once_rejects_unknown_workspace() {
        let (_tmp, services) =
            services_with_bin(PathBuf::from("/nonexistent/intentd-test/auggie")).await;
        let err = services
            .agent_complete_once_op(
                "hi".into(),
                None,
                None,
                Some(WorkspaceId::from("ws-missing")),
                None,
            )
            .await
            .unwrap_err();
        assert_eq!(err.code(), -32602);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn complete_once_with_system_prompt_passes_composed_prompt() {
        // Fake CLI echoes stdin back so we can confirm the "System: …\n\n<prompt>"
        // composition rides on the wire when systemPrompt is supplied.
        use std::os::unix::fs::PermissionsExt;
        let dir = crate::tests::test_tempdir("intentd-complete-sysprompt-");
        let bin = dir.path().join("auggie");
        std::fs::write(&bin, "#!/bin/sh\nprintf '🤖\\n'\ncat\n").unwrap();
        std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();
        let (_tmp, services) = services_with_bin(bin).await;
        let v = services
            .agent_complete_once_op("why?".into(), Some("be terse".into()), None, None, None)
            .await
            .unwrap();
        assert_eq!(v["text"], "System: be terse\n\nwhy?");
    }
}
