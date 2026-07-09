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
    /// and `timeout_ms` is positive.
    pub(crate) async fn agent_complete_once_op(
        &self,
        prompt: String,
        system_prompt: Option<String>,
        model: Option<String>,
        workspace_id: Option<WorkspaceId>,
        timeout_ms: Option<u64>,
    ) -> Result<Value> {
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
        let auggie = match &self.auggie_bin {
            Some(bin) => bin.clone(),
            None => intent_context::discovery::find_auggie()
                .ok_or_else(|| Error::Internal("auggie CLI not found".to_string()))?,
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

    struct TempDb {
        path: PathBuf,
    }

    impl TempDb {
        fn new() -> Self {
            let path = std::env::temp_dir()
                .join(format!("intentd-completeops-{}.db", uuid::Uuid::new_v4()));
            Self { path }
        }
    }

    impl Drop for TempDb {
        fn drop(&mut self) {
            for suffix in ["", "-wal", "-shm"] {
                let _ =
                    std::fs::remove_file(PathBuf::from(format!("{}{suffix}", self.path.display())));
            }
        }
    }

    async fn services_with_bin(bin: PathBuf) -> (TempDb, Services) {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let services = Services::new(store).with_auggie_bin(bin);
        (tmp, services)
    }

    #[cfg(unix)]
    fn fake_auggie(tag: &str, body: &str) -> PathBuf {
        use std::os::unix::fs::PermissionsExt;
        let dir =
            std::env::temp_dir().join(format!("intentd-complete-{tag}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let bin = dir.join("auggie");
        std::fs::write(&bin, format!("#!/bin/sh\ncat > /dev/null\n{body}\n")).unwrap();
        std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();
        bin
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

    #[cfg(unix)]
    #[tokio::test]
    async fn complete_once_returns_cleaned_reply() {
        let bin = fake_auggie("ok", "printf '🤖\\nslug-goes-here\\n'");
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
        let bin = fake_auggie("slow", "sleep 30");
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
        let bin = fake_auggie("fail", "exit 3");
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
        let dir = std::env::temp_dir().join(format!(
            "intentd-complete-sysprompt-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let bin = dir.join("auggie");
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
