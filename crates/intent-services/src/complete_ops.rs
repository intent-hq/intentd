//! `agent.completeOnce` — one-shot prompt→completion RPC (PROTOCOL §5.32).
//!
//! Stateless one-shot completion so background FE requests (slug generation,
//! note-status checks — the two remaining `background-request.service.ts`
//! callers) no longer need an ACPProvider or an ephemeral agent session. The
//! daemon owns the full lifecycle: spawn the provider, collect its cleaned
//! reply, reap the process on any failure path (timeout, cancel, drop). No
//! session/agent state is created, so there is nothing to garbage-collect on
//! error — cleanup is `kill_on_drop` + a unix process-group SIGKILL on
//! timeout, inherited from the shared `run_auggie_print` helper on the auggie
//! path and from [`crate::acp_adapter`] on the ACP path.
//!
//! Routing is by effective provider (`derived_default_provider`): auggie runs
//! the `auggie --print` CLI, claude-code / codex / pi run an ephemeral ACP
//! session ([`crate::one_shot_acp`]), and anything else — including
//! unset/undecidable settings and a provider whose adapter cannot be resolved
//! — returns `{ available: false, reason }` rather than an error.

use std::path::PathBuf;
use std::time::Duration;

use intent_core::{Error, Result, WorkspaceId};
use serde_json::{json, Value};

use crate::enhance_ops::{
    clean_agent_message, run_auggie_print, DEFAULT_TIMEOUT_MS, MAX_TIMEOUT_MS,
};
use crate::file_ops;
use crate::one_shot_acp::{run_one_shot_acp, OneShotCommand};
use crate::Services;

/// Providers served by the ephemeral ACP one-shot runner. Every other
/// provider (and unset/undecidable settings) resolves the gate closed.
const ACP_ONE_SHOT_PROVIDERS: &[&str] = &["claude-code", "codex", "pi"];

/// The typed `{ available: false, reason }` result for a provider that cannot
/// serve a one-shot completion.
fn unavailable(reason: impl std::fmt::Display) -> Value {
    json!({ "available": false, "reason": reason.to_string() })
}

/// Pick the one-shot launch for `provider`, mirroring the model probe's
/// precedent (`provider_models`): an `npx_only_package` provider always runs
/// the pinned package via `npx -y`, otherwise the `resolved_bin` wins with the
/// pinned `fallback_npx_package` as the fallback. `None` when nothing resolves
/// — the caller turns that into `{ available: false, reason }`.
///
/// `resolved_bin` / `npx` are the caller's discovery results (parameterized so
/// the precedence is unit-testable without an install). The provider's own
/// launch args (`base_args` plus the caller's `model` when the provider has a
/// CLI model flag) come from [`intent_providers::build_provider_args`], which
/// gates the flag itself — for providers with no CLI model flag the caller
/// applies `model` best-effort after `session/new` instead (see
/// [`config_option_model`]).
fn one_shot_launch(
    provider: &intent_providers::ProviderConfig,
    resolved_bin: Option<PathBuf>,
    npx: Option<PathBuf>,
    model: Option<&str>,
) -> Option<OneShotCommand> {
    let inputs = intent_providers::ArgInputs {
        model,
        ..Default::default()
    };
    let args = intent_providers::build_provider_args(provider, &inputs);
    if let Some(pkg) = provider.npx_only_package {
        return npx.map(|npx| OneShotCommand::npx(npx, pkg).args(args));
    }
    if let Some(bin) = resolved_bin {
        return Some(OneShotCommand::binary(bin, args));
    }
    let pkg = provider.fallback_npx_package?;
    // The daemon-managed npx fallback: keep a stray env override from
    // redirecting the adapter (mirrors the codex probe launch, #555).
    npx.map(|npx| {
        OneShotCommand::npx(npx, pkg)
            .args(args)
            .env_remove("CODEX_PATH")
            .env_remove("CODEX_CONFIG")
    })
}

/// The model to apply post-`session/new` via `session/set_config_option`:
/// the caller's `model` when the provider has no CLI model flag (claude-code
/// / codex / pi — their adapters take the model as a session config option,
/// not a spawn arg), filtered like [`intent_providers::build_provider_args`]
/// filters the flag (empty and the `"default"` sentinel mean "adapter
/// default"). `None` when the launch args already carry the model.
fn config_option_model<'m>(
    provider: &intent_providers::ProviderConfig,
    model: Option<&'m str>,
) -> Option<&'m str> {
    if provider.model_flag.is_some() {
        return None;
    }
    model.filter(|m| !m.is_empty() && *m != "default")
}

impl Services {
    /// `agent.completeOnce` (PROTOCOL §5.32): stateless one-shot completion.
    /// Composes an optional `system_prompt` with `prompt` in the same shape as
    /// `agent.enhancePrompt` (§5.31) and returns the cleaned reply verbatim
    /// under `text`. The router pre-validates `prompt` is non-empty and
    /// `timeout_ms` is positive.
    ///
    /// Routed on the settings-derived effective provider (provider of
    /// `model.default`, else `providers.active`): auggie keeps the existing
    /// CLI path, claude-code / codex / pi run an ephemeral ACP session, and
    /// anything else returns `{ available: false, reason }`. Unset/undecidable
    /// settings resolve the gate CLOSED: falling through to the first
    /// registered provider would always be auggie and functionally reinstate
    /// the removed hardcoded default (matches FE #759, where unset resolves
    /// disabled).
    pub(crate) async fn agent_complete_once_op(
        &self,
        prompt: String,
        system_prompt: Option<String>,
        model: Option<String>,
        workspace_id: Option<WorkspaceId>,
        timeout_ms: Option<u64>,
    ) -> Result<Value> {
        let effective_provider =
            crate::agent_session::derived_default_provider(&self.effective_settings());
        let effective_provider = match effective_provider.as_deref() {
            Some(p) => p.to_string(),
            None => {
                return Ok(unavailable(
                    "completeOnce requires a decidable effective default provider",
                ))
            }
        };

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
        // `System: <system>\n\n<prompt>` mirrors the FE streamChat composition
        // when a system prompt is supplied; otherwise the raw prompt is used
        // verbatim. Shared by both routes.
        let full_prompt = match system_prompt.as_deref().map(str::trim) {
            Some(s) if !s.is_empty() => format!("System: {s}\n\n{prompt}"),
            _ => prompt.clone(),
        };
        let timeout = timeout_ms.unwrap_or(DEFAULT_TIMEOUT_MS).min(MAX_TIMEOUT_MS);

        if effective_provider != "auggie" {
            return self
                .complete_once_via_acp(
                    &effective_provider,
                    &full_prompt,
                    model.as_deref(),
                    cwd,
                    timeout,
                )
                .await;
        }

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

    /// The non-auggie route: run `full_prompt` through an ephemeral ACP
    /// session on the provider's adapter. A provider with no one-shot support
    /// and one whose adapter cannot be resolved (no binary, no npx) both
    /// return `{ available: false, reason }`; a resolved adapter that then
    /// fails the turn surfaces as an error, matching the auggie route's
    /// spawn/exit failures.
    async fn complete_once_via_acp(
        &self,
        provider_id: &str,
        full_prompt: &str,
        model: Option<&str>,
        cwd: Option<PathBuf>,
        timeout_ms: u64,
    ) -> Result<Value> {
        if !ACP_ONE_SHOT_PROVIDERS.contains(&provider_id) {
            return Ok(unavailable(format!(
                "completeOnce is not supported for the effective default provider: {provider_id}"
            )));
        }
        let Some(provider) = intent_providers::find_provider(provider_id) else {
            return Ok(unavailable(format!("unknown provider: {provider_id}")));
        };
        // `providers.paths` is keyed by the provider that OWNS the primary
        // binary (`primary_binary_provider_id`), matching the agent-spawn
        // resolution; an empty value counts as unset.
        let explicit_path = self
            .effective_settings()
            .providers
            .paths
            .get(provider.primary_binary_provider_id())
            .map(|p| p.trim().to_string())
            .filter(|p| !p.is_empty());
        let resolved_bin = (provider.npx_only_package.is_none())
            .then(|| {
                intent_providers::find_provider_binary(
                    provider.primary_binary_provider_id(),
                    provider.command,
                    explicit_path.as_deref(),
                )
            })
            .flatten();
        let Some(cmd) =
            one_shot_launch(provider, resolved_bin, intent_providers::find_npx(), model)
        else {
            return Ok(unavailable(format!(
                "{provider_id}: no adapter could be resolved (binary not found and npx unavailable)"
            )));
        };
        let cmd = match cwd {
            Some(dir) => cmd.cwd(dir),
            None => cmd,
        };
        // codex loads MCP servers from its inherited CODEX_HOME regardless of
        // the empty ACP `mcpServers` list, so the one-shot child gets the same
        // isolated throwaway home the model probe uses — a one-shot must never
        // start user-configured MCP servers. The TempDir binding keeps the
        // isolated home alive for the duration of the run.
        let (cmd, _codex_home) = if provider_id == "codex" {
            match crate::provider_models::with_isolated_codex_home(cmd) {
                Ok((cmd, home)) => (cmd, Some(home)),
                Err(e) => {
                    return Ok(unavailable(format!(
                        "codex: failed to create isolated CODEX_HOME: {e}"
                    )))
                }
            }
        } else {
            (cmd, None)
        };
        match run_one_shot_acp(
            cmd,
            full_prompt,
            config_option_model(provider, model),
            Duration::from_millis(timeout_ms),
        )
        .await
        {
            Ok(reply) => Ok(json!({ "text": clean_agent_message(&reply) })),
            Err(err) => Err(Error::Internal(format!("{provider_id}: {err}"))),
        }
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
                "reason": "completeOnce requires a decidable effective default provider"
            }),
            "unset provider settings must close the gate, not fall back to the first registered provider"
        );
    }

    /// Services wired to a settings registry seeded with `keys`, with no
    /// auggie test seam — the routing tests drive the provider gate purely
    /// from settings.
    async fn services_with_settings(keys: &[(&str, serde_json::Value)]) -> (TempDb, Services) {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let registry = std::sync::Arc::new(
            crate::SettingsRegistry::load(tmp._dir.path().join("config.toml"))
                .expect("load registry"),
        );
        let applied: Vec<(String, serde_json::Value)> = keys
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.clone()))
            .collect();
        registry.apply(&applied).expect("apply settings");
        let services = Services::new(store).with_settings_registry(registry);
        (tmp, services)
    }

    /// Executable stand-in for a provider's ACP adapter: a shell wrapper that
    /// execs node on an inline mock adapter answering `initialize`,
    /// `session/new`, and one `session/prompt` with `reply` streamed as an
    /// `agent_message_chunk`.
    #[cfg(unix)]
    fn fake_acp_adapter(tag: &str, reply: &str) -> (tempfile::TempDir, PathBuf) {
        use std::os::unix::fs::PermissionsExt;
        let dir = crate::tests::test_tempdir(&format!("intentd-complete-acp-{tag}-"));
        let script = dir.path().join("adapter.mjs");
        std::fs::write(
            &script,
            format!(
                r#"import readline from 'node:readline';
const send = (o) => process.stdout.write(JSON.stringify(o) + '\n');
const rl = readline.createInterface({{ input: process.stdin, terminal: false }});
rl.on('line', (line) => {{
  if (!line.trim()) return;
  const msg = JSON.parse(line);
  if (msg.method === 'initialize') return send({{ jsonrpc: '2.0', id: msg.id, result: {{ protocolVersion: 1 }} }});
  if (msg.method === 'session/new') return send({{ jsonrpc: '2.0', id: msg.id, result: {{ sessionId: 's1' }} }});
  if (msg.method === 'session/prompt') {{
    send({{
      jsonrpc: '2.0',
      method: 'session/update',
      params: {{ sessionId: 's1', update: {{ sessionUpdate: 'agent_message_chunk', content: {{ type: 'text', text: {reply:?} }} }} }},
    }});
    send({{ jsonrpc: '2.0', id: msg.id, result: {{ stopReason: 'end_turn' }} }});
  }}
}});
"#
            ),
        )
        .expect("write mock adapter");
        let bin = dir.path().join("codex-acp");
        std::fs::write(
            &bin,
            format!(
                "#!/bin/sh\nexec node {:?} \"$@\"\n",
                script.to_string_lossy()
            ),
        )
        .expect("write adapter wrapper");
        std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();
        (dir, bin)
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn complete_once_routes_acp_provider_to_one_shot_runner() {
        // codex resolves its adapter through `providers.paths["codex"]`
        // (`find_provider_binary`'s explicit tier), so the whole route runs
        // against the mock adapter with no provider install.
        let (_dir, bin) = fake_acp_adapter("ok", "🤖\nslug-from-acp");
        let (_tmp, services) = services_with_settings(&[
            ("providers.active", serde_json::json!("codex")),
            (
                "providers.paths",
                serde_json::json!({ "codex": bin.to_string_lossy() }),
            ),
        ])
        .await;
        let v = services
            .agent_complete_once_op("make a slug".into(), None, None, None, None)
            .await
            .unwrap();
        assert_eq!(
            v["text"], "slug-from-acp",
            "the ACP reply must come back cleaned, like the auggie route"
        );
    }

    #[tokio::test]
    async fn complete_once_unavailable_for_provider_without_one_shot_support() {
        // opencode has no one-shot route: a typed unavailable result, never an
        // internal error.
        let (_tmp, services) =
            services_with_settings(&[("providers.active", serde_json::json!("opencode"))]).await;
        let v = services
            .agent_complete_once_op("hi".into(), None, None, None, None)
            .await
            .unwrap();
        assert_eq!(
            v,
            serde_json::json!({
                "available": false,
                "reason": "completeOnce is not supported for the effective default provider: opencode"
            })
        );
    }

    /// Regression for the MCP-isolation review (PR #991): the codex one-shot
    /// child must run under an isolated throwaway `CODEX_HOME` — never the
    /// user's real one, whose `config.toml` can register MCP servers. The
    /// mock adapter streams the `CODEX_HOME` it sees back as the reply.
    #[cfg(unix)]
    #[tokio::test]
    async fn complete_once_codex_runs_with_isolated_codex_home() {
        use std::os::unix::fs::PermissionsExt;
        let dir = crate::tests::test_tempdir("intentd-complete-acp-home-");
        let script = dir.path().join("adapter.mjs");
        std::fs::write(
            &script,
            r#"import readline from 'node:readline';
const send = (o) => process.stdout.write(JSON.stringify(o) + '\n');
const rl = readline.createInterface({ input: process.stdin, terminal: false });
rl.on('line', (line) => {
  if (!line.trim()) return;
  const msg = JSON.parse(line);
  if (msg.method === 'initialize') return send({ jsonrpc: '2.0', id: msg.id, result: { protocolVersion: 1 } });
  if (msg.method === 'session/new') return send({ jsonrpc: '2.0', id: msg.id, result: { sessionId: 's1' } });
  if (msg.method === 'session/prompt') {
    send({
      jsonrpc: '2.0',
      method: 'session/update',
      params: { sessionId: 's1', update: { sessionUpdate: 'agent_message_chunk', content: { type: 'text', text: process.env.CODEX_HOME ?? 'unset' } } },
    });
    send({ jsonrpc: '2.0', id: msg.id, result: { stopReason: 'end_turn' } });
  }
});
"#,
        )
        .expect("write mock adapter");
        let bin = dir.path().join("codex-acp");
        std::fs::write(
            &bin,
            format!(
                "#!/bin/sh\nexec node {:?} \"$@\"\n",
                script.to_string_lossy()
            ),
        )
        .expect("write adapter wrapper");
        std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();

        let (_tmp, services) = services_with_settings(&[
            ("providers.active", serde_json::json!("codex")),
            (
                "providers.paths",
                serde_json::json!({ "codex": bin.to_string_lossy() }),
            ),
        ])
        .await;
        let v = services
            .agent_complete_once_op("echo home".into(), None, None, None, None)
            .await
            .unwrap();
        let child_home = v["text"].as_str().expect("adapter echoed CODEX_HOME");
        assert!(
            child_home.contains("intentd-codex-home-"),
            "the one-shot child must see the isolated throwaway CODEX_HOME, got: {child_home}"
        );
    }

    #[test]
    fn config_option_model_gates_on_missing_cli_model_flag() {
        // claude-code / pi have no CLI model flag: the model rides
        // session/set_config_option — except the empty/"default" sentinels,
        // which mean "adapter default" (mirrors build_provider_args).
        let claude = intent_providers::find_provider("claude-code").unwrap();
        assert_eq!(config_option_model(claude, Some("opus-x")), Some("opus-x"));
        assert_eq!(config_option_model(claude, Some("")), None);
        assert_eq!(config_option_model(claude, Some("default")), None);
        assert_eq!(config_option_model(claude, None), None);
        let pi = intent_providers::find_provider("pi").unwrap();
        assert_eq!(config_option_model(pi, Some("pi-large")), Some("pi-large"));
        // codex-acp also has no CLI model flag on its plain launch, so the
        // one-shot applies the model the same way.
        let codex = intent_providers::find_provider("codex").unwrap();
        assert_eq!(config_option_model(codex, Some("gpt-5")), Some("gpt-5"));
        // A provider WITH a CLI model flag carries the model in its args; no
        // post-session application.
        let droid = intent_providers::find_provider("droid").unwrap();
        assert_eq!(config_option_model(droid, Some("m")), None);
    }

    #[test]
    fn one_shot_launch_resolution_precedence() {
        let npx = PathBuf::from("/usr/bin/npx");
        let bin = PathBuf::from("/opt/bin/codex-acp");

        // npx-only (claude-code, pi): the pinned package always wins, and no
        // npx means no launch at all.
        let claude = intent_providers::find_provider("claude-code").unwrap();
        assert!(one_shot_launch(claude, Some(bin.clone()), Some(npx.clone()), None).is_some());
        assert!(one_shot_launch(claude, Some(bin.clone()), None, None).is_none());

        // codex: resolved binary first, pinned npx fallback second, nothing
        // when neither resolves (the `{ available: false }` path).
        let codex = intent_providers::find_provider("codex").unwrap();
        assert!(one_shot_launch(codex, Some(bin), None, None).is_some());
        assert!(one_shot_launch(codex, None, Some(npx), None).is_some());
        assert!(one_shot_launch(codex, None, None, None).is_none());
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
