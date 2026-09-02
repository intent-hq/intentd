//! `agent.completeOnce` — one-shot prompt→completion RPC (PROTOCOL §5.32).
//!
//! Stateless one-shot completion so background FE requests (slug generation,
//! note-status checks — the two remaining `background-request.service.ts`
//! callers) no longer need an `ACPProvider` or an ephemeral agent session. The
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
//!
//! The ACP route is bounded: adapters are claimed from the daemon-wide
//! ephemeral-adapter semaphore ([`crate::acp_adapter`],
//! `agents.maxConcurrentAdapters`, monorepo#2062), so a fan-out of quick
//! actions queues instead of spawning a ~610 MB provider-CLI chain per call.
//! A call that spends its whole `timeoutMs` queued fails with
//! [`Error::AdapterBusy`] (`error.data.code = "adapter-busy"`), which is
//! deliberately distinguishable from a model/prompt timeout: nothing was
//! spawned, so the client may retry as soon as the daemon drains.

use std::path::PathBuf;
use std::time::Duration;

use intent_core::{Error, Result, WorkspaceId};
use serde_json::{json, Value};

use crate::enhance_ops::{
    clean_agent_message, run_auggie_print, DEFAULT_TIMEOUT_MS, MAX_TIMEOUT_MS,
};
use crate::file_ops;
use crate::one_shot_acp::{run_one_shot_acp, OneShotCommand, OneShotError};
use crate::Services;

/// Providers served by the ephemeral ACP one-shot runner. Every other
/// provider (and unset/undecidable settings) resolves the gate closed.
const ACP_ONE_SHOT_PROVIDERS: &[&str] = &["claude-code", "codex", "pi"];

/// The typed `{ available: false, reason }` result for a provider that cannot
/// serve a one-shot completion.
fn unavailable(reason: impl std::fmt::Display) -> Value {
    json!({ "available": false, "reason": reason.to_string() })
}

/// Resolve the quick-action model for a one-shot completion the caller sent no
/// `model` for (monorepo#1734): `quickActions.typeOverrides[type]`, then
/// `quickActions.defaultModel`, then `None` — the provider CLI default.
/// `quick_action_type` is the caller's optional `type` hint (`commit`, `pr`,
/// `review`, `fast`); the override map is FE-owned, so an unknown key simply
/// misses and falls through to the default.
///
/// `quickActions.providerSettings` is deliberately NOT a rung: it is the FE's
/// opaque per-provider snapshot cache (restored into the two keys above on a
/// provider switch), not an additional precedence tier.
///
/// The result is provider-guarded and returned BARE — the settings value is
/// user-authored and easily outlives a provider switch, so the CLI must never
/// be fed a foreign model id. A legacy compound `{provider}:{model}` value
/// (colon-bearing, pre-wire-rejection) is dropped with a warn log — settings
/// values are bare ids now. A bare id reuses `agent.create`'s asymmetric
/// cached-catalog evidence rule ([`ensure_bare_model_matches_provider`]): it is
/// dropped only when the effective provider's own cached catalog affirmatively
/// disproves ownership, so a cold start still passes it through.
///
/// Every drop falls to the next rung rather than erroring: a `-32602` here
/// would reject a model the caller never sent.
///
/// This chain is scoped to one-shot quick actions only — agent sessions,
/// delegated ones included, keep the background-agnostic creation-time chain
/// (monorepo#1729).
fn resolve_quick_action_model(
    settings: &intent_core::settings_file::SettingsFile,
    catalog: &crate::model_catalog::ModelCatalogCache,
    quick_action_type: Option<&str>,
    effective_provider: &str,
) -> Option<String> {
    let quick = &settings.quick_actions;
    let configured = quick_action_type
        .map(str::trim)
        .filter(|t| !t.is_empty())
        .and_then(|t| quick.type_overrides.get(t))
        .map(String::as_str)
        .map(str::trim)
        .filter(|m| !m.is_empty())
        .or_else(|| {
            quick
                .default_model
                .as_deref()
                .map(str::trim)
                .filter(|m| !m.is_empty())
        })?;

    let owned_bare = if configured.contains(':') {
        None
    } else {
        crate::agent_ops::ensure_bare_model_matches_provider(
            "agent.completeOnce",
            catalog,
            effective_provider,
            configured,
        )
        .ok()
        .map(|()| configured.to_string())
    };

    if owned_bare.is_none() {
        tracing::warn!(
            model = configured,
            provider = effective_provider,
            "configured quick-action model does not belong to the effective \
             provider; falling back to the CLI default"
        );
    }
    owned_bare
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
///
/// Providers flagged `config_option_model_strips_effort` (codex) get a
/// `{base}/{effort}` id stripped to its base, mirroring
/// `AgentManager::config_option_model_target`: the adapter's
/// `configOptions[id="model"]` select values are bare base ids, so a
/// suffixed value would never match.
fn config_option_model<'m>(
    provider: &intent_providers::ProviderConfig,
    model: Option<&'m str>,
) -> Option<&'m str> {
    if provider.model_flag.is_some() {
        return None;
    }
    let model = model.filter(|m| !m.is_empty() && *m != "default")?;
    if provider.config_option_model_strips_effort {
        let base = model.split_once('/').map_or(model, |(base, _)| base);
        return (!base.is_empty()).then_some(base);
    }
    Some(model)
}

impl Services {
    /// `agent.completeOnce` (PROTOCOL §5.32): stateless one-shot completion.
    /// Composes an optional `system_prompt` with `prompt` in the same shape as
    /// `agent.enhancePrompt` (§5.31) and returns the cleaned reply verbatim
    /// under `text`. The router pre-validates `prompt` is non-empty and
    /// `timeout_ms` is positive.
    ///
    /// Routed on the settings-derived effective provider
    /// (`model.defaultProvider`): auggie keeps the existing
    /// CLI path, claude-code / codex / pi run an ephemeral ACP session, and
    /// anything else returns `{ available: false, reason }`. Unset/undecidable
    /// settings resolve the gate CLOSED: falling through to the first
    /// registered provider would always be auggie and functionally reinstate
    /// the removed hardcoded default (matches FE #759, where unset resolves
    /// disabled).
    ///
    /// With no explicit `model`, the daemon resolves the user's quick-action
    /// settings itself ([`resolve_quick_action_model`], monorepo#1734) so any
    /// client — not just the FE — gets `quickActions.typeOverrides[type]` /
    /// `quickActions.defaultModel` for free; `quick_action_type` is the
    /// caller's optional `type` hint keying the override map.
    pub(crate) async fn agent_complete_once_op(
        &self,
        prompt: String,
        system_prompt: Option<String>,
        model: Option<String>,
        quick_action_type: Option<String>,
        workspace_id: Option<WorkspaceId>,
        timeout_ms: Option<u64>,
    ) -> Result<Value> {
        let settings = self.effective_settings();
        let effective_provider = crate::agent_session::derived_default_provider(&settings);
        let effective_provider = match effective_provider.as_deref() {
            Some(p) => p.to_string(),
            None => {
                return Ok(unavailable(
                    "completeOnce requires a decidable effective default provider",
                ))
            }
        };

        // An explicit client model always wins; only a caller that sent none
        // falls through to the quick-action settings chain.
        let model = match model.filter(|m| !m.trim().is_empty()) {
            Some(m) => Some(m),
            None => resolve_quick_action_model(
                &settings,
                &self.models_catalog,
                quick_action_type.as_deref(),
                &effective_provider,
            ),
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
                                "configured auggie path is not a valid file: {trimmed}"
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
            "One-shot completion",
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
        // npx discovery honors the test seam (`one_shot_npx`): `Some(inner)`
        // pins the result so the unresolvable branch below is reachable
        // hermetically on hosts where npx is installed.
        let npx = match &self.one_shot_npx {
            Some(pinned) => pinned.clone(),
            None => intent_providers::find_npx(),
        };
        let Some(cmd) = one_shot_launch(provider, resolved_bin, npx, model) else {
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
            // Queueing pressure is its own error shape, not a generic internal
            // failure: the caller waited out its whole timeout for a slot in
            // the daemon-wide adapter bound and no provider was ever launched,
            // so the client can back off and retry safely (monorepo#2062).
            Err(OneShotError::QueueTimeout { waited_ms, limit }) => Err(Error::AdapterBusy {
                provider: provider_id.to_string(),
                waited_ms,
                limit,
            }),
            Err(err) => Err(Error::Internal(format!("{provider_id}: {err}"))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use intent_store::Store;

    /// RAII temp `SQLite` store: the db (and its `-wal`/`-shm` sidecars) live in
    /// a guarded temp dir removed on drop — including on panic — unless
    /// `INTENTD_TEST_KEEP_TMP` (non-empty) is set.
    struct TempDb {
        dir: tempfile::TempDir,
        path: PathBuf,
    }

    impl TempDb {
        fn new() -> Self {
            let dir = crate::tests::test_tempdir("intentd-completeops-");
            let path = dir.path().join("store.db");
            Self { dir, path }
        }
    }

    /// Services with a fake CLI and `model.defaultProvider = "auggie"` so the
    /// provider gate is open: unset settings resolve the gate CLOSED
    /// (see `complete_once_unavailable_when_settings_unset`), so op-level
    /// tests must opt in to an auggie-active registry to reach the CLI.
    async fn services_with_bin(bin: PathBuf) -> (TempDb, Services) {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let registry = std::sync::Arc::new(
            crate::SettingsRegistry::load(tmp.dir.path().join("config.toml"))
                .expect("load registry"),
        );
        registry
            .apply(&[(
                "model.defaultProvider".to_string(),
                serde_json::json!("auggie"),
            )])
            .expect("set model.defaultProvider");
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
            .agent_complete_once_op("hi".into(), None, None, None, None, None)
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
        // `model.defaultProvider` unset.
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let services =
            Services::new(store).with_auggie_bin(PathBuf::from("/nonexistent/intentd-test/auggie"));
        let v = services
            .agent_complete_once_op("hi".into(), None, None, None, None, None)
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
            crate::SettingsRegistry::load(tmp.dir.path().join("config.toml"))
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
                r"import readline from 'node:readline';
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
"
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
            ("model.defaultProvider", serde_json::json!("codex")),
            (
                "providers.paths",
                serde_json::json!({ "codex": bin.to_string_lossy() }),
            ),
        ])
        .await;
        let v = services
            .agent_complete_once_op("make a slug".into(), None, None, None, None, None)
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
            services_with_settings(&[("model.defaultProvider", serde_json::json!("opencode"))])
                .await;
        let v = services
            .agent_complete_once_op("hi".into(), None, None, None, None, None)
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

    #[tokio::test]
    async fn complete_once_unavailable_when_adapter_unresolvable() {
        // The unresolvable-adapter branch, asserted hermetically: claude-code
        // is npx-only, so with npx pinned absent (the `one_shot_npx` seam —
        // real `find_npx` succeeds on any host with node) the launch resolves
        // to nothing and the op maps it to the exact `{ available: false,
        // reason }` envelope. Complements the env-gated
        // `wss_agent_complete_once_unavailable_when_adapter_unresolvable`
        // e2e, which self-skips wherever npx is installed.
        let (_tmp, services) =
            services_with_settings(&[("model.defaultProvider", serde_json::json!("claude-code"))])
                .await;
        let services = services.with_one_shot_npx(None);
        let v = services
            .agent_complete_once_op("hi".into(), None, None, None, None, None)
            .await
            .unwrap();
        assert_eq!(
            v,
            serde_json::json!({
                "available": false,
                "reason": "claude-code: no adapter could be resolved (binary not found and npx unavailable)"
            }),
            "an unresolvable adapter is a typed unavailable result, never an error"
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
            r"import readline from 'node:readline';
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
",
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
            ("model.defaultProvider", serde_json::json!("codex")),
            (
                "providers.paths",
                serde_json::json!({ "codex": bin.to_string_lossy() }),
            ),
        ])
        .await;
        let v = services
            .agent_complete_once_op("echo home".into(), None, None, None, None, None)
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
        // one-shot applies the model the same way — and codex sets
        // config_option_model_strips_effort, so a `{base}/{effort}` id is
        // stripped to its base (the adapter's option values are bare ids).
        let codex = intent_providers::find_provider("codex").unwrap();
        assert_eq!(config_option_model(codex, Some("gpt-5")), Some("gpt-5"));
        assert_eq!(
            config_option_model(codex, Some("gpt-5.3-codex/high")),
            Some("gpt-5.3-codex")
        );
        // Degenerate `/effort` with an empty base: no call at all.
        assert_eq!(config_option_model(codex, Some("/high")), None);
        // Providers WITHOUT the flag keep any `/` verbatim.
        assert_eq!(
            config_option_model(claude, Some("a/b")),
            Some("a/b"),
            "claude-code keeps '/' verbatim"
        );
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
            .agent_complete_once_op("make a slug".into(), None, None, None, None, None)
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
            .agent_complete_once_op("hi".into(), None, None, None, None, Some(200))
            .await
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("One-shot completion timed out after 200ms"),
            "got {err:?}"
        );
        // monorepo#4032: completeOnce timeouts (auto-commit generation rides
        // this op) must not masquerade as prompt-enhancement failures.
        assert!(
            !err.to_string().contains("Prompt enhancement"),
            "got {err:?}"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn complete_once_errors_on_nonzero_exit() {
        let (_bin_dir, bin) = fake_auggie("fail", "exit 3");
        let (_tmp, services) = services_with_bin(bin).await;
        let err = services
            .agent_complete_once_op("hi".into(), None, None, None, None, None)
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
            .apply(&[("model.defaultProvider".to_string(), json!("auggie"))])
            .expect("set model.defaultProvider");

        // Case 1: context.auggiePath set and valid → use it exclusively
        registry
            .apply(&[(
                "context.auggiePath".to_string(),
                json!(fake.to_str().unwrap()),
            )])
            .expect("set setting");
        let services = Services::new(store.clone()).with_settings_registry(registry.clone());
        let result = services
            .agent_complete_once_op("test".into(), None, None, None, None, None)
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
            .agent_complete_once_op("test".into(), None, None, None, None, None)
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
            .agent_complete_once_op(
                "why?".into(),
                Some("be terse".into()),
                None,
                None,
                None,
                None,
            )
            .await
            .unwrap();
        assert_eq!(v["text"], "System: be terse\n\nwhy?");
    }

    /// A `SettingsFile` carrying only the quick-action model keys.
    fn quick_action_settings(
        default_model: Option<&str>,
        type_overrides: &[(&str, &str)],
    ) -> intent_core::settings_file::SettingsFile {
        let mut settings = intent_core::settings_file::SettingsFile::default();
        settings.quick_actions.default_model = default_model.map(str::to_string);
        settings.quick_actions.type_overrides = type_overrides
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        settings
    }

    /// Empty catalog cache: no cached entry for any provider, so bare ids pass
    /// the asymmetric evidence guard (absence of evidence is not a mismatch).
    fn empty_catalog() -> crate::model_catalog::ModelCatalogCache {
        crate::model_catalog::ModelCatalogCache::new(None)
    }

    #[test]
    fn quick_action_model_precedence() {
        // typeOverrides[type] outranks defaultModel; an absent/blank override
        // (and an unknown type key) falls through to the default; nothing
        // configured resolves to the CLI default.
        let catalog = empty_catalog();
        let settings = quick_action_settings(Some("sonnet4.5"), &[("commit", "haiku4.5")]);
        assert_eq!(
            resolve_quick_action_model(&settings, &catalog, Some("commit"), "auggie"),
            Some("haiku4.5".to_string())
        );
        assert_eq!(
            resolve_quick_action_model(&settings, &catalog, Some("pr"), "auggie"),
            Some("sonnet4.5".to_string())
        );
        assert_eq!(
            resolve_quick_action_model(&settings, &catalog, Some("not-a-type"), "auggie"),
            Some("sonnet4.5".to_string())
        );
        assert_eq!(
            resolve_quick_action_model(&settings, &catalog, None, "auggie"),
            Some("sonnet4.5".to_string())
        );

        let blank = quick_action_settings(Some("  "), &[("commit", "")]);
        assert_eq!(
            resolve_quick_action_model(&blank, &catalog, Some("commit"), "auggie"),
            None,
            "blank values read as unset, not as an empty model id"
        );
        assert_eq!(
            resolve_quick_action_model(
                &intent_core::settings_file::SettingsFile::default(),
                &catalog,
                Some("commit"),
                "auggie"
            ),
            None
        );
    }

    #[test]
    fn quick_action_model_drops_legacy_compound_values() {
        // Settings values are bare model ids now: ANY colon-bearing legacy
        // compound value — owned, foreign, or malformed alike — is dropped
        // with a warn log rather than fed to the CLI.
        let catalog = empty_catalog();
        for legacy in [
            "auggie:sonnet4.5",
            "codex:gpt-5",
            ":sonnet4.5",
            "not-a-provider:sonnet4.5",
            "auggie:",
        ] {
            let settings = quick_action_settings(Some(legacy), &[]);
            assert_eq!(
                resolve_quick_action_model(&settings, &catalog, None, "auggie"),
                None,
                "{legacy} must fall through to the CLI default"
            );
        }
    }

    #[test]
    fn quick_action_bare_model_is_dropped_only_on_disproven_ownership() {
        // Bare ids reuse agent.create's asymmetric evidence rule: with no
        // cached catalog for the effective provider the id passes, and it is
        // dropped only when that provider's own catalog disproves ownership
        // while another provider claims it.
        let settings = quick_action_settings(Some("grok-4"), &[]);
        assert_eq!(
            resolve_quick_action_model(&settings, &empty_catalog(), None, "auggie"),
            Some("grok-4".to_string()),
            "no cached evidence must not drop a bare id"
        );

        // Use each provider's current catalog version key for ownership evidence.
        let catalog = empty_catalog();
        catalog.store_for_test(
            "auggie",
            crate::model_catalog::AUGGIE_CATALOG_VERSION,
            vec![serde_json::json!({ "id": "sonnet4.5", "provider": "auggie" })],
        );
        catalog.store_for_test(
            "grok",
            "",
            vec![serde_json::json!({ "id": "grok-4", "provider": "grok" })],
        );
        assert_eq!(
            resolve_quick_action_model(&settings, &catalog, None, "auggie"),
            None,
            "a bare id the effective provider's catalog disproves is dropped"
        );
        let owned = quick_action_settings(Some("sonnet4.5"), &[]);
        assert_eq!(
            resolve_quick_action_model(&owned, &catalog, None, "auggie"),
            Some("sonnet4.5".to_string())
        );
    }

    /// Fake auggie echoing its own argv so a test can assert the resolved
    /// `--model` reached the CLI.
    #[cfg(unix)]
    fn fake_auggie_echoing_args(tag: &str) -> (tempfile::TempDir, PathBuf) {
        fake_auggie(tag, "printf '🤖\\n%s\\n' \"$*\"")
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn complete_once_applies_quick_action_settings_when_model_omitted() {
        // monorepo#1734: with no explicit `model`, the daemon resolves the
        // user's quick-action settings itself — the `type` override first,
        // then the default — so any client gets the setting for free.
        let (_bin_dir, bin) = fake_auggie_echoing_args("quick-actions");
        let (tmp, services) = services_with_bin(bin).await;
        let registry = crate::SettingsRegistry::load(tmp.dir.path().join("config.toml"))
            .expect("load registry");
        registry
            .apply(&[
                (
                    "model.defaultProvider".to_string(),
                    serde_json::json!("auggie"),
                ),
                (
                    "quickActions.defaultModel".to_string(),
                    serde_json::json!("sonnet4.5"),
                ),
                (
                    "quickActions.typeOverrides".to_string(),
                    serde_json::json!({ "commit": "haiku4.5" }),
                ),
            ])
            .expect("apply quick-action settings");
        let services = services.with_settings_registry(std::sync::Arc::new(registry));

        let v = services
            .agent_complete_once_op("hi".into(), None, None, Some("commit".into()), None, None)
            .await
            .unwrap();
        assert!(
            v["text"].as_str().unwrap().contains("--model haiku4.5"),
            "the commit override must reach the CLI, got {v:?}"
        );

        let v = services
            .agent_complete_once_op("hi".into(), None, None, Some("pr".into()), None, None)
            .await
            .unwrap();
        assert!(
            v["text"].as_str().unwrap().contains("--model sonnet4.5"),
            "an unset override falls through to the quick-action default, got {v:?}"
        );

        // An explicit client model always wins over the settings chain.
        let v = services
            .agent_complete_once_op(
                "hi".into(),
                None,
                Some("opus4.7".into()),
                Some("commit".into()),
                None,
                None,
            )
            .await
            .unwrap();
        assert!(
            v["text"].as_str().unwrap().contains("--model opus4.7"),
            "an explicit model outranks quickActions.*, got {v:?}"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn complete_once_ignores_quick_action_provider_settings() {
        // `quickActions.providerSettings` is the FE's opaque per-provider
        // snapshot cache, not a precedence rung: with it as the only
        // quick-action key set, the CLI still runs with no `--model`.
        let (_bin_dir, bin) = fake_auggie_echoing_args("provider-settings");
        let (tmp, services) = services_with_bin(bin).await;
        let registry = crate::SettingsRegistry::load(tmp.dir.path().join("config.toml"))
            .expect("load registry");
        registry
            .apply(&[
                (
                    "model.defaultProvider".to_string(),
                    serde_json::json!("auggie"),
                ),
                (
                    "quickActions.providerSettings".to_string(),
                    serde_json::json!({ "auggie": { "defaultModel": "from-provider-settings" } }),
                ),
            ])
            .expect("apply quick-action settings");
        let services = services.with_settings_registry(std::sync::Arc::new(registry));

        let v = services
            .agent_complete_once_op("hi".into(), None, None, Some("commit".into()), None, None)
            .await
            .unwrap();
        assert!(
            !v["text"].as_str().unwrap().contains("--model"),
            "providerSettings must not resolve a model, got {v:?}"
        );
    }
}
