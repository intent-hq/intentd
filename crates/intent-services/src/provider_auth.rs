//! Daemon-owned provider auth-status probes (`host.providerAuthStatus`).
//!
//! Centralizes the per-provider "am I logged in?" checks the reference FE
//! orchestrated client-side (`provider-availability.service.ts`) so every
//! client can ask the daemon instead. One probe per provider, mirroring the
//! FE semantics being replaced:
//!
//! - **auggie** — `auggie model list`: explicit not-logged-in markers ⇒
//!   `false`; exit 0 ⇒ `true`; else unknown.
//! - **claude-code** — the real `claude` CLI with its registry
//!   `auth_check_args` (exit 0 ⇒ authenticated).
//! - **codex** — the real `codex` CLI with `login status` (same exit-code
//!   semantics; the codex-acp adapter is never spawned here).
//! - **opencode** — `opencode models`: non-zero exit ⇒ `false`; `true` iff at
//!   least one `provider/model` line ([`crate::provider_models`]).
//! - **droid** — ACP probe (initialize + session/new): non-empty model list ⇒
//!   `true`; explicit auth-required error ⇒ `false`; else unknown.
//! - **grok** — `grok models` parsed via
//!   [`intent_providers::parse_grok_models_command_output`] (the exit code is
//!   never trusted).
//! - **pi** — ACP probe via the pinned pi-acp npx adapter, gated on the `pi`
//!   CLI being installed: non-empty model list ⇒ `true`; an empty list or an
//!   auth-required error ⇒ `false`; else unknown.
//!
//! Not-installed providers are never probed (`authenticated: null`). Probes
//! run in parallel, each bounded by its own timeout. Results are cached per
//! provider for a short TTL with single-flighted probes (the pattern of
//! [`crate::model_catalog`], simplified); `force` bypasses the cache read but
//! still joins any in-flight probe.
//!
//! `intentd doctor` shares [`check_provider_auth_cli`] (the exit-code + grok
//! CLI probe) so the doctor report and the RPC cannot drift.

use std::collections::HashMap;
use std::ffi::OsStr;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use tokio::sync::OnceCell;

/// Every provider `host.providerAuthStatus` can probe, in response order.
pub const AUTH_PROBE_PROVIDERS: &[&str] = &[
    "auggie",
    "claude-code",
    "codex",
    "opencode",
    "droid",
    "grok",
    "pi",
];

/// Timeout for one CLI auth probe (`auth status` / `login status` /
/// `grok models` / `auggie model list`). Matches the doctor's historical 8s
/// budget; opencode and the ACP probes carry their own budgets in
/// [`crate::provider_models`].
const CLI_AUTH_TIMEOUT: Duration = Duration::from_secs(8);

/// How long one provider's probe outcome is served from cache. Auth state
/// changes rarely (a login/logout in a terminal), so a short TTL keeps
/// repeated status calls from re-spawning CLIs while staying fresh enough
/// for a recheck button (`force: true` bypasses it entirely).
const AUTH_CACHE_TTL: Duration = Duration::from_secs(60);

/// Outcome of one CLI auth probe, shared by `intentd doctor` (which formats
/// every variant distinctly) and the RPC (which folds the last three to
/// `null`/unknown).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CliAuthProbe {
    /// The CLI reported an authenticated state (exit 0, or grok markers).
    Authenticated,
    /// The CLI reported an unauthenticated state (non-zero exit, or grok
    /// markers).
    NotAuthenticated,
    /// The probe ran but produced no auth signal (grok: exit 0, no markers,
    /// no models).
    StatusUnknown,
    /// The CLI could not be spawned, or ran but failed outright.
    Failed,
    /// The probe hit [`CLI_AUTH_TIMEOUT`].
    TimedOut,
}

impl CliAuthProbe {
    /// The wire mapping for `host.providerAuthStatus`:
    /// `authenticated: true | false | null`.
    fn auth_status(self) -> Option<bool> {
        match self {
            CliAuthProbe::Authenticated => Some(true),
            CliAuthProbe::NotAuthenticated => Some(false),
            CliAuthProbe::StatusUnknown | CliAuthProbe::Failed | CliAuthProbe::TimedOut => None,
        }
    }
}

/// Best-effort CLI authentication probe for an installed provider: run its
/// `auth_check_args` with a short timeout. Most providers signal auth via the
/// exit code (0 ⇒ authenticated); grok's `models` probe exits 0 in both auth
/// states, so its stdout is parsed for the explicit auth markers instead.
/// Shared by `intentd doctor` and `host.providerAuthStatus`.
pub async fn check_provider_auth_cli(
    provider_id: &str,
    program: &OsStr,
    auth_check_args: &[&str],
) -> CliAuthProbe {
    if provider_id == "grok" {
        let mut cmd = tokio::process::Command::new(program);
        cmd.args(auth_check_args)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true);
        return match tokio::time::timeout(CLI_AUTH_TIMEOUT, cmd.output()).await {
            Ok(Ok(output)) => {
                let stdout = String::from_utf8_lossy(&output.stdout);
                let parsed = intent_providers::parse_grok_models_command_output(&stdout);
                grok_probe_outcome(
                    parsed.authenticated,
                    parsed.models.is_empty(),
                    output.status.success(),
                )
            }
            Ok(Err(_)) => CliAuthProbe::Failed,
            Err(_) => CliAuthProbe::TimedOut,
        };
    }
    let mut cmd = tokio::process::Command::new(program);
    cmd.args(auth_check_args)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true);
    match tokio::time::timeout(CLI_AUTH_TIMEOUT, cmd.status()).await {
        Ok(Ok(status)) if status.success() => CliAuthProbe::Authenticated,
        Ok(Ok(_)) => CliAuthProbe::NotAuthenticated,
        Ok(Err(_)) => CliAuthProbe::Failed,
        Err(_) => CliAuthProbe::TimedOut,
    }
}

/// Map a parsed `grok models` run to a probe outcome. Explicit markers win;
/// with no marker, a non-empty model list means the CLI is serving models
/// (authenticated); no markers and no models distinguishes a probe that ran
/// but said nothing (exit 0) from one that failed outright.
fn grok_probe_outcome(
    marker: Option<bool>,
    models_empty: bool,
    exit_success: bool,
) -> CliAuthProbe {
    match (marker, models_empty) {
        (Some(true), _) => CliAuthProbe::Authenticated,
        (Some(false), _) => CliAuthProbe::NotAuthenticated,
        (None, false) => CliAuthProbe::Authenticated,
        (None, true) if exit_success => CliAuthProbe::StatusUnknown,
        (None, true) => CliAuthProbe::Failed,
    }
}

/// Explicit not-logged-in markers in `auggie model list` output (parity with
/// the FE `checkAuggieAuth` regex).
fn auggie_output_unauthenticated(output: &str) -> bool {
    let lower = output.to_lowercase();
    [
        "not currently logged in",
        "not logged in",
        "not authenticated",
        "login required",
        "please log in",
    ]
    .iter()
    .any(|marker| lower.contains(marker))
}

/// auggie probe: `auggie model list` — explicit not-logged-in markers (in
/// stdout or stderr) ⇒ `false`; a clean exit ⇒ `true`; anything else is
/// unknown.
async fn check_auggie_auth(program: &OsStr) -> Option<bool> {
    let mut cmd = tokio::process::Command::new(program);
    cmd.args(["model", "list"])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    match tokio::time::timeout(CLI_AUTH_TIMEOUT, cmd.output()).await {
        Ok(Ok(output)) => {
            let combined = format!(
                "{}\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            if auggie_output_unauthenticated(&combined) {
                Some(false)
            } else if output.status.success() {
                Some(true)
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Timeout for the opencode readiness probe (`opencode models` can be slower
/// than a simple auth-file read; parity with the FE's 10s budget).
const OPENCODE_READY_TIMEOUT: Duration = Duration::from_secs(10);

/// Whether `opencode models` stdout carries at least one `provider/model`
/// line (parity with the FE `checkOpenCodeReady` line test).
fn opencode_models_ready(stdout: &str) -> bool {
    stdout.lines().any(|line| {
        let trimmed = line.trim();
        !trimmed.is_empty() && trimmed.contains('/') && !trimmed.starts_with('#')
    })
}

/// opencode probe: `opencode models` is the readiness gate — credentials may
/// come from `opencode auth login`, env vars, or a project `.env`, so there
/// is no single "am I logged in" signal. Non-zero exit ⇒ `false`; exit 0 is
/// ready iff at least one `provider/model` line; timeout/spawn ⇒ unknown.
async fn check_opencode_auth(program: &OsStr) -> Option<bool> {
    let mut cmd = tokio::process::Command::new(program);
    cmd.arg("models")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true);
    match tokio::time::timeout(OPENCODE_READY_TIMEOUT, cmd.output()).await {
        Ok(Ok(output)) => {
            if !output.status.success() {
                return Some(false);
            }
            Some(opencode_models_ready(&String::from_utf8_lossy(
                &output.stdout,
            )))
        }
        _ => None,
    }
}

/// Run one provider's auth probe. The caller has already gated on the
/// provider being installed (`program` is its resolved binary; pi passes the
/// resolved `pi` CLI purely as the install gate — its probe runs the pinned
/// npx adapter).
async fn probe_provider(provider_id: &'static str, program: std::ffi::OsString) -> Option<bool> {
    match provider_id {
        "auggie" => check_auggie_auth(&program).await,
        "claude-code" | "codex" | "grok" => {
            let args = intent_providers::find_provider(provider_id)
                .and_then(|cfg| cfg.auth_check_args)
                .unwrap_or_default();
            if args.is_empty() {
                return None;
            }
            check_provider_auth_cli(provider_id, &program, args)
                .await
                .auth_status()
        }
        "opencode" => check_opencode_auth(&program).await,
        "droid" => crate::provider_models::probe_droid_auth(program.into()).await,
        "pi" => crate::provider_models::probe_pi_auth().await,
        _ => None,
    }
}

/// Resolve a probe-able provider's install gate: the binary the probe needs
/// on the daemon host, or `None` when the provider is not installed (never
/// probed). claude-code gates on the real `claude` CLI (auth is owned by the
/// CLI, not the npx adapter); codex gates on the real `codex` CLI (not
/// codex-acp); pi gates on the `pi` CLI even though its probe runs the pinned
/// npx adapter.
fn resolve_probe_binary(provider_id: &str) -> Option<std::ffi::OsString> {
    let command = match provider_id {
        "auggie" => {
            return crate::auggie_discovery::find_auggie().map(|p| p.into_os_string());
        }
        "claude-code" => "claude",
        "codex" => "codex",
        "opencode" => "opencode",
        "droid" => "droid",
        "grok" => "grok",
        "pi" => "pi",
        _ => return None,
    };
    intent_providers::find_provider_binary(provider_id, command, None).map(|p| p.into_os_string())
}

/// Per-provider auth-status cache: last outcome + fetch instant, plus a
/// single-flight slot so concurrent status calls share one probe. In-memory
/// only — a daemon restart re-probes.
struct AuthStatusCache {
    entries: Mutex<HashMap<&'static str, (Instant, Option<bool>)>>,
    inflight: Mutex<HashMap<&'static str, Arc<OnceCell<Option<bool>>>>>,
}

impl AuthStatusCache {
    fn new() -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            inflight: Mutex::new(HashMap::new()),
        }
    }

    fn fresh(&self, provider_id: &str) -> Option<Option<bool>> {
        let entries = self.entries.lock().expect("auth cache poisoned");
        let (at, value) = entries.get(provider_id)?;
        (at.elapsed() < AUTH_CACHE_TTL).then_some(*value)
    }

    fn store(&self, provider_id: &'static str, value: Option<bool>) {
        self.entries
            .lock()
            .expect("auth cache poisoned")
            .insert(provider_id, (Instant::now(), value));
    }

    fn join_inflight(&self, provider_id: &'static str) -> Arc<OnceCell<Option<bool>>> {
        self.inflight
            .lock()
            .expect("auth inflight poisoned")
            .entry(provider_id)
            .or_default()
            .clone()
    }

    fn finish_inflight(&self, provider_id: &str, cell: &Arc<OnceCell<Option<bool>>>) {
        let mut inflight = self.inflight.lock().expect("auth inflight poisoned");
        if inflight
            .get(provider_id)
            .is_some_and(|cur| Arc::ptr_eq(cur, cell))
        {
            inflight.remove(provider_id);
        }
    }
}

/// The process-wide auth-status cache.
fn cache() -> &'static AuthStatusCache {
    static CACHE: OnceLock<AuthStatusCache> = OnceLock::new();
    CACHE.get_or_init(AuthStatusCache::new)
}

/// Resolve one provider's auth status through the cache: non-forced reads
/// within TTL serve the cached outcome; otherwise the probe runs
/// single-flighted (concurrent callers — forced or not — share the in-flight
/// result). Not-installed providers short-circuit to `None` without probing
/// or caching, so an install is picked up immediately.
async fn resolve_auth_status(provider_id: &'static str, force: bool) -> Option<bool> {
    let program = resolve_probe_binary(provider_id)?;
    if !force {
        if let Some(cached) = cache().fresh(provider_id) {
            return cached;
        }
    }
    let cell = cache().join_inflight(provider_id);
    let value = *cell
        .get_or_init(|| async {
            let value = probe_provider(provider_id, program).await;
            cache().store(provider_id, value);
            value
        })
        .await;
    cache().finish_inflight(provider_id, &cell);
    value
}

/// `host.providerAuthStatus` (§5.14): probe auth status for every probe-able
/// provider (or one, when `provider_id` is given) and return
/// `{ providers: [{ id, authenticated }] }` with
/// `authenticated: true | false | null` (`null` = unknown / probe failed /
/// not installed). Probes run in parallel, each bounded by its own timeout.
/// An unknown `provider_id` is an invalid-params error (`-32602`).
pub async fn provider_auth_status(provider_id: Option<&str>, force: bool) -> Result<Value, String> {
    let selected: Vec<&'static str> = match provider_id {
        Some(requested) => match AUTH_PROBE_PROVIDERS.iter().find(|id| **id == requested) {
            Some(id) => vec![id],
            None => return Err(format!("Unknown providerId: {requested}")),
        },
        None => AUTH_PROBE_PROVIDERS.to_vec(),
    };
    let handles: Vec<_> = selected
        .iter()
        .map(|id| {
            let id: &'static str = id;
            tokio::spawn(async move { resolve_auth_status(id, force).await })
        })
        .collect();
    let mut providers = Vec::with_capacity(handles.len());
    for (id, handle) in selected.iter().zip(handles) {
        // A panicked probe task degrades to unknown rather than failing the
        // whole status call.
        let authenticated = handle.await.unwrap_or(None);
        providers.push(json!({ "id": id, "authenticated": authenticated }));
    }
    Ok(json!({ "providers": providers }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_probe_wire_mapping() {
        assert_eq!(CliAuthProbe::Authenticated.auth_status(), Some(true));
        assert_eq!(CliAuthProbe::NotAuthenticated.auth_status(), Some(false));
        assert_eq!(CliAuthProbe::StatusUnknown.auth_status(), None);
        assert_eq!(CliAuthProbe::Failed.auth_status(), None);
        assert_eq!(CliAuthProbe::TimedOut.auth_status(), None);
    }

    #[test]
    fn grok_outcome_markers_win_over_models_and_exit() {
        assert_eq!(
            grok_probe_outcome(Some(true), true, false),
            CliAuthProbe::Authenticated
        );
        assert_eq!(
            grok_probe_outcome(Some(false), false, true),
            CliAuthProbe::NotAuthenticated
        );
        // No marker but a parsed model list ⇒ the CLI is serving models.
        assert_eq!(
            grok_probe_outcome(None, false, false),
            CliAuthProbe::Authenticated
        );
        // No markers, no models: exit code distinguishes silent from broken.
        assert_eq!(
            grok_probe_outcome(None, true, true),
            CliAuthProbe::StatusUnknown
        );
        assert_eq!(grok_probe_outcome(None, true, false), CliAuthProbe::Failed);
    }

    #[test]
    fn auggie_markers_match_fe_regex() {
        for output in [
            "You are NOT currently logged in.",
            "error: not logged in",
            "Not authenticated. Run auggie login.",
            "Login required",
            "Please log in first",
        ] {
            assert!(auggie_output_unauthenticated(output), "{output}");
        }
        assert!(!auggie_output_unauthenticated("model-a\nmodel-b\n"));
        assert!(!auggie_output_unauthenticated(""));
    }

    #[test]
    fn opencode_ready_requires_provider_model_line() {
        assert!(opencode_models_ready("anthropic/claude-sonnet-4\n"));
        assert!(opencode_models_ready("noise\nopenai/gpt-5\n"));
        assert!(!opencode_models_ready(""));
        assert!(!opencode_models_ready("# provider/model header\n"));
        assert!(!opencode_models_ready("no models configured\n"));
    }

    #[tokio::test]
    async fn unknown_provider_id_is_invalid_params() {
        let err = provider_auth_status(Some("not-a-provider"), false)
            .await
            .expect_err("unknown provider must error");
        assert!(err.contains("not-a-provider"), "{err}");
    }

    #[tokio::test]
    async fn scoped_result_carries_exactly_one_entry() {
        // grok resolution is a pure filesystem check, so this runs the real
        // resolve path; on hosts without grok the probe is skipped entirely
        // and authenticated is null — either way the shape holds.
        let result = provider_auth_status(Some("grok"), false)
            .await
            .expect("grok is a known provider");
        let providers = result["providers"].as_array().expect("providers array");
        assert_eq!(providers.len(), 1);
        assert_eq!(providers[0]["id"], "grok");
        assert!(
            providers[0]["authenticated"].is_boolean() || providers[0]["authenticated"].is_null()
        );
    }
}
