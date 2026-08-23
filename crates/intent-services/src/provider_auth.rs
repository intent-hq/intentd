//! Daemon-owned provider auth-status probes (`host.providerAuthStatus`).
//!
//! Centralizes the per-provider "am I logged in?" checks the reference FE
//! orchestrated client-side (`provider-availability.service.ts`) so every
//! client can ask the daemon instead. One probe per provider, mirroring the
//! FE semantics being replaced:
//!
//! - **auggie** — the real `auggie` CLI with its registry `auth_check_args`
//!   (`token print`; exit 0 ⇒ authenticated). The command's output IS the auth
//!   session secret, so it rides the generic exit-code arm where stdout and
//!   stderr are discarded — never captured, logged, or surfaced.
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
//! Not-installed providers are never probed (`authenticated: null`). The
//! install gate honors `providers.paths` overrides threaded by the transport
//! layer (monorepo#1086 — the settings live above this crate; the discovery
//! surface threads them the same way, monorepo#1065), so a provider reachable
//! only via a valid override is still probed. Probes run in parallel, each
//! bounded by its own timeout. Results are cached per provider for a short
//! TTL with single-flighted probes (the pattern of [`crate::model_catalog`],
//! simplified); `force` bypasses the cache read but still joins any in-flight
//! probe.
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
pub(crate) const AUTH_PROBE_PROVIDERS: &[&str] = &[
    "auggie",
    "claude-code",
    "codex",
    "opencode",
    "droid",
    "grok",
    "pi",
];

/// Timeout for one CLI auth probe (`auth status` / `login status` /
/// `grok models` / `auggie token print`). Matches the doctor's historical 8s
/// budget; opencode uses [`OPENCODE_READY_TIMEOUT`] and the droid/pi ACP
/// probes carry their own budgets in [`crate::provider_models`].
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
/// states, so its stdout is parsed for the explicit auth markers instead; and
/// opencode's `models` probe requires at least one `provider/model` stdout
/// line beyond exit 0 (credentials may come from `opencode auth login`, env
/// vars, or a project `.env`). Shared by `intentd doctor` and
/// `host.providerAuthStatus` so the two cannot drift.
///
/// The generic (exit-code) arm nulls stdout and stderr — auggie's `token print`
/// probe prints the auth session secret, so its output must never be captured.
///
/// Every arm spawns via [`probe_command`], which prepends the resolved
/// binary's own directory to the child's PATH (monorepo#1863): an
/// nvm-installed CLI's `#!/usr/bin/env node` shebang resolves the sibling
/// `node` even when the daemon's PATH (systemd user unit) carries none. An
/// exit of 127 (command-resolution failure) means the probe could not run at
/// all, so it maps to [`CliAuthProbe::Failed`] — never to
/// [`CliAuthProbe::NotAuthenticated`].
pub async fn check_provider_auth_cli(
    provider_id: &str,
    program: &OsStr,
    auth_check_args: &[&str],
) -> CliAuthProbe {
    match provider_id {
        "grok" => {
            let mut cmd = probe_command(program);
            cmd.args(auth_check_args)
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::null())
                .kill_on_drop(true);
            match tokio::time::timeout(CLI_AUTH_TIMEOUT, cmd.output()).await {
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
            }
        }
        "opencode" => {
            let mut cmd = probe_command(program);
            cmd.args(auth_check_args)
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::null())
                .kill_on_drop(true);
            match tokio::time::timeout(OPENCODE_READY_TIMEOUT, cmd.output()).await {
                Ok(Ok(output)) if could_not_run(output.status) => CliAuthProbe::Failed,
                Ok(Ok(output)) if !output.status.success() => CliAuthProbe::NotAuthenticated,
                Ok(Ok(output)) => {
                    if opencode_models_ready(&String::from_utf8_lossy(&output.stdout)) {
                        CliAuthProbe::Authenticated
                    } else {
                        CliAuthProbe::NotAuthenticated
                    }
                }
                Ok(Err(_)) => CliAuthProbe::Failed,
                Err(_) => CliAuthProbe::TimedOut,
            }
        }
        _ => {
            let mut cmd = probe_command(program);
            cmd.args(auth_check_args)
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .kill_on_drop(true);
            match tokio::time::timeout(CLI_AUTH_TIMEOUT, cmd.status()).await {
                Ok(Ok(status)) if status.success() => CliAuthProbe::Authenticated,
                Ok(Ok(status)) if could_not_run(status) => CliAuthProbe::Failed,
                Ok(Ok(_)) => CliAuthProbe::NotAuthenticated,
                Ok(Err(_)) => CliAuthProbe::Failed,
                Err(_) => CliAuthProbe::TimedOut,
            }
        }
    }
}

/// Base command for a CLI auth probe: the resolved binary spawned with the
/// enhanced PATH — its own parent directory prepended, then the enriched
/// tool dirs and the inherited PATH ([`intent_providers::enhanced_path`],
/// the same env the provider session / model-catalog probe spawns use). The
/// discovery tiers (intentd#1045) can find a binary in a directory the
/// daemon's PATH does not carry (an nvm versions bin dir under a systemd
/// user unit); spawning with the bare daemon env then fails with exit 127
/// because `#!/usr/bin/env node` cannot resolve the sibling `node`
/// (monorepo#1863).
///
/// A path-shaped `program` (more than one component — e.g. a relative
/// `providers.paths.*` override, which `resolve_auggie_override` accepts as
/// any existing file) is lexically absolutized first (no symlink
/// resolution): `enhanced_path` only prepends the parent dir of an absolute
/// path. A bare name (doctor's fallback when discovery has no resolved
/// path) is passed through untouched — absolutizing it would put the
/// process CWD at the head of the child's PATH.
fn probe_command(program: &OsStr) -> tokio::process::Command {
    let mut cmd = tokio::process::Command::new(program);
    let program_path = std::path::Path::new(program);
    let program_path = if program_path.components().count() > 1 {
        std::path::absolute(program_path).unwrap_or_else(|_| program_path.to_path_buf())
    } else {
        program_path.to_path_buf()
    };
    cmd.env("PATH", intent_providers::enhanced_path(Some(&program_path)));
    cmd
}

/// Whether an exit status is the shell/loader command-resolution failure
/// code (127 — e.g. `env: 'node': No such file or directory` from a
/// Node-shebang CLI). Such a probe never ran the CLI's auth check, so it
/// carries no auth verdict.
fn could_not_run(status: std::process::ExitStatus) -> bool {
    status.code() == Some(127)
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
        (Some(false), _) => CliAuthProbe::NotAuthenticated,
        (Some(true), _) | (None, false) => CliAuthProbe::Authenticated,
        (None, true) if exit_success => CliAuthProbe::StatusUnknown,
        (None, true) => CliAuthProbe::Failed,
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

/// Run one provider's auth probe. The caller has already gated on the
/// provider being installed (`program` is its resolved binary; pi passes the
/// resolved `pi` CLI purely as the install gate — its probe runs the pinned
/// npx adapter). CLI-probed providers (auggie, claude-code, codex, opencode,
/// grok) share [`check_provider_auth_cli`] with `intentd doctor`.
async fn probe_provider(provider_id: &'static str, program: std::ffi::OsString) -> Option<bool> {
    match provider_id {
        "auggie" | "claude-code" | "codex" | "opencode" | "grok" => {
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
        "droid" => crate::provider_models::probe_droid_auth(program.into()).await,
        "pi" => crate::provider_models::probe_pi_auth().await,
        _ => None,
    }
}

/// The `providers.paths` key a probe provider's install gate reads: the
/// provider that OWNS its primary binary
/// ([`intent_providers::ProviderConfig::primary_binary_provider_id`]),
/// matching spawn resolution. Today every probe-able provider owns its own
/// primary (only unsloth remaps, and unsloth is not probe-able).
fn override_key(provider_id: &'static str) -> &'static str {
    intent_providers::find_provider(provider_id).map_or(
        provider_id,
        intent_providers::ProviderConfig::primary_binary_provider_id,
    )
}

/// checkAuggie-parity validation for auggie's threaded override
/// (`context.auggiePath` → `providers.paths.auggie`, applied under the
/// `auggie` key by the transport layer): trimmed and an existing file or
/// symlink — the same acceptance as the transport's `resolve_auggie_path`,
/// not `find_provider_binary`'s absolute+executable explicit tier. An invalid
/// value falls through to canonical discovery.
fn resolve_auggie_override(path: &str) -> Option<std::path::PathBuf> {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        return None;
    }
    let p = std::path::PathBuf::from(trimmed);
    (p.is_file() || p.is_symlink()).then_some(p)
}

/// Resolve a probe-able provider's install gate: the binary the probe needs
/// on the daemon host, or `None` when the provider is not installed (never
/// probed). claude-code gates on the real `claude` CLI (auth is owned by the
/// CLI, not the npx adapter); codex gates on the real `codex` CLI (not
/// codex-acp); pi gates on the `pi` CLI even though its probe runs the pinned
/// npx adapter.
///
/// `override_path` is the raw `providers.paths` value for the provider's
/// [`override_key`], applied as `find_provider_binary`'s explicit-path tier
/// (monorepo#1086) — but only when the gate command IS the registry primary
/// command the key describes. The special-case gates (claude-code, codex, pi)
/// probe a binary that is not their registry primary (`claude-agent-acp`,
/// `codex-acp`, `pi-acp`), so an adapter override must not shadow or stand in
/// for the real CLI there; those gates ignore the override, matching spawn
/// resolution (npx-only warns it away) and discovery (monorepo#1065 skips
/// npx-only overrides). A valid applied override wins (and is what the probe
/// spawns — pi never gets here); an invalid one warns and falls through to
/// the auto-detection tiers. auggie's override is validated with checkAuggie
/// parity instead ([`resolve_auggie_override`]), falling through to
/// [`crate::auggie_discovery::find_auggie`].
fn resolve_probe_binary(
    provider_id: &str,
    override_path: Option<&str>,
) -> Option<std::ffi::OsString> {
    let command = match provider_id {
        "auggie" => {
            if let Some(p) = override_path.and_then(resolve_auggie_override) {
                return Some(p.into_os_string());
            }
            return crate::auggie_discovery::find_auggie().map(std::path::PathBuf::into_os_string);
        }
        "claude-code" => "claude",
        "codex" => "codex",
        "opencode" => "opencode",
        "droid" => "droid",
        "grok" => "grok",
        "pi" => "pi",
        _ => return None,
    };
    let override_applies =
        intent_providers::find_provider(provider_id).is_some_and(|cfg| cfg.command == command);
    intent_providers::find_provider_binary(
        provider_id,
        command,
        override_path.filter(|_| override_applies),
    )
    .map(std::path::PathBuf::into_os_string)
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

    #[allow(clippy::option_option)] // outer = cache freshness, inner = the cached tri-state
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
/// within TTL serve the cached outcome without touching the filesystem;
/// otherwise the binary is resolved and the probe runs single-flighted
/// (concurrent callers — forced or not — share the in-flight result).
/// Not-installed providers short-circuit to `None` without probing or
/// caching, so an install is picked up immediately; an uninstall — or an
/// `override_path` change to an already-cached provider — within the
/// TTL serves the stale cached value until expiry (accepted — the next
/// expired or forced read reports `None`).
async fn resolve_auth_status(
    provider_id: &'static str,
    force: bool,
    override_path: Option<String>,
) -> Option<bool> {
    if !force {
        if let Some(cached) = cache().fresh(provider_id) {
            return cached;
        }
    }
    let program = resolve_probe_binary(provider_id, override_path.as_deref())?;
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
///
/// `provider_paths` carries the `providers.paths` overrides (plus the
/// transport-applied `context.auggiePath` precedence under `auggie`), read
/// by the caller because the settings live above this function — the same
/// threading the discovery surface uses (monorepo#1065). The install gate
/// applies each provider's override so overridden providers get probed
/// (monorepo#1086); an empty map preserves auto-detection-only behavior.
///
/// # Errors
///
/// Returns an error string when `provider_id` names an unknown provider.
pub async fn provider_auth_status<S: std::hash::BuildHasher>(
    provider_id: Option<&str>,
    force: bool,
    provider_paths: &HashMap<String, String, S>,
) -> Result<Value, String> {
    let selected: Vec<&'static str> = match provider_id {
        Some(requested) => match AUTH_PROBE_PROVIDERS.iter().find(|id| **id == requested) {
            Some(id) => vec![id],
            None => return Err(format!("Unknown providerId: {requested}")),
        },
        None => AUTH_PROBE_PROVIDERS.to_vec(),
    };
    // JoinSet (not bare `tokio::spawn`) so dropping this future — client
    // disconnect / request cancellation — aborts in-flight probe tasks
    // instead of leaving CLIs/ACP probes running in the background.
    let mut set = tokio::task::JoinSet::new();
    for (index, id) in selected.iter().enumerate() {
        let id: &'static str = id;
        let override_path = provider_paths.get(override_key(id)).cloned();
        set.spawn(async move { (index, resolve_auth_status(id, force, override_path).await) });
    }
    // A panicked probe task degrades to unknown rather than failing the
    // whole status call; response order stays fixed regardless of
    // completion order.
    let mut statuses: Vec<Option<bool>> = vec![None; selected.len()];
    while let Some(joined) = set.join_next().await {
        if let Ok((index, authenticated)) = joined {
            statuses[index] = authenticated;
        }
    }
    let providers: Vec<Value> = selected
        .iter()
        .zip(statuses)
        .map(|(id, authenticated)| json!({ "id": id, "authenticated": authenticated }))
        .collect();
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

    /// auggie has no bespoke probe any more: it rides the registry
    /// `auth_check_args` (`token print`) through the generic exit-code arm of
    /// [`check_provider_auth_cli`], whose stdout/stderr are nulled — the
    /// command's output is the auth session secret.
    #[test]
    fn auggie_probes_via_registry_auth_check_args() {
        let auggie = intent_providers::find_provider("auggie").expect("auggie in registry");
        assert_eq!(auggie.auth_check_args, Some(&["token", "print"][..]));
    }

    /// Behavioral pin for auggie's routing: `check_provider_auth_cli` under
    /// `provider_id: "auggie"` must land on the generic exit-code arm — the
    /// outcome follows the exit status alone, and the stub's stdout (standing
    /// in for the auth session secret `auggie token print` emits) never
    /// influences it. A future refactor that gave auggie a capturing arm would
    /// break this test.
    #[cfg(unix)]
    #[tokio::test]
    async fn auggie_probe_rides_generic_exit_code_arm() {
        use std::os::unix::fs::PermissionsExt;
        let dir = unique_temp_dir("auggie-exit-code");
        // Both stubs print secret-shaped output; only the exit code differs.
        for (name, exit, expected) in [
            ("auggie-in", 0, CliAuthProbe::Authenticated),
            ("auggie-out", 1, CliAuthProbe::NotAuthenticated),
        ] {
            let stub = dir.path().join(name);
            std::fs::write(
                &stub,
                format!("#!/bin/sh\necho 'sess-secret-must-not-be-read'\nexit {exit}\n"),
            )
            .unwrap();
            std::fs::set_permissions(&stub, std::fs::Permissions::from_mode(0o755)).unwrap();
            let probe =
                check_provider_auth_cli("auggie", stub.as_os_str(), &["token", "print"]).await;
            assert_eq!(probe, expected, "stub {name}");
        }
    }

    /// monorepo#1863 regression: the probe child's PATH must carry the
    /// resolved binary's own directory so an nvm-installed CLI's
    /// `#!/usr/bin/env node` shebang resolves the sibling `node`. The stub
    /// execs a bare-named sibling that only resolves through that prepended
    /// directory (it exists nowhere on the inherited PATH) — before the fix
    /// the spawn exited 127 and the probe reported `NotAuthenticated`.
    #[cfg(unix)]
    #[tokio::test]
    async fn probe_child_path_carries_resolved_binary_dir() {
        use std::os::unix::fs::PermissionsExt;
        let dir = unique_temp_dir("path-prepend");
        let sibling = dir.path().join("intent-test-sibling-node");
        std::fs::write(&sibling, "#!/bin/sh\nexit 0\n").unwrap();
        std::fs::set_permissions(&sibling, std::fs::Permissions::from_mode(0o755)).unwrap();
        let stub = dir.path().join("auggie");
        std::fs::write(&stub, "#!/bin/sh\nexec intent-test-sibling-node\n").unwrap();
        std::fs::set_permissions(&stub, std::fs::Permissions::from_mode(0o755)).unwrap();
        let probe = check_provider_auth_cli("auggie", stub.as_os_str(), &["token", "print"]).await;
        assert_eq!(probe, CliAuthProbe::Authenticated);
    }

    /// monorepo#1863 regression: exit 127 is a command-resolution failure —
    /// the probe never ran the CLI's auth check — so it maps to Failed
    /// (`authenticated: null` on the wire), never to `NotAuthenticated`. Pinned
    /// on both exit-code arms (generic and opencode).
    #[cfg(unix)]
    #[tokio::test]
    async fn exit_127_is_probe_failure_not_logged_out() {
        use std::os::unix::fs::PermissionsExt;
        let dir = unique_temp_dir("exit-127");
        let stub = dir.path().join("stub-cli");
        std::fs::write(&stub, "#!/bin/sh\nexit 127\n").unwrap();
        std::fs::set_permissions(&stub, std::fs::Permissions::from_mode(0o755)).unwrap();
        for provider_id in ["auggie", "opencode"] {
            let probe = check_provider_auth_cli(provider_id, stub.as_os_str(), &["status"]).await;
            assert_eq!(probe, CliAuthProbe::Failed, "provider {provider_id}");
            assert_eq!(probe.auth_status(), None, "provider {provider_id}");
        }
    }

    /// Reads the PATH env `probe_command` sets on the child (no spawn).
    fn probe_command_child_path(program: &str) -> String {
        probe_command(std::ffi::OsStr::new(program))
            .as_std()
            .get_envs()
            .find(|(k, _)| *k == std::ffi::OsStr::new("PATH"))
            .and_then(|(_, v)| v)
            .expect("probe_command sets PATH")
            .to_string_lossy()
            .into_owned()
    }

    /// monorepo#1863 follow-up (PR review): a relative `providers.paths.*`
    /// override is a valid resolution, but `enhanced_path` only prepends the
    /// parent dir of an absolute path — so `probe_command` must lexically
    /// absolutize a path-shaped program first. Pinned by inspecting the
    /// child's PATH env directly (no spawn).
    #[test]
    fn probe_command_absolutizes_relative_program_for_path() {
        let path_env = probe_command_child_path("rel-dir/fake-cli");
        let expected_dir = std::path::absolute("rel-dir").expect("absolutize test dir");
        let first = path_env
            .split(if cfg!(windows) { ';' } else { ':' })
            .next()
            .expect("PATH has entries");
        assert_eq!(std::path::Path::new(first), expected_dir);
    }

    /// Counterpart pin: a bare program name (doctor's fallback when
    /// discovery has no resolved path) is NOT absolutized — that would put
    /// the process CWD at the head of the child's PATH, letting a
    /// CWD-resident binary shadow the real one.
    #[test]
    fn probe_command_bare_name_does_not_prepend_cwd() {
        let path_env = probe_command_child_path("fake-cli");
        let cwd = std::env::current_dir().expect("cwd");
        let first = path_env
            .split(if cfg!(windows) { ';' } else { ':' })
            .next()
            .expect("PATH has entries");
        assert_ne!(std::path::Path::new(first), cwd);
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
        let err = provider_auth_status(Some("not-a-provider"), false, &HashMap::new())
            .await
            .expect_err("unknown provider must error");
        assert!(err.contains("not-a-provider"), "{err}");
    }

    #[tokio::test]
    async fn scoped_result_carries_exactly_one_entry() {
        // Runs the real resolve path: on hosts without grok the probe is
        // skipped (authenticated: null); with grok installed, `grok models`
        // actually runs, bounded by the probe timeout. The assertions are
        // shape-only so the test passes in both environments. The spawned
        // Node CLI skips its compile cache (which would otherwise leave
        // `node-compile-cache/` in TMPDIR) via the crate-wide ctor in
        // `src/tests.rs` that exports NODE_DISABLE_COMPILE_CACHE=1.
        let result = provider_auth_status(Some("grok"), false, &HashMap::new())
            .await
            .expect("grok is a known provider");
        let providers = result["providers"].as_array().expect("providers array");
        assert_eq!(providers.len(), 1);
        assert_eq!(providers[0]["id"], "grok");
        assert!(
            providers[0]["authenticated"].is_boolean() || providers[0]["authenticated"].is_null()
        );
    }

    /// A fresh RAII temp dir; removed on drop (including on panic). Set
    /// `INTENTD_TEST_KEEP_TMP` (non-empty) to keep it around for debugging.
    fn unique_temp_dir(tag: &str) -> tempfile::TempDir {
        let mut dir = tempfile::Builder::new()
            .prefix(&format!("intent-provider-auth-{tag}-"))
            .tempdir()
            .expect("create test tempdir");
        if std::env::var_os("INTENTD_TEST_KEEP_TMP").is_some_and(|v| !v.is_empty()) {
            dir.disable_cleanup(true);
        }
        dir
    }

    #[cfg(unix)]
    fn make_executable(path: &std::path::Path) {
        use std::os::unix::fs::PermissionsExt;
        std::fs::write(path, "#!/bin/sh\nexit 0\n").unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    /// Pins the gate's `providers.paths` key mapping to spawn resolution
    /// (`primary_binary_provider_id`): every probe-able provider owns its own
    /// primary today, so the key is the provider id itself. If a probe-able
    /// provider ever remaps (the way unsloth rides opencode), this test forces
    /// the gate to be revisited alongside spawn resolution.
    #[test]
    fn probe_override_keys_match_spawn_resolution() {
        for id in AUTH_PROBE_PROVIDERS {
            let cfg = intent_providers::find_provider(id).expect("probe provider in registry");
            assert_eq!(cfg.primary_binary_provider_id(), *id, "provider {id}");
            assert_eq!(override_key(id), *id, "provider {id}");
        }
    }

    /// monorepo#1086 regression: a valid `providers.paths` override resolves
    /// the install gate even when nothing is auto-detectable, so the provider
    /// gets probed instead of reporting `authenticated: null` unconditionally.
    #[cfg(unix)]
    #[test]
    fn valid_override_resolves_install_gate() {
        let dir = unique_temp_dir("valid-override");
        let bin = dir.path().join("droid");
        make_executable(&bin);
        let resolved = resolve_probe_binary("droid", Some(bin.to_str().unwrap()));
        assert_eq!(resolved, Some(bin.into_os_string()));
    }

    /// An invalid override (missing / relative / non-executable) keeps the
    /// pre-override fall-through semantics: the gate resolves exactly what it
    /// would with no override at all.
    #[test]
    fn invalid_override_falls_through_to_auto_detection() {
        let baseline = resolve_probe_binary("droid", None);
        for bad in [
            "",
            "   ",
            "relative/droid",
            "/nonexistent/intent-test/droid",
        ] {
            assert_eq!(
                resolve_probe_binary("droid", Some(bad)),
                baseline,
                "{bad:?}"
            );
        }
    }

    /// The special-case gates (claude-code, codex, pi) probe a binary that is
    /// not the registry primary their `providers.paths` key describes, so a
    /// valid adapter override must neither open the gate nor shadow a
    /// PATH-resolved real CLI — the gate resolves exactly what no override
    /// would.
    #[cfg(unix)]
    #[test]
    fn special_case_gates_ignore_adapter_overrides() {
        for id in ["claude-code", "codex", "pi"] {
            let cfg = intent_providers::find_provider(id).expect("registry entry");
            assert_ne!(
                cfg.command, id,
                "{id}: gate command matches registry primary; drop it from this test"
            );
            let dir = unique_temp_dir(&format!("adapter-override-{id}"));
            let adapter = dir.path().join(cfg.command);
            make_executable(&adapter);
            let baseline = resolve_probe_binary(id, None);
            assert_eq!(
                resolve_probe_binary(id, Some(adapter.to_str().unwrap())),
                baseline,
                "{id}"
            );
        }
    }

    /// auggie's gate honors the threaded override with checkAuggie parity
    /// (existing file or symlink) and falls through to `find_auggie` on an
    /// invalid value.
    #[test]
    fn auggie_override_matches_check_auggie_semantics() {
        let dir = unique_temp_dir("auggie-override");
        let bin = dir.path().join("auggie");
        std::fs::write(&bin, "").unwrap();
        assert_eq!(
            resolve_probe_binary("auggie", Some(bin.to_str().unwrap())),
            Some(bin.into_os_string())
        );
        let baseline = resolve_probe_binary("auggie", None);
        for bad in ["", "   ", "/nonexistent/intent-test/auggie"] {
            assert_eq!(
                resolve_probe_binary("auggie", Some(bad)),
                baseline.clone(),
                "{bad:?}"
            );
        }
    }
}
