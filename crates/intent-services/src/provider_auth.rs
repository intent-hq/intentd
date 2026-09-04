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
//! - **claude-code** — two-stage (intent-hq/intent#3941): the real `claude`
//!   CLI with its registry `auth_check_args`: an explicit JSON `loggedIn`
//!   boolean is authoritative, regardless of the auth command's exit code,
//!   and skips the adapter; a logged-in report's identity metadata (email /
//!   orgName / subscriptionType) rides the cached verdict and the RPC
//!   response. Only inconclusive CLI outcomes fall back to an
//!   ACP probe via the pinned claude-agent-acp npx adapter. The
//!   fallback can only demote or stay unknown: explicit auth-required error
//!   ⇒ `false`; anything else — including a served model catalog, which the
//!   adapter returns without credentials — ⇒ unknown. Only the CLI probe
//!   can confirm `true` ([`crate::provider_models`]).
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
//! `intentd doctor` shares [`check_provider_auth_cli`] (the exit-code + JSON/model
//! CLI probe) so the doctor report and the RPC cannot drift. claude-code's
//! ACP fallback lives only here, above that shared helper — no doctor drift
//! is possible because doctor never prints an auth annotation for npx-only
//! providers (`report_provider_availability` skips their auth probe;
//! claude-code's doctor line reports npx availability only).

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
    "antigravity",
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
    /// The CLI reported an authenticated state (provider-specific signal).
    Authenticated,
    /// The CLI reported an unauthenticated state (provider-specific signal).
    NotAuthenticated,
    /// The probe ran but produced no auth signal (e.g. missing JSON boolean
    /// or grok: exit 0, no markers, no models).
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

/// Identity metadata a provider's auth probe can report alongside its
/// verdict (today: claude-code's `auth status` JSON — see
/// [`check_claude_code_auth_cli`]). Each field is whitespace-trimmed; an
/// all-absent identity is never constructed ([`claude_identity`] returns
/// `None` instead), so a stored/wire identity always carries at least one
/// field.
#[derive(Debug, Clone, PartialEq, Eq)]
struct AuthIdentity {
    email: Option<String>,
    org_name: Option<String>,
    subscription_type: Option<String>,
}

impl AuthIdentity {
    fn is_empty(&self) -> bool {
        self.email.is_none() && self.org_name.is_none() && self.subscription_type.is_none()
    }

    /// The wire projection for `host.providerAuthStatus`: an
    /// `identity: { email?, orgName?, subscriptionType? }` object carrying
    /// only the captured fields.
    fn to_json(&self) -> Value {
        let mut object = serde_json::Map::new();
        if let Some(email) = &self.email {
            object.insert("email".into(), Value::String(email.clone()));
        }
        if let Some(org_name) = &self.org_name {
            object.insert("orgName".into(), Value::String(org_name.clone()));
        }
        if let Some(subscription_type) = &self.subscription_type {
            object.insert(
                "subscriptionType".into(),
                Value::String(subscription_type.clone()),
            );
        }
        Value::Object(object)
    }
}

/// One provider's resolved auth outcome: the tri-state `authenticated`
/// verdict plus the optional identity metadata that rides it — through the
/// cache entry and onto the wire entry alike.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct AuthVerdict {
    authenticated: Option<bool>,
    identity: Option<AuthIdentity>,
}

impl AuthVerdict {
    /// A verdict with no identity metadata — every provider except
    /// claude-code, and claude-code's non-logged-in outcomes.
    fn plain(authenticated: Option<bool>) -> Self {
        Self {
            authenticated,
            identity: None,
        }
    }
}

/// Best-effort CLI authentication probe for an installed provider: run its
/// `auth_check_args` with a short timeout. Most providers signal auth via the
/// exit code (0 ⇒ authenticated); Claude's JSON `loggedIn` boolean is trusted
/// instead, with missing/malformed output left unknown. grok's `models` probe
/// exits 0 in both auth states, so its stdout is parsed for explicit auth markers; and
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
        "claude-code" => {
            check_claude_code_auth_cli(program, auth_check_args)
                .await
                .probe
        }
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

/// Outcome of the claude-code CLI probe: the shared probe verdict plus the
/// identity metadata a logged-in JSON report carries.
struct ClaudeCliStatus {
    probe: CliAuthProbe,
    identity: Option<AuthIdentity>,
}

/// The claude-code arm of [`check_provider_auth_cli`], additionally
/// capturing identity metadata. The parse is strict — only a top-level
/// object with a boolean `loggedIn` counts as machine-readable output — and
/// identity is kept only from a logged-in report: a logged-out report's
/// identity fields are dropped, and inconclusive outcomes never carry
/// identity. The shared wrapper discards the identity so `intentd doctor`
/// keeps its verdict-only view.
async fn check_claude_code_auth_cli(program: &OsStr, auth_check_args: &[&str]) -> ClaudeCliStatus {
    let mut cmd = probe_command(program);
    cmd.args(auth_check_args)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true);
    match tokio::time::timeout(CLI_AUTH_TIMEOUT, cmd.output()).await {
        Ok(Ok(output)) if could_not_run(output.status) || output.status.code().is_none() => {
            ClaudeCliStatus {
                probe: CliAuthProbe::Failed,
                identity: None,
            }
        }
        Ok(Ok(output)) => {
            // Never log the payload: auth status may include account details.
            let parsed = serde_json::from_slice::<Value>(&output.stdout).ok();
            let logged_in = parsed
                .as_ref()
                .and_then(|value| value.get("loggedIn").and_then(Value::as_bool));
            match logged_in {
                Some(true) => ClaudeCliStatus {
                    probe: CliAuthProbe::Authenticated,
                    identity: parsed.as_ref().and_then(claude_identity),
                },
                Some(false) => ClaudeCliStatus {
                    probe: CliAuthProbe::NotAuthenticated,
                    identity: None,
                },
                None if output.status.success() => ClaudeCliStatus {
                    probe: CliAuthProbe::StatusUnknown,
                    identity: None,
                },
                None => ClaudeCliStatus {
                    probe: CliAuthProbe::Failed,
                    identity: None,
                },
            }
        }
        Ok(Err(_)) => ClaudeCliStatus {
            probe: CliAuthProbe::Failed,
            identity: None,
        },
        Err(_) => ClaudeCliStatus {
            probe: CliAuthProbe::TimedOut,
            identity: None,
        },
    }
}

/// Identity fields of a parsed `claude auth status` object — `email`,
/// `orgName`, `subscriptionType`: strings only, whitespace-trimmed, empty
/// values dropped. `None` when no field survives, so an empty identity is
/// never cached or serialized.
fn claude_identity(value: &Value) -> Option<AuthIdentity> {
    let field = |key: &str| {
        value
            .get(key)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_owned)
    };
    let identity = AuthIdentity {
        email: field("email"),
        org_name: field("orgName"),
        subscription_type: field("subscriptionType"),
    };
    (!identity.is_empty()).then_some(identity)
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
/// npx adapter). CLI-probed providers (auggie, codex, opencode, grok) share
/// [`check_provider_auth_cli`] with `intentd doctor`; claude-code runs the
/// identity-capturing [`check_claude_code_auth_cli`] variant of that same
/// probe, then [`claude_code_auth_verdict`] decides whether the ACP fallback
/// probe is consulted. Only claude-code can attach identity metadata today.
///
/// `override_path` is the raw `providers.paths` value for the provider's
/// [`override_key`]. `program` (the install-gate binary from
/// [`resolve_probe_binary`]) already reflects it where it applies; the
/// claude-code arm additionally resolves it as the ADAPTER override
/// ([`intent_providers::resolve_npx_only_override`]) so the ACP fallback probe
/// runs the same adapter binary a session spawn would (monorepo#4352).
async fn probe_provider(
    provider_id: &'static str,
    program: std::ffi::OsString,
    override_path: Option<&str>,
) -> AuthVerdict {
    match provider_id {
        "auggie" | "codex" | "opencode" | "grok" => {
            let args = intent_providers::find_provider(provider_id)
                .and_then(|cfg| cfg.auth_check_args)
                .unwrap_or_default();
            if args.is_empty() {
                return AuthVerdict::default();
            }
            AuthVerdict::plain(
                check_provider_auth_cli(provider_id, &program, args)
                    .await
                    .auth_status(),
            )
        }
        "claude-code" => {
            let args = intent_providers::find_provider(provider_id)
                .and_then(|cfg| cfg.auth_check_args)
                .unwrap_or_default();
            if args.is_empty() {
                return AuthVerdict::default();
            }
            let status = check_claude_code_auth_cli(&program, args).await;
            let adapter_override =
                intent_providers::resolve_npx_only_override(provider_id, override_path);
            let authenticated = claude_code_auth_verdict(status.probe, move || {
                crate::provider_models::probe_claude_code_auth(adapter_override)
            })
            .await;
            AuthVerdict {
                authenticated,
                identity: status.identity,
            }
        }
        "droid" => {
            AuthVerdict::plain(crate::provider_models::probe_droid_auth(program.into()).await)
        }
        "pi" => AuthVerdict::plain(crate::provider_models::probe_pi_auth().await),
        "antigravity" => {
            AuthVerdict::plain(crate::provider_models::probe_antigravity_auth(program.into()).await)
        }
        _ => AuthVerdict::default(),
    }
}

/// Two-stage claude-code auth verdict (intent-hq/intent#3941). An explicit
/// `loggedIn` boolean from `claude auth status` is trusted as-is, without
/// spawning the adapter. Unlike the original #3941 fallback, a reported
/// logout is not hidden to accommodate suspected CLI false negatives; the
/// explicit live provider test can establish working authentication instead.
/// Only inconclusive outcomes (`StatusUnknown` / `Failed` / `TimedOut`) consult
/// `acp_fallback` — [`crate::provider_models::probe_claude_code_auth`], the
/// pinned claude-agent-acp adapter probe. The fallback can only demote to
/// `Some(false)` (the adapter's explicit auth-required error) or stay
/// unknown — never confirm `Some(true)`, because the adapter serves its
/// model catalog without credentials and there is no cheap auth-exercising
/// handshake step. The fallback is injected so the mapping is unit-testable
/// without spawning real CLIs.
async fn claude_code_auth_verdict<F, Fut>(cli: CliAuthProbe, acp_fallback: F) -> Option<bool>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Option<bool>>,
{
    match cli {
        CliAuthProbe::Authenticated => Some(true),
        CliAuthProbe::NotAuthenticated => Some(false),
        CliAuthProbe::StatusUnknown | CliAuthProbe::Failed | CliAuthProbe::TimedOut => {
            acp_fallback().await
        }
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
/// for the real CLI there; those gates ignore the override for the CLI check
/// (claude-code's ADAPTER override is applied separately, in
/// [`probe_provider`]'s ACP fallback — monorepo#4352). A valid applied
/// override wins (and is what the probe spawns — pi never gets here); an
/// invalid one warns and falls through to the auto-detection tiers. auggie's
/// override is validated with checkAuggie
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
        "antigravity" => "antigravity-acp",
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
    entries: Mutex<HashMap<&'static str, (Instant, AuthVerdict)>>,
    inflight: Mutex<HashMap<&'static str, Arc<OnceCell<AuthVerdict>>>>,
    /// Per-provider runtime-demotion counter ([`AuthStatusCache::demote`]).
    /// A probe captures the counter when it starts and its store is dropped
    /// if a demotion landed in between ([`AuthStatusCache::store_probe`]),
    /// so an already-in-flight probe's older verdict cannot overwrite the
    /// authoritative runtime auth failure (PR #1650 review).
    demotions: Mutex<HashMap<&'static str, u64>>,
}

impl AuthStatusCache {
    fn new() -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            inflight: Mutex::new(HashMap::new()),
            demotions: Mutex::new(HashMap::new()),
        }
    }

    fn fresh(&self, provider_id: &str) -> Option<AuthVerdict> {
        let entries = self.entries.lock().expect("auth cache poisoned");
        let (at, verdict) = entries.get(provider_id)?;
        (at.elapsed() < AUTH_CACHE_TTL).then(|| verdict.clone())
    }

    fn store(&self, provider_id: &'static str, verdict: AuthVerdict) {
        self.entries
            .lock()
            .expect("auth cache poisoned")
            .insert(provider_id, (Instant::now(), verdict));
    }

    /// The provider's current demotion counter — captured by a probe before
    /// it runs so [`AuthStatusCache::store_probe`] can detect a concurrent
    /// runtime demotion.
    fn demotion_epoch(&self, provider_id: &str) -> u64 {
        self.demotions
            .lock()
            .expect("auth demotions poisoned")
            .get(provider_id)
            .copied()
            .unwrap_or(0)
    }

    /// Store a probe outcome UNLESS a runtime demotion/promotion landed since
    /// `epoch` was captured: the runtime observed an authoritative outcome
    /// from the provider's adapter, which supersedes a probe that started
    /// earlier. The demotions lock is held across the store so a demotion
    /// cannot slip between the check and the write (lock order: demotions →
    /// entries, same as [`AuthStatusCache::demote`]).
    ///
    /// Returns the verdict callers must serve: the stored probe outcome, or —
    /// when superseded — the authoritative cached verdict that displaced it,
    /// so joined `providerAuthStatus` responses never expose a stale verdict
    /// (or identity metadata) that a mid-flight demotion just cleared.
    fn store_probe(
        &self,
        provider_id: &'static str,
        verdict: AuthVerdict,
        epoch: u64,
    ) -> AuthVerdict {
        let demotions = self.demotions.lock().expect("auth demotions poisoned");
        if demotions.get(provider_id).copied().unwrap_or(0) != epoch {
            tracing::debug!(
                provider = provider_id,
                "auth probe outcome discarded — an authoritative runtime verdict superseded it"
            );
            return self.fresh(provider_id).unwrap_or_default();
        }
        self.store(provider_id, verdict.clone());
        verdict
    }

    /// Harden the provider's verdict to `Some(false)` after an authoritative
    /// runtime auth failure and bump the demotion counter so any probe
    /// already in flight discards its (older) outcome instead of overwriting
    /// this one. Any cached identity is cleared — the runtime auth failure
    /// invalidates probe-time identity metadata. Lock order: demotions →
    /// entries (see [`AuthStatusCache::store_probe`]).
    fn demote(&self, provider_id: &'static str) {
        let mut demotions = self.demotions.lock().expect("auth demotions poisoned");
        *demotions.entry(provider_id).or_insert(0) += 1;
        self.store(provider_id, AuthVerdict::plain(Some(false)));
    }

    /// Harden the provider's verdict to `Some(true)` after the runtime
    /// observed an authoritative end-to-end success (a live test prompt
    /// answered). Bumps the same counter as [`AuthStatusCache::demote`] so a
    /// probe already in flight discards its (older) outcome instead of
    /// overwriting this one; lock order: demotions → entries. Cached identity
    /// is preserved on the refreshed entry only while the entry is still
    /// *fresh* — a test-prompt success proves the same session still works but
    /// reports no identity of its own, and an expired entry's identity may
    /// belong to a previous account (CLI account switch), so it is never
    /// revived past the TTL.
    fn promote(&self, provider_id: &'static str) {
        let mut demotions = self.demotions.lock().expect("auth demotions poisoned");
        *demotions.entry(provider_id).or_insert(0) += 1;
        let identity = self.fresh(provider_id).and_then(|verdict| verdict.identity);
        self.store(
            provider_id,
            AuthVerdict {
                authenticated: Some(true),
                identity,
            },
        );
    }

    fn join_inflight(&self, provider_id: &'static str) -> Arc<OnceCell<AuthVerdict>> {
        self.inflight
            .lock()
            .expect("auth inflight poisoned")
            .entry(provider_id)
            .or_default()
            .clone()
    }

    fn finish_inflight(&self, provider_id: &str, cell: &Arc<OnceCell<AuthVerdict>>) {
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

/// Resolve the cache key a (possibly alias/legacy) provider id gates and
/// demotes under: persisted legacy default aliases (`default` / `acp` /
/// `augment`) and unknown ids spawn the catalog fallback provider
/// ([`intent_providers::provider_config`]), so the verdict must be keyed by
/// the id that actually runs, not the persisted alias — otherwise an
/// alias-backed demotion is a no-op and alias-backed creates stay permissive
/// (PR #1650 review). Known catalog ids map to themselves.
fn auth_cache_key(provider_id: &str) -> &'static str {
    intent_providers::provider_config(provider_id).id
}

/// Read-only view of one provider's *fresh* cached auth verdict:
/// `Some(false)` only when a still-fresh cache entry holds an explicit
/// not-authenticated probe outcome; `Some(true)` for a fresh authenticated
/// outcome; `None` when the cache entry is absent, expired, or holds an
/// inconclusive (unknown) outcome. Never triggers a probe — callers gating
/// on it (the agent create/delegate auth gate in [`crate::agent_ops`]) must
/// stay permissive on `None`. Legacy alias ids resolve to the catalog
/// fallback provider's verdict ([`auth_cache_key`]) — the provider an
/// alias-backed create would actually spawn. Identity metadata never
/// influences the gate — it is wire-only enrichment.
pub(crate) fn cached_auth_verdict(provider_id: &str) -> Option<bool> {
    cache()
        .fresh(auth_cache_key(provider_id))
        .and_then(|verdict| verdict.authenticated)
}

/// The shared "provider is not authenticated" message body: names the
/// provider and the catalog login remedy ([`intent_providers::login_command`]);
/// for claude-code it also spells out the desktop-app caveat — a Claude
/// desktop-app sign-in does not carry over to the CLI credential chain
/// (intent-hq/intent#3941). Used by both the create/delegate gate in
/// [`crate::agent_ops`] and the runtime session/prompt auth-failure mapping
/// so the two surfaces stay word-for-word consistent.
pub(crate) fn not_authenticated_message(provider_id: &str) -> String {
    let display = intent_providers::provider_config(provider_id).display_name;
    let login_cmd = intent_providers::login_command(provider_id);
    let caveat = if provider_id == "claude-code" {
        " Note: signing into the Claude desktop app does not carry over to the CLI — run \
         \"claude\" in a terminal, then \"/login\"."
    } else {
        ""
    };
    format!(
        "provider \"{provider_id}\" ({display}) is not authenticated — run \
         \"{login_cmd}\" in a terminal, then retry.{caveat}"
    )
}

/// Demote one provider's cached auth verdict to a hard `false` after the
/// runtime observed an authoritative auth-required error from its adapter
/// (session/new / session/load / session/prompt). This makes the
/// create/delegate gate in [`crate::agent_ops`] reject follow-up spawns for
/// the cache TTL instead of letting each one die on its first turn. Legacy
/// alias ids demote the catalog fallback provider they actually spawn
/// ([`auth_cache_key`]). The demotion also supersedes any probe already in
/// flight ([`AuthStatusCache::demote`]) so a stale probe outcome cannot
/// overwrite this authoritative `false`, and clears any cached identity
/// metadata with it.
pub(crate) fn demote_auth_verdict(provider_id: &str) {
    cache().demote(auth_cache_key(provider_id));
}

/// Promote one provider's cached auth verdict to a hard `true` after the
/// runtime observed an authoritative end-to-end success — a live test prompt
/// (`host.providerTestPrompt`) got a real answer from the adapter, which is
/// stronger evidence than any local probe. Legacy alias ids promote the
/// catalog fallback provider they actually spawn ([`auth_cache_key`]), and
/// the promotion supersedes any probe already in flight
/// ([`AuthStatusCache::promote`]) so a stale probe outcome cannot overwrite
/// this authoritative `true`. Cached identity metadata is preserved on the
/// refreshed entry.
pub(crate) fn promote_auth_verdict(provider_id: &str) {
    cache().promote(auth_cache_key(provider_id));
}

/// Test seam: plant a verdict in the process-wide auth cache so gate tests
/// are deterministic without spawning probes. Seed `None` to restore the
/// permissive cached-unknown state.
#[cfg(test)]
pub(crate) fn seed_auth_verdict_for_tests(provider_id: &'static str, value: Option<bool>) {
    cache().store(provider_id, AuthVerdict::plain(value));
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
) -> AuthVerdict {
    if !force {
        if let Some(cached) = cache().fresh(provider_id) {
            return cached;
        }
    }
    let Some(program) = resolve_probe_binary(provider_id, override_path.as_deref()) else {
        return AuthVerdict::default();
    };
    let cell = cache().join_inflight(provider_id);
    let verdict = cell
        .get_or_init(|| async {
            // Captured before the probe runs: a runtime demotion landing
            // while the probe is in flight supersedes its (older) outcome —
            // `store_probe` drops the store instead of overwriting the
            // authoritative hard `false` (PR #1650 review), and hands back
            // the superseding cached verdict so every joined caller serves
            // the authoritative outcome, not the stale probe result.
            let epoch = cache().demotion_epoch(provider_id);
            let verdict = probe_provider(provider_id, program, override_path.as_deref()).await;
            cache().store_probe(provider_id, verdict, epoch)
        })
        .await
        .clone();
    cache().finish_inflight(provider_id, &cell);
    verdict
}

/// `host.providerAuthStatus` (§5.14): probe auth status for every probe-able
/// provider (or one, when `provider_id` is given) and return
/// `{ providers: [{ id, authenticated, identity? }] }` with
/// `authenticated: true | false | null` (`null` = unknown / probe failed /
/// not installed) and the additive optional `identity` object present only
/// when the provider's probe captured identity metadata (today: claude-code's
/// logged-in JSON report). Probes run in parallel, each bounded by its own
/// timeout. An unknown `provider_id` is an invalid-params error (`-32602`).
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
    let mut statuses: Vec<AuthVerdict> = vec![AuthVerdict::default(); selected.len()];
    while let Some(joined) = set.join_next().await {
        if let Ok((index, verdict)) = joined {
            statuses[index] = verdict;
        }
    }
    let providers: Vec<Value> = selected
        .iter()
        .zip(statuses)
        .map(|(id, verdict)| auth_status_entry(id, &verdict))
        .collect();
    Ok(json!({ "providers": providers }))
}

/// One `providers[]` wire entry: `{ id, authenticated }` plus the additive
/// `identity` object when the probe captured identity metadata.
fn auth_status_entry(id: &str, verdict: &AuthVerdict) -> Value {
    let mut entry = json!({ "id": id, "authenticated": verdict.authenticated });
    if let Some(identity) = &verdict.identity {
        entry["identity"] = identity.to_json();
    }
    entry
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

    /// Claude's JSON boolean wins over both conventional and contradictory
    /// auth exit codes. Invalid output never turns process success into login.
    #[cfg(unix)]
    #[tokio::test]
    async fn claude_code_cli_parses_explicit_auth_status() {
        use std::os::unix::fs::PermissionsExt;
        let dir = unique_temp_dir("claude-json");
        let args = intent_providers::find_provider("claude-code")
            .unwrap()
            .auth_check_args
            .unwrap();
        assert_eq!(args, &["auth", "status"]);
        let payloads = [
            (
                r#"{"loggedIn":false,"authMethod":"none","apiProvider":"firstParty"}"#,
                Some(false),
            ),
            (r#"{ "loggedIn": true }"#, Some(true)),
            ("", None),
            ("not json", None),
            (r#"{"loggedIn":false"#, None),
            (r#"{"authMethod":"oauth","apiProvider":"firstParty"}"#, None),
            (r#"{"loggedIn":"false"}"#, None),
            (r#"{"loggedIn":"true"}"#, None),
            (r#"{"loggedIn":null}"#, None),
            (r#"{"loggedIn":1}"#, None),
            (r#"[{"loggedIn":true}]"#, None),
            ("true", None),
        ];
        for (index, (payload, logged_in)) in payloads.iter().enumerate() {
            for exit in [0, 1] {
                let stub = dir.path().join(format!("claude-{index}-{exit}"));
                std::fs::write(
                    &stub,
                    format!(
                        "#!/bin/sh\n[ \"$*\" = 'auth status' ] || exit 127\nprintf '%s\\n' '{payload}'\nexit {exit}\n"
                    ),
                )
                .unwrap();
                std::fs::set_permissions(&stub, std::fs::Permissions::from_mode(0o755)).unwrap();
                let probe = check_provider_auth_cli("claude-code", stub.as_os_str(), args).await;
                assert_eq!(probe.auth_status(), *logged_in, "case {index}, exit {exit}");
                if logged_in.is_some() {
                    let verdict = claude_code_auth_verdict(probe, || async {
                        panic!("explicit CLI status must not invoke the adapter")
                    })
                    .await;
                    assert_eq!(verdict, *logged_in);
                }
            }
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn claude_code_cli_execution_failures_are_unknown() {
        use std::os::unix::fs::PermissionsExt;
        let dir = unique_temp_dir("claude-failure");
        let stub = dir.path().join("claude");
        let args = &["auth", "status"];
        let missing = check_provider_auth_cli("claude-code", stub.as_os_str(), args).await;
        assert_eq!(missing, CliAuthProbe::Failed);
        assert_eq!(missing.auth_status(), None);

        // Even a boolean printed before a command-resolution failure or signal
        // cannot establish that the CLI completed its auth check.
        for end in ["exit 127", "kill -TERM $$"] {
            std::fs::write(
                &stub,
                format!("#!/bin/sh\necho '{{\"loggedIn\":true}}'\n{end}\n"),
            )
            .unwrap();
            std::fs::set_permissions(&stub, std::fs::Permissions::from_mode(0o755)).unwrap();
            let probe = check_provider_auth_cli("claude-code", stub.as_os_str(), args).await;
            assert_eq!(probe, CliAuthProbe::Failed);
            assert_eq!(probe.auth_status(), None);
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn claude_code_cli_timeout_is_unknown() {
        use std::os::unix::fs::PermissionsExt;
        let dir = unique_temp_dir("claude-timeout");
        let stub = dir.path().join("claude");
        // exec avoids leaving a shell child behind when kill_on_drop fires.
        std::fs::write(
            &stub,
            "#!/bin/sh\necho '{\"loggedIn\":true}'\nexec sleep 60\n",
        )
        .unwrap();
        std::fs::set_permissions(&stub, std::fs::Permissions::from_mode(0o755)).unwrap();
        let probe =
            check_provider_auth_cli("claude-code", stub.as_os_str(), &["auth", "status"]).await;
        assert_eq!(probe, CliAuthProbe::TimedOut);
        assert_eq!(probe.auth_status(), None);
    }

    /// Identity metadata rides a logged-in JSON report only: strings are
    /// whitespace-trimmed with empty/non-string fields dropped, a logged-out
    /// report's identity fields are discarded, and a bare logged-in report
    /// carries no identity object at all.
    #[cfg(unix)]
    #[tokio::test]
    async fn claude_code_cli_captures_identity_only_when_logged_in() {
        use std::os::unix::fs::PermissionsExt;
        let dir = unique_temp_dir("claude-identity");
        let args = &["auth", "status"];
        let full = AuthIdentity {
            email: Some("dev@example.com".into()),
            org_name: Some("Example Org".into()),
            subscription_type: Some("max".into()),
        };
        let trimmed = AuthIdentity {
            email: None,
            org_name: Some("Example Org".into()),
            subscription_type: None,
        };
        let cases: [(&str, CliAuthProbe, Option<AuthIdentity>); 5] = [
            (
                r#"{"loggedIn":true,"email":"dev@example.com","orgName":"Example Org","subscriptionType":"max"}"#,
                CliAuthProbe::Authenticated,
                Some(full),
            ),
            (
                r#"{"loggedIn":true,"email":"   ","orgName":" Example Org ","subscriptionType":42}"#,
                CliAuthProbe::Authenticated,
                Some(trimmed),
            ),
            (r#"{"loggedIn":true}"#, CliAuthProbe::Authenticated, None),
            (
                r#"{"loggedIn":false,"email":"dev@example.com","orgName":"Example Org"}"#,
                CliAuthProbe::NotAuthenticated,
                None,
            ),
            (
                r#"{"email":"dev@example.com"}"#,
                CliAuthProbe::StatusUnknown,
                None,
            ),
        ];
        for (index, (payload, probe, identity)) in cases.iter().enumerate() {
            let stub = dir.path().join(format!("claude-{index}"));
            std::fs::write(
                &stub,
                format!(
                    "#!/bin/sh\n[ \"$*\" = 'auth status' ] || exit 127\nprintf '%s\\n' '{payload}'\nexit 0\n"
                ),
            )
            .unwrap();
            std::fs::set_permissions(&stub, std::fs::Permissions::from_mode(0o755)).unwrap();
            let status = check_claude_code_auth_cli(stub.as_os_str(), args).await;
            assert_eq!(status.probe, *probe, "case {index}");
            assert_eq!(status.identity, *identity, "case {index}");
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

    /// Only inconclusive CLI results consult ACP. A served model catalog
    /// supplies no auth evidence, so the fallback stays unknown in that case.
    #[tokio::test]
    async fn claude_code_inconclusive_cli_consults_acp_fallback() {
        use std::sync::atomic::{AtomicBool, Ordering};
        for cli in [
            CliAuthProbe::StatusUnknown,
            CliAuthProbe::Failed,
            CliAuthProbe::TimedOut,
        ] {
            for fallback in [Some(false), None] {
                let called = AtomicBool::new(false);
                let verdict = claude_code_auth_verdict(cli, || async {
                    called.store(true, Ordering::SeqCst);
                    fallback
                })
                .await;
                assert!(
                    called.load(Ordering::SeqCst),
                    "{cli:?} must consult the fallback"
                );
                assert_eq!(verdict, fallback, "{cli:?} verdict follows the fallback");
            }
        }
    }

    /// Explicit login AND logout skip ACP, regardless of its possible outcome.
    #[tokio::test]
    async fn claude_code_explicit_cli_status_skips_acp_fallback() {
        for cli in [CliAuthProbe::Authenticated, CliAuthProbe::NotAuthenticated] {
            let verdict = claude_code_auth_verdict(cli, || async {
                panic!("explicit login/logout must not spawn the adapter")
            })
            .await;
            assert_eq!(verdict, cli.auth_status());
        }
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
        for provider in ["droid", "antigravity"] {
            let bin = dir.path().join(provider);
            make_executable(&bin);
            let resolved = resolve_probe_binary(provider, Some(bin.to_str().unwrap()));
            assert_eq!(resolved, Some(bin.into_os_string()));
        }
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

    /// Runtime demotion (intent-hq/intent#3941): an authoritative
    /// auth-required failure hardens the cached verdict to `false` so the
    /// create/delegate gate rejects follow-up spawns for the cache TTL.
    /// Legacy alias ids (`acp` / `augment` / `default`) demote — and gate —
    /// under the catalog fallback provider they actually spawn, so an
    /// alias-backed demotion is not a silent no-op (PR #1650 review).
    #[test]
    fn demote_auth_verdict_hardens_probe_provider_and_canonicalizes_aliases() {
        // "pi": a provider no other test seeds, so mutating the
        // process-global cache here cannot race a parallel test that
        // consults the create/delegate gate for a real default provider.
        let prior = cache().fresh("pi");
        demote_auth_verdict("pi");
        assert_eq!(cached_auth_verdict("pi"), Some(false));
        cache().store("pi", prior.unwrap_or_default());

        // Alias/unknown ids resolve to the catalog fallback provider (the
        // one an alias-backed create actually spawns) for both the demotion
        // write and the gate read; known ids map to themselves. Pinned on
        // `auth_cache_key` directly — `demote_auth_verdict` and
        // `cached_auth_verdict` are one-line compositions over it, and
        // demoting the real fallback here would plant a hard `false` that
        // parallel tests could observe through the gate.
        let fallback = intent_providers::provider_config("acp").id;
        for alias in ["acp", "augment", "default", "not-a-provider"] {
            assert_eq!(auth_cache_key(alias), fallback, "{alias}");
        }
        assert_eq!(auth_cache_key("pi"), "pi");
        assert_eq!(auth_cache_key("claude-code"), "claude-code");
    }

    /// A probe already in flight when a runtime demotion lands must not
    /// overwrite the authoritative hard `false` with its older outcome:
    /// `store_probe` drops the store when the demotion epoch moved (PR
    /// #1650 review). A probe with an unmoved epoch still stores normally.
    /// Unique cache key (cache-level seams take any key) so the
    /// process-global mutation cannot race parallel tests; the public
    /// `demote_auth_verdict` composition over `auth_cache_key` is covered by
    /// the demotion test above.
    #[test]
    fn stale_probe_outcome_cannot_overwrite_runtime_demotion() {
        let key = "test-stale-probe-demote";

        // Probe captured its epoch, then a demotion landed mid-flight. The
        // superseded probe hands back the authoritative demoted verdict —
        // what every joined `providerAuthStatus` caller must serve — instead
        // of its own stale outcome.
        let epoch = cache().demotion_epoch(key);
        cache().demote(key);
        let served = cache().store_probe(key, AuthVerdict::plain(Some(true)), epoch);
        assert_eq!(
            cache().fresh(key),
            Some(AuthVerdict::plain(Some(false))),
            "stale probe stored"
        );
        assert_eq!(
            served,
            AuthVerdict::plain(Some(false)),
            "stale probe served"
        );

        // A fresh probe (epoch captured after the demotion) stores normally.
        let epoch = cache().demotion_epoch(key);
        let served = cache().store_probe(key, AuthVerdict::plain(Some(true)), epoch);
        assert_eq!(cache().fresh(key), Some(AuthVerdict::plain(Some(true))));
        assert_eq!(served, AuthVerdict::plain(Some(true)));
    }

    /// A live test-prompt success hardens the verdict to `true`, and its
    /// epoch bump supersedes a probe already in flight — the mirror image of
    /// the demotion test above. Unique cache key for the same parallel-test
    /// hygiene; `promote_auth_verdict` is the one-line composition of
    /// [`AuthStatusCache::promote`] over `auth_cache_key`, pinned above.
    #[test]
    fn promote_auth_verdict_hardens_verdict_and_supersedes_inflight_probe() {
        let key = "test-stale-probe-promote";

        let epoch = cache().demotion_epoch(key);
        cache().promote(key);
        assert_eq!(cache().fresh(key), Some(AuthVerdict::plain(Some(true))));
        // The stale probe (epoch captured before the promotion) is dropped,
        // and the superseded probe serves the authoritative hard `true`.
        let served = cache().store_probe(key, AuthVerdict::plain(Some(false)), epoch);
        assert_eq!(
            cache().fresh(key),
            Some(AuthVerdict::plain(Some(true))),
            "stale probe stored"
        );
        assert_eq!(served, AuthVerdict::plain(Some(true)), "stale probe served");
    }

    /// Promotion never revives identity metadata past the cache TTL: an
    /// expired entry's identity may belong to a previous account (CLI account
    /// switch after expiry), so a later test-prompt promotion refreshes the
    /// verdict to a hard `true` WITHOUT the stale identity. Unique cache key:
    /// this test owns it, so the process-global mutation cannot race parallel
    /// tests.
    #[test]
    fn promotion_never_revives_expired_identity() {
        let expired_at = Instant::now()
            .checked_sub(AUTH_CACHE_TTL + Duration::from_secs(1))
            .expect("test clock predates process start");
        cache().entries.lock().expect("auth cache poisoned").insert(
            "test-identity-expired-promote",
            (
                expired_at,
                AuthVerdict {
                    authenticated: Some(true),
                    identity: Some(AuthIdentity {
                        email: Some("old-account@example.com".into()),
                        org_name: Some("Old Org".into()),
                        subscription_type: Some("max".into()),
                    }),
                },
            ),
        );
        cache().promote("test-identity-expired-promote");
        assert_eq!(
            cache().fresh("test-identity-expired-promote"),
            Some(AuthVerdict::plain(Some(true)))
        );
    }

    /// Demotion clears the cached identity along with hardening the verdict
    /// (a runtime auth failure invalidates probe-time identity), while
    /// promotion preserves it (a test-prompt success proves the same session
    /// still works but reports no identity of its own). Unique cache keys:
    /// this test owns them, so the process-global mutation cannot race
    /// parallel tests.
    #[test]
    fn demotion_clears_identity_promotion_preserves_it() {
        let identity = AuthIdentity {
            email: Some("dev@example.com".into()),
            org_name: Some("Example Org".into()),
            subscription_type: Some("max".into()),
        };
        cache().store(
            "test-identity-demote",
            AuthVerdict {
                authenticated: Some(true),
                identity: Some(identity.clone()),
            },
        );
        cache().demote("test-identity-demote");
        assert_eq!(
            cache().fresh("test-identity-demote"),
            Some(AuthVerdict::plain(Some(false)))
        );

        cache().store(
            "test-identity-promote",
            AuthVerdict {
                authenticated: Some(false),
                identity: Some(identity.clone()),
            },
        );
        cache().promote("test-identity-promote");
        assert_eq!(
            cache().fresh("test-identity-promote"),
            Some(AuthVerdict {
                authenticated: Some(true),
                identity: Some(identity),
            })
        );
    }

    /// The wire entry carries the additive `identity` object only when the
    /// probe captured identity metadata, and the object carries only the
    /// captured fields.
    #[test]
    fn auth_status_entry_carries_identity_only_when_captured() {
        let plain = auth_status_entry("codex", &AuthVerdict::plain(Some(true)));
        assert_eq!(plain, json!({ "id": "codex", "authenticated": true }));
        let unknown = auth_status_entry("droid", &AuthVerdict::default());
        assert_eq!(unknown, json!({ "id": "droid", "authenticated": null }));

        let partial = AuthVerdict {
            authenticated: Some(true),
            identity: Some(AuthIdentity {
                email: Some("dev@example.com".into()),
                org_name: None,
                subscription_type: None,
            }),
        };
        assert_eq!(
            auth_status_entry("claude-code", &partial),
            json!({
                "id": "claude-code",
                "authenticated": true,
                "identity": { "email": "dev@example.com" }
            })
        );
    }

    /// The shared login message names the provider, carries the catalog
    /// login command, and appends the desktop-app caveat only for
    /// claude-code — one builder keeps the gate and the runtime seams
    /// word-for-word consistent.
    #[test]
    fn not_authenticated_message_names_provider_and_login_remedy() {
        let claude = not_authenticated_message("claude-code");
        assert!(claude.contains("\"claude-code\""), "{claude}");
        assert!(
            claude.contains(&intent_providers::login_command("claude-code")),
            "{claude}"
        );
        assert!(claude.contains("desktop app"), "{claude}");

        let droid = not_authenticated_message("droid");
        assert!(droid.contains("\"droid\""), "{droid}");
        assert!(
            droid.contains(&intent_providers::login_command("droid")),
            "{droid}"
        );
        assert!(!droid.contains("desktop app"), "{droid}");
    }
}
