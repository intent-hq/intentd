//! Per-provider model discovery (ACP probe + CLI).
//!
//! Ports the reference FE's per-provider model listing
//! (`cloudlands-fe/src/features/{claude-code,codex,pi,droid,opencode}/main`)
//! daemon-side, so the daemon can discover each provider's model catalog
//! itself instead of only auggie's. Each source is a pure async fetch that
//! returns a [`ProviderModelsFetch`]: model rows in the PROTOCOL §5.30 wire
//! shape `{ id, name, provider, description? }`, or `None` plus a
//! machine-readable warning when the catalog is unavailable.
//!
//! Sources:
//! - `claude-code` — ACP probe via the pinned npx adapter
//!   ([`intent_providers::CLAUDE_AGENT_ACP_NPX_PACKAGE`]).
//! - `codex` — ACP probe via a resolved `codex-acp` binary, falling back to
//!   the pinned npx package. Base models with reasoning-effort support carry
//!   their levels as `effortLevels` (parity with the FE). The FE
//!   additionally tries a `codex app-server` transport first, but that path
//!   exists to reuse its Electron-managed codex runtime; daemon-side the
//!   codex-acp probe reaches the same catalog (codex-acp itself queries the
//!   codex CLI), so probe-only is sufficient here.
//! - `pi` — ACP probe via the pinned npx adapter
//!   ([`intent_providers::PI_ACP_NPX_PACKAGE`]).
//! - `droid` — ACP probe via a resolved `droid` binary
//!   (`droid exec --output-format acp`), with auth-required detection.
//! - `opencode` — native CLI: `opencode models`, one `provider/model` per
//!   line.
//! - `grok` — native CLI: `grok models` parsed via
//!   [`intent_providers::parse_grok_models_command_output`] (auth markers +
//!   JSON payload + text rows; the exit code is never trusted).
//! - `unsloth` — HTTP fetch of the Hugging Face `unsloth` org's GGUF repos
//!   (`https://huggingface.co/api/models?author=unsloth&filter=gguf`), one
//!   wire row per repo (never per-quant), filtered to repos estimated to fit
//!   within ~70% of total system RAM (a parsed parameter count × a
//!   Q4-class bytes/param estimate). See [`parse::build_unsloth_rows`].
//!
//! auggie (existing CLI path in `agent_ops`) and cortex (open-gate empty
//! list — the provider CLI owns model selection) are deliberately NOT
//! implemented here — they live in [`crate::model_catalog`], whose
//! provider→source registry wires these sources into `models.list`
//! alongside them.

use std::path::{Path, PathBuf};
use std::time::Duration;

use intent_providers::{find_npx, find_provider_binary};
use serde_json::Value;

mod parse;
mod probe;

pub(crate) use parse::{gguf_bytes_fit_within_ram, is_default_pseudo_row};
use probe::{run_acp_probe, AcpProbeCommand, ProbeError};

/// Timeout for the one-shot `opencode models` CLI invocation.
const OPENCODE_CLI_TIMEOUT: Duration = Duration::from_secs(10);

/// Timeout for the one-shot `grok models` CLI invocation.
const GROK_CLI_TIMEOUT: Duration = Duration::from_secs(10);

/// Timeout for the Hugging Face `unsloth` catalog HTTP fetch.
const UNSLOTH_HF_TIMEOUT: Duration = Duration::from_secs(10);

/// Hugging Face `models` list API, scoped to the `unsloth` org's GGUF repos.
/// `limit=1000` covers the org's full catalog (~350 repos as of 2026-07) in
/// one request — HF's default page size (~50) would otherwise require
/// pagination we don't need.
const UNSLOTH_HF_API_URL: &str =
    "https://huggingface.co/api/models?author=unsloth&filter=gguf&limit=1000";

/// Result of a provider model-catalog fetch.
///
/// `models` is `Some(non-empty rows)` on success — rows use the PROTOCOL
/// §5.30 wire shape `{ id, name, provider, description? }`. On any failure
/// (adapter missing, npx missing, probe timeout, auth required, empty
/// response) `models` is `None` and `warning` carries the reason.
#[derive(Debug)]
pub(crate) struct ProviderModelsFetch {
    /// Wire-shaped model rows, or `None` when the catalog is unavailable.
    pub models: Option<Vec<Value>>,
    /// Machine-readable reason when `models` is `None`.
    pub warning: Option<String>,
}

impl ProviderModelsFetch {
    fn ok(models: Vec<Value>) -> Self {
        Self {
            models: Some(models),
            warning: None,
        }
    }

    fn unavailable(provider_id: &str, reason: impl std::fmt::Display) -> Self {
        Self {
            models: None,
            warning: Some(format!("{provider_id}: {reason}")),
        }
    }
}

/// Convert a probe outcome into the fetch result, attributing warnings to the
/// provider.
fn finish(provider_id: &str, outcome: Result<Vec<Value>, ProbeError>) -> ProviderModelsFetch {
    match outcome {
        Ok(models) => ProviderModelsFetch::ok(models),
        Err(err) => ProviderModelsFetch::unavailable(provider_id, err),
    }
}

/// Dispatch a model-catalog fetch by provider id. Providers without a
/// daemon-side source here (auggie's CLI path and cortex's open-gate empty
/// list are wired elsewhere) return `None` with a warning.
pub(crate) async fn fetch_provider_models(provider_id: &str) -> ProviderModelsFetch {
    match provider_id {
        "claude-code" => fetch_claude_code_models().await,
        "codex" => fetch_codex_models().await,
        "pi" => fetch_pi_models().await,
        "droid" => fetch_droid_models().await,
        "opencode" => fetch_opencode_models().await,
        "grok" => fetch_grok_models().await,
        "unsloth" => fetch_unsloth_models().await,
        other => ProviderModelsFetch::unavailable(other, "no dynamic model source"),
    }
}

/// claude-code: ACP probe via the pinned npx adapter. Models arrive in the
/// `session/new` result — `configOptions[id="model"].options` on current
/// adapters (≥ 0.60), `models.availableModels` on older ones — or a
/// `session/update` notification. The adapter's `default` pseudo-row is
/// resolved to the real model it stands for (marked `isDefault: true`) and
/// dropped whenever a real row exists; it is kept only as a sole row.
pub(crate) async fn fetch_claude_code_models() -> ProviderModelsFetch {
    let Some(npx) = find_npx() else {
        return ProviderModelsFetch::unavailable(
            "claude-code",
            "npx not found; cannot run the pinned claude-agent-acp adapter",
        );
    };
    let cmd = AcpProbeCommand::npx(npx, intent_providers::CLAUDE_AGENT_ACP_NPX_PACKAGE);
    finish(
        "claude-code",
        run_acp_probe(cmd, |v| parse::parse_acp_models(v, "claude-code")).await,
    )
}

/// codex: ACP probe via a resolved `codex-acp` binary, else the pinned npx
/// fallback. Effort-capable base models carry `effortLevels` on one row.
///
/// The probe child runs with an isolated `CODEX_HOME` (fresh per-probe temp
/// dir, removed after the probe) so the user's `~/.codex/config.toml` — and
/// any `mcp_servers` it registers — is never loaded by the throwaway
/// codex-acp process. `auth.json` is seeded into the isolated home so a
/// logged-in codex stays logged in, plus a minimal `config.toml` carrying
/// only the user's configured `model` / `model_reasoning_effort` so that
/// model appears in the reported catalog.
pub(crate) async fn fetch_codex_models() -> ProviderModelsFetch {
    let Some(cmd) =
        codex_probe_launch(find_provider_binary("codex", "codex-acp", None), find_npx())
    else {
        return ProviderModelsFetch::unavailable(
            "codex",
            "codex-acp binary not found and npx unavailable for the pinned fallback",
        );
    };
    let (cmd, codex_home) = match with_isolated_codex_home(cmd) {
        Ok(pair) => pair,
        Err(e) => {
            return ProviderModelsFetch::unavailable(
                "codex",
                format!("failed to create isolated CODEX_HOME: {e}"),
            )
        }
    };
    let outcome = run_acp_probe(cmd, parse::parse_codex_acp_models).await;
    drop(codex_home);
    finish("codex", outcome)
}

/// Pick the codex probe launch: a resolved `codex-acp` binary (the user's
/// escape hatch) spawns with the daemon env untouched, while the pinned npx
/// fallback is daemon-managed and gets `CODEX_PATH` / `CODEX_CONFIG` removed
/// from its inherited env so a stray value cannot redirect the adapter (#555).
fn codex_probe_launch(
    resolved_bin: Option<PathBuf>,
    npx: Option<PathBuf>,
) -> Option<AcpProbeCommand> {
    if let Some(bin) = resolved_bin {
        Some(AcpProbeCommand::binary(bin, Vec::new()))
    } else {
        npx.map(|npx| {
            AcpProbeCommand::npx(npx, intent_providers::config::CODEX_ACP_NPX_PACKAGE)
                .env_remove("CODEX_PATH")
                .env_remove("CODEX_CONFIG")
        })
    }
}

/// Attach a freshly created isolated `CODEX_HOME` to an ephemeral codex
/// launch. Shared by the model probe and the one-shot completion runner
/// ([`crate::complete_ops`]): both spawn throwaway codex-acp children that
/// must never load the user's real `~/.codex/config.toml` (and the
/// `mcp_servers` it can register). The returned [`tempfile::TempDir`] must
/// outlive the run; dropping it removes the throwaway home.
pub(crate) fn with_isolated_codex_home(
    cmd: AcpProbeCommand,
) -> std::io::Result<(AcpProbeCommand, tempfile::TempDir)> {
    let home = isolated_codex_home(user_codex_dir().as_deref())?;
    let cmd = cmd.env("CODEX_HOME", home.path().as_os_str());
    Ok((cmd, home))
}

/// The user's real codex home: `$CODEX_HOME` when set, else `~/.codex`.
/// Env vars that are set but empty are treated as unset.
fn user_codex_dir() -> Option<PathBuf> {
    let non_empty = |key: &str| std::env::var_os(key).filter(|v| !v.is_empty());
    if let Some(home) = non_empty("CODEX_HOME") {
        return Some(PathBuf::from(home));
    }
    let home = non_empty("HOME").or_else(|| non_empty("USERPROFILE"))?;
    Some(PathBuf::from(home).join(".codex"))
}

/// Top-level scalar keys copied from the user's `config.toml` into the
/// isolated probe home. `model` (and its effort) is what surfaces
/// user-configured models (e.g. a newer model than the adapter presets) in
/// codex-acp's reported catalog. Everything else — notably `mcp_servers` —
/// is deliberately never copied. Known limitation: a model configured only
/// via a codex profile (`profile = "x"` + `[profiles.x].model`) or backed by
/// a custom `[model_providers.*]` entry is not seeded — only top-level
/// scalars are read.
const CODEX_CONFIG_SEED_KEYS: &[&str] = &["model", "model_reasoning_effort"];

/// Create a fresh temp dir to serve as a probe's `CODEX_HOME` (codex requires
/// the directory to exist). `auth.json` is copied from `user_codex_dir` so a
/// logged-in codex stays logged in, and a minimal `config.toml` holding only
/// the [`CODEX_CONFIG_SEED_KEYS`] scalars is seeded so the user's configured
/// model shows up in the probe's catalog. The user's full `config.toml` is
/// deliberately NOT copied so user-configured `mcp_servers` never start under
/// the probe.
fn isolated_codex_home(user_codex_dir: Option<&Path>) -> std::io::Result<tempfile::TempDir> {
    let dir = tempfile::Builder::new()
        .prefix("intentd-codex-home-")
        .tempdir()?;
    if let Some(user_dir) = user_codex_dir {
        let auth = user_dir.join("auth.json");
        if auth.is_file() {
            if let Err(e) = std::fs::copy(&auth, dir.path().join("auth.json")) {
                tracing::warn!(
                    "failed to seed auth.json into isolated CODEX_HOME (probe will run logged-out): {e}"
                );
            }
        }
        if let Some(seed) = minimal_codex_config_seed(&user_dir.join("config.toml")) {
            if let Err(e) = std::fs::write(dir.path().join("config.toml"), seed) {
                tracing::warn!(
                    "failed to seed minimal config.toml into isolated CODEX_HOME (probe will use adapter presets): {e}"
                );
            }
        }
    }
    Ok(dir)
}

/// Build the minimal `config.toml` text to seed into the isolated probe home:
/// only the [`CODEX_CONFIG_SEED_KEYS`] top-level string values from the
/// user's config at `path`. Returns `None` — seed nothing, the probe still
/// works with adapter presets — when the file is absent, unreadable, or
/// malformed, or when none of the allowlisted keys hold a string.
fn minimal_codex_config_seed(path: &Path) -> Option<String> {
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return None,
        Err(e) => {
            tracing::warn!("could not read user codex config.toml; seeding nothing: {e}");
            return None;
        }
    };
    let doc: toml_edit::DocumentMut = match text.parse() {
        Ok(doc) => doc,
        Err(e) => {
            tracing::warn!("user codex config.toml is malformed; seeding nothing: {e}");
            return None;
        }
    };
    let mut seed = toml_edit::DocumentMut::new();
    for key in CODEX_CONFIG_SEED_KEYS {
        if let Some(value) = doc.get(key).and_then(|item| item.as_str()) {
            seed[key] = toml_edit::value(value);
        }
    }
    if seed.as_table().is_empty() {
        return None;
    }
    Some(seed.to_string())
}

/// pi: ACP probe via the pinned npx adapter. Models may arrive under
/// `models.availableModels`, `availableModels`, `models.available`, or
/// `configOptions[id="model"].options`.
pub(crate) async fn fetch_pi_models() -> ProviderModelsFetch {
    let Some(npx) = find_npx() else {
        return ProviderModelsFetch::unavailable(
            "pi",
            "npx not found; cannot run the pinned pi-acp adapter",
        );
    };
    let cmd = AcpProbeCommand::npx(npx, intent_providers::PI_ACP_NPX_PACKAGE);
    finish(
        "pi",
        run_acp_probe(cmd, |v| parse::parse_acp_models(v, "pi")).await,
    )
}

/// droid: ACP probe via a resolved `droid` binary
/// (`droid exec --output-format acp`). An explicit ACP auth-required error is
/// surfaced as a distinct warning (parity with the FE `droid-acp-probe.ts`).
pub(crate) async fn fetch_droid_models() -> ProviderModelsFetch {
    let Some(bin) = find_provider_binary("droid", "droid", None) else {
        return ProviderModelsFetch::unavailable("droid", "droid binary not found");
    };
    let args = vec![
        "exec".to_string(),
        "--output-format".to_string(),
        "acp".to_string(),
    ];
    let outcome = run_acp_probe(AcpProbeCommand::binary(bin, args), |v| {
        parse::parse_acp_models(v, "droid")
    })
    .await;
    match outcome {
        Err(ProbeError::Rpc(err)) if parse::is_auth_required_error(err.code, &err.message) => {
            ProviderModelsFetch::unavailable("droid", "authentication required")
        }
        other => finish("droid", other),
    }
}

/// droid auth probe (`host.providerAuthStatus`): the same ACP probe as
/// [`fetch_droid_models`], mapped to auth semantics (parity with the FE
/// `checkDroidReady`) — a non-empty model list ⇒ authenticated, an explicit
/// auth-required error ⇒ not authenticated, anything else (timeout, spawn
/// failure, empty catalog) ⇒ unknown. `bin` is the caller-resolved `droid`
/// binary (the caller's install gate).
pub(crate) async fn probe_droid_auth(bin: PathBuf) -> Option<bool> {
    let args = vec![
        "exec".to_string(),
        "--output-format".to_string(),
        "acp".to_string(),
    ];
    let outcome = run_acp_probe(AcpProbeCommand::binary(bin, args), |v| {
        parse::parse_acp_models(v, "droid")
    })
    .await;
    match outcome {
        Ok(models) if !models.is_empty() => Some(true),
        Err(ProbeError::Rpc(err)) if parse::is_auth_required_error(err.code, &err.message) => {
            Some(false)
        }
        Ok(_) | Err(_) => None,
    }
}

/// pi auth probe (`host.providerAuthStatus`): the same pinned-adapter ACP
/// probe as [`fetch_pi_models`], mapped to auth semantics — a non-empty
/// model list ⇒ authenticated; an empty list or an explicit auth-required
/// error ⇒ not authenticated (pi's adapter serves only credentialed models);
/// spawn failure / timeout / transport error ⇒ unknown. The caller gates on
/// the `pi` CLI being installed; the probe itself runs the pinned npx
/// adapter.
pub(crate) async fn probe_pi_auth() -> Option<bool> {
    let npx = find_npx()?;
    let cmd = AcpProbeCommand::npx(npx, intent_providers::PI_ACP_NPX_PACKAGE);
    let outcome = run_acp_probe(cmd, |v| parse::parse_acp_models(v, "pi")).await;
    match outcome {
        Ok(models) if !models.is_empty() => Some(true),
        Ok(_) | Err(ProbeError::Empty) => Some(false),
        Err(ProbeError::Rpc(err)) if parse::is_auth_required_error(err.code, &err.message) => {
            Some(false)
        }
        Err(_) => None,
    }
}

/// claude-code auth fallback probe (`host.providerAuthStatus`): the same
/// pinned-adapter ACP probe as [`fetch_claude_code_models`], mapped to auth
/// semantics by [`claude_code_acp_auth_verdict`]. Consulted only when the
/// cheap `claude auth status` CLI probe does not confirm login — that CLI
/// has known false negatives (anthropics/claude-code#76168). The fallback
/// can only demote to `Some(false)` (explicit auth-required error) or stay
/// unknown — it can never confirm `Some(true)`, because the adapter serves
/// its model catalog without credentials (see
/// [`claude_code_acp_auth_verdict`]). The caller gates on the `claude` CLI
/// being installed; the probe itself runs the pinned npx adapter
/// ([`intent_providers::CLAUDE_AGENT_ACP_NPX_PACKAGE`]).
pub(crate) async fn probe_claude_code_auth() -> Option<bool> {
    let npx = find_npx()?;
    let cmd = AcpProbeCommand::npx(npx, intent_providers::CLAUDE_AGENT_ACP_NPX_PACKAGE);
    let outcome = run_acp_probe(cmd, |v| parse::parse_acp_models(v, "claude-code")).await;
    claude_code_acp_auth_verdict(outcome)
}

/// Map a claude-code ACP probe outcome to the auth tri-state (the pure seam
/// unit tests drive without spawning the adapter). Only the adapter's
/// explicit auth-required RPC error (`-32000 Authentication required` —
/// intent-hq/intent#3178) is conclusive, demoting to `Some(false)`.
/// Everything else — INCLUDING a non-empty model list — stays unknown:
/// claude-agent-acp serves its model catalog uncredentialed (verified
/// empirically against v0.66.0 with a scratch HOME — `initialize` and
/// `session/new` succeed and return the full catalog in config options
/// while logged out, with `authMethods` empty either way; the auth error
/// only fires at `session/prompt` time, which a probe cannot afford to
/// send), so a served catalog proves the adapter spawned, not that the
/// user is logged in. Unlike pi — whose adapter serves only credentialed
/// models, so a non-empty list confirms `Some(true)` and an empty list
/// demotes to `Some(false)` — neither claude-code list shape is
/// conclusive.
fn claude_code_acp_auth_verdict(outcome: Result<Vec<Value>, ProbeError>) -> Option<bool> {
    match outcome {
        Err(ProbeError::Rpc(err)) if parse::is_auth_required_error(err.code, &err.message) => {
            Some(false)
        }
        _ => None,
    }
}

/// opencode: native CLI — run `opencode models` and parse one
/// `provider/model` per line (parity with the FE `opencode.ipc.ts`, which
/// routes the same command through `host.exec`).
pub(crate) async fn fetch_opencode_models() -> ProviderModelsFetch {
    let Some(bin) = find_provider_binary("opencode", "opencode", None) else {
        return ProviderModelsFetch::unavailable("opencode", "opencode binary not found");
    };
    match run_opencode_models_cli(bin, OPENCODE_CLI_TIMEOUT).await {
        Ok(stdout) => {
            let models = parse::parse_opencode_models(&stdout);
            if models.is_empty() {
                ProviderModelsFetch::unavailable("opencode", "no models reported")
            } else {
                ProviderModelsFetch::ok(models)
            }
        }
        Err(reason) => ProviderModelsFetch::unavailable("opencode", reason),
    }
}

/// Run `opencode models` with a hard timeout, returning stdout on exit 0.
/// On timeout the `output()` future is dropped, and `kill_on_drop` reaps the
/// child so a wedged CLI does not leak past the failed probe. The timeout is
/// injectable for tests; production passes [`OPENCODE_CLI_TIMEOUT`]. The
/// child runs with the enhanced PATH (binary's parent dir prepended,
/// matching the ACP probe spawns) so anything the opencode CLI shells out to
/// resolves in a packaged-app (minimal PATH) environment.
async fn run_opencode_models_cli(bin: PathBuf, timeout: Duration) -> Result<String, String> {
    let mut cmd = tokio::process::Command::new(&bin);
    cmd.arg("models")
        .env("PATH", intent_providers::enhanced_path(Some(&bin)))
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    let fut = cmd.output();
    let output = match tokio::time::timeout(timeout, fut).await {
        Ok(Ok(output)) => output,
        Ok(Err(e)) => return Err(format!("failed to run opencode models: {e}")),
        Err(_) => return Err("opencode models timed out".to_string()),
    };
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!(
            "opencode models exited with {}: {}",
            output.status,
            stderr_tail(&stderr)
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

/// The last ~200 chars of a trimmed stderr, for warning attribution. Walks
/// backwards from the end so the cost is bounded by the tail length, not the
/// full stderr size.
fn stderr_tail(stderr: &str) -> String {
    let trimmed = stderr.trim();
    let start = trimmed.char_indices().rev().nth(199).map_or(0, |(i, _)| i);
    trimmed[start..].to_string()
}

/// grok: native CLI — run `grok models` and parse stdout via
/// [`intent_providers::parse_grok_models_command_output`] (auth markers, then
/// a JSON payload, then text rows — parity with the FE grok probe).
pub(crate) async fn fetch_grok_models() -> ProviderModelsFetch {
    let Some(bin) = find_provider_binary("grok", "grok", None) else {
        return ProviderModelsFetch::unavailable("grok", "grok binary not found");
    };
    match run_grok_models_cli(bin, GROK_CLI_TIMEOUT).await {
        Ok(output) => grok_fetch_outcome(
            &String::from_utf8_lossy(&output.stdout),
            output.status,
            &String::from_utf8_lossy(&output.stderr),
        ),
        Err(reason) => ProviderModelsFetch::unavailable("grok", reason),
    }
}

/// Map one `grok models` run onto the fetch contract (the pure seam unit
/// tests drive without a real CLI). The exit code is never trusted for auth —
/// the CLI exits 0 in both auth states — so stdout is parsed regardless: an
/// explicit logged-out marker degrades to "authentication required", parsed
/// rows win otherwise, and only a run that produced neither is attributed to
/// its exit state (status + stderr tail, matching the opencode warning).
fn grok_fetch_outcome(
    stdout: &str,
    status: std::process::ExitStatus,
    stderr: &str,
) -> ProviderModelsFetch {
    let parsed = intent_providers::parse_grok_models_command_output(stdout);
    if parsed.authenticated == Some(false) {
        return ProviderModelsFetch::unavailable("grok", "authentication required");
    }
    let rows = parse::grok_wire_rows(&parsed.models);
    if !rows.is_empty() {
        ProviderModelsFetch::ok(rows)
    } else if status.success() {
        ProviderModelsFetch::unavailable("grok", "no models reported")
    } else {
        ProviderModelsFetch::unavailable(
            "grok",
            format!("grok models exited with {status}: {}", stderr_tail(stderr)),
        )
    }
}

/// Run `grok models` with a hard timeout, returning the raw output (stdout is
/// parsed by the caller — the exit code alone is never a failure signal). On
/// timeout the `output()` future is dropped and `kill_on_drop` reaps the
/// child. The timeout is injectable for tests; production passes
/// [`GROK_CLI_TIMEOUT`]. The child runs with the enhanced PATH (binary's
/// parent dir prepended, matching the opencode CLI and ACP probe spawns).
async fn run_grok_models_cli(
    bin: PathBuf,
    timeout: Duration,
) -> Result<std::process::Output, String> {
    let mut cmd = tokio::process::Command::new(&bin);
    cmd.arg("models")
        .env("PATH", intent_providers::enhanced_path(Some(&bin)))
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    match tokio::time::timeout(timeout, cmd.output()).await {
        Ok(Ok(output)) => Ok(output),
        Ok(Err(e)) => Err(format!("failed to run grok models: {e}")),
        Err(_) => Err("grok models timed out".to_string()),
    }
}

/// unsloth: fetch the Hugging Face `unsloth` org's GGUF repos and build one
/// wire row per repo, filtered to repos estimated to fit within ~70% of
/// total system RAM. On a platform where RAM detection is unsupported
/// ([`crate::agent_manager::total_memory_bytes`] returns `None`), the fit
/// filter is skipped and every repo is returned — never mis-filtered.
pub(crate) async fn fetch_unsloth_models() -> ProviderModelsFetch {
    let body = match fetch_unsloth_hf_catalog(UNSLOTH_HF_TIMEOUT).await {
        Ok(body) => body,
        Err(reason) => return ProviderModelsFetch::unavailable("unsloth", reason),
    };
    unsloth_fetch_outcome(&body, crate::agent_manager::total_memory_bytes())
}

/// GET [`UNSLOTH_HF_API_URL`] with a hard timeout, returning the raw JSON
/// response body. The timeout is injectable for tests (this function is not
/// itself unit-tested against the network; see [`unsloth_fetch_outcome`] for
/// the pure, fixture-driven parsing/filtering logic).
async fn fetch_unsloth_hf_catalog(timeout: Duration) -> Result<String, String> {
    let client = reqwest::Client::builder()
        .timeout(timeout)
        .build()
        .map_err(|e| format!("failed to build http client: {e}"))?;
    let resp = client
        .get(UNSLOTH_HF_API_URL)
        .header(reqwest::header::ACCEPT, "application/json")
        .send()
        .await
        .map_err(|e| format!("huggingface request failed: {e}"))?;
    let status = resp.status();
    if !status.is_success() {
        return Err(format!("huggingface returned {status}"));
    }
    resp.text()
        .await
        .map_err(|e| format!("failed to read huggingface response: {e}"))
}

/// Map one fetched HF catalog response onto the fetch contract (the pure
/// seam unit tests drive with a recorded fixture, no network). A response
/// that parses to zero repos, or whose fit filter hides every repo, degrades
/// to "no models reported" rather than an empty success — matching the
/// opencode/grok "no models reported" convention so an empty catalog is
/// never silently cached as valid.
fn unsloth_fetch_outcome(body: &str, total_ram_bytes: Option<u64>) -> ProviderModelsFetch {
    let repos = parse::parse_hf_unsloth_response(body);
    if repos.is_empty() {
        return ProviderModelsFetch::unavailable("unsloth", "no models reported");
    }
    let (rows, hidden) = parse::build_unsloth_rows(&repos, total_ram_bytes);
    if rows.is_empty() {
        return ProviderModelsFetch::unavailable(
            "unsloth",
            format!(
                "no models reported (all {hidden} repos too large for available memory or of unknown size)"
            ),
        );
    }
    if hidden > 0 {
        ProviderModelsFetch {
            models: Some(rows),
            warning: Some(format!(
                "unsloth: {hidden} repo(s) hidden (estimated to exceed available memory, or size unknown)"
            )),
        }
    } else {
        ProviderModelsFetch::ok(rows)
    }
}

#[cfg(test)]
mod tests;
