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
//!   the pinned npx package. Base models with reasoning-effort support are
//!   expanded into `{model}/{effort}` variants (parity with the FE). The FE
//!   additionally tries a `codex app-server` transport first, but that path
//!   exists to reuse its Electron-managed codex runtime; daemon-side the
//!   codex-acp probe reaches the same catalog (codex-acp itself queries the
//!   codex CLI), so probe-only is sufficient here.
//! - `pi` — ACP probe via the pinned npx adapter ([`PI_ACP_NPX_PACKAGE`];
//!   `pi` has no entry in the ACP provider registry, so the pin lives here).
//! - `droid` — ACP probe via a resolved `droid` binary
//!   (`droid exec --output-format acp`), with auth-required detection.
//! - `opencode` — native CLI: `opencode models`, one `provider/model` per
//!   line.
//!
//! auggie (existing CLI path in `agent_ops`) and cortex (static catalog) are
//! deliberately NOT implemented here — they live in [`crate::model_catalog`],
//! whose provider→source registry wires these five sources into `models.list`
//! alongside them.

use std::path::{Path, PathBuf};
use std::time::Duration;

use intent_providers::{find_npx, find_provider_binary};
use serde_json::Value;

mod parse;
mod probe;

use probe::{run_acp_probe, AcpProbeCommand, ProbeError};

/// Pinned npx package spec for the pi ACP adapter. Mirrors the FE pin
/// (`PI_ACP_NPX_PACKAGE` in `pi-resolver.ts`); bumping the version is a
/// deliberate code change.
pub const PI_ACP_NPX_PACKAGE: &str = "pi-acp@0.0.31";

/// Timeout for the one-shot `opencode models` CLI invocation.
const OPENCODE_CLI_TIMEOUT: Duration = Duration::from_secs(10);

/// Result of a provider model-catalog fetch.
///
/// `models` is `Some(non-empty rows)` on success — rows use the PROTOCOL
/// §5.30 wire shape `{ id, name, provider, description? }`. On any failure
/// (adapter missing, npx missing, probe timeout, auth required, empty
/// response) `models` is `None` and `warning` carries the reason.
#[derive(Debug)]
pub struct ProviderModelsFetch {
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
/// daemon-side source here (auggie's CLI path and cortex's static catalog are
/// wired elsewhere) return `None` with a warning.
pub async fn fetch_provider_models(provider_id: &str) -> ProviderModelsFetch {
    match provider_id {
        "claude-code" => fetch_claude_code_models().await,
        "codex" => fetch_codex_models().await,
        "pi" => fetch_pi_models().await,
        "droid" => fetch_droid_models().await,
        "opencode" => fetch_opencode_models().await,
        other => ProviderModelsFetch::unavailable(other, "no dynamic model source"),
    }
}

/// claude-code: ACP probe via the pinned npx adapter. Models arrive in the
/// `session/new` result — `configOptions[id="model"].options` on current
/// adapters (≥ 0.60), `models.availableModels` on older ones — or a
/// `session/update` notification. The adapter's real `default` row is
/// returned as-is.
pub async fn fetch_claude_code_models() -> ProviderModelsFetch {
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
/// fallback. Effort-variant base models expand into `{model}/{effort}` rows.
///
/// The probe child runs with an isolated `CODEX_HOME` (fresh per-probe temp
/// dir, removed after the probe) so the user's `~/.codex/config.toml` — and
/// any `mcp_servers` it registers — is never loaded by the throwaway
/// codex-acp process. Only `auth.json` is seeded into the isolated home so a
/// logged-in codex stays logged in.
pub async fn fetch_codex_models() -> ProviderModelsFetch {
    let cmd = if let Some(bin) = find_provider_binary("codex", "codex-acp", None) {
        AcpProbeCommand::binary(bin, Vec::new())
    } else if let Some(npx) = find_npx() {
        AcpProbeCommand::npx(npx, intent_providers::config::CODEX_ACP_NPX_PACKAGE)
    } else {
        return ProviderModelsFetch::unavailable(
            "codex",
            "codex-acp binary not found and npx unavailable for the pinned fallback",
        );
    };
    let (cmd, codex_home) = match codex_probe_with_isolated_home(cmd) {
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

/// Attach a freshly created isolated `CODEX_HOME` to the codex probe command.
/// The returned [`tempfile::TempDir`] must outlive the probe run; dropping it
/// removes the throwaway home.
fn codex_probe_with_isolated_home(
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

/// Create a fresh temp dir to serve as a probe's `CODEX_HOME` (codex requires
/// the directory to exist). Only `auth.json` is copied from `user_codex_dir`;
/// `config.toml` is deliberately NOT copied so user-configured `mcp_servers`
/// never start under the probe.
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
    }
    Ok(dir)
}

/// pi: ACP probe via the pinned npx adapter. Models may arrive under
/// `models.availableModels`, `availableModels`, `models.available`, or
/// `configOptions[id="model"].options`.
pub async fn fetch_pi_models() -> ProviderModelsFetch {
    let Some(npx) = find_npx() else {
        return ProviderModelsFetch::unavailable(
            "pi",
            "npx not found; cannot run the pinned pi-acp adapter",
        );
    };
    let cmd = AcpProbeCommand::npx(npx, PI_ACP_NPX_PACKAGE);
    finish(
        "pi",
        run_acp_probe(cmd, |v| parse::parse_acp_models(v, "pi")).await,
    )
}

/// droid: ACP probe via a resolved `droid` binary
/// (`droid exec --output-format acp`). An explicit ACP auth-required error is
/// surfaced as a distinct warning (parity with the FE `droid-acp-probe.ts`).
pub async fn fetch_droid_models() -> ProviderModelsFetch {
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

/// opencode: native CLI — run `opencode models` and parse one
/// `provider/model` per line (parity with the FE `opencode.ipc.ts`, which
/// routes the same command through `host.exec`).
pub async fn fetch_opencode_models() -> ProviderModelsFetch {
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
        let trimmed = stderr.trim();
        let tail: String = trimmed
            .chars()
            .skip(trimmed.chars().count().saturating_sub(200))
            .collect();
        return Err(format!(
            "opencode models exited with {}: {tail}",
            output.status
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

#[cfg(test)]
mod tests;
