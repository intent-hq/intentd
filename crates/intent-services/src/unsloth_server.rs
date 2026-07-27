//! Daemon-managed Unsloth server lifecycle (monorepo#878, spec "Proposed
//! design" §4).
//!
//! intentd owns a singleton local Unsloth server (one loaded model at a time —
//! llama.cpp constraint). On the first spawn of an unsloth-provider agent the
//! daemon picks the quant variant to serve — the best-fitting one from the
//! repo's actual GGUF file sizes (Hugging Face per-repo listing, same RAM
//! budget as the catalog's fit filter), falling back to the CLI-default
//! variant when size metadata is unavailable — then runs
//! `unsloth run --model <repo>:<quant> --disable-tools -p <port>`,
//! waits for the HTTP surface to come up, mints the opencode auth material via
//! `unsloth start opencode --no-launch --model <repo>` (which writes
//! `~/.unsloth/studio/auth/agents/opencode/opencode.json` with the baseURL,
//! apiKey, and real per-model token limits), and probes `/models` with the
//! minted key until the model finishes downloading/loading. The resulting
//! [`UnslothEndpoint`] feeds the `OPENCODE_CONFIG_CONTENT` injection
//! (`intent_providers::build_provider_env_with_unsloth`).
//!
//! Lifecycle policy (validated against a real install, 2026-07-27):
//! - start on demand at agent spawn; reuse the running server while it serves
//!   the requested repo; kill + respawn on model switch or a dead child.
//! - the server requires auth even on `/v1/models` — an HTTP 401/403 during
//!   probing means "server up, model maybe still loading", not failure.
//! - first use can mean a multi-GB Hugging Face download before the server is
//!   ready, so the model-ready timeout is generous and progress status is
//!   surfaced through the caller-supplied status callback.
//!
//! Test strategy: this module adds no new RPC method — it is spawn-path
//! plumbing behind the existing `agent.*` methods — and the production spawn
//! path requires the external `unsloth` binary, so the WSS-e2e convention
//! (AGENTS.md) does not apply directly. Coverage is crate-level: pure unit
//! tests (quant selection, probe classification, generated-config parsing
//! against a recorded fixture) plus lifecycle tests driving the real manager
//! against a stub `unsloth` shell script and a loopback HTTP responder — no
//! network, no real binary. The full path is exercised manually with a real
//! install (spec verification plan).

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use intent_core::{Error, Result};
use intent_providers::{UnslothEndpoint, UnslothModelLimit};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::Mutex as TokioMutex;

/// Default port the managed server listens on (`unsloth run -p <port>`;
/// matches the Unsloth CLI's own default).
const DEFAULT_PORT: u16 = 8888;

/// How long to wait for the spawned server process to answer HTTP at all
/// (any status code — the socket accepting requests, not model readiness).
const SERVER_UP_TIMEOUT: Duration = Duration::from_secs(120);

/// How long to wait for the served model to become ready. First use can mean
/// a multi-GB Hugging Face download, so this is deliberately generous.
const MODEL_READY_TIMEOUT: Duration = Duration::from_secs(30 * 60);

/// Poll interval between readiness probes.
const PROBE_INTERVAL: Duration = Duration::from_secs(2);

/// Cadence of "still waiting" status updates while the model loads.
const STATUS_UPDATE_INTERVAL: Duration = Duration::from_secs(15);

/// Per-request HTTP timeout for a single readiness probe.
const PROBE_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

/// How long `unsloth start opencode --no-launch` may take to mint the
/// opencode auth material (no model download happens on this path).
const MINT_TIMEOUT: Duration = Duration::from_secs(60);

/// How many trailing output lines of the server child are retained for
/// diagnostics when it dies during startup.
const OUTPUT_TAIL_LINES: usize = 40;

/// Hugging Face API base for the per-repo file listing used by the
/// spawn-time quant-variant selection (overridable in tests).
const HF_API_BASE: &str = "https://huggingface.co";

/// Timeout for the per-repo Hugging Face file-listing fetch. Deliberately
/// short: a slow or unreachable HF must never stall an agent spawn —
/// selection falls back to the CLI-default quant instead.
const HF_FILES_TIMEOUT: Duration = Duration::from_secs(8);

/// Install hint appended to "binary not found" errors.
const UNSLOTH_INSTALL_HINT: &str =
    "install the Unsloth CLI and ensure `unsloth` is on PATH (https://docs.unsloth.ai/)";

/// Status callback: receives short human-readable progress messages
/// ("starting server", "downloading/loading model…") the caller surfaces to
/// the user (e.g. as `agent:stream:status` events).
pub type StatusCallback = dyn Fn(String) + Send + Sync;

/// The error for a missing `unsloth` binary (graceful degradation: the
/// provider is unavailable with a clear install message). `InvalidInput`
/// (not `Internal`): this is an environment misconfiguration, and its
/// Display survives the JSON-RPC envelope (`domain_to_rpc` masks Internal
/// messages behind a literal "Internal error").
fn missing_binary_error() -> Error {
    Error::InvalidInput(format!("unsloth CLI not found — {UNSLOTH_INSTALL_HINT}"))
}

/// The error an in-flight or new `ensure_endpoint` gets when a daemon
/// shutdown was requested mid-startup.
fn shutting_down_error() -> Error {
    Error::Internal("unsloth server startup aborted: daemon is shutting down".to_string())
}

/// Fallback quant variant when per-repo size metadata is unavailable,
/// mirroring the Unsloth CLI's own `--gguf-variant` defaults (validated
/// 2026-07-27): `UD-Q4_K_XL` for `unsloth/*` GGUF repos, `Q4_K_M` otherwise.
pub(crate) fn default_quant_variant(repo_id: &str) -> &'static str {
    if repo_id.starts_with("unsloth/") {
        "UD-Q4_K_XL"
    } else {
        "Q4_K_M"
    }
}

/// `--model` argument for `unsloth run`: the repo id with the daemon-picked
/// quant variant suffix (`<repo>:<quant>`).
pub(crate) fn run_model_arg(repo_id: &str, quant: &str) -> String {
    format!("{repo_id}:{quant}")
}

/// Full-precision GGUF export tags. Valid files in unsloth repos, but never
/// auto-selected: at a comparable fit a Q8-class quant is practically
/// lossless and far cheaper to serve.
const FULL_PRECISION_TAGS: [&str; 3] = ["BF16", "F16", "F32"];

/// Whether an (uppercased) trailing filename token is a GGUF variant tag:
/// a quant family (`Q4_K_M`, `IQ2_XXS`, `TQ1_0`, …) or a full-precision
/// export tag.
fn is_variant_token(tag: &str) -> bool {
    if FULL_PRECISION_TAGS.contains(&tag) {
        return true;
    }
    let rest = tag
        .strip_prefix("IQ")
        .or_else(|| tag.strip_prefix("TQ"))
        .or_else(|| tag.strip_prefix("Q"));
    matches!(rest, Some(r) if r.starts_with(|c: char| c.is_ascii_digit())
        && r.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'))
}

/// Extract the (uppercased) quant-variant tag from a GGUF file path, e.g.
/// `gemma-4-it-UD-Q4_K_XL.gguf` → `UD-Q4_K_XL` and
/// `Q8_0/gemma-4-it-Q8_0-00001-of-00002.gguf` → `Q8_0`. Multi-part
/// suffixes (`-NNNNN-of-NNNNN`) are stripped before the tag is read; paths
/// that are not `.gguf` files or carry no recognizable trailing tag return
/// `None`.
pub(crate) fn quant_tag_from_gguf_path(path: &str) -> Option<String> {
    let file = path.rsplit('/').next().unwrap_or(path);
    let stem = file
        .strip_suffix(".gguf")
        .or_else(|| file.strip_suffix(".GGUF"))?;
    let mut tokens: Vec<&str> = stem.split('-').collect();
    let all_digits = |t: &str| !t.is_empty() && t.chars().all(|c| c.is_ascii_digit());
    if tokens.len() >= 3
        && tokens[tokens.len() - 2].eq_ignore_ascii_case("of")
        && all_digits(tokens[tokens.len() - 1])
        && all_digits(tokens[tokens.len() - 3])
    {
        tokens.truncate(tokens.len() - 3);
    }
    let tag = tokens.pop()?.to_ascii_uppercase();
    if !is_variant_token(&tag) {
        return None;
    }
    if tokens.last().is_some_and(|t| t.eq_ignore_ascii_case("UD")) {
        Some(format!("UD-{tag}"))
    } else {
        Some(tag)
    }
}

/// Parse a Hugging Face per-repo file listing
/// (`/api/models/<repo>?blobs=true` — `siblings` entries carry `rfilename`
/// and `size`) into total bytes per quant-variant tag. Multi-part GGUFs
/// sum all their parts; non-GGUF files and entries without a size are
/// skipped. Malformed JSON yields an empty map — the caller treats "no
/// size data" as "use the CLI default".
pub(crate) fn parse_repo_quant_sizes(body: &str) -> BTreeMap<String, u64> {
    let mut sizes = BTreeMap::new();
    let Ok(root) = serde_json::from_str::<Value>(body) else {
        return sizes;
    };
    let Some(siblings) = root.get("siblings").and_then(Value::as_array) else {
        return sizes;
    };
    for entry in siblings {
        let Some(path) = entry.get("rfilename").and_then(Value::as_str) else {
            continue;
        };
        let Some(size) = entry.get("size").and_then(Value::as_u64) else {
            continue;
        };
        let Some(tag) = quant_tag_from_gguf_path(path) else {
            continue;
        };
        *sizes.entry(tag).or_insert(0) += size;
    }
    sizes
}

/// Pick the quant variant to serve from a repo's actual per-variant file
/// sizes: the highest-quality (largest) quant that fits within the same
/// RAM budget as the catalog's fit filter
/// ([`crate::provider_models::gguf_bytes_fit_within_ram`] — ~70% of total
/// RAM with KV-cache headroom). Full-precision exports (BF16/F16/F32) are
/// never picked. When nothing fits, the smallest quant gives the repo its
/// best chance to run (the user explicitly picked it). Size ties prefer
/// Unsloth's `UD-*` dynamic quants (better quality per byte). Returns
/// `None` — "use the CLI default" — when the map has no quant candidates
/// or total RAM is unknown on this platform.
pub(crate) fn best_fitting_quant(
    sizes: &BTreeMap<String, u64>,
    total_ram_bytes: Option<u64>,
) -> Option<String> {
    let total_ram = total_ram_bytes?;
    let is_full_precision =
        |tag: &str| FULL_PRECISION_TAGS.contains(&tag.strip_prefix("UD-").unwrap_or(tag));
    let candidates: Vec<(&str, u64)> = sizes
        .iter()
        .filter(|(tag, _)| !is_full_precision(tag))
        .map(|(tag, size)| (tag.as_str(), *size))
        .collect();
    let fitting = candidates
        .iter()
        .filter(|(_, size)| crate::provider_models::gguf_bytes_fit_within_ram(*size, total_ram))
        .max_by_key(|(tag, size)| (*size, tag.starts_with("UD-")));
    if let Some((tag, _)) = fitting {
        return Some((*tag).to_string());
    }
    candidates
        .iter()
        .min_by_key(|(tag, size)| (*size, !tag.starts_with("UD-")))
        .map(|(tag, _)| (*tag).to_string())
}

/// Outcome of one readiness probe attempt against the managed server.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProbeOutcome {
    /// HTTP 200 — the server answered the authed `/models` request; the
    /// model is loaded and the endpoint is usable.
    Ready,
    /// Any other HTTP response (401/403 before auth or while the model is
    /// still downloading/loading, 5xx during startup): the server socket is
    /// up but the endpoint is not usable yet.
    UpNotReady,
    /// No HTTP response at all (connect refused/timeout): the server process
    /// has not opened its socket yet.
    Down,
}

/// Classify a probe's HTTP status (`None` = no HTTP response / connect
/// error). The server requires auth even on `/v1/models`, so non-200
/// responses are "up, not ready" rather than failures.
pub(crate) fn classify_probe(status: Option<u16>) -> ProbeOutcome {
    match status {
        Some(200) => ProbeOutcome::Ready,
        Some(_) => ProbeOutcome::UpNotReady,
        None => ProbeOutcome::Down,
    }
}

/// Path of the opencode auth config `unsloth start opencode --no-launch`
/// generates, under `home`:
/// `~/.unsloth/studio/auth/agents/opencode/opencode.json`.
pub(crate) fn generated_config_path(home: &Path) -> PathBuf {
    home.join(".unsloth")
        .join("studio")
        .join("auth")
        .join("agents")
        .join("opencode")
        .join("opencode.json")
}

/// Parse the generated opencode.json into an [`UnslothEndpoint`] for
/// `served_repo`. The file carries a `provider.unsloth-studio` block with
/// `options.baseURL` + `options.apiKey` and a `models` map whose entries hold
/// real per-model `limit: { context, output }` discovered from the server,
/// plus an optional top-level `compaction: { reserved }`. The `models` entry
/// is looked up by the full repo id; when absent (e.g. the CLI keyed it
/// differently) the endpoint still resolves without limits rather than
/// failing the spawn.
pub(crate) fn parse_generated_config(body: &str, served_repo: &str) -> Result<UnslothEndpoint> {
    let root: Value = serde_json::from_str(body).map_err(|e| {
        Error::Internal(format!(
            "unsloth generated opencode.json is not valid JSON: {e}"
        ))
    })?;
    let provider = root
        .get("provider")
        .and_then(|p| p.as_object())
        .and_then(|p| {
            // Prefer the known `unsloth-studio` key; fall back to any block
            // carrying `options` so a renamed key still resolves (map
            // iteration order is only reached when the known key is absent).
            p.get("unsloth-studio")
                .filter(|v| v.get("options").is_some())
                .or_else(|| p.values().find(|v| v.get("options").is_some()))
        })
        .ok_or_else(|| {
            Error::Internal(
                "unsloth generated opencode.json has no provider block with options".to_string(),
            )
        })?;
    let options = &provider["options"];
    let base_url = options
        .get("baseURL")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            Error::Internal(
                "unsloth generated opencode.json is missing options.baseURL".to_string(),
            )
        })?
        .to_string();
    let api_key = options
        .get("apiKey")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            Error::Internal("unsloth generated opencode.json is missing options.apiKey".to_string())
        })?
        .to_string();
    let model_entry = provider
        .get("models")
        .and_then(|m| m.get(served_repo))
        // Tolerate a `models` map keyed by something other than the exact
        // repo id by falling back to the sole entry when there is one.
        .or_else(|| {
            provider
                .get("models")
                .and_then(|m| m.as_object())
                .filter(|m| m.len() == 1)
                .and_then(|m| m.values().next())
        });
    let model_display_name = model_entry
        .and_then(|m| m.get("name"))
        .and_then(Value::as_str)
        .map(str::to_string);
    let limit = model_entry.and_then(|m| m.get("limit")).and_then(|l| {
        Some(UnslothModelLimit {
            context: l.get("context").and_then(Value::as_u64)?,
            output: l.get("output").and_then(Value::as_u64)?,
        })
    });
    let compaction_reserved = root
        .get("compaction")
        .and_then(|c| c.get("reserved"))
        .and_then(Value::as_u64);
    Ok(UnslothEndpoint {
        base_url,
        api_key,
        model_id: served_repo.to_string(),
        model_display_name,
        limit,
        compaction_reserved,
    })
}

/// One managed server child: the process handle plus what it was started
/// with, so reuse/restart decisions compare against the live state.
struct ManagedServer {
    child: Child,
    /// Full HF repo id the server was started with (no quant suffix).
    repo_id: String,
    /// The resolved endpoint, once minted+ready. `None` while starting.
    endpoint: Option<UnslothEndpoint>,
    /// Rolling tail of the child's combined stdout/stderr for diagnostics.
    output_tail: Arc<Mutex<std::collections::VecDeque<String>>>,
    /// The stdout/stderr drain tasks, kept so [`Self::tail`] can await them
    /// (post-kill the pipes hit EOF and the drains terminate promptly) —
    /// snapshotting without waiting races the child's final output and can
    /// lose the diagnostic exactly when it's needed.
    drain_tasks: Vec<tokio::task::JoinHandle<()>>,
}

/// How long [`ManagedServer::tail`] waits for the drain tasks to consume the
/// dead child's final output before snapshotting.
const DRAIN_SETTLE_TIMEOUT: Duration = Duration::from_secs(2);

impl ManagedServer {
    /// Whether the child process is still running (`try_wait` probe).
    fn is_alive(&mut self) -> bool {
        matches!(self.child.try_wait(), Ok(None))
    }

    /// Snapshot the retained output tail as one newline-joined string. Waits
    /// (bounded) for the drain tasks first so a fast-exiting child's final
    /// lines are captured — call only after the child is dead.
    async fn tail(&mut self) -> String {
        for task in self.drain_tasks.drain(..) {
            let _ = tokio::time::timeout(DRAIN_SETTLE_TIMEOUT, task).await;
        }
        let tail = self.output_tail.lock().unwrap();
        tail.iter().cloned().collect::<Vec<_>>().join("\n")
    }
}

/// Injectable configuration seams (tests override every external surface:
/// binary resolution, home dir, port, timeouts, HF metadata fetch, RAM).
pub(crate) struct UnslothConfig {
    /// Resolve the `unsloth` binary; `None` = not installed.
    pub resolve_binary: Box<dyn Fn() -> Option<PathBuf> + Send + Sync>,
    /// Home directory used to locate the generated opencode.json.
    pub home_dir: Option<PathBuf>,
    /// Port passed to `unsloth run -p`.
    pub port: u16,
    pub server_up_timeout: Duration,
    pub model_ready_timeout: Duration,
    pub probe_interval: Duration,
    pub mint_timeout: Duration,
    /// Hugging Face API base for the per-repo file listing the quant
    /// selection uses (tests point this at a loopback stub).
    pub hf_api_base: String,
    /// Timeout for that per-repo file-listing fetch.
    pub hf_files_timeout: Duration,
    /// Total system RAM probe; `None` = detection unsupported.
    pub total_memory_bytes: Box<dyn Fn() -> Option<u64> + Send + Sync>,
}

impl Default for UnslothConfig {
    fn default() -> Self {
        Self {
            resolve_binary: Box::new(|| {
                intent_providers::find_provider_binary("unsloth", "unsloth", None)
            }),
            home_dir: std::env::var_os("HOME")
                .or_else(|| std::env::var_os("USERPROFILE"))
                .map(PathBuf::from)
                .filter(|p| !p.as_os_str().is_empty()),
            port: DEFAULT_PORT,
            server_up_timeout: SERVER_UP_TIMEOUT,
            model_ready_timeout: MODEL_READY_TIMEOUT,
            probe_interval: PROBE_INTERVAL,
            mint_timeout: MINT_TIMEOUT,
            hf_api_base: HF_API_BASE.to_string(),
            hf_files_timeout: HF_FILES_TIMEOUT,
            total_memory_bytes: Box::new(crate::agent_manager::total_memory_bytes),
        }
    }
}

/// Singleton manager for the daemon-owned Unsloth server. One instance lives
/// on the [`crate::agent_manager::AgentManager`]; the async mutex serializes
/// concurrent unsloth-agent spawns so exactly one start/restart runs at a
/// time (the second spawner reuses the endpoint the first one produced).
pub struct UnslothServerManager {
    state: TokioMutex<Option<ManagedServer>>,
    config: UnslothConfig,
    /// Quant variants already selected this daemon lifetime, keyed by repo
    /// id. File sizes and total RAM don't change while the daemon runs, so
    /// a successful selection is worth exactly one HF round-trip per repo;
    /// failed lookups are NOT cached (transient network errors retry on the
    /// next spawn).
    quant_cache: Mutex<HashMap<String, String>>,
    /// Terminal shutdown latch. [`Self::shutdown`] sets it BEFORE taking the
    /// state lock so an in-flight startup (which can legitimately sit in its
    /// probe loop for many minutes during a first-use model download) notices
    /// at the next probe tick, aborts, and releases the lock — daemon
    /// shutdown never waits out the model-ready window.
    shutting_down: std::sync::atomic::AtomicBool,
}

impl Default for UnslothServerManager {
    fn default() -> Self {
        Self::with_config(UnslothConfig::default())
    }
}

impl UnslothServerManager {
    pub(crate) fn with_config(config: UnslothConfig) -> Self {
        Self {
            state: TokioMutex::new(None),
            config,
            quant_cache: Mutex::new(HashMap::new()),
            shutting_down: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Whether [`Self::shutdown`] has been requested.
    fn is_shutting_down(&self) -> bool {
        self.shutting_down
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Ensure a managed server is running and ready for `repo_id`, returning
    /// the endpoint to inject into the opencode spawn env. Reuses the live
    /// server when it already serves `repo_id`; kills + respawns on model
    /// switch or a dead child. `status` receives human-readable progress
    /// messages while a (potentially multi-GB first-use download) startup is
    /// in flight.
    pub async fn ensure_endpoint(
        &self,
        repo_id: &str,
        status: &StatusCallback,
    ) -> Result<UnslothEndpoint> {
        if self.is_shutting_down() {
            return Err(shutting_down_error());
        }
        let mut state = self.state.lock().await;

        // Reuse: live child serving the requested repo with a minted endpoint.
        if let Some(server) = state.as_mut() {
            if server.repo_id == repo_id && server.is_alive() {
                if let Some(ep) = &server.endpoint {
                    return Ok(ep.clone());
                }
            }
            // Model switch or dead/half-started child: tear down and respawn.
            let mut old = state.take().expect("state checked above");
            tracing::info!(
                old_repo = %old.repo_id,
                new_repo = %repo_id,
                "stopping managed unsloth server (model switch or dead child)"
            );
            kill_server_child(&mut old.child).await;
        }

        let binary = (self.config.resolve_binary)().ok_or_else(missing_binary_error)?;

        status(format!("Starting Unsloth server for {repo_id}…"));
        let quant = self.resolve_quant_variant(repo_id).await;
        let server = self.start_server(&binary, repo_id, &quant)?;
        *state = Some(server);

        match self
            .wait_and_mint(&binary, repo_id, state.as_mut().unwrap(), status)
            .await
        {
            Ok(endpoint) => {
                state.as_mut().unwrap().endpoint = Some(endpoint.clone());
                Ok(endpoint)
            }
            Err(e) => {
                // Startup failed: kill the half-started child so the next
                // attempt starts clean, and surface the output tail (with
                // any minted key material redacted — the error is
                // client-visible).
                let mut failed = state.take().expect("state set above");
                kill_server_child(&mut failed.child).await;
                let tail = redact_key_material(&failed.tail().await);
                if tail.is_empty() {
                    Err(e)
                } else {
                    Err(Error::Internal(format!("{e}\nserver output tail:\n{tail}")))
                }
            }
        }
    }

    /// Resolve the quant variant to serve for `repo_id`: the best-fitting
    /// one from the repo's actual GGUF file sizes when the HF listing is
    /// reachable ([`best_fitting_quant`]), the CLI-default variant
    /// otherwise. A slow or failing HF fetch degrades to the default within
    /// [`UnslothConfig::hf_files_timeout`] — it never fails the spawn.
    async fn resolve_quant_variant(&self, repo_id: &str) -> String {
        if let Some(quant) = self.quant_cache.lock().unwrap().get(repo_id) {
            return quant.clone();
        }
        match self.fetch_repo_file_listing(repo_id).await {
            Ok(body) => {
                let sizes = parse_repo_quant_sizes(&body);
                let quant = best_fitting_quant(&sizes, (self.config.total_memory_bytes)())
                    .unwrap_or_else(|| default_quant_variant(repo_id).to_string());
                tracing::info!(repo = %repo_id, quant = %quant, "selected unsloth quant variant");
                self.quant_cache
                    .lock()
                    .unwrap()
                    .insert(repo_id.to_string(), quant.clone());
                quant
            }
            Err(reason) => {
                let quant = default_quant_variant(repo_id).to_string();
                tracing::warn!(
                    repo = %repo_id,
                    reason = %reason,
                    quant = %quant,
                    "unsloth quant-variant lookup failed; using CLI default"
                );
                quant
            }
        }
    }

    /// GET the Hugging Face per-repo file listing
    /// (`/api/models/<repo>?blobs=true`) with a hard timeout, returning the
    /// raw JSON body. Mirrors the catalog fetch in
    /// [`crate::provider_models`]; errors are strings because the caller
    /// only logs them and falls back.
    async fn fetch_repo_file_listing(&self, repo_id: &str) -> std::result::Result<String, String> {
        let url = format!(
            "{}/api/models/{repo_id}?blobs=true",
            self.config.hf_api_base
        );
        let client = reqwest::Client::builder()
            .timeout(self.config.hf_files_timeout)
            .build()
            .map_err(|e| format!("failed to build http client: {e}"))?;
        let resp = client
            .get(&url)
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

    /// Spawn `unsloth run --model <repo>:<quant> --disable-tools -p <port>`
    /// as its own process-group leader with captured output.
    fn start_server(&self, binary: &Path, repo_id: &str, quant: &str) -> Result<ManagedServer> {
        let mut cmd = Command::new(binary);
        cmd.arg("run")
            .arg("--model")
            .arg(run_model_arg(repo_id, quant))
            .arg("--disable-tools")
            .arg("-p")
            .arg(self.config.port.to_string())
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        #[cfg(unix)]
        cmd.process_group(0);
        let mut child = cmd
            .spawn()
            .map_err(|e| Error::Internal(format!("failed to spawn unsloth server: {e}")))?;

        // Drain stdout/stderr into a rolling tail so a startup failure has a
        // diagnosable trace (and the pipes never fill up and stall the child).
        let output_tail = Arc::new(Mutex::new(std::collections::VecDeque::new()));
        let mut drain_tasks = Vec::new();
        if let Some(stdout) = child.stdout.take() {
            drain_tasks.push(tokio::spawn(drain_into_tail(stdout, output_tail.clone())));
        }
        if let Some(stderr) = child.stderr.take() {
            drain_tasks.push(tokio::spawn(drain_into_tail(stderr, output_tail.clone())));
        }

        tracing::info!(repo = %repo_id, quant = %quant, port = self.config.port, "spawned managed unsloth server");
        Ok(ManagedServer {
            child,
            repo_id: repo_id.to_string(),
            endpoint: None,
            output_tail,
            drain_tasks,
        })
    }

    /// Startup sequence after the child is spawned: wait for the HTTP socket,
    /// mint the opencode auth material, then probe with the minted key until
    /// the model is loaded (tolerating a first-use multi-GB download).
    async fn wait_and_mint(
        &self,
        binary: &Path,
        repo_id: &str,
        server: &mut ManagedServer,
        status: &StatusCallback,
    ) -> Result<UnslothEndpoint> {
        let probe_url = format!("http://127.0.0.1:{}/v1/models", self.config.port);
        let client = reqwest::Client::builder()
            .timeout(PROBE_REQUEST_TIMEOUT)
            .build()
            .map_err(|e| Error::Internal(format!("failed to build http client: {e}")))?;

        // Phase 1: socket up (any HTTP status).
        self.wait_until(self.config.server_up_timeout, server, || async {
            let outcome = classify_probe(probe_status(&client, &probe_url, None).await);
            outcome != ProbeOutcome::Down
        })
        .await
        .map_err(|e| match e {
            WaitError::ChildExited => Error::Internal(format!(
                "unsloth server exited during startup (model {repo_id})"
            )),
            WaitError::TimedOut => Error::Internal(format!(
                "unsloth server did not open its HTTP port within {}s",
                self.config.server_up_timeout.as_secs()
            )),
            WaitError::ShuttingDown => shutting_down_error(),
        })?;

        // Phase 2: mint the opencode auth material. `--no-launch` requires a
        // running server (validated: it errors with "No running Unsloth
        // server found" otherwise), which phase 1 guarantees.
        status(format!("Unsloth server up; preparing model {repo_id}…"));
        let endpoint = self.mint_endpoint(binary, repo_id).await?;

        // Phase 3: model ready — authed probe answers 200. First use can mean
        // a multi-GB download, so this window is generous and progress status
        // is refreshed periodically.
        let mut last_status = tokio::time::Instant::now();
        let result = self
            .wait_until(self.config.model_ready_timeout, server, || {
                let refresh = last_status.elapsed() >= STATUS_UPDATE_INTERVAL;
                if refresh {
                    last_status = tokio::time::Instant::now();
                    status(format!(
                        "Downloading/loading model {repo_id}… (first use may take several minutes)"
                    ));
                }
                let client = &client;
                let url = &probe_url;
                let key = endpoint.api_key.clone();
                async move {
                    classify_probe(probe_status(client, url, Some(&key)).await)
                        == ProbeOutcome::Ready
                }
            })
            .await;
        result.map_err(|e| match e {
            WaitError::ChildExited => Error::Internal(format!(
                "unsloth server exited while loading model {repo_id}"
            )),
            WaitError::TimedOut => Error::Internal(format!(
                "model {repo_id} did not become ready within {} minutes (download may still be in progress — retry later)",
                self.config.model_ready_timeout.as_secs() / 60
            )),
            WaitError::ShuttingDown => shutting_down_error(),
        })?;
        Ok(endpoint)
    }

    /// Run `unsloth start opencode --no-launch --model <repo>` and read the
    /// generated `~/.unsloth/studio/auth/agents/opencode/opencode.json`.
    /// `UNSLOTH_STUDIO_URL` is set when the managed port differs from the
    /// CLI default so the mint talks to OUR server.
    async fn mint_endpoint(&self, binary: &Path, repo_id: &str) -> Result<UnslothEndpoint> {
        let mut cmd = Command::new(binary);
        cmd.arg("start")
            .arg("opencode")
            .arg("--no-launch")
            .arg("--model")
            .arg(repo_id)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        if self.config.port != DEFAULT_PORT {
            cmd.env(
                "UNSLOTH_STUDIO_URL",
                format!("http://127.0.0.1:{}", self.config.port),
            );
        }
        let output = tokio::time::timeout(self.config.mint_timeout, async {
            cmd.output()
                .await
                .map_err(|e| Error::Internal(format!("failed to run unsloth start opencode: {e}")))
        })
        .await
        .map_err(|_| {
            Error::Internal(format!(
                "unsloth start opencode --no-launch timed out after {}s",
                self.config.mint_timeout.as_secs()
            ))
        })??;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            return Err(Error::Internal(format!(
                "unsloth start opencode --no-launch failed ({}): {}",
                output.status,
                if stderr.trim().is_empty() {
                    stdout.trim()
                } else {
                    stderr.trim()
                }
            )));
        }
        // InvalidInput (not Internal) for the same reason as
        // `missing_binary_error`: an unresolvable home directory is an
        // environment misconfiguration and the message must survive the
        // JSON-RPC envelope.
        let home = self.config.home_dir.as_deref().ok_or_else(|| {
            Error::InvalidInput(
                "cannot resolve home directory (HOME/USERPROFILE unset) — needed to read the unsloth-generated opencode config".to_string(),
            )
        })?;
        let path = generated_config_path(home);
        let body = tokio::fs::read_to_string(&path).await.map_err(|e| {
            Error::Internal(format!(
                "unsloth start opencode succeeded but generated config is unreadable ({}): {e}",
                path.display()
            ))
        })?;
        parse_generated_config(&body, repo_id)
    }

    /// Poll `check` every [`UnslothConfig::probe_interval`] until it returns
    /// `true`, the child exits, a shutdown is requested, or `timeout` elapses.
    async fn wait_until<F, Fut>(
        &self,
        timeout: Duration,
        server: &mut ManagedServer,
        mut check: F,
    ) -> std::result::Result<(), WaitError>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = bool>,
    {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if self.is_shutting_down() {
                return Err(WaitError::ShuttingDown);
            }
            if !server.is_alive() {
                return Err(WaitError::ChildExited);
            }
            if check().await {
                return Ok(());
            }
            if tokio::time::Instant::now() + self.config.probe_interval > deadline {
                return Err(WaitError::TimedOut);
            }
            tokio::time::sleep(self.config.probe_interval).await;
        }
    }

    /// Kill the managed server (daemon shutdown / explicit teardown). No-op
    /// when no server is running. Sets the terminal shutdown latch BEFORE
    /// taking the state lock, so an in-flight `ensure_endpoint` startup
    /// (potentially sitting in a minutes-long model-download probe loop)
    /// aborts at its next probe tick and releases the lock — shutdown waits
    /// at most ~one probe interval plus the startup's own cleanup kill.
    pub async fn shutdown(&self) {
        self.shutting_down
            .store(true, std::sync::atomic::Ordering::Relaxed);
        let mut state = self.state.lock().await;
        if let Some(mut server) = state.take() {
            tracing::info!(repo = %server.repo_id, "shutting down managed unsloth server");
            kill_server_child(&mut server.child).await;
        }
    }
}

/// Why a [`UnslothServerManager::wait_until`] loop stopped without success.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WaitError {
    ChildExited,
    TimedOut,
    /// [`UnslothServerManager::shutdown`] was requested mid-startup.
    ShuttingDown,
}

/// Redact minted key material from client-visible text: any `sk-…` token is
/// replaced with `sk-[redacted]` (the unsloth CLI prints endpoint + key
/// material on some startup paths, and the output tail rides an error the
/// client renders).
fn redact_key_material(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(idx) = rest.find("sk-") {
        out.push_str(&rest[..idx]);
        out.push_str("sk-[redacted]");
        let after = &rest[idx + 3..];
        let end = after
            .find(|c: char| !(c.is_ascii_alphanumeric() || c == '-' || c == '_'))
            .unwrap_or(after.len());
        rest = &after[end..];
    }
    out.push_str(rest);
    out
}

/// GET `url` (optionally with a Bearer key), returning the HTTP status code
/// or `None` on a connect/timeout error.
async fn probe_status(client: &reqwest::Client, url: &str, api_key: Option<&str>) -> Option<u16> {
    let mut req = client.get(url);
    if let Some(key) = api_key {
        req = req.bearer_auth(key);
    }
    req.send().await.ok().map(|r| r.status().as_u16())
}

/// Drain an async reader line-by-line into a bounded rolling tail.
async fn drain_into_tail<R>(reader: R, tail: Arc<Mutex<std::collections::VecDeque<String>>>)
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut lines = BufReader::new(reader).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        let mut tail = tail.lock().unwrap();
        if tail.len() >= OUTPUT_TAIL_LINES {
            tail.pop_front();
        }
        tail.push_back(line);
    }
}

/// SIGTERM the child's process group, wait briefly, then SIGKILL — same
/// convention as the provider-child teardown in `agent_manager`.
#[cfg(unix)]
async fn kill_server_child(child: &mut Child) {
    use nix::sys::signal::{killpg, Signal};
    use nix::unistd::Pid;
    if let Some(pid) = child.id() {
        let pgid = Pid::from_raw(pid as i32);
        let _ = killpg(pgid, Signal::SIGTERM);
        let _ = tokio::time::timeout(Duration::from_secs(2), child.wait()).await;
        let _ = killpg(pgid, Signal::SIGKILL);
    } else {
        let _ = child.start_kill();
    }
    let _ = child.wait().await;
}

#[cfg(not(unix))]
async fn kill_server_child(child: &mut Child) {
    let _ = child.start_kill();
    let _ = child.wait().await;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Recorded shape of the opencode.json `unsloth start opencode
    /// --no-launch` generates (validated against a real install, 2026-07-27).
    const GENERATED_CONFIG_FIXTURE: &str = r#"{
      "$schema": "https://opencode.ai/config.json",
      "provider": {
        "unsloth-studio": {
          "npm": "@ai-sdk/openai-compatible",
          "name": "Unsloth (local)",
          "options": {
            "baseURL": "http://127.0.0.1:8888/v1",
            "apiKey": "sk-unsloth-test-key"
          },
          "models": {
            "unsloth/gemma-4-26B-A4B-it-GGUF": {
              "name": "Gemma 4 26B A4B It",
              "limit": { "context": 262144, "output": 8192 }
            }
          }
        }
      },
      "model": "unsloth-studio/unsloth/gemma-4-26B-A4B-it-GGUF",
      "small_model": "unsloth-studio/unsloth/gemma-4-26B-A4B-it-GGUF",
      "compaction": { "auto": true, "reserved": 8192 }
    }"#;

    const REPO: &str = "unsloth/gemma-4-26B-A4B-it-GGUF";

    const GIB: u64 = 1024 * 1024 * 1024;

    /// Recorded shape of the Hugging Face per-repo file listing
    /// (`/api/models/<repo>?blobs=true` — `siblings` with `size`), covering
    /// flat single-file quants, a `UD-*` dynamic quant, a directory-nested
    /// multi-part quant, full-precision exports, non-GGUF files, and an
    /// entry without a size.
    const HF_REPO_FILES_FIXTURE: &str = r#"{
      "_id": "6851e1f6f0e2e9a8c7b41c2d",
      "id": "unsloth/gemma-4-26B-A4B-it-GGUF",
      "siblings": [
        { "rfilename": ".gitattributes", "size": 1519 },
        { "rfilename": "README.md", "size": 18213 },
        { "rfilename": "config.json", "size": 855 },
        { "rfilename": "gemma-4-26B-A4B-it-Q2_K.gguf", "blobId": "b1", "size": 9600000000 },
        { "rfilename": "gemma-4-26B-A4B-it-Q4_K_M.gguf", "blobId": "b2", "size": 15800000000 },
        { "rfilename": "UD-Q4_K_XL/gemma-4-26B-A4B-it-UD-Q4_K_XL-00001-of-00002.gguf", "blobId": "b3", "size": 9000000000 },
        { "rfilename": "UD-Q4_K_XL/gemma-4-26B-A4B-it-UD-Q4_K_XL-00002-of-00002.gguf", "blobId": "b4", "size": 8200000000 },
        { "rfilename": "gemma-4-26B-A4B-it-Q6_K.gguf", "blobId": "b5", "size": 21400000000 },
        { "rfilename": "gemma-4-26B-A4B-it-Q8_0.gguf", "blobId": "b6", "size": 27700000000 },
        { "rfilename": "BF16/gemma-4-26B-A4B-it-BF16-00001-of-00002.gguf", "blobId": "b7", "size": 30000000000 },
        { "rfilename": "BF16/gemma-4-26B-A4B-it-BF16-00002-of-00002.gguf", "blobId": "b8", "size": 22100000000 },
        { "rfilename": "gemma-4-26B-A4B-it-IQ1_S.gguf" }
      ]
    }"#;

    // --- quant selection ---

    #[test]
    fn default_quant_variant_matches_cli_defaults() {
        assert_eq!(
            default_quant_variant("unsloth/gemma-4-26B-A4B-it-GGUF"),
            "UD-Q4_K_XL"
        );
        assert_eq!(default_quant_variant("other-org/model-GGUF"), "Q4_K_M");
    }

    #[test]
    fn run_model_arg_appends_quant_suffix() {
        assert_eq!(
            run_model_arg("unsloth/gemma-4-26B-A4B-it-GGUF", "UD-Q4_K_XL"),
            "unsloth/gemma-4-26B-A4B-it-GGUF:UD-Q4_K_XL"
        );
        assert_eq!(
            run_model_arg("meta/llama-GGUF", "Q6_K"),
            "meta/llama-GGUF:Q6_K"
        );
    }

    #[test]
    fn quant_tag_extraction_from_gguf_paths() {
        assert_eq!(
            quant_tag_from_gguf_path("gemma-4-26B-A4B-it-Q4_K_M.gguf").as_deref(),
            Some("Q4_K_M")
        );
        assert_eq!(
            quant_tag_from_gguf_path("gemma-4-26B-A4B-it-UD-Q4_K_XL.gguf").as_deref(),
            Some("UD-Q4_K_XL")
        );
        // Directory-nested multi-part files resolve to the same tag.
        assert_eq!(
            quant_tag_from_gguf_path("Q8_0/gemma-4-26B-A4B-it-Q8_0-00001-of-00002.gguf").as_deref(),
            Some("Q8_0")
        );
        // Lowercase tags normalize to the CLI's uppercase convention.
        assert_eq!(
            quant_tag_from_gguf_path("model-iq2_xxs.gguf").as_deref(),
            Some("IQ2_XXS")
        );
        assert_eq!(
            quant_tag_from_gguf_path("model-TQ1_0.gguf").as_deref(),
            Some("TQ1_0")
        );
        assert_eq!(
            quant_tag_from_gguf_path("BF16/model-BF16-00002-of-00002.gguf").as_deref(),
            Some("BF16")
        );
        // Non-GGUF files and stems without a recognizable trailing tag.
        assert_eq!(quant_tag_from_gguf_path("README.md"), None);
        assert_eq!(quant_tag_from_gguf_path("config.json"), None);
        assert_eq!(quant_tag_from_gguf_path("model-instruct.gguf"), None);
        assert_eq!(quant_tag_from_gguf_path("Qwen-model.gguf"), None);
    }

    #[test]
    fn repo_quant_sizes_sums_multipart_and_skips_non_gguf() {
        let sizes = parse_repo_quant_sizes(HF_REPO_FILES_FIXTURE);
        assert_eq!(sizes.get("Q2_K"), Some(&9_600_000_000));
        assert_eq!(sizes.get("Q4_K_M"), Some(&15_800_000_000));
        // Multi-part quants sum all their parts.
        assert_eq!(sizes.get("UD-Q4_K_XL"), Some(&17_200_000_000));
        assert_eq!(sizes.get("Q6_K"), Some(&21_400_000_000));
        assert_eq!(sizes.get("Q8_0"), Some(&27_700_000_000));
        assert_eq!(sizes.get("BF16"), Some(&52_100_000_000));
        // The sizeless IQ1_S entry and non-GGUF files contribute nothing.
        assert_eq!(sizes.get("IQ1_S"), None);
        assert_eq!(sizes.len(), 6);
    }

    #[test]
    fn repo_quant_sizes_tolerates_malformed_bodies() {
        assert!(parse_repo_quant_sizes("not json").is_empty());
        assert!(parse_repo_quant_sizes("{}").is_empty());
        assert!(parse_repo_quant_sizes(r#"{ "siblings": "nope" }"#).is_empty());
    }

    #[test]
    fn best_fitting_quant_prefers_largest_that_fits() {
        let sizes = parse_repo_quant_sizes(HF_REPO_FILES_FIXTURE);
        // 64 GiB: Q8_0 fits (BF16 is never picked, regardless of RAM).
        assert_eq!(
            best_fitting_quant(&sizes, Some(64 * GIB)).as_deref(),
            Some("Q8_0")
        );
        // 32 GiB: Q8_0 no longer fits; the next-largest quant wins.
        assert_eq!(
            best_fitting_quant(&sizes, Some(32 * GIB)).as_deref(),
            Some("Q6_K")
        );
        // 16 GiB: only the smallest quant fits.
        assert_eq!(
            best_fitting_quant(&sizes, Some(16 * GIB)).as_deref(),
            Some("Q2_K")
        );
    }

    #[test]
    fn best_fitting_quant_falls_back_to_smallest_when_nothing_fits() {
        let sizes = parse_repo_quant_sizes(HF_REPO_FILES_FIXTURE);
        assert_eq!(
            best_fitting_quant(&sizes, Some(8 * GIB)).as_deref(),
            Some("Q2_K")
        );
    }

    #[test]
    fn best_fitting_quant_prefers_ud_dynamic_quants_on_size_ties() {
        let mut sizes = BTreeMap::new();
        sizes.insert("Q4_K_M".to_string(), 15_800_000_000);
        sizes.insert("UD-Q4_K_XL".to_string(), 15_800_000_000);
        assert_eq!(
            best_fitting_quant(&sizes, Some(64 * GIB)).as_deref(),
            Some("UD-Q4_K_XL")
        );
        // The smallest-quant fallback applies the same tie preference.
        assert_eq!(
            best_fitting_quant(&sizes, Some(4 * GIB)).as_deref(),
            Some("UD-Q4_K_XL")
        );
    }

    #[test]
    fn best_fitting_quant_returns_none_without_usable_data() {
        let sizes = parse_repo_quant_sizes(HF_REPO_FILES_FIXTURE);
        // RAM detection unsupported: never guess, use the CLI default.
        assert_eq!(best_fitting_quant(&sizes, None), None);
        // No quant candidates at all.
        assert_eq!(best_fitting_quant(&BTreeMap::new(), Some(64 * GIB)), None);
        // Full-precision-only repos yield no candidate either.
        let mut fp_only = BTreeMap::new();
        fp_only.insert("BF16".to_string(), 52_100_000_000);
        fp_only.insert("F16".to_string(), 52_100_000_000);
        assert_eq!(best_fitting_quant(&fp_only, Some(1024 * GIB)), None);
    }

    // --- readiness classification ---

    #[test]
    fn probe_classification() {
        assert_eq!(classify_probe(Some(200)), ProbeOutcome::Ready);
        // Auth-required responses mean "up, model maybe still loading".
        assert_eq!(classify_probe(Some(401)), ProbeOutcome::UpNotReady);
        assert_eq!(classify_probe(Some(403)), ProbeOutcome::UpNotReady);
        assert_eq!(classify_probe(Some(500)), ProbeOutcome::UpNotReady);
        assert_eq!(classify_probe(None), ProbeOutcome::Down);
    }

    // --- generated-config parsing ---

    #[test]
    fn parse_generated_config_full_fixture() {
        let ep = parse_generated_config(GENERATED_CONFIG_FIXTURE, REPO).expect("parses");
        assert_eq!(ep.base_url, "http://127.0.0.1:8888/v1");
        assert_eq!(ep.api_key, "sk-unsloth-test-key");
        assert_eq!(ep.model_id, REPO);
        assert_eq!(ep.model_display_name.as_deref(), Some("Gemma 4 26B A4B It"));
        assert_eq!(
            ep.limit,
            Some(UnslothModelLimit {
                context: 262144,
                output: 8192
            })
        );
        assert_eq!(ep.compaction_reserved, Some(8192));
    }

    #[test]
    fn parse_generated_config_falls_back_to_sole_models_entry() {
        // The CLI may key the models map differently from the exact repo id;
        // a sole entry still yields display name + limits.
        let body = GENERATED_CONFIG_FIXTURE.replace(REPO, "unsloth/gemma-4-26b-a4b-it-GGUF");
        let ep = parse_generated_config(&body, REPO).expect("parses");
        assert_eq!(ep.model_id, REPO);
        assert_eq!(ep.model_display_name.as_deref(), Some("Gemma 4 26B A4B It"));
        assert!(ep.limit.is_some());
    }

    #[test]
    fn parse_generated_config_missing_limit_still_resolves() {
        let body = r#"{
          "provider": { "unsloth-studio": {
            "options": { "baseURL": "http://127.0.0.1:8888/v1", "apiKey": "k" },
            "models": { "unsloth/x-GGUF": { "name": "X" } }
          } }
        }"#;
        let ep = parse_generated_config(body, "unsloth/x-GGUF").expect("parses");
        assert_eq!(ep.limit, None);
        assert_eq!(ep.compaction_reserved, None);
    }

    #[test]
    fn parse_generated_config_rejects_missing_fields() {
        assert!(parse_generated_config("not json", REPO).is_err());
        assert!(parse_generated_config("{}", REPO).is_err());
        let no_key = r#"{ "provider": { "p": { "options": { "baseURL": "http://x/v1" } } } }"#;
        let err = parse_generated_config(no_key, REPO).unwrap_err();
        assert!(err.to_string().contains("apiKey"), "got: {err}");
        let no_url = r#"{ "provider": { "p": { "options": { "apiKey": "k" } } } }"#;
        let err = parse_generated_config(no_url, REPO).unwrap_err();
        assert!(err.to_string().contains("baseURL"), "got: {err}");
    }

    #[test]
    fn parse_generated_config_prefers_unsloth_studio_key() {
        // A foreign provider block with options must not win over the known
        // `unsloth-studio` key, regardless of map iteration order.
        let body = r#"{
          "provider": {
            "aaa-other": { "options": { "baseURL": "http://evil/v1", "apiKey": "other" } },
            "unsloth-studio": {
              "options": { "baseURL": "http://127.0.0.1:8888/v1", "apiKey": "right" },
              "models": {}
            }
          }
        }"#;
        let ep = parse_generated_config(body, REPO).expect("parses");
        assert_eq!(ep.api_key, "right");
        assert_eq!(ep.base_url, "http://127.0.0.1:8888/v1");
    }

    #[test]
    fn redact_key_material_strips_sk_tokens() {
        assert_eq!(
            redact_key_material("api key: sk-abc123_DEF done"),
            "api key: sk-[redacted] done"
        );
        assert_eq!(
            redact_key_material("sk-one\nBearer sk-two."),
            "sk-[redacted]\nBearer sk-[redacted]."
        );
        assert_eq!(redact_key_material("no keys here"), "no keys here");
    }

    #[test]
    fn generated_config_path_shape() {
        let p = generated_config_path(Path::new("/home/u"));
        assert_eq!(
            p,
            PathBuf::from("/home/u/.unsloth/studio/auth/agents/opencode/opencode.json")
        );
    }

    // --- lifecycle (stub binary + loopback stub server; unix-only: the stub
    // binary is a shell script) ---

    #[cfg(unix)]
    mod lifecycle {
        use super::*;
        use std::io::Write;
        use std::os::unix::fs::PermissionsExt;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        /// Minimal HTTP responder on an ephemeral loopback port: 200 when the
        /// request carries `Bearer <key>`, 401 otherwise (mirrors the real
        /// server's auth-required `/v1/models`).
        async fn spawn_stub_http(key: &'static str) -> u16 {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind");
            let port = listener.local_addr().unwrap().port();
            tokio::spawn(async move {
                loop {
                    let Ok((mut sock, _)) = listener.accept().await else {
                        break;
                    };
                    tokio::spawn(async move {
                        let mut buf = vec![0u8; 4096];
                        let n = sock.read(&mut buf).await.unwrap_or(0);
                        let req = String::from_utf8_lossy(&buf[..n]).to_string();
                        let resp = if req.contains(&format!("Bearer {key}")) {
                            "HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\n{}"
                        } else {
                            "HTTP/1.1 401 Unauthorized\r\ncontent-length: 0\r\n\r\n"
                        };
                        let _ = sock.write_all(resp.as_bytes()).await;
                    });
                }
            });
            port
        }

        /// Write an executable stub `unsloth` script into `dir`. `run` logs
        /// its argv and sleeps; `start opencode` writes the generated config
        /// fixture (with the loopback port + key patched in) under the fake
        /// home. `run_behavior` overrides the `run` arm (e.g. instant exit).
        fn write_stub_binary(
            dir: &Path,
            home: &Path,
            port: u16,
            run_behavior: Option<&str>,
        ) -> PathBuf {
            let log = dir.join("stub.log");
            let cfg_dir = home.join(".unsloth/studio/auth/agents/opencode");
            let config = GENERATED_CONFIG_FIXTURE
                .replace("127.0.0.1:8888", &format!("127.0.0.1:{port}"))
                .replace('\'', "");
            let run = run_behavior
                .map(str::to_string)
                .unwrap_or_else(|| "sleep 300".to_string());
            let script = format!(
                "#!/bin/sh\necho \"$@\" >> '{log}'\ncase \"$1\" in\n  run) {run} ;;\n  start) mkdir -p '{cfg}' && cat > '{cfg}/opencode.json' <<'EOF'\n{config}\nEOF\n  ;;\nesac\n",
                log = log.display(),
                cfg = cfg_dir.display(),
            );
            let path = dir.join("unsloth");
            let mut f = std::fs::File::create(&path).expect("create stub");
            f.write_all(script.as_bytes()).expect("write stub");
            f.set_permissions(std::fs::Permissions::from_mode(0o755))
                .expect("chmod stub");
            path
        }

        /// Stub Hugging Face API on an ephemeral loopback port: answers every
        /// request with 200 + `body` and counts hits (for cache assertions).
        async fn spawn_stub_hf(body: &'static str) -> (u16, Arc<AtomicUsize>) {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind");
            let port = listener.local_addr().unwrap().port();
            let hits = Arc::new(AtomicUsize::new(0));
            let hits2 = hits.clone();
            tokio::spawn(async move {
                loop {
                    let Ok((mut sock, _)) = listener.accept().await else {
                        break;
                    };
                    hits2.fetch_add(1, Ordering::SeqCst);
                    tokio::spawn(async move {
                        let mut buf = vec![0u8; 4096];
                        let _ = sock.read(&mut buf).await;
                        let resp = format!(
                            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}",
                            body.len()
                        );
                        let _ = sock.write_all(resp.as_bytes()).await;
                    });
                }
            });
            (port, hits)
        }

        /// Fast test config pointing at the stub binary + fake home + port.
        /// The HF base points at a closed loopback port (instant connection
        /// refusal → CLI-default quant); tests covering the selection path
        /// override it with a [`spawn_stub_hf`] port.
        fn test_config(binary: PathBuf, home: PathBuf, port: u16) -> UnslothConfig {
            UnslothConfig {
                resolve_binary: Box::new(move || Some(binary.clone())),
                home_dir: Some(home),
                port,
                server_up_timeout: Duration::from_secs(5),
                model_ready_timeout: Duration::from_secs(10),
                probe_interval: Duration::from_millis(50),
                mint_timeout: Duration::from_secs(5),
                hf_api_base: "http://127.0.0.1:1".to_string(),
                hf_files_timeout: Duration::from_secs(2),
                total_memory_bytes: Box::new(|| Some(32 * GIB)),
            }
        }

        fn stub_log(dir: &Path) -> String {
            std::fs::read_to_string(dir.join("stub.log")).unwrap_or_default()
        }

        #[tokio::test]
        async fn missing_binary_degrades_with_install_hint() {
            let mgr = UnslothServerManager::with_config(UnslothConfig {
                resolve_binary: Box::new(|| None),
                ..UnslothConfig::default()
            });
            let err = mgr.ensure_endpoint(REPO, &|_| {}).await.unwrap_err();
            let msg = err.to_string();
            assert!(msg.contains("unsloth CLI not found"), "got: {msg}");
            assert!(msg.contains("docs.unsloth.ai"), "got: {msg}");
            assert!(
                matches!(err, Error::InvalidInput(_)),
                "message must survive the RPC envelope"
            );
        }

        #[tokio::test]
        async fn full_startup_reuse_and_model_switch() {
            let dir = tempfile::tempdir().expect("tempdir");
            let port = spawn_stub_http("sk-unsloth-test-key").await;
            let binary = write_stub_binary(dir.path(), dir.path(), port, None);
            let mgr = UnslothServerManager::with_config(test_config(
                binary,
                dir.path().to_path_buf(),
                port,
            ));

            // Cold start: spawns the server, mints auth, probes to ready.
            let messages = Arc::new(Mutex::new(Vec::new()));
            let m2 = messages.clone();
            let ep = mgr
                .ensure_endpoint(REPO, &move |m| m2.lock().unwrap().push(m))
                .await
                .expect("cold start resolves endpoint");
            assert_eq!(ep.api_key, "sk-unsloth-test-key");
            assert_eq!(ep.base_url, format!("http://127.0.0.1:{port}/v1"));
            assert_eq!(ep.model_id, REPO);
            assert!(ep.limit.is_some());
            assert!(
                !messages.lock().unwrap().is_empty(),
                "progress status surfaced"
            );
            let log = stub_log(dir.path());
            // The test config's HF base is unreachable, so quant selection
            // falls back to the CLI default without failing the spawn.
            assert!(
                log.contains(&format!(
                    "run --model {REPO}:UD-Q4_K_XL --disable-tools -p {port}"
                )),
                "server spawned with quant + --disable-tools: {log}"
            );
            assert!(
                log.contains("start opencode --no-launch"),
                "auth minted: {log}"
            );

            // Reuse: same repo — no second `run` invocation.
            let runs_before = stub_log(dir.path()).matches("run --model").count();
            let ep2 = mgr.ensure_endpoint(REPO, &|_| {}).await.expect("reuse");
            assert_eq!(ep2, ep);
            assert_eq!(
                stub_log(dir.path()).matches("run --model").count(),
                runs_before,
                "reuse must not respawn the server"
            );

            // Model switch: kill + respawn with the new repo.
            let other = "unsloth/other-model-GGUF";
            let ep3 = mgr.ensure_endpoint(other, &|_| {}).await.expect("switch");
            assert_eq!(ep3.model_id, other);
            let log = stub_log(dir.path());
            assert!(
                log.contains(&format!("run --model {other}:UD-Q4_K_XL")),
                "respawned with the new model: {log}"
            );
            assert_eq!(log.matches("run --model").count(), runs_before + 1);

            mgr.shutdown().await;
        }

        #[tokio::test]
        async fn spawn_uses_best_fitting_quant_from_hf_listing() {
            let dir = tempfile::tempdir().expect("tempdir");
            let port = spawn_stub_http("sk-unsloth-test-key").await;
            let (hf_port, _) = spawn_stub_hf(HF_REPO_FILES_FIXTURE).await;
            let binary = write_stub_binary(dir.path(), dir.path(), port, None);
            let mut config = test_config(binary, dir.path().to_path_buf(), port);
            config.hf_api_base = format!("http://127.0.0.1:{hf_port}");
            let mgr = UnslothServerManager::with_config(config);

            mgr.ensure_endpoint(REPO, &|_| {}).await.expect("starts");
            let log = stub_log(dir.path());
            // 32 GiB total RAM: Q6_K is the largest quant in the listing
            // that fits the catalog's RAM budget (Q8_0 does not).
            assert!(
                log.contains(&format!(
                    "run --model {REPO}:Q6_K --disable-tools -p {port}"
                )),
                "server spawned with the best-fitting quant: {log}"
            );
            mgr.shutdown().await;
        }

        #[tokio::test]
        async fn quant_selection_is_cached_per_repo() {
            let (hf_port, hits) = spawn_stub_hf(HF_REPO_FILES_FIXTURE).await;
            let mut config = test_config(
                PathBuf::from("/nonexistent/unsloth"),
                PathBuf::from("/nonexistent"),
                1,
            );
            config.hf_api_base = format!("http://127.0.0.1:{hf_port}");
            let mgr = UnslothServerManager::with_config(config);

            assert_eq!(mgr.resolve_quant_variant(REPO).await, "Q6_K");
            assert_eq!(mgr.resolve_quant_variant(REPO).await, "Q6_K");
            assert_eq!(
                hits.load(Ordering::SeqCst),
                1,
                "second resolution must hit the cache, not HF"
            );
        }

        #[tokio::test]
        async fn quant_lookup_failure_falls_back_to_cli_default() {
            // The default test config points HF at a closed port.
            let mgr = UnslothServerManager::with_config(test_config(
                PathBuf::from("/nonexistent/unsloth"),
                PathBuf::from("/nonexistent"),
                1,
            ));
            assert_eq!(mgr.resolve_quant_variant(REPO).await, "UD-Q4_K_XL");
            assert_eq!(
                mgr.resolve_quant_variant("other-org/model-GGUF").await,
                "Q4_K_M"
            );
        }

        #[tokio::test]
        async fn shutdown_kills_the_server_child() {
            let dir = tempfile::tempdir().expect("tempdir");
            let port = spawn_stub_http("sk-unsloth-test-key").await;
            let binary = write_stub_binary(dir.path(), dir.path(), port, None);
            let mgr = UnslothServerManager::with_config(test_config(
                binary,
                dir.path().to_path_buf(),
                port,
            ));
            mgr.ensure_endpoint(REPO, &|_| {}).await.expect("starts");
            let pid = {
                let mut state = mgr.state.lock().await;
                let server = state.as_mut().expect("server tracked");
                assert!(server.is_alive(), "child alive before shutdown");
                server.child.id().expect("pid")
            };
            mgr.shutdown().await;
            assert!(mgr.state.lock().await.is_none(), "state cleared");
            // The killed pid must no longer be a live process (signal 0 probe).
            let alive = unsafe { libc_kill_probe(pid) };
            assert!(!alive, "server child must be dead after shutdown");
        }

        /// `kill(pid, 0)` liveness probe via nix.
        unsafe fn libc_kill_probe(pid: u32) -> bool {
            nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid as i32), None).is_ok()
        }

        #[tokio::test]
        async fn child_exit_during_startup_surfaces_output_tail() {
            let dir = tempfile::tempdir().expect("tempdir");
            // `run` prints a diagnostic and exits immediately — the socket
            // never opens, so startup must fail fast with the tail attached.
            let binary = write_stub_binary(
                dir.path(),
                dir.path(),
                1, // unused port: the child exits before any probe succeeds
                Some("echo 'model not found: boom' >&2; exit 3"),
            );
            let mgr =
                UnslothServerManager::with_config(test_config(binary, dir.path().to_path_buf(), 1));
            let err = mgr.ensure_endpoint(REPO, &|_| {}).await.unwrap_err();
            let msg = err.to_string();
            assert!(msg.contains("exited during startup"), "got: {msg}");
            assert!(
                msg.contains("model not found: boom"),
                "tail attached: {msg}"
            );
            assert!(
                mgr.state.lock().await.is_none(),
                "failed startup must clear state so the next attempt starts clean"
            );
        }

        #[tokio::test]
        async fn shutdown_aborts_in_flight_startup_without_waiting_out_the_timeout() {
            let dir = tempfile::tempdir().expect("tempdir");
            // `run` sleeps but never opens the HTTP socket, so the startup
            // sits in its phase-1 probe loop for the whole (long) window
            // unless shutdown aborts it.
            let binary = write_stub_binary(dir.path(), dir.path(), 1, None);
            let mut config = test_config(binary, dir.path().to_path_buf(), 1);
            config.server_up_timeout = Duration::from_secs(600);
            let mgr = Arc::new(UnslothServerManager::with_config(config));

            let m2 = mgr.clone();
            let startup = tokio::spawn(async move { m2.ensure_endpoint(REPO, &|_| {}).await });
            // Give the startup time to spawn the child and enter the loop.
            tokio::time::sleep(Duration::from_millis(200)).await;

            let start = tokio::time::Instant::now();
            mgr.shutdown().await;
            assert!(
                start.elapsed() < Duration::from_secs(30),
                "shutdown must not wait out the startup window"
            );
            let err = startup.await.expect("join").unwrap_err();
            assert!(err.to_string().contains("shutting down"), "got: {err}");
            assert!(mgr.state.lock().await.is_none(), "no server left tracked");

            // Post-shutdown spawns are refused outright.
            let err = mgr.ensure_endpoint(REPO, &|_| {}).await.unwrap_err();
            assert!(err.to_string().contains("shutting down"), "got: {err}");
        }

        #[tokio::test]
        async fn mint_failure_fails_startup() {
            let dir = tempfile::tempdir().expect("tempdir");
            let port = spawn_stub_http("sk-unsloth-test-key").await;
            let binary_path = dir.path().join("unsloth");
            // Stub whose `start` arm fails (no generated config written).
            let script = "#!/bin/sh\ncase \"$1\" in\n  run) sleep 300 ;;\n  start) echo 'No running Unsloth server found' >&2; exit 1 ;;\nesac\n".to_string();
            std::fs::write(&binary_path, script).expect("write stub");
            std::fs::set_permissions(&binary_path, std::fs::Permissions::from_mode(0o755))
                .expect("chmod");
            let mgr = UnslothServerManager::with_config(test_config(
                binary_path,
                dir.path().to_path_buf(),
                port,
            ));
            let err = mgr.ensure_endpoint(REPO, &|_| {}).await.unwrap_err();
            let msg = err.to_string();
            assert!(
                msg.contains("No running Unsloth server found"),
                "got: {msg}"
            );
            assert!(mgr.state.lock().await.is_none(), "state cleared on failure");
        }
    }
}
