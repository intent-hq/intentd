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
//! Lifecycle policy (validated against a real install, 2026-07-27; revised
//! 2026-07-27 after dogfooding a first-use 16.7 GB download, monorepo#878):
//! - start on demand at agent spawn; reuse the running server while it serves
//!   the requested repo; kill + respawn on model switch or a dead child.
//! - the server requires auth even on `/v1/models` — an HTTP 401/403 during
//!   probing means "server up, model maybe still loading", not failure.
//! - first use can mean a multi-GB Hugging Face download, and — contrary to
//!   the original assumption — `unsloth start opencode --no-launch` can
//!   itself be the step that performs/waits on that download rather than the
//!   post-mint readiness probe. Both the mint invocation and the model-ready
//!   probe therefore get the same generous, progress-aware deadline (status
//!   updates surfaced through the caller-supplied callback throughout); only
//!   a dead managed-server child fails the wait early.
//! - a startup wait that times out (or a caller's spawn retry) while the
//!   managed server process is still alive does NOT kill it: the server (and
//!   any in-flight download it owns) is left running, and the NEXT
//!   `ensure_endpoint` call for the same repo attaches to and waits on that
//!   same server instead of killing + respawning it and discarding progress.
//!   Only a genuinely dead child, a model switch, or an explicit shutdown
//!   tears the server down.
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
use std::time::{Duration, Instant};

use intent_core::{Error, Result};
use intent_providers::{UnslothEndpoint, UnslothModelLimit};
use serde_json::Value;
use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
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
/// opencode auth material. Dogfooding (monorepo#878) showed this step can
/// itself perform/wait on the first-use multi-GB model download — not just
/// the post-mint readiness probe — so it shares [`MODEL_READY_TIMEOUT`]'s
/// generous deadline rather than a short fixed one; the mint wait polls
/// progressively (status updates, fails fast the moment the managed server
/// process dies) instead of blocking atomically on the command's exit.
const MINT_TIMEOUT: Duration = MODEL_READY_TIMEOUT;

/// How long the ADOPT-time mint probe may take (versus the generous
/// [`MINT_TIMEOUT`] for a daemon-owned startup). Adoption only takes an
/// occupant that is READY — a genuinely ready server mints in seconds — and
/// the daemon cannot observe a foreign process to fail fast on its death,
/// so a stalling occupant must degrade to the re-port path quickly instead
/// of holding the spawn (and the state lock) for the full download window.
const ADOPT_MINT_TIMEOUT: Duration = Duration::from_secs(30);

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

/// Severity of a [`StatusCallback`] message, mapping onto the
/// `agent:stream:status` event's `level` field (`"info"` / `"warning"`).
/// Progress updates are `Info`; `Warning` is reserved for disruptive
/// transitions the user should notice (e.g. a model switch restarting the
/// server out from under live agents).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StatusLevel {
    Info,
    Warning,
}

impl StatusLevel {
    /// The wire-level `level` string for `agent:stream:status` events.
    pub fn as_str(self) -> &'static str {
        match self {
            StatusLevel::Info => "info",
            StatusLevel::Warning => "warning",
        }
    }
}

/// Status callback: receives short human-readable progress messages
/// ("starting server", "downloading/loading model…") plus a severity the
/// caller surfaces to the user (e.g. as `agent:stream:status` events with
/// the matching `level`).
pub(crate) type StatusCallback = dyn Fn(StatusLevel, String) + Send + Sync;

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

/// The error an in-flight `ensure_endpoint` gets when `unsloth.stop` aborted
/// it mid-startup (not terminal — a later `ensure_endpoint` call may start a
/// new server).
fn stop_requested_error() -> Error {
    Error::Internal("unsloth server startup aborted: unsloth.stop was called".to_string())
}

/// The error for an unresolvable port conflict: the configured port is busy,
/// the occupant is not an adoptable Unsloth server serving the requested
/// model, and no free port could be picked. `InvalidInput` (not `Internal`)
/// for the same reason as [`missing_binary_error`]: this is an environment
/// problem the user must resolve, and the message must survive the JSON-RPC
/// envelope.
fn port_conflict_error(port: u16) -> Error {
    Error::InvalidInput(format!(
        "port {port} is already in use by another process and no free port could be \
         found — stop the process occupying port {port} and retry"
    ))
}

/// Whether the daemon could bind `port` on loopback right now (the managed
/// server binds it moments later — the gap is an inherent, acceptable
/// TOCTOU: a lost race surfaces as the child's own bind failure).
fn loopback_port_is_free(port: u16) -> bool {
    std::net::TcpListener::bind(("127.0.0.1", port)).is_ok()
}

/// Pick a free loopback port (OS-assigned ephemeral); `None` when even an
/// ephemeral bind fails.
fn pick_free_loopback_port() -> Option<u16> {
    std::net::TcpListener::bind(("127.0.0.1", 0))
        .and_then(|l| l.local_addr())
        .map(|a| a.port())
        .ok()
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

/// Lock a std mutex, recovering from poisoning: the guarded data here is a
/// plain map with no cross-field invariants, so a panic in another holder
/// can't have left it inconsistent — and an agent spawn must never crash
/// over a poisoned cache lock.
fn lock_ignore_poison<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
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
/// suffixes (`-NNNNN-of-NNNNN`) are stripped before the tag is read. When
/// the filename carries no recognizable trailing tag, the parent directory
/// name is tried (some repos use `Q8_0/model-00001-of-00002.gguf`
/// layouts); paths that are not `.gguf` files or carry no tag in either
/// place return `None`.
pub(crate) fn quant_tag_from_gguf_path(path: &str) -> Option<String> {
    let (dir, file) = match path.rsplit_once('/') {
        Some((dir, file)) => (Some(dir), file),
        None => (None, path),
    };
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
    if is_variant_token(&tag) {
        return if tokens.last().is_some_and(|t| t.eq_ignore_ascii_case("UD")) {
            Some(format!("UD-{tag}"))
        } else {
            Some(tag)
        };
    }
    let dir_name = dir?.rsplit('/').next()?.to_ascii_uppercase();
    let fits = is_variant_token(dir_name.strip_prefix("UD-").unwrap_or(&dir_name));
    fits.then_some(dir_name)
}

/// Parse a Hugging Face per-repo file listing
/// (`/api/models/<repo>?blobs=true` — `siblings` entries carry `rfilename`
/// and `size`) into total bytes per quant-variant tag. Multi-part GGUFs
/// sum all their parts; non-GGUF files and entries without a size are
/// skipped. Malformed JSON yields an empty map — the caller treats "no
/// size data" as "use the CLI default". Vision repos' `mmproj-*.gguf`
/// projector files are summed into their tag's total; that slightly
/// overestimates a quant's footprint, which is the conservative direction
/// (and the projector is resident at runtime anyway).
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

/// Snapshot of the managed server's live state, for the `unsloth.status`
/// RPC. Constructed by [`UnslothServerManager::status_snapshot`]; `None`
/// from that method means no server is running.
#[derive(Debug, Clone)]
pub(crate) struct UnslothStatus {
    /// Full HF repo id currently served (or being started).
    pub repo_id: String,
    /// Port the server actually listens on — may differ from the configured
    /// port when the daemon re-ported around a conflict, and is the
    /// occupant's port for an adopted (externally started) server.
    pub port: u16,
    /// OS pid of the managed server child, when known. `None` for an
    /// adopted server (the daemon does not own the occupant's process).
    pub pid: Option<u32>,
    /// Seconds since the child was spawned — or, for an adopted server
    /// (`pid: None`), since the daemon adopted it (the occupant's true
    /// start time is unobservable).
    pub uptime_secs: u64,
    /// Coarse startup phase: `"starting"`, `"minting"`, `"loading"`, or
    /// `"ready"`.
    pub phase: String,
    /// CPU percent summed across the server's process tree (raw `sysinfo`
    /// convention: 100 = one full core), sampled at snapshot time. `0.0`
    /// when the pid is unknown or the sample failed.
    pub cpu_percent: f32,
    /// Resident memory (bytes) summed across the server's process tree —
    /// `unsloth` spawns `llama-server` as a child that holds the model
    /// weights, so the tree total (not just the root process) is what
    /// matters for capacity planning.
    pub memory_bytes: u64,
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

/// Lightweight, `Clone`-able mirror of a [`ManagedServer`]'s identity, kept
/// under its own `std::sync::Mutex` so `unsloth.status` never has to take the
/// startup-serializing `state` [`TokioMutex`] (which a spawn can legitimately
/// hold for up to [`MODEL_READY_TIMEOUT`]). Updated at every point
/// `state`'s `Some`/`None`-ness changes (spawn, teardown-for-respawn, failed
/// startup, `stop`, `shutdown`).
#[derive(Clone)]
struct ServerIdentity {
    repo_id: String,
    pid: Option<u32>,
    started_at: Instant,
    /// The port the server actually listens on (see [`ManagedServer::port`]).
    port: u16,
}

/// One managed server child: the process handle plus what it was started
/// with, so reuse/restart decisions compare against the live state.
struct ManagedServer {
    /// The spawned server process, or `None` for an ADOPTED server — a
    /// compatible Unsloth server someone else started on the configured
    /// port that the daemon reuses but does not own (and must never kill).
    child: Option<Child>,
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
    /// When the child was spawned; feeds `unsloth.status`'s uptime field.
    started_at: Instant,
    /// The port the server actually listens on: the configured port
    /// normally, a picked free port after a re-port around a conflict, or
    /// the occupant's (configured) port for an adopted server.
    port: u16,
}

/// How long [`ManagedServer::tail`] waits for the drain tasks to consume the
/// dead child's final output before snapshotting.
const DRAIN_SETTLE_TIMEOUT: Duration = Duration::from_secs(2);

impl ManagedServer {
    /// Whether the child process is still running (`try_wait` probe). An
    /// adopted server (no child handle) is always reported alive here — its
    /// liveness is only observable through the HTTP readiness probes.
    fn is_alive(&mut self) -> bool {
        match self.child.as_mut() {
            Some(child) => matches!(child.try_wait(), Ok(None)),
            None => true,
        }
    }

    /// OS pid of the owned child; `None` for an adopted server.
    fn pid(&self) -> Option<u32> {
        self.child.as_ref().and_then(|c| c.id())
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

/// Resolver seam for the `unsloth` CLI binary. The argument is the
/// `providers.paths["unsloth"]` explicit override (read from settings at each
/// `ensure_endpoint` call), applied as the explicit-path tier of
/// `find_provider_binary` — an invalid override warns and falls through to
/// the normal resolution tiers.
type ResolveBinaryFn = Box<dyn Fn(Option<&str>) -> Option<PathBuf> + Send + Sync>;

/// Injectable configuration seams (tests override every external surface:
/// binary resolution, home dir, port, timeouts, HF metadata fetch, RAM).
pub(crate) struct UnslothConfig {
    /// Resolve the `unsloth` binary; `None` = not installed.
    pub resolve_binary: ResolveBinaryFn,
    /// Home directory used to locate the generated opencode.json.
    pub home_dir: Option<PathBuf>,
    /// Port passed to `unsloth run -p`.
    pub port: u16,
    pub server_up_timeout: Duration,
    pub model_ready_timeout: Duration,
    pub probe_interval: Duration,
    pub mint_timeout: Duration,
    /// Deadline for the ADOPT-time mint against a foreign occupant (short:
    /// a ready occupant mints in seconds; a stalling one must degrade to
    /// the re-port path instead of holding the spawn for the full window).
    pub adopt_mint_timeout: Duration,
    /// Hugging Face API base for the per-repo file listing the quant
    /// selection uses (tests point this at a loopback stub).
    pub hf_api_base: String,
    /// Timeout for that per-repo file-listing fetch.
    pub hf_files_timeout: Duration,
    /// Total system RAM probe; `None` = detection unsupported.
    pub total_memory_bytes: Box<dyn Fn() -> Option<u64> + Send + Sync>,
    /// Whether a given loopback port can be bound right now (port-conflict
    /// detection; tests override to simulate a busy configured port).
    pub port_is_free: Box<dyn Fn(u16) -> bool + Send + Sync>,
    /// Pick a free loopback port for a re-port around a conflict; `None` =
    /// no port available (tests override to force the unrecoverable path).
    pub pick_free_port: Box<dyn Fn() -> Option<u16> + Send + Sync>,
}

impl Default for UnslothConfig {
    fn default() -> Self {
        Self {
            resolve_binary: Box::new(|explicit| {
                intent_providers::find_provider_binary("unsloth", "unsloth", explicit)
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
            adopt_mint_timeout: ADOPT_MINT_TIMEOUT,
            hf_api_base: HF_API_BASE.to_string(),
            hf_files_timeout: HF_FILES_TIMEOUT,
            total_memory_bytes: Box::new(crate::agent_manager::total_memory_bytes),
            port_is_free: Box::new(loopback_port_is_free),
            pick_free_port: Box::new(pick_free_loopback_port),
        }
    }
}

/// Singleton manager for the daemon-owned Unsloth server. One instance lives
/// on the [`crate::agent_manager::AgentManager`]; the async mutex serializes
/// concurrent unsloth-agent spawns so exactly one start/restart runs at a
/// time (the second spawner reuses the endpoint the first one produced).
pub(crate) struct UnslothServerManager {
    state: TokioMutex<Option<ManagedServer>>,
    config: UnslothConfig,
    /// Quant variants already selected this daemon lifetime, keyed by repo
    /// id. File sizes and total RAM don't change while the daemon runs, so
    /// a successful selection is worth exactly one HF round-trip per repo;
    /// default fallbacks (failed fetch, malformed listing, no usable size
    /// data) are NOT cached, so transient HF issues retry on the next spawn.
    quant_cache: Mutex<HashMap<String, String>>,
    /// Terminal shutdown latch. [`Self::shutdown`] sets it BEFORE taking the
    /// state lock so an in-flight startup (which can legitimately sit in its
    /// probe loop for many minutes during a first-use model download) notices
    /// at the next probe tick, aborts, and releases the lock — daemon
    /// shutdown never waits out the model-ready window.
    shutting_down: std::sync::atomic::AtomicBool,
    /// One-shot latch set by [`Self::stop`] BEFORE taking the state lock, so
    /// an in-flight `ensure_endpoint` startup polling loop
    /// ([`Self::wait_until`]) notices at its next probe tick (≤
    /// [`UnslothConfig::probe_interval`]) and aborts instead of leaving
    /// `unsloth.stop` blocked behind the state lock for up to
    /// [`MODEL_READY_TIMEOUT`]. Unlike `shutting_down` this is NOT terminal:
    /// it is reset to `false` once consumed (by the aborted startup's
    /// teardown, or by `stop` itself when there was nothing to interrupt) so
    /// a later `ensure_endpoint` is unaffected. Checked in every startup
    /// polling loop, including [`Self::mint_endpoint`]'s.
    stop_requested: std::sync::atomic::AtomicBool,
    /// Coarse startup phase surfaced by `unsloth.status` (`"starting"`,
    /// `"minting"`, `"loading"`, `"ready"`); `None` when no server is
    /// running. Updated at each stage transition in
    /// [`Self::ensure_endpoint`]/[`Self::wait_and_mint`] and cleared on
    /// teardown (dead child, model switch, failed startup, or shutdown).
    phase: Mutex<Option<&'static str>>,
    /// Lightweight, lock-free-to-read mirror of the live server's identity
    /// (repo, pid, spawn time), kept in lockstep with `state`'s
    /// `Some`/`None`-ness. `unsloth.status` reads ONLY this (plus `phase`,
    /// both plain `std::sync::Mutex`es) so it never contends with `state`'s
    /// `TokioMutex` — which `ensure_endpoint` can hold across
    /// minutes-long startup awaits — keeping status observability
    /// responsive even mid-download.
    identity: Mutex<Option<ServerIdentity>>,
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
            stop_requested: std::sync::atomic::AtomicBool::new(false),
            phase: Mutex::new(None),
            identity: Mutex::new(None),
        }
    }

    /// Whether [`Self::shutdown`] has been requested.
    fn is_shutting_down(&self) -> bool {
        self.shutting_down
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Whether [`Self::stop`] has an outstanding, not-yet-consumed request.
    fn is_stop_requested(&self) -> bool {
        self.stop_requested
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Set the coarse startup phase (`unsloth.status`'s `phase` field).
    fn set_phase(&self, phase: Option<&'static str>) {
        *lock_ignore_poison(&self.phase) = phase;
    }

    /// Update the identity mirror (`unsloth.status`'s lock-free read path).
    fn set_identity(&self, identity: Option<ServerIdentity>) {
        *lock_ignore_poison(&self.identity) = identity;
    }

    /// Snapshot the managed server's live state for `unsloth.status`; `None`
    /// when no server is running OR the last-known pid is no longer alive
    /// (a dead child not yet reaped by the next `ensure_endpoint` call —
    /// checked via a unix signal-0 liveness probe, best-effort elsewhere).
    /// An ADOPTED server has no pid to probe, so its staleness is NOT
    /// detectable here: a vanished occupant keeps reporting until the next
    /// `ensure_endpoint` call's HTTP re-verification notices (adding an
    /// HTTP probe here would trade status responsiveness for it).
    /// Deliberately reads only the `identity`/`phase` mirrors (both plain
    /// `std::sync::Mutex`es), never `state`'s `TokioMutex`, so this stays
    /// responsive even while a startup is in flight (see [`Self::identity`]).
    /// `cpu_percent`/`memory_bytes` sum the server's whole process tree
    /// (`unsloth` spawns `llama-server` as a child holding the model
    /// weights) — best-effort: a sampling failure yields zeros rather than
    /// failing the whole snapshot.
    pub(crate) async fn status_snapshot(&self) -> Option<UnslothStatus> {
        let identity = lock_ignore_poison(&self.identity).clone()?;
        if let Some(pid) = identity.pid {
            if !pid_is_alive(pid) {
                return None;
            }
        }
        let phase = lock_ignore_poison(&self.phase)
            .unwrap_or("starting")
            .to_string();
        let (cpu_percent, memory_bytes) = match identity.pid {
            Some(pid) => sample_process_tree(pid).await,
            None => (0.0, 0),
        };
        Some(UnslothStatus {
            repo_id: identity.repo_id,
            port: identity.port,
            pid: identity.pid,
            uptime_secs: identity.started_at.elapsed().as_secs(),
            phase,
            cpu_percent,
            memory_bytes,
        })
    }

    /// Stop the managed server if one is running, returning whether one was
    /// actually stopped (`false` = already stopped, a no-op). Equivalent to
    /// [`Self::shutdown`] but does not set the terminal shutdown latch — a
    /// later `ensure_endpoint` call may start a new server. Sets
    /// [`Self::stop_requested`] BEFORE taking the state lock so an in-flight
    /// startup's polling loop aborts within one probe tick instead of
    /// blocking this call for the full startup window.
    pub async fn stop(&self) -> bool {
        let was_running = lock_ignore_poison(&self.identity).is_some();
        if !was_running {
            return false;
        }
        self.stop_requested
            .store(true, std::sync::atomic::Ordering::Relaxed);
        {
            let mut state = self.state.lock().await;
            if let Some(mut server) = state.take() {
                tracing::info!(repo = %server.repo_id, "stopping managed unsloth server (unsloth.stop)");
                if let Some(child) = server.child.as_mut() {
                    kill_server_child(child).await;
                }
                self.set_phase(None);
                self.set_identity(None);
            }
        }
        // Reset the one-shot latch whether we performed the teardown above or
        // an aborted `ensure_endpoint` already did (see `stop_requested`'s
        // doc comment) — either way the request has been fulfilled.
        self.stop_requested
            .store(false, std::sync::atomic::Ordering::Relaxed);
        true
    }

    /// Ensure a managed server is running and ready for `repo_id`, returning
    /// the endpoint to inject into the opencode spawn env. Reuses the live
    /// server when it already serves `repo_id`; kills + respawns on model
    /// switch or a dead child. `status` receives human-readable progress
    /// messages while a (potentially multi-GB first-use download) startup is
    /// in flight.
    ///
    /// `attached_agents` is the number of live agent sessions currently
    /// spawned against the managed server (the caller's
    /// `count_agents_with_provider("unsloth")` snapshot). When a model
    /// switch is about to kill a LIVE server serving a different repo and
    /// `attached_agents > 0`, a [`StatusLevel::Warning`] status message is
    /// emitted BEFORE the restart so clients can surface that those
    /// sessions are losing their model. A dead child's teardown never
    /// warns — there is no live server to disrupt.
    ///
    /// Retry reuse (monorepo#878): when a live server already serves
    /// `repo_id` but hasn't finished starting yet (no minted endpoint —
    /// e.g. a caller's spawn retry landed here while the FIRST attempt's
    /// wait is still, or again, in flight), this attaches to and waits on
    /// that SAME server rather than killing + respawning it. Killing on
    /// every retry would discard an in-flight multi-GB download's progress.
    /// This applies identically to the server a model switch just spawned:
    /// the switch's kill-old-then-spawn-new step runs through the same
    /// spawn + [`Self::wait_and_mint`] path as a fresh start, so a retry for
    /// the switched-to repo attaches to that new (possibly still cold-
    /// loading) server rather than tearing it down and spawning a third one
    /// (dogfooding repro: switching models mid-download hit the same 60s
    /// mint deadline this fix addresses, not just first-use starts).
    /// `unsloth_cli_override` is the `providers.paths["unsloth"]` setting
    /// (the caller's per-call settings snapshot), threaded into
    /// [`UnslothConfig::resolve_binary`] as the explicit-path tier for the
    /// `unsloth` CLI — it never affects the opencode ACP primary. The
    /// reuse/attach early-return paths below return before `resolve_binary`
    /// runs, so a changed override takes effect on the next cold start or
    /// model switch, not while a healthy managed server is being reused.
    pub async fn ensure_endpoint(
        &self,
        repo_id: &str,
        unsloth_cli_override: Option<&str>,
        attached_agents: usize,
        status: &StatusCallback,
    ) -> Result<UnslothEndpoint> {
        if self.is_shutting_down() {
            return Err(shutting_down_error());
        }
        let mut state = self.state.lock().await;

        // Live child serving the requested repo: reuse the minted endpoint
        // outright, or — if it's still starting — attach to it instead of
        // tearing it down. An ADOPTED server (no owned child) is re-verified
        // with one authed probe before reuse: the daemon cannot observe the
        // occupant's process, so HTTP is the only liveness signal — a
        // vanished occupant falls through to the normal teardown + port
        // logic instead of being reused forever.
        let mut attach = false;
        if let Some(server) = state.as_mut() {
            if server.repo_id == repo_id && server.is_alive() {
                match server.endpoint.clone() {
                    Some(ep) => {
                        let owned = server.child.is_some();
                        if owned || self.adopted_endpoint_still_ready(server.port, &ep).await {
                            return Ok(ep);
                        }
                    }
                    None => attach = true,
                }
            }
        }

        if attach {
            tracing::info!(
                repo = %repo_id,
                "attaching to already-starting unsloth server instead of respawning (retry reuse)"
            );
        } else if let Some(mut old) = state.take() {
            // Model switch or dead/half-started child: tear down and respawn.
            // A LIVE server being replaced for a different repo with agent
            // sessions still attached is user-visible disruption — warn
            // before the kill so clients can surface it (a dead child's
            // teardown has nothing left to disrupt, and dropping an ADOPTED
            // server never kills the occupant — it keeps running for its
            // owner, so there is nothing to warn about either).
            if old.child.is_some()
                && old.repo_id != repo_id
                && old.is_alive()
                && attached_agents > 0
            {
                let plural = if attached_agents == 1 { "" } else { "s" };
                status(
                    StatusLevel::Warning,
                    format!(
                        "Switching Unsloth model from {old} to {repo_id} restarts the local server — {attached_agents} live agent session{plural} on {old} will lose the loaded model",
                        old = old.repo_id,
                    ),
                );
            }
            tracing::info!(
                old_repo = %old.repo_id,
                new_repo = %repo_id,
                "stopping managed unsloth server (model switch or dead child)"
            );
            if let Some(child) = old.child.as_mut() {
                kill_server_child(child).await;
            }
            self.set_phase(None);
            self.set_identity(None);
        }

        let binary =
            (self.config.resolve_binary)(unsloth_cli_override).ok_or_else(missing_binary_error)?;

        if !attach {
            status(
                StatusLevel::Info,
                format!("Starting Unsloth server for {repo_id}…"),
            );
            // Resolve the quant BEFORE the port: the HF fetch can take up
            // to [`UnslothConfig::hf_files_timeout`], and a port picked
            // ahead of it would sit unclaimed for that long — widening the
            // window for another process to grab it before the spawn binds.
            let quant = self.resolve_quant_variant(repo_id).await;
            // Resolve the listen port BEFORE any spawn: the configured
            // port may be held by a user-run unsloth studio (adoptable) or
            // an unrelated process (re-port around it) — either way the
            // daemon must not spawn a child doomed to fail its bind.
            let configured = self.config.port;
            let port = if (self.config.port_is_free)(configured) {
                configured
            } else {
                if let Some((server, endpoint)) =
                    self.try_adopt(&binary, repo_id, configured, status).await
                {
                    status(
                        StatusLevel::Info,
                        format!(
                            "Reusing the Unsloth server already running on port {configured} — it serves {repo_id}"
                        ),
                    );
                    tracing::info!(
                        port = configured,
                        repo = %repo_id,
                        "adopted existing unsloth server on the configured port"
                    );
                    self.set_identity(Some(ServerIdentity {
                        repo_id: server.repo_id.clone(),
                        pid: None,
                        started_at: server.started_at,
                        port: configured,
                    }));
                    *state = Some(server);
                    self.set_phase(Some("ready"));
                    return Ok(endpoint);
                }
                match (self.config.pick_free_port)() {
                    Some(picked) => {
                        status(
                            StatusLevel::Info,
                            format!(
                                "Port {configured} is in use by another process; starting the Unsloth server on port {picked} instead"
                            ),
                        );
                        tracing::info!(
                            configured_port = configured,
                            picked_port = picked,
                            "configured unsloth port is busy; re-porting to a free one"
                        );
                        picked
                    }
                    None => return Err(port_conflict_error(configured)),
                }
            };
            // Re-check both latches: the HF fetch and a failed adoption
            // attempt above can each take a while, and a shutdown or
            // `unsloth.stop` may have arrived meanwhile — don't spawn a
            // child nobody will reap (a stop mid-`try_adopt` surfaces as a
            // swallowed mint failure, so the latch is still set here; it is
            // consumed exactly like an aborted startup's teardown would).
            if self.is_shutting_down() {
                return Err(shutting_down_error());
            }
            if self.is_stop_requested() {
                self.stop_requested
                    .store(false, std::sync::atomic::Ordering::Relaxed);
                return Err(stop_requested_error());
            }
            let server = self.start_server(&binary, repo_id, &quant, port)?;
            self.set_identity(Some(ServerIdentity {
                repo_id: server.repo_id.clone(),
                pid: server.pid(),
                started_at: server.started_at,
                port: server.port,
            }));
            *state = Some(server);
            self.set_phase(Some("starting"));
        } else {
            status(
                StatusLevel::Info,
                format!(
                    "Unsloth server for {repo_id} is already starting; waiting for it to become ready…"
                ),
            );
        }

        match self
            .wait_and_mint(&binary, repo_id, state.as_mut().unwrap(), status)
            .await
        {
            Ok(endpoint) => {
                state.as_mut().unwrap().endpoint = Some(endpoint.clone());
                self.set_phase(Some("ready"));
                Ok(endpoint)
            }
            Err(failure) if failure.preserve_server && !self.is_shutting_down() => {
                // A deadline timeout with the managed server still alive
                // (monorepo#878): leave it running. The next
                // `ensure_endpoint` call for this repo attaches to it above
                // instead of respawning and discarding an in-flight
                // download.
                tracing::warn!(
                    repo = %repo_id,
                    error = %failure.error,
                    "unsloth startup wait timed out but the managed server is still alive; leaving it running for a retry to attach to"
                );
                Err(failure.error)
            }
            Err(failure) => {
                // Genuinely dead child, mint failure, shutdown, or
                // `unsloth.stop` interrupting a long-running startup wait
                // (see `stop_requested`'s doc comment): tear down so the
                // next attempt starts clean, and surface the output tail
                // (with any minted key material redacted — the error is
                // client-visible).
                let mut failed = state.take().expect("state set above");
                self.set_phase(None);
                self.set_identity(None);
                self.stop_requested
                    .store(false, std::sync::atomic::Ordering::Relaxed);
                if let Some(child) = failed.child.as_mut() {
                    kill_server_child(child).await;
                }
                let tail = redact_key_material(&failed.tail().await);
                if tail.is_empty() {
                    Err(failure.error)
                } else {
                    Err(Error::Internal(format!(
                        "{}\nserver output tail:\n{tail}",
                        failure.error
                    )))
                }
            }
        }
    }

    /// Resolve the quant variant to serve for `repo_id`: the best-fitting
    /// one from the repo's actual GGUF file sizes when the HF listing is
    /// reachable ([`best_fitting_quant`]), the CLI-default variant
    /// otherwise. A slow or failing HF fetch degrades to the default within
    /// [`UnslothConfig::hf_files_timeout`] — it never fails the spawn. Only
    /// real selections are cached: default fallbacks (fetch failure, no
    /// usable size data, unknown RAM) retry the lookup on the next spawn.
    async fn resolve_quant_variant(&self, repo_id: &str) -> String {
        // The cache mutex guards a plain HashMap (no invariants can be
        // violated mid-panic), so a poisoned lock is safe to keep using —
        // never crash an agent spawn over it.
        if let Some(quant) = lock_ignore_poison(&self.quant_cache).get(repo_id) {
            return quant.clone();
        }
        let selected = match self.fetch_repo_file_listing(repo_id).await {
            Ok(body) => {
                let sizes = parse_repo_quant_sizes(&body);
                best_fitting_quant(&sizes, (self.config.total_memory_bytes)())
            }
            Err(reason) => {
                tracing::warn!(
                    repo = %repo_id,
                    reason = %reason,
                    "unsloth quant-variant lookup failed; using CLI default"
                );
                None
            }
        };
        match selected {
            Some(quant) => {
                tracing::info!(repo = %repo_id, quant = %quant, "selected unsloth quant variant");
                lock_ignore_poison(&self.quant_cache).insert(repo_id.to_string(), quant.clone());
                quant
            }
            None => default_quant_variant(repo_id).to_string(),
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
    /// as its own process-group leader with captured output. `port` is the
    /// resolved listen port (the configured one, or a picked free one when
    /// the configured port was busy).
    fn start_server(
        &self,
        binary: &Path,
        repo_id: &str,
        quant: &str,
        port: u16,
    ) -> Result<ManagedServer> {
        let mut cmd = Command::new(binary);
        cmd.arg("run")
            .arg("--model")
            .arg(run_model_arg(repo_id, quant))
            .arg("--disable-tools")
            .arg("-p")
            .arg(port.to_string())
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

        tracing::info!(repo = %repo_id, quant = %quant, port, "spawned managed unsloth server");
        Ok(ManagedServer {
            child: Some(child),
            repo_id: repo_id.to_string(),
            endpoint: None,
            output_tail,
            drain_tasks,
            started_at: Instant::now(),
            port,
        })
    }

    /// Try to adopt the server occupying the (busy) configured port: probe
    /// `/v1/models` unauthenticated — 401/403 means an auth-requiring
    /// Unsloth-like server ([`classify_probe`]'s convention, spec Status
    /// section) — then mint the opencode auth material against it and
    /// verify with an authed probe that it actually answers 200 (i.e. it
    /// serves the requested model, ready). `None` on any mismatch: a
    /// non-Unsloth occupant, a mint failure (e.g. it serves a different
    /// model), or a not-ready occupant — the caller then re-ports instead.
    /// Deliberately conservative: adoption only takes a READY server, so a
    /// mid-startup occupant is treated as busy rather than waited on (the
    /// daemon does not own it and cannot observe its process), and the
    /// adopt-time mint runs under the short
    /// [`UnslothConfig::adopt_mint_timeout`] — never the download-tolerant
    /// [`UnslothConfig::mint_timeout`] — so a stalling occupant degrades to
    /// the re-port path in seconds instead of holding the state lock.
    ///
    /// Side effect, accepted by design: the mint runs `unsloth start
    /// opencode --no-launch --model <repo>` against the user-owned
    /// occupant, which rewrites the generated
    /// `~/.unsloth/studio/auth/agents/opencode/opencode.json` even when
    /// adoption is then declined. That file is CLI-generated auth material
    /// (re-minted on every `unsloth start` invocation, including the
    /// daemon's own spawns on other ports), not user-authored config, so a
    /// declined adoption leaves it pointing at the occupant — exactly what
    /// a manual `unsloth start opencode` against that studio would produce.
    async fn try_adopt(
        &self,
        binary: &Path,
        repo_id: &str,
        port: u16,
        status: &StatusCallback,
    ) -> Option<(ManagedServer, UnslothEndpoint)> {
        let probe_url = format!("http://127.0.0.1:{port}/v1/models");
        let client = reqwest::Client::builder()
            .timeout(PROBE_REQUEST_TIMEOUT)
            .build()
            .ok()?;
        match classify_probe(probe_status(&client, &probe_url, None).await) {
            // 401/403 (or any non-200 HTTP answer) — an Unsloth-like server
            // is answering; try to mint against it.
            ProbeOutcome::UpNotReady => {}
            // 200 unauthenticated is NOT the managed server's shape (it
            // requires auth even on /v1/models) — treat as foreign.
            ProbeOutcome::Ready => return None,
            // The port is bind-busy but nothing answers HTTP: not an
            // Unsloth server (e.g. a raw TCP service).
            ProbeOutcome::Down => return None,
        }

        let mut server = ManagedServer {
            child: None,
            repo_id: repo_id.to_string(),
            endpoint: None,
            output_tail: Arc::new(Mutex::new(std::collections::VecDeque::new())),
            drain_tasks: Vec::new(),
            started_at: Instant::now(),
            port,
        };
        let endpoint = self
            .mint_endpoint(
                binary,
                repo_id,
                &mut server,
                status,
                self.config.adopt_mint_timeout,
            )
            .await
            .ok()?;
        // The minted key must prove the occupant serves the requested model
        // NOW (200, ready) — adoption never waits on a foreign server's
        // download: if it isn't ready, re-port and own the startup instead.
        if classify_probe(probe_status(&client, &probe_url, Some(&endpoint.api_key)).await)
            != ProbeOutcome::Ready
        {
            return None;
        }
        server.endpoint = Some(endpoint.clone());
        Some((server, endpoint))
    }

    /// One authed probe against an adopted server's `/v1/models`, used to
    /// re-verify it before reusing its cached endpoint: with no owned child
    /// process, HTTP is the only liveness signal the daemon has for it.
    /// Runs while the caller holds the `state` lock, but is hard-bounded by
    /// [`PROBE_REQUEST_TIMEOUT`] (one request, no polling loop) — a small,
    /// fixed cost next to the minutes-long startup waits the same lock
    /// already legitimately spans.
    async fn adopted_endpoint_still_ready(&self, port: u16, endpoint: &UnslothEndpoint) -> bool {
        let probe_url = format!("http://127.0.0.1:{port}/v1/models");
        let Ok(client) = reqwest::Client::builder()
            .timeout(PROBE_REQUEST_TIMEOUT)
            .build()
        else {
            return false;
        };
        classify_probe(probe_status(&client, &probe_url, Some(&endpoint.api_key)).await)
            == ProbeOutcome::Ready
    }

    /// Startup sequence after the child is spawned: wait for the HTTP socket,
    /// mint the opencode auth material, then probe with the minted key until
    /// the model is loaded (tolerating a first-use multi-GB download). Every
    /// error path is tagged [`StartupFailure::preserve_server`] so the caller
    /// ([`Self::ensure_endpoint`]) knows whether to leave the managed server
    /// running for a retry to attach to or tear it down.
    async fn wait_and_mint(
        &self,
        binary: &Path,
        repo_id: &str,
        server: &mut ManagedServer,
        status: &StatusCallback,
    ) -> std::result::Result<UnslothEndpoint, StartupFailure> {
        let probe_url = format!("http://127.0.0.1:{}/v1/models", server.port);
        let client = reqwest::Client::builder()
            .timeout(PROBE_REQUEST_TIMEOUT)
            .build()
            .map_err(|e| {
                StartupFailure::terminal(Error::Internal(format!(
                    "failed to build http client: {e}"
                )))
            })?;

        // Phase 1: socket up (any HTTP status). No model download happens on
        // this path — opening the HTTP listener is a fast, fixed-cost step
        // of the server's own startup, not something a first-use download
        // can legitimately stretch out. A timeout here means the process is
        // wedged (terminal): unlike phases 2/3, this is NOT preserved for a
        // retry to attach to, since a child that never opens its port will
        // never satisfy a later retry's wait either — preserving it would
        // leave a permanently-stuck process with no respawn path.
        self.wait_until(self.config.server_up_timeout, server, || async {
            let outcome = classify_probe(probe_status(&client, &probe_url, None).await);
            outcome != ProbeOutcome::Down
        })
        .await
        .map_err(|e| match e {
            WaitError::ChildExited => StartupFailure::terminal(Error::Internal(format!(
                "unsloth server exited during startup (model {repo_id})"
            ))),
            WaitError::TimedOut => StartupFailure::terminal(Error::Internal(format!(
                "unsloth server did not open its HTTP port within {}s",
                self.config.server_up_timeout.as_secs()
            ))),
            WaitError::ShuttingDown => StartupFailure::terminal(shutting_down_error()),
            WaitError::StopRequested => StartupFailure::terminal(stop_requested_error()),
        })?;

        // Phase 2: mint the opencode auth material. `--no-launch` requires a
        // running server (validated: it errors with "No running Unsloth
        // server found" otherwise), which phase 1 guarantees. Dogfooding
        // (monorepo#878) showed this invocation can itself perform/wait on
        // the first-use model download, so it gets the same generous,
        // progress-aware deadline as the phase-3 readiness probe below.
        status(
            StatusLevel::Info,
            format!("Unsloth server up; preparing model {repo_id}…"),
        );
        self.set_phase(Some("minting"));
        let endpoint = self
            .mint_endpoint(binary, repo_id, server, status, self.config.mint_timeout)
            .await?;
        self.set_phase(Some("loading"));

        // Phase 3: model ready — authed probe answers 200. First use can mean
        // a multi-GB download, so this window is generous and progress status
        // is refreshed periodically.
        let mut last_status = tokio::time::Instant::now();
        let result = self
            .wait_until(self.config.model_ready_timeout, server, || {
                let refresh = last_status.elapsed() >= STATUS_UPDATE_INTERVAL;
                if refresh {
                    last_status = tokio::time::Instant::now();
                    status(
                        StatusLevel::Info,
                        format!(
                            "Downloading/loading model {repo_id}… (first use may take several minutes)"
                        ),
                    );
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
            WaitError::ChildExited => StartupFailure::terminal(Error::Internal(format!(
                "unsloth server exited while loading model {repo_id}"
            ))),
            WaitError::TimedOut => StartupFailure::transient(Error::Internal(format!(
                "model {repo_id} did not become ready within {} minutes (download may still be in progress — retry later)",
                self.config.model_ready_timeout.as_secs() / 60
            ))),
            WaitError::ShuttingDown => StartupFailure::terminal(shutting_down_error()),
            WaitError::StopRequested => StartupFailure::terminal(stop_requested_error()),
        })?;
        Ok(endpoint)
    }

    /// Run `unsloth start opencode --no-launch --model <repo>` and read the
    /// generated `~/.unsloth/studio/auth/agents/opencode/opencode.json`.
    /// `UNSLOTH_STUDIO_URL` is set when the managed port differs from the
    /// CLI default so the mint talks to OUR server.
    ///
    /// Dogfooding (monorepo#878) showed this command can itself perform (or
    /// wait behind) the first-use multi-GB model download rather than the
    /// post-mint readiness probe, so — unlike blocking atomically on
    /// `cmd.output()` — this polls the mint child's liveness against
    /// `timeout` ([`UnslothConfig::mint_timeout`] for a daemon-owned
    /// startup, sharing [`MODEL_READY_TIMEOUT`]'s generous default; the
    /// short [`UnslothConfig::adopt_mint_timeout`] for an adoption probe),
    /// refreshing progress status periodically and failing fast only if
    /// the managed SERVER child dies or a shutdown is requested mid-mint.
    async fn mint_endpoint(
        &self,
        binary: &Path,
        repo_id: &str,
        server: &mut ManagedServer,
        status: &StatusCallback,
        timeout: Duration,
    ) -> std::result::Result<UnslothEndpoint, StartupFailure> {
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
        if server.port != DEFAULT_PORT {
            cmd.env(
                "UNSLOTH_STUDIO_URL",
                format!("http://127.0.0.1:{}", server.port),
            );
        }
        let mut mint_child = cmd.spawn().map_err(|e| {
            StartupFailure::terminal(Error::Internal(format!(
                "failed to run unsloth start opencode: {e}"
            )))
        })?;
        // Drain stdout/stderr concurrently with the poll loop below, on
        // background tasks: a mint that emits enough output (e.g. download
        // progress) to fill the OS pipe buffer would otherwise block on
        // write() while nothing reads the other end, making a live process
        // look hung until `mint_timeout` kills it. `cmd.output()` (the
        // pre-#878 code) drained concurrently for free; polling `try_wait()`
        // does not, so the drain has to be made explicit.
        let stdout_pipe = mint_child.stdout.take();
        let stderr_pipe = mint_child.stderr.take();
        let stdout_task = tokio::spawn(async move {
            let mut buf = Vec::new();
            if let Some(mut out) = stdout_pipe {
                let _ = out.read_to_end(&mut buf).await;
            }
            buf
        });
        let stderr_task = tokio::spawn(async move {
            let mut buf = Vec::new();
            if let Some(mut err) = stderr_pipe {
                let _ = err.read_to_end(&mut buf).await;
            }
            buf
        });

        let deadline = tokio::time::Instant::now() + timeout;
        let mut last_status = tokio::time::Instant::now();
        let exit_status = loop {
            if self.is_shutting_down() {
                let _ = mint_child.start_kill();
                let _ = mint_child.wait().await;
                stdout_task.abort();
                stderr_task.abort();
                return Err(StartupFailure::terminal(shutting_down_error()));
            }
            if self.is_stop_requested() {
                let _ = mint_child.start_kill();
                let _ = mint_child.wait().await;
                stdout_task.abort();
                stderr_task.abort();
                return Err(StartupFailure::terminal(stop_requested_error()));
            }
            if !server.is_alive() {
                let _ = mint_child.start_kill();
                let _ = mint_child.wait().await;
                stdout_task.abort();
                stderr_task.abort();
                return Err(StartupFailure::terminal(Error::Internal(format!(
                    "unsloth server exited while minting opencode auth for model {repo_id}"
                ))));
            }
            match mint_child.try_wait() {
                Ok(Some(exit)) => break exit,
                Ok(None) => {}
                Err(e) => {
                    stdout_task.abort();
                    stderr_task.abort();
                    return Err(StartupFailure::terminal(Error::Internal(format!(
                        "failed to poll unsloth start opencode: {e}"
                    ))));
                }
            }
            if tokio::time::Instant::now() >= deadline {
                let _ = mint_child.start_kill();
                let _ = mint_child.wait().await;
                stdout_task.abort();
                stderr_task.abort();
                // The managed server is still alive (checked above) — the
                // mint deadline just elapsed, possibly because it's waiting
                // behind a first-use download. Preserve the server so a
                // retry attaches instead of respawning.
                return Err(StartupFailure::transient(Error::Internal(format!(
                    "unsloth start opencode --no-launch timed out after {}s",
                    timeout.as_secs()
                ))));
            }
            if last_status.elapsed() >= STATUS_UPDATE_INTERVAL {
                last_status = tokio::time::Instant::now();
                status(
                    StatusLevel::Info,
                    format!("Preparing model {repo_id}… (first use may take several minutes)"),
                );
            }
            tokio::time::sleep(self.config.probe_interval).await;
        };
        let stdout = stdout_task.await.unwrap_or_default();
        let stderr = stderr_task.await.unwrap_or_default();
        if !exit_status.success() {
            let stderr = String::from_utf8_lossy(&stderr);
            let stdout = String::from_utf8_lossy(&stdout);
            return Err(StartupFailure::terminal(Error::Internal(format!(
                "unsloth start opencode --no-launch failed ({}): {}",
                exit_status,
                if stderr.trim().is_empty() {
                    stdout.trim()
                } else {
                    stderr.trim()
                }
            ))));
        }
        // InvalidInput (not Internal) for the same reason as
        // `missing_binary_error`: an unresolvable home directory is an
        // environment misconfiguration and the message must survive the
        // JSON-RPC envelope.
        let home = self.config.home_dir.as_deref().ok_or_else(|| {
            StartupFailure::terminal(Error::InvalidInput(
                "cannot resolve home directory (HOME/USERPROFILE unset) — needed to read the unsloth-generated opencode config".to_string(),
            ))
        })?;
        let path = generated_config_path(home);
        let body = tokio::fs::read_to_string(&path).await.map_err(|e| {
            StartupFailure::terminal(Error::Internal(format!(
                "unsloth start opencode succeeded but generated config is unreadable ({}): {e}",
                path.display()
            )))
        })?;
        parse_generated_config(&body, repo_id).map_err(StartupFailure::terminal)
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
            if self.is_stop_requested() {
                return Err(WaitError::StopRequested);
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
            if let Some(child) = server.child.as_mut() {
                kill_server_child(child).await;
            }
            self.set_phase(None);
            self.set_identity(None);
        }
    }
}

/// A failed startup wait ([`UnslothServerManager::wait_and_mint`]), carrying
/// whether the managed server is worth keeping alive for a subsequent
/// `ensure_endpoint` call to attach to (monorepo#878 retry reuse).
struct StartupFailure {
    error: Error,
    /// `true` only for a plain wait-deadline timeout with the managed server
    /// process still alive — never for a dead child, a mint command
    /// failure/parse error, or a shutdown-in-progress. Those cases mean the
    /// server (or the startup attempt) is not worth preserving.
    preserve_server: bool,
}

impl StartupFailure {
    /// A failure worth preserving the server for (deadline timeout, server
    /// still alive): the caller leaves the managed server running.
    fn transient(error: Error) -> Self {
        Self {
            error,
            preserve_server: true,
        }
    }

    /// A failure that means the server/attempt is not worth keeping: the
    /// caller tears it down.
    fn terminal(error: Error) -> Self {
        Self {
            error,
            preserve_server: false,
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
    /// [`UnslothServerManager::stop`] was requested mid-startup — unlike
    /// `ShuttingDown` this is not terminal; a later `ensure_endpoint` call
    /// may start a new server.
    StopRequested,
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

/// Best-effort liveness probe for `unsloth.status`'s staleness check: whether
/// `pid` still refers to a live process, via a unix signal-0 probe (no signal
/// actually delivered — `ESRCH` means the pid is gone). On non-unix this
/// always reports alive: [`ManagedServer::is_alive`]'s `try_wait` remains the
/// authoritative check there, exercised at the next `ensure_endpoint` call.
fn pid_is_alive(pid: u32) -> bool {
    #[cfg(unix)]
    {
        nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid as i32), None).is_ok()
    }
    #[cfg(not(unix))]
    {
        let _ = pid;
        true
    }
}

/// Sum CPU percent + resident memory across `root`'s whole process tree
/// (itself plus every descendant, discovered via the shared `ps`-based
/// [`intent_acp::descendant_pids`] walk) — `unsloth` spawns `llama-server`
/// as a child that holds the model weights, so the root pid alone
/// undercounts. `sysinfo` needs two refreshes
/// [`sysinfo::MINIMUM_CPU_UPDATE_INTERVAL`] apart to compute a CPU delta;
/// that wait is paid here since this only runs on an on-demand status
/// call, never on a hot path. Best-effort throughout: pids that have since
/// exited are silently skipped, never an error.
async fn sample_process_tree(root: u32) -> (f32, u64) {
    #[allow(unused_mut)]
    let mut pids: Vec<u32> = vec![root];
    #[cfg(unix)]
    pids.extend(
        intent_acp::descendant_pids(root)
            .await
            .into_iter()
            .filter_map(|p| u32::try_from(p).ok()),
    );
    let sys_pids: Vec<Pid> = pids.iter().map(|&p| Pid::from(p as usize)).collect();
    let refresh_kind = ProcessRefreshKind::nothing().with_cpu().with_memory();
    let mut sys = System::new();
    sys.refresh_processes_specifics(ProcessesToUpdate::Some(&sys_pids), true, refresh_kind);
    tokio::time::sleep(sysinfo::MINIMUM_CPU_UPDATE_INTERVAL).await;
    sys.refresh_processes_specifics(ProcessesToUpdate::Some(&sys_pids), true, refresh_kind);
    let mut cpu_percent = 0.0;
    let mut memory_bytes = 0u64;
    for pid in &sys_pids {
        if let Some(proc) = sys.process(*pid) {
            cpu_percent += proc.cpu_usage();
            memory_bytes += proc.memory();
        }
    }
    (cpu_percent, memory_bytes)
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
/// convention as the provider-child teardown in `agent_manager`. `unsloth`
/// spawns `llama-server` as a child (the same relationship
/// [`sample_process_tree`] accounts for), and — per
/// `intent_acp::descendant_sweep`'s rationale — a child CAN move into its own
/// process group and survive a plain `killpg`, so descendants are
/// snapshotted before the kill and swept afterwards regardless of process
/// group, exactly like the agent-provider teardown in `agent_manager`.
#[cfg(unix)]
async fn kill_server_child(child: &mut Child) {
    use nix::sys::signal::{killpg, Signal};
    use nix::unistd::Pid;
    if let Some(pid) = child.id() {
        let descendants = intent_acp::descendant_pids(pid).await;
        let pgid = Pid::from_raw(pid as i32);
        let _ = killpg(pgid, Signal::SIGTERM);
        let _ = tokio::time::timeout(Duration::from_secs(2), child.wait()).await;
        let _ = killpg(pgid, Signal::SIGKILL);
        intent_acp::sweep_escaped_descendants(&descendants).await;
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
        // Tag only in the parent directory (bartowski-style layout).
        assert_eq!(
            quant_tag_from_gguf_path("Q8_0/model-00001-of-00002.gguf").as_deref(),
            Some("Q8_0")
        );
        assert_eq!(
            quant_tag_from_gguf_path("UD-Q4_K_XL/model-00001-of-00002.gguf").as_deref(),
            Some("UD-Q4_K_XL")
        );
        // Non-GGUF files and stems without a recognizable trailing tag.
        assert_eq!(quant_tag_from_gguf_path("README.md"), None);
        assert_eq!(quant_tag_from_gguf_path("config.json"), None);
        assert_eq!(quant_tag_from_gguf_path("model-instruct.gguf"), None);
        assert_eq!(quant_tag_from_gguf_path("Qwen-model.gguf"), None);
        // Non-tag directory names don't rescue an untagged filename.
        assert_eq!(
            quant_tag_from_gguf_path("weights/model-00001-of-00002.gguf"),
            None
        );
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
            spawn_stub_http_abortable(key).await.0
        }

        /// [`spawn_stub_http`] variant that also hands back the accept-loop
        /// task handle so a test can abort it (dropping the listener and
        /// closing the port) to simulate the server vanishing.
        async fn spawn_stub_http_abortable(
            key: &'static str,
        ) -> (u16, tokio::task::JoinHandle<()>) {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind");
            let port = listener.local_addr().unwrap().port();
            let handle = tokio::spawn(async move {
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
            (port, handle)
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

        /// Stub HF API that answers every request with a bare status line
        /// and empty body (e.g. `404` for a gated/missing repo).
        async fn spawn_stub_hf_status(status_line: &'static str) -> u16 {
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
                        let _ = sock.read(&mut buf).await;
                        let resp = format!("HTTP/1.1 {status_line}\r\ncontent-length: 0\r\n\r\n");
                        let _ = sock.write_all(resp.as_bytes()).await;
                    });
                }
            });
            port
        }

        /// Stub HF API that accepts connections but never responds —
        /// exercises the `hf_files_timeout` budget.
        async fn spawn_stub_hf_stalled() -> u16 {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind");
            let port = listener.local_addr().unwrap().port();
            tokio::spawn(async move {
                let mut socks = Vec::new();
                loop {
                    let Ok((sock, _)) = listener.accept().await else {
                        break;
                    };
                    socks.push(sock);
                }
            });
            port
        }

        /// Fast test config pointing at the stub binary + fake home + port.
        /// The HF base points at a closed loopback port (instant connection
        /// refusal → CLI-default quant); tests covering the selection path
        /// override it with a [`spawn_stub_hf`] port. `port_is_free` always
        /// reports free because lifecycle tests point `port` at an
        /// already-listening loopback stub standing in for the managed
        /// server itself; port-conflict tests override it.
        fn test_config(binary: PathBuf, home: PathBuf, port: u16) -> UnslothConfig {
            UnslothConfig {
                resolve_binary: Box::new(move |_| Some(binary.clone())),
                home_dir: Some(home),
                port,
                // Generous "should not elapse" budgets: the success paths
                // complete in milliseconds, but under full parallel-suite
                // load a tight budget (e.g. 5s) can spuriously expire
                // (monorepo#972). Tests that exercise a timeout ACTUALLY
                // firing override the relevant field explicitly.
                server_up_timeout: Duration::from_secs(60),
                model_ready_timeout: Duration::from_secs(60),
                probe_interval: Duration::from_millis(50),
                mint_timeout: Duration::from_secs(60),
                adopt_mint_timeout: Duration::from_secs(60),
                hf_api_base: "http://127.0.0.1:1".to_string(),
                hf_files_timeout: Duration::from_secs(2),
                total_memory_bytes: Box::new(|| Some(32 * GIB)),
                port_is_free: Box::new(|_| true),
                pick_free_port: Box::new(pick_free_loopback_port),
            }
        }

        fn stub_log(dir: &Path) -> String {
            std::fs::read_to_string(dir.join("stub.log")).unwrap_or_default()
        }

        #[tokio::test]
        async fn missing_binary_degrades_with_install_hint() {
            let mgr = UnslothServerManager::with_config(UnslothConfig {
                resolve_binary: Box::new(|_| None),
                ..UnslothConfig::default()
            });
            let err = mgr
                .ensure_endpoint(REPO, None, 0, &|_, _| {})
                .await
                .unwrap_err();
            let msg = err.to_string();
            assert!(msg.contains("unsloth CLI not found"), "got: {msg}");
            assert!(msg.contains("docs.unsloth.ai"), "got: {msg}");
            assert!(
                matches!(err, Error::InvalidInput(_)),
                "message must survive the RPC envelope"
            );
        }

        /// Regression (providers.paths retarget): the `providers.paths["unsloth"]`
        /// override handed to `ensure_endpoint` must reach the config's
        /// `resolve_binary` seam — it is the explicit-path tier for the
        /// `unsloth` CLI, not the opencode ACP primary.
        #[tokio::test]
        async fn ensure_endpoint_threads_cli_override_into_resolve_binary() {
            let seen = Arc::new(Mutex::new(None::<Option<String>>));
            let seen2 = seen.clone();
            let mgr = UnslothServerManager::with_config(UnslothConfig {
                resolve_binary: Box::new(move |explicit| {
                    *seen2.lock().unwrap() = Some(explicit.map(str::to_string));
                    None
                }),
                ..UnslothConfig::default()
            });
            let _ = mgr
                .ensure_endpoint(REPO, Some("/custom/unsloth"), 0, &|_, _| {})
                .await
                .unwrap_err();
            assert_eq!(
                seen.lock().unwrap().clone(),
                Some(Some("/custom/unsloth".to_string())),
                "the override must be passed through to resolve_binary"
            );
        }

        /// Regression (providers.paths retarget): the DEFAULT config's
        /// `resolve_binary` applies the override as `find_provider_binary`'s
        /// explicit-path tier, which wins over every other tier when the
        /// path is absolute + executable.
        #[test]
        fn default_resolve_binary_honors_explicit_override() {
            let dir = tempfile::tempdir().expect("tempdir");
            let stub = dir.path().join("unsloth-override");
            std::fs::write(&stub, "#!/bin/sh\n").unwrap();
            std::fs::set_permissions(&stub, std::fs::Permissions::from_mode(0o755)).unwrap();
            let resolved = (UnslothConfig::default().resolve_binary)(Some(&stub.to_string_lossy()));
            assert_eq!(resolved.as_deref(), Some(stub.as_path()));
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
                .ensure_endpoint(REPO, None, 0, &move |_, m| m2.lock().unwrap().push(m))
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
            let ep2 = mgr
                .ensure_endpoint(REPO, None, 0, &|_, _| {})
                .await
                .expect("reuse");
            assert_eq!(ep2, ep);
            assert_eq!(
                stub_log(dir.path()).matches("run --model").count(),
                runs_before,
                "reuse must not respawn the server"
            );

            // Model switch: kill + respawn with the new repo.
            let other = "unsloth/other-model-GGUF";
            let ep3 = mgr
                .ensure_endpoint(other, None, 0, &|_, _| {})
                .await
                .expect("switch");
            assert_eq!(ep3.model_id, other);
            let log = stub_log(dir.path());
            assert!(
                log.contains(&format!("run --model {other}:UD-Q4_K_XL")),
                "respawned with the new model: {log}"
            );
            assert_eq!(log.matches("run --model").count(), runs_before + 1);

            mgr.shutdown().await;
        }

        /// Restart-on-switch with live agents attached: a `Warning`-level
        /// status message names both repos and the attached-session count
        /// BEFORE the old server is killed, and the switch still proceeds.
        #[tokio::test]
        async fn model_switch_with_live_agents_warns_before_restart() {
            let dir = tempfile::tempdir().expect("tempdir");
            let port = spawn_stub_http("sk-unsloth-test-key").await;
            let binary = write_stub_binary(dir.path(), dir.path(), port, None);
            let mgr = UnslothServerManager::with_config(test_config(
                binary,
                dir.path().to_path_buf(),
                port,
            ));

            // Cold start with live agents attached must NOT warn — there is
            // no old server to disrupt.
            let messages: Arc<Mutex<Vec<(StatusLevel, String)>>> = Arc::new(Mutex::new(Vec::new()));
            let m2 = messages.clone();
            mgr.ensure_endpoint(REPO, None, 2, &move |lvl, m| {
                m2.lock().unwrap().push((lvl, m))
            })
            .await
            .expect("cold start");
            assert!(
                messages
                    .lock()
                    .unwrap()
                    .iter()
                    .all(|(lvl, _)| *lvl == StatusLevel::Info),
                "cold start must not emit warnings: {:?}",
                messages.lock().unwrap()
            );

            // Switch with 2 live agents attached: exactly one warning, before
            // the new server's progress messages.
            let other = "unsloth/other-model-GGUF";
            messages.lock().unwrap().clear();
            let m3 = messages.clone();
            let ep = mgr
                .ensure_endpoint(other, None, 2, &move |lvl, m| {
                    m3.lock().unwrap().push((lvl, m))
                })
                .await
                .expect("switch");
            assert_eq!(ep.model_id, other);
            let msgs = messages.lock().unwrap().clone();
            let warnings: Vec<&String> = msgs
                .iter()
                .filter(|(lvl, _)| *lvl == StatusLevel::Warning)
                .map(|(_, m)| m)
                .collect();
            assert_eq!(warnings.len(), 1, "exactly one warning: {msgs:?}");
            let warning = warnings[0];
            assert!(warning.contains(REPO), "names the old repo: {warning}");
            assert!(warning.contains(other), "names the new repo: {warning}");
            assert!(
                warning.contains("2 live agent sessions"),
                "carries the attached-session count: {warning}"
            );
            assert_eq!(
                msgs.first().map(|(lvl, _)| *lvl),
                Some(StatusLevel::Warning),
                "warning must precede the restart's progress messages: {msgs:?}"
            );
            // The restart still happened.
            let log = stub_log(dir.path());
            assert!(
                log.contains(&format!("run --model {other}")),
                "switch respawned the server: {log}"
            );

            mgr.shutdown().await;
        }

        /// Restart-on-switch with NO live agents attached stays silent — the
        /// warning only fires when there are sessions to disrupt.
        #[tokio::test]
        async fn model_switch_without_live_agents_does_not_warn() {
            let dir = tempfile::tempdir().expect("tempdir");
            let port = spawn_stub_http("sk-unsloth-test-key").await;
            let binary = write_stub_binary(dir.path(), dir.path(), port, None);
            let mgr = UnslothServerManager::with_config(test_config(
                binary,
                dir.path().to_path_buf(),
                port,
            ));

            mgr.ensure_endpoint(REPO, None, 0, &|_, _| {})
                .await
                .expect("cold start");

            let messages: Arc<Mutex<Vec<(StatusLevel, String)>>> = Arc::new(Mutex::new(Vec::new()));
            let m2 = messages.clone();
            let other = "unsloth/other-model-GGUF";
            mgr.ensure_endpoint(other, None, 0, &move |lvl, m| {
                m2.lock().unwrap().push((lvl, m))
            })
            .await
            .expect("switch");
            assert!(
                messages
                    .lock()
                    .unwrap()
                    .iter()
                    .all(|(lvl, _)| *lvl == StatusLevel::Info),
                "switch without live agents must not warn: {:?}",
                messages.lock().unwrap()
            );

            mgr.shutdown().await;
        }

        /// A dead child's teardown-and-respawn (same repo) never warns even
        /// with live agents attached — those sessions already lost the
        /// server; there is nothing left to disrupt.
        #[tokio::test]
        async fn dead_child_respawn_does_not_warn_even_with_live_agents() {
            let dir = tempfile::tempdir().expect("tempdir");
            let port = spawn_stub_http("sk-unsloth-test-key").await;
            // `run` exits shortly after startup completes, standing in for a
            // server that crashed while agents were attached.
            let binary = write_stub_binary(dir.path(), dir.path(), port, Some("sleep 0.3"));
            let mgr = UnslothServerManager::with_config(test_config(
                binary,
                dir.path().to_path_buf(),
                port,
            ));

            mgr.ensure_endpoint(REPO, None, 1, &|_, _| {})
                .await
                .expect("cold start");
            // Wait for the stubbed server child to exit.
            tokio::time::sleep(Duration::from_millis(500)).await;

            let messages: Arc<Mutex<Vec<(StatusLevel, String)>>> = Arc::new(Mutex::new(Vec::new()));
            let m2 = messages.clone();
            mgr.ensure_endpoint(REPO, None, 1, &move |lvl, m| {
                m2.lock().unwrap().push((lvl, m))
            })
            .await
            .expect("dead-child respawn");
            assert!(
                messages
                    .lock()
                    .unwrap()
                    .iter()
                    .all(|(lvl, _)| *lvl == StatusLevel::Info),
                "dead-child respawn must not warn: {:?}",
                messages.lock().unwrap()
            );

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

            mgr.ensure_endpoint(REPO, None, 0, &|_, _| {})
                .await
                .expect("starts");
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
        async fn quant_lookup_non_2xx_falls_back_to_cli_default() {
            // Gated/missing repos answer the listing with an HTTP error.
            let hf_port = spawn_stub_hf_status("404 Not Found").await;
            let mut config = test_config(
                PathBuf::from("/nonexistent/unsloth"),
                PathBuf::from("/nonexistent"),
                1,
            );
            config.hf_api_base = format!("http://127.0.0.1:{hf_port}");
            let mgr = UnslothServerManager::with_config(config);
            assert_eq!(mgr.resolve_quant_variant(REPO).await, "UD-Q4_K_XL");
        }

        #[tokio::test]
        async fn quant_lookup_stalled_hf_degrades_within_the_timeout_budget() {
            // HF accepts the connection but never answers: resolution must
            // return the CLI default within hf_files_timeout, not hang.
            let hf_port = spawn_stub_hf_stalled().await;
            let mut config = test_config(
                PathBuf::from("/nonexistent/unsloth"),
                PathBuf::from("/nonexistent"),
                1,
            );
            config.hf_api_base = format!("http://127.0.0.1:{hf_port}");
            config.hf_files_timeout = Duration::from_millis(200);
            let mgr = UnslothServerManager::with_config(config);
            let started = std::time::Instant::now();
            let quant =
                tokio::time::timeout(Duration::from_secs(5), mgr.resolve_quant_variant(REPO))
                    .await
                    .expect("resolution must not hang past the timeout budget");
            assert_eq!(quant, "UD-Q4_K_XL");
            assert!(
                started.elapsed() < Duration::from_secs(2),
                "degraded well within budget, took {:?}",
                started.elapsed()
            );
        }

        #[tokio::test]
        async fn quant_default_fallback_is_not_cached() {
            // A 200 body with no usable size data falls back to the CLI
            // default without caching it — the next spawn retries HF.
            let (hf_port, hits) = spawn_stub_hf("{}").await;
            let mut config = test_config(
                PathBuf::from("/nonexistent/unsloth"),
                PathBuf::from("/nonexistent"),
                1,
            );
            config.hf_api_base = format!("http://127.0.0.1:{hf_port}");
            let mgr = UnslothServerManager::with_config(config);
            assert_eq!(mgr.resolve_quant_variant(REPO).await, "UD-Q4_K_XL");
            assert_eq!(mgr.resolve_quant_variant(REPO).await, "UD-Q4_K_XL");
            assert_eq!(
                hits.load(Ordering::SeqCst),
                2,
                "default fallbacks must not populate the cache"
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
            mgr.ensure_endpoint(REPO, None, 0, &|_, _| {})
                .await
                .expect("starts");
            let pid = {
                let mut state = mgr.state.lock().await;
                let server = state.as_mut().expect("server tracked");
                assert!(server.is_alive(), "child alive before shutdown");
                server.pid().expect("pid")
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
            let err = mgr
                .ensure_endpoint(REPO, None, 0, &|_, _| {})
                .await
                .unwrap_err();
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
            let startup =
                tokio::spawn(async move { m2.ensure_endpoint(REPO, None, 0, &|_, _| {}).await });
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
            let err = mgr
                .ensure_endpoint(REPO, None, 0, &|_, _| {})
                .await
                .unwrap_err();
            assert!(err.to_string().contains("shutting down"), "got: {err}");
        }

        #[tokio::test]
        async fn status_snapshot_stays_responsive_during_in_flight_startup() {
            // Regression: `status_snapshot` must never contend with the
            // startup-serializing `state` lock — it reads the lock-free
            // `identity`/`phase` mirrors instead. `run` sleeps but never
            // opens the HTTP socket, so the startup sits in its phase-1 probe
            // loop for the whole (long) window; `status_snapshot` must return
            // promptly throughout, not block behind `ensure_endpoint`.
            let dir = tempfile::tempdir().expect("tempdir");
            let binary = write_stub_binary(dir.path(), dir.path(), 1, None);
            let mut config = test_config(binary, dir.path().to_path_buf(), 1);
            config.server_up_timeout = Duration::from_secs(600);
            let mgr = Arc::new(UnslothServerManager::with_config(config));

            let m2 = mgr.clone();
            let startup =
                tokio::spawn(async move { m2.ensure_endpoint(REPO, None, 0, &|_, _| {}).await });
            // Give the startup time to spawn the child and enter the probe
            // loop (holding `state` for the remainder of the long timeout).
            tokio::time::sleep(Duration::from_millis(200)).await;

            let start = tokio::time::Instant::now();
            let status = tokio::time::timeout(Duration::from_secs(5), mgr.status_snapshot())
                .await
                .expect("status_snapshot must not block behind the startup's state lock")
                .expect("server tracked as running mid-startup");
            assert!(
                start.elapsed() < Duration::from_secs(1),
                "status_snapshot must return promptly, not wait out the startup timeout"
            );
            assert_eq!(status.repo_id, REPO);
            assert_eq!(status.phase, "starting");
            assert!(status.pid.is_some());

            mgr.shutdown().await;
            let _ = startup.await;
        }

        #[tokio::test]
        async fn stop_aborts_in_flight_startup_within_one_probe_tick() {
            // Regression: `unsloth.stop` must interrupt a long-running
            // startup instead of blocking behind `state`'s lock for the
            // whole startup window (the same shape as `shutdown`'s abort,
            // but non-terminal: a later `ensure_endpoint` may start anew).
            let dir = tempfile::tempdir().expect("tempdir");
            let binary = write_stub_binary(dir.path(), dir.path(), 1, None);
            let mut config = test_config(binary, dir.path().to_path_buf(), 1);
            config.server_up_timeout = Duration::from_secs(600);
            let mgr = Arc::new(UnslothServerManager::with_config(config));

            let m2 = mgr.clone();
            let startup =
                tokio::spawn(async move { m2.ensure_endpoint(REPO, None, 0, &|_, _| {}).await });
            // Wait until the startup has actually registered the starting
            // server (identity mirror set) rather than sleeping a fixed
            // 200ms: under full parallel-suite load the spawned task may not
            // have progressed that far yet, making `stop()` report `false`
            // (monorepo#972). Reads only the lock-free identity mirror — the
            // in-flight startup holds `state`'s lock for its whole window.
            let registered = tokio::time::Instant::now() + Duration::from_secs(30);
            while lock_ignore_poison(&mgr.identity).is_none() {
                assert!(
                    tokio::time::Instant::now() < registered,
                    "startup never registered a starting server"
                );
                tokio::time::sleep(Duration::from_millis(25)).await;
            }

            let start = tokio::time::Instant::now();
            assert!(
                mgr.stop().await,
                "stop reports true when a server was starting"
            );
            assert!(
                start.elapsed() < Duration::from_secs(10),
                "stop must abort the in-flight startup, not wait out its timeout"
            );
            let err = startup.await.expect("join").unwrap_err();
            assert!(err.to_string().contains("unsloth.stop"), "got: {err}");
            assert!(mgr.status_snapshot().await.is_none(), "no server tracked");

            // A later ensure_endpoint is unaffected (stop is not terminal).
            let dir2 = tempfile::tempdir().expect("tempdir");
            let port = spawn_stub_http("sk-unsloth-test-key").await;
            let binary2 = write_stub_binary(dir2.path(), dir2.path(), port, None);
            let mgr2 = UnslothServerManager::with_config(test_config(
                binary2,
                dir2.path().to_path_buf(),
                port,
            ));
            mgr2.ensure_endpoint(REPO, None, 0, &|_, _| {})
                .await
                .expect("stop is not terminal: a fresh manager still starts normally");
            mgr2.shutdown().await;
        }

        #[tokio::test]
        async fn status_snapshot_reports_absent_for_a_dead_untracked_pid() {
            // Regression: `status_snapshot` must not report `running: true`
            // off a stale identity once the child has actually exited AND
            // been reaped — e.g. its pid was recycled into an unrelated
            // process by the time of the snapshot. (A dead-but-not-yet-reaped
            // zombie still answers a signal-0 probe on unix, so this test
            // reaps the child directly — as the true OS parent — to exercise
            // the case `pid_is_alive` is actually meant to catch, rather than
            // relying on a race with the kernel's own reaping.)
            let dir = tempfile::tempdir().expect("tempdir");
            let port = spawn_stub_http("sk-unsloth-test-key").await;
            // `run` exits shortly after startup completes, standing in for a
            // server that crashes post-ready without the daemon noticing yet.
            let binary = write_stub_binary(dir.path(), dir.path(), port, Some("sleep 0.3"));
            let mgr = UnslothServerManager::with_config(test_config(
                binary,
                dir.path().to_path_buf(),
                port,
            ));
            mgr.ensure_endpoint(REPO, None, 0, &|_, _| {})
                .await
                .expect("starts");
            let pid = mgr
                .status_snapshot()
                .await
                .and_then(|s| s.pid)
                .expect("pid while running");

            // Reap the child directly (this test process is its true OS
            // parent), forcing the terminal "exited and reaped" state that a
            // signal-0 probe can actually observe.
            let _ = nix::sys::wait::waitpid(nix::unistd::Pid::from_raw(pid as i32), None);

            assert!(
                mgr.status_snapshot().await.is_none(),
                "status must not report a reaped-dead child as running"
            );
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
            let err = mgr
                .ensure_endpoint(REPO, None, 0, &|_, _| {})
                .await
                .unwrap_err();
            let msg = err.to_string();
            assert!(
                msg.contains("No running Unsloth server found"),
                "got: {msg}"
            );
            assert!(mgr.state.lock().await.is_none(), "state cleared on failure");
        }

        /// Regression: a mint child that writes more than one OS pipe buffer
        /// (~64KiB) of stdout — e.g. verbose download progress output —
        /// before exiting must not appear to hang. `mint_endpoint` polls
        /// `try_wait()` instead of blocking on `cmd.output()`, so if stdout
        /// isn't drained concurrently the child blocks on `write()` once the
        /// pipe fills, `try_wait()` never observes an exit, and a live
        /// process is misreported as timed out.
        #[tokio::test]
        async fn mint_with_large_stdout_output_does_not_hang() {
            let dir = tempfile::tempdir().expect("tempdir");
            let port = spawn_stub_http("sk-unsloth-test-key").await;
            let cfg_dir = dir.path().join(".unsloth/studio/auth/agents/opencode");
            let config = GENERATED_CONFIG_FIXTURE
                .replace("127.0.0.1:8888", &format!("127.0.0.1:{port}"))
                .replace('\'', "");
            let binary_path = dir.path().join("unsloth");
            let log = dir.path().join("stub.log");
            // Write ~500KiB of stdout (several times the typical 64KiB pipe
            // buffer) before writing the generated config, simulating a
            // mint invocation that streams verbose download progress.
            let script = format!(
                "#!/bin/sh\necho \"$@\" >> '{log}'\ncase \"$1\" in\n  run) sleep 300 ;;\n  start) yes 'downloading... progress line to fill the pipe buffer' | head -c 500000 && mkdir -p '{cfg}' && cat > '{cfg}/opencode.json' <<'EOF'\n{config}\nEOF\n  ;;\nesac\n",
                log = log.display(),
                cfg = cfg_dir.display(),
            );
            std::fs::write(&binary_path, script).expect("write stub");
            std::fs::set_permissions(&binary_path, std::fs::Permissions::from_mode(0o755))
                .expect("chmod");
            let config = test_config(binary_path, dir.path().to_path_buf(), port);
            let mgr = UnslothServerManager::with_config(config);

            let endpoint = tokio::time::timeout(
                Duration::from_secs(5),
                mgr.ensure_endpoint(REPO, None, 0, &|_, _| {}),
            )
            .await
            .expect("mint must complete well within mint_timeout, not hang on a full pipe")
            .expect("mint succeeds despite large stdout output");
            assert_eq!(endpoint.base_url, format!("http://127.0.0.1:{port}/v1"));

            mgr.shutdown().await;
        }

        /// A phase-1 (server socket) timeout is terminal, unlike the mint and
        /// model-ready phases: opening the HTTP listener is a fast, fixed
        /// cost of the server's own startup, not something a first-use
        /// download can stretch out. A child that never opens its port will
        /// never satisfy a later retry's wait either, so preserving it would
        /// leave a permanently-stuck process with no respawn path — the
        /// timeout must tear it down so the next call starts clean.
        #[tokio::test]
        async fn server_up_timeout_tears_down_the_child_unlike_mint_and_model_ready_timeouts() {
            let dir = tempfile::tempdir().expect("tempdir");
            // The stub never answers HTTP on any port (no listener spawned),
            // so phase 1 (`wait_until` on `probe_status`) never sees
            // anything but `Down` and must time out.
            let binary = write_stub_binary(dir.path(), dir.path(), 1, Some("sleep 300"));
            let mut config = test_config(binary, dir.path().to_path_buf(), 1);
            config.server_up_timeout = Duration::from_millis(200);
            let mgr = UnslothServerManager::with_config(config);

            let err = mgr
                .ensure_endpoint(REPO, None, 0, &|_, _| {})
                .await
                .unwrap_err();
            assert!(
                err.to_string().contains("did not open its HTTP port"),
                "got: {err}"
            );
            assert!(
                mgr.state.lock().await.is_none(),
                "a phase-1 timeout must tear down the child, not preserve it \
                 (it will never open its port for a later retry to attach to)"
            );
        }

        /// Regression for monorepo#878, bug 1: a mint step that outlives a
        /// short `mint_timeout` (simulating `unsloth start opencode
        /// --no-launch` blocking behind a first-use download) must time out
        /// WITHOUT killing the still-alive managed server — the old code
        /// blocked atomically on `cmd.output()` with a fixed 60s deadline and
        /// had no way to distinguish "still downloading" from "hung".
        #[tokio::test]
        async fn mint_timeout_preserves_a_still_alive_server() {
            let dir = tempfile::tempdir().expect("tempdir");
            let port = spawn_stub_http("sk-unsloth-test-key").await;
            // `start` never returns (simulates a long first-use download);
            // `run` sleeps, keeping the managed server process alive.
            let binary_path = dir.path().join("unsloth");
            let log = dir.path().join("stub.log");
            let script = format!(
                "#!/bin/sh\necho \"$@\" >> '{log}'\ncase \"$1\" in\n  run) sleep 300 ;;\n  start) sleep 300 ;;\nesac\n",
                log = log.display(),
            );
            std::fs::write(&binary_path, script).expect("write stub");
            std::fs::set_permissions(&binary_path, std::fs::Permissions::from_mode(0o755))
                .expect("chmod");
            let mut config = test_config(binary_path, dir.path().to_path_buf(), port);
            config.mint_timeout = Duration::from_millis(200);
            let mgr = UnslothServerManager::with_config(config);

            let pid_before = {
                let err = mgr
                    .ensure_endpoint(REPO, None, 0, &|_, _| {})
                    .await
                    .unwrap_err();
                assert!(err.to_string().contains("timed out"), "got: {err}");
                let mut state = mgr.state.lock().await;
                let server = state.as_mut().expect(
                    "a mint timeout with the server still alive must NOT clear state \
                     (monorepo#878: killing here discards an in-flight download)",
                );
                assert!(server.is_alive(), "managed server left running");
                server.pid().expect("pid")
            };

            // The managed SERVER child from the first attempt (not the mint
            // subprocess, which was killed on timeout) must not have been
            // reaped by a respawn.
            let alive = unsafe { libc_kill_probe(pid_before) };
            assert!(alive, "preserved server child must still be running");

            mgr.shutdown().await;
        }

        /// Regression for monorepo#878, bug 2: a spawn retry for a repo whose
        /// server is already starting/downloading must attach to and wait on
        /// the existing server rather than killing + respawning it (which
        /// discards in-flight download progress, e.g. "0.0 GB already
        /// cached" on the second attempt in the repro log). Simulated here
        /// with a `start` arm that always blocks past `mint_timeout`: the
        /// first `ensure_endpoint` call times out (preserving the server per
        /// the previous test), and the second — the "retry" — must attach to
        /// the SAME server rather than spawning a second one.
        #[tokio::test]
        async fn retry_attaches_to_an_in_flight_startup_instead_of_respawning() {
            let dir = tempfile::tempdir().expect("tempdir");
            let port = spawn_stub_http("sk-unsloth-test-key").await;
            let binary_path = dir.path().join("unsloth");
            let log = dir.path().join("stub.log");
            let script = format!(
                "#!/bin/sh\necho \"$@\" >> '{log}'\ncase \"$1\" in\n  run) sleep 300 ;;\n  start) sleep 300 ;;\nesac\n",
                log = log.display(),
            );
            std::fs::write(&binary_path, script).expect("write stub");
            std::fs::set_permissions(&binary_path, std::fs::Permissions::from_mode(0o755))
                .expect("chmod");
            let mut config = test_config(binary_path, dir.path().to_path_buf(), port);
            // Short enough to keep the test quick, wide enough that the mint
            // child's `echo` logging line has reliably executed before the
            // deadline kills it under full parallel-suite load — the log
            // count assertions below depend on it (monorepo#972).
            config.mint_timeout = Duration::from_secs(2);
            let mgr = UnslothServerManager::with_config(config);

            // Attempt 1: mint times out; server preserved (bug 1's fix).
            let err1 = mgr
                .ensure_endpoint(REPO, None, 0, &|_, _| {})
                .await
                .unwrap_err();
            assert!(err1.to_string().contains("timed out"), "got: {err1}");
            let pid1 = mgr
                .state
                .lock()
                .await
                .as_mut()
                .expect("server preserved after attempt 1")
                .pid()
                .expect("pid");

            // Attempt 2 ("the retry"): must attach to the same server, not
            // kill + respawn it.
            let err2 = mgr
                .ensure_endpoint(REPO, None, 0, &|_, _| {})
                .await
                .unwrap_err();
            assert!(err2.to_string().contains("timed out"), "got: {err2}");
            let pid2 = mgr
                .state
                .lock()
                .await
                .as_mut()
                .expect("server still preserved after attempt 2")
                .pid()
                .expect("pid");
            assert_eq!(
                pid1, pid2,
                "the retry must attach to the SAME server process, not respawn"
            );

            let log = stub_log(dir.path());
            assert_eq!(
                log.matches("run --model").count(),
                1,
                "only ONE server spawn across both attempts: {log}"
            );
            assert_eq!(
                log.matches("start opencode").count(),
                2,
                "each attempt re-attempts the mint against the SAME server: {log}"
            );

            mgr.shutdown().await;
        }

        /// Regression for the model-SWITCH repro (dogfooding, Qwen -> LFM):
        /// switching models kills the old server and spawns a new one for the
        /// new repo; if the new server's mint step outlives `mint_timeout`
        /// (e.g. it starts resolving/downloading a different quant variant
        /// mid-load), a spawn retry for the SAME (switched-to) repo must
        /// attach to that new server rather than killing + respawning it —
        /// exactly like the fresh-start case, since the switch's spawn goes
        /// through the identical `ensure_endpoint` -> `wait_and_mint` path.
        #[tokio::test]
        async fn retry_after_model_switch_attaches_to_the_new_server_instead_of_respawning() {
            let dir = tempfile::tempdir().expect("tempdir");
            let port = spawn_stub_http("sk-unsloth-test-key").await;

            // The FIRST model (old) starts and becomes ready normally, under
            // `test_config`'s generous mint budget — the old model's mint
            // must SUCCEED, so it must not race a short deadline under full
            // parallel-suite load (monorepo#972). The short `mint_timeout`
            // that drives the switch/retry timeouts is applied after.
            let binary = write_stub_binary(dir.path(), dir.path(), port, None);
            let config = test_config(binary.clone(), dir.path().to_path_buf(), port);
            let mut mgr = UnslothServerManager::with_config(config);

            let old_repo = "unsloth/qwen-old-model-GGUF";
            mgr.ensure_endpoint(old_repo, None, 0, &|_, _| {})
                .await
                .expect("old model cold-starts fine");
            // Same 2s rationale as the previous test: the timeout must fire
            // (the switch stub's `start` hangs), but only after the mint
            // child's `echo` line has reliably reached the log.
            mgr.config.mint_timeout = Duration::from_secs(2);

            // Rewrite the SAME binary path (resolved fresh on every spawn) to
            // one whose `run` and `start` both hang past `mint_timeout`
            // (simulating the new model's cold load / quant re-resolution
            // outliving the mint deadline), then request the NEW repo — a
            // model switch.
            let log = dir.path().join("stub.log");
            let switch_script = format!(
                "#!/bin/sh\necho \"$@\" >> '{log}'\ncase \"$1\" in\n  run) sleep 300 ;;\n  start) sleep 300 ;;\nesac\n",
                log = log.display(),
            );
            std::fs::write(&binary, switch_script).expect("rewrite stub for switch");
            std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o755))
                .expect("chmod");

            let new_repo = "unsloth/lfm-new-model-GGUF";
            let old_pid = mgr
                .state
                .lock()
                .await
                .as_mut()
                .expect("old server running before switch")
                .pid()
                .expect("pid");

            let err1 = mgr
                .ensure_endpoint(new_repo, None, 0, &|_, _| {})
                .await
                .unwrap_err();
            assert!(err1.to_string().contains("timed out"), "got: {err1}");
            let (new_pid, new_repo_seen) = {
                let mut state = mgr.state.lock().await;
                let server = state
                    .as_mut()
                    .expect("new (switched-to) server preserved after mint timeout");
                (server.pid().expect("pid"), server.repo_id.clone())
            };
            assert_ne!(
                old_pid, new_pid,
                "the switch must have killed the old server"
            );
            assert_eq!(
                new_repo_seen, new_repo,
                "the preserved server serves the NEW repo"
            );
            assert!(
                !unsafe { libc_kill_probe(old_pid) },
                "old server must be dead after the switch"
            );

            // Retry for the SAME (new) repo must attach to that same new
            // server, not kill + respawn a third one.
            let err2 = mgr
                .ensure_endpoint(new_repo, None, 0, &|_, _| {})
                .await
                .unwrap_err();
            assert!(err2.to_string().contains("timed out"), "got: {err2}");
            let retry_pid = mgr
                .state
                .lock()
                .await
                .as_mut()
                .expect("new server still preserved after the retry")
                .pid()
                .expect("pid");
            assert_eq!(
                new_pid, retry_pid,
                "the retry after a model switch must attach to the SAME new server, not respawn"
            );

            let full_log = stub_log(dir.path());
            assert_eq!(
                full_log.matches(&format!("run --model {new_repo}")).count(),
                1,
                "only ONE spawn for the NEW model across the switch + retry: {full_log}"
            );
            assert_eq!(
                full_log.matches("start opencode --no-launch --model unsloth/lfm").count(),
                2,
                "the switch and the retry each re-attempt the mint against the SAME new server: {full_log}"
            );

            mgr.shutdown().await;
        }

        // --- port-conflict detection: adopt a compatible occupant, re-port
        // around a foreign one, or fail with a clear error ---

        #[tokio::test]
        async fn busy_port_with_compatible_unsloth_server_is_adopted() {
            let dir = tempfile::tempdir().expect("tempdir");
            // The occupant: an auth-requiring server already on the
            // configured port (401 unauth, 200 with the minted key) —
            // exactly the managed server's own /v1/models shape.
            let occupant_port = spawn_stub_http("sk-unsloth-test-key").await;
            let binary = write_stub_binary(dir.path(), dir.path(), occupant_port, None);
            let mut config = test_config(binary, dir.path().to_path_buf(), occupant_port);
            config.port_is_free = Box::new(|_| false);
            config.pick_free_port =
                Box::new(|| panic!("a compatible occupant must be adopted, never re-ported"));
            let mgr = UnslothServerManager::with_config(config);

            let ep = mgr
                .ensure_endpoint(REPO, None, 0, &|_, _| {})
                .await
                .expect("adopts the compatible occupant");
            assert_eq!(ep.base_url, format!("http://127.0.0.1:{occupant_port}/v1"));

            let log = stub_log(dir.path());
            assert!(
                !log.contains("run --model"),
                "adoption must not spawn a second server: {log}"
            );

            let status = mgr
                .status_snapshot()
                .await
                .expect("adopted server is tracked");
            assert_eq!(status.port, occupant_port, "status reports the actual port");
            assert!(
                status.pid.is_none(),
                "an adopted server has no owned child pid"
            );
            assert_eq!(status.phase, "ready");

            // shutdown must not try to kill the occupant (no child to kill).
            mgr.shutdown().await;
        }

        #[tokio::test]
        async fn adopted_server_is_reused_across_calls_without_respawning() {
            let dir = tempfile::tempdir().expect("tempdir");
            let occupant_port = spawn_stub_http("sk-unsloth-test-key").await;
            let binary = write_stub_binary(dir.path(), dir.path(), occupant_port, None);
            let mut config = test_config(binary, dir.path().to_path_buf(), occupant_port);
            config.port_is_free = Box::new(|_| false);
            let mgr = UnslothServerManager::with_config(config);

            let ep1 = mgr
                .ensure_endpoint(REPO, None, 0, &|_, _| {})
                .await
                .expect("adopts");
            // The second call re-verifies the still-listening occupant with
            // one authed probe and reuses the cached endpoint — no re-mint,
            // and still no spawn.
            let ep2 = mgr
                .ensure_endpoint(REPO, None, 0, &|_, _| {})
                .await
                .expect("reuses the adopted server");
            assert_eq!(ep1.api_key, ep2.api_key);
            assert_eq!(ep1.base_url, ep2.base_url);

            let log = stub_log(dir.path());
            assert!(!log.contains("run --model"), "never spawned: {log}");
            assert_eq!(
                log.matches("start opencode").count(),
                1,
                "the reuse path must not re-mint: {log}"
            );
            mgr.shutdown().await;
        }

        #[tokio::test]
        async fn busy_port_with_foreign_occupant_re_ports_to_a_free_one() {
            let dir = tempfile::tempdir().expect("tempdir");
            // Where the re-ported managed server will "listen".
            let managed_port = spawn_stub_http("sk-unsloth-test-key").await;
            let binary = write_stub_binary(dir.path(), dir.path(), managed_port, None);
            // Configured port 1: reported busy, but nothing answers HTTP
            // (connect refused → ProbeOutcome::Down → not unsloth-like).
            let mut config = test_config(binary, dir.path().to_path_buf(), 1);
            config.port_is_free = Box::new(|_| false);
            config.pick_free_port = Box::new(move || Some(managed_port));
            let mgr = UnslothServerManager::with_config(config);

            let ep = mgr
                .ensure_endpoint(REPO, None, 0, &|_, _| {})
                .await
                .expect("re-ports around the foreign occupant");
            assert_eq!(ep.base_url, format!("http://127.0.0.1:{managed_port}/v1"));

            let log = stub_log(dir.path());
            assert!(
                log.contains(&format!("-p {managed_port}")),
                "the server must be spawned on the picked port, not the busy configured one: {log}"
            );

            let status = mgr.status_snapshot().await.expect("server tracked");
            assert_eq!(
                status.port, managed_port,
                "status reports the actual (picked) port, not the configured one"
            );
            assert!(status.pid.is_some(), "re-ported server is daemon-owned");
            mgr.shutdown().await;
        }

        #[tokio::test]
        async fn busy_port_serving_a_different_model_re_ports_instead_of_adopting() {
            let dir = tempfile::tempdir().expect("tempdir");
            // The occupant answers 401 unauthenticated (unsloth-like) but
            // rejects the key the adopt-mint produces — the authed
            // verification probe never sees 200, standing in for an
            // occupant serving a different model.
            let occupant_port = spawn_stub_http("sk-occupant-of-another-model").await;
            let managed_port = spawn_stub_http("sk-unsloth-test-key").await;
            let binary = write_stub_binary(dir.path(), dir.path(), managed_port, None);
            let mut config = test_config(binary, dir.path().to_path_buf(), occupant_port);
            config.port_is_free = Box::new(|_| false);
            config.pick_free_port = Box::new(move || Some(managed_port));
            let mgr = UnslothServerManager::with_config(config);

            let ep = mgr
                .ensure_endpoint(REPO, None, 0, &|_, _| {})
                .await
                .expect("declines adoption and starts its own server on a free port");
            assert_eq!(ep.base_url, format!("http://127.0.0.1:{managed_port}/v1"));

            let log = stub_log(dir.path());
            assert!(
                log.contains(&format!("-p {managed_port}")),
                "spawned on the picked port: {log}"
            );
            let status = mgr.status_snapshot().await.expect("server tracked");
            assert_eq!(status.port, managed_port);
            mgr.shutdown().await;
        }

        #[tokio::test]
        async fn busy_port_answering_200_unauthenticated_is_not_adopted() {
            let dir = tempfile::tempdir().expect("tempdir");
            // An occupant answering 200 WITHOUT auth is not the managed
            // server's shape (it requires auth even on /v1/models) — treat
            // as foreign and re-port.
            let (open_port, _hits) = spawn_stub_hf("{}").await;
            let managed_port = spawn_stub_http("sk-unsloth-test-key").await;
            let binary = write_stub_binary(dir.path(), dir.path(), managed_port, None);
            let mut config = test_config(binary, dir.path().to_path_buf(), open_port);
            config.port_is_free = Box::new(|_| false);
            config.pick_free_port = Box::new(move || Some(managed_port));
            let mgr = UnslothServerManager::with_config(config);

            let ep = mgr
                .ensure_endpoint(REPO, None, 0, &|_, _| {})
                .await
                .expect("re-ports around the auth-less occupant");
            assert_eq!(ep.base_url, format!("http://127.0.0.1:{managed_port}/v1"));
            mgr.shutdown().await;
        }

        #[tokio::test]
        async fn busy_port_with_no_free_port_fails_with_a_clear_error() {
            let dir = tempfile::tempdir().expect("tempdir");
            let binary = write_stub_binary(dir.path(), dir.path(), 1, None);
            let mut config = test_config(binary, dir.path().to_path_buf(), 1);
            config.port_is_free = Box::new(|_| false);
            config.pick_free_port = Box::new(|| None);
            let mgr = UnslothServerManager::with_config(config);

            let err = mgr
                .ensure_endpoint(REPO, None, 0, &|_, _| {})
                .await
                .unwrap_err();
            assert!(
                matches!(err, Error::InvalidInput(_)),
                "an unrecoverable port conflict is an environment problem \
                 (InvalidInput survives the JSON-RPC envelope), got: {err:?}"
            );
            let msg = err.to_string();
            assert!(msg.contains("port 1 is already in use"), "got: {msg}");
            assert!(
                mgr.state.lock().await.is_none(),
                "no server tracked after the conflict error"
            );
            assert!(
                !stub_log(dir.path()).contains("run --model"),
                "must not spawn a child doomed to fail its bind"
            );
        }

        #[tokio::test]
        async fn vanished_adopted_server_falls_through_to_a_fresh_spawn() {
            let dir = tempfile::tempdir().expect("tempdir");
            // The occupant is abortable so the test can make it vanish
            // after adoption; the daemon's own re-spawn then lands on a
            // second stub standing in for its managed server.
            let (occupant_port, occupant) = spawn_stub_http_abortable("sk-unsloth-test-key").await;
            let managed_port = spawn_stub_http("sk-unsloth-test-key").await;
            let binary = write_stub_binary(dir.path(), dir.path(), managed_port, None);
            let mut config = test_config(binary, dir.path().to_path_buf(), occupant_port);
            config.port_is_free = Box::new(|_| false);
            config.pick_free_port = Box::new(move || Some(managed_port));
            let mgr = UnslothServerManager::with_config(config);

            mgr.ensure_endpoint(REPO, None, 0, &|_, _| {})
                .await
                .expect("adopts the occupant");
            assert!(
                !stub_log(dir.path()).contains("run --model"),
                "no spawn while adopted"
            );

            // The occupant vanishes: the next call's authed re-verification
            // fails, the stale adopted entry is dropped (nothing to kill),
            // and the daemon spawns its own server instead of erroring or
            // reusing the dead endpoint forever.
            occupant.abort();
            let _ = occupant.await;

            let ep = mgr
                .ensure_endpoint(REPO, None, 0, &|_, _| {})
                .await
                .expect("re-spawns after the adopted occupant vanished");
            assert_eq!(ep.base_url, format!("http://127.0.0.1:{managed_port}/v1"));
            let log = stub_log(dir.path());
            assert_eq!(
                log.matches("run --model").count(),
                1,
                "exactly one spawn, after the occupant vanished: {log}"
            );
            let status = mgr.status_snapshot().await.expect("server tracked");
            assert!(status.pid.is_some(), "the replacement is daemon-owned");
            assert_eq!(status.port, managed_port);
            mgr.shutdown().await;
        }

        #[tokio::test]
        async fn shutdown_and_stop_never_kill_an_adopted_occupant() {
            let dir = tempfile::tempdir().expect("tempdir");
            let occupant_port = spawn_stub_http("sk-unsloth-test-key").await;
            let binary = write_stub_binary(dir.path(), dir.path(), occupant_port, None);
            let mut config = test_config(binary, dir.path().to_path_buf(), occupant_port);
            config.port_is_free = Box::new(|_| false);
            let mgr = UnslothServerManager::with_config(config);

            let ep = mgr
                .ensure_endpoint(REPO, None, 0, &|_, _| {})
                .await
                .expect("adopts");
            assert!(mgr.stop().await, "stop reports the adopted server");

            // The occupant must still answer HTTP after stop(): the daemon
            // dropped its tracking entry but owns no process to kill.
            let client = reqwest::Client::new();
            let probe_url = format!("http://127.0.0.1:{occupant_port}/v1/models");
            assert_eq!(
                probe_status(&client, &probe_url, Some(&ep.api_key)).await,
                Some(200),
                "occupant must survive unsloth.stop"
            );

            // Re-adopt, then shutdown() — same property.
            let ep = mgr
                .ensure_endpoint(REPO, None, 0, &|_, _| {})
                .await
                .expect("re-adopts");
            mgr.shutdown().await;
            assert_eq!(
                probe_status(&client, &probe_url, Some(&ep.api_key)).await,
                Some(200),
                "occupant must survive daemon shutdown"
            );
        }
    }
}
