//! Best-effort onboarding npm cache preparation. No ACP or authentication work.
//!
//! Admission is bounded by the three reviewed registry providers. Probes and
//! downloads run behind two permits, independently of the RPC acknowledgement.
//! Real launches cancel preparation and wait for its process tree to be reaped.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant, SystemTime};

use intent_acp::spawn::{prepare_npx_package, PreparedProvider, SpawnOptions};
use intent_core::settings_file::SettingsFile;
use intent_providers::{find_provider, ProviderConfig};
use sha2::{Digest, Sha256};
use tokio::sync::{OwnedMutexGuard, Semaphore};

const PROVIDERS: [&str; 3] = ["claude-code", "codex", "pi"];
const BACKOFF: Duration = Duration::from_secs(60);
const DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Default)]
struct Coordination {
    gate: Arc<tokio::sync::Mutex<()>>,
    launches: std::sync::atomic::AtomicUsize,
    cancel: tokio::sync::Notify,
}

static COORDINATION: LazyLock<HashMap<&'static str, Arc<Coordination>>> = LazyLock::new(|| {
    PROVIDERS
        .into_iter()
        .map(|id| (id, Arc::default()))
        .collect()
});

/// Held for the foreground provider process lifetime, including owned cleanup.
/// Increment before waiting so queued preparation cannot overtake a real launch.
pub(crate) struct LaunchGuard {
    coordination: Arc<Coordination>,
}

impl Drop for LaunchGuard {
    fn drop(&mut self) {
        self.coordination
            .launches
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
}

pub(crate) async fn before_launch(provider: &str) -> Option<LaunchGuard> {
    let coordination = COORDINATION.get(provider)?.clone();
    coordination
        .launches
        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let guard = LaunchGuard { coordination };
    guard.coordination.cancel.notify_waiters();
    let gate = guard.coordination.gate.clone().lock_owned().await;
    drop(gate);
    Some(guard)
}

#[derive(Default)]
struct State {
    pending: HashSet<&'static str>,
    outcomes: HashMap<&'static str, Outcome>,
}

#[derive(Clone)]
struct Outcome {
    context: [u8; 32],
    completed: Instant,
    receipt: Option<Receipt>,
}

/// Per-service ownership; clones of Services share this bounded scheduler.
pub(crate) struct Preparation {
    state: Arc<Mutex<State>>,
    slots: Arc<Semaphore>,
    tasks: Arc<crate::delivery_tasks::DeliveryTasks>,
}

impl Default for Preparation {
    fn default() -> Self {
        Self {
            state: Arc::default(),
            slots: Arc::new(Semaphore::new(2)),
            tasks: Arc::default(),
        }
    }
}

struct Pending {
    state: Arc<Mutex<State>>,
    id: &'static str,
}
impl Drop for Pending {
    fn drop(&mut self) {
        self.state.lock().unwrap().pending.remove(self.id);
    }
}

impl Preparation {
    pub(crate) fn enqueue(&self, ids: Vec<String>, settings: SettingsFile) {
        // Non-Unix cleanup cannot yet own the npx.cmd -> node.exe tree.
        // A best-effort no-op preserves ordinary on-demand launch safety.
        if !cfg!(unix) {
            return;
        }
        let selector: Arc<Selector> = Arc::new(select);
        self.enqueue_with(ids, settings, &selector);
    }

    fn enqueue_with(&self, ids: Vec<String>, settings: SettingsFile, select: &Arc<Selector>) {
        let settings = Arc::new(settings);
        for requested in ids {
            let Some(id) = PROVIDERS.into_iter().find(|id| *id == requested) else {
                continue;
            };
            if !self.state.lock().unwrap().pending.insert(id) {
                continue;
            }
            let pending = Pending {
                state: self.state.clone(),
                id,
            };
            let slots = self.slots.clone();
            let settings = settings.clone();
            let select = select.clone();
            let tasks = self.tasks.clone();
            self.tasks.spawn_draining(async move {
                let pending_guard = pending;
                let Ok(_slot) = slots.acquire().await else { return; };
                if tasks.is_closed() { return; }
                let selected = tokio::task::spawn_blocking(move || select(id, &settings)).await;
                let Ok(Some(job)) = selected else { return; };
                let state = pending_guard.state.clone();
                let context = job.context;
                let suppressed = tokio::task::spawn_blocking(move || {
                    let outcome = state.lock().unwrap().outcomes.get(id).cloned();
                    outcome.is_some_and(|outcome| {
                        outcome.context == context && match &outcome.receipt {
                            Some(receipt) => receipt.usable(),
                            None => outcome.completed.elapsed() < BACKOFF,
                        }
                    })
                }).await.unwrap_or(false);
                if suppressed { return; }
                let coordination = COORDINATION[id].clone();
                let cancelled = coordination.cancel.notified();
                tokio::pin!(cancelled);
                cancelled.as_mut().enable();
                if coordination.launches.load(std::sync::atomic::Ordering::SeqCst) != 0 { return; }
                let gate = tokio::select! {
                    gate = coordination.gate.clone().lock_owned() => gate,
                    () = tasks.closed() => return,
                };
                if coordination.launches.load(std::sync::atomic::Ordering::SeqCst) != 0 { return; }
                let context = job.context;
                let cancelled = async { tokio::select! { () = cancelled => {}, () = tasks.closed() => {} } };
                let result = run(job, DOWNLOAD_TIMEOUT, cancelled, gate).await;
                if result.is_none() {
                    tracing::debug!(provider = id, "adapter package preparation failed or cancelled; normal launch remains available");
                }
                pending_guard.state.lock().unwrap().outcomes.insert(id, Outcome {
                    context, completed: Instant::now(), receipt: result,
                });
            });
        }
    }

    pub(crate) async fn shutdown(&self) {
        self.tasks.shutdown().await;
    }
}

fn eligible(provider: &ProviderConfig, settings: &SettingsFile) -> bool {
    PROVIDERS.contains(&provider.id)
        && provider.npx_only_package.is_some()
        && !crate::agent_ops::provider_is_disabled(provider.id, settings.providers.enabled.as_ref())
        && intent_providers::gated_reason(provider).is_none()
        && !(provider.npx_only_honors_path_override
            && intent_providers::resolve_npx_only_override(
                provider,
                settings
                    .providers
                    .paths
                    .get(provider.id)
                    .map(String::as_str),
            )
            .is_some())
}

type Selector = dyn Fn(&'static str, &SettingsFile) -> Option<Job> + Send + Sync;

struct Job {
    prepared: PreparedProvider,
    receipt_path: PathBuf,
    context: [u8; 32],
}

/// This entire selection, including CLI/version probes and filesystem checks,
/// runs on a worker, never on the acknowledgement path. No auth probes.
fn select(id: &'static str, settings: &SettingsFile) -> Option<Job> {
    let provider = find_provider(id)?;
    if !eligible(provider, settings) {
        return None;
    }
    if let Some(cli) = intent_providers::installed_cli::InstalledCli::for_provider(id) {
        cli.resolve().ok()?;
    } else {
        let pi = crate::pi_cli::probe_pi_cli();
        if !pi_eligible(&pi) {
            return None;
        }
    }
    let npx = if id == "codex" {
        intent_providers::find_codex_npx()
    } else {
        intent_providers::find_npx()
    }?;
    let node = intent_providers::find_node()?;
    crate::npx_cli::guard_npx_version(&npx, Some(&node)).ok()?;
    build_job(
        provider,
        &npx,
        settings.agents.acp_node_max_old_space_mb,
        BTreeMap::new(),
    )
}

fn pi_eligible(pi: &crate::pi_cli::PiCliStatus) -> bool {
    pi.resolved_path.is_some() && crate::pi_cli::check_pi_cli_for_spawn(pi).is_ok()
}

fn build_job(
    provider: &ProviderConfig,
    npx: &Path,
    heap: Option<u32>,
    extra_env: BTreeMap<String, String>,
) -> Option<Job> {
    let package = provider.npx_only_package?;
    let (name, version) = package.rsplit_once('@')?;
    // Explicit --package installs the same npx cache entry as a real launch,
    // then executes only this daemon-owned metadata reader. ignore-scripts
    // also prevents dependency lifecycle hooks from starting provider code.
    let script = format!(
        r"
const fs = require('node:fs'), path = require('node:path');
const name = {}, version = {};
for (const bin of (process.env.PATH || '').split(path.delimiter)) {{
  if (path.basename(bin) !== '.bin') continue;
  const modules = path.dirname(bin), manifest = path.join(modules, name, 'package.json');
  try {{
    const pkg = JSON.parse(fs.readFileSync(manifest, 'utf8'));
    if (pkg.name !== name || pkg.version !== version) continue;
    fs.writeFileSync(path.join(__dirname, 'receipt.json'), JSON.stringify({{root:path.dirname(modules), name, version}}));
    process.exit(0);
  }} catch {{}}
}}
process.exit(1);
",
        serde_json::to_string(name).ok()?,
        serde_json::to_string(version).ok()?
    );
    let opts = SpawnOptions {
        provider,
        model: None,
        reasoning_effort: None,
        cwd: None,
        npx_launch_root: None,
        rules_file: None,
        mcp_config_file: None,
        env_mcp_config: None,
        unsloth_endpoint: None,
        quiet: false,
        provider_binary: None,
        extra_env,
        tools_to_remove: vec![],
        npx_fallback_binary: Some(npx),
        npx_fallback_package: Some(package),
        node_max_old_space_mb: heap,
    };
    let prepared = prepare_npx_package(&opts, &script).ok()?;
    let receipt_path = prepared
        .command
        .as_std()
        .get_current_dir()?
        .join("receipt.json");
    // The cache can be selected by inherited npm config, user/global npmrc,
    // HOME, or launch environment. Keep only an opaque digest, never secrets.
    let mut hash = Sha256::new();
    hash.update(package.as_bytes());
    hash.update(npx.as_os_str().as_encoded_bytes());
    let mut env: BTreeMap<_, _> = std::env::vars_os().collect();
    for (key, value) in prepared.command.as_std().get_envs() {
        if let Some(value) = value {
            env.insert(key.to_owned(), value.to_owned());
        } else {
            env.remove(key);
        }
    }
    for (key, value) in &env {
        hash.update(key.as_encoded_bytes());
        hash.update([0]);
        hash.update(value.as_encoded_bytes());
        hash.update([0]);
    }
    let mut configs = vec![npx.parent()?.join("../etc/npmrc")];
    if let Some(home) = env.get(std::ffi::OsStr::new("HOME")) {
        configs.push(PathBuf::from(home).join(".npmrc"));
    }
    for (key, value) in &env {
        if matches!(
            key.to_string_lossy().to_ascii_lowercase().as_str(),
            "npm_config_userconfig" | "npm_config_globalconfig"
        ) {
            configs.push(PathBuf::from(value));
        }
    }
    for config in configs {
        if let Ok(bytes) = std::fs::read(config) {
            hash.update(bytes);
        }
    }
    Some(Job {
        prepared,
        receipt_path,
        context: hash.finalize().into(),
    })
}

#[derive(Clone)]
struct Receipt {
    files: Vec<(PathBuf, u64, Option<SystemTime>)>,
}
impl Receipt {
    fn capture(path: &Path) -> Option<Self> {
        let value: serde_json::Value = serde_json::from_slice(&std::fs::read(path).ok()?).ok()?;
        let root = PathBuf::from(value["root"].as_str()?);
        let name = value["name"].as_str()?;
        let manifest = root.join("node_modules").join(name).join("package.json");
        let package: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&manifest).ok()?).ok()?;
        if package["version"] != value["version"] {
            return None;
        }
        let lock = root.join("node_modules/.package-lock.json");
        let tree: serde_json::Value = serde_json::from_slice(&std::fs::read(&lock).ok()?).ok()?;
        let mut paths = vec![manifest.clone(), lock];
        for relative in tree["packages"].as_object()?.keys() {
            let path = root.join(relative).join("package.json");
            paths.push(path);
        }
        let bins: Vec<(&str, &str)> = match &package["bin"] {
            serde_json::Value::String(bin) => vec![(name.rsplit('/').next()?, bin)],
            serde_json::Value::Object(bins) => bins
                .iter()
                .filter_map(|(name, bin)| bin.as_str().map(|bin| (name.as_str(), bin)))
                .collect(),
            _ => return None,
        };
        for (name, bin) in bins {
            paths.push(manifest.parent()?.join(bin));
            paths.push(root.join("node_modules/.bin").join(name));
        }
        let files = paths
            .into_iter()
            .map(|path| {
                let meta = std::fs::metadata(&path).ok()?;
                Some((path, meta.len(), meta.modified().ok()))
            })
            .collect::<Option<Vec<_>>>()?;
        Some(Self { files })
    }
    fn usable(&self) -> bool {
        self.files.iter().all(|(path, len, modified)| {
            std::fs::metadata(path)
                .is_ok_and(|meta| meta.len() == *len && meta.modified().ok() == *modified)
        })
    }
}

/// Own the process, isolation directory and cache lock through tree cleanup.
/// Dropping a caller cannot let a real launch race a still-running npm process.
struct ChildGroup {
    child: Option<tokio::process::Child>,
    pid: u32,
    held: Option<(PreparedProvider, OwnedMutexGuard<()>)>,
}
impl Drop for ChildGroup {
    fn drop(&mut self) {
        let Some(mut child) = self.child.take() else {
            return;
        };
        let held = self.held.take();
        let pid = self.pid;
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                crate::acp_adapter::reap_child(&mut child, pid).await;
                drop(held);
            });
        } else {
            #[cfg(unix)]
            {
                let _ = nix::sys::signal::killpg(
                    nix::unistd::Pid::from_raw(pid.cast_signed()),
                    nix::sys::signal::Signal::SIGKILL,
                );
            }
            let _ = child.start_kill();
            // Without a runtime, retain the directory rather than remove it
            // beneath a tree whose exit cannot be observed.
            std::mem::forget(held);
        }
    }
}
async fn run(
    job: Job,
    budget: Duration,
    cancelled: impl std::future::Future<Output = ()>,
    gate: OwnedMutexGuard<()>,
) -> Option<Receipt> {
    let Job {
        mut prepared,
        receipt_path,
        ..
    } = job;
    let child = prepared.command.spawn().ok()?;
    let pid = child.id()?;
    let mut group = ChildGroup {
        child: Some(child),
        pid,
        held: Some((prepared, gate)),
    };
    let child = group.child.as_mut().unwrap();
    let success = tokio::select! {
        result = tokio::time::timeout(budget, child.wait()) => result.ok().and_then(Result::ok).is_some_and(|s| s.success()),
        () = cancelled => false,
    };
    crate::acp_adapter::reap_child(child, pid).await;
    group.child.take();
    if !success {
        return None;
    }
    // Hold the neutral directory until the metadata reader has consumed it.
    tokio::task::spawn_blocking(move || {
        let _group = group;
        Receipt::capture(&receipt_path)
    })
    .await
    .ok()
    .flatten()
}

#[cfg(test)]
mod tests;
