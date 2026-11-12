//! Tailcat tunnel sidecar supervisor (`server.tunnel.*`).
//!
//! When `server.tunnel.enabled` is true and the WSS listener is up, the daemon
//! runs a bundled [tailcat](https://github.com/tailscale/tailcat) child
//! process (`tailcat serve --json --key=<key> <wss-port>`) that forwards
//! tunnel traffic to the local WSS port. The private key is persisted under
//! the daemon data dir (`tunnel/tailcat.private.json`), so the `tc...`
//! address — derived from the key — is stable across daemon and sidecar
//! restarts. The supervisor restarts the child with capped exponential
//! backoff when it exits unexpectedly, and kills it on stop (settings toggle
//! or daemon shutdown; `kill_on_drop` covers abort paths).

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use intent_core::{Error, Result};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};

/// Longest wait for the child's `{"listenAddr": ...}` stdout line before the
/// start attempt is failed (tailcat fetches a DERP map over the network on
/// startup, so allow for slow links).
const DEFAULT_ADDRESS_TIMEOUT: Duration = Duration::from_secs(60);

/// Restart backoff bounds for unexpected child exits.
const RESTART_BACKOFF_INITIAL: Duration = Duration::from_millis(500);
const RESTART_BACKOFF_MAX: Duration = Duration::from_secs(30);

/// Resolve the tailcat binary: `INTENTD_TAILCAT_BIN` env override (dev seam)
/// → `libexec/tailcat` next to the daemon binary (release archive layout) →
/// `tailcat` next to the daemon binary → bare `tailcat` (PATH lookup).
pub fn resolve_tailcat_bin() -> PathBuf {
    if let Ok(p) = std::env::var("INTENTD_TAILCAT_BIN") {
        if !p.is_empty() {
            return PathBuf::from(p);
        }
    }
    let name = if cfg!(windows) {
        "tailcat.exe"
    } else {
        "tailcat"
    };
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            let libexec = dir.join("libexec").join(name);
            if libexec.exists() {
                return libexec;
            }
            let sibling = dir.join(name);
            if sibling.exists() {
                return sibling;
            }
        }
    }
    PathBuf::from(name)
}

/// Map a tailcat spawn failure into an actionable error. `NotFound` means the
/// sidecar binary is missing entirely (`resolve_tailcat_bin` already probed
/// every location), which in practice means an intentd installed from a
/// release archive that predates the bundled sidecar — say so and how to fix
/// it instead of surfacing the raw OS error. The exception is an
/// `INTENTD_TAILCAT_BIN` override: it short-circuits the probe, so a missing
/// override path is the override's fault, not a stale install. Other spawn
/// failures keep the underlying error.
fn tailcat_spawn_error(action: &str, bin: &Path, e: &std::io::Error) -> Error {
    let overridden = std::env::var("INTENTD_TAILCAT_BIN").is_ok_and(|p| !p.is_empty());
    tailcat_spawn_error_impl(action, bin, e, overridden)
}

fn tailcat_spawn_error_impl(
    action: &str,
    bin: &Path,
    e: &std::io::Error,
    overridden: bool,
) -> Error {
    if e.kind() == std::io::ErrorKind::NotFound {
        if overridden {
            return Error::Internal(format!(
                "cannot run tailcat {action}: the INTENTD_TAILCAT_BIN override points \
                 at a missing binary ({}); fix or unset INTENTD_TAILCAT_BIN (when it \
                 is set, no other location is probed)",
                bin.display()
            ));
        }
        return Error::Internal(format!(
            "cannot run tailcat {action}: the tailcat sidecar binary was not found \
             ({}; checked libexec/ and the directory next to the intentd binary, \
             then PATH). The installation may be incomplete. Run `intentd update` \
             to update or repair it. If it still reports already up to date, \
             reinstall intentd to refresh the updater. Releases before \
             v0.9.10 did not bundle the tailcat sidecar \
             (https://github.com/tailscale/tailcat)",
            bin.display()
        ));
    }
    Error::Internal(format!(
        "cannot run tailcat {action} ({}): {e}",
        bin.display()
    ))
}

/// How long [`TunnelSupervisor::stop`] waits for a cancelled sidecar child to
/// exit before giving up and leaving local pairing blocked.
const STOP_CONFIRM_TIMEOUT: Duration = Duration::from_secs(10);

/// Wait until process `pid` no longer exists (`true`), or `timeout` passes
/// (`false`). `0` means no child was ever spawned. Off unix the exit cannot
/// be observed from a pid here, so it is never confirmed (`false`): local
/// pairing stays blocked rather than assume a requested kill finished.
async fn wait_for_exit(pid: u32, timeout: Duration) -> bool {
    #[cfg(unix)]
    {
        let Ok(raw) = i32::try_from(pid) else {
            return true;
        };
        if raw == 0 {
            return true;
        }
        let target = nix::unistd::Pid::from_raw(raw);
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if matches!(
                nix::sys::signal::kill(target, None),
                Err(nix::errno::Errno::ESRCH)
            ) {
                return true;
            }
            if tokio::time::Instant::now() >= deadline {
                return false;
            }
            // timing-guard: poll for the killed sidecar's exit, bounded by `timeout`
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    #[cfg(not(unix))]
    {
        let _ = timeout;
        // Awaited so the function stays a real `async fn` on every target.
        std::future::ready(pid == 0).await
    }
}

/// Kill `child` and wait for it; `true` once its exit is confirmed (an
/// already-reaped child counts). A failure is logged and reported so the
/// caller keeps local pairing blocked (§5.58 fails closed).
async fn kill_confirmed(child: &mut Child) -> bool {
    match child.kill().await {
        Ok(()) => true,
        Err(error) => {
            if matches!(child.try_wait(), Ok(Some(_))) {
                return true;
            }
            tracing::error!(%error, "could not stop the tailcat sidecar");
            false
        }
    }
}

/// Supervisor for the tailcat sidecar. One instance lives in the composition
/// root (`DaemonControl`); `start`/`stop` are idempotent and safe to call
/// from concurrent settings updates (single async Mutex over the state).
pub struct TunnelSupervisor {
    bin: PathBuf,
    /// `<data_dir>/tunnel/tailcat.private.json` — persisted named key; same
    /// key ⇒ same `tc...` address across restarts.
    key_path: PathBuf,
    address_timeout: Duration,
    state: tokio::sync::Mutex<Option<Running>>,
    /// Set from the moment a start begins (before the sidecar can forward
    /// anything) until a stop has killed it, including restart backoff.
    active: std::sync::atomic::AtomicBool,
    /// Told about every change of `active`, synchronously (§5.58: local
    /// pairing is off while the tunnel runs).
    active_hook: std::sync::Mutex<Option<ActiveHook>>,
}

/// Observer of [`TunnelSupervisor`] activity.
pub type ActiveHook = std::sync::Arc<dyn Fn(bool) + Send + Sync>;

/// Live sidecar state: the stable address plus handles to stop the
/// supervision task (which owns the child). `up` is cleared by the
/// supervision task while the child is down (crash → restart/backoff loop)
/// so the address accessors stop advertising a route nothing is serving.
struct Running {
    address: String,
    /// Process id of the newest sidecar child (0 before the first spawn),
    /// published as soon as each child is spawned.
    child_pid: std::sync::Arc<std::sync::atomic::AtomicU32>,
    up: std::sync::Arc<std::sync::atomic::AtomicBool>,
    recycle: std::sync::Arc<tokio::sync::Notify>,
    stop_tx: tokio::sync::watch::Sender<bool>,
    /// Resolves to whether every child it owned has a confirmed exit.
    task: tokio::task::JoinHandle<bool>,
}

impl TunnelSupervisor {
    /// `state_dir` is the directory holding the persisted key (created on
    /// first start), conventionally `<data_dir>/tunnel`.
    pub fn new(bin: PathBuf, state_dir: &Path) -> Self {
        Self {
            bin,
            key_path: state_dir.join("tailcat.private.json"),
            address_timeout: DEFAULT_ADDRESS_TIMEOUT,
            state: tokio::sync::Mutex::new(None),
            active: std::sync::atomic::AtomicBool::new(false),
            active_hook: std::sync::Mutex::new(None),
        }
    }

    /// Install the activity observer and tell it the current state.
    pub fn set_active_hook(&self, hook: ActiveHook) {
        let mut slot = self.active_hook.lock().expect("tunnel hook poisoned");
        hook(self.active.load(std::sync::atomic::Ordering::SeqCst));
        *slot = Some(hook);
    }

    /// Whether the sidecar is started (forwarding, starting, or in its
    /// restart backoff). Wider than [`address`](Self::address), which
    /// hides the address while the child is down.
    #[cfg(all(test, unix))]
    fn is_active(&self) -> bool {
        self.active.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn set_active(&self, active: bool) {
        let slot = self.active_hook.lock().expect("tunnel hook poisoned");
        let was = self
            .active
            .swap(active, std::sync::atomic::Ordering::SeqCst);
        if was != active {
            if let Some(hook) = slot.as_ref() {
                hook(active);
            }
        }
    }

    /// Test seam: shorten the address wait so failure paths don't stall tests.
    #[cfg(all(test, unix))]
    fn with_address_timeout(mut self, timeout: Duration) -> Self {
        self.address_timeout = timeout;
        self
    }

    /// Start the sidecar forwarding to `127.0.0.1:<ws_port>`, returning the
    /// stable `tc...` address once tailcat reports it. Idempotent: returns
    /// the current address when already running. Generates and persists the
    /// key on first use.
    pub async fn start(&self, ws_port: u16, derp_url: Option<String>) -> Result<String> {
        let mut state = self.state.lock().await;
        if let Some(running) = state.as_ref() {
            return Ok(running.address.clone());
        }
        self.ensure_key(derp_url.as_deref()).await?;
        // Active before the child exists, so nothing it forwards can reach a
        // loopback-only surface (§5.58) before local pairing is off.
        self.set_active(true);
        let child_pid = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let mut confirmed = true;
        let spawned = spawn_and_read_address(
            &self.bin,
            &self.key_path,
            ws_port,
            derp_url.as_deref(),
            self.address_timeout,
            &child_pid,
            None,
            &mut confirmed,
        )
        .await;
        let (child, address) = match spawned {
            Ok(spawned) => spawned,
            Err(error) => {
                if confirmed {
                    self.set_active(false);
                } else {
                    tracing::error!(
                        "tailcat sidecar exit unconfirmed; local pairing stays blocked"
                    );
                }
                return Err(error);
            }
        };
        let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
        let up = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
        let recycle = std::sync::Arc::new(tokio::sync::Notify::new());
        let task = intent_core::spawn_daemon(supervise(
            self.bin.clone(),
            self.key_path.clone(),
            ws_port,
            derp_url,
            self.address_timeout,
            address.clone(),
            child,
            stop_rx,
            std::sync::Arc::clone(&up),
            std::sync::Arc::clone(&recycle),
            std::sync::Arc::clone(&child_pid),
        ));
        *state = Some(Running {
            address: address.clone(),
            child_pid,
            up,
            recycle,
            stop_tx,
            task,
        });
        Ok(address)
    }

    /// Stop the sidecar (kill the child). Idempotent: no-op when stopped.
    pub async fn stop(&self) {
        let running = self.state.lock().await.take();
        if let Some(mut running) = running {
            let _ = running.stop_tx.send(true);
            // Give the supervision task a bounded window to kill the child
            // gracefully; `kill_on_drop` on the Command backstops the abort.
            match tokio::time::timeout(Duration::from_secs(5), &mut running.task).await {
                Ok(Ok(true)) => {}
                Ok(Ok(false)) => {
                    tracing::error!(
                        "tailcat sidecar exit unconfirmed after stop; local pairing stays blocked"
                    );
                    return;
                }
                Ok(Err(_)) | Err(_) => {
                    // Every wait in the task watches the stop signal and kills
                    // its child with a confirmed wait, so this is a last resort
                    // (a wedged kill). Cancelling drops the child
                    // (`kill_on_drop` only requests termination): wait for the
                    // cancellation, then for the process to be gone, before
                    // reopening local pairing (§5.58 fails closed).
                    running.task.abort();
                    let _ = (&mut running.task).await;
                    let pid = running.child_pid.load(std::sync::atomic::Ordering::SeqCst);
                    if !wait_for_exit(pid, STOP_CONFIRM_TIMEOUT).await {
                        tracing::error!(
                            pid,
                            "tailcat sidecar still running after stop; local pairing stays blocked"
                        );
                        return;
                    }
                }
            }
            self.set_active(false);
        }
    }

    /// Replace the running child with a new one at once. No-op when stopped.
    pub async fn recycle(&self) {
        if let Some(running) = self.state.lock().await.as_ref() {
            running.recycle.notify_one();
        }
    }

    /// Current `tc...` address, or `None` when stopped or while the child is
    /// down in the restart/backoff loop — the surfaces that advertise the
    /// address (pairing/status) must not hand out a route nothing serves.
    pub async fn address(&self) -> Option<String> {
        self.state
            .lock()
            .await
            .as_ref()
            .filter(|r| r.up.load(std::sync::atomic::Ordering::Relaxed))
            .map(|r| r.address.clone())
    }

    /// Current `tc...` address without awaiting: `None` when stopped, down
    /// (restart/backoff), or when the state lock is momentarily held. For
    /// synchronous read paths (`system.status`) that must not block — mirrors
    /// the `try_lock` fallback used for port/fingerprint there,
    /// self-correcting on the next call.
    pub fn address_now(&self) -> Option<String> {
        self.state.try_lock().ok().and_then(|s| {
            s.as_ref()
                .filter(|r| r.up.load(std::sync::atomic::Ordering::Relaxed))
                .map(|r| r.address.clone())
        })
    }

    /// Generate the persisted key on first use (`tailcat genkey`). The key
    /// derives the stable `tc...` address; `--fixed-region` bakes the DERP
    /// rendezvous region in so restarts land in the same place without
    /// re-probing. `derp_url` (when set) must be passed here too so region
    /// discovery uses the configured DERP map, not tailcat's default one.
    async fn ensure_key(&self, derp_url: Option<&str>) -> Result<()> {
        if self.key_path.exists() {
            return Ok(());
        }
        if let Some(dir) = self.key_path.parent() {
            std::fs::create_dir_all(dir).map_err(|e| {
                Error::Internal(format!(
                    "cannot create tunnel state dir {}: {e}",
                    dir.display()
                ))
            })?;
        }
        let output = genkey_command(&self.bin, &self.key_path, derp_url)
            .output()
            .await
            .map_err(|e| tailcat_spawn_error("genkey", &self.bin, &e))?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Error::Internal(format!(
                "tailcat genkey failed ({}): {}",
                output.status,
                stderr.trim()
            )));
        }
        Ok(())
    }
}

/// Build the `tailcat genkey` command: persisted key path, optional custom
/// DERP map URL (so `--fixed-region` discovery probes the configured map,
/// not the default one), fixed region baked into the key.
fn genkey_command(bin: &Path, key_path: &Path, derp_url: Option<&str>) -> Command {
    let mut cmd = Command::new(bin);
    cmd.arg("genkey")
        .arg(format!("--key={}", key_path.display()));
    if let Some(url) = derp_url.filter(|u| !u.is_empty()) {
        cmd.arg(format!("--derpmap-url={url}"));
    }
    cmd.arg("--fixed-region").stdin(Stdio::null());
    cmd
}

/// Build the `tailcat serve` command: JSON address output, the persisted key,
/// forwarding to the WSS port only. `derp_url` (when set) overrides the DERP
/// map URL used to resolve the relay region (self-hosted relay support).
fn serve_command(bin: &Path, key_path: &Path, ws_port: u16, derp_url: Option<&str>) -> Command {
    let mut cmd = Command::new(bin);
    cmd.arg("serve")
        .arg("--json")
        .arg(format!("--key={}", key_path.display()));
    if let Some(url) = derp_url.filter(|u| !u.is_empty()) {
        cmd.arg(format!("--derpmap-url={url}"));
    }
    cmd.arg(ws_port.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    cmd
}

/// Spawn the sidecar and wait (bounded) for the `{"listenAddr": ...}` line on
/// stdout. On success returns the child (stdout drained by the supervisor)
/// and the address.
#[expect(
    clippy::too_many_arguments,
    reason = "the stop signal and exit confirmation ride with the spawn inputs"
)]
async fn spawn_and_read_address(
    bin: &Path,
    key_path: &Path,
    ws_port: u16,
    derp_url: Option<&str>,
    timeout: Duration,
    child_pid: &std::sync::atomic::AtomicU32,
    mut stop_rx: Option<&mut tokio::sync::watch::Receiver<bool>>,
    confirmed: &mut bool,
) -> Result<(Child, String)> {
    let mut child = serve_command(bin, key_path, ws_port, derp_url)
        .spawn()
        .map_err(|e| tailcat_spawn_error("serve", bin, &e))?;
    if let Some(pid) = child.id() {
        child_pid.store(pid, std::sync::atomic::Ordering::SeqCst);
    }
    // Drain stderr into the daemon log so crash-loop causes (bad DERP map,
    // key parse failure, network errors) are diagnosable, and the child never
    // blocks on a full pipe.
    if let Some(stderr) = child.stderr.take() {
        intent_core::spawn_daemon(async move {
            let mut lines = BufReader::new(stderr).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                let line = line.trim();
                if !line.is_empty() {
                    tracing::debug!(target: "tailcat", "{line}");
                }
            }
        });
    }
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| Error::Internal("tailcat stdout not captured".to_string()))?;
    let mut lines = BufReader::new(stdout).lines();
    let read = tokio::time::timeout(timeout, async {
        while let Ok(Some(line)) = lines.next_line().await {
            if let Some(addr) = parse_listen_addr(&line) {
                return Some(addr);
            }
        }
        None
    });
    // A stop while waiting kills this child and waits for it to exit (on
    // every platform), so `stop` never has to abandon a forwarding child.
    let stopped = async {
        match stop_rx.as_deref_mut() {
            Some(rx) => {
                if *rx.borrow_and_update() {
                    return;
                }
                let _ = rx.changed().await;
            }
            None => std::future::pending().await,
        }
    };
    let address = tokio::select! {
        biased;
        () = stopped => {
            *confirmed &= kill_confirmed(&mut child).await;
            return Err(Error::Internal("tailcat stopped before reporting its address".to_string()));
        }
        address = read => address,
    };
    match address {
        Ok(Some(addr)) => {
            // Keep draining stdout in the background so the child never
            // blocks on a full pipe.
            intent_core::spawn_daemon(
                async move { while let Ok(Some(_)) = lines.next_line().await {} },
            );
            Ok((child, addr))
        }
        Ok(None) => {
            *confirmed &= kill_confirmed(&mut child).await;
            Err(Error::Internal(
                "tailcat exited before reporting its address".to_string(),
            ))
        }
        Err(_) => {
            *confirmed &= kill_confirmed(&mut child).await;
            Err(Error::Internal(format!(
                "tailcat did not report its address within {}s",
                timeout.as_secs()
            )))
        }
    }
}

/// Extract `listenAddr` from a tailcat `--json` stdout line.
fn parse_listen_addr(line: &str) -> Option<String> {
    let value: serde_json::Value = serde_json::from_str(line.trim()).ok()?;
    value
        .get("listenAddr")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
}

/// Own the child until stopped: on unexpected exit, restart with capped
/// exponential backoff (reset after a stable run). The persisted key makes
/// every restart report the same address; a restart that reports a different
/// one (key file replaced underneath us) is logged loudly but kept — the
/// supervisor's `address` still serves the original until stop/start.
#[expect(clippy::too_many_arguments)]
async fn supervise(
    bin: PathBuf,
    key_path: PathBuf,
    ws_port: u16,
    derp_url: Option<String>,
    address_timeout: Duration,
    expected_address: String,
    mut child: Child,
    mut stop_rx: tokio::sync::watch::Receiver<bool>,
    up: std::sync::Arc<std::sync::atomic::AtomicBool>,
    recycle: std::sync::Arc<tokio::sync::Notify>,
    child_pid: std::sync::Arc<std::sync::atomic::AtomicU32>,
) -> bool {
    // Whether every child this task killed has a confirmed exit; returned so
    // `stop` reopens local pairing only on proof (§5.58 fails closed).
    let mut confirmed = true;
    let mut backoff = RESTART_BACKOFF_INITIAL;
    loop {
        let mut delay = backoff;
        // Own the live child until it exits, a resume recycles it, or a stop
        // arrives. Stop wins any tie.
        let started = tokio::time::Instant::now();
        tokio::select! {
            biased;
            _ = stop_rx.changed() => {
                return kill_confirmed(&mut child).await && confirmed;
            }
            status = child.wait() => {
                if *stop_rx.borrow() {
                    return confirmed;
                }
                // Stop advertising the address while nothing serves it —
                // pairing/status surfaces omit tcAddress until the restart
                // below succeeds.
                up.store(false, std::sync::atomic::Ordering::Relaxed);
                tracing::warn!(
                    status = ?status.ok(),
                    "tailcat tunnel sidecar exited unexpectedly; restarting"
                );
                // A run that survived well past the backoff cap is "stable":
                // reset the backoff so a later crash restarts promptly.
                if started.elapsed() > RESTART_BACKOFF_MAX * 2 {
                    backoff = RESTART_BACKOFF_INITIAL;
                }
            }
            () = recycle.notified() => {
                up.store(false, std::sync::atomic::Ordering::Relaxed);
                confirmed &= kill_confirmed(&mut child).await;
                tracing::info!("tailcat tunnel sidecar restarting after a host resume");
                backoff = RESTART_BACKOFF_INITIAL;
                delay = Duration::ZERO;
            }
        }

        // Restart with capped exponential backoff until a spawn succeeds or
        // a stop arrives.
        child = loop {
            // A resume during the wait ends it and takes the pending signal,
            // so it does not recycle the child spawned next.
            tokio::select! {
                biased;
                _ = stop_rx.changed() => return confirmed,
                () = recycle.notified() => {
                    backoff = RESTART_BACKOFF_INITIAL;
                    delay = Duration::ZERO;
                }
                () = tokio::time::sleep(delay) => {}
            }
            if *stop_rx.borrow() {
                return confirmed;
            }
            if !delay.is_zero() {
                backoff = (backoff * 2).min(RESTART_BACKOFF_MAX);
            }
            match spawn_and_read_address(
                &bin,
                &key_path,
                ws_port,
                derp_url.as_deref(),
                address_timeout,
                &child_pid,
                Some(&mut stop_rx),
                &mut confirmed,
            )
            .await
            {
                Ok((new_child, addr)) => {
                    if addr == expected_address {
                        tracing::info!(address = %addr, "tailcat tunnel sidecar restarted");
                    } else {
                        tracing::error!(
                            expected = %expected_address,
                            actual = %addr,
                            "tailcat tunnel address changed across restart \
                             (key file replaced?); clients hold a stale address"
                        );
                    }
                    up.store(true, std::sync::atomic::Ordering::Relaxed);
                    break new_child;
                }
                Err(_) if *stop_rx.borrow() => return confirmed,
                Err(e) => {
                    tracing::warn!(error = %e, "tailcat tunnel restart failed; will retry");
                    delay = backoff;
                }
            }
        };
    }
}

/// Restart `tunnel`'s child on every host resume from `resumes`, until the
/// stream closes. A paused VM or a sleeping host thaws the same tailcat
/// process with stale relay and peer state: it stays alive, but no dial
/// reaches it until it restarts.
pub fn recycle_on_resume(
    tunnel: std::sync::Arc<TunnelSupervisor>,
    mut resumes: tokio::sync::broadcast::Receiver<crate::suspend::ResumeEvent>,
) -> tokio::task::JoinHandle<()> {
    use tokio::sync::broadcast::error::RecvError;
    intent_core::spawn_daemon(async move {
        loop {
            match resumes.recv().await {
                Ok(_) | Err(RecvError::Lagged(_)) => tunnel.recycle().await,
                Err(RecvError::Closed) => return,
            }
        }
    })
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    /// Write an executable fake-tailcat shell script into `dir`. The script
    /// handles both `genkey` (creates the key file) and `serve` (prints the
    /// JSON address derived from the key file's content, then sleeps).
    fn write_fake_tailcat(dir: &Path) -> PathBuf {
        let path = dir.join("fake-tailcat.sh");
        let script = r#"#!/bin/sh
key=""
for arg in "$@"; do
  case "$arg" in
    --key=*) key="${arg#--key=}" ;;
  esac
done
case "$1" in
  genkey)
    printf 'key-%s' $$ > "$key"
    ;;
  serve)
    printf '{"listenAddr":"tc-%s"}\n' "$(cat "$key")"
    exec sleep 600
    ;;
esac
"#;
        std::fs::write(&path, script).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        path
    }

    #[tokio::test]
    async fn start_reports_address_and_is_idempotent_and_stable_across_restarts() {
        let dir = tempfile::tempdir().unwrap();
        let bin = write_fake_tailcat(dir.path());
        let sup = TunnelSupervisor::new(bin.clone(), dir.path());

        let addr = sup.start(5181, None).await.unwrap();
        assert!(addr.starts_with("tc-key-"), "unexpected address: {addr}");
        assert_eq!(sup.address().await.as_deref(), Some(addr.as_str()));

        // Idempotent: second start returns the same address, no respawn.
        assert_eq!(sup.start(5181, None).await.unwrap(), addr);

        // Stable across supervisor restarts: the persisted key file survives,
        // so a fresh supervisor reports the same address.
        sup.stop().await;
        assert_eq!(sup.address().await, None);
        let sup2 = TunnelSupervisor::new(bin, dir.path());
        assert_eq!(sup2.start(5181, None).await.unwrap(), addr);
        sup2.stop().await;
    }

    #[tokio::test]
    async fn address_is_absent_while_child_is_down() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fake-tailcat.sh");
        // First serve run prints the address and waits for the exit marker,
        // then dies; later runs never print, so the restart loop keeps
        // failing (short address timeout) and the address must stay absent.
        let script = r#"#!/bin/sh
key=""
for arg in "$@"; do
  case "$arg" in
    --key=*) key="${arg#--key=}" ;;
  esac
done
dir=$(dirname "$key")
case "$1" in
  genkey)
    printf 'k' > "$key"
    ;;
  serve)
    n=$(cat "$dir/runs" 2>/dev/null || echo 0)
    n=$((n+1))
    echo "$n" > "$dir/runs"
    if [ "$n" -eq 1 ]; then
      printf '{"listenAddr":"tcTest"}\n'
      while [ ! -f "$dir/exit-now" ]; do sleep 0.1; done
      exit 1
    fi
    exec sleep 600
    ;;
esac
"#;
        std::fs::write(&path, script).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        let sup =
            TunnelSupervisor::new(path, dir.path()).with_address_timeout(Duration::from_secs(2));
        let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        sup.set_active_hook(std::sync::Arc::new({
            let seen = seen.clone();
            move |active| seen.lock().unwrap().push(active)
        }));

        let addr = sup.start(5181, None).await.unwrap();
        assert_eq!(addr, "tcTest");
        assert_eq!(sup.address().await.as_deref(), Some("tcTest"));
        assert!(sup.is_active());

        // Kill the child; the supervisor must stop advertising the address
        // while the restart/backoff loop has nothing serving it.
        std::fs::write(dir.path().join("exit-now"), b"").unwrap();
        let deadline = tokio::time::Instant::now()
            + intentd_test_support::test_timeout(Duration::from_secs(10));
        loop {
            if sup.address().await.is_none() {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "address still advertised after child exit"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(sup.address_now(), None);
        // §5.58: the restart backoff still counts as running (fail closed),
        // even though no address is advertised.
        assert!(sup.is_active(), "backoff must keep local pairing blocked");
        assert_eq!(*seen.lock().unwrap(), vec![false, true]);
        sup.stop().await;
        assert!(!sup.is_active());
        assert_eq!(*seen.lock().unwrap(), vec![false, true, false]);
    }

    /// §5.58: a stop during a restart still waiting for its child's address
    /// kills that child and waits for it to exit before local pairing
    /// reopens.
    #[tokio::test]
    async fn stop_during_restart_reopens_pairing_only_after_the_child_exits() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fake-tailcat.sh");
        // First serve prints the address, then exits on the marker; the
        // restarted one records its pid and never reports an address.
        let script = r#"#!/bin/sh
key=""
for arg in "$@"; do
  case "$arg" in
    --key=*) key="${arg#--key=}" ;;
  esac
done
dir=$(dirname "$key")
case "$1" in
  genkey)
    printf 'k' > "$key"
    ;;
  serve)
    n=$(cat "$dir/runs" 2>/dev/null || echo 0)
    n=$((n+1))
    echo "$n" > "$dir/runs"
    if [ "$n" -eq 1 ]; then
      printf '{"listenAddr":"tcTest"}\n'
      while [ ! -f "$dir/exit-now" ]; do sleep 0.1; done
      exit 1
    fi
    echo $$ > "$dir/restart-pid"
    exec sleep 600
    ;;
esac
"#;
        std::fs::write(&path, script).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        // The restart would wait far longer than stop's five-second grace.
        let sup =
            TunnelSupervisor::new(path, dir.path()).with_address_timeout(Duration::from_secs(60));
        let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        sup.set_active_hook(std::sync::Arc::new({
            let seen = seen.clone();
            move |active| seen.lock().unwrap().push(active)
        }));
        sup.start(5181, None).await.unwrap();
        std::fs::write(dir.path().join("exit-now"), b"").unwrap();
        let pid_file = dir.path().join("restart-pid");
        let deadline = tokio::time::Instant::now()
            + intentd_test_support::test_timeout(Duration::from_secs(15));
        let pid = loop {
            if let Some(pid) = std::fs::read_to_string(&pid_file)
                .ok()
                .and_then(|s| s.trim().parse::<i32>().ok())
            {
                break pid;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "restart never spawned"
            );
            // timing-guard: poll for the restarted child's pid file, bounded by the deadline
            tokio::time::sleep(Duration::from_millis(50)).await;
        };
        assert!(sup.is_active());

        sup.stop().await;
        assert!(
            matches!(
                nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), None),
                Err(nix::errno::Errno::ESRCH)
            ),
            "the restarted child is gone before pairing reopens"
        );
        assert!(!sup.is_active());
        assert_eq!(*seen.lock().unwrap(), vec![false, true, false]);
    }

    /// A live child is killed with a confirmed exit; an already-reaped one
    /// counts as confirmed too.
    #[tokio::test]
    async fn kill_confirmed_confirms_live_and_reaped_children() {
        let mut live = tokio::process::Command::new("sleep")
            .arg("30")
            .spawn()
            .unwrap();
        let pid = live.id().unwrap();
        assert!(kill_confirmed(&mut live).await);
        assert!(wait_for_exit(pid, Duration::ZERO).await);
        let mut done = tokio::process::Command::new("true").spawn().unwrap();
        done.wait().await.unwrap();
        assert!(kill_confirmed(&mut done).await);
    }

    /// Install a running state whose supervision task reports `confirmed`,
    /// as if it had just been asked to stop.
    async fn supervisor_reporting(
        confirmed: bool,
    ) -> (
        TunnelSupervisor,
        std::sync::Arc<std::sync::Mutex<Vec<bool>>>,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let sup = TunnelSupervisor::new(dir.path().join("unused"), dir.path());
        let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        sup.set_active_hook(std::sync::Arc::new({
            let seen = seen.clone();
            move |active| seen.lock().unwrap().push(active)
        }));
        sup.set_active(true);
        let (stop_tx, _stop_rx) = tokio::sync::watch::channel(false);
        *sup.state.lock().await = Some(Running {
            address: "tcTest".into(),
            child_pid: std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0)),
            up: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true)),
            recycle: std::sync::Arc::new(tokio::sync::Notify::new()),
            stop_tx,
            task: tokio::spawn(async move { confirmed }),
        });
        (sup, seen)
    }

    /// §5.58 fails closed: when the supervision task could not confirm a
    /// child's exit (a failed kill), `stop` leaves local pairing blocked.
    #[tokio::test]
    async fn stop_keeps_pairing_blocked_when_exit_is_unconfirmed() {
        let (sup, seen) = supervisor_reporting(false).await;
        sup.stop().await;
        assert!(
            sup.is_active(),
            "an unconfirmed exit must not reopen pairing"
        );
        assert_eq!(*seen.lock().unwrap(), vec![false, true]);

        let (sup, seen) = supervisor_reporting(true).await;
        sup.stop().await;
        assert!(!sup.is_active());
        assert_eq!(*seen.lock().unwrap(), vec![false, true, false]);
    }

    #[tokio::test]
    async fn wait_for_exit_reports_a_live_process_until_it_exits() {
        let mut child = std::process::Command::new("sleep")
            .arg("30")
            .spawn()
            .unwrap();
        let pid = child.id();
        // fixed-deadline: allow — a short probe of a process that is alive by construction
        assert!(!wait_for_exit(pid, Duration::from_millis(200)).await);
        child.kill().unwrap();
        child.wait().unwrap();
        assert!(
            wait_for_exit(
                pid,
                intentd_test_support::test_timeout(Duration::from_secs(5))
            )
            .await
        );
        assert!(wait_for_exit(0, Duration::ZERO).await);
    }

    /// A paused VM (E2B sandbox) or a sleeping host thaws the same tailcat
    /// process, whose relay and peer state went stale while frozen: it stays
    /// alive but no dial reaches it. A host resume must restart it.
    #[tokio::test]
    async fn host_resume_restarts_the_sidecar_with_the_same_address() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fake-tailcat.sh");
        let script = r#"#!/bin/sh
key=""
for arg in "$@"; do
  case "$arg" in
    --key=*) key="${arg#--key=}" ;;
  esac
done
dir=$(dirname "$key")
case "$1" in
  genkey)
    printf 'k' > "$key"
    ;;
  serve)
    echo $$ >> "$dir/runs"
    printf '{"listenAddr":"tcTest"}\n'
    exec sleep 600
    ;;
esac
"#;
        std::fs::write(&path, script).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        let sup = std::sync::Arc::new(TunnelSupervisor::new(path, dir.path()));
        assert_eq!(sup.start(5181, None).await.unwrap(), "tcTest");

        let tracker = crate::suspend::SuspendTracker::new();
        let recycler = recycle_on_resume(std::sync::Arc::clone(&sup), tracker.subscribe());
        let now = std::time::Instant::now();
        tracker.record_suspend(now, now, Duration::from_secs(60));

        let pids = || {
            std::fs::read_to_string(dir.path().join("runs"))
                .unwrap_or_default()
                .lines()
                .map(str::to_owned)
                .collect::<Vec<_>>()
        };
        let runs = || pids().len();
        let deadline = tokio::time::Instant::now()
            + intentd_test_support::test_timeout(Duration::from_secs(10));
        while runs() < 2 || sup.address().await.is_none() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "tailcat not restarted after host resume (runs = {})",
                runs()
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(runs(), 2);
        assert_eq!(sup.address().await.as_deref(), Some("tcTest"));
        let first = pids()[0].clone();
        let alive = std::process::Command::new("kill")
            .args(["-0", &first])
            .stderr(std::process::Stdio::null())
            .status()
            .unwrap()
            .success();
        assert!(!alive, "the frozen tailcat (pid {first}) is still running");

        recycler.abort();
        sup.stop().await;
    }

    /// Serializes tests that touch the process-global `INTENTD_TAILCAT_BIN`
    /// env var with tests whose error path reads it (`tailcat_spawn_error`).
    static TAILCAT_ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    /// Removes `INTENTD_TAILCAT_BIN` for the guard's lifetime and restores the
    /// prior value on drop (including on panic), so a test whose assertions
    /// depend on the var being unset does not inherit one from the calling
    /// shell. The caller must hold `TAILCAT_ENV_LOCK` for the guard's lifetime.
    struct TailcatEnvUnset {
        prev: Option<std::ffi::OsString>,
    }

    impl TailcatEnvUnset {
        fn new() -> Self {
            let prev = std::env::var_os("INTENTD_TAILCAT_BIN");
            std::env::remove_var("INTENTD_TAILCAT_BIN");
            Self { prev }
        }
    }

    impl Drop for TailcatEnvUnset {
        fn drop(&mut self) {
            match self.prev.take() {
                Some(v) => std::env::set_var("INTENTD_TAILCAT_BIN", v),
                None => std::env::remove_var("INTENTD_TAILCAT_BIN"),
            }
        }
    }

    #[tokio::test]
    async fn stop_is_idempotent_and_start_fails_on_missing_binary() {
        let _env = TAILCAT_ENV_LOCK.lock().await;
        // The asserted guidance is the unset-override branch of
        // `tailcat_spawn_error`; an INTENTD_TAILCAT_BIN inherited from the
        // shell would flip it to the override branch (intent-hq/intent#4348).
        let _unset = TailcatEnvUnset::new();
        let dir = tempfile::tempdir().unwrap();
        let sup = TunnelSupervisor::new(dir.path().join("no-such-tailcat"), dir.path())
            .with_address_timeout(Duration::from_secs(2));
        sup.stop().await; // no-op when never started
        let err = sup.start(5181, None).await.unwrap_err();
        // A missing binary must produce the actionable sidecar-not-found
        // guidance, not the raw ENOENT (regression: pre-v0.9.10 release
        // archives shipped without libexec/tailcat).
        let msg = err.to_string();
        assert!(
            msg.contains("sidecar binary was not found") && msg.contains("intentd update"),
            "error should carry the actionable guidance: {err}"
        );
        assert_eq!(sup.address().await, None);
    }

    #[test]
    fn tailcat_spawn_error_maps_not_found_to_actionable_guidance() {
        let bin = Path::new("/opt/intentd/libexec/tailcat");
        let not_found = std::io::Error::from(std::io::ErrorKind::NotFound);
        let msg = tailcat_spawn_error_impl("genkey", bin, &not_found, false).to_string();
        assert!(msg.contains("cannot run tailcat genkey"), "{msg}");
        assert!(msg.contains("sidecar binary was not found"), "{msg}");
        assert!(msg.contains("/opt/intentd/libexec/tailcat"), "{msg}");
        assert!(msg.contains("intentd update"), "{msg}");
        assert!(msg.contains("installation may be incomplete"), "{msg}");
        assert!(msg.contains("update or repair"), "{msg}");
        assert!(msg.contains("before v0.9.10"), "{msg}");
        assert!(
            msg.contains("reinstall intentd to refresh the updater"),
            "{msg}"
        );
        assert!(
            msg.contains("https://github.com/tailscale/tailcat"),
            "{msg}"
        );
        // The probe skips INTENTD_TAILCAT_BIN when it is unset, so the message
        // must not claim it was checked.
        assert!(!msg.contains("INTENTD_TAILCAT_BIN"), "{msg}");

        // An INTENTD_TAILCAT_BIN override short-circuits the probe: blame the
        // override, not a stale install ("intentd update" would not fix it).
        let msg =
            tailcat_spawn_error_impl("genkey", Path::new("/custom/tailcat"), &not_found, true)
                .to_string();
        assert!(msg.contains("cannot run tailcat genkey"), "{msg}");
        assert!(msg.contains("INTENTD_TAILCAT_BIN override"), "{msg}");
        assert!(msg.contains("/custom/tailcat"), "{msg}");
        assert!(msg.contains("fix or unset INTENTD_TAILCAT_BIN"), "{msg}");
        assert!(!msg.contains("intentd update"), "{msg}");
        assert!(!msg.contains("checked libexec/"), "{msg}");

        // Any other spawn failure keeps the raw OS error, no guidance.
        let denied = std::io::Error::from(std::io::ErrorKind::PermissionDenied);
        let msg = tailcat_spawn_error_impl("serve", bin, &denied, false).to_string();
        assert!(msg.contains("cannot run tailcat serve"), "{msg}");
        assert!(msg.contains(&denied.to_string()), "{msg}");
        assert!(!msg.contains("intentd update"), "{msg}");
    }

    #[tokio::test]
    async fn start_fails_when_child_exits_without_address() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fake-tailcat.sh");
        // genkey succeeds; serve exits without printing an address.
        std::fs::write(
            &path,
            "#!/bin/sh\ncase \"$1\" in genkey) : > \"${2#--key=}\" ;; serve) exit 1 ;; esac\n",
        )
        .unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        let sup =
            TunnelSupervisor::new(path, dir.path()).with_address_timeout(Duration::from_secs(5));
        let err = start_retrying_etxtbsy(&sup).await;
        assert!(
            err.to_string().contains("before reporting"),
            "unexpected error: {err}"
        );
        assert!(
            !sup.is_active(),
            "a failed start leaves the tunnel inactive"
        );
    }

    /// Retry `start` on the Unix ETXTBSY fork/exec race: a concurrent test's
    /// fork can briefly hold the just-written script's write fd open,
    /// failing our exec with "Text file busy".
    async fn start_retrying_etxtbsy(sup: &TunnelSupervisor) -> Error {
        for _ in 0..20 {
            match sup.start(5181, None).await {
                Ok(addr) => panic!("start unexpectedly succeeded: {addr}"),
                Err(e) if e.to_string().contains("Text file busy") => {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(e) => return e,
            }
        }
        panic!("exec kept hitting ETXTBSY");
    }

    #[test]
    fn parse_listen_addr_extracts_address() {
        assert_eq!(
            parse_listen_addr(r#"{"listenAddr":"tcABC"}"#).as_deref(),
            Some("tcABC")
        );
        assert_eq!(parse_listen_addr("not json"), None);
        assert_eq!(parse_listen_addr(r#"{"other":"x"}"#), None);
    }

    #[test]
    fn serve_command_includes_derp_url_only_when_set() {
        let cmd = serve_command(Path::new("tailcat"), Path::new("/k"), 5181, None);
        let args: Vec<_> = cmd
            .as_std()
            .get_args()
            .map(|a| a.to_string_lossy().to_string())
            .collect();
        assert!(args.contains(&"serve".to_string()));
        assert!(args.contains(&"--json".to_string()));
        assert!(args.contains(&"5181".to_string()));
        assert!(!args.iter().any(|a| a.starts_with("--derpmap-url")));

        let cmd = serve_command(
            Path::new("tailcat"),
            Path::new("/k"),
            5181,
            Some("https://derp.example.com/map.json"),
        );
        let args: Vec<_> = cmd
            .as_std()
            .get_args()
            .map(|a| a.to_string_lossy().to_string())
            .collect();
        assert!(args.contains(&"--derpmap-url=https://derp.example.com/map.json".to_string()));
    }

    #[test]
    fn resolve_tailcat_bin_prefers_env_override() {
        // Serialized by cargo's per-test process? No — env is process-global.
        // Use a unique var read + restore to stay hermetic.
        let _env = TAILCAT_ENV_LOCK.blocking_lock();
        let prev = std::env::var("INTENTD_TAILCAT_BIN").ok();
        std::env::set_var("INTENTD_TAILCAT_BIN", "/custom/tailcat");
        assert_eq!(resolve_tailcat_bin(), PathBuf::from("/custom/tailcat"));
        match prev {
            Some(v) => std::env::set_var("INTENTD_TAILCAT_BIN", v),
            None => std::env::remove_var("INTENTD_TAILCAT_BIN"),
        }
    }
}
