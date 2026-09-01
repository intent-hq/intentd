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
}

/// Live sidecar state: the stable address plus handles to stop the
/// supervision task (which owns the child).
struct Running {
    address: String,
    stop_tx: tokio::sync::watch::Sender<bool>,
    task: tokio::task::JoinHandle<()>,
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
        }
    }

    /// Test seam: shorten the address wait so failure paths don't stall tests.
    #[cfg(test)]
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
        let (child, address) = spawn_and_read_address(
            &self.bin,
            &self.key_path,
            ws_port,
            derp_url.as_deref(),
            self.address_timeout,
        )
        .await?;
        let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
        let task = tokio::spawn(supervise(
            self.bin.clone(),
            self.key_path.clone(),
            ws_port,
            derp_url,
            self.address_timeout,
            address.clone(),
            child,
            stop_rx,
        ));
        *state = Some(Running {
            address: address.clone(),
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
            if tokio::time::timeout(Duration::from_secs(5), &mut running.task)
                .await
                .is_err()
            {
                running.task.abort();
            }
        }
    }

    /// Current `tc...` address, or `None` when stopped.
    pub async fn address(&self) -> Option<String> {
        self.state.lock().await.as_ref().map(|r| r.address.clone())
    }

    /// Current `tc...` address without awaiting: `None` when stopped or when
    /// the state lock is momentarily held. For synchronous read paths
    /// (`system.status`) that must not block — mirrors the `try_lock` fallback
    /// used for port/fingerprint there, self-correcting on the next call.
    pub fn address_now(&self) -> Option<String> {
        self.state
            .try_lock()
            .ok()
            .and_then(|s| s.as_ref().map(|r| r.address.clone()))
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
            .map_err(|e| {
                Error::Internal(format!(
                    "cannot run tailcat genkey ({}): {e}",
                    self.bin.display()
                ))
            })?;
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
async fn spawn_and_read_address(
    bin: &Path,
    key_path: &Path,
    ws_port: u16,
    derp_url: Option<&str>,
    timeout: Duration,
) -> Result<(Child, String)> {
    let mut child = serve_command(bin, key_path, ws_port, derp_url)
        .spawn()
        .map_err(|e| Error::Internal(format!("cannot spawn tailcat ({}): {e}", bin.display())))?;
    // Drain stderr into the daemon log so crash-loop causes (bad DERP map,
    // key parse failure, network errors) are diagnosable, and the child never
    // blocks on a full pipe.
    if let Some(stderr) = child.stderr.take() {
        tokio::spawn(async move {
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
    let address = tokio::time::timeout(timeout, async {
        while let Ok(Some(line)) = lines.next_line().await {
            if let Some(addr) = parse_listen_addr(&line) {
                return Some(addr);
            }
        }
        None
    })
    .await;
    match address {
        Ok(Some(addr)) => {
            // Keep draining stdout in the background so the child never
            // blocks on a full pipe.
            tokio::spawn(async move { while let Ok(Some(_)) = lines.next_line().await {} });
            Ok((child, addr))
        }
        Ok(None) => {
            let _ = child.kill().await;
            Err(Error::Internal(
                "tailcat exited before reporting its address".to_string(),
            ))
        }
        Err(_) => {
            let _ = child.kill().await;
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
#[allow(clippy::too_many_arguments)]
async fn supervise(
    bin: PathBuf,
    key_path: PathBuf,
    ws_port: u16,
    derp_url: Option<String>,
    address_timeout: Duration,
    expected_address: String,
    mut child: Child,
    mut stop_rx: tokio::sync::watch::Receiver<bool>,
) {
    let mut backoff = RESTART_BACKOFF_INITIAL;
    loop {
        // Own the live child until it exits or a stop arrives.
        let started = tokio::time::Instant::now();
        tokio::select! {
            status = child.wait() => {
                if *stop_rx.borrow() {
                    return;
                }
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
            _ = stop_rx.changed() => {
                let _ = child.kill().await;
                return;
            }
        }

        // Restart with capped exponential backoff until a spawn succeeds or
        // a stop arrives.
        child = loop {
            tokio::select! {
                () = tokio::time::sleep(backoff) => {}
                _ = stop_rx.changed() => return,
            }
            backoff = (backoff * 2).min(RESTART_BACKOFF_MAX);
            match spawn_and_read_address(
                &bin,
                &key_path,
                ws_port,
                derp_url.as_deref(),
                address_timeout,
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
                    break new_child;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "tailcat tunnel restart failed; will retry");
                }
            }
        };
    }
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
    sleep 600
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
    async fn stop_is_idempotent_and_start_fails_on_missing_binary() {
        let dir = tempfile::tempdir().unwrap();
        let sup = TunnelSupervisor::new(dir.path().join("no-such-tailcat"), dir.path())
            .with_address_timeout(Duration::from_secs(2));
        sup.stop().await; // no-op when never started
        let err = sup.start(5181, None).await.unwrap_err();
        assert!(
            err.to_string().contains("tailcat"),
            "error should name tailcat: {err}"
        );
        assert_eq!(sup.address().await, None);
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
        let prev = std::env::var("INTENTD_TAILCAT_BIN").ok();
        std::env::set_var("INTENTD_TAILCAT_BIN", "/custom/tailcat");
        assert_eq!(resolve_tailcat_bin(), PathBuf::from("/custom/tailcat"));
        match prev {
            Some(v) => std::env::set_var("INTENTD_TAILCAT_BIN", v),
            None => std::env::remove_var("INTENTD_TAILCAT_BIN"),
        }
    }
}
