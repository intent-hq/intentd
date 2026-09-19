//! Detection of the macOS 27 `fm` CLI — Apple's on-device Foundation Models
//! front end — as a candidate one-shot backend for `agent.completeOnce`.
//!
//! The probe answers one question: can this host run `fm` right now? It is
//! usable when the binary exists, the machine-wide licence has been accepted
//! (`sudo fm license`, once, as root — every `fm` command fails with the
//! [`LICENCE_GATE_MARKER`] on stderr until then), and `fm available` reports
//! the system model ready. Both `fm available` and `fm license --status` must
//! exit 0.
//!
//! Spawning `fm` costs a process, so the verdict is **TTL-cached** and served
//! stale-while-revalidate, mirroring [`crate::model_catalog`]: a fresh entry
//! is returned as-is; a stale entry is returned immediately while one
//! background refresh runs (single-flighted — a second stale read neither
//! starts another probe nor waits); only a cold cache blocks the caller on the
//! probe, and concurrent cold callers share that one probe. Hot read RPCs
//! must use [`FmBackend::cached`], which never spawns.
//!
//! Non-macOS hosts short-circuit to unavailable without spawning anything.
//! The binary path is overridable with the [`FM_BIN_ENV`] environment
//! variable, which also lifts the platform gate so a fake script can drive
//! the probe in tests on any host.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::Serialize;
use tokio::sync::OnceCell;

/// Where macOS 27 ships the CLI.
pub const DEFAULT_FM_BIN: &str = "/usr/bin/fm";
/// Environment override for the `fm` binary path (tests / non-standard
/// installs). Setting it also bypasses the macOS-only gate.
pub const FM_BIN_ENV: &str = "INTENTD_FM_BIN";
/// How long a probe verdict stays fresh before a read triggers a background
/// re-probe. Availability changes rarely (licence acceptance, model
/// download), so a short TTL costs little and picks up `sudo fm license`
/// within minutes.
pub const FM_PROBE_TTL: Duration = Duration::from_secs(5 * 60);
/// Wall-clock cap on each `fm` subcommand the probe spawns.
pub const FM_PROBE_TIMEOUT: Duration = Duration::from_secs(10);
/// Distinctive stderr text `fm` prints before the licence is accepted.
pub const LICENCE_GATE_MARKER: &str = "NOT AGREED TO THE APPLE FOUNDATION MODELS CLI LEGAL NOTICE";

/// Whether the `fm` backend can serve a completion, with a human-readable
/// `reason` when it cannot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FmAvailability {
    pub available: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

impl FmAvailability {
    fn available() -> Self {
        Self {
            available: true,
            reason: None,
        }
    }

    fn unavailable(reason: impl Into<String>) -> Self {
        Self {
            available: false,
            reason: Some(reason.into()),
        }
    }
}

struct CachedVerdict {
    verdict: FmAvailability,
    probed_at: Instant,
}

type InflightCell = Arc<OnceCell<FmAvailability>>;

/// TTL-cached `fm` usability probe. Construct once per daemon
/// ([`FmBackend::from_env`]) and share behind an `Arc`.
pub struct FmBackend {
    /// `None` when the host cannot run `fm` at all (non-macOS, no override):
    /// every read short-circuits without spawning.
    bin: Option<PathBuf>,
    ttl: Duration,
    timeout: Duration,
    cache: Mutex<Option<CachedVerdict>>,
    inflight: Mutex<Option<InflightCell>>,
}

impl std::fmt::Debug for FmBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FmBackend")
            .field("bin", &self.bin)
            .field("ttl", &self.ttl)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}

/// The `fm` binary the probe should spawn, or `None` when the host is not
/// eligible: an explicit `override_bin` ([`FM_BIN_ENV`]) always wins and
/// lifts the platform gate; otherwise only macOS resolves [`DEFAULT_FM_BIN`].
#[must_use]
pub fn resolve_bin(override_bin: Option<&std::ffi::OsStr>, is_macos: bool) -> Option<PathBuf> {
    match override_bin {
        Some(bin) if !bin.is_empty() => Some(PathBuf::from(bin)),
        _ if is_macos => Some(PathBuf::from(DEFAULT_FM_BIN)),
        _ => None,
    }
}

impl FmBackend {
    /// Production constructor: [`FM_BIN_ENV`] override, else `/usr/bin/fm`
    /// on macOS, else an always-unavailable backend.
    #[must_use]
    pub fn from_env() -> Self {
        let override_bin = std::env::var_os(FM_BIN_ENV);
        Self::new(
            resolve_bin(override_bin.as_deref(), cfg!(target_os = "macos")),
            FM_PROBE_TTL,
            FM_PROBE_TIMEOUT,
        )
    }

    /// Backend over an explicit binary (or none — the non-macOS shape), with
    /// the given cache TTL and per-command timeout.
    #[must_use]
    pub fn new(bin: Option<PathBuf>, ttl: Duration, timeout: Duration) -> Self {
        Self {
            bin,
            ttl,
            timeout,
            cache: Mutex::new(None),
            inflight: Mutex::new(None),
        }
    }

    /// The binary this backend probes, when the host is eligible.
    pub fn bin(&self) -> Option<&Path> {
        self.bin.as_deref()
    }

    /// The last recorded verdict (fresh or stale) without spawning anything;
    /// `None` on a cold cache. Non-macOS hosts answer unavailable. This is
    /// the only entry point a hot read RPC may call.
    ///
    /// # Panics
    ///
    /// Panics if the cache mutex is poisoned (a prior panic while holding it).
    pub fn cached(&self) -> Option<FmAvailability> {
        if self.bin.is_none() {
            return Some(Self::host_unsupported());
        }
        self.cache
            .lock()
            .expect("fm cache poisoned")
            .as_ref()
            .map(|c| c.verdict.clone())
    }

    /// Current usability verdict. Fresh cache ⇒ returned as-is. Stale cache ⇒
    /// the stale verdict is returned now and one background re-probe is
    /// started (or skipped when one is already running). Cold cache ⇒ runs
    /// the probe (shared with any concurrent cold caller) and returns it.
    ///
    /// # Panics
    ///
    /// Panics if the cache or in-flight mutex is poisoned (a prior panic
    /// while holding it).
    pub async fn probe(self: &Arc<Self>) -> FmAvailability {
        let Some(bin) = self.bin.clone() else {
            return Self::host_unsupported();
        };
        let stale = {
            let cache = self.cache.lock().expect("fm cache poisoned");
            match cache.as_ref() {
                Some(c) if c.probed_at.elapsed() < self.ttl => return c.verdict.clone(),
                Some(c) => Some(c.verdict.clone()),
                None => None,
            }
        };
        if let Some(stale) = stale {
            if let Some(cell) = self.try_claim_inflight() {
                let this = Arc::clone(self);
                tokio::spawn(async move {
                    this.run_and_record(&bin, &cell).await;
                });
            }
            return stale;
        }
        let cell = self.join_inflight();
        self.run_and_record(&bin, &cell).await
    }

    fn host_unsupported() -> FmAvailability {
        FmAvailability::unavailable("fm backend requires macOS 27 (host is not macOS)")
    }

    /// Recording happens inside the initializer, so exactly one waiter — the
    /// one whose probe actually runs — records the verdict, before the
    /// in-flight slot is released. Followers only clone the shared result: a
    /// late-polled follower can never re-record an old verdict over a newer
    /// probe's, nor extend its freshness.
    async fn run_and_record(&self, bin: &Path, cell: &InflightCell) -> FmAvailability {
        let verdict = cell
            .get_or_init(|| async {
                let verdict = run_probe(bin, self.timeout).await;
                tracing::debug!(
                    target: "fm_backend",
                    available = verdict.available,
                    reason = verdict.reason.as_deref().unwrap_or(""),
                    bin = %bin.display(),
                    "fm probe completed"
                );
                *self.cache.lock().expect("fm cache poisoned") = Some(CachedVerdict {
                    verdict: verdict.clone(),
                    probed_at: Instant::now(),
                });
                verdict
            })
            .await
            .clone();
        self.finish_inflight(cell);
        verdict
    }

    /// Join (or create) the single in-flight probe so concurrent cold
    /// callers share one `fm` spawn.
    fn join_inflight(&self) -> InflightCell {
        self.inflight
            .lock()
            .expect("fm inflight poisoned")
            .get_or_insert_with(InflightCell::default)
            .clone()
    }

    /// Claim the in-flight slot only when empty — the stale-while-revalidate
    /// entry point: `None` means a refresh is already running, do nothing.
    fn try_claim_inflight(&self) -> Option<InflightCell> {
        let mut inflight = self.inflight.lock().expect("fm inflight poisoned");
        if inflight.is_some() {
            return None;
        }
        Some(inflight.insert(InflightCell::default()).clone())
    }

    /// Release the slot once recorded; only removes `cell` itself so a late
    /// finisher never evicts a newer probe.
    fn finish_inflight(&self, cell: &InflightCell) {
        let mut inflight = self.inflight.lock().expect("fm inflight poisoned");
        if inflight.as_ref().is_some_and(|cur| Arc::ptr_eq(cur, cell)) {
            *inflight = None;
        }
    }
}

/// One `fm` subcommand's outcome as the probe sees it.
enum FmRun {
    Exited(std::process::Output),
    NotFound,
    Spawn(std::io::Error),
    TimedOut,
}

/// Spawn `bin args…` with no stdin, bounded by `timeout`; on expiry the
/// whole process group is killed (`kill_on_drop` covers the direct child on
/// non-unix).
async fn run_fm(bin: &Path, args: &[&str], timeout: Duration) -> FmRun {
    let mut cmd = tokio::process::Command::new(bin);
    cmd.args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    #[cfg(unix)]
    cmd.process_group(0);
    let child = match cmd.spawn() {
        Ok(child) => child,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return FmRun::NotFound,
        Err(e) => return FmRun::Spawn(e),
    };
    let pid = child.id();
    match tokio::time::timeout(timeout, child.wait_with_output()).await {
        Ok(Ok(output)) => FmRun::Exited(output),
        Ok(Err(e)) => FmRun::Spawn(e),
        Err(_) => {
            #[cfg(unix)]
            if let Some(pid) = pid {
                use nix::sys::signal::{killpg, Signal};
                use nix::unistd::Pid;
                let _ = killpg(Pid::from_raw(pid.cast_signed()), Signal::SIGKILL);
            }
            #[cfg(not(unix))]
            let _ = pid;
            FmRun::TimedOut
        }
    }
}

/// Run one probe subcommand and map its outcome to a classified
/// unavailability reason, or `None` when it exited 0. Child stderr is only
/// *inspected* for [`LICENCE_GATE_MARKER`] and never echoed: an
/// [`FM_BIN_ENV`] override could print inherited environment, and reasons
/// reach clients and logs. `not_ready` names the non-zero-exit condition
/// for this subcommand (e.g. "system model unavailable").
async fn probe_step(
    bin: &Path,
    args: &[&str],
    not_ready: &str,
    timeout: Duration,
) -> Option<String> {
    let what = format!("fm {}", args.join(" "));
    match run_fm(bin, args, timeout).await {
        FmRun::NotFound => Some(format!("fm CLI not found at {}", bin.display())),
        FmRun::Spawn(e) => Some(format!("failed to run `{what}`: {}", e.kind())),
        FmRun::TimedOut => Some(format!(
            "`{what}` timed out after {}s",
            timeout.as_secs_f64()
        )),
        FmRun::Exited(output) if output.status.success() => None,
        FmRun::Exited(output) => {
            if String::from_utf8_lossy(&output.stderr).contains(LICENCE_GATE_MARKER) {
                return Some(
                    "fm licence not accepted; run `sudo fm license` once on this Mac".to_string(),
                );
            }
            let code = output.status.code().unwrap_or(-1);
            Some(format!("{not_ready} (`{what}` exited with code {code})"))
        }
    }
}

/// The uncached probe: `fm available` (system model ready) then
/// `fm license --status` (licence accepted), both exit 0.
async fn run_probe(bin: &Path, timeout: Duration) -> FmAvailability {
    if let Some(reason) =
        probe_step(bin, &["available"], "fm system model unavailable", timeout).await
    {
        return FmAvailability::unavailable(reason);
    }
    if let Some(reason) = probe_step(
        bin,
        &["license", "--status"],
        "fm licence status not ok",
        timeout,
    )
    .await
    {
        return FmAvailability::unavailable(reason);
    }
    FmAvailability::available()
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    const LONG_TTL: Duration = Duration::from_secs(3600);
    const TIMEOUT: Duration = Duration::from_secs(5);

    /// Fake `fm` inside an RAII temp dir: `body` runs with `$1` = subcommand
    /// (`available` / `license`) and `$LOG` = a file the script may append to.
    /// Keep the guard alive for the test.
    fn fake_fm(tag: &str, body: &str) -> (tempfile::TempDir, PathBuf, PathBuf) {
        use std::os::unix::fs::PermissionsExt;
        let dir = crate::tests::test_tempdir(&format!("intentd-fm-{tag}-"));
        let bin = dir.path().join("fm");
        let log = dir.path().join("calls.log");
        std::fs::write(
            &bin,
            format!("#!/bin/sh\nLOG={}\n{body}\n", shell_quote(&log)),
        )
        .unwrap();
        std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();
        (dir, bin, log)
    }

    fn shell_quote(p: &Path) -> String {
        format!("'{}'", p.display().to_string().replace('\'', "'\\''"))
    }

    fn backend(bin: PathBuf, ttl: Duration) -> Arc<FmBackend> {
        Arc::new(FmBackend::new(Some(bin), ttl, TIMEOUT))
    }

    fn calls(log: &Path) -> Vec<String> {
        std::fs::read_to_string(log)
            .unwrap_or_default()
            .lines()
            .map(str::to_string)
            .collect()
    }

    const LOGGING_OK: &str = "echo \"$*\" >> \"$LOG\"\nexit 0";

    #[test]
    fn resolve_bin_gates_on_macos_unless_overridden() {
        use std::ffi::OsStr;
        assert_eq!(resolve_bin(None, false), None);
        assert_eq!(resolve_bin(Some(OsStr::new("")), false), None);
        assert_eq!(resolve_bin(None, true), Some(PathBuf::from(DEFAULT_FM_BIN)));
        assert_eq!(
            resolve_bin(Some(OsStr::new("/opt/fake/fm")), false),
            Some(PathBuf::from("/opt/fake/fm")),
            "the env override lifts the platform gate"
        );
        assert_eq!(
            resolve_bin(Some(OsStr::new("/opt/fake/fm")), true),
            Some(PathBuf::from("/opt/fake/fm"))
        );
    }

    #[tokio::test]
    async fn non_macos_host_is_unavailable_without_spawning() {
        let backend = Arc::new(FmBackend::new(None, LONG_TTL, TIMEOUT));
        let v = backend.probe().await;
        assert!(!v.available);
        assert!(v.reason.as_deref().unwrap_or("").contains("macOS"), "{v:?}");
        assert_eq!(backend.cached(), Some(v));
        assert!(backend.cache.lock().unwrap().is_none(), "nothing recorded");
    }

    #[tokio::test]
    async fn missing_binary_is_unavailable() {
        let dir = crate::tests::test_tempdir("intentd-fm-missing-");
        let bin = dir.path().join("fm");
        let backend = backend(bin.clone(), LONG_TTL);
        let v = backend.probe().await;
        assert!(!v.available);
        let reason = v.reason.unwrap();
        assert!(reason.contains("not found"), "{reason}");
        assert!(reason.contains(&bin.display().to_string()), "{reason}");
    }

    #[tokio::test]
    async fn licence_not_accepted_is_reported() {
        let (_dir, bin, _log) = fake_fm(
            "licence",
            "echo 'YOU HAVE NOT AGREED TO THE APPLE FOUNDATION MODELS CLI LEGAL NOTICE & TERMS.' >&2\nexit 1",
        );
        let v = backend(bin, LONG_TTL).probe().await;
        assert!(!v.available);
        let reason = v.reason.unwrap();
        assert!(reason.contains("licence not accepted"), "{reason}");
        assert!(reason.contains("sudo fm license"), "{reason}");
    }

    #[tokio::test]
    async fn available_nonzero_is_classified_without_echoing_stderr() {
        let (_dir, bin, log) = fake_fm(
            "avail-fail",
            "echo \"$*\" >> \"$LOG\"\necho 'SECRET_TOKEN=abc123 model not ready' >&2\nexit 1",
        );
        let v = backend(bin, LONG_TTL).probe().await;
        assert!(!v.available);
        let reason = v.reason.unwrap();
        assert_eq!(
            reason,
            "fm system model unavailable (`fm available` exited with code 1)"
        );
        assert!(!reason.contains("SECRET_TOKEN"), "{reason}");
        assert_eq!(calls(&log), vec!["available"], "license is not consulted");
    }

    #[tokio::test]
    async fn license_status_nonzero_is_reported() {
        let (_dir, bin, log) = fake_fm(
            "license-fail",
            "echo \"$*\" >> \"$LOG\"\n[ \"$1\" = license ] && exit 3\nexit 0",
        );
        let v = backend(bin, LONG_TTL).probe().await;
        assert!(!v.available);
        let reason = v.reason.unwrap();
        assert_eq!(
            reason,
            "fm licence status not ok (`fm license --status` exited with code 3)"
        );
        assert_eq!(calls(&log), vec!["available", "license --status"]);
    }

    #[tokio::test]
    async fn timed_out_command_is_reported() {
        let (_dir, bin, _log) = fake_fm("timeout", "sleep 30\nexit 0");
        let backend = Arc::new(FmBackend::new(
            Some(bin),
            LONG_TTL,
            Duration::from_millis(200),
        ));
        let v = backend.probe().await;
        assert!(!v.available);
        assert!(v.reason.unwrap().contains("timed out"));
    }

    #[tokio::test]
    async fn both_commands_ok_is_available_and_cached() {
        let (_dir, bin, log) = fake_fm("ok", LOGGING_OK);
        let backend = backend(bin, LONG_TTL);
        assert_eq!(backend.cached(), None, "cold cache spawns nothing");
        let v = backend.probe().await;
        assert_eq!(v, FmAvailability::available());
        assert_eq!(calls(&log), vec!["available", "license --status"]);

        let again = backend.probe().await;
        assert_eq!(again, v);
        assert_eq!(backend.cached(), Some(v));
        assert_eq!(calls(&log).len(), 2, "fresh cache hit must not re-spawn");
    }

    #[tokio::test]
    async fn stale_cache_serves_last_verdict_and_refreshes_in_background() {
        let (dir, bin, log) = fake_fm(
            "stale",
            "echo \"$*\" >> \"$LOG\"\n[ -e \"$(dirname \"$LOG\")/ready\" ] || exit 1\nexit 0",
        );
        let backend = backend(bin, Duration::ZERO);
        let first = backend.probe().await;
        assert!(!first.available);
        assert_eq!(calls(&log).len(), 1);

        std::fs::write(dir.path().join("ready"), "").unwrap();
        let served = backend.probe().await;
        assert_eq!(served, first, "stale verdict is served immediately");

        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if backend.cached().is_some_and(|v| v.available) {
                break;
            }
            assert!(Instant::now() < deadline, "background refresh never landed");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(
            calls(&log),
            vec!["available", "available", "license --status"]
        );
    }

    #[tokio::test]
    async fn concurrent_cold_callers_share_one_probe() {
        let (_dir, bin, log) = fake_fm("shared", LOGGING_OK);
        let backend = backend(bin, LONG_TTL);
        let (a, b, c) = tokio::join!(backend.probe(), backend.probe(), backend.probe());
        assert!(a.available && b.available && c.available);
        assert_eq!(calls(&log), vec!["available", "license --status"]);
        assert!(backend.inflight.lock().unwrap().is_none(), "slot released");
    }

    /// Drive `f` through exactly one poll; `Some` when it completed.
    async fn poll_once<F: std::future::Future + Unpin>(f: &mut F) -> Option<F::Output> {
        std::future::poll_fn(|cx| {
            std::task::Poll::Ready(match std::pin::Pin::new(&mut *f).poll(cx) {
                std::task::Poll::Ready(v) => Some(v),
                std::task::Poll::Pending => None,
            })
        })
        .await
    }

    async fn wait_until_cached_available(backend: &FmBackend) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while !backend.cached().is_some_and(|v| v.available) {
            assert!(Instant::now() < deadline, "refresh never landed");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    #[tokio::test]
    async fn late_follower_does_not_overwrite_newer_verdict() {
        let (dir, bin, log) = fake_fm(
            "late-follower",
            "echo \"$*\" >> \"$LOG\"\n[ -e \"$(dirname \"$LOG\")/ready\" ] || exit 1\nexit 0",
        );
        let backend = backend(bin, Duration::ZERO);

        let a = backend.probe();
        let follower = backend.probe();
        tokio::pin!(a);
        tokio::pin!(follower);
        assert!(poll_once(&mut a).await.is_none(), "A is in flight");
        assert!(
            poll_once(&mut follower).await.is_none(),
            "follower joined A's cell"
        );

        let first = a.await;
        assert!(!first.available);
        assert_eq!(backend.cached(), Some(first.clone()));
        assert_eq!(calls(&log).len(), 1);
        assert!(
            backend.inflight.lock().unwrap().is_none(),
            "A released the slot"
        );

        std::fs::write(dir.path().join("ready"), "").unwrap();
        let served = backend.probe().await;
        assert_eq!(served, first, "stale A is served while B refreshes");
        wait_until_cached_available(&backend).await;
        let b = backend.cached().unwrap();
        assert_eq!(b, FmAvailability::available());

        let late = follower.await;
        assert_eq!(late, first, "follower still sees the verdict it joined");
        assert_eq!(backend.cached(), Some(b), "B must not be overwritten by A");
        assert_eq!(
            calls(&log),
            vec!["available", "available", "license --status"],
            "the follower spawned nothing"
        );
    }

    #[tokio::test]
    async fn licence_marker_is_detected_but_surrounding_stderr_is_not_echoed() {
        let (_dir, bin, _log) = fake_fm(
            "marker-noise",
            "echo 'HOME=/Users/leaked YOU HAVE NOT AGREED TO THE APPLE FOUNDATION MODELS CLI LEGAL NOTICE trailing' >&2\nexit 1",
        );
        let v = backend(bin, LONG_TTL).probe().await;
        let reason = v.reason.unwrap();
        assert_eq!(
            reason,
            "fm licence not accepted; run `sudo fm license` once on this Mac"
        );
        assert!(!reason.contains("leaked"), "{reason}");
    }

    #[test]
    fn availability_serializes_reason_only_when_present() {
        assert_eq!(
            serde_json::to_value(FmAvailability::available()).unwrap(),
            serde_json::json!({ "available": true })
        );
        assert_eq!(
            serde_json::to_value(FmAvailability::unavailable("x")).unwrap(),
            serde_json::json!({ "available": false, "reason": "x" })
        );
    }
}
