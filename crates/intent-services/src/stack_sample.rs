//! In-process daemon stack sampling behind `debug.sampleStacks`
//! (PROTOCOL §5.43, monorepo#1755).
//!
//! Unix-only: the capture rides `pprof`'s `setitimer(ITIMER_PROF)` +
//! `SIGPROF` sampler, so it is **CPU-time** sampling — threads that are
//! blocked/idle accumulate no samples (a busy-looping thread dominates the
//! report; a fully idle daemon can legitimately produce zero samples). The
//! rendered report always carries a header stating the parameters, so the
//! `report` string is never empty even when no samples landed. Non-Unix
//! platforms return [`Error::Unsupported`].

use intent_core::{Error, Result};

/// Sampling-window clamp bounds and default (milliseconds).
pub(crate) const MIN_DURATION_MS: i64 = 100;
pub(crate) const MAX_DURATION_MS: i64 = 10_000;
pub(crate) const DEFAULT_DURATION_MS: i64 = 1_000;

/// Sampling-frequency clamp bounds and default (Hz).
pub(crate) const MIN_FREQUENCY_HZ: i64 = 1;
pub(crate) const MAX_FREQUENCY_HZ: i64 = 250;
pub(crate) const DEFAULT_FREQUENCY_HZ: i64 = 99;

/// Effective sampling window: absent/non-numeric → default, else clamped.
pub(crate) fn effective_duration_ms(duration_ms: Option<i64>) -> i64 {
    duration_ms
        .unwrap_or(DEFAULT_DURATION_MS)
        .clamp(MIN_DURATION_MS, MAX_DURATION_MS)
}

/// Effective sampling frequency: absent/non-numeric → default, else clamped.
pub(crate) fn effective_frequency_hz(frequency_hz: Option<i64>) -> i64 {
    frequency_hz
        .unwrap_or(DEFAULT_FREQUENCY_HZ)
        .clamp(MIN_FREQUENCY_HZ, MAX_FREQUENCY_HZ)
}

/// Capture a stack sample of this daemon process and return the
/// `debug.sampleStacks` result payload (PROTOCOL §5.43).
///
/// One session at a time: the profiler's signal handler is process-global, so
/// a concurrent call is rejected with a typed `Error::Internal` instead of
/// being queued. The capture (guard install → sleep → report build/render)
/// runs on the blocking pool so the async runtime is never stalled.
pub(crate) async fn sample_stacks(
    duration_ms: Option<i64>,
    frequency_hz: Option<i64>,
) -> Result<serde_json::Value> {
    let duration_ms = effective_duration_ms(duration_ms);
    let frequency_hz = effective_frequency_hz(frequency_hz);

    #[cfg(not(unix))]
    {
        let _ = (duration_ms, frequency_hz);
        return Err(Error::Unsupported(
            "debug.sampleStacks is not supported on this platform (Unix-only)".to_string(),
        ));
    }

    #[cfg(unix)]
    {
        use std::sync::atomic::{AtomicBool, Ordering};

        static SAMPLING_IN_PROGRESS: AtomicBool = AtomicBool::new(false);

        if SAMPLING_IN_PROGRESS
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(Error::Internal(
                "a stack sampling session is already in progress".to_string(),
            ));
        }
        /// Panic-safe release of the in-progress flag (dropped inside the
        /// blocking task, so an unwinding capture still releases it).
        struct Release;
        impl Drop for Release {
            fn drop(&mut self) {
                SAMPLING_IN_PROGRESS.store(false, Ordering::Release);
            }
        }

        tokio::task::spawn_blocking(move || {
            let _release = Release;
            capture(duration_ms, frequency_hz)
        })
        .await
        .map_err(|e| Error::Internal(format!("stack sampling task failed: {e}")))?
    }
}

/// Pre-capture snapshot of the process-wide `SIGPROF` disposition and
/// `ITIMER_PROF` timer. `pprof`'s guard drop does not restore the prior
/// state — it sets `SIGPROF` to ignore and clears the timer — so without
/// this snapshot one call would permanently disable any external CPU
/// profiler already attached to the daemon.
#[cfg(unix)]
struct SigprofSnapshot {
    action: libc::sigaction,
    timer: libc::itimerval,
}

#[cfg(unix)]
fn snapshot_sigprof() -> Option<SigprofSnapshot> {
    // SAFETY: null `act` reads the current disposition without changing it;
    // `getitimer` only reads. Both write into locally owned zeroed structs.
    unsafe {
        let mut action: libc::sigaction = std::mem::zeroed();
        let mut timer: libc::itimerval = std::mem::zeroed();
        if libc::sigaction(libc::SIGPROF, std::ptr::null(), &mut action) != 0 {
            return None;
        }
        if libc::getitimer(libc::ITIMER_PROF, &mut timer) != 0 {
            return None;
        }
        Some(SigprofSnapshot { action, timer })
    }
}

/// Reinstate an external profiler captured by [`snapshot_sigprof`]. Only a
/// real prior handler is restored: when none existed, `pprof`'s post-drop
/// state (`SIGPROF` ignored, timer cleared) is kept — restoring `SIG_DFL`
/// would turn any stray late `SIGPROF` into process termination.
#[cfg(unix)]
struct RestoreSigprof(Option<SigprofSnapshot>);

#[cfg(unix)]
impl Drop for RestoreSigprof {
    fn drop(&mut self) {
        let Some(snap) = &self.0 else { return };
        let had_handler =
            snap.action.sa_sigaction != libc::SIG_DFL && snap.action.sa_sigaction != libc::SIG_IGN;
        if !had_handler {
            return;
        }
        let timer_armed = snap.timer.it_value.tv_sec != 0
            || snap.timer.it_value.tv_usec != 0
            || snap.timer.it_interval.tv_sec != 0
            || snap.timer.it_interval.tv_usec != 0;
        // SAFETY: reinstates a disposition/timer that were live on this
        // process moments ago; handler before timer so no SIGPROF is
        // delivered to a not-yet-restored handler.
        unsafe {
            let _ = libc::sigaction(libc::SIGPROF, &snap.action, std::ptr::null_mut());
            if timer_armed {
                let _ = libc::setitimer(libc::ITIMER_PROF, &snap.timer, std::ptr::null_mut());
            }
        }
    }
}

/// Blocking capture: install the profiler guard, sleep the window, then
/// build and render the report. Runs on the blocking pool only.
#[cfg(unix)]
fn capture(duration_ms: i64, frequency_hz: i64) -> Result<serde_json::Value> {
    // Declared before the guard so it drops after it: the guard's drop wipes
    // the SIGPROF state, then this reinstates any external profiler.
    let _restore = RestoreSigprof(snapshot_sigprof());

    // Blocklist per pprof guidance: unwinding through these from the signal
    // handler risks deadlocks on their internal locks.
    let guard = pprof::ProfilerGuardBuilder::default()
        .frequency(frequency_hz as i32)
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
        .map_err(|e| Error::Internal(format!("failed to start stack sampler: {e}")))?;

    std::thread::sleep(std::time::Duration::from_millis(duration_ms as u64));

    let report = guard
        .report()
        .build()
        .map_err(|e| Error::Internal(format!("failed to build stack sample report: {e}")))?;

    let sample_count: i64 = report.data.values().map(|c| *c as i64).sum();
    let distinct_stacks = report.data.len();
    let rendered = render_report(&report, duration_ms, frequency_hz, sample_count);

    Ok(serde_json::json!({
        "report": rendered,
        "durationMs": duration_ms,
        "frequencyHz": frequency_hz,
        "sampleCount": sample_count,
        "distinctStacks": distinct_stacks,
    }))
}

/// Render the aggregated report as a human-readable text document: a header
/// with the effective parameters, then one block per distinct stack (highest
/// sample count first), each naming its thread and frames.
#[cfg(unix)]
fn render_report(
    report: &pprof::Report,
    duration_ms: i64,
    frequency_hz: i64,
    sample_count: i64,
) -> String {
    use std::fmt::Write as _;

    let mut entries: Vec<(&pprof::Frames, isize)> =
        report.data.iter().map(|(f, c)| (f, *c)).collect();
    entries.sort_by(|a, b| {
        b.1.cmp(&a.1)
            .then_with(|| a.0.thread_id.cmp(&b.0.thread_id))
    });

    let mut out = String::new();
    let _ = writeln!(
        out,
        "intentd stack sample — {sample_count} samples, {} distinct stacks \
         ({duration_ms} ms at {frequency_hz} Hz, CPU-time sampling: idle/blocked \
         threads accumulate no samples)",
        entries.len()
    );
    if entries.is_empty() {
        let _ = writeln!(
            out,
            "\nNo samples were captured — the daemon consumed no measurable CPU \
             time during the sampling window."
        );
    }
    for (frames, count) in entries {
        let _ = writeln!(
            out,
            "\n{count} samples — thread \"{}\" (id {}):",
            frames.thread_name, frames.thread_id
        );
        let mut depth = 0usize;
        for frame in &frames.frames {
            for symbol in frame {
                let _ = write!(out, "  {depth:3}: {symbol}");
                if let (Some(filename), Some(lineno)) = (&symbol.filename, symbol.lineno) {
                    let _ = write!(out, " ({}:{lineno})", filename.display());
                }
                let _ = writeln!(out);
                depth += 1;
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The profiler (and the in-progress flag) is process-global, so the two
    /// capture-running tests below must not overlap; the cargo test harness
    /// runs tests on parallel threads by default.
    static CAPTURE_SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    #[test]
    fn duration_defaults_and_clamps() {
        assert_eq!(effective_duration_ms(None), DEFAULT_DURATION_MS);
        assert_eq!(effective_duration_ms(Some(0)), MIN_DURATION_MS);
        assert_eq!(effective_duration_ms(Some(-5)), MIN_DURATION_MS);
        assert_eq!(effective_duration_ms(Some(60_000)), MAX_DURATION_MS);
        assert_eq!(effective_duration_ms(Some(2_500)), 2_500);
    }

    #[test]
    fn frequency_defaults_and_clamps() {
        assert_eq!(effective_frequency_hz(None), DEFAULT_FREQUENCY_HZ);
        assert_eq!(effective_frequency_hz(Some(0)), MIN_FREQUENCY_HZ);
        assert_eq!(effective_frequency_hz(Some(10_000)), MAX_FREQUENCY_HZ);
        assert_eq!(effective_frequency_hz(Some(50)), 50);
    }

    /// The capture returns the documented payload with a never-empty `report`
    /// (the header always renders) and the effective clamped parameters, and
    /// never hangs the runtime (the window is short and runs on the blocking
    /// pool). Unix-only — non-Unix asserts the typed `Unsupported` error.
    #[tokio::test]
    async fn sample_stacks_returns_report_payload() {
        let _serial = CAPTURE_SERIAL.lock().await;
        let result = sample_stacks(Some(0), Some(0)).await;

        #[cfg(unix)]
        {
            let v = result.expect("sampling succeeds on unix");
            let report = v["report"].as_str().expect("report is a string");
            assert!(!report.is_empty(), "report is never empty");
            assert!(report.contains("intentd stack sample"), "header: {report}");
            assert_eq!(v["durationMs"], MIN_DURATION_MS);
            assert_eq!(v["frequencyHz"], MIN_FREQUENCY_HZ);
            assert!(v["sampleCount"].is_i64());
            assert!(v["distinctStacks"].is_u64() || v["distinctStacks"].is_i64());
        }

        #[cfg(not(unix))]
        {
            let err = result.expect_err("unsupported off unix");
            assert!(matches!(err, Error::Unsupported(_)));
        }
    }

    /// A pre-existing external `SIGPROF` handler (an attached CPU profiler)
    /// survives a sampling session: `pprof`'s guard drop wipes the SIGPROF
    /// disposition, and `RestoreSigprof` must reinstate the prior handler.
    #[cfg(unix)]
    #[tokio::test]
    async fn preexisting_sigprof_handler_is_restored() {
        let _serial = CAPTURE_SERIAL.lock().await;

        extern "C" fn external_handler(_: libc::c_int) {}

        // SAFETY: installs/reads SIGPROF dispositions on this test process
        // only; serialized with the other capture tests via CAPTURE_SERIAL.
        unsafe {
            let mut external: libc::sigaction = std::mem::zeroed();
            external.sa_sigaction = external_handler as *const () as usize;
            let mut previous: libc::sigaction = std::mem::zeroed();
            assert_eq!(
                libc::sigaction(libc::SIGPROF, &external, &mut previous),
                0,
                "install external handler"
            );

            sample_stacks(Some(0), Some(99))
                .await
                .expect("sampling succeeds");

            let mut after: libc::sigaction = std::mem::zeroed();
            assert_eq!(
                libc::sigaction(libc::SIGPROF, std::ptr::null(), &mut after),
                0,
                "read disposition after sampling"
            );
            // Put the original disposition back before asserting.
            libc::sigaction(libc::SIGPROF, &previous, std::ptr::null_mut());

            assert_eq!(
                after.sa_sigaction, external_handler as *const () as usize,
                "external SIGPROF handler must be restored after sampling"
            );
        }
    }

    /// Overlapping sessions are rejected: while one capture holds the
    /// process-global profiler, a second call fails with the typed
    /// "already in progress" error instead of queueing.
    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_sampling_is_rejected() {
        let _serial = CAPTURE_SERIAL.lock().await;
        let first = tokio::spawn(sample_stacks(Some(1_000), Some(99)));
        // Give the first capture time to install the profiler guard.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        let second = sample_stacks(Some(100), Some(99)).await;
        match second {
            Err(Error::Internal(msg)) => {
                assert!(msg.contains("already in progress"), "msg: {msg}")
            }
            other => panic!("expected in-progress rejection, got {other:?}"),
        }
        first
            .await
            .expect("join")
            .expect("first capture still succeeds");
    }
}
