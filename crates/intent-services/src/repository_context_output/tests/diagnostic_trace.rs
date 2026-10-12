//! Temporary, opt-in worker-only observation. No fixture data or request payloads.
use serde_json::{json, Value};
use std::io::Write;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Mutex, OnceLock,
};
use std::time::{Duration, Instant};

struct Recorder {
    file: std::fs::File,
    start: Instant,
    events: usize,
    write_ns: u128,
}
static RECORDER: OnceLock<Mutex<Recorder>> = OnceLock::new();
static ARM: AtomicUsize = AtomicUsize::new(0);
pub(super) static CALL: AtomicUsize = AtomicUsize::new(0);
static SPAN: AtomicUsize = AtomicUsize::new(0);

pub(crate) fn diagnostic(event: &str, detail: impl std::fmt::Display) {
    emit(event, json!({"detail": detail.to_string()}));
}
fn emit(event: &str, fields: Value) {
    let Some(recorder) = RECORDER.get() else {
        return;
    };
    let mut recorder = recorder.lock().unwrap_or_else(|e| e.into_inner());
    let now = Instant::now();
    let row = json!({"event":event,"pid":std::process::id(),"arm":ARM.load(Ordering::Relaxed),
        "host_index":CALL.load(Ordering::Relaxed),"wall_us":recorder.start.elapsed().as_micros(),"fields":fields});
    // File is unbuffered, preserving partial traces on panic without fsync/event.
    writeln!(recorder.file, "{row}").expect("diagnostic write");
    recorder.events += 1;
    recorder.write_ns += now.elapsed().as_nanos();
}
pub(crate) fn arm(count: usize) {
    ARM.store(count, Ordering::Relaxed);
    CALL.store(0, Ordering::Relaxed);
    diagnostic("arm_start", count);
}

pub(super) struct TraceSpan {
    id: usize,
    event: &'static str,
    index: usize,
    finished: bool,
}
impl TraceSpan {
    pub(super) fn new(event: &'static str, index: usize, records: Option<usize>) -> Self {
        let id = SPAN.fetch_add(1, Ordering::Relaxed) + 1;
        emit(
            "span_start",
            json!({"span":id,"stage":event,"index":index,"records":records}),
        );
        Self {
            id,
            event,
            index,
            finished: false,
        }
    }
    pub(super) fn finish(mut self, result: &str) {
        self.finished = true;
        emit(
            "span_end",
            json!({"span":self.id,"stage":self.event,"index":self.index,"result":result}),
        );
    }
}
impl Drop for TraceSpan {
    fn drop(&mut self) {
        if !self.finished {
            emit(
                "span_cancelled",
                json!({"span":self.id,"stage":self.event,"index":self.index}),
            );
        }
    }
}

#[derive(Default)]
struct Lag {
    samples: usize,
    max_us: u128,
    last: Option<Instant>,
}
pub(crate) struct Probe {
    task: tokio::task::JoinHandle<()>,
    lag: std::sync::Arc<Mutex<Lag>>,
}
impl Drop for Probe {
    fn drop(&mut self) {
        self.task.abort();
        let lag = self.lag.lock().unwrap_or_else(|e| e.into_inner());
        emit(
            "heartbeat_drop",
            json!({"samples":lag.samples,"max_lag_us":lag.max_us,
            "terminal_gap_us":lag.last.map(|t|t.elapsed().as_micros())}),
        );
        let stats = RECORDER.get().map(|r| {
            let r = r.lock().unwrap_or_else(|e| e.into_inner());
            (r.events, r.write_ns)
        });
        emit(
            "logger_summary",
            json!({"events_before_summary":stats.map(|x|x.0),"logging_ns":stats.map(|x|x.1)}),
        );
    }
}
pub(crate) fn start() -> Option<Probe> {
    let path = std::env::var_os("CONTEXT_BOUNDARY_DIAGNOSTIC_LOG")?;
    assert!(std::env::var_os("INTENT_CONTEXT_BOUNDARY_WORKER").is_some());
    let path = std::path::PathBuf::from(path);
    assert!(path.is_absolute());
    assert!(!path.starts_with(std::env::temp_dir()));
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .unwrap();
    assert!(RECORDER
        .set(Mutex::new(Recorder {
            file,
            start: Instant::now(),
            events: 0,
            write_ns: 0
        }))
        .is_ok());
    let git = std::env::split_paths(&std::env::var_os("PATH").unwrap())
        .map(|p| p.join("git"))
        .find(|p| {
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                p.is_file()
                    && p.metadata()
                        .is_ok_and(|m| m.permissions().mode() & 0o111 != 0)
            }
            #[cfg(not(unix))]
            {
                p.is_file()
            }
        })
        .unwrap()
        .canonicalize()
        .unwrap();
    let settings: Value = [
        "INTENTD_WORKSPACE_API_TIMEOUT_MS",
        "INTENTD_TEST_TIMEOUT_MULTIPLIER",
        "INTENTD_ASSERT_BOUND_CALLER",
    ]
    .into_iter()
    .map(|name| {
        (
            name.to_owned(),
            std::env::var(name)
                .ok()
                .map(|v| {
                    if v.bytes().all(|c| c.is_ascii_digit()) {
                        json!(v)
                    } else {
                        json!({"redacted_bytes":v.len()})
                    }
                })
                .unwrap_or(Value::Null),
        )
    })
    .collect();
    let config: Value = std::env::vars_os()
        .filter(|(k, _)| k.to_string_lossy().starts_with("GIT_CONFIG"))
        .map(|(k, v)| (k.to_string_lossy().into_owned(), json!({"bytes":v.len()})))
        .collect();
    #[cfg(unix)]
    let pgid = unsafe { libc::getpgrp() };
    #[cfg(not(unix))]
    let pgid = 0;
    emit(
        "worker_start",
        json!({"current_exe":std::env::current_exe().unwrap(),"git_path":git,"pgid":pgid,
        "settings":settings,"git_config_metadata":config,"tmpdir":std::env::temp_dir(),
        "runtime":"current_thread","fixture_guard_ms":15000}),
    );
    let lag = std::sync::Arc::new(Mutex::new(Lag {
        last: Some(Instant::now()),
        ..Lag::default()
    }));
    let state = lag.clone();
    emit(
        "heartbeat_initial",
        json!({"period_ms":25,"bound_seconds":120}),
    );
    let task = tokio::spawn(async move {
        let start = tokio::time::Instant::now();
        let mut ticks =
            tokio::time::interval_at(start + Duration::from_millis(25), Duration::from_millis(25));
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        while start.elapsed() < Duration::from_secs(120) {
            let scheduled = ticks.tick().await;
            let delay = scheduled.elapsed().as_micros();
            let mut lag = state.lock().unwrap();
            lag.samples += 1;
            lag.max_us = lag.max_us.max(delay);
            lag.last = Some(Instant::now());
            if lag.samples % 40 == 0 || delay > 10_000 {
                emit(
                    "heartbeat",
                    json!({"samples":lag.samples,"lag_us":delay,"max_lag_us":lag.max_us}),
                );
            }
        }
    });
    Some(Probe { task, lag })
}
