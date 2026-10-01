//! Shutdown diagnostics only: no timeout, cancellation or runtime policy changes.

use std::io::{self, Write};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tracing_appender::{non_blocking::NonBlocking, rolling::RollingFileAppender};
use tracing_subscriber::fmt::writer::{EitherWriter, MakeWriter, MutexGuardWriter};

pub(crate) const TARGET: &str = "intentd::shutdown";

pub(crate) fn is_timing_target(target: &str) -> bool {
    matches!(target, TARGET | "intent_store::close")
}

/// Explicit completion avoids reporting success on cancellation or unwind.
pub(crate) struct Phase {
    name: &'static str,
    started: Instant,
}

impl Phase {
    pub(crate) fn start(name: &'static str) -> Self {
        let started = Instant::now();
        tracing::info!(target: TARGET, phase = name, state = "started", elapsed_ms = 0_u64, "shutdown phase");
        Self { name, started }
    }

    pub(crate) fn complete(self) {
        tracing::info!(target: TARGET, phase = self.name, state = "completed",
            elapsed_ms = u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX),
            "shutdown phase");
    }

    pub(crate) fn failed(self) {
        tracing::info!(target: TARGET, phase = self.name, state = "failed",
            elapsed_ms = u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX),
            "shutdown phase");
    }
}

pub(crate) fn drop_runtime(runtime: tokio::runtime::Runtime) {
    let phase = Phase::start("runtime_drop");
    drop(runtime);
    phase.complete();
}

/// The queued writer and shutdown diagnostics share one rotation state and
/// serialize whole records through the same lock.
#[derive(Clone)]
pub(crate) struct SharedAppender(Arc<Mutex<RollingFileAppender>>);

impl SharedAppender {
    pub(crate) fn new(appender: RollingFileAppender) -> Self {
        Self(Arc::new(Mutex::new(appender)))
    }
}

impl Write for SharedAppender {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        // Hold the same lock as direct records across partial OS writes too.
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .write_all(buf)?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .flush()
    }
}

/// These few records must reach the file before the following blocking boundary,
/// including the last record after runtime drop. Ordinary logs remain queued.
/// This trades a possible file-lock/disk wait for visibility even if the process
/// is killed at the next boundary; it does not fsync or guarantee crash durability.
pub(crate) struct FileWriter {
    pub(crate) direct: SharedAppender,
    pub(crate) queued: NonBlocking,
}

impl<'a> MakeWriter<'a> for FileWriter {
    type Writer = EitherWriter<MutexGuardWriter<'a, RollingFileAppender>, NonBlocking>;

    fn make_writer(&'a self) -> Self::Writer {
        EitherWriter::B(self.queued.clone())
    }

    fn make_writer_for(&'a self, meta: &tracing::Metadata<'_>) -> Self::Writer {
        if is_timing_target(meta.target()) {
            EitherWriter::A(self.direct.0.as_ref().make_writer())
        } else {
            self.make_writer()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use std::time::Duration;
    use tracing_subscriber::{layer::SubscriberExt, Layer};

    struct Events(mpsc::Sender<String>);

    impl<S: tracing::Subscriber> Layer<S> for Events {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _: tracing_subscriber::layer::Context<'_, S>,
        ) {
            struct Fields(String);
            impl tracing::field::Visit for Fields {
                fn record_debug(
                    &mut self,
                    field: &tracing::field::Field,
                    value: &dyn std::fmt::Debug,
                ) {
                    use std::fmt::Write as _;
                    write!(self.0, "{}={value:?} ", field.name()).unwrap();
                }
            }
            let mut fields = Fields(String::new());
            event.record(&mut fields);
            let _ = self.0.send(fields.0);
        }
    }

    #[test]
    fn normal_runtime_drop_logs_before_waiting_for_blocking_work_and_after_release() {
        let dir = tempfile::tempdir().unwrap();
        let appender = tracing_appender::rolling::never(dir.path(), "shutdown.log");
        let direct = SharedAppender::new(appender);
        let (queued, _guard) = tracing_appender::non_blocking(direct.clone());
        let (events, observed) = mpsc::channel();
        // Event notification happens after the synchronous file layer writes.
        let subscriber = tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer()
                    .with_ansi(false)
                    .with_writer(FileWriter { direct, queued }),
            )
            .with(Events(events));
        let runtime = crate::build_runtime();
        let (entered, started) = mpsc::channel();
        let (release, released) = mpsc::channel();
        runtime.block_on(async {
            runtime.spawn_blocking(move || {
                entered.send(()).unwrap();
                // Dropping the sender on test failure also releases the worker.
                let _ = released.recv();
            });
        });
        started.recv_timeout(Duration::from_secs(10)).unwrap();
        let thread = std::thread::spawn(move || {
            tracing::subscriber::with_default(subscriber, || drop_runtime(runtime));
        });
        let start = observed.recv_timeout(Duration::from_secs(10)).unwrap();
        assert!(
            start.contains("phase=\"runtime_drop\" state=\"started\""),
            "{start}"
        );
        let before = std::fs::read_to_string(dir.path().join("shutdown.log")).unwrap();
        assert!(
            before.contains("phase=\"runtime_drop\" state=\"started\""),
            "{before}"
        );
        assert!(!before.contains("state=\"completed\""), "{before}");
        assert!(matches!(
            observed.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));
        release.send(()).unwrap();
        let end = observed.recv_timeout(Duration::from_secs(10)).unwrap();
        assert!(
            end.contains("phase=\"runtime_drop\" state=\"completed\""),
            "{end}"
        );
        assert!(end.contains("elapsed_ms="));
        thread.join().unwrap();
        let after = std::fs::read_to_string(dir.path().join("shutdown.log")).unwrap();
        assert!(
            after.contains("phase=\"runtime_drop\" state=\"completed\""),
            "{after}"
        );
    }

    #[test]
    fn queued_and_direct_records_share_one_serialized_appender() {
        let dir = tempfile::tempdir().unwrap();
        let direct = SharedAppender::new(tracing_appender::rolling::never(dir.path(), "mixed.log"));
        let (queued, guard) = tracing_appender::non_blocking(direct.clone());
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_writer(FileWriter { direct, queued })
            .finish();
        let dispatch = tracing::Dispatch::new(subscriber);
        let start = Arc::new(std::sync::Barrier::new(3));
        std::thread::scope(|scope| {
            for lane in 0..3 {
                let dispatch = dispatch.clone();
                let start = start.clone();
                scope.spawn(move || {
                    tracing::dispatcher::with_default(&dispatch, || {
                        start.wait();
                        for sequence in 0..100 {
                            // Large records also cover the queued writer's batched writes.
                            let payload = format!("{lane}:{sequence}:{}", "x".repeat(8192));
                            match lane {
                                0 => tracing::info!(target: "ordinary", payload, "record"),
                                1 => tracing::info!(target: TARGET, payload, "record"),
                                _ => {
                                    tracing::info!(target: "intent_store::close", payload, "record");
                                }
                            }
                        }
                    });
                });
            }
        });
        // Drain only the ordinary records; direct records need no guard flush.
        drop(guard);
        let log = std::fs::read_to_string(dir.path().join("mixed.log")).unwrap();
        let lines: std::collections::HashSet<_> = log.lines().collect();
        assert_eq!(log.lines().count(), 300);
        assert_eq!(lines.len(), 300);
        for (lane, target) in [(0, "ordinary"), (1, TARGET), (2, "intent_store::close")] {
            for sequence in 0..100 {
                let expected = format!(
                    " INFO {target}: record payload=\"{lane}:{sequence}:{}\"",
                    "x".repeat(8192)
                );
                assert!(
                    lines.contains(expected.as_str()),
                    "missing or interleaved {lane}:{sequence}"
                );
            }
        }
    }

    #[test]
    fn incomplete_failed_and_unwound_phases_do_not_report_success() {
        let (events, observed) = mpsc::channel();
        let subscriber = tracing_subscriber::registry().with(Events(events));
        tracing::subscriber::with_default(subscriber, || {
            {
                let _phase = Phase::start("cancelled");
            }
            Phase::start("failed").failed();
            let _ = std::panic::catch_unwind(|| {
                let _phase = Phase::start("unwound");
                panic!("test unwind");
            });
        });
        let records: Vec<_> = observed.try_iter().collect();
        assert_eq!(records.len(), 4, "{records:?}");
        assert!(records.iter().all(|r| !r.contains("state=\"completed\"")));
        assert!(records[2].contains("state=\"failed\""));
    }
}
