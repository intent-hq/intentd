//! The dial right after a cloud sandbox resumes: the sandbox's tailcat path
//! is not up yet, so the first pipe stalls (E2B) or is refused. The connect
//! retries quickly within its budget instead of spending the whole budget on
//! one stalled pipe and then waiting for the connector's backoff.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use super::*;
use crate::connect::{ConnectFailure, ConnectTuning};

const ADDRESS: &str = "tcsandbox1";

#[derive(Clone, Copy, Debug)]
enum Early {
    /// The pipe hangs, as a frozen sandbox's tailcat path does.
    Stalls,
    /// The pipe ends at once, as a peer whose listener is not up.
    Refused,
}

/// A tailcat stand-in that pipes to `127.0.0.1:<port>` only once `ready`
/// exists; before that it behaves as `early`. Each run adds a line to
/// `dir/pipes`.
#[cfg(unix)]
fn sandbox_tailcat(dir: &std::path::Path, ready: &std::path::Path, early: Early) -> PathBuf {
    use std::os::unix::fs::PermissionsExt as _;
    let path = dir.join("tailcat");
    let before = match early {
        Early::Stalls => "exec sleep 600",
        Early::Refused => "exit 8",
    };
    std::fs::write(
        &path,
        format!(
            "#!/usr/bin/env bash\n[ \"$1\" = \"{ADDRESS}\" ] || exit 7\n\
             echo >> '{}'\n\
             [ -e '{}' ] || {before}\n\
             exec 3<>/dev/tcp/127.0.0.1/$2 || exit 8\ncat <&3 &\ncat >&3\n",
            pipes(dir).display(),
            ready.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    path
}

/// The tailcat pipes the stand-in in `dir` has opened.
fn pipes(dir: &std::path::Path) -> PathBuf {
    dir.join("pipes")
}

fn pipes_opened(dir: &std::path::Path) -> usize {
    std::fs::read_to_string(pipes(dir)).map_or(0, |text| text.lines().count())
}

fn target(machine: &Machine, tailcat: PathBuf) -> crate::machine::MachineTarget {
    crate::machine::MachineTarget::from_parts(
        &machine.id(),
        "Sandbox",
        Vec::new(),
        machine.port,
        Some(ADDRESS.into()),
        &machine.certificate.fingerprint256,
        &machine.token,
        Some(tailcat),
    )
    .unwrap()
}

async fn dial_after_resume(
    backend: &Backend,
    target: &crate::machine::MachineTarget,
    tuning: &ConnectTuning,
) -> (Result<crate::connect::Connected, ConnectFailure>, Duration) {
    let owner = crate::lease::new_owner(backend.store(), &target.id)
        .await
        .unwrap();
    let generation = AtomicU64::new(1);
    let started = Instant::now();
    let connected = crate::connect::connect_after_resume(target, tuning, None, || {
        let hello = crate::lease::probe_hello(
            &owner,
            NodeCounter(generation.fetch_add(1, Ordering::SeqCst)),
        );
        async move { Ok(hello) }
    })
    .await;
    (connected, started.elapsed())
}

/// The live E2B resume lost ~10 s here: the first tailcat pipe stalled
/// until the whole connect budget was spent. With a budget far longer than
/// the test's hang guard, a sandbox that answers 1 s after the resume still
/// binds: the connect gave up on the stalled (or refused) pipe and opened a
/// fresh one, instead of waiting out the budget.
#[cfg(unix)]
#[tokio::test]
async fn a_sandbox_that_answers_soon_after_a_resume_binds_on_a_fresh_pipe() {
    for early in [Early::Stalls, Early::Refused] {
        let backend = Backend::new().await;
        let mut machine = backend.machine().await;
        let dir = tempfile::tempdir().unwrap();
        let ready = dir.path().join("ready");
        let target = target(&machine, sandbox_tailcat(dir.path(), &ready, early));
        // Ready 1 s after the first pipe, so that pipe is always early.
        let opening = {
            let ready = ready.clone();
            let dir = dir.path().to_owned();
            tokio::spawn(async move {
                while pipes_opened(&dir) == 0 {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
                std::fs::write(ready, b"").unwrap();
            })
        };
        let tuning = ConnectTuning {
            total: Duration::from_secs(300),
            ..ConnectTuning::default()
        };
        let (connected, _) = tokio::time::timeout(
            intentd_test_support::test_timeout(Duration::from_secs(10)),
            dial_after_resume(&backend, &target, &tuning),
        )
        .await
        .unwrap_or_else(|_| panic!("{early:?}: the dial waited on the early pipe"));
        let connected = connected.unwrap_or_else(|failure| panic!("{early:?}: {failure:?}"));
        assert_eq!(connected.route, format!("tailcat:{ADDRESS}"));
        assert!(
            pipes_opened(dir.path()) >= 2,
            "{early:?}: bound without a retry, so the sandbox was never early"
        );
        connected.dialed.link.close();
        machine.accept().await;
        opening.await.unwrap();
    }
}

/// A sandbox that never answers after the resume still fails as before:
/// `Unreachable`, once the whole connect budget is spent, not sooner.
#[cfg(unix)]
#[tokio::test]
async fn a_sandbox_that_never_answers_after_a_resume_fails_after_the_whole_budget() {
    let backend = Backend::new().await;
    let machine = backend.machine().await;
    let dir = tempfile::tempdir().unwrap();
    let never = dir.path().join("never");
    let target = target(&machine, sandbox_tailcat(dir.path(), &never, Early::Stalls));
    let tuning = ConnectTuning {
        total: Duration::from_secs(8),
        ..ConnectTuning::default()
    };
    let (connected, took) = dial_after_resume(&backend, &target, &tuning).await;
    let Err(ConnectFailure::Unreachable(attempts)) = connected else {
        panic!("expected unreachable, got {:?}", connected.map(|c| c.route));
    };
    assert!(!attempts.is_empty());
    assert!(took >= tuning.total, "gave up after {took:?}");
    assert!(pipes_opened(dir.path()) >= 2, "no retry within the budget");
}

/// A sandbox that refuses this backend's credential after the resume is
/// `Revoked` on the first pipe: a retry cannot change the answer.
#[cfg(unix)]
#[tokio::test]
async fn a_refused_credential_after_a_resume_is_revoked_without_a_retry() {
    let backend = Backend::new().await;
    let machine = backend.machine().await;
    let dir = tempfile::tempdir().unwrap();
    let ready = dir.path().join("ready");
    std::fs::write(&ready, b"").unwrap();
    machine.revoke().await;
    let target = target(&machine, sandbox_tailcat(dir.path(), &ready, Early::Stalls));
    let tuning = ConnectTuning {
        total: Duration::from_secs(300),
        ..ConnectTuning::default()
    };
    let (connected, _) = tokio::time::timeout(
        intentd_test_support::test_timeout(Duration::from_secs(10)),
        dial_after_resume(&backend, &target, &tuning),
    )
    .await
    .expect("the refused credential was retried");
    let Err(ConnectFailure::Revoked(attempts)) = connected else {
        panic!("expected revoked, got {:?}", connected.map(|c| c.route));
    };
    assert_eq!(attempts.len(), 1, "{attempts:?}");
    assert_eq!(pipes_opened(dir.path()), 1);
}
