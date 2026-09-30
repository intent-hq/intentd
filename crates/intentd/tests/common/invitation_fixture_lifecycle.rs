//! Exercise the invitation fixture's actual guard on return, error, and unwind.
//! The stand-in daemon has a live child holding stdout open; EOF proves that
//! dropping the guard also stopped that child, rather than only its parent.

use std::os::fd::OwnedFd;
use std::process::{Command, Stdio};
use std::time::Duration;

use intentd_test_support::GuardedChild;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::Daemon;

async fn assert_fixture_teardown(teardown: impl FnOnce(Daemon)) {
    let (reader, writer) = std::os::unix::net::UnixStream::pair().expect("stdout pair");
    reader.set_nonblocking(true).expect("nonblocking reader");
    let mut reader = tokio::net::UnixStream::from_std(reader).expect("async reader");
    let mut command = Command::new("sh");
    command
        .arg("-c")
        // timing-guard: parked child must outlive the EOF deadline unless fixture teardown stops it
        .arg("sh -c 'printf ready; exec sleep 600' & wait")
        .stdin(Stdio::null())
        .stdout(Stdio::from(OwnedFd::from(writer)))
        .stderr(Stdio::null());
    let child = GuardedChild::spawn(&mut command).expect("spawn fixture daemon");
    // Command retains its configured writer. Close it so only the process tree
    // can keep the read end open after teardown.
    drop(command);
    let daemon = Daemon { child };
    let mut ready = [0; 5];
    tokio::time::timeout(Duration::from_secs(3), reader.read_exact(&mut ready))
        .await
        .expect("descendant readiness deadline")
        .expect("descendant readiness");
    assert_eq!(&ready, b"ready");
    teardown(daemon);
    reader.shutdown().await.expect("close unused write half");
    let mut remaining = Vec::new();
    tokio::time::timeout(Duration::from_secs(3), reader.read_to_end(&mut remaining))
        .await
        .expect("fixture descendant still holds stdout after daemon teardown")
        .expect("read fixture EOF");
    assert!(remaining.is_empty());
}

#[tokio::test]
async fn fixture_drop_stops_descendant_on_return() {
    assert_fixture_teardown(drop).await;
}

#[tokio::test]
async fn fixture_drop_stops_descendant_on_error() {
    assert_fixture_teardown(|daemon| {
        fn fail(daemon: Daemon) -> Result<(), &'static str> {
            let _daemon = daemon;
            Err("deliberate fixture failure")
        }
        let result = fail(daemon);
        assert_eq!(result, Err("deliberate fixture failure"));
    })
    .await;
}

#[tokio::test]
async fn fixture_drop_stops_descendant_on_panic() {
    assert_fixture_teardown(|daemon| {
        let result = std::panic::catch_unwind(|| {
            let _daemon = daemon;
            panic!("deliberate fixture panic");
        });
        assert!(result.is_err(), "preserve the fixture panic outcome");
    })
    .await;
}
