#![cfg(target_os = "linux")]

use std::io::Read;
use std::process::{Command, Stdio};
use std::time::Duration;

use intentd_test_support::{test_timeout, GuardedChild};

#[test]
fn native_owner_confirms_reaping_without_proc_children() {
    let fixture = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/probe-owner-check.py");
    let mut command = Command::new("python3");
    command
        .arg(fixture)
        .arg(env!("CARGO_BIN_EXE_intentd"))
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = GuardedChild::spawn(&mut command).unwrap();
    let status = child
        .wait_with_timeout(test_timeout(Duration::from_secs(60)))
        .unwrap()
        .expect("process ownership regression fixture completed");
    let mut errors = String::new();
    child
        .stderr
        .take()
        .unwrap()
        .read_to_string(&mut errors)
        .unwrap();
    assert!(status.success(), "{errors}");
}
