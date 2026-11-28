//! A test that hangs on purpose, so the timeout report of `make test-stress`
//! can be seen end to end:
//!
//! ```text
//! INTENTD_TEST_HANG_FIXTURE=1 make test-stress \
//!   E='binary_id(=intentd-test-support::hanging_fixture)' COUNT=2
//! ```
//!
//! prints, for each TIMEOUT, the folder below and its `daemon.log` tail. A
//! `.config/nextest.toml` override kills it after 2 x 5s. Without
//! `INTENTD_TEST_HANG_FIXTURE` it passes at once, so ordinary runs skip the wait.

#[test]
fn hanging_fixture() {
    if std::env::var_os("INTENTD_TEST_HANG_FIXTURE").is_none_or(|value| value.is_empty()) {
        return;
    }
    let dir = tempfile::Builder::new()
        .prefix("itd-hang-")
        .tempdir()
        .expect("create the fixture folder");
    intentd_test_support::record_test_tempdir(dir.path());
    let log: String = (1..=5)
        .map(|n| format!("fixture daemon line {n}\n"))
        .chain(["last line before the hang\n".to_owned()])
        .collect();
    std::fs::write(dir.path().join("daemon.log"), log).expect("write daemon.log");
    loop {
        std::thread::park();
    }
}
