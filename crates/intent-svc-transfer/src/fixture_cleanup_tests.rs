//! Fixture databases of the transfer tests are cleaned up when the test
//! process exits (the intent-services check of the same name covers core's
//! own fixture consumers).

use std::process::{Command, Stdio};
use std::time::Duration;

use intent_services::feature_api::test_support::test_tempdir;
use intentd_test_support::GuardedChild;

/// Run representative fixture consumers in private child TMPDIRs. The parent
/// observes residue after test teardown without mutating its own environment.
#[test]
fn fixture_databases_are_cleaned_after_consumers_exit() {
    let root = test_tempdir("fixture-db-cleanup-");
    let mut residue = Vec::new();
    for (index, test) in [
        "transfer_export::tests::export_guards_and_abort",
        "transfer_export::tests::export_build_failure_cleans_up_and_allows_retry",
        "transfer_import::tests::import_lifecycle_begin_chunk_commit",
    ]
    .into_iter()
    .enumerate()
    {
        let scratch = root.path().join(index.to_string());
        std::fs::create_dir(&scratch).unwrap();
        let log_path = root.path().join(format!("{index}.log"));
        let log = std::fs::File::create(&log_path).unwrap();
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args(["--exact", test, "--nocapture"])
            .env("TMPDIR", &scratch)
            .env("TMP", &scratch)
            .env("TEMP", &scratch)
            .env_remove("INTENTD_TEST_KEEP_TMP")
            .stdin(Stdio::null())
            .stdout(log.try_clone().unwrap())
            .stderr(log);
        let mut child = GuardedChild::spawn(&mut command).unwrap();
        let status = child
            .wait_with_timeout(intentd_test_support::test_timeout(Duration::from_secs(120)))
            .unwrap();
        let output = std::fs::read_to_string(&log_path).unwrap();
        assert!(
            status.is_some_and(|status| status.success()) && output.contains("1 passed"),
            "fixture consumer {test} failed ({status:?}):\n{output}"
        );
        let remaining: Vec<_> = std::fs::read_dir(&scratch)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();
        if !remaining.is_empty() {
            residue.push((test, remaining));
        }
    }
    assert!(residue.is_empty(), "fixture residue: {residue:?}");
}
