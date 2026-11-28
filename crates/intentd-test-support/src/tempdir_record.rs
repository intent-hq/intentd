//! Where a test's scratch folders are, for a runner whose test was killed.
//!
//! A nextest timeout kills the test process, so no in-process code (the
//! panic hook that keeps a failed test's folder, intentd#1889) runs and the
//! runner sees only "TIMEOUT". `make test-stress` therefore names a record
//! file in [`TEMPDIR_RECORD_ENV`], and each test temp folder adds one line to
//! it when it is created. After the run, the runner reads the lines of each
//! timed-out iteration and prints the folder (still on disk: the killed
//! process never dropped its `TempDir`) and the tail of its logs.
//!
//! A line is tab-separated: `NEXTEST_STRESS_CURRENT`, `NEXTEST_BINARY_ID`,
//! `NEXTEST_TEST_NAME`, the test's pid, then the folder path (last, so a tab
//! in the path survives). `scripts/nextest_stress.py` in the monorepo reads it.

use std::io::Write as _;
use std::path::Path;

/// The record file `make test-stress` asks test temp folders to be listed in.
pub const TEMPDIR_RECORD_ENV: &str = "INTENTD_TEST_TEMPDIR_RECORD";

/// List `dir` in the file named by [`TEMPDIR_RECORD_ENV`]; a no-op when it
/// is unset. Best effort: a test never fails because its folder could not
/// be recorded.
pub fn record_test_tempdir(dir: &Path) {
    let Some(record) = std::env::var_os(TEMPDIR_RECORD_ENV).filter(|value| !value.is_empty())
    else {
        return;
    };
    let field = |name: &str| std::env::var(name).unwrap_or_default();
    append_tempdir_record(
        Path::new(&record),
        &field("NEXTEST_STRESS_CURRENT"),
        &field("NEXTEST_BINARY_ID"),
        &field("NEXTEST_TEST_NAME"),
        dir,
    );
}

fn append_tempdir_record(record: &Path, iteration: &str, binary: &str, test: &str, dir: &Path) {
    let line = format!(
        "{iteration}\t{binary}\t{test}\t{}\t{}\n",
        std::process::id(),
        dir.display()
    );
    // One write to an O_APPEND file, so lines from concurrent tests never interleave.
    let _ = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(record)
        .and_then(|mut file| file.write_all(line.as_bytes()));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_folder_appends_one_line_in_the_format_the_stress_runner_reads() {
        let scratch = tempfile::tempdir().expect("scratch");
        let record = scratch.path().join("tempdirs.tsv");
        let pid = std::process::id();
        append_tempdir_record(
            &record,
            "4",
            "intentd::suite_e2e",
            "a::b",
            Path::new("/x/itd-1"),
        );
        append_tempdir_record(&record, "", "", "", Path::new("/x/with\ttab"));
        assert_eq!(
            std::fs::read_to_string(&record).expect("record"),
            format!("4\tintentd::suite_e2e\ta::b\t{pid}\t/x/itd-1\n\t\t\t{pid}\t/x/with\ttab\n")
        );
    }
}
