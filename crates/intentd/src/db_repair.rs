//! `intentd doctor` integrity output and the offline `intentd repair-indexes`
//! command (intent-hq/intent#8592).

use std::path::Path;
use std::process::ExitCode;

use intent_store::{DerivedIndex, IntegrityReport};

/// Directory under the data dir that receives pre-repair database copies.
pub(crate) const BACKUP_DIR: &str = "backups";

/// Status tag for the integrity summary line.
#[derive(Clone, Copy)]
pub(crate) enum Severity {
    /// The problem is being reported before a repair attempt.
    Warn,
    /// The problem remains and fails the command.
    Fail,
}

/// Print a non-clean integrity report with the next step for each kind of
/// damage. `data_dir` is named in the advice for unrepairable damage.
pub(crate) fn print_problems(report: &IntegrityReport, data_dir: &Path, severity: Severity) {
    let tag = match severity {
        Severity::Warn => "WARN",
        Severity::Fail => "FAIL",
    };
    println!(
        "  [{tag}] integrity_check: {} issue(s) found",
        report.problems.len()
    );
    for problem in &report.problems {
        let message = problem.message.replace('\n', " ");
        match &problem.table {
            Some(table) => println!("    - {message} (table {table})"),
            None => println!("    - {message}"),
        }
    }
    for index in report.damaged_indexes() {
        println!(
            "  [WARN] the {} ({}) is damaged. It holds only derived data, so the \
             underlying rows are intact, but writes that update it fail with \
             \"database disk image is malformed\" (code 267) until it is rebuilt.",
            index.description(),
            index.table()
        );
    }
    if !report.damaged_indexes().is_empty() {
        println!(
            "         fix: quit the Intent app, run `intentd stop`, then \
             `intentd repair-indexes` (it backs up the database first)."
        );
    }
    if report.has_unrepairable() {
        let mut tables: Vec<&str> = report
            .problems
            .iter()
            .filter(|p| p.repairable.is_none())
            .filter_map(|p| p.table.as_deref())
            .collect();
        tables.sort_unstable();
        tables.dedup();
        let scope = if tables.is_empty() {
            String::new()
        } else {
            format!(" (tables: {})", tables.join(", "))
        };
        println!(
            "  [WARN] damage outside the rebuildable search indexes{scope}. \
             `intentd repair-indexes` cannot fix it. Stop writing: quit the Intent \
             app, run `intentd stop`, and copy the whole data folder ({}) somewhere \
             safe before attempting any recovery.",
            data_dir.display()
        );
    }
}

fn label(indexes: &[DerivedIndex]) -> String {
    indexes
        .iter()
        .map(|i| format!("{} ({})", i.description(), i.table()))
        .collect::<Vec<_>>()
        .join(", ")
}

/// `intentd repair-indexes`: back up, rebuild damaged derived indexes, re-check.
pub(crate) async fn cmd_repair_indexes(all: bool) -> ExitCode {
    let config = match crate::resolve_config() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("error: {e}");
            return ExitCode::FAILURE;
        }
    };
    // Hold the same single-instance lock as `intentd serve` for the whole
    // repair, so a daemon starting meanwhile refuses instead of racing it.
    let _instance = match crate::acquire_data_dir_lock(&config) {
        Ok(lock) => lock,
        Err(e) => {
            eprintln!("error: {e}");
            eprintln!("quit the Intent app and run `intentd stop` before `intentd repair-indexes`");
            return ExitCode::FAILURE;
        }
    };
    let backup_root = config.data_dir.join(BACKUP_DIR);
    println!("database: {}", config.db_path.display());
    let report =
        match intent_store::repair_derived_indexes(&config.db_path, &backup_root, all).await {
            Ok(report) => report,
            Err(e) => {
                eprintln!("error: {e}");
                return ExitCode::FAILURE;
            }
        };
    println!("[ok] backup written: {}", report.backup_dir.display());
    println!("before repair:");
    if report.before.is_ok() {
        println!("  [ok] integrity_check: ok");
    } else {
        print_problems(&report.before, &config.data_dir, Severity::Warn);
    }
    if report.rebuilt.is_empty() {
        println!("[ok] no derived index needed a rebuild (pass --all to rebuild anyway)");
    } else {
        println!("[ok] rebuilt: {}", label(&report.rebuilt));
    }
    println!("after repair:");
    if report.after.is_ok() {
        println!("  [ok] integrity_check: ok");
        ExitCode::SUCCESS
    } else {
        print_problems(&report.after, &config.data_dir, Severity::Fail);
        println!(
            "  [FAIL] the database still has problems; the pre-repair copy is in {}",
            report.backup_dir.display()
        );
        ExitCode::FAILURE
    }
}
