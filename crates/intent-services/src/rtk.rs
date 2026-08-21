//! RTK (compressed CLI output) detection and prompt injection.
//!
//! Detects the `rtk` binary on the daemon host, parses its subcommands from
//! `rtk help` output, and provides the filtered subcommand list for prompt
//! injection when enabled via `rtk.enabled` setting.
//!
//! Mirrors `cloudlands-fe/src/features/agent/main/rtk-detector.ts` including
//! its exclusion lists (`RTK_INTERNAL_COMMANDS` and `CONFLICTING_COMMANDS`).

use std::process::Command;
use std::sync::mpsc;
use std::sync::Mutex;
use std::time::Duration;

/// rtk-internal / meta commands that shouldn't be used as prefixes
const RTK_INTERNAL_COMMANDS: &[&str] = &[
    "smart",
    "err",
    "summary",
    "init",
    "config",
    "gain",
    "cc-economics",
    "discover",
    "learn",
    "proxy",
    "verify",
    "hook-audit",
    "rewrite",
    "help",
];

/// Commands that conflict with shell builtins or are too generic
const CONFLICTING_COMMANDS: &[&str] = &[
    "read", "test", "json", "deps", "env", "log", "lint", "format", "next",
];

fn is_excluded(cmd: &str) -> bool {
    RTK_INTERNAL_COMMANDS.contains(&cmd) || CONFLICTING_COMMANDS.contains(&cmd)
}

/// RTK detection result, cached per daemon run.
#[derive(Debug, Clone)]
pub(crate) struct RtkStatus {
    pub available: bool,
    pub subcommands: Vec<String>,
}

/// Module-level cache: detection runs at most once per daemon lifetime.
static CACHED_STATUS: Mutex<Option<RtkStatus>> = Mutex::new(None);

/// Detect whether rtk is installed and parse its subcommands.
/// Results are cached — only runs detection once per daemon process.
pub(crate) fn detect_rtk() -> RtkStatus {
    // Best-effort feature gate: if mutex is poisoned, clear it and re-detect
    let mut cache = CACHED_STATUS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(ref status) = *cache {
        return status.clone();
    }

    let status = do_detect();
    *cache = Some(status.clone());
    status
}

fn do_detect() -> RtkStatus {
    const DETECTION_TIMEOUT: Duration = Duration::from_secs(2);

    // 1. Check if rtk exists with a timeout
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let result = Command::new("which")
            .arg("rtk")
            .output()
            .ok()
            .filter(|out| out.status.success());
        let _ = tx.send(result);
    });

    let rtk_path = match rx.recv_timeout(DETECTION_TIMEOUT) {
        Ok(Some(_)) => "rtk",
        _ => {
            return RtkStatus {
                available: false,
                subcommands: vec![],
            }
        }
    };

    // 2. Parse subcommands from `rtk help` with a timeout
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let result = Command::new(rtk_path)
            .arg("help")
            .output()
            .ok()
            .filter(|out| out.status.success())
            .and_then(|out| String::from_utf8(out.stdout).ok());
        let _ = tx.send(result);
    });

    match rx.recv_timeout(DETECTION_TIMEOUT) {
        Ok(Some(stdout)) => {
            let subcommands = parse_rtk_help(&stdout);
            RtkStatus {
                available: !subcommands.is_empty(),
                subcommands,
            }
        }
        _ => RtkStatus {
            available: false,
            subcommands: vec![],
        },
    }
}

/// Parse the `Commands:` section of `rtk help` output.
/// Each command line looks like: `  ls             List directory...`
pub(crate) fn parse_rtk_help(output: &str) -> Vec<String> {
    let lines = output.lines();
    let mut in_commands = false;
    let mut subcommands = Vec::new();

    for line in lines {
        let trimmed = line.trim();

        // Start of Commands section
        if trimmed.eq_ignore_ascii_case("commands:")
            || trimmed.eq_ignore_ascii_case("available commands:")
        {
            in_commands = true;
            continue;
        }

        // End of commands section on blank line or new section header
        if in_commands
            && (trimmed.is_empty() || trimmed.chars().next().is_some_and(char::is_uppercase))
        {
            if !subcommands.is_empty() {
                break;
            }
            continue;
        }

        if in_commands {
            // Command lines start with 2+ spaces (mirrors rtk-detector.ts regex ^\s{2,})
            if line.len() >= 2 && line.chars().take(2).all(char::is_whitespace) {
                if let Some(cmd) = line.split_whitespace().next() {
                    if !is_excluded(cmd) {
                        subcommands.push(cmd.to_string());
                    }
                }
            }
        }
    }

    subcommands
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_rtk_help_basic() {
        let output = "Commands:\n  ls  List directory\n  cat  Cat files\n";
        assert_eq!(parse_rtk_help(output), vec!["ls", "cat"]);
    }

    #[test]
    fn test_parse_rtk_help_excludes_internal() {
        let output = "Commands:\n  ls  List\n  help  Show help\n  init  Initialize\n";
        assert_eq!(parse_rtk_help(output), vec!["ls"]);
    }

    #[test]
    fn test_parse_rtk_help_excludes_conflicting() {
        let output = "Commands:\n  ls  List\n  test  Run tests\n  read  Read file\n";
        assert_eq!(parse_rtk_help(output), vec!["ls"]);
    }

    #[test]
    fn test_parse_rtk_help_case_insensitive_header() {
        let output = "Available Commands:\n  ls  List\n";
        assert_eq!(parse_rtk_help(output), vec!["ls"]);
    }

    #[test]
    fn test_parse_rtk_help_stops_at_blank_line() {
        let output = "Commands:\n  ls  List\n  cat  Cat\n\nOptions:\n  --help  Show help\n";
        assert_eq!(parse_rtk_help(output), vec!["ls", "cat"]);
    }

    #[test]
    fn test_parse_rtk_help_stops_at_new_section() {
        let output = "Commands:\n  ls  List\n  cat  Cat\nOptions:\n  --help  Show help\n";
        assert_eq!(parse_rtk_help(output), vec!["ls", "cat"]);
    }

    #[test]
    fn test_parse_rtk_help_accepts_more_than_two_spaces() {
        // Real rtk help often uses varied indentation (3+ spaces)
        let output = "Commands:\n   ls    List directory\n    cat    Cat files\n  grep  Search\n";
        assert_eq!(parse_rtk_help(output), vec!["ls", "cat", "grep"]);
    }
}
