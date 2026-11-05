//! Per-invocation record of the `ws.*` calls a `workspace_api` script made,
//! rendered only when the script fails (intent-hq/intent#8592).
//!
//! A rejection the script does not catch replaces the whole tool result, so
//! the results of calls that already finished are lost. When one of those
//! calls had an external side effect — a `ws.host.exec` that sent a review `POST` —
//! the agent saw only the later error (a note write failing on a corrupted
//! store) and could not tell the POST had landed; a blind retry would repeat
//! it. The ledger lists every call in call order with its outcome, keeps a
//! bounded head of each `host.exec`'s output, and names calls that never
//! answered, so the agent can reconcile before retrying anything.

use std::fmt::Write as _;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use serde_json::Value;

/// Most plain successes rendered individually; beyond that they collapse
/// into counts. `host.exec`, failed and unanswered calls are always listed.
const MAX_PLAIN: usize = 20;
/// Characters kept from each `host.exec` stdout/stderr and from each error.
const MAX_TEXT: usize = 500;

/// `SQLite`'s text for the `SQLITE_CORRUPT` family (codes 11, 267, 523, 779)
/// and for `SQLITE_NOTADB` (26).
const CORRUPTION_MARKERS: &[&str] = &["database disk image is malformed", "file is not a database"];

/// What a finished `host.exec` reported, already bounded so the ledger never
/// holds a command's whole output.
struct ExecSummary {
    exit_code: Option<i64>,
    timed_out: bool,
    stdout: String,
    stderr: String,
}

enum Outcome {
    Pending,
    Succeeded,
    Executed(ExecSummary),
    Failed(String),
}

impl Outcome {
    /// Whether the entry must be listed even when plain successes collapse.
    fn is_notable(&self) -> bool {
        !matches!(self, Self::Succeeded)
    }
}

struct Entry {
    method: String,
    outcome: Outcome,
}

/// Shared between the host closure (records frames) and the dispatch
/// (renders on failure).
#[derive(Clone, Default)]
pub(super) struct CallLedger(Arc<Mutex<Vec<Entry>>>);

impl CallLedger {
    fn entries(&self) -> MutexGuard<'_, Vec<Entry>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Record a call when its host frame first runs. Returns the slot for
    /// [`Self::finish`].
    pub(super) fn start(&self, method: &str) -> usize {
        let mut entries = self.entries();
        entries.push(Entry {
            method: method.to_string(),
            outcome: Outcome::Pending,
        });
        entries.len() - 1
    }

    /// Record a call's reply. A frame dropped before replying stays pending.
    pub(super) fn finish(&self, slot: usize, result: &Result<Value, String>) {
        let mut entries = self.entries();
        let Some(entry) = entries.get_mut(slot) else {
            return;
        };
        entry.outcome = match result {
            Ok(value) if entry.method == "host.exec" => Outcome::Executed(summarize_exec(value)),
            Ok(_) => Outcome::Succeeded,
            Err(error) => Outcome::Failed(bounded(error)),
        };
    }

    /// Append the ledger to a failed script's error text. `include_output`
    /// is false when a private-result policy governs this invocation, so only
    /// exit codes show. The list renders only when some call succeeded or got
    /// no reply: a lone failed call is already the error itself.
    pub(super) fn annotate(&self, error: &str, include_output: bool) -> String {
        let entries = self.entries();
        let corrupted = std::iter::once(error)
            .chain(entries.iter().filter_map(|e| match &e.outcome {
                Outcome::Failed(message) => Some(message.as_str()),
                _ => None,
            }))
            .any(is_corruption);
        let mut out = error.to_string();
        if entries
            .iter()
            .any(|e| !matches!(e.outcome, Outcome::Failed(_)))
        {
            out.push_str(
                "\n\nws.* calls this script made before it failed (their results were not returned):",
            );
            let collapse = entries.len() > MAX_PLAIN;
            let mut collapsed = 0usize;
            for (index, entry) in entries.iter().enumerate() {
                if collapse && !entry.outcome.is_notable() {
                    collapsed += 1;
                    continue;
                }
                flush_collapsed(&mut out, &mut collapsed);
                let _ = write!(out, "\n{}. ws.{} — ", index + 1, entry.method);
                render_outcome(&mut out, &entry.outcome, include_output);
            }
            flush_collapsed(&mut out, &mut collapsed);
            out.push_str(
                "\nCalls that succeeded already took effect, and calls with no reply may have \
                 too: check their outcome before retrying so side effects are not repeated. \
                 Wrap each call in try/catch to keep its result when a later call fails.",
            );
        }
        if corrupted {
            out.push_str(
                "\n\nThe daemon's database reported corruption, so writes may not have been \
                 saved. Stop writing and tell the user: they should back up the Intent data \
                 folder and run `intentd doctor`, which checks database integrity, before any \
                 repair.",
            );
        }
        out
    }
}

fn flush_collapsed(out: &mut String, collapsed: &mut usize) {
    if *collapsed > 0 {
        let _ = write!(out, "\n   ({collapsed} other calls succeeded)");
        *collapsed = 0;
    }
}

fn summarize_exec(value: &Value) -> ExecSummary {
    let stream = |name: &str| {
        value
            .get(name)
            .and_then(Value::as_str)
            .map(|text| bounded(text.trim_end()))
            .unwrap_or_default()
    };
    ExecSummary {
        exit_code: value.get("exitCode").and_then(Value::as_i64),
        timed_out: value.get("timedOut").and_then(Value::as_bool) == Some(true),
        stdout: stream("stdout"),
        stderr: stream("stderr"),
    }
}

fn render_outcome(out: &mut String, outcome: &Outcome, include_output: bool) {
    match outcome {
        Outcome::Pending => out.push_str("no reply before the script ended; outcome unknown"),
        Outcome::Failed(message) => {
            out.push_str("failed: ");
            out.push_str(message);
        }
        Outcome::Succeeded => out.push_str("succeeded"),
        Outcome::Executed(exec) => {
            match exec.exit_code {
                Some(code) => {
                    let _ = write!(out, "succeeded (exitCode {code}");
                }
                None => out.push_str("succeeded (no exit code: killed by a signal"),
            }
            if exec.timed_out {
                out.push_str(", timed out");
            }
            out.push(')');
            if include_output {
                for (name, text) in [("stdout", &exec.stdout), ("stderr", &exec.stderr)] {
                    if !text.trim().is_empty() {
                        let _ = write!(out, "\n   {name}: {text}");
                    }
                }
            }
        }
    }
}

fn bounded(text: &str) -> String {
    let mut chars = text.chars();
    let head: String = chars.by_ref().take(MAX_TEXT).collect();
    if chars.next().is_some() {
        format!("{head}… (truncated)")
    } else {
        head
    }
}

fn is_corruption(message: &str) -> bool {
    CORRUPTION_MARKERS.iter().any(|m| message.contains(m))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn empty_ledger_leaves_plain_errors_alone() {
        assert_eq!(
            CallLedger::default().annotate("Error: boom", true),
            "Error: boom"
        );
    }

    #[test]
    fn omits_command_output_under_a_private_policy() {
        let ledger = CallLedger::default();
        let slot = ledger.start("host.exec");
        ledger.finish(slot, &Ok(json!({ "exitCode": 0, "stdout": "secret-ish" })));
        let text = ledger.annotate("Error: later", false);
        assert!(
            text.contains("1. ws.host.exec — succeeded (exitCode 0)"),
            "{text}"
        );
        assert!(!text.contains("secret-ish"), "{text}");
    }

    #[test]
    fn collapses_plain_successes_but_keeps_side_effects() {
        let ledger = CallLedger::default();
        let slot = ledger.start("host.exec");
        ledger.finish(
            slot,
            &Ok(json!({ "exitCode": 0, "stdout": "y".repeat(600) })),
        );
        for _ in 0..25 {
            let slot = ledger.start("note.read");
            ledger.finish(slot, &Ok(json!({})));
        }
        let slot = ledger.start("note.add");
        ledger.finish(slot, &Err("boom".to_string()));
        let text = ledger.annotate("Error: boom", true);
        assert!(
            text.contains("\n1. ws.host.exec — succeeded (exitCode 0)"),
            "{text}"
        );
        assert!(text.contains("… (truncated)"), "{text}");
        assert!(text.contains("(25 other calls succeeded)"), "{text}");
        assert!(!text.contains("ws.note.read"), "{text}");
        assert!(text.contains("\n27. ws.note.add — failed: boom"), "{text}");
    }

    #[test]
    fn lone_failed_call_adds_no_list() {
        let ledger = CallLedger::default();
        let slot = ledger.start("note.add");
        ledger.finish(slot, &Err("bad args".to_string()));
        assert_eq!(ledger.annotate("Error: bad args", true), "Error: bad args");
    }

    #[test]
    fn signal_killed_command_has_no_exit_code() {
        let ledger = CallLedger::default();
        let slot = ledger.start("host.exec");
        ledger.finish(slot, &Ok(json!({ "exitCode": null, "stdout": "" })));
        let text = ledger.annotate("Error: later", true);
        assert!(
            text.contains("succeeded (no exit code: killed by a signal)"),
            "{text}"
        );
    }

    #[test]
    fn recognizes_every_corruption_rendering() {
        assert!(is_corruption("(code: 11) database disk image is malformed"));
        assert!(is_corruption("(code: 26) file is not a database"));
        assert!(!is_corruption("(code: 5) database is locked"));
    }
}
