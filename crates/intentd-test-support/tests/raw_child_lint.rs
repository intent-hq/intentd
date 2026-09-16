//! Raw-`Child` source lint.
//!
//! Every e2e suite that owns a daemon, sitter, or fake sidecar as a bare
//! `std::process::Child` leaks it — and everything it spawned — when the test
//! panics before its teardown (intent-hq/intentd#1924: a reviewer caught a
//! bare `Child` return in the sitter e2e by hand). `intentd_test_support::
//! GuardedChild` kills the process group on drop; this source-scanning test
//! fails, naming `file:line`, wherever a test file under
//! `crates/*/tests/**/*.rs` outside the support crate names `Child` as a type,
//! so a new suite cannot quietly return a bare `Child`. Files that have not
//! been migrated yet are listed in [`BASELINE`], which can only shrink.
//!
//! The heuristic is deliberately small:
//!
//! - Comments and string / char literals are blanked first, so a `Child` in a
//!   doc comment or a fixture string never counts.
//! - A hit is the whole identifier token `Child` — optionally written as the
//!   path `process::Child`, `std::process::Child`, or `::std::process::Child`
//!   — in a type position: immediately after `->` (return type), after a
//!   single `:` (field, binding, or parameter type), as a generic argument
//!   (after `<`, or after a `,` whose enclosing bracket is `<`:
//!   `Option<Child>`, `Result<E, Child>`), as a tuple element (after `(`,
//!   or after a `,` whose enclosing bracket is `(`: `-> (Child, u16)`,
//!   `struct LiveProcess(Child)`, `Vec<(u16, Child)>`), or as an array /
//!   slice element (after `[`: `[Child; 1]`, `Box<[Child]>`). A visibility
//!   qualifier between the marker and the type — `pub`, `pub(crate)`,
//!   `pub(super)`, `pub(self)`, `pub(in path)` — is skipped, so tuple-struct
//!   fields such as `struct P(pub Child)` and `(u16, pub(crate) Child)` are
//!   hits too. `std::process::Child` has no public constructor, so it never
//!   appears as a value inside a call's parentheses — a bare `Child` there is
//!   always a type.
//!   `GuardedChild` and other identifiers merely ending in `Child` are not
//!   hits, nor is any other path such as `Kind::Child` or `portable_pty::Child`.
//!   Borrows (`&Child`, `&mut Child`) are not hits: the token is preceded by
//!   `&` / `mut`, not by a type-position marker. `use std::process::Child;`
//!   is not a hit (the path is preceded by `use`, not a type position).
//! - Exempt: everything under `crates/intentd-test-support/` (the crate that
//!   wraps `Child`). Only `crates/*/tests/**/*.rs` is scanned, so `src/` code
//!   is never considered.
//! - Opt-out: `// raw-child: allow — <reason>` on the line immediately above
//!   the hit's line. The marker counts only as a standalone `//` line comment
//!   (nothing but whitespace before it, not inside a `/* … */` block comment
//!   or a string literal, not trailing code), the token must be exactly
//!   `raw-child: allow` (a longer word such as `allowance` is malformed), and
//!   it must be followed by whitespace, an em dash or hyphen, and a nonempty
//!   reason. A malformed marker never suppresses the hit; the report says so.
//! - Baseline ratchet: a file in [`BASELINE`] may have hits today. An entry
//!   whose file has no hits any more (or no longer exists) fails the lint
//!   with a "remove from BASELINE" message, so the list only ever shrinks.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Component, Path, PathBuf};

/// Test files (relative to the workspace root, `/`-separated) that still own
/// a bare `std::process::Child`. Migrate a file onto
/// `intentd_test_support::GuardedChild`, then delete its entry here; the
/// lint fails while an entry has no hits, so the list can only shrink.
const BASELINE: &[&str] = &[
    "crates/intentd-sitter/tests/install_ps1_owner.rs",
    "crates/intentd-sitter/tests/install_sh_startup.rs",
    "crates/intentd-sitter/tests/sitter_update_e2e.rs",
    "crates/intentd/tests/common/mod.rs",
    "crates/intentd/tests/e2e_agent_features_gating.rs",
    "crates/intentd/tests/e2e_config_precedence.rs",
    "crates/intentd/tests/e2e_core_cli_commands.rs",
    "crates/intentd/tests/e2e_core_hermetic_workspaces_guard.rs",
    "crates/intentd/tests/e2e_core_orphan_trash_sweep.rs",
    "crates/intentd/tests/e2e_hook_lifecycle.rs",
    "crates/intentd/tests/e2e_startup_ordering.rs",
    "crates/intentd/tests/e2e_transport.rs",
    "crates/intentd/tests/e2e_wss_agent_config_option.rs",
    "crates/intentd/tests/e2e_wss_agent_create_name_explicitly_set.rs",
    "crates/intentd/tests/e2e_wss_agent_cross_provider_history.rs",
    "crates/intentd/tests/e2e_wss_agent_idle_timeout.rs",
    "crates/intentd/tests/e2e_wss_agent_lifecycle.rs",
    "crates/intentd/tests/e2e_wss_agent_list_active.rs",
    "crates/intentd/tests/e2e_wss_agent_midturn_failure.rs",
    "crates/intentd/tests/e2e_wss_agent_quota_failure.rs",
    "crates/intentd/tests/e2e_wss_agent_rehydration.rs",
    "crates/intentd/tests/e2e_wss_agent_set_model.rs",
    "crates/intentd/tests/e2e_wss_agent_spawn_retry.rs",
    "crates/intentd/tests/e2e_wss_agent_state_snapshot.rs",
    "crates/intentd/tests/e2e_wss_agent_turn_correlation.rs",
    "crates/intentd/tests/e2e_wss_agent_watch.rs",
    "crates/intentd/tests/e2e_wss_archive_from_hook.rs",
    "crates/intentd/tests/e2e_wss_archive_interrupt.rs",
    "crates/intentd/tests/e2e_wss_archive_parks_wakes.rs",
    "crates/intentd/tests/e2e_wss_archive_self.rs",
    "crates/intentd/tests/e2e_wss_auto_commit_llm.rs",
    "crates/intentd/tests/e2e_wss_auto_unarchive.rs",
    "crates/intentd/tests/e2e_wss_browser_client_pin.rs",
    "crates/intentd/tests/e2e_wss_browser_exec.rs",
    "crates/intentd/tests/e2e_wss_browser_tabs.rs",
    "crates/intentd/tests/e2e_wss_change_events.rs",
    "crates/intentd/tests/e2e_wss_chat_tool_result_ids.rs",
    "crates/intentd/tests/e2e_wss_chief_workspace.rs",
    "crates/intentd/tests/e2e_wss_codex_session_title.rs",
    "crates/intentd/tests/e2e_wss_delegate_batch.rs",
    "crates/intentd/tests/e2e_wss_delegate_provider_resolution.rs",
    "crates/intentd/tests/e2e_wss_delegation_group_persist.rs",
    "crates/intentd/tests/e2e_wss_display_status_hooks.rs",
    "crates/intentd/tests/e2e_wss_display_status_needs_attention.rs",
    "crates/intentd/tests/e2e_wss_flush_queued_messages.rs",
    "crates/intentd/tests/e2e_wss_git_clone.rs",
    "crates/intentd/tests/e2e_wss_git_discard.rs",
    "crates/intentd/tests/e2e_wss_git_reads.rs",
    "crates/intentd/tests/e2e_wss_git_writes.rs",
    "crates/intentd/tests/e2e_wss_github_device_flow.rs",
    "crates/intentd/tests/e2e_wss_gitignore_suppression.rs",
    "crates/intentd/tests/e2e_wss_harness_wake.rs",
    "crates/intentd/tests/e2e_wss_hook_mcp_tools.rs",
    "crates/intentd/tests/e2e_wss_host.rs",
    "crates/intentd/tests/e2e_wss_host_credential_env.rs",
    "crates/intentd/tests/e2e_wss_last_activity.rs",
    "crates/intentd/tests/e2e_wss_legacy_import.rs",
    "crates/intentd/tests/e2e_wss_mcp_oauth_refresh.rs",
    "crates/intentd/tests/e2e_wss_mcp_servers.rs",
    "crates/intentd/tests/e2e_wss_pending_questions.rs",
    "crates/intentd/tests/e2e_wss_poisoned_session_recreate.rs",
    "crates/intentd/tests/e2e_wss_prompt_error_transcript.rs",
    "crates/intentd/tests/e2e_wss_question_ask.rs",
    "crates/intentd/tests/e2e_wss_repo_config.rs",
    "crates/intentd/tests/e2e_wss_report_debounce.rs",
    "crates/intentd/tests/e2e_wss_restart_tail_recap.rs",
    "crates/intentd/tests/e2e_wss_resume_queue_order.rs",
    "crates/intentd/tests/e2e_wss_rtk.rs",
    "crates/intentd/tests/e2e_wss_runtime_control.rs",
    "crates/intentd/tests/e2e_wss_script_persistence.rs",
    "crates/intentd/tests/e2e_wss_script_restarting.rs",
    "crates/intentd/tests/e2e_wss_script_run_cancellation.rs",
    "crates/intentd/tests/e2e_wss_serve_resume_all.rs",
    "crates/intentd/tests/e2e_wss_server_pairing.rs",
    "crates/intentd/tests/e2e_wss_settings_atomic_rollback.rs",
    "crates/intentd/tests/e2e_wss_settings_live_reload.rs",
    "crates/intentd/tests/e2e_wss_setup_lifecycle.rs",
    "crates/intentd/tests/e2e_wss_setup_script.rs",
    "crates/intentd/tests/e2e_wss_specialist_frontmatter_model.rs",
    "crates/intentd/tests/e2e_wss_specialists_changed.rs",
    "crates/intentd/tests/e2e_wss_stall_annotation.rs",
    "crates/intentd/tests/e2e_wss_subscribe_snapshot_warn.rs",
    "crates/intentd/tests/e2e_wss_system_prompt_fallback.rs",
    "crates/intentd/tests/e2e_wss_tcp_origin_guard.rs",
    "crates/intentd/tests/e2e_wss_tilde_paths.rs",
    "crates/intentd/tests/e2e_wss_tool_payload_retention.rs",
    "crates/intentd/tests/e2e_wss_truncation_redrive.rs",
    "crates/intentd/tests/e2e_wss_unblocked_hints.rs",
    "crates/intentd/tests/e2e_wss_wake_or_create.rs",
    "crates/intentd/tests/e2e_wss_wake_resume.rs",
    "crates/intentd/tests/e2e_wss_workspace_activity_debounce.rs",
    "crates/intentd/tests/e2e_wss_workspace_cow.rs",
    "crates/intentd/tests/e2e_wss_workspace_create_submodule_progress.rs",
    "crates/intentd/tests/e2e_wss_workspace_direct_survival.rs",
    "crates/intentd/tests/e2e_wss_workspace_lifecycle_watchers.rs",
    "crates/intentd/tests/e2e_wss_workspace_owner_derivation.rs",
    "crates/intentd/tests/e2e_wss_workspace_worktree.rs",
    "crates/intentd/tests/uds_agent_runtime.rs",
    "crates/intentd/tests/uds_control.rs",
    "crates/intentd/tests/uds_git_credentials.rs",
    "crates/intentd/tests/uds_legacy_import.rs",
    "crates/intentd/tests/uds_rpc_profile_warn.rs",
];

const OPT_OUT_MARKER: &str = "// raw-child: allow";
const CHILD: &str = "Child";
/// Path prefixes (outermost first) under which the `Child` token counts.
const ALLOWED_PATHS: &[&[&str]] = &[&[], &["process"], &["std", "process"]];
/// The only prefix that may also be written with a leading `::`.
const ABSOLUTE_PATH: &[&str] = &["std", "process"];
const PUB: &str = "pub";
const EXEMPT_CRATE: &[&str] = &["crates", "intentd-test-support"];
const EXCERPT_CHARS: usize = 120;

#[derive(Debug, Clone, PartialEq, Eq)]
struct Hit {
    line: usize,
    excerpt: String,
    /// The line above carried something that starts like the opt-out marker
    /// but is malformed (longer token, or no reason).
    marker_malformed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Marker {
    Absent,
    WithReason,
    Malformed,
}

/// A real `//` line comment found by the lexer (never one nested inside a
/// block comment or a string literal).
struct LineComment {
    line: usize,
    /// Only whitespace precedes the `//` on its line.
    standalone: bool,
    text: String,
}

/// Source text with comments/literals blanked, plus the line comments the
/// lexer passed over on the way.
struct Stripped {
    text: String,
    line_comments: Vec<LineComment>,
}

/// Marker state of one line comment's text: `Absent` unless it starts with
/// the marker prefix; `WithReason` only when the token is exactly the marker
/// (not a longer word such as `allowance`) followed by whitespace, a dash,
/// and a nonempty reason; anything else that starts like the marker is
/// `Malformed`.
fn classify_marker(comment: &str) -> Marker {
    let Some(rest) = comment.strip_prefix(OPT_OUT_MARKER) else {
        return Marker::Absent;
    };
    if rest
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        return Marker::Malformed;
    }
    let after_space = rest.trim_start();
    if after_space.len() == rest.len() && !rest.is_empty() {
        return Marker::Malformed;
    }
    let Some(reason) = after_space
        .strip_prefix('—')
        .or_else(|| after_space.strip_prefix('-'))
    else {
        return Marker::Malformed;
    };
    if reason.trim().is_empty() {
        Marker::Malformed
    } else {
        Marker::WithReason
    }
}

/// Opt-out marker state per line; index 0 is a placeholder so the vector is
/// addressed by 1-based line number. Only a standalone `//` line comment can
/// carry the marker.
fn markers_by_line(src: &str, line_comments: &[LineComment]) -> Vec<Marker> {
    let mut out = vec![Marker::Absent; src.lines().count() + 1];
    for comment in line_comments.iter().filter(|c| c.standalone) {
        if let Some(slot) = out.get_mut(comment.line) {
            *slot = classify_marker(&comment.text);
        }
    }
    out
}

fn push_blank(out: &mut String, c: char) {
    out.push(if c == '\n' { '\n' } else { ' ' });
}

/// `Some(hashes)` when a raw string literal (`r"`, `r#"`, `br"`, `cr#"`, …)
/// starts at `i`; `None` otherwise. Cooked `b"…"` / `c"…"` strings need no
/// special case: their prefix letter is left as an inert identifier and the
/// `"` branch consumes the body.
fn raw_string_hashes(chars: &[char], i: usize) -> Option<usize> {
    let preceded_by_ident = i > 0 && (chars[i - 1].is_ascii_alphanumeric() || chars[i - 1] == '_');
    if preceded_by_ident {
        return None;
    }
    let mut j = i;
    if matches!(chars.get(j), Some('b' | 'c')) {
        j += 1;
    }
    if chars.get(j) != Some(&'r') {
        return None;
    }
    j += 1;
    let mut hashes = 0;
    while chars.get(j) == Some(&'#') {
        hashes += 1;
        j += 1;
    }
    (chars.get(j) == Some(&'"')).then_some(hashes)
}

/// Replaces every comment, string literal, and char literal with spaces
/// (newlines preserved) so neither their contents nor their delimiters take
/// part in token matching. Every `//` line comment the lexer consumes is also
/// reported, since only those may carry the opt-out marker.
fn blank_literals_and_comments(src: &str) -> Stripped {
    let chars: Vec<char> = src.chars().collect();
    let mut out = String::with_capacity(src.len());
    let mut line_comments = Vec::new();
    // Line bookkeeping is advanced lazily, only when a `//` comment is met,
    // so newlines swallowed by the block-comment and string loops still count.
    let mut line = 1usize;
    let mut line_start = 0usize;
    let mut counted_upto = 0usize;
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        let next = chars.get(i + 1).copied();
        if c == '/' && next == Some('/') {
            let start = i;
            for (offset, ch) in chars[counted_upto..start].iter().enumerate() {
                if *ch == '\n' {
                    line += 1;
                    line_start = counted_upto + offset + 1;
                }
            }
            counted_upto = start;
            while i < chars.len() && chars[i] != '\n' {
                out.push(' ');
                i += 1;
            }
            line_comments.push(LineComment {
                line,
                standalone: chars[line_start..start].iter().all(|c| c.is_whitespace()),
                text: chars[start..i].iter().collect(),
            });
        } else if c == '/' && next == Some('*') {
            let mut depth = 0usize;
            while i < chars.len() {
                if chars[i] == '/' && chars.get(i + 1) == Some(&'*') {
                    depth += 1;
                    out.push_str("  ");
                    i += 2;
                } else if chars[i] == '*' && chars.get(i + 1) == Some(&'/') {
                    depth -= 1;
                    out.push_str("  ");
                    i += 2;
                    if depth == 0 {
                        break;
                    }
                } else {
                    push_blank(&mut out, chars[i]);
                    i += 1;
                }
            }
        } else if let Some(hashes) = raw_string_hashes(&chars, i) {
            while chars[i] != '"' {
                out.push(' ');
                i += 1;
            }
            out.push(' ');
            i += 1;
            while i < chars.len() {
                let closing =
                    chars[i] == '"' && (1..=hashes).all(|k| chars.get(i + k) == Some(&'#'));
                push_blank(&mut out, chars[i]);
                i += 1;
                if closing {
                    out.push_str(&" ".repeat(hashes));
                    i += hashes;
                    break;
                }
            }
        } else if c == '"' {
            out.push(' ');
            i += 1;
            while i < chars.len() {
                let d = chars[i];
                push_blank(&mut out, d);
                i += 1;
                if d == '\\' {
                    if let Some(&escaped) = chars.get(i) {
                        push_blank(&mut out, escaped);
                        i += 1;
                    }
                } else if d == '"' {
                    break;
                }
            }
        } else if c == '\'' {
            // `'\…'` and `'x'` are char literals; anything else is a lifetime
            // or loop label, which never spells a type.
            if next == Some('\\') {
                let start = i;
                i += 2;
                if chars.get(i) == Some(&'u') {
                    while i < chars.len() && chars[i] != '}' {
                        i += 1;
                    }
                }
                i += 1;
                if chars.get(i) == Some(&'\'') {
                    i += 1;
                }
                i = i.min(chars.len());
                for &d in &chars[start..i] {
                    push_blank(&mut out, d);
                }
            } else if chars.get(i + 2) == Some(&'\'') {
                out.push_str("   ");
                i += 3;
            } else {
                out.push(' ');
                i += 1;
            }
        } else {
            out.push(c);
            i += 1;
        }
    }
    Stripped {
        text: out,
        line_comments,
    }
}

fn is_ident_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

/// Index of the last non-whitespace char strictly before `j`, if any.
fn last_non_ws_before(chars: &[char], j: usize) -> Option<usize> {
    (0..j).rev().find(|&k| !chars[k].is_whitespace())
}

/// Start index of the identifier ending just before `end` (exclusive), if
/// the char at `end - 1` is an identifier char.
fn ident_start_before(chars: &[char], end: usize) -> Option<usize> {
    let mut k = end;
    while k > 0 && is_ident_char(chars[k - 1]) {
        k -= 1;
    }
    (k < end).then_some(k)
}

/// The opening bracket enclosing position `k` (walking backwards over
/// balanced groups), if any. `->` and `=>` are not brackets.
fn enclosing_open_bracket(chars: &[char], k: usize) -> Option<char> {
    let mut depth = 0usize;
    for idx in (0..k).rev() {
        match chars[idx] {
            '>' if idx > 0 && matches!(chars[idx - 1], '-' | '=') => {}
            '>' | ')' | ']' | '}' => depth += 1,
            open @ ('<' | '(' | '[' | '{') => {
                if depth == 0 {
                    return Some(open);
                }
                depth -= 1;
            }
            _ => {}
        }
    }
    None
}

/// Start of the identifier ending at the last non-whitespace char before
/// `end`, if that char is an identifier char.
fn ident_before(chars: &[char], end: usize) -> Option<(usize, usize)> {
    let seg_end = last_non_ws_before(chars, end)?;
    let seg_start = ident_start_before(chars, seg_end + 1)?;
    Some((seg_start, seg_end))
}

/// Index of the `(` matching the `)` at `close`, if any.
fn matching_open_paren(chars: &[char], close: usize) -> Option<usize> {
    let mut depth = 0usize;
    for idx in (0..close).rev() {
        match chars[idx] {
            ')' => depth += 1,
            '(' if depth == 0 => return Some(idx),
            '(' => depth -= 1,
            _ => {}
        }
    }
    None
}

/// Start of a visibility qualifier — `pub` or `pub(...)` — ending just
/// before `end`, if there is one.
fn visibility_start_before(chars: &[char], end: usize) -> Option<usize> {
    let k = last_non_ws_before(chars, end)?;
    let ident_end = if chars[k] == ')' {
        matching_open_paren(chars, k)?
    } else {
        end
    };
    let (seg_start, seg_end) = ident_before(chars, ident_end)?;
    let word: String = chars[seg_start..=seg_end].iter().collect();
    (word == PUB).then_some(seg_start)
}

/// Whether the `Child` token starting at `start` sits in a type position
/// (see the module doc), once any `process::` / `std::process::` /
/// `::std::process::` path prefix and any visibility qualifier have been
/// walked back over. A path under any other prefix is never a hit.
fn is_type_position(chars: &[char], start: usize) -> bool {
    let mut path_start = start;
    let mut absolute = false;
    let mut segments: Vec<String> = Vec::new();
    while let Some(colon) = last_non_ws_before(chars, path_start) {
        if chars[colon] != ':' || colon == 0 || chars[colon - 1] != ':' {
            break;
        }
        // A segment sits flush against its `::`; a `::` with nothing (or
        // whitespace, as in `pub ::std::…`) before it is the crate root, and
        // only `::std::process` counts there.
        let Some(seg_start) = ident_start_before(chars, colon - 1) else {
            absolute = true;
            path_start = colon - 1;
            break;
        };
        segments.push(chars[seg_start..colon - 1].iter().collect());
        path_start = seg_start;
    }
    segments.reverse();
    let allowed: &[&[&str]] = if absolute {
        &[ABSOLUTE_PATH]
    } else {
        ALLOWED_PATHS
    };
    if !allowed.iter().any(|allowed| {
        allowed.len() == segments.len() && allowed.iter().zip(&segments).all(|(a, s)| *a == s)
    }) {
        return false;
    }
    let type_start = visibility_start_before(chars, path_start).unwrap_or(path_start);
    let Some(k) = last_non_ws_before(chars, type_start) else {
        return false;
    };
    match chars[k] {
        '>' => k > 0 && chars[k - 1] == '-',
        ':' => k == 0 || chars[k - 1] != ':',
        '<' | '(' | '[' => true,
        ',' => matches!(enclosing_open_bracket(chars, k), Some('<' | '(')),
        _ => false,
    }
}

fn excerpt(line: &str) -> String {
    let collapsed = line.split_whitespace().collect::<Vec<_>>().join(" ");
    let mut out: String = collapsed.chars().take(EXCERPT_CHARS).collect();
    if out.len() < collapsed.len() {
        out.push('…');
    }
    out
}

/// Scans one Rust source file's text and returns every `Child` type position
/// that is not suppressed by a reasoned opt-out marker.
fn scan_source(src: &str) -> Vec<Hit> {
    let stripped = blank_literals_and_comments(src);
    let markers = markers_by_line(src, &stripped.line_comments);
    let lines: Vec<&str> = src.lines().collect();
    let chars: Vec<char> = stripped.text.chars().collect();
    let mut hits = Vec::new();
    let mut line = 1usize;
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c == '\n' {
            line += 1;
            i += 1;
        } else if is_ident_char(c) {
            let start = i;
            while i < chars.len() && is_ident_char(chars[i]) {
                i += 1;
            }
            let word: String = chars[start..i].iter().collect();
            if word == CHILD && is_type_position(&chars, start) {
                let marker = markers.get(line - 1).copied().unwrap_or(Marker::Absent);
                if marker != Marker::WithReason {
                    hits.push(Hit {
                        line,
                        excerpt: excerpt(lines.get(line - 1).copied().unwrap_or_default()),
                        marker_malformed: marker == Marker::Malformed,
                    });
                }
            }
        } else {
            i += 1;
        }
    }
    hits
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("crates/intentd-test-support sits two levels under the workspace root")
        .to_path_buf()
}

fn is_exempt(rel: &Path) -> bool {
    let mut comps = rel.components();
    EXEMPT_CRATE
        .iter()
        .all(|want| matches!(comps.next(), Some(Component::Normal(c)) if c == *want))
}

/// Every `crates/*/tests/**/*.rs` file under `root`, sorted, as paths relative
/// to `root`. Only the `tests/` integration-test trees are scanned.
fn test_files(root: &Path) -> Vec<PathBuf> {
    fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
        let Ok(entries) = fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, out);
            } else if path.extension().is_some_and(|e| e == "rs") {
                out.push(path);
            }
        }
    }
    let mut out = Vec::new();
    let crates = root.join("crates");
    for entry in fs::read_dir(&crates).expect("crates/ dir").flatten() {
        let tests = entry.path().join("tests");
        if tests.is_dir() {
            walk(&tests, &mut out);
        }
    }
    let mut rel: Vec<PathBuf> = out
        .into_iter()
        .map(|p| p.strip_prefix(root).expect("under root").to_path_buf())
        .collect();
    rel.sort();
    rel
}

fn rel_key(rel: &Path) -> String {
    rel.components()
        .map(|c| c.as_os_str().to_string_lossy().into_owned())
        .collect::<Vec<_>>()
        .join("/")
}

#[test]
fn baseline_entries_are_unique_and_sorted() {
    for pair in BASELINE.windows(2) {
        assert!(
            pair[0] < pair[1],
            "BASELINE must be sorted and free of duplicates: {:?} is not before {:?}",
            pair[0],
            pair[1]
        );
    }
}

#[test]
fn no_raw_child_types_in_test_suites() {
    let root = workspace_root();
    let baseline: BTreeMap<&str, bool> = BASELINE.iter().map(|f| (*f, false)).collect();
    let mut baseline_seen = baseline;
    let mut failures: Vec<String> = Vec::new();

    for rel in test_files(&root) {
        if is_exempt(&rel) {
            continue;
        }
        let key = rel_key(&rel);
        let src = fs::read_to_string(root.join(&rel)).expect("read test file");
        let hits = scan_source(&src);
        if let Some(seen) = baseline_seen.get_mut(key.as_str()) {
            if hits.is_empty() {
                failures.push(format!(
                    "{key}: no raw `Child` type positions remain — remove it from BASELINE"
                ));
            }
            *seen = true;
            continue;
        }
        for hit in hits {
            let note = if hit.marker_malformed {
                " (the `raw-child: allow` marker above it is malformed: it must be exactly \
                 `// raw-child: allow — <reason>` with a nonempty reason)"
            } else {
                ""
            };
            failures.push(format!("{key}:{}: {}{note}", hit.line, hit.excerpt));
        }
    }
    for (file, seen) in &baseline_seen {
        if !seen {
            failures.push(format!(
                "{file}: listed in BASELINE but no such test file exists — remove it"
            ));
        }
    }

    assert!(
        failures.is_empty(),
        "\n\nRaw `std::process::Child` in test code leaks the process (and its \
         children) when the test panics before teardown. Hold it in \
         `intentd_test_support::GuardedChild` instead, or put \
         `// raw-child: allow — <reason>` on the line above.\n\n{}\n",
        failures.join("\n")
    );
}

#[cfg(test)]
mod heuristic {
    use super::{classify_marker, scan_source, Marker};

    fn hit_lines(src: &str) -> Vec<usize> {
        scan_source(src).into_iter().map(|h| h.line).collect()
    }

    #[test]
    fn return_field_binding_and_param_types_hit() {
        let src = "fn boot() -> Child { todo!() }\n\
                   struct S { child: Child }\n\
                   fn f(c: Child) { let x: Child = c; }\n\
                   fn g() -> std::process::Child { todo!() }\n\
                   fn h() -> process::Child { todo!() }\n";
        assert_eq!(hit_lines(src), vec![1, 2, 3, 3, 4, 5]);
    }

    #[test]
    fn generic_arguments_hit() {
        let src = "struct S { a: Option<Child>, b: Mutex<Option<Child>>, c: Result<E, Child> }\n";
        assert_eq!(hit_lines(src), vec![1, 1, 1]);
    }

    #[test]
    fn tuple_positions_hit() {
        let src = "fn f() -> (Child, u16) { todo!() }\n\
                   fn g() -> Vec<(u16, Child)> { todo!() }\n\
                   struct LiveProcess(Child);\n\
                   fn h() -> (u16, std::process::Child, Arc<C>) { todo!() }\n\
                   let x: (Child, u16) = todo!();\n\
                   fn k() -> (u16, &Child) { todo!() }\n";
        assert_eq!(hit_lines(src), vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn array_and_slice_elements_hit() {
        let src = "struct S { children: [Child; 1] }\n\
                   fn f() -> Box<[Child]> { todo!() }\n\
                   fn g(children: &mut [Child]) {}\n\
                   let x: [std::process::Child; 2] = todo!();\n\
                   fn h() -> Vec<[Option<Child>; 1]> { todo!() }\n\
                   let bytes: [u8; 4] = [0; 4];\n\
                   let pids = [child_pid, other_pid];\n\
                   let kinds = [Kind::Child, Kind::Parent];\n\
                   fn k() -> [GuardedChild; 1] { todo!() }\n";
        assert_eq!(hit_lines(src), vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn visibility_qualified_tuple_fields_hit() {
        let src = "struct P(pub Child);\n\
                   struct Q(pub(crate) Child);\n\
                   struct R(pub(super) Child, u16);\n\
                   struct S(u16, pub(self) Child);\n\
                   struct T(pub(in crate::a) u16, pub Child);\n\
                   struct U(pub std::process::Child);\n\
                   struct V(pub(crate) Option<Child>);\n\
                   struct W(pub &Child);\n\
                   struct X(pub GuardedChild);\n";
        assert_eq!(hit_lines(src), vec![1, 2, 3, 4, 5, 6, 7]);
    }

    #[test]
    fn absolute_std_path_hits() {
        let src = "fn a() -> ::std::process::Child { todo!() }\n\
                   struct S { child: ::std::process::Child }\n\
                   struct T(pub ::std::process::Child);\n\
                   fn b() -> Option<::std::process::Child> { todo!() }\n\
                   fn c() -> ::process::Child { todo!() }\n\
                   fn d() -> ::tokio::process::Child { todo!() }\n\
                   use ::std::process::Child;\n";
        assert_eq!(hit_lines(src), vec![1, 2, 3, 4]);
    }

    #[test]
    fn borrows_use_paths_and_other_identifiers_do_not_hit() {
        let src = "use std::process::{Child, Command};\n\
                   use std::process::Child;\n\
                   fn a(c: &Child) {}\n\
                   fn b(c: &mut Child) {}\n\
                   fn c() -> GuardedChild { todo!() }\n\
                   fn d() -> Option<GuardedChild> { todo!() }\n\
                   fn e() -> Kind::Child { todo!() }\n\
                   fn f() -> Box<dyn portable_pty::Child + Send> { todo!() }\n\
                   fn g() -> tokio::process::Child { todo!() }\n\
                   fn h() -> ChildStdout { todo!() }\n\
                   let x = Child::new();\n";
        assert_eq!(hit_lines(src), Vec::<usize>::new());
    }

    #[test]
    fn comments_and_literals_do_not_hit() {
        let src = "// fn boot() -> Child\n\
                   /* struct S { child: Child } */\n\
                   /// docs: -> Child\n\
                   const A: &str = \"-> Child\";\n\
                   const B: &str = r#\"x: Child\"#;\n\
                   const C: char = ':';\n\
                   fn f() -> Child { todo!() }\n";
        assert_eq!(hit_lines(src), vec![7]);
    }

    #[test]
    fn reasoned_marker_on_the_line_above_suppresses() {
        let src = "// raw-child: allow — the sitter reaps it itself\n\
                   fn a() -> Child { todo!() }\n\
                   // raw-child: allow - hyphen also fine\n\
                   fn b() -> Child { todo!() }\n\
                   // raw-child: allow — two lines above does not count\n\
                   \n\
                   fn c() -> Child { todo!() }\n";
        assert_eq!(hit_lines(src), vec![7]);
    }

    #[test]
    fn malformed_marker_does_not_suppress_and_is_reported() {
        let src = "// raw-child: allow\n\
                   fn a() -> Child { todo!() }\n\
                   // raw-child: allow —\n\
                   fn b() -> Child { todo!() }\n\
                   // raw-child: allowance — longer token\n\
                   fn c() -> Child { todo!() }\n\
                   let y = 1; // raw-child: allow — trailing, not standalone\n\
                   fn d() -> Child { todo!() }\n\
                   /* raw-child: allow — block comment */\n\
                   fn e() -> Child { todo!() }\n";
        let hits = scan_source(src);
        assert_eq!(
            hits.iter()
                .map(|h| (h.line, h.marker_malformed))
                .collect::<Vec<_>>(),
            vec![(2, true), (4, true), (6, true), (8, false), (10, false)]
        );
    }

    #[test]
    fn marker_classification() {
        assert_eq!(
            classify_marker("// raw-child: allow — why"),
            Marker::WithReason
        );
        assert_eq!(
            classify_marker("// raw-child: allow - why"),
            Marker::WithReason
        );
        assert_eq!(classify_marker("// raw-child: allow"), Marker::Malformed);
        assert_eq!(classify_marker("// raw-child: allow —"), Marker::Malformed);
        assert_eq!(
            classify_marker("// raw-child: allow why"),
            Marker::Malformed
        );
        assert_eq!(
            classify_marker("// raw-child: allowance — x"),
            Marker::Malformed
        );
        assert_eq!(classify_marker("// something else"), Marker::Absent);
    }
}
