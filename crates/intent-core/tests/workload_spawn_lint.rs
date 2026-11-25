//! Source lint: every non-test file that starts a process is classified as
//! a *workload* (user, agent or model code, which must run in the memory box,
//! intent-hq/intent#9809) or *internal* (the daemon's own helpers: git,
//! probes, installers), so a new spawn site cannot skip containment
//! unnoticed.
//!
//! A spawn site is a `Command::new(` (std or tokio), a
//! `CommandBuilder::new(` or a PTY `SpawnSpec::new(` outside test code
//! (whole test files, `#[cfg(test)]` items, `test_support`, and the
//! `intentd-test-support` crate are exempt). [`SITES`] is the golden list.
//! In a workload file every spawn site must be matched by a containment
//! helper call (`Workload::new(`, `host_exec_workload(` or `pty_launch(`),
//! counted per file; a spawn there that is the daemon's own helper carries
//! `// workload-spawn: allow — <reason>` on the line above. The lint fails on
//! an unlisted spawning file, a listed file that no longer spawns, a
//! workload file with more unmarked spawns than helper calls, and a
//! malformed marker.
//!
//! Limit: the count is per file, not per call site, and a spawn through a
//! builder in another file (e.g. `host_exec::build_command`) is seen only
//! where the builder lives.

use intentd_test_support::source_lint::{
    markers_by_line, test_code_files, tokenize, workspace_root, Marker, TestCodeText,
};

enum Class {
    /// Runs user, agent or model code: must use the containment helper.
    Workload,
    /// The daemon's own helper process; the reason.
    Internal(&'static str),
}

use Class::{Internal, Workload};

const GIT: Class = Internal("git plumbing");
const PROBE: Class = Internal("a short CLI version or capability probe");
const INSTALL: Class = Internal("installs or updates a CLI");

/// The golden list, by workspace-relative path.
const SITES: &[(&str, Class)] = &[
    (
        "crates/intent-acp/src/descendant_sweep.rs",
        Internal("ps listing for the descendant sweep"),
    ),
    ("crates/intent-acp/src/spawn.rs", Workload),
    (
        "crates/intent-context/src/auggie.rs",
        Internal("the daemon's context engine"),
    ),
    (
        "crates/intent-core/src/path_utils.rs",
        Internal("login-shell environment capture"),
    ),
    (
        "crates/intent-core/src/workload/cgroup.rs",
        Internal("systemctl delegation query"),
    ),
    (
        "crates/intent-core/src/workload/leaf.rs",
        Internal("the cgroup-exec shim execs a workload that already joined its leaf"),
    ),
    ("crates/intent-git/src/auth.rs", GIT),
    ("crates/intent-git/src/cow_checkout.rs", GIT),
    ("crates/intent-git/src/fetch.rs", GIT),
    ("crates/intent-git/src/ls_remote.rs", GIT),
    ("crates/intent-git/src/node_git.rs", GIT),
    ("crates/intent-git/src/repo_cache.rs", GIT),
    ("crates/intent-git/src/stage.rs", GIT),
    ("crates/intent-git/src/submodule.rs", GIT),
    (
        "crates/intent-node-link/src/dial.rs",
        Internal("the node link dialer"),
    ),
    ("crates/intent-pty/src/host.rs", Workload),
    ("crates/intent-services/src/acp_adapter.rs", Workload),
    ("crates/intent-services/src/agy_cli.rs", INSTALL),
    ("crates/intent-services/src/auggie_cli.rs", PROBE),
    ("crates/intent-services/src/bundled_cli.rs", INSTALL),
    ("crates/intent-services/src/clone_ops.rs", GIT),
    (
        "crates/intent-services/src/codex_diagnostics/catalog.rs",
        PROBE,
    ),
    ("crates/intent-services/src/codex_diagnostics/mod.rs", PROBE),
    (
        "crates/intent-services/src/codex_diagnostics/process/linux.rs",
        PROBE,
    ),
    (
        "crates/intent-services/src/codex_diagnostics/process/linux_fallback.rs",
        PROBE,
    ),
    ("crates/intent-services/src/cursor_cli.rs", PROBE),
    ("crates/intent-services/src/dsh_cli.rs", PROBE),
    ("crates/intent-services/src/enhance_ops.rs", Workload),
    ("crates/intent-services/src/host_exec.rs", Workload),
    ("crates/intent-services/src/installed_cli.rs", PROBE),
    ("crates/intent-services/src/lib.rs", GIT),
    ("crates/intent-services/src/mcp_servers.rs", Workload),
    ("crates/intent-services/src/npx_cli.rs", PROBE),
    ("crates/intent-services/src/pi_cli.rs", PROBE),
    ("crates/intent-services/src/plugins/ops.rs", GIT),
    (
        "crates/intent-services/src/provider_auth.rs",
        Internal("a provider sign-in or status CLI"),
    ),
    ("crates/intent-services/src/provider_cli.rs", PROBE),
    ("crates/intent-services/src/provider_cli_launch.rs", PROBE),
    ("crates/intent-services/src/provider_version.rs", PROBE),
    (
        "crates/intent-services/src/repository_context_reader.rs",
        GIT,
    ),
    ("crates/intent-services/src/rtk.rs", PROBE),
    ("crates/intent-services/src/session_import.rs", GIT),
    ("crates/intent-services/src/unsloth_server.rs", Workload),
    (
        "crates/intent-sourcecontrol/src/gh_sync.rs",
        Internal("the gh CLI"),
    ),
    (
        "crates/intent-sourcecontrol/src/token.rs",
        Internal("the gh CLI"),
    ),
    ("crates/intent-svc-runtime/src/archive_runner.rs", Workload),
    ("crates/intent-svc-runtime/src/runtime.rs", Workload),
    ("crates/intent-svc-runtime/src/script_ops.rs", Workload),
    ("crates/intent-svc-runtime/src/terminal_ops.rs", Workload),
    ("crates/intent-svc-transfer/src/transfer.rs", GIT),
    ("crates/intent-svc-transfer/src/transfer_git.rs", GIT),
    (
        "crates/intent-svc-transfer/src/transfer_materialize.rs",
        GIT,
    ),
    ("crates/intent-svc-transfer/src/transfer_submodules.rs", GIT),
    (
        "crates/intent-transport/src/host.rs",
        Internal("opens a URL or file for the desktop"),
    ),
    ("crates/intent-transport/src/host_env.rs", PROBE),
    (
        "crates/intent-transport/src/host_ops.rs",
        Internal("opens an editor or file manager"),
    ),
    (
        "crates/intentd-sitter/src/readiness.rs",
        Internal("the sitter's daemon probe"),
    ),
    (
        "crates/intentd-sitter/src/startup.rs",
        Internal("the sitter starting the daemon"),
    ),
    (
        "crates/intentd-sitter/src/supervisor.rs",
        Internal("the sitter starting the daemon"),
    ),
    (
        "crates/intentd/src/command_evidence.rs",
        Internal("an evidence run, already inside its caller's box"),
    ),
    ("crates/intentd/src/executor.rs", GIT),
    ("crates/intentd/src/executor/checkout.rs", GIT),
    ("crates/intentd/src/executor/scripts.rs", Workload),
    ("crates/intentd/src/executor/setup.rs", Workload),
    ("crates/intentd/src/executor/terminals.rs", Workload),
    ("crates/intentd/src/executor/workspace_api.rs", GIT),
    (
        "crates/intentd/src/plugin_exec.rs",
        Internal("a plugin helper, already inside its caller's box"),
    ),
    (
        "crates/intentd/src/tunnel.rs",
        Internal("the tailcat tunnel sidecar"),
    ),
];

const SPAWNS: &[&str] = &["Command", "CommandBuilder", "SpawnSpec"];
const HELPERS: &[&str] = &["Workload", "host_exec_workload", "pty_launch"];

struct Found {
    rel: String,
    unmarked: Vec<usize>,
    helpers: usize,
    problems: Vec<String>,
}

fn scan(rel: &str, src: &str, whole: bool) -> Option<Found> {
    if whole {
        return None;
    }
    let text = TestCodeText::new(src, false);
    let tokens = tokenize(&text.chars);
    let markers = markers_by_line(src, &text.lexed.line_comments, "workload-spawn");
    let mut found = Found {
        rel: rel.to_owned(),
        unmarked: Vec::new(),
        helpers: 0,
        problems: Vec::new(),
    };
    let mut spawns = 0;
    for window in tokens.windows(4) {
        let [ty, sep, new, open] = window else {
            continue;
        };
        if text.in_test(ty.start) || !open.is("(") {
            continue;
        }
        if sep.is("::") && new.is("new") && SPAWNS.iter().any(|s| ty.is(s)) {
            spawns += 1;
            match markers.get(ty.line - 1).copied().unwrap_or(Marker::Absent) {
                Marker::WithReason => {}
                Marker::Malformed => found.problems.push(format!(
                    "{rel}:{}: malformed marker; write `// workload-spawn: allow — <reason>`",
                    ty.line - 1
                )),
                Marker::Absent => found.unmarked.push(ty.line),
            }
        }
        if ty.is("Workload") && sep.is("::") && new.is("new") {
            found.helpers += 1;
        }
    }
    for window in tokens.windows(2) {
        let [name, open] = window else { continue };
        if open.is("(") && !text.in_test(name.start) && HELPERS[1..].iter().any(|h| name.is(h)) {
            found.helpers += 1;
        }
    }
    (spawns > 0).then_some(found)
}

fn exempt(rel: &str) -> bool {
    rel.starts_with("crates/intentd-test-support/")
        || rel.contains("/test_support/")
        || rel.ends_with("/test_support.rs")
        || rel.contains("/tests/")
}

#[test]
fn every_spawn_site_is_classified_and_workloads_use_the_helper() {
    let root = workspace_root();
    let found: Vec<Found> = test_code_files(&root)
        .into_iter()
        .filter(|f| !exempt(&f.rel))
        .filter_map(|f| scan(&f.rel, &f.src, f.whole))
        .collect();
    let mut problems = Vec::new();
    for f in &found {
        problems.extend(f.problems.iter().cloned());
        match SITES.iter().find(|(rel, _)| *rel == f.rel) {
            None => problems.push(format!(
                "{}:{} starts a process and is not in SITES: classify it as a workload \
                 (user, agent or model code; spawn it through intent_core::workload::Workload) \
                 or internal",
                f.rel,
                f.unmarked.first().copied().unwrap_or(1)
            )),
            Some((_, Workload)) if f.unmarked.len() > f.helpers => problems.push(format!(
                "{}: {} spawn site(s) (lines {:?}) but {} containment helper call(s): \
                 apply intent_core::workload::Workload to each workload spawn, or mark the \
                 daemon's own helper with `// workload-spawn: allow — <reason>`",
                f.rel,
                f.unmarked.len(),
                f.unmarked,
                f.helpers
            )),
            Some((_, Internal(why))) => assert!(!why.trim().is_empty(), "{}", f.rel),
            Some(_) => {}
        }
    }
    for (rel, _) in SITES {
        if !found.iter().any(|f| f.rel == *rel) {
            problems.push(format!(
                "{rel} is in SITES but starts no process: remove it"
            ));
        }
    }
    assert!(problems.is_empty(), "{}", problems.join("\n"));
}
