//! Source lint: every file in `crates/intent-services/src/` that starts a
//! process is classified, so a new provider spawn site cannot bypass the
//! orchestrator gate unnoticed (monorepo `docs/protocol/methods/nodes.md`,
//! "Orchestrator mode" → "One gate").
//!
//! An orchestrator never starts a provider process. Every site that does
//! asks `Services::may_spawn_provider_locally()`, at the site or at the
//! one entry point that reaches it. [`SITES`] is the golden list: each
//! non-test file that calls `Command::new(` is either a provider site,
//! with the gates that cover it, or another process (git, a user's shell,
//! an MCP server …) with why it is not a provider run. The lint fails on a
//! file that spawns and is not listed, on a listed file that no longer
//! spawns (stale), and on a gate that does not hold: a named function that
//! no longer asks the gate, or a transport orchestrator branch that is gone.
//! Test code is exempt: whole test files and `#[cfg(test)]` items as
//! `source_lint::test_code_files` classifies them, plus `test_support`.
//! Limit: a spawn written through an alias or a helper in another crate is
//! not seen.

use intentd_test_support::source_lint::{test_code_files, tokenize, workspace_root, TestCodeText};

const SRC_PREFIX: &str = "crates/intent-services/src/";

/// Where a provider spawn is gated: a function (`file`, `fn` name, path
/// from the workspace root) whose body must ask
/// `may_spawn_provider_locally()`, or a fixed text (an orchestrator branch
/// in the transport) that must be in `file`.
enum Gate {
    Fn(&'static str, &'static str),
    Text(&'static str, &'static str),
}

/// What a listed file's spawns are.
enum Spawn {
    /// A provider process (adapter, CLI probe, model or sign-in probe),
    /// reached only through these gates.
    Provider(&'static [Gate]),
    /// Not a provider run; the reason.
    Other(&'static str),
}

const HOST: &str = "crates/intent-transport/src/host.rs";

const MODELS: Gate = Gate::Fn("agent_ops.rs", "models_list_op");
const COMPLETE: Gate = Gate::Fn("complete_ops.rs", "agent_complete_once_op");
const MACHINE: Gate = Gate::Fn("complete_ops/remote.rs", "machine_one_shot");
const ENHANCE: Gate = Gate::Fn("enhance_ops.rs", "agent_enhance_prompt_op");
const SCRATCH: Gate = Gate::Fn("agent_manager/scratch_adapter.rs", "open_scratch_adapter");
const SPAWN: Gate = Gate::Fn("agent_manager.rs", "create_agent");
const PREPARE: Gate = Gate::Fn("lib.rs", "prepare_provider_adapters");
const REPORT: Gate = Gate::Fn("executor_capabilities.rs", "executor_provider_capabilities");
const STATUSES: Gate = Gate::Fn(
    "executor_capabilities.rs",
    "executor_provider_launch_statuses",
);
const PROFILE_AUTH: Gate = Gate::Fn("provider_profiles.rs", "provider_profiles_auth_status_impl");
const DISCOVERY: Gate = Gate::Text(
    HOST,
    "HostMethod::ProviderDiscovery\n            if api.daemon_mode() == intent_core::settings_file::DaemonMode::Orchestrator",
);
const AUTH: Gate = Gate::Text(
    HOST,
    "if api.daemon_mode() == intent_core::settings_file::DaemonMode::Orchestrator {\n                // Configured, never probed",
);

/// The golden list, by path under `crates/intent-services/src/`.
const SITES: &[(&str, Spawn)] = &[
    (
        "acp_adapter.rs",
        Spawn::Provider(&[COMPLETE, MACHINE, SCRATCH, MODELS]),
    ),
    ("agent_ops.rs", Spawn::Provider(&[MODELS])),
    (
        "agy_cli.rs",
        Spawn::Provider(&[DISCOVERY, REPORT, STATUSES]),
    ),
    (
        "auggie_cli.rs",
        Spawn::Provider(&[DISCOVERY, REPORT, STATUSES]),
    ),
    ("bundled_cli.rs", Spawn::Provider(&[PREPARE])),
    ("clone_ops.rs", Spawn::Other("git clone of a repository")),
    ("codex_diagnostics/catalog.rs", Spawn::Provider(&[MODELS])),
    ("codex_diagnostics/mod.rs", Spawn::Provider(&[MODELS])),
    (
        "codex_diagnostics/process/linux.rs",
        Spawn::Provider(&[MODELS]),
    ),
    (
        "cursor_cli.rs",
        Spawn::Provider(&[DISCOVERY, REPORT, STATUSES]),
    ),
    ("enhance_ops.rs", Spawn::Provider(&[ENHANCE, MACHINE])),
    (
        "host_exec.rs",
        Spawn::Other("the user's own command (host.exec)"),
    ),
    (
        "installed_cli.rs",
        Spawn::Provider(&[DISCOVERY, REPORT, STATUSES]),
    ),
    ("lib.rs", Spawn::Other("git reads of a workspace")),
    (
        "mcp_servers.rs",
        Spawn::Other("a user's stdio MCP server, not a provider run"),
    ),
    (
        "npx_cli.rs",
        Spawn::Provider(&[DISCOVERY, REPORT, STATUSES]),
    ),
    ("pi_cli.rs", Spawn::Provider(&[DISCOVERY, REPORT, STATUSES])),
    (
        "plugins/ops.rs",
        Spawn::Other("git fetch of an agent plugin"),
    ),
    ("provider_auth.rs", Spawn::Provider(&[AUTH, PROFILE_AUTH])),
    ("provider_cli.rs", Spawn::Provider(&[DISCOVERY])),
    ("provider_models/mod.rs", Spawn::Provider(&[MODELS])),
    (
        "provider_version.rs",
        Spawn::Provider(&[DISCOVERY, REPORT, STATUSES]),
    ),
    (
        "repository_context_reader.rs",
        Spawn::Other("git reads of a repository"),
    ),
    ("rtk.rs", Spawn::Other("the rtk output compressor")),
    (
        "session_import.rs",
        Spawn::Other("git rev-parse of a listed session's folder"),
    ),
    ("unsloth_server.rs", Spawn::Provider(&[SPAWN])),
];

/// The body of `fn name` in `src`: from its signature to its matching
/// closing brace. Braces in strings are rare enough here to ignore.
fn fn_body<'a>(src: &'a str, name: &str) -> Option<&'a str> {
    let start = src.find(&format!("fn {name}("))?;
    let open = start + src[start..].find('{')?;
    let mut depth = 0usize;
    for (offset, c) in src[open..].char_indices() {
        match c {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    return Some(&src[start..=open + offset]);
                }
            }
            _ => {}
        }
    }
    None
}

/// Whether `gate` holds in the tree at `root`; `Err` says why not.
fn check_gate(root: &std::path::Path, gate: &Gate) -> Result<(), String> {
    let read =
        |rel: &str| std::fs::read_to_string(root.join(rel)).map_err(|e| format!("read {rel}: {e}"));
    match gate {
        Gate::Fn(file, name) => {
            let rel = format!("{SRC_PREFIX}{file}");
            let src = read(&rel)?;
            let body = fn_body(&src, name).ok_or_else(|| format!("{rel}: no fn {name}"))?;
            if body.contains("may_spawn_provider_locally()") {
                Ok(())
            } else {
                Err(format!(
                    "{rel}: fn {name} no longer asks may_spawn_provider_locally()"
                ))
            }
        }
        Gate::Text(file, text) => {
            if read(file)?.contains(text) {
                Ok(())
            } else {
                Err(format!("{file}: the orchestrator branch `{text}` is gone"))
            }
        }
    }
}

/// The line of every non-test `Command::new(` in `src`.
fn spawns_in(src: &str, whole: bool) -> Vec<usize> {
    if whole {
        return Vec::new();
    }
    let text = TestCodeText::new(src, false);
    let tokens = tokenize(&text.chars);
    tokens
        .windows(4)
        .filter_map(|window| {
            let [ty, sep, new, open] = window else {
                return None;
            };
            (ty.is("Command")
                && sep.is("::")
                && new.is("new")
                && open.is("(")
                && !text.in_test(ty.start))
            .then_some(ty.line)
        })
        .collect()
}

fn is_test_support(inner: &str) -> bool {
    inner == "test_support.rs" || inner.starts_with("test_support/")
}

#[test]
fn every_process_spawn_site_is_classified() {
    let root = workspace_root();
    let mut found: Vec<(String, Vec<usize>)> = Vec::new();
    for file in test_code_files(&root) {
        let Some(inner) = file.rel.strip_prefix(SRC_PREFIX) else {
            continue;
        };
        if is_test_support(inner) {
            continue;
        }
        let lines = spawns_in(&file.src, file.whole);
        if !lines.is_empty() {
            found.push((inner.to_owned(), lines));
        }
    }
    let mut problems = Vec::new();
    for (inner, lines) in &found {
        if !SITES.iter().any(|(listed, _)| listed == inner) {
            problems.push(format!(
                "{SRC_PREFIX}{inner}:{} starts a process and is not in SITES: \
                 classify it (a provider spawn must pass may_spawn_provider_locally)",
                lines[0]
            ));
        }
    }
    for (listed, spawn) in SITES {
        if !found.iter().any(|(inner, _)| inner == listed) {
            problems.push(format!(
                "{SRC_PREFIX}{listed} is in SITES but starts no process: remove it"
            ));
        }
        if let Spawn::Other(why) = spawn {
            assert!(!why.trim().is_empty(), "{listed}: an empty classification");
        }
    }
    assert!(problems.is_empty(), "{}", problems.join("\n"));
}

/// Every gate a provider row names holds: the function asks
/// `may_spawn_provider_locally()`, or the transport's orchestrator branch is
/// there.
#[test]
fn every_named_gate_holds() {
    let root = workspace_root();
    let mut problems = Vec::new();
    for (listed, spawn) in SITES {
        if let Spawn::Provider(gates) = spawn {
            assert!(!gates.is_empty(), "{listed}: a provider row names no gate");
            for gate in *gates {
                if let Err(why) = check_gate(&root, gate) {
                    problems.push(format!("{listed}: {why}"));
                }
            }
        }
    }
    assert!(problems.is_empty(), "{}", problems.join("\n"));
}

#[test]
fn fn_body_ends_at_the_matching_brace() {
    let src = "fn a(x: u8) {\n    if x { b() }\n}\nfn c() {}\n";
    assert_eq!(
        fn_body(src, "a"),
        Some("fn a(x: u8) {\n    if x { b() }\n}")
    );
    assert_eq!(fn_body(src, "z"), None);
}

#[test]
fn detector_sees_spawns_outside_test_code_only() {
    let src = "fn a() {\n    let c = Command::new(\"git\");\n}\n#[cfg(test)]\nmod tests {\n    fn b() { Command::new(\"x\"); }\n}\n";
    assert_eq!(spawns_in(src, false), vec![2]);
    assert!(spawns_in(src, true).is_empty());
    let qualified = "let c = tokio::process::Command::new(bin);\n// Command::new(x)\n";
    assert_eq!(spawns_in(qualified, false), vec![1]);
}
