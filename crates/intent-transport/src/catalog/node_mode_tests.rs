//! Node-mode method list (monorepo nodes.md "Node mode"): every
//! client-callable method is either on [`NODE_MODE_METHODS`] or frozen in the
//! refused golden `tests/goldens/node_mode_refused.txt`, so a new method
//! cannot reach a daemon in node mode without a deliberate opt-in.

use super::tests::client_callable_universe;
use super::{
    canonical_method, node_mode_may_call, node_mode_serves_path, METHOD_ALIASES, NODE_MODE_METHODS,
    REVERSE_METHODS,
};
use std::collections::{BTreeSet, HashSet};
use std::fmt::Write as _;

const REFUSED_GOLDEN_REL: &str = "tests/goldens/node_mode_refused.txt";
const REGENERATE: &str = "INTENTD_UPDATE_GOLDENS=1 cargo test -p intent-transport --lib catalog::node_mode_tests::node_mode_refused_remainder_matches_golden";

fn allowed() -> HashSet<&'static str> {
    NODE_MODE_METHODS.iter().map(|(m, _)| *m).collect()
}

fn render_refused_golden() -> String {
    let allowed = allowed();
    let mut out = String::from(
        "# Golden list of every client-callable method a daemon in node mode refuses\n\
         # (-32003, data.code \"node-mode\"), one per line. Generated from\n\
         # crates/intent-transport/src/catalog.rs NODE_MODE_METHODS; do not hand-edit.\n\
         # Moving a method onto the node-mode list is a deliberate opt-in. Regenerate:\n",
    );
    let _ = writeln!(out, "#   {REGENERATE}");
    for method in client_callable_universe() {
        if !allowed.contains(method.as_str()) {
            out.push_str(&method);
            out.push('\n');
        }
    }
    out
}

fn golden_entries(text: &str) -> BTreeSet<String> {
    text.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(str::to_string)
        .collect()
}

#[test]
fn node_mode_refused_remainder_matches_golden() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(REFUSED_GOLDEN_REL);
    let expected = render_refused_golden();
    if std::env::var("INTENTD_UPDATE_GOLDENS").is_ok_and(|v| v == "1") {
        std::fs::write(&path, &expected).expect("write node-mode golden");
        return;
    }
    let actual = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| {
            panic!(
                "cannot read {}: {e}. Generate it with:\n  {REGENERATE}",
                path.display()
            )
        })
        .replace("\r\n", "\n");
    if actual == expected {
        return;
    }
    let want = golden_entries(&expected);
    let have = golden_entries(&actual);
    let mut msg = String::from(
        "Node-mode list drift: a client-callable method is neither on NODE_MODE_METHODS nor in the refused golden (or a golden entry is stale).\n\
         A new method is refused in node mode by default; keep it refused by regenerating the golden, or allow it with a one-line NODE_MODE_METHODS entry and mirror docs/protocol/methods/nodes.md.\n",
    );
    for m in want.difference(&have) {
        let _ = writeln!(msg, "  refused but not in the golden: {m}");
    }
    for m in have.difference(&want) {
        let _ = writeln!(msg, "  in the golden but no longer refused/cataloged: {m}");
    }
    if want == have {
        msg.push_str("  (same entries; the golden is not in canonical form)\n");
    }
    let _ = write!(msg, "Regenerate with:\n  {REGENERATE}");
    panic!("{msg}");
}

#[test]
fn node_mode_methods_are_sorted_unique_vetted_and_cataloged() {
    let listed: Vec<&str> = NODE_MODE_METHODS.iter().map(|(m, _)| *m).collect();
    let mut sorted = listed.clone();
    sorted.sort_unstable();
    sorted.dedup();
    assert_eq!(
        listed, sorted,
        "NODE_MODE_METHODS must be sorted and unique"
    );
    let universe = client_callable_universe();
    let aliases: HashSet<&str> = METHOD_ALIASES.iter().map(|(a, _)| *a).collect();
    for (method, note) in NODE_MODE_METHODS {
        assert!(!note.trim().is_empty(), "{method} needs a vetting note");
        assert!(!aliases.contains(method), "{method} is an alias");
        assert!(universe.contains(*method), "{method} is not cataloged");
    }
}

/// The served list is exactly the documented one (monorepo nodes.md "Node
/// mode"); a change here is a contract change.
#[test]
fn node_mode_list_is_the_documented_list() {
    let listed: Vec<&str> = NODE_MODE_METHODS.iter().map(|(m, _)| *m).collect();
    assert_eq!(
        listed,
        [
            "client.hello",
            "executor.list",
            "executor.mint",
            "executor.revoke",
            "host.status",
            "models.list",
            "pairing.getInfo",
            "pairing.getSelfInfo",
            "principal.me",
            "providers.catalog",
            "providers.profiles.list",
            "server.pairingInfo",
            "settings.get",
            "settings.update",
            "system.requestUpdate",
            "system.shutdown",
            "system.startupStatus",
            "system.status",
        ]
    );
}

#[test]
fn node_mode_lookup_denies_by_default() {
    for method in [
        "client.hello",
        "system.status",
        "executor.mint",
        "settings.update",
    ] {
        assert!(node_mode_may_call(method), "{method} must be served");
    }
    for method in [
        "",
        "unknown.method",
        "workspace.list",
        "workspace.create",
        "agent.create",
        "agent.delegate",
        "agent.sendMessage",
        "hook.schedule",
        "script.start",
        "prMonitor.create",
        "settings.list",
        "settings.reset",
        "host.exec",
        "server.rotateToken",
        "system.capabilities",
        "events.subscribe",
        "workspace.subscribe",
    ] {
        assert!(!node_mode_may_call(method), "{method} must be refused");
    }
    for method in REVERSE_METHODS {
        assert!(
            !node_mode_may_call(method),
            "reverse {method} must be refused"
        );
    }
    for (alias, canonical) in METHOD_ALIASES {
        assert_eq!(
            node_mode_may_call(alias),
            node_mode_may_call(canonical_method(canonical))
        );
    }
}

#[test]
fn node_mode_http_paths_keep_rpc_health_and_executor_link() {
    for path in [
        "/health",
        "/ws",
        crate::node_link::LINK_PATH,
        crate::node_link::GIT_PATH,
    ] {
        assert!(node_mode_serves_path(path), "{path} must be served");
    }
    for path in ["/invite", "/tunnel", "/", "/node", "/node/other", "/ws/"] {
        assert!(!node_mode_serves_path(path), "{path} must be refused");
    }
}
