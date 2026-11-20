//! Agent RPC classes: derived from `PAYLOAD_CLASSES` plus the agent lists,
//! frozen by `tests/goldens/agent_rpc_classes.txt`.

use super::{agent_rpc_class, agent_rpc_methods, refusal};
use crate::catalog::{
    PayloadClass, AGENT_FORBIDDEN_METHODS, AGENT_UNSUPPORTED_METHODS, PAYLOAD_CLASSES,
};
use intent_core::agent_rpc::AgentRpcClass;
use std::fmt::Write as _;

const GOLDEN_REL: &str = "tests/goldens/agent_rpc_classes.txt";
const REGENERATE: &str = "INTENTD_UPDATE_GOLDENS=1 cargo test -p intent-transport --lib agent_rpc::tests::agent_rpc_classes_match_golden";

fn render_golden() -> String {
    let mut out = String::from(
        "# Golden: the agent class of every daemon method on the Assistant's ws.rpc path.\n\
         # Generated from crates/intent-transport/src/catalog.rs PAYLOAD_CLASSES,\n\
         # AGENT_FORBIDDEN_METHODS and AGENT_UNSUPPORTED_METHODS; do not hand-edit.\n\
         # A new method is classified explicitly. Regenerate:\n",
    );
    let _ = writeln!(out, "#   {REGENERATE}");
    for row in agent_rpc_methods() {
        let _ = writeln!(out, "{} {}", row.class.as_str(), row.method);
    }
    out
}

#[test]
fn agent_rpc_classes_match_golden() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(GOLDEN_REL);
    let expected = render_golden();
    if std::env::var("INTENTD_UPDATE_GOLDENS").is_ok_and(|v| v == "1") {
        std::fs::write(&path, &expected).expect("write agent rpc golden");
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
    assert!(
        actual == expected,
        "Agent RPC class drift: a method's class changed or a method was added/removed.\n\
         Classify it in AGENT_FORBIDDEN_METHODS / AGENT_UNSUPPORTED_METHODS or PAYLOAD_CLASSES, then regenerate:\n  {REGENERATE}"
    );
}

#[test]
fn agent_lists_are_sorted_unique_and_cataloged() {
    for list in [AGENT_FORBIDDEN_METHODS, AGENT_UNSUPPORTED_METHODS] {
        let names: Vec<&str> = list.iter().map(|(m, _)| *m).collect();
        let mut sorted = names.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(names, sorted, "agent lists must be sorted and unique");
        for (method, reason) in list {
            assert!(!reason.is_empty(), "{method} needs a reason");
            assert!(
                PAYLOAD_CLASSES.iter().any(|row| row.method == *method),
                "{method} is not a cataloged method"
            );
        }
    }
    for (method, _) in AGENT_FORBIDDEN_METHODS {
        assert!(
            !AGENT_UNSUPPORTED_METHODS.iter().any(|(m, _)| m == method),
            "{method} is on both agent lists"
        );
    }
}

#[test]
fn classes_derive_from_payload_classes() {
    for row in PAYLOAD_CLASSES {
        let class = agent_rpc_class(row.method);
        let listed = AGENT_FORBIDDEN_METHODS
            .iter()
            .chain(AGENT_UNSUPPORTED_METHODS)
            .any(|(m, _)| *m == row.method);
        if listed {
            continue;
        }
        let channel = row.method.ends_with(".subscribe") || row.method.ends_with(".unsubscribe");
        let want = match row.class {
            PayloadClass::Subscription => AgentRpcClass::UnsupportedOnAgentPath,
            _ if channel => AgentRpcClass::UnsupportedOnAgentPath,
            PayloadClass::Mutation => AgentRpcClass::Mutating,
            _ => AgentRpcClass::Read,
        };
        assert_eq!(class, want, "{}", row.method);
    }
    assert_eq!(agent_rpc_methods().len(), PAYLOAD_CLASSES.len());
}

#[test]
fn unknown_methods_fail_closed_as_mutating() {
    assert_eq!(agent_rpc_class("nope.notAMethod"), AgentRpcClass::Mutating);
}

#[test]
fn spec_named_methods_are_forbidden() {
    for method in [
        "pairing.getInfo",
        "server.pairingInfo",
        "pairing.getSelfInfo",
        "server.rotateToken",
        "executor.mint",
        "agent.resolveProposal",
        "agent.resolveRpcProposal",
        "agent.respondPermission",
        "secret.provide",
        "secret.dismiss",
        "settings.update",
        "settings.reset",
        "terminal.write",
    ] {
        assert_eq!(
            agent_rpc_class(method),
            AgentRpcClass::ForbiddenToAgent,
            "{method}"
        );
        let err = refusal(method, agent_rpc_class(method)).expect("refused");
        assert_eq!(err.code, -32003);
        assert_eq!(err.data_code(), Some("forbidden-to-agent"));
    }
    for row in PAYLOAD_CLASSES {
        if row.method.starts_with("browser.") || row.method.starts_with("desktop.") {
            assert_eq!(
                agent_rpc_class(row.method),
                AgentRpcClass::ForbiddenToAgent,
                "{}",
                row.method
            );
        }
    }
}

#[test]
fn connection_stateful_methods_are_unsupported() {
    for method in [
        "client.hello",
        "events.subscribe",
        "agent.subscribe",
        "drafts.get",
    ] {
        assert_eq!(
            agent_rpc_class(method),
            AgentRpcClass::UnsupportedOnAgentPath,
            "{method}"
        );
        let err = refusal(method, agent_rpc_class(method)).expect("refused");
        assert_eq!(err.data_code(), Some("unsupported-on-agent-path"));
    }
    assert_eq!(agent_rpc_class("system.status"), AgentRpcClass::Read);
    assert_eq!(
        agent_rpc_class("workspace.archive"),
        AgentRpcClass::Mutating
    );
}
