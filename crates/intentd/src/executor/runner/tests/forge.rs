//! The backend's GitHub token on the machine (nodeProtocol 15): held in
//! memory, given to every spawned agent as `GH_TOKEN`, masked in its
//! journal, and dropped on `null`, a new `headSession` and a bad frame.

use intent_core::node_link::{
    ForgeToken, LinkId, SecretOutcome, SecretValue, FORGE_HOST_GITHUB, FORGE_TOKEN_INVALID,
};

use super::*;

const FORGE_CANARY: &str = "ghp_forgeRunnerCanary9Kx3";

fn forge(host: &str, token: Option<&str>) -> ForgeToken {
    ForgeToken {
        request_id: LinkId::new_v4(),
        host: host.to_owned(),
        token: token.map(SecretValue::from),
    }
}

/// Launch `id`, run one turn, and return its environment and journal.
async fn run_one(h: &Harness, id: &str, prompt: &str) -> (serde_json::Map<String, Value>, String) {
    h.runner.launch(launch(id)).await;
    h.runner
        .prompt(&AgentId::from(id), "t1".into(), text(prompt))
        .await;
    wait_for(&h.runner, "turn end", turn_ended(id, "t1")).await;
    let env = agent_env(&h.storage, id).await;
    h.runner.stop(&AgentId::from(id));
    let all = wait_for(&h.runner, "exit", exited(id)).await;
    (env, serde_json::to_string(&all).unwrap())
}

#[tokio::test]
async fn every_agent_gets_the_held_token_masked_in_its_journal() {
    let Some(h) = harness(1, &["--dump-env"]) else {
        return;
    };
    let (logs, _guard) = capture_logs();
    h.runner.adopt_head(Some("hs-1"), &[]);
    let reply = h
        .runner
        .forge_frame(forge(FORGE_HOST_GITHUB, Some(FORGE_CANARY)));
    assert_eq!(reply.outcome, SecretOutcome::Ok);
    assert!(!format!("{:?}", h.runner.head_link().forge()).contains(FORGE_CANARY));

    let (env, journal) = run_one(&h, "a1", "echo-env GH_TOKEN").await;
    assert_eq!(env["GH_TOKEN"], FORGE_CANARY);
    assert!(journal.contains("token [redacted]"), "{journal}");
    assert!(!journal.contains(FORGE_CANARY), "{journal}");
    let logs = String::from_utf8(lock(&logs).clone()).unwrap();
    assert!(!logs.contains(FORGE_CANARY), "{logs}");

    // `null` drops it: the next agent starts without one.
    h.runner.forge_frame(forge(FORGE_HOST_GITHUB, None));
    let (env, _) = run_one(&h, "a2", "hello").await;
    assert!(env.get("GH_TOKEN").is_none(), "{env:?}");
}

#[tokio::test]
async fn a_new_head_session_or_an_older_link_drops_the_token() {
    let Some(h) = harness(1, &[]) else {
        return;
    };
    h.runner.adopt_head(Some("hs-1"), &[]);
    h.runner
        .forge_frame(forge(FORGE_HOST_GITHUB, Some(FORGE_CANARY)));
    // The same session binding again keeps it.
    h.runner.adopt_head(Some("hs-1"), &[]);
    assert!(h.runner.head_link().forge().get().is_some());
    // A restarted backend's session drops it.
    h.runner.adopt_head(Some("hs-2"), &[]);
    assert!(h.runner.head_link().forge().get().is_none());

    h.runner
        .forge_frame(forge(FORGE_HOST_GITHUB, Some(FORGE_CANARY)));
    h.runner.drop_forge();
    assert!(h.runner.head_link().forge().get().is_none());
}

#[tokio::test]
async fn an_unknown_host_or_unusable_token_is_refused_and_changes_nothing() {
    let Some(h) = harness(1, &[]) else {
        return;
    };
    h.runner
        .forge_frame(forge(FORGE_HOST_GITHUB, Some(FORGE_CANARY)));
    for frame in [
        forge("gitlab.com", Some("other")),
        forge(FORGE_HOST_GITHUB, Some("")),
        forge(FORGE_HOST_GITHUB, Some("two\nlines")),
    ] {
        let reply = h.runner.forge_frame(frame);
        assert_eq!(reply.outcome, SecretOutcome::Failed);
        assert_eq!(reply.error.as_deref(), Some(FORGE_TOKEN_INVALID));
    }
    assert_eq!(
        h.runner
            .head_link()
            .forge()
            .get()
            .as_deref()
            .map(String::as_str),
        Some(FORGE_CANARY)
    );
}

/// A token handed over while an agent runs is masked in its journal too:
/// the next spawn (or the credential helper) may hand it out.
#[test]
fn the_redactor_masks_each_forge_token_it_was_given() {
    let redactor = Redactor::new(None);
    assert_eq!(redactor.text("a ghp_one b".into()), "a ghp_one b");
    redactor.forge("ghp_one");
    redactor.forge("ghp_two");
    redactor.forge("ghp_one");
    redactor.forge("");
    assert_eq!(
        redactor.text("a ghp_one b ghp_two".into()),
        "a [redacted] b [redacted]"
    );
}
