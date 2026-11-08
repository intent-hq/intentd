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
        forge(FORGE_HOST_GITHUB, Some("has space")),
        forge(FORGE_HOST_GITHUB, Some(&"x".repeat(16 * 1024 + 1))),
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

/// Every journal masks the held token and every token a live agent was
/// spawned with (node-link.md § Forge token on the machine, rule 10); a
/// token leaves the mask once no live agent holds it.
#[test]
fn the_redactor_masks_the_held_token_and_every_live_agents() {
    let forge = Arc::new(crate::executor::forge::ForgeToken::default());
    let redactor = Redactor::with_forge(None, Some(forge.clone()));
    let line = || redactor.text("a ghp_one b ghp_two".into());
    assert_eq!(line(), "a ghp_one b ghp_two");
    forge.set(Some(Zeroizing::new("ghp_one".to_owned())));
    assert_eq!(line(), "a [redacted] b ghp_two");
    let (_, one) = forge.spawn().unwrap();
    forge.set(Some(Zeroizing::new("ghp_two".to_owned())));
    assert_eq!(
        line(),
        "a [redacted] b [redacted]",
        "a live agent holds ghp_one"
    );
    drop(one);
    assert_eq!(line(), "a ghp_one b [redacted]");
    forge.clear();
    assert_eq!(line(), "a ghp_one b ghp_two");
}

/// A token handed over while an agent runs is masked in that agent's
/// journal at once, though its process was spawned without it.
#[tokio::test]
async fn a_token_handed_over_mid_run_is_masked_in_the_running_agents_journal() {
    let Some(h) = harness(1, &[]) else {
        return;
    };
    h.runner.adopt_head(Some("hs-1"), &[]);
    h.runner.launch(launch("a1")).await;
    h.runner
        .prompt(&AgentId::from("a1"), "t1".into(), text("hello"))
        .await;
    wait_for(&h.runner, "turn end", turn_ended("a1", "t1")).await;
    h.runner
        .forge_frame(forge(FORGE_HOST_GITHUB, Some(FORGE_CANARY)));
    h.runner
        .prompt(
            &AgentId::from("a1"),
            "t2".into(),
            text(&format!("say {FORGE_CANARY}")),
        )
        .await;
    wait_for(&h.runner, "turn end", turn_ended("a1", "t2")).await;
    h.runner.stop(&AgentId::from("a1"));
    let all = wait_for(&h.runner, "exit", exited("a1")).await;
    let journal = serde_json::to_string(&all).unwrap();
    assert!(journal.contains("echo: say [redacted]"), "{journal}");
    assert!(!journal.contains(FORGE_CANARY), "{journal}");
}
