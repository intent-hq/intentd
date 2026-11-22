//! A self-hosted relay (`server.tunnel.derpUrl`) for E2B sandboxes, against
//! the fake E2B. Each sandbox runs a fake tailcat that logs its arguments,
//! and a local relay serves a DERP map whose one node is a local listener.
//!
//! Proves: a relay whose map cannot be fetched fails the start with
//! `relay-unreachable` and deletes the sandbox; a start with the setting
//! checks that relay and runs every tailcat command with it and a key that
//! embeds its region, so tailcat never consults its default map; a start
//! keeps the tunnel its new daemon is still starting; a changed
//! setting moves the sandbox to the new relay at its next resume (new
//! tunnel address, `cloud.relayUrl`); an unreachable relay fails that
//! resume; and an empty setting brings back the default relay.

use std::sync::atomic::Ordering;

use super::*;
use crate::common::fake_relay::{dead_relay_url, fake_relay, FakeRelay};

/// A tailcat that appends each command line to `$TAILCAT_LOG`, writes a
/// key named after its path at genkey, and serves an address made from the
/// key it was given, `$TAILCAT_SERVE_DELAY` seconds after it starts.
pub(super) fn write_logging_tailcat(dir: &Path) -> std::path::PathBuf {
    use std::os::unix::fs::PermissionsExt;
    let path = dir.join("logging-tailcat.sh");
    let script = r#"#!/bin/sh
printf '%s\n' "$*" >> "$TAILCAT_LOG"
key=""
for arg in "$@"; do
  case "$arg" in
    --key=*) key="${arg#--key=}" ;;
  esac
done
case "$1" in
  genkey)
    printf '%s' "$key" | cksum | cut -d' ' -f1 > "$key"
    ;;
  serve)
    sleep "${TAILCAT_SERVE_DELAY:-0}" # // timing-guard: a tunnel that takes a while to come up
    printf '{"listenAddr":"tc%s"}\n' "$(cat "$key")"
    exec sleep 600 # // timing-guard: a fake sidecar serves until the daemon kills it
    ;;
esac
"#;
    std::fs::write(&path, script).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    path
}

pub(super) async fn set_relay(rpc_ws: &mut common::TlsWs, id: i64, url: &str) {
    ok(
        rpc_ws,
        id,
        "settings.update",
        json!({ "changes": [{ "path": "server.tunnel.derpUrl", "value": url }] }),
    )
    .await;
}

/// The cloud machine `cloudMachines.list` shows for agent `agent_id`.
async fn machine_of(rpc_ws: &mut common::TlsWs, id: i64, agent_id: &str) -> Option<Value> {
    let listed = ok(rpc_ws, id, "cloudMachines.list", json!({})).await;
    listed["machines"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|machine| machine["cloud"]["agentId"] == agent_id)
        .cloned()
}

/// The tailcat command lines logged since line `from`.
fn tailcat_lines(log: &Path, from: usize) -> Vec<String> {
    std::fs::read_to_string(log)
        .unwrap_or_default()
        .lines()
        .skip(from)
        .map(str::to_owned)
        .collect()
}

/// A backend against the fake E2B whose sandboxes run the logging tailcat;
/// returns it, the tailcat log and a create-params maker.
async fn relay_backend(prefix: &str) -> Option<(CloudBackend, std::path::PathBuf)> {
    let backend = CloudBackend::start(prefix, &[]).await?;
    let tailcat = write_logging_tailcat(backend.fake.scratch());
    let log = backend.fake.scratch().join("tailcat.log");
    backend.fake.state().sandbox_env.extend([
        (
            "INTENTD_TAILCAT_BIN".to_owned(),
            tailcat.to_string_lossy().into_owned(),
        ),
        ("TAILCAT_LOG".to_owned(), log.to_string_lossy().into_owned()),
    ]);
    Some((backend, log))
}

fn placed(ws_id: &str, provider: &str, name: &str) -> Value {
    json!({
        "workspaceId": ws_id, "name": name, "model": "default", "provider": provider,
        "placement": { "target": "e2b" },
    })
}

/// Start an agent on a sandbox over `relay` and check that every tailcat
/// command used only it. Returns the agent id, its machine and the log
/// lines seen.
async fn start_over(
    backend: &mut CloudBackend,
    log: &Path,
    relay: &FakeRelay,
    id: i64,
) -> (String, Value, usize) {
    set_relay(&mut backend.rpc_ws, id, &relay.url).await;
    let created = ok(
        &mut backend.rpc_ws,
        id + 1,
        "agent.create",
        placed(&backend.ws_id, &backend.keyed_ref, "Relayed"),
    )
    .await;
    let agent_id = created["agent"]["id"].as_str().unwrap().to_owned();
    send_and_reply(
        &mut backend.rpc_ws,
        (&backend.ws_id, &agent_id),
        "over my relay",
        id + 2,
    )
    .await;
    let machine = machine_of(&mut backend.rpc_ws, id + 10, &agent_id)
        .await
        .unwrap();
    assert_eq!(
        machine["cloud"]["relayUrl"],
        relay.url.as_str(),
        "{machine}"
    );
    assert!(machine["tcAddress"].as_str().is_some(), "{machine}");
    assert!(relay.map_hits.load(Ordering::SeqCst) >= 1);
    assert!(relay.node_hits.load(Ordering::SeqCst) >= 1);
    let lines = tailcat_lines(log, 0);
    let relay_flag = format!("--derpmap-url={}", relay.url);
    assert!(lines.iter().any(|l| l.starts_with("serve")), "{lines:?}");
    for line in &lines {
        assert!(
            line.contains(&relay_flag),
            "tailcat ran without the relay: {line}"
        );
        if line.starts_with("genkey") {
            assert!(line.contains("--embed-derp-map"), "{line}");
        }
    }
    (agent_id, machine, lines.len())
}

/// A relay whose map cannot be fetched fails the start with no fallback; a
/// reachable one is the only relay the sandbox's tailcat uses.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sandbox_start_uses_only_the_backends_relay() {
    let Some((mut backend, log)) = relay_backend("itd-e2b-relay-s-").await else {
        return;
    };
    let dead = dead_relay_url().await;
    set_relay(&mut backend.rpc_ws, 10, &dead).await;
    let refused = ok(
        &mut backend.rpc_ws,
        11,
        "agent.create",
        placed(&backend.ws_id, &backend.keyed_ref, "Dead"),
    )
    .await;
    let refused_id = refused["agent"]["id"].as_str().unwrap().to_owned();
    let failed = agent_until(
        &mut backend.rpc_ws,
        (&backend.ws_id, &refused_id),
        20,
        |a| a.get("placementError").is_some(),
    )
    .await;
    let error = &failed["placementError"];
    assert_eq!(error["code"], "cloud-machine-unavailable", "{failed}");
    assert_eq!(error["data"]["reason"], "relay-unreachable", "{failed}");
    assert_eq!(error["data"]["relayUrl"], dead.as_str(), "{failed}");
    assert_eq!(error["retryable"], true, "{failed}");
    let fake = backend.fake.clone();
    call_until(&fake, &mut backend.rpc_ws, "sbx1", |calls| {
        calls
            .iter()
            .any(|c| is_call(c, "DELETE", "/sandboxes/sbx1"))
    })
    .await;
    assert!(
        tailcat_lines(&log, 0).is_empty(),
        "no tailcat without its relay: {:?}",
        tailcat_lines(&log, 0)
    );

    let relay = fake_relay().await;
    start_over(&mut backend, &log, &relay, 30).await;
    assert_no_secret_logged(&backend.head_dir, &fake);
}

/// A new sandbox's daemon starts its tunnel before it is ready: the start
/// waits for that tunnel instead of restarting it. On E2B a tunnel
/// restarted that way left the first dial over the relay unanswered, so
/// every fresh start failed `machine-unreachable` (intent#9825).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fresh_start_keeps_the_tunnel_its_daemon_is_starting() {
    let Some((mut backend, log)) = relay_backend("itd-e2b-relay-f-").await else {
        return;
    };
    backend
        .fake
        .state()
        .sandbox_env
        .push(("TAILCAT_SERVE_DELAY".to_owned(), "3".to_owned()));
    let relay = fake_relay().await;
    start_over(&mut backend, &log, &relay, 10).await;
    let serves: Vec<String> = tailcat_lines(&log, 0)
        .into_iter()
        .filter(|line| line.starts_with("serve"))
        .collect();
    assert_eq!(serves.len(), 1, "the starting tunnel was restarted: {serves:?}");
    assert_no_secret_logged(&backend.head_dir, &backend.fake);
}

/// A changed setting moves a paused sandbox to the new relay at its next
/// resume; an unreachable relay fails that resume and serves no tunnel; an
/// empty setting brings back the default relay.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_relay_change_reaches_a_sandbox_at_its_next_resume() {
    let Some((mut backend, log)) = relay_backend("itd-e2b-relay-r-").await else {
        return;
    };
    let first = fake_relay().await;
    let second = fake_relay().await;
    let dead = dead_relay_url().await;
    let (agent_id, machine, seen) = start_over(&mut backend, &log, &first, 10).await;
    let first_address = machine["tcAddress"].as_str().unwrap().to_owned();
    let ws_id = backend.ws_id.clone();
    let agent = (ws_id.as_str(), agent_id.as_str());
    let rpc_ws = &mut backend.rpc_ws;

    let paused = ok(
        rpc_ws,
        60,
        "cloudMachines.pause",
        json!({ "machineId": machine["id"] }),
    )
    .await;
    assert_eq!(paused["machine"]["cloud"]["state"], "paused", "{paused}");
    set_relay(rpc_ws, 61, &second.url).await;
    send_and_reply(rpc_ws, agent, "over the new relay", 70).await;
    let moved = machine_of(rpc_ws, 80, &agent_id).await.unwrap();
    assert_eq!(moved["cloud"]["relayUrl"], second.url.as_str(), "{moved}");
    assert_ne!(
        moved["tcAddress"].as_str().unwrap(),
        first_address,
        "{moved}"
    );
    assert!(second.node_hits.load(Ordering::SeqCst) >= 1);
    let lines = tailcat_lines(&log, seen);
    let relay_flag = format!("--derpmap-url={}", second.url);
    assert!(
        lines
            .iter()
            .any(|l| l.starts_with("serve") && l.contains(&relay_flag)),
        "{lines:?}"
    );
    assert!(
        lines.iter().all(|l| l.contains("--derpmap-url=")),
        "{lines:?}"
    );
    let seen = seen + lines.len();

    ok(
        rpc_ws,
        90,
        "cloudMachines.pause",
        json!({ "machineId": machine["id"] }),
    )
    .await;
    set_relay(rpc_ws, 91, &dead).await;
    let mut events = agent_events(backend.port, backend.cfg.clone(), &ws_id).await;
    ok(
        rpc_ws,
        92,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "over a dead relay" }),
    )
    .await;
    let failure = failure_containing(&mut events, &agent_id, &dead).await;
    assert!(failure.contains("never falls back"), "{failure}");
    drop(events);
    let lines = tailcat_lines(&log, seen);
    assert!(lines.iter().all(|l| !l.starts_with("serve")), "{lines:?}");
    let seen = seen + lines.len();

    // Back to the relay the machine row still names: the failed switch left
    // the sandbox's tunnel off on the dead relay, so it is applied again.
    set_relay(rpc_ws, 93, &second.url).await;
    ok(
        rpc_ws,
        94,
        "agent.retry",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    reply_containing(rpc_ws, agent, "over a dead relay", 95).await;
    let back = machine_of(rpc_ws, 96, &agent_id).await.unwrap();
    assert_eq!(back["cloud"]["relayUrl"], second.url.as_str(), "{back}");
    let lines = tailcat_lines(&log, seen);
    assert!(
        lines
            .iter()
            .any(|l| l.starts_with("serve") && l.contains(&relay_flag)),
        "{lines:?}"
    );
    let seen = seen + lines.len();

    ok(
        rpc_ws,
        97,
        "cloudMachines.pause",
        json!({ "machineId": machine["id"] }),
    )
    .await;
    set_relay(rpc_ws, 100, "").await;
    send_and_reply(rpc_ws, agent, "back on the default relay", 101).await;
    let cleared = machine_of(rpc_ws, 110, &agent_id).await.unwrap();
    assert!(cleared["cloud"].get("relayUrl").is_none(), "{cleared}");
    let lines = tailcat_lines(&log, seen);
    assert!(
        lines
            .iter()
            .any(|l| l.starts_with("serve") && !l.contains("--derpmap-url")),
        "{lines:?}"
    );
    let seen = seen + lines.len();

    // On the default relay, a failed switch then a cleared setting: the row
    // names no relay either way, and the sandbox is still repaired.
    ok(
        rpc_ws,
        120,
        "cloudMachines.pause",
        json!({ "machineId": machine["id"] }),
    )
    .await;
    set_relay(rpc_ws, 121, &dead).await;
    let mut events = agent_events(backend.port, backend.cfg.clone(), &ws_id).await;
    ok(
        rpc_ws,
        122,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "off the default relay" }),
    )
    .await;
    failure_containing(&mut events, &agent_id, &dead).await;
    drop(events);
    set_relay(rpc_ws, 123, "").await;
    ok(
        rpc_ws,
        124,
        "agent.retry",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    reply_containing(rpc_ws, agent, "off the default relay", 125).await;
    let lines = tailcat_lines(&log, seen);
    assert!(
        lines
            .iter()
            .any(|l| l.starts_with("serve") && !l.contains("--derpmap-url")),
        "{lines:?}"
    );
    assert_no_secret_logged(&backend.head_dir, &backend.fake);
}
