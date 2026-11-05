use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{json, Value};

use super::{Limits, LinkTap, LinkTraffic, PeerKind, TrafficDirection, TrafficPeer};
use crate::node_link::{
    JournalBody, JournalRecord, LaunchCommand, LaunchProfileEnv, LinkFrame, LinkOutcome,
    LinkRequest,
};
use crate::nodes::NodeCounter;
use crate::{AgentId, WorkspaceId};

fn machine() -> TrafficPeer {
    TrafficPeer {
        kind: PeerKind::Machine,
        id: "m-1".into(),
        name: Some("Mac Studio".into()),
    }
}

fn limits(max_records: usize, max_payload_bytes: usize, queue: usize) -> Limits {
    Limits {
        max_records,
        max_payload_bytes,
        queue,
        queue_bytes: usize::MAX,
        batch_window: Duration::ZERO,
    }
}

fn traffic(limits: Limits) -> (Arc<LinkTraffic>, Arc<Mutex<Vec<Value>>>) {
    let traffic = Arc::new(LinkTraffic::with_limits(limits));
    let published = Arc::new(Mutex::new(Vec::new()));
    let sink = published.clone();
    traffic.set_sink(Arc::new(move |batch| sink.lock().unwrap().push(batch)));
    (traffic, published)
}

fn launch_frame() -> LinkFrame {
    LinkFrame::Launch(LaunchCommand {
        agent_id: AgentId::from_string("agent-1"),
        workspace_id: WorkspaceId::from_string("ws-1"),
        provider: "claude-code".into(),
        model: Some("opus".into()),
        base_commit: "abc123".into(),
        repo_key: "repo".into(),
        agent_features: json!({ "apiToken": "tok-secret", "inputTokens": 12 }),
        is_sub_agent: false,
        checkout: None,
        profile_env: Some(LaunchProfileEnv(json!({
            "providerRef": "claude-code@work",
            "fingerprint": "fp-1",
            "env": [
                { "key": "ANTHROPIC_API_KEY", "value": "sk-ant-secret" },
                { "key": "UNSET_ME", "value": null }
            ]
        }))),
    })
}

fn journal_frame(seq: u64) -> LinkFrame {
    LinkFrame::Journal(JournalRecord {
        seq: NodeCounter(seq),
        agent_id: AgentId::from_string("agent-1"),
        workspace_id: WorkspaceId::from_string("ws-1"),
        turn_id: None,
        recorded_at: "2026-11-03T00:00:00Z".into(),
        body: JournalBody::Started {
            base_commit: "abc".into(),
        },
    })
}

async fn settle(traffic: &LinkTraffic, retained: usize) -> Value {
    for _ in 0..200 {
        let status = traffic.status();
        if status["retained"].as_u64() == Some(retained as u64) {
            return status;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("capture never retained {retained}: {}", traffic.status());
}

#[tokio::test]
async fn off_records_nothing() {
    let (traffic, published) = traffic(limits(10, 1 << 16, 16));
    let tap = LinkTap::new(traffic.clone(), machine());
    tap.observe(TrafficDirection::Out, &launch_frame(), 100);
    tokio::time::sleep(Duration::from_millis(20)).await;
    let status = traffic.status();
    assert_eq!(status["active"], false);
    assert_eq!(status["retained"], 0);
    assert_eq!(status["nextSeq"], 1);
    assert!(status.get("startedAt").is_none());
    assert_eq!(traffic.snapshot(None, None)["records"], json!([]));
    assert!(published.lock().unwrap().is_empty());
}

#[tokio::test]
async fn on_records_both_directions_with_fields() {
    let (traffic, published) = traffic(limits(10, 1 << 16, 16));
    let status = traffic.start(true);
    assert_eq!(status["active"], true);
    assert!(status["startedAt"].is_string());
    let tap = LinkTap::new(traffic.clone(), machine());
    tap.observe(TrafficDirection::Out, &launch_frame(), 321);
    tap.observe(TrafficDirection::In, &journal_frame(7), 99);
    settle(&traffic, 2).await;
    let snapshot = traffic.snapshot(None, None);
    let records = snapshot["records"].as_array().unwrap();
    assert_eq!(records.len(), 2);
    let out = &records[0];
    assert_eq!(out["seq"], 1);
    assert_eq!(out["direction"], "out");
    assert_eq!(
        out["peer"],
        json!({ "kind": "machine", "id": "m-1", "name": "Mac Studio" })
    );
    assert_eq!(out["frameType"], "launch");
    assert_eq!(out["agentId"], "agent-1");
    assert_eq!(out["bytes"], 321);
    assert!(out["at"].as_str().unwrap().ends_with('Z'));
    assert!(out["summary"].as_str().unwrap().contains("claude-code"));
    assert_eq!(out["payload"]["type"], "launch");
    assert!(out.get("truncated").is_none());
    let inbound = &records[1];
    assert_eq!(inbound["seq"], 2);
    assert_eq!(inbound["direction"], "in");
    assert_eq!(inbound["frameType"], "journal");
    assert_eq!(inbound["bytes"], 99);
    assert!(inbound["summary"].as_str().unwrap().contains("started"));
    assert_eq!(snapshot["capture"]["nextSeq"], 3);
    let published = published.lock().unwrap();
    let total: usize = published
        .iter()
        .map(|batch| batch["records"].as_array().unwrap().len())
        .sum();
    assert_eq!(total, 2);
    assert_eq!(published.last().unwrap()["capture"]["active"], true);
}

#[tokio::test]
async fn launch_secrets_are_masked() {
    let (traffic, _) = traffic(limits(10, 1 << 16, 16));
    traffic.start(true);
    LinkTap::new(traffic.clone(), machine()).observe(TrafficDirection::Out, &launch_frame(), 10);
    settle(&traffic, 1).await;
    let record = traffic.snapshot(None, None)["records"][0].clone();
    let text = record.to_string();
    assert!(!text.contains("sk-ant-secret"), "{text}");
    assert!(!text.contains("tok-secret"), "{text}");
    let env = &record["payload"]["profileEnv"];
    assert_eq!(env["providerRef"], "claude-code@work");
    assert_eq!(env["fingerprint"], "fp-1");
    assert_eq!(env["env"][0]["key"], "ANTHROPIC_API_KEY");
    assert_eq!(env["env"][0]["value"], "***");
    assert_eq!(env["env"][1]["value"], Value::Null);
    assert_eq!(record["payload"]["agentFeatures"]["apiToken"], "***");
    assert_eq!(record["payload"]["agentFeatures"]["inputTokens"], 12);
}

#[test]
fn redaction_masks_keys_certificates_and_keeps_ids() {
    let mut value = json!({
        "password": "hunter2",
        "Authorization": "Bearer x",
        "nested": [{ "clientSecret": "s", "privateKey": "k", "cookie": "c" }],
        "credentialId": "cred-1",
        "fingerprint": "AB:CD",
        "requestId": "r-1",
        "note": "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----",
        "certificate": { "der": "x" },
        "accessToken": null,
        "plain": "hello",
        "private_key": "k1",
        "privateXkey": "k2",
        "usage": { "inputTokens": 5, "outputTokens": 6, "totalTokens": 11, "tokenized": true },
        "authHeaders": ["a"],
    });
    super::redact(&mut value);
    assert_eq!(value["password"], "***");
    assert_eq!(value["Authorization"], "***");
    assert_eq!(value["nested"][0]["clientSecret"], "***");
    assert_eq!(value["nested"][0]["privateKey"], "***");
    assert_eq!(value["nested"][0]["cookie"], "***");
    assert_eq!(value["credentialId"], "cred-1");
    assert_eq!(value["fingerprint"], "AB:CD");
    assert_eq!(value["requestId"], "r-1");
    assert_eq!(value["note"], "***");
    assert_eq!(value["certificate"], "***");
    assert_eq!(value["accessToken"], Value::Null);
    assert_eq!(value["plain"], "hello");
    assert_eq!(value["private_key"], "***");
    assert_eq!(value["privateXkey"], "***");
    assert_eq!(
        value["usage"],
        json!({ "inputTokens": 5, "outputTokens": 6, "totalTokens": 11, "tokenized": true })
    );
    assert_eq!(value["authHeaders"], json!(["a"]));
}

#[test]
fn secret_key_matches_the_documented_pattern() {
    for key in [
        "token",
        "ACCESS_TOKEN",
        "clientSecret",
        "password",
        "credentials",
        "Authorization",
        "setCookie",
        "privateKey",
        "private-key",
        "privatekey",
        "certificatePem",
    ] {
        assert!(super::secret_key(key), "{key}");
    }
    for key in [
        "fingerprint",
        "credentialId",
        "stopId",
        "requestId",
        "privateNotes",
        "keyring",
        "plain",
    ] {
        assert!(!super::secret_key(key), "{key}");
    }
}

#[tokio::test]
async fn start_and_stop_publish_the_capture_state() {
    let (traffic, published) = traffic(limits(10, 1 << 16, 16));
    let started = traffic.start(true);
    assert_eq!(started["payloads"], true);
    {
        let published = published.lock().unwrap();
        assert_eq!(published.len(), 1);
        assert_eq!(published[0]["records"], json!([]));
        assert_eq!(published[0]["capture"]["active"], true);
    }
    // A start that only changes payloads publishes too.
    assert_eq!(traffic.start(false)["payloads"], false);
    assert_eq!(published.lock().unwrap()[1]["capture"]["payloads"], false);
    let stopped = traffic.stop();
    assert_eq!(stopped["active"], false);
    {
        let published = published.lock().unwrap();
        assert_eq!(published.len(), 3);
        assert_eq!(published[2]["records"], json!([]));
        assert_eq!(published[2]["capture"]["active"], false);
        assert_eq!(published[2]["capture"]["retained"], 0);
    }
    // A stop while off publishes nothing.
    traffic.stop();
    assert_eq!(published.lock().unwrap().len(), 3);
}

#[tokio::test]
async fn request_records_carry_method_and_request_id_and_mask_params() {
    let (traffic, _) = traffic(limits(10, 1 << 16, 16));
    traffic.start(true);
    let request_id = uuid::Uuid::new_v4();
    let tap = LinkTap::new(traffic.clone(), machine());
    tap.observe(
        TrafficDirection::In,
        &LinkFrame::Request(LinkRequest {
            request_id,
            agent_id: AgentId::from_string("agent-1"),
            method: "workspace.call".into(),
            params: json!({
                "method": "note.read",
                "args": { "id": "spec", "githubToken": "ghp_secret" }
            }),
            timeout_ms: 1000,
        }),
        50,
    );
    tap.observe(
        TrafficDirection::Out,
        &LinkFrame::Response {
            request_id,
            outcome: LinkOutcome::Ok(json!({ "content": "hi" })),
        },
        40,
    );
    settle(&traffic, 2).await;
    let records = traffic.snapshot(None, None)["records"].clone();
    assert_eq!(records[0]["requestId"], request_id.to_string());
    let summary = records[0]["summary"].as_str().unwrap();
    assert!(summary.contains("note.read"), "{summary}");
    assert!(!summary.contains("workspace.call"), "{summary}");
    assert_eq!(
        records[0]["payload"]["params"]["args"]["githubToken"],
        "***"
    );
    assert!(!records.to_string().contains("ghp_secret"));
    assert_eq!(records[1]["requestId"], request_id.to_string());
    assert_eq!(records[1]["frameType"], "response");
}

#[tokio::test]
async fn ring_evicts_oldest_and_counts() {
    let (traffic, _) = traffic(limits(3, 1 << 16, 64));
    traffic.start(true);
    let tap = LinkTap::new(traffic.clone(), machine());
    for seq in 1..=5 {
        tap.observe(TrafficDirection::In, &journal_frame(seq), 10);
        // One at a time, so the queue never overflows.
        settle(&traffic, usize::try_from(seq.min(3)).unwrap()).await;
        for _ in 0..200 {
            if traffic.status()["nextSeq"] == seq + 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
    let snapshot = traffic.snapshot(None, None);
    let seqs: Vec<u64> = snapshot["records"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["seq"].as_u64().unwrap())
        .collect();
    assert_eq!(seqs, vec![3, 4, 5]);
    assert_eq!(snapshot["capture"]["retained"], 3);
    assert_eq!(snapshot["capture"]["evicted"], 2);
    assert_eq!(snapshot["capture"]["maxRecords"], 3);
    assert_eq!(snapshot["capture"]["dropped"], 0);
}

#[tokio::test]
async fn snapshot_pages_after_seq_with_limit() {
    let (traffic, _) = traffic(limits(10, 1 << 16, 64));
    traffic.start(true);
    let tap = LinkTap::new(traffic.clone(), machine());
    for seq in 1..=5 {
        tap.observe(TrafficDirection::In, &journal_frame(seq), 10);
    }
    settle(&traffic, 5).await;
    let page = traffic.snapshot(Some(2), Some(2));
    let seqs: Vec<u64> = page["records"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["seq"].as_u64().unwrap())
        .collect();
    assert_eq!(seqs, vec![3, 4]);
}

#[tokio::test]
async fn oversized_payload_is_omitted_and_counted() {
    let (traffic, _) = traffic(limits(10, 64, 16));
    traffic.start(true);
    LinkTap::new(traffic.clone(), machine()).observe(TrafficDirection::Out, &launch_frame(), 900);
    settle(&traffic, 1).await;
    let snapshot = traffic.snapshot(None, None);
    let record = &snapshot["records"][0];
    assert!(record.get("payload").is_none());
    assert_eq!(record["truncated"], true);
    assert_eq!(record["frameType"], "launch");
    assert_eq!(snapshot["capture"]["oversized"], 1);
}

#[tokio::test]
async fn payloads_off_keeps_records_without_bodies() {
    let (traffic, _) = traffic(limits(10, 1 << 16, 16));
    traffic.start(false);
    LinkTap::new(traffic.clone(), machine()).observe(TrafficDirection::Out, &launch_frame(), 10);
    settle(&traffic, 1).await;
    let record = traffic.snapshot(None, None)["records"][0].clone();
    assert!(record.get("payload").is_none());
    assert!(record.get("truncated").is_none());
    assert_eq!(traffic.status()["oversized"], 0);
}

#[tokio::test]
async fn full_queue_drops_and_counts() {
    let traffic = Arc::new(LinkTraffic::with_limits(Limits {
        max_records: 100,
        max_payload_bytes: 1 << 16,
        queue: 2,
        queue_bytes: usize::MAX,
        batch_window: Duration::ZERO,
    }));
    traffic.start(true);
    let tap = LinkTap::new(traffic.clone(), machine());
    // No await between observations: the collector cannot drain, so only
    // the queue's two slots are filled.
    for seq in 1..=6 {
        tap.observe(TrafficDirection::In, &journal_frame(seq), 10);
    }
    let status = settle(&traffic, 2).await;
    assert_eq!(status["dropped"], 4);
}

#[tokio::test]
async fn stop_clears_and_restart_resets() {
    let (traffic, published) = traffic(limits(10, 1 << 16, 16));
    traffic.start(true);
    let tap = LinkTap::new(traffic.clone(), machine());
    tap.observe(TrafficDirection::In, &journal_frame(1), 10);
    settle(&traffic, 1).await;
    let stopped = traffic.stop();
    assert_eq!(stopped["active"], false);
    assert_eq!(stopped["retained"], 0);
    assert_eq!(stopped["nextSeq"], 1);
    assert_eq!(traffic.snapshot(None, None)["records"], json!([]));
    let before = published.lock().unwrap().len();
    tap.observe(TrafficDirection::In, &journal_frame(2), 10);
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(traffic.status()["retained"], 0);
    assert_eq!(published.lock().unwrap().len(), before);
    // A second stop is a no-op.
    assert_eq!(traffic.stop()["active"], false);
    traffic.start(true);
    tap.observe(TrafficDirection::In, &journal_frame(3), 10);
    settle(&traffic, 1).await;
    assert_eq!(traffic.snapshot(None, None)["records"][0]["seq"], 1);
}

#[tokio::test]
async fn start_while_on_keeps_records_and_updates_payloads() {
    let (traffic, _) = traffic(limits(10, 1 << 16, 16));
    traffic.start(true);
    let tap = LinkTap::new(traffic.clone(), machine());
    tap.observe(TrafficDirection::In, &journal_frame(1), 10);
    settle(&traffic, 1).await;
    let started_at = traffic.status()["startedAt"].clone();
    let again = traffic.start(false);
    assert_eq!(again["retained"], 1);
    assert_eq!(again["startedAt"], started_at);
    tap.observe(TrafficDirection::In, &journal_frame(2), 10);
    settle(&traffic, 2).await;
    let records = traffic.snapshot(None, None)["records"].clone();
    assert!(records[0].get("payload").is_some());
    assert!(records[1].get("payload").is_none());
}

#[tokio::test]
async fn snapshot_stops_before_the_byte_cap_but_returns_at_least_one() {
    let (traffic, _) = traffic(limits(100, 1 << 16, 64));
    traffic.start(true);
    let tap = LinkTap::new(traffic.clone(), machine());
    let big = |turn: u64| LinkFrame::Prompt {
        agent_id: AgentId::from_string("agent-1"),
        turn_id: format!("t{turn}"),
        prompt: vec![json!({ "type": "text", "text": "x".repeat(60 * 1024) })],
    };
    for turn in 1..=20 {
        tap.observe(TrafficDirection::Out, &big(turn), 61 * 1024);
    }
    settle(&traffic, 20).await;
    let page = traffic.snapshot(None, Some(2000));
    let records = page["records"].as_array().unwrap();
    assert!(records.len() < 20, "{} records", records.len());
    assert!(!records.is_empty());
    let size = serde_json::to_vec(&page["records"]).unwrap().len();
    assert!(size <= super::SNAPSHOT_MAX_BYTES, "{size}");
    // Paging with afterSeq reaches the end.
    let seq = |r: &Value| r["seq"].as_u64().unwrap();
    let mut seen: Vec<u64> = records.iter().map(seq).collect();
    let mut pages = 1;
    while *seen.last().unwrap() + 1 < page["capture"]["nextSeq"].as_u64().unwrap() {
        let next = traffic.snapshot(seen.last().copied(), None);
        let more = next["records"].as_array().unwrap();
        assert!(!more.is_empty());
        assert!(serde_json::to_vec(more).unwrap().len() <= super::SNAPSHOT_MAX_BYTES);
        seen.extend(more.iter().map(seq));
        pages += 1;
    }
    assert!(pages > 1, "large records span pages");
    assert_eq!(
        seen,
        (1..=20).collect::<Vec<u64>>(),
        "every record once, in order"
    );
}

#[tokio::test]
async fn queued_bytes_are_bounded_and_released() {
    // The collector cannot run between these synchronous observes, so the
    // byte budget alone decides what is queued.
    let (traffic, _) = traffic(Limits {
        queue_bytes: 3000,
        ..limits(100, 1 << 16, 64)
    });
    traffic.start(true);
    let tap = LinkTap::new(traffic.clone(), machine());
    for seq in 1..=10 {
        tap.observe(TrafficDirection::In, &journal_frame(seq), 1000);
    }
    let status = settle(&traffic, 3).await;
    assert_eq!(status["dropped"], 7, "{status}");
    // Draining the queue gives the budget back.
    for seq in 11..=13 {
        tap.observe(TrafficDirection::In, &journal_frame(seq), 1000);
    }
    let status = settle(&traffic, 6).await;
    assert_eq!(status["dropped"], 7, "{status}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_batch_is_published_after_the_stop_that_cleared_it() {
    for _ in 0..50 {
        let (traffic, published) = traffic(limits(100, 1 << 16, 1024));
        traffic.start(true);
        let tap = LinkTap::new(traffic.clone(), machine());
        let feeder = std::thread::spawn(move || {
            for seq in 1..=2000 {
                tap.observe(TrafficDirection::In, &journal_frame(seq), 10);
            }
        });
        tokio::time::sleep(Duration::from_millis(1)).await;
        traffic.stop();
        feeder.join().unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        let published = published.lock().unwrap();
        let last = published.last().unwrap();
        assert_eq!(last["capture"]["active"], false, "{last}");
        assert_eq!(last["records"], json!([]), "{last}");
    }
}

#[tokio::test]
async fn large_frames_queue_a_light_capture() {
    let (traffic, _) = traffic(Limits {
        queue_bytes: 16 * 1024 * 1024,
        ..limits(100, 64 * 1024, 1024)
    });
    traffic.start(true);
    let tap = LinkTap::new(traffic.clone(), machine());
    let big = |turn: u64| LinkFrame::Prompt {
        agent_id: AgentId::from_string("agent-1"),
        turn_id: format!("t{turn}"),
        prompt: vec![json!({ "type": "text", "text": "x".repeat(1024 * 1024) })],
    };
    // 40 MiB of frames on the wire: queued as full frames they would pass
    // the 16 MiB budget; as light captures they stay small.
    for turn in 1..=40 {
        tap.observe(TrafficDirection::In, &big(turn), 1024 * 1024 + 64);
    }
    let queued = traffic.queued_bytes();
    assert!(queued <= 40 * super::LIGHT_BYTES, "{queued}");
    let status = settle(&traffic, 40).await;
    assert_eq!(status["dropped"], 0, "{status}");
    assert_eq!(status["oversized"], 40, "{status}");
    assert_eq!(traffic.queued_bytes(), 0);
    let records = traffic.snapshot(None, Some(1))["records"].clone();
    let record = &records[0];
    assert_eq!(record["frameType"], "prompt");
    assert_eq!(record["agentId"], "agent-1");
    assert_eq!(record["summary"], "prompt agent-1 turn t1");
    assert_eq!(record["bytes"], 1024 * 1024 + 64);
    assert_eq!(record["truncated"], true);
    assert!(record.get("payload").is_none());
}

#[tokio::test]
async fn light_captures_keep_every_summary() {
    // Payloads off: every frame is a light capture, and its summary still
    // names what the full frame would.
    let (traffic, _) = traffic(limits(100, 1 << 16, 64));
    traffic.start(false);
    let tap = LinkTap::new(traffic.clone(), machine());
    let request_id = uuid::Uuid::new_v4();
    let request = LinkFrame::Request(LinkRequest {
        request_id,
        agent_id: AgentId::from_string("agent-1"),
        method: crate::remote_launch::workspace_api::CALL_METHOD.into(),
        params: json!({ "method": "note.read", "args": { "apiToken": "secret" } }),
        timeout_ms: 1000,
    });
    let frames = [
        (launch_frame(), "launch agent-1 with claude-code (opus)"),
        (journal_frame(7), "journal #7 agent-1 started"),
        (request, "request note.read for agent-1"),
    ];
    for (frame, _) in &frames {
        tap.observe(TrafficDirection::Out, frame, 100);
    }
    settle(&traffic, frames.len()).await;
    let records = traffic.snapshot(None, None)["records"].clone();
    for (record, (_, summary)) in records.as_array().unwrap().iter().zip(&frames) {
        assert_eq!(record["summary"], *summary, "{record}");
        assert!(record.get("payload").is_none());
        assert!(record.get("truncated").is_none());
    }
    assert_eq!(records[2]["requestId"], request_id.to_string());
}

#[tokio::test]
async fn each_capture_has_a_later_started_at() {
    let (traffic, _) = traffic(limits(10, 1 << 16, 16));
    let mut previous = String::new();
    for _ in 0..20 {
        let started_at = traffic.start(true)["startedAt"]
            .as_str()
            .unwrap()
            .to_owned();
        traffic.stop();
        let parsed = |at: &str| {
            time::OffsetDateTime::parse(at, &time::format_description::well_known::Rfc3339).unwrap()
        };
        if !previous.is_empty() {
            assert!(
                parsed(&started_at) > parsed(&previous),
                "{previous} then {started_at}"
            );
        }
        previous = started_at;
    }
}

#[test]
fn next_start_bumps_a_tie_by_a_millisecond() {
    let ms = |ms: i128| time::OffsetDateTime::from_unix_timestamp_nanos(ms * 1_000_000).unwrap();
    let at = time::OffsetDateTime::from_unix_timestamp_nanos(1_000_123_456_789).unwrap();
    assert_eq!(super::next_start(None, at), ms(1_000_123));
    assert_eq!(super::next_start(Some(ms(1_000_123)), at), ms(1_000_124));
    assert_eq!(super::next_start(Some(ms(1_000_500)), at), ms(1_000_501));
    assert_eq!(super::next_start(Some(ms(1_000_100)), at), ms(1_000_123));
}

#[tokio::test]
async fn counter_summaries_are_unquoted() {
    let (traffic, _) = traffic(limits(10, 1 << 16, 16));
    traffic.start(true);
    let tap = LinkTap::new(traffic.clone(), machine());
    tap.observe(TrafficDirection::In, &journal_frame(7), 100);
    tap.observe(
        TrafficDirection::Out,
        &LinkFrame::Ack {
            seq: NodeCounter(7),
        },
        20,
    );
    settle(&traffic, 2).await;
    let records = traffic.snapshot(None, None)["records"].clone();
    assert_eq!(records[0]["summary"], "journal #7 agent-1 started");
    assert_eq!(records[1]["summary"], "ack #7");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stop_waits_for_a_batch_being_published() {
    // The sink holds a batch mid-publication while `stop` runs: the stop's
    // event must still come after it.
    let traffic = Arc::new(LinkTraffic::with_limits(limits(10, 1 << 16, 16)));
    let published = Arc::new(Mutex::new(Vec::<Value>::new()));
    let (entered_tx, entered_rx) = std::sync::mpsc::channel::<()>();
    let entered_tx = Mutex::new(entered_tx);
    let sink = published.clone();
    traffic.set_sink(Arc::new(move |batch: Value| {
        if batch["records"].as_array().is_some_and(|r| !r.is_empty()) {
            entered_tx.lock().unwrap().send(()).unwrap();
            std::thread::sleep(Duration::from_millis(100));
        }
        sink.lock().unwrap().push(batch);
    }));
    traffic.start(true);
    LinkTap::new(traffic.clone(), machine()).observe(TrafficDirection::In, &journal_frame(1), 10);
    let stopper = traffic.clone();
    let stop = tokio::task::spawn_blocking(move || {
        entered_rx.recv().unwrap();
        stopper.stop();
    });
    stop.await.unwrap();
    let published = published.lock().unwrap();
    let kinds: Vec<(bool, usize)> = published
        .iter()
        .map(|batch| {
            (
                batch["capture"]["active"].as_bool().unwrap(),
                batch["records"].as_array().unwrap().len(),
            )
        })
        .collect();
    assert_eq!(kinds, vec![(true, 0), (true, 1), (false, 0)]);
}

#[test]
fn hello_generation_is_unquoted() {
    // `binding.generation` is a NodeCounter, which serializes as a string.
    let hello = json!({ "type": "hello", "binding": { "generation": "3" } });
    assert_eq!(super::summarize("hello", &hello), "hello generation 3");
}

#[tokio::test]
async fn payloads_off_never_marks_a_large_frame_truncated() {
    let (traffic, _) = traffic(limits(10, 64, 16));
    traffic.start(false);
    LinkTap::new(traffic.clone(), machine()).observe(TrafficDirection::Out, &launch_frame(), 900);
    settle(&traffic, 1).await;
    let record = traffic.snapshot(None, None)["records"][0].clone();
    assert!(record.get("payload").is_none());
    assert!(record.get("truncated").is_none(), "{record}");
    assert_eq!(traffic.status()["oversized"], 0);
}

#[tokio::test]
async fn light_captures_bound_what_they_copy() {
    // Light captures charge a fixed cost, so what they keep must be bounded
    // too, however long the frame's strings and lists are.
    let (traffic, _) = traffic(limits(100, 64 * 1024, 64));
    traffic.start(false);
    let tap = LinkTap::new(traffic.clone(), machine());
    let huge = "m".repeat(1024 * 1024);
    let request = LinkFrame::Request(LinkRequest {
        request_id: uuid::Uuid::new_v4(),
        agent_id: AgentId::from_string("a".repeat(1024 * 1024)),
        method: crate::remote_launch::workspace_api::CALL_METHOD.into(),
        params: json!({ "method": huge }),
        timeout_ms: 1000,
    });
    let request_with_method = LinkFrame::Request(LinkRequest {
        request_id: uuid::Uuid::new_v4(),
        agent_id: AgentId::from_string("agent-1"),
        method: huge.clone(),
        params: json!({}),
        timeout_ms: 1000,
    });
    let welcome = LinkFrame::Welcome(crate::node_link::LinkWelcome {
        protocol: 6,
        node_id: huge.clone(),
        running_agents: (0..100_000)
            .map(|n| AgentId::from_string(format!("agent-{n}")))
            .collect(),
        capabilities: crate::node_link::MachineCapabilities::new(
            crate::placement::AgentPlatform {
                os: crate::nodes::NodeOs::Linux,
                arch: crate::nodes::NodeArch::X86_64,
            },
            "test",
            2,
            "2026-10-30T00:00:00Z",
        ),
        hosts: vec![huge.clone()],
        tc_address: None,
    });
    for frame in [&request, &request_with_method, &welcome] {
        tap.observe(TrafficDirection::In, frame, 3 * 1024 * 1024);
    }
    settle(&traffic, 3).await;
    let records = traffic.snapshot(None, None)["records"].clone();
    for record in records.as_array().unwrap() {
        let size = serde_json::to_vec(record).unwrap().len();
        assert!(size <= super::LIGHT_BYTES, "{size}: {record}");
    }
    assert_eq!(
        records[2]["summary"],
        "welcome protocol 6, 100000 running agents"
    );
}

fn script_start_frame() -> LinkFrame {
    LinkFrame::ScriptStart(crate::node_link::ScriptStartCommand {
        agent_id: AgentId::from_string("agent-1"),
        script_id: "script-1".into(),
        spawn_id: "run-1".into(),
        command: "npm run dev".into(),
        mode: crate::ScriptMode::Service,
        cwd: None,
        env: vec![crate::node_link::ScriptEnvVar {
            key: "DATABASE_URL".into(),
            value: "postgres://u:canary-db@h/db".into(),
        }],
    })
}

#[tokio::test]
async fn script_env_is_masked_in_script_start_and_forwarded_calls() {
    let (traffic, _) = traffic(limits(10, 1 << 16, 16));
    traffic.start(true);
    let tap = LinkTap::new(traffic.clone(), machine());
    tap.observe(TrafficDirection::Out, &script_start_frame(), 100);
    let request_id = uuid::Uuid::new_v4();
    tap.observe(
        TrafficDirection::In,
        &LinkFrame::Request(LinkRequest {
            request_id,
            agent_id: AgentId::from_string("agent-1"),
            method: crate::remote_launch::workspace_api::CALL_METHOD.into(),
            params: json!({
                "method": "script.create",
                "args": { "name": "dev", "command": "x", "env": { "PLAIN": "canary-obj" } },
                // Variables that happen to share a profile overlay's member
                // names are still an ordinary env map: every value masked.
                "more": [
                    { "env": { "env": "x", "DATABASE_URL": "postgres://user:canary-pw1@h/db" } },
                    { "env": { "providerRef": "y", "API_URL": "https://canary-pw2.example" } },
                ],
            }),
            timeout_ms: 1000,
        }),
        100,
    );
    tap.observe(
        TrafficDirection::Out,
        &LinkFrame::Response {
            request_id,
            outcome: LinkOutcome::Ok(json!({
                "scripts": [{ "id": "s", "env": { "SECRETLESS": "canary-row" } }],
                "nested": { "env": [
                    { "key": "K", "value": "canary-arr" },
                    { "key": "N", "value": null },
                    // Entries that are not `{ key, value }` pairs still get
                    // the generic redaction.
                    "-----BEGIN PRIVATE KEY----- canary-pem",
                    { "token": "canary-token" },
                ] },
            })),
        },
        100,
    );
    settle(&traffic, 3).await;
    let records = traffic.snapshot(None, None)["records"].clone();
    let text = records.to_string();
    for canary in [
        "canary-db",
        "canary-obj",
        "canary-row",
        "canary-arr",
        "canary-pw1",
        "canary-pw2",
        "canary-pem",
        "canary-token",
    ] {
        assert!(!text.contains(canary), "{canary} in {text}");
    }
    assert_eq!(records[0]["payload"]["env"][0]["key"], "DATABASE_URL");
    assert_eq!(records[0]["payload"]["env"][0]["value"], "***");
    assert_eq!(records[0]["payload"]["command"], "npm run dev");
    assert_eq!(
        records[1]["payload"]["params"]["args"]["env"]["PLAIN"],
        "***"
    );
    let more = &records[1]["payload"]["params"]["more"];
    assert_eq!(more[0]["env"]["env"], "***", "{more}");
    assert_eq!(more[0]["env"]["DATABASE_URL"], "***", "{more}");
    assert_eq!(more[1]["env"]["providerRef"], "***", "{more}");
    assert_eq!(more[1]["env"]["API_URL"], "***", "{more}");
    assert_eq!(
        records[2]["payload"]["outcome"]["ok"]["nested"]["env"][0]["key"],
        "K"
    );
    assert_eq!(
        records[2]["payload"]["outcome"]["ok"]["nested"]["env"][1]["value"],
        Value::Null
    );
}

#[tokio::test]
async fn script_frames_are_summarized() {
    let (traffic, _) = traffic(limits(10, 1 << 16, 16));
    traffic.start(false);
    let tap = LinkTap::new(traffic.clone(), machine());
    let run = |id: &str| (id.to_owned(), "run-1".to_owned());
    let (script_id, spawn_id) = run("script-1");
    let frames = [
        (
            script_start_frame(),
            "scriptStart script-1 spawn run-1 for agent-1 (service)",
        ),
        (
            LinkFrame::ScriptStop(crate::node_link::ScriptProcessRef {
                agent_id: crate::AgentId::from("agent-1"),
                script_id: script_id.clone(),
                spawn_id: spawn_id.clone(),
            }),
            "scriptStop script-1 spawn run-1 for agent-1",
        ),
        (
            LinkFrame::ScriptStarted(crate::node_link::ScriptStarted {
                agent_id: crate::AgentId::from("agent-1"),
                script_id: script_id.clone(),
                spawn_id: spawn_id.clone(),
                pid: 77,
            }),
            "scriptStarted script-1 spawn run-1 for agent-1 pid 77",
        ),
        (
            LinkFrame::ScriptOutput(crate::node_link::ScriptOutput {
                agent_id: crate::AgentId::from("agent-1"),
                script_id: script_id.clone(),
                spawn_id: spawn_id.clone(),
                seq: 3,
                data: "canary-output".into(),
            }),
            "scriptOutput script-1 spawn run-1 for agent-1 #3",
        ),
        (
            LinkFrame::ScriptExited(crate::node_link::ScriptExited {
                agent_id: crate::AgentId::from("agent-1"),
                script_id,
                spawn_id,
                exit_code: Some(2),
                error: None,
            }),
            "scriptExited script-1 spawn run-1 for agent-1 code 2",
        ),
    ];
    for (frame, _) in &frames {
        tap.observe(TrafficDirection::In, frame, 100);
    }
    settle(&traffic, frames.len()).await;
    let records = traffic.snapshot(None, None)["records"].clone();
    for (record, (_, summary)) in records.as_array().unwrap().iter().zip(&frames) {
        assert_eq!(record["summary"], *summary, "{record}");
    }
    for record in records.as_array().unwrap() {
        assert_eq!(record["agentId"], "agent-1", "{record}");
    }
    assert!(!records.to_string().contains("canary-output"));
}
