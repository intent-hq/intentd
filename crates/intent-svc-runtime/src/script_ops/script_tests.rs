use std::time::Duration;

use base64::Engine as _;
use intent_core::{ScriptCreateParams, ScriptMode, WorkspaceApi, WorkspaceId};
use intent_store::Store;
use serde_json::Value;

use intent_services::feature_api::events::{EventBus, Subscription, SubscriptionFilter};
use intent_services::feature_api::test_support::fixtures::{workspace, TempDb};
use intent_services::feature_api::Services;

/// Pure-liveness deadline for the event-driven waits below (monorepo#515):
/// `drain_until` returns as soon as the awaited event arrives, so this
/// bound only has to outlast a worst-case multi-suite machine stall
/// (login-shell spawn + exit-poll + bus delivery), never a passing run.
const LIVENESS: Duration = Duration::from_secs(300);
/// Service lifetime long enough that a service under test cannot exit
/// (and auto-restart, killing its PTY) before the post-wait assertions
/// run under load (monorepo#515). Strictly outlives `LIVENESS` so waits
/// bounded by it can never be outlived by the command exiting first.
/// Every test that starts one stops it.
const KEEPALIVE: &str = "sleep 3600";

struct Harness {
    _tmp: TempDb,
    services: Services,
    bus: EventBus,
    store: Store,
    ws: WorkspaceId,
}

async fn harness() -> Harness {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store.insert_workspace(&workspace(&ws)).await.expect("ws");
    let bus = EventBus::new(store.clone());
    let services = crate::test_runtime::with_runtime(Services::new_isolated(store.clone()))
        .with_event_bus(bus.clone());
    Harness {
        _tmp: tmp,
        services,
        bus,
        store,
        ws,
    }
}

fn subscribe(h: &Harness) -> Subscription {
    h.bus.subscribe(SubscriptionFilter {
        event_types: vec!["script:*".to_string()],
        workspace_id: Some(h.ws.0.clone()),
        ..Default::default()
    })
}

fn decode(s: &str) -> Vec<u8> {
    base64::engine::general_purpose::STANDARD.decode(s).unwrap()
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack.windows(needle.len()).any(|w| w == needle)
}

async fn create(h: &Harness, name: &str, command: &str, mode: ScriptMode) -> String {
    let params = ScriptCreateParams {
        name: name.to_string(),
        command: command.to_string(),
        mode,
        ..Default::default()
    };
    let v = h
        .services
        .script_create(h.ws.clone(), params)
        .await
        .expect("create");
    v["id"].as_str().expect("script id").to_string()
}

/// Drain script events until `pred` returns `Some`, accumulating decoded
/// `script:output` chunks along the way.
async fn drain_until<F, T>(sub: &mut Subscription, timeout: Duration, mut pred: F) -> (T, Vec<u8>)
where
    F: FnMut(&Value) -> Option<T>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    let mut data = Vec::new();
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let batch = tokio::time::timeout(remaining, sub.recv())
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "script event not delivered within {timeout:?}; output so far: {:?}",
                    String::from_utf8_lossy(&data)
                )
            })
            .expect("subscription open");
        for ev in &batch {
            let v = serde_json::to_value(ev).expect("serialize");
            if v["type"] == "script:output" {
                if let Some(chunk) = v["data"]["chunk"].as_str() {
                    data.extend_from_slice(&decode(chunk));
                }
            }
            if let Some(t) = pred(&v) {
                return (t, data);
            }
        }
    }
}

/// `script.run` runs a command-mode script once and captures its output.
#[intent_test_macros::daemon_test]
async fn command_runs_once_and_captures_output() {
    let h = harness().await;
    let id = create(&h, "echo", "echo run-once-output", ScriptMode::Command).await;
    // The timeout is a liveness bound only — the run ends when `echo`
    // exits — so keep it far above any plausible machine stall.
    let out = h
        .services
        .script_run(h.ws.clone(), id, None, Some(300))
        .await
        .expect("run");
    assert_eq!(out["timedOut"], false);
    assert_eq!(out["exitCode"], 0);
    assert!(
        out["output"]
            .as_str()
            .unwrap_or("")
            .contains("run-once-output"),
        "captured output: {:?}",
        out["output"]
    );
}

/// `script.output` returns the buffer as a plaintext string (a `[... lines]`
/// header + text), not an object — the ancestor/§5.8 parity contract.
#[intent_test_macros::daemon_test]
async fn output_returns_plaintext_buffer_with_header() {
    let h = harness().await;
    let mut sub = subscribe(&h);
    let id = create(
        &h,
        "svc",
        &format!("echo OUTPUT-PARITY-MARK ; {KEEPALIVE}"),
        ScriptMode::Service,
    )
    .await;
    h.services
        .script_start(h.ws.clone(), id.clone())
        .await
        .expect("start");
    drain_until(&mut sub, LIVENESS, |v| {
        if v["type"] == "script:output" {
            let bytes = v["data"]["chunk"].as_str().map(decode).unwrap_or_default();
            contains(&bytes, b"OUTPUT-PARITY-MARK").then_some(())
        } else {
            None
        }
    })
    .await;

    let out = h
        .services
        .script_output(h.ws.clone(), id.clone(), Some(10), None, None)
        .await
        .expect("output");
    let text = out
        .as_str()
        .unwrap_or_else(|| panic!("script.output must be a string, got: {out:?}"));
    assert!(text.starts_with('['), "header line present: {text:?}");
    assert!(
        text.contains("lines]"),
        "header is a [... lines] line: {text:?}"
    );
    assert!(
        text.contains("OUTPUT-PARITY-MARK"),
        "buffer text included: {text:?}"
    );
    h.services
        .script_stop(h.ws.clone(), id)
        .await
        .expect("stop");
}

/// Regression (paste/echo throughput follow-through): `script:output`
/// chunks are transient — broadcast to live subscribers but never
/// persisted to the event table — while `script:state` transitions stay
/// durable. Replay comes from `script.output` (the PTY buffer), not
/// events.
#[intent_test_macros::daemon_test]
async fn output_is_transient_state_is_durable() {
    let h = harness().await;
    let mut sub = subscribe(&h);
    let id = create(
        &h,
        "transient",
        &format!("echo TRANSIENT-OUTPUT-MARK ; {KEEPALIVE}"),
        ScriptMode::Service,
    )
    .await;
    h.services
        .script_start(h.ws.clone(), id.clone())
        .await
        .expect("start");
    drain_until(&mut sub, LIVENESS, |v| {
        if v["type"] == "script:output" {
            let bytes = v["data"]["chunk"].as_str().map(decode).unwrap_or_default();
            contains(&bytes, b"TRANSIENT-OUTPUT-MARK").then_some(())
        } else {
            None
        }
    })
    .await;
    h.services
        .script_stop(h.ws.clone(), id)
        .await
        .expect("stop");

    let output_rows = h
        .store
        .query_events(&intent_store::EventQuery {
            workspace_id: Some(h.ws.clone()),
            event_types: vec!["script:output".to_string()],
            ..Default::default()
        })
        .await
        .expect("query script:output");
    assert!(
        output_rows.is_empty(),
        "script:output must not be persisted (transient publish path)"
    );
    let state_rows = h
        .store
        .query_events(&intent_store::EventQuery {
            workspace_id: Some(h.ws.clone()),
            event_types: vec!["script:state".to_string()],
            ..Default::default()
        })
        .await
        .expect("query script:state");
    assert!(
        !state_rows.is_empty(),
        "script:state lifecycle transitions stay durable"
    );
}

/// `script.output` on a script that never produced output returns the bare
/// `"No output yet."` string (ancestor empty-buffer case).
#[intent_test_macros::daemon_test]
async fn output_empty_buffer_returns_placeholder() {
    let h = harness().await;
    let id = create(&h, "idle", "echo never-started", ScriptMode::Command).await;
    let out = h
        .services
        .script_output(h.ws.clone(), id, None, None, None)
        .await
        .expect("output");
    assert_eq!(out, Value::String("No output yet.".to_string()));
}

/// A service that exits faster than the too-fast floor is treated as a
/// config error and is NOT auto-restarted (the ported backoff guard).
///
/// Load-independent (monorepo#514, monorepo#623): the floor is injected as
/// `u128::MAX` so the supervisor's wall-clock measurement of the `echo` run
/// can never cross it — the no-restart decision is time-independent by
/// construction — and the test awaits the supervisor's *positive* "Exited
/// too quickly" separator (emitted right before it stops supervising)
/// instead of watching a fixed window for the absence of a restart. The
/// await deadline is purely liveness (real login-shell spawn + exit-poll +
/// bus delivery) and is kept far above any plausible multi-suite machine
/// stall (monorepo#623). A spurious restart still fails fast: the restart
/// separator or a `running` state with `restartCount` >= 1 would arrive
/// before the too-fast separator ever could.
#[intent_test_macros::daemon_test]
async fn service_too_fast_exit_does_not_restart() {
    let mut h = harness().await;
    h.services = h.services.with_script_too_fast_ms(u128::MAX);
    let mut sub = subscribe(&h);
    let id = create(&h, "boom", "echo boom", ScriptMode::Service).await;
    h.services
        .script_start(h.ws.clone(), id.clone())
        .await
        .expect("start");
    drain_until(&mut sub, LIVENESS, |v| {
        if v["type"] == "script:state"
            && v["data"]["status"] == "running"
            && v["data"]["restartCount"].as_i64().unwrap_or(0) >= 1
        {
            panic!(
                "too-fast-exit service must NOT auto-restart; saw restarted `running` script:state: {v}",
            );
        }
        if v["type"] == "script:output" {
            let chunk = v["data"]["chunk"].as_str().map(decode).unwrap_or_default();
            assert!(!contains(&chunk, b"Restarting (attempt"), "too-fast-exit service must NOT auto-restart; saw restart separator");
            return contains(&chunk, b"Exited too quickly").then_some(());
        }
        None
    })
    .await;
    let st = h
        .services
        .script_status(h.ws.clone(), id)
        .await
        .expect("status");
    assert_eq!(st["status"], "exited");
    assert_eq!(st["restartCount"], 0);
}

/// A service that runs past the 2s floor before exiting auto-restarts once,
/// surfacing `restartCount: 1` on the next `running` `script:state`.
#[intent_test_macros::daemon_test]
async fn service_auto_restarts_after_long_enough_run() {
    let h = harness().await;
    let mut sub = subscribe(&h);
    let id = create(&h, "svc", "sleep 2.1", ScriptMode::Service).await;
    let agent = intent_core::AgentId::from("agent-service-starter");
    intent_core::with_caller(
        intent_core::Caller::Agent {
            agent_id: agent.clone(),
        },
        h.services.script_start(h.ws.clone(), id.clone()),
    )
    .await
    .expect("start");
    let (respawned, _) = drain_until(&mut sub, LIVENESS, |v| {
        (v["type"] == "script:state"
            && v["data"]["status"] == "running"
            && v["data"]["restartCount"] == 1)
            .then(|| v["data"].clone())
    })
    .await;
    assert_eq!(
        respawned["startedBy"],
        serde_json::json!({ "kind": "agent", "agentId": agent.as_str() }),
        "the auto-restart keeps the run's starter: {respawned}"
    );
    // The `restartCount: 1` event above proves the single auto-restart;
    // a stalled scheduler can let the short-lived second run exit (and
    // restart again) before this status call, so only assert the floor.
    let st = h
        .services
        .script_status(h.ws.clone(), id.clone())
        .await
        .expect("status");
    assert!(
        st["restartCount"].as_i64().unwrap_or(0) >= 1,
        "restartCount reflects at least the observed restart: {st}"
    );
    h.services
        .script_stop(h.ws.clone(), id)
        .await
        .expect("stop");
}

/// A service whose output prints a local dev-server URL surfaces it on
/// `script:state` as `detectedUrl` (the `forward.*` hook).
#[intent_test_macros::daemon_test]
async fn service_url_detection_emits_state() {
    let h = harness().await;
    let mut sub = subscribe(&h);
    let id = create(
        &h,
        "dev",
        &format!("echo listening on http://localhost:3000/ ; {KEEPALIVE}"),
        ScriptMode::Service,
    )
    .await;
    h.services
        .script_start(h.ws.clone(), id.clone())
        .await
        .expect("start");
    let (url, _) = drain_until(&mut sub, LIVENESS, |v| {
        if v["type"] == "script:state" {
            v["data"]["detectedUrl"].as_str().map(str::to_string)
        } else {
            None
        }
    })
    .await;
    assert!(url.contains("localhost:3000"), "detected url: {url}");
    h.services
        .script_stop(h.ws.clone(), id)
        .await
        .expect("stop");
}

/// A running script's PTY stays hidden from `terminal.list` while its
/// scrollback remains readable through `script.output` (§12.2).
#[intent_test_macros::daemon_test]
async fn running_script_is_hidden_but_output_remains_available() {
    let h = harness().await;
    let mut sub = subscribe(&h);
    let id = create(
        &h,
        "svc",
        &format!("echo SCRIPT-PTY-MARK ; {KEEPALIVE}"),
        ScriptMode::Service,
    )
    .await;
    h.services
        .script_start(h.ws.clone(), id.clone())
        .await
        .expect("start");
    // Wait until the marker has streamed (so it is in the PTY scrollback).
    drain_until(&mut sub, LIVENESS, |v| {
        if v["type"] == "script:output" {
            let bytes = v["data"]["chunk"].as_str().map(decode).unwrap_or_default();
            contains(&bytes, b"SCRIPT-PTY-MARK").then_some(())
        } else {
            None
        }
    })
    .await;

    // Script-owned PTYs do not hydrate as generic terminal tabs.
    let list = h.services.terminal_list(h.ws.clone()).await.expect("list");
    assert!(
        list["terminals"].as_array().is_some_and(Vec::is_empty),
        "script PTY must not be listed: {list}"
    );

    // The same PTY scrollback remains available through the script API.
    let output = h
        .services
        .script_output(h.ws.clone(), id.clone(), None, None, None)
        .await
        .expect("script output");
    let text = output.as_str().expect("script output string");
    assert!(
        text.contains("SCRIPT-PTY-MARK"),
        "script.output reads the hidden PTY output: {text:?}"
    );
    h.services
        .script_stop(h.ws.clone(), id)
        .await
        .expect("stop");
}

/// The full `script.list` view pages by `limit`, cursor and byte budget, and
/// together the pages list every definition once in `(createdAt, id)` order.
#[intent_test_macros::daemon_test]
async fn full_list_pages_cover_every_definition_within_the_byte_budget() {
    let h = harness().await;
    let big = format!("echo {}", "x".repeat(200 * 1024));
    let mut ids = Vec::new();
    for i in 0..5 {
        let command = if i < 3 {
            big.clone()
        } else {
            "true".to_string()
        };
        ids.push(create(&h, &format!("s{i}"), &command, ScriptMode::Command).await);
    }
    let whole = h
        .services
        .script_list(h.ws.clone())
        .await
        .expect("unpaged list");
    let expected: Vec<Value> = whole["scripts"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["id"].clone())
        .collect();
    assert_eq!(expected.len(), 5);

    // Three 200 KiB commands: the 480,000-byte page budget stops the first page at two.
    let first = h
        .services
        .script_list_view(h.ws.clone(), serde_json::json!({}))
        .await
        .expect("first page");
    assert_eq!(first["scripts"].as_array().unwrap().len(), 2);
    assert!(serde_json::to_vec(&first).unwrap().len() < 500 * 1024);
    let mut seen: Vec<Value> = first["scripts"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["id"].clone())
        .collect();
    let mut cursor = first["nextCursor"].as_str().expect("more pages").to_owned();
    loop {
        let page = h
            .services
            .script_list_view(
                h.ws.clone(),
                serde_json::json!({ "view": "full", "limit": 2, "cursor": cursor }),
            )
            .await
            .expect("next page");
        let rows = page["scripts"].as_array().unwrap();
        assert!(!rows.is_empty() && rows.len() <= 2);
        assert!(rows.iter().all(|r| r["command"].is_string()), "full rows");
        seen.extend(rows.iter().map(|s| s["id"].clone()));
        match page.get("nextCursor") {
            Some(next) => cursor = next.as_str().unwrap().to_owned(),
            None => break,
        }
    }
    assert_eq!(seen, expected);

    // `limit` alone still caps the row count.
    let one = h
        .services
        .script_list_view(
            h.ws.clone(),
            serde_json::json!({ "view": "full", "limit": 1 }),
        )
        .await
        .expect("one row");
    assert_eq!(one["scripts"].as_array().unwrap().len(), 1);
    assert!(one["nextCursor"].is_string());

    // A summary cursor is not a full-view cursor, and vice versa.
    let summary = h
        .services
        .script_list_view(
            h.ws.clone(),
            serde_json::json!({ "view": "summary", "limit": 1 }),
        )
        .await
        .expect("summary page");
    for (options, label) in [
        (
            serde_json::json!({ "cursor": summary["nextCursor"] }),
            "summary cursor on the full view",
        ),
        (
            serde_json::json!({ "view": "summary", "cursor": first["nextCursor"] }),
            "full cursor on the summary view",
        ),
        (serde_json::json!({ "limit": 0 }), "limit 0"),
    ] {
        assert!(
            h.services
                .script_list_view(h.ws.clone(), options)
                .await
                .is_err(),
            "{label}"
        );
    }
}

/// One definition too big on its own (an escape-heavy 300 KB command, about
/// 600 KB as JSON) still fits the page: its largest fields are replaced by
/// `<field>Omitted` / `<field>Bytes`, `script.get` serves it whole, and the
/// cursors still cover every definition once.
#[intent_test_macros::daemon_test]
async fn full_list_bounds_an_oversized_singleton_row() {
    let h = harness().await;
    let quotes = format!("echo '{}'", "\"".repeat(300_000));
    let mut ids = Vec::new();
    for (i, command) in ["true", quotes.as_str(), "true"].into_iter().enumerate() {
        ids.push(create(&h, &format!("s{i}"), command, ScriptMode::Command).await);
    }
    let mut seen = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let params = cursor.as_ref().map_or_else(
            || serde_json::json!({}),
            |c| serde_json::json!({ "limit": 1, "cursor": c }),
        );
        let page = h
            .services
            .script_list_view(h.ws.clone(), params)
            .await
            .expect("page");
        let size = serde_json::to_vec(&page).unwrap().len();
        assert!(size <= 480_000, "page is {size} bytes");
        for row in page["scripts"].as_array().unwrap() {
            seen.push(row["id"].as_str().unwrap().to_owned());
            if row["name"] == "s1" {
                assert!(row.get("command").is_none(), "oversized command omitted");
                assert_eq!(row["commandOmitted"], true);
                assert_eq!(row["commandBytes"], quotes.len());
            } else {
                assert_eq!(row["command"], "true");
            }
        }
        match page.get("nextCursor") {
            Some(next) => cursor = Some(next.as_str().unwrap().to_owned()),
            None => break,
        }
    }
    assert_eq!(seen, ids);
    let whole = h
        .services
        .script_get(h.ws.clone(), ids[1].clone())
        .await
        .expect("get");
    assert_eq!(whole["command"], quotes);
}

/// One `script:output` event: its bytes and, for run-log output, its
/// `(runId, daemonBootId, startOffset, endOffset)`.
struct OutputEvent {
    bytes: Vec<u8>,
    cursor: Option<(String, String, u64, u64)>,
}

fn position(v: &Value, key: &str) -> u64 {
    v[key]
        .as_str()
        .unwrap_or_else(|| panic!("{key} is a decimal string: {v:?}"))
        .parse()
        .expect("decimal position")
}

/// Every `script:output` event until the streamed bytes contain `marker`.
async fn output_events_until(sub: &mut Subscription, marker: &[u8]) -> Vec<OutputEvent> {
    let mut events: Vec<OutputEvent> = Vec::new();
    let mut seen = Vec::new();
    drain_until(sub, LIVENESS, |v| {
        if v["type"] != "script:output" {
            return None;
        }
        let data = &v["data"];
        let bytes = decode(data["chunk"].as_str().expect("chunk"));
        let cursor = data.get("runId").map(|run| {
            (
                run.as_str().expect("runId").to_owned(),
                data["daemonBootId"]
                    .as_str()
                    .expect("daemonBootId")
                    .to_owned(),
                position(data, "startOffset"),
                position(data, "endOffset"),
            )
        });
        seen.extend_from_slice(&bytes);
        events.push(OutputEvent { bytes, cursor });
        contains(&seen, marker).then_some(())
    })
    .await;
    events
}

/// The log events' bytes, after checking they form one contiguous stream of
/// one run from position 0. Returns `(runId, bootId, bytes)`.
fn contiguous_run(events: &[OutputEvent]) -> (String, String, Vec<u8>) {
    let mut run: Option<(String, String)> = None;
    let mut bytes = Vec::new();
    for (cursor, chunk) in events
        .iter()
        .filter_map(|e| e.cursor.as_ref().map(|c| (c, &e.bytes)))
    {
        let (r, boot, start, end) = cursor;
        let id = (r.clone(), boot.clone());
        assert_eq!(run.get_or_insert_with(|| id.clone()), &id, "one stream");
        assert_eq!(*start, bytes.len() as u64, "events are contiguous");
        bytes.extend_from_slice(chunk);
        assert_eq!(*end, bytes.len() as u64, "range length is the chunk length");
    }
    let (run, boot) = run.expect("log output streamed");
    (run, boot, bytes)
}

async fn cursor_reply(h: &Harness, id: &str) -> (Value, Vec<u8>) {
    let reply = h
        .services
        .script_output_cursor(h.ws.clone(), id.to_owned(), Some(10_000))
        .await
        .expect("cursor output");
    let data = decode(reply["data"].as_str().expect("base64 data"));
    (reply, data)
}

/// `script:output` positions continue across a service auto-restart (the
/// run's log is appended to), restart from 0 on a new run, and agree with
/// the `cursor: true` reply byte for byte. Restart notices are not in the
/// log and carry no positions.
#[intent_test_macros::daemon_test]
async fn output_positions_span_auto_restarts_and_match_the_cursor_reply() {
    let mut h = harness().await;
    h.services = h.services.clone().with_script_too_fast_ms(0);
    let mut sub = subscribe(&h);
    let dir = tempfile::tempdir().expect("tempdir");
    let flag = dir.path().join("flag");
    let cmd = format!(
        "if [ -e \"{p}\" ]; then i=0; while [ $i -lt 300 ]; do echo second-$i; i=$((i+1)); done; echo SECOND-DONE; exec {KEEPALIVE}; \
         else : > \"{p}\"; i=0; while [ $i -lt 300 ]; do echo first-$i; i=$((i+1)); done; fi",
        p = flag.display()
    );
    let id = create(&h, "restarts", &cmd, ScriptMode::Service).await;
    h.services
        .script_start(h.ws.clone(), id.clone())
        .await
        .expect("start");
    let events = output_events_until(&mut sub, b"SECOND-DONE").await;
    let (run, boot, stream) = contiguous_run(&events);
    assert!(contains(&stream, b"first-299") && contains(&stream, b"second-299"));
    let notices: Vec<_> = events.iter().filter(|e| e.cursor.is_none()).collect();
    assert!(
        notices.iter().any(|e| contains(&e.bytes, b"Restarting")),
        "the restart notice streams without positions"
    );
    assert!(
        !contains(&stream, b"Restarting"),
        "notices are not log output"
    );

    let (reply, data) = cursor_reply(&h, &id).await;
    assert_eq!(reply["runId"], run.as_str());
    assert_eq!(reply["daemonBootId"], boot.as_str());
    assert_eq!(reply["startOffset"], "0");
    assert_eq!(reply["logStartOffset"], "0");
    assert_eq!(reply["endOffset"], stream.len().to_string());
    assert_eq!(
        data, stream,
        "the reply holds exactly the streamed log bytes"
    );

    // A new run is a new stream from 0.
    h.services
        .script_stop(h.ws.clone(), id.clone())
        .await
        .expect("stop");
    h.services
        .script_start(h.ws.clone(), id.clone())
        .await
        .expect("restart");
    let events = output_events_until(&mut sub, b"SECOND-DONE").await;
    let fresh: Vec<_> = events
        .into_iter()
        .filter(|e| e.cursor.as_ref().is_some_and(|c| c.0 != run))
        .collect();
    let (new_run, _, new_stream) = contiguous_run(&fresh);
    let (reply, data) = cursor_reply(&h, &id).await;
    assert_eq!(reply["runId"], new_run.as_str());
    assert_eq!(data, new_stream);
    h.services
        .script_stop(h.ws.clone(), id)
        .await
        .expect("stop");
}

/// A snapshot read while output streams ends on an event boundary: the
/// events at or past its `endOffset` continue it with no byte repeated or
/// missing.
#[intent_test_macros::daemon_test]
async fn a_mid_stream_snapshot_joins_live_events_exactly() {
    let h = harness().await;
    let mut sub = subscribe(&h);
    let cmd = format!(
        "i=0; while [ $i -lt 400 ]; do echo tick-$i; i=$((i+1)); sleep 0.002; done; echo TICKS-DONE; exec {KEEPALIVE}"
    );
    let id = create(&h, "ticks", &cmd, ScriptMode::Service).await;
    h.services
        .script_start(h.ws.clone(), id.clone())
        .await
        .expect("start");
    let mut events = output_events_until(&mut sub, b"tick-50\n").await;
    let (reply, data) = cursor_reply(&h, &id).await;
    events.extend(output_events_until(&mut sub, b"TICKS-DONE").await);
    let (run, _, stream) = contiguous_run(&events);
    assert_eq!(reply["runId"], run.as_str());
    let end = position(&reply, "endOffset");
    assert_eq!(position(&reply, "startOffset"), 0);
    assert_eq!(
        data,
        &stream[..data.len()],
        "the snapshot is the stream's prefix"
    );
    assert_eq!(data.len() as u64, end);
    assert!(
        events
            .iter()
            .filter_map(|e| e.cursor.as_ref())
            .all(|c| c.3 <= end || c.2 >= end),
        "no live chunk straddles the snapshot end"
    );
    let mut joined = data.clone();
    for e in &events {
        if e.cursor.as_ref().is_some_and(|c| c.2 >= end) {
            joined.extend_from_slice(&e.bytes);
        }
    }
    assert_eq!(joined, stream, "snapshot + later chunks rebuild the stream");
    h.services
        .script_stop(h.ws.clone(), id)
        .await
        .expect("stop");
}
