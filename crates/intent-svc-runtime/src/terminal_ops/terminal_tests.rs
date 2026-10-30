use std::time::Duration;

use base64::Engine as _;
use intent_core::{WorkspaceApi, WorkspaceId};
use intent_store::Store;
use serde_json::Value;

use intent_services::feature_api::events::{EventBus, Subscription, SubscriptionFilter};
use intent_services::feature_api::test_support::fixtures::{workspace, TempDb};
use intent_services::feature_api::Services;

struct Harness {
    _tmp: TempDb,
    services: Services,
    bus: EventBus,
    ws: WorkspaceId,
}

async fn harness() -> Harness {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store.insert_workspace(&workspace(&ws)).await.expect("ws");
    let bus = EventBus::new(store.clone());
    let services = crate::test_runtime::with_runtime(Services::new_isolated(store))
        .with_event_bus(bus.clone());
    Harness {
        _tmp: tmp,
        services,
        bus,
        ws,
    }
}

fn subscribe(h: &Harness) -> Subscription {
    h.bus.subscribe(SubscriptionFilter {
        event_types: vec!["terminal:*".to_string()],
        workspace_id: Some(h.ws.0.clone()),
        ..Default::default()
    })
}

fn b64(s: &str) -> String {
    base64::engine::general_purpose::STANDARD.encode(s.as_bytes())
}

fn decode(s: &str) -> Vec<u8> {
    base64::engine::general_purpose::STANDARD.decode(s).unwrap()
}

/// Drain terminal events until one of `event_type` arrives, accumulating the
/// decoded `terminal:data` chunks seen along the way. Returns the matching
/// event and the accumulated data bytes.
async fn drain_until(
    sub: &mut Subscription,
    event_type: &str,
    timeout: Duration,
) -> (Value, Vec<u8>) {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut data = Vec::new();
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let batch = tokio::time::timeout(remaining, sub.recv())
            .await
            .expect("terminal event delivered")
            .expect("subscription open");
        for ev in &batch {
            let v = serde_json::to_value(ev).expect("serialize");
            if v["type"] == "terminal:data" {
                if let Some(chunk) = v["data"]["chunk"].as_str() {
                    data.extend_from_slice(&decode(chunk));
                }
            }
            if v["type"] == event_type {
                return (v, data);
            }
        }
    }
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack.windows(needle.len()).any(|w| w == needle)
}

/// create → write echoes back as `terminal:data`; two subscribers see the
/// same output; kill emits `terminal:exit`.
#[intent_test_macros::daemon_test]
async fn create_streams_data_to_all_subscribers_then_exit() {
    let h = harness().await;
    let mut a = subscribe(&h);
    let mut b = subscribe(&h);
    let created = h
        .services
        .terminal_create(h.ws.clone(), 80, 24, None, Some("cat".into()), None)
        .await
        .expect("create");
    let terminal_id = created["terminalId"]
        .as_str()
        .expect("terminalId")
        .to_string();

    h.services
        .terminal_write(terminal_id.clone(), b64("ping-stream\n"))
        .await
        .expect("write");

    let (_, from_a) = drain_until(&mut a, "terminal:data", Duration::from_secs(5)).await;
    let (_, from_b) = drain_until(&mut b, "terminal:data", Duration::from_secs(5)).await;
    assert!(contains(&from_a, b"ping-stream"), "subscriber a sees echo");
    assert!(contains(&from_b, b"ping-stream"), "subscriber b sees echo");

    h.services
        .terminal_kill(terminal_id.clone())
        .await
        .expect("kill");
    let (exit, _) = drain_until(&mut a, "terminal:exit", Duration::from_secs(5)).await;
    assert_eq!(exit["data"]["terminalId"], terminal_id);
}

/// `terminal.getBuffer` back-fills scrollback for a late attach (the echoed
/// input survives in the server-side buffer), then `terminal.list` reports it.
#[intent_test_macros::daemon_test]
async fn get_buffer_backfills_and_list_reports() {
    let h = harness().await;
    let mut sub = subscribe(&h);
    let created = h
        .services
        .terminal_create(h.ws.clone(), 80, 24, None, Some("cat".into()), None)
        .await
        .expect("create");
    let terminal_id = created["terminalId"].as_str().unwrap().to_string();

    h.services
        .terminal_write(terminal_id.clone(), b64("BUFFERED\n"))
        .await
        .expect("write");
    // Wait until the echo has streamed (so it is in scrollback too).
    drain_until(&mut sub, "terminal:data", Duration::from_secs(5)).await;

    let buf = h
        .services
        .terminal_get_buffer(terminal_id.clone(), None)
        .await
        .expect("getBuffer");
    assert_eq!(buf["terminalId"], terminal_id);
    let bytes = decode(buf["data"].as_str().expect("data"));
    assert!(contains(&bytes, b"BUFFERED"), "scrollback back-fill");

    h.services
        .terminal_resize(terminal_id.clone(), 120, 40)
        .await
        .expect("resize");

    let list = h.services.terminal_list(h.ws.clone()).await.expect("list");
    // Envelope shape: { terminals: [{ id, name, cwd, isExecutingCommand }],
    // daemonBootId } (monorepo#1334).
    assert!(
        list["daemonBootId"].as_str().is_some_and(|s| !s.is_empty()),
        "daemonBootId is a non-empty string: {list}"
    );
    let terminals = list["terminals"].as_array().expect("terminals array");
    let entry = terminals
        .iter()
        .find(|t| t["id"].as_str() == Some(terminal_id.as_str()))
        .expect("list contains terminal");
    assert_eq!(entry["name"], "Terminal");
    assert!(entry["cwd"].is_string(), "cwd is a string");
    assert!(
        entry["isExecutingCommand"].is_boolean(),
        "isExecutingCommand is a boolean"
    );

    h.services.terminal_kill(terminal_id).await.expect("kill");
}

/// `terminal.list` reports the same `daemonBootId` on every call within
/// one `Services` instance (one daemon boot) and a fresh id for a new
/// instance (a restart) — the authoritative-lifetime signal clients use to
/// tell a same-boot empty list from a post-restart one (monorepo#1334).
#[intent_test_macros::daemon_test]
async fn list_boot_id_stable_within_boot_and_fresh_across_boots() {
    let h1 = harness().await;
    let first = h1
        .services
        .terminal_list(h1.ws.clone())
        .await
        .expect("list");
    let second = h1
        .services
        .terminal_list(h1.ws.clone())
        .await
        .expect("list");
    let boot1 = first["daemonBootId"].as_str().expect("daemonBootId string");
    assert_eq!(
        boot1,
        second["daemonBootId"].as_str().unwrap(),
        "bootId stable across calls within one instance"
    );
    assert!(first["terminals"].is_array(), "terminals array present");

    let h2 = harness().await;
    let other = h2
        .services
        .terminal_list(h2.ws.clone())
        .await
        .expect("list");
    assert_ne!(
        boot1,
        other["daemonBootId"].as_str().unwrap(),
        "a new Services instance mints a fresh bootId"
    );
}

/// `terminal.readOutput` returns a formatted, ANSI-stripped string: a header
/// (`Terminal {id} (cwd: ...)`), a `─`×40 separator, then the trailing lines.
/// A freshly created terminal with no output returns the empty sentinel.
#[intent_test_macros::daemon_test]
async fn read_output_formats_scrollback_and_empty_sentinel() {
    let h = harness().await;
    let mut sub = subscribe(&h);
    let created = h
        .services
        .terminal_create(h.ws.clone(), 80, 24, None, Some("cat".into()), None)
        .await
        .expect("create");
    let terminal_id = created["terminalId"].as_str().unwrap().to_string();

    // No output yet -> sentinel string.
    let empty = h
        .services
        .terminal_read_output(h.ws.clone(), terminal_id.clone(), None, None, None)
        .await
        .expect("readOutput empty");
    assert_eq!(empty, serde_json::json!("Terminal has no output yet."));

    h.services
        .terminal_write(terminal_id.clone(), b64("READOUT-MARK\n"))
        .await
        .expect("write");
    drain_until(&mut sub, "terminal:data", Duration::from_secs(5)).await;

    let out = h
        .services
        .terminal_read_output(h.ws.clone(), terminal_id.clone(), Some(50), None, None)
        .await
        .expect("readOutput");
    let text = out.as_str().expect("readOutput is a bare string");
    assert!(
        text.starts_with(&format!("Terminal {terminal_id} (cwd: ")),
        "header present: {text}"
    );
    assert!(text.contains(&"\u{2500}".repeat(40)), "separator present");
    assert!(text.contains("READOUT-MARK"), "echoed output present");

    // Unknown id and cross-workspace access map to internal errors.
    let unknown = h
        .services
        .terminal_read_output(h.ws.clone(), "pty-99999".into(), None, None, None)
        .await
        .unwrap_err();
    assert!(matches!(unknown, intent_core::Error::Internal(_)));
    let other_ws = WorkspaceId::new();
    let wrong = h
        .services
        .terminal_read_output(other_ws, terminal_id.clone(), None, None, None)
        .await
        .unwrap_err();
    assert!(matches!(wrong, intent_core::Error::Internal(_)));

    h.services.terminal_kill(terminal_id).await.expect("kill");
}

/// A process that exits on its own surfaces its exit code on `terminal:exit`.
#[intent_test_macros::daemon_test]
async fn natural_exit_reports_exit_code() {
    let h = harness().await;
    let mut sub = subscribe(&h);
    // `false` exits with code 1 immediately.
    let created = h
        .services
        .terminal_create(h.ws.clone(), 80, 24, None, Some("false".into()), None)
        .await
        .expect("create");
    let terminal_id = created["terminalId"].as_str().unwrap().to_string();

    let (exit, _) = drain_until(&mut sub, "terminal:exit", Duration::from_secs(5)).await;
    assert_eq!(exit["data"]["terminalId"], terminal_id);
    assert_eq!(exit["data"]["exitCode"], 1);
}

/// Unknown terminal ids map to `NotFound`; bad base64 input maps to
/// `InvalidParams`.
#[intent_test_macros::daemon_test]
async fn unknown_id_and_bad_base64_error() {
    let h = harness().await;
    let err = h
        .services
        .terminal_get_buffer("pty-99999".into(), None)
        .await
        .unwrap_err();
    assert!(matches!(err, intent_core::Error::NotFound(_)));

    let created = h
        .services
        .terminal_create(h.ws.clone(), 80, 24, None, Some("cat".into()), None)
        .await
        .expect("create");
    let terminal_id = created["terminalId"].as_str().unwrap().to_string();
    let err = h
        .services
        .terminal_write(terminal_id.clone(), "!!not base64!!".into())
        .await
        .unwrap_err();
    assert!(matches!(err, intent_core::Error::InvalidParams(_)));
    h.services.terminal_kill(terminal_id).await.expect("kill");
}
