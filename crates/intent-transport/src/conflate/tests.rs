//! Unit tests for the egress conflation buffer and per-type merge semantics.

use super::*;
use intent_core::events::{
    AGENT_STREAM_END, FILE_CHANGED, FILE_CREATED, NOTE_UPDATED, TERMINAL_EXIT,
};
use intent_core::{ActorType, EventActor, WorkspaceId};

fn event(event_type: &str, session_id: Option<&str>, data: Value) -> Event {
    Event {
        id: "ev-1".to_string(),
        workspace_id: WorkspaceId::from("ws-1"),
        timestamp: "2026-08-11T00:00:00Z".to_string(),
        event_type: event_type.to_string(),
        actor: EventActor {
            actor_type: ActorType::System,
            ..Default::default()
        },
        session_id: session_id.map(str::to_string),
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data,
    }
}

fn chunk(text: &str) -> String {
    BASE64.encode(text.as_bytes())
}

fn frame_data(frame: &str) -> Value {
    let v: Value = serde_json::from_str(frame).unwrap();
    v["params"]["event"]["data"].clone()
}

fn frame_type(frame: &str) -> String {
    let v: Value = serde_json::from_str(frame).unwrap();
    v["params"]["event"]["type"].as_str().unwrap().to_string()
}

// --- event_key: the conflatable set and its keys ---

#[test]
fn event_key_maps_the_conflatable_set() {
    let activity = event(AGENT_STREAM_ACTIVITY, Some("ag-1"), json!({}));
    assert_eq!(event_key(&activity), Some(Key::Activity("ag-1".into())));

    let terminal = event(TERMINAL_DATA, None, json!({ "terminalId": "t-1" }));
    assert_eq!(
        event_key(&terminal),
        Some(Key::Terminal("ws-1".into(), "t-1".into()))
    );

    let file = event(FILE_CHANGED, None, json!({ "path": "src/a.rs" }));
    assert_eq!(
        event_key(&file),
        Some(Key::File("ws-1".into(), "src/a.rs".into()))
    );
}

#[test]
fn event_key_refuses_everything_outside_the_conflatable_set() {
    for t in [
        NOTE_UPDATED,
        TERMINAL_EXIT,
        AGENT_STREAM_END,
        "task:updated",
    ] {
        let ev = event(t, Some("ag-1"), json!({ "path": "p", "terminalId": "t" }));
        assert_eq!(event_key(&ev), None, "{t} must be a barrier");
    }
}

#[test]
fn activity_key_falls_back_to_agent_id_in_data() {
    let ev = event(AGENT_STREAM_ACTIVITY, None, json!({ "agentId": "ag-2" }));
    assert_eq!(event_key(&ev), Some(Key::Activity("ag-2".into())));
}

// --- latest-wins merges ---

#[test]
fn activity_conflates_latest_wins_per_agent() {
    let mut buf: ConflationBuffer<EventItem> = ConflationBuffer::new();
    for (agent, n) in [("ag-1", 1), ("ag-2", 1), ("ag-1", 2), ("ag-1", 3)] {
        let ev = event(AGENT_STREAM_ACTIVITY, Some(agent), json!({ "n": n }));
        let key = event_key(&ev).unwrap();
        assert!(buf.push(key.clone(), EventItem::new(&key, ev)).is_none());
    }
    let first = buf.pop().unwrap().into_frame("sub-1");
    let second = buf.pop().unwrap().into_frame("sub-1");
    assert!(buf.pop().is_none());
    // ag-1 kept its first-arrival position but carries the newest payload.
    assert_eq!(frame_data(&first)["n"], json!(3));
    assert_eq!(frame_data(&second)["n"], json!(1));
}

#[test]
fn file_conflates_per_workspace_path_latest_wins() {
    let mut buf: ConflationBuffer<EventItem> = ConflationBuffer::new();
    let older = event(
        FILE_CREATED,
        None,
        json!({ "path": "a.rs", "action": "create" }),
    );
    let other = event(
        FILE_CHANGED,
        None,
        json!({ "path": "b.rs", "action": "modify" }),
    );
    let newer = event(
        FILE_CHANGED,
        None,
        json!({ "path": "a.rs", "action": "modify" }),
    );
    for ev in [older, other, newer] {
        let key = event_key(&ev).unwrap();
        assert!(buf.push(key.clone(), EventItem::new(&key, ev)).is_none());
    }
    let first = buf.pop().unwrap().into_frame("sub-1");
    let second = buf.pop().unwrap().into_frame("sub-1");
    assert!(buf.pop().is_none());
    // A later file:changed supersedes the pending file:created for the path
    // (latest event type wins wholesale — refetch-trigger semantics).
    assert_eq!(frame_type(&first), FILE_CHANGED);
    assert_eq!(frame_data(&first)["action"], json!("modify"));
    assert_eq!(frame_data(&second)["path"], json!("b.rs"));
}

#[test]
fn burst_summaries_conflate_per_directory() {
    let mut buf: ConflationBuffer<EventItem> = ConflationBuffer::new();
    let b1 = event(
        FILE_CHANGED,
        None,
        json!({ "path": "src", "burst": true, "affectedCount": 120 }),
    );
    let b2 = event(
        FILE_CHANGED,
        None,
        json!({ "path": "src", "burst": true, "affectedCount": 250 }),
    );
    for ev in [b1, b2] {
        let key = event_key(&ev).unwrap();
        assert!(buf.push(key.clone(), EventItem::new(&key, ev)).is_none());
    }
    let only = buf.pop().unwrap().into_frame("sub-1");
    assert!(buf.pop().is_none());
    assert_eq!(frame_data(&only)["affectedCount"], json!(250));
}

// --- terminal concat ---

#[test]
fn terminal_chunks_concatenate_in_arrival_order() {
    let mut buf: ConflationBuffer<EventItem> = ConflationBuffer::new();
    for text in ["hello ", "wor", "ld"] {
        let ev = event(
            TERMINAL_DATA,
            None,
            json!({ "terminalId": "t-1", "chunk": chunk(text) }),
        );
        let key = event_key(&ev).unwrap();
        assert!(buf.push(key.clone(), EventItem::new(&key, ev)).is_none());
    }
    let only = buf.pop().unwrap().into_frame("sub-1");
    assert!(buf.pop().is_none());
    let merged = frame_data(&only)["chunk"].as_str().unwrap().to_string();
    assert_eq!(BASE64.decode(merged).unwrap(), b"hello world");
}

#[test]
fn terminal_chunks_do_not_merge_across_terminals() {
    let mut buf: ConflationBuffer<EventItem> = ConflationBuffer::new();
    for (tid, text) in [("t-1", "aa"), ("t-2", "bb"), ("t-1", "cc")] {
        let ev = event(
            TERMINAL_DATA,
            None,
            json!({ "terminalId": tid, "chunk": chunk(text) }),
        );
        let key = event_key(&ev).unwrap();
        assert!(buf.push(key.clone(), EventItem::new(&key, ev)).is_none());
    }
    let first = buf.pop().unwrap().into_frame("sub-1");
    let second = buf.pop().unwrap().into_frame("sub-1");
    assert!(buf.pop().is_none());
    let d1 = frame_data(&first);
    let d2 = frame_data(&second);
    assert_eq!(d1["terminalId"], json!("t-1"));
    assert_eq!(
        BASE64.decode(d1["chunk"].as_str().unwrap()).unwrap(),
        b"aacc"
    );
    assert_eq!(BASE64.decode(d2["chunk"].as_str().unwrap()).unwrap(), b"bb");
}

#[test]
fn oversized_terminal_merge_seals_the_entry_and_starts_a_new_one() {
    let mut buf: ConflationBuffer<EventItem> = ConflationBuffer::new();
    let big = "x".repeat(TERMINAL_CONCAT_CAP_BYTES - 1);
    for text in [big.as_str(), "yy", "zz"] {
        let ev = event(
            TERMINAL_DATA,
            None,
            json!({ "terminalId": "t-1", "chunk": chunk(text) }),
        );
        let key = event_key(&ev).unwrap();
        assert!(buf.push(key.clone(), EventItem::new(&key, ev)).is_none());
    }
    // The big chunk refused the "yy" merge (sealed); "zz" merged onto "yy".
    let first = buf.pop().unwrap().into_frame("sub-1");
    let second = buf.pop().unwrap().into_frame("sub-1");
    assert!(buf.pop().is_none());
    let first_bytes = BASE64
        .decode(frame_data(&first)["chunk"].as_str().unwrap())
        .unwrap();
    assert_eq!(first_bytes.len(), TERMINAL_CONCAT_CAP_BYTES - 1);
    assert_eq!(
        BASE64
            .decode(frame_data(&second)["chunk"].as_str().unwrap())
            .unwrap(),
        b"yyzz"
    );
}

#[test]
fn undecodable_terminal_chunk_passes_through_verbatim() {
    let mut buf: ConflationBuffer<EventItem> = ConflationBuffer::new();
    let bad = event(
        TERMINAL_DATA,
        None,
        json!({ "terminalId": "t-1", "chunk": "!!not-base64!!" }),
    );
    let good = event(
        TERMINAL_DATA,
        None,
        json!({ "terminalId": "t-1", "chunk": chunk("ok") }),
    );
    for ev in [bad, good] {
        let key = event_key(&ev).unwrap();
        assert!(buf.push(key.clone(), EventItem::new(&key, ev)).is_none());
    }
    let first = buf.pop().unwrap().into_frame("sub-1");
    let second = buf.pop().unwrap().into_frame("sub-1");
    assert!(buf.pop().is_none());
    assert_eq!(frame_data(&first)["chunk"], json!("!!not-base64!!"));
    assert_eq!(
        BASE64
            .decode(frame_data(&second)["chunk"].as_str().unwrap())
            .unwrap(),
        b"ok"
    );
}

// --- ordering across keys ---

#[test]
fn cross_key_order_is_first_arrival_order() {
    let mut buf: ConflationBuffer<EventItem> = ConflationBuffer::new();
    let evs = [
        event(FILE_CHANGED, None, json!({ "path": "a.rs", "n": 1 })),
        event(AGENT_STREAM_ACTIVITY, Some("ag-1"), json!({ "n": 2 })),
        event(
            TERMINAL_DATA,
            None,
            json!({ "terminalId": "t-1", "chunk": chunk("x") }),
        ),
        // Updates to earlier keys must not move them.
        event(FILE_CHANGED, None, json!({ "path": "a.rs", "n": 4 })),
        event(AGENT_STREAM_ACTIVITY, Some("ag-1"), json!({ "n": 5 })),
    ];
    for ev in evs {
        let key = event_key(&ev).unwrap();
        assert!(buf.push(key.clone(), EventItem::new(&key, ev)).is_none());
    }
    let order: Vec<String> = std::iter::from_fn(|| buf.pop())
        .map(|i| frame_type(&i.into_frame("s")))
        .collect();
    assert_eq!(order, [FILE_CHANGED, AGENT_STREAM_ACTIVITY, TERMINAL_DATA]);
}

// --- chat block deltas ---

fn chunk_delta(added: bool, block_id: &str, text: &str) -> Value {
    let entity = json!({
        "agentId": "ag-1",
        "messageId": "m-1",
        "role": "assistant",
        "block": { "type": "text", "id": block_id, "text": text },
    });
    if added {
        json!({ "added": [entity], "updated": [], "removedIds": [] })
    } else {
        json!({ "added": [], "updated": [entity], "removedIds": [] })
    }
}

#[test]
fn chat_blocks_conflate_latest_text_first_bucket() {
    let mut buf: ConflationBuffer<ChatItem> = ConflationBuffer::new();
    for delta in [
        chunk_delta(true, "m-1:0", "Hel"),
        chunk_delta(false, "m-1:0", "Hello"),
        chunk_delta(false, "m-1:0", "Hello world"),
    ] {
        let (key, item) = ChatItem::from_delta(&delta).unwrap();
        assert!(buf.push(key, item).is_none());
    }
    let only = buf.pop().unwrap().into_delta();
    assert!(buf.pop().is_none());
    // Newest full text, delivered in the FIRST pending delta's bucket (added).
    assert_eq!(only["added"][0]["block"]["text"], json!("Hello world"));
    assert!(only["updated"].as_array().unwrap().is_empty());
}

#[test]
fn chat_blocks_do_not_merge_across_blocks() {
    let mut buf: ConflationBuffer<ChatItem> = ConflationBuffer::new();
    for delta in [
        chunk_delta(true, "m-1:0", "one"),
        chunk_delta(true, "m-1:1", "two"),
        chunk_delta(false, "m-1:0", "one more"),
    ] {
        let (key, item) = ChatItem::from_delta(&delta).unwrap();
        assert!(buf.push(key, item).is_none());
    }
    let first = buf.pop().unwrap().into_delta();
    let second = buf.pop().unwrap().into_delta();
    assert!(buf.pop().is_none());
    assert_eq!(first["added"][0]["block"]["text"], json!("one more"));
    assert_eq!(second["added"][0]["block"]["text"], json!("two"));
}

fn incremental_chunk_delta(added: bool, block_id: &str, text_delta: &str) -> Value {
    let entity = json!({
        "agentId": "ag-1",
        "messageId": "m-1",
        "role": "assistant",
        "block": { "type": "text", "id": block_id, "textDelta": text_delta },
    });
    if added {
        json!({ "added": [entity], "updated": [], "removedIds": [] })
    } else {
        json!({ "added": [], "updated": [entity], "removedIds": [] })
    }
}

#[test]
fn incremental_chat_blocks_concatenate_fragments_in_arrival_order() {
    let mut buf: ConflationBuffer<ChatItem> = ConflationBuffer::new();
    for delta in [
        incremental_chunk_delta(true, "m-1:0", "Hel"),
        incremental_chunk_delta(false, "m-1:0", "lo "),
        incremental_chunk_delta(false, "m-1:0", "world"),
    ] {
        let (key, item) = ChatItem::from_delta(&delta).unwrap();
        assert!(buf.push(key, item).is_none());
    }
    let only = buf.pop().unwrap().into_delta();
    assert!(buf.pop().is_none());
    // All fragments composed, delivered in the FIRST pending delta's bucket.
    assert_eq!(only["added"][0]["block"]["textDelta"], json!("Hello world"));
    assert!(only["updated"].as_array().unwrap().is_empty());
}

#[test]
fn incremental_chat_blocks_do_not_merge_across_blocks() {
    let mut buf: ConflationBuffer<ChatItem> = ConflationBuffer::new();
    for delta in [
        incremental_chunk_delta(true, "m-1:0", "one"),
        incremental_chunk_delta(true, "m-1:1", "two"),
        incremental_chunk_delta(false, "m-1:0", " more"),
    ] {
        let (key, item) = ChatItem::from_delta(&delta).unwrap();
        assert!(buf.push(key, item).is_none());
    }
    let first = buf.pop().unwrap().into_delta();
    let second = buf.pop().unwrap().into_delta();
    assert!(buf.pop().is_none());
    assert_eq!(first["added"][0]["block"]["textDelta"], json!("one more"));
    assert_eq!(second["added"][0]["block"]["textDelta"], json!("two"));
}

#[test]
fn oversized_incremental_merge_seals_the_entry_and_starts_a_new_one() {
    let mut buf: ConflationBuffer<ChatItem> = ConflationBuffer::new();
    let big = "a".repeat(CHAT_TEXT_CONCAT_CAP_BYTES - 1);
    for delta in [
        incremental_chunk_delta(true, "m-1:0", &big),
        incremental_chunk_delta(false, "m-1:0", "bb"),
        incremental_chunk_delta(false, "m-1:0", "cc"),
    ] {
        let (key, item) = ChatItem::from_delta(&delta).unwrap();
        assert!(buf.push(key, item).is_none());
    }
    // The refused merge sealed the big entry; the later fragments merged into
    // a fresh tail entry, so no frame exceeds the cap and nothing is lost.
    let first = buf.pop().unwrap().into_delta();
    let second = buf.pop().unwrap().into_delta();
    assert!(buf.pop().is_none());
    assert_eq!(first["added"][0]["block"]["textDelta"], json!(big));
    assert_eq!(second["updated"][0]["block"]["textDelta"], json!("bbcc"));
}

#[test]
fn mixed_encoding_merge_is_refused_defensively() {
    let mut buf: ConflationBuffer<ChatItem> = ConflationBuffer::new();
    for delta in [
        chunk_delta(true, "m-1:0", "full"),
        incremental_chunk_delta(false, "m-1:0", "frag"),
    ] {
        let (key, item) = ChatItem::from_delta(&delta).unwrap();
        assert!(buf.push(key, item).is_none());
    }
    let first = buf.pop().unwrap().into_delta();
    let second = buf.pop().unwrap().into_delta();
    assert!(buf.pop().is_none());
    assert_eq!(first["added"][0]["block"]["text"], json!("full"));
    assert_eq!(second["updated"][0]["block"]["textDelta"], json!("frag"));
}

#[test]
fn multi_entity_and_removal_deltas_are_not_conflatable() {
    // Two entities (a tool_use + tool_result pair).
    let two = json!({
        "added": [
            { "block": { "id": "m-1:2" } },
            { "block": { "id": "m-1:3" } },
        ],
        "updated": [],
        "removedIds": [],
    });
    assert!(ChatItem::from_delta(&two).is_none());
    // A reconcile carrying removedIds.
    let removal = json!({
        "added": [],
        "updated": [{ "block": { "id": "m-1:0" } }],
        "removedIds": ["m-1:5"],
    });
    assert!(ChatItem::from_delta(&removal).is_none());
}

#[test]
fn chat_event_conflatable_only_for_chunk_deltas() {
    let delta = event(CHAT_STREAM_DELTA, Some("ag-1"), json!({}));
    assert!(chat_event_conflatable(&delta));
    for t in [AGENT_STREAM_END, "agent:tool:call", "agent:message"] {
        assert!(!chat_event_conflatable(&event(t, Some("ag-1"), json!({}))));
    }
}

// --- offer: passthrough vs congestion, and drain ---

#[tokio::test]
async fn offer_passes_through_when_uncongested() {
    let (tx, mut rx) = mpsc::channel::<String>(4);
    let mut buf: ConflationBuffer<EventItem> = ConflationBuffer::new();
    let ev = event(AGENT_STREAM_ACTIVITY, Some("ag-1"), json!({ "n": 1 }));
    let key = event_key(&ev).unwrap();
    let item = EventItem::new(&key, ev);
    let outcome = offer(&mut buf, key, item, &tx, |i| i.into_frame("sub-1"));
    assert!(matches!(outcome, Enqueue::Sent));
    assert!(buf.is_empty());
    assert_eq!(frame_data(&rx.try_recv().unwrap())["n"], json!(1));
}

#[tokio::test]
async fn offer_buffers_and_conflates_when_the_lane_is_full() {
    let (tx, mut rx) = mpsc::channel::<String>(1);
    tx.try_send("occupying".to_string()).unwrap();
    let mut buf: ConflationBuffer<EventItem> = ConflationBuffer::new();
    for n in 1..=3 {
        let ev = event(AGENT_STREAM_ACTIVITY, Some("ag-1"), json!({ "n": n }));
        let key = event_key(&ev).unwrap();
        let item = EventItem::new(&key, ev);
        let outcome = offer(&mut buf, key, item, &tx, |i| i.into_frame("sub-1"));
        assert!(matches!(outcome, Enqueue::Buffered));
    }
    assert!(!buf.is_empty());
    // Lane clears; drain flushes ONE conflated frame carrying the newest.
    assert_eq!(rx.recv().await.unwrap(), "occupying");
    assert!(buf.drain_all(&tx, |i| i.into_frame("sub-1")).await);
    assert!(buf.is_empty());
    assert_eq!(frame_data(&rx.recv().await.unwrap())["n"], json!(3));
    assert!(rx.try_recv().is_err(), "exactly one conflated frame");
}

#[tokio::test]
async fn barrier_flushes_conflated_frames_before_the_terminal_event() {
    // The forwarder's barrier path: drain the buffer with blocking sends,
    // THEN send the barrier — a conflated terminal:data frame always lands
    // before its stream's terminal:exit.
    let (tx, mut rx) = mpsc::channel::<String>(8);
    let mut buf: ConflationBuffer<EventItem> = ConflationBuffer::new();
    for text in ["a", "b"] {
        let ev = event(
            TERMINAL_DATA,
            None,
            json!({ "terminalId": "t-1", "chunk": chunk(text) }),
        );
        let key = event_key(&ev).unwrap();
        assert!(buf.push(key.clone(), EventItem::new(&key, ev)).is_none());
    }
    let exit = event(TERMINAL_EXIT, None, json!({ "terminalId": "t-1" }));
    assert_eq!(event_key(&exit), None, "terminal:exit is a barrier");
    assert!(buf.drain_all(&tx, |i| i.into_frame("sub-1")).await);
    tx.send(events::build_event_notification("sub-1", &exit))
        .await
        .unwrap();
    let first = rx.recv().await.unwrap();
    let second = rx.recv().await.unwrap();
    assert_eq!(frame_type(&first), TERMINAL_DATA);
    assert_eq!(
        BASE64
            .decode(frame_data(&first)["chunk"].as_str().unwrap())
            .unwrap(),
        b"ab"
    );
    assert_eq!(frame_type(&second), TERMINAL_EXIT);
}

// --- buffer bounds: unbounded distinct keys must not grow memory ---

#[test]
fn push_rejects_beyond_the_entry_bound() {
    let mut buf: ConflationBuffer<EventItem> = ConflationBuffer::bounded(2, usize::MAX);
    for agent in ["ag-1", "ag-2"] {
        let ev = event(AGENT_STREAM_ACTIVITY, Some(agent), json!({}));
        let key = event_key(&ev).unwrap();
        assert!(buf.push(key.clone(), EventItem::new(&key, ev)).is_none());
    }
    // A third distinct key is handed back untouched…
    let ev = event(AGENT_STREAM_ACTIVITY, Some("ag-3"), json!({}));
    let key = event_key(&ev).unwrap();
    assert!(buf.push(key.clone(), EventItem::new(&key, ev)).is_some());
    // …but a same-key merge still lands (no new entry).
    let ev = event(AGENT_STREAM_ACTIVITY, Some("ag-1"), json!({ "n": 2 }));
    let key = event_key(&ev).unwrap();
    assert!(buf.push(key.clone(), EventItem::new(&key, ev)).is_none());
    let first = buf.pop().unwrap().into_frame("sub-1");
    assert_eq!(frame_data(&first)["n"], json!(2));
}

#[test]
fn push_rejects_beyond_the_byte_bound() {
    let mut buf: ConflationBuffer<EventItem> = ConflationBuffer::bounded(usize::MAX, 64);
    let ev = event(
        TERMINAL_DATA,
        None,
        json!({ "terminalId": "t-1", "chunk": chunk("0123456789") }),
    );
    let key = event_key(&ev).unwrap();
    assert!(buf.push(key.clone(), EventItem::new(&key, ev)).is_none());
    // A push whose cost would exceed the byte bound is handed back.
    let big = "x".repeat(60);
    let ev = event(
        TERMINAL_DATA,
        None,
        json!({ "terminalId": "t-2", "chunk": chunk(&big) }),
    );
    let key = event_key(&ev).unwrap();
    assert!(buf.push(key.clone(), EventItem::new(&key, ev)).is_some());
    // Popping frees budget; the same push then lands.
    assert!(buf.pop().is_some());
    let ev = event(
        TERMINAL_DATA,
        None,
        json!({ "terminalId": "t-2", "chunk": chunk(&big) }),
    );
    let key = event_key(&ev).unwrap();
    assert!(buf.push(key.clone(), EventItem::new(&key, ev)).is_none());
}

#[tokio::test]
async fn offer_hands_back_overflow_at_capacity() {
    let (tx, _rx) = mpsc::channel::<String>(1);
    tx.try_send("occupying".to_string()).unwrap();
    let mut buf: ConflationBuffer<EventItem> = ConflationBuffer::bounded(1, usize::MAX);
    let ev = event(AGENT_STREAM_ACTIVITY, Some("ag-1"), json!({}));
    let key = event_key(&ev).unwrap();
    let item = EventItem::new(&key, ev);
    assert!(matches!(
        offer(&mut buf, key, item, &tx, |i| i.into_frame("sub-1")),
        Enqueue::Buffered
    ));
    let ev = event(AGENT_STREAM_ACTIVITY, Some("ag-2"), json!({ "n": 9 }));
    let key = event_key(&ev).unwrap();
    let item = EventItem::new(&key, ev);
    let outcome = offer(&mut buf, key, item, &tx, |i| i.into_frame("sub-1"));
    // The rejected item comes back intact for the caller's blocking path.
    let Enqueue::Overflow(item) = outcome else {
        panic!("expected Overflow");
    };
    assert_eq!(frame_data(&item.into_frame("sub-1"))["n"], json!(9));
}

#[tokio::test]
async fn offer_reports_a_closed_lane() {
    let (tx, rx) = mpsc::channel::<String>(1);
    drop(rx);
    let mut buf: ConflationBuffer<EventItem> = ConflationBuffer::new();
    let ev = event(AGENT_STREAM_ACTIVITY, Some("ag-1"), json!({}));
    let key = event_key(&ev).unwrap();
    let item = EventItem::new(&key, ev);
    let outcome = offer(&mut buf, key, item, &tx, |i| i.into_frame("sub-1"));
    assert!(matches!(outcome, Enqueue::Closed));
}
