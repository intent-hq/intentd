//! Replay proofs against the production reader/fanout path, without sleeps.
use super::*;
use std::io::Cursor;

fn fanout(cap: usize) -> Arc<Mutex<Fanout>> {
    let (tx, _) = broadcast::channel(2);
    Arc::new(Mutex::new(Fanout {
        scrollback: Scrollback::new(cap),
        tx,
    }))
}

fn output(fanout: &Arc<Mutex<Fanout>>, bytes: &[u8]) {
    read_loop(Box::new(Cursor::new(bytes.to_vec())), fanout);
}

fn replay(rendered: &mut Vec<u8>, next: &mut u64, chunk: &OutputChunk) {
    assert_eq!(
        chunk.end_offset - chunk.start_offset,
        chunk.bytes.len() as u64
    );
    if chunk.end_offset <= *next {
        return;
    }
    assert!(
        chunk.start_offset <= *next,
        "gap must trigger a fresh snapshot"
    );
    rendered
        .extend_from_slice(&chunk.bytes[usize::try_from(*next - chunk.start_offset).unwrap()..]);
    *next = chunk.end_offset;
}

#[test]
fn identical_bytes_delayed_on_either_side_of_snapshot_are_not_duplicates() {
    for first_before_response in [false, true] {
        for second_before_response in [false, true] {
            let fanout = fanout(64);
            let mut live = fanout.lock().unwrap().tx.subscribe();
            output(&fanout, b"same");
            let first = live.try_recv().unwrap();
            let snapshot = fanout
                .lock()
                .unwrap()
                .scrollback
                .positioned_snapshot(usize::MAX);
            output(&fanout, b"same");
            let second = live.try_recv().unwrap();
            let mut pending = Vec::new();
            let mut delayed = Vec::new();
            for (before, event) in [
                (first_before_response, first),
                (second_before_response, second),
            ] {
                if before {
                    pending.push(event);
                } else {
                    delayed.push(event);
                }
            }
            let mut rendered = snapshot.bytes;
            let mut next = snapshot.end_offset;
            for event in pending.into_iter().chain(delayed) {
                replay(&mut rendered, &mut next, &event);
                // At-least-once delivery is harmless too.
                replay(&mut rendered, &mut next, &event);
            }
            assert_eq!(rendered, b"samesame");
            assert_eq!(next, 8);
        }
    }
}

#[test]
fn partial_overlap_wrap_and_tail_limits_preserve_absolute_positions() {
    let fanout = fanout(6);
    let mut live = fanout.lock().unwrap().tx.subscribe();
    output(&fanout, b"abcdef");
    let first = live.try_recv().unwrap();
    output(&fanout, b"ghij");
    let second = live.try_recv().unwrap();
    let guard = fanout.lock().unwrap();
    let snapshot = guard.scrollback.positioned_snapshot(usize::MAX);
    assert_eq!((snapshot.start_offset, snapshot.end_offset), (4, 10));
    assert_eq!(snapshot.bytes, b"efghij");
    for cap in [0, 2, 6, 100] {
        let tail = guard.scrollback.positioned_snapshot(cap);
        assert_eq!(tail.end_offset, 10);
        assert_eq!(tail.start_offset, 10 - tail.bytes.len() as u64);
    }
    drop(guard);
    let mut rendered = b"abc".to_vec();
    let mut next = 3;
    replay(&mut rendered, &mut next, &first);
    replay(&mut rendered, &mut next, &second);
    assert_eq!(rendered, b"abcdefghij");
    // A reconnect at offset 3 cannot recover byte 3 from this ring: explicit gap.
    assert!(snapshot.start_offset > 3);
}

#[test]
fn lag_is_visible_as_a_gap_and_snapshot_restores_the_retained_suffix() {
    let fanout = fanout(4);
    let mut live = fanout.lock().unwrap().tx.subscribe();
    for byte in b"abcdef" {
        output(&fanout, &[*byte]);
    }
    assert!(matches!(
        live.try_recv(),
        Err(broadcast::error::TryRecvError::Lagged(_))
    ));
    let event = live.try_recv().unwrap();
    assert!(event.start_offset > 0);
    let snapshot = fanout
        .lock()
        .unwrap()
        .scrollback
        .positioned_snapshot(usize::MAX);
    assert_eq!((snapshot.start_offset, snapshot.end_offset), (2, 6));
    assert_eq!(snapshot.bytes, b"cdef");
    let mut rendered = snapshot.bytes;
    let mut next = snapshot.end_offset;
    replay(&mut rendered, &mut next, &event);
    assert_eq!(rendered, b"cdef");
}

#[test]
fn zero_retention_and_clear_do_not_reset_offsets() {
    let mut scrollback = Scrollback::new(0);
    scrollback.push(b"abc");
    let empty = scrollback.positioned_snapshot(usize::MAX);
    assert_eq!((empty.start_offset, empty.end_offset), (3, 3));
    scrollback.clear();
    scrollback.push(b"d");
    assert_eq!(scrollback.positioned_snapshot(0).end_offset, 4);
}

#[test]
fn fresh_hosts_have_distinct_boot_scopes() {
    let old = PtyHost::new();
    let new = PtyHost::new();
    assert_ne!(old.daemon_boot_id(), new.daemon_boot_id());
    assert_eq!(old.daemon_boot_id(), old.daemon_boot_id());
}

#[test]
fn binary_chunk_larger_than_ring_keeps_its_full_event_range() {
    let fanout = fanout(3);
    let mut live = fanout.lock().unwrap().tx.subscribe();
    let bytes = b"\x00\xff\xc3\xa9\x1b[0m";
    output(&fanout, bytes);
    let event = live.try_recv().unwrap();
    assert_eq!(event.bytes, bytes);
    assert_eq!((event.start_offset, event.end_offset), (0, 8));
    let snapshot = fanout.lock().unwrap().scrollback.positioned_snapshot(2);
    assert_eq!(snapshot.bytes, b"0m");
    assert_eq!((snapshot.start_offset, snapshot.end_offset), (6, 8));
}
