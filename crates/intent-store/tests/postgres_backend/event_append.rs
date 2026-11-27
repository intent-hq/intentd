//! The event append in a domain write's transaction, on both backends: the
//! appended event commits with the row or not at all, and only a commit
//! hands it to the [`CommittedEvents`] hook, once, in seq order, after the
//! row is visible. On Postgres the hand-off runs before the writer slot is
//! released, so the next writer of the process commits after it. The
//! adopter is `Store::set_workspace_browser_client_with_event`.
use std::sync::{Arc, Mutex};
use std::time::Duration;

use intent_core::{ClientId, Error, Event, EventActor, Workspace, WorkspaceId};
use intent_store::{CommittedEvents, EventSeq, NewEvent, Store};
use intentd_test_support::{test_timeout, PausePoint};
use serde_json::json;

use crate::harness::{open, parity_test, test_db, TestDb};

/// Event type the fault trigger refuses: the append fails after the row
/// update, inside the same transaction.
const FAULT: &str = "test:append-fault";

fn ws() -> WorkspaceId {
    WorkspaceId("ws-pin".into())
}

fn client(id: &str) -> ClientId {
    ClientId::from_string(id)
}

/// A pin delta as the service builds it, or of `event_type`.
fn delta(event_type: &str, pin: &str) -> NewEvent {
    NewEvent {
        workspace_id: ws(),
        timestamp: "2026-11-27T00:00:00Z".into(),
        event_type: event_type.into(),
        actor: EventActor::default(),
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data: json!({ "workspaceId": "ws-pin", "changes": { "browserClientId": pin } }),
    }
}

async fn seed_workspace(store: &Store) {
    store
        .insert_workspace(&Workspace {
            id: ws(),
            title: "pin".into(),
            branch: "main".into(),
            created_at: "2026-11-27T00:00:00Z".into(),
            updated_at: "2026-11-27T00:00:00Z".into(),
            ..Workspace::default()
        })
        .await
        .expect("seed the workspace");
}

/// Records every hand-off.
#[derive(Default)]
struct Recorder(Mutex<Vec<Vec<(Event, EventSeq)>>>);

impl CommittedEvents for Recorder {
    fn committed(&self, events: &[(Event, EventSeq)]) {
        self.0.lock().unwrap().push(events.to_vec());
    }
}

impl Recorder {
    fn calls(&self) -> Vec<Vec<(Event, EventSeq)>> {
        self.0.lock().unwrap().clone()
    }
}

/// The events of `id` in the log.
async fn logged(store: &Store, id: &str) -> usize {
    store
        .events_after(0, -1)
        .await
        .unwrap()
        .iter()
        .filter(|(event, _)| event.id == id)
        .count()
}

async fn pin_deltas(store: &Store) -> usize {
    store
        .events_after(0, -1)
        .await
        .unwrap()
        .iter()
        .filter(|(event, _)| event.workspace_id == ws())
        .count()
}

/// Makes the append of a [`FAULT`] event fail on this run's backend.
async fn install_fault(store: &Store, db: Option<&TestDb>) {
    if let Some(db) = db {
        sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
            "CREATE OR REPLACE FUNCTION fail_append() RETURNS trigger LANGUAGE plpgsql AS $$ \
             BEGIN IF NEW.event_type = '{FAULT}' THEN RAISE EXCEPTION 'injected append failure'; \
             END IF; RETURN NEW; END $$; \
             DROP TRIGGER IF EXISTS fail_append ON event; \
             CREATE TRIGGER fail_append BEFORE INSERT ON event \
             FOR EACH ROW EXECUTE FUNCTION fail_append();"
        )))
        .execute(&db.admin)
        .await
        .expect("install the fault trigger");
    } else {
        sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
            "CREATE TRIGGER fail_append BEFORE INSERT ON event \
             WHEN NEW.event_type = '{FAULT}' \
             BEGIN SELECT RAISE(ABORT, 'injected append failure'); END;"
        )))
        .execute(store.write_pool())
        .await
        .expect("install the fault trigger");
    }
}

parity_test!(a_committed_append_hands_off_once_in_seq_order, |store| {
    seed_workspace(&store).await;
    let recorder = Recorder::default();

    let (first, first_seq) = store
        .set_workspace_browser_client_with_event(
            &ws(),
            Some(&client("desktop-a")),
            &delta("workspace:updated", "desktop-a"),
            &recorder,
        )
        .await
        .expect("pin A");
    let (second, second_seq) = store
        .set_workspace_browser_client_with_event(
            &ws(),
            Some(&client("desktop-b")),
            &delta("workspace:updated", "desktop-b"),
            &recorder,
        )
        .await
        .expect("pin B");

    assert!(first_seq < second_seq, "seq follows commit order");
    assert_eq!(
        recorder.calls(),
        vec![
            vec![(first.clone(), first_seq)],
            vec![(second.clone(), second_seq)]
        ],
        "each commit hands its event off once, in seq order"
    );
    assert_eq!(store.max_event_seq().await.unwrap(), second_seq);
    for event in [&first, &second] {
        assert_eq!(logged(&store, &event.id).await, 1, "inserted once");
    }
    assert_eq!(
        store.workspace_browser_client(&ws()).await.unwrap(),
        Some(client("desktop-b")),
        "the row committed with its event"
    );
});

parity_test!(
    a_rolled_back_append_hands_off_nothing,
    |store, _tenant, db| {
        seed_workspace(&store).await;
        let recorder = Recorder::default();
        store
            .set_workspace_browser_client_with_event(
                &ws(),
                Some(&client("desktop-a")),
                &delta("workspace:updated", "desktop-a"),
                &recorder,
            )
            .await
            .expect("pin A");

        // No row to update: nothing is appended or handed off.
        let missing = store
            .set_workspace_browser_client_with_event(
                &WorkspaceId("ws-missing".into()),
                None,
                &delta("workspace:updated", "none"),
                &recorder,
            )
            .await;
        assert!(matches!(missing, Err(Error::NotFound(_))), "{missing:?}");

        // The append fails after the row update: the update rolls back too.
        install_fault(&store, db).await;
        let failed = store
            .set_workspace_browser_client_with_event(
                &ws(),
                Some(&client("desktop-b")),
                &delta(FAULT, "desktop-b"),
                &recorder,
            )
            .await;
        assert!(failed.is_err(), "the append failed: {failed:?}");

        assert_eq!(recorder.calls().len(), 1, "only pin A was handed off");
        assert_eq!(pin_deltas(&store).await, 1, "only pin A's event is logged");
        assert_eq!(
            store.workspace_browser_client(&ws()).await.unwrap(),
            Some(client("desktop-a")),
            "the failed append rolled the pin back"
        );
    }
);

/// Holds its first hand-off at a pause point, recording every hand-off.
struct HeldHandOff {
    first: PausePoint,
    calls: Recorder,
}

impl CommittedEvents for HeldHandOff {
    fn committed(&self, events: &[(Event, EventSeq)]) {
        self.calls.committed(events);
        if self.first.arrivals() == 0 {
            self.first.pause_blocking();
        }
    }
}

/// Two stores of one tenant in one process share the writer slot. While
/// A's hand-off runs, B's write waits for the slot, so B commits and hands
/// off after A: hand-offs follow seq order within the process.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_next_writer_of_the_process_waits_for_a_hand_off() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let a = open(&db, "tenant").await;
    let b = open(&db, "tenant").await;
    seed_workspace(&a).await;
    let hand_off = Arc::new(HeldHandOff {
        first: PausePoint::unshared("first hand-off"),
        calls: Recorder::default(),
    });

    let mut writer_a = tokio::spawn({
        let (a, hand_off) = (a.clone(), hand_off.clone());
        async move {
            a.set_workspace_browser_client_with_event(
                &ws(),
                Some(&client("desktop-a")),
                &delta("workspace:updated", "desktop-a"),
                hand_off.as_ref(),
            )
            .await
        }
    });
    assert!(
        hand_off.first.reached(&mut writer_a).await.is_none(),
        "A is held in its hand-off"
    );

    let writer_b = tokio::spawn({
        let (b, hand_off) = (b.clone(), hand_off.clone());
        async move {
            b.set_workspace_browser_client_with_event(
                &ws(),
                Some(&client("desktop-b")),
                &delta("workspace:updated", "desktop-b"),
                hand_off.as_ref(),
            )
            .await
        }
    });
    tokio::time::timeout(test_timeout(Duration::from_secs(30)), async {
        while a.postgres_writers_waiting() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("B queues for the writer slot");
    assert!(!writer_b.is_finished(), "B wrote during A's hand-off");
    assert_eq!(hand_off.calls.calls().len(), 1);

    hand_off.first.release();
    let (_, seq_a) = writer_a.await.unwrap().expect("pin A");
    let (_, seq_b) = tokio::time::timeout(test_timeout(Duration::from_secs(30)), writer_b)
        .await
        .expect("B finishes once A's hand-off returned")
        .unwrap()
        .expect("pin B");
    let handed: Vec<EventSeq> = hand_off
        .calls
        .calls()
        .iter()
        .flatten()
        .map(|(_, seq)| *seq)
        .collect();
    assert_eq!(handed, vec![seq_a, seq_b]);
    assert!(seq_a < seq_b);
}
