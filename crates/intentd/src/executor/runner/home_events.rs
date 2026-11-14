//! File and git events of a home (monorepo `docs/protocol/node-link.md`,
//! "File and git events of a home", nodeProtocol 19), machine side: each
//! open home checkout is watched under its hidden record once its setup
//! is done, and the record's file and git events (the watch's, and those
//! the home calls emit) are relayed to the backend as `homeEvents`.
//!
//! Bounded: one frame per home every [`FLUSH_EVERY`] at most, with at most
//! [`MAX_HOME_EVENTS`] events; only the latest `changes:git-status` is kept,
//! and `file:*` events past the cap collapse into one burst summary. Best
//! effort: events while no link speaks [`HOMES_PROTOCOL`] are dropped, and
//! each bind refreshes every open home's git status instead. A closed home
//! sends nothing more. Data is shown as a home call's result is: paths
//! relative to the checkout, the record as the backend's workspace.

use std::collections::HashMap;
use std::sync::{Arc, Weak};
use std::time::Duration;

use serde_json::{json, Value};
use tokio::sync::broadcast;

use intent_core::node_link::{
    HomeEvent, HomeEvents, LinkFrame, HOMES_PROTOCOL, HOME_EVENT_TYPES, MAX_FRAME_BYTES,
    MAX_HOME_EVENTS,
};
use intent_core::{Event, HomeId, WorkspaceId};

use super::home_calls::{shown_roots, Hidden};
use super::homes::Home;
use super::{lock, Inner, Runner};

/// The least time between two `homeEvents` of one home.
pub(super) const FLUSH_EVERY: Duration = Duration::from_millis(250);

/// What one frame's events may take, below [`MAX_FRAME_BYTES`] with room
/// for the frame's own members.
const FRAME_BUDGET: usize = MAX_FRAME_BYTES - 64 * 1024;

const GIT_STATUS: &str = "changes:git-status";

fn is_file(event: &HomeEvent) -> bool {
    event.event_type.starts_with("file:")
}

/// A home's events waiting for its next frame.
#[derive(Debug, Default)]
pub(super) struct Batch {
    events: Vec<HomeEvent>,
    /// `file:*` events past the cap, counted into one burst summary.
    burst: Option<Burst>,
}

#[derive(Debug, Default, Clone, Copy)]
struct Burst {
    creates: u64,
    deletes: u64,
    modifies: u64,
}

impl Burst {
    /// Count `event` (a `file:*` event, or a burst summary of its own).
    fn count(&mut self, event: &HomeEvent) {
        let data = &event.data;
        if data.get("burst").and_then(Value::as_bool) == Some(true) {
            let n = |key: &str| data.get(key).and_then(Value::as_u64).unwrap_or(0);
            self.creates += n("creates");
            self.deletes += n("deletes");
            self.modifies += n("modifies");
            return;
        }
        match data.get("action").and_then(Value::as_str) {
            Some("create") => self.creates += 1,
            Some("delete") => self.deletes += 1,
            _ => self.modifies += 1,
        }
    }

    /// The summary, as the file watcher's own burst summary of the
    /// checkout's root.
    fn summary(self) -> HomeEvent {
        HomeEvent {
            event_type: "file:changed".to_owned(),
            data: json!({
                "path": ".",
                "relativePath": ".",
                "action": "modify",
                "burst": true,
                "affectedCount": self.creates + self.deletes + self.modifies,
                "creates": self.creates,
                "deletes": self.deletes,
                "modifies": self.modifies,
            }),
        }
    }
}

impl Batch {
    /// Add `event`: a `changes:git-status` replaces the one waiting, and a
    /// `file:*` event past the cap turns the waiting ones into a burst.
    pub(super) fn push(&mut self, event: HomeEvent) {
        if event.event_type == GIT_STATUS {
            self.events
                .retain(|waiting| waiting.event_type != GIT_STATUS);
        }
        let file = is_file(&event);
        if file {
            if let Some(burst) = self.burst.as_mut() {
                burst.count(&event);
                return;
            }
        }
        // One place stays free for a burst summary.
        if self.events.len() + 1 < MAX_HOME_EVENTS {
            self.events.push(event);
            return;
        }
        if file || self.events.iter().any(is_file) {
            let (files, rest): (Vec<_>, Vec<_>) = std::mem::take(&mut self.events)
                .into_iter()
                .partition(is_file);
            self.events = rest;
            let mut burst = Burst::default();
            for waiting in &files {
                burst.count(waiting);
            }
            if file {
                burst.count(&event);
            } else {
                self.events.push(event);
            }
            self.burst = Some(burst);
            return;
        }
        // Only git events wait, as many as a frame holds: the oldest goes.
        tracing::debug!("node: a home's git events overflowed; the oldest is dropped");
        self.events.remove(0);
        self.events.push(event);
    }

    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.events.is_empty() && self.burst.is_none()
    }

    /// The waiting events, oldest first, the burst summary last.
    pub(super) fn take(&mut self) -> Vec<HomeEvent> {
        let mut events = std::mem::take(&mut self.events);
        if let Some(burst) = self.burst.take() {
            events.push(burst.summary());
        }
        events
    }
}

/// One home's waiting events.
struct Waiting {
    home: Arc<Home>,
    record: WorkspaceId,
    batch: Batch,
}

impl Runner {
    /// Watch `home`'s checkout under its `record` once its setup is done,
    /// until it closes, and relay what the watch and its calls emit.
    pub(super) async fn watch_home_events(&self, home: &Arc<Home>, record: WorkspaceId) {
        let Some(records) = self.inner.home_records.clone() else {
            return;
        };
        let mut done = home.setup_done.subscribe();
        let mut closed = home.closed.subscribe();
        tokio::select! {
            _ = done.wait_for(|done| *done) => {}
            _ = closed.wait_for(|closed| *closed) => return,
        }
        let Some(watch) = records.watch(&record, &home.worktree) else {
            return;
        };
        self.start_home_relay();
        let mut slot = lock(&home.events_watch);
        if *home.closed.borrow() {
            return;
        }
        *slot = Some(watch);
    }

    /// Start the relay of this backend's homes, once.
    fn start_home_relay(&self) {
        if self
            .inner
            .home_relay
            .swap(true, std::sync::atomic::Ordering::AcqRel)
        {
            return;
        }
        let Some(events) = self
            .inner
            .home_records
            .as_ref()
            .and_then(|records| records.events())
        else {
            return;
        };
        let inner = Arc::downgrade(&self.inner);
        // caller-binding: allow — node-local relay of one backend's home events
        tokio::spawn(relay(inner, events));
    }

    /// Have every open home's git status sent again soon (after a bind, or
    /// when the relay lost events).
    pub(in crate::executor) fn refresh_home_events(&self) {
        let Some(records) = self.inner.home_records.as_ref() else {
            return;
        };
        let homes: Vec<Arc<Home>> = lock(&self.inner.homes).open.values().cloned().collect();
        for home in homes {
            let record = home
                .record
                .try_lock()
                .ok()
                .and_then(|record| record.clone());
            if let Some(record) = record {
                if lock(&home.events_watch).is_some() {
                    records.refresh(&record);
                }
            }
        }
    }

    /// Queue `event` for its home, when it is a relayed type of an open
    /// home's record.
    fn collect_home_event(&self, waiting: &mut HashMap<HomeId, Waiting>, event: &Event) {
        if !HOME_EVENT_TYPES.contains(&event.event_type.as_str()) {
            return;
        }
        let home = lock(&self.inner.homes)
            .open
            .values()
            .find(|home| {
                home.record
                    .try_lock()
                    .is_ok_and(|record| record.as_ref() == Some(&event.workspace_id))
            })
            .cloned();
        let Some(home) = home.filter(|home| !*home.closed.borrow()) else {
            return;
        };
        waiting
            .entry(home.of.home_id.clone())
            .or_insert_with(|| Waiting {
                home,
                record: event.workspace_id.clone(),
                batch: Batch::default(),
            })
            .batch
            .push(HomeEvent {
                event_type: event.event_type.clone(),
                data: event.data.clone(),
            });
    }

    /// Send every home's waiting events; those of a closed home, or with
    /// no link that speaks homes, are dropped.
    async fn flush_home_events(&self, waiting: &mut HashMap<HomeId, Waiting>) {
        let link = self
            .inner
            .head
            .get()
            .filter(|link| link.protocol() >= HOMES_PROTOCOL);
        for (_, mut entry) in waiting.drain() {
            let events = entry.batch.take();
            if events.is_empty() || *entry.home.closed.borrow() {
                continue;
            }
            let Some(link) = link.as_ref() else {
                continue;
            };
            let hidden = Hidden::of_home(
                shown_roots(&entry.home.worktree),
                &entry.record,
                &entry.home.of.workspace_id,
            );
            let events: Vec<HomeEvent> = events
                .into_iter()
                .map(|event| HomeEvent {
                    event_type: event.event_type,
                    data: hidden.value(event.data),
                })
                .collect();
            for events in frames(events) {
                // Closed while the earlier frames went: nothing more.
                if *entry.home.closed.borrow() {
                    break;
                }
                let frame = LinkFrame::HomeEvents(HomeEvents {
                    workspace_id: entry.home.of.workspace_id.clone(),
                    home_id: entry.home.of.home_id.clone(),
                    events,
                });
                if let Err(error) = link.send(&frame).await {
                    tracing::debug!(home = %entry.home.of.home_id, %error, "node: home events not sent; dropped");
                    break;
                }
            }
        }
    }
}

/// `events` in frames of at most [`FRAME_BUDGET`] bytes each; an event
/// that alone does not fit is dropped.
fn frames(events: Vec<HomeEvent>) -> Vec<Vec<HomeEvent>> {
    let mut frames = Vec::new();
    let mut current = Vec::new();
    let mut size = 0;
    for event in events {
        let len = serde_json::to_vec(&event).map_or(usize::MAX, |bytes| bytes.len() + 1);
        if len > FRAME_BUDGET {
            tracing::debug!(kind = %event.event_type, "node: a home event too large for a frame; dropped");
            continue;
        }
        if size + len > FRAME_BUDGET && !current.is_empty() {
            frames.push(std::mem::take(&mut current));
            size = 0;
        }
        size += len;
        current.push(event);
    }
    if !current.is_empty() {
        frames.push(current);
    }
    frames
}

/// Relay the home records' events of the runner `inner` until it or the
/// event bus is gone.
async fn relay(inner: Weak<Inner>, mut events: broadcast::Receiver<Arc<Event>>) {
    let mut waiting: HashMap<HomeId, Waiting> = HashMap::new();
    let mut tick = tokio::time::interval(FLUSH_EVERY);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            got = events.recv() => {
                let Some(inner) = inner.upgrade() else { return };
                let runner = Runner { inner };
                match got {
                    Ok(event) => runner.collect_home_event(&mut waiting, &event),
                    Err(broadcast::error::RecvError::Lagged(lost)) => {
                        tracing::info!(lost, "node: home events were lost; refreshing the homes' git status");
                        runner.refresh_home_events();
                    }
                    Err(broadcast::error::RecvError::Closed) => return,
                }
            }
            _ = tick.tick(), if !waiting.is_empty() => {
                let Some(inner) = inner.upgrade() else { return };
                Runner { inner }.flush_home_events(&mut waiting).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file(path: &str, action: &str) -> HomeEvent {
        HomeEvent {
            event_type: if action == "create" {
                "file:created"
            } else {
                "file:changed"
            }
            .into(),
            data: json!({ "path": path, "relativePath": path, "action": action }),
        }
    }

    fn status(n: u64) -> HomeEvent {
        HomeEvent {
            event_type: GIT_STATUS.into(),
            data: json!({ "workspaceId": "r", "status": { "uncommittedCount": n } }),
        }
    }

    #[test]
    fn a_batch_keeps_the_latest_git_status_in_order() {
        let mut batch = Batch::default();
        batch.push(status(1));
        batch.push(file("a", "create"));
        batch.push(status(2));
        let events = batch.take();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].data["path"], "a");
        assert_eq!(events[1].data["status"]["uncommittedCount"], 2);
        assert!(batch.is_empty());
    }

    #[test]
    fn file_events_past_the_cap_collapse_into_one_burst() {
        let mut batch = Batch::default();
        batch.push(HomeEvent {
            event_type: "git:commit".into(),
            data: json!({ "commit": "c1" }),
        });
        for n in 0..(MAX_HOME_EVENTS * 3) {
            let action = if n % 3 == 0 { "create" } else { "modify" };
            batch.push(file(&format!("f{n}"), action));
        }
        batch.push(status(0));
        let events = batch.take();
        assert!(events.len() <= MAX_HOME_EVENTS, "{}", events.len());
        assert_eq!(events[0].event_type, "git:commit");
        assert_eq!(events[1].event_type, GIT_STATUS);
        let burst = &events[2].data;
        assert_eq!(events.len(), 3);
        assert_eq!(burst["burst"], true);
        assert_eq!(burst["path"], ".");
        assert_eq!(burst["affectedCount"], MAX_HOME_EVENTS * 3);
        assert_eq!(burst["creates"], MAX_HOME_EVENTS as u64);
        assert!(batch.is_empty());
        // A burst of its own counts as its files.
        let mut again = Batch::default();
        for n in 0..MAX_HOME_EVENTS {
            again.push(file(&format!("g{n}"), "delete"));
        }
        again.push(events[2].clone());
        let summary = again.take().pop().unwrap();
        assert_eq!(summary.data["deletes"], MAX_HOME_EVENTS as u64);
        assert_eq!(summary.data["affectedCount"], MAX_HOME_EVENTS as u64 * 4);
    }

    #[test]
    fn git_events_never_pass_the_cap() {
        let mut batch = Batch::default();
        for n in 0..(MAX_HOME_EVENTS + 10) {
            batch.push(HomeEvent {
                event_type: "git:commit".into(),
                data: json!({ "commit": format!("c{n}") }),
            });
        }
        let events = batch.take();
        assert_eq!(events.len(), MAX_HOME_EVENTS - 1);
        assert_eq!(
            events.last().unwrap().data["commit"],
            format!("c{}", MAX_HOME_EVENTS + 9)
        );
    }

    #[test]
    fn frames_stay_under_the_budget() {
        let big = "x".repeat(FRAME_BUDGET / 3);
        let events: Vec<HomeEvent> = (0..4)
            .map(|n| HomeEvent {
                event_type: "git:commit".into(),
                data: json!({ "commit": n, "message": big }),
            })
            .chain(std::iter::once(HomeEvent {
                event_type: "git:commit".into(),
                data: json!({ "message": "y".repeat(FRAME_BUDGET) }),
            }))
            .collect();
        let frames = frames(events);
        assert_eq!(frames.iter().map(Vec::len).collect::<Vec<_>>(), [2, 2]);
        for events in frames {
            let frame = LinkFrame::HomeEvents(HomeEvents {
                workspace_id: WorkspaceId::from("ws-1"),
                home_id: HomeId::from("h1"),
                events,
            });
            assert!(frame.encode().is_ok());
        }
    }
}
