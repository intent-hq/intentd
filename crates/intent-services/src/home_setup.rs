//! How far a workspace home's opening has got on its machine (nodes.md
//! "Opening the home"): `workspace.create` returns once the home is
//! recorded, and the machine then fetches, makes the home checkout and runs
//! the setup script in the background. Kept in memory per `homeId`, never
//! stored; served as `WorkspaceHome.setup`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use intent_core::remote_launch::{RemoteSetupState, RemoteSetupStatus};
use intent_core::{HomeId, WorkspaceHome, WorkspaceId};

/// The output a home's setup is kept with (`setup.output`), as an agent's.
const OUTPUT_TAIL_BYTES: usize = 256 * 1024;

/// While output arrives, `workspace:updated` goes out at most this often.
const OUTPUT_PUBLISH_EVERY: Duration = Duration::from_secs(2);

/// How a home's setup finished, from its machine's `setupFinished` record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HomeSetupFinished {
    pub state: RemoteSetupState,
    pub exit_code: Option<i32>,
    pub error: Option<String>,
    pub finished_at: String,
}

struct Entry {
    home_id: HomeId,
    status: RemoteSetupStatus,
    /// How the open went, beyond `status`.
    open: Open,
    output: String,
    truncated: bool,
    published: Option<Instant>,
    /// A trailing announcement of throttled output is scheduled.
    trailing: bool,
}

/// How a home's open went, beyond its `status`.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Open {
    /// Opened (or opening) as usual.
    Normal,
    /// The open itself failed (not the setup script). The machine may still
    /// have made the checkout (its reply was lost), so its setup records
    /// still apply; the next open at the machine's bind reports again.
    Failed,
    /// Opened again after a failed open, and no output has come since: a
    /// `ready` answer then means the machine already held the checkout.
    Reopened,
    /// Taken as ready after a reopen answered `ready` with no output: setup
    /// records that still come move it on.
    AssumedReady,
}

impl Entry {
    fn is(&self, home_id: &HomeId) -> bool {
        &self.home_id == home_id
    }

    /// Whether the machine's setup records may still move this entry on:
    /// not once a setup it reported has finished.
    fn takes_records(&self) -> bool {
        matches!(
            self.status.state,
            RemoteSetupState::Pending | RemoteSetupState::Running
        ) || matches!(self.open, Open::Failed | Open::AssumedReady)
    }

    fn set(&mut self, status: RemoteSetupStatus) {
        self.status = status;
        self.open = Open::Normal;
    }
}

/// When to announce kept output (see [`HomeSetups::output`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputAnnounce {
    /// Now.
    Now,
    /// After this delay, then call [`HomeSetups::flush_output`].
    After(Duration),
    /// Not now: an announcement is already scheduled, or the output was
    /// dropped.
    No,
}

/// Every home's opening this backend lifetime has seen, by workspace.
/// Shared across `Services` clones. Each change returns whether the home's
/// `setup` changed, so the caller announces it. Memory stays bounded: one
/// entry per workspace, a ready home keeps no output, and a deleted or
/// archived workspace's entry is forgotten.
#[derive(Clone, Default)]
pub struct HomeSetups(Arc<Mutex<HashMap<WorkspaceId, Entry>>>);

impl HomeSetups {
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<WorkspaceId, Entry>> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn with<T>(
        &self,
        workspace_id: &WorkspaceId,
        home_id: &HomeId,
        default: T,
        f: impl FnOnce(&mut Entry) -> T,
    ) -> T {
        let mut map = self.lock();
        match map.get_mut(workspace_id).filter(|e| e.is(home_id)) {
            Some(entry) => f(entry),
            None => default,
        }
    }

    /// `home_id` is being opened for the first time (a new home at
    /// `workspace.create`): `pending`, with no output yet.
    pub fn opening(&self, workspace_id: &WorkspaceId, home_id: &HomeId) {
        self.lock().insert(
            workspace_id.clone(),
            Entry {
                home_id: home_id.clone(),
                status: RemoteSetupStatus::pending(),
                open: Open::Normal,
                output: String::new(),
                truncated: false,
                published: None,
                trailing: false,
            },
        );
    }

    /// The home is opened again (at its machine's bind). Only a home whose
    /// open failed goes back to `pending`.
    #[must_use = "true when the home's setup changed: announce it"]
    pub fn reopening(&self, workspace_id: &WorkspaceId, home_id: &HomeId) -> bool {
        self.with(workspace_id, home_id, false, |entry| {
            if entry.open != Open::Failed {
                return false;
            }
            entry.set(RemoteSetupStatus::pending());
            entry.open = Open::Reopened;
            entry.output.clear();
            entry.truncated = false;
            true
        })
    }

    /// The machine answered the open `ready`. A first open's setup records
    /// follow. A reopen that saw no checkout output found the checkout the
    /// machine already held, which runs nothing again: the home is ready
    /// (setup records that still come move it on).
    #[must_use = "true when the home's setup changed: announce it"]
    pub fn opened(&self, workspace_id: &WorkspaceId, home_id: &HomeId) -> bool {
        self.with(workspace_id, home_id, false, |entry| {
            if entry.open != Open::Reopened || entry.status.state != RemoteSetupState::Pending {
                return false;
            }
            entry.set(RemoteSetupStatus {
                state: RemoteSetupState::Completed,
                finished_at: Some(intent_core::now_iso()),
                ..RemoteSetupStatus::pending()
            });
            entry.open = Open::AssumedReady;
            entry.output = String::new();
            entry.truncated = false;
            true
        })
    }

    /// The machine could not open the home (or could not be reached, or its
    /// answer was lost): `failed` with `error`, while it was `pending`.
    #[must_use = "true when the home's setup changed: announce it"]
    pub fn open_failed(&self, workspace_id: &WorkspaceId, home_id: &HomeId, error: &str) -> bool {
        self.with(workspace_id, home_id, false, |entry| {
            if entry.status.state != RemoteSetupState::Pending {
                return false;
            }
            entry.set(RemoteSetupStatus {
                state: RemoteSetupState::Failed,
                error: Some(RemoteSetupStatus::capped_error(error)),
                finished_at: Some(intent_core::now_iso()),
                ..RemoteSetupStatus::pending()
            });
            entry.open = Open::Failed;
            true
        })
    }

    /// The checkout exists and the setup script started (`setupStarted`).
    /// A replayed record after the setup finished changes nothing.
    #[must_use = "true when the home's setup changed: announce it"]
    pub fn setup_started(
        &self,
        workspace_id: &WorkspaceId,
        home_id: &HomeId,
        started_at: &str,
    ) -> bool {
        self.with(workspace_id, home_id, false, |entry| {
            if !entry.takes_records() || entry.status.state == RemoteSetupState::Running {
                return false;
            }
            entry.set(RemoteSetupStatus {
                state: RemoteSetupState::Running,
                started_at: Some(started_at.to_owned()),
                ..RemoteSetupStatus::pending()
            });
            true
        })
    }

    /// The setup finished (`setupFinished`): completed, skipped or failed.
    /// A ready home drops its output.
    #[must_use = "true when the home's setup changed: announce it"]
    pub fn setup_finished(
        &self,
        workspace_id: &WorkspaceId,
        home_id: &HomeId,
        finished: HomeSetupFinished,
    ) -> bool {
        self.with(workspace_id, home_id, false, |entry| {
            if !entry.takes_records() {
                return false;
            }
            let started_at = (entry.status.state == RemoteSetupState::Running)
                .then(|| entry.status.started_at.clone())
                .flatten();
            let ready = matches!(
                finished.state,
                RemoteSetupState::Completed | RemoteSetupState::Skipped
            );
            entry.set(RemoteSetupStatus {
                state: finished.state,
                exit_code: finished.exit_code,
                error: finished
                    .error
                    .as_deref()
                    .map(RemoteSetupStatus::capped_error),
                started_at,
                finished_at: Some(finished.finished_at),
                ..RemoteSetupStatus::pending()
            });
            if ready {
                entry.output = String::new();
                entry.truncated = false;
            }
            true
        })
    }

    /// Keep a `setupOutput` chunk (`seq` 0 starts the output over), and say
    /// when to announce it: at most once every [`OUTPUT_PUBLISH_EVERY`],
    /// with one trailing announcement for output that arrives in between.
    /// A ready home keeps no output.
    #[must_use = "announce the output as it says"]
    pub fn output(
        &self,
        workspace_id: &WorkspaceId,
        home_id: &HomeId,
        seq: u64,
        data: &str,
        now: Instant,
    ) -> OutputAnnounce {
        self.with(workspace_id, home_id, OutputAnnounce::No, |entry| {
            if matches!(
                entry.status.state,
                RemoteSetupState::Completed | RemoteSetupState::Skipped
            ) && entry.open != Open::AssumedReady
            {
                return OutputAnnounce::No;
            }
            if entry.open == Open::Reopened {
                entry.open = Open::Normal;
            }
            if seq == 0 {
                entry.output.clear();
                entry.truncated = false;
            }
            entry.output.push_str(data);
            if entry.output.len() > OUTPUT_TAIL_BYTES {
                let mut cut = entry.output.len() - OUTPUT_TAIL_BYTES;
                while !entry.output.is_char_boundary(cut) {
                    cut += 1;
                }
                entry.output.drain(..cut);
                entry.truncated = true;
            }
            if entry.trailing {
                return OutputAnnounce::No;
            }
            match entry.published.map(|at| now.duration_since(at)) {
                Some(since) if since < OUTPUT_PUBLISH_EVERY => {
                    entry.trailing = true;
                    OutputAnnounce::After(OUTPUT_PUBLISH_EVERY.saturating_sub(since))
                }
                _ => {
                    entry.published = Some(now);
                    OutputAnnounce::Now
                }
            }
        })
    }

    /// The trailing announcement [`OutputAnnounce::After`] scheduled is
    /// due: whether to announce (the home is still this one).
    #[must_use = "true when the home's setup output should be announced"]
    pub fn flush_output(&self, workspace_id: &WorkspaceId, home_id: &HomeId, now: Instant) -> bool {
        self.with(workspace_id, home_id, false, |entry| {
            if !entry.trailing {
                return false;
            }
            entry.trailing = false;
            entry.published = Some(now);
            true
        })
    }

    /// Forget the workspace's home opening: the workspace was deleted or
    /// archived.
    pub fn forget(&self, workspace_id: &WorkspaceId) {
        self.lock().remove(workspace_id);
    }

    /// The home's `setup`, with its output on a detail read.
    #[must_use]
    pub fn status(
        &self,
        workspace_id: &WorkspaceId,
        home_id: &HomeId,
        detail: bool,
    ) -> Option<RemoteSetupStatus> {
        let map = self.lock();
        let entry = map.get(workspace_id).filter(|e| e.is(home_id))?;
        let mut status = entry.status.clone();
        if detail {
            status.output = Some(entry.output.clone());
            status.output_truncated = entry.truncated.then_some(true);
        }
        Some(status)
    }

    /// Add the home's `setup` to `home` as served.
    pub fn attach(&self, workspace_id: &WorkspaceId, home: &mut WorkspaceHome, detail: bool) {
        home.setup = self.status(workspace_id, &home.home_id, detail);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ids() -> (WorkspaceId, HomeId) {
        (WorkspaceId::from("ws-1"), HomeId::from("home-1"))
    }

    fn state(setups: &HomeSetups, ws: &WorkspaceId, home: &HomeId) -> Option<RemoteSetupState> {
        setups.status(ws, home, false).map(|s| s.state)
    }

    fn finished(state: RemoteSetupState, exit_code: Option<i32>) -> HomeSetupFinished {
        HomeSetupFinished {
            state,
            exit_code,
            error: None,
            finished_at: "2026-11-18T00:00:02Z".into(),
        }
    }

    #[test]
    fn a_home_nobody_opened_has_no_setup() {
        let (ws, home) = ids();
        let setups = HomeSetups::default();
        assert!(!setups.setup_started(&ws, &home, "t"));
        assert!(!setups.open_failed(&ws, &home, "boom"));
        assert!(!setups.reopening(&ws, &home));
        assert_eq!(setups.status(&ws, &home, true), None);
    }

    #[test]
    fn opening_runs_setup_then_is_ready() {
        let (ws, home) = ids();
        let setups = HomeSetups::default();
        setups.opening(&ws, &home);
        assert_eq!(state(&setups, &ws, &home), Some(RemoteSetupState::Pending));
        assert!(setups.setup_started(&ws, &home, "2026-11-18T00:00:01Z"));
        let running = setups.status(&ws, &home, false).unwrap();
        assert_eq!(running.state, RemoteSetupState::Running);
        assert_eq!(running.started_at.as_deref(), Some("2026-11-18T00:00:01Z"));
        assert!(setups.setup_finished(&ws, &home, finished(RemoteSetupState::Completed, Some(0))));
        let done = setups.status(&ws, &home, false).unwrap();
        assert_eq!(done.state, RemoteSetupState::Completed);
        assert_eq!(done.exit_code, Some(0));
        assert_eq!(done.started_at.as_deref(), Some("2026-11-18T00:00:01Z"));
        // A replayed record changes nothing.
        assert!(!setups.setup_started(&ws, &home, "later"));
        assert_eq!(
            state(&setups, &ws, &home),
            Some(RemoteSetupState::Completed)
        );
    }

    #[test]
    fn a_workspace_without_a_setup_script_is_skipped() {
        let (ws, home) = ids();
        let setups = HomeSetups::default();
        setups.opening(&ws, &home);
        assert!(setups.setup_finished(&ws, &home, finished(RemoteSetupState::Skipped, None)));
        assert_eq!(state(&setups, &ws, &home), Some(RemoteSetupState::Skipped));
    }

    #[test]
    fn a_failed_setup_script_keeps_its_exit_code() {
        let (ws, home) = ids();
        let setups = HomeSetups::default();
        setups.opening(&ws, &home);
        let _ = setups.setup_started(&ws, &home, "t");
        assert!(setups.setup_finished(&ws, &home, finished(RemoteSetupState::Failed, Some(2))));
        let failed = setups.status(&ws, &home, false).unwrap();
        assert_eq!(failed.state, RemoteSetupState::Failed);
        assert_eq!(failed.exit_code, Some(2));
        // Not an open failure: the next bind's open does not report again.
        assert!(!setups.reopening(&ws, &home));
    }

    #[test]
    fn a_failed_open_fails_and_the_next_open_reports_again() {
        let (ws, home) = ids();
        let setups = HomeSetups::default();
        setups.opening(&ws, &home);
        assert!(setups.open_failed(&ws, &home, "node-checkout-failed: fetch\nfailed"));
        let failed = setups.status(&ws, &home, false).unwrap();
        assert_eq!(failed.state, RemoteSetupState::Failed);
        assert_eq!(
            failed.error.as_deref(),
            Some("node-checkout-failed: fetch failed")
        );
        assert_eq!(failed.exit_code, None);
        assert!(setups.reopening(&ws, &home));
        assert_eq!(state(&setups, &ws, &home), Some(RemoteSetupState::Pending));
        // Reopening a pending home changes nothing.
        assert!(!setups.reopening(&ws, &home));
    }

    #[test]
    fn a_running_setup_is_not_an_open_failure() {
        let (ws, home) = ids();
        let setups = HomeSetups::default();
        setups.opening(&ws, &home);
        let _ = setups.setup_started(&ws, &home, "t");
        assert!(!setups.open_failed(&ws, &home, "late"));
        assert_eq!(state(&setups, &ws, &home), Some(RemoteSetupState::Running));
    }

    #[test]
    fn another_home_of_the_workspace_has_no_setup() {
        let (ws, home) = ids();
        let setups = HomeSetups::default();
        setups.opening(&ws, &home);
        let moved = HomeId::from("home-2");
        assert_eq!(setups.status(&ws, &moved, false), None);
        assert!(!setups.setup_started(&ws, &moved, "t"));
    }

    #[test]
    fn output_is_on_detail_reads_only_and_announced_at_most_every_two_seconds() {
        let (ws, home) = ids();
        let setups = HomeSetups::default();
        setups.opening(&ws, &home);
        let start = Instant::now();
        let at = |secs| start + Duration::from_secs(secs);
        assert_eq!(
            setups.output(&ws, &home, 0, "Fetching\n", start),
            OutputAnnounce::Now
        );
        // Output within 2 s schedules one trailing announcement.
        assert_eq!(
            setups.output(&ws, &home, 1, "Checking out\n", at(1)),
            OutputAnnounce::After(Duration::from_secs(1))
        );
        assert_eq!(
            setups.output(&ws, &home, 2, "Done\n", at(1)),
            OutputAnnounce::No
        );
        assert!(setups.flush_output(&ws, &home, at(2)));
        assert!(!setups.flush_output(&ws, &home, at(2)), "flushed once");
        assert_eq!(setups.status(&ws, &home, false).unwrap().output, None);
        let detail = setups.status(&ws, &home, true).unwrap();
        assert_eq!(
            detail.output.as_deref(),
            Some("Fetching\nChecking out\nDone\n")
        );
        assert_eq!(detail.output_truncated, None);
        // seq 0 starts over.
        assert_eq!(
            setups.output(&ws, &home, 0, "again\n", at(9)),
            OutputAnnounce::Now
        );
        assert_eq!(
            setups.status(&ws, &home, true).unwrap().output.as_deref(),
            Some("again\n")
        );
    }

    #[test]
    fn a_ready_home_keeps_no_output() {
        let (ws, home) = ids();
        let setups = HomeSetups::default();
        setups.opening(&ws, &home);
        let _ = setups.output(&ws, &home, 0, "lots of output\n", Instant::now());
        assert!(setups.setup_finished(&ws, &home, finished(RemoteSetupState::Completed, Some(0))));
        assert_eq!(
            setups.status(&ws, &home, true).unwrap().output.as_deref(),
            Some("")
        );
        assert_eq!(
            setups.output(&ws, &home, 9, "late\n", Instant::now()),
            OutputAnnounce::No
        );
        assert_eq!(
            setups.status(&ws, &home, true).unwrap().output.as_deref(),
            Some("")
        );
    }

    #[test]
    fn a_lost_open_reply_still_takes_the_machines_setup_records() {
        let (ws, home) = ids();
        let setups = HomeSetups::default();
        setups.opening(&ws, &home);
        // The link dropped before the reply, but the machine made the checkout.
        assert!(setups.open_failed(&ws, &home, "node-link-closed"));
        assert!(setups.setup_started(&ws, &home, "t"));
        assert_eq!(state(&setups, &ws, &home), Some(RemoteSetupState::Running));
        assert!(setups.setup_finished(&ws, &home, finished(RemoteSetupState::Completed, Some(0))));
        assert_eq!(
            state(&setups, &ws, &home),
            Some(RemoteSetupState::Completed)
        );
        // The next bind's reopen does not go back to pending.
        assert!(!setups.reopening(&ws, &home));
    }

    #[test]
    fn a_reopen_answered_ready_for_a_held_checkout_is_ready() {
        let (ws, home) = ids();
        let setups = HomeSetups::default();
        setups.opening(&ws, &home);
        assert!(setups.open_failed(&ws, &home, "node-link-closed"));
        assert!(setups.reopening(&ws, &home));
        // The machine held the checkout: ready at once, no output, no records.
        assert!(setups.opened(&ws, &home));
        assert_eq!(
            state(&setups, &ws, &home),
            Some(RemoteSetupState::Completed)
        );
        // Records that still come (a replay of the first open's) move it on.
        assert!(setups.setup_finished(&ws, &home, finished(RemoteSetupState::Failed, Some(1))));
        assert_eq!(state(&setups, &ws, &home), Some(RemoteSetupState::Failed));
    }

    #[test]
    fn a_reopen_that_makes_the_checkout_again_waits_for_its_setup() {
        let (ws, home) = ids();
        let setups = HomeSetups::default();
        setups.opening(&ws, &home);
        assert!(setups.open_failed(&ws, &home, "node-checkout-failed: fetch"));
        assert!(setups.reopening(&ws, &home));
        let _ = setups.output(&ws, &home, 0, "Fetching\n", Instant::now());
        assert!(!setups.opened(&ws, &home));
        assert_eq!(state(&setups, &ws, &home), Some(RemoteSetupState::Pending));
        // A first open's ready answer waits for the records too.
        setups.opening(&ws, &home);
        assert!(!setups.opened(&ws, &home));
        assert_eq!(state(&setups, &ws, &home), Some(RemoteSetupState::Pending));
    }

    #[test]
    fn a_forgotten_workspace_has_no_setup() {
        let (ws, home) = ids();
        let setups = HomeSetups::default();
        setups.opening(&ws, &home);
        setups.forget(&ws);
        assert_eq!(setups.status(&ws, &home, true), None);
    }

    #[test]
    fn output_keeps_the_last_256_kib() {
        let (ws, home) = ids();
        let setups = HomeSetups::default();
        setups.opening(&ws, &home);
        let now = Instant::now();
        let _ = setups.output(&ws, &home, 0, &"a".repeat(OUTPUT_TAIL_BYTES), now);
        let _ = setups.output(&ws, &home, 1, "tail", now);
        let detail = setups.status(&ws, &home, true).unwrap();
        let output = detail.output.unwrap();
        assert_eq!(output.len(), OUTPUT_TAIL_BYTES);
        assert!(output.ends_with("tail"));
        assert_eq!(detail.output_truncated, Some(true));
    }

    #[test]
    fn attach_serves_setup_on_the_home() {
        let (ws, home_id) = ids();
        let setups = HomeSetups::default();
        setups.opening(&ws, &home_id);
        let mut home = WorkspaceHome {
            home_id: home_id.clone(),
            node_id: Some("m-1".into()),
            cloud_machine_id: None,
            name: "Mac Studio".into(),
            state: intent_core::HomeState::Attached,
            since: "t".into(),
            last_backup_at: None,
            backup_warning: false,
            setup: None,
        };
        setups.attach(&ws, &mut home, false);
        let json = serde_json::to_value(&home).unwrap();
        assert_eq!(json["setup"], serde_json::json!({ "state": "pending" }));
        home.home_id = HomeId::from("home-2");
        setups.attach(&ws, &mut home, false);
        assert!(serde_json::to_value(&home).unwrap().get("setup").is_none());
    }
}
