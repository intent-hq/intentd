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
    /// The open itself failed (not the setup script): the next open at the
    /// machine's bind makes the checkout again, so it reports again.
    open_failed: bool,
    output: String,
    truncated: bool,
    published: Option<Instant>,
}

impl Entry {
    fn is(&self, home_id: &HomeId) -> bool {
        &self.home_id == home_id
    }
}

/// Every home's opening this backend lifetime has seen, by workspace.
/// Shared across `Services` clones. Each change returns whether the home's
/// `setup` changed, so the caller announces it.
#[derive(Clone, Default)]
pub struct HomeSetups(Arc<Mutex<HashMap<WorkspaceId, Entry>>>);

impl HomeSetups {
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<WorkspaceId, Entry>> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// `home_id` is being opened for the first time (a new home at
    /// `workspace.create`): `pending`, with no output yet.
    pub fn opening(&self, workspace_id: &WorkspaceId, home_id: &HomeId) {
        self.lock().insert(
            workspace_id.clone(),
            Entry {
                home_id: home_id.clone(),
                status: RemoteSetupStatus::pending(),
                open_failed: false,
                output: String::new(),
                truncated: false,
                published: None,
            },
        );
    }

    /// The home is opened again (at its machine's bind). Only a home whose
    /// open failed goes back to `pending`: one whose checkout exists runs
    /// nothing again.
    #[must_use = "true when the home's setup changed: announce it"]
    pub fn reopening(&self, workspace_id: &WorkspaceId, home_id: &HomeId) -> bool {
        let mut map = self.lock();
        let Some(entry) = map.get_mut(workspace_id).filter(|e| e.is(home_id)) else {
            return false;
        };
        if !entry.open_failed {
            return false;
        }
        entry.status = RemoteSetupStatus::pending();
        entry.open_failed = false;
        entry.output.clear();
        entry.truncated = false;
        true
    }

    /// The machine could not open the home (or could not be reached):
    /// `failed` with `error`, while it was `pending`.
    #[must_use = "true when the home's setup changed: announce it"]
    pub fn open_failed(&self, workspace_id: &WorkspaceId, home_id: &HomeId, error: &str) -> bool {
        let mut map = self.lock();
        let Some(entry) = map.get_mut(workspace_id).filter(|e| e.is(home_id)) else {
            return false;
        };
        if entry.status.state != RemoteSetupState::Pending {
            return false;
        }
        entry.status = RemoteSetupStatus {
            state: RemoteSetupState::Failed,
            error: Some(RemoteSetupStatus::capped_error(error)),
            finished_at: Some(intent_core::now_iso()),
            ..RemoteSetupStatus::pending()
        };
        entry.open_failed = true;
        true
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
        let mut map = self.lock();
        let Some(entry) = map.get_mut(workspace_id).filter(|e| e.is(home_id)) else {
            return false;
        };
        if entry.status.state != RemoteSetupState::Pending {
            return false;
        }
        entry.status = RemoteSetupStatus {
            state: RemoteSetupState::Running,
            started_at: Some(started_at.to_owned()),
            ..RemoteSetupStatus::pending()
        };
        true
    }

    /// The setup finished (`setupFinished`): completed, skipped or failed.
    #[must_use = "true when the home's setup changed: announce it"]
    pub fn setup_finished(
        &self,
        workspace_id: &WorkspaceId,
        home_id: &HomeId,
        finished: HomeSetupFinished,
    ) -> bool {
        let mut map = self.lock();
        let Some(entry) = map.get_mut(workspace_id).filter(|e| e.is(home_id)) else {
            return false;
        };
        let started_at = match entry.status.state {
            RemoteSetupState::Pending => None,
            RemoteSetupState::Running => entry.status.started_at.clone(),
            _ => return false,
        };
        entry.status = RemoteSetupStatus {
            state: finished.state,
            exit_code: finished.exit_code,
            error: finished
                .error
                .as_deref()
                .map(RemoteSetupStatus::capped_error),
            started_at,
            finished_at: Some(finished.finished_at),
            ..RemoteSetupStatus::pending()
        };
        true
    }

    /// Keep a `setupOutput` chunk (`seq` 0 starts the output over). Whether
    /// to announce it now: at most once every [`OUTPUT_PUBLISH_EVERY`].
    #[must_use = "true when the home's setup changed: announce it"]
    pub fn output(
        &self,
        workspace_id: &WorkspaceId,
        home_id: &HomeId,
        seq: u64,
        data: &str,
        now: Instant,
    ) -> bool {
        let mut map = self.lock();
        let Some(entry) = map.get_mut(workspace_id).filter(|e| e.is(home_id)) else {
            return false;
        };
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
        let due = entry
            .published
            .is_none_or(|at| now.duration_since(at) >= OUTPUT_PUBLISH_EVERY);
        if due {
            entry.published = Some(now);
        }
        due
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
        assert!(setups.output(&ws, &home, 0, "Fetching\n", start));
        assert!(!setups.output(
            &ws,
            &home,
            1,
            "Checking out\n",
            start + Duration::from_secs(1)
        ));
        assert!(setups.output(&ws, &home, 2, "Done\n", start + Duration::from_secs(2)));
        assert_eq!(setups.status(&ws, &home, false).unwrap().output, None);
        let detail = setups.status(&ws, &home, true).unwrap();
        assert_eq!(
            detail.output.as_deref(),
            Some("Fetching\nChecking out\nDone\n")
        );
        assert_eq!(detail.output_truncated, None);
        // seq 0 starts over.
        let _ = setups.output(&ws, &home, 0, "again\n", start + Duration::from_secs(9));
        assert_eq!(
            setups.status(&ws, &home, true).unwrap().output.as_deref(),
            Some("again\n")
        );
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
