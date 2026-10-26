//! Live-process activation tracking for named provider profiles
//! (PROTOCOL §5.54 "Activation").
//!
//! A provider process reads its profile's saved settings once, when it
//! starts; saving an edit never touches a running process. This registry
//! records, per live agent process started under a profile, the
//! `settingsFingerprint` it started with, next to the profile's current saved
//! fingerprint. From those two it answers:
//!
//! - `activation.runningProcesses` on profile reads: live processes of the
//!   profile whose fingerprint differs from the saved one;
//! - `usingEarlierProfileSettings` on an agent: its live process started
//!   with a fingerprint other than the saved one.
//!
//! Every mutation returns the [`ActivationChanges`] it caused (profiles whose
//! count moved, agents whose flag flipped) so the caller can emit
//! `providers:profiles-changed` (`change: "activation"`) and `agent:updated`.
//! The registry is value-free: it holds references and opaque fingerprints,
//! never environment keys or values.

use std::collections::{BTreeSet, HashMap};
use std::sync::Mutex;

use intent_core::{AgentId, WorkspaceId};

/// The registry key of a reference: the profile id inside a qualified
/// reference, `default:<base>` for the Default configuration of a
/// profile-capable base (PROTOCOL §5.54 "Default configuration overrides"),
/// and `None` for any other bare id.
fn profile_id_of(reference: &str) -> Option<String> {
    if crate::provider_profiles::is_default_config_ref(reference) {
        return Some(crate::provider_profiles::default_activation_key(reference));
    }
    crate::provider_profiles::parse_profile_ref(reference).map(|(_, id)| id.to_owned())
}

/// A profile's `runningProcesses` count and each of its agents' workspace
/// and earlier-settings flag, at one instant.
type Snapshot = (usize, HashMap<AgentId, (WorkspaceId, bool)>);

#[derive(Debug, Clone)]
struct LiveProcess {
    workspace_id: WorkspaceId,
    profile_id: String,
    fingerprint: String,
    token: u64,
}

#[derive(Debug, Default)]
struct State {
    live: HashMap<AgentId, LiveProcess>,
    /// Current saved fingerprint per profile id. Seeded by the first process
    /// that starts (its overlay was read fresh) and replaced by every saved
    /// edit; removed on delete.
    saved: HashMap<String, String>,
    next_token: u64,
}

impl State {
    fn stale(&self, process: &LiveProcess) -> bool {
        self.saved
            .get(&process.profile_id)
            .is_some_and(|saved| *saved != process.fingerprint)
    }

    fn running_processes(&self, profile_id: &str) -> usize {
        self.live
            .values()
            .filter(|p| p.profile_id == profile_id && self.stale(p))
            .count()
    }

    /// Per-profile counts and per-agent flags for `profile_id`, used to diff
    /// a mutation's before/after.
    fn snapshot(&self, profile_id: &str) -> Snapshot {
        let agents = self
            .live
            .iter()
            .filter(|(_, p)| p.profile_id == profile_id)
            .map(|(id, p)| (id.clone(), (p.workspace_id.clone(), self.stale(p))))
            .collect();
        (self.running_processes(profile_id), agents)
    }
}

/// What a registry mutation changed, for event emission.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct ActivationChanges {
    /// Profiles whose `activation.runningProcesses` changed.
    pub(crate) profiles: BTreeSet<String>,
    /// Agents whose `usingEarlierProfileSettings` flipped, with the new value.
    pub(crate) agents: Vec<(WorkspaceId, AgentId, bool)>,
}

impl ActivationChanges {
    pub(crate) fn is_empty(&self) -> bool {
        self.profiles.is_empty() && self.agents.is_empty()
    }

    fn diff(profile_id: &str, before: &Snapshot, after: &Snapshot) -> Self {
        let mut changes = Self::default();
        if before.0 != after.0 {
            changes.profiles.insert(profile_id.to_owned());
        }
        let mut ids: BTreeSet<&AgentId> = before.1.keys().collect();
        ids.extend(after.1.keys());
        for id in ids {
            let was = before.1.get(id).is_some_and(|(_, stale)| *stale);
            let now = after.1.get(id).is_some_and(|(_, stale)| *stale);
            if was != now {
                let ws = after.1.get(id).or_else(|| before.1.get(id)).map_or_else(
                    || WorkspaceId::from_string(String::new()),
                    |(ws, _)| ws.clone(),
                );
                changes.agents.push((ws, id.clone(), now));
            }
        }
        changes
    }
}

/// Registry of live profile processes (one per agent at most).
#[derive(Debug, Default)]
pub(crate) struct ProfileActivation {
    state: Mutex<State>,
}

impl ProfileActivation {
    /// Record a process for `agent_id` that started under `reference` with
    /// `fingerprint`. Replaces any earlier record for the agent. Returns the
    /// registration token (for [`Self::unregister`]) and the changes.
    pub(crate) fn register(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
        reference: &str,
        fingerprint: &str,
    ) -> Option<(u64, ActivationChanges)> {
        let profile_id = profile_id_of(reference)?;
        let mut state = self.state.lock().unwrap();
        let mut touched: BTreeSet<String> = BTreeSet::from([profile_id.clone()]);
        if let Some(previous) = state.live.get(agent_id) {
            touched.insert(previous.profile_id.clone());
        }
        let before: Vec<_> = touched
            .iter()
            .map(|id| (id.clone(), state.snapshot(id)))
            .collect();
        state.next_token += 1;
        let token = state.next_token;
        state
            .saved
            .entry(profile_id.clone())
            .or_insert_with(|| fingerprint.to_owned());
        state.live.insert(
            agent_id.clone(),
            LiveProcess {
                workspace_id: workspace_id.clone(),
                profile_id,
                fingerprint: fingerprint.to_owned(),
                token,
            },
        );
        let changes = Self::collect(&state, before);
        Some((token, changes))
    }

    /// Forget the process registered under `token` (a newer registration for
    /// the same agent is left alone).
    pub(crate) fn unregister(&self, agent_id: &AgentId, token: u64) -> ActivationChanges {
        let mut state = self.state.lock().unwrap();
        let Some(profile_id) = state
            .live
            .get(agent_id)
            .filter(|p| p.token == token)
            .map(|p| p.profile_id.clone())
        else {
            return ActivationChanges::default();
        };
        let before = vec![(profile_id.clone(), state.snapshot(&profile_id))];
        state.live.remove(agent_id);
        Self::collect(&state, before)
    }

    /// A profile's saved fingerprint changed (`Some`) or the profile was
    /// deleted (`None`).
    pub(crate) fn saved_changed(
        &self,
        profile_id: &str,
        fingerprint: Option<&str>,
    ) -> ActivationChanges {
        let mut state = self.state.lock().unwrap();
        let before = vec![(profile_id.to_owned(), state.snapshot(profile_id))];
        match fingerprint {
            Some(fp) => {
                state.saved.insert(profile_id.to_owned(), fp.to_owned());
            }
            None => {
                state.saved.remove(profile_id);
            }
        }
        Self::collect(&state, before)
    }

    fn collect(state: &State, before: Vec<(String, Snapshot)>) -> ActivationChanges {
        let mut changes = ActivationChanges::default();
        for (profile_id, snapshot) in before {
            let diff =
                ActivationChanges::diff(&profile_id, &snapshot, &state.snapshot(&profile_id));
            changes.profiles.extend(diff.profiles);
            for agent in diff.agents {
                if !changes.agents.iter().any(|(_, id, _)| *id == agent.1) {
                    changes.agents.push(agent);
                }
            }
        }
        changes
    }

    /// `activation.runningProcesses` for `profile_id`.
    pub(crate) fn running_processes(&self, profile_id: &str) -> usize {
        self.state.lock().unwrap().running_processes(profile_id)
    }

    /// `usingEarlierProfileSettings` for `agent_id`.
    pub(crate) fn uses_earlier_settings(&self, agent_id: &AgentId) -> bool {
        let state = self.state.lock().unwrap();
        state.live.get(agent_id).is_some_and(|p| state.stale(p))
    }

    /// The fingerprint `agent_id`'s live process started with, when it runs
    /// under `reference`.
    pub(crate) fn live_fingerprint(&self, agent_id: &AgentId, reference: &str) -> Option<String> {
        let profile_id = profile_id_of(reference)?;
        let state = self.state.lock().unwrap();
        state
            .live
            .get(agent_id)
            .filter(|p| p.profile_id == *profile_id)
            .map(|p| p.fingerprint.clone())
    }
}

/// Held by a live agent process started under a profile; dropping it (the
/// process is torn down: stop, idle reap, crash cleanup, respawn) removes
/// the registration and emits the resulting activation events.
pub(crate) struct ActivationLease {
    /// Boxed: `Services` is large and the lease lives in every agent handle.
    services: Box<crate::Services>,
    agent_id: AgentId,
    token: u64,
}

impl ActivationLease {
    pub(crate) fn new(services: crate::Services, agent_id: AgentId, token: u64) -> Self {
        Self {
            services: Box::new(services),
            agent_id,
            token,
        }
    }
}

impl Drop for ActivationLease {
    fn drop(&mut self) {
        let changes = self
            .services
            .profile_activation()
            .unregister(&self.agent_id, self.token);
        if changes.is_empty() {
            return;
        }
        let services = (*self.services).clone();
        match tokio::runtime::Handle::try_current() {
            Ok(runtime) => {
                runtime.spawn(async move { services.publish_activation_changes(changes).await });
            }
            Err(_) => {
                tracing::debug!(agent = %self.agent_id, "no runtime; activation change events skipped");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ws() -> WorkspaceId {
        WorkspaceId::from("ws-act")
    }

    /// Both supported base providers behave the same.
    const BASES: [&str; 2] = ["codex", "claude-code"];

    #[test]
    fn an_edit_marks_running_processes_and_new_processes_use_saved_settings() {
        for base in BASES {
            let reg = ProfileActivation::default();
            let work = format!("{base}@pwork");
            let a = AgentId::from("a-1");
            let (token_a, changes) = reg.register(&ws(), &a, &work, "f1").unwrap();
            assert!(changes.is_empty(), "{base}: a fresh process is current");
            assert_eq!(reg.running_processes("pwork"), 0);
            assert!(!reg.uses_earlier_settings(&a));

            // Save an edit: the running process keeps its settings.
            let changes = reg.saved_changed("pwork", Some("f2"));
            assert_eq!(changes.profiles, BTreeSet::from(["pwork".to_string()]));
            assert_eq!(changes.agents, vec![(ws(), a.clone(), true)], "{base}");
            assert_eq!(reg.running_processes("pwork"), 1);
            assert!(reg.uses_earlier_settings(&a));
            assert_eq!(reg.live_fingerprint(&a, &work).as_deref(), Some("f1"));

            // A new process (another agent, or a recreate) starts on saved.
            let b = AgentId::from("a-2");
            let (_, changes) = reg.register(&ws(), &b, &work, "f2").unwrap();
            assert!(changes.is_empty(), "{base}");
            assert_eq!(reg.running_processes("pwork"), 1);
            assert!(!reg.uses_earlier_settings(&b));

            // The old process exits (idle reap / crash): the notice clears.
            let changes = reg.unregister(&a, token_a);
            assert_eq!(changes.profiles, BTreeSet::from(["pwork".to_string()]));
            assert_eq!(changes.agents, vec![(ws(), a.clone(), false)]);
            assert_eq!(reg.running_processes("pwork"), 0);
            assert!(!reg.uses_earlier_settings(&a));
        }
    }

    #[test]
    fn the_indicator_clears_only_when_effective_matches_saved() {
        for base in BASES {
            let reg = ProfileActivation::default();
            let work = format!("{base}@pwork");
            let a = AgentId::from("a-1");
            reg.register(&ws(), &a, &work, "f1").unwrap();
            reg.saved_changed("pwork", Some("f2"));
            assert!(reg.uses_earlier_settings(&a));
            // Another unrelated edit keeps it pending.
            let changes = reg.saved_changed("pwork", Some("f3"));
            assert!(changes.is_empty(), "{base}: still 1 process, still stale");
            assert!(reg.uses_earlier_settings(&a));
            // Saving back to what the process runs clears it.
            let changes = reg.saved_changed("pwork", Some("f1"));
            assert_eq!(changes.agents, vec![(ws(), a.clone(), false)]);
            assert_eq!(reg.running_processes("pwork"), 0);
        }
    }

    #[test]
    fn a_respawn_replaces_the_record_and_a_stale_lease_cannot_remove_it() {
        let reg = ProfileActivation::default();
        let a = AgentId::from("a-1");
        let (old, _) = reg.register(&ws(), &a, "codex@pwork", "f1").unwrap();
        reg.saved_changed("pwork", Some("f2"));
        // Respawn on saved settings (idle reap + resume), then the old
        // handle's lease drops late.
        let (_, changes) = reg.register(&ws(), &a, "codex@pwork", "f2").unwrap();
        assert_eq!(changes.agents, vec![(ws(), a.clone(), false)]);
        assert!(reg.unregister(&a, old).is_empty());
        assert_eq!(
            reg.live_fingerprint(&a, "codex@pwork").as_deref(),
            Some("f2")
        );
        // A switch to another profile moves the record.
        let (_, _) = reg
            .register(&ws(), &a, "claude-code@ppersonal", "g1")
            .unwrap();
        assert_eq!(reg.live_fingerprint(&a, "codex@pwork"), None);
        assert_eq!(
            reg.live_fingerprint(&a, "claude-code@ppersonal").as_deref(),
            Some("g1")
        );
    }

    #[test]
    fn other_providers_and_deleted_profiles_never_report_earlier_settings() {
        let reg = ProfileActivation::default();
        let a = AgentId::from("a-1");
        assert!(reg.register(&ws(), &a, "auggie", "f1").is_none());
        reg.register(&ws(), &a, "codex@pwork", "f1").unwrap();
        reg.saved_changed("pwork", Some("f2"));
        assert!(reg.uses_earlier_settings(&a));
        let changes = reg.saved_changed("pwork", None);
        assert_eq!(changes.agents, vec![(ws(), a.clone(), false)]);
        assert_eq!(reg.running_processes("pwork"), 0);
    }

    /// The Default configuration of a profile base (PROTOCOL §5.54 "Default
    /// configuration overrides") is tracked under its own key: a saved edit
    /// marks its running processes, never a profile's.
    #[test]
    fn a_default_edit_marks_only_default_processes() {
        for base in BASES {
            let reg = ProfileActivation::default();
            let key = crate::provider_profiles::default_activation_key(base);
            let d = AgentId::from("a-default");
            let p = AgentId::from("a-profile");
            reg.register(&ws(), &d, base, "").unwrap();
            reg.register(&ws(), &p, &format!("{base}@pwork"), "f1")
                .unwrap();
            let changes = reg.saved_changed(&key, Some("fdef"));
            assert_eq!(changes.profiles, BTreeSet::from([key.clone()]), "{base}");
            assert_eq!(changes.agents, vec![(ws(), d.clone(), true)], "{base}");
            assert_eq!(reg.running_processes(&key), 1);
            assert_eq!(reg.running_processes("pwork"), 0);
            assert!(!reg.uses_earlier_settings(&p));
            assert_eq!(reg.live_fingerprint(&d, base).as_deref(), Some(""));
            assert_eq!(reg.live_fingerprint(&d, "auggie"), None);
        }
    }
}
