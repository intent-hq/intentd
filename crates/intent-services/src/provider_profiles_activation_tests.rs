//! Settings activation for named provider profiles (PROTOCOL §5.54): a
//! running provider process keeps the settings it started with, every newly
//! started process (including after an idle reap or a crash) reads the saved
//! profile, and profile reads/agents report processes still on earlier
//! settings. Synthetic fixtures for both Codex and Claude Code; no provider
//! process is launched (the registry is driven like `create_agent` does).

use std::sync::Arc;
use std::time::Duration;

use intent_core::{AgentCreateExtra, AgentId, WorkspaceId};
use intent_store::Store;
use serde_json::{json, Value};

use crate::events::{EventBus, Subscription, SubscriptionFilter};
use crate::settings::InMemorySecretStore;
use crate::test_support::test_tempdir;
use crate::tests::TempDb;
use crate::{Services, SettingsRegistry};

const BASES: [&str; 2] = ["codex", "claude-code"];

fn account_key(base: &str) -> &'static str {
    if base == "codex" {
        "CODEX_HOME"
    } else {
        "CLAUDE_CONFIG_DIR"
    }
}

struct Fixture {
    _db: TempDb,
    _root: tempfile::TempDir,
    svc: Services,
    bus: EventBus,
    ws: WorkspaceId,
}

impl Fixture {
    async fn new() -> Self {
        let root = test_tempdir("provider-profiles-activation-");
        let db = TempDb::new();
        let store = Store::open(&db.path).await.expect("open store");
        let bus = EventBus::new(store.clone());
        let ws = WorkspaceId::new();
        store
            .insert_workspace(&crate::tests::workspace(&ws))
            .await
            .expect("workspace");
        let data = root.path().join("data");
        let specialists = root.path().join("specialists");
        std::fs::create_dir_all(&data).unwrap();
        std::fs::create_dir_all(&specialists).unwrap();
        let registry =
            Arc::new(SettingsRegistry::load(data.join("config.toml")).expect("load registry"));
        let svc = Services::new(store)
            .with_event_bus(bus.clone())
            .with_settings_registry(registry)
            .with_secret_store(Arc::new(InMemorySecretStore::default()))
            .with_specialist_dirs(Some(specialists.clone()), Some(specialists))
            .with_provider_profiles_dir(&data);
        let fx = Self {
            _db: db,
            _root: root,
            svc,
            bus,
            ws,
        };
        fx.set(
            "providers.paths",
            json!({ "codex": "/bin/sh", "claude-code": "/bin/sh" }),
        );
        fx
    }

    fn set(&self, path: &str, value: Value) {
        self.svc
            .settings_registry()
            .expect("registry")
            .apply(&[(path.to_string(), value)])
            .expect("apply setting");
    }

    fn subscribe(&self) -> Subscription {
        self.bus.subscribe(SubscriptionFilter::default())
    }

    /// Create a profile; returns `(profileId, providerRef)`.
    async fn profile(&self, base: &str, name: &str) -> (String, String) {
        let created = self
            .svc
            .provider_profiles_create_impl(json!({
                "baseProviderId": base,
                "name": name,
                "env": [{ "key": account_key(base), "kind": "set",
                          "value": format!("/accounts/{name}") }],
            }))
            .await
            .expect("create profile");
        assert_eq!(created["profile"]["activation"]["runningProcesses"], 0);
        (
            created["profile"]["id"].as_str().unwrap().to_string(),
            created["profile"]["providerRef"]
                .as_str()
                .unwrap()
                .to_string(),
        )
    }

    async fn agent_on(&self, reference: &str) -> AgentId {
        let created = self
            .svc
            .agent_create_op(
                self.ws.clone(),
                None,
                None,
                None,
                None,
                None,
                false,
                AgentCreateExtra {
                    provider: Some(reference.to_string()),
                    ..Default::default()
                },
            )
            .await
            .expect("create agent");
        AgentId::from(created["agent"]["id"].as_str().expect("agent id"))
    }

    /// Start a "process" for `agent` the way `create_agent` does: read the
    /// saved overlay fresh, then register it.
    async fn start_process(
        &self,
        agent: &AgentId,
        reference: &str,
    ) -> (
        Arc<intent_acp::ProfileEnv>,
        crate::profile_activation::ActivationLease,
    ) {
        let profile = self
            .svc
            .resolve_selected_profile(reference)
            .await
            .expect("selectable")
            .expect("a profile overlay");
        let lease = self
            .svc
            .register_profile_process(&self.ws, agent, &profile)
            .await
            .expect("registered");
        (profile, lease)
    }

    async fn edit_env(&self, profile_id: &str, base: &str, value: &str) -> Value {
        self.svc
            .provider_profiles_update_impl(
                profile_id.to_string(),
                json!({ "env": [{ "key": account_key(base), "kind": "set", "value": value }] }),
            )
            .await
            .expect("update profile")
    }

    async fn listed_running(&self, profile_id: &str) -> Value {
        let listed = self
            .svc
            .provider_profiles_list_impl(None)
            .await
            .expect("list");
        listed["profiles"]
            .as_array()
            .unwrap()
            .iter()
            .find(|p| p["id"] == profile_id)
            .expect("listed")["activation"]["runningProcesses"]
            .clone()
    }

    async fn agent_wire(&self, agent: &AgentId) -> Value {
        let lite = self
            .svc
            .agent_get_op(agent.clone(), None)
            .await
            .expect("agent.get");
        serde_json::to_value(lite).unwrap()
    }
}

fn value_of<'a>(profile: &'a intent_acp::ProfileEnv, key: &str) -> Option<&'a str> {
    profile
        .entries()
        .find(|(k, _)| *k == key)
        .and_then(|(_, v)| v)
}

/// Collect events until `want` profile events and `agents` agent:updated
/// events with `usingEarlierProfileSettings` have arrived.
async fn collect(sub: &mut Subscription, want: usize, agents: usize) -> (Vec<Value>, Vec<Value>) {
    let (mut profiles, mut updated) = (Vec::new(), Vec::new());
    while profiles.len() < want || updated.len() < agents {
        let batch = tokio::time::timeout(Duration::from_secs(5), sub.recv())
            .await
            .unwrap_or_else(|_| panic!("events: {profiles:?} {updated:?}"))
            .expect("subscription open");
        for event in batch {
            let ev = serde_json::to_value(&event).unwrap();
            if ev["type"] == intent_core::events::PROVIDERS_PROFILES_CHANGED {
                profiles.push(ev["data"].clone());
            } else if ev["type"] == intent_core::events::AGENT_UPDATED
                && ev["data"].get("usingEarlierProfileSettings").is_some()
            {
                updated.push(ev["data"].clone());
            }
        }
    }
    (profiles, updated)
}

#[intent_test_macros::daemon_test]
async fn an_env_edit_leaves_the_running_process_unchanged_and_new_processes_use_it() {
    for base in BASES {
        let fx = Fixture::new().await;
        let (work_id, work) = fx.profile(base, "Work").await;
        let agent = fx.agent_on(&work).await;
        let (running, lease) = fx.start_process(&agent, &work).await;
        assert_eq!(
            value_of(&running, account_key(base)),
            Some("/accounts/Work")
        );
        assert_eq!(fx.listed_running(&work_id).await, 0, "{base}");
        assert!(fx
            .agent_wire(&agent)
            .await
            .get("usingEarlierProfileSettings")
            .is_none());

        let mut sub = fx.subscribe();
        let updated = fx.edit_env(&work_id, base, "/accounts/Other").await;
        assert_eq!(
            updated["activation"],
            json!({ "envChanged": true, "runningProcesses": 1 }),
            "{base}"
        );
        assert_eq!(updated["profile"]["activation"]["runningProcesses"], 1);
        assert_eq!(fx.listed_running(&work_id).await, 1, "{base}");
        let (profiles, agents) = collect(&mut sub, 2, 1).await;
        assert_eq!(profiles[0]["change"], "updated", "{base}");
        assert_eq!(profiles[1]["change"], "activation", "{base}");
        assert_eq!(profiles[1]["profileId"], work_id.as_str());
        assert!(
            profiles[1].get("profile").is_none(),
            "no profile on activation"
        );
        assert_eq!(
            profiles[1]["revision"].as_u64(),
            profiles[0]["revision"].as_u64().map(|r| r + 1)
        );
        assert_eq!(agents[0]["agentId"], agent.0.as_str());
        assert_eq!(agents[0]["usingEarlierProfileSettings"], true);
        // Nothing carries env keys or values.
        let wire = format!("{profiles:?}{agents:?}");
        assert!(!wire.contains("/accounts/") && !wire.contains(account_key(base)));

        // The running process keeps its settings: Save restarts nothing.
        assert_eq!(
            fx.svc
                .profile_activation()
                .live_fingerprint(&agent, &work)
                .as_deref(),
            Some(running.fingerprint())
        );
        assert_eq!(
            fx.agent_wire(&agent).await["usingEarlierProfileSettings"],
            true,
            "{base}"
        );

        // A new process (another conversation) starts on the saved values.
        let other = fx.agent_on(&work).await;
        let (fresh, _other_lease) = fx.start_process(&other, &work).await;
        assert_eq!(value_of(&fresh, account_key(base)), Some("/accounts/Other"));
        assert_ne!(fresh.fingerprint(), running.fingerprint());
        assert!(fx
            .agent_wire(&other)
            .await
            .get("usingEarlierProfileSettings")
            .is_none());
        assert_eq!(fx.listed_running(&work_id).await, 1, "{base}");
        drop(lease);
    }
}

#[intent_test_macros::daemon_test]
async fn an_idle_reap_and_resume_uses_the_saved_values_and_clears_the_indicator() {
    for base in BASES {
        let fx = Fixture::new().await;
        let (work_id, work) = fx.profile(base, "Work").await;
        let agent = fx.agent_on(&work).await;
        let (_, lease) = fx.start_process(&agent, &work).await;
        fx.edit_env(&work_id, base, "/accounts/Other").await;
        assert_eq!(fx.listed_running(&work_id).await, 1);

        // Idle reap (or crash cleanup) drops the handle and its lease.
        let mut sub = fx.subscribe();
        drop(lease);
        let (profiles, agents) = collect(&mut sub, 1, 1).await;
        assert_eq!(profiles[0]["change"], "activation", "{base}");
        assert_eq!(agents[0]["usingEarlierProfileSettings"], false, "{base}");
        assert_eq!(fx.listed_running(&work_id).await, 0, "{base}");
        assert!(fx
            .agent_wire(&agent)
            .await
            .get("usingEarlierProfileSettings")
            .is_none());

        // The automatic recreate on the next turn reads the saved values.
        let (resumed, _lease) = fx.start_process(&agent, &work).await;
        assert_eq!(
            value_of(&resumed, account_key(base)),
            Some("/accounts/Other")
        );
        assert_eq!(fx.listed_running(&work_id).await, 0, "{base}");
        assert!(fx
            .agent_wire(&agent)
            .await
            .get("usingEarlierProfileSettings")
            .is_none());
    }
}

#[intent_test_macros::daemon_test]
async fn the_pending_indicator_clears_only_when_effective_matches_saved() {
    for base in BASES {
        let fx = Fixture::new().await;
        let (work_id, work) = fx.profile(base, "Work").await;
        let agent = fx.agent_on(&work).await;
        let (_, _lease) = fx.start_process(&agent, &work).await;
        fx.edit_env(&work_id, base, "/accounts/Other").await;
        // A rename is not a settings change and changes nothing.
        let renamed = fx
            .svc
            .provider_profiles_update_impl(work_id.clone(), json!({ "name": "Job" }))
            .await
            .unwrap();
        assert_eq!(
            renamed["activation"],
            json!({ "envChanged": false, "runningProcesses": 1 })
        );
        // Saving the same values again mints a new fingerprint: still pending.
        fx.edit_env(&work_id, base, "/accounts/Work").await;
        assert_eq!(fx.listed_running(&work_id).await, 1, "{base}");
        assert_eq!(
            fx.agent_wire(&agent).await["usingEarlierProfileSettings"],
            true
        );
    }
}

#[intent_test_macros::daemon_test]
async fn a_removed_or_disabled_profile_requires_a_replacement_on_the_agent() {
    for base in BASES {
        let fx = Fixture::new().await;
        let (gone_id, gone) = fx.profile(base, "Gone").await;
        let (_, kept) = fx.profile(base, "Kept").await;
        let on_gone = fx.agent_on(&gone).await;
        let on_kept = fx.agent_on(&kept).await;
        let legacy = fx.agent_on(base).await;
        for agent in [&on_gone, &on_kept, &legacy] {
            assert!(fx
                .agent_wire(agent)
                .await
                .get("providerReplacementRequired")
                .is_none());
        }

        // An in-progress turn's process keeps running after removal; the
        // agent is not "on earlier settings", it needs a replacement.
        let (_, _lease) = fx.start_process(&on_gone, &gone).await;
        fx.svc
            .provider_profiles_delete_impl(gone_id.clone())
            .await
            .expect("delete");
        let wire = fx.agent_wire(&on_gone).await;
        assert_eq!(
            wire["providerReplacementRequired"],
            json!({ "providerRef": gone, "reason": "profile-missing" }),
            "{base}"
        );
        assert!(wire.get("usingEarlierProfileSettings").is_none());

        fx.set("providers.enabled", json!({ base: false }));
        assert_eq!(
            fx.agent_wire(&on_kept).await["providerReplacementRequired"],
            json!({ "providerRef": kept, "reason": "provider-disabled" }),
            "{base}"
        );
        // Legacy bare selections keep today's disabled-provider behavior.
        assert!(fx
            .agent_wire(&legacy)
            .await
            .get("providerReplacementRequired")
            .is_none());
    }
}
