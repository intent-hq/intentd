use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use intent_core::machines::{
    ExecutorLiveStatus, ExecutorLiveness, MachineRoutes, MachineRoutesSource,
};
use intent_core::{
    with_caller, AgentId, Caller, Error, ExecutorCredentialId, HostRole, PrincipalId,
};
use intent_store::Store;
use serde_json::{json, Value};

use crate::tests::TempDb;
use crate::Services;

struct Routes(Option<MachineRoutes>);

impl MachineRoutesSource for Routes {
    fn machine_routes(
        &self,
    ) -> Pin<Box<dyn Future<Output = intent_core::Result<MachineRoutes>> + Send + '_>> {
        let routes = self.0.clone();
        Box::pin(async move { routes.ok_or(Error::ListenerDown) })
    }
}

#[derive(Default)]
struct Live(Mutex<Vec<(String, ExecutorLiveStatus)>>);

impl ExecutorLiveness for Live {
    fn status(&self, credential_id: &ExecutorCredentialId) -> ExecutorLiveStatus {
        self.0
            .lock()
            .unwrap()
            .iter()
            .find(|(id, _)| id == credential_id.as_str())
            .map(|(_, status)| *status)
            .unwrap_or_default()
    }
}

fn routes() -> MachineRoutes {
    MachineRoutes {
        name: "Build box".into(),
        port: 7443,
        hosts: vec!["192.168.1.20".into(), "fd00::20".into()],
        tc_address: Some("tc-build-box".into()),
        fingerprint: "AB:CD:EF".into(),
    }
}

struct Fixture {
    _tmp: TempDb,
    _config: tempfile::TempDir,
    store: Store,
    services: Services,
    member: PrincipalId,
}

impl Fixture {
    fn set(&self, path: &str, value: Value) {
        self.services
            .settings_registry()
            .expect("registry")
            .apply(&[(path.to_string(), value)])
            .expect("apply");
    }
}

async fn fixture(enabled: bool, routes: Option<MachineRoutes>) -> Fixture {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let member = intent_core::Principal {
        id: PrincipalId::new(),
        identity: None,
        github_user_id: Some(77),
        login: Some("member".into()),
        display_name: None,
        avatar_url: None,
        is_primary: false,
        created_at: intent_core::now_iso(),
        updated_at: intent_core::now_iso(),
    };
    store.upsert_principal(&member).await.expect("member");
    let config = tempfile::TempDir::new().unwrap();
    let registry = Arc::new(
        crate::SettingsRegistry::load(config.path().join("config.toml")).expect("registry"),
    );
    let services = Services::new(store.clone()).with_settings_registry(registry);
    services.attach_machine_routes(Arc::new(Routes(routes)));
    let f = Fixture {
        _tmp: tmp,
        _config: config,
        store,
        services,
        member: member.id,
    };
    if enabled {
        f.set("remoteAgents.enabled", json!(true));
    }
    f
}

async fn owner(f: &Fixture) -> Caller {
    Caller::Wire {
        principal_id: f.store.get_primary_principal().await.unwrap().id,
        host_role: HostRole::Owner,
    }
}

fn mint_params() -> Value {
    json!({ "backendId": "backend-1", "backendName": "Linux backend", "backendFingerprint": "11:22" })
}

async fn mint(f: &Fixture, caller: Caller) -> intent_core::Result<Value> {
    with_caller(caller, f.services.executor_mint_op(mint_params())).await
}

#[tokio::test]
async fn owner_mints_a_hashed_credential_and_gets_the_token_once() {
    let f = fixture(true, Some(routes())).await;
    let minted = mint(&f, owner(&f).await).await.expect("mint");
    let token = minted["token"].as_str().unwrap().to_string();
    assert_eq!(token.len(), 64, "32 random bytes, hex");
    assert!(token
        .chars()
        .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()));
    let credential_id = minted["credentialId"].as_str().unwrap().to_string();
    let platform = intent_core::placement::AgentPlatform::host().expect("supported host");
    assert_eq!(
        minted["machine"],
        json!({
            "daemonId": f.store.daemon_id().await.unwrap(),
            "name": "Build box",
            "platform": platform,
            "port": 7443,
            "hosts": ["192.168.1.20", "fd00::20"],
            "tcAddress": "tc-build-box",
            "fingerprint": "AB:CD:EF",
        })
    );

    let resolved = f
        .store
        .resolve_active_executor_credential(&crate::invite_ops::hash_secret(&token))
        .await
        .unwrap()
        .expect("the stored hash admits the token");
    assert_eq!(resolved.id.as_str(), credential_id);
    assert_eq!(resolved.backend_id, "backend-1");
    assert_eq!(resolved.backend_name, "Linux backend");
    assert_eq!(resolved.backend_fingerprint.as_deref(), Some("11:22"));
    assert!(
        f.store
            .resolve_active_executor_credential(&token)
            .await
            .unwrap()
            .is_none(),
        "the token itself is never stored"
    );

    let again = mint(&f, Caller::Daemon).await.expect("mint again");
    assert_ne!(again["token"], minted["token"]);
    assert_ne!(again["credentialId"], minted["credentialId"]);
    let listed = with_caller(Caller::Daemon, f.services.executor_list_op())
        .await
        .unwrap();
    assert_eq!(
        listed["backends"].as_array().unwrap().len(),
        2,
        "minting again adds a credential"
    );
    assert!(
        !listed.to_string().contains(&token),
        "list never returns tokens"
    );
}

#[tokio::test]
async fn mint_omits_an_absent_tunnel_and_fingerprint() {
    let mut r = routes();
    r.tc_address = None;
    let f = fixture(true, Some(r)).await;
    let minted = with_caller(
        owner(&f).await,
        f.services
            .executor_mint_op(json!({ "backendId": "b", "backendName": "B" })),
    )
    .await
    .unwrap();
    assert!(minted["machine"].get("tcAddress").is_none());
    let listed = with_caller(Caller::Daemon, f.services.executor_list_op())
        .await
        .unwrap();
    assert!(listed["backends"][0].get("backendFingerprint").is_none());
    assert!(listed["backends"][0].get("lastUsedAt").is_none());
}

#[tokio::test]
async fn members_guests_agents_and_executors_cannot_mint_list_or_revoke() {
    let f = fixture(true, Some(routes())).await;
    let id = mint(&f, Caller::Daemon).await.unwrap()["credentialId"]
        .as_str()
        .unwrap()
        .to_string();
    let callers = [
        Caller::Wire {
            principal_id: f.member.clone(),
            host_role: HostRole::Member,
        },
        Caller::Wire {
            principal_id: f.member.clone(),
            host_role: HostRole::Guest,
        },
        Caller::Agent {
            agent_id: AgentId::new(),
        },
        Caller::Executor {
            credential_id: ExecutorCredentialId::from(id.clone()),
            backend_id: "backend-1".into(),
        },
        // In depth: a connection admitted as owner whose principal is no
        // longer the owner is refused by the durable role.
        Caller::Wire {
            principal_id: f.member.clone(),
            host_role: HostRole::Owner,
        },
    ];
    for caller in callers {
        let results = [
            mint(&f, caller.clone()).await,
            with_caller(caller.clone(), f.services.executor_list_op()).await,
            with_caller(caller.clone(), f.services.executor_revoke_op(&id)).await,
        ];
        for result in results {
            assert!(
                matches!(result, Err(Error::Forbidden(_))),
                "{caller:?}: {result:?}"
            );
        }
    }
    let listed = with_caller(Caller::Daemon, f.services.executor_list_op())
        .await
        .unwrap();
    assert_eq!(
        listed["backends"].as_array().unwrap().len(),
        1,
        "nothing was minted or revoked"
    );
}

#[tokio::test]
async fn mint_is_refused_while_remote_agents_are_disabled() {
    let f = fixture(false, Some(routes())).await;
    let refused = mint(&f, owner(&f).await).await;
    assert!(
        matches!(
            refused,
            Err(Error::RemoteAgentsDisabled {
                setting: "remoteAgents.enabled"
            })
        ),
        "{refused:?}"
    );
    // A member is refused as forbidden before the setting is consulted.
    let member = mint(
        &f,
        Caller::Wire {
            principal_id: f.member.clone(),
            host_role: HostRole::Member,
        },
    )
    .await;
    assert!(matches!(member, Err(Error::Forbidden(_))), "{member:?}");
    f.set("remoteAgents.enabled", json!(true));
    mint(&f, owner(&f).await).await.expect("mint once enabled");
    // Turning it off again keeps list and revoke available to the owner.
    f.set("remoteAgents.enabled", json!(false));
    let listed = with_caller(owner(&f).await, f.services.executor_list_op())
        .await
        .unwrap();
    let id = listed["backends"][0]["credentialId"]
        .as_str()
        .unwrap()
        .to_string();
    with_caller(owner(&f).await, f.services.executor_revoke_op(&id))
        .await
        .expect("revoke while disabled");
}

#[tokio::test]
async fn mint_validates_params_and_needs_a_running_listener() {
    let f = fixture(true, Some(routes())).await;
    let long = "x".repeat(129);
    for params in [
        json!({}),
        json!({ "backendName": "B" }),
        json!({ "backendId": "b" }),
        json!({ "backendId": "", "backendName": "B" }),
        json!({ "backendId": "  ", "backendName": "B" }),
        json!({ "backendId": "b", "backendName": "" }),
        json!({ "backendId": "b", "backendName": long }),
        json!({ "backendId": long, "backendName": "B" }),
        json!({ "backendId": 7, "backendName": "B" }),
        json!({ "backendId": "b", "backendName": "B", "backendFingerprint": 3 }),
        json!({ "backendId": "b", "backendName": "B", "backendFingerprint": "" }),
    ] {
        let result = with_caller(Caller::Daemon, f.services.executor_mint_op(params.clone())).await;
        assert!(
            matches!(result, Err(Error::InvalidParams(_))),
            "{params}: {result:?}"
        );
    }
    // 128 bytes is the limit, counted in UTF-8 bytes.
    let at_limit = "é".repeat(64);
    with_caller(
        Caller::Daemon,
        f.services
            .executor_mint_op(json!({ "backendId": "b", "backendName": at_limit })),
    )
    .await
    .expect("128 bytes");
    let over = format!("{at_limit}x");
    assert!(matches!(
        with_caller(
            Caller::Daemon,
            f.services
                .executor_mint_op(json!({ "backendId": "b", "backendName": over })),
        )
        .await,
        Err(Error::InvalidParams(_))
    ));

    let down = fixture(true, None).await;
    let refused = mint(&down, Caller::Daemon).await;
    assert!(matches!(refused, Err(Error::ListenerDown)), "{refused:?}");
    let listed = with_caller(Caller::Daemon, down.services.executor_list_op())
        .await
        .unwrap();
    assert_eq!(
        listed,
        json!({ "backends": [] }),
        "a failed mint stores nothing"
    );
}

#[tokio::test]
async fn list_reports_live_status_and_revoke_fences_the_token() {
    let f = fixture(true, Some(routes())).await;
    let first = mint(&f, Caller::Daemon).await.unwrap();
    let second = mint(&f, Caller::Daemon).await.unwrap();
    let first_id = first["credentialId"].as_str().unwrap().to_string();
    let second_id = second["credentialId"].as_str().unwrap().to_string();
    let live = Arc::new(Live::default());
    live.0.lock().unwrap().push((
        second_id.clone(),
        ExecutorLiveStatus {
            connected: true,
            running_agents: 2,
        },
    ));
    f.services.attach_executor_liveness(live);

    let listed = with_caller(owner(&f).await, f.services.executor_list_op())
        .await
        .unwrap();
    let backends = listed["backends"].as_array().unwrap();
    assert_eq!(backends.len(), 2);
    let row = |id: &str| {
        backends
            .iter()
            .find(|b| b["credentialId"] == id)
            .unwrap()
            .clone()
    };
    let first_row = row(&first_id);
    assert_eq!(first_row["backendId"], "backend-1");
    assert_eq!(first_row["backendName"], "Linux backend");
    assert_eq!(first_row["backendFingerprint"], "11:22");
    assert!(first_row["createdAt"].is_string());
    assert_eq!(first_row["connected"], false);
    assert_eq!(first_row["runningAgents"], 0);
    assert_eq!(row(&second_id)["connected"], true);
    assert_eq!(row(&second_id)["runningAgents"], 2);

    assert_eq!(
        with_caller(owner(&f).await, f.services.executor_revoke_op(&first_id))
            .await
            .unwrap(),
        json!({ "ok": true })
    );
    let token = first["token"].as_str().unwrap();
    assert!(
        f.store
            .resolve_active_executor_credential(&crate::invite_ops::hash_secret(token))
            .await
            .unwrap()
            .is_none(),
        "a revoked token never admits again"
    );
    // Revoking again is a no-op.
    assert_eq!(
        with_caller(owner(&f).await, f.services.executor_revoke_op(&first_id))
            .await
            .unwrap(),
        json!({ "ok": true })
    );
    let unknown = with_caller(owner(&f).await, f.services.executor_revoke_op("nope")).await;
    assert!(matches!(unknown, Err(Error::NotFound(_))), "{unknown:?}");
    let empty = with_caller(owner(&f).await, f.services.executor_revoke_op("")).await;
    assert!(matches!(empty, Err(Error::InvalidParams(_))), "{empty:?}");

    let listed = with_caller(owner(&f).await, f.services.executor_list_op())
        .await
        .unwrap();
    let ids: Vec<_> = listed["backends"]
        .as_array()
        .unwrap()
        .iter()
        .map(|b| b["credentialId"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(ids, [second_id], "revoked credentials are not listed");
}

#[derive(Clone, Default)]
struct AllEvents(Arc<Mutex<Vec<String>>>);

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for AllEvents {
    fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        struct Visitor(String);
        impl tracing::field::Visit for Visitor {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                use std::fmt::Write as _;
                let _ = write!(self.0, "{}={value:?} ", field.name());
            }
        }
        let mut visitor = Visitor(String::new());
        event.record(&mut visitor);
        self.0.lock().unwrap().push(visitor.0);
    }
}

#[tokio::test]
async fn the_minted_token_is_never_logged() {
    use tracing_subscriber::layer::SubscriberExt as _;
    let f = fixture(true, Some(routes())).await;
    let events = AllEvents::default();
    let _guard = crate::test_tracing::set_capture_default(
        tracing_subscriber::Registry::default().with(events.clone()),
    );
    let minted = mint(&f, owner(&f).await).await.unwrap();
    let id = minted["credentialId"].as_str().unwrap().to_string();
    with_caller(owner(&f).await, f.services.executor_list_op())
        .await
        .unwrap();
    with_caller(owner(&f).await, f.services.executor_revoke_op(&id))
        .await
        .unwrap();
    let token = minted["token"].as_str().unwrap();
    let logged = events.0.lock().unwrap().clone();
    assert!(
        logged.iter().all(|line| !line.contains(token)),
        "token leaked into logs: {logged:?}"
    );
}

#[tokio::test]
async fn revoke_tells_the_listeners_so_live_links_close() {
    use intent_core::WorkspaceApi as _;
    let f = fixture(true, Some(routes())).await;
    let minted = mint(&f, Caller::Daemon).await.unwrap();
    let id = minted["credentialId"].as_str().unwrap().to_string();
    let mut revocations = f.services.subscribe_executor_revocations().unwrap();

    let refused = with_caller(
        Caller::Wire {
            principal_id: f.member.clone(),
            host_role: HostRole::Member,
        },
        f.services.executor_revoke_op(&id),
    )
    .await;
    assert!(refused.is_err());
    let missing = with_caller(owner(&f).await, f.services.executor_revoke_op("nope")).await;
    assert!(matches!(missing, Err(Error::NotFound(_))));
    assert!(
        revocations.try_recv().is_err(),
        "a refused or unknown revoke closes nothing"
    );

    with_caller(owner(&f).await, f.services.executor_revoke_op(&id))
        .await
        .unwrap();
    assert_eq!(revocations.try_recv().unwrap().as_str(), id);
    // A repeat revoke repeats the notice, so a missed one can be retried.
    with_caller(owner(&f).await, f.services.executor_revoke_op(&id))
        .await
        .unwrap();
    assert_eq!(revocations.try_recv().unwrap().as_str(), id);
}
