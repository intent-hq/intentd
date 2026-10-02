//! Actual owners and provider IO. No fake authority, receipt, or admission fence.
use super::*;
use intent_core::caller::{with_caller, with_wire_credential, WireCredential};
use intent_core::repository_request::{
    RepositoryReadConnection, RepositoryReadRetirements, RepositoryWireEntry,
};
use intent_core::{
    Caller, FileSecretStore, HostInvite, HostRole, Principal, PrincipalId, WorkspaceApi,
    WorkspaceId, WorkspaceRole,
};
use intent_sourcecontrol::{GitlabDescriptor, GitlabHost};
use serde_json::{json, Value};
use std::future::Future;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const INSTANCE: &str = "https://forge.test:8443/install";
const PROJECT: &str = "Team/Sub/Project";
#[derive(Default)]
struct Provider {
    routes: Mutex<Vec<(String, String)>>,
    replies: Mutex<HashMap<String, (u16, Value)>>,
    pause: Mutex<Option<String>>,
    entered: Notify,
    release: Notify,
    active: AtomicUsize,
    completed: AtomicUsize,
}
struct Server {
    host: GitlabHost,
    descriptor: GitlabDescriptor,
    state: Arc<Provider>,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    task: Option<tokio::task::JoinHandle<()>>,
}
impl Server {
    async fn new() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        eprintln!(
            "resource provider instance={INSTANCE} endpoint={endpoint} credential=owned-synthetic"
        );
        let host = GitlabHost::parse("forge.test:8443")
            .unwrap()
            .with_api_origin(&endpoint)
            .unwrap();
        let descriptor = GitlabDescriptor::with_loopback_endpoint(
            GitlabInstance::parse(INSTANCE).unwrap(),
            &endpoint,
        )
        .unwrap();
        let state = Arc::new(Provider::default());
        let child_state = state.clone();
        let (stop, mut stopped) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let mut children = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    _ = &mut stopped => break,
                    result = listener.accept() => {
                        let (mut stream, _) = result.unwrap(); let state = child_state.clone();
                        children.spawn(async move {
                            struct Done(Arc<Provider>);
                            impl Drop for Done {fn drop(&mut self){self.0.active.fetch_sub(1,Ordering::SeqCst);self.0.completed.fetch_add(1,Ordering::SeqCst);}}
                            state.active.fetch_add(1, Ordering::SeqCst);
                            let _done = Done(state.clone());
                            let mut bytes = Vec::new(); let mut buf = [0;4096];
                            loop {
                                let n = stream.read(&mut buf).await.unwrap_or(0); if n==0{return;}
                                bytes.extend_from_slice(&buf[..n]); assert!(bytes.len()<=16384);
                                if bytes.windows(4).any(|v|v==b"\r\n\r\n"){break;}
                            }
                            let header=String::from_utf8(bytes).unwrap(); let mut words=header.split_whitespace();
                            let method=words.next().unwrap().to_owned(); let path=words.next().unwrap().to_owned();
                            assert_eq!(method,"GET"); assert!(header.contains("stored-pat"));
                            state.routes.lock().unwrap().push((method,path.clone()));
                            let route=path.split('?').next().unwrap();
                            // The response belongs to this original request, even if a newer
                            // request changes the configured reply before this hold is released.
                            let reply=state.replies.lock().unwrap().get(route).cloned().unwrap_or_else(||default_reply(route));
                            let pause={let mut p=state.pause.lock().unwrap();if p.as_deref()==Some(route){p.take();true}else{false}};
                            if pause {state.entered.notify_one();state.release.notified().await;}
                            let body=reply.1.to_string();
                            let head=format!("HTTP/1.1 {} Test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nRateLimit-Remaining: 17\r\nRateLimit-Limit: 100\r\nRateLimit-Reset: 1900000000\r\nConnection: close\r\n\r\n",reply.0,body.len());
                            let _=stream.write_all(format!("{head}{body}").as_bytes()).await;
                        });
                    },
                    result=children.join_next(),if !children.is_empty()=>{result.unwrap().unwrap();}
                }
            }
            // Every retained provider response has been released by the caller.
            while let Some(result) = children.join_next().await {
                result.unwrap();
            }
        });
        Self {
            host,
            descriptor,
            state,
            stop: Some(stop),
            task: Some(task),
        }
    }
    fn count(&self) -> usize {
        self.state.routes.lock().unwrap().len()
    }
    fn set(&self, route: &str, status: u16, value: Value) {
        self.state
            .replies
            .lock()
            .unwrap()
            .insert(route.into(), (status, value));
    }
    fn pause(&self, route: &str) {
        *self.state.pause.lock().unwrap() = Some(route.into());
    }
    async fn entered(&self) {
        tokio::time::timeout(ACQUIRE, self.state.entered.notified())
            .await
            .unwrap();
    }
    async fn finish(mut self) {
        self.state.release.notify_waiters();
        self.stop.take().unwrap().send(()).unwrap();
        tokio::time::timeout(ACQUIRE, self.task.take().unwrap())
            .await
            .unwrap()
            .unwrap();
        eprintln!(
            "resource provider joined: calls={} completed={} active={}",
            self.count(),
            self.state.completed.load(Ordering::SeqCst),
            self.state.active.load(Ordering::SeqCst)
        );
        assert_eq!(self.state.active.load(Ordering::SeqCst), 0);
        assert_eq!(self.state.completed.load(Ordering::SeqCst), self.count());
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        self.state.release.notify_waiters();
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}
fn route(kind: &str) -> String {
    format!("/api/v4/projects/Team%2FSub%2FProject/{kind}/7")
}
fn resource(kind: &str) -> Value {
    json!({"iid":7,"project_id":42,"source_project_id":42,"target_project_id":42,"source_branch":"topic","target_branch":"main","state":"opened","draft":false,"title":format!("{kind} seven"),"description":"original body","web_url":format!("{INSTANCE}/{PROJECT}/-/{kind}/7"),"sha":"0123456789012345678901234567890123456789","author":{"username":"fixture"},"created_at":"2026-01-01T00:00:00Z","updated_at":"2026-01-01T00:00:00Z"})
}
fn default_reply(path: &str) -> (u16, Value) {
    if path == "/api/v4/user" {
        (200, json!({"id":42,"username":"fixture","name":"Fixture"}))
    } else if path == route("merge_requests") {
        (200, resource("merge_requests"))
    } else if path == route("issues") {
        (200, resource("issues"))
    } else if path.ends_with("/approvals") {
        (
            200,
            json!({"approved_by":[],"approvals_left":0,"approvals_required":0}),
        )
    } else if path.ends_with("/discussions") {
        (200, json!([]))
    } else if path == "/api/v4/projects/Team%2FSub%2FProject" {
        (
            200,
            json!({"id":42,"path_with_namespace":PROJECT,"web_url":format!("{INSTANCE}/{PROJECT}"),"only_allow_merge_if_pipeline_succeeds":false,"only_allow_merge_if_all_discussions_are_resolved":false}),
        )
    } else {
        (404, json!({"message":"not found"}))
    }
}
struct Fixture {
    dir: tempfile::TempDir,
    services: Arc<Services>,
    workspace: WorkspaceId,
    owner: Caller,
}
impl Fixture {
    async fn new(server: &Server) -> Self {
        let dir = crate::test_support::test_tempdir("repository-resource-");
        let store = intent_store::Store::open(&dir.path().join("store.db"))
            .await
            .unwrap();
        let mut workspace = intent_core::chief_workspace();
        workspace.id = WorkspaceId::new();
        workspace.repository_path = None;
        workspace.path = None;
        store.insert_workspace(&workspace).await.unwrap();
        let registry = Arc::new(SettingsRegistry::load(dir.path().join("config.toml")).unwrap());
        registry
            .apply(&[
                (
                    "sourceControl.gitlab.host".into(),
                    json!(server.host.host()),
                ),
                (
                    "sourceControl.gitlab.instanceBaseUrl".into(),
                    json!(INSTANCE),
                ),
                (
                    "sourceControl.gitlab.apiBaseUrl".into(),
                    json!(server.host.base_url()),
                ),
                ("sourceControl.gitlab.oauthClientId".into(), json!("client")),
            ])
            .unwrap();
        let secrets = FileSecretStore::with_path(dir.path().join("secrets.json"));
        secrets
            .store(
                intent_sourcecontrol::gitlab_token::SECRET_ACCOUNT,
                "stored-pat",
            )
            .unwrap();
        let services = Arc::new(
            Services::new_repository_fixture(store, secrets, None)
                .with_settings_registry(registry.clone()),
        );
        services.initialize_repository_wire().await.unwrap();
        let guard = services.gitlab_credential_gate.lock().await;
        services
            .gitlab_credential_gate
            .install_settings_boundary(
                &registry,
                &services.secrets,
                &services.gitlab_secret_store,
                Some(server.descriptor.clone()),
            )
            .unwrap();
        drop(guard);
        services
            .reconcile_gitlab_repository_binding()
            .await
            .unwrap();
        let principal = services.store.get_primary_principal().await.unwrap();
        Self {
            dir,
            services,
            workspace: workspace.id,
            owner: Caller::Wire {
                principal_id: principal.id,
                host_role: HostRole::Owner,
            },
        }
    }
    fn query(&self) -> Query {
        Query {
            workspace_id: self.workspace.clone(),
        }
    }
    async fn socket(&self) -> Socket {
        Socket::new(&self.services, self.owner.clone(), None).await
    }
    async fn member(&self, role: HostRole) -> (Socket, Principal) {
        let person = Principal {
            id: PrincipalId::new(),
            identity: None,
            github_user_id: Some(8217),
            login: Some("resource-member".into()),
            display_name: None,
            avatar_url: None,
            is_primary: false,
            created_at: intent_core::now_iso(),
            updated_at: intent_core::now_iso(),
        };
        self.services.store.upsert_principal(&person).await.unwrap();
        let token = "resource-member-token-hash";
        if role == HostRole::Member {
            let owner = self.services.store.get_primary_principal().await.unwrap();
            let invite = HostInvite::new(
                "resource-invite".into(),
                owner.id,
                person.identity_key().unwrap(),
                person.login.clone().unwrap(),
                "proof".into(),
                None,
            )
            .unwrap();
            self.services
                .store
                .insert_host_invite(&invite)
                .await
                .unwrap();
            let generation = self
                .services
                .store
                .host_membership_state()
                .await
                .unwrap()
                .authorization_generation;
            self.services
                .store
                .join_host_by_invite(
                    &invite.id,
                    &person,
                    intent_store::HostJoinCredential::Proof {
                        token_hash: token,
                        authorization_generation: generation,
                    },
                )
                .await
                .unwrap();
        } else {
            self.services
                .store
                .insert_principal_credential(&person.id, token)
                .await
                .unwrap();
            self.services
                .store
                .add_workspace_member(&self.workspace, &person.id, WorkspaceRole::Collaborator)
                .await
                .unwrap();
        }
        let socket = Socket::new(
            &self.services,
            Caller::Wire {
                principal_id: person.id.clone(),
                host_role: role,
            },
            Some(WireCredential::Principal {
                principal_id: person.id.clone(),
                token_hash: token.into(),
            }),
        )
        .await;
        (socket, person)
    }
    fn detail(&self, c: &Capture, kind: RepositoryResourceKind, refresh: bool) -> Detail {
        Detail {
            workspace_id: self.workspace.clone(),
            read_lifetime_id: c.read_lifetime_id.clone(),
            target: ReviewTarget {
                repository: intent_core::RepositoryTarget {
                    provider: RepositoryProvider::Gitlab,
                    instance_base_url: INSTANCE.into(),
                    project_path: PROJECT.into(),
                },
                kind,
                number: 7,
            },
            refresh,
        }
    }
}
struct Socket {
    owner: Arc<dyn RepositoryReadConnection>,
    caller: Caller,
    credential: Option<WireCredential>,
    _inventory: Box<dyn RepositoryReadRetirements>,
    retirements: Box<dyn RepositoryResourceRetirements>,
}
impl Socket {
    async fn new(
        services: &Arc<Services>,
        caller: Caller,
        credential: Option<WireCredential>,
    ) -> Self {
        let entry = if credential.is_some() {
            RepositoryWireEntry::Bearer
        } else {
            RepositoryWireEntry::AdmittedLocal
        };
        let owner = with_caller(
            caller.clone(),
            with_wire_credential(credential.clone(), async {
                super::super::connection(services, entry).unwrap()
            }),
        )
        .await;
        let inventory = owner.take_retirements().unwrap();
        let retirements = owner.take_resource_retirements().unwrap();
        Self {
            owner,
            caller,
            credential,
            _inventory: inventory,
            retirements,
        }
    }
    async fn entered<T>(&self, body: impl Future<Output = T>) -> T {
        with_caller(
            self.caller.clone(),
            with_wire_credential(self.credential.clone(), body),
        )
        .await
    }
    async fn body(
        &self,
        services: &Services,
        frame: Frame,
    ) -> (Arc<dyn RepositoryReadRequestScope>, Result<Value>) {
        self.entered(async {
            let scope = self.owner.capture_resource(&frame).unwrap();
            let mut result = None;
            scope
                .scope(Box::pin(async {
                    result = Some(match frame {
                        Frame::Capture(q) => capture(services, q)
                            .await
                            .and_then(|v| serde_json::to_value(v).map_err(denied)),
                        Frame::Detail(q) => detail(services, q)
                            .await
                            .and_then(|v| serde_json::to_value(v).map_err(denied)),
                        Frame::Release(q) => release(services, q)
                            .await
                            .and_then(|v| serde_json::to_value(v).map_err(denied)),
                    });
                }))
                .await;
            (scope, result.unwrap())
        })
        .await
    }
    async fn transfer(
        &self,
        scope: &Arc<dyn RepositoryReadRequestScope>,
        body: &Result<Value>,
    ) -> (Result<()>, usize) {
        self.entered(async {
            let mut count = 0;
            let result = scope
                .deliver(
                    if body.is_ok() {
                        RepositoryReadReplyKind::Result
                    } else {
                        RepositoryReadReplyKind::ServiceError
                    },
                    &mut || {
                        count += 1;
                        Ok(())
                    },
                )
                .await;
            (result, count)
        })
        .await
    }
    async fn request(&self, services: &Services, frame: Frame) -> Result<Value> {
        let (scope, body) = self.body(services, frame).await;
        eprintln!("resource original body: {body:?}");
        let (sent, count) = self.transfer(&scope, &body).await;
        eprintln!("resource original transfer: {sent:?}; callbacks={count}");
        scope.retire();
        sent.and(body)
    }
    async fn capture(&self, f: &Fixture) -> Capture {
        serde_json::from_value(
            self.request(&f.services, Frame::Capture(f.query()))
                .await
                .unwrap(),
        )
        .unwrap()
    }
}
impl Drop for Socket {
    fn drop(&mut self) {
        self.owner.retire();
    }
}
async fn fixed_clock<T>(work: impl Future<Output = T>) -> T {
    tokio::time::pause();
    let result =
        tokio::select! {r=work=>r,()=async{loop{tokio::task::yield_now().await;}}=>unreachable!()};
    tokio::time::resume();
    result
}

#[intent_test_macros::daemon_test]
async fn resource_owner_member_mr_issue_warm_refresh_and_guest() {
    for role in [HostRole::Owner, HostRole::Member, HostRole::Guest] {
        let server = Server::new().await;
        let f = Fixture::new(&server).await;
        let s = if role == HostRole::Owner {
            f.socket().await
        } else {
            f.member(role).await.0
        };
        let before = server.count();
        if role == HostRole::Guest {
            assert!(s
                .request(&f.services, Frame::Capture(f.query()))
                .await
                .is_err());
            assert_eq!(before, server.count());
        } else {
            let c = s.capture(&f).await;
            assert_eq!(c.instances[0].instance_base_url, INSTANCE);
            assert_eq!(server.count(), before);
            let encoded = serde_json::to_value(&c).unwrap().to_string();
            for key in [
                "accountId",
                "connectionId",
                "generation",
                "stored-pat",
                "principalId",
            ] {
                assert!(!encoded.contains(key));
            }
            for kind in [
                RepositoryResourceKind::MergeRequest,
                RepositoryResourceKind::Issue,
            ] {
                let q = f.detail(&c, kind, false);
                let first = s
                    .request(&f.services, Frame::Detail(q.clone()))
                    .await
                    .unwrap();
                assert_eq!(first["target"]["number"], 7);
                assert_eq!(
                    first["outcome"]["kind"],
                    if kind == RepositoryResourceKind::Issue {
                        "issue"
                    } else {
                        "merge-request"
                    }
                );
                let cold = server.count();
                let warm = s
                    .request(&f.services, Frame::Detail(q.clone()))
                    .await
                    .unwrap();
                assert_eq!(first, warm);
                assert_eq!(cold, server.count());
                let mut q = q;
                q.refresh = true;
                assert!(s.request(&f.services, Frame::Detail(q)).await.is_ok());
                assert!(server.count() > cold);
            }
            assert!(f
                .services
                .store
                .get_workspace(&f.workspace)
                .await
                .unwrap()
                .repository_path
                .is_none());
        }
        drop(s);
        server.finish().await;
    }
}

#[intent_test_macros::daemon_test]
async fn resource_primary_denial_invalidates_held_body_and_recovers_fresh() {
    for status in [401, 403, 404] {
        let server = Server::new().await;
        let f = Fixture::new(&server).await;
        let s = f.socket().await;
        let c = s.capture(&f).await;
        let q = f.detail(&c, RepositoryResourceKind::Issue, false);
        let (original, body) = s.body(&f.services, Frame::Detail(q.clone())).await;
        assert!(body.is_ok(), "{body:?}");
        server.set(
            &route("issues"),
            status,
            json!({"message":"sensitive provider text"}),
        );
        let mut refresh = q.clone();
        refresh.refresh = true;
        let failure = s
            .request(&f.services, Frame::Detail(refresh.clone()))
            .await
            .unwrap();
        eprintln!("actual classified denial: {failure}");
        assert_eq!(failure["outcome"]["kind"], "failure");
        assert_eq!(failure["outcome"]["status"], status);
        assert_eq!(
            failure["outcome"]["code"],
            if status == 401 {
                "authentication"
            } else {
                "resource-denied"
            }
        );
        assert!(!failure.to_string().contains("sensitive provider text"));
        let (sent, count) = s.transfer(&original, &body).await;
        eprintln!("held pre-denial transfer {sent:?}/{count}");
        assert!(sent.is_err());
        assert_eq!(count, 0);
        original.retire();
        drop(original);
        server.set(&route("issues"), 200, resource("issues"));
        // Recovery is a fresh qualified scope/read, never resurrection of the refused body.
        if status == 401 {
            f.services
                .gitlab_connect_pat(server.host.clone(), "stored-pat".into())
                .await
                .unwrap();
        }
        let fresh = s.capture(&f).await;
        let before = server.count();
        let recovered = s
            .request(
                &f.services,
                Frame::Detail(f.detail(&fresh, RepositoryResourceKind::Issue, false)),
            )
            .await
            .unwrap();
        assert_eq!(recovered["outcome"]["kind"], "issue");
        assert!(server.count() > before);
        drop(s);
        server.finish().await;
    }
}

#[intent_test_macros::daemon_test]
async fn resource_pre_denial_inflight_success_cannot_restore_eligibility() {
    let server = Server::new().await;
    let f = Fixture::new(&server).await;
    let s = f.socket().await;
    let c = s.capture(&f).await;
    let q = f.detail(&c, RepositoryResourceKind::Issue, true);
    server.pause(&route("issues"));
    {
        let first = s.body(&f.services, Frame::Detail(q.clone()));
        tokio::pin!(first);
        tokio::select! {_=&mut first=>panic!("read did not reach original held provider"),()=server.entered()=>{}}
        server.set(&route("issues"), 403, json!({"message":"denied"}));
        let denied = s
            .request(&f.services, Frame::Detail(q.clone()))
            .await
            .unwrap();
        assert_eq!(denied["outcome"]["code"], "resource-denied");
        server.state.release.notify_one();
        let (scope, late) = first.await;
        eprintln!("late original body {late:?}");
        assert!(late.is_err());
        let (sent, n) = s.transfer(&scope, &late).await;
        assert!(sent.is_ok());
        assert_eq!(n, 1, "sanitized public error is permitted");
        scope.retire();
        drop(scope);
    }
    server.set(&route("issues"), 200, resource("issues"));
    let fresh = s.capture(&f).await;
    assert!(s
        .request(
            &f.services,
            Frame::Detail(f.detail(&fresh, RepositoryResourceKind::Issue, false))
        )
        .await
        .is_ok());
    drop(s);
    server.finish().await;
}

#[intent_test_macros::daemon_test]
async fn resource_optional_transient_rate_and_project_denial_keep_distinct_evidence() {
    for (path, status, expected) in [
        (
            format!("{}/approvals", route("merge_requests")),
            403,
            "restricted",
        ),
        (
            format!("{}/approvals", route("merge_requests")),
            500,
            "transient",
        ),
        (
            format!("{}/approvals", route("merge_requests")),
            429,
            "rate-limited",
        ),
        (route("issues"), 500, "transient"),
        (route("issues"), 429, "rate-limited"),
        (
            "/api/v4/projects/Team%2FSub%2FProject".into(),
            403,
            "project-denied",
        ),
    ] {
        let server = Server::new().await;
        let f = Fixture::new(&server).await;
        let s = f.socket().await;
        let c = s.capture(&f).await;
        server.set(&path, status, json!({"message":"do not expose"}));
        let kind = if path.contains("/issues/") {
            RepositoryResourceKind::Issue
        } else {
            RepositoryResourceKind::MergeRequest
        };
        let value = s
            .request(&f.services, Frame::Detail(f.detail(&c, kind, true)))
            .await
            .unwrap();
        eprintln!("typed optional/failure outcome {value}");
        assert!(!value.to_string().contains("do not expose"));
        if path.ends_with("/approvals") {
            assert_eq!(value["outcome"]["kind"], "merge-request");
            assert_eq!(
                value["outcome"]["snapshot"]["availability"]["approvals"],
                expected
            );
        } else {
            assert_eq!(value["outcome"]["kind"], "failure");
            assert_eq!(value["outcome"]["code"], expected);
        }
        assert_eq!(
            value["quota"]["remaining"],
            if status == 429 { "0" } else { "17" }
        );
        drop(s);
        server.finish().await;
    }
}

#[intent_test_macros::daemon_test]
async fn resource_target_identity_and_foreign_services_never_fallback() {
    let server = Server::new().await;
    let f = Fixture::new(&server).await;
    let s = f.socket().await;
    let foreign = f.socket().await;
    let c = s.capture(&f).await;
    let count = server.count();
    let q = f.detail(&c, RepositoryResourceKind::Issue, false);
    assert!(foreign
        .request(&f.services, Frame::Detail(q.clone()))
        .await
        .is_err());
    assert_eq!(server.count(), count);
    let second = Fixture::new(&server).await;
    let before = server.count();
    assert!(s.request(&second.services, Frame::Detail(q)).await.is_err());
    assert_eq!(server.count(), before);
    for (instance, project) in [
        ("https://unknown.test", PROJECT),
        ("https://forge.test/install", PROJECT),
        ("https://forge.test:8443/other", PROJECT),
        (INSTANCE, "Team/../Project"),
        (INSTANCE, "Team/%2FProject"),
        (INSTANCE, "Team//Project"),
        (INSTANCE, "Team\\Project"),
        ("https://forge.test:8443/install/", PROJECT),
    ] {
        let c = s.capture(&f).await;
        let mut q = f.detail(&c, RepositoryResourceKind::Issue, false);
        q.target.repository.instance_base_url = instance.into();
        q.target.repository.project_path = project.into();
        let before = server.count();
        assert!(s.request(&f.services, Frame::Detail(q)).await.is_err());
        assert_eq!(before, server.count());
    }
    for kind in [
        RepositoryResourceKind::Issue,
        RepositoryResourceKind::MergeRequest,
    ] {
        let route_kind = if kind == RepositoryResourceKind::Issue {
            "issues"
        } else {
            "merge_requests"
        };
        for mismatch in ["project", "iid"] {
            let c = s.capture(&f).await;
            let mut wrong = resource(route_kind);
            if mismatch == "project" {
                wrong["web_url"] = json!(format!("{INSTANCE}/Other/Project/-/{route_kind}/7"));
            } else {
                wrong["iid"] = json!(8);
            }
            server.set(&route(route_kind), 200, wrong);
            assert!(s
                .request(&f.services, Frame::Detail(f.detail(&c, kind, true)))
                .await
                .is_err());
            // A fresh authorized read must fetch the now-correct original item.
            server.set(&route(route_kind), 200, resource(route_kind));
            let fresh = s.capture(&f).await;
            let before = server.count();
            assert!(s
                .request(&f.services, Frame::Detail(f.detail(&fresh, kind, false)))
                .await
                .is_ok());
            assert!(server.count() > before);
        }
    }
    drop(foreign);
    drop(s);
    server.finish().await;
}

#[intent_test_macros::daemon_test]
async fn resource_real_authority_changes_refuse_first_protected_transfer_no_resurrection() {
    for mode in [
        "role",
        "credential",
        "workspace",
        "settings",
        "account",
        "release",
        "disconnect",
    ] {
        let server = Server::new().await;
        let f = Fixture::new(&server).await;
        let (s, person) = f.member(HostRole::Member).await;
        let c = s.capture(&f).await;
        let (scope, body) = s
            .body(
                &f.services,
                Frame::Detail(f.detail(&c, RepositoryResourceKind::Issue, false)),
            )
            .await;
        assert!(body.is_ok(), "{body:?}");
        let count = server.count();
        match mode {
            "role" => {
                f.services
                    .store
                    .remove_host_member(&person.id)
                    .await
                    .unwrap();
            }
            "credential" => {
                assert!(f
                    .services
                    .store
                    .revoke_principal_credential("resource-member-token-hash")
                    .await
                    .unwrap());
                f.services
                    .store
                    .insert_principal_credential(&person.id, "resource-replacement-token-hash")
                    .await
                    .unwrap();
            }
            "workspace" => {
                let original = f.services.store.get_workspace(&f.workspace).await.unwrap();
                f.services
                    .store
                    .delete_workspace(&f.workspace)
                    .await
                    .unwrap();
                f.services.store.insert_workspace(&original).await.unwrap();
            }
            "settings" => {
                for value in [true, false] {
                    with_caller(
                        Caller::Daemon,
                        f.services
                            .settings_update(json!([{ "path":"git.autoCommit", "value":value }])),
                    )
                    .await
                    .unwrap();
                }
            }
            "account" => {
                server.set(
                    "/api/v4/user",
                    200,
                    json!({"id":99,"username":"replacement","name":"Replacement"}),
                );
                f.services
                    .gitlab_connect_pat(server.host.clone(), "stored-pat".into())
                    .await
                    .unwrap();
                server.set(
                    "/api/v4/user",
                    200,
                    json!({"id":42,"username":"fixture","name":"Fixture"}),
                );
                f.services
                    .gitlab_connect_pat(server.host.clone(), "stored-pat".into())
                    .await
                    .unwrap();
            }
            "release" => {
                s.request(
                    &f.services,
                    Frame::Release(Bound {
                        workspace_id: f.workspace.clone(),
                        read_lifetime_id: c.read_lifetime_id.clone(),
                    }),
                )
                .await
                .unwrap();
            }
            "disconnect" => s.owner.retire(),
            _ => unreachable!(),
        }
        let (sent, n) = s.transfer(&scope, &body).await;
        eprintln!("{mode}: original protected delivery {sent:?}; callbacks {n}");
        assert!(sent.is_err());
        assert_eq!(n, 0);
        scope.retire();
        drop(scope);
        let after = server.count();
        assert!(s
            .request(
                &f.services,
                Frame::Detail(f.detail(&c, RepositoryResourceKind::Issue, false))
            )
            .await
            .is_err());
        assert_eq!(server.count(), after);
        if mode != "account" {
            assert_eq!(count, after);
        }
        drop(s);
        server.finish().await;
    }
}

#[intent_test_macros::daemon_test]
async fn resource_release_cancels_original_provider_future_and_joins() {
    let server = Server::new().await;
    let f = Fixture::new(&server).await;
    let s = f.socket().await;
    let c = s.capture(&f).await;
    server.pause(&route("issues"));
    {
        let read = s.body(
            &f.services,
            Frame::Detail(f.detail(&c, RepositoryResourceKind::Issue, false)),
        );
        tokio::pin!(read);
        tokio::select! {_=&mut read=>panic!("unexpected original body"),()=server.entered()=>{}}
        s.request(
            &f.services,
            Frame::Release(Bound {
                workspace_id: f.workspace.clone(),
                read_lifetime_id: c.read_lifetime_id.clone(),
            }),
        )
        .await
        .unwrap();
        let (scope, body) = tokio::time::timeout(ACQUIRE, read).await.unwrap();
        eprintln!("released original request {body:?}");
        assert!(body.is_err());
        scope.retire();
        drop(scope);
        server.state.release.notify_one();
    }
    drop(s);
    server.finish().await;
    assert_eq!(
        f.services
            .repository_resource_capacity
            .frames
            .available_permits(),
        LIMIT
    );
    assert_eq!(
        f.services
            .repository_resource_capacity
            .leases
            .available_permits(),
        LIMIT
    );
}

#[intent_test_macros::daemon_test]
async fn resource_nonrenewing_expiry_and_aggregate_capacity() {
    fixed_clock(async {
        let server = Server::new().await;
        let f = Fixture::new(&server).await;
        let mut s = f.socket().await;
        let c = s.capture(&f).await;
        let created = Instant::now();
        tokio::time::advance(TTL + Duration::from_millis(1)).await;
        let notice = s.retirements.next().await.unwrap();
        assert_eq!(notice.read_lifetime_ids, vec![c.read_lifetime_id.clone()]);
        assert!(Instant::now() - created >= TTL);
        assert!(s
            .request(
                &f.services,
                Frame::Detail(f.detail(&c, RepositoryResourceKind::Issue, false))
            )
            .await
            .is_err());
        let mut captures = Vec::new();
        for _ in 0..LIMIT {
            captures.push(s.capture(&f).await);
        }
        assert!(s
            .request(&f.services, Frame::Capture(f.query()))
            .await
            .is_err());
        let other = f.socket().await;
        assert!(other
            .request(&f.services, Frame::Capture(f.query()))
            .await
            .is_err());
        for c in captures {
            let q = Bound {
                workspace_id: f.workspace.clone(),
                read_lifetime_id: c.read_lifetime_id,
            };
            s.request(&f.services, Frame::Release(q)).await.unwrap();
        }
        assert_eq!(
            f.services
                .repository_resource_capacity
                .leases
                .available_permits(),
            LIMIT
        );
        let c = other.capture(&f).await;
        assert!(other
            .request(
                &f.services,
                Frame::Detail(f.detail(&c, RepositoryResourceKind::Issue, false))
            )
            .await
            .is_ok());
        drop(other);
        drop(s);
        server.finish().await;
    })
    .await;
}

#[intent_test_macros::daemon_test]
async fn resource_release_authority_wait_has_a_fixed_bound() {
    fixed_clock(async {
        let server = Server::new().await;
        let f = Fixture::new(&server).await;
        let s = f.socket().await;
        let capture = s.capture(&f).await;
        let before = server.count();
        let pool = f.services.store.read_pool();
        let mut held = Vec::new();
        for _ in 0..pool.options().get_max_connections() {
            held.push(pool.acquire().await.unwrap());
        }
        eprintln!(
            "release original Store read connections held={}",
            held.len()
        );
        assert_eq!(held.len(), pool.options().get_max_connections() as usize);
        let mut original = Box::pin(s.body(
            &f.services,
            Frame::Release(Bound {
                workspace_id: f.workspace.clone(),
                read_lifetime_id: capture.read_lifetime_id,
            }),
        ));
        let start = Instant::now();
        let entered =
            std::future::poll_fn(|cx| std::task::Poll::Ready(original.as_mut().poll(cx))).await;
        eprintln!(
            "release original first poll pending={}",
            entered.is_pending()
        );
        assert!(entered.is_pending());
        // Cross the original timer's millisecond tick, without increasing its
        // production deadline or inferring an internal wait from Pending.
        tokio::time::advance(ACQUIRE + Duration::from_millis(1)).await;
        tokio::task::yield_now().await;
        let returned =
            std::future::poll_fn(|cx| std::task::Poll::Ready(original.as_mut().poll(cx))).await;
        eprintln!(
            "release original after fixed advance ready={} elapsed={:?}",
            returned.is_ready(),
            start.elapsed()
        );
        let std::task::Poll::Ready((scope, body)) = returned else {
            panic!("original release did not return at its acquisition bound");
        };
        drop(original);
        eprintln!("release original bounded body={body:?}");
        assert!(matches!(&body, Err(Error::Forbidden(_))));
        assert_eq!(start.elapsed(), ACQUIRE + Duration::from_millis(1));
        let (sent, callbacks) = s.transfer(&scope, &body).await;
        eprintln!("release sanitized error transfer={sent:?} callbacks={callbacks}");
        assert!(sent.is_ok());
        assert_eq!(callbacks, 1);
        scope.retire();
        drop(scope);
        drop(held);
        assert_eq!(
            f.services
                .store
                .get_workspace(&f.workspace)
                .await
                .unwrap()
                .id,
            f.workspace
        );
        assert_eq!(server.count(), before);
        drop(s);
        assert_eq!(
            f.services
                .repository_resource_capacity
                .frames
                .available_permits(),
            LIMIT
        );
        assert_eq!(
            f.services
                .repository_resource_capacity
                .leases
                .available_permits(),
            LIMIT
        );
        server.finish().await;
    })
    .await;
}

#[cfg(target_os = "linux")]
struct FileReads(std::fs::File);
#[cfg(target_os = "linux")]
impl FileReads {
    fn new(path: &std::path::Path) -> Self {
        use std::os::fd::{AsRawFd, FromRawFd};
        use std::os::unix::ffi::OsStrExt;
        // SAFETY: owned new descriptor, valid fixed flags; converted once.
        let fd = unsafe { libc::inotify_init1(libc::IN_CLOEXEC | libc::IN_NONBLOCK) };
        assert!(fd >= 0);
        // SAFETY: this function owns the successful newly allocated fd.
        let file = unsafe { std::fs::File::from_raw_fd(fd) };
        let path = std::ffi::CString::new(path.as_os_str().as_bytes()).unwrap();
        // SAFETY: NUL terminated path remains live during the call; fd is owned.
        assert!(
            unsafe { libc::inotify_add_watch(file.as_raw_fd(), path.as_ptr(), libc::IN_OPEN) } >= 0
        );
        Self(file)
    }
    fn observed(&mut self) -> bool {
        let mut bytes = [0; 4096];
        match std::io::Read::read(&mut self.0, &mut bytes) {
            Ok(n) => {
                assert!(n >= std::mem::size_of::<libc::inotify_event>());
                true
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => false,
            Err(e) => panic!("owned secret-file observation failed {e}"),
        }
    }
}
#[cfg(target_os = "linux")]
#[intent_test_macros::daemon_test]
async fn resource_warm_cache_and_descriptor_do_not_open_secret_file() {
    let server = Server::new().await;
    let f = Fixture::new(&server).await;
    let s = f.socket().await;
    let mut opens = FileReads::new(&f.dir.path().join("secrets.json"));
    let before = server.count();
    let c = s.capture(&f).await;
    assert!(!opens.observed());
    assert_eq!(server.count(), before);
    let q = f.detail(&c, RepositoryResourceKind::Issue, false);
    s.request(&f.services, Frame::Detail(q.clone()))
        .await
        .unwrap();
    assert!(
        opens.observed(),
        "positive current cold-read file observation"
    );
    let cold = server.count();
    s.request(&f.services, Frame::Detail(q)).await.unwrap();
    assert!(!opens.observed());
    assert_eq!(cold, server.count());
    drop(s);
    server.finish().await;
}

#[intent_test_macros::daemon_test]
async fn resource_frame_read_budget_and_feed_loss_are_bounded() {
    let server = Server::new().await;
    let f = Fixture::new(&server).await;
    let s = f.socket().await;
    let held = s
        .entered(async {
            (0..LIMIT)
                .map(|_| {
                    s.owner
                        .capture_resource(&Frame::Capture(f.query()))
                        .unwrap()
                })
                .collect::<Vec<_>>()
        })
        .await;
    assert!(s
        .request(&f.services, Frame::Capture(f.query()))
        .await
        .is_err());
    assert_eq!(
        f.services
            .repository_resource_capacity
            .frames
            .available_permits(),
        0
    );
    for request in &held {
        request.retire();
    }
    drop(held);
    assert_eq!(
        f.services
            .repository_resource_capacity
            .frames
            .available_permits(),
        LIMIT
    );
    let c = s.capture(&f).await;
    let q = f.detail(&c, RepositoryResourceKind::Issue, false);
    for _ in 0..LIMIT {
        s.request(&f.services, Frame::Detail(q.clone()))
            .await
            .unwrap();
    }
    let before = server.count();
    assert!(s.request(&f.services, Frame::Detail(q)).await.is_err());
    assert_eq!(server.count(), before);
    let mut another = f.socket().await;
    let c = another.capture(&f).await;
    let lost = std::mem::replace(
        &mut another.retirements,
        Box::new(Receiver {
            connection: Weak::new(),
            settings: None,
            events: None,
        }),
    );
    drop(lost);
    assert!(another
        .request(
            &f.services,
            Frame::Detail(f.detail(&c, RepositoryResourceKind::Issue, false))
        )
        .await
        .is_err());
    drop(another);
    drop(s);
    server.finish().await;
}
