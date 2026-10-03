//! Local HTTP evidence only. The owning Services tests supply actual captured
//! Wire lifetimes; direct wrapper tests below use an explicit fixture credential.
use super::*;
use intent_core::FileSecretStore;
use intent_sourcecontrol::{GitlabDescriptor, GitlabHost, GitlabInstance};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Default)]
pub(super) struct Control {
    pub requests: Mutex<Vec<(String, String)>>,
    pub project: Mutex<Option<(u16, Value)>>,
    pub reviews: Mutex<Vec<Value>>,
    pub sha: Mutex<String>,
    pub posts: AtomicUsize,
    pub lost_post: AtomicBool,
    pub pause_post: AtomicBool,
    pub post_entered: Notify,
    pub post_release: Notify,
    pub malformed_post: AtomicBool,
    pub pause: Mutex<Option<String>>,
    pub entered: Notify,
    pub release: Notify,
}
pub(super) struct Server {
    pub host: GitlabHost,
    pub descriptor: GitlabDescriptor,
    pub control: Arc<Control>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Server {
    pub async fn new() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let host = GitlabHost::parse("gitlab.test")
            .unwrap()
            .with_api_origin(&origin)
            .unwrap();
        let descriptor = GitlabDescriptor::with_loopback_endpoint(
            GitlabInstance::parse("https://gitlab.test/forge").unwrap(),
            &origin,
        )
        .unwrap();
        let control = Arc::new(Control::default());
        let state = control.clone();
        let task = tokio::spawn(async move {
            let mut children = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    pair=listener.accept()=>{
                        let Ok((mut socket,_))=pair else{return};
                        let c=state.clone();
                        children.spawn(async move {
                            let mut request=Vec::new();
                            let mut buf=[0;4096];
                            let (header_end,len)=loop {
                                let Ok(n)=socket.read(&mut buf).await else{return};
                                if n==0{return;} request.extend_from_slice(&buf[..n]);
                                if request.len()>131_072{return;}
                                if let Some(end)=request.windows(4).position(|v|v==b"\r\n\r\n") {
                                    let headers=String::from_utf8_lossy(&request[..end]);
                                    let len=headers.lines().find_map(|l|{let (k,v)=l.split_once(':')?; k.eq_ignore_ascii_case("content-length").then(||v.trim().parse::<usize>().unwrap())}).unwrap_or(0);
                                    if request.len()>=end+4+len{break(end,len);}
                                }
                            };
                            let header=String::from_utf8_lossy(&request[..header_end]);
                            let mut words=header.split_whitespace();
                            let method=words.next().unwrap().to_string();
                            let path=words.next().unwrap().to_string();
                            // Store only routing observations, never the credential header.
                            c.requests.lock().unwrap().push((method.clone(),path.clone()));
                            assert!(header.contains("stored-pat"),"owned original token required");
                            let pause=c.pause.lock().unwrap().as_ref().is_some_and(|p|path.contains(p));
                            if pause { c.pause.lock().unwrap().take(); c.entered.notify_one(); c.release.notified().await; }
                            let route=path.split('?').next().unwrap();
                            let sha=c.sha.lock().unwrap().clone();
                            let (status,body)=if route=="/api/v4/user" {
                                (200,json!({"id":42,"username":"fixture","name":"Fixture","web_url":"https://gitlab.test/forge/fixture"}))
                            } else if route.ends_with("/repository/branches") {
                                (200,json!([{"name":"main","commit":{"id":sha}},{"name":"trunk","commit":{"id":sha}}]))
                            } else if route.contains("/repository/branches/") {
                                (200,json!({"name":route.rsplit('/').next().unwrap(),"commit":{"id":sha}}))
                            } else if route.ends_with("/merge_requests") && method=="POST" {
                                let input:Value=serde_json::from_slice(&request[header_end+4..header_end+4+len]).unwrap();
                                assert_eq!(input["source_branch"],"main"); assert_eq!(input["target_branch"],"trunk");
                                assert_ne!(input["draft"],true);
                                c.posts.fetch_add(1,Ordering::SeqCst);
                                let review=review(&sha);
                                c.reviews.lock().unwrap().push(review.clone());
                                if c.pause_post.swap(false,Ordering::SeqCst) {c.post_entered.notify_one(); c.post_release.notified().await;}
                                if c.lost_post.load(Ordering::SeqCst){return;}
                                if c.malformed_post.load(Ordering::SeqCst){(201,json!({"unconfirmed":true}))}else{(201,review)}
                            } else if route.ends_with("/merge_requests") {
                                (200,json!(*c.reviews.lock().unwrap()))
                            } else if route.starts_with("/api/v4/projects/") {
                                c.project.lock().unwrap().clone().unwrap_or((200,json!({"id":42,"path_with_namespace":"group/project","name":"project","default_branch":"trunk","web_url":"https://gitlab.test/forge/group/project"})))
                            } else { (404,json!({"message":"missing"})) };
                            let bytes=body.to_string();
                            let reply=format!("HTTP/1.1 {status} fixture\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{bytes}",bytes.len());
                            let _=socket.write_all(reply.as_bytes()).await;
                        });
                    }
                    _=children.join_next(),if !children.is_empty()=>{}
                }
            }
        });
        Self {
            host,
            descriptor,
            control,
            task,
        }
    }
}
pub(super) fn review(sha: &str) -> Value {
    json!({"iid":7,"project_id":42,"source_project_id":42,"target_project_id":42,"source_branch":"main","target_branch":"trunk","state":"opened","draft":false,"title":"Original ready MR","description":"original body","web_url":"https://gitlab.test/forge/group/project/-/merge_requests/7","sha":sha,"author":{"username":"fixture"},"created_at":"2026-01-01T00:00:00Z","updated_at":"2026-01-01T00:00:00Z"})
}
pub(super) async fn services(
    server: &Server,
    git: &crate::repository_admission_source_tests::fixtures::Fixture,
) -> (Arc<Services>, Arc<crate::SettingsRegistry>) {
    let registry =
        Arc::new(crate::SettingsRegistry::load(git.dir.path().join("config.toml")).unwrap());
    registry
        .apply(&[
            ("sourceControl.gitlab.host".into(), json!("gitlab.test")),
            (
                "sourceControl.gitlab.instanceBaseUrl".into(),
                json!(server.descriptor.instance().as_str()),
            ),
            (
                "sourceControl.gitlab.apiBaseUrl".into(),
                json!(server.host.base_url()),
            ),
            ("sourceControl.gitlab.oauthClientId".into(), json!("client")),
        ])
        .unwrap();
    let secrets = FileSecretStore::with_path(git.dir.path().join("secrets.json"));
    secrets
        .store(
            intent_sourcecontrol::gitlab_token::SECRET_ACCOUNT,
            "stored-pat",
        )
        .unwrap();
    let service = Arc::new(
        Services::new_repository_fixture(git.store.clone(), secrets, None)
            .with_settings_registry(registry.clone()),
    );
    service.initialize_repository_wire().await.unwrap();
    let guard = service.gitlab_credential_gate.lock().await;
    service
        .gitlab_credential_gate
        .install_settings_boundary(
            &registry,
            &service.secrets,
            &service.gitlab_secret_store,
            Some(server.descriptor.clone()),
        )
        .unwrap();
    drop(guard);
    service.reconcile_gitlab_repository_binding().await.unwrap();
    (service, registry)
}
struct Injected;
#[async_trait::async_trait]
impl intent_sourcecontrol::GitlabRequestCredentials for Injected {
    async fn token_for(
        &self,
        _: &GitlabInstance,
    ) -> intent_sourcecontrol::Result<intent_sourcecontrol::SecretString> {
        Ok("stored-pat".into())
    }
}
#[intent_test_macros::daemon_test]
async fn native_review_confirmed_project_wrapper_preserves_identity_and_refusals() {
    let s = Server::new().await;
    let p = GitLabSourceControl::new(s.descriptor.clone(), Arc::new(Injected)).unwrap();
    let repo = intent_core::RepoRef::new("group", "project");
    assert_eq!(
        p.confirmed_project_identity(&repo).await.unwrap(),
        (42, "group/project".into())
    );
    for value in [
        json!({"path_with_namespace":"group/project"}),
        json!({"id":0,"path_with_namespace":"group/project"}),
        json!({"id":42,"path_with_namespace":"foreign/project"}),
    ] {
        *s.control.project.lock().unwrap() = Some((200, value));
        assert!(p.confirmed_project_identity(&repo).await.is_err());
    }
    for status in [401, 403, 404] {
        *s.control.project.lock().unwrap() = Some((status, json!({"message":"refused"})));
        assert!(p.confirmed_project_identity(&repo).await.is_err());
    }
    assert_eq!(s.control.posts.load(Ordering::SeqCst), 0);
    assert_eq!(s.control.requests.lock().unwrap().len(), 7);
}

#[intent_test_macros::daemon_test]
async fn native_review_explicit_fixture_initializer_preserves_normal_and_one_time_refusal() {
    for mode in [0, 1, 2] {
        let server = Server::new().await;
        let git = crate::repository_admission_source_tests::fixtures::Fixture::new().await;
        let registry = Arc::new(
            crate::SettingsRegistry::load(git.dir.path().join("fixture-settings.toml")).unwrap(),
        );
        registry
            .apply(&[
                ("sourceControl.gitlab.host".into(), json!("gitlab.test")),
                (
                    "sourceControl.gitlab.instanceBaseUrl".into(),
                    json!(server.descriptor.instance().as_str()),
                ),
                (
                    "sourceControl.gitlab.apiBaseUrl".into(),
                    json!(server.host.base_url()),
                ),
                (
                    "sourceControl.gitlab.oauthClientId".into(),
                    json!("fixture"),
                ),
            ])
            .unwrap();
        let secrets = FileSecretStore::with_path(git.dir.path().join("fixture-secrets.json"));
        secrets
            .store(
                intent_sourcecontrol::gitlab_token::SECRET_ACCOUNT,
                "stored-pat",
            )
            .unwrap();
        let service = Services::new_repository_fixture(
            git.store.clone(),
            secrets,
            Some(server.descriptor.clone()),
        )
        .with_settings_registry(registry);
        if mode == 0 {
            assert!(service
                .initialize_gitlab_repository_binding()
                .await
                .is_err());
        } else {
            let descriptor = if mode == 1 {
                server.descriptor.clone()
            } else {
                GitlabDescriptor::with_loopback_endpoint(
                    GitlabInstance::parse("https://different.test").unwrap(),
                    server.host.base_url(),
                )
                .unwrap()
            };
            let result = service
                .initialize_repository_test_fixture(descriptor.clone())
                .await;
            assert_eq!(result.is_ok(), mode == 1);
            assert!(service
                .initialize_repository_test_fixture(descriptor)
                .await
                .is_err());
        }
        assert_eq!(
            service
                .gitlab_repository_connection_facts()
                .unwrap()
                .settled()
                .is_some(),
            mode == 1
        );
        assert_eq!(server.control.posts.load(Ordering::SeqCst), 0);
    }
}
