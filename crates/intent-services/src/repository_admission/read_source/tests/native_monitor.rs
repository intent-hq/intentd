//! Native recovery uses original persisted targets and fresh account admission.
use super::*;
use intent_core::repository_request::{
    RepositoryPrivateOutput, RepositoryReadConnection, RepositoryReadReplyKind,
    RepositoryReadRetirements, RepositoryWireEntry,
};
use intent_core::review_monitor::ReviewMonitorRecipientSupport;
use intent_core::{Caller, HostRole, with_caller};

struct NativeRecipient {
    services: Arc<Services>,
    caller: Caller,
    connection: Arc<dyn RepositoryReadConnection>,
    _retirements: Box<dyn RepositoryReadRetirements>,
}
impl NativeRecipient {
    async fn new(services: Arc<Services>) -> Self {
        services.initialize_repository_wire().await.unwrap();
        let caller = Caller::Wire {
            principal_id: services.store.get_primary_principal().await.unwrap().id,
            host_role: HostRole::Owner,
        };
        let connection = with_caller(caller.clone(), async {
            services
                .repository_read_connection(RepositoryWireEntry::AdmittedLocal)
                .unwrap()
        })
        .await;
        let retirements = connection.take_retirements().unwrap();
        with_caller(caller.clone(), async {
            connection
                .review_monitoring_hello(ReviewMonitorRecipientSupport::from_capabilities(
                    &json!({"reviewMonitoring":1}),
                ))
                .unwrap()
                .acknowledge();
        })
        .await;
        Self {
            services,
            caller,
            connection,
            _retirements: retirements,
        }
    }
    async fn list(&self, workspace: WorkspaceId) -> (Value, Arc<dyn RepositoryPrivateOutput>) {
        let services = self.services.clone();
        let connection = self.connection.clone();
        with_caller(self.caller.clone(), async {
            let scope = connection.capture();
            let mut result = None;
            scope
                .scope(Box::pin(async {
                    let value = services.pr_monitor_list(workspace, None).await.unwrap();
                    let frame = json!({"jsonrpc":"2.0","id":1,"result":value}).to_string();
                    let output = scope.bind_private_output(&frame).unwrap().unwrap();
                    scope
                        .deliver(RepositoryReadReplyKind::Result, &mut || Ok(()))
                        .await
                        .unwrap();
                    result = Some((value, output));
                }))
                .await;
            scope.retire();
            result.unwrap()
        })
        .await
    }
}
impl Drop for NativeRecipient {
    fn drop(&mut self) {
        self.connection.retire();
    }
}

#[intent_test_macros::daemon_test]
async fn native_monitor_qualified_list_uses_original_target_without_runtime_authority() {
    let provider = ReadServer::new().await;
    let f = ActualRead::new(&provider).await;
    routing_options(&f);
    let registered = routing_json(&run(&f.server(), "return await ws.mr.monitor(4);").await);
    assert_eq!(registered["ok"], true);
    let id = intent_core::PrMonitorId(registered["monitor"]["monitorId"].as_str().unwrap().into());
    let services = &f.auth.service;
    let before = services.store.get_pr_monitor(&id).await.unwrap();
    // Loss of runtime registration does not turn the stored account/target into
    // permission. The native recipient must independently admit that same tuple.
    services.review_monitor_authorities.lock().unwrap().clear();
    f.git.git(
        &f.git.path,
        &[
            "remote",
            "set-url",
            "origin",
            "https://github.com/other/replacement.git",
        ],
    );
    let calls = provider.count();
    let native = NativeRecipient::new(services.clone()).await;
    let (listed, output) = native.list(f.git.workspace.id.clone()).await;
    assert_eq!(
        listed["monitors"].as_array().unwrap().len(),
        1,
        "fresh original account admission must recover the explicit monitor row"
    );
    assert_eq!(listed["monitors"][0]["monitorId"], id.0);
    assert_eq!(
        listed["monitors"][0]["resource"],
        registered["monitor"]["resource"]
    );
    assert_eq!(listed["monitors"][0]["agentId"], f.agent.0);
    assert!(!output.frame().contains("monitorBinding"));
    output.with_current(&mut || Ok(())).unwrap();
    assert_eq!(services.store.get_pr_monitor(&id).await.unwrap(), before);
    assert_eq!(
        provider.count(),
        calls,
        "list admission cannot read through today's remote"
    );
    f.git.git(&f.git.path, &["remote", "remove", "origin"]);
    let (listed, _) = native.list(f.git.workspace.id.clone()).await;
    assert_eq!(
        listed["monitors"][0]["resource"],
        registered["monitor"]["resource"]
    );
    assert_eq!(services.store.get_pr_monitor(&id).await.unwrap(), before);
}

#[intent_test_macros::daemon_test]
async fn native_monitor_qualified_list_account_replacement_retires_queued_output() {
    let provider = ReadServer::new().await;
    let f = ActualRead::new(&provider).await;
    routing_options(&f);
    let registered = routing_json(&run(&f.server(), "return await ws.mr.monitor(4);").await);
    let id = intent_core::PrMonitorId(registered["monitor"]["monitorId"].as_str().unwrap().into());
    let services = &f.auth.service;
    let original = services.store.get_pr_monitor(&id).await.unwrap();
    let native = NativeRecipient::new(services.clone()).await;
    let (first, pending) = native.list(f.git.workspace.id.clone()).await;
    assert_eq!(first["monitors"].as_array().unwrap().len(), 1);
    pending.with_current(&mut || Ok(())).unwrap();
    services
        .gitlab_connect_pat(provider.fixture.host.clone(), "pat-second".into())
        .await
        .unwrap();
    let mut wrote = false;
    assert!(
        pending
            .with_current(&mut || {
                wrote = true;
                Ok(())
            })
            .is_err()
    );
    assert!(
        !wrote,
        "account replacement must fence the entire queued frame"
    );
    let calls = provider.count();
    let (other_account, _) = native.list(f.git.workspace.id.clone()).await;
    assert!(other_account["monitors"].as_array().unwrap().is_empty());
    assert_eq!(
        provider.count(),
        calls,
        "list cannot probe on another account"
    );
    assert_eq!(services.store.get_pr_monitor(&id).await.unwrap(), original);
    services
        .gitlab_connect_pat(provider.fixture.host.clone(), "stored-pat".into())
        .await
        .unwrap();
    assert!(
        pending.with_current(&mut || Ok(())).is_err(),
        "A-to-B-to-A cannot revive an old output receipt"
    );
    let (fresh, current) = native.list(f.git.workspace.id.clone()).await;
    assert_eq!(
        fresh["monitors"][0]["resource"],
        first["monitors"][0]["resource"]
    );
    current.with_current(&mut || Ok(())).unwrap();
    assert_eq!(services.store.get_pr_monitor(&id).await.unwrap(), original);
    native.connection.retire();
    assert!(current.with_current(&mut || Ok(())).is_err());
}
