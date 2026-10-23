//! Actual Services/ACP requests against a controlled peer, not native runtime proof.
use super::*;
use crate::repository_admission::lifecycle::physical_owner::{
    RepositoryCreationIntent, RepositoryCreationOwner,
};

#[derive(Clone, Copy, PartialEq, Eq)]
enum HeldMutation {
    None,
    Joined,
    Rejected,
    Cancelled,
    Retired,
    Initialized,
    Compatible,
    InitializedRejected,
    InitializedCancelled,
    CompatibleRejected,
    CompatibleCancelled,
    PendingDelete,
    PendingDeleteRejected,
    PendingDeleteCancelled,
    Credential,
    CredentialRejected,
    CredentialCancelled,
    DirectPat,
    DirectPatRejected,
    DirectPatCancelled,
    DirectRevoke,
    DirectRevokeRejected,
    DirectRevokeCancelled,
    DirectDevice,
    DirectDeviceRejected,
    DirectDeviceCancelled,
    ProactiveRefresh,
    ProactiveRefreshRejected,
    ProactiveRefreshCancelled,
    ProofRefresh,
    ProofRefreshRejected,
    ProofRefreshCancelled,
    RejectedAccessRefresh,
    RejectedAccessRefreshRejected,
    RejectedAccessRefreshCancelled,
    RejectedAccessExpired,
    RejectedAccessExpiredRejected,
    RejectedAccessExpiredCancelled,
    ProactiveExpired,
    ProactiveExpiredRejected,
    ProactiveExpiredCancelled,
    DirectSettings,
    DirectSettingsRejected,
    DirectSettingsCancelled,
    SettingsSecret,
    SettingsSecretRejected,
    SettingsSecretCancelled,
    SettingsReset,
    SettingsResetRejected,
    SettingsResetCancelled,
    SettingsConcurrent,
    SettingsOverlap,
    SettingsReadError,
    SettingsReadTimeout,
    SettingsReload,
    SettingsReloadRejected,
    SettingsReloadCancelled,
    SettingsReloadDrain,
    ReconcileStored,
    ReconcileStoredRejected,
    ReconcileStoredCancelled,
    ReconcileStoredChanged,
    ReconcileRefresh,
    ReconcileRefreshRejected,
    ReconcileRefreshCancelled,
    ReconcileRefreshChanged,
    DirectPatChanged,
    DirectDeviceChanged,
    DirectRevokeChanged,
    SettingsSecretChanged,
    SettingsResetChanged,
    ProactiveExpiredChanged,
    RejectedAccessExpiredChanged,
    SettingsBatchError,
    SettingsBatchErrorRejected,
    SettingsBatchErrorCancelled,
    SettingsCompensationError,
    SettingsCompensationErrorRejected,
    SettingsCompensationErrorCancelled,
}

#[tokio::test]
async fn services_failed_batch_waits_for_native_join_and_preserves_original_error() {
    run_qualified_new(false, false, HeldMutation::SettingsBatchError).await;
}

#[tokio::test]
async fn services_failed_batch_error_precedes_negative_native_join() {
    run_qualified_new(false, false, HeldMutation::SettingsBatchErrorRejected).await;
}

#[tokio::test]
async fn services_failed_batch_cancelled_waiter_retains_native_completion() {
    run_qualified_new(false, false, HeldMutation::SettingsBatchErrorCancelled).await;
}

#[tokio::test]
async fn services_failed_compensation_waits_for_native_join_without_rewriting_bytes() {
    run_qualified_new(false, false, HeldMutation::SettingsCompensationError).await;
}

#[tokio::test]
async fn services_failed_compensation_error_precedes_negative_native_join() {
    run_qualified_new(
        false,
        false,
        HeldMutation::SettingsCompensationErrorRejected,
    )
    .await;
}

#[tokio::test]
async fn services_failed_compensation_cancelled_waiter_retains_native_completion() {
    run_qualified_new(
        false,
        false,
        HeldMutation::SettingsCompensationErrorCancelled,
    )
    .await;
}

// A milestone is positive progress, never permission for the held mutation to
// complete. Poll the mutation first even when both futures are ready together.
async fn while_retirement_held<T, R: std::fmt::Debug>(
    mutation: std::pin::Pin<&mut impl std::future::Future<Output = R>>,
    milestone: impl std::future::Future<Output = T>,
    description: &str,
) -> T {
    timeout(intentd_test_support::test_timeout(Duration::from_secs(5)), async {
        tokio::select! {
            biased;
            result = mutation => panic!("Store completion escaped held retirement while waiting for {description}: {result:?}"),
            value = milestone => value,
        }
    })
    .await
    .unwrap_or_else(|_| panic!("held retirement did not reach {description}"))
}

#[tokio::test]
#[should_panic(
    expected = "Store completion escaped held retirement while waiting for ready milestone"
)]
async fn retirement_progress_rejects_simultaneous_completion() {
    let mut mutation = Box::pin(std::future::ready(()));
    while_retirement_held(mutation.as_mut(), std::future::ready(()), "ready milestone").await;
}

async fn run_qualified_new(bad_echo: bool, drop_confirmed: bool, mutation_case: HeldMutation) {
    let batch_error = matches!(
        mutation_case,
        HeldMutation::SettingsBatchError
            | HeldMutation::SettingsBatchErrorRejected
            | HeldMutation::SettingsBatchErrorCancelled
            | HeldMutation::SettingsCompensationError
            | HeldMutation::SettingsCompensationErrorRejected
            | HeldMutation::SettingsCompensationErrorCancelled
    );
    let compensation_error = matches!(
        mutation_case,
        HeldMutation::SettingsCompensationError
            | HeldMutation::SettingsCompensationErrorRejected
            | HeldMutation::SettingsCompensationErrorCancelled
    );
    let mutation_case = match mutation_case {
        HeldMutation::SettingsBatchError | HeldMutation::SettingsCompensationError => {
            HeldMutation::SettingsSecret
        }
        HeldMutation::SettingsBatchErrorRejected
        | HeldMutation::SettingsCompensationErrorRejected => HeldMutation::SettingsSecretRejected,
        HeldMutation::SettingsBatchErrorCancelled
        | HeldMutation::SettingsCompensationErrorCancelled => HeldMutation::SettingsSecretCancelled,
        other => other,
    };
    let hold_mutation = mutation_case != HeldMutation::None;
    let reject_join = matches!(
        mutation_case,
        HeldMutation::Rejected
            | HeldMutation::InitializedRejected
            | HeldMutation::CompatibleRejected
            | HeldMutation::PendingDeleteRejected
            | HeldMutation::CredentialRejected
            | HeldMutation::DirectPatRejected
            | HeldMutation::DirectRevokeRejected
            | HeldMutation::DirectDeviceRejected
            | HeldMutation::ProactiveRefreshRejected
            | HeldMutation::ProofRefreshRejected
            | HeldMutation::RejectedAccessRefreshRejected
            | HeldMutation::RejectedAccessExpiredRejected
            | HeldMutation::ProactiveExpiredRejected
            | HeldMutation::DirectSettingsRejected
            | HeldMutation::SettingsSecretRejected
            | HeldMutation::SettingsResetRejected
            | HeldMutation::SettingsReloadRejected
            | HeldMutation::ReconcileStoredRejected
            | HeldMutation::ReconcileRefreshRejected
    );
    let cancel_waiter = matches!(
        mutation_case,
        HeldMutation::Cancelled
            | HeldMutation::InitializedCancelled
            | HeldMutation::CompatibleCancelled
            | HeldMutation::PendingDeleteCancelled
            | HeldMutation::CredentialCancelled
            | HeldMutation::DirectPatCancelled
            | HeldMutation::DirectRevokeCancelled
            | HeldMutation::DirectDeviceCancelled
            | HeldMutation::ProactiveRefreshCancelled
            | HeldMutation::ProofRefreshCancelled
            | HeldMutation::RejectedAccessRefreshCancelled
            | HeldMutation::RejectedAccessExpiredCancelled
            | HeldMutation::ProactiveExpiredCancelled
            | HeldMutation::DirectSettingsCancelled
            | HeldMutation::SettingsSecretCancelled
            | HeldMutation::SettingsResetCancelled
            | HeldMutation::SettingsReloadCancelled
            | HeldMutation::ReconcileStoredCancelled
            | HeldMutation::ReconcileRefreshCancelled
    );
    let compatible = matches!(
        mutation_case,
        HeldMutation::Compatible
            | HeldMutation::CompatibleRejected
            | HeldMutation::CompatibleCancelled
    );
    let pending_delete = matches!(
        mutation_case,
        HeldMutation::PendingDelete
            | HeldMutation::PendingDeleteRejected
            | HeldMutation::PendingDeleteCancelled
    );
    let initialize = compatible
        || matches!(
            mutation_case,
            HeldMutation::Initialized
                | HeldMutation::InitializedRejected
                | HeldMutation::InitializedCancelled
        );
    let (tmp, services, bus, agent, workspace) = setup().await;
    let credential_server = if matches!(
        mutation_case,
        HeldMutation::DirectPat
            | HeldMutation::DirectPatChanged
            | HeldMutation::DirectPatRejected
            | HeldMutation::DirectPatCancelled
            | HeldMutation::DirectRevoke
            | HeldMutation::DirectRevokeChanged
            | HeldMutation::DirectRevokeRejected
            | HeldMutation::DirectRevokeCancelled
            | HeldMutation::DirectDevice
            | HeldMutation::DirectDeviceChanged
            | HeldMutation::DirectDeviceRejected
            | HeldMutation::DirectDeviceCancelled
            | HeldMutation::ProactiveRefresh
            | HeldMutation::ProactiveRefreshRejected
            | HeldMutation::ProactiveRefreshCancelled
            | HeldMutation::ProofRefresh
            | HeldMutation::ProofRefreshRejected
            | HeldMutation::ProofRefreshCancelled
            | HeldMutation::RejectedAccessRefresh
            | HeldMutation::RejectedAccessRefreshRejected
            | HeldMutation::RejectedAccessRefreshCancelled
            | HeldMutation::RejectedAccessExpired
            | HeldMutation::RejectedAccessExpiredChanged
            | HeldMutation::RejectedAccessExpiredRejected
            | HeldMutation::RejectedAccessExpiredCancelled
            | HeldMutation::ProactiveExpired
            | HeldMutation::ProactiveExpiredChanged
            | HeldMutation::ProactiveExpiredRejected
            | HeldMutation::ProactiveExpiredCancelled
            | HeldMutation::DirectSettings
            | HeldMutation::DirectSettingsRejected
            | HeldMutation::DirectSettingsCancelled
            | HeldMutation::SettingsSecret
            | HeldMutation::SettingsSecretChanged
            | HeldMutation::SettingsSecretRejected
            | HeldMutation::SettingsSecretCancelled
            | HeldMutation::SettingsReset
            | HeldMutation::SettingsResetChanged
            | HeldMutation::SettingsResetRejected
            | HeldMutation::SettingsResetCancelled
            | HeldMutation::SettingsConcurrent
            | HeldMutation::SettingsOverlap
            | HeldMutation::SettingsReadError
            | HeldMutation::SettingsReadTimeout
            | HeldMutation::SettingsReload
            | HeldMutation::SettingsReloadRejected
            | HeldMutation::SettingsReloadCancelled
            | HeldMutation::SettingsReloadDrain
            | HeldMutation::ReconcileStored
            | HeldMutation::ReconcileStoredRejected
            | HeldMutation::ReconcileStoredCancelled
            | HeldMutation::ReconcileStoredChanged
            | HeldMutation::ReconcileRefresh
            | HeldMutation::ReconcileRefreshRejected
            | HeldMutation::ReconcileRefreshCancelled
            | HeldMutation::ReconcileRefreshChanged
    ) {
        Some(
            crate::source_control_auth_ops::repository_owner::secret_reader::tests::Server::new()
                .await,
        )
    } else {
        None
    };
    let services = if let Some(forge) = &credential_server {
        let settings =
            Arc::new(crate::SettingsRegistry::load(tmp.dir.path().join("config.toml")).unwrap());
        settings
            .apply(&[
                ("sourceControl.gitlab.host".into(), json!(forge.host.host())),
                (
                    "sourceControl.gitlab.oauthClientId".into(),
                    json!("fixture-client"),
                ),
            ])
            .unwrap();
        Services::new_repository_fixture(
            services.store.clone(),
            intent_core::FileSecretStore::with_path(tmp.dir.path().join("secrets.json")),
            Some(forge.descriptor.clone()),
        )
        .with_settings_registry(settings)
        .with_event_bus(bus.clone())
    } else {
        services
    };
    if matches!(
        mutation_case,
        HeldMutation::DirectSettings
            | HeldMutation::DirectSettingsRejected
            | HeldMutation::DirectSettingsCancelled
            | HeldMutation::SettingsSecret
            | HeldMutation::SettingsSecretChanged
            | HeldMutation::SettingsSecretRejected
            | HeldMutation::SettingsSecretCancelled
            | HeldMutation::SettingsReset
            | HeldMutation::SettingsResetChanged
            | HeldMutation::SettingsResetRejected
            | HeldMutation::SettingsResetCancelled
            | HeldMutation::SettingsConcurrent
            | HeldMutation::SettingsOverlap
            | HeldMutation::SettingsReadError
            | HeldMutation::SettingsReadTimeout
            | HeldMutation::SettingsReload
            | HeldMutation::SettingsReloadRejected
            | HeldMutation::SettingsReloadCancelled
            | HeldMutation::SettingsReloadDrain
            | HeldMutation::ReconcileStored
            | HeldMutation::ReconcileStoredRejected
            | HeldMutation::ReconcileStoredCancelled
            | HeldMutation::ReconcileStoredChanged
            | HeldMutation::ReconcileRefresh
            | HeldMutation::ReconcileRefreshRejected
            | HeldMutation::ReconcileRefreshCancelled
            | HeldMutation::ReconcileRefreshChanged
    ) {
        let forge = credential_server.as_ref().unwrap();
        services
            .settings_registry
            .as_ref()
            .unwrap()
            .apply(&[
                (
                    "sourceControl.gitlab.instanceBaseUrl".into(),
                    json!(forge.descriptor.instance().as_str()),
                ),
                (
                    "sourceControl.gitlab.apiBaseUrl".into(),
                    json!(forge.host.base_url()),
                ),
            ])
            .unwrap();
        services
            .initialize_repository_test_fixture(forge.descriptor.clone())
            .await
            .unwrap();
        assert!(services.gitlab_credential_gate.has_settings_boundary());
    }
    let mut services = services;
    let batch_failures = if batch_error {
        let (secrets, failures) = crate::settings::completion_read_tests::failed_batch(
            &services.secrets,
            compensation_error,
        );
        services.secrets = Arc::new(secrets);
        Some(failures)
    } else {
        None
    };
    let projection_failure = matches!(
        mutation_case,
        HeldMutation::SettingsReadError | HeldMutation::SettingsReadTimeout
    );
    let read_failure = if projection_failure {
        let (secrets, failure) =
            crate::settings::completion_read_tests::controlled(&services.secrets);
        services.secrets = Arc::new(secrets);
        Some(failure)
    } else {
        None
    };
    let registry = services.repository_lifecycle_registry().await.unwrap();
    let creation = RepositoryCreationOwner::allocate(
        &registry,
        &services.store,
        workspace.clone(),
        agent.clone(),
        RepositoryCreationIntent::FirstSet,
    )
    .unwrap();
    let (client, peer) = tokio::io::duplex(16384);
    let (read, write) = tokio::io::split(client);
    let connection = Arc::new(Connection::new(
        write,
        read,
        None,
        ConnectionHooks::default(),
    ));
    let captured = Arc::new(std::sync::Mutex::new(Vec::<Value>::new()));
    let seen = captured.clone();
    let (retire_seen, mut retire_ready) = tokio::sync::oneshot::channel();
    let (retire_release, mut retire_wait) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (read, mut write) = tokio::io::split(peer);
        let mut lines = BufReader::new(read).lines();
        let mut retire_seen = Some(retire_seen);
        while let Some(line) = lines.next_line().await.unwrap() {
            let request: Value = serde_json::from_str(&line).unwrap();
            seen.lock().unwrap().push(request.clone());
            let result = match request["method"].as_str().unwrap() {
                "initialize" => {
                    json!({"protocolVersion":1,"agentCapabilities":{"loadSession":true},"authMethods":[],"_meta":{"reviewMonitoring":1,"intentCallbackRegistration":{"version":1,"method":"_intent/session/register_mcp_callback"}}})
                }
                "session/new" => {
                    json!({"sessionId":"native-original","_meta":{"reviewMonitoring":1,"reviewMonitoringContext":if bad_echo {Value::Null} else {request["params"]["_meta"]["reviewMonitoringContext"].clone()},"intentCallbackRegistration":{"version":1,"queryReceipt":"11111111-1111-4111-8111-111111111111"}}})
                }
                "_intent/session/retire_review_context" => {
                    if hold_mutation {
                        retire_seen.take().unwrap().send(()).unwrap();
                        (&mut retire_wait).await.unwrap();
                    }
                    json!({"retired":!reject_join})
                }
                other => panic!("unexpected request {other}"),
            };
            write
                .write_all(
                    format!(
                        "{}\n",
                        json!({"jsonrpc":"2.0","id":request["id"],"result":result})
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
        }
    });
    let mut provider = *intent_providers::find_provider("claude-code").unwrap();
    provider.supports_authenticate = false;
    let handshake = intent_acp::handshake::handshake_with_review_monitoring(
        connection.clone(),
        &provider,
        intent_acp::callback_registration::CallbackOffer::V1,
        intent_acp::review_monitoring::ReviewMonitoringOffer::V1,
    )
    .await
    .unwrap();
    let callbacks = handshake.callbacks.as_ref().unwrap();
    let outcome = services
        .create_repository_acp_session(
            (&connection, Some(callbacks)),
            &agent,
            "/fixture",
            vec![],
            creation,
            false,
        )
        .await;
    let context = captured
        .lock()
        .unwrap()
        .iter()
        .find(|r| r["method"] == "session/new")
        .unwrap()["params"]["_meta"]["reviewMonitoringContext"]
        .clone();
    assert_eq!(
        context["version"], 1,
        "actual Services startup must carry the original context"
    );
    if bad_echo {
        assert!(outcome.is_err());
        assert!(services
            .store
            .get_review_context_enrollment(&workspace, &agent, "native-original")
            .await
            .unwrap()
            .is_none());
        assert!(services
            .store
            .get_agent_session(&agent)
            .await
            .unwrap()
            .acp_session_id
            .is_none());
        {
            let requests = captured.lock().unwrap();
            let retired = requests
                .iter()
                .find(|r| r["method"] == "_intent/session/retire_review_context")
                .unwrap();
            assert_eq!(retired["params"]["loadAttemptId"], context["loadAttemptId"]);
            assert_eq!(retired["params"]["sessionId"], "native-original");
        }
        server.abort();
        let _ = server.await;
        return;
    }
    let outcome = outcome.unwrap();
    let saved = services
        .store
        .get_review_context_enrollment(&workspace, &agent, "native-original")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(saved.owner_key(), context["ownerKey"].as_str().unwrap());
    assert!(outcome.owner.is_ok());
    assert!(
        outcome
            .query
            .as_ref()
            .unwrap()
            .pending_review_monitoring()
            .unwrap()
            .acknowledged()
            .is_none(),
        "startup echo must not imply callback ACK"
    );
    // A local enrollment never licenses the ordinary, unprepared loader.
    assert!(services
        .prepare_acp_load(
            &connection,
            Some(callbacks),
            &handshake.ordinary.initialize,
            &agent,
            "/fixture",
            vec![]
        )
        .await
        .is_err());
    assert!(!captured
        .lock()
        .unwrap()
        .iter()
        .any(|r| r["method"] == "session/load"));
    if let Some(forge) = &credential_server {
        let changed_source = matches!(
            mutation_case,
            HeldMutation::DirectPatChanged
                | HeldMutation::DirectDeviceChanged
                | HeldMutation::DirectRevokeChanged
                | HeldMutation::SettingsSecretChanged
                | HeldMutation::SettingsResetChanged
                | HeldMutation::ProactiveExpiredChanged
                | HeldMutation::RejectedAccessExpiredChanged
        );
        let device = matches!(
            mutation_case,
            HeldMutation::DirectDevice
                | HeldMutation::DirectDeviceChanged
                | HeldMutation::DirectDeviceRejected
                | HeldMutation::DirectDeviceCancelled
        );
        let refresh = matches!(
            mutation_case,
            HeldMutation::ReconcileRefresh
                | HeldMutation::ReconcileRefreshRejected
                | HeldMutation::ReconcileRefreshCancelled
                | HeldMutation::ReconcileRefreshChanged
                | HeldMutation::ProactiveRefresh
                | HeldMutation::ProactiveRefreshRejected
                | HeldMutation::ProactiveRefreshCancelled
                | HeldMutation::ProofRefresh
                | HeldMutation::ProofRefreshRejected
                | HeldMutation::ProofRefreshCancelled
                | HeldMutation::RejectedAccessRefresh
                | HeldMutation::RejectedAccessRefreshRejected
                | HeldMutation::RejectedAccessRefreshCancelled
                | HeldMutation::RejectedAccessExpired
                | HeldMutation::RejectedAccessExpiredChanged
                | HeldMutation::RejectedAccessExpiredRejected
                | HeldMutation::RejectedAccessExpiredCancelled
                | HeldMutation::ProactiveExpired
                | HeldMutation::ProactiveExpiredChanged
                | HeldMutation::ProactiveExpiredRejected
                | HeldMutation::ProactiveExpiredCancelled
        );
        let rejected_access = matches!(
            mutation_case,
            HeldMutation::RejectedAccessRefresh
                | HeldMutation::RejectedAccessRefreshRejected
                | HeldMutation::RejectedAccessRefreshCancelled
                | HeldMutation::RejectedAccessExpired
                | HeldMutation::RejectedAccessExpiredChanged
                | HeldMutation::RejectedAccessExpiredRejected
                | HeldMutation::RejectedAccessExpiredCancelled
        );
        let expired = matches!(
            mutation_case,
            HeldMutation::RejectedAccessExpired
                | HeldMutation::RejectedAccessExpiredChanged
                | HeldMutation::RejectedAccessExpiredRejected
                | HeldMutation::RejectedAccessExpiredCancelled
                | HeldMutation::ProactiveExpired
                | HeldMutation::ProactiveExpiredChanged
                | HeldMutation::ProactiveExpiredRejected
                | HeldMutation::ProactiveExpiredCancelled
        );
        let revoke = matches!(
            mutation_case,
            HeldMutation::DirectRevoke
                | HeldMutation::DirectRevokeChanged
                | HeldMutation::DirectRevokeRejected
                | HeldMutation::DirectRevokeCancelled
        );
        let settings_change = matches!(
            mutation_case,
            HeldMutation::DirectSettings
                | HeldMutation::DirectSettingsRejected
                | HeldMutation::DirectSettingsCancelled
                | HeldMutation::SettingsSecret
                | HeldMutation::SettingsSecretChanged
                | HeldMutation::SettingsSecretRejected
                | HeldMutation::SettingsSecretCancelled
                | HeldMutation::SettingsReset
                | HeldMutation::SettingsResetChanged
                | HeldMutation::SettingsResetRejected
                | HeldMutation::SettingsResetCancelled
                | HeldMutation::SettingsConcurrent
                | HeldMutation::SettingsOverlap
                | HeldMutation::SettingsReadError
                | HeldMutation::SettingsReadTimeout
                | HeldMutation::SettingsReload
                | HeldMutation::SettingsReloadRejected
                | HeldMutation::SettingsReloadCancelled
                | HeldMutation::SettingsReloadDrain
        );
        let settings_secret = matches!(
            mutation_case,
            HeldMutation::SettingsSecret
                | HeldMutation::SettingsSecretChanged
                | HeldMutation::SettingsSecretRejected
                | HeldMutation::SettingsSecretCancelled
        );
        let settings_reset = matches!(
            mutation_case,
            HeldMutation::SettingsReset
                | HeldMutation::SettingsResetChanged
                | HeldMutation::SettingsResetRejected
                | HeldMutation::SettingsResetCancelled
        );
        let reconcile = matches!(
            mutation_case,
            HeldMutation::ReconcileStored
                | HeldMutation::ReconcileStoredRejected
                | HeldMutation::ReconcileStoredCancelled
                | HeldMutation::ReconcileStoredChanged
                | HeldMutation::ReconcileRefresh
                | HeldMutation::ReconcileRefreshRejected
                | HeldMutation::ReconcileRefreshCancelled
                | HeldMutation::ReconcileRefreshChanged
        );
        if (revoke || refresh || settings_change || reconcile)
            && !matches!(
                mutation_case,
                HeldMutation::ReconcileRefresh
                    | HeldMutation::ReconcileRefreshRejected
                    | HeldMutation::ReconcileRefreshCancelled
                    | HeldMutation::ReconcileRefreshChanged
            )
        {
            intent_core::with_caller(
                intent_core::Caller::Daemon,
                services.gitlab_connect_pat(forge.host.clone(), "pat-original".into()),
            )
            .await
            .unwrap();
        }
        if matches!(
            mutation_case,
            HeldMutation::ReconcileRefresh
                | HeldMutation::ReconcileRefreshRejected
                | HeldMutation::ReconcileRefreshCancelled
                | HeldMutation::ReconcileRefreshChanged
        ) {
            // Cold stored grant: no published fingerprint is rewritten by this fixture.
            services
                .gitlab_secret_store
                .store(
                    intent_sourcecontrol::gitlab_token::SECRET_ACCOUNT,
                    "pat-original",
                )
                .unwrap();
        }
        if refresh {
            services
                .gitlab_secret_store
                .store(
                    intent_sourcecontrol::gitlab_token::REFRESH_SECRET_ACCOUNT,
                    "original-refresh",
                )
                .unwrap();
            services
                .gitlab_secret_store
                .store(
                    intent_sourcecontrol::gitlab_token::EXPIRES_AT_SECRET_ACCOUNT,
                    if rejected_access { "9999999999" } else { "0" },
                )
                .unwrap();
            if rejected_access {
                *forge.control.denied_user.lock().unwrap() = Some("pat-original");
            }
        }
        if expired {
            *forge.control.token_reply.lock().unwrap() =
                Some((400, json!({"error":"invalid_grant"})));
        }
        let mut events = bus.subscribe(SubscriptionFilter {
            event_types: vec!["sourceControl:auth-changed".into()],
            ..Default::default()
        });
        let mut settings_events = bus.subscribe(SubscriptionFilter {
            event_types: vec!["settings:changed".into()],
            ..Default::default()
        });
        let directory = services.repository_connection_directory();
        let native = outcome.owner.as_ref().unwrap().native_allocation_for_test();
        directory.native_contexts.retain(native.clone()).unwrap();
        let mut completion_events = bus.subscribe(SubscriptionFilter {
            event_types: vec!["sourceControl:auth-changed".into()],
            ..Default::default()
        });
        let mut operation = Box::pin(intent_core::with_caller(
            intent_core::Caller::Daemon,
            async {
                if settings_change {
                    use intent_core::WorkspaceApi;
                    if matches!(
                        mutation_case,
                        HeldMutation::SettingsReload
                            | HeldMutation::SettingsReloadRejected
                            | HeldMutation::SettingsReloadCancelled
                            | HeldMutation::SettingsReloadDrain
                    ) {
                        let registry = services.settings_registry.as_ref().unwrap();
                        let expected = registry.snapshot();
                        let text = std::fs::read_to_string(registry.config_path())
                            .unwrap()
                            .replace("fixture-client", "replacement-client");
                        std::fs::write(registry.config_path(), &text).unwrap();
                        let (_stop, stopped) = tokio::sync::watch::channel(false);
                        services
                            .apply_prepared_settings_reload(
                                text,
                                expected,
                                crate::config_watcher::PreparedReloadAdmission::new(stopped),
                            )
                            .await
                            .map(|_changed| json!({"ok":true}))
                    } else if settings_reset {
                        services
                            .settings_reset("sourceControl.gitlab.token".into())
                            .await
                    } else if batch_error {
                        services
                            .settings_update(json!([
                                {"path":"sourceControl.gitlab.token","value":"replacement-pat"},
                                {"path":"linear.token","value":"failed-batch-write"}
                            ]))
                            .await
                    } else if settings_secret {
                        services.settings_update(json!([{"path":"sourceControl.gitlab.token","value":"replacement-pat"}])).await
                    } else if projection_failure {
                        services.settings_update(json!([
                        {"path":"sourceControl.gitlab.oauthClientId","value":"replacement-client"},
                        {"path":"linear.token","value":"persisted-linear-token"},
                        {"path":"identity.provider","value":"gitlab"}
                    ])).await
                    } else if mutation_case == HeldMutation::SettingsOverlap {
                        services.settings_update(json!([
                        {"path":"sourceControl.gitlab.oauthClientId","value":"replacement-client"},
                        {"path":"git.autoCommit","value":false}
                    ])).await
                    } else {
                        services.settings_update(json!([{"path":"sourceControl.gitlab.oauthClientId","value":"replacement-client"}])).await
                    }
                } else if device {
                    // Starting a device flow is not credential publication. Wait
                    // for its real background supervisor's completion event here.
                    services.gitlab_connect_device(forge.host.clone()).await?;
                    completion_events.recv().await.unwrap();
                    Ok(json!({"ok":true}))
                } else if reconcile {
                    services
                        .reconcile_gitlab_repository_binding()
                        .await
                        .map(|()| json!({"ok":true}))
                } else if refresh {
                    if matches!(
                        mutation_case,
                        HeldMutation::ProofRefresh
                            | HeldMutation::ProofRefreshRejected
                            | HeldMutation::ProofRefreshCancelled
                    ) {
                        let token = services
                            .stored_proof_token(&crate::source_control_auth_ops::Target::Gitlab {
                                host: forge.host.clone(),
                            })
                            .await?;
                        assert_eq!(token, "rotated");
                    } else {
                        let result =
                            crate::source_control_auth_ops::probe_gitlab(&services, &forge.host)
                                .await?;
                        if expired {
                            assert_eq!(
                                result,
                                crate::source_control_auth_ops::ProbeOutcome::NotConfigured
                            );
                        } else {
                            assert!(
                                matches!(
                                    result,
                                    crate::source_control_auth_ops::ProbeOutcome::Configured {
                                        method: "device",
                                        ..
                                    }
                                ),
                                "original refresh did not yield admitted account"
                            );
                        }
                    }
                    Ok(json!({"ok":true}))
                } else if revoke {
                    services.gitlab_revoke_owned(forge.host.clone()).await
                } else {
                    services
                        .gitlab_connect_pat(forge.host.clone(), "pat-original".into())
                        .await
                }
            },
        ));
        tokio::select! {
            result = &mut operation => panic!("credential completion escaped original retirement: {result:?}"),
            _ = &mut retire_ready => {},
            () = tokio::time::sleep(Duration::from_secs(5)) => panic!("credential did not retire original allocation"),
        }
        assert!(native.is_retired());
        assert!(
            timeout(
                intentd_test_support::test_timeout(Duration::from_millis(30)),
                &mut operation
            )
            .await
            .is_err(),
            "credential success escaped held native join"
        );
        let gate = timeout(
            intentd_test_support::test_timeout(Duration::from_secs(5)),
            services.gitlab_credential_gate.lock(),
        )
        .await
        .expect("native join retained credential gate");
        drop(gate);
        if settings_change {
            let accounts = timeout(
                intentd_test_support::test_timeout(Duration::from_secs(5)),
                services
                    .settings_secret_gates
                    .write(&["sourceControl.gitlab.token"]),
            )
            .await
            .expect("native join retained secret account gate");
            drop(accounts);
            let revision = timeout(
                intentd_test_support::test_timeout(Duration::from_secs(5)),
                services.settings_revision_gate.write(),
            )
            .await
            .expect("native join retained settings revision gate");
            drop(revision);
        }
        assert!(directory.binding().is_err());
        assert!(
            timeout(
                intentd_test_support::test_timeout(Duration::from_millis(30)),
                events.recv()
            )
            .await
            .is_err(),
            "auth event escaped original retirement"
        );
        if settings_change {
            assert!(
                timeout(
                    intentd_test_support::test_timeout(Duration::from_millis(30)),
                    settings_events.recv()
                )
                .await
                .is_err(),
                "settings success event escaped native completion"
            );
        }
        let unrelated_revision = if matches!(
            mutation_case,
            HeldMutation::SettingsConcurrent | HeldMutation::SettingsOverlap
        ) {
            use intent_core::WorkspaceApi;
            let result = timeout(intentd_test_support::test_timeout(Duration::from_secs(5)), intent_core::with_caller(intent_core::Caller::Daemon,
                services.settings_update(json!([{"path":"git.autoCommit","value":mutation_case == HeldMutation::SettingsOverlap}])))).await
                .expect("held native completion blocked unrelated settings").unwrap();
            let revision = result["revision"].as_u64().unwrap();
            let batch = timeout(
                intentd_test_support::test_timeout(Duration::from_secs(5)),
                settings_events.recv(),
            )
            .await
            .unwrap()
            .unwrap();
            assert!(batch.iter().any(|event| event.data["revision"] == revision));
            Some(revision)
        } else {
            None
        };
        let mut finite_drain = (mutation_case == HeldMutation::SettingsReloadDrain)
            .then(|| Box::pin(services.store_tasks.drain_finite()));
        if let Some(drain) = finite_drain.as_mut() {
            assert!(
                timeout(
                    intentd_test_support::test_timeout(Duration::from_millis(30)),
                    drain.as_mut()
                )
                .await
                .is_err(),
                "finite-task drain escaped the held reload retirement"
            );
        }
        let changed_reconcile = matches!(
            mutation_case,
            HeldMutation::ReconcileStoredChanged | HeldMutation::ReconcileRefreshChanged
        );
        if changed_reconcile || changed_source {
            // Actual out-of-band rewrite while the original native join is held.
            services
                .gitlab_secret_store
                .store(
                    intent_sourcecontrol::gitlab_token::SECRET_ACCOUNT,
                    "later-token",
                )
                .unwrap();
        }
        let hook_generation = services
            .identity_rekey_generation
            .load(std::sync::atomic::Ordering::SeqCst);
        if let Some(failure) = &read_failure {
            failure.store(
                if mutation_case == HeldMutation::SettingsReadTimeout {
                    2
                } else {
                    1
                },
                std::sync::atomic::Ordering::SeqCst,
            );
        }
        let persisted = std::fs::read(tmp.dir.path().join("secrets.json")).unwrap();
        if batch_error {
            assert_eq!(
                batch_failures
                    .as_ref()
                    .unwrap()
                    .load(std::sync::atomic::Ordering::SeqCst),
                if compensation_error { 3 } else { 1 },
                "original persistence and requested compensation failures must actually execute"
            );
            assert_eq!(
                services
                    .gitlab_secret_store
                    .load("sourceControl.gitlab.token")
                    .unwrap()
                    .as_deref(),
                Some(if compensation_error {
                    "replacement-pat"
                } else {
                    "pat-original"
                })
            );
        }
        let persisted_config =
            settings_change.then(|| std::fs::read(tmp.dir.path().join("config.toml")).unwrap());
        if device && (reject_join || changed_source) {
            drop(operation);
            retire_release.send(()).unwrap();
            timeout(
                intentd_test_support::test_timeout(Duration::from_secs(5)),
                services.store_tasks.drain_finite(),
            )
            .await
            .expect("rejected device retirement left its original supervisor running");
            assert!(services.gitlab_auth.lock().await.flow.is_none());
        } else if cancel_waiter {
            drop(operation);
            retire_release.send(()).unwrap();
        } else {
            retire_release.send(()).unwrap();
            let result = timeout(
                intentd_test_support::test_timeout(Duration::from_secs(5)),
                operation,
            )
            .await
            .unwrap();
            if batch_error {
                assert!(result.is_err());
                assert!(
                    format!("{result:?}").contains("injected original batch persistence failure"),
                    "native join replaced original persistence error: {result:?}"
                );
            } else if reject_join {
                assert!(
                    matches!(result, Err(intent_core::Error::Internal(ref message)) if message == "original native retirement unconfirmed")
                );
            } else if projection_failure {
                assert!(
                    result.is_err(),
                    "failed completion read fabricated a successful projection"
                );
            } else if changed_source {
                assert!(
                    result.is_err(),
                    "substituted source received original operation success"
                );
            } else if changed_reconcile {
                assert!(
                    matches!(result, Err(intent_core::Error::Internal(ref message)) if message == "repository credential settings require their original prepared owner")
                );
            } else {
                let result = result.unwrap();
                if mutation_case == HeldMutation::SettingsOverlap {
                    let value = result["applied"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .find(|entry| entry["path"] == "git.autoCommit")
                        .unwrap();
                    assert_eq!(
                        value["value"], true,
                        "deferred response replayed superseded value"
                    );
                    assert!(services.effective_settings().git.auto_commit);
                }
                if let Some(revision) = unrelated_revision {
                    assert!(
                        result["revision"].as_u64().unwrap() > revision,
                        "native completion returned an older revision than the intervening update"
                    );
                }
            }
        }
        if projection_failure {
            assert_eq!(
                services
                    .identity_rekey_generation
                    .load(std::sync::atomic::Ordering::SeqCst),
                hook_generation,
                "failed completion read invoked the success hook"
            );
        }
        if batch_error && cancel_waiter {
            timeout(
                intentd_test_support::test_timeout(Duration::from_secs(5)),
                services.settings_tasks.drain_finite(),
            )
            .await
            .expect("cancelled failed batch lost retained completion");
        }
        if reject_join || changed_reconcile || changed_source || projection_failure || batch_error {
            assert!(directory.binding().is_err());
            assert!(
                timeout(
                    intentd_test_support::test_timeout(Duration::from_millis(30)),
                    events.recv()
                )
                .await
                .is_err(),
                "unconfirmed retirement published auth event"
            );
            if settings_change {
                assert!(
                    timeout(
                        intentd_test_support::test_timeout(Duration::from_millis(30)),
                        settings_events.recv()
                    )
                    .await
                    .is_err(),
                    "unconfirmed retirement published settings success"
                );
            }
        } else if settings_change {
            let batch = timeout(
                intentd_test_support::test_timeout(Duration::from_secs(5)),
                settings_events.recv(),
            )
            .await
            .unwrap()
            .unwrap();
            if mutation_case == HeldMutation::SettingsOverlap {
                let value = batch
                    .iter()
                    .flat_map(|event| event.data["changes"].as_array().unwrap())
                    .find(|entry| entry["path"] == "git.autoCommit")
                    .unwrap();
                assert_eq!(
                    value["value"], true,
                    "deferred event replayed superseded value"
                );
            }
            if let Some(revision) = unrelated_revision {
                assert!(
                    batch
                        .iter()
                        .all(|event| event.data["revision"].as_u64().unwrap() > revision),
                    "deferred settings event regressed revision"
                );
            }
            if settings_secret {
                assert!(directory.binding().is_ok());
            } else {
                assert!(
                    directory.binding().is_err(),
                    "metadata/reset must not adopt retained secrets"
                );
            }
            assert!(timeout(
                intentd_test_support::test_timeout(Duration::from_millis(30)),
                events.recv()
            )
            .await
            .is_err());
        } else if expired {
            let batch = timeout(
                intentd_test_support::test_timeout(Duration::from_secs(5)),
                events.recv(),
            )
            .await
            .unwrap()
            .unwrap();
            assert!(batch.iter().any(|event| event.data["status"] == "expired"));
            assert!(directory.binding().is_err());
            assert!(services
                .gitlab_secret_store
                .load(intent_sourcecontrol::gitlab_token::SECRET_ACCOUNT)
                .unwrap()
                .is_none());
        } else if refresh || reconcile {
            if cancel_waiter {
                timeout(
                    intentd_test_support::test_timeout(Duration::from_secs(5)),
                    async {
                        while directory.binding().is_err() {
                            tokio::task::yield_now().await;
                        }
                    },
                )
                .await
                .expect("cancelled refresh lost original native completion");
            }
            assert!(directory.binding().is_ok());
            assert!(
                timeout(
                    intentd_test_support::test_timeout(Duration::from_millis(30)),
                    events.recv()
                )
                .await
                .is_err(),
                "refresh invented an auth transition"
            );
        } else {
            let batch = timeout(
                intentd_test_support::test_timeout(Duration::from_secs(5)),
                events.recv(),
            )
            .await
            .unwrap()
            .unwrap();
            assert!(
                batch
                    .iter()
                    .any(|event| event.data["status"]
                        == if revoke { "revoked" } else { "authorized" })
            );
            if revoke {
                assert!(directory.binding().is_err());
            } else {
                assert!(directory.binding().is_ok());
            }
        }
        if let Some(drain) = finite_drain {
            timeout(
                intentd_test_support::test_timeout(Duration::from_secs(5)),
                drain,
            )
            .await
            .expect("finite-task drain lost the original reload completion");
        }
        assert_eq!(
            std::fs::read(tmp.dir.path().join("secrets.json")).unwrap(),
            persisted,
            "native completion changed the already persisted credential outcome"
        );
        if let Some(persisted_config) = persisted_config {
            assert_eq!(
                std::fs::read(tmp.dir.path().join("config.toml")).unwrap(),
                persisted_config,
                "native completion rewrote committed settings"
            );
        }
        server.abort();
        let _ = server.await;
        return;
    }
    if matches!(
        mutation_case,
        HeldMutation::Credential
            | HeldMutation::CredentialRejected
            | HeldMutation::CredentialCancelled
    ) {
        use crate::repository_credentials::{
            RepositoryCredentialError, RepositoryMutationKind, SettledCredentialState,
        };
        // Explicit component enrollment of the SAME allocation: this fixture
        // tests the writer boundary, not original-account source admission.
        let directory = services.repository_connection_directory();
        let native = outcome.owner.as_ref().unwrap().native_allocation_for_test();
        directory.native_contexts.retain(native.clone()).unwrap();
        let ticket = directory
            .reserve_mutation(RepositoryMutationKind::Replace)
            .unwrap();
        directory.begin_mutation(&ticket).unwrap();
        assert!(
            native.is_retired(),
            "credential mutation did not synchronously close native eligibility"
        );
        timeout(
            intentd_test_support::test_timeout(Duration::from_secs(5)),
            &mut retire_ready,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            directory.finish_mutation(&ticket, SettledCredentialState::Disconnected),
            Err(RepositoryCredentialError::Indeterminate)
        );
        let mut join = Box::pin(directory.native_contexts.join());
        assert!(timeout(
            intentd_test_support::test_timeout(Duration::from_millis(30)),
            &mut join
        )
        .await
        .is_err());
        assert!(
            directory.binding().is_err(),
            "credential publication escaped held native join"
        );
        let joined = if cancel_waiter {
            drop(join);
            retire_release.send(()).unwrap();
            timeout(
                intentd_test_support::test_timeout(Duration::from_secs(5)),
                directory.native_contexts.join(),
            )
            .await
            .unwrap()
        } else {
            retire_release.send(()).unwrap();
            timeout(
                intentd_test_support::test_timeout(Duration::from_secs(5)),
                join,
            )
            .await
            .unwrap()
        };
        if reject_join {
            assert!(joined.is_err());
            assert_eq!(
                directory.finish_mutation(&ticket, SettledCredentialState::Disconnected),
                Err(RepositoryCredentialError::Indeterminate)
            );
        } else {
            joined.unwrap();
            assert!(directory
                .finish_mutation(&ticket, SettledCredentialState::Disconnected)
                .unwrap()
                .is_none());
        }
        assert_eq!(
            services
                .store
                .get_agent_session(&agent)
                .await
                .unwrap()
                .acp_session_id
                .as_deref(),
            Some("native-original")
        );
        server.abort();
        let _ = server.await;
        return;
    }
    if hold_mutation {
        let expected = if mutation_case == HeldMutation::Retired || pending_delete {
            "native-original"
        } else {
            "replacement"
        };
        let native = outcome.owner.as_ref().unwrap().native_allocation_for_test();
        // Force the schedule that used to fail: retirement has begun, but the
        // SQLite worker cannot commit until the test observes the old row.
        // Capture eligibility BEFORE blocking: waiting for a retirement request
        // here first would conceal a write-before-retirement regression.
        let commit_hold = if pending_delete {
            None
        } else {
            let (entered, waiting) = tokio::sync::oneshot::channel();
            let (release, blocked) = std::sync::mpsc::channel::<()>();
            let mut first_commit = Some((entered, blocked));
            let native = native.clone();
            let mut writer = services.store.write_pool().acquire().await.unwrap();
            writer
                .lock_handle()
                .await
                .unwrap()
                .set_commit_hook(move || {
                    let Some((entered, blocked)) = first_commit.take() else {
                        return true;
                    };
                    let _ = entered.send(native.is_retired());
                    // Timeout bounds a broken fixture; sender drop on assertion
                    // failure also unblocks the worker (and rejects the commit).
                    blocked
                        .recv_timeout(intentd_test_support::test_timeout(Duration::from_secs(10)))
                        .is_ok()
                });
            Some((waiting, release))
        };
        let (settlement_started, settlement_ready) = tokio::sync::oneshot::channel();
        let mut mutation = Box::pin(async {
            if mutation_case == HeldMutation::Retired {
                services
                    .store
                    .set_agent_session_retired_at(
                        &workspace,
                        &agent,
                        Some("2026-10-10T12:19:00Z"),
                        "2026-10-10T12:19:00Z",
                    )
                    .await?;
                Ok(expected.to_owned())
            } else if pending_delete {
                let guard = services
                    .store
                    .begin_repository_pending_delete(&[
                        intent_store::RepositoryLifecycleKey::Agent(agent.clone()),
                    ])
                    .await?;
                // Pending deletion releases serialization before begin and
                // does not write SQL. Neither a row nor an unlocked writer
                // proves retained settlement ownership. Its first poll spawns
                // that owner synchronously before waiting for the native join.
                assert!(
                    native.is_retired(),
                    "pending deletion did not close native eligibility"
                );
                let mut settlement = Box::pin(guard.settle_confirmed());
                let first_poll = std::future::poll_fn(|cx| {
                    std::task::Poll::Ready(std::future::Future::poll(settlement.as_mut(), cx))
                })
                .await;
                assert!(
                    first_poll.is_pending(),
                    "Store completion escaped held retirement during pending deletion"
                );
                settlement_started.send(()).unwrap();
                settlement.await?;
                Ok(expected.to_owned())
            } else if initialize {
                let replacement = RepositoryCreationOwner::allocate(
                    &registry,
                    &services.store,
                    workspace.clone(),
                    agent.clone(),
                    RepositoryCreationIntent::Replace {
                        expected: Some("native-original".into()),
                    },
                )
                .unwrap();
                let owner = if compatible {
                    let completion = replacement
                        .initialize_compatible(|| async {
                            Ok::<_, intent_core::Error>(("replacement".into(), ()))
                        })
                        .await;
                    completion.result?;
                    completion.owner?
                } else {
                    replacement
                        .initialize(&services.store, || async { Ok("replacement".into()) })
                        .await
                        .map_err(|_| {
                            intent_core::Error::Internal(
                                "initialization completion unavailable".into(),
                            )
                        })?
                };
                drop(owner);
                Ok(expected.to_owned())
            } else {
                services
                    .store
                    .replace_acp_session_id(&workspace, &agent, "native-original", "replacement")
                    .await
            }
        });
        if let Some((entered, release)) = commit_hold {
            let retired_at_commit =
                while_retirement_held(mutation.as_mut(), entered, "SQLite commit entry")
                    .await
                    .unwrap();
            assert!(retired_at_commit, "Store commit preceded native retirement");
            while_retirement_held(
                mutation.as_mut(),
                &mut retire_ready,
                "native retirement request",
            )
            .await
            .unwrap();
            let before_commit = while_retirement_held(
                mutation.as_mut(),
                services.store.get_agent_session(&agent),
                "held precommit row",
            )
            .await
            .unwrap();
            assert_eq!(
                before_commit.acp_session_id.as_deref(),
                Some("native-original")
            );
            assert!(before_commit.retired_at.is_none());
            release.send(()).unwrap();
            while_retirement_held(
                mutation.as_mut(),
                async {
                    loop {
                        let row = services.store.get_agent_session(&agent).await.unwrap();
                        let committed = if mutation_case == HeldMutation::Retired {
                            row.retired_at.as_deref() == Some("2026-10-10T12:19:00Z")
                        } else {
                            row.acp_session_id.as_deref() == Some(expected)
                        };
                        if committed {
                            break;
                        }
                        tokio::task::yield_now().await;
                    }
                },
                "committed row",
            )
            .await;
        } else {
            while_retirement_held(
                mutation.as_mut(),
                &mut retire_ready,
                "native retirement request",
            )
            .await
            .unwrap();
            while_retirement_held(
                mutation.as_mut(),
                settlement_ready,
                "retained pending-delete settlement",
            )
            .await
            .unwrap();
        }
        assert_eq!(
            while_retirement_held(
                mutation.as_mut(),
                services.store.get_agent_session(&agent),
                "intermediate session ID",
            )
            .await
            .unwrap()
            .acp_session_id
            .as_deref(),
            Some(expected)
        );
        // Preserve the no-effect API control. Its same-ID fast path takes
        // lifecycle serialization before reading. For SQL mutations this is
        // a separate milestone from visibility: releasing serialization and
        // spawning retained settlement happen in the same poll. Keep driving
        // the mutation through this boundary before cancelling its caller.
        while_retirement_held(
            mutation.as_mut(),
            services
                .store
                .set_acp_session_id(&workspace, &agent, expected),
            "Store serialization release",
        )
        .await
        .unwrap();
        // Independently acquire the actual single-connection writer.
        while_retirement_held(
            mutation.as_mut(),
            async {
                let mut writer = services.store.write_pool().acquire().await.unwrap();
                sqlx::query("BEGIN IMMEDIATE")
                    .execute(&mut *writer)
                    .await
                    .unwrap();
                sqlx::query("ROLLBACK").execute(&mut *writer).await.unwrap();
            },
            "actual single-writer pool release",
        )
        .await;
        let allocate = || {
            RepositoryCreationOwner::allocate(
                &registry,
                &services.store,
                workspace.clone(),
                agent.clone(),
                RepositoryCreationIntent::Replace {
                    expected: Some(expected.into()),
                },
            )
        };
        assert!(
            allocate().is_err(),
            "retirement wait published renewed eligibility"
        );
        if cancel_waiter {
            drop(mutation);
            retire_release.send(()).unwrap();
            timeout(
                intentd_test_support::test_timeout(Duration::from_secs(5)),
                async {
                    loop {
                        if let Ok(owner) = allocate() {
                            drop(owner);
                            break;
                        }
                        tokio::task::yield_now().await;
                    }
                },
            )
            .await
            .expect("cancelled waiter lost original committed settlement");
        } else {
            retire_release.send(()).unwrap();
            let result = timeout(
                intentd_test_support::test_timeout(Duration::from_secs(5)),
                &mut mutation,
            )
            .await
            .unwrap();
            if reject_join {
                if initialize {
                    assert!(
                        result.is_err(),
                        "unconfirmed initialization returned success"
                    );
                } else {
                    assert!(
                        matches!(result, Err(intent_core::Error::Internal(ref message)) if message == "original native retirement unconfirmed")
                    );
                }
                assert!(
                    allocate().is_err(),
                    "unconfirmed join removed its original barrier"
                );
            } else {
                assert_eq!(result.unwrap(), expected);
                assert!(allocate().is_ok());
            }
        }
        assert_eq!(
            services
                .store
                .get_agent_session(&agent)
                .await
                .unwrap()
                .acp_session_id
                .as_deref(),
            Some(expected)
        );
    }
    if drop_confirmed {
        let original = outcome.owner.as_ref().unwrap().native_allocation_for_test();
        drop(outcome);
        timeout(
            intentd_test_support::test_timeout(Duration::from_secs(5)),
            async {
                loop {
                    if captured
                        .lock()
                        .unwrap()
                        .iter()
                        .any(|r| r["method"] == "_intent/session/retire_review_context")
                    {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            },
        )
        .await
        .expect("confirmed physical owner lost its original native allocation");
        {
            let requests = captured.lock().unwrap();
            let retired = requests
                .iter()
                .find(|r| r["method"] == "_intent/session/retire_review_context")
                .unwrap();
            assert_eq!(
                retired["params"],
                json!({"sessionId":"native-original","loadAttemptId":context["loadAttemptId"]})
            );
        }
        registry
            .join_native_retirements(&[intent_store::RepositoryLifecycleKey::Agent(agent.clone())])
            .await
            .unwrap();
        assert!(services
            .store
            .get_review_context_enrollment(&workspace, &agent, "native-original")
            .await
            .unwrap()
            .is_some());
        crate::repository_admission::lifecycle::RepositoryLifecycleRegistry::verify_native_retirement_churn(callbacks.clone()).await;
        let transport = Arc::downgrade(&connection);
        drop(handshake);
        drop(connection);
        timeout(
            intentd_test_support::test_timeout(Duration::from_secs(2)),
            async {
                while transport.upgrade().is_some() {
                    tokio::task::yield_now().await;
                }
            },
        )
        .await
        .expect("positive native join retained the original ACP transport");
        original
            .join()
            .await
            .expect("existing exact waiter lost its positive tombstone");
    }
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn services_qualified_new_commits_only_original_echoed_native_family() {
    run_qualified_new(false, false, HeldMutation::None).await;
}
#[tokio::test]
async fn services_qualified_new_bad_echo_retires_exact_attempt_without_enrollment() {
    run_qualified_new(true, false, HeldMutation::None).await;
}

#[expect(
    clippy::fn_params_excessive_bools,
    reason = "independent cancellation and native-outcome regression axes"
)]
async fn retired_new_waits_for_remote_join(
    joined: bool,
    drop_waiter: bool,
    drop_before_response: bool,
    drop_after_commit: bool,
) {
    let (_tmp, services, _bus, agent, workspace) = setup().await;
    let registry = services.repository_lifecycle_registry().await.unwrap();
    let creation = RepositoryCreationOwner::allocate(
        &registry,
        &services.store,
        workspace.clone(),
        agent.clone(),
        RepositoryCreationIntent::FirstSet,
    )
    .unwrap();
    let retirement = creation.retirement();
    let (client, peer) = tokio::io::duplex(16384);
    let (read, write) = tokio::io::split(client);
    let connection = Arc::new(Connection::new(
        write,
        read,
        None,
        ConnectionHooks::default(),
    ));
    let (new_seen, mut new_ready) = tokio::sync::oneshot::channel();
    let (new_release, mut new_wait) = tokio::sync::oneshot::channel();
    let (retire_seen, mut retire_ready) = tokio::sync::oneshot::channel();
    let (retire_release, mut retire_wait) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (read, mut write) = tokio::io::split(peer);
        let mut lines = BufReader::new(read).lines();
        let mut new_seen = Some(new_seen);
        let mut retire_seen = Some(retire_seen);
        let mut original_context = Value::Null;
        while let Some(line) = lines.next_line().await.unwrap() {
            let request: Value = serde_json::from_str(&line).unwrap();
            let result = match request["method"].as_str().unwrap() {
                "initialize" => {
                    json!({"protocolVersion":1,"agentCapabilities":{"loadSession":true},"authMethods":[],"_meta":{"reviewMonitoring":1,"intentCallbackRegistration":{"version":1,"method":"_intent/session/register_mcp_callback"}}})
                }
                "session/new" => {
                    original_context =
                        request["params"]["_meta"]["reviewMonitoringContext"].clone();
                    assert_eq!(original_context["version"], 1);
                    new_seen
                        .take()
                        .unwrap()
                        .send(original_context.clone())
                        .unwrap();
                    (&mut new_wait).await.unwrap();
                    json!({"sessionId":"late-original","_meta":{"reviewMonitoring":1,"reviewMonitoringContext":original_context,"intentCallbackRegistration":{"version":1,"queryReceipt":"11111111-1111-4111-8111-111111111111"}}})
                }
                "_intent/session/retire_review_context" => {
                    assert_eq!(
                        request["params"],
                        json!({"sessionId":"late-original","loadAttemptId":original_context["loadAttemptId"]})
                    );
                    retire_seen.take().unwrap().send(()).unwrap();
                    (&mut retire_wait).await.unwrap();
                    json!({"retired": joined})
                }
                other => panic!("unexpected request {other}"),
            };
            write
                .write_all(
                    format!(
                        "{}\n",
                        json!({"jsonrpc":"2.0","id":request["id"],"result":result})
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
        }
    });
    let mut provider = *intent_providers::find_provider("claude-code").unwrap();
    provider.supports_authenticate = false;
    let handshake = intent_acp::handshake::handshake_with_review_monitoring(
        connection.clone(),
        &provider,
        intent_acp::callback_registration::CallbackOffer::V1,
        intent_acp::review_monitoring::ReviewMonitoringOffer::V1,
    )
    .await
    .unwrap();
    let operation = services.create_repository_acp_session(
        (&connection, handshake.callbacks.as_ref()),
        &agent,
        "/fixture",
        vec![],
        creation,
        false,
    );
    let mut operation = Box::pin(operation);
    let context: intent_acp::review_monitoring::ReviewMonitoringContext = tokio::select! {
        context = &mut new_ready => serde_json::from_value(context.unwrap()).unwrap(),
        _ = &mut operation => panic!("startup completed before original response"),
        () = tokio::time::sleep(Duration::from_secs(5)) => panic!("startup request absent"),
    };
    if drop_before_response || drop_after_commit {
        if drop_after_commit {
            new_release.send(()).unwrap();
            timeout(
                intentd_test_support::test_timeout(Duration::from_secs(5)),
                async {
                    loop {
                        if services
                            .store
                            .get_agent_session(&agent)
                            .await
                            .unwrap()
                            .acp_session_id
                            .as_deref()
                            == Some("late-original")
                        {
                            break;
                        }
                        tokio::task::yield_now().await;
                    }
                },
            )
            .await
            .expect("original initialization never committed");
            drop(operation);
        } else {
            drop(operation);
            new_release.send(()).unwrap();
        }
        timeout(
            intentd_test_support::test_timeout(Duration::from_secs(5)),
            &mut retire_ready,
        )
        .await
        .expect("cancelled startup lost the original response and native retirement")
        .unwrap();
        assert_eq!(
            services
                .store
                .get_agent_session(&agent)
                .await
                .unwrap()
                .acp_session_id
                .is_some(),
            drop_after_commit
        );
        assert_eq!(
            services
                .store
                .get_review_context_enrollment(&workspace, &agent, "late-original")
                .await
                .unwrap()
                .is_some(),
            drop_after_commit
        );
        let mut shutdown = Box::pin(services.store_tasks.drain_finite());
        assert!(
            timeout(
                intentd_test_support::test_timeout(Duration::from_millis(30)),
                &mut shutdown
            )
            .await
            .is_err(),
            "shutdown escaped held native retirement"
        );
        let shared = handshake
            .callbacks
            .as_ref()
            .unwrap()
            .begin_review_context_retirement("late-original", &context)
            .unwrap();
        let mut waiting = Box::pin(shared.join());
        assert!(timeout(
            intentd_test_support::test_timeout(Duration::from_millis(30)),
            &mut waiting
        )
        .await
        .is_err());
        retire_release.send(()).unwrap();
        assert_eq!(
            timeout(
                intentd_test_support::test_timeout(Duration::from_secs(5)),
                waiting
            )
            .await
            .unwrap()
            .is_ok(),
            joined
        );
        timeout(
            intentd_test_support::test_timeout(Duration::from_secs(5)),
            &mut shutdown,
        )
        .await
        .expect("retained startup task did not finish");
        assert_eq!(
            services
                .store
                .get_agent_session(&agent)
                .await
                .unwrap()
                .acp_session_id
                .is_some(),
            drop_after_commit
        );
        assert_eq!(
            services
                .store
                .get_review_context_enrollment(&workspace, &agent, "late-original")
                .await
                .unwrap()
                .is_some(),
            drop_after_commit
        );
        server.abort();
        let _ = server.await;
        return;
    }
    retirement.retire();
    new_release.send(()).unwrap();
    tokio::select! {
        _ = &mut retire_ready => {},
        _ = &mut operation => panic!("startup completed without joining original native allocation"),
        () = tokio::time::sleep(Duration::from_secs(5)) => panic!("retirement request absent"),
    }
    // Poll the operation while the peer explicitly holds its retirement result.
    assert!(timeout(
        intentd_test_support::test_timeout(Duration::from_millis(30)),
        &mut operation
    )
    .await
    .is_err());
    assert!(services
        .store
        .get_agent_session(&agent)
        .await
        .unwrap()
        .acp_session_id
        .is_none());
    assert!(services
        .store
        .get_review_context_enrollment(&workspace, &agent, "late-original")
        .await
        .unwrap()
        .is_none());
    if drop_waiter {
        drop(operation);
        let shared = handshake
            .callbacks
            .as_ref()
            .unwrap()
            .begin_review_context_retirement("late-original", &context)
            .unwrap();
        let mut waiting = Box::pin(shared.join());
        assert!(timeout(
            intentd_test_support::test_timeout(Duration::from_millis(30)),
            &mut waiting
        )
        .await
        .is_err());
        retire_release.send(()).unwrap();
        assert_eq!(
            timeout(
                intentd_test_support::test_timeout(Duration::from_secs(5)),
                waiting
            )
            .await
            .unwrap()
            .is_ok(),
            joined
        );
        assert!(services
            .store
            .get_agent_session(&agent)
            .await
            .unwrap()
            .acp_session_id
            .is_none());
        server.abort();
        let _ = server.await;
        return;
    }
    retire_release.send(()).unwrap();
    let Err(error) = timeout(
        intentd_test_support::test_timeout(Duration::from_secs(5)),
        &mut operation,
    )
    .await
    .unwrap() else {
        panic!("retired original startup was accepted");
    };
    if joined {
        assert!(matches!(error, intent_core::Error::Forbidden(_)));
    } else {
        assert!(
            matches!(error, intent_core::Error::Internal(ref message) if message == "Private native retirement unconfirmed")
        );
    }
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn services_retired_new_joins_exact_late_native_allocation_before_return() {
    retired_new_waits_for_remote_join(true, false, false, false).await;
}

#[tokio::test]
async fn services_retired_new_does_not_report_unconfirmed_native_join_as_success() {
    retired_new_waits_for_remote_join(false, false, false, false).await;
}

#[tokio::test]
async fn services_retired_new_cleanup_join_survives_cancelled_response_waiter() {
    retired_new_waits_for_remote_join(true, true, false, false).await;
    retired_new_waits_for_remote_join(false, true, false, false).await;
}

#[tokio::test]
async fn services_retired_new_caller_drop_before_response_retains_original_producer() {
    retired_new_waits_for_remote_join(true, false, true, false).await;
    retired_new_waits_for_remote_join(false, false, true, false).await;
}

#[tokio::test]
async fn services_retired_new_caller_drop_after_commit_keeps_persistence_and_joins_native() {
    retired_new_waits_for_remote_join(true, false, false, true).await;
    retired_new_waits_for_remote_join(false, false, false, true).await;
}

#[tokio::test]
async fn services_qualified_new_confirmed_owner_drop_retires_original_native_allocation() {
    run_qualified_new(false, true, HeldMutation::None).await;
}

#[tokio::test]
async fn services_store_mutation_joins_original_native_retirement_outside_sql_locks() {
    run_qualified_new(false, false, HeldMutation::Joined).await;
}

#[tokio::test]
async fn services_store_native_retirement_rejection_preserves_commit_and_barrier() {
    run_qualified_new(false, false, HeldMutation::Rejected).await;
}
#[tokio::test]
async fn services_store_native_retirement_settlement_survives_cancelled_waiter() {
    run_qualified_new(false, false, HeldMutation::Cancelled).await;
}

#[tokio::test]
async fn services_store_direct_retirement_waits_for_original_native_join() {
    run_qualified_new(false, false, HeldMutation::Retired).await;
}

#[tokio::test]
async fn services_store_initialization_waits_for_prior_native_join() {
    run_qualified_new(false, false, HeldMutation::Initialized).await;
}

#[tokio::test]
async fn services_store_compatibility_initialization_waits_for_prior_native_join() {
    run_qualified_new(false, false, HeldMutation::Compatible).await;
}

#[tokio::test]
async fn services_store_initialization_rejected_native_join_preserves_commit_and_barrier() {
    run_qualified_new(false, false, HeldMutation::InitializedRejected).await;
}

#[tokio::test]
async fn services_store_initialization_cancelled_waiter_retains_original_settlement() {
    run_qualified_new(false, false, HeldMutation::InitializedCancelled).await;
}

#[tokio::test]
async fn services_store_compatibility_rejected_native_join_preserves_commit_and_barrier() {
    run_qualified_new(false, false, HeldMutation::CompatibleRejected).await;
}

#[tokio::test]
async fn services_store_compatibility_cancelled_waiter_retains_original_settlement() {
    run_qualified_new(false, false, HeldMutation::CompatibleCancelled).await;
}

#[tokio::test]
async fn services_pending_delete_waits_for_original_private_native_context() {
    run_qualified_new(false, false, HeldMutation::PendingDelete).await;
}

#[tokio::test]
async fn services_pending_delete_rejected_native_join_retains_barrier() {
    run_qualified_new(false, false, HeldMutation::PendingDeleteRejected).await;
}

#[tokio::test]
async fn services_pending_delete_cancelled_waiter_retains_completion() {
    run_qualified_new(false, false, HeldMutation::PendingDeleteCancelled).await;
}

#[tokio::test]
async fn services_credential_publication_waits_for_original_native_join() {
    run_qualified_new(false, false, HeldMutation::Credential).await;
}

#[tokio::test]
async fn services_credential_rejected_native_join_blocks_publication() {
    run_qualified_new(false, false, HeldMutation::CredentialRejected).await;
}

#[tokio::test]
async fn services_credential_native_join_survives_cancelled_waiter() {
    run_qualified_new(false, false, HeldMutation::CredentialCancelled).await;
}

#[tokio::test]
async fn services_direct_pat_releases_gate_before_native_completion() {
    run_qualified_new(false, false, HeldMutation::DirectPat).await;
}

#[tokio::test]
async fn services_direct_pat_rejected_native_join_withholds_publication() {
    run_qualified_new(false, false, HeldMutation::DirectPatRejected).await;
}

#[tokio::test]
async fn services_direct_pat_cancelled_caller_retains_native_completion() {
    run_qualified_new(false, false, HeldMutation::DirectPatCancelled).await;
}

#[tokio::test]
async fn services_direct_revoke_releases_gate_before_native_completion() {
    run_qualified_new(false, false, HeldMutation::DirectRevoke).await;
}

#[tokio::test]
async fn services_direct_revoke_rejected_native_join_withholds_publication() {
    run_qualified_new(false, false, HeldMutation::DirectRevokeRejected).await;
}

#[tokio::test]
async fn services_direct_revoke_cancelled_caller_retains_native_completion() {
    run_qualified_new(false, false, HeldMutation::DirectRevokeCancelled).await;
}

#[tokio::test]
async fn services_device_grant_releases_gate_before_native_publication() {
    run_qualified_new(false, false, HeldMutation::DirectDevice).await;
}

#[tokio::test]
async fn services_device_grant_rejected_native_join_withholds_publication() {
    run_qualified_new(false, false, HeldMutation::DirectDeviceRejected).await;
}
#[tokio::test]
async fn services_device_grant_cancelled_waiter_retains_native_completion() {
    run_qualified_new(false, false, HeldMutation::DirectDeviceCancelled).await;
}

#[tokio::test]
async fn services_proactive_refresh_waits_outside_gate_for_native_completion() {
    run_qualified_new(false, false, HeldMutation::ProactiveRefresh).await;
}
#[tokio::test]
async fn services_proof_refresh_waits_outside_gate_for_native_completion() {
    run_qualified_new(false, false, HeldMutation::ProofRefresh).await;
}

#[tokio::test]
async fn services_proactive_refresh_refuses_unconfirmed_native_completion() {
    run_qualified_new(false, false, HeldMutation::ProactiveRefreshRejected).await;
}
#[tokio::test]
async fn services_proactive_refresh_cancelled_caller_retains_native_completion() {
    run_qualified_new(false, false, HeldMutation::ProactiveRefreshCancelled).await;
}
#[tokio::test]
async fn services_proof_refresh_refuses_unconfirmed_native_completion() {
    run_qualified_new(false, false, HeldMutation::ProofRefreshRejected).await;
}
#[tokio::test]
async fn services_proof_refresh_cancelled_caller_retains_native_completion() {
    run_qualified_new(false, false, HeldMutation::ProofRefreshCancelled).await;
}

#[tokio::test]
async fn services_rejected_access_refresh_waits_outside_gate_for_native_completion() {
    run_qualified_new(false, false, HeldMutation::RejectedAccessRefresh).await;
}

#[tokio::test]
async fn services_rejected_access_refresh_refuses_unconfirmed_native_completion() {
    run_qualified_new(false, false, HeldMutation::RejectedAccessRefreshRejected).await;
}

#[tokio::test]
async fn services_rejected_access_refresh_cancelled_caller_retains_completion() {
    run_qualified_new(false, false, HeldMutation::RejectedAccessRefreshCancelled).await;
}

#[tokio::test]
async fn services_rejected_access_expiry_event_waits_for_native_completion() {
    run_qualified_new(false, false, HeldMutation::RejectedAccessExpired).await;
}

#[tokio::test]
async fn services_rejected_access_expiry_refuses_unconfirmed_native_completion() {
    run_qualified_new(false, false, HeldMutation::RejectedAccessExpiredRejected).await;
}

#[tokio::test]
async fn services_rejected_access_expiry_cancelled_caller_retains_completion() {
    run_qualified_new(false, false, HeldMutation::RejectedAccessExpiredCancelled).await;
}

#[tokio::test]
async fn services_proactive_expiry_event_waits_for_native_completion() {
    run_qualified_new(false, false, HeldMutation::ProactiveExpired).await;
}

#[tokio::test]
async fn services_proactive_expiry_refuses_unconfirmed_native_completion() {
    run_qualified_new(false, false, HeldMutation::ProactiveExpiredRejected).await;
}

#[tokio::test]
async fn services_proactive_expiry_cancelled_caller_retains_completion() {
    run_qualified_new(false, false, HeldMutation::ProactiveExpiredCancelled).await;
}

#[tokio::test]
async fn services_settings_update_waits_outside_gates_for_native_completion() {
    run_qualified_new(false, false, HeldMutation::DirectSettings).await;
}

#[tokio::test]
async fn services_settings_update_refuses_unconfirmed_native_completion() {
    run_qualified_new(false, false, HeldMutation::DirectSettingsRejected).await;
}

#[tokio::test]
async fn services_settings_update_cancelled_caller_retains_completion() {
    run_qualified_new(false, false, HeldMutation::DirectSettingsCancelled).await;
}

#[tokio::test]
async fn services_settings_secret_waits_outside_gates_for_native_completion() {
    run_qualified_new(false, false, HeldMutation::SettingsSecret).await;
}

#[tokio::test]
async fn services_settings_secret_refuses_unconfirmed_native_completion() {
    run_qualified_new(false, false, HeldMutation::SettingsSecretRejected).await;
}

#[tokio::test]
async fn services_settings_secret_cancelled_caller_retains_completion() {
    run_qualified_new(false, false, HeldMutation::SettingsSecretCancelled).await;
}

#[tokio::test]
async fn services_settings_reset_waits_outside_gates_for_native_completion() {
    run_qualified_new(false, false, HeldMutation::SettingsReset).await;
}

#[tokio::test]
async fn services_settings_reset_refuses_unconfirmed_native_completion() {
    run_qualified_new(false, false, HeldMutation::SettingsResetRejected).await;
}

#[tokio::test]
async fn services_settings_reset_cancelled_caller_retains_completion() {
    run_qualified_new(false, false, HeldMutation::SettingsResetCancelled).await;
}

#[tokio::test]
async fn services_settings_native_completion_preserves_concurrent_revision_order() {
    run_qualified_new(false, false, HeldMutation::SettingsConcurrent).await;
}

#[tokio::test]
async fn services_settings_reload_waits_outside_gates_for_native_completion() {
    run_qualified_new(false, false, HeldMutation::SettingsReload).await;
}

#[tokio::test]
async fn services_settings_reload_native_refusal_preserves_published_file() {
    run_qualified_new(false, false, HeldMutation::SettingsReloadRejected).await;
}

#[tokio::test]
async fn services_settings_reload_cancelled_caller_retains_completion() {
    run_qualified_new(false, false, HeldMutation::SettingsReloadCancelled).await;
}

#[tokio::test]
async fn services_settings_reload_finite_drain_waits_for_native_completion() {
    run_qualified_new(false, false, HeldMutation::SettingsReloadDrain).await;
}

#[tokio::test]
async fn services_reconcile_stored_account_waits_for_original_native_completion() {
    run_qualified_new(false, false, HeldMutation::ReconcileStored).await;
}

#[tokio::test]
async fn services_reconcile_refresh_waits_for_original_native_completion() {
    run_qualified_new(false, false, HeldMutation::ReconcileRefresh).await;
}

#[tokio::test]
async fn services_reconcile_stored_refuses_unconfirmed_retirement() {
    run_qualified_new(false, false, HeldMutation::ReconcileStoredRejected).await;
}

#[tokio::test]
async fn services_reconcile_stored_cancelled_caller_retains_completion() {
    run_qualified_new(false, false, HeldMutation::ReconcileStoredCancelled).await;
}

#[tokio::test]
async fn services_reconcile_stored_rejects_changed_source_after_native_join() {
    run_qualified_new(false, false, HeldMutation::ReconcileStoredChanged).await;
}

#[tokio::test]
async fn services_reconcile_refresh_refuses_unconfirmed_retirement() {
    run_qualified_new(false, false, HeldMutation::ReconcileRefreshRejected).await;
}

#[tokio::test]
async fn services_reconcile_refresh_cancelled_caller_retains_completion() {
    run_qualified_new(false, false, HeldMutation::ReconcileRefreshCancelled).await;
}

#[tokio::test]
async fn services_reconcile_refresh_rejects_changed_source_after_native_join() {
    run_qualified_new(false, false, HeldMutation::ReconcileRefreshChanged).await;
}

#[tokio::test]
async fn services_settings_native_completion_projects_overlapping_current_values() {
    run_qualified_new(false, false, HeldMutation::SettingsOverlap).await;
}

#[tokio::test]
async fn services_c2_direct_pat_rejects_source_substitution_during_native_join() {
    run_qualified_new(false, false, HeldMutation::DirectPatChanged).await;
}

#[tokio::test]
async fn services_c2_direct_device_rejects_source_substitution_during_native_join() {
    run_qualified_new(false, false, HeldMutation::DirectDeviceChanged).await;
}

#[tokio::test]
async fn services_c2_direct_revoke_rejects_source_substitution_during_native_join() {
    run_qualified_new(false, false, HeldMutation::DirectRevokeChanged).await;
}

#[tokio::test]
async fn services_c2_settings_secret_rejects_source_substitution_during_native_join() {
    run_qualified_new(false, false, HeldMutation::SettingsSecretChanged).await;
}

#[tokio::test]
async fn services_c2_settings_reset_rejects_source_substitution_during_native_join() {
    run_qualified_new(false, false, HeldMutation::SettingsResetChanged).await;
}

#[tokio::test]
async fn services_c2_proactive_expired_rejects_source_substitution_during_native_join() {
    run_qualified_new(false, false, HeldMutation::ProactiveExpiredChanged).await;
}

#[tokio::test]
async fn services_c2_rejected_access_expired_rejects_source_substitution_during_native_join() {
    run_qualified_new(false, false, HeldMutation::RejectedAccessExpiredChanged).await;
}

#[intent_test_macros::daemon_test]
async fn services_c1_completion_backing_error_withholds_success() {
    run_qualified_new(false, false, HeldMutation::SettingsReadError).await;
}
#[intent_test_macros::daemon_test]
async fn services_c1_completion_backing_timeout_withholds_success() {
    run_qualified_new(false, false, HeldMutation::SettingsReadTimeout).await;
}
