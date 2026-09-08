use super::*;

fn operation(phase: Phase) -> Operation {
    Operation {
        shared: Arc::new(Mutex::new(Shared {
            status: Status {
                operation_id: Some("owner-operation".into()),
                supported: true,
                cli_detected: true,
                runtime_installed: false,
                phase,
            },
            binary: None,
        })),
        cancel: Cancellation::default(),
    }
}

#[tokio::test]
async fn disconnect_cancels_the_operation_and_cancellation_cannot_be_overwritten() {
    let op = operation(Phase::Checking);
    let signal = op.cancel.clone();
    op.cancel();
    finish(
        &op.shared,
        &op.cancel,
        Ok(Phase::Connected { model_count: 1 }),
    );
    assert_eq!(op.status().phase, Phase::Cancelled);
    drop(op);
    tokio::time::timeout(std::time::Duration::from_millis(50), signal.cancelled())
        .await
        .unwrap();
}

#[test]
fn repeat_connect_joins_only_pending_work_and_login_needs_explicit_auth_required_state() {
    for phase in [Phase::Checking, Phase::SignInRequired, Phase::SigningIn] {
        assert!(operation(phase).reusable());
    }
    for phase in [
        Phase::Idle,
        Phase::Cancelled,
        Phase::Connected { model_count: 1 },
    ] {
        let op = operation(phase);
        assert!(!op.reusable());
        assert!(!op.login(|_| async { panic!("login must not start") }));
    }
}

#[test]
fn installer_timeout_replaces_progress_with_a_retryable_failure() {
    let op = operation(Phase::Downloading {
        received: 10,
        total: 100,
    });
    finish(
        &op.shared,
        &op.cancel,
        Err(Failure::Install(InstallError::TimedOut)),
    );
    assert_eq!(
        op.status().phase,
        Phase::Failed {
            code: Failure::Install(InstallError::TimedOut)
        }
    );
    assert!(!op.reusable(), "retry must start a fresh operation");
}

#[test]
fn status_contains_only_safe_fields_and_camel_case_wire_states() {
    let op = operation(Phase::Failed {
        code: Failure::Install(InstallError::DownloadFailed),
    });
    assert_eq!(
        serde_json::to_value(op.status()).unwrap(),
        serde_json::json!({
            "operationId":"owner-operation", "supported":true, "cliDetected":true,
            "runtimeInstalled":false, "phase":"failed", "code":"downloadFailed"
        })
    );
    assert_eq!(
        serde_json::to_value(Phase::Connected { model_count: 3 }).unwrap(),
        serde_json::json!({"phase":"connected","modelCount":3})
    );
}

#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
#[tokio::test]
async fn deleted_runtime_at_custom_path_is_a_repair_error_not_an_install_request() {
    use std::os::unix::fs::PermissionsExt;
    let home = tempfile::tempdir().unwrap();
    let wrapper = home.path().join("antigravity-acp");
    let script = format!(
        "#!/bin/sh\nexec {} \"$@\"\n",
        home.path().join(runtime::SERVER).display()
    );
    std::fs::write(&wrapper, &script).unwrap();
    std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o755)).unwrap();
    assert!(!status(wrapper.to_str()).runtime_installed);
    let op = operation(Phase::Checking);
    assert_eq!(
        connect(
            &op.shared,
            &op.cancel,
            home.path().to_owned(),
            wrapper.to_str().map(str::to_owned)
        )
        .await,
        Err(Failure::Setup(SetupError::InvalidCustomPath))
    );
    assert!(!runtime::install_root(home.path()).exists());
    assert_eq!(std::fs::read_to_string(wrapper).unwrap(), script);
}
