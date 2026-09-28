//! The wrapper only schedules around the actual policy; it never grants access.
use super::*;
use crate::repository_read_source::tests::{run, ActualRead, ReadServer};
use intent_acp::mcp_server::private_results::McpPrivateBoundaryKind as Boundary;
use intent_acp::mcp_server::request_context::{McpRequestContext, McpRequestScope};
use intent_acp::mcp_server::WorkspaceMcpServer;
use std::time::Duration;
use tokio::sync::{Notify, Semaphore};

struct Hold {
    entered: Notify,
    release: Semaphore,
}
impl Hold {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            entered: Notify::new(),
            release: Semaphore::new(0),
        })
    }
    async fn wait(&self) {
        self.entered.notify_one();
        self.release.acquire().await.unwrap().forget();
    }
    async fn reached(&self) {
        tokio::time::timeout(Duration::from_secs(10), self.entered.notified())
            .await
            .unwrap();
    }
}

#[derive(Default)]
struct Control {
    hold: Mutex<Option<(Boundary, bool, Arc<Hold>)>>,
    events: Mutex<Vec<(Boundary, usize)>>,
    records: Mutex<Vec<Arc<ReadRecord>>>,
    foreign: Mutex<Vec<Arc<ReadRecord>>>,
}
struct ControlledContext {
    original: Arc<dyn McpRequestContext>,
    control: Arc<Control>,
}
struct ControlledScope {
    original: Arc<dyn McpRequestScope>,
    control: Arc<Control>,
}
struct ControlledPolicy {
    original: Arc<dyn McpPrivatePolicy>,
    control: Arc<Control>,
}
impl McpRequestContext for ControlledContext {
    fn capture(&self) -> Arc<dyn McpRequestScope> {
        Arc::new(ControlledScope {
            original: self.original.capture(),
            control: self.control.clone(),
        })
    }
}
impl McpRequestScope for ControlledScope {
    fn scope<'a>(&'a self, body: McpContextFuture<'a>) -> McpContextFuture<'a> {
        self.original.scope(body)
    }
    fn private_result_policy(&self) -> Option<Arc<dyn McpPrivatePolicy>> {
        self.original.private_result_policy().map(|original| {
            Arc::new(ControlledPolicy {
                original,
                control: self.control.clone(),
            }) as Arc<dyn McpPrivatePolicy>
        })
    }
}
impl McpPrivatePolicy for ControlledPolicy {
    fn capture_host(&self, call: McpHostCall) -> Box<dyn McpPrivateHostScope> {
        self.original.capture_host(call)
    }
    fn admit<'a>(
        &'a self,
        boundary: &'a McpPrivateBoundary,
        records: &'a [McpReadEvidence],
        packet: PreparedMcpTransfer<'a>,
    ) -> intent_js::BoxFuture<'a, McpPrivateAdmission> {
        Box::pin(async move {
            self.control
                .events
                .lock()
                .unwrap()
                .push((boundary.kind(), records.len()));
            let retained =
                crate::repository_read_source::tests::retain_and_check_records(records).await;
            let foreign = self.control.foreign.lock().unwrap().clone();
            if !foreign.is_empty() {
                crate::repository_read_source::tests::refuse_foreign_record_set(
                    &retained, &foreign,
                )
                .await;
            }
            *self.control.records.lock().unwrap() = retained;
            let hold = {
                let mut held = self.control.hold.lock().unwrap();
                if held
                    .as_ref()
                    .is_some_and(|(kind, _, _)| *kind == boundary.kind())
                {
                    held.take()
                } else {
                    None
                }
            };
            if let Some((_, false, gate)) = &hold {
                gate.wait().await;
            }
            let admitted = self.original.admit(boundary, records, packet).await;
            if let Some((_, true, gate)) = &hold {
                gate.wait().await;
            }
            admitted
        })
    }
}

fn controlled(f: &ActualRead, control: Arc<Control>) -> WorkspaceMcpServer {
    WorkspaceMcpServer::new(f.api(), f.git.workspace.id.clone())
        .with_caller_agent_id(Some(f.agent.clone()))
        .with_request_context(Arc::new(ControlledContext {
            original: Arc::new(f.context()),
            control,
        }))
}

#[intent_test_macros::daemon_test]
async fn real_host_transfer_and_later_output_each_require_original_live_authority() {
    for (boundary, after) in [
        (Boundary::HostPromise, false),
        (Boundary::HostPromise, true),
        (Boundary::DirectResponse, false),
        (Boundary::DirectResponse, true),
    ] {
        let http = ReadServer::new().await;
        let f = ActualRead::new(&http).await;
        let control = Arc::new(Control::default());
        let gate = Hold::new();
        *control.hold.lock().unwrap() = Some((boundary, after, gate.clone()));
        let endpoint = controlled(&f, control.clone());
        let task =
            tokio::spawn(async move { run(&endpoint, "return await ws.pr.snapshot(4);").await });
        gate.reached().await;
        f.owner.interrupt_requests();
        gate.release.add_permits(1);
        let reply = task.await.unwrap();
        assert_eq!(
            reply.to_string().contains("actual review"),
            boundary == Boundary::DirectResponse && after,
            "{reply}"
        );
        let events = control.events.lock().unwrap();
        assert_eq!(events.first(), Some(&(Boundary::HostPromise, 2)));
        assert!(events.iter().all(|(_, count)| *count == 2));
    }
}

#[intent_test_macros::daemon_test]
async fn caught_discarded_and_transformed_results_keep_all_original_records() {
    for code in [
        "await ws.pr.snapshot(4); return 'constant';",
        "const p=await ws.pr.snapshot(4); return p.title.length;",
        "try { await ws.pr.snapshot(4); throw Error('caught'); } catch(e) {} return 'constant';",
    ] {
        let http = ReadServer::new().await;
        let f = ActualRead::new(&http).await;
        let control = Arc::new(Control::default());
        let gate = Hold::new();
        *control.hold.lock().unwrap() = Some((Boundary::DirectResponse, false, gate.clone()));
        let endpoint = controlled(&f, control.clone());
        let task = tokio::spawn(async move { run(&endpoint, code).await });
        gate.reached().await;
        f.owner.interrupt_requests();
        gate.release.add_permits(1);
        let reply = task.await.unwrap();
        assert!(
            reply
                .to_string()
                .contains("Private result delivery refused"),
            "{reply}"
        );
        assert!(control
            .events
            .lock()
            .unwrap()
            .contains(&(Boundary::DirectResponse, 2)));
    }
}

#[intent_test_macros::daemon_test]
async fn actual_tcp_response_runs_the_same_real_aggregate_after_host_transfer() {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    let http = ReadServer::new().await;
    let f = ActualRead::new(&http).await;
    let control = Arc::new(Control::default());
    let gate = Hold::new();
    *control.hold.lock().unwrap() = Some((Boundary::TcpResponse, false, gate.clone()));
    let bridge =
        intent_acp::mcp_bridge::serve_workspace_mcp_tcp(Arc::new(controlled(&f, control.clone())))
            .await
            .unwrap();
    let mut socket = tokio::net::TcpStream::connect(bridge.addr()).await.unwrap();
    let request = crate::repository_read_source::tests::call("return await ws.pr.snapshot(4);");
    socket
        .write_all(format!("{request}\n").as_bytes())
        .await
        .unwrap();
    gate.reached().await;
    f.owner.interrupt_requests();
    gate.release.add_permits(1);
    let mut reply = String::new();
    tokio::time::timeout(
        Duration::from_secs(10),
        BufReader::new(socket).read_line(&mut reply),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(reply.contains("Private result delivery refused"), "{reply}");
    assert!(!reply.contains("actual review"));
    assert!(control
        .events
        .lock()
        .unwrap()
        .contains(&(Boundary::TcpResponse, 2)));
    drop(bridge);
}

#[intent_test_macros::daemon_test]
async fn artifact_start_admits_one_original_job_before_io_and_response_revalidates_again() {
    for after in [false, true] {
        let http = ReadServer::new().await;
        let f = ActualRead::new(&http).await;
        f.auth
            .registry
            .apply(&[(
                "workspaceApi.maxOutputChars".into(),
                serde_json::json!(1000),
            )])
            .unwrap();
        let control = Arc::new(Control::default());
        let gate = Hold::new();
        *control.hold.lock().unwrap() = Some((Boundary::ArtifactStart, after, gate.clone()));
        let endpoint = controlled(&f, control.clone());
        let task = tokio::spawn(async move {
            run(
                &endpoint,
                "const p=await ws.pr.snapshot(4); return p.title.repeat(300);",
            )
            .await
        });
        gate.reached().await;
        let folder = f.git.dir.path().join("tool-outputs");
        assert!(!folder.exists(), "no private I/O inside admission");
        tokio::time::timeout(
            Duration::from_secs(2),
            f.auth
                .service
                .worktree_locks
                .with_lock(&f.git.path, || async {}),
        )
        .await
        .unwrap();
        f.owner.interrupt_requests();
        gate.release.add_permits(1);
        let reply = task.await.unwrap();
        assert!(
            reply
                .to_string()
                .contains("Private result delivery refused"),
            "{reply}"
        );
        assert_eq!(folder.exists(), after);
        if after {
            let files = std::fs::read_dir(&folder)
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert_eq!(files.len(), 1);
            assert!(std::fs::read_to_string(files[0].path())
                .unwrap()
                .contains("actual review"));
        }
        assert!(control
            .events
            .lock()
            .unwrap()
            .contains(&(Boundary::ArtifactStart, 2)));
    }
}

#[intent_test_macros::daemon_test]
async fn original_ledger_counts_cache_hits_and_lazy_read_at_sixty_four_and_sixty_five() {
    for count in [63, 64] {
        let http = ReadServer::new().await;
        let f = ActualRead::new(&http).await;
        let control = Arc::new(Control::default());
        let code = format!(
            "for (let i=0;i<{count};i++) {{ await ws.pr.snapshot(4); }} return 'bounded success';"
        );
        let reply = run(&controlled(&f, control.clone()), &code).await;
        assert_eq!(
            reply.to_string().contains("bounded success"),
            count == 63,
            "{reply}"
        );
        let events = control.events.lock().unwrap();
        if count == 63 {
            assert!(
                events.contains(&(Boundary::DirectResponse, 64)),
                "{events:?}"
            );
        } else {
            assert!(
                reply
                    .to_string()
                    .contains("Private result delivery refused"),
                "{reply}"
            );
        }
    }
}

#[intent_test_macros::daemon_test]
async fn source_changes_after_host_disclosure_reject_constant_output_without_rebinding() {
    for change in ["head", "remote", "pending-delete", "physical"] {
        let http = ReadServer::new().await;
        let f = ActualRead::new(&http).await;
        let control = Arc::new(Control::default());
        let gate = Hold::new();
        *control.hold.lock().unwrap() = Some((Boundary::DirectResponse, false, gate.clone()));
        let endpoint = controlled(&f, control);
        let task = tokio::spawn(async move {
            run(&endpoint, "await ws.pr.snapshot(4); return 'constant';").await
        });
        gate.reached().await;
        let mut pending = None;
        match change {
            "head" => {
                f.git.git(
                    &f.git.path,
                    &[
                        "-c",
                        "user.name=Fixture",
                        "-c",
                        "user.email=fixture@example.invalid",
                        "commit",
                        "--allow-empty",
                        "-m",
                        "new head",
                    ],
                );
            }
            "remote" => {
                f.git.git(
                    &f.git.path,
                    &[
                        "remote",
                        "set-url",
                        "origin",
                        "https://gitlab.test/forge/other/changed.git",
                    ],
                );
            }
            "pending-delete" => {
                pending = Some(
                    f.auth
                        .service
                        .store
                        .begin_repository_pending_delete(&[
                            intent_store::RepositoryLifecycleKey::Workspace(
                                f.git.workspace.id.clone(),
                            ),
                        ])
                        .await
                        .unwrap(),
                );
            }
            _ => f.owner.retirement().retire(),
        }
        if let Some(pending) = pending {
            pending.settle_confirmed();
        }
        gate.release.add_permits(1);
        let reply = task.await.unwrap();
        assert!(
            reply
                .to_string()
                .contains("Private result delivery refused"),
            "{change}: {reply}"
        );
    }
}

#[intent_test_macros::daemon_test]
async fn original_public_scheduling_cancellation_retires_old_output_but_allows_fresh_capture() {
    use intent_core::{Caller, WorkspaceApi};
    for agent in [false, true] {
        let http = ReadServer::new().await;
        let f = ActualRead::new(&http).await;
        let control = Arc::new(Control::default());
        let gate = Hold::new();
        *control.hold.lock().unwrap() = Some((Boundary::DirectResponse, false, gate.clone()));
        let endpoint = controlled(&f, control);
        let task = tokio::spawn(async move {
            run(&endpoint, "await ws.pr.snapshot(4); return 'constant';").await
        });
        gate.reached().await;
        intent_core::with_caller(Caller::Daemon, async {
            if agent {
                f.auth
                    .service
                    .agent_schedule_delete(
                        f.agent.clone(),
                        Some(f.git.workspace.id.clone()),
                        60_000,
                    )
                    .await
                    .unwrap();
                assert!(f
                    .auth
                    .service
                    .agent_cancel_delete(f.agent.clone(), Some(f.git.workspace.id.clone()))
                    .await
                    .unwrap());
            } else {
                f.auth
                    .service
                    .schedule_workspace_delete(f.git.workspace.id.clone(), 60_000)
                    .await
                    .unwrap();
                assert!(f
                    .auth
                    .service
                    .cancel_workspace_delete(f.git.workspace.id.clone())
                    .await
                    .unwrap());
            }
        })
        .await;
        gate.release.add_permits(1);
        assert!(task
            .await
            .unwrap()
            .to_string()
            .contains("Private result delivery refused"));
        let fresh = run(&f.server(), "return await ws.pr.snapshot(4);").await;
        assert!(fresh.to_string().contains("actual review"), "{fresh}");
    }
}

#[intent_test_macros::daemon_test]
async fn original_claimed_workspace_timer_cannot_be_canceled_or_deliver_old_read() {
    use crate::delete_grace::PendingDeleteSubject;
    use intent_core::{Caller, WorkspaceApi};
    let http = ReadServer::new().await;
    let f = ActualRead::new(&http).await;
    let control = Arc::new(Control::default());
    let gate = Hold::new();
    *control.hold.lock().unwrap() = Some((Boundary::DirectResponse, false, gate.clone()));
    let endpoint = controlled(&f, control);
    let task =
        tokio::spawn(
            async move { run(&endpoint, "await ws.pr.snapshot(4); return 'constant';").await },
        );
    gate.reached().await;
    intent_core::with_caller(Caller::Daemon, async {
        f.auth
            .service
            .schedule_workspace_delete(f.git.workspace.id.clone(), 0)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            while f
                .auth
                .service
                .pending_workspace_deletes
                .deadline(&PendingDeleteSubject::Workspace(f.git.workspace.id.clone()))
                .unwrap()
                .is_some()
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(!f
            .auth
            .service
            .cancel_workspace_delete(f.git.workspace.id.clone())
            .await
            .unwrap());
    })
    .await;
    gate.release.add_permits(1);
    assert!(task
        .await
        .unwrap()
        .to_string()
        .contains("Private result delivery refused"));
    tokio::time::timeout(Duration::from_secs(10), async {
        while f
            .auth
            .service
            .store
            .get_workspace(&f.git.workspace.id)
            .await
            .is_ok()
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[intent_test_macros::daemon_test]
async fn original_settings_and_secret_replacement_cannot_repair_final_output() {
    use intent_core::WorkspaceApi;
    use intent_sourcecontrol::gitlab_token::SECRET_ACCOUNT;
    for secret in [false, true] {
        let http = ReadServer::new().await;
        let f = ActualRead::new(&http).await;
        let control = Arc::new(Control::default());
        let gate = Hold::new();
        *control.hold.lock().unwrap() = Some((Boundary::DirectResponse, false, gate.clone()));
        let endpoint = controlled(&f, control);
        let task = tokio::spawn(async move {
            run(&endpoint, "await ws.pr.snapshot(4); return 'constant';").await
        });
        gate.reached().await;
        let path = if secret {
            SECRET_ACCOUNT
        } else {
            "sourceControl.gitlab.oauthClientId"
        };
        intent_core::with_caller(intent_core::Caller::Daemon,
            f.auth.service.settings_update(serde_json::json!([{"path":path,"value":if secret {"pat-second"} else {"replacement"}}]))
        ).await.unwrap();
        gate.release.add_permits(1);
        assert!(task
            .await
            .unwrap()
            .to_string()
            .contains("Private result delivery refused"));
    }
}

#[intent_test_macros::daemon_test]
async fn mixed_live_original_requests_are_refused_without_releasing_shared_authority_checks() {
    let http = ReadServer::new().await;
    let f = ActualRead::new(&http).await;
    let first = Arc::new(Control::default());
    let gate = Hold::new();
    *first.hold.lock().unwrap() = Some((Boundary::DirectResponse, false, gate.clone()));
    let endpoint = controlled(&f, first.clone());
    let task = tokio::spawn(async move { run(&endpoint, "return await ws.pr.snapshot(4);").await });
    gate.reached().await;
    let second = Arc::new(Control::default());
    *second.foreign.lock().unwrap() = first.records.lock().unwrap().clone();
    let reply = run(&controlled(&f, second), "return await ws.pr.snapshot(4);").await;
    assert!(reply.to_string().contains("actual review"), "{reply}");
    gate.release.add_permits(1);
    assert!(task.await.unwrap().to_string().contains("actual review"));
}

#[intent_test_macros::daemon_test]
async fn original_artifact_cancellation_and_failed_io_do_not_become_replacement_transfers() {
    for mode in ["before", "after", "failed-io"] {
        let http = ReadServer::new().await;
        let f = ActualRead::new(&http).await;
        f.auth
            .registry
            .apply(&[(
                "workspaceApi.maxOutputChars".into(),
                serde_json::json!(1000),
            )])
            .unwrap();
        let control = Arc::new(Control::default());
        let gate = Hold::new();
        *control.hold.lock().unwrap() =
            Some((Boundary::ArtifactStart, mode != "before", gate.clone()));
        let folder = f.git.dir.path().join("tool-outputs");
        let endpoint = controlled(&f, control);
        let task = tokio::spawn(async move {
            run(
                &endpoint,
                "const p=await ws.pr.snapshot(4); return p.title.repeat(300);",
            )
            .await
        });
        gate.reached().await;
        if mode == "failed-io" {
            std::fs::write(&folder, "original obstruction").unwrap();
            gate.release.add_permits(1);
            let reply = task.await.unwrap();
            assert_eq!(
                std::fs::read_to_string(&folder).unwrap(),
                "original obstruction"
            );
            assert!(
                !reply.to_string().contains("The full output was written"),
                "{reply}"
            );
        } else {
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
            gate.release.add_permits(1);
            assert!(!folder.exists());
        }
        tokio::time::timeout(
            Duration::from_secs(2),
            f.auth
                .service
                .worktree_locks
                .with_lock(&f.git.path, || async {}),
        )
        .await
        .unwrap();
    }
}

#[intent_test_macros::daemon_test]
async fn real_attachment_admission_keeps_already_admitted_effect_distinct_from_response() {
    for after in [false, true] {
        let http = ReadServer::new().await;
        let f = ActualRead::new(&http).await;
        let registry = Arc::new(intent_core::TurnAttachmentRegistry::new());
        let control = Arc::new(Control::default());
        let gate = Hold::new();
        *control.hold.lock().unwrap() = Some((Boundary::Attachments, after, gate.clone()));
        let endpoint = controlled(&f, control).with_turn_attachments(Some(registry.clone()));
        let task = tokio::spawn(async move {
            run(&endpoint,"const p=await ws.pr.snapshot(4); return {__mcpContentItems:[{type:'resource',resource:{uri:'fixture://original',mimeType:'application/json',text:JSON.stringify({title:p.title})}}]};").await
        });
        gate.reached().await;
        assert_eq!(
            registry.pending_count_by_mime(&f.agent, "application/json"),
            0
        );
        f.owner.interrupt_requests();
        gate.release.add_permits(1);
        assert!(task
            .await
            .unwrap()
            .to_string()
            .contains("Private result delivery refused"));
        assert_eq!(
            registry.pending_count_by_mime(&f.agent, "application/json"),
            usize::from(after)
        );
    }
}

/// Scheduling/restoration around the actual service future; no admission path.
struct DeliveryControlApi {
    inner: Arc<dyn intent_core::WorkspaceApi>,
    settings: Mutex<Option<Arc<Hold>>>,
    restore: Mutex<Option<(std::path::PathBuf, std::path::PathBuf)>>,
}
impl intent_core::WorkspaceApi for DeliveryControlApi {
    fn agent_is_retired(&self, id: intent_core::AgentId) -> intent_js::BoxFuture<'_, bool> {
        self.inner.agent_is_retired(id)
    }
    fn get_workspace(
        &self,
        id: intent_core::WorkspaceId,
    ) -> intent_js::BoxFuture<'_, intent_core::Result<intent_core::Workspace>> {
        self.inner.get_workspace(id)
    }
    fn settings_get(
        &self,
        path: String,
    ) -> intent_js::BoxFuture<'_, intent_core::Result<serde_json::Value>> {
        Box::pin(async move {
            let gate = self.settings.lock().unwrap().take();
            if let Some(gate) = gate {
                gate.wait().await;
            }
            self.inner.settings_get(path).await
        })
    }
    fn pr_state(
        &self,
        id: intent_core::WorkspaceId,
        number: u64,
        repo: Option<String>,
    ) -> intent_js::BoxFuture<'_, intent_core::Result<serde_json::Value>> {
        // Delegate synchronously so the real producer still captures before await.
        let read = self.inner.pr_state(id, number, repo);
        Box::pin(async move {
            let result = read.await;
            if result.is_err() {
                let restore = self.restore.lock().unwrap().take();
                if let Some((from, to)) = restore {
                    std::fs::rename(from, to).unwrap();
                }
            }
            result
        })
    }
}

#[intent_test_macros::daemon_test]
async fn post_eval_settings_wait_preserves_retirement_and_cancellation_before_any_spill() {
    for cancel in [false, true] {
        let http = ReadServer::new().await;
        let f = ActualRead::new(&http).await;
        f.auth
            .registry
            .apply(&[(
                "workspaceApi.maxOutputChars".into(),
                serde_json::json!(1000),
            )])
            .unwrap();
        let gate = Hold::new();
        let api = Arc::new(DeliveryControlApi {
            inner: f.api(),
            settings: Mutex::new(Some(gate.clone())),
            restore: Mutex::new(None),
        });
        let endpoint = WorkspaceMcpServer::new(api, f.git.workspace.id.clone())
            .with_caller_agent_id(Some(f.agent.clone()))
            .with_request_context(Arc::new(f.context()));
        let task = tokio::spawn(async move {
            run(
                &endpoint,
                "const p=await ws.pr.snapshot(4); return p.title.repeat(300);",
            )
            .await
        });
        gate.reached().await;
        assert!(http.count() > 0);
        f.owner.interrupt_requests();
        if cancel {
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
        } else {
            gate.release.add_permits(1);
            assert!(task
                .await
                .unwrap()
                .to_string()
                .contains("Private result delivery refused"));
        }
        assert!(!f.git.dir.path().join("tool-outputs").exists());
    }
}

#[intent_test_macros::daemon_test]
async fn unavailable_git_control_does_not_repair_old_source_but_allows_same_request_fresh_call() {
    let http = ReadServer::new().await;
    let f = ActualRead::new(&http).await;
    let original = f.git.path.join(".git");
    let saved = f.git.path.join("saved-git");
    std::fs::rename(&original, &saved).unwrap();
    let api = Arc::new(DeliveryControlApi {
        inner: f.api(),
        settings: Mutex::new(None),
        restore: Mutex::new(Some((saved, original))),
    });
    let control = Arc::new(Control::default());
    let endpoint = WorkspaceMcpServer::new(api, f.git.workspace.id.clone())
        .with_caller_agent_id(Some(f.agent.clone()))
        .with_request_context(Arc::new(ControlledContext {
            original: Arc::new(f.context()),
            control: control.clone(),
        }));
    let reply = run(
        &endpoint,
        "try {await ws.pr.snapshot(4);} catch(e) {} return await ws.pr.snapshot(4);",
    )
    .await;
    assert!(reply.to_string().contains("actual review"), "{reply}");
    assert_eq!(
        *control.events.lock().unwrap(),
        vec![(Boundary::HostPromise, 2), (Boundary::DirectResponse, 2)]
    );
}

#[path = "tests/aliases.rs"]
mod aliases;
