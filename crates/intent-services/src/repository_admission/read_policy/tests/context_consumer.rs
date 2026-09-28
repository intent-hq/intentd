//! Public consumer checks use the actual Services API and original policy.
use crate::repository_context_live::tests::LiveFixture;
use crate::repository_context_output::tests::{controlled, tcp, Control, Hold, FACTS};
use crate::repository_read_source::tests::{run, ReadServer};
use intent_core::WorkspaceApi;
use intent_store::RepositorySelectionChange;
use std::sync::Arc;

#[intent_test_macros::daemon_test]
async fn context_consumer_saved_primary_choice_and_reset_control_actual_native_read() {
    let http = ReadServer::new().await;
    let f = LiveFixture::new(&http).await;
    f.base.git.git(
        &f.base.git.path,
        &[
            "remote",
            "add",
            "second",
            "https://github.com/team/other.git",
        ],
    );
    let ambiguous = run(&f.server(), "return await ws.pr.snapshot(4);").await;
    assert!(!ambiguous.to_string().contains("actual review"));
    assert_eq!(http.count(), 0);
    f.selection(RepositorySelectionChange::ExplicitRemote {
        remote_name: "origin".into(),
    })
    .await;
    let chosen = run(&f.server(), "return await ws.pr.snapshot(4);").await;
    assert!(chosen.to_string().contains("actual review"), "{chosen}");
    let count = http.count();
    f.base
        .git
        .git(&f.base.git.path, &["remote", "remove", "origin"]);
    let unresolved = run(&f.server(), "return await ws.pr.snapshot(4);").await;
    assert!(!unresolved.to_string().contains("actual review"));
    assert_eq!(http.count(), count);
    let (_, context) = f.observe().await;
    assert!(matches!(
        context.value().context.roots[0].review_selection.saved,
        intent_core::SavedReviewSelection::ExplicitRemote { .. }
    ));
    f.base
        .git
        .git(&f.base.git.path, &["remote", "remove", "second"]);
    f.selection(RepositorySelectionChange::Reset).await;
    let (_, no_remotes) = f.observe().await;
    assert!(
        no_remotes.value().context.roots[0]
            .review_selection
            .no_remotes
    );
    let empty = run(&f.server(), "return await ws.pr.snapshot(4);").await;
    assert!(!empty.to_string().contains("actual review"));
    assert_eq!(http.count(), count);
}

#[intent_test_macros::daemon_test]
async fn context_consumer_real_narrow_root_and_choice_mutations_classify_optional_and_required() {
    for mode in [
        "delete-optional",
        "optional-choice",
        "required-choice",
        "optional-git",
    ] {
        let http = ReadServer::new().await;
        let f = LiveFixture::new(&http).await;
        let root = f.root("optional-root").await;
        let control = Arc::new(Control::default());
        let hold = Hold::new();
        *control.hold.lock().unwrap() = Some(hold.clone());
        let server = controlled(&f, control.clone());
        let task = tokio::spawn(async move {
            tcp(
                server,
                "await ws.pr.snapshot(4); return 'completed-private-result';",
            )
            .await
        });
        hold.reached().await;
        assert_eq!(*control.events.lock().unwrap(), vec![2]);
        match mode {
            "delete-optional" => f
                .base
                .auth
                .service
                .store
                .delete_workspace_git_root(&root.id)
                .await
                .unwrap(),
            "optional-choice" => {
                let id = intent_core::RepositoryRootId {
                    workspace_id: f.session.workspace_id.clone(),
                    kind: intent_core::RepositoryRootKind::Registered {
                        git_root_id: root.id.clone(),
                    },
                };
                let snapshot = f
                    .base
                    .auth
                    .service
                    .store
                    .repository_selection_snapshot(&id)
                    .await
                    .unwrap();
                f.base
                    .auth
                    .service
                    .store
                    .write_repository_selection(
                        &snapshot,
                        RepositorySelectionChange::ExplicitRemote {
                            remote_name: "missing".into(),
                        },
                    )
                    .await
                    .result
                    .unwrap();
            }
            "required-choice" => {
                f.selection(RepositorySelectionChange::ExplicitRemote {
                    remote_name: "missing".into(),
                })
                .await;
            }
            "optional-git" => {
                f.base.git.git(
                    std::path::Path::new(&root.path),
                    &[
                        "remote",
                        "set-url",
                        "origin",
                        "https://github.com/team/changed.git",
                    ],
                );
            }
            _ => unreachable!(),
        }
        hold.resume();
        let reply = task.await.unwrap();
        assert!(!reply.to_string().contains(FACTS), "{mode}: {reply}");
        if mode == "required-choice" {
            assert!(
                reply
                    .to_string()
                    .contains("Private result delivery refused"),
                "{reply}"
            );
        } else {
            assert!(
                reply.to_string().contains("completed-private-result"),
                "{mode}: {reply}"
            );
        }
        f.owner.drain_jobs().await;
    }
}

#[intent_test_macros::daemon_test]
async fn context_consumer_required_settings_and_secret_replacement_keep_all_records_required() {
    for path in [
        "sourceControl.gitlab.oauthClientId",
        intent_sourcecontrol::gitlab_token::SECRET_ACCOUNT,
    ] {
        let http = ReadServer::new().await;
        let f = LiveFixture::new(&http).await;
        let control = Arc::new(Control::default());
        let hold = Hold::new();
        *control.hold.lock().unwrap() = Some(hold.clone());
        let server = controlled(&f, control.clone());
        let task = tokio::spawn(async move {
            tcp(
                server,
                "try { await ws.pr.snapshot(4); } catch(e) {} return 'constant';",
            )
            .await
        });
        hold.reached().await;
        intent_core::with_caller(
            intent_core::Caller::Daemon,
            f.base
                .auth
                .service
                .settings_update(serde_json::json!([{"path":path,"value":"replacement"}])),
        )
        .await
        .unwrap();
        hold.resume();
        let value = task.await.unwrap();
        assert!(
            value
                .to_string()
                .contains("Private result delivery refused"),
            "{value}"
        );
        assert!(!value.to_string().contains(FACTS));
        assert_eq!(*control.events.lock().unwrap(), vec![2]);
        f.owner.drain_jobs().await;
    }
}

#[intent_test_macros::daemon_test]
async fn context_consumer_ledger_counts_distinct_calls_at_sixty_four_and_sixty_five() {
    for count in [63, 64] {
        let http = ReadServer::new().await;
        let f = LiveFixture::new(&http).await;
        let control = Arc::new(Control::default());
        let reply=tcp(controlled(&f,control.clone()),&format!("for(let i=0;i<{count};i++) {{ try {{await ws.pr.snapshot(4);}} catch(e) {{}} }} return 'all-calls-kept';")).await;
        if count == 63 {
            assert!(reply.to_string().contains("all-calls-kept"), "{reply}");
            assert!(reply.to_string().contains(FACTS), "{reply}");
            assert_eq!(*control.events.lock().unwrap(), vec![64]);
        } else {
            assert!(
                reply
                    .to_string()
                    .contains("Private result delivery refused"),
                "{reply}"
            );
            assert!(!reply.to_string().contains(FACTS));
        }
        assert!(http.count() > 0);
        f.owner.drain_jobs().await;
    }
}
