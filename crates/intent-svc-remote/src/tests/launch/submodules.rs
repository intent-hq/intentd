//! Submodule pins of a remote agent (nodeProtocol 10, prepared checkout):
//! capture at admission, module `base` refs, the launch's `checkout`,
//! module git sessions confined to the agent's recorded pins, the final
//! stop's module pushes, and the refusal on an older machine.

use std::path::{Path, PathBuf};

use intent_core::node_link::{SubmodulePin, PREPARED_CHECKOUT_PROTOCOL};

use super::*;

const FILE: &str = "protocol.file.allow=always";

fn repo_with_commit(dir: &Path, file: &str) -> String {
    std::fs::create_dir_all(dir).unwrap();
    git(dir, &["init", "--quiet"]);
    std::fs::write(dir.join(file), format!("{file}\n")).unwrap();
    git(dir, &["add", file]);
    git(dir, &["commit", "--quiet", "-m", file]);
    git(dir, &["rev-parse", "HEAD"])
}

/// The superproject's submodules: `a` with nested `a/b`, `c` with
/// `update = none` (populated), `d` unpopulated, a plain repository
/// `nested` that is not a submodule, and `a` one backend-only commit ahead
/// of its gitlink.
struct Modules {
    _sources: tempfile::TempDir,
    a: String,
    b: String,
    c: String,
}

fn add_submodules(repo: &Path) -> Modules {
    let sources = tempfile::tempdir().unwrap();
    let src = |name: &str| sources.path().join(name);
    repo_with_commit(&src("b"), "b.txt");
    repo_with_commit(&src("a"), "a.txt");
    git(
        &src("a"),
        &[
            "-c",
            FILE,
            "submodule",
            "add",
            "--quiet",
            src("b").to_str().unwrap(),
            "b",
        ],
    );
    git(&src("a"), &["commit", "--quiet", "-m", "add b"]);
    repo_with_commit(&src("c"), "c.txt");
    repo_with_commit(&src("d"), "d.txt");
    for name in ["a", "c", "d"] {
        git(
            repo,
            &[
                "-c",
                FILE,
                "submodule",
                "add",
                "--quiet",
                src(name).to_str().unwrap(),
                name,
            ],
        );
    }
    git(
        repo,
        &["config", "-f", ".gitmodules", "submodule.c.update", "none"],
    );
    git(repo, &["add", ".gitmodules"]);
    git(repo, &["commit", "--quiet", "-m", "submodules"]);
    git(
        repo,
        &[
            "-c",
            FILE,
            "submodule",
            "update",
            "--quiet",
            "--init",
            "--recursive",
            "a",
        ],
    );
    git(repo, &["submodule", "deinit", "--quiet", "-f", "d"]);
    std::fs::write(repo.join("a/ahead.txt"), "backend only\n").unwrap();
    git(&repo.join("a"), &["add", "ahead.txt"]);
    git(
        &repo.join("a"),
        &["commit", "--quiet", "-m", "backend only"],
    );
    std::fs::write(repo.join("c/dirty.txt"), "uncommitted\n").unwrap();
    repo_with_commit(&repo.join("nested"), "nested.txt");
    Modules {
        a: git(&repo.join("a"), &["rev-parse", "HEAD"]),
        b: git(&repo.join("a/b"), &["rev-parse", "HEAD"]),
        c: git(&repo.join("c"), &["rev-parse", "HEAD"]),
        _sources: sources,
    }
}

/// The fixture on a protocol 10 link of a backend that offers the prepared
/// checkout.
async fn prepared_fixture() -> (Fixture, NodeEnd, Machine) {
    let (f, link, machine) = fixture().await;
    f.node
        .shared_ref()
        .set_protocol_for_test(PREPARED_CHECKOUT_PROTOCOL);
    f.node.shared_ref().offer_prepared_checkout_for_test();
    (f, link, machine)
}

fn key(dir: &Path) -> String {
    node_git::repo_key(dir).unwrap()
}

fn module_remote(agent: &AgentId, module_key: &str) -> AgentRemote {
    AgentRemote::new(workspace().as_str(), agent.as_str())
        .unwrap()
        .with_module(module_key)
        .unwrap()
}

impl Fixture {
    async fn set_view(&self, agent: &AgentId, view: &RemotePlacementView) {
        self.services
            .store()
            .set_agent_session_metadata_key_json(
                &workspace(),
                agent,
                REMOTE_PLACEMENT_METADATA_KEY,
                &serde_json::to_string(view).unwrap(),
                &intent_core::now_iso(),
            )
            .await
            .unwrap();
    }

    async fn launch_frame(&self, link: &mut NodeEnd, agent: &AgentId) -> LaunchCommand {
        let _runtime = self.port().runtime(&self.services, agent).await.unwrap();
        link.expect(|frame| match frame {
            LinkFrame::Launch(command) => Some(command),
            _ => None,
        })
        .await
    }
}

fn update_required(result: intent_core::Result<impl std::fmt::Debug>) -> String {
    match result {
        Err(Error::MachineUpdateRequired(update)) => {
            assert_eq!(update.update, intent_core::machines::UpdateSide::Machine);
            assert_eq!(update.data()["reason"], "prepared-checkout");
            update.message()
        }
        other => panic!("expected machine-update-required, got {other:?}"),
    }
}

fn is_denied<T: std::fmt::Debug>(result: &intent_core::Result<T>) -> bool {
    matches!(result, Err(Error::Forbidden(m)) if m == "node-git-denied")
}

fn is_unavailable<T: std::fmt::Debug>(result: &intent_core::Result<T>) -> bool {
    matches!(result, Err(Error::Internal(m))
        if m.starts_with(intent_transport::node_link::git_codes::UNAVAILABLE))
}

/// Admission pins every populated submodule at its backend HEAD (parents
/// first, `update = none` included, unpopulated ones skipped); placing the
/// agent writes `base` in each module repository; a nodeProtocol 10 launch
/// carries the pins.
#[tokio::test]
async fn populated_submodules_are_pinned_at_their_backend_heads() {
    let (f, mut link, _machine) = prepared_fixture().await;
    let modules = add_submodules(f.repo.path());
    let repo = f.repo.path();
    let (agent, view) = f.placed().await;

    let expected = vec![
        SubmodulePin {
            path: "a".into(),
            name: "a".into(),
            commit: modules.a.clone(),
            module_key: key(&repo.join("a")),
        },
        SubmodulePin {
            path: "a/b".into(),
            name: "b".into(),
            commit: modules.b.clone(),
            module_key: key(&repo.join("a/b")),
        },
        SubmodulePin {
            path: "c".into(),
            name: "c".into(),
            commit: modules.c.clone(),
            module_key: key(&repo.join("c")),
        },
    ];
    assert_eq!(view.pins(), expected);
    assert_eq!(f.view(&agent).await.pins(), expected);
    let session = f
        .services
        .store()
        .get_agent_session_with_transcript(&agent)
        .await
        .unwrap();
    let mut lite =
        intent_core::model::AgentLite::from_session(session, 0, None, None, None, None, None);
    // Detail reads carry the pins once this build offers the prepared
    // checkout; until then nothing new is projected.
    let shown = intent_core::node_link::offers_prepared_checkout().then(|| {
        json!([
            { "path": "a", "commit": modules.a },
            { "path": "a/b", "commit": modules.b },
            { "path": "c", "commit": modules.c },
        ])
    });
    assert_eq!(
        serde_json::to_value(&lite)
            .unwrap()
            .get("submodules")
            .cloned(),
        shown
    );
    lite.strip_detail_only_fields();
    assert!(serde_json::to_value(&lite)
        .unwrap()
        .get("submodules")
        .is_none());
    let base_ref = AgentRemote::new(workspace().as_str(), agent.as_str())
        .unwrap()
        .ref_name(intent_git::hub::AgentRef::Base);
    for pin in &expected {
        assert_eq!(
            git(&repo.join(&pin.path), &["rev-parse", &base_ref]),
            pin.commit
        );
    }

    let launch = f.launch_frame(&mut link, &agent).await;
    let checkout = launch
        .checkout
        .expect("a protocol-10 launch prepares the checkout");
    assert_eq!(checkout.submodules, expected);
    assert_eq!(checkout.setup, None);
    f.node.shutdown();
}

/// A module session reaches only a submodule recorded in the agent's own
/// pins, at a real gitlink inside the checkout, and only on nodeProtocol 10.
#[tokio::test]
async fn module_git_sessions_are_confined_to_the_agents_pins() {
    let (f, link, _machine) = prepared_fixture().await;
    let _modules = add_submodules(f.repo.path());
    let repo = f.repo.path().to_path_buf();
    let (agent, mut view) = f.placed().await;
    let (other, _) = f.placed().await;
    let access = crate::git::HeadGitAccess::new(f.node.shared());
    let binding = link.hello.binding.clone();
    let open = |agent: &AgentId, module_key: &str, service| {
        let remote = module_remote(agent, module_key);
        let access = &access;
        let binding = &binding;
        async move { access.repository(binding, &remote, service).await }
    };

    for path in ["a", "a/b", "c"] {
        let opened = open(&agent, &key(&repo.join(path)), GitService::UploadPack)
            .await
            .unwrap();
        assert_eq!(
            opened.canonicalize().unwrap(),
            repo.join(path).canonicalize().unwrap()
        );
    }
    assert!(is_denied(
        &open(
            &agent,
            "repo-0123456789abcdef0123456789abcdef",
            GitService::UploadPack
        )
        .await
    ));
    assert!(is_denied(
        &open(&agent, &key(&repo), GitService::UploadPack).await
    ));

    // Another agent's pins do not admit this one.
    let mut unpinned = f.view(&other).await;
    unpinned.submodules = Some(Vec::new());
    f.set_view(&other, &unpinned).await;
    assert!(is_denied(
        &open(&other, &key(&repo.join("a")), GitService::UploadPack).await
    ));

    // A recorded pin is checked against the checkout: a nested repository
    // that is not a gitlink, a gitlink replaced by a symlink to another
    // repository, a pin whose repository changed, an unpopulated gitlink.
    let outside = tempfile::tempdir().unwrap();
    repo_with_commit(outside.path(), "outside.txt");
    let moved = tempfile::tempdir().unwrap();
    std::fs::rename(repo.join("c"), moved.path().join("c")).unwrap();
    std::os::unix::fs::symlink(outside.path(), repo.join("c")).unwrap();
    let pin = |path: &str, module_key: String| SubmodulePin {
        path: path.into(),
        name: path.into(),
        commit: "0".repeat(40),
        module_key,
    };
    view.submodules = Some(vec![
        pin("nested", key(&repo.join("nested"))),
        pin("c", key(outside.path())),
        pin("a", key(&repo.join("a/b"))),
        pin("d", "repo-dddddddddddddddddddddddddddddddd".into()),
        pin("../escape", "repo-eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee".into()),
    ]);
    f.set_view(&agent, &view).await;
    assert!(is_denied(
        &open(&agent, &key(&repo.join("nested")), GitService::UploadPack).await
    ));
    assert!(is_denied(
        &open(&agent, &key(outside.path()), GitService::UploadPack).await
    ));
    assert!(is_denied(
        &open(&agent, &key(&repo.join("a/b")), GitService::UploadPack).await
    ));
    assert!(is_unavailable(
        &open(
            &agent,
            "repo-dddddddddddddddddddddddddddddddd",
            GitService::UploadPack
        )
        .await
    ));
    assert!(is_denied(
        &open(
            &agent,
            "repo-eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
            GitService::UploadPack
        )
        .await
    ));

    // A module session on a link below nodeProtocol 10 is invalid.
    f.node
        .shared_ref()
        .set_protocol_for_test(PREPARED_CHECKOUT_PROTOCOL - 1);
    let old = open(&agent, &key(&repo.join("nested")), GitService::UploadPack).await;
    assert!(
        matches!(old, Err(Error::InvalidParams(ref m)) if m == "node-git-invalid"),
        "{old:?}"
    );
    f.node.shutdown();
}

/// A module session serves only the agent's refs: the module repository's
/// own branches stay hidden.
#[tokio::test]
async fn a_module_session_advertises_only_the_agents_refs() {
    let (f, link, _machine) = prepared_fixture().await;
    let modules = add_submodules(f.repo.path());
    let (agent, _) = f.placed().await;
    let module_key = key(&f.repo.path().join("a"));
    let remote = module_remote(&agent, &module_key);
    let access = crate::git::HeadGitAccess::new(f.node.shared());
    let path = access
        .repository(&link.hello.binding, &remote, GitService::UploadPack)
        .await
        .unwrap();
    let mut command = node_git::service_command(GitService::UploadPack, &path, &remote).unwrap();
    let child = command.spawn().unwrap();
    drop(child.stdin);
    let mut advertised = String::new();
    tokio::io::AsyncReadExt::read_to_string(&mut child.stdout.unwrap(), &mut advertised)
        .await
        .unwrap();
    assert!(
        advertised.contains(&format!(
            "{} {}",
            modules.a,
            remote.ref_name(intent_git::hub::AgentRef::Base)
        )),
        "{advertised}"
    );
    // Every advertised ref is the agent's; `symref=HEAD:…` in the
    // capabilities names no ref.
    assert!(!advertised.contains(" refs/heads/"), "{advertised}");
    assert!(!advertised.contains(" HEAD"), "{advertised}");
    f.node.shutdown();
}

/// A final stop records the agent's module repositories, so their last
/// pushes are admitted (receive-pack only) after the placement is gone.
#[tokio::test]
async fn a_final_stop_admits_the_module_pushes() {
    let (f, mut link, _machine) = prepared_fixture().await;
    let _modules = add_submodules(f.repo.path());
    let repo: PathBuf = f.repo.path().to_path_buf();
    let (agent, _) = f.placed().await;
    let access = crate::git::HeadGitAccess::new(f.node.shared());
    let binding = link.hello.binding.clone();
    let stop_id = f
        .node
        .final_stop(
            &agent,
            intent_core::remote_launch::FinalStopReason::AgentDeleted,
        )
        .await;
    assert_eq!(link.stop_for(&agent).await, stop_id);
    sqlx::query("DELETE FROM agent_session WHERE id = ?")
        .bind(agent.as_str())
        .execute(f.backend.store().write_pool())
        .await
        .unwrap();
    let a = module_remote(&agent, &key(&repo.join("a/b")));
    let pushed = access
        .repository(&binding, &a, GitService::ReceivePack)
        .await
        .unwrap();
    assert_eq!(
        pushed.canonicalize().unwrap(),
        PathBuf::from(git(&repo.join("a/b"), &["rev-parse", "--absolute-git-dir"]))
            .canonicalize()
            .unwrap()
    );
    assert!(is_denied(
        &access
            .repository(&binding, &a, GitService::UploadPack)
            .await
    ));
    let unknown = module_remote(&agent, &key(&repo.join("nested")));
    assert!(is_denied(
        &access
            .repository(&binding, &unknown, GitService::ReceivePack)
            .await
    ));
    f.node.shutdown();
}

/// A machine below nodeProtocol 10 can neither prepare submodules nor run
/// the setup script, so a workspace with populated submodules or a setup
/// script is refused with machine-update-required; one with neither
/// launches as before, without a `checkout`.
#[tokio::test]
async fn an_older_machine_is_refused_only_when_the_checkout_needs_preparing() {
    let (f, mut link, _machine) = prepared_fixture().await;
    f.node
        .shared_ref()
        .set_protocol_for_test(PREPARED_CHECKOUT_PROTOCOL - 1);
    let (plain, _) = f.placed().await;
    let launch = f.launch_frame(&mut link, &plain).await;
    assert_eq!(launch.checkout, None);

    let config = f.repo.path().join(".intent");
    std::fs::create_dir_all(&config).unwrap();
    std::fs::write(
        config.join("config.json"),
        r#"{"setupScript":"echo ready"}"#,
    )
    .unwrap();
    let message = update_required(f.admit(r#"{"target":"remote"}"#).await);
    assert!(message.contains("setup script"), "{message}");
    std::fs::remove_file(config.join("config.json")).unwrap();

    let _modules = add_submodules(f.repo.path());
    let message = update_required(f.admit(r#"{"target":"remote"}"#).await);
    assert!(message.contains("Test machine"), "{message}");

    // An agent pinned on a protocol-10 link that later reconnects below
    // 10 is refused at its start rather than launched without them.
    f.node
        .shared_ref()
        .set_protocol_for_test(PREPARED_CHECKOUT_PROTOCOL);
    let (pinned, _) = f.placed().await;
    f.node
        .shared_ref()
        .set_protocol_for_test(PREPARED_CHECKOUT_PROTOCOL - 1);
    update_required(f.port().runtime(&f.services, &pinned).await.map(|_| ()));
    assert_eq!(
        f.view(&pinned).await.placement_error.map(|e| e.code),
        Some("machine-update-required".to_owned())
    );
    f.node.shutdown();
}

/// A prepared agent gets `checkout` on a protocol 10 launch even with no
/// submodule; an agent placed before pins were captured never does.
#[tokio::test]
async fn only_prepared_agents_get_a_checkout() {
    let (f, mut link, _machine) = prepared_fixture().await;
    let (prepared, _) = f.placed().await;
    let launch = f.launch_frame(&mut link, &prepared).await;
    assert_eq!(launch.checkout, Some(Box::default()));

    let (legacy, mut view) = f.placed().await;
    view.submodules = None;
    f.set_view(&legacy, &view).await;
    let launch = f.launch_frame(&mut link, &legacy).await;
    assert_eq!(launch.checkout, None);
    f.node.shutdown();
}

/// This build offers nodeProtocol 10 by default: an agent of a workspace
/// with submodules and a setup script gets `checkout` with its pins and the
/// script (resolved from the worktree's `.intent/config.json`), module
/// sessions are served, the projection shows the pins and the create notice
/// names the submodules and the setup.
#[tokio::test]
async fn the_prepared_checkout_carries_the_pins_and_the_setup_script() {
    let (f, mut link, _machine) = fixture().await;
    assert!(intent_core::node_link::offers_prepared_checkout());
    assert!(f.node.shared_ref().offers_prepared_checkout());
    f.node
        .shared_ref()
        .set_protocol_for_test(PREPARED_CHECKOUT_PROTOCOL);
    let _modules = add_submodules(f.repo.path());
    let config = f.repo.path().join(".intent");
    std::fs::create_dir_all(&config).unwrap();
    std::fs::write(
        config.join("config.json"),
        r#"{"setupScript":"echo ready"}"#,
    )
    .unwrap();

    let (agent, view) = f.placed().await;
    assert_eq!(view.pins().len(), 3);
    let launch = f.launch_frame(&mut link, &agent).await;
    let checkout = launch.checkout.expect("a prepared checkout");
    assert_eq!(checkout.submodules, view.pins());
    assert_eq!(
        checkout.setup,
        Some(intent_core::node_link::SetupCommand {
            script: "echo ready".into(),
            branch_name: "main".into(),
            source_branch: String::new(),
            timeout_secs: None,
        })
    );
    let placed = f.view(&agent).await;
    assert_eq!(placed.placement_error, None);
    assert_eq!(
        placed.setup.map(|setup| setup.state),
        Some(intent_core::remote_launch::RemoteSetupState::Pending),
        "the first prepared launch shows the checkout being prepared"
    );

    let access = crate::git::HeadGitAccess::new(f.node.shared());
    let module = module_remote(&agent, &key(&f.repo.path().join("a")));
    let served = access
        .repository(&link.hello.binding, &module, GitService::UploadPack)
        .await
        .unwrap();
    assert_eq!(
        served.canonicalize().unwrap(),
        f.repo.path().join("a").canonicalize().unwrap()
    );

    let session = f
        .services
        .store()
        .get_agent_session_with_transcript(&agent)
        .await
        .unwrap();
    let lite =
        intent_core::model::AgentLite::from_session(session, 0, None, None, None, None, None);
    assert!(serde_json::to_value(&lite)
        .unwrap()
        .get("submodules")
        .is_some());
    assert_ne!(
        intent_core::remote_launch::launch_notice("m", &f.base, true),
        intent_core::remote_launch::launch_notice("m", &f.base, false)
    );
    assert!(
        intent_core::remote_launch::launch_notice_with_setup("m", &f.base, false, true)
            .contains("setup script runs on that machine")
    );
    f.node.shutdown();
}

/// The machine's setup records and output reach the agent's placement:
/// `setupStarted` makes it `running`, `setupFinished` records the result
/// with its start time, each announced as `agent:updated`; `setupOutput`
/// is kept (seq 0 starts it over) and shown on detail reads only. Output
/// for an agent not attached here is dropped.
#[tokio::test]
async fn setup_records_and_output_reach_the_agents_placement() {
    use intent_core::node_link::{JournalBody, JournalRecord, SetupOutput, SetupState};
    use intent_core::nodes::NodeCounter;
    use intent_core::remote_launch::RemoteSetupState;
    use intent_core::WorkspaceApi as _;
    let (f, mut link, _machine) = prepared_fixture().await;
    let (agent, _) = f.placed().await;
    let _runtime = f.port().runtime(&f.services, &agent).await.unwrap();
    link.expect(|frame| match frame {
        LinkFrame::Launch(command) => Some(command),
        _ => None,
    })
    .await;
    let record = |seq: u64, body: JournalBody| JournalRecord {
        seq: NodeCounter(seq),
        agent_id: agent.clone(),
        workspace_id: workspace(),
        turn_id: None,
        recorded_at: intent_core::now_iso(),
        body,
    };
    let output = |agent: &AgentId, seq: u64, data: &str| {
        assert!(link
            .link
            .try_send_setup_output(SetupOutput {
                agent_id: agent.clone(),
                seq,
                data: data.into(),
            })
            .unwrap());
    };
    output(&agent, 0, "stale\n");
    output(&agent, 0, "Fetching a…\n");
    output(&agent, 1, "ready\n");
    output(&AgentId::new(), 0, "someone else\n");
    link.journal(record(
        1,
        JournalBody::SetupStarted {
            started_at: "2026-11-05T10:00:00Z".into(),
        },
    ))
    .await;
    link.acked(1).await;
    let running = f.view(&agent).await.setup.unwrap();
    assert_eq!(running.state, RemoteSetupState::Running);
    assert_eq!(running.started_at.as_deref(), Some("2026-11-05T10:00:00Z"));
    link.journal(record(
        2,
        JournalBody::SetupFinished {
            state: SetupState::Failed,
            exit_code: Some(3),
            error: None,
            finished_at: "2026-11-05T10:00:05Z".into(),
        },
    ))
    .await;
    link.acked(2).await;
    let done = f.view(&agent).await.setup.unwrap();
    assert_eq!(done.state, RemoteSetupState::Failed);
    assert_eq!(done.exit_code, Some(3));
    assert_eq!(done.started_at.as_deref(), Some("2026-11-05T10:00:00Z"));
    assert_eq!(done.finished_at.as_deref(), Some("2026-11-05T10:00:05Z"));
    assert_eq!(done.output, None, "the placement stores no output");

    let detail = f
        .services
        .agent_get(agent.clone(), Some(workspace()))
        .await
        .unwrap();
    let setup = serde_json::to_value(&detail).unwrap()["setup"].clone();
    assert_eq!(setup["state"], "failed", "{setup}");
    assert_eq!(setup["output"], "Fetching a…\nready\n", "{setup}");
    assert_eq!(setup["outputTruncated"], false, "{setup}");
    let mut row = detail;
    row.strip_detail_only_fields();
    let row = serde_json::to_value(&row).unwrap();
    assert_eq!(row["setup"]["state"], "failed", "{row}");
    assert!(row["setup"].get("output").is_none(), "{row}");
    f.node.shutdown();
}
