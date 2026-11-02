//! Fills the large fixture's documented gaps so every read reaches data:
//! state the fixture does not seed (git history, browser tabs, interrupted
//! agents, line attribution, …) is written here through the store, and
//! runtime-only state (terminals, script output, MCP servers, context) is
//! created over RPC once the daemon serves.
//!
//! Sizes stay realistic: each value is something a real client, agent or
//! repository produces, just many of them.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::process::Command;

use intent_core::{
    AgentStatus, BrowserTabInput, BrowserTabVisibility, ClientId, ContentType, LineAttributionData,
    Note, NoteId, NoteMetadata, NoteVersionAuthor, NoteVisibility, RepositoryProvider,
    RepositoryResourceKind, RepositoryTarget, ReviewTarget,
};
use intent_services::test_support::large_state::LargeState;
use intent_store::MonitorTargetProvenance;
use serde_json::{json, Value};

use crate::harness::Client;

/// Ids and paths of what this module seeds, for the sweep rows.
#[derive(Debug, Clone, Default)]
pub struct Seeded {
    /// Note whose body is thousands of checkbox tasks (`note.listTasks`).
    pub checklist_note: String,
    /// Sibling of the big workspace (same repository) for `crossWorkspace.*`.
    pub sibling_workspace: String,
    /// The PR URL many workspaces are linked to.
    pub pr_url: String,
    /// Branch at the git repo's first commit (branch-diff base).
    pub base_branch: String,
    /// A commit in the middle of the seeded history.
    pub mid_commit: String,
    /// Directory holding many repositories (`workspace.findRepositories`).
    pub repos_dir: PathBuf,
    /// Big attachment id (`file.getAttachmentInfo`).
    pub attachment: String,
    /// Terminal whose scrollback is large.
    pub terminal: String,
    /// Script whose output buffer is large.
    pub loud_script: String,
    /// One seeded MCP server id.
    pub mcp_server: String,
    /// The browser-tab host client id.
    pub browser_client: String,
    /// A registered git root of the big workspace.
    pub git_root: String,
}

/// Seeded counts, so proofs can demand pages near their limit.
pub const GIT_COMMITS: usize = 260;
pub const GIT_BRANCHES: usize = 400;
pub const CHECKLIST_TASKS: usize = 3_000;
pub const BROWSER_TABS: usize = 400;
pub const KNOWN_REPOS: usize = 800;
pub const FOUND_REPOS: usize = 600;
pub const CONVERSATIONS: usize = 150;
pub const ACTIVE_AGENTS: usize = 200;
pub const TERMINALS: usize = 40;
pub const MCP_SERVERS: usize = 120;
pub const CONTEXT_ITEMS: usize = 400;
pub const PR_LINKED_WORKSPACES: usize = 300;
pub const GIT_ROOTS: usize = 20;
pub const OAUTH_TOKENS: usize = 120;
pub const WATCHES: usize = 120;
pub const SCRIPT_MONITORS: usize = 250;
/// Idempotency key bound to the seeded attachment.
pub const ATTACHMENT_KEY: &str = "large-state-attachment-key";

#[track_caller]
fn git(dir: &Path, args: &[&str]) -> String {
    let out = Command::new("git")
        .current_dir(dir)
        .args([
            "-c",
            "user.name=Large State",
            "-c",
            "user.email=large@example.com",
        ])
        .args(args)
        .output()
        .unwrap_or_else(|e| panic!("git {args:?}: {e}"));
    assert!(
        out.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

/// Copy `from` over `to`, file by file (overwriting).
fn copy_tree(from: &Path, to: &Path) {
    for entry in std::fs::read_dir(from).expect("read dir") {
        let entry = entry.expect("dir entry");
        let target = to.join(entry.file_name());
        if entry.file_type().expect("file type").is_dir() {
            std::fs::create_dir_all(&target).expect("mkdir");
            copy_tree(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), &target).expect("copy");
        }
    }
}

/// Deterministic prose of `bytes` bytes.
fn prose(seed: usize, bytes: usize) -> String {
    const WORDS: [&str; 12] = [
        "agent", "budget", "commit", "daemon", "event", "fixture", "payload", "review", "stream",
        "sweep", "token", "worktree",
    ];
    let mut out = String::with_capacity(bytes + 16);
    let mut i = seed;
    while out.len() < bytes {
        i = i
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        out.push_str(WORDS[(i >> 33) % WORDS.len()]);
        out.push(if (i >> 40).is_multiple_of(11) {
            '\n'
        } else {
            ' '
        });
    }
    out.truncate(bytes);
    out
}

/// Turn the big workspace's worktree into a repository with history,
/// branches, a remote and a dirty working tree.
fn seed_git(worktree: &Path, seeded: &mut Seeded) {
    git(worktree, &["init", "-q", "-b", "main"]);
    git(
        worktree,
        &[
            "remote",
            "add",
            "origin",
            "https://github.com/intent-hq/large-state-repo-0.git",
        ],
    );
    std::fs::write(worktree.join(".gitignore"), "ignored/\n").expect("gitignore");
    git(worktree, &["add", "-A"]);
    git(
        worktree,
        &["commit", "-q", "-m", "Import the large-state worktree"],
    );
    seeded.base_branch = "large-state-base".into();
    git(worktree, &["branch", &seeded.base_branch]);
    // The workspace's base ref is `origin/main`: point it at the import so
    // history and ahead/behind reads have a boundary.
    git(
        worktree,
        &["update-ref", "refs/remotes/origin/main", "HEAD"],
    );
    // History in one fast-import: each commit adds a 6 KiB source file and
    // carries an agent-written multi-paragraph message.
    let mut stream = String::new();
    for i in 0..GIT_COMMITS {
        let body = prose(i, 6 * 1024);
        let message = format!(
            "feat: change {i} of the large-state history\n\n{}",
            prose(i + 7, 1_500)
        );
        let _ = write!(
            stream,
            "commit refs/heads/main\nmark :{m}\ncommitter Large State <large@example.com> {t} +0000\ndata {ml}\n{message}\n",
            m = i + 1,
            t = 1_700_000_000 + i * 60,
            ml = message.len(),
        );
        if i == 0 {
            stream.push_str("from refs/heads/main^0\n");
        }
        let _ = write!(
            stream,
            "M 100644 inline src/history/module_{i}.rs\ndata {}\n{body}\n",
            body.len()
        );
    }
    for b in 0..GIT_BRANCHES {
        let _ = writeln!(
            stream,
            "reset refs/heads/agent/large-state-branch-{b}-{}\nfrom :{}\n",
            "x".repeat(40),
            1 + b % GIT_COMMITS
        );
    }
    let mut child = Command::new("git")
        .current_dir(worktree)
        .args(["fast-import", "--quiet"])
        .stdin(std::process::Stdio::piped())
        .spawn()
        .expect("git fast-import");
    std::io::Write::write_all(child.stdin.as_mut().expect("stdin"), stream.as_bytes())
        .expect("fast-import stream");
    assert!(
        child.wait().expect("fast-import").success(),
        "fast-import failed"
    );
    git(worktree, &["reset", "-q", "--hard", "main"]);
    seeded.mid_commit = git(
        worktree,
        &["rev-parse", &format!("main~{}", GIT_COMMITS / 2)],
    );
    // A dirty tree: every skill edited, every history file edited, new files.
    for entry in std::fs::read_dir(worktree.join(".agents/skills")).expect("skills") {
        let path = entry.expect("skill").path().join("SKILL.md");
        let mut text = std::fs::read_to_string(&path).expect("skill body");
        text.push_str(&prose(text.len(), 600));
        std::fs::write(&path, text).expect("edit skill");
    }
    for i in 0..GIT_COMMITS {
        let path = worktree.join(format!("src/history/module_{i}.rs"));
        let mut text = std::fs::read_to_string(&path).expect("history file");
        text.insert_str(0, &prose(i + 3, 400));
        std::fs::write(&path, text).expect("edit history");
    }
    for i in 0..300 {
        std::fs::write(
            worktree.join(format!("src/history/untracked_{i}.rs")),
            prose(i + 11, 2_000),
        )
        .expect("untracked file");
    }
    git(
        worktree,
        &[
            "add",
            "src/history/untracked_0.rs",
            "src/history/module_0.rs",
        ],
    );
}

/// Store-level seeds; runs before the daemon serves.
pub async fn store(state: &LargeState) -> Seeded {
    let store = &state.store;
    let ids = &state.ids;
    let big_ws = &ids.big_workspace;
    let mut seeded = Seeded {
        sibling_workspace: ids.workspaces[40.min(ids.workspaces.len() - 1)].0.clone(),
        pr_url: "https://github.com/intent-hq/large-state-repo-0/pull/4242".into(),
        browser_client: "large-state-browser-host".into(),
        ..Seeded::default()
    };

    seed_git(&state.worktree, &mut seeded);

    // Registered git roots (sibling checkouts of the workspace). Root 0 is a
    // clone of the big repository carrying the same dirty working tree, so
    // root-scoped reads see the same volume; the rest are small packages.
    let roots_dir = state
        .worktree
        .parent()
        .expect("worktree parent")
        .join("roots");
    for i in 0..GIT_ROOTS {
        let dir = roots_dir.join(format!("sub-{i}"));
        if i == 0 {
            std::fs::create_dir_all(&roots_dir).expect("roots dir");
            git(
                &roots_dir,
                &[
                    "clone",
                    "-q",
                    &state.worktree.display().to_string(),
                    "sub-0",
                ],
            );
            git(
                &dir,
                &[
                    "update-ref",
                    "refs/remotes/origin/main",
                    &format!("main~{GIT_COMMITS}"),
                ],
            );
            copy_tree(&state.worktree.join(".agents"), &dir.join(".agents"));
            copy_tree(&state.worktree.join("src"), &dir.join("src"));
        } else {
            std::fs::create_dir_all(&dir).expect("sub repo");
            std::fs::write(dir.join("README.md"), prose(i, 2_000)).expect("sub readme");
            git(&dir, &["init", "-q", "-b", "main"]);
            git(&dir, &["add", "-A"]);
            git(&dir, &["commit", "-q", "-m", "init"]);
        }
        let (root, _) = store
            .upsert_workspace_git_root(&intent_core::WorkspaceGitRoot {
                id: intent_core::WorkspaceGitRootId::from_string(format!("large-state-root-{i}")),
                workspace_id: big_ws.clone(),
                path: std::fs::canonicalize(&dir)
                    .expect("root path")
                    .display()
                    .to_string(),
                source: intent_core::WorkspaceGitRootSource::Agent,
                repo_owner: Some("intent-hq".into()),
                repo_name: Some(format!("sub-{i}")),
                registered_by_agent_ids: vec![ids.big_agent.clone()],
                registered_commit_sha: None,
                pr_number: None,
                pr_url: None,
                pr_status: None,
                pull_requests: None,
                created_at: "2026-01-01T00:00:00.000Z".into(),
                updated_at: "2026-01-01T00:00:00.000Z".into(),
            })
            .await
            .expect("git root");
        if i == 0 {
            seeded.git_root.clone_from(&root.id.0);
        }
    }

    // MCP OAuth token bags (opaque, provider-issued).
    for i in 0..OAUTH_TOKENS {
        store
            .set_mcp_oauth_token(
                &format!("large-state-mcp-{i}"),
                &json!({ "access_token": prose(i, 1_200), "refresh_token": prose(i + 1, 400) })
                    .to_string(),
                "2026-01-01T00:00:00.000Z",
            )
            .await
            .expect("oauth token");
    }

    // Many repositories under one directory.
    let parent = state.worktree.parent().expect("worktree parent");
    seeded.repos_dir = parent.join("code");
    for i in 0..FOUND_REPOS {
        std::fs::create_dir_all(seeded.repos_dir.join(format!(
            "team-{}/service-{i}-with-a-descriptive-name/.git",
            i % 20
        )))
        .expect("repo dir");
    }

    // PR monitors: the fixture rows carry no target; qualify them the way
    // a legacy GitHub writer does so `prMonitor.list` serves them.
    let monitors = store
        .list_pr_monitors_by_workspace(big_ws)
        .await
        .expect("pr monitors");
    for m in monitors {
        let qualified = store
            .get_qualified_pr_monitor(&m.monitor_id)
            .await
            .expect("qualified monitor");
        let target = ReviewTarget {
            repository: RepositoryTarget {
                provider: RepositoryProvider::Github,
                instance_base_url: "https://github.com".into(),
                project_path: format!("{}/{}", m.repo_owner, m.repo_name),
            },
            kind: RepositoryResourceKind::PullRequest,
            number: u64::try_from(m.pr_number).expect("pr number"),
        };
        store
            .qualify_legacy_pr_monitor_target(
                &qualified,
                &target,
                MonitorTargetProvenance::LegacyGithubWriter,
            )
            .await
            .expect("qualify monitor");
    }

    // Shared principals carry a live credential (the sharing directory
    // lists only those).
    for (i, p) in ids.member_principals.iter().enumerate() {
        store
            .insert_principal_credential(p, &format!("{i:064x}"))
            .await
            .expect("principal credential");
    }

    // Every agent was interrupted by a restart.
    for (i, agent) in ids.agents.iter().enumerate() {
        let ws = store
            .get_agent_session(agent)
            .await
            .expect("agent session")
            .workspace_id;
        store
            .insert_interrupted_agent(
                agent,
                &ws,
                "active",
                &format!("2026-01-01T00:{:02}:00.000Z", i % 60),
            )
            .await
            .expect("interrupted agent");
    }

    // Agents actively working in sandboxes on tracked files, with
    // auto-commit on: the lock snapshot names them and their paths.
    store
        .set_workspace_auto_commit(big_ws, true)
        .await
        .expect("auto-commit");
    let mut active = 0;
    for agent in &ids.agents {
        if active == ACTIVE_AGENTS {
            break;
        }
        let mut s = store.get_agent_session(agent).await.expect("agent");
        if &s.workspace_id != big_ws {
            continue;
        }
        s.status = AgentStatus::Active;
        s.sandbox_path = Some(format!("/tmp/large-state-sandbox/{}", agent.0));
        store
            .update_agent_session(big_ws, &s)
            .await
            .expect("activate agent");
        active += 1;
    }

    // Browser tabs with long agent-visited URLs and page titles.
    let host = ClientId::from_string(&seeded.browser_client);
    store
        .upsert_client(
            &host,
            Some("Large state browser host"),
            None,
            &intent_core::ClientHostInfo::default(),
        )
        .await
        .expect("browser host");
    for i in 0..BROWSER_TABS {
        store
            .upsert_browser_tab(
                &host,
                BrowserTabInput {
                    tab_id: format!("tab-{i}"),
                    workspace_id: big_ws.clone(),
                    url: format!(
                        "https://example.com/search?q={}&page={i}",
                        prose(i, 600).replace([' ', '\n'], "+")
                    ),
                    requested_url: None,
                    title: Some(prose(i + 1, 200).replace('\n', " ")),
                    owner_agent_id: Some(ids.agents[i % ids.agents.len()].clone()),
                    owner_agent_name: Some(format!("Agent {i}")),
                    visibility: BrowserTabVisibility::Visible,
                    emulated_size: None,
                    displayed: Some(i % 2 == 0),
                },
            )
            .await
            .expect("browser tab");
    }

    // Known repositories.
    for i in 0..KNOWN_REPOS {
        store
            .upsert_known_repo(
                &format!(
                    "/home/dev/code/team-{}/service-{i}-with-a-descriptive-name",
                    i % 20
                ),
                &format!("service-{i}-with-a-descriptive-name"),
                Some("intent-hq"),
            )
            .await
            .expect("known repo");
    }

    // Line attribution for every line of the big note.
    let big_note = store
        .get_note(big_ws, &ids.big_note)
        .await
        .expect("big note");
    let attributions: serde_json::Map<String, Value> = (1..=big_note.content.lines().count())
        .map(|line| {
            (
                line.to_string(),
                json!({
                    "timestamp": 1_700_000_000_000_i64 + i64::try_from(line).unwrap_or(0),
                    "author": {
                        "id": ids.agents[line % ids.agents.len()].0,
                        "name": format!("Agent {}", line % 97),
                        "type": "agent",
                        "turnNumber": line % 40,
                    }
                }),
            )
        })
        .collect();
    let data: LineAttributionData = serde_json::from_value(json!({
        "noteId": ids.big_note.0,
        "workspaceId": big_ws.0,
        "computedAt": "2026-01-01T00:00:00.000Z",
        "attributions": attributions,
    }))
    .expect("line attribution");
    store
        .upsert_note_line_attribution(&data)
        .await
        .expect("line attribution");

    // A planning note with thousands of checkbox tasks.
    seeded.checklist_note = "large-state-checklist-note".into();
    let mut body = String::from("# Plan\n\n");
    for i in 0..CHECKLIST_TASKS {
        let _ = writeln!(
            body,
            "- [{}] Task {i}: {}",
            if i % 3 == 0 { "x" } else { " " },
            prose(i, 100).replace('\n', " ")
        );
    }
    store
        .insert_note_with_version(
            &Note {
                id: NoteId::from_string(&seeded.checklist_note),
                workspace_id: big_ws.clone(),
                title: "Large plan".into(),
                content: body,
                content_type: ContentType::Markdown,
                tags: vec!["plan".into()],
                is_pinned: false,
                is_archived: false,
                is_default: false,
                parent_id: None,
                visibility: NoteVisibility::Workspace,
                metadata: NoteMetadata::default(),
                created_at: "2026-01-01T00:00:00.000Z".into(),
                rev: 0,
                updated_at: "2026-01-01T00:00:00.000Z".into(),
            },
            &NoteVersionAuthor {
                id: ids.big_agent.0.clone(),
                name: "Large state".into(),
                author_type: "agent".into(),
            },
            "2026-01-01T00:00:00.000Z",
        )
        .await
        .expect("checklist note");

    // Many workspaces linked to one PR.
    for ws_id in ids.workspaces.iter().take(PR_LINKED_WORKSPACES) {
        let mut ws = store.get_workspace(ws_id).await.expect("workspace");
        ws.pr_url = Some(seeded.pr_url.clone());
        store.update_workspace(&ws).await.expect("link pr");
    }

    // One large attachment record.
    seeded.attachment = "large-state-attachment".into();
    store
        .insert_attachment_with_idempotency_key(
            &intent_store::AttachmentRecord {
                id: seeded.attachment.clone(),
                workspace_id: big_ws.clone(),
                file_name: format!("{}.png", "screenshot-".repeat(20)),
                mime_type: Some("image/png".into()),
                size: 25 * 1024 * 1024,
                // Far future: the key binding's retention window is relative to
                // now, and the fixture keeps fixed timestamps.
                uploaded_at: "2099-01-01T00:00:00.000Z".into(),
                stored_path: state
                    .worktree
                    .join("attachments/large.png")
                    .display()
                    .to_string(),
            },
            ATTACHMENT_KEY,
            "large-state-fingerprint",
            "2000-01-01T00:00:00.000Z",
        )
        .await
        .expect("attachment");

    seeded
}

/// A seeding call must succeed.
#[track_caller]
fn expect_ok(method: &str, reply: &crate::harness::Reply) {
    assert!(reply.error().is_none(), "seed {method}: {}", reply.frame);
}

/// Runtime seeds over RPC (and the services API, for state no RPC writes)
/// once the daemon serves.
pub async fn runtime(
    client: &mut Client,
    services: &intent_services::Services,
    state: &LargeState,
    seeded: &mut Seeded,
) {
    let ws = state.ids.big_workspace.0.clone();

    // The big agent watches the completion of many delegated children.
    // Even-indexed fixture agents live in the big workspace. Each watch
    // persists and publishes, so register them concurrently.
    let mut watches = tokio::task::JoinSet::new();
    for child in state.ids.agents.iter().skip(2).step_by(2).take(WATCHES) {
        let services = services.clone();
        let ws_id = state.ids.big_workspace.clone();
        let parent = state.ids.big_agent.clone();
        let child = child.clone();
        watches.spawn(intent_core::with_caller(
            intent_core::Caller::Daemon,
            async move {
                intent_core::WorkspaceApi::agent_watch_completion(&services, ws_id, parent, child)
                    .await
            },
        ));
    }
    while let Some(done) = watches.join_next().await {
        done.expect("watch task").expect("completion watch");
    }

    // Script monitors (the fixture's rows are settled by the startup
    // recovery, which finds no live run behind them).
    for i in 0..SCRIPT_MONITORS {
        state
            .store
            .insert_script_monitor(&intent_core::ScriptMonitor {
                monitor_id: format!("large-state-monitor-{i:04}"),
                workspace_id: state.ids.big_workspace.clone(),
                agent_id: state.ids.big_agent.clone(),
                // One active monitor per script: the fixture's script ids.
                script_id: format!("0000000a-0000-4000-8000-{i:012x}"),
                run_id: format!("large-state-run-{i}"),
                script_name: format!("script-{i}"),
                mode: intent_core::ScriptMode::Command,
                state: "active".into(),
                created_at: "2026-01-01T00:00:00.000Z".into(),
                expires_at: "2099-01-01T00:00:00.000Z".into(),
                output_pattern: Some(prose(i, 200).replace('\n', " ")),
                line_count: Some(200),
                settled_at: None,
                reason: None,
                result: None,
                trigger: None,
            })
            .await
            .expect("script monitor");
    }

    // Discussion conversations, each started by the owner.
    for i in 0..CONVERSATIONS {
        let r = client
            .call(
                "discussion.send",
                &json!({
                    "workspaceId": ws,
                    "messageId": format!("large-conversation-{i}"),
                    "body": prose(i, 2_000),
                    "createConversation": true,
                }),
            )
            .await;
        expect_ok("discussion.send", &r);
    }

    // Terminals running the default shell; the first prints a long build
    // log into its scrollback.
    for i in 0..TERMINALS {
        let r = client
            .call(
                "terminal.create",
                &json!({ "workspaceId": ws, "cols": 200, "rows": 50 }),
            )
            .await;
        expect_ok("terminal.create", &r);
        if i == 0 {
            seeded.terminal = r.result()["terminalId"]
                .as_str()
                .or_else(|| r.result()["id"].as_str())
                .unwrap_or_else(|| panic!("terminal id: {}", r.frame))
                .to_string();
        }
    }
    let r = client
        .call(
            "terminal.write",
            &json!({
                "terminalId": seeded.terminal,
                "data": base64::Engine::encode(
                    &base64::engine::general_purpose::STANDARD,
                    "for i in $(seq 1 30000); do echo \"line $i of a long build log with enough text to look real\"; done\n",
                ),
            }),
        )
        .await;
    expect_ok("terminal.write", &r);

    // A script with a long output buffer.
    let r = client
        .call(
            "script.create",
            &json!({
                "workspaceId": ws,
                "name": "large-state-loud",
                "command": "for i in $(seq 1 20000); do echo \"test output line $i: assertion passed with a realistic amount of detail\"; done",
                "mode": "command",
            }),
        )
        .await;
    expect_ok("script.create", &r);
    seeded.loud_script = r.result()["id"]
        .as_str()
        .or_else(|| r.result()["script"]["id"].as_str())
        .unwrap_or_else(|| panic!("script id: {}", r.frame))
        .to_string();
    let r = client
        .call(
            "script.start",
            &json!({ "workspaceId": ws, "scriptId": seeded.loud_script }),
        )
        .await;
    expect_ok("script.start", &r);

    // MCP servers with long launch configs.
    for i in 0..MCP_SERVERS {
        let r = client
            .call(
                "mcp.servers.create",
                &json!({
                    "config": {
                        "name": format!("large-state-mcp-{i}"),
                        "transport": "stdio",
                        "command": "/usr/bin/false",
                        "args": (0..20).map(|a| format!("--flag-{a}={}", prose(i * 20 + a, 40).replace([' ', '\n'], "-"))).collect::<Vec<_>>(),
                    }
                }),
            )
            .await;
        expect_ok("mcp.servers.create", &r);
        if i == 0 {
            seeded.mcp_server = r.result()["server"]["id"]
                .as_str()
                .unwrap_or_else(|| panic!("mcp server id: {}", r.frame))
                .to_string();
        }
    }

    // Workspace context items and a large UI context.
    let items: Vec<Value> = (0..CONTEXT_ITEMS)
        .map(|i| {
            json!({
                "id": format!("ctx-{i}"),
                "type": "link",
                "title": prose(i, 80).replace('\n', " "),
                "url": format!("https://example.com/docs/{i}"),
                "summary": prose(i + 1, 1_000),
            })
        })
        .collect();
    let r = client
        .call(
            "workspace.updateContext",
            &json!({ "workspaceId": ws, "items": items }),
        )
        .await;
    expect_ok("workspace.updateContext", &r);
    let panels: Vec<Value> = (0..200)
        .map(|i| json!({ "id": format!("panel-{i}"), "state": prose(i, 1_000) }))
        .collect();
    let r = client
        .call(
            "workspace.updateUiContext",
            &json!({ "workspaceId": ws, "uiContext": { "panels": panels } }),
        )
        .await;
    expect_ok("workspace.updateUiContext", &r);
}

/// Wait until the runtime seeds stop producing state: the loud script has
/// exited and the terminal has printed its whole log. Event-reading rows
/// are measured after this, so their pages do not depend on how far the
/// background output got.
pub async fn settle(client: &mut Client, state: &LargeState, seeded: &Seeded) {
    let ws = state.ids.big_workspace.0.clone();
    for _ in 0..2_400 {
        let script = client
            .call(
                "script.status",
                &json!({ "workspaceId": ws, "scriptId": seeded.loud_script }),
            )
            .await;
        let terminal = client
            .call(
                "terminal.readOutput",
                &json!({ "workspaceId": ws, "terminalId": seeded.terminal, "maxLines": 5 }),
            )
            .await;
        let script_done = script.result()["status"] == json!("exited");
        let terminal_done = terminal.result().to_string().contains("line 30000 of");
        if script_done && terminal_done {
            return;
        }
        // timing-guard: poll interval while seeded shells finish printing; no completion event covers both
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!("runtime seeds did not settle (loud script exited, terminal log printed)");
}
