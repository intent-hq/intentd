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
    /// Asset URL of a note asset that fits the object read cap.
    pub mid_asset: String,
    /// Asset URL of a note asset far over the object read cap.
    pub large_asset: String,
}

/// Committed text file far over the object read cap (`file.read`,
/// `git.showFile`). Its path and text hold no `e` or `s`, so file-name and
/// in-file search rows do not grow.
pub const FULL_FILE: &str = "big/full.txt";
/// Committed text file just under the object read cap.
pub const MID_FILE: &str = "big/mid.txt";
/// One line of the big files.
const BIG_LINE: &str = "0123456789 abcd fghijk lmnopqr tuvwxyz 0123456789\n";

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
/// Installed plugins under the isolated plugins home.
pub const PLUGINS: usize = 60;
pub const CONTEXT_ITEMS: usize = 400;
pub const PR_LINKED_WORKSPACES: usize = 300;
pub const GIT_ROOTS: usize = 20;
pub const OAUTH_TOKENS: usize = 120;
pub const WATCHES: usize = 120;
const WATCH_CONCURRENCY: usize = 4;
/// Commits of the big repository above the workspace boundary.
pub const BOUNDARY_DEPTH: usize = GIT_COMMITS / 2;
/// Agents in each of the `agent.list` bins the fixture leaves empty.
pub const RETIRED_AGENTS: usize = 300;
pub const ORPHANED_AGENTS: usize = 300;
pub const TOP_LEVEL_AGENTS: usize = 300;
/// Comment threads whose latest comment is by a user, and by an agent.
pub const USER_THREADS: usize = 150;
pub const AGENT_THREADS: usize = 150;
/// `file:changed` events by the big agent, all on [`HOT_FILE`].
pub const AGENT_FILE_EVENTS: usize = 300;
pub const HOT_FILE: &str = "src/large_state/hot_file.rs";
/// Tracked changes owned by the big agent.
pub const AGENT_TRACKED_CHANGES: usize = 500;
/// Provider profiles per base provider (the daemon's cap is 32).
pub const PROFILES_PER_PROVIDER: usize = 32;
/// Pending host invites.
pub const HOST_INVITES: usize = 40;
/// Agent whose one assistant message holds a tool result of
/// [`BIG_BLOCK_BYTES`] (block id [`BIG_BLOCK_ID`]): several capped pages of
/// `agent.getMessageBlock`.
pub const BIG_BLOCK_AGENT: &str = "agent-large-state-big-block";
pub const BIG_BLOCK_ID: &str = "large-state-big-tool-result";
pub const BIG_BLOCK_BYTES: usize = 2 * 1024 * 1024;

/// Backends with an executor credential on this machine, and machines
/// exposed to this backend.
pub const EXECUTOR_BACKENDS: usize = 30;
pub const EXPOSED_MACHINES: usize = 30;
/// Cloud machine rows (one per live E2B sandbox) and their provider.
pub const CLOUD_MACHINES: usize = 30;
/// Extra hello'd device connections kept open for `client.list`.
pub const DEVICES: usize = 30;
const DEVICE_KINDS: [&str; 3] = ["desktop", "laptop", "phone"];
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
/// Installed Agent Plugins with skills under an isolated plugins home
/// (`INTENTD_USER_PLUGINS_DIR`), so `plugin.list` has a full inventory to
/// return. Skills only: plugin MCP servers would be started by the hub.
fn seed_plugins(worktree: &Path) {
    let home = worktree
        .parent()
        .expect("worktree parent")
        .join("plugins-home");
    let description = "Plugin description long enough to weigh something. ".repeat(20);
    for i in 0..PLUGINS {
        let root = home.join("plugins").join(format!("plugin-{i:03}"));
        std::fs::create_dir_all(&root).expect("plugin dir");
        std::fs::write(
            root.join("plugin.json"),
            json!({
                "$schema": "https://agent-plugins.org/schemas/1.0.0/plugin.schema.json",
                "name": format!("plugin-{i:03}"),
                "version": "1.0.0",
                "description": description,
                "keywords": ["large", "state"],
            })
            .to_string(),
        )
        .expect("plugin manifest");
        for k in 0..3 {
            let skill = root.join("skills").join(format!("skill-{k}"));
            std::fs::create_dir_all(&skill).expect("skill dir");
            std::fs::write(
                skill.join("SKILL.md"),
                format!("---\nname: skill-{k}\ndescription: Seeded plugin skill {k}.\n---\nBody\n"),
            )
            .expect("skill file");
        }
    }
    std::env::set_var("INTENTD_USER_PLUGINS_DIR", &home);
}

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
    // A README naming many project terms (the voice vocabulary source).
    let mut readme = String::from("# Large state\n\n");
    for i in 0..400 {
        let _ = writeln!(
            readme,
            "- `PayloadBudget{i}Sweep` drives `LargeState{i}Fixture` through `IntentDaemon{}`.",
            i % 37
        );
    }
    std::fs::write(worktree.join("README.md"), readme).expect("readme");
    std::fs::write(
        worktree.join(".intent/config.json"),
        crate::proxies::repo_config_json(),
    )
    .expect("repo config");
    // Committed with the import, before the branch boundary, so branch
    // diffs and the dirty tree do not carry them.
    std::fs::create_dir_all(worktree.join("big")).expect("big dir");
    std::fs::write(
        worktree.join(FULL_FILE),
        BIG_LINE.repeat(2 * 1024 * 1024 / BIG_LINE.len()),
    )
    .expect("full file");
    std::fs::write(
        worktree.join(MID_FILE),
        BIG_LINE.repeat(400_000 / BIG_LINE.len()),
    )
    .expect("mid file");
    git(worktree, &["add", "-A"]);
    git(
        worktree,
        &["commit", "-q", "-m", "Import the large-state worktree"],
    );
    seeded.base_branch = "large-state-base".into();
    git(worktree, &["branch", &seeded.base_branch]);
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
    // The workspace's base ref is `origin/main`: point it halfway up the
    // history, so the workspace has commits on both sides of its boundary
    // (`file-tracking.loadCommits` with and without `includeOlder`).
    git(
        worktree,
        &[
            "update-ref",
            "refs/remotes/origin/main",
            &format!("main~{BOUNDARY_DEPTH}"),
        ],
    );
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
    seed_plugins(&state.worktree);

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
            .get_agent_session_with_transcript(agent)
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
        let mut s = store
            .get_agent_session_with_transcript(agent)
            .await
            .expect("agent");
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

    // `agent.list` bins: retired sessions, delegated sessions whose parent
    // is gone, and top-level sessions.
    for i in 0..RETIRED_AGENTS + ORPHANED_AGENTS + TOP_LEVEL_AGENTS {
        let id = intent_core::AgentId::from_string(format!("agent-large-state-bin-{i:04}"));
        let mut s = intent_services::test_support::fixtures::session(&id, big_ws);
        s.name = format!("Bin agent {i}");
        s.created_at = format!("2026-02-01T00:{:02}:{:02}.000Z", (i / 60) % 60, i % 60);
        s.updated_at.clone_from(&s.created_at);
        s.completion_report = Some(prose(i, 1_500));
        if i < RETIRED_AGENTS {
            s.retired_at = Some("2026-03-01T00:00:00.000Z".into());
        } else if i < RETIRED_AGENTS + ORPHANED_AGENTS {
            s.parent_agent_id = Some(intent_core::AgentId::from_string(format!(
                "agent-large-state-deleted-parent-{}",
                i % 7
            )));
            s.specialist = Some("implementor".into());
        }
        store.insert_agent_session(&s).await.expect("bin agent");
    }

    // Root-only comment threads: half end with a user, half with an agent.
    for t in 0..USER_THREADS + AGENT_THREADS {
        let id = format!("large-state-thread-{t:04}");
        let by_user = t < USER_THREADS;
        store
            .insert_comment(
                big_ws,
                &intent_core::Comment {
                    id: id.clone(),
                    thread_id: id,
                    note_id: Some(ids.big_note.clone()),
                    kind: intent_core::CommentType::Comment,
                    content: prose(t, 1_000),
                    author: if by_user {
                        "owner".into()
                    } else {
                        "Agent 0".into()
                    },
                    author_type: if by_user {
                        intent_core::AuthorType::User
                    } else {
                        intent_core::AuthorType::Agent
                    },
                    author_principal_id: None,
                    author_identity: None,
                    status: intent_core::CommentStatus::Open,
                    parent_id: None,
                    anchor: None,
                    anchor_text: Some(prose(t + 1, 80)),
                    anchor_before: None,
                    anchor_after: None,
                    suggestion_original: None,
                    suggestion_proposed: None,
                    agent_id: (!by_user).then(|| ids.big_agent.clone()),
                    is_orphaned: None,
                    created_at: "2026-02-01T00:00:00.000Z".into(),
                    updated_at: "2026-02-01T00:00:00.000Z".into(),
                },
            )
            .await
            .expect("thread");
    }

    // The big agent's current file activity: many edits of one hot file,
    // stamped now so the default recent-activity windows see them.
    let now = intent_core::clock::now_iso();
    let events: Vec<intent_store::NewEvent> = (0..AGENT_FILE_EVENTS)
        .map(|i| intent_store::NewEvent {
            workspace_id: big_ws.clone(),
            timestamp: now.clone(),
            event_type: intent_core::events::FILE_CHANGED.to_string(),
            actor: intent_core::EventActor {
                actor_type: intent_core::ActorType::Agent,
                id: Some(ids.big_agent.0.clone()),
                name: Some("Agent 0".into()),
                ..Default::default()
            },
            session_id: Some(ids.big_agent.0.clone()),
            correlation_id: None,
            parent_event_id: None,
            metadata: None,
            data: json!({ "path": HOT_FILE, "summary": prose(i, 300) }),
        })
        .collect();
    store
        .insert_events(&events)
        .await
        .expect("agent file events");

    // Tracked changes owned by the big agent.
    for i in 0..AGENT_TRACKED_CHANGES {
        store
            .upsert_tracked_change(&intent_store::NewTrackedChange {
                workspace_id: big_ws.clone(),
                path: format!("packages/app/src/agent_zero/module_{i}.tsx"),
                stage: "unstaged".into(),
                status: "modified".into(),
                agent_id: Some(ids.big_agent.0.clone()),
                session_id: None,
                turn: Some(1),
                commit_hash: None,
                old_blob_sha: Some(format!("{i:040x}")),
                new_blob_sha: Some(format!("{:040x}", i + 1)),
                additions: 10,
                deletions: 3,
            })
            .await
            .expect("agent tracked change");
    }

    // One very large tool output (a full build log captured by an agent).
    let user = json!([{ "type": "text", "text": "Run the full build and show me the log." }]);
    let assistant = json!([
        { "type": "text", "text": "Here is the build log." },
        { "type": "tool_result", "id": BIG_BLOCK_ID, "tool_use_id": "big-build",
          "output": prose(7, BIG_BLOCK_BYTES), "is_error": false },
    ]);
    store
        .insert_agent_session_with_messages(
            &intent_services::test_support::fixtures::session(
                &intent_core::AgentId::from_string(BIG_BLOCK_AGENT),
                big_ws,
            ),
            &[
                intent_store::ReplaceMessage {
                    role: "user",
                    content: &user,
                    metadata: None,
                    created_at: "2026-02-01T00:00:00.000Z",
                },
                intent_store::ReplaceMessage {
                    role: "assistant",
                    content: &assistant,
                    metadata: None,
                    created_at: "2026-02-01T00:00:01.000Z",
                },
            ],
        )
        .await
        .expect("big-block agent");

    // Daemon-to-daemon machines: backends this machine admits, and
    // machines exposed to this backend (each with several routes).
    for i in 0..EXECUTOR_BACKENDS {
        store
            .insert_executor_credential(
                &intent_core::machines::ExecutorCredential {
                    id: intent_core::ExecutorCredentialId::from(format!(
                        "large-state-credential-{i:02}"
                    )),
                    backend_id: format!("large-state-backend-{i:02}"),
                    backend_name: format!("Backend {i} — {}", prose(i, 60).replace('\n', " ")),
                    backend_fingerprint: Some(format!("{i:064x}")),
                    created_at: "2026-01-01T00:00:00.000Z".into(),
                    last_used_at: Some("2026-01-02T00:00:00.000Z".into()),
                    revoked_at: None,
                },
                &format!("{:064x}", i + 1_000),
            )
            .await
            .expect("executor credential");
    }
    for i in 0..EXPOSED_MACHINES {
        store
            .save_exposed_machine(&intent_core::machines::ExposedMachine {
                id: format!("large-state-machine-{i:02}"),
                name: format!("Machine {i} — {}", prose(i + 1, 60).replace('\n', " ")),
                hosts: (0..6).map(|h| format!("10.{i}.{h}.1")).collect(),
                port: 7443,
                tc_address: Some(format!("tc{i:032x}")),
                fingerprint: format!("{:064x}", i + 2_000),
                credential_id: intent_core::ExecutorCredentialId::from(format!(
                    "remote-credential-{i:02}"
                )),
                platform: None,
                capabilities: None,
                observed_at: None,
                state: intent_core::machines::RemoteMachineState::Disconnected,
                added_at: "2026-01-01T00:00:00.000Z".into(),
                last_connected_at: None,
                node_protocol: None,
                cloud: None,
            })
            .await
            .expect("exposed machine");
    }
    store
        .save_cloud_machine_provider(
            "e2b",
            &json!({
                "templateProject": "intent",
                "size": "4x8",
                "idlePauseMinutes": 10,
                "pausedDeleteDays": 7,
                "configuredAt": "2026-01-01T00:00:00.000Z",
                "sizes": ["2x4", "4x8", "8x16", "16x32"],
            })
            .to_string(),
        )
        .await
        .expect("cloud machine provider");
    for i in 0..CLOUD_MACHINES {
        store
            .save_exposed_machine(&intent_core::machines::ExposedMachine {
                id: format!("large-state-cloud-machine-{i:02}"),
                name: format!("E2B · {}", prose(i + 2, 60).replace('\n', " ")),
                hosts: Vec::new(),
                port: 5181,
                tc_address: Some(format!("tc{i:032x}")),
                fingerprint: format!("{:064x}", i + 3_000),
                credential_id: intent_core::ExecutorCredentialId::from(format!(
                    "cloud-credential-{i:02}"
                )),
                platform: None,
                capabilities: None,
                observed_at: None,
                state: intent_core::machines::RemoteMachineState::Disconnected,
                added_at: "2026-01-01T00:00:00.000Z".into(),
                last_connected_at: None,
                node_protocol: None,
                cloud: Some(intent_core::machines::CloudMachine {
                    provider: "e2b".into(),
                    cloud_machine_id: "e2b".into(),
                    sandbox_id: format!("sbx{i:024x}"),
                    state: intent_core::machines::CloudSandboxState::Running,
                    template: "intent/intent-node-16x32:0.11.139".into(),
                    size: intent_core::machines::CloudMachineSize::S16x32,
                    started_by: intent_core::machines::CloudStartedBy {
                        principal_id: format!("principal-{i:032x}"),
                        display_name: Some(prose(i + 3, 40).replace('\n', " ")),
                    },
                    agent_id: format!("agent-{i:032x}"),
                    workspace_id: big_ws.0.clone(),
                    started_at: "2026-01-01T00:00:00.000Z".into(),
                    running_seconds: u64::MAX,
                    running_since: None,
                    paused_at: None,
                }),
            })
            .await
            .expect("cloud machine");
    }

    // The workspace's remembered specialist choice.
    store
        .set_setting(
            &format!("workspace.agentCreationPreferences:{}", big_ws.0),
            &json!({ "specialistId": "implementor" }).to_string(),
        )
        .await
        .expect("creation preferences");

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
/// once the daemon serves. Returns device connections the caller keeps open
/// for the whole sweep.
pub async fn runtime(
    client: &mut Client,
    services: &intent_services::Services,
    state: &LargeState,
    socket: &Path,
    seeded: &mut Seeded,
) -> Vec<Client> {
    let ws = state.ids.big_workspace.0.clone();

    // The big agent watches the completion of many delegated children.
    // Even-indexed fixture agents live in the big workspace. Each watch
    // persists and publishes, so register a few at a time: unbounded
    // concurrency exhausts the store's read pool at full scale.
    let mut watches: tokio::task::JoinSet<intent_core::Result<serde_json::Value>> =
        tokio::task::JoinSet::new();
    for child in state.ids.agents.iter().skip(2).step_by(2).take(WATCHES) {
        if watches.len() >= WATCH_CONCURRENCY {
            if let Some(done) = watches.join_next().await {
                done.expect("watch task").expect("completion watch");
            }
        }
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

    // Many hello'd devices (desktop, laptop, phone) connected at once.
    let mut devices = Vec::new();
    for i in 0..DEVICES {
        let mut device = Client::connect_socket(socket).await;
        let r = device
            .call(
                "client.hello",
                &json!({
                    "clientId": format!("large-state-device-{i:02}"),
                    "name": format!("Intent on device {i} — {}", prose(i, 60).replace('\n', " ")),
                    "hostname": format!("large-state-host-{i}"),
                    "prettyHostname": format!("Large state host {i}"),
                    "deviceKind": DEVICE_KINDS[i % DEVICE_KINDS.len()],
                }),
            )
            .await;
        expect_ok("client.hello", &r);
        devices.push(device);
    }

    // Provider profiles, the most each base provider allows.
    for base in ["codex", "claude-code"] {
        for i in 0..PROFILES_PER_PROVIDER {
            let r = client
                .call(
                    "providers.profiles.create",
                    &json!({ "baseProviderId": base, "name": format!("{base} account {i}") }),
                )
                .await;
            expect_ok("providers.profiles.create", &r);
        }
    }

    // Pending host invites, created by the owner.
    let owner = intent_core::Caller::Wire {
        principal_id: state.ids.owner_principal.clone(),
        host_role: intent_core::HostRole::Owner,
    };
    for i in 0..HOST_INVITES {
        intent_core::with_caller(
            owner.clone(),
            intent_core::WorkspaceApi::host_invite_create(
                services,
                intent_core::InvitePin {
                    login: format!("invitee-{i}"),
                    provider: Some("github".into()),
                    host: None,
                },
            ),
        )
        .await
        .expect("host invite");
    }

    // Every member principal is connected, viewing the big note and
    // focused on the big agent: one presence row per member.
    for (i, member) in state.ids.member_principals.iter().enumerate() {
        let connection = format!("large-state-presence-{i}");
        let caller = intent_core::Caller::Wire {
            principal_id: member.clone(),
            host_role: intent_core::HostRole::Guest,
        };
        let api: &dyn intent_core::WorkspaceApi = services;
        intent_core::with_caller(caller.clone(), api.presence_connect(connection.clone()))
            .await
            .expect("presence connect");
        intent_core::with_caller(
            caller.clone(),
            api.note_presence_join(
                connection.clone(),
                format!("lease-{i}"),
                state.ids.big_workspace.clone(),
                state.ids.big_note.clone(),
            ),
        )
        .await
        .expect("note presence join");
        intent_core::with_caller(
            caller,
            api.presence_update(
                connection,
                json!({ "focus": [{
                    "workspaceId": ws,
                    "agentId": state.ids.big_agent.0,
                    "noteId": state.ids.big_note.0,
                }] }),
            ),
        )
        .await
        .expect("presence update");
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
    // Note assets: one under the object read cap, one far over it.
    for (decoded, name) in [(300 * 1024, "mid.png"), (4 * 1024 * 1024, "large.png")] {
        use base64::Engine as _;
        let data = base64::engine::general_purpose::STANDARD.encode(vec![0x89_u8; decoded]);
        let r = client
            .call(
                "note.saveAsset",
                &json!({ "workspaceId": ws, "data": data, "mimeType": "image/png", "originalName": name }),
            )
            .await;
        expect_ok("note.saveAsset", &r);
        let url = r.result()["url"].as_str().expect("asset url").to_string();
        if name == "mid.png" {
            seeded.mid_asset = url;
        } else {
            seeded.large_asset = url;
        }
    }
    devices
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
