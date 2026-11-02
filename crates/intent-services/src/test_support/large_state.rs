//! A large, deterministic daemon state for payload-budget sweeps.
//!
//! [`build_large_state`] opens a fresh store, seeds every entity a read
//! method returns at worst-case scale (through the store and services APIs),
//! and wraps it in an isolated [`Services`]. Content is derived from a fixed
//! seed and fixed timestamps, so two builds with the same [`LargeStateScale`]
//! hold the same rows and bodies (store-minted row ids aside).
//!
//! Most of the volume lands in one "big" workspace, agent and note — the ids
//! in [`LargeStateIds`] — so per-workspace reads see the worst case. Runtime
//! state that lives only in memory (terminal buffers, presence, file locks) is
//! not seeded here.

use std::path::{Path, PathBuf};

use intent_core::discussion::DiscussionAuthor;
use intent_core::{
    ActorType, AgentId, AuthorType, Comment, CommentStatus, CommentType, ContentType, EventActor,
    Hook, HookId, HookState, MessageOrigin, Note, NoteId, NoteMetadata, NoteVersionAuthor,
    NoteVisibility, PrMonitor, PrMonitorId, PrMonitorState, Principal, PrincipalId, Script,
    ScriptMode, ScriptPurpose, TaskAgentLink, TaskMetadata, TaskStatus, Workspace, WorkspaceId,
    WorkspaceInvite, WorkspaceRole,
};
use intent_store::{
    AgentQueueRow, LocalStamp, NewEvent, NewTrackedChange, PersistedEventSubscription,
    ReplaceMessage, Store, UsageRateDelta, UsageStatsDelta,
};
use serde_json::{json, Value};

use super::fixtures::session;
use super::test_tempdir;
use crate::secret_requests::Outcome;
use crate::Services;

/// Fixed epoch every seeded timestamp is offset from (2026-01-01T00:00:00Z).
const EPOCH_SECS: i64 = 1_767_225_600;

/// Rows per `insert_events` call: 10 binds per row stays under `SQLite`'s
/// 32,766 bound-parameter limit.
const EVENT_CHUNK: usize = 2_000;

/// Env var selecting the preset [`LargeStateScale::from_env`] returns:
/// `ci` (the default) or `full`.
pub const SCALE_ENV: &str = "INTENTD_LARGE_STATE_SCALE";

/// Scale knobs: row counts and the byte sizes of the large text fields.
///
/// Two presets: [`LargeStateScale::ci`] (the per-PR default) keeps the build
/// fast while every unbounded list and every large body still exceeds 1 MiB
/// if returned whole; [`LargeStateScale::full`] holds the spec's worst-case
/// targets and runs nightly. Select one with [`SCALE_ENV`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LargeStateScale {
    pub workspaces: usize,
    /// Bytes of each workspace's status message (capped at the daemon's
    /// `WORKSPACE_STATUS_MESSAGE_MAX_LENGTH`).
    pub workspace_text_bytes: usize,
    pub agents: usize,
    /// Bytes of each agent's name (agent lists cap names at 128 bytes).
    pub agent_name_bytes: usize,
    /// Bytes of each finished agent's completion report (agent-written,
    /// unvalidated).
    pub completion_report_bytes: usize,
    /// Agents (the first ones, the big agent included) with long conversations.
    pub long_conversation_agents: usize,
    /// Messages in each long conversation.
    pub big_agent_messages: usize,
    /// Messages in every other agent's conversation.
    pub agent_messages: usize,
    /// Bytes of each seeded tool output.
    pub tool_output_bytes: usize,
    pub notes: usize,
    pub note_body_bytes: usize,
    /// Body of the big note.
    pub big_note_body_bytes: usize,
    /// Versions of the versioned note (beyond its initial version). The store
    /// keeps only the newest `MAX_NOTE_VERSIONS` (50), so a few more than that
    /// exercises pruning; the version list is bounded by design.
    pub versioned_note_versions: usize,
    /// Title bytes of the versioned note (each version summary carries it).
    pub version_title_bytes: usize,
    /// Versions of every other note (beyond its initial version).
    pub note_versions: usize,
    /// Task notes among `notes` (each linked to an agent).
    pub task_notes: usize,
    pub comment_threads: usize,
    pub replies_per_thread: usize,
    pub comment_bytes: usize,
    pub events: usize,
    pub event_data_bytes: usize,
    pub discussion_messages: usize,
    pub discussion_body_bytes: usize,
    /// Distinct principals reacting to each discussion message.
    pub reactions_per_message: usize,
    pub scripts: usize,
    pub script_command_bytes: usize,
    pub script_monitors: usize,
    pub hooks: usize,
    /// Bytes of each hook's code and of its last logs.
    pub hook_text_bytes: usize,
    pub pr_monitors: usize,
    /// Bytes of each PR monitor snapshot.
    pub pr_snapshot_bytes: usize,
    pub specialists: usize,
    pub skills: usize,
    pub skill_description_bytes: usize,
    pub rules: usize,
    /// Bytes of each specialist, skill and rule document body.
    pub file_doc_bytes: usize,
    pub tracked_changes: usize,
    pub queued_messages: usize,
    pub queued_message_bytes: usize,
    pub event_subscriptions: usize,
    pub members: usize,
    pub invites: usize,
    pub drafts: usize,
    pub draft_bytes: usize,
    pub usage_days: usize,
    pub usage_rate_minutes: usize,
    pub settings: usize,
    pub setting_value_bytes: usize,
    pub secrets: usize,
}

impl LargeStateScale {
    /// Per-PR preset: fast to build, yet every unbounded list and large body
    /// is over 1 MiB if returned whole (the `ci` unit test asserts it).
    #[must_use]
    pub fn ci() -> Self {
        Self {
            workspaces: 1_300,
            workspace_text_bytes: 450,
            agents: 1_100,
            agent_name_bytes: 48,
            completion_report_bytes: 2_000,
            long_conversation_agents: 2,
            big_agent_messages: 140,
            agent_messages: 1,
            tool_output_bytes: 16 * 1024,
            notes: 400,
            note_body_bytes: 6 * 1024,
            big_note_body_bytes: 1_200 * 1024,
            versioned_note_versions: 60,
            version_title_bytes: 100,
            note_versions: 0,
            task_notes: 120,
            comment_threads: 300,
            replies_per_thread: 3,
            comment_bytes: 1_000,
            events: 6_000,
            event_data_bytes: 400,
            discussion_messages: 200,
            discussion_body_bytes: 6 * 1024,
            reactions_per_message: 1,
            scripts: 700,
            script_command_bytes: 1_200,
            script_monitors: 200,
            hooks: 300,
            hook_text_bytes: 2 * 1024,
            pr_monitors: 300,
            pr_snapshot_bytes: 2 * 1024,
            specialists: 150,
            skills: 1_100,
            skill_description_bytes: 900,
            rules: 60,
            file_doc_bytes: 20 * 1024,
            tracked_changes: 2_600,
            queued_messages: 800,
            queued_message_bytes: 1_500,
            event_subscriptions: 3_000,
            members: 50,
            invites: 50,
            drafts: 400,
            draft_bytes: 3 * 1024,
            usage_days: 14,
            usage_rate_minutes: 24 * 60,
            settings: 200,
            setting_value_bytes: 200,
            secrets: 32,
        }
    }

    /// Nightly preset with the spec's worst-case targets (never below `ci`).
    #[must_use]
    pub fn full() -> Self {
        Self {
            agents: 1_500,
            long_conversation_agents: 3,
            big_agent_messages: 4_000,
            agent_messages: 12,
            notes: 3_000,
            note_versions: 2,
            task_notes: 600,
            comment_threads: 800,
            replies_per_thread: 4,
            events: 100_000,
            discussion_messages: 2_000,
            reactions_per_message: 4,
            tracked_changes: 9_000,
            usage_days: 120,
            usage_rate_minutes: 7 * 24 * 60,
            ..Self::ci()
        }
    }

    /// The preset named by [`SCALE_ENV`]: `ci` when unset.
    ///
    /// # Panics
    ///
    /// When the variable holds anything but `ci` or `full`.
    #[must_use]
    #[track_caller]
    pub fn from_env() -> Self {
        match std::env::var(SCALE_ENV).as_deref() {
            Err(std::env::VarError::NotPresent) | Ok("ci") => Self::ci(),
            Ok("full") => Self::full(),
            other => panic!("{SCALE_ENV} must be `ci` or `full`, got {other:?}"),
        }
    }
}

impl Default for LargeStateScale {
    fn default() -> Self {
        Self::ci()
    }
}

/// Ids the sweep needs to aim reads at the heaviest rows.
#[derive(Debug, Clone)]
pub struct LargeStateIds {
    pub big_workspace: WorkspaceId,
    pub workspaces: Vec<WorkspaceId>,
    pub big_agent: AgentId,
    pub agents: Vec<AgentId>,
    pub big_note: NoteId,
    /// Small body, many versions, long title.
    pub versioned_note: NoteId,
    pub task_note: NoteId,
    pub comment_thread: String,
    pub discussion_conversation: String,
    pub discussion_message: String,
    pub script: String,
    pub hook: HookId,
    pub pr_monitor: PrMonitorId,
    pub owner_principal: PrincipalId,
    pub member_principals: Vec<PrincipalId>,
}

/// Row counts the builder seeded, asserted by the unit test.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LargeStateCounts {
    pub workspaces: usize,
    pub agents: usize,
    pub agent_messages: usize,
    pub notes: usize,
    pub note_versions: usize,
    pub task_links: usize,
    pub comments: usize,
    pub events: usize,
    pub discussion_messages: usize,
    pub discussion_reactions: usize,
    pub scripts: usize,
    pub script_monitors: usize,
    pub hooks: usize,
    pub pr_monitors: usize,
    pub tracked_changes: usize,
    pub queued_messages: usize,
    pub event_subscriptions: usize,
    pub principals: usize,
    pub invites: usize,
    pub drafts: usize,
    pub usage_stats: usize,
    pub usage_rate: usize,
    pub settings: usize,
    pub secrets: usize,
    pub specialists: usize,
    pub skills: usize,
    pub rules: usize,
}

/// A seeded store plus the services over it. Keep it alive for the whole
/// test: dropping it removes the database and the on-disk fixture trees.
pub struct LargeState {
    pub services: Services,
    pub store: Store,
    pub ids: LargeStateIds,
    pub counts: LargeStateCounts,
    /// Wall-clock time spent seeding each entity kind, in seeding order.
    pub timings: Vec<(&'static str, std::time::Duration)>,
    /// The big workspace's worktree (skills and rules live here).
    pub worktree: PathBuf,
    /// User-tier specialist directory, already wired into `services`.
    pub specialists_dir: PathBuf,
    /// Root for workspace provisioning, already wired into `services`.
    pub workspaces_root: PathBuf,
    _dir: tempfile::TempDir,
}

/// Records how long each seeding phase took.
#[derive(Default)]
struct PhaseTimer {
    current: Option<(&'static str, std::time::Instant)>,
    done: Vec<(&'static str, std::time::Duration)>,
}

impl PhaseTimer {
    fn mark(&mut self, next: &'static str) {
        if let Some((name, started)) = self.current.take() {
            self.done.push((name, started.elapsed()));
        }
        self.current = Some((next, std::time::Instant::now()));
    }
}

/// Bytes of the shared prose corpus every seeded text is sliced from.
const CORPUS_BYTES: usize = 64 * 1024;

/// Deterministic `SplitMix64` stream plus a prose corpus built from it, so
/// large texts are cheap slices rather than word-by-word generation.
struct Rng {
    state: u64,
    corpus: String,
}

impl Rng {
    fn new(seed: u64) -> Self {
        const WORDS: [&str; 16] = [
            "agent",
            "workspace",
            "note",
            "daemon",
            "payload",
            "budget",
            "sweep",
            "fixture",
            "render",
            "stream",
            "commit",
            "branch",
            "review",
            "token",
            "event",
            "monitor",
        ];
        let mut rng = Self {
            state: seed,
            corpus: String::new(),
        };
        let mut corpus = String::with_capacity(CORPUS_BYTES + 16);
        while corpus.len() < CORPUS_BYTES {
            corpus.push_str(WORDS[rng.below(WORDS.len())]);
            corpus.push(if rng.below(12) == 0 { '\n' } else { ' ' });
        }
        corpus.truncate(CORPUS_BYTES);
        rng.corpus = corpus;
        rng
    }

    fn next(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: usize) -> usize {
        usize::try_from(self.next() % (n.max(1) as u64)).unwrap_or(0)
    }

    /// Prose-like ASCII text of exactly `bytes` bytes, sliced from the corpus
    /// at a seeded offset (wrapping when longer than the corpus).
    fn text(&mut self, bytes: usize) -> String {
        let mut out = String::with_capacity(bytes);
        let mut at = self.below(CORPUS_BYTES);
        while out.len() < bytes {
            let take = (bytes - out.len()).min(CORPUS_BYTES - at);
            out.push_str(&self.corpus[at..at + take]);
            at = 0;
        }
        out
    }
}

fn ts(offset_secs: usize) -> String {
    let secs = EPOCH_SECS + i64::try_from(offset_secs).unwrap_or(i64::MAX / 2);
    chrono::DateTime::from_timestamp(secs, 0)
        .expect("fixture timestamp in range")
        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

fn uuid_like(kind: u32, n: usize) -> String {
    format!("{kind:08x}-0000-4000-8000-{n:012x}")
}

/// Index of the workspace that owns item `i`: half of every kind lands in
/// the big workspace (index 0), the rest round-robins over the others. The
/// big and versioned notes and the long conversations always land in it.
fn owner(i: usize, workspaces: usize) -> usize {
    if workspaces <= 1 || i.is_multiple_of(2) {
        0
    } else {
        1 + (i / 2) % (workspaces - 1)
    }
}

/// Build the seeded state at `scale`.
///
/// # Panics
///
/// When any seeding call fails — a fixture bug, not a test outcome.
#[expect(
    clippy::too_many_lines,
    reason = "one linear seeding script reads best top to bottom"
)]
pub async fn build_large_state(scale: &LargeStateScale) -> LargeState {
    let dir = test_tempdir("intentd-large-state-");
    let store = Store::open(&dir.path().join("large.db"))
        .await
        .expect("open store");
    let worktree = dir.path().join("worktree");
    let workspaces_root = dir.path().join("workspaces");
    let specialists_dir = dir.path().join("specialists");
    for d in [&worktree, &workspaces_root, &specialists_dir] {
        std::fs::create_dir_all(d).expect("fixture dir");
    }
    let mut rng = Rng::new(0x1A26_E57A_7E00_0001);
    let mut counts = LargeStateCounts::default();
    let mut timer = PhaseTimer::default();

    // Workspaces.
    timer.mark("workspaces");
    let workspaces: Vec<WorkspaceId> = (0..scale.workspaces.max(1))
        .map(|i| WorkspaceId::from_string(uuid_like(0x0001, i)))
        .collect();
    for (i, id) in workspaces.iter().enumerate() {
        let ws = Workspace {
            id: id.clone(),
            title: format!("Workspace {i}: {}", rng.text(48).replace('\n', " ")),
            branch: format!("feature/large-state-{i}"),
            status_message: Some(
                rng.text(
                    scale
                        .workspace_text_bytes
                        .min(intent_core::WORKSPACE_STATUS_MESSAGE_MAX_LENGTH),
                )
                .replace('\n', " "),
            ),
            label: Some(format!("label-{}", i % 12)),
            base_ref: Some("origin/main".into()),
            repository_owner: Some("intent-hq".into()),
            repository_name: Some(format!("large-state-repo-{}", i % 40)),
            tags: (0..4).map(|t| format!("tag-{t}")).collect(),
            worktree_path: (i == 0).then(|| worktree.display().to_string()),
            created_at: ts(i),
            updated_at: ts(i + 1),
            ..Default::default()
        };
        store.insert_workspace(&ws).await.expect("insert workspace");
    }
    counts.workspaces = workspaces.len();
    let big_ws = workspaces[0].clone();

    // Principals and memberships.
    timer.mark("principals");
    let owner_principal = store
        .get_primary_principal()
        .await
        .expect("primary principal")
        .id;
    let mut member_principals = Vec::new();
    for i in 0..scale.members {
        let id = PrincipalId::from_string(uuid_like(0x0002, i));
        store
            .upsert_principal(&Principal {
                id: id.clone(),
                identity: None,
                github_user_id: Some(1_000_000 + i64::try_from(i).unwrap_or(0)),
                login: Some(format!("member-{i}")),
                display_name: Some(format!("Member {i}")),
                avatar_url: Some(format!("https://avatars.example/{i}.png")),
                is_primary: false,
                created_at: ts(i),
                updated_at: ts(i),
            })
            .await
            .expect("upsert principal");
        store
            .add_workspace_member(&big_ws, &id, WorkspaceRole::Collaborator)
            .await
            .expect("add member");
        member_principals.push(id);
    }
    counts.principals = member_principals.len();
    for i in 0..scale.invites {
        store
            .insert_workspace_invite(&WorkspaceInvite {
                id: uuid_like(0x0003, i),
                workspace_id: big_ws.clone(),
                secret_hash: format!("{i:064x}"),
                secret: None,
                created_by_principal_id: owner_principal.clone(),
                pin_identity: None,
                pin_github_user_id: None,
                pin_login: Some(format!("invitee-{i}")),
                created_at: ts(i),
                expires_at: ts(400 * 86_400 + i),
                redeemed_at: None,
                redeemed_by_principal_id: None,
                revoked_at: None,
                redemption_count: 0,
            })
            .await
            .expect("insert invite");
    }
    counts.invites = scale.invites;

    // Agents and conversations.
    timer.mark("agents");
    let agents: Vec<AgentId> = (0..scale.agents.max(1))
        .map(|i| AgentId::from_string(format!("agent-{}", uuid_like(0x0004, i))))
        .collect();
    for (i, agent) in agents.iter().enumerate() {
        let long = i < scale.long_conversation_agents.max(1);
        let ws = &workspaces[if long { 0 } else { owner(i, workspaces.len()) }];
        let mut s = session(agent, ws);
        s.name = format!(
            "Agent {i} {}",
            rng.text(scale.agent_name_bytes).replace('\n', " ")
        );
        s.created_at = ts(i);
        s.updated_at = ts(i + 1);
        s.system_prompt = (i == 0).then(|| rng.text(4 * 1024));
        if !long {
            // A finished delegated agent: its report is agent-written and unvalidated.
            s.parent_agent_id = (ws == &workspaces[0]).then(|| agents[0].clone());
            s.specialist = Some(["implementor", "verifier", "pr-reviewer"][i % 3].into());
            s.completion_report = Some(rng.text(scale.completion_report_bytes));
            s.completion_report_timestamp = Some(ts(i + 2));
        }
        let n = if long {
            scale.big_agent_messages
        } else {
            scale.agent_messages
        };
        let contents: Vec<(&str, Value, String)> = (0..n)
            .map(|m| {
                let at = ts(i * 10 + m);
                if m % 2 == 0 {
                    (
                        "user",
                        json!([{ "type": "text", "text": rng.text(512) }]),
                        at,
                    )
                } else {
                    let call = format!("t{m}");
                    (
                        "assistant",
                        json!([
                            { "type": "thinking", "text": rng.text(256) },
                            { "type": "text", "text": rng.text(1024) },
                            { "type": "tool_use", "id": format!("m{m}:2"), "name": "Run tests",
                              "input": { "command": rng.text(80) }, "toolCallId": call,
                              "metadata": { "toolKind": "terminal", "status": "completed" } },
                            { "type": "tool_result", "id": format!("m{m}:3"), "tool_use_id": call,
                              "output": rng.text(scale.tool_output_bytes), "is_error": false },
                            { "type": "text", "text": rng.text(512) },
                        ]),
                        at,
                    )
                }
            })
            .collect();
        let rows: Vec<ReplaceMessage<'_>> = contents
            .iter()
            .map(|(role, content, at)| ReplaceMessage {
                role,
                content,
                metadata: None,
                created_at: at,
            })
            .collect();
        store
            .insert_agent_session_with_messages(&s, &rows)
            .await
            .expect("insert agent");
        counts.agent_messages += n;
    }
    counts.agents = agents.len();
    let big_agent = agents[0].clone();

    // Notes, versions, task notes and task links.
    timer.mark("notes");
    let author = NoteVersionAuthor {
        id: big_agent.0.clone(),
        name: "Large state".into(),
        author_type: "agent".into(),
    };
    let notes: Vec<NoteId> = (0..scale.notes.max(3))
        .map(|i| NoteId::from_string(uuid_like(0x0005, i)))
        .collect();
    let task_note_count = scale.task_notes.min(notes.len() - 2).max(1);
    for (i, id) in notes.iter().enumerate() {
        let ws = &workspaces[if i < 2 { 0 } else { owner(i, workspaces.len()) }];
        let is_task = i >= 2 && i < 2 + task_note_count;
        let mut note = Note {
            id: id.clone(),
            workspace_id: ws.clone(),
            title: format!(
                "Note {i}: {}",
                rng.text(if i == 1 {
                    scale.version_title_bytes
                } else {
                    40
                })
                .replace('\n', " ")
            ),
            content: rng.text(if i == 0 {
                scale.big_note_body_bytes
            } else {
                scale.note_body_bytes
            }),
            content_type: ContentType::Markdown,
            tags: vec!["large".into(), format!("n{}", i % 7)],
            is_pinned: i % 50 == 0,
            is_archived: false,
            is_default: false,
            parent_id: is_task.then(|| notes[0].clone()).filter(|_| ws == &big_ws),
            visibility: NoteVisibility::Workspace,
            metadata: NoteMetadata {
                task: is_task.then(|| TaskMetadata {
                    status: TaskStatus::InProgress,
                    assigned_agent_ids: vec![agents[i % agents.len()].clone()],
                    acceptance_criteria: (0..5).map(|_| rng.text(120)).collect(),
                    ..Default::default()
                }),
            },
            created_at: ts(i),
            rev: 0,
            updated_at: ts(i),
        };
        store
            .insert_note_with_version(&note, &author, &ts(i))
            .await
            .expect("insert note");
        counts.notes += 1;
        counts.note_versions += 1;
        let versions = if i == 1 {
            scale.versioned_note_versions
        } else {
            scale.note_versions
        };
        for v in 0..versions {
            note.content.push_str(&rng.text(64));
            note.rev += 1;
            store
                .append_note_version(&note, &author, &ts(i + v), note.rev)
                .await
                .expect("append note version");
            counts.note_versions += 1;
        }
        if is_task {
            store
                .upsert_task_agent_link(&TaskAgentLink {
                    workspace_id: ws.clone(),
                    note_id: id.clone(),
                    task_key: format!("task-{i}"),
                    task_text: rng.text(80),
                    agent_id: agents[i % agents.len()].0.clone(),
                    created_at: i64::try_from(i).unwrap_or(0),
                })
                .await
                .expect("task link");
            counts.task_links += 1;
        }
    }
    let big_note = notes[0].clone();

    // Comment threads on the big note.
    timer.mark("comments");
    for t in 0..scale.comment_threads.max(1) {
        let thread = uuid_like(0x0006, t);
        for r in 0..=scale.replies_per_thread {
            let c = Comment {
                id: if r == 0 {
                    thread.clone()
                } else {
                    uuid_like(0x0007, t * 1_000 + r)
                },
                thread_id: thread.clone(),
                note_id: Some(big_note.clone()),
                kind: if r == 0 {
                    CommentType::Suggestion
                } else {
                    CommentType::Comment
                },
                content: rng.text(scale.comment_bytes),
                author: format!("member-{}", r % scale.members.max(1)),
                author_type: if r % 2 == 0 {
                    AuthorType::User
                } else {
                    AuthorType::Agent
                },
                author_principal_id: None,
                author_identity: None,
                status: CommentStatus::Open,
                parent_id: (r > 0).then(|| thread.clone()),
                anchor: None,
                anchor_text: (r == 0).then(|| rng.text(80)),
                anchor_before: None,
                anchor_after: None,
                suggestion_original: (r == 0).then(|| rng.text(200)),
                suggestion_proposed: (r == 0).then(|| rng.text(200)),
                agent_id: (r % 2 == 1).then(|| big_agent.clone()),
                is_orphaned: None,
                created_at: ts(t * 10 + r),
                updated_at: ts(t * 10 + r),
            };
            store
                .insert_comment(&big_ws, &c)
                .await
                .expect("insert comment");
            counts.comments += 1;
        }
    }

    // Events.
    timer.mark("events");
    let mut batch = Vec::with_capacity(EVENT_CHUNK);
    for i in 0..scale.events {
        let ws = &workspaces[owner(i, workspaces.len())];
        batch.push(NewEvent {
            workspace_id: ws.clone(),
            timestamp: ts(i),
            event_type: ["agent.message", "note.updated", "file.changed", "task.updated"][i % 4]
                .to_string(),
            actor: EventActor {
                actor_type: ActorType::Agent,
                id: Some(agents[i % agents.len()].0.clone()),
                name: Some(format!("Agent {}", i % agents.len())),
                ..Default::default()
            },
            session_id: Some(agents[i % agents.len()].0.clone()),
            correlation_id: Some(uuid_like(0x0008, i)),
            parent_event_id: None,
            metadata: Some(json!({ "seq": i })),
            data: json!({ "path": format!("src/file_{}.rs", i % 500), "summary": rng.text(scale.event_data_bytes) }),
        });
        if batch.len() == EVENT_CHUNK {
            counts.events += store.insert_events(&batch).await.expect("events").len();
            batch.clear();
        }
    }
    if !batch.is_empty() {
        counts.events += store.insert_events(&batch).await.expect("events").len();
    }

    // Discussion messages and reactions in the big workspace.
    timer.mark("discussions");
    let human = DiscussionAuthor {
        principal_id: Some(owner_principal.clone()),
        agent_id: None,
        login: Some("owner".into()),
        display_name: Some("Owner".into()),
        avatar_url: None,
    };
    let mut discussion_conversation = String::new();
    let mut discussion_message = String::new();
    let reactors: Vec<PrincipalId> = std::iter::once(owner_principal.clone())
        .chain(member_principals.iter().cloned())
        .take(scale.reactions_per_message.max(1))
        .collect();
    for i in 0..scale.discussion_messages.max(1) {
        let (msg, _) = store
            .insert_discussion_message(
                &big_ws,
                &uuid_like(0x0009, i),
                &rng.text(scale.discussion_body_bytes),
                &human,
            )
            .await
            .expect("discussion message");
        for (r, principal) in reactors.iter().enumerate() {
            store
                .set_discussion_reaction(
                    &big_ws,
                    &msg.conversation_id,
                    &msg.id,
                    principal,
                    intent_core::discussion::DISCUSSION_REACTIONS[r % 5],
                    true,
                )
                .await
                .expect("discussion reaction");
            counts.discussion_reactions += 1;
        }
        counts.discussion_messages += 1;
        discussion_conversation = msg.conversation_id;
        discussion_message = msg.id;
    }

    // Scripts and script monitors.
    timer.mark("scripts");
    let mut scripts = Vec::new();
    for i in 0..scale.scripts.max(1) {
        let id = uuid_like(0x000A, i);
        store
            .upsert_script(&Script {
                // Agents run many one-off commands; a third are saved scripts.
                purpose: if i % 3 == 0 {
                    ScriptPurpose::Saved
                } else {
                    ScriptPurpose::OneOff
                },
                archived_at: None,
                last_run: None,
                id: id.clone(),
                workspace_id: big_ws.0.clone(),
                name: format!("script-{i}"),
                // Agent-generated shell pipelines; the command is unvalidated.
                command: format!(
                    "make target-{i} {}",
                    rng.text(scale.script_command_bytes).replace('\n', " ")
                ),
                cwd: Some("packages/app".into()),
                env: Some((0..8).map(|e| (format!("VAR_{e}"), rng.text(40))).collect()),
                mode: if i % 3 == 0 {
                    ScriptMode::Service
                } else {
                    ScriptMode::Command
                },
                category: Some(format!("category-{}", i % 5)),
                source: "agent".into(),
                auto_start: Some(false),
                created_at: ts(i),
                updated_at: Some(ts(i)),
            })
            .await
            .expect("upsert script");
        scripts.push(id);
    }
    counts.scripts = scripts.len();
    for i in 0..scale.script_monitors {
        store
            .insert_script_monitor(&intent_core::ScriptMonitor {
                monitor_id: uuid_like(0x000B, i),
                workspace_id: big_ws.clone(),
                agent_id: big_agent.clone(),
                script_id: scripts[i % scripts.len()].clone(),
                run_id: uuid_like(0x000C, i),
                script_name: format!("script-{}", i % scripts.len()),
                mode: ScriptMode::Command,
                state: "active".into(),
                created_at: ts(i),
                expires_at: ts(400 * 86_400 + i),
                output_pattern: Some(rng.text(60).replace('\n', " ")),
                line_count: Some(200),
                settled_at: None,
                reason: None,
                result: None,
                trigger: None,
            })
            .await
            .expect("insert script monitor");
    }
    counts.script_monitors = scale.script_monitors;

    // Hooks.
    timer.mark("hooks");
    let mut first_hook = None;
    for i in 0..scale.hooks.max(1) {
        let id = HookId::from_string(uuid_like(0x000D, i));
        store
            .insert_hook(&Hook {
                hook_id: id.clone(),
                workspace_id: big_ws.clone(),
                agent_id: agents[i % agents.len()].clone(),
                name: format!("hook-{i}"),
                code: rng.text(scale.hook_text_bytes),
                delay_ms: 60_000,
                cron: None,
                run_at: None,
                state: HookState::Scheduled,
                created_at: ts(i),
                last_run_at: Some(ts(i + 60)),
                next_run_at: Some(ts(i + 120)),
                run_count: 3,
                last_error: None,
                last_logs: Some(rng.text(scale.hook_text_bytes)),
                last_state: Some(json!({ "cursor": rng.text(200) }).to_string()),
                expires_at: Some(ts(400 * 86_400 + i)),
                perpetual: i % 2 == 0,
                dispatch_count: 0,
            })
            .await
            .expect("insert hook");
        first_hook.get_or_insert(id);
    }
    counts.hooks = scale.hooks.max(1);

    // PR monitors.
    timer.mark("pr_monitors");
    let mut first_pr_monitor = None;
    for i in 0..scale.pr_monitors.max(1) {
        let id = PrMonitorId::from_string(uuid_like(0x000E, i));
        let snapshot = json!({
            "checks": rng.text(scale.pr_snapshot_bytes / 2),
            "reviews": rng.text(scale.pr_snapshot_bytes / 2),
        })
        .to_string();
        store
            .insert_pr_monitor(&PrMonitor {
                monitor_id: id.clone(),
                workspace_id: big_ws.clone(),
                agent_id: agents[i % agents.len()].clone(),
                repo_owner: "intent-hq".into(),
                repo_name: "large-state".into(),
                pr_number: i64::try_from(i).unwrap_or(0) + 1,
                state: PrMonitorState::Active,
                last_snapshot: Some(snapshot.clone()),
                baseline_snapshot: Some(snapshot),
                pending_changes: (0..5).map(|_| rng.text(120)).collect(),
                pending_since: Some(ts(i)),
                last_change_at: Some(ts(i)),
                last_polled_at: Some(ts(i)),
                last_error: None,
                created_at: ts(i),
                updated_at: ts(i),
            })
            .await
            .expect("insert pr monitor");
        first_pr_monitor.get_or_insert(id);
    }
    counts.pr_monitors = scale.pr_monitors.max(1);

    // File-tracking changes.
    timer.mark("tracked_changes");
    for i in 0..scale.tracked_changes {
        store
            .upsert_tracked_change(&NewTrackedChange {
                workspace_id: big_ws.clone(),
                path: format!(
                    "packages/app/src/{}/module_{}/component_{i}.tsx",
                    ["features", "components", "services", "routes"][i % 4],
                    i % 40
                ),
                stage: "unstaged".into(),
                status: "modified".into(),
                agent_id: Some(agents[i % agents.len()].0.clone()),
                session_id: None,
                turn: Some(1),
                commit_hash: None,
                old_blob_sha: Some(format!("{i:040x}")),
                new_blob_sha: Some(format!("{:040x}", i + 1)),
                additions: 10,
                deletions: 3,
            })
            .await
            .expect("tracked change");
    }
    counts.tracked_changes = scale.tracked_changes;
    store
        .upsert_workspace_metrics(&big_ws, 10_000, 3_000, 400)
        .await
        .expect("workspace metrics");
    for agent in agents.iter().take(200) {
        store
            .upsert_agent_metrics(&big_ws, &agent.0, 100, 30, 4)
            .await
            .expect("agent metrics");
    }

    // Event subscriptions.
    timer.mark("event_subscriptions");
    for i in 0..scale.event_subscriptions {
        store
            .upsert_event_subscription(&PersistedEventSubscription {
                id: uuid_like(0x000F, i),
                workspace_id: big_ws.clone(),
                subscriber_agent_id: agents[i % agents.len()].clone(),
                event_types: intent_core::events::ALL_EVENT_TYPES
                    .iter()
                    .cycle()
                    .skip(i % intent_core::events::ALL_EVENT_TYPES.len())
                    .take(8)
                    .map(|t| (*t).to_string())
                    .collect(),
                exclude_self: true,
                batch_window_ms: 2_000,
                created_at: ts(i),
            })
            .await
            .expect("event subscription");
    }
    counts.event_subscriptions = scale.event_subscriptions;

    // Drafts (one per agent, bounded by the agent count).
    timer.mark("drafts");
    let client = intent_core::ClientId::from_string("large-state-client");
    store
        .upsert_client(
            &client,
            Some("Large state client"),
            None,
            &intent_core::ClientHostInfo::default(),
        )
        .await
        .expect("client");
    for i in 0..scale.drafts.min(agents.len()) {
        let ws = &workspaces[owner(i, workspaces.len())];
        store
            .upsert_draft(ws, &agents[i], &client, &rng.text(scale.draft_bytes), None)
            .await
            .expect("draft");
        counts.drafts += 1;
    }

    // Usage stats (hourly per model) and the minutely rate history.
    timer.mark("usage");
    for day in 0..scale.usage_days {
        for hour in 0..24_usize {
            for model in ["model-a", "model-b", "model-c"] {
                let at = ts(day * 86_400 + hour * 3_600);
                store
                    .add_usage_stats(
                        &at,
                        model,
                        "provider",
                        Some(&LocalStamp {
                            date: at[..10].to_string(),
                            hour: u8::try_from(hour).unwrap_or(0),
                        }),
                        &UsageStatsDelta {
                            input_tokens: 1_000,
                            output_tokens: 500,
                            runs: 2,
                            sessions_started: 1,
                            ..Default::default()
                        },
                    )
                    .await
                    .expect("usage stats");
                counts.usage_stats += 1;
            }
        }
    }
    for minute in 0..scale.usage_rate_minutes {
        store
            .add_usage_rate(
                &ts(minute * 60),
                &UsageRateDelta {
                    input_tokens: 100,
                    output_tokens: 50,
                    ..Default::default()
                },
            )
            .await
            .expect("usage rate");
    }
    counts.usage_rate = scale.usage_rate_minutes;

    // Settings rows.
    timer.mark("settings");
    for i in 0..scale.settings {
        store
            .set_setting(
                &format!("largeState.key{i}"),
                &json!(rng.text(scale.setting_value_bytes)).to_string(),
            )
            .await
            .expect("setting");
    }
    counts.settings = scale.settings;

    // Specialists (user tier), skills and rules (big workspace worktree).
    timer.mark("files");
    // Specialist prompts, skill bodies and rule files are user-written and
    // unvalidated; skill descriptions stay under 1 KiB like real ones.
    for i in 0..scale.specialists {
        write(
            &specialists_dir.join(format!("large-specialist-{i}.md")),
            &format!(
                "---\nname: \"Large specialist {i}\"\ndescription: \"{}\"\n---\n\n{}",
                rng.text(200).replace(['\n', '"'], " "),
                rng.text(scale.file_doc_bytes)
            ),
        );
    }
    counts.specialists = scale.specialists;
    for i in 0..scale.skills {
        write(
            &worktree
                .join(".agents/skills")
                .join(format!("large-skill-{i}"))
                .join("SKILL.md"),
            &format!(
                "---\nname: large-skill-{i}\ndescription: {}\n---\n\n{}",
                rng.text(scale.skill_description_bytes).replace('\n', " "),
                rng.text(2 * 1024)
            ),
        );
    }
    counts.skills = scale.skills;
    for i in 0..scale.rules {
        write(
            &worktree.join(".intent/rules").join(format!("rule-{i}.md")),
            &rng.text(scale.file_doc_bytes),
        );
    }
    counts.rules = scale.rules;

    let services = Services::new_isolated(store.clone())
        .with_workspaces_root(workspaces_root.clone())
        .with_assets_root(dir.path().join("assets"))
        .with_specialist_dirs(Some(specialists_dir.clone()), None);

    // Queued messages (in memory and persisted) for the big agent.
    timer.mark("queue");
    let mut queue = Vec::new();
    for i in 0..scale.queued_messages {
        let (queued, position) = services.enqueue_message_with_id(
            &big_agent,
            Some(uuid_like(0x0010, i)),
            rng.text(scale.queued_message_bytes),
            None,
            None,
            None,
            None,
            false,
            MessageOrigin::User,
        );
        queue.push(AgentQueueRow {
            id: queued.id.clone(),
            agent_id: big_agent.clone(),
            position: i64::try_from(position).unwrap_or(0),
            payload: serde_json::to_value(&queued).expect("queued payload"),
            created_at: queued.queued_at.clone(),
            turn_id: queued.turn_id.clone(),
        });
    }
    store
        .replace_agent_queue(&big_agent, &queue)
        .await
        .expect("persist queue");
    counts.queued_messages = queue.len();

    // Secrets (names only; values never leave the vault).
    timer.mark("secrets");
    for i in 0..scale.secrets {
        let name = format!("LARGE_SECRET_{i}");
        let ticket = services
            .secret_vault
            .request(&big_ws, &big_agent, &name)
            .unwrap_or_else(|_| panic!("secret request {name}"));
        services.secret_vault.answer(
            &big_ws,
            &ticket.request_id,
            Outcome::Provided,
            Some(zeroize::Zeroizing::new(format!("value-{i}"))),
        );
    }
    counts.secrets = scale.secrets;

    timer.mark("done");
    LargeState {
        timings: timer.done,
        ids: LargeStateIds {
            big_workspace: big_ws,
            workspaces,
            big_agent,
            agents,
            big_note,
            versioned_note: notes[1].clone(),
            task_note: notes[2].clone(),
            comment_thread: uuid_like(0x0006, 0),
            discussion_conversation,
            discussion_message,
            script: scripts[0].clone(),
            hook: first_hook.expect("hook"),
            pr_monitor: first_pr_monitor.expect("pr monitor"),
            owner_principal,
            member_principals,
        },
        counts,
        services,
        store,
        worktree,
        specialists_dir,
        workspaces_root,
        _dir: dir,
    }
}

fn write(path: &Path, content: &str) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("fixture dir");
    }
    std::fs::write(path, content).expect("fixture file");
}

#[cfg(test)]
mod tests {
    use intent_core::WorkspaceListFilter;

    use super::*;

    const MIB: usize = 1024 * 1024;

    async fn count(store: &Store, sql: &str) -> usize {
        let n: i64 = sqlx::query_scalar(sql)
            .fetch_one(store.read_pool())
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
        usize::try_from(n).expect("non-negative count")
    }

    fn json_len<T: serde::Serialize>(value: &T) -> usize {
        serde_json::to_vec(value).map_or(0, |v| v.len())
    }

    fn dir_bytes(dir: &Path) -> usize {
        walk_bytes(dir)
    }

    fn walk_bytes(path: &Path) -> usize {
        if path.is_dir() {
            std::fs::read_dir(path).map_or(0, |entries| {
                entries.flatten().map(|e| walk_bytes(&e.path())).sum()
            })
        } else {
            std::fs::metadata(path)
                .ok()
                .and_then(|m| usize::try_from(m.len()).ok())
                .unwrap_or(0)
        }
    }

    /// A scale far smaller than `ci`, for the determinism check.
    fn tiny() -> LargeStateScale {
        LargeStateScale {
            workspaces: 2,
            agents: 3,
            big_agent_messages: 6,
            notes: 4,
            big_note_body_bytes: 4 * 1024,
            versioned_note_versions: 2,
            task_notes: 1,
            comment_threads: 1,
            events: 10,
            discussion_messages: 2,
            scripts: 1,
            script_monitors: 1,
            hooks: 1,
            pr_monitors: 1,
            specialists: 1,
            skills: 1,
            rules: 1,
            tracked_changes: 1,
            queued_messages: 1,
            event_subscriptions: 1,
            members: 1,
            invites: 1,
            drafts: 1,
            usage_days: 1,
            usage_rate_minutes: 1,
            settings: 1,
            secrets: 1,
            ..LargeStateScale::ci()
        }
    }

    /// The `ci` preset seeds every entity kind with exactly the requested row
    /// counts, and every unbounded list and large body is over 1 MiB whole.
    #[tokio::test]
    async fn large_state_fixture_ci_counts_and_sizes() {
        let scale = LargeStateScale::ci();
        let started = std::time::Instant::now();
        let state = build_large_state(&scale).await;
        eprintln!(
            "large_state_fixture ci build: {:?}\nphases: {:?}",
            started.elapsed(),
            state.timings
        );
        let c = &state.counts;
        let s = &state.store;
        let ids = &state.ids;

        // Row counts.
        assert_eq!(
            count(
                s,
                "SELECT COUNT(*) FROM workspace WHERE id LIKE '00000001-%'"
            )
            .await,
            scale.workspaces
        );
        assert_eq!(
            count(
                s,
                "SELECT COUNT(*) FROM agent_session WHERE id LIKE 'agent-00000004-%'"
            )
            .await,
            scale.agents
        );
        assert_eq!(
            count(
                s,
                "SELECT COUNT(*) FROM agent_message WHERE agent_id LIKE 'agent-00000004-%'"
            )
            .await,
            scale.long_conversation_agents * scale.big_agent_messages
                + (scale.agents - scale.long_conversation_agents) * scale.agent_messages
        );
        assert_eq!(
            count(s, "SELECT COUNT(*) FROM note WHERE id LIKE '00000005-%'").await,
            scale.notes
        );
        assert_eq!(
            c.note_versions,
            scale.notes + scale.versioned_note_versions + (scale.notes - 1) * scale.note_versions
        );
        assert_eq!(
            count(s, "SELECT COUNT(*) FROM task_agent_link").await,
            scale.task_notes
        );
        assert_eq!(
            count(s, "SELECT COUNT(*) FROM comment").await,
            scale.comment_threads * (1 + scale.replies_per_thread)
        );
        assert_eq!(c.events, scale.events);
        assert!(count(s, "SELECT COUNT(*) FROM event").await >= scale.events);
        assert_eq!(c.discussion_messages, scale.discussion_messages);
        assert_eq!(
            c.discussion_reactions,
            scale.discussion_messages * scale.reactions_per_message
        );
        assert_eq!(
            count(s, "SELECT COUNT(*) FROM script WHERE id LIKE '0000000a-%'").await,
            scale.scripts
        );
        assert_eq!(
            count(s, "SELECT COUNT(*) FROM script_monitor").await,
            scale.script_monitors
        );
        assert_eq!(
            count(
                s,
                "SELECT COUNT(*) FROM hook WHERE hook_id LIKE '0000000d-%'"
            )
            .await,
            scale.hooks
        );
        assert_eq!(
            count(
                s,
                "SELECT COUNT(*) FROM pr_monitor WHERE monitor_id LIKE '0000000e-%'"
            )
            .await,
            scale.pr_monitors
        );
        assert_eq!(
            count(s, "SELECT COUNT(*) FROM tracked_changes").await,
            scale.tracked_changes
        );
        assert_eq!(
            count(s, "SELECT COUNT(*) FROM event_subscription").await,
            scale.event_subscriptions
        );
        assert_eq!(
            count(s, "SELECT COUNT(*) FROM agent_queue").await,
            scale.queued_messages
        );
        assert_eq!(
            count(s, "SELECT COUNT(*) FROM workspace_invite").await,
            scale.invites
        );
        assert_eq!(count(s, "SELECT COUNT(*) FROM draft").await, scale.drafts);
        assert_eq!(c.usage_stats, scale.usage_days * 24 * 3);
        assert_eq!(
            count(s, "SELECT COUNT(*) FROM usage_rate_minutely").await,
            scale.usage_rate_minutes
        );
        assert_eq!(
            count(
                s,
                "SELECT COUNT(*) FROM settings WHERE key LIKE 'largeState.%'"
            )
            .await,
            scale.settings
        );
        let listing = state
            .services
            .secret_vault
            .list(&ids.big_workspace, &ids.big_agent)
            .to_json();
        assert_eq!(
            listing["secrets"].as_array().map_or(0, Vec::len),
            scale.secrets,
            "{listing}"
        );

        // Sizes: each unbounded list or large body is over 1 MiB whole.
        let workspaces = s
            .list_workspaces(WorkspaceListFilter::default())
            .await
            .expect("workspaces");
        let agents = s
            .list_agent_session_summaries(&ids.big_workspace)
            .await
            .expect("agents");
        let notes = s.list_notes(&ids.big_workspace).await.expect("notes");
        let big_note = s
            .get_note(&ids.big_workspace, &ids.big_note)
            .await
            .expect("big note");
        let versions = s
            .list_note_versions(&ids.big_workspace, &ids.versioned_note)
            .await
            .expect("versions");
        assert_eq!(versions.len(), 50, "versions are pruned to the newest 50");
        let comments = s.list_comments(&ids.big_note).await.expect("comments");
        let hooks = s
            .list_hooks_by_workspace(&ids.big_workspace)
            .await
            .expect("hooks");
        let monitors = s
            .list_pr_monitors_by_workspace(&ids.big_workspace)
            .await
            .expect("pr monitors");
        let big_messages = s
            .get_agent_messages(&ids.big_agent, None)
            .await
            .expect("big agent messages");
        let second_messages = s
            .get_agent_messages(&ids.agents[1], None)
            .await
            .expect("second agent messages");
        let skills = intent_core::with_caller(
            intent_core::Caller::Daemon,
            intent_core::WorkspaceApi::skill_list(&state.services, ids.big_workspace.clone()),
        )
        .await
        .expect("skill list");
        let ws = ids.big_workspace.as_str();
        let sum = |sql: String| async move { count(s, &sql).await };
        let sizes = [
            ("workspaces", json_len(&workspaces)),
            ("agents (big workspace)", json_len(&agents)),
            ("big agent messages", json_len(&big_messages)),
            ("second long conversation", json_len(&second_messages)),
            ("notes (big workspace)", json_len(&notes)),
            ("big note body", big_note.content.len()),
            ("comments (big note)", json_len(&comments)),
            (
                "events (big workspace)",
                sum(format!(
                    "SELECT SUM(LENGTH(json_object('id', id, 'workspaceId', workspace_id, \
                     'timestamp', timestamp, 'type', event_type, 'actor', json(actor), \
                     'sessionId', session_id, 'correlationId', correlation_id, \
                     'metadata', json(metadata_json), 'data', json(data_json)))) \
                     FROM event WHERE workspace_id='{ws}'"
                ))
                .await,
            ),
            (
                "discussion messages",
                sum(format!(
                    "SELECT SUM(LENGTH(body)) FROM workspace_discussion_message \
                     WHERE workspace_id='{ws}'"
                ))
                .await,
            ),
            (
                "scripts",
                sum(format!(
                    "SELECT SUM(LENGTH(json_object('id', id, 'workspaceId', workspace_id, \
                     'name', name, 'command', command, 'cwd', cwd, 'env', json(env), \
                     'mode', mode, 'category', category, 'source', source, \
                     'purpose', purpose, 'createdAt', created_at, 'updatedAt', updated_at))) \
                     FROM script WHERE workspace_id='{ws}'"
                ))
                .await,
            ),
            ("hooks", json_len(&hooks)),
            ("pr monitors", json_len(&monitors)),
            (
                "tracked changes",
                sum(format!(
                    "SELECT SUM(LENGTH(json_object('id', id, 'workspaceId', workspace_id, \
                     'path', path, 'stage', stage, 'status', status, 'agentId', agent_id, \
                     'sessionId', session_id, 'turn', turn, 'commitHash', commit_hash, \
                     'oldBlobSha', old_blob_sha, 'newBlobSha', new_blob_sha, \
                     'additions', additions, 'deletions', deletions, \
                     'createdAt', created_at, 'updatedAt', updated_at))) \
                     FROM tracked_changes WHERE workspace_id='{ws}'"
                ))
                .await,
            ),
            (
                "event subscriptions",
                sum(format!(
                    "SELECT SUM(LENGTH(json_object('id', id, 'workspaceId', workspace_id, \
                     'subscriberAgentId', subscriber_agent_id, \
                     'eventTypes', json(event_types), 'excludeSelf', exclude_self, \
                     'batchWindowMs', batch_window_ms, 'createdAt', created_at))) \
                     FROM event_subscription WHERE workspace_id='{ws}'"
                ))
                .await,
            ),
            (
                "queued messages",
                sum("SELECT SUM(LENGTH(payload)) FROM agent_queue".to_string()).await,
            ),
            (
                "drafts",
                sum("SELECT SUM(LENGTH(text)) FROM draft".to_string()).await,
            ),
            // Not sized: settings (a fixed registry) and note versions (pruned to
            // the newest 50 on append).
            ("skills (skill.list)", json_len(&skills)),
            ("specialists", dir_bytes(&state.specialists_dir)),
            ("rules", dir_bytes(&state.worktree.join(".intent/rules"))),
        ];
        eprintln!("large_state_fixture ci sizes: {sizes:?}");
        let small: Vec<_> = sizes.iter().filter(|(_, n)| *n <= MIB).collect();
        assert!(small.is_empty(), "not over 1 MiB: {small:?}");
    }

    /// Two builds at the same scale hold byte-identical seeded content.
    #[tokio::test]
    async fn large_state_fixture_is_deterministic() {
        let a = build_large_state(&tiny()).await;
        let b = build_large_state(&tiny()).await;
        assert_eq!(a.counts, b.counts);
        for st in [&a, &b] {
            assert_eq!(st.ids.big_note, a.ids.big_note);
        }
        let note = |st: &LargeState| {
            let store = st.store.clone();
            let ws = st.ids.big_workspace.clone();
            let id = st.ids.big_note.clone();
            async move { store.get_note(&ws, &id).await.expect("big note") }
        };
        let (na, nb) = (note(&a).await, note(&b).await);
        assert_eq!((na.title, na.content), (nb.title, nb.content));
        let messages = |st: &LargeState| {
            let store = st.store.clone();
            let agent = st.ids.big_agent.clone();
            async move {
                store
                    .get_agent_messages(&agent, None)
                    .await
                    .expect("messages")
                    .into_iter()
                    .map(|m| m.content)
                    .collect::<Vec<_>>()
            }
        };
        assert_eq!(messages(&a).await, messages(&b).await);
    }

    #[test]
    fn large_state_fixture_scale_env_selects_presets() {
        use crate::test_support::fixtures::EnvGuard;
        {
            let _env = EnvGuard::unset(SCALE_ENV);
            assert_eq!(LargeStateScale::from_env(), LargeStateScale::ci());
        }
        {
            let _env = EnvGuard::set_all(&[(SCALE_ENV, "full")]);
            assert_eq!(LargeStateScale::from_env(), LargeStateScale::full());
        }
        let _env = EnvGuard::set_all(&[(SCALE_ENV, "huge")]);
        let panic = std::panic::catch_unwind(LargeStateScale::from_env)
            .expect_err("unknown preset must panic");
        let message = panic.downcast_ref::<String>().cloned().unwrap_or_default();
        assert!(message.contains("`ci` or `full`"), "{message}");
    }

    /// Builds the preset named by `INTENTD_LARGE_STATE_SCALE` and reports the
    /// build time. Ignored by default; the nightly job runs it with `full`.
    #[tokio::test]
    #[ignore = "slow: nightly runs it with INTENTD_LARGE_STATE_SCALE=full"]
    async fn large_state_fixture_full_scale_build_time() {
        let scale = LargeStateScale::from_env();
        let started = std::time::Instant::now();
        let state = build_large_state(&scale).await;
        eprintln!(
            "large_state_fixture build ({}): {:?}\nphases: {:?}\ncounts: {:?}",
            if scale == LargeStateScale::full() {
                "full"
            } else {
                "ci"
            },
            started.elapsed(),
            state.timings,
            state.counts
        );
        assert_eq!(state.counts.events, scale.events);
    }
}
