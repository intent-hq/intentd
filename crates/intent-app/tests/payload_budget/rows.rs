//! One sweep row per `BoundedRead` method: the parameter variations to call
//! it with, and for each variation a proof that the reply reached the large
//! data (or, for a reply that is small by design, why).
//!
//! Adding a `BoundedRead` method to the catalog without a row here fails
//! `payload_budget_rows_cover_the_catalog`; so does a router param that no
//! variant sets (see `params_scan`). A method that cannot be exercised in
//! this harness goes in [`CANNOT_EXERCISE`] with its observed outcome and a
//! reason instead.

use serde_json::{json, Value};

use crate::seed::{self, Seeded};

/// Items a maximal page of a byte-budgeted forge read must reach: the stub
/// upstreams fill every item with ~16 KiB of text, so the daemon's 480,000-byte
/// page budget serves about 27 of them before it resumes inside the
/// upstream page.
const BUDGETED_PAGE_ITEMS: usize = 20;

/// How a variant proves it reached the large data.
#[derive(Debug, Clone)]
pub enum Proof {
    /// The array at this JSON pointer (into `result`) holds at least `min`
    /// items.
    Items(&'static str, usize),
    /// The result serializes to at least this many bytes.
    Bytes(usize),
    /// The reply is small by design (its row says why); the pointer must
    /// resolve to a non-null value.
    Fixed(&'static str),
    /// A bounded cut: the array at the first pointer holds at least `min`
    /// items and the second pointer (into `result`) is `true` — the
    /// `truncated` / `hasMore` marker a byte ceiling sets.
    Cut(&'static str, usize, &'static str),
    /// Like [`Proof::Cut`], but the marker is per row: at least one item of
    /// the array carries `true` at the second pointer (relative to the row).
    RowsCut(&'static str, usize, &'static str),
    /// Like [`Proof::Cut`], but the array also holds fewer than `below`
    /// items: with `below` set to the requested count, the page was cut by
    /// bytes, not by count or by the data running out.
    ByteCut(&'static str, usize, usize, &'static str),
}

/// Request params of one variant.
#[derive(Debug, Clone)]
pub enum Params {
    /// Sent as-is.
    Static(Value),
    /// Call with `first`, read the cursor at `token_at` (a pointer into the
    /// result), then call again with `first` plus `{ token_param: cursor }`
    /// (a `/`-separated path nests it, e.g. `page/continuationToken`). The
    /// second reply is the one measured.
    NextPage {
        first: Value,
        token_at: &'static str,
        token_param: &'static str,
    },
}

impl Params {
    /// Every param key this variant sets (top level, plus nested keys
    /// written as `outer.inner`).
    pub fn keys(&self) -> Vec<String> {
        fn walk(prefix: &str, v: &Value, out: &mut Vec<String>) {
            if let Some(obj) = v.as_object() {
                for (k, inner) in obj {
                    out.push(format!("{prefix}{k}"));
                    if prefix.is_empty() && inner.is_object() {
                        walk(&format!("{k}."), inner, out);
                    }
                }
            }
        }
        let mut out = Vec::new();
        match self {
            Self::Static(v) => walk("", v, &mut out),
            Self::NextPage {
                first, token_param, ..
            } => {
                walk("", first, &mut out);
                let mut parts = token_param.split('/');
                let head = parts.next().unwrap_or_default();
                out.push(head.to_string());
                if let Some(inner) = parts.next() {
                    out.push(format!("{head}.{inner}"));
                }
            }
        }
        out
    }
}

/// One parameter variation of a method.
#[derive(Debug, Clone)]
pub struct Variant {
    pub name: &'static str,
    pub params: Params,
    pub proof: Proof,
}

/// The sweep row of one `BoundedRead` method.
#[derive(Debug, Clone)]
pub struct Row {
    pub method: &'static str,
    pub variants: Vec<Variant>,
    /// Required when any variant proves with [`Proof::Fixed`]: why the
    /// reply cannot grow with daemon state.
    pub small_by_design: Option<&'static str>,
    /// Router params no variant sets, with the reason.
    pub unexercised: &'static [(&'static str, &'static str)],
    /// For an arm that hands its whole params object on (not a typed
    /// struct): the function that reads it, as `(path from crates/, fn,
    /// variable)`. Its literal key reads are derived from source.
    pub parse: Option<(&'static str, &'static str, &'static str)>,
    /// Keys that function reads through a variable, which the scanner
    /// cannot see, each with where it comes from.
    pub declared: &'static [(&'static str, &'static str)],
}

/// Golden: `BoundedRead` methods this harness cannot drive to real data,
/// with the outcome observed today (`ok` or an error code) and why. Each is
/// still called; a changed outcome fails, so the entry is revisited when
/// the method becomes reachable.
pub const CANNOT_EXERCISE: &[(&str, &str, &str)] = &[
    (
        "server.pairingInfo",
        "-32601",
        "Answered from the ServerPairingInfo the intentd composition root passes to its listeners; intent-app cannot build one. One fixed pairing record.",
    ),
    (
        "sourceControl.gitlab.childGit.get",
        "-32602",
        "Needs the native repository-authority wiring of the intentd composition root (\"Child Git unavailable\" here). One policy flag.",
    ),
    (
        "sourceControl.read.resolveKnownRepository",
        "-32602",
        "Needs a read lifetime captured through repository-native wiring the test services lack. One resolution record.",
    ),
    (
        "system.gitCredential",
        "-32601",
        "Answered from the SystemControl of the intentd composition root, which intent-app cannot build. One scoped credential.",
    ),
    (
        "system.startupStatus",
        "-32601",
        "Answered from the SystemControl of the intentd composition root, which intent-app cannot build. Fixed startup fields.",
    ),
    (
        "system.status",
        "-32601",
        "Answered from the SystemControl of the intentd composition root, which intent-app cannot build. Fixed status fields.",
    ),
    (
        "workspace.repositoryContext",
        "-32602",
        "Needs a repository lifetime from the native repository-authority wiring the test services lack. One context record.",
    ),
    (
        "agent.listActive",
        "ok",
        "Lists live ACP streams of the agent manager; running real agent processes is out of scope here.",
    ),
    (
        "agent.memoryUsage",
        "ok",
        "Needs the agent manager's process-tree sampler; without one the daemon answers the empty early return.",
    ),
    (
        "agent.pendingPermissions",
        "ok",
        "Permission prompts live in the agent manager's registry, filled only by live ACP sessions.",
    ),
    (
        "agent.getModels",
        "ok",
        "Model options come from the installed provider CLI's model probe; the test host has no provider CLI, so the list is empty.",
    ),
    (
        "agent.workers.list",
        "ok",
        "Background workers are reported only by a live ACP agent process (agent_workers is fed from its frames); without one the reply is the no-session projection.",
    ),
    (
        "desktop.getState",
        "-32602",
        "Needs a desktop client that sent client.hello with the desktop capability (Electron only).",
    ),
    (
        "invite.inspect",
        "-32601",
        "Served only to the guest invite handshake on the WSS listener, not on UDS.",
    ),
    (
        "models.list",
        "ok",
        "The model catalog comes from the provider's live model probe; the test host has no provider CLI, so the catalog is empty.",
    ),
    (
        "remoteLinkTraffic.snapshot",
        "ok",
        "Records exist only while the owner captures live executor-link frames, which the test host has none of; with capture off the reply is the empty buffer. Bounded by limit and by a 448 KiB cap on the records' JSON.",
    ),
    (
        "pairing.getInfo",
        "-32601",
        "Served only on the WSS pairing handshake, not on UDS.",
    ),
    (
        "pairing.getSelfInfo",
        "-32601",
        "Served only on the WSS pairing handshake, not on UDS.",
    ),
];

/// Golden: methods whose reply size depends on the live process, not on
/// daemon state, so it differs run to run. They are swept and reported but
/// kept out of the baseline ratchet (a size that moves with the test
/// runner's thread count cannot ratchet).
pub const VOLATILE: &[(&str, &str)] = &[(
    "debug.sampleStacks",
    "Sampled stacks of every daemon thread: size follows the live thread count of the test process.",
)];

/// Methods whose reply is a page of the event log; the sweep runs them
/// alone, before any concurrent read can append to it.
pub const EVENT_LOG_READS: &[&str] = &[
    "event.agentActivity",
    "event.query",
    "event.workspaceSummary",
    "search.events",
];

/// Ids the rows aim at.
pub struct Ctx<'a> {
    pub ws: String,
    pub agent: String,
    pub note: String,
    pub versioned_note: String,
    pub task_note: String,
    pub thread: String,
    pub conversation: String,
    pub script: String,
    /// A saved script whose `command` is over one reply (`script.get`).
    pub huge_script: String,
    pub monitor: String,
    pub worktree: String,
    pub specialist: String,
    pub message_id: String,
    /// An assistant message holding a large `tool_result` block, and the
    /// block's id (`agent.getMessageBlock`).
    pub tool_message_id: String,
    /// The assistant message of [`seed::BIG_BLOCK_AGENT`].
    pub big_block_message_id: String,
    pub tool_block_id: String,
    /// A member principal with live presence (seeded).
    pub member_principal: String,
    pub seeded: &'a Seeded,
}

fn v(name: &'static str, params: Value, proof: Proof) -> Variant {
    Variant {
        name,
        params: Params::Static(params),
        proof,
    }
}

fn next(
    name: &'static str,
    first: Value,
    token_at: &'static str,
    token_param: &'static str,
    proof: Proof,
) -> Variant {
    Variant {
        name,
        params: Params::NextPage {
            first,
            token_at,
            token_param,
        },
        proof,
    }
}

fn row(method: &'static str, variants: Vec<Variant>) -> Row {
    Row {
        method,
        variants,
        small_by_design: None,
        unexercised: &[],
        parse: None,
        declared: &[],
    }
}

fn fixed(method: &'static str, why: &'static str, variants: Vec<Variant>) -> Row {
    Row {
        small_by_design: Some(why),
        ..row(method, variants)
    }
}

/// `ScriptListOptions::parse`, which `script.list` and `scriptMonitor.list`
/// hand their params to.
const SCRIPT_LIST_PARSE: (&str, &str, &str) = ("intent-core/src/script_lists.rs", "parse", "value");

/// `remote_scripts::machine_run_call`, which `script.*` hands its params to
/// when they carry `remoteAgentId` (a remote agent's machine run).
const MACHINE_RUN_PARSE: (&str, &str, &str) = (
    "intent-svc-runtime/src/remote_scripts.rs",
    "machine_run_call",
    "params",
);

/// The seeded daemon has no remote machines, so no machine run to address.
const MACHINE_RUN_UNEXERCISED: &[(&str, &str)] = &[(
    "remoteAgentId",
    "Addresses a remote agent's machine run (remoteScripts: 1); the seeded daemon has no remote machine. Its replies use the home run's bounded-output helpers, covered above 480,000 escaped bytes by machine_runs::tests::machine_run_output_replies_are_bounded.",
)];

impl Row {
    fn parsed_by(
        mut self,
        parse: (&'static str, &'static str, &'static str),
        declared: &'static [(&'static str, &'static str)],
    ) -> Self {
        self.parse = Some(parse);
        self.declared = declared;
        self
    }

    fn skip(mut self, unexercised: &'static [(&'static str, &'static str)]) -> Self {
        self.unexercised = unexercised;
        self
    }
}

/// Every sweep row, sorted by method.
#[expect(clippy::too_many_lines, reason = "one flat table reads best")]
pub fn rows(c: &Ctx<'_>) -> Vec<Row> {
    use Proof::{ByteCut, Bytes, Cut, Fixed, Items, RowsCut};
    let ws = c.ws.as_str();
    let agent = c.agent.as_str();
    let note = c.note.as_str();
    let s = c.seeded;
    let w = json!({ "workspaceId": ws });
    let wa = json!({ "workspaceId": ws, "agentId": agent });
    let wn = json!({ "workspaceId": ws, "noteId": note });
    vec![
        fixed(
            "accept-changes.getStatus",
            "Branch counters for one workspace.",
            vec![v("default", w.clone(), Fixed("/branch"))],
        ),
        row(
            "agent.diagnostics",
            vec![
                v("workspace", w.clone(), Bytes(64 * 1024)),
                v(
                    "limit=200",
                    json!({ "workspaceId": ws, "limit": 200 }),
                    Bytes(64 * 1024),
                ),
                v(
                    "max",
                    json!({ "workspaceId": ws, "agentId": agent, "taskNoteId": c.task_note, "staleRespondingAfterMs": 1, "limit": 200 }),
                    Bytes(1024),
                ),
            ],
        ),
        row(
            "agent.get",
            vec![
                v("default", json!({ "agentId": agent }), Bytes(8 * 1024)),
                v("workspace", wa.clone(), Bytes(8 * 1024)),
            ],
        ),
        row(
            "agent.getConversation",
            vec![
                v("default", wa.clone(), Items("/messages", 20)),
                v(
                    "limit=10000",
                    json!({ "workspaceId": ws, "agentId": agent, "limit": 10_000 }),
                    Items("/messages", 100),
                ),
                v(
                    "max",
                    json!({ "workspaceId": ws, "agentId": agent, "limit": 10_000, "projection": "slim", "includeInProgress": true }),
                    Items("/messages", 100),
                ),
                v(
                    "aroundMessageId",
                    json!({ "agentId": agent, "aroundMessageId": c.message_id, "limit": 10_000 }),
                    Items("/messages", 50),
                ),
                v(
                    "aroundIndex",
                    json!({ "agentId": agent, "aroundIndex": 70, "limit": 10_000 }),
                    Items("/messages", 50),
                ),
                next(
                    "nextToken",
                    json!({ "agentId": agent, "limit": 40 }),
                    "/nextToken",
                    "nextToken",
                    Items("/messages", 40),
                ),
            ],
        ),
        fixed(
            "agent.getCreationPreferences",
            "One workspace's specialist preference.",
            vec![v("default", w.clone(), Fixed("/specialistId"))],
        ),
        {
            let block = |extra: Value| {
                let mut p = json!({
                    "agentId": seed::BIG_BLOCK_AGENT,
                    "messageId": c.big_block_message_id,
                    "blockId": seed::BIG_BLOCK_ID,
                });
                for (k, val) in extra.as_object().expect("object") {
                    p[k] = val.clone();
                }
                p
            };
            row(
                "agent.getMessageBlock",
                vec![
                    v("default", block(json!({ "workspaceId": ws })), Bytes(400_000)),
                    v("maxBytes=100000", block(json!({ "maxBytes": 100_000 })), Bytes(90_000)),
                    v(
                        "maxBytes=10000000",
                        block(json!({ "maxBytes": 10_000_000 })),
                        Bytes(400_000),
                    ),
                    v("offset", block(json!({ "offset": 1_000_000 })), Bytes(400_000)),
                    next(
                        "nextOffset",
                        block(json!({})),
                        "/nextOffset",
                        "offset",
                        Bytes(400_000),
                    ),
                    v(
                        "unpaged",
                        json!({ "agentId": agent, "messageId": c.tool_message_id, "blockId": c.tool_block_id }),
                        Bytes(8 * 1024),
                    ),
                ],
            )
        },
        row(
            "agent.getQueue",
            vec![
                v("default", json!({ "agentId": agent }), Items("/queue", 50)),
                v("workspace", wa.clone(), Items("/queue", 50)),
                v(
                    "limit=200",
                    json!({ "agentId": agent, "limit": 200 }),
                    Items("/queue", 200),
                ),
                next(
                    "nextToken",
                    json!({ "agentId": agent, "limit": 100 }),
                    "/nextToken",
                    "nextToken",
                    Items("/queue", 100),
                ),
            ],
        ),
        row(
            "agent.getSession",
            vec![
                v("default", json!({ "agentId": agent }), Bytes(64 * 1024)),
                v("workspace", wa.clone(), Bytes(64 * 1024)),
            ],
        ),
        fixed(
            "agent.getSessionStats",
            "Token and turn counters of one session.",
            vec![
                v("default", json!({ "sessionId": agent }), Fixed("")),
                v("workspace", json!({ "sessionId": agent, "workspaceId": ws }), Fixed("")),
            ],
        ),
        row(
            "agent.getSubscriptions",
            vec![v("default", wa.clone(), Bytes(64 * 1024))],
        ),
        row(
            "agent.list",
            vec![
                v("default", w.clone(), Items("/agents", 100)),
                // 200-row pages can stop at the 448 KiB page budget first.
                v(
                    "limit=200",
                    json!({ "workspaceId": ws, "limit": 200 }),
                    Bytes(300 * 1024),
                ),
                next(
                    "nextToken",
                    json!({ "workspaceId": ws, "limit": 200 }),
                    "/nextToken",
                    "nextToken",
                    Bytes(300 * 1024),
                ),
                v(
                    "includeRetired",
                    json!({ "workspaceId": ws, "includeRetired": true, "limit": 200 }),
                    Bytes(300 * 1024),
                ),
                v(
                    "retiredOnly",
                    json!({ "workspaceId": ws, "retiredOnly": true }),
                    Items("/agents", 100),
                ),
                v(
                    "scope=delegated",
                    json!({ "workspaceId": ws, "scope": "delegated", "parentAgentId": agent, "limit": 200 }),
                    Bytes(300 * 1024),
                ),
                v(
                    "scope=delegated+orphanedOnly",
                    json!({ "workspaceId": ws, "scope": "delegated", "orphanedOnly": true }),
                    Items("/agents", 100),
                ),
                v(
                    "scope=topLevel",
                    json!({ "workspaceId": ws, "scope": "topLevel" }),
                    Items("/agents", 100),
                ),
            ],
        ),
        row(
            "agent.listInterrupted",
            vec![v("default", json!({}), Items("/agents", 1_000))],
        ),
        row(
            "agent.listUserMessages",
            vec![
                v("default", json!({ "agentId": agent }), Items("/items", 50)),
                v(
                    "max",
                    json!({ "agentId": agent, "workspaceId": ws, "limit": 10_000, "previewChars": 100_000 }),
                    Items("/items", 70),
                ),
                next(
                    "nextToken",
                    json!({ "agentId": agent, "limit": 20 }),
                    "/nextToken",
                    "nextToken",
                    Items("/items", 20),
                ),
            ],
        ),
        row(
            "agent.summary",
            vec![v("default", wa.clone(), Bytes(1024))],
        )
        .with_reason("A fixed-shape digest: counters plus the last message preview."),
        row(
            "browser.listTabs",
            vec![
                v("default", w.clone(), Items("/tabs", seed::BROWSER_TABS)),
            ],
        ),
        fixed(
            "client.list",
            "One row per live authenticated connection; seeded with many connected devices.",
            vec![v("default", json!({}), Items("/clients", seed::DEVICES))],
        ),
        row(
            "comment.getThread",
            vec![
                v(
                    "threadId",
                    json!({ "workspaceId": ws, "noteId": note, "threadId": c.thread }),
                    Items("/replies", 3),
                ),
                v(
                    "commentId",
                    json!({ "workspaceId": ws, "noteId": note, "commentId": c.thread }),
                    Items("/replies", 3),
                ),
            ],
        ),
        row(
            "comment.list",
            vec![
                v("default", wn.clone(), Items("/threads", 100)),
                v(
                    "max",
                    json!({ "workspaceId": ws, "noteId": note, "includeComments": true, "status": "open" }),
                    Items("/threads", 100),
                ),
                v(
                    "since",
                    json!({ "workspaceId": ws, "noteId": note, "since": "2000-01-01T00:00:00.000Z" }),
                    Items("/threads", 100),
                ),
                v(
                    "authorType=user",
                    json!({ "workspaceId": ws, "noteId": note, "authorType": "user" }),
                    Items("/threads", 100),
                ),
                v(
                    "authorType=agent",
                    json!({ "workspaceId": ws, "noteId": note, "authorType": "agent" }),
                    Items("/threads", 100),
                ),
                v(
                    "limit",
                    json!({ "workspaceId": ws, "noteId": note, "includeComments": true, "limit": 100_000 }),
                    Bytes(400 * 1024),
                ),
                next(
                    "nextToken",
                    json!({ "workspaceId": ws, "noteId": note, "includeComments": true, "limit": 50 }),
                    "/nextToken",
                    "nextToken",
                    Items("/threads", 50),
                ),
            ],
        ),
        row(
            "crossWorkspace.listNotes",
            vec![v(
                "default",
                json!({ "workspaceId": ws, "targetWorkspaceId": ws }),
                Items("", 200),
            )],
        ),
        row(
            "crossWorkspace.listSiblings",
            vec![
                v("default", w.clone(), Items("", 20)),
                v(
                    "includeRetired",
                    json!({ "workspaceId": ws, "includeRetired": true }),
                    Items("", 20),
                ),
            ],
        ),
        row(
            "crossWorkspace.readNote",
            vec![
                v(
                    "default",
                    json!({ "workspaceId": ws, "targetWorkspaceId": ws, "noteId": note }),
                    Bytes(400 * 1024),
                ),
                v(
                    "window",
                    json!({ "workspaceId": ws, "targetWorkspaceId": ws, "noteId": note, "startLine": 2, "maxLines": 100_000 }),
                    Bytes(400 * 1024),
                ),
                v(
                    "startOffset",
                    json!({ "workspaceId": ws, "targetWorkspaceId": ws, "noteId": note, "startLine": 1, "startOffset": 1 }),
                    Bytes(400 * 1024),
                ),
            ],
        ),
        fixed(
            "debug.sampleStacks",
            "A sampled stack report; bounded by thread count and sample count, both clamped.",
            vec![
                v("default", json!({ "durationMs": 50 }), Fixed("/report")),
                v(
                    "max",
                    json!({ "durationMs": 200, "frequencyHz": 1_000 }),
                    Fixed("/report"),
                ),
            ],
        ),
        row(
            "discussion.conversations",
            vec![
                v("default", w.clone(), Items("/conversations", 50)),
                v(
                    "limit=100",
                    json!({ "workspaceId": ws, "limit": 100 }),
                    Items("/conversations", 100),
                ),
                next(
                    "beforeId",
                    json!({ "workspaceId": ws, "limit": 50 }),
                    "/conversations/49/id",
                    "beforeId",
                    Items("/conversations", 50),
                ),
            ],
        )
        .skip(&[
            ("query", "Read only by discussion.recipients (shared router arm)."),
            ("afterPrincipalId", "Read only by discussion.recipients (shared router arm)."),
        ]),
        row(
            "discussion.list",
            vec![
                v("default", w.clone(), Cut("/messages", 50, "/hasMore")),
                v(
                    "limit=100",
                    json!({ "workspaceId": ws, "conversationId": c.conversation, "limit": 100 }),
                    ByteCut("/messages", 50, 100, "/hasMore"),
                ),
                v(
                    "beforeSequence",
                    json!({ "workspaceId": ws, "limit": 100, "beforeSequence": 150 }),
                    ByteCut("/messages", 50, 100, "/hasMore"),
                ),
                v(
                    "afterSequence",
                    json!({ "workspaceId": ws, "limit": 100, "afterSequence": 0 }),
                    ByteCut("/messages", 50, 100, "/hasMore"),
                ),
            ],
        ),
        row(
            "discussion.recipients",
            vec![
                v("default", w.clone(), Items("/members", 40)),
                v(
                    "limit=100",
                    json!({ "workspaceId": ws, "limit": 100, "query": "" }),
                    Items("/members", 50),
                ),
                next(
                    "query+afterPrincipalId",
                    json!({ "workspaceId": ws, "limit": 20, "query": "member" }),
                    "/members/19/principalId",
                    "afterPrincipalId",
                    Items("/members", 20),
                ),
            ],
        )
        .skip(&[("beforeId", "Read only by discussion.conversations (shared router arm).")]),
        row(
            "drafts.get",
            vec![v("default", wa.clone(), Bytes(1024))],
        )
        .with_reason("One composer draft per (workspace, agent, client)."),
        row(
            "event.agentActivity",
            vec![
                v(
                    "default",
                    json!({ "workspaceId": ws, "minutesAgo": 100_000_000 }),
                    Items("", 500),
                ),
                v(
                    "agent",
                    json!({ "workspaceId": ws, "agentId": agent, "minutesAgo": 100_000_000 }),
                    Items("", 100),
                ),
            ],
        ),
        row(
            "event.query",
            vec![
                v("default", w.clone(), RowsCut("", 50, "/truncated")),
                v(
                    "limit=100000",
                    json!({ "workspaceId": ws, "limit": 100_000 }),
                    RowsCut("", 500, "/truncated"),
                ),
                v(
                    "filters",
                    json!({ "workspaceId": ws, "limit": 100_000, "eventType": intent_core::events::FILE_CHANGED, "actorType": "agent", "actorId": agent, "path": seed::HOT_FILE, "minutesAgo": 100_000_000 }),
                    Items("", 200),
                ),
                v(
                    "paginate",
                    json!({ "workspaceId": ws, "limit": 100_000, "paginate": true }),
                    Items("/items", 100),
                ),
                next(
                    "nextToken",
                    json!({ "workspaceId": ws, "limit": 100_000, "paginate": true }),
                    "/nextToken",
                    "nextToken",
                    Items("/items", 100),
                ),
            ],
        ),
        row(
            "event.workspaceSummary",
            vec![
                v("default", w.clone(), Items("/recentFiles", 1)),
                v(
                    "minutesAgo",
                    json!({ "workspaceId": ws, "minutesAgo": 100_000_000 }),
                    Bytes(32 * 1024),
                ),
            ],
        ),
        row(
            "executor.list",
            vec![v("default", json!({}), Items("/backends", seed::EXECUTOR_BACKENDS))],
        ),
        row(
            "file-tracking.getAgentLocks",
            vec![v("default", w.clone(), Items("/lockedFilePaths", 100))],
        ),
        row(
            "file-tracking.getChanges",
            vec![
                v("default", w.clone(), Items("/changes", 500)),
                v(
                    "filter",
                    json!({ "workspaceId": ws, "filter": { "agentId": agent } }),
                    Items("/changes", 200),
                ),
            ],
        ),
        row(
            "file-tracking.loadCommits",
            vec![
                v("default", w.clone(), Items("/commits", 50)),
                v(
                    "limit=100000",
                    json!({ "workspaceId": ws, "limit": 100_000 }),
                    Items("/commits", seed::BOUNDARY_DEPTH),
                ),
                v(
                    "includeOlder",
                    json!({ "workspaceId": ws, "limit": 100_000, "includeOlder": true }),
                    Items("/commits", seed::BOUNDARY_DEPTH),
                ),
                next(
                    "nextToken",
                    json!({ "workspaceId": ws, "limit": 50 }),
                    "/nextToken",
                    "nextToken",
                    Items("/commits", 50),
                ),
            ],
        ),
        fixed(
            "file.exists",
            "Three flags for one path.",
            vec![v("default", json!({ "workspaceId": ws, "path": "." }), Fixed("/exists"))],
        ),
        fixed(
            "file.getAttachmentInfo",
            "Metadata of one attachment.",
            vec![
                v("attachmentId", json!({ "attachmentId": s.attachment }), Fixed("")),
                v(
                    "idempotencyKey",
                    json!({ "workspaceId": ws, "idempotencyKey": seed::ATTACHMENT_KEY }),
                    Fixed(""),
                ),
            ],
        ),
        row(
            "file.list",
            vec![
                v("default", w.clone(), Items("", 5)),
                v(
                    "path",
                    json!({ "workspaceId": ws, "path": ".agents/skills" }),
                    Items("", 1_000),
                ),
            ],
        )
        .with_reason("The worktree root holds a handful of entries; the skills directory holds over a thousand."),
        row(
            "file.read",
            vec![
                v(
                    "default",
                    json!({ "workspaceId": ws, "path": seed::MID_FILE }),
                    Bytes(380_000),
                ),
                v(
                    "maxBytes",
                    json!({ "workspaceId": ws, "path": seed::FULL_FILE, "maxBytes": 100_000_000 }),
                    Bytes(470_000),
                ),
                v(
                    "gitRootId",
                    json!({ "workspaceId": ws, "path": seed::FULL_FILE, "gitRootId": s.git_root, "maxBytes": 480_000 }),
                    Bytes(470_000),
                ),
            ],
        ),
        fixed(
            "file.stat",
            "Metadata of one path.",
            vec![v("default", json!({ "workspaceId": ws, "path": "." }), Fixed("/size"))],
        ),
        row(
            "file.tree",
            vec![
                v("default", w.clone(), Items("", 5)),
                v(
                    "path",
                    json!({ "workspaceId": ws, "path": ".agents/skills" }),
                    Items("", 1_000),
                ),
            ],
        )
        .with_reason("The worktree root holds a handful of entries; the skills directory holds over a thousand."),
        row(
            "git.branchDiff",
            vec![
                v(
                    "baseRef",
                    json!({ "workspaceId": ws, "baseRef": s.base_branch }),
                    Bytes(256 * 1024),
                ),
                v(
                    "baseCommitSha",
                    json!({ "workspaceId": ws, "baseCommitSha": s.mid_commit, "targetRef": "main" }),
                    Bytes(128 * 1024),
                ),
                v(
                    "paths",
                    json!({ "workspaceId": ws, "baseRef": s.base_branch, "paths": ["src/history/module_1.rs"] }),
                    Bytes(1024),
                ),
                v(
                    "maxBytes",
                    json!({ "workspaceId": ws, "baseRef": s.base_branch, "maxBytes": 1_000_000 }),
                    Bytes(256 * 1024),
                ),
                next(
                    "nextToken",
                    json!({ "workspaceId": ws, "baseRef": s.base_branch, "maxBytes": 65_536 }),
                    "/-/nextToken",
                    "nextToken",
                    Bytes(16 * 1024),
                ),
            ],
        ),
        fixed(
            "git.branchStatus",
            "Ahead/behind counters of one branch.",
            vec![
                v(
                    "repoPath",
                    json!({ "repoPath": c.worktree, "branchName": "main" }),
                    Fixed("/ahead"),
                ),
                v(
                    "gitRootId",
                    json!({ "workspaceId": ws, "gitRootId": s.git_root, "branchName": "main" }),
                    Fixed("/ahead"),
                ),
            ],
        ),
        row(
            "git.changes",
            vec![
                v("default", w.clone(), Items("", 1_000)),
                v(
                    "gitRootId",
                    json!({ "workspaceId": ws, "gitRootId": s.git_root }),
                    Items("", 1_000),
                ),
            ],
        ),
        fixed(
            "git.checkMergeConflicts",
            "A conflict verdict plus conflicting paths of one merge probe.",
            vec![
                v("default", w.clone(), Fixed("")),
                v(
                    "targetBranch",
                    json!({ "workspaceId": ws, "targetBranch": s.base_branch }),
                    Fixed(""),
                ),
            ],
        ),
        row(
            "git.commitDetails",
            vec![
                v(
                    "default",
                    json!({ "workspaceId": ws, "commitHash": s.mid_commit }),
                    Fixed("/message"),
                ),
                v(
                    "gitRootId",
                    json!({ "workspaceId": ws, "commitHash": "HEAD", "gitRootId": s.git_root }),
                    Fixed("/message"),
                ),
            ],
        )
        .with_reason("One commit: its headline and changed files."),
        row(
            "git.commits",
            vec![
                v("default", w.clone(), Items("/items", 50)),
                v(
                    "limit=100000",
                    json!({ "workspaceId": ws, "limit": 100_000 }),
                    Items("/items", 200),
                ),
                v(
                    "page",
                    json!({ "workspaceId": ws, "page": { "limit": 100_000 }, "gitRootId": s.git_root }),
                    Items("/items", 200),
                ),
                next(
                    "nextToken",
                    json!({ "workspaceId": ws, "limit": 100 }),
                    "/nextToken",
                    "nextToken",
                    Items("/items", 100),
                ),
                next(
                    "page.continuationToken",
                    json!({ "workspaceId": ws, "page": { "limit": 100 } }),
                    "/nextToken",
                    "page/continuationToken",
                    Items("/items", 100),
                ),
            ],
        ),
        row(
            "git.diffs",
            vec![
                v("default", w.clone(), Items("", 1_000)),
                v(
                    "maxBytes",
                    json!({ "workspaceId": ws, "maxBytes": 1_000_000 }),
                    Items("", 1_000),
                ),
                next(
                    "nextToken",
                    json!({ "workspaceId": ws, "maxBytes": 65_536 }),
                    "/-/nextToken",
                    "nextToken",
                    Items("", 100),
                ),
                v(
                    "staged",
                    json!({ "workspaceId": ws, "staged": true }),
                    Items("", 2),
                ),
                v(
                    "commitHash",
                    json!({ "workspaceId": ws, "commitHash": s.mid_commit }),
                    Items("", 1),
                ),
                v(
                    "paths",
                    json!({ "workspaceId": ws, "paths": ["src/history/module_1.rs"], "path": "src/history/module_2.rs", "gitRootId": s.git_root }),
                    Items("", 2),
                ),
            ],
        )
        .with_reason("Staged, single-commit and path-narrowed variants select a few files by design."),
        row(
            "git.getBranches",
            vec![
                v(
                    "default",
                    json!({ "repoPath": c.worktree }),
                    Items("/branches", seed::GIT_BRANCHES),
                ),
                v(
                    "includeRemote",
                    json!({ "repoPath": c.worktree, "includeRemote": true }),
                    Items("/branches", seed::GIT_BRANCHES),
                ),
            ],
        ),
        fixed(
            "git.getConfig",
            "A fixed set of git config keys.",
            vec![v("default", w.clone(), Fixed("/config"))],
        ),
        fixed(
            "git.getRemoteUrl",
            "One remote URL.",
            vec![
                v("default", json!({ "repoPath": c.worktree }), Fixed("")),
                v(
                    "remoteName",
                    json!({ "repoPath": c.worktree, "remoteName": "origin" }),
                    Fixed(""),
                ),
            ],
        ),
        row(
            "git.numstat",
            vec![
                v("default", w.clone(), Items("", 1_000)),
                v(
                    "base",
                    json!({ "workspaceId": ws, "baseRef": s.base_branch, "targetRef": "main" }),
                    Items("", 200),
                ),
                v(
                    "baseCommitSha",
                    json!({ "workspaceId": ws, "baseCommitSha": s.mid_commit }),
                    Items("", 100),
                ),
                v(
                    "paths",
                    json!({ "workspaceId": ws, "baseRef": s.base_branch, "paths": ["src/history/module_1.rs", "src/history/module_2.rs"] }),
                    Items("", 2),
                ),
                v(
                    "staged",
                    json!({ "workspaceId": ws, "staged": true }),
                    Items("", 2),
                ),
            ],
        )
        .with_reason("The staged variant selects the two staged files."),
        row(
            "git.showFile",
            vec![
                v(
                    "default",
                    json!({ "workspaceId": ws, "filePath": seed::MID_FILE, "ref": "HEAD" }),
                    Bytes(380_000),
                ),
                v(
                    "maxBytes",
                    json!({ "workspaceId": ws, "filePath": seed::FULL_FILE, "ref": "HEAD", "maxBytes": 100_000_000 }),
                    Bytes(470_000),
                ),
                v(
                    "gitRootId",
                    json!({ "workspaceId": ws, "filePath": seed::FULL_FILE, "ref": ":0", "gitRootId": s.git_root, "maxBytes": 480_000 }),
                    Bytes(470_000),
                ),
            ],
        ),
        row(
            "git.status",
            vec![
                v("default", w.clone(), Items("/files", 1_000)),
                v(
                    "max",
                    json!({ "workspaceId": ws, "gitRootId": s.git_root, "forceRefresh": true }),
                    Items("/files", 1_000),
                ),
            ],
        ),
        fixed(
            "gitRoot.list",
            "Registered git roots of one workspace; a handful per workspace.",
            vec![v("default", w.clone(), Items("/gitRoots", seed::GIT_ROOTS))],
        ),
        fixed(
            "github.authStatus",
            "Local credential status flags.",
            vec![v("default", json!({}), Fixed("/isConfigured"))],
        ),
        row(
            "hook.list",
            vec![
                // Pages stop at `limit` rows (default 100) or the page byte
                // budget (fewer than 100 of the fixture's hooks).
                v("default", w.clone(), Items("/hooks", 50)),
                v(
                    "includeRetired",
                    json!({ "workspaceId": ws, "includeRetired": true }),
                    Items("/hooks", 50),
                ),
                v(
                    "limit=200",
                    json!({ "workspaceId": ws, "limit": 200 }),
                    Items("/hooks", 50),
                ),
                next(
                    "cursor",
                    json!({ "workspaceId": ws, "limit": 50 }),
                    "/nextCursor",
                    "cursor",
                    Items("/hooks", 50),
                ),
            ],
        )
        .parsed_by(
            ("intent-svc-runtime/src/hook_manager.rs", "hook_list_page_params", "wire"),
            &[],
        ),
        fixed(
            "host.checkAuggie",
            "One probe result.",
            vec![v("default", json!({}), Fixed("/available"))],
        ),
        fixed(
            "host.checkGh",
            "One probe result.",
            vec![v("default", json!({}), Fixed("/available"))],
        ),
        fixed(
            "host.checkGit",
            "One probe result.",
            vec![v("default", json!({}), Fixed("/available"))],
        ),
        fixed(
            "host.checkNode",
            "One probe result.",
            vec![v("default", json!({}), Fixed("/available"))],
        ),
        fixed(
            "host.directoryStatus",
            "Flags for one directory.",
            vec![v("default", json!({ "path": c.worktree }), Fixed("/exists"))],
        ),
        fixed(
            "host.env",
            "The daemon's PATH and environment variable names.",
            vec![v("default", json!({}), Fixed("/pathEntries"))],
        ),
        fixed(
            "host.executionContext",
            "Provider defaults and connection summary.",
            vec![v("default", json!({}), Fixed("/enabledProviderIds"))],
        ),
        fixed(
            "host.findApp",
            "One lookup result.",
            vec![v("default", json!({ "name": "Terminal" }), Fixed("/installed"))],
        ),
        fixed(
            "host.findBinary",
            "One lookup result.",
            vec![v("default", json!({ "name": "git" }), Fixed("/available"))],
        ),
        fixed(
            "host.invite.list",
            "Pending host invites; created one at a time by the owner.",
            vec![v("default", json!({}), Items("/invites", seed::HOST_INVITES))],
        ),
        fixed(
            "host.invite.searchAccounts",
            "At most 10 accounts (limit 1–10).",
            vec![
                v(
                    "default",
                    json!({ "provider": "github", "query": "member" }),
                    Fixed("/users"),
                ),
                v(
                    "max",
                    json!({ "provider": "github", "host": "github.com", "query": "member", "limit": 10 }),
                    Fixed("/users"),
                ),
            ],
        ),
        row(
            "host.listDirectory",
            vec![
                v("default", json!({}), Fixed("/entries")),
                v(
                    "path",
                    json!({ "path": format!("{}/.agents/skills", c.worktree) }),
                    Items("/entries", 1_000),
                ),
            ],
        )
        .with_reason("The default lists the daemon user's home directory."),
        fixed(
            "host.listInstalledEditors",
            "Installed editor apps.",
            vec![v("default", json!({}), Fixed("/editors"))],
        ),
        fixed(
            "host.members.list",
            "Host members; bounded by the host member cap.",
            vec![v("default", json!({}), Fixed("/members"))],
        ),
        fixed(
            "host.providerAuthStatus",
            "One status row per known provider.",
            vec![
                v("default", json!({}), Fixed("/providers")),
                v("providerId", json!({ "providerId": "codex" }), Fixed("/providers")),
            ],
        ),
        fixed(
            "host.providerDiscovery",
            "One row per known provider.",
            vec![v("default", json!({}), Fixed("/providers"))],
        ),
        fixed(
            "host.status",
            "Host identity fields.",
            vec![v("default", json!({}), Fixed("/os"))],
        ),
        fixed(
            "host.toolAvailability",
            "One row per requested tool.",
            vec![
                v("default", json!({}), Fixed("/tools")),
                v("tools", json!({ "tools": ["git", "node", "gh"] }), Fixed("/tools")),
            ],
        ),
        fixed(
            "identity.authStatus",
            "Local credential status flags.",
            vec![
                v("default", json!({ "provider": "github" }), Fixed("/isConfigured")),
                v(
                    "host",
                    json!({ "provider": "gitlab", "host": "gitlab.com" }),
                    Fixed("/isConfigured"),
                ),
            ],
        ),
        fixed(
            "identity.getUser",
            "The one selected identity.",
            vec![
                v("default", json!({ "provider": "github" }), Fixed("")),
                v("host", json!({ "provider": "gitlab", "host": "gitlab.com" }), Fixed("")),
            ],
        ),
        fixed(
            "linear.authStatus",
            "Local credential status flags.",
            vec![v("default", json!({}), Fixed(""))],
        ),
        fixed(
            "mcp.oauth.get",
            "One server's token presence.",
            vec![v("default", json!({ "serverId": s.mcp_server }), Fixed("/serverId"))],
        ),
        row(
            "mcp.oauth.list",
            vec![v("default", json!({}), Items("/tokens", seed::OAUTH_TOKENS))],
        ),
        fixed(
            "mcp.servers.getStatus",
            "One server's status.",
            vec![v("default", json!({ "serverId": s.mcp_server }), Fixed(""))],
        ),
        row(
            "mcp.servers.list",
            vec![
                v("default", json!({}), Items("/servers", seed::MCP_SERVERS)),
                v("workspace", w.clone(), Items("/servers", seed::MCP_SERVERS)),
            ],
        ),
        fixed(
            "metrics.getAgentStats",
            "Three counters.",
            vec![v("default", json!({ "agentId": agent }), Fixed("/additions"))],
        ),
        row(
            "note.get",
            vec![
                v("default", wn.clone(), Bytes(400 * 1024)),
                v(
                    "maxContentBytes",
                    json!({ "workspaceId": ws, "noteId": note, "maxContentBytes": 64 * 1024 }),
                    Bytes(64 * 1024),
                ),
                next(
                    "contentOffset",
                    wn.clone(),
                    "/note/nextContentOffset",
                    "contentOffset",
                    Bytes(400 * 1024),
                ),
            ],
        ),
        row(
            "note.getVersion",
            vec![v(
                "default",
                json!({ "workspaceId": ws, "noteId": c.versioned_note, "v": 30 }),
                Bytes(4 * 1024),
            )],
        ),
        row(
            "note.lineAttribution.load",
            vec![
                v("default", wn.clone(), Bytes(256 * 1024)),
                v(
                    "window",
                    json!({ "workspaceId": ws, "noteId": note, "startLine": 2, "maxLines": 100_000 }),
                    Bytes(256 * 1024),
                ),
            ],
        ),
        row(
            "note.list",
            vec![
                v("default", w.clone(), Bytes(300 * 1024)),
                v(
                    "projection=slim",
                    json!({ "workspaceId": ws, "projection": "slim" }),
                    Items("/notes", 200),
                ),
                v(
                    "projection=full",
                    json!({ "workspaceId": ws, "projection": "full" }),
                    Bytes(300 * 1024),
                ),
                v(
                    "limit",
                    json!({ "workspaceId": ws, "projection": "slim", "limit": 100_000 }),
                    Items("/notes", 200),
                ),
                v(
                    "limit+full",
                    json!({ "workspaceId": ws, "projection": "full", "limit": 100_000 }),
                    Bytes(300 * 1024),
                ),
                next(
                    "nextToken",
                    json!({ "workspaceId": ws, "projection": "slim", "limit": 50 }),
                    "/nextToken",
                    "nextToken",
                    Items("/notes", 50),
                ),
            ],
        ),
        row(
            "note.listTasks",
            vec![v(
                "default",
                json!({ "workspaceId": ws, "noteId": s.checklist_note }),
                Items("/tasks", 1_000),
            ),
            v(
                "limit",
                json!({ "workspaceId": ws, "noteId": s.checklist_note, "limit": 100_000 }),
                Items("/tasks", 1_000),
            ),
            next(
                "nextToken",
                json!({ "workspaceId": ws, "noteId": s.checklist_note, "limit": 1_000 }),
                "/nextToken",
                "nextToken",
                Items("/tasks", 1_000),
            )],
        ),
        row(
            "note.listVersions",
            vec![v(
                "default",
                json!({ "workspaceId": ws, "noteId": c.versioned_note }),
                Items("", 50),
            )],
        ),
        row(
            "note.readAsset",
            vec![
                v(
                    "default",
                    json!({ "workspaceId": ws, "asset": s.mid_asset }),
                    Bytes(400_000),
                ),
                v(
                    "maxBytes",
                    json!({ "workspaceId": ws, "asset": s.large_asset, "maxBytes": 100_000_000 }),
                    Fixed("/truncated"),
                ),
            ],
        )
        .with_reason("An asset over maxBytes is sent without its bytes (its size and truncated flag only); the default variant reads one near the cap."),
        row(
            "plugin.list",
            vec![v("default", json!({}), Items("/plugins", seed::PLUGINS))],
        ),
        row(
            "prMonitor.list",
            vec![v("default", w.clone(), Items("/monitors", 200))],
        ),
        row(
            "presence.snapshot",
            vec![v("default", w.clone(), Items("/members", 40))],
        ),
        row(
            "principal.list",
            vec![v("default", json!({}), Items("/principals", 40))],
        ),
        fixed(
            "principal.me",
            "The calling principal.",
            vec![v("default", json!({}), Fixed("/id"))],
        ),
        fixed(
            "providers.catalog",
            "The compiled-in provider registry.",
            vec![v("default", json!({}), Fixed("/providers"))],
        ),
        fixed(
            "providers.setup.status",
            "One provider-setup operation's phase; the connection authorizes setup in its client.hello (`antigravitySetup: 1`).",
            vec![v("default", json!({ "providerId": "antigravity" }), Fixed("/phase"))],
        ),
        fixed(
            "providers.profiles.list",
            "Administrator-defined provider profiles; a handful.",
            vec![
                v("default", json!({}), Items("/profiles", 2 * seed::PROFILES_PER_PROVIDER)),
                v(
                    "baseProviderId",
                    json!({ "baseProviderId": "codex" }),
                    Items("/profiles", seed::PROFILES_PER_PROVIDER),
                ),
            ],
        ),
        fixed(
            "cloudMachines.list",
            "At most one provider (E2B) and one machine per live sandbox, which the owner's E2B plan caps; the seed has none.",
            vec![v("default", json!({}), Fixed("/providers"))],
        ),
        fixed(
            "remoteMachines.backendInfo",
            "This backend's own id, name and certificate pin.",
            vec![v("default", json!({}), Fixed("/backendId"))],
        ),
        row(
            "remoteMachines.list",
            vec![v("default", json!({}), Items("/machines", seed::EXPOSED_MACHINES))],
        ),
        row(
            "repo.list",
            vec![v("default", json!({}), Items("/repos", seed::KNOWN_REPOS))],
        ),
        fixed(
            "repoConfig.get",
            "One repository config file.",
            vec![v("default", w.clone(), Bytes(16 * 1024))],
        ),
        fixed(
            "repoConfig.has",
            "One flag.",
            vec![v("default", w.clone(), Fixed("/exists"))],
        ),
        row(
            "rules.get",
            vec![v(
                "default",
                json!({ "workspaceId": ws, "ruleType": "rule-0" }),
                Fixed("/enabled"),
            )],
        )
        .with_reason("One rules file of a fixed type."),
        row(
            "rules.list",
            vec![
                v("default", json!({}), Fixed("/rules")),
                v("workspace", w.clone(), Items("/rules/rules", 15)),
                v(
                    "limit=100000",
                    json!({ "workspaceId": ws, "limit": 100_000 }),
                    Items("/rules/rules", 15),
                ),
                next(
                    "nextToken",
                    json!({ "workspaceId": ws, "limit": 100_000 }),
                    "/nextToken",
                    "nextToken",
                    Items("/rules/rules", 15),
                ),
            ],
        )
        .with_reason("Unscoped, only the global user-override rules are listed."),
        row(
            "script.get",
            vec![
                v(
                    "default",
                    json!({ "workspaceId": ws, "scriptId": c.script }),
                    Bytes(1024),
                ),
                // A command over the budget is served in parts.
                v(
                    "oversizedCommand",
                    json!({ "workspaceId": ws, "scriptId": c.huge_script }),
                    Bytes(400 * 1024),
                ),
                next(
                    "commandOffset",
                    json!({ "workspaceId": ws, "scriptId": c.huge_script }),
                    "/nextCommandOffset",
                    "commandOffset",
                    Bytes(256 * 1024),
                ),
            ],
        )
        .with_reason("One script definition."),
        row(
            "script.list",
            vec![
                // Full-view pages: default 100 rows within the byte budget.
                v("default", w.clone(), Items("/scripts", 100)),
                v(
                    "view=full",
                    json!({ "workspaceId": ws, "view": "full", "archive": "active" }),
                    Items("/scripts", 100),
                ),
                v(
                    "view=full+limit=200",
                    json!({ "workspaceId": ws, "view": "full", "limit": 200 }),
                    Items("/scripts", 150),
                ),
                next(
                    "view=full+cursor",
                    json!({ "workspaceId": ws, "view": "full", "limit": 100 }),
                    "/nextCursor",
                    "cursor",
                    Items("/scripts", 100),
                ),
                v(
                    "view=summary",
                    json!({ "workspaceId": ws, "view": "summary", "limit": 200 }),
                    Items("/scripts", 200),
                ),
                next(
                    "cursor",
                    json!({ "workspaceId": ws, "view": "summary", "limit": 100 }),
                    "/nextCursor",
                    "cursor",
                    Items("/scripts", 100),
                ),
            ],
        )
        .parsed_by(SCRIPT_LIST_PARSE, &[("archive", "parse reads `value.get(filter)` with filter = \"archive\" for script lists")]),
        row(
            "script.output",
            vec![
                v(
                    "default",
                    json!({ "workspaceId": ws, "scriptId": s.loud_script }),
                    Bytes(4 * 1024),
                ),
                v(
                    "maxLines=100000",
                    json!({ "workspaceId": ws, "scriptId": s.loud_script, "maxLines": 100_000 }),
                    Bytes(256 * 1024),
                ),
                v(
                    "paginate",
                    json!({ "workspaceId": ws, "scriptId": s.loud_script, "maxLines": 100_000, "paginate": true }),
                    Bytes(8 * 1024),
                ),
                next(
                    "nextToken",
                    json!({ "workspaceId": ws, "scriptId": s.loud_script, "maxLines": 500, "paginate": true }),
                    "/nextToken",
                    "nextToken",
                    Bytes(8 * 1024),
                ),
            ],
        )
        .parsed_by(MACHINE_RUN_PARSE, &[])
        .skip(MACHINE_RUN_UNEXERCISED),
        fixed(
            "script.status",
            "Runtime state of one script.",
            vec![v(
                "default",
                json!({ "workspaceId": ws, "scriptId": s.loud_script }),
                Fixed("/status"),
            )],
        )
        .parsed_by(MACHINE_RUN_PARSE, &[])
        .skip(MACHINE_RUN_UNEXERCISED),
        fixed(
            "scriptMonitor.get",
            "One script monitor.",
            vec![v(
                "default",
                json!({ "workspaceId": ws, "monitorId": c.monitor }),
                Fixed(""),
            )],
        ),
        row(
            "scriptMonitor.list",
            vec![
                v("default", w.clone(), Items("/monitors", 200)),
                v(
                    "agent",
                    json!({ "workspaceId": ws, "agentId": agent, "view": "full", "state": "active" }),
                    Items("/monitors", 200),
                ),
                v(
                    "view=summary",
                    json!({ "workspaceId": ws, "view": "summary", "limit": 200 }),
                    Items("/monitors", 200),
                ),
                next(
                    "cursor",
                    json!({ "workspaceId": ws, "view": "summary", "limit": 50 }),
                    "/nextCursor",
                    "cursor",
                    Items("/monitors", 50),
                ),
            ],
        )
        .parsed_by(SCRIPT_LIST_PARSE, &[("state", "parse reads `value.get(filter)` with filter = \"state\" for monitor lists")]),
        fixed(
            "search.codebase",
            "Context-engine hits: at most the inline threshold (25) in the reply, more stream as events; the stub engine answers exactly the inline maximum.",
            vec![v(
                "default",
                json!({ "workspaceId": ws, "query": "payload budget", "requestId": "r-codebase" }),
                Items("/matches", crate::proxies::SEARCH_INLINE_MAX),
            )],
        ),
        row(
            "search.events",
            vec![
                v("default", json!({ "query": "agent" }), Items("/matches", 50)),
                v(
                    "max",
                    json!({ "query": "agent", "workspaceId": ws, "limit": 100_000, "requestId": "r-events" }),
                    Fixed("/requestId"),
                ),
            ],
        )
        .with_reason("Workspace-scoped hits over the inline threshold stream as search result events in fixed batches; the reply is then only the acknowledgement."),
        row(
            "search.fileNames",
            vec![
                v(
                    "default",
                    json!({ "workspaceId": ws, "pattern": "skill" }),
                    Fixed("/requestId"),
                ),
                v(
                    "max",
                    json!({ "workspaceId": ws, "pattern": "s", "limit": 100_000, "requestId": "r-files" }),
                    Items("/files", 500),
                ),
            ],
        )
        .with_reason("Workspace-scoped hits over the inline threshold stream as search result events in fixed batches; the reply is then only the acknowledgement."),
        row(
            "search.inFiles",
            vec![
                v(
                    "default",
                    json!({ "workspaceId": ws, "query": "payload" }),
                    Cut("/matches", 100, "/truncated"),
                ),
                v(
                    "max",
                    json!({ "workspaceId": ws, "query": "e", "requestId": "r-infiles", "opts": { "caseSensitive": false, "maxResults": 100_000 } }),
                    Cut("/matches", 1_000, "/truncated"),
                ),
            ],
        ),
        row(
            "search.messages",
            vec![
                v("default", json!({ "query": "agent" }), Items("/matches", 50)),
                v(
                    "max",
                    json!({ "query": "agent", "workspaceId": ws, "limit": 100_000, "preferWorkspaceId": ws, "requestId": "r-messages" }),
                    Fixed("/requestId"),
                ),
                v(
                    "agent+role",
                    json!({ "query": "agent", "agentId": agent, "role": "user", "limit": 100_000 }),
                    Items("/matches", 20),
                ),
            ],
        )
        .with_reason("Workspace-scoped hits over the inline threshold stream as search result events in fixed batches; the reply is then only the acknowledgement."),
        row(
            "search.notes",
            vec![
                v("default", json!({ "query": "agent" }), Items("/matches", 10)),
                v(
                    "max",
                    json!({ "query": "agent", "workspaceId": ws, "preferWorkspaceId": ws, "limit": 100_000, "includeArchived": true, "requestId": "r-notes" }),
                    Items("/matches", 100),
                ),
                next(
                    "nextToken",
                    json!({ "query": "agent", "limit": 20 }),
                    "/nextToken",
                    "nextToken",
                    Items("/matches", 20),
                ),
            ],
        ),
        fixed(
            "secret.list",
            "Secret names only; the vault holds at most 32.",
            vec![v("default", wa.clone(), Items("/secrets", 32))],
        ),
        fixed(
            "sentry.authStatus",
            "Local credential status flags.",
            vec![v("default", json!({}), Fixed(""))],
        ),
        fixed(
            "settings.get",
            "One setting.",
            vec![v("default", json!({ "path": "model.defaultProvider" }), Fixed("/path"))],
        ),
        fixed(
            "settings.list",
            "The fixed settings registry.",
            vec![v("default", json!({}), Fixed("/settings"))],
        ),
        row(
            "skill.list",
            vec![
                v("default", w.clone(), Items("", 300)),
                v(
                    "limit=100000",
                    json!({ "workspaceId": ws, "limit": 100_000 }),
                    Items("/skills", 300),
                ),
                next(
                    "nextToken",
                    json!({ "workspaceId": ws, "limit": 100_000 }),
                    "/nextToken",
                    "nextToken",
                    Items("/skills", 300),
                ),
            ],
        ),
        fixed(
            "sourceControl.authStatus",
            "Local credential status flags.",
            vec![
                v("default", json!({ "provider": "github" }), Fixed("/isConfigured")),
                v(
                    "max",
                    json!({ "provider": "gitlab", "host": "gitlab.com", "instanceBaseUrl": "https://gitlab.com" }),
                    Fixed("/isConfigured"),
                ),
            ],
        ),
        row(
            "specialist.get",
            vec![
                v("default", json!({ "id": c.specialist }), Bytes(16 * 1024)),
                v(
                    "max",
                    json!({ "id": c.specialist, "workspacePath": c.worktree, "provider": "claude-code" }),
                    Bytes(16 * 1024),
                ),
            ],
        ),
        row(
            "specialist.list",
            vec![
                v("default", json!({}), Items("/specialists", 8)),
                v(
                    "max",
                    json!({ "workspaceId": ws, "includeProject": true, "provider": "claude-code", "limit": 100_000 }),
                    Items("/specialists", 8),
                ),
                next(
                    "nextToken",
                    json!({ "limit": 100_000 }),
                    "/nextToken",
                    "nextToken",
                    Items("/specialists", 8),
                ),
            ],
        ),
        row(
            "stats.getRateHistory",
            vec![
                v("default", json!({}), Items("/samples", 60)),
                v("limit=1440", json!({ "limit": 1_440 }), Items("/samples", 1_000)),
            ],
        ),
        fixed(
            "stats.getUsage",
            "Aggregated totals, bucketed by a fixed calendar period.",
            vec![
                v("24h", json!({ "period": "24h" }), Fixed("/totals")),
                v(
                    "year",
                    json!({ "period": "year", "key": "2025", "tzOffsetMinutes": 120 }),
                    Fixed("/totals"),
                ),
            ],
        ),
        fixed(
            "system.capabilities",
            "Capability flags.",
            vec![v("default", json!({}), Fixed(""))],
        ),
        fixed(
            "task.get",
            "One task.",
            vec![v(
                "default",
                json!({ "workspaceId": ws, "taskNoteId": c.task_note }),
                Fixed("/task"),
            )],
        ),
        row(
            "task.getMyTask",
            vec![v(
                "default",
                json!({ "workspaceId": ws, "taskNoteId": c.task_note }),
                Bytes(4 * 1024),
            )],
        )
        .with_reason("One task note with its metadata."),
        row(
            "task.list",
            vec![
                v("default", w.clone(), Items("/tasks", 50)),
                v(
                    "status",
                    json!({ "workspaceId": ws, "status": "in_progress" }),
                    Items("/tasks", 50),
                ),
                v(
                    "limit",
                    json!({ "workspaceId": ws, "limit": 100_000 }),
                    Items("/tasks", 50),
                ),
                next(
                    "nextToken",
                    json!({ "workspaceId": ws, "limit": 25 }),
                    "/nextToken",
                    "nextToken",
                    Items("/tasks", 25),
                ),
            ],
        ),
        row(
            "task.listAgentLinks",
            vec![v("default", w.clone(), Items("/links", 50))],
        ),
        row(
            "terminal.getBuffer",
            vec![
                v(
                    "default",
                    json!({ "terminalId": s.terminal }),
                    Bytes(256 * 1024),
                ),
                v(
                    "maxBytes",
                    json!({ "terminalId": s.terminal, "maxBytes": 64 * 1024 }),
                    Bytes(32 * 1024),
                ),
            ],
        ),
        row(
            "terminal.list",
            vec![v("default", w.clone(), Items("/terminals", seed::TERMINALS))],
        ),
        row(
            "terminal.readOutput",
            vec![
                v(
                    "default",
                    json!({ "workspaceId": ws, "terminalId": s.terminal }),
                    Bytes(4 * 1024),
                ),
                v(
                    "maxLines=100000",
                    json!({ "workspaceId": ws, "terminalId": s.terminal, "maxLines": 100_000 }),
                    Bytes(128 * 1024),
                ),
                v(
                    "paginate",
                    json!({ "workspaceId": ws, "terminalId": s.terminal, "maxLines": 100_000, "paginate": true }),
                    Bytes(8 * 1024),
                ),
                next(
                    "nextToken",
                    json!({ "workspaceId": ws, "terminalId": s.terminal, "maxLines": 500, "paginate": true }),
                    "/nextToken",
                    "nextToken",
                    Bytes(8 * 1024),
                ),
            ],
        ),
        fixed(
            "unsloth.status",
            "The local model server's state.",
            vec![v("default", json!({}), Fixed("/running"))],
        ),
        fixed(
            "voice.getWorkspaceVocabulary",
            "Derived vocabulary terms; capped by the deriver (default 50).",
            vec![v("default", w.clone(), Items("/terms", 40))],
        ),
        fixed(
            "workspace.detectProjectType",
            "One project type.",
            vec![v("default", w.clone(), Fixed(""))],
        ),
        fixed(
            "workspace.diskUsage",
            "Disk usage counters.",
            vec![v("default", w.clone(), Fixed(""))],
        ),
        row(
            "workspace.findByPullRequest",
            vec![
                v("default", json!({ "url": s.pr_url }), Items("/workspaces", 200)),
                v(
                    "includeRetired",
                    json!({ "url": s.pr_url, "includeRetired": true }),
                    Items("/workspaces", 200),
                ),
            ],
        ),
        row(
            "workspace.findRepositories",
            vec![v(
                "default",
                json!({ "directory": s.repos_dir }),
                Items("/repositories", seed::FOUND_REPOS),
            )],
        ),
        fixed(
            "workspace.generateSetupScript",
            "A template script for one detected project type.",
            vec![v("default", w.clone(), Fixed("/setupScript"))],
        ),
        row(
            "workspace.get",
            vec![v("default", w.clone(), Bytes(1024))],
        )
        .with_reason("One workspace row with its detail fields."),
        fixed(
            "workspace.getAutoCommit",
            "One setting.",
            vec![v("default", w.clone(), Fixed("/autoCommit"))],
        ),
        fixed(
            "workspace.getBrowserClient",
            "The pinned browser client.",
            vec![v("default", w.clone(), Fixed("/browserClient"))],
        ),
        row(
            "workspace.getContext",
            vec![v("default", w.clone(), Items("/items", seed::CONTEXT_ITEMS))],
        ),
        fixed(
            "workspace.getSetupScript",
            "One saved setup script.",
            vec![v("default", w.clone(), Fixed("/setupScript"))],
        ),
        fixed(
            "workspace.getTokenUsage",
            "Token counters.",
            vec![v("default", w.clone(), Fixed("/tokenUsage"))],
        ),
        row(
            "workspace.getUiContext",
            vec![v("default", w.clone(), Bytes(128 * 1024))],
        ),
        row(
            "workspace.invite.list",
            vec![v("default", w.clone(), Items("/invites", 40))],
        ),
        row(
            "workspace.list",
            vec![
                // Pages of 100 (default) and 200 rows can both stop at the
                // 480,000-byte page budget first, so they prove reach by bytes.
                v("default", json!({}), Bytes(300 * 1024)),
                v("limit=200", json!({ "limit": 200 }), Bytes(300 * 1024)),
                next(
                    "nextToken",
                    json!({ "limit": 200 }),
                    "/nextToken",
                    "nextToken",
                    Bytes(300 * 1024),
                ),
                v(
                    "max",
                    json!({ "includeArchived": true, "includeRetired": true, "limit": 200 }),
                    Bytes(300 * 1024),
                ),
            ],
        ),
        row(
            "workspace.localChanges",
            vec![v("default", w.clone(), Items("/roots", 1))],
        )
        .with_reason("Change counters per git root."),
        row(
            "workspace.members.list",
            vec![v("default", w.clone(), Items("/members", 40))],
        ),
        row(
            "workspace.transfer.plan",
            vec![v("default", w, Bytes(1024))],
        )
        .with_reason("A transfer plan summary: counts and sizes per entity kind."),
    ]
}

impl Row {
    /// Attach the small-by-design reason to a row whose variants mix
    /// large-data proofs with a few `Fixed` ones.
    fn with_reason(mut self, why: &'static str) -> Self {
        self.small_by_design = Some(why);
        self
    }
}

/// Golden: snapshot channels whose seq-0 snapshot cannot carry large data
/// here, with why. The sweep still subscribes with
/// [`snapshot_golden_params`] and requires the snapshot to arrive.
pub const SNAPSHOT_CANNOT_EXERCISE: &[(&str, &str)] = &[
    (
        "agent.workers.subscribe",
        "Background workers are reported only by a live ACP agent process; without one the snapshot is the no-session projection.",
    ),
    (
        "presence.focus.subscribe",
        "One resolved focus target for one (viewer, principal) pair by design; it does not grow with daemon state.",
    ),
];

/// Subscribe params for a [`SNAPSHOT_CANNOT_EXERCISE`] channel.
pub fn snapshot_golden_params(c: &Ctx<'_>, channel: &str) -> Value {
    match channel {
        "presence.focus.subscribe" => {
            json!({ "workspaceId": c.ws, "principalId": c.member_principal })
        }
        _ => json!({ "workspaceId": c.ws, "agentId": c.agent }),
    }
}

/// One row per channel in `SNAPSHOT_CHANNELS`; proofs point into the
/// snapshot (`params.snapshot` of the seq-0 push).
pub fn snapshot_rows(c: &Ctx<'_>) -> Vec<Row> {
    use Proof::{Bytes, Fixed, Items};
    let ws = c.ws.as_str();
    let agent = c.agent.as_str();
    let w = json!({ "workspaceId": ws });
    vec![
        row("agent.subscribe", vec![v("default", w.clone(), Items("", 100))]),
        row(
            "chat.subscribe",
            vec![
                v("default", json!({ "agentId": agent }), Items("/messages", 20)),
                v(
                    "limit=200",
                    json!({ "agentId": agent, "workspaceId": ws, "limit": 200 }),
                    Items("/messages", 100),
                ),
                v(
                    "incremental",
                    json!({ "agentId": agent, "limit": 200, "deltaEncoding": "incremental", "projection": "slim" }),
                    Items("/messages", 100),
                ),
                v(
                    "progressive",
                    json!({ "agentId": agent, "limit": 200, "historyDelivery": "progressive" }),
                    Fixed("/messages"),
                ),
                v(
                    "sinceMessageId",
                    json!({ "agentId": agent, "limit": 200, "sinceMessageId": c.message_id }),
                    Items("/messages", 20),
                ),
            ],
        )
        .with_reason("Progressive history sends only the newest message in the snapshot and streams the rest."),
        row(
            "comment.subscribe",
            vec![
                v(
                    "default",
                    json!({ "workspaceId": ws, "noteId": c.note }),
                    Items("", 100),
                ),
                v(
                    "limit",
                    json!({ "workspaceId": ws, "noteId": c.note, "limit": 100_000 }),
                    Bytes(400 * 1024),
                ),
            ],
        ),
        row(
            "note.presence.subscribe",
            vec![v(
                "default",
                json!({ "workspaceId": ws, "noteId": c.note }),
                Items("/viewers", 40),
            )],
        ),
        row(
            "note.subscribe",
            vec![
                v("default", w.clone(), Bytes(300 * 1024)),
                v(
                    "projection=slim",
                    json!({ "workspaceId": ws, "projection": "slim" }),
                    Items("", 200),
                ),
                v(
                    "maxContentBytes",
                    json!({ "workspaceId": ws, "projection": "full", "maxContentBytes": 4096 }),
                    Bytes(300 * 1024),
                ),
                v(
                    "limit",
                    json!({ "workspaceId": ws, "projection": "slim", "limit": 100_000 }),
                    Items("", 200),
                ),
                v(
                    "limit+full",
                    json!({ "workspaceId": ws, "projection": "full", "limit": 100_000 }),
                    Bytes(300 * 1024),
                ),
            ],
        ),
        row("task.subscribe", vec![v("default", w, Items("", 50))]),
        row(
            "workspace.subscribe",
            vec![
                v("default", json!({}), Items("", 100)),
                v("includeRetired", json!({ "includeRetired": true }), Items("", 100)),
            ],
        ),
    ]
}

/// Golden: `ExternalProxy` methods the stub upstreams cannot reach (they
/// go through the native repository-authority wiring, which needs read
/// leases the test services cannot mint), with the observed outcome and
/// how their page size is bounded instead. `payload_budget_proxy_page_caps`
/// (read pages) and intent-services' `checkout_page_size_is_capped_at_100`
/// (checkout pages) prove the caps named here.
pub const PROXY_CANNOT_EXERCISE: &[(&str, &str, &str)] = &[
    (
        "sourceControl.checkout.branches",
        "-32602",
        "Native checkout lease; page size capped at 100 by `page_scope` (limit outside 1–100 is -32602).",
    ),
    (
        "sourceControl.checkout.project",
        "-32602",
        "Native checkout lease; one project, no page.",
    ),
    (
        "sourceControl.checkout.projects",
        "-32602",
        "Native checkout lease; page size capped at 100 by `page_scope` (limit outside 1–100 is -32602).",
    ),
    (
        "sourceControl.checkout.repoConfig",
        "-32602",
        "Native checkout lease; one config file at a fixed SHA, no page.",
    ),
    (
        "sourceControl.getUser",
        "-32602",
        "Needs a connected forge account in the repository-connection directory; one user, no page.",
    ),
    (
        "sourceControl.read.detail",
        "-32602",
        "Native read lease; one PR or MR, no page.",
    ),
    (
        "sourceControl.read.detailScoped",
        "-32602",
        "Native read lease; one PR or MR, no page.",
    ),
    (
        "sourceControl.read.discussions",
        "-32602",
        "Native read lease; `page.limit` is 1–100 by RepositoryPageQuery deserialization.",
    ),
    (
        "sourceControl.read.list",
        "-32602",
        "Native read lease; `query.limit` is 1–100 by RepositoryListQuery deserialization.",
    ),
];

/// One row per `ExternalProxy` method the stub upstreams serve.
#[expect(clippy::too_many_lines, reason = "one flat table reads best")]
pub fn proxy_rows() -> Vec<Row> {
    use Proof::{Bytes, Fixed, Items};
    let gh = json!({ "owner": "intent-hq", "repo": "large-state-repo-0" });
    let with = |extra: Value| {
        let mut p = gh.clone();
        for (k, v) in extra.as_object().expect("object") {
            p[k] = v.clone();
        }
        p
    };
    // Every paginated GitHub read: default page, the largest page, the
    // `perPage` alias, and the next page.
    // `full`: the item count a maximal page must reach. Reads whose items are
    // small fill the 100-item page; byte-budgeted forge reads (long bodies)
    // stop at the item budget and resume inside the upstream page, so they
    // prove a page of at least `BUDGETED_PAGE_ITEMS` long items instead.
    let paged_upto = |base: Value, at: &'static str, extra: Value, full: usize| -> Vec<Variant> {
        let merge = |more: Value| {
            let mut p = base.clone();
            for (k, v) in extra.as_object().expect("object") {
                p[k] = v.clone();
            }
            for (k, v) in more.as_object().expect("object") {
                p[k] = v.clone();
            }
            p
        };
        vec![
            v("default", base.clone(), Items(at, 10)),
            v(
                "limit=1000",
                merge(json!({ "limit": 1_000 })),
                Items(at, full),
            ),
            v(
                "perPage=1000",
                merge(json!({ "perPage": 1_000 })),
                Items(at, full),
            ),
            next(
                "nextToken",
                merge(json!({ "limit": 1_000 })),
                "/nextToken",
                "nextToken",
                Items(at, full),
            ),
        ]
    };
    let paged = |base: Value, at: &'static str, extra: Value| paged_upto(base, at, extra, 100);
    let budgeted = |base: Value, at: &'static str, extra: Value| {
        paged_upto(base, at, extra, BUDGETED_PAGE_ITEMS)
    };
    let single = |why: &'static str| why;
    vec![
        row(
            "github.branches.list",
            paged(gh.clone(), "/branches", json!({ "prefix": "agent/" })),
        ),
        fixed(
            "github.branches.listCached",
            single("Branch names cached from earlier list calls; empty until one ran."),
            vec![v("default", gh.clone(), Fixed(""))],
        ),
        row(
            "github.getReviewThreads",
            budgeted(with(json!({ "number": 1 })), "/threads", json!({})),
        ),
        fixed(
            "github.getUser",
            single("The signed-in user."),
            vec![v("default", json!({}), Fixed(""))],
        ),
        row(
            "github.issues.get",
            vec![v("default", with(json!({ "number": 1 })), Bytes(8 * 1024))],
        ),
        row(
            "github.issues.list",
            budgeted(
                gh.clone(),
                "/issues",
                json!({ "state": "open", "labels": "bug" }),
            ),
        ),
        row(
            "github.issues.search",
            budgeted(
                gh.clone(),
                "/issues",
                json!({ "filter": "assigned", "state": "open", "query": "budget", "repos": [{ "owner": "intent-hq", "repo": "large-state-repo-1" }] }),
            ),
        ),
        row(
            "github.listReviewComments",
            budgeted(with(json!({ "number": 1 })), "/comments", json!({})),
        ),
        row(
            "github.pulls.checks",
            vec![v("default", with(json!({ "number": 1 })), Bytes(4 * 1024))],
        ),
        row(
            "github.pulls.files",
            budgeted(
                with(json!({ "number": 1 })),
                "/files",
                json!({ "expectedHeadSha": "deadbeef" }),
            ),
        ),
        row(
            "github.pulls.get",
            vec![v("default", with(json!({ "number": 1 })), Bytes(8 * 1024))],
        ),
        row(
            "github.pulls.list",
            budgeted(
                gh.clone(),
                "/pulls",
                json!({ "state": "open", "head": "intent-hq:feature", "base": "main" }),
            ),
        ),
        row(
            "github.pulls.reviews",
            budgeted(with(json!({ "number": 1 })), "/reviews", json!({})),
        ),
        row("github.pulls.search", {
            let mut variants = budgeted(
                gh.clone(),
                "/pulls",
                json!({ "filter": "created", "state": "open", "query": "budget", "repos": [{ "owner": "intent-hq", "repo": "large-state-repo-1" }] }),
            );
            variants.push(v(
                "org",
                json!({ "org": "intent-hq", "limit": 1_000 }),
                Items("/pulls", BUDGETED_PAGE_ITEMS),
            ));
            variants
        }),
        fixed(
            "github.relatedRepos.list",
            single("The fork and its upstream: two repositories."),
            vec![v("default", with(json!({ "ref": "main" })), Fixed(""))],
        ),
        fixed(
            "github.repoConfig.get",
            single("One repository config file, parsed into the known config fields."),
            vec![v(
                "default",
                with(json!({ "ref": "main" })),
                Bytes(16 * 1024),
            )],
        ),
        fixed(
            "github.repos.get",
            single("One repository."),
            vec![v("default", gh.clone(), Fixed(""))],
        ),
        row("github.repos.list", paged(json!({}), "/repos", json!({}))),
        row("github.repos.search", {
            let mut variants = paged(json!({ "query": "large" }), "/repos", json!({}));
            variants.retain(|v| v.name != "perPage=1000");
            variants
        }),
        fixed(
            "github.users.search",
            single("Account search answers at most the clamped user-search limit."),
            vec![
                v("default", json!({ "query": "member" }), Fixed("")),
                v(
                    "limit=1000",
                    json!({ "query": "member", "limit": 1_000 }),
                    Fixed(""),
                ),
            ],
        ),
        row(
            "linear.getIssue",
            vec![
                v("id", json!({ "id": "issue-0" }), Bytes(8 * 1024)),
                v(
                    "identifier",
                    json!({ "identifier": "ENG-0" }),
                    Bytes(8 * 1024),
                ),
            ],
        ),
        row(
            "linear.listIssues",
            vec![
                v("default", json!({}), Items("/issues", 10)),
                v(
                    "limit=100000",
                    json!({ "limit": 100_000, "filter": "assigned" }),
                    Items("/issues", BUDGETED_PAGE_ITEMS),
                ),
                next(
                    "nextToken",
                    json!({ "limit": 100_000 }),
                    "/nextToken",
                    "nextToken",
                    Items("/issues", BUDGETED_PAGE_ITEMS),
                ),
            ],
        ),
        row(
            "linear.listLabels",
            vec![
                v("default", json!({}), Items("", 10)),
                v("limit=100000", json!({ "limit": 100_000 }), Items("", 200)),
            ],
        ),
        row(
            "linear.listProjects",
            vec![
                v("default", json!({}), Items("", 10)),
                v("limit=100000", json!({ "limit": 100_000 }), Items("", 100)),
            ],
        ),
        row(
            "linear.listTeams",
            vec![
                v("default", json!({}), Items("", 10)),
                v("limit=100000", json!({ "limit": 100_000 }), Items("", 200)),
            ],
        ),
        row(
            "linear.listWorkflowStates",
            vec![
                v("default", json!({}), Items("", 10)),
                v("limit=100000", json!({ "limit": 100_000 }), Items("", 200)),
            ],
        ),
        row(
            "linear.searchIssues",
            vec![
                v(
                    "default",
                    json!({ "query": "budget" }),
                    Items("/issues", 10),
                ),
                next(
                    "nextToken",
                    json!({ "query": "budget", "limit": 100_000 }),
                    "/nextToken",
                    "nextToken",
                    Items("/issues", BUDGETED_PAGE_ITEMS),
                ),
            ],
        ),
        fixed(
            "linear.viewer",
            single("The signed-in user."),
            vec![v("default", json!({}), Fixed(""))],
        ),
        row(
            "sentry.getIssue",
            vec![
                v("id", json!({ "id": "1" }), Bytes(1024)),
                v("shortId", json!({ "shortId": "APP-1" }), Bytes(1024)),
            ],
        )
        .with_reason("One issue."),
        row(
            "sentry.listIssues",
            vec![
                v("default", json!({}), Items("/issues", 10)),
                v(
                    "max",
                    json!({ "project": "app", "status": "unresolved", "query": "TypeError", "limit": 100_000 }),
                    Items("/issues", 100),
                ),
                next(
                    "nextToken",
                    json!({ "limit": 100_000 }),
                    "/nextToken",
                    "nextToken",
                    Items("/issues", 100),
                ),
            ],
        ),
        row(
            "sentry.listProjects",
            vec![
                v("default", json!({}), Items("", 10)),
                v("limit=100000", json!({ "limit": 100_000 }), Items("", 100)),
            ],
        ),
        row(
            "sentry.searchIssues",
            vec![
                v(
                    "default",
                    json!({ "query": "TypeError" }),
                    Items("/issues", 10),
                ),
                next(
                    "nextToken",
                    json!({ "query": "TypeError", "project": "app", "limit": 100_000 }),
                    "/nextToken",
                    "nextToken",
                    Items("/issues", 100),
                ),
            ],
        ),
    ]
}
