//! Where a remote agent's `workspace_api` calls run.
//!
//! A remote agent evaluates its `workspace_api` code on its execution node.
//! Each `host({ method, args })` frame is routed by its method: calls on the
//! agent's own checkout and host (attachment copies included) run on the node, a fixed set of control-plane
//! namespaces (saved scripts included: head keeps them and runs them on the
//! agent's machine; hooks run on head; the browser and the desktop act on the
//! user's client connected to head), plus single methods such as
//! `workspace.details`, `git.listRoots`, the terminal reads and
//! `app.question.ask` ([`FORWARDED_METHODS`]), is
//! forwarded to head over the node link (head checks the agent is on the
//! link's lease and routes the frame again, never trusting the node), and
//! everything else is refused with an explicit error instead of running on
//! the wrong host.

/// The link request method that carries one forwarded frame. Its params are
/// `{ "method": <frame method>, "args": <frame args> }`.
pub const CALL_METHOD: &str = "workspace.call";

/// The frame method the node forwards (never from agent code: the table
/// refuses it there) to read the workspace's effective auto-commit policy
/// before an agent-initiated `git.commit`. Head answers `{ "autoCommit": bool }`.
pub const COMMIT_POLICY_METHOD: &str = "git.commitPolicy";

/// The frame method the node forwards (never from agent code: the table
/// refuses it there) when one of the agent's `workspace_api` calls produced
/// cards (such as a sibling proposal), before the call answers the provider.
/// Args `{ "attachments": [{ "id", "mimeType", "uri", "name", "text" }] }`,
/// at most [`MAX_FORWARDED_ATTACHMENTS`]. Head registers them as one batch
/// for the agent's turn, as a local agent's call does, and answers `{}`.
pub const ATTACHMENTS_METHOD: &str = "turn.attachments";

/// The frame method the node forwards (never from agent code: the table
/// refuses it there) for the machine-placement text a local agent sees in
/// its `workspace_api` description and `ws.help("agent")`. Args `null`;
/// head answers `{ "help": string | null }`.
pub const PLACEMENT_HELP_METHOD: &str = "agent.placementHelp";

/// The frame method the node forwards (never from agent code: the table
/// refuses it there) for the bytes of an attachment the agent copies with
/// `ws.file.getAttachment`, which runs on the node. Args `{ attachmentId }`;
/// head answers `{ fileName, mimeType?, size, uploadedAt, data }` (`data` is
/// base64) for an attachment of the agent's workspace of at most
/// [`MAX_LINK_ATTACHMENT_BYTES`]. With `offset` and `length` too, head
/// answers one piece of a file of any size: `data` is the bytes from
/// `offset`, at most [`MAX_LINK_ATTACHMENT_CHUNK_BYTES`], and the answer
/// also carries `offset` and, at offset 0, the whole file's `sha256`. An
/// older backend ignores the range and answers as without it.
pub const ATTACHMENT_DATA_METHOD: &str = "file.attachmentData";

/// Largest attachment [`ATTACHMENT_DATA_METHOD`] carries whole: its base64
/// must fit one response frame ([`crate::node_link::MAX_FRAME_BYTES`]).
pub const MAX_LINK_ATTACHMENT_BYTES: u64 = 5 * 1024 * 1024;

/// Largest piece [`ATTACHMENT_DATA_METHOD`] carries when asked for a range.
pub const MAX_LINK_ATTACHMENT_CHUNK_BYTES: u64 = 4 * 1024 * 1024;

/// The frame method the node forwards (never from agent code: the table
/// refuses it there) for a provider's `tools/list` on one of the user MCP
/// servers proxied to head (nodeProtocol 11). Args `{ "serverId" }`; head
/// answers the server's raw MCP result.
pub const MCP_PROXY_LIST_TOOLS_METHOD: &str = "mcp.proxy.listTools";

/// The frame method the node forwards (never from agent code) for a
/// provider's `tools/call` on a proxied user MCP server (nodeProtocol 11).
/// Args `{ "serverId", "name", "arguments" }`; head answers the server's raw
/// MCP result.
pub const MCP_PROXY_CALL_TOOL_METHOD: &str = "mcp.proxy.callTool";

/// How long a proxied `tools/call` may run on head's hub; `ws.mcp` keeps
/// the hub's own shorter cap.
pub const MCP_PROXY_CALL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(600);

/// The link budget of one proxied frame: the call's own bound plus room
/// for the gates and the reply.
pub const MCP_PROXY_LINK_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(630);

/// The node's own frames to head. They ride [`CALL_METHOD`] but agent code
/// cannot send them, even when their namespace is forwarded.
pub const INTERNAL_METHODS: &[&str] = &[
    COMMIT_POLICY_METHOD,
    ATTACHMENTS_METHOD,
    PLACEMENT_HELP_METHOD,
    ATTACHMENT_DATA_METHOD,
    MCP_PROXY_LIST_TOOLS_METHOD,
    MCP_PROXY_CALL_TOOL_METHOD,
];

/// Most cards one [`ATTACHMENTS_METHOD`] frame carries.
pub const MAX_FORWARDED_ATTACHMENTS: usize = 32;

/// Namespaces forwarded to head. A remote agent's hooks belong to it but
/// run on head: their code and its `ws.host.exec` see head's worktree, and
/// their wakes reach the agent as agent messages. `browser` and `desktop`
/// act on the user's client connected to head, with a local agent's consent.
/// `mcp` lists and calls head's MCP servers, with a local agent's gates.
pub const FORWARDED_NAMESPACES: &[&str] = &[
    "note",
    "task",
    "agent",
    "pr",
    "mr",
    "comment",
    "event",
    "script",
    "primitive",
    "discussion",
    "crossWorkspace",
    "hook",
    "browser",
    "desktop",
    "mcp",
];

/// The forwarded call head answers with the roots the agent's machine has,
/// relative to its checkout, and the machine resolves in it.
pub const GIT_ROOTS_METHOD: &str = "git.listRoots";

/// Methods that run on the node, against the agent's checkout.
pub const LOCAL_METHODS: &[&str] = &[
    "file.read",
    "file.write",
    "file.list",
    "file.delete",
    "file.mkdir",
    "file.rename",
    "file.getAttachment",
    "git.commit",
    "host.exec",
    "workspace.info",
];

/// Single methods forwarded to head outside [`FORWARDED_NAMESPACES`]: the
/// `workspace.*` calls other than `info`, the secondary-root list, the
/// read-only terminal calls, `ws.search` and structured questions (registered for the
/// agent's turn on head, so they drain onto its message there). Head runs them as the agent with a local
/// agent's checks, so `setAgentName` renames only the caller, a sub-agent
/// cannot propose, and `archive` / `unarchive` act on the workspace on head
/// (not the agent's checkout); the node publishes a proposal's card with
/// [`ATTACHMENTS_METHOD`]. They ride `workspace.call` on every protocol: an
/// older machine refuses them itself, an older backend refuses them by its
/// own table.
pub const FORWARDED_METHODS: &[&str] = &[
    "workspace.details",
    "workspace.setTitle",
    "workspace.setStatusMessage",
    "workspace.setStatusImage",
    "workspace.setAgentName",
    "workspace.proposeSibling",
    "workspace.applyProposal",
    "workspace.archive",
    "workspace.unarchive",
    "workspace.context",
    "workspace.referenceDocs",
    "workspace.timeline",
    "workspace.emitNotification",
    "git.listRoots",
    "terminal.list",
    "terminal.readOutput",
    "search.workspace",
    "app.question.ask",
    "secret.request",
    "secret.list",
    "secret.forget",
];

/// Why secondary git roots (`git.registerRoot`, `git.unregisterRoot`, a
/// `gitRootId`) are refused on a remote agent.
pub const GIT_ROOTS_MESSAGE: &str = "remote-agent-unsupported: secondary git roots are registered on the backend; ws.git.listRoots lists this agent's submodules at their paths on its machine, so work in them by path (for example git -C <path> through ws.host.exec), not by gitRootId.";

/// Why `ws.app.*` is refused on a remote agent.
const APP_MESSAGE: &str = "remote-agent-unsupported: ws.app.* is for the Assistant workspace, whose agents run on the backend.";

/// Error code at the start of a refused frame's message.
pub const UNSUPPORTED_CODE: &str = "remote-agent-unsupported";

/// Where one frame runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Route {
    /// On the node.
    Local,
    /// On head, as the agent.
    Forward,
    /// Nowhere: refused with [`unsupported_message`].
    Unsupported,
}

/// Route the frame `method` (such as `note.read`).
#[must_use]
pub fn route(method: &str) -> Route {
    if INTERNAL_METHODS.contains(&method) {
        return Route::Unsupported;
    }
    if method.starts_with("help.") || LOCAL_METHODS.contains(&method) {
        return Route::Local;
    }
    if FORWARDED_METHODS.contains(&method) {
        return Route::Forward;
    }
    match method.split_once('.') {
        Some((namespace, rest))
            if !rest.is_empty() && FORWARDED_NAMESPACES.contains(&namespace) =>
        {
            Route::Forward
        }
        _ => Route::Unsupported,
    }
}

/// The lowest negotiated node-link protocol on which `method` is forwarded:
/// saved scripts need the protocol-8 script frames, so on an older link the
/// machine refuses `script.*` itself with [`unsupported_message`].
#[must_use]
pub fn forward_min_protocol(method: &str) -> u32 {
    if method.starts_with("script.") {
        crate::node_link::SCRIPT_PROTOCOL
    } else if method == MCP_PROXY_LIST_TOOLS_METHOD || method == MCP_PROXY_CALL_TOOL_METHOD {
        crate::node_link::MCP_PROXY_PROTOCOL
    } else if method.starts_with("secret.") {
        crate::node_link::SECRETS_PROTOCOL
    } else {
        crate::node_link::NODE_LINK_PROTOCOLS.min
    }
}

/// The error a forwarded `method` fails with on a link older than its
/// [`forward_min_protocol`].
#[must_use]
pub fn forward_refusal(method: &str) -> String {
    let min = forward_min_protocol(method);
    let feature = if method.starts_with("secret.") {
        "agent secrets on the machine"
    } else if min == crate::node_link::MCP_PROXY_PROTOCOL {
        "MCP server proxies"
    } else if min == crate::node_link::SCRIPT_PROTOCOL {
        "saved-script support"
    } else {
        "a newer node link"
    };
    format!(
        "{UNSUPPORTED_CODE}: `{method}` needs {feature} on both this machine and the backend (node-link protocol {min}); update Intent on both"
    )
}

/// The error a refused frame fails with.
#[must_use]
pub fn unsupported_message(method: &str) -> String {
    let namespace = method
        .split_once('.')
        .map_or(method, |(namespace, _)| namespace);
    if namespace == "app" {
        return APP_MESSAGE.to_owned();
    }
    if matches!(method, "git.registerRoot" | "git.unregisterRoot") {
        return GIT_ROOTS_MESSAGE.to_owned();
    }
    format!(
        "{UNSUPPORTED_CODE}: `{method}` is not supported on remote agents yet (ws.{namespace}.* is not available on an execution node). Remote agents can use ws.file, ws.git.commit, ws.host.exec and ws.workspace.info on their own checkout, and ws.{}, ws.app.question.ask, ws.secret (node-link protocol 14), ws.git.listRoots, ws.terminal.list and readOutput, ws.search, and the other ws.workspace calls (details, the setters, proposeSibling, applyProposal, archive, unarchive) through head.",
        FORWARDED_NAMESPACES
            .iter()
            .filter(|namespace| **namespace != "mr")
            .copied()
            .collect::<Vec<_>>()
            .join(", ws.")
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checkout_and_host_calls_run_on_the_node() {
        for method in [
            "file.read",
            "file.write",
            "file.list",
            "file.delete",
            "file.mkdir",
            "file.rename",
            "file.getAttachment",
            "git.commit",
            "host.exec",
            "help.index",
            "workspace.info",
        ] {
            assert_eq!(route(method), Route::Local, "{method}");
        }
    }

    /// Remote agents read their workspace's details and set its title,
    /// status and their own name through head (intent-hq/intent#8593);
    /// routing is per method, so an unknown workspace call stays refused.
    /// They ride `workspace.call` with no protocol gate: an
    /// older machine refuses them itself, an older backend refuses them.
    #[test]
    fn workspace_details_and_setters_are_forwarded_and_the_rest_refused() {
        assert_eq!(route("workspace.details"), Route::Forward);
        for method in [
            "workspace.setTitle",
            "workspace.setStatusMessage",
            "workspace.setStatusImage",
            "workspace.setAgentName",
            "workspace.proposeSibling",
        ] {
            assert_eq!(route(method), Route::Forward, "{method}");
            assert_eq!(
                forward_min_protocol(method),
                crate::node_link::NODE_LINK_PROTOCOLS.min,
                "{method}"
            );
        }
        for method in ["workspace.bogus", "workspace.", "workspace"] {
            assert_eq!(route(method), Route::Unsupported, "{method}");
        }
    }

    /// S2 of the remote-agent parity design: the backend-data namespaces and
    /// the remaining workspace calls are forwarded with no protocol gate (an
    /// older machine refuses them itself, an older backend by its table).
    #[test]
    fn backend_data_calls_are_forwarded_to_head() {
        for method in [
            "primitive.addReference",
            "primitive.addCli",
            "primitive.addPatch",
            "primitive.addAgentAction",
            "discussion.conversations",
            "discussion.send",
            "crossWorkspace.listSiblings",
            "crossWorkspace.readNote",
            "crossWorkspace.search",
            "search.workspace",
            "git.listRoots",
            "terminal.list",
            "terminal.readOutput",
            "workspace.context",
            "workspace.referenceDocs",
            "workspace.timeline",
            "workspace.emitNotification",
            "workspace.applyProposal",
            "workspace.archive",
            "workspace.unarchive",
        ] {
            assert_eq!(route(method), Route::Forward, "{method}");
            assert_eq!(
                forward_min_protocol(method),
                crate::node_link::NODE_LINK_PROTOCOLS.min,
                "{method}"
            );
        }
        // Terminals are read-only for remote agents: only the two reads.
        for method in ["terminal.write", "terminal.create", "terminal.kill"] {
            assert_eq!(route(method), Route::Unsupported, "{method}");
        }
    }

    /// S3 of the remote-agent parity design: hooks, structured questions,
    /// the browser and the desktop run on head as the agent (hooks on head's
    /// runtime, the browser and desktop on the user's client), with no
    /// protocol gate. The rest of `ws.app.*` stays refused.
    #[test]
    fn hooks_questions_browser_and_desktop_are_forwarded_to_head() {
        for method in [
            "hook.schedule",
            "hook.list",
            "hook.get",
            "hook.cancel",
            "hook.runNow",
            "app.question.ask",
            "browser.exec",
            "browser.listTabs",
            "browser.clients",
            "browser.docs",
            "desktop.startControl",
            "desktop.endControl",
            "desktop.listDisplay",
            "desktop.screenshot",
            "desktop.click",
            "mcp.listServers",
            "mcp.listTools",
            "mcp.callTool",
        ] {
            assert_eq!(route(method), Route::Forward, "{method}");
            assert_eq!(
                forward_min_protocol(method),
                crate::node_link::NODE_LINK_PROTOCOLS.min,
                "{method}"
            );
        }
        for method in ["app.question.list", "app.question", "app.agents.list"] {
            assert_eq!(route(method), Route::Unsupported, "{method}");
        }
    }

    /// The machine's internal frames ride `workspace.call` but agent code
    /// cannot send them, even when their namespace is forwarded.
    #[test]
    fn internal_frames_are_refused_from_agent_code() {
        for method in [
            COMMIT_POLICY_METHOD,
            ATTACHMENTS_METHOD,
            PLACEMENT_HELP_METHOD,
            MCP_PROXY_LIST_TOOLS_METHOD,
            MCP_PROXY_CALL_TOOL_METHOD,
        ] {
            assert!(INTERNAL_METHODS.contains(&method), "{method}");
            assert_eq!(route(method), Route::Unsupported, "{method}");
        }
        assert_eq!(PLACEMENT_HELP_METHOD, "agent.placementHelp");
        // The provider's proxied MCP servers ride `mcp.proxy.*`, inside the
        // forwarded `mcp` namespace, and only on nodeProtocol 11.
        assert_eq!(MCP_PROXY_LIST_TOOLS_METHOD, "mcp.proxy.listTools");
        assert_eq!(MCP_PROXY_CALL_TOOL_METHOD, "mcp.proxy.callTool");
        for method in [MCP_PROXY_LIST_TOOLS_METHOD, MCP_PROXY_CALL_TOOL_METHOD] {
            assert_eq!(
                forward_min_protocol(method),
                crate::node_link::MCP_PROXY_PROTOCOL,
                "{method}"
            );
        }
        assert!(MCP_PROXY_LINK_TIMEOUT > MCP_PROXY_CALL_TIMEOUT);
    }

    /// `app.*` and secondary git roots say why they are refused.
    #[test]
    fn app_and_git_root_refusals_explain_why() {
        for method in ["app.agents.list", "app.workspaces.list"] {
            assert_eq!(route(method), Route::Unsupported, "{method}");
            assert_eq!(
                unsupported_message(method),
                "remote-agent-unsupported: ws.app.* is for the Assistant workspace, whose agents run on the backend."
            );
        }
        for method in ["git.registerRoot", "git.unregisterRoot"] {
            assert_eq!(route(method), Route::Unsupported, "{method}");
            assert_eq!(unsupported_message(method), GIT_ROOTS_MESSAGE);
        }
        assert_eq!(
            GIT_ROOTS_MESSAGE,
            "remote-agent-unsupported: secondary git roots are registered on the backend; ws.git.listRoots lists this agent's submodules at their paths on its machine, so work in them by path (for example git -C <path> through ws.host.exec), not by gitRootId."
        );
    }

    #[test]
    fn the_control_plane_allowlist_is_forwarded_to_head() {
        for method in [
            "note.read",
            "task.updateStatus",
            "agent.create",
            "pr.snapshot",
            "mr.snapshot",
            "comment.add",
            "event.subscribe",
            "script.create",
            "script.run",
            "script.monitor",
        ] {
            assert_eq!(route(method), Route::Forward, "{method}");
        }
    }

    #[test]
    fn everything_else_is_refused_by_name() {
        for method in [
            "search.query",
            "secret.provide",
            "secret.dismiss",
            COMMIT_POLICY_METHOD,
            ATTACHMENTS_METHOD,
            ATTACHMENT_DATA_METHOD,
            "note",
            "note.",
            "",
        ] {
            assert_eq!(route(method), Route::Unsupported, "{method:?}");
        }
        let message = unsupported_message("search.query");
        assert!(
            message.starts_with(
                "remote-agent-unsupported: `search.query` is not supported on remote agents yet"
            ),
            "{message}"
        );
        assert!(message.contains("ws.search.* is"), "{message}");
        for forwarded in [
            "ws.script",
            "ws.primitive",
            "ws.discussion",
            "ws.crossWorkspace",
            "ws.hook",
            "ws.browser",
            "ws.desktop",
            "ws.mcp",
            "ws.app.question.ask",
            "ws.git.listRoots",
            "ws.terminal.list",
            "ws.secret",
            "ws.workspace calls (details",
            "archive",
        ] {
            assert!(message.contains(forwarded), "{forwarded}: {message}");
        }
    }

    /// S5 of the remote-agent parity design: a remote agent's secret
    /// requests, list and forget run on head as the agent (the values stay
    /// on its machine), only on a nodeProtocol 14 link; the refusal on an
    /// older one asks to update Intent on both.
    #[test]
    fn secret_calls_are_forwarded_on_protocol_14() {
        for method in ["secret.request", "secret.list", "secret.forget"] {
            assert_eq!(route(method), Route::Forward, "{method}");
            assert_eq!(
                forward_min_protocol(method),
                crate::node_link::SECRETS_PROTOCOL,
                "{method}"
            );
            let refused = forward_refusal(method);
            assert!(
                refused.starts_with("remote-agent-unsupported: `"),
                "{refused}"
            );
            assert!(refused.contains("node-link protocol 14"), "{refused}");
            assert!(refused.contains("secrets"), "{refused}");
        }
        assert!(forward_refusal("script.run").contains("saved-script"));
    }
}
