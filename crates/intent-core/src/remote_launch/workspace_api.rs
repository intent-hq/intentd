//! Where a remote agent's `workspace_api` calls run.
//!
//! A remote agent evaluates its `workspace_api` code on its execution node.
//! Each `host({ method, args })` frame is routed by its method: calls on the
//! agent's own checkout and host run on the node, a fixed set of control-plane
//! namespaces is forwarded to head over the node link (head checks the agent
//! is on the link's lease and routes the frame again, never trusting the
//! node), and everything else is refused with an explicit error instead of
//! running on the wrong host.

/// The link request method that carries one forwarded frame. Its params are
/// `{ "method": <frame method>, "args": <frame args> }`.
pub const CALL_METHOD: &str = "workspace.call";

/// Namespaces forwarded to head.
pub const FORWARDED_NAMESPACES: &[&str] =
    &["note", "task", "agent", "pr", "mr", "comment", "event"];

/// Methods that run on the node, against the agent's checkout.
pub const LOCAL_METHODS: &[&str] = &[
    "file.read",
    "file.write",
    "file.list",
    "file.delete",
    "file.mkdir",
    "file.rename",
    "git.commit",
    "host.exec",
];

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
    if method.starts_with("help.") || LOCAL_METHODS.contains(&method) {
        return Route::Local;
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

/// The error a refused frame fails with.
#[must_use]
pub fn unsupported_message(method: &str) -> String {
    let namespace = method
        .split_once('.')
        .map_or(method, |(namespace, _)| namespace);
    let what = if method == "file.getAttachment" {
        "attachments are".to_owned()
    } else {
        format!("ws.{namespace}.* is")
    };
    format!(
        "{UNSUPPORTED_CODE}: `{method}` is not supported on remote agents yet ({what} not available on an execution node). Remote agents can use ws.file, ws.git.commit and ws.host.exec on their own checkout, and ws.{} through head.",
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
            "git.commit",
            "host.exec",
            "help.index",
        ] {
            assert_eq!(route(method), Route::Local, "{method}");
        }
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
        ] {
            assert_eq!(route(method), Route::Forward, "{method}");
        }
    }

    #[test]
    fn everything_else_is_refused_by_name() {
        for method in [
            "script.list",
            "terminal.list",
            "browser.exec",
            "hook.schedule",
            "desktop.screenshot",
            "workspace.info",
            "mcp.listTools",
            "file.getAttachment",
            "git.registerRoot",
            "note",
            "note.",
            "",
        ] {
            assert_eq!(route(method), Route::Unsupported, "{method:?}");
        }
        let message = unsupported_message("script.list");
        assert!(
            message.starts_with(
                "remote-agent-unsupported: `script.list` is not supported on remote agents yet"
            ),
            "{message}"
        );
        assert!(message.contains("ws.script.*"), "{message}");
        assert!(unsupported_message("file.getAttachment").contains("attachments are"));
    }
}
