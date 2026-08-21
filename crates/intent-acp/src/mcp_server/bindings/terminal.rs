//! `ws.terminal.*` bindings (WSAPI-5).
//!
//! Read-only helpers exposed to agent code: `list` returns the workspace's
//! live PTY sessions and `readOutput` snapshots a terminal's ANSI-stripped
//! scrollback. Mirrors the reference `buildTerminalApi` in `ws-misc-api.ts`.

use std::sync::Arc;

use intent_core::{WorkspaceApi, WorkspaceId};
use serde_json::Value;

use super::{map_err, opt_i64, req_str};

pub(crate) const PRELUDE: &str = r"
    globalThis.ws = globalThis.ws || {};
    ws.terminal = {
        list: () => host({ method: 'terminal.list' }),
        readOutput: (terminalId, maxLines) =>
            host({ method: 'terminal.readOutput', args: { terminalId, maxLines } }),
    };
";

pub(crate) async fn dispatch(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    method: &str,
    args: &Value,
) -> Result<Value, String> {
    match method {
        "list" => {
            // The wire method returns the `{ terminals, daemonBootId }`
            // envelope (PROTOCOL §5.13; monorepo#1334); agents keep the bare
            // terminals array, so unwrap here to leave the agent-visible
            // contract unchanged.
            let mut listed = api.terminal_list(ws.clone()).await.map_err(map_err)?;
            let terminals = listed
                .as_object_mut()
                .and_then(|o| o.remove("terminals"))
                .unwrap_or(listed);
            Ok(terminals)
        }
        "readOutput" => {
            let terminal_id =
                req_str(args, "terminalId").map_err(|_| "terminalId is required".to_string())?;
            let max_lines = opt_i64(args, "maxLines");
            api.terminal_read_output(ws.clone(), terminal_id, max_lines, None, None)
                .await
                .map_err(map_err)
        }
        other => Err(format!("host: unknown method `terminal.{other}`")),
    }
}
