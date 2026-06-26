//! Pure builders for the `primitive.*` methods (PROTOCOL §5.x).
//!
//! Ports the TS `appendPrimitiveBlock` plus the four primitive constructors in
//! `ws-note-api.ts`: each method builds a versioned primitive object and appends
//! it to the target note as a fenced ```ws-block:<type>``` JSON block. Every
//! primitive carries `version: 1`, `createdBy: "agent"`, a uuid `id`, and an ISO
//! `createdAt`; the store glue lives in `lib.rs`.

use serde_json::{json, Value};

/// `createdBy` marker on every agent-authored primitive (TS literal).
const CREATED_BY: &str = "agent";

/// `primitive.addReference`: `target.kind` is `symbol` when the semantic id
/// names a symbol (`#symbol:`), else `file_range`; `snapshot` is only present
/// when caller code is supplied, with `filePath` = the pre-`#` portion.
pub(crate) fn reference(
    id: &str,
    created_at: &str,
    semantic_id: &str,
    description: &str,
    snapshot: Option<&str>,
) -> Value {
    let kind = if semantic_id.contains("#symbol:") {
        "symbol"
    } else {
        "file_range"
    };
    let mut primitive = json!({
        "id": id,
        "version": 1,
        "type": "reference",
        "createdAt": created_at,
        "createdBy": CREATED_BY,
        "target": { "kind": kind, "semanticId": semantic_id },
        "description": description,
    });
    if let Some(code) = snapshot {
        let file_path = semantic_id.split('#').next().unwrap_or(semantic_id);
        primitive["snapshot"] = json!({
            "code": code,
            "filePath": file_path,
            "language": "typescript",
        });
    }
    primitive
}

/// `primitive.addCli`: `cwd` falls back to `"./"` when no working directory is
/// supplied (JS `workingDirectory || './'`, so empty strings fall back too).
pub(crate) fn cli(
    id: &str,
    created_at: &str,
    command: &str,
    description: &str,
    working_directory: Option<&str>,
) -> Value {
    let cwd = working_directory.filter(|d| !d.is_empty()).unwrap_or("./");
    json!({
        "id": id,
        "version": 1,
        "type": "cli",
        "createdAt": created_at,
        "createdBy": CREATED_BY,
        "command": command,
        "description": description,
        "cwd": cwd,
        "display": { "showCommandPrefix": "$" },
    })
}

/// `primitive.addPatch`: a single-entry `patches` array of `{ filePath, diff }`.
pub(crate) fn patch(
    id: &str,
    created_at: &str,
    file_path: &str,
    diff: &str,
    description: &str,
) -> Value {
    json!({
        "id": id,
        "version": 1,
        "type": "patch",
        "createdAt": created_at,
        "createdBy": CREATED_BY,
        "description": description,
        "patches": [{ "filePath": file_path, "diff": diff }],
    })
}

/// `primitive.addAgentAction`: a triggerable action with empty `inputs`.
pub(crate) fn agent_action(
    id: &str,
    created_at: &str,
    agent_id: &str,
    goal: &str,
    description: &str,
) -> Value {
    json!({
        "id": id,
        "version": 1,
        "type": "agent_action",
        "createdAt": created_at,
        "createdBy": CREATED_BY,
        "agentId": agent_id,
        "goal": goal,
        "description": description,
        "inputs": [],
    })
}

/// Append `primitive` to `content` as a fenced ```ws-block:<block_type>``` JSON
/// block, mirroring the TS `appendPrimitiveBlock` concatenation exactly
/// (`\n\n` + fence + 2-space-pretty JSON + fence + `\n`).
pub(crate) fn append_block(content: &str, primitive: &Value, block_type: &str) -> String {
    let body = serde_json::to_string_pretty(primitive).unwrap_or_default();
    format!("{content}\n\n```ws-block:{block_type}\n{body}\n```\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reference_kind_switches_on_symbol_and_emits_snapshot() {
        let file = reference("p1", "2026-01-01T00:00:00.000Z", "src/a.ts#L1-5", "d", None);
        assert_eq!(file["type"], "reference");
        assert_eq!(file["version"], 1);
        assert_eq!(file["createdBy"], "agent");
        assert_eq!(file["target"]["kind"], "file_range");
        assert_eq!(file["target"]["semanticId"], "src/a.ts#L1-5");
        assert!(file.get("snapshot").is_none());

        let sym = reference(
            "p2",
            "2026-01-01T00:00:00.000Z",
            "src/a.ts#symbol:Foo",
            "d",
            Some("fn foo() {}"),
        );
        assert_eq!(sym["target"]["kind"], "symbol");
        assert_eq!(sym["snapshot"]["code"], "fn foo() {}");
        assert_eq!(sym["snapshot"]["filePath"], "src/a.ts");
        assert_eq!(sym["snapshot"]["language"], "typescript");
    }

    #[test]
    fn cli_defaults_cwd_and_sets_prefix() {
        let c = cli("p", "t", "ls -la", "list", None);
        assert_eq!(c["type"], "cli");
        assert_eq!(c["command"], "ls -la");
        assert_eq!(c["cwd"], "./");
        assert_eq!(c["display"]["showCommandPrefix"], "$");
        let c2 = cli("p", "t", "ls", "d", Some("sub/dir"));
        assert_eq!(c2["cwd"], "sub/dir");
        let c3 = cli("p", "t", "ls", "d", Some(""));
        assert_eq!(c3["cwd"], "./");
    }

    #[test]
    fn patch_and_agent_action_shapes() {
        let p = patch("p", "t", "src/a.ts", "@@ -1 +1 @@", "fix");
        assert_eq!(p["type"], "patch");
        assert_eq!(p["patches"][0]["filePath"], "src/a.ts");
        assert_eq!(p["patches"][0]["diff"], "@@ -1 +1 @@");

        let a = agent_action("p", "t", "agent-1", "do it", "desc");
        assert_eq!(a["type"], "agent_action");
        assert_eq!(a["agentId"], "agent-1");
        assert_eq!(a["goal"], "do it");
        assert_eq!(a["inputs"], json!([]));
    }

    #[test]
    fn append_block_wraps_with_fences_and_parses() {
        let prim = cli("id-1", "t", "echo hi", "say hi", None);
        let out = append_block("# Note", &prim, "cli");
        assert!(out.starts_with("# Note\n\n```ws-block:cli\n"));
        assert!(out.ends_with("\n```\n"));
        let json_body = out
            .split("```ws-block:cli\n")
            .nth(1)
            .unwrap()
            .rsplit_once("\n```\n")
            .unwrap()
            .0;
        let parsed: Value = serde_json::from_str(json_body).unwrap();
        assert_eq!(parsed, prim);
    }
}
