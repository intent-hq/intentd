//! Large-note regression tests for `ws.note.*` through the real `workspace_api`
//! JS engine (intent-hq/intent#7688). The fake keeps one note in memory so a
//! test can verify the persisted state after a mutation, independent of what
//! the script observed.

#![cfg(test)]

use std::sync::{Arc, Mutex};

use intent_core::{
    AgentId, BoxFuture, Error, Note, NoteAddInput, NoteAddResult, NoteEditInput,
    NoteEditLinesInput, NoteEditLinesResult, NoteEditResult, NoteId, NoteMetadata, Result,
    WorkspaceApi, WorkspaceId,
};
use serde_json::{json, Value};

use crate::WorkspaceMcpServer;

/// One stored note whose content each mutation really rewrites.
struct StoreApi {
    content: Mutex<String>,
}

impl StoreApi {
    fn new(content: String) -> Arc<Self> {
        Arc::new(Self {
            content: Mutex::new(content),
        })
    }

    fn content(&self) -> String {
        self.content.lock().unwrap().clone()
    }
}

fn note(ws: WorkspaceId, id: NoteId, content: String) -> Note {
    Note {
        id,
        workspace_id: ws,
        title: "big".to_string(),
        content,
        content_type: intent_core::ContentType::default(),
        tags: Vec::new(),
        is_pinned: false,
        is_archived: false,
        is_default: false,
        parent_id: None,
        visibility: intent_core::NoteVisibility::default(),
        metadata: NoteMetadata { task: None },
        created_at: "2026-01-01T00:00:00Z".to_string(),
        rev: 1,
        updated_at: "2026-01-01T00:00:00Z".to_string(),
    }
}

impl WorkspaceApi for StoreApi {
    fn settings_get(&self, path: String) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            let value = match path.as_str() {
                "workspaceApi.toonOutput" => json!(false),
                "workspaceApi.maxOutputChars" => json!(0),
                _ => Value::Null,
            };
            Ok(json!({ "path": path, "value": value }))
        })
    }

    fn get_note(&self, ws: WorkspaceId, note_id: NoteId) -> BoxFuture<'_, Result<Note>> {
        let content = self.content();
        Box::pin(async move { Ok(note(ws, note_id, content)) })
    }

    fn add_to_note(
        &self,
        _ws: WorkspaceId,
        note_id: NoteId,
        input: NoteAddInput,
        _caller: Option<AgentId>,
    ) -> BoxFuture<'_, Result<NoteAddResult>> {
        let mut guard = self.content.lock().unwrap();
        let old_content = guard.clone();
        guard.push_str(&input.content);
        let new_content = guard.clone();
        drop(guard);
        Box::pin(async move {
            Ok(NoteAddResult {
                ok: true,
                note_id,
                added_length: input.content.len(),
                total_length: new_content.len(),
                position: "at end".to_string(),
                old_content,
                new_content,
                converted_count: 0,
                created_task_note_ids: Vec::new(),
                created_tasks: Vec::new(),
                warnings: Vec::new(),
            })
        })
    }

    fn edit_note(
        &self,
        _ws: WorkspaceId,
        note_id: NoteId,
        input: NoteEditInput,
        _caller: Option<AgentId>,
    ) -> BoxFuture<'_, Result<NoteEditResult>> {
        let mut guard = self.content.lock().unwrap();
        let old_content = guard.clone();
        let Some(pos) = old_content.find(&input.old) else {
            return Box::pin(async { Err(Error::InvalidInput("no match".to_string())) });
        };
        *guard = old_content.replacen(&input.old, &input.new, 1);
        let new_content = guard.clone();
        drop(guard);
        Box::pin(async move {
            Ok(NoteEditResult {
                ok: true,
                note_id,
                old_text_length: input.old.len(),
                new_text_length: input.new.len(),
                match_position: i64::try_from(pos).unwrap(),
                old_content,
                new_content,
                converted_count: 0,
                created_task_note_ids: Vec::new(),
                created_tasks: Vec::new(),
                warnings: Vec::new(),
            })
        })
    }

    /// Same 1-based inclusive `split('\n')` numbering as the service's
    /// `apply_edit_lines`, so a bounded read can be checked against it.
    fn edit_note_lines(
        &self,
        _ws: WorkspaceId,
        note_id: NoteId,
        input: NoteEditLinesInput,
        _caller: Option<AgentId>,
    ) -> BoxFuture<'_, Result<NoteEditLinesResult>> {
        let mut guard = self.content.lock().unwrap();
        let old_content = guard.clone();
        let mut lines: Vec<&str> = old_content.split('\n').collect();
        let total_lines_before = lines.len();
        let start = usize::try_from(input.start).unwrap();
        let end = usize::try_from(input.end).unwrap();
        lines.splice(start - 1..end, std::iter::once(input.content.as_str()));
        let new_content = lines.join("\n");
        let total_lines_after = new_content.split('\n').count();
        guard.clone_from(&new_content);
        drop(guard);
        Box::pin(async move {
            Ok(NoteEditLinesResult {
                ok: true,
                note_id,
                start_line: input.start,
                end_line: input.end,
                total_lines_before,
                total_lines_after,
                old_content,
                new_content,
                converted_count: 0,
                created_task_note_ids: Vec::new(),
                created_tasks: Vec::new(),
                warnings: Vec::new(),
            })
        })
    }
}

async fn call(srv: &WorkspaceMcpServer, code: &str) -> (bool, String) {
    let resp = srv
        .handle_message(&json!({
            "jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": {
                "name": "workspace_api",
                "arguments": { "code": code, "summary": "large note test" }
            }
        }))
        .await
        .expect("tools/call must produce a response");
    let is_error = resp["result"]["isError"].as_bool().unwrap_or(false);
    let text = resp["result"]["content"][0]["text"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    (is_error, text)
}

/// A note of `lines` copies of `line` joined by newlines. Non-ASCII lines force `QuickJS` into 16-bit
/// strings, the widest representation per character.
fn multiline(lines: usize, line: &str) -> String {
    let mut s = String::with_capacity(lines * (line.len() + 1));
    for i in 0..lines {
        if i > 0 {
            s.push('\n');
        }
        s.push_str(line);
    }
    s
}

fn server(content: String) -> (WorkspaceMcpServer, Arc<StoreApi>) {
    let api = StoreApi::new(content);
    let srv = WorkspaceMcpServer::new(api.clone(), WorkspaceId::from_string("ws"));
    (srv, api)
}

/// 1.5M short non-ASCII lines (~3M UTF-16 units): the smallest shape the
/// size sweep found to exhaust the 64 MiB script heap on `ws.note.read`
/// when the reply was marshalled as JSON text and re-parsed in JS.
#[tokio::test]
async fn read_of_large_multiline_note_supports_a_tiny_projection() {
    let (srv, _api) = server(multiline(1_500_000, "é"));
    let (is_error, text) = call(
        &srv,
        "const n = await ws.note.read('big'); \
         return { lines: n.totalLines, raw: n.rawContent.length, head: n.content.slice(0, 9) };",
    )
    .await;
    assert!(
        !is_error,
        "read must not fail: {}",
        &text[..text.len().min(200)]
    );
    let body: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(body["lines"], json!(1_500_000));
    assert_eq!(body["raw"], json!(2_999_999));
    assert_eq!(body["head"], json!("   1 | é\n"));
}

/// 4M short non-ASCII lines (~8M UTF-16 units): the edit commits, then the
/// echoed `oldContent` + `newContent` used to exhaust the script heap so the
/// caller saw "out of memory" for a write that had happened.
#[tokio::test]
async fn committed_edit_on_huge_note_reports_success_with_bounded_result() {
    let (srv, api) = server(multiline(4_000_000, "é"));
    let (is_error, text) = call(
        &srv,
        "return await ws.note.edit('big', { old: 'é', new: 'EDITED' });",
    )
    .await;
    assert!(
        api.content().starts_with("EDITED\né\n"),
        "edit must be persisted"
    );
    assert!(
        !is_error,
        "a committed edit must not surface as an error: {text}"
    );
    let body: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(body["ok"], json!(true));
    assert_eq!(body["noteId"], json!("big"));
    assert_eq!(body["contentOmitted"], json!(true));
    assert!(body.get("oldContent").is_none() && body.get("newContent").is_none());
    assert!(
        text.len() < 4096,
        "result must be bounded, got {} bytes",
        text.len()
    );
}

#[tokio::test]
async fn committed_add_on_huge_note_reports_success_with_bounded_result() {
    let (srv, api) = server(multiline(4_000_000, "é"));
    let (is_error, text) = call(
        &srv,
        "return await ws.note.add('big', { content: '\\nTAIL' });",
    )
    .await;
    assert!(api.content().ends_with("é\nTAIL"), "add must be persisted");
    assert!(
        !is_error,
        "a committed add must not surface as an error: {text}"
    );
    let body: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(body["ok"], json!(true));
    assert_eq!(body["contentOmitted"], json!(true));
    assert!(
        text.len() < 4096,
        "result must be bounded, got {} bytes",
        text.len()
    );
}

/// Ordinary notes keep the full echo agents already consume.
#[tokio::test]
async fn small_note_mutations_keep_the_content_echo() {
    let (srv, _api) = server("alpha\nbeta".to_string());
    let (is_error, text) = call(
        &srv,
        "return await ws.note.edit('big', { old: 'beta', new: 'gamma' });",
    )
    .await;
    assert!(!is_error, "{text}");
    let body: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(body["oldContent"], json!("alpha\nbeta"));
    assert_eq!(body["newContent"], json!("alpha\ngamma"));
    assert!(body.get("contentOmitted").is_none());
}

/// `L1\nL2\n…\nL{lines}` — every line names its own 1-based number.
fn numbered_lines(lines: usize) -> String {
    let mut s = String::with_capacity(lines * 9);
    for i in 1..=lines {
        if i > 1 {
            s.push('\n');
        }
        s.push('L');
        s.push_str(&i.to_string());
    }
    s
}

/// A line-bounded read of a huge note returns only the requested lines, so
/// the script never materializes the whole note.
#[tokio::test]
async fn bounded_read_of_huge_note_returns_only_the_requested_lines() {
    let (srv, _api) = server(numbered_lines(1_500_000));
    let (is_error, text) = call(
        &srv,
        "return await ws.note.read('big', { startLine: 1000, maxLines: 3 });",
    )
    .await;
    assert!(!is_error, "{}", &text[..text.len().min(200)]);
    assert!(
        text.len() < 4096,
        "bounded read must not carry the whole note, got {} bytes",
        text.len()
    );
    let body: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(body["rawContent"], json!("L1000\nL1001\nL1002"));
    assert_eq!(
        body["content"],
        json!("1000 | L1000\n1001 | L1001\n1002 | L1002")
    );
    assert_eq!(body["totalLines"], json!(1_500_000));
    assert_eq!(body["startLine"], json!(1000));
    assert_eq!(body["endLine"], json!(1002));
    assert_eq!(body["truncated"], json!(true));
    assert_eq!(body["rev"], json!(1));
    assert_eq!(body["updatedAt"], json!("2026-01-01T00:00:00Z"));
    assert_eq!(body["id"], json!("big"));
    assert_eq!(body["title"], json!("big"));
}

/// The last page of a huge note: `maxLines` past the end stops at the last line.
#[tokio::test]
async fn bounded_read_of_huge_note_tail_stops_at_the_last_line() {
    let (srv, _api) = server(numbered_lines(1_500_000));
    let (is_error, text) = call(
        &srv,
        "const n = await ws.note.read('big', { startLine: 1499999, maxLines: 50 }); \
         return { raw: n.rawContent, start: n.startLine, end: n.endLine, truncated: n.truncated };",
    )
    .await;
    assert!(!is_error, "{text}");
    let body: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(body["raw"], json!("L1499999\nL1500000"));
    assert_eq!(body["start"], json!(1_499_999));
    assert_eq!(body["end"], json!(1_500_000));
    assert_eq!(body["truncated"], json!(true));
}

#[tokio::test]
async fn bounded_read_covering_the_whole_note_is_not_truncated() {
    let (srv, _api) = server("a\nb\nc".to_string());
    for opts in [
        "{ startLine: 1, maxLines: 10 }",
        "{ maxLines: 3 }",
        "{ startLine: 1 }",
    ] {
        let (is_error, text) =
            call(&srv, &format!("return await ws.note.read('big', {opts});")).await;
        assert!(!is_error, "{opts}: {text}");
        let body: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(body["rawContent"], json!("a\nb\nc"), "{opts}");
        assert_eq!(
            body["content"],
            json!("   1 | a\n   2 | b\n   3 | c"),
            "{opts}"
        );
        assert_eq!(body["totalLines"], json!(3), "{opts}");
        assert_eq!(body["startLine"], json!(1), "{opts}");
        assert_eq!(body["endLine"], json!(3), "{opts}");
        assert_eq!(body["truncated"], json!(false), "{opts}");
    }
    let (is_error, text) = call(&srv, "return await ws.note.read('big', { startLine: 2 });").await;
    assert!(!is_error, "{text}");
    let body: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(body["rawContent"], json!("b\nc"));
    assert_eq!(body["startLine"], json!(2));
    assert_eq!(body["endLine"], json!(3));
    assert_eq!(body["truncated"], json!(true));
}

/// Without options (or with an empty / nullish options object) the result
/// keeps its existing shape: no paging fields.
#[tokio::test]
async fn unbounded_read_keeps_its_existing_shape() {
    let (srv, _api) = server("a\nb".to_string());
    for call_expr in [
        "ws.note.read('big')",
        "ws.note.read('big', {})",
        "ws.note.read('big', { startLine: null, maxLines: undefined })",
    ] {
        let (is_error, text) = call(&srv, &format!("return await {call_expr};")).await;
        assert!(!is_error, "{call_expr}: {text}");
        let body: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(
            body,
            json!({
                "id": "big",
                "title": "big",
                "tags": [],
                "content": "   1 | a\n   2 | b",
                "rawContent": "a\nb",
                "totalLines": 2,
                "imageCount": 0,
                "images": [],
            }),
            "{call_expr}"
        );
    }
}

/// A page's line numbers address the same lines `ws.note.editLines` edits.
#[tokio::test]
async fn bounded_read_line_numbers_line_up_with_edit_lines() {
    let (srv, api) = server("a\nb\nc\nd".to_string());
    let (is_error, text) = call(
        &srv,
        "const n = await ws.note.read('big', { startLine: 3, maxLines: 1 }); \
         await ws.note.editLines('big', { start: n.startLine, end: n.endLine, content: n.rawContent.toUpperCase() }); \
         return n.rawContent;",
    )
    .await;
    assert!(!is_error, "{text}");
    assert_eq!(text, "\"c\"");
    assert_eq!(api.content(), "a\nb\nC\nd");
}

#[tokio::test]
async fn bounded_read_of_empty_note_returns_an_empty_page() {
    let (srv, _api) = server(String::new());
    let (is_error, text) = call(
        &srv,
        "return await ws.note.read('big', { startLine: 1, maxLines: 5 });",
    )
    .await;
    assert!(!is_error, "{text}");
    let body: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(body["rawContent"], json!(""));
    assert_eq!(body["content"], json!(""));
    assert_eq!(body["totalLines"], json!(0));
    assert_eq!(body["startLine"], json!(1));
    assert_eq!(body["endLine"], json!(0));
    assert_eq!(body["truncated"], json!(false));
}

#[tokio::test]
async fn bounded_read_rejects_invalid_options() {
    let (srv, _api) = server("a\nb\nc".to_string());
    for (opts, expected) in [
        ("{ startLine: 0 }", "startLine must be a positive integer"),
        ("{ startLine: -2 }", "startLine must be a positive integer"),
        ("{ startLine: 1.5 }", "startLine must be a positive integer"),
        ("{ startLine: 'x' }", "startLine must be a positive integer"),
        (
            "{ startLine: true }",
            "startLine must be a positive integer",
        ),
        ("{ maxLines: 0 }", "maxLines must be a positive integer"),
        ("{ maxLines: -1 }", "maxLines must be a positive integer"),
        ("{ maxLines: 2.5 }", "maxLines must be a positive integer"),
        (
            "{ startLine: 4 }",
            "startLine (4) exceeds total lines in note (3)",
        ),
    ] {
        let (is_error, text) =
            call(&srv, &format!("return await ws.note.read('big', {opts});")).await;
        assert!(is_error, "{opts} must be rejected, got: {text}");
        assert!(text.contains(expected), "{opts}: {text}");
    }
    let (srv, _api) = server(String::new());
    let (is_error, text) = call(&srv, "return await ws.note.read('big', { startLine: 2 });").await;
    assert!(
        is_error,
        "startLine past an empty note must be rejected: {text}"
    );
    assert!(
        text.contains("startLine (2) exceeds total lines in note (0)"),
        "{text}"
    );
}
