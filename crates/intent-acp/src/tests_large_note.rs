//! Large-note regression tests for `ws.note.*` through the real `workspace_api`
//! JS engine (intent-hq/intent#7688). The fake keeps one note in memory so a
//! test can verify the persisted state after a mutation, independent of what
//! the script observed.

#![cfg(test)]

use std::sync::{Arc, Mutex};

use intent_core::{
    AgentId, BoxFuture, Error, Note, NoteAddInput, NoteAddResult, NoteEditInput, NoteEditResult,
    NoteId, NoteMetadata, Result, WorkspaceApi, WorkspaceId,
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
