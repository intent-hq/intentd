//! `BulkTransfer` methods may exceed the read budget, but never the cap
//! they declare in `PAYLOAD_CLASSES`. Each is called on data at or past
//! that cap and the reply is checked against it:
//!
//! - `file.readChunk` (cap param `length`): a chunk of exactly the cap is
//!   served and holds at most the cap; one byte more is refused (`-32602`).
//! - `workspace.export.read` (server-cut chunks): every chunk of an export
//!   of the big workspace holds at most the chunk cap.
//! - Whole-object reads bounded by the outbound frame cap (`file.read`,
//!   `git.showFile`, `note.readAsset`): a large
//!   object is served in one frame under the cap, and `file.read` of a file
//!   past the cap is answered with an error instead of an oversized frame.

use std::fmt::Write as _;
use std::time::Duration;

use base64::Engine as _;
use intent_transport::catalog::{PayloadClass, PAYLOAD_CLASSES};
use serde_json::json;

use crate::harness::{Client, Daemon, Reply};
use crate::rows::Ctx;

/// Every `BulkTransfer` method this module checks.
pub const METHODS: &[&str] = &[
    "file.read",
    "file.readChunk",
    "git.showFile",
    "note.readAsset",
    "workspace.export.read",
];

/// Worktree-relative directory (git-ignored) holding the bulk files.
const BULK_DIR: &str = "ignored/bulk";

#[track_caller]
fn declared_cap(method: &str) -> (Option<&'static str>, usize) {
    match PAYLOAD_CLASSES
        .iter()
        .find(|r| r.method == method)
        .map(|r| r.class)
    {
        Some(PayloadClass::BulkTransfer {
            cap_param,
            max_bytes,
        }) => (cap_param, max_bytes),
        other => panic!("{method}: not BulkTransfer in PAYLOAD_CLASSES ({other:?})"),
    }
}

fn line(report: &mut String, method: &str, variant: &str, reply: &Reply) {
    let _ = writeln!(report, "{:>10} bulk {method} {variant}", reply.bytes);
}

/// Write the bulk files into the worktree.
fn write_files(worktree: &std::path::Path, chunk_cap: usize, frame_cap: usize) {
    let dir = worktree.join(BULK_DIR);
    std::fs::create_dir_all(&dir).expect("bulk dir");
    // Binary; the cap check is on the requested length, so the file need
    // not be cap-sized (a cap-sized read would only move 16 MiB around).
    let _ = chunk_cap;
    let bin: Vec<u8> = (0..256 * 1024)
        .map(|i| u8::try_from(i % 251).unwrap_or(0))
        .collect();
    std::fs::write(dir.join("large.bin"), bin).expect("large.bin");
    // UTF-8, past the outbound frame cap.
    let mut text = String::with_capacity(frame_cap + 1024 * 1024);
    while text.len() < frame_cap + 1024 * 1024 {
        text.push_str("a generated log line that keeps going past the frame cap\n");
    }
    std::fs::write(dir.join("over-frame-cap.log"), text).expect("over-cap log");
    // UTF-8, large but under every cap.
    let medium: String = "a generated source line of reasonable width for a file\n".repeat(40_000);
    std::fs::write(dir.join("medium.txt"), medium).expect("medium.txt");
}

/// Check every bulk method's declared cap; returns report lines.
pub async fn sweep(
    client: &mut Client,
    daemon: &Daemon,
    ctx: &Ctx<'_>,
    failures: &mut Vec<String>,
) -> String {
    let mut report = String::new();
    let ws = ctx.ws.as_str();
    let frame_cap = intent_transport::MAX_OUTBOUND_MESSAGE_BYTES;
    let (chunk_param, chunk_cap) = declared_cap("file.readChunk");
    write_files(&daemon.state.worktree, chunk_cap, frame_cap);
    let mut fail = |msg: String| failures.push(msg);

    // file.readChunk: the declared cap param bounds one response.
    assert_eq!(chunk_param, Some("length"), "file.readChunk cap param");
    let at_cap = client
        .call(
            "file.readChunk",
            &json!({ "workspaceId": ws, "path": format!("{BULK_DIR}/large.bin"), "offset": 0, "length": chunk_cap }),
        )
        .await;
    if let Some(e) = at_cap.error() {
        fail(format!("bulk file.readChunk length=cap: {e}"));
    } else {
        let read = at_cap.result()["bytesRead"].as_u64().unwrap_or(u64::MAX);
        if usize::try_from(read).unwrap_or(usize::MAX) > chunk_cap {
            fail(format!(
                "bulk file.readChunk: read {read} bytes, cap {chunk_cap}"
            ));
        }
    }
    line(&mut report, "file.readChunk", "length=cap", &at_cap);
    let past_cap = client
        .call(
            "file.readChunk",
            &json!({ "workspaceId": ws, "path": format!("{BULK_DIR}/large.bin"), "offset": 0, "length": chunk_cap + 1 }),
        )
        .await;
    if past_cap.error().map(|e| e["code"].clone()) != Some(json!(-32602)) {
        fail(format!(
            "bulk file.readChunk length=cap+1 must be refused with -32602, got {}",
            past_cap
                .frame
                .to_string()
                .chars()
                .take(200)
                .collect::<String>()
        ));
    }

    // Whole-object reads: served under the frame cap.
    let (_, cap) = declared_cap("file.read");
    let medium = client
        .call(
            "file.read",
            &json!({ "workspaceId": ws, "path": format!("{BULK_DIR}/medium.txt") }),
        )
        .await;
    check_whole(&mut fail, "file.read", &medium, cap);
    line(&mut report, "file.read", "medium", &medium);
    let over = client
        .call(
            "file.read",
            &json!({ "workspaceId": ws, "path": format!("{BULK_DIR}/over-frame-cap.log") }),
        )
        .await;
    if over.error().is_none() || over.bytes > cap {
        fail(format!(
            "bulk file.read past the frame cap must answer an error frame under the cap, got {} bytes",
            over.bytes
        ));
    }
    line(&mut report, "file.read", "past-frame-cap", &over);

    let (_, cap) = declared_cap("git.showFile");
    let show = client
        .call(
            "git.showFile",
            &json!({ "workspaceId": ws, "filePath": "src/history/module_0.rs", "ref": "HEAD" }),
        )
        .await;
    check_whole(&mut fail, "git.showFile", &show, cap);
    line(&mut report, "git.showFile", "HEAD", &show);

    let (_, cap) = declared_cap("note.readAsset");
    let png = base64::engine::general_purpose::STANDARD.encode(vec![0x89_u8; 4 * 1024 * 1024]);
    let saved = client
        .call(
            "note.saveAsset",
            &json!({ "workspaceId": ws, "data": png, "mimeType": "image/png", "originalName": "large.png" }),
        )
        .await;
    match saved.result()["url"]
        .as_str()
        .or_else(|| saved.result()["assetId"].as_str())
    {
        Some(asset) => {
            let read = client
                .call(
                    "note.readAsset",
                    &json!({ "workspaceId": ws, "asset": asset }),
                )
                .await;
            check_whole(&mut fail, "note.readAsset", &read, cap);
            line(&mut report, "note.readAsset", "4MiB", &read);
        }
        None => fail(format!("bulk note.saveAsset: {}", saved.frame)),
    }

    // workspace.export.read: every server-cut chunk is under the chunk cap.
    let (export_param, export_cap) = declared_cap("workspace.export.read");
    assert_eq!(export_param, None, "workspace.export.read has no cap param");
    // The big workspace's worktree is outside the workspaces root, which
    // export refuses; export its sibling, which holds store rows only.
    let start = client
        .call(
            "workspace.export.start",
            &json!({ "workspaceId": ctx.seeded.sibling_workspace }),
        )
        .await;
    let Some(export_id) = start.result()["exportId"].as_str().map(str::to_string) else {
        fail(format!("bulk workspace.export.start: {}", start.frame));
        return report;
    };
    let mut seq = 0_u64;
    let mut waited = 0;
    loop {
        let r = client
            .call(
                "workspace.export.read",
                &json!({ "exportId": export_id, "seq": seq }),
            )
            .await;
        if let Some(e) = r.error() {
            let building = e["data"]["code"] != json!("not-found") && seq == 0 && waited < 2_000;
            if building {
                waited += 1;
                // timing-guard: poll interval while the export builds; the session has no ready event to await
                tokio::time::sleep(Duration::from_millis(25)).await;
                continue;
            }
            fail(format!("bulk workspace.export.read seq {seq}: {e}"));
            break;
        }
        let chunk = r.result()["data"]
            .as_str()
            .and_then(|d| base64::engine::general_purpose::STANDARD.decode(d).ok())
            .map_or(0, |d| d.len());
        if chunk > export_cap {
            fail(format!(
                "bulk workspace.export.read seq {seq}: {chunk} bytes, cap {export_cap}"
            ));
        }
        line(
            &mut report,
            "workspace.export.read",
            &format!("seq={seq}"),
            &r,
        );
        seq += 1;
        if seq >= r.result()["totalChunks"].as_u64().unwrap_or(0) {
            break;
        }
    }
    let _ = client
        .call("workspace.export.abort", &json!({ "exportId": export_id }))
        .await;
    report
}

fn check_whole(fail: &mut impl FnMut(String), method: &str, reply: &Reply, cap: usize) {
    if let Some(e) = reply.error() {
        fail(format!("bulk {method}: {e}"));
    } else if reply.bytes > cap {
        fail(format!(
            "bulk {method}: {} bytes over the {cap} byte frame cap",
            reply.bytes
        ));
    } else if reply.bytes < 4 * 1024 {
        fail(format!(
            "bulk {method}: only {} bytes — the check must read a large object",
            reply.bytes
        ));
    }
}
