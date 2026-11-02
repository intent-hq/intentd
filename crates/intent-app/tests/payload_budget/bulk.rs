//! `BulkTransfer` methods may exceed the read budget, but never the cap
//! they declare in `PAYLOAD_CLASSES`. Each is called on data at or past
//! that cap and the reply is checked against it:
//!
//! - `file.readChunk` (cap param `length`): a chunk of exactly the cap is
//!   served and holds at most the cap; one byte more is refused (`-32602`).
//! - `workspace.export.read` (server-cut chunks): every chunk of an export
//!   of the big workspace holds at most the chunk cap.
//! - `git.showFileChunk` / `note.readAssetChunk` (cap param `length`): the
//!   same as `file.readChunk`, on a committed blob and a note asset.
//!
//! It also checks the default form of the capped whole-object reads
//! (`file.read`, `git.showFile`, `note.readAsset` without `maxBytes`, which
//! are `BoundedRead` rows): an object over the cap is refused with a small
//! `too-large` error frame, never a shortened or oversized reply.

use std::fmt::Write as _;
use std::time::Duration;

use base64::Engine as _;
use intent_transport::catalog::{PayloadClass, PAYLOAD_CLASSES};
use serde_json::json;

use crate::harness::{Client, Daemon, Reply};
use crate::rows::Ctx;

/// Every `BulkTransfer` method this module checks.
pub const METHODS: &[&str] = &[
    "file.readChunk",
    "git.showFileChunk",
    "note.readAssetChunk",
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
    let (_, chunk_cap) = declared_cap("file.readChunk");
    write_files(&daemon.state.worktree, chunk_cap, frame_cap);
    let mut fail = |msg: String| failures.push(msg);

    // Windowed reads: the declared cap param bounds one response.
    let windows = [
        (
            "file.readChunk",
            json!({ "workspaceId": ws, "path": format!("{BULK_DIR}/large.bin") }),
        ),
        (
            "git.showFileChunk",
            json!({ "workspaceId": ws, "filePath": crate::seed::FULL_FILE, "ref": "HEAD" }),
        ),
        (
            "note.readAssetChunk",
            json!({ "workspaceId": ws, "asset": ctx.seeded.large_asset }),
        ),
    ];
    for (method, base) in windows {
        let (param, cap) = declared_cap(method);
        assert_eq!(param, Some("length"), "{method} cap param");
        let with = |length: usize| {
            let mut p = base.clone();
            p["offset"] = json!(0);
            p["length"] = json!(length);
            p
        };
        let at_cap = client.call(method, &with(cap)).await;
        if let Some(e) = at_cap.error() {
            fail(format!("bulk {method} length=cap: {e}"));
        } else {
            let read = at_cap.result()["bytesRead"].as_u64().unwrap_or(u64::MAX);
            if usize::try_from(read).unwrap_or(usize::MAX) > cap {
                fail(format!("bulk {method}: read {read} bytes, cap {cap}"));
            } else if read < 256 * 1024 {
                fail(format!(
                    "bulk {method}: read only {read} bytes — the check must read a large object"
                ));
            }
        }
        line(&mut report, method, "length=cap", &at_cap);
        let past_cap = client.call(method, &with(cap + 1)).await;
        if past_cap.error().map(|e| e["code"].clone()) != Some(json!(-32602)) {
            fail(format!(
                "bulk {method} length=cap+1 must be refused with -32602, got {}",
                past_cap
                    .frame
                    .to_string()
                    .chars()
                    .take(200)
                    .collect::<String>()
            ));
        }
    }

    // Capped whole-object reads without maxBytes: an object over the cap is
    // refused with a small too-large frame (file.read past the frame cap too).
    let legacy = [
        (
            "file.read",
            json!({ "workspaceId": ws, "path": format!("{BULK_DIR}/over-frame-cap.log") }),
        ),
        (
            "git.showFile",
            json!({ "workspaceId": ws, "filePath": crate::seed::FULL_FILE, "ref": "HEAD" }),
        ),
        (
            "note.readAsset",
            json!({ "workspaceId": ws, "asset": ctx.seeded.large_asset }),
        ),
    ];
    // With REFUSE_OVER_CAP_WITHOUT_MAX_BYTES off the daemon serves the whole
    // object instead (as before protocol 15.4), up to the frame cap: a file
    // past that cap still answers an error frame, never an oversized one.
    let refuse = intent_transport::router::REFUSE_OVER_CAP_WITHOUT_MAX_BYTES;
    let frame_cap = intent_transport::MAX_OUTBOUND_MESSAGE_BYTES;
    for (method, params) in legacy {
        let r = client.call(method, &params).await;
        let code = r.error().map(|e| e["data"]["code"].clone());
        let past_frame_cap = method == "file.read";
        let ok = if refuse {
            code == Some(json!("too-large")) && r.bytes <= 4 * 1024
        } else if past_frame_cap {
            r.error().is_some() && r.bytes <= 4 * 1024
        } else {
            r.error().is_none() && r.bytes > 1024 * 1024 && r.bytes <= frame_cap
        };
        if !ok {
            fail(format!(
                "bulk {method} over the cap without maxBytes (refuse={refuse}): expected {}, got {} bytes: {}",
                if refuse || past_frame_cap { "a small error frame" } else { "the whole object under the frame cap" },
                r.bytes,
                r.frame.to_string().chars().take(200).collect::<String>()
            ));
        }
        let variant = if refuse {
            "over-cap-refused"
        } else {
            "over-cap-whole"
        };
        line(&mut report, method, variant, &r);
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
