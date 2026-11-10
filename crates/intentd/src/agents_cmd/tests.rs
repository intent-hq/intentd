use std::collections::HashMap;

use serde_json::json;

use super::*;

fn titles() -> HashMap<String, String> {
    HashMap::from([("ws-local".to_owned(), "Billing".to_owned())])
}

fn payload() -> Value {
    json!({
        "sampledAt": "2026-11-10T02:00:00Z",
        "totalBytes": 1_610_612_736_u64 + 524_288_000 + 2_048,
        "agents": [
            {
                "agentId": "agent-hosted",
                "agentName": "Fix login",
                "workspaceId": "ws-on-head",
                "workspaceTitle": "Auth work",
                "provider": "claude-code",
                "model": "opus",
                "remoteOwner": { "backendId": "head-1", "backendName": "Studio" },
                "rootPid": 500,
                "processCount": 3,
                "memoryBytes": 1_610_612_736_u64,
                "processes": [],
            },
            {
                "agentId": "agent-local",
                "agentName": "Refactor",
                "workspaceId": "ws-local",
                "provider": "codex",
                "rootPid": 100,
                "processCount": 1,
                "memoryBytes": 524_288_000,
                "processes": [],
            },
            {
                "agentId": "agent-old-peer",
                "workspaceId": "ws-elsewhere",
                "provider": "codex",
                "remoteOwner": { "backendId": "3f9a1c2d7e", "backendName": "" },
                "rootPid": 600,
                "processCount": 1,
                "memoryBytes": 2_048,
                "processes": [],
            },
        ],
    })
}

/// One row per agent with name, workspace, provider and model, owner,
/// memory and process count, then the total and sample time. A hosted row
/// uses its launch's labels; a local row's workspace title comes from the
/// backend's workspace list; a hosted row from an older backend (no labels)
/// falls back to its ids, and a blank backend name to the short backend id.
#[test]
fn the_table_names_local_and_hosted_agents() {
    assert_eq!(
        render_table(&payload(), &titles()),
        "\
AGENT           WORKSPACE     PROVIDER            OWNER          MEMORY  PROCS
Fix login       Auth work     claude-code · opus  for Studio    1.5 GiB      3
Refactor        Billing       codex               this backend  500 MiB      1
agent-old-peer  ws-elsewhere  codex               for 3f9a1c2d  2.0 KiB      1

3 agents, 2.0 GiB in total, sampled 2026-11-10T02:00:00Z
"
    );
}

/// A long name or title is cut with an ellipsis so the table stays readable.
#[test]
fn a_long_label_is_cut() {
    let mut payload = payload();
    payload["agents"] = json!([payload["agents"][0].clone()]);
    payload["agents"][0]["agentName"] = json!("x".repeat(60));
    let table = render_table(&payload, &HashMap::new());
    let row = table.lines().nth(1).unwrap();
    assert!(
        row.starts_with(&format!("{}… ", "x".repeat(MAX_CELL_CHARS - 1))),
        "{table}"
    );
    assert!(table.contains("\n1 agent, "), "{table}");
}

/// A daemon with no running agent, and one that has not sampled yet.
#[test]
fn no_agents_and_no_sample_say_so() {
    let empty = json!({ "sampledAt": "2026-11-10T02:00:00Z", "totalBytes": 0, "agents": [] });
    assert_eq!(
        render_table(&empty, &HashMap::new()),
        "No running agents (sampled 2026-11-10T02:00:00Z).\n"
    );
    let unsampled = json!({ "sampledAt": null, "totalBytes": null, "agents": [] });
    assert_eq!(
        render_table(&unsampled, &HashMap::new()),
        "No memory sample yet; try again in a few seconds.\n"
    );
}

/// `--json` prints the daemon's payload unchanged, hosted labels included.
#[test]
fn json_output_is_the_raw_payload() {
    let printed = render_json(&payload());
    let parsed: Value = serde_json::from_str(&printed).unwrap();
    assert_eq!(parsed, payload());
    assert!(printed.ends_with('\n'));
}

/// Workspace titles come from every page of `workspace.list`.
#[test]
fn workspace_titles_read_each_page() {
    let mut titles = HashMap::new();
    let next = add_titles(
        &mut titles,
        &json!({
            "workspaces": [{ "id": "ws-1", "title": "One" }, { "id": "ws-2", "title": "" }],
            "nextToken": "p2",
        }),
    );
    assert_eq!(next.as_deref(), Some("p2"));
    let next = add_titles(
        &mut titles,
        &json!({ "workspaces": [{ "id": "ws-3", "title": "Three" }], "nextToken": null }),
    );
    assert_eq!(next, None);
    assert_eq!(
        titles,
        HashMap::from([
            ("ws-1".to_owned(), "One".to_owned()),
            ("ws-3".to_owned(), "Three".to_owned()),
        ])
    );
}

#[test]
fn memory_is_shown_in_binary_units() {
    assert_eq!(format_bytes(512), "512 B");
    assert_eq!(format_bytes(1_536), "1.5 KiB");
    assert_eq!(format_bytes(524_288_000), "500 MiB");
    assert_eq!(format_bytes(12_884_901_888), "12.0 GiB");
}

/// Labels come from another backend: control characters (terminal escapes,
/// newlines) never reach the terminal, and the table keeps one line per row.
#[test]
fn control_characters_in_labels_are_replaced() {
    let mut payload = payload();
    payload["agents"] = json!([payload["agents"][0].clone()]);
    payload["agents"][0]["agentName"] = json!("evil\u{1b}[2J\nname");
    payload["agents"][0]["workspaceTitle"] = json!("tab\there");
    payload["agents"][0]["model"] = json!("op\u{7}us");
    payload["agents"][0]["remoteOwner"]["backendName"] = json!("St\u{9b}udio");
    let table = render_table(&payload, &HashMap::new());
    assert!(
        !table.chars().any(|c| c.is_control() && c != '\n'),
        "{table:?}"
    );
    assert_eq!(table.lines().count(), 4, "{table}");
    let row = table.lines().nth(1).unwrap();
    assert!(row.starts_with("evil [2J name  tab here"), "{row:?}");
    assert!(row.contains("claude-code · op us"), "{row:?}");
    assert!(row.contains("for St udio"), "{row:?}");
}

/// Wide characters take two terminal columns: the columns after them stay
/// aligned, and a long wide label is cut to the column budget.
#[test]
fn wide_characters_keep_columns_aligned() {
    let mut payload = payload();
    payload["agents"] = json!([payload["agents"][0].clone(), payload["agents"][1].clone()]);
    payload["agents"][0]["agentName"] = json!("修复登录");
    let table = render_table(&payload, &titles());
    let lines: Vec<&str> = table.lines().collect();
    let column = |line: &str, needle: &str| {
        unicode_width::UnicodeWidthStr::width(&line[..line.find(needle).unwrap()])
    };
    assert_eq!(
        column(lines[1], "Auth work"),
        column(lines[0], "WORKSPACE"),
        "{table}"
    );
    assert_eq!(
        column(lines[2], "Billing"),
        column(lines[0], "WORKSPACE"),
        "{table}"
    );

    payload["agents"][0]["agentName"] = json!("长".repeat(40));
    let table = render_table(&payload, &titles());
    let name = table.lines().nth(1).unwrap().split("  ").next().unwrap();
    assert!(name.ends_with('…'), "{name}");
    assert!(
        unicode_width::UnicodeWidthStr::width(name) <= MAX_CELL_CHARS,
        "{name}"
    );
}
