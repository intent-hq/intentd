//! `intentd agents`: the running agents and their memory, as the Agent
//! memory dialog shows them, read from `agent.memoryUsage` (§5.5).

use std::collections::HashMap;
use std::fmt::Write as _;
use std::path::Path;
use std::process::ExitCode;

use serde_json::{json, Value};
use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};

/// Widest text cell, in terminal columns.
const MAX_CELL_CHARS: usize = 32;
/// Most `workspace.list` pages read for local workspace titles.
const MAX_TITLE_PAGES: usize = 50;

/// Print the running agents of the daemon at `socket`: a table, or with
/// `json` the raw `agent.memoryUsage` payload.
pub(crate) async fn run(socket: &Path, json: bool) -> ExitCode {
    let payload = match call(socket, "agent.memoryUsage", json!({})).await {
        Ok(payload) => payload,
        Err(message) => {
            eprintln!("error: {message}");
            return ExitCode::FAILURE;
        }
    };
    if json {
        print!("{}", render_json(&payload));
        return ExitCode::SUCCESS;
    }
    let titles = workspace_titles(socket, &payload).await;
    print!("{}", render_table(&payload, &titles));
    ExitCode::SUCCESS
}

async fn call(socket: &Path, method: &str, params: Value) -> Result<Value, String> {
    let response = crate::rpc_call(socket, method, params)
        .await
        .map_err(|_| format!("intentd is not reachable at {}", socket.display()))?;
    if let Some(error) = response.get("error") {
        let message = error["message"].as_str().unwrap_or("unknown error");
        return Err(format!("{method} failed: {message}"));
    }
    Ok(response.get("result").cloned().unwrap_or(Value::Null))
}

/// Titles of the local rows' workspaces. A machine that only hosts agents
/// refuses `workspace.list` (node mode); its rows then show their ids.
async fn workspace_titles(socket: &Path, payload: &Value) -> HashMap<String, String> {
    let mut titles = HashMap::new();
    let has_local = rows(payload).any(|row| row.get("remoteOwner").is_none());
    if !has_local {
        return titles;
    }
    let mut params = json!({});
    for _ in 0..MAX_TITLE_PAGES {
        let Ok(page) = call(socket, "workspace.list", params).await else {
            break;
        };
        match add_titles(&mut titles, &page) {
            Some(token) => params = json!({ "nextToken": token }),
            None => break,
        }
    }
    titles
}

/// Add one `workspace.list` page's non-blank titles; the next page's token.
fn add_titles(titles: &mut HashMap<String, String>, page: &Value) -> Option<String> {
    for workspace in page["workspaces"].as_array().into_iter().flatten() {
        if let (Some(id), Some(title)) = (workspace["id"].as_str(), workspace["title"].as_str()) {
            if !title.trim().is_empty() {
                titles.insert(id.to_owned(), title.to_owned());
            }
        }
    }
    page["nextToken"].as_str().map(str::to_owned)
}

fn rows(payload: &Value) -> impl Iterator<Item = &Value> {
    payload["agents"].as_array().into_iter().flatten()
}

fn render_json(payload: &Value) -> String {
    let mut out = serde_json::to_string_pretty(payload).unwrap_or_else(|_| "null".to_owned());
    out.push('\n');
    out
}

fn render_table(payload: &Value, titles: &HashMap<String, String>) -> String {
    let Some(sampled_at) = payload["sampledAt"].as_str() else {
        return "No memory sample yet; try again in a few seconds.\n".to_owned();
    };
    let agents: Vec<&Value> = rows(payload).collect();
    if agents.is_empty() {
        return format!("No running agents (sampled {sampled_at}).\n");
    }
    let mut table = vec![[
        "AGENT".to_owned(),
        "WORKSPACE".to_owned(),
        "PROVIDER".to_owned(),
        "OWNER".to_owned(),
        "MEMORY".to_owned(),
        "PROCS".to_owned(),
    ]];
    table.extend(agents.iter().map(|row| cells(row, titles)));
    let mut widths = [0; 6];
    for row in &table {
        for (width, cell) in widths.iter_mut().zip(row) {
            *width = (*width).max(cell.width());
        }
    }
    let mut out = String::new();
    for row in &table {
        let mut line = String::new();
        for (index, (cell, width)) in row.iter().zip(widths).enumerate() {
            if index > 0 {
                line.push_str("  ");
            }
            // Padded by terminal columns, so wide characters stay aligned.
            // MEMORY and PROCS are numbers: right-aligned.
            let pad = " ".repeat(width.saturating_sub(cell.width()));
            if index >= 4 {
                line.push_str(&pad);
                line.push_str(cell);
            } else {
                line.push_str(cell);
                line.push_str(&pad);
            }
        }
        out.push_str(line.trim_end());
        out.push('\n');
    }
    let count = agents.len();
    let noun = if count == 1 { "agent" } else { "agents" };
    let total = payload["totalBytes"].as_u64().unwrap_or(0);
    let _ = writeln!(
        out,
        "\n{count} {noun}, {} in total, sampled {sampled_at}",
        format_bytes(total)
    );
    out
}

fn cells(row: &Value, titles: &HashMap<String, String>) -> [String; 6] {
    let text = |key: &str| row[key].as_str().filter(|value| !value.trim().is_empty());
    let agent_id = text("agentId").unwrap_or("?");
    let workspace_id = text("workspaceId").unwrap_or("?");
    let owner = row.get("remoteOwner");
    let workspace = match owner {
        Some(_) => text("workspaceTitle").unwrap_or(workspace_id),
        None => titles
            .get(workspace_id)
            .map_or(workspace_id, String::as_str),
    };
    let provider = cut(&match (text("provider"), text("model")) {
        (Some(provider), Some(model)) => format!("{provider} · {model}"),
        (Some(provider), None) => provider.to_owned(),
        (None, Some(model)) => model.to_owned(),
        (None, None) => "?".to_owned(),
    });
    let owner = owner.map_or_else(
        || "this backend".to_owned(),
        |owner| {
            let name = owner["backendName"]
                .as_str()
                .filter(|name| !name.trim().is_empty());
            let id = owner["backendId"].as_str().unwrap_or("?");
            format!("for {}", cut(name.unwrap_or_else(|| short_id(id))))
        },
    );
    [
        cut(text("agentName").unwrap_or(agent_id)),
        cut(workspace),
        provider,
        owner,
        format_bytes(row["memoryBytes"].as_u64().unwrap_or(0)),
        row["processCount"].as_u64().unwrap_or(0).to_string(),
    ]
}

/// A backend id shortened as the app shows it when the name is blank.
fn short_id(id: &str) -> &str {
    id.char_indices().nth(8).map_or(id, |(end, _)| &id[..end])
}

/// A cell's text: control characters (escapes and newlines a backend could
/// put in a label) become spaces, and a cell wider than
/// [`MAX_CELL_CHARS`] columns is cut with an ellipsis.
fn cut(value: &str) -> String {
    let value: String = value
        .chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect();
    if value.width() <= MAX_CELL_CHARS {
        return value;
    }
    let mut out = String::new();
    let mut used = 0;
    for c in value.chars() {
        let width = c.width().unwrap_or(0);
        if used + width > MAX_CELL_CHARS - 1 {
            break;
        }
        used += width;
        out.push(c);
    }
    out.push('…');
    out
}

/// Binary units, one decimal below 100 of a unit.
fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 4] = ["KiB", "MiB", "GiB", "TiB"];
    if bytes < 1024 {
        return format!("{bytes} B");
    }
    #[expect(
        clippy::cast_precision_loss,
        reason = "display only; one decimal is shown"
    )]
    let mut value = bytes as f64 / 1024.0;
    let mut unit = 0;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    if value < 100.0 {
        format!("{value:.1} {}", UNITS[unit])
    } else {
        format!("{value:.0} {}", UNITS[unit])
    }
}

#[cfg(test)]
mod tests;
