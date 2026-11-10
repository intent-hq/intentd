//! Secret redaction for the process command lines `agent.memoryUsage` serves
//! (§5.5). An agent's argv can carry its MCP bridge token, provider keys and
//! env values (the claude adapter's `--mcp-config` JSON), and the rows are
//! shown by `intentd agents --json` and the Agent memory dialog, where they
//! are easy to paste into an issue. Best-effort and over-eager by design: a
//! hidden harmless value costs nothing, a leaked token does.

use serde_json::Value;

/// What replaces a secret value.
pub(crate) const REDACTED: &str = "[redacted]";

/// Join `args` with single spaces, with secret values replaced by
/// [`REDACTED`]:
/// - a JSON object or array argument keeps its shape, but every value under
///   `env`, `headers` or a secret-named key is hidden, and its
///   string arrays get these same argument rules;
/// - a secret-named flag hides its value (`--token x`, `--api-key=x`);
/// - a secret-named assignment hides its value (`GH_TOKEN=x`);
/// - URL credentials (`https://user:pass@host`) and `Bearer` / `Basic`
///   credentials inside an argument are hidden.
pub(crate) fn redact_args(args: impl IntoIterator<Item = String>) -> String {
    let args: Vec<String> = args.into_iter().collect();
    redact_list(&args).join(" ")
}

fn redact_list(args: &[String]) -> Vec<String> {
    let mut out = Vec::with_capacity(args.len());
    let mut hide_next = false;
    for arg in args {
        if hide_next {
            hide_next = false;
            out.push(REDACTED.to_owned());
            continue;
        }
        if let Some(name) = flag_name(arg) {
            if !arg.contains('=') && is_secret_name(name) {
                hide_next = true;
                out.push(arg.clone());
                continue;
            }
        }
        out.push(redact_arg(arg));
    }
    out
}

/// The name of a `-x` / `--name` / `--name=value` flag.
fn flag_name(arg: &str) -> Option<&str> {
    let name = arg.strip_prefix('-')?;
    let name = name.strip_prefix('-').unwrap_or(name);
    let name = name.split_once('=').map_or(name, |(name, _)| name);
    (!name.is_empty()).then_some(name)
}

fn redact_arg(arg: &str) -> String {
    if let Some(json) = redact_json_text(arg) {
        return json;
    }
    if let Some((name, value)) = arg.split_once('=') {
        let bare = name.trim_start_matches('-');
        if !bare.is_empty() && !bare.contains([' ', '/', ':']) {
            if is_secret_name(bare) {
                return format!("{name}={REDACTED}");
            }
            if let Some(json) = redact_json_text(value) {
                return format!("{name}={json}");
            }
        }
    }
    redact_inline(arg)
}

/// A JSON object or array argument with its secrets hidden; `None` when
/// `text` is not one.
fn redact_json_text(text: &str) -> Option<String> {
    if !text.trim_start().starts_with(['{', '[']) {
        return None;
    }
    let mut value: Value = serde_json::from_str(text).ok()?;
    redact_json(&mut value);
    serde_json::to_string(&value).ok()
}

fn redact_json(value: &mut Value) {
    match value {
        Value::Object(map) => {
            for (key, child) in map.iter_mut() {
                let lower = key.to_ascii_lowercase();
                if lower == "env" || lower == "headers" || is_secret_name(key) {
                    hide_values(child);
                } else {
                    redact_json(child);
                }
            }
        }
        Value::Array(items) if items.iter().all(Value::is_string) => {
            let args: Vec<String> = items
                .iter()
                .filter_map(|item| item.as_str().map(str::to_owned))
                .collect();
            *items = redact_list(&args).into_iter().map(Value::String).collect();
        }
        Value::Array(items) => items.iter_mut().for_each(redact_json),
        Value::String(text) => *text = redact_inline(text),
        _ => {}
    }
}

/// Hide every value under `value`, keeping object keys.
fn hide_values(value: &mut Value) {
    match value {
        Value::Object(map) => map.values_mut().for_each(hide_values),
        Value::Array(items) => items.iter_mut().for_each(hide_values),
        Value::Null => {}
        other => *other = Value::String(REDACTED.to_owned()),
    }
}

/// URL credentials and `Bearer` / `Basic` credentials inside free text.
fn redact_inline(text: &str) -> String {
    let text = if text.contains("://") {
        redact_url_userinfo(text)
    } else {
        text.to_owned()
    };
    redact_auth_schemes(&text)
}

fn redact_url_userinfo(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(scheme_end) = rest.find("://") {
        out.push_str(&rest[..scheme_end + 3]);
        rest = &rest[scheme_end + 3..];
        let end = rest
            .find(['/', ' ', '\t', '\n', '"', '\'', '?', '#'])
            .unwrap_or(rest.len());
        match rest[..end].rfind('@') {
            Some(at) => {
                out.push_str(REDACTED);
                out.push_str(&rest[at..end]);
            }
            None => out.push_str(&rest[..end]),
        }
        rest = &rest[end..];
    }
    out.push_str(rest);
    out
}

fn redact_auth_schemes(text: &str) -> String {
    let words: Vec<&str> = text.split(' ').collect();
    let mut out = Vec::with_capacity(words.len());
    let mut hide_next = false;
    for word in words {
        if hide_next && !word.is_empty() {
            hide_next = false;
            out.push(REDACTED);
            continue;
        }
        hide_next = word.eq_ignore_ascii_case("bearer") || word.eq_ignore_ascii_case("basic");
        out.push(word);
    }
    out.join(" ")
}

/// Whether a flag, env or JSON key name names a secret.
fn is_secret_name(name: &str) -> bool {
    const PARTS: &[&str] = &[
        "token",
        "secret",
        "passw",
        "apikey",
        "api_key",
        "authorization",
        "credential",
        "private_key",
        "access_key",
        "cookie",
    ];
    let name = name.to_ascii_lowercase().replace('-', "_");
    name == "key"
        || name == "auth"
        || name.ends_with("_key")
        || name.ends_with("_auth")
        || PARTS.iter().any(|part| name.contains(part))
}

#[cfg(test)]
mod tests;
