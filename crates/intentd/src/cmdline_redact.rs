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
/// - a secret-named header (`Authorization: x`, `X-Api-Key:x`), URL
///   credentials (`https://user:pass@host`, `?token=x`) and `Bearer` /
///   `Basic` credentials inside an argument are hidden.
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
            // The value gets the argument rules: JSON, a nested assignment
            // (`--env=GH_TOKEN=x`), free text.
            return format!("{name}={}", redact_arg(value));
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
        Value::Array(items) => {
            // An argv-like array: a secret-named flag hides the next item.
            let mut hide_next = false;
            for item in items.iter_mut() {
                if std::mem::take(&mut hide_next) && !item.is_null() {
                    *item = Value::String(REDACTED.to_owned());
                    continue;
                }
                if let Value::String(arg) = item {
                    if flag_name(arg).is_some_and(|name| !arg.contains('=') && is_secret_name(name))
                    {
                        hide_next = true;
                        continue;
                    }
                    *arg = redact_arg(arg);
                } else {
                    redact_json(item);
                }
            }
        }
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

/// Secret-named headers, URL credentials and secret query parameters, and
/// `Bearer` / `Basic` credentials inside free text.
fn redact_inline(text: &str) -> String {
    if let Some(header) = redact_header(text) {
        return header;
    }
    let text = if text.contains("://") {
        redact_urls(text)
    } else {
        text.to_owned()
    };
    redact_auth_schemes(&text)
}

/// A `Name: value` / `Name:value` header whose name is secret-named
/// (`Authorization`, `X-Api-Key`), with its value hidden.
fn redact_header(text: &str) -> Option<String> {
    let (name, rest) = text.split_once(':')?;
    let valid_name = !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
    let value = rest.trim_start();
    if !valid_name || rest.starts_with("//") || value.is_empty() || !is_secret_name(name) {
        return None;
    }
    Some(format!("{}{REDACTED}", &text[..text.len() - value.len()]))
}

fn redact_urls(text: &str) -> String {
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
        let url_end = rest
            .find([' ', '\t', '\n', '"', '\''])
            .unwrap_or(rest.len());
        out.push_str(&redact_query(&rest[..url_end]));
        rest = &rest[url_end..];
    }
    out.push_str(rest);
    out
}

/// A URL's path, query and fragment with secret-named query values hidden.
fn redact_query(tail: &str) -> String {
    let Some((path, query)) = tail.split_once('?') else {
        return tail.to_owned();
    };
    let (query, fragment) = match query.split_once('#') {
        Some((query, fragment)) => (query, Some(fragment)),
        None => (query, None),
    };
    let params: Vec<String> = query
        .split('&')
        .map(|param| match param.split_once('=') {
            Some((key, _)) if is_secret_name(key) => format!("{key}={REDACTED}"),
            _ => param.to_owned(),
        })
        .collect();
    let mut out = format!("{path}?{}", params.join("&"));
    if let Some(fragment) = fragment {
        out.push('#');
        out.push_str(fragment);
    }
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
        // `Bearer`, also glued to a quote or `=` (`Authorization="Bearer x"`).
        let scheme = word.trim_start_matches(|c: char| !c.is_ascii_alphanumeric());
        hide_next = scheme.eq_ignore_ascii_case("bearer") || scheme.eq_ignore_ascii_case("basic");
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
