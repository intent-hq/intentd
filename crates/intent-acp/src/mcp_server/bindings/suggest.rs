//! Self-correcting `ws.*` errors (brief M4).
//!
//! [`guard_prelude`] wraps `ws` and every namespace object in a `Proxy`
//! installed after all namespace preludes. Reading a name that no bridge
//! could ever install returns a stub; calling the stub sends a
//! `help.suggest` frame, and [`unknown_method_message`] answers with the
//! closest real method, its signature and a `ws.help` pointer. Names that a
//! disabled `[agentFeatures]` toggle removed still read as `undefined`, so
//! feature detection (`if (ws.browser)`) keeps working.
//!
//! [`with_signature`] appends the method's signature to argument errors
//! (`X is required`, `must be …`) returned by any binding.
//!
//! Every signature is cut from [`help_namespace`] — the text `ws.help`
//! returns — so the two cannot drift. Not harness-gated (spec decision
//! 2.2): only error results change, never prompt text.

use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::sync::OnceLock;

use intent_core::settings_file::AgentFeaturesSettings;
use serde_json::Value;

use super::super::tools::{help_namespace, workspace_api_description};

/// Every `ws.*` path documented for any bridge (chief or not, all features
/// on). A missing name in this set was pruned by settings and stays
/// `undefined`; anything else is an invented name.
fn documented_paths() -> &'static BTreeSet<String> {
    static PATHS: OnceLock<BTreeSet<String>> = OnceLock::new();
    PATHS.get_or_init(|| {
        let features = AgentFeaturesSettings::default();
        [false, true]
            .into_iter()
            .flat_map(|chief| {
                let desc = workspace_api_description(chief, &features).into_owned();
                desc.lines()
                    .skip_while(|l| !l.starts_with("API:"))
                    .filter_map(doc_line_path)
                    .collect::<Vec<_>>()
            })
            .flat_map(|path| {
                let mut prefixes = Vec::new();
                let mut end = 0;
                for (i, c) in path.char_indices() {
                    if c == '.' {
                        prefixes.push(path[..i].to_string());
                    }
                    end = i + c.len_utf8();
                }
                prefixes.push(path[..end].to_string());
                prefixes
            })
            .collect()
    })
}

/// `ws.git.listRoots() → …` (indent 2) → `git.listRoots`.
fn doc_line_path(line: &str) -> Option<String> {
    let rest = line.strip_prefix("  ws.")?;
    let end = rest.find('(')?;
    let path = &rest[..end];
    path.chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '.')
        .then(|| path.to_string())
}

const GUARD_PRELUDE_TEMPLATE: &str = r"
    (() => {
        const DOCUMENTED = new Set(__WS_DOCUMENTED__);
        const PASS = new Set(['then', 'toJSON', 'constructor', 'prototype', 'inspect', 'valueOf', 'toString', 'length', 'name', 'call', 'apply', 'bind', 'catch', 'finally']);
        const wrapped = new WeakMap();
        const join = (path, prop) => (path ? path + '.' + prop : prop);
        const suggest = (path, known) => () => host({ method: 'help.suggest', args: { path, known } });
        const missingNamespace = (path, known) => new Proxy(function () {}, {
            get: (_t, prop) => (typeof prop !== 'string' || PASS.has(prop) ? undefined : suggest(join(path, prop), known)),
            apply: () => host({ method: 'help.suggest', args: { path, known } }),
        });
        const isPlain = (v) => v !== null && typeof v === 'object' && Object.getPrototypeOf(v) === Object.prototype && !Object.isFrozen(v);
        const wrap = (obj, path) => {
            if (wrapped.has(obj)) return wrapped.get(obj);
            const proxy = new Proxy(obj, {
                get(target, prop, receiver) {
                    if (typeof prop !== 'string' || prop in target || PASS.has(prop) || prop.startsWith('__')) {
                        const v = Reflect.get(target, prop, receiver);
                        return isPlain(v) ? wrap(v, join(path, prop)) : v;
                    }
                    const full = join(path, prop);
                    if (DOCUMENTED.has(full)) return undefined;
                    const known = Object.keys(target);
                    return path ? suggest(full, known) : missingNamespace(full, known);
                },
            });
            wrapped.set(obj, proxy);
            return proxy;
        };
        globalThis.ws = wrap(globalThis.ws || {}, '');
    })();
";

/// The guard fragment, appended after every namespace prelude.
pub(crate) fn guard_prelude() -> &'static str {
    static PRELUDE: OnceLock<String> = OnceLock::new();
    PRELUDE.get_or_init(|| {
        let names: Vec<&String> = documented_paths().iter().collect();
        let list = serde_json::to_string(&names).unwrap_or_else(|_| "[]".into());
        GUARD_PRELUDE_TEMPLATE.replace("__WS_DOCUMENTED__", &list)
    })
}

/// One documented method of a namespace: its last path segment and its
/// doc line cut to signature plus first sentence.
struct DocEntry {
    name: String,
    path: String,
    line: String,
}

/// The method lines of `namespace` exactly as `ws.help(namespace)` returns
/// them, limited to direct children (`app.workspaces` for `app` is skipped).
fn namespace_entries(
    is_chief: bool,
    features: &AgentFeaturesSettings,
    is_sub_agent: bool,
    namespace: &str,
) -> Vec<DocEntry> {
    let docs_ns = if namespace == "mr" { "pr" } else { namespace };
    let Ok(docs) = help_namespace(is_chief, features, is_sub_agent, docs_ns) else {
        return Vec::new();
    };
    let prefix = format!("{docs_ns}.");
    docs.lines()
        .filter_map(|line| {
            let path = doc_line_path(line)?;
            let name = path.strip_prefix(&prefix)?;
            if name.contains('.') {
                return None;
            }
            Some(DocEntry {
                name: name.to_string(),
                path: format!("{namespace}.{name}"),
                line: short_doc_line(line.trim_start(), docs_ns, namespace),
            })
        })
        .collect()
}

/// `sig  // comment` → `sig  // first sentence`, spelled for the caller's
/// alias namespace (`ws.mr.*` reads the `ws.pr.*` docs).
fn short_doc_line(line: &str, docs_ns: &str, namespace: &str) -> String {
    let line = match line.split_once("  // ") {
        Some((sig, comment)) => format!("{sig}  // {}", first_sentence(comment)),
        None => line.to_string(),
    };
    if docs_ns == namespace {
        line
    } else {
        line.replacen(&format!("ws.{docs_ns}."), &format!("ws.{namespace}."), 1)
    }
}

fn first_sentence(text: &str) -> &str {
    for (idx, _) in text.match_indices(". ") {
        let before = &text[..idx];
        let word = before.rsplit([' ', '(']).next().unwrap_or("");
        if ["e.g", "i.e", "etc", "vs", "cf"]
            .iter()
            .any(|a| word.eq_ignore_ascii_case(a))
            || before.ends_with('.')
        {
            continue;
        }
        return &text[..=idx];
    }
    text
}

/// Near-synonyms agents invent, mapped to real method names in priority
/// order. Only targets present in the namespace are used.
const ALIASES: &[(&[&str], &[&str])] = &[
    (
        &["get", "fetch", "load", "show", "open", "view", "getcontent"],
        &["read", "get", "details", "info", "snapshot", "status"],
    ),
    (
        &[
            "sendmessage",
            "message",
            "postmessage",
            "post",
            "notify",
            "tell",
        ],
        &["send"],
    ),
    (&["append", "addcontent", "insert"], &["add"]),
    (&["remove", "del", "rm"], &["delete"]),
    (&["run", "execute", "exec", "shell"], &["exec", "run"]),
    (&["spawn", "new", "start"], &["create", "start"]),
    (&["find", "lookup"], &["search", "query"]),
    (&["ls", "all", "getall"], &["list", "listPage"]),
];

const VERBS: &[&str] = &[
    "get", "fetch", "read", "list", "show", "load", "find", "query",
];

fn strip_verb(name: &str) -> &str {
    VERBS
        .iter()
        .find_map(|v| name.strip_prefix(v).filter(|r| !r.is_empty()))
        .unwrap_or(name)
}

fn levenshtein(a: &str, b: &str) -> usize {
    let b: Vec<char> = b.chars().collect();
    let mut row: Vec<usize> = (0..=b.len()).collect();
    for (i, ca) in a.chars().enumerate() {
        let mut prev = row[0];
        row[0] = i + 1;
        for (j, cb) in b.iter().enumerate() {
            let cur = row[j + 1];
            row[j + 1] = (prev + usize::from(ca != *cb)).min(row[j] + 1).min(cur + 1);
            prev = cur;
        }
    }
    row[b.len()]
}

/// The candidate closest to `guess`, or `None` when nothing is close.
fn closest<'a>(guess: &str, candidates: &[&'a str]) -> Option<&'a str> {
    let g = guess.to_ascii_lowercase();
    let lower: Vec<(String, &'a str)> = candidates
        .iter()
        .map(|c| (c.to_ascii_lowercase(), *c))
        .collect();
    let find = |want: &str| lower.iter().find(|(l, _)| l == want).map(|(_, c)| *c);
    if let Some(c) = find(&g) {
        return Some(c);
    }
    for (keys, targets) in ALIASES {
        if keys.contains(&g.as_str()) {
            if let Some(c) = targets.iter().find_map(|t| find(&t.to_ascii_lowercase())) {
                return Some(c);
            }
        }
    }
    let stem = |s: &str| strip_verb(s).trim_end_matches('s').to_string();
    let g_stem = stem(&g);
    if let Some((_, c)) = lower
        .iter()
        .find(|(l, _)| !g_stem.is_empty() && (stem(l) == g_stem))
    {
        return Some(c);
    }
    let best = lower
        .iter()
        .map(|(l, c)| (levenshtein(&g, l), *c))
        .min_by_key(|(d, _)| *d)
        .filter(|(d, _)| *d <= (g.len() / 3).max(1));
    if let Some((_, c)) = best {
        return Some(c);
    }
    lower
        .iter()
        .filter(|(l, _)| l.len() >= 4 && g.len() >= 4 && (l.contains(&g) || g.contains(l.as_str())))
        .min_by_key(|(l, _)| l.len().abs_diff(g.len()))
        .map(|(_, c)| *c)
}

/// The `help.suggest` answer for a call to the invented path `path`
/// (`git.roots`, `notes.read`, `notes`). `known` lists the names the JS
/// object actually carries at that level.
pub(crate) fn unknown_method_message(
    is_chief: bool,
    features: &AgentFeaturesSettings,
    is_sub_agent: bool,
    args: &Value,
) -> String {
    let path = args
        .get("path")
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim_start_matches("ws.");
    let known: Vec<String> = args
        .get("known")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();
    let (parent, leaf) = path.rsplit_once('.').unwrap_or(("", path));
    let mut out = format!("ws.{path} is not a function");
    let help_all = "Call ws.help() for the namespace index.";

    // Unknown namespace: `ws.notes.read` or a bare `ws.notes()`.
    let top = path.split('.').next().unwrap_or("");
    let (namespace, method) = if parent.is_empty() || !known_namespace(is_chief, features, top) {
        let wrong_ns = top;
        let present: Vec<String> = if parent.is_empty() {
            known.clone()
        } else {
            namespaces(is_chief, features)
        };
        let refs: Vec<&str> = present.iter().map(String::as_str).collect();
        let Some(ns) = closest(wrong_ns, &refs).map(str::to_string) else {
            let _ = write!(
                out,
                ": `ws.{wrong_ns}` does not exist. Namespaces: {}. {help_all}",
                present.join(", ")
            );
            return out;
        };
        let _ = write!(
            out,
            ": `ws.{wrong_ns}` does not exist; did you mean `ws.{ns}`?"
        );
        let rest = path
            .strip_prefix(wrong_ns)
            .unwrap_or("")
            .trim_start_matches('.');
        if rest.is_empty() {
            let _ = write!(out, " Call ws.help(\"{ns}\") for its methods.");
            return out;
        }
        (ns, rest.to_string())
    } else {
        let _ = write!(out, ": `ws.{parent}` has no method `{leaf}`.");
        (parent.to_string(), leaf.to_string())
    };

    let entries = namespace_entries(is_chief, features, is_sub_agent, &namespace);
    let mut names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
    if namespace == parent {
        for k in &known {
            if !names.contains(&k.as_str()) {
                names.push(k);
            }
        }
    }
    let help_ns = format!("Call ws.help(\"{namespace}\") for every {namespace} method.");
    if let Some(hit) = closest(&method, &names) {
        match entries.iter().find(|e| e.name == hit) {
            Some(e) => {
                let _ = write!(out, " Did you mean:\n  {}\n{help_ns}", e.line);
            }
            None => {
                let _ = write!(out, " Did you mean `ws.{namespace}.{hit}`? {help_ns}");
            }
        }
        return out;
    }
    if let Some(e) = elsewhere(is_chief, features, is_sub_agent, &method) {
        let _ = write!(
            out,
            " Did you mean:\n  {}\nSee ws.help(\"{}\").",
            e.line,
            e.path.rsplit_once('.').map_or("", |p| p.0)
        );
        return out;
    }
    let _ = write!(out, " Methods: {}. {help_ns}", names.join(", "));
    out
}

/// Top-level namespaces in this bridge's description.
fn namespaces(is_chief: bool, features: &AgentFeaturesSettings) -> Vec<String> {
    let desc = workspace_api_description(is_chief, features);
    let set: BTreeSet<String> = desc
        .lines()
        .skip_while(|l| !l.starts_with("API:"))
        .filter_map(doc_line_path)
        .filter_map(|p| p.split_once('.').map(|(ns, _)| ns.to_string()))
        .collect();
    let mut out: Vec<String> = set.into_iter().collect();
    if out.iter().any(|n| n == "pr") && !out.iter().any(|n| n == "mr") {
        out.push("mr".into());
    }
    out
}

fn known_namespace(is_chief: bool, features: &AgentFeaturesSettings, ns: &str) -> bool {
    namespaces(is_chief, features).iter().any(|n| n == ns)
}

/// An exact (case-insensitive) method of the same name in another
/// namespace, e.g. `ws.workspace.commit` → `ws.git.commit`.
fn elsewhere(
    is_chief: bool,
    features: &AgentFeaturesSettings,
    is_sub_agent: bool,
    method: &str,
) -> Option<DocEntry> {
    namespaces(is_chief, features)
        .iter()
        .filter(|ns| *ns != "mr")
        .flat_map(|ns| namespace_entries(is_chief, features, is_sub_agent, ns))
        .find(|e| e.name.eq_ignore_ascii_case(method))
}

/// Whether a binding error reports a missing or invalid argument.
fn is_argument_error(err: &str) -> bool {
    let lower = err.to_ascii_lowercase();
    if lower.contains("agent context") || lower.contains("only available") {
        return false;
    }
    [
        "is required",
        "must be",
        "invalid params",
        "expected a",
        "missing field",
        "invalid type",
        "unknown variant",
    ]
    .iter()
    .any(|p| lower.contains(p))
}

/// Append `method`'s signature to an argument error from its binding.
pub(crate) fn with_signature(
    is_chief: bool,
    features: &AgentFeaturesSettings,
    is_sub_agent: bool,
    method: &str,
    err: String,
) -> String {
    if !is_argument_error(&err) {
        return err;
    }
    let Some((namespace, name)) = method.rsplit_once('.') else {
        return err;
    };
    match namespace_entries(is_chief, features, is_sub_agent, namespace)
        .into_iter()
        .find(|e| e.name == name)
    {
        Some(e) => format!("{err}\nUsage: {}", e.line),
        None => err,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn closest_handles_real_invented_names() {
        assert_eq!(
            closest("sendMessage", &["send", "sendToTask"]),
            Some("send")
        );
        assert_eq!(
            closest("roots", &["commit", "listRoots"]),
            Some("listRoots")
        );
        assert_eq!(closest("get", &["read", "list"]), Some("read"));
        assert_eq!(closest("getDetails", &["info", "details"]), Some("details"));
        assert_eq!(closest("notes", &["note", "task"]), Some("note"));
        assert_eq!(closest("lsitPage", &["listPage", "get"]), Some("listPage"));
        assert_eq!(
            closest("frobnicateEverything", &["commit", "listRoots"]),
            None
        );
    }

    #[test]
    fn argument_errors_are_recognised() {
        assert!(is_argument_error("Note ID is required"));
        assert!(is_argument_error("invalid params: `x` must be a boolean"));
        assert!(!is_argument_error(
            "No agent context available. This tool must be called by an agent."
        ));
        assert!(!is_argument_error("Note not found: spec"));
    }

    #[test]
    fn every_documented_namespace_is_in_the_guard_set() {
        let paths = documented_paths();
        for p in [
            "note",
            "note.read",
            "git.listRoots",
            "app",
            "app.workspaces",
            "browser",
        ] {
            assert!(paths.contains(p), "{p}");
        }
        assert!(!paths.contains("git.roots"));
    }
}
