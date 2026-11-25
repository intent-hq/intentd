//! Source lint: every session `metadata` key the daemon writes through the
//! JSON single-key writer is protected from stale whole-document writes.
//!
//! A key written through `Store::set_agent_session_metadata_key_json` (or its
//! `_if_identity` form) is daemon state. `Store::update_agent_session` and
//! `Store::update_agent_session_metadata` write the caller's whole
//! `metadata` document, so a snapshot read before the daemon's write (a busy
//! agent's rename) puts the old value back unless the key is listed in
//! `intent_store::DAEMON_OWNED_METADATA_KEYS`. That happened to
//! `secretRequestResolutions` (intentd#3741) and to the proposal outcomes
//! (`pendingProposals`, `proposalResolutions`, `proposalCreatedWorkspaces`):
//! an answered proposal card came back as pending after a restart.
//!
//! The lint resolves the key argument of every non-test call: a string
//! literal, or a path ending in a `const NAME: &str = "…";` defined in
//! non-test source. It fails naming `file:line` when the key is neither in
//! `DAEMON_OWNED_METADATA_KEYS` nor in [`NOT_DAEMON_OWNED`] (with a reason).
//! A key passed through a variable must be listed in [`FORWARDED`] with its
//! call count and the keys it carries. Limits: a call made through an alias,
//! a function pointer or UFCS is not seen, and the string-valued writer
//! `Store::set_agent_session_metadata_key` (read markers, notices) is out of
//! scope; its keys are not yet classified.

use std::collections::{BTreeMap, BTreeSet};

use intent_store::DAEMON_OWNED_METADATA_KEYS;
use intentd_test_support::source_lint::{
    matching_close, test_code_files, tokenize, workspace_root, TestCodeText, Token,
};

/// The single-key writers and the index of their key argument.
const WRITERS: &[(&str, usize)] = &[
    ("set_agent_session_metadata_key_json", 2),
    ("set_agent_session_metadata_key_json_if_identity", 4),
];

/// Keys a single-key writer sets that whole-document writes may still carry.
const NOT_DAEMON_OWNED: &[(&str, &str)] = &[(
    "importedSession.handoffPending",
    "a nested flag of the `importedSession` object that session import \
     writes with the creation metadata; the protected list holds top-level \
     keys only, and a stale write can at most repeat the one-time handoff \
     notice",
)];

/// Calls whose key argument is a variable: `(file, variable, calls, keys)`.
const FORWARDED: &[(&str, &str, usize, &[&str])] = &[
    // The `Store` methods dispatching to the backend.
    ("crates/intent-store/src/agent_repo.rs", "key", 2, &[]),
    // `persist_proposal_maps` loops over the two proposal maps.
    (
        "crates/intent-services/src/agent_ops.rs",
        "key",
        1,
        &[
            intent_core::PROPOSAL_RESOLUTIONS_KEY,
            intent_core::PROPOSAL_CREATED_WORKSPACES_KEY,
        ],
    ),
];

#[derive(Debug, Clone, PartialEq, Eq)]
enum Key {
    Literal(String),
    Const(String),
    Forwarded(String),
    Unresolved(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Write {
    line: usize,
    key: Key,
}

fn is_const_name(name: &str) -> bool {
    name.chars().any(|c| c.is_ascii_uppercase())
        && name
            .chars()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
}

/// The string literal filling `start..end` of the blanked text, if that span
/// holds exactly one literal and no tokens.
fn literal_in(text: &TestCodeText, start: usize, end: usize) -> Option<String> {
    let mut found = text
        .lexed
        .literals
        .iter()
        .filter(|l| (start..end).contains(&l.offset));
    let literal = found.next()?;
    found.next().is_none().then(|| literal.cooked())
}

fn key_of(text: &TestCodeText, arg: &[Token], start: usize, end: usize) -> Key {
    let arg: Vec<&Token> = arg.iter().skip_while(|t| t.is("&")).collect();
    if arg.is_empty() {
        return literal_in(text, start, end)
            .map_or_else(|| Key::Unresolved(String::new()), Key::Literal);
    }
    let path = arg
        .iter()
        .enumerate()
        .all(|(i, t)| if i % 2 == 0 { t.is_ident() } else { t.is("::") })
        && arg.len() % 2 == 1;
    let last = &arg[arg.len() - 1].text;
    let written: Vec<&str> = arg.iter().map(|t| t.text.as_str()).collect();
    match (path, arg.len()) {
        (true, _) if is_const_name(last) => Key::Const(last.clone()),
        (true, 1) => Key::Forwarded(last.clone()),
        _ => Key::Unresolved(written.concat()),
    }
}

/// Every non-test call to a [`WRITERS`] method in `src`.
fn writes_in(src: &str, whole: bool) -> Vec<Write> {
    if whole {
        return Vec::new();
    }
    let text = TestCodeText::new(src, false);
    let tokens = tokenize(&text.chars);
    let mut out = Vec::new();
    for (i, window) in tokens.windows(3).enumerate() {
        let [dot, name, open] = window else {
            continue;
        };
        let Some(&(_, index)) = WRITERS.iter().find(|(m, _)| name.is(m)) else {
            continue;
        };
        if !dot.is(".") || !open.is("(") || text.in_test(name.start) {
            continue;
        }
        let open_at = i + 2;
        let Some(close_at) = matching_close(&tokens, open_at) else {
            continue;
        };
        let depth = tokens[open_at].depth + 1;
        let mut bounds = vec![open_at];
        bounds.extend(
            (open_at + 1..close_at).filter(|&j| tokens[j].is(",") && tokens[j].depth == depth),
        );
        bounds.push(close_at);
        let key = match (bounds.get(index), bounds.get(index + 1)) {
            (Some(&from), Some(&to)) => key_of(
                &text,
                &tokens[from + 1..to],
                tokens[from].end,
                tokens[to].start,
            ),
            _ => Key::Unresolved("<missing argument>".to_string()),
        };
        out.push(Write {
            line: name.line,
            key,
        });
    }
    out
}

/// Every `const NAME: &str = "…";` (or `&'static str`) in non-test `src`.
fn str_consts_in(src: &str, whole: bool) -> Vec<(String, String)> {
    if whole {
        return Vec::new();
    }
    let text = TestCodeText::new(src, false);
    let tokens = tokenize(&text.chars);
    let mut out = Vec::new();
    for (i, t) in tokens.iter().enumerate() {
        if !t.is("const") || text.in_test(t.start) {
            continue;
        }
        let rest: Vec<&str> = tokens[i + 1..]
            .iter()
            .take(6)
            .map(|t| t.text.as_str())
            .collect();
        let eq = match rest.as_slice() {
            [_, ":", "&", "str", "=", ..] => i + 5,
            [_, ":", "&", "static", "str", "="] => i + 6,
            _ => continue,
        };
        let Some(semi) = tokens.get(eq + 1).filter(|t| t.is(";")) else {
            continue;
        };
        if let Some(value) = literal_in(&text, tokens[eq].end, semi.start) {
            out.push((tokens[i + 1].text.clone(), value));
        }
    }
    out
}

struct Scan {
    /// `(file:line, key)` per resolved write.
    keys: Vec<(String, String)>,
    /// Calls per `(file, variable)` of a forwarded key.
    forwarded: BTreeMap<(String, String), usize>,
    problems: Vec<String>,
}

fn scan() -> Scan {
    let files = test_code_files(&workspace_root());
    let mut consts: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for file in &files {
        for (name, value) in str_consts_in(&file.src, file.whole) {
            consts.entry(name).or_default().insert(value);
        }
    }
    let mut scan = Scan {
        keys: Vec::new(),
        forwarded: BTreeMap::new(),
        problems: Vec::new(),
    };
    for file in &files {
        for write in writes_in(&file.src, file.whole) {
            let at = format!("{}:{}", file.rel, write.line);
            match write.key {
                Key::Literal(value) => scan.keys.push((at, value)),
                Key::Const(name) => match consts.get(&name).map(|v| v.iter().collect::<Vec<_>>()) {
                    Some(values) if values.len() == 1 => scan.keys.push((at, values[0].clone())),
                    Some(values) => scan.problems.push(format!(
                        "{at}: `{name}` names several string consts ({values:?}); \
                         use a literal or a uniquely named const"
                    )),
                    None => scan.problems.push(format!(
                        "{at}: no non-test `const {name}: &str = \"…\";` found"
                    )),
                },
                Key::Forwarded(var) => {
                    *scan.forwarded.entry((file.rel.clone(), var)).or_default() += 1;
                }
                Key::Unresolved(expr) => scan.problems.push(format!(
                    "{at}: cannot resolve the key argument `{expr}`; pass a string literal \
                     or a `&str` const"
                )),
            }
        }
    }
    for (file, var, _, keys) in FORWARDED {
        for key in *keys {
            scan.keys
                .push((format!("{file} (forwarded `{var}`)"), (*key).to_string()));
        }
    }
    scan
}

#[test]
fn daemon_written_metadata_keys_are_protected_from_whole_document_writes() {
    let scan = scan();
    let mut problems = scan.problems.clone();
    for (at, key) in &scan.keys {
        if !DAEMON_OWNED_METADATA_KEYS.contains(&key.as_str())
            && !NOT_DAEMON_OWNED.iter().any(|(k, _)| k == key)
        {
            problems.push(format!(
                "{at}: `{key}` is written by a single-key writer but is not in \
                 DAEMON_OWNED_METADATA_KEYS (crates/intent-store/src/agent_repo.rs), so a \
                 stale whole-session write can erase or revert it; add it there, or to \
                 NOT_DAEMON_OWNED in this lint with a reason"
            ));
        }
    }
    let expected: BTreeMap<(String, String), usize> = FORWARDED
        .iter()
        .map(|(file, var, calls, _)| (((*file).to_string(), (*var).to_string()), *calls))
        .collect();
    if scan.forwarded != expected {
        problems.push(format!(
            "calls passing the key through a variable changed: found {:?}, FORWARDED lists \
             {expected:?}; list each one with the keys it carries",
            scan.forwarded
        ));
    }
    assert!(problems.is_empty(), "{}", problems.join("\n"));
}

#[test]
fn opt_outs_are_live_and_explained() {
    let scan = scan();
    for (key, reason) in NOT_DAEMON_OWNED {
        assert!(!reason.trim().is_empty(), "`{key}` needs a reason");
        assert!(
            !DAEMON_OWNED_METADATA_KEYS.contains(key),
            "`{key}` is daemon-owned; drop it from NOT_DAEMON_OWNED"
        );
        assert!(
            scan.keys.iter().any(|(_, k)| k == key),
            "no single-key writer sets `{key}` any more; drop it from NOT_DAEMON_OWNED"
        );
    }
}

/// Guards against the lint silently passing after a rename: the scan still
/// resolves the keys the daemon writes today.
#[test]
fn scan_finds_the_known_daemon_written_keys() {
    let found: BTreeSet<String> = scan().keys.into_iter().map(|(_, k)| k).collect();
    for key in [
        intent_store::PROVIDER_RETURN_METADATA_KEY,
        intent_core::remote_launch::REMOTE_PLACEMENT_METADATA_KEY,
        intent_core::SECRET_REQUEST_RESOLUTIONS_KEY,
        intent_core::PENDING_PROPOSALS_KEY,
        intent_core::PROPOSAL_RESOLUTIONS_KEY,
        intent_core::PROPOSAL_CREATED_WORKSPACES_KEY,
    ] {
        assert!(
            found.contains(key),
            "the scan no longer sees writes of `{key}`"
        );
    }
}

#[test]
fn detector_resolves_key_arguments_outside_test_code_only() {
    let keys = |src: &str| -> Vec<(usize, Key)> {
        writes_in(src, false)
            .into_iter()
            .map(|w| (w.line, w.key))
            .collect()
    };
    let literal = "store\n    .set_agent_session_metadata_key_json(ws, id, \"a.b\", \"1\", now)\n";
    assert_eq!(keys(literal), [(2, Key::Literal("a.b".into()))]);
    let path = "s.set_agent_session_metadata_key_json(&ws, &id, intent_core::SOME_KEY, &v, &t);\n";
    assert_eq!(keys(path), [(1, Key::Const("SOME_KEY".into()))]);
    let identity =
        "s.set_agent_session_metadata_key_json_if_identity(w, i, p, m, OTHER_KEY, v, y, t);\n";
    assert_eq!(keys(identity), [(1, Key::Const("OTHER_KEY".into()))]);
    let forwarded = "s.set_agent_session_metadata_key_json(w, i, key, &f(a, b), t);\n";
    assert_eq!(keys(forwarded), [(1, Key::Forwarded("key".into()))]);
    let computed = "s.set_agent_session_metadata_key_json(w, i, &format!(\"{x}\"), v, t);\n";
    assert!(matches!(keys(computed)[..], [(1, Key::Unresolved(_))]));
    let cfg_test = "#[cfg(test)]\nmod tests {\n    fn f() { s.set_agent_session_metadata_key_json(w, i, \"k\", v, t); }\n}\n";
    assert!(keys(cfg_test).is_empty());
    let comment = "// s.set_agent_session_metadata_key_json(w, i, \"k\", v, t);\n";
    assert!(keys(comment).is_empty());
    assert!(writes_in(literal, true).is_empty());
    let defs = "pub const A_KEY: &str = \"a\";\nconst B: &'static str = \"b\";\nconst C: u8 = 1;\n";
    assert_eq!(
        str_consts_in(defs, false),
        [("A_KEY".into(), "a".into()), ("B".into(), "b".into())]
    );
}
