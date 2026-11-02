//! Derives, from the router's source, which request params each router arm
//! reads, so a sweep row that leaves a param unexercised fails.
//!
//! The router parses params ad hoc (`opt_str(params, "limit")`,
//! `params.get("projection")`, helper calls like `require_ws_note(params)`)
//! rather than through typed structs, so the derivation reads
//! `crates/intent-transport/src/router.rs` as text:
//!
//! - every top-level `fn` whose signature takes `params: &Map<String, Value>`
//!   is a helper; its keys are the literals it reads, plus the keys of the
//!   helpers it calls (to a fixpoint);
//! - an arm is the text from `"<method>" =>` (or `| "<method>"`) to the next
//!   sibling arm at the same indentation; its keys are the literals it reads
//!   plus the keys of every helper it calls with `params`.
//!
//! Arms that hand the whole object to a typed struct or a service
//! (`Value::Object(params.clone())`) are reported as opaque: their params
//! cannot be derived here and the sweep row declares them instead.
//! Fast-path methods (`host.*`, `system.*`, `browser.*`, `drafts.*`, …) are
//! parsed outside the router; their rows declare params by hand too.

use std::collections::{BTreeMap, BTreeSet};

/// Params one router arm reads.
#[derive(Debug, Default, Clone)]
pub struct ArmParams {
    pub keys: BTreeSet<String>,
    /// The arm passes the whole params object on (typed struct or service
    /// view), so `keys` is incomplete.
    pub opaque: bool,
}

/// Literal keys read from a `params` map in `body`.
fn literal_keys(body: &str) -> BTreeSet<String> {
    const PREFIXES: [&str; 4] = [
        "params.get(\"",
        "params,\"",
        "params[\"",
        "params.contains_key(\"",
    ];
    // Whitespace-free, so `params\n    .get("k")` matches too.
    let body: String = body.chars().filter(|c| !c.is_whitespace()).collect();
    let mut keys = BTreeSet::new();
    for prefix in PREFIXES {
        let mut rest = body.as_str();
        while let Some(at) = rest.find(prefix) {
            rest = &rest[at + prefix.len()..];
            if let Some(end) = rest.find('"') {
                let key = &rest[..end];
                if !key.is_empty()
                    && key
                        .chars()
                        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.')
                {
                    keys.insert(key.to_string());
                }
            }
        }
    }
    keys
}

/// Helper names `body` calls with `params` as the first argument.
fn helper_calls<'a>(body: &str, helpers: &'a BTreeMap<String, String>) -> Vec<&'a str> {
    helpers
        .keys()
        .filter(|name| body.contains(&format!("{name}(params")))
        .map(String::as_str)
        .collect()
}

/// Top-level `fn` bodies of `src` that take `params: &Map<String, Value>`.
fn helper_bodies(src: &str) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    let lines: Vec<&str> = src.lines().collect();
    let mut i = 0;
    while i < lines.len() {
        let line = lines[i];
        let decl = [
            "fn ",
            "pub(crate) fn ",
            "pub fn ",
            "async fn ",
            "pub(crate) async fn ",
        ]
        .iter()
        .find_map(|p| line.strip_prefix(p));
        let Some(decl) = decl else {
            i += 1;
            continue;
        };
        let name: String = decl
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
            .collect();
        let start = i;
        while i < lines.len() && lines[i] != "}" {
            i += 1;
        }
        let text = lines[start..i.min(lines.len())].join("\n");
        let signature = text.split('{').next().unwrap_or_default();
        if signature.contains("params: &Map<String, Value>") {
            out.insert(name, text);
        }
        i += 1;
    }
    out
}

/// Keys each helper reads, closed over the helpers it calls.
fn helper_keys(helpers: &BTreeMap<String, String>) -> BTreeMap<String, BTreeSet<String>> {
    let mut keys: BTreeMap<String, BTreeSet<String>> = helpers
        .iter()
        .map(|(name, body)| (name.clone(), literal_keys(body)))
        .collect();
    loop {
        let mut changed = false;
        for (name, body) in helpers {
            for callee in helper_calls(body, helpers) {
                if callee == name {
                    continue;
                }
                let add: Vec<String> = keys[callee].iter().cloned().collect();
                let own = keys.get_mut(name).expect("helper");
                for key in add {
                    changed |= own.insert(key);
                }
            }
        }
        if !changed {
            return keys;
        }
    }
}

/// Whether `line` opens (or continues) the alternation of an arm naming
/// `quoted` (`"method" =>`, `"a" | "method" =>`, `| "method"`).
fn names_method(line: &str, quoted: &str) -> bool {
    let t = line.trim_start();
    let t = t.strip_prefix("| ").unwrap_or(t);
    t.starts_with('"')
        && t.split("=>")
            .next()
            .unwrap_or_default()
            .split('|')
            .any(|part| part.trim() == quoted)
}

/// The arm body for `method` in `dispatch`: from the first line of its
/// alternation to the next sibling arm at the same indentation.
fn arm_body(dispatch: &str, method: &str) -> Option<String> {
    let quoted = format!("\"{method}\"");
    let lines: Vec<&str> = dispatch.lines().collect();
    let i = lines.iter().position(|l| names_method(l, &quoted))?;
    let mut first = i;
    while first > 0 && lines[first].trim_start().starts_with('|') {
        first -= 1;
    }
    let indent = lines[first].len() - lines[first].trim_start().len();
    let mut body = vec![lines[first]];
    for next in &lines[first + 1..] {
        let t = next.trim_start();
        let next_indent = next.len() - t.len();
        if !t.is_empty()
            && (next_indent < indent
                || (next_indent == indent
                    && (t.starts_with('"') || t.starts_with("_ =>") || t.starts_with('}'))))
        {
            break;
        }
        body.push(next);
    }
    Some(body.join("\n"))
}

/// The body of the router's top-level `async fn dispatch(`.
fn dispatch_body(src: &str) -> &str {
    let start = src
        .find("\nasync fn dispatch(")
        .expect("router dispatch fn");
    let rest = &src[start + 1..];
    let end = rest.find("\n}\n").expect("end of dispatch fn");
    &rest[..end]
}

/// Params each of `methods` reads in `router.rs`; methods without a router
/// arm are absent from the map.
pub fn router_params(methods: &[&str]) -> BTreeMap<String, ArmParams> {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../intent-transport/src/router.rs"
    );
    let src = std::fs::read_to_string(path).unwrap_or_else(|e| panic!("read {path}: {e}"));
    let helpers = helper_bodies(&src);
    let helper_keys = helper_keys(&helpers);
    let dispatch = dispatch_body(&src);
    let mut out = BTreeMap::new();
    for method in methods {
        let Some(body) = arm_body(dispatch, method) else {
            continue;
        };
        let mut arm = ArmParams {
            keys: literal_keys(&body),
            opaque: body.contains("Value::Object(params.clone())"),
        };
        for helper in helper_calls(&body, &helpers) {
            arm.keys.extend(helper_keys[helper].iter().cloned());
        }
        out.insert((*method).to_string(), arm);
    }
    out
}
