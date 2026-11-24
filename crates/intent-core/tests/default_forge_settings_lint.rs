//! Source lint: one place builds the default forge registry.
//!
//! A test daemon points `INTENTD_GITHUB_API_BASE_URI` at a local mock so its
//! fake tokens never reach api.github.com. A forge client built from
//! `SourceControlSettings::default()` keeps `https://api.github.com` unless the
//! builder applies that seam by hand, and sites kept forgetting to: during
//! intentd#3712's first e2e run the backend PR reads went to the real API and
//! got "Bad credentials", and `sourceControl.getUser` still did the same after
//! that fix. Every such build now goes through
//! `intent_services::pr_ops::default_forge_registry`, which applies the seam.
//!
//! This lint fails naming `file:line` for every forge build in non-test code
//! under `crates/*/src/**` (the shared walker skips `tests/` dirs and
//! `tests.rs` files; `#[cfg(test)]` items are blanked):
//!
//! - `SourceControlSettings::default`, called or passed as a function
//!   (`unwrap_or_else(SourceControlSettings::default)`);
//! - `SourceControlRegistry::from_settings`;
//! - `GitHubSourceControl::new(` / `::anonymous(` with a bare `None` argument,
//!   which also means api.github.com without the seam;
//! - `default_forge_registry(` / `resolve_source_control(` with a `None`
//!   (`None`, `Option::None`, `None::<_>`) override argument, which drops the
//!   calling `Services`' own override (about 25 PR reads ignored
//!   `Services::with_github_api_base_uri` this way).
//!
//! The resolver's file, [`RESOLVER_FILE`], may hold exactly one default-settings
//! build and one registry build: a second one there fails, and so does the
//! resolver moving away without this lint being updated. The config-file type
//! of the same name (`intent_core::settings_file::SourceControlSettings`) builds
//! no client, so a `settings_file::`-qualified default and the intent-core
//! sources are skipped. Comments and string literals never count. Limits: a
//! default built through an alias, `Default::default()` with an inferred type,
//! a struct literal, or a `None` held in a variable is not seen, and a
//! `*_tests.rs` module file under `src/` counts as non-test code (as in the
//! other lints). There is no opt-out marker: route the build through the
//! resolver or pass the resolved base.

use std::fs;

use intentd_test_support::source_lint::{
    blank_cfg_test_items, crate_src_files, lex, tokenize_str, workspace_root, Token,
};

const RESOLVER_FILE: &str = "crates/intent-services/src/pr_ops.rs";

/// The crate whose `SourceControlSettings` is the config-file type
/// (`intent_core::settings_file`), which builds no forge client; intent-core
/// cannot name the forge crate's type, so its sources are skipped.
const CONFIG_TYPE_CRATE: &str = "crates/intent-core/";

/// Build kinds: default forge settings, a registry build, a direct client
/// built with a literal `None` API base, and a resolver call that drops the
/// caller's override with a literal `None`.
const DEFAULT_SETTINGS: usize = 0;
const REGISTRY: usize = 1;
const DIRECT_CLIENT: usize = 2;
const RESOLVER_CALL: usize = 3;

/// Built at runtime so the tokens this lint matches stay out of its own text.
fn names() -> [String; 4] {
    [
        ["Source", "ControlSettings"].concat(),
        ["Source", "ControlRegistry"].concat(),
        ["GitHub", "SourceControl"].concat(),
        ["settings", "_file"].concat(),
    ]
}

/// The resolver entry points that take the caller's API-base override, with
/// the override's argument position.
fn resolver_names() -> [(String, usize); 2] {
    [
        (["default_forge", "_registry"].concat(), 0),
        (["resolve_source", "_control"].concat(), 1),
    ]
}

/// The tokens of top-level argument `n` of the call whose `(` is `toks[open]`.
fn nth_arg(toks: &[Token], open: usize, n: usize) -> Vec<&str> {
    let depth = toks[open].depth;
    let mut index = 0;
    let mut arg = Vec::new();
    for t in &toks[open + 1..] {
        let closes = t.depth == depth && t.text == ")";
        if closes || (t.depth == depth + 1 && t.text == ",") {
            if index == n || closes {
                break;
            }
            index += 1;
        } else if index == n {
            arg.push(t.text.as_str());
        }
    }
    if index == n {
        arg
    } else {
        Vec::new()
    }
}

/// `None`, `Option::None` (any path ending so), or `None::<_>`.
fn is_none_literal(arg: &[&str]) -> bool {
    matches!(arg, ["None"] | ["None", "::", ..] | [.., "::", "None"])
}

fn text_at(toks: &[Token], i: usize) -> &str {
    toks.get(i).map_or("", |t| t.text.as_str())
}

/// Whether the call whose `(` is `toks[open]` passes a bare `None` as any
/// top-level argument.
fn passes_bare_none(toks: &[Token], open: usize) -> bool {
    let depth = toks[open].depth;
    let mut arg: Vec<&str> = Vec::new();
    for t in &toks[open + 1..] {
        let closes = t.depth == depth && t.text == ")";
        if closes || (t.depth == depth + 1 && t.text == ",") {
            if arg == ["None"] {
                return true;
            }
            arg.clear();
            if closes {
                return false;
            }
        } else {
            arg.push(t.text.as_str());
        }
    }
    false
}

/// Every forge build in `src`'s non-test code, as `(kind, line)` in source
/// order: `SourceControlSettings::default` (called or passed as a function,
/// unless qualified by `settings_file::`), `SourceControlRegistry::from_settings`,
/// `GitHubSourceControl::new(` / `::anonymous(` with a bare `None`
/// argument (api.github.com without the seam), and resolver calls with a
/// `None` override argument (the caller's override dropped).
fn builds(src: &str) -> Vec<(usize, usize)> {
    let blanked = blank_cfg_test_items(&lex(src).blanked);
    let toks: Vec<Token> = tokenize_str(&blanked);
    let [settings, registry, client, config_module] = names();
    let resolvers = resolver_names();
    let mut out = Vec::new();
    for (i, t) in toks.iter().enumerate() {
        let override_at = resolvers
            .iter()
            .find(|(name, _)| *name == t.text)
            .map(|(_, at)| *at);
        if let Some(at) = override_at {
            if text_at(&toks, i + 1) == "(" && is_none_literal(&nth_arg(&toks, i + 1, at)) {
                out.push((RESOLVER_CALL, t.line));
            }
            continue;
        }
        if text_at(&toks, i + 1) != "::" {
            continue;
        }
        let member = text_at(&toks, i + 2);
        let config_type =
            i >= 2 && text_at(&toks, i - 1) == "::" && text_at(&toks, i - 2) == config_module;
        let kind = if t.text == settings && member == "default" && !config_type {
            Some(DEFAULT_SETTINGS)
        } else if t.text == registry && member == "from_settings" {
            Some(REGISTRY)
        } else if t.text == client
            && (member == "new" || member == "anonymous")
            && text_at(&toks, i + 3) == "("
            && passes_bare_none(&toks, i + 3)
        {
            Some(DIRECT_CLIENT)
        } else {
            None
        };
        if let Some(kind) = kind {
            out.push((kind, t.line));
        }
    }
    out
}

/// Whether `builds` is the resolver's shape: exactly one default-settings
/// build and one registry build.
fn is_resolver_shape(builds: &[(usize, usize)]) -> bool {
    [DEFAULT_SETTINGS, REGISTRY]
        .iter()
        .all(|k| builds.iter().filter(|b| b.0 == *k).count() == 1)
}

/// Offenders in one file as `file:line`. In the resolver's file the
/// default-settings and registry builds pass only in the resolver's shape;
/// direct clients and resolver calls given a `None` fail everywhere.
fn offenders_in(rel: &str, src: &str) -> Vec<String> {
    if rel.starts_with(CONFIG_TYPE_CRATE) {
        return Vec::new();
    }
    let found = builds(src);
    let resolver = rel == RESOLVER_FILE && is_resolver_shape(&found);
    found
        .into_iter()
        .filter(|(kind, _)| !resolver || matches!(*kind, DIRECT_CLIENT | RESOLVER_CALL))
        .map(|(_, line)| format!("{rel}:{line}"))
        .collect()
}

#[test]
fn forge_registries_are_built_only_by_the_resolver() {
    let root = workspace_root();
    assert!(
        root.join(RESOLVER_FILE).is_file(),
        "{RESOLVER_FILE} moved; update RESOLVER_FILE"
    );
    let files = crate_src_files(&root);
    assert!(
        !files.is_empty(),
        "no Rust sources under {}",
        root.display()
    );
    let mut offenders = Vec::new();
    let mut resolver_ok = false;
    for path in files {
        let rel = path
            .strip_prefix(&root)
            .unwrap_or(&path)
            .to_string_lossy()
            .replace('\\', "/");
        let src =
            fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        if rel == RESOLVER_FILE {
            resolver_ok = is_resolver_shape(&builds(&src));
        }
        offenders.extend(offenders_in(&rel, &src));
    }
    assert!(
        resolver_ok,
        "{RESOLVER_FILE} should hold the one default-settings build and the one registry \
         build of `default_forge_registry`"
    );
    assert!(
        offenders.is_empty(),
        "build the default forge client with `pr_ops::default_forge_registry` (it applies \
         the INTENTD_GITHUB_API_BASE_URI test seam) instead of default source-control \
         settings or a direct registry call, pass `invite_ops::resolve_api_base_uri(..)` \
         rather than `None` to a direct GitHub client, and pass the `Services`' own \
         `github_api_base_uri` rather than `None` to the resolver:\n{}",
        offenders.join("\n")
    );
}

#[test]
fn detector_flags_default_settings_and_registry_builds_outside_the_resolver() {
    let [settings, registry, ..] = names();
    let direct = format!(
        "fn f() {{\n    let s = intent_sourcecontrol::{settings}::default();\n    \
         {registry}::from_settings(&s, &secrets);\n}}\n"
    );
    assert_eq!(offenders_in("a.rs", &direct), vec!["a.rs:2", "a.rs:3"]);
    let spaced = format!("let s = {settings}\n    ::default ();\n");
    assert_eq!(offenders_in("b.rs", &spaced), vec!["b.rs:1"]);
    let update = format!("let s = S {{ x, ..{settings}::default() }};\n");
    assert_eq!(offenders_in("c.rs", &update), vec!["c.rs:1"]);
    let hidden = format!("// {settings}::default()\nlet t = \"{registry}::from_settings\";\n");
    assert!(offenders_in("d.rs", &hidden).is_empty());
    let in_test =
        format!("#[cfg(test)]\nmod tests {{\n    fn t() {{ {settings}::default(); }}\n}}\n");
    assert!(offenders_in("e.rs", &in_test).is_empty());
    let path_only = format!("fn f(s: &{settings}) {{}}\n");
    assert!(offenders_in("f.rs", &path_only).is_empty());
    let fn_path = format!("let s = opt.unwrap_or_else({settings}::default);\n");
    assert_eq!(offenders_in("g.rs", &fn_path), vec!["g.rs:1"]);
}

#[test]
fn the_config_file_type_is_not_a_forge_build() {
    let [settings, _, _, config_module] = names();
    let qualified = format!("let c = intent_core::{config_module}::{settings}::default();\n");
    assert!(offenders_in("crates/x/src/a.rs", &qualified).is_empty());
    let in_core = format!("let c = {settings}::default();\n");
    assert!(offenders_in("crates/intent-core/src/settings_file.rs", &in_core).is_empty());
    assert_eq!(offenders_in("crates/x/src/b.rs", &in_core).len(), 1);
}

#[test]
fn direct_clients_with_a_bare_none_base_are_flagged() {
    let [_, _, client, _] = names();
    let flagged = format!(
        "let a = {client}::new(&token, None)?;\nlet b = intent_sourcecontrol::github::{client}::anonymous(None);\n"
    );
    assert_eq!(offenders_in("a.rs", &flagged), vec!["a.rs:1", "a.rs:2"]);
    let seamed = format!(
        "let a = {client}::new(&token, base.as_deref())?;\n\
         let b = {client}::anonymous(f(None));\nlet c = {client}::new(t, Some(&x));\n"
    );
    assert!(offenders_in("b.rs", &seamed).is_empty());
    let in_resolver = format!("let a = {client}::new(&t, None);\n");
    assert_eq!(offenders_in(RESOLVER_FILE, &in_resolver).len(), 1);
}

#[test]
fn the_resolver_file_may_hold_exactly_one_build_of_each_kind() {
    let [settings, registry, ..] = names();
    let resolver = format!(
        "use x::{{{settings}, {registry}}};\nfn default_forge_registry() {{\n    \
         let s = {settings}::default();\n    {registry}::from_settings(&s, &secrets)\n}}\n"
    );
    assert!(offenders_in(RESOLVER_FILE, &resolver).is_empty());
    let second = format!("{resolver}fn other() {{ let _ = {settings}::default(); }}\n");
    assert_eq!(offenders_in(RESOLVER_FILE, &second).len(), 3);
    assert_eq!(offenders_in("elsewhere.rs", &resolver).len(), 2);
}

#[test]
fn resolver_calls_with_a_none_override_are_flagged() {
    let [(forge, _), (resolve, _)] = resolver_names();
    let flagged = format!(
        "let a = pr_ops::{forge}(None, &secrets).await;\n\
         let b = {resolve}(injected, None, &secrets).await;\n\
         let c = {resolve}(injected, Option::None, &s).await;\n\
         let d = {forge}(None::<&str>, &s).await;\n"
    );
    assert_eq!(
        offenders_in("a.rs", &flagged),
        vec!["a.rs:1", "a.rs:2", "a.rs:3", "a.rs:4"]
    );
    assert_eq!(offenders_in(RESOLVER_FILE, &flagged).len(), 4);
    let seamed = format!(
        "let a = pr_ops::{forge}(self.github_api_base_uri.as_deref(), &s).await;\n\
         let b = {resolve}(injected, api_base.as_deref(), &s).await;\n\
         let c = {resolve}(None, self.github_api_base_uri.as_deref(), &s).await;\n\
         async fn {forge}(override_uri: Option<&str>, s: &S) {{}}\n"
    );
    assert!(offenders_in("b.rs", &seamed).is_empty());
}
