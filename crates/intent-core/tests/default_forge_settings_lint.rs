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
//! This lint fails naming `file:line` for every `SourceControlSettings ::
//! default (` or `SourceControlRegistry :: from_settings` token sequence in
//! non-test code under `crates/*/src/**` (the shared walker skips `tests/`
//! dirs and `tests.rs` files; `#[cfg(test)]` items are blanked). The resolver's
//! file, [`RESOLVER_FILE`], may hold exactly one of each: a second use there
//! fails too, and so does the resolver moving away without this lint being
//! updated. Comments and string literals never count. Limit: a default built
//! through an alias, `Default::default()` with an inferred type, or a struct
//! literal is not seen. There is no opt-out marker: route the build through
//! the resolver instead.

use std::fs;

use intentd_test_support::source_lint::{
    blank_cfg_test_items, crate_src_files, lex, tokenize_str, workspace_root, Token,
};

const RESOLVER_FILE: &str = "crates/intent-services/src/pr_ops.rs";

/// Built at runtime so the tokens this lint matches stay out of its own text.
fn patterns() -> [[String; 3]; 2] {
    [
        [
            ["Source", "ControlSettings"].concat(),
            "::".to_string(),
            "default".to_string(),
        ],
        [
            ["Source", "ControlRegistry"].concat(),
            "::".to_string(),
            "from_settings".to_string(),
        ],
    ]
}

/// Every default-settings or registry build in `src`'s non-test code, as
/// `(pattern index, line)` in source order.
fn builds(src: &str) -> Vec<(usize, usize)> {
    let blanked = blank_cfg_test_items(&lex(src).blanked);
    let toks: Vec<Token> = tokenize_str(&blanked);
    let pats = patterns();
    let mut out = Vec::new();
    for (i, t) in toks.iter().enumerate() {
        for (p, pat) in pats.iter().enumerate() {
            let matched = pat
                .iter()
                .enumerate()
                .all(|(k, want)| toks.get(i + k).is_some_and(|tok| tok.text == *want));
            let call = p != 0 || toks.get(i + 3).is_some_and(|tok| tok.text == "(");
            if matched && call {
                out.push((p, t.line));
            }
        }
    }
    out
}

/// Whether `builds` is exactly one build of each kind (the resolver's shape).
fn is_resolver_shape(builds: &[(usize, usize)]) -> bool {
    (0..patterns().len()).all(|p| builds.iter().filter(|b| b.0 == p).count() == 1)
}

/// Offenders in one file as `file:line`: every build outside the resolver's
/// file, and every build in it unless it holds exactly one of each kind.
fn offenders_in(rel: &str, src: &str) -> Vec<String> {
    let found = builds(src);
    if rel == RESOLVER_FILE && is_resolver_shape(&found) {
        return Vec::new();
    }
    found
        .into_iter()
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
         settings or a direct registry call:\n{}",
        offenders.join("\n")
    );
}

#[test]
fn detector_flags_default_settings_and_registry_builds_outside_the_resolver() {
    let [d, r] = patterns();
    let (settings, registry) = (&d[0], &r[0]);
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
}

#[test]
fn the_resolver_file_may_hold_exactly_one_build_of_each_kind() {
    let [d, r] = patterns();
    let (settings, registry) = (&d[0], &r[0]);
    let resolver = format!(
        "use x::{{{settings}, {registry}}};\nfn default_forge_registry() {{\n    \
         let s = {settings}::default();\n    {registry}::from_settings(&s, &secrets)\n}}\n"
    );
    assert!(offenders_in(RESOLVER_FILE, &resolver).is_empty());
    let second = format!("{resolver}fn other() {{ let _ = {settings}::default(); }}\n");
    assert_eq!(offenders_in(RESOLVER_FILE, &second).len(), 3);
    assert_eq!(offenders_in("elsewhere.rs", &resolver).len(), 2);
}
