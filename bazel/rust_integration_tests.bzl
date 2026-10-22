"""One rust_test per Cargo integration test, discovered at load time.

Cargo builds every `tests/<name>.rs` and `tests/<name>/main.rs` as its own test
crate. This macro globs the same files, so a new integration test in an
existing crate needs no BUILD change: `bazel test //crates/<crate>:<name>`
runs it with the crate's common test attributes.

scripts/bazel-gen.py writes one call per crate. It passes the attributes every
integration test of the crate shares, and two small tables:

  * `overrides`: per-test deviations from the defaults, keyed by test name.
    Keys: `manual` (the reason; adds the "manual" tag), `exclusive` (bool),
    `timeout`, `workspace_sources` (bool; data dep on every crate's Rust
    sources), `cargo` (bool; the toolchain's cargo as data and as `CARGO`).
  * `exclude`: tests the generator writes as explicit rules instead (tests
    split into several targets, or targets Cargo does not autodiscover).

Tests named `*_lint` read Rust sources across the workspace (source lints, by
the convention source_lint_discovery_lint enforces), so they get
`//:workspace_rust_sources` without an override.
"""

load("@rules_rust//rust:defs.bzl", "rust_test")

CARGO_FILES = "@rules_rust//rust/toolchain:current_cargo_files"
WORKSPACE_TEST_DATA = "//:workspace_test_data"
WORKSPACE_RUST_SOURCES = "//:workspace_rust_sources"

_OVERRIDE_KEYS = ["cargo", "exclusive", "manual", "timeout", "workspace_sources"]

def integration_test_roots(paths):
    """Map Cargo integration-test crate roots to test names, as Cargo autodiscovery does.

    Args:
      paths: package-relative paths matching `tests/*.rs` or `tests/*/main.rs`.

    Returns:
      A list of (name, crate_root) pairs.
    """
    roots = []
    for path in paths:
        parts = path.split("/")
        if len(parts) == 2 and parts[1].endswith(".rs"):
            roots.append((parts[1][:-len(".rs")], path))
        elif len(parts) == 3 and parts[2] == "main.rs":
            roots.append((parts[1], path))
    return roots

def reads_workspace_sources(name):
    return name.endswith("_lint")

def rust_integration_tests(
        overrides = {},
        exclude = [],
        workspace_sources = False,
        data = [],
        rustc_env = {},
        tags = [],
        **kwargs):
    """Declare a rust_test for each integration test of the current package.

    Args:
      overrides: per-test deviations, {test name: {key: value}}; see the module doc.
      exclude: test names declared elsewhere in the package.
      workspace_sources: whether every test of the package gets the Rust sources
        of all crates as data (its shared test modules walk the workspace).
      data: extra run-time data of every test (for example test binaries).
      rustc_env: compile-time environment of every test.
      tags: tags of every test.
      **kwargs: other rust_test attributes shared by every test.
    """
    support = native.glob(["tests/*/**/*.rs"], allow_empty = True)
    roots = integration_test_roots(native.glob(["tests/*.rs", "tests/*/main.rs"], allow_empty = True))
    names = [name for name, _ in roots]
    for name, override in overrides.items():
        if name not in names:
            fail("rust_integration_tests: override for {}, which has no tests/{}.rs or tests/{}/main.rs; rerun scripts/bazel-gen.py".format(name, name, name))
        for key in override:
            if key not in _OVERRIDE_KEYS:
                fail("rust_integration_tests: unknown override key {} for {} (known: {})".format(key, name, ", ".join(_OVERRIDE_KEYS)))

    for name, root in roots:
        if name in exclude:
            continue
        override = overrides.get(name, {})
        test_data = [WORKSPACE_TEST_DATA]
        if workspace_sources or reads_workspace_sources(name) or override.get("workspace_sources"):
            test_data.append(WORKSPACE_RUST_SOURCES)
        test_data += data
        env = dict(rustc_env)
        if override.get("cargo"):
            # Cargo sets CARGO for every compile; tests that run `cargo
            # metadata` get the toolchain's cargo.
            test_data.append(CARGO_FILES)
            env["CARGO"] = "$(execpath {})".format(CARGO_FILES)
        env = {key: env[key] for key in sorted(env.keys())}
        test_tags = {tag: None for tag in tags}
        if override.get("exclusive"):
            test_tags["exclusive"] = None
        if override.get("manual"):
            test_tags["manual"] = None
        rust_test(
            name = name,
            timeout = override.get("timeout"),
            srcs = support if root.endswith("/main.rs") else [root] + support,
            crate_root = root,
            rustc_env = env,
            compile_data = [WORKSPACE_TEST_DATA],
            data = test_data,
            tags = sorted(test_tags.keys()),
            **kwargs
        )
