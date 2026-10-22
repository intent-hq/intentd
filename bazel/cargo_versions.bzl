"""Workspace-crate versions, read from the Cargo manifests when Bazel loads the build.

release-plz bumps every crate's [package] version on each release without
touching Bazel files. The generated BUILD files therefore carry no versions:
they load `CARGO_VERSIONS` from `@cargo_versions//:versions.bzl`, which this
extension writes from the current Cargo.toml files. Bazel watches the
manifests it reads here, so a version bump re-runs the extension and rebuilds
with the new CARGO_PKG_VERSION, with no regeneration or lock refresh. The
extension is marked reproducible, so MODULE.bazel.lock does not record it.

bazel/generated.MODULE.bazel registers one `cargo_versions.crate` tag per
workspace crate (scripts/bazel-gen.py writes it).
"""

def _value(line):
    """The right-hand side of a `key = value` TOML line, without a trailing comment."""
    value = line.partition("=")[2].strip()
    if value.startswith("\""):
        return value.split("\"")[1]
    return value.partition("#")[0].strip()

def _section_version(manifest, section):
    """`version` in a TOML [section]: a string, "workspace" when inherited, or None."""
    current = None
    for raw in manifest.splitlines():
        line = raw.strip()
        if line.startswith("["):
            current = line.partition("#")[0].strip()
            continue
        if current != section:
            continue
        key = line.partition("=")[0].strip()
        if key == "version":
            value = _value(line)
            if value.replace(" ", "") == "{workspace=true}":
                return "workspace"
            return value
        if key == "version.workspace" and _value(line) == "true":
            return "workspace"
    return None

def package_version(manifest, workspace_manifest, crate):
    """The [package] version of a Cargo.toml.

    Args:
      manifest: the crate's Cargo.toml text.
      workspace_manifest: a function returning the workspace Cargo.toml text,
        called only when the crate inherits `version.workspace = true`.
      crate: the crate name, for error messages.

    Returns:
      The version string.
    """
    version = _section_version(manifest, "[package]")
    if version == "workspace":
        version = _section_version(workspace_manifest(), "[workspace.package]")
    if not version or version == "workspace":
        fail("cargo_versions: no [package] version for crate %s" % crate)
    return version

def _cargo_versions_repo_impl(rctx):
    rctx.file("BUILD.bazel", "")
    rctx.file("versions.bzl", "CARGO_VERSIONS = %s\n" % repr(rctx.attr.versions))

_cargo_versions_repo = repository_rule(
    implementation = _cargo_versions_repo_impl,
    attrs = {"versions": attr.string_dict()},
)

def _cargo_versions_impl(mctx):
    versions = {}
    workspace = Label("//:Cargo.toml")
    for mod in mctx.modules:
        for tag in mod.tags.crate:
            versions[tag.name] = package_version(
                mctx.read(tag.manifest),
                lambda: mctx.read(workspace),
                tag.name,
            )
    _cargo_versions_repo(name = "cargo_versions", versions = versions)
    return mctx.extension_metadata(reproducible = True)

cargo_versions = module_extension(
    implementation = _cargo_versions_impl,
    tag_classes = {
        "crate": tag_class(attrs = {
            "name": attr.string(mandatory = True),
            "manifest": attr.label(mandatory = True),
        }),
    },
)
