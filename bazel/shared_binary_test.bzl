"""Run an existing test binary as another test target, with its own arguments.

scripts/bazel-gen.py uses this to split one compiled Rust test binary into
several Bazel tests (module shards, serialized tests, fixture or known-failing
companions) without compiling the crate again for each. The binary's runfiles
and run environment are forwarded unchanged; `args`, `tags`, `timeout` and the
other common test attributes apply to the runner target.

With `include_modules` or `exclude_modules` the target runs only the tests
whose top-level module (the test name up to the first `::`) is in
`include_modules` and not in `exclude_modules`, through
bazel/module_shard_test.sh. libtest's own filters are substring matches and
cannot express that.
"""

def _shared_binary_test_impl(ctx):
    binary = ctx.attr.binary
    executable = binary[DefaultInfo].files_to_run.executable
    out = ctx.actions.declare_file(ctx.label.name)
    if ctx.attr.include_modules or ctx.attr.exclude_modules:
        path = executable.short_path
        path = path[3:] if path.startswith("../") else ctx.workspace_name + "/" + path
        ctx.actions.expand_template(
            template = ctx.file._module_shard_launcher,
            output = out,
            substitutions = {
                "@@BINARY@@": '"${TEST_SRCDIR}/%s"' % path,
                "@@INCLUDE@@": " ".join(ctx.attr.include_modules),
                "@@EXCLUDE@@": " ".join(ctx.attr.exclude_modules),
            },
            is_executable = True,
        )
    else:
        ctx.actions.symlink(output = out, target_file = executable, is_executable = True)
    runfiles = ctx.runfiles(files = [executable]).merge(binary[DefaultInfo].default_runfiles)

    environment = {}
    inherited = list(ctx.attr.env_inherit)
    if RunEnvironmentInfo in binary:
        environment = dict(binary[RunEnvironmentInfo].environment)
        inherited = binary[RunEnvironmentInfo].inherited_environment + inherited
    return [
        DefaultInfo(executable = out, runfiles = runfiles),
        RunEnvironmentInfo(environment = environment, inherited_environment = inherited),
    ]

shared_binary_test = rule(
    implementation = _shared_binary_test_impl,
    test = True,
    attrs = {
        "binary": attr.label(
            doc = "The test binary to run (for example a rust_test).",
            executable = True,
            cfg = "target",
            mandatory = True,
        ),
        "env_inherit": attr.string_list(
            doc = "Extra client environment variables passed to the test.",
        ),
        "exclude_modules": attr.string_list(
            doc = "Skip tests whose top-level module is one of these.",
        ),
        "include_modules": attr.string_list(
            doc = "Run only tests whose top-level module is one of these.",
        ),
        "_module_shard_launcher": attr.label(
            default = Label("//bazel:module_shard_test.sh"),
            allow_single_file = True,
        ),
    },
)
