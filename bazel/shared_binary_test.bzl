"""Run an existing test binary as another test target, with its own arguments.

scripts/bazel-gen.py uses this to split one compiled Rust test binary into
several Bazel tests (module shards, serialized tests, fixture or known-failing
companions) without compiling the crate again for each. The binary's runfiles
and run environment are forwarded unchanged; `args`, `tags`, `timeout` and the
other common test attributes apply to the runner target.
"""

def _shared_binary_test_impl(ctx):
    binary = ctx.attr.binary
    executable = binary[DefaultInfo].files_to_run.executable
    out = ctx.actions.declare_file(ctx.label.name)
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
    },
)
