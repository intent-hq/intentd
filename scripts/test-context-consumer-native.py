#!/usr/bin/env python3
"""Require discovered and executed context regressions in the native services crate."""
import json
import os
from pathlib import Path
import re
import subprocess


PREFIX = "repository_read_policy::tests::context_consumer::"
REQUIRED = {
    PREFIX + name for name in (
        "context_consumer_saved_primary_choice_and_reset_control_actual_native_read",
        "context_consumer_real_narrow_root_and_choice_mutations_classify_optional_and_required",
        "context_consumer_required_settings_and_secret_replacement_keep_all_records_required",
        "context_consumer_ledger_counts_distinct_calls_at_sixty_four_and_sixty_five",
        "context_consumer_boundary_ignores_ambient_git_config",
        "context_consumer_isolation_keeps_primary_git_mutation_refusals",
        "isolation::child_config_isolation_preserves_local_config_caller_and_coverage_overrides",
        "isolation::cleanup_tests::failure_cleanup_stops_descendants_and_removes_owned_scratch",
        # This entry point is driven by the cleanup regression, not a ninth behavior.
        "isolation::cleanup_tests::cleanup_probe",
    )
}
COMMAND = ["cargo", "test", "--locked", "-p", "intent-services", "--lib", PREFIX]


def discover(output):
    names = re.findall(r"^(\S+): test$", output, re.M)
    if len(names) != len(set(names)) or any(not n.startswith(PREFIX) for n in names):
        raise RuntimeError("unexpected or duplicate context test discovery")
    missing = REQUIRED - set(names)
    if missing:
        raise RuntimeError("missing required context regressions: " + ", ".join(sorted(missing)))
    return sorted(names)


def require_execution(output, names):
    passed = re.findall(r"^test (\S+) \.\.\. ok$", output, re.M)
    results = re.findall(r"^test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored;", output, re.M)
    if sorted(passed) != names or results != [(str(len(names)), "0", "0")]:
        raise RuntimeError("context regressions did not all execute successfully (missing, failed or ignored)")


def run(args, env, log):
    with log.open("w", encoding="utf-8") as evidence:
        def emit(text):
            print(text, end="", flush=True)
            evidence.write(text)
            evidence.flush()

        emit("+ " + json.dumps(args) + "\n")
        with subprocess.Popen(args, env=env, text=True, encoding="utf-8", errors="replace",
                              stdout=subprocess.PIPE, stderr=subprocess.STDOUT) as process:
            lines = []
            for line in process.stdout:
                emit(line)
                lines.append(line)
            code = process.wait()
        emit(f"exit_code={code}\n")
    output = "".join(lines)
    if code:
        raise subprocess.CalledProcessError(code, args, output)
    return output


def main():
    directory = Path(os.environ["CONTEXT_CONSUMER_EVIDENCE"])
    directory.mkdir(parents=True, exist_ok=False)
    env = dict(os.environ, INTENTD_ASSERT_BOUND_CALLER="1", INTENTD_TEST_TIMEOUT_MULTIPLIER="1",
               CARGO_TERM_PROGRESS_WHEN="never", CARGO_TERM_COLOR="never")
    receipt = {"success": False, "caller_policy": "1", "timeout_multiplier": "1"}
    try:
        receipt["source_sha"] = run(["git", "rev-parse", "HEAD"], env, directory / "source.log").strip()
        run(["git", "--version"], env, directory / "git.log")
        receipt["toolchain"] = run(["rustc", "-vV"], env, directory / "rustc.log")
        host = re.search(r"^host: (\S+)$", receipt["toolchain"], re.M).group(1)
        receipt["target"] = host
        expected = {"Windows": "x86_64-pc-windows-msvc", "macOS": "aarch64-apple-darwin"}
        if os.environ.get("RUNNER_OS") in expected and host != expected[os.environ["RUNNER_OS"]]:
            raise RuntimeError(f"wrong native toolchain host: {host}")
        if env.get("CARGO_BUILD_TARGET", host) != host:
            raise RuntimeError("context proof requires the native build target")
        print(json.dumps(receipt, indent=2), flush=True)
        # Cargo resolves native executable suffixes and dynamic library paths on
        # Windows/macOS; no POSIX path translation or direct binary launch needed.
        names = discover(run(COMMAND + ["--", "--list", "--format=terse"], env, directory / "discovery.log"))
        receipt["discovered"] = names
        (directory / "discovered.json").write_text(json.dumps(names, indent=2) + "\n", encoding="utf-8")
        output = run(COMMAND + ["--", "--test-threads=1", "--format=pretty", "--color=never"],
                     env, directory / "execution.log")
        require_execution(output, names)
        receipt["success"] = True
    except Exception as error:
        receipt["error"] = str(error)
        raise
    finally:
        (directory / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
