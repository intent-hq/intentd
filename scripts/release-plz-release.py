#!/usr/bin/env python3
"""Run release once; preserve failures and report safe manual retry guidance.

No retry is safe solely from an HTTP status: release-plz may already have
pushed a tag. Preserve the release-before-release-PR gate.
"""

import json
import os
from pathlib import Path
import re
import subprocess
import sys


def is_quota_failure(log):
    # Require one complete structured error from the commit/PR read seen
    # in #5853. Do not combine a quota mention with a different HTTP error.
    for match in re.finditer(r"Response body:\s*(\{)", log):
        try:
            body, end = json.JSONDecoder().raw_decode(log[match.start(1):])
        except ValueError:
            continue
        if str(body.get("status")) != "403":
            continue
        if not re.match(
            r"\s*Caused by:\s*HTTP status client error \(403 Forbidden\) for url "
            r"\(https://api\.github\.com/repos/[^/\s]+/[^/\s]+/commits/[0-9a-f]+/pulls\)",
            log[match.start(1) + end:],
        ):
            continue
        message = body.get("message", "")
        if isinstance(message, str) and message.startswith("API rate limit exceeded for "):
            return True
    return False


def output(name, value):
    if path := os.environ.get("GITHUB_OUTPUT"):
        with Path(path).open("a") as stream:
            stream.write(f"{name}={value}\n")


def report_failure(log):
    quota = is_quota_failure(log)
    try:
        output("release_state", "quota_blocked" if quota else "failed")
    except OSError as error:
        print(f"Could not write release failure output: {error}", file=sys.stderr)
    lines = [
        "## Release quota blocked" if quota else "## Release failed",
        "Release PR remains blocked because the release command did not finish successfully.",
        "The command was invoked once; no automatic retry was attempted.",
        "Before any retry, inspect the failed run, remote tags for this commit/version, "
        "and downstream release runs for partial publication. Do not delete or move "
        "existing tags. A failed command does not prove that nothing was published.",
    ]
    if quota:
        lines.append(
            "GitHub reported a classified API quota failure. Use the quota for "
            "RELEASE_PLZ_TOKEN; a successful request with another token does not prove recovery."
        )
        # The pinned release-plz error has no response-header envelope.
        # Headers elsewhere in stderr cannot supply this response's timing.
        lines.append(
            "The reset time was not supplied in the classified release-plz response. "
            "Check GitHub's X-RateLimit-Reset / Retry-After response headers using "
            "the same token before retrying; do not assume the quota has reset."
        )
        lines.append(
            "After confirmed quota recovery and tag reconciliation, "
            "request one manual failed-jobs rerun. If quota persists, stop and investigate "
            "the token's shared request load instead of repeatedly rerunning publication."
        )
        repo = os.environ.get("GITHUB_REPOSITORY", "")
        run_id = os.environ.get("GITHUB_RUN_ID", "")
        if re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repo) and re.fullmatch(r"[0-9]+", run_id):
            lines.append(f"Manual command after those checks: `gh run rerun {run_id} --repo {repo} --failed`.")
    else:
        lines.append(
            "This is not a classified quota failure. Inspect the original error for "
            "authentication, permission, configuration, or publication failures; "
            "fix the cause before considering a retry."
        )
    summary = "\n\n".join(lines) + "\n"
    print(summary, file=sys.stderr)
    if path := os.environ.get("GITHUB_STEP_SUMMARY"):
        try:
            with Path(path).open("a") as stream:
                stream.write(summary)
        except OSError as error:
            # Diagnostics must not replace the publication command's exit.
            print(f"Could not write release failure summary: {error}", file=sys.stderr)


def main():
    token = os.environ.get("GITHUB_TOKEN", "")
    if not token:
        print("GITHUB_TOKEN is required for release-plz release", file=sys.stderr)
        return 1
    # Match release-plz/action v0.5.133's release argv. Configuration is still
    # discovered by release-plz; its tag/existing-release guards stay intact.
    try:
        result = subprocess.run(
            ["release-plz", "release", "--git-token", token, "--forge", "github", "-o", "json"],
            capture_output=True, text=True,
        )
    except OSError as error:
        print(f"Could not start release-plz: {error}", file=sys.stderr)
        return 1
    stdout = result.stdout.replace(token, "***")
    stderr = result.stderr.replace(token, "***")
    print(stdout, end="")
    print(stderr, end="", file=sys.stderr)
    if result.returncode:
        # Strip terminal colors only for classification; keep original logs.
        log = re.sub(r"\x1b\[[0-9;]*m", "", stderr)
        report_failure(log)
        return result.returncode if result.returncode > 0 else 128 - result.returncode
    try:
        releases = json.loads(stdout)["releases"]
        if not isinstance(releases, list):
            raise ValueError("releases must be an array")
    except (ValueError, KeyError, TypeError):
        print("release-plz did not return a valid releases array", file=sys.stderr)
        report_failure("")
        return 1
    output("releases", json.dumps(releases, separators=(",", ":")))
    output("releases_created", str(bool(releases)).lower())
    output("release_state", "success")
    return 0


if __name__ == "__main__":
    sys.exit(main())
