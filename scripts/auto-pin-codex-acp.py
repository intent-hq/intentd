#!/usr/bin/env python3
"""Propose one reviewed, exact Codex ACP pin update; never merge it.

Run from the intentd root with GH_TOKEN=RELEASE_PLZ_TOKEN, or --dry-run.
Only git objects and the dedicated remote branch are written; the checkout,
index, and local branches are untouched. Normal PR CI tests the proposed pin.
"""

import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile


REPO = "intent-hq/intentd"
BRANCH = "auto/codex-acp-pin"
REF = f"refs/heads/{BRANCH}"
CONFIG = "crates/intent-providers/src/config.rs"
PACKAGE = "@agentclientprotocol/codex-acp"
REGISTRY = "https://registry.npmjs.org/@agentclientprotocol%2fcodex-acp/latest"
BOT_NAME = "github-actions[bot]"
BOT_EMAIL = "41898282+github-actions[bot]@users.noreply.github.com"
PIN = re.compile(
    r'^(pub const CODEX_ACP_NPX_PACKAGE: &str = "'
    r'@agentclientprotocol/codex-acp@)([^"\r\n]+)(";)$', re.MULTILINE
)


class Refusal(Exception):
    """A failed prerequisite must stop publication, without logging credentials."""


def run(*args, input=None, env=None):
    try:
        result = subprocess.run(
            args, input=input, text=True, capture_output=True,
            env=env, timeout=120, check=True,
        )
    except (OSError, subprocess.SubprocessError) as error:
        # stderr/argv can contain credentials supplied by git helpers or gh.
        raise Refusal(f"{args[0]} {args[1]} failed; stopping publication.") from error
    return result.stdout


def git(*args, **kwargs):
    return run("git", *args, **kwargs)


def decode_json(value):
    try:
        return json.loads(value)
    except ValueError as error:
        raise Refusal("Invalid JSON from registry or GitHub; stopping publication.") from error


def version(value):
    if not isinstance(value, str) or not re.fullmatch(
        r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)", value
    ):
        raise Refusal("Expected an exact stable numeric SemVer; stopping publication.")
    return tuple(int(part) for part in value.split("."))


def read_pin(content):
    matches = list(PIN.finditer(content))
    if len(matches) != 1:
        raise Refusal("Expected exactly one CODEX_ACP_NPX_PACKAGE literal.")
    value = matches[0][2]
    version(value)
    return value


def replace_pin(content, target):
    read_pin(content)
    return PIN.sub(lambda match: match[1] + target + match[3], content)


def title(target):
    return f"fix: bump Codex ACP fallback to v{target}"


def message(current, target):
    return (
        f"{title(target)}\n\n"
        f"Managed Codex ACP pin: {current} -> {target}.\n"
        "Auto-pin-Codex-ACP: v1\n"
    )


def body(current, target):
    return (
        "<!-- auto-pin-codex-acp: v1 -->\n"
        f"Update `{PACKAGE}` from `{current}` to `{target}` in "
        "`CODEX_ACP_NPX_PACKAGE`. The fallback stays pinned to an exact version.\n\n"
        f"[Upstream release v{target}]"
        f"(https://github.com/agentclientprotocol/codex-acp/releases/tag/v{target}).\n\n"
        "Normal PR CI must pass, and a human must approve the merge. "
        "This automation never merges PRs.\n\n"
        "Add `hold-release` to pause automated updates. Human changes to the "
        "branch, title, or description also stop automatic updates.\n"
    )


def fetch_main():
    git("fetch", "--quiet", "--no-tags", "origin", "refs/heads/main")
    return git("rev-parse", "FETCH_HEAD").strip()


def config_at(commit):
    return git("show", f"{commit}:{CONFIG}")


def branch_head():
    result = git("ls-remote", "--heads", "origin", REF).splitlines()
    if not result:
        return ""
    if len(result) != 1 or result[0].split()[1] != REF:
        raise Refusal("Unexpected rolling branch lookup result.")
    return result[0].split()[0]


def find_pr():
    # Server-side owner:head and base filters, plus explicit repository checks:
    # a fork PR with an identical branch name must never be edited.
    pages = decode_json(run(
        "gh", "api", "--method", "GET", f"repos/{REPO}/pulls",
        "-f", "state=all", "-f", f"head=intent-hq:{BRANCH}",
        "-f", "base=main", "-f", "per_page=100", "--paginate", "--slurp",
    ))
    if not isinstance(pages, list) or any(not isinstance(page, list) for page in pages):
        raise Refusal("Unexpected GitHub pull request response.")
    matches = []
    for page in pages:
        for pr in page:
            if (
                pr["head"].get("repo") is not None
                and pr["head"]["repo"]["full_name"] == REPO
                and pr["base"]["repo"]["full_name"] == REPO
                and pr["head"]["ref"] == BRANCH
                and pr["base"]["ref"] == "main"
            ):
                matches.append(pr)
    opened = [pr for pr in matches if pr["state"] == "open"]
    if len(opened) > 1:
        raise Refusal("Multiple same-repository rolling PRs; inspect them manually.")
    if opened:
        return opened[0]
    if matches and not max(matches, key=lambda pr: pr["number"])["merged_at"]:
        raise Refusal("The last rolling PR was closed without merging; reopen it to resume.")
    return None


def inspect_branch(head, base):
    git("fetch", "--quiet", "--no-tags", "origin", REF)
    if git("rev-parse", "FETCH_HEAD").strip() != head:
        raise Refusal("Rolling branch changed during lookup; retry from fresh state.")
    parents = git("rev-list", "--parents", "-n", "1", head).split()
    if len(parents) != 2:
        raise Refusal("Rolling branch has unexpected history; preserving human edits.")
    parent = parents[1]
    git("merge-base", "--is-ancestor", parent, base)
    before, after = config_at(parent), config_at(head)
    current, target = read_pin(before), read_pin(after)
    identity = git("show", "-s", "--format=%an%n%ae%n%cn%n%ce", head).splitlines()
    expected_identity = [BOT_NAME, BOT_EMAIL, BOT_NAME, BOT_EMAIL]
    if (
        version(target) <= version(current)
        or after != replace_pin(before, target)
        or git("diff-tree", "--no-commit-id", "--name-status", "-r", parent, head).strip()
        != f"M\t{CONFIG}"
        or git("ls-tree", parent, CONFIG).split()[0]
        != git("ls-tree", head, CONFIG).split()[0]
        or identity != expected_identity
        or git("show", "-s", "--format=%B", head).strip() != message(current, target).strip()
    ):
        raise Refusal("Rolling branch has unexpected edits; preserving them for human review.")
    return current, target


def make_commit(base, content, current, target):
    # A private index keeps staging and the caller's working tree untouched.
    with tempfile.TemporaryDirectory(prefix="codex-acp-pin-") as directory:
        env = os.environ | {"GIT_INDEX_FILE": str(Path(directory) / "index")}
        blob = git("hash-object", "-w", "--stdin", input=replace_pin(content, target)).strip()
        git("read-tree", base, env=env)
        mode = git("ls-tree", base, CONFIG).split()[0]
        git("update-index", "--cacheinfo", f"{mode},{blob},{CONFIG}", env=env)
        tree = git("write-tree", env=env).strip()
        env |= {
            "GIT_AUTHOR_NAME": BOT_NAME, "GIT_COMMITTER_NAME": BOT_NAME,
            "GIT_AUTHOR_EMAIL": BOT_EMAIL, "GIT_COMMITTER_EMAIL": BOT_EMAIL,
        }
        return git("-c", "commit.gpgsign=false", "commit-tree", tree, "-p", base,
                   input=message(current, target), env=env).strip()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    if not os.environ.get("GH_TOKEN") and not args.dry_run:
        print("::warning::RELEASE_PLZ_TOKEN is not set; skipping Codex ACP publication.")
        return
    if os.environ.get("GITHUB_REPOSITORY", REPO) != REPO:
        raise Refusal("This automation only publishes to intent-hq/intentd.")
    origin = git("config", "--get", "remote.origin.url").strip()
    if origin not in (f"https://github.com/{REPO}.git", f"https://github.com/{REPO}",
                      f"git@github.com:{REPO}.git"):
        raise Refusal("Origin is not the expected intent-hq/intentd repository.")

    base = fetch_main()
    content = config_at(base)
    current = read_pin(content)
    manifest = decode_json(run(
        "curl", "--fail", "--silent", "--show-error", "--location",
        "--max-time", "60", "--retry", "2", REGISTRY,
    ))
    if not isinstance(manifest, dict) or manifest.get("name") != PACKAGE:
        raise Refusal("Registry response is not the expected Codex ACP package.")
    target = manifest.get("version")
    if version(target) <= version(current):
        print(f"Main pin {current} is not older than npm stable {target}; nothing to do.")
        return

    viewer = decode_json(run("gh", "api", "user"))["login"]
    pr = find_pr()
    if pr and any(label["name"] == "hold-release" for label in pr["labels"]):
        print("Rolling PR has hold-release; leaving it unchanged.")
        return
    head = branch_head()
    previous, proposed = inspect_branch(head, base) if head else (current, None)
    if pr and (
        not head or pr["user"]["login"] != viewer or pr["head"]["sha"] != head
        or pr["title"] != title(proposed) or pr["body"] != body(previous, proposed)
        or pr["draft"]
    ):
        raise Refusal("Rolling PR has unexpected ownership or edits; preserving it.")
    if proposed and version(proposed) > version(target):
        print(f"Rolling branch already proposes newer version {proposed}; nothing to do.")
        return
    if proposed == target and pr:
        print(f"Rolling PR already proposes {target}; no commit, push, or PR edit needed.")
        return
    if args.dry_run:
        print(f"dry-run: would propose {current} -> {target} on {BRANCH}; no publication.")
        return

    # Re-read metadata and the remote refs before creating or publishing a commit.
    # Reject any movement, including unrelated main changes; the next daily run
    # starts from a fresh base. A just-merged/deleted branch cannot be recreated.
    if find_pr() != pr:
        raise Refusal("Rolling PR changed during this run; retry from fresh state.")
    live = fetch_main()
    if live != base:
        print(f"Live main changed (pin {read_pin(config_at(live))}); skipping publication.")
        return
    if branch_head() != head:
        raise Refusal("Rolling branch changed during this run; preserving concurrent edits.")

    commit = head if proposed == target else make_commit(base, content, current, target)
    if commit != head:
        git("push", f"--force-with-lease={REF}:{head}", "origin", f"{commit}:{REF}")
    with tempfile.TemporaryDirectory(prefix="codex-acp-pr-") as directory:
        description = Path(directory) / "body.txt"
        description.write_text(body(previous if proposed == target else current, target))
        if pr:
            run("gh", "pr", "edit", str(pr["number"]), "--repo", REPO,
                "--title", title(target), "--body-file", str(description))
            print(f"Updated https://github.com/{REPO}/pull/{pr['number']} for {target}.")
        else:
            url = run("gh", "pr", "create", "--repo", REPO, "--base", "main",
                      "--head", BRANCH, "--title", title(target),
                      "--body-file", str(description)).strip()
            print(f"Opened {url} for {target}; human merge approval is required.")


if __name__ == "__main__":
    try:
        main()
    except (Refusal, KeyError, TypeError, ValueError, IndexError) as error:
        detail = str(error) if isinstance(error, Refusal) else "Unexpected response shape."
        print(f"::error::{detail}", file=sys.stderr)
        sys.exit(1)
