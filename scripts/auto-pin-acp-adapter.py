#!/usr/bin/env python3
"""Propose one exact ACP adapter pin update and arm it for the merge queue.

Run from the intentd root with --adapter codex|claude-code and
GH_TOKEN=RELEASE_PLZ_TOKEN, or --dry-run. Each adapter has its own rolling
branch and PR, so one adapter's refusal or hold never blocks another.
Only git objects and the dedicated remote branch are written; the checkout,
index, and local branches are untouched. Normal PR CI tests the proposed pin;
the PR is armed for squash auto-merge, never merged directly, so the merge
queue lands it only after CI passes on the merged tree. `hold-release`
disables auto-merge, and human edits disable the automation's own arming.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import time

from provider_cli_metadata import METADATA, package_snapshot, replace_package


REPO = "intent-hq/intentd"
CONFIG = "crates/intent-providers/src/config.rs"
BOT_NAME = "github-actions[bot]"
BOT_EMAIL = "41898282+github-actions[bot]@users.noreply.github.com"


class Adapter:
    """Everything that differs between managed adapters; the rules do not."""

    def __init__(self, *, key, label, package, branch, pin, pin_name, title, trailer,
                 marker, release_repo, purpose, engines_node=None):
        self.key = key  # --adapter value and adapter-cli-packages.json key
        self.label = label  # commit-message name
        self.package = package
        self.branch = branch
        self.ref = f"refs/heads/{branch}"
        self.pin = pin  # groups: prefix, version, suffix
        self.pin_name = pin_name
        self.title = title
        self.trailer = trailer
        self.marker = marker
        self.release_repo = release_repo
        self.purpose = purpose
        # Exact npm engines.node the daemon's user-facing requirement matches;
        # any other declaration needs a human to update that requirement.
        self.engines_node = engines_node
        self.registry = ("https://registry.npmjs.org/"
                         + package.replace("/", "%2f") + "/latest")


# Codex names, titles, trailers, and markers predate the generalization; keep
# them byte-identical so open and historical rolling PRs remain recognized.
ADAPTERS = {
    "codex": Adapter(
        key="codex",
        label="Codex ACP",
        package="@agentclientprotocol/codex-acp",
        branch="auto/codex-acp-pin",
        pin=re.compile(
            r'^(pub const CODEX_ACP_NPX_PACKAGE: &str = "'
            r'@agentclientprotocol/codex-acp@)([^"\r\n]+)(";)$', re.MULTILINE
        ),
        pin_name="CODEX_ACP_NPX_PACKAGE",
        title="fix: bump Codex ACP fallback to v{}",
        trailer="Auto-pin-Codex-ACP: v1",
        marker="<!-- auto-pin-codex-acp: v1 -->",
        release_repo="agentclientprotocol/codex-acp",
        purpose="The fallback stays pinned to an exact version.",
    ),
    "claude-code": Adapter(
        key="claude-code",
        label="claude-agent-acp",
        package="@agentclientprotocol/claude-agent-acp",
        branch="auto/claude-agent-acp-pin",
        pin=re.compile(
            r'^(macro_rules! claude_agent_acp_version \{\s*\(\) => \{\s*")'
            r'([^"\r\n]+)("\s*\};?\s*\})', re.MULTILINE
        ),
        pin_name="claude_agent_acp_version!",
        title="fix: bump claude-agent-acp pin to v{}",
        trailer="Auto-pin-Claude-Agent-ACP: v1",
        marker="<!-- auto-pin-claude-agent-acp: v1 -->",
        release_repo="agentclientprotocol/claude-agent-acp",
        purpose="The adapter stays pinned to an exact version.",
        # CLAUDE_AGENT_ACP_NODE_REQUIREMENT ("Node.js 22+") in config.rs.
        engines_node=">=22",
    ),
}
# Set by main() from --adapter before any other helper runs; there is no
# default, so a helper used without an adapter fails instead of acting on one.
ADAPTER = None


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
    matches = list(ADAPTER.pin.finditer(content))
    if len(matches) != 1:
        raise Refusal(f"Expected exactly one {ADAPTER.pin_name} literal.")
    value = matches[0][2]
    version(value)
    return value


def replace_pin(content, target):
    read_pin(content)
    return ADAPTER.pin.sub(lambda match: match[1] + target + match[3], content)


def title(target):
    return ADAPTER.title.format(target)


def message(current, target, previous_metadata_hash=None, package_metadata_hash=None):
    return (
        f"{title(target)}\n\n"
        f"Managed {ADAPTER.label} pin: {current} -> {target}.\n"
        f"{ADAPTER.trailer}\n"
    ) + (f"Previous-PR-Metadata-SHA256: {previous_metadata_hash}\n"
         if previous_metadata_hash else "") + (
        f"Adapter-Package-Metadata-SHA256: {package_metadata_hash}\n"
        if package_metadata_hash else "")


def pr_metadata_hash(pr):
    return hashlib.sha256(
        json.dumps([pr["number"], pr["title"], pr["body"]]).encode()
    ).hexdigest()


def body(current, target):
    return (
        f"{ADAPTER.marker}\n"
        f"Update `{ADAPTER.package}` from `{current}` to `{target}` in "
        f"`{ADAPTER.pin_name}`. {ADAPTER.purpose}\n\n"
        f"[Upstream release v{target}]"
        f"(https://github.com/{ADAPTER.release_repo}/releases/tag/v{target}).\n\n"
        "This automation arms squash auto-merge: the merge queue lands the update "
        "once normal PR CI passes, rerunning CI on the merged tree first.\n\n"
        "Add `hold-release` to pause automated updates and disable auto-merge. "
        "Human changes to the branch, title, or description also stop automatic "
        "updates and disable the automation's auto-merge.\n"
    )


ARM_ATTEMPTS = 3


def arm_auto_merge(number, head):
    """Queue the PR once CI passes, pinned to the verified head.

    GitHub syncs a pushed head and a new PR's mergeability asynchronously, so a
    first attempt can race them; retry briefly, then leave it to the next run.
    """
    delay = float(os.environ.get("AUTO_PIN_ARM_RETRY_SECONDS", "15"))
    for attempt in range(1, ARM_ATTEMPTS + 1):
        try:
            run("gh", "pr", "merge", str(number), "--repo", REPO, "--squash", "--auto",
                "--match-head-commit", head)
        except Refusal:
            if attempt < ARM_ATTEMPTS:
                time.sleep(delay)
                continue
            print(f"::warning::Could not arm auto-merge for #{number}; the next run retries.")
            return
        print(f"Armed squash auto-merge for #{number} at {head}.")
        return


def disable_auto_merge(pr, reason, dry_run):
    if dry_run:
        print(f"dry-run: would disable auto-merge for #{pr['number']} ({reason}).")
        return
    try:
        run("gh", "pr", "merge", str(pr["number"]), "--repo", REPO, "--disable-auto")
    except Refusal:
        print(f"::warning::Could not disable auto-merge for #{pr['number']} ({reason}).")
        return
    print(f"Disabled auto-merge for #{pr['number']} ({reason}).")


def armed_by(pr, login):
    auto_merge = pr.get("auto_merge")
    return bool(auto_merge) and (auto_merge.get("enabled_by") or {}).get("login") == login


def fetch_main():
    git("fetch", "--quiet", "--no-tags", "origin", "refs/heads/main")
    return git("rev-parse", "FETCH_HEAD").strip()


def config_at(commit):
    return git("show", f"{commit}:{CONFIG}")


def branch_head():
    result = git("ls-remote", "--heads", "origin", ADAPTER.ref).splitlines()
    if not result:
        return ""
    if len(result) != 1 or result[0].split()[1] != ADAPTER.ref:
        raise Refusal("Unexpected rolling branch lookup result.")
    return result[0].split()[0]


def find_pr():
    # Server-side owner:head and base filters, plus explicit repository checks:
    # a fork PR with an identical branch name must never be edited.
    pages = decode_json(run(
        "gh", "api", "--method", "GET", f"repos/{REPO}/pulls",
        "-f", "state=all", "-f", f"head=intent-hq:{ADAPTER.branch}",
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
                and pr["head"]["ref"] == ADAPTER.branch
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
    git("fetch", "--quiet", "--no-tags", "origin", ADAPTER.ref)
    if git("rev-parse", "FETCH_HEAD").strip() != head:
        raise Refusal("Rolling branch changed during lookup; retry from fresh state.")
    parents = git("rev-list", "--parents", "-n", "1", head).split()
    if len(parents) != 2:
        raise Refusal("Rolling branch has unexpected history; preserving human edits.")
    parent = parents[1]
    git("merge-base", "--is-ancestor", parent, base)
    before, after = config_at(parent), config_at(head)
    current, target = read_pin(before), read_pin(after)
    commit_message = git("show", "-s", "--format=%B", head).strip()
    receipt = re.search(r"\nPrevious-PR-Metadata-SHA256: ([0-9a-f]{64})(?:\n|$)", commit_message)
    previous_metadata_hash = receipt[1] if receipt else None
    metadata_receipt = re.search(r"\nAdapter-Package-Metadata-SHA256: ([0-9a-f]{64})$", commit_message)
    package_metadata_hash = metadata_receipt[1] if metadata_receipt else None
    changed = git("diff-tree", "--no-commit-id", "--name-status", "-r", parent, head).strip()
    expected_changes = f"M\t{CONFIG}"
    if package_metadata_hash:
        metadata = git("show", f"{head}:{METADATA}")
        if hashlib.sha256(metadata.encode()).hexdigest() != package_metadata_hash:
            raise Refusal("Adapter metadata was edited; preserving human changes.")
        previous = decode_json(git("show", f"{parent}:{METADATA}"))
        actual = decode_json(metadata)
        package = actual[ADAPTER.key]["package"]
        expected = decode_json(replace_package(json.dumps(previous), ADAPTER.key, package))
        if actual != expected or package.get("name") != ADAPTER.package or package.get("version") != target:
            raise Refusal("Unexpected adapter metadata changes; preserving human edits.")
        expected_changes = "\n".join(sorted([expected_changes, f"M\t{METADATA}"]))
    identity = git("show", "-s", "--format=%an%n%ae%n%cn%n%ce", head).splitlines()
    expected_identity = [BOT_NAME, BOT_EMAIL, BOT_NAME, BOT_EMAIL]
    if (
        version(target) <= version(current)
        or after != replace_pin(before, target)
        or changed != expected_changes
        or git("ls-tree", parent, CONFIG).split()[0]
        != git("ls-tree", head, CONFIG).split()[0]
        or identity != expected_identity
        or commit_message != message(current, target, previous_metadata_hash, package_metadata_hash).strip()
    ):
        raise Refusal("Rolling branch has unexpected edits; preserving them for human review.")
    return current, target, previous_metadata_hash


def make_commit(base, content, current, target, manifest, previous_metadata_hash=None):
    # A private index keeps staging and the caller's working tree untouched.
    with tempfile.TemporaryDirectory(prefix="acp-adapter-pin-") as directory:
        env = os.environ | {"GIT_INDEX_FILE": str(Path(directory) / "index")}
        blob = git("hash-object", "-w", "--stdin", input=replace_pin(content, target)).strip()
        git("read-tree", base, env=env)
        mode = git("ls-tree", base, CONFIG).split()[0]
        git("update-index", "--cacheinfo", f"{mode},{blob},{CONFIG}", env=env)
        metadata = replace_package(git("show", f"{base}:{METADATA}"), ADAPTER.key, manifest)
        metadata_blob = git("hash-object", "-w", "--stdin", input=metadata).strip()
        metadata_mode = git("ls-tree", base, METADATA).split()[0]
        git("update-index", "--cacheinfo", f"{metadata_mode},{metadata_blob},{METADATA}", env=env)
        package_metadata_hash = hashlib.sha256(metadata.encode()).hexdigest()
        tree = git("write-tree", env=env).strip()
        env |= {
            "GIT_AUTHOR_NAME": BOT_NAME, "GIT_COMMITTER_NAME": BOT_NAME,
            "GIT_AUTHOR_EMAIL": BOT_EMAIL, "GIT_COMMITTER_EMAIL": BOT_EMAIL,
        }
        return git("-c", "commit.gpgsign=false", "commit-tree", tree, "-p", base,
                   input=message(current, target, previous_metadata_hash, package_metadata_hash), env=env).strip()


def node_floor(declared):
    """The major of a plain `>=N`, `>=N.0`, or `>=N.0.0` range, else None."""
    match = isinstance(declared, str) and re.fullmatch(
        r"\s*>=\s*(0|[1-9][0-9]*)(?:\.0){0,2}\s*", declared)
    return int(match[1]) if match else None


def check_engines(manifest):
    if ADAPTER.engines_node is None:
        return
    engines = manifest.get("engines")
    declared = engines.get("node") if isinstance(engines, dict) else None
    if node_floor(declared) != node_floor(ADAPTER.engines_node):
        raise Refusal(
            f"{ADAPTER.package}@{manifest.get('version')} declares engines.node "
            f"{json.dumps(declared)}, not {json.dumps(ADAPTER.engines_node)}; update the "
            "daemon's Node.js requirement and pin by hand, then this automation's "
            "expected engines.node."
        )


def main():
    global ADAPTER
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--adapter", required=True, choices=sorted(ADAPTERS))
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    ADAPTER = ADAPTERS[args.adapter]
    if not os.environ.get("GH_TOKEN") and not args.dry_run:
        print(f"::warning::RELEASE_PLZ_TOKEN is not set; skipping {ADAPTER.package} publication.")
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
        "--max-time", "60", "--retry", "2", ADAPTER.registry,
    ))
    if not isinstance(manifest, dict) or manifest.get("name") != ADAPTER.package:
        raise Refusal(f"Registry response is not the expected {ADAPTER.package} package.")
    try:
        package_snapshot(manifest)
    except ValueError as error:
        raise Refusal("Invalid adapter dependency metadata; stopping publication.") from error
    target = manifest.get("version")
    if version(target) <= version(current):
        print(f"Main pin {current} is not older than npm stable {target}; nothing to do.")
        # An open rolling PR proposes a version npm no longer offers as latest
        # (or main already moved past it); never let our arming land it.
        if os.environ.get("GH_TOKEN"):
            try:
                viewer = decode_json(run("gh", "api", "user"))["login"]
                pr = find_pr()
            except Refusal:
                return
            if pr and armed_by(pr, viewer):
                disable_auto_merge(pr, "npm latest is not newer than main", args.dry_run)
        return

    viewer = decode_json(run("gh", "api", "user"))["login"]
    pr = find_pr()
    if pr and any(label["name"] == "hold-release" for label in pr["labels"]):
        if pr.get("auto_merge"):
            disable_auto_merge(pr, "hold-release", args.dry_run)
        print("Rolling PR has hold-release; leaving it unchanged.")
        return
    head = branch_head()
    try:
        previous, proposed, recorded_metadata_hash = (
            inspect_branch(head, base) if head else (current, None, None)
        )
    except Refusal:
        # Unexpected branch history (e.g. a human push): a human now owns it.
        if pr and armed_by(pr, viewer):
            disable_auto_merge(pr, "human branch changes", args.dry_run)
        raise
    metadata_matches = pr is not None and (
        pr["title"] == title(proposed) and pr["body"] == body(previous, proposed)
    )
    # A push may have succeeded before gh pr edit failed. The new bot commit
    # records the exact generated metadata verified before that push. Accept
    # only that recorded state, not arbitrary older/generated-looking text.
    recover_metadata = pr is not None and recorded_metadata_hash == pr_metadata_hash(pr)
    if pr and (
        not head or pr["user"]["login"] != viewer or pr["head"]["sha"] != head
        or not (metadata_matches or recover_metadata)
        or pr["draft"]
    ):
        # A human now owns the PR; never let the automation's arming merge it.
        if armed_by(pr, viewer):
            disable_auto_merge(pr, "human edits", args.dry_run)
        raise Refusal("Rolling PR has unexpected ownership or edits; preserving it.")
    if proposed and version(proposed) > version(target):
        print(f"Rolling branch already proposes newer version {proposed}; nothing to do.")
        # npm latest moved back below the proposal (withdrawn or deprecated).
        if pr and armed_by(pr, viewer):
            disable_auto_merge(pr, f"npm latest {target} is older than {proposed}",
                               args.dry_run)
        return
    # Only now is `target` about to be proposed or armed. Refuse before any
    # publication, but after the hold/human/withdrawn-version disarms above so
    # an unguarded npm latest never strands an armed proposal.
    check_engines(manifest)
    if proposed == target and pr and metadata_matches:
        print(f"Rolling PR already proposes {target}; no commit, push, or PR edit needed.")
        if not pr.get("auto_merge"):
            if args.dry_run:
                print(f"dry-run: would arm auto-merge for #{pr['number']}.")
            else:
                arm_auto_merge(pr["number"], head)
        return
    if args.dry_run:
        arming = "" if pr and pr.get("auto_merge") else " and arm auto-merge"
        if proposed == target and recover_metadata:
            print(f"dry-run: would repair PR metadata for {target}{arming}; no publication.")
        else:
            print(f"dry-run: would propose {current} -> {target} on {ADAPTER.branch} and arm "
                  "auto-merge; no publication.")
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

    commit = head if proposed == target else make_commit(
        base, content, current, target, manifest, pr_metadata_hash(pr) if pr else None,
    )
    if commit != head:
        git("push", f"--force-with-lease={ADAPTER.ref}:{head}", "origin", f"{commit}:{ADAPTER.ref}")
    with tempfile.TemporaryDirectory(prefix="acp-adapter-pr-") as directory:
        description = Path(directory) / "body.txt"
        description.write_text(body(previous if proposed == target else current, target))
        if pr:
            run("gh", "pr", "edit", str(pr["number"]), "--repo", REPO,
                "--title", title(target), "--body-file", str(description))
            print(f"Updated https://github.com/{REPO}/pull/{pr['number']} for {target}.")
            number = pr["number"]
        else:
            url = run("gh", "pr", "create", "--repo", REPO, "--base", "main",
                      "--head", ADAPTER.branch, "--title", title(target),
                      "--body-file", str(description)).strip()
            print(f"Opened {url} for {target}.")
            match = re.fullmatch(rf"https://github\.com/{REPO}/pull/([0-9]+)", url)
            if not match:
                print("::warning::Unexpected PR URL; the next run arms auto-merge.")
                return
            number = int(match.group(1))
    # Arm once, pinned to the verified head. GitHub keeps auto-merge enabled
    # across pushes by a writer (this bot), and an existing arming, possibly a
    # human's with their chosen method, is never overridden.
    if not (pr and pr.get("auto_merge")):
        arm_auto_merge(number, commit)


if __name__ == "__main__":
    try:
        main()
    except (Refusal, KeyError, TypeError, ValueError, IndexError) as error:
        detail = str(error) if isinstance(error, Refusal) else "Unexpected response shape."
        print(f"::error::{detail}", file=sys.stderr)
        sys.exit(1)
