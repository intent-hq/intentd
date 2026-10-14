#!/usr/bin/env python3
"""Read-only safety assessment of captured GitHub API responses, from stdin.

This is an investigation tool, NOT an automatic retirement implementation.
Every decision is a skip. No credentials, network requests, or mutations are
implemented. --dry-run is optional and there is deliberately no --apply mode.

Input bundle (raw JSON response bodies, collected with read-only gh api calls):
  repository: intent-hq/intentd
  workflow: GET /repos/{repository}/actions/workflows/ci.yml
  run_pages: all pages of GET /repos/{repository}/actions/workflows/{id}/runs
             filtered by event=merge_group (optionally status=queued, etc.)
  refreshed_runs: subsequent GET /repos/{repository}/actions/runs/{id} bodies
  refs: GET /repos/{repository}/git/matching-refs/heads/gh-readonly-queue/
  queue_pages: GraphQL query response pages, repository.mergeQueue(branch:"main")
               .entries { nodes { id state headCommit { oid } }
                          pageInfo { hasNextPage endCursor } }

Responses are observations, never an atomic snapshot or cancellation proof.
"complete" means only that the supplied pages pass consistency checks; it
does not establish freshness, transactional consistency, or ref permanence.
Errors must be preserved, never converted to empty lists. Events are not inputs
to policy: destroyed/deleted/ejected hints cannot grant cancellation authority.

Why no mutation capability (reviewed 2026-10-14):
https://docs.github.com/en/rest/actions/workflow-runs#cancel-a-workflow-run
addresses a run ID, not an attempt, ref, SHA or queue incarnation. Conditional
unsafe requests are unsupported unless explicitly documented by the endpoint:
https://docs.github.com/en/enterprise-cloud@latest/rest/using-the-rest-api/best-practices-for-using-the-rest-api#use-conditional-requests
Reruns retain the original SHA/ref and increment the attempt under the run ID:
https://docs.github.com/en/actions/how-tos/manage-workflow-runs/re-run-workflows-and-jobs
Repeated reads cannot close the final read/cancel gap. Missing/replaced refs
are useful observations, but not documented permanent retirement certificates.
"historical-attempt" is a positive identification of obsolete attempt metadata;
the REST cancel endpoint would target the current attempt instead. Even a
completed observation is a no-op, never authority for a subsequent POST.
"""

import argparse
import json
import re
import sys


REPOSITORY = "intent-hq/intentd"
WORKFLOW = ".github/workflows/ci.yml"
PREFIX = "gh-readonly-queue/main/"
SHA = re.compile(r"[0-9a-f]{40}")
IDENTITY = ("id", "run_attempt", "workflow_id", "path", "event", "repository",
            "head_repository", "head_branch", "head_sha")
LIVE = {"queued", "in_progress", "waiting", "pending", "requested"}
MAX_PAGES = 10
MAX_RUNS = 1000  # GitHub caps filtered workflow-run searches at 1,000 results.


def require(condition, message):
    if not condition:
        raise ValueError(message)


def positive_int(value):
    return type(value) is int and value > 0


def valid_sha(value):
    return isinstance(value, str) and SHA.fullmatch(value) is not None


def collect_runs(pages):
    require(isinstance(pages, list) and 0 < len(pages) <= MAX_PAGES, "invalid run pages")
    runs, seen, total = [], set(), None
    for page in pages:
        require(isinstance(page, dict), "invalid run page")
        count = page.get("total_count")
        require(type(count) is int and 0 <= count <= MAX_RUNS, "invalid or truncated run count")
        require(total is None or total == count, "run count changed during pagination")
        total = count
        rows = page.get("workflow_runs")
        require(isinstance(rows, list) and len(rows) <= 100, "invalid run rows")
        for row in rows:
            require(isinstance(row, dict) and positive_int(row.get("id")), "invalid run ID")
            require(row["id"] not in seen, "duplicate run across pages")
            seen.add(row["id"])
            runs.append(row)
    require(len(runs) == total, "incomplete run pages")
    return runs


def collect_heads(pages):
    require(isinstance(pages, list) and 0 < len(pages) <= MAX_PAGES, "invalid queue pages")
    heads, cursors, ids, unknown = set(), set(), set(), False
    for index, page in enumerate(pages):
        require(isinstance(page, dict) and not page.get("errors"), "GraphQL error")
        entries = page["data"]["repository"]["mergeQueue"]["entries"]
        nodes, info = entries["nodes"], entries["pageInfo"]
        require(isinstance(nodes, list) and len(nodes) <= 100, "invalid queue nodes")
        more = info["hasNextPage"]
        require(type(more) is bool and more == (index < len(pages) - 1), "incomplete queue pages")
        cursor = info["endCursor"]
        if more:
            require(isinstance(cursor, str) and cursor and cursor not in cursors, "invalid queue cursor")
            cursors.add(cursor)
        for node in nodes:
            require(isinstance(node["id"], str) and node["id"] not in ids, "invalid queue entry ID")
            ids.add(node["id"])
            require(node["state"] in {"QUEUED", "AWAITING_CHECKS", "MERGEABLE", "UNMERGEABLE", "LOCKED"},
                    "unknown queue state")
            if node["headCommit"] is None:
                unknown = True
            else:
                oid = node["headCommit"]["oid"]
                require(valid_sha(oid), "invalid queue head")
                heads.add(oid)
    return heads, unknown


def collect_refs(rows):
    require(isinstance(rows, list), "invalid refs response")
    refs = {}
    for row in rows:
        name, obj = row["ref"], row["object"]
        require(isinstance(name, str) and name.startswith("refs/heads/gh-readonly-queue/"), "invalid queue ref")
        require(name not in refs and obj["type"] == "commit" and valid_sha(obj["sha"]), "invalid ref identity")
        refs[name] = obj["sha"]
    return refs


def in_scope(run, workflow_id):
    return (run.get("repository", {}).get("full_name") == REPOSITORY
            and run.get("head_repository", {}).get("full_name") == REPOSITORY
            and run.get("event") == "merge_group" and run.get("path") == WORKFLOW
            and run.get("workflow_id") == workflow_id
            and isinstance(run.get("head_branch"), str)
            and run["head_branch"].startswith(PREFIX))


def identity(run):
    values = {key: run.get(key) for key in IDENTITY}
    for key in ("repository", "head_repository"):
        values[key] = run.get(key, {}).get("full_name")
    return values


def assess(bundle):
    result = {"mode": "read-only", "cancellation_supported": False, "complete": False,
              "limitation": "No atomic candidate/attempt precondition on GitHub cancellation; snapshots cannot authorize POST.",
              "decisions": []}
    try:
        require(bundle["repository"] == REPOSITORY, "repository not allowlisted")
        workflow = bundle["workflow"]
        require(workflow["path"] == WORKFLOW and positive_int(workflow["id"]), "workflow not allowlisted")
        runs = collect_runs(bundle["run_pages"])
        heads, unknown_head = collect_heads(bundle["queue_pages"])
        refs = collect_refs(bundle["refs"])
        refreshed = bundle["refreshed_runs"]
        require(isinstance(refreshed, list) and len(refreshed) <= MAX_RUNS, "invalid refreshed runs")
        current = {}
        for run in refreshed:
            require(isinstance(run, dict) and positive_int(run.get("id")), "invalid refreshed run")
            require(run["id"] not in current, "duplicate refreshed run")
            current[run["id"]] = run
        for run in runs:
            original_identity = identity(run)
            decision = {key: value for key, value in original_identity.items() if key != "head_branch"}
            decision.update(head_ref="refs/heads/" + str(run.get("head_branch", "")), action="skip")
            fresh = current.get(run["id"])
            if not in_scope(run, workflow["id"]):
                reason = "out-of-scope"
            elif fresh is None:
                reason = "unknown-run"
            elif not all(positive_int(r.get("run_attempt")) and valid_sha(r.get("head_sha")) for r in (run, fresh)):
                reason = "unknown-run"
            elif any(original_identity[key] != identity(fresh)[key] for key in IDENTITY if key != "run_attempt"):
                reason = "identity-changed"
            elif fresh["run_attempt"] > run["run_attempt"]:
                reason = "historical-attempt"
            elif fresh["run_attempt"] != run["run_attempt"]:
                reason = "identity-changed"
            elif fresh.get("status") == "completed" and isinstance(fresh.get("conclusion"), str):
                reason = "completed"
            elif fresh.get("status") not in LIVE or fresh.get("conclusion") is not None:
                reason = "unknown-run"
            elif run["head_sha"] in heads:
                reason = "current-candidate"
            elif run["head_sha"] in refs.values():
                reason = "ref-protects-sha"
            elif unknown_head:
                reason = "unknown-candidate-activity"
            elif decision["head_ref"] in refs:
                reason = "candidate-ref-replaced"
            else:
                reason = "candidate-not-observed"
            decision["reason"] = reason
            result["decisions"].append(decision)
        result["complete"] = True
    except (KeyError, TypeError, ValueError, AttributeError) as error:
        result["decisions"] = []
        result["error"] = f"Incomplete or invalid evidence: {error}"
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run", action="store_true", help="Explicitly select the only supported (read-only) mode")
    parser.parse_args()
    try:
        result = assess(json.load(sys.stdin))
    except (ValueError, OSError) as error:
        print(json.dumps({"mode": "read-only", "cancellation_supported": False,
                          "complete": False, "decisions": [], "error": str(error)}))
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["complete"] else 2


if __name__ == "__main__":
    sys.exit(main())
