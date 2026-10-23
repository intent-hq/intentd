#!/usr/bin/env python3
"""Check the provider error corpus against the pinned ACP adapter packages.

crates/intent-acp/tests/fixtures/provider-error-corpus.json lists every
structured error kind the pinned claude-agent-acp and codex-acp adapters can
attach to a failed prompt, and what the quota classifier should do with each
(intent-hq/intent#7615). This script downloads the exact pinned packages from
npm (versions from crates/intent-providers/data/adapter-cli-packages.json),
reads the kinds out of their code, and fails when:

- an adapter emits a kind the corpus does not list (a new limit wording would
  otherwise be missed silently by backup failover and the quota banner);
- the corpus lists a kind the adapter no longer emits;
- the adapter's own category for a kind (quota_exhausted / rate_limited versus
  anything else) disagrees with the corpus `expect`;
- a Claude Agent SDK USAGE_LIMIT_ERROR_PREFIXES entry has no corpus rendering.

CI runs it on every PR, so an adapter pin bump that adds an error kind fails
before it merges. To fix a failure, add the kind (with a real rendering) to the
corpus and run `make test-select E='package(intent-acp)'`.

Run from the intentd root: python3 -S scripts/check-provider-error-corpus.py
"""

import base64
import hashlib
import io
import json
from pathlib import Path
import re
import sys
import tarfile
import time
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
CORPUS = "crates/intent-acp/tests/fixtures/provider-error-corpus.json"
METADATA = "crates/intent-providers/data/adapter-cli-packages.json"
REGISTRY = "https://registry.npmjs.org/"
# The adapters' own failure categories that mean "the account's allowance".
QUOTA_CATEGORIES = {"quota_exhausted", "rate_limited"}
# Bare SDK prefixes that only mean a usage limit on the SDK's synthetic
# message; the classifier deliberately ignores them, and the corpus covers them
# through the full limit wordings (e.g. "You've hit your weekly limit").
OPEN_PREFIXES = {"You've hit your", "You've reached your"}
RENDERED_PREFIX = "Internal error: "


class Refusal(Exception):
    """The packages, the metadata, or the corpus cannot be read as expected."""


# --- Reading package source ------------------------------------------------

def js_strings(text):
    """The double- or single-quoted JS string literals in `text`, decoded."""
    out = []
    for match in re.finditer(r'"((?:[^"\\]|\\.)*)"|\'((?:[^\'\\]|\\.)*)\'', text):
        raw = match.group(1) if match.group(1) is not None else match.group(2)
        raw = re.sub(r"\\x([0-9a-fA-F]{2})", r"\\u00\1", raw).replace("\\'", "'")
        out.append(json.loads(f'"{raw}"'))
    return out


def claude_kinds(adapter_sources, sdk_types):
    """{kind: adapter category} for every errorKind claude-agent-acp can send.

    `adapter_sources` maps dist file names to their text; `sdk_types` is the
    Claude Agent SDK's sdk.d.ts.
    """
    union = re.search(r"type SDKAssistantMessageError\s*=\s*([^;]+);", sdk_types)
    if not union:
        raise Refusal("sdk.d.ts no longer declares SDKAssistantMessageError")
    sdk_kinds = js_strings(union.group(1))
    if not sdk_kinds:
        raise Refusal("SDKAssistantMessageError declares no kinds")

    categories = {}
    found_mapping = False
    for text in adapter_sources.values():
        body = re.search(
            r"function providerFailureCategory\([^)]*\)\s*\{(.*?)\n\}", text, re.DOTALL)
        if not body:
            continue
        found_mapping = True
        pending = []
        for token in re.finditer(r'case "([A-Za-z_]+)":|return "([A-Za-z_]+)";', body.group(1)):
            if token.group(1):
                pending.append(token.group(1))
            else:
                for kind in pending:
                    categories[kind] = token.group(2)
                pending = []
    if not found_mapping:
        raise Refusal("claude-agent-acp no longer defines providerFailureCategory")

    kinds = {kind: categories.get(kind, "provider_error") for kind in sdk_kinds}
    # Kinds the adapter attaches itself (errorKind: "x" / errorKindData("x")).
    for text in adapter_sources.values():
        for match in re.finditer(r'errorKind:\s*"([a-z_]+)"|errorKindData\("([a-z_]+)"\)', text):
            kinds.setdefault(match.group(1) or match.group(2), "adapter")
    return kinds


def claude_usage_limit_prefixes(sdk_module):
    """The SDK's exported USAGE_LIMIT_ERROR_PREFIXES, from its bundled sdk.mjs."""
    alias = re.search(r"\b([A-Za-z_$][\w$]*) as USAGE_LIMIT_ERROR_PREFIXES\b", sdk_module)
    name = alias.group(1) if alias else "USAGE_LIMIT_ERROR_PREFIXES"
    array = re.search(rf"(?<![\w$.]){re.escape(name)}\s*=\s*\[([^\]]*)\]", sdk_module)
    if not array:
        raise Refusal("the Claude Agent SDK no longer exports USAGE_LIMIT_ERROR_PREFIXES")
    prefixes = js_strings(array.group(1))
    if not prefixes:
        raise Refusal("USAGE_LIMIT_ERROR_PREFIXES is empty")
    return prefixes


def codex_kinds(index_js):
    """({string kind: category}, {structured kind: category}) for codex-acp."""
    tables = []
    for name in ("STRING_CODEX_ERROR_CATEGORIES", "STRUCTURED_CODEX_ERROR_CATEGORIES"):
        table = re.search(rf"\b{name}\s*=\s*\{{([^}}]*)\}}", index_js)
        if not table:
            raise Refusal(f"codex-acp no longer defines {name}")
        pairs = dict(re.findall(r'"?([A-Za-z_]+)"?\s*:\s*"([A-Za-z_]+)"', table.group(1)))
        if not pairs:
            raise Refusal(f"{name} is empty")
        tables.append(pairs)
    return tables[0], tables[1]


# --- Comparing with the corpus ---------------------------------------------

def compare_kinds(adapter, table_name, corpus_values, emitted):
    problems = []
    for kind in sorted(set(emitted) - set(corpus_values)):
        problems.append(
            f"{adapter}: the pinned adapter emits {table_name} `{kind}` (its category: "
            f"{emitted[kind]}) but the corpus does not classify it. Add it to {CORPUS} with "
            "the expect intentd needs, plus a real rendering if it is a limit.")
    for kind in sorted(set(corpus_values) - set(emitted)):
        problems.append(
            f"{adapter}: the corpus lists {table_name} `{kind}`, which the pinned adapter "
            "no longer emits. Remove it from the corpus.")
    for kind in sorted(set(emitted) & set(corpus_values)):
        entry = corpus_values[kind]
        adapter_says_quota = emitted[kind] in QUOTA_CATEGORIES
        corpus_says_quota = entry.get("expect") == "quota"
        if adapter_says_quota != corpus_says_quota and not entry.get("divergence"):
            problems.append(
                f"{adapter}: the pinned adapter files {table_name} `{kind}` under "
                f"{emitted[kind]}, but the corpus expects {entry.get('expect')}. Update "
                "`expect`, or add a `divergence` note explaining why intentd differs.")
    return problems


def compare_prefixes(corpus, prefixes):
    details = [
        r["message"][len(RENDERED_PREFIX):]
        for r in corpus["renderings"]
        if r.get("adapter") == "claude-code"
        and isinstance(r.get("message"), str) and r["message"].startswith(RENDERED_PREFIX)
    ]
    return [
        f"claude-code: the Claude Agent SDK treats messages starting {json.dumps(prefix)} as "
        f"usage limits, but no corpus rendering starts with it. Add the real wording to {CORPUS}."
        for prefix in prefixes
        if prefix not in OPEN_PREFIXES and not any(d.startswith(prefix) for d in details)
    ]


def check(corpus, claude_adapter_sources, claude_sdk_types, claude_sdk_module, codex_index):
    kinds = corpus["kinds"]
    problems = compare_kinds(
        "claude-code", "errorKind", kinds["claude-code"]["values"],
        claude_kinds(claude_adapter_sources, claude_sdk_types))
    problems += compare_prefixes(corpus, claude_usage_limit_prefixes(claude_sdk_module))
    strings, structured = codex_kinds(codex_index)
    problems += compare_kinds("codex", "codexErrorInfo", kinds["codex"]["values"], strings)
    problems += compare_kinds(
        "codex", "structured codexErrorInfo", kinds["codex"]["structuredValues"], structured)
    return problems


# --- Fetching the pinned packages -------------------------------------------

def fetch(url):
    last = None
    for attempt in range(3):
        try:
            with urllib.request.urlopen(url, timeout=60) as response:
                return response.read()
        except OSError as error:
            last = error
            time.sleep(2 * (attempt + 1))
    raise Refusal(f"could not download {url}: {last}")


def package_files(name, version, wanted):
    """{path inside the package: text} for members `wanted(path)` selects.

    Checks the tarball against the registry's sha512 integrity and reads
    members in memory; nothing is extracted to disk.
    """
    document = json.loads(fetch(REGISTRY + urllib.parse.quote(name, safe="@") + "/" + version))
    dist = document.get("dist", {})
    integrity = dist.get("integrity", "")
    if document.get("name") != name or document.get("version") != version \
            or not integrity.startswith("sha512-"):
        raise Refusal(f"registry metadata for {name}@{version} is not as expected")
    tarball = fetch(dist["tarball"])
    digest = base64.b64encode(hashlib.sha512(tarball).digest()).decode()
    if digest != integrity[len("sha512-"):]:
        raise Refusal(f"{name}@{version} tarball does not match its registry integrity")
    files = {}
    with tarfile.open(fileobj=io.BytesIO(tarball), mode="r:gz") as archive:
        for member in archive.getmembers():
            path = member.name.split("/", 1)[-1]
            if member.isfile() and wanted(path):
                files[path] = archive.extractfile(member).read().decode("utf-8")
    return files


def pinned_versions(root):
    metadata = json.loads((root / METADATA).read_text())
    claude, codex = metadata["claude-code"], metadata["codex"]
    return {
        "claude-adapter": (claude["package"]["name"], claude["package"]["version"]),
        "claude-sdk": (claude["runtime"]["name"], claude["runtime"]["version"]),
        "codex-adapter": (codex["package"]["name"], codex["package"]["version"]),
    }


def main():
    corpus = json.loads((ROOT / CORPUS).read_text())
    pins = pinned_versions(ROOT)
    adapter = package_files(*pins["claude-adapter"],
                            lambda p: p.startswith("dist/") and p.endswith(".js"))
    sdk = package_files(*pins["claude-sdk"], lambda p: p in ("sdk.d.ts", "sdk.mjs"))
    codex = package_files(*pins["codex-adapter"], lambda p: p == "dist/index.js")
    for label, files, needed in (("claude-agent-sdk", sdk, ("sdk.d.ts", "sdk.mjs")),
                                 ("codex-acp", codex, ("dist/index.js",))):
        missing = [path for path in needed if path not in files]
        if missing:
            raise Refusal(f"{label} no longer ships {', '.join(missing)}")
    problems = check(corpus, adapter, sdk["sdk.d.ts"], sdk["sdk.mjs"], codex["dist/index.js"])
    checked = ", ".join(f"{name}@{version}" for name, version in pins.values())
    if problems:
        for problem in problems:
            print(f"::error::{problem}", file=sys.stderr)
        print(f"Provider error corpus is out of date for {checked}.", file=sys.stderr)
        return 1
    print(f"Provider error corpus covers every error kind in {checked}.")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (Refusal, KeyError, TypeError, ValueError) as error:
        print(f"::error::{error}", file=sys.stderr)
        sys.exit(1)
