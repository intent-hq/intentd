#!/usr/bin/env python3
"""Install exactly the pinned claude-agent-acp adapter for the contract test.

The version comes from the claude_agent_acp_version! macro in
crates/intent-providers/src/config.rs, the expected Claude Agent SDK version
from crates/intent-providers/data/adapter-cli-packages.json. Unlike the frozen
callback fixture (prepare-acp-callback-fixture.py), this install follows the
pin, so every pin bump (including the rolling auto/claude-agent-acp-pin PRs)
is exercised by the claude_adapter_contract test in intent-services.

npm runs with lifecycle scripts disabled, optional dependencies omitted (the
test supplies its own stand-in Claude CLI) and a private HOME, npmrc and cache.
Only the verified adapter package directory is written to stdout.
"""

import argparse
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import time


ROOT = Path(__file__).resolve().parents[1]
ADAPTER = "@agentclientprotocol/claude-agent-acp"
SDK = "@anthropic-ai/claude-agent-sdk"
SEMVER = re.compile(r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)")


def _metadata_module():
    # Loaded by path so `python3 -I` (no script dir on sys.path) still works.
    spec = importlib.util.spec_from_file_location(
        "provider_cli_metadata", Path(__file__).with_name("provider_cli_metadata.py")
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


META = _metadata_module()


class Refusal(Exception):
    """The pin, the metadata, or the installed tree disagree."""


def exact(value, what):
    if not isinstance(value, str) or not SEMVER.fullmatch(value):
        raise Refusal(f"{what} must be an exact stable version, got {value!r}")
    return value


def expected_versions(root):
    """(adapter version, SDK version) the repository pins."""
    config = (root / META.CONFIG).read_text()
    spec = META.read_pin(config, "CLAUDE_AGENT_ACP_NPX_PACKAGE")
    name, version = spec.rsplit("@", 1)
    if name != ADAPTER:
        raise Refusal(f"unexpected claude-code adapter package {name!r}")
    metadata = json.loads((root / META.METADATA).read_text())["claude-code"]["package"]
    if metadata.get("version") != version:
        raise Refusal(
            f"{META.METADATA} describes {ADAPTER}@{metadata.get('version')} but the pin is {version}; "
            "run scripts/provider_cli_metadata.py"
        )
    sdk = metadata.get("dependencies", {}).get(SDK)
    return exact(version, "adapter pin"), exact(sdk, f"{SDK} dependency in {META.METADATA}")


def installed_version(prefix, package):
    path = prefix / "node_modules" / package / "package.json"
    if not path.is_file():
        raise Refusal(f"{package} is missing from the install at {prefix}")
    return json.loads(path.read_text()).get("version")


def validate(prefix, adapter_version, sdk_version):
    """Return the adapter package directory when the tree matches the pin."""
    got = installed_version(prefix, ADAPTER)
    if got != adapter_version:
        raise Refusal(f"installed {ADAPTER}@{got}, but the pin is {adapter_version}")
    got = installed_version(prefix, SDK)
    if got != sdk_version:
        raise Refusal(f"installed {SDK}@{got}, but {META.METADATA} expects {sdk_version}")
    package = prefix / "node_modules" / ADAPTER
    if not (package / "dist" / "index.js").is_file():
        raise Refusal(f"{ADAPTER}@{adapter_version} has no dist/index.js entry point")
    return package.resolve()


def npm_environment(work):
    private = work / "private"
    private.mkdir(mode=0o700, exist_ok=True)
    npmrc = private / "user.npmrc"
    npmrc.write_text("")
    global_npmrc = private / "global.npmrc"
    global_npmrc.write_text("")
    return {
        "PATH": os.environ.get("PATH", os.defpath),
        "HOME": str(private),
        "TMPDIR": str(private),
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "NODE_OPTIONS": "",
        "NPM_CONFIG_USERCONFIG": str(npmrc),
        "NPM_CONFIG_GLOBALCONFIG": str(global_npmrc),
        "NPM_CONFIG_CACHE": str(private / "npm-cache"),
        "NPM_CONFIG_REGISTRY": "https://registry.npmjs.org/",
        "NPM_CONFIG_IGNORE_SCRIPTS": "true",
        "NPM_CONFIG_UPDATE_NOTIFIER": "false",
        "NPM_CONFIG_AUDIT": "false",
        "NPM_CONFIG_FUND": "false",
        "DD_TRACE_ENABLED": "false",
        "DD_INSTRUMENTATION_TELEMETRY_ENABLED": "false",
    }


NPM_ATTEMPTS = 3


def run_npm(prefix, spec, env, attempts=NPM_ATTEMPTS, pause=time.sleep):
    """Install `spec`, retrying transient registry/network failures.

    A version mismatch is still caught by validate() afterwards, so a retry
    can never accept a different package.
    """
    npm = shutil.which("npm", path=env["PATH"])
    if npm is None:
        raise Refusal("npm is required (install Node.js 22+)")
    args = [npm, "install", "--prefix", str(prefix), "--ignore-scripts", "--omit=optional",
            "--no-audit", "--no-fund", "--no-save", "--no-package-lock",
            "--fetch-retries=4", spec]
    for attempt in range(1, attempts + 1):
        print(f"+ (attempt {attempt}/{attempts}) " + " ".join(args), file=sys.stderr, flush=True)
        result = subprocess.run(args, cwd=prefix, env=env, stdout=sys.stderr, stderr=sys.stderr,
                                timeout=300, check=False)
        if result.returncode == 0:
            return
        if attempt < attempts:
            pause(15 * attempt)
    raise Refusal(f"npm install {spec} failed ({result.returncode}) after {attempts} attempts")


def prepare(root, cache):
    adapter_version, sdk_version = expected_versions(root)
    if cache.is_symlink():
        raise Refusal("cache directory must not be a symlink")
    cache.mkdir(parents=True, exist_ok=True, mode=0o700)
    ready = cache / f"claude-agent-acp-{adapter_version}-sdk-{sdk_version}"
    if ready.is_dir() and not ready.is_symlink():
        try:
            return validate(ready, adapter_version, sdk_version)
        except Refusal as error:
            print(f"discarding stale install: {error}", file=sys.stderr)
            shutil.rmtree(ready)
    with tempfile.TemporaryDirectory(prefix=".install-", dir=cache) as temporary:
        work = Path(temporary)
        prefix = work / "prefix"
        prefix.mkdir()
        run_npm(prefix, f"{ADAPTER}@{adapter_version}", npm_environment(work))
        validate(prefix, adapter_version, sdk_version)
        try:
            prefix.rename(ready)
        except OSError:
            # A concurrent run published first; its install is verified below.
            if not ready.is_dir():
                raise
    return validate(ready, adapter_version, sdk_version)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cache-dir", type=Path, required=True)
    parser.add_argument("--root", type=Path, default=ROOT, help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    try:
        print(prepare(args.root, args.cache_dir.absolute()), flush=True)
    except (Refusal, OSError, ValueError, KeyError, subprocess.TimeoutExpired) as error:
        print(f"claude adapter pin: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
