#!/usr/bin/env python3
"""Refresh offline CLI-requirement evidence from exact pinned npm manifests.

Run python3 -S scripts/provider_cli_metadata.py after changing adapter pins.
Runtime code never contacts npm. Tests require snapshot versions to match pins.
"""
import json
from pathlib import Path
import re
import urllib.parse
import urllib.request

METADATA = "crates/intent-providers/data/adapter-cli-packages.json"
CONFIG = "crates/intent-providers/src/config.rs"
SECTIONS = ("dependencies", "optionalDependencies", "peerDependencies")


def package_snapshot(manifest):
    if not isinstance(manifest, dict) or not all(isinstance(manifest.get(k), str) for k in ("name", "version")):
        raise ValueError("package metadata must identify an exact name and version")
    result = {k: manifest[k] for k in ("name", "version")}
    for section in SECTIONS:
        values = manifest.get(section, {})
        if not isinstance(values, dict) or any(not isinstance(v, str) for v in values.values()):
            raise ValueError("invalid package dependency declarations")
        result[section] = values
    return result


def replace_package(content, provider, manifest):
    data = json.loads(content)
    data[provider]["package"] = package_snapshot(manifest)
    return json.dumps(data, indent=2, sort_keys=True) + "\n"


def read_pin(config, symbol):
    match = re.search(r'pub const ' + re.escape(symbol) + r': &str = "([^"\n]+)";', config)
    if match:
        return match[1]
    if symbol == "CLAUDE_AGENT_ACP_NPX_PACKAGE":
        version = re.search(r'macro_rules! claude_agent_acp_version.*?"([^"\n]+)"', config, re.S)
        if version:
            return "@agentclientprotocol/claude-agent-acp@" + version[1]
    raise ValueError("cannot read adapter pin " + symbol)


def refresh(root):
    path = root / METADATA
    content = path.read_text()
    config = (root / CONFIG).read_text()
    for provider, mapping in json.loads(content).items():
        pin = read_pin(config, mapping["pinConstant"])
        name, version = pin.rsplit("@", 1)
        url = "https://registry.npmjs.org/" + urllib.parse.quote(name, safe="") + "/" + urllib.parse.quote(version, safe="")
        with urllib.request.urlopen(url, timeout=30) as response:
            manifest = json.load(response)
        if manifest.get("name") != name or manifest.get("version") != version:
            raise ValueError("registry returned a different package than the exact pin")
        content = replace_package(content, provider, manifest)
    path.write_text(content)


if __name__ == "__main__":
    refresh(Path(__file__).resolve().parent.parent)
