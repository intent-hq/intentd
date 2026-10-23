#!/usr/bin/env python3
"""Refresh offline CLI-requirement evidence from exact pinned npm manifests.

Run python3 -S scripts/provider_cli_metadata.py after changing adapter pins.
Runtime code never contacts npm. Tests require snapshot versions to match pins.

A mapping with `runtimePackage` also snapshots that exact adapter dependency's
`runtimeVersionField` (e.g. the Claude Agent SDK's `claudeCodeVersion`): the
CLI version the adapter's runtime was built against, which the daemon uses as
the bundled-CLI pin and to derive the installed-CLI minimum.
"""
import json
from pathlib import Path
import re
import urllib.parse
import urllib.request

METADATA = "crates/intent-providers/data/adapter-cli-packages.json"
CONFIG = "crates/intent-providers/src/config.rs"
SECTIONS = ("dependencies", "optionalDependencies", "peerDependencies")
EXACT = re.compile(r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)")
REGISTRY = "https://registry.npmjs.org/"


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


def runtime_spec(content, provider, manifest):
    """(name, exact version) of the runtime dependency to snapshot, or None."""
    mapping = json.loads(content)[provider]
    name = mapping.get("runtimePackage")
    if name is None:
        return None
    package = package_snapshot(manifest)
    declared = package["optionalDependencies"].get(name, package["dependencies"].get(name))
    if not isinstance(declared, str) or not EXACT.fullmatch(declared):
        raise ValueError("runtime dependency " + name + " must be an exact stable version")
    return name, declared


def runtime_snapshot(mapping, runtime):
    field = mapping.get("runtimeVersionField")
    if not isinstance(runtime, dict) or not isinstance(field, str):
        raise ValueError("runtime metadata must identify an exact package and version field")
    value = runtime.get(field)
    if not isinstance(value, str) or not EXACT.fullmatch(value):
        raise ValueError("runtime " + field + " must be an exact stable version")
    return {"name": runtime.get("name"), "version": runtime.get("version"), field: value}


def replace_package(content, provider, manifest, runtime=None):
    """Snapshot the adapter manifest (and its runtime manifest when mapped)."""
    spec = runtime_spec(content, provider, manifest)
    data = json.loads(content)
    data[provider]["package"] = package_snapshot(manifest)
    if spec is not None:
        if not isinstance(runtime, dict) or (runtime.get("name"), runtime.get("version")) != spec:
            raise ValueError("runtime metadata must describe " + "@".join(spec))
        data[provider]["runtime"] = runtime_snapshot(data[provider], runtime)
    elif runtime is not None:
        raise ValueError("adapter mapping declares no runtimePackage")
    return json.dumps(data, indent=2, sort_keys=True) + "\n"


def registry_url(name, version):
    return REGISTRY + urllib.parse.quote(name, safe="") + "/" + urllib.parse.quote(version, safe="")


def fetch_manifest(name, version):
    with urllib.request.urlopen(registry_url(name, version), timeout=30) as response:
        manifest = json.load(response)
    if manifest.get("name") != name or manifest.get("version") != version:
        raise ValueError("registry returned a different package than the exact pin")
    return manifest


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
        manifest = fetch_manifest(name, version)
        spec = runtime_spec(content, provider, manifest)
        runtime = fetch_manifest(*spec) if spec else None
        content = replace_package(content, provider, manifest, runtime)
    path.write_text(content)


if __name__ == "__main__":
    refresh(Path(__file__).resolve().parent.parent)
