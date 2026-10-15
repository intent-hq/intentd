#!/usr/bin/env python3
"""Offline assembly of the separately pinned private-review runtime.

Does not install OS packages, enable a provider, or activate private support.
The ordinary entry remains default-off. Containment binaries and licenses are
bundled; Linux x64/glibc >=2.38 and usable user/PID namespaces remain host needs.
Only the adjacent pinned descriptor selects executable bytes. Input archives
are authenticated before extraction; their maintainer scripts are never run.
"""

import argparse
import hashlib
import importlib.util
from pathlib import Path
import subprocess
import sys
import tempfile


HERE = Path(__file__).resolve().parent
HELPER_SHA256 = "91ce43bc922c89142431351b87b5340f330ee1e8e000615e9c715d5ae2deae3f"
DESCRIPTOR_SHA256 = "4ef6f96a284fe05da344364eae60a9c8b686b856f3a9c3478ccdb6b285389d02"
helper = HERE / "prepare-claude-callback-runtime.py"
if hashlib.sha256(helper.read_bytes()).hexdigest() != HELPER_SHA256:
    raise SystemExit("review runtime refused: installer implementation mismatch")
spec = importlib.util.spec_from_file_location("review_runtime_archive", helper)
archive = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = archive
spec.loader.exec_module(archive)

CONTAINMENT_LAUNCHER = (
    b'#!/bin/sh\nset -eu\nruntime_bin=${0%/*}\n'
    b'LD_LIBRARY_PATH="$runtime_bin/../containment/lib"\n'
    b'export LD_LIBRARY_PATH\n'
    b'exec "$runtime_bin/../containment/bin/bwrap" "$@"\n'
)


def configuration():
    path = HERE / "claude-review-runtime.json"
    archive.require(archive.digest_file(path) == DESCRIPTOR_SHA256,
                    "untrusted review descriptor")
    data = archive.read_json(path.read_bytes())
    archive.require(data["format"] == "intent-claude-review-runtime-descriptor-v1",
                    "unsupported review descriptor")
    contract = archive.Contract(data["inventory"], data["artifact"], data["inputs"])
    archive.require(contract.inventory["format"] == "intent-claude-review-runtime-inventory-v1",
                    "unsupported review inventory")
    archive.validate_inventory(contract.files)
    archive.require(contract.bundle["identity"] == contract.identity
                    and contract.bundle["manifest_sha256"] == archive.sha(contract.manifest),
                    "review identity mismatch")
    return contract


def platform_check():
    archive.platform_check()
    import platform
    version = tuple(int(part) for part in platform.libc_ver()[1].split(".")[:2])
    archive.require(version >= (2, 38), "review runtime requires glibc >=2.38")


def stage_payload(contract, fixture, compiled, node, license_path, packages, target):
    base = archive.configuration()
    archive.require(base.identity == contract.inputs["base_identity"], "base identity mismatch")
    archive.require(archive.digest_file(archive.FIXTURE_MANIFEST)
                    == base.inputs["fixture_manifest_sha256"], "fixture trust root mismatch")
    original = archive.read_json(archive.FIXTURE_MANIFEST.read_bytes())
    archive.verify_tree(fixture, {**original["files"],
        "FIXTURE-MANIFEST.json": archive.file_entry(archive.FIXTURE_MANIFEST.read_bytes())})
    archive.verify_tree(compiled / "dist", contract.inputs["compiled_files"])
    archive.require(archive.digest_file(compiled / "package-lock.json")
                    == contract.inputs["build_lock_sha256"], "build lock mismatch")
    archive.require(archive.digest_file(HERE / "claude-review-runtime.patch")
                    == contract.inputs["patch_sha256"], "review patch mismatch")
    archive.check_file(node, base.inputs["node"], mode=False)
    archive.check_file(license_path, base.inputs["node_license"], mode=False)
    target.mkdir(mode=0o755)
    target.chmod(0o755)
    for name in sorted(archive.directories(contract.files), key=lambda value: (value.count("/"), value)):
        (target / name).mkdir(mode=0o755)
        (target / name).chmod(0o755)
    with tempfile.TemporaryDirectory(prefix=".review-packages-", dir=target.parent) as temporary:
        extracted = {}
        for package in contract.inputs["containment_packages"]:
            source = packages / package["archive"]
            archive.check_file(source, package["file"], mode=False)
            destination = Path(temporary) / package["package"]
            subprocess.run(["dpkg-deb", "--extract", str(source), str(destination)], check=True,
                           stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
            extracted[package["package"]] = destination
        for name, entry in sorted(contract.inventory["files"].items()):
            dest = target / name
            if entry["mode"] == "120000":
                dest.symlink_to(entry["target"])
                continue
            if name == "bin/claude-agent-acp":
                archive.write_file(dest, archive.LAUNCHER, 0o755)
                continue
            if name == "bin/review-containment":
                archive.write_file(dest, CONTAINMENT_LAUNCHER, 0o755)
                continue
            if name in contract.inputs["containment_files"]:
                component = contract.inputs["containment_files"][name]
                source = extracted[component["package"]] / component["path"]
            elif name.startswith("runtime/dist/"):
                source = compiled / name.removeprefix("runtime/")
            elif name.startswith("runtime/"):
                source = fixture / name.removeprefix("runtime/")
            elif name == "node/bin/node":
                source = node
            elif name == "node/LICENSE":
                source = license_path
            else:
                raise archive.InvalidRuntime("unknown review payload entry")
            archive.copy_file(source, dest, entry)
    archive.write_file(target / "manifest.json", contract.manifest)
    archive.verify_tree(target, contract.files)


def assemble(contract, args):
    output = archive.absolute_directory(args.output)
    archive.require(not output.exists() and not output.is_symlink(), "destination exists")
    archive.require(output.parent.is_dir(), "assembly parent must exist")
    with tempfile.TemporaryDirectory(prefix=".review-assemble-", dir=output.parent) as temporary:
        stage = Path(temporary) / "result"
        stage.mkdir(mode=0o755)
        payload = stage / "payload"
        stage_payload(contract, args.fixture, args.compiled, args.node, args.node_license,
                      args.packages, payload)
        bundle = stage / "claude-review-runtime.tar.gz"
        archive.archive_payload(payload, contract.files, bundle)
        archive.check_bundle(contract, bundle)
        archive.write_file(stage / "manifest.json", contract.manifest)
        archive.rename_absent(stage, output)
    return output / "claude-review-runtime.tar.gz"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    assembly = commands.add_parser("assemble")
    for flag in ("fixture", "compiled", "node", "node-license", "packages", "output"):
        assembly.add_argument("--" + flag, type=Path, required=True)
    installation = commands.add_parser("install")
    installation.add_argument("--bundle", type=Path, required=True)
    installation.add_argument("--root", type=Path, required=True)
    verification = commands.add_parser("verify")
    verification.add_argument("--root", type=Path, required=True)
    removal = commands.add_parser("remove")
    removal.add_argument("--root", type=Path, required=True)
    removal.add_argument("--identity", required=True)
    removal.add_argument("--confirm-deselected-and-drained", action="store_true")
    args = parser.parse_args()
    platform_check()
    contract = configuration()
    if args.command == "assemble":
        result = assemble(contract, args)
    elif args.command == "install":
        result = archive.install(contract, args.bundle, args.root)
    elif args.command == "verify":
        with archive.owned_root(args.root) as root:
            archive.verify_install(contract, root / contract.identity)
        result = args.root / contract.identity / "bin/claude-agent-acp"
    else:
        archive.remove(contract, args.root, args.identity, args.confirm_deselected_and_drained)
        result = args.identity
    print(result)


if __name__ == "__main__":
    try:
        main()
    except (archive.InvalidRuntime, OSError, ValueError, KeyError, TypeError,
            archive.tarfile.TarError, subprocess.CalledProcessError, KeyboardInterrupt) as error:
        print(f"review runtime preparation refused: {error}", file=sys.stderr)
        sys.exit(1)
