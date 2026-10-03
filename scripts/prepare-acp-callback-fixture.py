#!/usr/bin/env python3
"""Prepare the pinned Linux x64 test fixture, never a native agent installation.

Explicit bundle, offline cache, and opt-in source build share the same inventory
validator. Only the verified fixture directory is written to stdout. Diagnostics
and build receipts go to stderr. No package lifecycle scripts are enabled.
"""

import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import platform
import shutil
import signal
import stat
import subprocess
import sys
import tarfile
import tempfile
import time
import urllib.request


DEFAULT_DESCRIPTOR = (
    Path(__file__).resolve().parents[1]
    / "crates/intent-acp/tests/fixtures/claude-callback-adapter.json"
)


class InvalidFixture(Exception):
    """An input or completed preparation did not match its immutable contract."""


def require(condition, message):
    if not condition:
        raise InvalidFixture(message)


def digest(path):
    result = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(block)
    return result.hexdigest()


def receipt(event, **fields):
    print(json.dumps({"event": event, **fields}, sort_keys=True), file=sys.stderr, flush=True)


def read_json(path):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            require(key not in result, f"duplicate JSON key: {key}")
            result[key] = value
        return result

    return json.loads(path.read_text(), object_pairs_hook=unique)


def check_file(path, expected):
    require(path.is_file() and not path.is_symlink(), f"missing or linked file: {path}")
    if "bytes" in expected:
        require(path.stat().st_size == expected["bytes"], f"size mismatch: {path}")
    require(digest(path) == expected["sha256"], f"SHA256 mismatch: {path}")


def safe_name(name):
    path = PurePosixPath(name)
    require(
        bool(name) and not path.is_absolute() and "\\" not in name and "\0" not in name
        and all(part not in ("", ".", "..") for part in name.split("/")),
        f"unsafe path: {name!r}",
    )
    return path


def directories(entries):
    result = set()
    for name in entries:
        path = safe_name(name)
        result.update(str(parent) for parent in path.parents if str(parent) != ".")
    require(not result.intersection(entries), "file/directory collision in inventory")
    return result


def verify_entry(root, relative, expected):
    path = root / relative
    mode = path.lstat().st_mode
    if expected["mode"] == "120000":
        require(stat.S_ISLNK(mode), f"expected symlink: {relative}")
        target = os.readlink(path)
        require(target == expected["target"], f"link mismatch: {relative}")
        require(not Path(target).is_absolute(), f"absolute link: {relative}")
        require(path.resolve(strict=True).is_relative_to(root.resolve()), f"escaping link: {relative}")
        raw = os.fsencode(target)
        require(len(raw) == expected["bytes"] and hashlib.sha256(raw).hexdigest() == expected["sha256"], f"link digest mismatch: {relative}")
    else:
        require(expected["mode"] in ("100644", "100755"), f"unsupported mode: {relative}")
        require(stat.S_ISREG(mode), f"not a regular file: {relative}")
        require(stat.S_IMODE(mode) == int(expected["mode"][-3:], 8), f"mode mismatch: {relative}")
        check_file(path, expected)
        if "git_blob" in expected:
            raw = path.read_bytes()
            blob = hashlib.sha1(f"blob {len(raw)}\0".encode() + raw).hexdigest()
            require(blob == expected["git_blob"], f"Git blob mismatch: {relative}")


def verify_files(root, entries):
    require(root.is_dir() and not root.is_symlink(), f"missing or linked root: {root}")
    allowed_dirs = directories(entries)
    found = set()
    for parent, dirs, files in os.walk(root, followlinks=False):
        for name in dirs[:]:
            path = Path(parent) / name
            if path.is_symlink():
                dirs.remove(name)
                files.append(name)
            else:
                require(str(path.relative_to(root)) in allowed_dirs, f"extra directory: {path}")
        for name in files:
            path = Path(parent) / name
            relative = str(path.relative_to(root))
            require(relative in entries, f"extra payload: {relative}")
            expected = entries[relative]
            found.add(relative)
            verify_entry(root, relative, expected)
    require(found == set(entries), f"missing payload: {sorted(set(entries) - found)[:5]}")


def tree_id(entries):
    """Compute a Git tree identity without creating or importing Git objects."""
    root = {}
    for name, expected in entries.items():
        cursor = root
        parts = safe_name(name).parts
        for part in parts[:-1]:
            cursor = cursor.setdefault(part, {})
        cursor[parts[-1]] = (expected["mode"], expected["git_blob"])

    def tree(items):
        raw = b""
        for name, value in sorted(items.items(), key=lambda item: os.fsencode(item[0] + ("/" if isinstance(item[1], dict) else ""))):
            mode, oid = ("40000", tree(value)) if isinstance(value, dict) else value
            raw += mode.encode() + b" " + os.fsencode(name) + b"\0" + bytes.fromhex(oid)
        return hashlib.sha1(f"tree {len(raw)}\0".encode() + raw).hexdigest()

    return tree(root)


def extract(archive_path, target, prefix, entries, normalize_group_write=False, archive_inventory_sha256=None):
    """Extract only an exact inventory; symlinks are created after regular files."""
    require(not target.exists(), f"extraction target exists: {target}")
    allowed_dirs = directories(entries)
    seen = set()
    links = []
    headers = []
    target.mkdir(mode=0o755)
    with tarfile.open(archive_path, "r:gz") as archive:
        for member in archive:
            headers.append({"name": member.name, "type": member.type.decode(),
                            "mode": member.mode, "size": member.size, "link": member.linkname})
            name = member.name.rstrip("/") if member.isdir() else member.name
            safe_name(name)
            require(name not in seen, f"duplicate archive path: {name}")
            seen.add(name)
            require(name == prefix or name.startswith(prefix + "/"), f"wrong archive root: {name}")
            relative = name[len(prefix):].lstrip("/")
            if member.isdir():
                require(not relative or relative in allowed_dirs, f"extra archive directory: {name}")
                require(member.mode & 0o7000 == 0, f"special directory mode: {name}")
                (target / relative).mkdir(parents=True, exist_ok=True, mode=0o755)
                continue
            require(relative in entries, f"extra archive payload: {name}")
            expected = entries[relative]
            path = target / relative
            path.parent.mkdir(parents=True, exist_ok=True, mode=0o755)
            if member.issym():
                require(expected["mode"] == "120000" and member.linkname == expected["target"], f"unexpected link: {name}")
                require(not Path(member.linkname).is_absolute(), f"absolute link: {name}")
                require((path.parent / member.linkname).resolve().is_relative_to(target.resolve()), f"escaping link: {name}")
                links.append((path, member.linkname))
            else:
                require(member.type in (tarfile.REGTYPE, tarfile.AREGTYPE), f"unsupported archive entry: {name}")
                require(expected["mode"] in ("100644", "100755"), f"expected link: {name}")
                mode = int(expected["mode"][-3:], 8)
                # Codeload and the pinned bundle carry group-write bits absent
                # from their Git-mode inventories. Published cache modes are exact.
                allowed_modes = (mode, mode | 0o020) if normalize_group_write else (mode,)
                require(member.mode in allowed_modes, f"archive mode mismatch: {name}")
                require(member.size == expected["bytes"], f"archive size mismatch: {name}")
                with archive.extractfile(member) as source, path.open("xb") as output:
                    shutil.copyfileobj(source, output, 1024 * 1024)
                path.chmod(mode)
    if archive_inventory_sha256 is not None:
        raw = (json.dumps(headers, sort_keys=True, separators=(",", ":")) + "\n").encode()
        require(hashlib.sha256(raw).hexdigest() == archive_inventory_sha256, "archive header inventory mismatch")
    for path, link in links:
        path.symlink_to(link)
    verify_files(target, entries)


def configuration(path):
    descriptor = read_json(path)
    require(descriptor["format"] == "intent-acp-callback-fixture-v1", "unknown descriptor format")
    for name in ("patch", "manifest"):
        data = descriptor[name]
        require(Path(data["file"]).name == data["file"], f"non-local {name} descriptor")
        check_file(path.parent / data["file"], data)
    manifest = read_json(path.parent / descriptor["manifest"]["file"])
    require(manifest["platform"] == descriptor["platform"] == "linux", "unsupported fixture platform")
    require(manifest["architecture"] == descriptor["architecture"] == "x64", "unsupported fixture architecture")
    require(manifest["source_tree"] == descriptor["canonical"]["tree"], "manifest source mismatch")
    require(manifest["pack_sha256"] == descriptor["pack"]["sha256"], "manifest pack mismatch")
    require(manifest["lock_sha256"] == descriptor["lock_sha256"], "manifest lock mismatch")
    require(len(manifest["files"]) == descriptor["expected_payload_entries"], "payload count mismatch")
    require(len(manifest["runtime_dependencies"]) == descriptor["expected_runtime_packages"], "runtime count mismatch")
    for name in ("source", "canonical"):
        require(tree_id(descriptor[name]["entries"]) == descriptor[name]["tree"], f"{name} tree descriptor mismatch")
    return descriptor, manifest


def platform_check():
    require(sys.platform == "linux" and platform.machine().lower() in ("x86_64", "amd64"), "fixture requires Linux x64")
    require(not os.environ.get("NODE_OPTIONS"), "NODE_OPTIONS injection is not supported")


def environment(work):
    private = work / "private"
    private.mkdir(mode=0o700, exist_ok=True)
    config = private / "empty.npmrc"
    config.write_text("")
    global_config = private / "global.npmrc"
    global_config.write_text("")
    return {
        "PATH": os.environ.get("PATH", os.defpath),
        "HOME": str(private), "TMPDIR": str(private),
        "LANG": "C.UTF-8", "LC_ALL": "C.UTF-8", "TZ": "UTC",
        "NODE_OPTIONS": "", "NODE_DISABLE_COMPILE_CACHE": "1",
        "NPM_CONFIG_USERCONFIG": str(config), "NPM_CONFIG_GLOBALCONFIG": str(global_config),
        "NPM_CONFIG_CACHE": str(private / "npm-cache"),
        "NPM_CONFIG_REGISTRY": "https://registry.npmjs.org/",
        "NPM_CONFIG_IGNORE_SCRIPTS": "true", "NPM_CONFIG_UPDATE_NOTIFIER": "false",
        "NPM_CONFIG_AUDIT": "false", "NPM_CONFIG_FUND": "false",
        "GIT_CEILING_DIRECTORIES": str(work), "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_CONFIG_GLOBAL": str(config),
    }


def command(args, cwd, env, capture=False):
    receipt("command-start", argv=[str(a) for a in args], cwd=str(cwd))
    started = time.monotonic()
    child = subprocess.Popen(args, cwd=cwd, env=env, start_new_session=True,
                             stdout=subprocess.PIPE if capture else sys.stderr, stderr=sys.stderr)
    try:
        output, _ = child.communicate(timeout=600)
    finally:
        if child.poll() is None:
            os.killpg(child.pid, signal.SIGTERM)
            try:
                child.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(child.pid, signal.SIGKILL)
                child.wait()
    receipt("command-end", argv=[str(a) for a in args], exit_code=child.returncode,
            seconds=round(time.monotonic() - started, 3))
    require(child.returncode == 0, f"command failed ({child.returncode}): {args[0]}")
    return (output or b"").decode().strip()


def node_tool(descriptor, work, env):
    located = shutil.which("node", path=env["PATH"])
    require(located is not None, "install the descriptor's exact Node version first")
    node = Path(located).resolve()
    version = command([node, "--version"], work, env, capture=True)
    require(version == descriptor["tools"]["node"], f"Node version mismatch: {version}")
    receipt("node-tool", path=str(node), version=version, sha256=digest(node))
    return node


def npm_tool(descriptor, node, work, env):
    located = shutil.which("npm", path=env["PATH"])
    require(located is not None, "npm is required for the explicit source route")
    npm = Path(located).resolve()
    version = command([node, npm, "--version"], work, env, capture=True)
    if version != descriptor["tools"]["npm"]:
        prefix = work / "npm-tool"
        command([node, npm, "install", "--prefix", prefix, "--ignore-scripts", "--no-audit",
                 "--no-fund", "--no-package-lock", "npm@" + descriptor["tools"]["npm"]], work, env)
        npm = prefix / "node_modules/npm/bin/npm-cli.js"
        version = command([node, npm, "--version"], work, env, capture=True)
    require(version == descriptor["tools"]["npm"], f"npm version mismatch: {version}")
    receipt("npm-tool", path=str(npm), version=version, sha256=digest(npm))
    return [str(node), str(npm)]


def download(source, target):
    require(source["url"].startswith("https://codeload.github.com/agentclientprotocol/claude-agent-acp/tar.gz/"), "unexpected source locator")
    with urllib.request.urlopen(source["url"], timeout=60) as response, target.open("xb") as output:
        require(response.geturl() == source["url"], "unexpected source redirect")
        remaining = source["bytes"]
        while remaining:
            block = response.read(min(1024 * 1024, remaining))
            require(block, "truncated source download")
            output.write(block)
            remaining -= len(block)
        require(not response.read(1), "oversized source download")
    check_file(target, source)
    receipt("source-acquired", sha256=source["sha256"], bytes=source["bytes"])


def lock_check(root, descriptor):
    check_file(root / "package-lock.json", {"sha256": descriptor["lock_sha256"]})
    lock = read_json(root / "package-lock.json")
    for name, package in lock["packages"].items():
        if name:
            require(package.get("resolved", "").startswith("https://registry.npmjs.org/")
                    and package.get("integrity", "").startswith("sha512-"), f"unlocked dependency: {name}")
    return lock


def build_source(descriptor, manifest, descriptor_path, work, node, env):
    archive = work / "source.tar.gz"
    download(descriptor["source"], archive)
    source = work / "source"
    extract(archive, source, descriptor["source"]["root"], descriptor["source"]["entries"], normalize_group_write=True)
    patch = descriptor_path.parent / descriptor["patch"]["file"]
    check_file(patch, descriptor["patch"])
    command(["git", "apply", "--check", patch], source, env)
    command(["git", "apply", patch], source, env)
    verify_files(source, descriptor["canonical"]["entries"])
    lock_check(source, descriptor)
    npm = npm_tool(descriptor, node, work, env)
    command(npm + ["ci", "--ignore-scripts", "--no-audit", "--no-fund"], source, env)
    version = read_json(source / "node_modules/typescript/package.json")["version"]
    require(version == descriptor["tools"]["typescript"], "TypeScript version mismatch")
    command(npm + ["run", "build"], source, env)
    packs = work / "pack"
    packs.mkdir()
    command(npm + ["pack", "--ignore-scripts", "--json", "--pack-destination", str(packs)], source, env)
    packed = list(packs.iterdir())
    require(len(packed) == 1 and packed[0].is_file(), "expected one package archive")
    check_file(packed[0], descriptor["pack"])
    lock_check(source, descriptor)
    for name, expected in descriptor["canonical"]["entries"].items():
        verify_entry(source, name, expected)
    fixture = work / "fixture"
    extract(packed[0], fixture, "package", descriptor["pack"]["entries"])
    for name in [*descriptor["support"], "package-lock.json"]:
        target = fixture / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source / name, target)
    command(npm + ["ci", "--ignore-scripts", "--omit=dev", "--no-audit", "--no-fund"], fixture, env)
    verify_files(fixture, manifest["files"])
    shutil.copyfile(descriptor_path.parent / descriptor["manifest"]["file"], fixture / "FIXTURE-MANIFEST.json")
    return fixture


def fixture_entries(descriptor, manifest):
    return {**manifest["files"], "FIXTURE-MANIFEST.json": {
        "mode": "100644", "bytes": descriptor["manifest"]["bytes"],
        "sha256": descriptor["manifest"]["sha256"],
    }}


def validate_fixture(root, descriptor, manifest):
    verify_files(root, fixture_entries(descriptor, manifest))
    lock_check(root, descriptor)
    installed = read_json(root / "node_modules/.package-lock.json")["packages"]
    require(set(installed) == set(manifest["runtime_dependencies"]), "installed package set mismatch")
    for name, expected in manifest["runtime_dependencies"].items():
        require({key: installed[name].get(key) for key in ("version", "resolved", "integrity")} == expected,
                f"installed package identity mismatch: {name}")


def prepare(descriptor_path, cache, bundle=None, build=False):
    platform_check()
    descriptor_path = descriptor_path.resolve()
    descriptor, manifest = configuration(descriptor_path)
    # Validate explicit input even when a valid ready cache exists.
    if bundle is not None:
        check_file(bundle, descriptor["bundle"])
    require(not cache.is_symlink(), "cache directory must not be a symlink")
    cache.mkdir(parents=True, exist_ok=True, mode=0o700)
    cache = cache.resolve()
    key = "callback-fixture-v1-linux-x64-" + descriptor["manifest"]["sha256"]
    ready = cache / key
    fd = os.open(cache / (key + ".lock"), os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        with tempfile.TemporaryDirectory(prefix="." + key + "-", dir=cache) as temporary:
            work = Path(temporary)
            env = environment(work)
            node = node_tool(descriptor, work, env)
            if ready.exists() or ready.is_symlink():
                require(ready.is_dir() and not ready.is_symlink(), "invalid ready directory")
                require({p.name for p in ready.iterdir()} == {"fixture"}, "extra ready-cache entry")
                validate_fixture(ready / "fixture", descriptor, manifest)
                receipt("cache-verified", manifest=descriptor["manifest"]["sha256"])
                return (ready / "fixture").resolve()
            require(bundle is not None or build, "verified fixture cache missing; supply --bundle or --build-from-source")
            if bundle is not None:
                fixture = work / "fixture"
                extract(bundle, fixture, descriptor["bundle"]["root"], fixture_entries(descriptor, manifest),
                        normalize_group_write=descriptor["bundle"].get("normalize_group_write", False),
                        archive_inventory_sha256=descriptor["bundle"].get("archive_inventory_sha256"))
                route = "explicit-bundle"
            else:
                fixture = build_source(descriptor, manifest, descriptor_path, work, node, env)
                route = "source-build"
            validate_fixture(fixture, descriptor, manifest)
            publish = work / "ready"
            publish.mkdir()
            fixture.rename(publish / "fixture")
            publish.rename(ready)
            receipt("fixture-ready", route=route, payload_entries=len(manifest["files"]),
                    runtime_packages=len(manifest["runtime_dependencies"]), manifest=descriptor["manifest"]["sha256"])
            return (ready / "fixture").resolve()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--descriptor", type=Path, default=DEFAULT_DESCRIPTOR)
    parser.add_argument("--cache-dir", type=Path, required=True)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--bundle", type=Path)
    mode.add_argument("--offline", action="store_true")
    mode.add_argument("--build-from-source", action="store_true")
    args = parser.parse_args()

    def interrupted(_signal, _frame):
        raise InterruptedError("preparation interrupted")

    signal.signal(signal.SIGTERM, interrupted)
    os.umask(0o022)
    try:
        root = prepare(args.descriptor, args.cache_dir, args.bundle, args.build_from_source)
        print(root, flush=True)
    except (InvalidFixture, OSError, ValueError, KeyError, tarfile.TarError,
            subprocess.TimeoutExpired, KeyboardInterrupt) as error:
        print(f"callback fixture: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
