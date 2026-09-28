#!/usr/bin/env python3
"""Assemble and install the pinned Claude runtime without executing its code.

The adjacent descriptor is the trust root shipped with this tool. There is no
descriptor/manifest override. Assembly uses the complete authenticated canonical
fixture as input, but excludes its four test support files from the product.
Installation is offline and returns an absolute provider executable override;
it does not select a provider, authenticate, start a process, or modify settings.
Removal requires the deployment owner to deselect/drain that exact installation.
"""

import argparse
from contextlib import contextmanager
import ctypes
from dataclasses import dataclass
import fcntl
import gzip
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import platform
import posixpath
import shutil
import signal
import stat
import sys
import tarfile
import tempfile


DESCRIPTOR = Path(__file__).with_name("claude-callback-runtime.json")
DESCRIPTOR_SHA256 = "592a378105c930444b42ecd2e52bf5c6a017a5539dfe69ef7a94fb570ac9588d"
FIXTURE_MANIFEST = (
    Path(__file__).resolve().parents[1]
    / "crates/intent-acp/tests/fixtures/claude-callback-files.json"
)
SUPPORT = frozenset({
    "tests/callback-registration.test.mjs", "tests/mcp-peer.mjs",
    "tests/run-hermetic.mjs", "tests/scripted-query.mjs",
})
LAUNCHER = (
    b'#!/bin/sh\n'
    b'set -eu\n'
    b'runtime_bin=${0%/*}\n'
    b'exec "$runtime_bin/../node/bin/node" '
    b'"$runtime_bin/../runtime/dist/index.js" "$@"\n'
)
PREFIX = "claude-runtime"
ROOT_MARKER = ".intent-claude-runtime-root.json"
INSTALL_MARKER = ".intent-claude-runtime-install.json"


class InvalidRuntime(Exception):
    """An input, owned directory, or artifact violates the pinned contract."""


def require(condition, message):
    if not condition:
        raise InvalidRuntime(message)


def encoded(value):
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def read_json(raw):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            require(key not in result, f"duplicate JSON key: {key}")
            result[key] = value
        return result
    try:
        return json.loads(raw, object_pairs_hook=unique)
    except (ValueError, UnicodeError) as error:
        raise InvalidRuntime("malformed JSON") from error


def safe_name(name):
    require(isinstance(name, str) and name and "\\" not in name
            and not any(ord(c) < 32 for c in name)
            and not name.startswith("/")
            and all(p not in ("", ".", "..") for p in name.split("/")),
            f"unsafe relative path: {name!r}")
    return PurePosixPath(name)


def directories(entries):
    result = set()
    for name in entries:
        result.update(str(p) for p in safe_name(name).parents if str(p) != ".")
    require(not result.intersection(entries), "file/directory collision")
    return result


def validate_inventory(entries):
    directories(entries)
    for name, entry in entries.items():
        require(entry["mode"] in ("100644", "100755", "120000"), "invalid mode")
        require(type(entry["bytes"]) is int and entry["bytes"] >= 0, "invalid size")
        require(len(entry["sha256"]) == 64
                and all(c in "0123456789abcdef" for c in entry["sha256"]),
                "invalid digest")
        if entry["mode"] == "120000":
            target = entry["target"]
            require(target and not target.startswith("/") and "\\" not in target
                    and not any(ord(c) < 32 for c in target), "unsafe symlink")
            resolved = posixpath.normpath(posixpath.join(posixpath.dirname(name), target))
            require(resolved in entries and entries[resolved]["mode"] != "120000",
                    "link must target an inventoried regular file")
            require(sha(os.fsencode(target)) == entry["sha256"]
                    and len(os.fsencode(target)) == entry["bytes"], "invalid link digest")


@contextmanager
def regular(path):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        info = os.fstat(fd)
        require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1,
                f"not a single-link regular file: {path}")
        with os.fdopen(fd, "rb", closefd=False) as stream:
            yield stream, info
    finally:
        os.close(fd)


def digest_file(path):
    with regular(path) as (stream, _):
        return digest_stream(stream)


def digest_stream(stream):
    result = hashlib.sha256()
    for chunk in iter(lambda: stream.read(1024 * 1024), b""):
        result.update(chunk)
    return result.hexdigest()


def check_file(path, expected, mode=True):
    with regular(path) as (stream, info):
        require(info.st_size == expected["bytes"], f"size mismatch: {path}")
        if mode:
            require(stat.S_IMODE(info.st_mode) == int(expected["mode"][-3:], 8),
                    f"mode mismatch: {path}")
        result = hashlib.sha256()
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(chunk)
        require(result.hexdigest() == expected["sha256"], f"SHA256 mismatch: {path}")


def absolute_directory(path):
    path = Path(path)
    require(path.is_absolute() and ".." not in path.parts and path != Path("/"),
            "directory must be an unambiguous absolute path")
    for part in reversed((path, *path.parents)):
        if part.exists() or part.is_symlink():
            require(stat.S_ISDIR(part.lstat().st_mode), f"linked/non-directory root: {part}")
    return path


def verify_tree(root, entries):
    root = absolute_directory(root)
    require(root.is_dir(), f"missing tree: {root}")
    allowed_dirs = directories(entries)
    found = set()
    for parent, dirs, files in os.walk(root, followlinks=False):
        for name in dirs[:]:
            path = Path(parent) / name
            if path.is_symlink():
                dirs.remove(name)
                files.append(name)
            else:
                require(path.relative_to(root).as_posix() in allowed_dirs,
                        f"extra directory: {path}")
                require(stat.S_IMODE(path.stat().st_mode) == 0o755,
                        f"directory mode mismatch: {path}")
        for name in files:
            path = Path(parent) / name
            relative = path.relative_to(root).as_posix()
            require(relative in entries, f"extra payload: {relative}")
            entry = entries[relative]
            if entry["mode"] == "120000":
                require(path.is_symlink() and os.readlink(path) == entry["target"],
                        f"link mismatch: {relative}")
                require(path.resolve(strict=True).is_relative_to(root), "escaping symlink")
            else:
                check_file(path, entry)
            found.add(relative)
    require(found == set(entries), f"missing payload: {sorted(set(entries) - found)[:3]}")


def file_entry(raw, executable=False):
    return {"mode": "100755" if executable else "100644",
            "sha256": sha(raw), "bytes": len(raw)}


@dataclass(frozen=True)
class Contract:
    inventory: dict
    bundle: dict
    inputs: dict

    @property
    def identity(self):
        return sha(encoded(self.inventory))

    @property
    def manifest(self):
        return encoded({"identity": self.identity, "inventory": self.inventory})

    @property
    def files(self):
        return {**self.inventory["files"], "manifest.json": file_entry(self.manifest)}


def configuration():
    require(digest_file(DESCRIPTOR) == DESCRIPTOR_SHA256, "untrusted descriptor SHA256")
    data = read_json(DESCRIPTOR.read_bytes())
    require(data["format"] == "intent-claude-runtime-descriptor-v1", "unknown descriptor")
    contract = Contract(data["inventory"], data["artifact"], data["inputs"])
    require(contract.inventory["format"] == "intent-claude-runtime-inventory-v1",
            "unknown inventory")
    validate_inventory(contract.files)
    require(contract.bundle["identity"] == contract.identity, "identity mismatch")
    require(contract.bundle["manifest_sha256"] == sha(contract.manifest), "manifest mismatch")
    require(len(contract.inventory["files"]) == 6366, "production payload count mismatch")
    require(len(contract.inventory["runtime_packages"]) == 105, "runtime package count mismatch")
    require(contract.inventory["platform"] == "linux-x64-glibc", "unsupported platform")
    return contract


def platform_check():
    require(sys.platform == "linux" and platform.machine() in ("x86_64", "amd64"),
            "only Linux x64/glibc is supported")
    require(platform.libc_ver()[0] == "glibc", "glibc host required")


def write_file(path, raw, mode=0o644):
    with path.open("xb") as stream:
        stream.write(raw)
    path.chmod(mode)


def copy_file(source, destination, expected):
    with regular(source) as (stream, _), destination.open("xb") as output:
        shutil.copyfileobj(stream, output, 1024 * 1024)
    destination.chmod(int(expected["mode"][-3:], 8))
    check_file(destination, expected)


def stage_payload(contract, fixture, node, license_path, pack, target):
    """Verify the entire input, then copy only the fixed production projection."""
    require(digest_file(FIXTURE_MANIFEST) == contract.inputs["fixture_manifest_sha256"],
            "fixture manifest mismatch")
    fixture_manifest = read_json(FIXTURE_MANIFEST.read_bytes())
    input_files = fixture_manifest["files"]
    require(len(input_files) == 6367 and SUPPORT.issubset(input_files), "fixture shape mismatch")
    require(fixture_manifest["pack_sha256"] == contract.inputs["pack"]["sha256"]
            and fixture_manifest["lock_sha256"] == contract.inputs["lock_sha256"],
            "canonical input mismatch")
    require(fixture_manifest["runtime_dependencies"] == contract.inventory["runtime_packages"],
            "locked runtime graph mismatch")
    expected_runtime = {"runtime/" + p: e for p, e in input_files.items() if p not in SUPPORT}
    require(expected_runtime == {p: e for p, e in contract.inventory["files"].items()
                                 if p.startswith("runtime/")}, "projection mismatch")
    validate_inventory(input_files)
    # The accepted provisioner also carries the externally authenticated
    # manifest envelope. It is input metadata, not part of its own payload.
    verify_tree(fixture, {**input_files,
                         "FIXTURE-MANIFEST.json": file_entry(FIXTURE_MANIFEST.read_bytes())})
    check_file(pack, contract.inputs["pack"], mode=False)
    check_file(node, contract.inputs["node"], mode=False)
    check_file(license_path, contract.inputs["node_license"], mode=False)
    target.mkdir(mode=0o755)
    target.chmod(0o755)
    for name in sorted(directories(contract.files), key=lambda p: (p.count("/"), p)):
        (target / name).mkdir(mode=0o755)
        (target / name).chmod(0o755)
    for name, entry in sorted(contract.inventory["files"].items()):
        dest = target / name
        if entry["mode"] == "120000":
            dest.symlink_to(entry["target"])
        elif name == "bin/claude-agent-acp":
            write_file(dest, LAUNCHER, 0o755)
        else:
            source = (fixture / name.removeprefix("runtime/") if name.startswith("runtime/")
                      else node if name == "node/bin/node" else license_path)
            require(name.startswith("runtime/") or name in ("node/bin/node", "node/LICENSE"),
                    "unknown production entry")
            copy_file(source, dest, entry)
    write_file(target / "manifest.json", contract.manifest)
    verify_tree(target, contract.files)


def archive_payload(root, entries, output):
    """Canonical gzip(mtime=0)/PAX tar; no input metadata controls headers."""
    names = sorted({"", *directories(entries), *entries})
    with output.open("xb") as raw:
        with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0, compresslevel=6) as zipped:
            with tarfile.open(fileobj=zipped, mode="w", format=tarfile.PAX_FORMAT) as archive:
                for name in names:
                    header = tarfile.TarInfo(PREFIX + ("/" + name if name else ""))
                    header.uid = header.gid = header.mtime = 0
                    header.uname = header.gname = ""
                    if name not in entries:
                        header.type, header.mode = tarfile.DIRTYPE, 0o755
                        archive.addfile(header)
                    else:
                        entry = entries[name]
                        if entry["mode"] == "120000":
                            header.type, header.mode = tarfile.SYMTYPE, 0o777
                            header.linkname = entry["target"]
                            archive.addfile(header)
                        else:
                            header.mode = int(entry["mode"][-3:], 8)
                            header.size = entry["bytes"]
                            with regular(root / name) as (stream, _):
                                archive.addfile(header, stream)
        raw.flush()
        os.fsync(raw.fileno())
    output.chmod(0o644)


def check_bundle(contract, bundle):
    check_file(bundle, contract.bundle, mode=False)


def extract_bundle(contract, stream, target):
    entries = contract.files
    allowed_dirs = directories(entries)
    seen, files, links = set(), set(), []
    target.mkdir(mode=0o755)
    target.chmod(0o755)
    with tarfile.open(fileobj=stream, mode="r|gz") as archive:
        for member in archive:
            require(not member.pax_headers or set(member.pax_headers) <= {"path", "linkpath"},
                    "unsupported extended header")
            name = member.name
            safe_name(name)
            require(name not in seen, f"duplicate archive path: {name}")
            seen.add(name)
            require(name == PREFIX or name.startswith(PREFIX + "/"), "wrong archive prefix")
            relative = name[len(PREFIX):].lstrip("/")
            require(member.uid == member.gid == member.mtime == 0
                    and member.uname == member.gname == "", "noncanonical archive metadata")
            if member.isdir():
                require((not relative or relative in allowed_dirs) and member.mode == 0o755,
                        "unexpected archive directory")
                (target / relative).mkdir(parents=True, exist_ok=True, mode=0o755)
                (target / relative).chmod(0o755)
                continue
            require(relative in entries, f"extra archive payload: {relative}")
            entry = entries[relative]
            path = target / relative
            for ancestor in reversed(path.relative_to(target).parents):
                if str(ancestor) != ".":
                    (target / ancestor).mkdir(exist_ok=True, mode=0o755)
                    (target / ancestor).chmod(0o755)
            if member.issym():
                require(entry["mode"] == "120000" and member.linkname == entry["target"]
                        and member.mode == 0o777 and member.size == 0, "unexpected symlink")
                links.append((path, member.linkname))
            else:
                require(member.type == tarfile.REGTYPE and entry["mode"] != "120000",
                        "unsupported archive entry (including hardlink)")
                require(member.mode == int(entry["mode"][-3:], 8)
                        and member.size == entry["bytes"], "archive mode/size mismatch")
                with archive.extractfile(member) as source, path.open("xb") as output:
                    shutil.copyfileobj(source, output, 1024 * 1024)
                path.chmod(member.mode)
            files.add(relative)
    require(files == set(entries), "incomplete archive")
    for path, link in links:
        path.symlink_to(link)
    verify_tree(target, entries)


def rename_absent(source, destination):
    """Linux atomic publish that cannot replace even an empty destination."""
    libc = ctypes.CDLL(None, use_errno=True)
    try:
        rename = libc.renameat2
    except AttributeError as error:
        raise InvalidRuntime("renameat2(RENAME_NOREPLACE) is required") from error
    rename.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
    rename.restype = ctypes.c_int
    if rename(-100, os.fsencode(source), -100, os.fsencode(destination), 1) != 0:
        code = ctypes.get_errno()
        raise OSError(code, os.strerror(code), str(destination))


def owner_bytes(identity=None):
    return encoded({"format": "intent-claude-runtime-owned-v1", "uid": os.getuid(),
                    "identity": identity})


@contextmanager
def owned_root(root, create=False):
    root = absolute_directory(root)
    if create:
        # The caller supplies the parent; never create an ambiguous ancestor tree.
        require(root.parent.is_dir(), "install parent must already exist")
        root.mkdir(mode=0o700, exist_ok=True)
    require(root.is_dir(), "installation root is absent")
    info = root.stat()
    require(info.st_uid == os.getuid() and stat.S_IMODE(info.st_mode) == 0o700,
            "installation root must be owned by this uid with mode0700")
    marker = root / ROOT_MARKER
    if not marker.exists() and not marker.is_symlink():
        entries = set(p.name for p in root.iterdir())
        # A concurrent first installer may establish ownership during this
        # inventory. An appeared marker is only a reason to join the lock;
        # its exact bytes/mode/link state must still pass the locked check.
        require(create and (entries <= {".install.lock"}
                            or marker.exists() or marker.is_symlink()),
                "unowned installation root")
    flags = os.O_RDWR | os.O_NOFOLLOW | (os.O_CREAT if create else 0)
    fd = os.open(root / ".install.lock", flags, 0o600)
    try:
        info = os.fstat(fd)
        require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1
                and info.st_uid == os.getuid() and stat.S_IMODE(info.st_mode) == 0o600,
                "invalid installation lock")
        fcntl.flock(fd, fcntl.LOCK_EX)
        if not marker.exists() and not marker.is_symlink():
            require(create and set(p.name for p in root.iterdir()) == {".install.lock"},
                    "unowned installation root")
            write_file(marker, owner_bytes(), 0o600)
        check_file(marker, {**file_entry(owner_bytes()), "mode": "100600"})
        yield root
    finally:
        os.close(fd)


def installed_entries(contract):
    return {**contract.files, INSTALL_MARKER: {**file_entry(owner_bytes(contract.identity)),
                                              "mode": "100600"}}


def verify_install(contract, destination):
    info = destination.stat(follow_symlinks=False)
    require(stat.S_ISDIR(info.st_mode) and info.st_uid == os.getuid()
            and stat.S_IMODE(info.st_mode) == 0o755, "installed directory owner/mode mismatch")
    verify_tree(destination, installed_entries(contract))


def install(contract, bundle, root):
    # Authenticate even an explicitly supplied bundle on a warm installation.
    check_bundle(contract, bundle)
    with owned_root(root, create=True) as root:
        destination = root / contract.identity
        if destination.exists() or destination.is_symlink():
            verify_install(contract, destination)
        else:
            with tempfile.TemporaryDirectory(prefix=".stage-", dir=root) as scratch:
                staged = Path(scratch) / "runtime"
                with regular(bundle) as (stream, _):
                    # Recheck the same opened file used for extraction.
                    require(digest_stream(stream) == contract.bundle["sha256"], "bundle changed")
                    stream.seek(0)
                    extract_bundle(contract, stream, staged)
                write_file(staged / INSTALL_MARKER, owner_bytes(contract.identity), 0o600)
                verify_install(contract, staged)
                rename_absent(staged, destination)
        return destination / "bin/claude-agent-acp"


def remove(contract, root, identity, acknowledged):
    require(identity == contract.identity, "not this descriptor's identity")
    require(acknowledged, "deployment owner must confirm deselection and session drain")
    with owned_root(root) as root:
        destination = root / identity
        verify_install(contract, destination)
        with tempfile.TemporaryDirectory(prefix=".remove-", dir=root) as scratch:
            retired = Path(scratch) / "runtime"
            rename_absent(destination, retired)
            # Only the renamed original is removed; no replacement is followed.
            shutil.rmtree(retired)


def assemble(contract, fixture, node, license_path, pack, output):
    output = absolute_directory(output)
    require(not output.exists() and not output.is_symlink(), "assembly destination exists")
    require(output.parent.is_dir(), "assembly parent must exist")
    with tempfile.TemporaryDirectory(prefix=".assemble-", dir=output.parent) as scratch:
        stage = Path(scratch) / "result"
        stage.mkdir(mode=0o755)
        payload = stage / "payload"
        stage_payload(contract, fixture, node, license_path, pack, payload)
        bundle = stage / "claude-runtime.tar.gz"
        archive_payload(payload, contract.files, bundle)
        check_bundle(contract, bundle)
        write_file(stage / "manifest.json", contract.manifest)
        rename_absent(stage, output)
    return output / "claude-runtime.tar.gz"


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    assembly = commands.add_parser("assemble", help="verify local inputs; never execute packages")
    for flag in ("fixture", "node", "node-license", "pack", "output"):
        assembly.add_argument("--" + flag, type=Path, required=True)
    installer = commands.add_parser("install", help="offline install; prints absolute launcher")
    installer.add_argument("--bundle", type=Path, required=True)
    installer.add_argument("--root", type=Path, required=True)
    verification = commands.add_parser("verify", help="verify an owned installation")
    verification.add_argument("--root", type=Path, required=True)
    removal = commands.add_parser("remove", help="remove only the named owned installation")
    removal.add_argument("--root", type=Path, required=True)
    removal.add_argument("--identity", required=True)
    removal.add_argument("--confirm-deselected-and-drained", action="store_true")
    args = parser.parse_args(argv)
    platform_check()
    contract = configuration()
    if args.command == "assemble":
        result = assemble(contract, args.fixture, args.node, args.node_license, args.pack, args.output)
    elif args.command == "install":
        result = install(contract, args.bundle, args.root)
    elif args.command == "verify":
        with owned_root(args.root) as root:
            verify_install(contract, root / contract.identity)
        result = args.root / contract.identity / "bin/claude-agent-acp"
    else:
        remove(contract, args.root, args.identity, args.confirm_deselected_and_drained)
        result = args.identity
    print(result)


if __name__ == "__main__":
    def interrupted(_signum, _frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, interrupted)
    try:
        main()
    except (InvalidRuntime, OSError, ValueError, KeyError, TypeError, tarfile.TarError,
            KeyboardInterrupt) as error:
        print(f"runtime preparation refused: {error}", file=sys.stderr)
        sys.exit(1)
