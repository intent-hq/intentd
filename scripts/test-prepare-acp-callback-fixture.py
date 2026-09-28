#!/usr/bin/env python3
"""Offline provisioner controls; synthetic packages never execute npm or Node."""

import copy
import hashlib
import importlib.util
import io
import json
import multiprocessing
import os
from pathlib import Path
import shutil
import tarfile
import tempfile
import unittest
from unittest.mock import patch


SPEC = importlib.util.spec_from_file_location(
    "prepare_fixture", Path(__file__).with_name("prepare-acp-callback-fixture.py")
)
PREP = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PREP)
ORIGINAL_NODE_TOOL = PREP.node_tool


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def inventory(root, git=False):
    result = {}
    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            raw = os.fsencode(os.readlink(path))
            entry = {"mode": "120000", "target": os.readlink(path)}
        elif path.is_file():
            raw = path.read_bytes()
            entry = {"mode": "100755" if path.stat().st_mode & 0o100 else "100644"}
        else:
            continue
        entry.update(bytes=len(raw), sha256=sha(raw))
        if git:
            entry["git_blob"] = hashlib.sha1(f"blob {len(raw)}\0".encode() + raw).hexdigest()
        result[str(path.relative_to(root))] = entry
    return result


def write(path, raw):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(raw)
    path.chmod(0o644)


def archive(root, path, prefix):
    with tarfile.open(path, "w:gz") as output:
        for file in sorted(root.rglob("*")):
            output.add(file, arcname=prefix + "/" + str(file.relative_to(root)), recursive=False)


class Fixture:
    def __init__(self, root):
        self.root = root
        self.payload = root / "original"
        self.cache = root / "cache"
        self.bundle = root / "bundle.tar.gz"
        self.descriptor_path = root / "descriptor.json"
        write(self.payload / "package.json", b'{"name":"offline-fixture","version":"1"}\n')
        write(self.payload / "dist/index.js", b"// synthetic, never executed\n")
        (self.payload / "dist/index.js").chmod(0o755)
        write(self.payload / "package-lock.json", b'{"packages":{}}\n')
        write(self.payload / "node_modules/.package-lock.json", b'{"packages":{}}\n')
        write(self.payload / "node_modules/which/bin/node-which", b"synthetic executable\n")
        write(self.payload / "node_modules/@anthropic-ai/sdk/bin/cli", b"synthetic executable\n")
        for name, target in [("node-which", "../which/bin/node-which"),
                             ("anthropic-ai-sdk", "../@anthropic-ai/sdk/bin/cli")]:
            link = self.payload / "node_modules/.bin" / name
            link.parent.mkdir(parents=True, exist_ok=True)
            link.symlink_to(target)
        files = inventory(self.payload)
        entries = {"package.json": inventory(self.payload, git=True)["package.json"]}
        tree = PREP.tree_id(entries)
        self.manifest = {
            "platform": "linux", "architecture": "x64", "files": files,
            "source_tree": tree, "pack_sha256": "0" * 64,
            "lock_sha256": sha((self.payload / "package-lock.json").read_bytes()),
            "runtime_dependencies": {},
        }
        self.descriptor = {
            "format": "intent-acp-callback-fixture-v1", "platform": "linux", "architecture": "x64",
            "tools": {"node": "v24.21.0", "npm": "11.19.0", "typescript": "6.0.3"},
            "source": {"tree": tree, "entries": entries}, "canonical": {"tree": tree, "entries": entries},
            "pack": {"sha256": "0" * 64}, "bundle": {"root": "fixture"},
            "lock_sha256": self.manifest["lock_sha256"],
            "expected_payload_entries": len(files), "expected_runtime_packages": 0,
        }
        write(root / "delta.patch", b"")
        self.descriptor["patch"] = {"file": "delta.patch", "bytes": 0, "sha256": sha(b"")}
        self.seal()

    def seal(self):
        raw = (json.dumps(self.manifest, indent=2) + "\n").encode()
        write(self.root / "files.json", raw)
        write(self.payload / "FIXTURE-MANIFEST.json", raw)
        self.descriptor["manifest"] = {"file": "files.json", "bytes": len(raw), "sha256": sha(raw)}
        archive(self.payload, self.bundle, "fixture")
        self.descriptor["bundle"].update(bytes=self.bundle.stat().st_size, sha256=PREP.digest(self.bundle))
        write(self.descriptor_path, (json.dumps(self.descriptor) + "\n").encode())

    def prepare(self, bundle=True, build=False):
        return PREP.prepare(self.descriptor_path, self.cache, self.bundle if bundle else None, build)


def child_prepare(descriptor, cache, bundle, connection):
    try:
        connection.send((True, str(PREP.prepare(Path(descriptor), Path(cache), Path(bundle)))))
    except Exception as error:
        connection.send((False, str(error)))
    finally:
        connection.close()


class ProvisionerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="callback-provisioner-test-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.fixture = Fixture(self.root)
        # No network or real Node/npm process is allowed in this suite.
        self.addCleanup(patch.stopall)
        patch.dict(os.environ, {"NODE_OPTIONS": ""}).start()
        patch.object(PREP, "node_tool", return_value=Path("/synthetic/node")).start()
        patch.object(PREP.urllib.request, "urlopen", side_effect=AssertionError("offline test attempted network")).start()

    def refused(self, function, *args, match=None, **kwargs):
        with self.assertRaises((PREP.InvalidFixture, OSError, ValueError, tarfile.TarError)) as caught:
            function(*args, **kwargs)
        if match:
            self.assertIn(match, str(caught.exception))

    def test_cold_warm_and_relocated_payload_keep_modes_and_internal_links(self):
        ready = self.fixture.prepare()
        before = inventory(ready)
        self.assertEqual(ready, self.fixture.prepare(bundle=False))
        self.assertEqual(before, inventory(ready))
        copied = self.root / "relocated"
        shutil.copytree(ready, copied, symlinks=True)
        PREP.validate_fixture(copied, self.fixture.descriptor, self.fixture.manifest)
        self.assertEqual((copied / "dist/index.js").stat().st_mode & 0o777, 0o755)
        self.assertEqual(os.readlink(copied / "node_modules/.bin/node-which"), "../which/bin/node-which")

    def test_explicit_missing_truncated_and_wrong_identity_never_fall_through(self):
        ready = self.fixture.prepare()
        original = self.fixture.bundle.read_bytes()
        for raw in (original[:-1], b"X" + original[1:]):
            with self.subTest(length=len(raw)):
                self.fixture.bundle.write_bytes(raw)
                self.refused(self.fixture.prepare)
                self.assertTrue(ready.is_dir())
        self.fixture.bundle.unlink()
        self.refused(self.fixture.prepare)
        self.assertEqual(ready, self.fixture.prepare(bundle=False))

    def test_manifest_identity_is_checked_before_cache_exposure(self):
        self.fixture.prepare()
        (self.root / "files.json").write_bytes(b"{}")
        self.refused(self.fixture.prepare, bundle=False, match="size mismatch")

    def test_warm_mutation_removal_extra_mode_and_lock_fail_closed(self):
        ready = self.fixture.prepare()
        def mutation(kind):
            target = ready / "dist/index.js"
            if kind == "bytes": target.write_bytes(b"X" * target.stat().st_size)
            elif kind == "missing": target.unlink()
            elif kind == "extra": write(ready / "unexpected", b"extra")
            elif kind == "directory": (ready / "unexpected").mkdir()
            elif kind == "mode": target.chmod(0o644)
            elif kind == "lock": (ready / "package-lock.json").write_text("{}")
        for kind in ("bytes", "missing", "extra", "directory", "mode", "lock"):
            with self.subTest(kind=kind):
                mutation(kind)
                self.refused(self.fixture.prepare, bundle=False)
                shutil.rmtree(ready)
                shutil.copytree(self.fixture.payload, ready, symlinks=True)

    def test_runtime_package_tuple_requires_more_than_a_matching_install_exit(self):
        installed = self.fixture.payload / "node_modules/.package-lock.json"
        write(installed, b'{"packages":{"node_modules/fake":{"version":"1"}}}\n')
        self.fixture.manifest["files"]["node_modules/.package-lock.json"] = inventory(self.fixture.payload)["node_modules/.package-lock.json"]
        self.fixture.seal()
        self.refused(self.fixture.prepare, match="installed package set mismatch")

    def test_unsafe_tar_paths_duplicates_and_special_entries_are_rejected(self):
        for kind in ("traversal", "absolute", "duplicate", "hardlink", "fifo", "symlink-parent"):
            with self.subTest(kind=kind):
                archive_path = self.root / (kind + ".tgz")
                with tarfile.open(archive_path, "w:gz") as output:
                    info = tarfile.TarInfo({"traversal": "fixture/../outside", "absolute": "/outside"}.get(kind, "fixture/dist/index.js"))
                    info.mode = 0o755
                    if kind == "hardlink": info.type, info.linkname = tarfile.LNKTYPE, "/outside"
                    elif kind == "fifo": info.type = tarfile.FIFOTYPE
                    elif kind == "symlink-parent": info.name, info.type, info.linkname = "fixture/dist", tarfile.SYMTYPE, "../../outside"
                    else: info.size = len(b"// synthetic, never executed\n")
                    output.addfile(info, io.BytesIO(b"// synthetic, never executed\n") if info.isfile() else None)
                    if kind == "duplicate": output.addfile(info, io.BytesIO(b"// synthetic, never executed\n"))
                self.refused(PREP.extract, archive_path, self.root / ("out-" + kind), "fixture", self.fixture.manifest["files"])
                self.assertFalse((self.root / "outside").exists())

    def test_tampered_or_escaping_internal_link_is_never_followed(self):
        ready = self.fixture.prepare()
        link = ready / "node_modules/.bin/node-which"
        for target in ("/etc/passwd", "../../../../outside", "../@anthropic-ai/sdk/bin/cli"):
            link.unlink()
            link.symlink_to(target)
            self.refused(self.fixture.prepare, bundle=False, match="link mismatch")
        link.unlink()
        link.symlink_to("../which/bin/node-which")
        self.assertEqual(ready, self.fixture.prepare(bundle=False))

    def test_codeload_group_write_normalizes_only_for_git_source(self):
        path = self.root / "source-modes.tgz"
        raw = b"source\n"
        expected = {"file": {"bytes": len(raw), "sha256": sha(raw), "mode": "100644"}}
        with tarfile.open(path, "w:gz") as output:
            item = tarfile.TarInfo("source/file")
            item.mode, item.size = 0o664, len(raw)
            output.addfile(item, io.BytesIO(raw))
        PREP.extract(path, self.root / "source-mode", "source", expected, normalize_group_write=True)
        self.assertEqual((self.root / "source-mode/file").stat().st_mode & 0o777, 0o644)
        self.refused(PREP.extract, path, self.root / "bundle-mode", "source", expected, match="mode mismatch")
        headers = [{"name": "source/file", "type": "0", "mode": 0o664, "size": len(raw), "link": ""}]
        identity = sha((json.dumps(headers, sort_keys=True, separators=(",", ":")) + "\n").encode())
        PREP.extract(path, self.root / "pinned-bundle-mode", "source", expected,
                     normalize_group_write=True, archive_inventory_sha256=identity)
        self.refused(PREP.extract, path, self.root / "wrong-headers", "source", expected,
                     normalize_group_write=True, archive_inventory_sha256="0" * 64,
                     match="header inventory mismatch")

    def test_two_processes_publish_one_complete_root(self):
        context = multiprocessing.get_context("fork")
        processes, readers = [], []
        for _ in range(2):
            parent, child = context.Pipe(duplex=False)
            process = context.Process(target=child_prepare, args=(str(self.fixture.descriptor_path), str(self.fixture.cache), str(self.fixture.bundle), child))
            process.start()
            child.close()
            processes.append(process)
            readers.append(parent)
        try:
            replies = []
            for connection in readers:
                self.assertTrue(connection.poll(15), "preparer did not finish")
                replies.append(connection.recv())
            for process in processes:
                process.join(15)
                self.assertEqual(process.exitcode, 0)
            self.assertEqual(replies[0], replies[1])
            self.assertTrue(replies[0][0], replies)
            self.assertEqual(len([p for p in self.fixture.cache.iterdir() if p.is_dir()]), 1)
        finally:
            for connection in readers: connection.close()
            for process in processes:
                if process.is_alive(): process.kill(); process.join()

    def test_killed_staging_never_publishes_and_preserves_another_ready_root(self):
        other = self.root / "other"
        other.mkdir()
        previous = Fixture(other)
        previous.cache = self.fixture.cache
        old_root = previous.prepare()
        before = inventory(old_root)
        write(self.fixture.payload / "dist/index.js", b"new staged payload\n")
        self.fixture.manifest["files"]["dist/index.js"] = inventory(self.fixture.payload)["dist/index.js"]
        self.fixture.seal()
        context = multiprocessing.get_context("fork")
        entered = context.Event()
        held = context.Event()
        original = PREP.extract
        def hold(*args, **kwargs):
            original(*args, **kwargs)
            entered.set()
            held.wait(30)
        with patch.object(PREP, "extract", side_effect=hold):
            process = context.Process(target=self.fixture.prepare)
            process.start()
            try:
                self.assertTrue(entered.wait(15), "extraction boundary was not reached")
                process.kill()
                process.join(15)
            finally:
                if process.is_alive(): process.kill(); process.join()
        self.assertEqual(inventory(old_root), before)
        self.assertEqual(previous.prepare(bundle=False), old_root)
        self.refused(self.fixture.prepare, bundle=False, match="cache missing")
        self.fixture.prepare()
        self.assertEqual(inventory(old_root), before)

    def test_offline_miss_platform_and_injection_refuse_without_acquisition(self):
        self.refused(self.fixture.prepare, bundle=False, match="cache missing")
        with patch.object(PREP.platform, "machine", return_value="aarch64"):
            self.refused(self.fixture.prepare, match="Linux x64")
        with patch.dict(os.environ, {"NODE_OPTIONS": "--import /untrusted"}):
            self.refused(self.fixture.prepare, match="injection")

    def test_isolated_environment_drops_credentials_and_injected_configuration(self):
        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "synthetic", "NPM_TOKEN": "synthetic", "NPM_CONFIG_REGISTRY": "https://invalid", "NODE_OPTIONS": "--import invalid"}):
            env = PREP.environment(self.root)
        self.assertNotIn("ANTHROPIC_API_KEY", env)
        self.assertNotIn("NPM_TOKEN", env)
        self.assertEqual(env["NODE_OPTIONS"], "")
        self.assertEqual(env["NPM_CONFIG_REGISTRY"], "https://registry.npmjs.org/")
        self.assertNotEqual(env["NPM_CONFIG_GLOBALCONFIG"], env["NPM_CONFIG_USERCONFIG"])

    def test_actual_node_version_check_refuses_wrong_version(self):
        with patch.object(PREP.shutil, "which", return_value="/synthetic/node"), patch.object(PREP, "command", return_value="v22.0.0"):
            self.refused(ORIGINAL_NODE_TOOL, self.fixture.descriptor, self.root, {"PATH": ""}, match="Node version mismatch")

    def test_npm_mismatch_installs_only_the_exact_isolated_tool(self):
        calls = []
        target = self.root / "npm-tool/node_modules/npm/bin/npm-cli.js"

        def run(args, cwd, env, capture=False):
            calls.append([str(arg) for arg in args])
            if "install" in args:
                write(target, b"synthetic npm, never executed")
                return ""
            return "11.19.0" if Path(args[1]) == target else "11.18.0"

        with patch.object(PREP.shutil, "which", return_value="/synthetic/npm"), patch.object(PREP, "command", side_effect=run):
            command = PREP.npm_tool(self.fixture.descriptor, Path("/synthetic/node"), self.root, {"PATH": ""})
        self.assertEqual(command, ["/synthetic/node", str(target)])
        self.assertEqual(calls[1], ["/synthetic/node", "/synthetic/npm", "install", "--prefix",
                                  str(self.root / "npm-tool"), "--ignore-scripts", "--no-audit",
                                  "--no-fund", "--no-package-lock", "npm@11.19.0"])
        self.assertEqual(len(calls), 3)

    def test_pinned_descriptor_and_full_git_tree_hashes_are_consistent(self):
        descriptor, manifest = PREP.configuration(PREP.DEFAULT_DESCRIPTOR)
        self.assertEqual(len(descriptor["source"]["entries"]), 116)
        self.assertEqual(len(descriptor["canonical"]["entries"]), 121)
        self.assertEqual(len(manifest["files"]), 6367)


class SourceBoundaryTests(unittest.TestCase):
    def test_stubbed_pipeline_is_ordered_and_rejects_each_integrity_boundary(self):
        # This models tool outputs only. No git apply, npm, tsc or package code runs.
        for failure in (None, "archive", "patch", "source", "pack", "runtime"):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                f = Fixture(root)
                source_input = root / "source-input"
                source_input.mkdir()
                for name in ("package.json", "package-lock.json"):
                    shutil.copy2(f.payload / name, source_input / name)
                write(source_input / "AGENTS.md", b"source instructions\n")
                (source_input / "CLAUDE.md").symlink_to("AGENTS.md")
                source_entries = inventory(source_input, git=True)
                source_archive = root / "source-input.tgz"
                archive(source_input, source_archive, "baseline")
                descriptor = copy.deepcopy(f.descriptor)
                descriptor.update(source={"root": "baseline", "entries": source_entries}, canonical={"entries": source_entries}, support=[])
                package = root / "package-input"
                package.mkdir()
                for name in ("package.json", "dist/index.js"):
                    write(package / name, (f.payload / name).read_bytes())
                    (package / name).chmod((f.payload / name).stat().st_mode & 0o777)
                pack_archive = root / "package.tgz"
                archive(package, pack_archive, "package")
                descriptor["pack"] = {"entries": inventory(package), "sha256": PREP.digest(pack_archive)}
                runtime = root / "runtime-template"
                shutil.copytree(f.payload, runtime, symlinks=True)
                (runtime / "FIXTURE-MANIFEST.json").unlink()
                f.manifest["files"] = inventory(runtime)
                work = root / "work"
                work.mkdir()
                calls = []
                def fetch(_source, target):
                    shutil.copyfile(source_archive, target)
                    if failure == "archive": target.write_bytes(b"not an archive")
                def run(args, cwd, env, capture=False):
                    args = [str(arg) for arg in args]
                    calls.append(args)
                    if args[0] == "git":
                        if failure == "source" and "--check" not in args:
                            (cwd / "package.json").write_text("changed")
                    elif "ci" in args and "--omit=dev" not in args:
                        write(cwd / "node_modules/typescript/package.json", b'{"version":"6.0.3"}')
                    elif "pack" in args:
                        destination = Path(args[args.index("--pack-destination") + 1]) / "fixture.tgz"
                        shutil.copyfile(pack_archive, destination)
                        if failure == "pack": destination.write_bytes(b"wrong package")
                    elif "--omit=dev" in args:
                        shutil.copytree(runtime / "node_modules", cwd / "node_modules", symlinks=True)
                        if failure == "runtime": write(cwd / "unexpected", b"extra")
                    return ""
                if failure == "patch": (root / "delta.patch").write_bytes(b"wrong")
                with patch.object(PREP, "download", side_effect=fetch), patch.object(PREP, "command", side_effect=run), patch.object(PREP, "npm_tool", return_value=["npm"]):
                    operation = lambda: PREP.build_source(descriptor, f.manifest, f.descriptor_path, work, Path("node"), {})
                    if failure:
                        with self.assertRaises((PREP.InvalidFixture, tarfile.TarError)):
                            operation()
                    else:
                        result = operation()
                        self.assertEqual(result.name, "fixture")
                        self.assertEqual([args[1] for args in calls], ["apply", "apply", "ci", "run", "pack", "ci"])
                        for args in calls:
                            if args[0] == "npm" and args[1] != "run": self.assertIn("--ignore-scripts", args)


if __name__ == "__main__":
    unittest.main(verbosity=2)
