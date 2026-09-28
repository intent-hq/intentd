#!/usr/bin/env python3
"""Offline installer tests. Fake package/Node bytes never execute an adapter.

The optional --production-artifacts suite verifies and relocates the complete
real inventory without launching its Node, adapter, SDK or native binaries.
"""

import argparse
from contextlib import redirect_stderr
import copy
import importlib.util
import io
import json
import multiprocessing
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch


SPEC = importlib.util.spec_from_file_location(
    "runtime_preparer", Path(__file__).with_name("prepare-claude-callback-runtime.py"))
runtime = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = runtime
SPEC.loader.exec_module(runtime)
REAL_ARTIFACTS = None
CANONICAL_FIXTURE = None


def fake_package(root):
    """A separately labeled inert payload; no product trust-root override."""
    payload = root / "fake-payload"
    payload.mkdir()
    child = (f"#!{sys.executable} -S\n" +
             "import json,os,signal,sys\n"
             "if '--hold' in sys.argv:\n"
             " print(os.getpid(),flush=True)\n"
             " signal.pause()\n"
             "else:\n"
             " print(json.dumps({'cwd':os.getcwd(),'args':sys.argv[1:],"
             "'input':sys.stdin.read()}))\n"
             " print('inert stderr',file=sys.stderr)\n"
             " sys.exit(37)\n").encode()
    raw = {"runtime/dist/index.js": b"inert fixture, not JavaScript\n",
           "runtime/package-lock.json": b"{}\n", "node/bin/node": child,
           "node/LICENSE": b"inert test license\n", "bin/claude-agent-acp": runtime.LAUNCHER}
    entries = {p: runtime.file_entry(v, p in ("node/bin/node", "bin/claude-agent-acp"))
               for p, v in raw.items()}
    target = "../dist/index.js"
    entries["runtime/bin/link"] = {"mode": "120000", "target": target,
                                   "sha256": runtime.sha(target.encode()), "bytes": len(target)}
    for name in sorted(runtime.directories(entries), key=lambda p: (p.count("/"), p)):
        (payload / name).mkdir(mode=0o755)
    for name, value in raw.items():
        runtime.write_file(payload / name, value, int(entries[name]["mode"][-3:], 8))
    (payload / "runtime/bin/link").symlink_to(target)
    inventory = {"format": "private-inert-test-fixture", "files": entries}
    contract = runtime.Contract(inventory, {}, {})
    runtime.write_file(payload / "manifest.json", contract.manifest)
    bundle = root / "fake.tar.gz"
    runtime.archive_payload(payload, contract.files, bundle)
    contract = runtime.Contract(inventory, {"sha256": runtime.digest_file(bundle),
                                           "bytes": bundle.stat().st_size}, {})
    return contract, bundle, payload


def concurrent_worker(contract, bundle, root, barrier, replies):
    try:
        barrier.wait(timeout=10)
        replies.put((True, str(runtime.install(contract, bundle, root))))
    except BaseException as error:
        replies.put((False, repr(error)))


def held_worker(contract, bundle, root, pipe):
    def cancelled(_signal, _frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, cancelled)
    original = runtime.rename_absent
    def held(source, destination):
        pipe.send("ready-before-publication")
        pipe.recv()
        original(source, destination)
    runtime.rename_absent = held
    try:
        runtime.install(contract, bundle, root)
    except KeyboardInterrupt:
        pipe.send("cancelled")


class OfflineTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="claude-runtime-offline-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.contract, self.bundle, self.payload = fake_package(self.root)
        self.install_root = self.root / "installed with spaces"

    def install(self):
        return runtime.install(self.contract, self.bundle, self.install_root)

    def test_archive_is_reproducible_and_relocatable(self):
        second = self.root / "second.tar.gz"
        runtime.archive_payload(self.payload, self.contract.files, second)
        self.assertEqual(second.read_bytes(), self.bundle.read_bytes())
        self.install()
        relocated = self.root / "relocated"
        self.install_root.rename(relocated)
        runtime.verify_install(self.contract, relocated / self.contract.identity)
        runtime.remove(self.contract, relocated, self.contract.identity, True)
        self.assertFalse((relocated / self.contract.identity).exists())

    def test_manifest_identity_has_no_self_hash_cycle(self):
        envelope = runtime.read_json(self.contract.manifest)
        self.assertEqual(envelope["identity"], runtime.sha(runtime.encoded(envelope["inventory"])))
        self.assertNotIn("manifest.json", envelope["inventory"]["files"])
        modified = copy.deepcopy(envelope["inventory"])
        modified["files"]["node/LICENSE"]["sha256"] = "0" * 64
        self.assertNotEqual(runtime.sha(runtime.encoded(modified)), envelope["identity"])

    def test_self_consistent_replacement_descriptor_is_not_a_new_trust_root(self):
        descriptor = json.loads(runtime.DESCRIPTOR.read_text())
        descriptor["inventory"]["node_version"] = "untrusted-self-consistent-version"
        forged = runtime.Contract(descriptor["inventory"], descriptor["artifact"], descriptor["inputs"])
        descriptor["artifact"]["identity"] = forged.identity
        descriptor["artifact"]["manifest_sha256"] = runtime.sha(forged.manifest)
        replacement = self.root / "descriptor.json"
        replacement.write_text(json.dumps(descriptor))
        with patch.object(runtime, "DESCRIPTOR", replacement), self.assertRaises(runtime.InvalidRuntime):
            runtime.configuration()

    def test_installed_root_mode_drift_is_refused(self):
        self.install()
        destination = self.install_root / self.contract.identity
        destination.chmod(0o777)
        with self.assertRaises(runtime.InvalidRuntime):
            self.install()
        self.assertEqual(destination.stat().st_mode & 0o777, 0o777)

    def test_offline_runner_has_no_python_site_instrumentation(self):
        self.assertTrue(sys.flags.no_site)
        self.assertTrue(sys.flags.isolated)
        self.assertFalse(any(name == "ddtrace" or name.startswith("ddtrace.") for name in sys.modules))

    def test_no_cli_descriptor_or_manifest_override(self):
        for flag in ("--descriptor", "--manifest"):
            with self.subTest(flag=flag), redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit) as error:
                    runtime.main(["install", "--root", str(self.install_root),
                                  "--bundle", str(self.bundle), flag, "untrusted.json"])
                self.assertEqual(error.exception.code, 2)
        self.assertFalse(self.install_root.exists())

    def test_warm_is_idempotent_and_still_checks_explicit_bundle(self):
        launcher = self.install()
        inode = launcher.stat().st_ino
        self.assertEqual(launcher, self.install())
        self.assertEqual(inode, launcher.stat().st_ino)
        self.bundle.write_bytes(self.bundle.read_bytes() + b"tamper")
        with self.assertRaises(runtime.InvalidRuntime):
            self.install()
        self.assertEqual(inode, launcher.stat().st_ino)

    def test_drift_is_refused_without_repair(self):
        self.install()
        path = self.install_root / self.contract.identity / "node/LICENSE"
        path.write_bytes(b"modified")
        with self.assertRaises(runtime.InvalidRuntime):
            self.install()
        with self.assertRaises(runtime.InvalidRuntime):
            runtime.remove(self.contract, self.install_root, self.contract.identity, True)
        self.assertEqual(path.read_bytes(), b"modified")

    def test_symlink_and_hardlink_inputs_are_refused(self):
        for mode in ("symlink", "hardlink"):
            with self.subTest(mode=mode):
                linked = self.root / mode
                if mode == "symlink":
                    linked.symlink_to(self.bundle)
                else:
                    os.link(self.bundle, linked)
                try:
                    with self.assertRaises((runtime.InvalidRuntime, OSError)):
                        runtime.install(self.contract, linked, self.install_root)
                finally:
                    linked.unlink()
        self.assertFalse(self.install_root.exists())

    def test_full_tree_rejects_extras_missing_modes_links_and_hardlinks(self):
        mutations = ("extra", "missing", "mode", "link", "hardlink", "directory")
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                tree = self.root / mutation
                shutil.copytree(self.payload, tree, symlinks=True)
                regular = tree / "node/LICENSE"
                if mutation == "extra":
                    (tree / "unlisted").write_bytes(b"extra")
                elif mutation == "missing":
                    regular.unlink()
                elif mutation == "mode":
                    regular.chmod(0o664)
                elif mutation == "link":
                    p = tree / "runtime/bin/link"
                    p.unlink()
                    p.symlink_to("../../../outside")
                elif mutation == "hardlink":
                    regular.unlink()
                    os.link(tree / "runtime/package-lock.json", regular)
                else:
                    (tree / "empty-extra").mkdir()
                with self.assertRaises((runtime.InvalidRuntime, OSError)):
                    runtime.verify_tree(tree, self.contract.files)

    def test_inventory_rejects_escaping_chained_and_colliding_links(self):
        for target in ("/etc/passwd", "../../outside", "link", "../bin/link"):
            with self.subTest(target=target):
                entries = copy.deepcopy(self.contract.inventory["files"])
                entries["runtime/bin/link"] = {"mode": "120000", "target": target,
                                               "sha256": runtime.sha(target.encode()),
                                               "bytes": len(target)}
                with self.assertRaises(runtime.InvalidRuntime):
                    runtime.validate_inventory(entries)
        entries = {**self.contract.files, "runtime": runtime.file_entry(b"collision")}
        with self.assertRaises(runtime.InvalidRuntime):
            runtime.validate_inventory(entries)

    def hostile_archive(self, mutation):
        raw = io.BytesIO()
        with tarfile.open(self.bundle, "r:gz") as original:
            members = [(copy.copy(m), original.extractfile(m).read() if m.isfile() else None)
                       for m in original.getmembers()]
        index = next(i for i, (m, _) in enumerate(members) if m.isfile())
        member, data = members[index]
        if mutation == "traversal":
            member.name = "claude-runtime/../escape"
        elif mutation == "absolute":
            member.name = "/absolute"
        elif mutation == "duplicate":
            members.append((copy.copy(member), data))
        elif mutation == "hardlink":
            member.type, member.linkname, member.size = tarfile.LNKTYPE, "node/LICENSE", 0
        elif mutation == "special":
            member.type, member.size = tarfile.FIFOTYPE, 0
        elif mutation == "mode":
            member.mode = 0o4755
        elif mutation == "missing":
            del members[index]
        elif mutation == "escaping-link":
            next(m for m, _ in members if m.issym()).linkname = "../../../escape"
        elif mutation == "corrupt-file":
            members[index] = (member, b"x" * len(data))
        elif mutation == "extra":
            extra = tarfile.TarInfo("claude-runtime/extra")
            extra.size, extra.mode = 1, 0o644
            members.append((extra, b"x"))
        with tarfile.open(fileobj=raw, mode="w:gz") as out:
            for member, data in members:
                out.addfile(member, io.BytesIO(data) if member.isfile() else None)
        raw.seek(0)
        return raw

    def test_hostile_archives_never_escape_private_stage(self):
        for case in ("traversal", "absolute", "duplicate", "hardlink", "special", "mode",
                     "missing", "escaping-link", "corrupt-file", "extra"):
            with self.subTest(case=case), self.assertRaises((runtime.InvalidRuntime, OSError)):
                runtime.extract_bundle(self.contract, self.hostile_archive(case), self.root / case)
        self.assertFalse((self.root / "escape").exists())
        with self.assertRaises(tarfile.TarError):
            runtime.extract_bundle(self.contract, io.BytesIO(b"bad gzip"), self.root / "bad")

    def test_duplicate_json_and_malformed_json_are_rejected(self):
        for value in (b'{"x":1,"x":2}', b"{", b"\xff"):
            with self.subTest(value=value), self.assertRaises(runtime.InvalidRuntime):
                runtime.read_json(value)

    def test_unowned_ambiguous_and_symlink_roots_are_untouched(self):
        unowned = self.root / "unowned"
        unowned.mkdir(mode=0o700)
        (unowned / "sentinel").write_bytes(b"keep")
        for action in (lambda: runtime.install(self.contract, self.bundle, unowned),
                       lambda: runtime.remove(self.contract, unowned, self.contract.identity, True)):
            with self.assertRaises(runtime.InvalidRuntime):
                action()
            self.assertEqual({p.name for p in unowned.iterdir()}, {"sentinel"})
        linked = self.root / "linked-root"
        linked.symlink_to(unowned, target_is_directory=True)
        for root in (linked, Path("relative"), self.root / ".." / "ambiguous", Path("/")):
            with self.subTest(root=root), self.assertRaises(runtime.InvalidRuntime):
                runtime.install(self.contract, self.bundle, root)

    def test_existing_destination_cannot_be_overwritten_even_if_empty(self):
        original = runtime.rename_absent
        def collide(source, destination):
            destination.mkdir()
            original(source, destination)
        with patch.object(runtime, "rename_absent", collide), self.assertRaises(FileExistsError):
            self.install()
        self.assertEqual(list((self.install_root / self.contract.identity).iterdir()), [])
        self.assertFalse(list(self.install_root.glob(".stage-*")))

    def test_concurrent_installers_publish_one_original_tree(self):
        ctx = multiprocessing.get_context("fork")
        barrier, replies = ctx.Barrier(3), ctx.Queue()
        children = [ctx.Process(target=concurrent_worker,
                                args=(self.contract, self.bundle, self.install_root, barrier, replies))
                    for _ in range(3)]
        for child in children:
            child.start()
        try:
            results = [replies.get(timeout=15) for _ in children]
            self.assertTrue(all(ok for ok, _ in results), results)
            self.assertEqual(len({result for _, result in results}), 1)
            for child in children:
                child.join(timeout=10)
                self.assertEqual(child.exitcode, 0)
            runtime.verify_install(self.contract, self.install_root / self.contract.identity)
            self.assertFalse(list(self.install_root.glob(".stage-*")))
        finally:
            for child in children:
                if child.is_alive():
                    child.kill()
                child.join()

    def test_signal_before_publish_cleans_exact_private_stage(self):
        ctx = multiprocessing.get_context("fork")
        parent, child_pipe = ctx.Pipe()
        child = ctx.Process(target=held_worker,
                            args=(self.contract, self.bundle, self.install_root, child_pipe))
        child.start()
        try:
            self.assertTrue(parent.poll(10))
            self.assertEqual(parent.recv(), "ready-before-publication")
            self.assertFalse((self.install_root / self.contract.identity).exists())
            os.kill(child.pid, signal.SIGTERM)
            self.assertTrue(parent.poll(10))
            self.assertEqual(parent.recv(), "cancelled")
            child.join(timeout=10)
            self.assertEqual(child.exitcode, 0)
            self.assertFalse((self.install_root / self.contract.identity).exists())
            self.assertFalse(list(self.install_root.glob(".stage-*")))
        finally:
            if child.is_alive():
                child.kill()
            child.join()
            parent.close()
            child_pipe.close()

    def test_removal_requires_exact_owned_identity_and_explicit_drain_declaration(self):
        self.install()
        other = self.install_root / ("f" * 64)
        other.mkdir()
        (other / "sentinel").write_text("successor")
        for identity, acknowledged in (("f" * 64, True), (self.contract.identity, False)):
            with self.subTest(identity=identity), self.assertRaises(runtime.InvalidRuntime):
                runtime.remove(self.contract, self.install_root, identity, acknowledged)
        runtime.remove(self.contract, self.install_root, self.contract.identity, True)
        self.assertEqual((other / "sentinel").read_text(), "successor")
        with self.assertRaises(FileNotFoundError):
            runtime.remove(self.contract, self.install_root, self.contract.identity, True)

    def test_removal_never_follows_a_replacement_after_retirement(self):
        self.install()
        original = runtime.rename_absent
        destination = self.install_root / self.contract.identity
        def replace_after_move(source, target):
            original(source, target)
            source.mkdir()
            (source / "sentinel").write_text("replacement")
        with patch.object(runtime, "rename_absent", replace_after_move):
            runtime.remove(self.contract, self.install_root, self.contract.identity, True)
        self.assertEqual((destination / "sentinel").read_text(), "replacement")

    def test_missing_install_receipt_and_linked_identity_cannot_be_removed(self):
        self.install()
        destination = self.install_root / self.contract.identity
        (destination / runtime.INSTALL_MARKER).unlink()
        with self.assertRaises(runtime.InvalidRuntime):
            runtime.remove(self.contract, self.install_root, self.contract.identity, True)
        held = self.root / "held-original"
        destination.rename(held)
        destination.symlink_to(held, target_is_directory=True)
        with self.assertRaises(runtime.InvalidRuntime):
            runtime.remove(self.contract, self.install_root, self.contract.identity, True)
        self.assertTrue((held / "node/LICENSE").is_file())

    def test_launcher_preserves_cwd_literal_argv_stdio_and_exit_inert_child_only(self):
        self.install()
        relocated = self.root / "relocated inert runtime"
        self.install_root.rename(relocated)
        launcher = relocated / self.contract.identity / "bin/claude-agent-acp"
        args = ["space value", "$(not-executed)", "--cli", "quote'\"value", ""]
        result = subprocess.run([str(launcher), *args], cwd=self.root, input="inert stdin\n",
                                text=True, capture_output=True, timeout=10,
                                env={"PATH": "/usr/bin:/bin", "HOME": str(self.root)})
        self.assertEqual(result.returncode, 37)
        value = json.loads(result.stdout)
        self.assertEqual(value["cwd"], str(self.root))
        self.assertEqual(value["args"][1:], args)
        self.assertTrue(value["args"][0].endswith("/runtime/dist/index.js"))
        self.assertEqual(value["input"], "inert stdin\n")
        self.assertEqual(result.stderr, "inert stderr\n")

    def test_launcher_exec_keeps_pid_and_signal_inert_child_only(self):
        launcher = self.install()
        child = subprocess.Popen([str(launcher), "--hold"], stdout=subprocess.PIPE, text=True,
                                 env={"PATH": "/usr/bin:/bin", "HOME": str(self.root)})
        try:
            import selectors
            with selectors.DefaultSelector() as selector:
                selector.register(child.stdout, selectors.EVENT_READ)
                self.assertTrue(selector.select(10), "inert child did not start")
            self.assertEqual(int(child.stdout.readline()), child.pid)
            child.terminate()
            self.assertEqual(child.wait(timeout=10), -signal.SIGTERM)
        finally:
            if child.poll() is None:
                child.kill()
            child.wait()
            child.stdout.close()


class ProductionInventoryTests(unittest.TestCase):
    """Actual complete product bytes, no execution of any included executable."""

    def test_full_real_artifact_installs_relocates_verifies_and_removes(self):
        contract = runtime.configuration()
        bundle = REAL_ARTIFACTS / "claude-runtime.tar.gz"
        runtime.verify_tree(REAL_ARTIFACTS / "payload", contract.files)
        runtime.check_bundle(contract, bundle)
        self.assertEqual(len(contract.inventory["files"]), 6366)
        self.assertEqual(len(contract.inventory["runtime_packages"]), 105)
        self.assertEqual(sum(p.startswith("runtime/") for p in contract.inventory["files"]), 6363)
        self.assertFalse(any("runtime/" + p in contract.files for p in runtime.SUPPORT))
        locked = json.loads((REAL_ARTIFACTS / "payload/runtime/node_modules/.package-lock.json").read_text())
        self.assertEqual(set(locked["packages"]), set(contract.inventory["runtime_packages"]))
        for name, expected in contract.inventory["runtime_packages"].items():
            self.assertEqual({key: locked["packages"][name].get(key)
                              for key in ("version", "resolved", "integrity")}, expected)
        with tempfile.TemporaryDirectory(prefix="real-install-", dir=REAL_ARTIFACTS.parent) as temp:
            root = Path(temp) / "owned"
            launcher = runtime.install(contract, bundle, root)
            self.assertEqual(launcher, runtime.install(contract, bundle, root))
            relocated = Path(temp) / "relocated with spaces"
            root.rename(relocated)
            runtime.verify_install(contract, relocated / contract.identity)
            runtime.remove(contract, relocated, contract.identity, True)
            self.assertFalse((relocated / contract.identity).exists())

    def test_complete_real_input_rejects_changed_support_envelope_and_extra_file(self):
        self.assertIsNotNone(CANONICAL_FIXTURE)
        contract = runtime.configuration()
        with tempfile.TemporaryDirectory(prefix="real-input-", dir=REAL_ARTIFACTS.parent) as temp:
            root = Path(temp)
            fixture = root / "fixture"
            shutil.copytree(CANONICAL_FIXTURE, fixture, symlinks=True)
            cases = [("tests/mcp-peer.mjs", False), ("FIXTURE-MANIFEST.json", False),
                     ("unlisted.json", True)]
            for name, extra in cases:
                with self.subTest(name=name):
                    path = fixture / name
                    original = None if extra else path.read_bytes()
                    path.write_bytes(b"unexpected" if extra else b"X" + original[1:])
                    try:
                        with self.assertRaisesRegex(runtime.InvalidRuntime,
                                                    "(SHA256 mismatch|extra payload)"):
                            # Nonexistent package arguments prove refusal occurs
                            # before acquiring/copying any production payload.
                            runtime.stage_payload(contract, fixture, root / "absent-node",
                                                  root / "absent-license", root / "absent-pack",
                                                  root / "not-created")
                        self.assertFalse((root / "not-created").exists())
                    finally:
                        if extra:
                            path.unlink()
                        else:
                            path.write_bytes(original)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--production-artifacts", type=Path)
    parser.add_argument("--canonical-fixture", type=Path)
    parser.add_argument("--case", action="append")
    args, rest = parser.parse_known_args()
    REAL_ARTIFACTS = args.production_artifacts
    CANONICAL_FIXTURE = args.canonical_fixture
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(
        ProductionInventoryTests if REAL_ARTIFACTS else OfflineTests)
    if args.case:
        suite = unittest.TestSuite(OfflineTests(name) for name in args.case)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
