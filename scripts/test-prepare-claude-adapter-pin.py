#!/usr/bin/env python3
"""Offline controls for prepare-claude-adapter-pin.py; npm is always stubbed."""

import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch


SPEC = importlib.util.spec_from_file_location(
    "prepare_claude_adapter_pin", Path(__file__).with_name("prepare-claude-adapter-pin.py")
)
PREP = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PREP)

CONFIG = '''macro_rules! claude_agent_acp_version {
    () => {
        "%s"
    };
}
'''


def write_repo(root, pin="1.2.3", metadata_version="1.2.3", sdk="0.3.9"):
    (root / PREP.META.CONFIG).parent.mkdir(parents=True)
    (root / PREP.META.CONFIG).write_text(CONFIG % pin)
    (root / PREP.META.METADATA).parent.mkdir(parents=True, exist_ok=True)
    (root / PREP.META.METADATA).write_text(json.dumps({"claude-code": {"package": {
        "name": PREP.ADAPTER, "version": metadata_version,
        "dependencies": {PREP.SDK: sdk},
    }}}))


def fake_install(adapter="1.2.3", sdk="0.3.9", entry=True):
    def install(prefix, spec, env):
        assert env["NPM_CONFIG_IGNORE_SCRIPTS"] == "true"
        assert Path(env["HOME"]).is_relative_to(prefix.parent)
        install.specs.append(spec)
        for name, version in ((PREP.ADAPTER, adapter), (PREP.SDK, sdk)):
            package = prefix / "node_modules" / name
            package.mkdir(parents=True)
            (package / "package.json").write_text(json.dumps({"name": name, "version": version}))
        if entry:
            (prefix / "node_modules" / PREP.ADAPTER / "dist").mkdir()
            (prefix / "node_modules" / PREP.ADAPTER / "dist" / "index.js").write_text("")
    install.specs = []
    return install


class PrepareTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name) / "repo"
        self.cache = Path(self.tmp.name) / "cache"

    def tearDown(self):
        self.tmp.cleanup()

    def test_real_repository_pin_is_readable(self):
        adapter, sdk = PREP.expected_versions(PREP.ROOT)
        self.assertRegex(adapter, PREP.SEMVER)
        self.assertRegex(sdk, PREP.SEMVER)

    def test_installs_exact_pin_and_reuses_verified_cache(self):
        write_repo(self.root)
        install = fake_install()
        with patch.object(PREP, "run_npm", install):
            first = PREP.prepare(self.root, self.cache)
            second = PREP.prepare(self.root, self.cache)
        self.assertEqual(install.specs, [PREP.ADAPTER + "@1.2.3"])
        self.assertEqual(first, second)
        self.assertTrue(first.match("node_modules/@agentclientprotocol/claude-agent-acp"))

    def test_refuses_metadata_that_lags_the_pin(self):
        write_repo(self.root, pin="1.2.4")
        with self.assertRaisesRegex(PREP.Refusal, "describes .*@1.2.3 but the pin is 1.2.4"):
            PREP.expected_versions(self.root)

    def test_refuses_non_exact_sdk_dependency(self):
        write_repo(self.root, sdk="^0.3.9")
        with self.assertRaisesRegex(PREP.Refusal, "exact stable version"):
            PREP.expected_versions(self.root)

    def test_refuses_wrong_installed_adapter(self):
        write_repo(self.root)
        with patch.object(PREP, "run_npm", fake_install(adapter="1.2.2")):
            with self.assertRaisesRegex(PREP.Refusal, "installed .*@1.2.2, but the pin is 1.2.3"):
                PREP.prepare(self.root, self.cache)

    def test_refuses_wrong_installed_sdk(self):
        write_repo(self.root)
        with patch.object(PREP, "run_npm", fake_install(sdk="0.3.10")):
            with self.assertRaisesRegex(PREP.Refusal, "claude-agent-sdk@0.3.10, but .* expects 0.3.9"):
                PREP.prepare(self.root, self.cache)

    def test_refuses_missing_entry_point(self):
        write_repo(self.root)
        with patch.object(PREP, "run_npm", fake_install(entry=False)):
            with self.assertRaisesRegex(PREP.Refusal, "no dist/index.js"):
                PREP.prepare(self.root, self.cache)

    def test_stale_cache_is_reinstalled(self):
        write_repo(self.root)
        with patch.object(PREP, "run_npm", fake_install()):
            path = PREP.prepare(self.root, self.cache)
        (path / "package.json").write_text(json.dumps({"version": "9.9.9"}))
        install = fake_install()
        with patch.object(PREP, "run_npm", install):
            PREP.prepare(self.root, self.cache)
        self.assertEqual(len(install.specs), 1)

    def test_main_reports_refusal(self):
        write_repo(self.root, pin="1.2.4")
        self.assertEqual(PREP.main(["--cache-dir", str(self.cache), "--root", str(self.root)]), 1)


if __name__ == "__main__":
    unittest.main()
