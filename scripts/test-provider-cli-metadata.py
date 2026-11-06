#!/usr/bin/env python3
"""Offline extraction/synchronization tests for pinned adapter requirements."""
import json
import unittest
from provider_cli_metadata import package_snapshot, replace_package, read_pin, runtime_spec


CLAUDE = {"claude-code": {"cliPackage": "cli", "runtimePackage": "sdk",
                          "runtimeVersionField": "cliVersion", "package": {}}}
ADAPTER = {"name": "adapter", "version": "1.0.0", "dependencies": {"sdk": "0.3.280"}}
SDK = {"name": "sdk", "version": "0.3.280", "cliVersion": "2.1.280", "scripts": {"x": "y"}}


class PackageMetadataTests(unittest.TestCase):
    def test_snapshots_copy_declarations_not_engines_sdk_or_computed_minima(self):
        package = {"name":"adapter", "version":"2.0.0", "engines":{"node":">=22"},
                   "dependencies":{"cli":"^0.159.1", "sdk":"^9"},
                   "optionalDependencies":{"cli":"^0.159.2"},
                   "peerDependencies":{"peer-cli":">=1"}, "scripts":{"install":"do not run"}}
        result = package_snapshot(package)
        self.assertEqual(result["dependencies"], package["dependencies"])
        self.assertEqual(result["optionalDependencies"], package["optionalDependencies"])
        self.assertEqual(result["peerDependencies"], package["peerDependencies"])
        self.assertNotIn("engines", result)
        self.assertNotIn("scripts", result)
        self.assertNotIn("minimum", result)

    def test_pin_update_preserves_other_providers_and_copies_new_requirement(self):
        before = {"codex":{"cliPackage":"cli","package":{}}, "pi":{"package":{"version":"old"}}}
        updated = json.loads(replace_package(json.dumps(before), "codex", {
            "name":"adapter", "version":"3.0.0", "dependencies":{"cli":">=2.3.4"}}))
        self.assertEqual(updated["pi"], before["pi"])
        self.assertEqual(updated["codex"]["cliPackage"], "cli")
        self.assertEqual(updated["codex"]["package"]["dependencies"]["cli"], ">=2.3.4")
        self.assertEqual(updated["codex"]["package"]["version"], "3.0.0")

    def test_absent_declaration_remains_absent_and_bad_metadata_fails(self):
        self.assertEqual(package_snapshot({"name":"a","version":"1.0.0"})["dependencies"], {})
        for package in [None, {}, {"name":"a","version":"1", "dependencies":{"cli":False}}]:
            with self.assertRaises(ValueError): package_snapshot(package)

    def test_reads_literal_and_macro_pins_without_duplicated_versions(self):
        self.assertEqual(read_pin('pub const PIN: &str = "adapter@2.3.4";', "PIN"), "adapter@2.3.4")
        self.assertEqual(read_pin('macro_rules! claude_agent_acp_version { () => { "0.81.1" }; }', "CLAUDE_AGENT_ACP_NPX_PACKAGE"), "@agentclientprotocol/claude-agent-acp@0.81.1")
        with self.assertRaises(ValueError): read_pin("", "PIN")


    def test_runtime_snapshot_records_only_the_mapped_version_field(self):
        content = json.dumps(CLAUDE)
        self.assertEqual(runtime_spec(content, "claude-code", ADAPTER), ("sdk", "0.3.280"))
        updated = json.loads(replace_package(content, "claude-code", ADAPTER, SDK))
        self.assertEqual(updated["claude-code"]["runtime"],
                         {"name": "sdk", "version": "0.3.280", "cliVersion": "2.1.280"})
        # A snapshot is itself valid input, so automation can recompute it.
        again = replace_package(content, "claude-code", ADAPTER, updated["claude-code"]["runtime"])
        self.assertEqual(json.loads(again), updated)

    def test_runtime_mapping_rejects_missing_mismatched_or_inexact_runtime(self):
        content = json.dumps(CLAUDE)
        for runtime in (None, {**SDK, "version": "0.3.279"}, {**SDK, "name": "other"},
                        {**SDK, "cliVersion": "^2.1.0"}, {k: v for k, v in SDK.items() if k != "cliVersion"}):
            with self.assertRaises(ValueError, msg=runtime):
                replace_package(content, "claude-code", ADAPTER, runtime)
        for declared in ("^0.3.280", "latest", None):
            adapter = {**ADAPTER, "dependencies": {} if declared is None else {"sdk": declared}}
            with self.assertRaises(ValueError, msg=declared):
                runtime_spec(content, "claude-code", adapter)

    def test_unmapped_runtime_is_neither_fetched_nor_accepted(self):
        content = json.dumps({"codex": {"cliPackage": "cli", "package": {}}})
        self.assertIsNone(runtime_spec(content, "codex", ADAPTER))
        self.assertNotIn("runtime", json.loads(replace_package(content, "codex", ADAPTER))["codex"])
        with self.assertRaises(ValueError):
            replace_package(content, "codex", ADAPTER, SDK)


if __name__ == "__main__": unittest.main()
