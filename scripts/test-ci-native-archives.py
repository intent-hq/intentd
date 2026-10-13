#!/usr/bin/env python3
"""Opt-in real Linux native-harness relocation proof (large scoped Rust build).

Run from a CLEAN committed component checkout. --target must be private to this
run: it is temporarily renamed to make compiled producer paths inaccessible.
The source is cloned into --work-dir, then moved away too. No user's checkout is
moved. On exit the target is restored for dependency-cache reuse. Archives, logs,
and a JSON receipt remain in --work-dir for independent verification.
"""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import time

spec = importlib.util.spec_from_file_location("artifacts", Path(__file__).with_name("ci-test-artifacts.py"))
artifacts = importlib.util.module_from_spec(spec)
spec.loader.exec_module(artifacts)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-dir", type=Path, required=True)
    parser.add_argument("--target", type=Path, required=True)
    args = parser.parse_args()
    if platform.system() != "Linux":
        parser.error("this local proof is Linux-only; native CI still owes macOS/Windows oracles")
    root, target = args.work_dir.resolve(), args.target.resolve()
    root.mkdir(parents=True, exist_ok=False)
    source = Path(__file__).resolve().parents[1]
    commit = artifacts.source_commit(source)
    producer, consumer = root / "producer", root / "consumer"
    for path in (producer, consumer):
        subprocess.run(["git", "clone", "--quiet", "--shared", str(source), str(path)], check=True)
        subprocess.run(["git", "checkout", "--quiet", "--detach", commit], cwd=path, check=True)
    host = artifacts.output(["rustc", "-vV"]).split("host: ")[1].splitlines()[0]
    facts = dict(target=host, host=host, os="Linux", arch=platform.machine(), abi=artifacts.output(["ldd", "--version"]), sdk="none", image=platform.platform())
    recipes = artifacts.native_recipes("linux", "pull_request", host)
    # Explicit additional scopes exercise every changed harness, not an all-tests
    # or all-features build. Services remains in its original standalone graph.
    recipes["daemon"]["command"] += ["--test", "e2e_doctor_codex"]
    recipes["services"] = artifacts.native_recipes("macos", "merge_group", host)["services"]
    requests = [artifacts.capture_request(producer, facts, recipe, config_sha256=artifacts.sha((source / ".cargo/config.toml").read_bytes())) for recipe in recipes.values()]
    (root / "requests.json").write_text(json.dumps(requests, indent=2))
    started = time.monotonic()
    records = artifacts.produce(requests, producer, target, root / "published")
    hidden_target = target.with_name(target.name + ".unavailable")
    if hidden_target.exists():
        raise ValueError("hidden target path already exists")
    producer.rename(root / "producer.unavailable")
    target.rename(hidden_target)
    try:
        forbidden = root / "forbidden"
        forbidden.mkdir()
        for name in ("cargo", "rustc"):
            script = forbidden / name
            script.write_text('#!/bin/sh\necho "$0 $*" >> "'+str(root / 'compiler-invocations')+'"\nexit 97\n')
            script.chmod(0o755)
        env = dict(os.environ, PATH=str(forbidden)+os.pathsep+os.environ["PATH"], RUSTC=str(forbidden / "rustc"))
        prepared = {}
        for name, request, record in zip(recipes, requests, records):
            prepared[name] = artifacts.prepare(root / "published" / record["key"], request, consumer, root / ("extracted-"+name), env=env)
        env["INTENTD_TEST_SITTER_BIN"] = str(artifacts.companion_path(prepared["sitter-production"], "intentd-sitter", producer_group=prepared["daemon"]["producer_group"]))
        runs = [
            ("sitter", "intentd-sitter::cli_help", []),
            ("sitter", "intentd-sitter::supervisor_e2e", ["--exact", "install_log_contract_scripts_and_supervisor_match"]),
            ("sitter", "intentd-sitter::supervisor_e2e", ["background_start"]),
            ("daemon", "intentd::e2e_doctor_codex", ["--exact", "doctor_help_discloses_opt_in_and_its_limits"]),
            ("daemon", "intentd::e2e_doctor_codex", ["--exact", "default_managed_reports_configuration_without_materializing_or_querying"]),
            ("daemon", "intentd::e2e_detached_lifecycle", ["lifecycle_capture_"]),
            ("daemon", "intentd::e2e_detached_lifecycle", ["--ignored", "--exact", "detached_sitter_lifecycle_over_wss", "--nocapture"]),
            ("services", "intent-services", ["--exact", "complete_ops::tests::quick_action_effort_reaches_provider_with_explicit_model_and_cold_catalog"]),
        ]
        outcomes = []
        for name, binary, selection in runs:
            before = time.monotonic()
            result = artifacts.run_libtest(prepared[name], binary, selection, env=env)
            outcomes.append(dict(variant=name, binary=binary, args=selection, exit_code=result, seconds=time.monotonic()-before))
            if result:
                raise RuntimeError(f"relocated native proof failed: {binary} {selection}")
        result = artifacts.run_nextest(prepared["sitter"], ["-E", "binary(cli_help)", "--no-tests", "fail"], env=env)
        if result:
            raise RuntimeError("relocated nextest proof failed")
        if (root / "compiler-invocations").exists():
            raise RuntimeError("consumer attempted compilation")
        receipt = dict(commit=commit, variants=records, tests=outcomes, compiler_calls=0, producer_paths_absent=not producer.exists() and not target.exists(), total_seconds=time.monotonic()-started)
        (root / "receipt.json").write_text(json.dumps(receipt, indent=2)+"\n")
        print(json.dumps(receipt, indent=2))
    finally:
        hidden_target.rename(target)


if __name__ == "__main__":
    main()
