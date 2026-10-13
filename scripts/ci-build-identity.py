#!/usr/bin/env python3
"""Fail-closed identity for reusable CI builds, independent of artifact storage.

Usage: ci-build-identity.py key REQUEST.json
       ci-build-identity.py verify EXPECTED.json ACTUAL.json
       ci-build-identity.py group REQUEST.json
       ci-build-identity.py plan REQUESTS.json AVAILABLE_KEYS.json

The producer must collect this request AFTER provisioning and BEFORE compiling:
* source: actual clean checkout SHA, not a PR-head/event SHA substituted for it;
  INTENTD_BUILD_COMMIT must be unset or exactly that full SHA.
* platform: host/target triples, OS/arch, native ABI, SDK and immutable image or
  provisioned self-host environment revision (runner labels alone are unsafe).
* tools: full rustc -vV, Cargo/nextest/llvm-cov, C compiler and linker versions;
  use explicit 'none' for absent tools/SDKs, never an empty/unknown value. Record
  provisioned compiler versions consistently even for variants not using them.
* build: exact ordered producer argv, Cargo profile (not nextest run profile),
  requested features AND a digest of the resolved host/target feature graph,
  instrumentation flags, recipe digest, effective Cargo config digest (including
  parent/CARGO_HOME config and CLI overrides), and named input file digests.
* environment: effective build inputs, including native dependency settings and
  all CARGO_PROFILE_*, CARGO_TARGET_*, CC/CXX/AR/CFLAGS variants used by the build.
  Required common fields distinguish absent (null) from an empty string. Add
  further inputs as needed; their names and values participate in the key.

No machine probing or implicit defaults here: callers own completeness/accuracy
of the captured inputs and must control the build environment. Do not serialize
secrets; credentials are not build inputs. SHA equality is necessary, not proof
of artifact provenance or integrity. Storage must separately validate producer
trust, successful publication and an archive digest before executing anything.

Runtime test selection/results, job/run IDs, and output locations stay outside
the identity. Lists (especially argv and flags) preserve order conservatively.
Different scopes/features/profiles never receive automatic superset matching.
One producer group covers a commit/platform/compiler environment, independent of
the requested variant set. A group key is ONLY a serialization boundary, never
an artifact-reuse key. Ancillary nextest/llvm-cov versions and recipe/env/profile
differences stay in variant identities, so even overlapping requests serialize.
Run every missing variant sequentially in ONE platform producer step, sharing a
group-private Cargo target/cache (instrumentation may need its own subdirectory).
Stage each output before the next recipe can overwrite same-path binaries.

The pure planner accepts keys only from already validated, successfully published
artifacts. Repeat lookup/planning AFTER acquiring the producer group lock, then
publish each staged variant. Storage, locking, staging and execution are later
workflow integration responsibilities; this helper does not implement them.
Always rerun required tests. Bump the schema for incompatible contract changes.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re
import sys


REQUIRED_ENV = (
    "RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS", "RUSTDOCFLAGS",
    "CARGO_ENCODED_RUSTDOCFLAGS", "RUSTC_WRAPPER", "RUSTC_WORKSPACE_WRAPPER",
    "CARGO_INCREMENTAL", "CARGO_PROFILE_DEV_DEBUG", "CARGO_PROFILE_TEST_DEBUG",
    "CARGO_BUILD_TARGET", "CC", "CXX", "AR", "CFLAGS", "CXXFLAGS", "LDFLAGS",
    "SDKROOT", "MACOSX_DEPLOYMENT_TARGET", "INTENTD_BUILD_COMMIT",
    "INTENTD_EMBEDDED_BUILD_COMMIT", "TRANSFER_SELECTION_BUILD_REVISION",
)


def fields(value, names, path):
    if not isinstance(value, dict) or set(value) != set(names.split()):
        raise ValueError(f"{path}: expected fields {names}")


def string(value, path):
    if not isinstance(value, str) or not value.strip() or "\x00" in value:
        raise ValueError(f"{path}: expected nonempty string")


def string_list(value, path):
    if not isinstance(value, list):
        raise ValueError(f"{path}: expected string list")
    for item in value:
        string(item, path)


def digest(value, path, lengths=(64,)):
    if not isinstance(value, str) or len(value) not in lengths or not re.fullmatch(r"[0-9a-f]+", value):
        raise ValueError(f"{path}: expected full lowercase hex digest")


def string_map(value, required, path, *, nullable=False):
    if not isinstance(value, dict) or not set(required).issubset(value):
        raise ValueError(f"{path}: missing required fields")
    for key, item in value.items():
        string(key, path)
        if nullable:
            if item is not None and (not isinstance(item, str) or "\x00" in item):
                raise ValueError(f"{path}: expected string or null values")
        else:
            string(item, path)


def validate(document):
    fields(document, "schema source platform tools build environment", "identity")
    if type(document["schema"]) is not int or document["schema"] != 1:
        raise ValueError("identity: unsupported schema")
    source = document["source"]
    fields(source, "repository commit embedded_commit dirty", "source")
    string(source["repository"], "source.repository")
    digest(source["commit"], "source.commit", (40, 64))
    if source["dirty"] is not False:
        raise ValueError("source: reusable builds require a clean checkout")
    if source["embedded_commit"] != source["commit"]:
        raise ValueError("source: embedded commit must equal checkout commit")
    platform = document["platform"]
    fields(platform, "target host os arch abi sdk image", "platform")
    string_map(platform, (), "platform")
    string_map(document["tools"], ("rustc", "cargo", "nextest", "llvm_cov", "cc", "linker"), "tools")
    build = document["build"]
    fields(build, "kind profile commands features resolved_features_sha256 instrumentation recipe_sha256 cargo_config_sha256 inputs", "build")
    if build["kind"] not in ("test-archive", "binary", "check", "clippy"):
        raise ValueError("build: unsupported kind")
    string(build["profile"], "build.profile")
    commands = build["commands"]
    if not isinstance(commands, list) or not commands:
        raise ValueError("build.commands: expected nonempty argv list")
    for command in commands:
        string_list(command, "build.commands")
        if not command:
            raise ValueError("build.commands: empty argv")
    features = build["features"]
    fields(features, "default_features all_features requested", "build.features")
    for name in ("default_features", "all_features"):
        if type(features[name]) is not bool:
            raise ValueError(f"build.features.{name}: expected boolean")
    string_list(features["requested"], "build.features.requested")
    instrumentation = build["instrumentation"]
    fields(instrumentation, "mode flags", "build.instrumentation")
    if instrumentation["mode"] not in ("none", "llvm-cov"):
        raise ValueError("build.instrumentation: unsupported mode")
    string_list(instrumentation["flags"], "build.instrumentation.flags")
    for name in ("resolved_features_sha256", "recipe_sha256", "cargo_config_sha256"):
        digest(build[name], f"build.{name}")
    inputs = build["inputs"]
    string_map(inputs, ("Cargo.lock", "Cargo.toml", "rust-toolchain.toml"), "build.inputs")
    for name, value in inputs.items():
        digest(value, f"build.inputs.{name}")
    env = document["environment"]
    string_map(env, REQUIRED_ENV, "environment", nullable=True)
    for name in ("INTENTD_BUILD_COMMIT", "INTENTD_EMBEDDED_BUILD_COMMIT"):
        if env[name] is not None and env[name] != source["commit"]:
            raise ValueError(f"environment.{name}: must be unset or equal checkout commit")


def hashed_key(prefix, document):
    canonical = json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return prefix + hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def build_key(document):
    validate(document)
    return hashed_key("intentd-build-v1-", document)


def producer_group_key(document):
    validate(document)
    # A broader lock is safe: exact variant keys still govern executable reuse.
    # Excluding demand/recipe inputs prevents A+B and B+C from racing to build B.
    group = {"schema": document["schema"], "source": document["source"],
             "platform": document["platform"],
             "compiler": {name: document["tools"][name] for name in ("rustc", "cargo", "cc", "linker")}}
    return hashed_key("intentd-producer-v1-", group)


def plan_variants(requests, available_keys):
    """Plan exact misses, not locks or cache I/O; available keys must be verified."""
    if not isinstance(requests, list) or not requests:
        raise ValueError("requests: expected nonempty variant list")
    if not isinstance(available_keys, list) or any(
        not isinstance(key, str) or not re.fullmatch(r"intentd-build-v1-[0-9a-f]{64}", key)
        for key in available_keys
    ):
        raise ValueError("available_keys: expected exact validated variant keys")
    groups = {producer_group_key(request) for request in requests}
    if len(groups) != 1:
        raise ValueError("requests: variants require different producer groups")
    requested = sorted({build_key(request) for request in requests})
    available = set(available_keys)
    return {"producer_group": groups.pop(), "requested_keys": requested,
            "missing_keys": [key for key in requested if key not in available]}


def require_compatible(expected, actual):
    if build_key(expected) != build_key(actual):
        changed = sorted(name for name in expected if expected[name] != actual[name])
        raise ValueError("incompatible build identity: " + ", ".join(changed))


def unique_fields(pairs):
    result = {}
    for name, value in pairs:
        if name in result:
            raise ValueError("duplicate JSON field")
        result[name] = value
    return result


def read_request(path):
    return json.loads(Path(path).read_text(encoding="utf-8"), object_pairs_hook=unique_fields)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("key").add_argument("request")
    commands.add_parser("group").add_argument("request")
    plan = commands.add_parser("plan")
    plan.add_argument("requests")
    plan.add_argument("available_keys")
    verify = commands.add_parser("verify")
    verify.add_argument("expected")
    verify.add_argument("actual")
    args = parser.parse_args()
    try:
        if args.command == "key":
            print(build_key(read_request(args.request)))
        elif args.command == "group":
            print(producer_group_key(read_request(args.request)))
        elif args.command == "plan":
            print(json.dumps(plan_variants(read_request(args.requests), read_request(args.available_keys)), sort_keys=True))
        else:
            require_compatible(read_request(args.expected), read_request(args.actual))
    except (ValueError, OSError) as error:
        print(str(error), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
