#!/usr/bin/env python3
"""Local native archives: capture, sequential production, validation, execution.

No remote storage or locking here. Call produce ONCE per platform with all
required exact variants and a shared target/cache. Each key is published before
the next recipe or any test runs. The caller attests the provisioned platform,
C/linker versions and effective Cargo configuration (including parent config).
Archive checksums do not authenticate producers: storage must establish trust.
Consumers require the same clean source revision/fixtures and native runtime
prerequisites; they never invoke Cargo or compile. Direct libtest is preserved
for native proofs that require its capture/discovery semantics.
"""
import argparse
import hashlib
import importlib.util
import json
import os
import re
from pathlib import Path, PurePosixPath
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time

_spec = importlib.util.spec_from_file_location("identity", Path(__file__).with_name("ci-build-identity.py"))
identity = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(identity)


def sha(data):
    return hashlib.sha256(data).hexdigest()


def file_sha(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def reported_seconds(log, marker):
    matches = re.findall(marker + r"[^\n]* in ([0-9hms. ]+)\s*$", log, re.MULTILINE)
    if not matches:
        return None
    return sum(float(value) * {"h": 3600, "m": 60, "s": 1}[unit] for value, unit in re.findall(r"([0-9.]+)([hms])", matches[-1]))


def output(command, cwd=None, env=None):
    return subprocess.check_output(command, cwd=cwd, env=env, text=True).strip()


def native_recipes(platform, event, target):
    if platform not in ("linux", "macos", "windows") or event not in ("pull_request", "merge_group"):
        raise ValueError("unsupported native platform/event")
    common = ["--locked", "--target", target, "--target-dir", "{target}"]
    def tests(package, selection, companions):
        return {"kind": "test-archive", "command": ["cargo", "nextest", "archive", *common, "-p", package, *selection, "--cargo-message-format", "json", "--archive-file", "{archive}"], "companions": companions}
    daemon = ["--test", "e2e_detached_lifecycle"]
    if event == "merge_group" and platform == "macos":
        daemon += ["--test", "e2e_wss_tunnel", "--test", "e2e_doctor_codex"]
    if event == "merge_group" and platform == "windows":
        daemon += ["--test", "e2e_wss_setup_headless", "--test", "e2e_doctor_codex"]
    sitter = ["--lib", "--test", "cli_help"]
    if platform != "windows":
        sitter += ["--test", "supervisor_e2e"]
    recipes = {}
    if event == "merge_group" and platform != "linux":
        recipes["services"] = tests("intent-services", ["--lib"], [])
    if event == "merge_group" and platform == "macos":
        recipes["transport"] = tests("intent-transport", ["--lib"], [])
    if event == "merge_group" and platform == "windows":
        recipes["pty"] = tests("intent-pty", ["--lib"], [])
    recipes["daemon"] = tests("intentd", daemon, ["intentd"])
    recipes["sitter"] = tests("intentd-sitter", sitter, ["intentd-sitter"])
    recipes["sitter-production"] = {"kind": "binary", "command": ["cargo", "build", *common, "-p", "intentd-sitter", "--bin", "intentd-sitter", "--message-format=json"], "companions": ["intentd-sitter"]}
    return recipes


def build_environment():
    names = set(identity.REQUIRED_ENV)
    prefixes = ("CARGO_PROFILE_", "CARGO_TARGET_", "CARGO_BUILD_", "CC_", "CXX_", "AR_", "CFLAGS_", "CXXFLAGS_", "LDFLAGS_", "PKG_CONFIG", "OPENSSL", "LIBZ_", "ZLIB_", "VCPKG_", "CMAKE_", "RUSTC_", "DYLD_")
    names.update(name for name in os.environ if name.startswith(prefixes))
    names.update(("RUSTC", "RUSTDOC", "RUSTUP_TOOLCHAIN", "CARGO_HOME", "LD_PRELOAD", "LD_AUDIT", "LD_LIBRARY_PATH", "LIBRARY_PATH", "CPATH", "C_INCLUDE_PATH", "CPLUS_INCLUDE_PATH", "LIB", "INCLUDE", "DUMPBIN", "CL", "_CL_", "LINK", "_LINK_", "VCToolsVersion", "VCToolsInstallDir", "WindowsSdkDir", "WindowsSDKVersion", "UniversalCRTSdkDir", "UCRTVersion"))
    names.discard("CARGO_TARGET_DIR")
    return {name: os.environ.get(name) for name in sorted(names)}


def source_commit(source):
    commit = output(["git", "rev-parse", "HEAD"], source)
    if output(["git", "status", "--porcelain", "--untracked-files=normal"], source):
        raise ValueError("source: reusable artifacts require a clean checkout")
    return commit


def graph_args(command):
    args, i = [], 0
    while i < len(command):
        arg = command[i]
        if arg in ("-p", "--package", "--target", "--features", "-F", "--exclude", "--config"):
            args += command[i:i+2]
            i += 2
        else:
            if arg in ("--workspace", "--no-default-features", "--all-features", "--locked", "--offline", "--frozen"):
                args.append(arg)
            i += 1
    return args


def recipe_digest(recipe):
    # --source may be another checkout (including the small functional fixture).
    # Bind the archiver implementation too, not just Cargo's argv/source SHA.
    implementation = {"archive": file_sha(__file__), "identity": file_sha(identity.__file__)}
    if "coverage" in recipe:
        implementation["coverage"] = file_sha(Path(__file__).with_name("ci-coverage-artifacts.py"))
    return sha(json.dumps({"recipe": recipe, "implementation": implementation}, sort_keys=True, separators=(",", ":")).encode())


def validate_request(request):
    identity.fields(request, "identity recipe", "request")
    identity.validate(request["identity"])
    recipe, build = request["recipe"], request["identity"]["build"]
    identity.fields(recipe, "kind command companions" + (" coverage" if "coverage" in recipe else "") + (" compression" if "compression" in recipe else ""), "recipe")
    identity.string_list(recipe["command"], "recipe.command")
    identity.string_list(recipe["companions"], "recipe.companions")
    if build["commands"] != [recipe["command"]] or build["kind"] != recipe["kind"] or build["recipe_sha256"] != recipe_digest(recipe):
        raise ValueError("recipe differs from build identity")
    prefix = ["cargo", "nextest", "archive"] if recipe["kind"] == "test-archive" else ["cargo", "build"]
    if recipe["kind"] not in ("test-archive", "binary") or recipe["command"][:len(prefix)] != prefix:
        raise ValueError("unsupported producer recipe")
    if "{target}" not in recipe["command"] or (recipe["kind"] == "test-archive" and "{archive}" not in recipe["command"]):
        raise ValueError("recipe must use logical target/archive placeholders")
    coverage = recipe.get("coverage")
    if "compression" in recipe:
        if coverage is None or request["identity"]["platform"]["os"] != "linux" or recipe["kind"] != "test-archive":
            raise ValueError("compressed transport requires instrumented Linux test archive")
        coverage_module().validate_compression(recipe["compression"])
        if coverage["tools"]["cargo-llvm-cov"] != coverage_module().REPORT_VERSION:
            raise ValueError("unsupported compressed report version")
    if coverage is not None:
        identity.fields(coverage, "environment tools", "recipe.coverage")
        identity.string_map(coverage["environment"], ("CARGO_LLVM_COV", "LLVM_PROFILE_FILE"), "recipe.coverage.environment")
        identity.string_map(coverage["tools"], ("cargo-llvm-cov", "llvm-cov", "llvm-profdata"), "recipe.coverage.tools")
    instrumentation = {"mode": "llvm-cov", "flags": coverage_module().flags(coverage)} if coverage else {"mode": "none", "flags": []}
    if build["instrumentation"] != instrumentation or request["identity"]["tools"]["llvm_cov"] != (json.dumps(coverage["tools"], sort_keys=True) if coverage else "none"):
        raise ValueError("instrumentation differs from recipe")


def coverage_module():
    spec = importlib.util.spec_from_file_location("coverage_artifacts", Path(__file__).with_name("ci-coverage-artifacts.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def capture_request(source, platform, recipe, *, config_sha256, native_tools=None, instrumented=False, compressed=False):
    source = Path(source).resolve()
    commit = source_commit(source)
    if instrumented:
        recipe = dict(recipe, coverage=coverage_module().capture(source))
    if compressed:
        recipe = dict(recipe, compression=coverage_module().compression_recipe())
    command = recipe["command"]
    graph = output(["cargo", "tree", *graph_args(command), "--prefix", "none", "-e", "normal,build,dev", "-f", "{p} features=[{f}]"], source).replace(str(source), "{workspace}")
    files = output(["git", "ls-files"], source).splitlines()
    inputs = {name: file_sha(source / name) for name in files if name.endswith(("Cargo.toml", "Cargo.lock", ".cargo/config.toml", ".config/nextest.toml", "rust-toolchain.toml"))}
    for name in ("Cargo.lock", "Cargo.toml", "rust-toolchain.toml"):
        inputs.setdefault(name, sha(b""))
    features = []
    for i, arg in enumerate(command[:-1]):
        if arg in ("--features", "-F"):
            features += command[i+1].replace(",", " ").split()
    profile = "test" if recipe["kind"] == "test-archive" else "dev"
    if "--release" in command:
        profile = "release"
    for arg in ("--cargo-profile", "--profile"):
        if arg in command:
            profile = command[command.index(arg)+1]
    if native_tools is None:
        native_tools = {"cc": output(["cc", "--version"]), "linker": output(["ld", "--version"])}
    identity.fields(native_tools, "cc linker", "native_tools")
    document = {"schema": 1, "source": {"repository": "intent-hq/intentd", "commit": commit, "embedded_commit": commit, "dirty": False},
        "platform": platform, "tools": {"rustc": output(["rustc", "-vV"], source), "cargo": output(["cargo", "--version"], source),
        "nextest": output(["cargo-nextest", "nextest", "--version"], source), "llvm_cov": json.dumps(recipe["coverage"]["tools"], sort_keys=True) if instrumented else "none", **native_tools},
        "build": {"kind": recipe["kind"], "profile": profile, "commands": [command], "features": {"default_features": "--no-default-features" not in command, "all_features": "--all-features" in command, "requested": features},
        "resolved_features_sha256": sha(graph.encode()), "instrumentation": {"mode": "llvm-cov", "flags": coverage_module().flags(recipe["coverage"])} if instrumented else {"mode": "none", "flags": []}, "recipe_sha256": recipe_digest(recipe), "cargo_config_sha256": config_sha256, "inputs": inputs},
        "environment": build_environment()}
    request = {"identity": document, "recipe": recipe}
    validate_request(request)
    return request


def safe_join(root, relative):
    path = PurePosixPath(relative)
    if not relative or path.is_absolute() or ".." in path.parts or "\\" in relative or ":" in relative:
        raise ValueError(f"invalid artifact path: {relative}")
    result = Path(root).joinpath(*path.parts)
    if not result.resolve().is_relative_to(Path(root).resolve()):
        raise ValueError(f"artifact path escapes root: {relative}")
    return result


def check_source(source, expected):
    if source_commit(source) != expected["source"]["commit"]:
        raise ValueError("source commit differs from build identity")
    for name, digest in expected["build"]["inputs"].items():
        path = safe_join(source, name)
        if (file_sha(path) if path.is_file() else sha(b"")) != digest:
            raise ValueError(f"source input differs: {name}")


def nextest_command():
    executable = shutil.which("cargo-nextest")
    if not executable:
        raise ValueError("cargo-nextest required; cargo fallback forbidden")
    return [executable, "nextest"]


def extract_nextest(archive, source, destination, env=None):
    destination.mkdir(parents=True, exist_ok=False)
    subprocess.run([*nextest_command(), "list", "--archive-file", str(archive), "--extract-to", str(destination), "--workspace-remap", str(source), "--list-type", "binaries-only", "--message-format", "json"], cwd=source, env=env, stdout=subprocess.DEVNULL, check=True)
    return nextest_inventory(destination, source)


def nextest_inventory(destination, source):
    target = destination / "target"
    binaries = identity.read_request(target / "nextest/binaries-metadata.json")
    metadata = identity.read_request(target / "nextest/cargo-metadata.json")
    meta = binaries["rust-build-meta"]
    original = Path(meta["target-directory"])
    packages = {p["id"]: p for p in metadata["packages"]}
    tests = {}
    for key, binary in binaries["rust-binaries"].items():
        path = Path(binary["binary-path"]).relative_to(original).as_posix()
        package = packages[binary["package-id"]]
        cwd = Path(package["manifest_path"]).parent.relative_to(metadata["workspace_root"]).as_posix()
        if not safe_join(target, path).is_file() or not safe_join(source, cwd).is_dir():
            raise ValueError(f"missing archived test or runtime source: {key}")
        tests[key] = {"path": path, "cwd": cwd, "package": package["name"]}
    companions = {}
    for values in meta["non-test-binaries"].values():
        for binary in values:
            if binary["kind"] == "bin-exe":
                path, name = binary["path"], binary["name"]
                if not safe_join(target, path).is_file():
                    raise ValueError(f"missing companion: {name}")
                if name in companions and companions[name] != path:
                    raise ValueError("ambiguous companion name")
                companions[name] = path
    libraries = list(meta["linked-paths"])
    for directory in meta["base-output-directories"]:
        libraries += [directory, directory + "/deps"]
    libraries += [p.parent.relative_to(target).as_posix() for p in (target / "nextest/libdirs").rglob("*") if p.is_file()]
    for path in libraries:
        safe_join(target, path)
    if not tests:
        raise ValueError("empty test archive")
    return {"tests": tests, "companions": companions, "library_dirs": sorted(set(libraries))}


def inventory_files(directory):
    return {p.relative_to(directory).as_posix(): {"sha256": file_sha(p), "size": p.stat().st_size} for p in sorted(directory.rglob("*")) if p.is_file()}


def stage_binary_libraries(binary, destination, seen=None):
    """Copy non-system DLL/ELF dependencies; reject unsupported Mach-O paths.

    nextest owns libraries for test archives. This is only for a standalone
    production companion, which must not be rebuilt in its test feature graph.
    System libraries remain a prerequisite bound by the platform/ABI identity.
    """
    seen = set() if seen is None else seen
    if binary.resolve() in seen:
        return {"system": [], "bundled": []}
    seen.add(binary.resolve())
    system, copied = [], []
    if sys.platform == "win32":
        report = output([os.environ.get("DUMPBIN", "dumpbin"), "/DEPENDENTS", str(binary)])
        names = re.findall(r"^\s+([\w.-]+\.dll)\s*$", report, re.MULTILINE | re.IGNORECASE)
        system_root = Path(os.environ["SystemRoot"]) / "System32"
        search = [binary.parent, binary.parent / "deps", *map(Path, os.environ.get("PATH", "").split(os.pathsep))]
        for name in names:
            if (system_root / name).is_file() or name.lower().startswith(("api-ms-", "ext-ms-")):
                system.append(name)
                continue
            found = next((directory / name for directory in search if (directory / name).is_file()), None)
            if found is None:
                raise ValueError(f"missing runtime DLL: {name}")
            shutil.copy2(found, destination / name)
            copied.append(name)
            nested = stage_binary_libraries(found, destination, seen)
            system += nested["system"]
            copied += nested["bundled"]
    elif sys.platform == "darwin":
        report = output(["otool", "-L", str(binary)])
        for line in report.splitlines()[1:]:
            path = line.strip().split(" (", 1)[0]
            if path.startswith(("/usr/lib/", "/System/Library/")):
                system.append(path)
            else:
                # Rewriting install names would change the signed executable.
                # Fail explicitly instead of claiming this binary relocates.
                raise ValueError(f"unsupported standalone Mach-O dependency: {path}")
    else:
        # ldd also reports loader-injected host instrumentation. It is not a
        # DT_NEEDED dependency of this executable and must not be published.
        env = {name: value for name, value in os.environ.items() if name not in ("LD_PRELOAD", "LD_AUDIT")}
        result = subprocess.run(["ldd", str(binary)], text=True, capture_output=True, env=env)
        report = result.stdout + result.stderr
        if result.returncode and "statically linked" not in report and "not a dynamic executable" not in report:
            raise ValueError("cannot audit standalone binary libraries: " + report)
        # /etc/ld.so.preload can inject libraries even with a clean environment.
        # Follow only this object's DT_NEEDED edges, using ldd for resolution.
        needed = re.findall(r"\(NEEDED\).*Shared library: \[([^\]]+)\]", output(["readelf", "--dynamic", str(binary)]))
        resolved = dict(re.findall(r"^\s*(\S+) => (/\S+)", report, re.MULTILINE))
        for path in re.findall(r"^\s*(/\S+)\s+\(", report, re.MULTILINE):
            resolved.setdefault(Path(path).name, path)
        for name in needed:
            if "/" in name or name not in resolved:
                raise ValueError(f"missing or non-relocatable standalone library: {name}")
            path = resolved[name]
            library = Path(path)
            if path.startswith(("/lib/", "/lib64/", "/usr/lib/", "/usr/lib64/")):
                system.append(path)
            else:
                shutil.copy2(library, destination / library.name)
                copied.append(library.name)
                nested = stage_binary_libraries(library, destination, seen)
                system += nested["system"]
                copied += nested["bundled"]
    return {"system": sorted(set(system)), "bundled": sorted(set(copied))}


def verify_bundle(bundle, request):
    validate_request(request)
    manifest = identity.read_request(bundle / "manifest.json")
    identity.fields(manifest, "schema key identity recipe archive archive_sha256 archive_bytes files runtime timings" + (" report_archive" if "coverage" in request["recipe"] else ""), "manifest")
    if manifest["schema"] != 1:
        raise ValueError("unsupported artifact schema")
    identity.require_compatible(request["identity"], manifest["identity"])
    if manifest["key"] != identity.build_key(request["identity"]) or manifest["recipe"] != request["recipe"]:
        raise ValueError("manifest key/recipe mismatch")
    archive = safe_join(bundle, manifest["archive"])
    if archive.stat().st_size != manifest["archive_bytes"] or file_sha(archive) != manifest["archive_sha256"]:
        raise ValueError("archive size/digest mismatch")
    if "report_archive" in manifest:
        extra = manifest["report_archive"]
        identity.fields(extra, "name sha256 bytes", "report_archive")
        if extra["name"] != "report-objects.tar.gz":
            raise ValueError("invalid report archive name")
        path = bundle / extra["name"]
        if path.stat().st_size != extra["bytes"] or file_sha(path) != extra["sha256"]:
            raise ValueError("report archive size/digest mismatch")
    return manifest


def produce(requests, source, target, published):
    source, target, published = (Path(p).resolve() for p in (source, target, published))
    if any(path.is_relative_to(source) for path in (target, published)):
        raise ValueError("target/publication must be outside source checkout")
    for request in requests:
        validate_request(request)
    plan = identity.plan_variants([r["identity"] for r in requests], [])
    published.mkdir(parents=True, exist_ok=True)
    records, seen = [], set()
    for request in requests:
        expected, recipe = request["identity"], request["recipe"]
        key = identity.build_key(expected)
        if key in seen:
            continue
        seen.add(key)
        destination = published / key
        if destination.exists():
            manifest = verify_bundle(destination, request)
            records.append({"key": key, "reused": True, "archive_bytes": manifest["archive_bytes"]})
            continue
        check_source(source, expected)
        for tool, args in (("rustc", ["rustc", "-vV"]), ("cargo", ["cargo", "--version"]), ("nextest", ["cargo-nextest", "nextest", "--version"])):
            if output(args, source) != expected["tools"][tool]:
                raise ValueError(f"observed {tool} differs from identity")
        if build_environment() != expected["environment"]:
            raise ValueError("build environment differs from captured identity")
        with tempfile.TemporaryDirectory(prefix=".staging-", dir=published) as temporary:
            stage = Path(temporary)
            archive = stage / ("tests.tar.zst" if recipe["kind"] == "test-archive" else "binaries.tar.gz")
            variant_target = coverage_module().target_directory(target, recipe["coverage"]) if "coverage" in recipe else target
            environment = coverage_module().build_environment(recipe["coverage"], source, variant_target) if "coverage" in recipe else os.environ
            command = [str(archive) if x == "{archive}" else str(variant_target) if x == "{target}" else x for x in recipe["command"]]
            started = time.monotonic()
            with (stage / "build.jsonl").open("w") as log, (stage / "build.stderr.log").open("w") as diagnostics:
                with subprocess.Popen(command, cwd=source, env=dict(environment, CARGO_TERM_COLOR="never", CARGO_TERM_PROGRESS_WHEN="never"), stdout=log, stderr=subprocess.PIPE, text=True) as process:
                    for line in process.stderr:
                        diagnostics.write(line)
                        print(line, end="", file=sys.stderr, flush=True)
                    if process.wait():
                        log.flush()
                        diagnostics.flush()
                        failure = published / "failures" / key
                        failure.mkdir(parents=True, exist_ok=True)
                        for name in ("build.jsonl", "build.stderr.log"):
                            shutil.copy2(stage / name, failure / name)
                        # Cargo's JSON mode writes rendered compiler errors to
                        # stdout, while nextest's stderr only reports a summary.
                        for entry in (stage / "build.jsonl").read_text().splitlines():
                            try:
                                item = json.loads(entry)
                            except json.JSONDecodeError:
                                continue
                            if item.get("reason") == "compiler-message":
                                rendered = item.get("message", {}).get("rendered")
                                if rendered:
                                    print(rendered, file=sys.stderr, flush=True)
                        raise subprocess.CalledProcessError(process.returncode, command)
            built = time.monotonic()
            check_source(source, expected)
            diagnostics = (stage / "build.stderr.log").read_text()
            compile_seconds = reported_seconds(diagnostics, "Finished")
            archive_seconds = reported_seconds(diagnostics, "Archived")
            extraction = stage / "validation"
            if recipe["kind"] == "test-archive":
                runtime = extract_nextest(archive, source, extraction)
            else:
                extraction.mkdir()
                runtime = {"tests": {}, "companions": {}, "library_dirs": ["bin"], "dependencies": {}}
                for line in (stage / "build.jsonl").read_text().splitlines():
                    if not line.startswith("{"):
                        continue
                    item = json.loads(line)
                    if item.get("reason") == "compiler-artifact" and item.get("executable") and not item.get("profile", {}).get("test"):
                        name = item["target"]["name"]
                        if name in recipe["companions"]:
                            original = Path(item["executable"])
                            relative = "bin/" + original.name
                            dest = safe_join(extraction / "target", relative)
                            dest.parent.mkdir(parents=True, exist_ok=True)
                            shutil.copy2(original, dest)
                            runtime["companions"][name] = relative
                            runtime["dependencies"][name] = stage_binary_libraries(original, dest.parent)
                if set(recipe["companions"]) - runtime["companions"].keys():
                    raise ValueError("missing built companion")
                archive_started = time.monotonic()
                with tarfile.open(archive, "w:gz") as tar:
                    tar.add(extraction / "target", arcname="target")
                archive_seconds = time.monotonic()-archive_started
            if set(recipe["companions"]) - runtime["companions"].keys():
                raise ValueError("archive missing required companion binaries")
            report_archive = None
            if "coverage" in recipe:
                coverage_module().stage_report_objects(stage, variant_target, extraction)
                extra = stage / "report-objects.tar.gz"
                report_archive = {"name": extra.name, "sha256": file_sha(extra), "bytes": extra.stat().st_size}
            files = inventory_files(extraction)
            shutil.rmtree(extraction)
            compression_timings = coverage_module().compress_archive(archive, recipe["compression"]) if "compression" in recipe else {}
            manifest = {"schema": 1, "key": key, "identity": expected, "recipe": recipe,
                "archive": archive.name, "archive_sha256": file_sha(archive), "archive_bytes": archive.stat().st_size,
                "files": files, "runtime": runtime, "timings": {"build_command_seconds": built-started, "compile_seconds": compile_seconds, "archive_seconds": archive_seconds, "validation_and_staging_seconds": time.monotonic()-built-compression_timings.get("compression_seconds", 0), **compression_timings}}
            if report_archive is not None:
                manifest["report_archive"] = report_archive
            (stage / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True)+"\n")
            verify_bundle(stage, request)
            stage.rename(destination)
            records.append({"key": key, "reused": False, "archive_bytes": manifest["archive_bytes"], **manifest["timings"]})
    (published / "index.json").write_text(json.dumps({"producer_group": plan["producer_group"], "variants": records}, indent=2)+"\n")
    return records


def prepare(bundle, request, source, destination, *, env=None):
    bundle, source, destination = (Path(p).resolve() for p in (bundle, source, destination))
    manifest = verify_bundle(bundle, request)
    check_source(source, manifest["identity"])
    if output([*nextest_command(), "--version"], source, env) != manifest["identity"]["tools"]["nextest"]:
        raise ValueError("consumer nextest version differs from identity")
    started = time.monotonic()
    archive = bundle / manifest["archive"]
    if request["recipe"]["kind"] == "test-archive":
        runtime = extract_nextest(archive, source, destination, env)
    else:
        destination.mkdir(parents=True, exist_ok=False)
        with tarfile.open(archive, "r:gz") as tar:
            for member in tar.getmembers():
                safe_join(destination, member.name)
                if not (member.isfile() or member.isdir()):
                    raise ValueError("unsupported archive member")
            tar.extractall(destination, filter="data")
        runtime = manifest["runtime"]
    if "report_archive" in manifest:
        with tarfile.open(bundle / manifest["report_archive"]["name"], "r:gz") as tar:
            for member in tar.getmembers():
                path = safe_join(destination, member.name)
                if not member.isfile() or not member.name.startswith("target/") or path.suffix != ".so" or path.exists():
                    raise ValueError("invalid or colliding coverage report object")
            tar.extractall(destination, filter="data")
    if inventory_files(destination) != manifest["files"]:
        raise ValueError("extracted file inventory/digest mismatch")
    if runtime != manifest["runtime"]:
        raise ValueError("archive runtime inventory mismatch")
    if set(request["recipe"]["companions"]) - runtime["companions"].keys():
        raise ValueError("archive missing required companions")
    return {"source": str(source), "target": str(destination / "target"), "producer_group": identity.producer_group_key(manifest["identity"]), "runtime": runtime, "extract_seconds": time.monotonic()-started}


def companion_path(prepared, name, *, producer_group=None):
    if producer_group is not None and prepared["producer_group"] != producer_group:
        raise ValueError("companion belongs to a different source/platform producer group")
    relative = prepared["runtime"]["companions"].get(name)
    if relative is None:
        raise ValueError(f"missing companion: {name}")
    path = safe_join(prepared["target"], relative)
    if not path.is_file():
        raise ValueError(f"missing companion: {name}")
    return path


def runtime_env(prepared, env=None):
    target = Path(prepared["target"])
    result = dict(os.environ if env is None else env)
    for key in list(result):
        if key.startswith("NEXTEST_BIN_EXE_"):
            del result[key]
    for name, path in prepared["runtime"]["companions"].items():
        full = companion_path(prepared, name)
        result["NEXTEST_BIN_EXE_"+name] = str(full)
    paths = [str(safe_join(target, path)) for path in prepared["runtime"]["library_dirs"]]
    for variable in ("PATH", "DYLD_FALLBACK_LIBRARY_PATH", "LD_LIBRARY_PATH"):
        result[variable] = os.pathsep.join([*paths, result.get(variable, "")])
    return result


def run_nextest(prepared, args, *, env=None):
    target = Path(prepared["target"])
    allowed = {"-E", "--filterset", "--profile", "-P", "--run-ignored", "--test-threads", "--retries", "--no-tests", "--status-level", "--final-status-level", "--failure-output", "--success-output"}
    flags = {"--no-capture", "--no-fail-fast", "--fail-fast"}
    i = 0
    while i < len(args):
        if args[i] in allowed and i+1 < len(args):
            i += 2
        elif args[i] in flags:
            i += 1
        else:
            raise ValueError(f"unsupported consumer argument: {args[i]}")
    return subprocess.call([*nextest_command(), "run", "--cargo-metadata", str(target / "nextest/cargo-metadata.json"), "--binaries-metadata", str(target / "nextest/binaries-metadata.json"), "--workspace-remap", prepared["source"], "--target-dir-remap", str(target), "--show-progress", "none", *args], cwd=prepared["source"], env=runtime_env(prepared, env))


def run_libtest(prepared, binary_id, args, *, env=None):
    binary = prepared["runtime"]["tests"].get(binary_id)
    if binary is None:
        raise ValueError(f"missing exact test binary: {binary_id}")
    path = safe_join(prepared["target"], binary["path"])
    if not path.is_file():
        raise ValueError(f"missing archived binary: {binary_id}")
    cwd = safe_join(prepared["source"], binary["cwd"])
    environ = runtime_env(prepared, env)
    environ["CARGO_MANIFEST_DIR"] = str(cwd)
    result = subprocess.run([str(path), *args], cwd=cwd, env=environ, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    print(result.stdout, end="", flush=True)
    if result.returncode == 0 and "--list" not in args and not re.search(r"test result: ok\. [1-9][0-9]* passed;", result.stdout):
        print("archived libtest selected no passing tests", file=sys.stderr)
        return 1
    return result.returncode


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    capture = commands.add_parser("requests")
    capture.add_argument("--source", type=Path, default=Path.cwd())
    capture.add_argument("--platform", required=True, help="JSON with platform and native_tools provisioned facts")
    capture.add_argument("--native-platform", choices=("linux", "macos", "windows"), required=True)
    capture.add_argument("--event", choices=("pull_request", "merge_group"), required=True)
    capture.add_argument("--cargo-config-sha256", required=True)
    capture.add_argument("--output", required=True, type=Path)
    producer = commands.add_parser("produce")
    for name in ("requests", "source", "target", "published"):
        producer.add_argument("--"+name, type=Path, required=True)
    consumer = commands.add_parser("prepare")
    for name in ("bundle", "request", "source", "destination", "output"):
        consumer.add_argument("--"+name, type=Path, required=True)
    for name in ("run", "libtest"):
        run = commands.add_parser(name)
        run.add_argument("prepared", type=Path)
        if name == "libtest":
            run.add_argument("binary_id")
        run.add_argument("args", nargs=argparse.REMAINDER)
    companion = commands.add_parser("companion")
    companion.add_argument("prepared", type=Path)
    companion.add_argument("name")
    companion.add_argument("--producer-group")
    args = parser.parse_args()
    try:
        if args.command == "requests":
            facts = identity.read_request(args.platform)
            recipes = native_recipes(args.native_platform, args.event, facts["platform"]["target"])
            requests = [capture_request(args.source, facts["platform"], recipe, config_sha256=args.cargo_config_sha256, native_tools=facts["native_tools"]) for recipe in recipes.values()]
            args.output.write_text(json.dumps(requests, indent=2)+"\n")
        elif args.command == "produce":
            print(json.dumps(produce(identity.read_request(args.requests), args.source, args.target, args.published)))
        elif args.command == "prepare":
            result = prepare(args.bundle, identity.read_request(args.request), args.source, args.destination)
            args.output.write_text(json.dumps(result, indent=2)+"\n")
        elif args.command == "companion":
            print(companion_path(identity.read_request(args.prepared), args.name, producer_group=args.producer_group))
        else:
            selected = args.args[1:] if args.args[:1] == ["--"] else args.args
            prepared = identity.read_request(args.prepared)
            return run_libtest(prepared, args.binary_id, selected) if args.command == "libtest" else run_nextest(prepared, selected)
    except (ValueError, OSError, KeyError, subprocess.CalledProcessError) as error:
        print(str(error), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
