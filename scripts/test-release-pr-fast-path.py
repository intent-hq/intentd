#!/usr/bin/env python3
"""Offline metadata classifier regressions using real git diffs."""
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / 'scripts/release-pr-fast-path.sh'


class Fixture(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(prefix='release-metadata-')
        self.addCleanup(tmp.cleanup)
        self.repo = Path(tmp.name)
        self.git('init', '-q')
        self.git('config', 'user.name', 'Fixture')
        self.git('config', 'user.email', 'fixture@example.invalid')
        self.write('Cargo.toml', '[workspace]\nmembers = ["crates/*"]\n')
        for name, version in [('intentd', '0.9.134'), ('intent-core', '0.9.134'), ('intentd-sitter', '0.1.21')]:
            manifest = f'[package]\nname = "{name}"\nversion = "{version}"\n'
            if name == 'intentd':
                manifest += '[dependencies]\nintent-core = { path = "../intent-core", version = "0.9.134" }\nintentd-sitter = { path = "../intentd-sitter", version = "0.1.21" }\nexternal = { version = "0.9.134" }\n'
            self.write(f'crates/{name}/Cargo.toml', manifest)
        self.write('Cargo.lock', 'version = 4\n' + ''.join(
            f'\n[[package]]\nname = "{name}"\nversion = "{version}"\n{source}'
            for name, version, source in [('intent-core', '0.9.134', ''), ('intentd', '0.9.134', 'dependencies = ["intent-core", "intentd-sitter", "external"]\n'), ('intentd-sitter', '0.1.21', ''), ('external', '0.9.134', 'source = "registry+https://example.invalid"\nchecksum = "abc"\n')]))
        self.write('CHANGELOG.md', '# Changelog\n')
        self.write('crates/intentd/src/main.rs', 'fn main() {}\n')
        self.base = self.commit()

    def git(self, *args):
        return subprocess.check_output(['git', *args], cwd=self.repo, text=True, stderr=subprocess.PIPE).strip()

    def write(self, path, text):
        dest = self.repo / path
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(text)

    def replace(self, path, old, new):
        self.write(path, (self.repo / path).read_text().replace(old, new))

    def commit(self):
        self.git('add', '.')
        self.git('commit', '-qm', 'fixture')
        return self.git('rev-parse', 'HEAD')

    def bump(self, sitter=False, daemon=True):
        for name in ('intentd', 'intent-core') if daemon else ():
            self.replace(f'crates/{name}/Cargo.toml', 'version = "0.9.134"', 'version = "0.9.135"')
            self.replace('Cargo.lock', f'name = "{name}"\nversion = "0.9.134"', f'name = "{name}"\nversion = "0.9.135"')
        # An external dependency sharing the old version must remain untouched.
        self.replace('crates/intentd/Cargo.toml', 'external = { version = "0.9.135" }', 'external = { version = "0.9.134" }')
        if sitter:
            for name in ('intentd', 'intentd-sitter'):
                self.replace(f'crates/{name}/Cargo.toml', 'version = "0.1.21"', 'version = "0.1.22"')
            self.replace('Cargo.lock', 'name = "intentd-sitter"\nversion = "0.1.21"', 'name = "intentd-sitter"\nversion = "0.1.22"')
        self.write('CHANGELOG.md', '# Changelog\nRelease notes\n')

    def classify(self, expected):
        head = self.commit()
        result = subprocess.run(['bash', str(SCRIPT), self.base, head], cwd=self.repo, capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), f'fast_path={str(expected).lower()}', result.stderr)


class MetadataTests(Fixture):
    def test_daemon_release(self):
        self.bump()
        self.classify(True)

    def test_independent_sitter_and_daemon_versions(self):
        self.bump(sitter=True)
        self.classify(True)

    def test_library_old_version_can_differ_from_daemon(self):
        self.replace('crates/intent-core/Cargo.toml', '0.9.134', '0.8.0')
        self.replace('crates/intentd/Cargo.toml', 'path = "../intent-core", version = "0.9.134"', 'path = "../intent-core", version = "0.8.0"')
        self.replace('Cargo.lock', 'name = "intent-core"\nversion = "0.9.134"', 'name = "intent-core"\nversion = "0.8.0"')
        self.base = self.commit()
        self.bump()
        for path in ('crates/intent-core/Cargo.toml', 'crates/intentd/Cargo.toml', 'Cargo.lock'):
            self.replace(path, '0.8.0', '0.9.135')
        self.classify(True)

    def test_sitter_only_release(self):
        self.bump(sitter=True, daemon=False)
        self.classify(True)

    def test_external_manifest_version_even_when_matching_daemon_is_rejected(self):
        self.bump()
        self.replace('crates/intentd/Cargo.toml', 'external = { version = "0.9.134" }', 'external = { version = "0.9.135" }')
        self.classify(False)

    def test_wrong_internal_dependency_version_rejected(self):
        self.bump()
        self.replace('crates/intentd/Cargo.toml', 'path = "../intent-core", version = "0.9.135"', 'path = "../intent-core", version = "0.9.136"')
        self.classify(False)

    def test_code_change_rejected(self):
        self.bump()
        self.write('crates/intentd/src/main.rs', 'fn main() { panic!(); }\n')
        self.classify(False)

    def test_dependency_features_rejected(self):
        self.bump()
        self.replace('crates/intentd/Cargo.toml', 'path = "../intent-core"', 'features = ["extra"], path = "../intent-core"')
        self.classify(False)

    def test_external_lock_version_rejected(self):
        self.bump()
        self.replace('Cargo.lock', 'name = "external"\nversion = "0.9.134"', 'name = "external"\nversion = "0.9.135"')
        self.classify(False)

    def test_lock_checksum_rejected(self):
        self.bump()
        self.replace('Cargo.lock', 'checksum = "abc"', 'checksum = "def"')
        self.classify(False)

    def test_added_file_rejected(self):
        self.bump()
        self.write('crates/new/Cargo.toml', '[package]\nname = "new"\nversion = "0.9.135"\n')
        self.classify(False)

    def test_mode_change_rejected(self):
        self.bump()
        (self.repo / 'CHANGELOG.md').chmod(0o755)
        self.classify(False)

    def test_deleted_file_rejected(self):
        self.bump()
        (self.repo / 'CHANGELOG.md').unlink()
        self.classify(False)

    def test_registry_crate_named_like_workspace_member_rejected(self):
        extra = '\n[[package]]\nname = "intent-core"\nversion = "0.9.134"\nsource = "registry+https://example.invalid"\nchecksum = "def"\n'
        self.write('Cargo.lock', (self.repo / 'Cargo.lock').read_text() + extra)
        self.base = self.commit()
        self.bump()
        self.classify(False)

    def test_renamed_internal_dependency_and_target_table(self):
        self.replace('crates/intentd/Cargo.toml', '[dependencies]\nintent-core =', '[target.\'cfg(unix)\'.build-dependencies]\ncore-alias =')
        self.replace('crates/intentd/Cargo.toml', 'path = "../intent-core"', 'package = "intent-core", path = "../intent-core"')
        self.base = self.commit()
        self.bump(sitter=True)
        self.classify(True)

    def test_malformed_manifest_rejected(self):
        self.bump()
        self.replace('crates/intentd/Cargo.toml', '[dependencies]', '[dependencies')
        self.classify(False)

    def test_lock_dependency_change_rejected(self):
        self.bump()
        self.replace('Cargo.lock', '"intent-core", "intentd-sitter", "external"', '"intent-core", "external"')
        self.classify(False)

    def test_version_qualified_lock_references(self):
        self.replace('Cargo.lock', '"intent-core",', '"intent-core 0.9.134",')
        self.base = self.commit()
        self.bump()
        self.replace('Cargo.lock', '"intent-core 0.9.134",', '"intent-core 0.9.135",')
        self.classify(True)

    def test_notes_only_rejected(self):
        self.write('CHANGELOG.md', 'Notes\n')
        self.classify(False)


if __name__ == '__main__':
    unittest.main()
