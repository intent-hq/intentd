#!/usr/bin/env python3
"""Offline metadata classifier regressions using real git diffs."""
import json
import os
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
        # No background maintenance: git >= 2.47 detaches it after commit and
        # fetch, and it races TemporaryDirectory.cleanup on .git/objects.
        for key, value in [('maintenance.auto', 'false'), ('gc.auto', '0'), ('fetch.writeCommitGraph', 'false')]:
            self.git('config', key, value)
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

    def test_version_bump_with_stale_lockfile_rejected(self):
        # Without the lockfile bump every `--locked` build fails; the PR CI
        # fast path skips `check`, so the classifier must refuse it.
        self.bump()
        self.replace('Cargo.lock', 'name = "intentd"\nversion = "0.9.135"', 'name = "intentd"\nversion = "0.9.134"')
        self.classify(False)
        # Lockfile left out of the diff entirely.
        self.git('checkout', self.base, '--', 'Cargo.lock')
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


class ExactParentTests(Fixture):
    """--exact-parent (main pushes): head must be a single commit on base."""

    def run_exact(self, base, head):
        result = subprocess.run(['bash', str(SCRIPT), '--exact-parent', base, head], cwd=self.repo,
                                capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        return result.stdout.strip()

    def test_release_commit_directly_on_the_previous_tip(self):
        self.bump()
        self.assertEqual(self.run_exact(self.base, self.commit()), 'fast_path=true')

    def test_non_ancestor_push_that_rolls_back_code_is_rejected(self):
        original = self.base
        self.write('crates/intentd/src/main.rs', 'fn main() { println!("fixed code"); }\n')
        before = self.commit()
        self.git('checkout', '-q', '--detach', original)
        self.bump()
        after = self.commit()
        self.assertEqual(self.run_exact(before, after), 'fast_path=false')

    def test_release_with_an_intermediate_commit_is_rejected(self):
        self.write('crates/intentd/src/main.rs', 'fn main() { println!("code"); }\n')
        self.commit()
        self.bump()
        self.assertEqual(self.run_exact(self.base, self.commit()), 'fast_path=false')

    def test_merge_commit_is_rejected(self):
        self.git('checkout', '-q', '-b', 'side')
        self.bump()
        side = self.commit()
        self.git('checkout', '-q', '-')
        self.git('merge', '-q', '--no-ff', '-m', 'merge', side)
        self.assertEqual(self.run_exact(self.base, self.git('rev-parse', 'HEAD')), 'fast_path=false')


class FixtureHygieneTests(Fixture):
    def test_git_never_spawns_background_maintenance(self):
        # git >= 2.47 detaches `maintenance run --auto` after commit/fetch; it
        # kept writing .git/objects while TemporaryDirectory.cleanup ran,
        # failing CI with "Directory not empty: 'objects'". Force every
        # maintenance task due via a global config so only the fixture's
        # repo-local settings can keep git from spawning it.
        tmp = tempfile.TemporaryDirectory(prefix='release-metadata-trace-')
        self.addCleanup(tmp.cleanup)
        scratch = Path(tmp.name)
        (scratch / 'gitconfig').write_text(
            '[gc]\n\tauto = 1\n'
            '[maintenance "geometric-repack"]\n\tauto = -1\n'
            '[maintenance "commit-graph"]\n\tauto = -1\n'
            '[fetch]\n\twriteCommitGraph = true\n')
        trace = scratch / 'trace.json'
        env = {**os.environ, 'GIT_CONFIG_GLOBAL': str(scratch / 'gitconfig'), 'GIT_TRACE2_EVENT': str(trace)}
        for key in [k for k in env if k.startswith('GIT_CONFIG_') and k != 'GIT_CONFIG_GLOBAL']:
            env.pop(key)
        self.write('CHANGELOG.md', '# Changelog\nMore\n')
        self.git('remote', 'add', 'origin', str(self.repo))
        for args in (['commit', '-qam', 'more'], ['fetch', '-q', '--no-tags', 'origin', 'HEAD'], ['gc', '--auto']):
            subprocess.run(['git', *args], cwd=self.repo, env=env, check=True, capture_output=True)
        spawned = [event['argv'] for event in map(json.loads, trace.read_text().splitlines())
                   if event.get('event') == 'child_start' and 'argv' in event]
        background = [argv for argv in spawned if {'maintenance', 'gc', 'commit-graph', 'repack'} & set(argv)]
        self.assertEqual(background, [])


if __name__ == '__main__':
    unittest.main()
