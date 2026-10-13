#!/usr/bin/env python3
"""Offline regression coverage for optional Intel release publication."""
import hashlib
import importlib.util
import json
import os
import io
import shutil
import tarfile
from pathlib import Path
import subprocess
import tempfile
import unittest

import yaml

ROOT = Path(__file__).resolve().parents[1]
INTEL = 'x86_64-apple-darwin'
REQUIRED = ('aarch64-apple-darwin', 'aarch64-unknown-linux-musl',
            'x86_64-unknown-linux-musl', 'x86_64-pc-windows-msvc')


def module(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / 'scripts' / f'{name}.py')
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


def archive(root, target, valid=True):
    suffix = 'zip' if 'windows' in target else 'tar.xz'
    path = root / f'intentd-{target}.{suffix}'
    path.write_bytes(b'packaged binary')
    digest = hashlib.sha256(path.read_bytes()).hexdigest() if valid else '0' * 64
    path.with_name(path.name + '.sha256').write_text(f'{digest} *{path.name}\n')
    return path


class Publication(unittest.TestCase):
    def setUp(self):
        self.policy = module('optional-intel-release')
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        for target in REQUIRED:
            archive(self.root, target)

    def test_intel_success_and_absence(self):
        self.assertEqual(set(self.policy.validated_targets(self.root)), set(REQUIRED))
        archive(self.root, INTEL)
        self.assertEqual(set(self.policy.validated_targets(self.root)), {*REQUIRED, INTEL})

    def test_optional_partial_or_corrupt_artifacts_are_removed(self):
        for failure in ('archive', 'checksum', 'corrupt', 'encoding', 'wrong-name'):
            with self.subTest(failure=failure):
                path = archive(self.root, INTEL, valid=failure != 'corrupt')
                if failure == 'archive':
                    path.unlink()
                elif failure == 'checksum':
                    path.with_name(path.name + '.sha256').unlink()
                elif failure == 'encoding':
                    path.with_name(path.name + '.sha256').write_bytes(b'\xff\xfe')
                elif failure == 'wrong-name':
                    path.with_name(path.name + '.sha256').write_text(hashlib.sha256(path.read_bytes()).hexdigest() + ' *wrong.tar.xz')
                self.assertNotIn(INTEL, self.policy.validated_targets(self.root))
                self.assertFalse(list(self.root.glob(f'*{INTEL}*')))

    def test_omission_removes_stale_dist_metadata_and_download_links(self):
        name = f'intentd-{INTEL}.tar.xz'
        manifest = {'artifacts': {name: {}, name + '.sha256': {}, 'sha256.sum': {}},
                    'releases': [{'artifacts': [name, name + '.sha256', 'sha256.sum']}],
                    'upload_files': ['/tmp/' + name],
                    'announcement_github_body': f'Notes\n| [{name}](https://example/{name}) | Intel macOS |\n'}
        path = self.root / 'plan-dist-manifest.json'
        path.write_text(json.dumps(manifest))
        self.policy.validated_targets(self.root)
        result = json.loads(path.read_text())
        self.assertNotIn(INTEL, path.read_text())
        self.assertEqual(result['artifacts'], {'sha256.sum': {}})
        self.assertEqual(result['announcement_github_body'], 'Notes\n')

    def test_each_required_target_still_blocks(self):
        for target in REQUIRED:
            with self.subTest(target=target):
                path = archive(self.root, target, valid=False)
                with self.assertRaises(ValueError):
                    self.policy.validated_targets(self.root)
                archive(self.root, target)
                path.unlink()
                with self.assertRaises(ValueError):
                    self.policy.validated_targets(self.root)
                archive(self.root, target)

    def test_nonrelease_plan_has_no_intel_on_any_event(self):
        matrix = {'include': [{'targets': [target], 'dist_args': f'--target={target}'}
                              for target in (*REQUIRED, INTEL)]}
        for event, ref, allowed in (
            ('push', 'refs/tags/v1.2.3', True),
            ('push', 'refs/tags/v1.2.3-alpha.1', True),
            ('pull_request', 'refs/pull/1/merge', False),
            ('merge_group', 'refs/heads/gh-readonly-queue/main/pr-1', False),
            ('push', 'refs/heads/main', False),
            ('schedule', 'refs/heads/main', False),
            ('workflow_dispatch', 'refs/heads/main', False),
            ('workflow_dispatch', 'refs/tags/v1.2.3', False),
            ('push', 'refs/tags/sitter-v1.2.3', False),
            ('push', 'refs/tags/vnot-a-release', False),
        ):
            with self.subTest(event=event, ref=ref):
                result = self.policy.release_matrix(matrix, event, ref)
                targets = {t for row in result['include'] for t in row['targets']}
                self.assertEqual(targets, {*REQUIRED, INTEL} if allowed else set(REQUIRED))
                for row in result['include']:
                    self.assertEqual(row['optional'], row['targets'] == [INTEL])

    def test_combined_optional_required_matrix_is_rejected(self):
        with self.assertRaises(ValueError):
            self.policy.release_matrix({'include': [{'targets': [INTEL, REQUIRED[0]]}]},
                                       'push', 'refs/tags/v1.2.3')


class Consumers(unittest.TestCase):
    def test_channel_manifests_omit_intel_for_every_channel(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            assets = root / 'assets'
            assets.mkdir()
            for target in REQUIRED:
                archive(assets, target)
            stub = root / 'gh'
            stub.write_text("""#!/usr/bin/env -S python3 -I
import json, os, pathlib, shutil, sys
args = sys.argv[1:]
root = pathlib.Path(os.environ['TEST_ASSETS'])
if args[:2] == ['release', 'view']:
    print(json.dumps({'publishedAt': '2026-10-13T00:00:00Z', 'body': 'Release notes', 'isPrerelease': True, 'assets': [{'name': p.name} for p in root.iterdir()]}))
elif args[:2] == ['release', 'download']:
    shutil.copy(root / args[args.index('--pattern')+1], args[args.index('--dir')+1])
elif args[0] == 'api':
    print('HTTP/2.0 200 OK\\n\\n' + json.dumps({'id': 42, 'tag_name': 'v1.2.3'}))
elif args[:2] == ['release', 'edit']:
    pass
elif args[:2] == ['release', 'upload']:
    for value in args[3:args.index('--repo')]:
        shutil.copy(value, os.environ['TEST_MIRROR'])
else:
    raise SystemExit(99)
""")
            stub.chmod(0o755)
            mirror = root / 'mirror'
            mirror.mkdir()
            env = {**os.environ, 'PATH': str(root) + os.pathsep + os.environ['PATH'],
                   'TEST_ASSETS': str(assets), 'GITHUB_REPOSITORY': 'intent-hq/intentd',
                   'ASSET_REPO': 'intent-hq/intentd-releases', 'DEST_REPO': 'intent-hq/intentd-releases',
                   'DEST_GH_TOKEN': 'test-placeholder', 'TEST_MIRROR': str(mirror)}
            subprocess.run(['bash', str(ROOT / 'scripts/mirror-release-assets.sh'), 'v1.2.3'],
                           env=env, check=True, capture_output=True)
            self.assertEqual({p.name for p in mirror.iterdir()}, {p.name for p in assets.iterdir()})
            self.assertFalse(list(mirror.glob(f'*{INTEL}*')))
            for channel in ('alpha', 'beta', 'stable'):
                out = root / (channel + '.json')
                subprocess.run(['bash', str(ROOT / 'scripts/make-channel-manifest.sh'),
                                'v1.2.3', channel, str(out)], env=env, check=True, capture_output=True)
                manifest = json.loads(out.read_text())
                self.assertEqual(set(manifest['platforms']), set(REQUIRED))
                self.assertTrue(all('intentd-releases' in p['url'] for p in manifest['platforms'].values()))

    def test_fixed_sitter_retains_pair_and_installer_resolves_exact_intel(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            artifacts = root / 'artifacts'
            artifacts.mkdir()
            for target in REQUIRED:
                archive(artifacts, target)
            releases = root / 'releases'
            latest = releases / 'sitter-latest'
            latest.mkdir(parents=True)
            name = f'intentd-{INTEL}.tar.xz'
            old = latest / name
            payload = b'#!/bin/sh\necho "intentd-sitter 1.2.2"\n'
            with tarfile.open(old, 'w:xz') as tar:
                info = tarfile.TarInfo(f'intentd-{INTEL}/intentd')
                info.mode, info.size = 0o755, len(payload)
                tar.addfile(info, io.BytesIO(payload))
            checksum = old.with_name(name + '.sha256')
            checksum.write_text(hashlib.sha256(old.read_bytes()).hexdigest() + ' *' + name + '\n')
            old_pair = old.read_bytes(), checksum.read_bytes()
            scripts = root / 'scripts'
            scripts.mkdir()
            for name in ('install.sh', 'install.ps1', 'refresh-release-published-at.sh'):
                shutil.copy(ROOT / 'scripts' / name, scripts / name)
            bin_dir = root / 'bin'
            bin_dir.mkdir()
            stub = bin_dir / 'gh'
            stub.write_text("""#!/usr/bin/env -S python3 -I
import json, os, pathlib, shutil, sys
args = sys.argv[1:]
root = pathlib.Path(os.environ['TEST_RELEASES'])
if args[:2] == ['release', 'view'] or args[0] == 'api':
    print(json.dumps({'id':42, 'draft':False, 'prerelease':False}))
elif args[:2] == ['release', 'upload']:
    dest = root / args[2]
    dest.mkdir(exist_ok=True)
    for value in args[3:args.index('--repo')]:
        shutil.copy(value, dest)
else:
    raise SystemExit(99)
""")
            stub.chmod(0o755)
            curl = bin_dir / 'curl'
            curl.write_text("""#!/usr/bin/env -S python3 -I
import os, pathlib, shutil, sys
args = sys.argv[1:]
url = args[-1]
assert '/releases/download/sitter-latest/' in url
shutil.copy(pathlib.Path(os.environ['TEST_RELEASES']) / 'sitter-latest' / url.rsplit('/',1)[1], args[args.index('-o')+1])
""")
            curl.chmod(0o755)
            uname = bin_dir / 'uname'
            uname.write_text('#!/bin/sh\ncase "$1" in -s) echo Darwin;; -m) echo x86_64;; *) exit 99;; esac\n')
            uname.chmod(0o755)
            install_dir = root / 'installed'
            env = {**os.environ, 'PATH': os.pathsep.join((str(bin_dir), str(install_dir), os.environ['PATH'])),
                   'TEST_RELEASES': str(releases), 'GITHUB_REPOSITORY': 'intent-hq/intentd',
                   'TAG': 'sitter-v1.2.3', 'VERSION': '1.2.3', 'PRERELEASE': 'false',
                   'RELEASE_COMMIT': 'abc', 'INTENTD_INSTALL_DIR': str(install_dir),
                   'INTENTD_INSTALL_SERVICE': '0', 'INTENTD_CHANNEL': 'stable'}
            jobs = yaml.safe_load((ROOT / '.github/workflows/release-sitter.yml').read_text())['jobs']
            for label in ('Create GitHub Release on the tag', 'Refresh sitter-latest release'):
                step = next(s for s in jobs['publish']['steps'] if s.get('name') == label)
                subprocess.run(['bash', '-e', '-c', step['run']], cwd=root, env=env, check=True, capture_output=True)
            self.assertEqual((old.read_bytes(), checksum.read_bytes()), old_pair)
            self.assertFalse(list((releases / 'sitter-v1.2.3').glob(f'*{INTEL}*')))
            installer = ['sh', str(ROOT / 'scripts/install.sh'), '--no-service']
            result = subprocess.run(installer, env=env, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual((install_dir / 'intentd').read_bytes(), payload)
            self.assertIn('1.2.2', result.stdout)
            # A first-ever release with no Intel must not install another arch.
            old.unlink()
            checksum.unlink()
            (install_dir / 'intentd').unlink()
            result = subprocess.run(installer, env=env, capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertFalse((install_dir / 'intentd').exists())


class Homebrew(unittest.TestCase):
    def test_missing_intel_preserves_truthful_arch_scoped_previous_version(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            for target in (*REQUIRED, INTEL):
                archive(root, target)
            old = root / 'old.rb'
            new = root / 'new.rb'
            script = str(ROOT / 'scripts/render-sitter-homebrew-formula.sh')
            subprocess.run(['bash', script, '1.2.3', str(root), str(old)], check=True)
            (root / f'intentd-{INTEL}.tar.xz').unlink()
            subprocess.run(['bash', script, '1.2.4', str(root), str(new), str(old)], check=True)
            text = new.read_text()
            mac = text.split('  on_linux do')[0]
            self.assertIn('sitter-v1.2.3/intentd-x86_64-apple-darwin.tar.xz', mac)
            self.assertIn('      version "1.2.3"', mac)
            self.assertIn('sitter-v1.2.4/intentd-aarch64-apple-darwin.tar.xz', mac)
            self.assertNotIn('sitter-v1.2.4/intentd-x86_64-apple-darwin.tar.xz', text)
            # A second omission must keep the scoped version, not the global one.
            newer = root / 'newer.rb'
            subprocess.run(['bash', script, '1.2.5', str(root), str(newer), str(new)], check=True)
            self.assertIn('      version "1.2.3"', newer.read_text())

    def test_first_release_without_intel_has_no_dangling_mac_intel_url(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            for target in REQUIRED:
                archive(root, target)
            output = root / 'formula.rb'
            subprocess.run(['bash', str(ROOT / 'scripts/render-sitter-homebrew-formula.sh'),
                            '1.2.3', str(root), str(output)], check=True)
            self.assertNotIn(INTEL, output.read_text())
            self.assertIn('x86_64-unknown-linux-musl', output.read_text())


class Workflows(unittest.TestCase):
    def test_only_versioned_release_workflows_allow_intel(self):
        workflows = ROOT / '.github/workflows'
        for path in workflows.glob('*.yml'):
            workflow = yaml.safe_load(path.read_text())
            if path.stem in ('v-release', 'release-sitter'):
                events = workflow.get('on', workflow.get(True))
                self.assertEqual(set(events), {'pull_request', 'push'} if path.stem == 'v-release' else {'push'})
                self.assertEqual(set(events['push']), {'tags'})
                continue
            for job in workflow.get('jobs', {}).values():
                matrix = job.get('strategy', {}).get('matrix', {})
                if isinstance(matrix, dict):
                    for row in matrix.get('include', []):
                        self.assertNotEqual(row.get('target'), INTEL, path.name)
                        self.assertNotIn(INTEL, row.get('targets', []), path.name)
        for name, job_name in (('v-release', 'build-local-artifacts'), ('release-sitter', 'build')):
            job = yaml.safe_load((workflows / (name + '.yml')).read_text())['jobs'][job_name]
            upload = next(s for s in job['steps'] if s.get('uses', '').startswith('actions/upload-artifact@'))
            self.assertNotIn('if', upload)  # implicit success(), never upload failed builds

    def test_cache_matrix_excludes_intel(self):
        workflow = yaml.safe_load((ROOT / '.github/workflows/release-cache.yml').read_text())
        self.assertNotIn(INTEL, [row['target'] for row in workflow['jobs']['sitter']['strategy']['matrix']['include']])
        self.assertIn('optional-intel-release.py matrix', workflow['jobs']['plan']['steps'][-1]['run'])

    def test_optional_failure_does_not_relax_required_builds(self):
        dist = yaml.safe_load((ROOT / '.github/workflows/v-release.yml').read_text())['jobs']
        self.assertEqual(dist['build-local-artifacts']['continue-on-error'], '${{ matrix.optional }}')
        self.assertIn('optional-intel-release.py matrix', next(s['run'] for s in dist['plan']['steps'] if s.get('id') == 'plan'))
        for job in ('build-global-artifacts', 'host'):
            self.assertIn('optional-intel-release.py targets', '\n'.join(s.get('run', '') for s in dist[job]['steps']))
        sitter = yaml.safe_load((ROOT / '.github/workflows/release-sitter.yml').read_text())['jobs']
        self.assertEqual(sitter['build']['continue-on-error'], "${{ matrix.target == 'x86_64-apple-darwin' }}")
        self.assertIn('optional-intel-release.py targets', '\n'.join(s.get('run', '') for s in sitter['publish']['steps']))

    def test_customization_reapplies_and_is_idempotent(self):
        customize = module('configure-optional-intel-release')
        text = (ROOT / '.github/workflows/v-release.yml').read_text()
        self.assertEqual(customize.configure(text), text)
        self.assertEqual(customize.configure(customize.configure(text)), text)


if __name__ == '__main__':
    unittest.main()
