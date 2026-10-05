"""Credential-free Codex 0.160.0 native denial/reload regression fixture.

No model calls. All homes, configuration, skills and MCP executables are synthetic.
This deliberately asserts the late-source gap as well as effective suppression.
"""
import json
import os
from pathlib import Path
import selectors
import shutil
import subprocess
import sys
import tempfile
import time


class Rpc:
    def __init__(self, argv, cwd, env):
        self.child = subprocess.Popen(argv, cwd=cwd, env=env, stdin=subprocess.PIPE,
                                     stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.buffer = b''
        self.ident = 0
        self.selector = selectors.DefaultSelector()
        self.selector.register(self.child.stdout, selectors.EVENT_READ)

    def call(self, method, params):
        self.ident += 1
        self.child.stdin.write(json.dumps({'id': self.ident, 'method': method, 'params': params}).encode() + b'\n')
        self.child.stdin.flush()
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            while b'\n' in self.buffer:
                line, self.buffer = self.buffer.split(b'\n', 1)
                msg = json.loads(line)
                if msg.get('id') == self.ident:
                    assert 'error' not in msg, (method, msg.get('error'))
                    return msg['result']
            if not self.selector.select(max(0, deadline - time.monotonic())):
                break
            chunk = os.read(self.child.stdout.fileno(), 65536)
            if not chunk:
                break
            self.buffer += chunk
        if self.child.poll() is not None:
            raise RuntimeError((method, self.child.stderr.read().decode()))
        raise TimeoutError(method)

    def close(self):
        self.selector.close()
        self.child.terminate()
        try:
            self.child.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.child.kill()
            self.child.wait()


def write(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


def skills(rpc, cwd):
    response = rpc.call('skills/list', {'cwds': [str(cwd)], 'forceReload': True})
    return [s for d in response['data'] for s in d['skills']]


runtime = str(Path(sys.argv[1] if len(sys.argv) > 1 else shutil.which('codex')).resolve())
supplied = json.loads(Path(sys.argv[2]).read_text()) if len(sys.argv) > 2 else None
with tempfile.TemporaryDirectory(prefix='intent-codex-controls-') as tmp:
    root = Path(tmp)
    home, cwd, profile = [root / p for p in ('home', 'repo/nested', 'codex')]
    for p in (home, cwd, profile):
        p.mkdir(parents=True)
    repo = cwd.parent
    subprocess.run(['git', 'init', '-q', str(repo)], check=True)
    env = {'HOME': str(home), 'USERPROFILE': str(home), 'CODEX_HOME': str(profile), 'PATH': os.environ['PATH']}
    version = subprocess.check_output([runtime, '--version'], env=env, text=True).strip()
    assert version == 'codex-cli 0.160.0', version
    native_names = ('home.with.dot', 'ancestor', 'project', 'admin')
    markers = {name: root / (name + '-started') for name in (*native_names, 'approved', 'plugin')}
    server = root / 'mcp.py'
    write(server, '''import json, sys
from pathlib import Path
Path(sys.argv[1]).write_text('started')
for line in sys.stdin:
 m=json.loads(line)
 if 'id' not in m: continue
 result={'protocolVersion':'2024-11-05','capabilities':{'tools':{}},'serverInfo':{'name':'fixture','version':'1'}} if m['method']=='initialize' else {'tools':[{'name':'echo','description':'Fixture','inputSchema':{'type':'object','properties':{}}}]} if m['method']=='tools/list' else {}
 print(json.dumps({'jsonrpc':'2.0','id':m['id'],'result':result}),flush=True)
''')
    def mcp(name):
        return f'\n[mcp_servers.{json.dumps(name)}]\ncommand={json.dumps(sys.executable)}\nargs={json.dumps([str(server), str(markers[name])])}\n'
    write(profile / 'config.toml', f'[projects.{json.dumps(str(repo))}]\ntrust_level="trusted"\n' + mcp('home.with.dot'))
    write(repo / '.codex/config.toml', mcp('ancestor'))
    write(cwd / '.codex/config.toml', mcp('project'))
    plugin = repo / 'plugins/sentinel'
    marketplace = repo / '.agents/plugins/marketplace.json'
    write(plugin / '.codex-plugin/plugin.json', json.dumps({'name': 'sentinel', 'version': '1.0.0', 'description': 'Fixture', 'author': {'name': 'Intent fixture'}, 'skills': './skills/', 'mcpServers': './.mcp.json'}))
    write(plugin / 'skills/plugin-sentinel/SKILL.md', '---\nname: plugin-sentinel\ndescription: Fixture only\n---\nDo not invoke.\n')
    write(plugin / '.mcp.json', json.dumps({'mcpServers': {'sentinel': {'command': sys.executable, 'args': [str(server), str(markers['plugin'])]}}}))
    write(marketplace, json.dumps({'name': 'fixture', 'plugins': [{'name': 'sentinel', 'source': {'source': 'local', 'path': './plugins/sentinel'}, 'policy': {'installation': 'AVAILABLE', 'authentication': 'ON_INSTALL'}, 'category': 'Productivity'}]}))
    admin = root / 'admin'
    write(admin / 'config.toml', mcp('admin'))
    for base, name in ((home / '.agents/skills', 'home-sentinel'), (repo / '.agents/skills', 'ancestor-sentinel'), (cwd / '.agents/skills', 'project-sentinel'), (admin / 'skills', 'admin-sentinel')):
        write(base / name / 'SKILL.md', f'---\nname: {name}\ndescription: Fixture only\n---\nDo not invoke.\n')
    denied_paths = []
    bundled_paths = set()
    for mode in ('baseline', 'ephemeral', 'interactive'):
        controlled = mode != 'baseline'
        flags = []
        if controlled:
            flags += ['-c', 'features.plugins=false']
            flags += ['-c', 'mcp_servers={' + ','.join(json.dumps(n) + '={enabled=false}' for n in native_names) + '}']
            entries = ','.join('{path=' + json.dumps(p) + ',enabled=false}' for p in denied_paths if mode != 'interactive' or p not in bundled_paths)
            flags += ['-c', 'skills.config=[' + entries + ']']
            if supplied:
                key = 'interactive_runtime_args' if mode == 'interactive' else 'runtime_args'
                flags = [arg.replace('/__fixture__/', str(root) + '/') for arg in supplied[key]]
            if mode == 'interactive':
                flags += ['-c', 'mcp_servers.approved.command=' + json.dumps(sys.executable),
                          '-c', 'mcp_servers.approved.args=' + json.dumps([str(server), str(markers['approved'])])]
        # Fixture-only namespace: no host /etc files are read or changed.
        namespace = ['bwrap', '--unshare-user', '--die-with-parent', '--ro-bind', '/', '/', '--dev', '/dev', '--proc', '/proc',
                     '--bind', str(root), str(root), '--tmpfs', '/etc', '--ro-bind', str(admin), '/etc/codex', '--']
        rpc = Rpc([*namespace, runtime, 'app-server', *flags], cwd, env)
        try:
            rpc.call('initialize', {'clientInfo': {'name': 'intent-fixture', 'version': '1'}, 'capabilities': {'experimentalApi': True}})
            if not controlled:
                installed = rpc.call('plugin/install', {'marketplacePath': str(marketplace), 'pluginName': 'sentinel'})
                print(json.dumps({'plugin_install': installed}), flush=True)
            inventory = skills(rpc, cwd)
            config = rpc.call('config/read', {'cwd': str(cwd), 'includeLayers': True})
            rpc.call('thread/start', {'cwd': str(cwd), 'ephemeral': mode != 'interactive'})
            status = rpc.call('mcpServerStatus/list', {})
            names = {x['name'] for x in status['data']}
            print(json.dumps({'mode': mode, 'skills': [(s['name'], s['enabled'], s['scope'], s['path']) for s in inventory], 'mcp': sorted(names)}), flush=True)
            if not controlled:
                assert set(native_names) <= names
                assert all(markers[n].exists() for n in native_names)
                assert {'home-sentinel', 'ancestor-sentinel', 'project-sentinel', 'admin-sentinel'} <= {s['name'] for s in inventory if s['enabled']}
                assert any('plugin-sentinel' in s['name'] and s['enabled'] for s in inventory), 'plugin positive control absent'
                assert markers['plugin'].exists(), 'plugin MCP positive startup missing'
                markers['plugin'].unlink()
                denied_paths = [s['path'] for s in inventory]
                bundled_paths = {s['path'] for s in inventory if s['scope'] == 'system'}
                for n in native_names:
                    markers[n].unlink()
            else:
                effective_mcp = {s['name'] for s in status['data'] if s.get('tools')}
                assert effective_mcp == ({'approved'} if mode == 'interactive' else set()), status
                assert all(config['config']['mcp_servers'][n]['enabled'] is False for n in native_names)
                assert not any(markers[n].exists() for n in native_names)
                assert not markers['plugin'].exists(), 'disabled plugin MCP started'
                if mode == 'interactive':
                    assert markers['approved'].exists(), 'approved MCP did not start'
                    assert {s['path'] for s in inventory if s['enabled']} == bundled_paths
                else:
                    assert not any(s['enabled'] for s in inventory), inventory
                late = cwd / '.agents/skills/late-sentinel/SKILL.md'
                write(late, '---\nname: late-sentinel\ndescription: Fixture\n---\nDo not invoke.\n')
                refreshed = skills(rpc, cwd)
                assert any(s['name'] == 'late-sentinel' and s['enabled'] for s in refreshed), 'expected per-path denial limitation changed'
                late.unlink()
                print('PASS: known MCP and skills suppressed; reload exposes a new unlisted skill (not universal isolation)', flush=True)
        finally:
            rpc.close()

    # Test candidate boundaries without activating them in product launches.
    # Keep policy/config, session storage and the workspace visible; mask only
    # existing native skill directories in this fixture's Linux namespace.
    empty = root / 'empty-skills'
    empty.mkdir()
    instruction = repo / 'AGENTS.md'
    injected = root / 'intent-skills/approved/SKILL.md'
    write(instruction, 'Fixture workspace instructions\n')
    write(injected, 'Fixture Intent-injected skill\n')
    state = profile / 'sessions/fixture-state'
    write(state, 'Synthetic session state\n')
    late_root = cwd / 'created-after-launch'
    late = late_root / '.agents/skills/future-sentinel/SKILL.md'
    wildcard = '{path=' + json.dumps(str(late_root / '.agents/skills/*/SKILL.md')) + ',enabled=false}'
    boundary_flags = ['-c', 'features.plugins=false', '-c', 'skills.config=[' + wildcard + ']']
    masks = []
    for path in (home / '.agents/skills', repo / '.agents/skills', cwd / '.agents/skills', Path('/etc/codex/skills')):
        masks += ['--ro-bind', str(empty), str(path)]
    view = namespace[:-1] + masks + ['--']
    visible = [instruction, injected, state, profile / 'config.toml', Path('/etc/codex/config.toml')]
    subprocess.run([*view, sys.executable, '-c',
                    'from pathlib import Path; import sys; assert all(Path(p).read_text() for p in sys.argv[1:])',
                    *map(str, visible)], cwd=cwd, env=env, check=True)
    rpc = Rpc([*view, runtime, 'app-server', *boundary_flags], cwd, env)
    try:
        rpc.call('initialize', {'clientInfo': {'name': 'intent-fixture', 'version': '1'}, 'capabilities': {'experimentalApi': True}})
        assert not any(s['name'].endswith('-sentinel') for s in skills(rpc, cwd))
        rpc.call('skills/config/write', {'name': '*', 'enabled': False})
        write(late, '---\nname: future-sentinel\ndescription: Fixture\n---\nDo not invoke.\n')
        assert any(s['name'] == 'future-sentinel' and s['enabled'] for s in skills(rpc, late_root)), 'wildcard or filesystem-view limitation changed'
        rpc.call('skills/config/write', {'path': str(late), 'enabled': False})
        still_enabled = any(s['name'] == 'future-sentinel' and s['enabled'] for s in skills(rpc, late_root))
        print(json.dumps({'boundary_probe': 'existing skill directories masked; instructions, injected files, config and synthetic session state readable',
                          'new_nested_skill_visible': True, 'wildcard_denial_effective': False,
                          'post_discovery_exact_write_effective_with_cli_override': not still_enabled,
                          'native_model_tool_invocation_tested': False}), flush=True)
    finally:
        rpc.close()
