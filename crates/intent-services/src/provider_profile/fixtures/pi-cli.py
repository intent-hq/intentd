"""Credential-free Pi 0.81.0 RPC startup/resume fixture. No model call."""
import json
import os
from pathlib import Path
import selectors
import shutil
import subprocess
import sys
import tempfile
import time

runtime = Path(sys.argv[1]).resolve()
assert json.loads((runtime / 'package.json').read_text())['version'] == '0.81.0'
node = shutil.which('node')
controls = json.loads(Path(sys.argv[2]).read_text()) if len(sys.argv) > 2 else None
with tempfile.TemporaryDirectory(prefix='intent-pi-cli-') as scratch:
    root = Path(scratch)
    home, cwd, profile = [root / p for p in ('home', 'repo', 'owned')]
    for p in (home, cwd, profile):
        p.mkdir()
    if controls:
        profile = Path(controls['directory'])
    marker = root / 'ambient-executed'
    mcp_marker = root / 'ambient-mcp-started'
    native = cwd / '.pi'
    native.mkdir()
    extension = native / 'sentinel.mjs'
    extension.write_text("import fs from 'node:fs'; export default function () {fs.writeFileSync(" + json.dumps(str(marker)) + ",'executed');}")
    mcp = {'mcpServers':{'ambient-sentinel':{'command':sys.executable,'args':['-c', 'from pathlib import Path; Path(' + repr(str(mcp_marker)) + ').write_text("started")']}}}
    for mcp_file in (native / 'mcp.json', home / '.pi/agent/mcp.json'):
        mcp_file.parent.mkdir(parents=True, exist_ok=True)
        mcp_file.write_text(json.dumps(mcp))
    (native / 'settings.json').write_text(json.dumps({'extensions': ['./sentinel.mjs'], 'packages': ['npm:intent-profile-must-not-install'], 'defaultModel': 'wrong-model'}))
    for base in (home / '.pi/agent/skills', home / '.agents/skills', cwd / '.pi/skills', cwd / '.agents/skills'):
        skill = base / 'ambient-sentinel'
        skill.mkdir(parents=True)
        (skill / 'SKILL.md').write_text('---\nname: ambient-sentinel\ndescription: Must be excluded\n---\nFixture\n')
    if not controls:
        (profile / 'settings.json').write_text(json.dumps({'packages':[], 'extensions':[], 'skills':[], 'prompts':[]}))
        (profile / 'models.json').write_text(json.dumps({'providers':{'fixture':{'baseUrl':'http://127.0.0.1:1/v1','apiKey':'fixture-only','api':'openai-completions','models':[{'id':'fixture-model','name':'Fixture','reasoning':False,'input':['text'],'cost':{'input':0,'output':0,'cacheRead':0,'cacheWrite':0},'contextWindow':32000,'maxTokens':1024}]}}}))
        (profile / 'auth.json').write_text(json.dumps({'fixture':{'type':'api_key','key':'fixture-only'}}))
    (profile / 'sessions').mkdir(exist_ok=True)
    session = profile / 'sessions' / 'session.jsonl'
    session.write_text(json.dumps({'type':'session','version':3,'id':'e0f493c5-2c6b-42f3-8afb-e29df6da93f8','timestamp':'2026-10-04T00:00:00.000Z','cwd':str(cwd)}) + '\n')
    with session.open('a') as log:
        log.write(json.dumps({'type':'message','id':'fixture-assistant','parentId':None,'timestamp':'2026-10-04T00:00:01.000Z','message':{'role':'assistant','content':[{'type':'text','text':'Seeded fixture history.'}],'api':'openai-completions','provider':'fixture','model':'fixture-model','usage':{'input':0,'output':0,'cacheRead':0,'cacheWrite':0,'totalTokens':0,'cost':{'input':0,'output':0,'cacheRead':0,'cacheWrite':0,'total':0}},'stopReason':'stop','timestamp':1791072001000}}) + '\n')
    # A fake package manager makes the positive control safe: installation is
    # observable, but no network, lifecycle script, or real package install runs.
    bin_dir = root / 'bin'
    bin_dir.mkdir()
    install_marker = root / 'install-attempted'
    npm = bin_dir / 'npm'
    npm.write_text('#!' + sys.executable + '\nfrom pathlib import Path\nPath(' + repr(str(install_marker)) + ').write_text("attempted")\nraise SystemExit(99)\n')
    npm.chmod(0o700)
    safe_env = {'PATH':str(bin_dir) + os.pathsep + os.environ['PATH'], 'HOME':str(home), 'USERPROFILE':str(home), 'PI_CODING_AGENT_DIR':str(profile), 'PI_CODING_AGENT_SESSION_DIR':str(profile / 'sessions'), 'NODE_DISABLE_COMPILE_CACHE':'1', 'PI_SKIP_VERSION_CHECK':'1'}
    baseline = [node, str(runtime / 'dist/cli.js'), '--mode', 'rpc', '--provider','fixture','--model','fixture-model','--no-skills','--no-extensions','--approve']
    baseline_result = subprocess.run(baseline, cwd=cwd, env=safe_env, input='', capture_output=True, text=True, timeout=25)
    assert install_marker.exists(), 'positive control did not attempt project package installation'
    install_marker.unlink()
    # Prior trust and an installed project package must not override --no-approve.
    (profile / 'trust.json').write_text(json.dumps({str(cwd): True}))
    cached = native / 'npm/node_modules/intent-profile-cached'
    cached.mkdir(parents=True)
    (cached / 'package.json').write_text(json.dumps({'name':'intent-profile-cached','version':'1.0.0','pi':{'extensions':['index.mjs']}}))
    (cached / 'index.mjs').write_text(extension.read_text())
    project_settings = json.loads((native / 'settings.json').read_text())
    project_settings['packages'].append('npm:intent-profile-cached')
    (native / 'settings.json').write_text(json.dumps(project_settings))
    owned_marker = root / 'owned-loaded'
    owned_extension = root / 'owned-extension.mjs'
    owned_extension.write_text("import fs from 'node:fs'; export default function (pi) {fs.appendFileSync(" + json.dumps(str(owned_marker)) + ", 'loaded\\n'); pi.registerCommand('fixture-reload', {description:'Fixture reload', handler:async (_args, ctx) => {await ctx.reload();}});}")
    ids = []
    for run in range(2):
        flags = controls['runtime_args'] if controls else ['--no-skills','--no-extensions','--no-prompt-templates','--no-themes','--no-approve','--no-tools','--offline']
        # Exercise denial independently of --offline, including resumed launches.
        flags = [flag for flag in flags if flag != '--offline']
        if not controls:
            flags = [*flags, '-e', str(owned_extension)]
        argv = [node, str(runtime / 'dist/cli.js'), '--mode', 'rpc', '--provider','fixture','--model','fixture-model','--session',str(session), *flags]
        env = dict(safe_env)
        if controls:
            env.update(controls['environment'])
        with subprocess.Popen(argv, cwd=cwd, env=env, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) as child:
            try:
                commands = [{'id':'name','type':'set_session_name','name':'fixture'}, {'id':'state','type':'get_state'}, {'id':'commands','type':'get_commands'}, {'id':'models','type':'get_available_models'}]
                for command in commands:
                    child.stdin.write(json.dumps(command) + '\n')
                child.stdin.flush()
                responses = {}
                selector = selectors.DefaultSelector()
                selector.register(child.stdout, selectors.EVENT_READ)
                buffer = ""
                deadline = time.monotonic() + 25
                while len(responses) < len(commands) and time.monotonic() < deadline:
                    if not selector.select(max(0, deadline-time.monotonic())):
                        break
                    chunk = os.read(child.stdout.fileno(), 65536).decode()
                    if not chunk:
                        break
                    buffer += chunk
                    while '\n' in buffer:
                        line, buffer = buffer.split('\n', 1)
                        item = json.loads(line)
                        if item.get('type') == 'response':
                            assert item.get('success'), 'RPC fixture command failed'
                            responses[item['id']] = item
                assert len(responses) == len(commands), f'RPC responses incomplete: {list(responses)}'
                state = responses['state']['data']
                assert state['model']['id'] == 'fixture-model'
                assert state['model']['provider'] == 'fixture'
                ids.append(state['sessionId'])
                assert not any(c.get('source') == 'skill' or 'ambient' in c.get('name','') for c in responses['commands']['data']['commands'])
                assert any(m['id'] == 'fixture-model' for m in responses['models']['data']['models'])
                assert not marker.exists(), 'native extension executed'
                assert not mcp_marker.exists(), 'native MCP server started'
                assert not install_marker.exists(), 'unapproved package installation attempted'
                if not controls:
                    assert owned_marker.exists(), 'explicit owned extension did not load'
                    assert any(c.get('name') == 'fixture-reload' for c in responses['commands']['data']['commands']), 'owned extension command missing'
                    before_reload = owned_marker.read_text().count('loaded')
                    child.stdin.write(json.dumps({'id':'reload','type':'prompt','message':'/fixture-reload'}) + '\n')
                    child.stdin.flush()
                    reloaded = False
                    deadline = time.monotonic() + 15
                    while time.monotonic() < deadline:
                        if not selector.select(max(0, deadline-time.monotonic())):
                            break
                        chunk = os.read(child.stdout.fileno(), 65536).decode()
                        if not chunk:
                            break
                        buffer += chunk
                        while '\n' in buffer:
                            line, buffer = buffer.split('\n', 1)
                            item = json.loads(line)
                            assert item.get('type') != 'extension_error', 'owned reload command failed'
                            if item.get('id') == 'reload' and item.get('type') == 'response':
                                assert item.get('success'), 'reload command failed'
                                reloaded = True
                        if reloaded:
                            break
                    assert reloaded, 'owned reload command did not complete'
                    assert owned_marker.read_text().count('loaded') > before_reload, 'owned extension did not reload'
                    assert not install_marker.exists(), 'reload attempted unapproved package installation'
                    assert not marker.exists(), 'reload executed a project extension'
                selector.close()
            finally:
                child.terminate()
                try: child.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait()
    assert ids[0] == ids[1], f'resume lost session identity: {ids}'
    print('PASS Pi 0.81.0 actual CLI: owned model/auth, zero skill commands, ambient exclusion, stable resume, prior-trust package denial without offline')
