#!/usr/bin/env python3
"""Synthetic app-server contract; no network or real credentials."""
import json
import os
from pathlib import Path
import sys
import time

home = Path(os.environ['CODEX_HOME'])
worker = (home / 'worker').exists()
if not worker and (home / 'expected-env').exists():
    expected = json.loads((home / 'expected-env').read_text())
    assert all(os.environ.get(key) == value for key, value in expected.items())
assert '--strict-config' in sys.argv
if (home / 'invalid-config').exists():
    sys.exit(2)
if worker:
    assert 'cli_auth_credentials_store="ephemeral"' in sys.argv
    assert not (home / 'auth.json').exists()


def send(value):
    print(json.dumps(value), flush=True)


pending = None
for line in sys.stdin:
    request = json.loads(line)
    method = request.get('method')
    with (home / 'requests').open('a') as log:
        log.write(json.dumps({'method': method, 'pid': os.getpid()}) + '\n')
    if 'id' not in request:
        continue
    if method == 'getAuthStatus' and (home / 'unsupported-method').exists():
        send({'id': request['id'], 'error': {'code': -32601,
              'message': 'unsupported method; never-echo-this-secret'}})
        continue
    result = {}
    if method == 'test/echo':
        result = request['params']
    elif method == 'configRequirements/read':
        result = {'requirements': None}
        if (home / 'managed-store').exists():
            result = {'requirements': {'cliAuthCredentialsStore': (home / 'managed-store').read_text()}}
    elif method == 'config/read':
        result = {'layers': []}
    elif method == 'getAuthStatus':
        once = home / 'delay-once'
        if once.exists():
            delay = float(once.read_text())
            once.unlink()
            (home / 'auth-read-started').write_text('ready')
            time.sleep(delay)
        if (home / 'delay').exists():
            time.sleep(float((home / 'delay').read_text()))
        path = home / 'auth.json'
        state = json.loads(path.read_text()) if path.exists() else {}
        if request['params'].get('refreshToken') and 'next' in state:
            # A fake single-use issuer; the current refresh token is consumed.
            used = home / 'consumed'
            previous = used.read_text().splitlines() if used.exists() else []
            refresh = state['refresh_token']
            if refresh in previous:
                send({'id': request['id'], 'error': {'code': -1, 'message': 'reused'}})
                continue
            with used.open('a') as file:
                file.write(refresh + '\n')
            state['authToken'] = state.pop('next')
            state['refresh_token'] += '-next'
            path.write_text(json.dumps(state))
        result = {'requiresOpenaiAuth': True, **state}
        result.pop('refresh_token', None)
        result.pop('next', None)
    elif method == 'account/login/start':
        params = request['params']
        assert params['type'] == 'chatgptAuthTokens'
        assert set(params) == {'type', 'accessToken', 'chatgptAccountId'}
        (home / 'injected').write_text(params['accessToken'])
    elif method == 'account/logout':
        # Any forwarded logout is a regression, observable across child exit.
        (home / 'revoked').write_text('unexpected native revocation')
    elif method == 'turn/start' and (home / 'request-refresh').exists():
        pending = request['id']
        send({'id': 'refresh', 'method': 'account/chatgptAuthTokens/refresh',
              'params': {'reason': 'unauthorized', 'previousAccountId': 'account'}})
        continue
    elif request.get('id') == 'refresh':
        (home / 'refresh-outcome').write_text('error' if 'error' in request else 'success')
        send({'id': pending, **({'result': {}} if 'result' in request else {'error': request['error']})})
        continue
    elif method in ('thread/start', 'thread/resume') and (home / 'setup-error').exists():
        send({'id': request['id'], 'error': {'code': -1, 'message': (home / 'setup-error').read_text()}})
        continue
    send({'id': request['id'], 'result': result})
