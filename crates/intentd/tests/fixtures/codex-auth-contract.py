#!/usr/bin/env python3
"""Synthetic installed-Codex contract probes; no native credentials or model turns.

Run with absolute Codex and Intent daemon binary paths.
Native baseline controls and the maintained Intent bridge use synthetic tokens only.
"""
import base64
import datetime
import fcntl
import http.server
import json
import os
from pathlib import Path
import queue
import signal
import shlex
import shutil
import subprocess
import sys
import tempfile
import threading
import time

binary = Path(sys.argv[1])
assert binary.is_absolute() and binary.is_file(), 'Pass an absolute Codex binary path'
for system_file in ('config.toml', 'managed_config.toml', 'requirements.toml'):
    assert not Path('/etc/codex', system_file).exists(), 'Use a disposable host without system policy'
assert sys.platform == 'linux', 'This harness is only verified on Linux'


def jwt(account='synthetic-A', serial=1, expired=False):
    claims = {'sub': 'synthetic-user', 'email': 'synthetic@example.invalid',
              'exp': 1 if expired else int(time.time()) + 3600, 'serial': serial,
              'https://api.openai.com/auth': {'chatgpt_account_id': account,
                                             'chatgpt_user_id': 'synthetic-user',
                                             'chatgpt_plan_type': 'plus'}}
    enc = lambda obj: base64.urlsafe_b64encode(json.dumps(obj).encode()).decode().rstrip('=')
    return enc({'alg': 'none', 'typ': 'JWT'}) + '.' + enc(claims) + '.synthetic'


class Endpoint(http.server.BaseHTTPRequestHandler):
    posts = []
    rotate = False
    used = set()
    counter = 1
    lock = threading.Lock()
    barrier = None
    delay_response = 0
    consumed = threading.Event()

    def log_message(self, *args):
        pass

    def do_GET(self):
        # Exact 0.160.0 account/read routing contract, matching upstream's
        # app-server/tests/suite/v2/workspace_routing.rs fixtures.
        if self.path.startswith('/backend-api/wham/accounts/check'):
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({'accounts': [
                {'id': account, 'workspace_backend_origin': 'https://synthetic.invalid',
                 'account_routing_override': 'NO_CONSTRAINT'}
                for account in ('synthetic-A', 'synthetic-B')]}).encode())
        else:
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b'{}')

    def do_POST(self):
        body = self.rfile.read(int(self.headers.get('Content-Length', 0)))
        cls = type(self)
        cls.posts.append(self.path)
        result, status = {}, 503
        if self.path == '/oauth/revoke':
            status = 200
        elif self.path == '/oauth/token' and cls.rotate:
            request = json.loads(body)
            if cls.barrier is not None:
                cls.barrier.wait(timeout=5)
            with cls.lock:
                previous = request['refresh_token']
                if previous in cls.used:
                    status, result = 400, {'error': {'code':'refresh_token_reused'}}
                else:
                    cls.used.add(previous)
                    cls.consumed.set()
                    cls.counter += 1
                    status, result = 200, {'access_token':jwt(serial=cls.counter), 'id_token':jwt(serial=cls.counter), 'refresh_token':'rotated-' + str(cls.counter)}
        if self.path == '/oauth/token' and status == 200 and cls.delay_response:
            time.sleep(cls.delay_response)
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(result).encode())


endpoint = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Endpoint)
threading.Thread(target=endpoint.serve_forever, daemon=True).start()
origin = 'http://127.0.0.1:' + str(endpoint.server_port)


class Probe:
    def __init__(self, config='', seed=None, strict=True, prefix=(), extra_args=(), diagnostic=False, bridge=None, authority=None, start=True, adapter=None):
        self.adapter = adapter
        self.owner = None
        self.tmp = tempfile.TemporaryDirectory(prefix='intent-codex-auth-contract-')
        self.home = Path(self.tmp.name)
        self.codex_home = self.home / 'codex'
        self.codex_home.mkdir()
        self.auth = self.codex_home / 'auth.json'
        (self.codex_home / 'config.toml').write_text('chatgpt_base_url=' + json.dumps(origin + '/backend-api') + '\n' + config)
        if seed is not None:
            self.auth.write_text(json.dumps(seed))
        env = {'PATH': '/usr/bin:/bin', 'HOME': str(self.home), 'USERPROFILE': str(self.home),
               'CODEX_HOME': str(self.codex_home), 'XDG_CONFIG_HOME': str(self.home / 'config'),
               'XDG_DATA_HOME': str(self.home / 'data'), 'XDG_CACHE_HOME': str(self.home / 'cache'),
               'XDG_RUNTIME_DIR': str(self.home / 'runtime'), 'RUST_LOG': 'off',
               'HTTP_PROXY': origin, 'HTTPS_PROXY': origin, 'ALL_PROXY': origin,
               'NO_PROXY': 'localhost,127.0.0.1',
               'CODEX_REFRESH_TOKEN_URL_OVERRIDE': origin + '/oauth/token',
               'CODEX_REVOKE_TOKEN_URL_OVERRIDE': origin + '/oauth/revoke'}
        if authority and not bridge:
            env['CODEX_HOME'] = str(authority.codex_home)
            env['HOME'] = str(authority.home)
        prefix = prefix(self.home) if callable(prefix) else prefix
        args = [*prefix, str(binary), 'app-server', *extra_args] + (['--strict-config'] if strict else [])
        if bridge:
            worker = self.home / 'worker'
            worker.mkdir()
            (worker / 'config.toml').write_text((self.codex_home / 'config.toml').read_text())
            native_home = authority.codex_home if authority else self.codex_home
            user_home = authority.home if authority else self.home
            socket = self.home / 'authority.sock'
            if start:
                owner_args = [str(bridge), 'provider', 'codex-auth-owner', '--runtime', str(binary),
                              '--native-home', str(native_home), '--user-home', str(user_home), '--socket', str(socket)]
                self.owner = subprocess.Popen(owner_args, cwd=self.home, env=env, stdin=subprocess.PIPE,
                                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
            args = [str(bridge), 'provider', 'codex-auth-bridge', '--runtime', str(binary), '--native-home', str(native_home), '--user-home', str(user_home), '--profile', str(worker), '--authority-socket', str(socket), '--', 'app-server']
            self.worker_home = worker
            if adapter:
                wrapper = self.home / 'codex-native-auth.sh'
                wrapper.write_text('#!/bin/sh\nexec ' + shlex.join(args[:-1]) + ' "$@"\n')
                wrapper.chmod(0o700)
                env['CODEX_HOME'] = str(worker)
                env['CODEX_PATH'] = str(wrapper)
                args = [shutil.which('node'), str(adapter)]
        self.proc = None
        if not start:
            return
        self.proc = subprocess.Popen(args, cwd=self.home, env=env, text=True,
                                     stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                     stderr=None if diagnostic else subprocess.DEVNULL, start_new_session=True)
        self.frames = queue.Queue()
        self.counter = 0
        def read():
            for line in self.proc.stdout:
                try:
                    self.frames.put(json.loads(line))
                except ValueError:
                    continue
            self.frames.put(None)
        threading.Thread(target=read, daemon=True).start()

    def close(self):
        if self.proc is None:
            self.tmp.cleanup()
            return
        try:
            os.killpg(self.proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        self.proc.wait(timeout=3)
        if self.owner is not None:
            self.owner.stdin.close()
            self.owner.wait(timeout=20)
        self.tmp.cleanup()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def send(self, frame):
        if self.adapter:
            frame = {'jsonrpc': '2.0', **frame}
        self.proc.stdin.write(json.dumps(frame) + '\n')
        self.proc.stdin.flush()

    def call(self, method, params=None, timeout=8):
        self.counter += 1
        wanted = self.counter
        self.send({'id': wanted, 'method': method, 'params': params or {}})
        deadline = time.monotonic() + timeout
        while True:
            frame = self.frames.get(timeout=max(.01, deadline - time.monotonic()))
            if frame is None:
                raise EOFError('app-server exited')
            if frame.get('id') == wanted:
                return frame
            assert time.monotonic() < deadline, 'RPC deadline'

    def initialize(self, experimental=True):
        frame = self.call('initialize', {'clientInfo': {'name': 'synthetic-contract-probe', 'version': '0'},
                                        'capabilities': {'experimentalApi': experimental}})
        assert 'result' in frame, 'initialize failed'
        self.send({'method': 'initialized'})

    def status(self, refresh=False):
        frame = self.call('getAuthStatus', {'includeToken': True, 'refreshToken': refresh})
        assert 'result' in frame, 'getAuthStatus failed'
        return frame['result']


def auth_seed(token, account='synthetic-A'):
    return {'auth_mode': 'chatgpt', 'tokens': {'id_token': token, 'access_token': token,
            'refresh_token': 'synthetic-refresh', 'account_id': account},
            'last_refresh': datetime.datetime.now(datetime.timezone.utc).isoformat()}


def passed(name):
    print('PASS ' + name, flush=True)


def adapter_busy_recovery(bridge, adapter):
    with Probe(seed=auth_seed(jwt()), start=False) as native:
        with Probe(bridge=bridge, authority=native, adapter=adapter) as acp:
            initialized = acp.call('initialize', {
                'protocolVersion': 1, 'clientCapabilities': {},
                'clientInfo': {'name': 'intent-busy-contract', 'version': '1'},
            }, timeout=20)
            assert 'result' in initialized, 'actual ACP initialize failed'
            params = {'cwd': str(acp.home), 'mcpServers': []}
            created = acp.call('session/new', params, timeout=30)
            assert 'result' in created, 'actual ACP session setup failed'
            session_id = created['result']['sessionId']
            resume = {**params, 'sessionId': session_id}
            before = native.auth.read_bytes()
            revocations = Endpoint.posts.count('/oauth/revoke')
            with (native.codex_home / '.intent-auth.lock').open('r+') as lock:
                fcntl.flock(lock, fcntl.LOCK_EX)
                started = time.monotonic()
                busy = acp.call('session/load', resume)
                waited = time.monotonic() - started
                assert 'error' in busy, 'busy authority must reject this request'
                assert busy['error']['code'] != -32000, 'ACP must not report Authentication required'
                assert 'authentication is busy' in json.dumps(busy), 'busy guidance must reach ACP caller'
                assert 'codex login' not in json.dumps(busy), 'busy must not request native login'
                assert 2 <= waited < 8, 'busy response must remain bounded'
                assert native.auth.read_bytes() == before, 'busy request changed native credentials'
                assert Endpoint.posts.count('/oauth/revoke') == revocations
                assert acp.proc.poll() is None, 'adapter died on busy response'
                fcntl.flock(lock, fcntl.LOCK_UN)
            retried = acp.call('session/load', resume, timeout=30)
            assert 'result' in retried, ('same-adapter resume failed after busy', retried.get('error'))
            assert native.auth.read_bytes() == before
            assert Endpoint.posts.count('/oauth/revoke') == revocations
            assert not (acp.worker_home / 'auth.json').exists()
            passed('actual ACP busy classification and same-session retry preserve native login; wait=' + format(waited, '.3f') + 's')


def main():
    try:
        token = jwt()
        login = {'type': 'chatgptAuthTokens', 'accessToken': token, 'chatgptAccountId': 'synthetic-A'}
        with Probe('cli_auth_credentials_store="ephemeral"\n') as p:
            p.initialize()
            requirements = p.call('configRequirements/read')
            layers = p.call('config/read', {'includeLayers': True})
            assert 'result' in requirements and 'result' in layers
            print('POLICY_SHAPE ' + json.dumps({'requirements': requirements['result'],
                  'layer_names': [layer.get('name') for layer in layers['result'].get('layers', [])]}), flush=True)
            assert 'result' in p.call('account/login/start', login)
            passed('external login with experimental opt-in')
            status = p.status(True)
            assert status['authMethod'] == 'chatgptAuthTokens' and status['authToken'] == token
            passed('external token export remains unchanged on explicit account refresh')
            assert not p.auth.exists()
            passed('external login does not persist auth.json')
            before = len(Endpoint.posts)
            assert 'result' in p.call('account/logout')
            assert len(Endpoint.posts) == before
            passed('external logout makes no token/revoke request')
            assert p.status()['authToken'] is None
            passed('external logout clears worker auth')
        for name, config, experimental in [
                ('external login rejects missing experimental opt-in', '', False),
                ('external login preserves forced API policy', 'forced_login_method="api"\n', True),
                ('external login preserves forced workspace policy', 'forced_chatgpt_workspace_id="different"\n', True)]:
            with Probe('cli_auth_credentials_store="ephemeral"\n' + config) as p:
                p.initialize(experimental)
                assert 'error' in p.call('account/login/start', login)
                passed(name)
        with Probe(seed=auth_seed(token)) as p:
            p.initialize()
            assert p.status()['authToken'] == token
            newer = jwt(serial=2)
            p.auth.write_text(json.dumps(auth_seed(newer)))
            assert p.status()['authToken'] == token
            assert p.status(True)['authToken'] == newer
            passed('persistent native helper requires forced reload for same-account file change')
            p.auth.write_text(json.dumps(auth_seed(jwt('synthetic-B'), 'synthetic-B')))
            assert p.status(True)['authToken'] == newer
            p.auth.unlink()
            assert p.status(True)['authToken'] == newer
            passed('persistent native helper retains cached account after account switch and logout')
        with Probe(seed=auth_seed(token)) as p:
            p.initialize()
            assert p.status()['authToken'] == token
            before = len(Endpoint.posts)
            assert p.status(True)['authToken'] == token
            assert p.status(True)['authToken'] == token
            assert Endpoint.posts[before:] == ['/oauth/token', '/oauth/token']
            passed('failed native refresh returns RPC success and the unchanged token twice')
        bad_config = 'cli_auth_credentials_store="keyring"\nmodel_provider="missing-synthetic-provider"\n'
        file_key = {'OPENAI_API_KEY': 'synthetic-file-api-key'}
        with Probe(bad_config, file_key, strict=False) as p:
            p.initialize()
            assert p.status()['authToken'] == 'synthetic-file-api-key'
            passed('non-strict invalid keyring config falls back to file credentials')
        with Probe(bad_config, file_key, strict=True) as p:
            try:
                p.initialize()
            except (EOFError, BrokenPipeError):
                passed('strict invalid keyring config fails before initialization')
            else:
                raise AssertionError('strict startup unexpectedly succeeded')

        if len(sys.argv) > 2:
            bridge = Path(sys.argv[2])
            assert bridge.is_absolute() and bridge.is_file()
            # Proxy traffic must retain upstream's supported inline image size.
            # A nonexistent thread rejects before model work or image decoding.
            for label, executable in [('native', None), ('Intent', bridge)]:
                with Probe(seed=auth_seed(token), bridge=executable) as image_probe:
                    image_probe.initialize()
                    assert 'result' in image_probe.call('account/read', {'refreshToken': False})
                    image = 'data:image/png;base64,' + 'A' * (9 * 1024 * 1024)
                    result = image_probe.call('turn/start', {
                        'threadId': 'synthetic-missing-thread',
                        'input': [{'type': 'image', 'url': image}],
                    })
                    assert 'error' in result and 'thread' in result['error']['message'].lower()
                    assert 'result' in image_probe.call('account/read', {'refreshToken': False})
                    passed(label + ' accepts a 9MiB inline-image frame and stays available')
            # Local detach never routes account/logout to native auth. The
            # separate unmodified native control must still reach revoke.
            with Probe(seed=auth_seed(token), bridge=bridge) as p:
                p.initialize()
                assert 'result' in p.call('account/read', {'refreshToken':False})
                assert p.status()['authToken'] == token
                newer = jwt(serial=50)
                p.auth.write_text(json.dumps(auth_seed(newer)))
                assert 'result' in p.call('account/read', {'refreshToken':False})
                assert p.status()['authToken'] == newer
                before = len(Endpoint.posts)
                assert 'result' in p.call('account/logout')
                assert Endpoint.posts[before:] == []
                assert not (p.worker_home / 'auth.json').exists()
                assert p.auth.exists()
                passed('Intent observes relogin and locally detaches without refresh credentials or revoke')
            with Probe(seed=auth_seed(token)) as native:
                native.initialize()
                before = len(Endpoint.posts)
                assert 'result' in native.call('account/logout')
                assert '/oauth/revoke' in Endpoint.posts[before:]
                passed('explicit native logout retains its intended revoke behavior')
            Endpoint.rotate = True
            Endpoint.used.clear()
            with Probe(seed=auth_seed(jwt(expired=True)), bridge=bridge) as p:
                p.initialize()
                assert 'result' in p.call('account/read', {'refreshToken':False})
                assert p.status()['authToken'] != jwt(expired=True)
                assert len(Endpoint.used) == 1
                passed('expired access token renews through native authority before worker login')
            # Accepted refresh ownership survives the caller's shorter budget.
            Endpoint.used.clear()
            Endpoint.consumed.clear()
            Endpoint.delay_response = 8
            with Probe(seed=auth_seed(jwt(expired=True)), start=False) as native:
                before_revoke = Endpoint.posts.count('/oauth/revoke')
                with Probe(bridge=bridge, authority=native) as abandoned:
                    started = time.monotonic()
                    response = abandoned.call('initialize', {
                        'clientInfo': {'name': 'slow-refresh', 'version': '1'},
                        'capabilities': {'experimentalApi': True},
                    }, timeout=10)
                    assert Endpoint.consumed.is_set()
                    assert 'error' in response and 'busy' in response['error']['message']
                    assert time.monotonic() - started < 9
                # Closing the lease drained and reaped the owner after persistence.
                persisted = json.loads(native.auth.read_text())
                assert persisted['tokens']['refresh_token'].startswith('rotated-')
                Endpoint.delay_response = 0
                with Probe(bridge=bridge, authority=native) as retry:
                    retry.initialize()
                    assert 'result' in retry.call('account/read', {'refreshToken': False})
                assert len(Endpoint.used) == 1
                assert Endpoint.posts.count('/oauth/revoke') == before_revoke
                passed('slow single-use refresh survives caller timeout and lease closure; fresh retry succeeds')
            Endpoint.delay_response = 0
            Endpoint.used.clear()
            with Probe(seed=auth_seed(token), start=False) as native:
                # Force a sequential bootstrap refresh. A second independent
                # Intent worker must read the persisted replacement lineage.
                state = auth_seed(jwt(expired=True))
                state['last_refresh'] = '2000-01-01T00:00:00Z'
                native.auth.write_text(json.dumps(state))
                with Probe(bridge=bridge, authority=native) as worker:
                    worker.initialize()
                    assert 'result' in worker.call('account/read', {'refreshToken':False})
                    assert not (worker.worker_home / 'auth.json').exists()
                persisted = json.loads(native.auth.read_text())
                assert persisted['tokens']['refresh_token'].startswith('rotated-')
                with Probe(bridge=bridge, authority=native) as worker:
                    worker.initialize()
                    assert 'result' in worker.call('account/read', {'refreshToken':False})
                assert len(Endpoint.used) == 1
                passed('sequential Intent workers retain a single-use native refresh lineage')

            Endpoint.used.clear()
            with Probe(seed=auth_seed(token), start=False) as native:
                state = auth_seed(jwt(expired=True))
                state['last_refresh'] = '2000-01-01T00:00:00Z'
                native.auth.write_text(json.dumps(state))
                failures = []
                def worker():
                    try:
                        with Probe(bridge=bridge, authority=native) as p:
                            p.initialize()
                            assert 'result' in p.call('account/read', {'refreshToken':False})
                    except BaseException as error:
                        failures.append(type(error).__name__)
                threads = [threading.Thread(target=worker) for _ in range(2)]
                for thread in threads: thread.start()
                for thread in threads: thread.join(timeout=20)
                assert all(not thread.is_alive() for thread in threads)
                assert not failures, failures
                assert len(Endpoint.used) == 1
                passed('concurrent Intent workers serialize single-use native refresh')
            # Baseline control: ordinary native processes do not take Intent's
            # authority lock. Force both to present the same single-use token.
            Endpoint.used.clear()
            Endpoint.barrier = threading.Barrier(2)
            with Probe(seed=auth_seed(token), start=False) as native:
                before = len(Endpoint.posts)
                # Initialize the shared native SQLite schema before starting
                # the second process; the control measures refresh, not schema races.
                with Probe(authority=native) as first:
                    first.initialize()
                    with Probe(authority=native) as second:
                        second.initialize()
                        failures = []
                        def refresh(process):
                            try:
                                process.status(True)
                            except BaseException as error:
                                failures.append(type(error).__name__)
                        threads = [threading.Thread(target=refresh, args=(p,))
                                   for p in (first, second)]
                        for thread in threads: thread.start()
                        for thread in threads: thread.join(timeout=10)
                        assert all(not thread.is_alive() for thread in threads)
                        assert not failures, failures
                assert Endpoint.posts[before:].count('/oauth/token') == 2
                assert len(Endpoint.used) == 1
                passed('native/native baseline still presents a reused refresh token without Intent locking')
            Endpoint.barrier = None
    finally:
        endpoint.shutdown()
        endpoint.server_close()


if __name__ == "__main__":
    if len(sys.argv) == 4:
        try:
            adapter_busy_recovery(Path(sys.argv[2]), Path(sys.argv[3]))
        finally:
            endpoint.shutdown()
            endpoint.server_close()
    else:
        main()
