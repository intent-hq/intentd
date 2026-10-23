#!/usr/bin/env python3
"""Compile through a WebDAV sccache backend and prove reuse and invalidation.

By default a local HTTP fixture exercises real sccache without cache credentials.
--managed uses the configured self-hosted cache backend. --phase populate/verify splits reuse
across two independent CI jobs, with a unique source per workflow run.
"""
import argparse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import threading


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--managed', action='store_true')
    parser.add_argument('--phase', choices=['all', 'populate', 'verify'], default='all')
    args = parser.parse_args()
    env = dict(os.environ)
    if args.managed and not env.get('SCCACHE_WEBDAV_ENDPOINT'):
        # Deleting INTENT_SCCACHE_ENDPOINT is the documented way to run uncached;
        # the merge queue must stay open, so there is nothing to verify.
        print('::notice::No managed compiler cache is configured; skipping the managed cache smoke test.')
        return
    server = None
    entries = {}
    if not args.managed:
        for key in list(env):
            if key.startswith('SCCACHE_'):
                del env[key]

        class WebDAV(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_GET(self):
                body = entries.get(self.path)
                self.send_response(200 if body is not None else 404)
                self.send_header('Content-Length', str(len(body or b'')))
                self.end_headers()
                self.wfile.write(body or b'')

            def do_PUT(self):
                entries[self.path] = self.rfile.read(int(self.headers['Content-Length']))
                self.send_response(201)
                self.send_header('Content-Length', '0')
                self.end_headers()

            def do_PROPFIND(self):
                body = (f'<D:multistatus xmlns:D="DAV:"><D:response><D:href>{self.path}</D:href>'
                        '<D:propstat><D:prop><D:getlastmodified>Wed, 14 Oct 2026 00:00:00 GMT</D:getlastmodified><D:getcontentlength>0</D:getcontentlength><D:resourcetype><D:collection/></D:resourcetype>'
                        '</D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat>'
                        '</D:response></D:multistatus>').encode()
                self.send_response(207)
                self.send_header('Content-Type', 'application/xml')
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_MKCOL(self):
                self.send_response(201)
                self.send_header('Content-Length', '0')
                self.end_headers()

        server = ThreadingHTTPServer(('127.0.0.1', 0), WebDAV)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        env['SCCACHE_WEBDAV_ENDPOINT'] = f'http://127.0.0.1:{server.server_port}'
    sccache = os.environ.get('SCCACHE_BIN') or shutil.which('sccache')
    if not sccache:
        raise SystemExit('sccache is required')
    with socket.socket() as port:
        port.bind(('127.0.0.1', 0))
        env['SCCACHE_SERVER_PORT'] = str(port.getsockname()[1])
    env.update(CARGO_INCREMENTAL='0', SCCACHE_GHA_ENABLED='false')
    for key in ['RUSTFLAGS', 'CARGO_ENCODED_RUSTFLAGS']:
        env.pop(key, None)
    # Stable cwd across the two hosted jobs; source identity is run-specific.
    identity = os.environ.get('GITHUB_RUN_ID', 'local')
    directory = tempfile.TemporaryDirectory(prefix='managed-cache-smoke-') if args.phase == 'all' else None
    root = Path(directory.name if directory else Path(tempfile.gettempdir()) / ('intent-cache-smoke-' + identity))
    root.mkdir(exist_ok=True)
    env['SCCACHE_DIR'] = str(root / 'unused-local-cache')

    def run(*command):
        return subprocess.check_output(command, cwd=root, env=env, text=True, stderr=subprocess.STDOUT)

    def stats():
        return json.loads(run(sccache, '--show-stats', '--stats-format=json'))['stats']

    def build(value):
        # A fresh output directory rules out Cargo/incremental/local artifact reuse.
        shutil.rmtree(root / 'target', ignore_errors=True)
        (root / 'target').mkdir()
        (root / 'fixture.rs').write_text(f'// {identity}\npub fn answer() -> u32 {{ {value} }}\n')
        run(sccache, '--zero-stats')
        run(sccache, 'rustc', '--crate-name', 'cache_fixture', '--crate-type', 'rlib',
            '--emit=link', '-C', 'opt-level=2', 'fixture.rs', '--out-dir', 'target')
        measured = stats()
        (root / 'main.rs').write_text(f'fn main() {{ assert_eq!(cache_fixture::answer(), {value}); }}\n')
        binary = 'check.exe' if os.name == 'nt' else 'check'
        run('rustc', 'main.rs', '--extern', 'cache_fixture=target/libcache_fixture.rlib', '-o', 'target/' + binary)
        run(str(root / 'target' / binary))
        return measured

    try:
        run(sccache, '--start-server')
        if args.phase != 'verify':
            cold = build(42)
            assert cold['cache_write_errors'] == 0, cold
            if not args.managed:
                assert entries, 'WebDAV must receive compiled objects'
        if args.phase != 'populate':
            # Kill process-local state as well as build outputs before reuse.
            run(sccache, '--stop-server')
            run(sccache, '--start-server')
            warm = build(42)
            assert warm['cache_hits']['counts'].get('Rust', 0) > 0, warm
            changed = build(43 + int(os.environ.get('GITHUB_RUN_ATTEMPT', '1')))
            assert changed['cache_misses']['counts'].get('Rust', 0) > 0, changed
            assert changed['cache_write_errors'] == 0, changed
            print(json.dumps({'managed': args.managed, 'rust_hits': warm['cache_hits']['counts']['Rust'],
                              'changed_source_misses': changed['cache_misses']['counts']['Rust']}))
        else:
            print('Populated the managed cache for verification in a separate job.')
        assert not (root / 'unused-local-cache').exists(), 'remote test must not use a disk cache'
    finally:
        subprocess.run([sccache, '--stop-server'], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if server:
            server.shutdown()
        if directory:
            directory.cleanup()
        else:
            shutil.rmtree(root)


if __name__ == '__main__':
    main()
