#!/usr/bin/env python3
"""Run an unmodified released sitter against the new daemon and held startup.

The sitter argument must be built from the frozen base (b6fc9f881), not this PR.
No user data, credentials, services, or external network endpoints are used.
"""
import hashlib
import http.server
import io
import json
import os
from pathlib import Path
import shlex
import shutil
import signal
import socket
import sqlite3
import subprocess
import sys
import tarfile
import tempfile
import threading
import time

sitter, daemon, target, budget_ms, seed = sys.argv[1:]
budget = int(budget_ms) / 1000
version = subprocess.check_output([daemon, "--version"], text=True).strip().split()[1]


def wait_for(check, seconds=15):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        value = check()
        if value:
            return value
        time.sleep(0.025)  # timing-guard: bounded polling of owned subprocess state
    raise AssertionError("timed out waiting for fixture state")


def rpc(path, method):
    with socket.socket(socket.AF_UNIX) as client:
        client.settimeout(3)
        client.connect(str(path))
        client.sendall((json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": {"clientId":"legacy-sitter-test","name":"legacy-sitter-test"} if method == "client.hello" else {}}) + "\n").encode())
        return json.loads(client.makefile().readline())


for mode in ("start", "serve", "update"):
    with tempfile.TemporaryDirectory(prefix="legacy-sitter-startup-") as temp:
        root = Path(temp)
        shutil.copyfile(seed, root / "intentd.db")
        env = dict(os.environ)
        for name in ("GITHUB_TOKEN", "GH_TOKEN", "GH_ENTERPRISE_TOKEN", "GITHUB_ENTERPRISE_TOKEN", "GH_HOST", "INTENTD_SITTER_CHANNEL", "INTENTD_CHANNEL", "INTENTD_SOCKET", "INTENTD_SOCKET_PATH", "INTENTD_CONFIG", "INTENTD_TCP_PORT", "INTENTD_AUTH_TOKEN"):
            env.pop(name, None)
        (root / "gh-config").mkdir()
        env.update(INTENTD_DATA_DIR=temp, INTENTD_WORKSPACES_DIR=str(root / "workspaces"),
                   GH_CONFIG_DIR=str(root / "gh-config"), INTENTD_SECRETS_FILE=str(root / "secrets.json"),
                   INTENTD_CONFIG=str(root / "config.toml"), INTENTD_TCP_PORT="0",
                   INTENTD_TEST_INDEX_STARTUP_GATE=str(root / "gate"),
                   INTENTD_SITTER_READINESS_TIMEOUT_MS=budget_ms,
                   INTENTD_SITTER_KILL_TIMEOUT_MS="500", TOKIO_WORKER_THREADS="1")
        if budget_ms == "60000":
            env.pop("INTENTD_SITTER_READINESS_TIMEOUT_MS", None)  # exercise the actual default
        installed = "0.0.1" if mode == "update" else version
        binary_dir = root / "sitter" / "versions" / installed
        binary_dir.mkdir(parents=True)
        wrapper = ("#!/bin/sh\nexec " + shlex.quote(daemon) + ' "$@"\n').encode()
        (binary_dir / "intentd").write_bytes(wrapper)
        (binary_dir / "intentd").chmod(0o700)
        (binary_dir / "libexec").mkdir()
        (binary_dir / "libexec" / "tailcat").write_text("#!/bin/sh\nexit 1\n")
        (binary_dir / "libexec" / "tailcat").chmod(0o700)
        (binary_dir / "libexec" / "tailcat.LICENSE").write_text("fixture license")
        state_path = root / "sitter" / "state.json"
        state_path.write_text(json.dumps({"schema": 1, "channel": "stable", "current_version": installed}))
        archive = io.BytesIO()
        with tarfile.open(fileobj=archive, mode="w:gz") as tar:
            info = tarfile.TarInfo("intentd-" + target + "/intentd")
            info.mode = 0o755
            info.size = len(wrapper)
            tar.addfile(info, io.BytesIO(wrapper))
            for name in ("tailcat", "tailcat.LICENSE"):
                content = (binary_dir / "libexec" / name).read_bytes()
                info = tarfile.TarInfo("intentd-" + target + "/libexec/" + name)
                info.mode = 0o755 if name == "tailcat" else 0o644
                info.size = len(content)
                tar.addfile(info, io.BytesIO(content))
        payload = archive.getvalue()
        published = [installed]

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path.endswith(".json"):
                    body = json.dumps({"schema": 1, "version": published[0], "tag": "v" + published[0],
                        "platforms": {target: {"asset": "fixture.tar.gz", "url": base + "/fixture.tar.gz",
                                              "sha256": hashlib.sha256(payload).hexdigest()}}}).encode()
                else:
                    body = payload
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *_args):
                pass

        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        base = "http://127.0.0.1:" + str(server.server_port)
        env["INTENTD_SITTER_MANIFEST_BASE_URL"] = base
        threading.Thread(target=server.serve_forever, daemon=True).start()
        gate = socket.socket(socket.AF_UNIX)
        gate.bind(str(root / "gate"))
        gate.listen()
        gate.settimeout(20)
        log = open(root / "fixture.log", "w+")
        command = "serve" if mode in ("serve", "update") else "start"
        if command == "serve":
            env["INTENTD_SITTER_STARTING"] = "1"
        caller = subprocess.Popen([sitter, command], env=env, stdout=log, stderr=log, start_new_session=True)
        update = None
        peers = []

        def finish_indexing(peer):
            peer.sendall(b"c")
            next_peer, _ = gate.accept()
            peers.append(next_peer)
            stage = next_peer.recv(1)
            if stage == b"n":
                next_peer.sendall(b"c")
                next_peer, _ = gate.accept()
                peers.append(next_peer)
                stage = next_peer.recv(1)
            assert stage == b"c"
            next_peer.sendall(b"c")

        try:
            peer, _ = gate.accept()
            peers.append(peer)
            assert peer.recv(1) == b"i"
            path = root / "intentd.sock"
            if mode == "update":
                finish_indexing(peer)
                wait_for(lambda: "result" in rpc(path, "system.status"))
                # Fixture-owned derived state forces real rebuild work after restart.
                with sqlite3.connect(root / "intentd.db") as db:
                    db.execute("UPDATE note_page_head SET indexed_rev=-1")
                published[0] = version
                update = subprocess.Popen([sitter, "update"], env=env, stdout=log, stderr=log, start_new_session=True)
                peer, _ = gate.accept()
                peers.append(peer)
                assert peer.recv(1) == b"i"
            snapshot = rpc(path, "system.startupStatus")["result"]
            assert snapshot["progress"]["totalNotes"] == 1
            assert snapshot["progress"]["completedNotes"] == 0
            start = time.monotonic()
            while time.monotonic() - start < budget + 0.4:
                # The old sitter may call this liveness ready, but every wire
                # request still sees actual startup and cannot enter services.
                current = rpc(path, "system.startupStatus")["result"]
                assert current["attemptId"] == snapshot["attemptId"]
                assert current["progress"] == snapshot["progress"]
                for method in ("system.status", "client.hello", "note.add"):
                    assert rpc(path, method)["error"]["code"] == -32012
                if mode != "start":
                    assert caller.poll() is None, "old supervisor killed live startup"
                elif caller.poll() is not None:
                    assert caller.returncode == 0, "old outer startup failed"
                time.sleep(0.05)  # timing-guard: repeatedly prove liveness beyond frozen deadline
            if mode == "start":
                assert caller.wait(timeout=5) == 0
            if update:
                assert update.wait(timeout=5) == 0, "old update consumer must accept actual server version"
            finish_indexing(peer)
            wait_for(lambda: "result" in rpc(path, "system.status"))
            # A new connection now performs a genuine normal hello.
            assert "result" in rpc(path, "client.hello")
            print(f"PASS frozen sitter {mode}: live beyond {budget}s, wire gated, then fresh ready", flush=True)
        except BaseException:
            log.flush()
            log.seek(0)
            print(log.read(), file=sys.stderr)
            startlog = root / "sitter" / "start.log"
            if startlog.exists():
                print(startlog.read_text(), file=sys.stderr)
            raise
        finally:
            subprocess.run([sitter, "stop"], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=10)
            for proc in (update, caller):
                if proc is not None and proc.poll() is None:
                    os.killpg(proc.pid, signal.SIGKILL)
                    proc.wait()
            for peer in peers:
                peer.close()
            gate.close()
            server.shutdown()
            server.server_close()
            log.close()
