"""Real-runtime regression for #6982, invoked by codex_policy_runtime.rs.

Use an empty temporary home, dummy auth, and a loopback Responses endpoint.
The endpoint rejects inference after capturing tools; no paid model call occurs.
"""
import http.server
import json
import os
import selectors
import shlex
import shutil
import socket
import signal
import subprocess
import sys
import tempfile
import threading
import time

binary, adapter, raw_policy, metadata_version = sys.argv[1:]
policy = json.loads(raw_policy)
requests = []


def assert_tool_request_since(before, purpose):
    # A session can also make auxiliary requests without any tools. Require a
    # tool-bearing inference request for every tested lifecycle transition so
    # those auxiliary requests cannot make a missing prompt look successful.
    assert any(body.get("tools") and body.get("model") == model
               for _, body in requests[before:]), \
        f"{purpose}: no tool-bearing request for {model}; models={[body.get('model') for _, body in requests[before:]]}"


class Responses(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        body = self.rfile.read(int(self.headers["Content-Length"]))
        requests.append((self.path, json.loads(body)))
        self.send_response(400)
        self.end_headers()
        self.wfile.write(b'{"error":{"message":"intentional local fixture stop"}}')

    def log_message(self, *_args):
        pass


with tempfile.TemporaryDirectory(prefix="intent-codex-policy-") as home:
    env = {"PATH": os.environ["PATH"], "HOME": home, "CODEX_HOME": home}
    version = subprocess.check_output([binary, "--version"], env=env, text=True).strip()
    print(version, flush=True)
    server = http.server.HTTPServer(("127.0.0.1", 0), Responses)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    # Start with hostile but version-compatible user config. agents.enabled is
    # only supported in newer runtimes; probe its schema rather than guessing
    # a version boundary. These subprocesses cannot see the user's real home.
    probe = subprocess.run(
        [binary, "-c", "agents.enabled=true", "features", "list"],
        env=env, cwd=home, capture_output=True, timeout=15,
    )
    agents = "[agents]\nenabled=true\n" if probe.returncode == 0 else ""
    model = "intent-policy-" + metadata_version
    catalog = os.path.join(home, "models.json")
    # Synthetic metadata exercises the runtime's model-owned v1/v2 selection,
    # which can override disabled feature flags unless agents.enabled is false.
    with open(catalog, "w") as file:
        json.dump({"models": [{
            "slug": model, "display_name": model, "description": "policy fixture",
            "base_instructions": "Use tools when needed.",
            "default_reasoning_level": "medium",
            "supported_reasoning_levels": [{"effort": "medium", "description": "fixture"}],
            "shell_type": "shell_command",
            "visibility": "list", "supported_in_api": True, "priority": 0,
            "support_verbosity": False,
            "truncation_policy": {"mode": "tokens", "limit": 10000},
            "experimental_supported_tools": [],
            "multi_agent_version": None if metadata_version == "none" else metadata_version,
        }]}, file)
    print("model metadata multi_agent_version=" + metadata_version, flush=True)
    config = f'''model = "{model}"
model_catalog_json = "{catalog}"
model_provider = "fixture"
[model_providers.fixture]
name = "local policy fixture"
base_url = "http://127.0.0.1:{server.server_port}/v1"
wire_api = "responses"
request_max_retries = 0
stream_max_retries = 0
[features]
multi_agent = true
multi_agent_v2 = true
enable_request_compression = false
{agents}'''
    with open(os.path.join(home, "config.toml"), "w") as file:
        file.write(config)
    with open(os.path.join(home, "auth.json"), "w") as file:
        json.dump({"OPENAI_API_KEY": "fixture-not-a-real-key"}, file)
    with tempfile.TemporaryFile() as stderr:
        def start_adapter():
            child = subprocess.Popen(
                ["node", adapter], cwd=home,
                env={**env, "CODEX_PATH": binary, "CODEX_CONFIG": raw_policy},
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=stderr,
                start_new_session=True,
            )
            selector = selectors.DefaultSelector()
            selector.register(child.stdout, selectors.EVENT_READ)
            return child, selector

        def stop_adapter():
            selector.close()
            os.killpg(child.pid, signal.SIGTERM)
            child.wait(timeout=10)
            child.stdin.close()
            child.stdout.close()

        child, selector = start_adapter()
        next_id = 0
        pending = b""
        updates = []

        def request(method, params, allow_error=False):
            global next_id, pending
            next_id += 1
            child.stdin.write((json.dumps({"jsonrpc": "2.0", "id": next_id,
                                         "method": method, "params": params}) + "\n").encode())
            child.stdin.flush()
            deadline = time.monotonic() + 30
            while True:
                while b"\n" not in pending:
                    remaining = deadline - time.monotonic()
                    assert remaining > 0 and selector.select(remaining), f"{method} timed out"
                    chunk = os.read(child.stdout.fileno(), 65536)
                    assert chunk, f"{method}: adapter exited"
                    pending += chunk
                line, pending = pending.split(b"\n", 1)
                msg = json.loads(line)
                if msg.get("id") != next_id:
                    updates.append(msg)
                    continue
                assert allow_error or "error" not in msg, f"{method}: {msg.get('error')}"
                return msg.get("result", {"error": msg.get("error")})

        def confirm_title(session_id, fallback):
            global pending
            titles = [note["params"]["update"]["title"] for note in updates
                      if note.get("method") == "session/update"
                      and note.get("params", {}).get("sessionId") == session_id
                      and note.get("params", {}).get("update", {}).get("sessionUpdate") == "session_info_update"
                      and "title" in note["params"]["update"]]
            title = (titles[-1] if titles else None) or fallback
            before = len(requests)
            notes_before = len(updates)
            result = request("session/prompt", {"sessionId": session_id,
                "prompt": [{"type": "text", "text": "/rename " + title}]})
            assert result["stopReason"] == "end_turn"
            assert len(requests) == before, "rename must never infer"
            deadline = time.monotonic() + 5
            while not any(note.get("params", {}).get("sessionId") == session_id
                          and note.get("params", {}).get("update", {}).get("title") == title
                          for note in updates[notes_before:]):
                while b"\n" not in pending:
                    remaining = deadline - time.monotonic()
                    assert remaining > 0 and selector.select(remaining), f"rename missing title echo: {updates[notes_before:]}"
                    chunk = os.read(child.stdout.fileno(), 65536)
                    assert chunk, "adapter closed during rename confirmation"
                    pending += chunk
                line, pending = pending.split(b"\n", 1)
                updates.append(json.loads(line))
            assert not any(note.get("params", {}).get("update", {}).get("sessionUpdate") in
                           ("user_message_chunk", "agent_message_chunk")
                           for note in updates[notes_before:]), "rename must not add conversation content"
            return title

        try:
            initialized = request("initialize", {
                "protocolVersion": 1, "clientCapabilities": {},
                "clientInfo": {"name": "intent-policy-regression", "version": "1"},
            })
            print("adapter", initialized["agentInfo"]["version"], flush=True)
            # Intent discovers models via session/new, and launches persistent
            # and one-shot agents through the same adapter/config contract.
            for purpose in ("model discovery", "agent session"):
                params = {"cwd": home, "mcpServers": []}
                if purpose != "model discovery":
                    # Match production persistent-session metadata, including
                    # adapters which ignore this optional title field.
                    params["_meta"] = {"sessionTitle": "Intent policy agent"}
                session = request("session/new", params)
                assert session["models"]["availableModels"], purpose
                assert session["models"]["currentModelId"] == model + "[medium]", session["models"]
                print(purpose, "accepted, catalog nonempty", flush=True)
            confirm_title(session["sessionId"], "User chosen session title")
            before_new = len(requests)
            request("session/prompt", {"sessionId": session["sessionId"],
                    "prompt": [{"type": "text", "text": "Say hello."}]}, allow_error=True)
            assert_tool_request_since(before_new, "new session")
            before_live = len(requests)
            request("session/prompt", {"sessionId": session["sessionId"],
                    "prompt": [{"type": "text", "text": "Continue in the live session."}]}, allow_error=True)
            assert_tool_request_since(before_live, "live reused prompt")
            request("session/resume", {"sessionId": session["sessionId"], "cwd": home, "mcpServers": []})
            assert confirm_title(session["sessionId"], "MUST NOT replace user title") == "User chosen session title"
            before_same_process = len(requests)
            request("session/prompt", {"sessionId": session["sessionId"],
                    "prompt": [{"type": "text", "text": "Continue after same-process resume."}]}, allow_error=True)
            assert_tool_request_since(before_same_process, "same-process resumed prompt")
            # Intent recreates the adapter process when restoring a session.
            # Older runtimes ignore config changes on a still-loaded thread.
            stop_adapter()
            child, selector = start_adapter()
            pending = b""
            request("initialize", {"protocolVersion": 1, "clientCapabilities": {},
                    "clientInfo": {"name": "intent-policy-regression", "version": "1"}})
            request("session/load", {"sessionId": session["sessionId"], "cwd": home, "mcpServers": []})
            print("session/load accepted after process recreation", flush=True)
            assert confirm_title(session["sessionId"], "MUST NOT replace user title") == "User chosen session title"
            before_resume = len(requests)
            request("session/prompt", {"sessionId": session["sessionId"],
                    "prompt": [{"type": "text", "text": "Say hello again."}]}, allow_error=True)
            assert_tool_request_since(before_resume, "process-recreated loaded prompt")
            before_review = len(requests)
            request("session/prompt", {"sessionId": session["sessionId"],
                    "prompt": [{"type": "text", "text": "/review Inspect this session without changing files."}]}, allow_error=True)
            assert any(body.get("tools") for _, body in requests[before_review:]), "review must reach inference"
            utility = request("session/new", {"cwd": home, "mcpServers": []})
            confirm_title(utility["sessionId"], "Intent utility")
            before_utility = len(requests)
            request("session/prompt", {"sessionId": utility["sessionId"],
                    "prompt": [{"type": "text", "text": "Say hello utility."}]}, allow_error=True)
            assert_tool_request_since(before_utility, "utility prompt")
            daemon_binary = os.environ.get("INTENT_CODEX_TEST_DAEMON")
            if daemon_binary:
                # Exercise the real service one-shot runner and its title
                # confirmation, not just the equivalent ACP wire sequence.
                # Use a built-in model since the isolated utility profile
                # intentionally does not copy arbitrary user catalog paths.
                with open(os.path.join(home, "config.toml"), "w") as file:
                    file.write(config.replace(f'model = "{model}"', 'model = "gpt-6-sol"')
                               .replace(f'model_catalog_json = "{catalog}"\n', ""))
                bindir = os.path.join(home, "bin")
                os.mkdir(bindir)
                os.symlink(binary, os.path.join(bindir, "codex"))
                node = shutil.which("node")
                node_shim = os.path.join(bindir, "node")
                with open(node_shim, "w") as file:
                    file.write("#!/bin/sh\nexec " + shlex.quote(node) + " \"$@\"\n")
                os.chmod(node_shim, 0o700)
                npx = os.path.join(bindir, "npx")
                with open(npx, "w") as file:
                    file.write("#!/bin/sh\nprintf \"%s\\n\" \"$@\" >> \"$0.trace\"\nif [ \"$1\" = --version ]; then echo 10.9.2; exit 0; fi\nexec "
                               + shlex.quote(node) + " " + shlex.quote(adapter) + "\n")
                os.chmod(npx, 0o700)
                data = os.path.join(home, "data")
                os.makedirs(os.path.join(data, "workspaces"))
                daemon_env = {**env, "PATH": bindir + os.pathsep + env["PATH"],
                              "SHELL": "/bin/false", "INTENTD_DATA_DIR": data,
                              "INTENTD_WORKSPACES_DIR": os.path.join(data, "workspaces"),
                              "INTENTD_ASSERT_HERMETIC_ROOT": "1", "INTENTD_TCP_PORT": "0",
                              "GH_CONFIG_DIR": os.path.join(data, "gh-config"),
                              "INTENTD_SECRETS_FILE": os.path.join(data, "secrets.json")}
                daemon = subprocess.Popen([daemon_binary, "serve"], env=daemon_env, cwd=home,
                                          stdout=subprocess.DEVNULL, stderr=stderr, start_new_session=True)
                try:
                    uds = os.path.join(data, "intentd.sock")
                    deadline = time.monotonic() + 30
                    while not os.path.exists(uds):
                        assert daemon.poll() is None and time.monotonic() < deadline, "daemon startup failed"
                        time.sleep(.02)
                    before_daemon = len(requests)
                    with socket.socket(socket.AF_UNIX) as connection:
                        connection.settimeout(40)
                        connection.connect(uds)
                        connection.sendall((json.dumps({"jsonrpc":"2.0", "id":1,
                            "method":"host.providerTestPrompt", "params":{"providerId":"codex"}}) + "\n").encode())
                        response = json.loads(connection.makefile("r").readline())
                    assert any(body.get("model") == "gpt-6-sol"
                               and "say hello" in json.dumps(body.get("input", []))
                               for _, body in requests[before_daemon:]), response
                    with open(npx + ".trace") as trace:
                        assert "@agentclientprotocol/codex-acp@2.1.1" in trace.read(), "must use exact pinned adapter fixture"
                    print("production daemon utility path emitted requested inference", flush=True)
                finally:
                    os.killpg(daemon.pid, signal.SIGTERM)
                    try:
                        daemon.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        os.killpg(daemon.pid, signal.SIGKILL)
                        daemon.wait(timeout=5)
            forbidden = {"spawn_agent", "send_input", "wait", "wait_agent", "resume_agent",
                         "close_agent", "spawn_agents_on_csv"}
            def tool_names(tools):
                names = set()
                for tool in tools:
                    names.add(tool.get("name", tool.get("type")))
                    names.update(tool_names(tool.get("tools", [])))
                return names

            for index, (path, body) in enumerate(requests):
                names = tool_names(body.get("tools", []))
                assert not forbidden & names, \
                    f"request {index} {path} model={body.get('model')}: native delegation exposed: {forbidden & names}"
            print("native delegation absent on new/live/same-process-resumed/recreated-loaded/utility turns despite user enabling flags", flush=True)
        finally:
            stop_adapter()
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)
