"""Real-runtime regression for #6982, invoked by codex_policy_runtime.rs.

Use an empty temporary home, dummy auth, and a loopback Responses endpoint.
The endpoint rejects inference after capturing tools; no paid model call occurs.
"""
import http.server
import json
import os
import selectors
import signal
import subprocess
import sys
import tempfile
import threading
import time

binary, adapter, raw_policy = sys.argv[1:]
policy = json.loads(raw_policy)
requests = []


def assert_tool_request_since(before, purpose):
    # A session can also make auxiliary requests without any tools. Require a
    # tool-bearing inference request for every tested lifecycle transition so
    # those auxiliary requests cannot make a missing prompt look successful.
    assert any(body.get("tools") for _, body in requests[before:]), \
        f"{purpose}: no tool-bearing request reached the local fixture"


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
    config = f'''model = "gpt-5.4"
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
                    continue
                assert allow_error or "error" not in msg, f"{method}: {msg.get('error')}"
                return msg.get("result", {"error": msg.get("error")})

        try:
            initialized = request("initialize", {
                "protocolVersion": 1, "clientCapabilities": {},
                "clientInfo": {"name": "intent-policy-regression", "version": "1"},
            })
            print("adapter", initialized["agentInfo"]["version"], flush=True)
            # Intent discovers models via session/new, and launches persistent
            # and one-shot agents through the same adapter/config contract.
            for purpose in ("model discovery", "agent session"):
                session = request("session/new", {"cwd": home, "mcpServers": []})
                assert session["models"]["availableModels"], purpose
                print(purpose, "accepted, catalog nonempty", flush=True)
            request("session/prompt", {"sessionId": session["sessionId"],
                    "prompt": [{"type": "text", "text": "Say hello."}]}, allow_error=True)
            assert_tool_request_since(0, "new session")
            before_live = len(requests)
            request("session/prompt", {"sessionId": session["sessionId"],
                    "prompt": [{"type": "text", "text": "Continue in the live session."}]}, allow_error=True)
            assert_tool_request_since(before_live, "live reused prompt")
            request("session/resume", {"sessionId": session["sessionId"], "cwd": home, "mcpServers": []})
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
            before_resume = len(requests)
            request("session/prompt", {"sessionId": session["sessionId"],
                    "prompt": [{"type": "text", "text": "Say hello again."}]}, allow_error=True)
            assert_tool_request_since(before_resume, "process-recreated loaded prompt")
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
                assert not forbidden & names, f"request {index}: native delegation exposed: {forbidden & names}"
            print("native delegation absent on new/live/same-process-resumed/recreated-loaded turns despite user enabling flags", flush=True)
        finally:
            stop_adapter()
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)
