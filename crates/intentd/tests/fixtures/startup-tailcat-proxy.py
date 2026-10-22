#!/usr/bin/env python3
"""Local forwarding fixture for the saved Tailcat startup route (no relay access)."""
import json
import os
import select
import socket
import sys
import threading

assert sys.argv[1:3] == ["serve", "--json"]
key_arg = next(arg for arg in sys.argv if arg.startswith("--key="))
assert open(key_arg.split("=", 1)[1], encoding="utf-8").read() == "saved-test-key"
upstream = int(sys.argv[-1])
listener = socket.socket()
listener.bind(("127.0.0.1", int(os.environ["INTENTD_TEST_PROXY_PORT"])))
listener.listen()
print(json.dumps({"listenAddr": "tc-saved-startup-test:443"}), flush=True)


def forward(client):
    with client, socket.create_connection(("127.0.0.1", upstream)) as server:
        peers = {client: server, server: client}
        while True:
            for source in select.select(list(peers), [], [])[0]:
                data = source.recv(65536)
                if not data:
                    return
                peers[source].sendall(data)


while True:
    client, _ = listener.accept()
    threading.Thread(target=forward, args=(client,), daemon=True).start()
