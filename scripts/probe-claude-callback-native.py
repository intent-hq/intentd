#!/usr/bin/env python3
"""Bounded offline native-control probe. No credentials, model prompt or host network.

Only named control and Services startup cases are exposed. Product trust comes from the exact accepted
installer; the client, bridge, native CLI, sandbox and OS inputs are pinned here.
A real inert containment check runs before payload execution in the SAME namespace.
"""
import argparse
import ast
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import resource
import re
import select
import selectors
import signal
import socket
import stat
import subprocess
import sys
import time

OS_INPUTS = json.loads(r'''{"bwrap":{"bytes":72160,"mode":"0o755","path":"/usr/bin/bwrap","sha256":"52231e1caf55bcbc667b269f49c63599a6f7db4767ae6a039580d0ff853db712","uid":0},"mounts":{"/bin/sh":{"bytes":129784,"sha256":"86d31f6fb799e91fa21bad341484564510ca287703a16e9e46c53338776f4f42","source":"/usr/bin/dash"},"/lib/x86_64-linux-gnu/ld-linux-x86-64.so.2":{"bytes":236616,"sha256":"cd4df4f3c7b83673d61189bf2eaebd33ca4f2853ab9772b8a25e025ef99b1e81","source":"/usr/lib/x86_64-linux-gnu/ld-linux-x86-64.so.2"},"/lib/x86_64-linux-gnu/libc.so.6":{"bytes":2125328,"sha256":"8db37cf3f2169f59a0f07ef1fea308c35656668c64c8ff294e1860f4121eb161","source":"/usr/lib/x86_64-linux-gnu/libc.so.6"},"/lib/x86_64-linux-gnu/libcrypto.so.3":{"bytes":5309400,"sha256":"1451aceec262c3338052fa77542eb971d4ba311c6bf12d9aa70d0b56aca942f9","source":"/usr/lib/x86_64-linux-gnu/libcrypto.so.3"},"/lib/x86_64-linux-gnu/libdl.so.2":{"bytes":14408,"sha256":"292d5f5af2e7360b3e18c56591a4960115373ecf40627660f9149b6c68a33f80","source":"/usr/lib/x86_64-linux-gnu/libdl.so.2"},"/lib/x86_64-linux-gnu/libexpat.so.1":{"bytes":174336,"sha256":"c42ff317838b4b4639e2ea801905f0317177c6df7e31b2f0d0240e3c3ac0cfde","source":"/usr/lib/x86_64-linux-gnu/libexpat.so.1.9.1"},"/lib/x86_64-linux-gnu/libgcc_s.so.1":{"bytes":183024,"sha256":"d93224d2b0dab4247598be683adca02f5cf00586f99c187579cd7e92058fb7cb","source":"/usr/lib/x86_64-linux-gnu/libgcc_s.so.1"},"/lib/x86_64-linux-gnu/libm.so.6":{"bytes":952616,"sha256":"e9c4b28d340e415b8137480ec442662f981e1399386c5931dae0e886e3639e91","source":"/usr/lib/x86_64-linux-gnu/libm.so.6"},"/lib/x86_64-linux-gnu/libpthread.so.0":{"bytes":14408,"sha256":"a27ffa9bf233d61a5f02ddb0cf770dd6579021afc1aa8aec0fb58ee4a965281a","source":"/usr/lib/x86_64-linux-gnu/libpthread.so.0"},"/lib/x86_64-linux-gnu/librt.so.1":{"bytes":14624,"sha256":"c6e6288545e24b0b3cfbf33320bda9236521625d8c3d628f3444f1ed40e5c7c5","source":"/usr/lib/x86_64-linux-gnu/librt.so.1"},"/lib/x86_64-linux-gnu/libssl.so.3":{"bytes":696512,"sha256":"55869549f4c7d7221e311121696f135390a7172755459ad04aef831f855eb214","source":"/usr/lib/x86_64-linux-gnu/libssl.so.3"},"/lib/x86_64-linux-gnu/libstdc++.so.6":{"bytes":2592224,"sha256":"1fd75fe70354a416d75aef22bcae68c47bd25d20e2d0568c30b1a9838cf62f11","source":"/usr/lib/x86_64-linux-gnu/libstdc++.so.6.0.33"},"/lib/x86_64-linux-gnu/libz.so.1":{"bytes":113000,"sha256":"9b64150b28505a33d6bc3ecf709c279f6de97a1c184dbda65d06ee4537f6d286","source":"/usr/lib/x86_64-linux-gnu/libz.so.1.3"},"/lib64/ld-linux-x86-64.so.2":{"bytes":236616,"sha256":"cd4df4f3c7b83673d61189bf2eaebd33ca4f2853ab9772b8a25e025ef99b1e81","source":"/usr/lib/x86_64-linux-gnu/ld-linux-x86-64.so.2"},"/usr/bin/python3.12":{"bytes":8025024,"sha256":"a92f0f95e883390c7256b2e441484aac06b1002dbe1d924141a77c8d82f96223","source":"/usr/bin/python3.12"},"/usr/lib/python3.12/encodings/__init__.py":{"bytes":5884,"sha256":"78c4744d407690f321565488710b5aaf6486b5afa8d185637aa1e7633ab59cd8","source":"/usr/lib/python3.12/encodings/__init__.py"},"/usr/lib/python3.12/encodings/aliases.py":{"bytes":15677,"sha256":"6fdcc49ba23a0203ae6cf28e608f8e6297d7c4d77d52e651db3cb49b9564c6d2","source":"/usr/lib/python3.12/encodings/aliases.py"},"/usr/lib/python3.12/encodings/ascii.py":{"bytes":1248,"sha256":"578aa1173f7cc60dad2895071287fe6182bd14787b3fbf47a6c7983dfe3675e3","source":"/usr/lib/python3.12/encodings/ascii.py"},"/usr/lib/python3.12/encodings/utf_8.py":{"bytes":1005,"sha256":"ba0cac060269583523ca9506473a755203037c57d466a11aa89a30a5f6756f3d","source":"/usr/lib/python3.12/encodings/utf_8.py"},"/usr/lib/python3.12/os.py":{"bytes":39786,"sha256":"316d1b7307fd851bded3423c9d437e0a383c725d993f0fcff2e8b749fe560b62","source":"/usr/lib/python3.12/os.py"}}}''')
INSTALLER_SHA = "91ce43bc922c89142431351b87b5340f330ee1e8e000615e9c715d5ae2deae3f"
CLIENT_SHA = "5712e99ac1ea51edada02f5a4e95a056216d659cecd4f385d02fbc9da6d3b64e"
BRIDGE_SHA = "1f27c8d949c33878ee43e5d129d817f26cb985383d184be337599a9e9a8aad20"
BRIDGE_BYTES = 278795624
NATIVE_REL = "runtime/node_modules/@anthropic-ai/claude-agent-sdk-linux-x64/claude"
NATIVE_SHA = "1e08503dbdf3c2cb0d706d32f3408277388d1c76ef108673e8fe42c1b322925b"
NS_NAMES = ("user", "net", "mnt", "pid", "ipc", "uts")
ENV = {"HOME": "/home/probe", "XDG_CONFIG_HOME": "/home/probe/.config",
       "XDG_CACHE_HOME": "/home/probe/.cache", "TMPDIR": "/tmp",
       "CLAUDE_CONFIG_DIR": "/home/probe/.claude", "INTENTD_DATA_DIR": "/home/probe/intentd",
       "CLAUDE_CODE_EXECUTABLE": "/runtime/" + NATIVE_REL,
       "PATH": "/runtime/node/bin:/usr/bin:/bin", "PWD": "/work",
       "LANG": "C.UTF-8", "LC_ALL": "C.UTF-8",
       "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1", "DISABLE_TELEMETRY": "1",
       "DISABLE_ERROR_REPORTING": "1"}
OUTER_ENV = {"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8"}
OUTPUT_LIMIT = 262144
SERVICES_PREPARATION = Path("/home/clement/intent/workspaces/ideate-future/intent/.dev/slice-b/harness-native-startup-eventual-refusal/.dev/preparation")
SERVICES_ELF = SERVICES_PREPARATION / "artifacts/intent-services-lib-test"
SERVICES_SHA = "bd5d22f325d43af834efdd8c2cc13db1c2d6b7eea30a4daa5eaa0c461f096168"
SERVICES_BYTES = 546417904
SERVICES_FINGERPRINT_BYTES = {
    "attribution/raw-fingerprints.json": 947651,
    "attribution/fingerprint-number-tokens.json": 2228250,
    "attribution/fingerprint-qualification.json": 555,
}
SERVICES_SELECTORS = {
    "confirmed": "agent_manager::repository_origin::callback_delivery::tests::genuine_native_startup::normal_services_native_confirmed_startup",
    "legacy": "agent_manager::repository_origin::callback_delivery::tests::genuine_native_startup::normal_services_native_legacy_startup",
}
SERVICES_EVENTS = {
    "confirmed": ["runner-verified", "ordinary-started", "original-acknowledged", "original-reused", "owned-retired", "completed"],
    "legacy": ["runner-verified", "ordinary-started", "owned-retired", "completed"],
}
SERVICES_ENV = {**ENV, "INTENT_NATIVE_SERVICES_RUN": "1"}
# Immutable local build attribution, not registry provenance or reproducibility.
SERVICES_MANIFESTS = {
    "compile-v1/before.json": "8ae9935dc29883d3f4b427fb16545f810b1963596dc8797dfc35dcf07e05265a",
    "compile-v1/after.json": "8ae9935dc29883d3f4b427fb16545f810b1963596dc8797dfc35dcf07e05265a",
    "compile-v1/plan.json": "e1f462321283f576de818c42fb9840fed0a61f0e56972f0d54f6f3b2c0ceb5aa",
    "compile-v1/results.json": "b647973b9b613fe311d5458d7b93f11fc024df0ef13322e935a9876dd3e29189",
    "compile-v1/compile.stdout": "0b26f31c0c8514db256c93e20d7adbb46f0ebd19f94e9379c7f7f3c245368277",
    "attribution/elf.json": "362ab84887a4e4e0e893b3411b4e492c6000b39136a341e26834f1825a1aaacd",
    "attribution/cargo-selected.json": "1ef34ce85f20111fa7eca5715f24f36657275d8e2e01ab6ac2171e02ef85690e",
    "attribution/cargo-build-scripts.json": "04a8e0b091e32df1bb84fd0f60cde447e3a5854dff4cd9ff5e2f57be69bbcd04",
    "attribution/cargo-config-search.json": "987c7406f40aa9c97aaba3a577a8c33759997e2967b537ff6360d4c4496753b4",
    "attribution/dependency-files.json": "370aa25488083103b2d232ef2d06525bbcc63147e7d5dc2ecdcbc0393aec3174",
    "attribution/dependency-packages.json": "7c69e7ddf5b324b5112dce515ceb42271b604db63d8f6ec8044cfc593c039f2c",
    "attribution/generated-compiler-inputs.json": "91003809e192d8d3524fc47526751d1cacff05361442a5236e6613ac735d967f",
    "attribution/toolchain-inputs.json": "023837e4fc5f303cfc9c6cd0b3a8eebc604c9583b93842bc1e74febc413ec155",
    "attribution/compiler-dependency-records.json": "ce3b366ab3557406ba3369affa6fd364f9c8cddcd63074ace51ad21f294bbc8e",
    "attribution/os-closure.json": "e305b253ff4c1c7b8b6907e98d0a34059217b3063bbad5ebccb5c6e699d5f9e4",
    "attribution/raw-fingerprints.json": "1f5a7a8e6f449a773c6b768bec496cf2ae4ea3ec43940afa7d88836c8f62b9c0",
    "attribution/fingerprint-number-tokens.json": "66f35e21ec4f04038de9359bab959d1e255ef19738f560809278b3e9cd5cd9df",
    "attribution/fingerprint-qualification.json": "a5cc55e5e6e4b83fe58bafbad2da1031b192b70593a7144e4a7f3f8ab0d2d158"
}

class Refusal(Exception):
    pass

def require(value, message):
    if not value:
        raise Refusal(message)

def hash_file(path):
    result = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            result.update(chunk)
    return result.hexdigest()

def checked_file(path, digest, size=None, system=False):
    path = Path(path)
    info = path.lstat()
    require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1, "non-regular/linked input")
    require(not info.st_mode & 0o6022, "unsafe writable/set-id input")
    if system:
        require(info.st_uid == 0, "OS input must be root owned")
    if size is not None:
        require(info.st_size == size, "input size mismatch")
    require(hash_file(path) == digest, "input digest mismatch")
    return path

def source_bytes(path, digest):
    """Capture pinned source bytes; Git checkouts may retain a group-write bit.

    Only these captured bytes are executed or privately copied for a mount.
    OS and product inputs still use the stricter immutable-file check above.
    """
    with Path(path).open("rb") as stream:
        info = os.fstat(stream.fileno())
        require(not Path(path).is_symlink() and stat.S_ISREG(info.st_mode)
                and info.st_nlink == 1 and info.st_uid == os.getuid()
                and not info.st_mode & 0o6002, "unsafe source input")
        data = stream.read()
    require(hashlib.sha256(data).hexdigest() == digest, "source digest mismatch")
    return data

def load_installer():
    path = Path(__file__).with_name("prepare-claude-callback-runtime.py")
    data = source_bytes(path, INSTALLER_SHA)
    spec = importlib.util.spec_from_file_location("accepted_runtime_installer", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    exec(compile(data, str(path), "exec"), module.__dict__)
    module.configuration()
    return module

def check_os():
    require(sys.platform == "linux", "Linux namespaces required")
    checked_file(Path(OS_INPUTS["bwrap"]["path"]), OS_INPUTS["bwrap"]["sha256"],
                 OS_INPUTS["bwrap"]["bytes"], system=True)
    for entry in OS_INPUTS["mounts"].values():
        checked_file(Path(entry["source"]), entry["sha256"], entry["bytes"], system=True)

def namespaces():
    return {name: os.readlink("/proc/self/ns/" + name) for name in NS_NAMES}

def process_state(pid):
    try:
        raw = Path(f"/proc/{pid}/stat").read_text()
        parts = raw[raw.rfind(")") + 2:].split()
        return {"start": parts[19], "state": parts[0], "parent": int(parts[1])}
    except (OSError, IndexError):
        return None

class OriginalProcessTree:
    """Own an unreaped Popen root and pidfds, never reusable numeric identities.

    The root is not polled/reaped until all group signals finish. Its reserved
    PID therefore pins its original process-group number. Sampled descendants
    use pidfds exclusively, including children which leave that group. The CLI
    additionally owns bwrap's PID namespace; its teardown covers unsampled
    namespace members. Samples alone are not a census of arbitrary escaped trees.
    """
    def __init__(self, root):
        self.root = root
        self.seen = {}
        self.handles = {}
        self.closed = False
        self.live = []
        self.error = None

    @staticmethod
    def exited_handle(fd):
        return bool(select.select([fd], [], [], 0)[0])

    def root_exited(self):
        # WNOWAIT keeps even an already-exited child allocated until finish().
        try:
            return os.waitid(os.P_PID, self.root.pid,
                             os.WEXITED | os.WNOHANG | os.WNOWAIT) is not None
        except ChildProcessError as error:
            raise Refusal("original child ownership was lost before cleanup") from error

    def capture_root(self):
        self.root_exited()
        fd = os.pidfd_open(self.root.pid)
        try:
            self.root_exited()
            state = process_state(self.root.pid)
            require(state is not None, "original child observation missing")
        except BaseException:
            os.close(fd)
            raise
        self.handles[self.root.pid] = fd
        self.seen[self.root.pid] = state["start"]

    def sample(self, pid=None):
        pid = self.root.pid if pid is None else pid
        parent_fd = self.handles.get(pid)
        if parent_fd is None or self.exited_handle(parent_fd):
            return
        state = process_state(pid)
        if state is None or state["start"] != self.seen.get(pid):
            return  # Never overwrite or traverse a reappearing numeric identity.
        try:
            children = Path(f"/proc/{pid}/task/{pid}/children").read_text().split()
        except OSError:
            return
        for value in children:
            child = int(value)
            if child not in self.seen:
                if self.exited_handle(parent_fd):
                    return
                try:
                    fd = os.pidfd_open(child)
                except ProcessLookupError:
                    continue
                try:
                    observed = process_state(child)
                    # Both original handles must still be live around the PPID
                    # observation. Thus the observed parent number still denotes
                    # this retained parent, not a recycled parent/child chain.
                    if (observed is None or observed["parent"] != pid
                            or self.exited_handle(fd) or self.exited_handle(parent_fd)):
                        continue
                    self.handles[child] = fd
                    self.seen[child] = observed["start"]
                finally:
                    if self.handles.get(child) != fd:
                        os.close(fd)
            self.sample(child)

    def finish(self):
        if self.closed:
            if self.error is not None:
                raise self.error
            return list(self.live)
        lost = False
        sampling_error = None
        previous_mask = signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGINT, signal.SIGTERM})
        try:
            try:
                self.sample()
            except BaseException as error:
                sampling_error = error  # Teardown is still mandatory on sampling failure.
            try:
                self.root_exited()
            except Refusal:
                lost = True
            if not lost:
                # No poll/wait occurred: the original root still reserves PGID.
                try:
                    os.killpg(self.root.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            for fd in self.handles.values():
                try:
                    signal.pidfd_send_signal(fd, signal.SIGKILL)
                except ProcessLookupError:
                    pass  # The ORIGINAL handle exited; never select a new PID.
            require(not lost, "original child ownership was lost before cleanup")
            self.root.wait(timeout=3)
            until = time.monotonic() + 1
            while True:
                self.live = [pid for pid, fd in self.handles.items()
                             if not self.exited_handle(fd)]
                if not self.live or time.monotonic() >= until:
                    break
                time.sleep(.01)
            if sampling_error is not None:
                raise sampling_error
            return list(self.live)
        except BaseException as error:
            self.error = error
            raise
        finally:
            for fd in self.handles.values():
                os.close(fd)
            self.handles.clear()
            self.closed = True
            signal.pthread_sigmask(signal.SIG_SETMASK, previous_mask)

def process_limit():
    # RLIMIT_NPROC counts the shared host UID's threads, not just this tree.
    # Bound the absolute UID total without assuming the host has <128 threads.
    threads = 0
    for path in Path("/proc").glob("[0-9]*"):
        try:
            if path.stat().st_uid == os.getuid():
                threads += len(list((path / "task").iterdir()))
        except FileNotFoundError:
            pass
    value = threads + 128
    require(value <= 4096, "host UID exceeds the bounded process budget")
    return value

def limits(nproc):
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    resource.setrlimit(resource.RLIMIT_CPU, (40, 40))
    resource.setrlimit(resource.RLIMIT_NOFILE, (128, 128))
    resource.setrlimit(resource.RLIMIT_NPROC, (nproc, nproc))
    resource.setrlimit(resource.RLIMIT_FSIZE, (8388608, 8388608))
    resource.setrlimit(resource.RLIMIT_AS, (128 * 1024 ** 3, 128 * 1024 ** 3))
    resource.setrlimit(resource.RLIMIT_DATA, (2 * 1024 ** 3, 2 * 1024 ** 3))

def run_bounded(command, seconds, cap=OUTPUT_LIMIT):
    """Private execution primitive; public CLI never accepts a command/binary."""
    started = time.monotonic()
    nproc = process_limit()
    require(signal.getsignal(signal.SIGCHLD) == signal.SIG_DFL,
            "original child ownership requires default SIGCHLD handling")
    child = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE, env=OUTER_ENV, start_new_session=True,
                             preexec_fn=lambda: limits(nproc))
    owner = OriginalProcessTree(child)
    chunks = {"stdout": bytearray(), "stderr": bytearray()}
    reason = "exited"
    try:
        owner.capture_root()
        with selectors.DefaultSelector() as selector:
            for name in chunks:
                stream = getattr(child, name)
                os.set_blocking(stream.fileno(), False)
                selector.register(stream, selectors.EVENT_READ, name)
            while selector.get_map() or not owner.root_exited():
                owner.sample()
                if time.monotonic() - started > seconds:
                    reason = "deadline"
                    break
                for key, _ in selector.select(0.02):
                    data = os.read(key.fileobj.fileno(), 16384)
                    if not data:
                        selector.unregister(key.fileobj)
                        continue
                    remaining = cap - sum(map(len, chunks.values()))
                    chunks[key.data].extend(data[:remaining])
                    if len(data) > remaining:
                        reason = "output-limit"
                        break
                if reason != "exited":
                    break
    except KeyboardInterrupt:
        reason = "cancelled"
    finally:
        try:
            live = owner.finish()
        finally:
            child.stdout.close()
            child.stderr.close()
    return {"command": command, "outer_environment": OUTER_ENV, "returncode": child.returncode,
            "uid_thread_limit": nproc,
            "reason": reason, "elapsed_seconds": time.monotonic() - started,
            "stdout": chunks["stdout"].decode("utf-8", errors="replace"),
            "stderr": chunks["stderr"].decode("utf-8", errors="replace"),
            "observed_original_processes": {str(k): v for k, v in owner.seen.items()},
            "live_original_processes_after_cleanup": live}

INERT = r"""
import os,sys,_socket
c = @CONFIG@
assert sys.flags.isolated and sys.flags.no_site
actual = {name:os.readlink('/proc/self/ns/'+name) for name in c['parent']}
assert all(actual[k]!=v for k,v in c['parent'].items()), 'namespace was shared'
assert dict(os.environ)==c['environment'], ('unexpected environment keys',
    sorted(set(os.environ)-set(c['environment'])), sorted(set(c['environment'])-set(os.environ)),
    [k for k in c['environment'] if os.environ.get(k)!=c['environment'][k]])
assert not os.path.exists(c['hidden']), 'host sentinel visible'
assert open('/probe/sentinel').read()==c['sentinel']
try:
    open('/probe/sentinel','w').write('must not write')
    raise AssertionError('readonly probe mount writable')
except OSError:
    pass
for path in ('/home/probe','/work','/tmp'):
    assert os.listdir(path)==[], 'private directory was not empty'
    with open(path+'/inert-check','w') as f:f.write('inert')
    os.unlink(path+'/inert-check')
devices = [line.split(':')[0].strip() for line in open('/proc/net/dev').read().splitlines()[2:]]
assert devices==['lo'], ('external interfaces',devices)
assert len(open('/proc/net/route').read().splitlines())==1, 'external IPv4 route'
for line in open('/proc/net/ipv6_route').read().splitlines():
    assert line.split()[-1]=='lo', 'external IPv6 route'
s=_socket.socket(_socket.AF_INET,_socket.SOCK_STREAM);s.settimeout(.3)
try:
    s.connect(('127.0.0.1',c['port']))
    raise AssertionError('host loopback listener reachable')
except OSError:
    pass
finally:s.close()
listener=_socket.socket(_socket.AF_INET,_socket.SOCK_STREAM)
listener.bind(('127.0.0.1',c['port']));listener.listen(1)
client=_socket.socket(_socket.AF_INET,_socket.SOCK_STREAM);client.settimeout(.3)
client.connect(('127.0.0.1',c['port']))
fd,_=listener._accept();peer=_socket.socket(fileno=fd);peer.send(b'inert-loopback')
assert client.recv(64)==b'inert-loopback'
peer.close();client.close();listener.close()
print('CONTAINMENT '+repr({'namespaces':actual,'private_loopback':True,'host_listener_hidden':True,
      'readonly_probe':True,'host_sentinel_hidden':True,'interfaces':devices,'environment':dict(os.environ)}),flush=True)
if c['payload']:
    if c.get('services_case'):
        assert 'CapEff:\t0000000000000000' in open('/proc/self/status').read().splitlines()
        for path in ('/etc','/run','/sys','/root','/home/clement'):
            assert not os.path.exists(path), 'unexpected host path'
        mounts=[line.split() for line in open('/proc/self/mountinfo')]
        for path in ('/','/runtime','/bridge/intentd','/probe/services-test','/probe/services-contract.json'):
            assert any(row[4]==path and 'ro' in row[5].split(',') for row in mounts), 'writable input mount'
        os.execve('/probe/services-test',c['services_argv'],c['environment'])
    os.execve('/runtime/node/bin/node',['/runtime/node/bin/node','--max-old-space-size=256',
              '/probe/client.mjs'],c['environment'])
"""

def sandbox_command(scratch, config, payload=None, bridge=None):
    command = [OS_INPUTS["bwrap"]["path"], "--unshare-user", "--unshare-net", "--unshare-pid",
               "--unshare-ipc", "--unshare-uts", "--new-session", "--die-with-parent",
               "--cap-drop", "ALL", "--clearenv", "--tmpfs", "/"]
    parents = {"/probe", "/bridge", "/runtime", "/home", "/home/probe", "/work", "/tmp",
               "/proc", "/dev", "/usr/lib/python3.12/lib-dynload"}
    for name in OS_INPUTS["mounts"]:
        parents.update(str(p) for p in Path(name).parents if str(p) != "/")
    for path in sorted(parents, key=lambda p: (p.count("/"), p)):
        command += ["--dir", path]
    for dest, entry in sorted(OS_INPUTS["mounts"].items()):
        command += ["--ro-bind", entry["source"], dest]
    command += ["--ro-bind", str(scratch / "visible-sentinel"), "/probe/sentinel"]
    if payload is not None:
        command += ["--ro-bind", str(payload), "/runtime",
                    "--ro-bind", str(bridge), "/bridge/intentd"]
        if config.get("services_case"):
            require(config["services_argv"] == services_argv(config["services_case"]),
                    "unexpected Services execution arguments")
            require(config["environment"] == SERVICES_ENV, "unexpected Services environment")
            command += ["--ro-bind", str(SERVICES_ELF), "/probe/services-test",
                        "--ro-bind", str(scratch / "services-contract.json"), "/probe/services-contract.json"]
        else:
            command += ["--ro-bind", str(scratch / "client.mjs"), "/probe/client.mjs"]
    for name, dest in (("home", "/home/probe"), ("work", "/work"), ("tmp", "/tmp")):
        command += ["--bind", str(scratch / name), dest]
    command += ["--proc", "/proc", "--dev", "/dev", "--remount-ro", "/",
                "--chdir", "/work"]
    for name, value in sorted(config["environment"].items()):
        command += ["--setenv", name, value]
    command += ["--", "/usr/bin/python3.12", "-I", "-S", "-B", "-c",
                INERT.replace("@CONFIG@", repr(config))]
    return command

def containment(scratch, seconds, payload=None, bridge=None, client=None, services_case=None):
    scratch.mkdir(mode=0o700)
    environment = ENV if services_case is None else SERVICES_ENV
    if services_case is not None:
        services_argv(services_case)  # Closed case validation, never a caller selector.
    if payload is not None and services_case is None:
        require(client is not None and hashlib.sha256(client).hexdigest() == CLIENT_SHA,
                "missing pinned client bytes")
        (scratch / "client.mjs").write_bytes(client)
        (scratch / "client.mjs").chmod(0o444)
    for name in ("home", "work", "tmp"):
        (scratch / name).mkdir(mode=0o700)
    token = os.urandom(16).hex()
    hidden = scratch / "hidden-host-sentinel"
    hidden.write_text(token)
    (scratch / "visible-sentinel").write_text(token)
    with socket.socket() as host:
        host.bind(("127.0.0.1", 0))
        host.listen(1)
        host.settimeout(0.02)
        config = {"parent": namespaces(), "hidden": str(hidden), "sentinel": token,
                  "port": host.getsockname()[1], "environment": environment, "payload": payload is not None}
        if services_case is not None and payload is not None:
            config.update({"services_case": services_case, "services_argv": services_argv(services_case)})
            (scratch / "services-contract.json").write_bytes(services_contract(services_case, config["parent"]))
            (scratch / "services-contract.json").chmod(0o444)
        command = sandbox_command(scratch, config, payload, bridge)
        result = run_bounded(command, seconds)
        result["parent_namespaces"] = config["parent"]
        result["sandbox_environment"] = environment
        try:
            connection, _ = host.accept()
        except TimeoutError:
            result["host_canary_connection"] = False
        else:
            connection.close()
            result["host_canary_connection"] = True
    proofs = [x.removeprefix("CONTAINMENT ") for x in result["stdout"].splitlines()
              if x.startswith("CONTAINMENT ")]
    proof = ast.literal_eval(proofs[0]) if len(proofs) == 1 else None
    result["containment"] = proof
    result["containment_passed"] = bool(proof and not result["host_canary_connection"]
        and not result["live_original_processes_after_cleanup"]
        and proof["environment"] == environment and proof["interfaces"] == ["lo"]
        and all(proof[name] is True for name in ("private_loopback", "host_listener_hidden",
                                                "readonly_probe", "host_sentinel_hidden"))
        and all(proof["namespaces"][n] != config["parent"][n] for n in NS_NAMES))
    return result

def services_argv(case):
    require(case in SERVICES_SELECTORS, "unknown Services startup case")
    return ["/probe/services-test", SERVICES_SELECTORS[case], "--exact", "--ignored",
            "--nocapture", "--test-threads=1"]

def services_contract(case, original_namespaces):
    services_argv(case)
    require(set(original_namespaces) == set(NS_NAMES), "incomplete original namespaces")
    require(all(re.fullmatch(re.escape(name) + r":\[\d+\]", value)
                for name, value in original_namespaces.items()), "invalid original namespace identity")
    result = json.dumps({"format": "intent-services-native-startup-v1", "case": case,
                         "test_elf_sha256": SERVICES_SHA, "host_namespaces": original_namespaces},
                        sort_keys=True, separators=(",", ":")).encode() + b"\n"
    require(len(result) <= 4096, "Services contract exceeds bound")
    return result

def check_services_elf():
    path = checked_file(SERVICES_ELF, SERVICES_SHA, SERVICES_BYTES)
    require(stat.S_IMODE(path.stat().st_mode) == 0o555 and path.stat().st_uid == os.getuid(),
            "Services ELF ownership/mode mismatch")
    with path.open("rb") as stream:
        require(stream.read(6) == b"\x7fELF\x02\x01", "Services input is not the pinned ELF")
    return path

def check_services_fingerprints():
    """Authenticate current raw source/copy pairs; never round Cargo integers.

    The pinned collection and token index describe these current local files.
    Earlier binary64 metadata remains historical, without recovered raw tokens.
    """
    records = {}
    for name, size in SERVICES_FINGERPRINT_BYTES.items():
        data = source_bytes(SERVICES_PREPARATION / name, SERVICES_MANIFESTS[name])
        require(len(data) == size, "fingerprint manifest size changed")
        records[name] = json.loads(data)
    inventory = records["attribution/raw-fingerprints.json"]
    tokens = records["attribution/fingerprint-number-tokens.json"]
    qualification = records["attribution/fingerprint-qualification.json"]
    require(isinstance(inventory, dict) and len(inventory) == 944
            and isinstance(tokens, dict) and set(tokens) == set(inventory),
            "Cargo fingerprint inventory changed")
    source_root = SERVICES_PREPARATION.parents[1] / "target/debug"
    copy_root = SERVICES_PREPARATION / "attribution/fingerprint-raw"
    integer_count = large_count = 0
    for key, row in inventory.items():
        require(isinstance(row, dict) and set(row) == {"collectedAt", "source", "copy"}
                and isinstance(row["collectedAt"], str)
                and qualification["collectionStartedAt"] <= row["collectedAt"]
                <= qualification["collectionFinishedAt"], "invalid raw fingerprint record")
        try:
            relative = Path(key).relative_to(source_root)
        except ValueError as error:
            raise Refusal("foreign fingerprint source path") from error
        require(len(relative.parts) == 3 and relative.parts[0] == ".fingerprint"
                and all(part not in (".", "..") for part in relative.parts)
                and relative.suffix == ".json" and str(source_root / relative) == key,
                "invalid fingerprint source path")
        payloads = []
        for kind, expected_path in (("source", source_root / relative), ("copy", copy_root / relative)):
            item = row[kind]
            require(isinstance(item, dict)
                    and set(item) == {"path", "sha256", "bytes", "mode", "uid"}
                    and item["path"] == str(expected_path)
                    and type(item["bytes"]) is int and item["bytes"] > 0
                    and type(item["uid"]) is int and item["uid"] == os.getuid()
                    and isinstance(item["sha256"], str)
                    and re.fullmatch(r"[a-f0-9]{64}", item["sha256"])
                    and isinstance(item["mode"], str), "invalid raw fingerprint file identity")
            info = expected_path.lstat()
            require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1
                    and info.st_size == item["bytes"] and info.st_uid == item["uid"]
                    and oct(stat.S_IMODE(info.st_mode)) == item["mode"]
                    and (kind != "copy" or item["mode"] == "0o444"),
                    "raw fingerprint file metadata changed")
            data = source_bytes(expected_path, item["sha256"])
            after = expected_path.lstat()
            require((info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)
                    == (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns),
                    "raw fingerprint file replaced during verification")
            payloads.append(data)
        require(payloads[0] == payloads[1], "fingerprint source/copy bytes differ")
        data = payloads[0]
        json.loads(data)  # Python integers retain the exact decimal value.
        observed = []
        for match in re.finditer(rb'"(?:[^"\\]|\\.)*"|-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?', data):
            lexeme = match.group()
            if lexeme.startswith(b'"'):
                continue
            require(not any(c in lexeme for c in (b".", b"e", b"E")),
                    "unexpected noninteger Cargo fingerprint token")
            decimal = lexeme.decode("ascii")
            value = int(decimal)
            safe = abs(value) <= 9007199254740991
            observed.append({"byteOffset": match.start(), "lexeme": decimal, "integer": True,
                             "decimalInteger": str(value), "withinSafeInteger": safe})
            integer_count += 1
            large_count += not safe
        require(json.dumps(observed, sort_keys=True) == json.dumps(tokens[key], sort_keys=True),
                "fingerprint decimal token index changed")
    require(qualification["files"] == len(inventory)
            and qualification["integerTokenCount"] == integer_count
            and qualification["nonBinary64IntegerCount"] == large_count
            and qualification["parsedNumericMetadataRetained"] is False,
            "fingerprint collection qualification changed")
    return {"records": len(inventory), "integer_tokens": integer_count,
            "large_integer_tokens": large_count, "qualification": qualification}

def check_services_inputs():
    """Verify the accepted local compilation inputs; never build or launch them."""
    check_services_elf()
    raw = {name: source_bytes(SERVICES_PREPARATION / name, digest)
           for name, digest in SERVICES_MANIFESTS.items()}
    records = {name: json.loads(data) for name, data in raw.items() if name.endswith(".json")}
    sources = records["compile-v1/before.json"]
    require(sources == records["compile-v1/after.json"] and len(sources) == 1227,
            "frozen compiled source mismatch")
    root = Path(__file__).resolve().parent.parent
    for name, row in sources.items():
        path = root / name
        info = path.lstat()
        require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1,
                "non-regular compiled source")
        require(info.st_size == row["bytes"] and hash_file(path) == row["sha256"]
                and ("100755" if info.st_mode & 0o111 else "100644") == row["mode"],
                "composed source differs from compiled input: " + name)

    def local_input(row):
        path = Path(row["path"])
        if row.get("exists") is False:
            require(not path.exists() and not path.is_symlink(), "new Cargo configuration")
            return
        info = path.stat()
        require(stat.S_ISREG(info.st_mode) and info.st_size == row["bytes"]
                and oct(stat.S_IMODE(info.st_mode)) == row["mode"] and info.st_uid == row["uid"]
                and hash_file(path) == row["sha256"], "local compiled input changed: " + str(path))

    for name in ("dependency-files", "generated-compiler-inputs"):
        for row in records["attribution/" + name + ".json"].values():
            local_input(row)
    for name in ("toolchain-inputs", "cargo-config-search"):
        for row in records["attribution/" + name + ".json"]:
            local_input(row)
    fingerprints = check_services_fingerprints()
    for path, value in records["attribution/compiler-dependency-records.json"].items():
        require(Path(path).read_text() == value, "compiler dependency record changed")
    packages = records["attribution/dependency-packages.json"]
    files = records["attribution/dependency-files.json"]
    require(len(packages) == 404 and len(files) == 19472
            and all(row["registryChecksum"] is None and row["verifiedRegistryFiles"] == 0
                    for row in packages), "unexpected registry attribution")
    found = set()
    for package in packages:
        for directory, dirs, names in os.walk(package["root"]):
            dirs[:] = [name for name in dirs if name not in (".git", "target")]
            found.update(str(Path(directory) / name) for name in names)
    require(found == set(files), "external local dependency inventory changed")
    selected = records["attribution/cargo-selected.json"]
    cargo = [json.loads(line) for line in raw["compile-v1/compile.stdout"].splitlines()]
    require(cargo[-1] == {"reason": "build-finished", "success": True}
            and selected in cargo, "missing successful original compiler receipt")
    require(selected["features"] == [] and selected["profile"]["test"] is True,
            "unexpected Services build configuration")
    closure = records["attribution/os-closure.json"]
    require(closure["missingMounts"] == {} and closure["historicalMounts"] == 20,
            "unresolved Services OS closure")
    for row in closure["inspected"].values():
        if row["path"] == str(SERVICES_ELF):
            require(row["sha256"] == SERVICES_SHA and row["bytes"] == SERVICES_BYTES,
                    "Services attribution differs from independent pin")
        else:
            mount = OS_INPUTS["mounts"][row["logical"]]
            require(row["sha256"] == mount["sha256"] and row["bytes"] == mount["bytes"],
                    "Services requires an unpinned OS library")
    return {"elf_sha256": SERVICES_SHA, "elf_bytes": SERVICES_BYTES,
            "manifests": SERVICES_MANIFESTS, "compiled_source_entries": len(sources),
            "dependency_packages": len(packages), "local_dependency_files": len(files),
            "registry_checksum_verified_files": 0, "registry_provenance_or_reproducibility": False,
            "generated_inputs": len(records["attribution/generated-compiler-inputs.json"]),
            "fingerprints": fingerprints["records"], "raw_fingerprints": fingerprints,
            "toolchain_files": len(records["attribution/toolchain-inputs.json"])}

def services_milestones(stdout):
    result = []
    for line in stdout.splitlines():
        if '"nativeServicesStartup"' not in line:
            continue
        require(len(line.encode()) <= 16384, "oversized Services milestone")
        try:
            value = json.loads(line[line.index("{"):])
        except (ValueError, json.JSONDecodeError) as error:
            raise Refusal("malformed Services milestone") from error
        require(set(value) == {"nativeServicesStartup", "event", "facts"}
                and type(value["nativeServicesStartup"]) is int and value["nativeServicesStartup"] == 1
                and isinstance(value["facts"], dict), "invalid Services milestone shape")
        result.append(value)
    return result

def validate_services_result(case, result, deadline):
    selector = services_argv(case)[1]
    require(result["containment_passed"] is True, "Services namespace proof missing")
    require(result["returncode"] == 0 and result["reason"] == "exited"
            and result["elapsed_seconds"] <= deadline
            and result["live_original_processes_after_cleanup"] == [],
            "Services startup refused, failed, cancelled or bounded out")
    events = services_milestones(result["stdout"])
    require([event["event"] for event in events] == SERVICES_EVENTS[case],
            "missing, reordered, duplicate or unexpected Services milestones")
    facts = {event["event"]: event["facts"] for event in events}
    verified = facts["runner-verified"]
    require(verified.get("case") == case and verified.get("testElf") == SERVICES_SHA
            and verified.get("bridge") == BRIDGE_SHA
            and verified.get("runtimeIdentity") == "a08355be6f7f7aafd77977b0cdf030dc32f3fdce9178ec1883e0b04d71629597",
            "Services verified the wrong inputs")
    started = facts["ordinary-started"]
    require(started.get("legacy") is (case == "legacy")
            and all(isinstance(started.get(name), str) and started[name]
                    for name in ("sessionId", "agent", "workspace", "pendingEndpoint"))
            and type(started.get("adapterPid")) is int and started["adapterPid"] > 0,
            "ordinary original startup evidence missing")
    if case == "confirmed":
        ack, reused = facts["original-acknowledged"], facts["original-reused"]
        receipt = ack.get("receipt", "")
        require(isinstance(receipt, str) and len(receipt.encode()) <= 8192
                and "Acknowledged" in receipt and started["sessionId"] in receipt
                and ack.get("sessionId") == started["sessionId"]
                and ack.get("sameServicesReadAnchorConnection") is True
                and isinstance(ack.get("confirmedEndpoint"), str)
                and ack["confirmedEndpoint"] != started["pendingEndpoint"],
                "original acknowledged ownership evidence missing")
        require(reused.get("sameConnectionOriginSession") is True
                and reused.get("distinctOwnedCaptures") is True
                and type(reused.get("registrationCount")) is int and reused["registrationCount"] == 1,
                "original startup reuse evidence missing")
    retired = facts["owned-retired"]
    require(set(retired) == {"legacy", "handleAbsent", "originalConnectionReleased",
                             "pendingListenerRefused", "confirmedListenerRefused", "contextJobs"}
            and retired.get("legacy") is (case == "legacy") and retired.get("handleAbsent") is True
            and retired.get("originalConnectionReleased") is True
            and retired.get("pendingListenerRefused") is True
            and retired.get("confirmedListenerRefused") is (None if case == "legacy" else True)
            and retired.get("contextJobs") == "none prepared; drain completed"
            and facts["completed"].get("case") == case, "original retirement incomplete")
    lines = result["stdout"].splitlines()
    require(sum(line.startswith("test " + selector + " ... ") for line in lines) == 1,
            "missing exact selected libtest case")
    summaries = [line for line in lines if line.startswith("test result:")]
    require(len(summaries) == 1 and re.fullmatch(
        r"test result: ok\. 1 passed; 0 failed; 0 ignored; 0 measured; \d+ filtered out; finished in \d+(?:\.\d+)?s",
        summaries[0]), "missing one exact libtest pass")
    return events

def execute_services(args):
    case = args.case.removeprefix("services-")
    services_argv(case)
    require(args.bundle is not None and args.intentd is not None, "Services requires exact bundle and bridge")
    installer = load_installer()
    output = installer.absolute_directory(args.output)
    require(not output.exists() and output.parent.is_dir(), "new owned output directory required")
    check_os()
    inputs = check_services_inputs()
    contract = installer.configuration()
    installer.check_bundle(contract, args.bundle)
    checked_file(args.intentd, BRIDGE_SHA, BRIDGE_BYTES)
    require(stat.S_IMODE(args.intentd.stat().st_mode) == 0o555, "bridge mode mismatch")
    output.mkdir(mode=0o700)
    result = {"case": args.case, "inputs": inputs, "status": "starting",
              "services_sandbox_attempted": False, "services_milestones": [],
              "native_schedules": "not-reached", "namespace_before": namespaces()}
    payload = None
    def save():
        (output / "receipt.json").write_text(json.dumps(result, sort_keys=True, indent=2) + "\n")
    save()
    try:
        first = containment(output / "inert", min(args.deadline_seconds, 10), services_case=case)
        result["inert"] = first
        require(first["containment_passed"] and first["returncode"] == 0 and first["reason"] == "exited",
                "required inert namespace containment unavailable")
        launcher = installer.install(contract, args.bundle, output / "installed")
        payload = launcher.parent.parent
        installer.verify_install(contract, payload)
        checked_file(payload / NATIVE_REL, NATIVE_SHA, 233709640)
        result["services_sandbox_attempted"] = True
        result["native_schedules"] = "startup only; no prompt, private tool or model request"
        result["services"] = containment(output / "services", args.deadline_seconds,
                                         payload, args.intentd, services_case=case)
        result["services_milestones"] = services_milestones(result["services"]["stdout"])
        validate_services_result(case, result["services"], args.deadline_seconds)
        result["status"] = "completed"
    except (Exception, KeyboardInterrupt) as error:
        result["status"] = "refused"
        result["reason"] = str(error)[:2048]
    finally:
        try:
            if payload is not None:
                installer.verify_install(contract, payload)  # Includes exact owned marker.
                result["product_inventory_after"] = {"identity": contract.identity, "entries": 6367,
                                                      "owned_marker_verified": True}
            require(check_services_inputs() == inputs, "Services input attribution changed during execution")
            check_os()
            checked_file(args.intentd, BRIDGE_SHA, BRIDGE_BYTES)
            require(namespaces() == result["namespace_before"], "outer namespace changed")
            result["input_guards_after"] = True
        except (Exception, KeyboardInterrupt) as error:
            result["status"] = "refused"
            result["postcheck_error"] = str(error)[:2048]
        save()
    return 0 if result["status"] == "completed" else 1

def parser():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--case", choices=("containment", "native", "services-confirmed", "services-legacy"), required=True)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--bundle", type=Path)
    p.add_argument("--intentd", type=Path)
    p.add_argument("--deadline-seconds", type=int, default=40)
    return p

def execute(args):
    require(sys.flags.isolated and sys.flags.no_site, "use Python -I -S -B")
    require(1 <= args.deadline_seconds <= 45, "deadline must be 1..45 seconds")
    if args.case in ("services-confirmed", "services-legacy"):
        return execute_services(args)
    require((args.case == "native") == (args.bundle is not None and args.intentd is not None),
            "native case requires exact bundle and bridge; containment takes neither")
    if args.case == "containment":
        require(args.bundle is None and args.intentd is None, "unexpected payload option")
    installer = load_installer()
    output = installer.absolute_directory(args.output)
    require(not output.exists() and output.parent.is_dir(), "new owned output directory required")
    check_os()
    contract = installer.configuration()
    inputs = {"installer": INSTALLER_SHA, "descriptor": installer.DESCRIPTOR_SHA256,
              "probe_source": hash_file(Path(__file__)), "os": OS_INPUTS}
    if args.case == "native":
        installer.check_bundle(contract, args.bundle)
        checked_file(args.intentd, BRIDGE_SHA, BRIDGE_BYTES)
        client = source_bytes(Path(__file__).with_name("claude-callback-native-probe.mjs"), CLIENT_SHA)
        inputs.update({"artifact": contract.bundle, "bridge": BRIDGE_SHA, "client": CLIENT_SHA})
    output.mkdir(mode=0o700)
    result = {"case": args.case, "inputs": inputs, "native_sandbox_attempted": False,
              "native_schedules": "not-reached", "status": "starting"}
    def save():
        (output / "receipt.json").write_text(json.dumps(result, sort_keys=True, indent=2) + "\n")
    save()
    try:
        first = containment(output / "inert", min(args.deadline_seconds, 10))
        result["inert"] = first
        require(first["containment_passed"] and first["returncode"] == 0,
                "required inert namespace containment unavailable")
        if args.case == "native":
            launcher = installer.install(contract, args.bundle, output / "installed")
            payload = launcher.parent.parent
            installer.verify_install(contract, payload)
            checked_file(payload / NATIVE_REL, NATIVE_SHA, 233709640)
            result["native_sandbox_attempted"] = True
            result["native"] = containment(output / "native", args.deadline_seconds,
                                           payload, args.intentd, client)
            native = result["native"]
            result["payload_exec_permitted_after_containment"] = native["containment_passed"]
            require(native["containment_passed"], "native namespace containment failed before exec")
            result["native_schedules"] = "see original bounded ACP/MCP transcript"
            require(native["returncode"] == 0 and native["reason"] == "exited",
                    "native control refused, incomplete or bounded out; dependent cases not retried")
        result["status"] = "completed"
    except (Exception, KeyboardInterrupt) as error:
        result["status"] = "refused"
        result["reason"] = str(error)[:2048]
    finally:
        save()
    return 0 if result["status"] == "completed" else 1

def main(argv=None):
    args = parser().parse_args(argv)
    try:
        return execute(args)
    except Exception as error:
        print("probe refused before launch: " + str(error)[:2048], file=sys.stderr)
        return 1

if __name__ == "__main__":
    def interrupt(_signal, _frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, interrupt)
    sys.exit(main())
