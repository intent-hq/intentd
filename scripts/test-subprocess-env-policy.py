#!/usr/bin/env python3
"""Keep allowlisted subprocess environments able to start the test interpreter.

actions/setup-python ships shared-lib builds whose RUNPATH is
/opt/hostedtoolcache/Python/<version>/x64/lib. On self-hosted runners whose
tool cache lives elsewhere (tinybox: _work/_tool), the interpreter only starts
because setup-python exports LD_LIBRARY_PATH. A harness that runs
sys.executable (directly or through a ``#!{sys.executable}`` stub) with an
allowlisted ``env={...}`` that drops LD_LIBRARY_PATH then fails with exit 127
only on those runners (intentd#2488). This policy flags such environments
unless the file forwards LD_LIBRARY_PATH or the call carries
``# env-policy: no-python-child: <reason>`` because the child is not Python.
"""
import ast
from pathlib import Path
import re
import unittest

ROOT = Path(__file__).resolve().parents[1]
WAIVER = re.compile(r"#\s*env-policy:\s*no-python-child\b:?\s*(.*)$")


def is_closed_dict(node):
    """A dict literal (or dict(...) call) that cannot inherit os.environ."""
    if isinstance(node, ast.Dict):
        return None not in node.keys
    return (isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "dict"
            and not node.args and all(keyword.arg is not None for keyword in node.keywords))


def uses_sys_executable(tree):
    return any(isinstance(node, ast.Attribute) and node.attr == "executable"
               and isinstance(node.value, ast.Name) and node.value.id == "sys" for node in ast.walk(tree))


def forwards_ld_library_path(tree):
    return any(isinstance(node, ast.Constant) and node.value == "LD_LIBRARY_PATH" for node in ast.walk(tree))


def waiver(lines, *nodes):
    """The waiver reason on any line of the call or dict, '' if reasonless, None if absent."""
    for node in nodes:
        for line in lines[node.lineno - 1:node.end_lineno]:
            match = WAIVER.search(line)
            if match:
                return match[1].strip()
    return None


def violations(source, filename="fixture.py"):
    tree = ast.parse(source, filename)
    if not uses_sys_executable(tree) or forwards_ld_library_path(tree):
        return []
    assigned = {}
    for node in ast.walk(tree):
        if isinstance(node, (ast.Assign, ast.AnnAssign)) and node.value is not None and is_closed_dict(node.value):
            for target in node.targets if isinstance(node, ast.Assign) else [node.target]:
                if isinstance(target, (ast.Name, ast.Attribute)):
                    assigned.setdefault(ast.unparse(target), []).append(node.value)
    lines = source.splitlines()
    errors = []
    for call in ast.walk(tree):
        if not isinstance(call, ast.Call):
            continue
        for keyword in call.keywords:
            if keyword.arg != "env":
                continue
            value = keyword.value
            if is_closed_dict(value):
                candidates = [value]
            elif isinstance(value, (ast.Name, ast.Attribute)):
                candidates = assigned.get(ast.unparse(value), [])
            else:
                candidates = []
            for env in candidates:
                reason = waiver(lines, call, env)
                if reason:
                    continue
                problem = "waiver needs a reason" if reason == "" else "drops LD_LIBRARY_PATH"
                errors.append(f"{filename}:{call.lineno}: env {problem} (allowlisted env from line {env.lineno})")
    return sorted(set(errors))


class SubprocessEnvPolicy(unittest.TestCase):
    def test_repository_scripts_forward_ld_library_path(self):
        found = []
        for path in sorted((ROOT / "scripts").rglob("*.py")):
            found += violations(path.read_text(), str(path.relative_to(ROOT)))
        self.assertEqual(found, [], "\nForward LD_LIBRARY_PATH when it is set (see intentd#2488), "
                         "or mark a non-Python child with '# env-policy: no-python-child: <reason>'.")

    def test_flags_direct_literal_env(self):
        source = ("import subprocess, sys\n"
                  "subprocess.run([sys.executable, '-c', 'pass'], env={'PATH': '/usr/bin:/bin'})\n")
        self.assertEqual(violations(source), ["fixture.py:2: env drops LD_LIBRARY_PATH (allowlisted env from line 2)"])

    def test_flags_attribute_and_name_envs_and_shebang_stubs(self):
        source = ("import subprocess, sys\n"
                  "class T:\n"
                  "    def setUp(self):\n"
                  "        self.env = {'PATH': '/stub/bin', 'HOME': '/tmp'}\n"
                  "        stub = f'#!{sys.executable} -S\\n'\n"
                  "    def run(self):\n"
                  "        subprocess.run(['stub'], env=self.env)\n"
                  "env = dict(PATH='/usr/bin')\n"
                  "subprocess.Popen(['stub'], env=env)\n")
        self.assertEqual(violations(source), [
            "fixture.py:7: env drops LD_LIBRARY_PATH (allowlisted env from line 4)",
            "fixture.py:9: env drops LD_LIBRARY_PATH (allowlisted env from line 8)"])

    def test_allows_inherited_forwarded_and_unrelated_envs(self):
        inherited = ("import os, subprocess, sys\n"
                     "subprocess.run([sys.executable], env={**os.environ, 'X': '1'})\n"
                     "subprocess.run([sys.executable], env=dict(os.environ, X='1'))\n")
        forwarded = ("import os, subprocess, sys\n"
                     "env = {'PATH': '/usr/bin'}\n"
                     "if 'LD_LIBRARY_PATH' in os.environ:\n"
                     "    env['LD_LIBRARY_PATH'] = os.environ['LD_LIBRARY_PATH']\n"
                     "subprocess.run([sys.executable], env=env)\n")
        no_interpreter = "import subprocess\nsubprocess.run(['true'], env={'PATH': '/usr/bin'})\n"
        for source in (inherited, forwarded, no_interpreter):
            with self.subTest(source=source):
                self.assertEqual(violations(source), [])

    def test_waiver_requires_a_reason(self):
        waived = ("import subprocess, sys\n"
                  "subprocess.run(['bwrap', sys.executable],\n"
                  "               env={'PATH': '/usr/bin'})  # env-policy: no-python-child: real bubblewrap\n")
        reasonless = waived.replace(": real bubblewrap", "")
        self.assertEqual(violations(waived), [])
        self.assertEqual(violations(reasonless), [
            "fixture.py:2: env waiver needs a reason (allowlisted env from line 3)"])


if __name__ == "__main__":
    unittest.main()
