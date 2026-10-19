#!/usr/bin/env python3
"""Keep allowlisted subprocess environments able to start the test interpreter.

actions/setup-python ships shared-lib builds whose RUNPATH is
/opt/hostedtoolcache/Python/<version>/x64/lib. On self-hosted runners whose
tool cache lives elsewhere (tinybox: _work/_tool), the interpreter only starts
because setup-python exports LD_LIBRARY_PATH. A harness that runs the
interpreter (sys.executable, a ``#!{sys.executable}`` stub, or python3 from
PATH) with an allowlisted ``env={...}`` that drops LD_LIBRARY_PATH then fails
with exit 127 only on those runners (intentd#2490).

A closed env (a dict literal or ``dict(...)`` that cannot inherit os.environ)
reaching ``env=`` is accepted when the function that builds it forwards
LD_LIBRARY_PATH (the literal, or a module constant such as a LOADER_PATH tuple
that contains it), or when the call itself carries
``# env-policy: no-python-child: <reason>`` because its child is not Python.
Calls that pass a same-file helper's result (``env=child_env(...)``) are
checked against that helper's body. The scan gates on the file being able to
start Python at all; it does not follow ``{**base}`` chains or imports.
"""
import ast
import io
from pathlib import Path
import re
import tokenize
import unittest

ROOT = Path(__file__).resolve().parents[1]
LOADER = "LD_LIBRARY_PATH"
MARKER = re.compile(r"#\s*env-policy\b")
WAIVER = re.compile(r"#\s*env-policy: no-python-child: (\S.*)$")
PYTHON_COMMAND = re.compile(r"(?:.*/)?python(?:3(?:\.\d+)?)?$|#!.*\bpython")
FUNCTIONS = (ast.FunctionDef, ast.AsyncFunctionDef)


def is_closed_dict(node):
    """A dict literal or dict(...) call whose keys are all spelled out."""
    if isinstance(node, ast.Dict):
        return None not in node.keys
    return (isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "dict"
            and all(keyword.arg is not None for keyword in node.keywords)
            and all(is_closed_dict(arg) for arg in node.args))


def starts_python(tree):
    """The file can start an interpreter: sys.executable, `from sys import executable`, or python3."""
    for node in ast.walk(tree):
        if (isinstance(node, ast.Attribute) and node.attr == "executable"
                and isinstance(node.value, ast.Name) and node.value.id == "sys"):
            return True
        if isinstance(node, ast.ImportFrom) and node.module == "sys" and any(a.name == "executable" for a in node.names):
            return True
        if isinstance(node, ast.Constant) and isinstance(node.value, str) and PYTHON_COMMAND.match(node.value):
            return True
    return False


def forwards_loader(scope, loader_names):
    """The scope names LD_LIBRARY_PATH (or a constant holding it) other than to delete it."""
    removed = set()
    for node in ast.walk(scope):
        if isinstance(node, ast.Delete):
            removed |= {id(n) for target in node.targets for n in ast.walk(target)}
        elif isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "pop":
            removed |= {id(n) for arg in node.args for n in ast.walk(arg)}
    return any(id(node) not in removed and (
        (isinstance(node, ast.Constant) and node.value == LOADER)
        or (isinstance(node, ast.Name) and node.id in loader_names)) for node in ast.walk(scope))


def owners(tree):
    """Map each node id to its innermost enclosing function (or module) and class (or module)."""
    found = {}

    def walk(node, function, klass):
        for child in ast.iter_child_nodes(node):
            found[id(child)] = (function, klass)
            walk(child, child if isinstance(child, FUNCTIONS) else function,
                 child if isinstance(child, ast.ClassDef) else klass)

    walk(tree, tree, tree)
    return found


def violations(source, filename="fixture.py"):
    try:
        tree = ast.parse(source, filename)
    except SyntaxError as error:
        return [f"{filename}:{error.lineno}: cannot parse for the env policy: {error.msg}"]
    comments = {token.start[0]: token.string for token in tokenize.generate_tokens(io.StringIO(source).readline)
                if token.type == tokenize.COMMENT}
    errors = [f"{filename}:{number}: malformed waiver; use '# env-policy: no-python-child: <reason>'"
              for number, comment in comments.items() if MARKER.search(comment) and not WAIVER.search(comment)]
    if not starts_python(tree):
        return errors
    loader_names = {target.id for node in tree.body if isinstance(node, ast.Assign)
                    and any(isinstance(n, ast.Constant) and n.value == LOADER for n in ast.walk(node.value))
                    for target in node.targets if isinstance(target, ast.Name)}
    owner = owners(tree)
    helpers, bound = {}, {}
    for node in ast.walk(tree):
        if isinstance(node, FUNCTIONS):
            helpers.setdefault(node.name, node)
        if isinstance(node, (ast.Assign, ast.AnnAssign)) and node.value is not None and is_closed_dict(node.value):
            function, klass = owner[id(node)]
            for target in node.targets if isinstance(node, ast.Assign) else [node.target]:
                if isinstance(target, ast.Name):
                    bound.setdefault((id(function), target.id), []).append((node.value, function))
                elif isinstance(target, ast.Attribute) and isinstance(target.value, ast.Name) and target.value.id == "self":
                    bound.setdefault((id(klass), "self." + target.attr), []).append((node.value, function))
    for call in ast.walk(tree):
        if not isinstance(call, ast.Call):
            continue
        for keyword in call.keywords:
            if keyword.arg != "env":
                continue
            function, klass = owner[id(call)]
            value = keyword.value
            if is_closed_dict(value):
                candidates = [(value, function)]
            elif isinstance(value, ast.Name):
                candidates = bound.get((id(function), value.id), [])
            elif isinstance(value, ast.Attribute) and isinstance(value.value, ast.Name) and value.value.id == "self":
                candidates = bound.get((id(klass), "self." + value.attr), [])
            elif isinstance(value, ast.Call) and isinstance(value.func, ast.Name) and value.func.id in helpers:
                helper = helpers[value.func.id]
                candidates = [(node, helper) for node in ast.walk(helper) if is_closed_dict(node)]
            else:
                candidates = []
            if any(WAIVER.search(comments.get(number, "")) for number in range(call.lineno, call.end_lineno + 1)):
                continue
            errors += [f"{filename}:{call.lineno}: env drops {LOADER} (allowlisted env from line {env.lineno})"
                       for env, scope in candidates if not forwards_loader(scope, loader_names)]
    return sorted(set(errors), key=lambda e: (int(e.split(":")[1]), e))


class SubprocessEnvPolicy(unittest.TestCase):
    maxDiff = None

    def test_repository_scripts_forward_ld_library_path(self):
        found = []
        for path in sorted((ROOT / "scripts").rglob("*.py")):
            found += violations(path.read_text(encoding="utf-8"), str(path.relative_to(ROOT)))
        self.assertEqual(found, [], "\nForward LD_LIBRARY_PATH when it is set (see intentd#2490), "
                         "or mark a non-Python child with '# env-policy: no-python-child: <reason>'.")

    def test_flags_direct_literal_env(self):
        source = ("import subprocess, sys\n"
                  "subprocess.run([sys.executable, '-c', 'pass'], env={'PATH': '/usr/bin:/bin'})\n")
        self.assertEqual(violations(source), ["fixture.py:2: env drops LD_LIBRARY_PATH (allowlisted env from line 2)"])

    def test_flags_attribute_name_and_helper_envs(self):
        source = ("import subprocess, sys\n"
                  "def child_env():\n"
                  "    return {'PATH': '/usr/bin'}\n"
                  "class T:\n"
                  "    def setUp(self):\n"
                  "        self.env = {'PATH': '/stub/bin', 'HOME': '/tmp'}\n"
                  "        stub = f'#!{sys.executable} -S\\n'\n"
                  "    def run(self):\n"
                  "        subprocess.run(['stub'], env=self.env)\n"
                  "        subprocess.run(['stub'], env=child_env())\n"
                  "env = dict({'PATH': '/usr/bin'})\n"
                  "subprocess.Popen(['stub'], env=env)\n")
        self.assertEqual(violations(source), [
            "fixture.py:9: env drops LD_LIBRARY_PATH (allowlisted env from line 6)",
            "fixture.py:10: env drops LD_LIBRARY_PATH (allowlisted env from line 3)",
            "fixture.py:12: env drops LD_LIBRARY_PATH (allowlisted env from line 11)"])

    def test_python_from_path_and_imported_executable_count(self):
        for launch, imports in (("['python3', 'x.py']", ""), ("[executable, 'x.py']", "from sys import executable\n"),
                                ("['/opt/py/bin/python3.11']", "")):
            source = f"import subprocess\n{imports}subprocess.run({launch}, env={{'PATH': '/usr/bin'}})\n"
            with self.subTest(launch=launch):
                self.assertEqual(len(violations(source)), 1)

    def test_forwarding_is_scoped_to_the_function_building_the_env(self):
        source = ("import os, subprocess, sys\n"
                  "def forwarded():\n"
                  "    env = {'PATH': '/usr/bin'}\n"
                  "    env['LD_LIBRARY_PATH'] = os.environ.get('LD_LIBRARY_PATH', '')\n"
                  "    subprocess.run([sys.executable], env=env)\n"
                  "def dropped():\n"
                  "    env = {'PATH': '/usr/bin'}\n"
                  "    env.pop('LD_LIBRARY_PATH', None)\n"
                  "    subprocess.run([sys.executable], env=env)\n"
                  "def inherited():\n"
                  "    env = dict(os.environ)\n"
                  "    subprocess.run([sys.executable], env=env)\n")
        self.assertEqual(violations(source), ["fixture.py:9: env drops LD_LIBRARY_PATH (allowlisted env from line 7)"])

    def test_loader_tuple_constant_counts_as_forwarding(self):
        source = ("import os, subprocess, sys\n"
                  "LOADER_PATH = ('LD_LIBRARY_PATH', 'DYLD_LIBRARY_PATH')\n"
                  "def child_env():\n"
                  "    env = {'PATH': '/usr/bin'}\n"
                  "    env.update({n: os.environ[n] for n in LOADER_PATH if n in os.environ})\n"
                  "    return env\n"
                  "subprocess.run([sys.executable], env=child_env())\n")
        self.assertEqual(violations(source), [])
        self.assertEqual(len(violations(source.replace("    env.update", "    # env.update"))), 1)

    def test_allows_inherited_and_unrelated_envs(self):
        inherited = ("import os, subprocess, sys\n"
                     "subprocess.run([sys.executable], env={**os.environ, 'X': '1'})\n"
                     "subprocess.run([sys.executable], env=dict(os.environ, X='1'))\n")
        no_interpreter = "import subprocess\nsubprocess.run(['true'], env={'PATH': '/usr/bin'})\n"
        for source in (inherited, no_interpreter):
            with self.subTest(source=source):
                self.assertEqual(violations(source), [])

    def test_waiver_applies_to_its_call_only_and_needs_a_reason(self):
        waived = ("import subprocess, sys\n"
                  "class T:\n"
                  "    def setUp(self):\n"
                  "        self.env = {'PATH': '/usr/bin'}  # env-policy: no-python-child: shared env\n"
                  "    def a(self):\n"
                  "        subprocess.run(['bwrap'], env=self.env)  # env-policy: no-python-child: real bubblewrap\n"
                  "    def b(self):\n"
                  "        subprocess.run([sys.executable], env=self.env)\n")
        self.assertEqual(violations(waived), ["fixture.py:8: env drops LD_LIBRARY_PATH (allowlisted env from line 4)"])
        for marker in ("# env-policy: no-python-child", "# env-policy: no-python-child-ish: x",
                       "# env-policy: no-python-child because"):
            source = f"import subprocess\nsubprocess.run(['true'])  {marker}\n"
            with self.subTest(marker=marker):
                self.assertEqual(violations(source), [
                    "fixture.py:2: malformed waiver; use '# env-policy: no-python-child: <reason>'"])

    def test_unparsable_file_is_reported_not_raised(self):
        self.assertEqual(violations("def broken(:\n"), [
            "fixture.py:1: cannot parse for the env policy: invalid syntax"])


if __name__ == "__main__":
    unittest.main()
