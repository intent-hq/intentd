#!/usr/bin/env python3
"""List the scripts/ helpers and sibling files a script uses, transitively.

Test harnesses that run a script in an isolated scratch checkout (a fixture
repo with a minimal PATH) must copy every helper that script calls. A
hand-kept copy list breaks with "No such file or directory" (exit 127) the
first time the script starts calling a new helper: intentd#2896 added
ci-killed-rustc-retry.sh to changed-tests.sh and coverage-*.sh, and only the
full local release-scripts run caught the missing copy in the
test-changed-tests.sh and test-test-policy.py harnesses. Those harnesses, and
the optional-Intel release and review-runtime harnesses, now copy the list
this module derives instead.

A helper call is a `NAME.sh` / `NAME.py` path under `scripts/` (bare or after
`$repo_root/`, `"$(dirname "$0")/.."` and the like) or a `$var/NAME.sh` /
`$var/NAME.py` path where scripts/NAME exists (`"$here/x.sh"`,
`"$script_dir/x.sh"`). Whole-line `#` comments are skipped. A `scripts/` path
naming a file that does not exist is an error that names the caller and the
helper, so a harness fails fast instead of the script failing later with
exit 127 deep inside a test case. Mentions in docstrings count too, so this
one names files without their `scripts/` prefix: every copy_into harness
loads this module, so harness closures reach it.

A Python script also loads siblings from its own directory, helpers and data
files alike (reconcile-sitter-intel.py imports optional-intel-release.py;
prepare-claude-review-runtime.py reads claude-review-runtime.json). A sibling
load is a quoted file name passed to `Path(__file__).with_name(...)` or
joined as `Path(__file__).parent / ...` or `DIR / ...`, where the script binds
`DIR = Path(__file__).parent` on its own line (`.resolve()` allowed in each).
Paths from `.parents[1]`, `.parent.parent` or any other variable are outside
the script directory and ignored. A sibling load naming a file that does not
exist is the same fail-fast error. Only `.sh` and `.py` files are scanned:
data files (`.json`, `.patch`, `.ps1`, ...) are copied as leaves.

Usage: python3 -I -B scripts/script_helpers.py SCRIPT...
Prints each named script and every helper it reaches, one name per line,
sorted. Exit 0 on success, 1 when a called helper is missing, 2 on usage
errors.
"""

from pathlib import Path
import re
import shutil
import sys


SCRIPTS = Path(__file__).resolve().parent
NAME = r'([A-Za-z0-9_][A-Za-z0-9_.-]*\.(?:sh|py))\b'
# A literal scripts/ path names a helper of this repo: it must exist.
LITERAL = re.compile(r'(?<![A-Za-z0-9_.-])scripts/' + NAME)
# A $var/ path may point anywhere (a temp dir, the caller's cwd); it is a
# helper only when scripts/ has a file of that name.
VARIABLE = re.compile(r'\$\{?[A-Za-z_][A-Za-z0-9_]*\}?/' + NAME)
# Python sibling loads: any file name, quoted, resolved against the script's
# own directory.
SIBLING = r'([\'"])([A-Za-z0-9_][A-Za-z0-9_.-]*)\1'
HERE = r'Path\(__file__\)(?:\.resolve\(\))?'
WITH_NAME = re.compile(HERE + r'\.with_name\(\s*' + SIBLING)
INLINE_DIR = re.compile(HERE + r'\.parent\s*/\s*' + SIBLING)
DIR_BINDING = re.compile(r'^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*' + HERE + r'\.parent\s*(?:#.*)?$')
SCANNED = ('.sh', '.py')


class MissingHelper(Exception):
    pass


def calls(path):
    """The helper and sibling names one script calls or loads (not transitive)."""
    if path.suffix not in SCANNED:
        return set()
    lines = [
        (number, line)
        for number, line in enumerate(path.read_text(encoding='utf-8').splitlines(), 1)
        if not line.lstrip().startswith('#')
    ]
    directories = {match.group(1) for _, line in lines if (match := DIR_BINDING.match(line))}
    in_directory = None
    if directories:
        in_directory = re.compile(
            r'(?<![A-Za-z0-9_.])(?:' + '|'.join(sorted(directories)) + r')\s*/\s*' + SIBLING
        )
    found = {}
    for number, line in lines:
        for match in LITERAL.finditer(line):
            found.setdefault(match.group(1), (number, f'calls scripts/{match.group(1)}'))
        for match in VARIABLE.finditer(line):
            found.setdefault(match.group(1), (number, None))
        for pattern in (WITH_NAME, INLINE_DIR, in_directory):
            for match in pattern.finditer(line) if pattern else ():
                found.setdefault(match.group(2), (number, f'loads {match.group(2)}'))
    names = set()
    for name, (number, strict) in found.items():
        if (path.parent / name).is_file():
            names.add(name)
        elif strict:
            raise MissingHelper(
                f'{path.name}:{number} {strict}, which does not exist in {path.parent}'
            )
    return names


def closure(roots, scripts=SCRIPTS):
    """Every name in roots plus every helper they reach, sorted."""
    scripts = Path(scripts)
    seen, pending = set(), list(roots)
    while pending:
        name = pending.pop()
        if name in seen:
            continue
        path = scripts / name
        if not path.is_file():
            raise MissingHelper(f'scripts/{name} does not exist in {scripts}')
        seen.add(name)
        pending.extend(calls(path) - seen)
    return sorted(seen)


def copy_into(roots, destination, scripts=SCRIPTS):
    """Copy roots and their helpers into destination; return the names."""
    names = closure(roots, scripts)
    for name in names:
        shutil.copy2(Path(scripts) / name, Path(destination) / name)
    return names


def main(argv):
    if not argv or any(arg.startswith('-') for arg in argv):
        print('Usage: script_helpers.py SCRIPT...', file=sys.stderr)
        return 2
    try:
        names = closure(argv)
    except MissingHelper as error:
        print(f'error: {error}', file=sys.stderr)
        return 1
    print('\n'.join(names))
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
