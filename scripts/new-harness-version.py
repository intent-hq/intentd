#!/usr/bin/env python3
"""Mint a new harness version that starts as a copy of the current one.

Makes every routine edit of a harness version bump in one step, so the only
hand edit left is the new version's own change (doctrine, text surfaces or
feature defaults) in the new module:

1. intent-core model.rs: CURRENT_HARNESS_VERSION, and the new version appended
   to REPOSITORY_GUIDANCE_HARNESS_VERSIONS and
   REPOSITORY_MONITOR_GUIDANCE_HARNESS_VERSIONS.
2. intent-core prompt_breakdown.rs: harness_records_prompt_breakdown admits the
   new version, and its test asserts it (the not-admitted sample moves on).
3. intent-services context_budget.rs: PREVIOUS is the version being replaced.
4. intent-services harness/mod.rs: the module line and the REGISTRY row.
5. harness/v<FROM>.rs: its test no longer pins FROM as the current version.
6. harness/v<TO>.rs (new): reuses every FROM surface; its test pins TO as current.
7. With --docs (the make target passes it inside the monorepo): both version
   lists in the Prompt breakdown section of docs/protocol/methods/agents.md.
   That docs change is a separate monorepo PR that must merge first.

The admissions in 1 and 2 keep FROM's: FROM must already be admitted. A
version that should drop repository guidance or the prompt breakdown is not a
routine bump; edit those lists by hand afterwards.

Nothing is written unless every edit applies. Exit 0 on success, 1 when the
tree does not match (a missing anchor, an unexpected current version), 2 on a
usage error.

Usage (from packages/intentd): make new-harness-version FROM=3.15 TO=3.16
   or: python3 -I -B scripts/new-harness-version.py --from 3.15 --to 3.16 [--docs PATH] [--dry-run]
"""
import argparse
import os
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[1]
MODEL = "crates/intent-core/src/model.rs"
BREAKDOWN = "crates/intent-core/src/prompt_breakdown.rs"
BUDGET = "crates/intent-services/src/context_budget.rs"
HARNESS_DIR = "crates/intent-services/src/harness"
HARNESS_MOD = HARNESS_DIR + "/mod.rs"
GUIDANCE_LISTS = ("REPOSITORY_GUIDANCE_HARNESS_VERSIONS", "REPOSITORY_MONITOR_GUIDANCE_HARNESS_VERSIONS")
MAX_WIDTH = 100
# rustfmt fn_call_width: longer call arguments go one per line.
CALL_WIDTH = 60
VERSION = re.compile(r"^(\d+)\.(\d+)$")


class Mismatch(Exception):
    pass


def parse(version):
    match = VERSION.match(version)
    if not match:
        raise ValueError(f"{version!r} is not MAJOR.MINOR")
    return int(match.group(1)), int(match.group(2))


def module(version):
    return "v" + version.replace(".", "_")


def next_minor(version):
    major, minor = parse(version)
    return f"{major}.{minor + 1}"


def quoted(version):
    return f'"{version}"'


def sub_once(pattern, replacement, text, what, flags=0):
    new, count = re.subn(pattern, replacement, text, count=1, flags=flags)
    if count != 1:
        raise Mismatch(f"{what}: anchor not found")
    return new


def pack(items, indent="    "):
    """rustfmt's layout for a long array of short literals: fill each line to
    MAX_WIDTH, trailing comma."""
    lines, line = [], ""
    for item in items:
        candidate = f"{line} {item}," if line else f"{indent}{item},"
        if line and len(candidate) > MAX_WIDTH:
            lines.append(line)
            candidate = f"{indent}{item},"
        line = candidate
    lines.append(line)
    return "\n".join(lines)


def append_to_list(text, name, old, new):
    pattern = re.compile(rf"(pub const {name}: &\[&str\] = &\[\n)(.*?)(\n\];)", re.S)
    match = pattern.search(text)
    if not match:
        raise Mismatch(f"{MODEL}: {name} not found")
    items = re.findall(r'"[^"]*"', match.group(2))
    if not items or items[-1] != quoted(old):
        raise Mismatch(f"{MODEL}: {name} does not end with {quoted(old)}")
    return text[:match.start(2)] + pack(items + [quoted(new)]) + text[match.end(2):]


def edit_model(text, old, new):
    text = sub_once(rf'(pub const CURRENT_HARNESS_VERSION: &str = ){re.escape(quoted(old))};',
                    rf'\g<1>{quoted(new)};', text, f"{MODEL}: CURRENT_HARNESS_VERSION = {quoted(old)}")
    if not re.search(r"pub const STAGED_HARNESS_VERSION: Option<&str> = None;", text):
        raise Mismatch(f"{MODEL}: STAGED_HARNESS_VERSION is set; promote or clear the staged version by hand first")
    for name in GUIDANCE_LISTS:
        text = append_to_list(text, name, old, new)
    return text


def edit_breakdown(text, old, new):
    pattern = re.compile(r"( *)matches!\(\s*harness_version,\s*([^)]*?),?\s*\)")
    match = pattern.search(text)
    if not match:
        raise Mismatch(f"{BREAKDOWN}: matches!(harness_version, ...) not found")
    indent, versions = match.group(1), [v.strip() for v in match.group(2).split("|")]
    if versions[-1] != quoted(old):
        raise Mismatch(f"{BREAKDOWN}: harness_records_prompt_breakdown does not end with {quoted(old)}")
    alternatives = " | ".join(versions + [quoted(new)])
    one_line = f"{indent}matches!(harness_version, {alternatives})"
    if len(one_line) <= MAX_WIDTH and len(f"harness_version, {alternatives}") <= CALL_WIDTH:
        call = one_line
    else:
        inner = indent + "    "
        if len(inner + alternatives) > MAX_WIDTH:
            # rustfmt then puts each further alternative on its own line.
            alternatives = f"\n{inner}    | ".join(versions + [quoted(new)])
        call = f"{indent}matches!(\n{inner}harness_version,\n{inner}{alternatives}\n{indent})"
    text = text[:match.start()] + call + text[match.end():]

    text = sub_once(rf"^( *)assert!\(harness_records_prompt_breakdown\({re.escape(quoted(old))}\)\);\n",
                    rf"\g<0>\g<1>assert!(harness_records_prompt_breakdown({quoted(new)}));\n",
                    text, f"{BREAKDOWN}: assert!(harness_records_prompt_breakdown({quoted(old)}))", re.M)
    # The not-admitted samples must not include the new version.
    samples = re.search(r"for version in \[(.*?)\]", text, re.S)
    if not samples:
        raise Mismatch(f"{BREAKDOWN}: the not-admitted `for version in [...]` samples not found")
    if quoted(new) in samples.group(1):
        replacement = quoted(next_minor(new))
        if replacement in samples.group(1):
            raise Mismatch(f"{BREAKDOWN}: the not-admitted samples already name {replacement}")
        text = text[:samples.start(1)] + samples.group(1).replace(quoted(new), replacement) + text[samples.end(1):]
    return text


def edit_budget(text, old):
    return sub_once(r'(const PREVIOUS: &str = )"[^"]*";', rf'\g<1>{quoted(old)};', text, f"{BUDGET}: const PREVIOUS")


def mod_sort_key(line):
    return line.strip().removeprefix("pub(crate) mod ").removesuffix(";")


def edit_harness_mod(text, old, new):
    lines = text.split("\n")
    mods = [i for i, line in enumerate(lines) if re.fullmatch(r"pub\(crate\) mod v\d+(?:_\d+)?;", line)]
    if not mods or mods != list(range(mods[0], mods[-1] + 1)):
        raise Mismatch(f"{HARNESS_MOD}: the `pub(crate) mod v*;` lines are not one block")
    names = [mod_sort_key(lines[i]) for i in mods]
    if module(old) not in names:
        raise Mismatch(f"{HARNESS_MOD}: no `pub(crate) mod {module(old)};`")
    if module(new) in names:
        raise Mismatch(f"{HARNESS_MOD}: `pub(crate) mod {module(new)};` already exists")
    # rustfmt's reorder_modules (style edition 2021) sorts by plain string order.
    at = mods[0] + sum(1 for name in names if name < module(new))
    lines.insert(at, f"pub(crate) mod {module(new)};")
    text = "\n".join(lines)
    return sub_once(rf"(static REGISTRY: &\[&HarnessEntry\] = &\[\n(?:    .*\n)*    &{module(old)}::ENTRY,\n)(\];)",
                    rf"\g<1>    &{module(new)}::ENTRY,\n\g<2>", text,
                    f"{HARNESS_MOD}: REGISTRY ending with &{module(old)}::ENTRY")


def edit_previous_module(text, old):
    """The FROM module's test stops claiming FROM is current."""
    pattern = (r"assert_eq!\((\w+)\.version, intent_core::CURRENT_HARNESS_VERSION\);\n"
               r"\s*assert_eq!\(intent_core::STAGED_HARNESS_VERSION, None\);")
    new, count = re.subn(pattern, rf"assert_eq!(\g<1>.version, {quoted(old)});", text)
    if count == 0 and "CURRENT_HARNESS_VERSION" in text:
        raise Mismatch(f"{module(old)}.rs mentions CURRENT_HARNESS_VERSION but not in the expected test lines")
    return new


def entry_field(text, old, field):
    """FROM's own value for a field. default_features and feature_labels are
    only read in tests, so reading them through FROM's ENTRY would leave
    their `expect(dead_code)` unfulfilled: copy the value instead."""
    entry = re.search(r"static ENTRY: HarnessEntry = HarnessEntry \{\n(.*?)\n\};", text, re.S)
    value = entry and re.search(rf"^\s*{field}: (.+),$", entry.group(1), re.M)
    if not value:
        raise Mismatch(f"{module(old)}.rs: no `{field}:` in its ENTRY")
    value = value.group(1)
    if ".ENTRY." in value:
        raise Mismatch(f"{module(old)}.rs: `{field}` reads another ENTRY; give it its own value first")
    if re.fullmatch(r"[A-Za-z_]\w*", value):
        value = f"super::{module(old)}::{value}"
    return value


def new_module(old, new, previous_text):
    prior = f"super::{module(old)}::ENTRY"
    default_features = entry_field(previous_text, old, "default_features")
    feature_labels = entry_field(previous_text, old, "feature_labels")
    return f'''//! Harness {new}: {old} with no changes yet. Override here only what {new}
//! changes; every other surface stays {old}'s.

use super::HarnessEntry;

pub(crate) static ENTRY: HarnessEntry = HarnessEntry {{
    version: {quoted(new)},
    harness: {prior}.harness,
    doctrine: {prior}.doctrine,
    context: {prior}.context,
    default_features: {default_features},
    feature_labels: {feature_labels},
}};

#[cfg(test)]
mod tests {{
    /// Pin what {new} keeps from {old}; replace a line with the change it makes.
    #[test]
    fn {module(new)}_is_{module(old).removeprefix("v")}_unchanged() {{
        let current = super::super::resolve_entry({quoted(new)});
        let prior = super::super::resolve_entry({quoted(old)});
        assert_eq!(current.version, intent_core::CURRENT_HARNESS_VERSION);
        assert_eq!(intent_core::STAGED_HARNESS_VERSION, None);
        assert!(std::ptr::addr_eq(current.harness, prior.harness));
        assert!(std::ptr::eq(current.doctrine, prior.doctrine));
        assert!(std::ptr::eq(current.context, prior.context));
        assert_eq!((current.default_features)(), (prior.default_features)());
        assert_eq!(current.feature_labels, prior.feature_labels);
        assert!(intent_core::prompt_breakdown::harness_records_prompt_breakdown({quoted(new)}));
    }}
}}
'''


def edit_docs(text, old, new):
    """Extend the two English version lists ("3.13, 3.14 and 3.15") in the
    Prompt breakdown section."""
    start = text.find("#### Prompt breakdown\n")
    if start < 0:
        raise Mismatch("agents.md: no '#### Prompt breakdown' section")
    end = text.find("\n#", start + 1)
    end = len(text) if end < 0 else end
    section = text[start:end]
    version = r"\d+\.\d+"
    pattern = re.compile(rf"((?<![\d.]){version}(?:,\s+{version})*)(\s+)and(\s+){re.escape(old)}(?![\d]|\.\d)")
    section, count = pattern.subn(lambda m: f"{m.group(1)},{m.group(2)}{old} and{m.group(3)}{new}", section)
    if count != 2:
        raise Mismatch(f"agents.md: expected 2 version lists ending in {old} in Prompt breakdown, found {count}")
    return text[:start] + section + text[end:]


def plan(root, old, new, docs):
    """Map path -> new content for every edit, or raise Mismatch."""
    def read(relative):
        path = root / relative
        if not path.is_file():
            raise Mismatch(f"{relative}: not found")
        return path.read_text()

    new_path = root / HARNESS_DIR / f"{module(new)}.rs"
    if new_path.exists():
        raise Mismatch(f"{new_path.relative_to(root)} already exists")
    previous = f"{HARNESS_DIR}/{module(old)}.rs"
    changes = {
        root / MODEL: edit_model(read(MODEL), old, new),
        root / BREAKDOWN: edit_breakdown(read(BREAKDOWN), old, new),
        root / BUDGET: edit_budget(read(BUDGET), old),
        root / HARNESS_MOD: edit_harness_mod(read(HARNESS_MOD), old, new),
        root / previous: edit_previous_module(read(previous), old),
        new_path: new_module(old, new, read(previous)),
    }
    if docs is not None:
        if not docs.is_file():
            raise Mismatch(f"{docs}: not found")
        changes[docs] = edit_docs(docs.read_text(), old, new)
    return changes


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--from", dest="old", required=True, help="the current harness version, e.g. 3.15")
    parser.add_argument("--to", dest="new", required=True, help="the new harness version, e.g. 3.16")
    parser.add_argument("--root", type=Path, default=ROOT, help="the intentd checkout (default: this one)")
    parser.add_argument("--docs", type=Path, help="the monorepo's docs/protocol/methods/agents.md")
    parser.add_argument("--dry-run", action="store_true", help="list the files without writing them")
    args = parser.parse_args(argv)
    try:
        if parse(args.new) <= parse(args.old):
            parser.error(f"--to {args.new} must be newer than --from {args.old}")
    except ValueError as error:
        parser.error(str(error))

    try:
        changes = plan(args.root, args.old, args.new, args.docs)
    except Mismatch as error:
        print(f"new-harness-version: {error}. Nothing was written.", file=sys.stderr)
        return 1
    for path, content in changes.items():
        if not args.dry_run:
            path.write_text(content)
        print(("would write " if args.dry_run else "wrote ") + os.path.relpath(path))

    print(f"\nHarness {args.new} is registered as a copy of {args.old}. Next:")
    print(f"  1. Make the change in {HARNESS_DIR}/{module(args.new)}.rs (new resource sets go in")
    print("     resources/agent-instructions/ or resources/specialists/), and pin it in its test.")
    if args.docs is None:
        print("  2. Add the version to both lists in the Prompt breakdown section of the monorepo's")
        print("     docs/protocol/methods/agents.md (not found here; pass --docs).")
    else:
        print(f"  2. Open the monorepo PR for {args.docs.name} first; it must merge before this one.")
    print("  3. Run: make check && make test-select E='test(harness)'")
    return 0


if __name__ == "__main__":
    sys.exit(main())
