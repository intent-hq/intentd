#!/usr/bin/env python3
"""Classify committed docs changes without running Cargo or code from either tree.

Only regular documentation files and two tools.rs help string bodies qualify.
Scan both trees, including cfg-disabled Rust, for include_str!/include_bytes!.
Resolve literal paths, concat!, CARGO_MANIFEST_DIR and simple local literal-argument
macro wrappers. Other expressions fail closed unless their fixed filename suffix
excludes every changed document. This is source discovery, not Rust expansion or
build-script/runtime data-flow analysis. Integration modules with ambiguous target
ownership, external paths, non-workspace owners and unrecognized syntax use full CI.
"""
from dataclasses import dataclass
from functools import lru_cache
import fnmatch
import io
import json
import posixpath
import re
import subprocess
import sys


FULL = {"tier": "full", "packages": [], "tests": []}
TOOLS = "crates/intent-acp/src/mcp_server/tools.rs"
DESCRIPTIONS = {"WORKSPACE_API_DESCRIPTION", "WORKSPACE_API_DESCRIPTION_CHIEF"}
INCLUDES = {"include_str", "include_bytes"}
NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]*\Z")
TEXT_DOCS = {".md", ".markdown", ".rst", ".txt", ".adoc"}
DOC_ASSETS = {".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".pdf"}


def documentation(path):
    """Allow prose anywhere; other docs text/images only inside docs directories.

    Code, config, generated-site scripts, executable files and unknown extensions
    remain full CI even inside docs/. README without an extension is also prose.
    """
    name = posixpath.basename(path)
    extension = posixpath.splitext(name)[1].lower()
    readme = name.upper() == "README" or (name.upper().startswith("README.") and extension in TEXT_DOCS)
    in_docs = "docs" in path.split("/")[:-1]
    allowed = extension in {".md", ".markdown"} or readme or (in_docs and extension in TEXT_DOCS | DOC_ASSETS)
    return allowed, in_docs and extension in DOC_ASSETS


class NoMatch(Exception):
    pass


def require(condition, reason):
    if not condition:
        raise NoMatch(reason)


def git(*args, data=None):
    return subprocess.run(
        ["git", *args], input=data, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, check=True,
    ).stdout


@lru_cache(maxsize=None)
def blob(oid):
    return git("cat-file", "blob", oid)


class Tree:
    def __init__(self, rev):
        self.rev = rev
        self.entries = {}
        for entry in git("ls-tree", "-rz", rev).split(b"\0"):
            if entry:
                meta, path = entry.split(b"\t", 1)
                mode, kind, oid = meta.decode().split()
                self.entries[path.decode("utf-8")] = (mode, kind, oid)
        self.manifests = {}

    def data(self, path):
        mode, kind, oid = self.entries[path]
        require(mode == "100644" and kind == "blob", f"not a regular text file: {path}")
        return blob(oid)

    def read(self, path):
        return self.data(path).decode("utf-8")

    def manifest(self, path):
        import tomllib
        if path not in self.manifests:
            self.manifests[path] = tomllib.loads(self.read(path))
        return self.manifests[path]

    def crate_root(self, path):
        directory = posixpath.dirname(path)
        while directory:
            if directory + "/Cargo.toml" in self.entries:
                return directory
            directory = posixpath.dirname(directory)
        raise NoMatch(f"no owning crate for {path}")

    def owner(self, path):
        root = self.crate_root(path)
        workspace = self.manifest("Cargo.toml")["workspace"]
        require(any(fnmatch.fnmatchcase(root, p) for p in workspace["members"])
                and not any(fnmatch.fnmatchcase(root, p) for p in workspace.get("exclude", [])),
                f"owner is not a workspace member: {path}")
        manifest = self.manifest(root + "/Cargo.toml")
        package = manifest["package"]["name"]
        require(isinstance(package, str) and NAME.fullmatch(package), f"invalid package name: {path}")
        relative = posixpath.relpath(path, root)
        if relative.startswith("src/"):
            return package, None
        targets = []
        for target in manifest.get("test", []):
            name = target["name"]
            target_path = target.get("path")
            if target_path is None:
                target_path = f"tests/{name}.rs"
                if root + "/" + target_path not in self.entries:
                    target_path = f"tests/{name}/main.rs"
            if posixpath.normpath(target_path) == relative:
                targets.append(name)
        if not targets and manifest["package"].get("autotests", True):
            match = re.fullmatch(r"tests/([^/]+)\.rs|tests/([^/]+)/main\.rs", relative)
            if match:
                name = match.group(1) or match.group(2)
                require(not any(t["name"] == name for t in manifest.get("test", [])),
                        f"overridden integration target: {path}")
                targets.append(name)
        require(len(targets) == 1 and NAME.fullmatch(targets[0]),
                f"ambiguous integration/build/example owner: {path}")
        return package, targets[0]

    def rust_sources(self):
        # One batch avoids a git process per Rust file. Only files with includes
        # (or a discovered wrapper call) need lexing; comments still get lexed.
        paths = [p for p in self.entries if p.endswith(".rs")]
        oids = []
        for path in paths:
            mode, kind, oid = self.entries[path]
            require(mode == "100644" and kind == "blob", f"unknown Rust source mode: {path}")
            oids.append(oid)
        stream = io.BytesIO(git("cat-file", "--batch", data="".join(o + "\n" for o in oids).encode()))
        sources = {}
        for path, oid in zip(paths, oids):
            actual, kind, size = stream.readline().decode().split()
            require(actual == oid and kind == "blob", f"cannot read Rust source: {path}")
            sources[path] = stream.read(int(size)).decode("utf-8")
            require(stream.read(1) == b"\n", "invalid git batch response")
        return sources

    def include_path(self, path):
        # Check each component BEFORE collapsing '..': symlink/../file follows
        # the symlink physically, not lexically. A symlink may also hide a direct
        # include of a changed doc behind an otherwise unrelated filename.
        components = []
        for component in path.split("/"):
            if component in ("", "."):
                continue
            if component == "..":
                require(components, f"include escapes repository: {path}")
                components.pop()
                continue
            components.append(component)
            entry = self.entries.get("/".join(components))
            require(entry is None or entry[0] not in ("120000", "160000"),
                    f"symlink/submodule in include path: {path}")
        return "/".join(components)


@dataclass(frozen=True)
class Token:
    text: str
    start: int
    end: int
    kind: str = "code"
    body_start: int = 0
    body_end: int = 0


LEX = re.compile(
    r"\s+|//[^\n]*|/\*|(?:br|cr|r)(?P<hashes>\#*)\"|(?:b|c)?\""
    r"|(?:b)?'(?:\\(?:u\{[0-9A-Fa-f_]+\}|x[0-9A-Fa-f]{2}|[^\n])|[^'\\\n])'"
    r"|(?:r\#)?[^\W\d]\w*|.", re.DOTALL,
)
QUOTED = re.compile(r'(?:\\[\s\S]|[^"\\])*"')


@lru_cache(maxsize=256)
def lex(source):
    """Skip nested comments; keep literals atomic so fake includes stay inert."""
    tokens = []
    i = 0
    while i < len(source):
        match = LEX.match(source, i)
        require(match is not None, "unrecognized Rust token")
        text = match.group()
        end = match.end()
        kind, body_start, body_end = "code", 0, 0
        if text.isspace() or text.startswith("//"):
            i = end
            continue
        if text == "/*":
            depth = 1
            while depth:
                next_comment = re.search(r"/\*|\*/", source[end:])
                require(next_comment is not None, "unterminated Rust block comment")
                depth += 1 if next_comment.group() == "/*" else -1
                end += next_comment.end()
            i = end
            continue
        if match.group("hashes") is not None:
            closing = '"' + match.group("hashes")
            body_start = end
            body_end = source.find(closing, end)
            require(body_end >= 0, "unterminated Rust raw string")
            end = body_end + len(closing)
            kind = "raw" if text.startswith("r") else "other-string"
            text = source[i:end]
        elif text in ('"', 'b"', 'c"'):
            closing = QUOTED.match(source, end)
            require(closing is not None, "unterminated Rust string")
            body_start, body_end = end, closing.end() - 1
            end = closing.end()
            kind = "string" if text == '"' else "other-string"
            text = source[i:end]
        elif text.endswith("'") and len(text) > 1:
            kind = "char"
        elif text.startswith("r#"):
            text = text[2:]
        tokens.append(Token(text, i, end, kind, body_start, body_end))
        i = end
    return tokens


def words(tokens):
    return [t.text for t in tokens]


def group(tokens, start):
    closes = {"(": ")", "[": "]", "{": "}"}
    require(start < len(tokens) and tokens[start].text in closes, "expected Rust delimiter")
    stack = []
    for index in range(start, len(tokens)):
        text = tokens[index].text
        if text in closes:
            stack.append(closes[text])
        elif text in closes.values():
            require(stack and stack.pop() == text, "mismatched Rust delimiters")
            if not stack:
                return tokens[start + 1:index], index + 1
    raise NoMatch("unterminated Rust token group")


def arguments(tokens):
    parts, start, index = [], 0, 0
    while index < len(tokens):
        if tokens[index].text in ("(", "[", "{"):
            _, index = group(tokens, index)
        elif tokens[index].text == ",":
            require(index > start, "empty macro argument")
            parts.append(tokens[start:index])
            start = index = index + 1
        else:
            index += 1
    if start < len(tokens):
        parts.append(tokens[start:])
    return parts


def literal(token):
    if token.kind == "raw":
        first = token.text.index('"')
        hashes = first - 1
        return token.text[first + 1:len(token.text) - hashes - 1]
    require(token.kind == "string", "include argument is not a string literal")
    # A deliberately narrow Rust escape subset. Unknown escapes select full CI.
    value = token.text[1:-1]
    require(not re.search(r'\\(?![\\"nrt0])', value), "unsupported path string escape")
    escapes = {"\\": "\\", '"': '"', "n": "\n", "r": "\r", "t": "\t", "0": "\0"}
    return re.sub(r"\\(.)", lambda m: escapes[m.group(1)], value)


def expression(tokens, root):
    """Return literal pieces and None for unknown expansions, never execute them."""
    if len(tokens) == 1 and tokens[0].kind in ("string", "raw"):
        return [literal(tokens[0])]
    if len(tokens) > 3 and tokens[1].text == "!":
        inner, end = group(tokens, 2)
        if end == len(tokens):
            if tokens[0].text == "concat":
                return [piece for arg in arguments(inner) for piece in expression(arg, root)]
            if tokens[0].text == "env" and len(inner) == 1 and inner[0].kind in ("string", "raw"):
                if literal(inner[0]) == "CARGO_MANIFEST_DIR":
                    return ["/@repo/" + root]
    return [None]


def normalize_help(source):
    tokens = lex(source)
    spans = {}
    depth = 0
    for index, token in enumerate(tokens):
        if token.text == "{":
            depth += 1
        elif token.text == "}":
            depth -= 1
        if token.text != "const" or index + 7 >= len(tokens):
            continue
        name = tokens[index + 1].text
        if name not in DESCRIPTIONS:
            continue
        require(depth == 0 and name not in spans, "ambiguous help constant")
        require(words(tokens[index + 2:index + 6]) == [":", "&", "str", "="],
                "help constant declaration changed")
        value = tokens[index + 6]
        require(value.kind == "raw" and tokens[index + 7].text == ";", "help is not one raw string")
        spans[name] = (value.body_start, value.body_end)
    require(set(spans) == DESCRIPTIONS, "missing help constants")
    for start, end in sorted(spans.values(), reverse=True):
        source = source[:start] + "<help body>" + source[end:]
    return source


def wrapper(tokens):
    """Recognize only ($x:literal, ...) => { include_...!(...) }; wrappers."""
    params, index = group(tokens, 0)
    require(words(tokens[index:index + 2]) == ["=", ">"], "unsupported include macro rule")
    body, end = group(tokens, index + 2)
    require(words(tokens[end:]) in ([], [";"], [","]), "multiple include macro rules")
    names = []
    for param in arguments(params):
        require(len(param) == 4 and words(param)[::2] == ["$", ":"]
                and param[3].text == "literal", "unsupported include macro parameter")
        names.append(param[1].text)
    require(names and len(set(names)) == len(names), "ambiguous include macro parameters")
    require(len(body) > 3 and body[0].text in INCLUDES and body[1].text == "!",
            "include macro does more than include a file")
    expr, end = group(body, 2)
    require(words(body[end:]) in ([], [";"]), "include macro has extra expressions")
    return names, expr


def discovery(tree, changed):
    sources = tree.rust_sources()
    parsed = {path: lex(source) for path, source in sources.items()
              if "include_str" in source or "include_bytes" in source}
    wrappers, definitions = {}, {}
    # Macro expansion is restricted to this small, auditable shape. A macro with
    # arbitrary control flow/repetition/imports must not silently hide an include.
    for path, tokens in parsed.items():
        ranges = []
        for index, token in enumerate(tokens):
            if token.text != "macro_rules" or words(tokens[index + 1:index + 2]) != ["!"]:
                continue
            body, end = group(tokens, index + 3)
            if not any(t.text in INCLUDES for t in body):
                continue
            require(not any(t.text == "macro_export" for t in tokens), f"exported include macro: {path}")
            name = tokens[index + 2].text
            require(name not in wrappers, f"ambiguous include macro: {name}")
            try:
                params, expr = wrapper(body)
            except NoMatch as error:
                raise NoMatch(f"{path}: {error}") from error
            wrappers[name] = (path, params, expr)
            ranges.append((index, end))
        definitions[path] = ranges
    for path, source in sources.items():
        if path not in parsed and any(re.search(r"\b" + re.escape(name) + r"\b", source) for name in wrappers):
            parsed[path] = lex(source)

    owners = {path: set() for path in changed}
    used = set()
    for source_path, tokens in parsed.items():
        index = 0
        while index < len(tokens):
            skip = next((end for start, end in definitions.get(source_path, []) if start == index), None)
            if skip is not None:
                index = skip
                continue
            token = tokens[index]
            require(token.text not in INCLUDES or words(tokens[index + 1:index + 2]) == ["!"],
                    f"include macro import/alias or unknown use: {source_path}")
            if token.text not in INCLUDES | wrappers.keys() or words(tokens[index + 1:index + 2]) != ["!"]:
                index += 1
                continue
            expr, end = group(tokens, index + 2)
            if token.text in wrappers:
                definition, params, template = wrappers[token.text]
                require(source_path == definition, f"include wrapper used outside its defining file: {token.text}")
                args = arguments(expr)
                require(len(args) == len(params) and all(len(a) == 1 and a[0].kind in ("string", "raw") for a in args),
                        f"nonliteral include wrapper invocation: {source_path}")
                values = dict(zip(params, args))
                expr, cursor = [], 0
                while cursor < len(template):
                    if template[cursor].text == "$":
                        require(cursor + 1 < len(template) and template[cursor + 1].text in values,
                                "unknown include macro variable")
                        expr.extend(values[template[cursor + 1].text])
                        cursor += 2
                    else:
                        expr.append(template[cursor])
                        cursor += 1
                used.add(token.text)
            else:
                args = arguments(expr)
                require(len(args) == 1, f"unexpected include arguments: {source_path}")
                expr = args[0]
            require(not any(t.text in INCLUDES | wrappers.keys() for t in expr),
                    f"nested include expression needs full CI: {source_path}")
            # Root lookup is needed only for CARGO_MANIFEST_DIR expressions.
            root = tree.crate_root(source_path) if any(t.text == "env" for t in expr) else ""
            pieces = expression(expr, root)
            if None in pieces:
                # concat!(env!("OUT_DIR"), "/profile-Cargo.lock") cannot name
                # a changed .md file. Compare every admitted document/asset.
                suffix = (pieces[-1] or "").rsplit("/", 1)[-1]
                require(suffix and all(not p.endswith(suffix) for p in changed),
                        f"unresolved include could select changed docs: {source_path}")
            else:
                target = "".join(pieces)
                require(target and "\0" not in target and "\\" not in target, f"unsupported include path: {source_path}")
                if target.startswith("/@repo/"):
                    target = target[len("/@repo/"):]
                else:
                    require(not target.startswith("/"), f"external include path: {source_path}")
                    target = posixpath.join(posixpath.dirname(source_path), target)
                target = tree.include_path(target)
                if target in changed:
                    require(target in tree.entries, f"included document missing at {tree.rev}: {target}")
                    tree.data(target)
                    owners[target].add(tree.owner(source_path))
            index = end
    require(used == wrappers.keys(), "include wrapper has no provable literal callers")
    return owners


def classify(base, head):
    base = git("rev-parse", "--verify", "--end-of-options", base + "^{commit}").decode().strip()
    head = git("rev-parse", "--verify", "--end-of-options", head + "^{commit}").decode().strip()
    # The caller supplies the comparison base (PR base or merge-group base).
    # Never substitute a merge base that could hide changes in the tested tree.
    changes = git("diff", "--raw", "-z", "--no-abbrev", "--no-renames", base, head, "--").split(b"\0")
    require(changes[-1] == b"", "invalid git diff output")
    changes.pop()
    require(changes and len(changes) % 2 == 0, "empty or unreadable diff")
    before, after = Tree(base), Tree(head)
    docs, selected = set(), set()
    for meta, path_bytes in zip(changes[::2], changes[1::2]):
        path = path_bytes.decode("utf-8")
        old_mode, new_mode, _, _, status = meta.decode().lstrip(":").split()
        require((old_mode, new_mode, status) in {
            ("100644", "100644", "M"), ("000000", "100644", "A"), ("100644", "000000", "D")
        }, f"mode/type change or nonregular file: {path}")
        if path == TOOLS:
            require(status == "M" and normalize_help(before.read(path)) == normalize_help(after.read(path)),
                    "tools.rs changes outside the two help string bodies")
            selected.add(before.owner(path))
            selected.add(after.owner(path))
        else:
            allowed, binary = documentation(path)
            require(allowed, f"not a recognized documentation/help-only change: {path}")
            docs.add(path)
            for tree in (before, after):
                if path in tree.entries:
                    if binary:
                        tree.data(path)
                    else:
                        require("\0" not in tree.read(path), f"binary prose file: {path}")
    # Still scan when only tools.rs changed: ambiguous discovery must fail closed.
    for tree in (before, after):
        for owners in discovery(tree, docs).values():
            selected.update(owners)
    packages = sorted({package for package, target in selected if target is None})
    tests = [{"package": package, "target": target} for package, target in sorted(
        (p, t) for p, t in selected if t is not None
    )]
    return {"tier": "compiled" if selected else "docs", "packages": packages, "tests": tests}


def main():
    try:
        require(len(sys.argv) == 3, "usage: docs-ci-scope.sh BASE HEAD")
        result = classify(sys.argv[1], sys.argv[2])
    except Exception as error:
        print(f"docs-ci-scope: full CI: {error}", file=sys.stderr)
        result = FULL
    print(json.dumps(result, separators=(",", ":")))


if __name__ == "__main__":
    main()
