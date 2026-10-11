#!/usr/bin/env python3
"""Keep callback discovery/execution on a compatible changed-test build scope."""
import json
from pathlib import Path
import re
import sys


NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]*\Z")
SHA = re.compile(r"[0-9a-f]{40}\Z")


def scope(document, head):
    if (not isinstance(document, dict) or type(document.get("version")) is not int
            or document["version"] != 1 or not SHA.fullmatch(head)
            or document.get("head") != head
            or not isinstance(document.get("mergeBase"), str)
            or not SHA.fullmatch(document["mergeBase"])
            or not isinstance(document.get("plans"), list)):
        raise ValueError("invalid or stale changed-test plan")
    eligible = []
    for argv in document["plans"]:
        if not isinstance(argv, list) or not argv or not all(isinstance(a, str) for a in argv):
            raise ValueError("plan must be a nonempty argv array")
        packages = []
        index = 0
        while index < len(argv):
            flag = argv[index]
            if flag in ("-p", "--test"):
                index += 1
                if index == len(argv) or not NAME.fullmatch(argv[index]):
                    raise ValueError("invalid package or test name")
                if flag == "-p":
                    if argv[index] in packages:
                        raise ValueError("duplicate package")
                    packages.append(argv[index])
            elif flag not in ("--lib", "--bins", "--tests"):
                raise ValueError("unsupported changed-test argument: " + flag)
            index += 1
        if not packages:
            raise ValueError("plan has no packages")
        if "intent-services" in packages and "--lib" in argv:
            eligible.append(argv + ([] if "intent-acp" in packages else ["-p", "intent-acp"]))
    # Validate every plan before selecting one. An unknown schema/flag is an
    # error, not a reason to silently select a different graph. Named-test and
    # fixture-only changes keep the existing callback-only scope.
    return eligible[0] if eligible else ["-p", "intent-acp", "-p", "intent-services", "--lib"]


if __name__ == "__main__":
    try:
        if len(sys.argv) != 3:
            raise ValueError("usage: callback-build-scope.py PLAN_JSON HEAD")
        result = scope(json.loads(Path(sys.argv[1]).read_text()), sys.argv[2])
    except (ValueError, OSError) as error:
        sys.exit(str(error))
    print("\n".join(result))
