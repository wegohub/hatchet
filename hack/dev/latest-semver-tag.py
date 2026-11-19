#!/usr/bin/env python3
"""Select the highest Go version tag from git ls-remote --tags --refs output."""

import re
import sys


VERSION = re.compile(
    r"v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)"
    r"(?:-([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?"
)


def version_key(tag):
    match = VERSION.fullmatch(tag)
    if match is None:
        return None
    core = tuple(int(part) for part in match.group(1, 2, 3))
    prerelease = match.group(4)
    if prerelease is None:
        return core, True, ()
    identifiers = []
    for part in prerelease.split("."):
        if part.isdigit():
            if len(part) > 1 and part.startswith("0"):
                return None
            identifiers.append((0, int(part)))
        else:
            identifiers.append((1, part))
    # Numeric identifiers precede textual ones; a release follows its prereleases.
    return core, False, tuple(identifiers)


def main():
    candidates = []
    for line in sys.stdin:
        fields = line.split()
        if len(fields) != 2 or not fields[1].startswith("refs/tags/"):
            continue
        tag = fields[1][len("refs/tags/") :]
        key = version_key(tag)
        # Exclude submodule tags and build metadata, which is not a distinct Go version.
        if key is not None:
            candidates.append((key, tag))
    if not candidates:
        sys.exit("Error: No upstream semantic version tag found (expected vX.Y.Z[-prerelease]).")
    print(max(candidates)[1])


if __name__ == "__main__":
    main()
