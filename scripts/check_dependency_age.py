#!/usr/bin/env python3
"""Fail if a change introduces Go module versions younger than a minimum age.

Compares every go.mod in the working tree against a git base ref, collects the
(module, version) requirement pairs that are new or bumped, and looks up each
version's publish time on the Go module proxy. Any version published within
the last MIN_DEPENDENCY_AGE_DAYS days (default 14) is reported as a violation
and the script exits 1.

Environment:
    MIN_DEPENDENCY_AGE_DAYS  minimum age in days (default 14)
    DEP_AGE_PROXY_URL        Go module proxy base URL (default https://proxy.golang.org)

Usage:
    python3 scripts/check_dependency_age.py --base-ref origin/master
"""

import argparse
import json
import os
import re
import subprocess
import sys
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone

DEFAULT_MIN_AGE_DAYS = 14
DEFAULT_PROXY_URL = "https://proxy.golang.org"
FETCH_TIMEOUT_SECONDS = 10

_REQUIRE_INLINE = re.compile(r"^require\s+(\S+)\s+(\S+)")
_PSEUDO_VERSION = re.compile(r"-(\d{14})-[0-9a-f]{12}$")


def parse_go_mod_requires(content):
    """Return {module path: version} for all require directives in a go.mod."""
    requires = {}
    in_block = False
    for raw in content.splitlines():
        line = raw.split("//", 1)[0].strip()
        if not line:
            continue
        if in_block:
            if line == ")":
                in_block = False
                continue
            parts = line.split()
            if len(parts) >= 2:
                requires[parts[0]] = parts[1]
            continue
        if line.startswith("require ("):
            in_block = True
            continue
        m = _REQUIRE_INLINE.match(line)
        if m:
            requires[m.group(1)] = m.group(2)
    return requires


def new_requirements(base_content, head_content):
    """Pairs present in head but not in base (new modules and version bumps)."""
    base = parse_go_mod_requires(base_content) if base_content is not None else {}
    head = parse_go_mod_requires(head_content)
    return [(mod, ver) for mod, ver in head.items() if base.get(mod) != ver]


def escape_module_path(path):
    """Encode a module path or version for proxy URLs (uppercase -> !lower)."""
    return "".join("!" + c.lower() if c.isascii() and c.isupper() else c
                   for c in path)


def pseudo_version_time(version):
    """Commit time embedded in a pseudo-version, or None for plain semver."""
    m = _PSEUDO_VERSION.search(version)
    if not m:
        return None
    return datetime.strptime(m.group(1), "%Y%m%d%H%M%S").replace(
        tzinfo=timezone.utc)


def fetch_publish_time(module, version, proxy_url=None):
    """Publish time from the module proxy's @v/<version>.info, or None."""
    base = (proxy_url or os.environ.get("DEP_AGE_PROXY_URL")
            or DEFAULT_PROXY_URL).rstrip("/")
    url = "{}/{}/@v/{}.info".format(
        base, escape_module_path(module), escape_module_path(version))
    try:
        with urllib.request.urlopen(url, timeout=FETCH_TIMEOUT_SECONDS) as resp:
            info = json.load(resp)
    except (urllib.error.URLError, OSError, ValueError):
        return None
    stamp = info.get("Time")
    if not stamp:
        return None
    try:
        return datetime.fromisoformat(stamp.replace("Z", "+00:00"))
    except ValueError:
        return None


def find_violations(pairs, threshold_days, now, fetch_time):
    """Pairs younger than threshold_days as (module, version, publish_time)."""
    violations = []
    for module, version in sorted(pairs):
        published = fetch_time(module, version)
        if published is None:
            published = pseudo_version_time(version)
        if published is None:
            print("WARN could not determine publish time for {}@{}; skipping"
                  .format(module, version), file=sys.stderr)
            continue
        age = now - published
        if age < timedelta(days=threshold_days):
            violations.append((module, version, published))
    return violations


def _git(args):
    return subprocess.run(["git"] + args, capture_output=True, text=True)


def changed_go_mod_files(base_ref):
    result = _git(["diff", "--name-only", "--diff-filter=ACMR",
                   base_ref + "...HEAD", "--", "go.mod", "**/go.mod"])
    if result.returncode != 0:
        raise RuntimeError("git diff failed: " + result.stderr.strip())
    return [f for f in result.stdout.splitlines() if f]


def introduced_pairs(base_ref, paths):
    pairs = set()
    for path in paths:
        with open(path, encoding="utf-8") as f:
            head_content = f.read()
        shown = _git(["show", "{}:{}".format(base_ref, path)])
        base_content = shown.stdout if shown.returncode == 0 else None
        pairs.update(new_requirements(base_content, head_content))
    return sorted(pairs)


def main(argv):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-ref", required=True,
                        help="git ref to diff against (e.g. origin/master)")
    args = parser.parse_args(argv)

    raw_threshold = os.environ.get("MIN_DEPENDENCY_AGE_DAYS", "").strip()
    try:
        threshold_days = int(raw_threshold) if raw_threshold else DEFAULT_MIN_AGE_DAYS
    except ValueError:
        print("ERROR MIN_DEPENDENCY_AGE_DAYS must be an integer, got {!r}"
              .format(raw_threshold), file=sys.stderr)
        return 2

    try:
        paths = changed_go_mod_files(args.base_ref)
        pairs = introduced_pairs(args.base_ref, paths)
    except (RuntimeError, OSError) as err:
        print("ERROR {}".format(err), file=sys.stderr)
        return 2

    now = datetime.now(timezone.utc)
    violations = find_violations(pairs, threshold_days, now, fetch_publish_time)
    for module, version, published in violations:
        days_ago = (now - published).days
        print("VIOLATION {}@{} published {} ({} days ago, minimum {})".format(
            module, version, published.isoformat(), days_ago, threshold_days))

    print("Checked {} introduced dependency version(s); {} violation(s)."
          .format(len(pairs), len(violations)))
    return 1 if violations else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
