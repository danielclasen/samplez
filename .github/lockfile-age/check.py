"""Fails when Cargo.lock gains a crates.io release younger than 14 days.

usage: python3 check.py <base Cargo.lock> <Cargo.lock> <allow file>

Every crates.io package in the second lockfile and not, at that version,
in the first is looked up on crates.io; one published less than 14 days
ago fails the check unless the allow file lists it as
`<name> <version> <reason>`. Git and path packages are not checked.
"""

import json
import sys
import time
import tomllib
import urllib.request
from datetime import datetime, timedelta, timezone

MIN_AGE = timedelta(days=14)
REGISTRY = "registry+https://github.com/rust-lang/crates.io-index"
AGENT = "samplez-ci lockfile age check (github.com/danielclasen/samplez)"


def registry_packages(text):
    return {
        (p["name"], p["version"])
        for p in tomllib.loads(text).get("package", [])
        if p.get("source") == REGISTRY
    }


def allowed(text):
    out = {}
    for n, line in enumerate(text.splitlines(), 1):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split(None, 2)
        if len(parts) < 3:
            raise ValueError(f"allow file line {n}: needs <name> <version> <reason>")
        out[(parts[0], parts[1])] = parts[2]
    return out


def published(name, version):
    """The crates.io publish time of `name` at `version`."""
    req = urllib.request.Request(
        f"https://crates.io/api/v1/crates/{name}/{version}", headers={"User-Agent": AGENT}
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        created = json.load(r)["version"]["created_at"]
    # crates.io asks crawlers for at most one request per second.
    time.sleep(1)
    return datetime.fromisoformat(created)


def check(base, head, allow, now, lookup=published):
    """The problems, one line each, and the notes."""
    problems, notes = [], []
    added = sorted(registry_packages(head) - registry_packages(base))
    for name, version in added:
        age = now - lookup(name, version)
        if age >= MIN_AGE:
            continue
        reason = allow.get((name, version))
        days = age.total_seconds() / 86400
        if reason is None:
            problems.append(f"{name} {version} was published {days:.1f} days ago, under 14")
        else:
            notes.append(f"{name} {version} is {days:.1f} days old, allowed: {reason}")
    for name, version in sorted(set(allow) - set(added)):
        notes.append(f"{name} {version} is allowed but not added here")
    return problems, notes, len(added)


def main(base_path, head_path, allow_path):
    with open(base_path) as b, open(head_path) as h, open(allow_path) as a:
        problems, notes, n = check(
            b.read(), h.read(), allowed(a.read()), datetime.now(timezone.utc)
        )
    print(f"{n} crates.io packages added or changed")
    for line in notes + problems:
        print(line)
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main(*sys.argv[1:]))
