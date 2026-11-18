"""The verdict on the nmos-testing results that conformance.sh wrote.

usage: python3 gate.py <expected.json> <results dir>

Fails on any Fail, Warning or Not Implemented, on any other result that
is not Pass and not listed in expected.json with that result, on any
entry of expected.json that no longer matches, so the list cannot rot,
and on a run with fewer tests than its minimum in expected.json, so a
run that loses tests cannot pass.
"""

import json
import sys
from pathlib import Path

RUNS = ["is-04-01-unicast", "is-04-01-multicast", "is-05-01", "bcp-007-03-01"]
NEVER = {"Fail", "Warning", "Not Implemented"}
# Not tests: the tool reports its set-up and tear-down as Not Applicable.
FRAME = {"__init__", "set_up_tests", "tear_down_tests"}


def main(expected_path, results_dir):
    doc = json.loads(Path(expected_path).read_text())
    minimum = doc["minimum"]
    if sorted(minimum) != sorted(RUNS) or not all(isinstance(n, int) and n > 0 for n in minimum.values()):
        print(f"expected.json: the minimum needs a positive count for each of {RUNS}")
        return 1
    expected = {}
    for e in doc["expected"]:
        key = (e["run"], e["test"])
        if key in expected or e["result"] in NEVER or e["result"] == "Pass" or not e.get("reason"):
            print(f"expected.json: bad entry {e}")
            return 1
        expected[key] = e["result"]

    problems = []
    seen = set()
    for run in RUNS:
        path = Path(results_dir) / f"{run}.json"
        if not path.exists():
            problems.append(f"{run}: no results")
            continue
        results = json.loads(path.read_text())["results"]
        counts = {}
        for r in results:
            name, state = r["name"], r["state"]
            if name in FRAME and state == "Not Applicable":
                continue
            counts[state] = counts.get(state, 0) + 1
            key = (run, name)
            want = expected.get(key)
            seen.add(key)
            if state == "Pass":
                if want is not None:
                    problems.append(f"{run} {name}: passes, expected {want}; remove it from expected.json")
            elif state in NEVER or want != state:
                problems.append(f"{run} {name}: {state}: {r.get('detail') or r.get('description')}")
        total = sum(counts.values())
        print(f"{run}: {total} tests: " + ", ".join(f"{n} {s}" for s, n in sorted(counts.items())))
        if total < minimum[run]:
            problems.append(f"{run}: {total} tests ran, at least {minimum[run]} expected")
    for key in sorted(set(expected) - seen):
        problems.append(f"{key[0]} {key[1]}: in expected.json but not run")
    for p in problems:
        print(p)
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main(*sys.argv[1:]))
