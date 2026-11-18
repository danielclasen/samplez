#!/usr/bin/env bash
# Fills nmos-testing's specification cache <dir> with the AMWA
# repositories at the commits pinned in specs.txt: each pinned branch as a
# local branch and as origin/<branch>, which is what the tool checks out
# and rebases onto. No remote is configured and the last-pull stamp lies
# in the future, so the tool neither clones nor pulls.
#
# usage: tests/nmos/spec-cache.sh <dir>
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
cache=$1
mkdir -p "$cache"
grep -v '^#' "$here/specs.txt" | while read -r repo branch commit; do
  [ -n "$repo" ] || continue
  dir=$cache/$repo
  [ -d "$dir/.git" ] || git init -q "$dir"
  git -C "$dir" fetch -q --depth 1 "https://github.com/AMWA-TV/$repo" "$commit"
  git -C "$dir" update-ref "refs/remotes/origin/$branch" "$commit"
  git -C "$dir" checkout -q -B "$branch" "$commit"
done
python3 -c 'import datetime, pickle, sys
with open(sys.argv[1], "wb") as f:
    pickle.dump(datetime.datetime(9999, 1, 1), f)' "$cache/last_pull"
