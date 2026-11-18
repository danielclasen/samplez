#!/usr/bin/env bash
# Checks out the specification repositories of specs.txt at their pinned
# commits under the directory given (default target/nmos-specs), for
# `cargo test -p samplez-nmos --test schemas`. A checkout already at its
# commit is kept.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
out=${1:-target/nmos-specs}
mkdir -p "$out"
grep -v '^#' "$here/specs.txt" | while read -r dir url commit; do
  [ -n "$dir" ] || continue
  if [ "$(git -C "$out/$dir" rev-parse HEAD 2>/dev/null)" = "$commit" ]; then
    continue
  fi
  rm -rf "${out:?}/$dir"
  git init -q "$out/$dir"
  git -C "$out/$dir" fetch -q --depth 1 "$url" "$commit"
  git -C "$out/$dir" checkout -q FETCH_HEAD
done
