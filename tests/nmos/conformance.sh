#!/usr/bin/env bash
# Runs the AMWA nmos-testing node suites against the samplez-nmos example
# node: IS-04-01 with unicast and with multicast DNS-SD, IS-05-01 and
# BCP-007-03-01, each against a freshly started node, on one Docker bridge.
# Writes each suite's JSON results and the node's log to <out>, fails when
# a restart changed a resource id, and leaves the verdict to gate.py.
#
# usage: tests/nmos/conformance.sh <node binary> <node image> <tool image> <out>
#
# The node runs <node binary> in <node image>, which has to provide its
# libraries. Absolute activations need the kernel's TAI offset set: the
# node reads CLOCK_TAI, the tool computes TAI from UTC.
set -euo pipefail

node_bin=$(realpath "$1")
node_image=$2
tool_image=$3
out=$(mkdir -p "$4" && realpath "$4")
tag=nmos-$$
net=$tag
# Documentation addresses: a bridge in TEST-NET-2, since the tool's DNS
# zone uses TEST-NET-1 for its unreachable registry.
subnet=198.51.100.0/24
tool_ip=198.51.100.2
node_ip=198.51.100.3
domain=testsuite.nmos.tv

offset=$(python3 -c 'import time; print(round(time.clock_gettime(time.CLOCK_TAI) - time.clock_gettime(time.CLOCK_REALTIME)))')
if [ "$offset" != 37 ]; then
  echo "CLOCK_TAI - CLOCK_REALTIME is $offset s, not 37 s" >&2
  exit 1
fi

cleanup() {
  docker rm -f "$tag-node" "$tag-tool" >/dev/null 2>&1 || true
  docker network rm "$net" >/dev/null 2>&1 || true
}
trap cleanup EXIT
docker network create --subnet "$subnet" "$net" >/dev/null
docker volume create nmos-testing-specs >/dev/null

# Every resource id of the running node, sorted.
ids() {
  for r in self devices sources flows senders receivers; do
    curl -fsS "http://$node_ip:8081/x-nmos/node/v1.3/$r/"
    echo
  done | python3 -c 'import json,sys
ids = set()
for line in sys.stdin:
    v = json.loads(line)
    for r in (v if isinstance(v, list) else [v]):
        ids.add(r["id"])
print("\n".join(sorted(ids)))'
}

# run <name> <unicast|multicast|off> <suite> <tool suite arguments...>
run() {
  local name=$1 mode=$2 suite=$3
  shift 3
  local dns=() node_args=()
  if [ "$mode" = unicast ]; then
    dns=(--dns "$tool_ip" --dns-search "$domain")
    node_args=(--dns-sd-domain "$domain")
  fi
  docker run -d --name "$tag-node" --network "$net" --ip "$node_ip" "${dns[@]}" \
    -v "$node_bin:/usr/local/bin/nmos-node:ro" --user "$(id -u):$(id -g)" \
    --entrypoint /usr/local/bin/nmos-node "$node_image" \
    --advertise-host "$node_ip" --seed conformance "${node_args[@]}" >/dev/null
  for _ in $(seq 50); do
    curl -fsS -o /dev/null "http://$node_ip:8081/x-nmos/node/v1.3/self" && break
    sleep 0.2
  done
  ids > "$out/$name.ids"
  if [ -f "$out/first.ids" ]; then
    diff "$out/first.ids" "$out/$name.ids" || { echo "resource ids changed across a restart" >&2; exit 1; }
  else
    cp "$out/$name.ids" "$out/first.ids"
  fi

  mkdir -p "$out/config-$name"
  {
    echo "from . import Config as CONFIG"
    echo "CONFIG.ENABLE_DNS_SD = $([ "$mode" = off ] && echo False || echo True)"
    echo "CONFIG.DNS_SD_MODE = '$([ "$mode" = multicast ] && echo multicast || echo unicast)'"
    echo "CONFIG.DNS_DOMAIN = '$domain'"
  } > "$out/config-$name/UserConfig.py"
  echo "== $name: $suite ($mode)"
  docker run --rm --name "$tag-tool" --network "$net" --ip "$tool_ip" \
    -v "$out/config-$name:/config:ro" -v "$out:/results" -v nmos-testing-specs:/home/nmos-testing/cache \
    --entrypoint python3 "$tool_image" nmos-test.py suite "$suite" --selection all "$@" \
    --output "/results/$name.json" || true
  docker logs "$tag-node" > "$out/$name-node.log" 2>&1
  docker rm -f "$tag-node" >/dev/null
  test -s "$out/$name.json" || { echo "$name wrote no results" >&2; exit 1; }
}

run is-04-01-unicast unicast IS-04-01 --host "$node_ip" --port 8081 --version v1.3
run is-04-01-multicast multicast IS-04-01 --host "$node_ip" --port 8081 --version v1.3
run is-05-01 off IS-05-01 --host "$node_ip" --port 8081 --version v1.2
run bcp-007-03-01 off BCP-007-03-01 --host "$node_ip" "$node_ip" --port 8081 8081 --version v1.3 v1.2
