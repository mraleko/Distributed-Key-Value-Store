#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUNNER="$ROOT_DIR/scripts/run_cluster.sh"

if [[ -x /usr/local/go/bin/go ]]; then
  export PATH="/usr/local/go/bin:$PATH"
fi

node_addr() {
  case "$1" in
    n1) echo "127.0.0.1:9101" ;;
    n2) echo "127.0.0.1:9102" ;;
    n3) echo "127.0.0.1:9103" ;;
    *) return 1 ;;
  esac
}

cleanup() {
  "$RUNNER" stop >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "[demo] reset + start cluster"
"$RUNNER" clean >/dev/null 2>&1 || true
"$RUNNER" start >/dev/null

leader_id="$("$RUNNER" leader)"
leader_addr="$(node_addr "$leader_id")"

echo "[demo] leader=$leader_id addr=$leader_addr"

echo "[demo] PUT key=demo"
curl -sfL -X PUT "http://$leader_addr/kv/put" -H "Content-Type: application/json" -d '{"key":"demo","value":"raft-ok"}'
echo

echo "[demo] GET key=demo"
curl -sf "http://$leader_addr/kv/get?key=demo"
echo

echo "[demo] DELETE key=demo"
curl -sfL -X POST "http://$leader_addr/kv/del" -H "Content-Type: application/json" -d '{"key":"demo"}'
echo

echo "[demo] GET key=demo (after delete)"
curl -sf "http://$leader_addr/kv/get?key=demo"
echo

echo "[demo] cluster status"
"$RUNNER" status
