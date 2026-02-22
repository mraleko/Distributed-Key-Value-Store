#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUNNER="$ROOT_DIR/scripts/run_cluster.sh"

node_addr() {
  case "$1" in
    n1) echo "127.0.0.1:9101" ;;
    n2) echo "127.0.0.1:9102" ;;
    n3) echo "127.0.0.1:9103" ;;
    *) return 1 ;;
  esac
}

json_string() {
  local json="$1"
  local field="$2"
  echo "$json" | sed -nE "s/.*\"$field\":\"([^\"]*)\".*/\1/p"
}

json_number() {
  local json="$1"
  local field="$2"
  echo "$json" | sed -nE "s/.*\"$field\":([0-9]+).*/\1/p"
}

json_bool() {
  local json="$1"
  local field="$2"
  echo "$json" | sed -nE "s/.*\"$field\":(true|false).*/\1/p"
}

status_json() {
  local id="$1"
  local addr
  addr="$(node_addr "$id")"
  curl -sf --max-time 1 "http://$addr/cluster/status"
}

wait_for_leader() {
  local timeout_sec="$1"
  local allow_id="${2:-}"
  local deadline=$((SECONDS + timeout_sec))

  while (( SECONDS < deadline )); do
    for id in n1 n2 n3; do
      local status
      status="$(status_json "$id" 2>/dev/null || true)"
      if [[ -z "$status" ]]; then
        continue
      fi
      local role
      role="$(json_string "$status" "role")"
      if [[ "$role" != "leader" ]]; then
        continue
      fi
      if [[ -n "$allow_id" && "$id" == "$allow_id" ]]; then
        continue
      fi
      echo "$id"
      return 0
    done
    sleep 0.2
  done
  return 1
}

put_key() {
  local id="$1"
  local key="$2"
  local value="$3"
  local addr
  addr="$(node_addr "$id")"
  curl -sfL -X PUT "http://$addr/kv/put" \
    -H "Content-Type: application/json" \
    -d "{\"key\":\"$key\",\"value\":\"$value\"}" \
    >/dev/null
}

get_key() {
  local id="$1"
  local key="$2"
  local addr
  addr="$(node_addr "$id")"
  curl -sf "http://$addr/kv/get?key=$key"
}

wait_for_catchup() {
  local recovering_id="$1"
  local leader_id="$2"
  local timeout_sec="$3"
  local deadline=$((SECONDS + timeout_sec))

  while (( SECONDS < deadline )); do
    local leader_status recovering_status
    leader_status="$(status_json "$leader_id" 2>/dev/null || true)"
    recovering_status="$(status_json "$recovering_id" 2>/dev/null || true)"
    if [[ -z "$leader_status" || -z "$recovering_status" ]]; then
      sleep 0.2
      continue
    fi

    local leader_commit recovering_commit
    leader_commit="$(json_number "$leader_status" "commitIndex")"
    recovering_commit="$(json_number "$recovering_status" "commitIndex")"

    if [[ -n "$leader_commit" && -n "$recovering_commit" ]]; then
      if (( recovering_commit >= leader_commit )); then
        return 0
      fi
    fi
    sleep 0.2
  done
  return 1
}

cleanup() {
  "$RUNNER" stop >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "[chaos] cleaning previous cluster"
"$RUNNER" clean >/dev/null 2>&1 || true

echo "[chaos] starting 3-node cluster"
"$RUNNER" start

echo "[chaos] waiting for initial leader"
leader_id="$(wait_for_leader 20 || true)"
if [[ -z "$leader_id" ]]; then
  echo "[chaos] failed to elect initial leader"
  exit 1
fi
echo "[chaos] leader is $leader_id"

echo "[chaos] writing 100 keys"
for i in $(seq 1 100); do
  put_key "$leader_id" "k$i" "v$i"
done

echo "[chaos] killing leader $leader_id"
"$RUNNER" stop-node "$leader_id"

echo "[chaos] waiting for a new leader"
new_leader_id="$(wait_for_leader 25 "$leader_id" || true)"
if [[ -z "$new_leader_id" ]]; then
  echo "[chaos] failed to elect a new leader"
  exit 1
fi
echo "[chaos] new leader is $new_leader_id"

echo "[chaos] verifying reads after failover"
for i in $(seq 1 100); do
  resp="$(get_key "$new_leader_id" "k$i")"
  found="$(json_bool "$resp" "found")"
  value="$(json_string "$resp" "value")"
  if [[ "$found" != "true" || "$value" != "v$i" ]]; then
    echo "[chaos] read mismatch key=k$i found=$found value=$value"
    exit 1
  fi
done

echo "[chaos] writing additional keys after failover"
for i in $(seq 101 120); do
  put_key "$new_leader_id" "k$i" "v$i"
done

echo "[chaos] restarting old leader $leader_id"
"$RUNNER" start-node "$leader_id"

echo "[chaos] waiting for restarted node to catch up"
if ! wait_for_catchup "$leader_id" "$new_leader_id" 30; then
  echo "[chaos] restarted node did not catch up in time"
  exit 1
fi

echo "[chaos] verifying restarted node has replicated state"
for i in $(seq 1 120); do
  resp="$(get_key "$leader_id" "k$i")"
  found="$(json_bool "$resp" "found")"
  value="$(json_string "$resp" "value")"
  if [[ "$found" != "true" || "$value" != "v$i" ]]; then
    echo "[chaos] restart node mismatch key=k$i found=$found value=$value"
    exit 1
  fi
done

echo "[chaos] success: cluster survived leader failure and recovered"
