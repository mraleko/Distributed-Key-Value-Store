#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUNNER="$ROOT_DIR/scripts/run_cluster.sh"
RESULTS_DIR="$ROOT_DIR/benchmarks"
LATEST_RESULTS="$RESULTS_DIR/latest.md"

WRITE_COUNT="${1:-300}"
READ_COUNT="${2:-300}"

if ! [[ "$WRITE_COUNT" =~ ^[0-9]+$ ]] || ! [[ "$READ_COUNT" =~ ^[0-9]+$ ]]; then
  echo "usage: $0 [write_count] [read_count]"
  exit 1
fi

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
      if [[ "$role" == "leader" ]]; then
        echo "$id"
        return 0
      fi
    done
    sleep 0.2
  done
  return 1
}

now_ms() {
  perl -MTime::HiRes=time -e 'printf("%.0f\n", time() * 1000)'
}

put_key() {
  local addr="$1"
  local key="$2"
  local value="$3"
  curl -sfL -X PUT "http://$addr/kv/put" \
    -H "Content-Type: application/json" \
    -d "{\"key\":\"$key\",\"value\":\"$value\"}" \
    >/dev/null
}

get_key() {
  local addr="$1"
  local key="$2"
  curl -sf "http://$addr/kv/get?key=$key"
}

cleanup() {
  "$RUNNER" stop >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "[bench] cleaning previous cluster"
"$RUNNER" clean >/dev/null 2>&1 || true

echo "[bench] starting cluster"
"$RUNNER" start >/dev/null

leader_id="$(wait_for_leader 20 || true)"
if [[ -z "$leader_id" ]]; then
  echo "[bench] failed to elect leader"
  exit 1
fi
leader_addr="$(node_addr "$leader_id")"

echo "[bench] leader=$leader_id addr=$leader_addr"

echo "[bench] warming up"
for i in $(seq 1 20); do
  put_key "$leader_addr" "warmup$i" "w$i"
done

write_start_ms="$(now_ms)"
for i in $(seq 1 "$WRITE_COUNT"); do
  put_key "$leader_addr" "bench$i" "v$i"
done
write_end_ms="$(now_ms)"

write_duration_ms=$((write_end_ms - write_start_ms))
if (( write_duration_ms <= 0 )); then
  write_duration_ms=1
fi
write_tps="$(awk "BEGIN { printf \"%.2f\", $WRITE_COUNT / ($write_duration_ms / 1000.0) }")"
write_avg_ms="$(awk "BEGIN { printf \"%.3f\", $write_duration_ms / $WRITE_COUNT }")"

read_start_ms="$(now_ms)"
for i in $(seq 1 "$READ_COUNT"); do
  key_index=$(( ((i - 1) % WRITE_COUNT) + 1 ))
  resp="$(get_key "$leader_addr" "bench$key_index")"
  found="$(json_bool "$resp" "found")"
  value="$(json_string "$resp" "value")"
  if [[ "$found" != "true" || "$value" != "v$key_index" ]]; then
    echo "[bench] read verify failed key=bench$key_index found=$found value=$value"
    exit 1
  fi
done
read_end_ms="$(now_ms)"

read_duration_ms=$((read_end_ms - read_start_ms))
if (( read_duration_ms <= 0 )); then
  read_duration_ms=1
fi
read_tps="$(awk "BEGIN { printf \"%.2f\", $READ_COUNT / ($read_duration_ms / 1000.0) }")"
read_avg_ms="$(awk "BEGIN { printf \"%.3f\", $read_duration_ms / $READ_COUNT }")"

leader_status="$(status_json "$leader_id")"
term="$(json_number "$leader_status" "currentTerm")"
commit_index="$(json_number "$leader_status" "commitIndex")"

timestamp_utc="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
mkdir -p "$RESULTS_DIR"
cat > "$LATEST_RESULTS" <<EOF_RESULTS
# Benchmark Results

- Generated (UTC): $timestamp_utc
- Node count: 3
- Leader during run: $leader_id ($leader_addr)
- Writes: $WRITE_COUNT
- Reads: $READ_COUNT

## Write Path

- Total duration: ${write_duration_ms} ms
- Throughput: ${write_tps} ops/s
- Average latency: ${write_avg_ms} ms/op

## Read Path

- Total duration: ${read_duration_ms} ms
- Throughput: ${read_tps} ops/s
- Average latency: ${read_avg_ms} ms/op

## Cluster Snapshot

- currentTerm: ${term:-unknown}
- leaderCommitIndex: ${commit_index:-unknown}
EOF_RESULTS

echo "[bench] benchmark complete"
echo "[bench] writes: ${WRITE_COUNT} in ${write_duration_ms}ms (${write_tps} ops/s, avg ${write_avg_ms} ms/op)"
echo "[bench] reads:  ${READ_COUNT} in ${read_duration_ms}ms (${read_tps} ops/s, avg ${read_avg_ms} ms/op)"
echo "[bench] results written to $LATEST_RESULTS"
