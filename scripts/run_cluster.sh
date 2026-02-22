#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_PATH="$ROOT_DIR/bin/kvnode"
STATE_DIR="$ROOT_DIR/.cluster"
DATA_DIR="$STATE_DIR/data"
LOG_DIR="$STATE_DIR/logs"
PID_DIR="$STATE_DIR/pids"

PEERS="n1=127.0.0.1:9101,n2=127.0.0.1:9102,n3=127.0.0.1:9103"

node_addr() {
  case "$1" in
    n1) echo "127.0.0.1:9101" ;;
    n2) echo "127.0.0.1:9102" ;;
    n3) echo "127.0.0.1:9103" ;;
    *) return 1 ;;
  esac
}

ensure_bin() {
  if ! command -v go >/dev/null 2>&1; then
    echo "go toolchain not found in PATH"
    exit 1
  fi
  mkdir -p "$ROOT_DIR/bin"
  go build -o "$BIN_PATH" ./cmd/kvnode
}

pid_file() {
  echo "$PID_DIR/$1.pid"
}

is_running() {
  local pid="$1"
  if [[ -z "$pid" ]]; then
    return 1
  fi
  kill -0 "$pid" >/dev/null 2>&1
}

start_node() {
  local id="$1"
  local addr
  addr="$(node_addr "$id")"
  mkdir -p "$DATA_DIR/$id" "$LOG_DIR" "$PID_DIR"

  local pf
  pf="$(pid_file "$id")"
  if [[ -f "$pf" ]]; then
    local existing
    existing="$(cat "$pf")"
    if is_running "$existing"; then
      echo "$id already running pid=$existing"
      return 0
    fi
    rm -f "$pf"
  fi

  "$BIN_PATH" \
    --id "$id" \
    --listen "$addr" \
    --peers "$PEERS" \
    --data-dir "$DATA_DIR/$id" \
    >"$LOG_DIR/$id.log" 2>&1 &

  local pid=$!
  echo "$pid" >"$pf"
  echo "started $id pid=$pid addr=$addr"
}

stop_node() {
  local id="$1"
  local pf
  pf="$(pid_file "$id")"
  if [[ ! -f "$pf" ]]; then
    echo "$id not running"
    return 0
  fi

  local pid
  pid="$(cat "$pf")"
  if is_running "$pid"; then
    kill -TERM "$pid" >/dev/null 2>&1 || true
    for _ in $(seq 1 30); do
      if ! is_running "$pid"; then
        break
      fi
      sleep 0.1
    done
  fi

  if is_running "$pid"; then
    kill -KILL "$pid" >/dev/null 2>&1 || true
  fi
  rm -f "$pf"
  echo "stopped $id"
}

leader() {
  for _ in $(seq 1 60); do
    for id in n1 n2 n3; do
      local addr
      addr="$(node_addr "$id")"
      local status
      status="$(curl -sf --max-time 1 "http://$addr/cluster/status" || true)"
      if [[ -z "$status" ]]; then
        continue
      fi
      local role
      role="$(echo "$status" | sed -nE 's/.*"role":"([^"]+)".*/\1/p')"
      if [[ "$role" == "leader" ]]; then
        echo "$id"
        return 0
      fi
    done
    sleep 0.2
  done
  return 1
}

status_cluster() {
  for id in n1 n2 n3; do
    local pf
    pf="$(pid_file "$id")"
    local pid=""
    if [[ -f "$pf" ]]; then
      pid="$(cat "$pf")"
    fi
    local running="no"
    if is_running "$pid"; then
      running="yes"
    fi
    local addr
    addr="$(node_addr "$id")"
    local stat
    stat="$(curl -sf --max-time 1 "http://$addr/cluster/status" || true)"
    echo "$id pid=${pid:-none} running=$running status=${stat:-unreachable}"
  done
}

clean_cluster() {
  stop_node n1 || true
  stop_node n2 || true
  stop_node n3 || true
  rm -rf "$STATE_DIR"
  echo "cleaned $STATE_DIR"
}

usage() {
  cat <<USAGE
Usage: $0 <command>

Commands:
  start              Build and start all 3 nodes
  stop               Stop all 3 nodes
  status             Print process + /cluster/status for each node
  leader             Print current leader id
  start-node <id>    Start one node (n1|n2|n3)
  stop-node <id>     Stop one node (n1|n2|n3)
  clean              Stop nodes and remove .cluster data
USAGE
}

cmd="${1:-}"
case "$cmd" in
  start)
    ensure_bin
    start_node n1
    start_node n2
    start_node n3
    ;;
  stop)
    stop_node n1
    stop_node n2
    stop_node n3
    ;;
  status)
    status_cluster
    ;;
  leader)
    leader
    ;;
  start-node)
    ensure_bin
    start_node "${2:-}"
    ;;
  stop-node)
    stop_node "${2:-}"
    ;;
  clean)
    clean_cluster
    ;;
  *)
    usage
    exit 1
    ;;
esac
