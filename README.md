# Distributed Key-Value Store (Raft, Go stdlib)

Educational but production-minded distributed key-value store in Go using Raft consensus.

- 3 independent local processes
- Leader election + heartbeats
- Majority-based log replication and commit
- WAL-backed recovery (`currentTerm`, `votedFor`, log, commit index)
- HTTP/JSON client API + node-to-node Raft RPC
- Follower write redirection with HTTP `307`

## Repository Layout

- `cmd/kvnode/main.go` - process entrypoint, flags, wiring, shutdown
- `raft/` - Raft state machine, elections, replication, commit/apply pipeline
- `kv/` - deterministic command encoding/decoding + in-memory KV apply state machine
- `transport/` - HTTP server (client API + Raft RPC handlers) and Raft HTTP client
- `storage/` - append-only WAL and replay
- `scripts/` - cluster lifecycle + demo + chaos test + benchmark
- `benchmarks/` - generated benchmark results
- `README.md`

## Architecture Diagram (ASCII)

```text
                        +-----------------------+
                        |       HTTP Client     |
                        | PUT/DEL/GET/STATUS    |
                        +-----------+-----------+
                                    |
                                    v
        +-------------------------------------------------------+
        |                   3-Node Raft Cluster                 |
        |                                                       |
        |  +------------------+     +------------------+        |
        |  | n1: Raft + KV    |<--->| n2: Raft + KV    |        |
        |  | WAL: .cluster/...| RPC | WAL: .cluster/...|        |
        |  +------------------+     +------------------+        |
        |           ^                         ^                  |
        |           |                         |                  |
        |           +-----------<------------+                  |
        |                       RPC                              |
        |                  +------------------+                  |
        |                  | n3: Raft + KV    |                  |
        |                  | WAL: .cluster/...|                  |
        |                  +------------------+                  |
        +-------------------------------------------------------+

Legend:
- Client writes go to leader (followers return HTTP 307 redirect).
- Raft RPC = RequestVote + AppendEntries.
- Committed log entries are applied in order to each node's KV state machine.
```

## Clean Demo Command (One-Liner)

```bash
./scripts/demo.sh
```

What it does:

- starts a clean 3-node cluster
- finds leader
- executes `PUT`, `GET`, `DEL`, `GET`
- prints cluster status
- shuts down cleanly

## CLI

Each node runs:

```bash
go run ./cmd/kvnode \
  --id n1 \
  --listen 127.0.0.1:9101 \
  --peers n1=127.0.0.1:9101,n2=127.0.0.1:9102,n3=127.0.0.1:9103 \
  --data-dir .cluster/data/n1
```

Required flags:

- `--id`
- `--listen`
- `--peers`
- `--data-dir`

## Client API (HTTP/JSON)

- `PUT /kv/put` body: `{"key":"k","value":"v"}`
- `POST /kv/del` body: `{"key":"k"}`
- `GET /kv/get?key=k`
- `GET /cluster/status`

Status response:

```json
{
  "id": "n1",
  "role": "leader",
  "currentTerm": 4,
  "leaderId": "n1",
  "commitIndex": 123,
  "lastApplied": 123
}
```

### Follower write behavior

If a follower receives `PUT /kv/put` or `POST /kv/del`, it returns:

- `307 Temporary Redirect`
- `Location` header with leader URL
- JSON payload containing `leaderId` and `leaderAddr`

## Raft RPC Endpoints

- `POST /raft/requestVote`
- `POST /raft/appendEntries`
- `POST /raft/installSnapshot` (stubbed; returns `501`)

## How To Run Locally

### 1) Start cluster

```bash
./scripts/run_cluster.sh start
```

Ports used by default:

- `n1 -> 127.0.0.1:9101`
- `n2 -> 127.0.0.1:9102`
- `n3 -> 127.0.0.1:9103`

### 2) Check status

```bash
./scripts/run_cluster.sh status
./scripts/run_cluster.sh leader
```

### 3) Basic API calls

```bash
curl -X PUT http://127.0.0.1:9101/kv/put -H 'Content-Type: application/json' -d '{"key":"a","value":"1"}'
curl http://127.0.0.1:9102/kv/get?key=a
curl http://127.0.0.1:9103/cluster/status
```

### 4) Stop cluster

```bash
./scripts/run_cluster.sh stop
```

## Distributed Chaos Test

Full distributed failure scenario (`scripts/chaos_test.sh`):

1. Start 3 nodes
2. Write 100 keys
3. Kill current leader process
4. Wait for new leader election
5. Verify reads still return committed data
6. Write additional keys on new leader
7. Restart old leader
8. Verify restarted node catches up

Run:

```bash
./scripts/chaos_test.sh
```

## Benchmark Script + Results

Run benchmark:

```bash
./scripts/benchmark.sh 300 300
```

Outputs:

- machine-readable summary printed to terminal
- persisted markdown report at `benchmarks/latest.md`

Latest generated results in this repo (`benchmarks/latest.md`):

- Generated (UTC): `2026-02-22T23:35:50Z`
- Write throughput: `43.23 ops/s` (avg `23.133 ms/op`)
- Read throughput: `93.49 ops/s` (avg `10.697 ms/op`)

## Unit Tests

Raft unit tests focus on transitions and log matching:

- vote handling across term changes
- conflicting suffix truncation and replacement
- stepping down on higher-term append

Run:

```bash
go test ./...
```

## Raft Implementation Choices

- **Election timeouts**: randomized per node (`350-700ms` default)
- **Heartbeats**: fixed interval (`120ms` default)
- **Commit rule**: leader commits entries from current term after majority replication
- **Leader no-op on election**: new leader appends a no-op entry to advance safe commit of prior-term tail entries
- **Log matching**: follower rejects mismatched `prevLogIndex/prevLogTerm`; leader backs off `nextIndex`
- **Apply order**: strictly increasing index; state machine receives committed entries in order
- **Persistence**: append-only WAL records (`state`, `entry`, `truncate`, `commit`) with `fsync` per record
- **Recovery**: WAL replay reconstructs term/vote/log/commit; committed entries are reapplied deterministically on startup

## Design Tradeoffs

- **Durability vs write throughput**: WAL `fsync` on each record improves crash safety but reduces throughput.
- **Simplicity vs read freshness**: follower reads are served locally (simple, fast) but may be stale during lag.
- **Fast failover vs heartbeat traffic**: shorter heartbeat/election timing improves failover latency but increases steady-state RPC volume.
- **Implementation clarity vs feature completeness**: snapshot install/compaction is intentionally deferred to keep the core Raft path readable.
- **Operational ease vs security**: plain HTTP and no auth simplify local experimentation but are not production-hardening features.

## Consistency Guarantees

- **Writes**: committed only after majority replication (linearizable write path through leader)
- **Reads (`/kv/get`)**: served from local node state machine; on followers this can be stale if lagging
- **Failure tolerance**: with 3 nodes, cluster continues after 1 node failure

## Limitations

- Snapshot installation/compaction is not fully implemented (`/raft/installSnapshot` is currently a stub)
- WAL grows indefinitely (no compaction yet)
- Read path does not enforce leader lease/read-index; follower reads are eventually consistent
- No TLS/authentication; intended for local/dev and educational usage
