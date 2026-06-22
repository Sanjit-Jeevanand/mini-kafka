<div align="center">

# mini-kafka

**A distributed commit log built from scratch in pure Go.**

Segmented storage · sparse indexing · ISR replication · epoch-fenced leader election · idempotent producers · group commit

[![Go](https://img.shields.io/badge/Go-1.25-00ADD8?logo=go&logoColor=white)](https://go.dev)
[![Dependencies](https://img.shields.io/badge/dependencies-0-success)](go.mod)
[![Tests](https://img.shields.io/badge/tests-71%20passing-success)](#testing)
[![Race](https://img.shields.io/badge/-race-clean-success)](#testing)
[![Throughput](https://img.shields.io/badge/throughput-245k%20rec%2Fs-2563EB)](#benchmarks)
[![License](https://img.shields.io/badge/license-MIT-blue)](LICENSE)
[![Live](https://img.shields.io/badge/live-sanjit--ml.com%2Fmini--kafka-6c8cff)](https://sanjit-ml.com/mini-kafka/)

</div>

---

## What is this?

`mini-kafka` is a Kafka-inspired distributed commit log implemented from first principles — **no external runtime dependencies**, just the Go standard library. It is built in **8 incremental phases**, each one a self-contained layer with its own test suite and a standalone PDF writeup.

It is a learning project taken to production-grade depth: it has a hand-rolled binary wire protocol, crash-safe segmented storage, in-sync-replica replication with high-watermark consistency, epoch-fenced leader election that survives split-brain, an idempotent producer, a group-commit write path that sustains **245k records/sec at P99 < 20 ms**, and a deterministic fault-injection simulator that replays 10,000 randomized failure schedules to hunt for correctness bugs.

```
63 Go files   ·   ~6,300 LOC   ·   71 tests   ·   0 dependencies
```

---

## Highlights

| | |
|---|---|
| 📂 **Segmented commit log** | CRC-stamped records, sparse 8-byte offset index, segment rolling, crash recovery |
| 🔌 **Hand-rolled wire protocol** | Length-prefixed binary frames over raw TCP, fuzz-tested decoder, OOM defence |
| 🧵 **Per-partition ordering** | Key-hashed routing, sticky producer assignment, independent partition logs |
| 👥 **Consumer groups** | Group coordinator, sticky rebalance to minimise churn, durable offset commits |
| 🔁 **ISR replication** | High-watermark consistency, follower fetch loop, ISR shrink/expand on lag |
| 🗳️ **Leader election** | Durable monotonic epochs, epoch fencing rejects stale leaders, sub-3s failover |
| 🧪 **Fault simulator** | Seeded deterministic RNG, virtual clock, invariant checker, reproducible replays |
| ⚡ **Group commit** | One `fsync` per batch instead of per record — 800× throughput at high concurrency |

---

## Quick start

### Run with Docker (recommended)

```bash
git clone https://github.com/sanjit-jeevanand/mini-kafka.git
cd mini-kafka
docker compose -f infra/docker/docker-compose.yml up -d --build
```

This starts two services:

- **Broker** on `:9092` — the commit log
- **Landing page** on `:80` — project overview

### Run from source

```bash
# Start a broker
go run ./cmd/broker -addr :9092 -dir /tmp/mini-kafka -topic orders -partitions 4

# In another terminal, produce and fetch with the CLI
go run ./cmd/cli -broker localhost:9092 produce orders "user-123" "order-placed"
go run ./cmd/cli -broker localhost:9092 fetch   orders 0
go run ./cmd/cli -broker localhost:9092 meta    orders
```

### Broker flags

| Flag | Default | Description |
|------|---------|-------------|
| `-addr` | `:9092` | TCP listen address |
| `-dir` | `/tmp/mini-kafka` | Log segment storage directory |
| `-topic` | `default` | Topic name |
| `-partitions` | `4` | Number of partitions |
| `-max-conns` | `1024` | Maximum concurrent connections |

---

## Architecture

```
┌──────────┐   ┌──────────────┐   ┌─────────────┐   ┌────────────────┐
│ Producer │──▶│ Group Commit │──▶│ Segment Log │──▶│ ISR Replication│
│idempotent│   │  batch fsync │   │ CRC + index │   │ high-watermark │
└──────────┘   └──────────────┘   └─────────────┘   └───────┬────────┘
                                                            │
                                  ┌─────────────────────────▼─────────┐
                                  │  Leader Election + Epoch Fencing   │
                                  │      sub-3s failover, no loss      │
                                  └────────────────────────────────────┘

        All of the above validated by the deterministic Fault Simulator
             ( seeded RNG · virtual clock · 10,000 fault schedules )
```

The write path: a record is checked for idempotency (duplicate detection), enqueued to the group committer, batched behind a single `fsync`, written to the active segment, replicated to all in-sync replicas, and only then acknowledged with its offset.

---

## The 8 phases

Each phase builds on the last and ships with a PDF deep-dive in [`steps/`](steps/).

| Phase | Title | Key components | Tests |
|:-----:|-------|----------------|:-----:|
| **1** | Segmented Commit Log | `record.go` `index.go` `segment.go` `recovery.go` `log.go` | 7 |
| **2** | Wire Protocol & TCP Server | `frame.go` `codec.go` `server.go` `handler.go` `producer.go` | 17 |
| **3** | Topics, Partitions & Ordering | `topic.go` `partition.go` | ✓ |
| **4** | Consumer Groups & Sticky Rebalance | `coordinator.go` `rebalance.go` | 7 |
| **5** | ISR Replication | `replication.go` `isr.go` | 4 |
| **6** | Leader Election & Epoch Fencing | `epoch.go` `election.go` `controller.go` | 5 |
| **7** | Fault Injection Simulator | `sim/clock.go` `sim/network.go` `sim/simulator.go` | 10 |
| **8** | Idempotent Producer & Group Commit | `sequence.go` `groupcommit.go` `eval/metrics.go` | 9 |

---

## Benchmarks

Measured on an Apple M4 Pro, 500 concurrent producers, 5-second window:

```
$ go run ./cmd/metrics -workers 500 -duration 5s

Bullet 1 — throughput & latency
  Group commit: 183,682–244,909 rec/s   P50: 9.2ms   P99: 18.4ms

Bullet 2 — replication & failover
  Acknowledged lost records: 0   Failover: 1200ms–2400ms

Bullet 3 — fault injection
  Scenarios: 10,000   Seeds: 100   Violations found: 7
```

**How group commit works:** instead of each writer paying its own `fsync` (≈4 ms on NVMe, capping you at ~250 rec/s no matter how many goroutines you throw at it), a single background goroutine drains all pending requests, writes them, and calls `fsync` **once** for the whole batch. Throughput becomes `batch_size ÷ fsync_latency`. Batch size is *emergent* — the more concurrent producers, the larger the batches, the fewer syncs per record. **No durability trade-off**: `fsync` still completes before any offset is returned.

```bash
# Raw throughput comparison (per-record fsync vs group commit)
go run ./cmd/loadgen -workers 2500 -duration 5s
```

---

## Design notes

A few decisions worth calling out:

- **Epoch fencing uses `<`, not `≠`.** A sender with a *higher* epoch than the controller means the controller's state is stale — not that the sender is. Only a strictly lower epoch is definitively a stale leader. This asymmetry is the most common bug in fencing implementations, and it's covered by a dedicated test.

- **Advance the sequence number *after* the durable write, never before.** If the process crashes between the dedup check and the write, the producer's retry is treated as new (`seq == lastSeq + 1`) and succeeds. Exactly-once semantics without a two-phase commit.

- **The fault simulator is deterministic.** Same seed → same fault schedule, every run. All RNG draws happen under a mutex *before* the operation, so the schedule is independent of goroutine scheduling. When a 10,000-scenario sweep finds a bug, `Reproduce()` prints the one seed that triggers it.

- **Group commit releases the lock before fanning results out.** The log mutex protects segment state; the result channels don't. Releasing first lets the *next* batch start accumulating while the previous batch's callers are still waking up. That overlap is where the throughput multiplier comes from.

---

## Testing

```bash
# Everything, race detector on
go test -race ./...

# A single package, verbose
go test -race -v ./internal/log/

# The CI performance gate — fails if throughput regresses >10% from baseline
go test -run TestPerfGate ./internal/eval/ -update   # record baseline (first run)
go test -run TestPerfGate ./internal/eval/           # enforce on every run after
```

71 tests across 16 packages, all race-clean — including a property test for offset monotonicity, a fuzz test that proves the frame decoder never panics on adversarial input, a torn-write recovery simulation, and a split-brain fencing scenario.

---

## Project layout

```
mini-kafka/
├── cmd/
│   ├── broker/        # the broker binary
│   ├── cli/           # produce / fetch / meta client
│   ├── loadgen/       # throughput benchmark (per-record vs group commit)
│   └── metrics/       # live CV-metric harness
├── internal/
│   ├── log/           # segmented commit log + group commit   (phase 1, 8)
│   ├── proto/         # wire protocol: frames, codec, messages (phase 2)
│   ├── server/        # TCP server, backpressure, request router (phase 2)
│   ├── client/        # batching producer, polling consumer    (phase 2)
│   ├── topic/         # topics & partitions                    (phase 3)
│   ├── consumergroup/ # group coordinator & sticky rebalance   (phase 4)
│   ├── replication/   # ISR replication & high-watermark       (phase 5)
│   ├── cluster/       # epochs, election, controller           (phase 6)
│   ├── sim/           # deterministic fault simulator          (phase 7)
│   ├── idempotent/    # sequence tracking & idempotent producer(phase 8)
│   ├── eval/          # perf gate & CV-metric harness          (phase 8)
│   ├── config/        # broker configuration
│   ├── health/        # health checks
│   ├── logger/        # structured logging
│   ├── metrics/       # runtime metrics
│   └── tracing/       # request tracing
├── infra/
│   ├── docker/        # Dockerfile + docker-compose
│   ├── k8s/           # StatefulSet, headless service, PDB
│   └── terraform/     # EKS provisioning
├── web/               # landing page
└── steps/             # per-phase PDF writeups
```

---

## Built by

**Sanjit Jeevanand** — [github.com/sanjit-jeevanand](https://github.com/sanjit-jeevanand)

<sub>Not affiliated with Apache Kafka. Built for learning, taken seriously.</sub>
