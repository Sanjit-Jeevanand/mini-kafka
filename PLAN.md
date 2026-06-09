# Build Plan — 12 Phases (mini-Kafka in Go, Production-Grade)

Each phase: **teach → build to standard → break it (as a test) → understand → ADR + measured bullet.**
Every phase must clear the **production bar** in `BRIEF.md` (tests under `-race`, vet/lint, security,
observability, reliability, operability, delivery) before it counts as done. Don't skip the "break it"
step, and don't skip turning that failure into a regression/chaos test. The four differentiators
(self-built primitives, a correctness simulator, earned numbers, production-grade writeup) are
first-class throughout, not afterthoughts.

**Core (the CV-defining work):** Phases 0–7 (storage engine → protocol → partitions → consumers →
replication → leader election → correctness simulator). **Elevation to "production / exceptional":**
8–11 (exactly-once, performance, observability, Kubernetes + CI/CD + DR). If short on time, ship 0–7,
then 9 (numbers) and a basic 11 (K8s deploy).

---

## Phase 0 — Go foundations & quality gates
*Goal: a repo where it is impossible to merge broken, racy, unvetted, or insecure Go.*
1. Go module, sensible layout (`cmd/broker`, `cmd/cli`, `internal/log`, `internal/proto`, `internal/cluster`, `internal/sim`).
2. Wire `golangci-lint`, `gofumpt`, `go vet`, `go test -race`, `govulncheck` — all runnable via a `Makefile`/`justfile`.
3. `pre-commit` hooks running the same checks; a GitHub Actions CI workflow running them (with the **race detector**) on every push/PR.
4. Env-driven config (12-factor) + structured JSON logging with a `correlation_id` from line one; `context.Context` threaded through.
5. **Break it:** open a PR with a data race and an unformatted file → watch CI (`-race` + lint) go red and **block the merge**.
- **Learn:** Go toolchain, the race detector, table-driven tests, quality gates, why tooling-as-code beats discipline, pre-commit vs CI.
- **Bullet:** "Set up a race-gated Go CI pipeline (golangci-lint/vet/`go test -race`/govulncheck) via pre-commit + GitHub Actions, blocking non-conforming merges."

## Phase 1 — The segmented log storage engine (single broker, single partition)
*Goal: a durable, crash-safe append-only log — the heart of the whole system.*
1. Records (`offset`, `timestamp`, `key`, `value`, `CRC`) appended to a **segment** file; roll to a new segment at a size threshold.
2. A **sparse index** (`offset → file position`) per segment for O(log n) seeks; rebuild it from the log on startup.
3. `Append(record) → offset` and `Read(offset) → record`; bounded read buffers; `fsync` policy made explicit.
4. **Crash recovery:** on startup, scan the active segment, validate CRCs, truncate a torn final write cleanly.
5. Unit + property tests (`rapid`): for any sequence of appends, reads return them in order with stable offsets; CRC catches corruption.
6. **Break it:** `kill -9` mid-append, then corrupt a byte in a segment → restart and assert recovery detects/truncates and never returns a bad record.
- **Learn:** append-only logs, segments + sparse indexes, CRC integrity, page cache vs fsync trade-offs, crash-consistent recovery, why sequential I/O is fast.
- **Bullet:** "Built a segmented append-only commit-log storage engine in Go (sparse offset index, CRC integrity, crash-safe recovery), validated by property-based tests."

## Phase 2 — Wire protocol + Produce/Fetch over TCP
*Goal: talk to the log over the network, with batching and backpressure.*
1. A **length-prefixed binary protocol** (versioned header, request/response framing); encode/decode with bounds checks (no unbounded allocs from the wire).
2. `Produce` (append a batch) and `Fetch` (read from an offset) RPCs; a TCP server with a bounded connection/goroutine model.
3. **Producer batching** (size/linger) and `Fetch` returning record batches; a tiny CLI client.
4. Integration test (testcontainers-go) over the real socket; record **Produce P99** as a baseline.
5. **Break it:** send a truncated frame / lying length prefix → server rejects cleanly without crashing or over-allocating; pin as a fuzz/regression test.
- **Learn:** binary framing, batching to amortize syscalls, backpressure on a TCP server, bounded concurrency, defensive decoding, fuzzing untrusted input.
- **Bullet:** "Designed a versioned length-prefixed TCP protocol with batched Produce/Fetch and fuzz-tested defensive decoding."

## Phase 3 — Topics, partitions & per-partition ordering
*Goal: parallelism with a strict ordering guarantee.*
1. Topics with N **partitions**, each its own log; a **partitioner** (hash(key) → partition) with sticky behavior for null keys.
2. Independent per-partition writer goroutines; assert **per-partition total order** under many concurrent producers.
3. Expose partition metadata; a `Metadata` RPC so clients route to the right partition.
4. Property test: across any concurrent interleaving, each partition's offsets are gap-free and monotonically increasing.
5. **Break it:** hammer one partition from many producers → confirm ordering holds and throughput scales with partition count (plot it).
- **Learn:** partitioning for horizontal scale, the ordering-vs-parallelism trade-off (order only *within* a partition), key-based routing, sharding.
- **Bullet:** "Implemented topic partitioning with key-based routing and a per-partition total-order guarantee verified under concurrent load."

## Phase 4 — Consumers, offsets & consumer groups
*Goal: many consumers read cooperatively, tracking progress durably.*
1. `Fetch`-by-offset consumers; **committed offsets** stored durably (in an internal `__offsets` topic/log).
2. **Consumer groups**: assign partitions across group members; **rebalance** (sticky) when a member joins/leaves; a group coordinator.
3. At-least-once delivery semantics by default; document the duplicate window.
4. Test: kill a consumer mid-group → partitions reassign; no partition is unowned; no committed offset goes backwards.
5. **Break it:** crash a consumer between processing and committing → on restart it re-reads from the last commit (at-least-once); assert no loss.
- **Learn:** pull vs push, consumer offsets, consumer-group rebalancing, coordinator pattern, at-least-once vs exactly-once consumption, the commit/process ordering trap.
- **Bullet:** "Built consumer groups with sticky rebalancing and durable offset commits, providing at-least-once delivery with no offset regression."

## Phase 5 — Replication: leader/follower + ISR (centerpiece, part 1)
*Goal: a partition survives a broker dying — without losing acknowledged data.*
1. Multi-broker cluster; each partition has a **leader** + **followers**; followers **fetch** from the leader and append to their own log.
2. **High-watermark** = highest offset replicated to all ISR members; consumers only read up to it. `acks=0/1/all` on Produce.
3. **ISR (in-sync replica) set**: a follower that falls behind/stalls is removed from ISR; `acks=all` waits for the full ISR.
4. Tests: `acks=all` only acknowledges after ISR replication; killing a follower shrinks ISR and (optionally) blocks `acks=all`; restart re-syncs.
5. **Durability test (the big one):** `acks=all`, then `kill -9` the **leader** → assert no acknowledged record is lost once a new leader serves.
6. **Break it:** partition a follower so it lags → watch ISR shrink and `under_replicated_partitions` rise; heal → it rejoins ISR.
- **Learn:** leader/follower replication, high-watermark, ISR, acks semantics, the durability-vs-latency trade-off, replication lag as a first-class signal.
- **Bullet:** "Hand-built leader/follower replication with an ISR set, high-watermark, and tunable acks, guaranteeing zero acknowledged-data loss on leader `kill -9`."

## Phase 6 — Leader election & the controller (centerpiece, part 2)
*Goal: when a leader dies, the cluster picks a new one — correctly, with no log divergence.*
1. A **controller** tracking broker membership (heartbeats/leases) and assigning partition leaders.
2. **Leader election** on broker/leader failure using **epochs/terms** (Raft-inspired): a new leader has a higher epoch; stale leaders step down (fencing).
3. **Log truncation to the high-watermark** on leader change so followers can never keep records the new leader doesn't have (no divergence).
4. Bound and measure **failover time**; producers/consumers transparently retry to the new leader.
5. **Break it:** `kill -9` a leader broker → new leader elected within the SLO; assert **per-partition order is preserved across failover** and no acknowledged record is lost; induce a "stale leader returns" split-brain and confirm fencing rejects it.
- **Learn:** leader election, terms/epochs, fencing & zombie leaders, log divergence + truncation, split-brain, why a high-watermark makes failover safe, controller pattern.
- **Bullet:** "Built epoch-fenced leader election + high-watermark log truncation, surviving leader `kill -9` with sub-5s failover, preserved ordering, and zero data loss."

## Phase 7 — Correctness: deterministic fault-injection simulator (prove it)
*Goal: stop hoping it's correct; mechanically prove the invariants under adversarial failures.*
1. A **seeded, deterministic simulator** that drives the replication/election logic over a virtual clock + virtual network, injecting crashes, restarts, partitions, message reordering, and delays.
2. Express the invariants as **property-based checks** (`rapid`): (a) no acknowledged-record loss, (b) per-partition total order, (c) no committed-offset regression, (d) ISR/high-watermark safety, (e) at most one leader per epoch.
3. Shrinking: when a property fails, the simulator **minimizes** the schedule to the smallest reproducer; capture each found bug as a permanent regression test.
4. Run the simulator across **thousands of seeds in CI** as a required gate.
5. **Break it:** deliberately reintroduce a known-bad ordering (e.g. ack before ISR replication) → confirm the simulator catches it and produces a minimal failing schedule.
- **Learn:** deterministic simulation testing (FoundationDB/TigerBeetle style), property-based testing, invariant thinking, schedule shrinking, why this beats flaky integration chaos.
- **Bullet:** "Built a deterministic fault-injection simulator (crashes/partitions/reorderings) with property-based invariant checks; found and pinned N correctness bugs across 10,000+ randomized schedules, zero data loss or ordering violations in CI."

## Phase 8 — Idempotent producer + exactly-once produce (signature feature)
*Goal: a retried produce never duplicates a record.*
1. Assign each producer a **producer id**; tag batches with **monotonic sequence numbers** per partition.
2. The leader tracks the last sequence per (producer, partition) and **dedupes** retries/out-of-order batches → exactly-once produce.
3. Surface clear errors for sequence gaps; document the guarantee boundary (produce-side exactly-once; consumer side stays at-least-once unless offsets are committed atomically).
4. Property test: for any interleaving of retries/duplicates/leader changes, each record's effect appears **exactly once**.
5. **Break it:** force a produce retry across a leader failover → assert the record is not duplicated.
- **Learn:** idempotent producer, sequence fencing, at-least-once vs exactly-once *effects*, dedup windows, why exactly-once is a property of the *whole* path.
- **Bullet:** "Implemented an idempotent producer (producer id + per-partition sequence fencing) giving exactly-once produce, property-tested across retries and leader failover."

## Phase 9 — Performance: load test, profile, and tune
*Goal: real, defensible numbers — and proof it degrades gracefully.*
1. A custom Go load generator: ramp producers/consumers to find **max sustained records/sec** and **Produce P99** at each acks level; plot throughput vs partitions and vs brokers.
2. **pprof**-guided bottleneck hunt: fix one real bottleneck (e.g. per-record fsync → **group commit**, syscall overhead → **batching**, copies → **zero-copy `sendfile`**, lock contention → sharding). Record **before/after** with flame graphs.
3. Add a **performance-regression budget** to CI (fail if throughput/P99 regress beyond a threshold).
4. **Break it:** push past capacity → confirm backpressure/shedding holds, latency stays bounded, nothing crashes or loses acknowledged data.
- **Learn:** load testing, pprof/flame graphs, group commit, zero-copy, batching economics, lock contention, throughput = brokers × partitions × per-partition rate, perf budgets.
- **Bullet:** "Load-tested to X records/sec at Produce P99 < Y ms; pprof-guided group-commit + zero-copy changes for Nx throughput; added a CI performance budget."

## Phase 10 — Observability & SLOs
*Goal: see inside the cluster; turn replication lag and SLOs into operational signals.*
1. `/metrics` (Prometheus): RED (rate/errors/duration) + **replication lag**, **under-replicated partitions**, ISR size, bytes & records/sec, request-latency histograms.
2. **OpenTelemetry** traces spanning producer → leader → follower, joined by `correlation_id`; structured JSON logs throughout.
3. Define **SLOs** (Produce P99, durability, max replication lag, leader-election time) with **alerting rules** + a **Grafana dashboard**; add `/health` + `/ready`.
4. **Break it:** stall a follower → watch **replication lag** and **under-replicated-partitions** climb on the dashboard and fire the alert *before* any acknowledged data is at risk.
- **Learn:** replication lag as the #1 health signal, RED/USE, SLI/SLO/error budgets, distributed tracing across the replication path, health vs readiness.
- **Bullet:** "Instrumented replication-lag/under-replicated-partition metrics, OpenTelemetry tracing, and SLO alerting (Grafana) for end-to-end cluster visibility."

## Phase 11 — Kubernetes deploy + CI/CD + DR
*Goal: it runs as a real distributed system on Kubernetes, ships safely on every push, and survives disaster.*
1. **Multi-stage, non-root, distroless** Dockerfile; image scanned with **Trivy** in CI (fail on high-severity CVEs).
2. **Kubernetes**: brokers as a **StatefulSet** with stable network IDs + **PersistentVolumeClaims** (durable logs), a **headless Service** for peer discovery, **readiness gates**, **PodDisruptionBudgets**, resource limits; deploy locally on **kind/minikube** and to **EKS via Terraform**. Helm or kustomize for manifests.
3. **Graceful shutdown / rolling updates:** on `SIGTERM`, stop accepting, flush, hand off partition leadership, exit within the grace period so a rolling restart loses nothing.
4. **GitHub Actions**: `test(-race) → lint/vet → govulncheck → build → Trivy scan → deploy`, with a **rolling update + automated rollback** on failed readiness.
5. **Disaster recovery:** **PV snapshot** backup; **test a restore** of a broker's data; document **RPO/RTO**. Write the README (architecture diagram, ADRs, benchmark report with graphs, trade-offs, measured metrics) + runbooks.
6. **Break it:** `kubectl delete pod` a broker mid-load → StatefulSet reschedules it, the PVC reattaches, ISR re-syncs, leadership fails over, and **no acknowledged record is lost**; contrast by removing the SIGTERM handler.
- **Learn:** StatefulSets vs Deployments (stable identity + storage), PVCs, headless services + discovery, PodDisruptionBudgets, rolling updates, graceful drain on K8s, IaC, secrets, DR (RPO/RTO).
- **Bullet:** "Deployed the cluster on Kubernetes (StatefulSets + PVCs + PDBs) via Terraform/EKS with a race- and CVE-gated GitHub Actions pipeline, rolling deploy + rollback, graceful-drain leadership handoff, and a tested PV backup/restore (DR)."

---

## Suggested pace
~4–6 weeks at 1–2 hrs/day (replication + election + the simulator are the time sinks, and worth it).
Budget more than the job queue: this implements the hard primitives instead of orchestrating them, and
you're learning Go in parallel. **Phases 0–7 are the core** (storage engine, protocol, partitions,
consumers, replication, election, the correctness simulator). 8–11 turn a strong project into one that
reads as *production distributed systems*. If short on time, ship 0–7, then 9 (numbers) + a basic 11 (K8s).

## The payoff
By the end you'll have *built*, to a standard you can defend in a code review: a storage engine,
partitioning, replication + ISR, self-built leader election with fencing, exactly-once produce, a
deterministic correctness simulator, real load-tested numbers, full observability, and a Kubernetes
deployment with DR — **in Go**. That closes the Go and Kubernetes keyword gaps, complements the job
queue's async/eventual story with hard ordered/replicated/consensus depth, and gives you a second
project most new grads can't touch. Paired with one ML/systems project from your CV, that's near-complete
keyword coverage for backend SDE I, plus the rare ability to *defend* every word in a system-design loop.
