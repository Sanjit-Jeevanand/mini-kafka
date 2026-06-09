# Project Brief: Distributed Commit Log (mini-Kafka, Production-Grade, Learn-As-You-Build)

## What we're building
A **distributed, partitioned, replicated commit log** in **Go** — a mini-Kafka, built to the standard
you'd defend in a real code review, not "good enough for a demo." Producers append records to
topic partitions over a hand-rolled binary protocol; records persist to a **segmented append-only log
on disk**; partitions are **replicated across brokers** with a leader/follower model, an in-sync-replica
(ISR) set, and **leader election we build ourselves**; consumers read by offset with consumer groups
and rebalancing. The system must guarantee **per-partition ordering**, **zero loss of acknowledged
records** through broker crashes and leader failover, **exactly-once produce** (idempotent producer),
and be fully observable, tested, and shippable on Kubernetes on every push.

This is the second of a two-project backend portfolio (the first is a distributed job queue). The two
are deliberately complementary: the queue covers async / at-least-once / eventual semantics; this
covers **ordered, replicated, strongly-durable state and self-built leader election**. Written in **Go**
on purpose — it closes the "Python-only" gap, and Go is the idiomatic language for distributed infra.

Target final architecture:
```
Producers ─(binary TCP protocol, batched)─┐
                                          ▼
                         ┌──────── Broker (leader for partition P) ────────┐
                         │  Segmented log (segments + sparse index + CRC)   │
                         │  Page cache · group fsync · high-watermark       │
                         └───────────────┬───────────────┬─────────────────┘
                                         │ follower fetch │ follower fetch
                                         ▼                ▼
                                  Broker (follower)   Broker (follower)   ← ISR set
                                         ▲
                  Controller (broker membership + partition leader election, epoch/term-based)
                                         │
   Consumers ─(consumer groups · offset commits · rebalance)─ read from partition leaders
                                         │
   Observability: Prometheus (throughput, replication lag, under-replicated partitions) +
                  OpenTelemetry traces + structured logs + SLO alerts (Grafana)
                                         │
   Deterministic fault-injection simulator (crashes / partitions / reorderings) gates correctness in CI
                                         │
   Docker (multi-stage, non-root) → Kubernetes (StatefulSets + PVCs + headless svc) → Terraform (EKS)
                                         → GitHub Actions CI/CD (quality-gated, rolling deploy + rollback)
```

## How I want you to work with me (the learning loop — IMPORTANT)
This is a **learning project** *built to production standards*. The goal is that *I* understand
distributed systems **and** what "production-ready" actually means by the end. I am new to Go; teach
the Go idiom alongside the systems concept, but never let "learning Go" lower the bar.

For every phase and every meaningful step:
1. **Teach first.** Before writing code, explain the concept in 4–8 sentences: the problem it solves,
   the trade-offs, and how real systems (Kafka, Raft, etcd, BookKeeper) handle it. Name the
   system-design term explicitly (ISR, high-watermark, leader epoch, log truncation, etc.).
2. **Build incrementally, to standard.** Write the smallest slice that works *and meets the production
   bar below* — tests, types, race-checks, logging included, not bolted on later. One concept → one
   change → verify → next. Don't generate three files at once.
3. **Make me reason.** Before each non-trivial decision, ask me what I'd do and why. If I'm wrong,
   correct me with the reasoning. Pose a "what breaks if…?" question (e.g. "what if the leader acks,
   then dies before the follower fetches?").
4. **Force the failure — as an automated test.** After the happy path, deliberately break it (kill the
   leader mid-replication, partition a follower, corrupt a segment, duplicate a produce) so I *see* the
   failure mode — then capture it as a regression/chaos test in CI so it can never silently return.
   Never skip this step.
5. **Connect to interviews.** End each phase with: the resume bullet it earns (with a real measured
   metric, not a placeholder), the interview question it lets me answer, and a one-paragraph
   **Architecture Decision Record (ADR)** capturing the choice and trade-off.

## What makes this *exceptional*, not generic (the non-negotiable differentiators)
A glued-together log that leans on a library for the hard parts is forgettable. These four make it stand out;
treat them as first-class, not extras:
1. **Build the hard primitive myself.** The **segmented log storage engine**, the **replication + ISR**,
   and the **leader election** are hand-built and explained, not delegated to a library. This is the point.
2. **Prove correctness, don't assert it.** A **deterministic, seeded fault-injection simulator** interleaves
   crashes, partitions, and reorderings; **property-based tests** (rapid) assert the invariants
   (no acknowledged-record loss, per-partition total order, no committed-offset regression, ISR safety).
   "I built a simulator that found N divergence bugs" is the headline.
3. **Earn the numbers.** Every performance claim comes from a load test + a **pprof-guided** bottleneck
   hunt with before/after (batching, group fsync, zero-copy). The optimization *story* is the signal.
4. **Ship it like production, and write it up like production.** Kubernetes StatefulSets, ADRs,
   architecture diagram, a benchmark report with graphs, runbooks, and a "failures I induced and how the
   system survived" section. Half of "exceptional" is that a reviewer sees the depth in 30 seconds.

## The production bar (non-negotiable — applies to *every* phase)
Nothing is "done" until it clears all of these. We build them in from the start, not retrofit them.

- **Testing.** Unit tests for logic; integration tests against real brokers via **testcontainers-go**;
  the phase's failure mode captured as an automated test. Core reliability logic (replication, election,
  offsets, idempotency) covered by **property-based tests** (`rapid`) and the **deterministic simulator**.
  Everything runs under `go test -race`. CI is the gate — red build never merges.
- **Type safety & style.** `go vet` clean; `golangci-lint` clean; `gofumpt`-formatted; **no data races**
  (race detector in CI). No `//nolint` without a justified comment. `context.Context` on every blocking call.
- **Security.** AuthN on the client protocol (API keys / mTLS between brokers), per-tenant/topic authZ,
  all inputs validated and length-bounded (no unbounded allocations from the wire), secrets only via
  env / Kubernetes Secrets (never in code or git), least-privilege IAM, dependency scanning
  (`govulncheck`) and container image scanning (Trivy) in CI.
- **Observability.** Structured JSON logs with a `correlation_id`/request id propagated end-to-end;
  Prometheus metrics (RED + **replication lag** + **under-replicated partitions** + ISR size +
  bytes/records per sec); OpenTelemetry traces across producer → leader → follower; defined **SLOs**
  with alerting rules and a Grafana dashboard.
- **Reliability.** Graceful shutdown (SIGTERM → stop accepting, flush, hand off leadership), `/health` +
  `/ready` probes, explicit timeouts on every network/disk call, bounded goroutine pools and buffers,
  CRC checksums on every record, crash recovery on restart, retries with backoff + jitter, idempotent
  producer, and **log truncation to the high-watermark** on leader change (no divergent logs).
- **Operability.** 12-factor config via env; on-disk format and metadata changes are **versioned with a
  migration/upgrade path**; infrastructure as code via **Terraform**; **Kubernetes** manifests/Helm under
  source control; runbooks for each failure mode; ADRs for each significant decision.
- **Delivery.** CI/CD pipeline with ordered quality gates — `test(-race) → lint/vet → vuln scan → build →
  image scan → deploy` — Kubernetes **rolling update** with automated rollback on failed readiness checks.

## Non-functional requirements / SLOs (measure these, don't guess)
*Numbers below are build-to TARGETS. Phase 9 measures the real ones on documented hardware; we then
update the resume bullets to the measured values. Report the true number even if lower.*
- **Throughput:** sustain a documented **≥ 100k small records/sec per broker** with batching; scale with
  partitions/brokers until a named bottleneck.
- **Produce latency:** `acks=all`, RF=3, **P99 < 20 ms** under sustained load.
- **Durability:** **zero loss** of any `acks=all`-acknowledged record, even on `kill -9` of the leader.
- **Ordering:** **per-partition total order** preserved under concurrent producers *and* across leader
  failover (no log divergence; followers truncate to high-watermark).
- **Correctness:** **exactly-once produce** via idempotent producer (producer id + sequence numbers).
- **Availability:** survive the loss of any single broker at RF=3; **leader failover < 5 s**; API target 99.9%.
- **Resilience:** under overload, **backpressure / shed** with bounded latency — degrade, don't collapse.
- **Disaster recovery:** documented **RPO/RTO**; tested **PV snapshot backup + restore** of broker data.

## Tech constraints
- **Language:** **Go 1.22+**. Idiomatic Go — goroutines + channels for concurrency, `context` for
  cancellation/timeouts, `errors.Is/As`. I'm new to Go, so teach the idiom, but hold the bar.
- **Storage:** **own** segmented append-only log on disk (segments + sparse `.index` + CRC); **no external
  DB for the log**. Cluster metadata in an embedded store (BoltDB) or self-managed — relational DB depth
  already lives on my CV from the queue project, so here the depth is the **storage + replication engine**.
- **Networking:** **hand-rolled length-prefixed binary protocol over TCP** for the client and inter-broker
  paths — I want to learn framing, batching, and backpressure, not have a framework hide them. (gRPC
  allowed *only* for the control plane if it clearly buys correctness, with an ADR justifying it.)
- **Tooling:** `go test -race`, `go vet`, `golangci-lint`, `gofumpt`, **`rapid`** (property tests),
  **testcontainers-go**, **pprof** (profiling), a custom load generator (and/or `k6` against a gateway),
  `govulncheck`, Trivy, OpenTelemetry-Go, Prometheus client, `pre-commit`.
- **Infra:** Docker (multi-stage, non-root, distroless) locally via Compose **and** **Kubernetes**
  (StatefulSets + PVCs + headless Service + PodDisruptionBudgets) on **kind/minikube** locally and
  **EKS via Terraform** in the cloud. Helm or kustomize for manifests. GitHub Actions for CI/CD.
- Prefer the stdlib / one obvious library over frameworks that hide the mechanism I'm trying to learn —
  but never at the cost of the production bar above.

## Definition of done (per phase)
Code runs and is **demoable**; it **clears the entire production bar** (tests green under `-race` in CI,
vet/lint clean, secured, observable); I can **explain *why* every component exists and what I rejected**;
I've **watched the relevant failure happen, be handled, and be pinned by a test**; and I've written the
**ADR + resume bullet (with a measured metric)**. Track progress in a `PROGRESS.md` checklist. Don't
advance phases until I confirm I understand.

## My background
Strong Python/ML, **new to Go**, weaker on backend systems depth (storage engines, replication,
consensus, ordering) and on production engineering practice (testing, CI/CD, IaC, K8s, observability).
Targeting MAANG new-grad SDE. Studying system design in parallel — tie concepts to it. This project,
plus the job queue, is meant to light up the backend SDE keyword set (Go, Kubernetes, distributed
systems, replication, streaming) and, more importantly, let me *defend* every one of those words.
