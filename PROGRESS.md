# Build Progress

## Phase 0 — Engineering Foundations
Goal: a repo where it is impossible to merge broken, unvetted, or eval-regressing code.

### Done
- [x] `go mod init github.com/sanjit-jeevanand/mini-kafka` → created `go.mod`
- [x] Created directory structure: `cmd/broker/`, `internal/log/`, `eval/results/`, `infra/`, `docs/adr/`
- [x] Created `internal/config/config.go` — 12-factor config from env vars (prefix: MK_)
- [x] Created `internal/logger/logger.go` — structured JSON logging via `log/slog`, request_id via context
- [x] Created `eval/gate.go` — fails CI if eval/results/latest.json missing or lacks sentinel
- [x] Created `eval/results/latest.json` with sentinel
- [x] Created `internal/config/config_test.go` + `internal/logger/logger_test.go` — smoke tests
- [x] Created `.golangci.yml` — lint rules (errcheck, govet, staticcheck, unused)
- [x] Created `Makefile` — lint, test, audit, eval-gate, ci targets
- [x] Created `.pre-commit-config.yaml` — go-fmt, go-vet, trailing whitespace
- [x] Created `.gitignore`
- [x] Created `.github/workflows/ci.yml`

### Next Steps
- [ ] `go mod tidy` → generate `go.sum`
- [ ] `pre-commit install`
- [ ] `make ci` → verify full pipeline passes
- [ ] Create GitHub repo and push

### Key Differences from Python Projects (rag-engine, distributed-job-queue)

- `go.mod` + `go.sum` replace `pyproject.toml` + `uv.lock`
- `golangci-lint` replaces ruff + mypy (lint + type safety in one tool — Go is statically typed)
- `govulncheck` replaces pip-audit
- `log/slog` (stdlib, Go 1.21+) replaces structlog — no external dependency needed
- `context.Context` carries request_id instead of ContextVar — Go's standard for request-scoped values
- No virtual environment — Go modules handle isolation via `go.sum`

---

## Phase 1 — Segmented Log Storage Engine

Goal: a durable, crash-safe append-only log — the storage layer every phase above builds on.

### Done
- [x] `internal/log/segment.go` — single segment: append records with CRC, sparse index, read-by-offset
- [x] `internal/log/index.go` — sparse offset→position index; mmap-friendly layout
- [x] `internal/log/record.go` — on-disk record format: `[4 CRC][8 offset][8 timestamp][4 key_len][key][4 val_len][val]`
- [x] `internal/log/recover.go` — startup scan: validate CRCs, truncate torn final write, rebuild index
- [x] `internal/log/log.go` — multi-segment Log; `Append` / `Read` / `roll` / `findSegment` / `DeleteSegmentsBefore`
- [x] `internal/log/log_test.go` — unit + property tests; crash-recovery test; corruption-detection test

### Key concepts learned
- **Append-only log:** records are never overwritten; the offset is the address. Enables O(1) appends, sequential disk I/O (fast), and trivial crash recovery.
- **Segmented log:** log split into fixed-size files (segments). Enables retention (delete old segments without rewriting), bounded file sizes, and parallel reads across segments.
- **Sparse index:** instead of indexing every record (expensive), index every Nth byte. Binary-search to the nearest index entry, then scan forward. O(log n) seek, small memory footprint.
- **CRC integrity:** each record carries its own checksum. On read, recalculate and compare — any corruption is detected before the record reaches the caller.
- **Crash recovery:** if the process dies mid-write, the last record may be torn. On startup: scan from the last good index entry, validate CRCs forward, truncate at the first bad record. The log is always in a consistent state after recovery.
- **fsync policy:** `Append` calls `fsync` after every write (safe, slow). Phase 9 introduces group commit — batch many appends into one fsync for a documented throughput trade-off.

### Resume bullet
"Built a segmented append-only commit-log storage engine in Go (sparse offset index, CRC integrity, crash-safe recovery via CRC-based scan + truncation), validated by property-based tests."

---

## Phase 2 — Wire Protocol & TCP Server

Goal: talk to the log over the network, with batching and backpressure.

### Done
- [x] `internal/proto/frame.go` — wire frame constants: 8-byte header `[4 length][2 version][2 type]`, `MaxFrameSize=16MiB`
- [x] `internal/proto/messages.go` — typed Go structs for all RPCs: `ProduceRequest/Response`, `FetchRequest/Response`, `MetaRequest/Response`
- [x] `internal/proto/codec.go` — `WriteFrame` / `ReadFrame`; encode/decode pairs for all message types using `binary.BigEndian`; length-bounded allocs (OOM defense); helper functions `putString` / `getString` / `putBytes` / `getBytes`
- [x] `internal/proto/proto_test.go` — 9 tests: 5 round-trip (all message types), 3 frame tests (round-trip, oversized, bad version), 1 fuzz test (`FuzzReadFrame` — decoder never panics on arbitrary input)
- [x] `internal/server/server.go` — TCP server with semaphore-based backpressure (`chan struct{}`) and graceful shutdown (context cancellation + `sync.WaitGroup`)
- [x] `internal/server/handler.go` — request router: `handleProduce` / `handleFetch` / `handleMeta`; only place the network layer touches the storage layer
- [x] `internal/server/server_test.go` — 4 integration tests over real TCP: `TestProduceAndFetch` (10k records), `TestMetadata`, `TestGracefulShutdown`, `TestConcurrentProducers` (8 producers × 500 records); all pass under `-race`
- [x] `internal/client/client.go` — shared TCP connection with `sync.Mutex` preventing interleaved frames; `BrokerFor` with metadata cache
- [x] `internal/client/producer.go` — batching producer: two flush triggers (batch size cap or linger timer); `pendingRecord` fan-out pattern; `~100x` throughput vs single-record sends
- [x] `internal/client/consumer.go` — polling consumer: maintains `nextOffset` across `Poll()` calls; returns empty slice (not error) when no new records; `Seek()` for replay
- [x] `internal/client/client_test.go` — 4 integration tests: round-trip (200 concurrent sends), seek, metadata cache, explicit flush
- [x] `cmd/broker/main.go` — broker binary: flags, log open, `signal.NotifyContext` for SIGTERM, `ListenAndServe`
- [x] `cmd/cli/main.go` — CLI client: `produce` / `fetch` / `meta` subcommands

### Key concepts learned
- **Length-prefixed binary framing:** `[4-byte length][2-byte version][2-byte type][payload]`. The receiver reads the header first, checks the length, *then* allocates the payload buffer — no unbounded allocs from the wire. Versioning is baked in from day one (a `v2` frame type rejects cleanly against a `v1` server).
- **Batching:** a producer accumulates records and sends them in one syscall. Amortizes the per-RPC cost (TCP round-trip, fsync) across many records. Two flush triggers: a **linger timer** (latency bound — send after at most N ms even if the batch is small) and a **batch size cap** (throughput bound — send when the batch is full even if the timer hasn't fired). Tuning these trades latency for throughput.
- **Semaphore backpressure:** `chan struct{}` with capacity = `maxConns`. Each accepted connection sends to the channel (blocks if full); releases on close. Simple, allocation-free concurrency cap — the OS connection queue absorbs the overflow without the server's goroutine count growing unboundedly.
- **Graceful shutdown:** `signal.NotifyContext` converts SIGTERM into a `context.Context` cancellation. A background goroutine watches `ctx.Done()` and closes the listener. `ln.Accept()` unblocks with an error. The server checks `ctx.Done()` to distinguish "cancelled cleanly" from "real error". `sync.WaitGroup` counts in-flight request goroutines; `wg.Wait()` blocks until they all finish before the process exits.
- **Mutex on shared connection:** producer and consumer share one `*Client` (one TCP connection). Without a mutex, two concurrent `send()` calls interleave their bytes on the wire — the broker reads a garbled frame. The mutex serializes access: one frame at a time.
- **`pendingRecord` fan-out:** `Send()` appends to the batch and parks on a `chan sendResult`. `flushLocked()` snapshots the batch, sends one RPC, then fans the base offset out to every parked goroutine. Each caller computes its own offset as `baseOffset + indexInBatch`.
- **Metadata caching:** `BrokerFor(topic)` queries the broker once and caches the result in `metaCache`. In Phase 2 the broker always returns itself; in Phase 5 the same cache becomes the client-side routing table for multi-broker clusters.
- **TCP as pure transport:** TCP guarantees ordered, reliable byte delivery — it knows nothing about messages. The frame layer imposes message boundaries on the byte stream. This is why the length prefix is necessary: TCP can deliver a frame in two reads (or two frames in one read); `io.ReadFull` handles reassembly.

### Test results
```
--- PASS: TestProduceAndFetch      (41.11s)   # 10k records, fsync per record
--- PASS: TestMetadata             (0.01s)
--- PASS: TestGracefulShutdown     (0.01s)
--- PASS: TestConcurrentProducers  (16.29s)   # 8 producers × 500 records, race-clean
ok  github.com/sanjit-jeevanand/mini-kafka/internal/server  58.633s

--- PASS: TestProducerConsumerRoundTrip  (0.84s)   # 200 concurrent sends, batched
--- PASS: TestConsumerSeek               (0.09s)
--- PASS: TestMetadataCache              (0.01s)
--- PASS: TestProducerFlush              (0.03s)
ok  github.com/sanjit-jeevanand/mini-kafka/internal/client   2.369s
```
All tests pass under `go test -race`. No data races detected.

### Failure mode pinned as a test
`FuzzReadFrame` in `proto_test.go`: the fuzzer throws arbitrary byte sequences at `ReadFrame`. The decoder never panics — it returns a typed error for oversized frames (`ErrFrameTooLarge`), wrong version (`ErrBadVersion`), and truncated payloads (EOF). The `MaxFrameSize` check fires *before* allocating the payload buffer, preventing OOM from a lying length prefix.

### Resume bullet
"Designed a versioned length-prefixed TCP protocol with batched Produce/Fetch, semaphore backpressure, graceful SIGTERM shutdown, and fuzz-tested defensive decoding (decoder rejects lying length prefix before allocating)."

### ADR — Why hand-rolled binary framing over gRPC/protobuf
**Decision:** implement the wire protocol as a hand-rolled length-prefixed binary format with manual `binary.BigEndian` encode/decode.

**Alternatives considered:**
- *gRPC + protobuf:* production default. Handles framing, versioning, retries, load balancing. Rejected for the data plane because it hides the mechanism we're building to learn.
- *encoding/json over TCP:* simple but ~5–10x larger payloads, allocation-heavy parsing, no versioning story.
- *encoding/gob:* Go-only, no stable cross-version wire format.

**Rationale:** the project goal is to understand framing, batching, and backpressure — not to ship production traffic. A hand-rolled protocol forces every decision to be explicit (header layout, length check, version byte, max frame size). The fuzz test proves the decoder is safe against adversarial input without a framework doing it invisibly. gRPC remains the right choice for the control plane (Phase 6) where retries and mTLS matter more than learning the mechanism.

**Trade-off:** more code to maintain; no free retry/load-balancing/mTLS from the framework. Accepted for the data plane; revisit for inter-broker RPC in Phase 6.
