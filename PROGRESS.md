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
