# make lint       — run golangci-lint; fails on any lint or vet error
# make test       — run go test ./...; fails if any test fails
# make audit      — run govulncheck; fails on known vulnerabilities
# make eval-gate  — check eval/results/latest.json exists with a sentinel key
# make ci         — run all of the above in order; mirrors the GitHub Actions pipeline

.PHONY: lint test audit eval-gate ci

lint:
	golangci-lint run ./...

test:
	go test ./...

audit:
	govulncheck ./...

eval-gate:
	go run eval/gate.go

ci: lint test audit eval-gate
