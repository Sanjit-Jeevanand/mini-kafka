// Eval gate: fails if eval/results/latest.json is missing or lacks the sentinel key.
// Real metrics replace the sentinel in Phase 3; the gate habit is enforced from day one.
package main

import (
	"encoding/json"
	"fmt"
	"os"
)

const (
	resultsPath = "eval/results/latest.json"
	sentinelKey = "sentinel"
)

func main() {
	data, err := os.ReadFile(resultsPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "FAIL: %s missing — run the eval harness first\n", resultsPath)
		os.Exit(1)
	}

	var result map[string]any
	if err := json.Unmarshal(data, &result); err != nil {
		fmt.Fprintf(os.Stderr, "FAIL: could not parse %s: %v\n", resultsPath, err)
		os.Exit(1)
	}

	val, ok := result[sentinelKey]
	if !ok {
		fmt.Fprintf(os.Stderr, "FAIL: %q key missing from %s\n", sentinelKey, resultsPath)
		os.Exit(1)
	}

	fmt.Printf("OK: eval gate passed (sentinel=%v)\n", val)
}
