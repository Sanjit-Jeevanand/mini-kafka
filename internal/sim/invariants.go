package sim

import (
	"fmt"
	"strings"
)

type Violations []error

func (vs Violations) AsError() error {
	if len(vs) == 0 {
		return nil
	}
	msgs := make([]string, len(vs))
	for i, e := range vs {
		msgs[i] = e.Error()
	}
	return fmt.Errorf("invariant violations:\n  %s", strings.Join(msgs, "\n  "))
}

func CheckAllOwned(name string, n int, owned map[int]bool) error {
	for i := 0; i < n; i++ {
		if !owned[i] {
			return fmt.Errorf("%s: partition %d has no owner", name, i)
		}
	}
	return nil
}

func CheckMonotonic(name string, prev, curr uint64) error {
	if curr < prev {
		return fmt.Errorf("%s: decreased from %d to %d", name, prev, curr)
	}
	return nil
}

func CheckNoRegression(name string, prev, curr map[string]uint64) error {
	for k, currVal := range curr {
		if prevVal, ok := prev[k]; ok && currVal < prevVal {
			return fmt.Errorf("%s[%q]: regressed from %d to %d", name, k, prevVal, currVal)
		}
	}
	return nil
}
