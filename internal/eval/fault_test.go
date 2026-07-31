package eval

import "testing"

func TestFaultInjectionSweep(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping fault injection sweep: use -short to skip")
	}

	const numSeeds = 500
	result, err := MeasureFaultInjection(numSeeds)
	if err != nil {
		t.Fatalf("fault injection sweep failed: %v", err)
	}

	t.Logf("scenarios run: %d  unique seeds: %d  violations found: %d",
		result.ScenariosRun, result.UniqueSeeds, result.ViolationsFound)

	if result.ViolationsFound > 0 {
		for seed := 0; seed < numSeeds; seed++ {
			vs, _ := runFaultScenario(int64(seed))
			for _, v := range vs {
				t.Logf("  %s", v)
			}
		}
	}
}
