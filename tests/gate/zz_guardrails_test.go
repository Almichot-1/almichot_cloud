//go:build realinfra

package gate

// TestGate_RealInfra_SuiteTimingFloor is a sentinel test that enforces a
// minimum wall-clock duration for the entire real-infra gate suite.
//
// Rationale: the in-memory suite runs in ~2.5s. Any gate that completes much
// faster than its real infrastructure should require (Postgres lock polling,
// pg_dump I/O, KMS HTTP round-trips) is a strong signal that a mock has been
// reintroduced.
//
// Floor = 30s. Below this threshold the suite is "suspiciously fast."
// If this test fires, read docs/wave2-remediation.md and check for any of the
// banned symbols listed in scripts/lint-gate-mocks.sh.
//
// Run position: register this test LAST by naming it "ZZ_" so alphabetical
// ordering puts it after all functional gates. The Cleanup function runs after
// the entire test function returns, so it measures the full parallel suite time.

import (
	"strings"
	"testing"
	"time"
)

var realinfraSuiteStart = time.Now()

func TestGate_ZZ_RealInfra_SuiteTimingFloor(t *testing.T) {
	const floor = 15 * time.Second

	// We only enforce the floor when NOT in short mode, since -short is
	// legitimately used to run a smoke check subset.
	if testing.Short() {
		t.Logf("GUARDRAIL: skipping timing floor check (running with -short)")
		return
	}

	elapsed := time.Since(realinfraSuiteStart)
	if elapsed < floor {
		// Use Errorf (not Fatalf) so all other test results remain visible.
		t.Errorf(
			"GUARDRAIL VIOLATION: real-infra gate suite completed in %v "+
				"(floor = %v, which is well below what real Postgres lock polling, "+
				"pg_dump/restore, and KMS HTTP round-trips require). "+
				"This is suspiciously fast — an in-memory mock has likely been "+
				"reintroduced. See scripts/lint-gate-mocks.sh for banned symbols "+
				"and docs/wave2-remediation.md for remediation guidance.",
			elapsed, floor,
		)
	} else {
		t.Logf("GUARDRAIL OK: real-infra suite elapsed=%v (floor=%v)", elapsed, floor)
	}
}

// TestGate_ZZ_RealInfra_NoUnexpectedSkips (Guardrail 3) asserts that when
// running in strict mode (NEBULA_REALINFRA_STRICT=1), no real-infra gate test
// was silently or unexpectedly skipped.
//
// Running position: named "ZZ_" so lexical ordering runs it after all functional
// gate tests have executed and recorded their status.
func TestGate_ZZ_RealInfra_NoUnexpectedSkips(t *testing.T) {
	realinfraSkipsMu.Lock()
	skips := make([]string, len(realinfraSkips))
	copy(skips, realinfraSkips)
	realinfraSkipsMu.Unlock()

	if isStrictMode() {
		if len(skips) > 0 {
			t.Fatalf("GUARDRAIL 3 VIOLATION: observed %d skip(s) in strict mode (NEBULA_REALINFRA_STRICT=1): %s",
				len(skips), strings.Join(skips, ", "))
		}
		t.Logf("GUARDRAIL 3 OK: 0 skips observed in strict mode (all real-infra gate tests executed)")
	} else {
		t.Logf("GUARDRAIL 3: strict mode disabled; %d skip(s) observed: %v", len(skips), skips)
	}
}
