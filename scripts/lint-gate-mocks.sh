#!/usr/bin/env bash
# scripts/lint-gate-mocks.sh
#
# Guardrail 2: Banned-symbol lint for real-infra gate test functions.
#
# PURPOSE: Ensures that the five named mock types can never silently reappear
# inside the specific gate functions they are banned from. These mocks are
# correct and expected elsewhere (Phase 11-14 unit tests, component tests);
# this lint bans them ONLY inside the G-26, G-34, G-36, G-37, G-43, G-44,
# and G-46 real-infra gate functions.
#
# HOW IT WORKS:
#   1. Extracts the body of each banned gate function from the test file.
#   2. Greps for any of the banned symbol names within those extracted bodies.
#   3. Exits non-zero (failing the build) if any banned symbol is found.
#
# USAGE:
#   bash scripts/lint-gate-mocks.sh
#   # Returns 0 on success, 1 on any violation.
#
# CI integration (Makefile target: gate-lint):
#   Runs automatically as part of the pre-merge gate check.
#
# Banned symbols and the gates they are banned from:
#
#   Symbol                 | Banned in gate functions
#   -----------------------|----------------------------------------------------
#   MemoryAdvisoryLock     | TestGate_G43_*, TestGate_G44_*
#   MockKMSClient          | TestGate_G36_*, TestGate_G37_*
#   MemoryBackupManager    | TestGate_G46_*
#   MemoryRegistry         | TestGate_G26_*
#   MockBuildWorkerFactory | TestGate_G34_*   (if one ever appears)
#
# RATIONALE: These symbols have characteristic timing signatures:
#   - MemoryAdvisoryLock: promotion in ~15ms (vs ≥1s with real Postgres)
#   - MockKMSClient: zero HTTP requests (vs ≥2 with LocalStack)
#   - MemoryBackupManager: instant backup/restore (vs seconds with pg_dump)
#   - MemoryRegistry: no TCP socket (vs real HTTP over loopback)
# The wall-clock floor guardrail (TestGate_ZZ_*) is the runtime catch;
# this lint is the static-analysis catch that fires BEFORE the test runs.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
GATE_INFRA_FILE="$REPO_ROOT/tests/gate/mvp_gate_realinfra_test.go"

if [[ ! -f "$GATE_INFRA_FILE" ]]; then
  echo "ERROR: real-infra gate file not found: $GATE_INFRA_FILE"
  exit 1
fi

violations=0

# ── Helper: extract a function body between its opening brace and the matching
# closing brace. Outputs the raw function body text.
extract_func() {
  local func_name="$1"
  local file="$2"
  awk -v name="$func_name" '
    /^func / && $0 ~ name { inside=1; depth=0 }
    inside {
      for (i=1; i<=length($0); i++) {
        c=substr($0,i,1)
        if (c=="{") depth++
        if (c=="}") { depth--; if (depth==0) { print; inside=0; next } }
      }
      print
    }
  ' "$file"
}

check_gate() {
  local gate_pattern="$1"      # e.g. "TestGate_G43_"
  local banned_symbols=("${@:2}")  # remaining args are banned symbol names

  # Find matching function names in the file.
  local func_names
  func_names=$(grep -oP "^func ${gate_pattern}[A-Za-z0-9_]+" "$GATE_INFRA_FILE" || true)

  if [[ -z "$func_names" ]]; then
    return  # gate not yet in file; no violation possible
  fi

  while IFS= read -r func_name; do
    local body
    body=$(extract_func "$func_name" "$GATE_INFRA_FILE")

    for symbol in "${banned_symbols[@]}"; do
      if echo "$body" | grep -q "$symbol"; then
        echo "LINT VIOLATION: $func_name uses banned symbol '$symbol'"
        echo "  File: $GATE_INFRA_FILE"
        echo "  Banned symbols in this gate: ${banned_symbols[*]}"
        echo "  See docs/wave2-remediation.md for explanation."
        violations=$((violations + 1))
      fi
    done
  done <<< "$func_names"
}

echo "=== Nebula Gate Mock Lint (Guardrail 2) ==="
echo "File: $GATE_INFRA_FILE"
echo ""

check_gate "TestGate_G26_" "MemoryRegistry" "NewMemoryRegistry"
check_gate "TestGate_G34_" "MockBuildWorkerFactory" "atomic.Bool"  # atomic.Bool as *CP-side kill* means the shortcut returned
check_gate "TestGate_G36_" "MockKMSClient" "NewMockKMSClient"
check_gate "TestGate_G37_" "MockKMSClient" "NewMockKMSClient"
check_gate "TestGate_G43_" "MemoryAdvisoryLock" "NewMemoryAdvisoryLock"
check_gate "TestGate_G44_" "MemoryAdvisoryLock" "NewMemoryAdvisoryLock"
check_gate "TestGate_G46_" "MemoryBackupManager" "NewMemoryBackupManager"

echo ""
if [[ $violations -eq 0 ]]; then
  echo "✅ Gate mock lint PASSED — no banned symbols found in real-infra gate functions."
  exit 0
else
  echo "❌ Gate mock lint FAILED — $violations violation(s) found."
  echo ""
  echo "IMPORTANT: The banned symbols above are correct in unit/component tests."
  echo "They are ONLY banned inside the specific real-infra gate functions listed above."
  echo "Fix: replace the banned symbol with the real-infra equivalent as described in"
  echo "docs/wave2-remediation.md and the gate remediation spec."
  exit 1
fi
