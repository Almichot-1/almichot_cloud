package observability

import (
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"
	"time"
)

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("failed to create pipe: %v", err)
	}
	os.Stdout = w
	defer func() { os.Stdout = old }()

	fn()

	if err := w.Close(); err != nil {
		t.Fatalf("failed to close pipe writer: %v", err)
	}
	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("failed to read pipe: %v", err)
	}
	return string(data)
}

func TestProductionLogIsValidJSON(t *testing.T) {
	out := captureStdout(t, func() {
		logger := New("production")
		logger.Info().Msg("hello nebula")
	})

	out = strings.TrimSpace(out)
	if out == "" {
		t.Fatal("expected log output, got empty string")
	}

	var obj map[string]any
	if err := json.Unmarshal([]byte(out), &obj); err != nil {
		t.Fatalf("log line is not valid JSON: %v\noutput: %s", err, out)
	}

	if lvl, ok := obj["level"].(string); !ok || lvl != "info" {
		t.Errorf("expected level=info, got %v", obj["level"])
	}
	if ts, ok := obj["time"].(string); !ok {
		t.Errorf("expected time field, got %v", obj["time"])
	} else if _, err := time.Parse(time.RFC3339Nano, ts); err != nil {
		t.Errorf("time field not parseable as RFC3339Nano: %v", err)
	}
	if msg, ok := obj["message"].(string); !ok || msg != "hello nebula" {
		t.Errorf("expected message=hello nebula, got %v", obj["message"])
	}
}

func TestStructuredFieldsIncluded(t *testing.T) {
	out := captureStdout(t, func() {
		logger := New("production")
		logger = logger.With().Str("component", "test-suite").Logger()
		logger.Info().Str("instance_id", "inst-1").Msg("event with fields")
	})

	var obj map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &obj); err != nil {
		t.Fatalf("log line is not valid JSON: %v", err)
	}

	if comp, ok := obj["component"].(string); !ok || comp != "test-suite" {
		t.Errorf("expected component=test-suite, got %v", obj["component"])
	}
	if iid, ok := obj["instance_id"].(string); !ok || iid != "inst-1" {
		t.Errorf("expected instance_id=inst-1, got %v", obj["instance_id"])
	}
}

func TestEveryLineHasLevelTimeComponent(t *testing.T) {
	lines := captureStdout(t, func() {
		logger := New("production")
		logger = logger.With().Str("component", "cp").Logger()
		logger.Info().Msg("first")
		logger.Warn().Msg("second")
		logger.Error().Msg("third")
	})

	for _, line := range strings.Split(strings.TrimSpace(lines), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var obj map[string]any
		if err := json.Unmarshal([]byte(line), &obj); err != nil {
			t.Fatalf("log line is not valid JSON: %v\nline: %s", err, line)
		}
		if _, ok := obj["level"]; !ok {
			t.Errorf("line missing level: %s", line)
		}
		if _, ok := obj["time"]; !ok {
			t.Errorf("line missing time: %s", line)
		}
		if _, ok := obj["component"].(string); !ok {
			t.Errorf("line missing component: %s", line)
		}
	}
}