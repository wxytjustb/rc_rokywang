package logging

import (
	"bytes"
	"strings"
	"testing"
)

func TestNewJSONEnablesDebugFromEnvironment(t *testing.T) {
	t.Setenv("LOG_LEVEL", "debug")
	var output bytes.Buffer
	logger := NewJSON(&output)
	logger.Debug("memory event published", "event_id", "test-id")
	if !strings.Contains(output.String(), `"level":"DEBUG"`) || !strings.Contains(output.String(), `"event_id":"test-id"`) {
		t.Fatalf("debug log was not emitted: %s", output.String())
	}
}
