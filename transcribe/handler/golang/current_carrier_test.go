package golang

import (
	"strings"
	"testing"
)

func TestWriteLoweringRejectsUnconsumedCurrentCarrier(t *testing.T) {
	_, err := Lower(nil, Config{Records: []RecordType{{Current: "[]*Previous", CurrentValue: "*Envelope"}}})
	if err == nil || !strings.Contains(err.Error(), "metadata-aware mutation program") {
		t.Fatalf("error=%v", err)
	}
}
