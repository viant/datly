package invocation

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestNonObjectErrorPayloadOmitsStructuredContentOnWire(t *testing.T) {
	failed := true
	for _, payload := range []string{"null", "[]", "\"error text\""} {
		result := textToolResult([]byte(payload), &failed)
		wire, err := json.Marshal(result)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(wire), "structuredContent") {
			t.Fatalf("non-object structuredContent: %s", wire)
		}
		if !strings.Contains(string(wire), `"isError":true`) {
			t.Fatalf("error flag lost: %s", wire)
		}
	}
	result := textToolResult([]byte(`{"message":"invalid slug"}`), &failed)
	if result.StructuredContent == nil {
		t.Fatal("object error payload omitted")
	}
}
