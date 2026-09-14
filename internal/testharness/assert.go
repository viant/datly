package testharness

import (
	"encoding/json"
	"testing"

	"github.com/viant/assertly"
)

func AssertJSONEqual(t *testing.T, expectedJSON string, actual any) {
	t.Helper()
	if expectedJSON == "" {
		t.Fatalf("expected JSON must not be empty")
	}
	var expected any
	if err := json.Unmarshal([]byte(expectedJSON), &expected); err != nil {
		t.Fatalf("failed to unmarshal expected json: %v", err)
	}
	actualJSON, err := json.Marshal(actual)
	if err != nil {
		t.Fatalf("failed to marshal actual json: %v", err)
	}
	var actualDecoded any
	if err := json.Unmarshal(actualJSON, &actualDecoded); err != nil {
		t.Fatalf("failed to decode actual json: %v", err)
	}
	assertly.AssertValues(t, expected, actualDecoded)
}
