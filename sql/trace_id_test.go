package sql

import "testing"

func TestStableTraceID(t *testing.T) {
	traceID := StableTraceID("GET", "/users", "SELECT 1")
	if traceID == "" {
		t.Fatalf("expected stable trace id")
	}
	if traceID != StableTraceID("GET", "/users", "SELECT 1") {
		t.Fatalf("expected stable trace id to be deterministic")
	}
	if traceID == StableTraceID("POST", "/users", "SELECT 1") {
		t.Fatalf("expected different inputs to produce different trace ids")
	}
}
