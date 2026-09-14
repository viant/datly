package spec

import (
	"strings"
	"testing"
)

func TestHashSnapshotDeterministic(t *testing.T) {
	snapshot := Snapshot{
		Version: "v1",
		Keys: []Key{
			{Kind: KindComponent, Scope: "pkg/a", Name: "A"},
			{Kind: KindView, Scope: "pkg/a", Name: "Orders"},
		},
		Types: []TypeRef{
			{Package: "pkg/a", Name: "Input"},
		},
	}
	hash1, err := HashSnapshot(snapshot)
	if err != nil {
		t.Fatalf("HashSnapshot failed: %v", err)
	}
	hash2, err := HashSnapshot(snapshot)
	if err != nil {
		t.Fatalf("HashSnapshot failed: %v", err)
	}
	if hash1 != hash2 {
		t.Fatalf("hash mismatch: %s != %s", hash1, hash2)
	}
}

func TestMarshalSnapshotUsesDiagnosticsField(t *testing.T) {
	snapshot := Snapshot{
		Version:     "v1",
		Diagnostics: []Diagnostic{{Severity: SeverityWarn, Code: "deprecated", Message: "replace it"}},
	}
	data, err := MarshalSnapshot(snapshot)
	if err != nil {
		t.Fatalf("MarshalSnapshot failed: %v", err)
	}
	actual := string(data)
	if !strings.Contains(actual, `"diagnostics":[{"severity":"warn","code":"deprecated","message":"replace it"}]`) {
		t.Fatalf("unexpected snapshot JSON: %s", actual)
	}
}
