package spec

import "testing"

func TestKeyRoundTrip(t *testing.T) {
	key := Key{
		Kind:  KindComponent,
		Scope: "github.com/acme/app/report",
		Name:  "GET /v1/reports/:id",
	}
	encoded := key.String()
	decoded, err := ParseKey(encoded)
	if err != nil {
		t.Fatalf("ParseKey failed: %v", err)
	}
	if decoded != key {
		t.Fatalf("round trip mismatch: got %#v want %#v", decoded, key)
	}
}

func TestParseKeyRejectsBadShape(t *testing.T) {
	if _, err := ParseKey("component:onlytwo"); err == nil {
		t.Fatalf("expected parse failure")
	}
}
