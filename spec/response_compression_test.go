package spec

import "testing"

func TestResponseCompressionAuthority(t *testing.T) {
	source := &Settings{ResponseCompression: &ResponseCompression{Encoding: "gzip", MinSizeBytes: 2048}}
	copy := source.Clone()
	source.ResponseCompression.MinSizeBytes = 7
	if copy.IsZero() || copy.ResponseCompression.MinSizeBytes != 2048 {
		t.Fatal("compression authority was discarded or aliased")
	}
	for _, p := range []*ResponseCompression{{Encoding: "br", MinSizeBytes: 2}, {Encoding: "gzip", MinSizeBytes: -1}, {}} {
		if p.Validate() == nil {
			t.Fatalf("invalid compression accepted: %+v", p)
		}
	}
	if (&ResponseCompression{Encoding: "gzip"}).Validate() != nil {
		t.Fatal("zero byte threshold should be valid")
	}
}
