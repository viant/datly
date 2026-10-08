package report

import (
	"github.com/viant/sqlparser"
	"testing"
)

func BenchmarkLinkedReportIdentifier(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := reportOutputIdentifier("advertiser_id"); err != nil {
			b.Fatal(err)
		}
	}
}
func BenchmarkLinkedReportRelationIdentifier(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if reportIdentifierName("advertiser_id") != "advertiser_id" {
			b.Fatal("identity changed")
		}
	}
}
func TestReportIdentifierSemantics(t *testing.T) {
	for _, tc := range []struct {
		raw, want string
		valid     bool
	}{{"advertiser_id", "advertiser_id", true}, {"s.id", "id", true}, {" `s`.`a.b` ", "a.b", true}, {`"a""b"`, "a\"b", true}, {"id; DROP TABLE t", "", false}, {"", "", false}, {"1bad", "", false}, {"id+1", "", false}} {
		got, err := reportOutputIdentifier(tc.raw)
		if (err == nil) != tc.valid || tc.valid && got != tc.want {
			t.Fatalf("%q => %q %v", tc.raw, got, err)
		}
	}
}

func TestReportIdentifierFastPathMatchesNativeGrammar(t *testing.T) {
	for ch := byte(0); ch < 127; ch++ {
		for _, name := range []string{string(ch), "a" + string(ch) + "b", string(ch) + "id"} {
			if !simpleReportIdentifier(name) {
				continue
			}
			parts, err := sqlparser.TableIdentifierParts(name)
			if err != nil || len(parts) != 1 || parts[0] != name {
				t.Fatalf("fast path accepted %q: %v %v", name, parts, err)
			}
		}
	}
}
