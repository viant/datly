package sql

import (
	"strings"
	"testing"
)

func TestNormalizationPreservesExecutableProjection(t *testing.T) {
	for _, source := range []string{
		"SELECT id, name FROM users WHERE id = ?",
		"SELECT tenant, SUM(amount) AS total FROM spend GROUP BY tenant",
		"SELECT 'required(id)' AS description, id FROM users",
		"SELECT id /* tag(id) */ FROM users",
		"SELECT CASE WHEN amount > 0 THEN amount ELSE 0 END AS total FROM spend",
	} {
		if actual := NormalizeAuthoredSQL(source); actual != source {
			t.Errorf("executable SQL changed: %q -> %q", source, actual)
		}
	}
	for _, source := range []string{"SELECT id, SET_LIMIT(5) FROM users", "SELECT id, set_limit (5) FROM users", "SELECT id, set_limit/* policy */(5) FROM users"} {
		if actual := NormalizeAuthoredSQL(source); actual != "SELECT id FROM users" {
			t.Errorf("normalization directive retained: %q -> %q", source, actual)
		}
	}
}

func TestNormalizationGateMatchesParser(t *testing.T) {
	names := []string{"tag", "required", "cast", "sum", "concat"}
	for name := range removableSelectCalls {
		names = append(names, name)
	}
	for _, name := range names {
		for _, spelling := range []string{name, strings.ToUpper(name)} {
			for _, separator := range []string{"", " ", "/* comment */", "\n"} {
				source := "SELECT id, " + spelling + separator + "(amount) AS value FROM spend"
				want := normalizeParsedSelectProjection(source)
				if actual := NormalizeAuthoredSQL(source); actual != want {
					t.Errorf("gate changed parser behavior: %q -> %q, want %q", source, actual, want)
				}
			}
		}
	}
}

func BenchmarkProjectionNormalization(b *testing.B) {
	for _, test := range []struct{ name, sql string }{
		{"plain", "SELECT id, name FROM users WHERE id = ?"},
		{"aggregate", "SELECT tenant, SUM(amount) AS total FROM spend GROUP BY tenant"},
		{"directive", "SELECT id, set_limit(5) FROM users"},
	} {
		b.Run(test.name+"/parsed", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				strings.TrimSpace(normalizeParsedSelectProjection(unwrapProjectionSQL(strings.TrimSpace(test.sql))))
			}
		})
		b.Run(test.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				NormalizeAuthoredSQL(test.sql)
			}
		})
	}
}
