package sql

import (
	"reflect"
	"testing"

	"github.com/viant/sqlparser"
)

func TestCacheProjectionWrapperControlsPreserveFields(t *testing.T) {
	const inner = "SELECT region, SUM(amount) AS total FROM spend WHERE tenant=? GROUP BY region"
	want, err := (CacheProjection{SQL: inner}).Fields()
	if err != nil {
		t.Fatal(err)
	}
	for _, suffix := range []string{"", " LIMIT 5", " LIMIT 5 OFFSET 2"} {
		actual, err := (CacheProjection{SQL: "SELECT c.* FROM (" + inner + ") c" + suffix}).Fields()
		if err != nil || !reflect.DeepEqual(actual, want) {
			t.Fatalf("wrapper %q fields=%+v err=%v", suffix, actual, err)
		}
	}
	if _, err := (CacheProjection{SQL: "not valid SQL"}).Fields(); err == nil {
		t.Fatal("invalid cache SQL accepted")
	}
}

func BenchmarkCacheProjectionFields(b *testing.B) {
	const inner = "SELECT region, SUM(amount) AS total FROM spend WHERE tenant=? GROUP BY region"
	for _, test := range []struct{ name, sql string }{{"direct", inner}, {"wrapped", "SELECT c.* FROM (" + inner + ") c"}} {
		b.Run(test.name+"/parsed", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				text, _ := unwrapGroupedProjectionWrapper(test.sql, nil)
				statement, err := sqlparser.ParseQuery(text)
				if err != nil {
					b.Fatal(err)
				}
				if _, err := (CacheProjection{SQL: test.sql}).fields(statement); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(test.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := (CacheProjection{SQL: test.sql}).Fields(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
