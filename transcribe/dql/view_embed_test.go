package dql

import "testing"

func TestEmbeddedSQLRefsDeduplicatesRepeatedToken(t *testing.T) {
	actual := EmbeddedSQLRefs(`SELECT * FROM (${embed:sql/shared.sql}) a JOIN (${embed:sql/shared.sql}) b ON b.id = a.id`)
	if len(actual) != 1 || actual[0].Path != "sql/shared.sql" || actual[0].Raw != "${embed:sql/shared.sql}" {
		t.Fatalf("EmbeddedSQLRefs() = %+v", actual)
	}
}
