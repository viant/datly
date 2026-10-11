package sql

import "testing"

func TestNormalizationGateIgnoresOnlyInnerCandidates(t *testing.T) {
	for _, source := range []string{
		"SELECT records.* FROM (SELECT CAST(id AS SIGNED) AS id FROM `records`) records",
		"SELECT records.* FROM (SELECT id /* tag(id) */ FROM records) records",
		"SELECT records.* FROM (SELECT id FROM records WHERE description = 'set_limit(5)') records",
	} {
		if mayNormalizeProjection(source) {
			t.Fatalf("normalization unnecessarily reparses an unchanged outer projection: %s", source)
		}
		if got := NormalizeAuthoredSQL(source); got != normalizeParsedSelectProjection(source) {
			t.Fatalf("normalization changed parser behavior: %q", got)
		}
	}
	source := "SELECT records.*,set_limit(5) FROM (SELECT CAST(id AS SIGNED) AS id FROM `records`) records"
	if !mayNormalizeProjection(source) || NormalizeAuthoredSQL(source) != normalizeParsedSelectProjection(source) {
		t.Fatal("outer normalization directive was lost")
	}
}
