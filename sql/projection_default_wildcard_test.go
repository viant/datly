package sql

import (
	"github.com/viant/datly/data"
	"testing"
)

func TestDefaultOpaqueWildcardCanPrepareBeforeDatabaseColumns(t *testing.T) {
	source := `SELECT current.* FROM (SELECT e.*,d.status AS related_status FROM events e LEFT JOIN details d ON d.event_id=e.id) current`
	projection := SelectorProjection{SQL: source, View: &data.View{}}
	prepared, err := projection.Prepare(nil)
	if err != nil {
		t.Fatal(err)
	}
	if prepared.Source != source {
		t.Fatalf("default discovery SQL changed: %s", prepared.Source)
	}
	if _, err = projection.Prepare([]string{"id"}); err == nil {
		t.Fatal("explicit selector admitted without schema authority")
	}
}
func TestDefaultProjectionStillRejectsDuplicateExplicitColumns(t *testing.T) {
	if _, err := (SelectorProjection{SQL: `SELECT id AS same,name AS same FROM events`, View: &data.View{}}).Prepare(nil); err == nil {
		t.Fatal("duplicate explicit columns admitted")
	}
}
