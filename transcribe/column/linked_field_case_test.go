package column

import (
	"context"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
)

func TestLinkedFieldsRetainDistinctCaseAndSQLAliases(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	SQL := `SELECT 'local' AS DAY_PART_TYPE, 'global' AS DAYPART_TYPE`
	columns := []*spec.Column{
		{Name: "DayPartType", Source: "DAY_PART_TYPE", NameInferred: true, Type: spec.TypeRef{Name: "string"}, Tag: `sqlx:"DAY_PART_TYPE"`},
		{Name: "DaypartType", Source: "DAYPART_TYPE", NameInferred: true, Type: spec.TypeRef{Name: "string"}, Tag: `sqlx:"DAYPART_TYPE"`},
	}
	v := &spec.View{Name: "Constraints", Source: &spec.ViewSource{SQL: SQL}, Columns: columns}
	c := &spec.Component{Settings: &spec.Settings{DefaultConnector: "main"}, RootView: v}
	if err := New(Connections{"main": h.DB}).Refine(context.Background(), c, nil, nil); err != nil {
		t.Fatal(err)
	}
	if v.Source.SQL != SQL || len(v.Columns) != 2 {
		t.Fatalf("SQL or fields changed: %+v", v)
	}
	for i, want := range columns {
		got := v.Columns[i]
		if got.Name != want.Name || got.Source != want.Source || got.Tag != want.Tag || !got.NameInferred {
			t.Fatalf("field mapping changed: %+v", got)
		}
	}
}

func TestLinkedFieldCaseDoesNotPermitAmbiguousMappings(t *testing.T) {
	for _, tc := range []struct {
		name     string
		inferred bool
		source   string
	}{
		{"authored aliases", false, "DAYPART_TYPE"},
		{"same linked SQL source", true, "DAY_PART_TYPE"},
		{"unmapped linked field", true, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cols := []*spec.Column{
				{Name: "DayPartType", Source: "DAY_PART_TYPE", NameInferred: tc.inferred},
				{Name: "DaypartType", Source: tc.source, NameInferred: tc.inferred},
			}
			if err := validateColumns("canonical", cols); err == nil || !strings.Contains(err.Error(), "ambiguous") {
				t.Fatalf("ambiguous mappings accepted: %v", err)
			}
		})
	}
}
