package column

import (
	"github.com/viant/datly/spec"
	"testing"
)

func TestPlainConstraintRetainsProvenResultAlias(t *testing.T) {
	source := &spec.ViewSource{Table: "RECORD_STATS", SQL: `SELECT stats.* FROM (SELECT s.RECORD_ID AS recordId, s.GROUP_ID AS groupId, SUM(s.EVENT_COUNT) AS total FROM RECORD_STATS s ${predicate.Builder().CombineOr($predicate.FilterGroup(0, "AND")).Build("WHERE")} GROUP BY s.RECORD_ID,s.GROUP_ID) stats`}
	lineage, err := directProjectionLineage(source)
	if err != nil {
		t.Fatal(err)
	}
	constraints := map[string]tableConstraint{"record_id": {name: "RECORD_ID", primaryKey: true, notNull: true}, "group_id": {name: "GROUP_ID"}, "event_count": {name: "EVENT_COUNT"}}
	columns := []*spec.Column{{Name: "recordId", Source: "recordId"}, {Name: "groupId", Source: "groupId"}, {Name: "total", Source: "total", ExplicitType: true, Nullable: true, Type: spec.TypeRef{Name: "int", Pointer: true}}}
	applyTableConstraints(columns, constraints, lineage)
	for i, expected := range []string{`sqlx:"RECORD_ID|recordId"`, `sqlx:"GROUP_ID|groupId"`, ""} {
		if columns[i].Tag != expected {
			t.Fatalf("column %d tag=%q want=%q", i, columns[i].Tag, expected)
		}
	}
	if !columns[0].PrimaryKey || !columns[0].NotNull || columns[2].Source != "total" || !columns[2].Nullable || !columns[2].Type.Pointer {
		t.Fatalf("lost metadata: %+v", columns)
	}
	applyTableConstraints(columns, constraints, lineage)
	for i, name := range []string{"RecordId", "RenamedGroup", "Total"} {
		columns[i].Name = name
		columns[i].NameInferred = true
	}
	identities, err := resolveResultSources(columns, source)
	if err != nil {
		t.Fatal(err)
	}
	if err = identities.validateResults(columns, []string{"recordId", "groupId", "total"}); err != nil {
		t.Fatal(err)
	}
}

func TestPlainConstraintPreservesAuthoredMapping(t *testing.T) {
	lineage, err := directProjectionLineage(&spec.ViewSource{Table: "records", SQL: "SELECT r.RECORD_ID AS recordId FROM records r"})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ source, tag, want string }{
		{"recordId", `sqlx:"RECORD_ID|recordId,required=false,enc=JSON"`, `sqlx:"RECORD_ID|recordId,required=false,enc=JSON"`},
		{"recordId", `sqlx:"name=recordId"`, `sqlx:"name=recordId"`},
		{"recordId", `sqlx:"-"`, `sqlx:"-"`},
		{"UNRELATED", "", ""},
	} {
		col := &spec.Column{Name: "recordId", Source: tc.source, Tag: tc.tag}
		applyTableConstraints([]*spec.Column{col}, map[string]tableConstraint{"record_id": {name: "RECORD_ID", primaryKey: true}}, lineage)
		if col.Tag != tc.want {
			t.Fatalf("source=%q tag=%q want=%q", tc.source, col.Tag, tc.want)
		}
		if tc.source == "UNRELATED" && col.Source != tc.source {
			t.Fatal("repaired unrelated source")
		}
	}
	for _, name := range []string{"RECORD_ID", "record_id"} {
		direct, err := directProjectionLineage(&spec.ViewSource{Table: "records", SQL: "SELECT r.RECORD_ID AS " + name + " FROM records r"})
		if err != nil {
			t.Fatal(err)
		}
		col := &spec.Column{Name: name, Source: name}
		applyTableConstraints([]*spec.Column{col}, map[string]tableConstraint{"record_id": {name: "RECORD_ID"}}, direct)
		if col.Tag != "" {
			t.Fatalf("redundant alias tag: %q", col.Tag)
		}
	}
}
