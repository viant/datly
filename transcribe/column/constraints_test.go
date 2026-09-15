package column

import (
	"testing"

	"github.com/viant/datly/spec"
	"github.com/viant/sqlx/metadata/sink"
)

func TestConstraintsFromColumnsPreservesSQLXFacts(t *testing.T) {
	autoIncrement := true
	identifierDefault := "nextval('events_id_seq')"
	emailDefault := "'unknown'"
	constraints := constraintsFromColumns([]sink.Column{
		{Name: "ID", Key: "PRI", Default: &identifierDefault, IsAutoincrement: &autoIncrement},
		{Name: "EMAIL", Key: "UNI", Default: &emailDefault},
		{Name: "GROUP_ID", Key: "MUL"},
	})
	id := constraints["id"]
	if !id.primaryKey || !id.autoIncrement || id.unique || id.defaultValue == nil || *id.defaultValue != identifierDefault {
		t.Fatalf("ID constraint = %+v", id)
	}
	email := constraints["email"]
	if email.primaryKey || email.autoIncrement || !email.unique || email.defaultValue == nil || *email.defaultValue != emailDefault {
		t.Fatalf("EMAIL constraint = %+v", email)
	}
	if group := constraints["group_id"]; group.primaryKey || group.autoIncrement || group.unique || group.defaultValue != nil {
		t.Fatalf("GROUP_ID constraint = %+v", group)
	}
}

func TestTableNotNullRequiresMetadataEvidence(t *testing.T) {
	for _, tc := range []struct {
		value string
		want  bool
	}{{"NO", true}, {"YES", false}, {"false", true}, {"true", false}, {"", false}, {"UNKNOWN", false}, {"?", false}, {" 0 ", true}} {
		facts := constraintsFromColumns([]sink.Column{{Name: "enabled", Nullable: tc.value}})
		if facts["enabled"].notNull != tc.want {
			t.Fatalf("nullable=%q constraint=%+v", tc.value, facts["enabled"])
		}
	}
	base := &spec.Column{Name: "enabled", NotNull: true, Nullable: true}
	merged := mergeColumns([]*spec.Column{base}, []*spec.Column{{Name: "enabled", Nullable: true}})
	if merged[0].NotNull || !merged[0].Nullable || !base.NotNull {
		t.Fatal("fresh constraints or original metadata not preserved")
	}
}

func TestApplyTableConstraintsUsesDirectProjectionLineage(t *testing.T) {
	lineage, err := directProjectionLineage(&spec.ViewSource{
		Table: "events",
		SQL: `SELECT e.id AS event_id, e.name, e.score + 1 AS score, a.id AS account_id
			FROM events e JOIN accounts a ON a.id = e.account_id`,
	})
	if err != nil {
		t.Fatalf("directProjectionLineage() error = %v", err)
	}
	idDefault := "nextval('events_id_seq')"
	nameDefault := "'unknown'"
	columns := []*spec.Column{{Name: "event_id"}, {Name: "name"}, {Name: "score"}, {Name: "account_id"}}
	applyTableConstraints(columns, map[string]tableConstraint{
		"id":    {primaryKey: true, autoIncrement: true, defaultValue: &idDefault},
		"name":  {unique: true, defaultValue: &nameDefault},
		"score": {unique: true},
	}, lineage)
	if !columns[0].PrimaryKey || !columns[0].AutoIncrement || columns[0].Default == nil || *columns[0].Default != idDefault {
		t.Fatalf("event_id = %+v", columns[0])
	}
	if !columns[1].Unique || columns[1].Default == nil || *columns[1].Default != nameDefault {
		t.Fatalf("name = %+v", columns[1])
	}
	if columns[2].Unique || columns[3].PrimaryKey || columns[3].AutoIncrement || columns[3].Unique {
		t.Fatalf("non-direct columns inherited constraints: score=%+v account=%+v", columns[2], columns[3])
	}
}

func TestApplyTableConstraintsWildcardDoesNotEnrichComputedAlias(t *testing.T) {
	lineage, err := directProjectionLineage(&spec.ViewSource{
		Table: "events",
		SQL:   "SELECT e.*, e.score + 1 AS score FROM events e",
	})
	if err != nil {
		t.Fatalf("directProjectionLineage() error = %v", err)
	}
	columns := []*spec.Column{{Name: "id"}, {Name: "name"}, {Name: "score"}}
	applyTableConstraints(columns, map[string]tableConstraint{
		"id": {primaryKey: true}, "name": {unique: true}, "score": {unique: true},
	}, lineage)
	if !columns[0].PrimaryKey || !columns[1].Unique || columns[2].Unique {
		t.Fatalf("lineage = %+v, columns = id:%+v name:%+v score:%+v", lineage, columns[0], columns[1], columns[2])
	}
}

func TestDirectProjectionLineageRequiresExplicitMatchingTable(t *testing.T) {
	for _, source := range []*spec.ViewSource{
		{SQL: "SELECT id FROM events"},
		{Table: "events", SQL: "SELECT id FROM accounts"},
		{Table: "events", SQL: "SELECT id FROM events UNION SELECT id FROM archived_events"},
	} {
		lineage, err := directProjectionLineage(source)
		if err != nil {
			t.Fatalf("directProjectionLineage(%+v) error = %v", source, err)
		}
		if lineage.wildcard || len(lineage.direct) != 0 {
			t.Fatalf("unexpected lineage for %+v: %+v", source, lineage)
		}
	}
}

func TestDirectProjectionLineageRejectsAmbiguousJoinedSources(t *testing.T) {
	tests := []string{
		"SELECT a.id AS account_id FROM events e JOIN accounts a ON a.id = e.account_id",
		"SELECT events.id AS account_id FROM events e JOIN accounts events ON events.id = e.account_id",
		"SELECT * FROM events e JOIN accounts a ON a.id = e.account_id",
		"SELECT id FROM events e JOIN accounts a ON a.id = e.account_id",
	}
	for _, SQL := range tests {
		lineage, err := directProjectionLineage(&spec.ViewSource{Table: "events", SQL: SQL})
		if err != nil {
			t.Fatalf("directProjectionLineage(%q) error = %v", SQL, err)
		}
		if lineage.wildcard || len(lineage.direct) != 0 {
			t.Fatalf("ambiguous SQL %q produced lineage %+v", SQL, lineage)
		}
	}
}

func TestApplyTableConstraintsPreservesAuthoredDefault(t *testing.T) {
	authored := "'authored'"
	discovered := "'discovered'"
	columns := []*spec.Column{{Name: "status", Default: &authored}}
	applyTableConstraints(columns, map[string]tableConstraint{"status": {defaultValue: &discovered}}, &projectionLineage{
		direct: map[string]string{"status": "status"}, blocked: map[string]bool{},
	})
	if columns[0].Default == nil || *columns[0].Default != authored {
		t.Fatalf("default = %+v", columns[0].Default)
	}
}

func TestNamedViewProjectionLineage(t *testing.T) {
	for _, tc := range []struct {
		name, SQL string
		key       string
		blocked   string
	}{
		{"auxiliary table", `SELECT named.* FROM (SELECT e.* FROM (events) e WHERE e.id>0) named`, "id", ""},
		{"derived aliases", `SELECT named.* FROM (SELECT e.id AS event_id,e.score+1 AS score FROM (events) e WHERE e.id>0) named`, "event_id", "score"},
		{"nested aliases", `SELECT named.* FROM (SELECT inner_view.event_id AS event_key FROM (SELECT e.id AS event_id FROM (events) e) inner_view) named`, "event_key", ""},
		{"CTE aliases", `WITH selected_events AS (SELECT e.id AS event_id FROM (events) e) SELECT selected_events.* FROM selected_events`, "event_id", ""},
		{"joined qualified star", `SELECT e.* FROM events e JOIN other o ON o.id=e.id`, "id", ""},
		{"joined other star", `SELECT o.* FROM events e JOIN other o ON o.id=e.id`, "", "id"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lineage, err := directProjectionLineage(&spec.ViewSource{Table: "events", SQL: tc.SQL})
			if err != nil {
				t.Fatal(err)
			}
			columns := []*spec.Column{{Name: tc.key}, {Name: tc.blocked}}
			applyTableConstraints(columns, map[string]tableConstraint{"id": {primaryKey: true}, "score": {unique: true}}, lineage)
			if tc.key != "" && !columns[0].PrimaryKey {
				t.Fatalf("key authority lost: %+v", lineage)
			}
			if tc.blocked != "" && (columns[1].PrimaryKey || columns[1].Unique) {
				t.Fatalf("invented table authority: %+v", lineage)
			}
		})
	}
}
