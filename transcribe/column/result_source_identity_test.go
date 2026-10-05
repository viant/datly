package column

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
)

func TestResultSourceAnnotationsRejectUnrelatedGoName(t *testing.T) {
	for _, inferred := range []bool{false, true} {
		for _, annotation := range []*spec.Column{
			{Name: "Status", Source: "LOGICAL_STATUS", ExplicitType: true, Type: spec.TypeRef{Name: "string"}},
			{Name: "Status", Source: "LOGICAL_STATUS", Tag: `invariant:"Logical"`},
			{Name: "Status", Source: "LOGICAL_STATUS", DeleteMarker: true},
			{Name: "Status", Source: "LOGICAL_STATUS", ConcurrencyToken: true},
		} {
			annotation.NameInferred = inferred
			view := &spec.View{Namespace: "flight", Columns: []*spec.Column{annotation}}
			identities, err := resolveResultSources(view.Columns, nil)
			if err != nil {
				t.Fatal(err)
			}
			for _, tc := range []struct {
				outputs []string
				failure string
			}{
				{[]string{"STATUS"}, "absent"},
				{[]string{"STATUS", "LOGICAL_STATUS"}, ""},
				{[]string{"LOGICAL_STATUS", "STATUS"}, ""},
				{[]string{"LOGICAL_STATUS", "logical_status"}, "multiple"},
			} {
				err := validateResultAnnotations(view, tc.outputs, identities)
				if tc.failure == "" && err != nil || tc.failure != "" && (err == nil || !strings.Contains(err.Error(), tc.failure)) {
					t.Fatalf("inferred=%v outputs=%v error=%v", inferred, tc.outputs, err)
				}
			}
		}
	}
}

func TestResultSourcePlanRejectsAmbiguousOwnershipAndDuplicateResults(t *testing.T) {
	columns := []*spec.Column{{Name: "PersistedStatus", Source: "STATUS"}, {Name: "Status", Source: "status"}}
	if _, err := resolveResultSources(columns, nil); err == nil || !strings.Contains(err.Error(), "ambiguous") {
		t.Fatalf("ownership error=%v", err)
	}
	columns = columns[:1]
	identities, err := resolveResultSources(columns, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		outputs []string
		failure string
	}{
		{[]string{"Status", "STATUS"}, "multiple"},
		{[]string{"OTHER"}, "absent"},
		{[]string{"STATUS", "OTHER", "other"}, "multiple"},
	} {
		if err := identities.validateResults(columns, tc.outputs); err == nil || !strings.Contains(err.Error(), tc.failure) {
			t.Fatalf("outputs=%v error=%v", tc.outputs, err)
		}
	}
}

func TestResultSourceMergePreservesPhysicalAndLogicalMetadata(t *testing.T) {
	groupable := false
	defaultValue := "'authored'"
	physical := &spec.Column{Name: "PersistedStatus", Source: "STATUS", Type: spec.TypeRef{Name: "int", Pointer: true}, ExplicitType: true, Optional: true, Nullable: true, Tag: `json:"-" invariant:"Persisted"`, Codec: &spec.Codec{Body: "AsInt", Args: []string{"strict"}}, Groupable: &groupable, PrimaryKey: true, AutoIncrement: true, Unique: true, Default: &defaultValue, DeleteMarker: true, ConcurrencyToken: true}
	logical := &spec.Column{Name: "Status", Source: "LOGICAL_STATUS", Required: true, Tag: `json:"status" invariant:"Logical"`}
	physicalSnapshot := physical.Clone()
	logicalSnapshot := logical.Clone()
	for _, reverseBase := range []bool{false, true} {
		for _, reverseResult := range []bool{false, true} {
			base := []*spec.Column{physical, logical}
			if reverseBase {
				base = []*spec.Column{logical, physical}
			}
			discovered := []*spec.Column{{Name: "STATUS", Source: "STATUS", Type: spec.TypeRef{Name: "int64"}, DatabaseType: "INTEGER", Nullable: true}, {Name: "LOGICAL_STATUS", Source: "LOGICAL_STATUS", Type: spec.TypeRef{Name: "string"}, DatabaseType: "VARCHAR", Nullable: true}}
			if reverseResult {
				discovered[0], discovered[1] = discovered[1], discovered[0]
			}
			identities, err := resolveResultSources(base, nil)
			if err != nil {
				t.Fatal(err)
			}
			merged := mergeColumnsWithSources(base, discovered, identities)
			if len(merged) != 2 {
				t.Fatalf("result=%+v", merged)
			}
			byName := map[string]*spec.Column{}
			for _, column := range merged {
				byName[column.Name] = column
			}
			persisted := byName["PersistedStatus"]
			status := byName["Status"]
			if persisted.DatabaseType != "INTEGER" || persisted.Type != physical.Type || persisted.EffectiveType() != physical.EffectiveType() || persisted.Source != "STATUS" || persisted.Tag != physical.Tag || !persisted.PrimaryKey || !persisted.AutoIncrement || !persisted.Unique || persisted.Default == nil || *persisted.Default != defaultValue || !persisted.DeleteMarker || !persisted.ConcurrencyToken || persisted.Groupable == nil || *persisted.Groupable || !reflect.DeepEqual(persisted.Codec, physical.Codec) {
				t.Fatalf("physical metadata=%+v", persisted)
			}
			if status.Type.Name != "string" || status.DatabaseType != "VARCHAR" || status.Source != "LOGICAL_STATUS" || status.Nullable || !status.Required || status.Tag != logical.Tag || status.PrimaryKey || status.Codec != nil {
				t.Fatalf("logical metadata=%+v", status)
			}
			persisted.Codec.Args[0] = "mutated"
			*persisted.Default = "mutated"
			*persisted.Groupable = true
			if !reflect.DeepEqual(physical, physicalSnapshot) || !reflect.DeepEqual(logical, logicalSnapshot) {
				t.Fatal("merge mutated authored metadata")
			}
		}
	}
}

func TestResultSourceMergeNeverFallsBackToUnrelatedName(t *testing.T) {
	base := &spec.Column{Name: "Status", Source: "ABSENT", Tag: `json:"status"`}
	result := mergeColumns([]*spec.Column{base}, []*spec.Column{{Name: "STATUS", Source: "STATUS", Type: spec.TypeRef{Name: "int"}, DatabaseType: "INTEGER"}})
	if len(result) != 2 || !result[0].Type.IsZero() || result[0].DatabaseType != "" {
		t.Fatalf("unrelated metadata consumed: %+v", result)
	}
	identities, err := resolveResultSources([]*spec.Column{base}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = identities.validateResults([]*spec.Column{base}, []string{"STATUS"}); err == nil {
		t.Fatal("missing source accepted")
	}
}

func TestResultSourceDeclaredColumnsUseOnlyAuthoritativeIdentity(t *testing.T) {
	for _, inferred := range []bool{false, true} {
		columns := []*spec.Column{{Name: "Status", Source: "LOGICAL_STATUS", NameInferred: inferred, ExplicitType: true, Type: spec.TypeRef{Name: "string"}}, {Name: "PersistedStatus", Source: "STATUS", Type: spec.TypeRef{Name: "int"}}, {Name: "Sourceless", ExplicitType: true, Type: spec.TypeRef{Name: "int"}}}
		identities, err := resolveResultSources(columns, nil)
		if err != nil {
			t.Fatal(err)
		}
		declared, err := declaredResultColumns(columns, identities)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(declared, []string{"LOGICAL_STATUS", "Sourceless"}) {
			t.Fatalf("declared=%v", declared)
		}
		columns[0].Type = spec.TypeRef{}
		if _, err = declaredResultColumns(columns, identities); err == nil {
			t.Fatal("CAST without type accepted")
		}
	}
}

func TestResultSourceIdentityRetainsPostEnrichmentVendorAliases(t *testing.T) {
	for _, tc := range []struct {
		name, SQL, goName, tag string
		inferred, foreignKey   bool
	}{
		{name: "canonical alias", SQL: "SELECT c.PARENT_ID AS PARENT FROM child c", goName: "PARENT"},
		{name: "foreign key dual mapping", SQL: "SELECT c.PARENT_ID AS PARENT FROM child c", goName: "PARENT", foreignKey: true},
		{name: "renamed Go row", SQL: "SELECT c.PARENT_ID AS PARENT FROM child c", goName: "Renamed", tag: `sqlx:"PARENT_ID|PARENT"`, inferred: true},
		{name: "name option mapping", SQL: "SELECT c.PARENT_ID AS PARENT FROM child c", goName: "Renamed", tag: `sqlx:"name=PARENT_ID|PARENT"`, inferred: true},
		{name: "nested vendor alias", SQL: "SELECT d.* FROM (SELECT c.PARENT_ID AS PARENT FROM child c) d", goName: "Renamed", tag: `sqlx:"PARENT_ID|PARENT"`, inferred: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := &spec.ViewSource{Table: "child", SQL: tc.SQL}
			column := &spec.Column{Name: "PARENT", Source: "PARENT", ExplicitType: true, Type: spec.TypeRef{Name: "int"}, Tag: tc.tag}
			lineage, err := directProjectionLineage(source)
			if err != nil {
				t.Fatal(err)
			}
			constraint := tableConstraint{name: "PARENT_ID", notNull: true}
			if tc.foreignKey {
				constraint.reference = &tableReference{table: "parent", column: "ID"}
			}
			applyTableConstraints([]*spec.Column{column}, map[string]tableConstraint{"parent_id": constraint}, lineage)
			if column.Source != "PARENT_ID" || !column.NotNull {
				t.Fatalf("constraint compatibility=%+v", column)
			}
			column.Name = tc.goName
			column.NameInferred = tc.inferred
			identities, err := resolveResultSources([]*spec.Column{column}, source)
			if err != nil {
				t.Fatal(err)
			}
			if identities[column] != "PARENT" {
				t.Fatalf("identity=%q column=%+v", identities[column], column)
			}
			declared, err := declaredResultColumns([]*spec.Column{column}, identities)
			if err != nil || !reflect.DeepEqual(declared, []string{"PARENT"}) {
				t.Fatalf("declared=%v err=%v", declared, err)
			}
			view := &spec.View{Columns: []*spec.Column{column}}
			if err = validateResultAnnotations(view, []string{"PARENT"}, identities); err != nil {
				t.Fatal(err)
			}
			if err = identities.validateResults(view.Columns, []string{"PARENT"}); err != nil {
				t.Fatal(err)
			}
			merged := mergeColumnsWithSources(view.Columns, []*spec.Column{{Name: "PARENT", Source: "PARENT", DatabaseType: "INTEGER", Type: spec.TypeRef{Name: "int64"}}}, identities)
			if len(merged) != 1 || merged[0].Source != "PARENT_ID" || merged[0].DatabaseType != "INTEGER" || merged[0].Type.Name != "int" || merged[0].Tag != column.Tag || merged[0].NameInferred != tc.inferred {
				t.Fatalf("merged=%+v", merged)
			}
		})
	}
}

func TestResultSourceIdentityRequiresProvenLineage(t *testing.T) {
	for _, tc := range []struct {
		SQL, tag string
		failure  bool
	}{
		{"SELECT c.OTHER AS PARENT FROM child c", `sqlx:"PARENT_ID|PARENT"`, false},
		{"SELECT c.PARENT_ID+1 AS PARENT FROM child c", `sqlx:"PARENT_ID|PARENT"`, false},
		{"SELECT o.PARENT_ID AS PARENT FROM child c JOIN other o ON o.ID=c.PARENT_ID", `sqlx:"PARENT_ID|PARENT"`, false},
		{"SELECT c.PARENT_ID AS PARENT,c.PARENT_ID AS SECOND FROM child c", `sqlx:"PARENT_ID|PARENT|SECOND"`, true},
	} {
		column := &spec.Column{Name: "PARENT", Source: "PARENT_ID", Tag: tc.tag}
		identities, err := resolveResultSources([]*spec.Column{column}, &spec.ViewSource{Table: "child", SQL: tc.SQL})
		if tc.failure {
			if err == nil || !strings.Contains(err.Error(), "ambiguous") {
				t.Fatalf("SQL=%s err=%v", tc.SQL, err)
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if identities[column] != "PARENT_ID" {
			t.Fatalf("unproven identity=%q SQL=%s", identities[column], tc.SQL)
		}
		if err = identities.validateResults([]*spec.Column{column}, []string{"PARENT"}); err == nil {
			t.Fatal("unproven alias accepted")
		}
	}
}

func TestResultSourceDetectorDoesNotAuthorizeUnrelatedUnknownDriverColumn(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	if err := h.ExecStatements(ctx, `CREATE TABLE results(STATUS, LOGICAL_STATUS)`); err != nil {
		t.Fatal(err)
	}
	logical := &spec.Column{Name: "Status", Source: "LOGICAL_STATUS", ExplicitType: true, Type: spec.TypeRef{Name: "string"}}
	view := &spec.View{Columns: []*spec.Column{logical}}
	refiner := New(nil)
	if _, err := refiner.detectColumns(ctx, h.DB, view, "SELECT LOGICAL_STATUS FROM results"); err != nil {
		t.Fatalf("actual source rejected: %v", err)
	}
	if _, err := refiner.detectColumns(ctx, h.DB, view, "SELECT STATUS, LOGICAL_STATUS FROM results"); err == nil || !strings.Contains(strings.ToLower(err.Error()), "status") {
		t.Fatalf("unrelated Go name authorized unknown metadata: %v", err)
	}
	view.Columns = append(view.Columns, &spec.Column{Name: "PersistedStatus", Source: "STATUS", ExplicitType: true, Type: spec.TypeRef{Name: "int"}})
	if _, err := refiner.detectColumns(ctx, h.DB, view, "SELECT STATUS, LOGICAL_STATUS FROM results"); err != nil {
		t.Fatalf("explicit physical source rejected: %v", err)
	}
}

func TestResultSourceNoDatabaseValidationUsesActualSource(t *testing.T) {
	for _, SQL := range []string{"SELECT STATUS FROM results", "SELECT STATUS, LOGICAL_STATUS FROM results"} {
		view := &spec.View{Namespace: "flight", Source: &spec.ViewSource{SQL: SQL}, Columns: []*spec.Column{{Name: "Status", Source: "LOGICAL_STATUS", ExplicitType: true, Type: spec.TypeRef{Name: "string"}}}}
		err := New(nil).ValidateSourceProjections(&spec.Component{RootView: view}, nil)
		if strings.Contains(SQL, ", LOGICAL_STATUS") {
			if err != nil {
				t.Fatal(err)
			}
		} else if err == nil || !strings.Contains(err.Error(), "absent") {
			t.Fatalf("source validation error=%v", err)
		}
		if view.Source.SQL != SQL {
			t.Fatal("source SQL mutated")
		}
	}
}

func TestResultSourceIdentityDoesNotReplaceActualSourceWithUnrelatedTag(t *testing.T) {
	source := &spec.ViewSource{Table: "results", SQL: "SELECT r.STATUS,r.STATUS AS LOGICAL_STATUS FROM results r"}
	column := &spec.Column{Name: "Status", Source: "STATUS", Tag: `sqlx:"LOGICAL_STATUS"`}
	identities, err := resolveResultSources([]*spec.Column{column}, source)
	if err != nil {
		t.Fatal(err)
	}
	if identities[column] != "STATUS" {
		t.Fatalf("actual source replaced: %q", identities[column])
	}
}

func TestResultSourceRefinerKeepsPhysicalStatusSeparateFromLogicalStatus(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	if err := h.ExecStatements(ctx, `CREATE TABLE results(STATUS INTEGER, LOGICAL_STATUS TEXT)`); err != nil {
		t.Fatal(err)
	}
	for _, reversed := range []bool{false, true} {
		physical := &spec.Column{Name: "PersistedStatus", Source: "STATUS", Tag: `json:"-" invariant:"Physical"`, Type: spec.TypeRef{Name: "int", Pointer: true}, ExplicitType: true}
		logical := &spec.Column{Name: "Status", Source: "LOGICAL_STATUS", Tag: `json:"status" invariant:"Logical"`}
		columns := []*spec.Column{physical, logical}
		SQL := "SELECT r.STATUS,r.LOGICAL_STATUS FROM results r"
		if reversed {
			columns[0], columns[1] = columns[1], columns[0]
			SQL = "SELECT r.LOGICAL_STATUS,r.STATUS FROM results r"
		}
		view := &spec.View{Name: "Results", Namespace: "results", Source: &spec.ViewSource{SQL: SQL}, Columns: columns}
		component := &spec.Component{RootView: view, Settings: &spec.Settings{DefaultConnector: "main"}}
		if err := New(Connections{"main": h.DB}).Refine(ctx, component, nil, nil); err != nil {
			t.Fatal(err)
		}
		if len(view.Columns) != 2 {
			t.Fatalf("columns=%+v", view.Columns)
		}
		byName := map[string]*spec.Column{}
		for _, column := range view.Columns {
			byName[column.Name] = column
		}
		if byName["PersistedStatus"].Type != physical.Type || byName["PersistedStatus"].Source != "STATUS" || byName["PersistedStatus"].Tag != physical.Tag || byName["Status"].Type.Name != "string" || byName["Status"].Source != "LOGICAL_STATUS" || byName["Status"].Tag != logical.Tag {
			t.Fatalf("physical=%+v logical=%+v", byName["PersistedStatus"], byName["Status"])
		}
		if view.Source.SQL != SQL {
			t.Fatalf("authored SQL changed: %s", view.Source.SQL)
		}
	}
}
