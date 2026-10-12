package generate

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe/column"
	sqlio "github.com/viant/sqlx/io"
)

func TestPlainAggregateAliasDiscoveryEmissionRoundTrip(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	if err := h.ExecStatements(ctx, `CREATE TABLE RECORD_STATS (RECORD_ID INTEGER PRIMARY KEY, GROUP_ID INTEGER NOT NULL, EVENT_COUNT INTEGER NOT NULL)`, `INSERT INTO RECORD_STATS VALUES (1,7,3),(2,7,5)`); err != nil {
		t.Fatal(err)
	}
	SQL := `SELECT stats.* FROM (SELECT s.GROUP_ID AS groupId, SUM(s.EVENT_COUNT) AS total FROM RECORD_STATS s ${predicate.Builder().CombineOr($predicate.FilterGroup(0, "AND")).Build("WHERE")} GROUP BY s.GROUP_ID) stats`
	view := &spec.View{Name: "Stats", Source: &spec.ViewSource{Table: "RECORD_STATS", SQL: SQL}, Columns: []*spec.Column{{Name: "total", Source: "total", ExplicitType: true, Nullable: true, Type: spec.TypeRef{Name: "int", Pointer: true}}}}
	component := &spec.Component{Settings: &spec.Settings{DefaultConnector: "main"}, RootView: view}
	refiner := column.New(column.Connections{"main": h.DB})
	if err := refiner.Refine(ctx, component, nil, nil); err != nil {
		t.Fatal(err)
	}
	var generated []*spec.Column
	for index, col := range view.Columns {
		emitted := scalarColumnFieldTag(col, col.Source, false)
		field := reflect.StructOf([]reflect.StructField{{Name: fmt.Sprintf("RenamedField%d", index), Type: reflect.TypeFor[int](), Tag: reflect.StructTag(emitted)}}).Field(0)
		mapping := sqlio.ParseTag(field.Tag)
		generated = append(generated, &spec.Column{Name: field.Name, NameInferred: true, Source: mapping.Column, Tag: string(field.Tag), Type: col.Type, ExplicitType: col.ExplicitType, Nullable: col.Nullable})
		// Runtime descriptors use the first physical alternative as their source.
		for j, c := range mapping.Column {
			if c == '|' {
				generated[len(generated)-1].Source = mapping.Column[:j]
				break
			}
		}
	}
	runtimeView := &spec.View{Name: "Stats", Source: &spec.ViewSource{Table: "RECORD_STATS", SQL: SQL}, Columns: generated}
	runtimeComponent := &spec.Component{Settings: &spec.Settings{DefaultConnector: "main"}, RootView: runtimeView}
	if err := refiner.Refine(ctx, runtimeComponent, nil, nil); err != nil {
		t.Fatal(err)
	}
	if runtimeView.Source.SQL != SQL {
		t.Fatal("SQL changed")
	}
	foundAlias := false
	for _, col := range runtimeView.Columns {
		if col.Source == "GROUP_ID" {
			foundAlias = true
		}
		if col.Source == "GROUP_ID" && reflect.StructTag(col.Tag).Get("sqlx") != "GROUP_ID|groupId,required=true" {
			t.Fatalf("proven alias lost: %+v", col)
		}
	}
	if !foundAlias {
		t.Fatal("physical alias source lost")
	}
	var group, total int
	if err := h.DB.QueryRowContext(ctx, strings.Replace(SQL, `${predicate.Builder().CombineOr($predicate.FilterGroup(0, "AND")).Build("WHERE")}`, "", 1)).Scan(&group, &total); err != nil {
		t.Fatal(err)
	}
	if group != 7 || total != 8 {
		t.Fatalf("unexpected aggregate: %d %d", group, total)
	}
}
