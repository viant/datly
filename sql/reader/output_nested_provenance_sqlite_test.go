package reader_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/reader"
	"github.com/viant/datly/sql/reader/compiler"
	"github.com/viant/datly/sql/reader/readmeta"
)

type nestedOutputSide struct{ Total *outputEvidenceCount }
type nestedOutputEnvelope struct {
	Rows  []outputEvidenceRow
	Left  *nestedOutputSide
	Right nestedOutputSide
	Total *outputEvidenceCount
}

func TestNestedOutputSlotsKeepFullPathsAndAllocatePointersSQLite(t *testing.T) {
	for _, registered := range []bool{true, false} {
		t.Run(map[bool]string{true: "registered execution", false: "direct service"}[registered], func(t *testing.T) {
			ctx := context.Background()
			h := sqlite.New(t)
			if err := h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER,name TEXT)", "INSERT INTO records VALUES(1,'one')"); err != nil {
				t.Fatal(err)
			}
			root := &spec.View{Name: "records", Source: &spec.ViewSource{SQL: "SELECT id,name FROM records WHERE id<0"}}
			for _, slot := range []struct{ name, holder, SQL string }{
				{"left", "Left.Total", "SELECT COUNT(*) AS count FROM ($View.NonWindowSQL) parent"},
				{"right", "Right.Total", "SELECT COUNT(*)+10 AS count FROM ($View.NonWindowSQL) parent"},
				{"shadow", "Total", "SELECT COUNT(*)+100 AS count FROM ($View.NonWindowSQL) parent"},
			} {
				root.Relations = append(root.Relations, &spec.Relation{Name: slot.name, Holder: slot.holder, Kind: spec.RelationKindDerived, Cardinality: spec.CardinalityOne, View: &spec.View{Name: slot.name, Source: &spec.ViewSource{SQL: slot.SQL}}})
			}
			component := &spec.Component{RootView: root}
			inputType, outputType := reflect.TypeOf(struct{}{}), reflect.TypeOf(nestedOutputEnvelope{})
			plan, err := compiler.Compile(compiler.Input{Component: component, InputType: inputType, OutputType: outputType, DirectViewField: "Rows"})
			if err != nil {
				t.Fatal(err)
			}
			var output any
			var projection *readmeta.Result
			if registered {
				execution, err := reader.NewExecution(reader.Config{Component: component, InputType: inputType, OutputType: outputType, Plan: plan, SQL: &dsql.SQLComponent{DB: h.DB}})
				if err != nil {
					t.Fatal(err)
				}
				result, err := execution.ReadResult(ctx, &struct{}{}, nil, nil)
				if err != nil {
					t.Fatal(err)
				}
				output, projection = result.Data, result.Projection
			} else {
				session := &reader.Session{Component: component, InputType: inputType, OutputType: outputType, Artifact: plan, SQL: &dsql.SQLComponent{DB: h.DB}, CollectProjection: true}
				output, err = reader.NewService().Read(ctx, session, &struct{}{}, nil)
				if err != nil {
					t.Fatal(err)
				}
				projection = session.Projection
			}
			actual := output.(*nestedOutputEnvelope)
			if len(actual.Rows) != 0 || actual.Left == nil || actual.Left.Total == nil || actual.Right.Total == nil || actual.Total == nil || actual.Left.Total.Count != 0 || actual.Right.Total.Count != 10 || actual.Total.Count != 100 {
				t.Fatalf("nested output=%+v", actual)
			}
			for _, holder := range []string{"Left.Total", "Right.Total", "Total"} {
				slot, err := projection.Output(holder)
				if err != nil {
					t.Fatal(err)
				}
				row, err := slot.Row(0)
				if err != nil || slot.RootHolder() != holder || !row.Fields().Has("Count") {
					t.Fatalf("slot %s fields=%v err=%v", holder, row, err)
				}
			}
		})
	}
}
