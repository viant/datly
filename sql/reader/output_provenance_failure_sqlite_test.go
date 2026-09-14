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
)

func TestOutputProvenanceFailureDoesNotPublishPartialSlotsSQLite(t *testing.T) {
	for _, duplicate := range []bool{true, false} {
		t.Run(map[bool]string{true: "duplicate holder", false: "second query fails"}[duplicate], func(t *testing.T) {
			ctx := context.Background()
			h := sqlite.New(t)
			if err := h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER,name TEXT)", "INSERT INTO records VALUES(1,'one')"); err != nil {
				t.Fatal(err)
			}
			badHolder := "Bounds"
			if duplicate {
				badHolder = "Total"
			}
			component := &spec.Component{RootView: &spec.View{Name: "records", Source: &spec.ViewSource{SQL: "SELECT id,name FROM records"}, Relations: []*spec.Relation{
				{Name: "total", Holder: "Total", Kind: spec.RelationKindDerived, Cardinality: spec.CardinalityOne, View: &spec.View{Name: "total", Source: &spec.ViewSource{SQL: "SELECT COUNT(*) AS count FROM records"}}},
				{Name: "bad", Holder: badHolder, Kind: spec.RelationKindDerived, Cardinality: spec.CardinalityOne, View: &spec.View{Name: "bad", Source: &spec.ViewSource{SQL: "SELECT missing FROM absent_table"}}},
			}}}
			inputType, outputType := reflect.TypeOf(struct{}{}), reflect.TypeOf(outputEvidenceEnvelope{})
			plan, err := compiler.Compile(compiler.Input{Component: component, InputType: inputType, OutputType: outputType, DirectViewField: "Rows"})
			if err != nil {
				t.Fatal(err)
			}
			execution, err := reader.NewExecution(reader.Config{Component: component, InputType: inputType, OutputType: outputType, Plan: plan, SQL: &dsql.SQLComponent{DB: h.DB}})
			if duplicate {
				if err == nil {
					t.Fatal("duplicate output slots survived registration")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			result, err := execution.ReadResult(ctx, &struct{}{}, nil, nil)
			if err == nil || result != nil {
				t.Fatalf("partial output/evidence escaped: %+v err=%v", result, err)
			}
		})
	}
}
