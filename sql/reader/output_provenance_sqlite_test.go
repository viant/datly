package reader_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/viant/datly/bootstrap/cacheconfig"
	"github.com/viant/datly/data"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/reader"
	"github.com/viant/datly/sql/reader/compiler"
	"github.com/viant/datly/sql/reader/readmeta"
	"github.com/viant/sqlx/io/read/cache"
	xreader "github.com/viant/xdatly/reader"
)

type outputEvidenceRow struct {
	ID   int     `sqlx:"id"`
	Name *string `sqlx:"name"`
}
type outputEvidenceCount struct {
	Count      int    `sqlx:"count"`
	Unselected string `sqlx:"-"`
}
type outputEvidenceBounds struct {
	MinID      *int `sqlx:"min_id"`
	MaxID      *int `sqlx:"max_id"`
	Unselected *int `sqlx:"-"`
}
type outputEvidenceEnvelope struct {
	Rows   []outputEvidenceRow
	Total  *outputEvidenceCount
	Bounds outputEvidenceBounds
}

func TestDerivedOutputSlotsHaveIndependentSQLiteReadEvidence(t *testing.T) {
	for _, empty := range []bool{true, false} {
		t.Run(map[bool]string{true: "empty root", false: "populated root"}[empty], func(t *testing.T) {
			ctx := context.Background()
			h := sqlite.New(t)
			if err := h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER,name TEXT)", "INSERT INTO records VALUES(1,'one'),(2,NULL)"); err != nil {
				t.Fatal(err)
			}
			predicate := "id>0"
			if empty {
				predicate = "id<0"
			}
			allowNulls := true
			component := &spec.Component{RootView: &spec.View{Name: "records", AllowNulls: &allowNulls, Source: &spec.ViewSource{SQL: "SELECT id,name FROM records WHERE " + predicate + " ORDER BY id"}, Relations: []*spec.Relation{
				{Name: "total", Holder: "Total", Kind: spec.RelationKindDerived, Cardinality: spec.CardinalityOne, View: &spec.View{Name: "total", Source: &spec.ViewSource{SQL: "SELECT COUNT(*) AS count FROM ($View.NonWindowSQL) parent"}}},
				{Name: "bounds", Holder: "Bounds", Kind: spec.RelationKindDerived, Cardinality: spec.CardinalityOne, View: &spec.View{Name: "bounds", AllowNulls: &allowNulls, Source: &spec.ViewSource{SQL: "SELECT MIN(id) AS min_id, MAX(id) AS max_id FROM ($View.NonWindowSQL) parent"}}},
			}}}
			inputType, outputType := reflect.TypeOf(struct{}{}), reflect.TypeOf(outputEvidenceEnvelope{})
			plan, err := compiler.Compile(compiler.Input{Component: component, InputType: inputType, OutputType: outputType, DirectViewField: "Rows"})
			if err != nil {
				t.Fatal(err)
			}
			nativeCache, err := (cacheconfig.Config{Identity: "output-evidence", Settings: &spec.CacheSettings{Enabled: true, Location: t.TempDir(), TTL: "1m"}}).New()
			if err != nil {
				t.Fatal(err)
			}
			caches := map[*data.View]cache.Cache{plan.Root.View: nativeCache}
			for _, relation := range plan.Root.Relations {
				caches[relation.Target.View] = nativeCache
			}
			execution, err := reader.NewExecution(reader.Config{Component: component, InputType: inputType, OutputType: outputType, Plan: plan, SQL: &dsql.SQLComponent{DB: h.DB}, ReadCaches: caches})
			if err != nil {
				t.Fatal(err)
			}
			for pass := 0; pass < 2; pass++ {
				result, err := execution.ReadResult(ctx, &struct{}{}, nil, nil)
				if err != nil {
					t.Fatal(err)
				}
				output := result.Data.(*outputEvidenceEnvelope)
				count := 2
				if empty {
					count = 0
				}
				if len(output.Rows) != count || output.Total == nil || output.Total.Count != count || (output.Bounds.MinID == nil) != empty {
					t.Fatalf("typed output=%+v", output)
				}
				// This assertion is intentionally independent of root cardinality:
				// aggregate slots can contain real rows when the main view is empty.
				slots, ok := any(result.Projection).(interface {
					Output(string) (*readmeta.Result, error)
				})
				if !ok {
					t.Fatal("read result cannot address independent derived output-slot evidence")
				}
				for _, slot := range []struct {
					holder string
					loaded []string
				}{{"Total", []string{"Count"}}, {"Bounds", []string{"MinID", "MaxID"}}} {
					projection, err := slots.Output(slot.holder)
					if err != nil {
						t.Fatal(err)
					}
					if projection.RootHolder() != slot.holder || projection.DirectOutput() {
						t.Fatalf("wrong slot address %s", slot.holder)
					}
					row, err := projection.Row(0)
					if err != nil {
						t.Fatal(err)
					}
					for _, field := range slot.loaded {
						if !row.Fields().Has(field) {
							t.Fatalf("slot %s missing loaded %s", slot.holder, field)
						}
					}
					if row.Fields().Has("Unselected") {
						t.Fatalf("slot %s falsely marks unselected field", slot.holder)
					}
				}
				if pass == 0 {
					if err = h.ExecStatements(ctx, "DROP TABLE records"); err != nil {
						t.Fatal(err)
					}
				}
			}
		})
	}
}

type outputCountPartitions struct{}

func (outputCountPartitions) Partitions(context.Context, xreader.PartitionRequest) ([]xreader.Partition, error) {
	return []xreader.Partition{{Expression: "id <= ?", Args: []any{1}}, {Expression: "id > ?", Args: []any{1}}}, nil
}
func (outputCountPartitions) Reducer(context.Context) xreader.Reducer { return outputCountReducer{} }

type outputCountReducer struct{}

func (outputCountReducer) Reduce(_ context.Context, rows any) (any, error) {
	result := outputEvidenceCount{}
	for _, row := range rows.([]outputEvidenceCount) {
		result.Count += row.Count
	}
	return result, nil
}

func TestDerivedOutputReducerNeverInheritsPartitionColumnEvidenceSQLite(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	if err := h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER,name TEXT)", "INSERT INTO records VALUES(1,'one'),(2,'two')"); err != nil {
		t.Fatal(err)
	}
	component := &spec.Component{RootView: &spec.View{Name: "records", Source: &spec.ViewSource{SQL: "SELECT id,name FROM records"}, Relations: []*spec.Relation{{Name: "total", Holder: "Total", Kind: spec.RelationKindDerived, Cardinality: spec.CardinalityOne, View: &spec.View{Name: "total", Partitioning: &spec.Partitioning{Type: "outputCountPartitions", Concurrency: 2}, Source: &spec.ViewSource{SQL: "SELECT COUNT(*) AS count FROM records"}}}}}}
	inputType, outputType := reflect.TypeOf(struct{}{}), reflect.TypeOf(outputEvidenceEnvelope{})
	plan, err := compiler.Compile(compiler.Input{Component: component, InputType: inputType, OutputType: outputType, DirectViewField: "Rows", TypeLookup: func(string) (reflect.Type, error) { return reflect.TypeOf(outputCountPartitions{}), nil }})
	if err != nil {
		t.Fatal(err)
	}
	execution, err := reader.NewExecution(reader.Config{Component: component, InputType: inputType, OutputType: outputType, Plan: plan, SQL: &dsql.SQLComponent{DB: h.DB}})
	if err != nil {
		t.Fatal(err)
	}
	result, err := execution.ReadResult(ctx, &struct{}{}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if output := result.Data.(*outputEvidenceEnvelope); output.Total == nil || output.Total.Count != 2 {
		t.Fatalf("reduced output=%+v", output)
	}
	projection, err := result.Projection.Output("Total")
	if err != nil {
		t.Fatal(err)
	}
	row, err := projection.Row(0)
	if err != nil {
		t.Fatal(err)
	}
	if row.Fields().Known() || row.Fields().Has("Count") {
		t.Fatal("opaque reducer inherited native partition field evidence")
	}
}
