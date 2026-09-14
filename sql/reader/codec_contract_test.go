package reader_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/viant/datly/data"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/reader"
	"github.com/viant/datly/sql/reader/compiler"
	"github.com/viant/datly/sql/reader/rowcodec"
)

func TestColumnCodecContractValidation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*rowcodec.Config)
	}{
		{"missing_factory", func(c *rowcodec.Config) { c.Factory = nil }},
		{"missing_source", func(c *rowcodec.Config) { c.Columns[0].DataType = "" }},
		{"bad_source", func(c *rowcodec.Config) { c.Columns[0].DataType = "MissingType" }},
		{"missing_destination", func(c *rowcodec.Config) { c.Columns[0].Name = "Missing" }},
		{"conflicting_output", func(c *rowcodec.Config) { c.Columns[0].Codec.OutputType = "[]int" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			config := rowcodec.Config{RowType: reflect.TypeOf(codecTestRow{}), Factory: &splitCodecFactory{}, Columns: []*data.Column{{Name: "Tags", Column: "tags", DataType: "string", Codec: &spec.Codec{Body: "split"}}}}
			tc.change(&config)
			if _, err := rowcodec.Compile(config); err == nil {
				t.Fatal("invalid codec contract accepted")
			}
		})
	}
}

type CodecIdentity struct {
	ID int `sqlx:"id"`
}
type embeddedCodecRow struct {
	*CodecIdentity
	Codec0 int      `sqlx:"number"`
	Tags   []string `sqlx:"tags,type=string" codec:"split"`
}

func TestColumnCodecEmbeddedSQLite(t *testing.T) {
	type output struct{ Rows []embeddedCodecRow }
	for _, tc := range []struct {
		name, SQL string
		selected  bool
	}{{"selected_zero_ID", "SELECT id,number,tags FROM records", true}, {"projected_away", "SELECT number FROM records", false}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			db := sqlite.New(t)
			if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER,number INTEGER,tags TEXT)", "INSERT INTO records VALUES(0,7,'a')"); err != nil {
				t.Fatal(err)
			}
			component := &spec.Component{RootView: &spec.View{Name: "records", Source: &spec.ViewSource{SQL: tc.SQL}}}
			factory := &splitCodecFactory{}
			plan, err := compiler.Compile(compiler.Input{Component: component, InputType: reflect.TypeOf(struct{}{}), OutputType: reflect.TypeOf(output{}), DirectViewField: "Rows", CodecFactory: factory})
			if err != nil {
				t.Fatal(err)
			}
			execution, err := reader.NewExecution(reader.Config{Component: component, InputType: reflect.TypeOf(struct{}{}), OutputType: reflect.TypeOf(output{}), Plan: plan, SQL: &dsql.SQLComponent{DB: db.DB}})
			if err != nil {
				t.Fatal(err)
			}
			actual, err := execution.Read(ctx, &struct{}{}, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			rows := actual.(*output).Rows
			if len(rows) != 1 || rows[0].Codec0 != 7 {
				t.Fatalf("rows=%+v", rows)
			}
			if tc.selected {
				if rows[0].CodecIdentity == nil || rows[0].ID != 0 || !reflect.DeepEqual(rows[0].Tags, []string{"a"}) {
					t.Fatalf("selected row=%+v", rows[0])
				}
			} else if rows[0].CodecIdentity != nil || rows[0].Tags != nil || factory.calls.Load() != 0 {
				t.Fatalf("unselected fields changed: %+v", rows[0])
			}
		})
	}
}
