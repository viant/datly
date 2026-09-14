package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness/mcpclient"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/runtime/registry"
	dsql "github.com/viant/datly/sql"
	sqlreader "github.com/viant/datly/sql/reader"
	"github.com/viant/datly/transcribe"
	"github.com/viant/mcp-protocol/schema"
)

type derivedViewRow struct {
	ID int `sqlx:"id" json:"id"`
}
type derivedViewTotals struct {
	Count int `sqlx:"count" json:"count"`
}
type derivedViewBounds struct {
	Minimum int `sqlx:"minimum" json:"minimum"`
	Maximum int `sqlx:"maximum" json:"maximum"`
}
type derivedViewOutput struct {
	Rows   []derivedViewRow   `json:"rows"`
	Totals *derivedViewTotals `json:"totals"`
	Bounds *derivedViewBounds `json:"bounds"`
}

func TestDerivedViewsUseUnpaginatedSourceThroughNativeMCPSQLite(t *testing.T) {
	for _, spelling := range []string{"derived", "summary"} {
		for _, window := range []struct {
			name, SQL                    string
			row, count, minimum, maximum int
		}{
			{"page_control", "SELECT p.id, set_limit(p, 1) FROM records p ORDER BY p.id", 1, 3, 1, 3},
			{"authored_source_limit", "SELECT id FROM records ORDER BY id LIMIT 1 OFFSET 1", 2, 1, 2, 2},
		} {
			t.Run(spelling+"/"+window.name, func(t *testing.T) {
				ctx := context.Background()
				db := sqlite.New(t)
				if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER)", "INSERT INTO records VALUES(1),(2),(3)"); err != nil {
					t.Fatal(err)
				}
				compiled, err := transcribe.NewCompiler().Compile(ctx, &transcribe.Source{Scope: "example.com/derived", Name: "Records", Text: fmt.Sprintf(`#setting($_ = $route('/records','GET'))
#setting($_ = $mcp('records.derived'))
#define($_ = $Totals<Totals>(output/%s) /* SELECT COUNT(*) AS count FROM ($View.Records.NonWindowSQL) parent */)
#define($_ = $Bounds<Bounds>(output/%s) /* SELECT MIN(id) AS minimum, MAX(id) AS maximum FROM ($View.Records.NonWindowSQL) parent */)
%s`, spelling, spelling, window.SQL)})
				if err != nil {
					t.Fatal(err)
				}
				inputType, outputType := reflect.TypeOf(struct{}{}), reflect.TypeOf(derivedViewOutput{})
				artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: compiled.Component, InputType: inputType, OutputType: outputType, DirectViewField: "Rows"})
				if err != nil {
					t.Fatal(err)
				}
				execution, err := sqlreader.NewExecution(sqlreader.Config{Component: artifact.Component, InputType: inputType, OutputType: outputType, Plan: artifact.Reader, SQL: &dsql.SQLComponent{DB: db.DB}})
				if err != nil {
					t.Fatal(err)
				}
				native := mcpclient.New(t, runtimeToolService(t, &registry.RegisteredComponent{Component: artifact.Component, Input: artifact.Input, OutputType: outputType, Reader: execution}), schema.LatestProtocolVersion)
				result, err := native.CallTool(ctx, &schema.CallToolRequestParams{Name: "records.derived", Arguments: map[string]any{}})
				if err != nil || result == nil || result.IsError != nil && *result.IsError {
					_, readErr := execution.Read(ctx, &struct{}{}, nil, nil)
					t.Fatalf("MCP result=%+v err=%v reader=%v", result, err, readErr)
				}
				encoded, err := json.Marshal(result.StructuredContent)
				if err != nil {
					t.Fatal(err)
				}
				var actual derivedViewOutput
				if err := json.Unmarshal(encoded, &actual); err != nil {
					t.Fatal(err)
				}
				want := derivedViewOutput{Rows: []derivedViewRow{{ID: window.row}}, Totals: &derivedViewTotals{Count: window.count}, Bounds: &derivedViewBounds{Minimum: window.minimum, Maximum: window.maximum}}
				if !reflect.DeepEqual(actual, want) {
					t.Fatalf("derived output=%s expected=%+v", encoded, want)
				}
			})
		}
	}
}
