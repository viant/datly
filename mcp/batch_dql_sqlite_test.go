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

type batchDQLChild struct {
	ID       int  `sqlx:"id" json:"id"`
	ParentID int  `sqlx:"parent_id" json:"parentId"`
	Ready    bool `sqlx:"-" json:"ready"`
}

func (c *batchDQLChild) OnRelation(context.Context) { c.Ready = true }

type batchDQLParent struct {
	ID    int              `sqlx:"id" json:"id"`
	Items []*batchDQLChild `sqlx:"-" json:"items"`
}

type batchDQLOutput struct {
	Rows []*batchDQLParent `json:"rows"`
}

func TestDQLRelationBatchingThroughNativeMCPSQLite(t *testing.T) {
	for _, source := range []struct{ name, SQL string }{
		{"columns", "SELECT id,parent_id FROM children ORDER BY id"},
		{"computed_key", "SELECT id,parent_id+0 AS parent_id FROM children ORDER BY id"},
	} {
		for _, concurrency := range []int{0, 1, 3} {
			t.Run(source.name+"/"+fmt.Sprint(concurrency), func(t *testing.T) {
				h := sqlite.New(t)
				ctx := context.Background()
				if err := h.ExecStatements(ctx, "CREATE TABLE parents(id INTEGER)", "INSERT INTO parents VALUES(1),(2),(3)",
					"CREATE TABLE children(id INTEGER,parent_id INTEGER)", "INSERT INTO children VALUES(11,1),(12,1),(21,2),(22,2),(31,3),(32,3)"); err != nil {
					t.Fatal(err)
				}
				compiled, err := transcribe.NewCompiler().Compile(ctx, &transcribe.Source{Scope: "example.com/batches", Name: "Parents", Text: fmt.Sprintf(`#setting($_ = $route('/parents','GET'))
#setting($_ = $mcp('parents.read'))
SELECT p.*, items.*, batch_size(items, 1), batch_concurrency(items, %d)
FROM parents p JOIN (%s) items ON items.parent_id=p.id
ORDER BY p.id`, concurrency, source.SQL)})
				if err != nil {
					t.Fatal(err)
				}
				inputType, outputType := reflect.TypeOf(struct{}{}), reflect.TypeOf(batchDQLOutput{})
				artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: compiled.Component, InputType: inputType, OutputType: outputType, DirectViewField: "Rows"})
				if err != nil {
					t.Fatal(err)
				}
				child := artifact.Reader.Root.Relations[0].Target.View
				if child.Spec.BatchSize != 1 || child.Spec.BatchConcurrency != concurrency {
					t.Fatalf("DQL batch metadata lost: %+v", child)
				}
				execution, err := sqlreader.NewExecution(sqlreader.Config{Component: artifact.Component, InputType: inputType, OutputType: outputType, Plan: artifact.Reader, SQL: &dsql.SQLComponent{DB: h.DB}})
				if err != nil {
					t.Fatal(err)
				}
				native := mcpclient.New(t, runtimeToolService(t, &registry.RegisteredComponent{Component: artifact.Component, Input: artifact.Input, OutputType: outputType, Reader: execution}), schema.LatestProtocolVersion)
				result, err := native.CallTool(ctx, &schema.CallToolRequestParams{Name: "parents.read", Arguments: map[string]any{}})
				if err != nil || result == nil || result.IsError != nil && *result.IsError {
					_, readErr := execution.Read(ctx, &struct{}{}, nil, nil)
					t.Fatalf("MCP result=%+v error=%v reader=%v", result, err, readErr)
				}
				encoded, err := json.Marshal(result.StructuredContent)
				if err != nil {
					t.Fatal(err)
				}
				var actual batchDQLOutput
				if err := json.Unmarshal(encoded, &actual); err != nil {
					t.Fatal(err)
				}
				if len(actual.Rows) != 3 {
					t.Fatalf("parents=%+v", actual.Rows)
				}
				for index, parent := range actual.Rows {
					id := index + 1
					want := []*batchDQLChild{{ID: id*10 + 1, ParentID: id, Ready: true}, {ID: id*10 + 2, ParentID: id, Ready: true}}
					if parent.ID != id || !reflect.DeepEqual(parent.Items, want) {
						t.Fatalf("parent=%+v children=%+v expected=%+v", parent, parent.Items, want)
					}
				}
			})
		}
	}
}
