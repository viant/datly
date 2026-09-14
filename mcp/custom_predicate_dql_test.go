package mcp

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness/mcpclient"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/runtime/registry"
	dsql "github.com/viant/datly/sql"
	sqlreader "github.com/viant/datly/sql/reader"
	"github.com/viant/datly/transcribe"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/mcp-protocol/schema"
	"github.com/viant/x"
	xpredicate "github.com/viant/xdatly/predicate"
)

type predicateDQLInput struct {
	Minimum int `json:"minimum"`
}
type predicateDQLThreshold struct {
	Input *predicateDQLInput `bind:"kind=input,required"`
}

func (p *predicateDQLThreshold) Compute(_ context.Context, value any) (*xpredicate.Criteria, error) {
	return &xpredicate.Criteria{Expression: "id >= ?", Placeholders: []any{p.Input.Minimum}}, nil
}

func TestDQLCustomPredicateNativeMCPReadSQLite(t *testing.T) {
	h := sqlite.New(t)
	if err := h.ExecStatements(context.Background(), "CREATE TABLE records (id INTEGER PRIMARY KEY)", "INSERT INTO records VALUES (1),(2),(3)"); err != nil {
		t.Fatal(err)
	}
	types := typecatalog.NewCatalog()
	if err := types.Register(typecatalog.TypeOriginPackage, &x.Type{Type: reflect.TypeOf(predicateDQLThreshold{}), PkgPath: "example.com/query", Name: "Threshold"}); err != nil {
		t.Fatal(err)
	}
	compiled, err := transcribe.NewCompiler().Compile(context.Background(), &transcribe.Source{
		Scope: "example.com/query", Name: "Records", Types: types,
		Text: `#setting($_ = $route('/records','GET'))
#setting($_ = $mcp('records.query'))
#define($_ = $Minimum<int>(query/min).Required().WithPredicate(0,'handler','example.com/query.Threshold'))
SELECT id FROM records
${predicate.Builder().CombineAnd($predicate.FilterGroup(0, "AND")).Build("WHERE")}
ORDER BY id`,
	})
	if err != nil {
		t.Fatal(err)
	}
	type row struct {
		ID int `sqlx:"id" json:"id"`
	}
	type output struct {
		Rows []row `json:"rows"`
	}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: compiled.Component, Types: types, InputType: reflect.TypeOf(predicateDQLInput{}), OutputType: reflect.TypeOf(output{}), DirectViewField: "Rows"})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := sqlreader.NewExecution(sqlreader.Config{Component: artifact.Component, InputType: reflect.TypeOf(predicateDQLInput{}), OutputType: reflect.TypeOf(output{}), Plan: artifact.Reader, SQL: &dsql.SQLComponent{DB: h.DB}})
	if err != nil {
		t.Fatal(err)
	}
	registered := &registry.RegisteredComponent{Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeOf(output{}), Reader: reader}
	native := mcpclient.New(t, runtimeToolService(t, registered), schema.LatestProtocolVersion)
	for _, tt := range []struct {
		minimum int
		want    []row
	}{{1, []row{{1}, {2}, {3}}}, {2, []row{{2}, {3}}}, {3, []row{{3}}}} {
		result, err := native.CallTool(context.Background(), &schema.CallToolRequestParams{Name: "records.query", Arguments: map[string]any{"minimum": tt.minimum}})
		if err != nil || result == nil || result.IsError != nil && *result.IsError {
			t.Fatalf("MCP result=%+v error=%v", result, err)
		}
		encoded, err := json.Marshal(result.StructuredContent)
		if err != nil {
			t.Fatal(err)
		}
		var actual output
		if err := json.Unmarshal(encoded, &actual); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(actual.Rows, tt.want) {
			t.Fatalf("rows=%+v expected=%+v", actual.Rows, tt.want)
		}
	}
}
