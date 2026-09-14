package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/viant/datly/bootstrap"
	httpgateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/internal/testharness/mcpclient"
	"github.com/viant/datly/internal/testharness/sqlite"
	druntime "github.com/viant/datly/runtime"
	customhandler "github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/registry"
	dtag "github.com/viant/datly/tag"
	"github.com/viant/mcp-protocol/schema"
)

func TestGoShapeEmptyQueryPolicyHTTPAndMCP(t *testing.T) {
	type input struct {
		Tenant string `parameter:"Tenant,kind=query,in=tenant,value=7" json:"tenant"`
	}
	type row struct {
		ID int `sqlx:"id"`
	}
	type output struct {
		Rows []row `json:"rows"`
	}
	h := sqlite.New(t)
	if err := h.ExecStatements(context.Background(), "CREATE TABLE records (id INTEGER, tenant INTEGER)", "INSERT INTO records VALUES (1,7),(2,0)"); err != nil {
		t.Fatal(err)
	}
	for _, ignore := range []bool{false, true} {
		t.Run(fmt.Sprint(ignore), func(t *testing.T) {
			tag := reflect.StructTag(fmt.Sprintf(`component:"Records,path=/records,method=GET" ignoreEmptyQueryParameters:"%t" mcp:"[{\"kind\":\"tool\",\"name\":\"records\"}]"`, ignore))
			metadata, found, err := dtag.ParseComponent(tag)
			if err != nil || !found {
				t.Fatalf("tag=%v error=%v", tag, err)
			}
			component, err := (&bootstrap.RouteSource{PackagePath: "example.com/records", FieldName: "Records", Tag: metadata}).Resolve(reflect.TypeOf(input{}), reflect.TypeOf(output{}))
			if err != nil {
				t.Fatal(err)
			}
			artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeOf(input{}), OutputType: reflect.TypeOf(output{})})
			if err != nil {
				t.Fatal(err)
			}
			registered := &registry.RegisteredComponent{Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeOf(output{}), Handler: customhandler.NewFunc[input, output](func(ctx context.Context, input *input) (*output, error) {
				rows, err := h.ReadQuery(ctx, sqlite.Query{SQL: "SELECT id FROM records WHERE tenant=?", Args: []any{input.Tenant}}, reflect.TypeOf([]row{}))
				if err != nil {
					return nil, err
				}
				return &output{Rows: rows.([]row)}, nil
			})}
			runtime, err := druntime.NewRuntime([]*registry.RegisteredComponent{registered})
			if err != nil {
				t.Fatal(err)
			}
			service, err := New(Config{Components: []*registry.RegisteredComponent{registered}, Invoker: runtime})
			if err != nil {
				t.Fatal(err)
			}
			native := mcpclient.New(t, service, schema.LatestProtocolVersion)
			for _, value := range []string{"", "0", "7"} {
				recorder := httptest.NewRecorder()
				httpgateway.NewHandler(runtime, nil, "").ServeHTTP(recorder, httptest.NewRequest("GET", "/records?tenant="+value, nil))
				if recorder.Code != 200 {
					t.Fatalf("HTTP ignore=%t value=%q code=%d body=%s", ignore, value, recorder.Code, recorder.Body.String())
				}
				wantID := 1
				if value == "0" {
					wantID = 2
				}
				want := []row{{ID: wantID}}
				if value == "" && !ignore {
					want = nil
				}
				{
					var actual output
					if err := json.Unmarshal(recorder.Body.Bytes(), &actual); err != nil {
						t.Fatal(err)
					}
					if len(actual.Rows) != len(want) || len(want) > 0 && !reflect.DeepEqual(actual.Rows, want) {
						t.Fatalf("HTTP rows=%+v", actual.Rows)
					}
				}
				result, err := native.CallTool(context.Background(), &schema.CallToolRequestParams{Name: "records", Arguments: map[string]any{"tenant": value}})
				failed := err != nil || result == nil || result.IsError != nil && *result.IsError
				if failed {
					t.Fatalf("MCP ignore=%t value=%q result=%+v error=%v", ignore, value, result, err)
				}
				if !failed {
					encoded, err := json.Marshal(result.StructuredContent)
					if err != nil {
						t.Fatal(err)
					}
					var actual output
					if err := json.Unmarshal(encoded, &actual); err != nil {
						t.Fatal(err)
					}
					if len(actual.Rows) != len(want) || len(want) > 0 && !reflect.DeepEqual(actual.Rows, want) {
						t.Fatalf("MCP rows=%+v", actual.Rows)
					}
				}
			}
		})
	}
}
