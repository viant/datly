package mcp

import (
	"context"
	"encoding/json"
	"errors"
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
	"github.com/viant/datly/spec"
	"github.com/viant/jsonrpc"
	"github.com/viant/mcp-protocol/schema"
)

type exposureRow struct {
	ID int `sqlx:"id" json:"id"`
}
type exposureResult struct {
	Rows []exposureRow `json:"rows"`
}
type exposureDependencyInput struct {
	Tenant int `parameter:"Tenant,kind=query,in=tenant,required" json:"tenant"`
}
type exposurePublicInput struct {
	Tenant     int             `parameter:"Tenant,kind=query,in=tenant,required" json:"tenant"`
	Dependency *exposureResult `parameter:"Dependency,kind=component,in=GET:/internal-records,required" json:"-"`
}

func TestPackageExposureKeepsCrossProjectDependenciesSQLite(t *testing.T) {
	h := sqlite.New(t)
	if err := h.ExecStatements(context.Background(), "CREATE TABLE records (id INTEGER, tenant INTEGER)", "INSERT INTO records VALUES (1,7),(2,8)"); err != nil {
		t.Fatal(err)
	}
	parentSpec := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "example.org/app/api", Name: "Records"}, Routes: []*spec.Route{{Method: "GET", Path: "/records", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "records.public"}}}}}
	// A private exact path must remain reserved instead of falling through to
	// this public template after endpoint filtering.
	parentSpec.Routes = append(parentSpec.Routes, &spec.Route{Method: "GET", Path: "/{record}"})
	childSpec := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "example.net/shared/records", Name: "Lookup"}, Routes: []*spec.Route{{Method: "GET", Path: "/internal-records", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "records.lookup"}}}}}
	parent, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: parentSpec, InputType: reflect.TypeOf(exposurePublicInput{}), OutputType: reflect.TypeOf(exposureResult{})})
	if err != nil {
		t.Fatal(err)
	}
	child, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: childSpec, InputType: reflect.TypeOf(exposureDependencyInput{}), OutputType: reflect.TypeOf(exposureResult{})})
	if err != nil {
		t.Fatal(err)
	}
	registered := []*registry.RegisteredComponent{
		{Component: parent.Component, Input: parent.Input, OutputType: reflect.TypeOf(exposureResult{}), Handler: customhandler.NewFunc[exposurePublicInput, exposureResult](func(_ context.Context, input *exposurePublicInput) (*exposureResult, error) {
			return input.Dependency, nil
		})},
		{Component: child.Component, Input: child.Input, OutputType: reflect.TypeOf(exposureResult{}), Handler: customhandler.NewFunc[exposureDependencyInput, exposureResult](func(ctx context.Context, input *exposureDependencyInput) (*exposureResult, error) {
			rows, err := h.ReadQuery(ctx, sqlite.Query{SQL: "SELECT id FROM records WHERE tenant = ? ORDER BY id", Args: []any{input.Tenant}}, reflect.TypeOf([]exposureRow{}))
			if err != nil {
				return nil, err
			}
			return &exposureResult{Rows: rows.([]exposureRow)}, nil
		})},
	}
	for _, tt := range []struct {
		name               string
		include, exclude   []string
		public, dependency bool
	}{
		{"only app", []string{"example.org/app/..."}, nil, true, false},
		{"only other project", []string{"example.net/shared/records"}, nil, false, true},
		{"both projects", []string{"example.org/app/api", "example.net/shared/..."}, nil, true, true},
		{"excluded dependency", []string{"..."}, []string{"example.net/shared/..."}, true, false},
		{"none", []string{}, nil, false, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			runtime, err := druntime.NewRuntime(registered, druntime.WithExposedPackages(tt.include, tt.exclude))
			if err != nil {
				t.Fatal(err)
			}
			service, err := New(Config{Components: registered, Invoker: runtime})
			if err != nil {
				t.Fatal(err)
			}
			native := mcpclient.New(t, service, schema.LatestProtocolVersion)
			listed, err := native.ListTools(context.Background(), nil)
			if err != nil {
				var rpcErr *jsonrpc.Error
				if tt.public || tt.dependency || !errors.As(err, &rpcErr) || rpcErr.Code != -32601 {
					t.Fatal(err)
				}
			}
			listedNames := map[string]bool{}
			if listed != nil {
				for _, entry := range listed.Tools {
					listedNames[entry.Name] = true
				}
			}
			for _, endpoint := range []struct {
				path, tool string
				exposed    bool
			}{{"/records", "records.public", tt.public}, {"/internal-records", "records.lookup", tt.dependency}} {
				recorder := httptest.NewRecorder()
				httpgateway.NewHandler(runtime, nil, "").ServeHTTP(recorder, httptest.NewRequest("GET", endpoint.path+"?tenant=7", nil))
				if endpoint.exposed && recorder.Code != 200 || !endpoint.exposed && recorder.Code != 404 {
					t.Fatalf("%s HTTP %d: %s", endpoint.path, recorder.Code, recorder.Body.String())
				}
				if endpoint.exposed {
					var output exposureResult
					if err := json.Unmarshal(recorder.Body.Bytes(), &output); err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(output.Rows, []exposureRow{{ID: 1}}) {
						t.Fatalf("dependency result lost: %+v", output)
					}
				}
				_, found := service.Registry().ToolRegistry.Get(endpoint.tool)
				if found != endpoint.exposed {
					t.Fatalf("%s exposed=%t", endpoint.tool, found)
				}
				if listedNames[endpoint.tool] != endpoint.exposed {
					t.Fatalf("wire listing exposed %s=%t", endpoint.tool, listedNames[endpoint.tool])
				}
				if found {
					result, rpcErr := native.CallTool(context.Background(), &schema.CallToolRequestParams{Name: endpoint.tool, Arguments: map[string]any{"tenant": 7}})
					if rpcErr != nil || result == nil || result.IsError != nil && *result.IsError {
						t.Fatalf("%s result=%+v error=%v", endpoint.tool, result, rpcErr)
					}
					encoded, err := json.Marshal(result.StructuredContent)
					if err != nil {
						t.Fatal(err)
					}
					var output exposureResult
					if err := json.Unmarshal(encoded, &output); err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(output.Rows, []exposureRow{{ID: 1}}) {
						t.Fatalf("MCP dependency result lost: %s", encoded)
					}
				} else {
					result, rpcErr := native.CallTool(context.Background(), &schema.CallToolRequestParams{Name: endpoint.tool, Arguments: map[string]any{"tenant": 7}})
					if rpcErr == nil && (result == nil || result.IsError == nil || !*result.IsError) {
						t.Fatalf("hidden tool invoked: %s", endpoint.tool)
					}
				}
			}
		})
	}
}
