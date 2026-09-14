package mcp

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness/mcpclient"
	"github.com/viant/datly/internal/testharness/sqlite"
	rhandler "github.com/viant/datly/runtime/handler"
	veltyhandler "github.com/viant/datly/runtime/handler/velty"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/sql/dml"
	"github.com/viant/mcp-protocol/schema"
	xhandler "github.com/viant/xdatly/handler"
)

type validationResultService struct {
	result *xhandler.Validation
	err    error
}

func (s validationResultService) Validate(context.Context, any, ...any) (*xhandler.Validation, error) {
	return s.result, s.err
}

func TestNativeMCPValidationStopsSparseWriteSQLite(t *testing.T) {
	for _, kind := range []string{"Go", "Velty"} {
		for _, tt := range []struct {
			name       string
			service    validationResultService
			failed     bool
			stageFirst bool
		}{
			{"success", validationResultService{}, false, false},
			{"violations", validationResultService{result: &xhandler.Validation{Violations: []*xhandler.Violation{{Field: "Name", Message: "name rejected"}}}}, true, false},
			{"failed flag", validationResultService{result: &xhandler.Validation{Failed: true}}, true, false},
			{"service error", validationResultService{err: errors.New("validator unavailable")}, true, false},
			{"queued write then violation", validationResultService{result: &xhandler.Validation{Failed: true}}, true, true},
		} {
			t.Run(kind+"/"+tt.name, func(t *testing.T) {
				h := sqlite.New(t)
				if err := h.ExecStatements(context.Background(), "CREATE TABLE records (id INTEGER PRIMARY KEY, name TEXT, active BOOLEAN, quantity INTEGER, note TEXT)", "INSERT INTO records VALUES (1,'keep',1,9,NULL)"); err != nil {
					t.Fatal(err)
				}
				component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/records", Name: "Patch"}, Routes: []*spec.Route{{Method: "PATCH", Path: "/records", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "records.patch"}}}}}
				artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeOf(sparseInput{}), OutputType: reflect.TypeOf(sparseOutput{})})
				if err != nil {
					t.Fatal(err)
				}
				var handler rhandler.Handler = rhandler.HandlerFunc(func(ctx context.Context, invocation rhandler.Invocation) (any, error) {
					if tt.stageFirst {
						if _, err := NewRecordPatchHandler().Execute(ctx, invocation); err != nil {
							return nil, err
						}
					}
					value, _, err := invocation.Binder.Lookup(ctx, xhandler.ValidatorKey)
					if err != nil {
						return nil, err
					}
					result, err := value.(xhandler.Validator).Validate(ctx, invocation.Input)
					if err != nil {
						return nil, err
					}
					if err := result.Err(); err != nil {
						return nil, err
					}
					if tt.stageFirst {
						return &sparseOutput{OK: true}, nil
					}
					return NewRecordPatchHandler().Execute(ctx, invocation)
				})
				if kind == "Velty" {
					template := `$validator.Check($Input.Patch)
$dml.Update("records", $Input.Patch)
#set($Output.OK = true)`
					if tt.stageFirst {
						template = `$dml.Update("records", $Input.Patch)
$validator.Check($Input.Patch)
#set($Output.OK = true)`
					}
					handler, err = veltyhandler.New[sparseInput, sparseOutput](veltyhandler.Config{Template: template})
					if err != nil {
						t.Fatal(err)
					}
				}
				registered := &registry.RegisteredComponent{Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeOf(sparseOutput{}), Handler: handler, DataSource: dml.Source{DB: h.DB}, Capabilities: rhandler.InvocationCapabilities{Validator: tt.service}}
				native := mcpclient.New(t, runtimeToolService(t, registered), schema.LatestProtocolVersion)
				result, rpcErr := native.CallTool(context.Background(), &schema.CallToolRequestParams{Name: "records.patch", Arguments: map[string]any{"patch": map[string]any{"id": 1, "name": "new"}}})
				failed := rpcErr != nil || result == nil || result.IsError != nil && *result.IsError
				if failed != tt.failed {
					t.Fatalf("result=%+v error=%v", result, rpcErr)
				}
				name := "new"
				if tt.failed {
					name = "keep"
				}
				h.AssertQuery(t, context.Background(), sqlite.Query{SQL: "SELECT id,name,active,quantity,note FROM records"}, []sparseRecord{{ID: 1, Name: name, Active: true, Quantity: 9}})
			})
		}
	}
}
