package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness/mcpclient"
	"github.com/viant/datly/internal/testharness/sqlite"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/sql/dml"
	"github.com/viant/mcp-protocol/schema"
	xhandler "github.com/viant/xdatly/handler"
	"github.com/viant/xdatly/response"
)

func TestNativeMCPPublicErrorBodyDiscardsQueuedWrite(t *testing.T) {
	validation := &xhandler.Validation{Code: 401, Violations: []*xhandler.Violation{{Field: "Name", Message: "denied"}}}
	body := map[string]any{"message": "", "error": nil, "violations": []any{map[string]any{"field": "Name", "message": "denied"}}}
	for _, test := range []struct {
		name    string
		payload any
		err     error
	}{
		{"explicit object", body, &response.Error{Code: 401, Payload: body, Cause: errors.New("PRIVATE cause")}},
		{"explicit null", nil, &response.Error{Code: 401, Payload: nil, Cause: errors.New("PRIVATE cause")}},
		{"typed violations", validation, validation.Err()},
	} {
		t.Run(test.name, func(t *testing.T) {
			h := sqlite.New(t)
			ctx := context.Background()
			if err := h.ExecStatements(ctx, "CREATE TABLE records (id INTEGER PRIMARY KEY, name TEXT, active BOOLEAN, quantity INTEGER, note TEXT)", "INSERT INTO records VALUES (1,'keep',1,9,NULL)"); err != nil {
				t.Fatal(err)
			}
			component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/records", Name: "Patch"}, Routes: []*spec.Route{{Method: "PATCH", Path: "/records", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "records.patch"}}}}}
			artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeOf(sparseInput{}), OutputType: reflect.TypeOf(sparseOutput{})})
			if err != nil {
				t.Fatal(err)
			}
			handler := rhandler.HandlerFunc(func(ctx context.Context, invocation rhandler.Invocation) (any, error) {
				if _, err := NewRecordPatchHandler().Execute(ctx, invocation); err != nil {
					return nil, err
				}
				return &sparseOutput{OK: true}, fmt.Errorf("PRIVATE wrapper: %w", test.err)
			})
			registered := &registry.RegisteredComponent{Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeOf(sparseOutput{}), Handler: handler, DataSource: dml.Source{DB: h.DB}}
			native := mcpclient.New(t, runtimeToolService(t, registered), schema.LatestProtocolVersion)
			listed, err := native.ListTools(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			var inputSchema *schema.ToolInputSchema
			for _, tool := range listed.Tools {
				if tool.Name == "records.patch" {
					copy := tool.InputSchema
					inputSchema = &copy
				}
			}
			if inputSchema == nil {
				t.Fatal("tool missing from native listing")
			}
			result, err := native.CallTool(ctx, &schema.CallToolRequestParams{Name: "records.patch", Arguments: map[string]any{"patch": map[string]any{"id": 1, "name": "changed"}}})
			if err != nil || result == nil {
				t.Fatalf("native tool transport result=%+v err=%v", result, err)
			}
			if result.IsError == nil || !*result.IsError {
				t.Fatalf("expected MCP tool error: %+v", result)
			}
			encoded, err := json.Marshal(result)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(encoded), "PRIVATE") || strings.Contains(string(encoded), `"ok"`) {
				t.Fatalf("private/success content leaked: %s", encoded)
			}
			var wire struct {
				Content []struct {
					Text string `json:"text"`
				} `json:"content"`
				Structured json.RawMessage `json:"structuredContent"`
			}
			if err = json.Unmarshal(encoded, &wire); err != nil || len(wire.Content) != 1 {
				t.Fatalf("content=%s err=%v", encoded, err)
			}
			wantJSON, err := json.Marshal(test.payload)
			if err != nil {
				t.Fatal(err)
			}
			var want, actual any
			if err = json.Unmarshal(wantJSON, &want); err != nil {
				t.Fatal(err)
			}
			if err = json.Unmarshal([]byte(wire.Content[0].Text), &actual); err != nil || !reflect.DeepEqual(want, actual) {
				t.Fatalf("body=%s want=%s err=%v", wire.Content[0].Text, wantJSON, err)
			}
			if test.payload != nil {
				if err = json.Unmarshal(wire.Structured, &actual); err != nil || !reflect.DeepEqual(want, actual) {
					t.Fatalf("structured=%s want=%s err=%v", wire.Structured, wantJSON, err)
				}
			} else if len(wire.Structured) != 0 && string(wire.Structured) != "null" {
				t.Fatalf("explicit null acquired structured payload: %s", wire.Structured)
			}
			publicSchema, err := json.Marshal(inputSchema)
			if err != nil || strings.Contains(string(publicSchema), `"Has"`) || strings.Contains(string(publicSchema), `"has"`) {
				t.Fatalf("internal presence exposed: %s err=%v", publicSchema, err)
			}
			h.AssertQuery(t, ctx, sqlite.Query{SQL: "SELECT id,name,active,quantity,note FROM records"}, []sparseRecord{{ID: 1, Name: "keep", Active: true, Quantity: 9}})
		})
	}
}
