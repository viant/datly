package developer_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/viant/datly/internal/testharness/mcpclient"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/mcp/developer"
	dserver "github.com/viant/datly/mcp/server"
	"github.com/viant/datly/transcribe"
	"github.com/viant/datly/transcribe/column"
	"github.com/viant/jsonrpc"
	"github.com/viant/mcp-protocol/schema"
)

// This is an enabled loopback acceptance test. Environments that forbid
// listeners must run it in the parent's transport-enabled test environment.
func TestNativeDeveloperValidationDiscoveryAndCalls(t *testing.T) {
	for _, version := range []string{"2025-11-25", schema.LatestProtocolVersion} {
		t.Run(version, func(t *testing.T) {
			valid := newValidationFixture(t, "SELECT 1 AS ID")
			invalid := newValidationFixture(t, "SELECT (")
			discovered := newValidationFixture(t, "SELECT id FROM records")
			db := sqlite.New(t)
			if err := db.ExecStatements(context.Background(), "CREATE TABLE records(id INTEGER NOT NULL)", "PRAGMA query_only=ON"); err != nil {
				t.Fatal(err)
			}
			discovered.validator.Connector = "main"
			discovered.validator.ColumnRefiner = column.New(column.Connections{"main": db.DB})
			service, err := developer.New(developer.Config{Targets: map[string]transcribe.Validator{"valid": valid.validator, "invalid": invalid.validator, "schema": discovered.validator}})
			if err != nil {
				t.Fatal(err)
			}
			native := mcpclient.New(t, service, version)
			ctx := context.Background()
			listing, err := native.ListTools(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			if len(listing.Tools) != 7 {
				t.Fatalf("unexpected developer catalog: %+v", listing)
			}
			tool := validationToolMetadata(t, listing.Tools)
			if tool.Description == nil || tool.Annotations == nil || tool.Annotations.ReadOnlyHint == nil || !*tool.Annotations.ReadOnlyHint || tool.OutputSchema == nil || tool.OutputSchema.Properties["valid"]["type"] != "boolean" {
				t.Fatalf("missing discoverable validation contract: %+v", tool)
			}
			encoded, err := json.Marshal(tool.InputSchema)
			if err != nil {
				t.Fatal(err)
			}
			var input map[string]any
			if err = json.Unmarshal(encoded, &input); err != nil {
				t.Fatal(err)
			}
			if input["type"] != "object" || !reflect.DeepEqual(tool.InputSchema.Required, []string{"target"}) || len(tool.InputSchema.Properties) != 1 || !reflect.DeepEqual(tool.InputSchema.Properties["target"]["enum"], []any{"invalid", "schema", "valid"}) {
				t.Fatalf("unexpected wire input schema: %s", encoded)
			}
			for name, fixture := range map[string]validationFixture{"valid": valid, "invalid": invalid, "schema": discovered} {
				before := fixture.snapshot(t)
				expected, _ := fixture.validator.Validate(ctx)
				result, rpcErr := native.CallTool(ctx, &schema.CallToolRequestParams{Name: developer.ValidationTool, Arguments: map[string]any{"target": name}})
				if rpcErr != nil {
					t.Fatal(rpcErr)
				}
				assertReport(t, result, expected)
				if !reflect.DeepEqual(before, fixture.snapshot(t)) {
					t.Fatal("wire validation wrote project files")
				}
			}
			_, rpcErr := native.CallTool(ctx, &schema.CallToolRequestParams{Name: developer.ValidationTool, Arguments: map[string]any{"target": "valid", "dsn": "not-accepted"}})
			var protocolErr *jsonrpc.Error
			if !errors.As(rpcErr, &protocolErr) || protocolErr.Code != jsonrpc.InvalidParams {
				t.Fatalf("client authority override was not rejected: %v", rpcErr)
			}
		})
	}
}

// Exercises the real protocol handler/registry without claiming wire transport
// proof; the loopback test above remains separately enabled.
func TestDeveloperNativeHandlerCatalogAndReport(t *testing.T) {
	fixture := newValidationFixture(t, "SELECT (")
	service, err := developer.New(developer.Config{Targets: map[string]transcribe.Validator{"records": fixture.validator}})
	if err != nil {
		t.Fatal(err)
	}
	factory, err := dserver.NewHandler(service)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	handler, err := factory(ctx, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	handler.(*dserver.Handler).ClientInitialize = &schema.InitializeRequestParams{ProtocolVersion: schema.LatestProtocolVersion}
	listing, rpcErr := handler.ListTools(ctx, &jsonrpc.TypedRequest[*schema.ListToolsRequest]{Request: &schema.ListToolsRequest{}})
	if rpcErr != nil || len(listing.Tools) != 7 {
		t.Fatalf("native catalog: %+v %v", listing, rpcErr)
	}
	tool := validationToolMetadata(t, listing.Tools)
	encoded, err := json.Marshal(tool.InputSchema)
	if err != nil {
		t.Fatal(err)
	}
	var shape map[string]any
	if err = json.Unmarshal(encoded, &shape); err != nil || shape["type"] != "object" || !reflect.DeepEqual(tool.InputSchema.Required, []string{"target"}) {
		t.Fatalf("invalid native request schema: %s %v", encoded, err)
	}
	if len(tool.InputSchema.Properties) != 1 || tool.OutputSchema.Properties["valid"]["type"] != "boolean" {
		t.Fatalf("native input/output schema: %+v", tool)
	}
	expected, _ := fixture.validator.Validate(ctx)
	result, rpcErr := handler.CallTool(ctx, &jsonrpc.TypedRequest[*schema.CallToolRequest]{Request: &schema.CallToolRequest{Params: schema.CallToolRequestParams{Name: developer.ValidationTool, Arguments: map[string]any{"target": "records"}}}})
	if rpcErr != nil {
		t.Fatal(rpcErr)
	}
	assertReport(t, result, expected)
}

func validationToolMetadata(t *testing.T, tools []schema.Tool) schema.Tool {
	t.Helper()
	for _, tool := range tools {
		if tool.Name == developer.ValidationTool {
			return tool
		}
	}
	t.Fatal("validation tool missing")
	return schema.Tool{}
}
