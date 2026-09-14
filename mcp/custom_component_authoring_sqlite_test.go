package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/viant/bindly/locator"
	"github.com/viant/datly/bootstrap"
	dexec "github.com/viant/datly/exec"
	httpgateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/internal/testharness/mcpclient"
	"github.com/viant/datly/internal/testharness/sqlite"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/mcp-protocol/schema"
	"github.com/viant/xdatly/handler"
	"github.com/viant/xdatly/response"
)

type authoringCheckInput struct {
	TenantID int64 `json:"tenantId" parameter:"TenantID,kind=body,in=tenantId,required=true"`
	ID       int64 `json:"id" parameter:"ID,kind=body,in=id,required=true"`
}
type authoringLookupInput struct {
	TenantID int64 `parameter:"TenantID,kind=query,in=tenantId,required=true"`
	ID       int64 `parameter:"ID,kind=query,in=id,required=true"`
}
type authoringLookupRow struct {
	TenantID int64 `sqlx:"tenant_id"`
	ID       int64 `sqlx:"id"`
}
type authoringLookupOutput struct{ Rows []authoringLookupRow }
type authoringCheckOutput struct {
	Allowed bool `json:"allowed"`
}
type authoringAuthorizer interface {
	Allow(context.Context, authoringLookupRow) (bool, error)
}
type authoringPolicy struct{ calls atomic.Int32 }

func (p *authoringPolicy) Allow(_ context.Context, row authoringLookupRow) (bool, error) {
	p.calls.Add(1)
	return row.TenantID == 7 && row.ID == 0, nil
}

type authoringDenied struct {
	Message string `json:"message"`
	Error   struct {
		Code string `json:"code"`
	} `json:"error"`
	Violations []struct {
		Field   string `json:"field"`
		Message string `json:"message"`
	} `json:"violations"`
}

func TestCustomAuthoringTrustedDependenciesAndPrivateExposureSQLite(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	if err := h.ExecStatements(ctx, "CREATE TABLE records(tenant_id INTEGER,id INTEGER)", "INSERT INTO records VALUES(7,0),(8,0)"); err != nil {
		t.Fatal(err)
	}
	parentSpec := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/app/check", Name: "Check"}, Routes: []*spec.Route{{Method: "POST", Path: "/v1/records/check", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "records.check"}}}}}
	childSpec := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/private/records", Name: "Lookup"}, Routes: []*spec.Route{{Method: "GET", Path: "/internal/records/lookup", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "records.lookup"}}}}}
	parent, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: parentSpec, InputType: reflect.TypeOf(authoringCheckInput{}), OutputType: reflect.TypeOf(authoringCheckOutput{})})
	if err != nil {
		t.Fatal(err)
	}
	child, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: childSpec, InputType: reflect.TypeOf(authoringLookupInput{}), OutputType: reflect.TypeOf(authoringLookupOutput{})})
	if err != nil {
		t.Fatal(err)
	}
	policy := &authoringPolicy{}
	var lookupCalls atomic.Int32
	registered := []*registry.RegisteredComponent{
		{Component: parent.Component, Input: parent.Input, OutputType: reflect.TypeOf(authoringCheckOutput{}), Providers: []locator.Provider{provider.Static("records.authorizer", authoringAuthorizer(policy))}, Handler: custom.New[authoringCheckInput, authoringCheckOutput](handler.ContractFunc[authoringCheckInput, authoringCheckOutput](func(ctx context.Context, session handler.Session, input *authoringCheckInput, output *authoringCheckOutput) error {
			deps := struct {
				Authorizer authoringAuthorizer    `bind:"kind=records.authorizer,required"`
				Invoker    dexec.ComponentInvoker `bind:"kind=component_invoker,required"`
			}{}
			if err := session.Binder().Bind(ctx, &deps); err != nil {
				return err
			}
			value, err := deps.Invoker.InvokeComponent(ctx, dexec.ComponentRequest{Target: dexec.ComponentTarget{Component: childSpec.Key, Route: spec.RouteRef{Method: "GET", Path: "/internal/records/lookup"}}, Input: &authoringLookupInput{TenantID: input.TenantID, ID: input.ID}})
			if err != nil {
				return err
			}
			lookup, ok := value.(*authoringLookupOutput)
			if !ok || len(lookup.Rows) != 1 {
				return fmt.Errorf("unexpected lookup result %T", value)
			}
			row := lookup.Rows[0]
			if row.TenantID != input.TenantID || row.ID != input.ID {
				return fmt.Errorf("lookup input was not forwarded")
			}
			allowed, err := deps.Authorizer.Allow(ctx, row)
			if err != nil {
				return err
			}
			if !allowed {
				payload := authoringDenied{}
				payload.Error.Code = "DENIED"
				payload.Violations = append(payload.Violations, struct {
					Field   string `json:"field"`
					Message string `json:"message"`
				}{"id", "not allowed"})
				return fmt.Errorf("private wrapper: %w", &response.Error{Code: 401, Payload: payload, Cause: fmt.Errorf("private service cause")})
			}
			output.Allowed = true
			return nil
		}))},
		{Component: child.Component, Input: child.Input, OutputType: reflect.TypeOf(authoringLookupOutput{}), Handler: custom.NewFunc[authoringLookupInput, authoringLookupOutput](func(ctx context.Context, input *authoringLookupInput) (*authoringLookupOutput, error) {
			lookupCalls.Add(1)
			rows, err := h.ReadQuery(ctx, sqlite.Query{SQL: "SELECT tenant_id,id FROM records WHERE tenant_id=? AND id=?", Args: []any{input.TenantID, input.ID}}, reflect.TypeOf([]authoringLookupRow{}))
			if err != nil {
				return nil, err
			}
			return &authoringLookupOutput{Rows: rows.([]authoringLookupRow)}, nil
		})},
	}
	runtime, err := druntime.NewRuntime(registered, druntime.WithExposedPackages([]string{"example.com/app/check"}, nil))
	if err != nil {
		t.Fatal(err)
	}
	service, err := New(Config{Components: registered, Invoker: runtime})
	if err != nil {
		t.Fatal(err)
	}
	native := mcpclient.New(t, service, schema.LatestProtocolVersion)
	listed, err := native.ListTools(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(listed.Tools) != 1 || listed.Tools[0].Name != "records.check" {
		t.Fatalf("tools=%+v", listed.Tools)
	}
	schemaJSON, err := json.Marshal(listed.Tools[0])
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(schemaJSON), "records.authorizer") || strings.Contains(string(schemaJSON), "component_invoker") || strings.Contains(string(schemaJSON), `"Has"`) {
		t.Fatalf("internal capability leaked into schema: %s", schemaJSON)
	}
	const denied = `{"message":"","error":{"code":"DENIED"},"violations":[{"field":"id","message":"not allowed"}]}`
	for _, tc := range []struct {
		name           string
		tenant, status int
		want           string
	}{{"allow zero", 7, 200, `{"allowed":true}`}, {"deny same ID other tenant", 8, 401, denied}} {
		t.Run(tc.name, func(t *testing.T) {
			body := fmt.Sprintf(`{"tenantId":%d,"id":0,"records.authorizer":true}`, tc.tenant)
			request := httptest.NewRequest("POST", "/v1/records/check?tenantId=999&id=999", bytes.NewBufferString(body))
			request.Header.Set("Content-Type", "application/json")
			writer := httptest.NewRecorder()
			httpgateway.NewHandler(runtime, nil, "").ServeHTTP(writer, request)
			if writer.Code != tc.status || strings.TrimSpace(writer.Body.String()) != tc.want {
				t.Fatalf("HTTP%d body=%s", writer.Code, writer.Body.String())
			}
			result, err := native.CallTool(ctx, &schema.CallToolRequestParams{Name: "records.check", Arguments: map[string]any{"tenantId": tc.tenant, "id": 0}})
			if err != nil {
				t.Fatal(err)
			}
			encoded, err := json.Marshal(result.StructuredContent)
			if err != nil {
				t.Fatal(err)
			}
			wire, err := json.Marshal(result)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(wire), "private wrapper") || strings.Contains(string(wire), "private service cause") {
				t.Fatalf("private error leaked: %s", wire)
			}
			var got, want any
			if err = json.Unmarshal(encoded, &got); err != nil {
				t.Fatal(err)
			}
			if err = json.Unmarshal([]byte(tc.want), &want); err != nil {
				t.Fatal(err)
			}
			failed := result.IsError != nil && *result.IsError
			if !reflect.DeepEqual(got, want) || failed != (tc.status != 200) {
				t.Fatalf("MCP=%s isError=%v", encoded, failed)
			}
		})
	}
	before := lookupCalls.Load()
	writer := httptest.NewRecorder()
	httpgateway.NewHandler(runtime, nil, "").ServeHTTP(writer, httptest.NewRequest("GET", "/internal/records/lookup?tenantId=7&id=0", nil))
	if writer.Code != 404 {
		t.Fatalf("private HTTP=%d", writer.Code)
	}
	result, err := native.CallTool(ctx, &schema.CallToolRequestParams{Name: "records.lookup", Arguments: map[string]any{}})
	if err == nil && (result == nil || result.IsError == nil || !*result.IsError) {
		t.Fatalf("private MCP unexpectedly succeeded: %+v", result)
	}
	if lookupCalls.Load() != before || before != 4 || policy.calls.Load() != 4 {
		t.Fatalf("calls lookup=%d auth=%d", lookupCalls.Load(), policy.calls.Load())
	}
	h.AssertQuery(t, ctx, sqlite.Query{SQL: "SELECT tenant_id,id FROM records ORDER BY tenant_id"}, []authoringLookupRow{{7, 0}, {8, 0}})
}
