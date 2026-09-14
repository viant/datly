package runtime_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/bindly/locator"
	"github.com/viant/datly/bootstrap"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/mcpclient"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/mcp"
	druntime "github.com/viant/datly/runtime"
	rhandler "github.com/viant/datly/runtime/handler"
	handlerprovider "github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/mcp-protocol/schema"
	"github.com/viant/xdatly/connector"
	xhandler "github.com/viant/xdatly/handler"
)

type connectorInput struct {
	Name      string             `parameter:"Name,kind=query,in=name,required" json:"name"`
	Connector connector.Provider `parameter:"Connector,kind=connector,required" json:"-"`
}
type connectorRow struct {
	Value string `json:"value"`
}
type connectorOutput struct {
	Rows []connectorRow `json:"rows"`
}

type connectorFixture struct {
	first, second *sqlite.Harness
	sql           *dsql.SQLComponent
}

func (f *connectorFixture) init(t *testing.T) {
	t.Helper()
	f.first, f.second = sqlite.New(t), sqlite.New(t)
	for index, db := range []*sqlite.Harness{f.first, f.second} {
		if err := db.ExecStatements(context.Background(), "CREATE TABLE records(value TEXT)", fmt.Sprintf("INSERT INTO records VALUES('database-%d')", index+1)); err != nil {
			t.Fatal(err)
		}
	}
	f.sql = &dsql.SQLComponent{DB: f.first.DB}
	for _, entry := range []struct {
		name string
		db   *sqlite.Harness
	}{{"alpha", f.first}, {"beta", f.second}} {
		if err := f.sql.RegisterConnector(entry.name, entry.db.DB); err != nil {
			t.Fatal(err)
		}
	}
}

func (f *connectorFixture) component(t *testing.T, enabled bool) *registry.RegisteredComponent {
	t.Helper()
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/connectors", Name: "Lookup"}, Routes: []*spec.Route{{Method: "GET", Path: "/lookup", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "lookup"}}}}}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeOf(connectorInput{}), OutputType: reflect.TypeOf(connectorOutput{})})
	if err != nil {
		t.Fatal(err)
	}
	registered := &registry.RegisteredComponent{Component: artifact.Component, Input: artifact.Input, Output: artifact.Output, OutputType: reflect.TypeOf(connectorOutput{})}
	if enabled {
		registered.Capabilities.Connector = f.sql
	}
	registered.Handler = rhandler.HandlerFunc(func(ctx context.Context, invocation rhandler.Invocation) (any, error) {
		input := invocation.Input.(*connectorInput)
		dependencies := &struct {
			Connectors connector.Provider `bind:"kind=connector,required"`
		}{}
		if err := invocation.Binder.Bind(ctx, dependencies); err != nil {
			return nil, err
		}
		if dependencies.Connectors != input.Connector {
			return nil, fmt.Errorf("typed connector injection diverged")
		}
		db, err := dependencies.Connectors.Connector(ctx, input.Name)
		if err != nil {
			return nil, err
		}
		rows, err := (&sqlite.Harness{DB: db}).ReadQuery(ctx, sqlite.Query{SQL: "SELECT value FROM records"}, reflect.TypeOf([]connectorRow{}))
		if err != nil {
			return nil, err
		}
		return &connectorOutput{Rows: rows.([]connectorRow)}, nil
	})
	return registered
}

func TestNamedConnectorExactLookupSQLite(t *testing.T) {
	f := &connectorFixture{}
	f.init(t)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	for _, test := range []struct {
		name     string
		provider *dsql.SQLComponent
		ctx      context.Context
		key      string
		want     *sqlite.Harness
		fail     string
		cancel   bool
	}{
		{"first", f.sql, context.Background(), "alpha", f.first, "", false},
		{"second", f.sql, context.Background(), "beta", f.second, "", false},
		{"missing", f.sql, context.Background(), "missing", nil, "not registered", false},
		{"empty", f.sql, context.Background(), " ", nil, "name is required", false},
		{"case sensitive", f.sql, context.Background(), "Alpha", nil, "not registered", false},
		{"no default fallback", &dsql.SQLComponent{DB: f.first.DB}, context.Background(), "alpha", nil, "not registered", false},
		{"canceled", f.sql, canceled, "alpha", nil, "", true},
		{"nil provider", nil, context.Background(), "alpha", nil, "required", false},
		{"nil context", f.sql, nil, "alpha", nil, "required", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, err := test.provider.Connector(test.ctx, test.key)
			if test.cancel {
				if !errors.Is(err, context.Canceled) || db != nil {
					t.Fatalf("db=%v err=%v", db, err)
				}
				return
			}
			if test.fail != "" {
				if err == nil || !strings.Contains(err.Error(), test.fail) || db != nil {
					t.Fatalf("db=%v err=%v", db, err)
				}
				return
			}
			if err != nil || db != test.want.DB {
				t.Fatalf("wrong connector db=%v err=%v", db, err)
			}
			want := "database-1"
			if test.key == "beta" {
				want = "database-2"
			}
			(&sqlite.Harness{DB: db}).AssertQuery(t, context.Background(), sqlite.Query{SQL: "SELECT value FROM records"}, []connectorRow{{want}})
		})
	}
}

func TestConnectorTypedInjectionAndMCPVisibilitySQLite(t *testing.T) {
	f := &connectorFixture{}
	f.init(t)
	registered := f.component(t, true)
	rt, err := druntime.NewRuntime([]*registry.RegisteredComponent{registered})
	if err != nil {
		t.Fatal(err)
	}
	service, err := mcp.New(mcp.Config{Components: []*registry.RegisteredComponent{registered}, Invoker: rt})
	if err != nil {
		t.Fatal(err)
	}
	client := mcpclient.New(t, service, schema.LatestProtocolVersion)
	tools, err := client.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(tools.Tools) != 1 || len(tools.Tools[0].InputSchema.Properties) != 1 || tools.Tools[0].InputSchema.Properties["name"] == nil {
		t.Fatalf("internal provider leaked into MCP schema: %+v", tools)
	}
	for _, test := range []struct{ name, want string }{{"alpha", "database-1"}, {"beta", "database-2"}} {
		t.Run(test.name, func(t *testing.T) {
			result, err := client.CallTool(context.Background(), &schema.CallToolRequestParams{Name: "lookup", Arguments: map[string]any{"name": test.name}})
			if err != nil || result == nil || result.IsError != nil && *result.IsError {
				t.Fatalf("result=%+v err=%v", result, err)
			}
			data, err := json.Marshal(result.StructuredContent)
			if err != nil {
				t.Fatal(err)
			}
			if string(data) != fmt.Sprintf(`{"rows":[{"value":%q}]}`, test.want) {
				t.Fatalf("result=%s", data)
			}
		})
	}
	if result, err := client.CallTool(context.Background(), &schema.CallToolRequestParams{Name: "lookup", Arguments: map[string]any{"name": "alpha", "Connector": "override"}}); err == nil && (result == nil || result.IsError == nil || !*result.IsError) {
		t.Fatalf("MCP provider override accepted: %+v", result)
	}
	absent := f.component(t, false)
	without, err := druntime.NewRuntime([]*registry.RegisteredComponent{absent})
	if err != nil {
		t.Fatal(err)
	}
	_, err = without.InvokeComponent(context.Background(), dexec.ComponentRequest{Target: dexec.ComponentTarget{Component: absent.Component.Key, Route: spec.RouteRef{Method: "GET", Path: "/lookup"}}, Input: &connectorInput{Name: "alpha"}})
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "connector") {
		t.Fatalf("connector capability should require opt-in, got %v", err)
	}
}

type connectorScope []locator.Provider

func (s connectorScope) Providers() []locator.Provider { return s }

func TestConnectorProviderCannotBeOverriddenSQLite(t *testing.T) {
	f := &connectorFixture{}
	f.init(t)
	for _, layer := range []string{"component", "protocol", "child"} {
		t.Run(layer, func(t *testing.T) {
			component := f.component(t, true)
			provider := handlerprovider.Static(xhandler.ConnectorKey, &dsql.SQLComponent{DB: f.second.DB})
			if layer == "component" {
				component.Providers = []locator.Provider{provider}
			}
			rt, err := druntime.NewRuntime([]*registry.RegisteredComponent{component})
			if err != nil {
				t.Fatal(err)
			}
			if layer == "child" {
				_, err = rt.InvokeComponent(context.Background(), dexec.ComponentRequest{Target: dexec.ComponentTarget{Component: component.Component.Key, Route: spec.RouteRef{Method: "GET", Path: "/lookup"}}, Providers: []locator.Provider{provider}})
			} else {
				var scope dexec.ProviderScope
				if layer == "protocol" {
					scope = connectorScope{provider}
				}
				_, err = rt.ExecuteRoute(context.Background(), "GET", "/lookup", scope)
			}
			if err == nil || !strings.Contains(err.Error(), "protected runtime kind") {
				t.Fatalf("layer=%s err=%v", layer, err)
			}
		})
	}
}
