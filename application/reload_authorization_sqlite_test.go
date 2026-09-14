package application_test

import (
	"context"
	"fmt"
	"net/http/httptest"
	"testing"

	"github.com/viant/datly/application"
	"github.com/viant/datly/internal/testharness/mcpclient"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/mcp-protocol/authorization"
	oauthmeta "github.com/viant/mcp-protocol/oauth2/meta"
	"github.com/viant/mcp-protocol/schema"
)

func TestReloadAuthorizationIsGenerationScopedSQLite(t *testing.T) {
	ctx := context.Background()
	f := &reloadFixture{}
	f.init(t)
	manager, err := application.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	compile := func(revision int) func(context.Context, *typecatalog.Catalog) (*application.Build, error) {
		return func(ctx context.Context, types *typecatalog.Catalog) (*application.Build, error) {
			built, err := f.compile(revision)(ctx, types)
			if err != nil {
				return nil, err
			}
			for _, component := range built.Components {
				for _, route := range component.Component.Routes {
					route.MCP = nil
					route.APIKeyHeader = "X-Generation-Key"
					route.APIKeyValue = fmt.Sprint(revision)
				}
			}
			built.MCP.Authorization = &authorization.Policy{Global: &authorization.Authorization{RequiredScopes: []string{"read"}, ProtectedResourceMetadata: &oauthmeta.ProtectedResourceMetadata{Resource: fmt.Sprintf("https://api.example.com/v%d", revision)}}}
			return built, nil
		}
	}
	if err := manager.Reload(ctx, application.Request{Revision: 1, Compile: compile(1)}); err != nil {
		t.Fatal(err)
	}
	pinned, oldService, err := manager.Pin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, revision := range []int{1, 2} {
		if revision == 2 {
			if err := manager.Reload(ctx, application.Request{Revision: 2, Compile: compile(2)}); err != nil {
				t.Fatal(err)
			}
		}
		for _, key := range []string{"", "1", "2"} {
			request := httptest.NewRequest("GET", "/records", nil)
			request.Header.Set("X-Generation-Key", key)
			response := httptest.NewRecorder()
			manager.ServeHTTP(response, request)
			want := 403
			if key == fmt.Sprint(revision) {
				want = 200
			}
			if response.Code != want {
				t.Fatalf("revision=%d key=%s status=%d want=%d", revision, key, response.Code, want)
			}
		}
	}
	request := httptest.NewRequest("GET", "/records", nil).WithContext(pinned)
	request.Header.Set("X-Generation-Key", "1")
	response := httptest.NewRecorder()
	manager.ServeHTTP(response, request)
	if response.Code != 200 {
		t.Fatalf("pinned old authorization=%d", response.Code)
	}
	_, current, err := manager.Pin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if oldService.Authorization().Global.ProtectedResourceMetadata.Resource != "https://api.example.com/v1" || current.Authorization().Global.ProtectedResourceMetadata.Resource != "https://api.example.com/v2" {
		t.Fatal("authorization generations mixed")
	}
}

func TestReloadMCPPolicyChangesOnExistingSessionSQLite(t *testing.T) {
	ctx := context.Background()
	f := &reloadFixture{}
	f.init(t)
	manager, err := application.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Reload(ctx, application.Request{Revision: 1, Compile: f.compile(1)}); err != nil {
		t.Fatal(err)
	}
	client := (mcpclient.Config{Source: manager, ProtocolVersion: schema.LegacyProtocolVersion}).New(t)
	for _, step := range []struct {
		revision  uint64
		protected bool
	}{{1, false}, {2, true}, {3, false}} {
		if step.revision > 1 {
			if err := manager.Reload(ctx, application.Request{Revision: step.revision, Compile: func(ctx context.Context, types *typecatalog.Catalog) (*application.Build, error) {
				built, err := f.compile(2)(ctx, types)
				if err == nil && step.protected {
					built.MCP.Authorization = &authorization.Policy{Global: &authorization.Authorization{ProtectedResourceMetadata: &oauthmeta.ProtectedResourceMetadata{Resource: "https://api.example.com"}}}
				}
				return built, err
			}}); err != nil {
				t.Fatal(err)
			}
		}
		tool := "records.v2"
		if step.revision == 1 {
			tool = "records.v1"
		}
		_, err := client.CallTool(ctx, &schema.CallToolRequestParams{Name: tool})
		if (err != nil) != step.protected {
			t.Fatalf("revision=%d protected=%v err=%v", step.revision, step.protected, err)
		}
	}
}
