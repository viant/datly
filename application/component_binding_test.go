package application_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/viant/datly/application"
	"github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/mcpclient"
	"github.com/viant/datly/mcp/tool"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/mcp-protocol/schema"
)

func TestOrdinaryComponentReloadRejectsLegacyPins(t *testing.T) {
	ctx := context.Background()
	f := &reloadFixture{}
	f.init(t)
	manager, err := application.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := manager.Shutdown(ctx); err != nil {
			t.Error(err)
		}
	})
	publish := func(version int, release, fingerprint string) {
		t.Helper()
		compile := func(ctx context.Context, types *typecatalog.Catalog) (*application.Build, error) {
			built, err := f.compile(version)(ctx, types)
			if err != nil {
				return nil, err
			}
			for _, component := range built.Components {
				for _, route := range component.Component.Routes {
					for _, exposure := range route.MCP {
						exposure.Name = "bound.records"
					}
				}
			}
			built.MCP.LinkedArtifact = &exec.LinkedArtifact{Revision: release, ContentFingerprint: fingerprint}
			return built, nil
		}
		if err := manager.Reload(ctx, application.Request{Revision: uint64(version), Compile: compile}); err != nil {
			t.Fatal(err)
		}
	}
	publish(1, "released-artifact-one", strings.Repeat("a", 64))
	native := (mcpclient.Config{Source: manager, ProtocolVersion: "2026-07-28"}).New(t)
	observe := func() {
		t.Helper()
		listing, err := native.ListTools(ctx, nil)
		if err != nil || len(listing.Tools) != 1 {
			t.Fatalf("list %+v %v", listing, err)
		}
		if _, exists := listing.Tools[0].Meta[tool.ComponentBindingMetaKey]; exists {
			t.Fatal("ordinary component identity published on MCP wire")
		}
	}
	call := func(want string) {
		t.Helper()
		result, err := native.CallTool(ctx, &schema.CallToolRequestParams{Name: "bound.records"})
		if err != nil {
			t.Fatal(err)
		}
		if result.IsError != nil && *result.IsError {
			t.Fatalf("tool execution failed: %+v", result)
		}
		body, err := json.Marshal(result.StructuredContent)
		if err != nil || string(body) != want {
			t.Fatalf("active generation body=%s want=%s error=%v", body, want, err)
		}
	}
	reject := func(pin exec.ComponentBinding) {
		t.Helper()
		_, err := native.CallTool(ctx, &schema.CallToolRequestParams{Name: "bound.records", Meta: schema.RequestMetaObject{AdditionalProperties: map[string]interface{}{tool.ComponentBindingMetaKey: pin}}})
		if err == nil || !strings.Contains(err.Error(), "resource") {
			t.Fatalf("legacy pinned intent not rejected clearly: %v", err)
		}
	}
	observe()
	call(`{"rows":[{"value":7}]}`)
	one := exec.ComponentBinding{Revision: "released-artifact-one", ContentFingerprint: strings.Repeat("a", 64)}
	reject(one)
	publish(2, "released-artifact-two", strings.Repeat("b", 64))
	observe()
	reject(one)
	two := exec.ComponentBinding{Revision: "released-artifact-two", ContentFingerprint: strings.Repeat("b", 64)}
	reject(two)
	unknown := two
	unknown.Revision = "unavailable-artifact"
	reject(unknown)
	call(`{"rows":[{"value":"new"}]}`)
}
