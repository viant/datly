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

func TestExpectedComponentBindingPinnedGenerationAndUnavailableHistoricalArtifact(t *testing.T) {
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
	observe := func() exec.ComponentBinding {
		t.Helper()
		listing, err := native.ListTools(ctx, nil)
		if err != nil || len(listing.Tools) != 1 {
			t.Fatalf("list %+v %v", listing, err)
		}
		raw, err := json.Marshal(listing.Tools[0].Meta[tool.ComponentBindingMetaKey])
		if err != nil {
			t.Fatal(err)
		}
		var pin exec.ComponentBinding
		if err := json.Unmarshal(raw, &pin); err != nil {
			t.Fatal(err)
		}
		return pin
	}
	call := func(pin exec.ComponentBinding) error {
		_, err := native.CallTool(ctx, &schema.CallToolRequestParams{Name: "bound.records", Meta: schema.RequestMetaObject{AdditionalProperties: map[string]interface{}{tool.ComponentBindingMetaKey: pin}}})
		return err
	}
	one := observe()
	if err := call(one); err != nil {
		t.Fatal(err)
	}
	publish(2, "released-artifact-two", strings.Repeat("b", 64))
	// Same tool name after reload must never execute new bytes using an old pin.
	if err := call(one); err == nil {
		t.Fatal("historical pin fell back to current generation")
	}
	two := observe()
	if one == two {
		t.Fatal("source changed without a new binding")
	}
	if err := call(two); err != nil {
		t.Fatal(err)
	}
	unknown := two
	unknown.Revision = "unavailable-artifact"
	if err := call(unknown); err == nil {
		t.Fatal("unavailable artifact fell back to active source")
	}
}
