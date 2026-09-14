package application_test

import (
	"context"
	"testing"

	"github.com/viant/datly/application"
	"github.com/viant/datly/internal/testharness/mcpclient"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/mcp-protocol/schema"
)

func TestReloadMCPEmptyToPopulatedCatalogOnExistingSessionSQLite(t *testing.T) {
	for _, version := range []string{schema.LegacyProtocolVersion, schema.LatestProtocolVersion} {
		t.Run(version, func(t *testing.T) {
			ctx := context.Background()
			f := &reloadFixture{}
			f.init(t)
			manager, err := application.New(nil)
			if err != nil {
				t.Fatal(err)
			}
			empty := func(ctx context.Context, types *typecatalog.Catalog) (*application.Build, error) {
				built, err := f.compile(1)(ctx, types)
				if err != nil {
					return nil, err
				}
				for _, component := range built.Components {
					for _, route := range component.Component.Routes {
						route.MCP = nil
					}
				}
				return built, nil
			}
			if err := manager.Reload(ctx, application.Request{Revision: 1, Compile: empty}); err != nil {
				t.Fatal(err)
			}
			client := (mcpclient.Config{Source: manager, ProtocolVersion: version}).New(t)
			for _, step := range []struct {
				revision uint64
				want     string
			}{{1, ""}, {2, "records.v2"}, {3, ""}} {
				if step.revision > 1 {
					compile := empty
					if step.want != "" {
						compile = f.compile(2)
					}
					if err := manager.Reload(ctx, application.Request{Revision: step.revision, Compile: compile}); err != nil {
						t.Fatal(err)
					}
				}
				result, err := client.ListTools(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				if step.want == "" {
					if len(result.Tools) != 0 {
						t.Fatalf("removed catalog=%+v", result.Tools)
					}
				} else {
					if len(result.Tools) != 1 || result.Tools[0].Name != step.want {
						t.Fatalf("new catalog=%+v", result.Tools)
					}
					if _, err := client.CallTool(ctx, &schema.CallToolRequestParams{Name: step.want}); err != nil {
						t.Fatal(err)
					}
				}
			}
		})
	}
}
