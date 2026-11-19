package translator

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/afs"
	"github.com/viant/datly/cmd/options"
	"github.com/viant/datly/gateway"
	"github.com/viant/datly/gateway/runtime/standalone"
	"github.com/viant/datly/repository"
	"github.com/viant/datly/view"
)

func TestService_persistRouterRule_PreservesReportMetadataOnRouteComponent(t *testing.T) {
	routeRoot := t.TempDir()

	repoOptions := &options.Repository{
		RepositoryURL: routeRoot,
		APIPrefix:     "/v1/api",
	}
	cfg := &Config{
		repository: repoOptions,
		Config: &standalone.Config{
			Config: &gateway.Config{
				ExposableConfig: gateway.ExposableConfig{
					RouteURL: routeRoot,
				},
			},
		},
	}
	svc := &Service{
		Repository: &Repository{
			fs:     afs.New(),
			Config: cfg,
		},
		fs: afs.New(),
	}

	ruleOptions := &options.Rule{
		Project:      routeRoot,
		ModulePrefix: "dev",
		Source:       []string{routeRoot + "/vendors_grouping.sql"},
	}
	require.NoError(t, os.WriteFile(ruleOptions.Source[0], []byte("SELECT 1"), 0o600))
	require.NoError(t, ruleOptions.Init())

	resource := NewResource(ruleOptions, repoOptions, nil)
	resource.Rule.Root = "vendor"
	resource.Rule.Route.URI = "/vendors-grouping"
	resource.Rule.Route.Method = "GET"
	resource.Rule.Report = &repository.Report{Enabled: true}
	resource.Rule.Viewlets.Append(&Viewlet{
		Name: "vendor",
		View: &View{
			View: view.View{
				Name: "vendor",
			},
		},
	})

	require.NoError(t, svc.persistRouterRule(context.Background(), resource, "Reader"))
	require.NotEmpty(t, svc.Repository.Files)

	var persisted string
	for _, candidate := range svc.Repository.Files {
		if strings.HasSuffix(candidate.URL, "vendors_grouping.yaml") {
			persisted = candidate.Content
			break
		}
	}
	require.NotEmpty(t, persisted)
	require.Contains(t, persisted, "Report:")
	require.Contains(t, persisted, "Enabled: true")
}
