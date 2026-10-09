package standalone

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/mcpclient"
	"github.com/viant/datly/mcp/tool"
	"github.com/viant/datly/standalone/config"
	fixture "github.com/viant/datly/standalone/testdata/app"
	"github.com/viant/datly/standalone/testdata/app/handlerreport"
	"github.com/viant/mcp-protocol/schema"
)

func TestStandaloneLinkedArtifactOrdinaryMCPLazyAndEager(t *testing.T) {
	for _, eager := range []bool{false, true} {
		t.Run(fmt.Sprintf("eager=%v", eager), func(t *testing.T) {
			ctx := context.Background()
			f := fixture.New(t)
			require.NoError(t, f.DB.ExecStatements(ctx, "CREATE TABLE report_facts(country TEXT,region TEXT,site_id INTEGER,amount INTEGER,enabled INTEGER)", "INSERT INTO report_facts VALUES ('US','MA',1,2,1)"))
			f.WriteConfig(t, func(c map[string]any) {
				c["GoBootstrap"] = map[string]any{"Packages": []string{fixture.Module + "/handlerreport"}, "EagerComponents": eager, "LinkedOnly": !eager}
			})
			cfg, err := (config.Loader{}).Load(ctx, f.Config)
			require.NoError(t, err)
			_ = handlerreport.LinkedDatlyType
			artifact := &exec.LinkedArtifact{Revision: "fixture-artifact-release", ContentFingerprint: strings.Repeat("a", 64)}
			exports, err := handlerreport.Exports()
			require.NoError(t, err)
			server, err := New(ctx, Options{Config: cfg, Registry: exports, LinkedArtifact: artifact})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, server.Shutdown(ctx)) })
			require.NoError(t, server.Reload(ctx, 1))
			artifact.Revision = "caller-mutated"
			native := (mcpclient.Config{Source: server.manager, ProtocolVersion: "2026-07-28"}).New(t)
			listing, err := native.ListTools(ctx, nil)
			require.NoError(t, err)
			var found bool
			for _, entry := range listing.Tools {
				require.NotContains(t, entry.Meta, tool.ComponentBindingMetaKey)
				if entry.Name == "NativeReportCube" {
					found = true
				}
			}
			require.True(t, found)
			args := map[string]interface{}{"dimensions": map[string]interface{}{"country": true}, "measures": map[string]interface{}{"amount": true}, "filters": map[string]interface{}{"permit": true}}
			result, err := native.CallTool(ctx, &schema.CallToolRequestParams{Name: "NativeReportCube", Arguments: args})
			require.NoError(t, err)
			require.False(t, result.IsError != nil && *result.IsError)
			_, err = native.CallTool(ctx, &schema.CallToolRequestParams{Name: "NativeReportCube", Arguments: args, Meta: schema.RequestMetaObject{AdditionalProperties: map[string]interface{}{tool.ComponentBindingMetaKey: "legacy-pin"}}})
			require.Error(t, err)
		})
	}
}
