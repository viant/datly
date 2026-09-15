package standalone

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness/mcpclient"
	mcpserver "github.com/viant/datly/mcp/server"
	"github.com/viant/datly/standalone/config"
	"github.com/viant/datly/standalone/testdata/app/spend"
	"github.com/viant/mcp-protocol/schema"
	"github.com/viant/mcp/client"
)

func TestStandaloneDiscoveredReportsNativeMCP(t *testing.T) {
	for _, version := range []string{schema.LegacyProtocolVersion, schema.LatestProtocolVersion} {
		t.Run(version, func(t *testing.T) {
			s, f, _ := newStandaloneReport(t)
			native := (mcpclient.Config{Source: s.manager, ProtocolVersion: version}).New(t)
			assertStandaloneReportMCP(t, native, 150)
			query := filepath.Join(f.Root, "spend/queries/spend.sql")
			sql, err := os.ReadFile(query)
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(query, []byte(strings.ReplaceAll(string(sql), "SUM(s.amount)", "2 * SUM(s.amount)")), 0600))
			require.NoError(t, s.Reload(context.Background(), 2))
			assertStandaloneReportMCP(t, native, 300)
			require.NoError(t, os.Remove(query))
			require.Error(t, s.Reload(context.Background(), 3))
			assertStandaloneReportMCP(t, native, 300)
		})
	}
}

func assertStandaloneReportMCP(t *testing.T, native *client.Client, expected float64) {
	t.Helper()
	list, err := native.ListTools(context.Background(), nil)
	require.NoError(t, err)
	encoded, err := json.Marshal(list)
	require.NoError(t, err)
	require.Contains(t, string(encoded), "SpendCubeCompose")
	require.NotContains(t, string(encoded), "ProtectedCube")
	for _, name := range []string{"dimensions", "measures", "filters", "cubes", "sql"} {
		require.Contains(t, string(encoded), name)
	}
	for _, tc := range []struct{ name, kind, body string }{{"SpendCube", "cube", spendCube}, {"SpendCubeCompose", "compose", spendCompose}} {
		var args map[string]any
		require.NoError(t, json.Unmarshal([]byte(tc.body), &args))
		result, err := native.CallTool(context.Background(), &schema.CallToolRequestParams{Name: tc.name, Arguments: args})
		require.NoError(t, err)
		require.NotNil(t, result)
		require.False(t, result.IsError != nil && *result.IsError, "%s: %+v", tc.name, result)
		body, err := json.Marshal(result.StructuredContent)
		require.NoError(t, err)
		assertStandaloneReport(t, body, tc.kind, expected)
		// Denial is a structured tool error; missing required filters must not
		// silently become an unfiltered query on either native protocol.
		missing := strings.ReplaceAll(tc.body, `"tenant":"acme",`, "")
		require.NoError(t, json.Unmarshal([]byte(missing), &args))
		result, err = native.CallTool(context.Background(), &schema.CallToolRequestParams{Name: tc.name, Arguments: args})
		require.NoError(t, err)
		require.NotNil(t, result)
		require.True(t, result.IsError != nil && *result.IsError)
	}
	var args map[string]any
	require.NoError(t, json.Unmarshal([]byte(spendComposeMultiKey), &args))
	result, err := native.CallTool(context.Background(), &schema.CallToolRequestParams{Name: "SpendCubeCompose", Arguments: args})
	require.NoError(t, err)
	require.NotNil(t, result)
	require.False(t, result.IsError != nil && *result.IsError, "multi-key compose: %+v", result)
	body, err := json.Marshal(result.StructuredContent)
	require.NoError(t, err)
	assertStandaloneReport(t, body, "composeMultiKey", expected)
}

func TestStandaloneDiscoveredReportsStdioMCP(t *testing.T) {
	s, f, _ := newStandaloneReport(t)
	// Pass the same configured package set and verifier resources to the child.
	cfg := *s.source.config
	cfg.Info = nil // Loader has already normalized Info into OpenAPI.
	configuration, err := json.Marshal(cfg)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(f.Config, configuration, 0600))
	binary, err := os.Executable()
	require.NoError(t, err)
	for _, version := range []string{schema.LegacyProtocolVersion, schema.LatestProtocolVersion} {
		t.Run(version, func(t *testing.T) {
			native := (mcpclient.StdioConfig{Binary: binary, Args: []string{"-test.run=^TestStandaloneReportStdioProcess$", "--", f.Config}, ProtocolVersion: version}).New(t)
			assertStandaloneReportMCP(t, native, 150)
		})
	}
}

// A subprocess of the actual test binary avoids a second fixture module/build.
// Only the MCP server writes stdout; errors go to stderr before exit.
func TestStandaloneReportStdioProcess(t *testing.T) {
	args := os.Args
	if len(args) < 3 || args[len(args)-2] != "--" {
		return
	}
	run := func() error {
		ctx := context.Background()
		cfg, err := (config.Loader{}).Load(ctx, args[len(args)-1])
		if err != nil {
			return err
		}
		s, err := New(ctx, Options{Config: cfg, Registry: spend.Exports()})
		if err != nil {
			return err
		}
		defer s.Shutdown(ctx)
		if err = s.Reload(ctx, 1); err != nil {
			return err
		}
		protocol, err := mcpserver.New(mcpserver.Config{Source: s.manager, Transport: mcpserver.TransportConfig{Kind: mcpserver.TransportStdio}})
		if err != nil {
			return err
		}
		return protocol.Serve(ctx)
	}
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(0)
}
