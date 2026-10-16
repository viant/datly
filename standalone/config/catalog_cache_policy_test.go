package config_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/standalone/config"
)

func TestMCPToolsListLeaseConfigurationPreservesExplicitZero(t *testing.T) {
	for _, tc := range []struct {
		name, input string
		want        *int64
	}{
		{name: "omitted", input: ""},
		{name: "zero", input: "  ToolsListTtlMs: 0\n", want: new(int64)},
		{name: "custom", input: "  ToolsListTtlMs: 1200\n", want: func() *int64 { v := int64(1200); return &v }()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			file := filepath.Join(t.TempDir(), "config.yaml")
			require.NoError(t, os.WriteFile(file, []byte("GoBootstrap:\n  Packages: [example.com/app]\nMCP:\n  Address: 127.0.0.1:0\n"+tc.input), 0600))
			cfg, err := (config.Loader{}).Load(context.Background(), file)
			require.NoError(t, err)
			require.Equal(t, tc.want, cfg.MCP.ToolsListTtlMs)
		})
	}
	negative := int64(-1)
	_, err := (config.MCP{Address: "127.0.0.1:0", ToolsListTtlMs: &negative}).ListenAddress()
	require.Error(t, err)
}
