package config_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/standalone/config"
)

func TestStatusURIStrictLoadAndClone(t *testing.T) {
	for _, format := range []string{"json", "yaml"} {
		for _, uri := range []string{"", "/service/status/", " "} {
			t.Run(format+"/"+uri, func(t *testing.T) {
				var raw []byte
				if format == "json" {
					raw, _ = json.Marshal(map[string]any{"GoBootstrap": map[string]any{"Packages": []string{"example.com/api"}}, "Meta": map[string]any{"StatusURI": uri}})
				} else {
					raw = []byte(fmt.Sprintf("GoBootstrap:\n  Packages: [example.com/api]\nMeta:\n  StatusURI: %q\n", uri))
				}
				file := filepath.Join(t.TempDir(), "config."+format)
				require.NoError(t, os.WriteFile(file, raw, 0600))
				cfg, err := (config.Loader{}).Load(context.Background(), file)
				require.NoError(t, err)
				require.Equal(t, uri, cfg.Meta.StatusURI)
				clone, err := cfg.ResolveConstants()
				require.NoError(t, err)
				require.Equal(t, uri, clone.Meta.StatusURI)
				require.NoError(t, clone.Validate())
			})
		}
	}
}
