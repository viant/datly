package config_test

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/datly/standalone/config"
	"os"
	"path/filepath"
	"testing"
)

func TestPathSemanticsLoadCloneValidation(t *testing.T) {
	for _, format := range []string{"json", "yaml"} {
		for _, mode := range []string{"", "escaped", "decoded", "Decoded", "invalid"} {
			t.Run(format+"/"+mode, func(t *testing.T) {
				var raw []byte
				if format == "json" {
					raw, _ = json.Marshal(map[string]any{"GoBootstrap": map[string]any{"Packages": []string{"example.com/api"}}, "PathSemantics": mode})
				} else {
					raw = []byte(fmt.Sprintf("GoBootstrap:\n  Packages: [example.com/api]\nPathSemantics: %q\n", mode))
				}
				file := filepath.Join(t.TempDir(), "config."+format)
				if err := os.WriteFile(file, raw, 0600); err != nil {
					t.Fatal(err)
				}
				cfg, err := (config.Loader{}).Load(context.Background(), file)
				if err != nil {
					t.Fatal(err)
				}
				if cfg.PathSemantics != mode {
					t.Fatal("load lost value")
				}
				copy, err := cfg.ResolveConstants()
				if err != nil {
					t.Fatal(err)
				}
				if copy.PathSemantics != mode {
					t.Fatal("clone lost value")
				}
				err = copy.Validate()
				if (mode == "Decoded" || mode == "invalid") != (err != nil) {
					t.Fatalf("mode %q validation %v", mode, err)
				}
			})
		}
	}
}
