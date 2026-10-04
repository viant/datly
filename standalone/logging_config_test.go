package standalone

import (
	"context"
	"encoding/json"
	internallog "github.com/viant/datly/internal/logging"
	"github.com/viant/datly/runtime"
	"github.com/viant/datly/standalone/config"
	"testing"
)

func TestStandaloneLoggingProfileWithoutObservation(t *testing.T) {
	for _, tc := range []struct {
		name, document string
		enabled        bool
	}{
		{"native", `{}`, false},
		{"default", `{"Logging":{}}`, true},
		{"disabled", `{"Logging":{"EnableAudit":false,"EnableTracing":false}}`, false},
		{"trace", `{"Logging":{"EnableAudit":false,"EnableTracing":true}}`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var cfg config.Config
			if err := json.Unmarshal([]byte(tc.document), &cfg); err != nil {
				t.Fatal(err)
			}
			resolved, err := cfg.ResolveConstants()
			if err != nil {
				t.Fatal(err)
			}
			observation, err := (&source{config: resolved}).services(context.Background(), nil)
			if err != nil {
				t.Fatal(err)
			}
			owner, err := runtime.NewObservability(observation)
			if err != nil {
				t.Fatal(err)
			}
			defer owner.Shutdown(context.Background())
			if internallog.Enabled(owner.Recorder) != tc.enabled {
				t.Fatal("wrong logging policy")
			}
			if resolved.Metrics != nil || observation.OTel != nil || observation.Logger != nil {
				t.Fatal("logging changed unrelated observation policies")
			}
			if cfg.Logging != nil && cfg.Logging.EnableAudit != nil {
				previous := *resolved.Logging.EnableAudit
				*cfg.Logging.EnableAudit = !*cfg.Logging.EnableAudit
				if *resolved.Logging.EnableAudit != previous {
					t.Fatal("resolved policy aliases caller")
				}
			}
		})
	}
}
