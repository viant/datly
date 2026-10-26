package standalone

import (
	"context"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/standalone/config"
	fixture "github.com/viant/datly/standalone/testdata/app"
	"github.com/viant/datly/standalone/testdata/app/records"
)

func TestInvocationLoggerReceivesHTTPFailuresAcrossReload(t *testing.T) {
	type useCase struct {
		description string
		eager       bool
	}
	for _, tc := range []useCase{{description: "indexed"}, {description: "eager", eager: true}} {
		t.Run(tc.description, func(t *testing.T) {
			ctx := context.Background()
			f := fixture.New(t)
			cfg, err := (config.Loader{}).Load(ctx, f.Config)
			require.NoError(t, err)
			cfg.GoBootstrap.EagerComponents = tc.eager
			exports, err := records.Exports()
			require.NoError(t, err)
			var output testharness.Output
			logger := slog.New(slog.NewJSONHandler(&output, nil))
			server, err := New(ctx, Options{Config: cfg, Registry: exports, InvocationLogger: logger})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, server.Shutdown(ctx)) })
			for revision := uint64(1); revision <= 2; revision++ {
				require.NoError(t, server.Reload(ctx, revision))
				response := httptest.NewRecorder()
				server.ServeHTTP(response, httptest.NewRequest("GET", "/records/not-an-integer", nil))
				require.GreaterOrEqual(t, response.Code, 400)
			}
			require.Equal(t, 2, strings.Count(output.String(), `"msg":"HTTP route execution failed"`))
			require.Equal(t, 2, strings.Count(output.String(), `"level":"ERROR"`))
		})
	}
}
