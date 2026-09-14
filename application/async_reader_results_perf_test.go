package application_test

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/application"
	"github.com/viant/datly/typecatalog"
)

func TestCompletedReaderResultReuseCostSQLite(t *testing.T) {
	f := &readerResultsFixture{}
	f.init(t)
	f.start(t)
	f.schedule(t, "cost", 1)
	direct, err := application.New(nil)
	require.NoError(t, err)
	defer direct.Shutdown(context.Background())
	require.NoError(t, direct.Reload(context.Background(), application.Request{Revision: 1, Compile: func(ctx context.Context, types *typecatalog.Catalog) (*application.Build, error) {
		built, err := f.compile(ctx, types)
		if err == nil {
			built.HTTP.Async = nil
		}
		return built, err
	}}))
	for repeat := 0; repeat < 3; repeat++ {
		for _, mode := range []struct {
			name    string
			manager *application.Manager
		}{{"typed HTTP native cache", direct}, {"completed job HTTP native cache", f.manager}} {
			measured := testing.Benchmark(func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					request := httptest.NewRequest("GET", "/read-results?id=7&tenant=1&key=cost", nil)
					request.Header.Set("Authorization", "Bearer "+f.token)
					request.Header.Set("X-Key", "reader-key")
					response := httptest.NewRecorder()
					mode.manager.ServeHTTP(response, request)
					if response.Code != 200 {
						b.Fatalf("%d %s", response.Code, response.Body.String())
					}
				}
			})
			t.Logf("run=%d mode=%s ns/op=%d B/op=%d allocs/op=%d N=%d", repeat+1, mode.name, measured.NsPerOp(), measured.AllocedBytesPerOp(), measured.AllocsPerOp(), measured.N)
		}
	}
}
