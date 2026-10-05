package standalone

import (
	"context"
	"github.com/viant/datly/standalone/config"
	fixture "github.com/viant/datly/standalone/testdata/app"
	"github.com/viant/datly/standalone/testdata/app/records"
	"net/http/httptest"
	"testing"
)

func TestStandalonePathSemanticsReloadAndLazyDocs(t *testing.T) {
	for _, mode := range []string{"", "escaped", "decoded"} {
		t.Run(mode, func(t *testing.T) {
			f := fixture.New(t)
			f.WriteConfig(t, func(c map[string]any) {
				c["PathSemantics"] = mode
				c["Info"] = map[string]any{"title": "Path policy", "version": "1"}
			})
			ctx := context.Background()
			cfg, err := (config.Loader{}).Load(ctx, f.Config)
			if err != nil {
				t.Fatal(err)
			}
			exports, err := records.Exports()
			if err != nil {
				t.Fatal(err)
			}
			s, err := New(ctx, Options{Config: cfg, Registry: exports})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Shutdown(ctx)
			if s.source.http.PathSemantics != mode {
				t.Fatal("host copy lost value")
			}
			for _, rev := range []uint64{1, 2} {
				if err = s.Reload(ctx, rev); err != nil {
					t.Fatal(err)
				}
				for _, path := range []string{"/records/a%2Fb", "/v1/api/meta%2Fopenapi"} {
					req := httptest.NewRequest("GET", path, nil)
					originalURL := *req.URL
					originalURI := req.RequestURI
					w := httptest.NewRecorder()
					s.ServeHTTP(w, req)
					want := 400
					if path == "/v1/api/meta%2Fopenapi" {
						want = 404
					}
					if mode == "decoded" {
						want = 404
						if path == "/v1/api/meta%2Fopenapi" {
							want = 200
						}
					}
					if w.Code != want {
						t.Fatalf("revision%d mode%q path%s status%d wanted%d body%s", rev, mode, path, w.Code, want, w.Body.String())
					}
					if *req.URL != originalURL || req.RequestURI != originalURI {
						t.Fatal("inbound URL changed")
					}
				}
			}
		})
	}
}
