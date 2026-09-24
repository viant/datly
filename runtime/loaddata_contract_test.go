package runtime

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/viant/afs"
	"github.com/viant/afs/storage"
	"github.com/viant/bindly/locator"
	"github.com/viant/bindly/provider/request"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/handler/loaddata"
	"github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
)

type countedOpener struct {
	storage.Opener
	urls []string
}

func (s *countedOpener) OpenURL(ctx context.Context, url string, options ...storage.Option) (io.ReadCloser, error) {
	s.urls = append(s.urls, url)
	return s.Opener.OpenURL(ctx, url, options...)
}

func TestLoadDataUsesOrdinaryHandlerDI(t *testing.T) {
	type row struct {
		ID int `json:"id"`
	}
	for _, test := range []struct {
		name, format, compression, data string
		max                             int64
		want                            []row
		invalid                         bool
	}{
		{"single", "json", "", `{"id":1}`, 0, []row{{1}}, false},
		{"array", "json-array", "", `[{"id":1},{"id":2}]`, 0, []row{{1}, {2}}, false},
		{"ndjson", "ndjson", "", "{\"id\":1}\n{\"id\":2}\n", 0, []row{{1}, {2}}, false},
		{"gzip", "json", "gzip", `{"id":1}`, 0, []row{{1}}, false},
		{"trailing", "json", "", `{"id":1} {"id":2}`, 0, nil, true},
		{"bounded-inflated", "json", "gzip", `{"id":1}`, 4, nil, true},
		{"wrong-type", "json", "", `{"id":"not-an-int"}`, 0, nil, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			data := []byte(test.data)
			if test.compression == "gzip" {
				var compressed bytes.Buffer
				w := gzip.NewWriter(&compressed)
				_, _ = w.Write(data)
				if err := w.Close(); err != nil {
					t.Fatal(err)
				}
				data = compressed.Bytes()
			}
			// Suffix is deliberately misleading: compression is declared, never guessed.
			path := filepath.Join(t.TempDir(), "data.gz")
			if err := os.WriteFile(path, data, 0600); err != nil {
				t.Fatal(err)
			}
			fs := afs.New()
			defer fs.CloseAll()
			opener := &countedOpener{Opener: fs}
			cfg := loaddata.Config{URL: path, Format: test.format, Compression: test.compression, MaxBytes: test.max}
			encoded, err := json.Marshal(cfg)
			if err != nil {
				t.Fatal(err)
			}
			constant := string(encoded)
			component := componentSpec("LoadData", "GET", "/load-data", []*spec.Parameter{{Name: "Config", Source: spec.BindSource{Kind: "const", Name: "LoadData"}, Value: &constant}})
			artifact := componentArtifact(t, component, reflect.TypeFor[loaddata.Input](), reflect.TypeFor[loaddata.Output[row]]())
			rt, err := NewRuntime([]*registry.RegisteredComponent{{Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeFor[loaddata.Output[row]](), Handler: custom.New(loaddata.New[row]()), Providers: []locator.Provider{provider.Static("afs", opener)}}})
			if err != nil {
				t.Fatal(err)
			}
			defer rt.Shutdown(context.Background())
			req := httptest.NewRequest(http.MethodGet, "http://studio/load-data?Config=forged&Storage=forged", nil)
			scope, err := request.New(req)
			if err != nil {
				t.Fatal(err)
			}
			result, err := rt.ExecuteRoute(context.Background(), http.MethodGet, "/load-data", scope)
			if test.invalid {
				if err == nil {
					t.Fatal("invalid document was accepted")
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if got := result.(*loaddata.Output[row]).Data; !reflect.DeepEqual(got, test.want) {
					t.Fatalf("rows=%v want=%v", got, test.want)
				}
			}
			if !reflect.DeepEqual(opener.urls, []string{path}) {
				t.Fatalf("storage URL inferred or overridden: %v", opener.urls)
			}
		})
	}
}
