package application_test

import (
	"context"
	"github.com/viant/datly/application"
	bootstrapindex "github.com/viant/datly/bootstrap/index"
	gateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/gateway/openapi/openapi3"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/typecatalog"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestIndexedPathSemanticsLazyDocuments(t *testing.T) {
	for _, mode := range []string{"", "escaped", "decoded"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			(testharness.GeneratedModule{Path: "example.com/openapi"}).Write(t, root)
			for _, fixture := range []struct{ pkg, name, path string }{{"one", "One", "/one"}, {"two", "Two", "/two"}} {
				writeFixture(t, root, fixture.pkg+"/holder.go", `package `+fixture.pkg+`
import xdatly "github.com/viant/xdatly"
type Input struct { Name string `+"`"+`parameter:"Name,kind=query,in=name"`+"`"+` }
type Output struct { Name string }
type Holder struct { Route xdatly.Component[Input,Output] `+"`"+`component:"`+fixture.name+`,path=`+fixture.path+`,method=GET"`+"`"+` }
`)
			}
			snapshot, err := (bootstrapindex.Builder{Config: bootstrapindex.Config{BaseDir: root, Include: []string{"example.com/openapi/..."}}}).Build(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			materializer := &countingMaterializer{calls: map[string]int{}, regs: map[string]*registry.RegisteredComponent{}}
			manager, err := application.New(nil)
			if err != nil {
				t.Fatal(err)
			}
			if err = manager.Reload(context.Background(), application.Request{Revision: 1, Compile: func(context.Context, *typecatalog.Catalog) (*application.Build, error) {
				return &application.Build{Index: snapshot, Materializer: materializer, HTTP: gateway.Config{PathSemantics: mode, OpenAPI: &gateway.OpenAPIConfig{Info: openapi3.Info{Title: "Indexed", Version: "1"}}}}, nil
			}}); err != nil {
				t.Fatal(err)
			}
			if materializer.count("One")+materializer.count("Two") != 0 {
				t.Fatal("OpenAPI configuration materialized components at bootstrap")
			}
			normal := httptest.NewRecorder()
			manager.ServeHTTP(normal, httptest.NewRequest(http.MethodGet, "/one?name=Ada", nil))
			if normal.Code != http.StatusOK || materializer.count("One") != 1 || materializer.count("Two") != 0 {
				t.Fatalf("normal request status=%d calls=%v", normal.Code, materializer.calls)
			}
			for attempt := 0; attempt < 2; attempt++ {
				document := httptest.NewRecorder()
				manager.ServeHTTP(document, httptest.NewRequest(http.MethodGet, "/v1/api/meta%2Fopenapi", nil))
				want := 404
				if mode == "decoded" {
					want = 200
				}
				if document.Code != want {
					t.Fatalf("document status=%d body=%s", document.Code, document.Body.String())
				}
			}
			wantTwo := 0
			if mode == "decoded" {
				wantTwo = 1
			}
			if materializer.count("One") != 1 || materializer.count("Two") != wantTwo {
				t.Fatalf("OpenAPI component cache calls=%v", materializer.calls)
			}
			if err := manager.Shutdown(context.Background()); err != nil {
				t.Fatal(err)
			}
		})
	}
}
