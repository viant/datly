package reader_test

import (
	"context"
	"errors"
	requestprovider "github.com/viant/bindly/provider/request"
	"github.com/viant/datly/bootstrap"
	fixture "github.com/viant/datly/bootstrap/testdata/companionchild"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/runtime"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/reader"
	"net/http/httptest"
	"reflect"
	"testing"
)

func TestRootDeferredPageAllowsNativeSourceShapes(t *testing.T) {
	for name, source := range map[string]string{
		"grouped derived":     "SELECT r.* FROM (SELECT id FROM records GROUP BY id) r",
		"explicit projection": "SELECT id FROM records",
		"join":                "SELECT r.id FROM records r JOIN records other ON other.id = r.id",
		"distinct":            "SELECT DISTINCT id FROM records",
	} {
		t.Run(name, func(t *testing.T) { testRootDeferredPageSource(t, source) })
	}
}
func testRootDeferredPageSource(t *testing.T, source string) {

	found, err := bootstrap.ReflectSelectedPackages([]string{reflect.TypeFor[fixture.Component]().PkgPath()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	c, err := found.Components[0].Resolve(reflect.TypeFor[fixture.Input](), reflect.TypeFor[fixture.Output]())
	if err != nil {
		t.Fatal(err)
	}
	c.RootView.Source.SQL = source
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: c, InputType: reflect.TypeFor[fixture.Input](), OutputType: reflect.TypeFor[fixture.Output]()})
	if err != nil {
		t.Fatal(err)
	}
	h := sqlite.New(t)
	if err := h.ExecStatements(t.Context(), "CREATE TABLE records(id INTEGER)", "CREATE TABLE children(id INTEGER,parent_id INTEGER)", "INSERT INTO records VALUES(1),(2),(3)", "INSERT INTO children VALUES(10,1),(11,1),(12,1),(20,2),(21,2),(22,2),(30,3),(31,3),(32,3)"); err != nil {
		t.Fatal(err)
	}
	execution, err := reader.NewExecution(reader.Config{Component: artifact.Component, InputType: reflect.TypeFor[fixture.Input](), OutputType: reflect.TypeFor[fixture.Output](), Plan: artifact.Reader, SQL: &dsql.SQLComponent{DB: h.DB}})
	if err != nil {
		t.Fatal(err)
	}
	app, err := runtime.NewRuntime([]*runtime.RegisteredComponent{{Component: artifact.Component, Input: artifact.Input, Output: artifact.Output, OutputType: reflect.TypeFor[fixture.Output](), Reader: execution}})
	if err != nil {
		t.Fatal(err)
	}
	defer app.Shutdown(context.Background())
	for _, tc := range []struct {
		name, query   string
		parent, child int
		fail          bool
	}{
		{"Page owns root window", "page=2&root_limit=1&limit=1&offset=1", 2, 21, false},
		{"Page alias owns root window", "page=bad&_page=2&root_limit=1&limit=1&offset=1", 2, 21, false},
		{"root aliases and child scope", "root_limit=bad&_limit=1&root_offset=bad&_offset=1&limit=1&offset=1", 2, 21, false},
		{"valid primary wins", "root_limit=1&_limit=2&root_offset=0&_offset=1&limit=1&offset=1", 1, 11, false},
		{"empty primary is zero", "root_limit=1&root_offset=&_offset=1&limit=1&offset=1", 1, 11, false},
		{"malformed without alias preserves default", "root_limit=1&root_offset=bad&limit=1&offset=1", 1, 11, false},
		{"fields fail before malformed alias", "fields=unknown&root_limit=bad&_limit=bad", 0, 0, true},
		{"malformed alias rejected", "root_limit=bad&_limit=bad", 0, 0, true},
		{"repeated numeric rejected", "root_limit=1&root_limit=2", 0, 0, true},
		{"child cannot borrow root alias", "root_limit=1&limit=bad&_limit=1", 0, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scope, err := requestprovider.New(httptest.NewRequest("GET", "/child-selectors?"+tc.query, nil))
			if err != nil {
				t.Fatal(err)
			}
			value, err := app.InvokeComponent(t.Context(), dexec.ComponentRequest{Target: dexec.ComponentTarget{Component: artifact.Component.Key, Route: spec.RouteRef{Method: "GET", Path: "/child-selectors"}}, Providers: scope.Providers()})
			if tc.name == "fields fail before malformed alias" {
				var projection *dsql.UnknownProjectionColumnError
				if !errors.As(err, &projection) {
					t.Fatalf("projection should precede numeric error: %v", err)
				}
			}
			if tc.fail {
				if err == nil {
					t.Fatal("expected invalid selector error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			out := value.(*fixture.Output)
			if len(out.Data) != 1 || out.Data[0].ID != tc.parent || len(out.Data[0].Children) != 1 || out.Data[0].Children[0].ID != tc.child {
				t.Fatalf("root/child selection mismatch: %+v", out)
			}
		})
	}
}
