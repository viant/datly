package gorouter

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/view/extension"
	"github.com/viant/xreflect"
)

func TestDiscover_MultiFieldRouters(t *testing.T) {
	baseDir := t.TempDir()
	writeFile(t, filepath.Join(baseDir, "go.mod"), "module example.com/app\n\ngo 1.24\n")
	writeFile(t, filepath.Join(baseDir, "pkg", "routes", "routes.go"), `package routes

type ReportView struct {
	ID int `+"`"+`sqlx:"ID"`+"`"+`
}

type ReportInput struct {
	ID int `+"`"+`parameter:",kind=path,in=id"`+"`"+`
}

type ReportOutput struct {
	Data []*ReportView `+"`"+`parameter:",kind=output,in=view"`+"`"+`
}

type CreateInput struct {
	Name string `+"`"+`parameter:",kind=body,in=name"`+"`"+`
}

type CreateOutput struct {
	Status string `+"`"+`parameter:",kind=output,in=status"`+"`"+`
}

type Router struct {
	Report struct{} `+"`"+`component:",path=/v1/report/{id},method=GET,input=ReportInput,output=ReportOutput"`+"`"+`
	Create struct{} `+"`"+`component:",path=/v1/report,method=POST,input=CreateInput,output=CreateOutput"`+"`"+`
}
`)

	routes, err := Discover(context.Background(), baseDir, []string{"example.com/app/pkg/..."}, nil)
	if err != nil {
		t.Fatalf("discover failed: %v", err)
	}
	if len(routes) != 2 {
		t.Fatalf("unexpected route count: %d", len(routes))
	}
	if routes[0].InputRef == "" || routes[0].OutputRef == "" {
		t.Fatalf("expected fully-qualified contract refs, but had %#v", routes[0])
	}
	if routes[0].Source == nil || routes[0].Source.TypeRegistry == nil {
		t.Fatalf("expected synthetic source with registry")
	}
	if routes[0].Source.Type == nil {
		t.Fatalf("expected synthetic root type")
	}
}

func TestDiscover_ExcludePattern(t *testing.T) {
	baseDir := t.TempDir()
	writeFile(t, filepath.Join(baseDir, "go.mod"), "module example.com/app\n\ngo 1.24\n")
	writeFile(t, filepath.Join(baseDir, "pkg", "one", "one.go"), "package one\ntype In struct{}\ntype Out struct{}\ntype Router struct { Route struct{} `component:\",path=/one,method=GET,input=In,output=Out\"` }\n")
	writeFile(t, filepath.Join(baseDir, "pkg", "two", "two.go"), "package two\ntype In struct{}\ntype Out struct{}\ntype Router struct { Route struct{} `component:\",path=/two,method=GET,input=In,output=Out\"` }\n")

	routes, err := Discover(context.Background(), baseDir, []string{"example.com/app/pkg/..."}, []string{"example.com/app/pkg/two"})
	if err != nil {
		t.Fatalf("discover failed: %v", err)
	}
	if len(routes) != 1 {
		t.Fatalf("unexpected route count: %d", len(routes))
	}
	if routes[0].RoutePath != "/one" {
		t.Fatalf("unexpected route path: %s", routes[0].RoutePath)
	}
}

func TestDiscover_WildcardIncludesVendorSubtree(t *testing.T) {
	baseDir := t.TempDir()
	writeFile(t, filepath.Join(baseDir, "go.mod"), "module example.com/app\n\ngo 1.24\n")
	writeFile(t, filepath.Join(baseDir, "shape", "dev", "vendor", "list", "vendor.go"), `package list

type VendorInput struct {
	ID int `+"`"+`parameter:",kind=path,in=id"`+"`"+`
}

type VendorOutput struct {
}

type VendorRouter struct {
	Vendor struct{} `+"`"+`component:",path=/v1/vendors/{id},method=GET,input=VendorInput,output=VendorOutput"`+"`"+`
}
`)
	writeFile(t, filepath.Join(baseDir, "shape", "dev", "team", "delete", "team.go"), `package delete

type TeamInput struct {
	ID int `+"`"+`parameter:",kind=path,in=id"`+"`"+`
}

type TeamOutput struct{}

type TeamRouter struct {
	Team struct{} `+"`"+`component:",path=/v1/team/{id},method=DELETE,input=TeamInput,output=TeamOutput"`+"`"+`
}
`)

	routes, err := Discover(context.Background(), baseDir, []string{"example.com/app/shape/dev/..."}, nil)
	if err != nil {
		t.Fatalf("discover failed: %v", err)
	}
	if len(routes) != 2 {
		t.Fatalf("unexpected route count: %d", len(routes))
	}
	foundVendor := false
	for _, route := range routes {
		if route != nil && route.PackagePath == "example.com/app/shape/dev/vendor/list" {
			foundVendor = true
			break
		}
	}
	if !foundVendor {
		t.Fatalf("expected vendor subtree package to be discovered, got %#v", routes)
	}
}

func TestDiscover_ResolvesImportedEmbeddedOutputType(t *testing.T) {
	extension.InitRegistry()
	baseDir := t.TempDir()
	writeFile(t, filepath.Join(baseDir, "go.mod"), "module example.com/app\n\ngo 1.24\n")
	writeFile(t, filepath.Join(baseDir, "pkg", "routes", "routes.go"), `package routes

import "github.com/viant/xdatly/handler/response"

type ReportInput struct {
	ID int `+"`"+`parameter:",kind=path,in=id"`+"`"+`
}

type ReportOutput struct {
	response.Status `+"`"+`parameter:",kind=output,in=status"`+"`"+`
}

type Router struct {
	Report struct{} `+"`"+`component:",path=/v1/report/{id},method=GET,input=ReportInput,output=ReportOutput"`+"`"+`
}
`)

	routes, err := Discover(context.Background(), baseDir, []string{"example.com/app/pkg/..."}, nil)
	if err != nil {
		t.Fatalf("discover failed: %v", err)
	}
	if len(routes) != 1 {
		t.Fatalf("unexpected route count: %d", len(routes))
	}
	if routes[0].OutputRef != "example.com/app/pkg/routes.ReportOutput" {
		t.Fatalf("unexpected output ref: %s", routes[0].OutputRef)
	}
}

func TestDiscover_NormalizesHandlerType(t *testing.T) {
	baseDir := t.TempDir()
	writeFile(t, filepath.Join(baseDir, "go.mod"), "module example.com/app\n\ngo 1.24\n")
	writeFile(t, filepath.Join(baseDir, "pkg", "routes", "routes.go"), `package routes

import (
	"context"
	xhandler "github.com/viant/xdatly/handler"
)

type ReportInput struct {
	ID int `+"`"+`parameter:",kind=path,in=id"`+"`"+`
}

type ReportOutput struct {
	OK bool `+"`"+`parameter:",kind=output,in=view"`+"`"+`
}

type Handler struct{}

func (h *Handler) Exec(ctx context.Context, sess xhandler.Session) (interface{}, error) {
	return ReportOutput{OK: true}, nil
}

type Router struct {
	Report struct{} `+"`"+`component:",path=/v1/report/{id},method=GET,input=ReportInput,output=ReportOutput,handler=Handler"`+"`"+`
}
`)

	routes, err := Discover(context.Background(), baseDir, []string{"example.com/app/pkg/..."}, nil)
	if err != nil {
		t.Fatalf("discover failed: %v", err)
	}
	if len(routes) != 1 {
		t.Fatalf("unexpected route count: %d", len(routes))
	}
	tag := routes[0].Source.Type.Field(0).Tag.Get("component")
	if tag == "" || filepath.Base(routes[0].PackagePath) == "" {
		t.Fatalf("expected route component tag to be present")
	}
	if got := routes[0].Source.Type.Field(0).Tag.Get("component"); !strings.Contains(got, "handler=example.com/app/pkg/routes.Handler") {
		t.Fatalf("expected normalized handler ref in component tag, got %q", got)
	}
}

func TestDiscover_PrefersLinkedNamedType(t *testing.T) {
	extension.InitRegistry()
	type linkedReportView struct {
		ID   int    `sqlx:"ID"`
		Name string `sqlx:"NAME"`
	}
	if err := extension.Config.Types.Register("ReportView",
		xreflect.WithPackage("example.com/app/pkg/routes"),
		xreflect.WithReflectType(reflect.TypeOf(linkedReportView{})),
	); err != nil {
		t.Fatalf("register linked type: %v", err)
	}

	baseDir := t.TempDir()
	writeFile(t, filepath.Join(baseDir, "go.mod"), "module example.com/app\n\ngo 1.24\n")
	writeFile(t, filepath.Join(baseDir, "pkg", "routes", "routes.go"), `package routes

type ReportView struct {
	Items []*struct {
		ID int `+"`"+`sqlx:"ID"`+"`"+`
	} `+"`"+`view:",table=ITEM"`+"`"+`
}

type ReportInput struct{}

type ReportOutput struct {
	Data *ReportView `+"`"+`parameter:",kind=output,in=view"`+"`"+`
}

type Router struct {
	Report struct{} `+"`"+`component:",path=/v1/report,method=GET,input=ReportInput,output=ReportOutput,view=ReportView"`+"`"+`
}
`)

	routes, err := Discover(context.Background(), baseDir, []string{"example.com/app/pkg/..."}, nil)
	if err != nil {
		t.Fatalf("discover failed: %v", err)
	}
	if len(routes) != 1 {
		t.Fatalf("unexpected route count: %d", len(routes))
	}
	lookup := routes[0].Source.TypeRegistry.Lookup("example.com/app/pkg/routes.ReportView")
	if lookup == nil || lookup.Type == nil {
		t.Fatalf("expected route view to be registered")
	}
	if got, want := lookup.Type, reflect.TypeOf(linkedReportView{}); got != want {
		t.Fatalf("expected linked route view type %v, got %v", want, got)
	}
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir failed: %v", err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write failed: %v", err)
	}
}
