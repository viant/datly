package standalone

import (
	"context"
	"testing"

	"github.com/viant/datly/application"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/standalone/config"
	fixture "github.com/viant/datly/standalone/testdata/app"
	"github.com/viant/datly/standalone/testdata/app/records"
	"github.com/viant/datly/typecatalog"
)

func TestCompileLinkedComponentsIntoAnotherGeneration(t *testing.T) {
	ctx := context.Background()
	f := fixture.New(t)
	cfg, err := (config.Loader{}).Load(ctx, f.Config)
	if err != nil {
		t.Fatal(err)
	}
	cfg.GoBootstrap.Packages = []string{fixture.Module + "/records"}
	exports, err := records.Exports()
	if err != nil {
		t.Fatal(err)
	}
	build, err := Compile(ctx, Options{Config: cfg, Registry: exports})
	if err != nil {
		t.Fatal(err)
	}
	if len(build.Components) == 0 || build.Index != nil {
		t.Fatal("expected eager linked registrations")
	}
	manager, err := application.New(build.Types)
	if err != nil {
		_ = build.Shutdown(ctx)
		t.Fatal(err)
	}
	defer manager.Shutdown(ctx)
	if err = manager.Reload(ctx, application.Request{Revision: 1, Compile: func(context.Context, *typecatalog.Catalog) (*application.Build, error) { return build, nil }}); err != nil {
		t.Fatal(err)
	}
	result, err := manager.InvokeComponent(ctx, dexec.ComponentRequest{Target: dexec.ComponentTarget{Component: spec.Key{Kind: spec.KindComponent, Scope: fixture.Module + "/records", Name: "Read"}, Route: spec.RouteRef{Method: "GET", Path: "/records/{id}"}}, Input: &records.ReadInput{ID: 1}})
	if err != nil {
		t.Fatal(err)
	}
	output := result.(*records.ReadOutput)
	if len(output.Rows) != 1 || output.Rows[0].Name != "first" {
		t.Fatalf("unexpected compiled read: %+v", output)
	}
	if err = manager.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err = build.Shutdown(ctx); err != nil {
		t.Fatalf("repeat shutdown: %v", err)
	}
}
