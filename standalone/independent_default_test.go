package standalone

import (
	"context"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/standalone/config"
	fixture "github.com/viant/datly/standalone/testdata/app"
	"github.com/viant/datly/standalone/testdata/independentdefault"
	"testing"
)

func TestIndependentOrchestratorDoesNotInheritInstanceConnector(t *testing.T) {
	ctx := context.Background()
	f := fixture.New(t)
	cfg, err := (config.Loader{}).Load(ctx, f.Config)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Connector = "main"
	cfg.GoBootstrap.Packages = []string{independentdefault.LinkedType.PkgPath()}
	cfg.GoBootstrap.LinkedOnly = true
	server, err := New(ctx, Options{Config: cfg, RequireLinked: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Shutdown(context.Background()) })
	if err = server.Reload(ctx, 1); err != nil {
		t.Fatal(err)
	}
	value, err := server.InvokeComponent(ctx, dexec.ComponentRequest{Target: dexec.ComponentTarget{Component: spec.Key{Kind: spec.KindComponent, Scope: independentdefault.LinkedType.PkgPath(), Name: "Run"}, Route: spec.RouteRef{Method: "POST", Path: "/independent-default"}}, Input: &independentdefault.Input{}})
	if err != nil {
		t.Fatal(err)
	}
	out, ok := value.(*independentdefault.Output)
	if !ok || !out.Ready {
		t.Fatalf("orchestrator output %T %+v", value, value)
	}
}
