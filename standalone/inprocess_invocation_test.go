package standalone

import (
	"context"
	"testing"

	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/standalone/config"
	fixture "github.com/viant/datly/standalone/testdata/app"
	"github.com/viant/datly/standalone/testdata/app/records"
)

func TestStandaloneInvokesLinkedComponentWithoutListener(t *testing.T) {
	ctx := context.Background()
	f := fixture.New(t)
	cfg, err := (config.Loader{}).Load(ctx, f.Config)
	if err != nil {
		t.Fatal(err)
	}
	exports, err := records.Exports()
	if err != nil {
		t.Fatal(err)
	}
	server, err := New(ctx, Options{Config: cfg, Registry: exports})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Shutdown(context.Background()) })
	if err := server.Reload(ctx, 1); err != nil {
		t.Fatal(err)
	}
	value, err := server.InvokeComponent(ctx, dexec.ComponentRequest{Target: dexec.ComponentTarget{
		Component: spec.Key{Kind: spec.KindComponent, Scope: fixture.Module + "/records", Name: "Read"},
		Route:     spec.RouteRef{Method: "GET", Path: "/records/{id}"},
	}, Input: &records.ReadInput{ID: 1}})
	if err != nil {
		t.Fatal(err)
	}
	output, ok := value.(*records.ReadOutput)
	if !ok || len(output.Rows) != 1 || output.Rows[0].ID != 1 || output.Rows[0].Name != "first" {
		t.Fatalf("in-process read=%T %+v", value, value)
	}
	if len(server.listeners) != 0 {
		t.Fatal("in-process invocation unexpectedly opened a listener")
	}
}
