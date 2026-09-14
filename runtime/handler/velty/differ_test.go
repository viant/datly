package velty

import (
	"context"
	"github.com/viant/bindly"
	rdiffer "github.com/viant/datly/runtime/differ"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/handler/provider"
	xdiffer "github.com/viant/xdatly/differ"
	xhandler "github.com/viant/xdatly/handler"
	"testing"
)

func TestVeltyScopedTypedDifference(t *testing.T) {
	type marker struct {
		Count bool
		Other bool
	}
	type record struct {
		Count int
		Other int
		Has   *marker `setMarker:"true"`
	}
	type input struct{ Before, After *record }
	type output struct{ Changes *xdiffer.ChangeLog }
	h, err := New[input, output](Config{Template: `#set($Output.Changes = $differ.Diff($Input.Before, $Input.After))`})
	if err != nil {
		t.Fatal(err)
	}
	in := &input{Before: &record{Count: 5, Other: 9, Has: &marker{Count: true}}, After: &record{Count: 0, Other: 0, Has: &marker{Count: true}}}
	injector, err := bindly.NewInjector(bindly.WithProviders(provider.Capabilities(xhandler.Capabilities{Differ: rdiffer.New()})...))
	if err != nil {
		t.Fatal(err)
	}
	result, err := h.Execute(context.Background(), rhandler.Invocation{Input: in, Binder: rhandler.NewBinder(injector, in)})
	if err != nil {
		t.Fatal(err)
	}
	got := result.(*output).Changes
	if got == nil || len(got.Changes) != 1 || got.Changes[0].Path.String() != "Count" || got.Changes[0].To != 0 {
		t.Fatalf("result=%+v", got)
	}
	empty, err := bindly.NewInjector()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Execute(context.Background(), rhandler.Invocation{Input: in, Binder: rhandler.NewBinder(empty, in)}); err == nil {
		t.Fatal("missing capability accepted")
	}
}
