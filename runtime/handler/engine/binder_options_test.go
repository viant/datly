package engine

import (
	"context"
	"testing"

	"github.com/viant/bindly"
	"github.com/viant/bindly/locator/buildin"
	rhandler "github.com/viant/datly/runtime/handler"
)

func TestLifecycleBinderForwardsOptionsWithinLease(t *testing.T) {
	type source struct{ Value string }
	type target struct {
		Value string `bind:"kind=input,in=Value"`
	}
	injector, err := bindly.NewInjector(bindly.WithProviders(buildin.Struct("input", "", 1)))
	if err != nil {
		t.Fatal(err)
	}
	data := &dataScope{}
	defer data.releaseContexts()
	lease := &lifecycleScope{ctx: context.Background(), data: data, phase: "test"}
	binder := &lifecycleBinder{scope: lease, delegate: rhandler.NewBinder(injector, &source{Value: "canonical"})}
	var dst target
	if err = binder.Bind(context.Background(), &dst, bindly.WithSource(&source{Value: "explicit"})); err != nil || dst.Value != "explicit" {
		t.Fatalf("%+v %v", dst, err)
	}
	if err = lease.finish(func(context.Context) error { return nil }, nil); err != nil {
		t.Fatal(err)
	}
	dst.Value = ""
	if err = binder.Bind(context.Background(), &dst, bindly.WithSource(&source{Value: "after close"})); err == nil || dst.Value != "" {
		t.Fatal("options bypassed lease", err)
	}
}
