package runtime

import (
	"context"
	"testing"

	"github.com/viant/bindly"
	"github.com/viant/bindly/locator/buildin"
	rhandler "github.com/viant/datly/runtime/handler"
)

func TestComponentResultBinderForwardsOptionsWithoutChangingDefault(t *testing.T) {
	type result struct{ Value string }
	type target struct {
		Value string `bind:"kind=input,in=Value"`
	}
	injector, err := bindly.NewInjector(bindly.WithProviders(buildin.Struct("input", "", 1)))
	if err != nil {
		t.Fatal(err)
	}
	child := &result{Value: "child"}
	binder := &componentBinder{invoked: true, result: child, binder: rhandler.NewBinder(injector, child)}
	var explicit, normal target
	if err = binder.Bind(context.Background(), &explicit, bindly.WithSource(&result{Value: "explicit"})); err != nil || explicit.Value != "explicit" {
		t.Fatalf("%+v %v", explicit, err)
	}
	if err = binder.Bind(context.Background(), &normal); err != nil || normal.Value != "child" {
		t.Fatalf("%+v %v", normal, err)
	}
}
