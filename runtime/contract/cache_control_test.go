package contract

import (
	"context"
	"github.com/viant/bindly"
	xcontract "github.com/viant/xdatly/contract"
	"reflect"
	"testing"
)

type cacheControlState struct {
	Name, Description string
	NoCache           bool
}
type cacheControlBinder cacheControlState

func (b cacheControlBinder) Bind(_ context.Context, target any, _ ...bindly.BindOption) error {
	*target.(*cacheControlState) = cacheControlState(b)
	return nil
}

type cacheControlRecord struct{ Value string }

func (*cacheControlRecord) Contract(ctx context.Context, binder bindly.Binder) (*xcontract.Policy, error) {
	var state cacheControlState
	if err := binder.Bind(ctx, &state); err != nil {
		return nil, err
	}
	return &xcontract.Policy{Name: state.Name, NoCache: state.NoCache, Field: func(_ string, _ reflect.Type, f *xcontract.Field) (*xcontract.Field, error) {
		return f.Clone().SetDescription(state.Description), nil
	}}, nil
}
func TestPolicyNoCacheBypassesNamedVariant(t *testing.T) {
	plan, err := New(reflect.TypeFor[cacheControlRecord](), 8)
	if err != nil {
		t.Fatal(err)
	}
	cached, err := plan.ResolveRepresentation(t.Context(), cacheControlBinder{Name: "reader", Description: "cached"})
	if err != nil {
		t.Fatal(err)
	}
	for _, description := range []string{"fresh-1", "fresh-2"} {
		metadata, err := plan.ResolveRepresentation(t.Context(), cacheControlBinder{Name: "reader", NoCache: true, Description: description})
		if err != nil || metadata == cached || metadata.Field(0).Tag.Get("description") != description {
			t.Fatalf("cache bypass failed: %v", err)
		}
	}
	again, err := plan.ResolveRepresentation(t.Context(), cacheControlBinder{Name: "reader", Description: "cached"})
	if err != nil || again != cached {
		t.Fatal("bypass replaced the retained named variant", err)
	}
	if len(plan.variants) != 1 {
		t.Fatal("uncached variants retained")
	}
	for _, description := range []string{"anonymous-1", "anonymous-2"} {
		metadata, err := plan.ResolveRepresentation(t.Context(), cacheControlBinder{Description: description})
		if err != nil || metadata.Field(0).Tag.Get("description") != description {
			t.Fatal("empty name reused a variant", err)
		}
	}
	if len(plan.variants) != 1 {
		t.Fatal("anonymous variants retained")
	}
}

type cacheControlParent struct{ Child cacheControlRecord }

func (*cacheControlParent) Contract(context.Context, bindly.Binder) (*xcontract.Policy, error) {
	return &xcontract.Policy{NoCache: true}, nil
}
func TestParentNoCacheDisablesComposedVariantReuse(t *testing.T) {
	plan, err := New(reflect.TypeFor[cacheControlParent](), 8)
	if err != nil {
		t.Fatal(err)
	}
	for _, description := range []string{"first", "second"} {
		metadata, err := plan.ResolveRepresentation(t.Context(), cacheControlBinder{Name: "child", Description: description})
		if err != nil {
			t.Fatal(err)
		}
		child, err := metadata.Lookup("Child")
		if err != nil {
			t.Fatal(err)
		}
		if child.Field(0).Tag.Get("description") != description {
			t.Fatal("parent bypass ignored")
		}
	}
	if len(plan.variants) != 0 {
		t.Fatal("composed variant retained despite parent bypass")
	}
}
