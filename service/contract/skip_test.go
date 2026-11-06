package contract

import (
	"context"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/viant/bindly"
	xcontract "github.com/viant/xdatly/contract"
)

type skipPlain struct{ Secret string }
type skipNested struct{ Secret string }

var skippedParentVisits atomic.Int64
var nestedSkipHooks atomic.Int64

func (*skipNested) Contract(context.Context, bindly.Binder) (*xcontract.Policy, error) {
	nestedSkipHooks.Add(1)
	return &xcontract.Policy{Name: "nested", Field: func(_ string, _ reflect.Type, f *xcontract.Field) (*xcontract.Field, error) {
		if f.Name == "Secret" {
			return f.Hide(), nil
		}
		return nil, nil
	}}, nil
}

type skipRoot struct {
	Plain *skipPlain
	Child *skipNested
	Other *skipPlain
}

func (*skipRoot) Contract(context.Context, bindly.Binder) (*xcontract.Policy, error) {
	return &xcontract.Policy{Name: "root", Field: func(path string, parent reflect.Type, f *xcontract.Field) (*xcontract.Field, error) {
		if parent == reflect.TypeFor[skipRoot]() {
			switch f.Name {
			case "Plain", "Child":
				return f.SkipChildren(), nil
			}
		}
		if strings.HasPrefix(path, "Plain") || strings.HasPrefix(path, "Child") {
			skippedParentVisits.Add(1)
		}
		if f.Name == "Secret" {
			return f.Hide(), nil
		}
		return nil, nil
	}}, nil
}
func TestSkipChildrenIsPolicyLocalAndRetainsNestedHooks(t *testing.T) {
	skippedParentVisits.Store(0)
	nestedSkipHooks.Store(0)
	plan, err := New(reflect.TypeFor[skipRoot](), 8)
	if err != nil {
		t.Fatal(err)
	}
	base, _ := plan.walk.RewriteRepresentation(nil)
	binder, _ := bindly.NewInjector()
	actual, err := plan.ResolveRepresentation(t.Context(), binder)
	if err != nil {
		t.Fatal(err)
	}
	if skippedParentVisits.Load() != 0 || nestedSkipHooks.Load() != 1 {
		t.Fatalf("parent visits=%d nested hooks=%d", skippedParentVisits.Load(), nestedSkipHooks.Load())
	}
	if actual.FieldShape(0) != base.FieldShape(0) {
		t.Fatal("skip-only subtree lost linked identity")
	}
	for _, index := range []int{1, 2} {
		if actual.FieldShape(index).Elem().Field(0).Tag.Get("json") != "-" {
			t.Fatal("nested or sibling policy suppressed")
		}
	}
	if again, err := plan.ResolveRepresentation(t.Context(), binder); err != nil || again != actual || nestedSkipHooks.Load() != 2 {
		t.Fatal("cached schema bypassed current nested policy selection", err)
	}
}

type skipOnly struct{ Rows []*skipPlain }

func (*skipOnly) Contract(context.Context, bindly.Binder) (*xcontract.Policy, error) {
	return &xcontract.Policy{Name: "skip-only", Field: func(_ string, parent reflect.Type, f *xcontract.Field) (*xcontract.Field, error) {
		if parent == reflect.TypeFor[skipOnly]() {
			return f.SkipChildren(), nil
		}
		panic("skipped descendant callback")
	}}, nil
}
func TestSkipOnlyRetainsOriginalContract(t *testing.T) {
	plan, _ := New(reflect.TypeFor[skipOnly](), 8)
	binder, _ := bindly.NewInjector()
	metadata, err := plan.ResolveRepresentation(t.Context(), binder)
	if err != nil || metadata != nil {
		t.Fatal("skip became a schema edit", err)
	}
	original, err := plan.Resolve(t.Context(), binder)
	if err != nil || original != reflect.TypeFor[skipOnly]() {
		t.Fatal("linked original changed", err)
	}
}

type skipMiddle struct{ Leaf *skipPlain }

func (*skipMiddle) Contract(context.Context, bindly.Binder) (*xcontract.Policy, error) {
	return &xcontract.Policy{Name: "middle", Field: func(_ string, parent reflect.Type, f *xcontract.Field) (*xcontract.Field, error) {
		if parent == reflect.TypeFor[skipMiddle]() {
			return f.SkipChildren(), nil
		}
		panic("middle policy entered skipped child")
	}}, nil
}

type skipOuter struct{ Middle *skipMiddle }

func (*skipOuter) Contract(context.Context, bindly.Binder) (*xcontract.Policy, error) {
	return &xcontract.Policy{Name: "outer", Field: func(_ string, _ reflect.Type, f *xcontract.Field) (*xcontract.Field, error) {
		if f.ChildrenSkipped() {
			panic("nested policy traversal flag leaked to parent")
		}
		if f.Name == "Secret" {
			return f.Hide(), nil
		}
		return f.Clone(), nil
	}}, nil
}
func TestNestedSkipDoesNotSuppressEnclosingPolicy(t *testing.T) {
	plan, _ := New(reflect.TypeFor[skipOuter](), 8)
	binder, _ := bindly.NewInjector()
	metadata, err := plan.ResolveRepresentation(t.Context(), binder)
	if err != nil {
		t.Fatal(err)
	}
	leaf := metadata.FieldShape(0).Elem().FieldShape(0).Elem()
	if leaf.Field(0).Tag.Get("json") != "-" {
		t.Fatal("nested skip suppressed parent policy")
	}
}
