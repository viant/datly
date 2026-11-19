package contract

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/bindly"
	xcontract "github.com/viant/xdatly/contract"
)

type EmbeddedStable struct{ Public string }

type embeddedRoot struct {
	*EmbeddedStable
	Secret string
}

func (*embeddedRoot) Contract(context.Context, bindly.Binder) (*xcontract.Policy, error) {
	return &xcontract.Policy{Name: "hide-secret", Field: func(_ string, _ reflect.Type, f *xcontract.Field) (*xcontract.Field, error) {
		if f.Name == "Secret" {
			return f.Clone().Hide(), nil
		}
		return nil, nil
	}}, nil
}

func TestEffectiveContractPreservesUnchangedEmbeddedHolder(t *testing.T) {
	original := reflect.TypeFor[embeddedRoot]()
	plan, err := New(original, 4)
	if err != nil {
		t.Fatal(err)
	}
	binder, _ := bindly.NewInjector()
	effective, err := plan.Resolve(t.Context(), binder)
	if err != nil {
		t.Fatal(err)
	}
	holder := effective.Field(0)
	if !holder.Anonymous || holder.Type != original.Field(0).Type || holder.Offset != original.Field(0).Offset {
		t.Fatalf("embedded holder changed: %+v", holder)
	}
	if effective.Field(1).Tag.Get("json") != "-" {
		t.Fatal("sibling policy was not applied")
	}
}

type EmbeddedWithMethods struct{ Public string }

func (*EmbeddedWithMethods) Identity() string { return "linked" }

type embeddedMethodRoot struct {
	*EmbeddedWithMethods
	Secret string
}

func (*embeddedMethodRoot) Contract(ctx context.Context, b bindly.Binder) (*xcontract.Policy, error) {
	return (&embeddedRoot{}).Contract(ctx, b)
}
func TestEmbeddedMethodReflectionRestrictionIsExplicit(t *testing.T) {
	binder, _ := bindly.NewInjector()
	plan, err := New(reflect.TypeFor[embeddedMethodRoot](), 4)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = plan.Resolve(t.Context(), binder); err == nil || !strings.Contains(err.Error(), "embedded type with methods") {
		t.Fatalf("expected native reflection restriction, got %v", err)
	}
	// Without changes the linked type and its promoted methods need no reflection rebuild.
	static, err := New(reflect.TypeFor[struct {
		*EmbeddedWithMethods
		Other string
	}](), 4)
	if err != nil {
		t.Fatal(err)
	}
	actual, err := static.Resolve(t.Context(), nil)
	if err != nil || actual != static.Original() {
		t.Fatal("linked type changed", err)
	}
	if _, ok := actual.MethodByName("Identity"); !ok {
		t.Fatal("promoted method lost")
	}
}

func TestEmbeddedMethodContractResolvesOptInMetadata(t *testing.T) {
	binder, _ := bindly.NewInjector()
	original := reflect.TypeFor[embeddedMethodRoot]()
	plan, err := New(original, 4)
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := plan.ResolveRepresentation(t.Context(), binder)
	if err != nil {
		t.Fatal(err)
	}
	if metadata == nil || metadata.Original() != original || metadata.Field(1).Tag.Get("json") != "-" {
		t.Fatal("effective metadata was not resolved")
	}
	if !metadata.Field(0).Anonymous || metadata.Field(0).Type != original.Field(0).Type {
		t.Fatal("linked embedding changed")
	}
	again, err := plan.ResolveRepresentation(t.Context(), binder)
	if err != nil || again != metadata {
		t.Fatal("immutable variant not reused", err)
	}
	static, err := New(reflect.TypeFor[struct{ Value string }](), 4)
	if err != nil {
		t.Fatal(err)
	}
	unchanged, err := static.ResolveRepresentation(t.Context(), nil)
	if err != nil || unchanged != nil {
		t.Fatal("no-op unexpectedly opted into metadata", err)
	}
}
