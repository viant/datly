package engine

import (
	"context"
	"reflect"
	"testing"

	"github.com/viant/datly/internal/drainowner"
	"github.com/viant/datly/internal/testharness"
	rh "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/sql/dml"
	xh "github.com/viant/xdatly/handler"
)

type boundDMLForwarder struct{ xh.DML }

func TestBoundDMLExactActualEngineScope(t *testing.T) {
	db := testharness.NewSQLiteHarness(t)
	source := dml.Source{DB: db.DB}
	var childCtx context.Context
	var childCapability xh.DML
	var binding *drainowner.GuardBinding
	_, err := New().Execute(context.Background(), Request{Input: testRouteInput(t, reflect.TypeFor[struct{}]()), DataSource: source,
		Handler: rh.HandlerFunc(func(ctx context.Context, inv rh.Invocation) (any, error) {
			value, _, err := inv.Binder.Lookup(ctx, xh.DMLKey)
			if err != nil {
				return nil, err
			}
			capability := value.(xh.DML)
			scope := ctx.Value(dataScopeContextKey{}).(*dataScope)
			binding = drainowner.NewGuardBinding()
			calls := 0
			if err = scope.registerExecutionGuard(ctx, func(context.Context) error { calls++; return nil }, binding); err != nil {
				return nil, err
			}
			if err = ValidateBoundDML(ctx, capability, binding); err != nil {
				t.Fatal("focused native capability", err)
			}
			foreign := dml.NewData(db.DB)
			q := capability.(queueContractDMLCapability)
			wrongQueue := q
			wrongQueue.queue = foreign
			wrongService := q
			wrongService.service = foreign
			wrongGuard := q
			wrongGuard.guard = &mutationGuard{}
			for _, invalid := range []xh.DML{foreign, &boundDMLForwarder{capability}, wrongQueue, wrongService, wrongGuard} {
				if e := ValidateBoundDML(ctx, invalid, binding); e == nil {
					t.Fatal("foreign/forwarded/mixed capability accepted")
				}
			}
			if e := ValidateBoundDML(context.Background(), capability, binding); e == nil {
				t.Fatal("replaced context accepted")
			}
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			if e := ValidateBoundDML(canceled, capability, binding); e == nil {
				t.Fatal("canceled context accepted")
			}
			if calls != 0 {
				t.Fatal("association invoked callback")
			}
			_, err = New().Execute(PrepareComponent(ctx, ComponentBufferedImperative, ""), Request{Input: testRouteInput(t, reflect.TypeFor[struct{}]()), DataSource: source,
				Handler: rh.HandlerFunc(func(nested context.Context, child rh.Invocation) (any, error) {
					v, _, e := child.Binder.Lookup(nested, xh.DMLKey)
					if e != nil {
						return nil, e
					}
					childCtx, childCapability = nested, v.(xh.DML)
					if e = ValidateBoundDML(nested, childCapability, binding); e != nil {
						t.Fatal("nested view rejected", e)
					}
					if e = ValidateBoundDML(nested, capability, binding); e == nil {
						t.Fatal("same-owner ancestor accepted for child")
					}
					if e = ValidateBoundDML(ctx, childCapability, binding); e == nil {
						t.Fatal("same-owner child accepted for ancestor")
					}
					return nil, nil
				})})
			if err != nil {
				return nil, err
			}
			if e := ValidateBoundDML(childCtx, childCapability, binding); e == nil {
				t.Fatal("sealed child view accepted")
			}
			return nil, nil
		})})
	if err != nil {
		t.Fatal(err)
	}
	if e := ValidateBoundDML(childCtx, childCapability, binding); e == nil {
		t.Fatal("completed context accepted")
	}
}

func TestBoundDMLDoesNotResolveAnOwner(t *testing.T) {
	scope := newDataScope(independentSource{}) // Open panics if called.
	ctx := withDataScope(context.Background(), scope)
	if err := ValidateBoundDML(ctx, dmlCapability{guard: scope.mutationGuard()}, drainowner.NewGuardBinding()); err == nil {
		t.Fatal("unresolved scope accepted")
	}
	if scope.data != nil {
		t.Fatal("validation resolved an owner")
	}
}

type blockedAssociationSource struct {
	data             *dml.Data
	entered, release chan struct{}
}

func (s *blockedAssociationSource) Open(context.Context) (xh.Data, error) {
	close(s.entered)
	<-s.release
	return s.data, nil
}
func TestBoundDMLWaitsForFullResolutionPublication(t *testing.T) {
	native := dml.NewData(testharness.NewSQLiteHarness(t).DB)
	source := &blockedAssociationSource{data: native, entered: make(chan struct{}), release: make(chan struct{})}
	scope := newDataScope(source)
	ctx := withDataScope(context.Background(), scope)
	capability := newQueueAwareDMLCapability(native, scope.mutationGuard())
	binding := drainowner.NewGuardBinding()
	done := make(chan error, 1)
	go func() { _, err := scope.resolve(ctx); done <- err }()
	<-source.entered
	for i := 0; i < 100; i++ {
		if err := ValidateBoundDML(ctx, capability, binding); err == nil {
			t.Fatal("resolving owner accepted")
		}
	}
	close(source.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := scope.registerExecutionGuard(ctx, func(context.Context) error { return nil }, binding); err != nil {
		t.Fatal(err)
	}
	if err := ValidateBoundDML(ctx, capability, binding); err != nil {
		t.Fatal("fully resolved owner rejected", err)
	}
}
