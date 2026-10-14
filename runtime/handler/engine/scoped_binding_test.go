package engine

import (
	"context"
	"errors"
	"fmt"
	modernresponse "github.com/viant/xdatly/response"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/viant/bindly"
	"github.com/viant/bindly/locator"
	bindstate "github.com/viant/bindly/state"
	"github.com/viant/datly/runtime/handler/custom"
	handlerprovider "github.com/viant/datly/runtime/handler/provider"
	sqldml "github.com/viant/datly/sql/dml"
	xhandler "github.com/viant/xdatly/handler"
	xlogger "github.com/viant/xdatly/logger"
)

type scopedInput struct {
	Credential *string
	Child      *string
	Has        *struct{ Credential, Child bool } `setMarker:"true"`
}
type scopedOutput struct{ Value string }
type scopedContract struct {
	run     func(context.Context, xhandler.Session, *scopedInput, *scopedOutput) error
	samples atomic.Int32
}

func (c *scopedContract) BindsInputByScope() bool { c.samples.Add(1); return true }
func (c *scopedContract) Exec(ctx context.Context, s xhandler.Session, i *scopedInput, o *scopedOutput) error {
	return c.run(ctx, s, i, o)
}

func scopedRequest(t *testing.T, c *scopedContract, credential string, order *[]string, childErr error) Request {
	required := true
	route := testRouteInput(t, reflect.TypeFor[scopedInput](),
		bindly.BindingSpec{Path: "Credential", Name: "Credential", Location: bindstate.Location{Kind: "credential"}, Required: &required, ErrorCode: 401},
		bindly.BindingSpec{Path: "Child", Name: "Child", Scope: "lazy", Location: bindstate.Location{Kind: "child"}})
	return Request{Input: route, Handler: custom.New(c), OutputType: reflect.TypeFor[scopedOutput](), Providers: []locator.Provider{
		handlerprovider.New("credential", func(context.Context) (any, bool, error) {
			*order = append(*order, "default")
			if credential == "" {
				return nil, false, nil
			}
			return credential, true, nil
		}),
		handlerprovider.New("child", func(context.Context) (any, bool, error) {
			*order = append(*order, "child")
			return "resolved", childErr == nil, childErr
		}),
	}}
}
func TestScopedBindingCanonicalOrderAndLease(t *testing.T) {
	var order []string
	var retained xhandler.ScopedBinder
	var canonical *scopedInput
	c := &scopedContract{run: func(ctx context.Context, s xhandler.Session, i *scopedInput, o *scopedOutput) error {
		order = append(order, "begin")
		canonical = i
		if i.Has == nil || i.Credential == nil || i.Child != nil || !i.Has.Credential || i.Has.Child {
			t.Fatal("default prebinding missing or lazy binding eager")
		}
		b, ok := s.Binder().(xhandler.ScopedBinder)
		if !ok {
			t.Fatal("scoped binder missing")
		}
		retained = b
		value, found, err := b.Lookup(ctx, xhandler.InputKey)
		if err != nil || !found || value != i {
			t.Fatalf("canonical lookup %v %v %v", value, found, err)
		}
		if value, found, _ := b.Lookup(ctx, xhandler.InputSnapshotKey); found || value != nil {
			t.Fatal("fictitious snapshot")
		}
		for _, target := range []any{nil, (*scopedInput)(nil), &scopedInput{}, []int{1}} {
			if err := b.Bind(ctx, target); err == nil {
				t.Fatalf("accepted target %T", target)
			}
		}
		if err := b.BindScope(ctx, i, "Lazy"); err == nil {
			t.Fatal("case folded scope")
		}
		if i.Credential == nil || i.Has == nil || !i.Has.Credential || i.Has.Child || i.Child != nil {
			t.Fatalf("default state %+v", i)
		}
		marker := i.Has
		order = append(order, "authorize")
		if err := b.BindScope(ctx, i, "lazy"); err != nil {
			return err
		}
		if i.Has != marker || !i.Has.Child || i.Child == nil {
			t.Fatal("lazy replaced canonical markers")
		}
		o.Value = *i.Child
		order = append(order, "end")
		return nil
	}}
	req := scopedRequest(t, c, "allowed", &order, nil)
	result, err := New().Execute(t.Context(), req)
	if err != nil || result.(*scopedOutput).Value != "resolved" {
		t.Fatalf("result=%v error=%v", result, err)
	}
	if strings.Join(order, ",") != "default,begin,authorize,child,end" {
		t.Fatal(order)
	}
	if canonical == nil || c.samples.Load() != 1 {
		t.Fatal("policy not sampled once")
	}
	if err := retained.Bind(t.Context(), canonical); err == nil {
		t.Fatal("escaped binder")
	}
	if _, _, err := retained.Lookup(t.Context(), xhandler.InputKey); err == nil {
		t.Fatal("escaped lookup")
	}
}
func TestScopedBindingFailuresBeforeLazyAndNativeCause(t *testing.T) {
	sentinel := errors.New("child source failure")
	for _, tc := range []struct {
		name, credential string
		childErr         error
		want             string
	}{{"missing", "", nil, "default"}, {"denied", "denied", nil, "default,begin,authorize"}, {"child", "allowed", sentinel, "default,begin,authorize,child"}} {
		t.Run(tc.name, func(t *testing.T) {
			var order []string
			c := &scopedContract{run: func(ctx context.Context, s xhandler.Session, i *scopedInput, o *scopedOutput) error {
				order = append(order, "begin")
				b := s.Binder().(xhandler.ScopedBinder)
				order = append(order, "authorize")
				if *i.Credential == "denied" {
					return errors.New("denied")
				}
				return b.BindScope(ctx, i, "lazy")
			}}
			_, err := New().Execute(t.Context(), scopedRequest(t, c, tc.credential, &order, tc.childErr))
			if err == nil {
				t.Fatal("expected failure")
			}
			if tc.childErr != nil && !errors.Is(err, sentinel) {
				t.Fatal("lost real child cause")
			}
			if strings.Join(order, ",") != tc.want {
				t.Fatal(order)
			}
		})
	}
}
func TestScopedAdmissionRejectsBeforeProvider(t *testing.T) {
	var order []string
	c := &scopedContract{run: func(context.Context, xhandler.Session, *scopedInput, *scopedOutput) error {
		t.Fatal("executed")
		return nil
	}}
	cases := map[string]func(*Request){"bound": func(r *Request) { r.BoundInput = &scopedInput{} }, "resolved-foreign": func(r *Request) { r.ResolvedInput = &struct{}{} }, "paths": func(r *Request) { r.ResolvedPaths = []string{"Child"} }, "replay": func(r *Request) { r.Replay = &bindly.ReplayBinding{} }, "reader": func(r *Request) { r.CanonicalReader = true }, "sequence": func(r *Request) { r.SequenceStrategy = "x" }, "protected": func(r *Request) { r.ProtectedFlushTables = []string{"rows"} }, "completion": func(r *Request) { r.Completion = func(xhandler.Outcome) {} }, "independent": func(r *Request) { r.IndependentChildTransactions = true }}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			r := scopedRequest(t, c, "allowed", &order, nil)
			change(&r)
			if _, err := New().Execute(t.Context(), r); err == nil {
				t.Fatal("admitted")
			}
			if len(order) != 0 {
				t.Fatal(order)
			}
		})
	}
}
func TestScopedLeaseOverlapCancellationRepeatAndClose(t *testing.T) {
	var order []string
	c := &scopedContract{run: func(context.Context, xhandler.Session, *scopedInput, *scopedOutput) error { return nil }}
	r := scopedRequest(t, c, "allowed", &order, nil)
	if err := r.Input.PrepareScopePlans(); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	var b *scopedBinder
	provider := handlerprovider.New("child", func(ctx context.Context) (any, bool, error) {
		calls.Add(1)
		close(entered)
		if err := b.BindScope(ctx, b.input, "lazy"); err == nil {
			t.Error("reentry accepted")
		}
		<-release
		return "done", true, nil
	})
	injector, _ := bindly.NewInjector(bindly.WithProviders(provider))
	input := &scopedInput{}
	b = newScopedBinder(t.Context(), injector, input, r.Input, nil, nil)
	done := make(chan error, 1)
	go func() { done <- b.BindScope(t.Context(), input, "lazy") }()
	<-entered
	if err := b.BindScope(t.Context(), input, "lazy"); err == nil {
		t.Fatal("overlap accepted")
	}
	closed := make(chan error, 1)
	go func() { closed <- b.close() }()
	// Synchronize on the lease's own state rather than sleeping.
	for {
		b.mu.Lock()
		closing := b.closed
		b.mu.Unlock()
		if closing {
			break
		}
	}
	if _, _, err := b.Lookup(t.Context(), xhandler.InputKey); err == nil {
		t.Fatal("close admitted lookup")
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := <-closed; err == nil {
		t.Fatal("escaped work unreported")
	}
	if calls.Load() != 1 {
		t.Fatal(calls.Load())
	}
	// A new lease supports scoped repetition and an already canceled call.
	n := atomic.Int32{}
	injector, _ = bindly.NewInjector(bindly.WithProviders(handlerprovider.New("child", func(context.Context) (any, bool, error) { n.Add(1); return "ok", true, nil })))
	b = newScopedBinder(t.Context(), injector, &scopedInput{}, r.Input, nil, nil)
	for range 2 {
		if err := b.BindScope(t.Context(), b.input, "lazy"); err != nil {
			t.Fatal(err)
		}
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if err := b.BindScope(canceled, b.input, "lazy"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if n.Load() != 2 {
		t.Fatal(n.Load())
	}
	if err := b.close(); err != nil {
		t.Fatal(err)
	}
}
func TestScopedConcurrentInvocationsSeparateCanonicalInputs(t *testing.T) {
	var seen sync.Map
	var wg sync.WaitGroup
	c := &scopedContract{run: func(ctx context.Context, s xhandler.Session, i *scopedInput, o *scopedOutput) error {
		if _, loaded := seen.LoadOrStore(i, true); loaded {
			t.Error("shared input")
		}
		return nil
	}}
	r := scopedRequest(t, c, "allowed", new([]string), nil)
	// Providers are stateless here; the preceding order fixtures intentionally
	// append only within a single invocation.
	r.Providers = []locator.Provider{handlerprovider.New("credential", func(context.Context) (any, bool, error) { return "ok", true, nil })}
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := New().Execute(t.Context(), r); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if c.samples.Load() != 1 {
		t.Fatal(c.samples.Load())
	}
}

func TestScopedBindingPreservesExistingNativeCallerQueueWithoutDatabase(t *testing.T) {
	for _, credential := range []string{"allowed", ""} {
		t.Run("credential="+credential, func(t *testing.T) {
			// A real native DML owner with no DB handle proves selection cannot execute
			// SQL. Its queued source row and mutation report remain caller-owned.
			data := sqldml.NewData(nil)
			source := &staticDataSource{data: data}
			root, owns := invocationDataScope(t.Context(), source)
			if !owns {
				t.Fatal("root ownership")
			}
			ctx := withDataScope(t.Context(), root)
			if _, err := root.resolve(ctx); err != nil {
				t.Fatal(err)
			}
			defer root.releaseContexts()
			row := &struct {
				ID    int
				Value string
			}{ID: 71, Value: "current source cell"}
			if err := data.Insert("records", row); err != nil {
				t.Fatal(err)
			}
			before := data.MutationReport()
			if before.Queued != 1 || len(before.Results) != 0 {
				t.Fatal(before)
			}
			var order []string
			c := &scopedContract{run: func(ctx context.Context, s xhandler.Session, i *scopedInput, o *scopedOutput) error {
				scoped, _ := ctx.Value(dataScopeContextKey{}).(*dataScope)
				if scoped == nil || scoped.root != root {
					t.Fatal("detached parent root")
				}
				return s.Binder().(xhandler.ScopedBinder).BindScope(ctx, i, "lazy")
			}}
			req := scopedRequest(t, c, credential, &order, nil)
			if credential == "allowed" {
				req.ResolvedInput = &scopedInput{Credential: &credential}
				req.ResolvedPaths = []string{"Credential"}
			}
			req.BufferedComponentCalls = true
			_, err := New().Execute(PrepareComponent(ctx, ComponentBufferedImperative, ""), req)
			if credential == "allowed" && err != nil || credential == "" && err == nil {
				t.Fatal(err)
			}
			after := data.MutationReport()
			if source.opens != 1 || after.Queued != 1 || len(after.Results) != 0 || row.ID != 71 || row.Value != "current source cell" {
				t.Fatalf("caller changed opens%d report%+v row%+v", source.opens, after, row)
			}
			// The only Open above is the scoped parent preparation. Child selection
			// adds no Open, owner, flush, result, row mutation or completion.
		})
	}
}

func TestScopedScopeAdmissionRejectsViewsAndGroups(t *testing.T) {
	for _, kind := range []string{"view", "group"} {
		t.Run(kind, func(t *testing.T) {
			var order []string
			c := &scopedContract{run: func(context.Context, xhandler.Session, *scopedInput, *scopedOutput) error {
				t.Fatal("executed")
				return nil
			}}
			r := scopedRequest(t, c, "allowed", &order, nil)
			binding := bindly.BindingSpec{Path: "Child", Scope: "lazy", Location: bindstate.Location{Kind: "view"}}
			if kind == "group" {
				binding.Location.Kind = "component"
				binding.Location.In = "GET:/records"
				binding.ResolutionGroup = &bindly.ResolutionGroupSpec{Name: "selected", After: []string{"Credential"}}
			}
			r.Input = testRouteInput(t, reflect.TypeFor[scopedInput](), bindly.BindingSpec{Path: "Credential", Location: bindstate.Location{Kind: "credential"}}, binding)
			if _, err := New().Execute(t.Context(), r); err == nil {
				t.Fatal("admitted unsupported input")
			}
			if len(order) != 0 {
				t.Fatal("provider ran", order)
			}
		})
	}
}

type closedBindingOutput struct {
	binder   xhandler.Binder
	input    *scopedInput
	observed bool
}

func (o *closedBindingOutput) Finalize(ctx context.Context, cause error) error {
	if err := o.binder.Bind(ctx, o.input); err == nil {
		return errors.New("binder remained live through finalization")
	}
	o.observed = true
	return cause
}

type closeBindingContract struct{}

func (closeBindingContract) BindsInputByScope() bool { return true }
func (closeBindingContract) Exec(ctx context.Context, s xhandler.Session, i *scopedInput, o *closedBindingOutput) error {
	o.binder = s.Binder()
	o.input = i
	return nil
}
func TestScopedLeaseClosesBeforeOutputFinalization(t *testing.T) {
	var order []string
	r := scopedRequest(t, &scopedContract{}, "allowed", &order, nil)
	r.Handler = custom.New[scopedInput, closedBindingOutput](closeBindingContract{})
	r.OutputType = reflect.TypeFor[closedBindingOutput]()
	result, err := New().Execute(t.Context(), r)
	if err != nil || !result.(*closedBindingOutput).observed {
		t.Fatal(result, err)
	}
}
func TestScopedDerivedContextKeepsParentAuthorityAndLogger(t *testing.T) {
	log := &earlyOutputSink{}
	foreign := &earlyOutputSink{}
	parent := neutralDataScope(t.Context())
	var order []string
	r := scopedRequest(t, &scopedContract{}, "allowed", &order, nil)
	ctx := xlogger.WithContext(withDataScope(t.Context(), parent), log)
	injector, _ := bindly.NewInjector(bindly.WithProviders(handlerprovider.New("child", func(ctx context.Context) (any, bool, error) {
		scope, _ := ctx.Value(dataScopeContextKey{}).(*dataScope)
		if scope != parent || xlogger.FromContext(ctx) != log || ctx.Value("business-marker") != "call" {
			return nil, false, errors.New("call context replaced invocation authority")
		}
		return "ok", true, nil
	})))
	input := &scopedInput{}
	b := newScopedBinder(ctx, injector, input, r.Input, parent, nil)
	foreignData := neutralDataScope(t.Context())
	call := xlogger.WithContext(withDataScope(context.WithValue(t.Context(), "business-marker", "call"), foreignData), foreign)
	if err := b.BindScope(call, input, "lazy"); err != nil {
		t.Fatal(err)
	}
	if err := b.close(); err != nil {
		t.Fatal(err)
	}
	parent.releaseContexts()
}

func TestScopedMarkerAbsentAndFalseKeepFullAutomaticBinding(t *testing.T) {
	for _, markedFalse := range []bool{false, true} {
		t.Run(fmt.Sprint(markedFalse), func(t *testing.T) {
			var order []string
			c := &scopedContract{}
			r := scopedRequest(t, c, "allowed", &order, nil)
			run := func(_ context.Context, _ xhandler.Session, i *scopedInput, _ *scopedOutput) error {
				order = append(order, "begin")
				if i.Credential == nil || i.Child == nil || i.Has == nil || !i.Has.Credential || !i.Has.Child {
					t.Fatal("ordinary Scope interpretation changed")
				}
				return nil
			}
			if markedFalse {
				r.Handler = custom.New[scopedInput, scopedOutput](falseScopeContract{run: run})
			} else {
				r.Handler = custom.New[scopedInput, scopedOutput](xhandler.ContractFunc[scopedInput, scopedOutput](run))
			}
			if _, err := New().Execute(t.Context(), r); err != nil {
				t.Fatal(err)
			}
			if strings.Join(order, ",") != "default,child,begin" {
				t.Fatal(order)
			}
		})
	}
}

type falseScopeContract struct {
	run xhandler.ContractFunc[scopedInput, scopedOutput]
}

func (falseScopeContract) BindsInputByScope() bool { return false }
func (c falseScopeContract) Exec(ctx context.Context, s xhandler.Session, i *scopedInput, o *scopedOutput) error {
	return c.run(ctx, s, i, o)
}

func TestScopedEscapedWorkRetainsExplicitPublicPayload(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var lease *scopedBinder
	public := &modernresponse.Error{Code: 403, Cause: errors.New("public denial"), Payload: map[string]any{"reason": "explicit owner"}}
	c := &scopedContract{run: func(ctx context.Context, s xhandler.Session, i *scopedInput, o *scopedOutput) error {
		lease = s.Binder().(*scopedBinder)
		go func() { _ = lease.BindScope(ctx, i, "lazy") }()
		<-entered
		return public
	}}
	var order []string
	r := scopedRequest(t, c, "allowed", &order, nil)
	r.Providers = []locator.Provider{handlerprovider.New("credential", func(context.Context) (any, bool, error) { return "allowed", true, nil }), handlerprovider.New("child", func(context.Context) (any, bool, error) { close(entered); <-release; return "child", true, nil })}
	done := make(chan error, 1)
	go func() { _, err := New().Execute(t.Context(), r); done <- err }()
	<-entered
	for {
		lease.mu.Lock()
		closed := lease.closed
		lease.mu.Unlock()
		if closed {
			break
		}
		runtime.Gosched()
	}
	if _, _, err := lease.Lookup(t.Context(), xhandler.InputKey); err == nil {
		t.Fatal("closing lease lookup")
	}
	close(release)
	err := <-done
	var actual *modernresponse.Error
	if !errors.As(err, &actual) || actual != public || !strings.Contains(err.Error(), "escaped handler execution") {
		t.Fatalf("payload/cause/escaped failure lost: %v", err)
	}
}
func TestScopedPanicClosesLeaseBeforeEngineCompletion(t *testing.T) {
	var retained xhandler.ScopedBinder
	var input *scopedInput
	var order []string
	c := &scopedContract{run: func(_ context.Context, s xhandler.Session, i *scopedInput, _ *scopedOutput) error {
		retained = s.Binder().(xhandler.ScopedBinder)
		input = i
		panic("private scoped test panic")
	}}
	if _, err := New().Execute(t.Context(), scopedRequest(t, c, "allowed", &order, nil)); err == nil {
		t.Fatal("panic lost")
	}
	if err := retained.BindScope(t.Context(), input, "lazy"); err == nil {
		t.Fatal("panic lease retained")
	}
}

func TestScopedResolvedDefaultPlanAdmissionAndCanonicalState(t *testing.T) {
	for _, tc := range []struct {
		name    string
		paths   []string
		seed    any
		success bool
	}{
		{"valid", []string{"Credential"}, &scopedInput{Credential: scopedSeedString("seeded")}, true},
		{"lazy-outside-default", []string{"Child"}, &scopedInput{}, false},
		{"unknown", []string{"Unknown"}, &scopedInput{}, false},
		{"duplicate", []string{"Credential", "Credential"}, &scopedInput{}, false},
		{"typed-nil", []string{"Credential"}, (*scopedInput)(nil), false},
		{"wrong-type", []string{"Credential"}, &struct{}{}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var order []string
			c := &scopedContract{run: func(ctx context.Context, s xhandler.Session, i *scopedInput, o *scopedOutput) error {
				if !tc.success {
					t.Fatal("invalid resolved input executed")
				}
				seed := tc.seed.(*scopedInput)
				if i == seed || i.Has == nil || !i.Has.Credential || i.Has.Child || *i.Credential != "seeded" {
					t.Fatal("canonical default seed state", i)
				}
				jwt, has := i.Credential, i.Has
				if err := s.Binder().(xhandler.ScopedBinder).BindScope(ctx, i, "lazy"); err != nil {
					return err
				}
				if i.Credential != jwt || i.Has != has || !i.Has.Child || seed.Has != nil || seed.Child != nil {
					t.Fatal("canonical state or caller seed changed")
				}
				return nil
			}}
			req := scopedRequest(t, c, "must not resolve", &order, nil)
			req.ResolvedInput = tc.seed
			req.ResolvedPaths = tc.paths
			_, err := New().Execute(t.Context(), req)
			if tc.success {
				if err != nil || strings.Join(order, ",") != "child" {
					t.Fatal(err, order)
				}
			} else if err == nil || len(order) != 0 {
				t.Fatal("invalid resolved seed reached providers", err, order)
			}
		})
	}
}
func scopedSeedString(s string) *string { return &s }
