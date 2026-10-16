package output

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/viant/bindly/state"
	dexec "github.com/viant/datly/exec"
	handlerprovider "github.com/viant/datly/runtime/handler/provider"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/viant/bindly"
	bindinput "github.com/viant/bindly/input"
	dauth "github.com/viant/datly/runtime/auth"
	"github.com/viant/datly/spec"
	"github.com/viant/scy/auth/jwt"
	"github.com/viant/scy/auth/jwt/verifier"
	xcodec "github.com/viant/xdatly/codec"
	legacy "github.com/viant/xdatly/handler/response"
	modern "github.com/viant/xdatly/response"
)

type originalEnvelopeAPI interface {
	ErrorEnvelope(string) (any, bool, error)
}

var _ originalEnvelopeAPI = (*Plan)(nil)

func bindingPlan(t *testing.T) *Plan {
	t.Helper()
	p, e := (Compiler{}).Compile(CompileInput{Component: &spec.Component{Parameters: []*spec.Parameter{declaredErrorStatus("Status")}}, Type: reflect.TypeFor[*errorEnvelopeValue]()})
	if e != nil {
		t.Fatal(e)
	}
	return p
}
func bindingLeaf(name string, code int) *bindly.BindingError {
	return &bindly.BindingError{Path: name, Name: name, Kind: "header", In: "X-Record", Code: code, Cause: &bindinput.Error{Cause: errors.New("private decoder diagnostic")}}
}
func projected(t *testing.T, p *Plan, cause error, code int) *errorEnvelopeValue {
	t.Helper()
	v, ok, e := p.ErrorEnvelopeWithCause("selected public text", code, cause)
	if e != nil || !ok {
		t.Fatal(e, ok)
	}
	o := v.(*errorEnvelopeValue)
	if o.Message != "selected public text" || o.Warning != nil || o.Extras != nil || o.Data != nil || o.Meta.Cnt != 0 {
		t.Fatal("non-error state changed", o)
	}
	return o
}
func TestBindingErrorEnvelopeActualOwnersRawCodeAndPrivateCause(t *testing.T) {
	p := bindingPlan(t)
	leaf := bindingLeaf("Credential", 401)
	outer := &bindly.BindingError{Path: "TenantAccess", Name: "TenantAccess", Kind: "component", In: "GET:/access", Cause: fmt.Errorf("private wrapper prefix: %w", leaf)}
	if outer.Code != 0 || outer.StatusCode() != 401 {
		t.Fatal("raw/resolved control")
	}
	o := projected(t, p, outer, 401)
	entries, ok := o.Errors.([]*legacy.Error)
	if !ok || len(entries) != 1 {
		t.Fatal("nominal Errors", o.Errors)
	}
	top := entries[0]
	if top.Parameter != outer.Name || top.View != "" || top.Code != 401 || top.Err != outer.Cause {
		t.Fatal("outer authority", top)
	}
	nested, ok := top.Object.([]*legacy.Error)
	if !ok || len(nested) != 1 || nested[0].Parameter != leaf.Name || nested[0].Code != 401 || nested[0].Err != leaf.Cause {
		t.Fatal("nested authority", top.Object)
	}
	wire, e := json.Marshal(o)
	if e != nil || strings.Contains(string(wire), "private") || strings.Contains(string(wire), "\"view\"") {
		t.Fatal("cause leaked", string(wire), e)
	}
	outer.Code = 403
	o = projected(t, p, outer, 403)
	entries = o.Errors.([]*legacy.Error)
	if entries[0].Parameter != outer.Name || entries[0].Code != 403 || entries[0].Object != nil {
		t.Fatal("explicit outer boundary erased", entries[0])
	}
	if leaf.Code != 401 || outer.Cause == nil {
		t.Fatal("native cause mutated")
	}
	old := p.ErrorEnvelope
	plain, ok, e := old("old method value")
	if e != nil || !ok || plain.(*errorEnvelopeValue).Errors != nil {
		t.Fatal("old API changed")
	}
}
func TestBindingErrorEnvelopeJoinedOrderAndIsolation(t *testing.T) {
	p := bindingPlan(t)
	a, b := bindingLeaf("First", 401), bindingLeaf("Second", 403)
	cause := errors.Join(fmt.Errorf("untrusted prefix %w", a), fmt.Errorf("another prefix %w", b))
	o := projected(t, p, cause, 401)
	entries := o.Errors.([]*legacy.Error)
	if len(entries) != 2 || entries[0].Parameter != a.Name || entries[1].Parameter != b.Name {
		t.Fatal("join order", entries)
	}
	entries[0].SetStatusCode(499)
	entries[0].Message = "caller changed"
	entries[0].Object = []int{1}
	again := projected(t, p, cause, 401).Errors.([]*legacy.Error)
	if again[0] == entries[0] || again[0].Code != 401 || again[0].Object != nil || a.Code != 401 {
		t.Fatal("projection mutation escaped")
	}
	var wg sync.WaitGroup
	for i := 0; i < 48; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			o := projected(t, p, cause, 401)
			e := o.Errors.([]*legacy.Error)
			e[0].Parameter = "local"
		}()
	}
	wg.Wait()
	if projected(t, p, cause, 401).Errors.([]*legacy.Error)[0].Parameter != a.Name {
		t.Fatal("concurrent projection mutated source")
	}
}

type cyclicBindingCause struct{}

func (*cyclicBindingCause) Error() string   { panic("must not format cyclic diagnostics") }
func (e *cyclicBindingCause) Unwrap() error { return e }

type panickingBindingCause struct{}

func (panickingBindingCause) Error() string { return "private" }
func (panickingBindingCause) Unwrap() error { panic("foreign unwrap") }
func TestBindingErrorEnvelopeUnsupportedGraphsKeepPlainFactory(t *testing.T) {
	p := bindingPlan(t)
	leaf := bindingLeaf("Credential", 401)
	for _, tc := range []struct {
		name  string
		cause error
	}{{"nil", nil}, {"opaque", errors.New("private opaque")}, {"missing owner name", &bindly.BindingError{Path: "CannotInventName", Code: 401, Cause: leaf}}, {"mixed join", errors.Join(leaf, errors.New("unowned branch"))}, {"explicit server mixed join", &bindly.BindingError{Name: "Records", Code: 500, Cause: errors.Join(leaf, errors.New("unowned branch"))}}, {"cyclic", &bindly.BindingError{Name: "Record", Code: 401, Cause: &cyclicBindingCause{}}}, {"foreign panic", &bindly.BindingError{Name: "Record", Code: 401, Cause: panickingBindingCause{}}}, {"explicit nil body", &modern.Error{Code: 401, Payload: nil, Cause: leaf}}, {"unsafe private codec", &bindly.BindingError{Name: "Record", Code: 401, Cause: errors.New("private credential")}}, {"private interpolation", &bindly.BindingError{Name: "Record", Code: 401, Message: "policy ${error}", Cause: leaf}}} {
		t.Run(tc.name, func(t *testing.T) {
			if o := projected(t, p, tc.cause, 401); o.Errors != nil {
				t.Fatal("unsupported tree partially projected", o.Errors)
			}
		})
	}
}
func TestBindingErrorEnvelopeSQLServerMaskAndCauseIdentity(t *testing.T) {
	p := bindingPlan(t)
	cause := errors.New("SELECT private_column FROM private_table")
	binding := &bindly.BindingError{Name: "Records", Path: "Records", Cause: cause}
	o := projected(t, p, binding, 500)
	entry := o.Errors.([]*legacy.Error)[0]
	if entry.Code != 500 || entry.Message != "Internal Server Error" || entry.Err != cause {
		t.Fatal("server mask/cause", entry)
	}
	wire, _ := json.Marshal(o)
	if strings.Contains(string(wire), "private") {
		t.Fatal("SQL escaped", string(wire))
	}
}

// The public Bindly group controller is test admission only. This produces an
// actual ResolutionGroupError; it does not bypass Datly's finite native gate.
type bindingGroupController struct{ run *bindingGroupRun }

func (c bindingGroupController) Open(context.Context, bindly.ResolutionGroupPlan) (bindly.ResolutionGroupRun, error) {
	return c.run, nil
}

type bindingGroupRun struct {
	observed chan string
	closed   bool
}

func (r *bindingGroupRun) Enter(ctx context.Context, _ string) (context.Context, error) {
	return ctx, nil
}
func (r *bindingGroupRun) Observe(_ context.Context, o bindly.ResolutionOutcome) error {
	r.observed <- o.Path()
	return nil
}
func (r *bindingGroupRun) Close(context.Context) error { r.closed = true; return nil }
func TestBindingErrorEnvelopeRealOrderedResolutionGroup(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	type input struct {
		Seed          string
		Body          *struct{ ID int }
		First, Second int
	}
	entered := make(chan string, 2)
	release := map[string]chan struct{}{"First": make(chan struct{}), "Second": make(chan struct{})}
	causes := map[string]error{"First": &bindinput.Error{Code: 401, Cause: errors.New("private first")}, "Second": &bindinput.Error{Code: 403, Cause: errors.New("private second")}}
	provider := handlerprovider.Named("component", func(ctx context.Context, _ reflect.Type, name string) (any, bool, error) {
		entered <- name
		select {
		case <-release[name]:
			return nil, false, causes[name]
		case <-ctx.Done():
			return nil, false, ctx.Err()
		}
	})
	injector, e := bindly.NewInjector(bindly.WithProviders(provider, handlerprovider.Named("header", func(context.Context, reflect.Type, string) (any, bool, error) { return "present", true, nil }), handlerprovider.Named("body", func(context.Context, reflect.Type, string) (any, bool, error) { return &struct{ ID int }{1}, true, nil })))
	if e != nil {
		t.Fatal(e)
	}
	required := true
	specs := []bindly.BindingSpec{{Path: "Seed", Name: "Seed", Location: state.Location{Kind: "header", In: "X-Seed"}, Required: &required}, {Path: "Body", Name: "Body", Location: state.Location{Kind: "body"}, Required: &required}}
	for _, name := range []string{"First", "Second"} {
		specs = append(specs, bindly.BindingSpec{Path: name, Name: name, Location: state.Location{Kind: "component", In: name}, Required: &required, ResolutionGroup: &bindly.ResolutionGroupSpec{Name: "records", After: []string{"Seed", "Body"}}})
	}
	plan, e := injector.CompilePlan(reflect.TypeFor[input](), specs...)
	if e != nil {
		t.Fatal(e)
	}
	target := &input{}
	run := &bindingGroupRun{observed: make(chan string, 2)}
	done := make(chan error, 1)
	go func() {
		done <- injector.Bind(ctx, target, bindly.WithPlan(plan), bindly.WithSource(target), bindly.WithResolutionGroupController(bindingGroupController{run}))
	}()
	for range 2 {
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	order := []string{"Second", "First"}
	for _, name := range order {
		close(release[name])
		select {
		case observed := <-run.observed:
			if observed != name {
				t.Fatalf("completion %s vs%s", observed, name)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	e = <-done
	var group *bindly.ResolutionGroupError
	if !errors.As(e, &group) || !run.closed {
		t.Fatal("actual group absent", e)
	}
	result := projected(t, bindingPlan(t), fmt.Errorf("ordinary wrapper: %w", group), 403).Errors.([]*legacy.Error)
	failures := group.Failures()
	if len(result) != 2 || len(failures) != 2 {
		t.Fatal("group siblings lost")
	}
	for i, name := range order {
		if result[i].Parameter != name || result[i].Code != failures[i].StatusCode() || result[i].Err != causes[name] || result[i].Object != nil {
			t.Fatalf("ordered owner %d %#v", i, result[i])
		}
	}
	result[0].Code = 499
	if group.Failures()[0].Code != 0 || group.Failures()[0].StatusCode() != 403 {
		t.Fatal("group authority mutated")
	}
	t.Log("real Bindly group completion order Second,First; raw codes0; resolved403,401; nominal Errors retain same ordered causes")
}

func TestBindingErrorEnvelopeCauseAwareExactCompiledAuthority(t *testing.T) {
	leaf := bindingLeaf("Credential", 401)
	for _, tc := range []struct {
		name           string
		typ            reflect.Type
		field          string
		emit, eligible bool
	}{
		{"value output-source", reflect.TypeFor[errorEnvelopeValue](), "Status", false, true},
		{"pointer output-source", reflect.TypeFor[*errorEnvelopePointer](), "State", false, true},
		{"original Output-only", reflect.TypeFor[errorEnvelopeValue](), "Status", true, true},
		{"modern", reflect.TypeFor[struct{ Status modern.Status }](), "Status", false, false},
		{"unowned", reflect.TypeFor[errorEnvelopeValue](), "", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var params []*spec.Parameter
			if tc.field != "" {
				p := declaredErrorStatus(tc.field)
				if tc.emit {
					p.EmitOutput = true
					p.Source.Kind = "body"
				}
				params = []*spec.Parameter{p}
			}
			component := &spec.Component{Parameters: params}
			p, e := (Compiler{}).Compile(CompileInput{Component: component, Type: tc.typ})
			if e != nil {
				t.Fatal(e)
			}
			component.Parameters = nil
			if len(params) > 0 {
				params[0].Name = "mutated after compile"
			}
			value, ok, e := p.ErrorEnvelopeWithCause("public", 401, leaf)
			if e != nil || ok != tc.eligible {
				t.Fatal("cause-aware authority", ok, e)
			}
			if !ok {
				if value != nil {
					t.Fatal("ineligible output")
				}
				return
			}
			var holder *legacy.Status
			switch o := value.(type) {
			case errorEnvelopeValue:
				holder = &o.Status
			case *errorEnvelopePointer:
				holder = o.State
			default:
				t.Fatalf("declared shape %T", value)
			}
			entries, yes := holder.Errors.([]*legacy.Error)
			if !yes || len(entries) != 1 || entries[0].Parameter != leaf.Name || holder.Extras != nil || holder.Warning != nil {
				t.Fatal("compiled holder changed", holder)
			}
		})
	}
}

type opaqueDiagnosticStatus struct {
	code                    int
	statusCalls, errorCalls atomic.Int64
}

func (e *opaqueDiagnosticStatus) StatusCode() int { e.statusCalls.Add(1); return e.code }
func (e *opaqueDiagnosticStatus) Error() string {
	e.errorCalls.Add(1)
	return "PRIVATE_OPAQUE_DIAGNOSTIC"
}

func TestBindingErrorEnvelopeOpaqueInheritedStatusKeepsPlain(t *testing.T) {
	for _, name := range []string{"direct", "wrapped diagnostic", "wrapped owner", "explicit outer", "mixed join"} {
		t.Run(name, func(t *testing.T) {
			opaque := &opaqueDiagnosticStatus{code: 403}
			var diagnostic error = opaque
			if name == "wrapped diagnostic" {
				diagnostic = fmt.Errorf("private wrapper: %w", opaque)
			}
			invalid := &bindinput.Error{Cause: diagnostic}
			owner := &bindly.BindingError{Name: "Field", Cause: invalid}
			if name == "explicit outer" {
				owner.Code = 401
			}
			var cause error = owner
			if name == "wrapped owner" {
				cause = fmt.Errorf("private wrapper: %w", owner)
			}
			if name == "mixed join" {
				cause = errors.Join(bindingLeaf("Known", 401), owner)
			}
			if invalid.Code != 0 || invalid.StatusCode() != 403 || owner.StatusCode() != map[bool]int{true: 401, false: 403}[name == "explicit outer"] {
				t.Fatal("native inherited status authority changed")
			}
			selected := owner.StatusCode()
			message := dexec.ErrorMessage(cause, selected)
			if strings.Contains(message, "PRIVATE_") {
				t.Fatal("native safe message prerequisite")
			}
			beforeStatus, beforeError := opaque.statusCalls.Load(), opaque.errorCalls.Load()
			raw, eligible, projectionErr := bindingPlan(t).ErrorEnvelopeWithCause(message, selected, cause)
			if projectionErr != nil || !eligible {
				t.Fatal("compiled envelope", projectionErr)
			}
			value := raw.(*errorEnvelopeValue)
			if value.Errors != nil || value.Status.Status != "error" || value.Message != message {
				t.Fatalf("opaque inherited code acquired structured/default status: %#v", value.Errors)
			}
			if opaque.statusCalls.Load() != beforeStatus || opaque.errorCalls.Load() != beforeError {
				t.Fatal("projector called unknown diagnostic/coder")
			}
			if owner.Unwrap() != invalid || invalid.Cause != diagnostic {
				t.Fatal("original private cause changed")
			}
		})
	}
}

func TestBindingErrorEnvelopeRealGroupOpaqueInheritedStatusKeepsPlain(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	type input struct {
		Seed          string
		Body          *struct{ ID int }
		First, Second int
	}
	entered := make(chan string, 2)
	release := map[string]chan struct{}{"First": make(chan struct{}), "Second": make(chan struct{})}
	causes := map[string]error{"First": &bindinput.Error{Code: 401, Cause: errors.New("private first")}, "Second": &bindinput.Error{Cause: &opaqueDiagnosticStatus{code: 403}}}
	provider := handlerprovider.Named("component", func(ctx context.Context, _ reflect.Type, name string) (any, bool, error) {
		entered <- name
		select {
		case <-release[name]:
			return nil, false, causes[name]
		case <-ctx.Done():
			return nil, false, ctx.Err()
		}
	})
	injector, e := bindly.NewInjector(bindly.WithProviders(provider, handlerprovider.Named("header", func(context.Context, reflect.Type, string) (any, bool, error) { return "present", true, nil }), handlerprovider.Named("body", func(context.Context, reflect.Type, string) (any, bool, error) { return &struct{ ID int }{1}, true, nil })))
	if e != nil {
		t.Fatal(e)
	}
	required := true
	specs := []bindly.BindingSpec{{Path: "Seed", Name: "Seed", Location: state.Location{Kind: "header", In: "X-Seed"}, Required: &required}, {Path: "Body", Name: "Body", Location: state.Location{Kind: "body"}, Required: &required}}
	for _, name := range []string{"First", "Second"} {
		specs = append(specs, bindly.BindingSpec{Path: name, Name: name, Location: state.Location{Kind: "component", In: name}, Required: &required, ResolutionGroup: &bindly.ResolutionGroupSpec{Name: "records", After: []string{"Seed", "Body"}}})
	}
	plan, e := injector.CompilePlan(reflect.TypeFor[input](), specs...)
	if e != nil {
		t.Fatal(e)
	}
	target := &input{}
	run := &bindingGroupRun{observed: make(chan string, 2)}
	done := make(chan error, 1)
	go func() {
		done <- injector.Bind(ctx, target, bindly.WithPlan(plan), bindly.WithSource(target), bindly.WithResolutionGroupController(bindingGroupController{run}))
	}()
	for range 2 {
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	order := []string{"Second", "First"}
	for _, name := range order {
		close(release[name])
		select {
		case observed := <-run.observed:
			if observed != name {
				t.Fatalf("completion %s vs%s", observed, name)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	e = <-done
	var group *bindly.ResolutionGroupError
	if !errors.As(e, &group) || !run.closed {
		t.Fatal("actual group absent", e)
	}

	opaque := causes["Second"].(*bindinput.Error).Cause.(*opaqueDiagnosticStatus)
	failures := group.Failures()
	if failures[0].Code != 0 || failures[0].StatusCode() != 403 {
		t.Fatal("real group raw/resolved status")
	}
	beforeStatus, beforeError := opaque.statusCalls.Load(), opaque.errorCalls.Load()
	if o := projected(t, bindingPlan(t), group, 403); o.Errors != nil {
		t.Fatal("opaque group partially projected", o.Errors)
	}
	if opaque.statusCalls.Load() != beforeStatus || opaque.errorCalls.Load() != beforeError {
		t.Fatal("unknown group coder formatted/called")
	}
	if group.Failures()[0].Cause != causes["Second"] {
		t.Fatal("group original cause changed")
	}
}

type opaqueDiagnosticAs struct {
	coder *opaqueDiagnosticStatus
	calls atomic.Int64
}

func (e *opaqueDiagnosticAs) Error() string { return "PRIVATE_CUSTOM_AS_DIAGNOSTIC" }
func (e *opaqueDiagnosticAs) As(target any) bool {
	e.calls.Add(1)
	if p, ok := target.(*interface{ StatusCode() int }); ok {
		*p = e.coder
		return true
	}
	return false
}
func TestBindingErrorEnvelopeOpaqueAsStatusKeepsPlain(t *testing.T) {
	for _, name := range []string{"direct", "wrapped", "joined"} {
		t.Run(name, func(t *testing.T) {
			coder := &opaqueDiagnosticStatus{code: 403}
			opaque := &opaqueDiagnosticAs{coder: coder}
			var diagnostic error = opaque
			if name == "wrapped" {
				diagnostic = fmt.Errorf("private wrapper %w", opaque)
			}
			invalid := &bindinput.Error{Cause: diagnostic}
			owner := &bindly.BindingError{Name: "Field", Cause: invalid}
			if invalid.Code != 0 || invalid.StatusCode() != 403 || owner.StatusCode() != 403 {
				t.Fatal("native custom As inherited authority")
			}
			var cause error = owner
			if name == "joined" {
				cause = errors.Join(owner, bindingLeaf("Known", 401))
			}
			asCalls, statusCalls := opaque.calls.Load(), coder.statusCalls.Load()
			if o := projected(t, bindingPlan(t), cause, 403); o.Errors != nil {
				t.Fatal("custom As authority partially projected")
			}
			if opaque.calls.Load() != asCalls || coder.statusCalls.Load() != statusCalls || coder.errorCalls.Load() != 0 {
				t.Fatal("projector invoked unknown As/coder")
			}
			if invalid.Cause != diagnostic || owner.Unwrap() != invalid {
				t.Fatal("custom As cause changed")
			}
		})
	}
}

func genuineVerifierFailure(t *testing.T) *dauth.VerificationFailure {
	t.Helper()
	service, e := dauth.New(t.Context(), &dauth.Config{JWTValidator: &verifier.Config{CertURL: "private-diagnostic://invalid/GENERIC_PRIVATE_SENTINEL"}})
	if e != nil {
		t.Fatal(e)
	}
	codec, e := service.New(&xcodec.Config{Body: "JwtClaim", SourceType: reflect.TypeFor[string](), DestinationType: reflect.TypeFor[*jwt.Claims]()})
	if e != nil {
		t.Fatal(e)
	}
	_, e = codec.Value(t.Context(), "non-secret-generic-input")
	var failure *dauth.VerificationFailure
	if !errors.As(e, &failure) {
		t.Fatal("genuine configured service failure missing")
	}
	return failure
}
func TestBindingErrorEnvelopeRealGroupVerifierAlwaysKeepsPlain(t *testing.T) {
	failure := genuineVerifierFailure(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	type input struct {
		Seed          string
		Body          *struct{ ID int }
		First, Second int
	}
	entered := make(chan string, 2)
	release := map[string]chan struct{}{"First": make(chan struct{}), "Second": make(chan struct{})}
	causes := map[string]error{"First": &bindinput.Error{Code: 401, Cause: errors.New("private first")}, "Second": &bindinput.Error{Cause: failure}}
	provider := handlerprovider.Named("component", func(ctx context.Context, _ reflect.Type, name string) (any, bool, error) {
		entered <- name
		select {
		case <-release[name]:
			return nil, false, causes[name]
		case <-ctx.Done():
			return nil, false, ctx.Err()
		}
	})
	injector, e := bindly.NewInjector(bindly.WithProviders(provider, handlerprovider.Named("header", func(context.Context, reflect.Type, string) (any, bool, error) { return "present", true, nil }), handlerprovider.Named("body", func(context.Context, reflect.Type, string) (any, bool, error) { return &struct{ ID int }{1}, true, nil })))
	if e != nil {
		t.Fatal(e)
	}
	required := true
	specs := []bindly.BindingSpec{{Path: "Seed", Name: "Seed", Location: state.Location{Kind: "header", In: "X-Seed"}, Required: &required}, {Path: "Body", Name: "Body", Location: state.Location{Kind: "body"}, Required: &required}}
	for _, name := range []string{"First", "Second"} {
		specs = append(specs, bindly.BindingSpec{Path: name, Name: name, Location: state.Location{Kind: "component", In: name}, Required: &required, ResolutionGroup: &bindly.ResolutionGroupSpec{Name: "records", After: []string{"Seed", "Body"}}})
	}
	plan, e := injector.CompilePlan(reflect.TypeFor[input](), specs...)
	if e != nil {
		t.Fatal(e)
	}
	target := &input{}
	run := &bindingGroupRun{observed: make(chan string, 2)}
	done := make(chan error, 1)
	go func() {
		done <- injector.Bind(ctx, target, bindly.WithPlan(plan), bindly.WithSource(target), bindly.WithResolutionGroupController(bindingGroupController{run}))
	}()
	for range 2 {
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	order := []string{"Second", "First"}
	for _, name := range order {
		close(release[name])
		select {
		case observed := <-run.observed:
			if observed != name {
				t.Fatalf("completion %s vs%s", observed, name)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	e = <-done
	var group *bindly.ResolutionGroupError
	if !errors.As(e, &group) || !run.closed {
		t.Fatal("actual group absent", e)
	}

	failures := group.Failures()
	if failures[0].Code != 0 || failures[0].StatusCode() != 400 {
		t.Fatal("real group verifier raw/resolved status")
	}
	if o := projected(t, bindingPlan(t), group, 400); o.Errors != nil {
		t.Fatal("opaque verifier group partially projected")
	}
	if group.Failures()[0].Cause != causes["Second"] || causes["Second"].(*bindinput.Error).Cause != failure {
		t.Fatal("original verifier group cause changed")
	}
}
