package output

import (
	"errors"
	"reflect"
	"sync"
	"testing"

	"github.com/viant/bindly"
	"github.com/viant/datly/spec"
	legacy "github.com/viant/xdatly/handler/response"
	modern "github.com/viant/xdatly/response"
)

func outerViewPlan(t *testing.T, typ reflect.Type, field string, change func(*spec.Component)) *Plan {
	t.Helper()
	c := &spec.Component{Parameters: []*spec.Parameter{declaredErrorStatus(field)}, RootView: &spec.View{Name: "OrdersCase", Source: &spec.ViewSource{SQL: "SELECT id FROM orders"}}}
	if change != nil {
		change(c)
	}
	p, err := (Compiler{}).Compile(CompileInput{Component: c, Type: typ})
	if err != nil {
		t.Fatal(err)
	}
	c.RootView.Name = "later caller metadata"
	return p
}
func outerViewStatus(t *testing.T, p *Plan, cause error) *legacy.Status {
	t.Helper()
	v, ok, err := p.ErrorEnvelopeWithCause("selected public text", 401, cause)
	if err != nil || !ok {
		t.Fatal(err, ok)
	}
	switch o := v.(type) {
	case *errorEnvelopeValue:
		return &o.Status
	case *errorEnvelopePointer:
		return o.State
	default:
		t.Fatalf("unexpected output %T", v)
		return nil
	}
}
func TestOuterViewFreshNestedRecordsValueAndPointer(t *testing.T) {
	for _, tc := range []struct {
		name, field string
		typ         reflect.Type
	}{{"value", "Status", reflect.TypeFor[*errorEnvelopeValue]()}, {"pointer", "State", reflect.TypeFor[*errorEnvelopePointer]()}} {
		t.Run(tc.name, func(t *testing.T) {
			p := outerViewPlan(t, tc.typ, tc.field, nil)
			leaf := bindingLeaf("Credential", 401)
			owner := &bindly.BindingError{Name: "Access", Path: "Access", Kind: "component", Cause: leaf}
			for i := 0; i < 2; i++ {
				s := outerViewStatus(t, p, owner)
				details := s.Errors.([]*legacy.Error)
				if len(details) != 1 || details[0].View != "OrdersCase" || details[0].Parameter != "Access" || details[0].Code != 401 || details[0].Err != leaf || s.Message != "selected public text" {
					t.Fatal("fresh outer record changed unrelated authority")
				}
				child := details[0].Object.([]*legacy.Error)
				if len(child) != 1 || child[0].View != "" || child[0].Parameter != "Credential" || child[0].Code != 401 || child[0].Err != leaf.Cause {
					t.Fatal("nested detail gained root view or lost authority")
				}
				details[0].View = "caller mutation"
				child[0].View = "caller nested mutation"
			}
			if owner.Cause != leaf || leaf.Name != "Credential" || leaf.Code != 401 {
				t.Fatal("source cause changed")
			}
		})
	}
}
func TestOuterViewJoinedRecordsAndConcurrentIsolation(t *testing.T) {
	p := outerViewPlan(t, reflect.TypeFor[*errorEnvelopeValue](), "Status", nil)
	first, second := bindingLeaf("First", 401), bindingLeaf("Second", 403)
	cause := errors.Join(first, second)
	check := func() {
		s := outerViewStatus(t, p, cause)
		records := s.Errors.([]*legacy.Error)
		if len(records) != 2 || records[0].View != "OrdersCase" || records[1].View != "OrdersCase" || records[0].Parameter != "First" || records[1].Parameter != "Second" || records[0].Code != 401 || records[1].Code != 403 {
			t.Error("joined outer source order/code/view changed")
			return
		}
		records[0].View = "request-local"
	}
	check()
	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() { defer wg.Done(); check() }()
	}
	wg.Wait()
}
func TestOuterViewExclusionsAndUnsupportedProjection(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*spec.Component)
	}{
		{"no source", func(c *spec.Component) { c.RootView.Source = nil }},
		{"source-less", func(c *spec.Component) { c.RootView.Source = &spec.ViewSource{} }},
		{"in-memory", func(c *spec.Component) { c.RootView.InMemory = true }},
		{"auxiliary", func(c *spec.Component) { c.RootView.Auxiliary = true }},
		{"mutation", func(c *spec.Component) { c.Settings = &spec.Settings{Mutation: "patch"} }},
		{"custom handler", func(c *spec.Component) { c.Routes = []*spec.Route{{Handler: "example.NewHandler"}} }},
		{"blank source name", func(c *spec.Component) { c.RootView.Name = "" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := outerViewPlan(t, reflect.TypeFor[*errorEnvelopeValue](), "Status", tc.change)
			s := outerViewStatus(t, p, bindingLeaf("Credential", 401))
			records := s.Errors.([]*legacy.Error)
			if len(records) != 1 || records[0].View != "" {
				t.Fatal("excluded root metadata stamped")
			}
		})
	}
	p := outerViewPlan(t, reflect.TypeFor[*errorEnvelopeValue](), "Status", nil)
	verifier := genuineVerifierFailure(t)
	opaque := &opaqueDiagnosticStatus{code: 403}
	customAs := &opaqueDiagnosticAs{coder: opaque}
	for _, tc := range []struct {
		name  string
		cause error
	}{
		{"nil", nil}, {"unowned", errors.New("private diagnostic")},
		{"verifier", &bindly.BindingError{Name: "Credential", Code: 401, Cause: verifier}},
		{"wrapped verifier", &bindly.BindingError{Name: "Access", Cause: &bindly.BindingError{Name: "Credential", Code: 401, Cause: verifier}}},
		{"opaque coder", &bindly.BindingError{Name: "Credential", Cause: opaque}},
		{"custom As", &bindly.BindingError{Name: "Credential", Cause: customAs}},
		{"public nil body", &modern.Error{Code: 401, Payload: nil, Cause: bindingLeaf("Credential", 401)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			beforeStatus, beforeError, beforeAs := opaque.statusCalls.Load(), opaque.errorCalls.Load(), customAs.calls.Load()
			s := outerViewStatus(t, p, tc.cause)
			if s.Errors != nil || s.Message != "selected public text" {
				t.Fatal("unsupported graph gained details/view/text")
			}
			if opaque.statusCalls.Load() != beforeStatus || opaque.errorCalls.Load() != beforeError || customAs.calls.Load() != beforeAs {
				t.Fatal("projection invoked unknown diagnostic authority")
			}
		})
	}
}
