package output

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/viant/bindly"
	bindinput "github.com/viant/bindly/input"
	"github.com/viant/bindly/state"
	handlerprovider "github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/spec"
	legacy "github.com/viant/xdatly/handler/response"
)

type nominalPrivateCause struct{}

func (nominalPrivateCause) Error() string   { panic("private Error must not run") }
func (nominalPrivateCause) StatusCode() int { panic("private StatusCode must not run") }
func (nominalPrivateCause) As(any) bool     { panic("private As must not run") }
func (nominalPrivateCause) Unwrap() error   { panic("private Unwrap must not run") }
func nominalBusinessComponent() *spec.Component {
	return &spec.Component{Parameters: []*spec.Parameter{declaredErrorStatus("Status")}, RootView: &spec.View{Name: "AuthoritativeRecords", Source: &spec.ViewSource{SQL: "SELECT id FROM records"}}}
}
func nominalBusinessPlan(t *testing.T, c *spec.Component) *Plan {
	t.Helper()
	p, e := (Compiler{}).Compile(CompileInput{Component: c, Type: reflect.TypeFor[*errorEnvelopeValue]()})
	if e != nil {
		t.Fatal(e)
	}
	return p
}
func nominalBusinessCause() (*bindly.BindingError, *legacy.Error) {
	terminal := &legacy.Error{Code: 403, Message: "record access denied", Err: nominalPrivateCause{}}
	return &bindly.BindingError{Name: "Access", Path: "Access", Kind: "component", In: "GET:/access", Cause: terminal}, terminal
}
func nominalBusinessOutput(t *testing.T, p *Plan, cause error, status int) *errorEnvelopeValue {
	t.Helper()
	v, ok, e := p.ErrorEnvelopeWithCause("previous selected public text", status, cause)
	if e != nil || !ok {
		t.Fatal(e, ok)
	}
	return v.(*errorEnvelopeValue)
}
func assertNominalPlain(t *testing.T, p *Plan, cause error, status int) {
	t.Helper()
	o := nominalBusinessOutput(t, p, cause, status)
	if o.Message != "previous selected public text" || o.Errors != nil {
		t.Fatal("unsupported graph changed existing plain presentation")
	}
}

func TestNominalBusinessReaderOwnerPublicFieldsAndFreshness(t *testing.T) {
	c := nominalBusinessComponent()
	p := nominalBusinessPlan(t, c)
	c.RootView.Name = "mutated source"
	c.RootView.Source.SQL = ""
	c.Routes = []*spec.Route{{Handler: "later mutation"}}
	root, terminal := nominalBusinessCause()
	inner := &bindly.BindingError{Name: "Decision", Kind: "component", Cause: terminal}
	root.Cause = fmt.Errorf("untrusted wrapper: %w", inner)
	for _, code := range []int{0, 403} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			root.Code = code
			o := nominalBusinessOutput(t, p, fmt.Errorf("wrapper: %w", root), 403)
			if o.Message != terminal.Message || o.Status.Status != "error" {
				t.Fatal("nominal public message not selected")
			}
			details, ok := o.Errors.([]*legacy.Error)
			if !ok || len(details) != 1 {
				t.Fatal("nominal details missing")
			}
			top := details[0]
			if top.Parameter != "Access" || top.View != "AuthoritativeRecords" || top.Message != terminal.Message || top.Code != 403 || top.Err != root.Cause {
				t.Fatal("outer authority")
			}
			if code == 0 {
				children, ok := top.Object.([]*legacy.Error)
				if !ok || len(children) != 1 || children[0].Parameter != "Decision" || children[0].View != "" || children[0].Err != terminal || children[0].Object != nil {
					t.Fatal("nested owner authority")
				}
			} else if top.Object != nil {
				t.Fatal("explicit code ignored")
			}
			wire, e := json.Marshal(o)
			if e != nil || strings.Contains(string(wire), "private") || strings.Contains(string(wire), "wrapper") {
				t.Fatal("private data leaked")
			}
			top.Message = "caller mutation"
			again := nominalBusinessOutput(t, p, root, 403)
			if again.Message != terminal.Message || again.Errors.([]*legacy.Error)[0] == top {
				t.Fatal("fresh output not independent")
			}
		})
	}
	if terminal.Code != 403 || terminal.Message != "record access denied" || terminal.View != "" || terminal.Parameter != "" || terminal.Object != nil {
		t.Fatal("source terminal mutated")
	}
	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			o := nominalBusinessOutput(t, p, root, 403)
			o.Errors.([]*legacy.Error)[0].View = "request-local"
		}()
	}
	wg.Wait()
}

func TestNominalBusinessReaderCompileMetadataExclusions(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*spec.Component)
	}{
		{"no root", func(c *spec.Component) { c.RootView = nil }}, {"empty root name", func(c *spec.Component) { c.RootView.Name = "" }}, {"source less", func(c *spec.Component) { c.RootView.Source = nil }}, {"empty source", func(c *spec.Component) { c.RootView.Source = &spec.ViewSource{} }}, {"in memory", func(c *spec.Component) { c.RootView.InMemory = true }}, {"auxiliary", func(c *spec.Component) { c.RootView.Auxiliary = true }}, {"mutation", func(c *spec.Component) { c.Settings = &spec.Settings{Mutation: "patch"} }}, {"declared custom handler", func(c *spec.Component) { c.Routes = []*spec.Route{{Handler: "example.NewHandler"}} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := nominalBusinessComponent()
			tc.change(c)
			root, _ := nominalBusinessCause()
			assertNominalPlain(t, nominalBusinessPlan(t, c), root, 403)
		})
	}
	for _, source := range []*spec.ViewSource{{URI: "records.sql"}, {Table: "records"}} {
		c := nominalBusinessComponent()
		c.RootView.Source = source
		root, _ := nominalBusinessCause()
		if nominalBusinessOutput(t, nominalBusinessPlan(t, c), root, 403).Errors == nil {
			t.Fatal("existing executable metadata excluded")
		}
	}
}

func TestNominalBusinessUnsupportedAuthorityRetainsPlain(t *testing.T) {
	p := nominalBusinessPlan(t, nominalBusinessComponent())
	for _, name := range []string{"direct", "typed nil", "zero code", "server code", "empty message", "view", "parameter", "object", "typed nil object", "outer conflict", "selected mismatch", "authored message", "private template", "input diagnostic", "join", "wrapped mixed join"} {
		t.Run(name, func(t *testing.T) {
			root, terminal := nominalBusinessCause()
			var cause error = root
			status := 403
			switch name {
			case "direct":
				cause = terminal
			case "typed nil":
				cause = &bindly.BindingError{Name: "Access", Cause: (*legacy.Error)(nil)}
			case "zero code":
				terminal.Code = 0
			case "server code":
				terminal.Code = 500
				status = 500
			case "empty message":
				terminal.Message = ""
			case "view":
				terminal.View = "prestructured"
			case "parameter":
				terminal.Parameter = "prestructured"
			case "object":
				terminal.Object = nominalPrivateCause{}
			case "typed nil object":
				terminal.Object = (*int)(nil)
			case "outer conflict":
				root.Code = 401
			case "selected mismatch":
				status = 401
			case "authored message":
				root.Message = "authored decision"
			case "private template":
				root.Message = "authored ${error}"
			case "input diagnostic":
				root.Cause = &bindinput.Error{Code: 403, Cause: terminal}
			case "join":
				cause = errors.Join(root)
			case "wrapped mixed join":
				cause = fmt.Errorf("wrapper: %w", errors.Join(root, bindingLeaf("Other", 401)))
			}
			assertNominalPlain(t, p, cause, status)
		})
	}
}

func TestNominalBusinessActualResolutionGroupRetainsPlain(t *testing.T) {
	type input struct {
		Seed          string
		Body          *struct{ ID int }
		First, Second int
	}
	ctx := t.Context()
	_, terminal := nominalBusinessCause()
	injector, e := bindly.NewInjector(bindly.WithProviders(handlerprovider.Named("component", func(context.Context, reflect.Type, string) (any, bool, error) { return nil, false, terminal }), handlerprovider.Named("header", func(context.Context, reflect.Type, string) (any, bool, error) { return "present", true, nil }), handlerprovider.Named("body", func(context.Context, reflect.Type, string) (any, bool, error) { return &struct{ ID int }{1}, true, nil })))
	if e != nil {
		t.Fatal(e)
	}
	required := true
	specs := []bindly.BindingSpec{{Path: "Seed", Name: "Seed", Location: state.Location{Kind: "header", In: "X-Seed"}, Required: &required}, {Path: "Body", Name: "Body", Location: state.Location{Kind: "body"}, Required: &required}}
	for _, name := range []string{"First", "Second"} {
		specs = append(specs, bindly.BindingSpec{Path: name, Name: name, Location: state.Location{Kind: "component", In: name}, ResolutionGroup: &bindly.ResolutionGroupSpec{Name: "records", After: []string{"Seed", "Body"}}})
	}
	plan, e := injector.CompilePlan(reflect.TypeFor[input](), specs...)
	if e != nil {
		t.Fatal(e)
	}
	target := &input{}
	run := &bindingGroupRun{observed: make(chan string, 2)}
	cause := injector.Bind(ctx, target, bindly.WithPlan(plan), bindly.WithSource(target), bindly.WithResolutionGroupController(bindingGroupController{run}))
	var group *bindly.ResolutionGroupError
	if !errors.As(cause, &group) || !run.closed || len(group.Failures()) != 2 {
		t.Fatal("actual resolution group not exercised")
	}
	assertNominalPlain(t, nominalBusinessPlan(t, nominalBusinessComponent()), cause, 403)
}
