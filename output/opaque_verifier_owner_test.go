package output

import (
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/viant/bindly"
	bindinput "github.com/viant/bindly/input"
	dauth "github.com/viant/datly/runtime/auth"
	"github.com/viant/datly/spec"
	legacy "github.com/viant/xdatly/handler/response"
	modern "github.com/viant/xdatly/response"
)

type countedOwnerWrapper struct {
	cause error
	calls atomic.Int64
}

func (*countedOwnerWrapper) Error() string   { panic("wrapper text must not run") }
func (w *countedOwnerWrapper) Unwrap() error { w.calls.Add(1); return w.cause }
func opaqueOwnerPlan(t *testing.T, typ reflect.Type, field string) *Plan {
	return outerViewPlan(t, typ, field, nil)
}
func TestOpaqueVerifierExplicitOwnerFixedLiteralFieldsAndSingleParse(t *testing.T) {
	failure := genuineVerifierFailure(t)
	for _, tc := range []struct {
		name, field string
		typ         reflect.Type
	}{{"value", "Status", reflect.TypeFor[*errorEnvelopeValue]()}, {"pointer", "State", reflect.TypeFor[*errorEnvelopePointer]()}} {
		t.Run(tc.name, func(t *testing.T) {
			p := opaqueOwnerPlan(t, tc.typ, tc.field)
			for _, kind := range []string{"header", "query", "path", "cookie", "form", "body"} {
				for _, code := range []int{400, 401, 403, 422} {
					for _, literal := range []string{"", "  authored safe text  "} {
						owner := &bindly.BindingError{Name: "AccountCredential", Path: "private storage path", Kind: kind, In: "X-Account", Code: code, Message: literal, Cause: failure}
						wrapper := &countedOwnerWrapper{cause: owner}
						value, ok, err := p.ErrorEnvelopeWithCause("PRIVATE FALLBACK SENTINEL", code, wrapper)
						if err != nil || !ok {
							t.Fatal(err, ok)
						}
						var status *legacy.Status
						switch o := value.(type) {
						case *errorEnvelopeValue:
							status = &o.Status
						case *errorEnvelopePointer:
							status = o.State
						}
						records, ok := status.Errors.([]*legacy.Error)
						if !ok || len(records) != 1 {
							t.Fatal("direct explicit owner not presented")
						}
						want := http.StatusText(code)
						if literal != "" {
							want = literal
						}
						record := records[0]
						if status.Message != want || record.Message != want || record.View != "OrdersCase" || record.Parameter != owner.Name || record.Code != code || record.Err != failure || record.Object != nil {
							t.Fatal("safe public fields/cause identity changed")
						}
						if wrapper.calls.Load() != 1 {
							t.Fatal("error tree parsed more than once")
						}
						record.View = "caller mutation"
						if owner.Cause != failure || owner.Message != literal || owner.Code != code || owner.Name != "AccountCredential" {
							t.Fatal("source owner mutated")
						}
					}
				}
			}
		})
	}
}
func TestOpaqueVerifierRejectsInheritedNestedGroupedAndOpaqueAuthority(t *testing.T) {
	failure := genuineVerifierFailure(t)
	owner := func() *bindly.BindingError {
		return &bindly.BindingError{Name: "Credential", Kind: "header", Code: 401, Cause: failure}
	}
	p := opaqueOwnerPlan(t, reflect.TypeFor[*errorEnvelopeValue](), "Status")
	for _, tc := range []struct {
		name  string
		cause func() error
		code  int
	}{
		{"bare", func() error { return failure }, 401},
		{"typed nil verifier", func() error { o := owner(); o.Cause = (*dauth.VerificationFailure)(nil); return o }, 401},
		{"raw zero", func() error { o := owner(); o.Code = 0; return o }, 401},
		{"raw zero selected403", func() error { o := owner(); o.Code = 0; return o }, 403},
		{"input zero", func() error { return &bindinput.Error{Cause: failure} }, 403},
		{"input explicit403", func() error { return &bindinput.Error{Code: 403, Cause: failure} }, 403},
		{"input above owner", func() error { return &bindinput.Error{Code: 401, Cause: owner()} }, 401},
		{"input below owner", func() error { o := owner(); o.Cause = &bindinput.Error{Code: 401, Cause: failure}; return o }, 401},
		{"second owner", func() error { return &bindly.BindingError{Name: "Parent", Kind: "header", Code: 401, Cause: owner()} }, 401},
		{"wrapper below owner", func() error { o := owner(); o.Cause = &countedOwnerWrapper{cause: failure}; return o }, 401},
		{"single join", func() error { return errors.Join(owner()) }, 401},
		{"joined order", func() error { return errors.Join(owner(), bindingLeaf("Other", 401)) }, 401},
		{"reverse join", func() error { return errors.Join(bindingLeaf("Other", 401), owner()) }, 401},
		{"selected mismatch", func() error { return owner() }, 403},
		{"server", func() error { o := owner(); o.Code = 500; return o }, 500},
		{"not transport", func() error { o := owner(); o.Kind = "component"; return o }, 401},
		{"blank name", func() error { o := owner(); o.Name = " "; return o }, 401},
		{"template", func() error { o := owner(); o.Message = "policy ${anything}"; return o }, 401},
		{"modern explicit nil", func() error { return &modern.Error{Code: 401, Payload: nil, Cause: owner()} }, 401},
		{"cycle", func() error { return &cyclicBindingCause{} }, 401},
		{"budget", func() error {
			var e error = owner()
			for range 130 {
				e = &countedOwnerWrapper{cause: e}
			}
			return e
		}, 401},
	} {
		t.Run(tc.name, func(t *testing.T) {
			value, ok, err := p.ErrorEnvelopeWithCause("unchanged selected public text", tc.code, tc.cause())
			if err != nil || !ok {
				t.Fatal(err, ok)
			}
			status := value.(*errorEnvelopeValue).Status
			if status.Errors != nil || status.Message != "unchanged selected public text" {
				t.Fatal("unsupported verifier graph changed plain fallback")
			}
		})
	}
	for _, change := range []func(*spec.Component){func(c *spec.Component) { c.RootView.Source = nil }, func(c *spec.Component) { c.RootView.Name = "" }, func(c *spec.Component) { c.RootView.InMemory = true }, func(c *spec.Component) { c.RootView.Auxiliary = true }, func(c *spec.Component) { c.Settings = &spec.Settings{Mutation: "patch"} }, func(c *spec.Component) { c.Routes = []*spec.Route{{Handler: "example.New"}} }} {
		plan := outerViewPlan(t, reflect.TypeFor[*errorEnvelopeValue](), "Status", change)
		if s := outerViewStatus(t, plan, owner()); s.Errors != nil {
			t.Fatal("unsupported root metadata granted verifier view")
		}
	}
	missingRoot, err := (Compiler{}).Compile(CompileInput{Component: &spec.Component{Parameters: []*spec.Parameter{declaredErrorStatus("Status")}}, Type: reflect.TypeFor[*errorEnvelopeValue]()})
	if err != nil {
		t.Fatal(err)
	}
	if s := outerViewStatus(t, missingRoot, owner()); s.Errors != nil {
		t.Fatal("absent reader metadata admitted opaque verifier")
	}
	opaque := &opaqueDiagnosticStatus{code: 403}
	customAs := &opaqueDiagnosticAs{coder: opaque}
	for _, e := range []error{opaque, customAs, &bindly.BindingError{Name: "Field", Cause: &bindinput.Error{Cause: opaque}}, &bindly.BindingError{Name: "Field", Cause: &bindinput.Error{Cause: customAs}}} {
		beforeStatus, beforeError, beforeAs := opaque.statusCalls.Load(), opaque.errorCalls.Load(), customAs.calls.Load()
		value, _, err := p.ErrorEnvelopeWithCause("unchanged", 403, e)
		if err != nil || value.(*errorEnvelopeValue).Errors != nil {
			t.Fatal("unknown authority projected")
		}
		if opaque.statusCalls.Load() != beforeStatus || opaque.errorCalls.Load() != beforeError || customAs.calls.Load() != beforeAs {
			t.Fatal("new projector called unknown status/text/As")
		}
	}
}
func TestOpaqueVerifierConfiguredPrivateSentinelDoesNotEnterNewFields(t *testing.T) {
	failure := genuineVerifierFailure(t)
	p := opaqueOwnerPlan(t, reflect.TypeFor[*errorEnvelopeValue](), "Status")
	owner := &bindly.BindingError{Name: "ExternalCredential", Kind: "header", Code: 401, Err: failure}
	value, ok, err := p.ErrorEnvelopeWithCause("PRIVATE FALLBACK", 401, owner)
	if err != nil || !ok {
		t.Fatal(err, ok)
	}
	status := value.(*errorEnvelopeValue).Status
	record := status.Errors.([]*legacy.Error)[0]
	if status.Message != "Unauthorized" || record.Message != "Unauthorized" || record.Err != failure || record.View != "OrdersCase" || record.Parameter != "ExternalCredential" {
		t.Fatal("configured opaque private diagnostic influenced public fields")
	}
	if strings.Contains(fmt.Sprint(status.Message), "PRIVATE") {
		t.Fatal("private fallback reused")
	}
}

func TestOpaqueVerifierUnregisteredStatusRequiresNonblankLiteral(t *testing.T) {
	failure := genuineVerifierFailure(t)
	p := opaqueOwnerPlan(t, reflect.TypeFor[*errorEnvelopeValue](), "Status")
	for _, code := range []int{419, 499} {
		for _, message := range []string{"", " \t\n", "  safe configured credential policy  "} {
			owner := &bindly.BindingError{Name: "Credential", Kind: "header", Code: code, Message: message, Cause: failure}
			value, ok, err := p.ErrorEnvelopeWithCause("EXISTING NATIVE DIAGNOSTIC", code, owner)
			if err != nil || !ok {
				t.Fatal(err, ok)
			}
			output := value.(*errorEnvelopeValue)
			if strings.TrimSpace(message) == "" {
				if output.Errors != nil || output.Message != "EXISTING NATIVE DIAGNOSTIC" {
					t.Fatal("unregistered status acquired details or changed existing fallback")
				}
			} else {
				records, ok := output.Errors.([]*legacy.Error)
				if !ok || len(records) != 1 || records[0].Code != code || records[0].Message != message || output.Message != message || records[0].Err != failure {
					t.Fatal("explicit nonblank literal lost authority")
				}
			}
		}
	}
}

func TestOpaqueVerifierZeroCodeComponentAncestors(t *testing.T) {
	failure := genuineVerifierFailure(t)
	leaf := &bindly.BindingError{Name: "Credential", Kind: "header", Code: 401, Cause: failure}
	inner := &bindly.BindingError{Name: "Identity", Kind: "component", Cause: leaf}
	outer := &bindly.BindingError{Name: "Auth", Kind: "component", Cause: inner}
	p := opaqueOwnerPlan(t, reflect.TypeFor[*errorEnvelopeValue](), "Status")
	value, ok, err := p.ErrorEnvelopeWithCause("PRIVATE", 401, outer)
	if err != nil || !ok {
		t.Fatal(err)
	}
	status := value.(*errorEnvelopeValue).Status
	records := status.Errors.([]*legacy.Error)
	for i, want := range []string{"Auth", "Identity", "Credential"} {
		if len(records) != 1 || records[0].Parameter != want || records[0].Code != 401 || records[0].Message != "Unauthorized" {
			t.Fatal("nested owner lost")
		}
		if i == 0 && records[0].View != "OrdersCase" {
			t.Fatal("outer view lost")
		}
		if i < 2 {
			records = records[0].Object.([]*legacy.Error)
		} else if records[0].Err != failure {
			t.Fatal("private cause identity changed")
		}
	}
	for _, alter := range []func(*bindly.BindingError){func(b *bindly.BindingError) { b.Code = 401 }, func(b *bindly.BindingError) { b.Message = "authored" }, func(b *bindly.BindingError) { b.Kind = "header" }, func(b *bindly.BindingError) { b.Cause = &countedOwnerWrapper{cause: inner} }} {
		copy := *outer
		alter(&copy)
		v, _, e := p.ErrorEnvelopeWithCause("unchanged", 401, &copy)
		if e != nil || v.(*errorEnvelopeValue).Errors != nil {
			t.Fatal("unsupported ancestor admitted")
		}
	}
}
