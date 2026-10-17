package output

import (
	"context"
	"errors"
	"net/http"
	"reflect"
	"testing"

	"github.com/viant/bindly"
	bindinput "github.com/viant/bindly/input"
	"github.com/viant/bindly/provider/values"
	"github.com/viant/bindly/state"
	dauth "github.com/viant/datly/runtime/auth"
	handlerprovider "github.com/viant/datly/runtime/handler/provider"
	legacy "github.com/viant/xdatly/handler/response"
	modern "github.com/viant/xdatly/response"
)

func bindingFailureRequired(t *testing.T, code int, message string) *bindly.BindingError {
	t.Helper()
	type input struct{ Value string }
	required := true
	injector, err := bindly.NewInjector(bindly.WithProviders(values.New("header", map[string]any{})))
	if err != nil {
		t.Fatal(err)
	}
	plan, err := injector.CompilePlan(reflect.TypeFor[input](), bindly.BindingSpec{Path: "Value", Name: "Jwt", Location: state.Location{Kind: "header", In: "Authorization"}, Required: &required, ErrorCode: code, ErrorMessage: message})
	if err != nil {
		t.Fatal(err)
	}
	err = injector.Bind(t.Context(), &input{}, bindly.WithPlan(plan))
	var owner *bindly.BindingError
	if !errors.As(err, &owner) {
		t.Fatal("genuine required binding failure missing")
	}
	if text, ok := owner.RequiredAbsenceMessage(); !ok || text != "parameter Jwt is required" {
		t.Fatal("typed required-absence provenance missing")
	}
	return owner
}

func TestBindingFailureMessageRequiredAbsence(t *testing.T) {
	for _, code := range []int{400, 401, 403, 422} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			leaf := bindingFailureRequired(t, code, "")
			for _, cause := range []error{leaf, &bindly.BindingError{Name: "Auth", Kind: "component", Cause: leaf}, &countedOwnerWrapper{cause: &bindly.BindingError{Name: "Auth", Kind: "component", Cause: leaf}}} {
				message, ok := BindingFailureMessage(cause, code)
				if !ok || message != "parameter Jwt is required" {
					t.Fatalf("required absence = %q, %v", message, ok)
				}
			}
		})
	}
}

func TestBindingFailureMessageRejectsUnsupportedRequiredChains(t *testing.T) {
	for _, tc := range []struct {
		name   string
		cause  func(*testing.T) error
		status int
	}{
		{"nil", func(*testing.T) error { return nil }, 401},
		{"ordinary diagnostic", func(*testing.T) error { return errors.New("parameter Jwt is required") }, 401},
		{"nominal business", func(*testing.T) error { return &legacy.Error{Code: 401, Message: "parameter Jwt is required"} }, 401},
		{"nominal below owner", func(*testing.T) error {
			return &bindly.BindingError{Name: "Auth", Kind: "component", Cause: &legacy.Error{Code: 401, Message: "denied"}}
		}, 401},
		{"authored leaf", func(t *testing.T) error { return bindingFailureRequired(t, 401, "authored missing credential") }, 401},
		{"authored ancestor", func(t *testing.T) error {
			return &bindly.BindingError{Name: "Auth", Kind: "component", Message: "authored", Cause: bindingFailureRequired(t, 401, "")}
		}, 401},
		{"conflicting ancestor", func(t *testing.T) error {
			return &bindly.BindingError{Name: "Auth", Kind: "component", Code: 403, Cause: bindingFailureRequired(t, 401, "")}
		}, 401},
		{"selected mismatch", func(t *testing.T) error { return bindingFailureRequired(t, 401, "") }, 403},
		{"server status", func(t *testing.T) error { return bindingFailureRequired(t, 401, "") }, 500},
		{"server owner", func(t *testing.T) error { return bindingFailureRequired(t, 500, "") }, 500},
		{"success status", func(t *testing.T) error { return bindingFailureRequired(t, 401, "") }, 200},
		{"single join", func(t *testing.T) error { return errors.Join(bindingFailureRequired(t, 401, "")) }, 401},
		{"ambiguous join", func(t *testing.T) error {
			return errors.Join(bindingFailureRequired(t, 401, ""), bindingFailureRequired(t, 401, ""))
		}, 401},
		{"mixed join", func(t *testing.T) error {
			return errors.Join(bindingFailureRequired(t, 401, ""), errors.New("private"))
		}, 401},
		{"public body", func(t *testing.T) error {
			return &modern.Error{Code: 401, Payload: map[string]any{"existing": true}, Cause: bindingFailureRequired(t, 401, "")}
		}, 401},
		{"nil public body", func(t *testing.T) error { return &modern.Error{Code: 401, Cause: bindingFailureRequired(t, 401, "")} }, 401},
		{"copied owner", func(t *testing.T) error { copy := *bindingFailureRequired(t, 401, ""); return &copy }, 401},
		{"input wrapper", func(t *testing.T) error {
			return &bindinput.Error{Code: 401, Cause: bindingFailureRequired(t, 401, "")}
		}, 401},
		{"cycle", func(*testing.T) error { return &cyclicBindingCause{} }, 401},
		{"panic", func(*testing.T) error { return panickingBindingCause{} }, 401},
		{"budget", func(t *testing.T) error {
			var e error = bindingFailureRequired(t, 401, "")
			for range 130 {
				e = &countedOwnerWrapper{cause: e}
			}
			return e
		}, 401},
	} {
		t.Run(tc.name, func(t *testing.T) {
			message, ok := BindingFailureMessage(tc.cause(t), tc.status)
			if ok || message != "" {
				t.Fatalf("unsupported chain admitted %q, %v", message, ok)
			}
		})
	}
}

func TestBindingFailureMessageOpaqueVerifier(t *testing.T) {
	failure := genuineVerifierFailure(t)
	owner := func(code int, message string) *bindly.BindingError {
		return &bindly.BindingError{Name: "Credential", Kind: "header", Code: code, Message: message, Cause: failure}
	}
	for _, tc := range []struct {
		name  string
		cause error
		code  int
		want  string
	}{
		{"status text", owner(401, ""), 401, "Unauthorized"},
		{"safe literal", owner(403, "safe credential policy"), 403, "safe credential policy"},
		{"unregistered literal", owner(419, "safe credential policy"), 419, "safe credential policy"},
		{"outer transparent", &countedOwnerWrapper{cause: owner(401, "")}, 401, "Unauthorized"},
		{"component ancestors", &bindly.BindingError{Name: "Auth", Kind: "component", Cause: &bindly.BindingError{Name: "Identity", Kind: "component", Cause: owner(401, "")}}, 401, "Unauthorized"},
		{"bare", failure, 401, ""},
		{"typed nil", &bindly.BindingError{Name: "Credential", Kind: "header", Code: 401, Cause: (*dauth.VerificationFailure)(nil)}, 401, ""},
		{"zero owner", owner(0, ""), 401, ""},
		{"mismatch", owner(401, ""), 403, ""},
		{"server", owner(500, ""), 500, ""},
		{"unknown status", owner(419, ""), 419, ""},
		{"blank literal", owner(419, " \t\n"), 419, ""},
		{"template", owner(401, "policy ${error}"), 401, ""},
		{"other interpolation", owner(401, "policy ${anything}"), 401, ""},
		{"input bridge", &bindly.BindingError{Name: "Credential", Kind: "header", Code: 401, Cause: &bindinput.Error{Code: 401, Cause: failure}}, 401, ""},
		{"transparent bridge", &bindly.BindingError{Name: "Credential", Kind: "header", Code: 401, Cause: &countedOwnerWrapper{cause: failure}}, 401, ""},
		{"explicit ancestor", &bindly.BindingError{Name: "Auth", Kind: "component", Code: 401, Cause: owner(401, "")}, 401, ""},
		{"authored ancestor", &bindly.BindingError{Name: "Auth", Kind: "component", Message: "authored", Cause: owner(401, "")}, 401, ""},
		{"wrong owner kind", &bindly.BindingError{Name: "Credential", Kind: "component", Code: 401, Cause: failure}, 401, ""},
		{"joined", errors.Join(owner(401, "")), 401, ""},
		{"public body", &modern.Error{Code: 401, Payload: "existing", Cause: owner(401, "")}, 401, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			message, ok := BindingFailureMessage(tc.cause, tc.code)
			if message != tc.want || ok != (tc.want != "") {
				t.Fatalf("verifier message = %q, %v; want %q", message, ok, tc.want)
			}
		})
	}
}

func TestBindingFailureMessageDoesNotInvokeOpaqueAuthority(t *testing.T) {
	coder := &opaqueDiagnosticStatus{code: 401}
	customAs := &opaqueDiagnosticAs{coder: coder}
	for _, cause := range []error{coder, customAs, &bindly.BindingError{Name: "Credential", Kind: "header", Code: 401, Cause: coder}, &bindly.BindingError{Name: "Credential", Kind: "header", Code: 401, Cause: customAs}} {
		message, ok := BindingFailureMessage(cause, 401)
		if ok || message != "" {
			t.Fatal("unknown authority admitted")
		}
	}
	if coder.errorCalls.Load() != 0 || coder.statusCalls.Load() != 0 || customAs.calls.Load() != 0 {
		t.Fatal("opaque error/status/As invoked")
	}
}

func TestBindingFailureMessageRejectsActualResolutionGroup(t *testing.T) {
	type input struct {
		Seed          string
		Body          *struct{ ID int }
		First, Second int
	}
	leaf := bindingFailureRequired(t, 401, "")
	injector, err := bindly.NewInjector(bindly.WithProviders(
		handlerprovider.Named("component", func(context.Context, reflect.Type, string) (any, bool, error) { return nil, false, leaf }),
		values.New("header", map[string]any{"X-Seed": "present"}),
		handlerprovider.Named("body", func(context.Context, reflect.Type, string) (any, bool, error) { return &struct{ ID int }{1}, true, nil }),
	))
	if err != nil {
		t.Fatal(err)
	}
	required := true
	specs := []bindly.BindingSpec{
		{Path: "Seed", Name: "Seed", Location: state.Location{Kind: "header", In: "X-Seed"}, Required: &required},
		{Path: "Body", Name: "Body", Location: state.Location{Kind: "body"}, Required: &required},
	}
	for _, name := range []string{"First", "Second"} {
		specs = append(specs, bindly.BindingSpec{Path: name, Name: name, Location: state.Location{Kind: "component", In: name}, ResolutionGroup: &bindly.ResolutionGroupSpec{Name: "records", After: []string{"Seed", "Body"}}})
	}
	plan, err := injector.CompilePlan(reflect.TypeFor[input](), specs...)
	if err != nil {
		t.Fatal(err)
	}
	target := &input{}
	run := &bindingGroupRun{observed: make(chan string, 2)}
	cause := injector.Bind(t.Context(), target, bindly.WithPlan(plan), bindly.WithSource(target), bindly.WithResolutionGroupController(bindingGroupController{run}))
	var group *bindly.ResolutionGroupError
	if !errors.As(cause, &group) || !run.closed || len(group.Failures()) != 2 {
		t.Fatal("actual resolution group not exercised")
	}
	if message, ok := BindingFailureMessage(cause, 401); ok || message != "" {
		t.Fatal("ambiguous resolution group admitted")
	}
}
