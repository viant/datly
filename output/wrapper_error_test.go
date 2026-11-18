package output

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/bindly"
	bindinput "github.com/viant/bindly/input"
	"github.com/viant/tagly/jsonx"
)

func TestWrapperValidationBoundedProjection(t *testing.T) {
	policy, err := jsonx.Compile(reflect.StructTag(`jsonx:"wrapper=values"`), reflect.TypeFor[[]int]())
	if err != nil {
		t.Fatal(err)
	}
	_, _, native := policy.Apply([]byte(`{}`))
	body := func(cause error) *bindly.BindingError {
		return &bindly.BindingError{Name: "Payload", Path: "Payload", Kind: "body", Cause: &bindinput.Error{Cause: cause}}
	}
	parent := func(cause error) *bindly.BindingError {
		return &bindly.BindingError{Name: "Child", Path: "Child", Kind: "component", Cause: cause}
	}
	parentKind := func(kind string) *bindly.BindingError { e := parent(body(native)); e.Kind = kind; return e }
	authoredParent := parent(body(native))
	authoredParent.Message = "authored parent"
	conflictingParent := parent(body(native))
	conflictingParent.Code = 401
	for _, tc := range []struct {
		name   string
		cause  error
		status int
		admit  bool
	}{
		{"direct", body(native), 400, true},
		{"dependency", parent(body(native)), 400, true},
		{"transparent root", fmt.Errorf("PRIVATE: %w", parent(body(native))), 400, true},
		{"wrapped input", body(fmt.Errorf("PRIVATE: %w", native)), 400, false},
		{"unowned input", &bindinput.Error{Cause: native}, 400, false},
		{"joined private", errors.Join(body(native), errors.New("PRIVATE")), 400, false},
		{"inner join", body(errors.Join(native, errors.New("PRIVATE"))), 400, false},
		{"bridge wrapper", parent(fmt.Errorf("PRIVATE: %w", body(native))), 400, false},
		{"header parent", parentKind("header"), 400, false},
		{"query parent", parentKind("query"), 400, false},
		{"internal parent", parentKind("internal"), 400, false},
		{"authored parent", authoredParent, 400, false},
		{"conflicting parent", conflictingParent, 400, false},
		{"same text", body(errors.New("values is required PRIVATE")), 400, false},
		{"wrong status", body(native), 401, false},
		{"server status", body(native), 500, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			text, ok := BindingFailureMessage(tc.cause, tc.status)
			if ok != tc.admit || (ok && text != "values is required") {
				t.Fatalf("message(%q,%v),admit=%v", text, ok, tc.admit)
			}
			entries, _, projected := projectBindingErrors(tc.cause, tc.status, "")
			if projected {
				wire, e := json.Marshal(entries)
				if e != nil {
					t.Fatal(e)
				}
				if strings.Contains(string(wire), "PRIVATE") {
					t.Fatalf("private error leaked: %s", wire)
				}
				if tc.admit && !strings.Contains(string(wire), "values is required") {
					t.Fatalf("lost public validation: %s", wire)
				}
				if !tc.admit && strings.Contains(string(wire), "values is required") {
					t.Fatalf("rejected chain disclosed wrapper message: %s", wire)
				}
			} else if tc.admit {
				t.Fatal("eligible envelope rejected")
			}
		})
	}
	owner := body(native)
	owner.Message = "authored literal"
	entries, _, ok := projectBindingErrors(owner, 400, "")
	if !ok || len(entries) != 1 || entries[0].Message != "authored literal" {
		t.Fatalf("authored precedence: %+v", entries)
	}
	if owner.Cause.(*bindinput.Error).Cause != native {
		t.Fatal("original diagnostic mutated")
	}
}
