package exec

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/viant/bindly"
	bindinput "github.com/viant/bindly/input"
	"github.com/viant/tagly/jsonx"
)

type foreignWrapperMessage struct{}

func (foreignWrapperMessage) Error() string                 { return "PRIVATE" }
func (foreignWrapperMessage) PublicMessage() (string, bool) { return "PRIVATE", true }

func TestWrapperValidationMessageProvenance(t *testing.T) {
	policy, err := jsonx.Compile(reflect.StructTag(`jsonx:"wrapper=values"`), reflect.TypeFor[[]int]())
	if err != nil {
		t.Fatal(err)
	}
	_, _, native := policy.Apply([]byte(`{}`))
	if native == nil {
		t.Fatal("missing validation error")
	}
	body := func(cause error) *bindly.BindingError {
		return &bindly.BindingError{Name: "Payload", Path: "Payload", Kind: "body", Cause: &bindinput.Error{Cause: cause}}
	}
	for _, tc := range []struct {
		name   string
		err    error
		status int
		want   string
	}{
		{"native", body(native), 400, "values is required"},
		{"zero", body(&jsonx.WrapperError{}), 400, ""},
		{"nil typed", body((*jsonx.WrapperError)(nil)), 400, ""},
		{"same text", body(errors.New("values is required")), 400, ""},
		{"foreign interface", body(foreignWrapperMessage{}), 400, ""},
		{"private wrapper", body(fmt.Errorf("PRIVATE: %w", native)), 400, ""},
		{"joined cause", body(errors.Join(native, errors.New("PRIVATE"))), 400, ""},
		{"bare input", &bindinput.Error{Cause: native}, 400, ""},
		{"bare producer", native, 400, ""},
		{"outer wrapper", fmt.Errorf("PRIVATE: %w", body(native)), 400, ""},
		{"outer join", errors.Join(body(native), errors.New("PRIVATE")), 400, ""},
		{"unauthorized", body(native), 401, ""},
		{"server", body(native), 500, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := WrapperValidationMessage(tc.err, tc.status)
			if got != tc.want || ok != (tc.want != "") {
				t.Fatalf("got(%q,%v), want%q", got, ok, tc.want)
			}
		})
	}
	for _, kind := range []string{"component", "query", "header", "", "body"} {
		for _, code := range []int{0, 400, 401, 500} {
			for _, inputCode := range []int{0, 400, 401, 500} {
				owner := body(native)
				owner.Kind = kind
				owner.Code = code
				owner.Cause.(*bindinput.Error).Code = inputCode
				_, ok := WrapperValidationMessage(owner, 400)
				want := kind == "body" && (code == 0 || code == 400) && (inputCode == 0 || inputCode == 400)
				if ok != want {
					t.Errorf("kind=%s owner=%d input=%d: admitted=%v", kind, code, inputCode, ok)
				}
			}
		}
	}
	owner := body(native)
	owner.Message = "authored literal"
	if _, ok := WrapperValidationMessage(owner, 400); ok {
		t.Fatal("overrode authored policy")
	}
	if got := ErrorMessage(owner, 400); got != "authored literal" {
		t.Fatal(got)
	}
	owner.Message = ""
	if got := ErrorMessage(owner, 400); got != "values is required" {
		t.Fatal(got)
	}
	if got := ErrorMessage(owner, 500); got != "Internal Server Error" {
		t.Fatal(got)
	}
	owner.Name = ""
	if _, ok := WrapperValidationMessage(owner, 400); ok {
		t.Fatal("owner identity absent")
	}
}
