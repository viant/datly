package output

import (
	"encoding/json"
	"errors"
	"fmt"
	legacy "github.com/viant/xdatly/handler/response"
	"reflect"
	"strings"
	"testing"
)

type directPrivateObject struct{}

func (directPrivateObject) MarshalJSON() ([]byte, error) {
	panic("private Object must not be serialized")
}

type directLookalike struct{}

func (directLookalike) Error() string   { return "public" }
func (directLookalike) StatusCode() int { return 403 }
func TestDirectBusinessErrorFullStatusAndFreshness(t *testing.T) {
	p := nominalBusinessPlan(t, nominalBusinessComponent())
	cause := &legacy.Error{Code: 403, Message: "record denied", Err: nominalPrivateCause{}, Object: directPrivateObject{}, View: "not copied", Parameter: "not copied"}
	first := nominalBusinessOutput(t, p, cause, 403)
	if first.Message != cause.Message || !reflect.DeepEqual(first.Errors, []string{cause.Message}) {
		t.Fatal("original direct message array missing")
	}
	raw, e := json.Marshal(first)
	if e != nil {
		t.Fatal(e)
	}
	if strings.Contains(string(raw), "not copied") {
		t.Fatal("structured fields leaked")
	}
	first.Errors.([]string)[0] = "request local"
	second := nominalBusinessOutput(t, p, cause, 403)
	if !reflect.DeepEqual(second.Errors, []string{cause.Message}) {
		t.Fatal("shared array")
	}
	// The direct branch must not create authority when compilation found none.
	undeclared := nominalBusinessComponent()
	undeclared.Parameters = nil
	noAuthority := nominalBusinessPlan(t, undeclared)
	if v, ok, e := noAuthority.ErrorEnvelopeWithCause("fallback", 403, cause); e != nil || ok || v != nil {
		t.Fatal("undeclared authority admitted")
	}
}
func TestDirectBusinessErrorExclusionsRetainPlain(t *testing.T) {
	p := nominalBusinessPlan(t, nominalBusinessComponent())
	for _, tc := range []struct {
		name  string
		cause error
		code  int
	}{
		{"typed nil", (*legacy.Error)(nil), 403},
		{"zero", &legacy.Error{Message: "public"}, 403},
		{"mismatch", &legacy.Error{Code: 403, Message: "public"}, 401},
		{"server", &legacy.Error{Code: 500, Message: "public"}, 500},
		{"success", &legacy.Error{Code: 200, Message: "public"}, 200},
		{"empty", &legacy.Error{Code: 403}, 403},
		{"wrapped", fmt.Errorf("wrapper: %w", &legacy.Error{Code: 403, Message: "public"}), 403},
		{"joined", errors.Join(&legacy.Error{Code: 403, Message: "public"}), 403},
		{"lookalike", directLookalike{}, 403},
	} {
		t.Run(tc.name, func(t *testing.T) { assertNominalPlain(t, p, tc.cause, tc.code) })
	}
	// Preserve normalization when a typed nil nested detail slice reaches the helper.
	var nested []*legacy.Error
	if detailsValue(nested) != nil {
		t.Fatal("typed nil details retained in interface")
	}
}
