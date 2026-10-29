package compiler

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/viant/bindly"
	"github.com/viant/datly/spec"
	"reflect"
	"testing"
	"time"
)

type schemaDateLeaf struct {
	When *time.Time `json:"when"`
}
type schemaDateBody struct {
	Strict schemaDateLeaf `json:"strict"`
	Legacy schemaDateLeaf `json:"legacy"`
	Hidden *time.Time     `json:"hidden" internal:"true"`
}

func TestBodyWireDateTimeSchemaPreservesLinkedTypeAndMemberScope(t *testing.T) {
	field := reflect.StructField{Name: "Filters", Type: reflect.TypeFor[schemaDateBody]()}
	binding := bindly.BindingSpec{Extension: &spec.Parameter{WireSchemas: map[string]*spec.WireSchema{"Filters.strict.when": {Type: "string", Format: "date-time"}, "Filters.hidden": {Type: "string", Format: "date-time"}}}}
	binding.Location.Kind = "body"
	if err := applyBodyFormat(field, &binding); err != nil {
		t.Fatal(err)
	}
	if binding.Transformer == nil {
		t.Fatal("schema date validation missing")
	}
	for _, tc := range []struct {
		raw     string
		invalid bool
	}{{`{"strict":{"when":"not-a-date"}}`, true}, {`{"strict":{"when":"2026-10-08"}}`, true}, {`{"strict":{"when":"2026-10-08T00:00:00Z"},"legacy":{"when":"nonsense"},"hidden":"not-a-date"}`, false}, {`{"strict":{"when":null}}`, false}} {
		actual, err := binding.Transformer.Transform(context.Background(), nil, json.RawMessage(tc.raw))
		if (err != nil) != tc.invalid {
			t.Fatalf("%s err=%v cause=%v", tc.raw, err, errors.Unwrap(err))
		}
		if err == nil && reflect.TypeOf(actual) != field.Type {
			t.Fatalf("changed linked type: %T", actual)
		}
	}
	empty := bindly.BindingSpec{}
	empty.Location.Kind = "body"
	if err := applyBodyFormat(field, &empty); err != nil {
		t.Fatal(err)
	}
	if empty.Transformer != nil {
		t.Fatal("changed undeclared legacy dates")
	}
}
func TestBodyWireDateTimeSchemaRespectsAuthoredFormat(t *testing.T) {
	type record struct {
		When *time.Time `json:"when" format:"dateFormat=YYYY-MM-DD"`
	}
	field := reflect.StructField{Name: "Body", Type: reflect.TypeFor[record]()}
	binding := bindly.BindingSpec{Extension: &spec.Parameter{WireSchemas: map[string]*spec.WireSchema{"Body.when": {Type: "string", Format: "date-time"}}}}
	binding.Location.Kind = "body"
	if err := applyBodyFormat(field, &binding); err != nil {
		t.Fatal(err)
	}
	actual, err := binding.Transformer.Transform(context.Background(), nil, json.RawMessage(`{"when":"2026-10-08"}`))
	if err != nil {
		t.Fatal(err)
	}
	if actual.(record).When.Format("2006-01-02") != "2026-10-08" {
		t.Fatal("authored layout lost")
	}
}

func TestBodyWireSchemaUsesJSONMemberIdentity(t *testing.T) {
	type body struct {
		Target *time.Time `json:"legacy"`
		Other  *time.Time `json:"Target"`
	}
	field := reflect.StructField{Name: "Body", Type: reflect.TypeFor[body]()}
	binding := bindly.BindingSpec{Extension: &spec.Parameter{WireSchemas: map[string]*spec.WireSchema{"Body.Target": {Type: "string", Format: "date-time"}}}}
	binding.Location.Kind = "body"
	if err := applyBodyFormat(field, &binding); err != nil {
		t.Fatal(err)
	}
	if binding.Transformer == nil {
		t.Fatal("transformer absent")
	}
	for _, tc := range []struct {
		raw     string
		invalid bool
	}{{`{"Target":"2026-10-08"}`, true}, {`{"legacy":"2026-10-08"}`, false}} {
		_, err := binding.Transformer.Transform(context.Background(), nil, json.RawMessage(tc.raw))
		if (err != nil) != tc.invalid {
			t.Errorf("wire schema affected wrong public field for %s: err=%v wantInvalid=%v", tc.raw, err, tc.invalid)
		}
	}
}

func TestBodyWireSchemaTypedNilExtensionRemainsLegacy(t *testing.T) {
	type body struct {
		When *time.Time `json:"when"`
	}
	field := reflect.StructField{Name: "Body", Type: reflect.TypeFor[body]()}
	var parameter *spec.Parameter
	binding := bindly.BindingSpec{Extension: parameter}
	binding.Location.Kind = "body"
	if err := applyBodyFormat(field, &binding); err != nil {
		t.Fatal(err)
	}
}
