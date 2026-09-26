package openapi_test

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	gatewayhttp "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/gateway/openapi"
	"github.com/viant/datly/gateway/openapi/openapi3"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	xshape "github.com/viant/x/shape"
)

type PlainCommon struct {
	Value  string
	Unique int
}
type PlainLeft struct{ PlainCommon }
type PlainRight struct{ PlainCommon }
type PlainTagged struct {
	Different string `json:"Value"`
}
type PlainHolder struct {
	Name   string
	Number int `json:",string"`
}
type plainPrivate struct {
	Public  string
	private string
}
type plainZero int

func (z plainZero) IsZero() bool { return z == 7 }

type plainPointer *int
type plainBytes []byte

type plainQuoted struct {
	Int    int             `json:"int,string"`
	Uint   uint64          `json:"uint,string"`
	Float  float64         `json:"float,string"`
	Bool   bool            `json:"bool,string"`
	Text   string          `json:"text,string"`
	Ptr    *int            `json:"ptr,string"`
	Deep   **int           `json:"deep,string"`
	Named  plainPointer    `json:"named,string"`
	Array  []int           `json:"array,string"`
	Object struct{ V int } `json:"object,string"`
	Bytes  plainBytes      `json:"bytes"`
}

type plainProof struct {
	t   *testing.T
	doc *openapi3.OpenAPI
}

func (p plainProof) schema(s *openapi3.Schema) *openapi3.Schema {
	if len(s.AnyOf) > 0 {
		s = s.AnyOf[0]
	}
	if s.Ref != "" {
		s = p.doc.Components.Schemas[s.Ref[len("#/components/schemas/"):]]
	}
	return s
}
func (p plainProof) validate(s *openapi3.Schema, value any) bool {
	if s.Ref != "" {
		return p.validate(p.schema(s), value)
	}
	if len(s.AnyOf) > 0 {
		for _, branch := range s.AnyOf {
			if p.validate(branch, value) {
				return true
			}
		}
		return false
	}
	if s.Type == "" {
		return true
	}
	if value == nil {
		return s.Nullable
	}
	if len(s.Enum) > 0 {
		for _, allowed := range s.Enum {
			if reflect.DeepEqual(allowed, value) {
				return true
			}
		}
		return false
	}
	switch s.Type {
	case "object":
		obj, ok := value.(map[string]any)
		if !ok {
			return false
		}
		for _, name := range s.Required {
			if _, ok := obj[name]; !ok {
				return false
			}
		}
		for name, child := range obj {
			if property := s.Properties[name]; property != nil {
				if !p.validate(property, child) {
					p.t.Logf("wire/schema mismatch at %q: value=%#v schema=%+v", name, child, p.schema(property))
					return false
				}
			} else if s.AdditionalProperties != nil {
				if !p.validate(s.AdditionalProperties, child) {
					return false
				}
			} else {
				return false
			}
		}
	case "array":
		array, ok := value.([]any)
		if !ok {
			return false
		}
		if uint64(len(array)) < s.MinItems || s.MaxItems != nil && uint64(len(array)) > *s.MaxItems {
			return false
		}
		for _, child := range array {
			if !p.validate(s.Items, child) {
				return false
			}
		}
	case "string":
		_, ok := value.(string)
		return ok
	case "integer", "number":
		_, ok := value.(float64)
		return ok
	case "boolean":
		_, ok := value.(bool)
		return ok
	}
	return true
}

func TestPlainJSONWireMatchesStandardHTTP(t *testing.T) {
	zero := 0
	pointer := &zero
	type omitted struct {
		*PlainHolder
		Fixed   [2]int          `json:"fixed,omitempty"`
		Empty   [0]int          `json:"empty,omitempty"`
		Object  struct{ N int } `json:"object,omitempty"`
		Pointer *int            `json:"pointer,omitempty"`
		Zero    plainZero       `json:"zero,omitzero"`
		Both    plainZero       `json:"both,omitempty,omitzero"`
	}
	collisionType, err := (xshape.Runtime{}).Struct([]xshape.RuntimeField{{Name: "A", Type: reflect.TypeFor[string](), Tag: `json:"same"`}, {Name: "B", Type: reflect.TypeFor[int](), Tag: `json:"same"`}})
	require.NoError(t, err)
	collision := reflect.New(collisionType).Interface()
	tests := []struct {
		name            string
		value           any
		names, required []string
		types           map[string]string
	}{
		{name: "duplicate direct names disappear", value: collision},
		{name: "repeated embedded type disappears", value: struct {
			PlainLeft
			PlainRight
		}{PlainLeft{PlainCommon{"left", 1}}, PlainRight{PlainCommon{"right", 2}}}},
		{name: "tag wins same depth", value: struct {
			PlainCommon
			PlainTagged
		}{PlainCommon{"plain", 1}, PlainTagged{"tagged"}}, names: []string{"Unique", "Value"}, required: []string{"Unique", "Value"}},
		{name: "shallow untagged wins", value: struct {
			PlainTagged
			Value string
		}{PlainTagged{"deep"}, "shallow"}, names: []string{"Value"}, required: []string{"Value"}},
		{name: "private embedding public children", value: struct{ plainPrivate }{plainPrivate{"visible", "hidden"}}, names: []string{"Public"}, required: []string{"Public"}},
		{name: "named anonymous object", value: struct {
			*PlainHolder `json:"holder"`
		}{nil}, names: []string{"holder"}, required: []string{"holder"}},
		{name: "nil pointer promoted children optional", value: struct{ *PlainHolder }{nil}, names: []string{"Name", "Number"}},
		{name: "pointer promoted values", value: struct{ *PlainHolder }{&PlainHolder{"name", 0}}, names: []string{"Name", "Number"}, types: map[string]string{"Number": "string"}},
		{name: "omission zero method", value: omitted{Pointer: pointer, Zero: 7, Both: 7}, names: []string{"Name", "Number", "fixed", "empty", "object", "pointer", "zero", "both"}, required: []string{"fixed", "object"}},
		{name: "all quoted scalar zero", value: plainQuoted{Ptr: pointer, Deep: &pointer, Named: plainPointer(pointer), Text: "a\"b<>&", Bytes: plainBytes{1, 2}}, names: []string{"int", "uint", "float", "bool", "text", "ptr", "deep", "named", "array", "object", "bytes"}, required: []string{"int", "uint", "float", "bool", "text", "ptr", "deep", "named", "array", "object", "bytes"}, types: map[string]string{"int": "string", "uint": "string", "float": "string", "bool": "string", "text": "string", "ptr": "string", "deep": "integer", "array": "array", "object": "object", "bytes": "string"}},
		{name: "quoted null pointers", value: plainQuoted{}, names: []string{"int", "uint", "float", "bool", "text", "ptr", "deep", "named", "array", "object", "bytes"}, required: []string{"int", "uint", "float", "bool", "text", "ptr", "deep", "named", "array", "object", "bytes"}},
		{name: "stdlib ignores format and internal tags", value: struct {
			Label    string `json:"a=b" format:"name=ignored"`
			Internal string `internal:"true"`
			Hidden   string `json:"-"`
		}{"<plain>", "kept", "hidden"}, names: []string{"a=b", "Internal"}, required: []string{"a=b", "Internal"}},
		{name: "integer map keys", value: struct {
			Map map[int]*string `json:"map"`
		}{map[int]*string{7: nil}}, names: []string{"map"}, required: []string{"map"}},
		{name: "any standard JSON", value: struct {
			Value any `json:"value"`
		}{map[string]any{"nested": []any{nil, 3, "s"}}}, names: []string{"value"}, required: []string{"value"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			typ := reflect.TypeOf(tc.value)
			if typ.Kind() == reflect.Pointer {
				typ = typ.Elem()
			}
			entry := (fixture{output: typ, execute: func(context.Context, handler.Invocation) (any, error) { return tc.value, nil }}).registration(t)
			// No source metadata can switch a compiled plain output to another policy.
			entry.Component.Settings = &spec.Settings{CaseFormat: "lc", Output: &spec.OutputSettings{OmitEmpty: true}}
			document, err := (openapi.Generator{}).Generate(context.Background(), documentRequest(entry))
			require.NoError(t, err)
			proof := plainProof{t, document}
			responseSchema := document.Paths["/records"].Post.Responses["200"].Content["application/json"].Schema
			root := proof.schema(responseSchema)
			require.Len(t, root.Properties, len(tc.names))
			for _, name := range tc.names {
				require.Contains(t, root.Properties, name)
			}
			require.ElementsMatch(t, tc.required, root.Required)
			for name, kind := range tc.types {
				require.Equal(t, kind, proof.schema(root.Properties[name]).Type, name)
			}
			oracle, err := json.Marshal(tc.value)
			require.NoError(t, err)
			encoded, err := entry.Output.Encode(context.Background(), "json", tc.value)
			require.NoError(t, err)
			require.Equal(t, oracle, encoded.Data, "plain encoder bytes must not change")
			rt, err := druntime.NewRuntime([]*registry.RegisteredComponent{entry})
			require.NoError(t, err)
			recorder := httptest.NewRecorder()
			gatewayhttp.NewHandler(rt, nil, "test").ServeHTTP(recorder, httptest.NewRequest("POST", "/records", nil))
			require.Equal(t, 200, recorder.Code, recorder.Body.String())
			require.Equal(t, oracle, recorder.Body.Bytes())
			var value any
			require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &value))
			require.True(t, proof.validate(responseSchema, value), string(oracle))
			if tc.name == "all quoted scalar zero" {
				// The active encoding/json implementation is the wire oracle:
				// json/v2 stringifies this named pointer, while JSON v1 did not.
				switch value.(map[string]any)["named"].(type) {
				case string:
					require.Equal(t, "string", root.Properties["named"].Type)
				case float64:
					require.Equal(t, "integer", root.Properties["named"].Type)
				default:
					t.Fatalf("unexpected named pointer JSON value %#v", value.(map[string]any)["named"])
				}
				require.Equal(t, "byte", root.Properties["bytes"].Format)
				require.Contains(t, string(oracle), `"ptr":"0"`)
				require.Contains(t, string(oracle), `"bytes":"AQI="`)
			}
		})
	}
}
