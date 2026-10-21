package generate

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/output"
	"github.com/viant/datly/spec"
	dtag "github.com/viant/datly/tag"
)

func TestBodyJSONNamesPreserveOptionsAndNativePresentation(t *testing.T) {
	p := &Plan{Settings: dtag.Settings{Mutation: "patch", CaseFormat: "lc"}, Generation: &spec.GenerationSettings{}}
	fields := []Field{{Name: "ZeroCount"}, {Name: "Enabled"}, {Name: "OptionalZero", Tag: `json:",omitempty"`}, {Name: "Alias", Tag: `json:"legacy_name,omitempty"`}, {Name: "Metadata"}, {Name: "Children", RelationHolder: true}, {Name: "ExtraColumn"}}
	require.NoError(t, applyBodyJSONTags(p, fields, &spec.View{}))
	types := []reflect.Type{reflect.TypeFor[int](), reflect.TypeFor[bool](), reflect.TypeFor[*int](), reflect.TypeFor[string](), reflect.TypeFor[map[string]any](), reflect.TypeFor[[]string](), reflect.TypeFor[string]()}
	fs := []reflect.StructField{}
	for i, f := range fields {
		fs = append(fs, reflect.StructField{Name: f.Name, Type: types[i], Tag: reflect.StructTag(f.Tag)})
		if i != 3 {
			require.Empty(t, strings.Split(reflect.StructTag(f.Tag).Get("json"), ",")[0])
		}
	}
	typ := reflect.StructOf(fs)
	row := reflect.New(typ)
	zero := 0
	row.Elem().Field(2).Set(reflect.ValueOf(&zero))
	row.Elem().Field(4).Set(reflect.ValueOf(map[string]any{"APIKey": true}))
	row.Elem().Field(5).Set(reflect.ValueOf([]string{}))
	raw, e := json.Marshal(row.Interface())
	require.NoError(t, e)
	require.JSONEq(t, `{"ZeroCount":0,"Enabled":false,"OptionalZero":0,"Metadata":{"APIKey":true},"Children":[],"ExtraColumn":""}`, string(raw))
	plan, e := (output.Compiler{}).Compile(output.CompileInput{Type: typ, Component: &spec.Component{Settings: &spec.Settings{CaseFormat: "lc"}}})
	require.NoError(t, e)
	encoded, e := plan.Encode(context.Background(), "json", row.Interface())
	require.NoError(t, e)
	require.JSONEq(t, `{"zeroCount":0,"enabled":false,"optionalZero":0,"metadata":{"APIKey":true},"children":[],"extraColumn":""}`, string(encoded.Data))
	reader := []Field{{Name: "ZeroCount"}}
	require.NoError(t, applyInferredJSONTags(p, reader))
	require.Equal(t, "zeroCount", reflect.StructTag(reader[0].Tag).Get("json"))
}
