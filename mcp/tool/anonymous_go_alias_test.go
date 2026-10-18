package tool

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	bindstate "github.com/viant/bindly/state"
	"github.com/viant/datly/spec"
)

type anonymousGoAliasBody struct {
	Name    string `json:"name"`
	Enabled bool   `json:"enabled"`
	Count   int    `json:"count"`
	Values  []int  `json:"values"`
	Hidden  string `json:"hidden" internal:"true"`
	Has     bool   `json:"-"`
	Marker  bool   `json:"marker" setMarker:"true"`
}
type anonymousGoAliasInput struct {
	Body anonymousGoAliasBody `anonymous:"true"`
}

func TestAnonymousGoSpellingAndPrecedence(t *testing.T) {
	plan, err := NewCompiler(WithStrictArguments(true)).Compile(Input{Component: spec.Key{Kind: spec.KindComponent, Name: "aliases"}, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "aliases"}, Contract: testRouteContract(t, reflect.TypeFor[anonymousGoAliasInput](), []bindly.BindingSpec{{Path: "Body", Location: bindstate.Location{Kind: "body"}}})})
	require.NoError(t, err)
	props := plan.Metadata().InputSchema.Properties
	require.Contains(t, props, "name")
	require.NotContains(t, props, "Name")
	for _, n := range []string{"Hidden", "hidden", "Has", "Marker", "marker"} {
		require.NotContains(t, props, n)
		_, err := plan.Scope(map[string]any{n: true})
		require.ErrorContains(t, err, "unknown MCP argument")
	}
	for _, tc := range []struct {
		name string
		args map[string]any
		want anonymousGoAliasBody
	}{
		{"GoOnly", map[string]any{"Name": "go", "Enabled": false, "Count": 0, "Values": []int{}}, anonymousGoAliasBody{Name: "go", Values: []int{}}},
		{"JSONOnly", map[string]any{"name": "json", "enabled": true, "count": 7, "values": []int{7}}, anonymousGoAliasBody{Name: "json", Enabled: true, Count: 7, Values: []int{7}}},
		{"GoWins", map[string]any{"Name": "go", "name": "json", "Enabled": false, "enabled": true, "Count": 0, "count": 7, "Values": []int{}, "values": []int{7}}, anonymousGoAliasBody{Name: "go", Values: []int{}}},
		{"EqualBoth", map[string]any{"Name": "same", "name": "same"}, anonymousGoAliasBody{Name: "same"}},
		{"GoNilWins", map[string]any{"Values": nil, "values": []int{7}}, anonymousGoAliasBody{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			original := map[string]any{}
			for k, v := range tc.args {
				original[k] = v
			}
			scope, err := plan.Scope(tc.args)
			require.NoError(t, err)
			defer scope.Close()
			value, found, err := scope.Body().Locate(nil).Value(t.Context(), reflect.TypeFor[anonymousGoAliasBody](), "")
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, tc.want, value)
			require.Equal(t, original, tc.args)
		})
	}
}

func TestAnonymousGoAliasesKeepExplicitPolicyAndCollisions(t *testing.T) {
	t.Run("explicitNames", func(t *testing.T) {
		plan, err := NewCompiler().Compile(Input{Component: spec.Key{Kind: spec.KindComponent, Name: "explicit"}, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "explicit"}, Contract: testRouteContract(t, reflect.TypeFor[anonymousMCPInput](), []bindly.BindingSpec{{Path: "Payload", Location: bindstate.Location{Kind: "body"}}})})
		require.NoError(t, err)
		_, err = plan.Scope(map[string]any{"Name": "canonical", "legacy_name": "alias"})
		require.ErrorContains(t, err, "conflicting MCP argument")
	})
	for _, tc := range []struct {
		name   string
		fields []reflect.StructField
	}{
		{"publicName", []reflect.StructField{{Name: "Name", Type: reflect.TypeFor[string](), Tag: `json:"name"`}, {Name: "Other", Type: reflect.TypeFor[string](), Tag: `json:"Name"`}}},
		{"explicitAlias", []reflect.StructField{{Name: "Name", Type: reflect.TypeFor[string](), Tag: `json:"name"`}, {Name: "Other", Type: reflect.TypeFor[string](), Tag: `json:"other" mcp:"aliases=Name"`}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload := reflect.StructOf(tc.fields)
			input := reflect.StructOf([]reflect.StructField{{Name: "Body", Type: payload, Tag: `anonymous:"true"`}})
			_, err := NewCompiler().Compile(Input{Component: spec.Key{Kind: spec.KindComponent, Name: "collision"}, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "collision"}, Contract: testRouteContract(t, input, []bindly.BindingSpec{{Path: "Body", Location: bindstate.Location{Kind: "body"}}})})
			require.ErrorContains(t, err, "duplicate public argument")
		})
	}
}
