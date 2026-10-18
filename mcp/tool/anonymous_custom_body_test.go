package tool

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	bindstate "github.com/viant/bindly/state"
	"github.com/viant/datly/spec"
)

type anonymousCustomBody struct {
	Data      map[string]json.RawMessage `json:"data"`
	Has       map[string]bool            `json:"-"`
	JSONCalls int                        `json:"-"`
	XLSCalls  int                        `json:"-"`
	Raw       string                     `json:"-"`
}

func (*anonymousCustomBody) MarshalJSON() ([]byte, error) {
	panic("schema discovery must not call custom JSON")
}
func (b *anonymousCustomBody) UnmarshalJSON(raw []byte) error {
	var decoded struct {
		Data map[string]json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return err
	}
	b.Data = decoded.Data
	b.Has = map[string]bool{}
	b.JSONCalls++
	for k := range b.Data {
		b.Has[k] = true
	}
	if v, ok := b.Data["legacy_name"]; ok {
		b.Data["name"] = v
		delete(b.Data, "legacy_name")
	}
	return nil
}
func (b *anonymousCustomBody) UnmarshalXLS(_ context.Context, raw []byte) error {
	b.XLSCalls++
	b.Raw = string(raw)
	return nil
}

type anonymousCustomInput struct {
	Body *anonymousCustomBody `anonymous:"true"`
}

func TestAnonymousCustomJSONAndWholeBodyXLS(t *testing.T) {
	plan, err := NewCompiler().Compile(Input{Component: spec.Key{Kind: spec.KindComponent, Name: "import"}, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "import"}, Contract: testRouteContract(t, reflect.TypeFor[anonymousCustomInput](), []bindly.BindingSpec{{Path: "Body", Location: bindstate.Location{Kind: "body"}}})})
	require.NoError(t, err)
	properties := plan.Metadata().InputSchema.Properties
	require.Contains(t, properties, "data")
	require.Contains(t, properties, "Blob")
	for _, name := range []string{"Body", "Has", "JSONCalls", "XLSCalls", "Raw"} {
		require.NotContains(t, properties, name)
	}
	for _, tc := range []struct {
		name   string
		args   map[string]interface{}
		binary bool
	}{
		{"json", map[string]interface{}{"data": map[string]interface{}{"legacy_name": "one", "enabled": false, "count": 0, "nullable": nil}, "Has": map[string]interface{}{"forged": true}}, false},
		{"xls", map[string]interface{}{"Blob": originalBlob("workbook")}, true},
		{"blob wins", map[string]interface{}{"Blob": originalBlob("workbook"), "data": map[string]interface{}{"name": "ignored"}}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scope, err := plan.Scope(tc.args)
			require.NoError(t, err)
			defer scope.Close()
			value, found, err := scope.Body().Locate(nil).Value(t.Context(), reflect.TypeFor[anonymousCustomBody](), "")
			require.NoError(t, err)
			require.True(t, found)
			body, ok := value.(anonymousCustomBody)
			require.True(t, ok)
			if tc.binary {
				require.Equal(t, 1, body.XLSCalls)
				require.Zero(t, body.JSONCalls)
				require.Equal(t, "workbook", body.Raw)
				return
			}
			require.Equal(t, 1, body.JSONCalls)
			require.Zero(t, body.XLSCalls)
			require.JSONEq(t, `"one"`, string(body.Data["name"]))
			require.Equal(t, "false", string(body.Data["enabled"]))
			require.Equal(t, "0", string(body.Data["count"]))
			require.Equal(t, "null", string(body.Data["nullable"]))
			require.False(t, body.Has["omitted"])
			require.False(t, body.Has["forged"])
			require.True(t, body.Has["enabled"])
		})
	}
}

type anonymousSchemaBody struct {
	ID       int                  `json:"id" mcp:"name=Identifier"`
	Pointer  *int                 `json:"pointer"`
	Empty    int                  `json:"empty,omitempty"`
	Optional int                  `json:"optional" optional:"true"`
	Hidden   int                  `json:"hidden" mcp:"-"`
	Has      bool                 `json:"-"`
	Next     *anonymousSchemaBody `json:"next,omitempty"`
}
type anonymousSchemaInput struct {
	Body anonymousSchemaBody `anonymous:"true"`
}

func TestAnonymousRequiredSchemaDoesNotAddRuntimeValidation(t *testing.T) {
	plan, err := NewCompiler().Compile(Input{Component: spec.Key{Kind: spec.KindComponent, Name: "write"}, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "write"}, Contract: testRouteContract(t, reflect.TypeFor[anonymousSchemaInput](), []bindly.BindingSpec{{Path: "Body", Location: bindstate.Location{Kind: "body"}}})})
	require.NoError(t, err)
	require.Equal(t, []string{"Identifier"}, plan.Metadata().InputSchema.Required)
	require.NotContains(t, plan.Metadata().InputSchema.Properties, "hidden")
	for _, argument := range plan.Arguments() {
		require.False(t, argument.Required())
	}
	scope, err := plan.Scope(map[string]interface{}{})
	require.NoError(t, err)
	require.NoError(t, scope.Close())
}
