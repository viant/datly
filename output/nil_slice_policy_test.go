package output

import (
	"context"
	"encoding/json"
	"reflect"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/spec"
	structjson "github.com/viant/structology/encoding/json"
)

func nilPolicyPlan(t *testing.T, typ reflect.Type, policy, casing string) *Plan {
	t.Helper()
	p, err := (Compiler{}).Compile(CompileInput{Type: typ, Component: &spec.Component{Settings: &spec.Settings{CaseFormat: casing, Output: &spec.OutputSettings{NilSlicePolicy: policy}}}})
	require.NoError(t, err)
	return p
}

func TestNilSlicePolicyValidationAndDefaultSelection(t *testing.T) {
	type value struct {
		Items []int
		Bytes []byte
	}
	for _, policy := range []string{"", "null"} {
		for _, casing := range []string{"", "lc"} {
			p := nilPolicyPlan(t, reflect.TypeFor[value](), policy, casing)
			require.Equal(t, casing != "", p.jsonEncoder != nil)
			result, err := p.Encode(context.Background(), "json", value{})
			require.NoError(t, err)
			if casing == "" {
				require.JSONEq(t, `{"Items":null,"Bytes":null}`, string(result.Data))
			} else {
				require.JSONEq(t, `{"items":null,"bytes":null}`, string(result.Data))
			}
		}
	}
	for _, policy := range []string{"typo", "EMPTY_ARRAY", " null"} {
		_, err := (Compiler{}).Compile(CompileInput{Type: reflect.TypeFor[value](), Component: &spec.Component{Settings: &spec.Settings{Output: &spec.OutputSettings{NilSlicePolicy: policy}}}})
		require.ErrorContains(t, err, "unsupported output nil slice policy")
	}
}

func TestNilSlicePolicyNativeMatrixAndConcurrency(t *testing.T) {
	type row struct {
		Names []string `json:"names"`
	}
	type output struct {
		Primitive  []int            `json:"primitive"`
		Rows       []row            `json:"rows"`
		Nested     [][]int          `json:"nested"`
		Elements   []*[]int         `json:"elements"`
		Pointer    *[]int           `json:"pointer"`
		Held       *[]int           `json:"held"`
		Map        map[string][]int `json:"map"`
		NilMap     map[string]int   `json:"nilMap"`
		MapPointer *map[string]int  `json:"mapPointer"`
		Zero       int              `json:"zero"`
	}
	var nilSlice []int
	v := &output{Nested: [][]int{nil, {}, {1}}, Elements: []*[]int{nil, &nilSlice}, Held: &nilSlice, Map: map[string][]int{"nil": nil, "empty": {}, "full": {2}}}
	p := nilPolicyPlan(t, reflect.TypeOf(v), "empty_array", "")
	require.NotNil(t, p.jsonEncoder)
	want := `{"primitive":[],"rows":[],"nested":[[],[],[1]],"elements":[null,[]],"pointer":null,"held":[],"map":{"nil":[],"empty":[],"full":[2]},"nilMap":null,"mapPointer":null,"zero":0}`
	result, err := p.Encode(context.Background(), "json", v)
	require.NoError(t, err)
	require.JSONEq(t, want, string(result.Data))
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				r, e := p.Encode(context.Background(), "json", v)
				if e != nil {
					t.Error(e)
					return
				}
				var decoded any
				if e = json.Unmarshal(r.Data, &decoded); e != nil {
					t.Error(e)
				}
			}
		}()
	}
	wg.Wait()
	require.Nil(t, v.Primitive)
	require.Nil(t, *v.Held)
	require.Nil(t, v.Rows)
	stored, err := json.Marshal(v)
	require.NoError(t, err)
	require.Contains(t, string(stored), `"primitive":null`)
	require.Contains(t, string(stored), `"held":null`)
	for _, value := range []any{[]int(nil), []int{}, []int{3}, []row(nil), []row{}, []row{{Names: nil}}} {
		plan := nilPolicyPlan(t, reflect.TypeOf(value), "empty_array", "")
		r, e := plan.Encode(context.Background(), "json", value)
		require.NoError(t, e)
		require.NotEqual(t, "null", string(r.Data))
	}
}

func TestNilSlicePolicySchemaWireAndSelection(t *testing.T) {
	type record struct {
		Items    []int            `json:"items"`
		Pointer  *[]int           `json:"pointer"`
		Nullable []int            `json:"nullable" format:"nullable=true"`
		Map      map[string][]int `json:"map"`
		Hidden   []int            `json:"hidden,omitempty"`
	}
	for _, policy := range []string{"null", "empty_array"} {
		p := nilPolicyPlan(t, reflect.TypeFor[record](), policy, "lc")
		schema, err := p.JSONSchema()
		require.NoError(t, err)
		props := schema["properties"].(map[string]any)
		items := props["items"].(map[string]any)
		if policy == "empty_array" {
			require.Equal(t, "array", items["type"])
		} else {
			require.Equal(t, []string{"array", "null"}, items["type"])
		}
		require.Equal(t, []string{"array", "null"}, props["pointer"].(map[string]any)["type"])
		require.Equal(t, []string{"array", "null"}, props["nullable"].(map[string]any)["type"])
		require.Equal(t, []string{"object", "null"}, props["map"].(map[string]any)["type"])
		wire, err := p.Wire("json")
		require.NoError(t, err)
		require.NotNil(t, wire.JSON)
		for _, property := range wire.JSON.Properties() {
			switch property.Name() {
			case "items":
				require.Equal(t, policy != "empty_array", property.Shape().Nullable())
			case "pointer", "map":
				require.True(t, property.Shape().Nullable())
			}
		}
		encoded, err := p.Encode(context.Background(), "json", record{})
		require.NoError(t, err)
		var document map[string]any
		require.NoError(t, json.Unmarshal(encoded.Data, &document))
		if policy == "null" {
			require.Nil(t, document["nullable"])
		} else {
			require.Equal(t, []any{}, document["nullable"])
		}
		require.NotContains(t, document, "hidden")
	}
	p := nilPolicyPlan(t, reflect.TypeFor[record](), "empty_array", "")
	v := record{}
	filter, err := structjson.NewFieldFilter(reflect.TypeOf(v), []structjson.FieldSelection{{Fields: []string{"Items", "Pointer"}}})
	require.NoError(t, err)
	ctx := dexec.CaptureOutputSelection(context.Background())
	scoped, complete := dexec.ScopeOutputSelection(ctx)
	dexec.BeginOutputSelection(scoped)
	dexec.PublishOutputSelection(scoped, v, filter)
	complete(v, nil)
	result, err := p.Encode(ctx, "json", v)
	require.NoError(t, err)
	require.JSONEq(t, `{"items":[],"pointer":null}`, string(result.Data))
}

// Policy-only activation selects the existing native presentation engine. Its
// byte and format-tag rules intentionally remain different from standard JSON.
func TestNilSlicePolicyNativeActivationBoundaryAndCustomPrecedence(t *testing.T) {
	type namedBytes []byte
	type embedded struct {
		Embedded []int `json:"embedded"`
	}
	type record struct {
		embedded
		Bytes  []byte            `json:"bytes"`
		Named  namedBytes        `json:"named"`
		Raw    json.RawMessage   `json:"raw"`
		Custom *stableWireOutput `json:"custom"`
		Tagged []int             `format:"name=tagged"`
	}
	value := record{Bytes: []byte{1, 2}, Named: namedBytes{3}, Raw: json.RawMessage(`{"rawSlice":null}`), Custom: &stableWireOutput{Data: stableWireResult{Name: "A"}}}
	p := nilPolicyPlan(t, reflect.TypeOf(value), "empty_array", "")
	result, err := p.Encode(context.Background(), "json", value)
	require.NoError(t, err)
	require.JSONEq(t, `{"bytes":[1,2],"named":[3],"raw":{"rawSlice":null},"custom":{"name":"A"},"tagged":[]}`, string(result.Data))
	standard := nilPolicyPlan(t, reflect.TypeOf(value), "null", "")
	result, err = standard.Encode(context.Background(), "json", value)
	require.NoError(t, err)
	require.Contains(t, string(result.Data), `"bytes":"AQI="`)
	require.Contains(t, string(result.Data), `"named":"Aw=="`)
	schema, err := p.JSONSchema()
	require.NoError(t, err)
	props := schema["properties"].(map[string]any)
	require.Equal(t, "array", props["bytes"].(map[string]any)["type"])
	require.Empty(t, props["raw"])
	require.Empty(t, props["custom"])
	custom, err := (Compiler{Lookup: func(string) (reflect.Type, error) { return reflect.TypeFor[customJSON](), nil }}).Compile(CompileInput{Type: reflect.TypeFor[struct{ Items []int }](), Component: &spec.Component{Settings: &spec.Settings{JSONMarshalType: "custom", Output: &spec.OutputSettings{NilSlicePolicy: "empty_array"}}}})
	require.NoError(t, err)
	result, err = custom.Encode(context.Background(), "json", struct{ Items []int }{})
	require.NoError(t, err)
	require.JSONEq(t, `{"custom":{"Items":null}}`, string(result.Data))
	require.True(t, custom.JSONOnly())
}

func TestNilSlicePolicyFormatsAndTabularEnvelope(t *testing.T) {
	type row struct {
		ID int `json:"id"`
	}
	type envelope struct {
		Rows  []row    `json:"rows"`
		Extra []string `json:"extra"`
	}
	value := envelope{Rows: []row{{ID: 1}}}
	for _, policy := range []string{"null", "empty_array"} {
		p := nilPolicyPlan(t, reflect.TypeOf(value), policy, "")
		require.False(t, p.JSONOnly())
		for _, format := range []string{"xls", "xlsx"} {
			rowsPlan := nilPolicyPlan(t, reflect.TypeOf(value.Rows), policy, "")
			result, err := rowsPlan.Encode(context.Background(), format, value.Rows)
			require.NoError(t, err)
			require.NotEmpty(t, result.Data)
		}
		for _, format := range []string{"csv", "xml", "tabular"} {
			result, err := p.Encode(context.Background(), format, value)
			require.NoError(t, err)
			require.NotEmpty(t, result.Data)
			if format == "tabular" {
				var decoded map[string]any
				require.NoError(t, json.Unmarshal(result.Data, &decoded))
				if policy == "null" {
					require.Nil(t, decoded["extra"])
				} else {
					require.Equal(t, []any{}, decoded["extra"])
				}
			}
		}
		filter, err := structjson.NewFieldFilter(reflect.TypeOf(value), []structjson.FieldSelection{{Fields: []string{"Rows", "Extra"}}})
		require.NoError(t, err)
		ctx := dexec.CaptureOutputSelection(context.Background())
		scoped, complete := dexec.ScopeOutputSelection(ctx)
		dexec.BeginOutputSelection(scoped)
		dexec.PublishOutputSelection(scoped, value, filter)
		complete(value, nil)
		result, err := p.Encode(ctx, "tabular", value)
		require.NoError(t, err)
		var decoded map[string]any
		require.NoError(t, json.Unmarshal(result.Data, &decoded))
		if policy == "null" {
			require.Nil(t, decoded["extra"])
		} else {
			require.Equal(t, []any{}, decoded["extra"])
		}
	}
}

func TestNilSlicePolicyRootSlicePointerAgreement(t *testing.T) {
	var nilPointer *[]int
	var nilSlice []int
	p := nilPolicyPlan(t, reflect.TypeOf(nilPointer), "empty_array", "")
	for _, tc := range []struct {
		value *[]int
		want  string
	}{
		{nilPointer, "null"}, {&nilSlice, "[]"},
	} {
		encoded, err := p.Encode(context.Background(), "json", tc.value)
		require.NoError(t, err)
		require.JSONEq(t, tc.want, string(encoded.Data))
	}
	schema, err := p.JSONSchema()
	require.NoError(t, err)
	require.Equal(t, []string{"array", "null"}, schema["type"])
	wire, err := p.Wire("json")
	require.NoError(t, err)
	require.Equal(t, reflect.Pointer, wire.JSON.Kind())
	require.True(t, wire.JSON.Nullable())
	require.Equal(t, reflect.Slice, wire.JSON.Element().Kind())
	require.False(t, wire.JSON.Element().Nullable())
	require.Nil(t, nilSlice)
	for _, policy := range []string{"", "null"} {
		standard := nilPolicyPlan(t, reflect.TypeOf(nilPointer), policy, "")
		require.Nil(t, standard.jsonEncoder)
		schema, err := standard.JSONSchema()
		require.NoError(t, err)
		require.Equal(t, []string{"array", "null"}, schema["type"])
		encoded, err := standard.Encode(context.Background(), "json", &nilSlice)
		require.NoError(t, err)
		require.Equal(t, "null", string(encoded.Data))
	}
}
