package output

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
)

type stableWireResult struct {
	Name string `json:"name"`
}
type stableWireOutput struct{ Data stableWireResult }

func (o *stableWireOutput) MarshalJSON() ([]byte, error) { return json.Marshal(o.Data) }
func (*stableWireOutput) JSONWireType() reflect.Type     { return reflect.TypeFor[stableWireResult]() }

func TestCustomJSONDeclaresStableWireType(t *testing.T) {
	plan, err := (Compiler{}).Compile(CompileInput{Type: reflect.TypeFor[stableWireOutput]()})
	require.NoError(t, err)
	wire, err := plan.Wire("json")
	require.NoError(t, err)
	require.Equal(t, reflect.TypeFor[stableWireResult](), wire.Type)
	encoded, err := plan.Encode(context.Background(), "json", &stableWireOutput{Data: stableWireResult{Name: "Ada"}})
	require.NoError(t, err)
	require.JSONEq(t, `{"name":"Ada"}`, string(encoded.Data))
	for _, format := range []string{"csv", "xml", "xlsx", "tabular"} {
		_, err = plan.Wire(format)
		require.ErrorContains(t, err, "custom JSON output")
		_, err = plan.Encode(context.Background(), format, &stableWireOutput{Data: stableWireResult{Name: "Ada"}})
		require.ErrorContains(t, err, "custom JSON output")
	}
	_, err = (Compiler{}).Compile(CompileInput{Type: reflect.TypeFor[stableWireOutput](),
		Component: &spec.Component{Settings: &spec.Settings{Format: "csv"}}})
	require.ErrorContains(t, err, "custom JSON output cannot use csv")
	_, err = (Compiler{}).Compile(CompileInput{Type: reflect.TypeFor[stableWireOutput](),
		Component: &spec.Component{Routes: []*spec.Route{{Method: "GET", Path: "/records", Marshaller: "xml"}}}})
	require.ErrorContains(t, err, "custom JSON output cannot use xml")
}

func TestWireUsesCompiledPresentation(t *testing.T) {
	type record struct {
		ID     int    `json:"id"`
		Detail string `json:"detail"`
	}
	for _, tc := range []struct {
		name     string
		settings *spec.Settings
	}{
		{"plain", nil},
		{"exclude", &spec.Settings{Output: &spec.OutputSettings{Exclude: []string{"Detail"}}}},
		{"omit", &spec.Settings{Output: &spec.OutputSettings{OmitEmpty: true}}},
		{"case", &spec.Settings{CaseFormat: "lc"}},
		{"date", &spec.Settings{DateFormat: "yyyy-MM-dd"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			component := &spec.Component{Settings: tc.settings}
			plan, err := (Compiler{}).Compile(CompileInput{Component: component, Type: reflect.TypeFor[record]()})
			require.NoError(t, err)
			component.Settings = nil
			wire, err := plan.Wire("")
			require.NoError(t, err)
			require.Equal(t, reflect.TypeFor[record](), wire.Type)
			require.Equal(t, "application/json", wire.ContentType)
			if tc.name != "plain" {
				require.NotNil(t, wire.JSON)
			}
			encoded, err := plan.Encode(context.Background(), "", record{ID: 7, Detail: "value"})
			require.NoError(t, err)
			if tc.name == "exclude" {
				require.JSONEq(t, `{"id":7}`, string(encoded.Data))
			}
		})
	}
}

func TestWireMediaUsesEncodingOwner(t *testing.T) {
	type record struct {
		ID     int    `json:"id" csvName:"id"`
		Detail string `json:"detail" csvName:"detail"`
	}
	type rows struct {
		Data []record `json:"data"`
	}
	plan, err := (Compiler{}).Compile(CompileInput{Type: reflect.TypeFor[rows](), Component: &spec.Component{Settings: &spec.Settings{Format: "csv", Output: &spec.OutputSettings{Exclude: []string{"Detail"}}}}})
	require.NoError(t, err)
	wire, err := plan.Wire("")
	require.NoError(t, err)
	require.Equal(t, reflect.TypeFor[string](), wire.Type)
	require.Equal(t, "text/csv", wire.ContentType)
	encoded, err := plan.Encode(context.Background(), "", rows{Data: []record{{7, "private"}}})
	require.NoError(t, err)
	require.NotContains(t, string(encoded.Data), "private")
	wire, err = plan.Wire("json")
	require.NoError(t, err)
	require.NotNil(t, wire.JSON)
	wire, err = plan.Wire("xlsx")
	require.NoError(t, err)
	require.True(t, wire.Binary)
	plain, err := (Compiler{}).Compile(CompileInput{Type: reflect.TypeFor[record]()})
	require.NoError(t, err)
	_, err = plain.Wire("csv")
	require.ErrorContains(t, err, "requires typed rows")
}

func TestOutputFormatNamesAndNullsMatchWire(t *testing.T) {
	type record struct {
		Name  string `format:"name=CustomerName"`
		Exact string `json:"Exact_Name" format:"name=IgnoredName"`
		Note  *string
	}
	type envelope struct{ Records []record }
	plan, err := (Compiler{}).Compile(CompileInput{
		Type:      reflect.TypeFor[envelope](),
		Component: &spec.Component{Settings: &spec.Settings{CaseFormat: "lc"}},
	})
	require.NoError(t, err)
	encoded, err := plan.Encode(context.Background(), "json", envelope{Records: []record{{Name: "Ada", Exact: "fixed"}}})
	require.NoError(t, err)
	require.JSONEq(t, `{"records":[{"customerName":"Ada","Exact_Name":"fixed","note":null}]}`, string(encoded.Data))
	wire, err := plan.Wire("json")
	require.NoError(t, err)
	require.NotNil(t, wire.JSON)
	holders := wire.JSON.Properties()
	require.Len(t, holders, 1)
	require.Equal(t, "records", holders[0].Name())
	fields := holders[0].Shape().Element().Properties()
	require.Len(t, fields, 3)
	require.Equal(t, "customerName", fields[0].Name())
	require.Equal(t, "Exact_Name", fields[1].Name())
	require.Equal(t, "note", fields[2].Name())
	require.True(t, fields[2].Shape().Nullable())
}
