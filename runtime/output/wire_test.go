package output

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
)

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
