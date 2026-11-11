package output

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
)

func TestWithCaseFormatImmutableRecursivePlan(t *testing.T) {
	type child struct {
		MetricID int
		Explicit string `json:"Exact_Name"`
		Hidden   string
	}
	type result struct {
		ChildRows []child
		EmptyRows []child
	}
	component := &spec.Component{Settings: &spec.Settings{CaseFormat: "uc", Output: &spec.OutputSettings{Exclude: []string{"ChildRows.Hidden"}, NilSlicePolicy: "empty_array"}}}
	original, err := (Compiler{}).Compile(CompileInput{Component: component, Type: reflect.TypeFor[result](), DataField: "ChildRows", SelectableRows: true})
	require.NoError(t, err)
	adapted, err := original.WithCaseFormat("lc")
	require.NoError(t, err)
	require.NotSame(t, original, adapted)
	require.Equal(t, "uc", component.Settings.CaseFormat)
	require.Equal(t, "uc", original.compileInput.Component.Settings.CaseFormat)
	require.Equal(t, "ChildRows", adapted.compileInput.DataField)
	require.True(t, adapted.compileInput.SelectableRows)
	value := result{ChildRows: []child{{MetricID: 9, Explicit: "exact", Hidden: "private"}}}
	encoded, err := adapted.Encode(context.Background(), "json", value)
	require.NoError(t, err)
	require.JSONEq(t, `{"childRows":[{"metricId":9,"Exact_Name":"exact"}],"emptyRows":[]}`, string(encoded.Data))
	old, err := original.Encode(context.Background(), "json", value)
	require.NoError(t, err)
	require.Contains(t, string(old.Data), `"ChildRows"`)
	same, err := original.WithCaseFormat("")
	require.NoError(t, err)
	require.Same(t, original, same)
	_, err = original.WithCaseFormat("invalid-format")
	require.Error(t, err)
}

func TestWithCaseFormatPreservesEffectiveProjectionAndOpaqueEncoder(t *testing.T) {
	type source struct {
		PublicID int
		Secret   string
	}
	base, err := (Compiler{}).Compile(CompileInput{Type: reflect.TypeFor[source]()})
	require.NoError(t, err)
	projected := reflect.StructOf([]reflect.StructField{{Name: "PublicID", Type: reflect.TypeFor[int]()}, {Name: "Secret", Type: reflect.TypeFor[string](), Tag: `json:"-"`}})
	effective, err := base.WithType(projected)
	require.NoError(t, err)
	camel, err := effective.WithCaseFormat("lc")
	require.NoError(t, err)
	encoded, err := camel.Encode(context.Background(), "json", source{PublicID: 3, Secret: "private"})
	require.NoError(t, err)
	require.JSONEq(t, `{"publicId":3}`, string(encoded.Data))
	require.Equal(t, reflect.TypeFor[source](), camel.effectiveSource)
	custom, err := (Compiler{Lookup: func(string) (reflect.Type, error) { return reflect.TypeFor[configuredStandardJSON](), nil }}).Compile(CompileInput{Type: reflect.TypeFor[source](), Component: &spec.Component{Settings: &spec.Settings{JSONMarshalType: "custom"}}})
	require.NoError(t, err)
	same, err := custom.WithCaseFormat("lc")
	require.NoError(t, err)
	require.Same(t, custom, same, "custom-owned wire bytes are not rewritten")
}
