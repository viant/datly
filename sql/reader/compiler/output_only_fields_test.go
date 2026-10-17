package compiler

import (
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
	"reflect"
	"testing"
)

func TestCompiledVisibleTransientSelectorFields(t *testing.T) {
	type row struct {
		ID       int    `sqlx:"id"`
		Total    *int   `sqlx:"-" json:"computed-total"`
		Hidden   int    `sqlx:"-" json:"-"`
		Internal int    `sqlx:"-" internal:"true"`
		Ordinary string `sqlx:"ordinary"`
		private  int
	}
	type output struct{ Data []*row }
	plan, err := Compile(Input{Component: &spec.Component{Name: "VisibleTransient", RootView: &spec.View{Name: "records", Source: &spec.ViewSource{SQL: "SELECT id FROM records"}}}, InputType: reflect.TypeFor[struct{}](), OutputType: reflect.TypeFor[output](), DirectViewField: "Data"})
	require.NoError(t, err)
	fields := map[string]data.SelectorField{}
	for _, f := range plan.Root.View.SelectorFields {
		fields[f.GoName] = f
	}
	total, ok := fields["Total"]
	require.True(t, ok)
	require.True(t, total.OutputOnly)
	require.Equal(t, "computed-total", total.PublicName)
	require.Empty(t, total.Column)
	require.Equal(t, []int{1}, total.Index)
	require.False(t, fields["ID"].OutputOnly)
	require.False(t, fields["Ordinary"].OutputOnly)
	for _, name := range []string{"Hidden", "Internal", "private"} {
		_, ok := fields[name]
		require.False(t, ok, name)
	}
	for _, c := range plan.Root.View.Columns {
		require.NotEqual(t, "Total", c.Name, "output-only metadata must not enter SQL columns")
	}
}
