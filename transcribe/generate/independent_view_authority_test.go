package generate

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
)

func TestIndependentViewQualifiedAuthority(t *testing.T) {
	const pkg = "example.com/app/records"
	for _, tc := range []struct {
		name, explicit, parameter, output string
		reject                            bool
	}{
		{name: "local alias", explicit: "rows.CurrentRow"},
		{name: "full package", explicit: pkg + ".CurrentRow"},
		{name: "bare", explicit: "CurrentRow"},
		{name: "parameter alias", parameter: "[]*rows.CurrentRow"},
		{name: "same canonical authority", explicit: "rows.CurrentRow", parameter: "[]*" + pkg + ".CurrentRow", output: "[]*CurrentRow"},
		{name: "foreign same basename", explicit: "foreign.CurrentRow", reject: true},
		{name: "unknown qualifier", explicit: "missing.CurrentRow", reject: true},
		{name: "foreign parameter", parameter: "[]*foreign.CurrentRow", reject: true},
		{name: "conflicting row", explicit: "rows.CurrentRow", parameter: "[]*rows.Other", reject: true},
		{name: "conflicting output", parameter: "[]*rows.CurrentRow", output: "[]*foreign.CurrentRow", reject: true},
		{name: "unexported", explicit: "rows.currentRow", reject: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			view := &spec.View{Name: "Current", TypeName: tc.explicit}
			parameter := &spec.Parameter{Name: "Current", Source: spec.BindSource{Kind: "view", Name: "Current"}, TypeExpr: tc.parameter, OutputTypeExpr: tc.output}
			resolver := &planResolver{plan: &Plan{Package: pkg, Imports: []spec.ImportSpec{{Alias: "rows", Package: pkg}, {Alias: "foreign", Package: "example.com/other"}}}, input: Input{Component: &spec.Component{Parameters: []*spec.Parameter{parameter}}}}
			got, err := resolver.independentViewType(view, "Current")
			if tc.reject {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, "CurrentRow", got)
			require.Equal(t, tc.explicit, view.TypeName, "authored identity must remain qualified")
		})
	}
}
