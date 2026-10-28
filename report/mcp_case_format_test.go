package report

import (
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"reflect"
	"testing"
)

func TestDerivedToolsInheritMCPInputFormat(t *testing.T) {
	type useCase struct{ description, name, format string }
	for _, tc := range []useCase{{"legacy default", "Spend", ""}, {"named opt in", "Spend", "lu"}, {"unnamed opt in", "", "lc"}} {
		t.Run(tc.description, func(t *testing.T) {
			source := reportSource(t, &spec.ReportSettings{Enabled: true, Compose: &spec.CubeComposeSettings{Enabled: true}})
			source.OutputType = reflect.TypeFor[reportSourceRow]()
			source.Component.RootView.Relations = nil
			source.Component.Routes[0].MCP = []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: tc.name, CaseFormat: tc.format}}
			project, err := NewProjectCompiler(ProjectConfig{Types: typecatalog.NewCatalog()}).Compile([]Source{source})
			require.NoError(t, err)
			require.Len(t, project.Derived(), 2)
			for _, derived := range project.Derived() {
				require.Equal(t, tc.format, derived.Component.Routes[0].MCP[0].CaseFormat)
			}
		})
	}
}
