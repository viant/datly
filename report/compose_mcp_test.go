package report

import (
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"reflect"
	"testing"
)

func TestExplicitComposeMCPConvergenceAndSourceIndependence(t *testing.T) {
	strict := true
	exposure := &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "compose_spend", Description: "Quoted, \"text\"\nline", DescriptionPath: "docs/compose.md", CaseFormat: "uc", StrictArguments: &strict}
	source := reportSource(t, &spec.ReportSettings{Enabled: true, Compose: &spec.CubeComposeSettings{Enabled: true, MCP: exposure}})
	source.OutputType = reflect.TypeFor[reportSourceRow]()
	source.Component.RootView.Relations = nil
	for _, sourceName := range []string{"get_spend", "renamed_source"} {
		source.Component.Routes[0].MCP = []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: sourceName}}
		project, err := NewProjectCompiler(ProjectConfig{Types: typecatalog.NewCatalog()}).Compile([]Source{source})
		require.NoError(t, err)
		actual := project.Derived()[1].Component
		require.Equal(t, exposure, actual.Routes[0].MCP[0])
		require.NotSame(t, exposure, actual.Routes[0].MCP[0])
		require.NotSame(t, exposure.StrictArguments, actual.Routes[0].MCP[0].StrictArguments)
		require.Equal(t, exposure, ProjectedComponents(source.Component)[1].MCP)
		require.Equal(t, "SpendCubeCompose", actual.Key.Name)
		require.Equal(t, sourceName, source.Component.Routes[0].MCP[0].Name)
	}
	disabled := false
	source.Component.Settings.Report.Compose.MCPTool = &disabled
	project, err := NewProjectCompiler(ProjectConfig{Types: typecatalog.NewCatalog()}).Compile([]Source{source})
	require.NoError(t, err)
	require.Empty(t, project.Derived()[1].Component.Routes[0].MCP)
}
