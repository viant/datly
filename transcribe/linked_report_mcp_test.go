package transcribe

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly/resource"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/report"
	gen "github.com/viant/datly/transcribe/generate"
	"github.com/viant/datly/transcribe/testdata/linkedgrouping"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
)

// Keep a real linked holder in the authoring path: unlinked DQL bypasses the
// package-settings merge and cannot detect a dropped report exposure.
func TestLinkedReportMCPExposureTwoPass(t *testing.T) {
	const pkg = "github.com/viant/datly/transcribe/testdata/linkedgrouping"
	const text = `#package('github.com/viant/datly/transcribe/testdata/linkedgrouping')
#import('linked','github.com/viant/datly/transcribe/testdata/linkedgrouping')
#setting($_ = $route('/linked-spend','GET'))
#setting($_ = $output_type('linked.Output'))
#setting($_ = $mcp('get_spend', 'Read linked spend'))
#setting($_ = $cube('', '', '', '', '', '', '', true, 'aggregate_spend', 'Aggregate linked spend, independently', '', 'lc', false))
#setting($_ = $cubeCompose(true,true,8,100,30000,'',false,'compose_spend','Compose linked spend','','lc',false))
#define($_ = $Data<[]*linked.Row>(output/view))
SELECT s.*,type(s,'linked.Row'),grouping_enabled(s)
FROM (SELECT region,SUM(amount) AS total FROM sales GROUP BY region) s`
	found, err := bootstrap.ReflectSelectedPackages([]string{pkg}, nil)
	require.NoError(t, err)
	require.Len(t, found.Components, 1)
	cwd, err := os.Getwd()
	require.NoError(t, err)
	var previous map[string][]byte
	for pass := 0; pass < 2; pass++ {
		target := t.TempDir()
		catalog := typecatalog.NewCatalog()
		require.NoError(t, catalog.LinkRuntimeAll(typecatalog.TypeOriginPackage, x.NewType(reflect.TypeFor[linkedgrouping.Row]())))
		resources := resource.New()
		require.NoError(t, resources.Register("", linkedgrouping.Resources))
		compiled, err := (&PackageCompilation{Source: &Source{Scope: pkg, Name: "Spend", Text: text, Resources: resources}, Component: found.Components[0], InputType: reflect.TypeFor[linkedgrouping.Input](), OutputType: reflect.TypeFor[linkedgrouping.Output](), Types: catalog}).Compile(context.Background())
		require.NoError(t, err)
		exposure := compiled.Component.Settings.Report.MCP
		require.NotNil(t, exposure, "linked settings dropped authored cube metadata")
		require.Equal(t, "aggregate_spend", exposure.Name)
		require.Equal(t, "Aggregate linked spend, independently", exposure.Description)
		require.NotNil(t, exposure.StrictArguments)
		require.False(t, *exposure.StrictArguments)
		projected := report.ProjectedComponents(compiled.Component)
		require.Len(t, projected, 2)
		require.Equal(t, "compose_spend", projected[1].Name)
		require.Equal(t, compiled.Component.Settings.Report.Compose.MCP, projected[1].MCP)
		require.Equal(t, exposure.Name, projected[0].Name)
		require.Equal(t, exposure.Description, projected[0].Description)
		input, _, err := generationInput(filepath.Dir(cwd), "", compiled)
		require.NoError(t, err)
		generated, err := gen.New(input).Generate(target)
		require.NoError(t, err)
		require.Len(t, generated.Plan.Cubes, 1)
		sourceComponent, err := bootstrap.ResolveReportSourceWithContracts(compiled.Component, reflect.TypeFor[linkedgrouping.Input](), reflect.TypeFor[linkedgrouping.Output](), resources)
		require.NoError(t, err)
		described, err := bootstrap.DescribeInputContract(sourceComponent, reflect.TypeFor[linkedgrouping.Input](), nil)
		require.NoError(t, err)
		materialized, err := report.NewProjectCompiler(report.ProjectConfig{Types: catalog}).Compile([]report.Source{{Component: sourceComponent, Input: described, OutputType: reflect.TypeFor[linkedgrouping.Output]()}})
		require.NoError(t, err)
		require.Len(t, materialized.Derived(), 2)
		require.Equal(t, compiled.Component.Settings.Report.Compose.MCP, materialized.Derived()[1].Component.Routes[0].MCP[0])
		cube := generated.Plan.Cubes[0]
		require.Equal(t, exposure, cube.Component.Routes[0].MCP[0])
		require.Equal(t, exposure, cube.Definition.MCP)
		require.Equal(t, "get_spend", generated.Plan.Routes[0].MCP[0].Name)
		require.Equal(t, "SpendCube", cube.Component.Key.Name)
		require.Equal(t, "/linked-spend/cube", cube.Component.Routes[0].Path)
		current := map[string][]byte{}
		require.NoError(t, filepath.WalkDir(target, func(path string, entry os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() {
				return nil
			}
			relative, err := filepath.Rel(target, path)
			if err != nil {
				return err
			}
			current[relative], err = os.ReadFile(path)
			return err
		}))
		if pass == 1 {
			require.Equal(t, previous, current, "linked canonical second pass drift")
		}
		previous = current
	}
	// The linked source holder is shared immutable metadata, never rewritten.
	require.Nil(t, found.Components[0].Tag.ReportMCP)
}
