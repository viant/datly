package transcribe

import (
	"context"
	"github.com/viant/bindly/resource"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/readergraph"
	authoring "github.com/viant/datly/transcribe/compile"
	gen "github.com/viant/datly/transcribe/generate"
	"github.com/viant/datly/transcribe/testdata/linkedgrouping"
	"github.com/viant/datly/transcribe/testdata/outputoverlay"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// This goes through PackageCompilation with real linked Go output types and
// no ColumnRefiner, matching the source-only linked report authoring path.
func TestLinkedReportGroupingWithoutDiscovery(t *testing.T) {
	text := strings.Replace(outputOverlayDQL, "FROM (SELECT ID,NAME FROM records) r", "FROM (SELECT ID,MAX(NAME) AS NAME FROM records GROUP BY ID) r", 1)
	result, err := compileOutputOverlay(t, text, nil)
	require.NoError(t, err)
	require.NotNil(t, result.Component.RootView.Groupable)
	require.True(t, *result.Component.RootView.Groupable)
	require.NoError(t, bootstrap.CompileOutputColumns(result.Component, reflect.TypeFor[outputoverlay.Output]()))
	require.NoError(t, authoring.BackfillReportMetadata(result.Component))
	resolved, err := bootstrap.ResolveReportSourceWithContracts(result.Component, reflect.TypeFor[outputoverlay.Input](), reflect.TypeFor[outputoverlay.Output](), result.Source.Resources)
	require.NoError(t, err)
	found := 0
	for _, column := range resolved.RootView.Columns {
		switch column.Source {
		case "ID", "id":
			found++
			require.NotNil(t, column.Groupable)
			require.True(t, *column.Groupable, "untagged linked dimension lost SQL role")
		case "NAME", "name":
			found++
			require.NotNil(t, column.Groupable)
			require.False(t, *column.Groupable, "aggregate became dimension")
		}
	}
	require.Equal(t, 2, found, "both linked outputs must be checked")
}

func TestLinkedReportGenerationBackfillsGroupingAfterAdmission(t *testing.T) {
	const pkg = "github.com/viant/datly/transcribe/testdata/linkedgrouping"
	const text = `#package('github.com/viant/datly/transcribe/testdata/linkedgrouping')
#import('linked','github.com/viant/datly/transcribe/testdata/linkedgrouping')
#setting($_ = $route('/linked-spend','GET'))
#setting($_ = $output_type('linked.Output'))
#setting($_ = $cube())
#define($_ = $Data<[]*linked.Row>(output/view))
SELECT s.*,type(s,'linked.Row'),grouping_enabled(s)
FROM (SELECT region,SUM(amount) AS total FROM sales GROUP BY region) s`
	found, err := bootstrap.ReflectSelectedPackages([]string{pkg}, nil)
	require.NoError(t, err)
	require.Len(t, found.Components, 1)
	catalog := typecatalog.NewCatalog()
	require.NoError(t, catalog.LinkRuntimeAll(typecatalog.TypeOriginPackage, x.NewType(reflect.TypeFor[linkedgrouping.Row]())))
	resources := resource.New()
	require.NoError(t, resources.Register("", linkedgrouping.Resources))
	compiled, err := (&PackageCompilation{Source: &Source{Scope: pkg, Name: "Spend", Text: text, Resources: resources}, Component: found.Components[0], InputType: reflect.TypeFor[linkedgrouping.Input](), OutputType: reflect.TypeFor[linkedgrouping.Output](), Types: catalog}).Compile(context.Background())
	require.NoError(t, err)
	cwd, err := os.Getwd()
	require.NoError(t, err)
	input, _, err := generationInput(filepath.Dir(cwd), "", compiled)
	require.NoError(t, err)
	plan, err := gen.New(input).Plan()
	require.NoError(t, err)
	require.NotEmpty(t, plan.ReaderMetadata)
	_, metadataPath, _ := strings.Cut(plan.ReaderMetadata, ":")
	var snapshot *readergraph.Document
	for _, file := range plan.Resources.Files {
		if file.Path == metadataPath {
			snapshot, err = readergraph.StrictDecode([]byte(file.Content))
			require.NoError(t, err)
		}
	}
	require.NotNil(t, snapshot, "native reader metadata must be emitted")
	require.Len(t, snapshot.Component.RootView.Columns, 2)
	for _, column := range snapshot.Component.RootView.Columns {
		require.NotNil(t, column.Groupable, "snapshot lost SQL role")
		require.NotEmpty(t, column.Output, "snapshot lost SQL output identity")
		require.Equal(t, strings.EqualFold(column.Source, "region"), *column.Groupable)
	}
	require.NotEmpty(t, plan.Cubes)
	for _, cube := range plan.Cubes {
		dimensions, ok := cube.InputType.FieldByName("Dimensions")
		require.True(t, ok)
		require.Equal(t, 1, dimensions.Type.NumField(), "actual native cube lost linked SQL dimension")
		measures, ok := cube.InputType.FieldByName("Measures")
		require.True(t, ok)
		require.Equal(t, 1, measures.Type.NumField())
	}
}
