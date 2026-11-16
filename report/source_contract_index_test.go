package report

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/exec"
	"github.com/viant/datly/spec"
	"github.com/viant/x/shape"
	xcontract "github.com/viant/xdatly/contract"
)

func TestSourceContractIndexUsesNativeMetadataOnlyVisibility(t *testing.T) {
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: groupedReportComponent(&spec.ReportSettings{Enabled: true}), InputType: reflect.TypeFor[groupedSpendInput](), OutputType: reflect.TypeFor[groupedSpendOutput](), DirectViewField: "Rows", CodecFactory: &groupedCSVCodecFactory{}})
	require.NoError(t, err)
	index, err := compileSourceContractIndex(artifact.ReportSourceComponent(), reflect.TypeFor[groupedSpendOutput]())
	require.NoError(t, err)
	require.Equal(t, "Rows", index.rowPath)
	walker, err := shape.CompileRewrite(reflect.TypeFor[groupedSpendOutput]())
	require.NoError(t, err)
	representation, err := walker.RewriteRepresentation(func(_ string, parent reflect.Type, field *shape.FieldMetadata) (*shape.FieldMetadata, error) {
		if parent == reflect.TypeFor[groupedSpendRow]() && field.Name == "TotalSpend" {
			changed := (&xcontract.Field{Name: field.Name, Tag: field.Tag}).SetTag("mcp", "-")
			return &shape.FieldMetadata{Name: changed.Name, Tag: changed.Tag}, nil
		}
		return nil, nil
	})
	require.NoError(t, err)
	effective := &exec.EffectiveContract{Output: reflect.TypeFor[groupedSpendOutput](), OutputRepresentation: representation}
	visible, err := index.visibleFields(effective)
	require.NoError(t, err)
	t.Logf("index=%+v visible=%+v", index.fields, visible)
	require.False(t, visible["TotalSpend"])
	require.False(t, visible["total_spend"])
	require.True(t, visible["region"])
	holderHidden, err := walker.RewriteRepresentation(func(_ string, parent reflect.Type, field *shape.FieldMetadata) (*shape.FieldMetadata, error) {
		if parent == reflect.TypeFor[groupedSpendOutput]() && field.Name == "Rows" {
			changed := (&xcontract.Field{Name: field.Name, Tag: field.Tag}).SetTag("mcp", "-")
			return &shape.FieldMetadata{Name: changed.Name, Tag: changed.Tag}, nil
		}
		return nil, nil
	})
	require.NoError(t, err)
	effective.OutputRepresentation = holderHidden
	visible, err = index.visibleFields(effective)
	require.NoError(t, err)
	require.Empty(t, visible, "hidden row holder cannot expose its leaves")
	effective.OutputRepresentation = nil
	visible, err = index.visibleFields(effective)
	require.NoError(t, err)
	require.True(t, visible["TotalSpend"])
}
