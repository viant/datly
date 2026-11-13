package transcribe

import (
	"context"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe/column"
	"testing"
)

func TestFactoryInputShapePreparedMetadata(t *testing.T) {
	root, source := factoryInputShapeFixture(t)
	calls := 0
	source.PreparedColumns = func(_ context.Context, request column.PreparedColumnRequest) (*column.PreparedColumnResult, error) {
		calls++
		return &column.PreparedColumnResult{Table: "CI_AUDIENCE", Columns: []*spec.Column{
			{Name: "TARGET", Source: "TARGET", Type: spec.TypeRef{Name: "string"}, Nullable: true},
			{Name: "EXCLUSION", Source: "EXCLUSION", Type: spec.TypeRef{Name: "string"}, Nullable: true},
		}}, nil
	}
	authored := source.Text
	var first map[string]string
	for round := 0; round < 2; round++ {
		compiled, err := (&Discovery{BaseDir: root, GoBuild: source.GoBuild, PreparedColumns: source.PreparedColumns}).CompileSource(t.Context(), source)
		require.NoError(t, err)
		require.Equal(t, round+1, calls, "one metadata lookup for the declared body view")
		require.Nil(t, compiled.Component.RootView, "factory still owns execution")
		require.Empty(t, compiled.Component.Settings.Mutation)
		require.NotNil(t, compiled.ExternalHandler.InputShape)
		require.Len(t, compiled.ExternalHandler.InputShape.Columns, 2)
		generated, err := (Generator{Operation: "post"}).Generate(t.Context(), GenerationRequest{Compiled: compiled, Destination: root})
		require.NoError(t, err)
		require.Nil(t, generated.Result.Plan.MutationHandler)
		require.Empty(t, generated.Result.Plan.RootSource)
		require.Empty(t, generated.Result.Plan.Connector)
		require.Equal(t, authored, source.Text)
		current := sourceHandlerSnapshot(t, root)
		if round == 0 {
			first = current
		} else {
			require.Equal(t, first, current, "unchanged source must regenerate identically")
		}

	}
}

func TestFactoryPreparedMetadataAdmission(t *testing.T) {
	for _, both := range []bool{false, true} {
		root, source := factoryInputShapeFixture(t)
		calls := 0
		source.PreparedColumns = func(context.Context, column.PreparedColumnRequest) (*column.PreparedColumnResult, error) {
			calls++
			return nil, nil
		}
		if both {
			source.ColumnRefiner = column.New(nil)
		}
		_, err := (&Discovery{BaseDir: root, GoBuild: source.GoBuild, PreparedColumns: source.PreparedColumns, ColumnRefiner: source.ColumnRefiner}).CompileSource(t.Context(), source)
		if both {
			require.ErrorContains(t, err, "mutually exclusive")
			require.Zero(t, calls)
		} else {
			require.ErrorContains(t, err, "is missing")
			require.Equal(t, 1, calls)
		}
	}
}
