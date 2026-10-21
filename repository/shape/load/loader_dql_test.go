package load_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/repository/shape"
	shapeCompile "github.com/viant/datly/repository/shape/compile"
	shapeLoad "github.com/viant/datly/repository/shape/load"
	shapePlan "github.com/viant/datly/repository/shape/plan"
)

func TestLoadComponent_DQLUserMetadataPreservesBitColumns(t *testing.T) {
	dqlPath := filepath.Join("..", "..", "..", "e2e", "v1", "dql", "dev", "user", "user_metadata.dql")
	dqlPath, err := filepath.Abs(dqlPath)
	require.NoError(t, err)
	data, err := os.ReadFile(dqlPath)
	require.NoError(t, err)

	source := &shape.Source{
		Name: "user_metadata",
		Path: dqlPath,
		DQL:  string(data),
	}
	planned, err := shapeCompile.New().Compile(context.Background(), source)
	require.NoError(t, err)
	actualPlan, ok := shapePlan.ResultFrom(planned)
	require.True(t, ok)
	require.NotNil(t, actualPlan.TypeContext)
	assert.Equal(t, "github.com/viant/datly/e2e/v1/shape/dev/user/mysql_boolean", actualPlan.TypeContext.PackagePath)
	t.Logf("typectx: dir=%q name=%q path=%q", actualPlan.TypeContext.PackageDir, actualPlan.TypeContext.PackageName, actualPlan.TypeContext.PackagePath)

	artifact, err := shapeLoad.New().LoadComponent(context.Background(), planned)
	require.NoError(t, err)
	root, err := artifact.Resource.Views.Index().Lookup("user_metadata")
	require.NoError(t, err)
	require.NotNil(t, root)
	require.NotNil(t, root.Schema)
	require.NotNil(t, root.Schema.Type())
	t.Logf("schema type: %v", root.Schema.Type())

	names := make([]string, 0, len(root.Columns))
	for _, column := range root.Columns {
		if column == nil {
			continue
		}
		names = append(names, column.Name)
	}
	assert.Contains(t, names, "IS_ENABLED")
	assert.Contains(t, names, "IS_ACTIVATED")
}

func TestLoadComponent_DQLVarsHonorsDeclaredColumnType(t *testing.T) {
	dqlPath := filepath.Join("..", "..", "..", "e2e", "v1", "dql", "dev", "vendorsrv", "vars.dql")
	dqlPath, err := filepath.Abs(dqlPath)
	require.NoError(t, err)
	data, err := os.ReadFile(dqlPath)
	require.NoError(t, err)

	source := &shape.Source{
		Name: "vars",
		Path: dqlPath,
		DQL:  string(data),
	}
	planned, err := shapeCompile.New().Compile(context.Background(), source)
	require.NoError(t, err)
	actualPlan, ok := shapePlan.ResultFrom(planned)
	require.True(t, ok)
	for _, item := range actualPlan.Views {
		if item == nil || item.Name != "main" {
			continue
		}
		if item.Declaration != nil && item.Declaration.ColumnsConfig != nil {
			if cfg := item.Declaration.ColumnsConfig["Key3"]; cfg != nil {
				t.Logf("planned Key3 dataType=%q", cfg.DataType)
			}
		}
	}

	artifact, err := shapeLoad.New().LoadComponent(context.Background(), planned)
	require.NoError(t, err)
	root, err := artifact.Resource.Views.Index().Lookup("main")
	require.NoError(t, err)
	require.NotNil(t, root)
	require.NotNil(t, root.Schema)
	require.NotNil(t, root.Schema.Type())
	if cfg := root.ColumnsConfig["Key3"]; cfg != nil && cfg.DataType != nil {
		t.Logf("Key3 config dataType=%q", *cfg.DataType)
	}
	t.Logf("vars schema type: %v", root.Schema.Type())

	var key3Type string
	for _, column := range root.Columns {
		if column == nil || column.Name != "Key3" {
			continue
		}
		if column.ColumnType() != nil {
			key3Type = column.ColumnType().String()
		} else {
			key3Type = column.DataType
		}
	}
	assert.Equal(t, "bool", key3Type)
}
