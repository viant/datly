package shape_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	shape "github.com/viant/datly/repository/shape"
	shapeCompile "github.com/viant/datly/repository/shape/compile"
	shapeLoad "github.com/viant/datly/repository/shape/load"
)

func TestEngine_LoadDQLViews(t *testing.T) {
	engine := shape.New(
		shape.WithCompiler(shapeCompile.New()),
		shape.WithLoader(shapeLoad.New()),
		shape.WithName("/v1/api/reports/orders"),
	)
	artifacts, err := engine.LoadDQLViews(context.Background(), "SELECT id FROM ORDERS t")
	require.NoError(t, err)
	require.NotNil(t, artifacts)
	require.Len(t, artifacts.Views, 1)
	assert.Equal(t, "t", artifacts.Views[0].Name)
}

func TestEngine_LoadDQLComponent(t *testing.T) {
	engine := shape.New(
		shape.WithCompiler(shapeCompile.New()),
		shape.WithLoader(shapeLoad.New()),
		shape.WithName("/v1/api/reports/orders"),
	)
	artifact, err := engine.LoadDQLComponent(context.Background(), "SELECT id FROM ORDERS t")
	require.NoError(t, err)
	require.NotNil(t, artifact)
	require.NotNil(t, artifact.Component)

	component, ok := artifact.Component.(*shapeLoad.Component)
	require.True(t, ok)
	assert.Equal(t, "/v1/api/reports/orders", component.Name)
	assert.Equal(t, "t", component.RootView)
}

func TestEngine_LoadDQLComponent_DeclarationMetadata(t *testing.T) {
	engine := shape.New(
		shape.WithCompiler(shapeCompile.New()),
		shape.WithLoader(shapeLoad.New()),
		shape.WithName("/v1/api/reports/orders"),
	)
	dql := `
#set($_ = $limit<?>(view/limit).WithPredicate('ByID','id = ?', 1).QuerySelector('items') /* SELECT id FROM ORDERS o */)
SELECT id FROM ORDERS t`
	artifact, err := engine.LoadDQLComponent(context.Background(), dql)
	require.NoError(t, err)
	require.NotNil(t, artifact)
	component, ok := artifact.Component.(*shapeLoad.Component)
	require.True(t, ok)
	require.NotNil(t, component.Declarations)
	require.NotNil(t, component.QuerySelectors)
	require.NotNil(t, component.Predicates)
	assert.Equal(t, []string{"o"}, component.QuerySelectors["items"])
	require.NotNil(t, component.Declarations["o"])
	assert.Equal(t, "items", component.Declarations["o"].QuerySelector)
	require.NotEmpty(t, component.Predicates["o"])
	assert.Equal(t, "ByID", component.Predicates["o"][0].Name)
}
