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
