package repository

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/repository/contract"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
)

type shapeTestRow struct {
	ID int
}

type shapeTestOutput struct {
	Rows []shapeTestRow `view:"rows,table=REPORT" sql:"SELECT ID FROM REPORT"`
}

func TestComponents_mergeShapeViews_Enabled(t *testing.T) {
	resource := view.EmptyResource()
	components := &Components{
		Resource: resource,
		options:  &Options{shapePipeline: true},
	}

	component := &Component{
		Path: contract.Path{URI: "/v1/api/report", Method: "GET"},
		Contract: contract.Contract{
			Output: contract.Output{Type: state.Type{Schema: state.NewSchema(reflect.TypeOf(&shapeTestOutput{}))}},
		},
		View: view.NewRefView("rows"),
	}

	err := components.mergeShapeViews(context.Background(), component)
	require.NoError(t, err)
	require.Len(t, components.Resource.Views, 1)
	assert.Equal(t, "rows", components.Resource.Views[0].Name)
}

func TestComponents_mergeShapeViews_Disabled(t *testing.T) {
	resource := view.EmptyResource()
	components := &Components{
		Resource: resource,
		options:  &Options{shapePipeline: false},
	}

	component := &Component{
		Path: contract.Path{URI: "/v1/api/report", Method: "GET"},
		Contract: contract.Contract{
			Output: contract.Output{Type: state.Type{Schema: state.NewSchema(reflect.TypeOf(&shapeTestOutput{}))}},
		},
		View: view.NewRefView("rows"),
	}

	err := components.mergeShapeViews(context.Background(), component)
	require.NoError(t, err)
	assert.Len(t, components.Resource.Views, 0)
}
