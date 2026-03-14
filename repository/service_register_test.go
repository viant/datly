package repository

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/repository/contract"
	"github.com/viant/datly/repository/path"
	"github.com/viant/datly/view"
)

func TestServiceRegister_PreservesReportOnPath(t *testing.T) {
	paths, err := path.New(context.Background(), nil, "", time.Second)
	require.NoError(t, err)

	service := &Service{
		registry: NewRegistry("", nil, nil),
		paths:    paths,
	}

	enabled := true
	service.Register(&Component{
		Path: contract.Path{
			Method: "GET",
			URI:    "/v1/api/report",
		},
		View: &view.View{Groupable: true},
		Report: &Report{
			Enabled:    true,
			MCPTool:    &enabled,
			Input:      "NamedReportInput",
			Dimensions: "Dims",
			Measures:   "Metrics",
			Filters:    "Predicates",
			OrderBy:    "Sort",
			Limit:      "Take",
			Offset:     "Skip",
		},
	})

	require.Len(t, service.paths.Container.Items, 1)
	require.Len(t, service.paths.Container.Items[0].Paths, 2)
	require.NotNil(t, service.paths.Container.Items[0].Paths[0].Version)
	require.NotNil(t, service.paths.Container.Items[0].Paths[1].Version)
	report := service.paths.Container.Items[0].Paths[0].Report
	require.NotNil(t, report)
	require.True(t, report.Enabled)
	require.NotNil(t, report.MCPTool)
	require.True(t, *report.MCPTool)
	require.Equal(t, "NamedReportInput", report.Input)
	require.Equal(t, "Dims", report.Dimensions)
	require.Equal(t, "Metrics", report.Measures)
	require.Equal(t, "Predicates", report.Filters)
	require.Equal(t, "Sort", report.OrderBy)
	require.Equal(t, "Take", report.Limit)
	require.Equal(t, "Skip", report.Offset)

	_, err = service.registry.LookupProvider(context.Background(), &contract.Path{
		Method: http.MethodPost,
		URI:    "/v1/api/report/report",
	})
	require.NoError(t, err)
}
