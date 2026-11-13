package report

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
)

func TestReadOnlyPOSTSourceCubeExecutesThroughMCP(t *testing.T) {
	h := (groupedReportHarnessConfig{route: func(r *spec.Route) { r.Method = http.MethodPost }}).build(t)
	result := h.invokeMCP(t, groupedReportRequest{Dimensions: []string{"AccountID"}, Measures: []string{"TotalSpend", "OrderCount"}, Filters: groupedReportFilters{AccountIDs: "1", Tenant: "acme", Region: "EU", Channel: "web", Status: "active"}, Limit: intPointer(1)})
	require.Len(t, result.Rows, 1)
	require.Equal(t, 1, result.Rows[0].AccountID)
	require.Equal(t, float64(150), result.Rows[0].TotalSpend)
	require.Equal(t, 2, result.Rows[0].OrderCount)
}

func TestMutationSourceDoesNotBecomeCube(t *testing.T) {
	source := reportSource(t, &spec.ReportSettings{Enabled: true})
	source.Component.Settings.Mutation = "patch"
	for _, method := range []string{http.MethodGet, http.MethodPost} {
		source.Component.Routes[0].Method = method
		project, err := NewProjectCompiler(ProjectConfig{Types: typecatalog.NewCatalog()}).Compile([]Source{source})
		require.NoError(t, err)
		require.Empty(t, project.Derived())
		require.Empty(t, ProjectedComponents(source.Component))
	}
}
