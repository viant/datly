package report

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/exec"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
)

func TestCubeDeclaredNamesPreparedSQLite(t *testing.T) {
	for _, inferred := range []bool{false, true} {
		name := "authored mapping"
		if inferred {
			name = "inferred Go row"
		}
		t.Run(name, func(t *testing.T) {
			h := (groupedReportHarnessConfig{source: func(c *spec.Component, _ *typecatalog.Catalog) {
				for _, column := range c.RootView.Columns {
					column.NameInferred = inferred
				}
				c.RootView.Selector.OrderAliases = nil
			}, report: []func(*spec.ReportSettings){func(s *spec.ReportSettings) { s.Compose = &spec.CubeComposeSettings{Enabled: true} }}}).build(t)
			request := groupedReportRequest{Dimensions: []string{"AccountID"}, Measures: []string{"TotalSpend"}, OrderBy: []string{"account_id"}}
			request.Filters.Tenant = "acme"
			request.Filters.Region = "EU"
			request.Filters.Channel = "web"
			request.Filters.Status = "active"
			rows := h.invoke(t, request)
			encoded, err := json.Marshal(rows)
			require.NoError(t, err)
			require.Contains(t, string(encoded), "150")
			require.Contains(t, string(encoded), "70")
			plan := h.artifact.predefinedHandler.(*Handler).plan
			input := h.input(t, request)
			value, err := reportInput(input, plan.inputType)
			require.NoError(t, err)
			fields, err := plan.selectedFields(value)
			require.NoError(t, err)
			selector, err := plan.selector(value, fields)
			require.NoError(t, err)
			providers, err := plan.providers(value, selector)
			require.NoError(t, err)
			prepared, err := h.runtime.InvokeComponent(context.Background(), exec.ComponentRequest{Target: plan.target, Providers: providers, PrepareQuery: true})
			require.NoError(t, err)
			query := prepared.(*exec.PreparedQuery)
			require.Contains(t, query.SQL, "account_id")
			require.Contains(t, query.SQL, "total_spend")
			require.NotEmpty(t, query.Args)
			statement, err := h.sql.DB.PrepareContext(context.Background(), query.SQL)
			require.NoError(t, err)
			defer statement.Close()
			actual, err := statement.QueryContext(context.Background(), query.Args...)
			require.NoError(t, err)
			defer actual.Close()
			columns, err := actual.Columns()
			require.NoError(t, err)
			require.Equal(t, []string{"tenant", "account_id", "total_spend"}, columns)
			var ids []int
			var amounts []float64
			for actual.Next() {
				var tenant string
				var id int
				var amount float64
				require.NoError(t, actual.Scan(&tenant, &id, &amount))
				require.Equal(t, "acme", tenant)
				ids = append(ids, id)
				amounts = append(amounts, amount)
			}
			require.NoError(t, actual.Err())
			require.Equal(t, []int{1, 2}, ids)
			require.Equal(t, []float64{150, 70}, amounts)
		})
	}
}
