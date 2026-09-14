package report

import (
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/report/cubecompose"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
)

func TestCubeDeclaredOutputMetadata(t *testing.T) {
	for _, tc := range []struct {
		name, sql, source, alias string
		inferred                 bool
		reject                   string
	}{
		{name: "inferred Go name", sql: "SELECT account_id, SUM(amount) AS total_spend FROM spend GROUP BY account_id", source: "account_id", inferred: true},
		{name: "authored mapping", sql: "SELECT account_id, SUM(amount) AS total_spend FROM spend GROUP BY account_id", source: "account_id", alias: "AccountID"},
		{name: "SQL alias", sql: "SELECT account_id AS customer_id, SUM(amount) AS total_spend FROM spend GROUP BY account_id", source: "customer_id", inferred: true},
		{name: "hidden source", sql: "SELECT account_id AS customer_id, SUM(amount) AS total_spend FROM spend GROUP BY account_id", source: "account_id", reject: "no declared output"},
		{name: "inferred spelling", sql: "SELECT accountid, total_spend FROM spend", source: "account_id", inferred: true, reject: "no declared output"},
		{name: "duplicate direct outputs", sql: "SELECT a.account_id, b.account_id, total_spend FROM spend a JOIN spend b ON a.account_id=b.account_id", source: "account_id", reject: "duplicate output column"},
		{name: "duplicate AS", sql: "SELECT account_id, amount AS total_spend, amount AS total_spend FROM spend", source: "account_id", reject: "duplicate output column"},
		{name: "mapping chain", sql: "SELECT actual_id, total_spend FROM spend", source: "account_id", reject: "no declared output"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := reportSource(t, &spec.ReportSettings{Enabled: true, Compose: &spec.CubeComposeSettings{Enabled: true}})
			source.OutputType = reflect.TypeFor[[]reportSourceRow]()
			source.Component.RootView.Source.SQL = tc.sql
			source.Component.RootView.Columns[0].Source = tc.source
			source.Component.RootView.Columns[0].NameInferred = tc.inferred
			source.Component.RootView.Columns[1].NameInferred = true
			project, err := NewProjectCompiler(ProjectConfig{Types: typecatalog.NewCatalog()}).Compile([]Source{source})
			if tc.reject != "" {
				require.ErrorContains(t, err, tc.reject)
				return
			}
			require.NoError(t, err)
			derived := project.Derived()
			expectedSelection := tc.source
			if tc.alias != "" {
				expectedSelection = tc.alias
			}
			require.Equal(t, expectedSelection, derived[0].Plan.dimensions[0].name)
			dimension, ok := derived[0].InputType.FieldByName("Dimensions")
			require.True(t, ok)
			public, ok := dimension.Type.FieldByName("AccountID")
			require.True(t, ok)
			require.Equal(t, `accountID,omitempty`, public.Tag.Get("json"))
			h := derived[1].Handler.(*composeHandler)
			field, ok := h.catalog.Lookup(tc.source)
			require.True(t, ok)
			require.Equal(t, cubecompose.Dimension, field.Role)
			require.Equal(t, reflect.TypeFor[int](), field.Type)
			measure, ok := h.catalog.Lookup("total_spend")
			require.True(t, ok)
			require.Equal(t, cubecompose.Measure, measure.Role)
			for _, name := range []string{"accountid", "accountId", "account_id", "AccountID"} {
				_, ok := h.catalog.Lookup(name)
				expected := strings.EqualFold(name, tc.source) || tc.alias != "" && strings.EqualFold(name, tc.alias)
				require.Equal(t, expected, ok, name)
			}
			_, ok = h.catalog.Lookup("TotalSpend")
			require.False(t, ok)
		})
	}
}

func TestCubeTableDeclaredOutputMetadata(t *testing.T) {
	for _, inferred := range []bool{false, true} {
		source := reportSource(t, &spec.ReportSettings{Enabled: true, Compose: &spec.CubeComposeSettings{Enabled: true}})
		source.OutputType = reflect.TypeFor[[]reportSourceRow]()
		source.Component.RootView.Source = &spec.ViewSource{Table: "spend"}
		for _, column := range source.Component.RootView.Columns {
			column.NameInferred = inferred
		}
		project, err := NewProjectCompiler(ProjectConfig{Types: typecatalog.NewCatalog()}).Compile([]Source{source})
		require.NoError(t, err)
		expected := "AccountID"
		if inferred {
			expected = "account_id"
		}
		require.Equal(t, expected, project.Derived()[0].Plan.dimensions[0].name)
		h := project.Derived()[1].Handler.(*composeHandler)
		field, ok := h.catalog.Lookup(expected)
		require.True(t, ok)
		require.Equal(t, expected, field.SQLName)
	}
}

func TestCubeDuplicateMetadataOutput(t *testing.T) {
	source := reportSource(t, &spec.ReportSettings{Enabled: true})
	source.Component.RootView.Columns = append(source.Component.RootView.Columns, &spec.Column{Name: "Other", Source: "account_id"})
	_, err := NewProjectCompiler(ProjectConfig{Types: typecatalog.NewCatalog()}).Compile([]Source{source})
	require.ErrorContains(t, err, "duplicate projected column metadata")
}
