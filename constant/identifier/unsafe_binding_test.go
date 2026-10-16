package identifier

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/constant"
	"github.com/viant/datly/spec"
)

func TestQuotedUnsafeConstantBinding(t *testing.T) {
	defaultTable := "viant-mediator.mdp.bidalloc_campaign_fulfillment"
	for _, expression := range []string{"$Unsafe.HistoryTable", "${Unsafe.HistoryTable}", "`$Unsafe.HistoryTable`", "`${Unsafe.HistoryTable}`", `"$Unsafe.HistoryTable"`, "[$Unsafe.HistoryTable]"} {
		t.Run(expression, func(t *testing.T) {
			t.Parallel()
			component := &spec.Component{Parameters: []*spec.Parameter{{Name: "HistoryTable", Source: spec.BindSource{Kind: "const", Name: "frequencyTargetHistoryTable"}, Value: &defaultTable}}, RootView: &spec.View{Name: "history", Source: &spec.ViewSource{Table: expression, SQL: "SELECT * FROM " + expression}, Columns: []*spec.Column{{Name: "Owner", Tag: `sqlx:"owner,refTable=` + expression + `,refColumn=id"`}}}}
			// Double-quoted identifiers require escaped Go tags, unlike view metadata.
			if expression[0] == '"' {
				component.RootView.Columns = nil
			}
			before, err := json.Marshal(component)
			require.NoError(t, err)
			defaults, err := (Renderer{Product: "BigQuery"}).Component(component)
			require.NoError(t, err)
			parts, err := (Renderer{Product: "BigQuery"}).parts(defaults.RootView.Source.Table)
			require.NoError(t, err)
			require.Equal(t, strings.Split(defaultTable, "."), parts)
			for _, override := range []string{"", "viant-prod.mdp.bidalloc_campaign_fulfillment"} {
				instance := map[string]string{"HistoryTable": "wrong-project.mdp.decoy"}
				want := defaultTable
				if override != "" {
					instance["frequencyTargetHistoryTable"] = override
					want = override
				}
				values, err := constant.New(instance)
				require.NoError(t, err)
				result, err := (Renderer{Values: values, Product: "BigQuery"}).Component(component)
				require.NoError(t, err)
				parts, err := (Renderer{Product: "BigQuery"}).parts(result.RootView.Source.Table)
				require.NoError(t, err)
				require.Equal(t, strings.Split(want, "."), parts)
				require.Equal(t, component.RootView.Source.SQL, result.RootView.Source.SQL)
			}
			after, err := json.Marshal(component)
			require.NoError(t, err)
			require.Equal(t, before, after)
		})
	}
}

func TestUnsafeMetadataRejectsNonConstantAuthority(t *testing.T) {
	for _, kind := range []string{"query", "body", "header", ""} {
		for _, collision := range []bool{false, true} {
			valuesMap := map[string]string{"frequencyTargetHistoryTable": "viant-prod.mdp.history"}
			if collision {
				valuesMap["HistoryTable"] = "viant-prod.mdp.decoy"
			}
			values, err := constant.New(valuesMap)
			require.NoError(t, err)
			component := &spec.Component{RootView: &spec.View{Name: "history", Source: &spec.ViewSource{Table: "`$Unsafe.HistoryTable`"}}}
			if kind != "" {
				component.Parameters = []*spec.Parameter{{Name: "HistoryTable", Source: spec.BindSource{Kind: kind, Name: "table"}}}
			}
			_, err = (Renderer{Values: values, Product: "BigQuery"}).Component(component)
			require.Error(t, err, "kind=%q collision=%v", kind, collision)
		}
	}
}

func TestQuotedUnsafeBindingPreservesLiteralsAndRejectsMalformedTargets(t *testing.T) {
	for _, table := range []string{"", "records` JOIN secrets ON 1=1 --", "${Other}"} {
		component := &spec.Component{Parameters: []*spec.Parameter{{Name: "HistoryTable", Source: spec.BindSource{Kind: "const", Name: "physicalTable"}, Value: &table}}, RootView: &spec.View{Name: "history", Source: &spec.ViewSource{Table: "`$Unsafe.HistoryTable`"}}}
		_, err := (Renderer{Product: "BigQuery"}).Component(component)
		require.Error(t, err, table)
	}
	value := "viant-mediator.mdp.history"
	component := &spec.Component{Parameters: []*spec.Parameter{{Name: "HistoryTable", Source: spec.BindSource{Kind: "const", Name: "physicalTable"}, Value: &value}}, RootView: &spec.View{Name: "history", Source: &spec.ViewSource{Table: "`literal-project.mdp.history`", SQL: "SELECT * FROM `literal-project.mdp.history`"}}}
	result, err := (Renderer{Product: "BigQuery"}).Component(component)
	require.NoError(t, err)
	require.Equal(t, component.RootView.Source, result.RootView.Source)
}

func TestQuotedUnsafeBindingRequiresExactExpression(t *testing.T) {
	value := "viant-mediator.mdp.history"
	values, err := (*constant.Values)(nil).For(&spec.Component{Parameters: []*spec.Parameter{{Name: "HistoryTable", Source: spec.BindSource{Kind: "const", Name: "physicalTable"}, Value: &value}}})
	require.NoError(t, err)
	renderer := Renderer{Values: values, Product: "BigQuery"}
	for _, source := range []string{"`Unsafe_HistoryTable`", "` $Unsafe.HistoryTable`", "`$Unsafe.HistoryTable `", "`$Unsafe. HistoryTable`", "`${Unsafe.HistoryTable }`"} {
		actual, err := renderer.Identifier(source)
		require.NoError(t, err)
		require.Equal(t, source, actual, "must not discard literal text or alter malformed expressions")
		if strings.Contains(source, "$") {
			_, err := renderer.Metadata(source)
			require.Error(t, err)
		} else {
			actual, err := renderer.Metadata(source)
			require.NoError(t, err)
			require.Equal(t, source, actual)
		}
	}
}
