package identifier

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/constant"
	"github.com/viant/datly/spec"
)

func TestMetadataIdentifierProduct(t *testing.T) {
	values, err := constant.New(map[string]string{
		"mediator_project": "viant-e2e-mediator", "adelphic_project": "viant-e2e",
		"Table": "viant-e2e-mediator.automation.uniqueIds_fcap", "suffix": "",
	})
	require.NoError(t, err)
	for _, product := range []string{"BigQuery", "MySQL", "SQLite", ""} {
		t.Run(product, func(t *testing.T) {
			renderer := Renderer{Values: values, Product: product}
			for _, source := range []string{
				"${mediator_project}.automation.uniqueIds_fcap",
				"${adelphic_project}.scope3.emissions_data",
				"$Unsafe.Table", "${Unsafe.Table}",
			} {
				actual, err := renderer.Metadata(source)
				if product != "BigQuery" {
					require.Error(t, err, source)
					continue
				}
				require.NoError(t, err, source)
				require.NotContains(t, actual, "$")
				require.Contains(t, actual, "viant-e2e")
			}
			quoted := "`${mediator_project}.automation.uniqueIds_fcap`"
			actual, err := renderer.Metadata(quoted)
			require.NoError(t, err)
			if product == "BigQuery" {
				require.Equal(t, "`viant-e2e-mediator.automation.uniqueIds_fcap`", actual)
			} else {
				// Generic SQL treats a quoted dot as part of a single identifier.
				require.Equal(t, "viant-e2e-mediator.automation.uniqueIds_fcap", actual)
			}
			for _, invalid := range []string{
				"${mediator_project}..table", "${mediator_project}.automation.table; DROP TABLE x",
				"${mediator_project}.automation.table alias", "${missing}.automation.table",
				"`${mediator_project}.automation.table", "$Unsafe.Missing",
			} {
				_, err := renderer.Metadata(invalid)
				require.Error(t, err, invalid)
			}
		})
	}
}

func TestMetadataProductPerViewAndInstance(t *testing.T) {
	child := &spec.View{Name: "child", Source: &spec.ViewSource{Table: "${project}.scope3.emissions_data"}, Columns: []*spec.Column{{Name: "ID", Tag: `sqlx:"id,table=${project}.scope3.emissions_data,refTable=$Unsafe.Table,refDb=${project},refColumn=id"`}}}
	static := &spec.View{Name: "mysql", Source: &spec.ViewSource{Table: "RULES${suffix}", Bindings: &spec.ViewBindings{Connector: "ads"}}, Relations: []*spec.Relation{{View: &spec.View{Name: "grandchild", Source: &spec.ViewSource{Table: "OWNERS${suffix}"}}}}}
	authored := &spec.Component{Settings: &spec.Settings{DefaultConnector: "metrics"}, RootView: &spec.View{Name: "root", Source: &spec.ViewSource{Table: "$Unsafe.Table", SQL: "SELECT * FROM $Unsafe.Table"}, Relations: []*spec.Relation{{View: child}, {View: static}}}, Views: []*spec.View{{Name: "independent", Source: &spec.ViewSource{Table: "${project}.automation.uniqueIds_fcap"}}}}
	before, err := json.Marshal(authored)
	require.NoError(t, err)
	products := func(connector string) (string, error) {
		switch connector {
		case "metrics":
			return "BigQuery", nil
		case "ads":
			return "MySQL", nil
		}
		return "", fmt.Errorf("unknown connector %q", connector)
	}
	for _, project := range []string{"viant-e2e-mediator", "viant-prod-mediator"} {
		t.Run(project, func(t *testing.T) {
			t.Parallel()
			values, err := constant.New(map[string]string{"project": project, "Table": project + ".automation.uniqueIds_fcap", "suffix": "_01"})
			require.NoError(t, err)
			renderer := Renderer{Values: values, Products: products}
			actual, err := renderer.Component(authored)
			require.NoError(t, err)
			require.Equal(t, project+".automation.uniqueIds_fcap", actual.RootView.Source.Table)
			require.Equal(t, authored.RootView.Source.SQL, actual.RootView.Source.SQL)
			require.Equal(t, project+".scope3.emissions_data", actual.RootView.Relations[0].View.Source.Table)
			require.Equal(t, "id,table="+project+".scope3.emissions_data,refTable="+project+".automation.uniqueIds_fcap,refDb="+project+",refColumn=id", reflect.StructTag(actual.RootView.Relations[0].View.Columns[0].Tag).Get("sqlx"))
			require.Equal(t, "RULES_01", actual.RootView.Relations[1].View.Source.Table)
			require.Equal(t, "OWNERS_01", actual.RootView.Relations[1].View.Relations[0].View.Source.Table)
			require.Equal(t, project+".automation.uniqueIds_fcap", actual.Views[0].Source.Table)
			overridden := authored.Clone()
			overridden.Settings.DefaultConnector = "ads"
			overridden.RootView.Source.Bindings = &spec.ViewBindings{Connector: "metrics"}
			overridden.Views = nil
			_, err = renderer.Component(overridden)
			require.NoError(t, err, "root connector overrides the component default")
			denied := authored.Clone()
			denied.RootView.Relations[0].View.Source.Bindings = &spec.ViewBindings{Connector: "ads"}
			_, err = renderer.Component(denied)
			require.ErrorContains(t, err, "view child table")
			denied.RootView.Relations[0].View.Source.Bindings.Connector = "missing"
			_, err = renderer.Component(denied)
			require.ErrorContains(t, err, `connector "missing"`)
			after, err := json.Marshal(authored)
			require.NoError(t, err)
			require.Equal(t, before, after)
		})
	}
}
