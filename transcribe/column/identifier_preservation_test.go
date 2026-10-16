package column

import (
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/constant"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	sqlio "github.com/viant/sqlx/io"
)

func TestDiscoveryPreservesAuthoredIdentifierMetadata(t *testing.T) {
	h := sqlite.New(t)
	require.NoError(t, h.ExecStatements(t.Context(),
		"CREATE TABLE CI_TARGETING_RULE_SOAK01(id INTEGER PRIMARY KEY, code TEXT UNIQUE, owner_id INTEGER REFERENCES owners(id))",
		"CREATE TABLE owners(id INTEGER PRIMARY KEY)"))
	values, err := constant.New(map[string]string{"DbRuleTable": "CI_TARGETING_RULE_SOAK01", "OtherTable": "CI_TARGETING_RULE_SOAK01", "mdp_table_suffix": "_SOAK01"})
	require.NoError(t, err)
	for _, tc := range []struct{ name, table, SQL string }{
		{"unsafe", "$Unsafe.DbRuleTable", "SELECT r.* FROM (SELECT * FROM $Unsafe.DbRuleTable) r"},
		{"braced unsafe", "${Unsafe.DbRuleTable}", "SELECT r.* FROM (SELECT * FROM ${Unsafe.DbRuleTable}) r"},
		{"equal constants retain identity", "$Unsafe.OtherTable", "SELECT r.* FROM (SELECT * FROM $Unsafe.OtherTable) r"},
		{"auxiliary unsafe", "$Unsafe.DbRuleTable", "SELECT r.* FROM ($Unsafe.DbRuleTable) r"},
		{"composed", "`CI_TARGETING_RULE${mdp_table_suffix}`", "SELECT r.* FROM (SELECT * FROM `CI_TARGETING_RULE${mdp_table_suffix}`) r"},
		{"bare composed", "CI_TARGETING_RULE${mdp_table_suffix}", "SELECT r.* FROM (SELECT * FROM CI_TARGETING_RULE${mdp_table_suffix}) r"},
		{"literal collision", "CI_TARGETING_RULE_SOAK01", "SELECT r.* FROM (SELECT * FROM CI_TARGETING_RULE_SOAK01) r"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			view := &spec.View{Name: "Rules", Source: &spec.ViewSource{SQL: tc.SQL}}
			component := &spec.Component{Name: "Rules", Settings: &spec.Settings{DefaultConnector: "main"}, RootView: view}
			require.NoError(t, New(Connections{"main": h.DB}).Refine(t.Context(), component, nil, &TemplateInput{Const: values, Value: reflect.ValueOf(struct{ DbRuleTable, OtherTable string }{"CI_TARGETING_RULE_SOAK01", "CI_TARGETING_RULE_SOAK01"})}))
			require.Equal(t, tc.SQL, view.Source.SQL)
			require.Equal(t, tc.table, view.Source.Table)
			require.Len(t, view.Columns, 3)
			require.True(t, view.Columns[0].PrimaryKey)
			// SQLite's column metadata does not expose UNIQUE indexes. Exercise
			// their transfer with the same physical facts supplied by MySQL.
			lineage, err := directProjectionLineage(&spec.ViewSource{Table: "CI_TARGETING_RULE_SOAK01", SQL: "SELECT code FROM CI_TARGETING_RULE_SOAK01"})
			require.NoError(t, err)
			applyTableConstraints(view.Columns, map[string]tableConstraint{"code": {unique: true}}, lineage, view.Source.Table)
			code := sqlio.ParseTag(reflect.StructTag(view.Columns[1].Tag))
			require.True(t, view.Columns[1].Unique)
			require.Equal(t, tc.table, code.Table)
			owner := sqlio.ParseTag(reflect.StructTag(view.Columns[2].Tag))
			require.Equal(t, "owners", owner.RefTable, "ordinary literal FK targets remain literal")
			if strings.Contains(tc.table, "$") {
				require.NotContains(t, view.Columns[1].Tag, "CI_TARGETING_RULE_SOAK01")
			}
		})
	}
}

func TestForeignKeyIdentifierProvenance(t *testing.T) {
	values, err := constant.New(map[string]string{"Parents": "parents_soak", "OtherParents": "parents_soak", "Children": "children_soak"})
	require.NoError(t, err)
	for _, mode := range []string{"linked", "unproven", "ambiguous", "explicit literal", "ordinary literal", "self", "self collision", "unproven self collision"} {
		t.Run(mode, func(t *testing.T) {
			parent := &spec.View{Name: "Parents", Source: &spec.ViewSource{Table: "$Unsafe.Parents"}}
			other := &spec.View{Name: "OtherParents", Source: &spec.ViewSource{Table: "$Unsafe.OtherParents"}}
			child := &spec.View{Name: "Children", Source: &spec.ViewSource{Table: "$Unsafe.Children"}, Columns: []*spec.Column{{Name: "parent_id", Source: "parent_id"}}}
			link := func(p *spec.View) {
				p.Relations = []*spec.Relation{{View: child, On: []*spec.RelationLink{{ParentColumn: "id", ChildColumn: "parent_id"}}}}
			}
			link(parent)
			switch mode {
			case "unproven":
				parent.Relations = nil
			case "ambiguous":
				link(other)
			case "explicit literal":
				parent.Relations = nil
				child.Columns[0].Tag = `sqlx:"parent_id,refTable=parents_soak,refColumn=id"`
			case "ordinary literal":
				parent.Source.Table = "parents_soak"
				other.Source.Table = "parents_soak"
			case "self":
				child.Source.Table = "$Unsafe.Parents"
			case "self collision":
				child.Source.Table = "$Unsafe.OtherParents"
			case "unproven self collision":
				child.Source.Table = "$Unsafe.OtherParents"
				parent.Relations = nil
			}
			component := &spec.Component{Settings: &spec.Settings{DefaultConnector: "main"}, RootView: parent, Views: []*spec.View{other, child}}
			compilation := New(nil).BeginCompilation()
			require.NoError(t, compilation.captureIdentifierOrigins(component, nil, &TemplateInput{Const: values}))
			constraints := map[string]tableConstraint{"parent_id": {reference: &tableReference{table: "parents_soak", column: "id"}}}
			result, err := compilation.preserveConstraintTargets(child, constraints, nil, "SQLite")
			if mode == "ambiguous" || mode == "unproven" || mode == "unproven self collision" {
				require.ErrorContains(t, err, "environment-dependent foreign-key target")
				return
			}
			require.NoError(t, err)
			want := "$Unsafe.Parents"
			if mode == "ordinary literal" || mode == "explicit literal" {
				want = "parents_soak"
			}
			require.Equal(t, want, result["parent_id"].reference.table)
			require.Equal(t, "parents_soak", constraints["parent_id"].reference.table, "native facts must remain detached")
		})
	}
}

func TestForeignKeyTargetDoesNotIgnoreSchema(t *testing.T) {
	values, err := constant.New(map[string]string{"Table": "records"})
	require.NoError(t, err)
	view := &spec.View{Name: "Records", Source: &spec.ViewSource{Table: "$Unsafe.Table"}}
	compilation := New(nil).BeginCompilation()
	require.NoError(t, compilation.captureIdentifierOrigins(&spec.Component{RootView: view}, nil, &TemplateInput{Const: values}))
	constraints := map[string]tableConstraint{
		"parent_id": {reference: &tableReference{schema: "external", table: "records", column: "id"}},
		"other_id":  {reference: &tableReference{table: "RECORDS", column: "id"}},
	}
	result, err := compilation.preserveConstraintTargets(view, constraints, nil, "MySQL")
	require.NoError(t, err)
	require.Equal(t, "external", result["parent_id"].reference.schema)
	require.Equal(t, "records", result["parent_id"].reference.table)
	require.Equal(t, "RECORDS", result["other_id"].reference.table, "case-folding cannot prove identity for a case-sensitive database")
}

func TestDiscoveryPreservesNestedForeignKeyTarget(t *testing.T) {
	h := sqlite.New(t)
	require.NoError(t, h.ExecStatements(t.Context(),
		"CREATE TABLE parents_soak(id INTEGER PRIMARY KEY)",
		"CREATE TABLE children_soak(id INTEGER PRIMARY KEY,parent_id INTEGER REFERENCES parents_soak(id))"))
	values, err := constant.New(map[string]string{"parents": "parents_soak", "children": "children_soak"})
	require.NoError(t, err)
	parent := &spec.View{Name: "Parents", Source: &spec.ViewSource{SQL: "SELECT id FROM `${parents}`"}}
	child := &spec.View{Name: "Children", Source: &spec.ViewSource{SQL: "SELECT id,parent_id FROM `${children}`"}}
	parent.Relations = []*spec.Relation{{View: child, On: []*spec.RelationLink{{ParentColumn: "id", ChildColumn: "parent_id"}}}}
	component := &spec.Component{Name: "Parents", RootView: parent, Settings: &spec.Settings{DefaultConnector: "main"}}
	require.NoError(t, New(Connections{"main": h.DB}).Refine(t.Context(), component, nil, &TemplateInput{Const: values}))
	require.Equal(t, "`${parents}`", parent.Source.Table)
	require.Equal(t, "`${children}`", child.Source.Table)
	require.Len(t, child.Columns, 2)
	tag := sqlio.ParseTag(reflect.StructTag(child.Columns[1].Tag))
	require.Equal(t, "`${parents}`", tag.RefTable)
	require.Equal(t, "id", tag.RefColumn)
	require.Equal(t, "SELECT id,parent_id FROM `${children}`", child.Source.SQL)
}

func TestForeignKeySchemaPreservesComposedSourceIdentity(t *testing.T) {
	values, err := constant.New(map[string]string{"project": "soak-project", "dataset": "soak_ds"})
	require.NoError(t, err)
	view := &spec.View{Name: "Records", Source: &spec.ViewSource{Table: "`${project}.${dataset}.records`"}}
	compilation := New(nil).BeginCompilation()
	require.NoError(t, compilation.captureIdentifierOrigins(&spec.Component{RootView: view}, nil, &TemplateInput{Const: values}))
	constraints := map[string]tableConstraint{
		"owner_id":  {reference: &tableReference{schema: "soak_ds", table: "owners", column: "id"}},
		"parent_id": {reference: &tableReference{schema: "soak_ds", table: "records", column: "id"}},
	}
	result, err := compilation.preserveConstraintTargets(view, constraints, nil, "BigQuery")
	require.NoError(t, err)
	require.Equal(t, "soak_ds", result["owner_id"].reference.schema, "a matching literal schema alone proves no symbolic target identity")
	require.Equal(t, "owners", result["owner_id"].reference.table)
	require.Empty(t, result["parent_id"].reference.schema)
	require.Equal(t, "`${project}.${dataset}.records`", result["parent_id"].reference.table)
	require.Equal(t, "soak_ds", constraints["owner_id"].reference.schema)
}

func TestForeignKeyHyphenatedProjectUsesSourceProduct(t *testing.T) {
	values, err := constant.New(map[string]string{"project": "viant-e2e", "Table": "viant-e2e.scope3.emissions_data"})
	require.NoError(t, err)
	for _, table := range []string{"${project}.scope3.emissions_data", "$Unsafe.Table", "`${project}.scope3.emissions_data`"} {
		t.Run(table, func(t *testing.T) {
			view := &spec.View{Name: "Emissions", Source: &spec.ViewSource{Table: table}}
			compilation := New(nil).BeginCompilation()
			require.NoError(t, compilation.captureIdentifierOrigins(&spec.Component{RootView: view}, nil, &TemplateInput{Const: values}))
			constraints := map[string]tableConstraint{"parent_id": {reference: &tableReference{schema: "scope3", table: "emissions_data", column: "id"}}}
			resolved, err := compilation.preserveConstraintTargets(view, constraints, nil, "BigQuery")
			require.NoError(t, err)
			require.Equal(t, table, resolved["parent_id"].reference.table)
			require.Empty(t, resolved["parent_id"].reference.schema)
			require.Equal(t, table, view.Source.Table)
		})
	}
}
