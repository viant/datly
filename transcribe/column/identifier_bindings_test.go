package column

import (
	"reflect"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
)

func requestTableComponent() *spec.Component {
	return &spec.Component{Name: "Regions", Settings: &spec.Settings{DefaultConnector: "main", Const: map[string]string{"schema": "main", "Fixed": "regions"}}, Parameters: []*spec.Parameter{
		{Name: "RegionTable", Source: spec.BindSource{Kind: "query", Name: "table"}, TypeExpr: "string"},
		{Name: "DbRuleTable", Source: spec.BindSource{Kind: "const", Name: "DbRuleTable"}, TypeExpr: "string"},
	}}
}

func TestIdentifierBindingAuthority(t *testing.T) {
	bindings := newIdentifierBindings(requestTableComponent(), nil)
	for _, tc := range []struct {
		table   string
		dynamic bool
	}{
		{"regions", false}, {"$Unsafe.RegionTable", true}, {"${Unsafe.RegionTable}", true},
		{"${schema}.$Unsafe.RegionTable", true}, {"`fixed_${Unsafe.RegionTable}`", true},
		{"$Unsafe.DbRuleTable", false}, {"`${schema}.${Fixed}`", false},
	} {
		dynamic, err := bindings.dynamic(tc.table)
		require.NoError(t, err, tc.table)
		require.Equal(t, tc.dynamic, dynamic, tc.table)
	}
	_, err := bindings.dynamic("${schema}.$Unsafe.Undeclared")
	require.ErrorContains(t, err, "undeclared binding")
	for _, expression := range []string{"${schema.ToUpper()}", "#if($RegionTable)regions#end"} {
		_, err := bindings.dynamic(expression)
		require.ErrorContains(t, err, "unsupported identifier dependency")
	}
}

func TestExplicitDynamicIdentifierMetadataIsRejected(t *testing.T) {
	for _, metadata := range []string{"table", "unique table", "refTable", "refDb"} {
		t.Run(metadata, func(t *testing.T) {
			component := requestTableComponent()
			view := &spec.View{Name: "Regions", Source: &spec.ViewSource{SQL: "SELECT id FROM regions"}}
			component.RootView = view
			if metadata == "table" {
				view.Source.Table = "$Unsafe.RegionTable"
			} else {
				option := metadata
				if metadata == "unique table" {
					option = "unique,table"
				}
				view.Columns = []*spec.Column{{Name: "id", Tag: `sqlx:"id,` + option + `=$Unsafe.RegionTable"`}}
			}
			err := NormalizeIdentifierMetadata(component)
			require.ErrorContains(t, err, "Regions")
			require.ErrorContains(t, err, "explicit")
		})
	}
}

func TestRequestTableDiscoveryDoesNotInferIdentityOrConstraints(t *testing.T) {
	h := sqlite.New(t)
	require.NoError(t, h.ExecStatements(t.Context(), "CREATE TABLE regions(id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT NOT NULL,owner INTEGER REFERENCES regions(id))"))
	for _, sql := range []string{
		"SELECT r.* FROM $Unsafe.RegionTable r",
		"SELECT r.name FROM (SELECT * FROM $Unsafe.RegionTable) r",
		"SELECT r.name FROM (${embed:regions.sql}) r",
		"SELECT r.name FROM (SELECT * FROM ${schema}.$Unsafe.RegionTable) r",
	} {
		t.Run(sql, func(t *testing.T) {
			component := requestTableComponent()
			view := &spec.View{Name: "Regions", Namespace: "r", Source: &spec.ViewSource{SQL: sql}}
			if sql == "SELECT r.name FROM (${embed:regions.sql}) r" {
				view.Source.Embeds = []*spec.EmbeddedSQLRef{{Path: "regions.sql", Raw: "${embed:regions.sql}"}}
			}
			component.RootView = view
			resources := fstest.MapFS{"regions.sql": &fstest.MapFile{Data: []byte("SELECT * FROM $Unsafe.RegionTable")}}
			input := &TemplateInput{Value: reflect.ValueOf(struct{ RegionTable string }{"regions"})}
			require.NoError(t, New(Connections{"main": h.DB}).Refine(t.Context(), component, resources, input))
			require.Empty(t, view.Source.Table)
			require.Equal(t, sql, view.Source.SQL)
			if sql != "SELECT r.* FROM $Unsafe.RegionTable r" {
				require.Len(t, view.Columns, 1, "hidden identity must not be appended")
			}
			for _, column := range view.Columns {
				require.False(t, column.PrimaryKey)
				require.False(t, column.AutoIncrement)
				require.False(t, column.Unique)
				require.NotContains(t, column.Tag, "refTable=")
				require.NotEqual(t, "any", column.Type.Name)
			}
		})
	}
}

func TestRequestTableDiscoveryPreservesAuthoredColumnsAndOtherViews(t *testing.T) {
	h := sqlite.New(t)
	require.NoError(t, h.ExecStatements(t.Context(), "CREATE TABLE regions(id INTEGER PRIMARY KEY,name TEXT)"))
	component := requestTableComponent()
	authored := &spec.Column{Name: "id", Source: "id", PrimaryKey: true, Unique: true, Tag: `sqlx:"id,primaryKey,unique,table=regions" json:"identifier"`}
	root := &spec.View{Name: "Regions", Source: &spec.ViewSource{SQL: "SELECT r.* FROM $Unsafe.RegionTable r", Table: "$Unsafe.RegionTable", TableInferred: true}, Columns: []*spec.Column{authored.Clone()}}
	child := &spec.View{Name: "Fixed", Source: &spec.ViewSource{SQL: "SELECT * FROM regions"}}
	root.Relations = []*spec.Relation{{View: child, On: []*spec.RelationLink{{ParentColumn: "id", ChildColumn: "id"}}}}
	component.RootView = root
	input := &TemplateInput{Value: reflect.ValueOf(struct{ RegionTable string }{"regions"})}
	require.NoError(t, New(Connections{"main": h.DB}).Refine(t.Context(), component, nil, input))
	require.Empty(t, root.Source.Table)
	require.True(t, root.Columns[0].PrimaryKey)
	require.True(t, root.Columns[0].Unique)
	require.Equal(t, authored.Tag, root.Columns[0].Tag)
	require.Equal(t, "regions", child.Source.Table)
	require.True(t, child.Columns[0].PrimaryKey, "a dynamic parent must not suppress its static child's metadata")
}

func TestRequestTableDiscoveryClassifiesChildAndJoinedSourcesIndependently(t *testing.T) {
	h := sqlite.New(t)
	require.NoError(t, h.ExecStatements(t.Context(), "CREATE TABLE regions(id INTEGER PRIMARY KEY,name TEXT)"))
	for _, joined := range []bool{false, true} {
		component := requestTableComponent()
		root := &spec.View{Name: "Fixed", Source: &spec.ViewSource{SQL: "SELECT r.id,r.name FROM regions r"}}
		child := &spec.View{Name: "Regions", Source: &spec.ViewSource{SQL: "SELECT id,name FROM $Unsafe.RegionTable"}}
		if joined {
			root.Source.SQL += " JOIN $Unsafe.RegionTable v ON v.id=r.id"
		}
		root.Relations = []*spec.Relation{{View: child, On: []*spec.RelationLink{{ParentColumn: "id", ChildColumn: "id"}}}}
		component.RootView = root
		input := &TemplateInput{Value: reflect.ValueOf(struct{ RegionTable string }{"regions"})}
		require.NoError(t, New(Connections{"main": h.DB}).Refine(t.Context(), component, nil, input))
		require.Equal(t, "regions", root.Source.Table)
		require.True(t, root.Columns[0].PrimaryKey)
		require.Empty(t, child.Source.Table)
		require.False(t, child.Columns[0].PrimaryKey)
	}
}
