package column

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/viant/datly/constant"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
	sqltemplate "github.com/viant/datly/sql/template"
	"github.com/viant/sqlx"
	sqlio "github.com/viant/sqlx/io"
	_ "github.com/viant/sqlx/metadata/product/sqlite"
	"github.com/viant/sqlx/metadata/sink"
)

func TestRefinerDiscoversAndMergesSQLiteColumns(t *testing.T) {
	harness := testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	if err := harness.ExecStatements(ctx, `CREATE TABLE events (
		id INTEGER NOT NULL,
		name TEXT,
		score REAL,
		enabled BOOLEAN
	)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	groupable := true
	component := &spec.Component{
		Settings: &spec.Settings{DefaultConnector: "main"},
		RootView: &spec.View{
			Name: "Events", Groupable: &groupable, Source: &spec.ViewSource{Table: "events"},
			Columns: []*spec.Column{{Name: "name", Source: "display_name", Tag: `json:"name"`}},
		},
	}
	if err := New(Connections{"main": harness.DB}).Refine(ctx, component, nil, nil); err != nil {
		t.Fatalf("Refine() error = %v", err)
	}
	columns := component.RootView.Columns
	if len(columns) != 4 || columns[0].Name != "name" || columns[1].Name != "id" || columns[2].Name != "score" || columns[3].Name != "enabled" {
		t.Fatalf("columns = %+v", columns)
	}
	if columns[0].Source != "display_name" || columns[0].Tag != `json:"name"` || columns[0].Type.Name != "string" {
		t.Fatalf("merged name column = %+v", columns[0])
	}
	for _, column := range columns {
		if column.Type.Name == "" || column.Groupable == nil || !*column.Groupable {
			t.Fatalf("incomplete discovered column = %+v", column)
		}
	}
	if columns[1].Type.Name != "int" || columns[2].Type.Name != "float64" || columns[3].Type.Name != "bool" {
		t.Fatalf("column types = %+v", columns)
	}
}

func TestRefinerEnrichesExplicitSQLiteTableConstraints(t *testing.T) {
	harness := testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	if err := harness.ExecStatements(ctx, `CREATE TABLE constrained_events (
		id INTEGER PRIMARY KEY NOT NULL,
		status TEXT NOT NULL DEFAULT 'new',
		note TEXT
	)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	component := &spec.Component{
		Settings: &spec.Settings{DefaultConnector: "main"},
		RootView: &spec.View{Name: "Events", Source: &spec.ViewSource{Table: "constrained_events"}},
	}
	if err := New(Connections{"main": harness.DB}).Refine(ctx, component, nil, nil); err != nil {
		t.Fatalf("Refine() error = %v", err)
	}
	constraints, err := loadTableConstraints(ctx, harness.DB, "constrained_events")
	if err != nil {
		t.Fatalf("loadTableConstraints() error = %v", err)
	}
	columns := component.RootView.Columns
	if len(columns) != 3 || columns[0].Name != "id" || !columns[0].PrimaryKey {
		t.Fatalf("constraints = %+v, columns = first:%+v second:%+v third:%+v", constraints, columns[0], columns[1], columns[2])
	}
	if columns[1].Name != "status" || columns[1].Default == nil || *columns[1].Default != "'new'" {
		t.Fatalf("status constraint = %+v", columns[1])
	}
	if columns[2].Name != "note" || columns[2].Default != nil || columns[2].PrimaryKey || columns[2].Unique {
		t.Fatalf("note constraint = %+v", columns[2])
	}
}

func TestRefinerUsesProvenDirectTableLineage(t *testing.T) {
	harness := testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	if err := harness.ExecStatements(ctx, `CREATE TABLE lineage_events (
		id INTEGER PRIMARY KEY NOT NULL,
		status TEXT NOT NULL DEFAULT 'new'
	)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	tests := []struct {
		name        string
		source      *spec.ViewSource
		namespace   string
		wantPrimary bool
		wantDefault bool
	}{
		{
			name: "explicit table with alias",
			source: &spec.ViewSource{Table: "lineage_events", SQL: `SELECT e.id AS event_id, e.status
				FROM lineage_events e`},
			namespace: "e", wantPrimary: true, wantDefault: true,
		},
		{
			name: "direct query infers table authority",
			source: &spec.ViewSource{SQL: `SELECT e.id AS event_id, e.status
				FROM lineage_events e`},
			namespace: "e", wantPrimary: true, wantDefault: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			component := &spec.Component{
				Settings: &spec.Settings{DefaultConnector: "main"},
				RootView: &spec.View{Name: "Events", Namespace: test.namespace, Source: test.source},
			}
			if err := New(Connections{"main": harness.DB}).Refine(ctx, component, nil, nil); err != nil {
				t.Fatalf("Refine() error = %v", err)
			}
			columns := component.RootView.Columns
			if len(columns) != 2 || columns[0].Name != "event_id" || columns[0].PrimaryKey != test.wantPrimary ||
				(columns[1].Default != nil) != test.wantDefault {
				t.Fatalf("columns = event:%+v status:%+v", columns[0], columns[1])
			}
		})
	}
}

func TestRefinerResolvesEmbeddedSQLWithoutMutatingSource(t *testing.T) {
	harness := testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	if err := harness.ExecStatements(ctx,
		`CREATE TABLE owners (id INTEGER PRIMARY KEY)`,
		`CREATE TABLE events (id INTEGER PRIMARY KEY, owner_id INTEGER NOT NULL, name TEXT,
			FOREIGN KEY(owner_id) REFERENCES owners(id))`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	component := &spec.Component{
		Settings: &spec.Settings{DefaultConnector: "main"},
		RootView: &spec.View{Name: "Events", Source: &spec.ViewSource{
			URI: "events.sql",
		}},
	}
	resources := fstest.MapFS{"events.sql": &fstest.MapFile{Data: []byte(`SELECT record.id, record.owner_id, record.name
		FROM (SELECT e.* FROM events e) record`)}}
	if err := New(Connections{"main": harness.DB}).Refine(ctx, component, resources, nil); err != nil {
		t.Fatalf("Refine() error = %v", err)
	}
	if len(component.RootView.Columns) != 3 || component.RootView.Source.URI != "events.sql" || len(component.RootView.Source.Embeds) != 0 || component.RootView.Source.SQL != "" || component.RootView.Source.Table != "events" {
		t.Fatalf("component source/columns = %+v / %+v", component.RootView.Source, component.RootView.Columns)
	}
	if !component.RootView.Columns[0].PrimaryKey {
		t.Fatalf("embedded primary key was not discovered: %+v", component.RootView.Columns[0])
	}
	reference := sqlio.ParseTag(reflect.StructTag(component.RootView.Columns[1].Tag))
	if reference.RefTable != "owners" || reference.RefColumn != "id" {
		t.Fatalf("embedded foreign key was not discovered: tag=%q metadata=%+v", component.RootView.Columns[1].Tag, reference)
	}
}

func TestRefinerDiscoversColumnsWithUnresolvedPredicateBuilder(t *testing.T) {
	harness := testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	if err := harness.ExecStatements(ctx, `CREATE TABLE records (id INTEGER PRIMARY KEY, status TEXT NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	authored := `SELECT r.id, r.status FROM records r WHERE 1=1
${predicate.Builder().CombineAnd($predicate.FilterGroup(0, "AND")).Build("AND")}`
	component := &spec.Component{Settings: &spec.Settings{DefaultConnector: "main"}, RootView: &spec.View{
		Name: "Records", Source: &spec.ViewSource{SQL: authored},
	}}
	if err := New(Connections{"main": harness.DB}).Refine(ctx, component, nil, nil); err != nil {
		t.Fatal(err)
	}
	if component.RootView.Source.SQL != authored || len(component.RootView.Columns) != 2 || !component.RootView.Columns[0].PrimaryKey {
		t.Fatalf("source/columns = %q / %+v", component.RootView.Source.SQL, component.RootView.Columns)
	}
}

func TestRefinerDiscoversColumnsWithUnresolvedWherePredicateBuilder(t *testing.T) {
	harness := testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	if err := harness.ExecStatements(ctx, `CREATE TABLE records (id INTEGER PRIMARY KEY, status TEXT NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	authored := `SELECT r.id, r.status FROM records r
${predicate.Builder().CombineAnd($predicate.FilterGroup(1, "AND")).Build("WHERE")}`
	component := &spec.Component{Settings: &spec.Settings{DefaultConnector: "main"}, RootView: &spec.View{
		Name: "Records", Source: &spec.ViewSource{SQL: authored},
	}}
	values, err := constant.New(map[string]string{})
	if err != nil {
		t.Fatal(err)
	}
	input := &TemplateInput{Const: values, Value: reflect.ValueOf(struct{}{})}
	if err := New(Connections{"main": harness.DB}).Refine(ctx, component, nil, input); err != nil {
		t.Fatal(err)
	}
	if len(component.RootView.Columns) != 2 {
		t.Fatalf("columns = %+v", component.RootView.Columns)
	}
}

func TestRefinerRequiresExactConnector(t *testing.T) {
	component := &spec.Component{RootView: &spec.View{Name: "Events", Source: &spec.ViewSource{Table: "events"}}}
	err := New(Connections{}).Refine(context.Background(), component, nil, nil)
	if err == nil || !strings.Contains(err.Error(), "connector is required") {
		t.Fatalf("Refine() error = %v", err)
	}
}

func TestRefinerSkipsMetadataOnlySourceWithoutConnector(t *testing.T) {
	canonical := &spec.Column{Name: "id", Type: spec.TypeRef{Name: "int"}}
	component := &spec.Component{RootView: &spec.View{
		Name: "Events", Source: &spec.ViewSource{Bindings: &spec.ViewBindings{}},
		Columns: []*spec.Column{canonical},
	}}
	if err := New(Connections{}).Refine(context.Background(), component, nil, nil); err != nil {
		t.Fatalf("Refine() error = %v", err)
	}
	if len(component.RootView.Columns) != 1 || component.RootView.Columns[0] != canonical {
		t.Fatalf("columns = %+v", component.RootView.Columns)
	}
}

func TestRefinerEvaluatesVeltySQLWithoutHeuristicStripping(t *testing.T) {
	harness := testharness.NewSQLiteHarness(t)
	if err := harness.ExecStatements(context.Background(), "CREATE TABLE events (id INTEGER, name TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	component := &spec.Component{
		Settings: &spec.Settings{DefaultConnector: "main"},
		RootView: &spec.View{Name: "Events", Source: &spec.ViewSource{
			SQL: "SELECT #if($enabled) id #else name #end FROM events",
		}},
	}
	type input struct{ Enabled bool }
	inputType := reflect.TypeOf(input{})
	field, _ := inputType.FieldByName("Enabled")
	err := New(Connections{"main": harness.DB}).Refine(context.Background(), component, nil, &TemplateInput{
		Value:     reflect.ValueOf(input{Enabled: true}),
		Variables: []sqltemplate.Variable{{Name: "enabled", FieldIndex: field.Index}},
		ParameterResolver: sqlx.ParameterResolver(func(name string) (any, bool, error) {
			return true, strings.EqualFold(name, "enabled"), nil
		}),
	})
	if err != nil {
		t.Fatalf("Refine() error = %v", err)
	}
	if len(component.RootView.Columns) != 1 || component.RootView.Columns[0].Name != "id" {
		t.Fatalf("columns = %+v", component.RootView.Columns)
	}
}

func TestRefinerThreadsParentNonWindowSQLThroughOrdinaryRelation(t *testing.T) {
	harness := testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	if err := harness.ExecStatements(ctx, "CREATE TABLE users (id INTEGER, tenant_id INTEGER)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	child := &spec.View{Name: "Totals", Source: &spec.ViewSource{
		SQL: `#if($Enabled) SELECT parent.id AS total FROM ($View.Users.NonWindowSQL) parent #end`,
	}}
	component := &spec.Component{
		Name:     "UsersComponent",
		Settings: &spec.Settings{DefaultConnector: "main"},
		RootView: &spec.View{
			Name: "Users", Source: &spec.ViewSource{
				SQL: `#if($Enabled) SELECT id, tenant_id FROM users WHERE tenant_id = $TenantID #end`,
			},
			Relations: []*spec.Relation{{Name: "Totals", Kind: spec.RelationKindDerived, View: child}},
		},
	}
	type input struct {
		Enabled  bool
		TenantID int
	}
	inputType := reflect.TypeOf(input{})
	enabledField, _ := inputType.FieldByName("Enabled")
	tenantField, _ := inputType.FieldByName("TenantID")
	err := New(Connections{"main": harness.DB}).Refine(ctx, component, nil, &TemplateInput{
		Value: reflect.ValueOf(input{Enabled: true, TenantID: 7}),
		Variables: []sqltemplate.Variable{
			{Name: "Enabled", FieldIndex: enabledField.Index},
			{Name: "TenantID", FieldIndex: tenantField.Index},
		},
		ParameterResolver: sqlx.ParameterResolver(func(name string) (any, bool, error) {
			switch {
			case strings.EqualFold(name, "Enabled"):
				return true, true, nil
			case strings.EqualFold(name, "TenantID"):
				return 7, true, nil
			default:
				return nil, false, nil
			}
		}),
	})
	if err != nil {
		t.Fatalf("Refine() error = %v", err)
	}
	if len(component.RootView.Columns) != 2 || len(child.Columns) != 1 || child.Columns[0].Name != "total" {
		t.Fatalf("root columns = %+v, child columns = %+v", component.RootView.Columns, child.Columns)
	}
}

func TestCanonicalColumnsRejectsGoFieldCollision(t *testing.T) {
	_, err := canonicalColumns([]*sink.Column{{Name: "user_id", Type: "INTEGER"}, {Name: "UserId", Type: "INTEGER"}}, false)
	if err == nil || !strings.Contains(err.Error(), "map to Go field") {
		t.Fatalf("canonicalColumns() error = %v", err)
	}
}

func TestMergeColumnsDetachesCanonicalCodec(t *testing.T) {
	base := &spec.Column{Name: "name", Codec: &spec.Codec{Body: "AsString", Args: []string{"trim"}}}
	merged := mergeColumns([]*spec.Column{base}, []*spec.Column{{Name: "name", Type: spec.TypeRef{Name: "string"}}})
	merged[0].Codec.Args[0] = "changed"
	if base.Codec.Args[0] != "trim" {
		t.Fatalf("merge mutated base codec: %+v", base.Codec)
	}
}

func TestMergeColumnsPreservesAuthoredTypeAndExplicitGroupable(t *testing.T) {
	authoredGroupable := false
	discoveredGroupable := true
	base := &spec.Column{
		Name: "value", Type: spec.TypeRef{Package: "example.com/model", Name: "Value"}, Groupable: &authoredGroupable,
	}
	merged := mergeColumns([]*spec.Column{base}, []*spec.Column{{
		Name: "value", Type: spec.TypeRef{Name: "string"}, Groupable: &discoveredGroupable,
	}})
	if len(merged) != 1 || merged[0].Type.Package != "example.com/model" || merged[0].Type.Name != "Value" ||
		merged[0].Groupable == nil || *merged[0].Groupable {
		t.Fatalf("merged column = %+v", merged)
	}
	if base.Groupable == nil || *base.Groupable {
		t.Fatalf("merge mutated authored column = %+v", base)
	}
}

func TestRefinerInvariantCanonicalSourceAlias(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	if err := h.ExecStatements(ctx, `CREATE TABLE ORDERS(WINDOW_START INTEGER)`); err != nil {
		t.Fatal(err)
	}
	component := &spec.Component{Settings: &spec.Settings{DefaultConnector: "main"}, RootView: &spec.View{Name: "Orders", Namespace: "orders", Source: &spec.ViewSource{SQL: `SELECT o.WINDOW_START FROM ORDERS o`}, Columns: []*spec.Column{{Name: "Start", Source: "WINDOW_START", Tag: `invariant:"Window"`}}}}
	if err := New(Connections{"main": h.DB}).Refine(ctx, component, nil, nil); err != nil {
		t.Fatal(err)
	}
	columns := component.RootView.Columns
	if len(columns) != 1 || columns[0].Name != "Start" || columns[0].Source != "WINDOW_START" || columns[0].Type.Name != "int" || columns[0].Tag != `invariant:"Window"` {
		t.Fatalf("columns=%+v", columns)
	}
}

func TestRefinerTemplateCollectionsSQLite(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	if err := h.ExecStatements(ctx, `CREATE TABLE events(id INTEGER PRIMARY KEY, name TEXT NOT NULL)`, `INSERT INTO events VALUES(1,'one'),(2,'two')`); err != nil {
		t.Fatal(err)
	}
	type input struct{ IDs []int }
	template := `#foreach($id in $IDs)$id#if($foreach.HasNext),#end#end`
	for _, sql := range []string{
		`SELECT id, name FROM events WHERE id IN (` + template + `)`,
		`WITH current AS (SELECT id, name FROM events WHERE id IN (` + template + `)) SELECT id, name FROM current`,
		`SELECT e.id, e.name FROM (SELECT id, name FROM events WHERE id IN (` + template + `)) e`,
	} {
		for name, ids := range map[string][]int{"empty": nil, "populated": {1, 2}} {
			t.Run(sql+"/"+name, func(t *testing.T) {
				view := &spec.View{Name: "Current", Source: &spec.ViewSource{SQL: sql, Table: "events"}}
				component := &spec.Component{Settings: &spec.Settings{DefaultConnector: "main"}, RootView: view}
				value := reflect.ValueOf(&input{IDs: ids})
				err := New(Connections{"main": h.DB}).Refine(ctx, component, nil, &TemplateInput{Value: value, Variables: []sqltemplate.Variable{{Name: "IDs", FieldIndex: []int{0}}}})
				if err != nil {
					t.Fatal(err)
				}
				if view.Source.SQL != sql {
					t.Fatalf("authored source changed: %s", view.Source.SQL)
				}
				if len(view.Columns) != 2 || view.Columns[0].Name != "id" || view.Columns[0].Type.Name != "int" || !view.Columns[0].PrimaryKey || view.Columns[1].Type.Name != "string" {
					t.Fatalf("metadata lost: %+v", view.Columns)
				}
			})
		}
	}
}
