package transcribe

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/bindly/provider/body"
	"github.com/viant/datly/internal/testfixture/castmodel"
	"github.com/viant/datly/internal/testharness/sqlite"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/dml"
	"github.com/viant/datly/sql/reader"
	rcompile "github.com/viant/datly/sql/reader/compiler"
	tcolumn "github.com/viant/datly/transcribe/column"
	"github.com/viant/datly/transcribe/generate"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
)

func TestCASTImportedRichShapeSQLite(t *testing.T) {
	for _, tc := range []struct {
		name, request string
		unit          string
		cap           int
		wildcard      bool
	}{
		{"cap zero", `{"id":1,"bounds":{"cap":0}}`, "days", 0, false},
		{"unit empty", `{"id":1,"bounds":{"unit":""}}`, "", 7, false},
		{"logical omitted", `{"id":1}`, "days", 7, false},
		{"wildcard cap zero", `{"id":1,"bounds":{"cap":0}}`, "days", 0, true},
		{"wildcard unit empty", `{"id":1,"bounds":{"unit":""}}`, "", 7, true},
		{"wildcard logical omitted", `{"id":1}`, "days", 7, true},
		{"quoted cap zero", `{"id":1,"bounds":{"cap":0}}`, "days", 0, false},
		{"backtick unit empty", `{"id":1,"bounds":{"unit":""}}`, "", 7, false},
		{"CTE logical omitted", `{"id":1}`, "days", 7, false},
		{"CTE cap zero", `{"id":1,"bounds":{"cap":0}}`, "days", 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			h := sqlite.New(t)
			if err := h.ExecStatements(ctx, `CREATE TABLE records(id INTEGER PRIMARY KEY,unit TEXT,cap INTEGER,labels TEXT)`, `INSERT INTO records VALUES(1,'days',7,'["physical"]')`); err != nil {
				t.Fatal(err)
			}
			catalog := typecatalog.NewCatalog()
			if err := catalog.Register(typecatalog.TypeOriginPackage, x.NewType(reflect.TypeOf(castmodel.Bounds{}))); err != nil {
				t.Fatal(err)
			}
			const pkg = "github.com/viant/datly/internal/testfixture/castmodel"
			text := `#import('domain','` + pkg + `')
#setting($_ = $route('/records','GET'))
SELECT r.*, CAST(r.bounds AS domain.Bounds), tag(r.bounds,'sqlx:"-"'), tag(r.unit,'internal:"true"'), tag(r.cap,'internal:"true"'), CAST(r.labels AS '[]string'), tag(r.labels,'sqlx:"labels,enc=JSON"')
FROM (SELECT id, unit, cap, labels, '' AS bounds FROM records) r`
			if tc.wildcard {
				text = strings.Replace(text, "SELECT id, unit, cap, labels, '' AS bounds FROM records", "SELECT o.*, '' AS bounds FROM records o", 1)
			}
			if strings.HasPrefix(tc.name, "quoted") {
				text = strings.Replace(text, "'' AS bounds", `'' AS "bounds"`, 1)
			}
			if strings.HasPrefix(tc.name, "backtick") {
				text = strings.Replace(text, "'' AS bounds", "'' AS `bounds`", 1)
			}
			expectedInner := ""
			if strings.HasPrefix(tc.name, "CTE") {
				inner := `WITH physical AS (SELECT id,unit,cap,labels FROM records), shaped AS (SELECT id,unit,cap,labels, json_extract('{"arbitrary":1}', '$.arbitrary') AS bounds FROM physical) SELECT id,unit,cap,labels,bounds FROM shaped`
				expectedInner = inner
				text = strings.Replace(text, "SELECT id, unit, cap, labels, '' AS bounds FROM records", inner, 1)
			}
			compiled, err := NewCompiler().Compile(ctx, &Source{Name: "Records", Scope: "example.com/app/records", Connector: "main", Types: catalog, ColumnRefiner: tcolumn.New(tcolumn.Connections{"main": h.DB}), Text: text})
			if err != nil {
				t.Fatal(err)
			}
			if expectedInner != "" && !strings.Contains(compiled.Component.RootView.Source.SQL, expectedInner) {
				t.Fatal("inner CTE text changed")
			}
			resolver, err := typecatalog.NewResolver(catalog, typecatalog.TranscribeAuthority, &typecatalog.ResolutionContext{Imports: []typecatalog.PackageImport{{Alias: "domain", Package: pkg}}})
			if err != nil {
				t.Fatal(err)
			}
			generated, err := generate.New(generate.Input{Component: compiled.Component, TypeResolver: resolver, TargetPackage: "example.com/generated"}).Plan()
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, view := range generated.Views {
				for _, field := range view.Fields {
					if field.Name == "Bounds" {
						found = true
						if field.Type != "domain.Bounds" || !strings.Contains(field.Tag, `sqlx:"-"`) {
							t.Fatalf("field=%+v", field)
						}
					}
				}
			}
			if !found {
				t.Fatal("rich field missing")
			}
			for _, helper := range generated.HelperTypes {
				if helper.Name == "Bounds" {
					t.Fatal("duplicated imported domain shape")
				}
			}
			type output struct{ Rows []*castmodel.Record }
			plan, err := rcompile.Compile(rcompile.Input{Component: compiled.Component, InputType: reflect.TypeOf(struct{}{}), OutputType: reflect.TypeOf(output{}), DirectViewField: "Rows"})
			if err != nil {
				t.Fatal(err)
			}
			execution, err := reader.NewExecution(reader.Config{Component: compiled.Component, InputType: reflect.TypeOf(struct{}{}), OutputType: reflect.TypeOf(output{}), Plan: plan, SQL: &dsql.SQLComponent{DB: h.DB}})
			if err != nil {
				t.Fatal(err)
			}
			actual, err := execution.Read(ctx, &struct{}{}, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			rows := actual.(*output).Rows
			if len(rows) != 1 || rows[0].Bounds.Unit != "days" || rows[0].Bounds.Cap != 7 || !reflect.DeepEqual(rows[0].Labels, []string{"physical"}) {
				t.Fatalf("rows=%+v", rows)
			}
			encoded, err := json.Marshal(rows[0])
			if err != nil {
				t.Fatal(err)
			}
			var public struct {
				Unit   *string          `json:"unit"`
				Cap    *int             `json:"cap"`
				Has    any              `json:"Has"`
				Bounds castmodel.Bounds `json:"bounds"`
			}
			if err = json.Unmarshal(encoded, &public); err != nil {
				t.Fatal(err)
			}
			if public.Unit != nil || public.Cap != nil || public.Has != nil || public.Bounds.Unit != "days" || public.Bounds.Cap != 7 {
				t.Fatalf("internal leak: %s", encoded)
			}
			provider, err := body.New([]byte(tc.request), "application/json", nil)
			if err != nil {
				t.Fatal(err)
			}
			value, found, err := provider.Value(ctx, reflect.TypeOf(castmodel.Record{}), "")
			if err != nil || !found {
				t.Fatalf("bind: %v", err)
			}
			row := value.(castmodel.Record)
			if err = row.Init(ctx); err != nil {
				t.Fatal(err)
			}
			data := dml.NewData(h.DB)
			if err = data.BeginInvocation(); err != nil {
				t.Fatal(err)
			}
			defer data.Complete(ctx, context.Canceled)
			if row.Has.Unit || row.Has.Cap {
				if err = data.Update("records", &row); err != nil {
					t.Fatal(err)
				}
			}
			if err = data.Complete(ctx, nil); err != nil {
				t.Fatal(err)
			}
			h.AssertQuery(t, ctx, sqlite.Query{SQL: "SELECT unit,cap,labels FROM records WHERE id=1"}, []struct {
				Unit   string
				Cap    int
				Labels string
			}{{tc.unit, tc.cap, `["physical"]`}})
		})
	}
}

func TestCASTUnknownImportedShapeFailsGeneration(t *testing.T) {
	_, err := NewCompiler().Compile(context.Background(), &Source{Name: "Records", Types: typecatalog.NewCatalog(), Text: `#import('domain','example.com/missing')
#setting($_ = $route('/records','GET'))
SELECT r.*,CAST(r.bounds AS domain.Bounds),tag(r.bounds,'sqlx:"-"') FROM records r`})
	if err == nil {
		t.Fatal("unresolved imported type emitted")
	}
}

func TestCASTNamedPseudoViewOwnershipSQLite(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	if err := h.ExecStatements(ctx, `CREATE TABLE records(id INTEGER, unknown)`, `CREATE TABLE known(id INTEGER)`); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, sql, failure string
	}{
		{"child owns declaration", `SELECT r.*, child.*, CAST(child.bounds AS int), tag(child.bounds,'sqlx:"-"') FROM (SELECT id FROM records) r JOIN (SELECT id, '' AS bounds FROM records) child ON child.id=r.id`, ""},
		{"child wildcard", `SELECT r.*, child.*, CAST(child.bounds AS int), tag(child.bounds,'sqlx:"-"') FROM (SELECT id FROM records) r JOIN (SELECT o.*, '' AS bounds FROM known o) child ON child.id=r.id`, ""},
		{"root does not authorize child", `SELECT r.*, child.*, CAST(r.bounds AS int), tag(r.bounds,'sqlx:"-"') FROM (SELECT id, '' AS bounds FROM records) r JOIN (SELECT id, id+0 AS bounds FROM records) child ON child.id=r.id`, "unable discover column bounds type"},
		{"unknown physical remains strict", `SELECT r.*, CAST(r.bounds AS int), tag(r.bounds,'sqlx:"-"') FROM (SELECT o.*, '' AS bounds FROM records o) r`, "unable discover column unknown type"},
		{"inner alias is not view alias", `SELECT r.*, CAST(o.bounds AS int), tag(o.bounds,'sqlx:"-"') FROM (SELECT id, '' AS bounds FROM records o) r`, "has no canonical view"},
		{"database scope annotation restricted", `SELECT r.*, CAST(r.bounds AS int), tag(r.bounds,'sqlx:"-"') FROM (SELECT id, '' AS bounds, tag(o.id,'internal:"true"') FROM records o) r`, "outer"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			compiled, err := NewCompiler().Compile(ctx, &Source{Name: "Records", Connector: "main", ColumnRefiner: tcolumn.New(tcolumn.Connections{"main": h.DB}), Text: "#setting($_ = $route('/records','GET'))\n" + tc.sql})
			if tc.failure != "" {
				if err == nil || !strings.Contains(err.Error(), tc.failure) {
					t.Fatalf("expected %s, got %v", tc.failure, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			child := compiled.Component.RootView.Relations[0].View
			if len(compiled.Component.RootView.Columns) != 1 || len(child.Columns) != 2 || child.Columns[0].Name != "bounds" || child.Columns[0].Type.Name != "int" || !child.Columns[0].ExplicitType {
				t.Fatalf("declaration reached wrong view: %+v", compiled.Component.RootView)
			}
		})
	}
}
