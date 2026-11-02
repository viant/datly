package transcribe

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	tcolumn "github.com/viant/datly/transcribe/column"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
)

func TestReaderExplicitJSONOmission(t *testing.T) {
	type useCase struct{ description, input string }
	for _, tc := range []useCase{
		{"direct projection", "SELECT r.*, type(r,'Record'), tag(r.UNIT_RATE,'json:\"unitRate,omitempty\"') FROM records r"},
		{"derived projection", "SELECT r.*, type(r,'Record'), tag(r.UNIT_RATE,'json:\"unitRate,omitempty\"') FROM (SELECT * FROM records) r"},
	} {
		t.Run(tc.description, func(t *testing.T) {
			h := testharness.NewSQLiteHarness(t)
			ctx := context.Background()
			if err := h.ExecStatements(ctx, "CREATE TABLE records (ID INTEGER PRIMARY KEY, UNIT_RATE REAL)"); err != nil {
				t.Fatal(err)
			}
			root := t.TempDir()
			testharness.WriteGeneratedGoMod(t, root)
			catalog := typecatalog.NewCatalog()
			previous := reflect.TypeOf(struct {
				ID       int
				UnitRate *float64 `sqlx:"UNIT_RATE" json:"unitRate"`
			}{})
			if err := catalog.Register(typecatalog.TypeOriginGenerated, x.NewType(previous, x.WithName("Record"), x.WithPkgPath("example.com/generated/records"))); err != nil {
				t.Fatal(err)
			}
			generated, err := (Generator{Operation: "get"}).Generate(ctx, GenerationRequest{Destination: root, Source: &Source{Types: catalog, Scope: "example.com/generated/records", Name: "Records", Connector: "main", ColumnRefiner: tcolumn.New(tcolumn.Connections{"main": h.DB}), Text: "#package('example.com/generated/records')\n#setting($_ = $route('/records','GET'))\n#setting($_ = $case_format('lc'))\n" + tc.input}})
			if err != nil {
				t.Fatal(err)
			}
			for _, view := range generated.Result.Plan.Views {
				for _, field := range view.Fields {
					if field.Name == "UnitRate" {
						if got := reflect.StructTag(field.Tag).Get("json"); got != "unitRate,omitempty" {
							t.Fatalf("JSON tag = %q, want unitRate,omitempty", got)
						}
						return
					}
				}
			}
			t.Fatal("UnitRate field missing")
		})
	}
}

func TestReaderDiscoveryRefreshesJSONOmission(t *testing.T) {
	ctx := context.Background()
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(ctx, "CREATE TABLE records (ID INTEGER PRIMARY KEY, UNIT_RATE REAL)"); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	writeSourceFile(t, root, "go.mod", "module example.com/refresh\n\ngo 1.25\n")
	source := `#package('example.com/refresh/generated')
#setting($_ = $route('/records','GET'))
#setting($_ = $input_type('example.com/refresh/generated.Input'))
#setting($_ = $output_type('example.com/refresh/generated.Output'))
#setting($_ = $case_format('lc'))
#define($_ = $Data<[]*Record>(output/view))
SELECT r.*,type(r,'Record'),tag(r.UNIT_RATE,'json:"unitRate"') FROM (SELECT * FROM records) r`
	refiner := tcolumn.New(tcolumn.Connections{"main": h.DB})
	_, err := (Generator{Operation: "get"}).Generate(ctx, GenerationRequest{Destination: root, Source: &Source{Scope: "example.com/refresh/dql", Name: "records", Text: source, Connector: "main", ColumnRefiner: refiner}})
	if err != nil {
		t.Fatal(err)
	}
	writeSourceFile(t, root, "dql/records.dql", strings.Replace(source, `json:"unitRate"`, `json:"unitRate,omitempty"`, 1))
	project, err := (&Discovery{BaseDir: root, Include: []string{"example.com/refresh/dql"}, SourceFile: "records.dql", Connector: "main", ColumnRefiner: refiner}).Compile(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(project.Components) != 1 {
		t.Fatalf("components=%d", len(project.Components))
	}
	generated, err := (Generator{Operation: "get"}).Generate(ctx, GenerationRequest{Destination: root, Compiled: project.Components[0]})
	if err != nil {
		t.Fatal(err)
	}
	for _, view := range generated.Result.Plan.Views {
		for _, field := range view.Fields {
			if field.Name == "UnitRate" {
				if got := reflect.StructTag(field.Tag).Get("json"); got != "unitRate,omitempty" {
					t.Fatalf("JSON tag=%q", got)
				}
				return
			}
		}
	}
	t.Fatal("UnitRate missing from regenerated views")
}

func TestWriterDiscoveryRefreshesSQLProjection(t *testing.T) {
	ctx := context.Background()
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(ctx, "CREATE TABLE records (ID INTEGER PRIMARY KEY, NAME TEXT, UNIT_RATE REAL)"); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	writeSourceFile(t, root, "go.mod", "module example.com/refresh\n\ngo 1.25\n")
	source := `#package('example.com/refresh/generated')
#setting($_ = $route('/records','POST'))
#setting($_ = $input_type('example.com/refresh/generated.Input'))
#setting($_ = $output_type('example.com/refresh/generated.Output'))
#define($_ = $Data<[]*Record>(body/data).Optional())
SELECT r.*,type(r,'Record') FROM (SELECT ID, NAME, UNIT_RATE FROM records WHERE ID > 0) r`
	refiner := tcolumn.New(tcolumn.Connections{"main": h.DB})
	_, err := (Generator{Operation: "post"}).Generate(ctx, GenerationRequest{Destination: root, Source: &Source{Scope: "example.com/refresh/dql", Name: "records", Text: source, Connector: "main", ColumnRefiner: refiner}})
	if err != nil {
		t.Fatal(err)
	}
	writeSourceFile(t, root, "dql/records.dql", strings.Replace(source, "WHERE ID > 0", "WHERE ID > 1", 1))
	project, err := (&Discovery{BaseDir: root, Include: []string{"example.com/refresh/dql"}, SourceFile: "records.dql", Connector: "main", ColumnRefiner: refiner}).Compile(ctx)
	if err != nil {
		t.Fatal(err)
	}
	generated, err := (Generator{Operation: "post"}).Generate(ctx, GenerationRequest{Destination: root, Compiled: project.Components[0]})
	if err != nil {
		t.Fatal(err)
	}
	foundSQL := false
	for _, asset := range generated.Result.Plan.Resources.Files {
		if strings.Contains(asset.Content, "WHERE ID > 1") {
			foundSQL = true
		}
	}
	if !foundSQL {
		t.Fatalf("regenerated SQL retained the old projection: %+v", generated.Result.Plan.Resources.Files)
	}
	for _, v := range generated.Result.Plan.Views {
		for _, f := range v.Fields {
			if f.Name == "UnitRate" {
				return
			}
		}
	}
	t.Fatal("UnitRate missing from regenerated writer")
}
