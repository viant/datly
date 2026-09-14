package transcribe

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	tcolumn "github.com/viant/datly/transcribe/column"
	"github.com/viant/datly/typecatalog"
	xshape "github.com/viant/x/shape"
)

func TestProjectionRemovalRegeneratesReaderAndWriterShapes(t *testing.T) {
	for _, tc := range []struct {
		name    string
		handler HandlerOptions
	}{
		{name: "reader"},
		{name: "Go writer", handler: HandlerOptions{Target: HandlerGo, Operation: WritePost}},
		{name: "Velty writer", handler: HandlerOptions{Target: HandlerVelty, Operation: WritePost}},
		{name: "mutation writer", handler: HandlerOptions{Target: HandlerGo, Operation: WritePost, Go: GoHandlerOptions{Execution: GoExecutionMutation}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			h := testharness.NewSQLiteHarness(t)
			if err := h.ExecStatements(ctx, "CREATE TABLE EVENTS(ID INTEGER PRIMARY KEY AUTOINCREMENT, NAME TEXT NOT NULL, EXTRA INTEGER)"); err != nil {
				t.Fatal(err)
			}
			root := t.TempDir()
			testharness.WriteGeneratedGoMod(t, root)
			method := "GET"
			body := ""
			if tc.handler.Operation != "" {
				method = "POST"
				body = "#define($_ = $Events<[]*EventsView>(body/Data).Cardinality('Many').Required())\n"
			}
			header := fmt.Sprintf("#setting($_ = $route('/events', '%s'))\n%s#define($_ = $Status<int>(output/status).Output())\n#define($_ = $Data<[]*EventsView>(output/body))\n", method, body)
			catalog := typecatalog.NewCatalog()
			generate := func(projection string) *GeneratedPackage {
				t.Helper()
				result, err := NewCompiler().Transcribe(ctx, Request{Source: &Source{Scope: "example.com/generated/events", Name: "Events", Connector: "main", Text: header + "SELECT " + projection + " FROM EVENTS", Types: catalog, ColumnRefiner: tcolumn.New(tcolumn.Connections{"main": h.DB})}, Destination: root, Options: Options{Handler: tc.handler}})
				if err != nil {
					t.Fatal(err)
				}
				return result
			}
			logical := `,CAST(EVENTS.Logical AS string),tag(EVENTS.Logical,'sqlx:"-"')`
			first := generate("ID, NAME, EXTRA,CAST(EVENTS.EXTRA AS int)" + logical)
			rowType := first.Result.Plan.RootViewType
			shapePath := ""
			for _, view := range first.Result.Plan.Views {
				if view.Name == rowType {
					shapePath = filepath.Join(root, "generated", view.Destination)
				}
			}
			if shapePath == "" {
				t.Fatalf("root row shape missing: %s", rowType)
			}
			original, err := os.ReadFile(shapePath)
			if err != nil {
				t.Fatal(err)
			}
			addition := fmt.Sprintf("package %s\ntype %s struct { AuthoredNote string `sqlx:\"-\" json:\"-\"` }", first.Package.Name, rowType)
			customized, err := (xshape.SourceParser{}).AppendStructFields(original, []byte(addition))
			if err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(shapePath, customized, 0644); err != nil {
				t.Fatal(err)
			}
			methodSource := fmt.Sprintf("package %s\n// This method remains application-owned.\nfunc(r *%s) AuthoredNoteValue() string { return r.AuthoredNote }\n", first.Package.Name, rowType)
			methodPath := filepath.Join(root, "generated", "authored_note.go")
			if err = os.WriteFile(methodPath, []byte(methodSource), 0644); err != nil {
				t.Fatal(err)
			}
			cast := generate("ID, NAME, EXTRA,CAST(EVENTS.EXTRA AS *int)" + logical)
			for _, view := range cast.Result.Plan.Views {
				if view.Name == rowType {
					field, found := view.Field("Extra")
					if !found || field.Type != "*int" {
						t.Fatalf("writer CAST type not applied: %+v", field)
					}
				}
			}
			castConsumer := fmt.Sprintf("package %s\nvar _ *int = %s{}.Extra\n", first.Package.Name, rowType)
			castPath := filepath.Join(root, "generated", "cast_projection_consumer_test.go")
			if err := os.WriteFile(castPath, []byte(castConsumer), 0644); err != nil {
				t.Fatal(err)
			}
			command := exec.Command("go", "test", "-mod=mod", "./...")
			command.Dir = root
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("generated CAST consumer: %v\n%s", err, output)
			}
			generate("ID, NAME, EXTRA,CAST(EVENTS.EXTRA AS int)" + logical)
			scalarConsumer := fmt.Sprintf("package %s\nvar _ int = %s{}.Extra\n", first.Package.Name, rowType)
			if err := os.WriteFile(castPath, []byte(scalarConsumer), 0644); err != nil {
				t.Fatal(err)
			}
			command = exec.Command("go", "test", "-mod=mod", "./...")
			command.Dir = root
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("generated reverse CAST consumer: %v\n%s", err, output)
			}
			if err := os.Remove(castPath); err != nil {
				t.Fatal(err)
			}
			generate("ID, NAME" + logical)
			after, err := os.ReadFile(shapePath)
			if err != nil {
				t.Fatal(err)
			}
			generate("ID, NAME" + logical)
			again, err := os.ReadFile(shapePath)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(after), "Extra ") {
				t.Fatalf("removed field still emitted: %s", after)
			}
			if string(after) != string(again) {
				t.Fatal("repeated generation changed shape bytes")
			}
			actualMethod, err := os.ReadFile(methodPath)
			if err != nil || string(actualMethod) != methodSource {
				t.Fatal("authored method changed", err)
			}
			consumer := fmt.Sprintf(`package %s
import("reflect";"testing")
func TestProjectionShape(t *testing.T){
 typ:=reflect.TypeOf(%s{})
 if _,ok:=typ.FieldByName("Extra");ok{t.Fatal("removed SQL column remains in Go shape")}
 if _,ok:=typ.FieldByName("Name");!ok{t.Fatal("retained column disappeared")}
 if _,ok:=typ.FieldByName("Logical");!ok{t.Fatal("retained logical declaration disappeared")}
 if field,ok:=typ.FieldByName("Has");ok{marker:=field.Type;if marker.Kind()==reflect.Ptr{marker=marker.Elem()};if _,exists:=marker.FieldByName("Extra");exists{t.Fatal("removed SQL column remains in presence shape")}}
 if got:=(&%s{AuthoredNote:"retained"}).AuthoredNoteValue();got!="retained"{t.Fatal("authored field or method lost")}
}
`, first.Package.Name, rowType, rowType)
			if err = os.WriteFile(filepath.Join(root, "generated", "projection_shape_test.go"), []byte(consumer), 0644); err != nil {
				t.Fatal(err)
			}
			command = exec.Command("go", "test", "-mod=mod", "./...")
			command.Dir = root
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("generated projection consumer: %v\n%s", err, output)
			}
		})
	}
}

func TestProjectionHelpersUseCurrentShapeAcrossRegeneration(t *testing.T) {
	ctx := context.Background()
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(ctx, "CREATE TABLE EVENTS(ID INTEGER PRIMARY KEY, NAME TEXT NOT NULL, EXTRA INTEGER)"); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	catalog := typecatalog.NewCatalog()
	attempt := func(columns, helper string) (*GeneratedPackage, error) {
		t.Helper()
		source := &Source{Scope: "example.com/generated/events", Name: "Events", Connector: "main", Types: catalog, ColumnRefiner: tcolumn.New(tcolumn.Connections{"main": h.DB}), Text: `#setting($_ = $route('/events','POST'))
#define($_ = $Events<[]*EventsView>(body/Data).Cardinality('Many').Required())
#define($_ = $Derived<?>(param/Events) /* ` + helper + ` */)
#define($_ = $Status<int>(output/status).Output())
#define($_ = $Data<[]*EventsView>(output/body))
SELECT ` + columns + ` FROM EVENTS`}
		return NewCompiler().Transcribe(ctx, Request{Source: source, Destination: root})
	}
	generate := func(columns, helper string) *GeneratedPackage {
		t.Helper()
		result, err := attempt(columns, helper)
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	first := generate("ID,NAME,EXTRA,CAST(EVENTS.EXTRA AS int)", "SELECT Extra AS Value FROM `/`")
	updated := generate("ID,NAME,EXTRA,CAST(EVENTS.EXTRA AS *int)", "SELECT Extra AS Value FROM `/`")
	if len(updated.Result.Plan.HelperTypes) != 1 || updated.Result.Plan.HelperTypes[0].Fields[0].Type != "*int" {
		t.Fatalf("stale helper type: %+v", updated.Result.Plan.HelperTypes)
	}
	consumerPath := filepath.Join(root, "generated", "helper_shape_test.go")
	source := fmt.Sprintf("package %s\nvar _ *int = DerivedRow{}.Value\n", first.Package.Name)
	if err := os.WriteFile(consumerPath, []byte(source), 0644); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("go", "test", "-mod=mod", "./...")
	command.Dir = root
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("CAST helper consumer: %v\n%s", err, output)
	}
	if err := os.Remove(consumerPath); err != nil {
		t.Fatal(err)
	}
	shapePath := filepath.Join(root, "generated", first.Result.Plan.ViewDest)
	beforeFailure, err := os.ReadFile(shapePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := attempt("ID,NAME", "SELECT Extra AS Value FROM `/`"); err == nil {
		t.Fatal("stale generated catalog supplied a removed helper source field")
	}
	afterFailure, err := os.ReadFile(shapePath)
	if err != nil || string(afterFailure) != string(beforeFailure) {
		t.Fatal("failed helper regeneration changed source", err)
	}
	generate("ID,NAME", "SELECT Name FROM `/`")
	source = fmt.Sprintf(`package %s
import("reflect";"testing")
func TestHelperProjection(t *testing.T){
 if _,ok:=reflect.TypeOf(DerivedRow{}).FieldByName("Value");ok{t.Fatal("old generated helper field retained")}
 if _,ok:=reflect.TypeOf(DerivedRow{}).FieldByName("Name");!ok{t.Fatal("current helper projection missing")}
 if _,ok:=reflect.TypeOf(EventsView{}).FieldByName("Extra");ok{t.Fatal("stale catalog restored removed field")}
}
`, first.Package.Name)
	if err := os.WriteFile(consumerPath, []byte(source), 0644); err != nil {
		t.Fatal(err)
	}
	command = exec.Command("go", "test", "-mod=mod", "./...")
	command.Dir = root
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("removed helper consumer: %v\n%s", err, output)
	}
}
