package transcribe

import (
	"bytes"
	"context"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	tcolumn "github.com/viant/datly/transcribe/column"
	"github.com/viant/datly/typecatalog"
	loaderast "github.com/viant/x/loader/ast"
)

func TestGeneratorParameterTypeIdentityReload(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	writeSourceFile(t, root, "api/records/row.go", "package records\ntype ReadRow struct { ID int `sqlx:\"id\"` }\ntype Claims struct { Subject string }\n")
	writeSourceFile(t, root, "models/row.go", "package models\ntype Row struct { ID int `sqlx:\"id\"` }\n")
	catalog := typecatalog.NewCatalog()
	for _, dir := range []string{"api/records", "models"} {
		pkg, err := loaderast.LoadPackageFS(ctx, os.DirFS(root), dir)
		if err != nil {
			t.Fatal(err)
		}
		if err := catalog.RegisterPackage(typecatalog.TypeOriginPackage, pkg); err != nil {
			t.Fatal(err)
		}
	}
	db := testharness.NewSQLiteHarness(t)
	if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	const text = `#package('example.com/generated/api/records')
#import('self','example.com/generated/api/records')
#import('foreign','example.com/generated/models')
#setting($_ = $route('/records','GET'))
#setting($_ = $input_type('self.RecordsInput'))
#setting($_ = $output_type('self.RecordsOutput'))
#define($_ = $Data<[]*Record>(output/view))
#define($_ = $LocalOne<*self.ReadRow>(param/LocalOne).Optional())
#define($_ = $Local<[]*self.ReadRow>(view/Local) /* SELECT id FROM records */)
#define($_ = $Foreign<[]*foreign.Row>(view/Foreign) /* SELECT id FROM records */)
#define($_ = $Claims<string,*self.Claims>(header/Claims).Optional().WithCodec('json'))
SELECT r.*,type(r,'Record') FROM records r`
	writeSourceFile(t, root, "source/records.dql", text)
	refiner := tcolumn.New(tcolumn.Connections{"main": db.DB})
	request := GenerationRequest{Destination: root, Source: &Source{Name: "Records", Scope: "example.com/generated/api/records", Text: text, Connector: "main", ColumnRefiner: refiner, Types: catalog}}
	request.Source.GoBuild = request.Source.GoBuild.WithContext(nil)
	request.Source.GoBuild.Dir = root
	compiled, err := NewCompiler().Compile(ctx, request.Source)
	if err != nil {
		t.Fatal(err)
	}
	originalParams, err := json.Marshal(compiled.Component.Parameters)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := (Generator{Operation: "get"}).Generate(ctx, GenerationRequest{Destination: root, Compiled: compiled}); err != nil {
		t.Fatal(err)
	}
	afterParams, err := json.Marshal(compiled.Component.Parameters)
	if err != nil || !bytes.Equal(originalParams, afterParams) {
		t.Fatal("compiled authored parameter declarations changed")
	}
	assertCompiled := func(dir string) {
		t.Helper()
		cmd := exec.Command("go", "test", "-mod=mod", "./...")
		cmd.Dir = dir
		cmd.Env = append(os.Environ(), "GOWORK=off", "GO111MODULE=on")
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("generated module compile: %v\n%s", err, output)
		}
	}
	assertCompiled(root)
	snapshot := func(dir string) map[string]string {
		t.Helper()
		result := map[string]string{}
		for _, name := range []string{"input.go", "output.go"} {
			data, err := os.ReadFile(filepath.Join(dir, "api/records", name))
			if err != nil {
				t.Fatal(err)
			}
			result[name] = string(data)
		}
		return result
	}
	before := snapshot(root)
	if _, err := (Generator{Operation: "get"}).Generate(ctx, request); err != nil {
		t.Fatal(err)
	}
	for name, data := range snapshot(root) {
		if before[name] != data {
			t.Fatalf("repeat generation changed %s", name)
		}
	}
	input, err := os.ReadFile(filepath.Join(root, "api/records/input.go"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(input), `self "example.com/generated/api/records"`) {
		t.Fatal("generated input imports its own package")
	}
	// Discovery reconstructs parameters from generated Go metadata, without the
	// original source's import aliases. Retranscribe into a separate module so
	// destination collision policy for authored rows is independent of this test.
	discovery := &Discovery{BaseDir: root, Include: []string{"example.com/generated/source"}, SourceFile: "records.dql", Connector: "main", ColumnRefiner: refiner}
	destination := t.TempDir()
	testharness.WriteGeneratedGoMod(t, destination)
	if _, err := discovery.Generate(ctx, destination); err != nil {
		t.Fatalf("reload generated parameter identity: %v\n%s", err, input)
	}
	reloaded, err := os.ReadFile(filepath.Join(destination, "api/records/input.go"))
	if err != nil {
		t.Fatal(err)
	}
	if request.Source.Text != text {
		t.Fatal("authored DQL changed")
	}
	for _, want := range []string{"dataType=*example.com/generated/api/records.ReadRow", "dataType=[]*example.com/generated/api/records.ReadRow", "dataType=[]*example.com/generated/models.Row", "dataType=string"} {
		if !strings.Contains(string(input), want) || !strings.Contains(string(reloaded), want) {
			t.Errorf("missing canonical wire metadata %q:\n%s", want, input)
		}
	}
	for _, data := range [][]byte{input, reloaded} {
		file, err := parser.ParseFile(token.NewFileSet(), "input.go", data, 0)
		if err != nil {
			t.Fatal(err)
		}
		found := false
		ast.Inspect(file, func(node ast.Node) bool {
			field, ok := node.(*ast.Field)
			if !ok || len(field.Names) != 1 || field.Names[0].Name != "Claims" || field.Tag == nil || !strings.Contains(field.Tag.Value, "codec:") {
				return true
			}
			found = true
			pointer, ok := field.Type.(*ast.StarExpr)
			if !ok {
				t.Errorf("decoded Claims is not a pointer: %T", field.Type)
				return false
			}
			named, ok := pointer.X.(*ast.Ident)
			if !ok || named.Name != "Claims" {
				t.Errorf("decoded Claims type changed: %#v", pointer.X)
			}
			return false
		})
		if !found {
			t.Fatal("decoded Claims field missing")
		}
	}
	if strings.Contains(string(input), "dataType=*example.com/generated/api/records.Claims") {
		t.Fatal("codec destination replaced source wire type")
	}
}
