package transcribe

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/tag"
	"github.com/viant/datly/transcribe/column"
	linked "github.com/viant/datly/transcribe/testdata/linkedcontract"
	"github.com/viant/datly/typecatalog"
)

func TestGeneratorRequiresExplicitPackage(t *testing.T) {
	ctx := context.Background()
	db := testharness.NewSQLiteHarness(t)
	if err := db.ExecStatements(ctx, "CREATE TABLE RECORDS(ID INTEGER PRIMARY KEY,NAME TEXT)"); err != nil {
		t.Fatal(err)
	}
	for _, operation := range []string{"get", "patch", "post", "put"} {
		for _, entry := range []string{"Source", "Compiled"} {
			for _, tc := range []struct{ name, declaration string }{
				{"missing", ""}, {"imports_only", `#import('models','example.com/generated/models')`},
				{"blank", `#package('')`}, {"whitespace", `#package('   ')`}, {"malformed", `#package(123)`},
				{"traversal", `#package('../outside')`}, {"absolute", `#package('/outside')`}, {"foreign_module", `#package('example.net/other')`},
			} {
				t.Run(operation+"/"+entry+"/"+tc.name, func(t *testing.T) {
					root := t.TempDir()
					testharness.WriteGeneratedGoMod(t, root)
					before, err := os.ReadFile(filepath.Join(root, "go.mod"))
					if err != nil {
						t.Fatal(err)
					}
					source := &Source{Name: "Records", Scope: "example.com/generated/source", Connector: "main", Types: typecatalog.NewCatalog(), ColumnRefiner: column.New(column.Connections{"main": db.DB}), Text: tc.declaration + "\n" + fmt.Sprintf("#setting($_ = $route('/records','%s'))\nSELECT ID,NAME FROM RECORDS", strings.ToUpper(operation))}
					request := GenerationRequest{Destination: root, Source: source}
					if entry == "Compiled" {
						compiled, compileErr := NewCompiler().Compile(ctx, source)
						if compileErr != nil {
							err = compileErr
						} else {
							request.Source = nil
							request.Compiled = compiled
						}
					}
					if err == nil {
						_, err = (Generator{Operation: operation}).Generate(ctx, request)
					}
					if err == nil {
						t.Fatal("GEN accepted missing or invalid package destination")
					}
					if (tc.name == "missing" || tc.name == "imports_only") && !strings.Contains(err.Error(), "explicit #package") {
						t.Fatalf("incorrect diagnostic: %v", err)
					}
					entries, readErr := os.ReadDir(root)
					if readErr != nil || len(entries) != 2 {
						t.Fatalf("rejected GEN wrote artifacts: %v %v", entries, readErr)
					}
					after, readErr := os.ReadFile(filepath.Join(root, "go.mod"))
					if readErr != nil || string(before) != string(after) {
						t.Fatal("rejected GEN changed module", readErr)
					}
				})
			}
		}
	}
}

func TestGeneratorCompiledPackageAuthority(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	compiled, err := NewCompiler().Compile(ctx, &Source{Name: "Records", Scope: "example.com/generated/source", Types: typecatalog.NewCatalog(), Text: `#package('api/records')
#setting($_ = $route('/records','GET'))
SELECT 1 AS ID FROM RECORDS`})
	if err != nil {
		t.Fatal(err)
	}
	// The compiled canonical destination is authoritative. Do not rescan text or
	// confuse inherited type lookup context with the missing destination.
	for _, value := range []string{"", "   ", "../outside"} {
		altered, err := compiled.projectClone()
		if err != nil {
			t.Fatal(err)
		}
		altered.Component.TypeContext.PackagePath = value
		altered.Component.TypeContext.DefaultPackage = "api/records"
		if _, err = (Generator{Operation: "get"}).Generate(ctx, GenerationRequest{Compiled: altered, Destination: root}); err == nil {
			t.Fatal("invalid canonical package accepted", value)
		}
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 2 {
		t.Fatal("invalid canonical package wrote files", err)
	}
}

func TestGeneratorLinkedGoWithoutDQL(t *testing.T) {
	ctx := context.Background()
	pkg := reflect.TypeOf(linked.ComponentInput{}).PkgPath()
	compiled, err := (&PackageCompilation{
		Source:    &Source{Name: "Users", Scope: pkg},
		Component: &bootstrap.RouteSource{FieldName: "Users", PackagePath: pkg, InputType: "ComponentInput", OutputType: "ComponentOutput", Tag: tag.Component{Method: "GET", Path: "/users/{tenantID}", Connector: "main"}},
		InputType: reflect.TypeOf(linked.ComponentInput{}), OutputType: reflect.TypeOf(linked.ComponentOutput{}),
	}).Compile(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if compiled.Source.PackageComponent == nil || compiled.Source.Text != "" || compiled.Contracts.Input == nil || compiled.Contracts.Output == nil {
		t.Fatal("not a pure linked-Go compilation")
	}
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	if _, err = (Generator{Operation: "get"}).Generate(ctx, GenerationRequest{Compiled: compiled, Destination: root}); err != nil {
		t.Fatal(err)
	}
	// Adding DQL makes the high-level requirement applicable again.
	compiled.Source.Text = "#setting($_ = $route('/users','GET'))\nSELECT id FROM users"
	compiled.Component.TypeContext = &spec.TypeContext{DefaultPackage: pkg}
	if _, err = (Generator{Operation: "get"}).Generate(ctx, GenerationRequest{Compiled: compiled, Destination: root}); err == nil || !strings.Contains(err.Error(), "explicit #package") {
		t.Fatalf("DQL overlay escaped package requirement: %v", err)
	}
}
