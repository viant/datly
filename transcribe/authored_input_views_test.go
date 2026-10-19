package transcribe

import (
	"context"
	"go/ast"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	gen "github.com/viant/datly/transcribe/generate"
	"github.com/viant/datly/transcribe/gobuild"
	"github.com/viant/x"
	smodel "github.com/viant/x/syntetic/model"
)

const authoredReadTestPackage = "example.com/ordinaryread/rows"

func authoredReadTestResult(t *testing.T, withImport bool) (string, *Result) {
	t.Helper()
	root := t.TempDir()
	for _, name := range []string{"go.mod", "rows/readrow.go", "dql/probe.dql"} {
		sourceName := name
		if name == "go.mod" {
			sourceName += ".txt"
		}
		data, err := os.ReadFile(filepath.Join("testdata/ordinary_local_read_ownership", sourceName))
		if err != nil {
			t.Fatal(err)
		}
		if withImport && name == "rows/readrow.go" {
			text := strings.Replace(string(data), "package rows", "package rows\nimport tm \"time\"", 1)
			text = strings.Replace(text, "type ReadRow struct {", "type ReadRow struct {\n When tm.Time `sqlx:\"WHEN\"`", 1)
			data = []byte(text)
		}
		dest := filepath.Join(root, name)
		if err = os.MkdirAll(filepath.Dir(dest), 0700); err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(dest, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	project, err := (&Discovery{BaseDir: root, Include: []string{"example.com/ordinaryread/dql"}, SourceFile: "probe.dql", GoBuild: &gobuild.Context{Dir: root}}).Compile(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return root, project.Components[0]
}

func TestAuthoredInputDeclarationRequiresExactSourceAndImports(t *testing.T) {
	_, result := authoredReadTestResult(t, true)
	_, proof, err := authoredInputViewReferences(context.Background(), result)
	if err != nil || proof == nil {
		t.Fatalf("genuine proof unavailable: %v", err)
	}
	record := proof.declarations.types["ReadRow"][0]
	descriptor, err := result.TypeResolver.Descriptor(authoredReadTestPackage + ".ReadRow")
	if err != nil {
		t.Fatal(err)
	}
	if !authoredInputDeclaration(record, descriptor, authoredReadTestPackage, "ReadRow") {
		t.Fatal("genuine imported source declaration rejected")
	}
	for _, name := range []string{"nominal package", "catalog body", "referenced import", "import alias", "missing AST", "alias AST"} {
		t.Run(name, func(t *testing.T) {
			altered, err := (x.Cloner{}).Type(descriptor)
			if err != nil {
				t.Fatal(err)
			}
			switch name {
			case "nominal package":
				altered.PkgPath = "example.com/wrong"
			case "catalog body":
				altered.SynteticType.TypeSpec.Type.(*ast.StructType).Fields.List[0].Type = ast.NewIdent("string")
			case "referenced import":
				altered.SynteticType.Imports["tm"] = &smodel.ImportRef{Alias: "tm", Path: "net/http"}
			case "import alias":
				altered.SynteticType.Imports["tm"] = &smodel.ImportRef{Alias: "wrong", Path: "time"}
			case "missing AST":
				altered.SynteticType = nil
			case "alias AST":
				altered.SynteticType.TypeSpec.Assign = 1
			}
			if authoredInputDeclaration(record, altered, authoredReadTestPackage, "ReadRow") {
				t.Fatal("changed catalog authority accepted")
			}
		})
	}
}

func TestAuthoredInputProofRetainsSelectedBytesAndSet(t *testing.T) {
	for _, change := range []string{"bytes", "added file", "flags"} {
		t.Run(change, func(t *testing.T) {
			root, result := authoredReadTestResult(t, false)
			_, proof, err := authoredInputViewReferences(context.Background(), result)
			if err != nil || proof == nil {
				t.Fatalf("genuine proof unavailable: %v", err)
			}
			if err = proof.validate(context.Background()); err != nil {
				t.Fatal(err)
			}
			switch change {
			case "bytes":
				path := filepath.Join(root, "rows/readrow.go")
				raw, e := os.ReadFile(path)
				if e != nil {
					t.Fatal(e)
				}
				if e = os.WriteFile(path, append(raw, '\n'), 0600); e != nil {
					t.Fatal(e)
				}
			case "added file":
				if e := os.WriteFile(filepath.Join(root, "rows/extra.go"), []byte("package rows\nvar Extra = 1\n"), 0600); e != nil {
					t.Fatal(e)
				}
			case "flags":
				proof.build.Env = append(proof.build.Env, "GOFLAGS=-overlay=")
			}
			if err = proof.validate(context.Background()); err == nil {
				t.Fatal("changed compiler source authority retained")
			}
		})
	}
}

func TestAuthoredInputProvisionalUsesSelectedSnapshot(t *testing.T) {
	_, result := authoredReadTestResult(t, false)
	input, err := (&discoveryInputCompiler{component: result.Component, declarations: result.Declarations, viewBindings: result.ViewBindings, resolver: result.TypeResolver, source: result.Source, views: result.Views}).inputType()
	if err != nil {
		t.Fatal(err)
	}
	field, ok := input.FieldByName("Current")
	if !ok {
		t.Fatal("missing provisional input Current")
	}
	row := field.Type
	for row.Kind() == reflect.Pointer || row.Kind() == reflect.Slice {
		row = row.Elem()
	}
	for _, name := range []string{"Has", "Children"} {
		if _, ok = row.FieldByName(name); !ok {
			t.Fatalf("selected authored %s lost during provisional discovery", name)
		}
	}
	plan, err := gen.New(gen.Input{Component: result.Component, Declarations: result.Declarations, Views: result.Views, ViewBindings: result.ViewBindings, TypeResolver: result.TypeResolver, TargetPackage: authoredReadTestPackage}).Plan()
	if err != nil {
		t.Fatal(err)
	}
	if plan.ViewByType("ReadRow").Ownership != gen.ViewLinked {
		t.Fatal("final plan lost provisional snapshot")
	}
}

func TestAuthoredInputBodyNamesAreNeverBorrowed(t *testing.T) {
	_, result := authoredReadTestResult(t, false)
	// Exercise the exclusion against an actual native compiled graph, not a holder.
	names, err := authoredInputRootNames(result.Component, &gen.Plan{Package: authoredReadTestPackage, Imports: result.Component.TypeContext.Imports})
	if err != nil {
		t.Fatal(err)
	}
	if names[authoredReadTestPackage+".BodyRow"] == "" {
		t.Fatal("SQL-derived body type was not excluded")
	}
	if names[authoredReadTestPackage+".ReadRow"] != "" {
		t.Fatal("independent read was mistaken for body")
	}
}

func TestAuthoredInputNoBuildAuthorityRetainsExistingGeneration(t *testing.T) {
	_, result := authoredReadTestResult(t, false)
	result.Source.GoBuild = nil
	refs, proof, err := authoredInputViewReferences(context.Background(), result)
	if err != nil || len(refs) != 0 || proof != nil {
		t.Fatal("missing compiler authority granted a new link")
	}
}

func TestAuthoredInputDoesNotTightenExistingFactoryDeclarationPolicy(t *testing.T) {
	for _, declaration := range []string{"type ReadRow int", "type ReadRow = int"} {
		t.Run(declaration, func(t *testing.T) {
			root := t.TempDir()
			if err := os.WriteFile(filepath.Join(root, "go.mod"), []byte("module example.com/factorypolicy\n\ngo 1.25.0\n"), 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, "row.go"), []byte("package factorypolicy\n"+declaration+"\n"), 0600); err != nil {
				t.Fatal(err)
			}
			oldPolicy := &factoryDeclarations{ctx: context.Background(), build: &gobuild.Context{Dir: root}, pkg: "example.com/factorypolicy"}
			accepted, err := oldPolicy.authoredCurrent("ReadRow")
			if err != nil || !accepted {
				t.Fatalf("existing factory policy changed: %v", err)
			}
		})
	}
}

func TestAuthoredInputSelectedMissingOrAmbiguousDeclarationDoesNotLink(t *testing.T) {
	for _, change := range []string{"missing", "ambiguous"} {
		t.Run(change, func(t *testing.T) {
			root, result := authoredReadTestResult(t, false)
			if change == "missing" {
				file := filepath.Join(root, "rows/readrow.go")
				raw, err := os.ReadFile(file)
				if err != nil {
					t.Fatal(err)
				}
				raw = []byte(strings.Replace(string(raw), "type ReadRow struct", "type MissingRow struct", 1))
				if err = os.WriteFile(file, raw, 0600); err != nil {
					t.Fatal(err)
				}
			} else if err := os.WriteFile(filepath.Join(root, "rows/duplicate.go"), []byte("package rows\ntype ReadRow struct { Old int }\n"), 0600); err != nil {
				t.Fatal(err)
			}
			refs, proof, err := authoredInputViewReferences(context.Background(), result)
			if err != nil {
				t.Fatal(err)
			}
			if len(refs) != 0 || proof != nil {
				t.Fatal("stale genuine catalog granted missing/ambiguous selected declaration ownership")
			}
		})
	}
}

func TestAuthoredIndependentReadOverlayRequiresExplicitDQL(t *testing.T) {
	_, result := authoredReadTestResult(t, false)
	authored := result.Component.Clone()
	// A reloaded generated package carries URI-based metadata while the DQL
	// retains inline SQL. Authored row ownership must survive that transition.
	result.Source.PackageComponent = result.Component.Clone()
	for _, view := range result.Source.PackageComponent.Views {
		if view.Source != nil {
			view.Source.URI = "generated:current.sql"
		}
	}
	refs, proof, err := authoredInputViewReferences(context.Background(), result, authored)
	if err != nil || proof == nil || len(refs) != 1 {
		t.Fatalf("explicit row ownership lost: refs=%v proof=%v err=%v", refs, proof, err)
	}
	authored.Parameters = nil
	refs, proof, err = authoredInputViewReferences(context.Background(), result, authored)
	if err != nil || len(refs) != 0 || proof != nil {
		t.Fatalf("inherited row gained explicit ownership: %v %v", refs, err)
	}
}
