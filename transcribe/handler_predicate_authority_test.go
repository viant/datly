package transcribe

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	smodel "github.com/viant/x/syntetic/model"
	xpredicate "github.com/viant/xdatly/predicate"
)

type availablePredicate struct{}

func (*availablePredicate) Compute(context.Context, any) (*xpredicate.Criteria, error) {
	return nil, nil
}

type incompatiblePredicate struct{}

func (*incompatiblePredicate) Compute(context.Context, string) (*xpredicate.Criteria, error) {
	return nil, nil
}

func TestTranscriptionRejectsUnavailableHandlerPredicate(t *testing.T) {
	const packagePath = "example.com/security"
	source := func(catalog *typecatalog.Catalog) *Source {
		return &Source{Scope: "example.com/app", Name: "Records", Connector: "main", Types: catalog, Text: `#import('security','example.com/security')
#setting($_ = $route('/records','GET'))
#define($_ = $Minimum<int>(query/min).WithPredicate(0,'handler','security.Available'))
SELECT id FROM records ${predicate.Builder().CombineAnd($predicate.FilterGroup(0, "AND")).Build("WHERE")}`}
	}
	if _, err := NewCompiler().Compile(context.Background(), source(nil)); err == nil || !strings.Contains(err.Error(), "type authority is required") {
		t.Fatalf("missing authority error=%v", err)
	}
	catalog := typecatalog.NewCatalog()
	if _, err := NewCompiler().Compile(context.Background(), source(catalog)); err == nil || !strings.Contains(err.Error(), "was not found") || !strings.Contains(err.Error(), "run datly link sync and rebuild") {
		t.Fatalf("missing predicate error=%v", err)
	}
	if err := catalog.Register(typecatalog.TypeOriginPackage, x.NewType(reflect.TypeFor[availablePredicate](), x.WithPkgPath(packagePath), x.WithName("Available"))); err != nil {
		t.Fatal(err)
	}
	compiled, err := NewCompiler().Compile(context.Background(), source(catalog))
	if err != nil || compiled.Component.Parameters[0].Predicates[0].Args[0] != packagePath+".Available" {
		t.Fatalf("linked predicate=%v error=%v", compiled, err)
	}
	wrong := typecatalog.NewCatalog()
	if err = wrong.Register(typecatalog.TypeOriginPackage, x.NewType(reflect.TypeFor[incompatiblePredicate](), x.WithPkgPath(packagePath), x.WithName("Available"))); err != nil {
		t.Fatal(err)
	}
	if _, err = NewCompiler().Compile(context.Background(), source(wrong)); err == nil || !strings.Contains(err.Error(), "does not implement predicate.Handler") {
		t.Fatalf("incompatible linked predicate error=%v", err)
	}
}

func TestTranscriptionRejectsSourceOnlyPredicate(t *testing.T) {
	const packagePath = "example.com/security"
	for _, tc := range []struct {
		name, method string
	}{
		{"valid source method", "Compute(context.Context, any) (*predicate.Criteria, error)"},
		{"wrong source return", "Compute(context.Context, any) (bool, error)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			file, err := parser.ParseFile(token.NewFileSet(), "predicate.go", "package security\nfunc (*Available) "+tc.method+" { panic(0) }", 0)
			if err != nil {
				t.Fatal(err)
			}
			declared := &smodel.Type{Name: "Available", PkgPath: packagePath, TypeSpec: &ast.TypeSpec{Name: ast.NewIdent("Available"), Type: &ast.StructType{Fields: &ast.FieldList{}}},
				Imports: map[string]*smodel.ImportRef{"context": {Path: "context"}, "predicate": {Path: "github.com/viant/xdatly/predicate"}}, PtrMethodsAST: []*ast.FuncDecl{file.Decls[0].(*ast.FuncDecl)}}
			catalog := typecatalog.NewCatalog()
			if err = catalog.RegisterPackage(typecatalog.TypeOriginPackage, &smodel.Package{PkgPath: packagePath, Types: []*smodel.Type{declared}}); err != nil {
				t.Fatal(err)
			}
			source := &Source{Scope: "example.com/app", Name: "Records", Connector: "main", Types: catalog, Text: `#import('security','example.com/security')
#setting($_ = $route('/records','GET'))
#define($_ = $Minimum<int>(query/min).WithPredicate(0,'handler','security.Available'))
SELECT id FROM records ${predicate.Builder().CombineAnd($predicate.FilterGroup(0, "AND")).Build("WHERE")}`}
			_, err = NewCompiler().Compile(context.Background(), source)
			if err == nil || !strings.Contains(err.Error(), "not linked into this binary") || !strings.Contains(err.Error(), "run datly link sync and rebuild") {
				t.Fatalf("source-only predicate error=%v", err)
			}
		})
	}
}
