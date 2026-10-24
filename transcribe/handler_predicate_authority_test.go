package transcribe

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"strings"
	"testing"

	readerpredicate "github.com/viant/datly/runtime/predicate/velty"
	"github.com/viant/datly/spec"
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

func TestTranscriptionValidatesSourcePredicateContract(t *testing.T) {
	const packagePath = "example.com/security"
	for _, tc := range []struct {
		name, method              string
		accepted                  bool
		receiver, criteriaPackage string
	}{
		{"valid source method", "Compute(context.Context, any) (*predicate.Criteria, error)", true, "*Available", "github.com/viant/xdatly/predicate"},
		{"typed variadic", "Compute(context.Context, any, ...any) (*predicate.Criteria, error)", true, "*Available", "github.com/viant/xdatly/predicate"},
		{"value receiver", "Compute(context.Context, interface{}) (*predicate.Criteria, error)", true, "Available", "github.com/viant/xdatly/predicate"},
		{"wrong Criteria package", "Compute(context.Context, any) (*predicate.Criteria, error)", false, "*Available", "example.com/other"},
		{"wrong input", "Compute(context.Context, string) (*predicate.Criteria, error)", false, "*Available", "github.com/viant/xdatly/predicate"},
		{"variadic", "Compute(context.Context, ...any) (*predicate.Criteria, error)", false, "*Available", "github.com/viant/xdatly/predicate"},
		{"wrong context", "Compute(string, any) (*predicate.Criteria, error)", false, "*Available", "github.com/viant/xdatly/predicate"},
		{"missing Compute", "Other(context.Context, any) (*predicate.Criteria, error)", false, "*Available", "github.com/viant/xdatly/predicate"},
		{"wrong source return", "Compute(context.Context, any) (bool, error)", false, "*Available", "github.com/viant/xdatly/predicate"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			file, err := parser.ParseFile(token.NewFileSet(), "predicate.go", "package security\nfunc ("+tc.receiver+") "+tc.method+" { panic(0) }", 0)
			if err != nil {
				t.Fatal(err)
			}
			declared := &smodel.Type{Name: "Available", PkgPath: packagePath, TypeSpec: &ast.TypeSpec{Name: ast.NewIdent("Available"), Type: &ast.StructType{Fields: &ast.FieldList{}}},
				Imports: map[string]*smodel.ImportRef{"context": {Path: "context"}, "predicate": {Path: tc.criteriaPackage}}, PtrMethodsAST: []*ast.FuncDecl{file.Decls[0].(*ast.FuncDecl)}}
			if tc.receiver == "Available" {
				declared.MethodsAST = declared.PtrMethodsAST
				declared.PtrMethodsAST = nil
			}
			catalog := typecatalog.NewCatalog()
			if err = catalog.RegisterPackage(typecatalog.TypeOriginPackage, &smodel.Package{PkgPath: packagePath, Types: []*smodel.Type{declared}}); err != nil {
				t.Fatal(err)
			}
			source := &Source{Scope: "example.com/app", Name: "Records", Connector: "main", Types: catalog, Text: `#import('security','example.com/security')
#setting($_ = $route('/records','GET'))
#define($_ = $Minimum<int>(query/min).WithPredicate(0,'handler','security.Available'))
SELECT id FROM records ${predicate.Builder().CombineAnd($predicate.FilterGroup(0, "AND")).Build("WHERE")}`}
			compiled, err := NewCompiler().Compile(context.Background(), source)
			if tc.accepted {
				if err != nil {
					t.Fatal(err)
				}
				if got := compiled.Component.Parameters[0].Predicates[0].Args[0]; got != packagePath+".Available" {
					t.Fatalf("canonical identity=%s", got)
				}
				if runtimeErr := (readerpredicate.DefinitionCompiler{Catalog: catalog, RequireAvailable: true}).Compile(compiled.Component); runtimeErr == nil || !strings.Contains(runtimeErr.Error(), "not linked into this binary") {
					t.Fatalf("runtime must reject source-only handler: %v", runtimeErr)
				}
			} else if err == nil || !strings.Contains(err.Error(), "does not implement predicate.Handler") {
				t.Fatalf("incompatible source predicate error=%v", err)
			}
		})
	}
}

func TestTranscriptionRejectsNameOnlyPredicateAuthority(t *testing.T) {
	catalog := typecatalog.NewCatalog()
	const name = "example.com/security.Available"
	if err := catalog.Register(typecatalog.TypeOriginPackage, &x.Type{PkgPath: "example.com/security", Name: "Available", Definition: "struct{}"}); err != nil {
		t.Fatal(err)
	}
	component := &spec.Component{Parameters: []*spec.Parameter{{Name: "Minimum", Predicates: []*spec.Predicate{{Name: "handler", Args: []string{name}}}}}}
	if err := compilePredicateContracts(component, catalog, nil); err == nil || !strings.Contains(err.Error(), "no source declaration authority") {
		t.Fatalf("name-only authority error=%v", err)
	}
}

func TestTranscriptionTypedVariadicPredicateAuthority(t *testing.T) {
	const packagePath = "example.com/security"
	for _, tc := range []struct {
		name      string
		typ       reflect.Type
		wantError bool
	}{
		{"legacy rejects extras", reflect.TypeFor[availablePredicate](), true},
		{"variadic preserves extras", reflect.TypeFor[availableVariadicPredicate](), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			catalog := typecatalog.NewCatalog()
			if err := catalog.Register(typecatalog.TypeOriginPackage, x.NewType(tc.typ, x.WithPkgPath(packagePath), x.WithName("Available"))); err != nil {
				t.Fatal(err)
			}
			source := &Source{Scope: "example.com/app", Name: "Records", Connector: "main", Types: catalog, Text: `#import('security','example.com/security')
#setting($_ = $route('/records','GET'))
#define($_ = $Minimum<int>(query/min).WithPredicate(0,'handler','security.Available','alias','r','enabled',true,'limit',7))
SELECT id FROM records r ${predicate.Builder().CombineAnd($predicate.FilterGroup(0, "AND")).Build("WHERE")}`}
			compiled, err := NewCompiler().Compile(context.Background(), source)
			if tc.wantError {
				if err == nil || !strings.Contains(err.Error(), "VariadicHandler") {
					t.Fatalf("got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			got := compiled.Component.Parameters[0].Predicates[0]
			if got.Args[0] != packagePath+".Available" || !reflect.DeepEqual(got.HandlerArgs, []any{"alias", "r", "enabled", true, "limit", 7}) {
				t.Fatalf("%#v", got)
			}
			if err := (readerpredicate.DefinitionCompiler{Catalog: catalog, RequireAvailable: true}).Compile(compiled.Component); err != nil {
				t.Fatal(err)
			}
		})
	}
}

type availableVariadicPredicate struct{}

func (*availableVariadicPredicate) Compute(context.Context, any, ...any) (*xpredicate.Criteria, error) {
	return nil, nil
}
