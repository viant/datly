package bootstrap

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	xshape "github.com/viant/x/shape"
	model "github.com/viant/x/syntetic/model"
)

func predicateContextType(t testing.TB, pkg, name, definition string, imports map[string]string) *x.Type {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "input.go", "package contracts\ntype "+name+" "+definition, 0)
	require.NoError(t, err)
	declaration := file.Decls[0].(*ast.GenDecl).Specs[0].(*ast.TypeSpec)
	source := &model.Type{Name: name, PkgPath: pkg, TypeSpec: declaration, Imports: map[string]*model.ImportRef{}}
	for alias, path := range imports {
		source.Imports[alias] = &model.ImportRef{Path: path}
	}
	return &x.Type{Name: name, PkgPath: pkg, SynteticType: source}
}

func predicateContextResolver(t testing.TB, types ...*x.Type) *typecatalog.Resolver {
	t.Helper()
	catalog := typecatalog.NewCatalog()
	require.NoError(t, catalog.RegisterAll(typecatalog.TypeOriginPackage, types...))
	resolver, err := typecatalog.NewResolver(catalog, typecatalog.PackageAuthority, nil)
	require.NoError(t, err)
	return resolver
}

func TestPredicateContextsReuseDeclarationsAndEmbeddedOwnership(t *testing.T) {
	left := predicateContextType(t, "example.com/left", "Input", "struct{First, Second int}", map[string]string{"security": "example.com/left/security"})
	right := predicateContextType(t, "example.com/right", "Input", "struct{Third, Fourth int}", map[string]string{"security": "example.com/right/security"})
	alias := predicateContextType(t, "example.com/reader", "Alias", "= *left.Input", map[string]string{"left": left.PkgPath, "security": "example.com/wrong"})
	root := predicateContextType(t, "example.com/reader", "Root", "struct{Alias; right.Input}", map[string]string{"right": right.PkgPath})
	types := predicateContextResolver(t, root, alias, left, right)
	r := &packageComponentResolver{types: types, component: &spec.Component{Key: spec.Key{Scope: root.PkgPath}}}
	contract := &packageContract{descriptor: root}
	for _, tc := range []struct {
		index []int
		want  string
	}{
		{[]int{0, 0}, "example.com/left/security.Filter"},
		{[]int{0, 1}, "example.com/left/security.Filter"},
		{[]int{1, 0}, "example.com/right/security.Filter"},
		{[]int{1, 1}, "example.com/right/security.Filter"},
	} {
		param := &spec.Parameter{Name: "Value", Predicates: []*spec.Predicate{{Name: "handler", Args: []string{"security.Filter"}}}}
		require.NoError(t, r.resolveFieldPredicates(contract, xshape.Field{Name: "Value", Index: tc.index}, param))
		require.Equal(t, tc.want, param.Predicates[0].Args[0])
	}
	require.Len(t, r.predicateContexts, 2)
	require.Len(t, r.predicateDeclarations, 4)
	first, err := r.predicateFieldContext(root, []int{0, 0})
	require.NoError(t, err)
	second, err := r.predicateFieldContext(root, []int{0, 1})
	require.NoError(t, err)
	require.Same(t, first, second, "siblings share only their declaring context")
	for _, args := range [][]string{nil, {"security.Filter", "extra"}} {
		param := &spec.Parameter{Name: "Invalid", Predicates: []*spec.Predicate{{Name: "handler", Args: args}}}
		err := r.resolveFieldPredicates(contract, xshape.Field{Name: "Invalid", Index: []int{0, 1}}, param)
		require.Error(t, err, "a cached context must not bypass predicate validation")
	}
	// Private cache mutation cannot change the public resolver's snapshots.
	r.predicateDeclarations[left.Key()].SynteticType.Imports["security"].Path = "changed"
	fresh, err := types.Descriptor(left.Key())
	require.NoError(t, err)
	require.Equal(t, "example.com/left/security", fresh.SynteticType.Imports["security"].Path)
	require.Equal(t, "example.com/left/security", left.SynteticType.Imports["security"].Path)
}

func TestPredicateContextsPreserveAuthorityAndCompilationIsolation(t *testing.T) {
	root := predicateContextType(t, "example.com/reader", "Input", "struct{Value int}", map[string]string{"security": "example.com/fallback"})
	for _, pkg := range []string{"example.com/first", "example.com/second"} {
		t.Run(pkg, func(t *testing.T) {
			t.Parallel()
			authority := predicateContextType(t, root.PkgPath, root.Name, "struct{Value int}", map[string]string{"security": pkg})
			r := &packageComponentResolver{types: predicateContextResolver(t, authority), component: &spec.Component{}}
			param := &spec.Parameter{Name: "Value", Predicates: []*spec.Predicate{{Name: "handler", Args: []string{"security.Filter"}}}}
			require.NoError(t, r.resolveFieldPredicates(&packageContract{descriptor: root}, xshape.Field{Name: "Value", Index: []int{0}}, param))
			require.Equal(t, pkg+".Filter", param.Predicates[0].Args[0])
		})
	}
}

func TestPredicateContextsConcurrentContractResolution(t *testing.T) {
	input := predicateContextType(t, "example.com/contracts", "Input", "struct{Value int `parameter:\"Value,kind=query,in=value\" predicate:\"handler,security.Filter\"`}", map[string]string{"security": "example.com/security"})
	types := predicateContextResolver(t, input)
	component := &spec.Component{Key: spec.Key{Scope: "example.com/reader"}}
	for i := 0; i < 8; i++ {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			t.Parallel()
			result, err := (ContractResolver{Component: component, InputType: input, Types: types}).Resolve()
			require.NoError(t, err)
			require.Len(t, result.Parameters, 1)
			require.Equal(t, "example.com/security.Filter", result.Parameters[0].Predicates[0].Args[0])
			require.Empty(t, component.Parameters)
		})
	}
}

func TestPredicateContextsReflectionAndFailures(t *testing.T) {
	type embedded struct{ Value int }
	type input struct{ embedded }
	root := x.NewType(reflect.TypeFor[input]())
	r := &packageComponentResolver{component: &spec.Component{Key: spec.Key{Scope: root.PkgPath}, TypeContext: &spec.TypeContext{Imports: []spec.ImportSpec{{Alias: "security", Package: "example.com/security"}}}}}
	param := &spec.Parameter{Name: "Value", Predicates: []*spec.Predicate{{Name: "handler", Args: []string{"security.Filter"}}}}
	require.NoError(t, r.resolveFieldPredicates(&packageContract{descriptor: root}, xshape.Field{Name: "Value", Index: []int{0, 0}}, param))
	require.Equal(t, "example.com/security.Filter", param.Predicates[0].Args[0])
	for i := 0; i < 2; i++ {
		_, err := r.predicateFieldContext(root, []int{99, 0})
		require.ErrorContains(t, err, "invalid embedded field index")
	}
	require.Len(t, r.predicateContexts, 1, "failed owner resolution must not publish a context")
}

func BenchmarkPredicateFieldContexts(b *testing.B) {
	for _, embedded := range []bool{false, true} {
		b.Run(fmt.Sprintf("embedded=%v", embedded), func(b *testing.B) {
			const count = 128
			var fields strings.Builder
			fields.WriteString("struct{\n")
			for i := 0; i < count; i++ {
				fmt.Fprintf(&fields, "Field%d int\n", i)
			}
			fields.WriteString("}")
			input := predicateContextType(b, "example.com/contracts", "Input", fields.String(), map[string]string{"security": "example.com/security"})
			root := input
			if embedded {
				root = predicateContextType(b, "example.com/reader", "Input", "struct{contracts.Input}", map[string]string{"contracts": input.PkgPath})
			}
			types := predicateContextResolver(b, input, root)
			contract := &packageContract{descriptor: root}
			b.ReportAllocs()
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				r := &packageComponentResolver{types: types, component: &spec.Component{Key: spec.Key{Scope: root.PkgPath}}}
				for i := 0; i < count; i++ {
					index := []int{i}
					if embedded {
						index = []int{0, i}
					}
					param := &spec.Parameter{Name: "Value", Predicates: []*spec.Predicate{{Name: "handler", Args: []string{"security.Filter"}}}}
					if err := r.resolveFieldPredicates(contract, xshape.Field{Name: "Value", Index: index}, param); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}
