package transcribe

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe/dql"
	"github.com/viant/datly/transcribe/dql/statement"
	gen "github.com/viant/datly/transcribe/generate"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
)

func TestGeneratedRowAuthorityRejectsCompleteDependencyCycles(t *testing.T) {
	const pkg = "example.com/rows"
	for _, stale := range []bool{false, true} {
		for _, kind := range []string{"scalar", "structural map", "relation and scalar", "inferred relation and scalar"} {
			t.Run(kind+map[bool]string{true: " stale", false: " fresh"}[stale], func(t *testing.T) {
				parent := &spec.View{Name: "Parents", TypeName: "self.Parent"}
				child := &spec.View{Name: "Children", TypeName: "self.Child", Columns: []*spec.Column{{Name: "Back", ExplicitType: true, Type: spec.TypeRef{Package: pkg, Name: "Parent", Pointer: true}}}}
				switch kind {
				case "scalar":
					parent.Columns = []*spec.Column{{Name: "Child", ExplicitType: true, Type: spec.TypeRef{Package: pkg, Name: "Child", Pointer: true}}}
				case "structural map":
					parent.Columns = []*spec.Column{{Name: "Children", ExplicitType: true, Type: spec.TypeRef{Name: "map[string]*self.Child"}}}
				default:
					parent.Relations = []*spec.Relation{{Name: "Children", Holder: "Children", View: child}}
				}
				owners := map[string][]*spec.View{pkg + ".Parent": {parent}, pkg + ".Child": {child}}
				if kind == "inferred relation and scalar" {
					child.TypeName = ""
					delete(owners, pkg+".Child")
				}
				catalog := typecatalog.NewCatalog()
				if stale {
					for _, name := range []string{"Parent", "Child"} {
						require.NoError(t, catalog.Register(typecatalog.TypeOriginGenerated, x.NewType(reflect.TypeFor[struct{ Old int }](), x.WithName(name), x.WithPkgPath(pkg))))
					}
				}
				resolver, err := typecatalog.NewResolver(catalog, typecatalog.TranscribeAuthority, &typecatalog.ResolutionContext{PackagePath: pkg})
				require.NoError(t, err)
				authority := &generatedRowAuthority{plan: &gen.Plan{Package: pkg, Imports: []spec.ImportSpec{{Alias: "self", Package: pkg}}}, owners: owners, resolver: resolver, views: []*spec.View{parent, child}}
				require.ErrorContains(t, authority.validateCycles(), "dependency cycle")
			})
		}
	}
}

// The ownership pass must retain linked hook keys before decomposing SQL,
// even when a different column refers forward to a SQL-owned generated row.
func TestGeneratedRowAuthorityPreservesLinkedHookKeys(t *testing.T) {
	type parent struct {
		ID         int
		DerivedIDs []int `sqlx:"-" relationKey:"hook"`
	}
	const pkg = "example.com/hooks"
	catalog := typecatalog.NewCatalog()
	require.NoError(t, catalog.Register(typecatalog.TypeOriginPackage,
		x.NewType(reflect.TypeFor[parent](), x.WithName("Parent"), x.WithPkgPath(pkg))))
	resolver, err := typecatalog.NewResolver(catalog, typecatalog.TranscribeAuthority,
		&typecatalog.ResolutionContext{PackagePath: pkg})
	require.NoError(t, err)
	for _, dependency := range []string{"Child", "Missing"} {
		t.Run(dependency, func(t *testing.T) {
			sql := `SELECT p.*, c.*, type(p,'Parent'), type(c,'Child'), CAST(p.NESTED AS ` + dependency + `)
FROM parents p JOIN children c ON p.DERIVED_IDS=c.PARENT_ID`
			component := &spec.Component{RootView: &spec.View{Name: "parents"}, TypeContext: &spec.TypeContext{PackagePath: pkg}}
			prepared := &dql.PreparedSource{SQL: sql, Original: sql, Statements: statement.Parse(sql)}
			authority, err := collectGeneratedRowAuthority(component, prepared, nil, resolver, "hooks.dql", &SourceMap{original: sql})
			if dependency == "Missing" {
				require.ErrorContains(t, err, "Missing")
				require.Nil(t, authority)
				return
			}
			require.NoError(t, err)
			require.Len(t, authority.root.Relations, 1)
			require.Equal(t, "DerivedIDs", authority.root.Relations[0].On[0].ParentField)
			found := false
			for _, column := range authority.root.Columns {
				if column.Name == "DerivedIDs" {
					found = true
					require.Contains(t, column.Tag, `relationKey:"hook"`)
					require.Equal(t, spec.CardinalityMany, column.Type.Cardinality)
				}
			}
			require.True(t, found, "linked hook column lost")
			require.False(t, strings.Contains(authority.root.Source.SQL, "DERIVED_IDS"), "logical hook key projected into SQL")
		})
	}
}

// Explicit SQL naming cannot turn a selected authored recursive Go row into a
// generated cycle, while generated and DQL descriptors retain SQL ownership.
type recursiveLinkedAuthorityRow struct {
	Next *recursiveLinkedAuthorityRow
}

func TestGeneratedRowAuthorityPreservesAuthoredRecursiveRow(t *testing.T) {
	const pkg = "example.com/rows"
	const source = `#package('example.com/rows')
#import('self','example.com/rows')
#setting($_ = $route('/records', 'GET'))
#set($_ = $Data<[]*self.Row>(output/view))
SELECT r.Next, CAST(r.Next AS *self.Row), type(r,'self.Row') FROM records r`
	for _, origin := range []typecatalog.TypeOrigin{typecatalog.TypeOriginPackage, typecatalog.TypeOriginGenerated, typecatalog.TypeOriginDQL} {
		t.Run(string(origin), func(t *testing.T) {
			catalog := typecatalog.NewCatalog()
			require.NoError(t, catalog.Register(origin, x.NewType(reflect.TypeFor[recursiveLinkedAuthorityRow](), x.WithName("Row"), x.WithPkgPath(pkg))))
			result, err := NewCompiler().Compile(context.Background(), &Source{Scope: pkg, Name: "Records", Types: catalog, Text: source})
			if origin != typecatalog.TypeOriginPackage {
				require.ErrorContains(t, err, "generated named row dependency cycle")
				require.Nil(t, result)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, result.Views[gen.RootViewPath])
			require.Equal(t, pkg+".Row", result.Views[gen.RootViewPath].DescriptorKey)
		})
	}
}
