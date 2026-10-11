package transcribe

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
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
