package transcribe

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly/resource"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe/internal/link"
	"github.com/viant/datly/transcribe/testdata/borrowedinput/contracts"
	"github.com/viant/datly/transcribe/testdata/borrowedinput/registration"
	"github.com/viant/datly/transcribe/testdata/borrowedinput/rows"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
)

func TestBorrowedInputPublicCompilation(t *testing.T) {
	require.Equal(t, "Component", reflect.TypeFor[registration.Component]().Name())
	const pkg = "github.com/viant/datly/transcribe/testdata/borrowedinput/registration"
	reflected, err := bootstrap.ReflectPackages([]string{pkg})
	require.NoError(t, err)
	require.Len(t, reflected.Components, 1)
	route := reflected.Components[0]
	for _, descriptor := range []bool{false, true} {
		t.Run(map[bool]string{false: "linked", true: "descriptor"}[descriptor], func(t *testing.T) {
			catalog, cloneErr := reflected.Types.Clone()
			require.NoError(t, cloneErr)
			resources := resource.New()
			require.NoError(t, resources.Register("borrowed_contract", fstest.MapFS{"sql/current.sql": {Data: []byte("SELECT 1 AS id")}, "sql/other.sql": {Data: []byte("SELECT 1 AS id")}}))
			source := &Source{Scope: pkg, Name: "Read", Resources: resources}
			var result *Result
			if descriptor {
				resolver, resolveErr := typecatalog.NewResolver(catalog, typecatalog.PackageAuthority, nil)
				require.NoError(t, resolveErr)
				groups, groupErr := bootstrap.GroupPackageComponentSources(reflected.Components, resolver)
				require.NoError(t, groupErr)
				result, err = (&descriptorPackageCompilation{source: source, packageSource: groups[0], catalog: catalog}).compile(context.Background())
			} else {
				result, err = (&PackageCompilation{Source: source, Component: route, InputType: reflect.TypeFor[contracts.Input](), OutputType: reflect.TypeFor[contracts.Output](), Types: catalog}).Compile(context.Background())
			}
			require.NoError(t, err)
			require.Equal(t, pkg, result.Component.Key.Scope, "holder retains component destination")
			for _, reference := range result.Views {
				require.NotEqual(t, pkg+".LocalRow", reference.DescriptorKey)
				require.NotEqual(t, pkg+".ForeignRow", reference.DescriptorKey)
			}
			for _, typ := range []reflect.Type{reflect.TypeFor[contracts.LocalRow](), reflect.TypeFor[rows.ForeignRow]()} {
				key := typ.PkgPath() + "." + typ.Name()
				resolved, lookupErr := result.TypeResolver.Descriptor(key)
				require.NoError(t, lookupErr)
				require.Equal(t, typ, resolved.Type)
				found := false
				for _, reference := range result.Views {
					if reference.DescriptorKey == key {
						found = true
					}
				}
				require.True(t, found, "missing row reference %s", key)
			}
		})
	}
}

func TestBorrowedInputCanonicalWrappersAndFallback(t *testing.T) {
	const holder = "example.com/registration"
	owner := reflect.TypeFor[rows.ForeignRow]().PkgPath()
	for _, expression := range []string{"ForeignRow", "*ForeignRow", "[]ForeignRow", "[]*ForeignRow", "*[]*ForeignRow", "Unknown", "remote.ForeignRow", owner + ".ForeignRow"} {
		t.Run(expression, func(t *testing.T) {
			base := &spec.Component{TypeContext: &spec.TypeContext{Imports: []spec.ImportSpec{{Alias: "remote", Package: "example.com/remote"}}}, Parameters: []*spec.Parameter{{Name: "DifferentField", Source: spec.BindSource{Kind: "view", Name: "current"}, TypeExpr: expression, OutputTypeExpr: expression}}, Views: []*spec.View{{Key: spec.Key{Name: "current"}}}}
			identity, err := base.Views[0].Identity()
			require.NoError(t, err)
			authority := &packageAuthority{route: &bootstrap.RouteSource{PackagePath: holder}, inputViews: []linkedInputView{{identity: identity, descriptor: x.NewType(reflect.TypeFor[rows.ForeignRow]())}}}
			detached := base.Clone()
			require.NoError(t, authority.canonicalPackageViewTypes(detached))
			want := expression
			switch expression {
			case "Unknown":
				want = holder + ".Unknown"
			case "remote.ForeignRow":
				want = "example.com/remote.ForeignRow"
			case owner + ".ForeignRow":
			default:
				want = strings.Replace(expression, "ForeignRow", owner+".ForeignRow", 1)
			}
			require.Equal(t, want, detached.Parameters[0].TypeExpr)
			require.Equal(t, want, detached.Parameters[0].OutputTypeExpr)
			require.Equal(t, expression, base.Parameters[0].TypeExpr)
			authored := &spec.Component{Parameters: []*spec.Parameter{{Name: "DifferentField", Source: spec.BindSource{Kind: "view", Name: "current"}, TypeExpr: "remote.Replacement"}}}
			merged, err := (&link.Loader{PackageComponent: detached, AuthoredComponent: authored, DQLAuthored: true}).Load()
			require.NoError(t, err)
			require.Equal(t, "remote.Replacement", merged.Parameters[0].TypeExpr)
		})
	}
}

func TestBorrowedInputViewIdentityFallback(t *testing.T) {
	const holder = "example.com/registration"
	typ := reflect.TypeFor[rows.ForeignRow]()
	for _, tc := range []struct {
		name, binding, identity string
		wantOwner               string
	}{
		{"parameter name fallback", "", "current", typ.PkgPath()},
		{"unmatched binding keeps holder authority", "missing", "current", holder},
		{"unmatched linked identity keeps holder authority", "current", "other", holder},
	} {
		t.Run(tc.name, func(t *testing.T) {
			view := &spec.View{Key: spec.Key{Name: "current"}}
			linkedView := &spec.View{Key: spec.Key{Name: tc.identity}}
			identity, err := linkedView.Identity()
			require.NoError(t, err)
			component := &spec.Component{Parameters: []*spec.Parameter{{Name: "current", Source: spec.BindSource{Kind: "view", Name: tc.binding}, TypeExpr: "[]*ForeignRow"}}, Views: []*spec.View{view}}
			authority := &packageAuthority{route: &bootstrap.RouteSource{PackagePath: holder}, inputViews: []linkedInputView{{identity: identity, descriptor: x.NewType(typ)}}}
			require.NoError(t, authority.canonicalPackageViewTypes(component))
			require.Equal(t, "[]*"+tc.wantOwner+".ForeignRow", component.Parameters[0].TypeExpr)
		})
	}
}

func TestBorrowedInputExternalViewMetadata(t *testing.T) {
	require.Equal(t, "Component", reflect.TypeFor[registration.Component]().Name())
	const pkg = "github.com/viant/datly/transcribe/testdata/borrowedinput/registration"
	reflected, err := bootstrap.ReflectPackages([]string{pkg})
	require.NoError(t, err)
	require.Len(t, reflected.Components, 1)
	route := reflected.Components[0]
	original, err := route.Resolve(reflect.TypeFor[contracts.Input](), reflect.TypeFor[contracts.Output]())
	require.NoError(t, err)
	linked, err := newPackageViewResolver(original, nil).inputDescriptors(reflect.TypeFor[contracts.Input]())
	require.NoError(t, err)
	resolver, err := typecatalog.NewResolver(reflected.Types, typecatalog.PackageAuthority, nil)
	require.NoError(t, err)
	detached := original.Clone()
	authority := &packageAuthority{route: route, inputViews: linked}
	require.NoError(t, authority.canonicalPackageViewTypes(detached))
	parameterOnly := detached.Clone()
	for index := range parameterOnly.Views {
		parameterOnly.Views[index].TypeName = original.Views[index].TypeName
	}
	_, err = newPackageViewResolver(parameterOnly, resolver).externalInputDescriptors(pkg, nil)
	require.ErrorContains(t, err, "does not match package type", "parameter normalization alone leaves later typed view validation inconsistent")
	external, err := newPackageViewResolver(detached, resolver).externalInputDescriptors(pkg, nil)
	require.NoError(t, err)
	require.Len(t, external, 2)
	for index, view := range detached.Views {
		var row *x.Type
		identity, identityErr := view.Identity()
		require.NoError(t, identityErr)
		for _, candidate := range linked {
			if candidate.identity == identity {
				row = candidate.descriptor
			}
		}
		require.NotNil(t, row)
		require.Equal(t, row.Key(), view.TypeName)
		require.Equal(t, row.Name, original.Views[index].TypeName, "caller view stays unqualified")
		require.Equal(t, row.Name, linked[index].view.TypeName, "retained linked metadata stays unqualified")
	}
	for _, expression := range []string{"remote.ForeignRow", "example.com/remote.ForeignRow", "DifferentRow"} {
		t.Run(expression, func(t *testing.T) {
			conflicting := original.Clone()
			if conflicting.TypeContext == nil {
				conflicting.TypeContext = &spec.TypeContext{}
			}
			conflicting.TypeContext.Imports = append(conflicting.TypeContext.Imports, spec.ImportSpec{Alias: "remote", Package: "example.com/remote"})
			conflicting.Views[0].TypeName = expression
			require.NoError(t, authority.canonicalPackageViewTypes(conflicting))
			require.Equal(t, expression, conflicting.Views[0].TypeName, "explicit or unmatched view authority remains unchanged")
			_, err = newPackageViewResolver(conflicting, resolver).externalInputDescriptors(pkg, nil)
			require.ErrorContains(t, err, "does not match package type")
		})
	}
	authoredView := detached.Views[0].Clone()
	authoredView.TypeName = "remote.Replacement"
	merged, err := (&link.Loader{PackageComponent: detached, AuthoredComponent: &spec.Component{Views: []*spec.View{authoredView}}, DQLAuthored: true}).Load()
	require.NoError(t, err)
	require.Equal(t, "remote.Replacement", merged.Views[0].TypeName, "authored view overlay remains authoritative")
}
