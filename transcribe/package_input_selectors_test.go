package transcribe

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/spec"
	gen "github.com/viant/datly/transcribe/generate"
	"github.com/viant/datly/transcribe/internal/link"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
)

type SelectorLinkedSummary struct{ Count int }

type SelectorLinkedInput struct {
	ID   int `parameter:"ID,kind=query,in=id"`
	Page int `parameter:"Page,kind=query,in=page" querySelector:"view=rows,property=page"`
}

func TestSelectorBusinessInputRetainsExternalDirectBindingComparison(t *testing.T) {
	descriptor := x.NewType(reflect.TypeFor[SelectorLinkedInput]())
	summary := x.NewType(reflect.TypeFor[SelectorLinkedSummary]())
	catalog := typecatalog.NewCatalog()
	require.NoError(t, catalog.LinkRuntimeAll(typecatalog.TypeOriginPackage, descriptor, summary))
	resolver, err := typecatalog.NewResolver(catalog, typecatalog.PackageAuthority, nil)
	require.NoError(t, err)
	base := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/reader", Name: "Records"}, Name: "Records", Routes: []*spec.Route{{Method: "GET", Path: "/records"}}, Parameters: []*spec.Parameter{
		{Name: "ID", TypeExpr: "int", Source: spec.BindSource{Kind: "query", Name: "id"}},
		{Name: "Page", TypeExpr: "int", Source: spec.BindSource{Kind: "query", Name: "page"}, QuerySelector: &spec.QuerySelectorBinding{View: "rows", Property: spec.SelectorPropertyPage}},
	}}
	for _, tc := range []struct {
		name     string
		mutate   func(*spec.Component)
		retained bool
	}{
		{"side addition", nil, true},
		{"unrelated new linked output view", func(c *spec.Component) {
			c.Views = []*spec.View{{Name: "Meta", Namespace: "t", TypeName: summary.Key()}}
		}, true},
		{"ordinary addition", func(c *spec.Component) {
			c.Parameters = append(c.Parameters, &spec.Parameter{Name: "Extra", TypeExpr: "int", Source: spec.BindSource{Kind: "query", Name: "extra"}})
		}, false},
		{"ordinary type", func(c *spec.Component) { c.Parameters[0].TypeExpr = "int64" }, false},
		{"ordinary source", func(c *spec.Component) { c.Parameters[0].Source.Name = "other" }, false},
		{"ordinary default", func(c *spec.Component) { v := "3"; c.Parameters[0].Value = &v }, false},
		{"direct selector source", func(c *spec.Component) { c.Parameters[1].Source.Name = "other_page" }, false},
		{"direct selector view", func(c *spec.Component) { c.Parameters[1].QuerySelector.View = "other" }, false},
		{"direct selector default", func(c *spec.Component) { v := "2"; c.Parameters[1].Value = &v }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			current := base.Clone()
			current.Parameters = append(current.Parameters, &spec.Parameter{Name: "Fields", TypeExpr: "[]string", Source: spec.BindSource{Kind: "query", Name: "r_fields"}, QuerySelector: &spec.QuerySelectorBinding{View: "rows", Property: spec.SelectorPropertyFields}})
			if tc.mutate != nil {
				tc.mutate(current)
			}
			source := &Source{Text: "authored DQL"}
			result := &Result{Source: source, Component: current, TypeResolver: resolver}
			if len(current.Views) > 0 {
				identity, e := current.Views[0].Identity()
				require.NoError(t, e)
				result.Views = gen.ViewReferences{identity: &gen.ViewReference{DescriptorKey: summary.Key()}}
			}
			authority := &packageAuthority{source: source, route: &bootstrap.RouteSource{InputType: descriptor.Key()}, input: descriptor}
			contracts := &link.Contracts{PackageComponent: base, CompiledComponent: current}
			require.NoError(t, authority.retainSelectorBusinessInput(result, contracts))
			require.Equal(t, tc.retained, result.Contracts.Input != nil)
			require.Len(t, base.Parameters, 2)
			require.Equal(t, "id", base.Parameters[0].Source.Name)
			require.Equal(t, "page", base.Parameters[1].Source.Name)
		})
	}
}
