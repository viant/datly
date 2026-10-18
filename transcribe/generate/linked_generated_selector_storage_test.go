package generate

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
)

func TestGeneratedLinkedInputWithLegacyDirectSelectorRegenerates(t *testing.T) {
	input := namedDiscoveryDeclaration(t, "LegacyInput", "struct { Page int `parameter:\"Page,kind=query,in=page\" querySelector:\"view=rows,property=page\"` }")
	output := namedDiscoveryDeclaration(t, "LegacyOutput", "struct {}")
	resolver := namedDiscoveryResolver(t, input, output)
	component := &spec.Component{Name: "Records", Routes: []*spec.Route{{Method: "GET", Path: "/records"}}, Parameters: []*spec.Parameter{
		{Name: "Page", TypeExpr: "int", Source: spec.BindSource{Kind: "query", Name: "page"}, QuerySelector: &spec.QuerySelectorBinding{View: "rows", Property: spec.SelectorPropertyPage}},
		{Name: "Limit", TypeExpr: "int", Source: spec.BindSource{Kind: "query", Name: "limit"}, QuerySelector: &spec.QuerySelectorBinding{View: "rows", Property: spec.SelectorPropertyLimit}},
	}}
	in := Input{Component: component, TypeResolver: resolver, TargetPackage: input.PkgPath, PackageName: "records", Contracts: ContractReferences{Input: &ContractReference{Expression: input.Name, DescriptorKey: input.Key()}, Output: &ContractReference{Expression: output.Name, DescriptorKey: output.Key()}}}
	// Resolve the same canonical selector plan, before ownership classification.
	selectors, err := resolveSelectorFields(component, nil)
	require.NoError(t, err)
	p := &Plan{Package: input.PkgPath, Input: ContractPlan{Package: input.PkgPath, DescriptorKey: input.Key(), Ownership: ContractLinked}, QuerySelectors: selectors}
	r := &planResolver{input: in, types: resolver, plan: p}
	require.NoError(t, r.preserveLinkedSelectors(true))
	require.Equal(t, ContractGenerated, p.Input.Ownership)
	require.Equal(t, selectors, p.QuerySelectors, "regeneration must retain both direct legacy Page and new Limit for side migration")
}
