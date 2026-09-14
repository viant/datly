package generate

import (
	"testing"

	"github.com/viant/datly/spec"
)

func TestApplyGeneratedInputVeltyAliasesPreservesAuthoredAuthority(t *testing.T) {
	component := &spec.Component{Parameters: []*spec.Parameter{
		{Name: "teamID", Source: spec.BindSource{Kind: "query", Name: "teamID"}},
		{Name: "region", Source: spec.BindSource{Kind: "query", Name: "region"}, Tag: `velty:"names=territory"`},
		{Name: "result", Source: spec.BindSource{Kind: "output", Name: "body"}},
	}}
	plan := &Plan{Input: ContractPlan{Ownership: ContractGenerated, Fields: []Field{
		{Name: "TeamID", Tag: `parameter:"teamID,kind=query,in=teamID"`},
		{Name: "Region", Tag: `parameter:"region,kind=query,in=region" velty:"names=territory"`},
	}}}
	applyGeneratedInputVeltyAliases(plan, component)
	if actual := plan.Input.Fields[0].Tag; actual != `parameter:"teamID,kind=query,in=teamID" velty:"names=TeamID|teamID"` {
		t.Fatalf("generated alias tag = %q", actual)
	}
	if actual := plan.Input.Fields[1].Tag; actual != `parameter:"region,kind=query,in=region" velty:"names=territory"` {
		t.Fatalf("authored alias tag = %q", actual)
	}
}

func TestApplyGeneratedInputVeltyAliasesSkipsLinkedContract(t *testing.T) {
	component := &spec.Component{Parameters: []*spec.Parameter{{Name: "teamID", Source: spec.BindSource{Kind: "query", Name: "teamID"}}}}
	plan := &Plan{Input: ContractPlan{Ownership: ContractLinked, Fields: []Field{{Name: "TeamID", Tag: `parameter:"teamID"`}}}}
	applyGeneratedInputVeltyAliases(plan, component)
	if actual := plan.Input.Fields[0].Tag; actual != `parameter:"teamID"` {
		t.Fatalf("linked contract tag changed to %q", actual)
	}
}
