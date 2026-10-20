package handlergen

import (
	"github.com/viant/datly/spec"
	gen "github.com/viant/datly/transcribe/generate"
	plan "github.com/viant/datly/transcribe/handler/ast"
	"strings"
	"testing"
)

func TestAuxiliaryResultAssignmentExemptionUsesNativeLifecycleAuthority(t *testing.T) {
	for _, tc := range []struct {
		name                                    string
		aux, hooks, child, cycle, direct, velty bool
		wantError                               bool
	}{
		{name: "native auxiliary lifecycle", aux: true, hooks: true},
		{name: "ordinary writer", hooks: true, wantError: true},
		{name: "auxiliary without lifecycle", aux: true, wantError: true},
		{name: "writable descendant", aux: true, hooks: true, child: true, wantError: true},
		{name: "cycle", aux: true, hooks: true, cycle: true, wantError: true},
		{name: "direct Go", aux: true, hooks: true, direct: true, wantError: true},
		{name: "Velty", aux: true, hooks: true, velty: true, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := &spec.View{Name: "Request", Auxiliary: tc.aux}
			if tc.hooks {
				root.EntityHooks = "Lifecycle"
			}
			if tc.child {
				root.Relations = []*spec.Relation{{Name: "Child", View: &spec.View{Name: "Child"}}}
			}
			if tc.cycle {
				root.Relations = []*spec.Relation{{Name: "Self", View: root}}
			}
			options := Options{Handler: HandlerOptions{Target: HandlerGo, Operation: WritePatch, Go: GoHandlerOptions{Execution: GoExecutionMutation}}}
			if tc.direct {
				options.Handler.Go.Execution = GoExecutionDirect
			}
			if tc.velty {
				options.Handler.Target = HandlerVelty
			}
			generation := New(&Context{Component: &spec.Component{RootView: root}}, &gen.Input{TargetPackage: "example.com/fixture"}, options)
			semantic := &plan.Plan{Input: plan.ContractRef{Path: plan.FieldPath{"Input", "Body"}}, Output: &plan.ContractRef{Path: plan.FieldPath{"Output", "Data"}}}
			contracts := &gen.Plan{Input: gen.ContractPlan{Fields: []gen.Field{{Name: "Body", Type: "*Body"}}}, Output: gen.ContractPlan{Fields: []gen.Field{{Name: "Data", Type: "[]int"}}}}
			err := generation.validateContractAssignment(semantic, contracts)
			if tc.wantError && (err == nil || !strings.Contains(err.Error(), "conflicts with body type")) {
				t.Fatalf("ordinary mismatch guard lost: %v", err)
			}
			if !tc.wantError && err != nil {
				t.Fatal(err)
			}
		})
	}
}
