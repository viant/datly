package compiler

import (
	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
	"strings"
	"testing"
)

func auxiliaryResultRequest() Request {
	root := &spec.View{Name: "Request", Auxiliary: true, EntityHooks: "RequestLifecycle", Source: &spec.ViewSource{Table: "records"}, Columns: []*spec.Column{{Name: "ID", Source: "ID", Type: spec.TypeRef{Name: "int"}, PrimaryKey: true}}}
	return Request{Component: &spec.Component{Name: "Request", RootView: root, Parameters: []*spec.Parameter{{Name: "Body", TypeExpr: "*RequestBody", Source: spec.BindSource{Kind: "body"}}, {Name: "Data", TypeExpr: "[]int", Source: spec.BindSource{Kind: "output", Name: "body"}}}}, Operation: plan.OperationPatch}
}
func TestBuildInputAuxiliaryLifecyclePreservesIndependentResult(t *testing.T) {
	for _, outputless := range []bool{false, true} {
		request := auxiliaryResultRequest()
		if outputless {
			request.Component.Parameters = request.Component.Parameters[:1]
		}
		before := len(request.Component.Parameters)
		built, err := (&Compiler{}).BuildInput(request, "RequestBody")
		if err != nil {
			t.Fatal(err)
		}
		if len(built.Component.Parameters) != before || built.Input != "Body" || built.Output != "" || len(built.Currents) != 0 || built.Current != "" {
			t.Fatalf("auxiliary request acquired synthetic body result/current: %+v", built)
		}
		compiled, err := (&Compiler{}).Compile(built)
		if err != nil {
			t.Fatal(err)
		}
		if outputless && compiled.Output != nil {
			t.Fatal("outputless lifecycle acquired a result")
		}
		if !outputless && (compiled.Output == nil || strings.Join(compiled.Output.Path, ".") != "Output.Data" || built.Component.Parameters[1].TypeExpr != "[]int" || built.Component.Parameters[1].Source.Name != "body") {
			t.Fatal("authored list result changed")
		}
	}
}
func TestBuildInputAuxiliaryLifecycleRetainsOutputSelectionErrors(t *testing.T) {
	for _, tc := range []struct {
		name, selector, want string
		duplicate            bool
	}{{"missing explicit", "Missing", "was not found", false}, {"ambiguous authored", "", "ambiguous", true}} {
		t.Run(tc.name, func(t *testing.T) {
			request := auxiliaryResultRequest()
			request.Output = tc.selector
			if tc.duplicate {
				other := *request.Component.Parameters[1]
				other.Name = "Other"
				request.Component.Parameters = append(request.Component.Parameters, &other)
			}
			built, err := (&Compiler{}).BuildInput(request, "RequestBody")
			if err != nil {
				t.Fatal(err)
			}
			if built.Output != tc.selector {
				t.Fatal("explicit output selector changed")
			}
			if _, err = (&Compiler{}).Compile(built); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("output selection diagnostic lost: %v", err)
			}
		})
	}
}
