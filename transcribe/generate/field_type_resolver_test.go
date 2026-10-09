package generate

import (
	"errors"
	"testing"

	"github.com/viant/datly/spec"
	"github.com/viant/x"
)

func TestFieldTypeResolverPrefersGeneratedLocalViewToDefaultPackage(t *testing.T) {
	lookups := 0
	resolver := &fieldTypeResolver{
		plan:          &Plan{RootViewType: "Event", Views: []ViewPlan{{Type: "Event", Ownership: ViewGenerated}}},
		context:       &spec.TypeContext{DefaultPackage: "example.com/contracts"},
		targetPackage: "example.com/generated/events",
		lookup: func(string) (*x.Type, error) {
			lookups++
			return nil, errors.New("local type reached package lookup")
		},
	}
	actual, err := resolver.resolve("[]*Event")
	if err != nil {
		t.Fatal(err)
	}
	if actual != "[]*Event" || lookups != 0 {
		t.Fatalf("resolved = %q, lookups = %d", actual, lookups)
	}
}

func TestFieldTypeResolverGeneratedContractReferences(t *testing.T) {
	for _, test := range []struct {
		name, expression string
		contract         ContractPlan
		output           bool
		wantLocal        bool
	}{
		{"input pointer", "*Request", ContractPlan{Type: "Request", Ownership: ContractGenerated}, false, true},
		{"output slice", "[]*Result", ContractPlan{Type: "Result", Ownership: ContractGenerated}, true, true},
		{"input map", "map[string]*Request", ContractPlan{Type: "Request", Package: "example.com/generated", Ownership: ContractGenerated}, false, true},
		{"linked contract retains authority", "*Request", ContractPlan{Type: "Request", Ownership: ContractLinked}, false, false},
		{"other package retains authority", "*Request", ContractPlan{Type: "Request", Package: "example.com/elsewhere", Ownership: ContractGenerated}, false, false},
		{"unknown type retains authority", "*Unknown", ContractPlan{Type: "Request", Ownership: ContractGenerated}, false, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			plan := &Plan{Package: "example.com/generated"}
			if test.output {
				plan.Output = test.contract
			} else {
				plan.Input = test.contract
			}
			lookups := 0
			resolver := &fieldTypeResolver{plan: plan, context: &spec.TypeContext{DefaultPackage: "example.com/contracts"}, targetPackage: plan.Package, lookup: func(string) (*x.Type, error) { lookups++; return nil, errors.New("package type authority unavailable") }}
			actual, err := resolver.resolve(test.expression)
			if test.wantLocal {
				if err != nil || actual != test.expression || lookups != 0 {
					t.Fatalf("resolved=%q err=%v lookups=%d", actual, err, lookups)
				}
			} else if err == nil || lookups == 0 {
				t.Fatalf("external type lost authority: resolved=%q err=%v lookups=%d", actual, err, lookups)
			}
		})
	}
}
