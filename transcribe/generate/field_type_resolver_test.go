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

func TestFieldTypeResolverQualifiedGeneratedDeclaration(t *testing.T) {
	const pkg = "example.com/generated/records"
	for _, qualifier := range []string{"self", pkg} {
		for _, wrapper := range []string{"", "*", "[]", "[]*", "*[]*"} {
			t.Run(qualifier+wrapper, func(t *testing.T) {
				plan := &Plan{Package: pkg, Imports: []spec.ImportSpec{{Alias: "self", Package: pkg}}, Views: []ViewPlan{{Type: "Record", Ownership: ViewGenerated}}}
				resolver := &fieldTypeResolver{plan: plan, targetPackage: pkg, lookup: func(string) (*x.Type, error) {
					t.Fatal("generated declaration required existing Go descriptor")
					return nil, nil
				}}
				got, err := resolver.resolve(wrapper + qualifier + ".Record")
				if err != nil || got != wrapper+"Record" {
					t.Fatalf("got %q, %v", got, err)
				}
				if len(plan.knownLocalTypes) != 0 || len(plan.resolvedNamedTypes) != 0 {
					t.Fatal("generated declaration treated as discovered Go type")
				}
			})
		}
	}
}

func TestFieldTypeResolverQualifiedDeclarationOwnership(t *testing.T) {
	const pkg = "example.com/generated/records"
	for _, tc := range []struct {
		name string
		plan Plan
	}{
		{"root name only", Plan{RootViewType: "Record"}},
		{"linked row", Plan{Views: []ViewPlan{{Type: "Record", Ownership: ViewLinked}}}},
		{"foreign row", Plan{Views: []ViewPlan{{Type: "Record", Package: "example.com/foreign", Ownership: ViewGenerated}}}},
		{"linked contract", Plan{Input: ContractPlan{Type: "Record", Ownership: ContractLinked}}},
		{"foreign contract", Plan{Input: ContractPlan{Type: "Record", Package: "example.com/foreign", Ownership: ContractGenerated}}},
		{"foreign helper", Plan{HelperTypes: []HelperType{{Name: "Record", Package: "example.com/foreign"}}}},
		{"shared body without owner", Plan{Views: []ViewPlan{{Type: "Record", Ownership: ViewSharedBody, SharedBodyIdentity: "missing"}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.plan.Imports = []spec.ImportSpec{{Alias: "self", Package: pkg}}
			lookups := 0
			r := &fieldTypeResolver{plan: &tc.plan, targetPackage: pkg, lookup: func(string) (*x.Type, error) { lookups++; return nil, errors.New("missing descriptor") }}
			if got, err := r.resolve("[]*self.Record"); err == nil || lookups == 0 {
				t.Fatalf("invalid exemption: %q %v lookups=%d", got, err, lookups)
			}
		})
	}
}
