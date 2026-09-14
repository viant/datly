package transcribe

import (
	"testing"

	"github.com/viant/datly/spec"
	gen "github.com/viant/datly/transcribe/generate"
	plan "github.com/viant/datly/transcribe/handler/ast"
	handlergo "github.com/viant/datly/transcribe/handler/golang"
)

func TestCurrentCarrierUsesExactViewAuthority(t *testing.T) {
	for _, tc := range []struct {
		name, bound, root, rows, carrier string
		fail                             bool
	}{
		{"pointer rows", "[]*Previous", "Previous", "[]*Previous", "", false},
		{"value rows", "[]Previous", "Previous", "[]Previous", "", false},
		{"array rows", "[2]Previous", "Previous", "[2]Previous", "", false},
		{"named rows", "PreviousRows", "Previous", "[]*Previous", "PreviousRows", false},
		{"envelope", "*Envelope", "Previous", "[]*Previous", "*Envelope", false},
		{"generic envelope", "*Envelope[Previous]", "Previous", "[]*Previous", "*Envelope[Previous]", false},
		{"direct one", "*Previous", "Previous", "[]*Previous", "*Previous", false},
		{"wrong direct rows", "[]*Other", "Previous", "", "", true},
		{"wrong package rows", "[]*left.Previous", "right.Previous", "", "", true},
		{"invalid carrier", "[]", "Previous", "", "", true},
		{"wrapped root authority", "*Envelope", "*Previous", "", "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			generated := &gen.Plan{Input: gen.ContractPlan{Fields: []gen.Field{{Name: "Existing", Type: tc.bound}}}, Views: []gen.ViewPlan{
				{Identity: "unrelated", Type: "Decoy"}, {Identity: "current", Type: tc.root},
			}, Imports: []spec.ImportSpec{{Alias: "left", Package: "example.com/left"}, {Alias: "right", Package: "example.com/right"}}}
			generation := &handlerGeneration{input: &gen.Input{TargetPackage: "example.com/records"}}
			rows, carrier, err := generation.currentRecordTypes(generated, &plan.CurrentPlan{ViewIdentity: "current", InputPath: plan.FieldPath{"Input", "Existing"}})
			if (err != nil) != tc.fail || rows != tc.rows || carrier != tc.carrier {
				t.Fatalf("rows=%q carrier=%q error=%v", rows, carrier, err)
			}
		})
	}
}

func TestAppendRecordTypePreservesCurrentCarrier(t *testing.T) {
	generated := &gen.Plan{
		Input: gen.ContractPlan{Fields: []gen.Field{{Name: "Existing", Type: "*Envelope"}}},
		Views: []gen.ViewPlan{
			{Identity: "entity", Name: "Entity", Type: "Entity", Ownership: gen.ViewGenerated},
			{Identity: "current", Name: "Previous", Type: "Previous", Ownership: gen.ViewGenerated},
		},
	}
	generation := &handlerGeneration{input: &gen.Input{TargetPackage: "example.com/records"}}
	record := &plan.RecordPlan{Identity: "entity", InputPath: plan.FieldPath{"Input", "Records"}, Cardinality: spec.CardinalityMany,
		Current: &plan.CurrentPlan{ViewIdentity: "current", InputPath: plan.FieldPath{"Input", "Existing"}},
	}
	var records []handlergo.RecordType
	if err := generation.appendRecordType(&records, &plan.Plan{Operation: plan.OperationPatch, Root: record}, generated, record, "[]*Entity"); err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].Current != "[]*Previous" || records[0].CurrentValue != "*Envelope" || records[0].Value != "[]*Entity" {
		t.Fatalf("record types=%+v", records)
	}
	if generated.Input.Fields[0].Type != "*Envelope" {
		t.Fatal("carrier wiring rewrote the input contract")
	}
}

func TestCurrentCarrierRejectsMissingOrAmbiguousAuthority(t *testing.T) {
	generation := &handlerGeneration{input: &gen.Input{TargetPackage: "example.com/records"}}
	for _, views := range [][]gen.ViewPlan{nil, {{Identity: "other", Type: "Previous"}}, {{Identity: "current", Type: "Previous"}, {Identity: "current", Type: "Previous"}}} {
		generated := &gen.Plan{Input: gen.ContractPlan{Fields: []gen.Field{{Name: "Existing", Type: "*Envelope"}}}, Views: views}
		if _, _, err := generation.currentRecordTypes(generated, &plan.CurrentPlan{ViewIdentity: "current", InputPath: plan.FieldPath{"Input", "Existing"}}); err == nil {
			t.Fatalf("accepted views=%+v", views)
		}
	}
}
