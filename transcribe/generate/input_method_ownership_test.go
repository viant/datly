package generate

import "testing"

func TestGeneratedInputCompanionOwnership(t *testing.T) {
	body := "package records\nfunc (*Input) Keys() int {return 1}\nfunc (*Row) GetId() int {return 1}\n"
	type useCase struct {
		description, existing, proposed string
		expected                        bool
	}
	for _, tc := range []useCase{
		{"same owner generated helper", generatedHeader("Records") + body, body, true},
		{"authored helper", body, body, false},
		{"another owner", generatedHeader("Other") + body, body, false},
		{"modified implementation", generatedHeader("Records") + "package records;func (*Input) Keys() int{return 2}", body, false},
		{"extra authored method", generatedHeader("Records") + body + "func (*Input) Custom(){}", body, false},
		{"linked declaration protected", generatedHeader("Records") + body + "type Input struct{}", body, false},
		{"removed helper protected", generatedHeader("Records") + body, "package records;func (*Row) GetId()int{return 1}", false},
	} {
		t.Run(tc.description, func(t *testing.T) {
			p := &scaffoldPersistence{owner: "Records", plan: &Plan{Package: "example.com/records", Input: ContractPlan{Type: "Input", Package: "example.com/records", Ownership: ContractLinked}}, files: []EmittedFile{{Path: "entities.go", Content: tc.proposed}}}
			if got := p.ownedUnchangedInputMethods("entities.go", []byte(tc.existing)); got != tc.expected {
				t.Fatalf("owned=%v expected=%v", got, tc.expected)
			}
		})
	}
}
