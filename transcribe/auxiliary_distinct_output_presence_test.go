package transcribe

import (
	"strings"
	"testing"
)

func TestAuxiliaryBodyPresenceWithDistinctResponse(t *testing.T) {
	type useCase struct{ description, output string }
	for _, tc := range []useCase{
		{description: "same body response", output: "[]*Record"},
		{description: "independent response DTO", output: "[]*model.Caller"},
	} {
		t.Run(tc.description, func(t *testing.T) {
			text := strings.Replace(auxiliaryOwnerDQL, "$Data<[]*Record>", "$Data<"+tc.output+">", 1)
			text = strings.Replace(text, "$Current<[]*Record>", "$Current<[]*CurrentRow>", 1)
			text = strings.Replace(text, "TypeName('Record')", "TypeName('CurrentRow')", 1)
			text = strings.Replace(text, "$Candidates<[]*Record>", "$Candidates<[]*CandidateRow>", 1)
			text = strings.Replace(text, "TypeName('Record')", "TypeName('CandidateRow')", 1)
			root, source := auxiliaryOwnerFixture(t, text)
			generated, err := (Generator{Operation: "patch"}).Generate(t.Context(), GenerationRequest{Source: source, Destination: root})
			if err != nil {
				t.Fatal(err)
			}
			for _, view := range generated.Result.Plan.Views {
				if view.Name == "Record" {
					if len(view.SetMarkerFields) == 0 {
						t.Fatal("request body lost sparse presence when response type differs")
					}
					return
				}
			}
			t.Fatal("request body not generated")
		})
	}
}
