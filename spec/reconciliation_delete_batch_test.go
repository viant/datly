package spec

import "testing"

func TestReconciliationDeleteBatchSize(t *testing.T) {
	type useCase struct {
		description, input string
		expectedError      bool
	}
	for _, tc := range []useCase{
		{"default unchanged", `{"mode":"same-parent-root-first","roles":[{"holder":"Rows"}]}`, false},
		{"bounded opt in", `{"mode":"same-parent-root-first","roles":[{"holder":"Rows","deleteBatchSize":1000}]}`, false},
		{"negative invalid", `{"mode":"same-parent-root-first","roles":[{"holder":"Rows","deleteBatchSize":-1}]}`, true},
	} {
		t.Run(tc.description, func(t *testing.T) {
			_, err := ParseReconciliation(tc.input)
			if (err != nil) != tc.expectedError {
				t.Fatalf("error=%v", err)
			}
		})
	}
}
