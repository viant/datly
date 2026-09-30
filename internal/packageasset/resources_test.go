package packageasset

import "testing"

func TestResourceDescriptor(t *testing.T) {
	for _, tc := range []struct {
		name  string
		input Resources
		fail  bool
	}{
		{"valid", Resources{Namespace: "queries", Files: []string{"sql/query.sql"}}, false},
		{"namespace required", Resources{Files: []string{"query.sql"}}, true},
		{"qualified namespace", Resources{Namespace: "a:b", Files: []string{"query.sql"}}, true},
		{"empty files", Resources{Namespace: "queries"}, true},
		{"escape", Resources{Namespace: "queries", Files: []string{"../query.sql"}}, true},
		{"absolute", Resources{Namespace: "queries", Files: []string{"/query.sql"}}, true},
		{"duplicate", Resources{Namespace: "queries", Files: []string{"query.sql", "query.sql"}}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.input.Validate(); (err != nil) != tc.fail {
				t.Fatalf("validation: %v", err)
			}
		})
	}
}
