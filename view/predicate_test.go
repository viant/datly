package view

import "testing"

func TestValidatePredicateArgs_Duration(t *testing.T) {
	testCases := []struct {
		name      string
		predicate string
		value     interface{}
		args      []string
		wantErr   bool
	}{
		{
			name:      "thirty days requires seventh arg",
			predicate: "duration",
			value:     "thirty_days",
			args:      []string{"d", "cd", "h", "ch", "yd", "wd"},
			wantErr:   true,
		},
		{
			name:      "thirty days with seventh arg",
			predicate: "duration",
			value:     "thirty_days",
			args:      []string{"d", "cd", "h", "ch", "yd", "wd", "md"},
			wantErr:   false,
		},
		{
			name:      "week remains backward compatible with six args",
			predicate: "duration",
			value:     "week",
			args:      []string{"d", "cd", "h", "ch", "yd", "wd"},
			wantErr:   false,
		},
	}
	for _, testCase := range testCases {
		err := validatePredicateArgs(testCase.predicate, testCase.value, testCase.args)
		if testCase.wantErr && err == nil {
			t.Fatalf("%s: expected error", testCase.name)
		}
		if !testCase.wantErr && err != nil {
			t.Fatalf("%s: unexpected error: %v", testCase.name, err)
		}
	}
}
