package shared

import "testing"

func TestCountPlaceholders(t *testing.T) {
	testCases := []struct {
		name   string
		query  string
		expect int
	}{
		{
			name:   "simple placeholders",
			query:  "SELECT * FROM t WHERE a = ? AND b = ?",
			expect: 2,
		},
		{
			name:   "ignore single quoted regex",
			query:  "SELECT REGEXP_REPLACE(col, r'^(?:https?://)?(?:www\\.)?', '') FROM t WHERE a = ?",
			expect: 1,
		},
		{
			name:   "ignore comments and quoted text",
			query:  "SELECT '?' -- ?\nFROM t /* ? */ WHERE x = ?",
			expect: 1,
		},
	}

	for _, testCase := range testCases {
		if actual := countPlaceholders(testCase.query); actual != testCase.expect {
			t.Fatalf("%s: expected %d placeholders, got %d", testCase.name, testCase.expect, actual)
		}
	}
}
