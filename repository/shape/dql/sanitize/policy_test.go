package sanitize

import "testing"

func TestRewritePolicy_Rewrite(t *testing.T) {
	testCases := []struct {
		name     string
		raw      string
		declared map[string]bool
		consts   map[string]bool
		expect   string
	}{
		{
			name:   "plain selector becomes placeholder + unsafe",
			raw:    "$ID",
			expect: "$criteria.AppendBinding($Unsafe.ID)",
		},
		{
			name:   "unsafe selector preserved",
			raw:    "$Unsafe.ID",
			expect: "$Unsafe.ID",
		},
		{
			name:     "declared selector remains local placeholder",
			raw:      "$x",
			declared: map[string]bool{"x": true},
			expect:   "$criteria.AppendBinding($x)",
		},
		{
			name:   "const selector keeps raw unsafe path",
			raw:    "$ConstID",
			consts: map[string]bool{"ConstID": true},
			expect: "$Unsafe.ConstID",
		},
		{
			name:   "function call is untouched",
			raw:    "$Foo.Bar()",
			expect: "$Foo.Bar()",
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			policy := newRewritePolicy(testCase.declared, testCase.consts)
			if actual := policy.rewrite(testCase.raw); actual != testCase.expect {
				t.Fatalf("unexpected rewrite: %s", actual)
			}
		})
	}
}
