package sanitize

import (
	"testing"

	"github.com/viant/velty"
)

func TestRewritePolicy_Rewrite(t *testing.T) {
	testCases := []struct {
		name     string
		raw      string
		declared map[string]bool
		foreach  map[string]bool
		consts   map[string]bool
		kind     velty.ExprContextKind
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
			name:     "declared foreach variable in body uses placeholder",
			raw:      "$rec.ID",
			declared: map[string]bool{"rec": true},
			foreach:  map[string]bool{"rec": true},
			kind:     velty.CtxForEachBody,
			expect:   "$criteria.AppendBinding($rec.ID)",
		},
		{
			name:     "declared dotted parameter uses unsafe placeholder in append context",
			raw:      "$Jwt.UserID",
			declared: map[string]bool{"Jwt": true},
			expect:   "$criteria.AppendBinding($Unsafe.Jwt.UserID)",
		},
		{
			name:   "function arg gets unsafe prefix",
			raw:    "$VendorID",
			kind:   velty.CtxFuncArg,
			expect: "$Unsafe.VendorID",
		},
		{
			name:   "prefixed function arg is preserved",
			raw:    "$sql.Eq",
			kind:   velty.CtxFuncArg,
			expect: "$sql.Eq",
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
			policy := newRewritePolicy(testCase.declared, testCase.foreach, testCase.consts)
			if actual := policy.rewrite(testCase.raw, testCase.kind); actual != testCase.expect {
				t.Fatalf("unexpected rewrite: %s", actual)
			}
		})
	}
}
