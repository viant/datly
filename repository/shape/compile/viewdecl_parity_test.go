package compile

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	dqldiag "github.com/viant/datly/repository/shape/dql/diag"
	dqlshape "github.com/viant/datly/repository/shape/dql/shape"
)

func TestViewDecl_ParityFixtures(t *testing.T) {
	testCases := []struct {
		name          string
		viewName      string
		tail          string
		expectDiag    string
		expectTag     string
		expectCodec   string
		expectHandler string
		expectPreds   int
	}{
		{
			name:          "tag/codec/handler",
			viewName:      "limit",
			tail:          ".WithTag('json:\"id\"').WithCodec(AsJSON).WithHandler('Build')",
			expectTag:     `json:"id"`,
			expectCodec:   "AsJSON",
			expectHandler: "Build",
		},
		{
			name:       "status arg validation",
			viewName:   "limit",
			tail:       ".WithStatusCode('x')",
			expectDiag: dqldiag.CodeDeclOptionArgs,
		},
		{
			name:       "query selector validation",
			viewName:   "customer_id",
			tail:       ".QuerySelector('items')",
			expectDiag: dqldiag.CodeDeclQuerySelector,
		},
		{
			name:        "predicate forms",
			viewName:    "limit",
			tail:        ".WithPredicate('ByID','id=?',1).EnsurePredicate('Tenant','tenant=?',2)",
			expectPreds: 2,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			view := &declaredView{Name: testCase.viewName}
			var diags []*dqlshape.Diagnostic
			applyDeclaredViewOptions(view, testCase.tail, "SELECT 1", 0, &diags)
			if testCase.expectDiag != "" {
				require.NotEmpty(t, diags)
				assert.Equal(t, testCase.expectDiag, diags[0].Code)
				return
			}
			assert.Equal(t, testCase.expectTag, view.Tag)
			assert.Equal(t, testCase.expectCodec, view.Codec)
			assert.Equal(t, testCase.expectHandler, view.HandlerName)
			assert.Len(t, view.Predicates, testCase.expectPreds)
		})
	}
}
