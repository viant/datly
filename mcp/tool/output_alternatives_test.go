package tool

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/exec"
	"github.com/viant/datly/runtime/output"
	"github.com/viant/x/shape"
	"github.com/xeipuuv/gojsonschema"
)

type alternativeRecursive struct {
	Name  string                `json:"name"`
	Child *alternativeRecursive `json:"child"`
}

func TestOutputAlternativeReferencesStayInsideCompleteBranch(t *testing.T) {
	typ := reflect.TypeFor[alternativeRecursive]()
	base, err := (output.Compiler{}).Compile(output.CompileInput{Type: typ})
	require.NoError(t, err)
	p := &Plan{compilerInput: Input{Output: base}, caseFormat: "lc"}
	// An older external resolver can supply typed alternatives without prepared
	// native artifacts. Its fallback still uses the exact output codec owner.
	schema, err := p.discoveryOutputSchema(&exec.EffectiveContract{Output: typ, OutputAlternatives: []*shape.Representation{nil, nil}})
	require.NoError(t, err)
	document, err := json.Marshal(schema)
	require.NoError(t, err)
	require.Contains(t, string(document), `"$ref":"#/anyOf/0"`)
	require.Contains(t, string(document), `"$ref":"#/anyOf/1"`)
	for _, value := range []*alternativeRecursive{{}, {Name: "root", Child: &alternativeRecursive{Name: "child", Child: &alternativeRecursive{Name: "leaf"}}}} {
		encoded, err := base.Encode(t.Context(), "json", value)
		require.NoError(t, err)
		result, err := gojsonschema.Validate(gojsonschema.NewBytesLoader(document), gojsonschema.NewBytesLoader(encoded.Data))
		require.NoError(t, err)
		require.True(t, result.Valid(), "%v", result.Errors())
	}
}

func TestOutputAlternativeRejectsNonEnvelope(t *testing.T) {
	typ := reflect.TypeFor[[]int]()
	base, err := (output.Compiler{}).Compile(output.CompileInput{Type: typ})
	require.NoError(t, err)
	p := &Plan{compilerInput: Input{Output: base}}
	_, err = p.discoveryOutputSchema(&exec.EffectiveContract{Output: typ, OutputAlternatives: []*shape.Representation{nil}})
	require.ErrorContains(t, err, "object envelope")
}
