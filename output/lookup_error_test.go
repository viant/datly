package output

import (
	"errors"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/readerlookup"
	modern "github.com/viant/xdatly/response"
)

func TestModernLookupEnvelopeRequiresClassification(t *testing.T) {
	type result struct {
		modern.Status
		Rows []int
	}
	p, err := (Compiler{}).Compile(CompileInput{Component: errorDataComponent("Rows", "[]int"), Type: reflect.TypeFor[result]()})
	require.NoError(t, err)
	classification := readerlookup.EmptyResult(true)
	value, ok, err := p.ErrorEnvelopeWithCause("untrusted duplicate text", 403, errors.Join(classification, classification))
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, result{Status: modern.Status{Status: "error", Message: "Forbidden"}, Rows: []int{}}, value)
	for _, cause := range []error{errors.New("Forbidden"), &modern.Error{Code: 403, Payload: map[string]any{"privateContract": true}, Cause: classification}, errors.Join(classification, errors.New("other"))} {
		value, ok, err := p.ErrorEnvelopeWithCause("public", 403, cause)
		require.NoError(t, err)
		require.False(t, ok)
		require.Nil(t, value)
	}
	value, ok, err = p.ErrorEnvelope("public")
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, value)
}
