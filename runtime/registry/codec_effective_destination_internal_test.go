package registry

import (
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	"github.com/viant/scy/auth/jwt"
	"reflect"
	"testing"
)

func TestCoverageCodecDestinationCompletionPreservesExplicitMismatch(t *testing.T) {
	a := &spec.Parameter{Name: "Jwt", TypeExpr: "string", Codec: &spec.Codec{Body: "JwtClaim"}}
	b := a.Clone()
	b.OutputTypeExpr = "*example.com/other.Claims"
	left, right := InputField{destinationType: reflect.TypeFor[*jwt.Claims]()}, InputField{destinationType: reflect.TypeFor[*jwt.Claims]()}
	left.binding.Extension = a
	right.binding.Extension = b
	require.ErrorContains(t, coverageParameter(left, right, true), "different public parameter metadata")
	require.Empty(t, a.OutputTypeExpr)
	require.Equal(t, "*example.com/other.Claims", b.OutputTypeExpr)
}
