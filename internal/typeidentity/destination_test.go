package typeidentity

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
)

func TestNormalizeDestinationWithoutLookupUsesOnlyOwnImports(t *testing.T) {
	for _, tc := range []struct {
		name, expression, want string
		context                *spec.TypeContext
	}{
		{name: "own import", expression: "*jwt.Claims", want: "*github.com/viant/scy/auth/jwt.Claims", context: &spec.TypeContext{Imports: []spec.ImportSpec{{Alias: "jwt", Package: "github.com/viant/scy/auth/jwt"}}}},
		{name: "unknown explicit qualifier", expression: "*explicit.Claims", want: "*explicit.Claims"},
		{name: "unknown short name", expression: "*Claims", want: "*Claims", context: &spec.TypeContext{PackagePath: "example.com/owner"}},
		{name: "already qualified", expression: "*github.com/viant/scy/auth/jwt.Claims", want: "*github.com/viant/scy/auth/jwt.Claims"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := &spec.Parameter{TypeExpr: "string", OutputTypeExpr: tc.expression, Codec: &spec.Codec{Body: "JwtClaim", OutputType: "*explicit.CodecDestination", Args: []string{"keep"}}}
			before := *p.Codec
			before.Args = append([]string(nil), p.Codec.Args...)
			require.NoError(t, NormalizeDestination(p, "example.com/default", tc.context, nil))
			require.Equal(t, tc.want, p.OutputTypeExpr)
			require.Equal(t, "string", p.TypeExpr)
			require.Equal(t, &before, p.Codec)
		})
	}
}
