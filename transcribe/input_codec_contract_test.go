package transcribe

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/transcribe/internal/link"
	xshape "github.com/viant/x/shape"
)

type CodecContractClaims struct{ Subject string }
type CodecContractOtherClaims struct{ Other string }
type codecContractInput struct {
	Jwt *CodecContractClaims `parameter:"Jwt,kind=header,in=Authorization,dataType=string,errorCode=401,required=true" codec:"JwtClaim"`
}

func TestPackageInputCodecRetainsExactDestinationContract(t *testing.T) {
	descriptor := xshape.Linked(reflect.TypeFor[codecContractInput]()).Descriptor()
	component, err := (bootstrap.ContractResolver{InputType: descriptor}).Resolve()
	require.NoError(t, err)
	require.Len(t, component.Parameters, 1)
	p := component.Parameters[0]
	require.Equal(t, "string", p.TypeExpr)
	require.Equal(t, "*github.com/viant/datly/transcribe.CodecContractClaims", p.OutputTypeExpr)
	require.Equal(t, "JwtClaim", p.Codec.Body)
	require.Empty(t, p.Codec.OutputType)
	require.Equal(t, 401, p.ErrorStatusCode)
	require.NotNil(t, p.Required)
	require.True(t, *p.Required)
	for _, tc := range []struct {
		name, destination string
		linked            bool
	}{
		{"same destination", "*github.com/viant/datly/transcribe.CodecContractClaims", true},
		{"changed destination", "*github.com/viant/datly/transcribe.CodecContractOtherClaims", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			authored := component.Clone()
			authored.Parameters[0].OutputTypeExpr = tc.destination
			contracts := &link.Contracts{PackageComponent: component, CompiledComponent: authored, Input: descriptor, Source: &bootstrap.RouteSource{InputType: descriptor.Key()}}
			references, err := contracts.References()
			require.NoError(t, err)
			require.Equal(t, tc.linked, references.Input != nil)
		})
	}
}
