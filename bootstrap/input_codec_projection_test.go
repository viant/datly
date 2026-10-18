package bootstrap

import (
	"reflect"
	"testing"

	"github.com/viant/datly/spec"
	xshape "github.com/viant/x/shape"
)

type codecProjectionClaims struct{ Subject string }
type codecProjectionInput struct{ Jwt *codecProjectionClaims }

func TestInputCodecProjectionPreservesExplicitDestinationAndWire(t *testing.T) {
	fields, err := xshape.New(linkedContractType(reflect.TypeFor[codecProjectionInput]()), nil).Fields()
	if err != nil {
		t.Fatal(err)
	}
	p := &spec.Parameter{TypeExpr: "string", OutputTypeExpr: "*explicit.Claims", ErrorStatusCode: 401, Codec: &spec.Codec{Body: "JwtClaim", OutputType: "*codec.Claims"}}
	resolver := &packageComponentResolver{component: &spec.Component{}}
	if err = resolver.resolveParamType(inputContract, fields[0], p); err != nil {
		t.Fatal(err)
	}
	if p.TypeExpr != "string" || p.OutputTypeExpr != "*explicit.Claims" || p.Codec.OutputType != "*codec.Claims" || p.Codec.Body != "JwtClaim" || p.ErrorStatusCode != 401 {
		t.Fatalf("explicit codec contract changed: %+v", p)
	}
}
