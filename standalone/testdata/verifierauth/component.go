package verifierauth

import (
	"context"
	"errors"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/scy/auth/jwt"
	"github.com/viant/xdatly"
	xauth "github.com/viant/xdatly/auth"
	"github.com/viant/xdatly/handler"
	"reflect"
)

type Component struct {
	Verify xdatly.Component[Input, Output] `component:"Verify,path=/verify-only,method=GET,handler=NewVerify"`
}

var Datly = new(Component)
var LinkedType = reflect.TypeFor[Component]()

func (Component) DatlyHandler(name string) func() (rhandler.TypedHandler, error) {
	if name == "NewVerify" {
		return custom.Factory(NewVerify)
	}
	return nil
}

type Input struct {
	JWT *jwt.Claims `parameter:"JWT,kind=header,in=Authorization,dataType=string,required=true,errorCode=401" codec:"JwtClaim" json:"-"`
}
type Output struct {
	Subject        string `json:"subject"`
	Verifier       bool   `json:"verifier"`
	Signer         bool   `json:"signer"`
	DefaultEnabled bool   `json:"defaultEnabled"`
}
type verify struct {
	Auth xauth.Auth `bind:"kind=auth,required"`
}

func NewVerify() handler.Contract[Input, Output] { return &verify{} }
func (h *verify) Exec(_ context.Context, _ handler.Session, input *Input, out *Output) error {
	if h.Auth == nil || h.Auth.Verifier() == nil || input.JWT == nil {
		return errors.New("missing verifier-only capability")
	}
	out.Subject = input.JWT.Subject
	out.Verifier = true
	out.Signer = h.Auth.Signer() != nil
	_, err := h.Auth.Authenticator(xauth.VendorDefault)
	out.DefaultEnabled = err == nil
	return nil
}
