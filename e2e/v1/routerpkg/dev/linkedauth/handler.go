package linkedauth

import (
	"context"
	"reflect"

	"github.com/viant/scy/auth/jwt"
	"github.com/viant/xdatly"
	xhandler "github.com/viant/xdatly/handler"
	"github.com/viant/xdatly/handler/response"
	"github.com/viant/xdatly/types/core"
	"github.com/viant/xdatly/types/custom/dependency/checksum"
)

const packageName = "github.com/viant/datly/e2e/v1/routerpkg/dev/linkedauth"

func init() {
	core.RegisterType(packageName, "LinkedAuthInput", reflect.TypeOf(LinkedAuthInput{}), checksum.GeneratedTime)
	core.RegisterType(packageName, "LinkedAuthOutput", reflect.TypeOf(LinkedAuthOutput{}), checksum.GeneratedTime)
	core.RegisterType(packageName, "LinkedAuthPayload", reflect.TypeOf(LinkedAuthPayload{}), checksum.GeneratedTime)
	core.RegisterType(packageName, "Handler", reflect.TypeOf(Handler{}), checksum.GeneratedTime)
}

type LinkedAuthInput struct {
	Jwt  *jwt.Claims `parameter:",kind=header,in=Authorization,dataType=string,errorCode=401" codec:"JwtClaim"`
	Echo string      `parameter:",kind=query,in=echo"`
}

type LinkedAuthPayload struct {
	UserID    int    `json:"userID"`
	FirstName string `json:"firstName,omitempty"`
	Echo      string `json:"echo,omitempty"`
}

type LinkedAuthOutput struct {
	response.Status `parameter:",kind=output,in=status" json:",omitempty"`
	Data            *LinkedAuthPayload `parameter:",kind=output,in=view" json:"data,omitempty"`
}

type Handler struct{}

func (h *Handler) Exec(ctx context.Context, sess xhandler.Session) (interface{}, error) {
	input := &LinkedAuthInput{}
	if err := sess.Stater().Bind(ctx, input); err != nil {
		return nil, err
	}
	if input.Jwt == nil {
		return nil, response.NewError(401, "unauthorized access")
	}
	return &LinkedAuthOutput{
		Data: &LinkedAuthPayload{
			UserID:    input.Jwt.UserID,
			FirstName: input.Jwt.FirstName,
			Echo:      input.Echo,
		},
	}, nil
}

type LinkedAuthRouter struct {
	LinkedAuth xdatly.Component[LinkedAuthInput, LinkedAuthOutput] `component:",path=/v1/api/shape/dev/linked/auth,method=GET,connector=dev,input=LinkedAuthInput,output=LinkedAuthOutput,handler=Handler"`
}
