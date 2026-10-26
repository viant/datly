package components

import (
	"context"
	"github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/xdatly"
	h "github.com/viant/xdatly/handler"
)

type MbusComponent struct {
	Publish xdatly.Component[PublishInput, PublishOutput] `component:"Publish,path=/publish,method=POST,handler=NewPublish"`
}

func (MbusComponent) DatlyHandler(name string) func() (handler.TypedHandler, error) {
	if name == "NewPublish" || name == Package+".NewPublish" {
		return custom.Factory(NewPublish)
	}
	return nil
}

type PublishInput struct {
	Bus  h.MessageBus `parameter:"Bus,kind=mbus,required=true" json:"-"`
	Body string       `parameter:"Body,kind=body,in=body" json:"body"`
}
type PublishOutput struct {
	ID string `json:"id"`
}
type publishHandler struct{}

func NewPublish() h.Contract[PublishInput, PublishOutput] { return &publishHandler{} }
func (*publishHandler) Exec(ctx context.Context, _ h.Session, in *PublishInput, out *PublishOutput) error {
	ack, err := in.Bus.Push(ctx, in.Bus.Message("records", in.Body))
	if err != nil {
		return err
	}
	out.ID = ack.MessageID
	return nil
}
