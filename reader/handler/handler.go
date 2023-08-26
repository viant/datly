package handler

import (
	"context"
	"github.com/viant/datly/reader"
	_ "github.com/viant/datly/reader/locator"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/session"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/state/kind/locator"
	"github.com/viant/structology"
	"net/http"
	"reflect"
)

type (
	Handler struct {
		output          *structology.StateType
		parameters      state.Parameters
		ErrorStatusCode *int
	}

	//Response reader handler response
	Response struct {
		Output     *reader.Output
		Value      interface{}
		StatusCode int
		Error      error
	}
)

func (h *Handler) Handle(ctx context.Context, aView *view.View, state *session.State, opts ...reader.Option) *Response {
	ret := &Response{}
	err := h.readData(ctx, aView, state, ret, opts)
	if err != nil {
		ret.StatusCode = h.errorStatusCode()
		ret.Error = err
		return ret
	}
	if h.output == nil {
		return ret
	}
	ret.Value = ret.Output.SyncData()
	resultState := h.output.NewState()
	if err = state.SetState(ctx, h.parameters, resultState, state.Indirect(true, locator.WithCustomOption(ret.Output))); err != nil {
		ret.StatusCode = http.StatusInternalServerError
		ret.Error = err
		return ret
	}
	ret.Value = resultState.State()
	return ret
}

func (h *Handler) readData(ctx context.Context, aView *view.View, state *session.State, ret *Response, opts []reader.Option) error {
	destValue := reflect.New(aView.Schema.SliceType())
	dest := destValue.Interface()
	aSession, err := reader.NewSession(dest, aView)
	if err != nil {
		return err
	}
	for _, opt := range opts {
		if err = opt(aSession); err != nil {
			return err
		}
	}
	if err = state.Populate(ctx); err != nil {
		return err
	}
	aSession.State = state.ResourceState()
	if err = reader.New().Read(context.TODO(), aSession); err != nil {
		return err //TODO add 501 for database errors ?
	}
	aSession.Output.SyncData()
	ret.Output = &aSession.Output
	return nil
}

func (h *Handler) errorStatusCode() int {
	defaultCode := http.StatusBadRequest
	if h.ErrorStatusCode != nil {
		defaultCode = *h.ErrorStatusCode
	}
	return defaultCode
}

// New creates a new reader handler
func New(output *structology.StateType, parameters state.Parameters) *Handler {
	return &Handler{output: output, parameters: parameters}
}
