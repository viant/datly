package handler

import (
	"context"
	goJson "encoding/json"
	"github.com/viant/datly/router/status"
	_ "github.com/viant/datly/service/locator"
	reader2 "github.com/viant/datly/service/reader"
	"github.com/viant/datly/utils/httputils"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/session"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/state/kind/locator"
	"github.com/viant/structology"
	"github.com/viant/xdatly/handler/response"
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
		Reader     *reader2.Output
		Output     interface{}
		OutputType reflect.Type
		Status     *response.Status
		StatusCode int
		Error      error
		http.Header
	}
)

func (r *Response) SetError(err error, statusCode int) {
	r.Error = err
	r.StatusCode, r.Status.Message, r.Status.Errors = status.NormalizeErr(err, statusCode)
	r.Status.Status = "error"

}
func (h *Handler) Handle(ctx context.Context, aView *view.View, state *session.State, opts ...reader2.Option) *Response {
	ret := &Response{Header: http.Header{}, Status: &response.Status{Status: "ok"}}
	err := h.readData(ctx, aView, state, ret, opts)
	if err != nil {
		ret.SetError(err, h.errorStatusCode())
		return ret
	}
	if h.output == nil {
		return ret
	}
	if h.output == nil || !h.output.IsDefined() {
		return ret
	}
	resultState := h.output.NewState()
	if err = state.SetState(ctx, h.parameters, resultState, state.Indirect(true,
		locator.WithCustomOption(ret.Reader, ret.Status))); err != nil {
		ret.StatusCode = http.StatusInternalServerError
		ret.Error = err
		return ret
	}
	ret.OutputType = h.output.Type()
	ret.Output = resultState.State()
	return ret
}

func (h *Handler) readData(ctx context.Context, aView *view.View, aState *session.State, ret *Response, opts []reader2.Option) error {
	destValue := reflect.New(aView.Schema.SliceType())
	dest := destValue.Interface()
	aSession, err := reader2.NewSession(dest, aView)
	if err != nil {
		return err
	}
	aSession.IncludeSQL = true
	for _, opt := range opts {
		if err = opt(aSession); err != nil {
			return err
		}
	}
	if err = aState.Populate(ctx); err != nil {
		return err
	}
	aSession.State = aState.State()
	if err = reader2.New().Read(context.TODO(), aSession); err != nil {
		return err //TODO add 501 for database errors ?
	}
	ret.Reader = &aSession.Output

	ret.Output = ret.Reader.Data
	if aSession.View.Schema.Cardinality == state.One && h.output == nil {
		slice := reflect.ValueOf(ret.Output)
		switch slice.Len() {
		case 0:
			ret.Output = nil
		case 1:
			ret.Output = reflect.ValueOf(ret.Output).Index(0).Interface()
		}
	}

	h.publishViewMetaIfNeeded(aView, ret)
	h.publishMetricsIfNeeded(aSession, ret)
	return nil
}

func (h *Handler) publishViewMetaIfNeeded(aView *view.View, ret *Response) {
	templateMeta := aView.Template.Meta
	if ret.Reader.ViewMeta == nil || templateMeta == nil {
		return
	}
	if templateMeta.Kind != view.MetaTypeHeader {
		return
	}
	data, err := goJson.Marshal(ret.Reader.ViewMeta)
	if err != nil {
		ret.StatusCode = http.StatusInternalServerError
		ret.Status.Status = "error"
		ret.Status.Message = err.Error()
	}
	ret.Header.Add(templateMeta.Name, string(data))
}

func (h *Handler) publishMetricsIfNeeded(aSession *reader2.Session, ret *Response) {
	if aSession.RevealMetric {
		return
	}
	for _, info := range aSession.Metrics {
		if info.Execution == nil {
			continue
		}
		if !aSession.IncludeSQL {
			info = info.HideSQL()
		}
		data, err := goJson.Marshal(info)
		if err != nil {
			continue
		}
		ret.Header.Add(httputils.DatlyResponseHeaderMetrics+"-"+info.Name(), string(data))
	}
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