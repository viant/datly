package handler

import (
	"context"
	goJson "github.com/goccy/go-json"
	"github.com/viant/datly/gateway/router/status"
	_ "github.com/viant/datly/repository/locator/async"
	_ "github.com/viant/datly/repository/locator/component"
	_ "github.com/viant/datly/repository/locator/meta"
	_ "github.com/viant/datly/repository/locator/output"
	_ "github.com/viant/datly/service/executor/handler/locator"

	reader "github.com/viant/datly/service/reader"
	"github.com/viant/datly/service/session"
	"github.com/viant/datly/utils/httputils"
	"github.com/viant/datly/view"
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
		outputType      *state.Type
		ErrorStatusCode *int
	}

	//Response reader handler response
	Response struct {
		Reader     *reader.Output
		Output     interface{}
		OutputType reflect.Type
		Status     *response.Status
		Metrics    response.Metrics
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
func (h *Handler) Handle(ctx context.Context, aView *view.View, aSession *session.Session, opts ...reader.Option) *Response {
	ret := &Response{Header: http.Header{}, Status: &response.Status{Status: "ok"}}
	err := h.readData(ctx, aView, aSession, ret, opts)
	if err != nil {
		ret.SetError(err, h.errorStatusCode())
		return ret
	}
	if h.output == nil {
		return ret
	}
	if h.output == nil || !h.output.IsDefined() || h.outputType.IsAnonymous() {
		return ret
	}

	resultState := h.output.NewState()
	statelet := aSession.State().Lookup(aView)

	var locatorOptions []locator.Option
	locatorOptions = append(locatorOptions, locator.WithParameterLookup(func(ctx context.Context, parameter *state.Parameter) (interface{}, bool, error) {
		return aSession.LookupValue(ctx, parameter, aSession.Indirect(true, locatorOptions...))
	}),
		locator.WithMetrics(ret.Metrics),
		locator.WithView(aView),
		locator.WithState(statelet.Template),
		locator.WithCustom(ret.Reader, ret.Status))

	var options = aSession.Indirect(true, locatorOptions...)

	if err = aSession.SetState(ctx, h.outputType.Parameters, resultState, options); err != nil {
		ret.StatusCode = http.StatusInternalServerError
		ret.Error = err
		return ret
	}
	ret.OutputType = h.output.Type()
	output := resultState.State()
	if reflect.TypeOf(output).Kind() == reflect.Struct {
		resultState.SyncPointer()
		output = resultState.StatePtr()
	}
	ret.Output = output
	return ret
}

func (h *Handler) readData(ctx context.Context, aView *view.View, aState *session.Session, ret *Response, opts []reader.Option) error {
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
	if err = aState.Populate(ctx); err != nil {
		return err
	}
	aSession.State = aState.State()
	if err = reader.New().Read(ctx, aSession); err != nil {
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
	h.publishViewSummaryIfNeeded(aView, ret)
	h.publishMetricsIfNeeded(aSession, ret)
	return nil
}

func (h *Handler) publishViewSummaryIfNeeded(aView *view.View, ret *Response) {
	templateMeta := aView.Template.Summary
	if ret.Reader.DataSummary == nil || templateMeta == nil {
		return
	}
	if templateMeta.Kind != view.MetaKindHeader {
		return
	}
	data, err := goJson.Marshal(ret.Reader.DataSummary)
	if err != nil {
		ret.StatusCode = http.StatusInternalServerError
		ret.Status.Status = "error"
		ret.Status.Message = err.Error()
	}
	ret.Header.Add(templateMeta.Name, string(data))
}

func (h *Handler) publishMetricsIfNeeded(aSession *reader.Session, ret *Response) {
	ret.Metrics = aSession.Metrics
	if aSession.RevealMetric {
		return
	}
	for _, info := range aSession.Metrics {
		if info.Executions == nil {
			continue
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
func New(output *structology.StateType, outputType *state.Type) *Handler {
	return &Handler{output: output, outputType: outputType}
}
