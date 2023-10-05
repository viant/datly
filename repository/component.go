package repository

import (
	"context"
	"github.com/francoispqt/gojay"
	"github.com/viant/afs"
	"github.com/viant/datly/gateway/router/marshal"
	"github.com/viant/datly/gateway/router/marshal/config"
	"github.com/viant/datly/gateway/router/marshal/json"
	"github.com/viant/datly/internal/setter"
	"github.com/viant/datly/repository/async"
	"github.com/viant/datly/repository/content"
	"github.com/viant/datly/repository/contract"
	"github.com/viant/datly/repository/version"
	"github.com/viant/datly/service/executor/handler"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/utils/formatter"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state/kind/locator"
	"github.com/viant/xreflect"
	"net/http"
	"reflect"
	"strings"
)

// Component represents abstract API view/handler based component
type (
	Component struct {
		version.Version
		contract.Path
		contract.Contract
		content.Content
		Async          *async.Config `json:",omitempty"`
		View           *view.View    `json:",omitempty"`
		NamespacedView *view.NamespacedView
		Handler        *handler.Handler `json:",omitempty"`
		indexedView    view.NamedViews
		SourceURL      string

		dispatcher contract.Dispatcher
		types      *xreflect.Types
	}
)

func (c *Component) TypeRegistry() *xreflect.Types {
	return c.types
}

func (c *Component) Init(ctx context.Context, resource *view.Resource) (err error) {
	c.types = resource.TypeRegistry()
	if c.Output.Style == contract.BasicStyle {
		c.Output.Field = ""
	}

	if c.Handler != nil {
		if err = c.Handler.Init(ctx, resource); err != nil {
			return err
		}
	}
	err = c.initView(ctx, resource)
	if err != nil {
		return err
	}
	if err := c.initInputParameters(ctx, resource); err != nil {
		return err
	}
	if err = c.Contract.Init(ctx, &c.Path, c.View); err != nil {
		return err
	}
	if err := c.normalizePaths(); err != nil {
		return err
	}
	if err := c.initTransforms(ctx); err != nil {
		return nil
	}
	if err := c.Content.InitMarshaller(c.IOConfig(), c.Output.Exclude, c.BodyType(), c.OutputType()); err != nil {
		return err
	}
	if err = c.Async.Init(ctx, resource, c.View); err != nil {
		return err
	}
	return nil
}

func (r *Component) initTransforms(ctx context.Context) error {
	for _, transform := range r.Transforms {
		if err := transform.Init(ctx, afs.New(), r.types.Lookup); err != nil {
			return err
		}
	}

	return nil
}

func (c *Component) initInputParameters(ctx context.Context, resource *view.Resource) error {
	inputParameters := resource.Parameters
	for _, parameter := range c.View.InputParameters() {
		inputParameters.Append(parameter)
	}
	if c.Async != nil {
		inputParameters.Append(c.Async.JobMatchKey)
		if c.Async.UserID != nil {
			inputParameters.Append(c.Async.UserID)
		}
		if c.Async.UserEmail != nil {
			inputParameters.Append(c.Async.UserEmail)
		}
	}
	c.Contract.Input.Type.Parameters = inputParameters
	return nil
}

func (c *Component) initView(ctx context.Context, resource *view.Resource) error {
	c.View.Standalone = true
	if c.View.Name == "" {
		c.View.Name = c.View.Ref
	}
	if err := c.View.Init(ctx, resource); err != nil {
		return err
	}
	c.NamespacedView = view.IndexViews(c.View)
	c.indexedView = resource.Views.Index()
	return nil
}

func (c *Component) Exclusion(state *view.State) []*json.FilterEntry {
	result := make([]*json.FilterEntry, 0)
	state.Lock()
	defer state.Unlock()
	for viewName, selector := range state.Views {
		if len(selector.Columns) == 0 {
			continue
		}
		var aPath string
		nsView := c.NamespacedView.ByName(viewName)
		if nsView == nil {
			aPath = ""
		} else {
			aPath = nsView.Path
		}
		fields := make([]string, len(selector.Fields))
		for i := range selector.Fields {
			fields[i] = selector.Fields[i]
		}
		result = append(result, &json.FilterEntry{
			Path:   aPath,
			Fields: fields,
		})

	}
	return result
}

func (c *Component) LocatorOptions(request *http.Request, unmarshal shared.Unmarshal) []locator.Option {
	var result []locator.Option
	result = append(result, locator.WithUnmarshal(unmarshal))
	if c.dispatcher != nil {
		result = append(result, locator.WithDispatcher(c.dispatcher))
	}
	result = append(result, locator.WithRequest(request))
	result = append(result, locator.WithURIPattern(c.URI))
	result = append(result, locator.WithIOConfig(c.IOConfig()))
	result = append(result, locator.WithInputParameters(c.Input.Type.Parameters.Index()))
	result = append(result, locator.WithOutputParameters(c.Output.Type.Parameters))
	if c.Input.Body.Schema != nil {
		result = append(result, locator.WithBodyType(c.BodyType()))
	}
	if len(c.indexedView) > 0 {
		result = append(result, locator.WithViews(c.indexedView))
	}
	return result
}

func (c *Component) IOConfig() config.IOConfig {
	return config.IOConfig{
		OmitEmpty:  c.Output.OmitEmpty,
		CaseFormat: *c.Output.FormatCase(),
		Exclude:    config.Exclude(c.Output.Exclude).Index(),
		DateLayout: c.DateFormat,
	}
}

func (r *Component) UnmarshalFunc(request *http.Request) shared.Unmarshal {
	contentType := request.Header.Get(content.HeaderContentType)
	setter.SetStringIfEmpty(&contentType, request.Header.Get(strings.ToLower(content.HeaderContentType)))
	switch contentType {
	case content.CSVContentType:
		return r.Content.CSV.Unmarshal
	}
	jsonPathInterceptor := json.UnmarshalerInterceptors{}
	unmarshallerInterceptors := r.UnmarshallerInterceptors()
	for i := range unmarshallerInterceptors {
		transform := unmarshallerInterceptors[i]
		jsonPathInterceptor[transform.Path] = r.transformFn(request, transform)
	}
	return func(bytes []byte, i interface{}) error {
		return r.Content.JsonMarshaller.Unmarshal(bytes, i, jsonPathInterceptor, request)
	}
}

func (r *Component) normalizePaths() error {
	if !r.Output.ShouldNormalizeExclude() {
		return nil
	}
	for i, transform := range r.Transforms {
		r.Transforms[i].Path = formatter.NormalizePath(transform.Path)
	}
	return nil
}

func (r *Component) transformFn(request *http.Request, transform *marshal.Transform) func(dst interface{}, decoder *gojay.Decoder, options ...interface{}) error {
	unmarshaller := transform.UnmarshalerInto()
	if unmarshaller != nil {
		return unmarshaller.UnmarshalJSONWithOptions
	}
	return func(dst interface{}, decoder *gojay.Decoder, options ...interface{}) error {
		evaluate, err := transform.Evaluate(request, decoder, r.types.Lookup)
		if err != nil {
			return err
		}
		if evaluate.Ctx.Decoder.Decoded != nil {
			reflect.ValueOf(dst).Elem().Set(reflect.ValueOf(evaluate.Ctx.Decoder.Decoded))
		}
		return nil
	}
}
