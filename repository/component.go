package repository

import (
	"context"
	"embed"
	"fmt"
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
	"github.com/viant/datly/service"
	"github.com/viant/datly/service/executor/handler"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/utils/formatter"
	"github.com/viant/datly/utils/types"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/extension"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/state/kind/locator"
	"github.com/viant/datly/view/tags"
	"github.com/viant/tagly/format/text"
	"github.com/viant/xdatly/docs"
	xhandler "github.com/viant/xdatly/handler"
	hstate "github.com/viant/xdatly/handler/state"
	"github.com/viant/xreflect"
	"net/http"
	"reflect"
	"strings"
)

// Component represents abstract API view/handler based component
type (
	Component struct {
		version.Version `json:"-" yaml:"-"`
		contract.Path
		contract.Contract
		content.Content `json:",omitempty" yaml:",inline"`
		Async           *async.Config `json:",omitempty"`
		View            *view.View    `json:",omitempty"`
		NamespacedView  *view.NamespacedView
		Handler         *handler.Handler `json:",omitempty"`
		indexedView     view.NamedViews
		SourceURL       string

		dispatcher contract.Dispatcher
		types      *xreflect.Types
		ioConfig   *config.IOConfig
		doc        docs.Service
		embedFs    *embed.FS
		with       []string
	}

	ComponentOption func(c *Component) error
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
		c.Contract.Service = service.TypeExecutor
	}

	err = c.initView(ctx, resource)
	if err != nil {
		return err
	}
	if err := c.initInputParameters(ctx, resource); err != nil {
		return err
	}
	if err = c.Contract.Init(ctx, &c.Path, c.View, resource); err != nil {
		return err
	}

	if err = c.updatedViewSchemaWithNamedType(ctx, resource); err != nil {
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
	lookupType := resource.LookupType()
	if err := c.Content.Marshaller.Init(lookupType); err != nil {
		return err
	}
	if err = c.Async.Init(ctx, resource, c.View); err != nil {
		return err
	}
	c.doc, _ = resource.Doc()
	return nil
}

func (c *Component) updatedViewSchemaWithNamedType(ctx context.Context, resource *view.Resource) error {
	outputSchema := c.Contract.Output.Type.Schema

	if param := c.Contract.Output.Type.Parameters.LookupByLocation(state.KindOutput, "view"); param != nil && outputSchema.IsNamed() {
		oType := types.EnsureStruct(outputSchema.Type())
		if viewField, ok := oType.FieldByName(param.Name); ok {
			if !c.View.Schema.IsNamed() {
				c.View.SetNamedType(viewField.Type)
				if !isGeneratorContext(ctx) {
					param.Schema.SetType(viewField.Type)
				}
			}
		}
		if summaryParam := c.Contract.Output.Type.Parameters.LookupByLocation(state.KindOutput, "summary"); summaryParam != nil {
			if aTag, _ := tags.Parse(reflect.StructTag(param.Tag), nil, tags.SQLSummaryTag); aTag != nil {
				if summary := aTag.SummarySQL; summary.SQL != "" {
					c.View.Template.Summary = &view.TemplateSummary{Name: summaryParam.Name, Source: summary.SQL, Schema: summaryParam.Schema}
					if err := c.View.Template.Summary.Init(ctx, c.View.Template, resource); err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

func (c *Component) initTransforms(ctx context.Context) error {
	for _, transform := range c.Transforms {
		if err := transform.Init(ctx, afs.New(), c.types.Lookup); err != nil {
			return err
		}
	}

	return nil
}

func (c *Component) initInputParameters(ctx context.Context, resource *view.Resource) error {
	if len(c.Contract.Input.Type.Parameters) > 0 {
		return nil
	}
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

func (c *Component) LocatorOptions(request *http.Request, form *hstate.Form, unmarshal shared.Unmarshal) []locator.Option {

	var result []locator.Option

	if unmarshal != nil {
		result = append(result, locator.WithUnmarshal(unmarshal))
	}
	if c.dispatcher != nil {
		result = append(result, locator.WithDispatcher(c.dispatcher))
	}
	if form != nil {
		result = append(result, locator.WithForm(form))
	}

	if request != nil {
		result = append(result, locator.WithRequest(request))

	}
	if c.View != nil {
		result = append(result, locator.WithResource(c.View.GetResource()))
	}
	result = append(result, locator.WithURIPattern(c.URI))
	result = append(result, locator.WithIOConfig(c.IOConfig()))
	result = append(result, locator.WithInputParameters(c.Input.Type.Parameters.Index()))
	result = append(result, locator.WithOutputParameters(c.Output.Type.Parameters))
	if c.Input.Body.Schema != nil {
		bodyType := c.Input.Body.Schema.Type()
		result = append(result, locator.WithBodyType(bodyType))
	}
	if len(c.indexedView) > 0 {
		result = append(result, locator.WithViews(c.indexedView))
	}
	return result
}

func (c *Component) IOConfig() *config.IOConfig {
	ret := c.ioConfig
	if ret != nil {
		return c.ioConfig
	}
	ret = &config.IOConfig{
		OmitEmpty:  c.Output.OmitEmpty,
		CaseFormat: text.NewCaseFormat(string(c.Output.CaseFormat)),
		Exclude:    config.Exclude(c.Output.Exclude).Index(),
		DateFormat: c.DateFormat,
	}
	c.ioConfig = ret
	return ret
}

func (c *Component) UnmarshalFunc(request *http.Request) shared.Unmarshal {
	contentType := request.Header.Get(content.HeaderContentType)
	setter.SetStringIfEmpty(&contentType, request.Header.Get(strings.ToLower(content.HeaderContentType)))
	switch contentType {
	case content.XMLContentType:
		return c.Content.Marshaller.XML.Unmarshal
	case content.CSVContentType:
		return c.Content.CSV.Unmarshal
	default:
		switch c.Output.DataFormat {
		case content.XMLFormat:
			return c.Content.Marshaller.XML.Unmarshal
		}
	}
	jsonPathInterceptor := json.UnmarshalerInterceptors{}
	unmarshallerInterceptors := c.UnmarshallerInterceptors()
	for i := range unmarshallerInterceptors {
		transform := unmarshallerInterceptors[i]
		jsonPathInterceptor[transform.Path] = c.transformFn(request, transform)
	}
	return func(bytes []byte, i interface{}) error {
		return c.Content.Marshaller.JSON.JsonMarshaller.Unmarshal(bytes, i, jsonPathInterceptor, request)
	}
}

func (c *Component) normalizePaths() error {
	if !c.Output.ShouldNormalizeExclude() {
		return nil
	}
	for i, transform := range c.Transforms {
		c.Transforms[i].Path = formatter.NormalizePath(transform.Path)
	}
	return nil
}

func (c *Component) transformFn(request *http.Request, transform *marshal.Transform) func(dst interface{}, decoder *gojay.Decoder, options ...interface{}) error {
	unmarshaller := transform.UnmarshalerInto()
	if unmarshaller != nil {
		return unmarshaller.UnmarshalJSONWithOptions
	}
	return func(dst interface{}, decoder *gojay.Decoder, options ...interface{}) error {
		evaluate, err := transform.Evaluate(request, decoder, c.types.Lookup)
		if err != nil {
			return err
		}
		if evaluate.Ctx.Decoder.Decoded != nil {
			reflect.ValueOf(dst).Elem().Set(reflect.ValueOf(evaluate.Ctx.Decoder.Decoded))
		}
		return nil
	}
}

func (c *Component) Doc() (docs.Service, bool) {
	return c.doc, c.doc != nil
}

func NewComponent(path *contract.Path, options ...ComponentOption) (*Component, error) {
	ret := &Component{Path: *path, View: &view.View{}}
	res := &view.Resource{}
	res.SetTypes(extension.Config.Types)
	ret.View.SetResource(res)
	for _, opt := range options {
		if err := opt(ret); err != nil {
			return nil, err
		}
	}

	return ret, nil
}

func WithView(aView *view.View) ComponentOption {
	return func(c *Component) error {
		c.View = aView
		return nil
	}
}

func WithHandler(aHandler xhandler.Handler) ComponentOption {
	return func(c *Component) error {
		c.Handler = handler.NewHandler(aHandler)
		if c.View == nil {
			c.View = &view.View{}
		}
		c.View.Mode = view.ModeHandler
		c.Service = service.TypeExecutor
		return nil
	}
}

func WithInput(inputType reflect.Type) ComponentOption {
	return func(c *Component) error {
		c.Contract.Input.Type = state.Type{Schema: state.NewSchema(inputType)}
		if err := c.Contract.Input.Type.Init(); err != err {
			return fmt.Errorf("failed to initalize input: %w", err)
		}
		return nil
	}
}

func WithOutputCaseFormat(format string) ComponentOption {
	return func(c *Component) error {
		c.Contract.Output.CaseFormat = text.CaseFormat(format)
		return nil
	}
}

func WithContract(inputType, outputType reflect.Type, embedFs *embed.FS, viewOptions ...view.Option) ComponentOption {
	return func(c *Component) error {
		if outputType == nil {
			outputType = reflect.TypeOf(struct{}{})
		}
		c.embedFs = embedFs
		resource := c.View.Resource()
		sType, err := state.NewType(state.WithResource(resource), state.WithFS(embedFs), state.WithSchema(state.NewSchema(inputType)))
		if err != nil {
			return err
		}
		c.Contract.Input.Type = *sType
		if err := c.Contract.Input.Type.Init(); err != err {
			return fmt.Errorf("failed to initalize input: %w", err)
		}
		if len(c.Contract.Input.Type.Parameters) > 0 {
			for i := range c.Contract.Input.Type.Parameters {
				resource.AppendParameter(c.Contract.Input.Type.Parameters[i])
			}
		}
		c.Contract.Output.Type = state.Type{Schema: state.NewSchema(outputType)}
		if err := c.Contract.Output.Type.Init(state.WithFS(embedFs)); err != err {
			return fmt.Errorf("failed to initalize output: %w", err)
		}
		if c.Contract.Output.CaseFormat == "" {
			//todo this needs to be pass through from options
			c.Contract.Output.CaseFormat = "lc"
		}
		sTypes := types.EnsureStruct(outputType)
		viewName := sTypes.Name()
		if index := strings.LastIndex(viewName, "Output"); index != -1 {
			viewName = viewName[:index]
			if c.Handler != nil {
				viewName += "Handler"
			}
		}
		table := ""
		if viewParameter := c.Contract.Output.Type.Parameters.LookupByLocation(state.KindOutput, "view"); viewParameter != nil {
			viewOptions = append(viewOptions, view.WithViewType(viewParameter.Schema.SliceType().Elem()))
			aTag, err := tags.ParseViewTags(reflect.StructTag(viewParameter.Tag), embedFs)
			if err != nil {
				return fmt.Errorf("invalid output view %v tag: %w", viewName, err)
			}
			if aView := aTag.View; aView != nil {
				setter.SetStringIfEmpty(&viewName, aView.Name)
				table = aView.Table
				if aView.Match != "" {
					viewOptions = append(viewOptions, view.WithMatchStrategy(aView.Match))
				}

			}

			if aTag.SQL.SQL != "" {
				anInputType := c.Contract.Input.Type
				viewOptions = append(viewOptions, view.WithSQL(string(aTag.SQL.SQL), anInputType.Parameters...))
			}

			if aTag.View.Connector != "" {
				viewOptions = append(viewOptions, view.WithConnector(&view.Connector{Reference: shared.Reference{Ref: aTag.View.Connector}}))
			}
			if aTag.View.Batch != 0 {
				viewOptions = append(viewOptions, view.WithBatchSize(aTag.View.Batch))
			}

			if aTag.View.PartitionerType != "" {
				viewOptions = append(viewOptions, view.WithPartitioned(&view.Partitioned{DataType: aTag.View.PartitionerType, Concurrency: aTag.View.PartitionedConcurrency}))
			}

			viewOptions = append(viewOptions, view.WithFS(c.embedFs))
		}
		viewOptions = append(viewOptions, view.WithResource(resource))
		aView := view.NewView(viewName, table, viewOptions...)
		c.View = aView
		return nil
	}
}
