package repository

import (
	"context"
	"embed"
	"fmt"
	"net/http"
	"reflect"
	"strings"

	"github.com/francoispqt/gojay"
	"github.com/viant/afs"
	"github.com/viant/datly/gateway/router/marshal"
	"github.com/viant/datly/gateway/router/marshal/config"
	"github.com/viant/datly/gateway/router/marshal/json"
	"github.com/viant/datly/internal/setter"
	"github.com/viant/datly/repository/async"
	content "github.com/viant/datly/repository/content"
	"github.com/viant/datly/repository/contract"
	"github.com/viant/datly/repository/handler"
	"github.com/viant/datly/repository/shape/typectx"
	"github.com/viant/datly/repository/version"
	"github.com/viant/datly/service"
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
)

// Component represents abstract API view/handler based component
type (
	Component struct {
		version.Version `json:"-" yaml:"-"`
		contract.Meta
		contract.Path
		contract.Contract
		content.Content `json:",omitempty" yaml:",inline"`
		Async           *async.Config `json:",omitempty"`
		View            *view.View    `json:",omitempty"`
		NamespacedView  *view.NamespacedView
		Handler         *handler.Handler `json:",omitempty"`
		TypeContext     *typectx.Context `json:",omitempty" yaml:",omitempty"`
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

func (c *Component) Docs() *state.Docs {
	ret := &state.Docs{}
	if c.View == nil {
		return ret
	}
	res := c.View.GetResource()
	if res == nil {
		return ret
	}
	if res.Docs == nil {
		return ret
	}
	return res.Docs.Docs
}

// LookupParameter lookups parameter by name
func (c *Component) LookupParameter(name string) *state.Parameter {
	parameter := c.Input.Type.Parameters.Lookup(name)
	if parameter == nil {
		parameter = c.Output.Type.Parameters.Lookup(name)
		if parameter == nil {
			parameter = c.View.Template.Parameters.Lookup(name)
		}
	}
	return parameter
}

func (c *Component) TypeRegistry() *xreflect.Types {
	return c.types
}

func (c *Component) Init(ctx context.Context, resource *view.Resource) (err error) {
	c.types = resource.TypeRegistry()
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
	// For read components (GET), expose and enable offset/limit/fields/page/orderBy for each namespaced view.
	if strings.EqualFold(c.Path.Method, http.MethodGet) {
		// Helper to enable limit/offset for a view with namespace prefix (if any)
		ensureSelectors := func(v *view.View, nsPrefix string) {
			if v == nil {
				return
			}
			if v.Selector == nil {
				v.Selector = &view.Config{}
			}
			if v.Selector.Constraints == nil {
				v.Selector.Constraints = &view.Constraints{}
			}
			// Enable constraints
			v.Selector.Constraints.Limit = true
			v.Selector.Constraints.Offset = true
			v.Selector.Constraints.Projection = true
			v.Selector.Constraints.OrderBy = true

			// Limit param
			if v.Selector.LimitParameter == nil {
				p := *view.QueryStateParameters.LimitParameter
				p.Description = view.Description(view.LimitQuery, v.Name)
				if nsPrefix != "" {
					p.In = state.NewQueryLocation(nsPrefix + view.LimitQuery)
				}
				v.Selector.LimitParameter = &p
			} else if v.Selector.LimitParameter.Description == "" {
				v.Selector.LimitParameter.Description = view.Description(view.LimitQuery, v.Name)
			}

			// Offset param
			if v.Selector.OffsetParameter == nil {
				p := *view.QueryStateParameters.OffsetParameter
				p.Description = view.Description(view.OffsetQuery, v.Name)
				if nsPrefix != "" {
					p.In = state.NewQueryLocation(nsPrefix + view.OffsetQuery)
				}
				v.Selector.OffsetParameter = &p
			} else if v.Selector.OffsetParameter.Description == "" {
				v.Selector.OffsetParameter.Description = view.Description(view.OffsetQuery, v.Name)
			}

			// Fields param (controls which fields are included)
			if v.Selector.FieldsParameter == nil {
				p := *view.QueryStateParameters.FieldsParameter
				p.Description = view.Description(view.FieldsQuery, v.Name)
				if nsPrefix != "" {
					p.In = state.NewQueryLocation(nsPrefix + view.FieldsQuery)
				}
				v.Selector.FieldsParameter = &p
			} else if v.Selector.FieldsParameter.Description == "" {
				v.Selector.FieldsParameter.Description = view.Description(view.FieldsQuery, v.Name)
			}

			// Page param (paging interface on top of limit/offset)
			if v.Selector.PageParameter == nil {
				p := *view.QueryStateParameters.PageParameter
				p.Description = view.Description(view.PageQuery, v.Name)
				if nsPrefix != "" {
					p.In = state.NewQueryLocation(nsPrefix + view.PageQuery)
				}
				v.Selector.PageParameter = &p
			} else if v.Selector.PageParameter.Description == "" {
				v.Selector.PageParameter.Description = view.Description(view.PageQuery, v.Name)
			}

			// OrderBy param
			if v.Selector.OrderByParameter == nil {
				p := *view.QueryStateParameters.OrderByParameter
				p.Description = view.Description(view.OrderByQuery, v.Name)
				if nsPrefix != "" {
					p.In = state.NewQueryLocation(nsPrefix + view.OrderByQuery)
				}
				v.Selector.OrderByParameter = &p
			} else if v.Selector.OrderByParameter.Description == "" {
				v.Selector.OrderByParameter.Description = view.Description(view.OrderByQuery, v.Name)
			}
		}

		// Root view
		nsPrefix := ""
		if c.View.Selector != nil && c.View.Selector.Namespace != "" {
			nsPrefix = c.View.Selector.Namespace
		}
		ensureSelectors(c.View, nsPrefix)

		// All related views via NamespacedView
		if c.NamespacedView != nil {
			for _, nsView := range c.NamespacedView.Views {
				v := nsView.View
				// Determine ns prefix from NamespacedView (prefer non-empty namespace if present)
				pfx := ""
				for _, ns := range nsView.Namespaces {
					if ns != "" {
						pfx = ns
						break
					}
				}
				ensureSelectors(v, pfx)
			}
		}
	}
	holder := ""
	if c.Contract.Output.Type.Parameters != nil {
		if rootHolder := c.Contract.Output.Type.Parameters.LookupByLocation(state.KindOutput, "view"); rootHolder != nil {
			if !rootHolder.IsAnonymous() {
				holder = rootHolder.Name
			}
		}
	}
	c.NamespacedView = view.IndexViews(c.View, holder)
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
	// Delegate to options-based variant for symmetry and centralization.
	return c.UnmarshalFor(WithUnmarshalRequest(request))
}

// UnmarshalOption configures unmarshal behavior for Component.UnmarshalFor.
type UnmarshalOption func(*unmarshalOptions)

type unmarshalOptions struct {
	request      *http.Request
	contentType  string
	interceptors json.UnmarshalerInterceptors
}

// WithUnmarshalRequest supplies an http request for content-type detection and transforms.
func WithUnmarshalRequest(r *http.Request) UnmarshalOption {
	return func(o *unmarshalOptions) { o.request = r }
}

// WithContentType overrides the detected content type.
func WithContentType(ct string) UnmarshalOption {
	return func(o *unmarshalOptions) { o.contentType = ct }
}

// WithUnmarshalInterceptors adds/overrides JSON path interceptors.
func WithUnmarshalInterceptors(m json.UnmarshalerInterceptors) UnmarshalOption {
	return func(o *unmarshalOptions) {
		if o.interceptors == nil {
			o.interceptors = json.UnmarshalerInterceptors{}
		}
		for k, v := range m {
			o.interceptors[k] = v
		}
	}
}

// UnmarshalFor returns a request-scoped unmarshal function applying content-type detection and transforms.
func (c *Component) UnmarshalFor(opts ...UnmarshalOption) shared.Unmarshal {
	options := &unmarshalOptions{}
	for _, opt := range opts {
		if opt != nil {
			opt(options)
		}
	}

	// Resolve content type if request present
	contentType := options.contentType
	if contentType == "" && options.request != nil {
		contentType = options.request.Header.Get(content.HeaderContentType)
		setter.SetStringIfEmpty(&contentType, options.request.Header.Get(strings.ToLower(content.HeaderContentType)))
	}

	switch contentType {
	case content.XMLContentType:
		return c.Content.Marshaller.XML.Unmarshal
	case content.CSVContentType:
		return c.Content.CSV.Unmarshal
	}
	// Fallback to data format preference when no content type or not matched
	if c.Output.DataFormat == content.XMLFormat {
		return c.Content.Marshaller.XML.Unmarshal
	}

	// Build JSON path interceptors from component transforms and any user-provided ones
	interceptors := options.interceptors
	if interceptors == nil {
		interceptors = json.UnmarshalerInterceptors{}
	}
	if options.request != nil {
		for _, transform := range c.UnmarshallerInterceptors() {
			interceptors[transform.Path] = c.transformFn(options.request, transform)
		}
	}

	req := options.request // capture for closure
	return func(data []byte, dest interface{}) error {
		if len(interceptors) > 0 || req != nil {
			return c.Content.Marshaller.JSON.JsonMarshaller.Unmarshal(data, dest, interceptors, req)
		}
		return c.Content.Marshaller.JSON.JsonMarshaller.Unmarshal(data, dest)
	}
}

// MarshalOption configures marshal behavior for Component.MarshalFunc.
type MarshalOption func(*marshalOptions)

type marshalOptions struct {
	request *http.Request
	format  string
	field   string
	filters []*json.FilterEntry
}

// WithRequest supplies an http request for deriving format and state-based exclusions.
func WithRequest(r *http.Request) MarshalOption { return func(o *marshalOptions) { o.request = r } }

// WithFormat overrides the output format (e.g. content.JSONFormat, content.CSVFormat, etc.).
func WithFormat(format string) MarshalOption { return func(o *marshalOptions) { o.format = format } }

// WithField overrides the field used by tabular JSON embedding.
func WithField(field string) MarshalOption { return func(o *marshalOptions) { o.field = field } }

// WithFilters sets explicit JSON field filters (exclusion-based projection).
func WithFilters(filters []*json.FilterEntry) MarshalOption {
	return func(o *marshalOptions) { o.filters = filters }
}

// MarshalFunc returns a request-scoped marshaller closure applying options like format and exclusions.
// If no format is specified, it defaults to JSON for non-reader services and derives from request for readers.
func (c *Component) MarshalFunc(opts ...MarshalOption) shared.Marshal {
	options := &marshalOptions{}
	for _, opt := range opts {
		if opt != nil {
			opt(options)
		}
	}

	// Resolve format
	format := options.format
	if format == "" {
		if options.request != nil && c.Service == service.TypeReader {
			format = c.Output.Format(options.request.URL.Query())
		} else {
			format = content.JSONFormat
		}
	}

	// Resolve field (used for tabular JSON embedding)
	field := options.field
	if field == "" {
		field = c.Output.Field()
	}

	// Resolve filters (explicit only)
	filters := options.filters

	return func(src interface{}) ([]byte, error) {
		return c.Content.Marshal(format, field, src, filters)
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
	if c == nil {
		return nil, false
	}
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
		if err = c.Contract.Input.Type.Init(); err != err {
			return fmt.Errorf("failed to initalize input: %w", err)
		}
		if len(c.Contract.Input.Type.Parameters) > 0 {
			for i := range c.Contract.Input.Type.Parameters {
				resource.AppendParameter(c.Contract.Input.Type.Parameters[i])
			}
		}
		c.Contract.Output.Type = state.Type{Schema: state.NewSchema(outputType)}
		if err = c.Contract.Output.Type.Init(state.WithFS(embedFs)); err != nil {
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

				if aView.Cache != "" {
					aCache := &view.Cache{Reference: shared.Reference{Ref: aView.Cache}}
					viewOptions = append(viewOptions, view.WithCache(aCache))
				}
				if aView.Limit != nil {
					viewOptions = append(viewOptions, view.WithLimit(aView.Limit))
				}

				if aTag.View.PublishParent {
					viewOptions = append(viewOptions, view.WithViewPublishParent(aTag.View.PublishParent))
				}
			}

			if aTag.SQL.SQL != "" {
				anInputType := c.Contract.Input.Type
				viewOptions = append(viewOptions, view.WithSQL(string(aTag.SQL.SQL), anInputType.Parameters...))
			}

			if aTag.View.Connector != "" {
				viewOptions = append(viewOptions, view.WithConnector(&view.Connector{Connection: view.Connection{DBConfig: view.DBConfig{Reference: shared.Reference{Ref: aTag.View.Connector}}}}))
			}
			if aTag.View.Batch != 0 {
				viewOptions = append(viewOptions, view.WithBatchSize(aTag.View.Batch))
			}
			if aTag.View.Limit != nil {
				viewOptions = append(viewOptions, view.WithLimit(aTag.View.Limit))
			}

			if aTag.View.RelationalConcurrency != 0 {
				viewOptions = append(viewOptions, view.WithRelationalConcurrency(aTag.View.RelationalConcurrency))
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
