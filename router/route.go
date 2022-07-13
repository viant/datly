package router

import (
	"context"
	"fmt"
	"github.com/viant/datly/codec"
	"github.com/viant/datly/router/cache"
	"github.com/viant/datly/router/marshal"
	"github.com/viant/datly/router/marshal/json"
	"github.com/viant/datly/view"
	"github.com/viant/toolbox/format"
	"github.com/viant/xunsafe"
	"net/http"
	"reflect"
	"strings"
)

type Style string
type ServiceType string

const pkgPath = "github.com/viant/datly/router"

const (
	BasicStyle         Style = "Basic"
	ComprehensiveStyle Style = "Comprehensive"

	ReaderServiceType ServiceType = "Reader"
)

type (
	Routes []*Route
	Route  struct {
		Visitor *codec.Visitor
		URI     string
		Method  string
		Service ServiceType
		View    *view.View
		Cors    *Cors
		Output
		Index

		ParamStatusError *int
		Cache            *cache.Cache
		Compression      *Compression

		_resource         *view.Resource
		_requestBodyParam *view.Parameter
	}

	Output struct {
		Cardinality      view.Cardinality `json:",omitempty"`
		CaseFormat       view.CaseFormat  `json:",omitempty"`
		OmitEmpty        bool             `json:",omitempty"`
		Style            Style            `json:",omitempty"`
		ResponseField    string           `json:",omitempty"`
		Transforms       marshal.Transforms
		Exclude          []string
		NormalizeExclude *bool
		_caser           *format.Case
		_excluded        map[string]bool
		_marshaller      *json.Marshaller
		_responseSetter  *responseSetter
	}

	responseSetter struct {
		statusField *xunsafe.Field
		bodyField   *xunsafe.Field
		rType       reflect.Type
	}

	ResponseStatus struct {
		Status  string      `json:",omitempty"`
		Message interface{} `json:",omitempty"`
	}
)

func (r *Route) Init(ctx context.Context, resource *Resource) error {
	if r.Style == BasicStyle {
		r.ResponseField = ""
	}

	if err := r.initCardinality(); err != nil {
		return err
	}
	r.View.Standalone = true
	if r.View.Name == "" {
		r.View.Name = r.View.Ref
	}
	if err := r.View.Init(ctx, resource.Resource); err != nil {
		return err
	}
	if err := r.initVisitor(resource); err != nil {
		return err
	}

	if err := r.initStyle(); err != nil {
		return err
	}

	if err := r.Index.Init(r.View, r.ResponseField); err != nil {
		return err
	}

	if err := r.initCaser(); err != nil {
		return err
	}

	if err := r.normalizePaths(); err != nil {
		return err
	}

	if err := r.initMarshaller(); err != nil {
		return err
	}

	if err := r.initServiceType(); err != nil {
		return err
	}

	if err := r.initRequestBody(); err != nil {
		return err
	}

	if err := r.initCache(ctx); err != nil {
		return err
	}

	r.initCors(resource)
	r.initCompression(resource)
	r.indexExcluded()

	return nil
}

func (r *Route) initVisitor(resource *Resource) error {
	if r.Visitor == nil {
		r.Visitor = &codec.Visitor{}
		return nil
	}

	if r.Visitor.Reference.Ref != "" {
		refVisitor, err := resource._visitors.Lookup(r.Visitor.Ref)
		if err != nil {
			return err
		}

		r.Visitor.Inherit(refVisitor)
	}

	return nil
}

func (r *Route) initCardinality() error {
	switch r.Cardinality {
	case view.One, view.Many:
		return nil
	case "":
		r.Cardinality = view.Many
		return nil
	default:
		return fmt.Errorf("unsupported cardinality type %v\n", r.Cardinality)
	}
}

func (r *Route) initMarshaller() error {
	marshaller, err := json.New(r.responseType(), marshal.Default{
		OmitEmpty:  r.OmitEmpty,
		CaseFormat: *r._caser,
		Transforms: r.Transforms.Index(),
		Exclude:    marshal.Exclude(r.Exclude).Index(),
	})

	if err != nil {
		return err
	}

	r._marshaller = marshaller
	return nil
}

func (r *Route) initCors(resource *Resource) {
	if r.Cors == nil {
		r.Cors = resource.Cors
		return
	}

	r.Cors.inherit(resource.Cors)
}

func (r *Route) initStyle() error {
	if r.Style == "" || r.Style == BasicStyle {
		r.Style = BasicStyle
		return nil
	}

	if r.Style == ComprehensiveStyle {
		if r.ResponseField == "" {
			r.ResponseField = "ResponseBody"
		}

		responseFields := make([]reflect.StructField, 2)
		responseFields[0] = reflect.StructField{
			Name:      "ResponseStatus",
			Type:      reflect.TypeOf(ResponseStatus{}),
			Anonymous: true,
		}

		responseFields[1] = reflect.StructField{
			Name:    r.ResponseField,
			PkgPath: pkgPath,
			Type:    r.cardinalityType(),
		}

		responseType := reflect.StructOf(responseFields)
		r._responseSetter = &responseSetter{
			statusField: xunsafe.FieldByName(responseType, "ResponseStatus"),
			bodyField:   xunsafe.FieldByName(responseType, r.ResponseField),
			rType:       responseType,
		}

		return nil
	}

	return fmt.Errorf("unsupported style %v", r.Style)
}

func (r *Route) cardinalityType() reflect.Type {
	if r.Cardinality == view.Many {
		return r.View.Schema.SliceType()
	}

	return r.View.Schema.Type()
}

func (r *Route) responseType() reflect.Type {
	if r._responseSetter != nil {
		return r._responseSetter.rType
	}

	return r.View.Schema.Type()
}

func (r *Route) initServiceType() error {
	switch r.Service {
	case "", ReaderServiceType:
		r.Service = ReaderServiceType
		return nil
	}

	switch r.Method {
	case http.MethodGet:
		r.Service = ReaderServiceType
		return nil
	default:
		return fmt.Errorf("http method %v unsupported", r.Method)
	}
}

func (r *Route) initRequestBody() error {
	if r.Method == http.MethodGet {
		return nil
	}

	return r.initRequestBodyFromParams()
}

func (r *Route) initRequestBodyFromParams() error {
	params := make([]*view.Parameter, 0)
	r.findRequestBodyParams(r.View, &params)

	if len(params) == 0 {
		return nil
	}

	rType := params[0].Schema.Type()
	for i := 1; i < len(params); i++ {
		if params[i].Schema.Type() != rType {
			return fmt.Errorf("parameters request body type missmatch: wanted %v got %v", rType.String(), params[i].Schema.Type().String())
		}
	}

	r._requestBodyParam = params[0]
	return nil
}

func (r *Route) findRequestBodyParams(aView *view.View, params *[]*view.Parameter) {
	for i, parameter := range aView.Template.Parameters {
		if parameter.In.Kind == view.RequestBodyKind {
			*params = append(*params, aView.Template.Parameters[i])
		}

		if parameter.View() != nil {
			r.findRequestBodyParams(parameter.View(), params)
		}
	}

	for _, relation := range aView.With {
		r.findRequestBodyParams(&relation.Of.View, params)
	}
}

func (r *Route) initCache(ctx context.Context) error {
	if r.Cache == nil {
		return nil
	}

	return r.Cache.Init(ctx)
}

func (r *Route) initCompression(resource *Resource) {
	if r.Compression != nil {
		return
	}

	r.Compression = resource.Compression
}

func (i *Index) ViewByPrefix(prefix string) (*view.View, error) {
	aView, ok := i.viewByPrefix(prefix)
	if !ok {
		return nil, fmt.Errorf("not found view with prefix %v", prefix)
	}

	return aView, nil
}

func (r *Route) ShouldNormalizeExclude() bool {
	return r.NormalizeExclude == nil || *r.NormalizeExclude
}

func (r *Route) normalizePaths() error {
	if !r.ShouldNormalizeExclude() {
		return nil
	}

	if err := r.initCaser(); err != nil {
		return err
	}

	aBool := false
	r.NormalizeExclude = &aBool

	for i, excluded := range r.Exclude {
		lastDot := strings.LastIndex(excluded, ".")
		if lastDot == -1 {
			r.Exclude[i] = r._caser.Format(excluded, format.CaseUpperCamel)
		} else {
			r.Exclude[i] = excluded[:lastDot+1] + r._caser.Format(excluded[lastDot+1:], format.CaseUpperCamel)
		}
	}

	for i, transform := range r.Transforms {
		path := transform.Path
		lastDot := strings.LastIndex(path, ".")
		if lastDot == -1 {
			r.Transforms[i].Path = r._caser.Format(path, format.CaseUpperCamel)
		} else {
			r.Transforms[i].Path = path[:lastDot+1] + r._caser.Format(path[lastDot+1:], format.CaseUpperCamel)
		}
	}

	return nil
}

func (r *Route) PrefixByView(aView *view.View) (string, bool) {
	return r.Index.prefixByView(aView)
}

func (r *Route) indexExcluded() {
	r._excluded = map[string]bool{}
	for _, excluded := range r.Exclude {
		r._excluded[excluded] = true
	}
}

func (r *Route) initCaser() error {
	if r._caser != nil {
		return nil
	}

	if r.CaseFormat == "" {
		r.CaseFormat = view.UpperCamel
	}

	var err error
	caser, err := r.CaseFormat.Caser()
	if err != nil {
		return err
	}

	r._caser = &caser

	return nil
}
