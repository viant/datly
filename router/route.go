package router

import (
	"context"
	"fmt"
	"github.com/viant/datly/data"
	"github.com/viant/datly/router/marshal"
	"github.com/viant/datly/router/marshal/json"
	"github.com/viant/datly/visitor"
	"github.com/viant/xunsafe"
	"reflect"
)

type Style string

const pkgPath = "github.com/viant/datly/router"

const (
	BasicStyle         Style = "Basic"
	ComprehensiveStyle Style = "Comprehensive"
)

type (
	Routes []*Route
	Route  struct {
		Visitor *visitor.Visitor
		URI     string
		Method  string
		View    *data.View
		Cors    *Cors
		Output

		Index Index

		_resource *data.Resource
	}

	Output struct {
		Cardinality data.Cardinality
		CaseFormat  data.CaseFormat
		OmitEmpty   bool

		_marshaller     *json.Marshaller
		Style           Style //enum Basic, Comprehensice , Status: ok, error, + error with structre
		ResponseField   string
		_responseSetter *responseSetter
	}

	responseSetter struct {
		statusField *xunsafe.Field
		bodyField   *xunsafe.Field
		rType       reflect.Type
	}

	ResponseStatus struct {
		Status  string `json:",omitempty"`
		Message string `json:",omitempty"`
	}
)

func (r *Route) Init(ctx context.Context, resource *Resource) error {
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

	if err := r.initMarshaller(); err != nil {
		return err
	}

	r.initCors(resource)

	return nil
}

func (r *Route) initVisitor(resource *Resource) error {
	if r.Visitor == nil {
		r.Visitor = &visitor.Visitor{}
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
	case data.One, data.Many:
		return nil
	case "":
		r.Cardinality = data.Many
		return nil
	default:
		return fmt.Errorf("unsupported cardinality type %v\n", r.Cardinality)
	}
}

func (r *Route) initMarshaller() error {
	if r.CaseFormat == "" {
		r.CaseFormat = data.UpperCamel
	}

	caser, err := r.CaseFormat.Caser()
	if err != nil {
		return err
	}

	marshaller, err := json.New(r.responseType(), marshal.Default{
		OmitEmpty:  r.OmitEmpty,
		CaseFormat: caser,
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
	if r.Cardinality == data.Many {
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

func (i *Index) ViewByPrefix(prefix string) (*data.View, error) {
	view, ok := i._viewsByPrefix[prefix]
	if !ok {
		return nil, fmt.Errorf("not found view with prefix %v", prefix)
	}

	return view, nil
}
