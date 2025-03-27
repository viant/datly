package router

import (
	"context"
	"fmt"
	"github.com/francoispqt/gojay"
	"github.com/viant/afs"
	"github.com/viant/datly/gateway/router/marshal"
	"github.com/viant/datly/gateway/router/marshal/json"
	"github.com/viant/datly/internal/setter"
	"github.com/viant/datly/repository"
	"github.com/viant/datly/repository/content"
	"github.com/viant/datly/repository/path"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/utils/formatter"
	"github.com/viant/datly/utils/httputils"
	"github.com/viant/datly/view"
	http2 "github.com/viant/xdatly/handler/http"
	"net/http"
	"reflect"
	"strings"
)

const pkgPath = "github.com/viant/datly/gateway/router"

const (
	HeaderContentType = "Content-Type"
)

type (

	//deprecated
	Routes []*Route
	//deprecated
	Route struct {
		APIKey      *path.APIKey      `json:",omitempty"`
		Cors        *path.Cors        `json:",omitempty"`
		Internal    bool              `json:"Internal,omitempty" yaml:"Internal,omitempty" `
		Connector   string            `json:",omitempty"`
		ContentURL  string            `json:"ContentURL,omitempty" yaml:"ContentURL,omitempty" `
		Compression *path.Compression `json:",omitempty"`

		Transforms marshal.Transforms `json:",omitempty"`

		repository.Component

		_unmarshallerInterceptors marshal.Transforms

		_resource     *view.Resource
		_apiKeys      []*path.APIKey
		_routeMatcher func(route *http2.Route) (*Route, error)
		_router       *Handler
	}
)

func (r *Route) IsMetricsEnabled(req *http.Request) bool {
	return r.IsMetricInfo(req) || r.IsMetricDebug(req)
}

func (r *Route) IsMetricInfo(req *http.Request) bool {
	if !r.Output.IsRevealMetric() {
		return false
	}
	value := req.Header.Get(httputils.DatlyRequestMetricsHeader)
	if value == "" {
		value = req.Header.Get(strings.ToLower(httputils.DatlyRequestMetricsHeader))
	}
	return strings.ToLower(value) == httputils.DatlyInfoHeaderValue
}

func (r *Route) HttpURI() string {
	return r.URI
}

// TODO move/merge with content.UnmarshalFunc
// possible remove marshaller interceptors all together
func (r *Route) UnmarshalFunc(request *http.Request) shared.Unmarshal {
	contentType := request.Header.Get(HeaderContentType)
	setter.SetStringIfEmpty(&contentType, request.Header.Get(strings.ToLower(HeaderContentType)))
	switch contentType {
	case content.XMLContentType:
		return r.Marshaller.XML.Unmarshal
	case content.CSVContentType:
		return r.CSV.Unmarshal
	default:
	}
	jsonPathInterceptor := json.UnmarshalerInterceptors{}
	for i := range r._unmarshallerInterceptors {
		transform := r._unmarshallerInterceptors[i]
		jsonPathInterceptor[transform.Path] = r.transformFn(request, transform)
	}

	return func(bytes []byte, i interface{}) error {
		return r.Marshaller.JSON.JsonMarshaller.Unmarshal(bytes, i, jsonPathInterceptor, request)
	}
}

func (r *Route) CorsEnabled() bool {
	return r.Cors != nil
}

func (r *Route) Init(ctx context.Context, resource *Resource) error {
	if err := r.Component.Init(ctx, resource.Resource); err != nil {
		return err
	}
	r.initCompression(resource)

	if err := r.normalizePaths(); err != nil {
		return err
	}
	if err := r.initTransforms(ctx); err != nil {
		return nil
	}
	r._unmarshallerInterceptors = r.Transforms.FilterByKind(marshal.TransformKindUnmarshal)
	if err := r.Component.Content.InitMarshaller(r.Component.IOConfig(), r.Output.Exclude, r.BodyType(), r.OutputType()); err != nil {
		return err
	}
	if r.APIKey != nil {
		r._apiKeys = append(r._apiKeys, r.APIKey)
	}
	return nil
}

func (r *Route) IsCacheDisabled(req *http.Request) bool {
	return (req.Header.Get(httputils.DatlyRequestDisableCacheHeader) != "" || req.Header.Get(strings.ToLower(httputils.DatlyRequestDisableCacheHeader)) != "")
}

func (r *Route) IsMetricDebug(req *http.Request) bool {
	if !r.Output.IsRevealMetric() {
		return false
	}
	value := req.Header.Get(httputils.DatlyRequestMetricsHeader)
	if value == "" {
		value = req.Header.Get(strings.ToLower(httputils.DatlyRequestMetricsHeader))
	}
	return strings.ToLower(value) == httputils.DatlyDebugHeaderValue
}

func (r *Route) PgkPath(fieldName string) string {
	var responseFieldPgkPath string
	if fieldName[0] < 'A' || fieldName[0] > 'Z' {
		responseFieldPgkPath = pkgPath
	}
	return responseFieldPgkPath
}

func (r *Route) initCompression(resource *Resource) {
	if r.Compression != nil {
		return
	}

	r.Compression = resource.Compression
}

func (r *Route) normalizePaths() error {
	if !r.Output.ShouldNormalizeExclude() {
		return nil
	}
	for i, transform := range r.Transforms {
		r.Transforms[i].Path = formatter.NormalizePath(transform.Path)
	}
	return nil
}

func (r *Route) AddApiKeys(keys ...*path.APIKey) {
	r._apiKeys = append(r._apiKeys, keys...)
}

func (r *Route) transformFn(request *http.Request, transform *marshal.Transform) func(dst interface{}, decoder *gojay.Decoder, options ...interface{}) error {
	unmarshaller := transform.UnmarshalerInto()
	if unmarshaller != nil {
		return unmarshaller.UnmarshalJSONWithOptions
	}

	return func(dst interface{}, decoder *gojay.Decoder, options ...interface{}) error {
		evaluate, err := transform.Evaluate(request, decoder, r._resource.LookupType())
		if err != nil {
			return err
		}
		if evaluate.Ctx.Decoder.Decoded != nil {
			reflect.ValueOf(dst).Elem().Set(reflect.ValueOf(evaluate.Ctx.Decoder.Decoded))
		}
		return nil
	}
}

func (r *Route) initTransforms(ctx context.Context) error {
	for _, transform := range r.Transforms {
		if err := transform.Init(ctx, afs.New(), r._resource.LookupType()); err != nil {
			return err
		}
	}

	return nil
}

func (r *Route) match(ctx context.Context, route *http2.Route) (*Route, error) {
	if r._routeMatcher == nil {
		return nil, fmt.Errorf("route matcher was empty")
	}

	return r._routeMatcher(route)
}

func (r *Route) SetRouteLookup(lookup func(route *http2.Route) (*Route, error)) {
	r._routeMatcher = lookup
}
