package http

import (
	"context"
	"fmt"
	"mime"
	stdhttp "net/http"
	"reflect"

	"github.com/viant/bindly/locator"
	requestprovider "github.com/viant/bindly/provider/request"
	"github.com/viant/structology"
)

// HTTP form parameters retain v0's merged body/query semantics. Bindly's
// independent query and form sources remain unchanged for other protocols.
type httpRequestScope struct {
	*requestprovider.Scope
	mergeQuery bool
	formQuery  locator.Provider
}

func newHTTPRequestScope(req *stdhttp.Request, path map[string]string, onDemand ...bool) (*httpRequestScope, error) {
	mediaType, _, _ := mime.ParseMediaType(req.Header.Get("Content-Type"))
	options := []requestprovider.Option{requestprovider.WithPathParams(path)}
	// Register only for XLSX: a decoder registry also selects a different JSON
	// decoding path in Bindly. Preserve ordinary JSON binding and presence.
	if mediaType == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" {
		options = append(options, requestprovider.WithDecoder(mediaType, requestprovider.DecoderFunc(func(raw []byte, target any) error {
			return unmarshalXLSBody(req.Context(), raw, target)
		})))
	}
	var scope *requestprovider.Scope
	var err error
	if len(onDemand) > 0 && onDemand[0] {
		scope, err = requestprovider.NewDeferred(req, options...)
	} else {
		scope, err = requestprovider.New(req, options...)
	}
	if err != nil {
		return nil, err
	}
	// Form lookup preserves explicit empty values independently of query-only
	// selector policies, matching request.Form rather than request.Query rules.
	query := requestprovider.NewValues(requestprovider.WithForm(req.URL.Query())).Form()
	return &httpRequestScope{Scope: scope, mergeQuery: mediaType != "multipart/form-data", formQuery: query}, nil
}

func (s *httpRequestScope) Providers() []locator.Provider {
	providers := s.Scope.Providers()
	if !s.mergeQuery {
		return providers
	}
	for i, provider := range providers {
		if provider.Kind() == requestprovider.FormKind {
			providers[i] = &mergedFormProvider{Provider: provider, query: s.formQuery}
		}
	}
	return providers
}

type mergedFormProvider struct {
	locator.Provider
	query locator.Provider
}

func (p *mergedFormProvider) Locate(state *structology.State) locator.Locator {
	return &mergedFormLocator{body: p.Provider.Locate(state), query: p.query.Locate(state)}
}

type mergedFormLocator struct{ body, query locator.Locator }

func (*mergedFormLocator) Kind() string { return requestprovider.FormKind }

func (l *mergedFormLocator) Value(ctx context.Context, target reflect.Type, name string) (any, bool, error) {
	valuesType := reflect.TypeFor[[]string]()
	body, bodyFound, err := l.body.Value(ctx, valuesType, name)
	if err != nil {
		return nil, false, err
	}
	query, queryFound, err := l.query.Value(ctx, valuesType, name)
	if err != nil {
		return nil, false, err
	}
	if !bodyFound && !queryFound {
		return nil, false, nil
	}
	var values []string
	if bodyFound {
		values = append(values, body.([]string)...)
	}
	if queryFound {
		values = append(values, query.([]string)...)
	}
	for target != nil && target.Kind() == reflect.Pointer {
		target = target.Elem()
	}
	collection := target != nil && (target.Kind() == reflect.Slice || target.Kind() == reflect.Array) && target != reflect.TypeFor[[]byte]()
	if len(values) == 1 && !collection {
		return values[0], true, nil
	}
	return values, true, nil
}

// unmarshalXLSBody retains the legacy context-aware body contract. Bindly
// supplies a pointer to the declared body type, including **T for *T bodies.
func unmarshalXLSBody(ctx context.Context, raw []byte, target any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	value := reflect.ValueOf(target)
	for value.IsValid() && value.Kind() == reflect.Pointer {
		if value.IsNil() {
			if !value.CanSet() {
				break
			}
			value.Set(reflect.New(value.Type().Elem()))
		}
		if decoder, ok := value.Interface().(interface {
			UnmarshalXLS(context.Context, []byte) error
		}); ok {
			return decoder.UnmarshalXLS(ctx, raw)
		}
		value = value.Elem()
	}
	return fmt.Errorf("XLSX body type %T does not implement UnmarshalXLS", target)
}
