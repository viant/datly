package compile

import (
	"context"
	"net/http"
	"strings"

	"github.com/viant/datly/repository/contract/signature"
	"github.com/viant/datly/repository/shape/plan"
)

// ComponentContract represents resolved component contract metadata.
type ComponentContract struct {
	RouteKey   string
	Method     string
	URI        string
	OutputType string
	Types      []*plan.Type
}

// ComponentResolver resolves component contract metadata for a route key.
type ComponentResolver interface {
	ResolveContract(ctx context.Context, routeKey string) (*ComponentContract, error)
}

// SignatureResolver adapts repository/contract/signature service
// to compile-time component contract resolution.
type SignatureResolver struct {
	service *signature.Service
}

// NewSignatureResolver creates signature-backed component resolver.
func NewSignatureResolver(ctx context.Context, apiPrefix, routesURL string) (*SignatureResolver, error) {
	srv, err := signature.New(ctx, apiPrefix, routesURL)
	if err != nil {
		return nil, err
	}
	return &SignatureResolver{service: srv}, nil
}

// ResolveContract resolves component contract by route key.
func (s *SignatureResolver) ResolveContract(_ context.Context, routeKey string) (*ComponentContract, error) {
	method, uri := splitRouteKey(routeKey)
	sig, err := s.service.Signature(method, uri)
	if err != nil {
		return nil, err
	}
	ret := &ComponentContract{
		RouteKey: normalizeRouteKey(method, uri),
		Method:   method,
		URI:      normalizeURI(uri),
	}
	if sig.Output != nil {
		if dataType := strings.TrimSpace(sig.Output.DataType); dataType != "" {
			ret.OutputType = dataType
		} else if name := strings.TrimSpace(sig.Output.Name); name != "" {
			name = strings.Trim(name, "*")
			if name != "" {
				ret.OutputType = "*" + name
			}
		}
	}
	for _, item := range sig.Types {
		if item == nil {
			continue
		}
		ret.Types = append(ret.Types, &plan.Type{
			Name:        strings.TrimSpace(item.Name),
			Alias:       strings.TrimSpace(item.Alias),
			DataType:    strings.TrimSpace(item.DataType),
			Cardinality: strings.TrimSpace(string(item.Cardinality)),
			Package:     strings.TrimSpace(item.Package),
			ModulePath:  strings.TrimSpace(item.ModulePath),
		})
	}
	return ret, nil
}

func splitRouteKey(routeKey string) (string, string) {
	routeKey = strings.TrimSpace(routeKey)
	if routeKey == "" {
		return http.MethodGet, "/"
	}
	if idx := strings.Index(routeKey, ":"); idx != -1 {
		method := strings.ToUpper(strings.TrimSpace(routeKey[:idx]))
		uri := strings.TrimSpace(routeKey[idx+1:])
		if method == "" {
			method = http.MethodGet
		}
		if uri == "" {
			uri = "/"
		}
		return method, uri
	}
	return http.MethodGet, routeKey
}
