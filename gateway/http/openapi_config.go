package http

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/viant/datly/gateway/openapi/openapi3"
	"golang.org/x/net/http/httpguts"
)

const DefaultOpenAPIURI = "/v1/api/meta/openapi"
const DefaultDocURI = "/v1/api/meta/doc"

// OpenAPIConfig opts application staging into document generation. Document
// access is independent of the component security described inside documents.
// Nil access policies retain original public document-route behavior.
type OpenAPIConfig struct {
	Info            openapi3.Info   `json:"Info" yaml:"Info"`
	AggregateAccess *DocumentAccess `json:"AggregateAccess,omitempty" yaml:"AggregateAccess,omitempty"`
	RouteAccess     *DocumentAccess `json:"RouteAccess,omitempty" yaml:"RouteAccess,omitempty"`
	// StartupExports is consumed by standalone after initial publication, before
	// listener admission. Rendering remains owned by Manager.ExportOpenAPI.
	StartupExports []DocumentExport `json:"StartupExports,omitempty" yaml:"StartupExports,omitempty"`
}

type DocumentExport struct {
	URL    string
	Path   string
	Format string
}

type DocumentAccess struct {
	APIKeyHeader string `json:"APIKeyHeader" yaml:"APIKeyHeader"`
	APIKeyValue  string `json:"APIKeyValue" yaml:"APIKeyValue"`
}

func (p *DocumentAccess) Validate() error {
	if p == nil {
		return nil
	}
	if !httpguts.ValidHeaderFieldName(p.APIKeyHeader) || p.APIKeyValue == "" || strings.ContainsAny(p.APIKeyValue, "\r\n") {
		return fmt.Errorf("API-key access requires a valid header and nonempty single-line value")
	}
	return nil
}

// Authorize applies the same explicit key policy to documents and configured
// warmup administration. A nil document policy retains public-document behavior.
func (p *DocumentAccess) Authorize(request *http.Request) error {
	if p == nil {
		return nil
	}
	if request == nil || !(APIKey{Value: p.APIKeyValue}).matchesValue(request.Header.Get(p.APIKeyHeader)) {
		return fmt.Errorf("API key denied")
	}
	return nil
}
