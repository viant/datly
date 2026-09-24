package remote

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	xhttp "github.com/viant/xdatly/client/http"
	xmcp "github.com/viant/xdatly/client/mcp"
)

const (
	// TransportHTTP invokes one HTTP endpoint through an injected xhttp.Provider.
	TransportHTTP = "http"
	// TransportMCP calls one MCP tool through an injected xmcp.Provider.
	TransportMCP = "mcp"

	// SourceJSON decodes an HTTP response body as one JSON document.
	SourceJSON = "json"
	// SourceStructuredContent uses CallToolResult.StructuredContent as the document.
	SourceStructuredContent = "structuredContent"
	// SourceText decodes the concatenated MCP text content as a JSON document.
	SourceText = "text"

	// ValidityUnix reads an absolute expiry as seconds since the Unix epoch.
	ValidityUnix = "unix"
	// ValidityRFC3339 reads an absolute expiry as an RFC 3339 string.
	ValidityRFC3339 = "rfc3339"
	// ValiditySeconds reads a relative lifetime in seconds.
	ValiditySeconds = "seconds"
)

// ClientOptions selects exactly one configured protocol. Transport names the
// protocol; the matching section carries the public xdatly options that are
// handed, unchanged, to the injected provider of that protocol. It is
// deployment metadata bound with kind=const, never a caller value.
type ClientOptions struct {
	Transport string         `json:"transport"`
	HTTP      *xhttp.Options `json:"http,omitempty"`
	MCP       *xmcp.Options  `json:"mcp,omitempty"`
}

// Validate checks the structural declaration the handler depends on for its
// request mapping: one protocol section, an absolute endpoint, a supported
// method or a tool name, unique canonical header names and well-formed limits.
// Zero or omitted limits mean unlimited.
func (o *ClientOptions) Validate() error {
	if o == nil {
		return fmt.Errorf("client options are required")
	}
	switch o.Transport {
	case TransportHTTP:
		if o.MCP != nil {
			return fmt.Errorf("transport %q cannot declare an mcp section", o.Transport)
		}
		if o.HTTP == nil {
			return fmt.Errorf("http section is required for transport %q", TransportHTTP)
		}
		if err := validateEndpointURL(o.HTTP.URL); err != nil {
			return fmt.Errorf("http url: %w", err)
		}
		switch o.HTTP.Method {
		case http.MethodGet, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		default:
			return fmt.Errorf("http method %q must be one of GET, POST, PUT, PATCH, DELETE", o.HTTP.Method)
		}
		if _, err := canonicalHeaders(o.HTTP.Header); err != nil {
			return fmt.Errorf("http header: %w", err)
		}
		if err := validateLimits(o.HTTP.Timeout, o.HTTP.MaxResponseBytes); err != nil {
			return fmt.Errorf("http %w", err)
		}
	case TransportMCP:
		if o.HTTP != nil {
			return fmt.Errorf("transport %q cannot declare an http section", o.Transport)
		}
		if o.MCP == nil {
			return fmt.Errorf("mcp section is required for transport %q", TransportMCP)
		}
		if err := validateEndpointURL(o.MCP.URL); err != nil {
			return fmt.Errorf("mcp url: %w", err)
		}
		if strings.TrimSpace(o.MCP.Tool) == "" {
			return fmt.Errorf("mcp tool is required")
		}
		if _, err := canonicalHeaders(o.MCP.Header); err != nil {
			return fmt.Errorf("mcp header: %w", err)
		}
		if o.MCP.MaxSessions < 0 {
			return fmt.Errorf("mcp maxSessions must be positive")
		}
		if err := validateLimits(o.MCP.Timeout, o.MCP.MaxResponseBytes); err != nil {
			return fmt.Errorf("mcp %w", err)
		}
	default:
		return fmt.Errorf("transport must be %q or %q, got %q", TransportHTTP, TransportMCP, o.Transport)
	}
	return nil
}

func validateLimits(timeout string, maxBytes int64) error {
	if timeout != "" {
		parsed, err := time.ParseDuration(timeout)
		if err != nil {
			return fmt.Errorf("timeout: %w", err)
		}
		if parsed < 0 {
			return fmt.Errorf("timeout %s must not be negative", timeout)
		}
	}
	if maxBytes < 0 {
		return fmt.Errorf("maxResponseBytes %d must be positive", maxBytes)
	}
	return nil
}

func validateEndpointURL(raw string) error {
	if strings.TrimSpace(raw) == "" {
		return fmt.Errorf("url is required")
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return err
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fmt.Errorf("url %q must use http or https", raw)
	}
	if parsed.Host == "" {
		return fmt.Errorf("url %q requires a host", raw)
	}
	if parsed.User != nil {
		return fmt.Errorf("url %q must not embed credentials", raw)
	}
	return nil
}

func canonicalHeaders(source map[string]string) (map[string]string, error) {
	result := make(map[string]string, len(source))
	for name, value := range source {
		canonical := http.CanonicalHeaderKey(strings.TrimSpace(name))
		if canonical == "" {
			return nil, fmt.Errorf("header name is required")
		}
		if _, ok := result[canonical]; ok {
			return nil, fmt.Errorf("header %q is declared more than once", canonical)
		}
		result[canonical] = value
	}
	return result, nil
}

// Config is the complete handler declaration: the selected client options
// plus the explicit request/response mappings and cache policy. It is
// authored as component metadata and bound as one typed constant.
type Config struct {
	Client ClientOptions `json:"client"`
	// Request maps component input paths onto the outbound call.
	Request []RequestMapping `json:"request,omitempty"`
	// Response maps response document paths onto the declared output.
	Response Response `json:"response"`
	// Credential attaches one server-resolved secret to the call.
	Credential *Credential `json:"credential,omitempty"`
	// Cache enables explicit result caching; absent means no caching.
	Cache *Cache `json:"cache,omitempty"`
}

// DecodeConfig strictly decodes one configuration object. Applications that
// load declarations outside Datly's typed const binder can use this utility.
func DecodeConfig(data []byte) (*Config, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var raw json.RawMessage
	if err := decoder.Decode(&raw); err != nil {
		return nil, fmt.Errorf("decode remote configuration: %w", err)
	}
	if len(raw) == 0 || raw[0] != '{' {
		return nil, fmt.Errorf("remote configuration must be an object")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("remote configuration must contain one object")
	}
	strict := json.NewDecoder(bytes.NewReader(raw))
	strict.DisallowUnknownFields()
	var config Config
	if err := strict.Decode(&config); err != nil {
		return nil, fmt.Errorf("decode remote configuration: %w", err)
	}
	return &config, nil
}

// RequestMapping copies one input value, unchanged, onto exactly one request
// target. Input is a compiled transform selector path over the component input struct
// (for example Token or Scope.Tenant). Header forwards the value as the named
// outgoing header (the conventional pairing is an input bound from
// header/Authorization forwarded as Authorization). Body is a JSON Pointer
// inside the JSON object body; "" selects the whole body. Argument names one
// MCP tool argument.
type RequestMapping struct {
	Input    string  `json:"input"`
	Header   string  `json:"header,omitempty"`
	Query    string  `json:"query,omitempty"`
	Path     string  `json:"path,omitempty"`
	Body     *string `json:"body,omitempty"`
	Argument string  `json:"argument,omitempty"`
}

// Response declares how the remote document populates the output contract.
type Response struct {
	// Source selects the document: json (HTTP), structuredContent or text (MCP).
	Source   string            `json:"source,omitempty"`
	Mappings []ResponseMapping `json:"mappings"`
	// Validity reads an explicit expiry from the document and caps cache TTL.
	Validity *Validity `json:"validity,omitempty"`
}

// ResponseMapping copies one JSON Pointer location of the response document
// into one output selector path. A missing or null location fails unless the
// mapping is optional.
type ResponseMapping struct {
	Path     string `json:"path"`
	Output   string `json:"output"`
	Optional bool   `json:"optional,omitempty"`
}

// Validity locates an explicit expiry inside the response document.
type Validity struct {
	Path   string `json:"path"`
	Format string `json:"format"`
}

// Credential attaches one server-resolved secret read from the configured
// resource store. Its content is forwarded unchanged (include any scheme in
// the stored value) as the named header, or as an MCP tool argument when the
// remote API is explicitly argument-based. Caller inputs are forwarded through
// RequestMapping instead, never through Credential.
type Credential struct {
	Resource string `json:"resource"`
	Header   string `json:"header,omitempty"`
	Argument string `json:"argument,omitempty"`
}

// Cache opts into an explicitly registered named backend. Backend capacity is
// configured when the cache is registered, not per remote declaration. Keys
// include the configuration and mapped request values; Partition adds
// server-owned values such as a verified principal or tenant.
type Cache struct {
	Name      string   `json:"name"`
	TTL       string   `json:"ttl"`
	Partition []string `json:"partition,omitempty"`
}
