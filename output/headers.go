package output

import (
	"fmt"
	"maps"
	"net/http"
	"sort"
	"strings"

	"golang.org/x/net/http/httpguts"
)

// compileHeaders validates static ordinary-success metadata. Representation,
// framing and CORS remain owned by their existing HTTP adapters.
func compileHeaders(authored map[string]string) (map[string]string, error) {
	if len(authored) == 0 {
		return nil, nil
	}
	names := make([]string, 0, len(authored))
	for name := range authored {
		names = append(names, name)
	}
	sort.Strings(names)
	result := make(map[string]string, len(authored))
	for _, name := range names {
		value := authored[name]
		if !httpguts.ValidHeaderFieldName(name) || !httpguts.ValidHeaderFieldValue(value) {
			return nil, fmt.Errorf("invalid static response header %q", name)
		}
		key := http.CanonicalHeaderKey(name)
		switch key {
		case "Content-Type", "Content-Length", "Transfer-Encoding", "Content-Encoding", "Content-Disposition", "Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization", "Te", "Trailer", "Upgrade", "Vary":
			return nil, fmt.Errorf("static response header %q is owned by the HTTP adapter", name)
		}
		if strings.HasPrefix(key, "Access-Control-") {
			return nil, fmt.Errorf("static response header %q is owned by CORS", name)
		}
		if _, exists := result[key]; exists {
			return nil, fmt.Errorf("duplicate static response header %q", key)
		}
		result[key] = value
	}
	return result, nil
}

// Headers returns an isolated snapshot; HTTP applies it only after successful
// ordinary output encoding. It is not part of the typed body or MCP result.
func (p *Plan) Headers() map[string]string {
	if p == nil {
		return nil
	}
	return maps.Clone(p.headers)
}
