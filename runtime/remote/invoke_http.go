package remote

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"

	xhttp "github.com/viant/xdatly/client/http"
)

// invokeHTTP builds the explicit request from the mapped materials, executes
// it through the borrowed client and decodes the JSON document. Static
// headers, timeout and byte bounds are the client's; status and document
// extraction are the handler's.
func invokeHTTP(ctx context.Context, client xhttp.Client, p *plan, m *materials) (any, error) {
	endpoint, err := p.endpoint(m)
	if err != nil {
		return nil, err
	}
	var body io.Reader
	if m.hasBody {
		encoded, err := json.Marshal(m.body)
		if err != nil {
			return nil, fmt.Errorf("encode request body: %w", err)
		}
		body = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(ctx, p.method, endpoint, body)
	if err != nil {
		return nil, fmt.Errorf("build remote request: %w", err)
	}
	request.Header.Set("Accept", "application/json")
	if m.hasBody {
		request.Header.Set("Content-Type", "application/json")
	}
	header, _ := p.outbound(m)
	for name, values := range header {
		request.Header.Del(name)
		for _, value := range values {
			request.Header.Add(name, value)
		}
	}
	response, err := client.Do(request)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil && !errors.Is(err, ctxErr) {
			return nil, fmt.Errorf("remote %s %s: %w", p.method, p.url, ctxErr)
		}
		return nil, err
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, fmt.Errorf("remote %s %s: %w", p.method, p.url, err)
	}
	if response.StatusCode < 200 || response.StatusCode > 299 {
		return nil, &StatusError{Method: p.method, URL: p.url, StatusCode: response.StatusCode}
	}
	return decodeJSONDocument(data)
}

// endpoint fills URL placeholders from path mappings and appends mapped query
// values to any static query of the configured URL.
func (p *plan) endpoint(m *materials) (string, error) {
	var missing string
	raw := placeholderPattern.ReplaceAllStringFunc(p.url, func(match string) string {
		name := match[1 : len(match)-1]
		value, ok := m.path[name]
		if !ok && missing == "" {
			missing = name
		}
		return url.PathEscape(value)
	})
	if missing != "" {
		return "", fmt.Errorf("url placeholder {%s} has no value", missing)
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("remote url: %w", err)
	}
	if len(m.query) > 0 {
		query := parsed.Query()
		for name, values := range m.query {
			for _, value := range values {
				query.Add(name, value)
			}
		}
		parsed.RawQuery = query.Encode()
	}
	return parsed.String(), nil
}

// StatusError reports a non-2xx remote HTTP response without echoing its body.
type StatusError struct {
	Method     string
	URL        string
	StatusCode int
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("remote %s %s returned status %d", e.Method, e.URL, e.StatusCode)
}

func decodeJSONDocument(data []byte) (any, error) {
	if len(bytes.TrimSpace(data)) == 0 {
		return nil, fmt.Errorf("response body is empty")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var document any
	if err := decoder.Decode(&document); err != nil {
		return nil, fmt.Errorf("decode JSON response: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("decode JSON response: unexpected trailing content")
	}
	return document, nil
}
