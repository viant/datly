package tool

import (
	"context"
	"io"
	"net/http"
)

// targetRequest carries compiler-owned route metadata, never an MCP argument.
// The request is local binding state; it is not sent to this synthetic origin.
func (p *Plan) targetRequest(ctx context.Context, body io.Reader) (*http.Request, error) {
	request, err := http.NewRequestWithContext(ctx, p.target.Route.Method, "http://localhost"+p.target.Route.Path, body)
	if err != nil {
		return nil, err
	}
	request.RequestURI = request.URL.RequestURI()
	return request, nil
}
