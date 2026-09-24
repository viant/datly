// Package proxy provides an ordinary custom handler for explicitly configured
// HTTP forwarding. Native input binding and runtime lifecycle rules apply.
package proxy

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	xhttp "github.com/viant/xdatly/client/http"
	xhandler "github.com/viant/xdatly/handler"
	"github.com/viant/xdatly/response"
)

type Header struct{ From, To string }

// Config is deployment-owned forwarding policy. Nothing from the incoming
// request selects the destination or is forwarded without these options.
type Config struct {
	Client       xhttp.Options `json:"client"`
	Headers      []Header      `json:"headers,omitempty"`
	ForwardQuery bool          `json:"forwardQuery,omitempty"`
	ForwardBody  bool          `json:"forwardBody,omitempty"`
}

type Input struct {
	Config  *Config        `parameter:"Config,kind=const,in=Proxy,required"`
	HTTP    xhttp.Provider `bind:"kind=http_client,required"`
	Request *http.Request  `parameter:"Request,kind=http_request,required"`
}

// Init is the same input initialization hook available to application handlers.
func (i *Input) Init(context.Context) error {
	if i.Config == nil || i.HTTP == nil || i.Request == nil {
		return fmt.Errorf("proxy configuration, HTTP provider and request are required")
	}
	endpoint, err := url.Parse(i.Config.Client.URL)
	if err != nil || endpoint.Host == "" || (endpoint.Scheme != "http" && endpoint.Scheme != "https") || endpoint.User != nil {
		return fmt.Errorf("proxy requires an absolute HTTP endpoint without embedded credentials")
	}
	if i.Config.Client.Method == "" {
		return fmt.Errorf("proxy method is required")
	}
	for _, header := range i.Config.Headers {
		if strings.TrimSpace(header.From) == "" || strings.TrimSpace(header.To) == "" {
			return fmt.Errorf("proxy header source and destination are required")
		}
	}
	return nil
}

type Output struct{ response.Response }
type Handler struct{}

func New() xhandler.Contract[Input, Output] { return &Handler{} }

func (*Handler) Exec(ctx context.Context, _ xhandler.Session, input *Input, output *Output) error {
	client, err := input.HTTP.Client(ctx, input.Config.Client)
	if err != nil {
		return err
	}
	if client == nil {
		return fmt.Errorf("proxy provider returned no HTTP client")
	}
	endpoint, err := url.Parse(input.Config.Client.URL)
	if err != nil {
		return err
	}
	if input.Config.ForwardQuery {
		query := endpoint.Query()
		for key, values := range input.Request.URL.Query() {
			for _, value := range values {
				query.Add(key, value)
			}
		}
		endpoint.RawQuery = query.Encode()
	}
	var body io.Reader
	if input.Config.ForwardBody {
		body = input.Request.Body
	}
	request, err := http.NewRequestWithContext(ctx, input.Config.Client.Method, endpoint.String(), body)
	if err != nil {
		return err
	}
	for _, header := range input.Config.Headers {
		for _, value := range input.Request.Header.Values(header.From) {
			request.Header.Add(header.To, value)
		}
	}
	stripHopHeaders(request.Header)
	received, err := client.Do(request)
	if err != nil {
		return err
	}
	defer received.Body.Close()
	data, err := io.ReadAll(received.Body)
	if err != nil {
		return err
	}
	headers := received.Header.Clone()
	stripHopHeaders(headers)
	options := []response.Option{response.WithStatusCode(received.StatusCode), response.WithBytes(data), response.WithCompressionType(headers.Get("Content-Encoding")), response.WithHeaders(headers)}
	output.Response = response.NewBuffered(options...)
	return nil
}

func stripHopHeaders(header http.Header) {
	for _, value := range header.Values("Connection") {
		for _, name := range strings.Split(value, ",") {
			header.Del(strings.TrimSpace(name))
		}
	}
	for _, name := range []string{"Connection", "Proxy-Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization", "TE", "Trailer", "Transfer-Encoding", "Upgrade"} {
		header.Del(name)
	}
}

var _ xhandler.Contract[Input, Output] = (*Handler)(nil)
