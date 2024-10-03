package handler

import (
	"context"
	"fmt"
	"github.com/viant/xdatly/handler"
	xhttp "github.com/viant/xdatly/handler/http"
	"reflect"
)

const (
	ProxyHandler = "Proxy"
)

// ProxyProvider represents Proxy handler
type ProxyProvider struct{}

func (p *ProxyProvider) New(ctx context.Context, opts ...handler.Option) (handler.Handler, error) {
	options := handler.NewOptions(opts)
	if len(options.Arguments) == 0 {
		return nil, fmt.Errorf("proxy handler argument was empty")
	}
	if len(options.Arguments) != 2 {
		return nil, fmt.Errorf("invalid proxy handler argument: %v, exected method,url", options.Arguments)
	}
	if options.InputType == nil {
		return nil, fmt.Errorf("proxy handler input type was empty")
	}
	method := options.Arguments[0]
	URL := options.Arguments[1]
	ret := &Proxy{method: method, redirectURL: URL, inputType: options.InputType}
	return ret, nil
}

// Proxy represents redirecting handler
type Proxy struct {
	redirectURL string
	method      string
	inputType   reflect.Type
}

// Exec executes handler
func (p *Proxy) Exec(ctx context.Context, session handler.Session) (interface{}, error) {
	input := reflect.New(p.inputType).Interface()
	if err := session.Stater().Into(ctx, input); err != nil {
		return nil, err
	}
	httper := session.Http()
	request, err := httper.RequestOf(ctx, input)
	if err != nil {
		return nil, err
	}
	redirect := &xhttp.Route{URL: p.redirectURL, Method: p.method}
	err = session.Http().Redirect(ctx, redirect, request)
	return nil, err
}
