package gateway

import (
	"context"
	"encoding/json"
	"net/http"
)

func (r *Router) NewConfigRoute() *Route {
	return &Route{
		RouteMeta: RouteMeta{
			Method: http.MethodGet,
			URL:    r.Config.Meta.ConfigURI,
		},
		Handler: func(ctx context.Context, response http.ResponseWriter, req *http.Request) {
			r.handleConfig(response)
		},
	}
}

func (r *Router) handleConfig(writer http.ResponseWriter) {
	statusCode, content := r.handleConfigResponseBody()
	setContentType(writer, statusCode, "text/yaml")
	write(writer, statusCode, content)
}

func (r *Router) handleConfigResponseBody() (int, []byte) {
	JSON, err := json.Marshal(r.Config.ExposableConfig)
	if err != nil {
		return http.StatusInternalServerError, []byte(err.Error())
	}
	return http.StatusOK, JSON
}
