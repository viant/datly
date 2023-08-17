package gateway

import (
	"encoding/json"
	async2 "github.com/viant/xdatly/handler/async"
	"net/http"
)

func (r *Router) NewConfigRoute() *Route {
	return &Route{
		RouteMeta: RouteMeta{
			Method: http.MethodGet,
			URL:    r.config.Meta.ConfigURI,
		},
		Handler: func(response http.ResponseWriter, req *http.Request, _ *async2.Job) {
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
	JSON, err := json.Marshal(r.config.ExposableConfig)
	if err != nil {
		return http.StatusInternalServerError, []byte(err.Error())
	}
	return http.StatusOK, JSON
}
