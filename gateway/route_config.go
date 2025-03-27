package gateway

import (
	"context"
	"encoding/json"
	"github.com/viant/datly/repository/contract"
	"net/http"
)

func (r *Router) NewConfigRoute() *Route {
	return &Route{
		Path: contract.NewPath(http.MethodGet, r.config.Meta.ConfigURI),
		Handler: func(ctx context.Context, response http.ResponseWriter, req *http.Request) {
			r.handleConfig(response)
		},
		Config:  r.config.Logging,
		Version: r.config.Version,
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
