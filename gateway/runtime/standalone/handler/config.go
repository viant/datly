package handler

import (
	"encoding/json"
	"github.com/viant/datly/gateway"
	"github.com/viant/datly/gateway/runtime/standalone/endpoint"
	"github.com/viant/datly/gateway/runtime/standalone/meta"
	"net/http"
)

type (
	config struct {
		config Config
	}

	Config struct {
		Gateway  *gateway.Config
		Endpoint *endpoint.Config
		Meta     *meta.Config
	}
)

func (h *config) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if !meta.IsAuthorized(request, h.config.Meta.AllowedSubnet) {
		writer.WriteHeader(http.StatusForbidden)
		return
	}
	JSON, err := json.Marshal(h.config)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.Write(JSON)
}

//NewConfig creates config handler
func NewConfig(gateway *gateway.Config, endpoint *endpoint.Config, meta *meta.Config) http.Handler {
	handler := &config{}
	handler.config.Endpoint = endpoint
	handler.config.Meta = meta
	handler.config.Gateway = gateway
	return handler
}
