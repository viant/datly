package handler

import (
	"encoding/json"
	"github.com/viant/datly/gateway"
	meta2 "github.com/viant/datly/gateway/runtime/meta"
	"github.com/viant/datly/gateway/runtime/standalone/endpoint"
	"net/http"
)

type (
	config struct {
		config Config
	}

	Config struct {
		Gateway  *gateway.Config
		Endpoint *endpoint.Config
		Meta     *meta2.Config
	}
)

func (h *config) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if !meta2.IsAuthorized(request, h.config.Meta.AllowedSubnet) {
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
func NewConfig(gateway *gateway.Config, endpoint *endpoint.Config, meta *meta2.Config) http.Handler {
	handler := &config{}
	handler.config.Endpoint = endpoint
	handler.config.Meta = meta
	handler.config.Gateway = gateway
	return handler
}
