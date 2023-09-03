package gateway

import (
	"context"
	"encoding/json"
	"github.com/viant/datly/gateway/router"
	"gopkg.in/yaml.v3"
	"net/http"
)

func (r *Router) NewViewMetaHandler(URL string, route *router.Route) *Route {
	return &Route{
		RouteMeta: RouteMeta{
			Method: http.MethodGet,
			URL:    URL,
		},
		Routes: []*router.Route{route},
		Handler: func(ctx context.Context, r http.ResponseWriter, req *http.Request) {
			handleViewMeta(r, route)
		},
	}
}

func handleViewMeta(r http.ResponseWriter, route *router.Route) {
	responseCode, body := viewMetaResponse(route)
	setContentType(r, responseCode, "text/yaml")
	write(r, responseCode, body)
}

func setContentType(r http.ResponseWriter, responseCode int, contentType string) {
	if responseCode >= 200 && responseCode <= 299 {
		r.Header().Set("Content-Type", contentType)
	}
}

func viewMetaResponse(route *router.Route) (int, []byte) {
	JSON, err := json.Marshal(route.View)
	if err != nil {
		return http.StatusInternalServerError, []byte(err.Error())
	}

	transient := map[string]interface{}{}
	err = json.Unmarshal(JSON, &transient)
	if err != nil {
		return http.StatusInternalServerError, []byte(err.Error())
	}

	YAML, err := yaml.Marshal(transient)
	if err != nil {
		return http.StatusInternalServerError, []byte(err.Error())
	}

	return http.StatusOK, YAML
}
