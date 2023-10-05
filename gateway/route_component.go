package gateway

import (
	"context"
	"encoding/json"
	"github.com/viant/datly/repository"
	"github.com/viant/datly/repository/contract"
	"gopkg.in/yaml.v3"
	"net/http"
)

func (r *Router) NewViewMetaHandler(URL string, provider *repository.Provider) *Route {
	return &Route{
		Path:      contract.NewPath(http.MethodGet, URL),
		Providers: []*repository.Provider{provider},
		Handler: func(ctx context.Context, response http.ResponseWriter, req *http.Request) {
			handleViewMeta(ctx, response, provider)
		},
	}
}

func handleViewMeta(ctx context.Context, response http.ResponseWriter, provider *repository.Provider) {
	component, err := provider.Component(ctx)
	if err != nil {
		http.Error(response, err.Error(), http.StatusInternalServerError)
		return
	}
	responseCode, body := viewMetaResponse(component)
	setContentType(response, responseCode, "text/yaml")
	write(response, responseCode, body)
}

func setContentType(r http.ResponseWriter, responseCode int, contentType string) {
	if responseCode >= 200 && responseCode <= 299 {
		r.Header().Set("Content-Type", contentType)
	}
}

func viewMetaResponse(component *repository.Component) (int, []byte) {
	JSON, err := json.Marshal(component.View)
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
