package handler

import (
	"encoding/json"
	meta2 "github.com/viant/datly/gateway/runtime/meta"
	"github.com/viant/datly/view"
	"gopkg.in/yaml.v3"
	"net/http"
	"strings"
)

type (
	metaView struct {
		URIPrefix string
		meta      *meta2.Config
		lookup    ViewLookupFn
	}

	ViewLookupFn func(method, location string) (*view.View, error)
)

func (v *metaView) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if !meta2.IsAuthorized(request, v.meta.AllowedSubnet) {
		writer.WriteHeader(http.StatusForbidden)
		return
	}

	URI := request.RequestURI
	if index := strings.Index(URI, v.URIPrefix); index != -1 {
		URI = URI[index+len(v.URIPrefix):]
	}

	aView, err := v.lookup(request.Method, URI)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}

	JSON, err := json.Marshal(aView)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}
	transient := map[string]interface{}{}
	err = json.Unmarshal(JSON, &transient)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}
	YAML, err := yaml.Marshal(transient)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}
	writer.Header().Set("Content-Type", "text/yaml")
	writer.Write(YAML)
}

// NewView creates view handler
func NewView(URI string, meta *meta2.Config, lookup ViewLookupFn) http.Handler {
	handler := &metaView{lookup: lookup, meta: meta, URIPrefix: URI}
	return handler
}
