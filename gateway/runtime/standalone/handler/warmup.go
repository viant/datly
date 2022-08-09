package handler

import (
	"encoding/json"
	"github.com/viant/datly/gateway/runtime/meta"
	"github.com/viant/datly/gateway/warmup"
	"net/http"
	"path"
	"strings"
)

type (
	cacheWarmup struct {
		URIPrefix string
		meta      *meta.Config
		lookup    warmup.PreCachables
	}
)

func (v *cacheWarmup) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if !meta.IsAuthorized(request, v.meta.AllowedSubnet) {
		writer.WriteHeader(http.StatusForbidden)
		return
	}
	URI := request.RequestURI
	if index := strings.Index(URI, v.meta.CacheWarmURI); index != -1 {
		URI = path.Join(v.URIPrefix, URI[index+len(v.meta.CacheWarmURI):])
	}
	response := warmup.PreCache(v.lookup, URI)
	data, err := json.Marshal(response)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}
	writer.Write(data)
}

// NewCacheWarmup creates a cache warmup handler
func NewCacheWarmup(URI string, meta *meta.Config, lookup warmup.PreCachables) http.Handler {
	handler := &cacheWarmup{lookup: lookup, meta: meta, URIPrefix: URI}
	return handler
}
