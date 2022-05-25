package handler

import (
	"encoding/json"
	"fmt"
	meta2 "github.com/viant/datly/gateway/runtime/meta"
	"net/http"
	"time"
)

type (
	info struct {
		Version   string
		Status    string
		UpTime    string
		StartTime time.Time
	}

	status struct {
		info info
		meta *meta2.Config
	}
)

func (h *status) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if !meta2.IsAuthorized(request, h.meta.AllowedSubnet) {
		writer.WriteHeader(http.StatusForbidden)
		return
	}
	h.info.UpTime = fmt.Sprintf("%s", time.Now().Sub(h.info.StartTime))
	JSON, err := json.Marshal(&h.info)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.Write(JSON)
}

//NewStatus creates a status handler
func NewStatus(version string, meta *meta2.Config) http.Handler {
	handler := &status{}
	handler.info.Version = version
	handler.info.StartTime = time.Now()
	handler.info.Status = "UP"
	handler.meta = meta
	return handler
}
