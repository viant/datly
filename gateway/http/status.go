package http

import (
	"encoding/json"
	stdhttp "net/http"
	"time"
)

type statusInfo struct {
	Version   string
	Status    string
	UpTime    string
	StartTime time.Time
}

func (h *Handler) serveStatus(writer stdhttp.ResponseWriter, request *stdhttp.Request) bool {
	// The legacy router registers only this exact GET route. Other methods and
	// paths continue through normal routing, including its not-found response.
	if h.statusURI == "" || request.Method != stdhttp.MethodGet || h.routingPath(request) != h.statusURI {
		return false
	}
	info := statusInfo{Version: h.version, Status: "UP", UpTime: time.Since(h.startTime).String(), StartTime: h.startTime}
	payload, err := json.Marshal(info)
	if err != nil {
		stdhttp.Error(writer, err.Error(), stdhttp.StatusInternalServerError)
		return true
	}
	writer.Header().Set("Content-Type", "application/json")
	_, _ = writer.Write(payload)
	return true
}
