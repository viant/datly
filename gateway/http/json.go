package http

import (
	"encoding/json"
	stdhttp "net/http"
)

func writeJSON(writer stdhttp.ResponseWriter, statusCode int, payload any) {
	var data []byte
	if payload != nil {
		var err error
		data, err = json.Marshal(payload)
		if err != nil {
			statusCode = stdhttp.StatusInternalServerError
			data = []byte(`{"status":"error","message":"internal server error","error":"internal server error"}`)
			writer.Header().Del("Content-Length")
			writer.Header().Del("Content-Encoding")
		}
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(responseStatusCode(statusCode))
	_, _ = writer.Write(data)
}
