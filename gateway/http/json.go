package http

import (
	"encoding/json"
	stdhttp "net/http"
)

func writeJSON(writer stdhttp.ResponseWriter, statusCode int, payload any) error {
	var data []byte
	var encodeErr error
	if payload != nil {
		var err error
		data, err = json.Marshal(payload)
		if err != nil {
			encodeErr = err
			statusCode = stdhttp.StatusInternalServerError
			data = []byte(`{"status":"error","message":"internal server error","error":"internal server error"}`)
			writer.Header().Del("Content-Length")
			writer.Header().Del("Content-Encoding")
		}
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(responseStatusCode(statusCode))
	_, writeErr := writer.Write(data)
	if encodeErr != nil {
		return encodeErr
	}
	return writeErr
}
