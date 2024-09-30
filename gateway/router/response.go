package router

import (
	"github.com/viant/datly/utils/httputils"
	"net/http"
	"time"
)

type ResponseWithMetrics struct {
	startTime time.Time
	http.ResponseWriter
}

func (r *ResponseWithMetrics) Write(data []byte) (int, error) {
	r.writeMetricHeader()
	return r.ResponseWriter.Write(data)
}

func (r *ResponseWithMetrics) WriteHeader(statusCode int) {
	r.writeMetricHeader()
	r.ResponseWriter.WriteHeader(statusCode)
}

func (r *ResponseWithMetrics) writeMetricHeader() {
	r.Header().Set(httputils.DatlyServiceTimeHeader, time.Since(r.startTime).String())
}

func NewMetricResponseWithTime(writer http.ResponseWriter, start time.Time) *ResponseWithMetrics {
	return &ResponseWithMetrics{
		startTime:      start,
		ResponseWriter: writer,
	}
}

func NewMetricResponse(writer http.ResponseWriter) *ResponseWithMetrics {
	return NewMetricResponseWithTime(writer, time.Now())
}
