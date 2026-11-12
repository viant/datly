package gateway

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/logger"
	"github.com/viant/gmetric/counter"
	"github.com/viant/xdatly/handler/exec"
)

type routeTestCounter struct {
	values map[interface{}]int
}

func newRouteTestCounter() *routeTestCounter {
	return &routeTestCounter{values: map[interface{}]int{}}
}

func (c *routeTestCounter) Begin(started time.Time) counter.OnDone {
	return func(time.Time, ...interface{}) int64 { return 0 }
}

func (c *routeTestCounter) DecrementValue(value interface{}) int64 {
	c.values[value]--
	return int64(c.values[value])
}

func (c *routeTestCounter) IncrementValue(value interface{}) int64 {
	c.values[value]++
	return int64(c.values[value])
}

func TestRouteHandleIncrementsRequestBucket(t *testing.T) {
	counter := newRouteTestCounter()
	route := &Route{
		Counter: logger.NewCounter(counter),
		Handler: func(ctx context.Context, response http.ResponseWriter, req *http.Request) {
			ctx.Value(exec.ContextKey).(*exec.Context).StatusCode = http.StatusCreated
			response.WriteHeader(http.StatusCreated)
		},
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/api/test", nil)
	recorder := httptest.NewRecorder()
	status := route.Handle(recorder, req)

	require.Equal(t, http.StatusCreated, status)
	require.Equal(t, 1, counter.values["Request"])
	require.Equal(t, 1, counter.values["Success"])
	require.Equal(t, 1, counter.values["status:2xx"])
}
