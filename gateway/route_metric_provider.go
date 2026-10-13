package gateway

import (
	"github.com/viant/gmetric/counter"
	"github.com/viant/gmetric/counter/base"
)

const (
	routeRequestMetric   = "Request"
	routeSuccessMetric   = "Success"
	routeErrorMetric     = "Error"
	routeStatus2xxMetric = "status:2xx"
	routeStatus4xxMetric = "status:4xx"
	routeStatus5xxMetric = "status:5xx"
)

func newRouteMetricProvider() counter.Provider {
	return base.NewProvider(
		routeRequestMetric,
		routeSuccessMetric,
		routeErrorMetric,
		routeStatus2xxMetric,
		routeStatus4xxMetric,
		routeStatus5xxMetric,
	)
}
