package gateway

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRouterNewGlobalMetricRoutes(t *testing.T) {
	router := &Router{config: &Config{}}

	routes := router.NewGlobalMetricRoutes("/v1/api/meta/metric")

	require.Len(t, routes, 7)
	require.Equal(t, "/v1/api/meta/metric/operations", routes[0].Path.URI)
	require.Equal(t, "/v1/api/meta/metric/operation/{name}", routes[1].Path.URI)
	require.Equal(t, "/v1/api/meta/metric/operation/{name}/cumulative/{metric}", routes[2].Path.URI)
	require.Equal(t, "/v1/api/meta/metric/operation/{name}/recent/{metric}", routes[3].Path.URI)
	require.Equal(t, "/v1/api/meta/metric/operation/{name}/recent", routes[4].Path.URI)
	require.Equal(t, "/v1/api/meta/metric/counters", routes[5].Path.URI)
	require.Equal(t, "/v1/api/meta/metric/counter/{name}", routes[6].Path.URI)
	require.Nil(t, routes[0].ApiKeys)
}
