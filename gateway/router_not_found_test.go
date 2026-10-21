package gateway

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/repository/contract"
)

func TestRouterAvailableRoutesErr_DefaultHidesRoutes(t *testing.T) {
	router := &Router{
		config: &Config{},
		paths: []*contract.Path{
			contract.NewPath(http.MethodGet, "/v1/api/orders"),
		},
	}
	recorder := httptest.NewRecorder()
	err := router.availableRoutesErr(http.StatusNotFound, fmt.Errorf("not found route with Method: GET and URL: /"))

	router.handleErrorCode(recorder, http.StatusNotFound, err)

	require.Equal(t, http.StatusNotFound, recorder.Code)
	require.Equal(t, "not found route with Method: GET and URL: /", recorder.Body.String())
}

func TestRouterAvailableRoutesErr_ShowRoutesWhenConfigured(t *testing.T) {
	showAvailableRoutes := true
	router := &Router{
		config: &Config{
			ExposableConfig: ExposableConfig{
				ShowAvailableRoutes: &showAvailableRoutes,
			},
		},
		paths: []*contract.Path{
			contract.NewPath(http.MethodGet, "/v1/api/orders"),
		},
	}
	recorder := httptest.NewRecorder()
	err := router.availableRoutesErr(http.StatusNotFound, fmt.Errorf("not found route with Method: GET and URL: /"))

	router.handleErrorCode(recorder, http.StatusNotFound, err)

	require.Equal(t, http.StatusNotFound, recorder.Code)
	actual := &AvailableRoutesError{}
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), actual))
	require.Equal(t, "not found route with Method: GET and URL: /", actual.Message)
	require.Len(t, actual.Paths, 1)
	require.Equal(t, "/v1/api/orders", actual.Paths[0].URI)
}
