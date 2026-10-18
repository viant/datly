package application_test

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/application"
	gateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/typecatalog"
)

type managerStatus struct {
	Version, Status, UpTime string
	StartTime               time.Time
}

func readManagerStatus(t *testing.T, manager *application.Manager) managerStatus {
	t.Helper()
	w := httptest.NewRecorder()
	manager.ServeHTTP(w, httptest.NewRequest("GET", gateway.DefaultStatusURI, nil))
	require.Equal(t, 200, w.Code, w.Body.String())
	var info managerStatus
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &info))
	require.Equal(t, "UP", info.Status)
	return info
}

func requireStatusContinuity(t *testing.T, before, after managerStatus) {
	t.Helper()
	require.True(t, before.StartTime.Equal(after.StartTime), "handler replacement reset service start")
	previous, err := time.ParseDuration(before.UpTime)
	require.NoError(t, err)
	current, err := time.ParseDuration(after.UpTime)
	require.NoError(t, err)
	require.GreaterOrEqual(t, current, previous)
}

func TestManagerStatusContinuityAcrossReload(t *testing.T) {
	beforeNew := time.Now()
	manager, err := application.New(nil)
	require.NoError(t, err)
	afterNew := time.Now()
	t.Cleanup(func() { require.NoError(t, manager.Shutdown(context.Background())) })
	var first managerStatus
	for i, version := range []string{"first", "second"} {
		require.NoError(t, manager.Reload(context.Background(), application.Request{Revision: uint64(i + 1), Compile: func(context.Context, *typecatalog.Catalog) (*application.Build, error) {
			return &application.Build{Version: version}, nil
		}}))
		info := readManagerStatus(t, manager)
		require.Equal(t, version, info.Version)
		require.False(t, info.StartTime.Before(beforeNew))
		require.False(t, info.StartTime.After(afterNew))
		if i == 0 {
			first = info
		} else {
			requireStatusContinuity(t, first, info)
		}
	}
}
