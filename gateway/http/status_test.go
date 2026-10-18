package http

import (
	"context"
	"encoding/json"
	stdhttp "net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	druntime "github.com/viant/datly/runtime"
)

func statusHandler(t *testing.T, config Config, start time.Time) *Handler {
	t.Helper()
	rt, err := druntime.NewRuntime(nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, rt.Shutdown(context.Background())) })
	h, err := config.Build(context.Background(), HandlerInput{Runtime: rt, Version: "status-fixture", StartTime: start})
	require.NoError(t, err)
	return h
}

func TestStatusMetadata(t *testing.T) {
	start := time.Now().Add(-time.Hour)
	h := statusHandler(t, Config{}, start)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", DefaultStatusURI+"?ignored=true", nil))
	require.Equal(t, stdhttp.StatusOK, w.Code)
	require.Equal(t, "application/json", w.Header().Get("Content-Type"))
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &fields))
	require.Len(t, fields, 4, "legacy status has no envelope or readiness fields")
	for _, name := range []string{"Version", "Status", "UpTime", "StartTime"} {
		require.Contains(t, fields, name)
	}
	var info statusInfo
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &info))
	require.Equal(t, "status-fixture", info.Version)
	require.Equal(t, "UP", info.Status)
	require.True(t, start.Equal(info.StartTime))
	uptime, err := time.ParseDuration(info.UpTime)
	require.NoError(t, err)
	require.GreaterOrEqual(t, uptime, time.Hour)
}

func TestStatusExactGETRouting(t *testing.T) {
	for _, uri := range []string{"", "/service/status/", " "} {
		t.Run(uri, func(t *testing.T) {
			h := statusHandler(t, Config{Meta: Meta{StatusURI: uri}}, time.Time{})
			path := uri
			if path == "" || path == " " {
				path = DefaultStatusURI
			}
			for _, tc := range []struct {
				method, path string
				want         int
			}{{"GET", path, 200}, {"HEAD", path, 404}, {"POST", path, 404}, {"OPTIONS", path, 404}, {"GET", path + "/", 404}, {"GET", path + "/extra", 404}} {
				want := tc.want
				if uri == " " {
					want = 404
				}
				w := httptest.NewRecorder()
				h.ServeHTTP(w, httptest.NewRequest(tc.method, tc.path, nil))
				require.Equal(t, want, w.Code, "%s %s: %s", tc.method, tc.path, w.Body.String())
				require.Empty(t, w.Header().Get("Allow"))
			}
			if uri == "/service/status/" {
				for _, other := range []string{DefaultStatusURI, "/service/status"} {
					w := httptest.NewRecorder()
					h.ServeHTTP(w, httptest.NewRequest("GET", other, nil))
					require.Equal(t, 404, w.Code)
				}
			}
			if uri != " " {
				w := httptest.NewRecorder()
				h.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
				var info statusInfo
				require.NoError(t, json.Unmarshal(w.Body.Bytes(), &info))
				require.False(t, info.StartTime.IsZero())
				require.WithinDuration(t, time.Now(), info.StartTime, time.Second)
			}
		})
	}
}

func TestStatusAllowedSubnetUsesRemoteAddrPrefix(t *testing.T) {
	h := statusHandler(t, Config{Meta: Meta{AllowedSubnet: []string{"10.2."}}}, time.Time{})
	for _, tc := range []struct {
		remote string
		want   int
	}{{"10.2.3.4:1234", 200}, {"10.2.example:1234", 200}, {"192.0.2.1:1234", 403}} {
		r := httptest.NewRequest("GET", DefaultStatusURI, nil)
		r.RemoteAddr = tc.remote
		r.Header.Set("Forwarded", "for=10.2.3.4")
		r.Header.Set("X-Forwarded-For", "10.2.3.4")
		r.Header.Set("X-Real-IP", "10.2.3.4")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		require.Equal(t, tc.want, w.Code)
	}
}

func TestStatusConcurrentRequests(t *testing.T) {
	start := time.Now().Add(-time.Minute)
	h := statusHandler(t, Config{}, start)
	var workers sync.WaitGroup
	for worker := 0; worker < 32; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for call := 0; call < 10; call++ {
				w := httptest.NewRecorder()
				h.ServeHTTP(w, httptest.NewRequest("GET", DefaultStatusURI, nil))
				var info statusInfo
				if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &info) != nil || info.Version != "status-fixture" || info.Status != "UP" || !start.Equal(info.StartTime) {
					t.Errorf("invalid concurrent status: %d %s", w.Code, w.Body.String())
				}
				if uptime, err := time.ParseDuration(info.UpTime); err != nil || uptime < time.Minute {
					t.Errorf("invalid concurrent uptime: %q", info.UpTime)
				}
			}
		}()
	}
	workers.Wait()
}
