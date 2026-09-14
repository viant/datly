package application_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/application"
	authfixture "github.com/viant/datly/internal/testharness/auth"
	"github.com/viant/datly/runtime/jobs"
	xasync "github.com/viant/xdatly/async"
)

func TestHTTPAsyncDeclaredJWTSourceAndExpirySQLite(t *testing.T) {
	for _, derived := range []bool{false, true} {
		name := "direct"
		if derived {
			name = "derived"
		}
		t.Run(name, func(t *testing.T) {
			f := &httpAsyncFixture{derivedJWT: derived}
			f.init(t, false)
			f.jwt = authfixture.NewJWT(t)
			f.config.Authorize = func(_ context.Context, access jobs.Access) error {
				switch input := access.Input.(type) {
				case *httpAsyncInput:
					if input.JWT == nil || input.JWT.Subject != "approved" || input.Current != nil || input.InitCount != 0 {
						t.Errorf("invalid capture authorization input: %+v", input)
					}
				case *httpAsyncStatusInput:
					if input.JWT == nil || input.JWT.Subject != "approved" {
						t.Error("status did not verify current JWT")
					}
				}
				return nil
			}
			f.start(t)
			f.token = f.jwt.Sign(t, time.Now().Add(-time.Hour))
			denied := f.request("PATCH", "/inventory?id=7&key=bad", `{"quantity":0}`, "allowed")
			require.Equal(t, 401, denied.Code, denied.Body.String())
			require.Zero(t, f.calls.Load())
			var count int
			require.NoError(t, f.jobDB.DB.QueryRow("SELECT count(*) FROM APP_JOBS").Scan(&count))
			require.Zero(t, count)
			f.token = f.jwt.Sign(t, time.Now().Add(time.Hour))
			accepted := f.request("PATCH", "/inventory?id=7&key=raw", `{"quantity":0}`, "allowed")
			require.Equal(t, 200, accepted.Code, accepted.Body.String())
			var output httpAsyncOutput
			require.NoError(t, json.Unmarshal(accepted.Body.Bytes(), &output))
			record, err := f.store.Get(context.Background(), output.Job.ID)
			require.NoError(t, err)
			var state map[string]json.RawMessage
			require.NoError(t, json.Unmarshal([]byte(record.State), &state))
			key := "JWT"
			if derived {
				key = "Token"
				require.NotContains(t, state, "JWT")
			}
			var raw string
			require.NoError(t, json.Unmarshal(state[key], &raw))
			require.Equal(t, "Bearer "+f.token, raw)
			require.NotContains(t, state, "Current")
			require.NotContains(t, state, "InitCount")
			// Replace only the raw credential in the durable state to deterministically
			// model expiry before delivery, using the real verifier rather than a clock stub.
			expired := f.jwt.Sign(t, time.Now().Add(-time.Hour))
			state[key], err = json.Marshal("Bearer " + expired)
			require.NoError(t, err)
			replacement, err := json.Marshal(state)
			require.NoError(t, err)
			_, err = f.jobDB.DB.Exec("UPDATE APP_JOBS SET State=? WHERE ID=?", string(replacement), output.Job.ID)
			require.NoError(t, err)
			object, err := f.fs.Object(context.Background(), output.Job.EventURL)
			require.NoError(t, err)
			require.Error(t, f.manager.DispatchStorageEvent(context.Background(), object))
			record, err = f.store.Get(context.Background(), output.Job.ID)
			require.NoError(t, err)
			require.Equal(t, xasync.StatusPending, record.Status)
			require.Zero(t, f.calls.Load())
			// Current HTTP inspection verifies its own declared token, not stored claims.
			status := f.request("GET", "/job-status/"+output.Job.ID, "", "allowed")
			require.Equal(t, 200, status.Code, status.Body.String())
			f.token = expired
			status = f.request("GET", "/job-status/"+output.Job.ID, "", "allowed")
			require.Equal(t, 401, status.Code, status.Body.String())
			// A decoded claims object is never accepted as the replay credential.
			state[key] = json.RawMessage(`{"sub":"approved","exp":9999999999}`)
			replacement, err = json.Marshal(state)
			require.NoError(t, err)
			_, err = f.jobDB.DB.Exec("UPDATE APP_JOBS SET State=? WHERE ID=?", string(replacement), output.Job.ID)
			require.NoError(t, err)
			require.Error(t, f.manager.DispatchStorageEvent(context.Background(), object))
			require.Zero(t, f.calls.Load())
		})
	}
}
func TestHTTPAsyncJWTActuallyExpiresWhileQueuedSQLite(t *testing.T) {
	f := &httpAsyncFixture{}
	f.init(t, false)
	f.jwt = authfixture.NewJWT(t)
	expiry := time.Now().Add(2 * time.Second)
	f.token = f.jwt.Sign(t, expiry)
	f.start(t)
	res := f.request("PATCH", "/inventory?id=7&key=expires", `{"quantity":0}`, "allowed")
	require.Equal(t, 200, res.Code, res.Body.String())
	var output httpAsyncOutput
	require.NoError(t, json.Unmarshal(res.Body.Bytes(), &output))
	require.NoError(t, f.manager.Shutdown(context.Background()))
	// A later process/startup sees the same durable row and event, after expiry.
	time.Sleep(time.Until(expiry) + time.Second)
	f.config.Watch.JobURL = f.config.Notification.Destination
	f.config.Watch.FailedJobURL = f.config.Notification.Destination + "-failed"
	f.config.Watch.PollInterval = 5 * time.Millisecond
	reported := make(chan error, 1)
	f.config.Watch.Error = func(_ string, err error) {
		select {
		case reported <- err:
		default:
		}
	}
	manager, err := application.New(nil, application.WithAsync(f.config))
	require.NoError(t, err)
	defer manager.Shutdown(context.Background())
	require.NoError(t, manager.Reload(context.Background(), application.Request{Revision: 1, Compile: f.compile}))
	select {
	case <-reported:
	case <-time.After(5 * time.Second):
		t.Fatal("expired queued JWT was not rejected")
	}
	require.NoError(t, manager.Shutdown(context.Background()))
	record, err := f.store.Get(context.Background(), output.Job.ID)
	require.NoError(t, err)
	require.Equal(t, xasync.StatusPending, record.Status)
	require.Zero(t, f.calls.Load())
}
