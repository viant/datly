package runtime

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/jobs"
	sqldml "github.com/viant/datly/sql/dml"
	xasync "github.com/viant/xdatly/async"
	xhandler "github.com/viant/xdatly/handler"
)

type exchangePolicy struct {
	controls jobs.Controls
	owner    string
}

func (p exchangePolicy) Select(context.Context, any) (jobs.Controls, error) { return p.controls, nil }
func (p exchangePolicy) Owns(job *xasync.Job) bool                          { return job.URI == p.owner }
func TestAsyncExchangeScheduleSyncInspectAndReuseSQLite(t *testing.T) {
	for _, sync := range []bool{false, true} {
		name := "schedule"
		if sync {
			name = "sync"
		}
		t.Run(name, func(t *testing.T) {
			f := newAsyncReaderFixture(t)
			ctx := context.Background()
			require.NoError(t, f.harness.ExecStatements(ctx, "CREATE TABLE exchange_audit(ID INTEGER)"))
			registered := f.runtime.registered[f.target.Component.String()]
			calls := 0
			registered.DataSource = sqldml.Source{DB: f.harness.DB}
			registered.Handler = rhandler.HandlerFunc(func(ctx context.Context, inv rhandler.Invocation) (any, error) {
				calls++
				value, _, err := inv.Binder.Lookup(ctx, xhandler.DataKey)
				if err != nil {
					return nil, err
				}
				err = value.(xhandler.Data).Execute("INSERT INTO exchange_audit VALUES(?)", inv.Input.(*asyncReaderInput).ID)
				return &asyncReaderOutput{}, err
			})
			var inspectionInput bool
			service, err := f.runtime.NewAsyncService(jobs.Config{Store: f.store, Authorize: func(_ context.Context, access jobs.Access) error {
				if access.Action == jobs.Inspect && access.Input != nil {
					inspectionInput = true
				}
				return nil
			}})
			require.NoError(t, err)
			submission := f.submission(&asyncReaderInput{ID: 7, Tenant: 1})
			submission.Policy = exchangePolicy{controls: jobs.Controls{Sync: sync, MatchKey: "users/key"}}
			result, err := service.Exchange(ctx, submission)
			require.NoError(t, err)
			require.NotNil(t, result.Job)
			if sync {
				require.Equal(t, xasync.StatusDone, result.Job.Status)
				require.Equal(t, 1, calls)
			} else {
				require.Equal(t, xasync.StatusPending, result.Job.Status)
				require.Zero(t, calls)
			}
			repeat, err := service.Exchange(ctx, submission)
			if sync {
				require.ErrorIs(t, err, jobs.ErrResultUnavailable)
			} else {
				require.NoError(t, err)
			}
			require.True(t, repeat.Reused)
			require.Equal(t, result.Job.ID, repeat.Job.ID)
			submission.Policy = exchangePolicy{controls: jobs.Controls{JobID: result.Job.ID}, owner: submission.Job.URI}
			inspected, err := service.Exchange(ctx, submission)
			require.NoError(t, err)
			require.Equal(t, result.Job.ID, inspected.Job.ID)
			require.True(t, inspectionInput)
			submission.Policy = exchangePolicy{controls: jobs.Controls{JobID: result.Job.ID}, owner: "/different"}
			_, err = service.Exchange(ctx, submission)
			require.ErrorIs(t, err, jobs.ErrNotFound)
			var count int
			require.NoError(t, f.harness.DB.QueryRow("SELECT count(*) FROM exchange_audit").Scan(&count))
			require.Equal(t, calls, count)
			require.LessOrEqual(t, calls, 1)
		})
	}
}
