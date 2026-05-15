package warmup

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	datlywarmup "github.com/viant/datly/warmup"
)

func TestAppendPreCachedUsesEntryRows(t *testing.T) {
	response := &Response{}
	result := &datlywarmup.Result{
		Rows: 30,
		Entries: []*datlywarmup.EntryResult{
			{View: "periodSummary#", Column: "order_id", Params: "Period=today", Elapsed: "1s", TimeTaken: time.Second, Rows: 10},
			{View: "periodSummary#", Column: "order_id", Params: "Period=month", Elapsed: "2s", TimeTaken: 2 * time.Second, Rows: 20},
		},
	}

	appendPreCached(response, "/v1/api/cache/warmup/order", result)

	require.Len(t, response.PreCached, 2)
	require.Equal(t, "Period=today", response.PreCached[0].Params)
	require.Equal(t, 10, response.PreCached[0].Rows)
	require.Equal(t, "Period=month", response.PreCached[1].Params)
	require.Equal(t, 20, response.PreCached[1].Rows)
	require.Equal(t, "/v1/api/cache/warmup/order", response.PreCached[1].URI)
}
