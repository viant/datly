package dml

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	mysqlmeta "github.com/viant/sqlx/metadata/product/mysql"
	"github.com/viant/sqlx/metadata/registry"
)

type ignoredLink struct {
	Subscription int `sqlx:"report_suite_subscription_id"`
	Campaign     int `sqlx:"campaign_id"`
}

func TestNativeInsertIgnoreBatchOrderMappingAndOwner(t *testing.T) {
	for _, n := range []int{2, 137} {
		for _, mode := range []string{"commit", "external", "tail failure"} {
			t.Run(fmt.Sprintf("%d/%s", n, mode), func(t *testing.T) {
				ctx := context.Background()
				recorder := &upsertDriver{failTail: mode == "tail failure"}
				db := sql.OpenDB(recorder)
				defer db.Close()
				var opts []Option
				var tx *sql.Tx
				if mode == "external" {
					var err error
					tx, err = db.BeginTx(ctx, nil)
					require.NoError(t, err)
					defer tx.Rollback()
					opts = append(opts, WithTx(tx))
				}
				data := NewData(db, opts...)
				data.dialect = registry.LookupDialect(mysqlmeta.MySQL5())
				require.NoError(t, data.BeginInvocation())
				rows := make([]*ignoredLink, n)
				for i := range rows {
					rows[i] = &ignoredLink{Subscription: 31, Campaign: i + 1}
				}
				first := rows[0]
				var affected int64
				require.NoError(t, data.InsertIgnoreWithBatchSize("links", rows, len(rows), &affected))
				require.Len(t, data.queue, 1)
				require.True(t, data.queue[0].appendBarrier)
				require.True(t, data.queue[0].insertIgnore)
				require.Equal(t, n, data.queue[0].batchSize)
				// Copy slice backing while retaining element identity and order.
				rows[0] = &ignoredLink{Subscription: 99, Campaign: 99}
				queued := data.queue[0].data.([]*ignoredLink)
				require.Same(t, first, queued[0])
				require.Empty(t, recorder.queries, "queue admission must not execute")
				require.NoError(t, data.Insert("events", &ignoredLink{1, 2}))
				err := data.Complete(ctx, nil)
				if mode == "tail failure" {
					require.ErrorContains(t, err, "event failure")
				} else {
					require.NoError(t, err)
				}
				require.Len(t, recorder.queries, 2, "one source slice means one multirow statement")
				require.Equal(t, "INSERT IGNORE INTO `links`(report_suite_subscription_id,campaign_id) VALUES "+insertIgnoreTuples(n), recorder.queries[0])
				require.Len(t, recorder.args[0], 2*n)
				for i := 0; i < n; i++ {
					require.Equal(t, []driver.NamedValue{{Ordinal: 2*i + 1, Value: int64(31)}, {Ordinal: 2*i + 2, Value: int64(i + 1)}}, recorder.args[0][2*i:2*i+2])
				}
				require.Equal(t, int64(1), affected)
				require.Equal(t, 1, recorder.begins)
				switch mode {
				case "commit":
					require.Equal(t, 1, recorder.commits)
					require.Zero(t, recorder.rollbacks)
				case "tail failure":
					require.Equal(t, 1, recorder.rollbacks)
					require.Zero(t, recorder.commits)
				case "external":
					require.Zero(t, recorder.commits)
					require.Zero(t, recorder.rollbacks)
					require.True(t, recorder.active)
				}
			})
		}
	}
}

func insertIgnoreTuples(n int) string {
	result := ""
	for i := 0; i < n; i++ {
		if i > 0 {
			result += ","
		}
		result += "(?,?)"
	}
	return result
}

func TestNativeInsertIgnoreServiceCacheIsolationAndEmptySlice(t *testing.T) {
	recorder := &upsertDriver{}
	db := sql.OpenDB(recorder)
	defer db.Close()
	data := NewData(db)
	data.dialect = registry.LookupDialect(mysqlmeta.MySQL5())
	require.NoError(t, data.BeginInvocation())
	var affected int64
	rows := []*ignoredLink{{1, 2}}
	const suffix = "ON DUPLICATE KEY UPDATE campaign_id = VALUES(campaign_id)"
	methods := []string{"plain", "ignore", "plain", "suffix", "ignore", "suffix", "plain"}
	for _, mode := range methods {
		switch mode {
		case "plain":
			require.NoError(t, data.InsertWithBatchSize("links", rows, 1, &affected))
		case "ignore":
			require.NoError(t, data.InsertIgnoreWithBatchSize("links", rows, 1, &affected))
		case "suffix":
			require.NoError(t, data.InsertWithBatchSizeAndOnDuplicateKeySQL("links", rows, 1, &affected, suffix))
		}
	}
	require.NoError(t, data.InsertIgnoreWithBatchSize("links", []*ignoredLink{}, 1, &affected))
	require.Len(t, data.queue, len(methods))
	require.NoError(t, data.Complete(context.Background(), nil))
	require.Len(t, recorder.queries, len(methods))
	for i, mode := range methods {
		prefix := "INSERT INTO "
		if mode == "ignore" {
			prefix = "INSERT IGNORE INTO "
		}
		tail := ""
		if mode == "suffix" {
			tail = " " + suffix
		}
		require.Equal(t, prefix+"`links`(report_suite_subscription_id,campaign_id) VALUES (?,?)"+tail, recorder.queries[i])
	}
	require.Len(t, data.insertServices, 2, "plain and ignore cached separately; suffix stays uncached")
}
