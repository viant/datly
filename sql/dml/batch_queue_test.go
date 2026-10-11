package dml

import (
	"context"
	"github.com/viant/datly/internal/testharness/sqlite"
	sqldialect "github.com/viant/sqlx/metadata/info/dialect"
	"github.com/viant/sqlx/testutil/sqlfault"
	"strings"
	"testing"
)

type batchRegion struct {
	Order  int `sqlx:"AD_ORDER_ID,primaryKey"`
	Region int `sqlx:"DATACENTER_REGION_ID,primaryKey"`
}

func TestExplicitBatchPreservesSQLXChunkSizeAndAffectedRows(t *testing.T) {
	for _, failure := range []bool{false, true} {
		t.Run(map[bool]string{false: "affected and missing keys", true: "late insert rollback"}[failure], func(t *testing.T) {
			ctx := context.Background()
			h := sqlite.New(t)
			if err := h.ExecStatements(ctx, "CREATE TABLE mapping(AD_ORDER_ID INTEGER NOT NULL,DATACENTER_REGION_ID INTEGER NOT NULL,PRIMARY KEY(AD_ORDER_ID,DATACENTER_REGION_ID))"); err != nil {
				t.Fatal(err)
			}
			if failure {
				if err := h.ExecStatements(ctx, "CREATE TRIGGER fail_tail BEFORE INSERT ON mapping WHEN NEW.AD_ORDER_ID=8001 BEGIN SELECT RAISE(ABORT,'tail batch failure'); END"); err != nil {
					t.Fatal(err)
				}
			}
			var prepared []string
			db := h.FaultDB(t, func(_ context.Context, c sqlfault.Call) error {
				if c.Phase == "prepare" && strings.HasPrefix(c.SQL, "INSERT INTO mapping") {
					prepared = append(prepared, c.SQL)
				}
				return nil
			})
			data := NewData(db)
			dialect, err := data.dialectFor(ctx, db)
			if err != nil {
				t.Fatal(err)
			}
			forced := *dialect
			forced.Insert = sqldialect.InsertWithMultiValues
			data.dialect = &forced
			rows := make([]*batchRegion, 8001)
			for i := range rows {
				rows[i] = &batchRegion{Order: i + 1, Region: 1}
			}
			affected := int64(-1)
			if err = data.BeginInvocation(); err != nil {
				t.Fatal(err)
			}
			if err = data.InsertWithBatchSize("mapping", rows, 8000, &affected); err != nil {
				t.Fatal(err)
			}
			if affected != -1 {
				t.Fatal("affected rows assigned before drain")
			}
			err = data.Complete(ctx, nil)
			var count int
			if e := h.DB.QueryRow("SELECT COUNT(*) FROM mapping").Scan(&count); e != nil {
				t.Fatal(e)
			}
			if failure {
				if err == nil || count != 0 || affected != -1 {
					t.Fatalf("rollback err=%v count=%d affected=%d", err, count, affected)
				}
				return
			}
			if err != nil || count != 8001 || affected != 8001 {
				t.Fatalf("insert err=%v count=%d affected=%d", err, count, affected)
			}
			found := false
			for _, query := range prepared {
				if strings.Count(query, "?") == 16000 {
					found = true
				}
			}
			if !found {
				t.Fatal("SQLX 8000-row INSERT statement was not prepared")
			}
			// A nonexistent key must not inflate the database's affected-row result.
			deleting := []*batchRegion{{Order: 1, Region: 1}, {Order: 1, Region: 1}, {Order: 99999, Region: 1}}
			deleted := int64(-1)
			next := NewData(db)
			if err = next.BeginInvocation(); err != nil {
				t.Fatal(err)
			}
			if err = next.DeleteWithBatchSize("mapping", deleting, 8000, &deleted); err != nil {
				t.Fatal(err)
			}
			if err = next.Complete(ctx, nil); err != nil {
				t.Fatal(err)
			}
			if deleted != 1 {
				t.Fatalf("affected delete=%d want1", deleted)
			}
		})
	}
}

func TestExplicitBatchRejectsInvalidBeforeQueue(t *testing.T) {
	data := NewData(nil)
	count := int64(7)
	for _, method := range []func(string, any, int, *int64) error{data.InsertWithBatchSize, data.DeleteWithBatchSize} {
		for _, tc := range []struct {
			rows any
			size int
			dest *int64
		}{{[]*batchRegion{{1, 1}}, 0, &count}, {[]*batchRegion{{1, 1}}, 1, nil}, {[]*batchRegion{nil}, 1, &count}, {[]int{1}, 1, &count}} {
			if err := method("mapping", tc.rows, tc.size, tc.dest); err == nil {
				t.Fatal("invalid batch accepted")
			}
		}
	}
	if count != 7 || len(data.queue) != 0 {
		t.Fatal("invalid batch changed output or queue")
	}
}
