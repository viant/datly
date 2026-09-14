package jobs_test

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/runtime/jobs"
	"github.com/viant/sqlx/metadata/database"
	"github.com/viant/sqlx/metadata/info"
	xasync "github.com/viant/xdatly/async"
)

func TestProvisionJobTableSQLite(t *testing.T) {
	for _, tc := range []struct{ table, dataset string }{{"", ""}, {`"job.table"`, "main"}, {`"job""table"`, "main"}, {`main."qualified.jobs"`, ""}} {
		t.Run(tc.table+tc.dataset, func(t *testing.T) {
			ctx := context.Background()
			h := sqlite.New(t)
			config := jobs.SQLConfig{DB: h.DB, Table: tc.table, Dataset: tc.dataset}
			store, err := jobs.NewSQLStore(ctx, config)
			require.NoError(t, err)
			record := &jobs.Record{Job: xasync.Job{ID: "kept", Status: xasync.StatusPending, Request: xasync.Request{Method: "GET", URI: "/records"}, CreationTime: time.Now().UTC().Truncate(time.Microsecond)}, State: `{}`}
			require.NoError(t, store.Create(ctx, record))
			for i := 0; i < 3; i++ {
				config.DisableTableCreation = i%2 == 0
				again, err := jobs.NewSQLStore(ctx, config)
				require.NoError(t, err)
				actual, err := again.Get(ctx, record.ID)
				require.NoError(t, err)
				require.Equal(t, record, actual)
			}
		})
	}
}

func TestJobTableConcurrentInitializationSQLite(t *testing.T) {
	h := sqlite.New(t)
	ctx := context.Background()
	var wg sync.WaitGroup
	results := make(chan error, 10)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			store, err := jobs.NewSQLStore(ctx, jobs.SQLConfig{DB: h.DB})
			if err == nil {
				err = store.Create(ctx, &jobs.Record{Job: xasync.Job{ID: fmt.Sprint(i), Status: xasync.StatusPending, Request: xasync.Request{Method: "GET", URI: "/records"}, CreationTime: time.Now().UTC()}})
			}
			results <- err
		}(i)
	}
	wg.Wait()
	close(results)
	for err := range results {
		require.NoError(t, err)
	}
	var count int
	require.NoError(t, h.DB.QueryRow(`SELECT count(*) FROM DATLY_JOBS`).Scan(&count))
	require.Equal(t, 10, count)
}

func TestJobSchemaFailureAndRecoverySQLite(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	config := jobs.SQLConfig{DB: h.DB, DisableTableCreation: true}
	store, err := jobs.NewSQLStore(ctx, config)
	require.Nil(t, store)
	require.ErrorContains(t, err, "DisableTableCreation=true")
	for _, tc := range []jobs.SQLConfig{{DB: h.DB, Table: "jobs; DELETE FROM users"}, {DB: h.DB, Table: "x.y.z"}, {DB: h.DB, Table: "main.jobs", Dataset: "main"}, {DB: h.DB, Dataset: "bad;--"}, {DB: h.DB, Dataset: "unattached"}} {
		store, err = jobs.NewSQLStore(ctx, tc)
		require.Nil(t, store)
		require.Error(t, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	store, err = jobs.NewSQLStore(canceled, jobs.SQLConfig{DB: h.DB})
	require.Nil(t, store)
	require.ErrorIs(t, err, context.Canceled)
	// Open a real read-only connection, not a mocked DDL executor.
	ro, err := sql.Open("sqlite3", "file:"+filepath.Join(h.TempDir, "test.db")+"?mode=ro")
	require.NoError(t, err)
	defer ro.Close()
	store, err = jobs.NewSQLStore(ctx, jobs.SQLConfig{DB: ro})
	require.Nil(t, store)
	require.ErrorContains(t, err, "readonly")
	config.DisableTableCreation = false
	store, err = jobs.NewSQLStore(ctx, config)
	require.NoError(t, err)
	readonlyStore, err := jobs.NewSQLStore(ctx, jobs.SQLConfig{DB: ro})
	require.NoError(t, err)
	require.NotNil(t, readonlyStore)
	// Schema init proves access/shape, not future DML grants.
	require.Error(t, readonlyStore.Create(ctx, &jobs.Record{Job: xasync.Job{ID: "denied", Status: xasync.StatusPending, Request: xasync.Request{Method: "GET", URI: "/records"}, CreationTime: time.Now()}}))
	h.DB.SetMaxOpenConns(1)
	held, err := h.DB.Conn(ctx)
	require.NoError(t, err)
	deadline, cancelDeadline := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancelDeadline()
	_, err = jobs.NewSQLStore(deadline, config)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.NoError(t, held.Close())
}

func TestExistingJobShapeIsNotRewrittenSQLite(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	require.NoError(t, h.ExecStatements(ctx, `CREATE TABLE DATLY_JOBS(ID TEXT PRIMARY KEY, Custom TEXT)`, `INSERT INTO DATLY_JOBS VALUES('keep','untouched')`))
	var before string
	require.NoError(t, h.DB.QueryRow(`SELECT sql FROM sqlite_master WHERE name='DATLY_JOBS'`).Scan(&before))
	store, err := jobs.NewSQLStore(ctx, jobs.SQLConfig{DB: h.DB})
	require.Nil(t, store)
	require.ErrorContains(t, err, "initialize original job table")
	var after, record string
	require.NoError(t, h.DB.QueryRow(`SELECT sql FROM sqlite_master WHERE name='DATLY_JOBS'`).Scan(&after))
	require.Equal(t, before, after)
	require.NoError(t, h.DB.QueryRow(`SELECT Custom FROM DATLY_JOBS WHERE ID='keep'`).Scan(&record))
	require.Equal(t, "untouched", record)
}

func TestProvisionedJobNullabilitySQLite(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	store, err := jobs.NewSQLStore(ctx, jobs.SQLConfig{DB: h.DB})
	require.NoError(t, err)
	r := &jobs.Record{Job: xasync.Job{ID: "nulls", Status: xasync.StatusPending, Request: xasync.Request{Method: "GET", URI: "/records"}, CreationTime: time.Now().UTC().Truncate(time.Microsecond)}}
	require.NoError(t, store.Create(ctx, r))
	require.NoError(t, h.ExecStatements(ctx, `UPDATE DATLY_JOBS SET Deactivated=NULL WHERE ID='nulls'`))
	actual, err := store.Get(ctx, r.ID)
	require.NoError(t, err)
	require.Equal(t, r, actual)
	rows, err := h.DB.Query(`PRAGMA table_info(DATLY_JOBS)`)
	require.NoError(t, err)
	defer rows.Close()
	nullable := map[string]bool{}
	for _, name := range []string{"Connector", "TableName", "TableDataset", "TableSchema", "CreateDisposition", "Template", "WriteDisposition", "Cache", "CacheKey", "CacheSet", "CacheNamespace", "UserEmail", "UserID", "Error", "StartTime", "EndTime", "ExpiryTime", "Deactivated"} {
		nullable[name] = true
	}
	count := 0
	for rows.Next() {
		var index, notnull, pk int
		var name, kind string
		var fallback any
		require.NoError(t, rows.Scan(&index, &name, &kind, &notnull, &fallback, &pk))
		require.Equal(t, !nullable[name], notnull == 1, name)
		if name == "ID" {
			require.Equal(t, 1, pk)
		} else {
			require.Zero(t, pk, name)
		}
		count++
	}
	require.NoError(t, rows.Err())
	require.Equal(t, 34, count)
}

func TestPreprovisionedJobTableWithoutVendorDDL(t *testing.T) {
	h := sqlite.New(t)
	ctx := context.Background()
	require.NoError(t, h.ExecStatements(ctx, sqlite.DatlyJobsSchema))
	ansi := &info.Dialect{Product: database.Product{Name: "ANSI"}, Placeholder: "?"}
	_, err := jobs.NewSQLStore(ctx, jobs.SQLConfig{DB: h.DB, Dialect: ansi})
	require.NoError(t, err)
	_, err = jobs.NewSQLStore(ctx, jobs.SQLConfig{DB: h.DB, Dialect: ansi, Table: "missing"})
	require.ErrorContains(t, err, "table creation unsupported")
}

func TestProvisionJobTableInAttachedDatasetSQLite(t *testing.T) {
	h := sqlite.New(t)
	ctx := context.Background()
	h.DB.SetMaxOpenConns(1)
	require.NoError(t, h.ExecStatements(ctx, `ATTACH ':memory:' AS "job.space"`))
	store, err := jobs.NewSQLStore(ctx, jobs.SQLConfig{DB: h.DB, Table: `"odd.jobs"`, Dataset: `"job.space"`})
	require.NoError(t, err)
	r := &jobs.Record{Job: xasync.Job{ID: "attached", Status: xasync.StatusPending, Request: xasync.Request{Method: "GET", URI: "/records"}, CreationTime: time.Now().UTC().Truncate(time.Microsecond)}}
	require.NoError(t, store.Create(ctx, r))
	actual, err := store.Get(ctx, r.ID)
	require.NoError(t, err)
	require.Equal(t, r, actual)
	var count int
	require.NoError(t, h.DB.QueryRow(`SELECT count(*) FROM main.sqlite_master WHERE type='table'`).Scan(&count))
	require.Zero(t, count)
	require.NoError(t, h.DB.QueryRow(`SELECT count(*) FROM "job.space"."odd.jobs"`).Scan(&count))
	require.Equal(t, 1, count)
}
