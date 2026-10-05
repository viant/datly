package reader

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/data"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/observability"
	"github.com/viant/datly/spec"
	sqlxread "github.com/viant/sqlx/io/read"
	"github.com/viant/sqlx/io/read/cache"
	"github.com/viant/sqlx/io/read/cache/afs"
	"github.com/viant/sqlx/testutil/sqlfault"
)

func TestReadingDataQueryPhaseSQLite(t *testing.T) {
	for _, tc := range []struct {
		name                           string
		phases                         []string
		callbacks, queries, reconnects int
		canceled, zero                 bool
	}{
		{name: "prepare exhaustion", phases: []string{"prepare", "prepare", "prepare"}, reconnects: 2},
		{name: "prepare recovery", phases: []string{"prepare"}, callbacks: 1, queries: 1, reconnects: 1},
		{name: "query then prepare exhaustion", phases: []string{"query", "prepare", "prepare"}, callbacks: 1, queries: 1, reconnects: 2},
		{name: "query recovery", phases: []string{"query"}, callbacks: 1, queries: 2, reconnects: 1},
		{name: "zero rows", callbacks: 1, queries: 1, zero: true},
		{name: "query cancellation", phases: []string{"query"}, callbacks: 1, queries: 1, canceled: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := sqlite.New(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			faults, queries, reconnects, classified, calls := 0, 0, 0, 0, 0
			failure := errors.New("driver: invalid connection")
			db := h.FaultDB(t, func(_ context.Context, c sqlfault.Call) error {
				if c.Phase == "query" {
					queries++
				}
				if faults < len(tc.phases) && c.Phase == tc.phases[faults] {
					faults++
					if tc.canceled {
						cancel()
					}
					return failure
				}
				return nil
			})
			policy := sqlxread.RetryPolicy{Attempts: 3, Recoverable: func(err error) bool { classified++; return strings.Contains(err.Error(), "invalid connection") }, Reconnect: func(context.Context) (*sql.DB, error) { reconnects++; return db, nil }}
			owner := observability.NewRecorder(nil, observability.WithReadingData(func(_ string, _ time.Duration, _ string, n int, _ []any, err error) {
				calls++
				if tc.zero {
					require.Zero(t, n)
				}
				if tc.canceled {
					require.ErrorIs(t, err, context.Canceled)
				}
			}))
			s := &Session{recorder: owner}
			read := s.beginView(ctx, &data.View{Spec: spec.View{Name: "phase"}})
			query := "SELECT 7 AS id"
			if tc.zero {
				query += " WHERE 0"
			}
			err := (rowRead{retry: policy, newRow: func() any { return &cursorRow{} }, options: []sqlxread.Option{sqlxread.WithRetry(policy)}}).query(ctx, rowQuery{db: db, read: read, query: &cache.ParmetrizedQuery{SQL: query}, visit: func(any) error { return nil }})
			require.Equal(t, tc.callbacks, calls)
			require.Equal(t, tc.queries, queries)
			require.Equal(t, tc.reconnects, reconnects)
			if tc.canceled {
				require.ErrorIs(t, err, context.Canceled)
				require.Zero(t, classified)
			} else {
				require.Equal(t, len(tc.phases), classified)
			}
			require.Len(t, read.metric.Executions, 1)
			require.False(t, read.metric.Executions[0].EndTime.Before(read.metric.Executions[0].StartTime))
		})
	}
}

func TestReadingDataCacheReplaySQLite(t *testing.T) {
	for _, zero := range []bool{false, true} {
		t.Run(map[bool]string{false: "row", true: "empty"}[zero], func(t *testing.T) {
			h := sqlite.New(t)
			native, err := afs.NewCache(t.TempDir(), time.Minute, "phase", nil)
			require.NoError(t, err)
			query := "SELECT 7 AS id"
			if zero {
				query += " WHERE 0"
			}
			calls := 0
			owner := observability.NewRecorder(nil, observability.WithReadingData(func(_ string, _ time.Duration, _ string, n int, _ []any, err error) {
				calls++
				require.NoError(t, err)
				if zero {
					require.Zero(t, n)
				} else {
					require.Equal(t, 1, n)
				}
			}))
			for i := 0; i < 2; i++ {
				stats := &cache.Stats{}
				s := &Session{recorder: owner}
				read := s.beginView(context.Background(), &data.View{Spec: spec.View{Name: "cache"}})
				db := h.DB
				if i == 1 {
					db = h.FaultDB(t, func(_ context.Context, c sqlfault.Call) error {
						if c.Phase == "prepare" || c.Phase == "query" {
							t.Fatal("cache replay queried database")
						}
						return nil
					})
				}
				require.NoError(t, (rowRead{readCache: native, stats: stats, newRow: func() any { return &cursorRow{} }, options: []sqlxread.Option{sqlxread.WithCache(native), sqlxread.WithCacheStats(stats)}}).query(context.Background(), rowQuery{db: db, read: read, query: &cache.ParmetrizedQuery{SQL: query}, visit: func(any) error { return nil }}))
				if i == 1 {
					require.True(t, stats.FoundLazy)
				}
			}
			require.Equal(t, 2, calls)
		})
	}
}

func TestReadingDataAcquisitionFailureSQLite(t *testing.T) {
	h := sqlite.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	calls := 0
	owner := observability.NewRecorder(nil, observability.WithReadingData(func(string, time.Duration, string, int, []any, error) { calls++ }))
	s := &Session{recorder: owner}
	read := s.beginView(ctx, &data.View{Spec: spec.View{Name: "canceled"}})
	err := (rowRead{newRow: func() any { return &cursorRow{} }}).query(ctx, rowQuery{db: h.DB, read: read, query: &cache.ParmetrizedQuery{SQL: "SELECT 7 AS id"}, visit: func(any) error { return nil }})
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, calls)
	require.Len(t, read.metric.Executions, 1)
}

type phaseObservationLog struct{ events *[]string }

func (p phaseObservationLog) Debug(string, ...any)           {}
func (p phaseObservationLog) Warn(string, ...any)            {}
func (p phaseObservationLog) Info(message string, _ ...any)  { *p.events = append(*p.events, message) }
func (p phaseObservationLog) Error(message string, _ ...any) { *p.events = append(*p.events, message) }

func TestReadingDataCleanupAndDiagnosticOrder(t *testing.T) {
	driver := &cursorConnector{}
	db := sql.OpenDB(driver)
	defer db.Close()
	events := []string{}
	owner := observability.NewRecorder(phaseObservationLog{events: &events}, observability.WithReadingData(func(_ string, _ time.Duration, _ string, _ int, _ []any, _ error) {
		// The simulated driver rejects preparation while the previous cursor is open.
		stmt, err := db.PrepareContext(context.Background(), "SELECT id")
		require.NoError(t, err)
		require.NoError(t, stmt.Close())
		events = append(events, "reading")
	}))
	s := &Session{recorder: owner}
	read := s.beginView(context.Background(), &data.View{Spec: spec.View{Name: "ordered"}})
	failure := errors.New("visit failed")
	err := (rowRead{stats: &cache.Stats{Type: cache.TypeWrite}, newRow: func() any { return &cursorRow{} }}).query(context.Background(), rowQuery{db: db, read: read, query: &cache.ParmetrizedQuery{SQL: "SELECT id"}, visit: func(any) error { return failure }})
	require.ErrorIs(t, err, failure)
	require.Zero(t, driver.overlaps.Load())
	require.Equal(t, []string{"reading", "datly cache read", "datly SQL read failed"}, events)
	require.Equal(t, int64(1), owner.Values("ordered")["cache:miss"])
}

type phaseCache struct {
	cache.Cache
	getErr, sourceErr error
	entry             *cache.Entry
	gets, sources     int
}

func (c *phaseCache) Get(context.Context, string, []interface{}, ...interface{}) (*cache.Entry, error) {
	c.gets++
	return c.entry, c.getErr
}
func (c *phaseCache) AsSource(context.Context, *cache.Entry) (cache.Source, error) {
	c.sources++
	return nil, c.sourceErr
}
func (*phaseCache) Rollback(context.Context, *cache.Entry) error { return nil }

type phaseLookupCache struct {
	*phaseCache
	lookupErr error
	lookups   int
}

func (c *phaseLookupCache) Lookup(context.Context, string, []interface{}, ...interface{}) (*cache.Entry, error) {
	c.lookups++
	return c.entry, c.lookupErr
}

func TestReadingDataCacheFailurePhasesSQLite(t *testing.T) {
	for _, mode := range []string{"get-error", "source-error", "lookup-error", "lookup-miss", "unsupported-lookup", "no-cache-only-miss", "get-success-prepare-error"} {
		t.Run(mode, func(t *testing.T) {
			h := sqlite.New(t)
			failure := errors.New("cache failure")
			base := &phaseCache{sourceErr: failure}
			var native cache.Cache = base
			only := false
			wantCalls := 1
			switch mode {
			case "get-error":
				base.getErr = failure
			case "source-error":
				base.entry = &cache.Entry{ReadCloser: &cache.ReadCloser{}, Meta: cache.Meta{Fields: []*cache.Field{{}}}}
			case "lookup-error":
				native = &phaseLookupCache{phaseCache: base, lookupErr: failure}
				only = true
			case "lookup-miss":
				native = &phaseLookupCache{phaseCache: base}
				only = true
			case "unsupported-lookup":
				only = true
			case "no-cache-only-miss":
				native = nil
				only = true
			case "get-success-prepare-error":
				wantCalls = 0
			}
			calls := 0
			owner := observability.NewRecorder(nil, observability.WithReadingData(func(_ string, _ time.Duration, _ string, _ int, _ []any, err error) { calls++; require.Error(t, err) }))
			s := &Session{recorder: owner}
			read := s.beginView(context.Background(), &data.View{Spec: spec.View{Name: "cache-errors"}})
			options := []sqlxread.Option{sqlxread.WithCacheOnly(only)}
			if native != nil {
				options = append(options, sqlxread.WithCache(native))
			}
			err := (rowRead{readCache: native, cacheOnly: only, newRow: func() any { return &cursorRow{} }, options: options}).query(context.Background(), rowQuery{db: h.DB, read: read, query: &cache.ParmetrizedQuery{SQL: "SELECT id FROM missing_records"}, visit: func(any) error { t.Fatal("unexpected row"); return nil }})
			require.Error(t, err)
			require.Equal(t, wantCalls, calls)
			require.Len(t, read.metric.Executions, 1)
			switch mode {
			case "get-error", "source-error", "lookup-error":
				require.ErrorIs(t, err, failure)
			case "lookup-miss", "no-cache-only-miss":
				require.ErrorIs(t, err, cache.ErrMiss)
			case "unsupported-lookup":
				require.ErrorIs(t, err, cache.ErrLookupUnsupported)
				require.Zero(t, base.gets)
			}
			if mode == "source-error" {
				require.Equal(t, 1, base.sources)
			}
		})
	}
}
