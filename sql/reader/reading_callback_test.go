package reader

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/viant/datly/data"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/observability"
	"github.com/viant/datly/spec"
	"github.com/viant/sqlx/io/read/cache"
)

func TestReadingDataVisitFailureAndPanicSQLite(t *testing.T) {
	for _, mode := range []string{"success", "visit-error", "panic", "disabled"} {
		t.Run(mode, func(t *testing.T) {
			h := sqlite.New(t)
			ctx := context.Background()
			type row struct {
				ID int `sqlx:"id"`
			}
			calls := 0
			failure := errors.New("visitor failure")
			var callback observability.ReadingData
			if mode != "disabled" {
				callback = func(view string, duration time.Duration, query string, count int, args []any, err error) {
					calls++
					if view != "records" || duration <= 0 || query != "SELECT ? AS id" || count != 1 || len(args) != 1 || args[0] != 7 {
						t.Fatal("callback lost native inputs")
					}
					if (err != nil) != (mode == "visit-error") || (err != nil && !errors.Is(err, failure)) {
						t.Fatalf("callback error=%v", err)
					}
				}
			}
			owner := observability.NewRecorder(nil, observability.WithReadingData(callback))
			session := &Session{recorder: owner}
			read := session.beginView(ctx, &data.View{Spec: spec.View{Name: "records"}})
			query := rowQuery{db: h.DB, read: read, query: &cache.ParmetrizedQuery{SQL: "SELECT ? AS id", Args: []any{7}}, visit: func(any) error {
				if mode == "panic" {
					panic(failure)
				}
				if mode == "visit-error" {
					return failure
				}
				return nil
			}}
			var err error
			func() {
				defer func() {
					if p := recover(); p != nil && (mode != "panic" || p != failure) {
						t.Fatalf("unexpected panic: %v", p)
					}
				}()
				err = (rowRead{newRow: func() any { return &row{} }}).query(ctx, query)
			}()
			want := 1
			if mode == "panic" || mode == "disabled" {
				want = 0
			}
			if calls != want {
				t.Fatalf("callbacks=%d want=%d", calls, want)
			}
			if mode == "visit-error" && !errors.Is(err, failure) {
				t.Fatal(err)
			}
			if len(read.metric.Executions) != 1 {
				t.Fatal("native capture lost")
			}
			if mode == "panic" && read.metric.Executions[0].Error == "" {
				t.Fatal("panic capture lost")
			}
		})
	}
}
