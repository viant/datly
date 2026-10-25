package reader

import (
	"context"
	"testing"

	"github.com/viant/datly/data"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/observability"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/sqlx/io/read/cache"
)

func TestRowQueryReportsOptedInInclusivePhaseWithoutSQL(t *testing.T) {
	h := sqlite.New(t)
	for _, optedIn := range []bool{false, true} {
		ctx := context.Background()
		if optedIn {
			ctx = observability.WithToolTiming(ctx)
		}
		session := &Session{recorder: observability.NewRecorder(nil)}
		read := session.beginView(ctx, &data.View{Spec: spec.View{Name: "timed"}})
		err := (rowRead{newRow: func() any { return &cursorRow{} }}).query(ctx, rowQuery{
			db: h.DB, read: read,
			query: &cache.ParmetrizedQuery{SQL: "SELECT 7 AS id"},
			visit: func(any) error { return nil },
		})
		if err != nil {
			t.Fatal(err)
		}
		got := observability.ToolTimingSnapshot(ctx)
		if !optedIn {
			if got != nil {
				t.Fatalf("ordinary query emitted timing: %#v", got)
			}
			continue
		}
		if len(got) != 1 || got["queryall_inclusive"].Calls != 1 || got["queryall_inclusive"].DurationMs < 0 {
			t.Fatalf("inclusive query timing = %#v", got)
		}
	}
}

func TestConnectorResolutionFailureStillRecordsOptedInPhase(t *testing.T) {
	ctx := observability.WithToolTiming(context.Background())
	_, err := viewConnection(ctx, &Session{SQL: &dsql.SQLComponent{}}, &ViewPlan{View: &data.View{Spec: spec.View{Name: "timed"}}})
	if err == nil {
		t.Fatal("expected missing connector database")
	}
	got := observability.ToolTimingSnapshot(ctx)
	if len(got) != 1 || got["connector_resolve"].Calls != 1 {
		t.Fatalf("connector resolution timing = %#v", got)
	}
}
