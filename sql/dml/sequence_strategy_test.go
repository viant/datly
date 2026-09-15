package dml

import (
	"context"
	"testing"

	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/sqlx/metadata/info/dialect"
)

func TestSequenceStrategyConfiguration(t *testing.T) {
	for _, strategy := range []dialect.PresetIDStrategy{"", dialect.PresetIDWithReservation, dialect.PresetIDWithTransientTransaction} {
		t.Run(string(strategy), func(t *testing.T) {
			h := sqlite.New(t)
			ctx := context.Background()
			if err := h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT)"); err != nil {
				t.Fatal(err)
			}
			value, err := (Source{DB: h.DB, SequenceStrategy: strategy}).Open(ctx)
			if err != nil {
				t.Fatal(err)
			}
			data := value.(*Data)
			want := strategy
			if want == "" {
				want = dialect.PresetIDWithReservation
			}
			if data.sequenceStrategy != want {
				t.Fatalf("effective source strategy = %q, want %q", data.sequenceStrategy, want)
			}
			if err = data.BeginInvocation(); err != nil {
				t.Fatal(err)
			}
			row := &concurrentSequenceRow{Name: "strategy"}
			if err = data.Allocate(ctx, "records", row, "ID"); err != nil {
				t.Fatal(err)
			}
			if err = data.Insert("records", row); err != nil {
				t.Fatal(err)
			}
			if err = data.Complete(ctx, nil); err != nil {
				t.Fatal(err)
			}
			if row.ID != 1 {
				t.Fatal("configured native allocation lost")
			}
		})
	}
}
