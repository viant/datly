package dml

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
)

func TestCompletionPreservesRecordedFailureWithoutRepeatingIt(t *testing.T) {
	for _, mode := range []string{"same", "wrapped", "joined", "distinct"} {
		t.Run(mode, func(t *testing.T) {
			h := testharness.NewSQLiteHarness(t)
			ctx := context.Background()
			if err := h.ExecStatements(ctx, "CREATE TABLE completion_rows (id INTEGER PRIMARY KEY)"); err != nil {
				t.Fatal(err)
			}
			data := NewData(h.DB)
			if err := data.BeginInvocation(); err != nil {
				t.Fatal(err)
			}
			if err := data.Start(ctx); err != nil {
				t.Fatal(err)
			}
			if _, err := data.tx.ExecContext(ctx, "INSERT INTO completion_rows VALUES (1)"); err != nil {
				t.Fatal(err)
			}
			failure := errors.New("recorded operation failure")
			additional := errors.New("separate handler failure")
			data.markFailed(failure)
			var cause error
			switch mode {
			case "same":
				cause = failure
			case "wrapped":
				cause = fmt.Errorf("handler: %w", failure)
			case "joined":
				cause = errors.Join(additional, failure)
			case "distinct":
				cause = additional
			}
			err := data.Complete(ctx, cause)
			if !errors.Is(err, failure) || strings.Count(err.Error(), failure.Error()) != 1 {
				t.Fatalf("recorded cause duplicated or lost: %v", err)
			}
			if mode == "joined" || mode == "distinct" {
				if !errors.Is(err, additional) || strings.Count(err.Error(), additional.Error()) != 1 {
					t.Fatalf("separate cause lost or repeated: %v", err)
				}
			} else if err != cause {
				t.Fatalf("original cause identity lost: %v", err)
			}
			var count int
			if err := h.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM completion_rows").Scan(&count); err != nil {
				t.Fatal(err)
			}
			if count != 0 {
				t.Fatalf("failed invocation committed %d rows", count)
			}
		})
	}
}
