package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/viant/datly/sql/dml"
	"github.com/viant/sqlx/testutil/reservationdb"
)

func TestSequenceStrategyOmittedRootNativeDefaultLive(t *testing.T) {
	for _, testCase := range []struct {
		driver    string
		matching  string
		differing string
	}{
		{driver: "mysql", matching: "transient", differing: "reservation"},
		{driver: "postgres", matching: "reservation", differing: "transient"},
	} {
		t.Run(testCase.driver, func(t *testing.T) {
			var source dml.Source
			if testCase.driver == "mysql" {
				database := reservationdb.OpenTransient(t)
				source = dml.Source{DB: database.DB}
			} else {
				database := reservationdb.Open(t, testCase.driver)
				source = dml.Source{DB: database.DB}
			}

			ctx := context.Background()
			root, _ := invocationDataScope(ctx, source)
			opened, err := root.resolve(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if opened == nil || root.sequenceResolved != testCase.matching {
				t.Fatalf("native default = %q, want %q", root.sequenceResolved, testCase.matching)
			}
			rootData := root.data

			matching, _ := invocationDataScope(withDataScope(ctx, root), source)
			matching.sequenceStrategy = testCase.matching
			if data, resolveErr := matching.resolve(ctx); resolveErr != nil || data == nil || matching.unit != root {
				t.Fatalf("matching child did not share root: data=%T err=%v", data, resolveErr)
			}

			conflicting, _ := invocationDataScope(withDataScope(ctx, root), source)
			conflicting.sequenceStrategy = testCase.differing
			if _, resolveErr := conflicting.resolve(ctx); !errors.Is(resolveErr, ErrSequenceStrategyConflict) {
				t.Fatalf("differing child did not conflict: %v", resolveErr)
			}
			if root.data != rootData || root.sequenceResolved != testCase.matching {
				t.Fatal("conflicting child switched or reopened the root")
			}
			_ = root.complete(ctx, errors.New("test cleanup"))
		})
	}
}
