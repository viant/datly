package reader

import (
	"context"
	"errors"
	"reflect"
	"testing"

	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/sqlx/metadata/info"
)

func TestComposedProjectionParameterBudgetSQLite(t *testing.T) {
	type row struct {
		ID int `sqlx:"id"`
	}
	for _, test := range []struct {
		name       string
		maximum    int
		invalidSQL bool
		wantError  bool
	}{
		{"exact combined budget", 3, false, false},
		{"combined frames exceed budget", 2, false, true},
		{"budget checked before SQL preparation", 2, true, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			h := sqlite.New(t)
			reader := &projectionReader{db: h.DB, dialect: &info.Dialect{MaxPlaceholders: test.maximum}}
			SQL := "SELECT a.id FROM (SELECT ? AS id) a JOIN (SELECT ? AS id) b ON a.id = b.id WHERE a.id > ?"
			if test.invalidSQL {
				SQL = "INVALID QUERY"
			}
			result, err := reader.ReadProjection(context.Background(), dexec.ProjectionRequest{SQL: SQL, Args: []any{7, 7, 0}, RowType: reflect.TypeFor[row]()})
			if test.wantError {
				var limit *parameterLimitError
				if !errors.As(err, &limit) || limit.actual != 3 || limit.maximum != 2 || result != nil {
					t.Fatalf("result=%v error=%v", result, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			rows := result.([]*row)
			if len(rows) != 1 || rows[0].ID != 7 {
				t.Fatalf("rows=%+v", rows)
			}
		})
	}
}
