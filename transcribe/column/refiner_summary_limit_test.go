package column

import (
	"context"
	"reflect"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
)

func TestSummaryDiscoveryUsesNonzeroPagingLimit(t *testing.T) {
	ctx := context.Background()
	query, err := evaluateSource(ctx, &spec.ViewSource{SQL: "SELECT COUNT(*) / $View.Limit AS pages FROM ($View.NonWindowSQL) parent"}, nil,
		&TemplateInput{Value: reflect.ValueOf(struct{}{})}, &expandedQuery{SQL: "SELECT 1 AS id UNION ALL SELECT 2 AS id"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	harness := testharness.NewSQLiteHarness(t)
	var pages int
	if err := harness.DB.QueryRowContext(ctx, query.SQL, query.Args...).Scan(&pages); err != nil {
		t.Fatalf("discovery summary must not divide by zero: %v; SQL=%s", err, query.SQL)
	}
	if pages != 2 {
		t.Fatalf("expected non-null representative page count, got %d", pages)
	}
}
