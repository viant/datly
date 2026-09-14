package reader

import (
	"errors"
	"fmt"

	"github.com/viant/sqlx/metadata/info"
)

type parameterLimitError struct{ actual, maximum int }

func (e *parameterLimitError) Error() string {
	return fmt.Sprintf("query has %d bind parameters, exceeding dialect limit %d", e.actual, e.maximum)
}

type parameterBudget struct{ dialect *info.Dialect }

func (b parameterBudget) check(count int) error {
	maximum := b.dialect.MaxPlaceholderCount()
	if count > maximum {
		return &parameterLimitError{actual: count, maximum: maximum}
	}
	return nil
}

// readWithinBudget only retries a query that failed preflight, before scanning
// any rows. Whole key tuples stay together and splitting remains within the
// existing worker rather than multiplying fetch concurrency.
func (r *relationRead) readWithinBudget(batch relationBatch, columns []string) error {
	err := r.readBatch(batch.placeholders, batch.composite, columns)
	var limit *parameterLimitError
	if !errors.As(err, &limit) {
		return err
	}
	count := len(batch.placeholders)
	if len(batch.composite) > 0 {
		count = len(batch.composite)
	}
	if count <= 1 {
		return fmt.Errorf("relation %s cannot fit one key tuple: %w", viewName(r.plan.View), err)
	}
	return r.splitWithinBudget(batch, columns, count)
}

func (r *relationRead) splitWithinBudget(batch relationBatch, columns []string, count int) error {
	middle := count / 2
	left, right := relationBatch{}, relationBatch{}
	if len(batch.composite) > 0 {
		left.composite, right.composite = batch.composite[:middle], batch.composite[middle:]
	} else {
		left.placeholders, right.placeholders = batch.placeholders[:middle], batch.placeholders[middle:]
	}
	if err := r.ctx.Err(); err != nil {
		return err
	}
	if err := r.readWithinBudget(left, columns); err != nil {
		return err
	}
	return r.readWithinBudget(right, columns)
}
