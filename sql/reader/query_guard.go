package reader

import (
	"context"
	"database/sql"

	dexec "github.com/viant/datly/exec"
)

// WithQueryGuard returns a detached reader that admits each fully bound SQL
// statement before opening its row reader. Root, relation batches, partitions,
// derived outputs and warmup retain their native read/population boundaries. The guard
// must be concurrency-safe; it must not modify SQL or arguments or execute the
// business query. It receives the source connector for metadata-only admission.
func (e *Execution) WithQueryGuard(guard func(context.Context, *sql.DB, string, []any) error) dexec.Reader {
	if e == nil {
		return e
	}
	result := *e
	result.config.queryGuard = guard
	return &result
}
