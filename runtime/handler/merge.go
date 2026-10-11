package handler

import (
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/moption"
)

// MergeDML queues the existing SQLX merge operation in the invocation journal.
// The result is populated when the operation is drained, like a buffered
// statement result. Transaction completion remains with the invocation owner.
// SQLX fetches its destination snapshot outside that transaction; callers must
// use a separate transaction when an earlier journal write would need to be
// visible to that fetch. Native DML rejects that uncommitted-prefix case.
type MergeDML interface {
	Merge(table string, rows any, config info.MergeConfig, result *info.MergeResult, options ...moption.Option) error
}
