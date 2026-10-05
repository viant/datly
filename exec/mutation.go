package exec

import xhandler "github.com/viant/xdatly/handler"

// MutationReporterKey binds detached, invocation-scoped execution evidence.
const MutationReporterKey xhandler.ValueKey = "mutationReporter"

// MutationResult is detached execution evidence, not a planned write or a
// transaction handle. Batched inserts report an aggregate count and Records.
type MutationResult struct {
	Operation string
	Table     string
	Records   int
	Affected  int64
	Error     error
	// Contention is classified from a supported driver's transaction/snapshot
	// error code, never from a message substring or a proposed write.
	Contention bool
}

// MutationReport includes every queued operation, including unexecuted work.
// Recovery must not infer a single-operation invocation from partial results.
type MutationReport struct {
	// Contention describes the completed invocation's driver-coded failure,
	// including failures before DML was queued. It is not inferred from intent.
	Contention bool
	Queued     int
	Results    []MutationResult
	// Nested records whether any queued operation belongs to a child frame.
	Nested bool
}

// TransactionContentionClassifier interprets errors under the database unit's
// resolved dialect authority, without giving recovery hooks a DB handle.
type TransactionContentionClassifier interface {
	TransactionContention(error) bool
}

type MutationReporter interface {
	MutationReport() MutationReport
}
