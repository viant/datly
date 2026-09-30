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
	Queued  int
	Results []MutationResult
	// Nested records whether any queued operation belongs to a child frame.
	Nested bool
}

type MutationReporter interface {
	MutationReport() MutationReport
}
