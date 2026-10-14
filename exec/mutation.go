package exec

import xhandler "github.com/viant/xdatly/handler"

const MutationReporterKey = xhandler.MutationReporterKey

type MutationResult = xhandler.MutationResult
type MutationReport = xhandler.MutationReport
type MutationReporter = xhandler.MutationReporter

// TransactionContentionClassifier interprets errors under the database unit's
// resolved dialect authority, without giving recovery hooks a DB handle.
type TransactionContentionClassifier interface {
	TransactionContention(error) bool
}
