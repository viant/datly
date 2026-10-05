package handler

import (
	"context"
	"github.com/viant/datly/exec"
	xhandler "github.com/viant/xdatly/handler"
)

type Recovery int

const (
	RecoveryNone Recovery = iota
	// RecoveryAccept returns the output corrected by the business hook.
	RecoveryAccept
	// RecoveryRetry rebinds original request facts with fresh read dependencies.
	RecoveryRetry
)

// MutationOutcome retains actual transaction and execution evidence. Attempt
// is zero-based. The engine permits at most one replay of a single mutation.
type MutationOutcome struct {
	xhandler.Outcome
	Mutation   exec.MutationResult
	Attempt    int
	RetryLimit int
}

// MutationRecoverer is an opt-in bridge for generated writer business hooks.
// It is called only by a single-operation, locally owned completed root.
// Recover never owns transaction handles or executes manual DML.
type MutationRecoverer interface {
	SupportsMutationRecovery() bool
	RecoverMutation(context.Context, Invocation, any, MutationOutcome) (Recovery, error)
}

// TransactionRetryer explicitly opts a root graph into bounded replay after
// confirmed rollback. It can only request a fresh invocation, never accept
// partial output or acquire transaction ownership.
type TransactionRetryer interface {
	SupportsTransactionRetry() bool
	RetryTransaction(context.Context, Invocation, any, MutationOutcome) (bool, error)
}

// ScopedMutationRecoverer is a framework-only policy for automatically allocated
// scope values after a known root rollback. Caller transactions cannot enter it.
type ScopedMutationRecoverer interface {
	RecoverScopedMutation(context.Context, Invocation, exec.MutationReport, xhandler.Outcome) (bool, error)
	ScopedMutationRetryLimit() int
}

// MutationReplayContext preserves immutable insert classification across native
// retries; a newly arrived identity cannot become an UPDATE on the next attempt.
type MutationReplayContext interface {
	MutationReplayContext(context.Context, Invocation) context.Context
}
