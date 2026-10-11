package ast

import "github.com/viant/datly/spec"

// StatementSelector is resolved against the generated contract authority.
// Path starts at Input or Output; no runtime expression or lookup is stored.
type StatementSelector struct {
	Path        FieldPath
	Type        spec.TypeRef
	Addressable bool
}

// StatementPlan preserves one authored buffered operation and its ordered bindings.
// Execution, allocation, batching and transaction policies remain service-owned.
type StatementPlan struct {
	// Method selects an existing buffered DML operation. Empty retains ExecuteWithResult.
	Method        string
	SQL           string
	SQLConstants  []string
	Arguments     []StatementSelector
	LastInsertID  StatementSelector
	MergeTable    StatementSelector
	MergeRows     StatementSelector
	MergeConfig   StatementSelector
	MergeResult   StatementSelector
	BatchTable    StatementSelector
	BatchRows     StatementSelector
	BatchSize     StatementSelector
	BatchAffected StatementSelector
}
