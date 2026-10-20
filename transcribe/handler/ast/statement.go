package ast

import "github.com/viant/datly/spec"

// StatementSelector is resolved against the generated contract authority.
// Path starts at Input or Output; no runtime expression or lookup is stored.
type StatementSelector struct {
	Path        FieldPath
	Type        spec.TypeRef
	Addressable bool
	// PointerParents are proven pointer prefixes dereferenced by this selector.
	PointerParents []FieldPath
}

// StatementPlan preserves one authored buffered operation and its ordered bindings.
// Execution, allocation, batching and transaction policies remain service-owned.
type StatementPlan struct {
	// Method selects an existing buffered DML operation. Empty retains ExecuteWithResult.
	// CollectionOrdinals are the zero-based positional bindings expanded for []int.
	CollectionOrdinals []int
	Method             string
	SQL                string
	SQLConstants       []string
	Arguments          []StatementSelector
	LastInsertID       StatementSelector
	MergeTable         StatementSelector
	MergeRows          StatementSelector
	MergeConfig        StatementSelector
	MergeResult        StatementSelector
	BatchTable         StatementSelector
	BatchRows          StatementSelector
	BatchSize          StatementSelector
	BatchAffected      StatementSelector

	// BatchOnDuplicateKeySQL is present only for an authored fifth string literal.
	BatchOnDuplicateKeySQL *string
}
