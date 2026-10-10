package ast

import "github.com/viant/datly/spec"

// StatementSelector is resolved against the generated contract authority.
// Path starts at Input or Output; no runtime expression or lookup is stored.
type StatementSelector struct {
	Path        FieldPath
	Type        spec.TypeRef
	Addressable bool
}

// StatementPlan preserves one authored SQL statement and its ordered bindings.
// It has no record classification, allocator, batching, or retry policy.
type StatementPlan struct {
	SQL          string
	Arguments    []StatementSelector
	LastInsertID StatementSelector
}
