// Package ast defines the target-neutral semantic tree used to generate Go and
// Velty handlers. It contains no canonical-metadata compiler or target syntax.
package ast

import "github.com/viant/datly/spec"

// Operation identifies generated write-handler behavior.
type Operation string

const (
	OperationPost  Operation = "post"
	OperationPut   Operation = "put"
	OperationPatch Operation = "patch"
)

// Action identifies one DML decision in a write policy.
type Action string

const (
	ActionInsert Action = "insert"
	ActionUpdate Action = "update"
)

// FieldPath is a prevalidated generated-contract selector path.
type FieldPath []string

// ContractRef identifies one canonical parameter and its generated field.
type ContractRef struct {
	ParamIdentity string
	Path          FieldPath
	Cardinality   spec.Cardinality
}

// KeyPart describes one ordered canonical record key.
type KeyPart struct {
	Field  string
	Source string
	Type   spec.TypeRef
}

// SequencePlan describes identifier allocation through the injected sequencer.
type SequencePlan struct {
	Destination FieldPath
	Selector    FieldPath
	// Field is the canonical local row field, independent of root traversal syntax.
	Field FieldRef
}

// LinkConversion identifies the typed assignment needed for relation keys and
// current-to-entity field projections.
// Velty dereferences transparently; the Go target consumes this explicitly.
type LinkConversion string

const (
	LinkDirect      LinkConversion = "direct"
	LinkAddress     LinkConversion = "address"
	LinkDereference LinkConversion = "dereference"
)

// KeyLink describes one ordered parent-to-child relation key assignment.
type KeyLink struct {
	Parent          KeyPart
	Child           KeyPart
	ParentNamespace string
	ChildNamespace  string
	Conversion      LinkConversion
}

// CurrentPlan describes the ordinary input datapoint used for PATCH lookup.
// LookupProjection names a pure typed projection for a generated auxiliary
// read. The enclosing relation supplies identity and value-link semantics.
type LookupProjection struct {
	Name    string
	Columns []string
}

func (p *LookupProjection) Clone() *LookupProjection {
	if p == nil {
		return nil
	}
	result := *p
	result.Columns = append([]string(nil), p.Columns...)
	return &result
}

type CurrentPlan struct {
	Lookup        *LookupProjection
	ParamIdentity string
	ViewIdentity  string
	InputPath     FieldPath
	Keys          []KeyPart
	// Fields is the declared current-to-entity projection, not evidence that a
	// particular read selected these fields. PreviousFields requires read provenance.
	Fields []CurrentField
	// Self names the actual read row's self-reference holders, not entity edges.
	Self []FieldRef
}

// FieldRef describes a canonical scalar field without assigning key semantics.
type FieldRef struct {
	Field  string
	Source string
	Type   spec.TypeRef
}

// CurrentField describes one typed assignment from a database current row into
// the entity-shaped Previous value. Dereference requires a non-nil source;
// consumers must not silently turn a loaded SQL NULL into a scalar zero.
type CurrentField struct {
	Current    FieldRef
	Entity     FieldRef
	Conversion LinkConversion
}

// WritePolicy makes every root DML decision explicit for target lowering.
type WritePolicy struct {
	ValuePath FieldPath
	Existing  Action
	Missing   Action
	Allowed   []Action
	Order     int
}

// RelationPlan describes one typed child traversal from a parent record.
type RelationPlan struct {
	Identity    string
	FieldPath   FieldPath
	Cardinality spec.Cardinality
	Links       []KeyLink
	Child       *RecordPlan
}

// SelfRelationPlan describes holder-specific linking within the same row type.
// It has no child plan: the existing record role owns recursive traversal.
type SelfRelationPlan struct {
	FieldPath FieldPath
	Links     []KeyLink
}

// RecordPlan describes one canonical record write and its ordered children.
type RecordPlan struct {
	Identity string
	// Auxiliary records remain available to business reads/indexes, but are
	// excluded from generated sequencing and mutation traversal.
	Auxiliary   bool
	InputPath   FieldPath
	Current     *CurrentPlan
	Table       string
	Cardinality spec.Cardinality
	Keys        []KeyPart
	// PresenceFields are generated-view fields whose programmatic assignments
	// must be exposed to SQLX PATCH updates through the generated Has marker.
	// Package-linked record authority leaves this empty.
	PresenceFields []string
	Entity         *EntityPlan
	Sequence       *SequencePlan
	Write          WritePolicy
	Relations      []*RelationPlan
	SelfRelations  []SelfRelationPlan
}

// TracksPresence reports whether generated business logic owns the presence
// bit for field. The slice keeps the semantic plan deterministic and portable
// across target lowerers.
func (r *RecordPlan) TracksPresence(field string) bool {
	if r == nil {
		return false
	}
	for _, candidate := range r.PresenceFields {
		if candidate == field {
			return true
		}
	}
	return false
}

// IdentityKeys returns the final entity identity when refined, otherwise the
// source-view identity. Callers must treat the returned metadata as immutable.
func (r *RecordPlan) IdentityKeys() []KeyPart {
	if r == nil {
		return nil
	}
	if r.Entity != nil && r.Entity.Keys != nil {
		return r.Entity.Keys
	}
	return r.Keys
}

// Plan is the immutable target-neutral input to handler target lowering.
type Plan struct {
	Operation Operation
	Input     ContractRef
	Output    *ContractRef
	Root      *RecordPlan
}
