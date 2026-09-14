package ast

import "github.com/viant/datly/spec"

// EntityField describes a canonical field whose presence is owned by the
// mutable entity contract. Identity presence does not authorize an UPDATE SET.
type EntityField struct {
	Name      string
	Path      FieldPath
	Type      spec.TypeRef
	Identity  bool
	Relation  bool
	Self      bool
	Writable  bool
	Invariant string
}

// InvariantGroup is a normalized group of business fields whose omitted values
// may be hydrated from the matched database projection before validation.
type InvariantGroup struct {
	Name   string
	Fields []string
}

// LateWriteEffects describes native row behavior executed during write flush,
// after generic mutation business validation has already completed.
type LateWriteEffects struct {
	Unresolved    string
	InsertHook    bool
	UpdateHook    bool
	DefaultFields []string
}

// EntityPlan carries immutable generation facts for typed setters and original
// presence capture. Hook invocation phases are separate handler semantics.
type EntityPlan struct {
	Type          spec.TypeRef
	MarkerField   string
	MarkerPointer bool
	Owned         bool
	MarkerType    spec.TypeRef
	Fields        []EntityField
	Keys          []KeyPart
	Invariants    []InvariantGroup
	Hooks         spec.TypeRef
	HooksBind     bool
	LateWrite     LateWriteEffects
}

func (p *EntityPlan) Clone() *EntityPlan {
	if p == nil {
		return nil
	}
	result := *p
	result.LateWrite.DefaultFields = append([]string(nil), p.LateWrite.DefaultFields...)
	result.Fields = append([]EntityField(nil), p.Fields...)
	for index := range result.Fields {
		result.Fields[index].Path = append(FieldPath(nil), p.Fields[index].Path...)
	}
	result.Keys = append([]KeyPart(nil), p.Keys...)
	result.Invariants = append([]InvariantGroup(nil), p.Invariants...)
	for i := range result.Invariants {
		result.Invariants[i].Fields = append([]string(nil), p.Invariants[i].Fields...)
	}
	return &result
}
