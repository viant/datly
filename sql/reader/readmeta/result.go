package readmeta

import (
	"fmt"
	"strings"
)

// Record describes one final row ordinal and its actual holder attachments.
// Source tokens remain inside the collector; no reconstructed view identity is
// exposed from incomplete structural metadata.
type Record struct {
	fields    *Fields
	relations map[string][]*Record
}

func NewRecord(fields *Fields, relations map[string][]*Record) *Record {
	result := &Record{fields: fields, relations: make(map[string][]*Record, len(relations))}
	for name, rows := range relations {
		result.relations[name] = append([]*Record(nil), rows...)
	}
	return result
}
func (r *Record) Fields() *Fields {
	if r == nil {
		return nil
	}
	return r.fields
}
func (r *Record) Relation(holder string) ([]*Record, bool) {
	if r == nil {
		return nil, false
	}
	rows, ok := r.relations[holder]
	return append([]*Record(nil), rows...), ok
}

// RelationRow returns immutable evidence without copying the sibling collection.
func (r *Record) RelationRow(holder string, index int) (*Record, error) {
	if holder == "" {
		return nil, fmt.Errorf("read relation holder is required")
	}
	if r == nil {
		return nil, fmt.Errorf("read relation %s provenance is unknown", holder)
	}
	rows, known := r.relations[holder]
	if !known {
		return nil, fmt.Errorf("read relation %s provenance is unknown", holder)
	}
	if index < 0 || index >= len(rows) {
		return nil, fmt.Errorf("read relation %s ordinal %d is out of range", holder, index)
	}
	return rows[index], nil
}

// Result addresses root rows by final output ordinal. RootHolder is empty for
// direct outputs; a one-valued output has zero or one root record.
type Result struct {
	rootHolder string
	direct     bool
	rows       []*Record
	outputs    map[string]*Result
}

// WithOutputs composes completed output slots without mutating either result
// or caller-owned collections. Slots are canonical Go holder paths, not view
// aliases. A slot with no rows is known empty; unknown rows contain nil Fields.
func (r *Result) WithOutputs(slots map[string][]*Record) (*Result, error) {
	if r == nil {
		return nil, fmt.Errorf("read result is required")
	}
	if r.direct && len(slots) != 0 {
		return nil, fmt.Errorf("direct read result has no output envelope slots")
	}
	result := *r
	result.outputs = make(map[string]*Result, len(r.outputs)+len(slots))
	for holder, value := range r.outputs {
		result.outputs[holder] = value
	}
	for holder, rows := range slots {
		if holder == "" || strings.TrimSpace(holder) != holder {
			return nil, fmt.Errorf("canonical output holder is required")
		}
		if holder == r.rootHolder {
			return nil, fmt.Errorf("output holder %s conflicts with the root view", holder)
		}
		if _, exists := result.outputs[holder]; exists {
			return nil, fmt.Errorf("duplicate output holder %s", holder)
		}
		result.outputs[holder] = NewResult(holder, false, rows)
	}
	return &result, nil
}

// Output selects immutable evidence for an exact output holder. The existing
// Row/Rows methods continue to address only the main root view.
func (r *Result) Output(holder string) (*Result, error) {
	if r == nil {
		return nil, fmt.Errorf("read result is required")
	}
	if holder == r.rootHolder && (holder != "" || r.direct) {
		return r, nil
	}
	if result, ok := r.outputs[holder]; ok {
		return result, nil
	}
	return nil, fmt.Errorf("read output holder %q provenance is unavailable", holder)
}

func NewResult(rootHolder string, direct bool, rows []*Record) *Result {
	return &Result{rootHolder: rootHolder, direct: direct, rows: append([]*Record(nil), rows...)}
}
func (r *Result) RootHolder() string {
	if r == nil {
		return ""
	}
	return r.rootHolder
}
func (r *Result) DirectOutput() bool { return r != nil && r.direct }
func (r *Result) Rows() []*Record {
	if r == nil {
		return nil
	}
	return append([]*Record(nil), r.rows...)
}

// Row returns immutable evidence in constant time without copying all roots.
func (r *Result) Row(index int) (*Record, error) {
	if r == nil || index < 0 || index >= len(r.rows) {
		return nil, fmt.Errorf("read root ordinal %d is out of range", index)
	}
	return r.rows[index], nil
}
