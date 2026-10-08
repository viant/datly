package spec

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// Reconciliation declares one finite same-parent plan and a fixed role traversal.
// Holder order is both the allocation graph order and the native traversal order.
type Reconciliation struct {
	Mode       string               `json:"mode"`
	RootFields []string             `json:"rootFields,omitempty"`
	Roles      []ReconciliationRole `json:"roles"`
}
type ReconciliationRole struct {
	Holder        string   `json:"holder"`
	Fields        []string `json:"fields,omitempty"`
	AdoptIdentity bool     `json:"adoptIdentity,omitempty"`
}

func ParseReconciliation(text string) (*Reconciliation, error) {
	var result Reconciliation
	decoder := json.NewDecoder(bytes.NewBufferString(text))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil {
		return nil, fmt.Errorf("finite_reconciliation: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return nil, fmt.Errorf("finite_reconciliation requires one descriptor")
	}
	if result.Mode != "same-parent-root-first" || len(result.Roles) == 0 {
		return nil, fmt.Errorf("finite_reconciliation requires same-parent-root-first and declared leaf roles")
	}
	seen := map[string]bool{}
	for _, role := range result.Roles {
		if role.Holder == "" || seen[role.Holder] {
			return nil, fmt.Errorf("finite_reconciliation has an empty or duplicate holder")
		}
		seen[role.Holder] = true
	}
	return &result, nil
}
func (r *Reconciliation) Clone() *Reconciliation {
	if r == nil {
		return nil
	}
	out := *r
	out.RootFields = append([]string(nil), r.RootFields...)
	out.Roles = append([]ReconciliationRole(nil), r.Roles...)
	for i := range out.Roles {
		out.Roles[i].Fields = append([]string(nil), r.Roles[i].Fields...)
	}
	return &out
}

// ValidateReconciliationView rejects unsupported policies before generation.
// Runtime compilation additionally verifies canonical Go fields and loaded keys.
func ValidateReconciliationView(root *View, operation string) error {
	if root == nil {
		return nil
	}
	seenViews := map[*View]bool{}
	var visit func(*View) error
	visit = func(view *View) error {
		if view == nil || seenViews[view] {
			return nil
		}
		seenViews[view] = true
		if view != root && view.Reconciliation != nil {
			return fmt.Errorf("finite_reconciliation cannot be declared on a descendant")
		}
		for _, rel := range view.Relations {
			if rel != nil {
				if e := visit(rel.View); e != nil {
					return e
				}
			}
		}
		return nil
	}
	if e := visit(root); e != nil {
		return e
	}
	config := root.Reconciliation
	if config == nil {
		return nil
	}
	if config.Mode != "same-parent-root-first" || operation != "patch" || root.Auxiliary || root.SelfReference != nil || root.EntityHooks == "" || len(config.Roles) == 0 || len(config.Roles) != len(root.Relations) {
		return fmt.Errorf("finite_reconciliation requires a physical PATCH root with lifecycle and declared direct leaves")
	}
	if err := validateReconciliationFields(root, config.RootFields); err != nil {
		return err
	}
	views := []*View{root}
	seen := map[string]bool{}
	adoptions := 0
	for i, role := range config.Roles {
		rel := root.Relations[i]
		if rel == nil || rel.View == nil || role.Holder == "" || seen[role.Holder] || role.Holder != rel.Holder || rel.Cardinality == CardinalityOne || rel.View.Cardinality == CardinalityOne || rel.View.Auxiliary || rel.View.SelfReference != nil || len(rel.View.Relations) != 0 {
			return fmt.Errorf("finite_reconciliation requires every direct writable leaf holder in declared graph order")
		}
		if err := validateReconciliationFields(rel.View, role.Fields); err != nil {
			return err
		}
		seen[role.Holder] = true
		if role.AdoptIdentity {
			adoptions++
		}
		views = append(views, rel.View)
	}
	if adoptions > 1 {
		return fmt.Errorf("finite_reconciliation admits one identity adoption role")
	}
	for _, view := range views {
		if view.WriterIdentityPolicy != "" || view.WriterActionPolicy != "" || view.QueueContract != "" || view.MutationPredicateGroup != nil || view.OnDeleteNotFound != "" || view.InMemory {
			return fmt.Errorf("finite_reconciliation unsupported identity/action/queue/mutation policy combination")
		}
		for _, column := range view.Columns {
			if column != nil && (column.DeleteMarker || column.ConcurrencyToken || containsScopedSequence(column.Tag)) {
				return fmt.Errorf("finite_reconciliation does not support markers, concurrency tokens or scoped sequences")
			}
		}
	}
	return nil
}
func containsScopedSequence(tag string) bool {
	return strings.Contains(tag, "sequenceScope") || strings.Contains(tag, "sequence:\"")
}

func validateReconciliationFields(view *View, fields []string) error {
	seen := map[string]bool{}
	for _, name := range fields {
		if name == "" || seen[name] {
			return fmt.Errorf("finite_reconciliation empty or duplicate field declaration")
		}
		seen[name] = true
		if len(view.Columns) == 0 {
			continue
		} // provisional discovery shape only
		var selected *Column
		normalize := func(text string) string { return strings.ToLower(strings.ReplaceAll(text, "_", "")) }
		for _, column := range view.Columns {
			if column != nil && (normalize(column.Name) == normalize(name) || normalize(column.Output) == normalize(name)) {
				if selected != nil {
					return fmt.Errorf("finite_reconciliation ambiguous field %s", name)
				}
				selected = column
			}
		}
		if selected == nil || selected.PrimaryKey || selected.AutoIncrement {
			return fmt.Errorf("finite_reconciliation unknown or identity field %s", name)
		}
	}
	return nil
}
