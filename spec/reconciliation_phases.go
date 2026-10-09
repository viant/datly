package spec

import (
	"fmt"
	"strings"
)

// SourcePhases is a fixed authoring schedule, not a hook-returned action list.
// Parsing/compiling its internal metadata does not enable runtime admission.
type ReconciliationSourcePhases struct {
	Insert []ReconciliationPhase `json:"insert"`
	Update []ReconciliationPhase `json:"update"`
}
type ReconciliationPhase struct {
	Name        string                      `json:"name"`
	Holder      string                      `json:"holder"`
	Scope       string                      `json:"scope"`
	Workflow    string                      `json:"workflow"`
	UpdateBasis string                      `json:"updateBasis,omitempty"`
	Followup    *ReconciliationRootFollowup `json:"followup,omitempty"`
}
type ReconciliationRootFollowup struct {
	Placement string   `json:"placement"`
	Fields    []string `json:"fields"`
}

func (s *ReconciliationSourcePhases) Clone() *ReconciliationSourcePhases {
	if s == nil {
		return nil
	}
	clone := func(in []ReconciliationPhase) []ReconciliationPhase {
		out := append([]ReconciliationPhase(nil), in...)
		for i := range out {
			if in[i].Followup != nil {
				copy := *in[i].Followup
				copy.Fields = append([]string(nil), copy.Fields...)
				out[i].Followup = &copy
			}
		}
		return out
	}
	return &ReconciliationSourcePhases{Insert: clone(s.Insert), Update: clone(s.Update)}
}

func (r *Reconciliation) ValidateSourcePhases() error {
	if r == nil || r.Mode != "source-phases" || r.RootAction != "all-supplied-positive-keys" || r.SourcePhases == nil {
		return fmt.Errorf("source phases require the source-phases root decision")
	}
	holders := map[string]bool{}
	for _, role := range r.Roles {
		if role.Holder == "" || holders[role.Holder] {
			return fmt.Errorf("source phases require unique declared holders")
		}
		holders[role.Holder] = true
	}
	fields := map[string]bool{}
	for _, name := range r.RootFields {
		fields[name] = true
	}
	for _, branch := range [][]ReconciliationPhase{r.SourcePhases.Insert, r.SourcePhases.Update} {
		if len(branch) == 0 {
			return fmt.Errorf("source phases require both finite branch schedules")
		}
		names := map[string]bool{}
		followups := 0
		for _, phase := range branch {
			if phase.Name == "" || strings.TrimSpace(phase.Name) != phase.Name || phase.Name == "root" || names[phase.Name] || !holders[phase.Holder] {
				return fmt.Errorf("source phases require unique named phases and declared holders")
			}
			names[phase.Name] = true
			if phase.Scope != "all-roots" && phase.Scope != "each-root" {
				return fmt.Errorf("source phase %s has unsupported scope", phase.Name)
			}
			updates := false
			switch phase.Workflow {
			case "current-deletes", "working-inserts", "current-deletes-then-working-inserts":
			case "working-updates-then-inserts", "working-updates-current-deletes-then-inserts":
				updates = true
			default:
				return fmt.Errorf("source phase %s has unsupported workflow", phase.Name)
			}
			if updates {
				if phase.UpdateBasis != "working" && phase.UpdateBasis != "current" {
					return fmt.Errorf("source phase %s requires a declared update basis", phase.Name)
				}
			} else if phase.UpdateBasis != "" {
				return fmt.Errorf("source phase %s has no update projection", phase.Name)
			}
			if phase.Followup != nil {
				followups++
				if followups > 1 || phase.Scope != "each-root" || phase.Workflow == "current-deletes" || len(phase.Followup.Fields) == 0 || (phase.Followup.Placement != "after-group" && phase.Followup.Placement != "after-phase") {
					return fmt.Errorf("source phases admit one bounded per-root followup phase")
				}
				seen := map[string]bool{}
				for _, name := range phase.Followup.Fields {
					if name == "" || !fields[name] || seen[name] {
						return fmt.Errorf("source phase %s has an undeclared or duplicate followup field", phase.Name)
					}
					seen[name] = true
				}
			}
		}
	}
	return nil
}
