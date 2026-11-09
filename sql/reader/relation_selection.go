package reader

import (
	"context"
	"fmt"
	"strings"

	"github.com/viant/datly/data"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/sql/reader/collector"
)

type relationSelection struct {
	view    *data.View
	holders []string
}

func (e *Execution) ValidateRelationSelection(ctx context.Context) error {
	return e.session().configureRelationSelection(ctx)
}

func (s *Session) configureRelationSelection(ctx context.Context) error {
	s.relationSelection = nil
	s.omittedOutput = ""
	if name, holder, omit := dexec.OmittedDerivedOutput(ctx, s.Component.Key); omit {
		view := s.Artifact.Root.View
		if view.Spec.CanonicalName() == name {
			for _, relation := range view.Relations {
				if relation.IsOutput() && relation.Holder == holder && holder != "" {
					s.omittedOutput = holder
					return nil
				}
			}
		}
		return fmt.Errorf("unknown derived output holder %q on root view %q", holder, name)
	}
	name, names, present := dexec.SelectedRelations(ctx, s.Component.Key)
	if !present {
		return nil
	}
	view, err := s.Artifact.ViewIndex.Resolve(name)
	if err != nil {
		return fmt.Errorf("relation selection: %w", err)
	}
	if view.Spec.CanonicalName() != name {
		return fmt.Errorf("relation selection requires canonical view %q", name)
	}
	selected := &relationSelection{view: view}
	for _, name := range names {
		field, ambiguous := resolveSelectorField(view, name)
		if ambiguous {
			return fmt.Errorf("ambiguous relation selection %q", name)
		}
		if field == nil || !field.Holder {
			return fmt.Errorf("unknown public relation selection %q", name)
		}
		holder := field.GoName
		found := false
		for _, relation := range view.Relations {
			if relation != nil && !relation.IsOutput() && strings.EqualFold(relation.Holder, holder) {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("unknown authored relation selection %q", name)
		}
		selected.holders = append(selected.holders, holder)
	}
	s.relationSelection = selected
	return nil
}

func (s *Session) relationCollectors(parent *collector.Collector, selector *collector.Statelet) []*collector.Collector {
	selection := s.relationSelection
	if selection == nil || selection.view != parent.View() {
		return parent.Relations(selector)
	}
	if len(selection.holders) == 0 {
		return nil
	}
	// Intersect with ordinary field selection: a trusted relation restriction
	// cannot expand the source's requested graph or add missing grouping keys.
	holders := make([]string, 0, len(selection.holders))
	for _, holder := range selection.holders {
		if selector == nil || selector.Has(holder) {
			holders = append(holders, holder)
		}
	}
	if len(holders) == 0 {
		return nil
	}
	return parent.Relations(&collector.Statelet{Columns: holders})
}
