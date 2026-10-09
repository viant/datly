package writer

import (
	"fmt"
	xhandler "github.com/viant/xdatly/handler"
	"reflect"
	"sync/atomic"
)

// Internal finite-phase authority. Public source-phases compilation is still
// unavailable. This token adds no journal, allocator or hook-supplied actions.
type sourceSliceGroup struct {
	attempted atomic.Bool
	owner     *Program
	record    *Record
	table     string
	members   []sourceSliceMember
}
type sourceSliceMember struct {
	action           *Action
	frame, parent    *Frame
	entity           reflect.Value
	tracked, indexed bool
	position         int
	slot             queueSlotSeal
}

func (p *Program) mintSourceSliceGroup(actions []*Action) error {
	if len(actions) == 0 {
		return fmt.Errorf("empty native source-slice group")
	}
	group := &sourceSliceGroup{owner: p}
	seen := map[*Action]bool{}
	for _, action := range actions {
		frame := p.actionFrame(action)
		if frame == nil || action.Kind != xhandler.WriteInsert || frame.Record == nil || frame.Record.QueueContract != "source-slice" || !frame.holderIndexed || action.sourceGroup != nil || p.sourceSliceGroups[action] != nil || seen[action] {
			return fmt.Errorf("invalid native source-slice member")
		}
		if group.record == nil {
			group.record = frame.Record
			group.table = frame.Record.Table
		}
		if group.record != frame.Record || group.table == "" {
			return fmt.Errorf("native source-slice requires one physical record")
		}
		slot, err := p.captureQueueSlots(frame)
		if err != nil {
			return err
		}
		if err = slot.validate(reflect.ValueOf(p.input)); err != nil {
			return err
		}
		// Copy the reflect.Value, not the mutable Action/Frame value location.
		group.members = append(group.members, sourceSliceMember{action: action, frame: frame, parent: frame.Parent, entity: reflect.ValueOf(frame.Entity.Interface()), tracked: frame.holderTracked, indexed: frame.holderIndexed, position: frame.holderPosition, slot: slot})
		seen[action] = true
	}
	// Publish only after every member was captured successfully.
	if p.sourceSliceGroups == nil {
		p.sourceSliceGroups = map[*Action]*sourceSliceGroup{}
	}
	for _, member := range group.members {
		member.action.sourceGroup = group
		p.sourceSliceGroups[member.action] = group
	}
	return nil
}

func (p *Program) validateSourceSliceGroup(group *sourceSliceGroup, actions []*Action) error {
	if group == nil || group.owner != p || group.attempted.Load() || len(actions) == 0 || len(actions) != len(group.members) || group.record == nil || group.record.Table != group.table || group.record.QueueContract != "source-slice" {
		return fmt.Errorf("invalid native source-slice group authority")
	}

	// One declared native group appears exactly once in the available action span.
	// Detect whole-token repetition before its first append, not on its second use.
	counts := map[*Action]int{}
	for _, action := range p.actions.Rows {
		counts[action]++
	}
	first := -1
	for i, action := range p.actions.Rows {
		if action == actions[0] {
			first = i
			break
		}
	}
	if first < 0 || first+len(actions) > len(p.actions.Rows) {
		return fmt.Errorf("native source-slice group absent from action span")
	}
	for i, action := range actions {
		if counts[action] != 1 || p.actions.Rows[first+i] != action {
			return fmt.Errorf("native source-slice repeated or noncontiguous group")
		}
	}
	for i, member := range group.members {
		action := actions[i]
		frame := p.actionFrame(action)
		if action != member.action || action.sourceGroup != group || p.sourceSliceGroups[action] != group || action.Kind != xhandler.WriteInsert || frame != member.frame || frame.Record != group.record || frame.Parent != member.parent || frame.holderTracked != member.tracked || frame.holderIndexed != member.indexed || frame.holderPosition != member.position || frame.Entity.Type() != member.entity.Type() || frame.Entity.Pointer() != member.entity.Pointer() {
			return fmt.Errorf("native source-slice member authority changed at %d", i)
		}
		if err := member.slot.validate(reflect.ValueOf(p.input)); err != nil {
			return err
		}
	}
	return nil
}

func (p *Program) sourceSliceAdmissionEnd(start int) (int, error) {
	action := p.actions.Rows[start]
	if action == nil {
		return start, fmt.Errorf("nil native writer action")
	}
	if action.sourceGroup != p.sourceSliceGroups[action] {
		return start, fmt.Errorf("native source-slice membership changed")
	}
	if action.sourceGroup == nil {
		return p.sourceSliceEnd(start), nil
	}
	group := action.sourceGroup
	end := start + len(group.members)
	if end > len(p.actions.Rows) {
		return start, fmt.Errorf("partial native source-slice group")
	}
	if err := p.validateSourceSliceGroup(group, p.actions.Rows[start:end]); err != nil {
		return start, err
	}
	return end, nil
}
