package writer

import (
	"fmt"
	"reflect"
	"testing"

	xhandler "github.com/viant/xdatly/handler"
)

func ownershipFixture(count int) *Program {
	p := &Program{frames: &MutationFrames{}, actions: &MutationActions{}}
	record := &Record{}
	for i := range count {
		entity := reflect.ValueOf(new(int))
		*entity.Interface().(*int) = i
		frame := &Frame{Entity: entity, Record: record}
		p.frames.Rows = append(p.frames.Rows, frame)
		p.actions.Rows = append(p.actions.Rows, &Action{Kind: xhandler.WriteUpdate, Entity: entity, frame: frame})
	}
	return p
}

func BenchmarkActionFrameOwnership(b *testing.B) {
	for _, count := range []int{1075, 9796} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			p := ownershipFixture(count)
			for _, action := range p.actions.Rows {
				if p.actionFrame(action) != action.frame {
					b.Fatal("lost ownership")
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				for index, action := range p.actions.Rows {
					if p.sourceSliceEnd(index) != index || p.actionFrame(action) != action.frame {
						b.Fatal("lost action boundary or ownership")
					}
				}
			}
		})
	}
}

func TestActionFrameCurrentMembership(t *testing.T) {
	for _, mutation := range []string{"replace slot", "remove in place", "reorder", "rebuild", "foreign frame", "replace entity"} {
		t.Run(mutation, func(t *testing.T) {
			t.Parallel()
			p := ownershipFixture(3)
			action := p.actions.Rows[1]
			if p.actionFrame(action) != action.frame {
				t.Fatal("initial ownership missing")
			}
			want := action.frame
			switch mutation {
			case "replace slot":
				copy := *action.frame
				p.frames.Rows[1] = &copy // Same entity, length, and backing array.
				want = nil
			case "remove in place":
				copy(p.frames.Rows[1:], p.frames.Rows[2:])
				p.frames.Rows = p.frames.Rows[:2]
				want = nil
			case "reorder":
				p.frames.Rows[0], p.frames.Rows[1] = p.frames.Rows[1], p.frames.Rows[0]
			case "rebuild":
				copy := *action.frame
				p.frames = &MutationFrames{Rows: []*Frame{&copy}}
				want = nil
			case "foreign frame":
				copy := *action.frame
				action.frame = &copy
				want = nil
			case "replace entity":
				action.frame.Entity = reflect.ValueOf(new(int))
				want = nil
			}
			if got := p.actionFrame(action); got != want {
				t.Fatalf("got %p want %p", got, want)
			}
		})
	}
}

func TestActionFrameAliasedEntitiesRetainRoles(t *testing.T) {
	p := ownershipFixture(2)
	p.frames.Rows[1].Record = &Record{}
	p.frames.Rows[1].Entity = p.frames.Rows[0].Entity
	p.actions.Rows[1].Entity = p.frames.Rows[0].Entity
	for range 3 {
		for _, action := range p.actions.Rows {
			if p.actionFrame(action) != action.frame {
				t.Fatal("aliased entity lost its exact frame/record role")
			}
		}
	}
	for _, kind := range []xhandler.WriteAction{xhandler.WriteUpdate, xhandler.WriteDelete} {
		p.actions.Rows[0].Kind = kind
		p.frames.Rows[0] = nil
		if p.sourceSliceEnd(0) != 0 || p.actionFrame(p.actions.Rows[0]) != nil {
			t.Fatal("non-insert source-slice shortcut bypassed authoritative action validation")
		}
	}
}
