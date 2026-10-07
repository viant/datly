package writer

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	xhandler "github.com/viant/xdatly/handler"
)

type auditMatchedDML struct {
	fakeCapabilities
	match *xhandler.Match
}

func (d *auditMatchedDML) UpdateWithOptions(_ string, _ any, options ...xhandler.Option) error {
	var applied xhandler.Options
	for _, option := range options {
		option(&applied)
	}
	d.match = applied.IfMatch
	return nil
}

func TestReflectionAuditPreviousToken(t *testing.T) {
	for _, layout := range []string{"canonical", "reordered-projection", "empty-index"} {
		t.Run(layout, func(t *testing.T) {
			p, frame, row := auditFixture()
			if layout == "reordered-projection" {
				frame.Previous = reflect.ValueOf(&struct {
					Version *int64
					Noise   string
				}{ptr(int64(7)), "unrelated"})
			}
			if layout == "empty-index" {
				frame.Record.ConcurrencyToken.Index = nil
			}
			// Init advances the working token. Checks use the captured original;
			// atomic DML uses Previous, never the newly advanced working token.
			*row.Version = 8
			if err := p.checkConcurrency(frame); err != nil {
				t.Fatal(err)
			}
			dml := &auditMatchedDML{}
			if err := p.queuePhysical(context.Background(), dml, dml, &Action{Kind: xhandler.WriteUpdate, Entity: frame.Entity}, frame, nil); err != nil {
				t.Fatal(err)
			}
			if dml.match == nil || dml.match.Column != "Version" || *dml.match.Value.(*int64) != 7 {
				t.Fatalf("wrong persisted match: %+v", dml.match)
			}
			frame.ExpectedToken = reflect.ValueOf(ptr(int64(6)))
			var conflict *xhandler.Conflict
			if err := p.checkConcurrency(frame); !errors.As(err, &conflict) {
				t.Fatalf("stale token accepted: %v", err)
			}
		})
	}
	for _, supplied := range []bool{false, true} {
		t.Run(fmt.Sprintf("nil-token-supplied-%v", supplied), func(t *testing.T) {
			p, frame, _ := auditFixture()
			frame.Previous.Interface().(*auditRow).Version = nil
			frame.Entity.Interface().(*auditRow).Version = nil
			frame.Entity.Interface().(*auditRow).Has.Version = supplied
			frame.ExpectedToken = cloneTokenValue(frame.Entity.Elem().Field(1))
			frame.Original = originalPresence{presence: snapshotPresence(frame.Record, frame.Entity.Elem()), available: true}
			if err := p.checkConcurrency(frame); (err == nil) != supplied {
				t.Fatalf("nil token presence conflated: %v", err)
			}
			if supplied {
				frame.Previous.Interface().(*auditRow).Version = ptr(int64(0))
				if err := p.checkConcurrency(frame); err == nil {
					t.Fatal("nil and nonnil zero conflated")
				}
			}
		})
	}
	p, frame, _ := auditFixture()
	frame.Previous = reflect.ValueOf(&struct{ Noise int }{})
	if err := p.checkConcurrency(frame); err == nil {
		t.Fatal("missing token accepted")
	}
	dml := &auditMatchedDML{}
	if err := p.queuePhysical(context.Background(), dml, dml, &Action{Kind: xhandler.WriteUpdate, Entity: frame.Entity}, frame, nil); err == nil {
		t.Fatal("missing persisted token queued")
	}
}

func TestReflectionAuditInvariantSparseBackfill(t *testing.T) {
	for _, layout := range []string{"canonical", "reordered-projection"} {
		t.Run(layout, func(t *testing.T) {
			p, frame, row := auditFixture()
			if layout == "reordered-projection" {
				frame.Previous = reflect.ValueOf(&struct {
					Qty  int64
					Name *string
				}{11, ptr("previous")})
			}
			if err := p.applyInvariants(frame); err != nil {
				t.Fatal(err)
			}
			if row.Qty != 11 || row.Has.Qty || !frame.Fields.Has("Qty") || frame.Original.Has("Qty") || *row.Name != "changed" {
				t.Fatal("backfill changed sparse presence or client value")
			}
			// Active Qty backfills an explicitly nil Previous Name without
			// asserting client presence or changing the saved previous row.
			row.Has.Name, row.Has.Qty = false, true
			row.Name = ptr("discarded")
			if layout == "canonical" {
				frame.Previous.Interface().(*auditRow).Name = nil
			} else {
				frame.Previous.Elem().FieldByName("Name").SetZero()
			}
			if err := p.applyInvariants(frame); err != nil {
				t.Fatal(err)
			}
			if row.Name != nil || row.Has.Name || !frame.Fields.Has("Name") {
				t.Fatal("nil pointer backfill or coverage lost")
			}
		})
	}
	for _, previous := range []any{&struct{ Name *string }{}, &struct {
		Name *string
		Qty  string
	}{Qty: "wrong type"}} {
		p, frame, _ := auditFixture()
		frame.Previous = reflect.ValueOf(previous)
		if err := p.applyInvariants(frame); err == nil {
			t.Fatal("unavailable or incompatible backfill accepted")
		}
	}
}

func TestReflectionAuditLinkedAssignmentContract(t *testing.T) {
	type namedA int64
	type namedB int64
	for _, tc := range []struct {
		name                string
		source, destination any
		fail                bool
	}{
		{"scalar", int64(0), int64(11), false},
		{"scalar-pointer", int64(0), (*int64)(nil), false},
		{"pointer-scalar", ptr(int64(0)), int64(11), false},
		{"pointer-alias", ptr(int64(11)), (*int64)(nil), false},
		{"typed-nil", (*int64)(nil), ptr(int64(11)), false},
		{"nil-to-scalar", (*int64)(nil), int64(11), true},
		{"named-preserved", namedA(11), namedA(0), false},
		{"named-rejected", namedA(11), namedB(0), true},
		{"numeric-width-rejected", int64(11), int32(0), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := reflect.ValueOf(tc.source)
			destination := reflect.New(reflect.TypeOf(tc.destination)).Elem()
			destination.Set(reflect.ValueOf(tc.destination))
			err := assignLinkedValue(destination, source)
			if (err != nil) != tc.fail {
				t.Fatalf("assignment error: %v", err)
			}
			if tc.fail {
				return
			}
			if !concurrencyTokenEqual(destination.Interface(), tc.source) {
				t.Fatal("assignment changed value or null distinction")
			}
			if tc.name == "pointer-alias" && destination.Pointer() != source.Pointer() {
				t.Fatal("pointer alias contract changed")
			}
			if tc.name == "scalar-pointer" && (destination.IsNil() || destination.Elem().Int() != 0) {
				t.Fatal("explicit zero became nil")
			}
		})
	}
	if err := assignLinkedValue(reflect.ValueOf(int64(1)), reflect.ValueOf(int64(2))); err == nil {
		t.Fatal("unsettable destination accepted")
	}
}

func TestReflectionAuditSharedMetadataIndependentPrograms(t *testing.T) {
	_, base, _ := auditFixture()
	record := base.Record
	for i := 0; i < 16; i++ {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			t.Parallel()
			p, frame, row := auditFixture()
			p.metadata.Root, frame.Record, frame.Fields.record = record, record, record
			parent := &auditRow{ID: 10}
			frame.Parent = &Frame{Entity: reflect.ValueOf(parent)}
			stateType := reflect.TypeFor[xhandler.LifecycleContext[auditRow, auditRow, auditOutput]]()
			for j := 0; j < 50; j++ {
				if err := p.checkConcurrency(frame); err != nil {
					t.Fatal(err)
				}
				if err := p.applyInvariants(frame); err != nil {
					t.Fatal(err)
				}
				state := p.entityHookState(stateType, frame).Interface().(xhandler.LifecycleContext[auditRow, auditRow, auditOutput])
				if state.Previous != frame.Previous.Interface().(*auditRow) || state.Previous == row || state.Parent != parent || state.Output != p.output || state.Location != "Rows[0]" || state.Original.Has("Qty") || !state.PreviousFields.Has("Qty") {
					t.Fatal("generic lifecycle state changed")
				}
				dml := &auditMatchedDML{}
				if err := p.queuePhysical(context.Background(), dml, dml, &Action{Kind: xhandler.WriteUpdate, Entity: frame.Entity}, frame, nil); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestReflectionAuditHookStateTypeSafety(t *testing.T) {
	p, frame, _ := auditFixture()
	frame.Parent = &Frame{Entity: reflect.ValueOf(&auditRow{ID: 10})}
	type otherEntity struct{ ID string }
	type otherParent struct{ ID string }
	type otherOutput struct{ Count string }
	typ := reflect.TypeFor[xhandler.LifecycleContext[otherEntity, otherParent, otherOutput]]()
	state := p.entityHookState(typ, frame).Interface().(xhandler.LifecycleContext[otherEntity, otherParent, otherOutput])
	if state.Previous != nil || state.Parent != nil || state.Output != nil || state.Location != frame.Location || !state.Original.Has("Name") {
		t.Fatal("incompatible generic pointer types assigned or original state lost")
	}
}

func TestReflectionAuditMarkerNilAndSnapshot(t *testing.T) {
	for _, cached := range []bool{false, true} {
		t.Run(fmt.Sprint(cached), func(t *testing.T) {
			_, frame, row := auditFixture()
			field := frame.Record.Fields[2]
			if !cached {
				field.marker = nil
			}
			row.Has = nil
			entity := reflect.ValueOf(row).Elem()
			if supplied(entity, field) {
				t.Fatal("nil holder treated as supplied")
			}
			original := snapshotPresence(frame.Record, entity)
			markSupplied(entity, field)
			if row.Has == nil || !supplied(entity, field) || original.Has("Name") {
				t.Fatal("marker write changed original snapshot")
			}
			row.Has.Name = false
			if supplied(entity, field) {
				t.Fatal("live marker clear ignored")
			}
		})
	}
}
