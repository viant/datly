package writer

import (
	"context"
	"reflect"
	"testing"

	xhandler "github.com/viant/xdatly/handler"
)

type auditHas struct{ ID, Version, Name, Qty bool }
type auditRow struct {
	ID      int64
	Version *int64
	Name    *string
	Qty     int64
	Has     *auditHas `setMarker:"true"`
}
type auditOutput struct{ Count int }

// All plans, paths and source values are prepared outside the timed region.
// Each operation is one field assignment, one state, one row check or one
// relation pass, as named. These are not database or end-to-end benchmarks.
func auditFixture() (*Program, *Frame, *auditRow) {
	typ := reflect.TypeFor[auditRow]()
	marker := entityMarker(typ)
	has, _ := typ.FieldByName("Has")
	fields := make([]Field, 0, 4)
	for i := 0; i < 4; i++ {
		f := typ.Field(i)
		m, _ := reflect.TypeFor[auditHas]().FieldByName(f.Name)
		field := Field{Name: f.Name, Column: f.Name, Index: f.Index}
		compileMarker(&field, has, m, marker)
		fields = append(fields, field)
	}
	record := &Record{EntityType: typ, Path: "Rows", Table: "rows", Fields: fields, Keys: fields[:1], ConcurrencyToken: &fields[1], Invariants: map[string][]Field{"pair": fields[2:4]}}
	record.indexFields()
	version, name := int64(7), "previous"
	row := &auditRow{ID: 1, Version: ptr(version), Name: ptr("changed"), Has: &auditHas{ID: true, Version: true, Name: true}}
	previous := &auditRow{ID: 1, Version: &version, Name: &name, Qty: 11}
	entity := reflect.ValueOf(row)
	frame := &Frame{Record: record, Entity: entity, Previous: reflect.ValueOf(previous), ExpectedToken: cloneTokenValue(entity.Elem().Field(1)), Action: xhandler.WriteUpdate, Location: "Rows[0]", Fields: livePresence(record, entity.Elem()), Original: originalPresence{presence: snapshotPresence(record, entity.Elem()), available: true}}
	program := &Program{output: &auditOutput{}, metadata: &Metadata{Root: record}, frames: &MutationFrames{Rows: []*Frame{frame}}}
	return program, frame, row
}

func BenchmarkReflectionAuditPrevious(b *testing.B) {
	for _, method := range []string{"checkConcurrency", "applyInvariants", "queuePhysical"} {
		b.Run(method, func(b *testing.B) {
			p, frame, _ := auditFixture()
			dml := &fakeCapabilities{}
			action := &Action{Kind: xhandler.WriteUpdate, Entity: frame.Entity}
			ctx := context.Background()
			// Preallocate the forced coverage; measure steady backfill, not first-use allocation.
			frame.Fields.force("Qty")
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var err error
				switch method {
				case "checkConcurrency":
					err = p.checkConcurrency(frame)
				case "applyInvariants":
					err = p.applyInvariants(frame)
				case "queuePhysical":
					err = p.queuePhysical(ctx, dml, dml, action, frame, nil)
				}
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
