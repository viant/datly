package writer

import (
	"reflect"
	"testing"
	"time"

	h "github.com/viant/xdatly/handler"
)

func TestRetainedCurrentMappedValues(t *testing.T) {
	type row struct {
		Name      *string
		Updated   *time.Time
		Transient bool
	}
	name := "stored"
	stamp := time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC)
	same := stamp.In(time.FixedZone("offset", 3600))
	later := stamp.Add(time.Second)
	type useCase struct {
		description       string
		current, previous row
		loaded            fieldSet
		wantError         bool
	}
	for _, tc := range []useCase{
		{description: "transient field needs no database evidence", current: row{Name: &name, Transient: true}, previous: row{Name: &name}, loaded: fieldSet{"Name": true, "Updated": true}},
		{description: "equal timestamp with different location", current: row{Name: &name, Updated: &same}, previous: row{Name: &name, Updated: &stamp}, loaded: fieldSet{"Name": true, "Updated": true}},
		{description: "changed timestamp", current: row{Name: &name, Updated: &later}, previous: row{Name: &name, Updated: &stamp}, loaded: fieldSet{"Name": true, "Updated": true}, wantError: true},
		{description: "null differs from a value", current: row{}, previous: row{Name: &name}, loaded: fieldSet{"Name": true, "Updated": true}, wantError: true},
		{description: "unloaded mapped field", current: row{}, previous: row{}, loaded: fieldSet{"Name": true}, wantError: true},
	} {
		t.Run(tc.description, func(t *testing.T) {
			record := &Record{Fields: []Field{{Name: "Name", Column: "name", Index: []int{0}}, {Name: "Updated", Column: "updated", Index: []int{1}}, {Name: "Transient", Column: "-", Index: []int{2}}}}
			p := &Program{previousFields: map[*Record]fieldSet{record: tc.loaded}}
			frame := &Frame{Record: record, Action: h.WriteUpdate, Entity: reflect.ValueOf(&tc.current), Previous: reflect.ValueOf(&tc.previous)}
			err := p.validateRetainedCurrent(frame)
			if (err != nil) != tc.wantError {
				t.Fatalf("error=%v wantError=%v", err, tc.wantError)
			}
		})
	}
}
