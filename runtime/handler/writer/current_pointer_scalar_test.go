package writer

import (
	"context"
	h "github.com/viant/xdatly/handler"
	"reflect"
	"strings"
	"testing"
)

type currentScalarMarker struct{ Id, ParentId, Qty bool }
type currentScalarRow struct {
	Id, ParentId, Qty int
	Name              *string
	Has               *currentScalarMarker `setMarker:"true"`
}
type currentPointerRow struct {
	Id            int
	ParentId, Qty *int
	Name          *string
}
type currentScalarObserver struct{}

func (*currentScalarObserver) ObserveQueueAttempt(context.Context, h.QueueAttemptEvent) {}
func currentScalarFixture() (*Program, *Record) {
	fields := []Field{{Name: "Id", Column: "ID", Index: []int{0}, Has: []int{4, 0}}, {Name: "ParentId", Column: "PARENT_ID", Index: []int{1}, Has: []int{4, 1}}, {Name: "Qty", Column: "QTY", Index: []int{2}, Has: []int{4, 2}}, {Name: "Name", Column: "NAME", Index: []int{3}}}
	record := &Record{Name: "Children", Path: "Parents/Children", EntityType: reflect.TypeFor[currentScalarRow](), Fields: fields, Keys: fields[:1], CurrentField: 0, Auxiliary: true}
	p := &Program{metadata: &Metadata{Operation: "patch"}, database: &DatabaseSnapshot{Rows: map[rowIdentity]reflect.Value{}, ByRecord: map[*Record][]reflect.Value{}}, hook: reflect.ValueOf(&currentScalarObserver{}), original: &OriginalInput{Presence: map[uintptr]originalPresence{}}, frames: &MutationFrames{}}
	return p, record
}
func TestCurrentPointerToScalarCopiesNonnilAndLoadedEvidence(t *testing.T) {
	p, r := currentScalarFixture()
	parent, zero := 11, 0
	if err := p.indexCurrent(r, reflect.ValueOf([]*currentPointerRow{{Id: 9, ParentId: &parent, Qty: &zero}})); err != nil {
		t.Fatal(err)
	}
	previous := p.database.ByRecord[r][0].Interface().(*currentScalarRow)
	if previous.ParentId != 11 || previous.Qty != 0 || previous.Name != nil || previous.Has != nil {
		t.Fatalf("pointer current copy differs: %+v", previous)
	}
	for _, field := range []string{"Id", "ParentId", "Qty", "Name"} {
		if !p.previousFields[r].Has(field) {
			t.Fatalf("successfully copied field %s not loaded", field)
		}
	}
}
func TestCurrentNilPointerScalarRejectsWithoutLoadedCertificate(t *testing.T) {
	for _, second := range []bool{false, true} {
		t.Run(map[bool]string{false: "first", true: "after-valid-prefix"}[second], func(t *testing.T) {
			p, r := currentScalarFixture()
			p.previousFields = map[*Record]fieldSet{r: {"ParentId": true}} // failed recapture cannot retain a stale certificate
			parent, zero := 11, 0
			rows := []*currentPointerRow{}
			if second {
				rows = append(rows, &currentPointerRow{Id: 8, ParentId: &parent, Qty: &zero})
			}
			rows = append(rows, &currentPointerRow{Id: 9, ParentId: nil, Qty: &zero})
			err := p.indexCurrent(r, reflect.ValueOf(rows))
			if err == nil || !strings.Contains(err.Error(), "cannot assign *int to int") {
				t.Fatalf("nil scalar current was silently admitted: %v", err)
			}
			if p.previousFields[r] != nil {
				t.Fatalf("failed conversion certified loaded scalar data: %v", p.previousFields[r])
			}
		})
	}
}
func TestCurrentPointerMappingPreservesParentScopeAndSparseOriginal(t *testing.T) {
	for _, foreign := range []bool{false, true} {
		t.Run(map[bool]string{false: "admitted-parent", true: "foreign-parent-rejected"}[foreign], func(t *testing.T) {
			p, r := currentScalarFixture()
			parentId, qty := 11, 7
			currentParent := parentId
			if foreign {
				currentParent = 12
			}
			if err := p.indexCurrent(r, reflect.ValueOf([]*currentPointerRow{{Id: 9, ParentId: &currentParent, Qty: &qty}})); err != nil {
				t.Fatal(err)
			}
			type parent struct {
				Id       int
				Children []*currentScalarRow
			}
			parentRecord := &Record{Name: "Parents", Path: "Parents", EntityType: reflect.TypeFor[parent](), Fields: []Field{{Name: "Id", Index: []int{0}}}}
			parentRecord.Relations = []*Relation{{Field: []int{1}, Child: r, Links: []Link{{Parent: parentRecord.Fields[0], Child: r.Fields[1]}}}}
			parentFrame := &Frame{Record: parentRecord, Entity: reflect.ValueOf(&parent{Id: parentId}), Location: "Parents[0]"}
			body := &currentScalarRow{Id: 9, ParentId: 88, Has: &currentScalarMarker{Id: true}}
			original := p.captureEntityOriginal(r, reflect.ValueOf(body))
			err := p.buildEntityFrame(context.Background(), nil, r, reflect.ValueOf(body), parentFrame, 0, true)
			if foreign {
				if err == nil || !strings.Contains(err.Error(), "outside parent scope") {
					t.Fatalf("foreign nullable current escaped scope: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(p.frames.Rows) != 1 || p.frames.Rows[0].Previous.Interface().(*currentScalarRow).ParentId != 11 {
				t.Fatal("canonical Previous lost nonnil FK")
			}
			if original.presence.hasPos(1) || p.frames.Rows[0].Original.Has("ParentId") || body.Has.ParentId {
				t.Fatal("current capture altered sparse client Original.Has")
			}
		})
	}
}
func TestCurrentPointerMappingRetainsExistingConversionBoundary(t *testing.T) {
	p, r := currentScalarFixture()
	type unsupported struct {
		Id       int
		ParentId *int64
	}
	value := int64(11)
	if err := p.indexCurrent(r, reflect.ValueOf([]*unsupported{{Id: 9, ParentId: &value}})); err != nil {
		t.Fatal(err)
	}
	if p.database.ByRecord[r][0].Interface().(*currentScalarRow).ParentId != 0 || p.previousFields[r].Has("ParentId") {
		t.Fatal("unsupported pointer numeric conversion was broadened")
	}
	p, r = currentScalarFixture()
	type scalar struct {
		Id, ParentId, Qty int
		Name              *string
	}
	if err := p.indexCurrent(r, reflect.ValueOf([]*scalar{{Id: 9, ParentId: 11, Qty: 0, Name: nil}})); err != nil {
		t.Fatal(err)
	}
	if p.database.ByRecord[r][0].Interface().(*currentScalarRow).ParentId != 11 || !p.previousFields[r].Has("Name") {
		t.Fatal("existing scalar or nullable pointer destination policy changed")
	}
}
