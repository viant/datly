package collector

import (
	"reflect"
	"testing"

	"github.com/viant/datly/data"
	"github.com/viant/xunsafe"
)

type indexVisitorRow struct {
	ID1 int
	ID2 int
}

type compositeParentRow struct {
	A int
	B int
}

func TestIndexValueByRel_SkipsNilInt64Pointers(t *testing.T) {
	rowType := reflect.TypeOf(indexVisitorRow{})
	view := newTestView(&data.View{}, rowType)
	var dest []indexVisitorRow
	c := NewCollector(view, &dest, false)

	relation := &Relation{
		Relation: &data.Relation{Name: "SingleLink"},
		On: Links{
			newTestLink("users", "id1", "ID1", xunsafe.FieldByName(rowType, "ID1")),
		},
	}

	value := int64(7)
	c.indexValueByRel([]*int64{nil, &value}, relation, 0)

	entries := c.valuePosition["users"]["id1"]
	total := 0
	for _, positions := range entries {
		total += len(positions)
	}
	if total != 1 {
		t.Fatalf("expected exactly one indexed position after skipping nil pointers, got %d entries: %+v", total, entries)
	}
}

func TestIndexCompositePositions_SkipsNilLinks(t *testing.T) {
	rowType := reflect.TypeOf(compositeParentRow{})
	view := newTestView(&data.View{}, rowType)
	var dest []compositeParentRow
	c := NewCollector(view, &dest, false)
	item := c.NewItem()().(*compositeParentRow)
	item.A = 1
	item.B = 2

	aField := xunsafe.FieldByName(rowType, "A")
	relation := &Relation{
		Relation: &data.Relation{Name: "Composite"},
		On: Links{
			newTestLink("parents", "a", "A", aField),
			nil,
		},
	}

	c.indexCompositePositions(relation)

	signature := relationCompositeSignature(relation.On)
	if got := c.compositeValuePosition[signature]; len(got) != 0 {
		t.Fatalf("expected nil-link composite relation to skip indexing, got %+v", got)
	}
}
