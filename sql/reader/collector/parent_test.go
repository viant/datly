package collector

import (
	"reflect"
	"testing"

	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
)

type parentPlaceholderRow struct {
	ID int
}

type childPlaceholderRow struct{}

type parentBootstrapRow struct {
	Children []*childPlaceholderRow
}

type testColumn struct {
	name     string
	scanType reflect.Type
}

func (c testColumn) Name() string           { return c.name }
func (c testColumn) ScanType() reflect.Type { return c.scanType }

func TestParentPlaceholders_FieldlessScalarUsesParentValuePositions(t *testing.T) {
	parentView := newTestView(&data.View{}, reflect.TypeOf(parentPlaceholderRow{}))
	childView := newTestView(&data.View{}, reflect.TypeOf(childPlaceholderRow{}))
	parentView.Relations = []*Relation{newTestRelation(parentView, childView, "Children", "", spec.CardinalityMany,
		Links{newTestLink("parents", "parent_id", "parent_id", nil)},
		Links{newTestLink("children", "parent_id", "parent_id", nil)})}

	var parents []parentPlaceholderRow
	parentCollector := NewCollector(parentView, &parents, false)
	parentCollector.NewItem()().(*parentPlaceholderRow).ID = 1
	parentValue := parentCollector.Resolve(testColumn{name: "parent_id", scanType: reflect.TypeOf(0)})(nil).(*int)
	*parentValue = 9
	parentCollector.Fetched()

	children := parentCollector.Relations(nil)
	if len(children) != 1 {
		t.Fatalf("expected 1 child collector, got %d", len(children))
	}

	scalar, composite, columns := children[0].ParentPlaceholders()
	if len(composite) != 0 {
		t.Fatalf("expected no composite placeholders, got %+v", composite)
	}
	if len(columns) != 1 || columns[0] != "children.parent_id" {
		t.Fatalf("unexpected placeholder columns: %+v", columns)
	}
	if len(scalar) != 1 || scalar[0] != 9 {
		t.Fatalf("expected scalar placeholders [9], got %+v", scalar)
	}
}

func TestParentPlaceholders_FieldlessCompositePreservesParentTuples(t *testing.T) {
	parentView := newTestView(&data.View{}, reflect.TypeOf(parentPlaceholderRow{}))
	childView := newTestView(&data.View{}, reflect.TypeOf(childPlaceholderRow{}))
	parentView.Relations = []*Relation{newTestRelation(parentView, childView, "Children", "", spec.CardinalityMany,
		Links{
			newTestLink("parents", "tenant_key", "tenant", nil),
			newTestLink("parents", "local_key", "parent_id", nil),
		},
		Links{
			newTestLink("children", "tenant", "tenant", nil),
			newTestLink("children", "parent_id", "parent_id", nil),
		})}

	var parents []parentPlaceholderRow
	parentCollector := NewCollector(parentView, &parents, false)
	for _, key := range []struct {
		tenant string
		id     int
	}{{tenant: "acme", id: 1}, {tenant: "beta", id: 2}} {
		parentCollector.NewItem()()
		tenant := parentCollector.Resolve(testColumn{name: "tenant_key", scanType: reflect.TypeOf("")})(nil).(*string)
		*tenant = key.tenant
		id := parentCollector.Resolve(testColumn{name: "local_key", scanType: reflect.TypeOf(0)})(nil).(*int)
		*id = key.id
	}
	parentCollector.Fetched()

	children := parentCollector.Relations(nil)
	if len(children) != 1 {
		t.Fatalf("expected 1 child collector, got %d", len(children))
	}
	scalar, composite, columns := children[0].ParentPlaceholders()
	if len(scalar) != 0 {
		t.Fatalf("expected no scalar placeholders, got %+v", scalar)
	}
	if !reflect.DeepEqual(columns, []string{"children.tenant", "children.parent_id"}) {
		t.Fatalf("unexpected placeholder columns: %+v", columns)
	}
	want := [][]interface{}{{"acme", 1}, {"beta", 2}}
	if !reflect.DeepEqual(composite, want) {
		t.Fatalf("composite placeholders: got %+v, want %+v", composite, want)
	}
}

func TestParentRow_FieldlessSingleLinkUsesBufferedValues(t *testing.T) {
	parentView := newTestView(&data.View{}, reflect.TypeOf(parentPlaceholderRow{}))
	childView := newTestView(&data.View{}, reflect.TypeOf(childPlaceholderRow{}))
	parentView.Relations = []*Relation{newTestRelation(parentView, childView, "Children", "", spec.CardinalityMany,
		Links{newTestLink("parents", "parent_id", "parent_id", nil)},
		Links{newTestLink("children", "parent_id", "parent_id", nil)})}

	var parents []parentPlaceholderRow
	parentCollector := NewCollector(parentView, &parents, false)
	parent := parentCollector.NewItem()().(*parentPlaceholderRow)
	parent.ID = 7
	parentValue := parentCollector.Resolve(testColumn{name: "parent_id", scanType: reflect.TypeOf(0)})(nil).(*int)
	*parentValue = 9
	parentCollector.Fetched()

	children := parentCollector.Relations(nil)
	if len(children) != 1 {
		t.Fatalf("expected 1 child collector, got %d", len(children))
	}
	childCollector := children[0]
	childValue := childCollector.Resolve(testColumn{name: "parent_id", scanType: reflect.TypeOf(0)})(nil).(*int)
	*childValue = 9

	resolveParent := childCollector.ParentRow()
	if resolveParent == nil {
		t.Fatalf("expected parent row resolver")
	}
	actual, err := resolveParent(&childPlaceholderRow{})
	if err != nil {
		t.Fatalf("unexpected parent resolver error: %v", err)
	}
	parentRow, ok := actual.(*parentPlaceholderRow)
	if !ok {
		t.Fatalf("expected *parentPlaceholderRow, got %T", actual)
	}
	if parentRow.ID != 7 {
		t.Fatalf("expected resolved parent ID 7, got %+v", parentRow)
	}
}

func TestBootstrapFromParentHolder_AppendsFromPointerSlice(t *testing.T) {
	parentView := newTestView(&data.View{}, reflect.TypeOf(parentBootstrapRow{}))
	childView := newTestView(&data.View{}, reflect.TypeOf(childPlaceholderRow{}))
	parentView.Relations = []*Relation{newTestRelation(parentView, childView, "Children", "Children", spec.CardinalityMany, nil, nil)}

	var parents []parentBootstrapRow
	parentCollector := NewCollector(parentView, &parents, false)
	parent := parentCollector.NewItem()().(*parentBootstrapRow)
	parent.Children = []*childPlaceholderRow{{}, nil, {}}
	parentCollector.Fetched()

	children := parentCollector.Relations(nil)
	if len(children) != 1 {
		t.Fatalf("expected 1 child collector, got %d", len(children))
	}
	if ok := children[0].BootstrapFromParentHolder(); !ok {
		t.Fatalf("expected bootstrap to append child rows")
	}
	if children[0].Len() != 2 {
		t.Fatalf("expected 2 bootstrapped child rows, got %d", children[0].Len())
	}
}
