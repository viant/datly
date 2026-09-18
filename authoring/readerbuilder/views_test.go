package readerbuilder

import (
	"context"
	"strings"
	"testing"
)

func TestServiceRelatedViewCRUD(t *testing.T) {
	service := New(Config{Name: "Records"})
	added := service.Apply(context.Background(), Request{DQL: baseDQL, Operation: Operation{Type: OperationAddView, View: &ViewMutation{
		Name: "items", SQL: "SELECT i.id,i.record_id FROM items i", Parent: "records", Join: "LEFT JOIN", On: "items.record_id=records.id",
	}}})
	if !added.Applied || len(added.Structure.Views) != 2 || !strings.Contains(added.DQL, "items.*") {
		t.Fatalf("added=%+v", added)
	}
	updated := service.Apply(context.Background(), Request{DQL: added.DQL, Operation: Operation{Type: OperationUpdateView, View: &ViewMutation{
		Name: "items", SQL: "SELECT i.id,i.record_id,i.name FROM items i",
	}}})
	if !updated.Applied || !strings.Contains(updated.DQL, "i.name") {
		t.Fatalf("updated=%+v", updated)
	}
	removed := service.Apply(context.Background(), Request{DQL: updated.DQL, Operation: Operation{Type: OperationRemoveView, View: &ViewMutation{Name: "items"}}})
	if !removed.Applied || len(removed.Structure.Views) != 1 || strings.Contains(removed.DQL, "items.*") {
		t.Fatalf("removed=%+v", removed)
	}
}

func TestServiceAddViewValidatesRequestedParent(t *testing.T) {
	source := `#setting($_ = $route('/records','GET'))
SELECT records.*,items.* FROM (SELECT id FROM records) records
JOIN (SELECT id,record_id FROM items) items ON items.record_id=records.id`
	response := New(Config{Name: "Records"}).Apply(context.Background(), Request{DQL: source, Operation: Operation{Type: OperationAddView, View: &ViewMutation{
		Name: "notes", SQL: "SELECT id,item_id FROM notes", Parent: "records", On: "notes.item_id=items.id",
	}}})
	if response.Applied || response.DQL != source || len(response.Diagnostics) == 0 {
		t.Fatalf("response=%+v", response)
	}
}

func TestServiceRemoveViewPreservesSiblingAndOuterClause(t *testing.T) {
	source := `#setting($_ = $route('/records','GET'))
SELECT records.*,items.*,notes.*
FROM (SELECT id FROM records) records
LEFT OUTER JOIN (SELECT id,record_id FROM items) items ON items.record_id=records.id
INNER JOIN (SELECT id,record_id FROM notes) notes ON notes.record_id=records.id
ORDER BY records.id`
	response := New(Config{Name: "Records"}).Apply(context.Background(), Request{DQL: source, Operation: Operation{Type: OperationRemoveView, View: &ViewMutation{Name: "items"}}})
	if !response.Applied || strings.Contains(response.DQL, "LEFT OUTER") || strings.Contains(response.DQL, "items.*") || !strings.Contains(response.DQL, "INNER JOIN") || !strings.Contains(response.DQL, "ORDER BY") {
		t.Fatalf("response=%+v", response)
	}
}

func TestServiceRemoveParentWithChildRollsBack(t *testing.T) {
	source := `#setting($_ = $route('/records','GET'))
SELECT records.*,items.*,notes.*
FROM (SELECT id FROM records) records
JOIN (SELECT id,record_id FROM items) items ON items.record_id=records.id
JOIN (SELECT id,item_id FROM notes) notes ON notes.item_id=items.id`
	response := New(Config{Name: "Records"}).Apply(context.Background(), Request{DQL: source, Operation: Operation{Type: OperationRemoveView, View: &ViewMutation{Name: "items"}}})
	if response.Applied || response.DQL != source || len(response.Diagnostics) == 0 {
		t.Fatalf("response=%+v", response)
	}
}

func TestServiceDerivedViewCRUD(t *testing.T) {
	service := New(Config{Name: "Records"})
	added := service.Apply(context.Background(), Request{DQL: baseDQL, Operation: Operation{Type: OperationAddView, View: &ViewMutation{
		Name: "totals", Kind: "derived", SQL: "SELECT COUNT(*) AS count FROM ($View.Records.NonWindowSQL) parent", Parent: "records",
	}}})
	if !added.Applied || !strings.Contains(added.DQL, `$totals<Totals>(output/derived)`) {
		t.Fatalf("added=%+v", added)
	}
	relation := findRelation(added.Structure.Component.RootView, "totals")
	if relation == nil || relation.Kind != "derived" || relation.View == nil || relation.View.Source == nil || !strings.Contains(relation.View.Source.SQL, "COUNT(*)") {
		t.Fatalf("relation=%+v", relation)
	}
	updated := service.Apply(context.Background(), Request{DQL: added.DQL, Operation: Operation{Type: OperationUpdateView, View: &ViewMutation{
		Name: "totals", Kind: "derived", SQL: "SELECT COUNT(*) AS count, MAX(id) AS max_id FROM ($View.Records.NonWindowSQL) parent",
	}}})
	if !updated.Applied || !strings.Contains(updated.DQL, "MAX(id)") || strings.Count(updated.DQL, "output/derived") != 1 {
		t.Fatalf("updated=%+v", updated)
	}
	removed := service.Apply(context.Background(), Request{DQL: updated.DQL, Operation: Operation{Type: OperationRemoveView, View: &ViewMutation{Name: "totals", Kind: "derived"}}})
	if !removed.Applied || strings.Contains(removed.DQL, "output/derived") || findRelation(removed.Structure.Component.RootView, "totals") != nil {
		t.Fatalf("removed=%+v", removed)
	}
}

func TestServiceDerivedViewRejectsJoinAndNestedParent(t *testing.T) {
	service := New(Config{Name: "Records"})
	for _, mutation := range []*ViewMutation{
		{Name: "totals", Kind: "derived", SQL: "SELECT COUNT(*) AS count", Join: "JOIN"},
		{Name: "totals", Kind: "derived", SQL: "SELECT COUNT(*) AS count", Parent: "items"},
	} {
		response := service.Apply(context.Background(), Request{DQL: baseDQL, Operation: Operation{Type: OperationAddView, View: mutation}})
		if response.Applied || response.DQL != baseDQL || len(response.Diagnostics) == 0 {
			t.Fatalf("response=%+v", response)
		}
	}
}
