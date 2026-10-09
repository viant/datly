package writer

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness/sqlite"
	rh "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/sql/dml"
	h "github.com/viant/xdatly/handler"
)

type currentValidationHas struct{ ID, OrderID, Label bool }
type currentValidationRow struct {
	ID      int                   `sqlx:"id,primaryKey"`
	OrderID int                   `sqlx:"order_id,refTable=orders,refColumn=id"`
	Label   string                `sqlx:"label"`
	Has     *currentValidationHas `sqlx:"-" setMarker:"true"`
}

func TestFiniteCurrentValidationAdmittedReferences(t *testing.T) {
	db := sqlite.New(t)
	ctx := context.Background()
	if err := db.ExecStatements(ctx, "CREATE TABLE orders(id INTEGER PRIMARY KEY)", "CREATE TABLE lines(id INTEGER PRIMARY KEY, order_id INTEGER, label TEXT)", "INSERT INTO orders(id) VALUES(1)", "INSERT INTO lines(id,order_id,label) VALUES(10,1,'old')"); err != nil {
		t.Fatal(err)
	}
	root := &Record{Table: "orders", EntityType: reflect.TypeFor[benchOrder](), Fields: []Field{{Name: "ID", Column: "id", Index: []int{0}}}}
	child := &Record{Table: "lines", Path: "Rows[0].Lines", EntityType: reflect.TypeFor[currentValidationRow](), Fields: []Field{
		{Name: "ID", Column: "id", Index: []int{0}, Has: []int{3, 0}},
		{Name: "OrderID", Column: "order_id", Index: []int{1}, Has: []int{3, 1}, RefTable: "orders", RefColumn: "id"},
		{Name: "Label", Column: "label", Index: []int{2}, Has: []int{3, 2}},
	}}
	child.Keys = child.Fields[:1]
	id := 1
	parent := &Frame{Record: root, Entity: reflect.ValueOf(&benchOrder{ID: &id}), Action: h.WriteInsert}
	incoming := &currentValidationRow{ID: 10, OrderID: 1, Label: "incoming", Has: &currentValidationHas{ID: true}}
	previous := &currentValidationRow{ID: 10, OrderID: 1, Label: "old", Has: &currentValidationHas{ID: true}}
	canonical := &Frame{Record: child, Entity: reflect.ValueOf(incoming), Previous: reflect.ValueOf(previous), Parent: parent, Location: "Rows[0].Lines[0]", Original: originalPresence{}}
	p := &Program{metadata: &Metadata{Root: root}, frames: &MutationFrames{Rows: []*Frame{parent, canonical}}, typeFields: map[reflect.Type]fieldSet{}}
	p.graphIndex() // deliberately retain pre-allocation cached key 1
	id = 77
	p.reconciliation = &reconciliationAttempt{rootAdmitted: true, rootAppendPrefix: 1, roots: []reconciliationRootOccurrences{{frame: parent}}}
	p.rootAdmissionSpan = []*Action{{Kind: h.WriteInsert, Entity: parent.Entity, frame: parent}}
	image := &currentValidationRow{ID: 10, OrderID: 77, Label: "planned", Has: &currentValidationHas{ID: true, OrderID: true, Label: true}}
	effective := *canonical
	effective.Entity = reflect.ValueOf(image)
	effective.Action = h.WriteUpdate
	options, err := p.finiteCurrentValidationOptions(canonical, &effective)
	if err != nil {
		t.Fatal(err)
	}
	if options.Previous != previous || options.Location != canonical.Location || !options.Fields.Has("Label") || options.Fields.Has("OrderID") || incoming.Has.Label {
		t.Fatal("effective presence or admitted allocated root reference lost", options)
	}
	validator := dml.NewData(db.DB).FrameworkValidator()
	result, err := validator.Validate(ctx, []*currentValidationRow{image}, []h.ValidationOptions{options})
	if err != nil || result.Err() != nil {
		t.Fatal("buffered admitted parent reference failed", err, result.Err())
	}
	// No successful native admission: the same canonical INSERT is not proof.
	p.reconciliation.rootAdmitted = false
	options, err = p.finiteCurrentValidationOptions(canonical, &effective)
	if err != nil || !options.Fields.Has("OrderID") {
		t.Fatal("unreached producer exemption", err)
	}
	result, err = validator.Validate(ctx, []*currentValidationRow{image}, []h.ValidationOptions{options})
	if err == nil && result.Err() == nil {
		t.Fatal("unmatched physical FK silently accepted")
	}
	// Native SQLX schema violations keep the writer path and phase classification.
	if err != nil {
		t.Fatal("expected schema result rather than operational error", err)
	}
	failure := finiteCurrentValidationFailure(child.Path, result, nil)
	probe := &observedWriterProbe{}
	_, scope := rh.WithPhaseObserver(ctx, probe, 1, 1)
	scope.Notify(ctx, h.PhaseValidation, h.PhaseEnd, failure)
	if failure == nil || !strings.Contains(failure.Error(), "validate writer "+child.Path) || len(probe.events) != 1 || probe.events[0].Result != h.PhaseViolations {
		t.Fatal("native validation classification lost", failure, probe.events)
	}
	// An earlier non-root INSERT cannot substitute for the admitted root.
	p.reconciliation.rootAdmitted = true
	other := *parent
	other.Parent = parent
	p.frames.Rows = []*Frame{&other, canonical, parent}
	options, err = p.finiteCurrentValidationOptions(canonical, &effective)
	if err != nil || !options.Fields.Has("OrderID") {
		t.Fatal("unreached child producer exemption", err)
	}
}

// Replaced binders must be rejected before resolving protected validation.
type finiteCurrentValidationBinder struct {
	h.Binder
	frameworkLookups int
}

func (b *finiteCurrentValidationBinder) Lookup(ctx context.Context, key h.ValueKey) (any, bool, error) {
	if key == h.FrameworkValidatorKey {
		b.frameworkLookups++
	}
	return b.Binder.Lookup(ctx, key)
}
