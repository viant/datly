package writer

import (
	"context"
	"reflect"
	"testing"

	"github.com/viant/datly/spec"
	xhandler "github.com/viant/xdatly/handler"
)

type unitHas struct {
	ID, Start, End, Name, Remove bool
}

func TestUniversalWriterRecognizesEarlierGraphInsertReference(t *testing.T) {
	type parent struct {
		ID *int `sqlx:"id,primaryKey=true"`
	}
	type child struct {
		ID       *int `sqlx:"id,primaryKey=true"`
		ParentID *int `sqlx:"parent_id,refTable=parents,refColumn=id"`
	}
	id, childID := 7, 9
	parentRecord := &Record{Table: "parents", EntityType: reflect.TypeFor[parent](), Fields: []Field{{Name: "ID", Column: "id", Index: []int{0}}}}
	childRecord := &Record{Table: "children", EntityType: reflect.TypeFor[child](), Fields: []Field{{Name: "ID", Column: "id", Index: []int{0}}, {Name: "ParentID", Column: "parent_id", Index: []int{1}, RefTable: "parents", RefColumn: "id"}}}
	parentFrame := &Frame{Entity: reflect.ValueOf(&parent{ID: &id}), Record: parentRecord, Action: xhandler.WriteInsert}
	childFrame := &Frame{Entity: reflect.ValueOf(&child{ID: &childID, ParentID: &id}), Record: childRecord, Action: xhandler.WriteInsert}
	program := &Program{frames: &MutationFrames{Rows: []*Frame{parentFrame, childFrame}}}
	options := program.validationOptions(childFrame, true)
	if len(options.SatisfiedReferences) != 1 || options.SatisfiedReferences[0].Field != "ParentID" {
		t.Fatalf("satisfied references=%+v", options.SatisfiedReferences)
	}
	program.frames.Rows = []*Frame{childFrame, parentFrame}
	if options = program.validationOptions(childFrame, true); len(options.SatisfiedReferences) != 0 {
		t.Fatalf("later insert satisfied reference=%+v", options.SatisfiedReferences)
	}
}

func TestUniversalWriterTreatsIdentityOnlySparseUpdateAsNoOp(t *testing.T) {
	id := 7
	record := &Record{Keys: []Field{{Name: "ID"}}}
	if hasMutableFields(&Frame{Record: record, Fields: fieldSet{"ID": true}}) {
		t.Fatal("identity-only sparse update was treated as mutable")
	}
	name := "updated"
	if !hasMutableFields(&Frame{Record: record, Fields: fieldSet{"ID": true, "Name": true}, Entity: reflect.ValueOf(&struct {
		ID   *int
		Name *string
	}{ID: &id, Name: &name})}) {
		t.Fatal("supplied non-key field was not treated as mutable")
	}
}

func TestUniversalWriterRecognizesLifecycleSparseUpdatePresence(t *testing.T) {
	id, name := 7, "updated"
	row := &unitRow{ID: &id, Name: &name, Has: &unitHas{ID: true, Name: true}}
	record := &Record{EntityType: reflect.TypeFor[unitRow](), Keys: []Field{{Name: "ID"}}, Fields: []Field{{Name: "ID", Index: []int{0}, Has: []int{5, 0}}, {Name: "Name", Index: []int{3}, Has: []int{5, 3}}}}
	frame := &Frame{Record: record, Entity: reflect.ValueOf(row), Fields: fieldSet{"ID": true}, Action: xhandler.WriteUpdate}
	for field, present := range suppliedFields(frame.Entity.Elem(), frame.Record.Fields) {
		if present {
			frame.Fields[field] = true
		}
	}
	if !hasMutableFields(frame) {
		t.Fatal("lifecycle setter presence was not recognized as a sparse update")
	}
}

type unitRow struct {
	ID     *int     `sqlx:"ID,primaryKey=true,autoincrement=true"`
	Start  *int     `sqlx:"START" invariant:"Window"`
	End    *int     `sqlx:"END" invariant:"Window"`
	Name   *string  `sqlx:"NAME"`
	Remove bool     `sqlx:"-" writer:"delete"`
	Has    *unitHas `setMarker:"true" sqlx:"-" json:"-"`
}

type unitInput struct {
	Rows    []*unitRow `parameter:"Rows,kind=body,in=data" view:"Rows,table=records"`
	Current []*unitRow `parameter:"Current,kind=view,in=Current" view:"Current,table=records"`
}

type unitOutput struct {
	Data []*unitRow `parameter:"Data,kind=output,in=body"`
}

type toOneHas struct{ ID, Child bool }
type toOneChildHas struct{ ID, ParentID bool }
type toOneParent struct {
	ID    *int        `sqlx:"ID,primaryKey=true"`
	Child *toOneChild `view:"Child,table=children" on:"ID=PARENT_ID"`
	Has   *toOneHas   `setMarker:"true" sqlx:"-" json:"-"`
}
type toOneChild struct {
	ID       *int           `sqlx:"ID,primaryKey=true"`
	ParentID *int           `sqlx:"PARENT_ID"`
	Has      *toOneChildHas `setMarker:"true" sqlx:"-" json:"-"`
}
type toOneInput struct {
	Rows []*toOneParent `parameter:"Rows,kind=body,in=data" view:"Rows,table=parents"`
}
type toOneOutput struct {
	Data []*toOneParent `parameter:"Data,kind=output,in=body"`
}

func unitComponent() *spec.Component {
	return &spec.Component{
		Key: spec.Key{Kind: spec.KindComponent, Scope: reflect.TypeFor[unitRow]().PkgPath(), Name: "Rows"}, Name: "Rows",
		Settings: &spec.Settings{Mutation: "patch"},
		RootView: &spec.View{Name: "Rows", Source: &spec.ViewSource{Table: "records"}, Columns: []*spec.Column{
			{Name: "ID", Source: "ID", PrimaryKey: true, AutoIncrement: true},
			{Name: "START", Source: "START"}, {Name: "END", Source: "END"}, {Name: "NAME", Source: "NAME"},
		}},
	}
}

func TestUniversalWriterCompilesPlanOnceAndBackfillsInvariant(t *testing.T) {
	handler, err := New(unitComponent(), reflect.TypeFor[unitInput](), reflect.TypeFor[unitOutput](), "patch")
	if err != nil {
		t.Fatal(err)
	}
	if handler.metadata.Root == nil || handler.metadata.Root.Table != "records" || handler.metadata.Root.Sequence == nil || len(handler.metadata.Root.Invariants["Window"]) != 2 {
		t.Fatalf("compiled metadata = %+v", handler.metadata.Root)
	}
	id, start, end := 1, 3, 9
	row := &unitRow{ID: &id, Start: &start, Has: &unitHas{ID: true, Start: true}}
	input := &unitInput{Rows: []*unitRow{row}}
	snapshot, err := handler.CaptureInput(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	program := snapshot.(*Program)
	if program.metadata != handler.metadata {
		t.Fatal("invocation recompiled immutable writer metadata")
	}
	previous := &unitRow{ID: &id, Start: &start, End: &end}
	frame := &Frame{Entity: reflect.ValueOf(row), Previous: reflect.ValueOf(previous), Fields: fieldSet{"ID": true, "Start": true}, Record: handler.metadata.Root}
	if err = program.applyInvariants(frame); err != nil {
		t.Fatal(err)
	}
	if row.End == nil || *row.End != end || row.Has.End || !frame.Fields.Has("End") {
		t.Fatalf("invariant backfill/presence = row:%+v fields:%v", row, frame.Fields)
	}
	original := program.original.Presence[reflect.ValueOf(row).Pointer()]
	if !original.Available() || !original.Has("Start") || original.Has("End") {
		t.Fatalf("original presence = %+v", original)
	}
}

func TestUniversalWriterRetainsAuxiliaryRootMetadata(t *testing.T) {
	component := unitComponent()
	component.RootView.Auxiliary = true
	handler, err := New(component, reflect.TypeFor[unitInput](), reflect.TypeFor[unitOutput](), "patch")
	if err != nil {
		t.Fatal(err)
	}
	if handler.metadata.Root == nil || !handler.metadata.Root.Auxiliary {
		t.Fatalf("auxiliary root metadata = %+v", handler.metadata.Root)
	}
}

func TestUniversalWriterBuildsToOneRelationFrames(t *testing.T) {
	component := &spec.Component{
		Key: spec.Key{Kind: spec.KindComponent, Scope: reflect.TypeFor[toOneParent]().PkgPath(), Name: "Rows"}, Name: "Rows",
		Settings: &spec.Settings{Mutation: "post"},
		RootView: &spec.View{Name: "Rows", Source: &spec.ViewSource{Table: "parents"}, Columns: []*spec.Column{{Name: "ID", Source: "ID", PrimaryKey: true}}, Relations: []*spec.Relation{{
			Name: "Child", Holder: "Child", Cardinality: spec.CardinalityOne,
			View: &spec.View{Name: "Child", Source: &spec.ViewSource{Table: "children"}, Columns: []*spec.Column{{Name: "ID", Source: "ID", PrimaryKey: true}, {Name: "PARENT_ID", Source: "PARENT_ID"}}},
			On:   []*spec.RelationLink{{ParentColumn: "ID", ChildColumn: "PARENT_ID"}},
		}}},
	}
	handler, err := New(component, reflect.TypeFor[toOneInput](), reflect.TypeFor[toOneOutput](), "post")
	if err != nil {
		t.Fatal(err)
	}
	parentID, childID := 1, 2
	input := &toOneInput{Rows: []*toOneParent{{ID: &parentID, Child: &toOneChild{ID: &childID}, Has: &toOneHas{ID: true, Child: true}}}}
	snapshot, err := handler.CaptureInput(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	program := snapshot.(*Program)
	if err = program.buildRecordFrames(program.metadata.Root, reflect.ValueOf(input.Rows), nil); err != nil {
		t.Fatalf("build to-one frames: %v", err)
	}
	if len(program.frames.Rows) != 2 || program.frames.Rows[1].Parent != program.frames.Rows[0] {
		t.Fatalf("to-one frames = %+v", program.frames.Rows)
	}
}
