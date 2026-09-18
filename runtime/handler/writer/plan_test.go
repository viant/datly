package writer

import (
	"context"
	"reflect"
	"testing"

	"github.com/viant/datly/spec"
)

type unitHas struct {
	ID, Start, End, Name, Remove bool
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
