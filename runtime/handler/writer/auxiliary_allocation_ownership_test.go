package writer

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	xhandler "github.com/viant/xdatly/handler"
)

type allocationAdmissionChild struct {
	ID       *int    `sqlx:"id,primaryKey,autoincrement"`
	ParentID *int    `sqlx:"parent_id,refTable=parents,refColumn=id"`
	Label    *string `sqlx:"label"`
}
type allocationAdmissionParent struct {
	ID       *int                        `sqlx:"id,primaryKey,autoincrement"`
	Children []*allocationAdmissionChild `view:"Children,table=children" on:"ID=ParentID"`
}
type allocationAdmissionInput struct {
	Rows            []*allocationAdmissionParent `parameter:"Rows,kind=body,in=data" view:"Rows,table=parents"`
	CurrentRows     []*allocationAdmissionParent `parameter:"CurrentRows,kind=view" view:"CurrentRows,table=parents"`
	CurrentChildren []*allocationAdmissionChild  `parameter:"CurrentChildren,kind=view" view:"CurrentChildren,table=children"`
}
type allocationAdmissionOutput struct {
	Data []*allocationAdmissionParent `parameter:"Data,kind=output,in=body"`
}

// Genuine framework validation must consult database evidence for an auxiliary
// parent, rather than accepting its insert-shaped lifecycle action as a producer.
func TestAuxiliaryAllocationPhysicalProducerAdmissionSQLite(t *testing.T) {
	for _, tc := range []struct {
		name      string
		id        int
		child     bool
		wantError bool
	}{
		{"unresolved required link", 0, true, true},
		{"absent supplied parent", 42, true, true},
		{"supplied database parent", 1, true, false},
		{"zero auxiliary leaf", 0, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			db := sqlite.New(t)
			db.DB.SetMaxOpenConns(1)
			if err := db.ExecStatements(ctx, strings.Split(sqSchema, "\n")...); err != nil {
				t.Fatal(err)
			}
			if err := db.ExecStatements(ctx, "INSERT INTO parents(id,name) VALUES(1,'original')"); err != nil {
				t.Fatal(err)
			}
			writes := sqObserveWrites(t, ctx, db)
			row := &allocationAdmissionParent{ID: ptr(tc.id)}
			var child *allocationAdmissionChild
			if tc.child {
				child = &allocationAdmissionChild{ParentID: ptr(tc.id), Label: ptr("child")}
				row.Children = []*allocationAdmissionChild{child}
			}
			in := &allocationAdmissionInput{Rows: []*allocationAdmissionParent{row}, CurrentRows: []*allocationAdmissionParent{{ID: ptr(1)}}}
			component := sqComponent(reflect.TypeFor[allocationAdmissionParent]().PkgPath(), "patch", "")
			component.RootView.Auxiliary = true
			result, err := runSQLiteWriter(t, ctx, db, component, in, &allocationAdmissionOutput{}, "patch")
			if (err != nil) != tc.wantError {
				t.Fatalf("error=%v wantError=%v", err, tc.wantError)
			}
			if writes["parents"].Load() != 0 || tc.wantError && writes["children"].Load() != 0 {
				t.Fatalf("invalid physical producer performed DML: parents=%d children=%d error=%v", writes["parents"].Load(), writes["children"].Load(), err)
			}
			if tc.wantError {
				t.Logf("rejected before physical DML: %v", err)
				if child.ID != nil && *child.ID != 0 {
					t.Fatal("rejected producer reached sequence allocation")
				}
			} else {
				out := result.(*allocationAdmissionOutput)
				if len(out.Data) != 1 || out.Data[0] != row {
					t.Fatal("output pointer changed")
				}
				if tc.child && (child.ID == nil || *child.ID == 0 || writes["children"].Load() != 1) {
					t.Fatal("valid physical descendant not allocated/persisted")
				}
			}
			if *row.ID != tc.id {
				t.Fatal("auxiliary supplied/zero identity changed")
			}
		})
	}
}

func TestAuxiliaryAllocationRootMetadataKeepsSQLIdentity(t *testing.T) {
	for _, operation := range []string{"patch", "post", "put"} {
		t.Run(operation, func(t *testing.T) {
			component := unitComponent()
			component.RootView.Auxiliary = true
			before := *component.RootView.Columns[0]
			metadata, e := Compile(component, reflect.TypeFor[unitInput](), reflect.TypeFor[unitOutput](), operation)
			if e != nil {
				t.Fatal(e)
			}
			if metadata.Sequence != nil || metadata.Root.Sequence != nil || metadata.Root.Selector != "" || len(metadata.Root.Keys) != 1 || metadata.Root.CurrentField < 0 {
				t.Fatalf("auxiliary executable plan remains: %+v", metadata.Root)
			}
			if *component.RootView.Columns[0] != before || !before.PrimaryKey || !before.AutoIncrement || !strings.Contains(metadata.EntityType.Field(0).Tag.Get("sqlx"), "autoincrement=true") {
				t.Fatal("truthful SQL identity/Current metadata changed")
			}
		})
	}
}
func TestAuxiliaryAllocationNestedNumericMetadata(t *testing.T) {
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: reflect.TypeFor[auxiliaryChildParent]().PkgPath(), Name: "Rows"}, Name: "Rows", Settings: &spec.Settings{Mutation: "post"}, RootView: &spec.View{Name: "Rows", Source: &spec.ViewSource{Table: "parents"}, Columns: []*spec.Column{{Name: "id", Source: "id", PrimaryKey: true}}}}
	metadata, e := Compile(component, reflect.TypeFor[auxiliaryChildInput](), reflect.TypeFor[auxiliaryChildOutput](), "post")
	if e != nil {
		t.Fatal(e)
	}
	child := metadata.Root.Relations[0].Child
	if !child.Auxiliary || child.Sequence != nil || child.Selector != "" || len(child.Keys) != 1 || len(metadata.Root.Relations[0].Links) != 1 || metadata.Root.Sequence == nil {
		t.Fatalf("numeric auxiliary/physical ownership drift: %+v %+v", metadata.Root, child)
	}
}

func TestAuxiliaryAllocationAllInferencePathsKeepTruthfulSchema(t *testing.T) {
	for _, autoColumn := range []bool{false, true} {
		component := sqComponent(reflect.TypeFor[allocationAdmissionParent]().PkgPath(), "patch", "")
		component.RootView.Auxiliary = true
		component.RootView.Columns[0].AutoIncrement = autoColumn
		childView := component.RootView.Relations[0].View
		childView.Auxiliary = true
		childView.Columns[0].AutoIncrement = autoColumn
		metadata, err := Compile(component, reflect.TypeFor[allocationAdmissionInput](), reflect.TypeFor[allocationAdmissionOutput](), "patch")
		if err != nil {
			t.Fatal(err)
		}
		for _, record := range []*Record{metadata.Root, metadata.Root.Relations[0].Child} {
			if !record.Auxiliary || record.Sequence != nil || record.Selector != "" || len(record.Keys) != 1 || record.CurrentField < 0 || !strings.Contains(record.EntityType.Field(0).Tag.Get("sqlx"), "autoincrement") {
				t.Fatalf("autoincrement tag inference remains executable or metadata lost: %+v", record)
			}
		}
		if component.RootView.Columns[0].AutoIncrement != autoColumn || childView.Columns[0].AutoIncrement != autoColumn {
			t.Fatal("authored SQL metadata changed")
		}
		// Numeric-primary-key inference does not depend on autoincrement tags.
		numeric := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: reflect.TypeFor[auxiliaryChildParent]().PkgPath(), Name: "Rows"}, Name: "Rows", Settings: &spec.Settings{Mutation: "post"}, RootView: &spec.View{Name: "Rows", Auxiliary: true, Source: &spec.ViewSource{Table: "parents"}, Columns: []*spec.Column{{Name: "id", Source: "id", PrimaryKey: true, AutoIncrement: autoColumn}}}}
		got, err := Compile(numeric, reflect.TypeFor[auxiliaryChildInput](), reflect.TypeFor[auxiliaryChildOutput](), "post")
		if err != nil {
			t.Fatal(err)
		}
		for _, record := range []*Record{got.Root, got.Root.Relations[0].Child} {
			if record.Sequence != nil || record.Selector != "" || len(record.Keys) != 1 {
				t.Fatalf("numeric-only inference executable: %+v", record)
			}
		}
	}
}
func TestAuxiliaryInsertLabelCannotProducePhysicalReference(t *testing.T) {
	type parent struct{ ID *int }
	type child struct{ ID, ParentID *int }
	for _, value := range []int{0, 41} {
		t.Run(map[bool]string{true: "unresolved", false: "supplied"}[value == 0], func(t *testing.T) {
			id, childID := value, 3
			parentField := Field{Name: "ID", Column: "id", Index: []int{0}}
			childField := Field{Name: "ParentID", Column: "parent_id", Index: []int{1}, RefTable: "parents", RefColumn: "id"}
			parentRecord := &Record{Auxiliary: true, Table: "parents", EntityType: reflect.TypeFor[parent](), Fields: []Field{parentField}}
			childRecord := &Record{Table: "children", EntityType: reflect.TypeFor[child](), Fields: []Field{{Name: "ID", Column: "id", Index: []int{0}}, childField}}
			parentRecord.Relations = []*Relation{{Child: childRecord, Links: []Link{{Parent: parentField, Child: childField}}}}
			parentFrame := &Frame{Record: parentRecord, Action: xhandler.WriteInsert, Entity: reflect.ValueOf(&parent{ID: &id})}
			childFrame := &Frame{Record: childRecord, Action: xhandler.WriteInsert, Entity: reflect.ValueOf(&child{ID: &childID, ParentID: &id}), Parent: parentFrame}
			program := &Program{frames: &MutationFrames{Rows: []*Frame{parentFrame, childFrame}}}
			for _, started := range []bool{false, true} {
				options := program.validationOptions(childFrame, started)
				if len(options.SatisfiedReferences) != 0 || options.DeferredFields != nil {
					t.Fatalf("auxiliary insert label authorized/deferred physical reference: %+v", options)
				}
			}
			expected := 1
			if value != 0 {
				expected = 2
			}
			if len(program.graphIndex().inserts) != expected {
				t.Fatalf("only actual child fields may appear as physical producers: %+v", program.graphIndex().inserts)
			}
			parentRecord.Auxiliary = false
			program.graph = nil
			if value != 0 && len(program.validationOptions(childFrame, true).SatisfiedReferences) != 1 {
				t.Fatal("legitimate physical producer authority lost")
			}
		})
	}
}
