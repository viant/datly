package writer

import (
	"context"
	"reflect"
	"testing"

	"github.com/viant/datly/internal/testharness/sqlite"
	xhandler "github.com/viant/xdatly/handler"
)

type graphCacheParent struct {
	ID       *int               `sqlx:"id,primaryKey,autoincrement"`
	Children []*graphCacheChild `view:"Children,table=children" on:"ID=ParentID"`
}
type graphCacheChild struct {
	ID             *int `sqlx:"id,primaryKey,autoincrement"`
	ParentID       *int `sqlx:"parent_id,refTable=parents,refColumn=id"`
	CopiedParentID *int `sqlx:"copied_parent_id,refTable=parents,refColumn=id"`
}
type graphCacheInput struct {
	Rows []*graphCacheParent `parameter:"Rows,kind=body,in=data" view:"Rows,table=parents"`
}
type graphCacheOutput struct {
	Data []*graphCacheParent `parameter:"Data,kind=output,in=body"`
}
type graphCacheHooks struct{}

var _graphCacheHookType = reflect.TypeFor[graphCacheHooks]()

func (*graphCacheHooks) Init(context.Context, *graphCacheParent, xhandler.LifecycleContext[graphCacheParent, xhandler.NoParent, graphCacheOutput]) error {
	return nil
}
func (*graphCacheHooks) Validate(context.Context, *graphCacheParent, xhandler.LifecycleContext[graphCacheParent, xhandler.NoParent, graphCacheOutput]) error {
	return nil
}
func (*graphCacheHooks) AfterSequence(_ context.Context, parent *graphCacheParent, _ xhandler.LifecycleContext[graphCacheParent, xhandler.NoParent, graphCacheOutput]) error {
	for _, child := range parent.Children {
		child.CopiedParentID = ptr(*parent.ID)
	}
	return nil
}

// The initial reference pass indexes an unresolved AUTO_INCREMENT ID. Normal
// allocation and AfterSequence later supply a second FK to that pending insert.
// Direct relation validation already covers ParentID; CopiedParentID must use
// fresh graph evidence rather than querying a parent not inserted in SQL yet.
func TestGraphReferenceCacheAfterNativeAllocationSQLite(t *testing.T) {
	for _, suppliedZero := range []bool{false, true} {
		t.Run(map[bool]string{false: "omitted ID", true: "supplied zero ID"}[suppliedZero], func(t *testing.T) {
			ctx := context.Background()
			db := sqlite.New(t)
			db.DB.SetMaxOpenConns(1)
			if err := db.ExecStatements(ctx, "CREATE TABLE parents(id INTEGER PRIMARY KEY AUTOINCREMENT)", "CREATE TABLE children(id INTEGER PRIMARY KEY AUTOINCREMENT,parent_id INTEGER NOT NULL REFERENCES parents(id),copied_parent_id INTEGER REFERENCES parents(id))"); err != nil {
				t.Fatal(err)
			}
			parent := &graphCacheParent{Children: []*graphCacheChild{{}}}
			if suppliedZero {
				parent.ID = ptr(0)
			}
			input := &graphCacheInput{Rows: []*graphCacheParent{parent}}
			component := sqComponent(reflect.TypeFor[graphCacheParent]().PkgPath(), "post", "graphCacheHooks")
			if _, err := runSQLiteWriter(t, ctx, db, component, input, &graphCacheOutput{}, "post"); err != nil {
				t.Fatal(err)
			}
			if parent.ID == nil || *parent.ID == 0 || parent.Children[0].ParentID == nil || *parent.Children[0].ParentID != *parent.ID || parent.Children[0].CopiedParentID == nil || *parent.Children[0].CopiedParentID != *parent.ID {
				t.Fatal("allocated identities/links were not preserved")
			}
			var count int
			if err := db.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM children c JOIN parents p ON p.id=c.parent_id AND p.id=c.copied_parent_id").Scan(&count); err != nil || count != 1 {
				t.Fatalf("persisted graph count=%d err=%v", count, err)
			}
		})
	}
}

func TestGraphReferenceCacheReusesRefreshedIndex(t *testing.T) {
	program := benchGraph(50)
	if err := program.orderFramesByReferences(); err != nil {
		t.Fatal(err)
	}
	before := program.graphIndex()
	for _, frame := range program.frames.Rows {
		switch row := frame.Entity.Interface().(type) {
		case *benchOrder:
			*row.ID += 100
		case *benchLine:
			*row.OrderID += 100
		}
	}
	if err := program.reconcileLinks(true); err != nil {
		t.Fatal(err)
	}
	after := program.graphIndex()
	if after == before {
		t.Fatal("phase retained stale value-keyed graph index")
	}
	for _, frame := range program.frames.Rows {
		if _, ok := frame.Entity.Interface().(*benchLine); ok {
			references := program.satisfiedGraphReferences(frame)
			if len(references) != 1 || references[0].Field != "OrderID" {
				t.Fatal("mutated producer reference lost", references)
			}
		}
		if program.graphIndex() != after {
			t.Fatal("unchanged validation graph rebuilt its index per frame")
		}
	}
}
