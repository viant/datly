package writer

import (
	"fmt"
	"reflect"
	"testing"

	xhandler "github.com/viant/xdatly/handler"
)

type benchOrder struct {
	ID *int `sqlx:"id,primaryKey=true"`
}
type benchLine struct {
	ID      *int `sqlx:"id,primaryKey=true"`
	OrderID *int `sqlx:"order_id,refTable=orders,refColumn=id"`
}

// benchGraph builds n orders each with one line referencing it, listed lines
// first so ordering has real work to do.
func benchGraph(n int) *Program {
	orders := &Record{Name: "Orders", Path: "Orders", Table: "orders", EntityType: reflect.TypeFor[benchOrder](), Fields: []Field{{Name: "ID", Column: "id", Index: []int{0}}}, Keys: []Field{{Name: "ID", Column: "id", Index: []int{0}}}}
	lines := &Record{Name: "Lines", Path: "Lines", Table: "lines", EntityType: reflect.TypeFor[benchLine](), Fields: []Field{{Name: "ID", Column: "id", Index: []int{0}}, {Name: "OrderID", Column: "order_id", Index: []int{1}, RefTable: "orders", RefColumn: "id"}}, Keys: []Field{{Name: "ID", Column: "id", Index: []int{0}}}}
	program := &Program{frames: &MutationFrames{}}
	for i := 0; i < n; i++ {
		lineID, orderID := 1_000_000+i, i+1
		program.frames.Rows = append(program.frames.Rows, &Frame{Record: lines, Action: xhandler.WriteInsert, Entity: reflect.ValueOf(&benchLine{ID: &lineID, OrderID: &orderID})})
	}
	for i := 0; i < n; i++ {
		orderID := i + 1
		program.frames.Rows = append(program.frames.Rows, &Frame{Record: orders, Action: xhandler.WriteInsert, Entity: reflect.ValueOf(&benchOrder{ID: &orderID})})
	}
	return program
}

func TestOrderFramesByReferencesPlacesReferencedInsertsFirst(t *testing.T) {
	program := benchGraph(50)
	if err := program.orderFramesByReferences(); err != nil {
		t.Fatal(err)
	}
	seen := map[int]bool{}
	for _, frame := range program.frames.Rows {
		switch entity := frame.Entity.Interface().(type) {
		case *benchOrder:
			seen[*entity.ID] = true
		case *benchLine:
			if !seen[*entity.OrderID] {
				t.Fatalf("line %d ordered before order %d", *entity.ID, *entity.OrderID)
			}
			if refs := program.satisfiedGraphReferences(frame); len(refs) != 1 || refs[0].Field != "OrderID" {
				t.Fatalf("line %d graph references = %#v", *entity.ID, refs)
			}
		}
	}
}

func BenchmarkOrderFramesByReferences(b *testing.B) {
	for _, n := range []int{100, 1000, 4000} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				program := benchGraph(n)
				b.StartTimer()
				if err := program.orderFramesByReferences(); err != nil {
					b.Fatal(err)
				}
				for _, frame := range program.frames.Rows {
					program.satisfiedGraphReferences(frame)
				}
			}
		})
	}
}
