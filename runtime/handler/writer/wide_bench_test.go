package writer

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/spec"
)

// wideTypes builds an entity with fieldCount string columns plus a setMarker
// holder, and the matching writer input/output contracts, to model real
// records with hundreds of fields.
func wideTypes(fieldCount int) (entity, input, output reflect.Type) {
	hasFields := []reflect.StructField{{Name: "ID", Type: reflect.TypeOf(false)}}
	entityFields := []reflect.StructField{{Name: "ID", Type: reflect.TypeOf((*int)(nil)), Tag: `sqlx:"id,primaryKey=true"`}}
	for i := 0; i < fieldCount; i++ {
		name := fmt.Sprintf("F%d", i)
		hasFields = append(hasFields, reflect.StructField{Name: name, Type: reflect.TypeOf(false)})
		entityFields = append(entityFields, reflect.StructField{Name: name, Type: reflect.TypeOf((*string)(nil)), Tag: reflect.StructTag(fmt.Sprintf(`sqlx:"f%d"`, i))})
	}
	hasType := reflect.StructOf(hasFields)
	entityFields = append(entityFields, reflect.StructField{Name: "Has", Type: reflect.PointerTo(hasType), Tag: `setMarker:"true" sqlx:"-"`})
	entity = reflect.StructOf(entityFields)
	rows := reflect.SliceOf(reflect.PointerTo(entity))
	input = reflect.StructOf([]reflect.StructField{
		{Name: "Rows", Type: rows, Tag: `parameter:"Rows,kind=body,in=data" view:"Rows,table=wide"`},
		{Name: "CurrentRows", Type: rows, Tag: `parameter:"CurrentRows,kind=view" view:"CurrentRows,table=wide"`},
	})
	output = reflect.StructOf([]reflect.StructField{
		{Name: "Data", Type: rows, Tag: `parameter:"Data,kind=output,in=body"`},
		{Name: "Status", Type: reflect.TypeOf("")},
	})
	return entity, input, output
}

// wideInput creates rowCount rows that supply every field, plus the matching
// Current rows, so each frame is a sparse update touching all columns.
func wideInput(entity, inputType reflect.Type, rowCount, fieldCount int) any {
	input := reflect.New(inputType)
	rows := reflect.MakeSlice(inputType.Field(0).Type, 0, rowCount)
	current := reflect.MakeSlice(inputType.Field(1).Type, 0, rowCount)
	hasType := entity.Field(fieldCount + 1).Type.Elem()
	for i := 0; i < rowCount; i++ {
		row, previous := reflect.New(entity), reflect.New(entity)
		id := i + 1
		row.Elem().Field(0).Set(reflect.ValueOf(&id))
		previous.Elem().Field(0).Set(reflect.ValueOf(&id))
		has := reflect.New(hasType)
		for f := 0; f < hasType.NumField(); f++ {
			has.Elem().Field(f).SetBool(true)
		}
		for f := 1; f <= fieldCount; f++ {
			changed, original := fmt.Sprintf("v%d", i), "original"
			row.Elem().Field(f).Set(reflect.ValueOf(&changed))
			previous.Elem().Field(f).Set(reflect.ValueOf(&original))
		}
		row.Elem().Field(fieldCount + 1).Set(has)
		rows = reflect.Append(rows, row)
		current = reflect.Append(current, previous)
	}
	input.Elem().Field(0).Set(rows)
	input.Elem().Field(1).Set(current)
	return input.Interface()
}

func wideComponent() *spec.Component {
	return &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "bench", Name: "Rows"}, Name: "Rows", Settings: &spec.Settings{Mutation: "patch"},
		RootView: &spec.View{Name: "Rows", Source: &spec.ViewSource{Table: "wide"}, Columns: []*spec.Column{{Name: "id", Source: "id", PrimaryKey: true}}}}
}

func TestWideEntityPatchUpdatesEveryRow(t *testing.T) {
	entity, inputType, outputType := wideTypes(50)
	handler, err := New(wideComponent(), inputType, outputType, "patch")
	if err != nil {
		t.Fatal(err)
	}
	input := wideInput(entity, inputType, 7, 50)
	capabilities := &fakeCapabilities{}
	snapshot, err := handler.CaptureInput(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = handler.Execute(context.Background(), rhandler.Invocation{Input: input, Snapshot: snapshot, Binder: capabilities}); err != nil {
		t.Fatal(err)
	}
	if capabilities.updates != 7 {
		t.Fatalf("updates = %d, want 7", capabilities.updates)
	}
}

func BenchmarkPrepareWideEntity(b *testing.B) {
	for _, shape := range []struct{ rows, fields int }{{200, 200}, {1000, 200}, {200, 500}} {
		b.Run(fmt.Sprintf("%drows_x_%dfields", shape.rows, shape.fields), func(b *testing.B) {
			b.ReportAllocs()
			entity, inputType, outputType := wideTypes(shape.fields)
			handler, err := New(wideComponent(), inputType, outputType, "patch")
			if err != nil {
				b.Fatal(err)
			}
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				input := wideInput(entity, inputType, shape.rows, shape.fields)
				b.StartTimer()
				snapshot, err := handler.CaptureInput(context.Background(), input)
				if err != nil {
					b.Fatal(err)
				}
				if _, err = handler.Execute(context.Background(), rhandler.Invocation{Input: input, Snapshot: snapshot, Binder: &fakeCapabilities{}}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
