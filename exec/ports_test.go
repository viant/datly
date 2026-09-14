package exec

import (
	"context"
	"reflect"
	"testing"

	"github.com/viant/datly/spec"
	"github.com/viant/sqlx"
	xhandler "github.com/viant/xdatly/handler"
)

func TestComponentTargetStringIncludesNormalizedComponentAndRouteIdentity(t *testing.T) {
	target := ComponentTarget{
		Component: spec.Key{Kind: spec.KindComponent, Scope: "example.com/api", Name: "Users"},
		Route:     spec.RouteRef{Method: "get", Path: "/users"},
	}
	if actual := target.String(); actual != "component:example.com/api:Users GET:/users" {
		t.Fatalf("ComponentTarget.String() = %q", actual)
	}
}

type portData struct {
	operations []string
}

func (d *portData) Insert(table string, _ any) error {
	d.operations = append(d.operations, "insert:"+table)
	return nil
}

func (d *portData) Update(table string, _ any) error {
	d.operations = append(d.operations, "update:"+table)
	return nil
}

func (d *portData) Delete(table string, _ any) error {
	d.operations = append(d.operations, "delete:"+table)
	return nil
}

func (d *portData) Execute(statement string, _ ...any) error {
	d.operations = append(d.operations, "execute:"+statement)
	return nil
}

func (d *portData) Allocate(context.Context, string, any, string) error { return nil }
func (d *portData) Flush(context.Context, string) error                 { return nil }

type portDataSource struct{ data xhandler.Data }

func (s portDataSource) Open(context.Context) (xhandler.Data, error) { return s.data, nil }

func TestDataSourceOpensCompleteHandlerDataCapability(t *testing.T) {
	data := &portData{}
	var dml xhandler.DML = data
	if methods := interfaceMethodNames(reflect.TypeFor[xhandler.DML]()); !reflect.DeepEqual(methods, []string{"Delete", "Execute", "Insert", "Update"}) {
		t.Fatalf("DML methods = %v", methods)
	}
	if methods := interfaceMethodNames(reflect.TypeFor[xhandler.Data]()); !reflect.DeepEqual(methods, []string{"Allocate", "Delete", "Execute", "Flush", "Insert", "Update"}) {
		t.Fatalf("Data methods = %v", methods)
	}
	if err := dml.Insert("events", struct{}{}); err != nil {
		t.Fatal(err)
	}
	var sequencer xhandler.Sequencer = data
	if err := sequencer.Allocate(context.Background(), "events", new(int), "ID"); err != nil {
		t.Fatal(err)
	}
	resolved, err := (portDataSource{data: data}).Open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err = resolved.Flush(context.Background(), "events"); err != nil {
		t.Fatal(err)
	}
	if len(data.operations) != 1 || data.operations[0] != "insert:events" {
		t.Fatalf("operations = %v", data.operations)
	}
}

type portReader struct{ input any }

func (r *portReader) Read(_ context.Context, input any, _ xhandler.Binder, _ sqlx.ParameterResolver) (any, error) {
	r.input = input
	return input, nil
}

type portPredicate struct{}

func (portPredicate) ContextType() reflect.Type { return reflect.TypeOf(struct{}{}) }
func (portPredicate) NewContext(context.Context, xhandler.Binder, func(...any)) (any, error) {
	return struct{}{}, nil
}

func TestReadAndPredicatePortsRemainImplementationNeutral(t *testing.T) {
	input := &struct{ ID int }{ID: 7}
	reader := &portReader{}
	actual, err := Reader(reader).Read(context.Background(), input, nil, nil)
	if err != nil || actual != input || reader.input != input {
		t.Fatalf("Read() = (%v, %v), captured=%v", actual, err, reader.input)
	}
	predicate := PredicateEvaluator(portPredicate{})
	if predicate.ContextType() != reflect.TypeOf(struct{}{}) {
		t.Fatalf("ContextType() = %v", predicate.ContextType())
	}
	if actual, err = predicate.NewContext(context.Background(), nil, nil); err != nil || reflect.TypeOf(actual) != reflect.TypeOf(struct{}{}) {
		t.Fatalf("NewContext() = (%T, %v)", actual, err)
	}
}

func interfaceMethodNames(typeOf reflect.Type) []string {
	result := make([]string, typeOf.NumMethod())
	for index := 0; index < typeOf.NumMethod(); index++ {
		result[index] = typeOf.Method(index).Name
	}
	return result
}
