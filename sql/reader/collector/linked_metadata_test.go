package collector

import (
	"fmt"
	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
	"reflect"
	"testing"
)

func TestHiddenRelationOutputLabel(t *testing.T) {
	link := &Link{Link: &data.Link{Column: "ORDER_ID", Output: "parent_key", Field: "Hidden"}, KeySource: KeySourceColumn}
	relation := &Relation{On: Links{link}}
	c := &Collector{view: &View{Relations: []*Relation{relation}}, values: map[string]*[]any{}}
	*c.ReserveSQLKey("parent_key") = 42
	value, err := c.linkKeyAt(nil, link, 0)
	if err != nil || value != 42 {
		t.Fatalf("%v %v", value, err)
	}
	if !c.SQLKeyColumns()["parent_key"] || c.SQLKeyColumns()["ORDER_ID"] {
		t.Fatal(c.SQLKeyColumns())
	}
	other := *link.Link
	other.Output = "another_key"
	if relationIndexIdentity(link) == relationIndexIdentity(&Link{Link: &other, KeySource: KeySourceColumn}) {
		t.Fatal("different result columns share index")
	}
}
func BenchmarkLinkedKeyAccessor(b *testing.B) {
	type row struct {
		ID int `sqlx:"id"`
	}
	typ := reflect.TypeFor[row]()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, _, err := compileLinkField(typ, "ID"); err != nil {
			b.Fatal(err)
		}
	}
}

func accessorGraphFixture(n int) (*data.View, map[*data.View]reflect.Type) {
	type child struct {
		ParentID int `sqlx:"parent_id"`
	}
	childType := reflect.TypeFor[child]()
	fields := []reflect.StructField{{Name: "ID", Type: reflect.TypeFor[int](), Tag: `sqlx:"id"`}}
	root := &data.View{}
	types := map[*data.View]reflect.Type{}
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("Children%d", i)
		fields = append(fields, reflect.StructField{Name: name, Type: reflect.SliceOf(reflect.PointerTo(childType))})
		view := &data.View{}
		types[view] = childType
		root.Relations = append(root.Relations, &data.Relation{Name: name, Holder: name, Cardinality: spec.CardinalityMany, On: data.Links{{Column: "id", Field: "ID"}}, Of: &data.RelationRef{View: view, On: data.Links{{Column: "parent_id", Field: "ParentID"}}}})
	}
	types[root] = reflect.StructOf(fields)
	return root, types
}
func TestLinkedGraphReusesKeyAccessors(t *testing.T) {
	root, types := accessorGraphFixture(2)
	graph, err := Compile(root, types)
	if err != nil {
		t.Fatal(err)
	}
	a, b := graph.Root.Relations[0], graph.Root.Relations[1]
	if a.On[0].XField != b.On[0].XField || a.Of.On[0].XField != b.Of.On[0].XField {
		t.Fatal("same typed relation key was compiled more than once")
	}
	if a.On[0].XField == a.Of.On[0].XField {
		t.Fatal("different row types share an accessor")
	}
	other, err := Compile(root, types)
	if err != nil {
		t.Fatal(err)
	}
	if other.Root.Relations[0].On[0].XField == a.On[0].XField {
		t.Fatal("accessor cache leaked between compilations")
	}
}
func BenchmarkLinkedAccessorGraph(b *testing.B) {
	for _, count := range []int{1, 16} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			root, types := accessorGraphFixture(count)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := Compile(root, types); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestLinkedAccessorCachePreservesProvenance(t *testing.T) {
	type row struct {
		ID      int
		Hidden  int `sqlx:"-"`
		Hook    int `sqlx:"-" relationKey:"hook"`
		Invalid int `relationKey:"invalid"`
	}
	compiler := &graphCompiler{keyFields: map[keyFieldIdentity]compiledKeyField{}}
	for _, tc := range []struct {
		name   string
		source KeySource
		typed  bool
	}{{"ID", KeySourceField, true}, {"Hidden", KeySourceColumn, false}, {"Absent", KeySourceColumn, false}, {"Hook", KeySourceHook, true}} {
		first, source, err := compiler.linkField(reflect.TypeFor[row](), tc.name)
		if err != nil || source != tc.source || (first != nil) != tc.typed {
			t.Fatalf("%s %v %v", tc.name, source, err)
		}
		second, source, err := compiler.linkField(reflect.TypeFor[*row](), tc.name)
		if err != nil || source != tc.source || first != second {
			t.Fatalf("cached %s %v %v", tc.name, source, err)
		}
	}
	for i := 0; i < 2; i++ {
		if _, _, err := compiler.linkField(reflect.TypeFor[row](), "Invalid"); err == nil {
			t.Fatal("invalid provenance accepted")
		}
	}
}
