package generate

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
)

type factorySparseLinkedRow struct {
	Id     int                     `sqlx:"ID"`
	Flag   *bool                   `sqlx:"FLAG" json:"flag,omitempty"`
	Status *int                    `sqlx:"-" json:"status,omitempty"`
	Has    *factorySparseLinkedHas `setMarker:"true" sqlx:"-" json:"-"`
}
type factorySparseLinkedHas struct{ Id, Flag, Status bool }
type factorySparseMissingMarker struct {
	Id int `sqlx:"ID"`
}
type factorySparseBadMarker struct {
	Id  int                  `sqlx:"ID"`
	Has *struct{ Id string } `setMarker:"true"`
}
type factorySparseValueMarker struct {
	Id  int               `sqlx:"ID"`
	Has struct{ Id bool } `setMarker:"true"`
}
type factorySparseDoublePointerMarker struct {
	Id  int                      `sqlx:"ID"`
	Has **factorySparseLinkedHas `setMarker:"true"`
}
type factorySparseArrayMarker struct {
	Id  int                        `sqlx:"ID"`
	Has *[1]factorySparseLinkedHas `setMarker:"true"`
}
type factoryNamedMarkerList []factorySparseLinkedHas
type factoryNamedMarkerArray [1]factorySparseLinkedHas
type factoryNamedMarkerPointer *factorySparseLinkedHas
type factorySparseNamedListMarker struct {
	Id  int                     `sqlx:"ID"`
	Has *factoryNamedMarkerList `setMarker:"true"`
}
type factorySparseNamedArrayMarker struct {
	Id  int                      `sqlx:"ID"`
	Has *factoryNamedMarkerArray `setMarker:"true"`
}
type factorySparseNamedPointerMarker struct {
	Id  int                        `sqlx:"ID"`
	Has *factoryNamedMarkerPointer `setMarker:"true"`
}
type factorySparseUnprojectedMissingMarker struct {
	Id   int                `sqlx:"ID"`
	Flag bool               `sqlx:"-" json:"flag,omitempty"`
	Has  *struct{ Id bool } `setMarker:"true" json:"-"`
}
type factorySparseAmbiguousMapping struct {
	Id    int                       `sqlx:"ID"`
	Other int                       `sqlx:"ID"`
	Has   *struct{ Id, Other bool } `setMarker:"true"`
}
type factorySparseParent struct {
	Children []*factorySparseLinkedRow
	Has      *struct{ Children bool } `setMarker:"true"`
}

type factoryNestedRowsList []factorySparseLinkedRow
type factoryNestedRowsArray [1]factorySparseLinkedRow
type factoryNestedRowsPointer *factorySparseLinkedRow

type factoryNestedListParent struct {
	Children []*factoryNestedRowsList
	Has      *struct{ Children bool } `setMarker:"true"`
}
type factoryNestedArrayParent struct {
	Children []*factoryNestedRowsArray
	Has      *struct{ Children bool } `setMarker:"true"`
}
type factoryNestedPointerParent struct {
	Children []*factoryNestedRowsPointer
	Has      *struct{ Children bool } `setMarker:"true"`
}

func TestFactoryNestedLinkedBodyRejectsNamedRowContainers(t *testing.T) {
	for _, parent := range []any{factoryNestedListParent{}, factoryNestedArrayParent{}, factoryNestedPointerParent{}} {
		view := &spec.View{Name: "Parents", Relations: []*spec.Relation{{Name: "children", Holder: "Children", Cardinality: spec.CardinalityMany, View: &spec.View{Name: "Children"}}}}
		err := validateFactoryLinkedFixture(t, parent, view)
		if err == nil || !strings.Contains(err.Error(), "must be a named struct") {
			t.Fatalf("parent %T should reject nested row container, got %v", parent, err)
		}
	}
}

func TestFactoryNestedSourceOnlyRowKindAuthority(t *testing.T) {
	const pkg = "example.com/external"
	for _, expression := range []string{"Leaf", "[]Leaf", "[1]Leaf", "*Leaf"} {
		t.Run(expression, func(t *testing.T) {
			catalog := typecatalog.NewCatalog()
			var parent *x.Type
			for name, expression := range map[string]string{
				"Parent":    "struct { Children []*Child; Has *ParentHas `setMarker:\"true\" json:\"-\"` }",
				"ParentHas": "struct { Children bool }", "Child": expression,
				"Leaf":    "struct { Id int `sqlx:\"ID\"`; Has *LeafHas `setMarker:\"true\" json:\"-\"` }",
				"LeafHas": "struct { Id bool }",
			} {
				descriptor := &x.Type{PkgPath: pkg, Name: name, SynteticType: syntheticTypeInPackage(t, pkg, name, expression)}
				if err := catalog.Register(typecatalog.TypeOriginPackage, descriptor); err != nil {
					t.Fatal(err)
				}
				if name == "Parent" {
					parent = descriptor
				}
			}
			resolver, err := typecatalog.NewResolver(catalog, typecatalog.TranscribeAuthority, nil)
			if err != nil {
				t.Fatal(err)
			}
			g := New(Input{TypeResolver: resolver})
			view := &spec.View{Name: "Parents", Relations: []*spec.Relation{{Name: "children", Holder: "Children", Cardinality: spec.CardinalityMany, View: &spec.View{Name: "Children", Columns: []*spec.Column{{Name: "id", Type: spec.TypeRef{Name: "int"}}}}}}}
			r := &planResolver{plan: &Plan{Package: "example.com/consumer"}, types: g.resolver, input: Input{ExternalHandler: &ExternalHandler{GeneratedContracts: true, InputShape: view}}}
			err = r.validateLinkedViewCasts(view, parent)
			if expression == "Leaf" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), "must be a named struct") {
				t.Fatalf("%s: expected named struct rejection, got %v", expression, err)
			}
		})
	}
}

func TestFactoryLinkedBodyValidatesInferredFieldsAndPresence(t *testing.T) {
	for _, tc := range []struct {
		name        string
		row         any
		columns     []*spec.Column
		destination string
		failure     string
	}{
		{name: "inferred field", row: factorySparseLinkedRow{}},
		{name: "nullable false", row: factorySparseLinkedRow{}, columns: []*spec.Column{{Name: "flag", Type: spec.TypeRef{Name: "bool", Pointer: true}, Optional: true}}},
		{name: "authored transient authority", row: factorySparseLinkedRow{}, columns: []*spec.Column{{Name: "status", Type: spec.TypeRef{Name: "int", Pointer: true}, Optional: true}}},
		{name: "inferred mismatch", row: factorySparseLinkedRow{}, columns: []*spec.Column{{Name: "id", Type: spec.TypeRef{Name: "string"}}}, failure: "uneditable linked field"},
		{name: "missing field", row: factorySparseLinkedRow{}, columns: []*spec.Column{{Name: "missing", Type: spec.TypeRef{Name: "int"}}}, failure: "was not found"},
		{name: "physical mapping mismatch", row: factorySparseLinkedRow{}, columns: []*spec.Column{{Name: "id", Source: "OTHER_ID", Type: spec.TypeRef{Name: "int"}}}, failure: "SQLX mapping"},
		{name: "missing marker", row: factorySparseMissingMarker{}, failure: "presence provider"},
		{name: "bad marker member", row: factorySparseBadMarker{}, failure: "must be bool"},
		{name: "value marker", row: factorySparseValueMarker{}, failure: "pointer to a struct"},
		{name: "double pointer marker", row: factorySparseDoublePointerMarker{}, failure: "pointer to a struct"},
		{name: "array marker", row: factorySparseArrayMarker{}, failure: "pointer to a struct"},
		{name: "named list marker", row: factorySparseNamedListMarker{}, failure: "pointer to a struct"},
		{name: "named array marker", row: factorySparseNamedArrayMarker{}, failure: "pointer to a struct"},
		{name: "named pointer marker", row: factorySparseNamedPointerMarker{}, failure: "pointer to a struct"},
		{name: "unprojected transient marker", row: factorySparseUnprojectedMissingMarker{}, failure: "Has.Flag must be bool"},
		{name: "ambiguous mapping", row: factorySparseAmbiguousMapping{}, failure: "ambiguous SQLX"},
		{name: "foreign destination", row: factorySparseLinkedRow{}, destination: "child.go", failure: "generated destination"},
		{name: "named list is not a row", row: factoryNamedMarkerList{}, failure: "must be a named struct"},
		{name: "named array is not a row", row: factoryNamedMarkerArray{}, failure: "must be a named struct"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			columns := tc.columns
			if columns == nil {
				columns = []*spec.Column{{Name: "id", Type: spec.TypeRef{Name: "int"}}}
			}
			view := &spec.View{Name: "Rows", Dest: tc.destination, Columns: columns}
			err := validateFactoryLinkedFixture(t, tc.row, view)
			if tc.failure == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.failure) {
				t.Fatalf("want %q, got %v", tc.failure, err)
			}
		})
	}
}

func TestFactoryLinkedBodyValidatesNestedHolder(t *testing.T) {
	for _, tc := range []struct {
		name, holder, childType string
		cardinality             spec.Cardinality
		failure                 string
	}{
		{name: "nested", holder: "Children", cardinality: spec.CardinalityMany},
		{name: "wrong cardinality", holder: "Children", cardinality: spec.CardinalityOne, failure: "cardinality"},
		{name: "missing holder", holder: "Other", cardinality: spec.CardinalityMany, failure: "holder"},
		{name: "wrong child identity", holder: "Children", cardinality: spec.CardinalityMany, childType: "Wrong", failure: "does not match"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			child := &spec.View{Name: "Rows", TypeName: tc.childType, Columns: []*spec.Column{{Name: "id", Type: spec.TypeRef{Name: "int"}}}}
			view := &spec.View{Name: "Parents", Relations: []*spec.Relation{{Name: "children", Holder: tc.holder, Cardinality: tc.cardinality, View: child}}}
			err := validateFactoryLinkedFixture(t, factorySparseParent{}, view)
			if tc.failure == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.failure) {
				t.Fatalf("want %q, got %v", tc.failure, err)
			}
		})
	}
}

func validateFactoryLinkedFixture(t *testing.T, row any, view *spec.View) error {
	t.Helper()
	catalog := typecatalog.NewCatalog()
	descriptor := x.NewType(reflect.TypeOf(row))
	if err := catalog.Register(typecatalog.TypeOriginPackage, descriptor); err != nil {
		t.Fatal(err)
	}
	resolver, err := typecatalog.NewResolver(catalog, typecatalog.TranscribeAuthority, nil)
	if err != nil {
		t.Fatal(err)
	}
	g := New(Input{TypeResolver: resolver})
	r := &planResolver{plan: &Plan{Package: descriptor.PkgPath}, types: g.resolver, input: Input{ExternalHandler: &ExternalHandler{GeneratedContracts: true, InputShape: view}}}
	before, _ := json.Marshal(view)
	err = r.validateLinkedViewCasts(view, descriptor)
	after, _ := json.Marshal(view)
	if string(before) != string(after) {
		t.Fatal("linked source metadata changed")
	}
	return err
}

func TestFactoryLinkedBodySourceOnlyMarkerAuthority(t *testing.T) {
	const pkg = "example.com/external"
	for _, tc := range []struct {
		name, wrapper, marker string
		failure               string
	}{
		{"valid", "*Has", "Id, Flag bool", ""},
		{"missing public logical marker", "*Has", "Id bool", "Has.Flag must be bool"},
		{"double pointer", "**Has", "Id, Flag bool", "pointer to a struct"},
		{"array pointer", "*[1]Has", "Id, Flag bool", "pointer to a struct"},
		{"named slice", "*Has", "[]Element", "pointer to a struct"},
		{"named array", "*Has", "[1]Element", "pointer to a struct"},
		{"named pointer", "*Has", "*Element", "pointer to a struct"},
		{"named struct", "*Has", "Element", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			catalog := typecatalog.NewCatalog()
			row := &x.Type{PkgPath: pkg, Name: "Row", SynteticType: syntheticTypeInPackage(t, pkg, "Row", "struct { Id int `sqlx:\"ID\"`; Flag bool `sqlx:\"-\" json:\"flag,omitempty\"`; Has "+tc.wrapper+" `setMarker:\"true\" json:\"-\"` }")}
			markerExpression := "struct { " + tc.marker + " }"
			if strings.Contains(tc.marker, "Element") {
				markerExpression = tc.marker
			}
			marker := &x.Type{PkgPath: pkg, Name: "Has", SynteticType: syntheticTypeInPackage(t, pkg, "Has", markerExpression)}
			element := &x.Type{PkgPath: pkg, Name: "Element", SynteticType: syntheticTypeInPackage(t, pkg, "Element", "struct { Id, Flag bool }")}
			for _, descriptor := range []*x.Type{row, marker, element} {
				if err := catalog.Register(typecatalog.TypeOriginPackage, descriptor); err != nil {
					t.Fatal(err)
				}
			}
			resolver, err := typecatalog.NewResolver(catalog, typecatalog.TranscribeAuthority, nil)
			if err != nil {
				t.Fatal(err)
			}
			g := New(Input{TypeResolver: resolver})
			view := &spec.View{Name: "Rows", Columns: []*spec.Column{{Name: "id", Type: spec.TypeRef{Name: "int"}}}}
			r := &planResolver{plan: &Plan{Package: "example.com/parent"}, types: g.resolver, input: Input{ExternalHandler: &ExternalHandler{GeneratedContracts: true, InputShape: view}}}
			err = r.validateLinkedViewCasts(view, row)
			if tc.failure == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.failure) {
				t.Fatalf("want %q, got %v", tc.failure, err)
			}
		})
	}
}
