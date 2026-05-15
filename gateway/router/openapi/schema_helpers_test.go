package openapi

import (
	"context"
	"reflect"
	"testing"

	"github.com/viant/datly/gateway/router/openapi/openapi3"
	"github.com/viant/datly/repository"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
)

type sampleNested struct {
	ID int
}

type sampleWithField struct {
	UserName string `json:"user_name" desc:"user name desc" example:"bob"`
}

func TestSchemaSliceItem(t *testing.T) {
	tests := []struct {
		name         string
		typeName     string
		rType        reflect.Type
		expectType   reflect.Type
		expectSchema string
	}{
		{name: "named element", typeName: "Entry", rType: reflect.TypeOf([]sampleNested{}), expectType: reflect.TypeOf(sampleNested{}), expectSchema: "sampleNested"},
		{name: "anonymous element", typeName: "Entry", rType: reflect.TypeOf([]struct{ Value int }{}), expectType: reflect.TypeOf(struct{ Value int }{}), expectSchema: "EntryItem"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Schema{tag: Tag{TypeName: tt.typeName}}
			item := s.SliceItem(tt.rType)
			if item.rType != tt.expectType {
				t.Fatalf("expected %v, got %v", tt.expectType, item.rType)
			}
			if item.tag.TypeName != tt.expectSchema {
				t.Fatalf("expected %q, got %q", tt.expectSchema, item.tag.TypeName)
			}
		})
	}
}

func TestSchemaField(t *testing.T) {
	component := &repository.Component{}
	rType := reflect.TypeOf(sampleWithField{})
	field := rType.Field(0)

	tests := []struct {
		name          string
		tag           *Tag
		expectField   string
		expectDesc    string
		expectExample string
	}{
		{name: "uses json name", tag: &Tag{JSONName: "custom_name"}, expectField: "custom_name", expectDesc: "user name desc", expectExample: "bob"},
		{name: "falls back to formatted name", tag: &Tag{}, expectField: "UserName", expectDesc: "user name desc", expectExample: "bob"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Schema{ioConfig: component.IOConfig()}
			got, err := s.Field(field, tt.tag)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.fieldName != tt.expectField {
				t.Fatalf("expected field %q, got %q", tt.expectField, got.fieldName)
			}
			if got.description != tt.expectDesc {
				t.Fatalf("expected description %q, got %q", tt.expectDesc, got.description)
			}
			if got.example != tt.expectExample {
				t.Fatalf("expected example %q, got %q", tt.expectExample, got.example)
			}
		})
	}
}

func TestContainsAny(t *testing.T) {
	tests := []struct {
		name   string
		format string
		values []string
		expect bool
	}{
		{name: "contains", format: "2006-01-02T15:04:05", values: []string{"15", "04"}, expect: true},
		{name: "not contains", format: "2006-01-02", values: []string{"15", "04", "05"}, expect: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := containsAny(tt.format, tt.values...)
			if got != tt.expect {
				t.Fatalf("expected %v, got %v", tt.expect, got)
			}
		})
	}
}

func TestAsOpenAPIType(t *testing.T) {
	container := NewContainer()
	tests := []struct {
		name   string
		rType  reflect.Type
		api    string
		format string
		ok     bool
	}{
		{name: "int64", rType: reflect.TypeOf(int64(1)), api: integerOutput, format: int64Format, ok: true},
		{name: "uint32", rType: reflect.TypeOf(uint32(1)), api: integerOutput, format: int32Format, ok: true},
		{name: "float64", rType: reflect.TypeOf(float64(1)), api: numberOutput, format: doubleFormat, ok: true},
		{name: "bool", rType: reflect.TypeOf(true), api: booleanOutput, format: empty, ok: true},
		{name: "string", rType: reflect.TypeOf(""), api: stringOutput, format: empty, ok: true},
		{name: "ptr", rType: reflect.TypeOf(new(int)), api: integerOutput, format: int64Format, ok: true},
		{name: "unsupported struct", rType: reflect.TypeOf(struct{}{}), api: empty, format: empty, ok: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			api, format, ok := container.asOpenApiType(tt.rType)
			if ok != tt.ok {
				t.Fatalf("expected ok=%v, got %v", tt.ok, ok)
			}
			if api != tt.api || format != tt.format {
				t.Fatalf("expected %s/%s, got %s/%s", tt.api, tt.format, api, format)
			}
		})
	}
}

func TestToOpenApiType(t *testing.T) {
	container := NewContainer()
	tests := []struct {
		name      string
		rType     reflect.Type
		wantError bool
	}{
		{name: "supported", rType: reflect.TypeOf(int(1)), wantError: false},
		{name: "unsupported", rType: reflect.TypeOf(struct{}{}), wantError: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := container.toOpenApiType(tt.rType)
			if (err != nil) != tt.wantError {
				t.Fatalf("wantError=%v got err=%v", tt.wantError, err)
			}
		})
	}
}

func TestSchemaRef(t *testing.T) {
	container := NewContainer()
	tests := []struct {
		name        string
		schemaName  string
		description string
		expectRef   string
	}{
		{name: "basic", schemaName: "MyType", description: "desc", expectRef: "#/components/schemas/MyType"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := container.SchemaRef(tt.schemaName, tt.description)
			if got.Ref != tt.expectRef {
				t.Fatalf("expected ref %q, got %q", tt.expectRef, got.Ref)
			}
			if got.Description != tt.description {
				t.Fatalf("expected description %q, got %q", tt.description, got.Description)
			}
		})
	}
}

func TestUpdatedDocumentation(t *testing.T) {
	tests := []struct {
		name          string
		tag           *Tag
		docs          *state.Docs
		field         *Schema
		expectDesc    string
		expectExample string
	}{
		{
			name:       "column docs",
			tag:        &Tag{Table: "users", Column: "name"},
			docs:       &state.Docs{Columns: state.Documentation{"users.name": "column desc", "users.name$example": "alice"}},
			field:      &Schema{path: "pkg.User.Name", name: "Name"},
			expectDesc: "column desc", expectExample: "alice",
		},
		{
			name:       "path docs fallback",
			tag:        &Tag{},
			docs:       &state.Docs{Paths: state.Documentation{"pkg.User.Name": "path desc"}},
			field:      &Schema{path: "pkg.User.Name", name: "Name"},
			expectDesc: "path desc", expectExample: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			updatedDocumentation(tt.tag, tt.docs, tt.field)
			if tt.field.description != tt.expectDesc {
				t.Fatalf("expected description %q, got %q", tt.expectDesc, tt.field.description)
			}
			if tt.field.example != tt.expectExample {
				t.Fatalf("expected example %q, got %q", tt.expectExample, tt.field.example)
			}
		})
	}
}

func TestMatchesViewTable(t *testing.T) {
	v := &view.View{Table: "users", Alias: "u", Name: "UsersView"}
	tests := []struct {
		name   string
		table  string
		expect bool
	}{
		{name: "table", table: "users", expect: true},
		{name: "alias", table: "u", expect: true},
		{name: "name", table: "UsersView", expect: true},
		{name: "empty", table: "", expect: true},
		{name: "miss", table: "products", expect: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := matchesViewTable(v, tt.table); got != tt.expect {
				t.Fatalf("expected %v, got %v", tt.expect, got)
			}
		})
	}
}

func TestHasInternalColumnTag(t *testing.T) {
	tag := `internal:"true"`
	relTag := `internal:"true"`
	v := &view.View{
		Table: "users",
		ColumnsConfig: map[string]*view.ColumnConfig{
			"ID": {Tag: &tag},
		},
		With: []*view.Relation{
			{Of: &view.ReferenceView{View: view.View{Table: "orders", ColumnsConfig: map[string]*view.ColumnConfig{"OrderID": {Tag: &relTag}}}}},
		},
	}

	tests := []struct {
		name   string
		view   *view.View
		table  string
		column string
		expect bool
	}{
		{name: "nil view", view: nil, table: "users", column: "ID", expect: false},
		{name: "empty column", view: v, table: "users", column: "", expect: false},
		{name: "current view", view: v, table: "users", column: "ID", expect: true},
		{name: "relation", view: v, table: "orders", column: "OrderID", expect: true},
		{name: "not internal", view: v, table: "users", column: "Name", expect: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := hasInternalColumnTag(tt.view, tt.table, tt.column); got != tt.expect {
				t.Fatalf("expected %v, got %v", tt.expect, got)
			}
		})
	}
}

func TestAddToSchemaSimpleBranches(t *testing.T) {
	container := NewContainer()
	component := &ComponentSchema{component: &repository.Component{View: &view.View{}}, schemas: container}
	type mapRecord struct {
		ID int `json:"id"`
	}
	tests := []struct {
		name                    string
		rType                   reflect.Type
		expectType              string
		expectPropsLen          int
		expectAdditionalType    string
		expectAdditionalItemRef bool
	}{
		{name: "interface", rType: reflect.TypeOf((*interface{})(nil)).Elem(), expectType: objectOutput, expectPropsLen: 0},
		{name: "map primitive value", rType: reflect.TypeOf(map[string]int{}), expectType: objectOutput, expectPropsLen: 0, expectAdditionalType: integerOutput},
		{name: "map array value", rType: reflect.TypeOf(map[string][]string{}), expectType: objectOutput, expectPropsLen: 0, expectAdditionalType: arrayOutput},
		{name: "map object value", rType: reflect.TypeOf(map[string]mapRecord{}), expectType: objectOutput, expectPropsLen: 0, expectAdditionalItemRef: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dst := &openapi3.Schema{}
			err := container.addToSchema(context.Background(), component, dst, &Schema{rType: tt.rType, ioConfig: component.component.IOConfig()})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if dst.Type != tt.expectType {
				t.Fatalf("expected type %q, got %q", tt.expectType, dst.Type)
			}
			if len(dst.Properties) != tt.expectPropsLen {
				t.Fatalf("expected %d properties, got %d", tt.expectPropsLen, len(dst.Properties))
			}
			if tt.expectAdditionalType != "" {
				if dst.AdditionalProperties == nil {
					t.Fatalf("expected additionalProperties schema")
				}
				if dst.AdditionalProperties.Type != tt.expectAdditionalType {
					t.Fatalf("expected additionalProperties type %q, got %q", tt.expectAdditionalType, dst.AdditionalProperties.Type)
				}
			}
			if tt.expectAdditionalItemRef {
				if dst.AdditionalProperties == nil {
					t.Fatalf("expected additionalProperties schema")
				}
				if dst.AdditionalProperties.Ref == "" {
					t.Fatalf("expected additionalProperties to reference a schema")
				}
			}
		})
	}
}
