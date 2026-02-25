package openapi

import (
	"context"
	"reflect"
	"testing"

	"github.com/viant/datly/gateway/router/openapi/openapi3"
	"github.com/viant/datly/repository"
	"github.com/viant/datly/view"
	"github.com/viant/xreflect"
)

type testAnimal interface {
	Kind() string
}

type testDog struct{}

func (testDog) Kind() string { return "dog" }

type testCat struct{}

func (*testCat) Kind() string { return "cat" }

type testTree struct{}

type testUnsupported chan int

func (testUnsupported) Kind() string { return "unsupported" }

func TestSchemaBuildHelpers_Table(t *testing.T) {
	t.Run("apply schema example", func(t *testing.T) {
		dst := &openapi3.Schema{}
		applySchemaExample(dst, &Schema{tag: Tag{Example: "abc"}})
		if dst.Example != "abc" {
			t.Fatalf("expected example to be applied")
		}
		applySchemaExample(dst, &Schema{})
		if dst.Example != "abc" {
			t.Fatalf("expected empty example not to override existing value")
		}
	})

	t.Run("root table", func(t *testing.T) {
		queryComp := &ComponentSchema{component: &repository.Component{View: &view.View{Mode: view.ModeQuery, Table: "users"}}}
		if got := rootTable(queryComp); got != "users" {
			t.Fatalf("expected users, got %q", got)
		}
		nonQueryComp := &ComponentSchema{component: &repository.Component{View: &view.View{Mode: view.Mode("Other"), Table: "users"}}}
		if got := rootTable(nonQueryComp); got != "" {
			t.Fatalf("expected empty root table for non-query mode")
		}
	})

	t.Run("normalize field tag", func(t *testing.T) {
		tests := []struct {
			name      string
			tag       Tag
			rootTable string
			table     string
			wantTable string
			updated   bool
			column    string
		}{
			{name: "column sets root table", tag: Tag{Column: "ID"}, rootTable: "users", table: "", wantTable: "users", updated: true, column: "ID"},
			{name: "table infers column", tag: Tag{}, rootTable: "", table: "users", wantTable: "users", updated: false, column: "FIRST_NAME"},
			{name: "ignored tag", tag: Tag{Ignore: true}, rootTable: "users", table: "users", wantTable: "users", updated: false, column: ""},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				tag := tt.tag
				updated := normalizeFieldTag(&tag, "FirstName", tt.rootTable, tt.table)
				if updated != tt.updated {
					t.Fatalf("expected updated=%v, got %v", tt.updated, updated)
				}
				if tag.Table != tt.wantTable {
					t.Fatalf("expected table %q, got %q", tt.wantTable, tag.Table)
				}
				if tag.Column != tt.column {
					t.Fatalf("expected column %q, got %q", tt.column, tag.Column)
				}
			})
		}
	})

	t.Run("should skip by tag", func(t *testing.T) {
		tag := `internal:"true"`
		component := &ComponentSchema{component: &repository.Component{View: &view.View{Table: "users", ColumnsConfig: map[string]*view.ColumnConfig{"ID": {Tag: &tag}}}}}
		if !shouldSkipByTag(component, &Tag{Ignore: true}) {
			t.Fatalf("expected ignored tag to be skipped")
		}
		if !shouldSkipByTag(component, &Tag{Table: "users", Column: "ID"}) {
			t.Fatalf("expected internal column to be skipped")
		}
		if shouldSkipByTag(component, &Tag{Ignore: false, Table: "users", Column: "Name"}) {
			t.Fatalf("did not expect non-internal column to be skipped")
		}
	})

	t.Run("should skip struct field", func(t *testing.T) {
		type sample struct {
			exported string
			Visible  string
			Hidden   string `json:"-"`
			Internal string `internal:"true"`
		}
		rType := reflect.TypeOf(sample{})
		if !shouldSkipStructField(rType.Field(0)) {
			t.Fatalf("expected unexported field to be skipped")
		}
		if shouldSkipStructField(rType.Field(1)) {
			t.Fatalf("did not expect visible field to be skipped")
		}
		if !shouldSkipStructField(rType.Field(2)) {
			t.Fatalf("expected json:- field to be skipped")
		}
		if !shouldSkipStructField(rType.Field(3)) {
			t.Fatalf("expected internal:true field to be skipped")
		}
	})

	t.Run("add time schema default and pre-existing example", func(t *testing.T) {
		dst := &openapi3.Schema{}
		addTimeSchema(dst, &Schema{})
		if dst.Type != stringOutput || dst.Format != "date-time" || dst.Pattern == "" {
			t.Fatalf("unexpected default time schema: type=%s format=%s pattern=%s", dst.Type, dst.Format, dst.Pattern)
		}
		existing := &openapi3.Schema{Example: "preset"}
		addTimeSchema(existing, &Schema{})
		if existing.Example != "preset" {
			t.Fatalf("expected existing example to be preserved")
		}
	})

	t.Run("interface oneOf scaffolding", func(t *testing.T) {
		t.Setenv("DATLY_OPENAPI_POLY_STRICT", "false")
		component := newTestComponent(t)
		types := xreflect.NewTypes()
		if err := types.Register("Animal", xreflect.WithPackage("test"), xreflect.WithReflectType(reflect.TypeOf((*testAnimal)(nil)).Elem())); err != nil {
			t.Fatalf("register interface failed: %v", err)
		}
		if err := types.Register("Dog", xreflect.WithPackage("test"), xreflect.WithReflectType(reflect.TypeOf(testDog{}))); err != nil {
			t.Fatalf("register dog failed: %v", err)
		}
		if err := types.Register("Cat", xreflect.WithPackage("test"), xreflect.WithReflectType(reflect.TypeOf(testCat{}))); err != nil {
			t.Fatalf("register cat failed: %v", err)
		}
		if err := types.Register("DogAlias", xreflect.WithPackage("test"), xreflect.WithReflectType(reflect.TypeOf(testDog{}))); err != nil {
			t.Fatalf("register dog alias failed: %v", err)
		}
		if err := types.Register("Tree", xreflect.WithPackage("test"), xreflect.WithReflectType(reflect.TypeOf(testTree{}))); err != nil {
			t.Fatalf("register tree failed: %v", err)
		}
		if err := types.Register("Unsupported", xreflect.WithPackage("test"), xreflect.WithReflectType(reflect.TypeOf(testUnsupported(nil)))); err != nil {
			t.Fatalf("register unsupported failed: %v", err)
		}
		setUnexportedField(component, "types", types)

		container := NewContainer()
		componentSchema := &ComponentSchema{component: component, schemas: container}
		dst := &openapi3.Schema{}
		err := container.addToSchema(context.Background(), componentSchema, dst, &Schema{
			rType:    reflect.TypeOf((*testAnimal)(nil)).Elem(),
			ioConfig: component.IOConfig(),
		})
		if err != nil {
			t.Fatalf("unexpected addToSchema error: %v", err)
		}
		if dst.Type != objectOutput {
			t.Fatalf("expected object type for interface, got %q", dst.Type)
		}
		if len(dst.OneOf) != 2 {
			t.Fatalf("expected oneOf variants for interface, got %d", len(dst.OneOf))
		}
		if dst.Discriminator == nil {
			t.Fatalf("expected discriminator to be set for oneOf interface schema")
		}
		if dst.Discriminator.PropertyName != "type" {
			t.Fatalf("expected discriminator propertyName type, got %q", dst.Discriminator.PropertyName)
		}
		if len(dst.Discriminator.Mapping) != 2 {
			t.Fatalf("expected discriminator mapping entries, got %d", len(dst.Discriminator.Mapping))
		}
		if dst.Discriminator.Mapping["Dog"] != "#/components/schemas/Dog" {
			t.Fatalf("unexpected discriminator mapping for Dog: %q", dst.Discriminator.Mapping["Dog"])
		}
		if dst.Discriminator.Mapping["Cat"] != "#/components/schemas/Cat" {
			t.Fatalf("unexpected discriminator mapping for Cat: %q", dst.Discriminator.Mapping["Cat"])
		}

		dogSchema := container.generatedSchemas["Dog"]
		if dogSchema == nil || dogSchema.Properties["type"] == nil {
			t.Fatalf("expected discriminator property injected in Dog schema")
		}
		if !containsString(dogSchema.Required, "type") {
			t.Fatalf("expected discriminator property required in Dog schema")
		}

		if dst.Extension == nil {
			t.Fatalf("expected best-effort extension metadata")
		}
		skipped, ok := dst.Extension["x-datly-polymorphism-skipped"].([]string)
		if !ok || len(skipped) == 0 {
			t.Fatalf("expected skipped implementors extension")
		}
	})

	t.Run("interface oneOf fallback without registry", func(t *testing.T) {
		component := &repository.Component{}
		container := NewContainer()
		componentSchema := &ComponentSchema{component: component, schemas: container}
		dst := &openapi3.Schema{}
		err := container.addToSchema(context.Background(), componentSchema, dst, &Schema{
			rType:    reflect.TypeOf((*testAnimal)(nil)).Elem(),
			ioConfig: component.IOConfig(),
		})
		if err != nil {
			t.Fatalf("unexpected addToSchema error: %v", err)
		}
		if dst.Type != objectOutput {
			t.Fatalf("expected object type for interface fallback, got %q", dst.Type)
		}
		if len(dst.OneOf) != 0 {
			t.Fatalf("expected no oneOf variants when registry is unavailable, got %d", len(dst.OneOf))
		}
		if dst.Discriminator != nil {
			t.Fatalf("expected no discriminator without variants")
		}
	})

	t.Run("implements interface", func(t *testing.T) {
		tests := []struct {
			name      string
			candidate reflect.Type
			want      bool
		}{
			{name: "value receiver", candidate: reflect.TypeOf(testDog{}), want: true},
			{name: "pointer receiver", candidate: reflect.TypeOf(testCat{}), want: true},
			{name: "not implementor", candidate: reflect.TypeOf(struct{}{}), want: false},
		}
		iface := reflect.TypeOf((*testAnimal)(nil)).Elem()
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if got := implementsInterface(tt.candidate, iface); got != tt.want {
					t.Fatalf("expected %v, got %v", tt.want, got)
				}
			})
		}
	})

	t.Run("interface variants nil component", func(t *testing.T) {
		container := NewContainer()
		variants, skipped, err := container.interfaceVariants(context.Background(), nil, &Schema{}, reflect.TypeOf((*testAnimal)(nil)).Elem())
		if err != nil {
			t.Fatalf("unexpected interfaceVariants error: %v", err)
		}
		if len(variants) != 0 {
			t.Fatalf("expected no variants for nil component, got %d", len(variants))
		}
		if len(skipped) != 0 {
			t.Fatalf("expected no skipped variants for nil component")
		}
	})

	t.Run("interface oneOf strict mode", func(t *testing.T) {
		t.Setenv("DATLY_OPENAPI_POLY_STRICT", "true")
		component := newTestComponent(t)
		types := xreflect.NewTypes()
		if err := types.Register("Animal", xreflect.WithPackage("test"), xreflect.WithReflectType(reflect.TypeOf((*testAnimal)(nil)).Elem())); err != nil {
			t.Fatalf("register interface failed: %v", err)
		}
		if err := types.Register("Dog", xreflect.WithPackage("test"), xreflect.WithReflectType(reflect.TypeOf(testDog{}))); err != nil {
			t.Fatalf("register dog failed: %v", err)
		}
		if err := types.Register("Unsupported", xreflect.WithPackage("test"), xreflect.WithReflectType(reflect.TypeOf(testUnsupported(nil)))); err != nil {
			t.Fatalf("register unsupported failed: %v", err)
		}
		setUnexportedField(component, "types", types)

		container := NewContainer()
		componentSchema := &ComponentSchema{component: component, schemas: container}
		dst := &openapi3.Schema{}
		err := container.addToSchema(context.Background(), componentSchema, dst, &Schema{
			rType:    reflect.TypeOf((*testAnimal)(nil)).Elem(),
			ioConfig: component.IOConfig(),
		})
		if err == nil {
			t.Fatalf("expected strict mode polymorphism error")
		}
	})

	t.Run("oneOf discriminator and helper branches", func(t *testing.T) {
		t.Run("empty refs yield nil discriminator", func(t *testing.T) {
			discriminator := oneOfDiscriminator(openapi3.SchemaList{{Type: objectOutput}, nil})
			if discriminator != nil {
				t.Fatalf("expected nil discriminator when refs are absent")
			}
		})

		t.Run("apply discriminator skips non-object and missing schema", func(t *testing.T) {
			container := NewContainer()
			container.generatedSchemas["User"] = &openapi3.Schema{Type: objectOutput}
			container.generatedSchemas["Arr"] = &openapi3.Schema{Type: arrayOutput}
			container.applyDiscriminatorToVariants(&openapi3.Discriminator{
				PropertyName: "kind",
				Mapping: map[string]string{
					"user": "#/components/schemas/User",
					"arr":  "#/components/schemas/Arr",
					"miss": "#/components/schemas/Missing",
				},
			})
			user := container.generatedSchemas["User"]
			if user == nil || user.Properties["kind"] == nil {
				t.Fatalf("expected discriminator property on object variant")
			}
			if !containsString(user.Required, "kind") {
				t.Fatalf("expected discriminator property required on object variant")
			}
			arr := container.generatedSchemas["Arr"]
			if arr != nil && arr.Properties != nil {
				if _, ok := arr.Properties["kind"]; ok {
					t.Fatalf("did not expect discriminator property on non-object variant")
				}
			}
		})

		t.Run("refName invalid variants", func(t *testing.T) {
			if got := refName(""); got != "" {
				t.Fatalf("expected empty ref name for empty ref")
			}
			if got := refName("abc"); got != "" {
				t.Fatalf("expected empty ref name for malformed ref")
			}
			if got := refName("abc/"); got != "" {
				t.Fatalf("expected empty ref name for trailing slash")
			}
		})
	})
}
