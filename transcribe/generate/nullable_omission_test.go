package generate

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	dtag "github.com/viant/datly/tag"
)

func TestWriterSQLNullableOmissionDefault(t *testing.T) {
	for _, nullable := range []bool{false, true} {
		for _, shape := range []string{"default", "required", "optional", "cast-value", "cast-pointer"} {
			for _, mutation := range []string{"", "patch", "post", "put"} {
				t.Run(strings.Join([]string{mutation, shape, map[bool]string{false: "not-null", true: "nullable"}[nullable]}, "/"), func(t *testing.T) {
					column := &spec.Column{Name: "value", Source: "VALUE", Type: spec.TypeRef{Name: "string"}, Nullable: nullable, NotNull: !nullable}
					switch shape {
					case "required":
						column.Required = true
					case "optional":
						column.Optional = true
					case "cast-value":
						column.ExplicitType = true
					case "cast-pointer":
						column.ExplicitType = true
						column.Type.Pointer = true
					}
					plan := &Plan{Settings: dtag.Settings{Mutation: mutation, CaseFormat: "lc"}}
					fields, err := resolveScalarViewFields(plan, &spec.View{Name: "Records", Columns: []*spec.Column{column}}, false)
					require.NoError(t, err)
					require.NoError(t, applyInferredJSONTags(plan, fields))
					wantOmit := nullable && mutation != ""
					require.Equal(t, wantOmit, strings.Contains(reflect.StructTag(fields[0].Tag).Get("json"), "omitempty"))
					require.Equal(t, column.EffectiveType().Pointer, strings.HasPrefix(fields[0].Type, "*"))
					require.Equal(t, !nullable, strings.Contains(reflect.StructTag(fields[0].Tag).Get("sqlx"), "required=true"))
					fieldType := reflect.TypeFor[string]()
					if strings.HasPrefix(fields[0].Type, "*") {
						fieldType = reflect.PointerTo(fieldType)
					}
					row := reflect.New(reflect.StructOf([]reflect.StructField{{Name: "Value", Type: fieldType, Tag: reflect.StructTag(fields[0].Tag)}})).Elem()
					encoded, err := json.Marshal(row.Interface())
					require.NoError(t, err)
					if wantOmit {
						require.JSONEq(t, `{}`, string(encoded))
					} else if fieldType.Kind() == reflect.Pointer {
						require.JSONEq(t, `{"value":null}`, string(encoded))
					} else {
						require.JSONEq(t, `{"value":""}`, string(encoded))
					}
					if fieldType.Kind() == reflect.Pointer {
						row.Field(0).Set(reflect.New(reflect.TypeFor[string]()))
						encoded, err = json.Marshal(row.Interface())
						require.NoError(t, err)
						require.JSONEq(t, `{"value":""}`, string(encoded), "present zero pointer must survive omission")
					}
				})
			}
		}
	}
}

func TestWriterNullableOmissionHonorsAuthoredPolicyAndCollisions(t *testing.T) {
	plan := &Plan{Settings: dtag.Settings{Mutation: "patch", CaseFormat: "lc"}}
	for _, raw := range []string{`json:"named"`, `json:","`, `json:"-"`, `json:",string"`, `internal:"true"`, `sqlx:"-" diff:"-" internal:"true" json:"-"`} {
		fields, err := resolveScalarViewFields(plan, &spec.View{Name: "Records", Columns: []*spec.Column{{Name: "value", Type: spec.TypeRef{Name: "string"}, Nullable: true, Tag: raw}}}, false)
		require.NoError(t, err)
		require.NotContains(t, fields[0].Tag, "omitempty", raw)
	}
	fields, err := resolveScalarViewFields(plan, &spec.View{Name: "Records", Columns: []*spec.Column{
		{Name: "API", Type: spec.TypeRef{Name: "string"}, Nullable: true},
		{Name: "Api", Type: spec.TypeRef{Name: "string"}, Nullable: true},
	}}, false)
	require.NoError(t, err)
	// Exercise the existing complete-struct collision fixture after SQL
	// nullability has supplied each scalar's original tag snapshot.
	fields[0].Name, fields[1].Name = "SampleSeenDay", "SampleSeen_Day"
	require.NoError(t, applyInferredJSONTags(plan, fields))
	for _, field := range fields {
		require.Equal(t, ",omitempty", reflect.StructTag(field.Tag).Get("json"), "collision fallback retains inferred omission")
	}
}

func TestGeneratedFactoryBodyNullableOmissionScope(t *testing.T) {
	nullable := func(name string) *spec.Column {
		return &spec.Column{Name: name, Source: strings.ToUpper(name), Type: spec.TypeRef{Name: "string", Pointer: true}, Nullable: true}
	}
	body := &spec.View{Name: "Records", Key: spec.Key{Scope: "body"}, TypeName: "Body", Columns: []*spec.Column{nullable("label"), {Name: "id", Type: spec.TypeRef{Name: "int"}, NotNull: true}, {Name: "explicit", Type: spec.TypeRef{Name: "string", Pointer: true}, Nullable: true, Tag: `json:"explicit"`}}}
	nested := &spec.View{Name: "Children", TypeName: "Child", Columns: []*spec.Column{nullable("label")}}
	body.Relations = []*spec.Relation{{Name: "children", Holder: "Children", View: nested, Cardinality: spec.CardinalityMany}}
	current := &spec.View{Name: "Records", Key: spec.Key{Scope: "current"}, TypeName: "Current", Columns: []*spec.Column{nullable("label")}}
	handler := &ExternalHandler{GeneratedContracts: true, InputShape: body}
	plan := &Plan{RootViewType: "Body", ViewDest: "views.go", Settings: dtag.Settings{CaseFormat: "lc"}}
	resolver := &planResolver{plan: plan, input: Input{Component: &spec.Component{Name: "Factory", Views: []*spec.View{current}}, ExternalHandler: handler}}
	_, err := resolver.resolveViews()
	require.NoError(t, err)
	require.Empty(t, plan.Settings.Mutation)
	require.Nil(t, plan.MutationHandler)
	byType := map[string]ViewPlan{}
	for _, view := range plan.Views {
		byType[view.Type] = view
	}
	require.Contains(t, byType, "Body")
	require.Contains(t, byType, "Current")
	require.Contains(t, byType, "Child")
	for _, name := range []string{"Body", "Child"} {
		require.Contains(t, byType[name].Fields[0].Tag, "omitempty")
	}
	require.NotContains(t, byType["Current"].Fields[0].Tag, "omitempty")
	require.NotContains(t, byType["Body"].Fields[1].Tag, "omitempty")
	require.Equal(t, "explicit", reflect.StructTag(byType["Body"].Fields[2].Tag).Get("json"))
	for _, h := range []*ExternalHandler{nil, {InputShape: body}, {GeneratedContracts: true}} {
		require.Empty(t, generatedFactoryBodyViews(h))
	}
	// Cycles and nil relation entries must not widen the graph or recurse forever.
	nested.Relations = []*spec.Relation{nil, {View: body}}
	require.Len(t, generatedFactoryBodyViews(handler), 2)

	for _, raw := range []string{`json:","`, `json:"-"`, `json:",string"`, `internal:"true"`, `sqlx:"-" internal:"true" json:"-"`} {
		view := &spec.View{Name: "Override", Columns: []*spec.Column{{Name: "value", Type: spec.TypeRef{Name: "string", Pointer: true}, Nullable: true, Tag: raw}}}
		fields, err := resolveScalarViewFieldsWithOmission(plan, view, false, true)
		require.NoError(t, err)
		require.NotContains(t, fields[0].Tag, "omitempty", raw)
	}
	field := byType["Body"].Fields[0]
	typ := reflect.StructOf([]reflect.StructField{{Name: field.Name, Type: reflect.TypeFor[*string](), Tag: reflect.StructTag(field.Tag)}, {Name: "Id", Type: reflect.TypeFor[int](), Tag: reflect.StructTag(byType["Body"].Fields[1].Tag)}})
	row := reflect.New(typ).Elem()
	raw, err := json.Marshal(row.Interface())
	require.NoError(t, err)
	require.JSONEq(t, `{"id":0}`, string(raw))
	empty := ""
	row.Field(0).Set(reflect.ValueOf(&empty))
	raw, err = json.Marshal(row.Interface())
	require.NoError(t, err)
	require.JSONEq(t, `{"label":"","id":0}`, string(raw))
}
