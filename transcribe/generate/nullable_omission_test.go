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
