package generate

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	dtag "github.com/viant/datly/tag"
)

func TestWriterScalarJSONPresentationPolicy(t *testing.T) {
	for _, tc := range []struct {
		name, mutation, format, raw, want string
		omit                              bool
	}{
		{name: "default unchanged", mutation: "patch", raw: `sqlx:"user_id"`, want: ""},
		{name: "reader casing", format: "lc", raw: `sqlx:"user_id"`, want: "userId"},
		{name: "existing casing", mutation: "patch", format: "lc", raw: `sqlx:"user_id"`, want: "userId"},
		{name: "omit opt in", mutation: "patch", format: "lc", omit: true, raw: `sqlx:"user_id"`, want: "userId,omitempty"},
		{name: "omit without casing", mutation: "patch", omit: true, raw: `sqlx:"user_id"`, want: ",omitempty"},
		{name: "authored omission", mutation: "patch", format: "lc", raw: `json:",omitempty" sqlx:"user_id"`, want: "userId,omitempty"},
		{name: "explicit rename", mutation: "patch", format: "lc", omit: true, raw: `json:"owner" sqlx:"user_id"`, want: "owner"},
		{name: "explicit omission override", mutation: "patch", format: "lc", omit: true, raw: `json:"," sqlx:"user_id"`, want: "userId"},
		{name: "ignored", mutation: "patch", format: "lc", omit: true, raw: `json:"-" sqlx:"user_id"`, want: "-"},
		{name: "internal unchanged", mutation: "patch", format: "lc", omit: true, raw: `internal:"true" sqlx:"user_id"`, want: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := &Plan{Generation: &spec.GenerationSettings{WriterOmitEmpty: tc.omit}, Settings: dtag.Settings{Mutation: tc.mutation, CaseFormat: tc.format}}
			tag := reflect.StructTag(writerScalarJSONTag(p, "UserId", tc.raw))
			require.Equal(t, tc.want, tag.Get("json"))
			require.Equal(t, reflect.StructTag(tc.raw).Get("sqlx"), tag.Get("sqlx"))
			require.Equal(t, reflect.StructTag(tc.raw).Get("internal"), tag.Get("internal"))
		})
	}
}

func TestWriterGeneratedJSONNamesOmissionAndPresence(t *testing.T) {
	plan := testPlan(t, &spec.Component{Name: "Records", Settings: &spec.Settings{Mutation: "patch", CaseFormat: "lc", Generation: &spec.GenerationSettings{WriterOmitEmpty: true}}, RootView: &spec.View{Name: "Records", Columns: []*spec.Column{
		{Name: "user_id", Type: spec.TypeRef{Name: "string"}},
		{Name: "title", Type: spec.TypeRef{Name: "string"}, Tag: `json:"Title"`},
		{Name: "secret", Type: spec.TypeRef{Name: "string"}, Tag: `json:"-" internal:"true"`},
	}}})
	var fields []reflect.StructField
	for _, f := range plan.Views[0].Fields {
		if f.Name == "Has" {
			require.Contains(t, f.Tag, `json:"-"`)
			continue
		}
		fields = append(fields, reflect.StructField{Name: f.Name, Type: reflect.TypeFor[string](), Tag: reflect.StructTag(f.Tag)})
	}
	row := reflect.New(reflect.StructOf(fields)).Elem()
	body, err := json.Marshal(row.Interface())
	require.NoError(t, err)
	require.JSONEq(t, `{"Title":""}`, string(body))
	row.FieldByName("UserId").SetString("u1")
	row.FieldByName("Secret").SetString("private")
	body, err = json.Marshal(row.Interface())
	require.NoError(t, err)
	require.JSONEq(t, `{"userId":"u1","Title":""}`, string(body))
}

func TestReaderGeneratedJSONNamesKeepMapKeysAndOmission(t *testing.T) {
	p := &Plan{Generation: &spec.GenerationSettings{WriterOmitEmpty: true}, Settings: dtag.Settings{CaseFormat: "lc"}}
	typ := reflect.StructOf([]reflect.StructField{{Name: "ConversationId", Type: reflect.TypeFor[string](), Tag: reflect.StructTag(writerScalarJSONTag(p, "ConversationId", `sqlx:"conversation_id"`))}, {Name: "Metadata", Type: reflect.TypeFor[map[string]any](), Tag: reflect.StructTag(writerScalarJSONTag(p, "Metadata", `sqlx:"metadata"`))}})
	row := reflect.New(typ).Elem()
	row.FieldByName("Metadata").Set(reflect.ValueOf(map[string]any{"MixedCase": true, "nested": map[string]any{"APIKey": 2}}))
	body, err := json.Marshal(row.Interface())
	require.NoError(t, err)
	require.JSONEq(t, `{"conversationId":"","metadata":{"MixedCase":true,"nested":{"APIKey":2}}}`, string(body))
}
