package output

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	"github.com/xeipuuv/gojsonschema"
)

type SchemaEmbedded struct {
	RecordCount int
}

type schemaOpaque struct{ Hidden int }

func (schemaOpaque) MarshalJSON() ([]byte, error) { return []byte(`"custom"`), nil }

type schemaRecursive struct {
	DisplayName string
	Next        *schemaRecursive
}

func TestJSONSchemaMatchesEncoder(t *testing.T) {
	type row struct {
		ID      int
		Null    *int
		Zero    int
		False   bool
		Empty   string
		Omitted string `json:",omitempty"`
		Tagged  int    `json:"Exact_Name"`
	}
	type envelope struct {
		*SchemaEmbedded
		Rows    []row
		ByName  map[string]row
		Bytes   []byte
		Dynamic any
		Opaque  schemaOpaque
		Next    *schemaRecursive
		Date    time.Time
		Quoted  int `json:",string"`
		Hidden  int `json:"-"`
	}
	for _, casing := range []string{"", "lc"} {
		t.Run("case="+casing, func(t *testing.T) {
			settings := &spec.Settings{CaseFormat: casing}
			plan, err := (Compiler{}).Compile(CompileInput{Type: reflect.TypeFor[envelope](), Component: &spec.Component{Settings: settings}})
			require.NoError(t, err)
			document, err := plan.JSONSchema()
			require.NoError(t, err)
			properties := document["properties"].(map[string]any)
			rowName, idName, recordName := "Rows", "ID", "RecordCount"
			if casing != "" {
				rowName, idName, recordName = "rows", "id", "recordCount"
			}
			require.Contains(t, properties, recordName)
			require.NotContains(t, document["required"], recordName, "nil embedded pointer may omit promoted fields")
			rowSchema := properties[rowName].(map[string]any)["items"].(map[string]any)
			require.Contains(t, rowSchema["properties"], idName)
			require.Contains(t, rowSchema["properties"], "Exact_Name")
			for _, value := range []*envelope{
				{},
				{SchemaEmbedded: &SchemaEmbedded{}, Rows: []row{{}}, ByName: map[string]row{"Original_Key": {}},
					Bytes: []byte{1, 2}, Dynamic: map[string]any{"arbitrary": false}, Quoted: 3,
					Next: &schemaRecursive{Next: &schemaRecursive{DisplayName: "child"}}},
			} {
				encoded, err := plan.Encode(context.Background(), "json", value)
				require.NoError(t, err)
				assertJSONSchemaValid(t, document, encoded.Data)
			}
			// Discovery depends on the compiled plan, not subsequently changed settings.
			settings.CaseFormat = "uc"
			again, err := plan.JSONSchema()
			require.NoError(t, err)
			require.Equal(t, document, again)
		})
	}
}

func TestJSONSchemaUsesFieldFormatsAndCanonicalExclusions(t *testing.T) {
	type child struct {
		DisplayName string
		Secret      string
	}
	type value struct {
		Left      child
		Right     child
		Explicit  string    `json:"EXPLICIT" format:"name=Ignored"`
		Formatted string    `format:"name=DifferentName"`
		LocalCase string    `format:"name=DisplayName,caseFormat=lu"`
		Date      time.Time `format:"dateFormat=YYYY-MM-DD"`
		Internal  string    `internal:"true"`
		Ignored   string    `format:"ignore"`
	}
	plan, err := (Compiler{}).Compile(CompileInput{Type: reflect.TypeFor[value](), Component: &spec.Component{Settings: &spec.Settings{
		CaseFormat: "lc", Output: &spec.OutputSettings{Exclude: []string{"Left.Secret"}},
	}}})
	require.NoError(t, err)
	document, err := plan.JSONSchema()
	require.NoError(t, err)
	properties := document["properties"].(map[string]any)
	require.NotContains(t, properties["left"].(map[string]any)["properties"], "secret")
	require.Contains(t, properties["right"].(map[string]any)["properties"], "secret")
	require.Contains(t, properties, "EXPLICIT")
	require.Contains(t, properties, "differentName")
	require.Contains(t, properties, "display_name")
	require.NotContains(t, properties, "internal")
	require.NotContains(t, properties, "ignored")
	require.Equal(t, "date", properties["date"].(map[string]any)["format"])
	encoded, err := plan.Encode(context.Background(), "json", &value{Date: time.Date(2026, 9, 23, 0, 0, 0, 0, time.UTC)})
	require.NoError(t, err)
	assertJSONSchemaValid(t, document, encoded.Data)
}

func TestJSONSchemaDeclinesAmbiguousNames(t *testing.T) {
	type value struct {
		SampleSeen_1Day int
		SampleSeen_7Day int
	}
	plan, err := (Compiler{}).Compile(CompileInput{Type: reflect.TypeFor[value](), Component: &spec.Component{Settings: &spec.Settings{CaseFormat: "lc"}}})
	require.NoError(t, err)
	_, err = plan.JSONSchema()
	require.ErrorIs(t, err, ErrSchemaUnavailable)
}

func assertJSONSchemaValid(t *testing.T, document map[string]any, data []byte) {
	t.Helper()
	schema, err := json.Marshal(document)
	require.NoError(t, err)
	validation, err := gojsonschema.Validate(gojsonschema.NewBytesLoader(schema), gojsonschema.NewBytesLoader(data))
	require.NoError(t, err)
	require.True(t, validation.Valid(), "data=%s errors=%v", data, validation.Errors())
}
