package openapi_test

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	gatewayhttp "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/gateway/openapi"
	"github.com/viant/datly/gateway/openapi/openapi3"
	"github.com/viant/datly/internal/testharness/sqlite"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
)

type ProjectedHeader struct {
	PublicName string
	Secret     string
}
type projectedRecord struct {
	Label  string `format:"name=displayLabel"`
	Secret string
	Note   *string
	Count  int `format:"nullable=true"`
	Bytes  []byte
	At     time.Time `format:"dateFormat=yyyy-MM-dd"`
}
type projectedEnvelope struct {
	*ProjectedHeader
	Rows     []*projectedRecord `json:"data"`
	Detail   *projectedRecord   `format:"name=payload"`
	Name     string             `json:"exactName" format:"name=ignored"`
	Zero     *int
	Internal string               `internal:"true"`
	Has      *struct{ Rows bool } `setMarker:"true"`
}

// acceptanceSchema validates the emitted subset against actual decoded wire
// values, including reference/null branches and required nested properties.
type acceptanceSchema struct {
	t   *testing.T
	doc *openapi3.OpenAPI
}

func (a acceptanceSchema) admits(s *openapi3.Schema, value any) bool {
	if s.Ref != "" {
		return a.admits(a.doc.Components.Schemas[s.Ref[len("#/components/schemas/"):]], value)
	}
	if len(s.AnyOf) > 0 {
		for _, candidate := range s.AnyOf {
			if a.admits(candidate, value) {
				return true
			}
		}
		return false
	}
	if value == nil {
		return s.Nullable
	}
	if len(s.Enum) > 0 {
		for _, candidate := range s.Enum {
			if reflect.DeepEqual(candidate, value) {
				return true
			}
		}
		return false
	}
	switch s.Type {
	case "object":
		object, ok := value.(map[string]any)
		if !ok {
			return false
		}
		for _, name := range s.Required {
			if _, ok := object[name]; !ok {
				return false
			}
		}
		for name, child := range object {
			if property := s.Properties[name]; property != nil {
				if !a.admits(property, child) {
					return false
				}
			} else if s.AdditionalProperties != nil {
				if !a.admits(s.AdditionalProperties, child) {
					return false
				}
			} else {
				return false
			}
		}
	case "array":
		array, ok := value.([]any)
		if !ok {
			return false
		}
		if uint64(len(array)) < s.MinItems || s.MaxItems != nil && uint64(len(array)) > *s.MaxItems {
			return false
		}
		for _, child := range array {
			if !a.admits(s.Items, child) {
				return false
			}
		}
	case "string":
		_, ok := value.(string)
		return ok
	case "integer", "number":
		_, ok := value.(float64)
		return ok
	case "boolean":
		_, ok := value.(bool)
		return ok
	}
	return true
}
func (a acceptanceSchema) object(s *openapi3.Schema) *openapi3.Schema {
	if len(s.AnyOf) > 0 {
		s = s.AnyOf[0]
	}
	if s.Ref != "" {
		s = a.doc.Components.Schemas[s.Ref[len("#/components/schemas/"):]]
	}
	return s
}

func TestProjectedJSONPlanAndHTTP(t *testing.T) {
	zero := 0
	note := "note"
	for _, omit := range []bool{false, true} {
		t.Run(map[bool]string{false: "retain zero and null", true: "omit empty"}[omit], func(t *testing.T) {
			for _, populated := range []bool{false, true} {
				value := &projectedEnvelope{}
				if populated {
					value = &projectedEnvelope{ProjectedHeader: &ProjectedHeader{"visible", "private"}, Rows: []*projectedRecord{{Label: "row", Secret: "private", Note: &note, Bytes: []byte{1, 2}, At: time.Date(2026, 9, 14, 0, 0, 0, 0, time.UTC)}, nil}, Detail: &projectedRecord{Label: "detail", Secret: "private"}, Name: "exact", Zero: &zero, Internal: "private", Has: &struct{ Rows bool }{true}}
				}
				entry := (fixture{output: reflect.TypeFor[projectedEnvelope](), component: &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "Projected"}, Routes: []*spec.Route{{Method: "GET", Path: "/projected"}}, Settings: &spec.Settings{CaseFormat: "lc", Output: &spec.OutputSettings{Exclude: []string{"ProjectedHeader.Secret", "Rows.Secret", "Detail.Secret"}, OmitEmpty: omit}}}, execute: func(context.Context, handler.Invocation) (any, error) { return value, nil }}).registration(t)
				// The compiled plan remains authoritative after mutable source settings change.
				entry.Component.Settings = nil
				doc, err := (openapi.Generator{}).Generate(context.Background(), documentRequest(entry))
				require.NoError(t, err)
				schema := doc.Paths["/projected"].Get.Responses["200"].Content["application/json"].Schema
				proof := acceptanceSchema{t, doc}
				root := proof.object(schema)
				require.NotContains(t, root.Properties, "Has")
				require.NotContains(t, root.Properties, "internal")
				require.NotContains(t, root.Properties, "secret")
				require.Contains(t, root.Properties, "exactName")
				require.Contains(t, root.Properties, "payload")
				require.NotContains(t, root.Required, "publicName")
				child := proof.object(root.Properties["data"].Items)
				require.NotContains(t, child.Properties, "secret")
				require.Equal(t, "date", child.Properties["at"].Format)
				require.Equal(t, "array", child.Properties["bytes"].Type)
				if omit {
					require.Empty(t, root.Required)
				} else {
					require.Contains(t, root.Required, "payload")
				}
				encoded, err := entry.Output.Encode(context.Background(), "json", value)
				require.NoError(t, err)
				rt, err := druntime.NewRuntime([]*registry.RegisteredComponent{entry})
				require.NoError(t, err)
				recorder := httptest.NewRecorder()
				gatewayhttp.NewHandler(rt, nil, "test").ServeHTTP(recorder, httptest.NewRequest("GET", "/projected", nil))
				require.Equal(t, 200, recorder.Code, recorder.Body.String())
				require.JSONEq(t, string(encoded.Data), recorder.Body.String())
				require.NotContains(t, recorder.Body.String(), "private")
				var wire any
				require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &wire))
				require.True(t, proof.admits(schema, wire), recorder.Body.String())
				if !omit && populated {
					require.Contains(t, recorder.Body.String(), `"count":null`)
				}
				if omit && populated {
					require.NotContains(t, recorder.Body.String(), `"zero"`)
				}
				// Published native metadata is immutable and safe for concurrent encoding/docs.
				var wg sync.WaitGroup
				for i := 0; i < 4; i++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						_, err := entry.Output.Encode(context.Background(), "json", value)
						require.NoError(t, err)
						_, err = (openapi.Generator{}).Generate(context.Background(), documentRequest(entry))
						require.NoError(t, err)
					}()
				}
				wg.Wait()
			}
		})
	}
}

func TestProjectedReaderJSONAndAttachmentSQLite(t *testing.T) {
	type row struct {
		ID          int     `sqlx:"id"`
		DisplayName string  `sqlx:"name"`
		Secret      string  `sqlx:"secret"`
		Note        *string `sqlx:"note"`
	}
	type result struct {
		Rows []row `json:"data"`
	}
	h := sqlite.New(t)
	ctx := context.Background()
	require.NoError(t, h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER,name TEXT,secret TEXT,note TEXT)", "INSERT INTO records VALUES(7,'Ada','private',NULL)"))
	for _, format := range []string{"json", "csv", "xlsx"} {
		t.Run(format, func(t *testing.T) {
			component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "Records"}, Routes: []*spec.Route{{Method: "GET", Path: "/records", Marshaller: format}}, Settings: &spec.Settings{CaseFormat: "lc", Output: &spec.OutputSettings{Exclude: []string{"Secret"}, Title: "Records export"}}, RootView: &spec.View{Name: "Records", Source: &spec.ViewSource{SQL: "SELECT id,name,secret,note FROM records"}}, Parameters: []*spec.Parameter{{Name: "Rows", Source: spec.BindSource{Kind: "output", Name: "view"}}}}
			artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: reflect.TypeFor[result]()})
			require.NoError(t, err)
			reader, err := artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: &dsql.SQLComponent{DB: h.DB}})
			require.NoError(t, err)
			entry, err := artifact.Registration(registry.RegisteredComponent{Reader: reader})
			require.NoError(t, err)
			doc, err := (openapi.Generator{}).Generate(ctx, documentRequest(entry))
			require.NoError(t, err)
			rt, err := druntime.NewRuntime([]*registry.RegisteredComponent{entry})
			require.NoError(t, err)
			response := httptest.NewRecorder()
			gatewayhttp.NewHandler(rt, nil, "test").ServeHTTP(response, httptest.NewRequest("GET", "/records", nil))
			require.Equal(t, 200, response.Code, response.Body.String())
			contract := doc.Paths["/records"].Get.Responses["200"]
			require.Contains(t, contract.Content, response.Header().Get("Content-Type"))
			if format == "json" {
				var value any
				require.NoError(t, json.Unmarshal(response.Body.Bytes(), &value))
				proof := acceptanceSchema{t, doc}
				require.True(t, proof.admits(contract.Content["application/json"].Schema, value))
				require.JSONEq(t, `{"data":[{"id":7,"displayName":"Ada","note":null}]}`, response.Body.String())
				require.Empty(t, contract.Headers)
			} else {
				require.Equal(t, response.Header().Get("Content-Disposition"), contract.Headers["Content-Disposition"].Schema.Enum[0])
			}
			// Existing request override changes the real media, not the compiled default doc.
			override := httptest.NewRecorder()
			gatewayhttp.NewHandler(rt, nil, "test").ServeHTTP(override, httptest.NewRequest("GET", "/records?_format=json", nil))
			require.Equal(t, "application/json", override.Header().Get("Content-Type"))
			require.Empty(t, override.Header().Get("Content-Disposition"))
		})
	}
}

func TestProjectedJSONCollisionAndOpaqueEncoderFail(t *testing.T) {
	type collision struct {
		FooBar  int
		Foo_Bar int
	}
	for _, tc := range []struct {
		name   string
		output reflect.Type
		want   string
	}{
		{"case collision", reflect.TypeFor[collision](), "collision"},
		{"custom Go encoder", reflect.TypeFor[marshalOutput](), "opaque JSON encoder"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			entry := (fixture{output: tc.output, component: &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "Opaque"}, Routes: []*spec.Route{{Method: "GET", Path: "/opaque"}}, Settings: &spec.Settings{CaseFormat: "lc"}}}).registration(t)
			doc, err := (openapi.Generator{}).Generate(context.Background(), documentRequest(entry))
			require.ErrorContains(t, err, tc.want)
			require.Nil(t, doc)
		})
	}
}
