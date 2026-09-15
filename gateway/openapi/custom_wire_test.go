package openapi_test

import (
	"context"
	"encoding/json"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	gateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/gateway/openapi"
	"github.com/viant/datly/gateway/openapi/openapi3"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

type customWireRecord struct {
	ID  uuid.UUID
	Raw json.RawMessage
}
type customWireInput struct {
	Payload customWireRecord `parameter:"Payload,kind=body,required=true"`
}

func TestCustomWireTypesServeWithOpenAPI(t *testing.T) {
	for _, caseFormat := range []string{"", "lc"} {
		t.Run("case="+caseFormat, func(t *testing.T) {
			entry := (fixture{input: reflect.TypeFor[customWireInput](), output: reflect.TypeFor[customWireRecord](), component: &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "CustomWire"}, Routes: []*spec.Route{{Method: "POST", Path: "/custom"}}, Settings: &spec.Settings{CaseFormat: caseFormat}}, execute: func(_ context.Context, inv handler.Invocation) (any, error) {
				return inv.Input.(*customWireInput).Payload, nil
			}}).registration(t)
			doc, err := (openapi.Generator{}).Generate(context.Background(), documentRequest(entry))
			require.NoError(t, err)
			proof := documentAssertion{t: t, document: doc}
			input := proof.deref(doc.Paths["/custom"].Post.RequestBody.Content["application/json"].Schema)
			require.Equal(t, "string", proof.deref(input.Properties["ID"]).Type)
			require.Empty(t, proof.deref(input.Properties["Raw"]).Type)
			output := proof.deref(doc.Paths["/custom"].Post.Responses["200"].Content["application/json"].Schema)
			idKey, rawKey := "ID", "Raw"
			if caseFormat == "lc" {
				idKey, rawKey = "id", "raw"
			}
			require.Equal(t, "string", proof.deref(output.Properties[idKey]).Type)
			require.Empty(t, proof.deref(output.Properties[rawKey]).Type)
			rt, err := druntime.NewRuntime([]*registry.RegisteredComponent{entry})
			require.NoError(t, err)
			server, err := (gateway.Config{OpenAPI: &gateway.OpenAPIConfig{Info: openapi3.Info{Title: "Custom wire", Version: "1"}}}).Build(context.Background(), gateway.HandlerInput{Runtime: rt, Components: []*registry.RegisteredComponent{entry}})
			require.NoError(t, err)
			for _, raw := range []string{`{"nested":true}`, `[1,true]`, `7`, `null`} {
				body := `{"ID":"123e4567-e89b-12d3-a456-426614174000","Raw":` + raw + `}`
				request := httptest.NewRequest("POST", "/custom", strings.NewReader(body))
				request.Header.Set("Content-Type", "application/json")
				recorder := httptest.NewRecorder()
				server.ServeHTTP(recorder, request)
				require.Equal(t, 200, recorder.Code, recorder.Body.String())
				require.JSONEq(t, `{"`+idKey+`":"123e4567-e89b-12d3-a456-426614174000","`+rawKey+`":`+raw+`}`, recorder.Body.String())
			}
			recorder := httptest.NewRecorder()
			server.ServeHTTP(recorder, httptest.NewRequest("GET", gateway.DefaultOpenAPIURI, nil))
			require.Equal(t, 200, recorder.Code, recorder.Body.String())
			require.True(t, json.Valid(recorder.Body.Bytes()))
		})
	}
}

type marshalOnlyWire struct{ Value string }

func (marshalOnlyWire) MarshalJSON() ([]byte, error) { panic("schema must not invoke MarshalJSON") }

type unmarshalOnlyWire struct{ Value string }

func (*unmarshalOnlyWire) UnmarshalJSON([]byte) error { panic("schema must not invoke UnmarshalJSON") }

func TestCustomWireDirectionsAreIndependent(t *testing.T) {
	for _, tc := range []struct {
		name        string
		typ         reflect.Type
		inputObject bool
	}{
		{"marshal only", reflect.TypeFor[marshalOnlyWire](), true},
		{"unmarshal only", reflect.TypeFor[unmarshalOnlyWire](), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := reflect.StructOf([]reflect.StructField{{Name: "Payload", Type: tc.typ, Tag: `parameter:"Payload,kind=body,required=true"`}})
			entry := (fixture{input: input, output: tc.typ}).registration(t)
			doc, err := (openapi.Generator{}).Generate(context.Background(), documentRequest(entry))
			require.NoError(t, err)
			proof := documentAssertion{t: t, document: doc}
			in := proof.deref(doc.Paths["/records"].Post.RequestBody.Content["application/json"].Schema)
			out := proof.deref(doc.Paths["/records"].Post.Responses["200"].Content["application/json"].Schema)
			if tc.inputObject {
				require.Equal(t, "object", in.Type)
				require.Contains(t, in.Properties, "Value")
				require.Empty(t, out.Type)
			} else {
				require.Empty(t, in.Type)
				require.Equal(t, "object", out.Type)
				require.Contains(t, out.Properties, "Value")
			}
		})
	}
}
