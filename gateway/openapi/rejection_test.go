package openapi_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/gateway/openapi"
	"github.com/viant/datly/spec"
)

type embeddedOutput struct{ child }
type marshalOutput struct{}

func (marshalOutput) MarshalJSON() ([]byte, error) { return []byte(`"custom"`), nil }

func TestUnrepresentableContractsFailClosed(t *testing.T) {
	for _, tc := range []struct {
		name          string
		input, output reflect.Type
		settings      *spec.Settings
		want          string
	}{
		{name: "unsupported JSON value", output: reflect.TypeFor[struct{ C chan int }](), want: "cannot be represented"},
		{name: "visible output marker", output: reflect.TypeFor[struct {
			Has *struct{ A bool } `setMarker:"true"`
		}](), want: "must be hidden"},
		{name: "Authorization text", input: reflect.TypeFor[struct {
			Token string `parameter:"Token,kind=header,in=Authorization"`
		}](), want: "without runtime policy"},
		{name: "invalid transport header", input: reflect.TypeFor[struct {
			Value string `parameter:"Value,kind=header,in=Bad Header"`
		}](), want: "invalid HTTP header"},
		{name: "query object", input: reflect.TypeFor[struct {
			Item child `parameter:"Item,kind=query,in=item"`
		}](), want: "object serialization"},
		{name: "header arrays", input: reflect.TypeFor[struct {
			IDs []int `parameter:"IDs,kind=header,in=ids"`
		}](), want: "no matching OpenAPI serialization"},
		{name: "duplicate query name", input: reflect.TypeFor[struct {
			A string `parameter:"A,kind=query,in=q"`
			B string `parameter:"B,kind=query,in=q"`
		}](), want: "duplicate transport"},
		{name: "duplicate case-folded header", input: reflect.TypeFor[struct {
			A string `parameter:"A,kind=header,in=X-Name"`
			B string `parameter:"B,kind=header,in=x-name"`
		}](), want: "duplicate transport"},
		{name: "overlapping body", input: reflect.TypeFor[struct {
			A child  `parameter:"A,kind=body"`
			B string `parameter:"B,kind=body,in=name"`
		}](), want: "overlap"},
		{name: "mixed form and JSON", input: reflect.TypeFor[struct {
			A string `parameter:"A,kind=form,in=a"`
			B string `parameter:"B,kind=body,in=b"`
		}](), want: "mixed body and form"},
		{name: "tabular", settings: &spec.Settings{Format: "tabular"}, want: "wire schema projection"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := fixture{input: tc.input, output: tc.output}
			if tc.settings != nil {
				f.component = &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "Records"}, Routes: []*spec.Route{{Method: "POST", Path: "/records"}}, Settings: tc.settings}
			}
			entry, buildErr := f.build()
			if tc.name == "duplicate query name" || tc.name == "duplicate case-folded header" {
				require.ErrorContains(t, buildErr, "ambiguous input alias")
				return
			}
			require.NoError(t, buildErr)
			doc, err := (openapi.Generator{}).Generate(context.Background(), documentRequest(entry))
			require.ErrorContains(t, err, tc.want)
			require.Nil(t, doc)
		})
	}
}

func TestXMLResponseHasOpaqueTextSchema(t *testing.T) {
	entry := (fixture{component: &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "Records"},
		Routes: []*spec.Route{{Method: "POST", Path: "/records"}}, Settings: &spec.Settings{Format: "xml"}}}).registration(t)
	doc, err := (openapi.Generator{}).Generate(context.Background(), documentRequest(entry))
	require.NoError(t, err)
	require.Equal(t, "string", doc.Paths["/records"].Post.Responses["200"].Content["application/xml"].Schema.Type)
}

func TestRouteAndRegistrationFailures(t *testing.T) {
	for _, tc := range []struct{ name, method, path, header, key, want string }{
		{"CONNECT", "CONNECT", "/records", "", "", "cannot be represented"},
		{"missing path input", "GET", "/records/{id}", "", "", "no effective input"},
		{"empty API key", "GET", "/records", "X-Key", "", "absent header"},
		{"invalid header", "GET", "/records", "bad header", "value", "invalid API-key"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			entry := (fixture{component: &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "Records"}, Routes: []*spec.Route{{Method: tc.method, Path: tc.path, APIKeyHeader: tc.header, APIKeyValue: tc.key}}}}).registration(t)
			doc, err := (openapi.Generator{}).Generate(context.Background(), documentRequest(entry))
			require.Nil(t, doc)
			require.ErrorContains(t, err, tc.want)
		})
	}
	entry := (fixture{}).registration(t)
	_, err := (openapi.Generator{}).Generate(context.Background(), documentRequest(entry, entry))
	require.ErrorContains(t, err, "duplicate component")
	copyEntry := *entry
	copyEntry.Component = entry.Component.Clone()
	copyEntry.Component.Key.Name = "Other"
	_, err = (openapi.Generator{}).Generate(context.Background(), documentRequest(entry, &copyEntry))
	require.ErrorContains(t, err, "duplicate route")
	copyEntry.OutputType = reflect.TypeFor[string]()
	_, err = (openapi.Generator{}).Generate(context.Background(), documentRequest(&copyEntry))
	require.ErrorContains(t, err, "inconsistent")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = (openapi.Generator{}).Generate(ctx, documentRequest(entry))
	require.ErrorIs(t, err, context.Canceled)
	request := documentRequest(entry)
	request.Info.Title = ""
	_, err = (openapi.Generator{}).Generate(context.Background(), request)
	require.ErrorContains(t, err, "title and version")
}
