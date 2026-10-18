package registry_test

import (
	"context"
	"encoding/json"
	"github.com/viant/bindly/resource"
	"reflect"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/readergraph"
	"github.com/viant/datly/internal/testharness/mcpclient"
	"github.com/viant/datly/mcp"
	"github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/mcp-protocol/schema"
	"github.com/viant/x"
)

type olderJWTOutput struct {
	Rows []*coverageResult `parameter:",kind=output,in=view" view:"records" sql:"uri=records.sql"`
}

func TestScopedJWTOlderReaderGraphDestinationProjectsWithoutMutation(t *testing.T) {
	required := true
	parent := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: reflect.TypeFor[canonicalJWTParentInput]().PkgPath(), Name: "CanonicalParent"}, TypeContext: &spec.TypeContext{Imports: []spec.ImportSpec{{Alias: "jwt", Package: "github.com/viant/scy/auth/jwt"}}}, Routes: []*spec.Route{{Method: "GET", Path: "/canonical-coverage", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "CanonicalScopedSearch"}}}}, Parameters: []*spec.Parameter{{Name: "Jwt", TypeExpr: "string", Source: spec.BindSource{Kind: "header", Name: "Authorization"}, Required: &required, ErrorStatusCode: 401, Codec: &spec.Codec{Body: "JwtClaim"}}}}
	parent.Settings = &spec.Settings{}
	parent.Parameters = append(parent.Parameters,
		&spec.Parameter{Name: "Lazy", TypeExpr: "*coverageResult", Source: spec.BindSource{Kind: "component", Name: "GET:/canonical-coverage-child"}, Scope: "lazy"},
		&spec.Parameter{Name: "Query", TypeExpr: "string", Source: spec.BindSource{Kind: "query", Name: "query"}})
	parent.Parameters = append(parent.Parameters, &spec.Parameter{Name: "Rows", TypeExpr: "[]*coverageResult", Source: spec.BindSource{Kind: "output", Name: "view"}})
	parent.RootView = &spec.View{Name: "records", TypeName: "coverageResult", Source: &spec.ViewSource{URI: "records.sql"}, Columns: []*spec.Column{{Name: "ID", Type: spec.TypeRef{Name: "int"}}}}
	body := []byte("SELECT 1 AS ID")
	resources := resource.New()
	require.NoError(t, resources.Register("", fstest.MapFS{"records.sql": {Data: body}}))
	var err error
	parent, err = readergraph.Admit(parent, x.NewType(reflect.TypeFor[canonicalJWTParentInput]()), x.NewType(reflect.TypeFor[olderJWTOutput]()), map[string][]byte{"records.sql": body})
	require.NoError(t, err)
	// Consume serialized canonical metadata on the parent and plain Go field
	// metadata on its scoped child, just as native reader publication does.
	raw, err := json.Marshal(parent)
	require.NoError(t, err)
	var persisted spec.Component
	require.NoError(t, json.Unmarshal(raw, &persisted))
	persisted.ReaderGraph = parent.ReaderGraph
	authority := persisted.ReaderGraph
	child := &spec.Component{Parameters: []*spec.Parameter{{Name: "Jwt", TypeExpr: "string", OutputTypeExpr: "*github.com/viant/scy/auth/jwt.Claims", Source: spec.BindSource{Kind: "header", Name: "Authorization"}, ErrorStatusCode: 401, Codec: &spec.Codec{Body: "JwtClaim"}}}, Key: spec.Key{Kind: spec.KindComponent, Scope: "private", Name: "CanonicalChild"}, Routes: []*spec.Route{{Method: "GET", Path: "/canonical-coverage-child"}}}
	factory := coverageFactory(t)
	var entries []*registry.RegisteredComponent
	for i, c := range []*spec.Component{&persisted, child} {
		input := reflect.TypeFor[canonicalJWTParentInput]()
		if i == 1 {
			input = reflect.TypeFor[canonicalJWTChildInput]()
		}
		out := reflect.TypeFor[coverageResult]()
		if i == 0 {
			out = reflect.TypeFor[olderJWTOutput]()
		}
		artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: c, InputType: input, OutputType: out, CodecFactory: factory, Resources: resources})
		require.NoError(t, err)
		entry, err := artifact.Registration(registry.RegisteredComponent{Handler: handler.HandlerFunc(func(context.Context, handler.Invocation) (any, error) {
			t.Error("metadata executed handler")
			return nil, nil
		})})
		require.NoError(t, err)
		entries = append(entries, entry)
	}
	catalog, err := registry.NewInputCatalog(entries)
	require.NoError(t, err)
	fields, err := catalog.FieldsFor(entries[0].Component.Key, spec.RouteRef{Method: "GET", Path: "/canonical-coverage"})
	require.NoError(t, err)
	require.Len(t, fields, 2)
	for _, field := range fields {
		if field.Binding().Location.Kind == "header" {
			require.True(t, field.VerifiesJWT())
			require.True(t, *field.Binding().Required)
		}
	}
	invoker := &coverageInvoker{}
	service, err := mcp.New(mcp.Config{Components: entries, Invoker: invoker})
	require.NoError(t, err)
	tools, err := mcpclient.New(t, service, schema.LatestProtocolVersion).ListTools(t.Context(), nil)
	require.NoError(t, err)
	require.Len(t, tools.Tools, 1)
	require.Len(t, tools.Tools[0].InputSchema.Properties, 1)
	require.NotNil(t, tools.Tools[0].InputSchema.Properties["Query"])
	require.Zero(t, invoker.calls)
	after, err := json.Marshal(&persisted)
	require.NoError(t, err)
	require.Equal(t, string(raw), string(after))
	require.Empty(t, persisted.Parameters[0].OutputTypeExpr)
	require.Same(t, authority, persisted.ReaderGraph)
	require.Same(t, authority, entries[0].Component.ReaderGraph)
}
