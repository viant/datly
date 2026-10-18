package registry_test

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness/mcpclient"
	"github.com/viant/datly/mcp"
	"github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/mcp-protocol/schema"
	"github.com/viant/scy/auth/jwt"
)

type canonicalJWTParentInput struct {
	Jwt   *jwt.Claims     `parameter:"Jwt,kind=header,in=Authorization,dataType=string,errorCode=401,required=true" codec:"JwtClaim"`
	Lazy  *coverageResult `parameter:"Lazy,kind=component,in=GET:/canonical-coverage-child,scope=lazy"`
	Query string          `parameter:"Query,kind=query,in=query,required=false"`
}
type canonicalJWTChildInput struct {
	Jwt *jwt.Claims `parameter:"Jwt,kind=header,in=Authorization,dataType=string,errorCode=401,required=false" codec:"JwtClaim"`
}

func TestScopedJWTCanonicalMetadataAndPlainBootstrapProjectTogether(t *testing.T) {
	required := true
	parent := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "public", Name: "CanonicalParent"}, TypeContext: &spec.TypeContext{Imports: []spec.ImportSpec{{Alias: "jwt", Package: "github.com/viant/scy/auth/jwt"}}}, Routes: []*spec.Route{{Method: "GET", Path: "/canonical-coverage", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "CanonicalScopedSearch"}}}}, Parameters: []*spec.Parameter{{Name: "Jwt", TypeExpr: "string", OutputTypeExpr: "*github.com/viant/scy/auth/jwt.Claims", Source: spec.BindSource{Kind: "header", Name: "Authorization"}, Required: &required, ErrorStatusCode: 401, Codec: &spec.Codec{Body: "JwtClaim"}}}}
	// Consume serialized canonical metadata on the parent and plain Go field
	// metadata on its scoped child, just as native reader publication does.
	raw, err := json.Marshal(parent)
	require.NoError(t, err)
	var persisted spec.Component
	require.NoError(t, json.Unmarshal(raw, &persisted))
	child := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "private", Name: "CanonicalChild"}, Routes: []*spec.Route{{Method: "GET", Path: "/canonical-coverage-child"}}}
	factory := coverageFactory(t)
	var entries []*registry.RegisteredComponent
	for i, c := range []*spec.Component{&persisted, child} {
		input := reflect.TypeFor[canonicalJWTParentInput]()
		if i == 1 {
			input = reflect.TypeFor[canonicalJWTChildInput]()
		}
		artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: c, InputType: input, OutputType: reflect.TypeFor[coverageResult](), CodecFactory: factory})
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
}
