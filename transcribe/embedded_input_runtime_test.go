package transcribe

import (
	"context"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	requestprovider "github.com/viant/bindly/provider/request"
	"github.com/viant/datly/bootstrap"
	mcptool "github.com/viant/datly/mcp/tool"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/handler/engine"
	predicatevelty "github.com/viant/datly/runtime/predicate/velty"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	xpredicate "github.com/viant/xdatly/predicate"
)

type EmbeddedRangeInput struct {
	Minimum     int  `json:"minimum"`
	Initialized bool `json:"-"`
}

func (v *EmbeddedRangeInput) Init(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	v.Minimum++
	v.Initialized = true
	return nil
}

type EmbeddedMinimumPredicate struct{}

func (*EmbeddedMinimumPredicate) Compute(_ context.Context, value any, args ...any) (*xpredicate.Criteria, error) {
	options, err := xpredicate.Pairs(args...)
	if err != nil {
		return nil, err
	}
	// Test-only predicate: authored identifiers deliberately vary between callers.
	return &xpredicate.Criteria{Expression: options["Alias"].(string) + "." + options["Column"].(string) + " >= ?", Placeholders: []any{value.(EmbeddedRangeInput).Minimum}}, nil
}

func TestDynamicDQLRetainsEmbeddedInputValue(t *testing.T) {
	types := typecatalog.NewCatalog()
	typ := reflect.TypeFor[EmbeddedRangeInput]()
	require.NoError(t, types.Register(typecatalog.TypeOriginPackage, x.NewType(typ)))
	require.NoError(t, types.Register(typecatalog.TypeOriginPackage, x.NewType(reflect.TypeFor[EmbeddedMinimumPredicate]())))
	dql := `#package('example.com/dynamic/reader')
#import('filters','` + typ.PkgPath() + `')
#setting($_ = $route('/records','POST'))
#define($_ = $EmbeddedRangeInput<filters.EmbeddedRangeInput>(body/window).Required().Tag('embedded:"true" mcp:"name=window"').WithPredicate(0,'handler','filters.EmbeddedMinimumPredicate','Alias','r','Column','id'))
#define($_ = $Selection<string>(body/selection).Optional())
#define($_ = $DerivedMinimum<int>(input/EmbeddedRangeInput.Minimum).Required())
#define($_ = $Rows<[]*Record>(output/view))
SELECT r.*,type(r,'Record'),CAST(r.id AS int) FROM (SELECT 1 AS id) r`
	contract, err := NewCompiler().RuntimeContractsInModule(t.Context(), "example.com/dynamic", &Source{Scope: "example.com/dynamic/reader", Name: "Records", Text: dql, Types: types})
	require.NoError(t, err)
	field, ok := contract.InputType.FieldByName("EmbeddedRangeInput")
	require.True(t, ok)
	require.True(t, field.Anonymous)
	require.Equal(t, typ, field.Type)
	_, ok = contract.InputType.FieldByName("Selection")
	require.True(t, ok)
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: contract.Component, InputType: contract.InputType, OutputType: contract.OutputType, Types: contract.Types, Resources: contract.Resources})
	require.NoError(t, err)
	route, ok := artifact.Input.ForRoute(spec.RouteRef{Method: "POST", Path: "/records"})
	require.True(t, ok)
	tool, err := mcptool.NewCompiler().Compile(mcptool.Input{Contract: route, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "records"}})
	require.NoError(t, err)
	require.Contains(t, tool.Metadata().InputSchema.Properties, "window")
	require.NotContains(t, tool.Metadata().InputSchema.Properties, "minimum", "Go embedding must not implicitly flatten MCP body arguments")
	program, err := predicatevelty.Compile(predicatevelty.CompileInput{Component: contract.Component, InputType: contract.InputType, Lookup: func(string) (reflect.Type, error) { return reflect.TypeFor[EmbeddedMinimumPredicate](), nil }})
	require.NoError(t, err)
	req := httptest.NewRequest("POST", "/records", strings.NewReader(`{"window":{"minimum":7},"selection":"chosen"}`))
	req.Header.Set("Content-Type", "application/json")
	scope, err := requestprovider.New(req)
	require.NoError(t, err)
	defer scope.Close()
	handler := rhandler.HandlerFunc(func(ctx context.Context, invocation rhandler.Invocation) (any, error) {
		value := reflect.ValueOf(invocation.Input).Elem()
		marker := value.FieldByName("Has")
		require.False(t, marker.IsNil())
		require.True(t, marker.Elem().FieldByName("EmbeddedRangeInput").Bool())
		require.True(t, marker.Elem().FieldByName("Selection").Bool())
		base := value.FieldByName("EmbeddedRangeInput").Interface().(EmbeddedRangeInput)
		require.Equal(t, 8, base.Minimum)
		require.True(t, base.Initialized)
		require.Equal(t, "chosen", value.FieldByName("Selection").String())
		require.EqualValues(t, 7, value.FieldByName("DerivedMinimum").Int(), "derived scalar is a binding-time snapshot, not a live alias")
		criteria, err := program.Criteria(ctx, invocation.Binder, 0)
		require.NoError(t, err)
		require.Equal(t, "( r.id >= ? )", criteria.Expression)
		require.Equal(t, []any{8}, criteria.Placeholders, "predicate must read the initialized embedded value")
		return nil, nil
	})
	_, err = engine.New().Execute(t.Context(), engine.Request{Input: route, Providers: scope.Providers(), Handler: handler})
	require.NoError(t, err)
	mcpScope, err := tool.Scope(map[string]any{"window": map[string]any{"minimum": 7}, "selection": "chosen"})
	require.NoError(t, err)
	defer mcpScope.Close()
	_, err = engine.New().Execute(t.Context(), engine.Request{Input: route, Providers: mcpScope.Providers(), Handler: handler})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = engine.New().Execute(ctx, engine.Request{Input: route, Providers: mcpScope.Providers(), Handler: rhandler.HandlerFunc(func(context.Context, rhandler.Invocation) (any, error) {
		t.Fatal("canceled invocation reached handler")
		return nil, nil
	})})
	require.ErrorIs(t, err, context.Canceled)
}
