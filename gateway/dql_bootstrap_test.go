package gateway

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	marshalconfig "github.com/viant/datly/gateway/router/marshal/config"
	marshaljson "github.com/viant/datly/gateway/router/marshal/json"
	"github.com/viant/datly/repository"
	"github.com/viant/datly/repository/contract"
	shape "github.com/viant/datly/repository/shape"
	shapeCompile "github.com/viant/datly/repository/shape/compile"
	shapeLoad "github.com/viant/datly/repository/shape/load"
	operator2 "github.com/viant/datly/service/operator"
	"github.com/viant/datly/service/session"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/state/kind/locator"
	"github.com/viant/tagly/format/text"
)

func TestConfigValidate_AllowsEmptyRouteURLWithDQLBootstrap(t *testing.T) {
	cfg := &Config{
		ExposableConfig: ExposableConfig{
			DQLBootstrap: &DQLBootstrap{
				Sources: []string{"./testdata/*.dql"},
			},
		},
	}
	require.NoError(t, cfg.Validate())
}

func TestConfigValidate_FailsWithoutRouteAndBootstrap(t *testing.T) {
	cfg := &Config{}
	require.ErrorContains(t, cfg.Validate(), "RouteURL was empty")
}

func TestConfigValidate_FailsForEmptyBootstrapSources(t *testing.T) {
	cfg := &Config{
		ExposableConfig: ExposableConfig{
			DQLBootstrap: &DQLBootstrap{},
		},
	}
	require.ErrorContains(t, cfg.Validate(), "DQLBootstrap.Sources was empty")
}

func TestDiscoverDQLBootstrapSources(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, "sql", "nested"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(root, "sql", "a.dql"), []byte("SELECT 1"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(root, "sql", "nested", "b.sql"), []byte("SELECT 2"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(root, "sql", "nested", "skip.dql"), []byte("SELECT 3"), 0o644))

	sources, err := discoverDQLBootstrapSources(
		[]string{filepath.Join(root, "sql", "**", "*")},
		[]string{filepath.Join(root, "sql", "**", "skip.dql")},
	)
	require.NoError(t, err)
	require.Len(t, sources, 2)
	assert.Contains(t, sources, filepath.Join(root, "sql", "a.dql"))
	assert.Contains(t, sources, filepath.Join(root, "sql", "nested", "b.sql"))
}

func TestCompileBootstrapComponent_UsesShapeRouteMetadata(t *testing.T) {
	ctx := context.Background()
	repo, err := repository.New(ctx, repository.WithComponentURL(""), repository.WithNoPlugin())
	require.NoError(t, err)
	connectors, err := repo.Resources().Lookup(view.ResourceConnectors)
	require.NoError(t, err)
	connectors.Connectors = append(connectors.Connectors, &view.Connector{
		Connection: view.Connection{
			DBConfig: view.DBConfig{
				Name:   "test_conn",
				Driver: "sqlite3",
				DSN:    ":memory:",
			},
		},
	})

	root := t.TempDir()
	source := filepath.Join(root, "orders.dql")
	dql := "#setting($_ = $connector('test_conn'))\n#setting($_ = $route('/v1/api/orders', 'POST'))\nSELECT 1 AS id"
	require.NoError(t, os.WriteFile(source, []byte(dql), 0o644))

	component, err := compileBootstrapComponent(ctx, shapeCompile.New(), shapeLoad.New(), repo, source, &DQLBootstrap{}, "/v1/api")
	require.NoError(t, err)
	require.NotNil(t, component)
	assert.Equal(t, "POST", component.Method)
	assert.Equal(t, "/v1/api/orders", component.URI)
}

func TestDQLBootstrapEffectivePrecedence(t *testing.T) {
	assert.Equal(t, DQLBootstrapPrecedenceRoutesWins, (&DQLBootstrap{}).EffectivePrecedence())
	assert.Equal(t, DQLBootstrapPrecedenceDQLWins, (&DQLBootstrap{Precedence: "dql_wins"}).EffectivePrecedence())
	assert.Equal(t, DQLBootstrapPrecedenceRoutesWins, (&DQLBootstrap{Precedence: "unknown"}).EffectivePrecedence())
}

func TestApplyDQLBootstrap_Precedence(t *testing.T) {
	ctx := context.Background()
	repo, err := repository.New(ctx, repository.WithComponentURL(""), repository.WithNoPlugin())
	require.NoError(t, err)

	route := contract.Path{Method: "GET", URI: "/v1/api/test"}
	repo.Register(&repository.Component{Path: route})
	connectors, err := repo.Resources().Lookup(view.ResourceConnectors)
	require.NoError(t, err)
	connectors.Connectors = append(connectors.Connectors, &view.Connector{
		Connection: view.Connection{
			DBConfig: view.DBConfig{
				Name:   "test_conn",
				Driver: "sqlite3",
				DSN:    "sqlite:./test.db",
			},
		},
	})

	root := t.TempDir()
	source := filepath.Join(root, "test.dql")
	require.NoError(t, os.WriteFile(source, []byte("#setting($_ = $connector('test_conn'))\n#setting($_ = $route('/v1/api/test', 'GET'))\nSELECT 1 AS id"), 0o644))
	srv := &Service{Config: &Config{ExposableConfig: ExposableConfig{APIPrefix: "/v1/api"}}}

	routesWins := &DQLBootstrap{
		Sources:    []string{source},
		Precedence: DQLBootstrapPrecedenceRoutesWins,
	}
	require.NoError(t, srv.applyDQLBootstrap(ctx, repo, routesWins))
	provider, err := repo.Registry().LookupProvider(ctx, &route)
	require.NoError(t, err)
	require.NotNil(t, provider)
	component, err := provider.Component(ctx)
	require.NoError(t, err)
	assert.Nil(t, component.View)

	dqlWins := &DQLBootstrap{
		Sources:    []string{source},
		Precedence: DQLBootstrapPrecedenceDQLWins,
	}
	require.NoError(t, srv.applyDQLBootstrap(ctx, repo, dqlWins))
	provider, err = repo.Registry().LookupProvider(ctx, &route)
	require.NoError(t, err)
	require.NotNil(t, provider)
	component, err = provider.Component(ctx)
	require.NoError(t, err)
	require.NotNil(t, component.View)
	assert.Equal(t, "test", component.View.Name)
}

func TestCompileBootstrapComponent_PreservesShapeIOAndGroupingMetadata(t *testing.T) {
	ctx := context.Background()
	repo, err := repository.New(ctx, repository.WithComponentURL(""), repository.WithNoPlugin())
	require.NoError(t, err)

	connectors, err := repo.Resources().Lookup(view.ResourceConnectors)
	require.NoError(t, err)
	connectors.Connectors = append(connectors.Connectors, &view.Connector{
		Connection: view.Connection{
			DBConfig: view.DBConfig{
				Name:   "dev",
				Driver: "mysql",
				DSN:    "root:dev@tcp(127.0.0.1:3306)/dev?parseTime=true",
			},
		},
	})

	root := t.TempDir()
	source := filepath.Join(root, "vendors_grouping.dql")
	dql := `
#setting($_ = $connector('dev'))
#setting($_ = $route('/v1/api/shape/dev/vendors-grouping', 'GET'))
#define($_ = $VendorIDs<[]int>(query/vendorIDs))
#define($_ = $Fields<[]string>(query/_fields).Optional().QuerySelector('vendor'))
#define($_ = $OrderBy<string>(query/_orderby).Optional().QuerySelector('vendor'))
#define($_ = $Data<?>(output/view).Embed())
SELECT vendor.*,
       groupable(vendor),
       allowed_order_by_columns(vendor, 'accountId:ACCOUNT_ID,userCreated:USER_CREATED,totalId:TOTAL_ID,maxId:MAX_ID')
FROM (
    SELECT ACCOUNT_ID,
           USER_CREATED,
           SUM(ID) AS TOTAL_ID,
           MAX(ID) AS MAX_ID
    FROM VENDOR t
    WHERE t.ID IN ($VendorIDs)
    GROUP BY 1, 2
) vendor`
	require.NoError(t, os.WriteFile(source, []byte(dql), 0o644))

	planResult, err := shapeCompile.New().Compile(ctx, &shape.Source{
		Name: "vendors_grouping",
		Path: source,
		DQL:  dql,
	})
	require.NoError(t, err)
	artifact, err := shapeLoad.New().LoadComponent(ctx, planResult)
	require.NoError(t, err)
	loaded, ok := artifact.Component.(*shapeLoad.Component)
	require.True(t, ok)
	sourceRoot := lookupRootView(artifact.Resource, loaded.RootView)
	require.NotNil(t, sourceRoot)
	require.NotNil(t, sourceRoot.ColumnsConfig["ACCOUNT_ID"])
	require.NotNil(t, sourceRoot.ColumnsConfig["ACCOUNT_ID"].Groupable)
	assert.True(t, *sourceRoot.ColumnsConfig["ACCOUNT_ID"].Groupable)

	component, err := compileBootstrapComponent(ctx, shapeCompile.New(), shapeLoad.New(), repo, source, &DQLBootstrap{}, "/v1/api/shape")
	require.NoError(t, err)
	require.NotNil(t, component)
	require.NotNil(t, component.View)
	require.True(t, component.View.Groupable)
	require.NotNil(t, component.View.Selector)
	require.NotNil(t, component.View.Selector.Constraints)
	assert.True(t, component.View.Selector.Constraints.OrderBy)
	assert.Equal(t, "ACCOUNT_ID", component.View.Selector.Constraints.OrderByColumn["accountId"])
	assert.Equal(t, "ACCOUNT_ID", component.View.Selector.Constraints.OrderByColumn["accountid"])
	require.NotNil(t, component.View.ColumnsConfig["ACCOUNT_ID"])
	require.NotNil(t, component.View.ColumnsConfig["ACCOUNT_ID"].Groupable)
	assert.True(t, *component.View.ColumnsConfig["ACCOUNT_ID"].Groupable)
	assert.Equal(t, text.CaseFormatLowerCamel, component.Output.CaseFormat)

	inputVendorIDs := component.Input.Type.Parameters.Lookup("VendorIDs")
	require.NotNil(t, inputVendorIDs)
	assert.Equal(t, state.KindQuery, inputVendorIDs.In.Kind)
	assert.Equal(t, "vendorIDs", inputVendorIDs.In.Name)

	inputFields := component.Input.Type.Parameters.Lookup("Fields")
	require.NotNil(t, inputFields)
	assert.Equal(t, state.KindQuery, inputFields.In.Kind)
	assert.Equal(t, "_fields", inputFields.In.Name)

	outputView := component.Output.Type.Parameters.LookupByLocation(state.KindOutput, "view")
	require.NotNil(t, outputView)
	assert.Contains(t, outputView.Tag, `anonymous:"true"`)
	assert.Equal(t, state.Many, component.Output.Cardinality)
}

func TestCompileBootstrapComponent_MetaFormatOutputTypeMatchesRootView(t *testing.T) {
	ctx := context.Background()
	repo, err := repository.New(ctx, repository.WithComponentURL(""), repository.WithNoPlugin())
	require.NoError(t, err)

	connectors, err := repo.Resources().Lookup(view.ResourceConnectors)
	require.NoError(t, err)
	connectors.Connectors = append(connectors.Connectors, &view.Connector{
		Connection: view.Connection{
			DBConfig: view.DBConfig{
				Name:   "dev",
				Driver: "sqlite3",
				DSN:    ":memory:",
			},
		},
	})

	source := filepath.Join("..", "e2e", "v1", "dql", "dev", "vendorsrv", "meta_format.dql")
	dqlBytes, err := os.ReadFile(source)
	require.NoError(t, err)
	planResult, err := shapeCompile.New().Compile(ctx, &shape.Source{
		Name: "meta_format",
		Path: source,
		DQL:  string(dqlBytes),
	})
	require.NoError(t, err)
	artifact, err := shapeLoad.New().LoadComponent(ctx, planResult)
	require.NoError(t, err)
	loaded, ok := artifact.Component.(*shapeLoad.Component)
	require.True(t, ok)
	loadedRoot := lookupRootView(artifact.Resource, loaded.RootView)
	require.NotNil(t, loadedRoot)
	require.NotNil(t, loadedRoot.Schema)
	t.Logf("loaded root view schema type: %v", loadedRoot.Schema.Type())

	component, err := compileBootstrapComponent(ctx, shapeCompile.New(), shapeLoad.New(), repo, source, &DQLBootstrap{}, "/v1/api/shape")
	require.NoError(t, err)
	require.NotNil(t, component)
	require.NotNil(t, component.View)
	require.NotNil(t, component.View.Schema)
	require.NotNil(t, component.View.Schema.Type())

	outputView := component.Output.Type.Parameters.LookupByLocation(state.KindOutput, "view")
	require.NotNil(t, outputView)
	require.NotNil(t, outputView.Schema)
	require.NotNil(t, outputView.Schema.Type())

	rootType := component.View.OutputType()
	outputType := outputView.OutputType()
	assert.Equal(t, rootType.Kind(), outputType.Kind())
	if rootType.Kind() == reflect.Slice {
		assert.Equal(t, rootType.Elem().Kind(), outputType.Elem().Kind())
	}

	outputSummary := component.Output.Type.Parameters.LookupByLocation(state.KindOutput, "summary")
	require.NotNil(t, outputSummary)
	require.NotNil(t, outputSummary.Schema)
	require.NotNil(t, outputSummary.Schema.Type())
	require.NotNil(t, component.View.Template)
	require.NotNil(t, component.View.Template.Summary)
	require.NotNil(t, component.View.Template.Summary.Schema)
	require.NotNil(t, component.View.Template.Summary.Schema.Type())
	assert.Equal(t, component.View.Template.Summary.Schema.Type().String(), outputSummary.Schema.Type().String())
}

func TestCompileBootstrapComponent_MetaFormatLiveOutputMarshal(t *testing.T) {
	ctx := context.Background()
	repo, err := repository.New(ctx, repository.WithComponentURL(""), repository.WithNoPlugin())
	require.NoError(t, err)

	connectors, err := repo.Resources().Lookup(view.ResourceConnectors)
	require.NoError(t, err)
	connectors.Connectors = append(connectors.Connectors, &view.Connector{
		Connection: view.Connection{
			DBConfig: view.DBConfig{
				Name:   "dev",
				Driver: "mysql",
				DSN:    "root:dev@tcp(127.0.0.1:3306)/dev?parseTime=true",
			},
		},
	})

	source := filepath.Join("..", "e2e", "v1", "dql", "dev", "vendorsrv", "meta_format.dql")
	component, err := compileBootstrapComponent(ctx, shapeCompile.New(), shapeLoad.New(), repo, source, &DQLBootstrap{}, "/v1/api/shape")
	require.NoError(t, err)
	require.NotNil(t, component.View)
	require.NotNil(t, component.View.Schema)
	t.Logf("root view schema type: %v", component.View.Schema.Type())
	if outputView := component.Output.Type.Parameters.LookupByLocation(state.KindOutput, "view"); outputView != nil && outputView.Schema != nil {
		t.Logf("output/view schema type: %v", outputView.Schema.Type())
	}
	if outputSummary := component.Output.Type.Parameters.LookupByLocation(state.KindOutput, "summary"); outputSummary != nil && outputSummary.Schema != nil {
		t.Logf("output/summary schema type: %v", outputSummary.Schema.Type())
	}

	svc := operator2.New()
	req, err := http.NewRequest(http.MethodGet, "http://127.0.0.1/v1/api/shape/dev/meta/vendors-format/", nil)
	require.NoError(t, err)
	sess := session.New(component.View, session.WithComponent(component), session.WithLocatorOptions(locator.WithRequest(req)))
	outputValue, err := svc.Operate(ctx, sess, component)
	require.NoError(t, err)
	require.NotNil(t, outputValue)
	t.Logf("output type: %T", outputValue)

	marshaller := marshaljson.New(&marshalconfig.IOConfig{CaseFormat: component.Output.CaseFormat})
	_, err = marshaller.Marshal(outputValue)
	require.NoError(t, err)
}

func TestCompileBootstrapComponent_UserAclMaterializesAnonymousOutputStateType(t *testing.T) {
	ctx := context.Background()
	repo, err := repository.New(ctx, repository.WithComponentURL(""), repository.WithNoPlugin())
	require.NoError(t, err)

	connectors, err := repo.Resources().Lookup(view.ResourceConnectors)
	require.NoError(t, err)
	connectors.Connectors = append(connectors.Connectors, &view.Connector{
		Connection: view.Connection{
			DBConfig: view.DBConfig{
				Name:   "dev",
				Driver: "sqlite3",
				DSN:    ":memory:",
			},
		},
	})

	source := filepath.Join("..", "e2e", "v1", "dql", "dev", "vendorsrv", "user_acl.dql")
	component, err := compileBootstrapComponent(ctx, shapeCompile.New(), shapeLoad.New(), repo, source, &DQLBootstrap{}, "/v1/api/shape")
	require.NoError(t, err)
	require.NotNil(t, component)
	require.True(t, component.Output.Type.Type().IsDefined())
	require.NotNil(t, component.Output.Type.Schema)
	require.NotNil(t, component.Output.Type.Schema.Type())

	outputView := component.Output.Type.Parameters.LookupByLocation(state.KindOutput, "view")
	require.NotNil(t, outputView)
	require.NotNil(t, outputView.Schema)
	require.NotNil(t, outputView.Schema.Type())
	assert.Equal(t, reflect.Pointer, outputView.OutputType().Kind())
}

func TestCompileBootstrapComponent_PatchBasicOneBodyParameterIsSingular(t *testing.T) {
	ctx := context.Background()
	repo, err := repository.New(ctx, repository.WithComponentURL(""), repository.WithNoPlugin())
	require.NoError(t, err)

	connectors, err := repo.Resources().Lookup(view.ResourceConnectors)
	require.NoError(t, err)
	connectors.Connectors = append(connectors.Connectors, &view.Connector{
		Connection: view.Connection{
			DBConfig: view.DBConfig{
				Name:   "dev",
				Driver: "sqlite3",
				DSN:    ":memory:",
			},
		},
	})

	source := filepath.Join("..", "e2e", "v1", "dql", "dev", "events", "patch_basic_one.dql")
	component, err := compileBootstrapComponent(ctx, shapeCompile.New(), shapeLoad.New(), repo, source, &DQLBootstrap{}, "/v1/api/shape")
	require.NoError(t, err)
	require.NotNil(t, component)

	bodyParams := component.Input.Type.Parameters.FilterByKind(state.KindRequestBody)
	require.Len(t, bodyParams, 1)
	body := bodyParams[0]
	require.NotNil(t, body)
	require.NotNil(t, body.Schema)
	assert.Equal(t, state.One, body.Schema.Cardinality)
	require.NotNil(t, body.Schema.Type())
	assert.NotEqual(t, reflect.Slice, body.Schema.Type().Kind(), "body schema should not remain slice-shaped")

	inputStateType := component.Input.Type.Type()
	require.NotNil(t, inputStateType)
	inputType := inputStateType.Type()
	require.NotNil(t, inputType)
	if inputType.Kind() == reflect.Ptr {
		inputType = inputType.Elem()
	}
	field, ok := inputType.FieldByName("Foos")
	require.True(t, ok)
	assert.NotEqual(t, reflect.Slice, field.Type.Kind(), "input Foos field should not remain slice-shaped")
}
