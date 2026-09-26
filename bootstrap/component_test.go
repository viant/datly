package bootstrap

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/viant/datly/spec"
	dtag "github.com/viant/datly/tag"
)

type packageViewRow struct {
	ID   int
	Name string
}

type packageTaggedInput struct {
	Existing []*packageViewRow `parameter:"Existing,kind=view,in=Existing,cardinality=Many,cacheable=true" view:"Existing,table=users,connector=analytics,batch=25,batchConcurrency=2,publishParent=true,relationalConcurrency=4,allowNulls=true,groupable=true,partitioner=example.Partitioner,concurrency=3" sql:"SELECT id, name FROM users WHERE tenant = :Tenant"`
	Tenant   int               `parameter:"Tenant,kind=query,in=tenant,required"`
}

type packageTaggedOutput struct {
	Data []*packageViewRow `parameter:",kind=output,in=view" view:"Users,table=users,connector=analytics,batch=10" sql:"SELECT id, name FROM users WHERE tenant = :Tenant"`
}

type packageParameterMetadataInput struct {
	Projection []string `bind:"Fields,kind=query,in=fields,dataType=string" codec:"structql,outputType=[]string,trim" querySelector:"users" predicate:"contains,group=2,u,name" predicate:"tenant,applyWhenAbsent=true,tenant_id=7" desc:"selected fields" example:"id,name"`
}

type packageViewOptionsInput struct {
	Rows []*packageViewRow `bind:"Rows,kind=view,in=Rows" view:"Rows,type=UserRow,dest=users.go,table=users,connector=analytics,cache=users-cache,limit=25,selectorNamespace=u,selectorProjection=true,selectorOrderBy=true,selectorCriteria=true,selectorLimit=true,selectorOffset=true,selectorPage=true,selectorFilterable={ID,Name},selectorOrderByColumns={created:CreatedAt}"`
}

func TestContractResolverBuildsCanonicalInputMetadata(t *testing.T) {
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/package", Name: "Users"}}
	actual, err := (ContractResolver{Component: component, InputType: linkedContractType(reflect.TypeOf(packageTaggedInput{}))}).Resolve()
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if len(component.Parameters) != 0 || len(component.Views) != 0 {
		t.Fatal("resolver mutated the supplied component")
	}
	if actual == component || len(actual.Parameters) != 2 || len(actual.Views) != 1 {
		t.Fatalf("resolved component = %+v", actual)
	}
	viewParam := actual.Parameters[0]
	if viewParam.Name != "Existing" || viewParam.Source != (spec.BindSource{Kind: "view", Name: "Existing"}) ||
		viewParam.Cardinality != "Many" || viewParam.Cacheable == nil || !*viewParam.Cacheable {
		t.Fatalf("view param = %+v", viewParam)
	}
	queryParam := actual.Parameters[1]
	if queryParam.Source != (spec.BindSource{Kind: "query", Name: "tenant"}) || queryParam.Required == nil || !*queryParam.Required {
		t.Fatalf("query param = %+v", queryParam)
	}
	view := actual.Views[0]
	if view.Key != (spec.Key{Kind: spec.KindView, Scope: "example.com/package", Name: "Existing"}) ||
		view.Source.Table != "users" || view.Source.SQL != "SELECT id, name FROM users WHERE tenant = :Tenant" ||
		view.Source.Bindings == nil || view.Source.Bindings.Connector != "analytics" || view.BatchSize != 25 || view.BatchConcurrency != 2 ||
		!view.PublishParent || view.RelationalConcurrency != 4 || view.AllowNulls == nil || !*view.AllowNulls ||
		view.Groupable == nil || !*view.Groupable || view.Partitioning == nil ||
		view.Partitioning.Type != "example.Partitioner" || view.Partitioning.Concurrency != 3 {
		t.Fatalf("view = %+v", view)
	}
}

func TestContractResolverPreservesCanonicalAuthority(t *testing.T) {
	type input struct {
		Existing []*packageViewRow `parameter:"Existing,kind=view,in=Existing" view:"Existing,table=package_users"`
		Tenant   int               `parameter:"Tenant,kind=query,in=tenant"`
	}
	canonicalView := &spec.View{Name: "Existing", Source: &spec.ViewSource{SQL: "SELECT id FROM dql_users"}}
	component := &spec.Component{
		Parameters: []*spec.Parameter{
			{Name: "Existing", Source: spec.BindSource{Kind: "view", Name: "Existing"}},
			{Name: "Tenant", Source: spec.BindSource{Kind: "path", Name: "tenantID"}},
		},
		Views: []*spec.View{canonicalView},
	}
	actual, err := (ContractResolver{Component: component, InputType: linkedContractType(reflect.TypeOf(input{}))}).Resolve()
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if len(actual.Parameters) != 3 || actual.Parameters[1].Source.Kind != "path" || actual.Parameters[1].Source.Name != "tenantID" ||
		actual.Parameters[2].Source.Kind != "query" || actual.Parameters[2].Source.Name != "tenant" {
		t.Fatalf("distinct canonical and package params were not preserved: %+v", actual.Parameters)
	}
	if len(actual.Views) != 1 || actual.Views[0].Source.SQL != "SELECT id FROM dql_users" || actual.Views[0].Source.Table != "" {
		t.Fatalf("package tags overrode canonical view: %+v", actual.Views)
	}
}

func TestContractResolverRejectsConflictingViewNames(t *testing.T) {
	type input struct {
		Rows []*packageViewRow `parameter:"Rows,kind=view,in=Current" view:"Archived,table=users"`
	}
	_, err := (ContractResolver{InputType: linkedContractType(reflect.TypeOf(input{}))}).Resolve()
	if err == nil || !strings.Contains(err.Error(), `view binding "Current" conflicts with view tag "Archived"`) {
		t.Fatalf("expected view-name conflict, got %v", err)
	}
}

func TestBuildArtifactCompilesPackageViewDependency(t *testing.T) {
	type input struct {
		Existing []*packageViewRow `parameter:"Existing,kind=view,in=Existing" view:"Existing,table=users"`
	}
	artifact, err := BuildArtifact(ArtifactInput{
		Component: &spec.Component{Routes: []*spec.Route{{Method: "GET", Path: "/rows"}}},
		InputType: reflect.TypeOf(input{}),
	})
	if err != nil {
		t.Fatalf("BuildArtifact() error = %v", err)
	}
	if artifact.Component == nil || len(artifact.Component.Views) != 1 || len(artifact.ViewDependencies) != 1 ||
		artifact.ViewDependencies[0].TargetType != reflect.TypeOf([]*packageViewRow{}) || !artifact.ViewDependencies[0].Plan.DirectOutput {
		t.Fatalf("artifact = %+v", artifact)
	}
}

func TestContractResolverPreservesSQLResourceURI(t *testing.T) {
	type input struct {
		Rows []*packageViewRow `parameter:"Rows,kind=view,in=Rows" view:"Rows" sql:"uri=components:sql/rows.sql"`
	}
	component, err := (ContractResolver{InputType: linkedContractType(reflect.TypeOf(input{}))}).Resolve()
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	source := component.Views[0].Source
	if source.SQL != "" || source.URI != "components:sql/rows.sql" || len(source.Embeds) != 0 {
		t.Fatalf("resource source = %+v", source)
	}
}

func TestContractResolverClassifiesBindingURI(t *testing.T) {
	type input struct {
		Asset string `bind:"Asset,kind=query,in=asset,uri=assets:asset.sql"`
		Item  string `bind:"Item,kind=path,in=id,uri=/items/{id}"`
	}
	component, err := (ContractResolver{
		Component: &spec.Component{Routes: []*spec.Route{{Method: "GET", Path: "/items/{id}"}}},
		InputType: linkedContractType(reflect.TypeOf(input{})),
	}).Resolve()
	if err != nil {
		t.Fatal(err)
	}
	if len(component.Parameters) != 2 {
		t.Fatalf("params = %+v", component.Parameters)
	}
	asset, item := component.Parameters[0], component.Parameters[1]
	if asset.ResourceRef != "assets:asset.sql" || asset.Activation != nil {
		t.Fatalf("asset metadata = %+v", asset)
	}
	if item.Activation == nil || item.Activation.URI != "/items/{id}" || item.ResourceRef != "" {
		t.Fatalf("item metadata = %+v", item)
	}
}

func TestContractResolverPreservesInlineSQLWithSourceURI(t *testing.T) {
	type input struct {
		Rows []*packageViewRow `parameter:"Rows,kind=view,in=Rows" view:"Rows,uri=components:sql/rows.sql" sql:"SELECT id FROM rows"`
	}
	component, err := (ContractResolver{InputType: linkedContractType(reflect.TypeOf(input{}))}).Resolve()
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	source := component.Views[0].Source
	if source.SQL != "SELECT id FROM rows" || source.URI != "components:sql/rows.sql" || len(source.Embeds) != 0 {
		t.Fatalf("inline resource source = %+v", source)
	}
}

func TestRouteSourceResolveCreatesCanonicalPackageComponent(t *testing.T) {
	source := &RouteSource{
		HolderType: "Routes", FieldName: "Users", PackagePath: "example.com/app/users",
		Tag: dtag.Component{
			Name: "UserLookup", RouteName: "List", Method: "GET", Path: "/users", Connector: "analytics", Marshaller: "tabular", Description: "User lookup", Example: `{"id":1}`,
			APIKeyHeader: "X-Key", APIKeyValue: " secret ",
			Input: "UsersInput", Output: "UsersOutput", Handler: "HandleUsers", WarmupTarget: "GET:/users/read", Report: true, ReportLinkedInputType: "ReportInput", ReportDimensions: "Dimensions",
		},
	}
	component, err := source.Resolve(reflect.TypeOf(packageTaggedInput{}), nil)
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if component.Key != (spec.Key{Kind: spec.KindComponent, Scope: "example.com/app/users", Name: "UserLookup"}) ||
		component.Name != "UserLookup" || component.Description != "User lookup" || component.Example != `{"id":1}` || len(component.Routes) != 1 || component.Routes[0].Method != "GET" ||
		component.Routes[0].Name != "List" || component.Routes[0].Path != "/users" || component.Routes[0].Marshaller != "tabular" || component.Routes[0].Handler != "HandleUsers" ||
		component.Routes[0].APIKeyHeader != "X-Key" || component.Routes[0].APIKeyValue != " secret " || component.Settings == nil || component.Settings.DefaultConnector != "analytics" ||
		component.Settings.InputType != "UsersInput" || component.Settings.OutputType != "UsersOutput" || component.Settings.Report == nil ||
		component.Settings.WarmupTarget == nil || component.Settings.WarmupTarget.String() != "GET:/users/read" ||
		!component.Settings.Report.Enabled || component.Settings.Report.LinkedInputType != "ReportInput" || component.Settings.Report.InputLayout == nil ||
		component.Settings.Report.InputLayout.Dimensions != "Dimensions" ||
		len(component.Views) != 1 || len(component.Parameters) != 2 {
		t.Fatalf("component = %+v", component)
	}
}

func TestRouteSourceResolveUsesGenericContractTypesByDefault(t *testing.T) {
	source := &RouteSource{
		FieldName: "Users", PackagePath: "example.com/app/users",
		InputType: "UsersInput", OutputType: "UsersOutput",
		Tag: dtag.Component{Method: "GET", Path: "/users"},
	}
	component, err := source.Resolve(reflect.TypeOf(packageTaggedInput{}), reflect.TypeOf(packageTaggedOutput{}))
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if component.Settings == nil || component.Settings.InputType != "UsersInput" || component.Settings.OutputType != "UsersOutput" {
		t.Fatalf("generic contract settings = %+v", component.Settings)
	}
}

func TestRouteSourceResolveRejectsTaggedContractOverride(t *testing.T) {
	source := &RouteSource{
		PackagePath: "example.com/app/users", InputType: "UsersInput", OutputType: "UsersOutput",
		Tag: dtag.Component{Input: "OtherInput"},
	}
	_, err := source.Resolve(reflect.TypeOf(packageTaggedInput{}), reflect.TypeOf(packageTaggedOutput{}))
	if err == nil || !strings.Contains(err.Error(), "conflicts with Component contract") {
		t.Fatalf("expected tagged contract conflict, got %v", err)
	}
}

func TestContractResolverPreservesCanonicalParamWithConflictingSource(t *testing.T) {
	type input struct {
		Tenant string `parameter:"Tenant,kind=header,in=tenant"`
	}
	component, err := (ContractResolver{
		Component: &spec.Component{Parameters: []*spec.Parameter{{Name: "Tenant", Source: spec.BindSource{Kind: "query", Name: "tenant"}}}},
		InputType: linkedContractType(reflect.TypeOf(input{})),
	}).Resolve()
	if err != nil {
		t.Fatal(err)
	}
	if len(component.Parameters) != 2 || component.Parameters[0].Source.Kind != "query" || component.Parameters[1].Source.Kind != "header" {
		t.Fatalf("params = %+v", component.Parameters)
	}
}

func TestContractResolverBuildsCanonicalRootViewFromOutput(t *testing.T) {
	component, err := (ContractResolver{
		Component:  &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/package", Name: "Users"}},
		OutputType: linkedContractType(reflect.TypeOf(packageTaggedOutput{})),
	}).Resolve()
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if len(component.Parameters) != 1 || component.Parameters[0].Name != "Data" ||
		component.Parameters[0].Source != (spec.BindSource{Kind: "output", Name: "view"}) {
		t.Fatalf("output params = %+v", component.Parameters)
	}
	view := component.RootView
	if view == nil || view.Name != "Users" || view.Source == nil || view.Source.Table != "users" ||
		view.Source.SQL != "SELECT id, name FROM users WHERE tenant = :Tenant" ||
		view.Source.Bindings == nil || view.Source.Bindings.Connector != "analytics" || view.BatchSize != 10 {
		t.Fatalf("root view = %+v", view)
	}
}

func TestContractResolverBuildsCanonicalOutputRelationFromTags(t *testing.T) {
	type totals struct {
		Count int `sqlx:"count"`
	}
	type output struct {
		Data   []*packageViewRow `parameter:"Data,kind=output,in=view" view:"Users" sql:"SELECT id, name FROM users"`
		Totals totals            `parameter:"Totals,kind=output,in=summary" view:"Totals" sql:"SELECT COUNT(*) AS count FROM ($View.Users.NonWindowSQL) parent"`
	}
	component, err := (ContractResolver{
		Component:  &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/package", Name: "Users"}},
		OutputType: linkedContractType(reflect.TypeOf(output{})),
	}).Resolve()
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if component.RootView == nil || len(component.RootView.Relations) != 1 {
		t.Fatalf("root view = %+v", component.RootView)
	}
	relation := component.RootView.Relations[0]
	if relation == nil || relation.Kind != spec.RelationKindDerived || relation.Name != "Totals" || relation.Holder != "Totals" ||
		relation.Cardinality != spec.CardinalityOne || relation.View == nil || relation.View.Name != "Totals" || relation.View.Source == nil ||
		relation.View.Source.SQL != "SELECT COUNT(*) AS count FROM ($View.Users.NonWindowSQL) parent" {
		t.Fatalf("relation = %+v", relation)
	}
}

func TestContractResolverSummaryBeforeRootOutput(t *testing.T) {
	type totals struct {
		Count int `sqlx:"count"`
	}
	type output struct {
		Meta *totals           `parameter:"Meta,kind=output,in=derived" view:"Totals" sql:"SELECT COUNT(*) AS count FROM ($View.NonWindowSQL) parent"`
		Data []*packageViewRow `parameter:"Data,kind=output,in=view" view:"Users" sql:"SELECT id, name FROM users"`
	}
	for _, declaredRoot := range []bool{false, true} {
		component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "Users"}}
		if declaredRoot {
			component.RootView = &spec.View{Name: "Users"}
		}
		resolved, err := (ContractResolver{Component: component, OutputType: linkedContractType(reflect.TypeFor[output]())}).Resolve()
		if err != nil {
			t.Fatalf("declared root=%v: %v", declaredRoot, err)
		}
		root := resolved.RootView
		if root == nil || root.Source == nil || root.Source.SQL != "SELECT id, name FROM users" || len(root.Relations) != 1 || root.Relations[0].Holder != "Meta" {
			t.Fatalf("declared root=%v: summary lost during root resolution: %+v", declaredRoot, root)
		}
	}
}

func TestContractResolverReconcilesCanonicalOutputRelationToAliasedField(t *testing.T) {
	type totals struct {
		Count int `sqlx:"count"`
	}
	type output struct {
		Aggregate totals `parameter:"Totals,kind=output,in=summary" view:"Totals" sql:"SELECT 999 AS count"`
	}
	canonicalSQL := "SELECT COUNT(*) AS count FROM users"
	component, err := (ContractResolver{
		Component: &spec.Component{
			Parameters: []*spec.Parameter{{Name: "Totals", Source: spec.BindSource{Kind: "output", Name: "summary"}}},
			RootView: &spec.View{
				Name: "Users", Source: &spec.ViewSource{SQL: "SELECT id FROM users"},
				Relations: []*spec.Relation{{
					Name: "Totals", Kind: spec.RelationKindDerived, Holder: "Totals", Cardinality: spec.CardinalityOne,
					View: &spec.View{Name: "Totals", Source: &spec.ViewSource{SQL: canonicalSQL}},
				}},
			},
		},
		OutputType: linkedContractType(reflect.TypeOf(output{})),
	}).Resolve()
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if len(component.RootView.Relations) != 1 || component.RootView.Relations[0].Holder != "Aggregate" ||
		component.RootView.Relations[0].View.Source.SQL != canonicalSQL {
		t.Fatalf("relations = %+v", component.RootView.Relations)
	}
}

func TestContractResolverRejectsSubviewAndOutputRelationHolderCollision(t *testing.T) {
	type totals struct {
		Count int `sqlx:"count"`
	}
	type output struct {
		Totals totals `parameter:"Totals,kind=output,in=summary" view:"Totals" sql:"SELECT COUNT(*) AS count FROM users"`
	}
	_, err := (ContractResolver{
		Component: &spec.Component{
			Parameters: []*spec.Parameter{{Name: "Totals", Source: spec.BindSource{Kind: "output", Name: "summary"}}},
			RootView: &spec.View{
				Name: "Users", Source: &spec.ViewSource{SQL: "SELECT id FROM users"},
				Relations: []*spec.Relation{{
					Name: "Rows", Kind: spec.RelationKindSubview, Holder: "Totals", Cardinality: spec.CardinalityMany,
					View: &spec.View{Name: "Rows", Source: &spec.ViewSource{SQL: "SELECT id FROM rows"}},
				}},
			},
		},
		OutputType: linkedContractType(reflect.TypeOf(output{})),
	}).Resolve()
	if err == nil || !strings.Contains(err.Error(), "conflicts with subview relation") {
		t.Fatalf("expected relation-holder conflict, got %v", err)
	}
}

func TestContractResolverBuildsMultipleCanonicalOutputRelationsFromTags(t *testing.T) {
	type totals struct {
		Count int `sqlx:"count"`
	}
	type bounds struct {
		MinID int `sqlx:"min_id"`
		MaxID int `sqlx:"max_id"`
	}
	type output struct {
		Data   []*packageViewRow `parameter:"Data,kind=output,in=view" view:"Users" sql:"SELECT id, name FROM users"`
		Totals totals            `parameter:"Totals,kind=output,in=summary" view:"Totals" sql:"SELECT COUNT(*) AS count FROM users"`
		Bounds bounds            `parameter:"Bounds,kind=output,in=summary" view:"Bounds" sql:"SELECT MIN(id) AS min_id, MAX(id) AS max_id FROM users"`
	}
	component, err := (ContractResolver{OutputType: linkedContractType(reflect.TypeOf(output{}))}).Resolve()
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if component.RootView == nil || len(component.RootView.Relations) != 2 ||
		component.RootView.Relations[0].Name != "Totals" || component.RootView.Relations[0].Holder != "Totals" ||
		component.RootView.Relations[1].Name != "Bounds" || component.RootView.Relations[1].Holder != "Bounds" {
		t.Fatalf("relations = %+v", component.RootView)
	}
}

func TestContractResolverRejectsDifferentSummaryOwningAliasedField(t *testing.T) {
	type totals struct {
		Count int `sqlx:"count"`
	}
	type output struct {
		Aggregate totals `parameter:"Totals,kind=output,in=summary" view:"Totals" sql:"SELECT COUNT(*) AS count FROM users"`
	}
	_, err := (ContractResolver{
		Component: &spec.Component{
			Parameters: []*spec.Parameter{{Name: "Totals", Source: spec.BindSource{Kind: "output", Name: "summary"}}},
			RootView: &spec.View{
				Name: "Users", Source: &spec.ViewSource{SQL: "SELECT id FROM users"},
				Relations: []*spec.Relation{{
					Name: "Other", Kind: spec.RelationKindDerived, Holder: "Aggregate", Cardinality: spec.CardinalityOne,
					View: &spec.View{Name: "Other", Source: &spec.ViewSource{SQL: "SELECT 999 AS count"}},
				}},
			},
		},
		OutputType: linkedContractType(reflect.TypeOf(output{})),
	}).Resolve()
	if err == nil || !strings.Contains(err.Error(), `holder is already owned by relation "Other"`) {
		t.Fatalf("expected aliased-holder ownership conflict, got %v", err)
	}
}

func TestRouteSourceResolveUsesComponentRootViewName(t *testing.T) {
	source := &RouteSource{
		FieldName: "Users", PackagePath: "example.com/app/users",
		Tag: dtag.Component{Method: "GET", Path: "/users", View: "Users"},
	}
	component, err := source.Resolve(nil, reflect.TypeOf(packageTaggedOutput{}))
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if component.RootView == nil || component.RootView.Name != "Users" || component.RootView.Source == nil || component.RootView.Source.Table != "users" {
		t.Fatalf("root view = %+v", component.RootView)
	}
}

func TestContractResolverBuildsCanonicalParameterMetadata(t *testing.T) {
	component, err := (ContractResolver{InputType: linkedContractType(reflect.TypeOf(packageParameterMetadataInput{}))}).Resolve()
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if len(component.Parameters) != 1 {
		t.Fatalf("params = %+v", component.Parameters)
	}
	param := component.Parameters[0]
	if param.Description != "selected fields" || param.Example != "id,name" || param.QuerySelector == nil ||
		param.QuerySelector.View != "users" || param.QuerySelector.Property != spec.SelectorPropertyFields || len(param.Predicates) != 2 ||
		param.Predicates[0].Name != "contains" || param.Predicates[0].Group != 2 || len(param.Predicates[0].Args) != 2 ||
		param.Predicates[1].Name != "tenant" || !param.Predicates[1].ApplyWhenAbsent || len(param.Predicates[1].Args) != 1 ||
		param.TypeExpr != "string" || param.Codec == nil || param.Codec.Body != "structql" || param.Codec.OutputType != "[]string" || len(param.Codec.Args) != 1 || param.Codec.Args[0] != "trim" {
		t.Fatalf("param = %+v", param)
	}
}

func TestContractResolverKeepsSelectorPropertyWithDistinctPhysicalNames(t *testing.T) {
	type input struct {
		PodFields    []string `parameter:"PodFields,kind=query,in=podFields" querySelector:"view=Read,property=fields"`
		ViewerFields []string `parameter:"ViewerFields,kind=query,in=viewerFields" querySelector:"view=viewer,property=fields"`
	}
	component, err := (ContractResolver{InputType: linkedContractType(reflect.TypeOf(input{}))}).Resolve()
	if err != nil {
		t.Fatal(err)
	}
	if len(component.Parameters) != 2 {
		t.Fatalf("parameters=%+v", component.Parameters)
	}
	for _, param := range component.Parameters {
		if param.QuerySelector == nil || param.QuerySelector.Property != spec.SelectorPropertyFields {
			t.Fatalf("selector metadata=%+v", param)
		}
	}
}

func TestContractResolverRejectsTransportCodecWithoutSourceDataType(t *testing.T) {
	type input struct {
		Projection []string `bind:"Fields,kind=query,in=fields" codec:"CSV,outputType=[]string"`
	}
	_, err := (ContractResolver{InputType: linkedContractType(reflect.TypeOf(input{}))}).Resolve()
	if err == nil || !strings.Contains(err.Error(), "codec source dataType is required") {
		t.Fatalf("Resolve() error = %v, want missing codec source dataType", err)
	}
}

func TestContractResolverProjectsReflectedFieldTypesIntoCanonicalContract(t *testing.T) {
	type input struct {
		IDs []int     `parameter:"IDs,kind=query,in=id"`
		At  time.Time `parameter:"At,kind=query,in=at"`
	}
	component, err := (ContractResolver{InputType: linkedContractType(reflect.TypeOf(input{}))}).Resolve()
	if err != nil {
		t.Fatal(err)
	}
	if len(component.Parameters) != 2 || component.Parameters[0].TypeExpr != "[]int" || component.Parameters[1].TypeExpr != "time.Time" {
		t.Fatalf("params = %+v", component.Parameters)
	}
	if component.TypeContext == nil || len(component.TypeContext.Imports) != 1 ||
		component.TypeContext.Imports[0] != (spec.ImportSpec{Alias: "time", Package: "time"}) {
		t.Fatalf("type context = %+v", component.TypeContext)
	}
}

func TestContractResolverDoesNotCollapseLogicalNameAcrossSources(t *testing.T) {
	type input struct {
		Projection []string `bind:"Fields,kind=query,in=package_fields" querySelector:"package" predicate:"package" desc:"package" example:"package"`
	}
	component := &spec.Component{Parameters: []*spec.Parameter{{
		Name: "Fields", Source: spec.BindSource{Kind: "path", Name: "canonical_fields"},
		Description: "canonical", Example: "canonical",
		Predicates:    []*spec.Predicate{{Name: "canonical"}},
		QuerySelector: &spec.QuerySelectorBinding{View: "canonical", Property: spec.SelectorPropertyFields},
	}}}
	actual, err := (ContractResolver{Component: component, InputType: linkedContractType(reflect.TypeOf(input{}))}).Resolve()
	if err != nil {
		t.Fatal(err)
	}
	if len(actual.Parameters) != 2 {
		t.Fatalf("params = %+v", actual.Parameters)
	}
	canonical, packageField := actual.Parameters[0], actual.Parameters[1]
	if canonical.Source.Kind != "path" || canonical.Source.Name != "canonical_fields" || canonical.Description != "canonical" ||
		packageField.Source.Kind != "query" || packageField.Source.Name != "package_fields" || packageField.Description != "package" {
		t.Fatalf("distinct params were collapsed: %+v", actual.Parameters)
	}
}

func TestBuildArtifactCompilesPredicateForBindLogicalAlias(t *testing.T) {
	type input struct {
		Projection string `bind:"Search,kind=query,in=search" predicate:"equal,u,name"`
	}
	component := &spec.Component{Routes: []*spec.Route{{Method: "GET", Path: "/search"}}}
	artifact, err := BuildArtifact(ArtifactInput{Component: component, InputType: reflect.TypeOf(input{})})
	if err != nil {
		t.Fatal(err)
	}
	route, ok := artifact.Input.ForRoute(spec.RouteRef{Method: "GET", Path: "/search"})
	if artifact == nil || !ok || route.Plan() == nil || len(artifact.Component.Parameters) != 1 || artifact.Component.Parameters[0].Name != "Search" {
		t.Fatalf("artifact = %+v", artifact)
	}
}

func TestContractResolverRejectsQuerySelectorOnUnsupportedParameter(t *testing.T) {
	type input struct {
		Tenant string `bind:"Tenant,kind=query,in=tenant" querySelector:"users"`
	}
	if _, err := (ContractResolver{InputType: linkedContractType(reflect.TypeOf(input{}))}).Resolve(); err == nil || !strings.Contains(err.Error(), "query selector is not supported") {
		t.Fatalf("Resolve() error = %v, want unsupported selector", err)
	}
}

func TestContractResolverBuildsCanonicalViewOptions(t *testing.T) {
	component, err := (ContractResolver{InputType: linkedContractType(reflect.TypeOf(packageViewOptionsInput{}))}).Resolve()
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if len(component.Views) != 1 {
		t.Fatalf("views = %+v", component.Views)
	}
	view := component.Views[0]
	if view.TypeName != "UserRow" || view.Dest != "users.go" || view.Source == nil ||
		view.Source.Bindings == nil || view.Source.Bindings.Connector != "analytics" ||
		view.Source.Bindings.CacheName != "users-cache" || view.Source.Controls == nil || view.Source.Controls.Limit == nil ||
		*view.Source.Controls.Limit != 25 || view.Selector == nil || view.Selector.Namespace != "u" || !view.Selector.AllowFields ||
		!view.Selector.AllowOrderBy || !view.Selector.AllowCriteria || !view.Selector.AllowLimit || !view.Selector.AllowOffset ||
		!view.Selector.AllowPage || len(view.Selector.Filterable) != 2 || view.Selector.OrderAliases["created"] != "CreatedAt" {
		t.Fatalf("view = %+v", view)
	}
}
