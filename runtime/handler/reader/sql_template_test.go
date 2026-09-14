package reader

import (
	"context"
	"net/url"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/assertly"
	"github.com/viant/bindly/locator"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness"
	handlerengine "github.com/viant/datly/runtime/handler/engine"
	handlerprovider "github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/spec"
	rsql "github.com/viant/datly/sql"
	sqltemplate "github.com/viant/datly/sql/template"
	"github.com/viant/datly/transcribe"
	xhandler "github.com/viant/xdatly/handler"
	xstate "github.com/viant/xdatly/state"
)

func TestReaderHandler_ExecutesAuthoredDQLBindingsAcrossRootAndRelation(t *testing.T) {
	ctx := context.Background()
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(ctx,
		`CREATE TABLE authored_orders (id INTEGER PRIMARY KEY, tenant_id INTEGER);`,
		`CREATE TABLE authored_items (id INTEGER PRIMARY KEY, order_id INTEGER, tenant_id INTEGER);`,
		`INSERT INTO authored_orders(id, tenant_id) VALUES (1, 1), (2, 2);`,
		`INSERT INTO authored_items(id, order_id, tenant_id) VALUES (10, 1, 1), (20, 2, 2), (21, 2, 2);`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	type input struct {
		TenantID int `parameter:"TenantID,kind=query,in=tenantID,required"`
	}
	type item struct {
		ID       int `sqlx:"id"`
		OrderID  int `sqlx:"order_id"`
		TenantID int `sqlx:"tenant_id"`
	}
	type order struct {
		ID       int     `sqlx:"id"`
		TenantID int     `sqlx:"tenant_id"`
		Items    []*item `view:"items" on:"ID:id=OrderID:order_id"`
	}
	type output struct {
		Data []*order `parameter:"Data,kind=output,in=view" view:"Orders" sql:"SELECT id, tenant_id FROM authored_orders"`
	}

	compiled, err := transcribe.NewCompiler().Compile(ctx, &transcribe.Source{
		Name: "Orders",
		Text: `#setting($_ = $route('/orders', 'GET'))
#define($_ = $TenantID<int>(query/tenantID).Required())
#set($rootTenant = $TenantID)
SELECT o.*
FROM authored_orders o
JOIN (
  SELECT id, order_id, tenant_id
  FROM authored_items items
  WHERE tenant_id = $TenantID
) items ON items.order_id = o.id
WHERE o.tenant_id = $rootTenant`,
	})
	if err != nil {
		t.Fatalf("transcribe failed: %v", err)
	}
	artifact, err := buildArtifact(bootstrap.ArtifactInput{
		Component: compiled.Component, InputType: reflect.TypeOf(input{}), OutputType: reflect.TypeOf(output{}), DirectViewField: "Data",
	})
	if err != nil {
		t.Fatalf("artifact build failed: %v", err)
	}
	if artifact.Reader.Root.Template == nil || len(artifact.Reader.Root.Relations) != 1 || artifact.Reader.Root.Relations[0].Target.Template == nil {
		t.Fatal("expected root and relation SQL templates")
	}
	rootQuery, err := artifact.Reader.Root.Template.Evaluate(ctx, sqltemplate.Invocation{
		Input: reflect.ValueOf(input{TenantID: 2}),
	})
	if err != nil {
		t.Fatalf("evaluate root SQL: %v", err)
	}
	childPlan := artifact.Reader.Root.Relations[0].Target
	childQuery, err := childPlan.Template.Evaluate(ctx, sqltemplate.Invocation{
		Input: reflect.ValueOf(input{TenantID: 2}),
	})
	if err != nil {
		t.Fatalf("evaluate relation SQL: %v", err)
	}
	for name, query := range map[string]sqltemplate.Result{"root": rootQuery, "relation": childQuery} {
		if strings.Contains(query.SQL, "$TenantID") || strings.Contains(query.SQL, "$rootTenant") || strings.Contains(query.SQL, "tenant_id = 2") {
			t.Fatalf("%s SQL interpolated an input value: %q", name, query.SQL)
		}
		if !strings.Contains(query.SQL, "tenant_id = ?") && !strings.Contains(query.SQL, "tenant_id = :TenantID") {
			t.Fatalf("%s SQL did not bind an emitted input: SQL=%q args=%#v", name, query.SQL, query.Args)
		}
	}
	actual, err := NewService().Read(ctx, &Session{
		Component: compiled.Component, OutputType: reflect.TypeOf(output{}),
		Input: routeInput(t, artifact), Artifact: artifact.Reader, SQL: &rsql.SQLComponent{DB: h.DB},
		Scope: testharness.Request{}.WithQuery(url.Values{"tenantID": {"2"}}),
	})
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	assertly.AssertValues(t, &output{Data: []*order{{
		ID: 2, TenantID: 2, Items: []*item{
			{ID: 20, OrderID: 2, TenantID: 2},
			{ID: 21, OrderID: 2, TenantID: 2},
		},
	}}}, actual)
}

func TestReaderHandler_UsesScopedQueryDataAcrossNamedConnectors(t *testing.T) {
	ctx := context.Background()
	rootDB := testharness.NewSQLiteHarness(t)
	childDB := testharness.NewSQLiteHarness(t)
	grandchildDB := testharness.NewSQLiteHarness(t)
	if err := rootDB.ExecStatements(ctx,
		`CREATE TABLE scoped_users (id INTEGER PRIMARY KEY, tenant_id INTEGER, name TEXT);`,
		`INSERT INTO scoped_users(id, tenant_id, name) VALUES (1, 1, 'one'), (2, 2, 'two');`,
	); err != nil {
		t.Fatalf("root setup failed: %v", err)
	}
	if err := childDB.ExecStatements(ctx,
		`CREATE TABLE scoped_profiles (id INTEGER PRIMARY KEY, user_id INTEGER, tenant_id INTEGER, title TEXT);`,
		`INSERT INTO scoped_profiles(id, user_id, tenant_id, title) VALUES (10, 1, 1, 'admin'), (20, 2, 2, 'viewer');`,
	); err != nil {
		t.Fatalf("child setup failed: %v", err)
	}
	if err := grandchildDB.ExecStatements(ctx,
		`CREATE TABLE scoped_settings (id INTEGER PRIMARY KEY, profile_id INTEGER, tenant_id INTEGER, theme TEXT);`,
		`INSERT INTO scoped_settings(id, profile_id, tenant_id, theme) VALUES (100, 10, 1, 'dark'), (200, 20, 2, 'light');`,
	); err != nil {
		t.Fatalf("grandchild setup failed: %v", err)
	}

	type input struct{}
	type setting struct {
		ID        int
		ProfileID int
		Theme     string
	}
	type profile struct {
		ID      int
		UserID  int
		Title   string
		Setting *setting `view:"setting,connector=grandchild" sql:"SELECT id, profile_id, theme FROM scoped_settings WHERE tenant_id = $Tenant $View.ParentJoinOn(\"AND\",\"profile_id\")" on:"ID:id=ProfileID:profile_id"`
	}
	type user struct {
		ID      int
		Name    string
		Profile *profile `view:"profile,connector=child" sql:"SELECT id, user_id, title FROM scoped_profiles WHERE tenant_id = $Tenant $View.ParentJoinOn(\"AND\",\"user_id\")" on:"ID:id=UserID:user_id"`
	}
	type output struct {
		Data []*user
	}
	required := true
	component := &spec.Component{
		Name: "ScopedUsers",
		RootView: &spec.View{Source: &spec.ViewSource{
			SQL:      `SELECT id, name FROM scoped_users WHERE tenant_id = $Tenant ORDER BY id`,
			Bindings: &spec.ViewBindings{Connector: "root"},
		}},
		Parameters: []*spec.Parameter{
			{Name: "Tenant", Source: spec.BindSource{Kind: "context", Name: "tenant"}, TypeExpr: "int", Required: &required},
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
	}
	artifact, err := buildArtifact(bootstrap.ArtifactInput{
		Component: component, InputType: reflect.TypeOf(input{}), OutputType: reflect.TypeOf(output{}), DirectViewField: "Data",
	})
	if err != nil {
		t.Fatalf("artifact build failed: %v", err)
	}
	childPlan := artifact.Reader.Root.Relations[0].Target
	if artifact.Reader.Root.Template == nil || childPlan == nil || childPlan.Template == nil || len(childPlan.Relations) != 1 || childPlan.Relations[0].Target.Template == nil {
		t.Fatal("expected root, child, and grandchild SQL templates")
	}
	sqlComponent := &rsql.SQLComponent{}
	for name, db := range map[string]*testharness.Harness{"root": rootDB, "child": childDB, "grandchild": grandchildDB} {
		if err := sqlComponent.RegisterConnector(name, db.DB); err != nil {
			t.Fatalf("register %s connector: %v", name, err)
		}
	}
	session := &Session{
		Component: component, OutputType: reflect.TypeOf(output{}),
		Input: routeInput(t, artifact), Artifact: artifact.Reader, SQL: sqlComponent,
	}
	providerFor := func(tenant int) locator.Provider {
		return handlerprovider.Static(xhandler.ValueKey("Tenant"), tenant)
	}
	for _, testCase := range []struct {
		name   string
		tenant int
		want   *output
	}{
		{
			name:   "tenant one",
			tenant: 1,
			want: &output{Data: []*user{{
				ID: 1, Name: "one",
				Profile: &profile{ID: 10, UserID: 1, Title: "admin", Setting: &setting{ID: 100, ProfileID: 10, Theme: "dark"}},
			}}},
		},
		{
			name:   "tenant two",
			tenant: 2,
			want: &output{Data: []*user{{
				ID: 2, Name: "two",
				Profile: &profile{ID: 20, UserID: 2, Title: "viewer", Setting: &setting{ID: 200, ProfileID: 20, Theme: "light"}},
			}}},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			execution, err := NewService().execution(session)
			if err != nil {
				t.Fatalf("reader registration failed: %v", err)
			}
			actual, err := handlerengine.New().Execute(ctx, handlerengine.Request{
				Input:     routeInput(t, artifact),
				Providers: []locator.Provider{providerFor(testCase.tenant)},
				Handler:   NewHandler(execution, routeInput(t, artifact)),
			})
			if err != nil {
				t.Fatalf("reader execution failed: %v", err)
			}
			assertly.AssertValues(t, testCase.want, actual)
		})
	}
}

func TestReaderHandler_UsesOutputRelationConnectorAndScopedQueryData(t *testing.T) {
	ctx := context.Background()
	rootDB := testharness.NewSQLiteHarness(t)
	outputDB := testharness.NewSQLiteHarness(t)
	if err := rootDB.ExecStatements(ctx,
		`CREATE TABLE output_users (id INTEGER PRIMARY KEY, tenant_id INTEGER);`,
		`INSERT INTO output_users(id, tenant_id) VALUES (1, 1), (2, 2);`,
	); err != nil {
		t.Fatalf("root setup failed: %v", err)
	}
	if err := outputDB.ExecStatements(ctx,
		`CREATE TABLE output_totals (tenant_id INTEGER PRIMARY KEY, total INTEGER);`,
		`INSERT INTO output_totals(tenant_id, total) VALUES (1, 11), (2, 22);`,
	); err != nil {
		t.Fatalf("output setup failed: %v", err)
	}
	type input struct{}
	type row struct{ ID int }
	type totals struct{ Total int }
	type output struct {
		Data   []*row
		Totals totals
	}
	required := true
	component := &spec.Component{
		Name: "OutputConnector",
		RootView: &spec.View{
			Name: "Users",
			Source: &spec.ViewSource{
				SQL:      `SELECT id FROM output_users WHERE tenant_id = $Tenant`,
				Bindings: &spec.ViewBindings{Connector: "root"},
			},
			Relations: []*spec.Relation{outputRelation("Totals", `SELECT total FROM output_totals WHERE tenant_id = $Tenant`)},
		},
		Parameters: []*spec.Parameter{
			{Name: "Tenant", Source: spec.BindSource{Kind: "context", Name: "tenant"}, TypeExpr: "int", Required: &required},
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
			{Name: "Totals", Source: spec.BindSource{Kind: "output", Name: "summary"}},
		},
	}
	artifact, err := buildArtifact(bootstrap.ArtifactInput{
		Component: component, InputType: reflect.TypeOf(input{}), OutputType: reflect.TypeOf(output{}), DirectViewField: "Data",
	})
	if err != nil {
		t.Fatalf("artifact build failed: %v", err)
	}
	if len(artifact.Reader.Root.View.Relations) != 1 || artifact.Reader.Root.View.Relations[0].Of == nil || artifact.Reader.Root.View.Relations[0].Of.View == nil {
		t.Fatal("expected output relation view")
	}
	artifact.Reader.Root.Relations[0].Target.Connector = "output"
	sqlComponent := &rsql.SQLComponent{}
	if err := sqlComponent.RegisterConnector("root", rootDB.DB); err != nil {
		t.Fatalf("register root connector: %v", err)
	}
	if err := sqlComponent.RegisterConnector("output", outputDB.DB); err != nil {
		t.Fatalf("register output connector: %v", err)
	}
	session := &Session{
		Component: component, OutputType: reflect.TypeOf(output{}),
		Input: routeInput(t, artifact), Artifact: artifact.Reader, SQL: sqlComponent,
		Providers: []locator.Provider{handlerprovider.Static(xhandler.ValueKey("Tenant"), 2)},
	}
	actual, err := NewService().Read(ctx, session)
	if err != nil {
		t.Fatalf("reader execution failed: %v", err)
	}
	assertly.AssertValues(t, &output{Data: []*row{{ID: 2}}, Totals: totals{Total: 22}}, actual)
}

func TestService_ReadBound_ExecutesNativePredicateAndLoopBindings(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE predicate_users (id INTEGER PRIMARY KEY, name TEXT);`,
		`INSERT INTO predicate_users(id, name) VALUES (1, 'one'), (2, 'two'), (3, 'three');`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	type inputHas struct {
		Search bool
		IDs    bool
	}
	type input struct {
		Search string
		IDs    []int
		Has    *inputHas `setMarker:"true"`
	}
	type row struct {
		ID   int    `sqlx:"id"`
		Name string `sqlx:"name"`
	}
	type output struct {
		Data []*row
	}
	component := &spec.Component{
		Name: "PredicateUsers",
		RootView: &spec.View{Source: &spec.ViewSource{SQL: `SELECT id, name FROM predicate_users u WHERE 1=1
${predicate.Builder().CombineAnd($predicate.FilterGroup(0, "AND")).Build("AND")}
AND id IN (#foreach($id in $IDs)$id#if($foreach.HasNext),#end#end)
ORDER BY id`}},
		Parameters: []*spec.Parameter{
			{Name: "Search", Source: spec.BindSource{Kind: "query", Name: "search"}, Predicates: []*spec.Predicate{{Name: "contains", Args: []string{"u", "name"}}}},
			{Name: "IDs", Source: spec.BindSource{Kind: "query", Name: "ids"}},
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
	}
	artifact, err := buildArtifact(bootstrap.ArtifactInput{
		Component: component, InputType: reflect.TypeOf(input{}), OutputType: reflect.TypeOf(output{}), DirectViewField: "Data",
	})
	if err != nil {
		t.Fatalf("artifact build failed: %v", err)
	}
	if artifact.Reader.Root.Template == nil {
		t.Fatal("expected compiled predicate and SQL programs")
	}
	session := &Session{
		Component: component, OutputType: reflect.TypeOf(output{}),
		Input: routeInput(t, artifact), Artifact: artifact.Reader, SQL: &rsql.SQLComponent{DB: h.DB},
	}
	for _, testCase := range []struct {
		name  string
		input input
		want  *output
	}{
		{name: "present predicate", input: input{Search: "o", IDs: []int{1, 3}, Has: &inputHas{Search: true, IDs: true}}, want: &output{Data: []*row{{ID: 1, Name: "one"}}}},
		{name: "absent predicate", input: input{Search: "three", IDs: []int{1, 3}, Has: &inputHas{IDs: true}}, want: &output{Data: []*row{{ID: 1, Name: "one"}, {ID: 3, Name: "three"}}}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			actual, err := NewService().ReadBound(context.Background(), session, &testCase.input)
			if err != nil {
				t.Fatalf("read failed: %v", err)
			}
			assertly.AssertValues(t, testCase.want, actual)
		})
	}
}

func TestService_ReadBound_BindsLoopLocalsAcrossNestedViews(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE loop_users (id INTEGER PRIMARY KEY, name TEXT);`,
		`CREATE TABLE loop_profiles (id INTEGER PRIMARY KEY, user_id INTEGER, title TEXT);`,
		`CREATE TABLE loop_settings (id INTEGER PRIMARY KEY, profile_id INTEGER, theme TEXT);`,
		`INSERT INTO loop_users(id, name) VALUES (1, 'one'), (2, 'two');`,
		`INSERT INTO loop_profiles(id, user_id, title) VALUES (10, 1, 'admin'), (20, 2, 'viewer');`,
		`INSERT INTO loop_settings(id, profile_id, theme) VALUES (100, 10, 'dark'), (200, 20, 'light');`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	type input struct {
		UserIDs []int
		Titles  []string
		Themes  []string
	}
	type settings struct {
		ID        int
		ProfileID int
		Theme     string
	}
	type profile struct {
		ID       int
		UserID   int
		Title    string
		Settings *settings `view:"settings" sql:"SELECT id, profile_id, theme FROM loop_settings WHERE 1=1 $View.ParentJoinOn(\"AND\",\"profile_id\") AND theme IN (#foreach($theme in $Themes)$theme#if($foreach.HasNext),#end#end)" on:"ID:id=ProfileID:profile_id"`
	}
	type row struct {
		ID      int
		Name    string
		Profile *profile `view:"profile" sql:"SELECT id, user_id, title FROM loop_profiles WHERE 1=1 $View.ParentJoinOn(\"AND\",\"user_id\") AND title IN (#foreach($title in $Titles)$title#if($foreach.HasNext),#end#end)" on:"ID:id=UserID:user_id"`
	}
	type output struct {
		Data []*row
	}
	component := &spec.Component{
		Name:     "LoopUsers",
		RootView: &spec.View{Source: &spec.ViewSource{SQL: `SELECT id, name FROM loop_users WHERE id IN (#foreach($id in $UserIDs)$id#if($foreach.HasNext),#end#end) ORDER BY id`}},
		Parameters:   []*spec.Parameter{{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}}},
	}
	artifact, err := buildArtifact(bootstrap.ArtifactInput{
		Component: component, InputType: reflect.TypeOf(input{}), OutputType: reflect.TypeOf(output{}), DirectViewField: "Data",
	})
	if err != nil {
		t.Fatalf("artifact build failed: %v", err)
	}
	profilePlan := artifact.Reader.Root.Relations[0].Target
	settingsPlan := profilePlan.Relations[0].Target
	if artifact.Reader.Root.Template == nil || profilePlan.Template == nil || settingsPlan.Template == nil {
		t.Fatal("expected root, child, and grandchild SQL programs")
	}
	session := &Session{
		Component: component, OutputType: reflect.TypeOf(output{}),
		Input: routeInput(t, artifact), Artifact: artifact.Reader, SQL: &rsql.SQLComponent{DB: h.DB},
	}
	actual, err := NewService().ReadBound(context.Background(), session, &input{
		UserIDs: []int{1, 2}, Titles: []string{"admin"}, Themes: []string{"dark"},
	})
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	assertly.AssertValues(t, &output{Data: []*row{
		{ID: 1, Name: "one", Profile: &profile{ID: 10, UserID: 1, Title: "admin", Settings: &settings{ID: 100, ProfileID: 10, Theme: "dark"}}},
		{ID: 2, Name: "two"},
	}}, actual)
}

func TestService_Read_ExecutesCompiledSQLTemplateForRootAndOutputRelation(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`,
		`INSERT INTO users(id, name) VALUES (1, 'one'), (2, 'two'), (3, 'three');`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	type input struct {
		MinID int
		MaxID int
	}
	type row struct {
		ID   int    `sqlx:"id"`
		Name string `sqlx:"name"`
	}
	type totals struct {
		Count int `sqlx:"count"`
	}
	type output struct {
		Data   []*row
		Totals totals
	}
	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo", Name: "Users"},
		Name: "Users",
		RootView: &spec.View{
			Key:  spec.Key{Kind: spec.KindView, Scope: "example.com/demo", Name: "Users"},
			Name: "Users",
			Source: &spec.ViewSource{SQL: `SELECT id, name FROM users WHERE 1=1
#set($threshold = $minID)
#if($threshold > 0)
AND id >= $threshold
#end
ORDER BY id`},
			Relations: []*spec.Relation{outputRelation("Totals", `SELECT COUNT(*) AS count FROM ($View.Users.NonWindowSQL) parent
#if($maxID > 0)
WHERE parent.id <= $maxID
#end`)},
		},
		Parameters: []*spec.Parameter{
			{Name: "minID", Source: spec.BindSource{Kind: "query", Name: "min_id"}},
			{Name: "maxID", Source: spec.BindSource{Kind: "query", Name: "max_id"}},
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
			{Name: "Totals", Source: spec.BindSource{Kind: "output", Name: "summary"}},
		},
	}
	artifact, err := buildArtifact(bootstrap.ArtifactInput{
		Component:       component,
		InputType:       reflect.TypeOf(input{}),
		OutputType:      reflect.TypeOf(output{}),
		DirectViewField: "Data",
	})
	if err != nil {
		t.Fatalf("artifact build failed: %v", err)
	}
	if artifact.Reader.Root.Template == nil {
		t.Fatal("expected compiled root SQL template")
	}
	if len(artifact.Reader.Root.Relations) != 1 || artifact.Reader.Root.Relations[0].Target == nil || artifact.Reader.Root.Relations[0].Target.Template == nil {
		t.Fatal("expected compiled output relation SQL template")
	}
	for _, testCase := range []struct {
		name  string
		minID string
		maxID string
		want  *output
	}{
		{name: "root condition false", minID: "0", maxID: "2", want: &output{Data: []*row{{ID: 1, Name: "one"}}, Totals: totals{Count: 2}}},
		{name: "root and output conditions true", minID: "2", maxID: "2", want: &output{Data: []*row{{ID: 2, Name: "two"}}, Totals: totals{Count: 1}}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			session := &Session{
				Component:  component,
				OutputType: reflect.TypeOf(output{}),
				Input:      routeInput(t, artifact), Artifact: artifact.Reader,
				SQL: &rsql.SQLComponent{DB: h.DB},
				Scope: testharness.Request{}.WithQuery(url.Values{
					"min_id": []string{testCase.minID},
					"max_id": []string{testCase.maxID},
				}),
				Providers: rootSelectors(xstate.Selector{Limit: 1}),
			}
			actual, err := NewService().Read(context.Background(), session)
			if err != nil {
				t.Fatalf("read failed: %v", err)
			}
			assertly.AssertValues(t, testCase.want, actual)
		})
	}
}
