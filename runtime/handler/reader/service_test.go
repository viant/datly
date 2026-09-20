package reader

import (
	"context"
	"encoding/json"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/viant/afs/option"
	"github.com/viant/assertly"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/data"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
	rsql "github.com/viant/datly/sql"
	sqlreader "github.com/viant/datly/sql/reader"
	rcollector "github.com/viant/datly/sql/reader/collector"
	"github.com/viant/sqlx/io/read/cache"
	cacheafs "github.com/viant/sqlx/io/read/cache/afs"
	xstate "github.com/viant/xdatly/state"
)

type nullableCoalescedInput struct{}

type nullableCoalescedRow struct {
	Name string
}

type nullableCoalescedOutput struct {
	Data []*nullableCoalescedRow
}

type nullablePreservedInput struct{}

type nullablePreservedRow struct {
	Name *string
}

type nullablePreservedOutput struct {
	Data []*nullablePreservedRow
}

type nullableWildcardInput struct{}

type nullableWildcardRow struct {
	ID   int
	Name string
}

type nullableWildcardOutput struct {
	Data []*nullableWildcardRow
}

func TestService_Read_BindsInputExecutesSQLAndShapesOutput(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`,
		`INSERT INTO users(id, name) VALUES (7, 'john')`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/users", Name: "Users"},
		Name: "Users",
		RootView: &spec.View{
			Source: &spec.ViewSource{
				SQL: "SELECT id, name FROM users WHERE id = :ID",
			},
		},
		Parameters: []*spec.Parameter{
			{Name: "ID", Source: spec.BindSource{Kind: "path", Name: "id"}},
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
	}

	type input struct {
		ID int
	}
	type row struct {
		ID   int
		Name string
	}
	type output struct {
		Data []*row
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
	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact),
		Artifact:   artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
		Scope:      testharness.Request{}.WithPathParams(map[string]string{"id": "7"}).WithQuery(url.Values{}),
	}

	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	assertly.AssertValues(t, &output{
		Data: []*row{{ID: 7, Name: "john"}},
	}, actual)
}

func TestService_Read_CoalescesNullableScalarByDefault_SQLite(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE nullable_users (id INTEGER PRIMARY KEY, name TEXT);`,
		`INSERT INTO nullable_users(id, name) VALUES (1, NULL), (2, 'john')`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	component := &spec.Component{
		RootView: &spec.View{
			Source:  &spec.ViewSource{SQL: "SELECT name FROM nullable_users ORDER BY id"},
			Columns: []*spec.Column{{Name: "Name", Source: "name", Type: spec.TypeRef{Name: "string"}, Nullable: true}},
		},
		Parameters: []*spec.Parameter{{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}}},
	}
	artifact, err := buildArtifact(bootstrap.ArtifactInput{
		Component: component, InputType: reflect.TypeOf(nullableCoalescedInput{}), OutputType: reflect.TypeOf(nullableCoalescedOutput{}), DirectViewField: "Data",
	})
	if err != nil {
		t.Fatalf("artifact build failed: %v", err)
	}
	actual, err := NewService().Read(context.Background(), &Session{
		Component: component, OutputType: reflect.TypeOf(nullableCoalescedOutput{}),
		Input: routeInput(t, artifact), Artifact: artifact.Reader, SQL: &rsql.SQLComponent{DB: h.DB},
		Scope: testharness.Request{}.WithQuery(url.Values{}),
	})
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	assertly.AssertValues(t, &nullableCoalescedOutput{Data: []*nullableCoalescedRow{{Name: ""}, {Name: "john"}}}, actual)
}

func TestService_Read_AllowNullsPreservesNullablePointer_SQLite(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE nullable_users (id INTEGER PRIMARY KEY, name TEXT);`,
		`INSERT INTO nullable_users(id, name) VALUES (1, NULL), (2, 'john')`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	allowNulls := true
	component := &spec.Component{
		RootView: &spec.View{
			AllowNulls: &allowNulls,
			Source:     &spec.ViewSource{SQL: "SELECT name FROM nullable_users ORDER BY id"},
			Columns:    []*spec.Column{{Name: "Name", Source: "name", Type: spec.TypeRef{Name: "string"}, Nullable: true}},
		},
		Parameters: []*spec.Parameter{{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}}},
	}
	artifact, err := buildArtifact(bootstrap.ArtifactInput{
		Component: component, InputType: reflect.TypeOf(nullablePreservedInput{}), OutputType: reflect.TypeOf(nullablePreservedOutput{}), DirectViewField: "Data",
	})
	if err != nil {
		t.Fatalf("artifact build failed: %v", err)
	}
	actual, err := NewService().Read(context.Background(), &Session{
		Component: component, OutputType: reflect.TypeOf(nullablePreservedOutput{}),
		Input: routeInput(t, artifact), Artifact: artifact.Reader, SQL: &rsql.SQLComponent{DB: h.DB},
		Scope: testharness.Request{}.WithQuery(url.Values{}),
	})
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	name := "john"
	assertly.AssertValues(t, &nullablePreservedOutput{Data: []*nullablePreservedRow{{Name: nil}, {Name: &name}}}, actual)
}

func TestService_Read_CoalescesNullableScalarFromWildcard_SQLite(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE nullable_users (id INTEGER PRIMARY KEY, name TEXT);`,
		`INSERT INTO nullable_users(id, name) VALUES (1, NULL), (2, 'john')`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	component := &spec.Component{
		RootView: &spec.View{
			Source: &spec.ViewSource{SQL: "SELECT * FROM nullable_users ORDER BY id"},
			Columns: []*spec.Column{
				{Name: "ID", Source: "id", Type: spec.TypeRef{Name: "int"}},
				{Name: "Name", Source: "name", Type: spec.TypeRef{Name: "string"}, Nullable: true},
			},
		},
		Parameters: []*spec.Parameter{{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}}},
	}
	artifact, err := buildArtifact(bootstrap.ArtifactInput{
		Component: component, InputType: reflect.TypeOf(nullableWildcardInput{}), OutputType: reflect.TypeOf(nullableWildcardOutput{}), DirectViewField: "Data",
	})
	if err != nil {
		t.Fatalf("artifact build failed: %v", err)
	}
	actual, err := NewService().Read(context.Background(), &Session{
		Component: component, OutputType: reflect.TypeOf(nullableWildcardOutput{}),
		Input: routeInput(t, artifact), Artifact: artifact.Reader, SQL: &rsql.SQLComponent{DB: h.DB},
		Scope: testharness.Request{}.WithQuery(url.Values{}),
	})
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	assertly.AssertValues(t, &nullableWildcardOutput{Data: []*nullableWildcardRow{{ID: 1, Name: ""}, {ID: 2, Name: "john"}}}, actual)
}

func TestService_Read_UsesSQLXReadCache(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	if err := h.ExecStatements(ctx,
		`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`,
		`INSERT INTO users(id, name) VALUES (7, 'john')`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	component := &spec.Component{
		Key:      spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/cache", Name: "Users"},
		Name:     "Users",
		RootView: &spec.View{Source: &spec.ViewSource{SQL: "SELECT id, name FROM users WHERE id = :ID"}},
		Parameters: []*spec.Parameter{
			{Name: "ID", Source: spec.BindSource{Kind: "path", Name: "id"}},
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
	}
	type input struct {
		ID int
	}
	type row struct {
		ID   int
		Name string
	}
	type output struct {
		Data []*row
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
	readCache, err := cacheafs.NewCache("mem://localhost/datly-reader-sqlx-cache/", time.Hour, t.Name(), option.NewStream(0, 0))
	if err != nil {
		t.Fatalf("create sqlx cache failed: %v", err)
	}
	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact),
		Artifact:   artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
		Scope:      testharness.Request{}.WithPathParams(map[string]string{"id": "7"}).WithQuery(url.Values{}),
		ReadCaches: map[*data.View]cache.Cache{artifact.Reader.Root.View: readCache},
	}

	service := NewService()
	first, err := service.Read(ctx, session)
	if err != nil {
		t.Fatalf("initial read failed: %v", err)
	}
	assertly.AssertValues(t, &output{Data: []*row{{ID: 7, Name: "john"}}}, first)
	if err := h.ExecStatements(ctx, `DELETE FROM users WHERE id = 7`); err != nil {
		t.Fatalf("delete source row failed: %v", err)
	}
	second, err := service.Read(ctx, session)
	if err != nil {
		t.Fatalf("cached read failed: %v", err)
	}
	assertly.AssertValues(t, first, second)
}

func TestService_Read_ErrorsOnInvalidSession(t *testing.T) {
	_, err := NewService().Read(context.Background(), &Session{})
	if err == nil {
		t.Fatalf("expected invalid session to fail")
	}
}

func TestService_Read_ErrorsOnInvalidRequestBody(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/users", Name: "Users"},
		Name: "Users",
		RootView: &spec.View{
			Source: &spec.ViewSource{
				SQL: "SELECT 1",
			},
		},
		Parameters: []*spec.Parameter{
			{Name: "Payload", Source: spec.BindSource{Kind: "body"}},
		},
	}

	type input struct {
		Payload map[string]any
	}

	artifact, err := buildArtifact(bootstrap.ArtifactInput{
		Component: component,
		InputType: reflect.TypeOf(input{}),
	})
	if err != nil {
		t.Fatalf("artifact build failed: %v", err)
	}

	session := &Session{
		Component: component,
		Input:     routeInput(t, artifact),
		Artifact:  artifact.Reader,
		SQL:       &rsql.SQLComponent{DB: h.DB},
		Scope:     testharness.Request{}.WithBody([]byte(`{"foo":1}`), "text/plain"),
	}

	if _, err := NewService().Read(context.Background(), session); err == nil {
		t.Fatalf("expected invalid request body to fail")
	}
}

func TestService_Read_NoViewSlotReturnsTypedOutput(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/users", Name: "Users"},
		Name: "Users",
		RootView: &spec.View{
			Source: &spec.ViewSource{SQL: "SELECT 1"},
		},
	}

	type input struct{}
	type output struct {
		Status string
	}

	artifact, err := buildArtifact(bootstrap.ArtifactInput{
		Component:  component,
		InputType:  reflect.TypeOf(input{}),
		OutputType: reflect.TypeOf(output{}),
	})
	if err != nil {
		t.Fatalf("artifact build failed: %v", err)
	}

	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact),
		Artifact:   artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
		Scope:      testharness.Request{}.WithQuery(url.Values{}),
	}

	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	assertly.AssertValues(t, &output{Status: "ok"}, actual)
}

func TestService_Read_ReturnsNilWhenOutputTypeIsNil(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`,
		`INSERT INTO users(id, name) VALUES (7, 'john')`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/users", Name: "Users"},
		Name: "Users",
		RootView: &spec.View{
			Source: &spec.ViewSource{
				SQL: "SELECT id, name FROM users",
			},
		},
	}
	inputArtifact, err := buildArtifact(bootstrap.ArtifactInput{
		Component: component, InputType: reflect.TypeOf(struct{}{}),
	})
	if err != nil {
		t.Fatalf("input artifact build failed: %v", err)
	}

	session := &Session{
		Component: component,
		Input:     routeInput(t, inputArtifact),
		Artifact:  minimalPlan(component),
		SQL:       &rsql.SQLComponent{DB: h.DB},
		Scope:     testharness.Request{}.WithQuery(url.Values{}),
	}

	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	if actual != nil {
		t.Fatalf("expected nil output, got %+v", actual)
	}
}

func TestService_Read_SupportsPointerOutputType(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`,
		`INSERT INTO users(id, name) VALUES (7, 'john')`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/users", Name: "Users"},
		Name: "Users",
		RootView: &spec.View{
			Source: &spec.ViewSource{
				SQL: "SELECT id, name FROM users WHERE id = :ID",
			},
		},
		Parameters: []*spec.Parameter{
			{Name: "ID", Source: spec.BindSource{Kind: "path", Name: "id"}},
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
	}

	type input struct{ ID int }
	type row struct {
		ID   int
		Name string
	}
	type output struct {
		Data []*row
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
	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(&output{}),
		Input:      routeInput(t, artifact),
		Artifact:   artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
		Scope:      testharness.Request{}.WithPathParams(map[string]string{"id": "7"}).WithQuery(url.Values{}),
	}

	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	assertly.AssertValues(t, &output{
		Data: []*row{{ID: 7, Name: "john"}},
	}, actual)
}

func TestService_Read_IgnoresUnmappedSelectedColumnsLikeOriginalDatly(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`,
		`INSERT INTO users(id, name) VALUES (7, 'john')`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/users", Name: "Users"},
		Name: "Users",
		RootView: &spec.View{
			Source: &spec.ViewSource{
				SQL: "SELECT id, name, 'ignored' AS extra_column FROM users WHERE id = :ID",
			},
		},
		Parameters: []*spec.Parameter{
			{Name: "ID", Source: spec.BindSource{Kind: "path", Name: "id"}},
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
	}

	type input struct {
		ID int
	}
	type row struct {
		ID   int
		Name string
	}
	type output struct {
		Data []*row
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

	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact),
		Artifact:   artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
		Scope:      testharness.Request{}.WithPathParams(map[string]string{"id": "7"}).WithQuery(url.Values{}),
	}

	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	assertly.AssertValues(t, &output{
		Data: []*row{{ID: 7, Name: "john"}},
	}, actual)
}

func TestService_Read_SelectsSQLAliasWithoutOutputFieldAndUsesUnmappedHandler(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`,
		`INSERT INTO users(id, name) VALUES (7, 'john')`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/users", Name: "Users"},
		Name: "Users",
		RootView: &spec.View{
			Source: &spec.ViewSource{
				SQL: "SELECT id, name, 'ignored' AS extra_column FROM users WHERE id = :ID",
			},
		},
		Parameters: []*spec.Parameter{
			{Name: "ID", Source: spec.BindSource{Kind: "path", Name: "id"}},
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
	}

	type input struct {
		ID int
	}
	type row struct {
		ID int
	}
	type output struct {
		Data []*row
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

	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact),
		Artifact:   artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
		Scope:      testharness.Request{}.WithPathParams(map[string]string{"id": "7"}).WithQuery(url.Values{}),
		Providers: rootSelectors(xstate.Selector{
			Columns: []string{"id", "extra_column"},
		}),
	}

	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	assertly.AssertValues(t, &output{
		Data: []*row{{ID: 7}},
	}, actual)
}

func TestService_Read_InvokesOnFetchLifecycle(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`,
		`INSERT INTO users(id, name) VALUES (7, 'john')`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/users", Name: "Users"},
		Name: "Users",
		RootView: &spec.View{
			Source: &spec.ViewSource{
				SQL: "SELECT id, name FROM users WHERE id = :ID",
			},
		},
		Parameters: []*spec.Parameter{
			{Name: "ID", Source: spec.BindSource{Kind: "path", Name: "id"}},
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
	}

	type input struct {
		ID int
	}
	type output struct {
		Data []*fetchRow
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

	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact),
		Artifact:   artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
		Scope:      testharness.Request{}.WithPathParams(map[string]string{"id": "7"}).WithQuery(url.Values{}),
	}

	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	typed, ok := actual.(*output)
	if !ok {
		t.Fatalf("unexpected output type %T", actual)
	}
	if len(typed.Data) != 1 || !typed.Data[0].Fetched {
		t.Fatalf("expected OnFetch lifecycle to mark the row, got %+v", typed.Data)
	}
}

func TestService_Read_PropagatesFetchLifecycleError(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`,
		`INSERT INTO users(id, name) VALUES (7, 'john')`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/users", Name: "Users"},
		Name: "Users",
		RootView: &spec.View{
			Source: &spec.ViewSource{
				SQL: "SELECT id, name FROM users WHERE id = :ID",
			},
		},
		Parameters: []*spec.Parameter{
			{Name: "ID", Source: spec.BindSource{Kind: "path", Name: "id"}},
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
	}

	type input struct{ ID int }
	type output struct {
		Data []*failingFetchRow
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

	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact),
		Artifact:   artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
		Scope:      testharness.Request{}.WithPathParams(map[string]string{"id": "7"}).WithQuery(url.Values{}),
	}

	if _, err := NewService().Read(context.Background(), session); err == nil || !strings.Contains(err.Error(), "fetch failed") {
		t.Fatalf("expected fetch lifecycle error, got %v", err)
	}
}

func TestService_Read_BindsTypedSummary(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`,
		`INSERT INTO users(id, name) VALUES (7, 'john')`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/users", Name: "Users"},
		Name: "Users",
		RootView: &spec.View{
			Name: "Users", Source: &spec.ViewSource{SQL: "SELECT id, name FROM users WHERE id = :ID"},
			Relations: []*spec.Relation{outputRelation("Summary", "SELECT COUNT(*) AS total FROM users WHERE id = :ID")},
		},
		Parameters: []*spec.Parameter{
			{Name: "ID", Source: spec.BindSource{Kind: "path", Name: "id"}},
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
			{Name: "Summary", Source: spec.BindSource{Kind: "output", Name: "summary"}},
		},
	}

	type input struct{ ID int }
	type row struct {
		ID   int
		Name string
	}
	type summary struct{ Total int }
	type output struct {
		Data    []*row
		Summary summary
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

	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact),
		Artifact:   artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
		Scope:      testharness.Request{}.WithPathParams(map[string]string{"id": "7"}).WithQuery(url.Values{}),
	}

	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	assertly.AssertValues(t, &output{
		Data:    []*row{{ID: 7, Name: "john"}},
		Summary: summary{Total: 1},
	}, actual)
}

func TestService_Read_BindsPointerSummary(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`,
		`INSERT INTO users(id, name) VALUES (7, 'john')`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/users", Name: "Users"},
		Name: "Users",
		RootView: &spec.View{
			Name: "Users", Source: &spec.ViewSource{SQL: "SELECT id, name FROM users WHERE id = :ID"},
			Relations: []*spec.Relation{outputRelation("Summary", "SELECT COUNT(*) AS total FROM users WHERE id = :ID")},
		},
		Parameters: []*spec.Parameter{
			{Name: "ID", Source: spec.BindSource{Kind: "path", Name: "id"}},
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
			{Name: "Summary", Source: spec.BindSource{Kind: "output", Name: "summary"}},
		},
	}

	type input struct{ ID int }
	type row struct {
		ID   int
		Name string
	}
	type summary struct{ Total int }
	type output struct {
		Data    []*row
		Summary *summary
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

	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact),
		Artifact:   artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
		Scope:      testharness.Request{}.WithPathParams(map[string]string{"id": "7"}).WithQuery(url.Values{}),
	}

	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	assertly.AssertValues(t, &output{
		Data:    []*row{{ID: 7, Name: "john"}},
		Summary: &summary{Total: 1},
	}, actual)
}

func TestService_Read_PointerSummaryNoRowsLeavesNil(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`,
		`INSERT INTO users(id, name) VALUES (7, 'john')`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/users", Name: "Users"},
		Name: "Users",
		RootView: &spec.View{
			Name: "Users", Source: &spec.ViewSource{SQL: "SELECT id, name FROM users WHERE id = :ID"},
			Relations: []*spec.Relation{outputRelation("Summary", "SELECT total FROM (SELECT 1 AS total) t WHERE 1 = 0")},
		},
		Parameters: []*spec.Parameter{
			{Name: "ID", Source: spec.BindSource{Kind: "path", Name: "id"}},
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
			{Name: "Summary", Source: spec.BindSource{Kind: "output", Name: "summary"}},
		},
	}

	type input struct{ ID int }
	type row struct {
		ID   int
		Name string
	}
	type summary struct{ Total int }
	type output struct {
		Data    []*row
		Summary *summary
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

	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact),
		Artifact:   artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
		Scope:      testharness.Request{}.WithPathParams(map[string]string{"id": "7"}).WithQuery(url.Values{}),
	}

	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	assertly.AssertValues(t, &output{
		Data: []*row{{ID: 7, Name: "john"}},
	}, actual)
}

func TestService_Read_TypedSummaryNoRowsLeavesZeroValue(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`,
		`INSERT INTO users(id, name) VALUES (7, 'john')`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/users", Name: "Users"},
		Name: "Users",
		RootView: &spec.View{
			Name: "Users", Source: &spec.ViewSource{SQL: "SELECT id, name FROM users WHERE id = :ID"},
			Relations: []*spec.Relation{outputRelation("Summary", "SELECT total FROM (SELECT 1 AS total) t WHERE 1 = 0")},
		},
		Parameters: []*spec.Parameter{
			{Name: "ID", Source: spec.BindSource{Kind: "path", Name: "id"}},
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
			{Name: "Summary", Source: spec.BindSource{Kind: "output", Name: "summary"}},
		},
	}

	type input struct{ ID int }
	type row struct {
		ID   int
		Name string
	}
	type summary struct{ Total int }
	type output struct {
		Data    []*row
		Summary summary
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

	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact),
		Artifact:   artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
		Scope:      testharness.Request{}.WithPathParams(map[string]string{"id": "7"}).WithQuery(url.Values{}),
	}

	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	assertly.AssertValues(t, &output{
		Data:    []*row{{ID: 7, Name: "john"}},
		Summary: summary{},
	}, actual)
}

func TestService_Read_ErrorsOnInvalidSummarySQL(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`,
		`INSERT INTO users(id, name) VALUES (7, 'john')`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/users", Name: "Users"},
		Name: "Users",
		RootView: &spec.View{
			Name: "Users", Source: &spec.ViewSource{SQL: "SELECT id, name FROM users WHERE id = :ID"},
			Relations: []*spec.Relation{outputRelation("Summary", "SELECT missing FROM missing_table")},
		},
		Parameters: []*spec.Parameter{
			{Name: "ID", Source: spec.BindSource{Kind: "path", Name: "id"}},
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
			{Name: "Summary", Source: spec.BindSource{Kind: "output", Name: "summary"}},
		},
	}

	type input struct{ ID int }
	type row struct {
		ID   int
		Name string
	}
	type summary struct{ Total int }
	type output struct {
		Data    []*row
		Summary summary
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

	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact),
		Artifact:   artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
		Scope:      testharness.Request{}.WithPathParams(map[string]string{"id": "7"}).WithQuery(url.Values{}),
	}

	if _, err := NewService().Read(context.Background(), session); err == nil {
		t.Fatalf("expected invalid summary SQL to fail")
	}
}

func TestService_Read_BindsTypedSummaryWithUnmappedExtraColumn(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`,
		`INSERT INTO users(id, name) VALUES (7, 'john')`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/users", Name: "Users"},
		Name: "Users",
		RootView: &spec.View{
			Name: "Users", Source: &spec.ViewSource{SQL: "SELECT id, name FROM users WHERE id = :ID"},
			Relations: []*spec.Relation{outputRelation("Summary", "SELECT COUNT(*) AS total, 'ignored' AS extra_column FROM users WHERE id = :ID")},
		},
		Parameters: []*spec.Parameter{
			{Name: "ID", Source: spec.BindSource{Kind: "path", Name: "id"}},
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
			{Name: "Summary", Source: spec.BindSource{Kind: "output", Name: "summary"}},
		},
	}

	type input struct{ ID int }
	type row struct {
		ID   int
		Name string
	}
	type summary struct{ Total int }
	type output struct {
		Data    []*row
		Summary summary
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

	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact),
		Artifact:   artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
		Scope:      testharness.Request{}.WithPathParams(map[string]string{"id": "7"}).WithQuery(url.Values{}),
	}

	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	assertly.AssertValues(t, &output{
		Data:    []*row{{ID: 7, Name: "john"}},
		Summary: summary{Total: 1},
	}, actual)
}

func TestService_Read_BindsSummaryFromRootNonWindowSQL(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`,
		`INSERT INTO users(id, name) VALUES (1, 'zeta'), (2, 'alpha'), (3, 'beta')`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/users", Name: "Users"},
		Name: "Users",
		RootView: &spec.View{
			Name: "Users",
			Source: &spec.ViewSource{
				SQL: "SELECT id, name FROM users",
			},
			Relations: []*spec.Relation{outputRelation("Summary", "SELECT COUNT(*) AS total FROM ($View.Users.NonWindowSQL) t")},
		},
		Parameters: []*spec.Parameter{
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
			{Name: "Summary", Source: spec.BindSource{Kind: "output", Name: "summary"}},
		},
	}

	type input struct{}
	type row struct {
		ID   int
		Name string
	}
	type summary struct {
		Total int
	}
	type output struct {
		Data    []*row
		Summary summary
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

	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact),
		Artifact:   artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
		Scope:      testharness.Request{}.WithQuery(url.Values{}),
		Providers: rootSelectors(xstate.Selector{
			OrderBy: "name ASC",
			Limit:   1,
		}),
	}

	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	assertly.AssertValues(t, &output{
		Data: []*row{
			{ID: 2, Name: "alpha"},
		},
		Summary: summary{Total: 3},
	}, actual)
}

func TestService_Read_BindsSummaryFromFilteredNonWindowRootSQL(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`,
		`INSERT INTO users(id, name) VALUES (1, 'zeta'), (2, 'alpha'), (3, 'adam'), (4, 'beta')`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/users", Name: "Users"},
		Name: "Users",
		RootView: &spec.View{
			Name: "Users",
			Source: &spec.ViewSource{
				SQL: "SELECT id, name FROM users",
			},
			Relations: []*spec.Relation{outputRelation("Summary", "SELECT COUNT(*) AS total FROM ($View.Users.NonWindowSQL) t")},
		},
		Parameters: []*spec.Parameter{
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
			{Name: "Summary", Source: spec.BindSource{Kind: "output", Name: "summary"}},
		},
	}

	type input struct{}
	type row struct {
		ID   int
		Name string
	}
	type summary struct {
		Total int
	}
	type output struct {
		Data    []*row
		Summary summary
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

	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact),
		Artifact:   artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
		Scope:      testharness.Request{}.WithQuery(url.Values{}),
		Providers: rootSelectors(xstate.Selector{
			OrderBy:      "name ASC",
			Limit:        1,
			Criteria:     "name LIKE ?",
			Placeholders: []interface{}{"a%"},
		}),
	}

	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	assertly.AssertValues(t, &output{
		Data: []*row{
			{ID: 3, Name: "adam"},
		},
		Summary: summary{Total: 2},
	}, actual)
}

func TestService_Read_SummaryIgnoresSelectorProjection(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`,
		`INSERT INTO users(id, name) VALUES (1, 'zeta'), (2, 'alpha'), (3, 'adam'), (4, 'beta')`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/users", Name: "Users"},
		Name: "Users",
		RootView: &spec.View{
			Name: "Users",
			Source: &spec.ViewSource{
				SQL: "SELECT id, name FROM users",
			},
			Relations: []*spec.Relation{outputRelation("Summary", "SELECT COUNT(*) AS total FROM ($View.Users.NonWindowSQL) t")},
		},
		Parameters: []*spec.Parameter{
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
			{Name: "Summary", Source: spec.BindSource{Kind: "output", Name: "summary"}},
		},
	}

	type input struct{}
	type row struct {
		ID   int
		Name string
	}
	type summary struct {
		Total int
	}
	type output struct {
		Data    []*row
		Summary summary
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

	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact),
		Artifact:   artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
		Scope:      testharness.Request{}.WithQuery(url.Values{}),
		Providers: rootSelectors(xstate.Selector{
			Fields:       []string{"Name"},
			OrderBy:      "name ASC",
			Limit:        1,
			Criteria:     "name LIKE ?",
			Placeholders: []interface{}{"a%"},
		}),
	}

	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	assertly.AssertValues(t, &output{
		Data: []*row{
			{ID: 0, Name: "adam"},
		},
		Summary: summary{Total: 2},
	}, actual)
}

func TestService_Read_BindsOneToManySubview(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`,
		`CREATE TABLE accounts (id INTEGER PRIMARY KEY, user_id INTEGER, name TEXT);`,
		`INSERT INTO users(id, name) VALUES (7, 'john')`,
		`INSERT INTO accounts(id, user_id, name) VALUES (1, 7, 'a1'), (2, 7, 'a2')`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/users", Name: "Users"},
		Name: "Users",
		RootView: &spec.View{
			Source: &spec.ViewSource{SQL: "SELECT id, name FROM users WHERE id = :ID"},
		},
		Parameters: []*spec.Parameter{
			{Name: "ID", Source: spec.BindSource{Kind: "path", Name: "id"}},
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
	}

	type input struct{ ID int }
	type account struct {
		ID     int
		UserID int
		Name   string
	}
	type row struct {
		ID       int
		Name     string
		Accounts []*account `view:"accounts" sql:"SELECT id, user_id, name FROM accounts WHERE user_id IN (?)" on:"ID:id=UserID:user_id"`
	}
	type output struct {
		Data []*row
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

	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact),
		Artifact:   artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
		Scope:      testharness.Request{}.WithPathParams(map[string]string{"id": "7"}).WithQuery(url.Values{}),
	}

	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	assertly.AssertValues(t, &output{
		Data: []*row{{
			ID:   7,
			Name: "john",
			Accounts: []*account{
				{ID: 1, UserID: 7, Name: "a1"},
				{ID: 2, UserID: 7, Name: "a2"},
			},
		}},
	}, actual)
}

func TestService_Read_BindsOneToManySubviewWithUnmappedExtraColumn(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`,
		`CREATE TABLE accounts (id INTEGER PRIMARY KEY, user_id INTEGER, name TEXT);`,
		`INSERT INTO users(id, name) VALUES (7, 'john')`,
		`INSERT INTO accounts(id, user_id, name) VALUES (1, 7, 'a1'), (2, 7, 'a2')`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/users", Name: "Users"},
		Name: "Users",
		RootView: &spec.View{
			Source: &spec.ViewSource{SQL: "SELECT id, name FROM users WHERE id = :ID"},
		},
		Parameters: []*spec.Parameter{
			{Name: "ID", Source: spec.BindSource{Kind: "path", Name: "id"}},
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
	}

	type input struct{ ID int }
	type account struct {
		ID     int
		UserID int
		Name   string
	}
	type row struct {
		ID       int
		Name     string
		Accounts []*account `view:"accounts" sql:"SELECT id, user_id, name, 'ignored' AS extra_column FROM accounts WHERE user_id IN (?)" on:"ID:id=UserID:user_id"`
	}
	type output struct {
		Data []*row
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

	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact),
		Artifact:   artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
		Scope:      testharness.Request{}.WithPathParams(map[string]string{"id": "7"}).WithQuery(url.Values{}),
	}

	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	assertly.AssertValues(t, &output{
		Data: []*row{{
			ID:   7,
			Name: "john",
			Accounts: []*account{
				{ID: 1, UserID: 7, Name: "a1"},
				{ID: 2, UserID: 7, Name: "a2"},
			},
		}},
	}, actual)
}

func TestService_Read_AppendsRelationColumnInWhenSubviewSQLOmitsIt(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`,
		`CREATE TABLE accounts (id INTEGER PRIMARY KEY, user_id INTEGER, name TEXT);`,
		`INSERT INTO users(id, name) VALUES (7, 'john')`,
		`INSERT INTO accounts(id, user_id, name) VALUES (1, 7, 'a1'), (2, 7, 'a2'), (3, 9, 'a3')`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/users", Name: "Users"},
		Name: "Users",
		RootView: &spec.View{
			Source: &spec.ViewSource{SQL: "SELECT id, name FROM users WHERE id = :ID"},
		},
		Parameters: []*spec.Parameter{
			{Name: "ID", Source: spec.BindSource{Kind: "path", Name: "id"}},
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
	}

	type input struct{ ID int }
	type account struct {
		ID     int
		UserID int
		Name   string
	}
	type row struct {
		ID       int
		Name     string
		Accounts []*account `view:"accounts" sql:"SELECT id, user_id, name FROM accounts ORDER BY id" on:"ID:id=UserID:user_id"`
	}
	type output struct {
		Data []*row
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

	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact),
		Artifact:   artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
		Scope:      testharness.Request{}.WithPathParams(map[string]string{"id": "7"}).WithQuery(url.Values{}),
	}

	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	assertly.AssertValues(t, &output{
		Data: []*row{{
			ID:   7,
			Name: "john",
			Accounts: []*account{
				{ID: 1, UserID: 7, Name: "a1"},
				{ID: 2, UserID: 7, Name: "a2"},
			},
		}},
	}, actual)
}

func TestService_Read_ReplacesSubviewWhereCriteriaToken(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`,
		`CREATE TABLE accounts (id INTEGER PRIMARY KEY, user_id INTEGER, name TEXT);`,
		`INSERT INTO users(id, name) VALUES (7, 'john')`,
		`INSERT INTO accounts(id, user_id, name) VALUES (1, 7, 'a1'), (2, 7, 'a2'), (3, 9, 'a3')`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/users", Name: "Users"},
		Name: "Users",
		RootView: &spec.View{
			Source: &spec.ViewSource{SQL: "SELECT id, name FROM users WHERE id = :ID"},
		},
		Parameters: []*spec.Parameter{
			{Name: "ID", Source: spec.BindSource{Kind: "path", Name: "id"}},
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
	}

	type input struct{ ID int }
	type account struct {
		ID     int
		UserID int
		Name   string
	}
	type row struct {
		ID       int
		Name     string
		Accounts []*account `view:"accounts" sql:"SELECT id, user_id, name FROM accounts $WHERE_CRITERIA ORDER BY id" on:"ID:id=UserID:user_id"`
	}
	type output struct {
		Data []*row
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

	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact),
		Artifact:   artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
		Scope:      testharness.Request{}.WithPathParams(map[string]string{"id": "7"}).WithQuery(url.Values{}),
	}

	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	assertly.AssertValues(t, &output{
		Data: []*row{{
			ID:   7,
			Name: "john",
			Accounts: []*account{
				{ID: 1, UserID: 7, Name: "a1"},
				{ID: 2, UserID: 7, Name: "a2"},
			},
		}},
	}, actual)
}

func TestService_Read_ReplacesSubviewAndCriteriaToken(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`,
		`CREATE TABLE accounts (id INTEGER PRIMARY KEY, user_id INTEGER, name TEXT);`,
		`INSERT INTO users(id, name) VALUES (7, 'john')`,
		`INSERT INTO accounts(id, user_id, name) VALUES (1, 7, 'a1'), (2, 7, 'a2'), (3, 9, 'a3')`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/users", Name: "Users"},
		Name: "Users",
		RootView: &spec.View{
			Source: &spec.ViewSource{SQL: "SELECT id, name FROM users WHERE id = :ID"},
		},
		Parameters: []*spec.Parameter{
			{Name: "ID", Source: spec.BindSource{Kind: "path", Name: "id"}},
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
	}

	type input struct{ ID int }
	type account struct {
		ID     int
		UserID int
		Name   string
	}
	type row struct {
		ID       int
		Name     string
		Accounts []*account `view:"accounts" sql:"SELECT id, user_id, name FROM accounts WHERE name LIKE 'a%' $AND_CRITERIA ORDER BY id" on:"ID:id=UserID:user_id"`
	}
	type output struct {
		Data []*row
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

	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact),
		Artifact:   artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
		Scope:      testharness.Request{}.WithPathParams(map[string]string{"id": "7"}).WithQuery(url.Values{}),
	}

	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	assertly.AssertValues(t, &output{
		Data: []*row{{
			ID:   7,
			Name: "john",
			Accounts: []*account{
				{ID: 1, UserID: 7, Name: "a1"},
				{ID: 2, UserID: 7, Name: "a2"},
			},
		}},
	}, actual)
}

func TestService_Read_ReplacesSubviewOrCriteriaToken(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`,
		`CREATE TABLE accounts (id INTEGER PRIMARY KEY, user_id INTEGER, name TEXT);`,
		`INSERT INTO users(id, name) VALUES (7, 'john')`,
		`INSERT INTO accounts(id, user_id, name) VALUES (1, 7, 'a1'), (2, 7, 'a2'), (3, 9, 'a3')`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/users", Name: "Users"},
		Name: "Users",
		RootView: &spec.View{
			Source: &spec.ViewSource{SQL: "SELECT id, name FROM users WHERE id = :ID"},
		},
		Parameters: []*spec.Parameter{
			{Name: "ID", Source: spec.BindSource{Kind: "path", Name: "id"}},
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
	}

	type input struct{ ID int }
	type account struct {
		ID     int
		UserID int
		Name   string
	}
	type row struct {
		ID       int
		Name     string
		Accounts []*account `view:"accounts" sql:"SELECT id, user_id, name FROM accounts WHERE 1 = 0 $OR_CRITERIA ORDER BY id" on:"ID:id=UserID:user_id"`
	}
	type output struct {
		Data []*row
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

	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact),
		Artifact:   artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
		Scope:      testharness.Request{}.WithPathParams(map[string]string{"id": "7"}).WithQuery(url.Values{}),
	}

	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	assertly.AssertValues(t, &output{
		Data: []*row{{
			ID:   7,
			Name: "john",
			Accounts: []*account{
				{ID: 1, UserID: 7, Name: "a1"},
				{ID: 2, UserID: 7, Name: "a2"},
			},
		}},
	}, actual)
}

func TestService_Read_ReplacesSubviewColumnInToken(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`,
		`CREATE TABLE accounts (id INTEGER PRIMARY KEY, user_id INTEGER, name TEXT);`,
		`INSERT INTO users(id, name) VALUES (7, 'john')`,
		`INSERT INTO accounts(id, user_id, name) VALUES (1, 7, 'a1'), (2, 7, 'a2'), (3, 9, 'a3')`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/users", Name: "Users"},
		Name: "Users",
		RootView: &spec.View{
			Source: &spec.ViewSource{SQL: "SELECT id, name FROM users WHERE id = :ID"},
		},
		Parameters: []*spec.Parameter{
			{Name: "ID", Source: spec.BindSource{Kind: "path", Name: "id"}},
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
	}

	type input struct{ ID int }
	type account struct {
		ID     int
		UserID int
		Name   string
	}
	type row struct {
		ID       int
		Name     string
		Accounts []*account `view:"accounts" sql:"SELECT id, user_id, name FROM accounts WHERE $COLUMN_IN ORDER BY id" on:"ID:id=UserID:user_id"`
	}
	type output struct {
		Data []*row
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

	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact),
		Artifact:   artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
		Scope:      testharness.Request{}.WithPathParams(map[string]string{"id": "7"}).WithQuery(url.Values{}),
	}

	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	assertly.AssertValues(t, &output{
		Data: []*row{{
			ID:   7,
			Name: "john",
			Accounts: []*account{
				{ID: 1, UserID: 7, Name: "a1"},
				{ID: 2, UserID: 7, Name: "a2"},
			},
		}},
	}, actual)
}

func TestService_Read_ReplacesCompositeSubviewColumnInToken(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE audiences (feature_type TEXT, feature_value TEXT, name TEXT);`,
		`CREATE TABLE signal_performance (feature_type TEXT, value TEXT, metric INTEGER);`,
		`INSERT INTO audiences(feature_type, feature_value, name) VALUES ('country', 'PL', 'Poland'), ('country', 'US', 'USA')`,
		`INSERT INTO signal_performance(feature_type, value, metric) VALUES ('country', 'PL', 10), ('country', 'US', 20), ('country', 'DE', 30)`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/audiences", Name: "Audiences"},
		Name: "Audiences",
		RootView: &spec.View{
			Source: &spec.ViewSource{SQL: "SELECT feature_type, feature_value, name FROM audiences ORDER BY feature_value"},
		},
		Parameters: []*spec.Parameter{
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
	}

	type performance struct {
		FeatureType string `sqlx:"feature_type"`
		Value       string `sqlx:"value"`
		Metric      int    `sqlx:"metric"`
	}
	type row struct {
		FeatureType       string       `sqlx:"feature_type"`
		FeatureValue      string       `sqlx:"feature_value"`
		Name              string       `sqlx:"name"`
		SignalPerformance *performance `view:"signalPerformance" sql:"SELECT feature_type, value, metric FROM signal_performance WHERE $COLUMN_IN ORDER BY value" on:"FeatureType:feature_type=FeatureType:feature_type,FeatureValue:feature_value=Value:value"`
	}
	type output struct {
		Data []*row
	}

	artifact, err := buildArtifact(bootstrap.ArtifactInput{
		Component:       component,
		InputType:       reflect.TypeOf(struct{}{}),
		OutputType:      reflect.TypeOf(output{}),
		DirectViewField: "Data",
	})
	if err != nil {
		t.Fatalf("artifact build failed: %v", err)
	}

	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact),
		Artifact:   artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
		Scope:      testharness.Request{}.WithQuery(url.Values{}),
	}

	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	assertly.AssertValues(t, &output{
		Data: []*row{
			{
				FeatureType:  "country",
				FeatureValue: "PL",
				Name:         "Poland",
				SignalPerformance: &performance{
					FeatureType: "country",
					Value:       "PL",
					Metric:      10,
				},
			},
			{
				FeatureType:  "country",
				FeatureValue: "US",
				Name:         "USA",
				SignalPerformance: &performance{
					FeatureType: "country",
					Value:       "US",
					Metric:      20,
				},
			},
		},
	}, actual)
}

func TestService_Read_ReplacesCompositeSubviewWhereCriteriaToken(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE audiences (feature_type TEXT, feature_value TEXT, name TEXT);`,
		`CREATE TABLE signal_performance (feature_type TEXT, value TEXT, metric INTEGER);`,
		`INSERT INTO audiences(feature_type, feature_value, name) VALUES ('country', 'PL', 'Poland'), ('country', 'US', 'USA')`,
		`INSERT INTO signal_performance(feature_type, value, metric) VALUES ('country', 'PL', 10), ('country', 'US', 20), ('country', 'DE', 30)`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/audiences", Name: "Audiences"},
		Name: "Audiences",
		RootView: &spec.View{
			Source: &spec.ViewSource{SQL: "SELECT feature_type, feature_value, name FROM audiences ORDER BY feature_value"},
		},
		Parameters: []*spec.Parameter{
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
	}

	type performance struct {
		FeatureType string `sqlx:"feature_type"`
		Value       string `sqlx:"value"`
		Metric      int    `sqlx:"metric"`
	}
	type row struct {
		FeatureType       string       `sqlx:"feature_type"`
		FeatureValue      string       `sqlx:"feature_value"`
		Name              string       `sqlx:"name"`
		SignalPerformance *performance `view:"signalPerformance" sql:"SELECT feature_type, value, metric FROM signal_performance $WHERE_CRITERIA ORDER BY value" on:"FeatureType:feature_type=FeatureType:feature_type,FeatureValue:feature_value=Value:value"`
	}
	type output struct {
		Data []*row
	}

	artifact, err := buildArtifact(bootstrap.ArtifactInput{
		Component:       component,
		InputType:       reflect.TypeOf(struct{}{}),
		OutputType:      reflect.TypeOf(output{}),
		DirectViewField: "Data",
	})
	if err != nil {
		t.Fatalf("artifact build failed: %v", err)
	}

	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact),
		Artifact:   artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
		Scope:      testharness.Request{}.WithQuery(url.Values{}),
	}

	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	assertly.AssertValues(t, &output{
		Data: []*row{
			{
				FeatureType:  "country",
				FeatureValue: "PL",
				Name:         "Poland",
				SignalPerformance: &performance{
					FeatureType: "country",
					Value:       "PL",
					Metric:      10,
				},
			},
			{
				FeatureType:  "country",
				FeatureValue: "US",
				Name:         "USA",
				SignalPerformance: &performance{
					FeatureType: "country",
					Value:       "US",
					Metric:      20,
				},
			},
		},
	}, actual)
}

func TestService_Read_ReplacesCompositeSubviewAndCriteriaToken(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE audiences (feature_type TEXT, feature_value TEXT, name TEXT);`,
		`CREATE TABLE signal_performance (feature_type TEXT, value TEXT, metric INTEGER);`,
		`INSERT INTO audiences(feature_type, feature_value, name) VALUES ('country', 'PL', 'Poland'), ('country', 'US', 'USA')`,
		`INSERT INTO signal_performance(feature_type, value, metric) VALUES ('country', 'PL', 10), ('country', 'US', 20), ('country', 'DE', 30)`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/audiences", Name: "Audiences"},
		Name: "Audiences",
		RootView: &spec.View{
			Source: &spec.ViewSource{SQL: "SELECT feature_type, feature_value, name FROM audiences ORDER BY feature_value"},
		},
		Parameters: []*spec.Parameter{
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
	}

	type performance struct {
		FeatureType string `sqlx:"feature_type"`
		Value       string `sqlx:"value"`
		Metric      int    `sqlx:"metric"`
	}
	type row struct {
		FeatureType       string       `sqlx:"feature_type"`
		FeatureValue      string       `sqlx:"feature_value"`
		Name              string       `sqlx:"name"`
		SignalPerformance *performance `view:"signalPerformance" sql:"SELECT feature_type, value, metric FROM signal_performance WHERE metric > 0 $AND_CRITERIA ORDER BY value" on:"FeatureType:feature_type=FeatureType:feature_type,FeatureValue:feature_value=Value:value"`
	}
	type output struct {
		Data []*row
	}

	artifact, err := buildArtifact(bootstrap.ArtifactInput{
		Component:       component,
		InputType:       reflect.TypeOf(struct{}{}),
		OutputType:      reflect.TypeOf(output{}),
		DirectViewField: "Data",
	})
	if err != nil {
		t.Fatalf("artifact build failed: %v", err)
	}

	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact),
		Artifact:   artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
		Scope:      testharness.Request{}.WithQuery(url.Values{}),
	}

	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	assertly.AssertValues(t, &output{
		Data: []*row{
			{
				FeatureType:  "country",
				FeatureValue: "PL",
				Name:         "Poland",
				SignalPerformance: &performance{
					FeatureType: "country",
					Value:       "PL",
					Metric:      10,
				},
			},
			{
				FeatureType:  "country",
				FeatureValue: "US",
				Name:         "USA",
				SignalPerformance: &performance{
					FeatureType: "country",
					Value:       "US",
					Metric:      20,
				},
			},
		},
	}, actual)
}

func TestService_Read_ReplacesCompositeSubviewOrCriteriaToken(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE audiences (feature_type TEXT, feature_value TEXT, name TEXT);`,
		`CREATE TABLE signal_performance (feature_type TEXT, value TEXT, metric INTEGER);`,
		`INSERT INTO audiences(feature_type, feature_value, name) VALUES ('country', 'PL', 'Poland'), ('country', 'US', 'USA')`,
		`INSERT INTO signal_performance(feature_type, value, metric) VALUES ('country', 'PL', 10), ('country', 'US', 20), ('country', 'DE', 30)`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/audiences", Name: "Audiences"},
		Name: "Audiences",
		RootView: &spec.View{
			Source: &spec.ViewSource{SQL: "SELECT feature_type, feature_value, name FROM audiences ORDER BY feature_value"},
		},
		Parameters: []*spec.Parameter{
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
	}

	type performance struct {
		FeatureType string `sqlx:"feature_type"`
		Value       string `sqlx:"value"`
		Metric      int    `sqlx:"metric"`
	}
	type row struct {
		FeatureType       string       `sqlx:"feature_type"`
		FeatureValue      string       `sqlx:"feature_value"`
		Name              string       `sqlx:"name"`
		SignalPerformance *performance `view:"signalPerformance" sql:"SELECT feature_type, value, metric FROM signal_performance WHERE 1 = 0 $OR_CRITERIA ORDER BY value" on:"FeatureType:feature_type=FeatureType:feature_type,FeatureValue:feature_value=Value:value"`
	}
	type output struct {
		Data []*row
	}

	artifact, err := buildArtifact(bootstrap.ArtifactInput{
		Component:       component,
		InputType:       reflect.TypeOf(struct{}{}),
		OutputType:      reflect.TypeOf(output{}),
		DirectViewField: "Data",
	})
	if err != nil {
		t.Fatalf("artifact build failed: %v", err)
	}

	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact),
		Artifact:   artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
		Scope:      testharness.Request{}.WithQuery(url.Values{}),
	}

	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	assertly.AssertValues(t, &output{
		Data: []*row{
			{
				FeatureType:  "country",
				FeatureValue: "PL",
				Name:         "Poland",
				SignalPerformance: &performance{
					FeatureType: "country",
					Value:       "PL",
					Metric:      10,
				},
			},
			{
				FeatureType:  "country",
				FeatureValue: "US",
				Name:         "USA",
				SignalPerformance: &performance{
					FeatureType: "country",
					Value:       "US",
					Metric:      20,
				},
			},
		},
	}, actual)
}

func TestService_Read_AppliesSelectorBindings_SQLite(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`,
		`INSERT INTO users(id, name) VALUES (1, 'zeta'), (2, 'alpha'), (3, 'beta')`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/users", Name: "Users"},
		Name: "Users",
		RootView: &spec.View{
			Source: &spec.ViewSource{
				SQL: "SELECT id, name FROM users",
			},
		},
		Parameters: []*spec.Parameter{
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
	}

	type input struct{}
	type row struct {
		ID   int
		Name string
	}
	type output struct {
		Data []*row
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

	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact),
		Artifact:   artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
		Scope:      testharness.Request{}.WithQuery(url.Values{}),
		Providers: rootSelectors(xstate.Selector{
			OrderBy: "name ASC",
			Limit:   2,
			Offset:  0,
		}),
	}

	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	assertly.AssertValues(t, &output{
		Data: []*row{
			{ID: 2, Name: "alpha"},
			{ID: 3, Name: "beta"},
		},
	}, actual)
}

func TestService_Read_AppliesSelectorPage_SQLite(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`,
		`INSERT INTO users(id, name) VALUES (1, 'zeta'), (2, 'alpha'), (3, 'beta'), (4, 'delta')`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	limit := 2
	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/users", Name: "Users"},
		Name: "Users",
		RootView: &spec.View{
			Source: &spec.ViewSource{
				SQL: "SELECT id, name FROM users",
				Controls: &spec.ViewControls{
					OrderBy: "id ASC",
					Limit:   &limit,
				},
			},
		},
		Parameters: []*spec.Parameter{
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
	}

	type input struct{}
	type row struct {
		ID   int
		Name string
	}
	type output struct {
		Data []*row
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

	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact),
		Artifact:   artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
		Scope:      testharness.Request{}.WithQuery(url.Values{}),
		Providers: rootSelectors(xstate.Selector{
			Page: 2,
		}),
	}

	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	assertly.AssertValues(t, &output{
		Data: []*row{
			{ID: 3, Name: "beta"},
			{ID: 4, Name: "delta"},
		},
	}, actual)
}

func TestService_Read_AppliesSelectorCriteria_SQLite(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`,
		`INSERT INTO users(id, name) VALUES (1, 'zeta'), (2, 'alpha'), (3, 'beta')`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/users", Name: "Users"},
		Name: "Users",
		RootView: &spec.View{
			Source: &spec.ViewSource{
				SQL: "SELECT id, name FROM users $WHERE_SELECTOR_CRITERIA ORDER BY id",
			},
		},
		Parameters: []*spec.Parameter{
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
	}

	type input struct{}
	type row struct {
		ID   int
		Name string
	}
	type output struct {
		Data []*row
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

	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact),
		Artifact:   artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
		Scope:      testharness.Request{}.WithQuery(url.Values{}),
		Providers: rootSelectors(xstate.Selector{
			Criteria:     "name LIKE ?",
			Placeholders: []interface{}{"a%"},
		}),
	}

	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	assertly.AssertValues(t, &output{
		Data: []*row{
			{ID: 2, Name: "alpha"},
		},
	}, actual)
}

func TestService_Read_AppendsImplicitSelectorCriteria_SQLite(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, tenant_id INTEGER, name TEXT);`,
		`INSERT INTO users(id, tenant_id, name) VALUES (1, 7, 'zeta'), (2, 7, 'alpha'), (3, 8, 'alpha')`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/users", Name: "Users"},
		Name: "Users",
		RootView: &spec.View{
			Source: &spec.ViewSource{
				SQL: "SELECT id, name FROM users WHERE tenant_id = :TenantID ORDER BY id",
			},
		},
		Parameters: []*spec.Parameter{
			{Name: "TenantID", Source: spec.BindSource{Kind: "path", Name: "tenantId"}},
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
	}

	type input struct {
		TenantID int
	}
	type row struct {
		ID   int
		Name string
	}
	type output struct {
		Data []*row
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

	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact),
		Artifact:   artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
		Scope:      testharness.Request{}.WithPathParams(map[string]string{"tenantId": "7"}).WithQuery(url.Values{}),
		Providers: rootSelectors(xstate.Selector{
			Criteria:     "name LIKE ?",
			Placeholders: []interface{}{"a%"},
		}),
	}

	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	assertly.AssertValues(t, &output{
		Data: []*row{
			{ID: 2, Name: "alpha"},
		},
	}, actual)
}

func TestService_Read_StripsWhereCriteriaTokenWithoutRelation_SQLite(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`,
		`INSERT INTO users(id, name) VALUES (1, 'zeta'), (2, 'alpha')`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/users", Name: "Users"},
		Name: "Users",
		RootView: &spec.View{
			Source: &spec.ViewSource{
				SQL: "SELECT id, name FROM users WHERE $WHERE_CRITERIA ORDER BY id",
			},
		},
		Parameters: []*spec.Parameter{
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
	}

	type input struct{}
	type row struct {
		ID   int
		Name string
	}
	type output struct {
		Data []*row
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

	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact),
		Artifact:   artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
		Scope:      testharness.Request{}.WithQuery(url.Values{}),
	}

	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	assertly.AssertValues(t, &output{
		Data: []*row{
			{ID: 1, Name: "zeta"},
			{ID: 2, Name: "alpha"},
		},
	}, actual)
}

func TestService_Read_StripsAndCriteriaTokenWithoutRelation_SQLite(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`,
		`INSERT INTO users(id, name) VALUES (1, 'zeta'), (2, 'alpha')`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/users", Name: "Users"},
		Name: "Users",
		RootView: &spec.View{
			Source: &spec.ViewSource{
				SQL: "SELECT id, name FROM users WHERE 1=1 $AND_CRITERIA ORDER BY id",
			},
		},
		Parameters: []*spec.Parameter{
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
	}

	type input struct{}
	type row struct {
		ID   int
		Name string
	}
	type output struct {
		Data []*row
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

	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact),
		Artifact:   artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
		Scope:      testharness.Request{}.WithQuery(url.Values{}),
	}

	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	assertly.AssertValues(t, &output{
		Data: []*row{
			{ID: 1, Name: "zeta"},
			{ID: 2, Name: "alpha"},
		},
	}, actual)
}

func TestService_Read_StripsOrCriteriaTokenWithoutRelation_SQLite(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`,
		`INSERT INTO users(id, name) VALUES (1, 'zeta'), (2, 'alpha')`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/users", Name: "Users"},
		Name: "Users",
		RootView: &spec.View{
			Source: &spec.ViewSource{
				SQL: "SELECT id, name FROM users WHERE 1=0 $OR_CRITERIA ORDER BY id",
			},
		},
		Parameters: []*spec.Parameter{
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
	}

	type input struct{}
	type row struct {
		ID   int
		Name string
	}
	type output struct {
		Data []*row
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

	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact),
		Artifact:   artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
		Scope:      testharness.Request{}.WithQuery(url.Values{}),
	}

	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	assertly.AssertValues(t, &output{
		Data: []*row{},
	}, actual)
}

func TestService_Read_AppendsImplicitSelectorCriteriaBeforeGroupBy_SQLite(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, tenant_id INTEGER, name TEXT);`,
		`INSERT INTO users(id, tenant_id, name) VALUES (1, 7, 'alpha'), (2, 7, 'adam'), (3, 8, 'beta')`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/users", Name: "Users"},
		Name: "Users",
		RootView: &spec.View{
			Source: &spec.ViewSource{
				SQL: "SELECT tenant_id, COUNT(*) AS total FROM users GROUP BY tenant_id ORDER BY tenant_id",
			},
		},
		Parameters: []*spec.Parameter{
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
	}

	type input struct{}
	type row struct {
		TenantID int `sqlx:"tenant_id"`
		Total    int `sqlx:"total"`
	}
	type output struct {
		Data []*row
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

	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact),
		Artifact:   artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
		Scope:      testharness.Request{}.WithQuery(url.Values{}),
		Providers: rootSelectors(xstate.Selector{
			Criteria:     "tenant_id = ?",
			Placeholders: []interface{}{7},
		}),
	}

	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	assertly.AssertValues(t, &output{
		Data: []*row{
			{TenantID: 7, Total: 2},
		},
	}, actual)

	// Clause placement does not grant access to columns absent from registered
	// metadata and the authored projection. Such business filters use predicates.
	session.Providers = rootSelectors(xstate.Selector{Criteria: "name LIKE ?", Placeholders: []any{"a%"}})
	if _, err := NewService().Read(context.Background(), session); err == nil {
		t.Fatal("undeclared criteria column was accepted")
	}
}

func groupableRootView(sqlText string) *spec.View {
	groupable := true
	return &spec.View{Groupable: &groupable, Source: &spec.ViewSource{SQL: sqlText}}
}

func TestService_Read_GroupedProjectionRenumbersGroupBy_SQLite(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, tenant_id INTEGER, name TEXT);`,
		`INSERT INTO users(id, tenant_id, name) VALUES (1, 7, 'alpha'), (2, 7, 'adam'), (3, 8, 'beta')`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	component := &spec.Component{
		Key:      spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/users", Name: "Users"},
		Name:     "Users",
		RootView: groupableRootView("SELECT tenant_id, name, COUNT(*) AS total FROM users GROUP BY tenant_id, name ORDER BY tenant_id, name"),
		Parameters: []*spec.Parameter{
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
	}

	type input struct{}
	type row struct {
		TenantID int `sqlx:"tenant_id"`
		Total    int `sqlx:"total"`
	}
	type output struct {
		Data []*row
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

	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact),
		Artifact:   artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
		Scope:      testharness.Request{}.WithQuery(url.Values{}),
		Providers: rootSelectors(xstate.Selector{
			Columns: []string{"tenant_id", "total"},
		}),
	}

	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	assertly.AssertValues(t, &output{
		Data: []*row{
			{TenantID: 7, Total: 2},
			{TenantID: 8, Total: 1},
		},
	}, actual)
}

func TestService_Read_GroupedProjectionAggregateOnlyDropsGroupBy_SQLite(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, tenant_id INTEGER, name TEXT);`,
		`INSERT INTO users(id, tenant_id, name) VALUES (1, 7, 'alpha'), (2, 7, 'adam'), (3, 8, 'beta')`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	component := &spec.Component{
		Key:      spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/users", Name: "Users"},
		Name:     "Users",
		RootView: groupableRootView("SELECT tenant_id, COUNT(*) AS total FROM users GROUP BY tenant_id ORDER BY tenant_id"),
		Parameters: []*spec.Parameter{
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
	}

	type input struct{}
	type row struct {
		Total int `sqlx:"total"`
	}
	type output struct {
		Data []*row
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

	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact),
		Artifact:   artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
		Scope:      testharness.Request{}.WithQuery(url.Values{}),
		Providers: rootSelectors(xstate.Selector{
			Columns: []string{"total"},
		}),
	}

	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	assertly.AssertValues(t, &output{
		Data: []*row{
			{Total: 3},
		},
	}, actual)
}

func TestService_Read_GroupedProjectionKeepsRelationJoinKey_SQLite(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, tenant_id INTEGER, name TEXT);`,
		`CREATE TABLE accounts (id INTEGER PRIMARY KEY, user_id INTEGER, name TEXT);`,
		`INSERT INTO users(id, tenant_id, name) VALUES (7, 1, 'alpha')`,
		`INSERT INTO accounts(id, user_id, name) VALUES (1, 7, 'a1'), (2, 7, 'a2')`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	component := &spec.Component{
		Key:      spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/users", Name: "Users"},
		Name:     "Users",
		RootView: groupableRootView("SELECT id, COUNT(*) AS total FROM users GROUP BY id ORDER BY id"),
		Parameters: []*spec.Parameter{
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
	}

	type input struct{}
	type account struct {
		ID     int
		UserID int
		Name   string
	}
	type row struct {
		ID       int        `sqlx:"id"`
		Total    int        `sqlx:"total"`
		Accounts []*account `view:"accounts" sql:"SELECT id, user_id, name FROM accounts WHERE user_id IN (?)" on:"ID:id=UserID:user_id"`
	}
	type output struct {
		Data []*row
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

	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact),
		Artifact:   artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
		Scope:      testharness.Request{}.WithQuery(url.Values{}),
		Providers: rootSelectors(xstate.Selector{
			Columns: []string{"total", "Accounts"},
		}),
	}

	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	assertly.AssertValues(t, &output{
		Data: []*row{{
			ID:    7,
			Total: 1,
			Accounts: []*account{
				{ID: 1, UserID: 7, Name: "a1"},
				{ID: 2, UserID: 7, Name: "a2"},
			},
		}},
	}, actual)
}

func TestService_Read_GroupedProjectionKeepsAliasedRelationJoinKey_SQLite(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, tenant_id INTEGER, name TEXT);`,
		`CREATE TABLE accounts (id INTEGER PRIMARY KEY, user_id INTEGER, name TEXT);`,
		`INSERT INTO users(id, tenant_id, name) VALUES (7, 1, 'alpha')`,
		`INSERT INTO accounts(id, user_id, name) VALUES (1, 7, 'a1'), (2, 7, 'a2')`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	component := &spec.Component{
		Key:      spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/users", Name: "Users"},
		Name:     "Users",
		RootView: groupableRootView("SELECT u.id AS user_id, COUNT(*) AS total FROM users u GROUP BY u.id ORDER BY u.id"),
		Parameters: []*spec.Parameter{
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
	}

	type input struct{}
	type account struct {
		ID     int
		UserID int
		Name   string
	}
	type row struct {
		UserID   int        `sqlx:"user_id"`
		Total    int        `sqlx:"total"`
		Accounts []*account `view:"accounts" sql:"SELECT id, user_id, name FROM accounts WHERE user_id IN (?)" on:"UserID:user_id=UserID:user_id"`
	}
	type output struct {
		Data []*row
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

	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact),
		Artifact:   artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
		Scope:      testharness.Request{}.WithQuery(url.Values{}),
		Providers: rootSelectors(xstate.Selector{
			Columns: []string{"total", "Accounts"},
		}),
	}

	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	assertly.AssertValues(t, &output{
		Data: []*row{{
			UserID: 7,
			Total:  1,
			Accounts: []*account{
				{ID: 1, UserID: 7, Name: "a1"},
				{ID: 2, UserID: 7, Name: "a2"},
			},
		}},
	}, actual)
}

func TestService_Read_GroupedProjectionKeepsQualifiedSourceRelationAlias_SQLite(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE contexts (segment_id TEXT, name TEXT);`,
		`CREATE TABLE context_values (id INTEGER PRIMARY KEY, segment_id TEXT, value TEXT);`,
		`INSERT INTO contexts(segment_id, name) VALUES ('seg-1', 'ctx')`,
		`INSERT INTO context_values(id, segment_id, value) VALUES (1, 'seg-1', 'v1'), (2, 'seg-1', 'v2')`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	component := &spec.Component{
		Key:      spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/contexts", Name: "Contexts"},
		Name:     "Contexts",
		RootView: groupableRootView("SELECT c.segment_id AS contextual_value, c.name FROM contexts c GROUP BY c.segment_id, c.name ORDER BY c.segment_id"),
		Parameters: []*spec.Parameter{
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
	}

	type input struct{}
	type child struct {
		ID      int
		Segment string `sqlx:"segment_id"`
		Value   string
	}
	type row struct {
		ContextualValue string   `sqlx:"contextual_value"`
		Name            string   `sqlx:"name"`
		Values          []*child `view:"values" sql:"SELECT id, segment_id, value FROM context_values WHERE segment_id IN (?)" on:"ContextualValue:contextual_value=Segment:segment_id"`
	}
	type output struct {
		Data []*row
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

	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact),
		Artifact:   artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
		Scope:      testharness.Request{}.WithQuery(url.Values{}),
		Providers: rootSelectors(xstate.Selector{
			Columns: []string{"name", "Values"},
		}),
	}

	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	assertly.AssertValues(t, &output{
		Data: []*row{{
			ContextualValue: "seg-1",
			Name:            "ctx",
			Values: []*child{
				{ID: 1, Segment: "seg-1", Value: "v1"},
				{ID: 2, Segment: "seg-1", Value: "v2"},
			},
		}},
	}, actual)
}

func TestService_Read_GroupedCTEProjectionKeepsSelectedNonAggregateDimension_SQLite(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (tenant_id INTEGER, name TEXT, category TEXT, amount INTEGER);`,
		`INSERT INTO users(tenant_id, name, category, amount) VALUES (1, 'a', 'sports', 10), (1, 'b', 'sports', 20), (2, 'c', 'news', 30)`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	component := &spec.Component{
		Key:      spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/users", Name: "Users"},
		Name:     "Users",
		RootView: groupableRootView("WITH last_n AS (SELECT tenant_id, name, category, amount FROM users) SELECT tenant_id, category, SUM(amount) AS total FROM last_n GROUP BY tenant_id, name, category ORDER BY tenant_id LIMIT 1000"),
		Parameters: []*spec.Parameter{
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
	}

	type input struct{}
	type row struct {
		TenantID int    `sqlx:"tenant_id"`
		Category string `sqlx:"category"`
		Total    int    `sqlx:"total"`
	}
	type output struct {
		Data []*row
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

	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact),
		Artifact:   artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
		Scope:      testharness.Request{}.WithQuery(url.Values{}),
		Providers: rootSelectors(xstate.Selector{
			Columns: []string{"tenant_id", "category", "total"},
		}),
	}

	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	assertly.AssertValues(t, &output{
		Data: []*row{
			{TenantID: 1, Category: "sports", Total: 30},
			{TenantID: 2, Category: "news", Total: 30},
		},
	}, actual)
}

func TestService_Read_GroupedReportProjectionDropsOrderByOnPrunedDimension_SQLite(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (tenant_id INTEGER, name TEXT, amount INTEGER);`,
		`INSERT INTO users(tenant_id, name, amount) VALUES (1, 'a', 10), (1, 'b', 20), (2, 'c', 30)`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	component := &spec.Component{
		Key:      spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/users", Name: "Users"},
		Name:     "Users",
		RootView: groupableRootView("WITH last_n AS (SELECT tenant_id, name, amount FROM users) SELECT name, SUM(amount) AS total FROM last_n GROUP BY name, tenant_id ORDER BY tenant_id LIMIT 1000"),
		Parameters: []*spec.Parameter{
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
	}

	type input struct{}
	type row struct {
		Total int `sqlx:"total"`
	}
	type output struct {
		Data []*row
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

	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact),
		Artifact:   artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
		Scope:      testharness.Request{}.WithQuery(url.Values{}),
		Providers: rootSelectors(xstate.Selector{
			Columns: []string{"total"},
		}),
	}

	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	assertly.AssertValues(t, &output{
		Data: []*row{{Total: 60}},
	}, actual)
}

func TestService_Read_GroupedProjectionKeepsOrderBySelectedAggregateAlias_SQLite(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (tenant_id INTEGER, amount INTEGER);`,
		`INSERT INTO users(tenant_id, amount) VALUES (1, 10), (1, 15), (2, 30)`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	component := &spec.Component{
		Key:      spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/users", Name: "Users"},
		Name:     "Users",
		RootView: groupableRootView("SELECT tenant_id, SUM(amount) AS total FROM users GROUP BY tenant_id ORDER BY total DESC"),
		Parameters: []*spec.Parameter{
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
	}

	type input struct{}
	type row struct {
		Total int `sqlx:"total"`
	}
	type output struct {
		Data []*row
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

	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact),
		Artifact:   artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
		Scope:      testharness.Request{}.WithQuery(url.Values{}),
		Providers: rootSelectors(xstate.Selector{
			Columns: []string{"total"},
		}),
	}

	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	assertly.AssertValues(t, &output{
		Data: []*row{{Total: 55}},
	}, actual)
}

func TestService_Read_GroupedProjectionWithSelectorCriteria_SQLite(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (tenant_id INTEGER, name TEXT, amount INTEGER);`,
		`INSERT INTO users(tenant_id, name, amount) VALUES (1, 'alpha', 10), (1, 'adam', 20), (2, 'beta', 30)`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	component := &spec.Component{
		Key:      spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/users", Name: "Users"},
		Name:     "Users",
		RootView: groupableRootView("SELECT tenant_id, name, SUM(amount) AS total FROM users GROUP BY tenant_id, name ORDER BY total DESC"),
		Parameters: []*spec.Parameter{
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
	}

	type input struct{}
	type row struct {
		TenantID int `sqlx:"tenant_id"`
		Total    int `sqlx:"total"`
	}
	type output struct {
		Data []*row
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

	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact),
		Artifact:   artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
		Scope:      testharness.Request{}.WithQuery(url.Values{}),
		Providers: rootSelectors(xstate.Selector{
			Columns:      []string{"tenant_id", "total"},
			Criteria:     "name LIKE ?",
			Placeholders: []interface{}{"a%"},
		}),
	}

	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	assertly.AssertValues(t, &output{
		Data: []*row{{TenantID: 1, Total: 30}},
	}, actual)
}

func TestService_Read_GroupedProjectionAddsGroupByWhenMissing_SQLite(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (tenant_id INTEGER, amount INTEGER);`,
		`INSERT INTO users(tenant_id, amount) VALUES (1, 10), (1, 15), (2, 30)`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	component := &spec.Component{
		Key:      spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/users", Name: "Users"},
		Name:     "Users",
		RootView: groupableRootView("SELECT tenant_id, SUM(amount) AS total FROM users ORDER BY total DESC"),
		Parameters: []*spec.Parameter{
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
	}

	type input struct{}
	type row struct {
		TenantID int `sqlx:"tenant_id"`
		Total    int `sqlx:"total"`
	}
	type output struct {
		Data []*row
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

	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact),
		Artifact:   artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
		Scope:      testharness.Request{}.WithQuery(url.Values{}),
		Providers: rootSelectors(xstate.Selector{
			Columns: []string{"tenant_id", "total"},
		}),
	}

	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	assertly.AssertValues(t, &output{
		Data: []*row{
			{TenantID: 2, Total: 30},
			{TenantID: 1, Total: 25},
		},
	}, actual)
}

func TestService_Read_ReplacesPaginationToken_SQLite(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`,
		`INSERT INTO users(id, name) VALUES (1, 'zeta'), (2, 'alpha'), (3, 'beta'), (4, 'delta')`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	limit := 2
	offset := 1
	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/users", Name: "Users"},
		Name: "Users",
		RootView: &spec.View{
			Source: &spec.ViewSource{
				SQL: "SELECT id, name FROM (SELECT * FROM users ORDER BY id $PAGINATION) t",
				Controls: &spec.ViewControls{
					Limit:  &limit,
					Offset: &offset,
				},
			},
		},
		Parameters: []*spec.Parameter{
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
	}

	type input struct{}
	type row struct {
		ID   int
		Name string
	}
	type output struct {
		Data []*row
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

	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact),
		Artifact:   artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
		Scope:      testharness.Request{}.WithQuery(url.Values{}),
	}

	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	assertly.AssertValues(t, &output{
		Data: []*row{
			{ID: 2, Name: "alpha"},
			{ID: 3, Name: "beta"},
		},
	}, actual)
}

func TestService_Read_DecodesJSONEncodedRowSlicesViaSQLXTags(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE audiences (id INTEGER PRIMARY KEY, tags TEXT, scores TEXT);`,
		`INSERT INTO audiences(id, tags, scores) VALUES (7, '["sports","news"]', '[1,2,3]')`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/audiences", Name: "Audiences"},
		Name: "Audiences",
		RootView: &spec.View{
			Source: &spec.ViewSource{
				SQL: "SELECT id, tags, scores FROM audiences WHERE id = :ID",
			},
		},
		Parameters: []*spec.Parameter{
			{Name: "ID", Source: spec.BindSource{Kind: "path", Name: "id"}},
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
	}

	type input struct {
		ID int
	}
	type jsonAudienceRow struct {
		ID     int      `sqlx:"id"`
		Tags   []string `sqlx:"tags,enc=JSON"`
		Scores []int    `sqlx:"scores,enc=JSON"`
	}
	type output struct {
		Data []*jsonAudienceRow
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

	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact),
		Artifact:   artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
		Scope:      testharness.Request{}.WithPathParams(map[string]string{"id": "7"}).WithQuery(url.Values{}),
	}

	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	assertly.AssertValues(t, &output{
		Data: []*jsonAudienceRow{{
			ID:     7,
			Tags:   []string{"sports", "news"},
			Scores: []int{1, 2, 3},
		}},
	}, actual)
}

func TestService_Read_DecodesCSVEncodedRowSlicesViaSQLXTags(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE audiences (id INTEGER PRIMARY KEY, tags TEXT, scores TEXT);`,
		`INSERT INTO audiences(id, tags, scores) VALUES (7, 'sports,news', '1,2,3')`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/audiences", Name: "Audiences"},
		Name: "Audiences",
		RootView: &spec.View{
			Source: &spec.ViewSource{
				SQL: "SELECT id, tags, scores FROM audiences WHERE id = :ID",
			},
		},
		Parameters: []*spec.Parameter{
			{Name: "ID", Source: spec.BindSource{Kind: "path", Name: "id"}},
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
	}

	type input struct {
		ID int
	}
	type csvAudienceRow struct {
		ID     int      `sqlx:"id"`
		Tags   []string `sqlx:"tags,enc=CSV"`
		Scores []int    `sqlx:"scores,enc=CSV"`
	}
	type output struct {
		Data []*csvAudienceRow
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

	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact),
		Artifact:   artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
		Scope:      testharness.Request{}.WithPathParams(map[string]string{"id": "7"}).WithQuery(url.Values{}),
	}

	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	assertly.AssertValues(t, &output{
		Data: []*csvAudienceRow{{
			ID:     7,
			Tags:   []string{"sports", "news"},
			Scores: []int{1, 2, 3},
		}},
	}, actual)
}

func TestService_Read_AppliesSelectorProjectionAndKeepsRelationJoinKey(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`,
		`CREATE TABLE accounts (id INTEGER PRIMARY KEY, user_id INTEGER, name TEXT);`,
		`INSERT INTO users(id, name) VALUES (7, 'john')`,
		`INSERT INTO accounts(id, user_id, name) VALUES (1, 7, 'a1'), (2, 7, 'a2')`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/users", Name: "Users"},
		Name: "Users",
		RootView: &spec.View{
			Source: &spec.ViewSource{SQL: "SELECT id, name FROM users WHERE id = :ID"},
		},
		Parameters: []*spec.Parameter{
			{Name: "ID", Source: spec.BindSource{Kind: "path", Name: "id"}},
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
	}

	type input struct{ ID int }
	type account struct {
		ID     int
		UserID int
		Name   string
	}
	type projectedUserRow struct {
		ID       int
		Name     string
		Accounts []*account `view:"accounts" sql:"SELECT id, user_id, name FROM accounts WHERE user_id IN (?)" on:"ID:id=UserID:user_id"`
	}
	type output struct {
		Data []*projectedUserRow
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

	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact),
		Artifact:   artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
		Scope:      testharness.Request{}.WithPathParams(map[string]string{"id": "7"}).WithQuery(url.Values{}),
		Providers: rootSelectors(xstate.Selector{
			Columns: []string{"Name", "Accounts"},
		}),
	}

	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	assertly.AssertValues(t, &output{
		Data: []*projectedUserRow{{
			ID:   7,
			Name: "john",
			Accounts: []*account{
				{ID: 1, UserID: 7, Name: "a1"},
				{ID: 2, UserID: 7, Name: "a2"},
			},
		}},
	}, actual)
}

func TestService_Read_AppliesSelectorFieldProjectionAndKeepsRelationJoinKey(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`,
		`CREATE TABLE accounts (id INTEGER PRIMARY KEY, user_id INTEGER, name TEXT);`,
		`INSERT INTO users(id, name) VALUES (7, 'john')`,
		`INSERT INTO accounts(id, user_id, name) VALUES (1, 7, 'a1'), (2, 7, 'a2')`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/users", Name: "Users"},
		Name: "Users",
		RootView: &spec.View{
			Source: &spec.ViewSource{SQL: "SELECT id, name FROM users WHERE id = :ID"},
		},
		Parameters: []*spec.Parameter{
			{Name: "ID", Source: spec.BindSource{Kind: "path", Name: "id"}},
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
	}

	type input struct{ ID int }
	type account struct {
		ID     int
		UserID int
		Name   string
	}
	type projectedUserRow struct {
		ID       int
		Name     string
		Accounts []*account `view:"accounts" sql:"SELECT id, user_id, name FROM accounts WHERE user_id IN (?)" on:"ID:id=UserID:user_id"`
	}
	type output struct {
		Data []*projectedUserRow
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

	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact),
		Artifact:   artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
		Scope:      testharness.Request{}.WithPathParams(map[string]string{"id": "7"}).WithQuery(url.Values{}),
		Providers: rootSelectors(xstate.Selector{
			Fields: []string{"Name", "Accounts"},
		}),
	}

	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	assertly.AssertValues(t, &output{
		Data: []*projectedUserRow{{
			ID:   7,
			Name: "john",
			Accounts: []*account{
				{ID: 1, UserID: 7, Name: "a1"},
				{ID: 2, UserID: 7, Name: "a2"},
			},
		}},
	}, actual)
}

func TestService_Read_AppliesSelectorProjectionBySQLAlias(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`,
		`CREATE TABLE accounts (id INTEGER PRIMARY KEY, user_id INTEGER, name TEXT);`,
		`INSERT INTO users(id, name) VALUES (7, 'john')`,
		`INSERT INTO accounts(id, user_id, name) VALUES (1, 7, 'a1'), (2, 7, 'a2')`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/users", Name: "Users"},
		Name: "Users",
		RootView: &spec.View{
			Source: &spec.ViewSource{SQL: "SELECT id, name AS display_name FROM users WHERE id = :ID"},
		},
		Parameters: []*spec.Parameter{
			{Name: "ID", Source: spec.BindSource{Kind: "path", Name: "id"}},
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
	}

	type input struct{ ID int }
	type account struct {
		ID     int
		UserID int
		Name   string
	}
	type row struct {
		ID          int        `sqlx:"id"`
		DisplayName string     `sqlx:"display_name"`
		Accounts    []*account `view:"accounts" sql:"SELECT id, user_id, name FROM accounts WHERE user_id IN (?)" on:"ID:id=UserID:user_id"`
	}
	type output struct {
		Data []*row
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

	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact),
		Artifact:   artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
		Scope:      testharness.Request{}.WithPathParams(map[string]string{"id": "7"}).WithQuery(url.Values{}),
		Providers: rootSelectors(xstate.Selector{
			Columns: []string{"display_name", "Accounts"},
		}),
	}

	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	assertly.AssertValues(t, &output{
		Data: []*row{{
			ID:          7,
			DisplayName: "john",
			Accounts: []*account{
				{ID: 1, UserID: 7, Name: "a1"},
				{ID: 2, UserID: 7, Name: "a2"},
			},
		}},
	}, actual)
}

func TestService_Read_AppliesExplicitSelectorAliasAndSkipsUnselectedRelation(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE metrics (advertiser_id INTEGER, spend REAL);`,
		`INSERT INTO metrics(advertiser_id, spend) VALUES (7, 12.5)`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/metrics", Name: "Metrics"},
		Name: "Metrics",
		RootView: &spec.View{
			Source: &spec.ViewSource{SQL: "SELECT advertiser_id, spend FROM metrics"},
		},
		Parameters: []*spec.Parameter{
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
	}

	type input struct{}
	type account struct {
		ID int
	}
	type row struct {
		AdvertiserID int        `sqlx:"advertiser_id" selectorAlias:"advertiserId" json:"advertiserId"`
		Spend        float64    `sqlx:"spend" json:"spend"`
		Accounts     []*account `view:"accounts" sql:"SELECT id FROM missing_accounts WHERE advertiser_id IN (?)" on:"AdvertiserID:advertiser_id=ID:advertiser_id" json:"accounts,omitempty"`
	}
	type output struct {
		Data []*row
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

	for _, selected := range []string{"advertiserId", "advertiser_id"} {
		t.Run(selected, func(t *testing.T) {
			session := &Session{
				Component:  component,
				OutputType: reflect.TypeOf(output{}),
				Input:      routeInput(t, artifact),
				Artifact:   artifact.Reader,
				SQL:        &rsql.SQLComponent{DB: h.DB},
				Scope:      testharness.Request{}.WithQuery(url.Values{}),
				Providers: rootSelectors(xstate.Selector{
					Columns: []string{selected, "spend"},
				}),
			}

			actual, err := NewService().Read(context.Background(), session)
			if err != nil {
				t.Fatalf("read failed: %v", err)
			}
			assertly.AssertValues(t, &output{Data: []*row{{AdvertiserID: 7, Spend: 12.5}}}, actual)
			encoded, err := json.Marshal(actual)
			if err != nil {
				t.Fatalf("marshal failed: %v", err)
			}
			assertly.AssertValues(t, `{"Data":[{"advertiserId":7,"spend":12.5}]}`, string(encoded))
		})
	}
}

func TestService_Read_SelectedRelationUsesUnmappedParentColumn(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (parent_id INTEGER, name TEXT);`,
		`CREATE TABLE accounts (id INTEGER PRIMARY KEY, user_id INTEGER, name TEXT);`,
		`INSERT INTO users(parent_id, name) VALUES (7, 'john')`,
		`INSERT INTO accounts(id, user_id, name) VALUES (1, 7, 'a1'), (2, 7, 'a2')`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/users", Name: "Users"},
		Name: "Users",
		RootView: &spec.View{
			Source: &spec.ViewSource{SQL: "SELECT parent_id, name FROM users"},
		},
		Parameters: []*spec.Parameter{
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
	}

	type account struct {
		ID     int
		UserID int
		Name   string
	}
	type row struct {
		Name     string
		Accounts []*account
	}
	type output struct {
		Data []*row
	}
	type input struct{}

	artifact, err := buildArtifact(bootstrap.ArtifactInput{
		Component:       component,
		InputType:       reflect.TypeOf(input{}),
		OutputType:      reflect.TypeOf(output{}),
		DirectViewField: "Data",
	})
	if err != nil {
		t.Fatalf("artifact build failed: %v", err)
	}

	rowType := reflect.TypeOf(row{})
	accountType := reflect.TypeOf(account{})
	rootView := &data.View{Spec: spec.View{Name: "Users",
		Source: component.RootView.Source}, Relations: []*data.Relation{},
	}
	rootView.Relations = append(rootView.Relations, &data.Relation{
		Name:        "Accounts",
		Kind:        spec.RelationKindSubview,
		Holder:      "Accounts",
		Cardinality: spec.CardinalityMany,
		On: data.Links{
			data.NewLink("", "parent_id", "ParentID"),
		},
		Of: &data.RelationRef{
			View: &data.View{Spec: spec.View{Name: "accounts",
				Source: &spec.ViewSource{SQL: "SELECT id, user_id, name FROM accounts WHERE user_id IN (?)"}},
			},
			On: data.Links{
				data.NewLink("", "user_id", "UserID"),
			},
			MatchStrategy: data.MatchSequential,
		},
	})
	collection, err := rcollector.Compile(rootView, map[*data.View]reflect.Type{
		rootView:                      rowType,
		rootView.Relations[0].Of.View: accountType,
	})
	if err != nil {
		t.Fatalf("compile collector graph: %v", err)
	}
	artifact.Reader, err = sqlreader.NewPlan(sqlreader.PlanConfig{
		SelectorBindings: artifact.Reader.SelectorBindings, ViewIndex: sqlreader.NewViewIndex(component, rootView),
		OutputViewField: artifact.Reader.OutputViewField, DirectOutput: artifact.Reader.DirectOutput,
		RootView: rootView, Collection: collection,
	})
	if err != nil {
		t.Fatalf("assemble reader plan: %v", err)
	}

	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact),
		Artifact:   artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
		Scope:      testharness.Request{}.WithQuery(url.Values{}),
		Providers: rootSelectors(xstate.Selector{
			Columns: []string{"Name", "Accounts"},
		}),
	}

	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	assertly.AssertValues(t, &output{
		Data: []*row{{
			Name: "john",
			Accounts: []*account{
				{ID: 1, UserID: 7, Name: "a1"},
				{ID: 2, UserID: 7, Name: "a2"},
			},
		}},
	}, actual)
}

func TestService_Read_AppliesSelectorProjectionByExpressionAlias(t *testing.T) {
	useCases := []struct {
		description string
		sqlText     string
		expectName  string
	}{
		{
			description: "case expression alias maps through projection",
			sqlText:     "SELECT id, (CASE WHEN name <> '' THEN name ELSE NULL END) AS display_name FROM users WHERE id = :ID",
			expectName:  "john",
		},
		{
			description: "coalesce expression alias maps through projection",
			sqlText:     "SELECT id, COALESCE(name, 'n/a') AS display_name FROM users WHERE id = :ID",
			expectName:  "john",
		},
	}

	for _, useCase := range useCases {
		t.Run(useCase.description, func(t *testing.T) {
			h := testharness.NewSQLiteHarness(t)
			if err := h.ExecStatements(context.Background(),
				`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`,
				`INSERT INTO users(id, name) VALUES (7, 'john')`,
			); err != nil {
				t.Fatalf("setup failed: %v", err)
			}

			component := &spec.Component{
				Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/users", Name: "Users"},
				Name: "Users",
				RootView: &spec.View{
					Source: &spec.ViewSource{SQL: useCase.sqlText},
				},
				Parameters: []*spec.Parameter{
					{Name: "ID", Source: spec.BindSource{Kind: "path", Name: "id"}},
					{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
				},
			}

			type input struct{ ID int }
			type row struct {
				DisplayName string `sqlx:"display_name"`
			}
			type output struct {
				Data []*row
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

			session := &Session{
				Component:  component,
				OutputType: reflect.TypeOf(output{}),
				Input:      routeInput(t, artifact),
				Artifact:   artifact.Reader,
				SQL:        &rsql.SQLComponent{DB: h.DB},
				Scope:      testharness.Request{}.WithPathParams(map[string]string{"id": "7"}).WithQuery(url.Values{}),
				Providers: rootSelectors(xstate.Selector{
					Columns: []string{"display_name"},
				}),
			}

			actual, err := NewService().Read(context.Background(), session)
			if err != nil {
				t.Fatalf("read failed: %v", err)
			}

			assertly.AssertValues(t, &output{
				Data: []*row{{
					DisplayName: useCase.expectName,
				}},
			}, actual)
		})
	}
}

func TestService_Read_AppliesSelectorFieldProjectionBySQLAlias(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`,
		`CREATE TABLE accounts (id INTEGER PRIMARY KEY, user_id INTEGER, name TEXT);`,
		`INSERT INTO users(id, name) VALUES (7, 'john')`,
		`INSERT INTO accounts(id, user_id, name) VALUES (1, 7, 'a1'), (2, 7, 'a2')`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/users", Name: "Users"},
		Name: "Users",
		RootView: &spec.View{
			Source: &spec.ViewSource{SQL: "SELECT id, name AS display_name FROM users WHERE id = :ID"},
		},
		Parameters: []*spec.Parameter{
			{Name: "ID", Source: spec.BindSource{Kind: "path", Name: "id"}},
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
	}

	type input struct{ ID int }
	type account struct {
		ID     int
		UserID int
		Name   string
	}
	type row struct {
		ID          int        `sqlx:"id"`
		DisplayName string     `sqlx:"display_name"`
		Accounts    []*account `view:"accounts" sql:"SELECT id, user_id, name FROM accounts WHERE user_id IN (?)" on:"ID:id=UserID:user_id"`
	}
	type output struct {
		Data []*row
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

	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact),
		Artifact:   artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
		Scope:      testharness.Request{}.WithPathParams(map[string]string{"id": "7"}).WithQuery(url.Values{}),
		Providers: rootSelectors(xstate.Selector{
			Fields: []string{"display_name", "Accounts"},
		}),
	}

	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	assertly.AssertValues(t, &output{
		Data: []*row{{
			ID:          7,
			DisplayName: "john",
			Accounts: []*account{
				{ID: 1, UserID: 7, Name: "a1"},
				{ID: 2, UserID: 7, Name: "a2"},
			},
		}},
	}, actual)
}

func TestService_Read_ErrorsOnMissingSelectedRootColumn(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`,
		`INSERT INTO users(id, name) VALUES (7, 'john')`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/users", Name: "Users"},
		Name: "Users",
		RootView: &spec.View{
			Source: &spec.ViewSource{SQL: "SELECT id, name FROM users WHERE id = :ID"},
		},
		Parameters: []*spec.Parameter{
			{Name: "ID", Source: spec.BindSource{Kind: "path", Name: "id"}},
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
	}

	type input struct{ ID int }
	type row struct {
		ID   int
		Name string
	}
	type output struct {
		Data []*row
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

	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact),
		Artifact:   artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
		Scope:      testharness.Request{}.WithPathParams(map[string]string{"id": "7"}).WithQuery(url.Values{}),
		Providers: rootSelectors(xstate.Selector{
			Columns: []string{"missing"},
		}),
	}

	_, err = NewService().Read(context.Background(), session)
	if err == nil || !strings.Contains(err.Error(), "not found column") {
		t.Fatalf("expected missing selected column error, got %v", err)
	}
}

func TestService_Read_EmptyRootSkipsChildRelationQueries(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`,
		`CREATE TABLE accounts (id INTEGER PRIMARY KEY, user_id INTEGER, name TEXT);`,
		`INSERT INTO accounts(id, user_id, name) VALUES (1, 7, 'a1')`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/users", Name: "Users"},
		Name: "Users",
		RootView: &spec.View{
			Source: &spec.ViewSource{SQL: "SELECT id, name FROM users WHERE id = :ID"},
		},
		Parameters: []*spec.Parameter{
			{Name: "ID", Source: spec.BindSource{Kind: "path", Name: "id"}},
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
	}

	type input struct{ ID int }
	type account struct {
		ID     int
		UserID int
		Name   string
	}
	type row struct {
		ID       int
		Name     string
		Accounts []*account `view:"accounts" sql:"SELECT id, user_id, name FROM accounts WHERE user_id IN (?)" on:"ID:id=UserID:user_id"`
	}
	type output struct {
		Data []*row
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

	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact),
		Artifact:   artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
		Scope:      testharness.Request{}.WithPathParams(map[string]string{"id": "999"}).WithQuery(url.Values{}),
	}

	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	assertly.AssertValues(t, &output{Data: []*row{}}, actual)
}

type relationAwareRow struct {
	ID            int
	Name          string
	Accounts      []*relationAwareAccount `view:"accounts" sql:"SELECT id, user_id, name FROM accounts WHERE user_id IN (?)" on:"ID:id=UserID:user_id"`
	RelationReady bool
}

type relationAwareAccount struct {
	ID     int
	UserID int
	Name   string
}

func (r *relationAwareRow) OnRelation(context.Context) {
	r.RelationReady = len(r.Accounts) > 0
}

func TestService_Read_InvokesOnRelationLifecycle(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`,
		`CREATE TABLE accounts (id INTEGER PRIMARY KEY, user_id INTEGER, name TEXT);`,
		`INSERT INTO users(id, name) VALUES (7, 'john')`,
		`INSERT INTO accounts(id, user_id, name) VALUES (1, 7, 'a1')`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/users", Name: "Users"},
		Name: "Users",
		RootView: &spec.View{
			Source: &spec.ViewSource{SQL: "SELECT id, name FROM users WHERE id = :ID"},
		},
		Parameters: []*spec.Parameter{
			{Name: "ID", Source: spec.BindSource{Kind: "path", Name: "id"}},
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
	}

	type input struct{ ID int }
	type output struct {
		Data []*relationAwareRow
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

	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact),
		Artifact:   artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
		Scope:      testharness.Request{}.WithPathParams(map[string]string{"id": "7"}).WithQuery(url.Values{}),
	}

	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	typed := actual.(*output)
	if len(typed.Data) != 1 || !typed.Data[0].RelationReady {
		t.Fatalf("expected OnRelation to run after child binding, got %+v", typed.Data)
	}
}

func TestService_Read_BindsNestedSubviewRelations(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`,
		`CREATE TABLE accounts (id INTEGER PRIMARY KEY, user_id INTEGER, name TEXT);`,
		`CREATE TABLE account_notes (id INTEGER PRIMARY KEY, account_id INTEGER, text TEXT);`,
		`INSERT INTO users(id, name) VALUES (7, 'john')`,
		`INSERT INTO accounts(id, user_id, name) VALUES (1, 7, 'a1')`,
		`INSERT INTO account_notes(id, account_id, text) VALUES (101, 1, 'n1'), (102, 1, 'n2')`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/users", Name: "Users"},
		Name: "Users",
		RootView: &spec.View{
			Source: &spec.ViewSource{SQL: "SELECT id, name FROM users WHERE id = :ID"},
		},
		Parameters: []*spec.Parameter{
			{Name: "ID", Source: spec.BindSource{Kind: "path", Name: "id"}},
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
	}

	type input struct{ ID int }
	type note struct {
		ID        int
		AccountID int
		Text      string
	}
	type account struct {
		ID     int
		UserID int
		Name   string
		Notes  []*note `view:"notes" sql:"SELECT id, account_id, text FROM account_notes WHERE account_id IN (?)" on:"ID:id=AccountID:account_id"`
	}
	type row struct {
		ID       int
		Name     string
		Accounts []*account `view:"accounts" sql:"SELECT id, user_id, name FROM accounts WHERE user_id IN (?)" on:"ID:id=UserID:user_id"`
	}
	type output struct {
		Data []*row
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

	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact),
		Artifact:   artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
		Scope:      testharness.Request{}.WithPathParams(map[string]string{"id": "7"}).WithQuery(url.Values{}),
	}

	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	assertly.AssertValues(t, &output{
		Data: []*row{{
			ID:   7,
			Name: "john",
			Accounts: []*account{{
				ID:     1,
				UserID: 7,
				Name:   "a1",
				Notes: []*note{
					{ID: 101, AccountID: 1, Text: "n1"},
					{ID: 102, AccountID: 1, Text: "n2"},
				},
			}},
		}},
	}, actual)
}

func TestService_Read_ErrorsOnNestedChildRelationFailure(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`,
		`CREATE TABLE accounts (id INTEGER PRIMARY KEY, user_id INTEGER, name TEXT);`,
		`INSERT INTO users(id, name) VALUES (7, 'john')`,
		`INSERT INTO accounts(id, user_id, name) VALUES (1, 7, 'a1')`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/users", Name: "Users"},
		Name: "Users",
		RootView: &spec.View{
			Source: &spec.ViewSource{SQL: "SELECT id, name FROM users WHERE id = :ID"},
		},
		Parameters: []*spec.Parameter{
			{Name: "ID", Source: spec.BindSource{Kind: "path", Name: "id"}},
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
	}

	type input struct{ ID int }
	type note struct {
		ID int
	}
	type account struct {
		ID     int
		UserID int
		Name   string
		Notes  []*note `view:"notes" sql:"SELECT id FROM missing_notes WHERE account_id IN (?)" on:"ID:id=ID:id"`
	}
	type row struct {
		ID       int
		Name     string
		Accounts []*account `view:"accounts" sql:"SELECT id, user_id, name FROM accounts WHERE user_id IN (?)" on:"ID:id=UserID:user_id"`
	}
	type output struct {
		Data []*row
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

	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact),
		Artifact:   artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
		Scope:      testharness.Request{}.WithPathParams(map[string]string{"id": "7"}).WithQuery(url.Values{}),
	}

	if _, err := NewService().Read(context.Background(), session); err == nil {
		t.Fatalf("expected nested child relation failure")
	}
}

func TestService_Read_BindsReadAllSubviewRelations(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`,
		`CREATE TABLE accounts (id INTEGER PRIMARY KEY, user_id INTEGER, name TEXT);`,
		`INSERT INTO users(id, name) VALUES (7, 'john')`,
		`INSERT INTO accounts(id, user_id, name) VALUES (1, 7, 'a1'), (2, 7, 'a2'), (3, 9, 'a3')`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/users", Name: "Users"},
		Name: "Users",
		RootView: &spec.View{
			Source: &spec.ViewSource{SQL: "SELECT id, name FROM users WHERE id = :ID"},
		},
		Parameters: []*spec.Parameter{
			{Name: "ID", Source: spec.BindSource{Kind: "path", Name: "id"}},
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
	}

	type input struct{ ID int }
	type account struct {
		ID     int
		UserID int
		Name   string
	}
	type row struct {
		ID       int
		Name     string
		Accounts []*account `view:"accounts,match=read_all" sql:"SELECT id, user_id, name FROM accounts ORDER BY id" on:"ID:id=UserID:user_id"`
	}
	type output struct {
		Data []*row
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

	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact),
		Artifact:   artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
		Scope:      testharness.Request{}.WithPathParams(map[string]string{"id": "7"}).WithQuery(url.Values{}),
	}

	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	assertly.AssertValues(t, &output{
		Data: []*row{{
			ID:   7,
			Name: "john",
			Accounts: []*account{
				{ID: 1, UserID: 7, Name: "a1"},
				{ID: 2, UserID: 7, Name: "a2"},
			},
		}},
	}, actual)
}

func TestService_Read_BindsReadAllNestedSubviewRelations(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`,
		`CREATE TABLE accounts (id INTEGER PRIMARY KEY, user_id INTEGER, name TEXT);`,
		`CREATE TABLE account_notes (id INTEGER PRIMARY KEY, account_id INTEGER, text TEXT);`,
		`INSERT INTO users(id, name) VALUES (7, 'john')`,
		`INSERT INTO accounts(id, user_id, name) VALUES (1, 7, 'a1'), (2, 7, 'a2')`,
		`INSERT INTO account_notes(id, account_id, text) VALUES (101, 1, 'n1'), (102, 2, 'n2')`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/users", Name: "Users"},
		Name: "Users",
		RootView: &spec.View{
			Source: &spec.ViewSource{SQL: "SELECT id, name FROM users WHERE id = :ID"},
		},
		Parameters: []*spec.Parameter{
			{Name: "ID", Source: spec.BindSource{Kind: "path", Name: "id"}},
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
	}

	type input struct{ ID int }
	type note struct {
		ID        int
		AccountID int
		Text      string
	}
	type account struct {
		ID     int
		UserID int
		Name   string
		Notes  []*note `view:"notes,match=read_all" sql:"SELECT id, account_id, text FROM account_notes ORDER BY id" on:"ID:id=AccountID:account_id"`
	}
	type row struct {
		ID       int
		Name     string
		Accounts []*account `view:"accounts,match=read_all" sql:"SELECT id, user_id, name FROM accounts ORDER BY id" on:"ID:id=UserID:user_id"`
	}
	type output struct {
		Data []*row
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

	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact),
		Artifact:   artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
		Scope:      testharness.Request{}.WithPathParams(map[string]string{"id": "7"}).WithQuery(url.Values{}),
	}

	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	assertly.AssertValues(t, &output{
		Data: []*row{{
			ID:   7,
			Name: "john",
			Accounts: []*account{
				{ID: 1, UserID: 7, Name: "a1", Notes: []*note{{ID: 101, AccountID: 1, Text: "n1"}}},
				{ID: 2, UserID: 7, Name: "a2", Notes: []*note{{ID: 102, AccountID: 2, Text: "n2"}}},
			},
		}},
	}, actual)
}

func TestService_Read_SkipsRelationWithoutSQLSource(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`,
		`INSERT INTO users(id, name) VALUES (7, 'john')`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/users", Name: "Users"},
		Name: "Users",
		RootView: &spec.View{
			Source: &spec.ViewSource{SQL: "SELECT id, name FROM users WHERE id = :ID"},
		},
		Parameters: []*spec.Parameter{
			{Name: "ID", Source: spec.BindSource{Kind: "path", Name: "id"}},
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
	}

	type input struct{ ID int }
	type profile struct {
		ID int
	}
	type row struct {
		ID      int
		Name    string
		Profile *profile `view:"profile" on:"ID:id=ID:id"`
	}
	type output struct {
		Data []*row
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

	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact),
		Artifact:   artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
		Scope:      testharness.Request{}.WithPathParams(map[string]string{"id": "7"}).WithQuery(url.Values{}),
	}

	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	assertly.AssertValues(t, &output{
		Data: []*row{{ID: 7, Name: "john"}},
	}, actual)
}

func TestService_Read_SkipsRelationWithEmptySQLSource(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`,
		`INSERT INTO users(id, name) VALUES (7, 'john')`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/users", Name: "Users"},
		Name: "Users",
		RootView: &spec.View{
			Source: &spec.ViewSource{SQL: "SELECT id, name FROM users WHERE id = :ID"},
		},
		Parameters: []*spec.Parameter{
			{Name: "ID", Source: spec.BindSource{Kind: "path", Name: "id"}},
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
	}

	type input struct{ ID int }
	type profile struct{ ID int }
	type row struct {
		ID      int
		Name    string
		Profile *profile `view:"profile" sql:"" on:"ID:id=ID:id"`
	}
	type output struct {
		Data []*row
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

	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact),
		Artifact:   artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
		Scope:      testharness.Request{}.WithPathParams(map[string]string{"id": "7"}).WithQuery(url.Values{}),
	}

	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	assertly.AssertValues(t, &output{
		Data: []*row{{ID: 7, Name: "john"}},
	}, actual)
}

func TestService_Read_PrunesUnselectedRelationBeforeBadSQL(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`,
		`INSERT INTO users(id, name) VALUES (7, 'john')`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/users", Name: "Users"},
		Name: "Users",
		RootView: &spec.View{
			Source: &spec.ViewSource{SQL: "SELECT id, name FROM users WHERE id = :ID"},
		},
		Parameters: []*spec.Parameter{
			{Name: "ID", Source: spec.BindSource{Kind: "path", Name: "id"}},
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
	}

	type input struct{ ID int }
	type account struct{ ID int }
	type row struct {
		ID       int
		Name     string
		Accounts []*account `view:"accounts" sql:"SELECT FROM accounts" on:"ID:id=ID:id"`
	}
	type output struct {
		Data []*row
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

	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact),
		Artifact:   artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
		Scope:      testharness.Request{}.WithPathParams(map[string]string{"id": "7"}).WithQuery(url.Values{}),
		Providers: rootSelectors(xstate.Selector{
			Columns: []string{"Name"},
		}),
	}

	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	assertly.AssertValues(t, &output{
		Data: []*row{{ID: 0, Name: "john"}},
	}, actual)
}

func TestService_Read_ErrorsOnInvalidRootSQL(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/users", Name: "Users"},
		Name: "Users",
		RootView: &spec.View{
			Source: &spec.ViewSource{SQL: "SELECT missing FROM users"},
		},
		Parameters: []*spec.Parameter{
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
	}

	type input struct{}
	type row struct{ ID int }
	type output struct {
		Data []*row
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

	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact),
		Artifact:   artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
		Scope:      testharness.Request{}.WithQuery(url.Values{}),
	}

	if _, err := NewService().Read(context.Background(), session); err == nil {
		t.Fatalf("expected invalid root SQL to fail")
	}
}

func TestService_Read_ErrorsOnMalformedRootSQL(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/users", Name: "Users"},
		Name: "Users",
		RootView: &spec.View{
			Source: &spec.ViewSource{SQL: "SELECT FROM users"},
		},
		Parameters: []*spec.Parameter{
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
	}

	type input struct{}
	type row struct{ ID int }
	type output struct {
		Data []*row
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

	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact),
		Artifact:   artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
		Scope:      testharness.Request{}.WithQuery(url.Values{}),
	}

	if _, err := NewService().Read(context.Background(), session); err == nil {
		t.Fatalf("expected malformed root SQL to fail")
	}
}

func TestService_Read_BindsOneToOneSubview(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`,
		`CREATE TABLE profiles (id INTEGER PRIMARY KEY, user_id INTEGER, title TEXT);`,
		`INSERT INTO users(id, name) VALUES (7, 'john')`,
		`INSERT INTO profiles(id, user_id, title) VALUES (1, 7, 'admin')`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/users", Name: "Users"},
		Name: "Users",
		RootView: &spec.View{
			Source: &spec.ViewSource{SQL: "SELECT id, name FROM users WHERE id = :ID"},
		},
		Parameters: []*spec.Parameter{
			{Name: "ID", Source: spec.BindSource{Kind: "path", Name: "id"}},
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
	}

	type input struct{ ID int }
	type profile struct {
		ID     int
		UserID int
		Title  string
	}
	type row struct {
		ID      int
		Name    string
		Profile *profile `view:"profile" sql:"SELECT id, user_id, title FROM profiles WHERE user_id IN (?)" on:"ID:id=UserID:user_id"`
	}
	type output struct {
		Data []*row
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

	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact),
		Artifact:   artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
		Scope:      testharness.Request{}.WithPathParams(map[string]string{"id": "7"}).WithQuery(url.Values{}),
	}

	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	assertly.AssertValues(t, &output{
		Data: []*row{{
			ID:   7,
			Name: "john",
			Profile: &profile{
				ID:     1,
				UserID: 7,
				Title:  "admin",
			},
		}},
	}, actual)
}

func TestService_Read_ErrorsOnInvalidChildRelationSQL(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`,
		`INSERT INTO users(id, name) VALUES (7, 'john')`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/users", Name: "Users"},
		Name: "Users",
		RootView: &spec.View{
			Source: &spec.ViewSource{SQL: "SELECT id, name FROM users WHERE id = :ID"},
		},
		Parameters: []*spec.Parameter{
			{Name: "ID", Source: spec.BindSource{Kind: "path", Name: "id"}},
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
	}

	type input struct{ ID int }
	type account struct {
		ID int
	}
	type row struct {
		ID       int
		Name     string
		Accounts []*account `view:"accounts" sql:"SELECT id FROM missing_accounts WHERE user_id IN (?)" on:"ID:id=ID:id"`
	}
	type output struct {
		Data []*row
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

	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact),
		Artifact:   artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
		Scope:      testharness.Request{}.WithPathParams(map[string]string{"id": "7"}).WithQuery(url.Values{}),
	}

	if _, err := NewService().Read(context.Background(), session); err == nil {
		t.Fatalf("expected invalid child relation SQL to fail")
	}
}

func TestService_Read_ErrorsOnMalformedChildRelationSQL(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`,
		`INSERT INTO users(id, name) VALUES (7, 'john')`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/users", Name: "Users"},
		Name: "Users",
		RootView: &spec.View{
			Source: &spec.ViewSource{SQL: "SELECT id, name FROM users WHERE id = :ID"},
		},
		Parameters: []*spec.Parameter{
			{Name: "ID", Source: spec.BindSource{Kind: "path", Name: "id"}},
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
	}

	type input struct{ ID int }
	type account struct {
		ID int
	}
	type row struct {
		ID       int
		Name     string
		Accounts []*account `view:"accounts" sql:"SELECT FROM accounts" on:"ID:id=ID:id"`
	}
	type output struct {
		Data []*row
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

	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact),
		Artifact:   artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
		Scope:      testharness.Request{}.WithPathParams(map[string]string{"id": "7"}).WithQuery(url.Values{}),
	}

	if _, err := NewService().Read(context.Background(), session); err == nil {
		t.Fatalf("expected malformed child relation SQL to fail")
	}
}

func TestService_Read_ErrorsOnChildRelationBuildFailure(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`,
		`CREATE TABLE accounts (id INTEGER PRIMARY KEY, user_id INTEGER, name TEXT);`,
		`INSERT INTO users(id, name) VALUES (7, 'john')`,
		`INSERT INTO accounts(id, user_id, name) VALUES (1, 7, 'a1')`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/users", Name: "Users"},
		Name: "Users",
		RootView: &spec.View{
			Source: &spec.ViewSource{SQL: "SELECT id, name FROM users WHERE id = :ID"},
		},
		Parameters: []*spec.Parameter{
			{Name: "ID", Source: spec.BindSource{Kind: "path", Name: "id"}},
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
	}

	type input struct{ ID int }
	type account struct {
		ID int
	}
	type row struct {
		ID       int
		Name     string
		Accounts []*account `view:"accounts" sql:"SELECT id FROM accounts WHERE user_id = :Missing" on:"ID:id=ID:id"`
	}
	type output struct {
		Data []*row
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

	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact),
		Artifact:   artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
		Scope:      testharness.Request{}.WithPathParams(map[string]string{"id": "7"}).WithQuery(url.Values{}),
	}

	if _, err := NewService().Read(context.Background(), session); err == nil {
		t.Fatalf("expected child relation build failure")
	}
}

func TestService_Read_CompositeSQLKeyWithoutGoField(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (tenant_id INTEGER, id INTEGER, name TEXT);`,
		`CREATE TABLE profiles (tenant_id INTEGER, user_id INTEGER, title TEXT);`,
		`INSERT INTO users(tenant_id, id, name) VALUES (1, 7, 'john')`,
		`INSERT INTO profiles(tenant_id, user_id, title) VALUES (1, 7, 'admin')`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/users", Name: "Users"},
		Name: "Users",
		RootView: &spec.View{
			Source: &spec.ViewSource{SQL: "SELECT tenant_id, id, name FROM users"},
		},
		Parameters: []*spec.Parameter{
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
	}

	type profile struct {
		TenantID int
		UserID   int
		Title    string
	}
	type row struct {
		TenantID int
		ID       int
		Name     string
		Profile  *profile
	}
	type output struct {
		Data []*row
	}
	type input struct{}

	artifact, err := buildArtifact(bootstrap.ArtifactInput{
		Component:       component,
		InputType:       reflect.TypeOf(input{}),
		OutputType:      reflect.TypeOf(output{}),
		DirectViewField: "Data",
	})
	if err != nil {
		t.Fatalf("artifact build failed: %v", err)
	}

	rowType := reflect.TypeOf(row{})
	profileType := reflect.TypeOf(profile{})
	rootView := &data.View{Spec: spec.View{Name: "Users",
		Source: component.RootView.Source}, Relations: []*data.Relation{{
		Name:        "Profile",
		Kind:        spec.RelationKindSubview,
		Holder:      "Profile",
		Cardinality: spec.CardinalityOne,
		On: data.Links{
			data.NewLink("", "tenant_id", "TenantID"),
			data.NewLink("", "id", "ID"),
		},
		Of: &data.RelationRef{
			View: &data.View{Spec: spec.View{Name: "profile",
				Source: &spec.ViewSource{SQL: "SELECT tenant_id, user_id, title FROM profiles"}}, Relations: []*data.Relation{},
			},
			On: data.Links{
				data.NewLink("", "tenant_id", "TenantID"),
				data.NewLink("", "user_id", "MissingUserID"),
			},
			MatchStrategy: data.MatchSequential,
		},
	}},
	}
	collection, err := rcollector.Compile(rootView, map[*data.View]reflect.Type{
		rootView:                      rowType,
		rootView.Relations[0].Of.View: profileType,
	})
	if err != nil {
		t.Fatalf("compile collector graph: %v", err)
	}
	artifact.Reader, err = sqlreader.NewPlan(sqlreader.PlanConfig{
		SelectorBindings: artifact.Reader.SelectorBindings, ViewIndex: sqlreader.NewViewIndex(component, rootView),
		OutputViewField: artifact.Reader.OutputViewField, DirectOutput: artifact.Reader.DirectOutput,
		RootView: rootView, Collection: collection,
	})
	if err != nil {
		t.Fatalf("assemble reader plan: %v", err)
	}

	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact),
		Artifact:   artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
		Scope:      testharness.Request{}.WithQuery(url.Values{}),
	}

	// MissingUserID is not a Go field, but user_id is present in SQL and
	// mapped to UserID. The SQL-backed join must still attach the child.
	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read SQL-backed composite relation: %v", err)
	}
	result := actual.(*output)
	if len(result.Data) != 1 || result.Data[0].Profile == nil || result.Data[0].Profile.UserID != 7 {
		t.Fatalf("expected attached profile, got %+v", result.Data)
	}
}

func TestService_Read_BindsNestedOneToOneSubview(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`,
		`CREATE TABLE profiles (id INTEGER PRIMARY KEY, user_id INTEGER, title TEXT);`,
		`CREATE TABLE profile_settings (id INTEGER PRIMARY KEY, profile_id INTEGER, theme TEXT);`,
		`INSERT INTO users(id, name) VALUES (7, 'john')`,
		`INSERT INTO profiles(id, user_id, title) VALUES (1, 7, 'admin')`,
		`INSERT INTO profile_settings(id, profile_id, theme) VALUES (9, 1, 'dark')`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/users", Name: "Users"},
		Name: "Users",
		RootView: &spec.View{
			Source: &spec.ViewSource{SQL: "SELECT id, name FROM users WHERE id = :ID"},
		},
		Parameters: []*spec.Parameter{
			{Name: "ID", Source: spec.BindSource{Kind: "path", Name: "id"}},
			{Name: "include", Source: spec.BindSource{Kind: "query", Name: "include"}},
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
	}

	type input struct {
		ID      int
		Include bool
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
		Settings *settings `view:"settings" sql:"#if($include) SELECT id, profile_id, theme FROM profile_settings WHERE 1=1 $View.ParentJoinOn(\"AND\",\"profile_id\") #else SELECT id, profile_id, theme FROM profile_settings WHERE 1=0 #end" on:"ID:id=ProfileID:profile_id"`
	}
	type row struct {
		ID      int
		Name    string
		Profile *profile `view:"profile" sql:"#if($include) SELECT id, user_id, title FROM profiles WHERE 1=1 $View.ParentJoinOn(\"AND\",\"user_id\") #else SELECT id, user_id, title FROM profiles WHERE 1=0 #end" on:"ID:id=UserID:user_id"`
	}
	type output struct {
		Data []*row
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

	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact),
		Artifact:   artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
		Scope: testharness.Request{}.WithPathParams(map[string]string{"id": "7"}).WithQuery(url.Values{
			"include": []string{"true"},
		}),
	}

	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	assertly.AssertValues(t, &output{
		Data: []*row{{
			ID:   7,
			Name: "john",
			Profile: &profile{
				ID:     1,
				UserID: 7,
				Title:  "admin",
				Settings: &settings{
					ID:        9,
					ProfileID: 1,
					Theme:     "dark",
				},
			},
		}},
	}, actual)
}
