package reader

import (
	"context"
	"net/url"
	"reflect"
	"testing"

	"github.com/viant/assertly"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
	rsql "github.com/viant/datly/sql"
	"github.com/viant/datly/transcribe"
	xstate "github.com/viant/xdatly/state"
)

func TestService_Read_TableSourceWithTableRelation_SQLite(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, full_name TEXT);`,
		`CREATE TABLE accounts (id INTEGER PRIMARY KEY, user_id INTEGER, label TEXT);`,
		`INSERT INTO users(id, full_name) VALUES (1, 'john'), (2, 'jane');`,
		`INSERT INTO accounts(id, user_id, label) VALUES (10, 1, 'primary'), (20, 2, 'other');`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/users", Name: "Users"},
		Name: "Users",
		RootView: &spec.View{
			Source:   &spec.ViewSource{Table: "users"},
			Selector: &spec.Selector{AllowFields: true, DefaultOrder: "id ASC"},
		},
		Parameters: []*spec.Parameter{{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}}},
	}
	type input struct{}
	type account struct {
		ID     int    `sqlx:"id"`
		UserID int    `sqlx:"user_id"`
		Label  string `sqlx:"label"`
	}
	type row struct {
		ID       int        `sqlx:"id"`
		Name     string     `sqlx:"full_name"`
		Accounts []*account `view:"accounts,table=accounts" on:"ID:id=UserID:user_id"`
	}
	type output struct{ Data []*row }

	artifact, err := buildArtifact(bootstrap.ArtifactInput{
		Component: component, InputType: reflect.TypeOf(input{}), OutputType: reflect.TypeOf(output{}), DirectViewField: "Data",
	})
	if err != nil {
		t.Fatalf("artifact build failed: %v", err)
	}
	session := &Session{
		Component: component, OutputType: reflect.TypeOf(output{}), Input: routeInput(t, artifact), Artifact: artifact.Reader,
		SQL: &rsql.SQLComponent{DB: h.DB}, Scope: testharness.Request{}.WithQuery(url.Values{}),
		Providers: rootSelectors(xstate.Selector{Fields: []string{"ID", "full_name", "Accounts"}}),
	}
	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	assertly.AssertValues(t, &output{Data: []*row{
		{ID: 1, Name: "john", Accounts: []*account{{ID: 10, UserID: 1, Label: "primary"}}},
		{ID: 2, Name: "jane", Accounts: []*account{{ID: 20, UserID: 2, Label: "other"}}},
	}}, actual)
}

func TestService_Read_AppliesSelectorsToExactViewScope_SQLite(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, full_name TEXT);`,
		`CREATE TABLE accounts (id INTEGER PRIMARY KEY, user_id INTEGER, label TEXT);`,
		`INSERT INTO users(id, full_name) VALUES (1, 'john'), (2, 'jane');`,
		`INSERT INTO accounts(id, user_id, label) VALUES (10, 1, 'first'), (20, 1, 'second'), (30, 1, 'third'), (40, 2, 'other');`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/users", Name: "Users"},
		Name: "Users",
		RootView: &spec.View{
			Name:     "users",
			Source:   &spec.ViewSource{Table: "users"},
			Selector: &spec.Selector{AllowFields: true, AllowOrderBy: true, AllowLimit: true},
		},
		Parameters: []*spec.Parameter{{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}}},
	}
	type input struct{}
	type account struct {
		ID     int    `sqlx:"id"`
		UserID int    `sqlx:"user_id"`
		Label  string `sqlx:"label"`
	}
	type row struct {
		ID       int        `sqlx:"id"`
		Name     string     `sqlx:"full_name"`
		Accounts []*account `view:"accounts,table=accounts" on:"ID:id=UserID:user_id"`
	}
	type output struct{ Data []*row }

	artifact, err := buildArtifact(bootstrap.ArtifactInput{
		Component: component, InputType: reflect.TypeOf(input{}), OutputType: reflect.TypeOf(output{}), DirectViewField: "Data",
	})
	if err != nil {
		t.Fatalf("artifact build failed: %v", err)
	}
	childView := artifact.Reader.Root.View.Relations[0].Of.View
	childView.Spec.Selector = &spec.Selector{AllowFields: true, AllowOrderBy: true, AllowLimit: true}
	session := &Session{
		Component: component, OutputType: reflect.TypeOf(output{}), Input: routeInput(t, artifact), Artifact: artifact.Reader,
		SQL: &rsql.SQLComponent{DB: h.DB}, Scope: testharness.Request{}.WithQuery(url.Values{}),
		Providers: selectorProviders(xstate.Selectors{
			&xstate.NamedSelector{Name: "users", Selector: xstate.Selector{
				Fields: []string{"ID", "full_name", "accounts"}, OrderBy: "id ASC", Limit: 1,
			}},
			&xstate.NamedSelector{Name: "accounts", Selector: xstate.Selector{
				Fields: []string{"ID", "Label"}, OrderBy: "id DESC", Limit: 2,
			}},
		}),
	}

	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	assertly.AssertValues(t, &output{Data: []*row{{
		ID: 1, Name: "john", Accounts: []*account{
			{ID: 30, UserID: 1, Label: "third"},
			{ID: 20, UserID: 1, Label: "second"},
		},
	}}}, actual)
}

func TestService_Read_BindsDQLQuerySelectorsByView_SQLite(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, full_name TEXT);`,
		`CREATE TABLE accounts (id INTEGER PRIMARY KEY, user_id INTEGER, label TEXT);`,
		`INSERT INTO users(id, full_name) VALUES (1, 'john'), (2, 'jane');`,
		`INSERT INTO accounts(id, user_id, label) VALUES (10, 1, 'first'), (20, 1, 'second'), (30, 1, 'third');`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	compiled, err := transcribe.NewCompiler().Compile(context.Background(), &transcribe.Source{
		Scope: "example.com/demo/users",
		Name:  "Users",
		Text: `
#setting($_ = $route('/users', 'GET'))
#define($_ = $Fields<[]string>(query/fields).Optional().QuerySelector('users'))
#define($_ = $Limit<int>(query/childLimit).Optional().QuerySelector('accounts'))
#define($_ = $Data<?>(output/view))
SELECT id, full_name FROM users`,
	})
	if err != nil {
		t.Fatalf("parse component failed: %v", err)
	}
	component := compiled.Component
	component.RootView.Selector = &spec.Selector{
		AllowFields: true, DefaultOrder: "id ASC", DefaultLimit: 1,
	}
	type input struct {
		Fields []string
		Limit  int
	}
	type account struct {
		ID     int    `sqlx:"id"`
		UserID int    `sqlx:"user_id"`
		Label  string `sqlx:"label"`
	}
	type row struct {
		ID       int        `sqlx:"id"`
		Name     string     `sqlx:"full_name"`
		Accounts []*account `view:"accounts,table=accounts" on:"ID:id=UserID:user_id"`
	}
	type output struct{ Data []*row }
	artifact, err := buildArtifact(bootstrap.ArtifactInput{
		Component: component, InputType: reflect.TypeOf(input{}), OutputType: reflect.TypeOf(output{}), DirectViewField: "Data",
	})
	if err != nil {
		t.Fatalf("artifact build failed: %v", err)
	}
	artifact.Reader.Root.View.Relations[0].Of.View.Spec.Selector = &spec.Selector{
		AllowLimit: true, DefaultOrder: "id DESC",
	}
	session := &Session{
		Component: component, OutputType: reflect.TypeOf(output{}), Input: routeInput(t, artifact), Artifact: artifact.Reader,
		SQL: &rsql.SQLComponent{DB: h.DB},
		Scope: testharness.Request{}.WithQuery(url.Values{
			"fields":     []string{"id", "full_name", "Accounts"},
			"childLimit": []string{"2"},
		}),
	}

	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	assertly.AssertValues(t, &output{Data: []*row{{
		ID: 1, Name: "john", Accounts: []*account{
			{ID: 30, UserID: 1, Label: "third"},
			{ID: 20, UserID: 1, Label: "second"},
		},
	}}}, actual)
}
