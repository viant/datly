package reader

import (
	"context"
	"fmt"
	"net/url"
	"reflect"
	"testing"
	"time"

	"github.com/viant/afs"
	"github.com/viant/afs/option"
	"github.com/viant/assertly"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/data"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
	rsql "github.com/viant/datly/sql"
	"github.com/viant/sqlx/io/read/cache"
	cacheafs "github.com/viant/sqlx/io/read/cache/afs"
)

func TestService_RelationMatcherPreservesScalarWindowOnCacheReplay(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE cache_users (id INTEGER PRIMARY KEY);`,
		`CREATE TABLE cache_accounts (id INTEGER PRIMARY KEY, user_id INTEGER);`,
		`INSERT INTO cache_users(id) VALUES (1), (2);`,
		`INSERT INTO cache_accounts(id, user_id) VALUES (1, 1), (2, 1), (3, 2), (4, 2);`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	type input struct{}
	type account struct {
		ID     int
		UserID int
	}
	type user struct {
		ID       int
		Accounts []*account `view:"accounts" sql:"SELECT id, user_id FROM cache_accounts WHERE user_id IN (?) ORDER BY user_id, id" on:"ID:id=UserID:user_id"`
	}
	type output struct{ Data []*user }
	session := relationCacheSession(t, h, reflect.TypeOf(input{}), reflect.TypeOf(output{}), &spec.Component{
		Name:       "ScalarRelationCache",
		RootView:   &spec.View{Source: &spec.ViewSource{SQL: `SELECT id FROM cache_users ORDER BY id`}},
		Parameters: []*spec.Parameter{{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}}},
	})
	limit, offset := 1, 1
	session.Artifact.Root.View.Relations[0].Of.View.Spec.Source.Controls = &spec.ViewControls{Limit: &limit, Offset: &offset}

	want := &output{Data: []*user{
		{ID: 1, Accounts: []*account{{ID: 2, UserID: 1}}},
		{ID: 2, Accounts: []*account{{ID: 4, UserID: 2}}},
	}}
	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("cold read failed: %v", err)
	}
	assertly.AssertValues(t, want, actual)
	if _, err := h.DB.Exec(`DELETE FROM cache_accounts`); err != nil {
		t.Fatalf("delete child source: %v", err)
	}
	actual, err = NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("cache replay failed: %v", err)
	}
	assertly.AssertValues(t, want, actual)
}

func TestService_RelationMatcherPreservesCompositeWindowOnCacheReplay(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE cache_signals (id INTEGER PRIMARY KEY, feature_type TEXT, feature_value TEXT);`,
		`CREATE TABLE cache_performance (id INTEGER PRIMARY KEY, feature_type TEXT, value TEXT);`,
		`INSERT INTO cache_signals(id, feature_type, feature_value) VALUES (1, 'country', 'PL'), (2, 'country', 'US');`,
		`INSERT INTO cache_performance(id, feature_type, value) VALUES
			(1, 'country', 'PL'), (2, 'country', 'PL'),
			(3, 'country', 'US'), (4, 'country', 'US');`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	type input struct{}
	type performance struct {
		ID          int
		FeatureType string
		Value       string
	}
	type signal struct {
		ID           int
		FeatureType  string
		FeatureValue string
		Performances []*performance `view:"performances" sql:"SELECT id, feature_type, value FROM cache_performance WHERE $COLUMN_IN ORDER BY feature_type, value, id" on:"FeatureType:feature_type=FeatureType:feature_type,FeatureValue:feature_value=Value:value"`
	}
	type output struct{ Data []*signal }
	session := relationCacheSession(t, h, reflect.TypeOf(input{}), reflect.TypeOf(output{}), &spec.Component{
		Name:       "CompositeRelationCache",
		RootView:   &spec.View{Source: &spec.ViewSource{SQL: `SELECT id, feature_type, feature_value FROM cache_signals ORDER BY id`}},
		Parameters: []*spec.Parameter{{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}}},
	})
	limit, offset := 1, 1
	session.Artifact.Root.View.Relations[0].Of.View.Spec.Source.Controls = &spec.ViewControls{Limit: &limit, Offset: &offset}

	want := &output{Data: []*signal{
		{ID: 1, FeatureType: "country", FeatureValue: "PL", Performances: []*performance{{ID: 2, FeatureType: "country", Value: "PL"}}},
		{ID: 2, FeatureType: "country", FeatureValue: "US", Performances: []*performance{{ID: 4, FeatureType: "country", Value: "US"}}},
	}}
	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("cold read failed: %v", err)
	}
	assertly.AssertValues(t, want, actual)
	if _, err := h.DB.Exec(`DELETE FROM cache_performance`); err != nil {
		t.Fatalf("delete child source: %v", err)
	}
	actual, err = NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("cache replay failed: %v", err)
	}
	assertly.AssertValues(t, want, actual)
}

func TestService_RelationCacheReplaysNestedTypedViews(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE nested_users (id INTEGER PRIMARY KEY);`,
		`CREATE TABLE nested_accounts (id INTEGER PRIMARY KEY, user_id INTEGER);`,
		`CREATE TABLE nested_entries (id INTEGER PRIMARY KEY, account_id INTEGER);`,
		`INSERT INTO nested_users(id) VALUES (1), (2);`,
		`INSERT INTO nested_accounts(id, user_id) VALUES (1, 1), (2, 1), (3, 2), (4, 2);`,
		`INSERT INTO nested_entries(id, account_id) VALUES
			(1, 1), (2, 1), (3, 2), (4, 2),
			(5, 3), (6, 3), (7, 4), (8, 4);`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	type input struct{}
	type entry struct {
		ID        int
		AccountID int
	}
	type account struct {
		ID      int
		UserID  int
		Entries []*entry `view:"entries" sql:"SELECT id, account_id FROM nested_entries WHERE account_id IN (?) ORDER BY account_id, id" on:"ID:id=AccountID:account_id"`
	}
	type user struct {
		ID       int
		Accounts []*account `view:"accounts" sql:"SELECT id, user_id FROM nested_accounts WHERE user_id IN (?) ORDER BY user_id, id" on:"ID:id=UserID:user_id"`
	}
	type output struct{ Data []*user }
	session := relationCacheSession(t, h, reflect.TypeOf(input{}), reflect.TypeOf(output{}), &spec.Component{
		Name:       "NestedRelationCache",
		RootView:   &spec.View{Source: &spec.ViewSource{SQL: `SELECT id FROM nested_users ORDER BY id`}},
		Parameters: []*spec.Parameter{{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}}},
	})
	limit, offset := 1, 1
	accounts := session.Artifact.Root.View.Relations[0].Of.View
	accounts.Spec.Source.Controls = &spec.ViewControls{Limit: &limit, Offset: &offset}
	accounts.Relations[0].Of.View.Spec.Source.Controls = &spec.ViewControls{Limit: &limit, Offset: &offset}

	want := &output{Data: []*user{
		{ID: 1, Accounts: []*account{{ID: 2, UserID: 1, Entries: []*entry{{ID: 4, AccountID: 2}}}}},
		{ID: 2, Accounts: []*account{{ID: 4, UserID: 2, Entries: []*entry{{ID: 8, AccountID: 4}}}}},
	}}
	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("cold read failed: %v", err)
	}
	assertly.AssertValues(t, want, actual)
	if _, err := h.DB.Exec(`DELETE FROM nested_entries; DELETE FROM nested_accounts`); err != nil {
		t.Fatalf("delete relation sources: %v", err)
	}
	actual, err = NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("nested cache replay failed: %v", err)
	}
	assertly.AssertValues(t, want, actual)
}

func TestService_RelationCacheIdentityIsScopedToPreparedView(t *testing.T) {
	root := testharness.NewSQLiteHarness(t)
	left := testharness.NewSQLiteHarness(t)
	right := testharness.NewSQLiteHarness(t)
	if err := root.ExecStatements(context.Background(),
		`CREATE TABLE scoped_users (id INTEGER PRIMARY KEY);`,
		`INSERT INTO scoped_users(id) VALUES (1);`,
	); err != nil {
		t.Fatalf("root setup failed: %v", err)
	}
	for _, setup := range []struct {
		harness *testharness.Harness
		id      int
	}{
		{harness: left, id: 10},
		{harness: right, id: 20},
	} {
		if err := setup.harness.ExecStatements(context.Background(),
			`CREATE TABLE scoped_items (id INTEGER PRIMARY KEY, user_id INTEGER);`,
			fmt.Sprintf(`INSERT INTO scoped_items(id, user_id) VALUES (%d, 1);`, setup.id),
		); err != nil {
			t.Fatalf("connector setup failed: %v", err)
		}
	}
	type input struct{}
	type item struct {
		ID     int
		UserID int
	}
	type user struct {
		ID    int
		Left  []*item `view:"left,connector=left" sql:"SELECT id, user_id FROM scoped_items WHERE user_id IN (?)" on:"ID:id=UserID:user_id"`
		Right []*item `view:"right,connector=right" sql:"SELECT id, user_id FROM scoped_items WHERE user_id IN (?)" on:"ID:id=UserID:user_id"`
	}
	type output struct{ Data []*user }
	component := &spec.Component{
		Name:       "ScopedRelationCaches",
		RootView:   &spec.View{Source: &spec.ViewSource{SQL: `SELECT id FROM scoped_users`}},
		Parameters: []*spec.Parameter{{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}}},
	}
	artifact, err := buildArtifact(bootstrap.ArtifactInput{
		Component: component, InputType: reflect.TypeOf(input{}), OutputType: reflect.TypeOf(output{}), DirectViewField: "Data",
	})
	if err != nil {
		t.Fatalf("artifact build failed: %v", err)
	}
	sqlComponent := &rsql.SQLComponent{DB: root.DB}
	if err := sqlComponent.RegisterConnector("left", left.DB); err != nil {
		t.Fatalf("register left connector: %v", err)
	}
	if err := sqlComponent.RegisterConnector("right", right.DB); err != nil {
		t.Fatalf("register right connector: %v", err)
	}
	cacheRoot := "mem://localhost/" + t.Name()
	cacheFS := afs.New()
	for _, location := range []string{cacheRoot, cacheRoot + "/left", cacheRoot + "/right"} {
		if err := cacheFS.Create(context.Background(), location, 0o755, true); err != nil {
			t.Fatalf("create cache directory %s: %v", location, err)
		}
	}
	leftCache, err := cacheafs.NewCache(cacheRoot+"/left/", time.Hour, t.Name()+"-left", option.NewStream(0, 0))
	if err != nil {
		t.Fatalf("create left cache: %v", err)
	}
	rightCache, err := cacheafs.NewCache(cacheRoot+"/right/", time.Hour, t.Name()+"-right", option.NewStream(0, 0))
	if err != nil {
		t.Fatalf("create right cache: %v", err)
	}
	leftView := artifact.Reader.Root.View.Relations[0].Of.View
	rightView := artifact.Reader.Root.View.Relations[1].Of.View
	session := &Session{
		Component: component, OutputType: reflect.TypeOf(output{}), Input: routeInput(t, artifact), Artifact: artifact.Reader,
		SQL: sqlComponent, ReadCaches: map[*data.View]cache.Cache{leftView: leftCache, rightView: rightCache},
		Scope: testharness.Request{}.WithQuery(url.Values{}),
	}
	want := &output{Data: []*user{{ID: 1, Left: []*item{{ID: 10, UserID: 1}}, Right: []*item{{ID: 20, UserID: 1}}}}}
	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("cold read failed: %v", err)
	}
	assertly.AssertValues(t, want, actual)
	if _, err := left.DB.Exec(`DELETE FROM scoped_items`); err != nil {
		t.Fatalf("delete left source: %v", err)
	}
	if _, err := right.DB.Exec(`DELETE FROM scoped_items`); err != nil {
		t.Fatalf("delete right source: %v", err)
	}
	actual, err = NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("scoped cache replay failed: %v", err)
	}
	assertly.AssertValues(t, want, actual)
}

func relationCacheSession(t *testing.T, h *testharness.Harness, inputType, outputType reflect.Type, component *spec.Component) *Session {
	t.Helper()
	artifact, err := buildArtifact(bootstrap.ArtifactInput{
		Component: component, InputType: inputType, OutputType: outputType, DirectViewField: "Data",
	})
	if err != nil {
		t.Fatalf("artifact build failed: %v", err)
	}
	readCache, err := cacheafs.NewCache("mem://localhost/"+t.Name()+"/", time.Hour, t.Name(), option.NewStream(0, 0))
	if err != nil {
		t.Fatalf("create sqlx cache: %v", err)
	}
	session := &Session{
		Component: component, OutputType: outputType, Input: routeInput(t, artifact), Artifact: artifact.Reader,
		SQL: &rsql.SQLComponent{DB: h.DB}, ReadCaches: map[*data.View]cache.Cache{},
		Scope: testharness.Request{}.WithQuery(url.Values{}),
	}
	var register func(*data.View)
	register = func(view *data.View) {
		if view == nil {
			return
		}
		session.ReadCaches[view] = readCache
		for _, relation := range view.Relations {
			if relation != nil && relation.Of != nil {
				register(relation.Of.View)
			}
		}
	}
	register(artifact.Reader.Root.View)
	return session
}
