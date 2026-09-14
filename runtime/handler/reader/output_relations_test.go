package reader

import (
	"context"
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
	"github.com/viant/sqlx/io/read/cache"
	cacheafs "github.com/viant/sqlx/io/read/cache/afs"
	xreader "github.com/viant/xdatly/reader"
	xstate "github.com/viant/xdatly/state"
)

type partitionedOutputTotal struct {
	Count int `sqlx:"count"`
}

type outputTotalPartitioner struct{}

func (*outputTotalPartitioner) Partitions(context.Context, xreader.PartitionRequest) ([]xreader.Partition, error) {
	return []xreader.Partition{
		{Expression: "id <= ?", Args: []any{2}},
		{Expression: "id > ?", Args: []any{2}},
	}, nil
}

func (*outputTotalPartitioner) Reducer(context.Context) xreader.Reducer {
	return outputTotalReducer{}
}

type outputTotalReducer struct{}

func (outputTotalReducer) Reduce(_ context.Context, rows any) (any, error) {
	result := partitionedOutputTotal{}
	for _, row := range rows.([]partitionedOutputTotal) {
		result.Count += row.Count
	}
	return result, nil
}

func TestService_Read_BindsMultipleTypedOutputRelations(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`,
		`INSERT INTO users(id, name) VALUES (1, 'one'), (2, 'two'), (3, 'three');`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	type input struct {
		MinID int
	}
	type row struct {
		ID int `sqlx:"id"`
	}
	type totals struct {
		Count int `sqlx:"count"`
	}
	type bounds struct {
		MinID int `sqlx:"min_id"`
		MaxID int `sqlx:"max_id"`
	}
	type output struct {
		Data   []*row
		Totals totals
		Bounds *bounds
	}
	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo", Name: "Users"},
		Name: "Users",
		RootView: &spec.View{
			Key:    spec.Key{Kind: spec.KindView, Scope: "example.com/demo", Name: "Users"},
			Name:   "Users",
			Source: &spec.ViewSource{SQL: "SELECT id FROM users WHERE id >= :MinID ORDER BY id"},
			Relations: []*spec.Relation{
				outputRelation("Totals", "SELECT COUNT(*) AS count FROM ($View.Users.NonWindowSQL) parent"),
				outputRelation("Bounds", "SELECT MIN(id) AS min_id, MAX(id) AS max_id FROM ($View.Users.NonWindowSQL) parent"),
			},
		},
		Parameters: []*spec.Parameter{
			{Name: "MinID", Source: spec.BindSource{Kind: "query", Name: "min_id"}},
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
			{Name: "Totals", Source: spec.BindSource{Kind: "output", Name: "summary"}},
			{Name: "Bounds", Source: spec.BindSource{Kind: "output", Name: "summary"}},
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
	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact), Artifact: artifact.Reader,
		SQL:       &rsql.SQLComponent{DB: h.DB},
		Scope:     testharness.Request{}.WithQuery(url.Values{"min_id": []string{"2"}}),
		Providers: rootSelectors(xstate.Selector{Limit: 1}),
	}

	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	assertly.AssertValues(t, &output{
		Data:   []*row{{ID: 2}},
		Totals: totals{Count: 2},
		Bounds: &bounds{MinID: 2, MaxID: 3},
	}, actual)
}

func TestService_Read_ReducesPartitionedOutputRelation(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE partitioned_output_users (id INTEGER PRIMARY KEY);`,
		`INSERT INTO partitioned_output_users(id) VALUES (1), (2), (3), (4);`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	type input struct{}
	type row struct {
		ID int `sqlx:"id"`
	}
	type output struct {
		Data   []*row
		Totals partitionedOutputTotal
	}
	totals := outputRelation("Totals", "SELECT COUNT(*) AS count FROM partitioned_output_users")
	totals.View.Partitioning = &spec.Partitioning{Type: "example.OutputTotalPartitioner", Concurrency: 2}
	component := &spec.Component{
		Name: "PartitionedOutput",
		RootView: &spec.View{
			Name: "Users", Source: &spec.ViewSource{SQL: "SELECT id FROM partitioned_output_users ORDER BY id"},
			Relations: []*spec.Relation{totals},
		},
		Parameters: []*spec.Parameter{
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
			{Name: "Totals", Source: spec.BindSource{Kind: "output", Name: "summary"}},
		},
	}
	artifact, err := buildArtifact(bootstrap.ArtifactInput{
		Component: component, InputType: reflect.TypeOf(input{}), OutputType: reflect.TypeOf(output{}), DirectViewField: "Data",
		Types: testTypeCatalog(t, map[string]reflect.Type{"example.OutputTotalPartitioner": reflect.TypeOf(outputTotalPartitioner{})}),
	})
	if err != nil {
		t.Fatalf("artifact build failed: %v", err)
	}
	session := &Session{
		Component: component, OutputType: reflect.TypeOf(output{}), Input: routeInput(t, artifact), Artifact: artifact.Reader,
		SQL: &rsql.SQLComponent{DB: h.DB}, Scope: testharness.Request{}.WithQuery(url.Values{}),
	}
	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	assertly.AssertValues(t, &output{
		Data: []*row{{ID: 1}, {ID: 2}, {ID: 3}, {ID: 4}}, Totals: partitionedOutputTotal{Count: 4},
	}, actual)
}

func TestBuildArtifact_RejectsPartitionedOutputRelationWithoutReducer(t *testing.T) {
	type input struct{}
	type row struct{ ID int }
	type output struct {
		Data   []*row
		Totals partitionedOutputTotal
	}
	totals := outputRelation("Totals", "SELECT COUNT(*) AS count FROM users")
	totals.View.Partitioning = &spec.Partitioning{Type: "example.RangePartitioner"}
	component := &spec.Component{
		Name: "InvalidPartitionedOutput",
		RootView: &spec.View{
			Name: "Users", Source: &spec.ViewSource{SQL: "SELECT id FROM users"}, Relations: []*spec.Relation{totals},
		},
		Parameters: []*spec.Parameter{
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
			{Name: "Totals", Source: spec.BindSource{Kind: "output", Name: "summary"}},
		},
	}
	_, err := buildArtifact(bootstrap.ArtifactInput{
		Component: component, InputType: reflect.TypeOf(input{}), OutputType: reflect.TypeOf(output{}), DirectViewField: "Data",
		Types: testTypeCatalog(t, map[string]reflect.Type{"example.RangePartitioner": reflect.TypeOf(rangePartitioner{})}),
	})
	if err == nil || !strings.Contains(err.Error(), "requires a reducer") {
		t.Fatalf("expected partitioned output reducer validation, got %v", err)
	}
}

func TestService_Read_OutputRelationReceivesParentSelectorWindow(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE window_users (id INTEGER PRIMARY KEY);`,
		`INSERT INTO window_users(id) VALUES (1), (2), (3);`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	type input struct{}
	type row struct {
		ID int `sqlx:"id"`
	}
	type window struct {
		Limit  int `sqlx:"limit_value"`
		Offset int `sqlx:"offset_value"`
		Page   int `sqlx:"page_value"`
		Count  int `sqlx:"count"`
	}
	type output struct {
		Data   []*row
		Window window
	}
	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo", Name: "WindowUsers"},
		Name: "WindowUsers",
		RootView: &spec.View{
			Key:    spec.Key{Kind: spec.KindView, Scope: "example.com/demo", Name: "WindowUsers"},
			Name:   "WindowUsers",
			Source: &spec.ViewSource{SQL: "SELECT id FROM window_users ORDER BY id"},
			Selector: &spec.Selector{
				AllowPage: true, DefaultLimit: 1,
			},
			Relations: []*spec.Relation{outputRelation("Window", `SELECT $View.Limit AS limit_value, $View.Offset AS offset_value, $View.Page AS page_value,
COUNT(*) AS count FROM ($View.WindowUsers.NonWindowSQL) parent`)},
		},
		Parameters: []*spec.Parameter{
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
			{Name: "Window", Source: spec.BindSource{Kind: "output", Name: "summary"}},
		},
	}
	artifact, err := buildArtifact(bootstrap.ArtifactInput{
		Component: component, InputType: reflect.TypeOf(input{}), OutputType: reflect.TypeOf(output{}), DirectViewField: "Data",
	})
	if err != nil {
		t.Fatalf("artifact build failed: %v", err)
	}
	session := &Session{
		Component: component, OutputType: reflect.TypeOf(output{}), Input: routeInput(t, artifact), Artifact: artifact.Reader,
		SQL: &rsql.SQLComponent{DB: h.DB}, Scope: testharness.Request{}.WithQuery(url.Values{}),
		Providers: rootSelectors(xstate.Selector{Page: 2}),
	}
	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	assertly.AssertValues(t, &output{
		Data:   []*row{{ID: 2}},
		Window: window{Limit: 1, Offset: 1, Page: 2, Count: 3},
	}, actual)
}

func TestService_Read_BindsTypedOutputRelationFromTableSource(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`,
		`INSERT INTO users(id, name) VALUES (1, 'one'), (2, 'two'), (3, 'three');`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	type input struct{}
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
			Key:       spec.Key{Kind: spec.KindView, Scope: "example.com/demo", Name: "Users"},
			Name:      "Users",
			Source:    &spec.ViewSource{Table: "users"},
			Relations: []*spec.Relation{outputRelation("Totals", "SELECT COUNT(*) AS count FROM ($View.Users.NonWindowSQL) parent")},
		},
		Parameters: []*spec.Parameter{
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
	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact), Artifact: artifact.Reader,
		SQL:       &rsql.SQLComponent{DB: h.DB},
		Scope:     testharness.Request{}.WithQuery(url.Values{}),
		Providers: rootSelectors(xstate.Selector{Limit: 1}),
	}

	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	assertly.AssertValues(t, &output{
		Data:   []*row{{ID: 1, Name: "one"}},
		Totals: totals{Count: 3},
	}, actual)
}

func TestService_Read_AppliesSelectorToPlainOutputRelationSQL(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`,
		`INSERT INTO users(id, name) VALUES (1, 'one'), (2, 'two'), (3, 'three');`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	type input struct{}
	type row struct {
		ID   int    `sqlx:"id"`
		Name string `sqlx:"name"`
	}
	type output struct {
		Data   []*row
		Latest row
	}
	component := &spec.Component{
		Name: "Users",
		RootView: &spec.View{
			Name:      "Users",
			Source:    &spec.ViewSource{SQL: "SELECT id, name FROM users ORDER BY id"},
			Relations: []*spec.Relation{outputRelation("Latest", "SELECT id, name FROM users")},
		},
		Parameters: []*spec.Parameter{
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
			{Name: "Latest", Source: spec.BindSource{Kind: "output", Name: "summary"}},
		},
	}
	artifact, err := buildArtifact(bootstrap.ArtifactInput{
		Component: component, InputType: reflect.TypeOf(input{}), OutputType: reflect.TypeOf(output{}), DirectViewField: "Data",
	})
	if err != nil {
		t.Fatalf("artifact build failed: %v", err)
	}
	latestView := artifact.Reader.Root.View.Relations[0].Of.View
	latestView.Spec.Selector = &spec.Selector{AllowOrderBy: true, AllowLimit: true}
	session := &Session{
		Component: component, OutputType: reflect.TypeOf(output{}), Input: routeInput(t, artifact), Artifact: artifact.Reader,
		SQL: &rsql.SQLComponent{DB: h.DB}, Scope: testharness.Request{}.WithQuery(url.Values{}),
		Providers: selectorProviders(xstate.Selectors{&xstate.NamedSelector{
			Name: latestView.Spec.Name, Selector: xstate.Selector{OrderBy: "id DESC", Limit: 1},
		}}),
	}

	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	assertly.AssertValues(t, &output{
		Data:   []*row{{ID: 1, Name: "one"}, {ID: 2, Name: "two"}, {ID: 3, Name: "three"}},
		Latest: row{ID: 3, Name: "three"},
	}, actual)
}

func TestService_Read_ReplaysRootAndOutputRelationFromSQLXCache(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	if err := h.ExecStatements(ctx,
		`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`,
		`INSERT INTO users(id, name) VALUES (1, 'one'), (2, 'two'), (3, 'three');`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	type input struct{}
	type row struct {
		ID int `sqlx:"id"`
	}
	type totals struct {
		Count int `sqlx:"count"`
	}
	type output struct {
		Data   []*row
		Totals totals
	}
	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/cache", Name: "UsersWithTotals"},
		Name: "UsersWithTotals",
		RootView: &spec.View{
			Key:       spec.Key{Kind: spec.KindView, Scope: "example.com/demo/cache", Name: "Users"},
			Name:      "Users",
			Source:    &spec.ViewSource{SQL: "SELECT id FROM users ORDER BY id"},
			Relations: []*spec.Relation{outputRelation("Totals", "SELECT COUNT(*) AS count FROM ($View.Users.NonWindowSQL) parent")},
		},
		Parameters: []*spec.Parameter{
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
	readCache, err := cacheafs.NewCache("mem://localhost/datly-output-relation-sqlx-cache/", time.Hour, t.Name(), option.NewStream(0, 0))
	if err != nil {
		t.Fatalf("create sqlx cache failed: %v", err)
	}
	session := &Session{
		Component:  component,
		OutputType: reflect.TypeOf(output{}),
		Input:      routeInput(t, artifact), Artifact: artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
		Scope:      testharness.Request{}.WithQuery(url.Values{}),
		ReadCaches: map[*data.View]cache.Cache{},
	}
	session.ReadCaches[artifact.Reader.Root.View] = readCache
	for _, relation := range artifact.Reader.Root.View.Relations {
		if relation != nil && relation.Of != nil && relation.Of.View != nil {
			session.ReadCaches[relation.Of.View] = readCache
		}
	}

	service := NewService()
	first, err := service.Read(ctx, session)
	if err != nil {
		t.Fatalf("initial read failed: %v", err)
	}
	want := &output{Data: []*row{{ID: 1}, {ID: 2}, {ID: 3}}, Totals: totals{Count: 3}}
	assertly.AssertValues(t, want, first)
	if err := h.ExecStatements(ctx,
		`DELETE FROM users;`,
		`INSERT INTO users(id, name) VALUES (4, 'four');`,
	); err != nil {
		t.Fatalf("mutate source rows failed: %v", err)
	}
	second, err := service.Read(ctx, session)
	if err != nil {
		t.Fatalf("cached read failed: %v", err)
	}
	assertly.AssertValues(t, want, second)
}

func outputRelation(name, sqlText string) *spec.Relation {
	return &spec.Relation{
		Name: name, Kind: spec.RelationKindDerived, Holder: name, Cardinality: spec.CardinalityOne,
		View: &spec.View{Name: name, Source: &spec.ViewSource{SQL: sqlText}},
	}
}
