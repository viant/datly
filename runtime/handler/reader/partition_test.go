package reader

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"path/filepath"
	"reflect"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mattn/go-sqlite3"
	"github.com/viant/afs/option"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/data"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
	rsql "github.com/viant/datly/sql"
	"github.com/viant/sqlx/io/read/cache"
	cacheafs "github.com/viant/sqlx/io/read/cache/afs"
	xreader "github.com/viant/xdatly/reader"
)

type rangePartitioner struct{}

func (*rangePartitioner) Partitions(_ context.Context, request xreader.PartitionRequest) ([]xreader.Partition, error) {
	if request.View == "accounts" {
		return []xreader.Partition{{Expression: "id <= ?", Args: []any{20}}, {Expression: "id > ?", Args: []any{20}}}, nil
	}
	return []xreader.Partition{{Expression: "id <= ?", Args: []any{2}}, {Expression: "id > ?", Args: []any{2}}}, nil
}

type partitionAccount struct {
	ID     int
	UserID int
}

type partitionUser struct {
	ID       int
	Accounts []*partitionAccount `view:"accounts,partitioner=example.RangePartitioner,concurrency=1" sql:"SELECT id, user_id FROM partition_accounts WHERE user_id IN (?) ORDER BY id" on:"ID:id=UserID:user_id"`
}

type partitionOutput struct {
	Data []*partitionUser
}

func TestService_ReadsRootAndChildPartitionsThroughSQLXCache(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE partition_users (id INTEGER PRIMARY KEY);`,
		`CREATE TABLE partition_accounts (id INTEGER PRIMARY KEY, user_id INTEGER);`,
		`INSERT INTO partition_users(id) VALUES (1), (2), (3), (4);`,
		`INSERT INTO partition_accounts(id, user_id) VALUES (10, 1), (20, 2), (30, 3), (40, 4);`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	type input struct{}
	component := &spec.Component{
		Name: "PartitionedUsers",
		RootView: &spec.View{
			Name:         "users",
			Source:       &spec.ViewSource{SQL: `SELECT id FROM partition_users ORDER BY id`},
			Partitioning: &spec.Partitioning{Type: "example.RangePartitioner", Concurrency: 1},
		},
		Parameters: []*spec.Parameter{{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}}},
	}
	artifact, err := buildArtifact(bootstrap.ArtifactInput{
		Component: component, InputType: reflect.TypeOf(input{}), OutputType: reflect.TypeOf(partitionOutput{}), DirectViewField: "Data",
		Types: testTypeCatalog(t, map[string]reflect.Type{"example.RangePartitioner": reflect.TypeOf(rangePartitioner{})}),
	})
	if err != nil {
		t.Fatalf("artifact build failed: %v", err)
	}
	readCache, err := cacheafs.NewCache("mem://localhost/"+t.Name()+"/", time.Hour, t.Name(), option.NewStream(0, 0))
	if err != nil {
		t.Fatalf("create SQLx cache: %v", err)
	}
	session := &Session{
		Component: component, OutputType: reflect.TypeOf(partitionOutput{}), Input: routeInput(t, artifact), Artifact: artifact.Reader,
		SQL: &rsql.SQLComponent{DB: h.DB}, Scope: testharness.Request{}.WithQuery(url.Values{}), ReadCaches: map[*data.View]cache.Cache{},
	}
	registerViewCaches(session, artifact.Reader.Root.View, readCache)

	assertPartitionOutput(t, readPartitionOutput(t, session))
	if _, err := h.DB.Exec(`DELETE FROM partition_accounts; DELETE FROM partition_users`); err != nil {
		t.Fatalf("delete source rows: %v", err)
	}
	assertPartitionOutput(t, readPartitionOutput(t, session))
}

func registerViewCaches(session *Session, view *data.View, service cache.Cache) {
	if view == nil {
		return
	}
	session.ReadCaches[view] = service
	for _, relation := range view.Relations {
		if relation != nil && relation.Of != nil {
			registerViewCaches(session, relation.Of.View, service)
		}
	}
}

func readPartitionOutput(t *testing.T, session *Session) *partitionOutput {
	t.Helper()
	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("partition read failed: %v", err)
	}
	return actual.(*partitionOutput)
}

func assertPartitionOutput(t *testing.T, actual *partitionOutput) {
	t.Helper()
	if len(actual.Data) != 4 {
		t.Fatalf("unexpected root partition rows: %#v", actual)
	}
	for i, user := range actual.Data {
		if user.ID != i+1 || len(user.Accounts) != 1 || user.Accounts[0].UserID != user.ID {
			t.Fatalf("unexpected partition relation at %d: user=%#v output=%#v", i, user, actual)
		}
	}
}

type reducingPartitioner struct{}

func (*reducingPartitioner) Partitions(context.Context, xreader.PartitionRequest) ([]xreader.Partition, error) {
	return []xreader.Partition{{Expression: "id >= ?", Args: []any{1}}, {Expression: "id >= ?", Args: []any{1}}}, nil
}

func (*reducingPartitioner) Reducer(context.Context) xreader.Reducer { return partitionReducer{} }

type partitionReducer struct{}

type reducedRow struct {
	ID int
}

func (partitionReducer) Reduce(_ context.Context, rows any) (any, error) {
	items := rows.([]reducedRow)
	byID := map[int]reducedRow{}
	for _, item := range items {
		byID[item.ID] = item
	}
	result := make([]reducedRow, 0, len(byID))
	for _, item := range byID {
		result = append(result, item)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result, nil
}

func TestService_PartitionReducerReceivesTypedRows(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(), `CREATE TABLE reduced_users (id INTEGER PRIMARY KEY);`, `INSERT INTO reduced_users(id) VALUES (1), (2);`); err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	type input struct{}
	type output struct{ Data []*reducedRow }
	component := &spec.Component{Name: "Reduced", RootView: &spec.View{Name: "reduced", Source: &spec.ViewSource{SQL: `SELECT id FROM reduced_users`}, Partitioning: &spec.Partitioning{Type: "example.Reducer"}}, Parameters: []*spec.Parameter{{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}}}}
	artifact, err := buildArtifact(bootstrap.ArtifactInput{
		Component: component, InputType: reflect.TypeOf(input{}), OutputType: reflect.TypeOf(output{}), DirectViewField: "Data",
		Types: testTypeCatalog(t, map[string]reflect.Type{"example.Reducer": reflect.TypeOf(reducingPartitioner{})}),
	})
	if err != nil {
		t.Fatalf("artifact build failed: %v", err)
	}
	session := &Session{Component: component, OutputType: reflect.TypeOf(output{}), Input: routeInput(t, artifact), Artifact: artifact.Reader, SQL: &rsql.SQLComponent{DB: h.DB}, Scope: testharness.Request{}.WithQuery(url.Values{})}
	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("partition read failed: %v", err)
	}
	result := actual.(*output)
	if len(result.Data) != 2 || result.Data[0].ID != 1 || result.Data[1].ID != 2 {
		t.Fatalf("unexpected reduced rows: %#v", result)
	}
}

type failingPartitioner struct{}

func (*failingPartitioner) Partitions(context.Context, xreader.PartitionRequest) ([]xreader.Partition, error) {
	result := make([]xreader.Partition, 8)
	for i := range result {
		result[i] = xreader.Partition{Expression: "id = ?", Args: []any{i + 1}}
	}
	return result, nil
}

type partitionProbe struct {
	active  atomic.Int32
	maximum atomic.Int32
	started atomic.Int32
	release chan struct{}
}

func (p *partitionProbe) read(id int64) (int64, error) {
	active := p.active.Add(1)
	defer p.active.Add(-1)
	for maximum := p.maximum.Load(); active > maximum && !p.maximum.CompareAndSwap(maximum, active); maximum = p.maximum.Load() {
	}
	if p.started.Add(1) == 2 {
		close(p.release)
	}
	select {
	case <-p.release:
	case <-time.After(time.Second):
		return 0, fmt.Errorf("partition workers did not start concurrently")
	}
	if id == 1 {
		return 0, fmt.Errorf("partition failure")
	}
	time.Sleep(100 * time.Millisecond)
	return id, nil
}

func TestService_PartitionFailureWaitsForBoundedWorkers(t *testing.T) {
	probe := &partitionProbe{release: make(chan struct{})}
	driverName := "datly_partition_worker_test"
	sql.Register(driverName, &sqlite3.SQLiteDriver{ConnectHook: func(conn *sqlite3.SQLiteConn) error {
		return conn.RegisterFunc("partition_probe", probe.read, false)
	}})
	db, err := sql.Open(driverName, filepath.Join(t.TempDir(), "partition.db"))
	if err != nil {
		t.Fatalf("open sqlite failed: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	db.SetMaxOpenConns(2)
	if _, err = db.Exec(`CREATE TABLE worker_rows (id INTEGER PRIMARY KEY); INSERT INTO worker_rows(id) VALUES (1), (2), (3), (4), (5), (6), (7), (8)`); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	type input struct{}
	type output struct{ Data []*reducedRow }
	component := &spec.Component{
		Name: "PartitionFailure",
		RootView: &spec.View{
			Name:         "workers",
			Source:       &spec.ViewSource{SQL: `SELECT partition_probe(id) AS id FROM worker_rows`},
			Partitioning: &spec.Partitioning{Type: "example.FailingPartitioner", Concurrency: 2},
		},
		Parameters: []*spec.Parameter{{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}}},
	}
	artifact, err := buildArtifact(bootstrap.ArtifactInput{
		Component: component, InputType: reflect.TypeOf(input{}), OutputType: reflect.TypeOf(output{}), DirectViewField: "Data",
		Types: testTypeCatalog(t, map[string]reflect.Type{"example.FailingPartitioner": reflect.TypeOf(failingPartitioner{})}),
	})
	if err != nil {
		t.Fatalf("artifact build failed: %v", err)
	}
	session := &Session{Component: component, OutputType: reflect.TypeOf(output{}), Input: routeInput(t, artifact), Artifact: artifact.Reader, SQL: &rsql.SQLComponent{DB: db}, Scope: testharness.Request{}.WithQuery(url.Values{})}

	started := time.Now()
	_, err = NewService().Read(context.Background(), session)
	if err == nil {
		t.Fatal("expected partition failure")
	}
	if elapsed := time.Since(started); elapsed < 75*time.Millisecond {
		t.Fatalf("read returned before active partition worker exited: %v", elapsed)
	}
	if active := probe.active.Load(); active != 0 {
		t.Fatalf("partition work remained active after Read returned: %d", active)
	}
	if maximum := probe.maximum.Load(); maximum != 2 {
		t.Fatalf("unexpected maximum partition concurrency: %d", maximum)
	}
	if count := probe.started.Load(); count != 2 {
		t.Fatalf("queued partitions continued after cancellation: %d", count)
	}
}
