package reader

import (
	"context"
	"errors"
	"net/url"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/viant/afs/option"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/data"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
	rsql "github.com/viant/datly/sql"
	"github.com/viant/sqlx/io/read/cache"
	cacheafs "github.com/viant/sqlx/io/read/cache/afs"
	xhandler "github.com/viant/xdatly/handler"
)

type siblingFetchProbe struct {
	active  atomic.Int32
	maximum atomic.Int32
	calls   atomic.Int32
}

type siblingFetchProbeKey struct{}

type siblingFetchChild struct {
	Tenant   string
	ParentID int
	Value    string
}

func (c *siblingFetchChild) OnFetch(ctx context.Context) error {
	probe, _ := ctx.Value(siblingFetchProbeKey{}).(*siblingFetchProbe)
	if probe == nil {
		return nil
	}
	active := probe.active.Add(1)
	for maximum := probe.maximum.Load(); active > maximum && !probe.maximum.CompareAndSwap(maximum, active); maximum = probe.maximum.Load() {
	}
	probe.calls.Add(1)
	defer probe.active.Add(-1)
	select {
	case <-time.After(25 * time.Millisecond):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type siblingFetchParent struct {
	Tenant  string               `sqlx:"-"`
	LocalID int                  `sqlx:"-"`
	A       []*siblingFetchChild `view:"a" sql:"SELECT tenant, parent_id, value FROM sibling_a WHERE $COLUMN_IN" on:"Tenant:tenant_key=Tenant:tenant,LocalID:local_key=ParentID:parent_id"`
	B       []*siblingFetchChild `view:"b" sql:"SELECT tenant, parent_id, value FROM sibling_b WHERE $COLUMN_IN" on:"Tenant:tenant_key=Tenant:tenant,LocalID:local_key=ParentID:parent_id"`
	C       []*siblingFetchChild `view:"c" sql:"SELECT tenant, parent_id, value FROM sibling_c WHERE $COLUMN_IN" on:"Tenant:tenant_key=Tenant:tenant,LocalID:local_key=ParentID:parent_id"`
}

type siblingFetchOutput struct {
	Data []*siblingFetchParent
}

func TestService_BoundsSiblingRelationsOnColdAndNativeCacheReplay(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE sibling_parent (tenant_key TEXT, local_key INTEGER);`,
		`CREATE TABLE sibling_a (tenant TEXT, parent_id INTEGER, value TEXT);`,
		`CREATE TABLE sibling_b (tenant TEXT, parent_id INTEGER, value TEXT);`,
		`CREATE TABLE sibling_c (tenant TEXT, parent_id INTEGER, value TEXT);`,
		`INSERT INTO sibling_parent(tenant_key, local_key) VALUES ('acme', 1), ('beta', 2);`,
		`INSERT INTO sibling_a(tenant, parent_id, value) VALUES ('acme', 1, 'a-acme'), ('beta', 2, 'a-beta'), ('acme', 2, 'a-cross'), ('beta', 1, 'a-cross');`,
		`INSERT INTO sibling_b(tenant, parent_id, value) VALUES ('acme', 1, 'b-acme'), ('beta', 2, 'b-beta'), ('acme', 2, 'b-cross'), ('beta', 1, 'b-cross');`,
		`INSERT INTO sibling_c(tenant, parent_id, value) VALUES ('acme', 1, 'c-acme'), ('beta', 2, 'c-beta'), ('acme', 2, 'c-cross'), ('beta', 1, 'c-cross');`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	type input struct{}
	component := &spec.Component{
		Name: "SiblingFetch",
		RootView: &spec.View{Name: "parents", Source: &spec.ViewSource{
			SQL: `SELECT tenant_key, local_key FROM sibling_parent ORDER BY local_key`,
		}},
		Parameters: []*spec.Parameter{{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}}},
	}
	artifact, err := buildArtifact(bootstrap.ArtifactInput{
		Component: component, InputType: reflect.TypeOf(input{}), OutputType: reflect.TypeOf(siblingFetchOutput{}), DirectViewField: "Data",
	})
	if err != nil {
		t.Fatalf("artifact build failed: %v", err)
	}
	readCache, err := cacheafs.NewCache("mem://localhost/"+t.Name()+"/", time.Hour, t.Name(), option.NewStream(0, 0))
	if err != nil {
		t.Fatalf("create SQLx cache: %v", err)
	}
	session := &Session{
		Component: component, OutputType: reflect.TypeOf(siblingFetchOutput{}), Input: routeInput(t, artifact), Artifact: artifact.Reader,
		SQL: &rsql.SQLComponent{DB: h.DB}, Scope: testharness.Request{}.WithQuery(url.Values{}), ReadCaches: map[*data.View]cache.Cache{},
	}
	registerViewCaches(session, artifact.Reader.Root.View, readCache)
	service := &Service{relationFetchConcurrency: 2}

	readAndAssertSiblingFetch(t, service, session)
	if _, err := h.DB.Exec(`DELETE FROM sibling_a; DELETE FROM sibling_b; DELETE FROM sibling_c; DELETE FROM sibling_parent`); err != nil {
		t.Fatalf("delete source rows: %v", err)
	}
	readAndAssertSiblingFetch(t, service, session)
}

func readAndAssertSiblingFetch(t *testing.T, service *Service, session *Session) {
	t.Helper()
	probe := &siblingFetchProbe{}
	ctx := context.WithValue(context.Background(), siblingFetchProbeKey{}, probe)
	actual, err := service.Read(ctx, session)
	if err != nil {
		t.Fatalf("read sibling relations: %v", err)
	}
	parents := actual.(*siblingFetchOutput).Data
	if len(parents) != 2 || len(parents[0].A) != 1 || len(parents[0].B) != 1 || len(parents[0].C) != 1 ||
		len(parents[1].A) != 1 || len(parents[1].B) != 1 || len(parents[1].C) != 1 {
		t.Fatalf("unexpected sibling graph: %#v", parents)
	}
	if parents[0].A[0].Value != "a-acme" || parents[0].B[0].Value != "b-acme" || parents[0].C[0].Value != "c-acme" ||
		parents[1].A[0].Value != "a-beta" || parents[1].B[0].Value != "b-beta" || parents[1].C[0].Value != "c-beta" {
		t.Fatalf("cross-combination row attached to sibling graph: %#v", parents)
	}
	if probe.calls.Load() != 6 || probe.maximum.Load() != 2 || probe.active.Load() != 0 {
		t.Fatalf("unexpected sibling concurrency calls=%d max=%d active=%d", probe.calls.Load(), probe.maximum.Load(), probe.active.Load())
	}
}

var errSiblingFetch = errors.New("sibling fetch failed")

type siblingFailureProbe struct {
	waitStarted chan struct{}
	dataSync    chan *xhandler.DataSync
	waitOnce    sync.Once
}

type siblingFailureProbeKey struct{}

type failingSiblingChild struct {
	ParentID int
}

func (c *failingSiblingChild) OnFetch(ctx context.Context) error {
	probe, _ := ctx.Value(siblingFailureProbeKey{}).(*siblingFailureProbe)
	if probe == nil {
		return errSiblingFetch
	}
	dataSync, _ := ctx.Value(xhandler.DataSyncKey).(*xhandler.DataSync)
	probe.dataSync <- dataSync
	<-probe.waitStarted
	return errSiblingFetch
}

type waitingSiblingChild struct {
	ParentID int
}

func (c *waitingSiblingChild) OnFetch(ctx context.Context) error {
	probe, _ := ctx.Value(siblingFailureProbeKey{}).(*siblingFailureProbe)
	if probe != nil {
		probe.waitOnce.Do(func() { close(probe.waitStarted) })
	}
	<-ctx.Done()
	return ctx.Err()
}

type skippedSiblingChild struct {
	ParentID int
}

type siblingFailureParent struct {
	ID      int
	Fail    []*failingSiblingChild `view:"fail,publishParent=true" sql:"SELECT parent_id FROM sibling_fail WHERE parent_id IN (?)" on:"ID:id=ParentID:parent_id"`
	Wait    []*waitingSiblingChild `view:"wait,publishParent=true" sql:"SELECT parent_id FROM sibling_wait WHERE parent_id IN (?)" on:"ID:id=ParentID:parent_id"`
	Skipped []*skippedSiblingChild `view:"skipped" sql:"SELECT parent_id FROM sibling_skipped WHERE parent_id IN (?)" on:"ID:id=ParentID:parent_id"`
}

type siblingFailureOutput struct {
	Data []*siblingFailureParent
}

func TestService_SiblingFailureCancelsJoinsAndReleasesDataSync(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE sibling_failure_parent (id INTEGER);`,
		`CREATE TABLE sibling_fail (parent_id INTEGER);`,
		`CREATE TABLE sibling_wait (parent_id INTEGER);`,
		`CREATE TABLE sibling_skipped (parent_id INTEGER);`,
		`INSERT INTO sibling_failure_parent(id) VALUES (1);`,
		`INSERT INTO sibling_fail(parent_id) VALUES (1);`,
		`INSERT INTO sibling_wait(parent_id) VALUES (1);`,
		`INSERT INTO sibling_skipped(parent_id) VALUES (1);`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	type input struct{}
	component := &spec.Component{
		Name: "SiblingFailure",
		RootView: &spec.View{Name: "parents", Source: &spec.ViewSource{
			SQL: `SELECT id FROM sibling_failure_parent`,
		}},
		Parameters: []*spec.Parameter{{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}}},
	}
	artifact, err := buildArtifact(bootstrap.ArtifactInput{
		Component: component, InputType: reflect.TypeOf(input{}), OutputType: reflect.TypeOf(siblingFailureOutput{}), DirectViewField: "Data",
	})
	if err != nil {
		t.Fatalf("artifact build failed: %v", err)
	}
	probe := &siblingFailureProbe{waitStarted: make(chan struct{}), dataSync: make(chan *xhandler.DataSync, 1)}
	ctx := context.WithValue(context.Background(), siblingFailureProbeKey{}, probe)
	_, err = (&Service{relationFetchConcurrency: 2}).Read(ctx, &Session{
		Component: component, OutputType: reflect.TypeOf(siblingFailureOutput{}), Input: routeInput(t, artifact), Artifact: artifact.Reader,
		SQL: &rsql.SQLComponent{DB: h.DB}, Scope: testharness.Request{}.WithQuery(url.Values{}),
	})
	if !errors.Is(err, errSiblingFetch) {
		t.Fatalf("read error: got %v, want %v", err, errSiblingFetch)
	}
	dataSync := <-probe.dataSync
	if dataSync == nil {
		t.Fatal("failing child did not receive DataSync")
	}
	for _, holder := range []string{"Fail", "Wait", "Skipped"} {
		if lock := dataSync.Get(holder); lock != nil {
			t.Fatalf("DataSync holder %s remained locked after failure", holder)
		}
	}
}
