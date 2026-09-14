package reader

import (
	"context"
	"net/url"
	"reflect"
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
)

type batchScalarChild struct {
	ID         int
	ParentCode int
}

type batchCompositeChild struct {
	ID       int
	Tenant   string
	ParentID int
}

type batchRelationProbe struct {
	active  atomic.Int32
	maximum atomic.Int32
	calls   atomic.Int32
}

type batchRelationProbeKey struct{}

func (c *batchCompositeChild) OnRelation(ctx context.Context) {
	probe, _ := ctx.Value(batchRelationProbeKey{}).(*batchRelationProbe)
	if probe == nil {
		return
	}
	active := probe.active.Add(1)
	for maximum := probe.maximum.Load(); active > maximum && !probe.maximum.CompareAndSwap(maximum, active); maximum = probe.maximum.Load() {
	}
	probe.calls.Add(1)
	time.Sleep(20 * time.Millisecond)
	probe.active.Add(-1)
}

type batchParent struct {
	Code      int
	Tenant    string
	LocalID   int
	Scalar    []*batchScalarChild    `view:"scalar,batch=2,partitioner=example.RangePartitioner,concurrency=1" sql:"SELECT id, parent_code FROM batch_scalar WHERE parent_code IN (?) ORDER BY id" on:"Code:code=ParentCode:parent_code"`
	Composite []*batchCompositeChild `view:"composite,batch=2,relationalConcurrency=2" sql:"SELECT id, tenant, parent_id FROM batch_composite WHERE $COLUMN_IN ORDER BY id" on:"Tenant:tenant=Tenant:tenant,LocalID:local_id=ParentID:parent_id"`
}

type batchOutput struct {
	Data []*batchParent
}

func TestService_RelationBatchesReplayThroughNativeSQLXCache(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE batch_parent (code INTEGER, tenant TEXT, local_id INTEGER);`,
		`CREATE TABLE batch_scalar (id INTEGER, parent_code INTEGER);`,
		`CREATE TABLE batch_composite (id INTEGER, tenant TEXT, parent_id INTEGER);`,
		`INSERT INTO batch_parent(code, tenant, local_id) VALUES (1, 'a', 1), (2, 'a', 2), (3, 'b', 1), (4, 'b', 2), (5, 'c', 1);`,
		`INSERT INTO batch_scalar(id, parent_code) VALUES (10, 1), (20, 2), (30, 3), (40, 4), (50, 5);`,
		`INSERT INTO batch_composite(id, tenant, parent_id) VALUES (11, 'a', 1), (21, 'a', 2), (31, 'b', 1), (41, 'b', 2), (51, 'c', 1);`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	type input struct{}
	component := &spec.Component{
		Name: "BatchedRelations",
		RootView: &spec.View{Name: "parents", Source: &spec.ViewSource{
			SQL: `SELECT code, tenant, local_id FROM batch_parent ORDER BY code`,
		}},
		Parameters: []*spec.Parameter{{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}}},
	}
	artifact, err := buildArtifact(bootstrap.ArtifactInput{
		Component: component, InputType: reflect.TypeOf(input{}), OutputType: reflect.TypeOf(batchOutput{}), DirectViewField: "Data",
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
		Component: component, OutputType: reflect.TypeOf(batchOutput{}), Input: routeInput(t, artifact), Artifact: artifact.Reader,
		SQL: &rsql.SQLComponent{DB: h.DB}, Scope: testharness.Request{}.WithQuery(url.Values{}), ReadCaches: map[*data.View]cache.Cache{},
	}
	registerViewCaches(session, artifact.Reader.Root.View, readCache)

	readAndAssertBatchOutput(t, session)
	if _, err := h.DB.Exec(`DELETE FROM batch_composite; DELETE FROM batch_scalar; DELETE FROM batch_parent`); err != nil {
		t.Fatalf("delete source rows: %v", err)
	}
	readAndAssertBatchOutput(t, session)
}

func readAndAssertBatchOutput(t *testing.T, session *Session) {
	t.Helper()
	probe := &batchRelationProbe{}
	ctx := context.WithValue(context.Background(), batchRelationProbeKey{}, probe)
	actual, err := NewService().Read(ctx, session)
	if err != nil {
		t.Fatalf("batch read failed: %v", err)
	}
	assertBatchOutput(t, actual.(*batchOutput))
	if calls := probe.calls.Load(); calls != 5 {
		t.Fatalf("unexpected OnRelation calls: %d", calls)
	}
	if maximum := probe.maximum.Load(); maximum != 2 {
		t.Fatalf("unexpected OnRelation concurrency: %d", maximum)
	}
	if active := probe.active.Load(); active != 0 {
		t.Fatalf("OnRelation work remained active: %d", active)
	}
}

func assertBatchOutput(t *testing.T, actual *batchOutput) {
	t.Helper()
	if len(actual.Data) != 5 {
		t.Fatalf("unexpected parent rows: %#v", actual)
	}
	for i, parent := range actual.Data {
		if len(parent.Scalar) != 1 || parent.Scalar[0].ParentCode != parent.Code {
			t.Fatalf("unexpected scalar relation at %d: %#v", i, parent)
		}
		if len(parent.Composite) != 1 || parent.Composite[0].Tenant != parent.Tenant || parent.Composite[0].ParentID != parent.LocalID {
			t.Fatalf("unexpected composite relation at %d: %#v", i, parent)
		}
	}
}
