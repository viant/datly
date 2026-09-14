package reader_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe"
	tcolumn "github.com/viant/datly/transcribe/column"
	"github.com/viant/datly/typecatalog"
	xshape "github.com/viant/x/shape"
	xreader "github.com/viant/xdatly/reader"
)

type batchTraceKey struct{}

var batchReadFailure = errors.New("batch child failed")

type batchReadTrace struct {
	active, max      atomic.Int32
	arrivals         atomic.Int32
	ready            chan struct{}
	once             sync.Once
	parallel         int
	mu               sync.Mutex
	fetched, related map[int]int
	parents          []any
	parentType       reflect.Type
	holder           *xshape.Accessor
	expected         int
	fail, cancel     bool
	cancelFunc       context.CancelFunc
	premature        bool
}
type batchRuntimeChild struct {
	ID       int    `sqlx:"id"`
	ParentID int    `sqlx:"parent_id"`
	TenantID string `sqlx:"tenant_id"`
	Related  bool   `sqlx:"-"`
}

type batchRuntimePartitions struct{}

func (batchRuntimePartitions) Partitions(context.Context, xreader.PartitionRequest) ([]xreader.Partition, error) {
	return []xreader.Partition{{Expression: "id <= ?", Args: []any{1000}}, {Expression: "id > ?", Args: []any{1000}}}, nil
}

func (c *batchRuntimeChild) OnFetch(ctx context.Context) error {
	trace := ctx.Value(batchTraceKey{}).(*batchReadTrace)
	active := trace.active.Add(1)
	defer trace.active.Add(-1)
	for previous := trace.max.Load(); active > previous && !trace.max.CompareAndSwap(previous, active); previous = trace.max.Load() {
	}
	trace.mu.Lock()
	trace.fetched[c.ID]++
	trace.parents = append(trace.parents, ctx.Value(trace.parentType))
	trace.mu.Unlock()
	if trace.arrivals.Add(1) >= int32(trace.parallel) {
		trace.once.Do(func() { close(trace.ready) })
	}
	select {
	case <-trace.ready:
	case <-ctx.Done():
		return ctx.Err()
	}
	if trace.cancel {
		trace.cancelFunc()
		return ctx.Err()
	}
	if trace.fail && c.ParentID == 3 {
		return batchReadFailure
	}
	return nil
}
func (c *batchRuntimeChild) OnRelation(ctx context.Context) {
	trace := ctx.Value(batchTraceKey{}).(*batchReadTrace)
	trace.mu.Lock()
	defer trace.mu.Unlock()
	if len(trace.fetched) != trace.expected || trace.active.Load() != 0 {
		trace.premature = true
	}
	trace.related[c.ID]++
	c.Related = true
}

func TestPublicBatchFetchConcurrencyThroughRuntimeSQLite(t *testing.T) {
	for _, test := range []struct {
		name              string
		size, concurrency int
		fail, cancel      bool
		mode              string
	}{
		{"single", 6, 0, false, false, ""}, {"serial_batches", 1, 0, false, false, ""}, {"concurrent_batches", 1, 2, false, false, ""}, {"bounded_to_batches", 2, 99, false, false, ""}, {"batch_error", 1, 2, true, false, ""}, {"cancellation", 1, 2, false, true, ""}, {"dql_concurrent", 1, 2, false, false, "dql"}, {"composite_concurrent", 1, 2, false, false, "composite"}, {"partitioned_batches", 1, 2, false, false, "partition"},
	} {
		t.Run(test.name, func(t *testing.T) {
			db := sqlite.New(t)
			seed := "INSERT INTO parents VALUES(1,'a',1),(2,'a',2),(3,'a',3),(4,'b',4),(5,'b',5),(6,'b',6)"
			if test.mode == "composite" {
				seed = "INSERT INTO parents VALUES(1,'a',1),(2,'a',2),(3,'a',3),(1,'b',4),(2,'b',5),(3,'b',6)"
			}
			if err := db.ExecStatements(context.Background(), "CREATE TABLE parents(id INTEGER,tenant_id TEXT,ordinal INTEGER)", seed, "CREATE TABLE children(id INTEGER,parent_id INTEGER,tenant_id TEXT)", "INSERT INTO children SELECT ordinal*10+1,id,tenant_id FROM parents UNION ALL SELECT ordinal*10+2,id,tenant_id FROM parents"); err != nil {
				t.Fatal(err)
			}
			links := "ID:id=ParentID:parent_id"
			parentSQL := "SELECT id FROM parents ORDER BY id"
			fields := []xshape.RuntimeField{{Name: "ID", Type: reflect.TypeOf(0), Tag: `sqlx:"id"`}}
			if test.mode == "composite" {
				links += ",TenantID:tenant_id=TenantID:tenant_id"
				parentSQL = "SELECT id,tenant_id,ordinal FROM parents ORDER BY ordinal"
				fields = append(fields, xshape.RuntimeField{Name: "TenantID", Type: reflect.TypeOf(""), Tag: `sqlx:"tenant_id"`}, xshape.RuntimeField{Name: "Ordinal", Type: reflect.TypeOf(0), Tag: `sqlx:"ordinal"`})
			}
			var catalog *typecatalog.Catalog
			partitionTag := ""
			if test.mode == "partition" {
				catalog = typecatalog.NewCatalog()
				descriptor := xshape.Linked(reflect.TypeOf(batchRuntimePartitions{})).Descriptor()
				if err := catalog.Register(typecatalog.TypeOriginPackage, descriptor); err != nil {
					t.Fatal(err)
				}
				partitionTag = ",partitioner=" + descriptor.Key() + ",concurrency=2"
			}
			fields = append(fields, xshape.RuntimeField{Name: "Children", Type: reflect.TypeOf([]*batchRuntimeChild{}), Tag: reflect.StructTag(fmt.Sprintf(`sqlx:"-" view:"Children,batch=%d,batchConcurrency=%d,publishParent=true%s" on:%q sql:"SELECT id,parent_id,tenant_id FROM children WHERE $COLUMN_IN ORDER BY id DESC"`, test.size, test.concurrency, partitionTag, links))})
			parent, err := (xshape.Runtime{}).Struct(fields)
			if err != nil {
				t.Fatal(err)
			}
			outputField := "Rows"
			outputTag := reflect.StructTag(fmt.Sprintf(`parameter:"Rows,kind=output,in=view" view:"Parents" sql:%q`, parentSQL))
			if test.mode == "dql" {
				outputField = "Data"
				outputTag = `parameter:"Data,kind=output,in=view"`
			}
			output, err := (xshape.Runtime{}).Struct([]xshape.RuntimeField{{Name: outputField, Type: reflect.SliceOf(reflect.PointerTo(parent)), Tag: outputTag}})
			if err != nil {
				t.Fatal(err)
			}
			component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "Batch"}, Name: "Batch", Routes: []*spec.Route{{Method: "GET", Path: "/graph"}}}
			if test.mode == "dql" {
				compiled, err := transcribe.NewCompiler().Compile(context.Background(), &transcribe.Source{Name: "Batch", Scope: "example.com/batch", Connector: "main", Types: typecatalog.NewCatalog(), ColumnRefiner: tcolumn.New(tcolumn.Connections{"main": db.DB}), Text: fmt.Sprintf("#setting($_ = $route('/graph','GET'))\nSELECT p.*,batch_size(Children,%d),batch_concurrency(Children,%d),publish_parent(Children),order_by(Children,'id DESC') FROM parents p JOIN children Children ON Children.parent_id=p.id ORDER BY p.id", test.size, test.concurrency)})
				if err != nil {
					t.Fatal(err)
				}
				component = compiled.Component
			}
			app, artifact, err := (typedGraphFixture{db: db, component: component, output: output, types: catalog}).compile()
			if err != nil {
				t.Fatal(err)
			}
			view := artifact.Reader.Root.Relations[0].Target.View
			if view.Spec.BatchSize != test.size || view.Spec.BatchConcurrency != test.concurrency {
				t.Fatalf("public batch settings lost: %+v", view)
			}
			holder, err := xshape.Linked(parent).Accessor("Children")
			if err != nil {
				t.Fatal(err)
			}
			jobs := (6 + test.size - 1) / test.size
			parallel := test.concurrency
			if parallel <= 0 {
				parallel = 1
			}
			if parallel > jobs {
				parallel = jobs
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			trace := &batchReadTrace{ready: make(chan struct{}), parallel: parallel, fetched: map[int]int{}, related: map[int]int{}, parentType: reflect.PointerTo(parent), holder: holder, expected: 12, fail: test.fail, cancel: test.cancel, cancelFunc: cancel}
			ctx = context.WithValue(ctx, batchTraceKey{}, trace)
			result, err := app.InvokeComponent(ctx, dexec.ComponentRequest{Target: dexec.ComponentTarget{Component: component.Key, Route: spec.RouteRef{Method: "GET", Path: "/graph"}}, Input: &struct{}{}})
			if trace.active.Load() != 0 {
				t.Fatal("fetch workers did not drain")
			}
			if test.fail || test.cancel {
				want := batchReadFailure
				if test.cancel {
					want = context.Canceled
				}
				if err == nil || !errors.Is(err, want) {
					t.Fatalf("fetch error %v expected %v", err, want)
				}
				if len(trace.related) != 0 {
					t.Fatal("OnRelation ran after incomplete fetch")
				}
				for _, parent := range trace.parents {
					if parent == nil {
						t.Fatal("parent was not published")
					}
					children, err := holder.Get(parent)
					if err != nil {
						t.Fatal(err)
					}
					if children.Len() != 0 {
						t.Fatal("failed multi-batch read published partial parent rows")
					}
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if trace.max.Load() != int32(parallel) {
				t.Fatalf("observed concurrency %d expected %d", trace.max.Load(), parallel)
			}
			if trace.premature || len(trace.related) != 12 {
				t.Fatalf("incomplete relation callbacks: premature=%v callbacks=%v", trace.premature, trace.related)
			}
			for id, count := range trace.fetched {
				if count != 1 || trace.related[id] != 1 {
					t.Fatalf("row%d fetch%d relation%d", id, count, trace.related[id])
				}
			}
			rowsAccessor, _ := xshape.Linked(output).Accessor(outputField)
			rows, err := rowsAccessor.Get(result)
			if err != nil {
				t.Fatal(err)
			}
			if rows.Len() != 6 {
				t.Fatalf("parent count %d", rows.Len())
			}
			for index := 0; index < rows.Len(); index++ {
				children, err := holder.Get(rows.Index(index).Interface())
				if err != nil {
					t.Fatal(err)
				}
				if children.Len() != 2 {
					t.Fatalf("parent%d children%d", index+1, children.Len())
				}
				for childIndex := 0; childIndex < 2; childIndex++ {
					child := children.Index(childIndex).Interface().(*batchRuntimeChild)
					if child.ID != (index+1)*10+2-childIndex || !child.Related {
						t.Fatalf("unstable batch attachment %+v", child)
					}
				}
			}
		})
	}
}
