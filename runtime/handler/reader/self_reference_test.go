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

type selfTreeDetail struct {
	NodeID int
	Value  string
}

type selfTreeNode struct {
	ID       int
	ParentID *int
	Name     string
	Details  []*selfTreeDetail `view:"details,batch=2" sql:"SELECT node_id, value FROM self_tree_detail WHERE node_id IN (?) ORDER BY node_id" on:"ID:id=NodeID:node_id"`
	Children []*selfTreeNode   `self:"child=ID,parent=ParentID"`
}

type selfTreeProbe struct {
	calls       atomic.Int32
	withDetails atomic.Int32
	rootSize    atomic.Int32
}

type selfTreeProbeKey struct{}

func (n *selfTreeNode) OnRelation(ctx context.Context) {
	probe, _ := ctx.Value(selfTreeProbeKey{}).(*selfTreeProbe)
	if probe == nil {
		return
	}
	probe.calls.Add(1)
	if len(n.Details) == 1 {
		probe.withDetails.Add(1)
	}
	if n.ID == 1 {
		probe.rootSize.Store(int32(len(n.Children)))
	}
}

type selfTreeOutput struct {
	Data []*selfTreeNode
}

func TestService_SelfReferenceReplaysTypedTreeThroughNativeSQLXCache(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE self_tree_node (sort_order INTEGER, id INTEGER, parent_id INTEGER, name TEXT);`,
		`CREATE TABLE self_tree_detail (node_id INTEGER, value TEXT);`,
		`INSERT INTO self_tree_node(sort_order, id, parent_id, name) VALUES (1, 4, 2, 'grandchild'), (2, 1, NULL, 'root'), (3, 3, 1, 'second'), (4, 2, 1, 'first'), (5, 5, 99, 'orphan');`,
		`INSERT INTO self_tree_detail(node_id, value) VALUES (1, 'd1'), (2, 'd2'), (3, 'd3'), (4, 'd4'), (5, 'd5');`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	type input struct{}
	component := &spec.Component{
		Name: "SelfTree",
		RootView: &spec.View{Name: "nodes", Source: &spec.ViewSource{
			SQL: `SELECT id, parent_id, name FROM self_tree_node ORDER BY sort_order`,
		}},
		Parameters: []*spec.Parameter{{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}}},
	}
	artifact, err := buildArtifact(bootstrap.ArtifactInput{
		Component: component, InputType: reflect.TypeOf(input{}), OutputType: reflect.TypeOf(selfTreeOutput{}), DirectViewField: "Data",
	})
	if err != nil {
		t.Fatalf("artifact build failed: %v", err)
	}
	if artifact.Reader.Root.Collector == nil || artifact.Reader.Root.Collector.Tree == nil {
		t.Fatal("self-reference tree plan was not compiled")
	}
	readCache, err := cacheafs.NewCache("mem://localhost/"+t.Name()+"/", time.Hour, t.Name(), option.NewStream(0, 0))
	if err != nil {
		t.Fatalf("create SQLx cache: %v", err)
	}
	session := &Session{
		Component: component, OutputType: reflect.TypeOf(selfTreeOutput{}), Input: routeInput(t, artifact), Artifact: artifact.Reader,
		SQL: &rsql.SQLComponent{DB: h.DB}, Scope: testharness.Request{}.WithQuery(url.Values{}), ReadCaches: map[*data.View]cache.Cache{},
	}
	registerViewCaches(session, artifact.Reader.Root.View, readCache)

	readAndAssertSelfTree(t, session)
	if _, err := h.DB.Exec(`DELETE FROM self_tree_detail; DELETE FROM self_tree_node`); err != nil {
		t.Fatalf("delete source rows: %v", err)
	}
	readAndAssertSelfTree(t, session)
}

func readAndAssertSelfTree(t *testing.T, session *Session) {
	t.Helper()
	probe := &selfTreeProbe{}
	ctx := context.WithValue(context.Background(), selfTreeProbeKey{}, probe)
	actual, err := NewService().Read(ctx, session)
	if err != nil {
		t.Fatalf("self-reference read failed: %v", err)
	}
	output := actual.(*selfTreeOutput)
	if len(output.Data) != 2 || output.Data[0].ID != 1 || output.Data[1].ID != 5 {
		t.Fatalf("unexpected roots: %#v", output.Data)
	}
	root := output.Data[0]
	if len(root.Children) != 2 || root.Children[0].ID != 3 || root.Children[1].ID != 2 {
		t.Fatalf("unexpected root children: %#v", root.Children)
	}
	if len(root.Children[1].Children) != 1 || root.Children[1].Children[0].ID != 4 {
		t.Fatalf("unexpected grandchild tree: %#v", root.Children[1].Children)
	}
	if calls := probe.calls.Load(); calls != 5 {
		t.Fatalf("OnRelation calls: got %d, want 5", calls)
	}
	if details := probe.withDetails.Load(); details != 5 {
		t.Fatalf("OnRelation rows with hydrated details: got %d, want 5", details)
	}
	if children := probe.rootSize.Load(); children != 2 {
		t.Fatalf("root OnRelation saw %d children, want 2", children)
	}
}

type invalidSelfTreeNode struct {
	ID       int
	Children []*invalidSelfTreeNode `self:"child=ID,parent=Missing"`
}

func TestBuildArtifact_RejectsInvalidSelfReference(t *testing.T) {
	type input struct{}
	type output struct {
		Data []*invalidSelfTreeNode
	}
	component := &spec.Component{
		Name:       "InvalidSelfTree",
		RootView:   &spec.View{Name: "nodes", Source: &spec.ViewSource{SQL: `SELECT id FROM nodes`}},
		Parameters: []*spec.Parameter{{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}}},
	}
	_, err := buildArtifact(bootstrap.ArtifactInput{
		Component: component, InputType: reflect.TypeOf(input{}), OutputType: reflect.TypeOf(output{}), DirectViewField: "Data",
	})
	if err == nil {
		t.Fatal("expected invalid self-reference to fail artifact construction")
	}
}

type specSelfTreeNode struct {
	ID       int                 `sqlx:"node_id"`
	ParentID *int                `sqlx:"parent_node_id"`
	Children []*specSelfTreeNode `sqlx:"-"`
}

func TestBuildArtifact_CompilesSpecSelfReferenceByColumnName(t *testing.T) {
	type input struct{}
	type output struct {
		Data []*specSelfTreeNode
	}
	component := &spec.Component{
		Name: "SpecSelfTree",
		RootView: &spec.View{
			Name: "nodes", Source: &spec.ViewSource{SQL: `SELECT node_id, parent_node_id FROM nodes`},
			SelfReference: &spec.SelfReference{Holder: "Children", Child: "node_id", Parent: "parent_node_id"},
		},
		Parameters: []*spec.Parameter{{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}}},
	}
	artifact, err := buildArtifact(bootstrap.ArtifactInput{
		Component: component, InputType: reflect.TypeOf(input{}), OutputType: reflect.TypeOf(output{}), DirectViewField: "Data",
	})
	if err != nil {
		t.Fatalf("compile spec self-reference: %v", err)
	}
	if artifact.Reader.Root.Collector == nil || artifact.Reader.Root.Collector.Tree == nil {
		t.Fatal("spec self-reference tree plan was not compiled")
	}
}

type childSelfTreeNode struct {
	OwnerID  int
	ID       int
	ParentID *int
	Children []*childSelfTreeNode `self:"child=ID,parent=ParentID"`
}

type childSelfTreeOwner struct {
	ID    int
	Nodes []*childSelfTreeNode `view:"nodes,match=read_all" sql:"SELECT owner_id, id, parent_id FROM child_self_tree ORDER BY sort_order" on:"ID:id=OwnerID:owner_id"`
}

type packageAuthoritySelfTreeNode struct {
	ID                 int
	PackageParentID    *int
	TranscribeParentID *int
	Children           []*packageAuthoritySelfTreeNode `self:"child=ID,parent=PackageParentID"`
}

func TestBuildArtifact_PackageSelfReferenceWinsInNormalBootstrap(t *testing.T) {
	type input struct{}
	type output struct {
		Data []*packageAuthoritySelfTreeNode
	}
	component := &spec.Component{
		Name: "PackageAuthoritySelfTree",
		RootView: &spec.View{
			Name: "nodes", Source: &spec.ViewSource{SQL: `SELECT id, package_parent_id, transcribe_parent_id FROM nodes`},
			SelfReference: &spec.SelfReference{Holder: "Children", Child: "ID", Parent: "TranscribeParentID"},
		},
		Parameters: []*spec.Parameter{{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}}},
	}
	artifact, err := buildArtifact(bootstrap.ArtifactInput{
		Component: component, InputType: reflect.TypeOf(input{}), OutputType: reflect.TypeOf(output{}), DirectViewField: "Data",
	})
	if err != nil {
		t.Fatalf("compile package-authority self-reference: %v", err)
	}
	if actual := artifact.Reader.Root.View.Spec.SelfReference.Parent; actual != "PackageParentID" {
		t.Fatalf("package self-reference parent: got %q, want PackageParentID", actual)
	}
}

type childSelfTreeOutput struct {
	Data []*childSelfTreeOwner
}

func TestService_SelfReferenceRelationRebindsTreeRootsToParent(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE child_self_owner (id INTEGER);`,
		`CREATE TABLE child_self_tree (sort_order INTEGER, owner_id INTEGER, id INTEGER, parent_id INTEGER);`,
		`INSERT INTO child_self_owner(id) VALUES (7);`,
		`INSERT INTO child_self_tree(sort_order, owner_id, id, parent_id) VALUES (1, 7, 3, 2), (2, 7, 1, NULL), (3, 7, 2, 1);`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	type input struct{}
	component := &spec.Component{
		Name: "ChildSelfTree",
		RootView: &spec.View{Name: "owners", Source: &spec.ViewSource{
			SQL: `SELECT id FROM child_self_owner`,
		}},
		Parameters: []*spec.Parameter{{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}}},
	}
	artifact, err := buildArtifact(bootstrap.ArtifactInput{
		Component: component, InputType: reflect.TypeOf(input{}), OutputType: reflect.TypeOf(childSelfTreeOutput{}), DirectViewField: "Data",
	})
	if err != nil {
		t.Fatalf("artifact build failed: %v", err)
	}
	actual, err := NewService().Read(context.Background(), &Session{
		Component: component, OutputType: reflect.TypeOf(childSelfTreeOutput{}), Input: routeInput(t, artifact), Artifact: artifact.Reader,
		SQL: &rsql.SQLComponent{DB: h.DB}, Scope: testharness.Request{}.WithQuery(url.Values{}),
	})
	if err != nil {
		t.Fatalf("read child self-reference: %v", err)
	}
	owners := actual.(*childSelfTreeOutput).Data
	if len(owners) != 1 || len(owners[0].Nodes) != 1 || owners[0].Nodes[0].ID != 1 {
		t.Fatalf("unexpected child roots: %#v", owners)
	}
	root := owners[0].Nodes[0]
	if len(root.Children) != 1 || root.Children[0].ID != 2 || len(root.Children[0].Children) != 1 || root.Children[0].Children[0].ID != 3 {
		t.Fatalf("unexpected child tree: %#v", root)
	}
}
