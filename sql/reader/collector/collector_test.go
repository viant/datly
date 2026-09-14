package collector_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/sql/reader/collector"
)

// ──────────────────────────────────────────────────────────────────────────────
// Test fixtures
// ──────────────────────────────────────────────────────────────────────────────

type User struct {
	ID       int
	Name     string
	Accounts []Account
}

type UserPtrAccounts struct {
	ID       int
	Name     string
	Accounts []*Account
}

type Account struct {
	ID     int
	UserID int
	Name   string
}

type Vendor struct {
	ID           int
	ProductsMeta *ProductsMeta
}

type ProductsMeta struct {
	VendorId int
	Count    int
}

func userElemType() reflect.Type    { return reflect.TypeOf(User{}) }
func userPtrElemType() reflect.Type { return reflect.TypeOf(UserPtrAccounts{}) }
func accountElemType() reflect.Type { return reflect.TypeOf(Account{}) }
func vendorElemType() reflect.Type  { return reflect.TypeOf(Vendor{}) }

func compiledView(t *testing.T, metadata *data.View, rowTypes map[*data.View]reflect.Type) *collector.View {
	t.Helper()
	graph, err := collector.Compile(metadata, rowTypes)
	if err != nil {
		t.Fatalf("compile collector graph: %v", err)
	}
	return graph.Root
}

// ──────────────────────────────────────────────────────────────────────────────
// TestNewCollector_BasicAppend verifies that NewItem + Fetched + Dest
// round-trips without panicking.
// ──────────────────────────────────────────────────────────────────────────────

func TestNewCollector_BasicAppend(t *testing.T) {
	metadata := &data.View{}
	view := compiledView(t, metadata, map[*data.View]reflect.Type{metadata: userElemType()})

	var dest []User
	c := collector.NewCollector(view, &dest, false)

	newItem := c.NewItem()
	item := newItem()
	u, ok := item.(*User)
	if !ok {
		t.Fatalf("expected *User, got %T", item)
	}
	u.ID = 1
	u.Name = "Alice"

	if c.Len() != 1 {
		t.Fatalf("expected Len 1, got %d", c.Len())
	}

	c.Fetched()

	result := c.Dest().([]User)
	if len(result) != 1 {
		t.Fatalf("expected 1 user, got %d", len(result))
	}
	if result[0].Name != "Alice" {
		t.Fatalf("unexpected user name: %s", result[0].Name)
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// TestVisitor_SingleParent_ManyAccounts verifies the Many-cardinality visitor
// populates parent.Accounts after scanning child rows.
// ──────────────────────────────────────────────────────────────────────────────

func TestVisitor_SingleParent_ManyAccounts(t *testing.T) {
	accountsMetadata := &data.View{}
	userMetadata := &data.View{Relations: []*data.Relation{{
		Name: "Accounts", Holder: "Accounts", Cardinality: spec.CardinalityMany,
		On: data.Links{data.NewLink("users", "id", "ID")},
		Of: &data.RelationRef{
			View: accountsMetadata, On: data.Links{data.NewLink("accounts", "user_id", "UserID")}, MatchStrategy: data.MatchSequential,
		},
	}}}
	userView := compiledView(t, userMetadata, map[*data.View]reflect.Type{userMetadata: userElemType(), accountsMetadata: accountElemType()})

	// Seed one user into parent collector; the parent visitor must be called
	// per row so that it indexes user.ID → position for child lookup.
	var users []User
	parentCollector := collector.NewCollector(userView, &users, false)
	parentVisitor := parentCollector.Visitor(context.Background())
	newUser := parentCollector.NewItem()
	u := newUser().(*User)
	u.ID = 10
	u.Name = "Bob"
	if err := parentVisitor(u); err != nil {
		t.Fatalf("parent visitor: %v", err)
	}
	parentCollector.Fetched()

	// Create child collector via Relations
	children := parentCollector.Relations(nil)
	if len(children) != 1 {
		t.Fatalf("expected 1 child collector, got %d", len(children))
	}
	childC := children[0]
	visitor := childC.Visitor(context.Background())

	// Allocate account rows via the child collector's NewItem
	newAcct := childC.NewItem()
	a1 := newAcct().(*Account)
	a1.ID, a1.UserID, a1.Name = 100, 10, "Checking"
	a2 := newAcct().(*Account)
	a2.ID, a2.UserID, a2.Name = 101, 10, "Savings"

	if err := visitor(a1); err != nil {
		t.Fatalf("visitor a1: %v", err)
	}
	if err := visitor(a2); err != nil {
		t.Fatalf("visitor a2: %v", err)
	}

	result := parentCollector.Dest().([]User)
	if len(result) != 1 {
		t.Fatalf("expected 1 user, got %d", len(result))
	}
	if len(result[0].Accounts) != 2 {
		t.Fatalf("expected 2 accounts on user Bob, got %d", len(result[0].Accounts))
	}
}

func TestMergeData_ReadAll_ManyPointerAccounts(t *testing.T) {
	accountsMetadata := &data.View{}
	userMetadata := &data.View{Relations: []*data.Relation{{
		Name: "Accounts", Holder: "Accounts", Cardinality: spec.CardinalityMany,
		On: data.Links{data.NewLink("users", "id", "ID")},
		Of: &data.RelationRef{
			View: accountsMetadata, On: data.Links{data.NewLink("accounts", "user_id", "UserID")}, MatchStrategy: data.MatchReadAll,
		},
	}}}
	userView := compiledView(t, userMetadata, map[*data.View]reflect.Type{userMetadata: userPtrElemType(), accountsMetadata: accountElemType()})

	var users []UserPtrAccounts
	parentCollector := collector.NewCollector(userView, &users, false)
	children := parentCollector.Relations(nil)
	child := children[0]
	childVisitor := child.Visitor(context.Background())

	newAcct := child.NewItem()
	a1 := newAcct().(*Account)
	a1.ID, a1.UserID, a1.Name = 100, 10, "Checking"
	a2 := newAcct().(*Account)
	a2.ID, a2.UserID, a2.Name = 101, 10, "Savings"
	if err := childVisitor(a1); err != nil {
		t.Fatalf("child visitor a1: %v", err)
	}
	if err := childVisitor(a2); err != nil {
		t.Fatalf("child visitor a2: %v", err)
	}
	child.Fetched()

	parentVisitor := parentCollector.Visitor(context.Background())
	newUser := parentCollector.NewItem()
	u := newUser().(*UserPtrAccounts)
	u.ID = 10
	u.Name = "Bob"
	if err := parentVisitor(u); err != nil {
		t.Fatalf("parent visitor: %v", err)
	}
	parentCollector.Fetched()
	if err := parentCollector.MergeData(); err != nil {
		t.Fatalf("MergeData failed: %v", err)
	}

	result := parentCollector.Dest().([]UserPtrAccounts)
	if len(result) != 1 {
		t.Fatalf("expected 1 user, got %d", len(result))
	}
	if len(result[0].Accounts) != 2 {
		t.Fatalf("expected 2 accounts on user Bob, got %d", len(result[0].Accounts))
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// TestDataSync_PutWaitDelete exercises the DataSync lifecycle
// ──────────────────────────────────────────────────────────────────────────────

func TestDataSync_PutWaitDelete(t *testing.T) {
	metadata := &data.View{}
	view := compiledView(t, metadata, map[*data.View]reflect.Type{metadata: userElemType()})
	var dest []User
	c := collector.NewCollector(view, &dest, false)

	ds := c.DataSync()
	if ds == nil {
		t.Fatal("DataSync should be non-nil")
	}
	ds.Put("accounts")
	done := make(chan bool, 1)
	go func() {
		ds.Wait("accounts")
		done <- true
	}()
	ds.Delete("accounts")
	<-done // should not hang
}
