package reader

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
	xhandler "github.com/viant/xdatly/handler"
)

type cachedLifecycleAccount struct {
	ID            int
	UserID        int
	FetchCount    int  `sqlx:"-"`
	RelationCount int  `sqlx:"-"`
	FetchComplete bool `sqlx:"-"`
}

func (a *cachedLifecycleAccount) OnFetch(context.Context) error {
	a.FetchCount++
	a.FetchComplete = true
	return nil
}

func (a *cachedLifecycleAccount) OnRelation(context.Context) {
	a.RelationCount++
}

type cachedLifecycleUser struct {
	ID                  int
	Accounts            []*cachedLifecycleAccount `view:"accounts" sql:"SELECT id, user_id FROM hook_accounts WHERE user_id IN (?) ORDER BY id" on:"ID:id=UserID:user_id"`
	FetchCount          int                       `sqlx:"-"`
	RelationCount       int                       `sqlx:"-"`
	FetchSawRelation    bool                      `sqlx:"-"`
	RelationSawFetch    bool                      `sqlx:"-"`
	RelationSawAccounts bool                      `sqlx:"-"`
}

func (u *cachedLifecycleUser) OnFetch(context.Context) error {
	u.FetchCount++
	u.FetchSawRelation = len(u.Accounts) != 0
	return nil
}

func (u *cachedLifecycleUser) OnRelation(context.Context) {
	u.RelationCount++
	u.RelationSawFetch = u.FetchCount == 1
	u.RelationSawAccounts = len(u.Accounts) == 1
}

type cachedLifecycleOutput struct {
	Data []*cachedLifecycleUser
}

func TestService_LifecycleOrderingIsIdenticalOnSQLXCacheReplay(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE hook_users (id INTEGER PRIMARY KEY);`,
		`CREATE TABLE hook_accounts (id INTEGER PRIMARY KEY, user_id INTEGER);`,
		`INSERT INTO hook_users(id) VALUES (1);`,
		`INSERT INTO hook_accounts(id, user_id) VALUES (10, 1);`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	type input struct{}
	session := relationCacheSession(t, h, reflect.TypeOf(input{}), reflect.TypeOf(cachedLifecycleOutput{}), &spec.Component{
		Name:     "LifecycleCache",
		RootView: &spec.View{Source: &spec.ViewSource{SQL: `SELECT id FROM hook_users`}},
		Parameters:   []*spec.Parameter{{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}}},
	})

	assertLifecycleOutput(t, readLifecycleOutput(t, session))
	if _, err := h.DB.Exec(`DELETE FROM hook_accounts; DELETE FROM hook_users`); err != nil {
		t.Fatalf("delete source rows: %v", err)
	}
	assertLifecycleOutput(t, readLifecycleOutput(t, session))
}

func readLifecycleOutput(t *testing.T, session *Session) *cachedLifecycleOutput {
	t.Helper()
	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	result, ok := actual.(*cachedLifecycleOutput)
	if !ok {
		t.Fatalf("unexpected output type %T", actual)
	}
	return result
}

func assertLifecycleOutput(t *testing.T, actual *cachedLifecycleOutput) {
	t.Helper()
	if len(actual.Data) != 1 || len(actual.Data[0].Accounts) != 1 {
		t.Fatalf("unexpected relation output: %#v", actual)
	}
	user := actual.Data[0]
	account := user.Accounts[0]
	if user.FetchCount != 1 || user.RelationCount != 1 || user.FetchSawRelation || !user.RelationSawFetch || !user.RelationSawAccounts {
		t.Fatalf("unexpected user lifecycle state: %#v", user)
	}
	if account.FetchCount != 1 || account.RelationCount != 1 || !account.FetchComplete {
		t.Fatalf("unexpected account lifecycle state: %#v", account)
	}
}

type publishedParentUser struct {
	ID       int
	Accounts []*publishedParentAccount `view:"accounts,publishParent=true" sql:"SELECT id, user_id FROM published_accounts WHERE user_id IN (?)" on:"ID:id=UserID:user_id"`
}

type publishedParentAccount struct {
	ID          int
	UserID      int
	ParentID    int           `sqlx:"-"`
	HasDataSync bool          `sqlx:"-"`
	WaitDone    chan struct{} `sqlx:"-"`
}

func (a *publishedParentAccount) OnFetch(ctx context.Context) error {
	parent, _ := ctx.Value(reflect.TypeOf((*publishedParentUser)(nil))).(*publishedParentUser)
	if parent != nil {
		a.ParentID = parent.ID
	}
	dataSync, _ := ctx.Value(xhandler.DataSyncKey).(*xhandler.DataSync)
	a.HasDataSync = dataSync != nil
	if dataSync != nil {
		a.WaitDone = make(chan struct{})
		go func() {
			dataSync.Wait("Accounts")
			close(a.WaitDone)
		}()
	}
	return nil
}

type publishedParentOutput struct {
	Data []*publishedParentUser
}

func TestService_PublishParentContextAndDataSyncReleaseSurviveSQLXCacheReplay(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE published_users (id INTEGER PRIMARY KEY);`,
		`CREATE TABLE published_accounts (id INTEGER PRIMARY KEY, user_id INTEGER);`,
		`INSERT INTO published_users(id) VALUES (7);`,
		`INSERT INTO published_accounts(id, user_id) VALUES (70, 7);`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	type input struct{}
	session := relationCacheSession(t, h, reflect.TypeOf(input{}), reflect.TypeOf(publishedParentOutput{}), &spec.Component{
		Name:     "PublishParentCache",
		RootView: &spec.View{Source: &spec.ViewSource{SQL: `SELECT id FROM published_users`}},
		Parameters:   []*spec.Parameter{{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}}},
	})

	assertPublishedParent(t, readPublishedParent(t, session))
	if _, err := h.DB.Exec(`DELETE FROM published_accounts; DELETE FROM published_users`); err != nil {
		t.Fatalf("delete source rows: %v", err)
	}
	assertPublishedParent(t, readPublishedParent(t, session))
}

func readPublishedParent(t *testing.T, session *Session) *publishedParentOutput {
	t.Helper()
	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	result, ok := actual.(*publishedParentOutput)
	if !ok {
		t.Fatalf("unexpected output type %T", actual)
	}
	return result
}

func assertPublishedParent(t *testing.T, actual *publishedParentOutput) {
	t.Helper()
	if len(actual.Data) != 1 || len(actual.Data[0].Accounts) != 1 {
		t.Fatalf("unexpected relation output: %#v", actual)
	}
	account := actual.Data[0].Accounts[0]
	if account.ParentID != 7 || !account.HasDataSync || account.WaitDone == nil {
		t.Fatalf("missing published parent context: %#v", account)
	}
	select {
	case <-account.WaitDone:
	case <-time.After(time.Second):
		t.Fatal("DataSync relation lock was not released")
	}
}

type hiddenCompositeUser struct {
	Tenant   string
	ID       int
	Accounts []*hiddenCompositeAccount `view:"accounts,publishParent=true" sql:"SELECT id, tenant, user_id FROM hidden_accounts WHERE $COLUMN_IN ORDER BY id" on:"Tenant:tenant=HiddenTenant:tenant,ID:id=HiddenUserID:user_id"`
}

type hiddenCompositeAccount struct {
	ID           int
	ParentTenant string `sqlx:"-"`
	ParentID     int    `sqlx:"-"`
}

func (a *hiddenCompositeAccount) OnFetch(ctx context.Context) error {
	parent, _ := ctx.Value(reflect.TypeOf((*hiddenCompositeUser)(nil))).(*hiddenCompositeUser)
	if parent != nil {
		a.ParentTenant = parent.Tenant
		a.ParentID = parent.ID
	}
	return nil
}

type hiddenCompositeOutput struct {
	Data []*hiddenCompositeUser
}

func TestService_PublishParentResolvesHiddenCompositeKeysOnSQLXCacheReplay(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE hidden_users (tenant TEXT, id INTEGER, PRIMARY KEY (tenant, id));`,
		`CREATE TABLE hidden_accounts (id INTEGER PRIMARY KEY, tenant TEXT, user_id INTEGER);`,
		`INSERT INTO hidden_users(tenant, id) VALUES ('acme', 1), ('acme', 2);`,
		`INSERT INTO hidden_accounts(id, tenant, user_id) VALUES (10, 'acme', 1), (20, 'acme', 2);`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	type input struct{}
	session := relationCacheSession(t, h, reflect.TypeOf(input{}), reflect.TypeOf(hiddenCompositeOutput{}), &spec.Component{
		Name:     "HiddenCompositeParent",
		RootView: &spec.View{Source: &spec.ViewSource{SQL: `SELECT tenant, id FROM hidden_users ORDER BY id`}},
		Parameters:   []*spec.Parameter{{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}}},
	})

	assertHiddenCompositeParents(t, readHiddenCompositeParents(t, session))
	if _, err := h.DB.Exec(`DELETE FROM hidden_accounts; DELETE FROM hidden_users`); err != nil {
		t.Fatalf("delete source rows: %v", err)
	}
	assertHiddenCompositeParents(t, readHiddenCompositeParents(t, session))
}

func readHiddenCompositeParents(t *testing.T, session *Session) *hiddenCompositeOutput {
	t.Helper()
	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	result, ok := actual.(*hiddenCompositeOutput)
	if !ok {
		t.Fatalf("unexpected output type %T", actual)
	}
	return result
}

func assertHiddenCompositeParents(t *testing.T, actual *hiddenCompositeOutput) {
	t.Helper()
	if len(actual.Data) != 2 {
		t.Fatalf("unexpected parent rows: %#v", actual)
	}
	for index, user := range actual.Data {
		if len(user.Accounts) != 1 {
			t.Fatalf("unexpected child rows for parent %d: %#v", index, user)
		}
		account := user.Accounts[0]
		if account.ParentTenant != user.Tenant || account.ParentID != user.ID {
			t.Fatalf("unexpected published parent for account %d: user=%#v account=%#v", account.ID, user, account)
		}
	}
}

type outputHookRoot struct {
	ID int
}

type outputHookRelation struct {
	Count         int
	FetchCount    int  `sqlx:"-"`
	RelationCount int  `sqlx:"-"`
	FetchFirst    bool `sqlx:"-"`
}

func (r *outputHookRelation) OnFetch(context.Context) error {
	r.FetchCount++
	r.FetchFirst = r.RelationCount == 0
	return nil
}

func (r *outputHookRelation) OnRelation(context.Context) {
	r.RelationCount++
}

type outputHookResult struct {
	Data   []*outputHookRoot
	Totals outputHookRelation
}

func TestService_OutputRelationHooksRunOnSQLXCacheReplay(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE output_hook_users (id INTEGER PRIMARY KEY);`,
		`INSERT INTO output_hook_users(id) VALUES (1), (2);`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	type input struct{}
	session := relationCacheSession(t, h, reflect.TypeOf(input{}), reflect.TypeOf(outputHookResult{}), &spec.Component{
		Name: "OutputHook",
		RootView: &spec.View{
			Name: "Users", Source: &spec.ViewSource{SQL: `SELECT id FROM output_hook_users ORDER BY id`},
			Relations: []*spec.Relation{outputRelation("Totals", `SELECT COUNT(*) AS count FROM ($View.Users.NonWindowSQL) parent`)},
		},
		Parameters: []*spec.Parameter{
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
			{Name: "Totals", Source: spec.BindSource{Kind: "output", Name: "summary"}},
		},
	})

	assertOutputHooks(t, readOutputHooks(t, session))
	if _, err := h.DB.Exec(`DELETE FROM output_hook_users`); err != nil {
		t.Fatalf("delete source rows: %v", err)
	}
	assertOutputHooks(t, readOutputHooks(t, session))
}

func readOutputHooks(t *testing.T, session *Session) *outputHookResult {
	t.Helper()
	actual, err := NewService().Read(context.Background(), session)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	result, ok := actual.(*outputHookResult)
	if !ok {
		t.Fatalf("unexpected output type %T", actual)
	}
	return result
}

func assertOutputHooks(t *testing.T, actual *outputHookResult) {
	t.Helper()
	if len(actual.Data) != 2 || actual.Totals.Count != 2 || actual.Totals.FetchCount != 1 || actual.Totals.RelationCount != 1 || !actual.Totals.FetchFirst {
		t.Fatalf("unexpected output hook state: %#v", actual)
	}
}
