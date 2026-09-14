package golang

import (
	"strconv"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
)

func TestLowerRecursivePatchExecutesTypedWriteHooks(t *testing.T) {
	component, semantic, bindings := recursivePatchFixture(t)
	runGeneratedRecursiveModule(t, component, semantic, bindings, recursiveHookRuntimeSource)
}

func TestLowerRecursivePatchExecutesOneToOneRelation(t *testing.T) {
	component, _, bindings := recursivePatchFixture(t)
	component.RootView.Relations[0].Cardinality = spec.CardinalityOne
	semantic := recursiveSemanticPlan(plan.OperationPatch)
	semantic.Root.Relations[0].Cardinality = spec.CardinalityOne
	semantic.Root.Relations[0].Child.Cardinality = spec.CardinalityOne
	runGeneratedRecursiveModule(t, component, semantic, bindings, recursiveOneRuntimeSource)
}

func TestLowerRecursivePatchExecutesCompoundChildKeyAndAddressLink(t *testing.T) {
	component, _, bindings := recursivePatchFixture(t)
	tenant := &spec.Column{Name: "TENANT_ID", Source: "TENANT_ID", Type: spec.TypeRef{Name: "int64"}}
	childTenant := &spec.Column{Name: "TENANT_ID", Source: "TENANT_ID", Type: spec.TypeRef{Name: "int64", Pointer: true}}
	component.RootView.Columns = append(component.RootView.Columns, tenant)
	component.RootView.Relations[0].View.Columns = append(component.RootView.Relations[0].View.Columns, childTenant)
	component.RootView.Relations[0].On = append(component.RootView.Relations[0].On,
		&spec.RelationLink{ParentColumn: tenant.Name, ChildColumn: childTenant.Name})
	component.Views[0].Columns = append(component.Views[0].Columns, tenant.Clone())
	component.Views[1].Columns = append(component.Views[1].Columns, childTenant.Clone())

	semantic := recursiveSemanticPlan(plan.OperationPatch)
	child := semantic.Root.Relations[0].Child
	tenantKey := plan.KeyPart{Field: "TenantId", Type: childTenant.Type}
	child.Keys = append(child.Keys, tenantKey)
	child.Current.Keys = append(child.Current.Keys, tenantKey)
	semantic.Root.Relations[0].Links = append(semantic.Root.Relations[0].Links, plan.KeyLink{
		Parent:     plan.KeyPart{Field: "TenantId", Source: "TENANT_ID", Type: tenant.Type},
		Child:      plan.KeyPart{Field: "TenantId", Source: "TENANT_ID", Type: childTenant.Type},
		Conversion: plan.LinkAddress,
	})
	if conversion := semantic.Root.Relations[0].Links[1].Conversion; conversion != plan.LinkAddress {
		t.Fatalf("tenant relation conversion = %q, want %q", conversion, plan.LinkAddress)
	}
	runGeneratedRecursiveModule(t, component, semantic, bindings, recursiveCompoundRuntimeSource)
}

const recursiveOneBody = `{"Data":[{"id":1,"name":"order-after","items":{"id":1,"name":"item-after","details":[{"id":1,"note":"detail-after"},{"note":"detail-new"}]}},{"name":"order-new","items":{"name":"new-order-item","details":[{"note":"new-order-detail"}]}}]}`

const recursiveOneAssertions = `
	if len(result.Data) != 2 || result.Data[1].Id == nil || *result.Data[1].Id != 2 { t.Fatalf("orders: %+v", result.Data) }
	if result.Data[0].Items == nil || result.Data[0].Items.Id == nil || *result.Data[0].Items.Id != 1 || result.Data[0].Items.OrderId != 1 { t.Fatalf("existing item: %+v", result.Data[0].Items) }
	if result.Data[1].Items == nil || result.Data[1].Items.Id == nil || *result.Data[1].Items.Id != 2 || result.Data[1].Items.OrderId != 2 { t.Fatalf("new item: %+v", result.Data[1].Items) }
	if len(result.Data[0].Items.Details) != 2 || result.Data[0].Items.Details[1].Id == nil || *result.Data[0].Items.Details[1].Id != 2 || result.Data[0].Items.Details[1].ItemId != 1 { t.Fatalf("existing details: %+v", result.Data[0].Items.Details) }
	var counts [3]int
	for index, table := range []string{"ORDERS", "ITEMS", "DETAILS"} {
		if err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&counts[index]); err != nil { t.Fatal(err) }
	}
	if counts != [3]int{2, 2, 3} { t.Fatalf("row counts: %v", counts) }
	var values string
	if err = db.QueryRowContext(ctx, "SELECT GROUP_CONCAT(ORDER_ID, ',') FROM (SELECT ORDER_ID FROM ITEMS ORDER BY ID)").Scan(&values); err != nil || values != "1,2" { t.Fatalf("item links=%q err=%v", values, err) }
	if err = db.QueryRowContext(ctx, "SELECT GROUP_CONCAT(ITEM_ID, ',') FROM (SELECT ITEM_ID FROM DETAILS ORDER BY ID)").Scan(&values); err != nil || values != "1,1,2" { t.Fatalf("detail links=%q err=%v", values, err) }
`

func recursiveOneRuntimeSource(inputType, outputType string) string {
	return recursiveRuntimeSourceCase(inputType, outputType, recursiveOneBody, recursiveOneAssertions)
}

const recursiveCompoundBody = `{"Data":[{"id":1,"tenantId":7,"name":"order-after","items":[{"id":1,"name":"item-after","details":[{"id":1,"note":"detail-after"},{"note":"detail-new"}]},{"name":"item-new","details":[{"note":"new-item-detail"}]}]},{"tenantId":8,"name":"order-new","items":[{"name":"new-order-item","details":[{"note":"new-order-detail"}]}]}]}`

const recursiveCompoundAssertions = recursiveManyAssertions + `
	if result.Data[0].Items[0].TenantId == nil || *result.Data[0].Items[0].TenantId != 7 || result.Data[0].Items[1].TenantId == nil || *result.Data[0].Items[1].TenantId != 7 || result.Data[1].Items[0].TenantId == nil || *result.Data[1].Items[0].TenantId != 8 { t.Fatalf("tenant links: %+v", result.Data) }
	if err = db.QueryRowContext(ctx, "SELECT GROUP_CONCAT(TENANT_ID, ',') FROM (SELECT TENANT_ID FROM ITEMS ORDER BY ID)").Scan(&values); err != nil || values != "7,7,8" { t.Fatalf("tenant links=%q err=%v", values, err) }
`

func recursiveCompoundRuntimeSource(inputType, outputType string) string {
	source := recursiveRuntimeSourceCase(inputType, outputType, recursiveCompoundBody, recursiveCompoundAssertions)
	replacements := map[string]string{
		"CREATE TABLE ORDERS (ID INTEGER PRIMARY KEY AUTOINCREMENT, NAME TEXT NOT NULL)":                                                 "CREATE TABLE ORDERS (ID INTEGER PRIMARY KEY AUTOINCREMENT, NAME TEXT NOT NULL, TENANT_ID INTEGER NOT NULL, UNIQUE(ID, TENANT_ID))",
		"CREATE TABLE ITEMS (ID INTEGER PRIMARY KEY AUTOINCREMENT, ORDER_ID INTEGER NOT NULL REFERENCES ORDERS(ID), NAME TEXT NOT NULL)": "CREATE TABLE ITEMS (ID INTEGER PRIMARY KEY AUTOINCREMENT, ORDER_ID INTEGER NOT NULL, NAME TEXT NOT NULL, TENANT_ID INTEGER NOT NULL, FOREIGN KEY (ORDER_ID, TENANT_ID) REFERENCES ORDERS(ID, TENANT_ID))",
		"INSERT INTO ORDERS(ID, NAME) VALUES (1, 'order-before')":                                                                        "INSERT INTO ORDERS(ID, NAME, TENANT_ID) VALUES (1, 'order-before', 7)",
		"INSERT INTO ITEMS(ID, ORDER_ID, NAME) VALUES (1, 1, 'item-before')":                                                             "INSERT INTO ITEMS(ID, ORDER_ID, NAME, TENANT_ID) VALUES (1, 1, 'item-before', 7)",
		"SELECT ID, NAME FROM ORDERS WHERE ID = 1":                                                                                       "SELECT ID, NAME, TENANT_ID FROM ORDERS WHERE ID = 1",
		"SELECT ID, ORDER_ID, NAME FROM ITEMS WHERE ID = 1":                                                                              "SELECT ID, ORDER_ID, NAME, TENANT_ID FROM ITEMS WHERE ID = 1",
		`{Name: "NAME", Source: "NAME", Type: spec.TypeRef{Name: "string"}},
		}},
		{Name: "CurrentItems"`: `{Name: "NAME", Source: "NAME", Type: spec.TypeRef{Name: "string"}},
			{Name: "TENANT_ID", Source: "TENANT_ID", Type: spec.TypeRef{Name: "int64"}},
		}},
		{Name: "CurrentItems"`,
		`{Name: "NAME", Source: "NAME", Type: spec.TypeRef{Name: "string"}},
		}},
		{Name: "CurrentDetails"`: `{Name: "NAME", Source: "NAME", Type: spec.TypeRef{Name: "string"}},
			{Name: "TENANT_ID", Source: "TENANT_ID", Type: spec.TypeRef{Name: "int64", Pointer: true}},
		}},
		{Name: "CurrentDetails"`,
	}
	for before, after := range replacements {
		if strings.Count(source, before) != 1 {
			panic("recursive runtime fixture fragment not found exactly once: " + before)
		}
		source = strings.Replace(source, before, after, 1)
	}
	return source
}

func recursiveHookRuntimeSource(inputType, outputType string) string {
	hookAssertions := recursiveManyAssertions + `
	wantHookEvents := "order:init,order:validate,order:items,item:init,item:validate,item:details,detail:init,detail:validate,detail:init,detail:validate,item:init,item:validate,item:details,detail:init,detail:validate,order:init,order:validate,order:items,item:init,item:validate,item:details,detail:init,detail:validate"
	if actual := strings.Join(writeHookEvents, ","); actual != wantHookEvents { t.Fatalf("write hooks:\n got %s\nwant %s", actual, wantHookEvents) }
	assertCounts := func(want [3]int) {
		t.Helper()
		var actual [3]int
		for index, table := range []string{"ORDERS", "ITEMS", "DETAILS"} {
			if scanErr := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&actual[index]); scanErr != nil { t.Fatal(scanErr) }
		}
		if actual != want { t.Fatalf("row counts after rejected hook: %v", actual) }
	}
	runRejected := func(body, fragment string) {
		t.Helper()
		writeHookEvents = nil
		rejectedRequest := httptest.NewRequest("PATCH", "/orders", strings.NewReader(body))
		rejectedRequest.Header.Set("Content-Type", "application/json")
		rejectedScope, scopeErr := requestprovider.New(rejectedRequest)
		if scopeErr != nil { t.Fatal(scopeErr) }
		_, executeErr := handlerengine.New().Execute(ctx, handlerengine.Request{
			Input: routeInput, Scope: rejectedScope,
			Providers: []locator.Provider{viewProvider}, DataSource: sqldml.Source{DB: db},
			Handler: customhandler.New[{{INPUT}}, {{OUTPUT}}](NewOrdersHandler()),
		})
		if executeErr == nil || !strings.Contains(executeErr.Error(), fragment) { t.Fatalf("rejected hook error = %v, want %q", executeErr, fragment) }
		assertCounts([3]int{2, 3, 4})
	}
	runRejected(` + strconv.Quote(`{"Data":[{"id":1,"name":"order-not-flushed","items":[{"id":1,"name":"validation-reject"}]}]}`) + `, "item validation rejected")
	runRejected(` + strconv.Quote(`{"Data":[{"id":1,"name":"order-not-flushed","items":[{"id":1,"name":"relation-reject","details":[{"id":1,"note":"not-flushed"}]}]}]}`) + `, "detail relation rejected")
	var persistedName string
	if err = db.QueryRowContext(ctx, "SELECT NAME FROM ORDERS WHERE ID = 1").Scan(&persistedName); err != nil || persistedName != "order-after" { t.Fatalf("rejected parent update persisted: name=%q err=%v", persistedName, err) }
`
	source := recursiveRuntimeSourceCase(inputType, outputType, recursiveManyBody, hookAssertions)
	source = strings.ReplaceAll(source, "{{INPUT}}", inputType)
	source = strings.ReplaceAll(source, "{{OUTPUT}}", outputType)
	source = strings.Replace(source, "\t\"context\"\n", "\t\"context\"\n\t\"errors\"\n", 1)
	hooks := `var writeHookEvents []string

func (o *Order) InitWrite(context.Context) error {
	writeHookEvents = append(writeHookEvents, "order:init")
	return nil
}

func (o *Order) ValidateWrite(context.Context) error {
	writeHookEvents = append(writeHookEvents, "order:validate")
	return nil
}

func (o *Order) BeforeItemsWrite(_ context.Context, items []*Item) error {
	writeHookEvents = append(writeHookEvents, "order:items")
	return nil
}

func (i *Item) InitWrite(context.Context) error {
	writeHookEvents = append(writeHookEvents, "item:init")
	if i.OrderId == 0 { return errors.New("item relation key was not propagated") }
	return nil
}

func (i *Item) ValidateWrite(context.Context) error {
	writeHookEvents = append(writeHookEvents, "item:validate")
	if i.Name == "validation-reject" { return errors.New("item validation rejected") }
	return nil
}

func (i *Item) BeforeDetailsWrite(_ context.Context, details []*Detail) error {
	writeHookEvents = append(writeHookEvents, "item:details")
	if i.Name == "relation-reject" { return errors.New("detail relation rejected") }
	return nil
}

func (d *Detail) InitWrite(context.Context) error {
	writeHookEvents = append(writeHookEvents, "detail:init")
	if d.ItemId == 0 { return errors.New("detail relation key was not propagated") }
	return nil
}

func (d *Detail) ValidateWrite(context.Context) error {
	writeHookEvents = append(writeHookEvents, "detail:validate")
	return nil
}

`
	source = strings.Replace(source, "func TestGeneratedRecursiveGoHandler", hooks+"func TestGeneratedRecursiveGoHandler", 1)
	return source
}
