package golang

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
	gen "github.com/viant/datly/transcribe/generate"
	plan "github.com/viant/datly/transcribe/handler/ast"
)

func TestLowerRecursivePatchGeneratesTypedThreeLevelTraversal(t *testing.T) {
	component, semantic, bindings := recursivePatchFixture(t)
	_ = component
	semantic.Root.Relations[0].Child.PresenceFields = []string{"OrderId"}
	semantic.Root.Relations[0].Child.Relations[0].Child.PresenceFields = []string{"ItemId"}
	asset, err := Lower(semantic, recursiveGoConfig(semantic, nil))
	if err != nil {
		t.Fatalf("Lower() error = %v", err)
	}
	source, err := asset.Source()
	if err != nil {
		t.Fatal(err)
	}
	text := string(source)
	for _, expected := range []string{
		`dependencies.Sequencer.Allocate(ctx, "ORDERS", input.Orders, "Id")`,
		`dependencies.Sequencer.Allocate(ctx, "ITEMS", input.Orders, "Items/Id")`,
		`dependencies.Sequencer.Allocate(ctx, "DETAILS", input.Orders, "Items/Details/Id")`,
		`current1ByKey := make(map[int64]*CurrentItem`,
		`current2ByKey := make(map[int64]*CurrentDetail`,
		`record1.OrderId = *record0.Id`,
		`if record1.Has == nil`,
		`record1.Has = &ItemHas{}`,
		`record1.Has.OrderId = true`,
		`record2.ItemId = *record1.Id`,
		`record2.Has.ItemId = true`,
		`type ordersHandlerContractRelation0Writer interface`,
		`BeforeDetailsWrite(context.Context, []*Detail) error`,
		`type ordersHandlerContractRelation1Writer interface`,
		`BeforeItemsWrite(context.Context, []*Item) error`,
		`interface{}(record0).(xhandler.WriteInitializer)`,
		`writeInitializer0.InitWrite(ctx)`,
		`interface{}(record0).(xhandler.WriteValidator)`,
		`writeValidator0.ValidateWrite(ctx)`,
		`relationWriter1.BeforeItemsWrite(ctx, record0.Items)`,
		`dependencies.DML.Update("DETAILS", record2)`,
		`dependencies.DML.Insert("DETAILS", record2)`,
	} {
		if !strings.Contains(text, expected) {
			t.Fatalf("generated recursive source missing %q:\n%s", expected, text)
		}
	}
	compileGeneratedHandler(t, source, recursiveContractSource())
	if len(bindings) != 3 {
		t.Fatalf("recursive view bindings = %v", bindings)
	}
}

func TestLowerRecursivePostAndPutRetainTypedTraversal(t *testing.T) {
	tests := []struct {
		name      string
		operation plan.Operation
		method    string
	}{
		{name: "post", operation: plan.OperationPost, method: "Insert"},
		{name: "put", operation: plan.OperationPut, method: "Update"},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			semantic := recursiveSemanticPlan(testCase.operation)
			config := Config{
				Package: "orders", Factory: "NewOrdersHandler", InputType: "Input", OutputType: "Output",
				Records: []RecordType{
					{Identity: semantic.Root.Identity, Path: append(plan.FieldPath(nil), semantic.Root.InputPath...), Value: "[]*Order"},
					{Identity: semantic.Root.Relations[0].Child.Identity, Path: append(plan.FieldPath(nil), semantic.Root.Relations[0].Child.InputPath...), Value: "[]*Item"},
					{Identity: semantic.Root.Relations[0].Child.Relations[0].Child.Identity, Path: append(plan.FieldPath(nil), semantic.Root.Relations[0].Child.Relations[0].Child.InputPath...), Value: "[]*Detail"},
				},
			}
			asset, err := Lower(semantic, config)
			if err != nil {
				t.Fatalf("Lower() error = %v", err)
			}
			source, err := asset.Source()
			if err != nil {
				t.Fatal(err)
			}
			text := string(source)
			for _, expected := range []string{
				`record1.OrderId = *record0.Id`,
				`record2.ItemId = *record1.Id`,
				`dependencies.DML.` + testCase.method + `("ORDERS", record0)`,
				`dependencies.DML.` + testCase.method + `("ITEMS", record1)`,
				`dependencies.DML.` + testCase.method + `("DETAILS", record2)`,
			} {
				if !strings.Contains(text, expected) {
					t.Fatalf("generated recursive %s source missing %q:\n%s", testCase.operation, expected, text)
				}
			}
			if strings.Contains(text, "currentByKey") || strings.Contains(text, "CurrentKey") {
				t.Fatalf("generated recursive %s source contains PATCH index logic:\n%s", testCase.operation, text)
			}
			compileGeneratedHandler(t, source, recursiveContractSource())
		})
	}
}

func TestLowerRecursivePatchRequiresEveryTargetType(t *testing.T) {
	_, semantic, _ := recursivePatchFixture(t)
	config := recursiveGoConfig(semantic, nil)
	config.Records = config.Records[:2]
	_, err := Lower(semantic, config)
	if err == nil || !strings.Contains(err.Error(), "has no target type binding") {
		t.Fatalf("Lower() error = %v", err)
	}
}

func TestLowerRecursivePatchRetainsNestedSequencerCapability(t *testing.T) {
	_, semantic, _ := recursivePatchFixture(t)
	semantic.Root.Sequence = nil
	asset, err := Lower(semantic, recursiveGoConfig(semantic, nil))
	if err != nil {
		t.Fatalf("Lower() error = %v", err)
	}
	source, err := asset.Source()
	if err != nil {
		t.Fatal(err)
	}
	text := string(source)
	if !strings.Contains(text, "kind=sequencer") || strings.Contains(text, `Allocate(ctx, "ORDERS"`) ||
		!strings.Contains(text, `Allocate(ctx, "ITEMS"`) {
		t.Fatalf("nested sequence capability/source:\n%s", text)
	}
}

func TestLowerRecursivePatchRejectsDuplicateAndUnusedTypeBindings(t *testing.T) {
	_, semantic, _ := recursivePatchFixture(t)
	t.Run("duplicate", func(t *testing.T) {
		config := recursiveGoConfig(semantic, nil)
		config.Records = append(config.Records, config.Records[0])
		_, err := Lower(semantic, config)
		if err == nil || !strings.Contains(err.Error(), "is duplicated") {
			t.Fatalf("Lower() error = %v", err)
		}
	})
	t.Run("unused", func(t *testing.T) {
		config := recursiveGoConfig(semantic, nil)
		config.Records = append(config.Records, RecordType{
			Identity: "view:Unused", Path: plan.FieldPath{"Input", "Unused"},
			Value: "[]*Unused", Current: "[]*CurrentUnused",
		})
		_, err := Lower(semantic, config)
		if err == nil || !strings.Contains(err.Error(), "is unused") {
			t.Fatalf("Lower() error = %v", err)
		}
	})
}

func TestLowerRecursivePatchSupportsOneToOneRelation(t *testing.T) {
	_, semantic, _ := recursivePatchFixture(t)
	relation := semantic.Root.Relations[0]
	relation.Cardinality = spec.CardinalityOne
	relation.Child.Cardinality = spec.CardinalityOne
	config := recursiveGoConfig(semantic, nil)
	config.Records[1].Value = "*Item"
	asset, err := Lower(semantic, config)
	if err != nil {
		t.Fatalf("Lower() error = %v", err)
	}
	source, err := asset.Source()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(source), "record1 := record0.Items") ||
		!strings.Contains(string(source), "if record1 != nil") ||
		!strings.Contains(string(source), "BeforeItemsWrite(context.Context, *Item) error") {
		t.Fatalf("generated one-to-one traversal:\n%s", source)
	}
	contract := strings.Replace(recursiveContractSource(), "Items []*Item", "Items *Item", 1)
	compileGeneratedHandler(t, source, contract)
}

func TestLowerRecursivePatchSupportsCompoundChildKeyAndLinkConversions(t *testing.T) {
	_, semantic, _ := recursivePatchFixture(t)
	child := semantic.Root.Relations[0].Child
	tenantKey := plan.KeyPart{Field: "TenantId", Type: spec.TypeRef{Name: "int64", Pointer: true}}
	child.Keys = append(child.Keys, tenantKey)
	child.Current.Keys = append(child.Current.Keys, tenantKey)
	relation := semantic.Root.Relations[0]
	relation.Links = append(relation.Links,
		plan.KeyLink{Parent: plan.KeyPart{Field: "TenantId"}, Child: plan.KeyPart{Field: "TenantId"}, Conversion: plan.LinkAddress},
		plan.KeyLink{Parent: plan.KeyPart{Field: "RegionId"}, Child: plan.KeyPart{Field: "RegionId"}, Conversion: plan.LinkDirect},
	)
	asset, err := Lower(semantic, recursiveGoConfig(semantic, nil))
	if err != nil {
		t.Fatalf("Lower() error = %v", err)
	}
	source, err := asset.Source()
	if err != nil {
		t.Fatal(err)
	}
	text := string(source)
	for _, expected := range []string{
		"type ordersHandlerContractRelation1Key struct",
		"TenantId: *value.TenantId",
		"record1.TenantId = &record0.TenantId",
		"record1.RegionId = record0.RegionId",
	} {
		if !strings.Contains(text, expected) {
			t.Fatalf("generated compound/link source missing %q:\n%s", expected, text)
		}
	}
	contract := strings.NewReplacer(
		"type Item struct { Id *int64; OrderId int64; Name string; Details []*Detail; Has *ItemHas }",
		"type Item struct { Id *int64; OrderId int64; TenantId *int64; RegionId int64; Name string; Details []*Detail; Has *ItemHas }",
		"type Order struct { Id *int64; Name string; Items []*Item }",
		"type Order struct { Id *int64; TenantId int64; RegionId int64; Name string; Items []*Item }",
		"type CurrentItem struct { Id *int64; OrderId int64; Name string }",
		"type CurrentItem struct { Id *int64; OrderId int64; TenantId *int64; RegionId int64; Name string }",
	).Replace(recursiveContractSource())
	compileGeneratedHandler(t, source, contract)
}

func TestLowerRecursivePatchSupportsRepeatedViewOccurrences(t *testing.T) {
	_, semantic, _ := recursivePatchFixture(t)
	first := semantic.Root.Relations[0].Child
	second := *first
	second.InputPath = plan.FieldPath{"Input", "Orders", "AlternateItems"}
	second.Sequence = &plan.SequencePlan{Destination: plan.FieldPath{"Input", "Orders"}, Selector: plan.FieldPath{"AlternateItems", "Id"}}
	second.Write = first.Write
	second.Write.ValuePath = append(plan.FieldPath(nil), second.InputPath...)
	second.Write.Order = 3
	second.Relations = nil
	semantic.Root.Relations = append(semantic.Root.Relations, &plan.RelationPlan{
		Identity: "AlternateItems", FieldPath: plan.FieldPath{"AlternateItems"}, Cardinality: spec.CardinalityMany,
		Links: append([]plan.KeyLink(nil), semantic.Root.Relations[0].Links...), Child: &second,
	})
	config := recursiveGoConfig(semantic, nil)
	config.Records = append(config.Records, RecordType{
		Identity: second.Identity, Path: append(plan.FieldPath(nil), second.InputPath...),
		Value: "[]*Item", Current: "[]*CurrentItem",
	})
	asset, err := Lower(semantic, config)
	if err != nil {
		t.Fatalf("Lower() error = %v", err)
	}
	source, err := asset.Source()
	if err != nil {
		t.Fatal(err)
	}
	text := string(source)
	if !strings.Contains(text, "for _, record3 := range record0.AlternateItems") ||
		!strings.Contains(text, "current1ByKey[recordKey3]") ||
		strings.Count(text, "current1ByKey := make(") != 1 ||
		strings.Contains(text, "current3ByKey := make(") {
		t.Fatalf("repeated-view lowering did not reuse typed index authority:\n%s", text)
	}
	contract := strings.Replace(recursiveContractSource(),
		"type Order struct { Id *int64; Name string; Items []*Item }",
		"type Order struct { Id *int64; Name string; Items []*Item; AlternateItems []*Item }", 1)
	compileGeneratedHandler(t, source, contract)
}

func TestLowerRecursivePatchExecutesThroughUnifiedEngine(t *testing.T) {
	component, semantic, bindings := recursivePatchFixture(t)
	runGeneratedRecursiveModule(t, component, semantic, bindings, recursiveRuntimeSource)
}

func runGeneratedRecursiveModule(t *testing.T, component *spec.Component, semantic *plan.Plan, bindings gen.ViewBindings, runtimeSource func(string, string) string) {
	t.Helper()
	targetPackage := "example.com/generated/orders"
	setMarkerViews := recursiveSetMarkerViews(semantic.Root)
	generatedPlan, err := gen.New(gen.Input{
		Component: component, TargetPackage: targetPackage, ViewBindings: bindings, SetMarkerViews: setMarkerViews,
	}).Plan()
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}
	applyRecursivePresenceFields(t, semantic.Root, generatedPlan)
	config := recursiveGoConfig(semantic, generatedPlan.Imports)
	config.InputType = generatedPlan.Input.Type
	config.OutputType = generatedPlan.Output.Type
	config.Records = generatedRecursiveRecordTypes(t, semantic, generatedPlan)
	asset, err := Lower(semantic, config)
	if err != nil {
		t.Fatalf("Lower() error = %v", err)
	}
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	packageDir := filepath.Join(root, "orders")
	generated, err := gen.New(gen.Input{
		Component: component, TargetPackage: targetPackage, ViewBindings: bindings, SetMarkerViews: setMarkerViews,
		ContractHandler: &gen.ContractHandlerAsset{Factory: asset.Factory, File: asset.File},
	}).Generate(packageDir)
	if err != nil {
		t.Fatalf("Generate() error = %v", err)
	}
	source := runtimeSource(generated.Plan.Input.Type, generated.Plan.Output.Type)
	if err = os.WriteFile(filepath.Join(packageDir, "recursive_runtime_test.go"), []byte(source), 0o644); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("go", "test", "-mod=mod", "./...")
	command.Dir = root
	if output, runErr := command.CombinedOutput(); runErr != nil {
		t.Fatalf("generated recursive Go module failed: %v\n%s\n%s", runErr, output, source)
	}
}

func recursiveSetMarkerViews(root *plan.RecordPlan) map[string]bool {
	result := map[string]bool{}
	var visit func(*plan.RecordPlan)
	visit = func(record *plan.RecordPlan) {
		if record == nil {
			return
		}
		result[record.Identity] = true
		for _, relation := range record.Relations {
			if relation != nil {
				visit(relation.Child)
			}
		}
	}
	visit(root)
	return result
}

func applyRecursivePresenceFields(t *testing.T, root *plan.RecordPlan, generated *gen.Plan) {
	t.Helper()
	byIdentity := map[string][]string{}
	for _, view := range generated.Views {
		if view.Ownership == gen.ViewGenerated && len(view.SetMarkerFields) > 0 {
			byIdentity[view.Identity] = view.SetMarkerFields
		}
	}
	var apply func(*plan.RecordPlan)
	apply = func(record *plan.RecordPlan) {
		if record == nil {
			t.Fatal("nil recursive record")
		}
		record.PresenceFields = append([]string(nil), byIdentity[record.Identity]...)
		for _, relation := range record.Relations {
			if relation == nil || relation.Child == nil {
				t.Fatal("incomplete recursive relation")
			}
			apply(relation.Child)
		}
	}
	apply(root)
}

func recursivePatchFixture(t *testing.T) (*spec.Component, *plan.Plan, gen.ViewBindings) {
	t.Helper()
	pointerInt := spec.TypeRef{Name: "int64", Pointer: true}
	integer := spec.TypeRef{Name: "int64"}
	detail := &spec.View{
		Name: "Details", TypeName: "Detail", Source: &spec.ViewSource{Table: "DETAILS"},
		Columns: []*spec.Column{
			{Name: "ID", Source: "ID", Type: pointerInt, PrimaryKey: true},
			{Name: "ITEM_ID", Source: "ITEM_ID", Type: integer},
			{Name: "NOTE", Source: "NOTE", Type: spec.TypeRef{Name: "string"}},
		},
	}
	item := &spec.View{
		Name: "Items", TypeName: "Item", Source: &spec.ViewSource{Table: "ITEMS"},
		Columns: []*spec.Column{
			{Name: "ID", Source: "ID", Type: pointerInt, PrimaryKey: true},
			{Name: "ORDER_ID", Source: "ORDER_ID", Type: integer},
			{Name: "NAME", Source: "NAME", Type: spec.TypeRef{Name: "string"}},
		},
		Relations: []*spec.Relation{{
			Name: "Details", Holder: "Details", Cardinality: spec.CardinalityMany, View: detail,
			On: []*spec.RelationLink{{ParentColumn: "ID", ChildColumn: "ITEM_ID"}},
		}},
	}
	root := &spec.View{
		Name: "Orders", TypeName: "Order", Source: &spec.ViewSource{Table: "ORDERS"},
		Columns: []*spec.Column{
			{Name: "ID", Source: "ID", Type: pointerInt, PrimaryKey: true},
			{Name: "NAME", Source: "NAME", Type: spec.TypeRef{Name: "string"}},
		},
		Relations: []*spec.Relation{{
			Name: "Items", Holder: "Items", Cardinality: spec.CardinalityMany, View: item,
			On: []*spec.RelationLink{{ParentColumn: "ID", ChildColumn: "ORDER_ID"}},
		}},
	}
	currentOrder := currentView("CurrentOrders", "CurrentOrder", root)
	currentItem := currentView("CurrentItems", "CurrentItem", item)
	currentDetail := currentView("CurrentDetails", "CurrentDetail", detail)
	component := &spec.Component{
		Name: "Orders", Routes: []*spec.Route{{Method: "PATCH", Path: "/orders"}}, RootView: root,
		Parameters: []*spec.Parameter{
			{Name: "Orders", Source: spec.BindSource{Kind: "body", Name: "Data"}, TypeExpr: "[]*Order", Cardinality: string(spec.CardinalityMany)},
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "body"}, TypeExpr: "[]*Order", EmitOutput: true},
			{Name: "CurrentOrders", Source: spec.BindSource{Kind: "view", Name: "CurrentOrders"}, TypeExpr: "[]*CurrentOrder", Cardinality: string(spec.CardinalityMany)},
			{Name: "CurrentItems", Source: spec.BindSource{Kind: "view", Name: "CurrentItems"}, TypeExpr: "[]*CurrentItem", Cardinality: string(spec.CardinalityMany)},
			{Name: "CurrentDetails", Source: spec.BindSource{Kind: "view", Name: "CurrentDetails"}, TypeExpr: "[]*CurrentDetail", Cardinality: string(spec.CardinalityMany)},
		},
		Views: []*spec.View{currentOrder, currentItem, currentDetail},
	}
	bindings := gen.ViewBindings{}
	for index, view := range component.Views {
		identity, err := view.Identity()
		if err != nil {
			t.Fatal(err)
		}
		bindings[component.Parameters[index+2].Identity()] = identity
	}
	semantic := recursiveSemanticPlan(plan.OperationPatch)
	return component, semantic, bindings
}

func currentView(name, typeName string, source *spec.View) *spec.View {
	result := &spec.View{Name: name, TypeName: typeName}
	for _, column := range source.Columns {
		result.Columns = append(result.Columns, column.Clone())
	}
	return result
}

func recursiveGoConfig(semantic *plan.Plan, imports []spec.ImportSpec) Config {
	return Config{
		Package: "orders", Factory: "NewOrdersHandler", InputType: "Input", OutputType: "Output", Imports: imports,
		Records: []RecordType{
			{Identity: semantic.Root.Identity, Path: append(plan.FieldPath(nil), semantic.Root.InputPath...), Value: "[]*Order", Current: "[]*CurrentOrder"},
			{Identity: semantic.Root.Relations[0].Child.Identity, Path: append(plan.FieldPath(nil), semantic.Root.Relations[0].Child.InputPath...), Value: "[]*Item", Current: "[]*CurrentItem"},
			{Identity: semantic.Root.Relations[0].Child.Relations[0].Child.Identity, Path: append(plan.FieldPath(nil), semantic.Root.Relations[0].Child.Relations[0].Child.InputPath...), Value: "[]*Detail", Current: "[]*CurrentDetail"},
		},
	}
}

func generatedRecursiveRecordTypes(t *testing.T, semantic *plan.Plan, generated *gen.Plan) []RecordType {
	t.Helper()
	rootType := generatedInputFieldType(t, generated, semantic.Root.InputPath)
	var result []RecordType
	var appendRecord func(*plan.RecordPlan, string)
	appendRecord = func(record *plan.RecordPlan, valueType string) {
		currentType := generatedInputFieldType(t, generated, record.Current.InputPath)
		result = append(result, RecordType{
			Identity: record.Identity, Path: append(plan.FieldPath(nil), record.InputPath...),
			Value: valueType, Current: currentType,
		})
		shape, err := parseRecordShape(valueType, record.Cardinality)
		if err != nil {
			t.Fatalf("parse generated record %q type %q: %v", record.Identity, valueType, err)
		}
		var view *gen.ViewPlan
		for index := range generated.Views {
			if generated.Views[index].Type == shape.base {
				view = &generated.Views[index]
				break
			}
		}
		if len(record.Relations) != 0 && view == nil {
			t.Fatalf("generated record %q type %q has no view plan", record.Identity, shape.base)
		}
		for _, relation := range record.Relations {
			var childType string
			for _, field := range view.Fields {
				if field.Name == relation.FieldPath[len(relation.FieldPath)-1] {
					childType = field.Type
					break
				}
			}
			if childType == "" {
				t.Fatalf("generated view %q has no relation field %q", view.Type, relation.FieldPath)
			}
			appendRecord(relation.Child, childType)
		}
	}
	appendRecord(semantic.Root, rootType)
	return result
}

func recursiveContractSource() string {
	return `package orders

type DetailHas struct { ItemId bool }
type Detail struct { Id *int64; ItemId int64; Note string; Has *DetailHas }
type ItemHas struct { OrderId bool }
type Item struct { Id *int64; OrderId int64; Name string; Details []*Detail; Has *ItemHas }
type Order struct { Id *int64; Name string; Items []*Item }
type CurrentOrder struct { Id *int64; Name string }
type CurrentItem struct { Id *int64; OrderId int64; Name string }
type CurrentDetail struct { Id *int64; ItemId int64; Note string }
type Input struct {
	Orders []*Order
	CurrentOrders []*CurrentOrder
	CurrentItems []*CurrentItem
	CurrentDetails []*CurrentDetail
}
type Output struct { Data []*Order }
`
}

func recursiveRuntimeSource(inputType, outputType string) string {
	return recursiveRuntimeSourceCase(inputType, outputType, recursiveManyBody, recursiveManyAssertions)
}

const recursiveManyBody = `{"Data":[{"id":1,"name":"order-after","items":[{"id":1,"name":"item-after","details":[{"id":1,"note":"detail-after"},{"note":"detail-new"}]},{"name":"item-new","details":[{"note":"new-item-detail"}]}]},{"name":"order-new","items":[{"name":"new-order-item","details":[{"note":"new-order-detail"}]}]}]}`

const recursiveManyAssertions = `
	if len(result.Data) != 2 || result.Data[1].Id == nil || *result.Data[1].Id != 2 { t.Fatalf("orders: %+v", result.Data) }
	if len(result.Data[0].Items) != 2 || result.Data[0].Items[1].Id == nil || *result.Data[0].Items[1].Id != 2 || result.Data[0].Items[1].OrderId != 1 { t.Fatalf("items: %+v", result.Data) }
	if len(result.Data[1].Items) != 1 || result.Data[1].Items[0].Id == nil || *result.Data[1].Items[0].Id != 3 || result.Data[1].Items[0].OrderId != 2 { t.Fatalf("new order items: %+v", result.Data) }
	if len(result.Data[0].Items[0].Details) != 2 || result.Data[0].Items[0].Details[1].Id == nil || *result.Data[0].Items[0].Details[1].Id != 2 || result.Data[0].Items[0].Details[1].ItemId != 1 { t.Fatalf("details: %+v", result.Data) }
	var counts [3]int
	for index, table := range []string{"ORDERS", "ITEMS", "DETAILS"} {
		if err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&counts[index]); err != nil { t.Fatal(err) }
	}
	if counts != [3]int{2, 3, 4} { t.Fatalf("row counts: %v", counts) }
	var values string
	if err = db.QueryRowContext(ctx, "SELECT GROUP_CONCAT(ORDER_ID, ',') FROM (SELECT ORDER_ID FROM ITEMS ORDER BY ID)").Scan(&values); err != nil || values != "1,1,2" { t.Fatalf("item links=%q err=%v", values, err) }
	if err = db.QueryRowContext(ctx, "SELECT GROUP_CONCAT(ITEM_ID, ',') FROM (SELECT ITEM_ID FROM DETAILS ORDER BY ID)").Scan(&values); err != nil || values != "1,1,2,3" { t.Fatalf("detail links=%q err=%v", values, err) }
	if err = db.QueryRowContext(ctx, "SELECT GROUP_CONCAT(NOTE, ',') FROM (SELECT NOTE FROM DETAILS ORDER BY ID)").Scan(&values); err != nil || values != "detail-after,detail-new,new-item-detail,new-order-detail" { t.Fatalf("details=%q err=%v", values, err) }
`

func recursiveRuntimeSourceCase(inputType, outputType, body, assertions string) string {
	source := `package orders

import (
	"context"
	"database/sql"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/viant/bindly/locator"
	requestprovider "github.com/viant/bindly/provider/request"
	"github.com/viant/datly/bootstrap"
	customhandler "github.com/viant/datly/runtime/handler/custom"
	handlerengine "github.com/viant/datly/runtime/handler/engine"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	sqldml "github.com/viant/datly/sql/dml"
	viewprovider "github.com/viant/datly/sql/reader/provider"
	_ "github.com/viant/sqlx/metadata/product/sqlite"
)

func TestGeneratedRecursiveGoHandler(t *testing.T) {
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "orders.db"))
	if err != nil { t.Fatal(err) }
	defer db.Close()
	ctx := context.Background()
	if _, err = db.ExecContext(ctx, "PRAGMA foreign_keys = ON"); err != nil { t.Fatal(err) }
	for _, statement := range []string{
		"CREATE TABLE ORDERS (ID INTEGER PRIMARY KEY AUTOINCREMENT, NAME TEXT NOT NULL)",
		"CREATE TABLE ITEMS (ID INTEGER PRIMARY KEY AUTOINCREMENT, ORDER_ID INTEGER NOT NULL REFERENCES ORDERS(ID), NAME TEXT NOT NULL)",
		"CREATE TABLE DETAILS (ID INTEGER PRIMARY KEY AUTOINCREMENT, ITEM_ID INTEGER NOT NULL REFERENCES ITEMS(ID), NOTE TEXT NOT NULL)",
		"INSERT INTO ORDERS(ID, NAME) VALUES (1, 'order-before')",
		"INSERT INTO ITEMS(ID, ORDER_ID, NAME) VALUES (1, 1, 'item-before')",
		"INSERT INTO DETAILS(ID, ITEM_ID, NOTE) VALUES (1, 1, 'detail-before')",
		"PRAGMA foreign_keys = OFF",
		"UPDATE ITEMS SET ORDER_ID = 99 WHERE ID = 1",
		"UPDATE DETAILS SET ITEM_ID = 99 WHERE ID = 1",
		"PRAGMA foreign_keys = ON",
	} {
		if _, err = db.ExecContext(ctx, statement); err != nil { t.Fatal(err) }
	}
	component := &spec.Component{Routes: []*spec.Route{{Method: "PATCH", Path: "/orders"}}, Parameters: []*spec.Parameter{
		{Name: "Orders", Source: spec.BindSource{Kind: "body", Name: "Data"}},
		{Name: "CurrentOrders", Source: spec.BindSource{Kind: "view", Name: "CurrentOrders"}, Cardinality: string(spec.CardinalityMany)},
		{Name: "CurrentItems", Source: spec.BindSource{Kind: "view", Name: "CurrentItems"}, Cardinality: string(spec.CardinalityMany)},
		{Name: "CurrentDetails", Source: spec.BindSource{Kind: "view", Name: "CurrentDetails"}, Cardinality: string(spec.CardinalityMany)},
		{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "body"}, EmitOutput: true},
	}, Views: []*spec.View{
		{Name: "CurrentOrders", TypeName: "CurrentOrder", Source: &spec.ViewSource{SQL: "SELECT ID, NAME FROM ORDERS WHERE ID = 1"}, Columns: []*spec.Column{
			{Name: "ID", Source: "ID", Type: spec.TypeRef{Name: "int64", Pointer: true}, PrimaryKey: true},
			{Name: "NAME", Source: "NAME", Type: spec.TypeRef{Name: "string"}},
		}},
		{Name: "CurrentItems", TypeName: "CurrentItem", Source: &spec.ViewSource{SQL: "SELECT ID, ORDER_ID, NAME FROM ITEMS WHERE ID = 1"}, Columns: []*spec.Column{
			{Name: "ID", Source: "ID", Type: spec.TypeRef{Name: "int64", Pointer: true}, PrimaryKey: true},
			{Name: "ORDER_ID", Source: "ORDER_ID", Type: spec.TypeRef{Name: "int64"}},
			{Name: "NAME", Source: "NAME", Type: spec.TypeRef{Name: "string"}},
		}},
		{Name: "CurrentDetails", TypeName: "CurrentDetail", Source: &spec.ViewSource{SQL: "SELECT ID, ITEM_ID, NOTE FROM DETAILS WHERE ID = 1"}, Columns: []*spec.Column{
			{Name: "ID", Source: "ID", Type: spec.TypeRef{Name: "int64", Pointer: true}, PrimaryKey: true},
			{Name: "ITEM_ID", Source: "ITEM_ID", Type: spec.TypeRef{Name: "int64"}},
			{Name: "NOTE", Source: "NOTE", Type: spec.TypeRef{Name: "string"}},
		}},
	}}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{
		Component: component, InputType: reflect.TypeOf({{INPUT}}{}), OutputType: reflect.TypeOf({{OUTPUT}}{}),
	})
	if err != nil { t.Fatal(err) }
	routeInput, ok := artifact.Input.ForRoute(spec.RouteRef{Method: "PATCH", Path: "/orders"})
	if !ok { t.Fatal("compiled route input contract was not found") }
	viewProvider, err := viewprovider.New(viewprovider.Config{
		Dependencies: artifact.ViewDependencies, Input: artifact.Input, SQL: &dsql.SQLComponent{DB: db},
	})
	if err != nil { t.Fatal(err) }
	request := httptest.NewRequest("PATCH", "/orders", strings.NewReader({{BODY}}))
	request.Header.Set("Content-Type", "application/json")
	scope, err := requestprovider.New(request)
	if err != nil { t.Fatal(err) }
	actual, err := handlerengine.New().Execute(ctx, handlerengine.Request{
		Input: routeInput, Scope: scope,
		Providers: []locator.Provider{viewProvider}, DataSource: sqldml.Source{DB: db},
		Handler: customhandler.New[{{INPUT}}, {{OUTPUT}}](NewOrdersHandler()),
	})
	if err != nil { t.Fatal(err) }
	result := actual.(*{{OUTPUT}})
{{ASSERTIONS}}
}
`
	return strings.NewReplacer(
		"{{INPUT}}", inputType,
		"{{OUTPUT}}", outputType,
		"{{BODY}}", strconv.Quote(body),
		"{{ASSERTIONS}}", assertions,
	).Replace(source)
}
