package velty

import (
	"strings"
	"testing"

	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
)

func TestRenderRootPatchMany(t *testing.T) {
	value := &plan.Plan{
		Operation: plan.OperationPatch,
		Output:    &plan.ContractRef{Path: plan.FieldPath{"Output", "Data"}},
		Root: &plan.RecordPlan{
			InputPath: plan.FieldPath{"Input", "Events"}, Table: "EVENTS", Cardinality: spec.CardinalityMany,
			Keys:     []plan.KeyPart{{Field: "Id"}},
			Sequence: &plan.SequencePlan{Destination: plan.FieldPath{"Input", "Events"}, Selector: plan.FieldPath{"Id"}},
			Current:  &plan.CurrentPlan{InputPath: plan.FieldPath{"Input", "CurrentEvents"}, Keys: []plan.KeyPart{{Field: "Id"}}},
			Write:    plan.WritePolicy{Existing: plan.ActionUpdate, Missing: plan.ActionInsert},
		},
	}
	actual, err := Render(value)
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}
	want := `$sequencer.Allocate("EVENTS", $Input.Events, "Id")
#set($CurrentEventsById = $Input.CurrentEvents.IndexBy("Id"))
#foreach($RecEvents in $Input.Events)
  #set($datlyWriteIndex0 = $foreach.Index)
  #if($writeHooks.Present("Events", $datlyWriteIndex0))
    $writeHooks.Entity("Events", $datlyWriteIndex0)
    #set($RecEvents = $Input.Events[$datlyWriteIndex0])
    #if($CurrentEventsById.HasKey($RecEvents.Id) == true)
      $dml.Update("EVENTS", $writeHooks.Value("Events", $datlyWriteIndex0));
    #else
      $dml.Insert("EVENTS", $writeHooks.Value("Events", $datlyWriteIndex0));
    #end
  #end
#end
#set($Output.Data = $Input.Events)`
	if actual != want {
		t.Fatalf("Render():\n%s\nwant:\n%s", actual, want)
	}
}

func TestRenderRootPatchManyWithCompoundKey(t *testing.T) {
	value := &plan.Plan{
		Operation: plan.OperationPatch,
		Root: &plan.RecordPlan{
			Identity: "Events", InputPath: plan.FieldPath{"Input", "Events"}, Table: "EVENTS", Cardinality: spec.CardinalityMany,
			Keys: []plan.KeyPart{{Field: "TenantId"}, {Field: "Id"}},
			Current: &plan.CurrentPlan{
				InputPath: plan.FieldPath{"Input", "CurrentEvents"},
				Keys:      []plan.KeyPart{{Field: "TenantId"}, {Field: "EventId"}},
			},
			Write: plan.WritePolicy{Existing: plan.ActionUpdate, Missing: plan.ActionInsert},
		},
	}
	actual, err := Render(value)
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}
	want := `$index.Build("CurrentEventsByTenantIdAndId", $Input.CurrentEvents, $Input, "Events", "TenantId", "TenantId", "EventId", "Id")
#foreach($RecEvents in $Input.Events)
  #set($datlyWriteIndex0 = $foreach.Index)
  #if($writeHooks.Present("Events", $datlyWriteIndex0))
    $writeHooks.Entity("Events", $datlyWriteIndex0)
    #set($RecEvents = $Input.Events[$datlyWriteIndex0])
    #if($index.Has("CurrentEventsByTenantIdAndId", $RecEvents) == true)
      $dml.Update("EVENTS", $writeHooks.Value("Events", $datlyWriteIndex0));
    #else
      $dml.Insert("EVENTS", $writeHooks.Value("Events", $datlyWriteIndex0));
    #end
  #end
#end`
	if actual != want {
		t.Fatalf("Render():\n%s\nwant:\n%s", actual, want)
	}
}

func TestRenderRejectsMalformedOutputRoot(t *testing.T) {
	value := &plan.Plan{
		Operation: plan.OperationPost,
		Output:    &plan.ContractRef{Path: plan.FieldPath{"Input", "Data"}},
		Root: &plan.RecordPlan{
			InputPath: plan.FieldPath{"Input", "Events"}, Table: "EVENTS", Cardinality: spec.CardinalityOne,
			Write: plan.WritePolicy{Missing: plan.ActionInsert},
		},
	}
	_, err := Render(value)
	if err == nil || !strings.Contains(err.Error(), "rooted at Output") {
		t.Fatalf("Render() error = %v", err)
	}
}

func TestRenderRecursivePatchMany(t *testing.T) {
	root := recursivePatchPlan()
	root.Relations[0].Child.PresenceFields = []string{"OrderId"}
	actual, err := Render(&plan.Plan{Operation: plan.OperationPatch, Root: root})
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}
	want := `$sequencer.Allocate("ORDERS", $Input.Orders, "Id")
$sequencer.Allocate("ITEMS", $Input.Orders, "Items/Id")
#set($CurrentOrdersById = $Input.CurrentOrders.IndexBy("Id"))
#set($CurrentItemsById = $Input.CurrentItems.IndexBy("Id"))
#foreach($RecOrders in $Input.Orders)
  #set($datlyWriteIndex0 = $foreach.Index)
  #if($writeHooks.Present("Orders", $datlyWriteIndex0))
    $writeHooks.Entity("Orders", $datlyWriteIndex0)
    #set($RecOrders = $Input.Orders[$datlyWriteIndex0])
    #if($CurrentOrdersById.HasKey($RecOrders.Id) == true)
      $dml.Update("ORDERS", $writeHooks.Value("Orders", $datlyWriteIndex0));
    #else
      $dml.Insert("ORDERS", $writeHooks.Value("Orders", $datlyWriteIndex0));
    #end
    $writeHooks.Relation("Orders", "Items", $datlyWriteIndex0)
    #set($RecOrders = $Input.Orders[$datlyWriteIndex0])
    #foreach($RecItems in $Input.Orders[$datlyWriteIndex0].Items)
      #set($datlyWriteIndex1 = $foreach.Index)
      #if($writeHooks.Present("Orders/Items", $datlyWriteIndex0, $datlyWriteIndex1))
        #set($Input.Orders[$datlyWriteIndex0].Items[$datlyWriteIndex1].OrderId = $Input.Orders[$datlyWriteIndex0].Id)
        $writeHooks.Mark("Orders/Items", "OrderId", $datlyWriteIndex0, $datlyWriteIndex1)
        $writeHooks.Entity("Orders/Items", $datlyWriteIndex0, $datlyWriteIndex1)
        #set($RecItems = $Input.Orders[$datlyWriteIndex0].Items[$datlyWriteIndex1])
        #if($CurrentItemsById.HasKey($RecItems.Id) == true)
          $dml.Update("ITEMS", $writeHooks.Value("Orders/Items", $datlyWriteIndex0, $datlyWriteIndex1));
        #else
          $dml.Insert("ITEMS", $writeHooks.Value("Orders/Items", $datlyWriteIndex0, $datlyWriteIndex1));
        #end
      #end
    #end
  #end
#end`
	if actual != want {
		t.Fatalf("Render():\n%s\nwant:\n%s", actual, want)
	}
}

func TestRenderRecursivePatchUsesNestedCompoundRecordPath(t *testing.T) {
	root := recursivePatchPlan()
	child := root.Relations[0].Child
	child.Keys = append(child.Keys, plan.KeyPart{Field: "TenantId"})
	child.Current.Keys = append(child.Current.Keys, plan.KeyPart{Field: "CurrentTenantId"})
	actual, err := Render(&plan.Plan{Operation: plan.OperationPatch, Root: root})
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}
	for _, expected := range []string{
		`$index.Build("CurrentItemsByIdAndTenantId", $Input.CurrentItems, $Input, "Orders/Items", "Id", "Id", "CurrentTenantId", "TenantId")`,
		`$index.Has("CurrentItemsByIdAndTenantId", $RecItems)`,
	} {
		if !strings.Contains(actual, expected) {
			t.Fatalf("Render() missing %q:\n%s", expected, actual)
		}
	}
}

func TestRenderRecursivePostOneRelation(t *testing.T) {
	root := recursivePatchPlan()
	root.Current = nil
	root.Sequence = nil
	root.Cardinality = spec.CardinalityOne
	root.Write = plan.WritePolicy{Missing: plan.ActionInsert}
	root.Relations[0].Cardinality = spec.CardinalityOne
	root.Relations[0].Child.Cardinality = spec.CardinalityOne
	root.Relations[0].Child.Current = nil
	root.Relations[0].Child.Sequence = nil
	root.Relations[0].Child.Write = plan.WritePolicy{Missing: plan.ActionInsert}
	root.Relations[0].Links = append(root.Relations[0].Links,
		plan.KeyLink{Parent: plan.KeyPart{Field: "TenantId"}, Child: plan.KeyPart{Field: "TenantId"}, Conversion: plan.LinkDirect})
	actual, err := Render(&plan.Plan{Operation: plan.OperationPost, Root: root})
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}
	want := `#if($writeHooks.Present("Orders"))
  $writeHooks.Entity("Orders")
  #set($RecOrders = $Input.Orders)
  $dml.Insert("ORDERS", $writeHooks.Value("Orders"));
  $writeHooks.Relation("Orders", "Items")
  #set($RecOrders = $Input.Orders)
  #if($writeHooks.Present("Orders/Items"))
    #set($Input.Orders.Items.OrderId = $Input.Orders.Id)
    #set($Input.Orders.Items.TenantId = $Input.Orders.TenantId)
    $writeHooks.Entity("Orders/Items")
    #set($RecItems = $Input.Orders.Items)
    $dml.Insert("ITEMS", $writeHooks.Value("Orders/Items"));
  #end
#end`
	if actual != want {
		t.Fatalf("Render():\n%s\nwant:\n%s", actual, want)
	}
}

func recursivePatchPlan() *plan.RecordPlan {
	child := &plan.RecordPlan{
		Identity: "Items", InputPath: plan.FieldPath{"Input", "Orders", "Items"}, Table: "ITEMS", Cardinality: spec.CardinalityMany,
		Keys:     []plan.KeyPart{{Field: "Id"}},
		Sequence: &plan.SequencePlan{Destination: plan.FieldPath{"Input", "Orders"}, Selector: plan.FieldPath{"Items", "Id"}},
		Current:  &plan.CurrentPlan{InputPath: plan.FieldPath{"Input", "CurrentItems"}, Keys: []plan.KeyPart{{Field: "Id"}}},
		Write:    plan.WritePolicy{Existing: plan.ActionUpdate, Missing: plan.ActionInsert},
	}
	return &plan.RecordPlan{
		Identity: "Orders", InputPath: plan.FieldPath{"Input", "Orders"}, Table: "ORDERS", Cardinality: spec.CardinalityMany,
		Keys:     []plan.KeyPart{{Field: "Id"}},
		Sequence: &plan.SequencePlan{Destination: plan.FieldPath{"Input", "Orders"}, Selector: plan.FieldPath{"Id"}},
		Current:  &plan.CurrentPlan{InputPath: plan.FieldPath{"Input", "CurrentOrders"}, Keys: []plan.KeyPart{{Field: "Id"}}},
		Write:    plan.WritePolicy{Existing: plan.ActionUpdate, Missing: plan.ActionInsert},
		Relations: []*plan.RelationPlan{{
			Identity: "Items", FieldPath: plan.FieldPath{"Items"}, Cardinality: spec.CardinalityMany,
			Links: []plan.KeyLink{{Parent: plan.KeyPart{Field: "Id"}, Child: plan.KeyPart{Field: "OrderId"}, Conversion: plan.LinkDirect}}, Child: child,
		}},
	}
}
