package golang

import (
	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
)

const (
	eventsViewIdentity         = "view::Events|namespace:"
	currentEventsViewIdentity  = "view::CurrentEvents|namespace:"
	ordersViewIdentity         = "view::Orders|namespace:"
	itemsViewIdentity          = "view::Items|namespace:"
	detailsViewIdentity        = "view::Details|namespace:"
	currentOrdersViewIdentity  = "view::CurrentOrders|namespace:"
	currentItemsViewIdentity   = "view::CurrentItems|namespace:"
	currentDetailsViewIdentity = "view::CurrentDetails|namespace:"
)

func rootSemanticPlan(operation plan.Operation, compound bool) *plan.Plan {
	keys := []plan.KeyPart{{Field: "Id", Source: "ID", Type: spec.TypeRef{Name: "int64", Pointer: true}}}
	if compound {
		keys = append(keys, plan.KeyPart{Field: "TenantId", Source: "TENANT_ID", Type: spec.TypeRef{Name: "int64"}})
	}
	root := &plan.RecordPlan{
		Identity: eventsViewIdentity, InputPath: plan.FieldPath{"Input", "Events"},
		Table: "EVENTS", Cardinality: spec.CardinalityMany, Keys: cloneFixtureKeys(keys),
		Write: fixtureWritePolicy(operation, plan.FieldPath{"Input", "Events"}, 0),
	}
	if operation == plan.OperationPatch {
		root.Current = &plan.CurrentPlan{
			ParamIdentity: "currentevents|view|currentevents", ViewIdentity: currentEventsViewIdentity,
			InputPath: plan.FieldPath{"Input", "CurrentEvents"}, Keys: cloneFixtureKeys(keys),
		}
	}
	if operation != plan.OperationPut && !compound {
		root.Sequence = &plan.SequencePlan{
			Destination: plan.FieldPath{"Input", "Events"}, Selector: plan.FieldPath{"Id"},
		}
	}
	output := plan.ContractRef{
		ParamIdentity: "data|output|body|output", Path: plan.FieldPath{"Output", "Data"},
		Cardinality: spec.CardinalityMany,
	}
	return &plan.Plan{
		Operation: operation,
		Input: plan.ContractRef{
			ParamIdentity: "events|body|data", Path: plan.FieldPath{"Input", "Events"},
			Cardinality: spec.CardinalityMany,
		},
		Output: &output,
		Root:   root,
	}
}

func recursiveSemanticPlan(operation plan.Operation) *plan.Plan {
	id := plan.KeyPart{Field: "Id", Source: "ID", Type: spec.TypeRef{Name: "int64", Pointer: true}}
	orderID := plan.KeyPart{Field: "OrderId", Source: "ORDER_ID", Type: spec.TypeRef{Name: "int64"}}
	itemID := plan.KeyPart{Field: "ItemId", Source: "ITEM_ID", Type: spec.TypeRef{Name: "int64"}}

	detail := &plan.RecordPlan{
		Identity: detailsViewIdentity, InputPath: plan.FieldPath{"Input", "Orders", "Items", "Details"},
		Table: "DETAILS", Cardinality: spec.CardinalityMany, Keys: []plan.KeyPart{id},
		Write: fixtureWritePolicy(operation, plan.FieldPath{"Input", "Orders", "Items", "Details"}, 2),
	}
	item := &plan.RecordPlan{
		Identity: itemsViewIdentity, InputPath: plan.FieldPath{"Input", "Orders", "Items"},
		Table: "ITEMS", Cardinality: spec.CardinalityMany, Keys: []plan.KeyPart{id},
		Write: fixtureWritePolicy(operation, plan.FieldPath{"Input", "Orders", "Items"}, 1),
		Relations: []*plan.RelationPlan{{
			Identity: "Details", FieldPath: plan.FieldPath{"Details"}, Cardinality: spec.CardinalityMany,
			Links: []plan.KeyLink{{
				Parent: id, Child: itemID, Conversion: plan.LinkDereference,
			}},
			Child: detail,
		}},
	}
	root := &plan.RecordPlan{
		Identity: ordersViewIdentity, InputPath: plan.FieldPath{"Input", "Orders"},
		Table: "ORDERS", Cardinality: spec.CardinalityMany, Keys: []plan.KeyPart{id},
		Write: fixtureWritePolicy(operation, plan.FieldPath{"Input", "Orders"}, 0),
		Relations: []*plan.RelationPlan{{
			Identity: "Items", FieldPath: plan.FieldPath{"Items"}, Cardinality: spec.CardinalityMany,
			Links: []plan.KeyLink{{
				Parent: id, Child: orderID, Conversion: plan.LinkDereference,
			}},
			Child: item,
		}},
	}

	if operation == plan.OperationPatch {
		root.Current = fixtureCurrent("CurrentOrders", currentOrdersViewIdentity, id)
		item.Current = fixtureCurrent("CurrentItems", currentItemsViewIdentity, id)
		detail.Current = fixtureCurrent("CurrentDetails", currentDetailsViewIdentity, id)
	}
	if operation != plan.OperationPut {
		root.Sequence = &plan.SequencePlan{Destination: plan.FieldPath{"Input", "Orders"}, Selector: plan.FieldPath{"Id"}}
		item.Sequence = &plan.SequencePlan{Destination: plan.FieldPath{"Input", "Orders"}, Selector: plan.FieldPath{"Items", "Id"}}
		detail.Sequence = &plan.SequencePlan{Destination: plan.FieldPath{"Input", "Orders"}, Selector: plan.FieldPath{"Items", "Details", "Id"}}
	}
	output := plan.ContractRef{
		ParamIdentity: "data|output|body|output", Path: plan.FieldPath{"Output", "Data"},
		Cardinality: spec.CardinalityMany,
	}
	return &plan.Plan{
		Operation: operation,
		Input: plan.ContractRef{
			ParamIdentity: "orders|body|data", Path: plan.FieldPath{"Input", "Orders"},
			Cardinality: spec.CardinalityMany,
		},
		Output: &output,
		Root:   root,
	}
}

func fixtureCurrent(name, viewIdentity string, key plan.KeyPart) *plan.CurrentPlan {
	return &plan.CurrentPlan{
		ParamIdentity: nameToCurrentParamIdentity(name), ViewIdentity: viewIdentity,
		InputPath: plan.FieldPath{"Input", name}, Keys: []plan.KeyPart{key},
	}
}

func nameToCurrentParamIdentity(name string) string {
	switch name {
	case "CurrentOrders":
		return "currentorders|view|currentorders"
	case "CurrentItems":
		return "currentitems|view|currentitems"
	case "CurrentDetails":
		return "currentdetails|view|currentdetails"
	default:
		panic("unsupported current fixture " + name)
	}
}

func fixtureWritePolicy(operation plan.Operation, path plan.FieldPath, order int) plan.WritePolicy {
	result := plan.WritePolicy{ValuePath: append(plan.FieldPath(nil), path...), Order: order}
	switch operation {
	case plan.OperationPost:
		result.Missing = plan.ActionInsert
		result.Allowed = []plan.Action{plan.ActionInsert}
	case plan.OperationPut:
		result.Existing = plan.ActionUpdate
		result.Allowed = []plan.Action{plan.ActionUpdate}
	case plan.OperationPatch:
		result.Existing = plan.ActionUpdate
		result.Missing = plan.ActionInsert
		result.Allowed = []plan.Action{plan.ActionInsert, plan.ActionUpdate}
	}
	return result
}

func cloneFixtureKeys(keys []plan.KeyPart) []plan.KeyPart {
	return append([]plan.KeyPart(nil), keys...)
}
