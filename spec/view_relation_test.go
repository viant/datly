package spec

import "testing"

func TestViewClonePreservesRelationGraphIdentity(t *testing.T) {
	child := &View{Name: "Items", Namespace: "i", Source: &ViewSource{Table: "items"}}
	root := &View{Name: "Orders", Namespace: "o", Relations: []*Relation{
		{Name: "Items", Kind: RelationKindSubview, Cardinality: CardinalityMany, MatchStrategy: MatchReadAll, View: child,
			On: []*RelationLink{{ParentNamespace: "o", ParentColumn: "id", ChildNamespace: "i", ChildColumn: "order_id"}}},
		{Name: "ItemsAgain", Kind: RelationKindSubview, Cardinality: CardinalityMany, View: child},
	}}
	actual := root.Clone()
	if actual == root || len(actual.Relations) != 2 || actual.Relations[0].View == child {
		t.Fatalf("clone = %#v", actual)
	}
	if actual.Relations[0].View != actual.Relations[1].View {
		t.Fatal("shared child identity was not preserved")
	}
	if actual.Relations[0].MatchStrategy != MatchReadAll {
		t.Fatalf("match strategy = %q", actual.Relations[0].MatchStrategy)
	}
	actual.Relations[0].On[0].ParentColumn = "changed"
	if root.Relations[0].On[0].ParentColumn != "id" {
		t.Fatal("relation link was not deeply cloned")
	}
}

func TestViewCloneIsolatesMutableMetadata(t *testing.T) {
	allowNulls := true
	defaultValue := "'active'"
	source := &View{
		Cardinality: CardinalityOne,
		AllowNulls:  &allowNulls,
		Groupable:   &allowNulls,
		Selector: &Selector{
			Filterable:   []FieldPath{"ID"},
			OrderAliases: map[string]FieldPath{"created": "CreatedAt"},
		},
		Partitioning:  &Partitioning{Arguments: []string{"tenant_id"}},
		SelfReference: &SelfReference{Holder: "Children", Child: "ID", Parent: "ParentID"},
		Columns:       []*Column{{Name: "ID", Default: &defaultValue, Codec: &Codec{Body: "AsString", Args: []string{"trim"}}}},
	}
	cloned := source.Clone()
	cloned.Selector.Filterable[0] = "Name"
	cloned.Selector.OrderAliases["created"] = "UpdatedAt"
	cloned.Partitioning.Arguments[0] = "account_id"
	cloned.SelfReference.Holder = "Nodes"
	cloned.Columns[0].Name = "Changed"
	*cloned.Columns[0].Default = "'changed'"
	cloned.Columns[0].Codec.Args[0] = "changed"
	*cloned.AllowNulls = false
	if source.Selector.Filterable[0] != "ID" || source.Selector.OrderAliases["created"] != "CreatedAt" ||
		source.Partitioning.Arguments[0] != "tenant_id" || source.SelfReference.Holder != "Children" ||
		!*source.AllowNulls || cloned.Cardinality != CardinalityOne || cloned.Groupable == nil || !*cloned.Groupable ||
		source.Columns[0].Name != "ID" || source.Columns[0].Default == nil || *source.Columns[0].Default != "'active'" ||
		source.Columns[0].Codec.Args[0] != "trim" {
		t.Fatalf("clone mutated source metadata: %+v", source)
	}
}

func TestViewClonePreservesExecutionControls(t *testing.T) {
	source := &View{BatchSize: 25, BatchConcurrency: 3, PublishParent: true, RelationalConcurrency: 4}
	actual := source.Clone()
	if actual.BatchSize != 25 || actual.BatchConcurrency != 3 || !actual.PublishParent || actual.RelationalConcurrency != 4 {
		t.Fatalf("execution controls = %+v", actual)
	}
}
