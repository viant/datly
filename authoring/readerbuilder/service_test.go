package readerbuilder

import (
	"context"
	"strings"
	"testing"
)

const baseDQL = `#setting($_ = $route('/records','GET'))
#define($_ = $IDs<[]int>(query/ids).Optional())
SELECT records.*
FROM (SELECT r.id,r.name FROM records r) records`

func TestServiceInspectReturnsCanonicalAndAuthoringMetadata(t *testing.T) {
	response := New(Config{Name: "Records", Scope: "example.com/records", AvailableConnectors: []string{"main"}}).Apply(context.Background(), Request{
		DQL: baseDQL, Operation: Operation{Type: OperationInspect},
	})
	if !response.Applied || response.DQL != baseDQL || len(response.Diagnostics) != 0 {
		t.Fatalf("response=%+v expansions=%+v", response, response.Structure.PredicateExpansions)
	}
	if response.Structure == nil || response.Structure.Component == nil || len(response.Structure.Views) != 1 || response.Structure.Views[0].Name != "records" {
		t.Fatalf("structure=%+v", response.Structure)
	}
}

func TestServiceAddPredicateAlsoAddsMissingViewExpansion(t *testing.T) {
	response := New(Config{Name: "Records", Scope: "example.com/records"}).Apply(context.Background(), Request{
		DQL: baseDQL,
		Operation: Operation{Type: OperationAddFieldPredicate, Predicate: &PredicateMutation{
			Field: "IDs", View: "records", Group: 2, Name: "in", Args: []string{"r", "id"},
		}},
	})
	if !response.Applied || len(response.Diagnostics) != 0 {
		t.Fatalf("response=%+v expansions=%+v", response, response.Structure.PredicateExpansions)
	}
	for _, expected := range []string{
		`.WithPredicate(2, "in", "r", "id")`,
		`$predicate.FilterGroup(2, "AND")`,
		`.Build("WHERE")`,
	} {
		if !strings.Contains(response.DQL, expected) {
			t.Fatalf("missing %q in:\n%s", expected, response.DQL)
		}
	}
	if len(response.Structure.PredicateExpansions) != 1 || response.Structure.PredicateExpansions[0].View != "records" {
		t.Fatalf("expansions=%+v", response.Structure.PredicateExpansions)
	}
}

func TestServiceRejectsImplicitlySharingPredicateGroup(t *testing.T) {
	source := `#setting($_ = $route('/records','GET'))
#define($_ = $IDs<[]int>(query/ids).Optional())
SELECT records.*,items.*
FROM (SELECT r.id FROM records r ${predicate.Builder().CombineAnd($predicate.FilterGroup(0, "AND")).Build("WHERE")}) records
JOIN (SELECT i.id,i.record_id FROM items i) items ON items.record_id=records.id`
	response := New(Config{Name: "Records"}).Apply(context.Background(), Request{
		DQL: source,
		Operation: Operation{Type: OperationAddFieldPredicate, Predicate: &PredicateMutation{
			Field: "IDs", View: "items", Group: 0, Name: "in", Args: []string{"i", "id"},
		}},
	})
	if response.Applied || response.DQL != source || len(response.Diagnostics) == 0 || !strings.Contains(response.Diagnostics[len(response.Diagnostics)-1].Message, "also expanded") {
		t.Fatalf("response=%+v", response)
	}
}

func TestServiceExplicitSharedScopeStillAddsMissingTargetExpansion(t *testing.T) {
	source := `#setting($_ = $route('/records','GET'))
#define($_ = $IDs<[]int>(query/ids).Optional())
SELECT records.*,items.*
FROM (SELECT r.id FROM records r ${predicate.Builder().CombineAnd($predicate.FilterGroup(0, "AND")).Build("WHERE")}) records
JOIN (SELECT i.id,i.record_id FROM items i) items ON items.record_id=records.id`
	response := New(Config{Name: "Records"}).Apply(context.Background(), Request{
		DQL: source,
		Operation: Operation{Type: OperationAddFieldPredicate, Predicate: &PredicateMutation{
			Field: "IDs", View: "items", ExpansionViews: []string{"records", "items"}, Group: 0, Name: "in", Args: []string{"i", "id"},
		}},
	})
	if !response.Applied || strings.Count(response.DQL, "$predicate.FilterGroup(0") != 2 {
		t.Fatalf("response=%+v", response)
	}
}

func TestServiceAddsMissingGroupToExistingCompositionExplicitly(t *testing.T) {
	source := strings.Replace(baseDQL, `FROM (SELECT r.id,r.name FROM records r) records`, `FROM (SELECT r.id,r.name FROM records r ${predicate.Builder().CombineAnd($predicate.FilterGroup(0, "AND")).Build("WHERE")}) records`, 1)
	response := New(Config{Name: "Records"}).Apply(context.Background(), Request{DQL: source, Operation: Operation{Type: OperationAddFieldPredicate, Predicate: &PredicateMutation{
		Field: "IDs", View: "records", Group: 1, Name: "in", Args: []string{"r", "id"}, GroupOperator: "OR", CombineOperator: "AND",
	}}})
	if !response.Applied || !strings.Contains(response.DQL, `.And().CombineAnd($predicate.FilterGroup(1, "OR")).Build("WHERE")`) {
		t.Fatalf("response=%+v", response)
	}
	ambiguous := New(Config{Name: "Records"}).Apply(context.Background(), Request{DQL: source, Operation: Operation{Type: OperationAddFieldPredicate, Predicate: &PredicateMutation{
		Field: "IDs", View: "records", Group: 1, Name: "in", Args: []string{"r", "id"},
	}}})
	if ambiguous.Applied {
		t.Fatalf("ambiguous=%+v", ambiguous)
	}
}

func TestServiceDoesNotAssociateRawExpansionWithLaterBuilder(t *testing.T) {
	source := strings.Replace(baseDQL, `FROM (SELECT r.id,r.name FROM records r) records`, `FROM (SELECT r.id,r.name FROM records r ${predicate.Expand(0)} ${predicate.Builder().CombineAnd($predicate.FilterGroup(1, "AND")).Build("AND")}) records`, 1)
	response := New(Config{Name: "Records"}).Apply(context.Background(), Request{DQL: source, Operation: Operation{Type: OperationAddFieldPredicate, Predicate: &PredicateMutation{
		Field: "IDs", View: "records", Group: 2, Name: "in", Args: []string{"r", "id"}, CombineOperator: "AND",
	}}})
	if response.Applied || response.DQL != source {
		t.Fatalf("response=%+v", response)
	}
}

func TestInspectionRecognizesExpandWithOnce(t *testing.T) {
	source := strings.Replace(baseDQL, `FROM (SELECT r.id,r.name FROM records r) records`, `FROM (SELECT r.id,r.name FROM records r ${predicate.ExpandWith(3, "OR")}) records`, 1)
	response := New(Config{Name: "Records"}).Apply(context.Background(), Request{DQL: source, Operation: Operation{Type: OperationInspect}})
	if !response.Applied || len(response.Structure.PredicateExpansions) != 1 || response.Structure.PredicateExpansions[0].Method != "ExpandWith" || response.Structure.PredicateExpansions[0].Group != 3 {
		t.Fatalf("response=%+v expansions=%+v", response, response.Structure.PredicateExpansions)
	}
}

func TestServiceUpdateAndRemovePredicateUseDeclarationOccurrence(t *testing.T) {
	source := strings.Replace(baseDQL, `.Optional()`, `.Optional().WithPredicate(0,'in','r','id')`, 1)
	source = strings.Replace(source, `FROM (SELECT r.id,r.name FROM records r) records`, `FROM (SELECT r.id,r.name FROM records r ${predicate.Builder().CombineAnd($predicate.FilterGroup(0, "AND")).Build("WHERE")}) records`, 1)
	service := New(Config{Name: "Records"})
	updated := service.Apply(context.Background(), Request{DQL: source, Operation: Operation{Type: OperationUpdateFieldPredicate, Predicate: &PredicateMutation{
		Field: "IDs", Occurrence: 0, View: "records", Group: 0, Name: "not_in", Args: []string{"r", "id"},
	}}})
	if !updated.Applied || !strings.Contains(updated.DQL, `WithPredicate(0, "not_in", "r", "id")`) {
		t.Fatalf("updated=%+v", updated)
	}
	removed := service.Apply(context.Background(), Request{DQL: updated.DQL, Operation: Operation{Type: OperationRemoveFieldPredicate, Predicate: &PredicateMutation{Field: "IDs", Occurrence: 0}}})
	if !removed.Applied || strings.Contains(removed.DQL, "WithPredicate") {
		t.Fatalf("removed=%+v", removed)
	}
}

func TestServiceGenericFunctionCRUD(t *testing.T) {
	service := New(Config{Name: "Records"})
	added := service.Apply(context.Background(), Request{DQL: baseDQL, Operation: Operation{
		Type: OperationAddFunction, Function: &FunctionMutation{Name: "set_limit", Args: []string{"records", "25"}},
	}})
	if !added.Applied || !strings.Contains(added.DQL, `set_limit(records, 25)`) || len(added.Structure.Functions) != 1 {
		t.Fatalf("added=%+v", added)
	}
	updated := service.Apply(context.Background(), Request{DQL: added.DQL, Operation: Operation{
		Type: OperationUpdateFunction, Function: &FunctionMutation{Name: "set_limit", ExpectedArgs: []string{"records", "25"}, Args: []string{"records", "50"}},
	}})
	if !updated.Applied || !strings.Contains(updated.DQL, `set_limit(records, 50)`) {
		t.Fatalf("updated=%+v", updated)
	}
	removed := service.Apply(context.Background(), Request{DQL: updated.DQL, Operation: Operation{
		Type: OperationRemoveFunction, Function: &FunctionMutation{Name: "set_limit", ExpectedArgs: []string{"records", "50"}},
	}})
	if !removed.Applied || strings.Contains(removed.DQL, "set_limit") {
		t.Fatalf("removed=%+v", removed)
	}
}

func TestServiceGenericCastSupportsPseudoColumn(t *testing.T) {
	source := strings.Replace(baseDQL, `r.id,r.name`, `r.id,r.name,'' AS payload`, 1)
	response := New(Config{Name: "Records"}).Apply(context.Background(), Request{DQL: source, Operation: Operation{
		Type: OperationAddFunction, Function: &FunctionMutation{Name: "cast", Args: []string{"records.payload", "Payload"}},
	}})
	if !response.Applied || !strings.Contains(response.DQL, `cast(records.payload AS Payload)`) {
		t.Fatalf("response=%+v", response)
	}
}

func TestServiceSettingsAndPredefinedConnector(t *testing.T) {
	service := New(Config{Name: "Records", AvailableConnectors: []string{"main"}})
	set := service.Apply(context.Background(), Request{DQL: baseDQL, Operation: Operation{
		Type: OperationSetSetting, Setting: &SettingMutation{Name: "connector", Args: []string{"'main'"}},
	}})
	if !set.Applied || !strings.Contains(set.DQL, `$connector('main')`) || set.Structure.Component.Settings.DefaultConnector != "main" {
		t.Fatalf("set=%+v", set)
	}
	unknown := service.Apply(context.Background(), Request{DQL: set.DQL, Operation: Operation{
		Type: OperationSetSetting, Setting: &SettingMutation{Name: "connector", Args: []string{"'missing'"}},
	}})
	if unknown.Applied || unknown.DQL != set.DQL {
		t.Fatalf("unknown=%+v", unknown)
	}
	removed := service.Apply(context.Background(), Request{DQL: set.DQL, Operation: Operation{
		Type: OperationSetSetting, Setting: &SettingMutation{Name: "connector", Remove: true},
	}})
	if !removed.Applied || strings.Contains(removed.DQL, "$connector") {
		t.Fatalf("removed=%+v", removed)
	}
}

func TestServiceValidatesNamedViewCache(t *testing.T) {
	service := New(Config{Name: "Records", AvailableCaches: []string{"shared"}})
	accepted := service.Apply(context.Background(), Request{DQL: baseDQL, Operation: Operation{
		Type: OperationAddFunction, Function: &FunctionMutation{Name: "use_cache", Args: []string{"records", "'shared'"}},
	}})
	if !accepted.Applied || accepted.Structure.Component.RootView.Source.Bindings.CacheName != "shared" {
		t.Fatalf("accepted=%+v", accepted)
	}
	rejected := service.Apply(context.Background(), Request{DQL: baseDQL, Operation: Operation{
		Type: OperationAddFunction, Function: &FunctionMutation{Name: "use_cache", Args: []string{"records", "'missing'"}},
	}})
	if rejected.Applied {
		t.Fatalf("rejected=%+v", rejected)
	}
}

func TestServiceAuthorsCacheWarmupAndReaderMCP(t *testing.T) {
	service := New(Config{Name: "Records"})
	location := strings.ReplaceAll(t.TempDir(), "'", "''")
	response := service.Apply(context.Background(), Request{DQL: baseDQL, Operation: Operation{
		Type: OperationSetSetting, Setting: &SettingMutation{Name: "cache", Args: []string{"'records'", "'1m'"}, Options: []FunctionMutation{{Name: "WithLocation", Args: []string{"'" + location + "'"}}}},
	}})
	if !response.Applied {
		t.Fatalf("cache=%+v", response)
	}
	response = service.Apply(context.Background(), Request{DQL: response.DQL, Operation: Operation{
		Type: OperationSetSetting, Setting: &SettingMutation{Name: "cache_warmup", Args: []string{"'id'", "'ids=1,2'"}},
	}})
	if !response.Applied || response.Structure.Component.Settings.Cache.Warmup == nil || response.Structure.Component.Settings.Cache.Warmup.IndexColumn != "id" {
		t.Fatalf("warmup=%+v", response)
	}
	response = service.Apply(context.Background(), Request{DQL: response.DQL, Operation: Operation{
		Type: OperationSetSetting, Setting: &SettingMutation{Name: "mcp", Args: []string{"'records.list'", "'List records'"}},
	}})
	if !response.Applied || len(response.Structure.Component.Routes[0].MCP) != 1 || response.Structure.Component.Routes[0].MCP[0].Name != "records.list" {
		t.Fatalf("mcp=%+v", response)
	}
}

func TestServiceRejectsIncompleteInlineCache(t *testing.T) {
	response := New(Config{Name: "Records"}).Apply(context.Background(), Request{DQL: baseDQL, Operation: Operation{
		Type: OperationSetSetting, Setting: &SettingMutation{Name: "cache", Args: []string{"'records'", "'1m'"}},
	}})
	if response.Applied || response.DQL != baseDQL || len(response.Diagnostics) == 0 {
		t.Fatalf("response=%+v", response)
	}
}

func TestServiceAddsQuerySelectorField(t *testing.T) {
	optional := false
	response := New(Config{Name: "Records"}).Apply(context.Background(), Request{DQL: baseDQL, Operation: Operation{
		Type: OperationAddField, Field: &Field{Name: "Limit", Type: "int", SourceKind: "query", SourceName: "limit", Required: &optional, QuerySelector: "Records"},
	}})
	if !response.Applied || !strings.Contains(response.DQL, `.QuerySelector("Records")`) {
		t.Fatalf("response=%+v", response)
	}
}

func TestServiceUpdatesAndRemovesInputWithoutDroppingPredicateOptions(t *testing.T) {
	source := `#setting($_ = $route('/records','GET'))
#define($_ = $Limit<int>(query/limit).Optional().QuerySelector("Records").WithPredicate(0,"less_or_equal","r","id"))
SELECT records.* FROM (SELECT r.id FROM records r ${predicate.Builder().CombineAnd($predicate.FilterGroup(0, "AND")).Build("WHERE")}) records`
	required := true
	selector := ""
	service := New(Config{Name: "Records"})
	updated := service.Apply(context.Background(), Request{DQL: source, Operation: Operation{Type: OperationUpdateField, Field: &Field{
		ExistingName: "Limit", Name: "Limit", Type: "int64", SourceKind: "query", SourceName: "max", Required: &required, UpdateQuerySelector: &selector,
	}}})
	if !updated.Applied || !strings.Contains(updated.DQL, `$Limit<int64>(query/max).Required().WithPredicate`) || strings.Contains(updated.DQL, "QuerySelector") {
		t.Fatalf("updated=%+v", updated)
	}
	removed := service.Apply(context.Background(), Request{DQL: updated.DQL, Operation: Operation{Type: OperationRemoveField, Field: &Field{ExistingName: "Limit"}}})
	if !removed.Applied || strings.Contains(removed.DQL, "$Limit") {
		t.Fatalf("removed=%+v", removed)
	}
}

func TestServiceUpdateFieldRejectsRename(t *testing.T) {
	response := New(Config{Name: "Records"}).Apply(context.Background(), Request{DQL: baseDQL, Operation: Operation{Type: OperationUpdateField, Field: &Field{ExistingName: "IDs", Name: "Other"}}})
	if response.Applied || response.DQL != baseDQL || len(response.Diagnostics) == 0 || !strings.Contains(response.Diagnostics[len(response.Diagnostics)-1].Message, "reference-aware") {
		t.Fatalf("response=%+v", response)
	}
}

func TestServiceEnablesSelectorPolicyWithGenericFunction(t *testing.T) {
	response := New(Config{Name: "Records"}).Apply(context.Background(), Request{DQL: baseDQL, Operation: Operation{
		Type: OperationAddFunction, Function: &FunctionMutation{Name: "selector_page", Args: []string{"records", "true"}},
	}})
	if !response.Applied || response.Structure.Component.RootView.Selector == nil || !response.Structure.Component.RootView.Selector.AllowPage {
		t.Fatalf("response=%+v", response)
	}
}

func TestInspectionIgnoresPredicateTextInQuotedAndCommentedRegions(t *testing.T) {
	source := `#setting($_ = $route('/records','GET'))
SELECT records.* FROM (SELECT '${predicate.FilterGroup(0, "AND")}' AS literal
FROM records r -- $predicate.Expand(1)
) records`
	response := New(Config{Name: "Records"}).Apply(context.Background(), Request{DQL: source, Operation: Operation{Type: OperationInspect}})
	if !response.Applied || len(response.Structure.PredicateExpansions) != 0 {
		t.Fatalf("response=%+v", response)
	}
}

func TestInspectionDoesNotExposeInnerDatabaseAliasAsView(t *testing.T) {
	source := `#setting($_ = $route('/records','GET'))
SELECT records.* FROM (SELECT nested.id FROM (SELECT id FROM records) nested) records`
	response := New(Config{Name: "Records"}).Apply(context.Background(), Request{DQL: source, Operation: Operation{Type: OperationInspect}})
	if !response.Applied || len(response.Structure.Views) != 1 || response.Structure.Views[0].Name != "records" {
		t.Fatalf("views=%+v diagnostics=%+v", response.Structure.Views, response.Diagnostics)
	}
}

func TestInspectionDoesNotTreatProjectionSubqueryAsView(t *testing.T) {
	source := `#setting($_ = $route('/records','GET'))
SELECT (SELECT MAX(id) FROM audit) latest,records.*
FROM (SELECT id FROM records) records`
	response := New(Config{Name: "Records"}).Apply(context.Background(), Request{DQL: source, Operation: Operation{Type: OperationInspect}})
	if !response.Applied || len(response.Structure.Views) != 1 || response.Structure.Views[0].Name != "records" {
		t.Fatalf("views=%+v diagnostics=%+v", response.Structure.Views, response.Diagnostics)
	}
}

func TestInspectInvalidDQLIsSuccessfulPartialInspection(t *testing.T) {
	response := New(Config{Name: "Records"}).Apply(context.Background(), Request{DQL: "SELECT (", Operation: Operation{Type: OperationInspect}})
	if !response.Applied || response.DQL != "SELECT (" || response.Structure == nil || response.Structure.Status != "partial" || len(response.Diagnostics) == 0 {
		t.Fatalf("response=%+v", response)
	}
}
