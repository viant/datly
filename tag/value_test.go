package tag

import (
	"reflect"
	"testing"

	"github.com/viant/datly/spec"
)

func TestParseCodec(t *testing.T) {
	actual, err := ParseCodec(`AsStrings,separator="|",trim`)
	if err != nil {
		t.Fatalf("ParseCodec() error = %v", err)
	}
	if actual == nil || actual.Name != "AsStrings" || actual.Body != "AsStrings" || len(actual.Arguments) != 2 || actual.Arguments[0] != "separator=|" || actual.Arguments[1] != "trim" {
		t.Fatalf("unexpected codec: %+v", actual)
	}
}

func TestCodecValueRoundTripsCanonicalMetadata(t *testing.T) {
	source := Codec{
		Body: "structql", OutputType: "[]int64",
		Arguments: []string{
			"SELECT ARRAY_AGG(Id) AS Values FROM `/` LIMIT 1", "separator=,",
			"outputType=raw", "body=raw", "name=raw",
		},
	}
	value, err := source.Value()
	if err != nil {
		t.Fatalf("Value() error = %v", err)
	}
	actual, err := ParseCodec(value)
	if err != nil {
		t.Fatalf("ParseCodec() error = %v\n%s", err, value)
	}
	if actual.Body != source.Body || actual.OutputType != source.OutputType || len(actual.Arguments) != len(source.Arguments) {
		t.Fatalf("round trip = %+v\n%s", actual, value)
	}
	for index := range source.Arguments {
		if actual.Arguments[index] != source.Arguments[index] {
			t.Fatalf("argument %d = %q, want %q\n%s", index, actual.Arguments[index], source.Arguments[index], value)
		}
	}
}

func TestQuerySelectorValueRoundTrips(t *testing.T) {
	value, err := (QuerySelector{View: "users=current,next"}).Value()
	if err != nil {
		t.Fatalf("Value() error = %v", err)
	}
	actual, err := ParseQuerySelector(value)
	if err != nil || actual.View != "users=current,next" {
		t.Fatalf("ParseQuerySelector() = %+v, %v\n%s", actual, err, value)
	}
}

func TestQuerySelectorValueRoundTripsExplicitProperty(t *testing.T) {
	value, err := (QuerySelector{View: "viewer", Property: spec.SelectorPropertyFields}).Value()
	if err != nil {
		t.Fatal(err)
	}
	actual, err := ParseQuerySelector(value)
	if err != nil || actual.View != "viewer" || actual.Property != spec.SelectorPropertyFields {
		t.Fatalf("ParseQuerySelector() = %+v, %v\n%s", actual, err, value)
	}
	for _, invalid := range []string{"view=viewer,property=unknown", "view=viewer,property=fields,property=fields", "property=fields"} {
		if _, err := ParseQuerySelector(invalid); err == nil {
			t.Fatalf("expected invalid selector metadata %q to fail", invalid)
		}
	}
}

func TestParseFieldSelectorAlias(t *testing.T) {
	type row struct {
		AdvertiserID int `sqlx:"advertiser_id" selectorAlias:"advertiserId"`
	}
	field, ok := reflect.TypeFor[row]().FieldByName("AdvertiserID")
	if !ok {
		t.Fatal("missing field")
	}
	actual, err := ParseField(field)
	if err != nil {
		t.Fatalf("ParseField() error = %v", err)
	}
	if actual.SelectorAlias != "advertiserId" {
		t.Fatalf("SelectorAlias = %q", actual.SelectorAlias)
	}
}

func TestParseSelf(t *testing.T) {
	actual, err := ParseSelf("child=ID,parent=ParentID")
	if err != nil || actual == nil || actual.Child != "ID" || actual.Parent != "ParentID" {
		t.Fatalf("ParseSelf() = %+v, %v", actual, err)
	}
	for _, value := range []string{"child=ID", "child=ID,parent=ParentID,guess=yes"} {
		if _, err := ParseSelf(value); err == nil {
			t.Fatalf("expected %q to fail", value)
		}
	}
}

func TestParseView(t *testing.T) {
	actual, err := ParseView(`accounts,connector=analytics,cache=account-cache,limit=50,batch=25,match=read_all,publishParent=true,partitioner=example.Partitioner,concurrency=3,relationalConcurrency=4,allowNulls=true,groupable=true,selectorNamespace=a,selectorProjection=true,selectorOrderBy=true,selectorCriteria=true,selectorLimit=true,selectorOffset=true,selectorPage=true,selectorFilterable={ID,Name},selectorOrderByColumns={created:CreatedAt,name:Name}`)
	if err != nil {
		t.Fatalf("ParseView() error = %v", err)
	}
	if actual == nil || actual.Name != "accounts" || actual.Connector != "analytics" || actual.Cache != "account-cache" || actual.Limit == nil || *actual.Limit != 50 || actual.Batch != 25 || actual.Match != "read_all" || !actual.PublishParent || actual.Partitioning == nil || actual.Partitioning.Type != "example.Partitioner" || actual.Partitioning.Concurrency != 3 || actual.RelationalConcurrency != 4 || actual.AllowNulls == nil || !*actual.AllowNulls || actual.Groupable == nil || !*actual.Groupable || actual.Selector == nil || actual.Selector.Namespace != "a" || !actual.Selector.AllowFields || !actual.Selector.AllowOrderBy || !actual.Selector.AllowCriteria || !actual.Selector.AllowLimit || !actual.Selector.AllowOffset || !actual.Selector.AllowPage || len(actual.Selector.Filterable) != 2 || len(actual.Selector.OrderAliases) != 2 {
		t.Fatalf("unexpected view: %+v", actual)
	}
}

func TestParseViewRejectsMalformedOptions(t *testing.T) {
	for _, value := range []string{`accounts,batch=nope`, `accounts,limit=-1`, `accounts,match=unknown`, `accounts,publishParent=maybe`, `accounts,relationalConcurrency=-1`, `accounts,relationalConcurrency=nope`, `accounts,allowNulls=maybe`, `accounts,groupable=maybe`, `accounts,concurrency=3`, `accounts,selectorProjection=maybe`, `accounts,selectorOrderByColumns={bad}`, `accounts,selectorOrderByColumns={name:Name,name:Other}`, `accounts,unknown=value`} {
		if _, err := ParseView(value); err == nil {
			t.Fatalf("expected %q to fail", value)
		}
	}
}

func TestViewValueRoundTripsCanonicalOptions(t *testing.T) {
	allowNulls := true
	groupable := false
	limit := 25
	offset := 5
	source := View{
		Name: "items", TypeName: "ItemRow", Dest: "items.go", URI: "queries/items.sql", Connector: "analytics", Table: "order_items", Cache: "items", CacheWarmup: "startup",
		OrderBy: "created_at DESC", Limit: &limit, Offset: &offset,
		Batch: 50, PublishParent: true, RelationalConcurrency: 3, AllowNulls: &allowNulls, Groupable: &groupable,
		Partitioning: &spec.Partitioning{Type: "example.Partitioner", Concurrency: 2},
		Selector: &spec.Selector{Namespace: "i", AllowFields: true, AllowOrderBy: true, NoLimit: true,
			DefaultOrder: "created_at DESC", DefaultLimit: 100,
			Filterable: []spec.FieldPath{"ID"}, Orderable: []spec.FieldPath{"CREATED_AT"},
			OrderAliases: map[string]spec.FieldPath{"created": "CREATED_AT"}},
	}
	value, err := source.Value()
	if err != nil {
		t.Fatalf("Value() error = %v", err)
	}
	actual, err := ParseView(value)
	if err != nil {
		t.Fatalf("ParseView() error = %v\n%s", err, value)
	}
	if actual.Name != source.Name || actual.TypeName != source.TypeName || actual.Dest != source.Dest || actual.URI != source.URI ||
		actual.Connector != source.Connector || actual.Table != source.Table ||
		actual.CacheWarmup != source.CacheWarmup || actual.OrderBy != source.OrderBy ||
		actual.Limit == nil || *actual.Limit != limit || actual.Offset == nil || *actual.Offset != offset ||
		actual.AllowNulls == nil || !*actual.AllowNulls ||
		actual.Groupable == nil || *actual.Groupable || actual.Selector == nil ||
		actual.Selector.DefaultOrder != source.Selector.DefaultOrder || actual.Selector.DefaultLimit != 100 || !actual.Selector.NoLimit ||
		len(actual.Selector.Orderable) != 1 || actual.Selector.OrderAliases["created"] != "CREATED_AT" {
		t.Fatalf("round trip = %+v\n%s", actual, value)
	}
}

func TestViewValueRoundTripsCommaBearingScalar(t *testing.T) {
	for _, expected := range []string{`created_at DESC, owner's\rank`, `'owner rank`} {
		value, err := (View{OrderBy: expected}).Value()
		if err != nil {
			t.Fatalf("Value(%q) error = %v", expected, err)
		}
		actual, err := ParseView(value)
		if err != nil {
			t.Fatalf("ParseView() error = %v\n%s", err, value)
		}
		if actual == nil || actual.OrderBy != expected {
			t.Fatalf("round trip = %+v\n%s", actual, value)
		}
	}
}

func TestRelationValueRoundTripsCompoundLinks(t *testing.T) {
	links := []*RelationLink{
		{Parent: RelationPart{Field: "ID", Namespace: "o", Column: "id"}, Child: RelationPart{Field: "OrderID", Namespace: "i", Column: "order_id"}},
		{Parent: RelationPart{Field: "TenantID", Namespace: "o", Column: "tenant_id"}, Child: RelationPart{Field: "TenantID", Namespace: "i", Column: "tenant_id"}},
	}
	value, err := RelationValue(links)
	if err != nil {
		t.Fatalf("RelationValue() error = %v", err)
	}
	actual, err := ParseRelation(value)
	if err != nil || len(actual) != 2 || actual[1].Parent.Namespace != "o" || actual[1].Child.Column != "tenant_id" {
		t.Fatalf("ParseRelation() = %+v, %v\n%s", actual, err, value)
	}
}
