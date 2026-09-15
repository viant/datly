package compile

import (
	"errors"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
	"github.com/viant/sqlparser/expr"
)

func TestReaderCompileBuildsCompoundRelationChain(t *testing.T) {
	root := &spec.View{Name: "Orders", Source: &spec.ViewSource{SQL: "authored SQL"}}
	actual, err := NewReader().Compile(ReadInput{View: root, SQL: `SELECT o.*
FROM orders o
JOIN order_items i ON i.order_id = o.id AND i.tenant_id = o.tenant_id
JOIN products p ON p.id = i.product_id`})

	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	if actual == root || actual.Namespace != "o" || actual.Source.Table != "orders" || actual.Source.SQL != "" {
		t.Fatalf("root = %+v", actual)
	}
	if len(actual.Relations) != 1 {
		t.Fatalf("relations = %#v", actual.Relations)
	}
	items := actual.Relations[0]
	if items.Name != "i" || items.Holder != "I" || items.ParentNamespace != "o" || len(items.On) != 2 {
		t.Fatalf("items relation = %+v", items)
	}
	if items.On[0].ParentColumn != "id" || items.On[0].ChildColumn != "order_id" ||
		items.On[1].ParentColumn != "tenant_id" || items.On[1].ChildColumn != "tenant_id" {
		t.Fatalf("items links = %+v", items.On)
	}
	if len(items.View.Relations) != 1 {
		t.Fatalf("nested relations = %#v", items.View.Relations)
	}
	products := items.View.Relations[0]
	if products.ParentNamespace != "i" || len(products.On) != 1 || products.On[0].ChildColumn != "id" {
		t.Fatalf("products relation = %+v", products)
	}
}

func TestReaderCompileLowersProjectionExclusions(t *testing.T) {
	actual, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Vendors", Source: &spec.ViewSource{}}, SQL: `SELECT vendor.* EXCEPT ID,
products.* EXCEPT (VENDOR_ID, INTERNAL_NOTE)
FROM vendors vendor
JOIN products products ON products.vendor_id = vendor.id`})

	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	if strings.Contains(strings.ToUpper(actual.Source.SQL), "EXCEPT") {
		t.Fatalf("executable SQL retains EXCEPT: %s", actual.Source.SQL)
	}
	if len(actual.Columns) != 1 || actual.Columns[0].Name != "ID" || actual.Columns[0].Tag != `internal:"true"` {
		t.Fatalf("root columns = %+v", actual.Columns)
	}
	if len(actual.Relations) != 1 || len(actual.Relations[0].View.Columns) != 2 {
		t.Fatalf("relations = %+v", actual.Relations)
	}
	for _, column := range actual.Relations[0].View.Columns {
		if column.Tag != `internal:"true"` {
			t.Fatalf("child column = %+v", column)
		}
	}
}

func TestReaderCompileDecomposesMultiViewSources(t *testing.T) {
	actual, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Orders", Source: &spec.ViewSource{}}, SQL: `SELECT orders.*, items.*
FROM (SELECT id, tenant_id FROM orders WHERE active = 1) orders
JOIN (SELECT order_id, tenant_id, name FROM items WHERE archived = 0) items
  ON items.order_id = orders.id AND items.tenant_id = orders.tenant_id`})

	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	if strings.Join(strings.Fields(actual.Source.SQL), " ") != "SELECT * FROM (SELECT id, tenant_id FROM orders WHERE active = 1) orders" || actual.Source.Table != "orders" {
		t.Fatalf("root source = %+v", actual.Source)
	}
	child := actual.Relations[0].View
	if strings.Join(strings.Fields(child.Source.SQL), " ") != "SELECT * FROM (SELECT order_id, tenant_id, name FROM items WHERE archived = 0) items" || child.Source.Table != "items" {
		t.Fatalf("child source = %+v", child.Source)
	}
}

func TestReaderCompileDecomposesCTEBackedMultiViewSources(t *testing.T) {
	actual, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Orders", Source: &spec.ViewSource{}}, SQL: `WITH
active_orders AS (SELECT id, tenant_id FROM orders WHERE active = 1),
active_items AS (SELECT order_id, tenant_id, name FROM items WHERE archived = 0)
SELECT orders.*, items.*
FROM active_orders orders
JOIN active_items items ON items.order_id = orders.id AND items.tenant_id = orders.tenant_id`})

	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	if !strings.Contains(actual.Source.SQL, "WITH active_orders AS") ||
		!strings.Contains(actual.Source.SQL, "SELECT * FROM active_orders orders") || strings.Contains(actual.Source.SQL, " JOIN ") {
		t.Fatalf("root source = %+v", actual.Source)
	}
	child := actual.Relations[0].View
	if !strings.Contains(child.Source.SQL, "WITH active_orders AS") ||
		!strings.Contains(child.Source.SQL, "SELECT * FROM active_items items") || strings.Contains(child.Source.SQL, " JOIN ") {
		t.Fatalf("child source = %+v", child.Source)
	}
}

func TestReaderCompileRejectsOuterUnionWithRelations(t *testing.T) {
	_, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Orders", Source: &spec.ViewSource{}}, SQL: `
SELECT orders.*, items.* FROM orders orders JOIN items items ON items.order_id = orders.id
UNION ALL SELECT archived.*, archived_items.* FROM archived_orders archived
JOIN archived_items archived_items ON archived_items.order_id = archived.id`})

	var compileError *Error
	if !errors.As(err, &compileError) || compileError.Code != CodeRelationUnsupported ||
		!strings.Contains(err.Error(), "outer UNION") {
		t.Fatalf("Compile() error = %#v", err)
	}
}

func TestReaderCompileKeepsSingleViewExecutableSQL(t *testing.T) {
	actual, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Orders", Source: &spec.ViewSource{}}, SQL: `SELECT orders.* EXCEPT INTERNAL_NOTE FROM orders orders WHERE active = 1`})

	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	if !strings.Contains(actual.Source.SQL, "WHERE active = 1") || strings.Contains(strings.ToUpper(actual.Source.SQL), "EXCEPT") {
		t.Fatalf("root source = %+v", actual.Source)
	}
}

func TestReaderCompileLowersUnqualifiedRootProjectionExclusion(t *testing.T) {
	actual, err := NewReader().Compile(ReadInput{
		View: &spec.View{Name: "Orders", Source: &spec.ViewSource{}},
		SQL:  `SELECT * EXCEPT INTERNAL_NOTE FROM orders`,
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	if len(actual.Columns) != 1 || actual.Columns[0].Name != "INTERNAL_NOTE" ||
		actual.Columns[0].Tag != `internal:"true"` || strings.Contains(strings.ToUpper(actual.Source.SQL), "EXCEPT") {
		t.Fatalf("root = %+v", actual)
	}
}

func TestReaderCompilePreservesPreambleWhenSingleViewSQLIsRewritten(t *testing.T) {
	actual, err := NewReader().Compile(ReadInput{
		View:     &spec.View{Name: "Orders", Source: &spec.ViewSource{}},
		SQL:      `SELECT orders.* EXCEPT INTERNAL_NOTE, set_limit(orders, 10) FROM orders orders WHERE id = $criteria`,
		Template: TemplateFrame{Prefix: `#set($criteria = $ID)`},
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	if !strings.HasPrefix(actual.Source.SQL, "#set($criteria = $ID)\nSELECT") ||
		strings.Contains(strings.ToUpper(actual.Source.SQL), "EXCEPT") || strings.Contains(strings.ToLower(actual.Source.SQL), "set_limit(") {
		t.Fatalf("root source = %+v", actual.Source)
	}
}

func TestReaderCompilePreservesBalancedTemplateFrameWhenRewriting(t *testing.T) {
	actual, err := NewReader().Compile(ReadInput{
		View:     &spec.View{Name: "Orders", Source: &spec.ViewSource{}},
		SQL:      `SELECT orders.* EXCEPT INTERNAL_NOTE FROM orders orders`,
		Template: TemplateFrame{Prefix: `#if($Enabled)`, Suffix: `#end`},
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	if !strings.HasPrefix(actual.Source.SQL, "#if($Enabled)\nSELECT") || !strings.HasSuffix(actual.Source.SQL, "\n#end") ||
		strings.Contains(strings.ToUpper(actual.Source.SQL), "EXCEPT") {
		t.Fatalf("root source = %+v", actual.Source)
	}
}

func TestReaderCompilePreservesBalancedTemplateFrameForEveryDecomposedView(t *testing.T) {
	actual, err := NewReader().Compile(ReadInput{
		View: &spec.View{Name: "Orders", Source: &spec.ViewSource{}},
		SQL: `SELECT orders.*, items.* FROM orders orders
JOIN items items ON items.order_id = orders.id`,
		Template: TemplateFrame{Prefix: `#if($Enabled)`, Suffix: `#end`},
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	for _, source := range []*spec.ViewSource{actual.Source, actual.Relations[0].View.Source} {
		if source == nil || !strings.HasPrefix(source.SQL, "#if($Enabled)\nSELECT") || !strings.HasSuffix(source.SQL, "\n#end") {
			t.Fatalf("source = %+v", source)
		}
	}
}

func TestReaderCompilePreservesRootOwnedOuterClauses(t *testing.T) {
	actual, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Orders", Source: &spec.ViewSource{}}, SQL: `
SELECT orders.*, items.*
FROM orders orders
JOIN items items ON items.order_id = orders.id
WHERE orders.tenant_id = $TenantID
ORDER BY orders.id DESC
LIMIT 20 OFFSET 5`})

	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	SQL := actual.Source.SQL
	for _, expected := range []string{"SELECT * FROM orders orders", "WHERE orders.tenant_id = $TenantID", "ORDER BY orders.id DESC", "LIMIT 20", "OFFSET 5"} {
		if !strings.Contains(SQL, expected) {
			t.Fatalf("root source %q does not contain %q", SQL, expected)
		}
	}
	if strings.Contains(SQL, " JOIN ") {
		t.Fatalf("root source retains relation join: %s", SQL)
	}
}

func TestReaderCompileRejectsChildOwnedOuterClause(t *testing.T) {
	_, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Orders", Source: &spec.ViewSource{}}, SQL: `
SELECT orders.*, items.* FROM orders orders
JOIN items items ON items.order_id = orders.id
WHERE items.active = 1`})

	var compileError *Error
	if !errors.As(err, &compileError) || compileError.Code != CodeRelationUnsupported ||
		!strings.Contains(err.Error(), `child view "items"`) {
		t.Fatalf("Compile() error = %#v", err)
	}
}

func TestReaderCompileRejectsUnqualifiedOuterClause(t *testing.T) {
	for _, clause := range []string{"WHERE active = 1", "ORDER BY id"} {
		_, err := NewReader().Compile(ReadInput{
			View: &spec.View{Name: "Orders", Source: &spec.ViewSource{}},
			SQL: `SELECT orders.*, items.* FROM orders orders
JOIN items items ON items.order_id = orders.id ` + clause,
		})
		var compileError *Error
		if !errors.As(err, &compileError) || compileError.Code != CodeRelationUnsupported ||
			!strings.Contains(err.Error(), "ambiguous unqualified column") {
			t.Fatalf("Compile(%q) error = %#v", clause, err)
		}
	}
}

func TestReaderCompileProjectionExclusionsMergeExistingMetadata(t *testing.T) {
	root := &spec.View{Name: "Vendors", Source: &spec.ViewSource{}, Columns: []*spec.Column{{
		Name: "ID", Source: "vendor_id", Type: spec.TypeRef{Name: "int64"}, Tag: `json:"id"`,
	}}}
	actual, err := NewReader().Compile(ReadInput{View: root, SQL: `SELECT vendor.* EXCEPT vendor_id FROM vendors vendor`})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	if len(actual.Columns) != 1 || actual.Columns[0].Type.Name != "int64" ||
		!strings.Contains(actual.Columns[0].Tag, `json:"id"`) || !strings.Contains(actual.Columns[0].Tag, `internal:"true"`) {
		t.Fatalf("columns = %+v", actual.Columns)
	}
}

func TestReaderCompileRejectsUnknownProjectionExclusionNamespace(t *testing.T) {
	_, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Vendors", Source: &spec.ViewSource{}}, SQL: `SELECT missing.* EXCEPT ID FROM vendors vendor`})

	var compileError *Error
	if !errors.As(err, &compileError) || compileError.Code != CodeViewDirective ||
		!strings.Contains(err.Error(), `namespace "missing"`) {
		t.Fatalf("Compile() error = %#v", err)
	}
}

func TestReaderCompileLowersTargetedViewDirectives(t *testing.T) {
	actual, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Orders", Source: &spec.ViewSource{}}, SQL: `SELECT o.*, i.*,
order_by(o, 'created_at DESC'), use_cache(o, 'orders'),
set_limit(i, 25), use_connector(i, 'analytics'), cache_warmup(i, 'startup')
FROM orders o JOIN order_items i ON i.order_id = o.id`})

	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	if actual.Source.Controls == nil || actual.Source.Controls.OrderBy != "created_at DESC" ||
		actual.Source.Bindings == nil || actual.Source.Bindings.CacheName != "orders" {
		t.Fatalf("root source = %+v", actual.Source)
	}
	child := actual.Relations[0].View
	if child.Source.Controls == nil || child.Source.Controls.Limit == nil || *child.Source.Controls.Limit != 25 ||
		child.Source.Bindings == nil || child.Source.Bindings.Connector != "analytics" || child.Source.Bindings.CacheWarmup != "startup" {
		t.Fatalf("child source = %+v", child.Source)
	}
	for _, name := range []string{"order_by", "use_cache", "set_limit", "use_connector", "cache_warmup"} {
		if strings.Contains(strings.ToLower(actual.Source.SQL), name+"(") {
			t.Fatalf("executable SQL retains %s: %s", name, actual.Source.SQL)
		}
	}
}

func TestReaderCompileLowersCanonicalViewDecorators(t *testing.T) {
	actual, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Orders", Source: &spec.ViewSource{}}, SQL: `SELECT o.*, i.*,
allow_nulls(o), grouping_enabled(o),
allowed_order_by_columns(o, 'o.created:CREATED_AT,id'),
self_ref(i, 'Children', 'ID', 'PARENT_ID'),
cardinality(o, 'One'), cardinality(i, 'one'),
type(o, 'OrderRow'), dest(o, 'orders.go'), type(i, 'ItemRow'), dest(i, 'items.go')
FROM orders o JOIN order_items i ON i.order_id = o.id`})

	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	if actual.AllowNulls == nil || !*actual.AllowNulls || actual.Groupable == nil || !*actual.Groupable || actual.Cardinality != spec.CardinalityOne {
		t.Fatalf("root decorators = %+v", actual)
	}
	if actual.TypeName != "OrderRow" || actual.Dest != "orders.go" {
		t.Fatalf("root generation metadata = %+v", actual)
	}
	if actual.Selector == nil || !actual.Selector.AllowOrderBy ||
		actual.Selector.OrderAliases["o.created"] != "CREATED_AT" || actual.Selector.OrderAliases["created"] != "CREATED_AT" ||
		len(actual.Selector.Orderable) != 1 || actual.Selector.Orderable[0] != "id" {
		t.Fatalf("root selector = %+v", actual.Selector)
	}
	relation := actual.Relations[0]
	if relation.Cardinality != spec.CardinalityOne || relation.View.Cardinality != spec.CardinalityOne {
		t.Fatalf("relation cardinality = %+v", relation)
	}
	if relation.View.TypeName != "ItemRow" || relation.View.Dest != "items.go" {
		t.Fatalf("relation generation metadata = %+v", relation.View)
	}
	if self := relation.View.SelfReference; self == nil || self.Holder != "Children" || self.Child != "ID" || self.Parent != "PARENT_ID" {
		t.Fatalf("self reference = %+v", self)
	}
	for _, name := range []string{"allow_nulls", "grouping_enabled", "allowed_order_by_columns", "self_ref", "cardinality", "type", "dest"} {
		if strings.Contains(strings.ToLower(actual.Source.SQL), name+"(") {
			t.Fatalf("executable SQL retains %s: %s", name, actual.Source.SQL)
		}
	}
}

func TestReaderCompileLowersExecutionViewDecorators(t *testing.T) {
	actual, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Orders", Source: &spec.ViewSource{}}, SQL: `SELECT o.*, i.*,
batch_size(i, 200), batch_concurrency(i, 2), publish_parent(i), relational_concurrency(i, 4),
set_partitioner(i, 'example.ItemPartitioner', 3), match_strategy(i, 'read_all')
FROM orders o JOIN order_items i ON i.order_id = o.id`})

	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	if len(actual.Relations) != 1 {
		t.Fatalf("relations = %+v", actual.Relations)
	}
	relation := actual.Relations[0]
	child := relation.View
	if child.BatchSize != 200 || child.BatchConcurrency != 2 || !child.PublishParent || child.RelationalConcurrency != 4 ||
		child.Partitioning == nil || child.Partitioning.Type != "example.ItemPartitioner" || child.Partitioning.Concurrency != 3 ||
		relation.MatchStrategy != spec.MatchReadAll {
		t.Fatalf("execution decorators = relation:%+v child:%+v", relation, child)
	}
	for _, name := range []string{"batch_size", "batch_concurrency", "publish_parent", "relational_concurrency", "set_partitioner", "match_strategy"} {
		if strings.Contains(strings.ToLower(actual.Source.SQL), name+"(") {
			t.Fatalf("executable SQL retains %s: %s", name, actual.Source.SQL)
		}
	}
}

func TestReaderCompileMergesAdditiveAllowedOrderByDecorators(t *testing.T) {
	actual, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Orders", Source: &spec.ViewSource{}}, SQL: `SELECT o.*,
allowed_order_by_columns(o, 'created:created_at'), allowed_order_by_columns(o, 'id')
FROM orders o`})

	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	if actual.Selector == nil || actual.Selector.OrderAliases["created"] != "created_at" ||
		len(actual.Selector.Orderable) != 1 || actual.Selector.Orderable[0] != "id" {
		t.Fatalf("selector = %+v", actual.Selector)
	}
}

func TestReaderCompileRejectsDuplicateSingletonViewDirectives(t *testing.T) {
	tests := []string{
		`SELECT o.*, batch_size(o, 10), batch_size(o, 20) FROM orders o`,
		`SELECT o.*, relational_concurrency(o, 2), relational_concurrency(o, 4) FROM orders o`,
		`SELECT o.*, set_partitioner(o, 'One'), set_partitioner(o, 'Two') FROM orders o`,
		`SELECT o.*, cardinality(o, 'one'), cardinality(o, 'many') FROM orders o`,
		`SELECT o.*, type(o, 'One'), type(o, 'Two') FROM orders o`,
		`SELECT o.*, dest(o, 'one.go'), dest(o, 'two.go') FROM orders o`,
		`SELECT o.*, groupable(o), grouping_enabled(o) FROM orders o`,
		`SELECT o.*, i.*, match_strategy(i, 'read_all'), match_strategy(i, 'read_matched') FROM orders o JOIN items i ON i.order_id = o.id`,
		`SELECT wrapper.*, use_cache(wrapper, 'outer') FROM (SELECT o.*, use_cache(o, 'inner') FROM orders o) wrapper`,
	}
	for _, SQL := range tests {
		_, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Orders", Source: &spec.ViewSource{}}, SQL: SQL})
		var compileError *Error
		if !errors.As(err, &compileError) || compileError.Code != CodeViewDirective {
			t.Fatalf("Compile(%q) error = %#v", SQL, err)
		}
	}
}

func TestReaderCompileRejectsInvalidViewDirective(t *testing.T) {
	tests := []string{
		`SELECT o.*, set_limit(o, 'many') FROM orders o`,
		`SELECT o.*, use_connector(missing, 'analytics') FROM orders o`,
		`SELECT o.*, use_connector(Orders, 'analytics') FROM orders o`,
		`SELECT o.*, use_connector(o, CONCAT('ana', 'lytics')) FROM orders o`,
		`SELECT o.*, use_cache(o, 123) FROM orders o`,
		`SELECT o.*, cache_warmup(o, true) FROM orders o`,
		`SELECT o.*, order_by(o, created_at) FROM orders o`,
		`SELECT o.*, use_cache(LOWER(o), 'orders') FROM orders o`,
		`SELECT o.*, COALESCE(use_cache(o, 'orders'), '') FROM orders o`,
		`SELECT set_limit(o, 10) FROM orders o`,
		`SELECT o.*, allow_nulls(o, true) FROM orders o`,
		`SELECT o.*, groupable(missing) FROM orders o`,
		`SELECT o.*, allowed_order_by_columns(o, '') FROM orders o`,
		`SELECT o.*, allowed_order_by_columns(o, 'display:name,display:id') FROM orders o`,
		`SELECT o.*, allowed_order_by_columns(o, 'Display:name,display:id') FROM orders o`,
		`SELECT o.*, allowed_order_by_columns(o, 'id,id:other') FROM orders o`,
		`SELECT o.*, allowed_order_by_columns(o, 'id,o.id:other') FROM orders o`,
		`SELECT o.*, allowed_order_by_columns(o, 'o.id:other,id') FROM orders o`,
		`SELECT o.*, cardinality(o, 'some') FROM orders o`,
		`SELECT o.*, self_ref(o, 'Children', ID, 'PARENT_ID') FROM orders o`,
		`SELECT o.*, type(o, OrderRow) FROM orders o`,
		`SELECT o.*, dest(o, '') FROM orders o`,
		`SELECT o.*, batch_size(o, -1) FROM orders o`,
		`SELECT o.*, batch_size(o, '10') FROM orders o`,
		`SELECT o.*, batch_concurrency(o, -1) FROM orders o`,
		`SELECT o.*, batch_concurrency(o, '2') FROM orders o`,
		`SELECT o.*, batch_concurrency(o, 1.5) FROM orders o`,
		`SELECT o.*, batch_concurrency(missing, 2) FROM orders o`,
		`SELECT o.*, publish_parent(o, true) FROM orders o`,
		`SELECT o.*, relational_concurrency(o, -1) FROM orders o`,
		`SELECT o.*, set_partitioner(o, '', 2) FROM orders o`,
		`SELECT o.*, set_partitioner(o, 'example.Partitioner', '2') FROM orders o`,
		`SELECT o.*, match_strategy(o, 'read_all') FROM orders o`,
		`SELECT o.*, i.*, match_strategy(i, 'unknown') FROM orders o JOIN items i ON i.order_id = o.id`,
	}
	for _, SQL := range tests {
		_, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Orders", Source: &spec.ViewSource{}}, SQL: SQL})
		var compileError *Error
		if !errors.As(err, &compileError) || compileError.Code != CodeViewDirective {
			t.Fatalf("Compile(%q) error = %#v", SQL, err)
		}
	}
}

func TestReaderCompileRejectsNestedSourceViewDirectives(t *testing.T) {
	for _, SQL := range []string{
		`SELECT wrapper.*, items.* FROM (SELECT o.*, set_limit(o, 50) FROM orders o) wrapper JOIN (SELECT i.*, use_cache(i, 'items'), use_connector(i, 'analytics') FROM items i) items ON items.order_id = wrapper.id`,
		`SELECT orders.*, items.* FROM orders orders JOIN (SELECT i.*, use_cache(i, 'items') FROM (${embed:sql/items.sql}) i) items ON items.order_id = orders.id`,
		`WITH source AS (SELECT o.*, use_cache(o, 'orders') FROM orders o) SELECT wrapper.*, set_limit(wrapper, 10) FROM source wrapper`,
		`SELECT wrapper.* FROM orders wrapper UNION ALL SELECT archived.*, allow_nulls(archived) FROM archived_orders archived`,
	} {
		_, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Orders", Source: &spec.ViewSource{}}, SQL: SQL})
		var compileError *Error
		if !errors.As(err, &compileError) || compileError.Code != CodeViewDirective || !strings.Contains(err.Error(), "inside database SQL") {
			t.Fatalf("Compile(%q) error = %v", SQL, err)
		}
	}
}

func TestReaderCompileOuterControlsPreserveNestedSQL(t *testing.T) {
	const SQL = `SELECT orders.*, items.*, set_limit(orders,50),use_cache(items,'items'),use_connector(items,'analytics') FROM (SELECT o.* FROM orders o WHERE o.active=1) orders JOIN (SELECT i.* FROM (${embed:sql/items.sql}) i) items ON items.order_id=orders.id`
	actual, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Orders", Source: &spec.ViewSource{SQL: SQL}}, SQL: SQL})
	if err != nil {
		t.Fatal(err)
	}
	child := actual.Relations[0].View
	if actual.Source.Controls == nil || actual.Source.Controls.Limit == nil || *actual.Source.Controls.Limit != 50 {
		t.Fatalf("root controls=%+v", actual.Source.Controls)
	}
	if child.Source.Bindings == nil || child.Source.Bindings.CacheName != "items" || child.Source.Bindings.Connector != "analytics" || len(child.Source.Embeds) != 1 || !strings.Contains(child.Source.SQL, "${embed:sql/items.sql}") {
		t.Fatalf("child=%+v", child.Source)
	}
	if !strings.Contains(actual.Source.SQL, "o.active=1") {
		t.Fatal(actual.Source.SQL)
	}
}

func TestReaderCompileKeepsUnionBranchScopesInternal(t *testing.T) {
	tests := []struct {
		name string
		SQL  string
	}{
		{
			name: "branch local CTE shadows outer CTE",
			SQL: `WITH source AS (SELECT a.* FROM audit a)
SELECT left_source.*, use_cache(left_source, 'outer') FROM source left_source
UNION ALL
WITH source AS (SELECT b.* FROM backup b)
SELECT right_source.* FROM source right_source`,
		},
		{
			name: "branch join remains internal",
			SQL: `WITH source AS (SELECT a.* FROM audit a)
SELECT left_source.*, use_cache(left_source, 'outer') FROM source left_source
UNION ALL
SELECT archived.* FROM archived archived
JOIN source internal_source ON internal_source.id = archived.id`,
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			actual, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Audit", Source: &spec.ViewSource{}}, SQL: testCase.SQL})
			if err != nil {
				t.Fatalf("Compile() error = %v", err)
			}
			if actual.Source == nil || actual.Source.Bindings == nil || actual.Source.Bindings.CacheName != "outer" || len(actual.Relations) != 0 {
				t.Fatalf("root = %+v", actual)
			}
		})
	}
}

func TestReaderCompileRejectsInvalidNestedViewDirectivePlacement(t *testing.T) {
	tests := []string{
		`SELECT x.* FROM (SELECT o.*, COALESCE(use_cache(o, 'orders'), '') FROM orders o) x`,
		`SELECT x.*, j.* FROM (SELECT o.*, use_cache(j, 'internal') FROM orders o JOIN audit j ON j.order_id = o.id) x JOIN jobs j ON j.order_id = x.id`,
		`WITH j AS (SELECT a.*, use_cache(a, 'wrong') FROM audit a) SELECT o.*, j.* FROM orders o JOIN jobs j ON j.order_id = o.id`,
		`WITH source AS (SELECT a.*, use_cache(a, 'shared') FROM audit a) SELECT left_source.*, right_source.* FROM source left_source JOIN source right_source ON right_source.id = left_source.id`,
		`WITH source AS (SELECT a.*, use_cache(a, 'shared') FROM audit a) SELECT left_source.* FROM source left_source UNION ALL SELECT right_source.* FROM source right_source`,
	}
	for _, SQL := range tests {
		_, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Orders", Source: &spec.ViewSource{}}, SQL: SQL})
		var compileError *Error
		if !errors.As(err, &compileError) || compileError.Code != CodeViewDirective {
			t.Fatalf("Compile(%q) error = %#v", SQL, err)
		}
	}
}

func TestDatabaseSourceRejectsUnparseableSQL(t *testing.T) {
	err := validateDatabaseSource(&expr.Raw{Raw: `(SELECT i.* FROM items i WHERE ')`})
	var compileError *Error
	if !errors.As(err, &compileError) || compileError.Code != CodeSQLParse {
		t.Fatalf("validateDatabaseSource() error = %#v", err)
	}
}

func TestReaderCompileRejectsDuplicateViewNamespaces(t *testing.T) {
	_, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Orders", Source: &spec.ViewSource{}}, SQL: `SELECT o.*, i.*
FROM orders o
JOIN order_items i ON i.order_id = o.id
JOIN invoices i ON i.order_id = o.id`})

	var compileError *Error
	if !errors.As(err, &compileError) || compileError.Code != CodeRelationAmbiguous {
		t.Fatalf("Compile() error = %#v", err)
	}
}

func TestReaderCompilePreservesChildSubqueryEmbedMetadata(t *testing.T) {
	actual, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Orders", Source: &spec.ViewSource{}}, SQL: `SELECT o.*, i.* FROM orders o JOIN (${embed:sql/items.sql}) i ON i.order_id = o.id`})

	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	source := actual.Relations[0].View.Source
	if source == nil || len(source.Embeds) != 1 || source.Embeds[0].Path != "sql/items.sql" ||
		!strings.Contains(source.SQL, source.Embeds[0].Raw) {
		t.Fatalf("child source = %+v", source)
	}
}

func TestReaderCompileSetLimitZeroDisablesDefaultLimit(t *testing.T) {
	actual, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Orders", Source: &spec.ViewSource{}}, SQL: `SELECT o.*, set_limit(o, 0) FROM orders o`})

	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	if actual.Selector == nil || !actual.Selector.NoLimit {
		t.Fatalf("selector = %+v", actual.Selector)
	}
	if actual.Source.Controls != nil && actual.Source.Controls.Limit != nil {
		t.Fatalf("controls = %+v", actual.Source.Controls)
	}
}

func TestReaderCompilePreservesSubquerySource(t *testing.T) {
	actual, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Orders", Source: &spec.ViewSource{}}, SQL: `SELECT o.*, i.* FROM (SELECT * FROM orders) o JOIN (SELECT * FROM items) i ON i.order_id = o.id`})

	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	if actual.Namespace != "o" || actual.Source.Table != "orders" {
		t.Fatalf("root = %+v", actual)
	}
	child := actual.Relations[0].View
	if child.Source == nil || child.Source.SQL != "SELECT * FROM (SELECT * FROM items) i" {
		t.Fatalf("child source = %+v", child.Source)
	}
}

func TestReaderCompileRejectsNonEqualityAndUnqualifiedRelations(t *testing.T) {
	tests := []string{
		`SELECT o.* FROM orders o JOIN items i ON i.order_id > o.id`,
		`SELECT o.* FROM orders o JOIN items i ON order_id = id`,
		`SELECT o.* FROM orders o JOIN items i ON i.order_id = o.id OR i.tenant_id = o.tenant_id`,
		`SELECT o.* FROM orders o JOIN items i ON i.order_id = o.id AND 1 > 0`,
	}
	for _, SQL := range tests {
		_, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Orders", Source: &spec.ViewSource{}}, SQL: SQL})
		var compileError *Error
		if !errors.As(err, &compileError) || compileError.Code != CodeRelationUnsupported && compileError.Code != CodeRelationAmbiguous {
			t.Fatalf("Compile(%q) error = %#v", SQL, err)
		}
	}
}

func TestReaderCompileReportsRelationPredicateSpan(t *testing.T) {
	for _, testCase := range []struct {
		name string
		SQL  string
		code string
	}{
		{name: "unsupported", SQL: `SELECT o.* FROM orders o JOIN items i ON i.order_id > o.id`, code: CodeRelationUnsupported},
		{name: "ambiguous", SQL: `SELECT o.* FROM orders o JOIN items i ON x.order_id = o.id`, code: CodeRelationAmbiguous},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Orders", Source: &spec.ViewSource{}}, SQL: testCase.SQL})
			var compileError *Error
			if !errors.As(err, &compileError) || compileError.Code != testCase.code {
				t.Fatalf("Compile() error = %#v", err)
			}
			wantStart := strings.Index(testCase.SQL, "ON ")
			if compileError.Offset != wantStart || compileError.End != len(testCase.SQL) {
				t.Fatalf("span = [%d:%d], want [%d:%d]", compileError.Offset, compileError.End, wantStart, len(testCase.SQL))
			}
		})
	}
}

func TestReaderCompileRejectsOtherLiteralConjunct(t *testing.T) {
	_, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Orders", Source: &spec.ViewSource{}}, SQL: `SELECT o.* FROM orders o JOIN items i ON i.order_id = o.id AND 2 = 2`})

	var compileError *Error
	if !errors.As(err, &compileError) || compileError.Code != CodeRelationUnsupported {
		t.Fatalf("Compile() error = %#v", err)
	}
}

func TestReaderCompileUsesTerminalSchemaQualifiedTableName(t *testing.T) {
	actual, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Orders", Source: &spec.ViewSource{}}, SQL: `SELECT orders.* FROM sales.orders JOIN sales.items ON items.order_id = orders.id`})

	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	if actual.Namespace != "orders" || actual.Relations[0].View.Namespace != "items" {
		t.Fatalf("root = %+v, relation = %+v", actual, actual.Relations[0])
	}
}

func TestReaderCompileToOneJoinHint(t *testing.T) {
	for _, condition := range []string{"i.order_id=o.id AND 1=1", "i.order_id=o.id AND (1 = 1)", "1=1 AND i.order_id=o.id", "i.order_id=o.id AND i.tenant=o.tenant AND 1=1"} {
		t.Run(condition, func(t *testing.T) {
			actual, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Orders", Source: &spec.ViewSource{}}, SQL: "SELECT o.* FROM orders o JOIN items i ON " + condition})
			if err != nil {
				t.Fatal(err)
			}
			rel := actual.Relations[0]
			if rel.Cardinality != spec.CardinalityOne || rel.View.Cardinality != spec.CardinalityOne {
				t.Fatalf("cardinality=%s/%s", rel.Cardinality, rel.View.Cardinality)
			}
			want := 1
			if strings.Contains(condition, "i.tenant") {
				want = 2
			}
			if len(rel.On) != want {
				t.Fatalf("relation keys=%v", rel.On)
			}
		})
	}
	for _, condition := range []string{"1=1", "i.order_id=o.id OR 1=1", "i.order_id=o.id AND '1'='1'"} {
		if _, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Orders", Source: &spec.ViewSource{}}, SQL: "SELECT o.* FROM orders o JOIN items i ON " + condition}); err == nil {
			t.Fatalf("unsupported condition accepted: %s", condition)
		}
	}
}

func TestReaderCompileToOneHintIsJoinScoped(t *testing.T) {
	actual, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Orders", Source: &spec.ViewSource{}}, SQL: "SELECT o.* FROM orders o JOIN items i ON i.order_id=o.id AND 1=1 JOIN notes n ON n.order_id=o.id"})
	if err != nil {
		t.Fatal(err)
	}
	if len(actual.Relations) != 2 || actual.Relations[0].Cardinality != spec.CardinalityOne || actual.Relations[1].Cardinality != spec.CardinalityMany {
		t.Fatalf("join-scoped cardinalities: %+v", actual.Relations)
	}
}

func TestReaderCompileToOneHintDirectivePrecedence(t *testing.T) {
	for _, tc := range []struct {
		name, projection, hint string
		want                   spec.Cardinality
	}{
		{"hint", "", " AND 1=1", spec.CardinalityOne},
		{"explicit many overrides hint", ", cardinality(i, 'Many')", " AND 1=1", spec.CardinalityMany},
		{"explicit one without hint", ", cardinality(i, 'One')", "", spec.CardinalityOne},
	} {
		t.Run(tc.name, func(t *testing.T) {
			view, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Orders", Source: &spec.ViewSource{}}, SQL: "SELECT o.*, i.*" + tc.projection + " FROM orders o JOIN items i ON i.order_id=o.id" + tc.hint})
			if err != nil {
				t.Fatal(err)
			}
			relation := view.Relations[0]
			if relation.Cardinality != tc.want || relation.View.Cardinality != tc.want || len(relation.On) != 1 || relation.On[0].ParentColumn != "id" || relation.On[0].ChildColumn != "order_id" {
				t.Fatalf("directive/marker metadata: %+v", relation)
			}
		})
	}
}

func TestReaderCompileToOneHintLeavesPhysicalJoinSQL(t *testing.T) {
	for _, outerHint := range []string{"", " AND 1=1"} {
		source := `SELECT o.*, i.* FROM (SELECT a.id FROM orders a JOIN tenants t ON a.tenant=t.id AND 1=1) o JOIN (SELECT b.order_id FROM items b JOIN products p ON b.product_id=p.id AND 1=1) i ON i.order_id=o.id` + outerHint
		view, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Orders", Source: &spec.ViewSource{}}, SQL: source})
		if err != nil {
			t.Fatal(err)
		}
		if len(view.Relations) != 1 || len(view.Relations[0].View.Relations) != 0 {
			t.Fatal("physical joins became view relations")
		}
		want := spec.CardinalityMany
		if outerHint != "" {
			want = spec.CardinalityOne
		}
		if view.Relations[0].Cardinality != want {
			t.Fatal("physical marker changed outer cardinality")
		}
		for _, SQL := range []string{view.Source.SQL, view.Relations[0].View.Source.SQL} {
			if !strings.Contains(SQL, "JOIN") || !(strings.Contains(SQL, "1 = 1") || strings.Contains(SQL, "1=1")) {
				t.Fatalf("physical join changed: %s", SQL)
			}
		}
	}
}

func TestReaderRejectsDatabaseScopeAnnotations(t *testing.T) {
	for _, annotation := range []string{
		`set_limit(o,10)`, `use_cache(o,'orders')`, `use_connector(o,'main')`, `cache_warmup(o,'orders')`,
		`cardinality(o,'One')`, `allow_nulls(o)`, `entity_hooks(o,'Hooks')`, `tag(o.ID,'json:"id"')`, `invariant(o.ID,'Identity')`,
	} {
		t.Run(annotation, func(t *testing.T) {
			SQL := `SELECT orders.* FROM (SELECT o.*,` + annotation + ` FROM ORDERS o) orders`
			_, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Orders", Source: &spec.ViewSource{SQL: SQL}}, SQL: SQL})
			var compileErr *Error
			if !errors.As(err, &compileErr) || compileErr.Code != CodeViewDirective {
				t.Fatalf("error=%v", err)
			}
		})
	}
}

func TestReaderPreservesDatabaseExpressions(t *testing.T) {
	for _, expression := range []string{
		`CAST(o.ID AS INTEGER)`, `CAST(o.ID AS DECIMAL(10,2))`, `COALESCE(o.ID,0)`, `ABS(o.ID)`, `CASE WHEN o.ID>0 THEN o.ID ELSE 0 END`,
	} {
		t.Run(expression, func(t *testing.T) {
			SQL := `SELECT orders.*,set_limit(orders,10) FROM (SELECT ` + expression + ` AS value FROM ORDERS o WHERE o.ID>0) orders`
			got, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Orders", Source: &spec.ViewSource{SQL: SQL}}, SQL: SQL})
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(got.Source.SQL, expression) || !strings.Contains(got.Source.SQL, "WHERE o.ID>0") {
				t.Fatalf("database SQL changed: %s", got.Source.SQL)
			}
		})
	}
}
