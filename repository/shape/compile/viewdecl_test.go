package compile

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	dqldiag "github.com/viant/datly/repository/shape/dql/diag"
	dqlshape "github.com/viant/datly/repository/shape/dql/shape"
	"github.com/viant/datly/repository/shape/plan"
)

func TestViewDecl_ExtractSetBlocks(t *testing.T) {
	dql := "#set($_ = $Extra<?>(view/extra_view) /* SELECT id FROM EXTRA e */)\n" +
		"#define($_ = $Extra2<?>(view/extra_view_2) /* SELECT id FROM EXTRA2 e */)\n" +
		"SELECT id FROM ORDERS o"
	blocks := extractSetBlocks(dql)
	require.Len(t, blocks, 2)
	assert.Contains(t, blocks[0].Body, "$Extra")
	assert.Contains(t, blocks[1].Body, "$Extra2")
}

func TestViewDecl_ParseSetDeclarationBody(t *testing.T) {
	holder, kind, location, tail, ok := parseSetDeclarationBody("$_ = $Extra<?>(view/extra_view).WithURI('/x')")
	require.True(t, ok)
	assert.Equal(t, "Extra", holder)
	assert.Equal(t, "view", kind)
	assert.Equal(t, "extra_view", location)
	assert.Contains(t, tail, ".WithURI('/x')")
}

func TestViewDecl_ApplyOptions_InvalidCardinality(t *testing.T) {
	view := &declaredView{Name: "extra"}
	var diags []*dqlshape.Diagnostic
	applyDeclaredViewOptions(view, ".Cardinality('few')", "SELECT 1", 0, &diags)
	require.NotEmpty(t, diags)
	assert.Equal(t, dqldiag.CodeViewCardinality, diags[0].Code)
}

func TestViewDecl_AppendDeclaredViews(t *testing.T) {
	dql := "#set($_ = $Extra<?>(view/extra_view).WithURI('/x') /* SELECT code FROM EXTRA e */)"
	result := &plan.Result{
		ViewsByName: map[string]*plan.View{},
		ByPath:      map[string]*plan.Field{},
	}
	appendDeclaredViews(dql, result)
	require.NotEmpty(t, result.Views)
	found := false
	for _, item := range result.Views {
		if item != nil && item.SQLURI == "/x" {
			found = true
			break
		}
	}
	assert.True(t, found)
}

func TestViewDecl_ApplyOptions_Extended(t *testing.T) {
	view := &declaredView{Name: "limit"}
	var diags []*dqlshape.Diagnostic
	tail := ".WithTag('json:\"id\"').WithCodec(AsJSON,'x').WithHandler('Build',a,b)." +
		"WithStatusCode(422).WithErrorMessage('bad req').WithPredicate('ByID','id = ?', 101)." +
		"EnsurePredicate('Tenant','tenant_id = ?', 7).QuerySelector('qs').WithCache('c1').WithLimit(10)." +
		"Cacheable(true).When('x > 1').Scope('team').WithType('[]Order').Of('list').Value('abc').Async().Output()"
	applyDeclaredViewOptions(view, tail, "SELECT 1", 0, &diags)

	require.Empty(t, diags)
	assert.Equal(t, `json:"id"`, view.Tag)
	assert.Equal(t, "AsJSON", view.Codec)
	require.Len(t, view.CodecArgs, 1)
	assert.Equal(t, "'x'", view.CodecArgs[0])
	assert.Equal(t, "Build", view.HandlerName)
	require.Len(t, view.HandlerArgs, 2)
	assert.Equal(t, "a", view.HandlerArgs[0])
	assert.Equal(t, "b", view.HandlerArgs[1])
	require.NotNil(t, view.StatusCode)
	assert.Equal(t, 422, *view.StatusCode)
	assert.Equal(t, "bad req", view.ErrorMessage)
	require.Len(t, view.Predicates, 2)
	assert.Equal(t, "ByID", view.Predicates[0].Name)
	assert.False(t, view.Predicates[0].Ensure)
	assert.Equal(t, "Tenant", view.Predicates[1].Name)
	assert.True(t, view.Predicates[1].Ensure)
	assert.Equal(t, "qs", view.QuerySelector)
	assert.Equal(t, "c1", view.CacheRef)
	require.NotNil(t, view.Limit)
	assert.Equal(t, 10, *view.Limit)
	require.NotNil(t, view.Cacheable)
	assert.True(t, *view.Cacheable)
	assert.Equal(t, "x > 1", view.When)
	assert.Equal(t, "team", view.Scope)
	assert.Equal(t, "[]Order", view.DataType)
	assert.Equal(t, "list", view.Of)
	assert.Equal(t, "abc", view.Value)
	assert.True(t, view.Async)
	assert.True(t, view.Output)
}

func TestViewDecl_ApplyOptions_QuerySelectorValidation(t *testing.T) {
	view := &declaredView{Name: "customer_id"}
	var diags []*dqlshape.Diagnostic
	applyDeclaredViewOptions(view, ".QuerySelector('q')", "SELECT 1", 0, &diags)
	require.NotEmpty(t, diags)
	assert.Equal(t, dqldiag.CodeDeclQuerySelector, diags[0].Code)
}

func TestViewDecl_SplitArgs_Nested(t *testing.T) {
	args := splitArgs(`'a', fn(1,2), {'k': [1,2]}, "x,y"`)
	require.Len(t, args, 4)
	assert.Equal(t, "'a'", args[0])
	assert.Equal(t, "fn(1,2)", args[1])
	assert.Equal(t, "{'k': [1,2]}", args[2])
	assert.Equal(t, `"x,y"`, args[3])
}

func TestViewDecl_AppendDeclaredViews_ExtendedDeclarationMetadata(t *testing.T) {
	dql := "#set($_ = $limit<?>(view/limit).WithTag('json:\"id\"').WithCodec(AsJSON).WithHandler('Build',a)." +
		"WithStatusCode(409).WithErrorMessage('conflict').WithPredicate('ByID','id=?',1)." +
		"EnsurePredicate('Tenant','tenant=?',2).QuerySelector('items').WithCache('c1').WithLimit(5)." +
		"Cacheable(false).When('x').Scope('s').WithType('Order').Of('o').Value('v').Async().Output() /* SELECT id FROM EXTRA e */)"
	result := &plan.Result{
		ViewsByName: map[string]*plan.View{},
		ByPath:      map[string]*plan.Field{},
	}
	appendDeclaredViews(dql, result)
	require.NotEmpty(t, result.Views)
	var target *plan.View
	for _, item := range result.Views {
		if item != nil && item.Name == "e" {
			target = item
			break
		}
	}
	require.NotNil(t, target)
	require.NotNil(t, target.Declaration)
	assert.Equal(t, `json:"id"`, target.Declaration.Tag)
	assert.Equal(t, "AsJSON", target.Declaration.Codec)
	assert.Equal(t, "Build", target.Declaration.HandlerName)
	require.NotNil(t, target.Declaration.StatusCode)
	assert.Equal(t, 409, *target.Declaration.StatusCode)
	assert.Equal(t, "conflict", target.Declaration.ErrorMessage)
	assert.Equal(t, "items", target.Declaration.QuerySelector)
	assert.Equal(t, "c1", target.Declaration.CacheRef)
	require.NotNil(t, target.Declaration.Limit)
	assert.Equal(t, 5, *target.Declaration.Limit)
	require.NotNil(t, target.Declaration.Cacheable)
	assert.False(t, *target.Declaration.Cacheable)
	assert.Equal(t, "x", target.Declaration.When)
	assert.Equal(t, "s", target.Declaration.Scope)
	assert.Equal(t, "Order", target.Declaration.DataType)
	assert.Equal(t, "o", target.Declaration.Of)
	assert.Equal(t, "v", target.Declaration.Value)
	assert.True(t, target.Declaration.Async)
	assert.True(t, target.Declaration.Output)
	require.Len(t, target.Declaration.Predicates, 2)
}

func TestViewDecl_AppendDeclaredViews_AttachSummaryFromMetaViewSQL(t *testing.T) {
	root := &plan.View{Name: "Browser", Path: "Browser", Holder: "Browser"}
	result := &plan.Result{
		Views:       []*plan.View{root},
		ViewsByName: map[string]*plan.View{"Browser": root},
		ByPath:      map[string]*plan.Field{},
	}
	dql := "#set($_ = $Summary<?>(view/summary) /* SELECT COUNT(1) CNT FROM ($View.browser.SQL) t */)"

	appendDeclaredViews(dql, result)

	require.Len(t, result.Views, 1)
	require.NotNil(t, root)
	assert.Contains(t, root.Summary, "COUNT(1)")
	assert.Contains(t, root.Summary, "$View.browser.SQL")
}

func TestViewDecl_AppendDeclaredViews_MetaViewSQL_NoParentFallbackToView(t *testing.T) {
	result := &plan.Result{
		ViewsByName: map[string]*plan.View{},
		ByPath:      map[string]*plan.Field{},
	}
	dql := "#set($_ = $Summary<?>(view/summary) /* SELECT COUNT(1) CNT FROM ($View.browser.SQL) t */)"

	appendDeclaredViews(dql, result)

	require.Len(t, result.Views, 1)
	assert.Empty(t, result.Views[0].Summary)
	assert.NotEmpty(t, result.Views[0].Name)
}
