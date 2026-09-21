package dql

import (
	"context"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/viant/assertly"
	"github.com/viant/datly/spec"
	xcodec "github.com/viant/xdatly/codec"
)

type csvAsIntsCodec struct{}

func (csvAsIntsCodec) Value(_ context.Context, raw interface{}, _ ...xcodec.Option) (interface{}, error) {
	text, _ := raw.(string)
	if text == "" {
		return []int(nil), nil
	}
	items := strings.Split(text, ",")
	result := make([]int, 0, len(items))
	for _, item := range items {
		value, err := strconv.Atoi(strings.TrimSpace(item))
		if err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	return result, nil
}

type csvAsIntsFactory struct{}

func (csvAsIntsFactory) New(_ *xcodec.Config, _ ...xcodec.Option) (xcodec.Instance, error) {
	return csvAsIntsCodec{}, nil
}

func TestParseComponentSource(t *testing.T) {
	source := `#setting($_ = $route('/total', 'GET'))
SELECT 1`

	component, err := parseComponentSource("example.com/demo/forecasting", "Total", source)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	if len(component.Routes) != 1 {
		t.Fatalf("expected one route")
	}
	assertly.AssertValues(t, "/total", component.Routes[0].Path)
	assertly.AssertValues(t, "GET", component.Routes[0].Method)
	if component.RootView == nil || component.RootView.Source == nil {
		t.Fatalf("expected root view with source")
	}
	assertly.AssertValues(t, "SELECT 1", component.RootView.Source.SQL)
}

func TestParseComponentSource_WithAPIKeyDirective(t *testing.T) {
	source := `#setting($_ = $route('/secure', 'GET'))
#setting($_ = $api_key('X-Api-Key', 'secret'))
SELECT 1`

	component, err := parseComponentSource("example.com/demo/secure", "Secure", source)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	if len(component.Routes) != 1 {
		t.Fatalf("expected one route")
	}
	assertly.AssertValues(t, "X-Api-Key", component.Routes[0].APIKeyHeader)
	assertly.AssertValues(t, "secret", component.Routes[0].APIKeyValue)
}

func TestParseComponentSource_NormalizesRouteMethods(t *testing.T) {
	source := `#setting($_ = $route('/v1/items', 'get', 'POST', 'GET'))
SELECT 1`
	component, err := parseComponentSource("example.com/demo/items", "Items", source)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if len(component.Routes) != 2 || component.Routes[0].Method != "GET" || component.Routes[1].Method != "POST" {
		t.Fatalf("routes = %#v", component.Routes)
	}
}

func TestParseComponentSource_RejectsInvalidRoute(t *testing.T) {
	tests := []string{
		`#setting($_ = $route('items', 'GET'))` + "\nSELECT 1",
		`#setting($_ = $route('/items', 'GOT'))` + "\nSELECT 1",
		`#setting($_ = $route(/items, 'GET'))` + "\nSELECT 1",
	}
	for _, source := range tests {
		if _, err := parseComponentSource("example.com/demo/items", "Items", source); err == nil {
			t.Fatalf("expected invalid route error for %q", source)
		}
	}
}

func TestParseComponentSource_WithBindingErrorMetadataOnParam(t *testing.T) {
	source := `#setting($_ = $route('/auth', 'GET'))
#define($_ = $Authorization<string>(header/Authorization).Required().WithStatusCode(401).WithErrorMessage('unauthorized: ${error}'))
SELECT 1`

	component, err := parseComponentSource("example.com/demo/auth", "Auth", source)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	if len(component.Parameters) != 1 {
		t.Fatalf("expected one param")
	}
	assertly.AssertValues(t, 401, component.Parameters[0].ErrorStatusCode)
	assertly.AssertValues(t, "unauthorized: ${error}", component.Parameters[0].ErrorMessage)
}

func TestParseComponentSource_QuerySelectorMetadata(t *testing.T) {
	source := `#setting($_ = $route('/users', 'GET'))
#define($_ = $Fields<[]string>(query/fields).Optional().QuerySelector('users'))
#define($_ = $Page<int>(query/page).Optional().QuerySelector('accounts'))
SELECT id FROM users`

	component, err := parseComponentSource("example.com/demo/users", "Users", source)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	assertly.AssertValues(t, &spec.QuerySelectorBinding{View: "users", Property: spec.SelectorPropertyFields}, component.Parameters[0].QuerySelector)
	assertly.AssertValues(t, &spec.QuerySelectorBinding{View: "accounts", Property: spec.SelectorPropertyPage}, component.Parameters[1].QuerySelector)
	if component.Parameters[0].Cacheable == nil || *component.Parameters[0].Cacheable || component.Parameters[1].Cacheable == nil || *component.Parameters[1].Cacheable {
		t.Fatalf("query selectors must disable value caching unless explicitly overridden: %+v", component.Parameters)
	}
}

func TestParseComponentSource_AppliesOriginalRequiredDefaults(t *testing.T) {
	source := `#setting($_ = $route('/required-defaults', 'POST'))
#define($_ = $Search<string>(query/search))
#define($_ = $Authorization<string>(header/Authorization))
#define($_ = $Payload<string>(body/))
SELECT 1`
	component, err := parseComponentSource("example.com/demo/defaults", "Defaults", source)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	if len(component.Parameters) != 3 || component.Parameters[0].Required == nil || *component.Parameters[0].Required {
		t.Fatalf("query parameter must default to optional: %+v", component.Parameters)
	}
	if component.Parameters[1].Required == nil || !*component.Parameters[1].Required {
		t.Fatalf("header parameter must default to required: %+v", component.Parameters[1])
	}
	if component.Parameters[2].Required != nil {
		t.Fatalf("body parameter must retain provider/default policy: %+v", component.Parameters[2])
	}
}

func TestParseComponentSource_RejectsInvalidQuerySelector(t *testing.T) {
	testCases := []struct {
		name        string
		declaration string
		errorPart   string
	}{
		{name: "unsupported property", declaration: `$Tenant<string>(query/tenant).QuerySelector('users')`, errorPart: "cannot be used"},
		{name: "missing view", declaration: `$Limit<int>(query/limit).QuerySelector()`, errorPart: "exactly one view"},
		{name: "duplicate", declaration: `$Limit<int>(query/limit).QuerySelector('users').QuerySelector('accounts')`, errorPart: "duplicate"},
		{name: "missing option paren", declaration: `$Limit<int>(query/limit).QuerySelector 'users'`, errorPart: "expected '('"},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			source := "#setting($_ = $route('/users', 'GET'))\n#define($_ = " + testCase.declaration + ")\nSELECT 1"
			_, err := parseComponentSource("example.com/demo/users", "Users", source)
			if err == nil || !strings.Contains(err.Error(), testCase.errorPart) {
				t.Fatalf("expected error containing %q, got %v", testCase.errorPart, err)
			}
		})
	}
}

func TestParseQuerySelector_RejectsUnterminatedArgumentGroup(t *testing.T) {
	_, err := (&declarationOptionParser{paramName: "Limit", tail: `.QuerySelector('users'`}).parse()
	if err == nil || !strings.Contains(err.Error(), "unterminated argument group") {
		t.Fatalf("expected unterminated QuerySelector error, got %v", err)
	}
}

func TestParseComponentSource_WithStructuredCacheWarmup(t *testing.T) {
	source := `#setting($_ = $route('/warmup', 'GET'))
#setting($_ = $cache('aerospike').WithTimeToLiveMs(60000))
#setting($_ = $cache_warmup('order_id', 'Connector=bq_metrics_prewarm', 'IndexParameter=OrderId', 'Period=today,yesterday', 'Granularity=hour,day'))
#define($_ = $OrderId<string>(query/orderId))
#define($_ = $Period<string>(query/period))
#define($_ = $Granularity<string>(query/granularity))
SELECT 1`

	component, err := parseComponentSource("example.com/demo/warmup", "Warmup", source)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	if component.Settings == nil || component.Settings.Cache == nil || component.Settings.Cache.Warmup == nil {
		t.Fatalf("expected cache warmup settings")
	}
	warmup := component.Settings.Cache.Warmup
	assertly.AssertValues(t, "order_id", warmup.IndexColumn)
	assertly.AssertValues(t, "OrderId", warmup.IndexParameter)
	assertly.AssertValues(t, "bq_metrics_prewarm", warmup.Connector)
	if len(warmup.Cases) != 1 || len(warmup.Cases[0].Set) != 2 {
		t.Fatalf("expected one warmup case with two params")
	}
}

func TestParseComponentSource_WithBoundedCacheWarmup(t *testing.T) {
	component, err := parseComponentSource("example.com/cache", "records", `#package('example.com/cache')
#setting($_ = $route('/records','GET'))
#setting($_ = $cache('records','5m').WithLocation('/tmp/records'))
#setting($_ = $cache_warmup('tenant_id','IndexParameter=TenantID','MaxCases=30','Limit=100','FieldNames=id,name','Period=today,yesterday'))
SELECT 1`)
	if err != nil {
		t.Fatal(err)
	}
	warmup := component.Settings.Cache.Warmup
	if warmup == nil || warmup.MaxCases == nil || *warmup.MaxCases != 30 || warmup.Limit == nil || *warmup.Limit != 100 || !reflect.DeepEqual(warmup.FieldNames, []string{"id", "name"}) || len(warmup.Cases) != 1 {
		t.Fatalf("warmup=%+v", warmup)
	}
}

func TestParseComponentSource_MergesRepeatedStructuredCacheWarmup(t *testing.T) {
	source := `#setting($_ = $route('/warmup', 'GET'))
#setting($_ = $cache('aerospike').WithTimeToLiveMs(60000))
#setting($_ = $cache_warmup('order_id', 'Connector=bq_metrics_prewarm', 'IndexParameter=OrderId', 'Period=today,yesterday'))
#setting($_ = $cache_warmup('order_id', 'IndexParameter=OrderId', 'Period=lastweek'))
#define($_ = $OrderId<string>(query/orderId))
#define($_ = $Period<string>(query/period))
SELECT 1`

	component, err := parseComponentSource("example.com/demo/warmup", "Warmup", source)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	warmup := component.Settings.Cache.Warmup
	if warmup == nil {
		t.Fatalf("expected cache warmup settings")
	}
	assertly.AssertValues(t, "order_id", warmup.IndexColumn)
	assertly.AssertValues(t, "OrderId", warmup.IndexParameter)
	assertly.AssertValues(t, "bq_metrics_prewarm", warmup.Connector)
	if len(warmup.Cases) != 2 {
		t.Fatalf("expected two warmup cases, got %d", len(warmup.Cases))
	}
}

func TestParseComponentSource_WithWarmupIndexMeta(t *testing.T) {
	source := `#setting($_ = $route('/warmup', 'GET'))
#setting($_ = $cache('aerospike').WithTimeToLiveMs(60000))
#setting($_ = $cache_warmup('order_id', 'IndexMeta=true'))
SELECT 1`

	component, err := parseComponentSource("example.com/demo/warmup", "Warmup", source)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	if component.Settings == nil || component.Settings.Cache == nil || component.Settings.Cache.Warmup == nil {
		t.Fatalf("expected cache warmup settings")
	}
	assertly.AssertValues(t, true, component.Settings.Cache.Warmup.IndexMeta)
}

func TestParseComponentSource_RejectsDuplicateCacheWarmupIdentity(t *testing.T) {
	// Different index columns sharing one index parameter are additive plural
	// warmups, but their effective identities collide and fail compilation.
	source := `#setting($_ = $route('/warmup', 'GET'))
#setting($_ = $cache('aerospike').WithTimeToLiveMs(60000))
#setting($_ = $cache_warmup('order_id', 'IndexParameter=OrderId'))
#setting($_ = $cache_warmup('campaign_id', 'IndexParameter=OrderId'))
SELECT 1`

	_, err := parseComponentSource("example.com/demo/warmup", "Warmup", source)
	if err == nil {
		t.Fatalf("expected duplicate warmup identity error")
	}
	if !strings.Contains(err.Error(), "duplicate warmup name") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestParseComponentSource_AdditivePluralCacheWarmup(t *testing.T) {
	source := `#setting($_ = $route('/warmup', 'GET'))
#setting($_ = $cache('aerospike').WithTimeToLiveMs(60000))
#setting($_ = $cache_warmup('advertiser_id', 'IndexParameter=AdvertiserID', 'Period=today,yesterday'))
#setting($_ = $cache_warmup('campaign_id', 'IndexParameter=CampaignID', 'Priority=2', 'Connector=prewarm', 'Period=today'))
#define($_ = $AdvertiserID<int>(query/advertiserId))
#define($_ = $CampaignID<int>(query/campaignId))
#define($_ = $Period<string>(query/period))
SELECT 1`

	component, err := parseComponentSource("example.com/demo/warmup", "Warmup", source)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	cache := component.Settings.Cache
	if cache == nil || cache.Warmup == nil || len(cache.Warmups) != 1 {
		t.Fatalf("expected singular plus one plural warmup, got %+v", cache)
	}
	assertly.AssertValues(t, "advertiser_id", cache.Warmup.IndexColumn)
	assertly.AssertValues(t, "AdvertiserID", cache.Warmup.IndexParameter)
	assertly.AssertValues(t, 0, cache.Warmup.Priority)
	if len(cache.Warmup.Cases) != 1 || len(cache.Warmup.Cases[0].Set[0].Values) != 2 {
		t.Fatalf("singular warmup lost its own cases: %+v", cache.Warmup.Cases)
	}
	plural := cache.Warmups[0]
	assertly.AssertValues(t, "campaign_id", plural.IndexColumn)
	assertly.AssertValues(t, "CampaignID", plural.IndexParameter)
	assertly.AssertValues(t, 2, plural.Priority)
	assertly.AssertValues(t, "prewarm", plural.Connector)
	if len(plural.Cases) != 1 || len(plural.Cases[0].Set[0].Values) != 1 {
		t.Fatalf("plural warmup lost its own cases: %+v", plural.Cases)
	}
	effective, err := cache.EffectiveWarmups()
	if err != nil || len(effective) != 2 {
		t.Fatalf("effective warmups=%d err=%v", len(effective), err)
	}
}

func TestParseComponentSource_SharedCacheWarmupCases(t *testing.T) {
	source := `#setting($_ = $route('/warmup', 'GET'))
#setting($_ = $cache('aerospike').WithTimeToLiveMs(60000))
#setting($_ = $cache_warmup_cases('recent', 'Period=today,yesterday'))
#setting($_ = $cache_warmup_cases('broad', 'Period=week,month'))
#setting($_ = $cache_warmup('advertiser_id', 'IndexParameter=AdvertiserID', 'CaseRefs=recent,broad'))
#setting($_ = $cache_warmup('campaign_id', 'IndexParameter=CampaignID', 'Name=campaign', 'CaseRefs=recent', 'Period=lastweek'))
#define($_ = $AdvertiserID<int>(query/advertiserId))
#define($_ = $CampaignID<int>(query/campaignId))
#define($_ = $Period<string>(query/period))
SELECT 1`

	component, err := parseComponentSource("example.com/demo/warmup", "Warmup", source)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	cache := component.Settings.Cache
	if cache == nil || len(cache.SharedCases) != 2 {
		t.Fatalf("expected two shared case sets, got %+v", cache)
	}
	effective, err := cache.EffectiveWarmups()
	if err != nil || len(effective) != 2 {
		t.Fatalf("effective warmups=%d err=%v", len(effective), err)
	}
	// Referenced cases expand ahead of inline cases, independently per warmup.
	if len(effective[0].Cases) != 2 {
		t.Fatalf("advertiser cases=%+v", effective[0].Cases)
	}
	if len(effective[1].Cases) != 2 || effective[1].Cases[1].Set[0].Values[0] != "lastweek" {
		t.Fatalf("campaign cases=%+v", effective[1].Cases)
	}
	assertly.AssertValues(t, "campaign", effective[1].EffectiveName())
	// Raw metadata keeps unexpanded CaseRefs so both runtimes normalize identically.
	if len(cache.Warmup.CaseRefs) != 2 || len(cache.Warmups[0].CaseRefs) != 1 {
		t.Fatalf("caseRefs were not preserved: %+v %+v", cache.Warmup.CaseRefs, cache.Warmups[0].CaseRefs)
	}
}

func TestParseComponentSource_DQLDirectives(t *testing.T) {
	source := `#setting($_ = $route('/v1/api/example/vendors/{vendorID}', 'GET'))
#package('example.com/demo/vendors')
#import('pkg','example.com/demo/vendors/pkg')
#settings($_ = $meta('docs/orders.md'))
#setting($_ = $connector('analytics'))
#setting($_ = $useTemplate('patch'))
#setting($_ = $report('VendorInput','Dims','Metrics','Predicates','Sort','Take','Skip',false))
#setting($_ = $cache(true, '5m'))
#settings($_ = $mcp('orders.search', 'Search orders', 'docs/mcp/orders.md'))
#setting($_ = $dest('vendor.go'))
#setting($_ = $input_dest('vendor_input.go'))
#setting($_ = $output_dest('vendor_output.go'))
#setting($_ = $router_dest('vendor_router.go'))
#setting($_ = $input_type('VendorInput'))
#setting($_ = $output_type('VendorOutput'))
#settings($_ = $marshal('application/json','pkg.OrderJSON'))
#settings($_ = $unmarshal('application/json','pkg.OrderIn'))
#settings($_ = $unmarshal('application/xml','pkg.OrderXMLIn'))
#settings($_ = $format('tabular_json'))
#settings($_ = $date_format('2006-01-02'))
#settings($_ = $case_format('lc'))
#setting($_ = $const('Vendor','VENDOR'))
#define($_ = $VendorID<int>(path/vendorID))
#define($_ = $Name<string>(query/name).WithPredicate(0, 'contains', 'v', 'NAME').Optional())
SELECT * FROM VENDOR WHERE ID = $VendorID`

	component, err := parseComponentSource("example.com/demo/vendors", "Vendor", source)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	if len(component.Routes) != 1 {
		t.Fatalf("expected one route")
	}
	if component.Settings == nil {
		t.Fatalf("expected component settings")
	}
	if component.TypeContext == nil {
		t.Fatalf("expected type context")
	}
	assertly.AssertValues(t, "example.com/demo/vendors", component.TypeContext.DefaultPackage)
	assertly.AssertValues(t, []spec.ImportSpec{{Alias: "pkg", Package: "example.com/demo/vendors/pkg"}}, component.TypeContext.Imports)
	assertly.AssertValues(t, "patch", component.Settings.Generation.Template)
	assertly.AssertValues(t, "analytics", component.Settings.DefaultConnector)
	assertly.AssertValues(t, "docs/orders.md", component.Settings.Generation.DescriptionResource)
	if component.Settings.Report == nil {
		t.Fatalf("expected report settings")
	}
	if !component.Settings.Report.Enabled {
		t.Fatal("expected report to be enabled")
	}
	if component.Settings.Report.MCPTool == nil || *component.Settings.Report.MCPTool {
		t.Fatal("expected report MCP tool exposure to be disabled")
	}
	if component.Settings.Cache == nil {
		t.Fatalf("expected cache settings")
	}
	if len(component.Routes[0].MCP) != 1 {
		t.Fatalf("expected route MCP exposure")
	}
	assertly.AssertValues(t, "VendorInput", component.Settings.Report.LinkedInputType)
	assertly.AssertValues(t, "Dims", component.Settings.Report.InputLayout.Dimensions)
	assertly.AssertValues(t, "Metrics", component.Settings.Report.InputLayout.Measures)
	assertly.AssertValues(t, "Predicates", component.Settings.Report.InputLayout.Filters)
	assertly.AssertValues(t, "Sort", component.Settings.Report.InputLayout.OrderBy)
	assertly.AssertValues(t, "Take", component.Settings.Report.InputLayout.Limit)
	assertly.AssertValues(t, "Skip", component.Settings.Report.InputLayout.Offset)
	assertly.AssertValues(t, true, component.Settings.Cache.Enabled)
	assertly.AssertValues(t, "5m", component.Settings.Cache.TTL)
	assertly.AssertValues(t, spec.MCPExposureTool, component.Routes[0].MCP[0].Kind)
	assertly.AssertValues(t, "orders.search", component.Routes[0].MCP[0].Name)
	assertly.AssertValues(t, "Search orders", component.Routes[0].MCP[0].Description)
	assertly.AssertValues(t, "docs/mcp/orders.md", component.Routes[0].MCP[0].DescriptionPath)
	assertly.AssertValues(t, "vendor.go", component.Settings.Generation.ViewFile)
	assertly.AssertValues(t, "vendor_input.go", component.Settings.Generation.InputFile)
	assertly.AssertValues(t, "vendor_output.go", component.Settings.Generation.OutputFile)
	assertly.AssertValues(t, "vendor_router.go", component.Settings.Generation.RouterFile)
	assertly.AssertValues(t, "VendorInput", component.Settings.InputType)
	assertly.AssertValues(t, "VendorOutput", component.Settings.OutputType)
	assertly.AssertValues(t, "pkg.OrderJSON", component.Settings.JSONMarshalType)
	assertly.AssertValues(t, "pkg.OrderIn", component.Settings.JSONUnmarshalType)
	assertly.AssertValues(t, "pkg.OrderXMLIn", component.Settings.XMLUnmarshalType)
	assertly.AssertValues(t, "tabular", component.Settings.Format)
	assertly.AssertValues(t, "2006-01-02", component.Settings.DateFormat)
	assertly.AssertValues(t, "lc", component.Settings.CaseFormat)
	assertly.AssertValues(t, map[string]string{"Vendor": "VENDOR"}, component.Settings.Const)
	assertly.AssertValues(t, "/v1/api/example/vendors/{vendorID}", component.Routes[0].Path)
	assertly.AssertValues(t, "GET", component.Routes[0].Method)
	if len(component.Parameters) != 2 {
		t.Fatalf("expected two params, got %d", len(component.Parameters))
	}
	assertly.AssertValues(t, "VendorID", component.Parameters[0].Name)
	assertly.AssertValues(t, "define", string(component.Parameters[0].Declaration))
	assertly.AssertValues(t, "path", component.Parameters[0].Source.Kind)
	assertly.AssertValues(t, "vendorID", component.Parameters[0].Source.Name)
	assertly.AssertValues(t, "int", component.Parameters[0].TypeExpr)
	assertly.AssertValues(t, "Name", component.Parameters[1].Name)
	assertly.AssertValues(t, "define", string(component.Parameters[1].Declaration))
	assertly.AssertValues(t, "query", component.Parameters[1].Source.Kind)
	assertly.AssertValues(t, "name", component.Parameters[1].Source.Name)
	assertly.AssertValues(t, "string", component.Parameters[1].TypeExpr)
	if len(component.Parameters[1].Predicates) != 1 {
		t.Fatalf("expected one predicate, got %d", len(component.Parameters[1].Predicates))
	}
	assertly.AssertValues(t, 0, component.Parameters[1].Predicates[0].Group)
	assertly.AssertValues(t, "contains", component.Parameters[1].Predicates[0].Name)
	assertly.AssertValues(t, []string{"v", "NAME"}, component.Parameters[1].Predicates[0].Args)
	if component.RootView == nil || component.RootView.Source == nil {
		t.Fatalf("expected root view with source")
	}
	assertly.AssertValues(t, "SELECT * FROM VENDOR WHERE ID = $VendorID", component.RootView.Source.SQL)
}

func TestParseComponentSource_StripsTypeContextDirectivesFromSQLBody(t *testing.T) {
	source := `#setting($_ = $route('/v1/api/example/items', 'GET'))
#package('example.com/demo/items')
#import('models','example.com/demo/items/models')
#define($_ = $ItemID<int>(query/itemID))
SELECT * FROM ITEM WHERE ID = :ItemID`

	component, err := parseComponentSource("example.com/demo/items", "Item", source)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	if component.RootView == nil || component.RootView.Source == nil {
		t.Fatalf("expected root view with source")
	}
	assertly.AssertValues(t, "SELECT * FROM ITEM WHERE ID = :ItemID", component.RootView.Source.SQL)
}

func TestParseComponentSource_ApplyWhenAbsentPredicate(t *testing.T) {
	source := `#setting($_ = $route('/v1/api/example/auth', 'GET'))
#define($_ = $Auth<string>(header/Authorization).ApplyWhenAbsentPredicate('Tenant', 'tenant_id = ?', '42'))
SELECT 1`

	component, err := parseComponentSource("example.com/demo/auth", "Auth", source)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	if len(component.Parameters) != 1 {
		t.Fatalf("expected one param, got %d", len(component.Parameters))
	}
	if len(component.Parameters[0].Predicates) != 1 {
		t.Fatalf("expected one predicate, got %d", len(component.Parameters[0].Predicates))
	}
	assertly.AssertValues(t, true, component.Parameters[0].Predicates[0].ApplyWhenAbsent)
	assertly.AssertValues(t, "Tenant", component.Parameters[0].Predicates[0].Name)
	assertly.AssertValues(t, []string{"tenant_id = ?", "42"}, component.Parameters[0].Predicates[0].Args)
}

func TestParseComponentSource_RejectsEnsurePredicate(t *testing.T) {
	source := `#setting($_ = $route('/v1/api/example/auth', 'GET'))
#define($_ = $Auth<string>(header/Authorization).EnsurePredicate('Tenant', 'tenant_id = ?', '42'))
SELECT 1`

	_, err := parseComponentSource("example.com/demo/auth", "Auth", source)
	if err == nil || !strings.Contains(err.Error(), "unsupported declaration option EnsurePredicate") {
		t.Fatalf("expected obsolete EnsurePredicate to fail, got %v", err)
	}
}

func TestParseComponentSource_WithCodec(t *testing.T) {
	source := `#setting($_ = $route('/v1/api/example/codec', 'GET'))
#define($_ = $Ids<string>(query/ids).WithCodec('AsInts'))
#define($_ = $Claims<string>(header/Authorization).WithCodec('JwtClaim', 'sub', 'fallback').Optional())
SELECT 1`

	component, err := parseComponentSource("example.com/demo/codec", "Codec", source)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	if len(component.Parameters) != 2 {
		t.Fatalf("expected two params, got %d", len(component.Parameters))
	}
	assertly.AssertValues(t, "AsInts", component.Parameters[0].Codec.Body)
	assertly.AssertValues(t, []string(nil), component.Parameters[0].Codec.Args)
	assertly.AssertValues(t, "JwtClaim", component.Parameters[1].Codec.Body)
	assertly.AssertValues(t, []string{"sub", "fallback"}, component.Parameters[1].Codec.Args)
	assertly.AssertValues(t, false, *component.Parameters[1].Required)
}

func TestParseComponentSource_WithCodecCoexistsWithPredicateAndBareName(t *testing.T) {
	source := `#setting($_ = $route('/v1/api/example/codec-order', 'GET'))
#define($_ = $Ids<string,[]int>(query/ids).WithPredicate(0, 'in', 't', 'ID').withcodec(AsInts).Optional())
SELECT 1`

	component, err := parseComponentSource("example.com/demo/codec", "CodecOrder", source)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	if len(component.Parameters) != 1 {
		t.Fatalf("expected one param, got %d", len(component.Parameters))
	}
	assertly.AssertValues(t, "[]int", component.Parameters[0].OutputTypeExpr)
	if len(component.Parameters[0].Predicates) != 1 {
		t.Fatalf("expected one predicate, got %d", len(component.Parameters[0].Predicates))
	}
	assertly.AssertValues(t, "AsInts", component.Parameters[0].Codec.Body)
	assertly.AssertValues(t, false, *component.Parameters[0].Required)
}

func TestParseComponentSource_RejectsEmptyCodecArgs(t *testing.T) {
	source := `#setting($_ = $route('/v1/api/example/codec-empty', 'GET'))
#define($_ = $Ids<string>(query/ids).WithCodec())
SELECT 1`

	_, err := parseComponentSource("example.com/demo/codec", "CodecEmpty", source)
	if err == nil || !strings.Contains(err.Error(), "expects at least 1 argument") {
		t.Fatalf("expected invalid codec error, got %v", err)
	}
}

func TestParseComponentSource_PreservesDeclarationOptionSurface(t *testing.T) {
	source := `#setting($_ = $route('/v1/api/example/options', 'GET'))
#define($_ = $Fields<string>(query/fields).WithURI('assets:fields.sql').WithTag('json:"fields,omitempty"').Optional().Cacheable(false).QuerySelector('users').WithPredicate(2, 'contains', 'u', 'name').ApplyWhenAbsentPredicate('tenant', 'tenant_id = 7').When('enabled').Scope('request').Of('Filter').WithType('[]string').WithCodec('CSV', 'trim').WithStatusCode(422).WithErrorMessage('bad fields').WithDescription('Selected fields').WithExample('id,name').Value('id,name').Cardinality('Many').Async())
SELECT 1`

	component, err := parseComponentSource("example.com/demo/options", "Options", source)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	if len(component.Parameters) != 1 {
		t.Fatalf("expected one param, got %d", len(component.Parameters))
	}
	param := component.Parameters[0]
	if param.TypeExpr != "[]string" || param.Tag != `json:"fields,omitempty"` || param.Cardinality != "Many" {
		t.Fatalf("unexpected type/tag/cardinality: %+v", param)
	}
	if param.Required == nil || *param.Required || param.Cacheable == nil || *param.Cacheable {
		t.Fatalf("unexpected required/cacheable: required=%v cacheable=%v", param.Required, param.Cacheable)
	}
	if param.ResourceRef != "assets:fields.sql" || param.Activation != nil || param.When != "enabled" || param.Scope != "request" || param.With != "Filter" || param.Value == nil || *param.Value != "id,name" || !param.Async {
		t.Fatalf("unexpected binding metadata: %+v", param)
	}
	if param.ErrorStatusCode != 422 || param.ErrorMessage != "bad fields" {
		t.Fatalf("unexpected error metadata: %+v", param)
	}
	if param.Description != "Selected fields" || param.Example != "id,name" {
		t.Fatalf("unexpected documentation metadata: %+v", param)
	}
	if param.Codec == nil || param.Codec.Body != "CSV" || len(param.Codec.Args) != 1 || param.Codec.Args[0] != "trim" {
		t.Fatalf("unexpected codec: %+v", param.Codec)
	}
	if param.QuerySelector == nil || param.QuerySelector.View != "users" || len(param.Predicates) != 2 || !param.Predicates[1].ApplyWhenAbsent {
		t.Fatalf("unexpected selector/predicates: selector=%+v predicates=%+v", param.QuerySelector, param.Predicates)
	}
}

func TestParseComponentSource_ClassifiesAbsoluteWithURIAsRouteActivation(t *testing.T) {
	source := `#setting($_ = $route('/items/{id}', 'GET'))
#define($_ = $Fields<string>(query/fields).WithURI('/items/{id}'))
SELECT 1`
	component, err := parseComponentSource("example.com/demo/items", "Items", source)
	if err != nil {
		t.Fatal(err)
	}
	var fields *spec.Parameter
	for _, param := range component.Parameters {
		if param != nil && param.Name == "Fields" {
			fields = param
			break
		}
	}
	if fields == nil || fields.Activation == nil || fields.Activation.URI != "/items/{id}" || fields.ResourceRef != "" {
		t.Fatalf("Fields activation = %+v", fields)
	}
}

func TestParseComponentSource_PreservesIndependentViewOptions(t *testing.T) {
	source := `#import('model','example.com/model')
#setting($_ = $route('/v1/api/example/view-options', 'GET'))
#define($_ = $Authorization<*AuthorizationRow>(view/authorization).WithURI('queries/authorization.sql').Connector('analytics').Cardinality('One').Type('AuthorizationRow').Dest('authorization.go').WithCache('authorization-cache').WithLimit(3).ColumnType('Authorized','bool').ColumnTag('Authorized','internal:"true"').ColumnGroupable('Authorized',false).ColumnType('Created','time.Time').ColumnType('Owner','model.Owner') /* SELECT authorized, created, owner FROM authorization */)
SELECT 1`

	component, err := parseComponentSource("example.com/demo/options", "Options", source)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	if len(component.Views) != 1 {
		t.Fatalf("independent views = %+v", component.Views)
	}
	view := component.Views[0]
	if view.Name != "Authorization" || view.TypeName != "AuthorizationRow" || view.Dest != "authorization.go" || view.Cardinality != spec.CardinalityOne {
		t.Fatalf("view generation metadata = %+v", view)
	}
	if view.Source == nil || view.Source.URI != "queries/authorization.sql" || len(view.Source.Embeds) != 0 ||
		view.Source.Bindings == nil || view.Source.Bindings.Connector != "analytics" || view.Source.Bindings.CacheName != "authorization-cache" ||
		view.Source.Controls == nil || view.Source.Controls.Limit == nil || *view.Source.Controls.Limit != 3 {
		t.Fatalf("view source metadata = %+v", view.Source)
	}
	if len(view.Columns) != 3 {
		t.Fatalf("view columns = %+v", view.Columns)
	}
	authorized, created, owner := view.Columns[0], view.Columns[1], view.Columns[2]
	if authorized.Name != "Authorized" || authorized.Type.Name != "bool" || authorized.Tag != `internal:"true"` || authorized.Groupable == nil || *authorized.Groupable {
		t.Fatalf("authorized column = %+v", authorized)
	}
	if created.Type.Package != "time" || created.Type.Name != "Time" || owner.Type.Package != "example.com/model" || owner.Type.Name != "Owner" {
		t.Fatalf("qualified column types = created:%+v owner:%+v", created.Type, owner.Type)
	}
}

func TestParseComponentSource_DefineViewOptionsReplaceEquivalentSet(t *testing.T) {
	source := `#setting($_ = $route('/v1/api/example/view-precedence', 'GET'))
#set($_ = $Audit<[]AuditRow>(view/audit).Connector('stale').WithLimit(9) /* SELECT id FROM stale_audit */)
#define($_ = $Audit<[]AuditRow>(view/audit).Connector('current').WithLimit(1) /* SELECT id FROM audit */)
SELECT 1`

	component, err := parseComponentSource("example.com/demo/audit", "AuditLookup", source)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	if len(component.Parameters) != 1 || component.Parameters[0].Declaration != spec.DeclarationKindDefine ||
		component.Parameters[0].DeclarationSQL != "SELECT id FROM audit" || len(component.Views) != 1 {
		t.Fatalf("define precedence = params:%+v views:%+v", component.Parameters, component.Views)
	}
	view := component.Views[0]
	if view.Source == nil || view.Source.SQL != "SELECT id FROM audit" || view.Source.Bindings == nil ||
		view.Source.Bindings.Connector != "current" || view.Source.Controls == nil ||
		view.Source.Controls.Limit == nil || *view.Source.Controls.Limit != 1 {
		t.Fatalf("effective view = %+v", view)
	}
}

func TestPrepareSourceRejectsInvalidIndependentViewOptionsAtOptionSpan(t *testing.T) {
	tests := []struct {
		name    string
		kind    string
		option  string
		span    string
		message string
	}{
		{name: "view option on query", kind: "query/id", option: ".Connector('db')", span: ".Connector", message: "cannot use view option"},
		{name: "negative limit", kind: "view/items", option: ".WithLimit(-1)", span: ".WithLimit", message: "non-negative integer"},
		{name: "unknown type alias", kind: "view/items", option: ".ColumnType('ID','missing.ID')", span: ".ColumnType", message: "is not declared with #import"},
		{name: "duplicate column facet", kind: "view/items", option: ".ColumnTag('ID','json:\"id\"').WithColumnTag('id','sqlx:\"id\"')", span: ".WithColumnTag", message: "duplicate"},
		{name: "unsupported per-view handler", kind: "view/items", option: ".WithHandler('Build')", span: ".WithHandler", message: "unsupported declaration option"},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			source := "#setting($_ = $route('/items', 'GET'))\n#define($_ = $ID<int>(" + testCase.kind + ")" + testCase.option + " /* SELECT id FROM items */)\nSELECT 1"
			prepared := PrepareSource(source)
			if err := prepared.Err(); err == nil || !strings.Contains(err.Error(), testCase.message) {
				t.Fatalf("PrepareSource() error = %v", err)
			}
			want := strings.Index(source, testCase.span)
			if len(prepared.Diagnostics) != 1 || prepared.Diagnostics[0].Offset != want {
				t.Fatalf("diagnostics = %+v, want offset %d", prepared.Diagnostics, want)
			}
		})
	}
}

func TestParseComponentSource_DoesNotTreatCommentMarkersInOptionValuesAsDeclarationSQL(t *testing.T) {
	source := `#setting($_ = $route('/v1/api/example/options', 'GET'))
#define($_ = $ID<string>(query/id).WithErrorMessage('bad /* marker */ value'))
SELECT 1`

	component, err := parseComponentSource("example.com/demo/options", "Options", source)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	if len(component.Parameters) != 1 {
		t.Fatalf("expected one param, got %d", len(component.Parameters))
	}
	param := component.Parameters[0]
	if param.ErrorMessage != "bad /* marker */ value" || param.DeclarationSQL != "" {
		t.Fatalf("unexpected declaration metadata: %+v", param)
	}
}

func TestPrepareSourceRejectsInvalidDeclarationOptionsAtOptionSpan(t *testing.T) {
	tests := []struct {
		name    string
		option  string
		span    string
		message string
	}{
		{name: "unknown", option: ".UnknownFlag()", message: "unsupported declaration option"},
		{name: "invalid bool", option: ".Cacheable('x')", message: "requires true or false"},
		{name: "empty codec", option: ".WithCodec()", message: "expects at least 1 argument"},
		{name: "invalid cardinality", option: ".Cardinality('some')", message: "supports only One or Many"},
		{name: "async args", option: ".Async(true)", message: "expects at most 0 argument"},
		{name: "invalid status", option: ".WithStatusCode(42)", message: "HTTP status code"},
		{name: "empty type", option: ".WithType()", message: "expects at least 1 argument"},
		{name: "duplicate required", option: ".Optional().Required()", span: ".Required()", message: "duplicate Required"},
		{name: "trailing syntax", option: ".Optional() garbage", span: "garbage", message: "unsupported declaration syntax"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			source := "#setting($_ = $route('/options', 'GET'))\n#define($_ = $Fields<string>(query/fields)" + test.option + ")\nSELECT 1"
			prepared := PrepareSource(source)
			if err := prepared.Err(); err == nil || !strings.Contains(err.Error(), test.message) {
				t.Fatalf("PrepareSource() error = %v", err)
			}
			span := test.span
			if span == "" {
				span = test.option
			}
			if len(prepared.Diagnostics) != 1 || prepared.Diagnostics[0].Offset != strings.Index(source, span) {
				t.Fatalf("diagnostic = %+v, want option offset %d", prepared.Diagnostics, strings.Index(source, span))
			}
		})
	}
}

func TestPrepareSourceRejectsInvalidDefineDeclarationShape(t *testing.T) {
	for _, declaration := range []string{
		`#define($_ = $1ID<int>(query/id))`,
		`#define($_ = $_ID<int>(query/id))`,
	} {
		prepared := PrepareSource(declaration + "\nSELECT 1")
		if err := prepared.Err(); err == nil || !strings.Contains(err.Error(), "invalid #define declaration") {
			t.Fatalf("PrepareSource(%q) error = %v", declaration, err)
		}
		if len(prepared.Diagnostics) != 1 || prepared.Diagnostics[0].Offset != 0 {
			t.Fatalf("diagnostics = %+v", prepared.Diagnostics)
		}
	}
}

func TestParseComponentSource_SetIsNotDeclaration(t *testing.T) {
	source := `#setting($_ = $route('/v1/api/example/items', 'GET'))
#define($_ = $ItemID<int>(query/itemID))
#set($criteria = $ItemID)
SELECT * FROM ITEM WHERE ID = $criteria`

	component, err := parseComponentSource("example.com/demo/items", "Item", source)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	if len(component.Parameters) != 1 {
		t.Fatalf("expected one declared param, got %d", len(component.Parameters))
	}
	assertly.AssertValues(t, "ItemID", component.Parameters[0].Name)
	assertly.AssertValues(t, "#set($criteria = $ItemID)\nSELECT * FROM ITEM WHERE ID = $criteria", component.RootView.Source.SQL)
}

func TestParseComponentSource_SetDeclaration_IsParsedAsParam(t *testing.T) {
	source := `#setting($_ = $route('/v1/api/example/jobs', 'GET'))
#set($_ = $Module<string>(query/module).WithPredicate(0,'in','t','Module'))
#set($_ = $Summary<?>(output/summary) /* SELECT 1 AS summary_value */)
#set($_ = $Status<?>(output/status))
#set($_ = $Data<?>(output/view))
SELECT * FROM JOBS WHERE 1=1`

	component, err := parseComponentSource("example.com/demo/jobs", "Jobs", source)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	if len(component.Parameters) != 4 {
		t.Fatalf("expected four declared params, got %d", len(component.Parameters))
	}
	assertly.AssertValues(t, "set", string(component.Parameters[0].Declaration))
	assertly.AssertValues(t, "Module", component.Parameters[0].Name)
	assertly.AssertValues(t, "query", component.Parameters[0].Source.Kind)
	assertly.AssertValues(t, "module", component.Parameters[0].Source.Name)
	assertly.AssertValues(t, "string", component.Parameters[0].TypeExpr)
	if len(component.Parameters[0].Predicates) != 1 {
		t.Fatalf("expected one predicate, got %d", len(component.Parameters[0].Predicates))
	}
	assertly.AssertValues(t, "in", component.Parameters[0].Predicates[0].Name)
	assertly.AssertValues(t, []string{"t", "Module"}, component.Parameters[0].Predicates[0].Args)
	assertly.AssertValues(t, "Summary", component.Parameters[1].Name)
	assertly.AssertValues(t, "output", component.Parameters[1].Source.Kind)
	assertly.AssertValues(t, "summary", component.Parameters[1].Source.Name)
	assertly.AssertValues(t, "Status", component.Parameters[2].Name)
	assertly.AssertValues(t, "output", component.Parameters[2].Source.Kind)
	assertly.AssertValues(t, "status", component.Parameters[2].Source.Name)
	assertly.AssertValues(t, "Data", component.Parameters[3].Name)
	assertly.AssertValues(t, "output", component.Parameters[3].Source.Kind)
	assertly.AssertValues(t, "view", component.Parameters[3].Source.Name)
	assertly.AssertValues(t, "SELECT * FROM JOBS WHERE 1=1", component.RootView.Source.SQL)
	assertly.AssertValues(t, "SELECT 1 AS summary_value", component.Parameters[1].DeclarationSQL)
}

func TestParseComponentSource_PreservesOutputSummaryDeclarationSQL(t *testing.T) {
	source := `#setting($_ = $route('/v1/api/users/summary', 'GET'))
#setting($_ = $output_type('UsersSummaryOutput'))
#define($_ = $ID<int>(path/id))
#set($_ = $Summary<SummaryView>(output/summary) /* SELECT COUNT(*) AS total_accounts FROM accounts WHERE user_id >= :ID */)
SELECT id, name FROM users WHERE id >= :ID`

	component, err := parseComponentSource("example.com/demo/users", "UsersSummary", source)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if len(component.Parameters) != 2 {
		t.Fatalf("expected input and output declarations, got %d", len(component.Parameters))
	}
	assertly.AssertValues(t, "Summary", component.Parameters[1].Name)
	assertly.AssertValues(t, "output", component.Parameters[1].Source.Kind)
	assertly.AssertValues(t, "summary", component.Parameters[1].Source.Name)
	assertly.AssertValues(t, "SELECT COUNT(*) AS total_accounts FROM accounts WHERE user_id >= :ID", component.Parameters[1].DeclarationSQL)
}

func TestParseComponentSource_AllowsBodyInputAndOutputDeclarationsWithSameName(t *testing.T) {
	source := `#setting($_ = $route('/v1/api/example/events', 'POST'))
#set($_ = $Events<Event>(body/).Tag('anonymous:"true"').Required())
#set($_ = $Events<Event>(body/).Tag('anonymous:"true"').Output())
SELECT 1`

	component, err := parseComponentSource("example.com/demo/events", "Events", source)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	if len(component.Parameters) != 2 {
		t.Fatalf("expected two params, got %d", len(component.Parameters))
	}
	assertly.AssertValues(t, "Events", component.Parameters[0].Name)
	assertly.AssertValues(t, false, component.Parameters[0].EmitOutput)
	assertly.AssertValues(t, "Events", component.Parameters[1].Name)
	assertly.AssertValues(t, true, component.Parameters[1].EmitOutput)
}

func TestParseComponentSource_MixedSetDeclarationAndVeltySet(t *testing.T) {
	source := `#setting($_ = $route('/v1/api/example/items', 'GET'))
#set($_ = $ItemID<int>(query/itemID))
#set($criteria = $ItemID)
SELECT * FROM ITEM WHERE ID = $criteria`

	component, err := parseComponentSource("example.com/demo/items", "Item", source)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	if len(component.Parameters) != 1 {
		t.Fatalf("expected one declared param, got %d", len(component.Parameters))
	}
	assertly.AssertValues(t, "set", string(component.Parameters[0].Declaration))
	assertly.AssertValues(t, "ItemID", component.Parameters[0].Name)
	assertly.AssertValues(t, "#set($criteria = $ItemID)\nSELECT * FROM ITEM WHERE ID = $criteria", component.RootView.Source.SQL)
}

func TestParseComponentSource_PrefersDefineOverEquivalentSetDeclaration(t *testing.T) {
	source := `#setting($_ = $route('/v1/api/example/prefer-define', 'POST'))
#set($_ = $User<LegacyUser>(body/).Required())
#define($_ = $User<PreferredUser>(body/).Required())
SELECT 1`

	component, err := parseComponentSource("example.com/demo/items", "PreferDefine", source)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	if len(component.Parameters) != 1 {
		t.Fatalf("expected one declared param, got %d", len(component.Parameters))
	}
	assertly.AssertValues(t, "User", component.Parameters[0].Name)
	assertly.AssertValues(t, "define", string(component.Parameters[0].Declaration))
	assertly.AssertValues(t, "PreferredUser", component.Parameters[0].TypeExpr)
}

func TestParseComponentSource_ExtractsEmbeddedSQLRefs(t *testing.T) {
	source := `#setting($_ = $route('/v1/api/example/top-sites', 'GET'))
SELECT use_connector(site, 'ci_ads')
FROM (
    ${embed:sql/event.sql}
) eventv2
JOIN (
    ${embed:components:sql/site.sql}
) site ON site.ID = eventv2.siteId`

	component, err := parseComponentSource("example.com/demo/top-sites", "TopSites", source)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	if component.RootView == nil || component.RootView.Source == nil {
		t.Fatalf("expected root view with source")
	}
	if len(component.RootView.Source.Embeds) != 2 {
		t.Fatalf("expected two embedded SQL refs, got %d", len(component.RootView.Source.Embeds))
	}
	assertly.AssertValues(t, "sql/event.sql", component.RootView.Source.Embeds[0].Path)
	assertly.AssertValues(t, "${embed:sql/event.sql}", component.RootView.Source.Embeds[0].Raw)
	assertly.AssertValues(t, "components:sql/site.sql", component.RootView.Source.Embeds[1].Path)
	assertly.AssertValues(t, "${embed:components:sql/site.sql}", component.RootView.Source.Embeds[1].Raw)
}

func TestParseComponentSource_NoTemplateSettingLeavesSettingsNil(t *testing.T) {
	source := `#setting($_ = $route('/v1/api/example/plain', 'GET'))
SELECT 1`

	component, err := parseComponentSource("example.com/demo/plain", "Plain", source)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	if component.Settings != nil {
		t.Fatalf("expected nil settings when no useTemplate directive is present")
	}
}

func TestParseComponentSource_RejectsEmptyGenerationSettings(t *testing.T) {
	testCases := []struct {
		name      string
		directive string
		message   string
	}{
		{name: "template", directive: `$useTemplate(' ')`, message: "empty template type"},
		{name: "description resource", directive: `$meta(' ')`, message: "empty path"},
		{name: "view file", directive: `$dest(' ')`, message: "empty destination"},
		{name: "input file", directive: `$input_dest(' ')`, message: "empty destination"},
		{name: "output file", directive: `$output_dest(' ')`, message: "empty destination"},
		{name: "router file", directive: `$router_dest(' ')`, message: "empty destination"},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			source := "#setting($_ = " + testCase.directive + ")\nSELECT 1"
			component, err := parseComponentSource("example.com/demo/invalid", "Invalid", source)
			if err == nil || component != nil || !strings.Contains(err.Error(), testCase.message) {
				t.Fatalf("parseComponentSource() component=%+v error=%v", component, err)
			}
		})
	}
}

func TestParseComponentSource_CubeDirectiveAlias(t *testing.T) {
	source := `#setting($_ = $route('/v1/api/example/cube', 'GET'))
#setting($_ = $cube('CubeInput','Dims','Metrics','Predicates','Sort','Take','Skip'))
SELECT 1`

	component, err := parseComponentSource("example.com/demo/cube", "Cube", source)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	if component.Settings == nil || component.Settings.Report == nil {
		t.Fatalf("expected report settings from cube directive")
	}
	assertly.AssertValues(t, "CubeInput", component.Settings.Report.LinkedInputType)
	assertly.AssertValues(t, "Dims", component.Settings.Report.InputLayout.Dimensions)
	assertly.AssertValues(t, "Metrics", component.Settings.Report.InputLayout.Measures)
	assertly.AssertValues(t, "Predicates", component.Settings.Report.InputLayout.Filters)
	assertly.AssertValues(t, "Sort", component.Settings.Report.InputLayout.OrderBy)
	assertly.AssertValues(t, "Take", component.Settings.Report.InputLayout.Limit)
	assertly.AssertValues(t, "Skip", component.Settings.Report.InputLayout.Offset)
}

func TestParseComponentSource_CacheProviderDirective(t *testing.T) {
	source := `#setting($_ = $route('/v1/api/example/cache', 'GET'))
#setting($_ = $cache('aerospike').WithProvider('aerospike://127.0.0.1:3000/test').WithLocation('${view.Name}').WithTimeToLiveMs(3600000))
SELECT 1`

	component, err := parseComponentSource("example.com/demo/cache", "Cache", source)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	if component.Settings == nil || component.Settings.Cache == nil {
		t.Fatalf("expected cache settings")
	}
	assertly.AssertValues(t, true, component.Settings.Cache.Enabled)
	assertly.AssertValues(t, "aerospike", component.Settings.Cache.Name)
	assertly.AssertValues(t, "aerospike://127.0.0.1:3000/test", component.Settings.Cache.Provider)
	assertly.AssertValues(t, "${view.Name}", component.Settings.Cache.Location)
	assertly.AssertValues(t, 3600000, component.Settings.Cache.TimeToLiveMs)
}

func TestParseComponentSource_CapturesDeclarationCommentSQL(t *testing.T) {
	// Comprehensive-many shape: a body collection plus param/ helper and view/
	// current-state declarations carrying /* ... */ SQL bodies. The SQL must be
	// preserved on the declared params (contract capture only, not executed).
	source := "#setting($_ = $route('/v1/api/example/events-many-declsql', 'POST'))\n" +
		"#define($_ = $Events<[]Event>(body/Data).Required())\n" +
		"#define($_ = $CurEventsId<?>(param/Events) /*\n? SELECT ARRAY_AGG(Id) AS Values FROM EVENTS LIMIT 1\n*/\n)\n" +
		"#define($_ = $CurEvents<[]*Event>(view/CurEvents) /*\n? SELECT * FROM EVENTS WHERE $criteria.In(\"ID\", $CurEventsId.Values)\n*/\n)\n" +
		"SELECT 1"

	component, err := parseComponentSource("example.com/demo/events", "EventsMany", source)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}

	byName := map[string]*spec.Parameter{}
	for _, param := range component.Parameters {
		byName[param.Name] = param
	}

	events := byName["Events"]
	if events == nil {
		t.Fatalf("expected Events body param, got %+v", component.Parameters)
	}
	assertly.AssertValues(t, "", events.DeclarationSQL)

	curEventsId := byName["CurEventsId"]
	if curEventsId == nil {
		t.Fatalf("expected CurEventsId param helper, got %+v", component.Parameters)
	}
	assertly.AssertValues(t, "param", curEventsId.Source.Kind)
	if !strings.Contains(curEventsId.DeclarationSQL, "SELECT ARRAY_AGG(Id) AS Values FROM EVENTS LIMIT 1") {
		t.Fatalf("expected helper declaration SQL preserved, got %q", curEventsId.DeclarationSQL)
	}

	curEvents := byName["CurEvents"]
	if curEvents == nil {
		t.Fatalf("expected CurEvents current-state view, got %+v", component.Parameters)
	}
	assertly.AssertValues(t, "view", curEvents.Source.Kind)
	if !strings.Contains(curEvents.DeclarationSQL, "SELECT * FROM EVENTS WHERE $criteria.In(\"ID\", $CurEventsId.Values)") {
		t.Fatalf("expected current-state declaration SQL preserved, got %q", curEvents.DeclarationSQL)
	}

	// Declaration comment SQL must not leak into the executable root SQL body.
	assertly.AssertValues(t, "SELECT 1", component.RootView.Source.SQL)
}

func TestParseComponentSource_NormalizesStructQLParamCodec(t *testing.T) {
	source := "#setting($_ = $route('/events', 'PATCH'))\n" +
		"#define($_ = $Events<[]Event>(body/Data).Cardinality('Many').Required())\n" +
		"#define($_ = $CurEventsId<?>(param/Events) /*\n" +
		"? SELECT ARRAY_AGG(Id) AS Values FROM `/` LIMIT 1\n" +
		"*/)\nSELECT 1"
	component, err := parseComponentSource("example.com/events", "Events", source)
	if err != nil {
		t.Fatal(err)
	}
	var helper *spec.Parameter
	for _, param := range component.Parameters {
		if param != nil && param.Name == "CurEventsId" {
			helper = param
			break
		}
	}
	if helper == nil || helper.Codec == nil || helper.Codec.Body != "structql" || len(helper.Codec.Args) != 1 ||
		helper.Codec.Args[0] != "SELECT ARRAY_AGG(Id) AS Values FROM `/` LIMIT 1" {
		t.Fatalf("StructQL helper = %+v", helper)
	}
}

func TestParseComponentSource_PreservesMalformedParamSQLForCompilerDiagnostics(t *testing.T) {
	component, err := parseComponentSource("example.com/events", "Events", `#setting($_ = $route('/events', 'PATCH'))
#define($_ = $Broken<?>(param/Events) /* {not-json} SELECT 1 */)
SELECT 1`)
	if err != nil {
		t.Fatal(err)
	}
	if len(component.Parameters) != 1 || component.Parameters[0].DeclarationSQL != "{not-json} SELECT 1" || component.Parameters[0].Codec != nil {
		t.Fatalf("malformed declaration metadata = %+v", component.Parameters)
	}
}

func TestParseComponentSource_CapturesDeclarationCommentSQLForSetCompatibilityForm(t *testing.T) {
	source := "#setting($_ = $route('/v1/api/example/foos-declsql-set', 'POST'))\n" +
		"#set($_ = $CurFoosId<?>(param/Foos) /* ? SELECT id FROM FOOS */)\n" +
		"SELECT 1"

	component, err := parseComponentSource("example.com/demo/foos", "FoosDeclSet", source)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	if len(component.Parameters) != 1 {
		t.Fatalf("expected one declared param, got %d", len(component.Parameters))
	}
	param := component.Parameters[0]
	assertly.AssertValues(t, "set", string(param.Declaration))
	assertly.AssertValues(t, "CurFoosId", param.Name)
	assertly.AssertValues(t, "? SELECT id FROM FOOS", param.DeclarationSQL)
}

func TestParseComponentSource_ImplicitSQLDeclarationBecomesViewState(t *testing.T) {
	source := "#setting($_ = $route('/v1/api/example/differ', 'PUT'))\n" +
		"#define($_ = $CurFoosId<?>(param/Foos) /* ? SELECT ARRAY_AGG(Id) AS Values FROM / LIMIT 1 */)\n" +
		"#set($_ = $FoosDBRecords /* ? SELECT * FROM FOOS WHERE $criteria.In(\"ID\", $CurFoosId.Values) */)\n" +
		"SELECT 1"

	component, err := parseComponentSource("example.com/demo/foos", "FoosDiffer", source)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	if len(component.Parameters) != 2 {
		t.Fatalf("expected two declared params, got %d", len(component.Parameters))
	}
	byName := map[string]*spec.Parameter{}
	for _, param := range component.Parameters {
		byName[param.Name] = param
	}
	implicit := byName["FoosDBRecords"]
	if implicit == nil {
		t.Fatalf("expected implicit SQL declaration to be preserved, got %+v", component.Parameters)
	}
	assertly.AssertValues(t, "set", string(implicit.Declaration))
	assertly.AssertValues(t, "view", implicit.Source.Kind)
	assertly.AssertValues(t, "FoosDBRecords", implicit.Source.Name)
	if !strings.Contains(implicit.DeclarationSQL, "SELECT * FROM FOOS WHERE $criteria.In(\"ID\", $CurFoosId.Values)") {
		t.Fatalf("expected implicit declaration SQL preserved, got %q", implicit.DeclarationSQL)
	}
}

func TestParseComponentSource_ImplicitSQLDeclarationWithViewOptionsMaterializesIndependentView(t *testing.T) {
	source := "#setting($_ = $route('/v1/api/example/implicit-view', 'GET'))\n" +
		"#define($_ = $Inventory.Connector('analytics').WithLimit(2) /* SELECT id FROM inventory */)\n" +
		"SELECT 1"

	prepared := PrepareSource(source)
	if err := prepared.Err(); err != nil {
		t.Fatalf("unexpected prepare error: %v", err)
	}
	if len(prepared.Directives.Views) != 1 {
		t.Fatalf("normalized views = %+v", prepared.Directives.Views)
	}
	normalized := prepared.Directives.Views[0]
	if normalized.Name != "Inventory" || normalized.Source == nil || normalized.Source.Bindings == nil ||
		normalized.Source.Bindings.Connector != "analytics" || normalized.Source.Controls == nil ||
		normalized.Source.Controls.Limit == nil || *normalized.Source.Controls.Limit != 2 {
		t.Fatalf("normalized implicit view = %+v", normalized)
	}

	component, err := ParsePreparedComponentSource("example.com/demo/inventory", "InventoryLookup", prepared)
	if err != nil {
		t.Fatalf("unexpected component error: %v", err)
	}
	if len(component.Views) != 1 || component.Views[0] == normalized {
		t.Fatalf("component views must be detached from normalized directives: %+v", component.Views)
	}
	component.Views[0].Source.Bindings.Connector = "changed"
	if normalized.Source.Bindings.Connector != "analytics" {
		t.Fatalf("component mutation leaked into normalized view: %+v", normalized)
	}
}

func TestPrepareSourceRejectsImplicitParameterWithViewOptionsAtOptionSpan(t *testing.T) {
	source := "#setting($_ = $route('/v1/api/example/implicit-param', 'GET'))\n" +
		"#define($_ = $Inventory.Connector('analytics') /* SELECT id FROM `/inventory` */)\n" +
		"SELECT 1"

	prepared := PrepareSource(source)
	if len(prepared.Diagnostics) != 1 {
		t.Fatalf("diagnostics = %+v", prepared.Diagnostics)
	}
	diagnostic := prepared.Diagnostics[0]
	expectedOffset := strings.Index(source, ".Connector")
	if diagnostic.Offset != expectedOffset || !strings.Contains(diagnostic.Message, "cannot use view options for inferred param declaration") {
		t.Fatalf("diagnostic = %+v, expected offset %d", diagnostic, expectedOffset)
	}
}
