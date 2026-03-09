package compile

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/repository/shape/plan"
)

func TestExtractViewHints_WithQuotedConnector(t *testing.T) {
	dql := "SELECT use_connector(match, 'bq_sitemgmt_match'), use_connector(site, \"ci_ads\"), allow_nulls(match), groupable(match), set_limit(match, 0)"
	hints := extractViewHints(dql)
	require.Len(t, hints, 2)
	assert.Equal(t, "bq_sitemgmt_match", hints["match"].Connector)
	assert.Equal(t, "ci_ads", hints["site"].Connector)
	require.NotNil(t, hints["match"].AllowNulls)
	assert.True(t, *hints["match"].AllowNulls)
	require.NotNil(t, hints["match"].Groupable)
	assert.True(t, *hints["match"].Groupable)
	require.NotNil(t, hints["match"].NoLimit)
	assert.True(t, *hints["match"].NoLimit)
}

func TestExtractViewHints_AllowedOrderByColumns(t *testing.T) {
	dql := "SELECT allowed_order_by_columns(vendor, 'accountId:ACCOUNT_ID,vendor.userCreated:USER_CREATED,totalId:TOTAL_ID')"
	hints := extractViewHints(dql)
	require.Contains(t, hints, "vendor")
	require.NotNil(t, hints["vendor"].SelectorOrderBy)
	assert.True(t, *hints["vendor"].SelectorOrderBy)
	assert.Equal(t, "ACCOUNT_ID", hints["vendor"].SelectorOrderByNames["accountId"])
	assert.Equal(t, "ACCOUNT_ID", hints["vendor"].SelectorOrderByNames["accountid"])
	assert.Equal(t, "USER_CREATED", hints["vendor"].SelectorOrderByNames["vendor.userCreated"])
	assert.Equal(t, "USER_CREATED", hints["vendor"].SelectorOrderByNames["vendor.usercreated"])
	assert.Equal(t, "USER_CREATED", hints["vendor"].SelectorOrderByNames["userCreated"])
	assert.Equal(t, "TOTAL_ID", hints["vendor"].SelectorOrderByNames["totalId"])
	assert.Equal(t, "TOTAL_ID", hints["vendor"].SelectorOrderByNames["totalid"])
}

func TestExtractViewHints_MixedCaseAndUnquotedConnector(t *testing.T) {
	dql := "SELECT USE_CONNECTOR(match, ci_ads), Allow_Nulls(match), set_limit(match, -1)"
	hints := extractViewHints(dql)
	require.Contains(t, hints, "match")
	assert.Equal(t, "ci_ads", hints["match"].Connector)
	require.NotNil(t, hints["match"].AllowNulls)
	assert.True(t, *hints["match"].AllowNulls)
	require.NotNil(t, hints["match"].NoLimit)
	assert.False(t, *hints["match"].NoLimit)
}

func TestExtractViewHints_DestAndType(t *testing.T) {
	dql := "SELECT dest(vendor,'vendor.go'), type(vendor,'Vendor'), dest(products,'vendor.go'), type(products,'Products') FROM VENDOR vendor"
	hints := extractViewHints(dql)
	require.Contains(t, hints, "vendor")
	require.Contains(t, hints, "products")
	assert.Equal(t, "vendor.go", hints["vendor"].Dest)
	assert.Equal(t, "Vendor", hints["vendor"].TypeName)
	assert.Equal(t, "vendor.go", hints["products"].Dest)
	assert.Equal(t, "Products", hints["products"].TypeName)
}

func TestExtractViewHints_Cardinality(t *testing.T) {
	dql := "SELECT cardinality(products_meta, 'one'), cardinality(products, 'many')"
	hints := extractViewHints(dql)
	require.Contains(t, hints, "products_meta")
	require.Contains(t, hints, "products")
	assert.Equal(t, "one", hints["products_meta"].Cardinality)
	assert.Equal(t, "many", hints["products"].Cardinality)
}

func TestApplyViewHints_Metadata(t *testing.T) {
	trueValue := true
	result := &plan.Result{
		Views: []*plan.View{
			{Name: "match", Table: "MATCH", Cardinality: "many"},
		},
	}
	applyViewHints(result, map[string]viewHint{
		"match": {
			Connector:       "ci_ads",
			AllowNulls:      &trueValue,
			Groupable:       &trueValue,
			NoLimit:         &trueValue,
			Cardinality:     "one",
			Dest:            "match.go",
			TypeName:        "Match",
			SelectorOrderBy: &trueValue,
			SelectorOrderByNames: map[string]string{
				"accountId": "ACCOUNT_ID",
			},
		},
	})
	require.Len(t, result.Views, 1)
	assert.Equal(t, "ci_ads", result.Views[0].Connector)
	require.NotNil(t, result.Views[0].AllowNulls)
	assert.True(t, *result.Views[0].AllowNulls)
	require.NotNil(t, result.Views[0].Groupable)
	assert.True(t, *result.Views[0].Groupable)
	require.NotNil(t, result.Views[0].SelectorNoLimit)
	assert.True(t, *result.Views[0].SelectorNoLimit)
	require.NotNil(t, result.Views[0].SelectorOrderBy)
	assert.True(t, *result.Views[0].SelectorOrderBy)
	assert.Equal(t, "ACCOUNT_ID", result.Views[0].SelectorOrderByColumns["accountId"])
	assert.Equal(t, "one", strings.ToLower(result.Views[0].Cardinality))
	require.NotNil(t, result.Views[0].Declaration)
	assert.Equal(t, "match.go", result.Views[0].Declaration.Dest)
	assert.Equal(t, "Match", result.Views[0].Declaration.TypeName)
}

func TestApplyViewHints_MetadataCaseInsensitiveAlias(t *testing.T) {
	trueValue := true
	result := &plan.Result{
		Views: []*plan.View{
			{Name: "User", Holder: "User", Table: "USER"},
		},
	}
	applyViewHints(result, map[string]viewHint{
		"user": {
			Self:       &plan.SelfReference{Holder: "Team", Child: "ID", Parent: "MGR_ID"},
			AllowNulls: &trueValue,
		},
	})
	require.Len(t, result.Views, 1)
	require.NotNil(t, result.Views[0].Self)
	assert.Equal(t, "Team", result.Views[0].Self.Holder)
	assert.Equal(t, "ID", result.Views[0].Self.Child)
	assert.Equal(t, "MGR_ID", result.Views[0].Self.Parent)
}

func TestStripProjectionHintCalls_RemovesSelfRefFromSQL(t *testing.T) {
	sqlText := "SELECT user.* EXCEPT MGR_ID, self_ref(user, 'Team', 'ID', 'MGR_ID'), cardinality(user, 'one'), groupable(user), allowed_order_by_columns(user, 'id:ID') FROM (SELECT t.* FROM USER t) user"
	actual := stripProjectionHintCalls(sqlText)
	assert.NotContains(t, strings.ToLower(actual), "self_ref(")
	assert.NotContains(t, strings.ToLower(actual), "cardinality(")
	assert.NotContains(t, strings.ToLower(actual), "groupable(")
	assert.NotContains(t, strings.ToLower(actual), "allowed_order_by_columns(")
	assert.Contains(t, strings.ToLower(actual), "user.* except mgr_id")
}

func TestAppendRelationViews_SQLSelection(t *testing.T) {
	testCases := []struct {
		name             string
		rawDQL           string
		relationTable    string
		expectContains   string
		expectNotContain string
	}{
		{
			name: "prefers raw join subquery SQL when available",
			rawDQL: `
SELECT wrapper.*,
       vendor.*
FROM (SELECT ID FROM VENDOR WHERE ID = $vendorID) wrapper
JOIN (SELECT * FROM VENDOR t WHERE t.ID = $criteria.AppendBinding($Unsafe.vendorID)) vendor ON vendor.ID = wrapper.ID`,
			relationTable:    "(SELECT * FROM VENDOR t WHERE t.ID = 1)",
			expectContains:   "$criteria.AppendBinding($Unsafe.vendorID)",
			expectNotContain: "t.ID = 1",
		},
		{
			name: "falls back to relation table SQL when raw join SQL missing",
			rawDQL: `
SELECT wrapper.*
FROM (SELECT ID FROM VENDOR WHERE ID = $vendorID) wrapper`,
			relationTable:  "(SELECT * FROM VENDOR t WHERE t.ID = 1)",
			expectContains: "t.ID = 1",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			result := &plan.Result{
				ViewsByName: map[string]*plan.View{},
			}
			root := &plan.View{
				Relations: []*plan.Relation{
					{
						Name:  "vendor",
						Ref:   "vendor",
						Table: testCase.relationTable,
						On: []*plan.RelationLink{
							{Expression: "vendor.ID = wrapper.ID"},
						},
					},
				},
			}

			appendRelationViews(result, root, nil, testCase.rawDQL)
			require.Len(t, result.Views, 1)
			assert.Contains(t, result.Views[0].SQL, testCase.expectContains)
			if testCase.expectNotContain != "" {
				assert.NotContains(t, result.Views[0].SQL, testCase.expectNotContain)
			}
		})
	}
}

func TestAppendRelationViews_ComplexTreeAnyLevel(t *testing.T) {
	rawDQL := `
SELECT wrapper.*,
       vendor.*,
       products.*,
       reviews.*
FROM (SELECT ID FROM VENDOR WHERE ID = $vendorID) wrapper
JOIN (SELECT * FROM VENDOR t WHERE t.ID = $criteria.AppendBinding($Unsafe.vendorID)) vendor ON vendor.ID = wrapper.ID
JOIN (SELECT * FROM PRODUCT p WHERE p.VENDOR_ID = $criteria.AppendBinding($Unsafe.vendorID)) products ON products.VENDOR_ID = vendor.ID
JOIN (SELECT * FROM REVIEW r WHERE r.PRODUCT_ID = products.ID) reviews ON reviews.PRODUCT_ID = products.ID`

	result := &plan.Result{
		ViewsByName: map[string]*plan.View{},
	}
	root := &plan.View{
		Relations: []*plan.Relation{
			{
				Name:   "vendor",
				Ref:    "vendor",
				Parent: "wrapper",
				Table:  "(SELECT * FROM VENDOR t WHERE t.ID = 1)",
				On: []*plan.RelationLink{
					{Expression: "vendor.ID = wrapper.ID"},
				},
			},
			{
				Name:   "products",
				Ref:    "products",
				Parent: "vendor",
				Table:  "(SELECT * FROM PRODUCT p WHERE p.VENDOR_ID = 1)",
				On: []*plan.RelationLink{
					{Expression: "products.VENDOR_ID = vendor.ID"},
				},
			},
			{
				Name:   "reviews",
				Ref:    "reviews",
				Parent: "products",
				Table:  "(SELECT * FROM REVIEW r WHERE r.PRODUCT_ID = products.ID)",
				On: []*plan.RelationLink{
					{Expression: "reviews.PRODUCT_ID = products.ID"},
				},
			},
		},
	}

	appendRelationViews(result, root, nil, rawDQL)
	require.Len(t, result.Views, 3)
	require.Contains(t, result.ViewsByName, "vendor")
	require.Contains(t, result.ViewsByName, "products")
	require.Contains(t, result.ViewsByName, "reviews")
	assert.Contains(t, result.ViewsByName["vendor"].SQL, "$criteria.AppendBinding($Unsafe.vendorID)")
	assert.Contains(t, result.ViewsByName["products"].SQL, "$criteria.AppendBinding($Unsafe.vendorID)")
	assert.Contains(t, result.ViewsByName["reviews"].SQL, "r.PRODUCT_ID = products.ID")
}

func TestAppendRelationViews_ComplexTreeCrossLevelJoin(t *testing.T) {
	rawDQL := `
SELECT wrapper.*,
       vendor.*,
       products.*,
       stats.*
FROM (SELECT ID FROM VENDOR WHERE ID = $vendorID) wrapper
JOIN (SELECT * FROM VENDOR t WHERE t.ID = $criteria.AppendBinding($Unsafe.vendorID)) vendor ON vendor.ID = wrapper.ID
JOIN (SELECT * FROM PRODUCT p WHERE p.VENDOR_ID = vendor.ID) products ON products.VENDOR_ID = vendor.ID
JOIN (SELECT COUNT(1) AS CNT, v.ID AS VENDOR_ID FROM VENDOR v WHERE v.ID = wrapper.ID) stats ON stats.VENDOR_ID = wrapper.ID`

	result := &plan.Result{
		ViewsByName: map[string]*plan.View{},
	}
	root := &plan.View{
		Relations: []*plan.Relation{
			{
				Name:   "vendor",
				Ref:    "vendor",
				Parent: "wrapper",
				Table:  "(SELECT * FROM VENDOR t WHERE t.ID = 1)",
				On: []*plan.RelationLink{
					{Expression: "vendor.ID = wrapper.ID"},
				},
			},
			{
				Name:   "products",
				Ref:    "products",
				Parent: "vendor",
				Table:  "(SELECT * FROM PRODUCT p WHERE p.VENDOR_ID = vendor.ID)",
				On: []*plan.RelationLink{
					{Expression: "products.VENDOR_ID = vendor.ID"},
				},
			},
			{
				Name:   "stats",
				Ref:    "stats",
				Parent: "products",
				Table:  "(SELECT COUNT(1) AS CNT, v.ID AS VENDOR_ID FROM VENDOR v WHERE v.ID = wrapper.ID)",
				On: []*plan.RelationLink{
					{Expression: "stats.VENDOR_ID = wrapper.ID"},
				},
			},
		},
	}

	appendRelationViews(result, root, nil, rawDQL)
	require.Len(t, result.Views, 3)
	require.Contains(t, result.ViewsByName, "stats")
	assert.Contains(t, result.ViewsByName["stats"].SQL, "v.ID = wrapper.ID")
}
