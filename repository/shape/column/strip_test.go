package column

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/viant/datly/view"
)

func TestStripTemplateVariables(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		expect string
	}{
		{
			name:   "no templates",
			input:  "SELECT * FROM VENDOR WHERE ID = 1",
			expect: "SELECT * FROM VENDOR WHERE ID = 1",
		},
		{
			name:   "simple variable",
			input:  "SELECT * FROM VENDOR WHERE ID = $vendorID",
			expect: "SELECT * FROM VENDOR WHERE ID = ''",
		},
		{
			name:   "variable with dot method",
			input:  "SELECT * FROM VENDOR WHERE ID IN ($Unsafe.vendorIDs)",
			expect: "SELECT * FROM VENDOR WHERE ID IN ('')",
		},
		{
			name:   "variable with method call",
			input:  "SELECT * FROM PRODUCT WHERE 1=1 $View.ParentJoinOn(\"AND\",\"VENDOR_ID\")",
			expect: "SELECT * FROM PRODUCT WHERE 1=1 ''",
		},
		{
			name:   "criteria binding",
			input:  "SELECT * FROM VENDOR t WHERE t.ID IN ($criteria.AppendBinding($Unsafe.vendorIDs))",
			expect: "SELECT * FROM VENDOR t WHERE t.ID IN ('')",
		},
		{
			name:   "expression in braces",
			input:  "SELECT * FROM VENDOR WHERE ${predicate.Build(\"AND\")}",
			expect: "SELECT * FROM VENDOR WHERE ''",
		},
		{
			name:   "if directive",
			input:  "SELECT * FROM PRODUCT WHERE 1=1 #if($vendorID < 0) AND 1=2 #end",
			expect: "SELECT * FROM PRODUCT WHERE 1=1  ",
		},
		{
			name:   "foreach directive",
			input:  "#foreach($item in $items) INSERT INTO T VALUES($item.ID) #end",
			expect: " ",
		},
		{
			name:   "set directive with parens",
			input:  "#set($x = 1)\nSELECT * FROM T",
			expect: " \nSELECT * FROM T",
		},
		{
			name:   "mixed templates and SQL",
			input:  "SELECT vendor.*, products.* FROM (SELECT * FROM VENDOR t) vendor JOIN (SELECT * FROM PRODUCT t WHERE 1=1 ${predicate.Builder().CombineOr($predicate.FilterGroup(0, \"AND\")).Build(\"AND\")}) products ON products.VENDOR_ID = vendor.ID",
			expect: "SELECT vendor.*, products.* FROM (SELECT * FROM VENDOR t) vendor JOIN (SELECT * FROM PRODUCT t WHERE 1=1 '') products ON products.VENDOR_ID = vendor.ID",
		},
		{
			name:   "UNION ALL with templates",
			input:  "SELECT ID, NAME, VENDOR_ID FROM PRODUCT t WHERE 1=1 $View.ParentJoinOn(\"AND\",\"VENDOR_ID\") UNION ALL SELECT ID, NAME, VENDOR_ID FROM PRODUCT t WHERE 1=1 $View.ParentJoinOn(\"AND\",\"VENDOR_ID\")",
			expect: "SELECT ID, NAME, VENDOR_ID FROM PRODUCT t WHERE 1=1 '' UNION ALL SELECT ID, NAME, VENDOR_ID FROM PRODUCT t WHERE 1=1 ''",
		},
		{
			name:   "nested if",
			input:  "SELECT * FROM T WHERE 1=1 #if($a > 0) AND A=$a #if($b > 0) AND B=$b #end #end",
			expect: "SELECT * FROM T WHERE 1=1  ",
		},
		{
			name:   "const variable substitution",
			input:  "SELECT * FROM $Vendor t WHERE t.ID IN ($vendorIDs)",
			expect: "SELECT * FROM '' t WHERE t.ID IN ('')",
		},
		{
			name:   "settings directive at top",
			input:  "#setting($_ = $route('/api/v1/test', 'GET'))\nSELECT * FROM T",
			expect: " \nSELECT * FROM T",
		},
		{
			name:   "package and import directives",
			input:  "#package('dev/vendor')\n#import('pkg', 'github.com/acme/pkg')\nSELECT * FROM T",
			expect: "  SELECT * FROM T",
		},
		{
			name:   "complex predicate builder",
			input:  "WHERE ${predicate.Builder().CombineOr($predicate.FilterGroup(0, \"AND\")).Build(\"AND\")}",
			expect: "WHERE ''",
		},
		{
			name:   "dollar at end",
			input:  "SELECT * FROM T WHERE X = $",
			expect: "SELECT * FROM T WHERE X = $",
		},
		{
			name:   "dollar number (not a variable)",
			input:  "SELECT * FROM T WHERE X = $1",
			expect: "SELECT * FROM T WHERE X = $1",
		},
		{
			name:   "cast expression",
			input:  "SELECT CAST($Jwt.FirstName AS CHAR) AS FIRST_NAME FROM T",
			expect: "SELECT CAST('' AS CHAR) AS FIRST_NAME FROM T",
		},
		{
			name:   "logger and unsafe",
			input:  "#foreach($rec in $Unsafe.Records) UPDATE T SET V=$rec.Value WHERE ID=$rec.ID; #end",
			expect: " ",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := stripTemplateVariables(tt.input)
			assert.Equal(t, tt.expect, got)
		})
	}
}

func TestStripTemplateVariables_CTE(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		expect string
	}{
		{
			name: "CTE with template params",
			input: `WITH params AS (
    SELECT DATE_SUB(CURRENT_DATE(), INTERVAL $EndDayInterval DAY) AS end_date,
           CAST(GREATEST($Page, 1) AS INT64) AS page_number
),
perf AS (
    SELECT p.agency_id, SUM(p.impressions) AS imps
    FROM fact_performance p
    JOIN params prm ON TRUE
    WHERE p.event_date BETWEEN prm.start_date AND prm.end_date
      ${predicate.Builder().CombineOr($predicate.FilterGroup(0, "AND")).Build("AND")}
    GROUP BY 1
)
SELECT v.* FROM perf v ORDER BY v.agency_id`,
			expect: `WITH params AS (
    SELECT DATE_SUB(CURRENT_DATE(), INTERVAL '' DAY) AS end_date,
           CAST(GREATEST('', 1) AS INT64) AS page_number
),
perf AS (
    SELECT p.agency_id, SUM(p.impressions) AS imps
    FROM fact_performance p
    JOIN params prm ON TRUE
    WHERE p.event_date BETWEEN prm.start_date AND prm.end_date
      ''
    GROUP BY 1
)
SELECT v.* FROM perf v ORDER BY v.agency_id`,
		},
		{
			name:   "CTE with backtick tables (BigQuery)",
			input:  "WITH data AS (SELECT * FROM `project.dataset.table` t WHERE t.ID = $id) SELECT * FROM data",
			expect: "WITH data AS (SELECT * FROM `project.dataset.table` t WHERE t.ID = '') SELECT * FROM data",
		},
		{
			name:   "UNION ALL in CTE",
			input:  "WITH combined AS (SELECT * FROM T1 WHERE ID = $a UNION ALL SELECT * FROM T2 WHERE ID = $b) SELECT * FROM combined",
			expect: "WITH combined AS (SELECT * FROM T1 WHERE ID = '' UNION ALL SELECT * FROM T2 WHERE ID = '') SELECT * FROM combined",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := stripTemplateVariables(tt.input)
			assert.Equal(t, tt.expect, got)
		})
	}
}

// TestDiscoverySQL_Strategy documents the column discovery strategy for different SQL patterns.
// BigQuery CTEs cannot use WHERE 1=0 (full cost incurred), so the strategy should be:
//   - Simple SELECT * FROM table → use table metadata (INFORMATION_SCHEMA or SELECT * WHERE 1=0)
//   - SELECT with explicit columns → column names from AST, types from table metadata
//   - CTE/WITH queries → parse final SELECT, resolve CTE chain to source tables, use metadata
//   - SQL with velocity templates → strip templates, then apply above rules
func TestDiscoverySQL_Strategy(t *testing.T) {
	tests := []struct {
		name       string
		table      string
		sql        string
		expected   string
		desc       string
		assertions func(t *testing.T, result string)
	}{
		{
			name:     "wildcard with templates — uses table fallback",
			table:    "VENDOR",
			sql:      "SELECT * FROM VENDOR t WHERE t.ID = $vendorID",
			expected: "VENDOR",
			desc:     "template variables → table fallback (safe for all backends)",
		},
		{
			name:     "wildcard with EXCEPT — uses table fallback",
			table:    "VENDOR",
			sql:      "SELECT vendor.* EXCEPT VENDOR_ID FROM VENDOR vendor",
			expected: "VENDOR",
			desc:     "EXCEPT clause → table fallback (EXCEPT is datly extension)",
		},
		{
			name:  "clean explicit SQL gets falsified",
			table: "VENDOR",
			sql:   "SELECT ID, NAME FROM VENDOR WHERE 1=1",
			desc:  "clean SQL → falsified with 1=0 injected",
			assertions: func(t *testing.T, result string) {
				assert.Contains(t, result, "1 = 0")
				assert.Contains(t, strings.ToUpper(result), "ID")
				assert.Contains(t, strings.ToUpper(result), "NAME")
			},
		},
		{
			name:     "empty SQL uses table",
			table:    "VENDOR",
			sql:      "",
			expected: "VENDOR",
			desc:     "no SQL → use table name",
		},
		{
			name:     "CTE with templates — uses table fallback",
			table:    "VENDOR",
			sql:      "WITH cte AS (SELECT * FROM VENDOR WHERE ID = $id) SELECT * FROM cte",
			expected: "VENDOR",
			desc:     "CTE with templates → table fallback (safe for BigQuery)",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &view.View{
				Name:  "test",
				Table: tt.table,
			}
			if tt.sql != "" {
				v.Template = &view.Template{Source: tt.sql}
			}
			got := discoverySQL(v)
			if tt.assertions != nil {
				tt.assertions(t, got)
			} else {
				assert.Equal(t, tt.expected, got, tt.desc)
			}
		})
	}
}

func TestAllPlaceholderColumns(t *testing.T) {
	tests := []struct {
		name   string
		names  []string
		expect bool
	}{
		{"empty", nil, false},
		{"real columns", []string{"ID", "NAME"}, false},
		{"all placeholders", []string{"col_1", "col_2"}, true},
		{"mixed", []string{"col_1", "NAME"}, false},
		{"single placeholder", []string{"col_1"}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var cols view.Columns
			for _, n := range tt.names {
				cols = append(cols, &view.Column{Name: n})
			}
			assert.Equal(t, tt.expect, allPlaceholderColumns(cols))
		})
	}
}

func TestNeedsDiscovery(t *testing.T) {
	tests := []struct {
		name   string
		view   *view.View
		expect bool
	}{
		{"nil view", nil, false},
		{"no columns", &view.View{Name: "t"}, true},
		{"placeholder columns", &view.View{Name: "t", Columns: view.Columns{&view.Column{Name: "col_1"}}}, true},
		{"real columns no wildcard", &view.View{Name: "t", Columns: view.Columns{&view.Column{Name: "ID"}}}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expect, needsDiscovery(tt.view))
		})
	}
}
