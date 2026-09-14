package report

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestGroupedSQLiteReportGeneratedAndLinkedInputs(t *testing.T) {
	for _, linked := range []bool{false, true} {
		name := "generated"
		if linked {
			name = "linked"
		}
		t.Run(name, func(t *testing.T) {
			harness := newGroupedReportHarness(t, linked)
			cases := []struct {
				name    string
				request groupedReportRequest
				want    *groupedSpendOutput
			}{
				{
					name: "multiple dimensions codec and non-query filters",
					request: groupedReportRequest{
						Dimensions: []string{"Tenant", "AccountID", "Region"},
						Measures:   []string{"TotalSpend", "OrderCount"},
						Filters: groupedReportFilters{
							AccountIDs: "1,2", Tenant: "acme", Region: "EU", Channel: "web", Status: "active",
						},
						OrderBy: []string{"TotalSpend DESC"},
					},
					want: &groupedSpendOutput{Rows: []*groupedSpendRow{
						{Tenant: "acme", AccountID: 1, Region: "EU", TotalSpend: 150, OrderCount: 2, Details: []*groupedSpendDetail{{Tenant: "acme", AccountID: 1, Label: "alpha"}, {Tenant: "acme", AccountID: 1, Label: "beta"}}},
						{Tenant: "acme", AccountID: 2, Region: "EU", TotalSpend: 70, OrderCount: 1, Details: []*groupedSpendDetail{{Tenant: "acme", AccountID: 2, Label: "gamma"}}},
					}},
				},
				{
					name: "aggregate only",
					request: groupedReportRequest{
						Measures: []string{"TotalSpend", "OrderCount"},
						Filters: groupedReportFilters{
							AccountIDs: "1,2", Tenant: "acme", Region: "EU", Channel: "web", Status: "active",
						},
					},
					want: &groupedSpendOutput{Rows: []*groupedSpendRow{{TotalSpend: 220, OrderCount: 3}}},
				},
				{
					name: "compound relation order limit and offset",
					request: groupedReportRequest{
						Dimensions: []string{"Tenant", "AccountID"}, Measures: []string{"TotalSpend"},
						Filters: groupedReportFilters{AccountIDs: "1,2,3", Tenant: "acme"},
						OrderBy: []string{"TotalSpend DESC"}, Limit: intPointer(1), Offset: intPointer(1),
					},
					want: &groupedSpendOutput{Rows: []*groupedSpendRow{{
						Tenant: "acme", AccountID: 2, TotalSpend: 100,
						Details: []*groupedSpendDetail{{Tenant: "acme", AccountID: 2, Label: "gamma"}},
					}}},
				},
			}

			for _, testCase := range cases {
				t.Run(testCase.name, func(t *testing.T) {
					actual := harness.invoke(t, testCase.request)
					if !reflect.DeepEqual(actual, testCase.want) {
						t.Fatalf("grouped output = %s, want %s", jsonValue(actual), jsonValue(testCase.want))
					}
				})
			}

			// Reuse the same compiled report and reader plans after different selector
			// shapes to prove invocation state does not leak between calls.
			repeated := harness.invoke(t, cases[0].request)
			if !reflect.DeepEqual(repeated, cases[0].want) {
				t.Fatalf("reused plan output = %#v, want %#v", repeated, cases[0].want)
			}
			fromMCP := harness.invokeMCP(t, cases[0].request)
			if !reflect.DeepEqual(fromMCP, cases[0].want) {
				t.Fatalf("MCP grouped output = %#v, want %#v", fromMCP, cases[0].want)
			}
			if calls := harness.codec.calls.Load(); calls != int32(len(cases)+2) {
				t.Fatalf("codec calls = %d, want %d", calls, len(cases)+2)
			}
		})
	}
}

func intPointer(value int) *int {
	return &value
}

func jsonValue(value any) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "<json error: " + err.Error() + ">"
	}
	return string(encoded)
}
