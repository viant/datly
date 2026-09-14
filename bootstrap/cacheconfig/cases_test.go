package cacheconfig

import (
	"github.com/viant/datly/spec"
	"reflect"
	"testing"
)

func TestWarmupCases(t *testing.T) {
	for _, tc := range []struct {
		name              string
		required, exclude bool
		max               int
		want              []any
	}{
		{"optional_default", false, false, 0, []any{"1", "2", nil}},
		{"required", true, false, 0, []any{"1", "2"}},
		{"exclude_default", false, true, 0, []any{"1", "2"}},
		{"budget", false, false, 1, []any{"1"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			settings := &spec.CacheWarmupSettings{MaxCases: &tc.max, FieldNames: []string{"ID"}, Cases: []*spec.CacheWarmupCase{{Set: []*spec.CacheWarmupParam{{Name: "Tenant", Values: []string{"1", "2"}, ExcludeDefault: tc.exclude}}}}}
			var actual []any
			err := (Cases{Settings: settings, Required: map[string]bool{"Tenant": tc.required}}).ForEach(func(value Case) error {
				actual = append(actual, value.Values["Tenant"])
				if !reflect.DeepEqual(value.FieldNames, []string{"ID"}) {
					t.Fatal(value)
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(actual, tc.want) {
				t.Fatalf("values=%v,want %v", actual, tc.want)
			}
		})
	}
}

func TestWarmupCaseProductAndValidation(t *testing.T) {
	settings := &spec.CacheWarmupSettings{Cases: []*spec.CacheWarmupCase{{FieldNames: []string{"Name"}, Set: []*spec.CacheWarmupParam{{Name: "Tenant", Values: []string{"a", "b"}, ExcludeDefault: true}, {Name: "Region", Values: []string{"EU", "US"}, ExcludeDefault: true}}}}}
	count := 0
	if err := (Cases{Settings: settings}).ForEach(func(value Case) error {
		count++
		if value.FieldNames[0] != "Name" {
			t.Fatal(value)
		}
		return nil
	}); err != nil || count != 4 {
		t.Fatalf("product count=%d,error=%v", count, err)
	}
	settings.Cases = append(settings.Cases, &spec.CacheWarmupCase{Set: []*spec.CacheWarmupParam{{Name: "missing", ExcludeDefault: true}}})
	count = 0
	if err := (Cases{Settings: settings}).ForEach(func(Case) error { count++; return nil }); err == nil || count != 0 {
		t.Fatalf("invalid configuration ran %d cases, error=%v", count, err)
	}
}

func TestWarmupCaseBudgetCountsSummaryEntries(t *testing.T) {
	budget := 1
	count := 0
	err := (Cases{Settings: &spec.CacheWarmupSettings{MaxCases: &budget}, EntryCost: 2}).ForEach(func(Case) error { count++; return nil })
	if err != nil || count != 0 {
		t.Fatalf("case exceeded entry budget: count=%d,error=%v", count, err)
	}
}
