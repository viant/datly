package dql

import (
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	"testing"
)

func TestExplicitSelectorTagAuthoring(t *testing.T) {
	for _, tc := range []struct {
		name, decl string
		want       *spec.QuerySelectorBinding
		failure    string
	}{
		{"explicit fluent property", `$FlightsPage<int>(query/flightsPage).QuerySelector('flights','page')`, &spec.QuerySelectorBinding{View: "flights", Property: spec.SelectorPropertyPage}, ""},
		{"explicit property overrides name", `$Limit<int>(query/page).QuerySelector('flights','page')`, &spec.QuerySelectorBinding{View: "flights", Property: spec.SelectorPropertyPage}, ""},
		{"legacy fluent property", `$Page<int>(query/page).QuerySelector('flights')`, &spec.QuerySelectorBinding{View: "flights", Property: spec.SelectorPropertyPage}, ""},
		{"invalid explicit property", `$FlightsPage<int>(query/flightsPage).QuerySelector('flights','unknown')`, nil, "cannot be used"},
		{"empty explicit property", `$FlightsPage<int>(query/flightsPage).QuerySelector('flights','')`, nil, "cannot be used"},
		{"too many arguments", `$Page<int>(query/page).QuerySelector('flights','page','extra')`, nil, "optional property"},
		{"prefixed", `$FlightsPage<int>(query/flightsPage).Optional().Tag('querySelector:"view=flights,property=page"')`, &spec.QuerySelectorBinding{View: "flights", Property: spec.SelectorPropertyPage}, ""},
		{"fixed name", `$Fields<[]string>(query/fields).Tag('querySelector:"flights"')`, &spec.QuerySelectorBinding{View: "flights", Property: spec.SelectorPropertyFields}, ""},
		{"matching declarations", `$Page<int>(query/page).QuerySelector('flights').Tag('querySelector:"view=flights,property=page"')`, &spec.QuerySelectorBinding{View: "flights", Property: spec.SelectorPropertyPage}, ""},
		{"conflicting view", `$Page<int>(query/page).QuerySelector('root').Tag('querySelector:"view=flights,property=page"')`, nil, "conflicts"},
		{"prefixed requires property", `$FlightsPage<int>(query/flightsPage).Tag('querySelector:"flights"')`, nil, "explicit property"},
		{"invalid property", `$FlightsPage<int>(query/flightsPage).Tag('querySelector:"view=flights,property=unknown"')`, nil, "unsupported"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			component, err := parseComponentSource("example.com/selectors", "Read", "#setting($_ = $route('/selectors','GET'))\n#define($_ = "+tc.decl+")\nSELECT 1")
			if tc.failure != "" {
				require.ErrorContains(t, err, tc.failure)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, component.Parameters[0].QuerySelector)
			require.NotNil(t, component.Parameters[0].Cacheable)
			require.False(t, *component.Parameters[0].Cacheable)
		})
	}
}
