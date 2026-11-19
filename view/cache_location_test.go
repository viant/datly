package view

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCacheProviderLocationViewFields(t *testing.T) {
	v := &View{Name: "orders", Alias: "o", Table: "order_items", Selector: &Config{}}
	for _, tc := range []struct{ location, want string }{
		{"cache/${View.Name}", "cache/orders"},
		{"cache/${View.Alias}", "cache/o"},
		{"cache/${View.Table}", "cache/order_items"},
		{`cache/${View\.Name}`, "cache/orders"},
		{`cache/${View\.Alias}`, "cache/o"},
		{`cache/${View\.Table}`, "cache/order_items"},
		{"${View.Name}_${View.Alias}_${View.Table}", "orders_o_order_items"},
	} {
		t.Run(tc.location, func(t *testing.T) {
			c := &Cache{Location: tc.location}
			location, err := c.ExpandedLocation(v)
			require.NoError(t, err)
			require.Equal(t, tc.want, location)
		})
	}
}
