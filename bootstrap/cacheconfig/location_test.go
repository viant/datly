package cacheconfig

import (
	"github.com/viant/datly/spec"
	"testing"
)

func TestExpandLocationViewMetadata(t *testing.T) {
	view := &spec.View{Name: "orders", Namespace: "o", Source: &spec.ViewSource{Table: "order_items"}}
	for _, tc := range []struct{ input, want string }{
		{"cache/${View.Name}", "cache/orders"},
		{"cache/${View.Alias}", "cache/o"},
		{"cache/${View.Table}", "cache/order_items"},
		{`${View\.Name}/${View\.Alias}/${View\.Table}`, "orders/o/order_items"},
		{"$View.Name/$View.Alias/$View.Table", "orders/o/order_items"},
		{"${View.Name}/${region}", "orders/${region}"},
	} {
		t.Run(tc.input, func(t *testing.T) {
			if got := ExpandLocation(tc.input, view); got != tc.want {
				t.Fatalf("got %q want %q", got, tc.want)
			}
		})
	}
}
