package openapi

import (
	"testing"

	openapi3 "github.com/viant/datly/gateway/router/openapi/openapi3"
)

func TestDedupe(t *testing.T) {
	tests := []struct {
		name        string
		in          []*openapi3.Parameter
		expectNames []string
	}{
		{
			name: "dedupes by name and location",
			in: []*openapi3.Parameter{
				{Name: "id", In: "query"},
				{Name: "id", In: "query"},
				{Name: "id", In: "path"},
				{Name: "limit", In: "query"},
			},
			expectNames: []string{"id:query", "id:path", "limit:query"},
		},
		{name: "empty", in: nil, expectNames: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out := dedupe(tt.in)
			if len(out) != len(tt.expectNames) {
				t.Fatalf("expected len %d, got %d", len(tt.expectNames), len(out))
			}
			for i := range out {
				actual := out[i].Name + ":" + out[i].In
				if actual != tt.expectNames[i] {
					t.Fatalf("at %d expected %q, got %q", i, tt.expectNames[i], actual)
				}
			}
		})
	}
}
